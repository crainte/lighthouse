//! Router construction and HTTP handlers served directly by the global node
//! (as opposed to `relay`, which proxies to regions, and `merge`, which
//! folds their answers together).

use super::merge::{query_usize, query_value, scope_aggregate_checks};
use super::wire::{PushAck, RegionStatePush};
use super::GlobalNode;
use super::{admin, cluster, relay};
use crate::coordinator::storage::SectionDisplay;
use crate::models::{CheckId, NodeState};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get, post, put};
use axum::{body::Bytes, Json, Router};
use serde::Serialize;
use std::collections::HashMap;
use tokio::sync::broadcast;

pub(super) fn router(node: GlobalNode) -> Router {
    let router = Router::new()
        // Aggregated reads served from cache — the shapes the SPA already
        // consumes, so the dashboard renders with no frontend change.
        .route(
            "/metrics",
            get(|State(node): State<GlobalNode>| async move {
                (
                    [(
                        header::CONTENT_TYPE,
                        "application/openmetrics-text; version=1.0.0; charset=utf-8",
                    )],
                    crate::openmetrics::render(&node.metrics()),
                )
            }),
        )
        .route("/api/v1/status", get(status_handler))
        .route("/api/v1/cluster", get(cluster_handler))
        .route("/api/v1/admin/region-tree", get(region_tree_handler))
        .route("/api/v1/alerts", get(alerts_handler))
        .route("/api/v1/incidents", get(incidents_handler))
        // The fleet view is per-region, so it must be gathered from every
        // region and merged — the single-region relay would show only the
        // first region's agents. Explicit route so it never falls through to
        // that relay.
        .route("/api/v1/agents", get(agents_handler))
        // Agent credentials live in the region whose agents present them.
        // Listing gathers every region; minting goes to the one region named,
        // never fanned out (that would mint a different secret per region).
        .route(
            "/api/v1/admin/agent-credentials",
            get(list_credentials_handler).post(mint_credential_handler),
        )
        // Revoke and delete address one credential that only its own region
        // holds, so the others' 404s must not decide the answer.
        .route(
            "/api/v1/admin/agent-credentials/{id}",
            delete(revoke_credential_handler),
        )
        .route(
            "/api/v1/admin/agent-credentials/{id}/purge",
            post(purge_credential_handler),
        )
        // Incidents are regional and never replicated: a hand-made one is
        // opened in the one region the operator names, not fanned out (that
        // would open a separate copy per region). Edits to an existing one
        // route by its composite id through the relay below.
        .route("/api/v1/admin/incidents", post(create_incident_handler))
        // The catalogue this node holds authoritatively (Phase 1): checks
        // and sections. Explicit routes so they never fall through to the
        // regional relay below.
        .route(
            "/api/v1/admin/checks",
            get(admin::list_checks_handler).post(admin::create_check_handler),
        )
        .route(
            "/api/v1/admin/checks/order",
            put(admin::reorder_checks_handler),
        )
        .route(
            "/api/v1/admin/checks/{id}",
            put(admin::update_check_handler).delete(admin::delete_check_handler),
        )
        .route(
            "/api/v1/admin/sections",
            get(admin::list_sections_handler)
                .post(admin::create_section_handler)
                .put(admin::reorder_sections_handler),
        )
        .route(
            "/api/v1/admin/sections/{name}",
            put(admin::rename_section_handler).delete(admin::delete_section_handler),
        )
        .route("/api/v1/sections", get(admin::public_sections_handler))
        // Region registration and removal, and decommissioning a peer
        // global node. See `regions`.
        .route(
            "/api/v1/admin/regions",
            get(admin::list_regions_handler).post(admin::mint_region_handler),
        )
        .route(
            "/api/v1/admin/regions/{name}",
            delete(admin::remove_region_handler),
        )
        .route(
            "/api/v1/admin/cluster/members/{node_id}",
            delete(admin::decommission_member_handler),
        )
        // Auth: same contract as the coordinator so the SPA's login flow
        // works unchanged. `session` also carries a `mode` marker.
        .route(
            "/api/v1/login",
            post(login_handler).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/api/v1/logout", post(logout_handler))
        .route("/api/v1/session", get(session_handler))
        // Live dashboard socket. The global node has no incident engine, so
        // it emits only `state_change`; the SPA refetches authoritative
        // status on each one, exactly as it does against a coordinator.
        .route("/api/v1/ws", get(ws_handler))
        // Push ingest from regional coordinators. Token-gated.
        .route(
            "/internal/global/state",
            // Decoded size: a 100k-check snapshot is ~23 MB of JSON (~2.8 MB
            // on the wire, zstd-compressed).
            post(push_handler).layer(DefaultBodyLimit::max(64 * 1024 * 1024)),
        )
        // Global-to-global config plane (Phase 4): the same shape a
        // coordinator's own `/internal/config`/`/internal/digest` are,
        // peer-gated by `global.peer_token` rather than a region's.
        .route(
            "/internal/config",
            post(cluster::config_handler)
                .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
                .get(cluster::config_entries_handler),
        )
        .route("/internal/digest", get(cluster::digest_handler))
        .route("/internal/cluster/ping", post(cluster::ping_handler))
        // Everything else under /api/v1 that a coordinator answers is
        // relayed to regions: drill-down reads (history, uptime,
        // sparklines) to the owning region, and admin operations this
        // node is not authoritative for (notifiers, credentials, rollup
        // policies, maintenance…) fanned out to every region so the fleet
        // stays uniform; incident writes go only to the incident's owner.
        // `any` so writes, not just GETs, reach the relay.
        .fallback(any(relay::relay_handler))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(axum::middleware::from_fn(crate::security::security_headers));
    crate::http::compressed(router).with_state(node)
}

/// A live event pushed to dashboard WebSocket clients. Only `state_change` is
/// emitted from the global node — it has no incident engine of its own; those
/// events reach the SPA when it refetches after a state change. The shape
/// matches the coordinator's `WsEvent::StateChange` so the SPA's existing
/// parser consumes it unchanged.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum GlobalWsEvent {
    StateChange {
        check_id: CheckId,
        from: NodeState,
        to: NodeState,
        timestamp: i64,
    },
}

// ---- Push ingest ------------------------------------------------------------

async fn push_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Json(push): Json<RegionStatePush>,
) -> Response {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match node.admit_push(bearer, &push.region, push.advertise_url.as_deref()) {
        Ok(()) => {}
        Err(super::regions::PushDenied::Unauthenticated) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "unknown or revoked region registration key",
            )
        }
        Err(super::regions::PushDenied::WrongRegion(bound)) => {
            return error_response(
                StatusCode::FORBIDDEN,
                &format!("this key is registered to region {bound:?}"),
            )
        }
    }
    let now = chrono::Utc::now().timestamp_millis();
    // Diff the cross-region rollup around the push so a dashboard socket sees a
    // `state_change` the instant a check flips *globally* — a region reporting
    // a check newly down that no other region reports up, say. Per-region
    // churn that does not move the rollup emits nothing, which is correct: the
    // aggregated dashboard has not changed.
    let before = node.inner.cache.state_snapshot(now);
    let outcome = node.inner.cache.apply_push(push, now);
    let after = node.inner.cache.state_snapshot(now);
    broadcast_state_changes(&node, &before, &after, now);
    // Re-evaluate global alerts on every push: it is the only thing that
    // changes cached state, and staleness is checked here too so a region that
    // stops pushing is still noticed on the next surviving region's push.
    node.inner.alerter.evaluate(&node.inner.cache, now).await;
    Json(PushAck {
        accepted: outcome == super::cache::PushOutcome::Applied,
        resync: outcome == super::cache::PushOutcome::Resync,
    })
    .into_response()
}

/// Emit a `state_change` for every check whose cross-region rollup moved. A
/// check that appeared (no prior state) is announced as a transition from
/// `Unknown`; one that vanished is left alone — the SPA drops checks the next
/// status refetch omits, and inventing a "to: unknown" event would only race
/// that refetch.
fn broadcast_state_changes(
    node: &GlobalNode,
    before: &HashMap<CheckId, NodeState>,
    after: &HashMap<CheckId, NodeState>,
    now: i64,
) {
    for (check_id, &to) in after {
        let from = before.get(check_id).copied().unwrap_or(NodeState::Unknown);
        if from != to {
            // A send error just means no dashboard is connected; that is the
            // normal idle state, not a failure.
            let _ = node.inner.events.send(GlobalWsEvent::StateChange {
                check_id: *check_id,
                from,
                to,
                timestamp: now,
            });
        }
    }
}

async fn ws_handler(State(node): State<GlobalNode>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| handle_ws(socket, node))
}

async fn handle_ws(mut socket: WebSocket, node: GlobalNode) {
    let mut rx = node.inner.events.subscribe();
    loop {
        match rx.recv().await {
            Ok(ev) => {
                let Ok(txt) = serde_json::to_string(&ev) else {
                    continue;
                };
                if socket.send(Message::Text(txt.into())).await.is_err() {
                    break; // client gone
                }
            }
            // A slow client that fell behind the ring buffer just resumes from
            // the newest event; the next status refetch reconciles any gap.
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

// ---- Aggregated reads -------------------------------------------------------

#[derive(Serialize)]
struct StatusResponse {
    checks: Vec<CheckStatusDto>,
    page: usize,
    limit: usize,
    total_checks: usize,
    has_more: bool,
    agents: Vec<FleetRowDto>,
    maintenance: Vec<serde_json::Value>,
    sections: Vec<serde_json::Value>,
}

/// Matches the coordinator's public `CheckStatusDto` field-for-field, so the
/// same `Check` interface in the frontend deserializes it.
#[derive(Serialize)]
struct CheckStatusDto {
    check_id: CheckId,
    name: String,
    #[serde(rename = "status")]
    state: NodeState,
    latency_ms: u64,
    last_check: i64,
    /// `None` when no region measured anything in the window; see the
    /// coordinator's own status DTO.
    uptime_24h: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<crate::models::CheckKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    latency_graph: bool,
    position: i32,
}

/// One region rendered as a public fleet row (`kind: "region"`), which the
/// status page already knows how to draw — so regions appear where agents
/// would on a single coordinator.
#[derive(Serialize)]
struct FleetRowDto {
    kind: &'static str,
    name: String,
    status: &'static str,
    healthy: u32,
    total: u32,
}

async fn status_handler(State(node): State<GlobalNode>, uri: Uri) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let page = query_usize(uri.query(), "page").unwrap_or(0);
    let limit = crate::page::bounded(query_usize(uri.query(), "limit"), 100);
    let section = query_value(uri.query(), "section");
    let all = query_value(uri.query(), "all").as_deref() == Some("true");
    // Global owns the section catalogue. A region's cached check metadata does
    // not know if its group is presented inline or as a separate page.
    let defined_sections = match node.inner.store.list_sections().await {
        Ok(sections) => sections,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not load section metadata",
            )
                .into_response()
        }
    };
    let paged_sections: std::collections::HashSet<String> = defined_sections
        .iter()
        .filter(|s| s.display == SectionDisplay::Page)
        .map(|s| s.name.clone())
        .collect();
    let mut all_checks = node.inner.cache.aggregate(now);
    // Section rollups cover *all* reporting checks, not merely the bounded
    // public page. A paged section must not become "0 checks" or "unknown".
    let mut grouped = std::collections::BTreeMap::<String, Vec<NodeState>>::new();
    for check in &all_checks {
        if let Some(name) = &check.group {
            grouped.entry(name.clone()).or_default().push(check.state);
        }
    }
    let section_rollups: Vec<serde_json::Value> = defined_sections
        .iter()
        .filter_map(|section| {
            let states = grouped.get(&section.name)?;
            let counts = states.iter().copied().map(|state| (state, 1usize));
            let status =
                crate::rollup::rollup_counts(&crate::rollup::RollupPolicy::CHECK_DEFAULT, counts);
            Some(serde_json::json!({
                "name": section.name,
                "status": status,
                "checks": states.len(),
                "down": states.iter().filter(|&&s| s == NodeState::Down).count(),
                "degraded": states.iter().filter(|&&s| s == NodeState::Degraded).count(),
            }))
        })
        .collect();
    scope_aggregate_checks(&mut all_checks, section.as_deref(), all, &paged_sections);
    let mut checks = all_checks
        .into_iter()
        .map(|c| CheckStatusDto {
            check_id: c.check_id,
            name: c.name,
            state: c.state,
            latency_ms: c.latency_ms,
            last_check: c.last_check,
            uptime_24h: c.uptime_24h,
            status_code: c.status_code,
            error: c.error,
            kind: c.kind,
            group: c.group,
            latency_graph: c.latency_graph,
            position: c.position,
        })
        .collect::<Vec<_>>();
    checks.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.check_id.cmp(&b.check_id))
    });
    let total_checks = checks.len();
    let start = page.saturating_mul(limit).min(total_checks);
    let end = start.saturating_add(limit).min(total_checks);
    let has_more = end < total_checks;
    checks.drain(..start);
    checks.truncate(end - start);
    let agents = node
        .inner
        .cache
        .roster(now)
        .into_iter()
        .map(|r| FleetRowDto {
            kind: "region",
            name: r.region,
            // The public fleet row is three-state (up/degraded/down). A dark or
            // never-seen region has no "unknown" to show there, so it reads as
            // down — which is what a region that has stopped answering is.
            status: match r.health {
                NodeState::Up => "up",
                NodeState::Degraded => "degraded",
                NodeState::Down | NodeState::Unknown => "down",
            },
            // Real per-region probe-agent counts the region reported, falling
            // back to a 0/0 for a region that has not yet pushed any fleet data
            // rather than a fabricated 1/1.
            healthy: r.agents_healthy,
            total: r.agents_total,
        })
        .collect();
    Json(StatusResponse {
        checks,
        page,
        limit,
        total_checks,
        has_more,
        agents,
        maintenance: Vec::new(),
        sections: section_rollups,
    })
    .into_response()
}

// The region roster, shaped as the cluster-membership view so the admin
// "cluster" panel lists regions instead of peer coordinators.
#[derive(Clone, Serialize)]
struct ClusterMemberDto {
    node_id: String,
    url: String,
    generation: i64,
    last_seen: i64,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    build: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<u32>,
    /// The regional coordinator whose push this entry was last built from.
    #[serde(skip_serializing_if = "Option::is_none")]
    origin: Option<String>,
}

#[derive(Serialize)]
struct ClusterResponse {
    #[serde(rename = "self")]
    me: ClusterMemberDto,
    members: Vec<ClusterMemberDto>,
    now: i64,
    mode: &'static str,
    links: Vec<ClusterLinkDto>,
    link_stale_after_ms: i64,
    regions: Vec<RegionRosterDto>,
    /// Global-to-global peers, distinct from `members` (regional reports).
    global_peers: Vec<ClusterMemberDto>,
    /// Observed links between global control-plane peers.
    global_links: Vec<crate::coordinator::cluster::LinkObservation>,
    global_link_stale_after_ms: Option<i64>,
}

/// One region's own coordinator cluster, exactly as that region's
/// `/api/v1/cluster` reports it. Gathered on request rather than pushed: it is
/// a diagnostic read by an open Topology page, and costs nothing otherwise.
#[derive(Serialize)]
struct RegionRosterDto {
    region: String,
    /// Every coordinator in the region, its `self` first. Empty when the
    /// region did not answer.
    members: Vec<serde_json::Value>,
    links: Vec<serde_json::Value>,
    now: Option<i64>,
    link_stale_after_ms: Option<i64>,
    available: bool,
}

impl RegionRosterDto {
    fn from_view(region: String, mut view: serde_json::Value) -> Self {
        let mut members = vec![view["self"].take()];
        members.extend(take_array(&mut view["members"]));
        Self {
            region,
            members,
            links: take_array(&mut view["links"]),
            now: view["now"].as_i64(),
            link_stale_after_ms: view["link_stale_after_ms"].as_i64(),
            available: true,
        }
    }

    fn unavailable(region: String) -> Self {
        Self {
            region,
            members: Vec::new(),
            links: Vec::new(),
            now: None,
            link_stale_after_ms: None,
            available: false,
        }
    }

    /// The build one of this region's coordinators reported.
    fn build_of(&self, node_id: &str) -> Option<String> {
        self.members
            .iter()
            .find(|m| m["node_id"] == node_id)
            .and_then(|m| m["build"].as_str())
            .map(str::to_string)
    }
}

fn take_array(value: &mut serde_json::Value) -> Vec<serde_json::Value> {
    match value.take() {
        serde_json::Value::Array(items) => items,
        _ => Vec::new(),
    }
}

#[derive(Serialize)]
struct ClusterLinkDto {
    source: String,
    target: String,
    source_generation: i64,
    observed_at: i64,
    outcome: &'static str,
}

fn global_member_dto(member: crate::coordinator::cluster::Member) -> ClusterMemberDto {
    ClusterMemberDto {
        node_id: member.node_id,
        url: member.url,
        generation: member.generation,
        last_seen: member.last_seen,
        state: member.state.as_str(),
        build: member.build,
        protocol: member.protocol,
        origin: None,
    }
}

async fn cluster_handler(
    State(node): State<GlobalNode>,
    _admin: crate::http::AdminSession,
) -> Response {
    let gathered = relay::gather_regions(&node, "/api/v1/cluster").await;
    let now = chrono::Utc::now().timestamp_millis();
    let roster = node.inner.cache.roster(now);
    let mut regions: Vec<RegionRosterDto> = gathered
        .answers
        .into_iter()
        .map(|(region, view)| RegionRosterDto::from_view(region, view))
        .collect();
    regions.extend(
        gathered
            .unavailable
            .into_iter()
            .map(RegionRosterDto::unavailable),
    );
    regions.sort_by(|a, b| a.region.cmp(&b.region));
    let members = roster
        .iter()
        .map(|r| {
            let origin = (!r.origin.is_empty()).then(|| r.origin.clone());
            ClusterMemberDto {
                node_id: r.region.clone(),
                url: r.endpoints.first().cloned().unwrap_or_default(),
                generation: 0,
                last_seen: if r.age_ms < 0 { now } else { now - r.age_ms },
                state: if r.stale { "dead" } else { "alive" },
                build: origin.as_deref().and_then(|origin| {
                    regions
                        .iter()
                        .find(|g| g.region == r.region)
                        .and_then(|g| g.build_of(origin))
                }),
                protocol: None,
                origin,
            }
        })
        .collect();
    let links: Vec<ClusterLinkDto> = roster
        .iter()
        .filter(|r| r.age_ms >= 0)
        .map(|r| ClusterLinkDto {
            source: r.region.clone(),
            target: "global".into(),
            source_generation: 0,
            observed_at: now - r.age_ms,
            outcome: if r.stale { "failed" } else { "connected" },
        })
        .collect();
    // Global peers are a separate cluster, not regional report members.
    let (me, global_peers, global_links, global_link_stale_after_ms) =
        if let Some(cluster) = node.inner.cluster.as_ref() {
            let me = global_member_dto(cluster.self_member(now));
            let mut peers: Vec<_> = cluster
                .snapshot()
                .into_iter()
                .filter(|peer| peer.node_id != me.node_id)
                .map(global_member_dto)
                .collect();
            // HashMap-backed membership snapshots have no stable peer order.
            peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
            peers.insert(0, me.clone());
            (
                me,
                peers,
                cluster.link_snapshot(),
                Some(cluster.link_stale_after_ms()),
            )
        } else {
            let me = ClusterMemberDto {
                node_id: "global".into(),
                url: String::new(),
                generation: 0,
                last_seen: now,
                state: "alive",
                build: Some(crate::version::current().to_string()),
                protocol: None,
                origin: None,
            };
            (me.clone(), vec![me], vec![], None)
        };
    // Region reports terminate at the serving global peer, not a
    // synthetic "global" id when global clustering is configured.
    let links: Vec<ClusterLinkDto> = links
        .into_iter()
        .map(|mut link| {
            link.target = me.node_id.clone();
            link
        })
        .collect();
    Json(ClusterResponse {
        me,
        members,
        now,
        mode: "global",
        links,
        link_stale_after_ms: node.inner.cache.staleness_ms(),
        regions,
        global_peers,
        global_links,
        global_link_stale_after_ms,
    })
    .into_response()
}

/// The coordinator's region tree, assembled across regions: each region
/// answers for its own subtree (zones, agents, and the policies it stores and
/// applies), and this node supplies the check root it computes itself.
#[derive(Serialize)]
struct RegionTreeResponse {
    checks: Vec<CheckTreeDto>,
    unavailable_regions: Vec<String>,
}

#[derive(Serialize)]
struct CheckTreeDto {
    check_id: CheckId,
    name: String,
    state: NodeState,
    /// What this node rolls regions up by. A stored `global` override is
    /// applied by each region to its own root, never here.
    effective_policy: crate::rollup::RollupPolicy,
    /// Region nodes as each region reported them. A region that did not
    /// answer carries its cached state, no zones and a `null` policy.
    regions: Vec<serde_json::Value>,
}

async fn region_tree_handler(
    State(node): State<GlobalNode>,
    _admin: crate::http::AdminSession,
) -> Response {
    let gathered = match relay::gather_some(&node, "/api/v1/admin/region-tree").await {
        Ok(gathered) => gathered,
        Err(response) => return response,
    };
    let now = chrono::Utc::now().timestamp_millis();
    Json(merge_region_tree(node.inner.cache.aggregate(now), gathered)).into_response()
}

fn merge_region_tree(
    aggregate: Vec<super::cache::AggregatedCheck>,
    gathered: relay::Gathered,
) -> RegionTreeResponse {
    use std::collections::BTreeMap;
    // check -> (name, region -> that region's own node for the check)
    let mut reported: HashMap<CheckId, (String, BTreeMap<String, serde_json::Value>)> =
        HashMap::new();
    for (_, mut body) in gathered.answers {
        for mut check in take_array(&mut body["checks"]) {
            let Some(id) = check["check_id"].as_str().and_then(|s| s.parse().ok()) else {
                continue;
            };
            let entry = reported.entry(id).or_default();
            if entry.0.is_empty() {
                entry.0 = check["name"].as_str().unwrap_or_default().to_string();
            }
            for region in take_array(&mut check["regions"]) {
                if let Some(name) = region["region"].as_str() {
                    entry.1.entry(name.to_string()).or_insert(region);
                }
            }
        }
    }
    let mut checks: Vec<CheckTreeDto> = aggregate
        .into_iter()
        .map(|c| {
            let (_, mut nodes) = reported.remove(&c.check_id).unwrap_or_default();
            for (region, state) in c.per_region {
                nodes.entry(region.clone()).or_insert_with(|| {
                    serde_json::json!({
                        "region": region,
                        "state": state,
                        "effective_policy": null,
                        "zones": [],
                    })
                });
            }
            CheckTreeDto {
                check_id: c.check_id,
                name: c.name,
                state: c.state,
                effective_policy: super::cache::ROLLUP,
                regions: nodes.into_values().collect(),
            }
        })
        .collect();
    // Checks no live push carries (hidden monitors, or a region whose push is
    // stale) still have regional trees; roll them up the way the cache would.
    let mut rest: Vec<_> = reported.into_iter().collect();
    rest.sort_by_key(|(id, _)| *id);
    checks.extend(rest.into_iter().map(|(check_id, (name, nodes))| {
        let states: Vec<NodeState> = nodes
            .values()
            .filter_map(|n| serde_json::from_value(n["state"].clone()).ok())
            .collect();
        CheckTreeDto {
            check_id,
            name,
            state: crate::rollup::rollup(&super::cache::ROLLUP, &states),
            effective_policy: super::cache::ROLLUP,
            regions: nodes.into_values().collect(),
        }
    }));
    RegionTreeResponse {
        checks,
        unavailable_regions: gathered.unavailable,
    }
}

/// The admin fleet view, gathered from every region and merged. Each region
/// only knows its own agents, so a single-region relay (the fallback) would
/// show one region's fleet and silently drop the rest. This queries one
/// coordinator per region — the cluster replicates within a region — with the
/// trust token, then unions the results.
///
/// Agents are deduped by id (an id is unique across regions in practice; if two
/// regions ever name the same id, first-region-wins is stable because
/// `gather_regions` answers in region-name order). The two check-name
/// lists are unioned, since a check unrunnable in one region is worth surfacing
/// even if another region can run it.
async fn agents_handler(
    State(node): State<GlobalNode>,
    _admin: crate::http::AdminSession,
) -> Response {
    match relay::gather_some(&node, "/api/v1/agents").await {
        Ok(gathered) => Json(merge_agents(gathered)).into_response(),
        Err(response) => response,
    }
}

/// The two fleet-health lists each region reports: a bounded sample of names
/// and the count the sample was taken from.
const FLEET_DIAGNOSTICS: [&str; 2] = ["checks_with_no_live_agent", "checks_no_agent_can_run"];

/// Union the regions' fleets. Totals are summed, not recounted from the
/// samples: a region caps its sample, so the union of samples undercounts
/// exactly when the count matters. A check orphaned in two regions is two
/// problems and counts twice.
fn merge_agents(gathered: relay::Gathered) -> serde_json::Value {
    let mut agents: Vec<serde_json::Value> = Vec::new();
    let mut seen_ids = std::collections::HashSet::new();
    let mut samples: [std::collections::BTreeSet<String>; 2] = Default::default();
    let mut totals = [0u64; 2];
    for (_, body) in gathered.answers {
        for a in body["agents"].as_array().into_iter().flatten() {
            let id = a["agent_id"].as_str().unwrap_or("");
            if id.is_empty() || seen_ids.insert(id.to_string()) {
                agents.push(a.clone());
            }
        }
        for (i, key) in FLEET_DIAGNOSTICS.iter().enumerate() {
            let sample = body[*key].as_array();
            totals[i] += body[format!("{key}_total")]
                .as_u64()
                .unwrap_or_else(|| sample.map_or(0, |s| s.len() as u64));
            samples[i].extend(
                sample
                    .into_iter()
                    .flatten()
                    .filter_map(|s| s.as_str().map(str::to_string)),
            );
        }
    }
    let mut out = serde_json::json!({
        "agents": agents,
        "unavailable_regions": gathered.unavailable,
    });
    for (i, key) in FLEET_DIAGNOSTICS.iter().enumerate() {
        out[*key] = serde_json::json!(std::mem::take(&mut samples[i]));
        out[format!("{key}_total")] = serde_json::json!(totals[i]);
    }
    out
}

/// Every region's agent credentials in one list. Each region holds only its
/// own, so a single-region relay would hide the rest.
async fn list_credentials_handler(
    State(node): State<GlobalNode>,
    _admin: crate::http::AdminSession,
) -> Response {
    match relay::gather_some(&node, "/api/v1/admin/agent-credentials").await {
        Ok(gathered) => Json(merge_credentials(gathered)).into_response(),
        Err(response) => response,
    }
}

fn merge_credentials(gathered: relay::Gathered) -> serde_json::Value {
    let mut seen = std::collections::HashSet::new();
    let mut credentials: Vec<serde_json::Value> = gathered
        .answers
        .into_iter()
        .flat_map(|(_, mut body)| take_array(&mut body["credentials"]))
        .filter(|c| seen.insert(c["id"].as_str().unwrap_or_default().to_string()))
        .collect();
    credentials.sort_by_key(|c| {
        (
            c["region"].as_str().unwrap_or_default().to_string(),
            c["zone"].as_str().unwrap_or_default().to_string(),
            c["created_at"].as_i64().unwrap_or_default(),
        )
    });
    serde_json::json!({
        "credentials": credentials,
        "unavailable_regions": gathered.unavailable,
    })
}

async fn revoke_credential_handler(
    State(node): State<GlobalNode>,
    axum::extract::Path(id): axum::extract::Path<String>,
    _admin: crate::http::AdminSession,
) -> Response {
    credential_write(&node, Method::DELETE, &id, "").await
}

async fn purge_credential_handler(
    State(node): State<GlobalNode>,
    axum::extract::Path(id): axum::extract::Path<String>,
    _admin: crate::http::AdminSession,
) -> Response {
    credential_write(&node, Method::POST, &id, "/purge").await
}

async fn credential_write(node: &GlobalNode, method: Method, id: &str, suffix: &str) -> Response {
    // The id is spliced into a region URL: anything beyond the minted
    // alphabet could address a different path there.
    if id.is_empty()
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return error_response(StatusCode::NOT_FOUND, "no such credential");
    }
    relay::write_to_holding_region(
        node,
        &method,
        &format!("/api/v1/admin/agent-credentials/{id}{suffix}"),
    )
    .await
}

#[derive(serde::Deserialize)]
struct MintCredentialRequest {
    region: String,
}

/// Mint an agent credential in the region it is scoped to. The region's
/// coordinator mints, stores the hash and replicates it to its peers; the
/// token passes through here once, on the way back to the operator.
async fn mint_credential_handler(
    State(node): State<GlobalNode>,
    crate::http::AdminJson(body): crate::http::AdminJson<serde_json::Value>,
) -> Response {
    let Ok(request) = serde_json::from_value::<MintCredentialRequest>(body.clone()) else {
        return error_response(StatusCode::BAD_REQUEST, "region is required");
    };
    let region = request.region.trim();
    let now = chrono::Utc::now().timestamp_millis();
    let Some((_, endpoints)) = node
        .inner
        .cache
        .region_endpoints(now)
        .into_iter()
        .find(|(name, _)| name == region)
    else {
        return error_response(
            StatusCode::NOT_FOUND,
            &format!("region {region:?} is not registered or has never reported"),
        );
    };
    // The region validates and answers; its response, token included, is
    // returned verbatim.
    relay::relay_single_path(
        &node,
        &Method::POST,
        "/api/v1/admin/agent-credentials",
        &Bytes::from(body.to_string()),
        &endpoints,
        true,
    )
    .await
}

/// Open a hand-made incident in the region named by `region`. The region
/// validates the rest of the body; its answer comes back with the incident's
/// composite id, so the editor can keep addressing it through its owner.
async fn create_incident_handler(
    State(node): State<GlobalNode>,
    crate::http::AdminJson(mut body): crate::http::AdminJson<serde_json::Value>,
) -> Response {
    let region = body
        .as_object_mut()
        .and_then(|fields| fields.remove("region"))
        .and_then(|value| value.as_str().map(|s| s.trim().to_string()))
        .filter(|region| !region.is_empty());
    let Some(region) = region else {
        return error_response(StatusCode::BAD_REQUEST, "region is required");
    };
    let now = chrono::Utc::now().timestamp_millis();
    let Some((_, endpoints)) = node
        .inner
        .cache
        .region_endpoints(now)
        .into_iter()
        .find(|(name, _)| *name == region)
    else {
        return error_response(
            StatusCode::NOT_FOUND,
            &format!("region {region:?} is not registered or has never reported"),
        );
    };
    relay::relay_incident_detail(
        &node,
        &Method::POST,
        "/api/v1/admin/incidents",
        &Bytes::from(body.to_string()),
        &endpoints,
        true,
        &region,
    )
    .await
}

/// Global has no incidents or alerts of its own — the regions own those. Return
/// empty envelopes so the SPA's incident and alert panels load cleanly rather
/// than erroring on a 404.
async fn alerts_handler(State(node): State<GlobalNode>, uri: Uri) -> Response {
    let regions = node
        .inner
        .cache
        .region_endpoints(chrono::Utc::now().timestamp_millis());
    // A newly started global control plane has no regional data source yet.
    // This is a valid *empty* feed, not a relay failure. Once a region has
    // reported, relay_merge still fails closed if that region stops answering.
    if regions.is_empty() {
        return Json(serde_json::json!({ "alerts": [] })).into_response();
    }
    relay::relay_merge(&node, &Method::GET, &uri, &Bytes::new(), &regions, false).await
}

async fn incidents_handler(State(node): State<GlobalNode>, uri: Uri) -> Response {
    let regions = node
        .inner
        .cache
        .region_endpoints(chrono::Utc::now().timestamp_millis());
    if regions.is_empty() {
        return Json(serde_json::json!({ "incidents": [], "groups": [] })).into_response();
    }
    relay::relay_merge(&node, &Method::GET, &uri, &Bytes::new(), &regions, false).await
}

// ---- Auth -------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct LoginRequest {
    password: String,
}

#[derive(Serialize)]
struct SessionResponse {
    authenticated: bool,
    mode: &'static str,
}

async fn login_handler(
    State(node): State<GlobalNode>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<LoginRequest>,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let client_ip = crate::security::client_ip(peer.ip(), &headers, &node.inner.trusted_proxies);
    match node.inner.auth.login(&body.password, client_ip, now) {
        Ok(token) => {
            let secure = if node.inner.secure_cookies {
                "; Secure"
            } else {
                ""
            };
            let cookie = format!(
                "{}={}; HttpOnly; SameSite=Strict; Path=/; Max-Age={}{}",
                crate::auth::GLOBAL_SESSION_COOKIE,
                token,
                12 * 60 * 60,
                secure,
            );
            (
                StatusCode::OK,
                [(header::SET_COOKIE, cookie)],
                Json(SessionResponse {
                    authenticated: true,
                    mode: "global",
                }),
            )
                .into_response()
        }
        Err(crate::auth::LoginError::Disabled) => error_response(
            StatusCode::FORBIDDEN,
            "no admin password is configured on this server",
        ),
        Err(crate::auth::LoginError::RateLimited) => {
            error_response(StatusCode::TOO_MANY_REQUESTS, "too many attempts")
        }
        Err(crate::auth::LoginError::Invalid) => {
            error_response(StatusCode::UNAUTHORIZED, "invalid password")
        }
    }
}

async fn logout_handler(State(node): State<GlobalNode>, headers: HeaderMap) -> Response {
    if let Some(token) = session_token(&headers) {
        node.inner.auth.logout(token);
    }
    let secure = if node.inner.secure_cookies {
        "; Secure"
    } else {
        ""
    };
    let cleared = format!(
        "{}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0{}",
        crate::auth::GLOBAL_SESSION_COOKIE,
        secure,
    );
    (
        StatusCode::OK,
        [(header::SET_COOKIE, cleared)],
        Json(SessionResponse {
            authenticated: false,
            mode: "global",
        }),
    )
        .into_response()
}

async fn session_handler(State(node): State<GlobalNode>, headers: HeaderMap) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let authenticated = session_token(&headers).is_some_and(|t| node.inner.auth.validate(t, now));
    Json(SessionResponse {
        authenticated,
        mode: "global",
    })
    .into_response()
}

fn session_token(headers: &HeaderMap) -> Option<&str> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    crate::auth::cookie_value(cookies, crate::auth::GLOBAL_SESSION_COOKIE)
}

pub(super) fn is_admin(node: &GlobalNode, headers: &HeaderMap) -> bool {
    let now = chrono::Utc::now().timestamp_millis();
    session_token(headers).is_some_and(|t| node.inner.auth.validate(t, now))
}

pub(crate) use crate::http::error_response;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global::cache::AggregatedCheck;
    use serde_json::json;

    fn gathered(answers: Vec<(&str, serde_json::Value)>, unavailable: &[&str]) -> relay::Gathered {
        relay::Gathered {
            answers: answers
                .into_iter()
                .map(|(region, body)| (region.to_string(), body))
                .collect(),
            unavailable: unavailable.iter().map(|r| r.to_string()).collect(),
        }
    }

    #[test]
    fn merged_fleet_keeps_each_regions_totals_not_its_sample_length() {
        let merged = merge_agents(gathered(
            vec![
                (
                    "tw",
                    json!({
                        "agents": [{"agent_id": "a"}],
                        "checks_with_no_live_agent": ["x"],
                        "checks_with_no_live_agent_total": 120,
                        "checks_no_agent_can_run": [],
                        "checks_no_agent_can_run_total": 0,
                    }),
                ),
                (
                    "us",
                    // A region predating the totals counts its sample.
                    json!({
                        "agents": [{"agent_id": "a"}, {"agent_id": "b"}],
                        "checks_with_no_live_agent": ["x", "y"],
                        "checks_no_agent_can_run": ["z"],
                    }),
                ),
            ],
            &["eu"],
        ));
        assert_eq!(merged["agents"].as_array().unwrap().len(), 2);
        assert_eq!(merged["checks_with_no_live_agent"], json!(["x", "y"]));
        assert_eq!(merged["checks_with_no_live_agent_total"], 122);
        assert_eq!(merged["checks_no_agent_can_run_total"], 1);
        assert_eq!(merged["unavailable_regions"], json!(["eu"]));
    }

    #[test]
    fn credentials_from_every_region_are_listed_once_in_location_order() {
        let cred = |id: &str, region: &str, zone: &str| json!({"id": id, "region": region, "zone": zone, "created_at": 1, "revoked": false});
        let merged = merge_credentials(gathered(
            vec![
                ("tw", json!({"credentials": [cred("t1", "tw", "a")]})),
                (
                    "us",
                    json!({"credentials": [cred("u2", "us", "b"), cred("u1", "us", "a"), cred("t1", "tw", "a")]}),
                ),
            ],
            &[],
        ));
        let ids: Vec<&str> = merged["credentials"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["t1", "u1", "u2"]);
    }

    #[test]
    fn region_roster_reports_the_publishers_real_build() {
        let roster = RegionRosterDto::from_view(
            "us".into(),
            json!({
                "self": {"node_id": "us-a", "build": "0.1.0 (abc, built x)"},
                "members": [{"node_id": "us-b", "build": "0.1.0 (def, built y)"}],
                "links": [{"source": "us-a", "target": "us-b"}],
                "now": 5,
                "link_stale_after_ms": 9,
            }),
        );
        assert_eq!(roster.members.len(), 2);
        assert_eq!(roster.links.len(), 1);
        assert_eq!(
            roster.build_of("us-b").as_deref(),
            Some("0.1.0 (def, built y)")
        );
        assert_eq!(roster.build_of("us-c"), None);
    }

    fn aggregated(id: CheckId, per_region: &[(&str, NodeState)]) -> AggregatedCheck {
        AggregatedCheck {
            check_id: id,
            name: "api".into(),
            state: NodeState::Down,
            per_region: per_region
                .iter()
                .map(|(r, s)| (r.to_string(), *s))
                .collect(),
            latency_ms: 0,
            last_check: 0,
            uptime_24h: None,
            status_code: None,
            error: None,
            kind: None,
            group: None,
            latency_graph: false,
            position: 0,
        }
    }

    #[test]
    fn global_region_tree_carries_regional_subtrees_and_its_own_root_policy() {
        let api = CheckId::from_u128(1);
        let hidden = CheckId::from_u128(2);
        let stored = json!({"degraded_when": "all", "down_when": "all"});
        let us_tree = json!({"checks": [
            {"check_id": api.to_string(), "name": "api", "regions": [{
                "region": "us", "state": "down", "effective_policy": stored,
                "zones": [{"zone": "a", "state": "down", "effective_policy": stored, "agents": [
                    {"id": "agent-1", "state": "down", "staleness_ms": 10}
                ]}],
            }]},
            {"check_id": hidden.to_string(), "name": "hidden", "regions": [{
                "region": "us", "state": "degraded", "effective_policy": stored, "zones": [],
            }]},
        ]});
        let tree = merge_region_tree(
            vec![aggregated(
                api,
                &[("us", NodeState::Down), ("tw", NodeState::Up)],
            )],
            gathered(vec![("us", us_tree)], &["tw"]),
        );
        let json = serde_json::to_value(&tree).unwrap();
        let api_node = &json["checks"][0];
        assert_eq!(api_node["name"], "api");
        assert_eq!(
            api_node["effective_policy"],
            json!({"degraded_when": "any", "down_when": "any"})
        );
        let regions = api_node["regions"].as_array().unwrap();
        assert_eq!(regions[0]["region"], "tw");
        assert_eq!(regions[0]["effective_policy"], serde_json::Value::Null);
        assert_eq!(regions[0]["state"], "up");
        assert_eq!(regions[1]["effective_policy"], stored);
        assert_eq!(regions[1]["zones"][0]["agents"][0]["id"], "agent-1");
        // A check no push carries still appears, rolled up by the same rule.
        assert_eq!(json["checks"][1]["check_id"], hidden.to_string());
        assert_eq!(json["checks"][1]["state"], "degraded");
        assert_eq!(json["unavailable_regions"], json!(["tw"]));
    }
}
