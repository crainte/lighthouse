//! The global coordinator (`warden global`), Stage B of the design.
//!
//! It holds **no raw data**. Regional coordinators push a state summary every
//! few seconds; the global node caches the latest per region ([`cache`]),
//! serves an aggregated dashboard over the same public API the coordinator
//! does — so the embedded SPA renders unchanged — and reverse-proxies any read
//! that needs raw history back to a coordinator in the owning region. A single
//! node today; the push/cache split is shaped so global-to-global replication
//! can be added later without changing the wire format (see [`wire`]).

mod admin;
pub mod alert;
pub mod cache;
pub mod cluster;
pub mod incidents;
pub mod reconcile;
pub mod store;
pub mod wire;

use crate::config::GlobalConfig;
use crate::models::{CheckId, NodeState};
use alert::GlobalAlerter;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get, post, put};
use axum::{body::Bytes, Json, Router};
use cache::GlobalCache;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, CONTROLS};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::warn;
use wire::{PushAck, RegionStatePush};

/// A live event pushed to dashboard WebSocket clients. Only `state_change` is
/// emitted from the global node — it has no incident engine of its own; those
/// events reach the SPA when it refetches after a state change. The shape
/// matches the coordinator's `WsEvent::StateChange` so the SPA's existing
/// parser consumes it unchanged.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GlobalWsEvent {
    StateChange {
        check_id: CheckId,
        from: NodeState,
        to: NodeState,
        timestamp: i64,
    },
}

/// Shared global-node state for axum handlers.
#[derive(Clone)]
pub struct GlobalNode {
    inner: Arc<Inner>,
}

struct Inner {
    cache: GlobalCache,
    alerter: GlobalAlerter,
    auth: crate::auth::Auth,
    /// Secret regional coordinators present on `/internal/global/*`. Also the
    /// credential the global node presents *back* to a region as a `Bearer` to
    /// relay admin operations an operator drives from the global dashboard,
    /// and as the push destination for the durable catalogue in `store`.
    token: Option<String>,
    secure_cookies: bool,
    /// Reverse proxies trusted to supply `X-Forwarded-For` for login rate
    /// limiting, parsed by `GlobalConfig::validate`. `new` is not fallible,
    /// so a config that bypassed `validate` (only possible from a direct,
    /// non-`main` construction, e.g. a test) falls back to trusting nothing.
    trusted_proxies: crate::security::TrustedProxies,
    /// Client for proxying drill-down reads and admin writes to regions.
    client: reqwest::Client,
    /// Fan-out for dashboard WebSocket clients. A `state_change` is published
    /// here whenever an incoming push moves a check's cross-region rollup.
    events: broadcast::Sender<GlobalWsEvent>,
    /// The durable catalogue (SCALING.md "Placement-scoped configuration",
    /// Phase 1). Authoritative for checks and sections; everything else the
    /// global dashboard edits still goes straight to regions the way it
    /// always has. See `store`'s module doc.
    store: store::GlobalStore,
    /// Membership and config anti-entropy against other global nodes
    /// (Phase 4). `None` when `global.peers` is not configured — a single
    /// global node is a complete deployment on its own.
    cluster: Option<Arc<crate::coordinator::cluster::Cluster>>,
    /// Secret other global nodes present on `/internal/*`. Kept alongside
    /// `cluster` rather than read out of it: a coordinator's own
    /// `require_peer` gates the same way, against its own copy rather than
    /// reaching into `Cluster`, which has no public accessor for it.
    peer_token: Option<String>,
    /// How often `spawn_background`'s heartbeat loop pings every global peer.
    gossip_interval_secs: u64,
    /// How often `spawn_background`'s anti-entropy loop reconciles the
    /// catalogue with a peer global node and with every known region.
    anti_entropy_secs: u64,
}

impl GlobalNode {
    /// Opens the durable catalogue and, when `global.peers` is configured,
    /// this node's own cluster membership — the two things a global node
    /// needs that a cache-only one did not. Everything else is unchanged
    /// from before Phase 1.
    pub async fn new(cfg: &GlobalConfig) -> anyhow::Result<Self> {
        let cache = GlobalCache::new(cfg.region_staleness_secs, &cfg.regions);
        // Signing key from the shared push token when present, so a restart
        // does not invalidate every admin session — the same trick the
        // coordinator uses with its peer token.
        let auth = match cfg.resolved_token() {
            Some(token) => {
                crate::auth::Auth::with_signing_key(cfg.resolved_admin_password(), token.as_bytes())
            }
            None => crate::auth::Auth::new(cfg.resolved_admin_password()),
        };
        if !auth.admin_enabled() {
            warn!("no admin password set (WARDEN_ADMIN_PASSWORD); the admin UI is disabled");
        }
        let store = store::GlobalStore::open(cfg.db_path.clone(), cfg.resolved_node_id()).await?;
        let node_id = store.node_id().to_string();
        let cluster = match (cfg.resolved_peer_token(), cfg.resolved_advertise_url()) {
            (Some(token), Some(url)) => {
                let now = chrono::Utc::now().timestamp_millis();
                let c = crate::coordinator::cluster::Cluster::new(
                    node_id,
                    url,
                    now,
                    cfg.peers.clone(),
                    Some(token),
                    cfg.peer_timeout_secs,
                );
                Some(Arc::new(c))
            }
            _ => None,
        };
        let peer_token = cfg.resolved_peer_token();
        let (events, _) = broadcast::channel(256);
        Ok(Self {
            inner: Arc::new(Inner {
                cache,
                alerter: GlobalAlerter::new(cfg.notifiers.clone()),
                auth,
                token: cfg.resolved_token(),
                secure_cookies: cfg.secure_cookies,
                trusted_proxies: crate::security::TrustedProxies::parse(&cfg.trusted_proxies)
                    .unwrap_or_default(),
                client: reqwest::Client::new(),
                events,
                store,
                cluster,
                peer_token,
                gossip_interval_secs: cfg.gossip_interval_secs,
                anti_entropy_secs: cfg.anti_entropy_secs,
            }),
        })
    }

    /// Starts the background loops a running global node needs beyond
    /// serving requests: membership heartbeat and config anti-entropy against
    /// peer global nodes (a no-op without `global.peers`), and catalogue
    /// reconciliation against every known region. Mirrors
    /// `Coordinator::spawn_cluster`; kept separate from `new` for the same
    /// reason — a test that only wants a router has no use for timers.
    pub fn spawn_background(&self) {
        cluster::spawn(self);
        reconcile::spawn_region_reconciler(self.clone());
    }

    pub fn router(self) -> Router {
        Router::new()
            // Aggregated reads served from cache — the shapes the SPA already
            // consumes, so the dashboard renders with no frontend change.
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
                "/api/v1/admin/checks/:id",
                put(admin::update_check_handler).delete(admin::delete_check_handler),
            )
            .route(
                "/api/v1/admin/sections",
                get(admin::list_sections_handler)
                    .post(admin::create_section_handler)
                    .put(admin::reorder_sections_handler),
            )
            .route(
                "/api/v1/admin/sections/:name",
                put(admin::rename_section_handler).delete(admin::delete_section_handler),
            )
            .route("/api/v1/sections", get(admin::public_sections_handler))
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
                post(push_handler).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
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
            // policies, incidents, maintenance…) fanned out to every region so
            // the fleet stays uniform. `any` so writes, not just GETs, reach
            // the relay.
            .fallback(any(relay_handler))
            .layer(DefaultBodyLimit::max(1024 * 1024))
            .layer(axum::middleware::from_fn(crate::security::security_headers))
            .with_state(self)
    }
}

// ---- Push ingest ------------------------------------------------------------

async fn push_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Json(push): Json<RegionStatePush>,
) -> Response {
    if let Some(denied) = require_push_token(&node, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    // Diff the cross-region rollup around the push so a dashboard socket sees a
    // `state_change` the instant a check flips *globally* — a region reporting
    // a check newly down that no other region reports up, say. Per-region
    // churn that does not move the rollup emits nothing, which is correct: the
    // aggregated dashboard has not changed.
    let before = node.inner.cache.state_snapshot(now);
    node.inner.cache.apply_push(push, now);
    let after = node.inner.cache.state_snapshot(now);
    broadcast_state_changes(&node, &before, &after, now);
    // Re-evaluate global alerts on every push: it is the only thing that
    // changes cached state, and staleness is checked here too so a region that
    // stops pushing is still noticed on the next surviving region's push.
    node.inner.alerter.evaluate(&node.inner.cache, now).await;
    Json(PushAck { accepted: true }).into_response()
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
                if socket.send(Message::Text(txt)).await.is_err() {
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

fn require_push_token(node: &GlobalNode, headers: &HeaderMap) -> Option<Response> {
    let expected = node.inner.token.as_deref()?;
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if presented == Some(expected) {
        None
    } else {
        Some(error_response(
            StatusCode::UNAUTHORIZED,
            "invalid push token",
        ))
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
    let mut checks = node
        .inner
        .cache
        .aggregate(now)
        .into_iter()
        .filter(|check| {
            section
                .as_deref()
                .is_none_or(|want| check.group.as_deref() == Some(want))
        })
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
        sections: Vec::new(),
    })
    .into_response()
}

// The region roster, shaped as the cluster-membership view so the admin
// "cluster" panel lists regions instead of peer coordinators.
#[derive(Serialize)]
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
}

#[derive(Serialize)]
struct ClusterLinkDto {
    source: String,
    target: &'static str,
    source_generation: i64,
    observed_at: i64,
    outcome: &'static str,
}

async fn cluster_handler(State(node): State<GlobalNode>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let roster = node.inner.cache.roster(now);
    let members = roster
        .iter()
        .map(|r| ClusterMemberDto {
            node_id: r.region.clone(),
            url: r.endpoints.first().cloned().unwrap_or_default(),
            generation: 0,
            last_seen: if r.age_ms < 0 { now } else { now - r.age_ms },
            state: if r.stale { "dead" } else { "alive" },
            build: (!r.origin.is_empty()).then(|| format!("origin {}", r.origin)),
            protocol: None,
        })
        .collect();
    let links = roster
        .iter()
        .filter(|r| r.age_ms >= 0)
        .map(|r| ClusterLinkDto {
            source: r.region.clone(),
            target: "global",
            source_generation: 0,
            observed_at: now - r.age_ms,
            outcome: if r.stale { "failed" } else { "connected" },
        })
        .collect();
    Json(ClusterResponse {
        me: ClusterMemberDto {
            node_id: "global".into(),
            url: String::new(),
            generation: 0,
            last_seen: now,
            state: "alive",
            build: Some(crate::version::current().to_string()),
            protocol: None,
        },
        members,
        now,
        mode: "global",
        links,
        link_stale_after_ms: node.inner.cache.staleness_ms(),
    })
    .into_response()
}

// Aggregated region tree: check -> region -> state, matching the coordinator's
// RegionTreeResponse closely enough that the admin region-tree panel renders.
// The global node has no zones or agents, so those arrays are empty.
#[derive(Serialize)]
struct RegionTreeResponse {
    checks: Vec<CheckTreeDto>,
}

#[derive(Serialize)]
struct CheckTreeDto {
    check_id: CheckId,
    state: NodeState,
    // The SPA reads `effective_policy.degraded_when`; without it the region-tree
    // panel dereferences undefined and crashes. The global node has no stored
    // per-node overrides, so it reports the built-in rollup defaults it actually
    // aggregates by.
    effective_policy: crate::rollup::RollupPolicy,
    regions: Vec<RegionNodeDto>,
}

#[derive(Serialize)]
struct RegionNodeDto {
    region: String,
    state: NodeState,
    effective_policy: crate::rollup::RollupPolicy,
    zones: Vec<serde_json::Value>,
}

async fn region_tree_handler(State(node): State<GlobalNode>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let checks = node
        .inner
        .cache
        .aggregate(now)
        .into_iter()
        .map(|c| {
            let mut regions: Vec<RegionNodeDto> = c
                .per_region
                .into_iter()
                .map(|(region, state)| RegionNodeDto {
                    region,
                    state,
                    // Regions roll their zones up by the region default; the
                    // global node has no zone visibility but reports the policy
                    // edge so the panel renders and edits read sanely.
                    effective_policy: crate::rollup::RollupPolicy::REGION_DEFAULT,
                    zones: Vec::new(),
                })
                .collect();
            regions.sort_by(|a, b| a.region.cmp(&b.region));
            CheckTreeDto {
                check_id: c.check_id,
                state: c.state,
                effective_policy: crate::rollup::RollupPolicy::CHECK_DEFAULT,
                regions,
            }
        })
        .collect();
    Json(RegionTreeResponse { checks }).into_response()
}

/// The admin fleet view, gathered from every region and merged. Each region
/// only knows its own agents, so a single-region relay (the fallback) would
/// show one region's fleet and silently drop the rest. This queries one
/// coordinator per region — the cluster replicates within a region — with the
/// trust token, then unions the results.
///
/// Agents are deduped by id (an id is unique across regions in practice; if two
/// regions ever name the same id, first-region-wins is stable because
/// `region_endpoints` orders regions deterministically). The two check-name
/// lists are unioned, since a check unrunnable in one region is worth surfacing
/// even if another region can run it.
async fn agents_handler(State(node): State<GlobalNode>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let regions = node.inner.cache.region_endpoints(now);
    if regions.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to serve this query yet",
        );
    }

    let mut agents: Vec<serde_json::Value> = Vec::new();
    let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut no_live_agent: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut no_agent_can_run: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    let mut answered = false;

    for (region, endpoints) in &regions {
        // One coordinator per region answers; the cluster replicates the fleet
        // view internally, so trying the first live endpoint is enough.
        let mut got = None;
        for base in endpoints {
            let req = region_request(
                &node,
                &Method::GET,
                base,
                "/api/v1/agents",
                &Bytes::new(),
                true,
            );
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<serde_json::Value>().await {
                        Ok(v) => {
                            got = Some(v);
                            break;
                        }
                        Err(e) => {
                            warn!(region = %region, base = %base, error = %e, "agents relay: bad json")
                        }
                    }
                }
                Ok(resp) => {
                    warn!(region = %region, base = %base, status = %resp.status(), "agents relay: non-2xx")
                }
                Err(e) => {
                    warn!(region = %region, base = %base, error = %e, "agents relay failed, trying next")
                }
            }
        }
        let Some(body) = got else { continue };
        answered = true;
        if let Some(list) = body.get("agents").and_then(|a| a.as_array()) {
            for a in list {
                let id = a.get("agent_id").and_then(|v| v.as_str()).unwrap_or("");
                if id.is_empty() || seen_ids.insert(id.to_string()) {
                    agents.push(a.clone());
                }
            }
        }
        collect_strings(&body, "checks_with_no_live_agent", &mut no_live_agent);
        collect_strings(&body, "checks_no_agent_can_run", &mut no_agent_can_run);
    }

    if !answered {
        return error_response(
            StatusCode::BAD_GATEWAY,
            "no region coordinator answered the fleet query",
        );
    }

    Json(serde_json::json!({
        "agents": agents,
        "checks_with_no_live_agent": no_live_agent.into_iter().collect::<Vec<_>>(),
        "checks_no_agent_can_run": no_agent_can_run.into_iter().collect::<Vec<_>>(),
    }))
    .into_response()
}

/// Fold one region's string array into an accumulating set.
fn collect_strings(
    body: &serde_json::Value,
    key: &str,
    out: &mut std::collections::BTreeSet<String>,
) {
    if let Some(list) = body.get(key).and_then(|v| v.as_array()) {
        for s in list {
            if let Some(s) = s.as_str() {
                out.insert(s.to_string());
            }
        }
    }
}

/// Global has no incidents or alerts of its own — the regions own those. Return
/// empty envelopes so the SPA's incident and alert panels load cleanly rather
/// than erroring on a 404.
async fn alerts_handler(State(node): State<GlobalNode>, uri: Uri) -> Response {
    relay_merge(
        &node,
        &Method::GET,
        &uri,
        &Bytes::new(),
        &node
            .inner
            .cache
            .region_endpoints(chrono::Utc::now().timestamp_millis()),
        false,
    )
    .await
}

async fn incidents_handler(State(node): State<GlobalNode>, uri: Uri) -> Response {
    relay_merge(
        &node,
        &Method::GET,
        &uri,
        &Bytes::new(),
        &node
            .inner
            .cache
            .region_endpoints(chrono::Utc::now().timestamp_millis()),
        false,
    )
    .await
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

fn is_admin(node: &GlobalNode, headers: &HeaderMap) -> bool {
    let now = chrono::Utc::now().timestamp_millis();
    session_token(headers).is_some_and(|t| node.inner.auth.validate(t, now))
}

fn require_admin(node: &GlobalNode, headers: &HeaderMap) -> Option<Response> {
    (!is_admin(node, headers))
        .then(|| error_response(StatusCode::UNAUTHORIZED, "authentication required"))
}

// ---- Region relay: drill-down reads, admin reads, admin write fan-out ------

/// Everything under `/api/v1` the global node does not serve from its own
/// cache or its own catalogue (`global::admin` — checks and sections,
/// intercepted by explicit routes before this fallback is ever reached) is
/// relayed to regional coordinators. Three shapes:
///
/// * **Public reads** (`GET`, no `/admin/`) — a drill-down: route to the region
///   that owns the named check, take the first answer.
/// * **Admin reads** (`GET /api/v1/admin/...`) — same single-answer relay, but
///   the operator's global session is translated into the shared trust token so
///   the region authorizes it. De-duplication of list results is left to the
///   region: they run identical config, so any one region's list is the fleet's.
/// * **Admin writes** (`POST`/`PUT`/`PATCH`/`DELETE`) not covered by
///   `global::admin` — notifiers, credentials, rollup policies, maintenance,
///   incidents — are fanned out to *every* region so the fleet stays uniform.
///   The operator sees the first region's response; a failure on any region is
///   surfaced so a partial fan-out is never silently reported as success. This
///   is the pre-Phase-1 mechanism, unreconciled if a region misses it; see
///   `fan_out_write`'s doc for why the catalogue no longer uses it.
///
/// Non-API paths serve the embedded SPA.
async fn relay_handler(
    State(node): State<GlobalNode>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().trim_start_matches('/');
    if !path.starts_with("api/") {
        return static_asset(&uri);
    }
    let now = chrono::Utc::now().timestamp_millis();
    let is_admin_path = path.starts_with("api/v1/admin/");
    let is_write = !matches!(method, Method::GET | Method::HEAD);
    // A signed-in operator here relays as admin to the regions. This gates
    // writes and admin paths, and — for reads — decides whether the region is
    // asked as admin (full detail, and some "public" reads like `/agents` are
    // in fact admin-gated) or anonymously.
    let operator = is_admin(&node, &headers);

    // Writes and admin paths require a signed-in operator; the region only ever
    // sees the trust token, never the operator's session.
    if (is_admin_path || is_write) && !operator {
        return error_response(StatusCode::UNAUTHORIZED, "authentication required");
    }

    let composite = extract_incident_id(path)
        .as_deref()
        .and_then(parse_composite_incident_id);
    if is_write {
        // Regional incidents are not replicated between regions. Once an
        // incident has been returned from a merged list its composite id is its
        // routing key, so edits must go only to that owner, with the local id.
        if let Some((region, local_id)) = composite {
            let Some(endpoints) = node
                .inner
                .cache
                .region_endpoints(now)
                .into_iter()
                .find_map(|(name, endpoints)| (name == region).then_some(endpoints))
            else {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "incident owner region is unavailable",
                );
            };
            let pq = rewrite_incident_path(&uri, &local_id);
            return relay_single_path(&node, &method, &pq, &body, &endpoints, true).await;
        }
        return fan_out_write(&node, &method, &uri, &body, now).await;
    }

    if let Some((region, local_id)) = composite {
        let Some(endpoints) = node
            .inner
            .cache
            .region_endpoints(now)
            .into_iter()
            .find_map(|(name, endpoints)| (name == region).then_some(endpoints))
        else {
            return error_response(
                StatusCode::NOT_FOUND,
                "incident owner region is unavailable",
            );
        };
        let pq = rewrite_incident_path(&uri, &local_id);
        return relay_incident_detail(&node, &method, &pq, &body, &endpoints, operator, &region)
            .await;
    }

    // Data-bearing reads need one answer from every owner region. Global keeps
    // no storage: it merges only these transient responses.
    let region_endpoints = match extract_check_id(path) {
        Some(id) => node.inner.cache.owner_region_endpoints(id, now),
        None if path == "api/v1/alerts"
            || path == "api/v1/incidents"
            || path == "api/v1/admin/results"
            // The status page's batched series. Fleet-wide *data*, not config:
            // a check is observed only by the agents of the region holding it,
            // so one region answers with empty buckets for every check it does
            // not probe — which the page renders as "Collecting latency data…"
            // for checks that have plenty of it elsewhere.
            || path == "api/v1/uptime"
            || path == "api/v1/latency-points" =>
        {
            node.inner.cache.region_endpoints(now)
        }
        None => Vec::new(),
    };
    if !region_endpoints.is_empty() {
        return relay_merge(&node, &method, &uri, &body, &region_endpoints, operator).await;
    }

    // Config and other singleton reads are identical in every region.
    relay_single(
        &node,
        &method,
        &uri,
        &body,
        &node.inner.cache.all_endpoints(now),
        operator,
    )
    .await
}

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS.add(b'/').add(b'%').add(b'?').add(b'#');

fn decode_path_segment(segment: &str) -> Option<String> {
    percent_decode_str(segment)
        .decode_utf8()
        .ok()
        .map(Into::into)
}

fn extract_check_id(path: &str) -> Option<CheckId> {
    let rest = path.strip_prefix("api/v1/checks/")?;
    let id = rest.split('/').next()?;
    decode_path_segment(id)?.parse().ok()
}

fn extract_incident_id(path: &str) -> Option<String> {
    let path = path.strip_prefix("api/v1/")?;
    let path = path.strip_prefix("admin/").unwrap_or(path);
    let rest = path.strip_prefix("incidents/")?;
    let id = rest
        .split('/')
        .next()
        .filter(|id| !id.is_empty() && *id != "purge-short")?;
    decode_path_segment(id)
}

/// A regional incident id is namespaced at the global boundary. The separator
/// is deliberately a character regional ids do not mint (`:`), making the
/// mapping reversible without global storage.
fn composite_incident_id(region: &str, id: &str) -> String {
    format!("{region}:{id}")
}

fn parse_composite_incident_id(id: &str) -> Option<(String, String)> {
    let (region, local) = id.split_once(':')?;
    (!region.is_empty() && !local.is_empty()).then(|| (region.to_string(), local.to_string()))
}

fn rewrite_incident_path(uri: &Uri, local_id: &str) -> String {
    let pq = uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or(uri.path());
    let (path, query) = pq.split_once('?').unwrap_or((pq, ""));
    let marker = "/incidents/";
    let rewritten = path.find(marker).map_or_else(
        || path.to_string(),
        |start| {
            let id_start = start + marker.len();
            let suffix = path[id_start..]
                .find('/')
                .map(|i| &path[id_start + i..])
                .unwrap_or("");
            format!(
                "{}{}{}",
                &path[..id_start],
                utf8_percent_encode(local_id, PATH_SEGMENT_ENCODE_SET),
                suffix
            )
        },
    );
    if query.is_empty() {
        rewritten
    } else {
        format!("{rewritten}?{query}")
    }
}

/// Build a region-bound request, attaching the trust token when the operation
/// is admin so the region authorizes it as the global node acting for an
/// operator. Non-admin reads carry no credential — they hit public endpoints.
fn region_request(
    node: &GlobalNode,
    method: &Method,
    base: &str,
    pq: &str,
    body: &Bytes,
    authed: bool,
) -> reqwest::RequestBuilder {
    let url = format!("{}{}", base.trim_end_matches('/'), pq);
    let mut req = node
        .inner
        .client
        .request(method.clone(), &url)
        .body(body.clone());
    if authed {
        if let Some(token) = node.inner.token.as_deref() {
            req = req.bearer_auth(token);
        }
    }
    // A body from the SPA is always JSON; tell the region so it parses it.
    if !body.is_empty() {
        req = req.header(header::CONTENT_TYPE, "application/json");
    }
    req
}

/// Relay a read to the first endpoint that answers, returning its response
/// verbatim. `authed` attaches the trust token (admin reads).
async fn relay_single_path(
    node: &GlobalNode,
    method: &Method,
    pq: &str,
    body: &Bytes,
    endpoints: &[String],
    authed: bool,
) -> Response {
    if endpoints.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to serve this query yet",
        );
    }
    for base in endpoints {
        match region_request(node, method, base, pq, body, authed)
            .send()
            .await
        {
            Ok(resp) => return passthrough(resp).await,
            Err(e) => warn!(base = %base, error = %e, "region relay failed, trying next"),
        }
    }
    error_response(
        StatusCode::BAD_GATEWAY,
        "no region coordinator answered the query",
    )
}

/// Read one coordinator from every owner region concurrently. A merged response
/// is authoritative only when every owner contributed; partial history is more
/// misleading than a visible outage.
async fn relay_merge(
    node: &GlobalNode,
    method: &Method,
    uri: &Uri,
    body: &Bytes,
    regions: &[(String, Vec<String>)],
    authed: bool,
) -> Response {
    // Batched chart pages are selected from the global aggregate below. Do not
    // let every region apply its own page first: regional order and membership
    // differ, so that double pagination can omit every globally selected id.
    let pq = if matches!(uri.path(), "/api/v1/uptime" | "/api/v1/latency-points") {
        chart_relay_path(uri)
    } else {
        uri.path_and_query()
            .map(|p| p.as_str())
            .unwrap_or(uri.path())
            .to_string()
    };
    if regions.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to serve this query yet",
        );
    }

    let mut requests = tokio::task::JoinSet::new();
    for (region, endpoints) in regions {
        let node = node.clone();
        let method = method.clone();
        let pq = pq.clone();
        let body = body.clone();
        let region = region.clone();
        let endpoints = endpoints.clone();
        requests.spawn(async move {
            let result = fetch_region_json(&node, &method, &pq, &body, &endpoints, authed).await;
            (region, result)
        });
    }

    let mut merged = Vec::with_capacity(regions.len());
    while let Some(result) = requests.join_next().await {
        match result {
            Ok((region, Ok(value))) => merged.push((region, value)),
            Ok((region, Err(message))) => {
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("region '{region}' is unavailable: {message}"),
                );
            }
            Err(error) => {
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("region relay task failed: {error}"),
                )
            }
        }
    }
    match merge_responses(uri.path(), uri.query(), merged) {
        Ok(mut response) => {
            if matches!(uri.path(), "/api/v1/uptime" | "/api/v1/latency-points") {
                filter_chart_page(node, uri, &mut response.0);
            }
            if uri.path().ends_with("/incidents") {
                attach_incident_groups(node, &mut response.0);
            }
            response.into_response()
        }
        Err(message) => error_response(StatusCode::BAD_GATEWAY, &message),
    }
}

/// Add a `"groups"` array alongside the merged `"incidents"` list: the same
/// per-region rows, folded by same-check overlapping windows and attributed
/// to a cross-region upstream where one applies. Presentation only — the
/// flat `"incidents"` array this reads is left exactly as `merge_responses`
/// built it, so a caller keyed to individual incident ids (the admin editor,
/// `IncidentEditor`) is unaffected. See `incidents::group_and_attribute` for
/// the rules.
fn attach_incident_groups(node: &GlobalNode, response: &mut serde_json::Value) {
    let Some(items) = response.get("incidents").and_then(|v| v.as_array()) else {
        return;
    };
    let raw: Vec<incidents::RawIncident> = items
        .iter()
        .filter_map(incidents::RawIncident::from_json)
        .collect();
    if raw.is_empty() {
        return;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let depends_on = node.inner.cache.depends_on_map(now);
    let groups = incidents::group_and_attribute(&raw, &depends_on, now);
    response["groups"] =
        serde_json::to_value(groups).unwrap_or_else(|_| serde_json::Value::Array(Vec::new()));
}

/// Remove status-page selectors before fanning a batched chart request out.
/// The global aggregate supplies the one authoritative page after merging.
fn chart_relay_path(uri: &Uri) -> String {
    let query = uri.query().map(|query| {
        query
            .split('&')
            .filter(|pair| {
                !matches!(
                    pair.split_once('=').map(|(key, _)| key),
                    Some("page" | "limit" | "section")
                )
            })
            .collect::<Vec<_>>()
            .join("&")
    });
    // A global page is capped at 1,000 rows. Ask each region for that full
    // candidate set so a regional default page (100) cannot hide ids selected
    // by a later global page.
    match query.as_deref() {
        Some("") | None => format!("{}?limit=1000", uri.path()),
        Some(query) => format!("{}?{query}&limit=1000", uri.path()),
    }
}

/// Keep chart entries to precisely the ids rendered by the global status page.
fn filter_chart_page(node: &GlobalNode, uri: &Uri, response: &mut serde_json::Value) {
    let page = query_usize(uri.query(), "page").unwrap_or(0);
    let limit = crate::page::bounded(query_usize(uri.query(), "limit"), 100);
    let section = query_value(uri.query(), "section");
    let mut checks = node
        .inner
        .cache
        .aggregate(chrono::Utc::now().timestamp_millis());
    checks.retain(|check| {
        section
            .as_deref()
            .is_none_or(|want| check.group.as_deref() == Some(want))
    });
    checks.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.check_id.cmp(&b.check_id))
    });
    let start = page.saturating_mul(limit).min(checks.len());
    let end = start.saturating_add(limit).min(checks.len());
    let wanted: Vec<String> = checks[start..end]
        .iter()
        .map(|check| check.check_id.to_string())
        .collect();
    filter_chart_entries(response, &wanted);
}

fn filter_chart_entries(response: &mut serde_json::Value, wanted: &[String]) {
    if let Some(entries) = response
        .get_mut("checks")
        .and_then(|value| value.as_array_mut())
    {
        entries.retain(|entry| {
            entry
                .get("check_id")
                .and_then(|id| id.as_str())
                .is_some_and(|id| wanted.iter().any(|wanted| wanted == id))
        });
        entries.sort_by_key(|entry| {
            entry
                .get("check_id")
                .and_then(|id| id.as_str())
                .and_then(|id| wanted.iter().position(|wanted| wanted == id))
                .unwrap_or(usize::MAX)
        });
    }
}

fn query_usize(query: Option<&str>, name: &str) -> Option<usize> {
    query_value(query, name)?.parse().ok()
}

fn query_value(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| percent_decode_str(value).decode_utf8_lossy().into_owned())
    })
}

async fn fetch_region_json(
    node: &GlobalNode,
    method: &Method,
    pq: &str,
    body: &Bytes,
    endpoints: &[String],
    authed: bool,
) -> Result<serde_json::Value, String> {
    let mut last_error = String::from("no reachable coordinator");
    for base in endpoints {
        match region_request(node, method, base, pq, body, authed)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => match response.json().await {
                Ok(value) => return Ok(value),
                Err(error) => last_error = format!("invalid JSON: {error}"),
            },
            // A coordinator answered authoritatively. Retrying a peer could
            // mask authorization/client failures or return inconsistent data.
            Ok(response) => return Err(format!("HTTP {}", response.status())),
            Err(error) => last_error = error.to_string(),
        }
    }
    Err(last_error)
}

fn merge_responses(
    path: &str,
    query: Option<&str>,
    responses: Vec<(String, serde_json::Value)>,
) -> Result<Json<serde_json::Value>, String> {
    let first = responses
        .first()
        .map(|(_, value)| value)
        .cloned()
        .unwrap_or_default();
    // Before the suffix rules: `/api/v1/uptime` also ends with `/uptime`, and
    // the batched shape is a list of the single-check one, not the thing itself.
    if path == "/api/v1/uptime" || path == "/api/v1/latency-points" {
        return merge_check_series(path, responses).map(Json);
    }
    if path.ends_with("/uptime") {
        return merge_uptime(&first, &responses).map(Json);
    }
    if path.ends_with("/latency-sparkline") {
        return merge_latency(&first, &responses).map(Json);
    }

    let key = if path.ends_with("/history") {
        "history"
    } else if path.ends_with("/results") {
        "results"
    } else if path.ends_with("/alerts") {
        "alerts"
    } else {
        "incidents"
    };
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
    for (region, value) in responses {
        for mut item in value
            .get(key)
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let dedup_key = merged_item_key(key, &region, &item);
            if !seen.insert(dedup_key) {
                continue;
            }
            rewrite_merged_identity(key, &region, &mut item);
            items.push(item);
        }
    }
    items.sort_by_key(|item| std::cmp::Reverse(item_timestamp(item)));
    if let Some(limit) = query_limit(query) {
        items.truncate(limit);
    }
    Ok(Json(serde_json::json!({ key: items })))
}

fn merged_item_key(key: &str, region: &str, item: &serde_json::Value) -> String {
    let id = item
        .get("id")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    if key == "alerts" || key == "incidents" {
        return format!("{region}:{id}");
    }
    format!(
        "{}:{}:{}",
        item.get("check_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default(),
        item.get("agent_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default(),
        item_timestamp(item)
    )
}

fn rewrite_merged_identity(key: &str, region: &str, item: &mut serde_json::Value) {
    if key == "incidents" {
        rewrite_incident_value(region, item);
    }
    if key == "alerts" {
        if let Some(id) = item.get("id").and_then(|value| value.as_str()) {
            item["id"] = serde_json::Value::String(composite_incident_id(region, id));
        }
        if let Some(id) = item.get("incident_id").and_then(|value| value.as_str()) {
            item["incident_id"] = serde_json::Value::String(composite_incident_id(region, id));
        }
    }
}

fn item_timestamp(item: &serde_json::Value) -> i64 {
    item.get("timestamp")
        .or_else(|| item.get("started_at"))
        .and_then(|value| value.as_i64())
        .unwrap_or_default()
}

fn query_limit(query: Option<&str>) -> Option<usize> {
    query?.split('&').find_map(|pair| {
        pair.split_once('=')
            .and_then(|(key, value)| (key == "limit").then(|| value.parse().ok()).flatten())
    })
}

/// Merge a batched series — one response per region, each carrying every check
/// that region knows — by grouping on `check_id` and reusing the single-check
/// merges. A check observed in two regions has its buckets summed exactly as
/// the drill-down endpoints already sum them; a check observed in one is
/// carried through unchanged.
fn merge_check_series(
    path: &str,
    responses: Vec<(String, serde_json::Value)>,
) -> Result<serde_json::Value, String> {
    let latency = path.ends_with("latency-points");
    let mut order: Vec<String> = Vec::new();
    let mut by_check: HashMap<String, Vec<(String, serde_json::Value)>> = HashMap::new();
    for (region, value) in &responses {
        for entry in value["checks"].as_array().into_iter().flatten() {
            let Some(id) = entry.get("check_id").and_then(|value| value.as_str()) else {
                continue;
            };
            by_check
                .entry(id.to_string())
                .or_insert_with(|| {
                    order.push(id.to_string());
                    Vec::new()
                })
                .push((region.clone(), entry.clone()));
        }
    }

    let mut checks = Vec::with_capacity(order.len());
    for id in order {
        let entries = by_check.remove(&id).unwrap_or_default();
        let first = entries
            .first()
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        let mut merged = if latency {
            let mut merged = merge_latency(&first, &entries)?;
            // The single-check endpoint answers with a per-agent breakdown, an
            // overall band and its own window; the batched shape carries none
            // of them, and the page would parse them once per check on it.
            if let Some(object) = merged.as_object_mut() {
                object.remove("overall");
                object.remove("agents");
                object.remove("from");
                object.remove("to");
                object.remove("buckets");
            }
            merged
        } else {
            merge_uptime(&first, &entries)?
        };
        // Display name is config, identical in every region, and not something
        // either single-check merge carries.
        if let Some(name) = first.get("name") {
            merged["name"] = name.clone();
        }
        checks.push(merged);
    }

    let first = responses
        .first()
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    let mut out = serde_json::json!({ "checks": checks });
    for key in ["from", "to", "days", "buckets"] {
        if let Some(value) = first.get(key) {
            out[key] = value.clone();
        }
    }
    Ok(out)
}

fn merge_uptime(
    first: &serde_json::Value,
    responses: &[(String, serde_json::Value)],
) -> Result<serde_json::Value, String> {
    let grids = bucket_grids(responses, "buckets", |bucket| {
        Some((bucket.get("start")?.as_i64()?, bucket.get("end")?.as_i64()?))
    })?;
    let mut totals = vec![(0_u64, 0_u64, 0.0_f64, 0_u64); grids.first().map_or(0, Vec::len)];
    for (_, value) in responses {
        for (index, bucket) in value["buckets"].as_array().unwrap().iter().enumerate() {
            totals[index].0 += bucket["up"].as_u64().unwrap_or(0);
            totals[index].1 += bucket["total"].as_u64().unwrap_or(0);
            if let Some(uptime) = bucket["uptime"].as_f64() {
                totals[index].2 += uptime;
                totals[index].3 += 1;
            }
        }
    }
    let buckets = first["buckets"].as_array().unwrap().iter().zip(totals).map(|(bucket, (up, total, uptime_sum, uptime_regions))| {
        serde_json::json!({"start": bucket["start"], "end": bucket["end"], "up": up, "total": total, "uptime": (uptime_regions > 0).then(|| uptime_sum / uptime_regions as f64)})
    }).collect::<Vec<_>>();
    let overall = responses
        .iter()
        .filter_map(|(_, value)| value["overall_uptime"].as_f64())
        .fold((0.0_f64, 0_u64), |(sum, count), uptime| {
            (sum + uptime, count + 1)
        });
    Ok(serde_json::json!({
        "check_id": first["check_id"],
        "days": first["days"],
        "overall_uptime": (overall.1 > 0).then(|| overall.0 / overall.1 as f64),
        "buckets": buckets
    }))
}

fn bucket_grids<F>(
    responses: &[(String, serde_json::Value)],
    key: &str,
    mut bounds: F,
) -> Result<Vec<Vec<(i64, i64)>>, String>
where
    F: FnMut(&serde_json::Value) -> Option<(i64, i64)>,
{
    let mut grids = Vec::with_capacity(responses.len());
    let mut expected_len = None;
    for (region, value) in responses {
        let buckets = value[key]
            .as_array()
            .ok_or_else(|| format!("region '{region}' returned no {key} array"))?;
        if let Some(length) = expected_len {
            if buckets.len() != length {
                return Err(format!(
                    "incompatible {key} grids: region '{region}' has {} buckets, expected {length}",
                    buckets.len()
                ));
            }
        } else {
            expected_len = Some(buckets.len());
        }
        let grid = buckets.iter().enumerate().map(|(index, bucket)| {
            bounds(bucket).ok_or_else(|| format!("incompatible {key} grid: region '{region}' bucket {index} has invalid bounds"))
        }).collect::<Result<Vec<_>, _>>()?;
        if grid.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(format!(
                "incompatible {key} grid: region '{region}' buckets are not ordered"
            ));
        }
        grids.push(grid);
    }
    Ok(grids)
}

fn merge_latency(
    first: &serde_json::Value,
    responses: &[(String, serde_json::Value)],
) -> Result<serde_json::Value, String> {
    let grids = bucket_grids(responses, "points", |point| {
        point
            .get("timestamp")?
            .as_i64()
            .map(|timestamp| (timestamp, timestamp))
    })?;
    let mut points = vec![(0_u64, 0_u64, 0.0); grids.first().map_or(0, Vec::len)];
    let mut overall = (0_u64, 0_u64, None, 0.0, 0_u64);
    let mut agents: HashMap<String, (u64, u64, Option<u64>, f64, u64)> = HashMap::new();
    for (_, value) in responses {
        for (index, point) in value["points"].as_array().unwrap().iter().enumerate() {
            let up = point["up"].as_u64().unwrap_or(0);
            points[index].0 += point["attempts"].as_u64().unwrap_or(0);
            points[index].1 += up;
            points[index].2 += point["avg_latency_ms"].as_f64().unwrap_or(0.0) * up as f64;
        }
        merge_latency_band(&mut overall, value.get("overall"));
        for agent in value["agents"].as_array().into_iter().flatten() {
            let Some(id) = agent.get("agent_id").and_then(|value| value.as_str()) else {
                continue;
            };
            merge_latency_band(
                agents.entry(id.to_string()).or_insert((0, 0, None, 0.0, 0)),
                Some(agent),
            );
        }
    }
    let points = first["points"].as_array().unwrap().iter().zip(points).map(|(point, (attempts, up, sum))| {
        serde_json::json!({"timestamp": point["timestamp"], "attempts": attempts, "up": up, "avg_latency_ms": (up > 0).then(|| sum / up as f64)})
    }).collect::<Vec<_>>();
    let agents = agents
        .into_iter()
        .map(|(agent_id, band)| {
            let mut value = latency_band_json(band);
            value["agent_id"] = serde_json::Value::String(agent_id);
            value
        })
        .collect::<Vec<_>>();
    Ok(
        serde_json::json!({"check_id": first["check_id"], "from": first["from"], "to": first["to"], "buckets": points.len(), "points": points, "overall": latency_band_json(overall), "agents": agents}),
    )
}

fn merge_latency_band(
    target: &mut (u64, u64, Option<u64>, f64, u64),
    band: Option<&serde_json::Value>,
) {
    let Some(band) = band else { return };
    let attempts = band
        .get("attempts")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let up = band.get("up").and_then(|value| value.as_u64()).unwrap_or(0);
    target.0 += attempts;
    target.1 += up;
    if let Some(min) = band.get("min_ms").and_then(|value| value.as_u64()) {
        target.2 = Some(target.2.map_or(min, |current| current.min(min)));
    }
    if let Some(avg) = band.get("avg_ms").and_then(|value| value.as_f64()) {
        target.3 += avg * up as f64;
    }
    if let Some(max) = band.get("max_ms").and_then(|value| value.as_u64()) {
        target.4 = target.4.max(max);
    }
}

fn latency_band_json(
    (attempts, up, min, sum, max): (u64, u64, Option<u64>, f64, u64),
) -> serde_json::Value {
    serde_json::json!({"attempts": attempts, "up": up, "min_ms": min, "avg_ms": (up > 0).then(|| sum / up as f64), "max_ms": (up > 0).then_some(max)})
}

async fn relay_incident_detail(
    node: &GlobalNode,
    method: &Method,
    pq: &str,
    body: &Bytes,
    endpoints: &[String],
    authed: bool,
    region: &str,
) -> Response {
    let response = relay_single_path(node, method, pq, body, endpoints, authed).await;
    let status = response.status();
    if !status.is_success() {
        return response;
    }
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(mut value) => {
            if let Some(incident) = value.get_mut("incident") {
                rewrite_incident_value(region, incident);
            }
            Json(value).into_response()
        }
        Err(_) => (status, bytes).into_response(),
    }
}

fn rewrite_incident_value(region: &str, incident: &mut serde_json::Value) {
    if let Some(id) = incident.get("id").and_then(|value| value.as_str()) {
        incident["id"] = serde_json::Value::String(composite_incident_id(region, id));
    }
    if let Some(origin_check) = incident
        .get("origin_check")
        .and_then(|value| value.as_str())
    {
        incident["origin_check"] = serde_json::Value::String(origin_check.to_string());
    }
    if let Some(updates) = incident
        .get_mut("updates")
        .and_then(|value| value.as_array_mut())
    {
        for update in updates {
            if let Some(id) = update.get("id").and_then(|value| value.as_str()) {
                update["id"] = serde_json::Value::String(composite_incident_id(region, id));
            }
        }
    }
}

async fn relay_single(
    node: &GlobalNode,
    method: &Method,
    uri: &Uri,
    body: &Bytes,
    endpoints: &[String],
    authed: bool,
) -> Response {
    let pq = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path());
    if endpoints.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to serve this query yet",
        );
    }
    for base in endpoints {
        match region_request(node, method, base, pq, body, authed)
            .send()
            .await
        {
            Ok(resp) => return passthrough(resp).await,
            Err(e) => warn!(base = %base, error = %e, "region relay failed, trying next"),
        }
    }
    error_response(
        StatusCode::BAD_GATEWAY,
        "no region coordinator answered the query",
    )
}

/// Fan an admin write out to one live coordinator in every region, for
/// everything the global tier is *not* authoritative for (`global::admin`
/// intercepts checks and sections before this is ever reached — see its
/// module doc). Regions run identical config, so the write must land
/// everywhere or the fleet diverges. The first region's response is returned
/// to the operator; if any region errors (transport or a non-2xx), that
/// failure is surfaced instead so a partial apply is never reported as
/// success.
///
/// This is the old, all-or-nothing mechanism SCALING.md's "Placement-scoped
/// configuration" describes the gap in: a region that is merely stale drops
/// out of `region_endpoints` and misses the write silently, and nothing
/// reconciles it afterwards. Phase 1 closes that gap for the catalogue by
/// routing it through `store::GlobalStore` and `reconcile` instead; widening
/// the fix to notifiers, credentials, rollup policies, maintenance and
/// incidents is Phase 2/3 work (see `store`'s module doc for why each of
/// those is not simply added to the authoritative list today).
async fn fan_out_write(
    node: &GlobalNode,
    method: &Method,
    uri: &Uri,
    body: &Bytes,
    now: i64,
) -> Response {
    let pq = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path());
    let regions = node.inner.cache.region_endpoints(now);
    if regions.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to apply this change yet",
        );
    }
    let mut first_ok: Option<Response> = None;
    for (region, endpoints) in &regions {
        match apply_to_region(node, method, endpoints, pq, body).await {
            Ok(resp) => {
                if first_ok.is_none() {
                    first_ok = Some(resp);
                }
            }
            Err(msg) => {
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("change failed in region '{region}': {msg}"),
                );
            }
        }
    }
    first_ok.unwrap_or_else(|| {
        error_response(
            StatusCode::BAD_GATEWAY,
            "no region coordinator applied the change",
        )
    })
}

/// Apply a write to a region by trying its coordinators until one answers.
/// A non-2xx from a reachable coordinator is a real rejection (validation, a
/// duplicate name) — return it as the region's error rather than trying the
/// next coordinator, which would only apply the same write twice.
///
/// A `DELETE` that 404s is the exception: delete is idempotent, so a region
/// that is already missing the target has the state the operator asked for.
/// Treating that as failure would make a single drifted region wedge every
/// delete, which is exactly the drift fan-out exists to heal.
async fn apply_to_region(
    node: &GlobalNode,
    method: &Method,
    endpoints: &[String],
    pq: &str,
    body: &Bytes,
) -> Result<Response, String> {
    let mut last_transport_err = String::from("no reachable coordinator");
    for base in endpoints {
        match region_request(node, method, base, pq, body, true)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success()
                    || (method == Method::DELETE && status == StatusCode::NOT_FOUND)
                {
                    return Ok(passthrough(resp).await);
                }
                let text = resp.text().await.unwrap_or_default();
                return Err(format!("HTTP {} {}", status.as_u16(), text));
            }
            Err(e) => last_transport_err = e.to_string(),
        }
    }
    Err(last_transport_err)
}

/// Turn a region's `reqwest::Response` into our `Response`, preserving status
/// and content type so the SPA sees exactly what the coordinator returned.
async fn passthrough(resp: reqwest::Response) -> Response {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let body = resp.bytes().await.unwrap_or_default();
    (status, [(header::CONTENT_TYPE, ct)], body).into_response()
}

// ---- Embedded SPA (shared build under frontend/dist) ------------------------

#[derive(rust_embed::RustEmbed)]
#[folder = "frontend/dist"]
struct Assets;

fn static_asset(uri: &Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let lookup = if path.is_empty() { "index.html" } else { path };
    if let Some(content) = Assets::get(lookup) {
        let mime = mime_guess::from_path(lookup).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response();
    }
    match Assets::get("index.html") {
        Some(content) => Html(content.data).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "dashboard not built — run `bun run build` in frontend/",
        )
            .into_response(),
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn error_response(code: StatusCode, message: &str) -> Response {
    (
        code,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The status page asks one question for the whole estate, but a check is
    /// only observed where its agents are. Relaying that question to a single
    /// region answers "no data" for every check that region does not probe,
    /// which the page renders as "Collecting latency data…" — so the batched
    /// series must be asked of every region and merged per check.
    #[test]
    fn batched_latency_points_merge_every_region_per_check() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({
                    "from": 0, "to": 100, "buckets": 2,
                    "checks": [
                        {"check_id": "probed-in-us", "name": "web", "points": [
                            {"timestamp": 0, "attempts": 4, "up": 4, "avg_latency_ms": 100.0},
                            {"timestamp": 50, "attempts": 4, "up": 2, "avg_latency_ms": 200.0}
                        ]},
                        {"check_id": "probed-in-both", "points": [
                            {"timestamp": 0, "attempts": 2, "up": 2, "avg_latency_ms": 10.0},
                            {"timestamp": 50, "attempts": 2, "up": 2, "avg_latency_ms": 10.0}
                        ]}
                    ]
                }),
            ),
            (
                "tw".into(),
                serde_json::json!({
                    "from": 0, "to": 100, "buckets": 2,
                    "checks": [
                        {"check_id": "probed-in-us", "name": "web", "points": [
                            {"timestamp": 0, "attempts": 0, "up": 0, "avg_latency_ms": null},
                            {"timestamp": 50, "attempts": 0, "up": 0, "avg_latency_ms": null}
                        ]},
                        {"check_id": "probed-in-both", "points": [
                            {"timestamp": 0, "attempts": 2, "up": 2, "avg_latency_ms": 30.0},
                            {"timestamp": 50, "attempts": 2, "up": 0, "avg_latency_ms": null}
                        ]}
                    ]
                }),
            ),
        ];
        let Json(merged) = merge_responses("/api/v1/latency-points", None, responses).unwrap();
        let checks = merged["checks"].as_array().unwrap();
        assert_eq!(checks.len(), 2, "every check, once");

        let us = &checks[0];
        assert_eq!(us["check_id"], "probed-in-us");
        assert_eq!(us["name"], "web", "display name survives the merge");
        assert_eq!(
            us["points"][0]["attempts"], 4,
            "a region that does not probe the check must not erase the one that does"
        );

        let both = &checks[1];
        assert_eq!(both["points"][0]["attempts"], 4, "2 + 2 attempts");
        assert_eq!(
            both["points"][0]["avg_latency_ms"], 20.0,
            "latency is weighted by successful attempts, not averaged twice"
        );
        assert_eq!(both["points"][1]["attempts"], 4);
        assert_eq!(
            both["points"][1]["avg_latency_ms"], 10.0,
            "a region with no successes contributes none of the mean"
        );
    }

    /// `/api/v1/uptime` also ends with `/uptime`, so the batched shape must be
    /// recognised before the single-check rule that would read its fields.
    #[test]
    fn batched_uptime_merges_per_check_rather_than_as_one_check() {
        let bucket = |up: u64, total: u64| {
            serde_json::json!({"start": 0, "end": 10, "up": up, "total": total,
                               "uptime": up as f64 / total as f64})
        };
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"days": 1, "checks": [
                    {"check_id": "c1", "name": "web", "days": 1, "overall_uptime": 1.0,
                     "buckets": [bucket(10, 10)]}
                ]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"days": 1, "checks": [
                    {"check_id": "c1", "name": "web", "days": 1, "overall_uptime": 0.5,
                     "buckets": [bucket(5, 10)]}
                ]}),
            ),
        ];
        let Json(merged) = merge_responses("/api/v1/uptime", None, responses).unwrap();
        let checks = merged["checks"].as_array().unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0]["buckets"][0]["up"], 15);
        assert_eq!(checks[0]["buckets"][0]["total"], 20);
        assert_eq!(checks[0]["name"], "web");
        assert_eq!(merged["days"], 1);
    }

    #[test]
    fn chart_page_filter_keeps_the_global_page_after_multi_region_merge() {
        // More than one regional page: ids 100..199 would never be returned if
        // each region were sent page=1 before the global merge. The selected
        // page is instead applied once to the merged global ids.
        let wanted: Vec<String> = (100..200).map(|id| format!("check-{id:03}")).collect();
        let mut merged = serde_json::json!({"checks": (0..202).rev().map(|id| {
            serde_json::json!({"check_id": format!("check-{id:03}")})
        }).collect::<Vec<_>>()});
        filter_chart_entries(&mut merged, &wanted);
        let checks = merged["checks"].as_array().unwrap();
        assert_eq!(checks.len(), 100);
        assert_eq!(checks.first().unwrap()["check_id"], "check-100");
        assert_eq!(checks.last().unwrap()["check_id"], "check-199");
    }

    #[test]
    fn chart_relays_do_not_forward_regional_page_selectors() {
        let uri: Uri = "/api/v1/uptime?days=30&page=1&limit=100&section=edge&buckets=90"
            .parse()
            .unwrap();
        assert_eq!(
            chart_relay_path(&uri),
            "/api/v1/uptime?days=30&buckets=90&limit=1000"
        );
    }

    #[test]
    fn merged_lists_deduplicate_and_apply_the_requested_limit() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"alerts": [{"id": "a", "timestamp": 10}, {"id": "b", "timestamp": 30}]}),
            ),
            (
                "us".into(),
                serde_json::json!({"alerts": [{"id": "a", "timestamp": 10}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"alerts": [{"id": "a", "timestamp": 20}]}),
            ),
        ];
        let Json(merged) = merge_responses("/api/v1/alerts", Some("limit=2"), responses).unwrap();
        let alerts = merged["alerts"].as_array().unwrap();
        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[0]["id"], "us:b");
        assert_eq!(alerts[1]["id"], "tw:a");
    }

    #[test]
    fn latency_merge_weights_bands_and_preserves_agents() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "from": 0, "to": 10, "points": [{"timestamp": 0, "attempts": 2, "up": 1, "avg_latency_ms": 100.0}], "overall": {"attempts": 2, "up": 1, "min_ms": 100, "avg_ms": 100.0, "max_ms": 100}, "agents": [{"agent_id": "a", "attempts": 2, "up": 1, "min_ms": 100, "avg_ms": 100.0, "max_ms": 100}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "from": 0, "to": 10, "points": [{"timestamp": 0, "attempts": 3, "up": 2, "avg_latency_ms": 200.0}], "overall": {"attempts": 3, "up": 2, "min_ms": 150, "avg_ms": 200.0, "max_ms": 250}, "agents": [{"agent_id": "b", "attempts": 3, "up": 2, "min_ms": 150, "avg_ms": 200.0, "max_ms": 250}]}),
            ),
        ];
        let merged = merge_latency(&responses[0].1, &responses).unwrap();
        assert_eq!(merged["points"][0]["avg_latency_ms"], 166.66666666666666);
        assert_eq!(merged["overall"]["avg_ms"], 166.66666666666666);
        let agents = merged["agents"].as_array().unwrap();
        assert_eq!(agents.len(), 2);
        assert!(agents.iter().all(|agent| agent.get("band").is_none()));
        assert!(agents.iter().all(|agent| agent["attempts"].is_number()));
        assert!(agents.iter().all(|agent| agent["avg_ms"].is_number()));
    }

    #[test]
    fn encoded_path_ids_are_decoded_for_routing_and_reencoded_for_relay() {
        // Checks are addressed by UUID now, not by a percent-encoded name, so
        // the routing path only needs to parse the id back out.
        let id = CheckId::from_u128(0x0d0e_ad10);
        assert_eq!(
            extract_check_id(&format!("api/v1/checks/{id}/history")),
            Some(id)
        );
        let id = extract_incident_id("api/v1/admin/incidents/us%3Aupstream-1").unwrap();
        assert_eq!(
            parse_composite_incident_id(&id),
            Some(("us".into(), "upstream-1".into()))
        );
        let uri = "/api/v1/admin/incidents/us%3Aupstream-1?x=1"
            .parse()
            .unwrap();
        assert_eq!(
            rewrite_incident_path(&uri, "upstream-1"),
            "/api/v1/admin/incidents/upstream-1?x=1"
        );
    }

    #[test]
    fn bucket_merges_use_first_region_grid_despite_millisecond_skew() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "days": 1, "buckets": [{"start": 1_000, "end": 2_000, "up": 2, "total": 2}, {"start": 2_000, "end": 3_000, "up": 1, "total": 2}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "days": 1, "buckets": [{"start": 1_003, "end": 2_003, "up": 3, "total": 3}, {"start": 2_003, "end": 3_003, "up": 2, "total": 2}]}),
            ),
        ];
        let merged = merge_uptime(&responses[0].1, &responses).unwrap();
        assert_eq!(merged["buckets"][0]["start"], 1_000);
        assert_eq!(merged["buckets"][0]["end"], 2_000);
        assert_eq!(merged["buckets"][0]["up"], 5);
        assert_eq!(merged["buckets"][0]["total"], 5);

        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "from": 10, "to": 20, "points": [{"timestamp": 10_000, "attempts": 2, "up": 2, "avg_latency_ms": 100.0}], "overall": {}}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "from": 13, "to": 23, "points": [{"timestamp": 10_003, "attempts": 3, "up": 3, "avg_latency_ms": 200.0}], "overall": {}}),
            ),
        ];
        let merged = merge_latency(&responses[0].1, &responses).unwrap();
        assert_eq!(merged["from"], 10);
        assert_eq!(merged["to"], 20);
        assert_eq!(merged["points"][0]["timestamp"], 10_000);
        assert_eq!(merged["points"][0]["attempts"], 5);
    }

    #[test]
    fn uptime_merge_keeps_time_weighted_state_for_empty_attempt_buckets() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "days": 1, "overall_uptime": 1.0, "buckets": [{"start": 1, "end": 2, "up": 0, "total": 0, "uptime": 1.0}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "days": 1, "overall_uptime": 0.5, "buckets": [{"start": 1, "end": 2, "up": 0, "total": 0, "uptime": 0.5}]}),
            ),
        ];

        let merged = merge_uptime(&responses[0].1, &responses).unwrap();

        assert_eq!(merged["buckets"][0]["total"], 0);
        assert_eq!(merged["buckets"][0]["uptime"], 0.75);
        assert_eq!(merged["overall_uptime"], 0.75);
    }

    #[test]
    fn incompatible_bucket_grids_return_clear_errors() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "days": 1, "buckets": [{"start": 1, "end": 2, "up": 1, "total": 1}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "days": 1, "buckets": []}),
            ),
        ];
        assert_eq!(
            merge_uptime(&responses[0].1, &responses).unwrap_err(),
            "incompatible buckets grids: region 'tw' has 0 buckets, expected 1"
        );
    }

    #[test]
    fn incident_detail_rewrites_incident_and_update_ids() {
        let mut incident = serde_json::json!({"id": "i1", "updates": [{"id": "u1"}]});
        rewrite_incident_value("us", &mut incident);
        assert_eq!(incident["id"], "us:i1");
        assert_eq!(incident["updates"][0]["id"], "us:u1");
    }

    // ---- Placement-scoped configuration, Phase 1 & 4 (SCALING.md) ----

    use crate::config::{CoordinatorConfig, GlobalPushConfig, GlobalRegion};
    use crate::coordinator::Coordinator;

    async fn serve_router(router: Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        addr
    }

    fn admin_cookie(node: &GlobalNode, password: &str) -> String {
        let token = node
            .inner
            .auth
            .login(
                password,
                "127.0.0.1".parse().unwrap(),
                chrono::Utc::now().timestamp_millis(),
            )
            .unwrap();
        format!("{}={}", crate::auth::GLOBAL_SESSION_COOKIE, token)
    }

    /// Phase 1's central claim: a catalogue write commits to the global store
    /// as one local operation, so it is unaffected by a region being down —
    /// no 502, no partial fan-out — and the down region converges on its own
    /// once it is reachable again, with no repeat of the write from the
    /// operator.
    #[tokio::test]
    async fn admin_write_survives_an_unreachable_region_and_converges_once_it_returns() {
        let global_token = "global-token";
        let admin_password = "secret";
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let coord_addr = serve_router(coord.clone().router()).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            admin_password: Some(admin_password.into()),
            db_path: ":memory:".into(),
            regions: vec![GlobalRegion {
                name: "us".into(),
                // Port 1 is a reserved, never-listening port: an admin write
                // must not depend on this ever answering.
                endpoints: vec!["http://127.0.0.1:1".into()],
            }],
            ..Default::default()
        })
        .await
        .unwrap();
        let cookie = admin_cookie(&node, admin_password);
        let global_addr = serve_router(node.clone().router()).await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("http://{global_addr}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &cookie)
            .json(&serde_json::json!({"name": "web", "type": "http", "target": "https://example.com"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "the write itself must not fail"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        let check_name = body["check"]["name"].as_str().unwrap().to_string();
        assert_eq!(check_name, "web");

        // The catalogue is authoritative on the global store regardless of
        // whether any region ever answers.
        let (checks, total, _) = node.inner.store.list_checks(0, 10).await.unwrap();
        assert_eq!(total, 1);
        assert_eq!(checks[0].name, "web");

        // The region has not seen it yet — its only endpoint never answers.
        assert!(coord.list_checks_for_test().await.is_empty());

        // The region "returns": it pushes a fresh state summary naming its
        // real, reachable address, exactly as `Coordinator::spawn_global_push`
        // would on its own timer.
        let push = wire::RegionStatePush {
            region: "us".into(),
            origin: "coordinator-us".into(),
            advertise_url: Some(format!("http://{coord_addr}")),
            pushed_at: chrono::Utc::now().timestamp_millis(),
            region_health: crate::models::NodeState::Up,
            agents_healthy: 1,
            agents_total: 1,
            checks: Vec::new(),
        };
        client
            .post(format!("http://{global_addr}/internal/global/state"))
            .bearer_auth(global_token)
            .json(&push)
            .send()
            .await
            .unwrap();

        // Drive one reconciliation pass directly rather than waiting on
        // `reconcile::RECONCILE_INTERVAL`.
        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }

        let checks = coord.list_checks_for_test().await;
        assert_eq!(
            checks.len(),
            1,
            "the region converges without a repeated write"
        );
        assert_eq!(checks[0].name, "web");
    }

    /// Phase 4: two global nodes reconcile the catalogue between themselves —
    /// a write applied only to one converges onto the other through the same
    /// digest-and-backfill anti-entropy a coordinator's peers already use.
    #[tokio::test]
    async fn two_global_nodes_converge_through_anti_entropy() {
        let global_token = "global-token";
        let peer_token = "global-peer-token";

        let node_a = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            node_id: Some("global-a".into()),
            peer_token: Some(peer_token.into()),
            advertise_url: Some("http://global-a.invalid".into()),
            ..Default::default()
        })
        .await
        .unwrap();
        let addr_a = serve_router(node_a.clone().router()).await;

        // A write that only ever reaches node A — node B never hears it any
        // other way in this test, standing in for "B was down when A wrote".
        node_a
            .inner
            .store
            .create_check(
                serde_json::json!({"name": "web", "type": "http", "target": "https://example.com"}),
            )
            .await
            .ok()
            .unwrap();

        let node_b = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            node_id: Some("global-b".into()),
            peer_token: Some(peer_token.into()),
            advertise_url: Some("http://global-b.invalid".into()),
            peers: vec![format!("http://{addr_a}")],
            ..Default::default()
        })
        .await
        .unwrap();

        assert!(node_b
            .inner
            .store
            .list_checks(0, 10)
            .await
            .unwrap()
            .0
            .is_empty());

        cluster::reconcile_with_peer(&node_b, &format!("http://{addr_a}")).await;

        let (checks, total, _) = node_b.inner.store.list_checks(0, 10).await.unwrap();
        assert_eq!(total, 1);
        assert_eq!(checks[0].name, "web");
    }

    /// Phase 1: the catalogue is durable, not merely cached — a restart with
    /// the same `db_path` reopens the same checks rather than starting empty.
    #[tokio::test]
    async fn global_node_restart_preserves_the_catalogue() {
        let dir = std::env::temp_dir().join(format!(
            "warden-global-restart-test-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("global.duckdb").to_string_lossy().to_string();
        let cfg = GlobalConfig {
            token: Some("global-token".into()),
            db_path: db_path.clone(),
            ..Default::default()
        };

        let node = GlobalNode::new(&cfg).await.unwrap();
        node.inner
            .store
            .create_check(
                serde_json::json!({"name": "web", "type": "http", "target": "https://example.com"}),
            )
            .await
            .ok()
            .unwrap();
        drop(node);

        let reopened = GlobalNode::new(&cfg).await.unwrap();
        let (checks, total, _) = reopened.inner.store.list_checks(0, 10).await.unwrap();
        assert_eq!(total, 1, "the catalogue survives a restart");
        assert_eq!(checks[0].name, "web");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The data plane stays regional: a coordinator keeps serving status with
    /// no global node reachable at all — not merely tolerant of a slow one,
    /// but functionally independent of it. `spawn_global_push` is started to
    /// show its failure against an address nothing listens on costs nothing
    /// beyond a logged warning.
    #[tokio::test]
    async fn regions_keep_serving_status_with_the_global_tier_entirely_down() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://127.0.0.1:1".into(),
                token: Some("global-token".into()),
                token_env: None,
                push_secs: 1,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        coord.spawn_global_push();
        coord
            .create_check_for_test(crate::config::CheckConfig {
                name: "web".into(),
                kind: crate::models::CheckKind::Http,
                target: "https://example.com".into(),
                ..Default::default()
            })
            .await;
        let coord_addr = serve_router(coord.router()).await;

        let resp = reqwest::get(format!("http://{coord_addr}/api/v1/status"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["checks"][0]["name"], "web");
    }

    // ---- Placement-scoped configuration, Phase 2 & 3 (SCALING.md) ----

    /// SCALING.md's concrete disclosure case: `unifi`/`upstream`, placed to
    /// `["us"]` only, still had their full definitions — one with a push
    /// credential in its target — sitting in `coordinator-tw`'s database.
    /// This is the test that Phase 2 reconciliation actually closes that
    /// gap: a check whose placement admits only `us` must never reach
    /// `tw`'s store at all.
    #[tokio::test]
    async fn a_check_placed_to_one_region_never_reaches_another() {
        let global_token = "global-token";
        let admin_password = "secret";
        let us = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let us_addr = serve_router(us.clone().router()).await;

        let tw = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "tw".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let tw_addr = serve_router(tw.clone().router()).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            admin_password: Some(admin_password.into()),
            db_path: ":memory:".into(),
            regions: vec![
                GlobalRegion {
                    name: "us".into(),
                    endpoints: vec![format!("http://{us_addr}")],
                },
                GlobalRegion {
                    name: "tw".into(),
                    endpoints: vec![format!("http://{tw_addr}")],
                },
            ],
            ..Default::default()
        })
        .await
        .unwrap();
        let cookie = admin_cookie(&node, admin_password);
        let global_addr = serve_router(node.clone().router()).await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("http://{global_addr}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &cookie)
            .json(&serde_json::json!({
                "name": "unifi",
                "type": "http",
                // A referenced secret, per Phase 0 ("Secrets are referenced,
                // not carried") — a literal credential in a query value is
                // rejected outright, which is a separate control from this
                // test's placement-scoping claim.
                "target": "https://192.168.1.1/push?token=${PUSH_TOKEN}",
                "placement": {"regions": ["us"]},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Drive reconciliation for both regions directly rather than waiting
        // on `push_now`'s spawned task or `RECONCILE_INTERVAL`.
        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }

        let us_checks = us.list_checks_for_test().await;
        assert_eq!(us_checks.len(), 1, "the placed region receives it");
        assert_eq!(us_checks[0].name, "unifi");

        let tw_checks = tw.list_checks_for_test().await;
        assert!(
            tw_checks.is_empty(),
            "a region never placed must never hold the definition, secret included: {tw_checks:?}"
        );
    }

    /// Narrowing a check's placement away from a region must unplace it
    /// there — never delete it, which would write a `config_tombstone` that
    /// replicates and deletes the check in the region still probing it
    /// (SCALING.md "Unplace is disable-shaped, not delete-shaped"). Checked
    /// directly against the region's own digest: a tombstone would leave
    /// `config_digest`'s Check-kind count at 1 (the tombstone row), not 0.
    #[tokio::test]
    async fn unplace_writes_no_tombstone_and_the_holding_region_keeps_the_check() {
        let global_token = "global-token";
        let us = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let us_addr = serve_router(us.clone().router()).await;

        let tw = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "tw".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let tw_addr = serve_router(tw.clone().router()).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            regions: vec![
                GlobalRegion {
                    name: "us".into(),
                    endpoints: vec![format!("http://{us_addr}")],
                },
                GlobalRegion {
                    name: "tw".into(),
                    endpoints: vec![format!("http://{tw_addr}")],
                },
            ],
            ..Default::default()
        })
        .await
        .unwrap();

        // No placement named: every region admits it, so both get it.
        let (_, entry) = node
            .inner
            .store
            .create_check(serde_json::json!({
                "name": "dns", "type": "http", "target": "https://example.com",
            }))
            .await
            .ok()
            .unwrap();
        let check_id: crate::models::CheckId = entry.id.parse().unwrap();

        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }
        assert_eq!(us.list_checks_for_test().await.len(), 1, "us receives it");
        assert_eq!(
            tw.list_checks_for_test().await.len(),
            1,
            "tw receives it too"
        );

        // Narrow placement to `us` only.
        node.inner
            .store
            .update_check(
                check_id,
                serde_json::json!({
                    "name": "dns", "type": "http", "target": "https://example.com",
                    "placement": {"regions": ["us"]},
                }),
            )
            .await
            .ok()
            .unwrap();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }

        assert_eq!(
            us.list_checks_for_test().await.len(),
            1,
            "the still-placed region keeps it"
        );
        assert!(
            tw.list_checks_for_test().await.is_empty(),
            "tw's placement no longer admits it"
        );

        let tw_digest = tw.config_digest_for_test().await;
        let check_digest = tw_digest
            .iter()
            .find(|d| d.kind == crate::coordinator::storage::ConfigKind::Check)
            .unwrap();
        assert_eq!(
            check_digest.count, 0,
            "unplace must leave no tombstone behind — a tombstone here would \
             replicate and delete the check in us too"
        );
    }

    /// Once a region has converged, a further sweep must cost exactly one
    /// digest comparison — no full-table push, no id-set diff, no unplace
    /// call. A placement-scoped digest that still amplified would defeat the
    /// whole point of Phase 2, the same way a whole-catalogue digest against
    /// a subset region already did before this change.
    #[tokio::test]
    async fn converged_region_costs_no_further_paging_once_reconciled() {
        async fn log_requests(
            axum::extract::State(log): axum::extract::State<RequestLog>,
            req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> Response {
            log.lock()
                .unwrap()
                .push((req.method().to_string(), req.uri().path().to_string()));
            next.run(req).await
        }
        type RequestLog = std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>;

        let global_token = "global-token";
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let log: RequestLog = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let router = coord
            .clone()
            .router()
            .layer(axum::middleware::from_fn_with_state(
                log.clone(),
                log_requests,
            ));
        let coord_addr = serve_router(router).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            regions: vec![GlobalRegion {
                name: "us".into(),
                endpoints: vec![format!("http://{coord_addr}")],
            }],
            ..Default::default()
        })
        .await
        .unwrap();
        node.inner
            .store
            .create_check(serde_json::json!({
                "name": "web", "type": "http", "target": "https://example.com",
            }))
            .await
            .ok()
            .unwrap();

        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }
        assert_eq!(
            coord.list_checks_for_test().await.len(),
            1,
            "first sweep converges the region"
        );

        log.lock().unwrap().clear();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }
        let seen = log.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![(
                "GET".to_string(),
                "/internal/global/config-digest".to_string()
            )],
            "a converged region must cost exactly one digest read per sweep, not a full page"
        );
    }

    /// The data plane's independence claim, specifically for a check that
    /// arrived *through* placement-scoped reconciliation rather than a
    /// direct write: once placed, a region that never hears from global
    /// again keeps probing and serving status for it. Nothing regional ever
    /// unplaces a check on its own — that verb is only ever driven by an
    /// incoming call from the global tier — so losing contact with global
    /// must read as "no further instructions", never as "unplace
    /// everything".
    #[tokio::test]
    async fn a_region_cut_off_from_global_keeps_what_it_was_placed() {
        let global_token = "global-token";
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://127.0.0.1:1".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let coord_addr = serve_router(coord.clone().router()).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            regions: vec![GlobalRegion {
                name: "us".into(),
                endpoints: vec![format!("http://{coord_addr}")],
            }],
            ..Default::default()
        })
        .await
        .unwrap();
        node.inner
            .store
            .create_check(serde_json::json!({
                "name": "web", "type": "http", "target": "https://example.com",
            }))
            .await
            .ok()
            .unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }
        assert_eq!(
            coord.list_checks_for_test().await.len(),
            1,
            "reconciliation placed it"
        );

        // The global node vanishes entirely; no further sweep ever reaches
        // this region again.
        drop(node);

        let resp = reqwest::get(format!("http://{coord_addr}/api/v1/status"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["checks"][0]["name"], "web");
        assert_eq!(
            coord.list_checks_for_test().await.len(),
            1,
            "still placed with no global reachable at all"
        );
    }
}
