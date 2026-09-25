//! Router construction and HTTP handlers served directly by the global node
//! (as opposed to `relay`, which proxies to regions, and `merge`, which
//! folds their answers together).

use super::merge::{query_usize, query_value};
use super::wire::{PushAck, RegionStatePush};
use super::GlobalNode;
use super::{admin, cluster, relay};
use crate::models::{CheckId, NodeState};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post, put};
use axum::{body::Bytes, Json, Router};
use serde::Serialize;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tracing::warn;

pub(super) fn router(node: GlobalNode) -> Router {
    Router::new()
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
        .fallback(any(relay::relay_handler))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(axum::middleware::from_fn(crate::security::security_headers))
        .with_state(node)
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

async fn cluster_handler(
    State(node): State<GlobalNode>,
    _admin: crate::http::AdminSession,
) -> Response {
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

async fn region_tree_handler(
    State(node): State<GlobalNode>,
    _admin: crate::http::AdminSession,
) -> Response {
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
async fn agents_handler(
    State(node): State<GlobalNode>,
    _admin: crate::http::AdminSession,
) -> Response {
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
            let req = relay::region_request(
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
    relay::relay_merge(
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
    relay::relay_merge(
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

pub(super) fn is_admin(node: &GlobalNode, headers: &HeaderMap) -> bool {
    let now = chrono::Utc::now().timestamp_millis();
    session_token(headers).is_some_and(|t| node.inner.auth.validate(t, now))
}

pub(crate) use crate::http::error_response;
