//! The global coordinator (`warden global`), Stage B of the design.
//!
//! It holds **no raw data**. Regional coordinators push a state summary every
//! few seconds; the global node caches the latest per region ([`cache`]),
//! serves an aggregated dashboard over the same public API the coordinator
//! does — so the embedded SPA renders unchanged — and reverse-proxies any read
//! that needs raw history back to a coordinator in the owning region. A single
//! node today; the push/cache split is shaped so global-to-global replication
//! can be added later without changing the wire format (see [`wire`]).

pub mod alert;
pub mod cache;
pub mod wire;

use crate::config::GlobalConfig;
use crate::models::{CheckId, NodeState};
use alert::GlobalAlerter;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get, post};
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
    /// relay admin operations an operator drives from the global dashboard.
    token: Option<String>,
    secure_cookies: bool,
    /// Client for proxying drill-down reads and admin writes to regions.
    client: reqwest::Client,
    /// Fan-out for dashboard WebSocket clients. A `state_change` is published
    /// here whenever an incoming push moves a check's cross-region rollup.
    events: broadcast::Sender<GlobalWsEvent>,
}

impl GlobalNode {
    pub fn new(cfg: &GlobalConfig) -> Self {
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
        let (events, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(Inner {
                cache,
                alerter: GlobalAlerter::new(cfg.notifiers.clone()),
                auth,
                token: cfg.resolved_token(),
                secure_cookies: cfg.secure_cookies,
                client: reqwest::Client::new(),
                events,
            }),
        }
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
            // Everything else under /api/v1 that a coordinator answers is
            // relayed to regions: drill-down reads (history, uptime,
            // sparklines) to the owning region, and admin operations
            // (add/edit/delete checks, notifiers, sections, incidents…) fanned
            // out to every region so the fleet stays uniform. `any` so writes,
            // not just GETs, reach the relay.
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
    agents: Vec<FleetRowDto>,
    maintenance: Vec<serde_json::Value>,
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

async fn status_handler(State(node): State<GlobalNode>) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let checks = node
        .inner
        .cache
        .aggregate(now)
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
        .collect();
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
        agents,
        maintenance: Vec::new(),
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
    Json(body): Json<LoginRequest>,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    match node.inner.auth.login(&body.password, peer.ip(), now) {
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

/// Everything under `/api/v1` the global node does not serve from its own cache
/// is relayed to regional coordinators. Three shapes:
///
/// * **Public reads** (`GET`, no `/admin/`) — a drill-down: route to the region
///   that owns the named check, take the first answer.
/// * **Admin reads** (`GET /api/v1/admin/...`) — same single-answer relay, but
///   the operator's global session is translated into the shared trust token so
///   the region authorizes it. De-duplication of list results is left to the
///   region: they run identical config, so any one region's list is the fleet's.
/// * **Admin writes** (`POST`/`PUT`/`PATCH`/`DELETE`) — fanned out to *every*
///   region so a check added or edited here exists uniformly across the fleet.
///   The operator sees the first region's response; a failure on any region is
///   surfaced so a partial fan-out is never silently reported as success.
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
    let pq = uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or(uri.path())
        .to_string();
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
        Ok(response) => response.into_response(),
        Err(message) => error_response(StatusCode::BAD_GATEWAY, &message),
    }
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

/// Fan an admin write out to one live coordinator in every region. Regions run
/// identical config, so the write must land everywhere or the fleet diverges.
/// The first region's response is returned to the operator; if any region
/// errors (transport or a non-2xx), that failure is surfaced instead so a
/// partial apply is never reported as success.
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
    // Public global creation is a single logical operation. Mint the UUIDv7
    // here, before sending the document to any region, so every replica stores
    // the same immutable identity rather than each regional handler minting one.
    let minted_body = match mint_global_check_id(method, uri.path(), body) {
        Ok(body) => body,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, &message),
    };
    let body = &minted_body;
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
/// Replace any client-supplied ID for a global check creation with one UUIDv7.
/// Other writes pass through untouched.
fn mint_global_check_id(method: &Method, path: &str, body: &Bytes) -> Result<Bytes, String> {
    if method != Method::POST || !matches!(path, "/api/v1/admin/checks" | "/api/v1/checks") {
        return Ok(body.clone());
    }
    let mut value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| "check creation body must be valid JSON".to_string())?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "check creation body must be a JSON object".to_string())?;
    object.insert(
        "id".to_string(),
        serde_json::Value::String(crate::models::new_check_id().to_string()),
    );
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|e| format!("could not encode check creation: {e}"))
}

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
}
