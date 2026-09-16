//! The status-page reads: everything an anonymous visitor's browser calls,
//! plus the raw-results explorer, which shares this module because it is
//! shaped the same way even though it happens to require a session.
//!
//! **Nothing here trusts a session to gate what it returns — only to widen
//! it.** `is_admin` decides whether `status_handler` and `history_handler`
//! hand back a probe's raw error text or the summary everyone else gets; it
//! never decides whether the response is sent at all. `results_handler` is
//! the one handler in this file that *is* gated, by `require_admin`, and it
//! lives here anyway because "recent results, filtered" is the same shape of
//! read as the public ones, just with the redaction switched permanently on.
//! The rule this file exists to hold is simple to state and easy to get
//! backwards under pressure to add a field: an admin session may only ever
//! make a response here say *more*, never less to someone without one.
//!
//! **`public_error` is the one place that downgrade happens**, and it works
//! from a value the probe already classified (`FailureKind`) rather than
//! pattern-matching the error text itself — a rewritten message could
//! otherwise turn into a leak by accident.

use super::super::fleet::FleetRowDto;
use super::super::{error_response, fleet_health, internal_error, storage, Coordinator};
use super::session::{require_admin, session_token};
use super::settings::{announced_maintenance, MaintenanceNoticeDto};
use crate::models::{CheckId, NodeState, Status};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

// ---- API response DTOs. The consumer is frontend/src/lib/api.ts; keep the two
// in step when changing a shape here. ----

#[derive(Serialize)]
struct StatusResponse {
    checks: Vec<CheckStatusDto>,
    /// Fleet liveness, aggregated. Public on purpose, and aggregated for the
    /// same reason `/api/v1/agents` is not: a region row says whether
    /// monitoring is healthy without publishing the agent ids, zones and
    /// cadences that map where it runs from. Empty when the section is off.
    agents: Vec<FleetRowDto>,
    /// Maintenance windows running right now that asked to be announced. Only
    /// the announced ones: a silent window suppresses paging and tells nobody,
    /// which is a deliberate choice an operator makes per window.
    maintenance: Vec<MaintenanceNoticeDto>,
}

#[derive(Serialize)]
struct CheckStatusDto {
    check_id: CheckId,
    /// Mutable display name. The status page renders this, never `check_id`.
    name: String,
    #[serde(rename = "status")]
    state: NodeState,
    latency_ms: u64,
    last_check: i64,
    uptime_24h: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Probe type from config, so the UI can label a check http/tcp/dns/icmp.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<crate::models::CheckKind>,
    /// Configured group name. Absent means the check is listed loose.
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    /// Whether this check opted into a latency graph on the status page.
    latency_graph: bool,
    /// When the probe runs, as cron. The *target* is deliberately absent: this
    /// endpoint is public, and probe URLs expose internal infrastructure.
    /// Targets are served only to authenticated admins.
    #[serde(skip_serializing_if = "Option::is_none")]
    cron: Option<String>,
    /// Display order within the section, so the page renders the order an admin
    /// arranged rather than falling back to alphabetical.
    position: i32,
}

#[derive(Serialize)]
struct HistoryResponse {
    check_id: CheckId,
    name: String,
    history: Vec<HistoryDto>,
}

#[derive(Serialize)]
struct HistoryDto {
    timestamp: i64,
    /// Which agent produced this attempt. Several agents probe the same check,
    /// so (timestamp, agent_id) is what makes a row unique.
    agent_id: String,
    status: Status,
    latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct ResultsResponse {
    results: Vec<ResultDto>,
}

/// One raw probe result for the admin explorer. Carries the **raw** `error`
/// verbatim — this DTO is only ever produced behind an admin session, so unlike
/// `HistoryDto` there is no `public_error` downgrade.
#[derive(Serialize)]
struct ResultDto {
    check_id: CheckId,
    name: String,
    agent_id: String,
    timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    zone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    status: Status,
    latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// The failure category, for a coloured pill. `None` on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<crate::models::FailureKind>,
}

#[derive(Serialize)]
struct AlertsResponse {
    alerts: Vec<AlertDto>,
}

/// An alert as the UI reads it. `alert_type` is derived here rather than
/// stored: StateChange / Degradation / Recovery is a pure function of the two
/// state columns, so a stored copy could only ever disagree with them.
#[derive(Serialize)]
struct AlertDto {
    id: String,
    scope: &'static str,
    node_id: String,
    name: String,
    alert_type: &'static str,
    previous_state: Option<NodeState>,
    current_state: NodeState,
    timestamp: i64,
    incident_id: Option<String>,
}

impl AlertDto {
    fn from_row(r: storage::AlertRow, name: String) -> Self {
        let alert_type = match (r.previous_state, r.current_state) {
            (_, NodeState::Degraded) => "degradation",
            (Some(NodeState::Down | NodeState::Degraded), NodeState::Up) => "recovery",
            _ => "state_change",
        };
        Self {
            id: r.id,
            scope: r.scope.as_str(),
            node_id: r.node_id,
            name,
            alert_type,
            previous_state: r.previous_state,
            current_state: r.current_state,
            timestamp: r.timestamp,
            incident_id: r.incident_id,
        }
    }
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct LimitQuery {
    limit: Option<usize>,
    from: Option<i64>,
    to: Option<i64>,
    /// Keep only attempts with this evaluated status, e.g. `?status=down`.
    /// Needed because a busy day holds thousands of results, so the failures a
    /// reader is looking for fall outside any newest-N slice.
    status: Option<String>,
}

/// Server-side filters for the raw-results explorer. Free-text error search is
/// deliberately absent — it runs client-side on the returned batch.
#[derive(Deserialize)]
pub(in crate::coordinator) struct ResultsQuery {
    check: Option<String>,
    agent: Option<String>,
    /// `up` | `down` | `unknown`. An unrecognised value filters nothing.
    status: Option<String>,
    region: Option<String>,
    zone: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct DaysQuery {
    days: Option<usize>,
    /// How many segments to split the window into; the strip keeps a constant
    /// bar count across ranges.
    buckets: Option<usize>,
}

#[derive(Serialize)]
struct UptimeResponse {
    check_id: CheckId,
    name: String,
    days: usize,
    overall_uptime: Option<f64>,
    buckets: Vec<UptimeBucketDto>,
}

#[derive(Serialize)]
struct UptimeBucketDto {
    start: i64,
    end: i64,
    uptime: Option<f64>,
    up: u32,
    total: u32,
}

#[derive(Serialize)]
struct LatencySparklineResponse {
    check_id: CheckId,
    name: String,
    /// Window actually charted, so the client never has to assume it.
    from: i64,
    to: i64,
    buckets: usize,
    points: Vec<LatencyPointDto>,
    /// Latency spread and reliability over the whole window, exact rather than
    /// sampled from the newest rows.
    overall: LatencyBandDto,
    /// Same, per reporting agent: answers "which agent was failing?".
    agents: Vec<AgentBandDto>,
}

#[derive(Serialize)]
struct LatencyPointDto {
    timestamp: i64,
    /// All outcomes observed in this segment, including failures.
    attempts: u32,
    /// Successful outcomes in this segment. Only these contribute latency.
    up: u32,
    /// `null` when no attempt passed — not the same as a 0ms reply.
    avg_latency_ms: Option<f64>,
}

#[derive(Serialize)]
struct LatencyBandDto {
    attempts: u32,
    up: u32,
    /// Spread over successful attempts only; `None` when none passed.
    min_ms: Option<u64>,
    avg_ms: Option<f64>,
    max_ms: Option<u64>,
}

impl From<&storage::LatencyBand> for LatencyBandDto {
    fn from(b: &storage::LatencyBand) -> Self {
        Self {
            attempts: b.attempts,
            up: b.up,
            min_ms: b.min_ms,
            avg_ms: b.avg_ms(),
            max_ms: b.max_ms,
        }
    }
}

#[derive(Serialize)]
struct AgentBandDto {
    agent_id: String,
    #[serde(flatten)]
    band: LatencyBandDto,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct HoursQuery {
    hours: Option<usize>,
    buckets: Option<usize>,
    /// Explicit window, overriding `hours`. Lets a clicked uptime bar chart the
    /// segment it actually covers instead of a `now`-anchored guess.
    from: Option<i64>,
    to: Option<i64>,
}

// ---- Handlers ----

pub(in crate::coordinator) async fn status_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    // The status page is public; raw probe errors name resolved addresses and
    // expected values, so only an admin sees them verbatim.
    let detailed = is_admin(&coord, &headers);
    let (rows, confirmed) = {
        let store = coord.inner.storage.lock();
        // Confirmed states are read under the same guard. A node that learned a
        // check's state by gossip has no measurements of its own, so it has no
        // `list_status` row to carry that state on.
        (store.list_status(now), store.list_current_state())
    };
    let confirmed: std::collections::HashMap<CheckId, NodeState> = confirmed
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.check_id, r.state))
        .collect();
    match rows {
        Ok(rows) => {
            let by_id = coord.inner.checks.read();
            let mut checks: Vec<CheckStatusDto> = rows
                .into_iter()
                // Only currently-configured, published checks. A row with no
                // config was deleted; its history stays queryable, but a status
                // page claiming "blackhole — Down" for something nobody is
                // monitoring any more is worse than saying nothing.
                .filter(|r| by_id.get(&r.check_id).is_some_and(|m| m.visible))
                .map(|r| {
                    let meta = by_id.get(&r.check_id);
                    CheckStatusDto {
                        kind: meta.map(|m| m.kind),
                        group: meta.and_then(|m| m.group.clone()),
                        latency_graph: meta.is_some_and(|m| m.latency_graph),
                        cron: meta.map(|m| m.cron.clone()),
                        position: meta.map_or(i32::MAX, |m| m.position),
                        name: meta.map_or_else(|| r.check_id.to_string(), |m| m.name.clone()),
                        check_id: r.check_id,
                        state: r.state,
                        latency_ms: r.latency_ms,
                        last_check: r.last_check,
                        uptime_24h: r.uptime_24h,
                        status_code: r.status_code,
                        error: public_error(r.error, r.failure, detailed),
                    }
                })
                .collect();

            // A configured check that hasn't reported yet still belongs on the
            // page. Omitting it means adding a five-minute check looks like
            // nothing happened until its first run — the monitor is missing
            // exactly when someone is watching for it to appear.
            let reported: std::collections::HashSet<CheckId> =
                checks.iter().map(|c| c.check_id).collect();
            let pending: Vec<CheckStatusDto> = by_id
                .iter()
                .filter(|(id, m)| m.visible && !reported.contains(id))
                .map(|(id, m)| CheckStatusDto {
                    check_id: *id,
                    name: m.name.clone(),
                    // A check with no measurements here still has a confirmed
                    // state if a peer gossiped one to us. Prefer it over the
                    // configured default: a cluster that knows a service is
                    // DOWN must not read "unknown" on the node that heard it
                    // second-hand. Falls back to `default_status`, which is what
                    // a check reads as before its first result.
                    state: confirmed
                        .get(id)
                        .copied()
                        .unwrap_or_else(|| m.default_status.into()),
                    latency_ms: 0,
                    last_check: 0,
                    uptime_24h: 0.0,
                    status_code: None,
                    error: None,
                    kind: Some(m.kind),
                    group: m.group.clone(),
                    latency_graph: m.latency_graph,
                    cron: Some(m.cron.clone()),
                    position: m.position,
                })
                .collect();
            checks.extend(pending);
            Json(StatusResponse {
                checks,
                agents: fleet_health(&coord, now),
                maintenance: announced_maintenance(&coord, now),
            })
            .into_response()
        }
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn history_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<CheckId>,
    Query(q): Query<LimitQuery>,
    headers: HeaderMap,
) -> Response {
    let limit = q.limit.unwrap_or(100).min(1000);
    // An unrecognised value filters nothing rather than silently returning empty.
    let status = q.status.as_deref().and_then(|s| match s {
        "up" => Some(Status::Up),
        "down" => Some(Status::Down),
        "unknown" => Some(Status::Unknown),
        _ => None,
    });
    // Admins get the real message; everyone else gets a summary. This endpoint
    // feeds the public check page, and raw probe errors quote resolved
    // addresses and expected values.
    let detailed = is_admin(&coord, &headers);
    let name = coord.check_name(id);
    match coord.history(id, limit, q.from, q.to, status) {
        Ok(rows) => {
            let history = rows
                .into_iter()
                .map(|r| HistoryDto {
                    timestamp: r.timestamp,
                    agent_id: r.agent_id,
                    status: r.status,
                    latency_ms: r.latency_ms,
                    status_code: r.status_code,
                    error: public_error(r.error, r.failure, detailed),
                })
                .collect();
            Json(HistoryResponse {
                check_id: id,
                name,
                history,
            })
            .into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// The raw probe-results explorer: recent results across all checks and agents,
/// newest first, narrowed server-side. Admin-only — the rows carry raw errors
/// that quote what the probe saw, so an anonymous caller must never reach here.
pub(in crate::coordinator) async fn results_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<ResultsQuery>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let limit = q.limit.unwrap_or(200).min(1000);
    // An unrecognised value filters nothing rather than silently returning empty.
    let status = q.status.as_deref().and_then(|s| match s {
        "up" => Some(Status::Up),
        "down" => Some(Status::Down),
        "unknown" => Some(Status::Unknown),
        _ => None,
    });
    let check_id = match q.check.as_deref().map(|s| s.parse::<CheckId>()) {
        Some(Ok(id)) => Some(id),
        Some(Err(_)) => {
            return error_response(StatusCode::BAD_REQUEST, "check must be a valid check id")
        }
        None => None,
    };
    let filter = storage::ResultsFilter {
        check_id,
        agent_id: q.agent,
        status,
        region: q.region,
        zone: q.zone,
    };
    let names = coord.inner.checks.read();
    match coord.recent_results(&filter, limit) {
        Ok(rows) => {
            let results = rows
                .into_iter()
                .map(|r| ResultDto {
                    name: names
                        .get(&r.check_id)
                        .map_or_else(|| r.check_id.to_string(), |m| m.name.clone()),
                    check_id: r.check_id,
                    agent_id: r.agent_id,
                    timestamp: r.timestamp,
                    zone: r.zone,
                    region: r.region,
                    status: r.status,
                    latency_ms: r.latency_ms,
                    status_code: r.status_code,
                    // Raw, verbatim: this path is admin-only by the gate above.
                    error: r.error,
                    failure: r.failure,
                })
                .collect();
            Json(ResultsResponse { results }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// Whether this request carries a valid admin session.
///
/// `pub(in crate::coordinator)` because `require_admin`, in `session.rs`,
/// widens the same question into a gate: this function only ever decides
/// how much detail a public response gets, never whether it is sent.
pub(in crate::coordinator) fn is_admin(coord: &Coordinator, headers: &HeaderMap) -> bool {
    let now = chrono::Utc::now().timestamp_millis();
    if session_token(headers).is_some_and(|t| coord.inner.auth.validate(t, now)) {
        return true;
    }
    // Phase 6: the global coordinator relays admin operations on an operator's
    // behalf, authenticating with the shared cross-region secret rather than a
    // per-node session it could never mint. Constant-time compare so the token
    // cannot be recovered by timing, matching how the password is checked.
    if let Some(trust) = coord.inner.admin_trust_token.as_deref() {
        if let Some(presented) = super::session::bearer(headers) {
            use subtle::ConstantTimeEq;
            if bool::from(presented.as_bytes().ct_eq(trust.as_bytes())) {
                return true;
            }
        }
    }
    false
}

/// Raw failure text for admins, the probe's own classification for everyone else.
///
/// The summary comes from a value the probe set, never from matching on the
/// text: a reworded message must not be able to turn into a leak.
fn public_error(
    error: Option<String>,
    failure: Option<crate::models::FailureKind>,
    detailed: bool,
) -> Option<String> {
    match (error, detailed) {
        (None, _) => None,
        (Some(e), true) => Some(e),
        (Some(_), false) => Some(
            failure
                .map(|f| f.summary())
                .unwrap_or_else(|| crate::models::LEGACY_FAILURE_SUMMARY.to_string()),
        ),
    }
}

pub(in crate::coordinator) async fn uptime_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<CheckId>,
    Query(q): Query<DaysQuery>,
) -> Response {
    let days = q.days.unwrap_or(90).clamp(1, 365);
    // The strip keeps a constant bar count; a shorter window just means finer
    // segments rather than fewer bars.
    let buckets = q.buckets.unwrap_or(90).clamp(1, 500);
    let now = chrono::Utc::now().timestamp_millis();
    // Use the resolved metadata, not the persisted optional override: a check
    // inherits the coordinator default when its own setting is absent.
    let degraded_is_downtime = coord.degraded_is_downtime(id);
    let window_start = now - days as i64 * 86_400_000;
    let result = (|| -> anyhow::Result<_> {
        let storage = coord.inner.storage.lock();
        let rows = storage.uptime_buckets(id, days, buckets, now, degraded_is_downtime)?;
        let overall = storage.uptime_between(id, window_start, now, degraded_is_downtime)?;
        Ok((rows, overall))
    })();
    match result {
        Ok((rows, overall_uptime)) => {
            let buckets = rows
                .into_iter()
                .map(|b| UptimeBucketDto {
                    start: b.start,
                    end: b.end,
                    uptime: b.uptime,
                    up: b.up.min(u32::MAX as u64) as u32,
                    total: b.total.min(u32::MAX as u64) as u32,
                })
                .collect();
            Json(UptimeResponse {
                check_id: id,
                name: coord.check_name(id),
                days,
                overall_uptime,
                buckets,
            })
            .into_response()
        }
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn latency_sparkline_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<CheckId>,
    Query(q): Query<HoursQuery>,
) -> Response {
    let buckets = q.buckets.unwrap_or(120).clamp(1, 500);
    let now = chrono::Utc::now().timestamp_millis();
    // An explicit window wins; otherwise chart the last `hours` up to now. Up to
    // a year, so the graph can follow the same range switcher as the uptime
    // strip instead of being silently truncated at 7 days.
    let (from, to) = match (q.from, q.to) {
        (Some(f), Some(t)) if t > f => (f, t),
        _ => {
            let hours = q.hours.unwrap_or(24).clamp(1, 24 * 365) as i64;
            (now - hours * 3_600_000, now)
        }
    };
    match coord
        .inner
        .storage
        .lock()
        .latency_series(id, from, to, buckets)
    {
        Ok(series) => {
            let points = series
                .points
                .into_iter()
                .map(|p| LatencyPointDto {
                    timestamp: p.timestamp,
                    attempts: p.attempts,
                    up: p.up,
                    avg_latency_ms: p.avg_latency_ms,
                })
                .collect();
            Json(LatencySparklineResponse {
                check_id: id,
                name: coord.check_name(id),
                from,
                to,
                buckets,
                points,
                overall: (&series.overall).into(),
                agents: series
                    .agents
                    .iter()
                    .map(|a| AgentBandDto {
                        agent_id: a.agent_id.clone(),
                        band: (&a.band).into(),
                    })
                    .collect(),
            })
            .into_response()
        }
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn alerts_handler(
    State(coord): State<Coordinator>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).min(1000);
    let rows = coord.inner.storage.lock().recent_alerts(limit);
    match rows {
        Ok(rows) => {
            let alerts = rows
                .into_iter()
                .map(|row| {
                    let name = coord.alert_name(row.scope, &row.node_id);
                    AlertDto::from_row(row, name)
                })
                .collect();
            Json(AlertsResponse { alerts }).into_response()
        }
        Err(e) => internal_error(e),
    }
}
