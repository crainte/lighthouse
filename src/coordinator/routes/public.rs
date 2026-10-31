//! The status-page reads: everything an anonymous visitor's browser calls,
//! plus the raw-results explorer, which shares this module because it is
//! shaped the same way even though it happens to require a session.
//!
//! **Nothing here trusts a session to gate what it returns — only to widen
//! it.** `is_admin` decides whether `status_handler` and `history_handler`
//! hand back a probe's raw error text or the summary everyone else gets; it
//! never decides whether the response is sent at all. `results_handler` is
//! the one handler in this file that *is* gated, by `crate::http::AdminSession`,
//! and it lives here anyway because "recent results, filtered" is the same shape of
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
use super::super::router::{error_response, fleet_health, internal_error};
use super::super::{storage, Coordinator};
use super::session::session_token;
use super::settings::{announced_maintenance, MaintenanceNoticeDto};
use crate::http::AdminSession;
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
    /// The requested, bounded page of visible checks. Chart endpoints accept the
    /// same scope/page/limit tuple and therefore return series for precisely
    /// these rows rather than an unbounded superset.
    checks: Vec<CheckStatusDto>,
    page: usize,
    limit: usize,
    total_checks: usize,
    has_more: bool,
    /// Fleet liveness, aggregated. Public on purpose, and aggregated for the
    /// same reason `/api/v1/agents` is not: a region row says whether
    /// monitoring is healthy without publishing the agent ids, zones and
    /// cadences that map where it runs from. Empty when the section is off.
    agents: Vec<FleetRowDto>,
    /// Maintenance windows running right now that asked to be announced. Only
    /// the announced ones: a silent window suppresses paging and tells nobody,
    /// which is a deliberate choice an operator makes per window.
    maintenance: Vec<MaintenanceNoticeDto>,
    /// One rolled-up state per section that has checks. Computed here rather
    /// than in the browser so a section tile and a zone row cannot disagree
    /// about what "this set of children is degraded" means — a section is
    /// another node set, and it gets the same `RollupPolicy` machinery.
    sections: Vec<SectionRollupDto>,
}

#[derive(Serialize)]
struct SectionRollupDto {
    name: String,
    #[serde(rename = "status")]
    state: NodeState,
    /// Counts for the tile's caption. A tile shows no history strip, so these
    /// are all a collapsed section says about itself, and "12 checks, 1 down"
    /// is the difference between a red tile an operator can triage and one
    /// they have to open to understand.
    checks: usize,
    degraded: usize,
    down: usize,
    /// Members paused by the operator. When this equals `checks`, the section
    /// has no live health to report and the tile says Paused.
    paused: usize,
}

#[derive(Serialize)]
struct CheckStatusDto {
    check_id: CheckId,
    /// Mutable display name. The status page renders this, never `check_id`.
    name: String,
    /// A paused check stays published with its history but does not contribute
    /// to current service health. Distinct from `visible`.
    enabled: bool,
    #[serde(rename = "status")]
    state: NodeState,
    latency_ms: u64,
    last_check: i64,
    /// `None` when the window holds no observation at all — a check configured
    /// here but placed elsewhere, or one probed for the first time moments ago.
    /// Not `0.0`: "nothing was measured" and "nothing was up" are different
    /// answers, and only one of them is an outage.
    uptime_24h: Option<f64>,
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
pub(in crate::coordinator) struct StatusQuery {
    /// A named section, or omitted for the primary page (loose + inline checks).
    section: Option<String>,
    /// Include every visible check, regardless of section display mode. The
    /// admin Overview needs all monitors, not only the primary public scope.
    all: Option<bool>,
    /// Zero-based page within the selected scope.
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct DaysQuery {
    days: Option<usize>,
    /// How many segments to split the window into; the strip keeps a constant
    /// bar count across ranges.
    buckets: Option<usize>,
    /// Scope the request to one section. See [`storage::StatusScope`].
    section: Option<String>,
    all: Option<bool>,
    /// Must match the status request's bounded page.
    page: Option<usize>,
    limit: Option<usize>,
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

/// Every visible check's strip in one response.
///
/// Carries the same per-check shape as `UptimeResponse` rather than a flatter
/// one, so the page keys it exactly as it keyed the N individual responses this
/// replaces.
#[derive(Serialize)]
struct UptimeStripsResponse {
    days: usize,
    checks: Vec<UptimeResponse>,
}

/// Sparkline points for every check that draws one.
///
/// Deliberately not `LatencySparklineResponse`: the row chart reads only
/// `points`, while `overall` and `agents` exist for the single-check detail
/// view. Serving them here would double the scan to fill fields this page never
/// renders.
#[derive(Serialize)]
struct LatencyPointsResponse {
    from: i64,
    to: i64,
    buckets: usize,
    checks: Vec<CheckLatencyPointsDto>,
}

#[derive(Serialize)]
struct CheckLatencyPointsDto {
    check_id: CheckId,
    points: Vec<LatencyPointDto>,
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
    /// Scope the request to one section. See [`storage::StatusScope`].
    section: Option<String>,
    all: Option<bool>,
    /// Must match the status request's bounded page.
    page: Option<usize>,
    limit: Option<usize>,
}

/// One page of visible checks in `scope`, each paired with its resolved
/// `degraded_is_downtime`. Scope, order and paging run in SQL over the
/// catalogue; only the page's ids are resolved against cached metadata.
pub(in crate::coordinator) fn check_page(
    coord: &Coordinator,
    storage: &storage::Storage,
    scope: &storage::StatusScope,
    latency_graph_only: bool,
    page: usize,
    limit: usize,
) -> anyhow::Result<(Vec<(CheckId, bool)>, usize)> {
    let found = storage.visible_check_page(scope, latency_graph_only, page, limit)?;
    let by_id = coord.inner.checks.read();
    let checks = found
        .ids
        .into_iter()
        .map(|id| (id, by_id.get(&id).is_some_and(|m| m.degraded_is_downtime)))
        .collect();
    Ok((checks, found.total))
}

// ---- Handlers ----

pub(in crate::coordinator) async fn status_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<StatusQuery>,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    // The status page is public; raw probe errors name resolved addresses and
    // expected values, so only an admin sees them verbatim.
    let detailed = is_admin(&coord, &headers);
    let scope = if q.all.unwrap_or(false) {
        storage::StatusScope::All
    } else {
        storage::StatusScope::from_section(q.section)
    };
    let (page, limit) = (q.page.unwrap_or(0), crate::page::bounded(q.limit, 100));
    let reader = coord.clone();
    let (statuses, total_checks, section_states) = match coord
        .db(move |storage| {
            let (checks, total) = check_page(&reader, storage, &scope, false, page, limit)?;
            Ok((
                storage.status_for(&checks, now)?,
                total,
                storage.section_states()?,
            ))
        })
        .await
    {
        Ok(value) => value,
        Err(e) => return internal_error(e),
    };
    let checks: Vec<CheckStatusDto> = {
        let by_id = coord.inner.checks.read();
        statuses
            .into_iter()
            .filter_map(|status| {
                let check_id = status.check_id();
                let meta = by_id.get(&check_id)?;
                // A configured check that hasn't reported yet still belongs on
                // the page, carrying any state a peer gossiped.
                let r = status.resolve(meta.default_status.into());
                Some(CheckStatusDto {
                    check_id,
                    name: meta.name.clone(),
                    enabled: meta.enabled,
                    state: r.state,
                    latency_ms: r.latency_ms,
                    last_check: r.last_check,
                    uptime_24h: r.uptime_24h,
                    status_code: r.status_code,
                    error: public_error(r.error, r.failure, detailed),
                    kind: Some(meta.kind),
                    group: meta.group.clone(),
                    latency_graph: meta.latency_graph,
                    cron: Some(meta.cron.clone()),
                    position: meta.position,
                })
            })
            .collect()
    };
    let has_more = page.saturating_add(1).saturating_mul(limit) < total_checks;
    let agents = fleet_health(&coord, now).await;
    Json(StatusResponse {
        checks,
        page,
        limit,
        total_checks,
        has_more,
        agents,
        maintenance: announced_maintenance(&coord, now),
        sections: section_rollups(&coord, section_states),
    })
    .into_response()
}

/// Fold each section's checks into one state, using the same policy lookup the
/// zone and region rows use.
///
/// The per-section override key is `section:<name>`, so an operator who wants a
/// section to go red only when most of it fails writes that in the same place
/// they set it for a zone. The fallback is `CHECK_DEFAULT` — the policy already
/// applied to a set of checks at the global root — not a fourth constant
/// invented for sections.
///
/// Loose checks are deliberately absent: they render as their own rows, and a
/// rollup over "everything with no section" is not a thing the page shows.
fn section_rollups(
    coord: &Coordinator,
    sections: std::collections::BTreeMap<String, storage::SectionStates>,
) -> Vec<SectionRollupDto> {
    let by_id = coord.inner.checks.read();
    let policies = coord.inner.rollup_policies.read();
    sections
        .into_iter()
        .map(|(name, counts)| {
            let states: Vec<(NodeState, usize)> = counts
                .states
                .into_iter()
                .chain(
                    counts
                        .pending
                        .iter()
                        .filter_map(|id| by_id.get(id))
                        .map(|m| (NodeState::from(m.default_status), 1)),
                )
                .collect();
            let count = |wanted: NodeState| -> usize {
                states
                    .iter()
                    .filter(|(s, _)| *s == wanted)
                    .map(|(_, n)| n)
                    .sum()
            };
            let policy = policies
                .get(&format!("section:{name}"))
                .copied()
                .unwrap_or(crate::rollup::RollupPolicy::CHECK_DEFAULT);
            SectionRollupDto {
                state: crate::rollup::rollup_counts(&policy, states.iter().copied()),
                checks: states.iter().map(|(_, n)| n).sum::<usize>() + counts.paused,
                paused: counts.paused,
                degraded: count(NodeState::Degraded),
                down: count(NodeState::Down),
                name,
            }
        })
        .collect()
}

pub(in crate::coordinator) async fn history_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<CheckId>,
    Query(q): Query<LimitQuery>,
    headers: HeaderMap,
) -> Response {
    let limit = crate::page::bounded(q.limit, 100);
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
    match coord.history(id, limit, q.from, q.to, status).await {
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
    _admin: AdminSession,
    Query(q): Query<ResultsQuery>,
) -> Response {
    let limit = crate::page::bounded(q.limit, 200);
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
            return error_response(StatusCode::BAD_REQUEST, "check must be a valid check id");
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
    match coord.recent_results(filter, limit).await {
        Ok(rows) => {
            let names = coord.inner.checks.read();
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
/// `pub(in crate::coordinator)` because `crate::http::AdminSession` widens
/// the same question into a gate: this function only ever decides how much
/// detail a public response gets, never whether it is sent.
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
    let result = coord
        .db(move |storage| {
            let rows = storage.uptime_buckets(id, days, buckets, now, degraded_is_downtime)?;
            let overall = storage.uptime_between(id, window_start, now, degraded_is_downtime)?;
            Ok((rows, overall))
        })
        .await;
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
        .db(move |storage| storage.latency_series(id, from, to, buckets))
        .await
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

/// Every visible check's uptime strip in one request.
///
/// The status page draws a strip per check, so fetching them individually made
/// a page load cost one request and one database job per check. The visibility
/// rule is the status page's, so the strips cover exactly the rows it lists.
pub(in crate::coordinator) async fn uptime_strips_handler(
    State(coord): State<Coordinator>,
    Query(q): Query<DaysQuery>,
) -> Response {
    let days = q.days.unwrap_or(90).clamp(1, 365);
    let buckets = q.buckets.unwrap_or(90).clamp(1, 500);
    let now = chrono::Utc::now().timestamp_millis();
    let scope = if q.all.unwrap_or(false) {
        storage::StatusScope::All
    } else {
        storage::StatusScope::from_section(q.section)
    };
    let page = q.page.unwrap_or(0);
    let limit = crate::page::bounded(q.limit, 100);
    let reader = coord.clone();
    match coord
        .db(move |storage| {
            let (wanted, _) = check_page(&reader, storage, &scope, false, page, limit)?;
            storage.uptime_strips(&wanted, days, buckets, now)
        })
        .await
    {
        Ok(strips) => Json(UptimeStripsResponse {
            days,
            checks: strips
                .into_iter()
                .map(|(check_id, strip)| UptimeResponse {
                    check_id,
                    name: coord.check_name(check_id),
                    days,
                    overall_uptime: strip.overall,
                    buckets: strip
                        .buckets
                        .into_iter()
                        .map(|b| UptimeBucketDto {
                            start: b.start,
                            end: b.end,
                            uptime: b.uptime,
                            up: b.up.min(u32::MAX as u64) as u32,
                            total: b.total.min(u32::MAX as u64) as u32,
                        })
                        .collect(),
                })
                .collect(),
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// Sparkline points for every visible check that opted into a latency graph.
///
/// Checks without one are omitted rather than returned empty: the page skips
/// them anyway, and sending empty series would grow the payload for nothing.
pub(in crate::coordinator) async fn latency_points_handler(
    State(coord): State<Coordinator>,
    Query(q): Query<HoursQuery>,
) -> Response {
    let buckets = q.buckets.unwrap_or(120).clamp(1, 500);
    let now = chrono::Utc::now().timestamp_millis();
    let (from, to) = match (q.from, q.to) {
        (Some(f), Some(t)) if t > f => (f, t),
        _ => {
            let hours = q.hours.unwrap_or(24).clamp(1, 24 * 365) as i64;
            (now - hours * 3_600_000, now)
        }
    };
    let scope = if q.all.unwrap_or(false) {
        storage::StatusScope::All
    } else {
        storage::StatusScope::from_section(q.section)
    };
    let page = q.page.unwrap_or(0);
    let limit = crate::page::bounded(q.limit, 100);
    match coord
        .db(move |storage| {
            let ids = storage.visible_check_page(&scope, true, page, limit)?.ids;
            storage.latency_points_for(&ids, from, to, buckets)
        })
        .await
    {
        Ok(series) => Json(LatencyPointsResponse {
            from,
            to,
            buckets,
            checks: series
                .into_iter()
                .map(|(check_id, points)| CheckLatencyPointsDto {
                    check_id,
                    points: points
                        .into_iter()
                        .map(|p| LatencyPointDto {
                            timestamp: p.timestamp,
                            attempts: p.attempts,
                            up: p.up,
                            avg_latency_ms: p.avg_latency_ms,
                        })
                        .collect(),
                })
                .collect(),
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn alerts_handler(
    State(coord): State<Coordinator>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = crate::page::bounded(q.limit, 50);
    let rows = coord.db(move |storage| storage.recent_alerts(limit)).await;
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
