use crate::config::CheckConfig;
use crate::coordinator::hlc::Hlc;
use crate::credential::AgentCredential;
use crate::models::{CheckId, CheckKind, CheckResult, NodeState, Status};
use anyhow::{bail, Result};
use duckdb::{Connection, Statement};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const MILLIS_PER_MINUTE: i64 = 60_000;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;
const RAW_RETENTION_MILLIS: i64 = 30 * 24 * MILLIS_PER_HOUR;
const MINUTE_ROLLUP_RETENTION_MILLIS: i64 = 90 * 24 * MILLIS_PER_HOUR;
// Thirteen 30-day months is deliberately a conservative fixed-duration
// retention window; unlike calendar months it remains unambiguous in UTC.
const HOUR_ROLLUP_RETENTION_MILLIS: i64 = 13 * 30 * 24 * MILLIS_PER_HOUR;

/// DuckDB-backed store for check results and materialized current state.
/// The dedicated database worker exclusively owns this single DuckDB connection.
pub struct Storage {
    conn: Connection,
    incident_node_id: parking_lot::RwLock<String>,
}

/// One row of check history. Multiple agents probe the same check, so rows are
/// only unique by (timestamp, agent_id) — timestamp alone collides.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRow {
    pub timestamp: i64, // unix millis
    pub agent_id: String,
    pub status: Status,
    pub latency_ms: u64,
    /// HTTP status code observed, when the probe was HTTP.
    pub status_code: Option<u16>,
    /// Why the probe failed, verbatim from the agent. `None` on success.
    /// Never publish this: it quotes what the probe saw.
    pub error: Option<String>,
    /// The same failure as a category, safe to publish. `None` for rows written
    /// before probes classified themselves.
    pub failure: Option<crate::models::FailureKind>,
}

/// One raw probe result, across all checks and agents, for the admin explorer.
/// Unlike `HistoryRow` this carries the check and location, because the feed is
/// cross-check and cross-agent — the operator needs to know *which* check and
/// *where* without having already narrowed to one.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultRow {
    pub check_id: CheckId,
    pub agent_id: String,
    pub timestamp: i64, // unix millis
    /// Where the probe ran. `None` for results recorded before agents declared
    /// a location.
    pub zone: Option<String>,
    pub region: Option<String>,
    pub status: Status,
    pub latency_ms: u64,
    pub status_code: Option<u16>,
    /// Why the probe failed, verbatim. Admin-only: it quotes what the probe saw.
    pub error: Option<String>,
    /// The failure as a category, safe to publish. `None` on success or for
    /// rows written before probes classified themselves.
    pub failure: Option<crate::models::FailureKind>,
}

/// Server-side narrowing for the raw-results explorer. Every field is optional;
/// an absent field is "no filter on this column". Free-text error search is
/// deliberately absent — it runs client-side on the returned batch.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultsFilter {
    pub check_id: Option<CheckId>,
    pub agent_id: Option<String>,
    pub status: Option<Status>,
    pub region: Option<String>,
    pub zone: Option<String>,
}

/// Current status of a check plus recent uptime, for the status grid.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusRow {
    pub check_id: CheckId,
    /// The coordinator's published conclusion, which may be Degraded.
    pub state: NodeState,
    pub latency_ms: u64,
    pub last_check: i64, // unix millis of latest result
    /// Time-weighted fraction 0.0..=1.0 of the 24h before now spent Up,
    /// computed from `state_history` spans rather than from raw result rows.
    pub uptime_24h: f64,
    /// Status code from the most recent result.
    pub status_code: Option<u16>,
    /// Failure reason from the most recent result, so the dashboard can say why
    /// a check is down rather than only that it is. Never publish it verbatim.
    pub error: Option<String>,
    /// That same failure as a category, safe to publish.
    pub failure: Option<crate::models::FailureKind>,
}

/// The coordinator's PUBLISHED decision per check, materialized so the status
/// page and a restarted process agree with the in-memory tracker.
///
/// `consecutive_failures` no longer lives here: failures are counted per agent
/// now, in `agent_check_state`. The column survives in the table for older
/// readers but the coordinator no longer means anything by it.
#[derive(Debug, Clone, PartialEq)]
pub struct CurrentStateRow {
    pub check_id: CheckId,
    pub state: NodeState,
    /// The state the hold-down timer is arguing for, and when it first said so.
    /// `None` when the rollup agrees with what is published.
    pub pending: Option<(NodeState, i64)>,
}

/// One agent's confirmed opinion of one check — a leaf of the region tree.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentCheckStateRow {
    pub check_id: CheckId,
    pub agent_id: String,
    /// Auth-stamped leaf topology, persisted so restart rollups use the same
    /// zone/region membership as the observation that established the belief.
    pub zone: String,
    pub region: String,
    pub status: Status,
    pub consecutive_failures: u32,
    pub last_check: i64,
}

/// What the coordinator publishes for one check after a rollup: the state,
/// any hold-down timer in flight, and when it was last observed.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedStateRow {
    pub check_id: CheckId,
    pub state: NodeState,
    pub pending: Option<(NodeState, i64)>,
    pub last_check: i64,
}

/// A stored rollup policy for one node of the tree.
#[derive(Debug, Clone, PartialEq)]
pub struct RollupPolicyRow {
    /// Namespaced: `global`, `region:us-east`, `zone:us-east-1a`.
    pub node_id: String,
    pub node_type: NodeType,
    pub policy: crate::rollup::RollupPolicy,
    pub updated_at: i64,
    pub updated_by: Option<String>,
}

/// Which level of the tree a node sits at.
///
/// Unrelated to `Incident.global`, which means "shown page-wide" and is about
/// presentation. Two different words called global; do not conflate them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    Global,
    Region,
    Zone,
}

impl NodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeType::Global => "global",
            NodeType::Region => "region",
            NodeType::Zone => "zone",
        }
    }

    pub fn from_db(s: &str) -> NodeType {
        match s {
            "region" => NodeType::Region,
            "zone" => NodeType::Zone,
            _ => NodeType::Global,
        }
    }
}

/// One span of published state for a check: it entered `state` at `since` and
/// stayed there until the next row's `since` (or until now, for the last one).
#[derive(Debug, Clone, PartialEq)]
pub struct StateSpan {
    pub state: NodeState,
    pub since: i64,
}

/// A probe agent that has reported results, derived from observed `agent_id`s.
/// There is no agent registry — an agent exists because it sent data.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRow {
    pub agent_id: String,
    /// Where it last reported from. Read from its most recent result rather
    /// than a registry, for the same reason the agent list itself is: an agent
    /// that has been moved should be shown where it actually is now.
    pub zone: String,
    pub region: String,
    pub last_seen: i64, // unix millis of its most recent result
    pub checks_reporting: u32,
    /// Results it submitted in the last 24h, as a rough liveness signal.
    pub results_24h: u32,
}

/// A persisted alert: one confirmed state transition. The event log, distinct
/// from an incident, which is the operator-facing record of an outage. An
/// alert names the incident it belongs to, stamped as the row is written —
/// membership is a fact the coordinator knows at that moment and no reader can
/// re-derive from a time window.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRow {
    pub id: String,
    /// What kind of thing moved. `Check` is what pages; `Zone` and `Region`
    /// rows are the event log behind it, written so the fleet view can show
    /// "us-east lost reach to X" as an event rather than as an inference.
    pub scope: AlertScope,
    /// The check, agent or zone that moved.
    pub node_id: String,
    /// `None` for a first-ever observation, which has nothing to move from.
    pub previous_state: Option<NodeState>,
    pub current_state: NodeState,
    pub timestamp: i64,
    /// The incident this transition belongs to, when it belongs to one.
    pub incident_id: Option<String>,
}

/// One notification that has not been delivered. The transition it carries is
/// everything a retry needs, so the row is self-contained: no join, and nothing
/// to dangle when the alert it came from is pruned.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedRow {
    /// Immutable delivery identity. It distinguishes transitions that share a
    /// notifier, check, and millisecond, and stays stable across retries.
    pub id: String,
    /// The routing *name*, resolved against the live notifier list at each
    /// attempt. Editing a backend therefore fixes its queued notifications;
    /// deleting it drops them, because there is no longer anyone to page.
    pub notifier: String,
    pub check_id: CheckId,
    pub previous_state: Option<NodeState>,
    pub current_state: NodeState,
    /// Failed attempts so far, first included.
    pub attempts: i32,
    /// When to try next. `None` means given up — attempts exhausted.
    pub next_attempt: Option<i64>,
    pub last_error: String,
    pub queued_at: i64,
}

/// What an alert is about. Stored rather than derived — it is not a function
/// of anything else on the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertScope {
    Check,
    Agent,
    Zone,
    Region,
}

impl AlertScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertScope::Check => "check",
            AlertScope::Agent => "agent",
            AlertScope::Zone => "zone",
            AlertScope::Region => "region",
        }
    }

    pub fn from_db(s: &str) -> AlertScope {
        match s {
            "agent" => AlertScope::Agent,
            "zone" => AlertScope::Zone,
            "region" => AlertScope::Region,
            _ => AlertScope::Check,
        }
    }
}

/// Where an incident is in its life. The single lifecycle field: "open" means
/// anything other than `Resolved`, so there is no second status column to fall
/// out of sync with it.
///
/// `Detected` is deliberately not `Investigating`: a probe noticing an outage
/// is not a human looking at one, and on a public page the difference is the
/// expensive kind of lie. Acknowledging is the `Detected` -> `Investigating`
/// edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentState {
    Detected,
    Investigating,
    Identified,
    Monitoring,
    Resolved,
}

impl IncidentState {
    pub fn as_str(&self) -> &'static str {
        match self {
            IncidentState::Detected => "detected",
            IncidentState::Investigating => "investigating",
            IncidentState::Identified => "identified",
            IncidentState::Monitoring => "monitoring",
            IncidentState::Resolved => "resolved",
        }
    }

    /// Anything unrecognised reads as `Detected` rather than failing the row:
    /// a state nobody has acted on is the safe reading of a value we cannot
    /// interpret.
    pub fn from_db(s: &str) -> IncidentState {
        match s {
            "investigating" => IncidentState::Investigating,
            "identified" => IncidentState::Identified,
            "monitoring" => IncidentState::Monitoring,
            "resolved" => IncidentState::Resolved,
            _ => IncidentState::Detected,
        }
    }

    pub fn is_resolved(&self) -> bool {
        matches!(self, IncidentState::Resolved)
    }
}

/// How a check came to be attached to an incident. Recorded so reconciliation
/// can retract what it added without touching what an operator added by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AffectSource {
    /// The check whose DOWN opened the incident.
    Origin,
    /// Attached by `depends_on` reconciliation.
    Dependency,
    /// Attached by an operator in the editor.
    Manual,
}

impl AffectSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            AffectSource::Origin => "origin",
            AffectSource::Dependency => "dependency",
            AffectSource::Manual => "manual",
        }
    }

    pub fn from_db(s: &str) -> AffectSource {
        match s {
            "origin" => AffectSource::Origin,
            "dependency" => AffectSource::Dependency,
            _ => AffectSource::Manual,
        }
    }

    /// Whether this attachment gates auto-resolve. Manual ones do not: an
    /// operator naming a check they suspect must not be able to wedge an
    /// incident open forever.
    pub fn gates_resolution(&self) -> bool {
        !matches!(self, AffectSource::Manual)
    }
}

/// A check attached to an incident, and how it got there.
#[derive(Debug, Clone, PartialEq)]
pub struct IncidentCheckRow {
    pub check_id: CheckId,
    pub source: AffectSource,
}

/// One entry in an incident's narrative. Every state change is one of these,
/// including the two the coordinator writes itself on auto-open and
/// auto-resolve, so the timeline can never disagree with the state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentUpdateRow {
    pub id: String,
    pub incident_id: String,
    /// The state this update announced.
    pub state: IncidentState,
    pub body: String,
    /// `None` when the coordinator wrote it. A non-None author is what makes
    /// an update an acknowledgement; there is no separate ack column.
    pub author: Option<String>,
    pub created_at: i64,
}

/// The operator-facing record of an outage.
#[derive(Debug, Clone, PartialEq)]
pub struct IncidentRow {
    pub id: String,
    pub title: String,
    pub state: IncidentState,
    /// When the outage began, unix millis. Editable: an operator may move it
    /// back to when customers noticed rather than when a probe did. Moving it
    /// does not re-own alerts — those carry their own stamp.
    pub started_at: i64,
    /// When it was declared over; `None` while open.
    pub resolved_at: Option<i64>,
    /// Rendered page-wide rather than against its checks. A global incident
    /// with no checks at all is the maintenance-notice case.
    pub global: bool,
    /// The check whose DOWN opened it. `None` for a manually opened incident,
    /// which reconciliation therefore ignores entirely.
    pub origin_check: Option<CheckId>,
    /// Every check this incident affects, with how each was attached.
    pub checks: Vec<IncidentCheckRow>,
}

impl IncidentRow {
    /// How long the outage has run: to its resolution, or to now while open.
    pub fn duration_ms(&self, now_millis: i64) -> i64 {
        self.resolved_at.unwrap_or(now_millis) - self.started_at
    }

    /// The checks whose recovery gates auto-resolve — everything except what
    /// an operator attached by hand.
    pub fn gating_checks(&self) -> impl Iterator<Item = CheckId> + '_ {
        self.checks
            .iter()
            .filter(|c| c.source.gates_resolution())
            .map(|c| c.check_id)
    }
}

/// One segment of the uptime strip. Not necessarily a day: the window is split
/// into a fixed number of segments, so a 7-day view has ~2-hour segments.
/// `up` and `total` count probe results; uptime itself remains time-weighted.
#[derive(Debug, Clone, PartialEq)]
pub struct UptimeBucket {
    /// Exclusive lower bound of the segment, unix millis.
    pub start: i64,
    /// Inclusive upper bound, unix millis. Handed to the UI so it can query the
    /// exact window a bar covers without re-deriving the segment width.
    pub end: i64,
    /// Fraction 0.0..=1.0, or None when the segment has no data at all.
    pub uptime: Option<f64>,
    /// Successful probe attempts in this segment.
    pub up: u64,
    /// All probe attempts in this segment.
    pub total: u64,
}

/// A single latency data point for the sparkline chart.
/// One check's uptime strip plus its exact figure for the whole window. Paired
/// because every caller that draws the strip also prints the number, and
/// deriving the number from the rounded segments would disagree with it.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckUptime {
    pub buckets: Vec<UptimeBucket>,
    pub overall: Option<f64>,
}

/// One check's per-segment totals while folding the aggregated rows DuckDB
/// returned. Parallel vectors rather than a `Vec` of structs because every
/// segment exists whether or not a row landed in it, and the empty ones are the
/// common case on a sparse window.
struct SegmentCounts {
    sum_ms: Vec<f64>,
    attempts: Vec<u32>,
    up: Vec<u32>,
}

impl SegmentCounts {
    fn new(buckets: usize) -> Self {
        Self {
            sum_ms: vec![0.0; buckets],
            attempts: vec![0; buckets],
            up: vec![0; buckets],
        }
    }
}

/// The segment grid an uptime strip is drawn on. Shared by the single-check and
/// batched paths so their segment edges cannot drift apart: a strip fetched for
/// one check must tile identically to the same check's strip on the status page.
struct BucketGrid {
    lower: i64,
    bucket_ms: i64,
    buckets: usize,
}

impl BucketGrid {
    fn new(now_millis: i64, days: usize, buckets: usize) -> Self {
        const DAY: i64 = 86_400_000;
        let buckets = buckets.max(1);
        let bucket_ms = ((days as i64 * DAY) / buckets as i64).max(1);
        // Recompute the span from the bucket width so segment edges tile exactly:
        // integer division would otherwise leave a sliver unaccounted for.
        Self {
            lower: now_millis - bucket_ms * buckets as i64,
            bucket_ms,
            buckets,
        }
    }

    /// Lower bound is exclusive, so shift before dividing: a timestamp landing
    /// exactly on a boundary belongs to the segment below it.
    fn index(&self, ts: i64) -> usize {
        (((ts - self.lower - 1).max(0) / self.bucket_ms) as usize).min(self.buckets - 1)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LatencyPoint {
    pub timestamp: i64, // unix millis of the window start
    /// Every probe outcome in the segment. This is distinct from `up`: failed
    /// attempts establish that the check was observed, but do not measure its
    /// response latency.
    pub attempts: u32,
    /// Successful probe outcomes in the segment.
    pub up: u32,
    /// Mean latency across successful attempts, or `None` when none passed.
    /// A segment with no successful attempts is not a 0ms response, and
    /// collapsing the two paints an imaginary flat line along the axis.
    pub avg_latency_ms: Option<f64>,
}

/// How the public page renders a section. An operator choice per section, not a
/// global rule: a three-check section is better inline, and forcing a click for
/// it would be worse than a flat page. A large one is unreadable inline and
/// costs a strip per check to render, which is what `Page` bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SectionDisplay {
    /// Render the section's rows on the primary page, as it always has.
    #[default]
    Inline,
    /// Render a summary tile on the primary page linking to the section's own
    /// page, where its rows and their charts are loaded on demand.
    Page,
}

impl SectionDisplay {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Page => "page",
        }
    }

    /// Anything unrecognised reads as `Inline`. A row written by a newer peer
    /// with a mode this build has never heard of must still render its checks;
    /// hiding the section instead would hide live monitoring.
    pub fn from_db(s: &str) -> Self {
        match s {
            "page" => Self::Page,
            _ => Self::Inline,
        }
    }
}

/// A named, ordered group on the status page. Stored separately from checks so
/// it can exist while empty and be renamed without touching what is in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Section {
    pub name: String,
    pub position: i32,
    /// Whether the public page renders this section collapsed on first load.
    /// The initial render state of an *inline* section, and meaningless for a
    /// paged one — a tile has nothing to collapse.
    pub collapsed: bool,
    /// Defaulted so a section document written by a peer that predates the mode
    /// still parses, rendering as it does today rather than failing the batch.
    #[serde(default)]
    pub display: SectionDisplay,
}

impl Section {
    /// Section names appear as headings and are matched against
    /// `checks.group_name`, so an empty or absurd one is rejected at the
    /// boundary. Shared by the coordinator's own admin API (`routes::sections`)
    /// and the global tier's (`global::store`): both accept the same
    /// submission from the same admin console.
    pub fn valid_name(name: &str) -> Result<String, String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err("section name is required".into());
        }
        if trimmed.chars().count() > 60 {
            return Err("section name must be 60 characters or fewer".into());
        }
        Ok(trimmed.to_string())
    }
}

/// Approximate gap between runs, for the legacy `interval_secs` column only.
/// Nothing reads it back; it exists so a rolled-back binary isn't left with a
/// zero interval.
fn cron_to_seconds(cron: &str) -> u64 {
    let Ok(schedule) = CheckConfig::parse_cron(cron) else {
        return 30;
    };
    let mut it = schedule.upcoming(chrono::Utc);
    match (it.next(), it.next()) {
        (Some(a), Some(b)) => (b - a).num_seconds().clamp(1, 86_400) as u64,
        _ => 30,
    }
}

/// Reliability and latency spread over one window.
///
/// `min`/`avg`/`max` cover *successful* attempts only: a failed probe's
/// recorded duration is however long it took to give up, not a measurement of
/// the service, and letting timeouts in makes the spread meaningless.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LatencyBand {
    pub attempts: u32,
    pub up: u32,
    pub min_ms: Option<u64>,
    pub max_ms: Option<u64>,
    sum_ms: f64,
}

impl LatencyBand {
    fn merge(&mut self, other: &Self) {
        self.attempts += other.attempts;
        self.up += other.up;
        self.sum_ms += other.sum_ms;
        self.min_ms = match (self.min_ms, other.min_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_ms = self.max_ms.max(other.max_ms);
    }

    /// Mean latency of the successful attempts, or `None` when none passed.
    pub fn avg_ms(&self) -> Option<f64> {
        (self.up > 0).then(|| self.sum_ms / self.up as f64)
    }
}

/// One agent's share of a window. Several agents probe the same check, so
/// "which agent was failing?" is only answerable per agent.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentBand {
    pub agent_id: String,
    pub band: LatencyBand,
}

/// An empty location is the same as none: an agent that sent "" told us nothing,
/// and storing it would create a second spelling of unknown.
fn non_empty(s: &str) -> Option<&str> {
    (!s.trim().is_empty()).then_some(s)
}

/// Return known UP and total duration for the half-open interval `[from, to)`.
/// `Unknown` is deliberately no-data, while Degraded follows the check's
/// already-resolved SLA setting.
fn uptime_counts(
    spans: &[StateSpan],
    from: i64,
    to: i64,
    degraded_is_downtime: bool,
) -> Option<(u64, u64)> {
    if spans.is_empty() || to <= from {
        return None;
    }

    let mut up_ms = 0u64;
    let mut known_ms = 0u64;
    for (i, span) in spans.iter().enumerate() {
        let span_end = spans.get(i + 1).map(|next| next.since).unwrap_or(i64::MAX);
        let start = span.since.max(from);
        let end = span_end.min(to);
        if end <= start || span.state == NodeState::Unknown {
            continue;
        }
        let duration = (end - start) as u64;
        known_ms = known_ms.saturating_add(duration);
        if span.state == NodeState::Up
            || (span.state == NodeState::Degraded && !degraded_is_downtime)
        {
            up_ms = up_ms.saturating_add(duration);
        }
    }
    (known_ms > 0).then_some((up_ms, known_ms))
}

/// Latency shape over a window plus exact per-agent attribution for it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LatencySeries {
    pub points: Vec<LatencyPoint>,
    pub overall: LatencyBand,
    pub agents: Vec<AgentBand>,
}

/// An incident as it replicates: the row plus everything that hangs off it.
/// Shipped whole rather than as three separate wire messages, because
/// `upsert_incident` has to apply the checks and updates in the same
/// transaction as the row or a reader could see an incident with no checks
/// yet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicatedIncident {
    pub id: String,
    pub version: Hlc,
    pub title: String,
    pub state: IncidentState,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
    pub global: bool,
    pub origin_check: Option<CheckId>,
    pub checks: Vec<(CheckId, AffectSource)>,
    pub updates: Vec<IncidentUpdateRow>,
}

/// One check's contribution to a results digest: what a peer needs to know to
/// decide whether it is missing anything for this check, without shipping the
/// rows themselves.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckDigest {
    pub check_id: CheckId,
    /// Oldest raw observation still retained for this check. This is maintained
    /// alongside the replication high-water mark, so digesting it never scans
    /// raw history.
    pub min_ts: i64,
    pub max_ts: i64,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncidentTombstone {
    pub id: String,
    pub deleted_at: i64,
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamDigest {
    pub max_millis: i64,
    pub max_id: String,
    pub count: i64,
}

/// A rollup table's contribution to a digest.
///
/// `StreamDigest` works for alerts/incidents/tombstones because those rows
/// are append-only: a row is written once and its identity and payload never
/// change again, so the newest timestamp plus a count is enough to tell two
/// replicas apart. A rollup row is not append-only — it is an upsert keyed on
/// `(check_id, agent_id, bucket_start)` whose `attempts`/`up`/`latency_sum`
/// keep growing for as long as its bucket accepts new raw observations.
/// Two peers can agree on `max_bucket_start` and `count` while their
/// counters disagree (one missed a batch of raw rows that landed in a bucket
/// both already hold), and a digest that could not see that would report
/// them converged when they are not.
///
/// `attempts_sum`/`up_sum`/`latency_sum_sum` are running totals across every
/// row in the table, maintained incrementally alongside the rollup rows
/// themselves (see `rollup_merge_from_raw_sql` and
/// `rollup_merge_from_backfill_sql`) rather than recomputed by scanning the
/// table on every digest. They work as a cheap checksum because every one of
/// them is a sum of non-negative contributions: each raw observation adds a
/// fixed, positive amount to exactly one bucket's counters exactly once, so
/// the totals are monotonic and independent of how writes were batched.
/// A bucket missing on one side therefore leaves at least one total strictly
/// smaller there — there is no way for two replicas holding different data to
/// coincidentally produce the same triple the way, say, two different
/// checksums of equal length could collide by construction. It is not
/// cryptographic, and does not need to be: like every other digest in this
/// module it exists to decide whether a backfill pull is worth making, not to
/// prove equality.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RollupDigest {
    pub max_bucket_start: i64,
    pub count: i64,
    pub attempts_sum: i64,
    pub up_sum: i64,
    pub latency_sum_sum: i64,
}

/// One rollup bucket, on the wire and as the unit `apply_minute_rollups` /
/// `apply_hour_rollups` merge. Shared by the minute and hour tables — they
/// differ only in bucket width, never in shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollupRow {
    pub check_id: CheckId,
    pub agent_id: String,
    pub bucket_start: i64,
    pub attempts: i64,
    pub up: i64,
    pub latency_sum: i64,
    pub latency_min: Option<i64>,
    pub latency_max: Option<i64>,
}

/// One standing raw-data gap: this node's retained history for `check_id`
/// ends at `from_ts`, `peer`'s begins at `to_ts`, and nothing between them is
/// held by either side. `observed_at` is when the condition was last seen,
/// not when it started — the gap is redetected every sweep it persists, and
/// each detection refreshes this row rather than appending a new one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawDataGapRow {
    pub check_id: CheckId,
    pub peer: String,
    pub from_ts: i64,
    pub to_ts: i64,
    pub observed_at: i64,
}

/// Which config table an entry belongs to. `kind` on `ConfigEntry` and
/// `config_tombstones`, and the discriminant a digest groups by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigKind {
    Check,
    Section,
    Setting,
    Credential,
    RollupPolicy,
}

impl ConfigKind {
    /// Every kind, for the callers that must sweep all of them — an untagged
    /// `GET /internal/config`, and config anti-entropy. Adding a variant above
    /// without adding it here would silently stop replicating that table, so
    /// the exhaustive match in `as_str` is the reminder to come back.
    pub const ALL: [ConfigKind; 5] = [
        ConfigKind::Check,
        ConfigKind::Section,
        ConfigKind::Setting,
        ConfigKind::Credential,
        ConfigKind::RollupPolicy,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigKind::Check => "check",
            ConfigKind::Section => "section",
            ConfigKind::Setting => "setting",
            ConfigKind::Credential => "credential",
            ConfigKind::RollupPolicy => "rollup_policy",
        }
    }

    /// `None` on anything unrecognised, unlike the other `from_db`s here: a
    /// config kind this binary doesn't know about must not be silently folded
    /// into one it does, or a tombstone could be applied against the wrong
    /// table.
    pub fn from_db(s: &str) -> Option<Self> {
        match s {
            "check" => Some(ConfigKind::Check),
            "section" => Some(ConfigKind::Section),
            "setting" => Some(ConfigKind::Setting),
            "credential" => Some(ConfigKind::Credential),
            "rollup_policy" => Some(ConfigKind::RollupPolicy),
            _ => None,
        }
    }

    /// The table this kind's rows live in, and the column that is its primary
    /// key — the one place that mapping is written, so every reader and
    /// writer of a config row agrees on it.
    fn table_and_pk(&self) -> (&'static str, &'static str) {
        match self {
            ConfigKind::Check => ("checks", "id"),
            ConfigKind::Section => ("sections", "name"),
            ConfigKind::Setting => ("settings", "key"),
            ConfigKind::Credential => ("agent_credentials", "id"),
            ConfigKind::RollupPolicy => ("rollup_policy", "node_id"),
        }
    }
}

/// What a replicated alert did on arrival: a new event, a replay of one already
/// held, or an id whose payload disagrees with the local event of the same id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertApply {
    Inserted,
    Replay,
    Conflict,
}

/// A row's contribution to a digest: enough to tell a peer whether it is
/// behind, without shipping every document. `max_hlc` covers rows and
/// tombstones together, since a peer missing a delete is just as behind as
/// one missing an edit.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigDigest {
    pub kind: ConfigKind,
    pub max_hlc: Option<Hlc>,
    pub count: i64,
}

/// A fixed-cost summary of everything replicated, for a peer to compare
/// against its own and ask only for what differs. `window_start` bounds the
/// results digest so it stays cheap against a table that can hold millions of
/// rows; alerts and incidents are windowed the same way for the same reason.
#[derive(Debug, Clone, PartialEq)]
pub struct Digest {
    pub checks: Vec<CheckDigest>,
    pub window_start: i64,
    pub alerts: StreamDigest,
    pub incidents: StreamDigest,
    pub incident_tombstones: StreamDigest,
    pub latency_minute_rollups: RollupDigest,
    pub latency_hour_rollups: RollupDigest,
    pub config: Vec<ConfigDigest>,
}

/// One replicated config row, on the wire and as the unit `apply_config_entry`
/// decides about. `document` is the whole row as JSON — never a hand-picked
/// subset of fields, so a column this binary doesn't know about yet still
/// round-trips through a node that does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub kind: ConfigKind,
    /// The row's primary key: check name, section name, setting key,
    /// credential id, or rollup node id.
    pub id: String,
    pub hlc: Hlc,
    /// The whole row as a JSON document. `None` iff this is a tombstone.
    pub document: Option<String>,
}

impl ConfigEntry {
    pub fn is_tombstone(&self) -> bool {
        self.document.is_none()
    }
}

/// Mirrors `AgentCredential` for the wire only. `credential.rs` does not
/// derive `Serialize` — it is owned by a different concern and adding one
/// there is out of scope here — so this is the envelope's own copy of the
/// shape. It carries hashes and salts, never a plaintext secret, because
/// there is no plaintext left after mint to leak. Pinned by a round-trip test
/// that fails the moment the two struct shapes drift apart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(in crate::coordinator) struct RollupPolicyDoc {
    pub node_type: NodeType,
    pub policy: crate::rollup::RollupPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CredentialDoc {
    id: String,
    zone: String,
    region: String,
    secret_hash: String,
    salt: String,
    created_at: i64,
    revoked_at: Option<i64>,
}

impl From<&AgentCredential> for CredentialDoc {
    // Destructured exhaustively, with no `..`: a field added to
    // `AgentCredential` makes this fail to compile instead of silently
    // vanishing off the wire the next time a credential replicates.
    fn from(c: &AgentCredential) -> Self {
        let AgentCredential {
            id,
            zone,
            region,
            secret_hash,
            salt,
            created_at,
            revoked_at,
        } = c.clone();
        CredentialDoc {
            id,
            zone,
            region,
            secret_hash,
            salt,
            created_at,
            revoked_at,
        }
    }
}

impl From<CredentialDoc> for AgentCredential {
    fn from(c: CredentialDoc) -> Self {
        let CredentialDoc {
            id,
            zone,
            region,
            secret_hash,
            salt,
            created_at,
            revoked_at,
        } = c;
        AgentCredential {
            id,
            zone,
            region,
            secret_hash,
            salt,
            created_at,
            revoked_at,
        }
    }
}

/// Who last polled an agent, and how fast — the persisted twin of
/// `Inner::seen`, so a cluster of coordinators agrees on fleet health instead
/// of each holding its own view in memory.
///
/// `window_ms` travels with the row rather than being applied by the reader:
/// liveness is `now - at <= window_ms`, and the window is the cadence the
/// agent declared to *whichever* node it polled. A peer applying its own
/// `missed_polls` to someone else's agent would read an agent polling every
/// two minutes as unreachable while it is polling happily.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentPollRow {
    pub agent_id: String,
    pub zone: String,
    pub region: String,
    pub at: i64,
    pub window_ms: i64,
    pub rtt_ms: Option<i64>,
    /// What the agent reported it is running, for the same reason the rest of
    /// this row travels: an operator on any node should see the whole fleet,
    /// not only the agents that happen to poll the node they opened.
    #[serde(default)]
    pub build: Option<String>,
    /// Check kinds the agent declared, comma-separated as it sent them.
    ///
    /// `None` means it never declared — read as "anything", never as
    /// "nothing". Stored as the wire string rather than parsed: a peer may
    /// legitimately name a kind this node has never heard of, and reducing that
    /// to the kinds this binary knows would make an agent look less capable on
    /// one node than on another for no reason but build order.
    #[serde(default)]
    pub kinds: Option<String>,
}

/// Column names DuckDB currently has for `table`, used by `verify_schema` to
/// diff an opened database against the schema the DDL constant declares.
fn table_column_names(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT column_name FROM information_schema.columns WHERE table_name = ?")?;
    let rows = stmt.query_map(duckdb::params![table], |r| r.get::<_, String>(0))?;
    rows.collect::<std::result::Result<_, _>>()
        .map_err(Into::into)
}

/// Compare an opened database's tables and columns against what `SCHEMA_DDL`
/// declares, by applying the same DDL to a scratch in-memory connection and
/// diffing `information_schema.columns` rather than hand-maintaining a second
/// list that could drift from the DDL itself.
///
/// A missing table or column fails clearly: this project runs no in-place
/// migrations before 1.0 (see SCALING.md's "Pre-1.0 database policy"), so a
/// database that predates a column this binary requires must be rebuilt, not
/// silently read as though the column were there. Extra columns — a rollback
/// to an older binary reopening a newer database — are tolerated.
fn verify_schema(conn: &Connection) -> Result<()> {
    let reference = Connection::open_in_memory()?;
    reference.execute_batch(SCHEMA_DDL)?;
    let tables: Vec<String> = {
        let mut stmt = reference.prepare(
            "SELECT table_name FROM information_schema.tables
             WHERE table_schema = 'main' AND table_type = 'BASE TABLE'",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    for table in tables {
        let expected = table_column_names(&reference, &table)?;
        let actual = table_column_names(conn, &table)?;
        for column in &expected {
            if !actual.iter().any(|c| c == column) {
                bail!(
                    "database schema is missing column `{table}.{column}`: this database \
                     predates the supported schema and cannot be opened. See SCALING.md's \
                     \"Pre-1.0 database policy\" for the rebuild procedure."
                );
            }
        }
    }
    Ok(())
}

/// Merge `ingest_agent_stage` into `agent_check_state`, keeping the newest
/// belief per `(check_id, agent_id)`. One statement, shared by local ingest and
/// replication so the two cannot drift on what "newest" means.
const AGENT_STATE_MERGE_SQL: &str = "INSERT INTO agent_check_state
     SELECT check_id, agent_id, status, consecutive_failures, zone, region, last_check
     FROM ingest_agent_stage
     QUALIFY row_number() OVER (
         PARTITION BY check_id, agent_id ORDER BY last_check DESC, ordinal DESC
     ) = 1
     ON CONFLICT (check_id, agent_id) DO UPDATE SET
         status = excluded.status,
         consecutive_failures = excluded.consecutive_failures,
         zone = excluded.zone,
         region = excluded.region,
         last_check = excluded.last_check
     WHERE excluded.last_check >= agent_check_state.last_check;";

/// Fold rows just written to `check_results` into the tables derived from them:
/// the anti-entropy watermark and the per-check latest result the status page
/// reads. `staged` names the table holding exactly the rows that were inserted
/// — every row in it is counted, so a caller that deduplicates must pass the
/// surviving rows rather than everything it was offered.
fn derived_results_sql(staged: &str) -> String {
    format!(
        "{minute}
         {hour}
         INSERT INTO result_replication_watermarks
         SELECT check_id, MIN(ts_millis), MAX(ts_millis), COUNT(*) FROM {staged} GROUP BY check_id
         ON CONFLICT (check_id) DO UPDATE SET
             min_ts = LEAST(result_replication_watermarks.min_ts, excluded.min_ts),
             max_ts = GREATEST(result_replication_watermarks.max_ts, excluded.max_ts),
             result_count = result_replication_watermarks.result_count + excluded.result_count;
         INSERT INTO latest_check_results
         SELECT check_id, agent_id, ts_millis, status, latency_ms, status_code, error, failure
         FROM {staged}
         QUALIFY row_number() OVER (
             PARTITION BY check_id ORDER BY ts_millis DESC, agent_id DESC
         ) = 1
         ON CONFLICT (check_id) DO UPDATE SET
             agent_id = excluded.agent_id,
             ts_millis = excluded.ts_millis,
             status = excluded.status,
             latency_ms = excluded.latency_ms,
             status_code = excluded.status_code,
             error = excluded.error,
             failure = excluded.failure
         WHERE excluded.ts_millis > latest_check_results.ts_millis
            OR (excluded.ts_millis = latest_check_results.ts_millis
                AND excluded.agent_id > latest_check_results.agent_id);",
        minute = rollup_merge_from_raw_sql(
            "check_latency_minute_rollups",
            "check_latency_minute_rollups",
            MILLIS_PER_MINUTE,
            staged
        ),
        hour = rollup_merge_from_raw_sql(
            "check_latency_hour_rollups",
            "check_latency_hour_rollups",
            MILLIS_PER_HOUR,
            staged
        ),
    )
}

/// Bucket `staged` raw rows into one row per `(check_id, agent_id,
/// bucket_start)`, `bucket_ms` wide. The shared shape behind both
/// `rollup_merge_from_raw_sql` and `rebuild_latency_rollups` — one place that
/// decides what a bucket's columns mean.
fn bucketed_raw_rows_sql(staged: &str, bucket_ms: i64) -> String {
    format!(
        "SELECT check_id, agent_id, (ts_millis // {bucket_ms}) * {bucket_ms} AS bucket_start,
                COUNT(*) AS attempts, COUNT(*) FILTER (WHERE status = 'up') AS up,
                COALESCE(SUM(latency_ms) FILTER (WHERE status = 'up'), 0) AS latency_sum,
                MIN(latency_ms) FILTER (WHERE status = 'up') AS latency_min,
                MAX(latency_ms) FILTER (WHERE status = 'up') AS latency_max
         FROM {staged} GROUP BY 1, 2, 3"
    )
}

/// Fold newly-inserted raw rows in `staged` into rollup `table`, maintaining
/// `rollup_replication_watermarks` for `stream` in the same statement batch.
///
/// The watermark insert runs *before* the rollup upsert below it and reads
/// `table` as it stood before this batch touched it, so `row_count` only
/// grows for a `(check_id, agent_id, bucket_start)` this table did not
/// already hold — merging into an existing bucket must not inflate the
/// digest's notion of how many buckets exist.
///
/// Summing `attempts`/`up`/`latency_sum` straight into both the watermark and
/// the rollup row is sound here specifically because `staged` holds rows this
/// node has never folded into a rollup before: `derived_results_sql`'s own
/// contract is that `staged` names exactly the rows just inserted into
/// `check_results`, and a raw row is inserted at most once, ever (dedup runs
/// before this is called, both for local ingest and for replicated results).
/// There is no replay of `staged` to be idempotent against — replaying would
/// mean inserting the same raw row into `check_results` twice, which is the
/// bug this merge is trusting elsewhere not to happen. Compare
/// `rollup_merge_from_backfill_sql`, which cannot make that assumption about
/// a page received from a peer and uses `GREATEST` instead of addition for
/// exactly that reason.
fn rollup_merge_from_raw_sql(table: &str, stream: &str, bucket_ms: i64, staged: &str) -> String {
    let bucketed = bucketed_raw_rows_sql(staged, bucket_ms);
    format!(
        "INSERT INTO rollup_replication_watermarks
             (stream, max_bucket_start, row_count, attempts_sum, up_sum, latency_sum_sum)
         SELECT '{stream}', COALESCE(MAX(b.bucket_start), 0),
                COUNT(*) FILTER (WHERE r.check_id IS NULL),
                COALESCE(SUM(b.attempts), 0), COALESCE(SUM(b.up), 0), COALESCE(SUM(b.latency_sum), 0)
         FROM ({bucketed}) b
         LEFT JOIN {table} r
           ON r.check_id = b.check_id AND r.agent_id = b.agent_id AND r.bucket_start = b.bucket_start
         HAVING COUNT(*) > 0
         ON CONFLICT (stream) DO UPDATE SET
             max_bucket_start = GREATEST(rollup_replication_watermarks.max_bucket_start, excluded.max_bucket_start),
             row_count = rollup_replication_watermarks.row_count + excluded.row_count,
             attempts_sum = rollup_replication_watermarks.attempts_sum + excluded.attempts_sum,
             up_sum = rollup_replication_watermarks.up_sum + excluded.up_sum,
             latency_sum_sum = rollup_replication_watermarks.latency_sum_sum + excluded.latency_sum_sum;
         INSERT INTO {table}
         SELECT * FROM ({bucketed}) b
         ON CONFLICT (check_id, agent_id, bucket_start) DO UPDATE SET
             attempts = {table}.attempts + excluded.attempts,
             up = {table}.up + excluded.up,
             latency_sum = {table}.latency_sum + excluded.latency_sum,
             latency_min = LEAST({table}.latency_min, excluded.latency_min),
             latency_max = GREATEST({table}.latency_max, excluded.latency_max);"
    )
}

/// Merge `staged` rollup rows received from a peer's backfill page into
/// rollup `table`, maintaining `rollup_replication_watermarks` for `stream`
/// alongside them.
///
/// This is deliberately not the same arithmetic as
/// `rollup_merge_from_raw_sql`. A backfilled row is not a delta of raw
/// observations this node has not counted yet — it is a peer's *total* for a
/// bucket, and this node has no way to tell whether that total overlaps with
/// what it already holds (both could have derived the same bucket from raw
/// independently, or one could be a strict superset of the other after a
/// retention gap). Summing them could double-count; the only combination
/// that is always safe is to keep whichever side's counters are larger,
/// field by field. `GREATEST` is commutative, associative and idempotent —
/// applying the same page once, twice, or interleaved with another peer's
/// page all converge on the same row, which is exactly what "receiving the
/// same page twice must not inflate `attempts`" requires. `latency_min`/
/// `latency_max` already used `LEAST`/`GREATEST` for the same reason, before
/// this function existed.
///
/// The one thing this trades away: if this node's own raw-derived total for
/// a live bucket is briefly ahead of the peer's on some fields and behind on
/// others (only possible for a bucket still accepting new raw on both sides),
/// `GREATEST` keeps the larger of each rather than one side's whole row. That
/// is a non-issue for the buckets this function exists to recover — closed,
/// out-of-raw-retention buckets that no longer change — and merely harmless
/// for a live one, which the raw-derived path keeps advancing regardless.
fn rollup_merge_from_backfill_sql(table: &str, stream: &str, staged: &str) -> String {
    format!(
        "INSERT INTO rollup_replication_watermarks
             (stream, max_bucket_start, row_count, attempts_sum, up_sum, latency_sum_sum)
         SELECT '{stream}', COALESCE(MAX(s.bucket_start), 0),
                COUNT(*) FILTER (WHERE r.check_id IS NULL),
                COALESCE(SUM(GREATEST(s.attempts, COALESCE(r.attempts, 0)) - COALESCE(r.attempts, 0)), 0),
                COALESCE(SUM(GREATEST(s.up, COALESCE(r.up, 0)) - COALESCE(r.up, 0)), 0),
                COALESCE(SUM(GREATEST(s.latency_sum, COALESCE(r.latency_sum, 0)) - COALESCE(r.latency_sum, 0)), 0)
         FROM {staged} s
         LEFT JOIN {table} r
           ON r.check_id = s.check_id AND r.agent_id = s.agent_id AND r.bucket_start = s.bucket_start
         HAVING COUNT(*) > 0
         ON CONFLICT (stream) DO UPDATE SET
             max_bucket_start = GREATEST(rollup_replication_watermarks.max_bucket_start, excluded.max_bucket_start),
             row_count = rollup_replication_watermarks.row_count + excluded.row_count,
             attempts_sum = rollup_replication_watermarks.attempts_sum + excluded.attempts_sum,
             up_sum = rollup_replication_watermarks.up_sum + excluded.up_sum,
             latency_sum_sum = rollup_replication_watermarks.latency_sum_sum + excluded.latency_sum_sum;
         INSERT INTO {table}
         SELECT * FROM {staged}
         ON CONFLICT (check_id, agent_id, bucket_start) DO UPDATE SET
             attempts = GREATEST({table}.attempts, excluded.attempts),
             up = GREATEST({table}.up, excluded.up),
             latency_sum = GREATEST({table}.latency_sum, excluded.latency_sum),
             latency_min = LEAST({table}.latency_min, excluded.latency_min),
             latency_max = GREATEST({table}.latency_max, excluded.latency_max);"
    )
}

/// What a batch of config entries is arbitrated against: the newest stamp each
/// `(kind, id)` currently carries here, and which of them carry a tombstone at
/// all — a delete that is not there does not need clearing.
#[derive(Default)]
struct ConfigStamps {
    newest: std::collections::HashMap<(ConfigKind, String), Hlc>,
    tombstoned: std::collections::HashSet<(ConfigKind, String)>,
}

/// The complete table schema, applied verbatim to every opened database.
/// `CREATE TABLE IF NOT EXISTS` is a no-op wherever a table already exists,
/// so this changes nothing on a currently-running production database: every
/// column below was, at some point, added by an `ALTER TABLE` this project
/// used to run on `open`, and any database that has been opened by a build
/// carrying that `ALTER` already has the column on disk. What's new is that
/// a database that predates one of these columns is no longer silently
/// patched up — see `verify_schema`, and SCALING.md's "Pre-1.0 database
/// policy".
const SCHEMA_DDL: &str = "CREATE TABLE IF NOT EXISTS check_results (
    check_id   UUID    NOT NULL,
    agent_id   VARCHAR NOT NULL,
    ts_millis  BIGINT  NOT NULL,
    status     VARCHAR NOT NULL,
    latency_ms BIGINT  NOT NULL,
    status_code INTEGER,
    error      VARCHAR,
    -- Where the probe ran. Nullable: results recorded before agents
    -- declared a location read as unknown rather than being guessed.
    zone       VARCHAR,
    region     VARCHAR,
    -- Why it failed, as a value. The public page renders this rather
    -- than the free text, which quotes whatever the probe saw.
    failure    VARCHAR
);
-- One row per check, probe domain, and UTC minute/hour. `agent_id`
-- is the independent observation domain: replicas in a domain must
-- deduplicate, while distinct domains must never be averaged away.
CREATE TABLE IF NOT EXISTS check_latency_minute_rollups (
    check_id UUID NOT NULL,
    agent_id VARCHAR NOT NULL,
    bucket_start BIGINT NOT NULL,
    attempts BIGINT NOT NULL,
    up BIGINT NOT NULL,
    latency_sum BIGINT NOT NULL,
    latency_min BIGINT,
    latency_max BIGINT,
    PRIMARY KEY (check_id, agent_id, bucket_start)
);
CREATE TABLE IF NOT EXISTS check_latency_hour_rollups (
    check_id UUID NOT NULL,
    agent_id VARCHAR NOT NULL,
    bucket_start BIGINT NOT NULL,
    attempts BIGINT NOT NULL,
    up BIGINT NOT NULL,
    latency_sum BIGINT NOT NULL,
    latency_min BIGINT,
    latency_max BIGINT,
    PRIMARY KEY (check_id, agent_id, bucket_start)
);
CREATE TABLE IF NOT EXISTS latest_check_results (
    check_id    UUID PRIMARY KEY,
    agent_id    VARCHAR NOT NULL,
    ts_millis   BIGINT NOT NULL,
    status      VARCHAR NOT NULL,
    latency_ms  BIGINT NOT NULL,
    status_code INTEGER,
    error       VARCHAR,
    failure     VARCHAR
);
CREATE TABLE IF NOT EXISTS result_replication_watermarks (
    check_id     UUID PRIMARY KEY,
    min_ts       BIGINT NOT NULL,
    max_ts       BIGINT NOT NULL,
    result_count BIGINT NOT NULL
);
-- One row per rollup table ('check_latency_minute_rollups' or
-- '..._hour_rollups'), maintained on every write instead of scanned
-- on every digest — the same reason `result_replication_watermarks`
-- exists instead of aggregating `check_results` per sweep, and more
-- pointed here: a rollup bucket lives for the whole retention tier
-- (90 days/13 months), so the table this summarizes is not small.
-- See `RollupDigest` for why a rollup needs more than max/count to
-- be compared safely.
CREATE TABLE IF NOT EXISTS rollup_replication_watermarks (
    stream           VARCHAR PRIMARY KEY,
    max_bucket_start BIGINT NOT NULL,
    row_count        BIGINT NOT NULL,
    attempts_sum     BIGINT NOT NULL,
    up_sum           BIGINT NOT NULL,
    latency_sum_sum  BIGINT NOT NULL
);
-- `rollup_digest` is a point lookup, not a scan, so both rows must exist
-- from the first open onward rather than only once something has been
-- rolled up. Harmless to repeat: an existing row (real counters, on every
-- database that has ever ingested a result) is left untouched.
INSERT INTO rollup_replication_watermarks
    (stream, max_bucket_start, row_count, attempts_sum, up_sum, latency_sum_sum)
VALUES ('check_latency_minute_rollups', 0, 0, 0, 0, 0),
       ('check_latency_hour_rollups', 0, 0, 0, 0, 0)
ON CONFLICT (stream) DO NOTHING;
-- A durable record of `anti_entropy_sweep` finding a peer whose
-- retained raw history starts after ours ends: nothing anti-entropy
-- can do closes that hole in raw detail, so it is written down
-- rather than only logged and counted. `(check_id, peer)` is the
-- key rather than an append-only id, because the same standing gap
-- is redetected every sweep it persists and a row per sweep would
-- grow without bound; each detection instead refreshes the one row
-- for that pairing. See SCALING.md's Data retention tiers section for
-- what recovery does and does not mean once minute/hour rollups
-- replicate on their own (item A alongside this table).
CREATE TABLE IF NOT EXISTS raw_data_gaps (
    check_id    UUID    NOT NULL,
    peer        VARCHAR NOT NULL,
    from_ts     BIGINT  NOT NULL,
    to_ts       BIGINT  NOT NULL,
    observed_at BIGINT  NOT NULL,
    PRIMARY KEY (check_id, peer)
);
-- The event log: one row per confirmed state transition.
-- `incident_id` is stamped as the row is written, because
-- membership is a fact the coordinator knows at that moment and a
-- reader cannot re-derive it — a time-window join cannot tell two
-- overlapping causes apart, and it would silently absorb unrelated
-- history the moment a check joined an incident's affects list.
-- No `alert_type` column: StateChange/Degradation/Recovery is a
-- function of the two state columns, so storing it invites drift.
CREATE TABLE IF NOT EXISTS replication_stream_watermarks (
    stream      VARCHAR PRIMARY KEY,
    max_millis  BIGINT NOT NULL,
    max_id      VARCHAR NOT NULL,
    item_count  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts (
    id          VARCHAR PRIMARY KEY,
    scope       VARCHAR NOT NULL,
    node_id     VARCHAR NOT NULL,
    -- NULL for a first-ever observation: nothing to move from.
    previous_state VARCHAR,
    current_state  VARCHAR NOT NULL,
    ts_millis   BIGINT  NOT NULL,
    incident_id VARCHAR
);
-- The operator-facing record of an outage. `state` is the ONLY
-- lifecycle field: open means state <> 'resolved'. Duration is
-- derived, never stored, and so is acknowledgement: that is the
-- existence of an incident_updates row with an author, not a
-- column here.
CREATE TABLE IF NOT EXISTS incidents (
    id          VARCHAR PRIMARY KEY,
    title       VARCHAR NOT NULL,
    -- detected, investigating, identified, monitoring, resolved.
    -- Automatic incidents start at 'detected': a probe noticing is
    -- not a human investigating.
    state       VARCHAR NOT NULL,
    started_at  BIGINT  NOT NULL,
    resolved_at BIGINT,
    global      BOOLEAN NOT NULL DEFAULT FALSE,
    -- NULL for a manually opened incident, which reconciliation
    -- therefore never touches.
    origin_check UUID,
    version_millis BIGINT NOT NULL,
    version_node VARCHAR NOT NULL
);
-- Which checks an incident affects. `source` (origin, dependency,
-- manual) exists so reconciliation can retract what it added
-- without disturbing what an operator added by hand.
CREATE TABLE IF NOT EXISTS incident_checks (
    incident_id VARCHAR NOT NULL,
    check_id    UUID    NOT NULL,
    source      VARCHAR NOT NULL,
    PRIMARY KEY (incident_id, check_id)
);
-- The narrative. Every state change is one of these, including the
-- two the coordinator writes itself on auto-open and auto-resolve,
-- so the timeline can never disagree with the state. A non-NULL
-- author is what makes an update an acknowledgement.
CREATE TABLE IF NOT EXISTS incident_updates (
    id          VARCHAR PRIMARY KEY,
    incident_id VARCHAR NOT NULL,
    state       VARCHAR NOT NULL,
    body        VARCHAR NOT NULL,
    author      VARCHAR,
    created_at  BIGINT  NOT NULL
);
-- The CONFIRMED state per check, materialized for fast reads. The
-- raw `check_results` table keeps every observation; this table
-- holds what the coordinator decided they mean — a check with
-- `failures_before_down` = 3 stays 'up' here through its first two
-- failed probes. Read by the status page, seeded into the in-memory
-- tracker on startup so a restart resumes a confirmation in flight.
--
-- `pending_state`/`pending_since` are the hold-down timer, persisted
-- so it survives a restart: without `pending_since` a node rebooting
-- mid-window would restart the window, and a coordinator restarting
-- more often than the hold-down could suppress an outage indefinitely.
-- `pending_state` sits beside it because \"something has been pending
-- for 90 seconds\" alone can't say whether a newly-arrived Down
-- inherits that clock or restarts it.
CREATE TABLE IF NOT EXISTS current_state (
    check_id   UUID PRIMARY KEY,
    status     VARCHAR NOT NULL,
    last_check BIGINT  NOT NULL,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    pending_state VARCHAR,
    pending_since BIGINT
);
-- One agent's confirmed opinion of one check: the leaves of the
-- region tree. `current_state` above is the rolled-up conclusion;
-- this is the evidence it was rolled up from.
--
-- Persisted for the same reason `current_state` is — so a restart
-- resumes confirmations rather than restarting them — but *per
-- agent*, because that is now where `failures_before_down` is
-- counted. Without this, a restart would reset every agent's
-- in-flight confirmation at once and a real outage would take
-- another full threshold to re-confirm.
CREATE TABLE IF NOT EXISTS agent_check_state (
    check_id   UUID    NOT NULL,
    agent_id   VARCHAR NOT NULL,
     status     VARCHAR NOT NULL,
     consecutive_failures INTEGER NOT NULL DEFAULT 0,
     zone       VARCHAR NOT NULL DEFAULT 'unknown',
     region     VARCHAR NOT NULL DEFAULT 'unknown',
     last_check BIGINT  NOT NULL,
    PRIMARY KEY (check_id, agent_id)
);
-- Rollup policy per tree node. Deliberately NOT named `region_tree`
-- (design doc :1348), because it does not store the tree: structure
-- is derived from agent credentials, which cannot drift from the
-- real fleet the way a declared topology can. Only the policy — a
-- genuine operator decision that nothing else can supply — is
-- stored here.
--
-- `node_id` is a namespaced key: 'global', 'region:us-east',
-- 'zone:us-east-1a'. A node with no row uses the built-in default
-- for its level.
--
-- (updated_at, updated_by) makes this a citizen of the HLC
-- last-writer-wins config stream alongside checks, sections,
-- settings and credentials. A policy that existed only on whichever
-- coordinator an admin happened to hit would make two nodes
-- disagree about whether a service is down, which is precisely the
-- disagreement clustering exists to prevent.
CREATE TABLE IF NOT EXISTS rollup_policy (
    node_id       VARCHAR PRIMARY KEY,
    node_type     VARCHAR NOT NULL,
    degraded_when VARCHAR NOT NULL,
    down_when     VARCHAR NOT NULL,
    -- JSON, e.g. {\"degraded_q\":2,\"down_q\":3}. Quorum counts live
    -- here rather than in the threshold strings so those stay a
    -- closed vocabulary.
    config        VARCHAR,
    updated_at    BIGINT  NOT NULL,
    updated_by    VARCHAR
);
-- One row per published transition: the state a check was in, and
-- when it entered it. The open span is the row with the greatest
-- `since`.
--
-- This exists because uptime was being computed as the UP fraction
-- of raw result rows, which is wrong twice over: it is agent-blind
-- (a check probed by four agents with one permanently broken reads
-- 75% forever) and it is count-weighted, so changing a check's
-- interval silently reweights its history. Spans are
-- time-weighted, agree with what the status page actually showed,
-- and can express whether Degraded counted as downtime.
CREATE TABLE IF NOT EXISTS state_history (
    check_id UUID    NOT NULL,
    state    VARCHAR NOT NULL,
    since    BIGINT  NOT NULL,
    PRIMARY KEY (check_id, since)
);
-- Check definitions, editable at runtime from the admin UI. Seeded
-- from the config file on first boot; the table is authoritative
-- afterwards, so an edit isn't silently reverted by a restart.
-- `id` is the immutable identity — a UUIDv7 minted once, by the
-- application, and never reassigned; `name` is the mutable,
-- initially-unique display value everything else here is keyed
-- against. `group` is a SQL keyword, hence group_name.
CREATE TABLE IF NOT EXISTS checks (
    id          UUID PRIMARY KEY,
    name        VARCHAR NOT NULL UNIQUE,
    kind        VARCHAR NOT NULL,
    target      VARCHAR NOT NULL,
    interval_secs BIGINT NOT NULL,
    timeout_secs  BIGINT NOT NULL,
    group_name  VARCHAR,
    latency_graph BOOLEAN NOT NULL,
    enabled     BOOLEAN NOT NULL,
    -- Published on the public status page. Separate from `enabled`:
    -- a check can be probed and alerted on without being shown.
    visible     BOOLEAN NOT NULL DEFAULT TRUE,
    -- Lua deciding UP/DOWN. NULL means the built-in rule.
    eval        VARCHAR,
    -- Display order on the status page.
    position    INTEGER DEFAULT 0,
    -- JSON rather than a comma-joined string: an agent id is arbitrary
    -- config text and a separator inside one would silently split it.
    agents      VARCHAR,
    -- The whole definition, serialized. A check now carries per-type
    -- option blocks, headers and schedules; a column per field was a
    -- migration treadmill, so the document is authoritative and the
    -- flat columns here survive only for SQL to filter and order by.
    config      VARCHAR,
    expect_status INTEGER,
    expect_body_contains VARCHAR,
    -- (updated_at, updated_by) is the HLC this row was last written
    -- at. It is the whole conflict-resolution story for clustered
    -- config: a higher stamp wins, ties break on the node id, and
    -- every node therefore picks the same winner without asking.
    updated_at  BIGINT  NOT NULL,
    updated_by  VARCHAR
);
-- One row per region a check's `placement.regions` explicitly
-- names, projected from `checks.config` the same way the flat
-- columns above are — so deciding which checks a region's
-- placement admits is a join, never a per-check JSON parse. A
-- check with no rows here has an empty `placement.regions`,
-- which `Placement` already treats as every region; queries
-- follow that same rule. See SCALING.md's Placement-scoped
-- configuration section, Phase 2: this is what lets a region's
-- desired set be computed without reading every check document
-- on every reconciliation sweep.
CREATE TABLE IF NOT EXISTS check_placement_regions (
    check_id UUID    NOT NULL,
    region   VARCHAR NOT NULL,
    PRIMARY KEY (check_id, region)
);
-- Status-page sections. A row here, rather than just a string on
-- each check, so a section can be created empty, renamed, and
-- ordered independently of what is in it.
CREATE TABLE IF NOT EXISTS sections (
    name     VARCHAR PRIMARY KEY,
    position INTEGER NOT NULL,
    collapsed BOOLEAN DEFAULT FALSE,
    -- 'inline' or 'page'. Stored as text rather than a boolean so
    -- a third mode does not need a second column.
    display VARCHAR DEFAULT 'inline',
    -- HLC, as on `checks`.
    updated_at BIGINT,
    updated_by VARCHAR
);
-- Per-zone agent credentials. Only the hash is kept: the plaintext
-- is returned once at mint and is not reconstructible from this.
-- `zone`/`region` live here rather than being asserted by the agent,
-- because placement trusts an agent's claimed location.
CREATE TABLE IF NOT EXISTS agent_credentials (
    id          VARCHAR PRIMARY KEY,
    zone        VARCHAR NOT NULL,
    region      VARCHAR NOT NULL,
    secret_hash VARCHAR NOT NULL,
    salt        VARCHAR NOT NULL,
    created_at  BIGINT  NOT NULL,
    -- Kept after revocation so an operator can still see that the
    -- credential existed and when it stopped working.
    revoked_at  BIGINT,
    -- HLC. Revocation replicates as an ordinary LWW update, which
    -- is what makes a revoked credential stop working fleet-wide
    -- rather than only on the node it was revoked from.
    updated_at  BIGINT,
    updated_by  VARCHAR
);
-- Page-level settings edited in the admin UI. One JSON document per
-- key: they are small, always read whole, and never filtered on
-- their internals, so a column per field would buy nothing but
-- migrations. The config file only seeds a key that is absent.
CREATE TABLE IF NOT EXISTS settings (
    key   VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL,
    -- HLC, as on `checks`. Whole-document LWW per key, which is
    -- what these already were locally — now across nodes.
    updated_at BIGINT,
    updated_by VARCHAR
);
-- Notifications that have not been delivered yet. A row exists
-- *because* a send failed: success deletes it, so an empty table is
-- the healthy state and there is no 'sent' row to prune.
--
-- It stores the transition to re-send rather than an alert id: the
-- retry needs check and states, nothing else, and a foreign key into
-- a table with its own retention window would dangle. No surrogate
-- Every delivery has an immutable id, so distinct transitions at
-- the same millisecond cannot replace one another.
--
-- `next_attempt` NULL means given up: attempts are exhausted and
-- nobody was paged. That is derived state made explicit, rather than
-- a `status` column that can disagree with `attempts`.
CREATE TABLE IF NOT EXISTS notification_queue (
    id             VARCHAR PRIMARY KEY,
    notifier       VARCHAR NOT NULL,
    check_id       UUID    NOT NULL,
    previous_state VARCHAR,
    current_state  VARCHAR NOT NULL,
    attempts       INTEGER NOT NULL,
    next_attempt   BIGINT,
    last_error     VARCHAR NOT NULL,
    queued_at      BIGINT  NOT NULL
);
-- Who else is in the cluster. Persisted rather than kept in memory
-- so a restarted node rejoins from what it already knew instead of
-- waiting to be re-seeded by whichever peer pings it first.
CREATE TABLE IF NOT EXISTS cluster_members (
    node_id    VARCHAR PRIMARY KEY,
    url        VARCHAR NOT NULL,
    -- Process start time. A restart supersedes stale gossip about
    -- the old process, which is what stops a returning node from
    -- flapping between alive and dead as old views arrive.
    generation BIGINT  NOT NULL,
    last_seen  BIGINT  NOT NULL,
    state      VARCHAR NOT NULL    -- alive | suspect | dead
);
-- Config deletes are themselves replicated state. Without this row
-- a peer's anti-entropy sweep sees a check we do not have, decides
-- we are behind, and hands the deleted check straight back.
-- `kind` names the config table, `id` its primary key.
CREATE TABLE IF NOT EXISTS config_tombstones (
    kind       VARCHAR NOT NULL,   -- check | section | setting | credential | rollup_policy
    id         VARCHAR NOT NULL,
    deleted_at BIGINT  NOT NULL,   -- HLC millis
    node_id    VARCHAR NOT NULL,   -- HLC tiebreak
    PRIMARY KEY (kind, id)
);
-- Who last polled which agent, and how fast. This used to live only
-- in `Inner::seen`, in memory, so two clustered coordinators
-- disagreed about who was actually polling; persisting it here
-- means replication carries fleet health for free instead of each
-- node needing its own gossip message for it.
CREATE TABLE IF NOT EXISTS agent_polls (
    agent_id  VARCHAR PRIMARY KEY,
    zone      VARCHAR NOT NULL,
    region    VARCHAR NOT NULL,
    -- `at` is a reserved word in DuckDB's SQL grammar, hence the
    -- column name differing from the Rust field it backs.
    polled_at BIGINT  NOT NULL,
    window_ms BIGINT  NOT NULL,
    rtt_ms    BIGINT,
    -- What the agent said it is, on its last poll to whichever node
    -- it polls. Nullable: an agent predating either declaration
    -- reports neither, and absent must not read as none-at-all.
    build     VARCHAR,
    -- Comma-separated wire names, stored unparsed so a kind this
    -- node has never heard of survives the round trip.
    declared_kinds VARCHAR
);
-- An incident delete is replicated state for the same reason a
-- config delete is: an absence alone cannot be told apart from
-- never having heard of it, so a peer's next push or the
-- reconciler would hand the deleted incident straight back.
CREATE TABLE IF NOT EXISTS incident_tombstones (
    id         VARCHAR PRIMARY KEY,
    deleted_at BIGINT  NOT NULL,
    node_id    VARCHAR NOT NULL
);";

impl Storage {
    /// Open (or create) the database at `path` and ensure the schema exists.
    /// `path` may be ":memory:" for tests, which skips `verify_schema` below:
    /// a fresh in-memory database is created from `SCHEMA_DDL` itself, so it
    /// always matches.
    ///
    /// A database created before native-UUID check identity, or missing any
    /// table or column `SCHEMA_DDL` declares, is not opened and upgraded in
    /// place — it is unsupported and must be rebuilt. Check identity is
    /// cluster-shared state (every node's copy of `checks`, `alerts`,
    /// results and history must agree on what a given check *is*), so a
    /// migration run node-locally on `open` cannot converge it: each node
    /// would mint or remap ids independently and disagree with its peers
    /// forever after, which is exactly what the name-keyed-to-UUID migration
    /// this project shipped and then removed did in practice. See
    /// SCALING.md's "Pre-1.0 database policy".
    pub fn open(path: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };
        conn.execute_batch(SCHEMA_DDL)?;
        if path != ":memory:" {
            verify_schema(&conn)?;
        }
        let store = Self {
            conn,
            incident_node_id: parking_lot::RwLock::new("local".to_string()),
        };
        // Self-heals `replication_stream_watermarks` after a crash between an
        // alerts/incidents write and its incremental watermark update in
        // `record_stream_item` — the two are separate statements, not one
        // transaction. Cheap: the streams it covers (alerts, incidents,
        // incident_tombstones) are retention-bounded, unlike `check_results`.
        store.rebuild_stream_watermarks()?;
        // Self-heals `latest_check_results`: ordinary ingest keeps it exactly
        // in step with `check_results` inside one transaction, but nothing
        // stops a direct SQL statement (an operator, a future maintenance
        // path) from touching one table without the other, and there is no
        // way to detect that in isolation later. Scans all of `check_results`,
        // but once per process lifetime rather than per write.
        store.rebuild_latest_check_results()?;
        // Staging tables live for the life of this connection, so every write
        // path can assume them. Created here rather than inside the writers:
        // the replication path runs inside an explicit transaction, and DDL
        // interleaved with DML there is a needless thing to depend on.
        store.prepare_ingest()?;
        Ok(store)
    }

    /// Every configured check, by name. Includes disabled ones: the admin UI has
    /// to show them to re-enable them.
    pub fn list_checks(&self) -> Result<Vec<CheckConfig>> {
        let mut stmt = self.conn.prepare(
            "SELECT config FROM checks WHERE config IS NOT NULL ORDER BY position, name",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            let raw = row?;
            // A row that won't parse is skipped rather than taking down every
            // read: one bad document must not hide the rest of the checks.
            match serde_json::from_str::<CheckConfig>(&raw) {
                Ok(c) => out.push(c),
                Err(e) => tracing::error!(error = %e, "skipping unreadable check row"),
            }
        }
        Ok(out)
    }

    /// Every id `checks` currently holds. Reads only the primary key — no
    /// `config` JSON parse — so a peer's "what do you have" reconciliation
    /// query (`global::reconcile::reconcile_checks`) costs nothing like
    /// `list_checks()`'s per-document scan, and stays cheap however many
    /// checks are held.
    pub fn check_ids(&self) -> Result<Vec<CheckId>> {
        let mut stmt = self.conn.prepare("SELECT id FROM checks ORDER BY id")?;
        let rows = stmt.query_map([], |r| r.get::<_, CheckId>(0))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Ids `region`'s placement admits — the desired set a global node
    /// computes locally, then diffs against what the region itself reports
    /// holding (`check_ids`, fetched over `/internal/global/check-ids`) to
    /// find what to push and what to unplace. See `check_placement_regions`'s
    /// table comment for the "no rows means every region" rule this mirrors.
    pub fn check_ids_for_region(&self, region: &str) -> Result<Vec<CheckId>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.id FROM checks c
             WHERE NOT EXISTS (
                       SELECT 1 FROM check_placement_regions r WHERE r.check_id = c.id
                   )
                OR EXISTS (
                       SELECT 1 FROM check_placement_regions r
                       WHERE r.check_id = c.id AND r.region = ?
                   )
             ORDER BY c.id",
        )?;
        let rows = stmt.query_map(duckdb::params![region], |r| r.get::<_, CheckId>(0))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Create or replace a check by id — the immutable identity, never the
    /// mutable `name`, so an edit that only renames a check still updates the
    /// same row instead of leaving a stale one behind under the old name.
    ///
    /// `config` is the definition; the other columns are a projection of it so
    /// SQL can order and filter without parsing every document.
    pub fn upsert_check(&self, c: &CheckConfig, now_millis: i64) -> Result<()> {
        // DuckDB's ON CONFLICT needs the same column list twice; deleting first
        // is shorter and the two statements are cheap at this table's size.
        self.conn
            .execute("DELETE FROM checks WHERE id = ?", duckdb::params![c.id])?;
        self.conn.execute(
            "INSERT INTO checks
                (id, name, kind, target, interval_secs, timeout_secs, group_name,
                 latency_graph, enabled, expect_status, expect_body_contains,
                 visible, eval, position, agents, config, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                c.id,
                c.name,
                c.kind.as_db(),
                c.target,
                // Legacy column, kept populated so an older binary rolled back
                // onto this database still finds a usable interval.
                cron_to_seconds(&c.cron) as i64,
                c.timeout_secs as i64,
                c.group,
                c.latency_graph,
                c.enabled,
                c.expect.status.map(|s| s as i32),
                c.expect.body_contains,
                c.visible,
                c.eval,
                c.position,
                serde_json::to_string(&c.agents).unwrap_or_else(|_| "[]".into()),
                serde_json::to_string(c)?,
                now_millis,
            ],
        )?;
        self.project_placement_regions(c.id, &c.placement.regions)?;
        Ok(())
    }

    /// Rewrites `check_placement_regions` for one check from its placement's
    /// `regions` list, the same delete-then-insert shape `upsert_check` uses
    /// for the row itself. An empty list leaves no rows behind, matching
    /// `Placement`'s "empty means every region" rule so a reader never has to
    /// special-case "no rows yet" against "explicitly no regions".
    fn project_placement_regions(&self, check_id: CheckId, regions: &[String]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM check_placement_regions WHERE check_id = ?",
            duckdb::params![check_id],
        )?;
        // Deduplicated so a placement that (however it happened) names the
        // same region twice cannot violate the table's primary key.
        let unique: std::collections::BTreeSet<&String> = regions.iter().collect();
        for region in unique {
            self.conn.execute(
                "INSERT INTO check_placement_regions (check_id, region) VALUES (?, ?)",
                duckdb::params![check_id, region],
            )?;
        }
        Ok(())
    }

    /// Position a new check after everything else, so creating one never
    /// silently reshuffles the status page.
    pub fn next_check_position(&self) -> Result<i32> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM checks",
            [],
            |r| r.get(0),
        )?)
    }

    /// Apply an explicit display order, by id. Ids not listed keep their
    /// relative order after the listed ones.
    pub fn reorder_checks(&self, order: &[CheckId], now_millis: i64) -> Result<()> {
        // Rewrite whole rows rather than UPDATE-ing the position column: the
        // document is authoritative, so touching only the column would reorder
        // the index and leave every reader seeing the old order.
        let mut checks = self.list_checks()?;
        let rank = |id: CheckId| {
            order
                .iter()
                .position(|n| *n == id)
                // Ids the caller didn't mention keep their relative order
                // after the ones it did.
                .unwrap_or(usize::MAX)
        };
        checks.sort_by(|a, b| {
            rank(a.id)
                .cmp(&rank(b.id))
                .then_with(|| a.position.cmp(&b.position))
                .then_with(|| a.name.cmp(&b.name))
        });
        for (i, mut check) in checks.into_iter().enumerate() {
            check.position = i as i32;
            self.upsert_check(&check, now_millis)?;
        }
        Ok(())
    }

    /// Delete every result an agent reported. The agent list is derived from
    /// these rows, so this is what makes an agent disappear.
    pub fn purge_agent(&self, agent_id: &str) -> Result<u64> {
        let rows = self.conn.execute(
            "DELETE FROM check_results WHERE agent_id = ?",
            duckdb::params![agent_id],
        )?;
        self.rebuild_result_watermarks()?;
        self.rebuild_latest_check_results()?;
        // The fleet view is the union of results and poll rows, so leaving the
        // poll row behind would purge an agent that then reappeared, healthy.
        self.delete_agent_poll(agent_id)?;
        Ok(rows as u64)
    }

    /// Delete a check's recorded results and alerts, keeping its definition.
    /// Returns how many result rows went, so the UI can say what it did.
    pub fn purge_check_data(&self, check_id: CheckId) -> Result<u64> {
        let rows = self.conn.execute(
            "DELETE FROM check_results WHERE check_id = ?",
            duckdb::params![check_id],
        )?;
        self.conn.execute(
            "DELETE FROM result_replication_watermarks WHERE check_id = ?",
            duckdb::params![check_id],
        )?;
        self.conn.execute(
            "DELETE FROM latest_check_results WHERE check_id = ?",
            duckdb::params![check_id],
        )?;
        self.conn.execute(
            "DELETE FROM alerts WHERE node_id = ?",
            duckdb::params![check_id.to_string()],
        )?;
        Ok(rows as u64)
    }

    /// Remove a check definition and the state derived from its local placement,
    /// without touching raw results or the config tombstone stream. A later
    /// placement with the same immutable id therefore reconnects its history.
    pub fn unplace_check_local(&self, check_id: CheckId) -> Result<bool> {
        self.transaction(|store| {
            let removed = store
                .conn
                .execute("DELETE FROM checks WHERE id = ?", duckdb::params![check_id])?;
            if removed == 0 {
                return Ok(false);
            }
            store.conn.execute(
                "DELETE FROM current_state WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            store.conn.execute(
                "DELETE FROM agent_check_state WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            store.conn.execute(
                "DELETE FROM check_placement_regions WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            Ok(true)
        })
    }

    /// Returns whether a row was actually removed, so the handler can 404
    /// instead of reporting success for an id that never existed.
    pub fn delete_check(&self, check_id: CheckId) -> Result<bool> {
        let n = self
            .conn
            .execute("DELETE FROM checks WHERE id = ?", duckdb::params![check_id])?;
        self.conn.execute(
            "DELETE FROM check_placement_regions WHERE check_id = ?",
            duckdb::params![check_id],
        )?;
        Ok(n > 0)
    }

    /// Sections in display order.
    pub fn list_sections(&self) -> Result<Vec<Section>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, position, collapsed, display FROM sections ORDER BY position, name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Section {
                name: row.get(0)?,
                position: row.get::<_, i32>(1)?,
                collapsed: row.get::<_, Option<bool>>(2)?.unwrap_or(false),
                display: row
                    .get::<_, Option<String>>(3)?
                    .as_deref()
                    .map_or(SectionDisplay::Inline, SectionDisplay::from_db),
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Create a section at the end. Returns false when the name is taken.
    pub fn create_section(
        &self,
        name: &str,
        collapsed: bool,
        display: SectionDisplay,
    ) -> Result<bool> {
        let exists: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sections WHERE name = ?",
            duckdb::params![name],
            |r| r.get(0),
        )?;
        if exists > 0 {
            return Ok(false);
        }
        let next: i32 = self.conn.query_row(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM sections",
            [],
            |r| r.get(0),
        )?;
        self.conn.execute(
            "INSERT INTO sections (name, position, collapsed, display) VALUES (?, ?, ?, ?)",
            duckdb::params![name, next, collapsed, display.as_str()],
        )?;
        Ok(true)
    }

    /// Rename a section, carrying its checks with it. Without the second update
    /// every check in the section would silently become loose.
    pub fn rename_section(&self, from: &str, to: &str, now_millis: i64) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE sections SET name = ? WHERE name = ?",
            duckdb::params![to, from],
        )?;
        if n == 0 {
            return Ok(false);
        }
        // Each affected check is rewritten whole: its group lives in the
        // document, so updating only the column would leave readers on the old
        // name and the check apparently orphaned.
        for mut check in self.list_checks()? {
            if check.group.as_deref() == Some(from) {
                check.group = Some(to.to_string());
                self.upsert_check(&check, now_millis)?;
            }
        }
        Ok(true)
    }

    /// Set whether a section starts collapsed on the public page.
    pub fn set_section_collapsed(&self, name: &str, collapsed: bool) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE sections SET collapsed = ? WHERE name = ?",
            duckdb::params![collapsed, name],
        )?;
        Ok(n > 0)
    }

    /// Set whether the public page renders a section inline or as its own page.
    pub fn set_section_display(&self, name: &str, display: SectionDisplay) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE sections SET display = ? WHERE name = ?",
            duckdb::params![display.as_str(), name],
        )?;
        Ok(n > 0)
    }

    /// Delete a section. Its checks are kept and become loose — deleting a
    /// heading must never delete the monitoring underneath it.
    pub fn delete_section(&self, name: &str, now_millis: i64) -> Result<bool> {
        let n = self
            .conn
            .execute("DELETE FROM sections WHERE name = ?", duckdb::params![name])?;
        if n == 0 {
            return Ok(false);
        }
        for mut check in self.list_checks()? {
            if check.group.as_deref() == Some(name) {
                check.group = None;
                self.upsert_check(&check, now_millis)?;
            }
        }
        Ok(true)
    }

    /// Apply an explicit order. Names not listed keep their relative order after
    /// the listed ones, so a stale client can't silently drop a section.
    pub fn reorder_sections(&self, order: &[String]) -> Result<()> {
        let mut position = 0i32;
        for name in order {
            self.conn.execute(
                "UPDATE sections SET position = ? WHERE name = ?",
                duckdb::params![position, name],
            )?;
            position += 1;
        }
        // Anything the caller didn't mention is appended in its existing order,
        // so a client working from a stale list can't strand a section at
        // position 0 alongside the ones it did name.
        let remaining: Vec<String> = self
            .list_sections()?
            .into_iter()
            .map(|s| s.name)
            .filter(|n| !order.contains(n))
            .collect();
        for name in remaining {
            self.conn.execute(
                "UPDATE sections SET position = ? WHERE name = ?",
                duckdb::params![position, name],
            )?;
            position += 1;
        }
        Ok(())
    }

    /// Adopt the section names already present on checks, so a deployment that
    /// predates the sections table doesn't start with an empty list.
    pub fn seed_sections_from_checks(&self) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT group_name FROM checks
             WHERE group_name IS NOT NULL
               AND group_name NOT IN (SELECT name FROM sections)
             ORDER BY group_name",
        )?;
        let names: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<_, _>>()?;
        for name in names {
            self.create_section(&name, false, SectionDisplay::Inline)?;
        }
        Ok(())
    }

    /// Recompute both latency rollup tables from raw `check_results`.
    /// Incremental maintenance in `rollup_merge_from_raw_sql` keeps them
    /// current on every write, so this is not called on the ordinary `open`
    /// path — only by tests that bulk-seed `check_results` directly and need
    /// the rollups a real `insert_results` call would have maintained.
    #[cfg(test)]
    fn rebuild_latency_rollups(&self) -> Result<()> {
        self.conn.execute_batch(&format!(
            "DELETE FROM check_latency_minute_rollups;
             INSERT INTO check_latency_minute_rollups
             SELECT check_id, agent_id, (ts_millis // {MILLIS_PER_MINUTE}) * {MILLIS_PER_MINUTE},
                    COUNT(*), COUNT(*) FILTER (WHERE status = 'up'),
                    COALESCE(SUM(latency_ms) FILTER (WHERE status = 'up'), 0),
                    MIN(latency_ms) FILTER (WHERE status = 'up'),
                    MAX(latency_ms) FILTER (WHERE status = 'up')
             FROM check_results GROUP BY 1, 2, 3;
             DELETE FROM check_latency_hour_rollups;
             INSERT INTO check_latency_hour_rollups
             SELECT check_id, agent_id, (ts_millis // {MILLIS_PER_HOUR}) * {MILLIS_PER_HOUR},
                    COUNT(*), COUNT(*) FILTER (WHERE status = 'up'),
                    COALESCE(SUM(latency_ms) FILTER (WHERE status = 'up'), 0),
                    MIN(latency_ms) FILTER (WHERE status = 'up'),
                    MAX(latency_ms) FILTER (WHERE status = 'up')
             FROM check_results GROUP BY 1, 2, 3;"
        ))?;
        // The rollup tables were just replaced wholesale, so whatever the
        // watermark previously held (likely all zeros — this path only runs
        // for an old database that had no rollups at all) is now wrong.
        self.rebuild_rollup_watermarks()
    }

    /// Recompute `rollup_replication_watermarks` from the rollup tables
    /// themselves. Unlike the incremental maintenance in
    /// `rollup_merge_from_raw_sql`/`rollup_merge_from_backfill_sql`, this
    /// scans both tables in full, so it is reserved for `rebuild_latency_rollups`
    /// re-seeding both tables wholesale, never every write or every restart.
    #[cfg(test)]
    fn rebuild_rollup_watermarks(&self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM rollup_replication_watermarks
                 WHERE stream IN ('check_latency_minute_rollups', 'check_latency_hour_rollups');
             INSERT INTO rollup_replication_watermarks
             SELECT 'check_latency_minute_rollups', COALESCE(MAX(bucket_start), 0), COUNT(*),
                    COALESCE(SUM(attempts), 0), COALESCE(SUM(up), 0), COALESCE(SUM(latency_sum), 0)
             FROM check_latency_minute_rollups;
             INSERT INTO rollup_replication_watermarks
             SELECT 'check_latency_hour_rollups', COALESCE(MAX(bucket_start), 0), COUNT(*),
                    COALESCE(SUM(attempts), 0), COALESCE(SUM(up), 0), COALESCE(SUM(latency_sum), 0)
             FROM check_latency_hour_rollups;",
        )?;
        Ok(())
    }

    fn rebuild_result_watermarks(&self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM result_replication_watermarks;
             INSERT INTO result_replication_watermarks
                 SELECT check_id, MIN(ts_millis), MAX(ts_millis), COUNT(*)
                 FROM check_results GROUP BY check_id;",
        )?;
        Ok(())
    }

    fn rebuild_stream_watermarks(&self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM replication_stream_watermarks;
             INSERT INTO replication_stream_watermarks
             SELECT 'alerts', COALESCE(MAX(ts_millis), 0), COALESCE(MAX_BY(id, (ts_millis, id)), ''), COUNT(*) FROM alerts;
             INSERT INTO replication_stream_watermarks
             SELECT 'incidents', COALESCE(MAX(version_millis), 0),
                    COALESCE(MAX_BY(version_node || chr(0) || id, (version_millis, version_node, id)), ''),
                    COUNT(*) FROM incidents;
             INSERT INTO replication_stream_watermarks
                           SELECT 'incident_tombstones', COALESCE(MAX(deleted_at), 0),
                     COALESCE(MAX_BY(node_id || chr(0) || id, (deleted_at, node_id, id)), ''),
                     COUNT(*) FROM incident_tombstones;",
        )?;
        Ok(())
    }

    fn stream_digest(&self, stream: &str) -> Result<StreamDigest> {
        self.conn.query_row(
            "SELECT max_millis, max_id, item_count FROM replication_stream_watermarks WHERE stream = ?",
            duckdb::params![stream],
            |row| Ok(StreamDigest { max_millis: row.get(0)?, max_id: row.get(1)?, count: row.get(2)? }),
        ).map_err(Into::into)
    }

    /// Read `rollup_replication_watermarks` for one rollup table — a point
    /// lookup, never a scan of the rollup table it summarizes, because every
    /// write maintains the watermark incrementally alongside it.
    fn rollup_digest(&self, stream: &str) -> Result<RollupDigest> {
        self.conn
            .query_row(
                "SELECT max_bucket_start, row_count, attempts_sum, up_sum, latency_sum_sum
                 FROM rollup_replication_watermarks WHERE stream = ?",
                duckdb::params![stream],
                |row| {
                    Ok(RollupDigest {
                        max_bucket_start: row.get(0)?,
                        count: row.get(1)?,
                        attempts_sum: row.get(2)?,
                        up_sum: row.get(3)?,
                        latency_sum_sum: row.get(4)?,
                    })
                },
            )
            .map_err(Into::into)
    }

    /// Recompute `latest_check_results` from raw `check_results`. Not called
    /// on the ordinary `open` path — incremental maintenance in
    /// `derived_results_sql` keeps it current on every write, and `purge_agent`
    /// calls this directly after deleting an agent's rows — but available for
    /// the same bulk-load scenarios as `rebuild_latency_rollups`.
    fn rebuild_latest_check_results(&self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM latest_check_results;
             INSERT INTO latest_check_results
                 (check_id, agent_id, ts_millis, status, latency_ms, status_code, error, failure)
             SELECT check_id, agent_id, ts_millis, status, latency_ms, status_code, error, failure
             FROM (
                 SELECT check_id, agent_id, ts_millis, status, latency_ms, status_code, error,
                        failure,
                        row_number() OVER (
                            PARTITION BY check_id ORDER BY ts_millis DESC, agent_id DESC
                        ) AS rn
                 FROM check_results
             ) ranked
             WHERE rn = 1;",
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn execute_test_sql(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)?;
        Ok(())
    }

    pub(crate) fn transaction<T>(&self, operation: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        self.conn.execute_batch("BEGIN")?;
        match operation(self) {
            Ok(value) => {
                if let Err(error) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(error.into());
                }
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub fn persist_ingest(
        &self,
        rows: &[(CheckResult, Status)],
        agent_states: &[AgentCheckStateRow],
        published_states: &[PublishedStateRow],
    ) -> Result<()> {
        self.prepare_ingest()?;
        self.transaction(|store| {
            store.persist_ingest_uncommitted(rows, agent_states, published_states)
        })
    }

    pub(crate) fn prepare_ingest(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS ingest_results_stage AS SELECT * FROM check_results WITH NO DATA;
             CREATE TEMP TABLE IF NOT EXISTS ingest_agent_stage (
                 ordinal BIGINT, check_id UUID, agent_id VARCHAR, status VARCHAR,
                 consecutive_failures INTEGER, zone VARCHAR, region VARCHAR, last_check BIGINT
             );
             CREATE TEMP TABLE IF NOT EXISTS ingest_state_stage (
                 check_id UUID, status VARCHAR, pending_state VARCHAR,
                 pending_since BIGINT, last_check BIGINT
              );
             CREATE TEMP TABLE IF NOT EXISTS replicate_new_results AS SELECT * FROM check_results WITH NO DATA;
             CREATE TEMP TABLE IF NOT EXISTS replicate_probe_stage AS SELECT * FROM check_results WITH NO DATA;
             CREATE TEMP TABLE IF NOT EXISTS prune_results_counts (check_id UUID, removed BIGINT);
             CREATE TEMP TABLE IF NOT EXISTS prune_results_min_ts (check_id UUID, min_ts BIGINT);",

        )?;
        Ok(())
    }

    pub(crate) fn persist_ingest_uncommitted(
        &self,
        rows: &[(CheckResult, Status)],
        agent_states: &[AgentCheckStateRow],
        published_states: &[PublishedStateRow],
    ) -> Result<()> {
        self.conn.execute_batch("DELETE FROM ingest_results_stage; DELETE FROM ingest_agent_stage; DELETE FROM ingest_state_stage;")?;
        self.stage_results(rows)?;
        self.stage_agent_states(agent_states)?;
        {
            let mut appender = self.conn.appender("ingest_state_stage")?;
            for state in published_states {
                appender.append_row(duckdb::params![
                    state.check_id,
                    state.state.as_str(),
                    state.pending.map(|(pending, _)| pending.as_str()),
                    state.pending.map(|(_, since)| since),
                    state.last_check,
                ])?;
            }
            appender.flush()?;
        }
        self.conn.execute_batch(&format!(
            "INSERT INTO check_results SELECT * FROM ingest_results_stage;
             {results}
             {agents}
             INSERT INTO current_state
                 (check_id, status, last_check, consecutive_failures, pending_state, pending_since)
             SELECT check_id, status, last_check, 0, pending_state, pending_since
             FROM ingest_state_stage
             ON CONFLICT (check_id) DO UPDATE SET
                 status = excluded.status,
                 last_check = excluded.last_check,
                 consecutive_failures = 0,
                 pending_state = excluded.pending_state,
                 pending_since = excluded.pending_since;
             INSERT INTO state_history
             SELECT staged.check_id, staged.status, staged.last_check
             FROM ingest_state_stage staged
             LEFT JOIN LATERAL (
                 SELECT state FROM state_history history
                 WHERE history.check_id = staged.check_id
                 ORDER BY since DESC LIMIT 1
             ) newest ON true
             WHERE newest.state IS NULL OR newest.state <> staged.status
             ON CONFLICT DO NOTHING;",
            results = derived_results_sql("ingest_results_stage"),
            agents = AGENT_STATE_MERGE_SQL,
        ))?;
        Ok(())
    }

    /// Fill `ingest_results_stage` from a batch. Shared by local ingest and
    /// replication: both write the same rows, and an appender is an order of
    /// magnitude cheaper than a prepared insert per row.
    fn stage_results(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        let mut appender = self.conn.appender("ingest_results_stage")?;
        for (result, status) in rows {
            appender.append_row(duckdb::params![
                result.check_id,
                result.agent_id,
                result.timestamp.timestamp_millis(),
                status.as_str(),
                result.latency_ms as i64,
                result.status_code.map(|code| code as i32),
                result.error,
                non_empty(&result.zone),
                non_empty(&result.region),
                result
                    .failure
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
            ])?;
        }
        appender.flush()?;
        Ok(())
    }

    /// Fill `ingest_agent_stage`. `ordinal` preserves batch order so the merge
    /// can break `last_check` ties on arrival order rather than on scan order.
    fn stage_agent_states(&self, states: &[AgentCheckStateRow]) -> Result<()> {
        let mut appender = self.conn.appender("ingest_agent_stage")?;
        for (ordinal, state) in states.iter().enumerate() {
            appender.append_row(duckdb::params![
                ordinal as i64,
                state.check_id,
                state.agent_id,
                state.status.as_str(),
                state.consecutive_failures as i32,
                state.zone,
                state.region,
                state.last_check,
            ])?;
        }
        appender.flush()?;
        Ok(())
    }

    /// Merge a batch of per-agent beliefs in one statement. The replication
    /// path used to run one upsert per belief, which on a coalesced batch of a
    /// few thousand checks held the single database worker for seconds and
    /// starved local ingest behind it.
    pub(crate) fn update_agent_check_states_uncommitted(
        &self,
        states: &[AgentCheckStateRow],
    ) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("DELETE FROM ingest_agent_stage;")?;
        self.stage_agent_states(states)?;
        self.conn.execute_batch(AGENT_STATE_MERGE_SQL)?;
        Ok(())
    }

    /// Persist a batch of raw results with their evaluated status.
    pub fn insert_results(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        // One transaction for the whole batch: DuckDB otherwise commits per
        // statement, which makes large seeds (100k+ rows) crawl.
        self.conn.execute_batch("BEGIN")?;
        let insert = || -> Result<()> {
            let mut stmt = self.conn.prepare(
                "INSERT INTO check_results
                    (check_id, agent_id, ts_millis, status, latency_ms, status_code,
                     error, failure, zone, region)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for (r, status) in rows {
                stmt.execute(duckdb::params![
                    r.check_id,
                    r.agent_id,
                    r.timestamp.timestamp_millis(),
                    status.as_str(),
                    r.latency_ms as i64,
                    r.status_code.map(|c| c as i32),
                    r.error,
                    r.failure.as_ref().map(serde_json::to_string).transpose()?,
                    non_empty(&r.zone),
                    non_empty(&r.region),
                ])?;
            }
            self.conn
                .execute_batch("DELETE FROM ingest_results_stage;")?;
            self.stage_results(rows)?;
            self.conn
                .execute_batch(&derived_results_sql("ingest_results_stage"))?;
            Ok(())
        };
        match insert() {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Same rows as `insert_results`, loaded through DuckDB's Appender rather
    /// than a prepared statement executed once per row. For a real ingest
    /// batch the two cost about the same; for a many-thousand-row bulk load
    /// — `seed_mock`'s reason for existing — the Appender is the difference
    /// between seconds and tens of seconds, which is why it exists as its own
    /// method instead of replacing `insert_results` outright.
    fn append_results_bulk(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        let mut appender = self.conn.appender_with_columns(
            "check_results",
            &[
                "check_id",
                "agent_id",
                "ts_millis",
                "status",
                "latency_ms",
                "status_code",
                "error",
                "failure",
                "zone",
                "region",
            ],
        )?;
        for (r, status) in rows {
            appender.append_row(duckdb::params![
                r.check_id,
                r.agent_id,
                r.timestamp.timestamp_millis(),
                status.as_str(),
                r.latency_ms as i64,
                r.status_code.map(|c| c as i32),
                r.error,
                r.failure.as_ref().map(serde_json::to_string).transpose()?,
                non_empty(&r.zone),
                non_empty(&r.region),
            ])?;
        }
        appender.flush()?;
        self.conn
            .execute_batch("DELETE FROM ingest_results_stage;")?;
        self.stage_results(rows)?;
        self.conn
            .execute_batch(&derived_results_sql("ingest_results_stage"))?;
        Ok(())
    }

    /// Return history rows for a check, newest first, optionally within a time
    /// range and optionally restricted to one evaluated status.
    ///
    /// The status filter matters: a day can hold thousands of results, so the
    /// failures worth reading about are not necessarily in the newest `limit`.
    pub fn result_coverage(
        &self,
        check_ids: &[CheckId],
        agent_ids: &[String],
        since: i64,
    ) -> Result<Vec<(String, u64)>> {
        if check_ids.is_empty() || agent_ids.is_empty() {
            return Ok(Vec::new());
        }
        let check_placeholders = std::iter::repeat_n("?", check_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let agent_placeholders = std::iter::repeat_n("?", agent_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT agent_id, COUNT(DISTINCT check_id)
             FROM check_results
             WHERE ts_millis >= ? AND check_id IN ({check_placeholders})
               AND agent_id IN ({agent_placeholders})
             GROUP BY agent_id"
        );
        let mut params = vec![duckdb::types::Value::BigInt(since)];
        params.extend(
            check_ids
                .iter()
                .map(|id| duckdb::types::Value::Text(id.to_string())),
        );
        params.extend(agent_ids.iter().cloned().map(duckdb::types::Value::Text));
        let param_refs: Vec<&dyn duckdb::types::ToSql> = params
            .iter()
            .map(|value| value as &dyn duckdb::types::ToSql)
            .collect();
        let mut stmt = self.conn.prepare(&sql)?;
        Ok(stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })?
            .collect::<std::result::Result<_, _>>()?)
    }

    pub fn history(
        &self,
        check_id: CheckId,
        limit: usize,
        from: Option<i64>,
        to: Option<i64>,
        status: Option<Status>,
    ) -> Result<Vec<HistoryRow>> {
        let mut sql = String::from(
            "SELECT ts_millis, agent_id, status, latency_ms, status_code, error, failure
             FROM check_results WHERE check_id = ?",
        );
        if from.is_some() {
            sql.push_str(" AND ts_millis >= ?");
        }
        if to.is_some() {
            sql.push_str(" AND ts_millis < ?");
        }
        if status.is_some() {
            sql.push_str(" AND status = ?");
        }
        // Deterministic order when several agents report the same millisecond.
        sql.push_str(" ORDER BY ts_millis DESC, agent_id ASC LIMIT ?");
        let mut stmt = self.conn.prepare(&sql)?;
        // Build params dynamically.
        let mut params: Vec<duckdb::types::Value> =
            vec![duckdb::types::Value::Text(check_id.to_string())];
        if let Some(f) = from {
            params.push(duckdb::types::Value::BigInt(f));
        }
        if let Some(t) = to {
            params.push(duckdb::types::Value::BigInt(t));
        }
        // Must be pushed in the same order the clauses were appended above.
        if let Some(s) = status {
            params.push(duckdb::types::Value::Text(s.as_str().to_owned()));
        }
        params.push(duckdb::types::Value::BigInt(limit as i64));
        let param_refs: Vec<&dyn duckdb::types::ToSql> = params
            .iter()
            .map(|v| v as &dyn duckdb::types::ToSql)
            .collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(HistoryRow {
                    timestamp: row.get::<_, i64>(0)?,
                    agent_id: row.get::<_, String>(1)?,
                    status: Status::from_db(&row.get::<_, String>(2)?),
                    latency_ms: row.get::<_, i64>(3)? as u64,
                    status_code: row.get::<_, Option<i32>>(4)?.map(|c| c as u16),
                    error: row.get::<_, Option<String>>(5)?,
                    // Rows written before probes classified themselves have no
                    // value here; readers fall back to the neutral summary.
                    failure: row
                        .get::<_, Option<String>>(6)?
                        .and_then(|f| serde_json::from_str(&f).ok()),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Recent raw probe results across all checks and agents, newest first,
    /// narrowed by `filter`. This backs the admin explorer, so it returns the
    /// raw `error` — the caller is responsible for admin-gating.
    ///
    /// SQL is built dynamically like `history()`: clauses and params are
    /// appended in the same order so the positional `?`s line up.
    pub fn recent_results(&self, filter: &ResultsFilter, limit: usize) -> Result<Vec<ResultRow>> {
        let mut sql = String::from(
            "SELECT check_id, agent_id, ts_millis, zone, region, status, latency_ms,
                    status_code, error, failure
             FROM check_results WHERE 1 = 1",
        );
        if filter.check_id.is_some() {
            sql.push_str(" AND check_id = ?");
        }
        if filter.agent_id.is_some() {
            sql.push_str(" AND agent_id = ?");
        }
        if filter.status.is_some() {
            sql.push_str(" AND status = ?");
        }
        if filter.region.is_some() {
            sql.push_str(" AND region = ?");
        }
        if filter.zone.is_some() {
            sql.push_str(" AND zone = ?");
        }
        // Deterministic order when several agents report the same millisecond.
        sql.push_str(" ORDER BY ts_millis DESC, agent_id ASC LIMIT ?");
        let mut stmt = self.conn.prepare(&sql)?;
        // Must be pushed in the same order the clauses were appended above.
        let mut params: Vec<duckdb::types::Value> = Vec::new();
        if let Some(c) = &filter.check_id {
            params.push(duckdb::types::Value::Text(c.to_string()));
        }
        if let Some(a) = &filter.agent_id {
            params.push(duckdb::types::Value::Text(a.clone()));
        }
        if let Some(s) = filter.status {
            params.push(duckdb::types::Value::Text(s.as_str().to_owned()));
        }
        if let Some(r) = &filter.region {
            params.push(duckdb::types::Value::Text(r.clone()));
        }
        if let Some(z) = &filter.zone {
            params.push(duckdb::types::Value::Text(z.clone()));
        }
        params.push(duckdb::types::Value::BigInt(limit as i64));
        let param_refs: Vec<&dyn duckdb::types::ToSql> = params
            .iter()
            .map(|v| v as &dyn duckdb::types::ToSql)
            .collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(ResultRow {
                    check_id: row.get::<_, CheckId>(0)?,
                    agent_id: row.get::<_, String>(1)?,
                    timestamp: row.get::<_, i64>(2)?,
                    zone: row.get::<_, Option<String>>(3)?,
                    region: row.get::<_, Option<String>>(4)?,
                    status: Status::from_db(&row.get::<_, String>(5)?),
                    latency_ms: row.get::<_, i64>(6)? as u64,
                    status_code: row.get::<_, Option<i32>>(7)?.map(|c| c as u16),
                    error: row.get::<_, Option<String>>(8)?,
                    // Rows written before probes classified themselves have no
                    // value here; readers fall back to the neutral summary.
                    failure: row
                        .get::<_, Option<String>>(9)?
                        .and_then(|f| serde_json::from_str(&f).ok()),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Return an unused stable local event id. The first event keeps the
    /// readable base; same-millisecond events receive a deterministic suffix.
    /// IDs are persisted and replicated as written, so replication retains the
    /// exact event identity rather than generating another one.
    fn record_stream_item(&self, stream: &str, millis: i64, id: &str, delta: i64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO replication_stream_watermarks (stream, max_millis, max_id, item_count)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (stream) DO UPDATE SET
                 max_millis = CASE WHEN (excluded.max_millis, excluded.max_id) >
                                        (replication_stream_watermarks.max_millis, replication_stream_watermarks.max_id)
                                   THEN excluded.max_millis ELSE replication_stream_watermarks.max_millis END,
                 max_id = CASE WHEN (excluded.max_millis, excluded.max_id) >
                                    (replication_stream_watermarks.max_millis, replication_stream_watermarks.max_id)
                               THEN excluded.max_id ELSE replication_stream_watermarks.max_id END,
                 item_count = replication_stream_watermarks.item_count + excluded.item_count",
            duckdb::params![stream, millis, id, delta],
        )?;
        Ok(())
    }

    pub fn set_incident_node_id(&self, node_id: &str) {
        *self.incident_node_id.write() = node_id.to_string();
    }

    fn incident_node_id(&self) -> String {
        self.incident_node_id.read().clone()
    }

    fn touch_incident(&self, id: &str, millis: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE incidents SET version_millis = GREATEST(version_millis + 1, ?), version_node = ?
             WHERE id = ?",
            duckdb::params![millis, self.incident_node_id(), id],
        )?;
        let version: Option<i64> = self
            .conn
            .query_row(
                "SELECT version_millis FROM incidents WHERE id = ?",
                duckdb::params![id],
                |row| row.get(0),
            )
            .ok();
        if let Some(version) = version {
            let cursor_id = format!("{}\0{id}", self.incident_node_id());
            self.record_stream_item("incidents", version, &cursor_id, 0)?;
        }
        Ok(())
    }

    fn next_event_id(&self, table: &str, base: &str) -> Result<String> {
        for suffix in 0_u64.. {
            let id = if suffix == 0 {
                base.to_owned()
            } else {
                format!("{base}-{suffix}")
            };
            let exists: bool = self.conn.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE id = ?)"),
                duckdb::params![id],
                |row| row.get(0),
            )?;
            if !exists {
                return Ok(id);
            }
        }
        unreachable!("u64 event id suffixes are exhausted")
    }

    /// Persist an alert: one confirmed state transition. `incident_id` is
    /// stamped here, by the caller that just decided which incident this
    /// transition belongs to.
    pub fn insert_alert(
        &self,
        scope: AlertScope,
        node_id: &str,
        previous_state: Option<NodeState>,
        current_state: NodeState,
        ts_millis: i64,
        incident_id: Option<&str>,
    ) -> Result<String> {
        let id = self.next_event_id(
            "alerts",
            &format!("{node_id}-{ts_millis}-{}", current_state.as_str()),
        )?;
        self.conn.execute(
            "INSERT INTO alerts
                 (id, scope, node_id, previous_state, current_state, ts_millis, incident_id)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                id,
                scope.as_str(),
                node_id,
                previous_state.map(|s| s.as_str()),
                current_state.as_str(),
                ts_millis,
                incident_id,
            ],
        )?;
        self.record_stream_item("alerts", ts_millis, &id, 1)?;
        Ok(id)
    }

    /// Return the most recent `limit` alerts, newest first.
    pub fn recent_alerts(&self, limit: usize) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts
             ORDER BY ts_millis DESC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![limit as i64], Self::alert_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The alerts stamped as belonging to one incident, oldest first — the
    /// machine evidence behind the operator's narrative.
    pub fn alerts_for_incident(&self, incident_id: &str) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts
             WHERE incident_id = ?
             ORDER BY ts_millis ASC",
        )?;
        let rows = stmt
            .query_map(duckdb::params![incident_id], Self::alert_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Record a notification delivery intention or update its retry state.
    /// Callers creating an alert do this inside the alert transaction; a crash
    /// after commit therefore leaves an owed delivery for the next dispatcher.
    pub fn enqueue_notification(&self, row: &QueuedRow) -> Result<()> {
        self.conn.execute(
            "INSERT INTO notification_queue
                 (id, notifier, check_id, previous_state, current_state,
                  attempts, next_attempt, last_error, queued_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (id) DO UPDATE SET
                 attempts = excluded.attempts,
                 next_attempt = excluded.next_attempt,
                 last_error = excluded.last_error",
            duckdb::params![
                row.id,
                row.notifier,
                row.check_id,
                row.previous_state.map(|s| s.as_str()),
                row.current_state.as_str(),
                row.attempts,
                row.next_attempt,
                row.last_error,
                row.queued_at,
            ],
        )?;
        Ok(())
    }

    /// Queued notifications whose next attempt is due, oldest first — an outage
    /// should be paged in the order it happened.
    pub fn due_notifications(&self, now_millis: i64, limit: usize) -> Result<Vec<QueuedRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, notifier, check_id, previous_state, current_state,
                    attempts, next_attempt, last_error, queued_at
             FROM notification_queue
             WHERE next_attempt IS NOT NULL AND next_attempt <= ?
             ORDER BY queued_at ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![now_millis, limit as i64], Self::queued_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Everything still undelivered, newest first, whether waiting or given up.
    /// Read by the notifier admin panel: "nobody was paged" is exactly the kind
    /// of failure this project keeps refusing to leave silent.
    pub fn undelivered_notifications(&self, limit: usize) -> Result<Vec<QueuedRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, notifier, check_id, previous_state, current_state,
                    attempts, next_attempt, last_error, queued_at
             FROM notification_queue
             ORDER BY queued_at DESC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![limit as i64], Self::queued_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn delete_notification(&self, id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM notification_queue WHERE id = ?",
            duckdb::params![id],
        )?;
        Ok(())
    }

    fn queued_row(row: &duckdb::Row) -> duckdb::Result<QueuedRow> {
        Ok(QueuedRow {
            id: row.get::<_, String>(0)?,
            notifier: row.get::<_, String>(1)?,
            check_id: row.get::<_, CheckId>(2)?,
            previous_state: row
                .get::<_, Option<String>>(3)?
                .as_deref()
                .map(NodeState::from_db),
            current_state: NodeState::from_db(&row.get::<_, String>(4)?),
            attempts: row.get::<_, i32>(5)?,
            next_attempt: row.get::<_, Option<i64>>(6)?,
            last_error: row.get::<_, String>(7)?,
            queued_at: row.get::<_, i64>(8)?,
        })
    }

    fn alert_row(row: &duckdb::Row) -> duckdb::Result<AlertRow> {
        Ok(AlertRow {
            id: row.get::<_, String>(0)?,
            scope: AlertScope::from_db(&row.get::<_, String>(1)?),
            node_id: row.get::<_, String>(2)?,
            previous_state: row
                .get::<_, Option<String>>(3)?
                .as_deref()
                .map(NodeState::from_db),
            current_state: NodeState::from_db(&row.get::<_, String>(4)?),
            timestamp: row.get::<_, i64>(5)?,
            incident_id: row.get::<_, Option<String>>(6)?,
        })
    }

    /// The open incident **the coordinator owns** for a check: one it opened
    /// from a probe, identified by having an `origin_check`. A check is in at
    /// most one of these, because reconciliation joins an existing incident
    /// rather than opening a second.
    ///
    /// Manually created incidents are excluded on purpose. An operator's
    /// incident has no origin check, so the reconciler has nothing to follow —
    /// it must not adopt dependents into it, must not let a failing check ride
    /// it instead of opening a real one, and above all must not resolve it. A
    /// maintenance notice is over when a human says so.
    pub fn open_incident_for(&self, check_id: CheckId) -> Result<Option<IncidentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT i.id
             FROM incidents i
             JOIN incident_checks c ON c.incident_id = i.id
             WHERE c.check_id = ?
               AND i.state <> 'resolved'
               AND i.origin_check IS NOT NULL
             ORDER BY i.started_at DESC
             LIMIT 1",
        )?;
        let mut ids = stmt
            .query_map(duckdb::params![check_id], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        match ids.pop() {
            Some(id) => self.incident(&id),
            None => Ok(None),
        }
    }

    /// Open an incident. `origin_check` is `None` for one an operator created,
    /// which reconciliation then ignores entirely. The opening update is
    /// written in the same call, so an incident never exists without the entry
    /// explaining it.
    #[allow(clippy::too_many_arguments)]
    pub fn create_incident(
        &self,
        id: &str,
        title: &str,
        state: IncidentState,
        started_at: i64,
        global: bool,
        origin_check: Option<CheckId>,
        checks: &[IncidentCheckRow],
        first_update: Option<(&str, Option<&str>)>,
    ) -> Result<IncidentRow> {
        self.conn.execute(
            "INSERT INTO incidents
                 (id, title, state, started_at, global, origin_check, version_millis, version_node)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                id,
                title,
                state.as_str(),
                started_at,
                global,
                origin_check,
                started_at,
                self.incident_node_id()
            ],
        )?;
        for c in checks {
            self.conn.execute(
                "INSERT INTO incident_checks (incident_id, check_id, source) VALUES (?, ?, ?)",
                duckdb::params![id, c.check_id, c.source.as_str()],
            )?;
        }
        if let Some((body, author)) = first_update {
            self.add_update(id, state, body, author, started_at)?;
        }
        self.touch_incident(id, started_at)?;
        let cursor_id = format!("{}\0{id}", self.incident_node_id());
        self.record_stream_item("incidents", started_at, &cursor_id, 1)?;
        self.incident(id)?
            .ok_or_else(|| anyhow::anyhow!("incident {id} vanished immediately after insert"))
    }

    /// Attach a check to an incident. Idempotent on (incident, check): a repeat
    /// only upgrades how it got there, and never downgrades an `origin`.
    pub fn attach_check(
        &self,
        incident_id: &str,
        check_id: CheckId,
        source: AffectSource,
    ) -> Result<()> {
        let existing: Option<String> = {
            let mut stmt = self.conn.prepare(
                "SELECT source FROM incident_checks WHERE incident_id = ? AND check_id = ?",
            )?;
            let mut rows = stmt
                .query_map(duckdb::params![incident_id, check_id], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows.pop()
        };
        match existing.as_deref().map(AffectSource::from_db) {
            // The origin is how the incident came to exist; nothing reclassifies it.
            Some(AffectSource::Origin) => Ok(()),
            Some(current) if current == source => Ok(()),
            Some(_) => {
                self.conn.execute(
                    "UPDATE incident_checks SET source = ?
                     WHERE incident_id = ? AND check_id = ?",
                    duckdb::params![source.as_str(), incident_id, check_id],
                )?;
                self.touch_incident(incident_id, chrono::Utc::now().timestamp_millis())
            }
            None => {
                self.conn.execute(
                    "INSERT INTO incident_checks (incident_id, check_id, source)
                     VALUES (?, ?, ?)",
                    duckdb::params![incident_id, check_id, source.as_str()],
                )?;
                self.touch_incident(incident_id, chrono::Utc::now().timestamp_millis())
            }
        }
    }

    /// Detach a check. `only_source` restricts it to attachments of one kind,
    /// so reconciliation can retract a `dependency` without touching what an
    /// operator added by hand.
    pub fn detach_check(
        &self,
        incident_id: &str,
        check_id: CheckId,
        only_source: Option<AffectSource>,
    ) -> Result<()> {
        let changed = match only_source {
            Some(s) => self.conn.execute(
                "DELETE FROM incident_checks
                 WHERE incident_id = ? AND check_id = ? AND source = ?",
                duckdb::params![incident_id, check_id, s.as_str()],
            )?,
            None => self.conn.execute(
                "DELETE FROM incident_checks WHERE incident_id = ? AND check_id = ?",
                duckdb::params![incident_id, check_id],
            )?,
        };
        if changed != 0 {
            self.touch_incident(incident_id, chrono::Utc::now().timestamp_millis())?;
        }
        Ok(())
    }

    /// Replace exactly the manual attachments, leaving origin and dependency
    /// rows alone. The affects editor saves through this.
    pub fn set_manual_checks(&self, incident_id: &str, checks: &[CheckId]) -> Result<()> {
        let current: std::collections::BTreeSet<CheckId> = {
            let mut stmt = self.conn.prepare(
                "SELECT check_id FROM incident_checks WHERE incident_id = ? AND source = 'manual'",
            )?;
            stmt.query_map(duckdb::params![incident_id], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let desired: std::collections::BTreeSet<CheckId> = checks.iter().copied().collect();
        if current == desired {
            return Ok(());
        }
        self.conn.execute(
            "DELETE FROM incident_checks WHERE incident_id = ? AND source = 'manual'",
            duckdb::params![incident_id],
        )?;
        let mut stmt = self.conn.prepare(
            "INSERT INTO incident_checks (incident_id, check_id, source) VALUES (?, ?, 'manual')",
        )?;
        for check_id in desired {
            stmt.execute(duckdb::params![incident_id, check_id])?;
        }
        self.touch_incident(incident_id, chrono::Utc::now().timestamp_millis())
    }

    /// Add an update, moving the incident to the state it announces. This is
    /// the only way an incident changes state — operator posts, acknowledgement
    /// and the coordinator's own auto-open/auto-resolve entries all land here,
    /// so the timeline can never disagree with the state.
    ///
    /// `author` is `None` when the coordinator wrote it; a non-None author on a
    /// `detected` incident is what constitutes an acknowledgement.
    pub fn add_update(
        &self,
        incident_id: &str,
        state: IncidentState,
        body: &str,
        author: Option<&str>,
        now_millis: i64,
    ) -> Result<IncidentUpdateRow> {
        let id = self.next_event_id("incident_updates", &format!("{incident_id}-u{now_millis}"))?;
        self.conn.execute(
            "INSERT INTO incident_updates (id, incident_id, state, body, author, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![id, incident_id, state.as_str(), body, author, now_millis],
        )?;
        // resolved_at tracks the state rather than being set beside it: moving
        // back out of `resolved` (an operator reopening) must clear it, or the
        // incident would read as both open and finished.
        if state.is_resolved() {
            self.conn.execute(
                "UPDATE incidents SET state = ?, resolved_at = ? WHERE id = ?",
                duckdb::params![state.as_str(), now_millis, incident_id],
            )?;
        } else {
            self.conn.execute(
                "UPDATE incidents SET state = ?, resolved_at = NULL WHERE id = ?",
                duckdb::params![state.as_str(), incident_id],
            )?;
        }
        self.touch_incident(incident_id, now_millis)?;
        Ok(IncidentUpdateRow {
            id,
            incident_id: incident_id.to_owned(),
            state,
            body: body.to_owned(),
            author: author.map(str::to_owned),
            created_at: now_millis,
        })
    }

    /// An incident's narrative, oldest first.
    pub fn updates_for_incident(&self, incident_id: &str) -> Result<Vec<IncidentUpdateRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, incident_id, state, body, author, created_at
             FROM incident_updates
             WHERE incident_id = ?
             ORDER BY created_at ASC",
        )?;
        let rows = stmt
            .query_map(duckdb::params![incident_id], |row| {
                Ok(IncidentUpdateRow {
                    id: row.get::<_, String>(0)?,
                    incident_id: row.get::<_, String>(1)?,
                    state: IncidentState::from_db(&row.get::<_, String>(2)?),
                    body: row.get::<_, String>(3)?,
                    author: row.get::<_, Option<String>>(4)?,
                    created_at: row.get::<_, i64>(5)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Whether a human has touched this incident. Derived from the updates
    /// rather than stored: the acknowledgement IS the first authored update, so
    /// a column beside it would be a second copy of the same fact. Stays true
    /// when an operator skipped straight past `investigating`.
    pub fn is_acknowledged(&self, incident_id: &str) -> Result<bool> {
        let mut stmt = self.conn.prepare(
            "SELECT 1 FROM incident_updates
             WHERE incident_id = ? AND author IS NOT NULL LIMIT 1",
        )?;
        let mut rows = stmt
            .query_map(duckdb::params![incident_id], |r| r.get::<_, i32>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows.pop().is_some())
    }

    /// Edit the operator-owned fields. Deliberately cannot touch `state` —
    /// that moves only by posting an update.
    pub fn edit_incident(
        &self,
        id: &str,
        title: &str,
        started_at: i64,
        global: bool,
    ) -> Result<Option<IncidentRow>> {
        let Some(existing) = self.incident(id)? else {
            return Ok(None);
        };
        if existing.title == title && existing.started_at == started_at && existing.global == global
        {
            return Ok(Some(existing));
        }
        self.conn.execute(
            "UPDATE incidents SET title = ?, started_at = ?, global = ? WHERE id = ?",
            duckdb::params![title, started_at, global, id],
        )?;
        self.touch_incident(id, chrono::Utc::now().timestamp_millis())?;
        self.incident(id)
    }

    /// One incident with its affected checks.
    pub fn incident(&self, id: &str) -> Result<Option<IncidentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, title, state, started_at, resolved_at, global, origin_check
             FROM incidents WHERE id = ?",
        )?;
        let mut rows = stmt
            .query_map(duckdb::params![id], Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        match rows.pop() {
            Some(mut row) => {
                row.checks = self.checks_for_incident(&row.id)?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    fn checks_for_incident(&self, incident_id: &str) -> Result<Vec<IncidentCheckRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT check_id, source FROM incident_checks
             WHERE incident_id = ? ORDER BY check_id",
        )?;
        let rows = stmt
            .query_map(duckdb::params![incident_id], |row| {
                Ok(IncidentCheckRow {
                    check_id: row.get::<_, CheckId>(0)?,
                    source: AffectSource::from_db(&row.get::<_, String>(1)?),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Recent incidents, newest first. `open_only` filters to current outages —
    /// which is `state <> 'resolved'`, since state is the only lifecycle field.
    pub fn list_incidents(
        &self,
        limit: usize,
        open_only: Option<bool>,
        since: Option<i64>,
    ) -> Result<Vec<IncidentRow>> {
        let mut sql = String::from(
            "SELECT id, title, state, started_at, resolved_at, global, origin_check
             FROM incidents WHERE TRUE",
        );
        match open_only {
            Some(true) => sql.push_str(" AND state <> 'resolved'"),
            Some(false) => sql.push_str(" AND state = 'resolved'"),
            None => {}
        }
        if since.is_some() {
            sql.push_str(" AND (state <> 'resolved' OR resolved_at >= ?)");
        }
        sql.push_str(" ORDER BY started_at DESC LIMIT ?");
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = if let Some(since) = since {
            stmt.query_map(duckdb::params![since, limit as i64], Self::incident_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(duckdb::params![limit as i64], Self::incident_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for row in &mut rows {
            row.checks = self.checks_for_incident(&row.id)?;
        }
        Ok(rows)
    }

    /// A check's incidents, newest first — every incident it was attached to,
    /// however it got there.
    pub fn incidents_for_check(&self, check_id: CheckId, limit: usize) -> Result<Vec<IncidentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT i.id, i.title, i.state, i.started_at, i.resolved_at, i.global, i.origin_check
             FROM incidents i
             JOIN incident_checks c ON c.incident_id = i.id
             WHERE c.check_id = ?
             ORDER BY i.started_at DESC
             LIMIT ?",
        )?;
        let mut rows = stmt
            .query_map(duckdb::params![check_id, limit as i64], Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for row in &mut rows {
            row.checks = self.checks_for_incident(&row.id)?;
        }
        Ok(rows)
    }

    fn incident_row(row: &duckdb::Row) -> duckdb::Result<IncidentRow> {
        Ok(IncidentRow {
            id: row.get::<_, String>(0)?,
            title: row.get::<_, String>(1)?,
            state: IncidentState::from_db(&row.get::<_, String>(2)?),
            started_at: row.get::<_, i64>(3)?,
            resolved_at: row.get::<_, Option<i64>>(4)?,
            global: row.get::<_, bool>(5)?,
            origin_check: row.get::<_, Option<CheckId>>(6)?,
            checks: Vec::new(),
        })
    }

    /// One StatusRow per check_id: the latest result's latency/code/error, the
    /// coordinator's published state, and time-weighted uptime over the 24h
    /// window `(now-24h, now]`. A check with no results at all is absent.
    ///
    /// **Uptime is no longer the UP fraction of raw result rows.** That figure
    /// was agent-blind in exactly the way the state machine used to be: a check
    /// probed by four agents with one permanently broken read 75% uptime
    /// forever, because every agent's rows landed in the same numerator. It was
    /// also count-weighted, so changing a check's interval retroactively
    /// reweighted its history, and it had no way to express Degraded because
    /// raw results cannot be Degraded. It is now computed from `state_history`
    /// spans — the same states the status page actually displayed.
    pub fn list_status(&self, now_millis: i64) -> Result<Vec<StatusRow>> {
        let window_start = now_millis - 86_400_000;
        // Which checks count Degraded minutes against their SLA. Read once per
        // call rather than per check: the config document is authoritative for
        // every field that is not a projected column, and this is one of them.
        let degraded_downtime: std::collections::HashSet<CheckId> = self
            .list_checks()?
            .into_iter()
            .filter(|c| c.degraded_is_downtime.unwrap_or(false))
            .map(|c| c.id)
            .collect();

        // Every "latest" field is read from one materialized row. Keeping the
        // complete row avoids both a full-history window scan and the stale NULL
        // behavior of per-column `arg_max`.
        //
        // The displayed status is the coordinator's PUBLISHED state
        // (`current_state`), not the raw last observation: it already accounts
        // for per-agent confirmation, the rollup and hold-down, and that
        // decision must survive a restart. Checks with no row yet (pre-upgrade
        // data) fall back to the raw latest status, lifted into a NodeState.
        let mut stmt = self.conn.prepare(
            "SELECT
                 latest.check_id,
                 COALESCE(cs.status, latest.status) AS status,
                 latest.latency_ms,
                 latest.ts_millis,
                 latest.status_code,
                 latest.error,
                 latest.failure
             FROM latest_check_results latest
             LEFT JOIN current_state cs ON cs.check_id = latest.check_id",
        )?;
        let mut rows = stmt
            .query_map([], |row| {
                Ok(StatusRow {
                    check_id: row.get::<_, CheckId>(0)?,
                    state: NodeState::from_db(&row.get::<_, String>(1)?),
                    latency_ms: row.get::<_, i64>(2)? as u64,
                    last_check: row.get::<_, i64>(3)?,
                    // Filled below: uptime needs a per-check window walk that
                    // does not belong inside a row mapper.
                    uptime_24h: 0.0,
                    status_code: row.get::<_, Option<i32>>(4)?.map(|c| c as u16),
                    error: row.get::<_, Option<String>>(5)?,
                    failure: row
                        .get::<_, Option<String>>(6)?
                        .and_then(|f| serde_json::from_str(&f).ok()),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut spans_by_check = BTreeMap::<CheckId, Vec<StateSpan>>::new();
        let mut span_stmt = self.conn.prepare(
            "SELECT check_id, state, since
             FROM state_history
             WHERE since >= ?
               AND check_id IN (SELECT check_id FROM latest_check_results)
             UNION ALL
             SELECT check_id, arg_max(state, since) AS state, MAX(since) AS since
             FROM state_history
             WHERE since < ?
               AND check_id IN (SELECT check_id FROM latest_check_results)
             GROUP BY check_id
             ORDER BY check_id, since",
        )?;
        let spans = span_stmt.query_map(duckdb::params![window_start, window_start], |row| {
            Ok((
                row.get::<_, CheckId>(0)?,
                StateSpan {
                    state: NodeState::from_db(&row.get::<_, String>(1)?),
                    since: row.get::<_, i64>(2)?,
                },
            ))
        })?;
        for span in spans {
            let (check_id, span) = span?;
            spans_by_check.entry(check_id).or_default().push(span);
        }
        for row in &mut rows {
            let degraded_counts = degraded_downtime.contains(&row.check_id);
            row.uptime_24h = spans_by_check
                .get(&row.check_id)
                .and_then(|spans| uptime_counts(spans, window_start, now_millis, degraded_counts))
                .map(|(up_ms, known_ms)| up_ms as f64 / known_ms as f64)
                // No spans in the window at all — a check upgraded into this
                // scheme moments ago, or one nobody was probing. Reads as fully
                // up rather than fully down: an absence of recorded outage is
                // not an outage, and 0% on every check after an upgrade would
                // be a worse lie than 100%.
                .unwrap_or(1.0);
        }
        Ok(rows)
    }

    /// Persist what the coordinator currently publishes for a check: the
    /// rolled-up state, the hold-down timer in flight, and when it was last
    /// observed. Read back at startup so a restart resumes both the published
    /// state and any window it was part-way through.
    pub fn update_current_state(
        &self,
        check_id: CheckId,
        state: NodeState,
        pending: Option<(NodeState, i64)>,
        last_check: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO current_state
                 (check_id, status, last_check, consecutive_failures, pending_state, pending_since)
             VALUES (?, ?, ?, 0, ?, ?)",
            duckdb::params![
                check_id,
                state.as_str(),
                last_check,
                pending.map(|(s, _)| s.as_str()),
                pending.map(|(_, since)| since),
            ],
        )?;
        Ok(())
    }

    /// All persisted published states, for seeding the tracker on startup.
    pub fn list_current_state(&self) -> Result<Vec<CurrentStateRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT check_id, status, pending_state, pending_since FROM current_state")?;
        let rows = stmt
            .query_map([], |row| {
                let pending_state = row.get::<_, Option<String>>(2)?;
                let pending_since = row.get::<_, Option<i64>>(3)?;
                Ok(CurrentStateRow {
                    check_id: row.get::<_, CheckId>(0)?,
                    state: NodeState::from_db(&row.get::<_, String>(1)?),
                    // Both halves or neither: a `pending_state` with no
                    // timestamp cannot be resumed and a timestamp with no state
                    // cannot be matched against a candidate, so a half-written
                    // pair is discarded rather than guessed at.
                    pending: pending_state
                        .zip(pending_since)
                        .map(|(s, since)| (NodeState::from_db(&s), since)),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Persist one agent's confirmed opinion of one check.
    #[allow(clippy::too_many_arguments)]
    pub fn update_agent_check_state(
        &self,
        check_id: CheckId,
        agent_id: &str,
        zone: &str,
        region: &str,
        status: Status,
        consecutive_failures: u32,
        last_check: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO agent_check_state
                 (check_id, agent_id, status, consecutive_failures, zone, region, last_check)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (check_id, agent_id) DO UPDATE SET
                 status = excluded.status,
                 consecutive_failures = excluded.consecutive_failures,
                 zone = excluded.zone,
                 region = excluded.region,
                 last_check = excluded.last_check
             WHERE excluded.last_check >= agent_check_state.last_check",
            duckdb::params![
                check_id,
                agent_id,
                status.as_str(),
                consecutive_failures as i32,
                zone,
                region,
                last_check
            ],
        )?;
        Ok(())
    }

    /// Every persisted per-agent belief, for seeding the tracker's leaves.
    pub fn list_agent_check_state(&self) -> Result<Vec<AgentCheckStateRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT check_id, agent_id, zone, region, status, consecutive_failures, last_check
              FROM agent_check_state",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AgentCheckStateRow {
                    check_id: row.get::<_, CheckId>(0)?,
                    agent_id: row.get::<_, String>(1)?,
                    zone: row.get::<_, String>(2)?,
                    region: row.get::<_, String>(3)?,
                    status: Status::from_db(&row.get::<_, String>(4)?),
                    consecutive_failures: row.get::<_, i32>(5)? as u32,
                    last_check: row.get::<_, i64>(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ---- Rollup policy ----

    /// Every stored policy. Nodes without a row use their level's default, so
    /// this is usually short and is cached by the caller.
    pub fn list_rollup_policies(&self) -> Result<Vec<RollupPolicyRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT node_id, node_type, degraded_when, down_when, config, updated_at, updated_by
             FROM rollup_policy ORDER BY node_id",
        )?;
        let rows = stmt
            .query_map([], Self::rollup_policy_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn rollup_policy_row(row: &duckdb::Row) -> duckdb::Result<RollupPolicyRow> {
        let config: Option<String> = row.get(4)?;
        let quorums: serde_json::Value = config
            .as_deref()
            .and_then(|c| serde_json::from_str(c).ok())
            .unwrap_or(serde_json::Value::Null);
        let q = |key: &str| quorums.get(key).and_then(|v| v.as_u64()).map(|v| v as u32);
        Ok(RollupPolicyRow {
            node_id: row.get::<_, String>(0)?,
            node_type: NodeType::from_db(&row.get::<_, String>(1)?),
            policy: crate::rollup::RollupPolicy {
                degraded_when: crate::rollup::Threshold::from_db(
                    &row.get::<_, String>(2)?,
                    q("degraded_q"),
                ),
                down_when: crate::rollup::Threshold::from_db(
                    &row.get::<_, String>(3)?,
                    q("down_q"),
                ),
            },
            updated_at: row.get::<_, i64>(5)?,
            updated_by: row.get::<_, Option<String>>(6)?,
        })
    }

    /// Write a node's policy, last-writer-wins on the HLC like every other
    /// replicated config row.
    pub fn upsert_rollup_policy(
        &self,
        node_id: &str,
        node_type: NodeType,
        policy: &crate::rollup::RollupPolicy,
        hlc: &Hlc,
    ) -> Result<()> {
        use crate::rollup::Threshold;
        let mut config = serde_json::Map::new();
        if let Threshold::Quorum(q) = policy.degraded_when {
            config.insert("degraded_q".into(), q.into());
        }
        if let Threshold::Quorum(q) = policy.down_when {
            config.insert("down_q".into(), q.into());
        }
        let config = if config.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(config).to_string())
        };
        self.conn.execute(
            "INSERT OR REPLACE INTO rollup_policy
                 (node_id, node_type, degraded_when, down_when, config, updated_at, updated_by)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                node_id,
                node_type.as_str(),
                policy.degraded_when.as_str(),
                policy.down_when.as_str(),
                config,
                hlc.millis,
                hlc.node_id,
            ],
        )?;
        Ok(())
    }

    /// Drop a node's policy, returning it to its level's default.
    pub fn delete_rollup_policy(&self, node_id: &str) -> Result<bool> {
        let n = self.conn.execute(
            "DELETE FROM rollup_policy WHERE node_id = ?",
            duckdb::params![node_id],
        )?;
        Ok(n > 0)
    }

    // ---- State history ----

    /// Open a new span for a check, closing the previous one implicitly.
    ///
    /// Idempotent on `(check_id, since)`, and a no-op when the newest span
    /// already carries this state: a coordinator that re-publishes the same
    /// conclusion must not fragment its own history into thousands of
    /// zero-length spans.
    pub fn record_state(&self, check_id: CheckId, state: NodeState, since: i64) -> Result<()> {
        let newest: Option<String> = self
            .conn
            .query_row(
                "SELECT state FROM state_history WHERE check_id = ?
                 ORDER BY since DESC LIMIT 1",
                duckdb::params![check_id],
                |r| r.get(0),
            )
            .ok();
        if newest.as_deref().map(NodeState::from_db) == Some(state) {
            return Ok(());
        }
        self.conn.execute(
            "INSERT OR REPLACE INTO state_history (check_id, state, since) VALUES (?, ?, ?)",
            duckdb::params![check_id, state.as_str(), since],
        )?;
        Ok(())
    }

    /// Every span for a check, oldest first.
    pub fn state_spans(&self, check_id: CheckId) -> Result<Vec<StateSpan>> {
        let mut stmt = self.conn.prepare(
            "SELECT state, since FROM state_history WHERE check_id = ? ORDER BY since ASC",
        )?;
        let rows = stmt
            .query_map(duckdb::params![check_id], |r| {
                Ok(StateSpan {
                    state: NodeState::from_db(&r.get::<_, String>(0)?),
                    since: r.get::<_, i64>(1)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Time-weighted uptime over `(from, to]`, as a fraction.
    ///
    /// Counted over spans rather than probes, which fixes three things at once:
    /// a check probed by many agents is no longer scored on how many of them
    /// were healthy, a check whose interval changed is no longer reweighted
    /// retroactively, and `degraded_is_downtime` becomes expressible at all —
    /// raw results have no Degraded to count.
    ///
    /// `Unknown` time is excluded from both numerator and denominator: a
    /// coordinator that was not running, or a check nobody was probing, did not
    /// observe downtime and must not invent uptime either. A window that is
    /// entirely Unknown returns `None` rather than 0.0, so callers can tell
    /// "perfectly broken" from "nothing known".
    pub fn uptime_between(
        &self,
        check_id: CheckId,
        from: i64,
        to: i64,
        degraded_is_downtime: bool,
    ) -> Result<Option<f64>> {
        let spans = self.state_spans(check_id)?;
        let Some((up_ms, known_ms)) = uptime_counts(&spans, from, to, degraded_is_downtime) else {
            return Ok(None);
        };
        Ok(Some(up_ms as f64 / known_ms as f64))
    }

    // ---- Agent credentials ----

    /// Every minted credential, revoked ones included. The caller caches these
    /// and matches against them per request, so this is read at boot and after
    /// each mint or revoke rather than on the ingest path.
    pub fn list_agent_credentials(&self) -> Result<Vec<AgentCredential>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, zone, region, secret_hash, salt, created_at, revoked_at
             FROM agent_credentials ORDER BY region, zone, created_at",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AgentCredential {
                    id: row.get::<_, String>(0)?,
                    zone: row.get::<_, String>(1)?,
                    region: row.get::<_, String>(2)?,
                    secret_hash: row.get::<_, String>(3)?,
                    salt: row.get::<_, String>(4)?,
                    created_at: row.get::<_, i64>(5)?,
                    revoked_at: row.get::<_, Option<i64>>(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// A settings document, or `None` when the key has never been written.
    /// `None` is what makes the config file a seed rather than a default that
    /// silently overwrites an admin's edit on every restart.
    pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM settings WHERE key = ?")?;
        let mut rows = stmt.query_map(duckdb::params![key], |r| r.get::<_, String>(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn put_setting(&self, key: &str, value: &str) -> Result<()> {
        // Delete-then-insert, like `upsert_check`: DuckDB's ON CONFLICT wants
        // the column list twice and this table has one row per page.
        self.conn
            .execute("DELETE FROM settings WHERE key = ?", duckdb::params![key])?;
        self.conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            duckdb::params![key, value],
        )?;
        Ok(())
    }

    pub fn insert_agent_credential(&self, c: &AgentCredential) -> Result<()> {
        self.conn.execute(
            "INSERT INTO agent_credentials
                 (id, zone, region, secret_hash, salt, created_at, revoked_at)
             VALUES (?, ?, ?, ?, ?, ?, NULL)",
            duckdb::params![c.id, c.zone, c.region, c.secret_hash, c.salt, c.created_at],
        )?;
        Ok(())
    }

    /// Returns whether a live credential was actually revoked, so the handler
    /// can 404 rather than report success for an id that never existed — or
    /// report success twice for one already revoked.
    pub fn revoke_agent_credential(&self, id: &str, now: i64) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE agent_credentials SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
            duckdb::params![now, id],
        )?;
        Ok(n > 0)
    }
    /// Delete rows older than the retention window. `results_days` covers raw
    /// probe results; `alerts_days` covers state-transition alerts and CLOSED
    /// incidents. An open incident is current reality, never pruned. Config
    /// tombstones use the LARGER of the two windows: a node down through the
    /// shorter one would come back, still hold a check its tombstone says is
    /// deleted, and hand it straight back on the next anti-entropy sweep —
    /// the exact resurrection tombstones exist to prevent. Returns the number
    /// of rows removed.
    /// Delete raw results older than `before`, keeping the two tables derived
    /// from them true without rebuilding either.
    ///
    /// Both rebuilds used to scan everything that *survived* the delete, which
    /// is the expensive half of retention: pruning one day out of thirty read
    /// twenty-nine. Neither is necessary.
    ///
    /// `result_replication_watermarks.max_ts` is a maximum above the cutoff by
    /// definition, so deleting below the cutoff cannot change it; only
    /// `result_count` moves, by exactly what was removed per check. A check
    /// whose count reaches zero has no rows left and loses its row, which is
    /// what a rebuild would have done.
    ///
    /// `latest_check_results` holds one row per check, the newest. If that row
    /// is at or above the cutoff it survives the delete and is still correct;
    /// if it is below the cutoff then every row for that check was below it
    /// too, so the entry must go. That is a one-row-per-check delete, not a
    /// window function over the whole history.
    pub(crate) fn prune_results(&self, before: i64) -> Result<u64> {
        let before = self.rollup_covered_horizon(before)?;
        self.conn
            .execute_batch("DELETE FROM prune_results_counts; DELETE FROM prune_results_min_ts;")?;
        self.conn.execute(
            "INSERT INTO prune_results_counts
             SELECT check_id, COUNT(*) FROM check_results WHERE ts_millis < ?
             GROUP BY check_id",
            duckdb::params![before],
        )?;
        let removed = self.conn.execute(
            "DELETE FROM check_results WHERE ts_millis < ?",
            duckdb::params![before],
        )? as u64;
        if removed == 0 {
            return Ok(0);
        }
        self.conn.execute(
            "DELETE FROM latest_check_results WHERE ts_millis < ?",
            duckdb::params![before],
        )?;
        // One grouped query over checks touched by this prune advances their
        // retained lower bounds; it avoids issuing a history scan per check.
        self.conn.execute_batch(
            "INSERT INTO prune_results_min_ts
                 SELECT results.check_id, MIN(results.ts_millis)
                 FROM check_results results
                 JOIN prune_results_counts pruned ON pruned.check_id = results.check_id
                 GROUP BY results.check_id;
             UPDATE result_replication_watermarks
SET min_ts = COALESCE(remaining.min_ts, result_replication_watermarks.min_ts),
                     result_count = result_replication_watermarks.result_count - pruned.removed
             FROM prune_results_counts pruned
             LEFT JOIN prune_results_min_ts remaining ON remaining.check_id = pruned.check_id
             WHERE pruned.check_id = result_replication_watermarks.check_id;
             DELETE FROM result_replication_watermarks WHERE result_count <= 0;",
        )?;
        Ok(removed)
    }

    /// How far retention may actually delete, given what has been summarized.
    ///
    /// Raw results are the only copy of a latency observation until a rollup
    /// covers it. Deleting on a clock alone assumes the rollup write always
    /// succeeded, and the one time that assumption is wrong is the one time it
    /// matters: a bug or a partial restore leaves a window with no raw rows
    /// and no summary of them either, and nothing afterwards can tell the
    /// difference between that and a window where nothing was measured.
    ///
    /// So the horizon is pulled back to the start of the oldest minute that
    /// raw data claims and the rollups do not. That is deliberately
    /// conservative — it stalls retention rather than losing history, and a
    /// stall is visible in the database-size metric that is already monitored,
    /// while silent loss is visible in nothing.
    ///
    /// Only minute rollups are consulted. Hour rollups are derived from the
    /// same ingest and cover a superset of the window, so a minute gap is the
    /// first thing to appear and the strictest thing to wait for.
    fn rollup_covered_horizon(&self, requested: i64) -> Result<i64> {
        let uncovered: Option<i64> = self.conn.query_row(
            "SELECT MIN(raw.bucket_start) FROM (
                 SELECT check_id, agent_id,
                        ts_millis // ? * ? AS bucket_start
                 FROM check_results WHERE ts_millis < ?
             ) raw
             ANTI JOIN check_latency_minute_rollups rolled
               ON rolled.check_id = raw.check_id
              AND rolled.agent_id = raw.agent_id
              AND rolled.bucket_start = raw.bucket_start",
            duckdb::params![MILLIS_PER_MINUTE, MILLIS_PER_MINUTE, requested],
            |row| row.get(0),
        )?;
        match uncovered {
            Some(bucket) if bucket < requested => {
                tracing::warn!(
                    horizon = requested,
                    uncovered_from = bucket,
                    "retention: raw results are not yet covered by a minute rollup; \
                     holding the prune horizon back rather than deleting unsummarized history"
                );
                Ok(bucket)
            }
            _ => Ok(requested),
        }
    }

    /// The oldest raw result held, or `None` when there are none. Cheap: a
    /// minimum over a column written in ascending order.
    pub fn oldest_result_ts(&self) -> Result<Option<i64>> {
        let oldest: Option<i64> = self
            .conn
            .query_row("SELECT MIN(ts_millis) FROM check_results", [], |row| {
                row.get(0)
            })
            .unwrap_or(None);
        Ok(oldest)
    }

    /// Deliberately issues no `CHECKPOINT` of its own. `benchmark_checkpoint_after_prune`
    /// measured one at production scale (1,000 checks, 30 raw days, a
    /// one-day/2.88M-row prune): the file never shrinks — two checkpoints
    /// back to back are byte-identical — and DuckDB has already applied the
    /// prune's ~23 MB of WAL by the time an explicit call would run, once its
    /// own automatic threshold trips. Adding one here would only spend
    /// hundreds of milliseconds (431 ms measured, past the single-worker
    /// slow-call threshold) restating work DuckDB already did, for a file
    /// size that would not change. Revisit if `wal_bytes` is ever observed
    /// growing without bound between prunes in production — that would mean
    /// DuckDB's own threshold is not tripping on this workload — not before.
    pub fn prune(&self, results_days: u64, alerts_days: u64, now_millis: i64) -> Result<u64> {
        let results_before = now_millis - (results_days as i64) * 86_400_000;
        let mut removed = self.prune_results(results_before)?;
        // Rollups outlive raw observations. Their cutoffs are independent of
        // the configurable raw window, so reducing raw retention cannot erase
        // the summaries that make old latency views meaningful.
        removed += self.prune_rollups(
            "check_latency_minute_rollups",
            MILLIS_PER_MINUTE,
            now_millis - MINUTE_ROLLUP_RETENTION_MILLIS,
        )?;
        removed += self.prune_rollups(
            "check_latency_hour_rollups",
            MILLIS_PER_HOUR,
            now_millis - HOUR_ROLLUP_RETENTION_MILLIS,
        )?;
        removed += self.prune_metadata(results_days, alerts_days, now_millis)?;
        Ok(removed)
    }

    /// Delete rollup rows fully expired against `before`, taking their
    /// contribution back out of `rollup_replication_watermarks` in the same
    /// pass. `row_count`/`attempts_sum`/`up_sum`/`latency_sum_sum` are
    /// maintained incrementally everywhere else in this file specifically so
    /// a digest never has to scan the rollup tables; a prune that deleted rows
    /// without also subtracting them here would leave that watermark counting
    /// rows that no longer exist, permanently overstating this node to every
    /// peer it digests against. `max_bucket_start` is untouched: pruning only
    /// ever removes the oldest buckets, so the newest one cannot be among them.
    fn prune_rollups(&self, table: &str, bucket_ms: i64, before: i64) -> Result<u64> {
        let (count, attempts, up, latency_sum): (i64, i64, i64, i64) = self.conn.query_row(
            &format!(
                "SELECT COUNT(*), COALESCE(SUM(attempts), 0), COALESCE(SUM(up), 0),
                        COALESCE(SUM(latency_sum), 0)
                 FROM {table} WHERE bucket_start + ? <= ?"
            ),
            duckdb::params![bucket_ms, before],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if count == 0 {
            return Ok(0);
        }
        self.conn.execute(
            &format!("DELETE FROM {table} WHERE bucket_start + ? <= ?"),
            duckdb::params![bucket_ms, before],
        )?;
        self.conn.execute(
            "UPDATE rollup_replication_watermarks SET
                 row_count = row_count - ?,
                 attempts_sum = attempts_sum - ?,
                 up_sum = up_sum - ?,
                 latency_sum_sum = latency_sum_sum - ?
             WHERE stream = ?",
            duckdb::params![count, attempts, up, latency_sum, table],
        )?;
        Ok(count as u64)
    }

    /// Everything retention removes apart from raw results: the event log,
    /// resolved incidents and their narrative, given-up notifications, closed
    /// state spans, and expired tombstones. Bounded by the size of those
    /// streams rather than by raw history, so unlike `prune_results` it has no
    /// reason to be sliced.
    pub(crate) fn prune_metadata(
        &self,
        results_days: u64,
        alerts_days: u64,
        now_millis: i64,
    ) -> Result<u64> {
        // These deletes maintain several denormalized streams. Keep the whole
        // cleanup atomic so a failed watermark rebuild or dependent delete
        // cannot expose an incident without its rows (or stale stream counts).
        self.transaction(|store| {
            store.prune_metadata_uncommitted(results_days, alerts_days, now_millis)
        })
    }

    fn prune_metadata_uncommitted(
        &self,
        results_days: u64,
        alerts_days: u64,
        now_millis: i64,
    ) -> Result<u64> {
        let results_before = now_millis - (results_days as i64) * 86_400_000;
        let alerts_before = now_millis - (alerts_days as i64) * 86_400_000;
        let mut removed = 0u64;
        removed += self.conn.execute(
            "DELETE FROM alerts WHERE ts_millis < ?",
            duckdb::params![alerts_before],
        )? as u64;
        // Closed incidents take their narrative and their affects rows with
        // them: DuckDB has no ON DELETE CASCADE, so leaving these behind would
        // accumulate updates pointing at incidents that no longer exist.
        removed += self.conn.execute(
            "DELETE FROM incident_updates WHERE incident_id IN
                 (SELECT id FROM incidents WHERE state = 'resolved' AND resolved_at < ?)",
            duckdb::params![alerts_before],
        )? as u64;
        removed += self.conn.execute(
            "DELETE FROM incident_checks WHERE incident_id IN
                 (SELECT id FROM incidents WHERE state = 'resolved' AND resolved_at < ?)",
            duckdb::params![alerts_before],
        )? as u64;
        removed += self.conn.execute(
            "DELETE FROM incidents WHERE state = 'resolved' AND resolved_at < ?",
            duckdb::params![alerts_before],
        )? as u64;
        // Given-up notifications are history of the same kind as an alert, so
        // they age out on the same window. Rows still waiting are current work
        // and are never pruned — dropping one silently un-pages an outage.
        removed += self.conn.execute(
            "DELETE FROM notification_queue
             WHERE next_attempt IS NULL AND queued_at < ?",
            duckdb::params![alerts_before],
        )? as u64;
        // State spans age out with results, since that is the history they
        // describe. The *open* span is kept regardless of age: it is not
        // history, it is what the check reads as right now, and deleting it
        // would make a long-stable check look unknown.
        // Written as an anti-join against one grouped pass rather than the
        // correlated `<> (SELECT MAX(since) ... WHERE h.check_id = ...)` it
        // replaced. Worth saying plainly that the correlated form was not the
        // quadratic disaster it reads as: DuckDB decorrelates it, and on 3M
        // spans the two measure 2.4 s against 2.2 s whether those spans are
        // spread over 10,000 checks or concentrated in 100 flapping ones. This
        // form is kept for saying what it means, not for the 10%. See
        // `benchmark_state_history_prune`.
        removed += self.conn.execute(
            "DELETE FROM state_history
             WHERE since < ?
               AND NOT EXISTS (
                   SELECT 1 FROM (
                       SELECT check_id, MAX(since) AS newest
                       FROM state_history GROUP BY check_id
                   ) latest
                   WHERE latest.check_id = state_history.check_id
                     AND latest.newest = state_history.since)",
            duckdb::params![results_before],
        )? as u64;
        let tombstone_days = results_days.max(alerts_days);
        let tombstone_before = now_millis - (tombstone_days as i64) * 86_400_000;
        removed += self.prune_tombstones(tombstone_before)? as u64;
        removed += self.prune_incident_tombstones(tombstone_before)? as u64;
        // A gap not redetected in an entire alert window is either fixed (a
        // snapshot restore, an extended catch-up store) or the peer that
        // reported it is gone; either way it stops being operationally
        // useful to keep past the horizon everything else here ages out on.
        removed += self.conn.execute(
            "DELETE FROM raw_data_gaps WHERE observed_at < ?",
            duckdb::params![alerts_before],
        )? as u64;
        self.rebuild_stream_watermarks()?;
        Ok(removed)
    }

    /// Delete resolved incidents shorter than `max_secs`, with everything
    /// stamped as belonging to them.
    ///
    /// This exists to clean up after the bug the region tree fixes: with
    /// `failures_before_down = 1` and no hold-down, one agent's single failed
    /// probe opened an incident that the next probe closed 50 seconds later.
    /// Those rows are not history anybody wants, and leaving them makes the
    /// incident list unreadable for exactly the checks with the most agents.
    ///
    /// Only *resolved* incidents are touched. An open incident has no duration
    /// yet, and a short-lived open one is simply a new one.
    pub fn purge_short_incidents(&self, max_secs: u64) -> Result<u64> {
        let max_ms = (max_secs as i64).saturating_mul(1000);
        // DuckDB has no ON DELETE CASCADE, so remove every dependent row before
        // its incident. Alerts are deliberately included: this cleanup erases a
        // known false-positive outage, not merely its operator narrative.
        let selector = "SELECT id FROM incidents
             WHERE state = 'resolved' AND resolved_at IS NOT NULL
                AND resolved_at - started_at < ?";
        let mut removed = 0u64;
        for table in ["incident_updates", "incident_checks", "alerts"] {
            removed += self.conn.execute(
                &format!("DELETE FROM {table} WHERE incident_id IN ({selector})"),
                duckdb::params![max_ms],
            )? as u64;
        }

        removed += self.conn.execute(
            "DELETE FROM incidents
             WHERE state = 'resolved' AND resolved_at IS NOT NULL
               AND resolved_at - started_at < ?",
            duckdb::params![max_ms],
        )? as u64;
        if removed != 0 {
            self.rebuild_stream_watermarks()?;
        }
        Ok(removed)
    }

    /// Force DuckDB to apply its WAL to the main file now, rather than
    /// waiting for its own automatic threshold or a clean shutdown. The
    /// prune path deliberately does not call this — see the comment on
    /// `prune` — but a backup is a different situation: it copies the file
    /// out from under this process on a separate durable medium, and DuckDB
    /// can only guarantee that copy is openable if the copy captures a state
    /// the WAL is not needed to reconstruct.
    pub fn checkpoint(&self) -> Result<()> {
        self.conn.execute_batch("CHECKPOINT")?;
        Ok(())
    }

    /// How many raw results are stored. Shown before a purge, so the confirm
    /// dialog can name what it is about to destroy.
    pub fn count_results(&self) -> Result<u64> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM check_results", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    /// Delete every raw result, and the per-agent beliefs derived from them.
    ///
    /// Needed when the fleet's topology is relabelled. Zone and region are
    /// stamped onto each result from the agent's *credential*, so re-minting
    /// credentials under new labels leaves old rows carrying the old ones — and
    /// since the region tree derives its structure from observed locations,
    /// those rows would surface as permanently-stale single-agent regions that
    /// no agent can ever refresh or remove.
    ///
    /// `state_history` is deliberately NOT cleared: it is the coordinator's
    /// record of what it published, which relabelling does not invalidate.
    pub fn purge_results(&self) -> Result<u64> {
        let mut removed = self.conn.execute("DELETE FROM check_results", [])? as u64;
        self.conn
            .execute("DELETE FROM result_replication_watermarks", [])?;
        self.conn.execute("DELETE FROM latest_check_results", [])?;
        removed += self.conn.execute("DELETE FROM agent_check_state", [])? as u64;
        Ok(removed)
    }

    /// Every agent that has ever reported, newest-seen first. Agents are not
    /// registered anywhere — they exist because results carry their `agent_id`,
    /// so this reflects what is actually reporting rather than what is configured.
    ///
    /// Heartbeat pings are written under a reserved id because they arrive over
    /// HTTP with no agent behind them. Listing it would invent a monitoring
    /// agent that nobody deployed and that no pod corresponds to.
    pub fn list_agents(&self, now_millis: i64) -> Result<Vec<AgentRow>> {
        let window_start = now_millis - 86_400_000;
        let mut stmt = self.conn.prepare(
            "SELECT
                 agent_id,
                 max(ts_millis)                  AS last_seen,
                 count(DISTINCT check_id)        AS checks_reporting,
                 SUM(CASE WHEN ts_millis > ? AND ts_millis <= ? THEN 1 ELSE 0 END) AS results_24h,
                 -- Its location as of its newest result, so relocating an agent
                 -- is reflected without any registry to update.
                 arg_max(zone, ts_millis)        AS zone,
                 arg_max(region, ts_millis)      AS region
             FROM check_results
             WHERE agent_id != ?
             GROUP BY agent_id
             ORDER BY last_seen DESC",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![window_start, now_millis, crate::models::HEARTBEAT_AGENT],
                |row| {
                    Ok(AgentRow {
                        agent_id: row.get::<_, String>(0)?,
                        last_seen: row.get::<_, i64>(1)?,
                        zone: row
                            .get::<_, Option<String>>(4)?
                            .unwrap_or_else(|| crate::config::UNKNOWN_LOCATION.into()),
                        region: row
                            .get::<_, Option<String>>(5)?
                            .unwrap_or_else(|| crate::config::UNKNOWN_LOCATION.into()),
                        checks_reporting: row.get::<_, i64>(2)? as u32,
                        results_24h: row.get::<_, Option<i64>>(3)?.unwrap_or(0) as u32,
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Uptime split into `buckets` equal segments over the `days` days ending at
    /// `now_millis`, oldest-first. Segments are `days*DAY/buckets` wide, so the
    /// strip keeps a constant number of bars while a shorter window simply gives
    /// finer resolution (7 days over 90 segments is ~1h52m each).
    ///
    /// A segment with no results has `uptime == None`, which the UI renders as
    /// "no data" rather than as an outage. Always returns exactly `buckets`.
    pub fn uptime_buckets(
        &self,
        check_id: CheckId,
        days: usize,
        buckets: usize,
        now_millis: i64,
        degraded_is_downtime: bool,
    ) -> Result<Vec<UptimeBucket>> {
        let grid = BucketGrid::new(now_millis, days, buckets);
        let buckets = grid.buckets;
        let spans = self.state_spans(check_id)?;
        let mut result_up = vec![0_u64; buckets];
        let mut result_total = vec![0_u64; buckets];
        let mut stmt = self.conn.prepare(
            "SELECT ts_millis, status FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
        )?;
        let rows = stmt.query_map(duckdb::params![check_id, grid.lower, now_millis], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (ts, status) = row?;
            let index = grid.index(ts);
            result_total[index] = result_total[index].saturating_add(1);
            if status == "up" {
                result_up[index] = result_up[index].saturating_add(1);
            }
        }

        Ok((0..buckets)
            .map(|i| {
                let start = grid.lower + i as i64 * grid.bucket_ms;
                let end = start + grid.bucket_ms;
                let uptime = uptime_counts(&spans, start, end, degraded_is_downtime)
                    .map(|(up_ms, known_ms)| up_ms as f64 / known_ms as f64);
                UptimeBucket {
                    start,
                    end,
                    uptime,
                    up: result_up[i],
                    total: result_total[i],
                }
            })
            .collect())
    }

    /// Uptime strips for many checks in two queries rather than two per check.
    ///
    /// The status page draws one strip per visible check, so the per-check path
    /// turned a page load into N round trips and N scans. Here the bucketing is
    /// pushed into DuckDB, so `check_results` is scanned once and yields at most
    /// one row per check per segment.
    ///
    /// `checks` carries each check's resolved `degraded_is_downtime`, because
    /// that is a per-check decision the caller has already made and storage must
    /// not guess at.
    pub fn uptime_strips(
        &self,
        checks: &[(CheckId, bool)],
        days: usize,
        buckets: usize,
        now_millis: i64,
    ) -> Result<BTreeMap<CheckId, CheckUptime>> {
        let grid = BucketGrid::new(now_millis, days, buckets);
        let wanted: std::collections::HashMap<CheckId, bool> = checks.iter().copied().collect();

        let mut spans: BTreeMap<CheckId, Vec<StateSpan>> = BTreeMap::new();
        {
            let mut stmt = self.conn.prepare(
                "SELECT check_id, state, since FROM state_history ORDER BY check_id, since",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, CheckId>(0)?,
                    NodeState::from_db(&row.get::<_, String>(1)?),
                    row.get::<_, i64>(2)?,
                ))
            })?;
            for row in rows {
                let (check_id, state, since) = row?;
                if wanted.contains_key(&check_id) {
                    spans
                        .entry(check_id)
                        .or_default()
                        .push(StateSpan { state, since });
                }
            }
        }

        let mut counts: BTreeMap<CheckId, (Vec<u64>, Vec<u64>)> = BTreeMap::new();
        {
            let mut stmt = self.conn.prepare(
                "SELECT check_id, ((ts_millis - ? - 1) // ?)::BIGINT AS idx,
                        COUNT(*), COUNT(*) FILTER (WHERE status = 'up')
                 FROM check_results
                 WHERE ts_millis > ? AND ts_millis <= ?
                 GROUP BY 1, 2",
            )?;
            let rows = stmt.query_map(
                duckdb::params![grid.lower, grid.bucket_ms, grid.lower, now_millis],
                |row| {
                    Ok((
                        row.get::<_, CheckId>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )?;
            for row in rows {
                let (check_id, index, total, up) = row?;
                if !wanted.contains_key(&check_id) {
                    continue;
                }
                let entry = counts
                    .entry(check_id)
                    .or_insert_with(|| (vec![0; grid.buckets], vec![0; grid.buckets]));
                let index = (index.max(0) as usize).min(grid.buckets - 1);
                entry.0[index] = entry.0[index].saturating_add(up.max(0) as u64);
                entry.1[index] = entry.1[index].saturating_add(total.max(0) as u64);
            }
        }

        Ok(checks
            .iter()
            .map(|(check_id, degraded_is_downtime)| {
                let spans = spans.get(check_id).map(Vec::as_slice).unwrap_or_default();
                let counted = counts.get(check_id);
                let strip = (0..grid.buckets)
                    .map(|i| {
                        let start = grid.lower + i as i64 * grid.bucket_ms;
                        let end = start + grid.bucket_ms;
                        UptimeBucket {
                            start,
                            end,
                            uptime: uptime_counts(spans, start, end, *degraded_is_downtime)
                                .map(|(up_ms, known_ms)| up_ms as f64 / known_ms as f64),
                            up: counted.map(|c| c.0[i]).unwrap_or(0),
                            total: counted.map(|c| c.1[i]).unwrap_or(0),
                        }
                    })
                    .collect();
                let overall = uptime_counts(spans, grid.lower, now_millis, *degraded_is_downtime)
                    .map(|(up_ms, known_ms)| up_ms as f64 / known_ms as f64);
                (
                    *check_id,
                    CheckUptime {
                        buckets: strip,
                        overall,
                    },
                )
            })
            .collect())
    }

    /// Aggregate latency from disjoint retention tiers. Buckets ending at a
    /// handoff belong to the older tier; raw observations start strictly after
    /// that handoff. This matches the `(from, to]` public window convention and
    /// prevents a probe on a boundary from being counted twice.
    fn latency_source_sql(from: i64, to: i64) -> String {
        let raw_start =
            (to - RAW_RETENTION_MILLIS).div_euclid(MILLIS_PER_MINUTE) * MILLIS_PER_MINUTE;
        let minute_start =
            (to - MINUTE_ROLLUP_RETENTION_MILLIS).div_euclid(MILLIS_PER_HOUR) * MILLIS_PER_HOUR;
        format!(
            "SELECT check_id, agent_id, ts_millis, 1::BIGINT attempts,
                    (status = 'up')::BIGINT up,
                    CASE WHEN status = 'up' THEN latency_ms ELSE 0 END latency_sum,
                    CASE WHEN status = 'up' THEN latency_ms END latency_min,
                    CASE WHEN status = 'up' THEN latency_ms END latency_max
             FROM check_results WHERE ts_millis > {from} AND ts_millis <= {to}
                AND ts_millis > {raw_start}
             UNION ALL
             SELECT check_id, agent_id, bucket_start + {MILLIS_PER_MINUTE}, attempts, up,
                    latency_sum, latency_min, latency_max
             FROM check_latency_minute_rollups
             WHERE bucket_start >= {minute_start} AND bucket_start < {raw_start}
               -- A rollup is indivisible: only use it when its entire bucket
               -- lies inside the requested `(from, to]` window.
               AND bucket_start >= {from}
               AND bucket_start + {MILLIS_PER_MINUTE} <= {to}
             UNION ALL
             SELECT check_id, agent_id, bucket_start + {MILLIS_PER_HOUR}, attempts, up,
                    latency_sum, latency_min, latency_max
             FROM check_latency_hour_rollups
             WHERE bucket_start < {minute_start}
               AND bucket_start >= {from}
               AND bucket_start + {MILLIS_PER_HOUR} <= {to}",
        )
    }

    /// Sparkline points for many checks in one scan.
    ///
    /// Deliberately points only: the status page's row chart reads nothing else,
    /// while the per-agent breakdown and overall band belong to the single-check
    /// detail view, which keeps using `latency_series`. Computing them here would
    /// double the scan to populate fields nobody on this page renders.
    pub fn latency_points_for(
        &self,
        checks: &[CheckId],
        from: i64,
        to: i64,
        buckets: usize,
    ) -> Result<BTreeMap<CheckId, Vec<LatencyPoint>>> {
        let buckets = buckets.max(1);
        let bucket_ms = ((to - from) / buckets as i64).max(1);
        let wanted: std::collections::HashSet<CheckId> = checks.iter().copied().collect();

        let mut folded: BTreeMap<CheckId, SegmentCounts> = BTreeMap::new();
        let source = Self::latency_source_sql(from, to);
        let mut stmt = self.conn.prepare(&format!(
            "SELECT check_id, ((ts_millis - ? - 1) // ?)::BIGINT AS idx,
                    SUM(attempts), SUM(up), COALESCE(SUM(latency_sum), 0)
             FROM ({source}) observations
             GROUP BY 1, 2"
        ))?;
        let rows = stmt.query_map(duckdb::params![from, bucket_ms], |row| {
            Ok((
                row.get::<_, CheckId>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        for row in rows {
            let (check_id, index, attempts, up, sum_ms) = row?;
            if !wanted.contains(&check_id) {
                continue;
            }
            let entry = folded
                .entry(check_id)
                .or_insert_with(|| SegmentCounts::new(buckets));
            let index = (index.max(0) as usize).min(buckets - 1);
            entry.sum_ms[index] += sum_ms.max(0) as f64;
            entry.attempts[index] = entry.attempts[index].saturating_add(attempts.max(0) as u32);
            entry.up[index] = entry.up[index].saturating_add(up.max(0) as u32);
        }

        Ok(checks
            .iter()
            .map(|check_id| {
                let counted = folded.get(check_id);
                let points = (0..buckets)
                    .map(|i| {
                        let up = counted.map(|c| c.up[i]).unwrap_or(0);
                        LatencyPoint {
                            timestamp: from + i as i64 * bucket_ms,
                            attempts: counted.map(|c| c.attempts[i]).unwrap_or(0),
                            up,
                            avg_latency_ms: (up > 0)
                                .then(|| counted.map(|c| c.sum_ms[i]).unwrap_or(0.0) / up as f64),
                        }
                    })
                    .collect();
                (*check_id, points)
            })
            .collect())
    }

    /// Average latency for a check over an explicit window, split into the
    /// requested number of equal segments (oldest first), plus exact per-agent
    /// attribution over the same window. Segments with no attempts carry
    /// `None`, not zero, so sparse timestamps are represented faithfully.
    ///
    /// The window is explicit rather than `now`-anchored so a historical
    /// segment (a clicked uptime bar) can be charted truthfully instead of
    /// being approximated by the newest N rows.
    pub fn latency_series(
        &self,
        check_id: CheckId,
        from: i64,
        to: i64,
        buckets: usize,
    ) -> Result<LatencySeries> {
        // The window and grid are exactly what the caller asked for. A young
        // check or sparse probe schedule legitimately leaves empty segments;
        // those are `None`, never invented 0ms measurements. Keeping every
        // requested segment also lets the client align series across ranges.
        let buckets = buckets.max(1);
        let bucket_ms = ((to - from) / buckets as i64).max(1);

        let mut sum = vec![0.0f64; buckets];
        let mut attempts = vec![0u32; buckets];
        let mut up = vec![0u32; buckets];
        let mut agents: BTreeMap<String, LatencyBand> = BTreeMap::new();

        let source = Self::latency_source_sql(from, to);
        let mut stmt = self.conn.prepare(&format!(
            "SELECT ts_millis, latency_sum, agent_id, attempts, up, latency_min, latency_max
             FROM ({source}) observations WHERE check_id = ?"
        ))?;
        let rows = stmt.query_map(duckdb::params![check_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })?;
        for r in rows {
            let (ts, latency_sum, agent_id, row_attempts, row_up, latency_min, latency_max) = r?;
            // Lower bound is exclusive, so shift before dividing: a timestamp
            // landing exactly on a boundary belongs to the segment below it.
            let i = (((ts - from - 1).max(0) / bucket_ms) as usize).min(buckets - 1);
            attempts[i] = attempts[i].saturating_add(row_attempts.max(0) as u32);
            sum[i] += latency_sum.max(0) as f64;
            up[i] = up[i].saturating_add(row_up.max(0) as u32);
            let band = agents.entry(agent_id).or_default();
            band.attempts = band.attempts.saturating_add(row_attempts.max(0) as u32);
            band.up = band.up.saturating_add(row_up.max(0) as u32);
            band.sum_ms += latency_sum.max(0) as f64;
            if let Some(minimum) = latency_min {
                let minimum = minimum as u64;
                band.min_ms = Some(band.min_ms.map_or(minimum, |old| old.min(minimum)));
            }
            if let Some(maximum) = latency_max {
                band.max_ms = Some(
                    band.max_ms
                        .map_or(maximum as u64, |old| old.max(maximum as u64)),
                );
            }
        }

        let points = (0..buckets)
            .map(|i| LatencyPoint {
                timestamp: from + i as i64 * bucket_ms,
                attempts: attempts[i],
                up: up[i],
                avg_latency_ms: (up[i] > 0).then(|| sum[i] / up[i] as f64),
            })
            .collect();

        let mut overall = LatencyBand::default();
        for b in agents.values() {
            overall.merge(b);
        }
        Ok(LatencySeries {
            points,
            overall,
            agents: agents
                .into_iter()
                .map(|(agent_id, band)| AgentBand { agent_id, band })
                .collect(),
        })
    }

    /// Seed synthetic data spanning ~90 days before `now_millis` when the store
    /// is empty. Idempotent: a no-op when check_results already has rows.
    /// Returns true if seeding occurred.
    pub fn seed_mock(&self, now_millis: i64) -> Result<bool> {
        let count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM check_results", [], |r| r.get(0))?;
        if count > 0 {
            return Ok(false);
        }

        let checks = [
            "api-gateway",
            "postgres-primary",
            "redis-cache",
            "web-frontend",
        ];
        // Demo data has no corresponding `checks` row (there is nothing to
        // seed identity from), so each mock name gets its own synthetic id,
        // used consistently for every row this function writes about it.
        let ids: std::collections::HashMap<&str, CheckId> = checks
            .iter()
            .map(|name| (*name, crate::models::new_check_id()))
            .collect();
        let days = 90usize;
        const DAY: i64 = 86_400_000;
        // Sample timestamps oldest-first. Older days are sparse (30-min); the
        // most recent 24h is dense (5-min) so the latency sparkline is detailed
        // without seeding 90 days at high resolution.
        let recent_step = 5 * 60 * 1000i64; // 5 min
        let sparse_step = 30 * 60 * 1000i64; // 30 min
                                             // day 0 = oldest, day (days-1) = today. `day_of(ts)` maps a timestamp to
                                             // its day index for outage lookup and alert placement.
        let day_of = |ts: i64| -> usize {
            let ago = ((now_millis - ts) / DAY) as usize;
            (days - 1).saturating_sub(ago)
        };
        let mut samples: Vec<i64> = Vec::new();
        // Sparse span: from 90 days ago up to 24h ago.
        let mut ts = now_millis - days as i64 * DAY;
        while ts <= now_millis - DAY {
            samples.push(ts);
            ts += sparse_step;
        }
        // Dense span: last 24h.
        ts = now_millis - DAY + recent_step;
        while ts <= now_millis {
            samples.push(ts);
            ts += recent_step;
        }

        // Outage windows expressed as (check, inclusive day range) where the
        // check reports DOWN. day 0 is the oldest, day (days-1) is today.
        let is_down = |name: &str, day: usize| -> bool {
            match name {
                // A full bad day ~30 days ago plus a recent blip.
                "redis-cache" => day == 60 || day == 88,
                // One brief outage ~20 days ago.
                "api-gateway" => day == 70,
                _ => false,
            }
        };

        let mut rows: Vec<(CheckResult, Status)> = Vec::new();
        for name in checks {
            for (i, &ts) in samples.iter().enumerate() {
                let down = is_down(name, day_of(ts));
                let (status, code, latency) = if down {
                    (Status::Down, Some(503u16), 0)
                } else {
                    (Status::Up, Some(200u16), 20 + ((i % 7) * 5) as i64)
                };
                rows.push((
                    CheckResult {
                        body: None,
                        headers: None,
                        metric: None,
                        metrics: None,
                        zone: "seed".into(),
                        region: "seed".into(),
                        check_id: ids[name],
                        agent_id: "seed".to_string(),
                        kind: crate::models::CheckKind::Http,
                        timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
                        latency_ms: latency as u64,
                        status_code: code,
                        body_matched: None,
                        error: if down {
                            Some("503 Service Unavailable".into())
                        } else {
                            None
                        },
                        failure: down.then_some(crate::models::FailureKind::UnexpectedStatus),
                    },
                    status,
                ));
            }
        }
        // `insert_results` goes through a prepared statement per row, which
        // is the right shape for a real ingest batch (tens of rows) but not
        // for the ~18k rows 90 days of demo history produces: DuckDB's
        // execution engine is vectorized, and single-row `execute` calls pay
        // for that on every call. The Appender API is the bulk-load path it
        // documents for exactly this case, so seeding stays fast even though
        // ordinary ingest has no reason to use it.
        self.append_results_bulk(&rows)?;

        // Matching incidents at the start of each bad day and its recovery, so
        // the incidents list mirrors the bars. Seeded through the real
        // lifecycle — an incident with its opening and resolving updates, and
        // alerts stamped with its id — so the demo exercises the same shapes
        // the coordinator writes.
        let day_start_ts = |day: usize| now_millis - (days - 1 - day) as i64 * DAY;
        let outage = |name: &str, down_day: usize| -> Result<()> {
            let check_id = ids[name];
            let started = day_start_ts(down_day);
            let ended = day_start_ts(down_day + 1);
            let id = format!("{name}-{started}");
            self.create_incident(
                &id,
                &format!("{name} unreachable"),
                IncidentState::Detected,
                started,
                false,
                Some(check_id),
                &[IncidentCheckRow {
                    check_id,
                    source: AffectSource::Origin,
                }],
                Some(("connection refused", None)),
            )?;
            self.insert_alert(
                AlertScope::Check,
                &check_id.to_string(),
                Some(NodeState::Up),
                NodeState::Down,
                started,
                Some(&id),
            )?;
            self.insert_alert(
                AlertScope::Check,
                &check_id.to_string(),
                Some(NodeState::Down),
                NodeState::Up,
                ended,
                Some(&id),
            )?;
            self.add_update(
                &id,
                IncidentState::Resolved,
                "Recovered; the check has been passing since.",
                None,
                ended,
            )?;
            Ok(())
        };
        outage("redis-cache", 60)?;
        outage("redis-cache", 88)?;
        outage("api-gateway", 70)?;

        Ok(true)
    }

    // ---- Cluster membership ----

    /// Persist the member view so a restarted node rejoins from what it knew.
    /// Delete-then-insert the whole set in one transaction: DuckDB commits per
    /// statement otherwise, which is the difference between one fsync and one
    /// per member on every heartbeat.
    pub fn save_cluster_members(
        &self,
        members: &[crate::coordinator::cluster::Member],
    ) -> Result<()> {
        self.conn.execute_batch("BEGIN")?;
        let write = || -> Result<()> {
            self.conn.execute_batch("DELETE FROM cluster_members")?;
            let mut stmt = self.conn.prepare(
                "INSERT INTO cluster_members (node_id, url, generation, last_seen, state)
                 VALUES (?, ?, ?, ?, ?)",
            )?;
            for m in members {
                stmt.execute(duckdb::params![
                    m.node_id,
                    m.url,
                    m.generation,
                    m.last_seen,
                    m.state.as_str(),
                ])?;
            }
            Ok(())
        };
        match write() {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Every persisted member, for rejoining the cluster on restart.
    pub fn list_cluster_members(&self) -> Result<Vec<crate::coordinator::cluster::Member>> {
        let mut stmt = self
            .conn
            .prepare("SELECT node_id, url, generation, last_seen, state FROM cluster_members")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::coordinator::cluster::Member {
                    node_id: row.get::<_, String>(0)?,
                    url: row.get::<_, String>(1)?,
                    generation: row.get::<_, i64>(2)?,
                    last_seen: row.get::<_, i64>(3)?,
                    state: crate::coordinator::cluster::MemberState::from_db(
                        &row.get::<_, String>(4)?,
                    ),
                    // Not stored: see `Member::build`. A restored row says who
                    // was out there, not what they are running now; the first
                    // ping after boot fills this in.
                    build: None,
                    protocol: None,
                })
            })?
            .collect::<std::result::Result<_, _>>()?;
        Ok(rows)
    }

    // ---- Data-plane replication (results, alerts, incidents) ----

    /// Insert only rows this node does not already have. Returns how many
    /// were actually inserted.
    ///
    /// `check_results` carries no unique constraint — adding a primary key to
    /// a hot table is a migration this project does not do — so dedup is an
    /// anti-join on `(check_id, agent_id, ts_millis)`, done once for the whole
    /// batch through a staging table rather than once per row.
    ///
    /// Most of a batch never reaches the anti-join: rows newer than the
    /// per-check high-water mark cannot be duplicates, which is every row of an
    /// ordinary replication push. What remains — a repair replaying old rows —
    /// is probed with the scan bounded to that subset's own time range. The
    /// batch is also deduplicated against itself, so the same row arriving
    /// twice in one batch inserts once. Replaying a whole batch remains a
    /// no-op, which is what anti-entropy depends on.
    pub fn insert_results_dedup(&self, rows: &[(CheckResult, Status)]) -> Result<usize> {
        self.transaction(|store| store.insert_results_dedup_uncommitted(rows))
    }

    pub(crate) fn insert_results_dedup_uncommitted(
        &self,
        rows: &[(CheckResult, Status)],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.conn.execute_batch(
            "DELETE FROM ingest_results_stage;
             DELETE FROM replicate_new_results;
             DELETE FROM replicate_probe_stage;",
        )?;
        self.stage_results(rows)?;
        // Split the batch on this node's own high-water mark before touching
        // `check_results` at all. A row newer than the newest one held for its
        // check cannot be a duplicate of anything, which in steady-state
        // replication is every row in the batch — so the ordinary path never
        // probes the history table, and its cost stops depending on how much
        // history there is. Only repair, which replays old rows by definition,
        // pays for the anti-join.
        //
        // This rests on `result_replication_watermarks.max_ts` being the true
        // maximum for the check. Every writer of `check_results` maintains it
        // in the same statement or transaction, `Storage::open` backfills it,
        // and `prune` keeps it true by construction — deleting rows below a
        // cutoff cannot lower a maximum above it.
        self.conn.execute_batch(
            "INSERT INTO replicate_new_results
             SELECT staged.* FROM ingest_results_stage staged
             LEFT JOIN result_replication_watermarks mark
                    ON mark.check_id = staged.check_id
             WHERE mark.max_ts IS NULL OR staged.ts_millis > mark.max_ts
             QUALIFY row_number() OVER (
                 PARTITION BY staged.check_id, staged.agent_id, staged.ts_millis
                 ORDER BY staged.ts_millis
             ) = 1;
             INSERT INTO replicate_probe_stage
             SELECT staged.* FROM ingest_results_stage staged
             JOIN result_replication_watermarks mark
               ON mark.check_id = staged.check_id
             WHERE staged.ts_millis <= mark.max_ts;",
        )?;
        let probed: i64 =
            self.conn
                .query_row("SELECT count(*) FROM replicate_probe_stage", [], |row| {
                    row.get(0)
                })?;
        if probed > 0 {
            self.conn.execute_batch(
                "INSERT INTO replicate_new_results
                 SELECT staged.* FROM replicate_probe_stage staged
                 WHERE NOT EXISTS (
                     SELECT 1 FROM check_results existing
                     WHERE existing.check_id = staged.check_id
                       AND existing.agent_id = staged.agent_id
                       AND existing.ts_millis = staged.ts_millis
                       -- Sound because a duplicate of a staged row carries that
                       -- row's exact `ts_millis`, never below this minimum, so
                       -- nothing that could match is excluded. Results are
                       -- appended in time order, so the bound prunes by zone
                       -- map instead of probing every retained row.
                       AND existing.ts_millis >= (SELECT MIN(ts_millis) FROM replicate_probe_stage)
                 )
                 QUALIFY row_number() OVER (
                     PARTITION BY staged.check_id, staged.agent_id, staged.ts_millis
                     ORDER BY staged.ts_millis
                 ) = 1;",
            )?;
        }
        self.conn.execute_batch(&format!(
            "INSERT INTO check_results SELECT * FROM replicate_new_results;
             {derived}",
            derived = derived_results_sql("replicate_new_results"),
        ))?;
        let inserted: i64 =
            self.conn
                .query_row("SELECT count(*) FROM replicate_new_results", [], |row| {
                    row.get(0)
                })?;
        Ok(inserted as usize)
    }

    /// Insert a replicated alert exactly once. An existing id is a replay only
    /// when its complete immutable payload agrees; replacing a different event
    /// would silently corrupt the event log after an id collision, so a
    /// disagreeing payload is reported as `Conflict` and nothing is written.
    ///
    /// Reported rather than raised because the caller is applying a batch:
    /// failing the transaction would make one disagreeing row permanently block
    /// every other row travelling with it, and anti-entropy would replay that
    /// same doomed batch forever.
    pub fn upsert_alert(&self, a: &AlertRow) -> Result<AlertApply> {
        let existing = self
            .conn
            .query_row(
                "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts WHERE id = ?",
                duckdb::params![a.id],
                Self::alert_row,
            )
            .ok();
        if let Some(existing) = existing {
            return Ok(if existing == *a {
                AlertApply::Replay
            } else {
                AlertApply::Conflict
            });
        }
        self.conn.execute(
            "INSERT INTO alerts
                 (id, scope, node_id, previous_state, current_state, ts_millis, incident_id)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                a.id,
                a.scope.as_str(),
                a.node_id,
                a.previous_state.map(|s| s.as_str()),
                a.current_state.as_str(),
                a.timestamp,
                a.incident_id,
            ],
        )?;
        self.record_stream_item("alerts", a.timestamp, &a.id, 1)?;
        Ok(AlertApply::Inserted)
    }

    /// Apply a replicated incident by id. Exact replays are no-ops; an update
    /// id may be reused only for its identical immutable payload, never to
    /// overwrite a different incident's narrative.
    pub fn upsert_incident(&self, i: &ReplicatedIncident) -> Result<bool> {
        self.transaction(|store| store.upsert_incident_uncommitted(i))
    }

    pub(crate) fn upsert_incident_uncommitted(&self, i: &ReplicatedIncident) -> Result<bool> {
        // Rows and tombstones share one total order. A newer incident may
        // intentionally recreate a deleted id; an equal/newer tombstone wins,
        // while a stale delete cannot erase a newer row.
        let superseded_tombstone = match self.incident_tombstone(&i.id)? {
            Some(t) => {
                let tombstone_version = Hlc::new(t.deleted_at, t.node_id);
                if tombstone_version >= i.version {
                    return Ok(false);
                }
                true
            }
            None => false,
        };
        let existed = if let Some(existing) = self.replicated_incident(&i.id)? {
            if existing.version > i.version || existing == *i {
                return Ok(false);
            }
            if existing.version == i.version
                && serde_json::to_vec(&existing)? >= serde_json::to_vec(i)?
            {
                return Ok(false);
            }
            true
        } else {
            false
        };
        for update in &i.updates {
            if update.incident_id != i.id {
                bail!(
                    "replicated update {} belongs to {}, not {}",
                    update.id,
                    update.incident_id,
                    i.id
                );
            }
            let existing = self
                .conn
                .query_row(
                    "SELECT id, incident_id, state, body, author, created_at
                 FROM incident_updates WHERE id = ?",
                    duckdb::params![update.id],
                    |row| {
                        Ok(IncidentUpdateRow {
                            id: row.get(0)?,
                            incident_id: row.get(1)?,
                            state: IncidentState::from_db(&row.get::<_, String>(2)?),
                            body: row.get(3)?,
                            author: row.get(4)?,
                            created_at: row.get(5)?,
                        })
                    },
                )
                .ok();
            if let Some(existing) = existing {
                if existing.incident_id != i.id {
                    bail!(
                        "replicated incident update id {} belongs to another incident",
                        update.id
                    );
                }
            }
        }
        let write = || -> Result<()> {
            self.conn
                .execute("DELETE FROM incidents WHERE id = ?", duckdb::params![i.id])?;
            if superseded_tombstone {
                self.conn.execute(
                    "DELETE FROM incident_tombstones WHERE id = ?",
                    duckdb::params![i.id],
                )?;
            }
            self.conn.execute(
                "INSERT INTO incidents
                     (id, title, state, started_at, resolved_at, global, origin_check,
                      version_millis, version_node)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    i.id,
                    i.title,
                    i.state.as_str(),
                    i.started_at,
                    i.resolved_at,
                    i.global,
                    i.origin_check,
                    i.version.millis,
                    i.version.node_id,
                ],
            )?;
            self.conn.execute(
                "DELETE FROM incident_checks WHERE incident_id = ?",
                duckdb::params![i.id],
            )?;
            for (check_id, source) in &i.checks {
                self.conn.execute(
                    "INSERT INTO incident_checks (incident_id, check_id, source)
                     VALUES (?, ?, ?)",
                    duckdb::params![i.id, check_id, source.as_str()],
                )?;
            }
            self.conn.execute(
                "DELETE FROM incident_updates WHERE incident_id = ?",
                duckdb::params![i.id],
            )?;
            for u in &i.updates {
                self.conn.execute(
                    "INSERT INTO incident_updates
                         (id, incident_id, state, body, author, created_at)
                      VALUES (?, ?, ?, ?, ?, ?)",
                    duckdb::params![u.id, i.id, u.state.as_str(), u.body, u.author, u.created_at],
                )?;
            }
            let cursor_id = format!("{}\0{}", i.version.node_id, i.id);
            self.record_stream_item(
                "incidents",
                i.version.millis,
                &cursor_id,
                i64::from(!existed),
            )?;
            if superseded_tombstone {
                self.rebuild_stream_watermarks()?;
            }
            Ok(())
        };
        write()?;
        Ok(true)
    }

    /// One incident in the shape it replicates as. `None` when it does not
    /// exist locally.
    pub fn replicated_incident(&self, id: &str) -> Result<Option<ReplicatedIncident>> {
        let Some(row) = self.incident(id)? else {
            return Ok(None);
        };
        let updates = self.updates_for_incident(id)?;
        Ok(Some(ReplicatedIncident {
            id: row.id,
            version: self.conn.query_row(
                "SELECT version_millis, version_node FROM incidents WHERE id = ?",
                duckdb::params![id],
                |r| Ok(Hlc::new(r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )?,
            title: row.title,
            state: row.state,
            started_at: row.started_at,
            resolved_at: row.resolved_at,
            global: row.global,
            origin_check: row.origin_check,
            checks: row
                .checks
                .into_iter()
                .map(|c| (c.check_id, c.source))
                .collect(),
            updates,
        }))
    }

    // ---- Anti-entropy ----

    /// A fixed-cost summary of everything replicated, for a peer to compare
    /// against its own. Full retained-history digests read compact metadata
    /// maintained on write; explicitly windowed digests scan only that window.
    pub fn digest(&self, window_start: i64) -> Result<Digest> {
        let checks = {
            let (sql, params): (&str, Vec<i64>) = if window_start == 0 {
                (
                    "SELECT check_id, min_ts, max_ts, result_count
                      FROM result_replication_watermarks
                     ORDER BY check_id",
                    Vec::new(),
                )
            } else {
                (
                    "SELECT check_id, MIN(ts_millis), MAX(ts_millis), COUNT(*) FROM check_results
                      WHERE ts_millis >= ? GROUP BY check_id ORDER BY check_id",
                    vec![window_start],
                )
            };
            let mut stmt = self.conn.prepare(sql)?;
            let rows = stmt.query_map(duckdb::params_from_iter(params), |row| {
                Ok(CheckDigest {
                    check_id: row.get(0)?,
                    min_ts: row.get(1)?,
                    max_ts: row.get(2)?,
                    count: row.get(3)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let alerts = self.stream_digest("alerts")?;
        let incidents = self.stream_digest("incidents")?;
        let incident_tombstones = self.stream_digest("incident_tombstones")?;
        let latency_minute_rollups = self.rollup_digest("check_latency_minute_rollups")?;
        let latency_hour_rollups = self.rollup_digest("check_latency_hour_rollups")?;
        let config = self.config_digest()?;
        Ok(Digest {
            checks,
            window_start,
            alerts,
            incidents,
            incident_tombstones,
            latency_minute_rollups,
            latency_hour_rollups,
            config,
        })
    }

    /// Every config kind's digest, with no results/alerts/rollup scan riding
    /// along — what `digest()` computes for its own `config` field, exposed on
    /// its own for a store that holds only the config plane. The global tier
    /// (`global::store::GlobalStore`) is the reason this exists: it has no
    /// results to digest and would otherwise have to call the much more
    /// expensive `digest()` and throw away everything but this part.
    pub fn config_digest(&self) -> Result<Vec<ConfigDigest>> {
        ConfigKind::ALL
            .into_iter()
            .map(|k| self.config_digest_for(k))
            .collect()
    }

    /// One kind's contribution to a digest: how many entries it has, and the
    /// highest stamp among them. Rows and tombstones are unioned into the same
    /// scan, because a peer missing a delete is exactly as behind as one
    /// missing an edit — the digest must not be able to look "caught up" while
    /// still missing a tombstone.
    fn config_digest_for(&self, kind: ConfigKind) -> Result<ConfigDigest> {
        let (table, _) = kind.table_and_pk();
        let sql = format!(
            "SELECT millis, node FROM (
                 SELECT updated_at AS millis, updated_by AS node FROM {table}
                 UNION ALL
                 SELECT deleted_at AS millis, node_id AS node FROM config_tombstones
                 WHERE kind = ?
             )"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(duckdb::params![kind.as_str()], |r| {
                Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<String>>(1)?))
            })?
            .collect::<duckdb::Result<Vec<_>>>()?;
        Ok(Self::digest_from_stamp_rows(kind, rows))
    }

    /// `region`'s placement-scoped contribution to a Check-kind digest: the
    /// same shape `config_digest_for` produces for the whole table, but the
    /// live-row half is restricted to checks `region`'s placement admits (via
    /// `check_placement_regions`, never a `config` JSON parse). Tombstones
    /// stay unscoped — an outright delete propagates to every region
    /// regardless of placement, since nothing here tracks a check's *past*
    /// placement to scope a tombstone against.
    ///
    /// This is what makes Phase 2 reconciliation subset-correct instead of
    /// amplifying: comparing a *whole-catalogue* check digest against a
    /// region that legitimately holds only its placed subset would never
    /// converge, and would re-page the full catalogue every sweep — the
    /// exact cost this project has already removed twice elsewhere (see
    /// `STREAM_RESCAN_INTERVAL` and the replicated-dedup high-water mark).
    /// A region that genuinely holds what its placement calls for produces a
    /// digest that matches this one, so a converged region costs one HTTP
    /// digest comparison per sweep and nothing else.
    pub fn config_digest_for_region(&self, region: &str) -> Result<ConfigDigest> {
        let sql = "
            SELECT millis, node FROM (
                SELECT c.updated_at AS millis, c.updated_by AS node
                FROM checks c
                WHERE NOT EXISTS (
                          SELECT 1 FROM check_placement_regions r WHERE r.check_id = c.id
                      )
                   OR EXISTS (
                          SELECT 1 FROM check_placement_regions r
                          WHERE r.check_id = c.id AND r.region = ?
                      )
                UNION ALL
                SELECT deleted_at AS millis, node_id AS node
                FROM config_tombstones WHERE kind = 'check'
            )";
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt
            .query_map(duckdb::params![region], |r| {
                Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<String>>(1)?))
            })?
            .collect::<duckdb::Result<Vec<_>>>()?;
        Ok(Self::digest_from_stamp_rows(ConfigKind::Check, rows))
    }

    /// Folds `(millis, node)` stamp pairs — a row or tombstone's contribution
    /// — into the count/max-stamp shape every digest query above produces.
    /// Shared so the two queries above cannot silently compute "caught up"
    /// differently from one another.
    fn digest_from_stamp_rows(
        kind: ConfigKind,
        rows: Vec<(Option<i64>, Option<String>)>,
    ) -> ConfigDigest {
        let mut count = 0i64;
        let mut max_hlc: Option<Hlc> = None;
        for (millis, node) in rows {
            count += 1;
            // NULL here is a row written before stamps existed: the oldest
            // possible HLC, so it never wins a max() against a real one.
            let hlc = Hlc::new(millis.unwrap_or(0), node.unwrap_or_default());
            max_hlc = Some(match max_hlc {
                Some(m) if m >= hlc => m,
                _ => hlc,
            });
        }
        ConfigDigest {
            kind,
            max_hlc,
            count,
        }
    }

    /// Paged raw rows for backfill. `(since, since_agent_id)` is exclusive and
    /// rows come back oldest-first so the caller can advance the stable cursor.
    /// Without an agent id, all rows at `since` are excluded for the first page.
    ///
    /// `kind`, `body_matched`, `body`, `headers`, `metric` and `metrics` are
    /// never in `check_results` — the table keeps the coordinator's verdict,
    /// not the probe's raw observation — so they read back as their defaults.
    /// The replica gets what was persisted, not what was probed, and that is
    /// the whole point of replicating the observer's verdict rather than
    /// re-evaluating it on every node.
    pub fn results_since(
        &self,
        check_id: CheckId,
        since: i64,
        since_agent_id: Option<&str>,
        until: i64,
        limit: usize,
    ) -> Result<Vec<(CheckResult, Status)>> {
        let mut stmt = self.conn.prepare(
            "SELECT agent_id, ts_millis, status, latency_ms, status_code, error,
                    failure, zone, region
             FROM check_results
             WHERE check_id = ? AND ts_millis <= ?
               AND (ts_millis > ? OR (? IS NOT NULL AND ts_millis = ? AND agent_id > ?))
             ORDER BY ts_millis ASC, agent_id ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![
                    check_id,
                    until,
                    since,
                    since_agent_id,
                    since,
                    since_agent_id,
                    limit as i64
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<i32>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut out = Vec::with_capacity(rows.len());
        for (agent_id, ts, status, latency_ms, status_code, error, failure, zone, region) in rows {
            let result = CheckResult {
                check_id,
                agent_id,
                zone: zone.unwrap_or_default(),
                region: region.unwrap_or_default(),
                // Never stored — see the doc comment above.
                kind: CheckKind::Http,
                timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap_or_default(),
                latency_ms: latency_ms as u64,
                status_code: status_code.map(|c| c as u16),
                body_matched: None,
                error,
                failure: failure.and_then(|f| serde_json::from_str(&f).ok()),
                body: None,
                headers: None,
                metric: None,
                metrics: None,
            };
            out.push((result, Status::from_db(&status)));
        }
        Ok(out)
    }

    /// All raw result rows after the exclusive `(timestamp, check_id,
    /// agent_id)` cursor, oldest first. The complete key means a page cannot
    /// skip rows when more than one check and agent share a timestamp.
    pub fn results_after(
        &self,
        since: i64,
        since_check_id: Option<CheckId>,
        since_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(CheckResult, Status)>> {
        let mut stmt = self.conn.prepare(
            "SELECT check_id, agent_id, ts_millis, status, latency_ms, status_code, error,
                    failure, zone, region
             FROM check_results
             WHERE ts_millis > ? OR (ts_millis = ? AND (
                 ? IS NULL OR check_id > ? OR (check_id = ? AND agent_id > ?)
             ))
             ORDER BY ts_millis ASC, check_id ASC, agent_id ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![
                    since,
                    since,
                    since_check_id,
                    since_check_id,
                    since_check_id,
                    since_agent_id,
                    limit as i64
                ],
                |row| {
                    Ok((
                        row.get::<_, CheckId>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i32>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    check_id,
                    agent_id,
                    ts,
                    status,
                    latency_ms,
                    status_code,
                    error,
                    failure,
                    zone,
                    region,
                )| {
                    (
                        CheckResult {
                            check_id,
                            agent_id,
                            zone: zone.unwrap_or_default(),
                            region: region.unwrap_or_default(),
                            kind: CheckKind::Http,
                            timestamp: chrono::DateTime::from_timestamp_millis(ts)
                                .unwrap_or_default(),
                            latency_ms: latency_ms as u64,
                            status_code: status_code.map(|c| c as u16),
                            body_matched: None,
                            error,
                            failure: failure.and_then(|f| serde_json::from_str(&f).ok()),
                            body: None,
                            headers: None,
                            metric: None,
                            metrics: None,
                        },
                        Status::from_db(&status),
                    )
                },
            )
            .collect())
    }

    /// Alerts after the exclusive `(timestamp, id)` cursor, oldest first.
    pub fn alerts_since(
        &self,
        since_ts: i64,
        since_id: &str,
        limit: usize,
    ) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts
             WHERE ts_millis > ? OR (ts_millis = ? AND id > ?)
             ORDER BY ts_millis ASC, id ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![since_ts, since_ts, since_id, limit as i64],
                Self::alert_row,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Incidents changed after the exclusive stable mutation cursor.
    pub fn incidents_since(
        &self,
        since_version: i64,
        since_id: &str,
        limit: usize,
    ) -> Result<Vec<ReplicatedIncident>> {
        let (since_node, since_incident_id) = since_id.split_once('\0').unwrap_or(("", since_id));
        let ids: Vec<String> = {
            let mut stmt = self.conn.prepare(
                "SELECT id FROM incidents
                 WHERE version_millis > ?
                    OR (version_millis = ? AND (version_node > ?
                        OR (version_node = ? AND id > ?)))
                 ORDER BY version_millis ASC, version_node ASC, id ASC LIMIT ?",
            )?;
            stmt.query_map(
                duckdb::params![
                    since_version,
                    since_version,
                    since_node,
                    since_node,
                    since_incident_id,
                    limit as i64
                ],
                |r| r.get::<_, String>(0),
            )?
            .collect::<std::result::Result<_, _>>()?
        };
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(i) = self.replicated_incident(&id)? {
                out.push(i);
            }
        }
        Ok(out)
    }

    pub fn incident_tombstones_since(
        &self,
        since_deleted_at: i64,
        since_node_id: &str,
        since_id: &str,
        limit: usize,
    ) -> Result<Vec<IncidentTombstone>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, deleted_at, node_id FROM incident_tombstones
              WHERE deleted_at > ?
                 OR (deleted_at = ? AND node_id > ?)
                 OR (deleted_at = ? AND node_id = ? AND id > ?)
              ORDER BY deleted_at ASC, node_id ASC, id ASC LIMIT ?",
        )?;
        stmt.query_map(
            duckdb::params![
                since_deleted_at,
                since_deleted_at,
                since_node_id,
                since_deleted_at,
                since_node_id,
                since_id,
                limit as i64
            ],
            |row| {
                Ok(IncidentTombstone {
                    id: row.get(0)?,
                    deleted_at: row.get(1)?,
                    node_id: row.get(2)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
    }

    /// Paged rollup rows for `/internal/backfill/latency-minute` and
    /// `-hour`, ordered oldest-bucket-first with `check_id` and `agent_id`
    /// breaking ties — the same composite shape `results_after` uses for raw
    /// rows, which is what lets one cursored sweep walk the whole table
    /// instead of one request per divergent check. `table` is the only
    /// difference between the minute and hour callers.
    fn rollups_since(
        &self,
        table: &str,
        since_bucket: i64,
        since_check_id: Option<CheckId>,
        since_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RollupRow>> {
        let sql = format!(
            "SELECT check_id, agent_id, bucket_start, attempts, up, latency_sum,
                    latency_min, latency_max
             FROM {table}
             WHERE bucket_start > ? OR (bucket_start = ? AND (
                 ? IS NULL OR check_id > ? OR (check_id = ? AND agent_id > ?)
             ))
             ORDER BY bucket_start ASC, check_id ASC, agent_id ASC
             LIMIT ?"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(
            duckdb::params![
                since_bucket,
                since_bucket,
                since_check_id,
                since_check_id,
                since_check_id,
                since_agent_id,
                limit as i64
            ],
            |row| {
                Ok(RollupRow {
                    check_id: row.get(0)?,
                    agent_id: row.get(1)?,
                    bucket_start: row.get(2)?,
                    attempts: row.get(3)?,
                    up: row.get(4)?,
                    latency_sum: row.get(5)?,
                    latency_min: row.get(6)?,
                    latency_max: row.get(7)?,
                })
            },
        )?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn minute_rollups_since(
        &self,
        since_bucket: i64,
        since_check_id: Option<CheckId>,
        since_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RollupRow>> {
        self.rollups_since(
            "check_latency_minute_rollups",
            since_bucket,
            since_check_id,
            since_agent_id,
            limit,
        )
    }

    pub fn hour_rollups_since(
        &self,
        since_bucket: i64,
        since_check_id: Option<CheckId>,
        since_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RollupRow>> {
        self.rollups_since(
            "check_latency_hour_rollups",
            since_bucket,
            since_check_id,
            since_agent_id,
            limit,
        )
    }

    /// Merge a backfilled page into rollup `table` via a staging table, the
    /// same shape `stage_results` uses for raw rows: load once through an
    /// appender, then let one set-based statement do the merge instead of a
    /// prepared statement per row.
    fn apply_rollups(&self, table: &str, rows: &[RollupRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS ingest_rollup_stage AS
                 SELECT * FROM {table} WITH NO DATA;
             DELETE FROM ingest_rollup_stage;"
        ))?;
        {
            let mut appender = self.conn.appender("ingest_rollup_stage")?;
            for row in rows {
                appender.append_row(duckdb::params![
                    row.check_id,
                    row.agent_id,
                    row.bucket_start,
                    row.attempts,
                    row.up,
                    row.latency_sum,
                    row.latency_min,
                    row.latency_max,
                ])?;
            }
            appender.flush()?;
        }
        self.conn.execute_batch(&rollup_merge_from_backfill_sql(
            table,
            table,
            "ingest_rollup_stage",
        ))?;
        Ok(())
    }

    pub fn apply_minute_rollups(&self, rows: &[RollupRow]) -> Result<()> {
        self.apply_rollups("check_latency_minute_rollups", rows)
    }

    pub fn apply_hour_rollups(&self, rows: &[RollupRow]) -> Result<()> {
        self.apply_rollups("check_latency_hour_rollups", rows)
    }

    // ---- Retention-aware peer recovery (raw-data gaps) ----

    /// Record (or refresh) a standing gap: this node's raw history for
    /// `check_id` ends at `from_ts`, `peer`'s begins at `to_ts`, and neither
    /// side holds anything between them. Keyed on `(check_id, peer)` rather
    /// than appended, because `anti_entropy_sweep` redetects a standing gap on
    /// every sweep it persists — see the table's own comment in `open`.
    pub fn record_raw_data_gap(&self, gap: &RawDataGapRow) -> Result<()> {
        self.conn.execute(
            "INSERT INTO raw_data_gaps (check_id, peer, from_ts, to_ts, observed_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT (check_id, peer) DO UPDATE SET
                 from_ts = excluded.from_ts,
                 to_ts = excluded.to_ts,
                 observed_at = excluded.observed_at",
            duckdb::params![
                gap.check_id,
                gap.peer,
                gap.from_ts,
                gap.to_ts,
                gap.observed_at
            ],
        )?;
        Ok(())
    }

    /// Every standing raw-data gap, newest-observed first, for the admin
    /// route — an operator asking which checks have holes and over what
    /// range, not a paged replication cursor. Small and admin-only, so
    /// returning the whole table and letting the route apply `page::take` is
    /// simpler than a second cursor scheme for a table this size.
    pub fn list_raw_data_gaps(&self) -> Result<Vec<RawDataGapRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT check_id, peer, from_ts, to_ts, observed_at
             FROM raw_data_gaps ORDER BY observed_at DESC, check_id ASC, peer ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RawDataGapRow {
                check_id: row.get(0)?,
                peer: row.get(1)?,
                from_ts: row.get(2)?,
                to_ts: row.get(3)?,
                observed_at: row.get(4)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    // ---- Config replication ----

    /// The stamps a batch of entries is arbitrated against, loaded in one query
    /// per kind rather than two per entry. A bulk config push — provisioning
    /// ten thousand checks, or a peer replaying its whole config — is otherwise
    /// thousands of point lookups on the one database worker, and every read
    /// and write on this node queues behind them.
    fn local_config_stamps(&self, entries: &[ConfigEntry]) -> Result<ConfigStamps> {
        let mut stamps = ConfigStamps::default();
        let mut by_kind: std::collections::HashMap<ConfigKind, Vec<&str>> =
            std::collections::HashMap::new();
        for entry in entries {
            by_kind.entry(entry.kind).or_default().push(&entry.id);
        }
        for (kind, ids) in by_kind {
            let (table, pk) = kind.table_and_pk();
            let placeholders = vec!["?"; ids.len()].join(", ");
            let params: Vec<&dyn duckdb::types::ToSql> = ids
                .iter()
                .map(|id| id as &dyn duckdb::types::ToSql)
                .collect();
            let sql = format!(
                "SELECT CAST({pk} AS VARCHAR), updated_at, updated_by
                 FROM {table} WHERE CAST({pk} AS VARCHAR) IN ({placeholders})"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params.as_slice(), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            })?;
            for row in rows {
                let (id, millis, node) = row?;
                // A row whose `updated_at` is NULL was written before stamps
                // existed, or by a path this project hasn't converted yet. It
                // counts as the oldest possible HLC — (0, "") — so anything
                // replicated beats it rather than the merge failing to compare.
                stamps.newest.insert(
                    (kind, id),
                    Hlc::new(millis.unwrap_or(0), node.unwrap_or_default()),
                );
            }

            let sql = format!(
                "SELECT id, deleted_at, node_id FROM config_tombstones
                 WHERE kind = ? AND id IN ({placeholders})"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let kind_str = kind.as_str();
            let mut tomb_params: Vec<&dyn duckdb::types::ToSql> =
                vec![&kind_str as &dyn duckdb::types::ToSql];
            tomb_params.extend(params);
            let rows = stmt.query_map(tomb_params.as_slice(), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    Hlc::new(r.get::<_, i64>(1)?, r.get::<_, String>(2)?),
                ))
            })?;
            for row in rows {
                let (id, hlc) = row?;
                let key = (kind, id);
                stamps.tombstoned.insert(key.clone());
                match stamps.newest.get(&key) {
                    Some(existing) if *existing >= hlc => {}
                    _ => {
                        stamps.newest.insert(key, hlc);
                    }
                }
            }
        }
        Ok(stamps)
    }

    fn prepare_config_delete_statements(&self) -> Result<Vec<Statement<'_>>> {
        [
            ConfigKind::Check,
            ConfigKind::Section,
            ConfigKind::Setting,
            ConfigKind::Credential,
            ConfigKind::RollupPolicy,
        ]
        .into_iter()
        .map(|kind| {
            let (table, pk) = kind.table_and_pk();
            self.conn
                .prepare(&format!("DELETE FROM {table} WHERE {pk} = ?"))
        })
        .collect::<duckdb::Result<_>>()
        .map_err(Into::into)
    }

    /// Write a document over the row named `id`, stamped at `hlc`. The only
    /// place any config table is written from a parsed document, so the
    /// encodings live here and nowhere else.
    fn apply_config_document(
        &self,
        kind: ConfigKind,
        id: &str,
        doc: &str,
        hlc: &Hlc,
    ) -> Result<()> {
        match kind {
            ConfigKind::Check => {
                // `doc` IS `checks.config` verbatim — the whole `CheckConfig`,
                // authoritative already. `upsert_check` is the one place that
                // projects it into the flat columns SQL filters and orders by,
                // so reusing it is what keeps this from becoming a second,
                // hand-maintained field list that drifts from the first.
                let config: CheckConfig = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed check document for {id}: {e}"))?;
                if config.id.to_string() != id {
                    bail!(
                        "check document id {} does not match config key {id}",
                        config.id
                    );
                }
                self.upsert_check(&config, hlc.millis)?;
                self.conn.execute(
                    "UPDATE checks SET updated_by = ? WHERE id = ?",
                    duckdb::params![hlc.node_id, id],
                )?;
            }
            ConfigKind::Section => {
                let s: Section = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed section document for {id}: {e}"))?;
                self.conn
                    .execute("DELETE FROM sections WHERE name = ?", duckdb::params![id])?;
                self.conn.execute(
                    "INSERT INTO sections (name, position, collapsed, display, updated_at, updated_by)
                     VALUES (?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        s.name,
                        s.position,
                        s.collapsed,
                        s.display.as_str(),
                        hlc.millis,
                        hlc.node_id
                    ],
                )?;
            }
            ConfigKind::Setting => {
                let value: String = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed setting document for {id}: {e}"))?;
                self.conn
                    .execute("DELETE FROM settings WHERE key = ?", duckdb::params![id])?;
                self.conn.execute(
                    "INSERT INTO settings (key, value, updated_at, updated_by)
                     VALUES (?, ?, ?, ?)",
                    duckdb::params![id, value, hlc.millis, hlc.node_id],
                )?;
            }
            ConfigKind::Credential => {
                let c: CredentialDoc = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed credential document for {id}: {e}"))?;
                self.conn.execute(
                    "DELETE FROM agent_credentials WHERE id = ?",
                    duckdb::params![id],
                )?;
                self.conn.execute(
                    "INSERT INTO agent_credentials
                         (id, zone, region, secret_hash, salt, created_at, revoked_at,
                          updated_at, updated_by)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        c.id,
                        c.zone,
                        c.region,
                        c.secret_hash,
                        c.salt,
                        c.created_at,
                        c.revoked_at,
                        hlc.millis,
                        hlc.node_id,
                    ],
                )?;
            }
            ConfigKind::RollupPolicy => {
                let doc: RollupPolicyDoc = serde_json::from_str(doc).map_err(|e| {
                    anyhow::anyhow!("malformed rollup policy document for {id}: {e}")
                })?;
                doc.policy
                    .validate()
                    .map_err(|e| anyhow::anyhow!("invalid rollup policy for {id}: {e}"))?;
                self.upsert_rollup_policy(id, doc.node_type, &doc.policy, hlc)?;
            }
        }
        Ok(())
    }

    /// The ONLY place "is this newer?" is decided for replicated config.
    /// Local writes are meant to go through this same function — the caller
    /// stamps a fresh `Hlc` and calls it exactly like a replicated entry
    /// would — so a local edit and one that arrived over the wire share one
    /// code path and can never disagree about what wins.
    ///
    /// Returns whether `e` was applied. `false` means the local stamp was
    /// already greater than or equal to `e.hlc`, so nothing changed —
    /// comparison uses `Hlc`'s derived `Ord`, never re-implemented here.
    pub fn apply_config_entry(&self, e: &ConfigEntry) -> Result<bool> {
        self.apply_config_entries(std::slice::from_ref(e))
            .map(|applied| applied.into_iter().next().unwrap_or(false))
    }

    /// Apply a bounded config batch in one transaction. LWW arbitration remains
    /// per entry, but a crash can never expose only a prefix of accepted writes.
    /// Callers use the returned flags to update only caches whose rows changed.
    pub fn apply_config_entries(&self, entries: &[ConfigEntry]) -> Result<Vec<bool>> {
        self.conn.execute_batch("BEGIN")?;
        let write = || -> Result<Vec<bool>> {
            let mut stamps = self.local_config_stamps(entries)?;
            let mut deletes = self.prepare_config_delete_statements()?;
            let mut tombstone_deletes = self
                .conn
                .prepare("DELETE FROM config_tombstones WHERE kind = ? AND id = ?")?;
            let mut tombstone_inserts = self.conn.prepare(
                "INSERT INTO config_tombstones (kind, id, deleted_at, node_id)
                 VALUES (?, ?, ?, ?)",
            )?;
            entries
                .iter()
                .map(|entry| {
                    self.apply_config_entry_uncommitted(
                        entry,
                        &mut stamps,
                        &mut deletes[entry.kind as usize],
                        &mut tombstone_deletes,
                        &mut tombstone_inserts,
                    )
                })
                .collect()
        };
        match write() {
            Ok(applied) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(applied)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn apply_config_entry_uncommitted(
        &self,
        e: &ConfigEntry,
        stamps: &mut ConfigStamps,
        delete: &mut Statement<'_>,
        tombstone_delete: &mut Statement<'_>,
        tombstone_insert: &mut Statement<'_>,
    ) -> Result<bool> {
        let key = (e.kind, e.id.clone());
        if let Some(local) = stamps.newest.get(&key) {
            if e.hlc <= *local {
                return Ok(false);
            }
        }
        let tombstoned = stamps.tombstoned.contains(&key);
        // The snapshot has to track what this batch itself writes. Two entries
        // for one id in a single batch must still resolve by HLC, exactly as
        // they would arriving separately — otherwise the last one in the vector
        // wins and an older edit can overwrite a newer one.
        stamps.newest.insert(key.clone(), e.hlc.clone());
        if e.is_tombstone() {
            stamps.tombstoned.insert(key.clone());
        } else {
            stamps.tombstoned.remove(&key);
        }
        if e.is_tombstone() {
            if e.kind != ConfigKind::Check {
                delete.execute(duckdb::params![e.id])?;
            } else if let Ok(check_id) = e.id.parse::<CheckId>() {
                delete.execute(duckdb::params![e.id])?;
                // Bypasses `delete_check`, which does this too, because a
                // replicated tombstone applies straight through the prepared
                // per-kind statement above rather than that helper.
                self.conn.execute(
                    "DELETE FROM check_placement_regions WHERE check_id = ?",
                    duckdb::params![check_id],
                )?;
            }
            if tombstoned {
                tombstone_delete.execute(duckdb::params![e.kind.as_str(), e.id])?;
            }
            tombstone_insert.execute(duckdb::params![
                e.kind.as_str(),
                e.id,
                e.hlc.millis,
                e.hlc.node_id
            ])?;
        } else {
            // A malformed document is an error, not a silent skip: the caller
            // logs it, but this must never half-apply a document over a good
            // local row.
            let doc = e.document.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "config entry for {} has no document and is not a tombstone",
                    e.id
                )
            })?;
            self.apply_config_document(e.kind, &e.id, doc, &e.hlc)?;
            // The edit supersedes any tombstone for this id — leaving one
            // behind would make the next digest sweep look inconsistent.
            if tombstoned {
                tombstone_delete.execute(duckdb::params![e.kind.as_str(), e.id])?;
            }
        }
        Ok(true)
    }

    /// Read back the entry a row would replicate as, for pushing to peers
    /// after a local write. `None` when the row does not exist.
    pub fn config_entry(&self, kind: ConfigKind, id: &str) -> Result<Option<ConfigEntry>> {
        let doc: Option<(String, Option<i64>, Option<String>)> = match kind {
            ConfigKind::Check => {
                // A caller (or a peer on an older binary) can name an id that
                // is not a UUID at all. That is not an error here: nothing in
                // this table can match it, which is exactly "no such row".
                let Ok(id) = id.parse::<CheckId>() else {
                    return Ok(None);
                };
                let mut stmt = self
                    .conn
                    .prepare("SELECT config, updated_at, updated_by FROM checks WHERE id = ?")?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                })?;
                rows.next()
                    .transpose()?
                    .and_then(|(doc, at, by)| doc.map(|doc| (doc, at, by)))
            }
            ConfigKind::Section => {
                let mut stmt = self.conn.prepare(
                    "SELECT name, position, collapsed, display, updated_at, updated_by
                     FROM sections WHERE name = ?",
                )?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    let section = Section {
                        name: r.get::<_, String>(0)?,
                        position: r.get::<_, i32>(1)?,
                        collapsed: r.get::<_, Option<bool>>(2)?.unwrap_or(false),
                        display: r
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map_or(SectionDisplay::Inline, SectionDisplay::from_db),
                    };
                    Ok((
                        section,
                        r.get::<_, Option<i64>>(4)?,
                        r.get::<_, Option<String>>(5)?,
                    ))
                })?;
                match rows.next().transpose()? {
                    Some((section, at, by)) => Some((serde_json::to_string(&section)?, at, by)),
                    None => None,
                }
            }
            ConfigKind::Setting => {
                let mut stmt = self
                    .conn
                    .prepare("SELECT value, updated_at, updated_by FROM settings WHERE key = ?")?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                })?;
                match rows.next().transpose()? {
                    Some((value, at, by)) => Some((serde_json::to_string(&value)?, at, by)),
                    None => None,
                }
            }
            ConfigKind::Credential => {
                let mut stmt = self.conn.prepare(
                    "SELECT id, zone, region, secret_hash, salt, created_at, revoked_at,
                            updated_at, updated_by
                     FROM agent_credentials WHERE id = ?",
                )?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    let c = CredentialDoc {
                        id: r.get(0)?,
                        zone: r.get(1)?,
                        region: r.get(2)?,
                        secret_hash: r.get(3)?,
                        salt: r.get(4)?,
                        created_at: r.get(5)?,
                        revoked_at: r.get(6)?,
                    };
                    Ok((
                        c,
                        r.get::<_, Option<i64>>(7)?,
                        r.get::<_, Option<String>>(8)?,
                    ))
                })?;
                match rows.next().transpose()? {
                    Some((c, at, by)) => Some((serde_json::to_string(&c)?, at, by)),
                    None => None,
                }
            }
            ConfigKind::RollupPolicy => {
                let mut stmt = self.conn.prepare(
                    "SELECT node_id, node_type, degraded_when, down_when, config,
                            updated_at, updated_by
                     FROM rollup_policy WHERE node_id = ?",
                )?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    let policy = Self::rollup_policy_row(r)?;
                    Ok((
                        RollupPolicyDoc {
                            node_type: policy.node_type,
                            policy: policy.policy,
                        },
                        Some(policy.updated_at),
                        policy.updated_by,
                    ))
                })?;
                match rows.next().transpose()? {
                    Some((doc, at, by)) => Some((serde_json::to_string(&doc)?, at, by)),
                    None => None,
                }
            }
        };
        Ok(doc.map(|(document, at, by)| ConfigEntry {
            kind,
            id: id.to_owned(),
            // Same NULL-as-oldest-HLC reading as the merge rule, so a row a
            // peer has never heard a stamp for still replicates as *something*
            // comparable rather than being unrepresentable.
            hlc: Hlc::new(at.unwrap_or(0), by.unwrap_or_default()),
            document: Some(document),
        }))
    }

    /// Read one ordered, bounded page of entries for a config kind. Both the
    /// candidate-id scan and the document reads are bounded, so a peer pull
    /// never materializes the full config table before applying its page size.
    pub fn config_entries_page(
        &self,
        kind: ConfigKind,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConfigEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let (table, pk) = kind.table_and_pk();
        // Select only a page of ids before projecting documents. The union
        // keeps live rows and tombstones in the same cursor order.
        let sql = format!(
            "SELECT id, deleted_at, node_id FROM (\
                 SELECT {pk}::VARCHAR AS id, NULL::BIGINT AS deleted_at, NULL::VARCHAR AS node_id \
                 FROM {table} \
                 UNION ALL \
                 SELECT id, deleted_at, node_id FROM config_tombstones WHERE kind = ?\
             ) entries \
             WHERE ? IS NULL OR id > ? \
             ORDER BY id \
             LIMIT ?"
        );
        let rows = self
            .conn
            .prepare(&sql)?
            .query_map(
                duckdb::params![kind.as_str(), after, after, limit as i64],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut entries = Vec::with_capacity(rows.len());
        for (id, deleted_at, node_id) in rows {
            if let Some(deleted_at) = deleted_at {
                entries.push(ConfigEntry {
                    kind,
                    id,
                    hlc: Hlc::new(deleted_at, node_id.unwrap_or_default()),
                    document: None,
                });
            } else if let Some(entry) = self.config_entry(kind, &id)? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Every entry of a kind, tombstones included, for an anti-entropy sweep.
    pub fn config_entries(&self, kind: ConfigKind) -> Result<Vec<ConfigEntry>> {
        let (table, pk) = kind.table_and_pk();
        let ids: Vec<String> = {
            // Cast explicitly: `pk` is a native `UUID` column for checks and
            // `VARCHAR` for everything else, and `FromSql for String` does not
            // accept a UUID column directly.
            let sql = format!("SELECT {pk}::VARCHAR FROM {table}");
            let mut stmt = self.conn.prepare(&sql)?;
            stmt.query_map([], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(e) = self.config_entry(kind, &id)? {
                out.push(e);
            }
        }
        let mut stmt = self
            .conn
            .prepare("SELECT id, deleted_at, node_id FROM config_tombstones WHERE kind = ?")?;
        let tombstones = stmt
            .query_map(duckdb::params![kind.as_str()], |r| {
                Ok(ConfigEntry {
                    kind,
                    id: r.get::<_, String>(0)?,
                    hlc: Hlc::new(r.get::<_, i64>(1)?, r.get::<_, String>(2)?),
                    document: None,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        out.extend(tombstones);
        Ok(out)
    }

    /// Stamp an existing row without changing it — used by paths that mutate
    /// a row through an older helper that doesn't know about replication.
    pub fn stamp_config(&self, kind: ConfigKind, id: &str, hlc: &Hlc) -> Result<()> {
        let (table, pk) = kind.table_and_pk();
        if kind == ConfigKind::Check {
            let Ok(id) = id.parse::<CheckId>() else {
                return Ok(());
            };
            self.conn.execute(
                &format!("UPDATE {table} SET updated_at = ?, updated_by = ? WHERE {pk} = ?"),
                duckdb::params![hlc.millis, hlc.node_id, id],
            )?;
        } else {
            self.conn.execute(
                &format!("UPDATE {table} SET updated_at = ?, updated_by = ? WHERE {pk} = ?"),
                duckdb::params![hlc.millis, hlc.node_id, id],
            )?;
        }
        Ok(())
    }

    /// Delete tombstones older than `before_millis`. Pruned separately from
    /// `prune()`'s other windows and on a longer one — the caller passes the
    /// larger of `results_days`/`alerts_days` because a node that was down for
    /// the *shorter* window would come back, still hold a check its tombstone
    /// says is deleted, and hand it straight back on the next anti-entropy
    /// sweep. That resurrection is exactly what a tombstone exists to
    /// prevent, so it must outlive every window a node could plausibly be
    /// offline for.
    pub fn prune_tombstones(&self, before_millis: i64) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM config_tombstones WHERE deleted_at < ?",
            duckdb::params![before_millis],
        )?;
        Ok(n)
    }

    // ---- Fleet health ----

    /// Record when an agent was last polled and how long it took to answer.
    /// One row per agent: only the latest poll matters, so this replaces
    /// rather than accumulates.
    ///
    /// A **merge**, not a blind replace: a row is only overwritten by a
    /// strictly newer `at`. That is what makes applying a peer's rows
    /// idempotent and order-independent — batches cross the wire out of order,
    /// and a stale one arriving late must not un-see a live agent. Read then
    /// write is safe without a transaction because DuckDB is single-writer and
    /// the caller already holds the storage mutex.
    pub fn record_agent_poll(&self, row: &AgentPollRow) -> Result<()> {
        let newer_stored: i64 = self.conn.query_row(
            "SELECT count(*) FROM agent_polls WHERE agent_id = ? AND polled_at >= ?",
            duckdb::params![row.agent_id, row.at],
            |r| r.get(0),
        )?;
        if newer_stored > 0 {
            return Ok(());
        }
        self.conn.execute(
            "INSERT OR REPLACE INTO agent_polls
                 (agent_id, zone, region, polled_at, window_ms, rtt_ms, build, declared_kinds)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                row.agent_id,
                row.zone,
                row.region,
                row.at,
                row.window_ms,
                row.rtt_ms,
                row.build,
                row.kinds,
            ],
        )?;
        Ok(())
    }

    /// Forget an agent's poll row. Called from `purge_agent`: the fleet view
    /// reads this table as well as the results, so a purge that left the row
    /// behind would put the agent straight back on the status page.
    pub fn delete_agent_poll(&self, agent_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM agent_polls WHERE agent_id = ?",
            duckdb::params![agent_id],
        )?;
        Ok(())
    }

    /// Every agent's last-known poll, for the fleet health view and for
    /// replicating it to peers.
    pub fn list_agent_polls(&self) -> Result<Vec<AgentPollRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT agent_id, zone, region, polled_at, window_ms, rtt_ms, build, declared_kinds
             FROM agent_polls",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AgentPollRow {
                    agent_id: row.get(0)?,
                    zone: row.get(1)?,
                    region: row.get(2)?,
                    at: row.get(3)?,
                    window_ms: row.get(4)?,
                    rtt_ms: row.get(5)?,
                    build: row.get(6)?,
                    kinds: row.get(7)?,
                })
            })?
            .collect::<std::result::Result<_, _>>()?;
        Ok(rows)
    }

    // ---- Incident deletion ----

    /// Erase an incident: its narrative, its affects rows, and the incident
    /// itself, leaving a tombstone so a peer's push cannot resurrect it.
    ///
    /// Linked alerts are **unlinked, not deleted**. An alert is the raw record
    /// that a check flipped; an incident is the narrative written around it.
    /// Deleting the narrative must not erase the fact, or the alert feed and
    /// the uptime figure would start disagreeing about the same outage.
    ///
    /// Returns false when there was no such incident, so a handler can 404
    /// rather than report success for an id that never existed. A repeat
    /// delete of an already-tombstoned id is still false, and still harmless.
    pub fn delete_incident(&self, id: &str, now_millis: i64, node_id: &str) -> Result<bool> {
        let deleted_at = self
            .replicated_incident(id)?
            .map(|incident| now_millis.max(incident.version.millis + 1))
            .unwrap_or(now_millis);
        self.apply_incident_tombstone(&IncidentTombstone {
            id: id.to_string(),
            deleted_at,
            node_id: node_id.to_string(),
        })
    }

    pub fn apply_incident_tombstone(&self, tombstone: &IncidentTombstone) -> Result<bool> {
        self.transaction(|store| store.apply_incident_tombstone_uncommitted(tombstone))
    }

    pub(crate) fn apply_incident_tombstone_uncommitted(
        &self,
        tombstone: &IncidentTombstone,
    ) -> Result<bool> {
        let id = &tombstone.id;
        let incoming_version = Hlc::new(tombstone.deleted_at, tombstone.node_id.clone());
        if let Some(incident) = self.replicated_incident(id)? {
            if incident.version > incoming_version {
                return Ok(false);
            }
        }
        let tombstone_existed = if let Some(existing) = self.incident_tombstone(id)? {
            if Hlc::new(existing.deleted_at, existing.node_id) >= incoming_version {
                return Ok(false);
            }
            true
        } else {
            false
        };
        let write = || -> Result<bool> {
            self.conn.execute(
                "DELETE FROM incident_updates WHERE incident_id = ?",
                duckdb::params![id],
            )?;
            self.conn.execute(
                "DELETE FROM incident_checks WHERE incident_id = ?",
                duckdb::params![id],
            )?;
            self.conn.execute(
                "UPDATE alerts SET incident_id = NULL WHERE incident_id = ?",
                duckdb::params![id],
            )?;
            let removed = self
                .conn
                .execute("DELETE FROM incidents WHERE id = ?", duckdb::params![id])?;
            // The tombstone is written even when the row was already gone: a
            // peer that deletes an incident this node never received still has
            // to be able to stop it arriving afterwards.
            self.conn.execute(
                "INSERT OR REPLACE INTO incident_tombstones (id, deleted_at, node_id)
                 VALUES (?, ?, ?)",
                duckdb::params![id, tombstone.deleted_at, tombstone.node_id],
            )?;
            let cursor_id = format!("{}\0{id}", tombstone.node_id);
            self.record_stream_item(
                "incident_tombstones",
                tombstone.deleted_at,
                &cursor_id,
                i64::from(!tombstone_existed),
            )?;
            // Replacement and deletion can invalidate a stream's previous max,
            // not only its count. Rare deletes favor exact metadata over an
            // incremental update that cannot know the next-highest row.
            self.rebuild_stream_watermarks()?;
            Ok(removed > 0)
        };
        write()
    }

    /// Whether an incident id has been deleted anywhere in the cluster.
    pub fn incident_tombstone(&self, id: &str) -> Result<Option<IncidentTombstone>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, deleted_at, node_id FROM incident_tombstones WHERE id = ?")?;
        let mut rows = stmt.query_map(duckdb::params![id], |row| {
            Ok(IncidentTombstone {
                id: row.get(0)?,
                deleted_at: row.get(1)?,
                node_id: row.get(2)?,
            })
        })?;
        rows.next().transpose().map_err(Into::into)
    }

    pub fn incident_tombstoned(&self, id: &str) -> Result<bool> {
        let found: i64 = self.conn.query_row(
            "SELECT count(*) FROM incident_tombstones WHERE id = ?",
            duckdb::params![id],
            |r| r.get(0),
        )?;
        Ok(found > 0)
    }

    /// Every incident id this node knows to be deleted, for replicating the
    /// deletion to a peer that has not heard about it yet.
    pub fn list_incident_tombstones(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT id FROM incident_tombstones")?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<_, _>>()?;
        Ok(rows)
    }

    /// Delete incident tombstones older than `before_millis`. Same window and
    /// same reasoning as `prune_tombstones`: a tombstone has to outlive the
    /// longest outage a peer could plausibly have been down for, or that peer
    /// comes back and pushes the incident again.
    pub fn prune_incident_tombstones(&self, before_millis: i64) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM incident_tombstones WHERE deleted_at < ?",
            duckdb::params![before_millis],
        )?;
        if n != 0 {
            self.rebuild_stream_watermarks()?;
        }
        Ok(n)
    }

    /// Erase everything a check ever produced: its results, its alerts, every
    /// incident it opened, and its rows inside incidents another check owns.
    ///
    /// Deliberately separate from `purge_check_data`, which keeps the incident
    /// record — "reset the graph" and "this never happened" are different
    /// intentions, and only one of them removes a page visitors have read.
    /// Incidents go through `delete_incident` one at a time rather than a bulk
    /// DELETE, so each leaves a tombstone and the erase survives replication.
    pub fn purge_check_everything(
        &self,
        check_id: CheckId,
        now_millis: i64,
        node_id: &str,
    ) -> Result<PurgeSummary> {
        let results = self.purge_check_data(check_id)?;
        let owned: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT id FROM incidents WHERE origin_check = ?")?;
            stmt.query_map(duckdb::params![check_id], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let mut incidents = 0u64;
        for id in &owned {
            if self.delete_incident(id, now_millis, node_id)? {
                incidents += 1;
            }
        }
        // Incidents this check merely joined belong to whoever opened them, so
        // they survive; only this check's membership of them goes. Route each
        // removal through the normal mutator so its replication version moves.
        let memberships: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT incident_id FROM incident_checks WHERE check_id = ?")?;
            stmt.query_map(duckdb::params![check_id], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        for incident_id in memberships {
            self.detach_check(&incident_id, check_id, None)?;
        }
        Ok(PurgeSummary {
            results,
            incidents,
            deleted_incidents: owned,
        })
    }
}

/// What an erase actually removed, so the UI can say it rather than claiming
/// something generic. `deleted_incidents` is also what the caller replicates.
#[derive(Debug, Clone, PartialEq)]
pub struct PurgeSummary {
    pub results: u64,
    pub incidents: u64,
    pub deleted_incidents: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    /// Deterministic per-name check id so test bodies stay readable while
    /// storage only ever sees `CheckId`. Packing the name's bytes (rather than
    /// hashing) also keeps `Ord` on the id agreeing with `Ord` on the name for
    /// these short ASCII fixtures, which several tests rely on for ordering.
    pub(super) fn cid(name: &str) -> CheckId {
        let mut bytes = [0u8; 16];
        let src = name.as_bytes();
        let n = src.len().min(16);
        bytes[..n].copy_from_slice(&src[..n]);
        CheckId::from_u128(u128::from_be_bytes(bytes))
    }

    pub(super) fn result(check_id: &str, ts_millis: i64, latency_ms: u64) -> CheckResult {
        CheckResult {
            zone: "test-zone".into(),
            region: "test-region".into(),
            body: None,
            headers: None,
            check_id: cid(check_id),
            agent_id: "agent".into(),
            kind: crate::models::CheckKind::Http,
            timestamp: DateTime::from_timestamp_millis(ts_millis).unwrap(),
            latency_ms,
            status_code: Some(200),
            body_matched: None,
            error: None,
            failure: None,
            metric: None,
            metrics: None,
        }
    }

    fn agent_state(check_id: &str, agent_id: &str, status: Status, ts: i64) -> AgentCheckStateRow {
        AgentCheckStateRow {
            check_id: cid(check_id),
            agent_id: agent_id.into(),
            zone: "test-zone".into(),
            region: "test-region".into(),
            status,
            consecutive_failures: 0,
            last_check: ts,
        }
    }

    fn published(check_id: &str, state: NodeState, ts: i64) -> PublishedStateRow {
        PublishedStateRow {
            check_id: cid(check_id),
            state,
            pending: None,
            last_check: ts,
        }
    }

    /// One batched write must leave every projection exactly where the
    /// row-at-a-time path left it: newest observation wins per check, equal
    /// timestamps break ties on the greater agent, and per-agent beliefs
    /// collapse to the newest row for that agent.
    #[test]
    fn persist_ingest_projects_the_newest_observation_per_check_and_agent() {
        let s = Storage::open(":memory:").unwrap();
        let mut older = result("a", 1_000, 5);
        older.agent_id = "agent-b".into();
        let mut tie = result("a", 2_000, 7);
        tie.agent_id = "agent-a".into();
        let mut newest = result("a", 2_000, 9);
        newest.agent_id = "agent-c".into();

        s.persist_ingest(
            &[
                (older, Status::Up),
                (tie, Status::Down),
                (newest, Status::Up),
            ],
            &[
                agent_state("a", "agent-b", Status::Up, 1_000),
                agent_state("a", "agent-a", Status::Down, 2_000),
                agent_state("a", "agent-a", Status::Up, 2_500),
            ],
            &[published("a", NodeState::Up, 2_000)],
        )
        .unwrap();

        assert_eq!(s.count_results().unwrap(), 3);
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.checks[0].count, 3);
        assert_eq!(digest.checks[0].max_ts, 2_000);
        let latest = s.list_status(3_000).unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].latency_ms, 9, "greater agent wins an exact tie");

        let mut beliefs = s.list_agent_check_state().unwrap();
        beliefs.sort_by(|l, r| l.agent_id.cmp(&r.agent_id));
        assert_eq!(beliefs.len(), 2);
        assert_eq!(beliefs[0].last_check, 2_500);
        assert_eq!(beliefs[0].status, Status::Up);
        assert_eq!(beliefs[1].last_check, 1_000);
        assert_eq!(
            s.list_current_state().unwrap(),
            vec![CurrentStateRow {
                check_id: cid("a"),
                state: NodeState::Up,
                pending: None,
            }]
        );
    }

    /// A late batch must not rewind what is already published, and repeating a
    /// state must not fragment history into a span per probe.
    #[test]
    fn persist_ingest_ignores_stale_rows_and_records_only_transitions() {
        let s = Storage::open(":memory:").unwrap();
        s.persist_ingest(
            &[(result("a", 5_000, 11), Status::Up)],
            &[agent_state("a", "agent", Status::Up, 5_000)],
            &[published("a", NodeState::Up, 5_000)],
        )
        .unwrap();
        s.persist_ingest(
            &[(result("a", 1_000, 99), Status::Down)],
            &[agent_state("a", "agent", Status::Down, 1_000)],
            &[published("a", NodeState::Up, 5_100)],
        )
        .unwrap();

        assert_eq!(s.count_results().unwrap(), 2, "raw history keeps both rows");
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.checks[0].max_ts, 5_000);
        assert_eq!(digest.checks[0].count, 2);
        let latest = s.list_status(6_000).unwrap();
        assert_eq!(latest[0].latency_ms, 11, "a stale row cannot rewind latest");
        let beliefs = s.list_agent_check_state().unwrap();
        assert_eq!(beliefs[0].status, Status::Up, "stale belief is ignored");
        assert_eq!(
            s.state_spans(cid("a")).unwrap(),
            vec![StateSpan {
                state: NodeState::Up,
                since: 5_000,
            }],
            "an unchanged state opens no second span"
        );

        s.persist_ingest(
            &[(result("a", 6_000, 12), Status::Down)],
            &[agent_state("a", "agent", Status::Down, 6_000)],
            &[published("a", NodeState::Down, 6_000)],
        )
        .unwrap();
        assert_eq!(
            s.state_spans(cid("a")).unwrap().len(),
            2,
            "a changed state opens exactly one new span"
        );
    }

    /// Every projection commits together or not at all: a failing batch must
    /// not leave raw rows behind, and the connection must stay usable.
    #[test]
    fn persist_ingest_rolls_back_every_projection_on_failure() {
        let s = Storage::open(":memory:").unwrap();
        s.persist_ingest(
            &[(result("a", 1_000, 5), Status::Up)],
            &[agent_state("a", "agent", Status::Up, 1_000)],
            &[published("a", NodeState::Up, 1_000)],
        )
        .unwrap();
        // Current state is written after the raw rows, watermarks, latest
        // result, and agent belief of the same batch, so rejecting it proves
        // those earlier writes are rolled back rather than half-kept.
        s.conn
            .execute_batch("CREATE UNIQUE INDEX one_check_per_ts ON current_state (last_check)")
            .unwrap();

        let failed = s.persist_ingest(
            &[(result("b", 1_000, 6), Status::Down)],
            &[agent_state("b", "agent", Status::Down, 1_000)],
            &[published("b", NodeState::Down, 1_000)],
        );
        assert!(failed.is_err(), "a rejected projection fails the batch");
        assert_eq!(s.count_results().unwrap(), 1, "raw rows rolled back");
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.checks.len(), 1, "watermarks rolled back");
        assert_eq!(digest.checks[0].check_id, cid("a"));
        assert_eq!(s.list_status(3_000).unwrap().len(), 1);
        assert_eq!(s.list_agent_check_state().unwrap().len(), 1);
        assert_eq!(s.list_current_state().unwrap().len(), 1);
        assert!(s.state_spans(cid("b")).unwrap().is_empty());

        s.conn.execute_batch("DROP INDEX one_check_per_ts").unwrap();
        s.persist_ingest(
            &[(result("b", 1_000, 6), Status::Down)],
            &[agent_state("b", "agent", Status::Down, 1_000)],
            &[published("b", NodeState::Down, 1_000)],
        )
        .expect("the connection survives a failed batch");
        assert_eq!(s.count_results().unwrap(), 2);
    }

    #[test]
    fn ingest_transition_failure_rolls_back_all_rows_and_recovers() {
        let s = Storage::open(":memory:").unwrap();
        s.prepare_ingest().unwrap();
        let attempt = || {
            s.transaction(|store| {
                store.persist_ingest_uncommitted(
                    &[(result("a", 1_000, 5), Status::Down)],
                    &[agent_state("a", "agent", Status::Down, 1_000)],
                    &[published("a", NodeState::Down, 1_000)],
                )?;
                store.create_incident(
                    "incident-a",
                    "Down",
                    IncidentState::Detected,
                    1_000,
                    false,
                    Some(cid("a")),
                    &[IncidentCheckRow {
                        check_id: cid("a"),
                        source: AffectSource::Origin,
                    }],
                    None,
                )?;
                store.insert_alert(
                    AlertScope::Check,
                    &cid("a").to_string(),
                    Some(NodeState::Up),
                    NodeState::Down,
                    1_000,
                    Some("incident-a"),
                )?;
                Ok(())
            })
        };
        s.conn
            .execute_batch("CREATE UNIQUE INDEX reject_alert ON alerts (ts_millis)")
            .unwrap();
        s.conn.execute_batch("INSERT INTO alerts (id, scope, node_id, current_state, ts_millis) VALUES ('block', 'check', 'block', 'up', 1000)").unwrap();
        assert!(attempt().is_err());
        assert_eq!(s.count_results().unwrap(), 0);
        assert!(s.list_current_state().unwrap().is_empty());
        assert!(s.list_agent_check_state().unwrap().is_empty());
        assert!(s.state_spans(cid("a")).unwrap().is_empty());
        assert!(s.incident("incident-a").unwrap().is_none());
        s.conn
            .execute_batch("DELETE FROM alerts WHERE id = 'block'")
            .unwrap();
        attempt().unwrap();
        assert_eq!(s.count_results().unwrap(), 1);
        assert!(s.incident("incident-a").unwrap().is_some());
        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
    }

    #[test]
    fn uncommitted_ingest_is_not_visible_after_reopen() {
        let path =
            std::env::temp_dir().join(format!("warden-ingest-{}.duckdb", uuid::Uuid::now_v7()));
        {
            let s = Storage::open(path.to_str().unwrap()).unwrap();
            s.prepare_ingest().unwrap();
            s.conn.execute_batch("BEGIN").unwrap();
            s.persist_ingest_uncommitted(
                &[(result("a", 1_000, 5), Status::Down)],
                &[agent_state("a", "agent", Status::Down, 1_000)],
                &[published("a", NodeState::Down, 1_000)],
            )
            .unwrap();
            s.create_incident(
                "incident-a",
                "Down",
                IncidentState::Detected,
                1_000,
                false,
                Some(cid("a")),
                &[],
                None,
            )
            .unwrap();
        }
        let s = Storage::open(path.to_str().unwrap()).unwrap();
        assert_eq!(s.count_results().unwrap(), 0);
        assert!(s.list_current_state().unwrap().is_empty());
        assert!(s.incident("incident-a").unwrap().is_none());
        drop(s);
        std::fs::remove_file(path).unwrap();
    }

    /// A database missing a column the current schema requires must fail
    /// `Storage::open` clearly, not silently read the check back with a
    /// default it never had. Regression test for `verify_schema`.
    #[test]
    fn opening_a_database_missing_a_current_column_fails_clearly() {
        let path = std::env::temp_dir().join(format!(
            "warden-missing-column-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path = path.to_string_lossy().into_owned();
        {
            let conn = Connection::open(&path).unwrap();
            // Shaped like the current `incidents` table but missing
            // `version_millis`/`version_node`, added after the first release.
            conn.execute_batch(
                "CREATE TABLE incidents (
                    id VARCHAR PRIMARY KEY,
                    title VARCHAR NOT NULL,
                    state VARCHAR NOT NULL,
                    started_at BIGINT NOT NULL,
                    resolved_at BIGINT,
                    global BOOLEAN NOT NULL DEFAULT FALSE,
                    origin_check UUID
                );",
            )
            .unwrap();
        }

        let message = match Storage::open(&path) {
            Ok(_) => panic!("a database missing a required column must not open"),
            Err(err) => err.to_string(),
        };
        assert!(
            message.contains("incidents.version_millis")
                || message.contains("incidents.version_node"),
            "error should name the missing column, got: {message}"
        );
        assert!(
            message.contains("Pre-1.0 database policy"),
            "error should point at the rebuild procedure, got: {message}"
        );
        std::fs::remove_file(path).unwrap();
    }

    /// A fresh file database reopens without error and keeps its rows —
    /// the ordinary, non-degraded path `verify_schema` must never reject.
    #[test]
    fn a_fresh_file_database_reopens_and_keeps_its_data() {
        let path = std::env::temp_dir().join(format!(
            "warden-fresh-reopen-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path = path.to_string_lossy().into_owned();
        let check = CheckConfig {
            id: CheckId::now_v7(),
            name: "fresh".into(),
            target: "https://example.test".into(),
            ..Default::default()
        };
        {
            let s = Storage::open(&path).unwrap();
            s.upsert_check(&check, 1).unwrap();
        }

        let reopened = Storage::open(&path).expect("a fresh database must reopen cleanly");
        assert_eq!(reopened.list_checks().unwrap()[0].id, check.id);
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn fresh_schema_marker_never_rewrites_new_random_check_ids() {
        let path =
            std::env::temp_dir().join(format!("warden-storage-{}.duckdb", uuid::Uuid::now_v7()));
        let path = path.to_string_lossy().into_owned();
        let id = CheckId::now_v7();
        let check = CheckConfig {
            id,
            name: "new-check".into(),
            target: "https://example.test".into(),
            ..Default::default()
        };
        Storage::open(&path)
            .unwrap()
            .upsert_check(&check, 1)
            .unwrap();
        let reopened = Storage::open(&path).unwrap();
        assert_eq!(reopened.list_checks().unwrap()[0].id, id);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn failure_reason_survives_the_round_trip_to_readers() {
        // An operator needs to know *why* a check is down, not just that it is.
        // The reason is written by the agent and must reach both readers.
        let s = Storage::open(":memory:").unwrap();
        let now = 5_000i64;
        let mut refused = result("a", now, 0);
        refused.status_code = None;
        refused.error = Some("connection refused".into());

        let mut wrong_code = result("a", now - 1000, 12);
        wrong_code.status_code = Some(404);

        s.insert_results(&[(wrong_code, Status::Down), (refused, Status::Down)])
            .unwrap();
        // history() surfaces the per-attempt reason.
        let hist = s.history(cid("a"), 10, None, None, None).unwrap();
        assert_eq!(hist[0].error.as_deref(), Some("connection refused"));
        assert_eq!(
            hist[0].status_code, None,
            "a refused connection has no HTTP code"
        );
        assert_eq!(
            hist[1].error, None,
            "a 404 is a response, not a transport error"
        );
        assert_eq!(
            hist[1].status_code,
            Some(404),
            "the unexpected code is preserved"
        );

        // list_status() surfaces the reason for the *latest* attempt only.
        let rows = s.list_status(now).unwrap();
        let a = rows.iter().find(|r| r.check_id == cid("a")).unwrap();
        assert_eq!(
            a.error.as_deref(),
            Some("connection refused"),
            "dashboard shows the newest failure reason"
        );
    }

    #[test]
    fn recent_results_filters_and_orders_across_checks_and_agents() {
        // The explorer is cross-check and cross-agent, so build a spread: two
        // checks, two agents, mixed status and location.
        let s = Storage::open(":memory:").unwrap();
        let now = 100_000i64;

        // A helper local to this test: vary the columns the explorer filters on.
        let make = |check: &str,
                    agent: &str,
                    ago: i64,
                    status: Status,
                    region: &str,
                    zone: &str,
                    error: Option<&str>| {
            let mut r = result(check, now - ago, 7);
            r.agent_id = agent.into();
            r.region = region.into();
            r.zone = zone.into();
            r.error = error.map(Into::into);
            (r, status)
        };

        s.insert_results(&[
            make(
                "web",
                "agent-a",
                4000,
                Status::Up,
                "us-east",
                "us-east-1a",
                None,
            ),
            make(
                "web",
                "agent-b",
                3000,
                Status::Down,
                "eu-west",
                "eu-west-1a",
                Some("connection refused"),
            ),
            make(
                "ddns-wa",
                "agent-a",
                2000,
                Status::Down,
                "us-east",
                "us-east-1a",
                Some(r#"resolved to 97.7.61.14, expected one of ["7.…"]"#),
            ),
            make(
                "ddns-wa",
                "agent-b",
                1000,
                Status::Up,
                "eu-west",
                "eu-west-1a",
                None,
            ),
        ])
        .unwrap();

        // No filter: every row, newest first.
        let all = s.recent_results(&ResultsFilter::default(), 100).unwrap();
        assert_eq!(all.len(), 4);
        assert!(
            all.windows(2).all(|w| w[0].timestamp >= w[1].timestamp),
            "newest first"
        );
        assert_eq!(all[0].check_id, cid("ddns-wa"), "1s ago is the newest");

        // Per-column filters narrow correctly.
        let by_check = s
            .recent_results(
                &ResultsFilter {
                    check_id: Some(cid("web")),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(by_check.len(), 2);
        assert!(by_check.iter().all(|r| r.check_id == cid("web")));

        let by_agent = s
            .recent_results(
                &ResultsFilter {
                    agent_id: Some("agent-a".into()),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(by_agent.len(), 2);
        assert!(by_agent.iter().all(|r| r.agent_id == "agent-a"));

        let down = s
            .recent_results(
                &ResultsFilter {
                    status: Some(Status::Down),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(down.len(), 2);
        assert!(down.iter().all(|r| r.status == Status::Down));

        let by_region = s
            .recent_results(
                &ResultsFilter {
                    region: Some("eu-west".into()),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(by_region.len(), 2);
        assert!(by_region
            .iter()
            .all(|r| r.region.as_deref() == Some("eu-west")));

        let by_zone = s
            .recent_results(
                &ResultsFilter {
                    zone: Some("us-east-1a".into()),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(by_zone.len(), 2);
        assert!(by_zone
            .iter()
            .all(|r| r.zone.as_deref() == Some("us-east-1a")));

        // Combined filters AND together.
        let combined = s
            .recent_results(
                &ResultsFilter {
                    check_id: Some(cid("web")),
                    status: Some(Status::Down),
                    region: Some("eu-west".into()),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(combined.len(), 1);
        assert_eq!(combined[0].agent_id, "agent-b");

        // The LIMIT caps the row count, still newest first.
        // The two newest rows are the two ddns-wa attempts (1s and 2s ago),
        // ahead of the web rows at 3s/4s.
        let capped = s.recent_results(&ResultsFilter::default(), 2).unwrap();
        assert_eq!(capped.len(), 2);
        assert!(capped.iter().all(|r| r.check_id == cid("ddns-wa")));
        assert!(capped[0].timestamp >= capped[1].timestamp);
    }

    #[test]
    fn recent_results_round_trips_the_raw_ddns_error() {
        // The whole point of the explorer: the verbatim error the agent wrote,
        // including the resolved IP and expected values, reaches the reader.
        let s = Storage::open(":memory:").unwrap();
        let now = 5_000i64;
        let leaked = r#"resolved to 97.7.61.14, expected one of ["7.…"]"#;

        let mut r = result("ddns-wa", now, 0);
        r.status_code = None;
        r.error = Some(leaked.into());
        r.failure = Some(crate::models::FailureKind::UnexpectedDnsAnswer);

        s.insert_results(&[(r, Status::Down)]).unwrap();

        let rows = s.recent_results(&ResultsFilter::default(), 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].error.as_deref(), Some(leaked));
        assert_eq!(
            rows[0].failure,
            Some(crate::models::FailureKind::UnexpectedDnsAnswer),
            "the category round-trips beside the raw text"
        );
        assert_eq!(rows[0].status, Status::Down);
    }

    #[test]
    fn recovered_check_does_not_report_a_stale_failure_reason() {
        // Regression: per-column arg_max() skips NULLs, so a check that failed and
        // then recovered kept surfacing the old error next to an "up" status.
        let s = Storage::open(":memory:").unwrap();
        let now = 10_000i64;

        let mut failed = result("a", now - 1000, 5);
        failed.status_code = None;
        failed.error = Some("connection refused".into());

        // The newest row succeeded: status_code 200, error None.
        let recovered = result("a", now, 11);

        s.insert_results(&[(failed, Status::Down), (recovered, Status::Up)])
            .unwrap();

        let rows = s.list_status(now).unwrap();
        let a = rows.iter().find(|r| r.check_id == cid("a")).unwrap();
        assert_eq!(
            a.state,
            NodeState::Up,
            "without a published state, the latest observation is lifted"
        );
        assert_eq!(
            a.error, None,
            "a recovered check must not carry the old error"
        );
        assert_eq!(
            a.status_code,
            Some(200),
            "code comes from the same latest row"
        );
        assert_eq!(a.latency_ms, 11, "latency comes from the same latest row");
    }
    #[test]
    fn history_round_trips_newest_first_with_limit_and_check_filter() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_results(&[
            (result("a", 1000, 10), Status::Up),
            (result("a", 2000, 20), Status::Down),
            (result("a", 3000, 30), Status::Up),
            (result("b", 1500, 99), Status::Down),
        ])
        .unwrap();

        // Full history for "a": newest first, and "b"'s row (latency 99) excluded.
        let hist = s.history(cid("a"), 10, None, None, None).unwrap();
        assert_eq!(
            hist,
            vec![
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 3000,
                    status: Status::Up,
                    latency_ms: 30,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 2000,
                    status: Status::Down,
                    latency_ms: 20,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 1000,
                    status: Status::Up,
                    latency_ms: 10,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
            ]
        );

        // Limit caps to the N newest rows.
        let capped = s.history(cid("a"), 2, None, None, None).unwrap();
        assert_eq!(
            capped,
            vec![
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 3000,
                    status: Status::Up,
                    latency_ms: 30,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 2000,
                    status: Status::Down,
                    latency_ms: 20,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
            ]
        );
    }

    /// Three transitions, one of them stamped with an incident. Asserts the
    /// exact row shape rather than a count, so a column silently dropped from
    /// the SELECT fails here instead of surfacing as a blank cell in the UI.
    #[test]
    fn recent_alerts_round_trips_newest_first_with_exact_fields() {
        let s = Storage::open(":memory:").unwrap();
        let a1 = s
            .insert_alert(
                AlertScope::Check,
                "a",
                Some(NodeState::Up),
                NodeState::Down,
                1000,
                Some("inc-1"),
            )
            .unwrap();
        let b1 = s
            .insert_alert(
                AlertScope::Check,
                "b",
                Some(NodeState::Down),
                NodeState::Up,
                2000,
                None,
            )
            .unwrap();
        // A first-ever observation has nothing to move from.
        let a2 = s
            .insert_alert(AlertScope::Check, "a", None, NodeState::Unknown, 3000, None)
            .unwrap();

        let got = s.recent_alerts(10).unwrap();
        assert_eq!(
            got,
            vec![
                AlertRow {
                    id: a2,
                    scope: AlertScope::Check,
                    node_id: "a".into(),
                    previous_state: None,
                    current_state: NodeState::Unknown,
                    timestamp: 3000,
                    incident_id: None,
                },
                AlertRow {
                    id: b1,
                    scope: AlertScope::Check,
                    node_id: "b".into(),
                    previous_state: Some(NodeState::Down),
                    current_state: NodeState::Up,
                    timestamp: 2000,
                    incident_id: None,
                },
                AlertRow {
                    id: a1,
                    scope: AlertScope::Check,
                    node_id: "a".into(),
                    previous_state: Some(NodeState::Up),
                    current_state: NodeState::Down,
                    timestamp: 1000,
                    incident_id: Some("inc-1".into()),
                },
            ]
        );
    }

    #[test]
    fn local_alerts_with_identical_payloads_in_one_millisecond_are_not_lost() {
        let s = Storage::open(":memory:").unwrap();
        let first = s
            .insert_alert(
                AlertScope::Check,
                "web",
                Some(NodeState::Up),
                NodeState::Down,
                1_000,
                None,
            )
            .unwrap();
        let second = s
            .insert_alert(
                AlertScope::Check,
                "web",
                Some(NodeState::Up),
                NodeState::Down,
                1_000,
                None,
            )
            .unwrap();

        assert_ne!(
            first, second,
            "distinct local events need distinct replication ids"
        );
        let alerts = s.recent_alerts(10).unwrap();
        assert_eq!(alerts.len(), 2, "neither same-ms alert may be lost");
        assert_eq!(
            alerts
                .iter()
                .map(|a| a.id.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn recent_alerts_caps_to_the_newest_limit() {
        let s = Storage::open(":memory:").unwrap();
        for (node, ts) in [("a", 1000), ("b", 2000), ("a", 3000)] {
            s.insert_alert(
                AlertScope::Check,
                node,
                Some(NodeState::Up),
                NodeState::Down,
                ts,
                None,
            )
            .unwrap();
        }

        let got = s.recent_alerts(2).unwrap();
        assert_eq!(
            got.iter().map(|a| a.timestamp).collect::<Vec<_>>(),
            vec![3000, 2000],
            "the newest two, newest first"
        );
    }

    /// The evidence join: only the alerts stamped with this incident come back,
    /// oldest first. A time-window join would have swept up the unrelated one.
    #[test]
    fn alerts_for_incident_returns_only_its_own_stamped_transitions() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            1000,
            Some("inc-1"),
        )
        .unwrap();
        // Same instant, different incident: proves membership is the stamp and
        // not the clock.
        s.insert_alert(
            AlertScope::Check,
            "b",
            Some(NodeState::Up),
            NodeState::Down,
            1000,
            Some("inc-2"),
        )
        .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Down),
            NodeState::Up,
            2000,
            Some("inc-1"),
        )
        .unwrap();

        let got = s.alerts_for_incident("inc-1").unwrap();
        assert_eq!(
            got.iter()
                .map(|a| (a.node_id.as_str(), a.timestamp))
                .collect::<Vec<_>>(),
            vec![("a", 1000), ("a", 2000)],
            "only inc-1's transitions, oldest first"
        );
    }

    #[test]
    fn list_status_reports_latest_per_check_and_windowed_uptime() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        s.insert_results(&[
            // Check "a": 3 UP + 1 DOWN in-window; latest (greatest ts) is the DOWN.
            (result("a", now - 3000, 10), Status::Up),
            (result("a", now - 2000, 11), Status::Up),
            (result("a", now - 1000, 12), Status::Up),
            (result("a", now, 50), Status::Down),
            // Check "b": all UP in-window ...
            (result("b", now - 5000, 20), Status::Up),
            (result("b", now - 4000, 21), Status::Up),
            // ... plus one stale DOWN exactly on the exclusive lower bound: must be excluded.
            (result("b", now - day, 99), Status::Down),
        ])
        .unwrap();
        // Uptime is derived from published-state spans, rather than raw probe
        // counts. The three seconds Up followed by one second Down yield 75%.
        s.record_state(cid("a"), NodeState::Up, now - 4000).unwrap();
        s.record_state(cid("a"), NodeState::Down, now - 1000)
            .unwrap();

        let rows = s.list_status(now).unwrap();
        assert_eq!(rows.len(), 2, "one row per distinct check_id");
        let find = |id: &str| rows.iter().find(|r| r.check_id == cid(id)).unwrap().clone();

        let a = find("a");
        assert_eq!(
            a.state,
            NodeState::Down,
            "without a published state, the latest observation is lifted"
        );
        assert_eq!(a.latency_ms, 50, "latest row's latency");
        assert_eq!(a.last_check, now, "latest row's timestamp");
        assert!(
            (a.uptime_24h - 0.75).abs() < 1e-9,
            "3 UP / 4 in-window, got {}",
            a.uptime_24h
        );

        let b = find("b");
        assert_eq!(b.state, NodeState::Up);
        assert_eq!(b.latency_ms, 21);
        assert_eq!(b.last_check, now - 4000);
        // Stale DOWN at the exclusive lower bound is filtered out, so 2 UP / 2 in-window.
        assert!(
            (b.uptime_24h - 1.0).abs() < 1e-9,
            "stale row must be excluded, got {}",
            b.uptime_24h
        );
    }

    #[test]
    fn list_status_projection_handles_out_of_order_equal_timestamp_and_nulls() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let mut failure = result("a", now, 50);
        failure.agent_id = "agent-a".into();
        failure.status_code = None;
        failure.error = Some("failed".into());
        failure.failure = Some(crate::models::FailureKind::ConnectionFailed);
        let mut recovery = result("a", now, 12);
        recovery.agent_id = "agent-z".into();
        recovery.status_code = Some(204);
        let stale = result("a", now - 1, 99);

        s.insert_results(&[(failure, Status::Down), (recovery, Status::Up)])
            .unwrap();
        s.insert_results(&[(stale, Status::Down)]).unwrap();

        let row = s.list_status(now).unwrap().remove(0);
        assert_eq!(row.state, NodeState::Up);
        assert_eq!(row.latency_ms, 12);
        assert_eq!(row.status_code, Some(204));
        assert_eq!(row.error, None);
        assert_eq!(row.failure, None);
    }

    #[test]
    fn opening_repairs_an_incomplete_latest_results_projection() {
        let path = std::env::temp_dir().join(format!(
            "warden-latest-results-repair-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let now = 1_000_000_000_000i64;
        {
            let s = Storage::open(&path_string).unwrap();
            s.insert_results(&[
                (result("a", now, 10), Status::Up),
                (result("b", now, 20), Status::Down),
            ])
            .unwrap();
            s.conn
                .execute(
                    "DELETE FROM latest_check_results WHERE check_id = ?",
                    duckdb::params![cid("b")],
                )
                .unwrap();
            assert_eq!(s.list_status(now).unwrap().len(), 1);
        }

        let reopened = Storage::open(&path_string).unwrap();
        assert_eq!(reopened.list_status(now).unwrap().len(), 2);
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn list_status_projection_is_rebuilt_after_agent_purge() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let mut older = result("a", now - 1, 10);
        older.agent_id = "kept".into();
        let mut latest = result("a", now, 20);
        latest.agent_id = "purged".into();
        s.insert_results(&[(older, Status::Up), (latest, Status::Down)])
            .unwrap();

        s.purge_agent("purged").unwrap();

        let row = s.list_status(now).unwrap().remove(0);
        assert_eq!(row.state, NodeState::Up);
        assert_eq!(row.latency_ms, 10);
        assert_eq!(row.last_check, now - 1);
    }

    #[test]
    fn seed_mock_seeds_when_empty_then_is_idempotent() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        assert!(s.seed_mock(now).unwrap(), "seeds an empty store");
        let seeded = s.list_status(now).unwrap();
        assert!(
            !seeded.is_empty(),
            "seeded store yields at least one status"
        );

        assert!(!s.seed_mock(now).unwrap(), "second seed is a no-op");
        let after = s.list_status(now).unwrap();
        assert_eq!(after.len(), seeded.len(), "idempotent seed adds no checks");
    }

    #[test]
    fn uptime_buckets_are_time_weighted_from_state_spans() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        // Raw results provide tooltip counts, but only published state spans
        // determine the time-weighted uptime ratio.
        s.insert_results(&[
            (result("x", now - 3 * day + 1, 10), Status::Down),
            (result("x", now - 2 * day + 1, 10), Status::Up),
            (result("x", now - day + 1, 10), Status::Up),
        ])
        .unwrap();
        s.record_state(cid("x"), NodeState::Up, now - 3 * day)
            .unwrap();
        s.record_state(cid("x"), NodeState::Down, now - day - day / 2)
            .unwrap();
        s.record_state(cid("x"), NodeState::Degraded, now - day / 2)
            .unwrap();

        let buckets = s.uptime_buckets(cid("x"), 3, 3, now, false).unwrap();
        assert_eq!(buckets.len(), 3, "exactly `days` buckets, oldest-first");
        assert_eq!(buckets[0].uptime, Some(1.0));
        assert_eq!(buckets[0].up, 0);
        assert_eq!(buckets[0].total, 1);
        assert_eq!(buckets[1].uptime, Some(0.5));
        assert_eq!(buckets[1].up, 1);
        assert_eq!(buckets[1].total, 1);
        assert_eq!(buckets[2].uptime, Some(0.5));
        assert_eq!(buckets[2].up, 1);
        assert_eq!(buckets[2].total, 1);
        assert!(
            buckets.iter().all(|bucket| bucket.total <= 1),
            "tooltip counts are probe attempts, not milliseconds"
        );

        let degraded_down = s.uptime_buckets(cid("x"), 3, 3, now, true).unwrap();
        assert_eq!(degraded_down[2].uptime, Some(0.0));
        assert_eq!(
            s.uptime_between(cid("x"), now - 3 * day, now, true)
                .unwrap(),
            Some(0.5)
        );
    }

    /// The batched strip replaces N per-check queries on the status page, so it
    /// must return exactly what those queries returned — same segment edges,
    /// same counts, same time-weighted fraction, and the same per-check
    /// `degraded_is_downtime` decision.
    #[test]
    fn batched_uptime_strips_agree_with_the_per_check_query() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        for (name, offset) in [("x", 0), ("y", day / 4)] {
            s.insert_results(&[
                (result(name, now - 2 * day + offset, 10), Status::Up),
                (result(name, now - day + offset, 20), Status::Down),
                (result(name, now - day / 2 + offset, 30), Status::Up),
            ])
            .unwrap();
        }
        s.record_state(cid("x"), NodeState::Up, now - 3 * day)
            .unwrap();
        s.record_state(cid("x"), NodeState::Down, now - day)
            .unwrap();
        s.record_state(cid("x"), NodeState::Up, now - day / 2)
            .unwrap();
        s.record_state(cid("y"), NodeState::Degraded, now - 3 * day)
            .unwrap();

        // `y` treats Degraded as downtime and `x` does not, so a shared flag
        // would show up as a mismatch on one of them.
        let checks = [(cid("x"), false), (cid("y"), true)];
        let batched = s.uptime_strips(&checks, 3, 12, now).unwrap();
        assert_eq!(batched.len(), 2);

        for (check_id, degraded_is_downtime) in checks {
            let expected = s
                .uptime_buckets(check_id, 3, 12, now, degraded_is_downtime)
                .unwrap();
            let got = &batched[&check_id];
            assert_eq!(got.buckets, expected, "{check_id} strip must match");
            assert_eq!(
                got.overall,
                s.uptime_between(check_id, now - 3 * day, now, degraded_is_downtime)
                    .unwrap(),
                "{check_id} overall must match the single-check figure"
            );
        }
    }

    /// A check with no rows at all must still get a full strip of empty
    /// segments, because the page renders one bar per segment regardless.
    #[test]
    fn batched_uptime_strips_cover_checks_with_no_data() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        let strips = s
            .uptime_strips(&[(cid("absent"), false)], 7, 90, now)
            .unwrap();
        let strip = &strips[&cid("absent")];
        assert_eq!(strip.buckets.len(), 90);
        assert!(strip.buckets.iter().all(|b| b.uptime.is_none()));
        assert!(strip.buckets.iter().all(|b| b.total == 0));
        assert_eq!(strip.overall, None, "nothing known is not zero uptime");
    }

    /// Same contract for the sparkline: one scan for many checks must produce
    /// the points the per-check series produced.
    #[test]
    fn batched_latency_points_agree_with_the_per_check_series() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        for (name, base) in [("x", 10u64), ("y", 100)] {
            let mut down = result(name, now - 3 * hour, base + 5);
            down.agent_id = "agent-b".into();
            s.insert_results(&[
                (result(name, now - 4 * hour, base), Status::Up),
                (down, Status::Down),
                (result(name, now - hour, base + 15), Status::Up),
            ])
            .unwrap();
        }

        let from = now - 6 * hour;
        let ids = [cid("x"), cid("y"), cid("absent")];
        let batched = s.latency_points_for(&ids, from, now, 6).unwrap();
        assert_eq!(batched.len(), 3);

        for check_id in ids {
            let expected = s.latency_series(check_id, from, now, 6).unwrap().points;
            assert_eq!(batched[&check_id], expected, "{check_id} points must match");
        }
        assert!(
            batched[&cid("absent")].iter().all(|p| p.attempts == 0),
            "a check with no rows still gets a full, empty series"
        );
    }

    #[test]
    fn uptime_buckets_no_rows_returns_empty_segments() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        let buckets = s.uptime_buckets(cid("absent"), 4, 4, now, false).unwrap();
        assert_eq!(
            buckets.len(),
            4,
            "always exactly `days` buckets even with no data"
        );
        for (i, b) in buckets.iter().enumerate() {
            assert_eq!(
                b.start,
                now - (4 - i as i64) * day,
                "bucket {i} window start"
            );
            assert_eq!(b.up, 0);
            assert_eq!(b.total, 0);
            assert_eq!(b.uptime, None, "bucket {i} has no data");
        }
    }

    #[test]
    fn latency_sparkline_returns_hourly_averages() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        // Four distinct instants, so a 4-bucket split is fully supported by the
        // data and isn't capped.
        s.insert_results(&[
            (result("x", now, 50), Status::Up),
            (result("x", now - 600_000, 100), Status::Up),
            (result("x", now - hour - 600_000, 75), Status::Up),
            (result("x", now - 2 * hour - 600_000, 60), Status::Up),
            // Decoy check "y" should not count.
            (result("y", now, 999), Status::Up),
        ])
        .unwrap();

        let points = s
            .latency_series(cid("x"), now - 4 * hour, now, 4)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "four instants support four buckets");

        // Segments are one hour each: the requested window divided by the
        // buckets it supports, not narrowed to the data inside it.
        assert_eq!(points[0].timestamp, now - 4 * hour, "window as requested");
        assert_eq!(points[3].timestamp, now - hour);

        // Bucket 0 spans the hour before any result exists, and must not read
        // as a 0ms response.
        assert_eq!(points[0].avg_latency_ms, None, "empty segment -> None");
        assert_eq!(points[1].avg_latency_ms, Some(60.0));
        assert_eq!(points[2].avg_latency_ms, Some(75.0));
        assert_eq!(
            points[3].avg_latency_ms,
            Some(75.0),
            "newest segment averages 50 and 100"
        );

        // Decoy "y" must not be counted.
        let y_points = s
            .latency_series(cid("y"), now - 4 * hour, now, 4)
            .unwrap()
            .points;
        assert_eq!(
            y_points.last().unwrap().avg_latency_ms,
            Some(999.0),
            "decoy 'y' has its own data"
        );
    }

    #[test]
    fn every_requested_range_charts_that_range() {
        // The 24h/7d/30d/90d selector must actually change the span, including
        // for a check younger than the range. An earlier fix narrowed the window
        // to the data inside it, which read well for a young check but made all
        // four ranges render identically once the check was younger than them —
        // the control silently stopped doing anything.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        // Five hours old, exactly the shape that triggered the report.
        let rows: Vec<_> = (0..60)
            .map(|i| {
                (
                    result("young", now - i * 300_000, 20 + i as u64),
                    Status::Up,
                )
            })
            .collect();
        s.insert_results(&rows).unwrap();

        let span_of = |window: i64| {
            let p = s
                .latency_series(cid("young"), now - window, now, 120)
                .unwrap()
                .points;
            (p.last().unwrap().timestamp - p[0].timestamp, p.len())
        };
        let (day, _) = span_of(24 * hour);
        let (week, _) = span_of(7 * 24 * hour);
        let (month, _) = span_of(30 * 24 * hour);

        assert!(
            day < week && week < month,
            "each range must chart its own span: 24h={day} 7d={week} 30d={month}"
        );
        // And each is the requested window, not the data's extent.
        let requested = (30 * 24 * hour) as f64;
        let drift = (month as f64 - requested).abs() / requested;
        assert!(drift < 0.05, "30d view spans ~30 days, got {month}ms");
    }

    #[test]
    fn a_lone_populated_segment_is_still_a_reading() {
        // The other half of the report: over a 30-day window a five-hour-old
        // check fills a single segment. That is the truth and it must be
        // returned as data — the chart drew nothing and claimed to be
        // "Collecting latency data..." while sitting on 120 results.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let month = 30 * 24 * 3_600_000i64;

        let rows: Vec<_> = (0..60)
            .map(|i| (result("young", now - i * 30_000, 40), Status::Up))
            .collect();
        s.insert_results(&rows).unwrap();

        let series = s
            .latency_series(cid("young"), now - month, now, 120)
            .unwrap();
        let populated: Vec<_> = series
            .points
            .iter()
            .filter(|p| p.avg_latency_ms.is_some())
            .collect();
        assert!(
            !populated.is_empty(),
            "a check with 60 results must report a reading over any window"
        );
        assert_eq!(
            series.overall.attempts, 60,
            "and the summary counts every attempt regardless of bucketing"
        );
    }

    #[test]
    fn a_check_that_stopped_reporting_still_shows_its_silence() {
        // Silence is data. This check reported for an hour then went quiet 20
        // hours ago; charting only the hour it spoke would hide the outage
        // entirely, so the window must keep running to the requested end.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        let rows: Vec<_> = (0..12)
            .map(|i| {
                (
                    result("quiet", now - 21 * hour + i * 300_000, 25),
                    Status::Up,
                )
            })
            .collect();
        s.insert_results(&rows).unwrap();

        let points = s
            .latency_series(cid("quiet"), now - 24 * hour, now, 12)
            .unwrap();
        let trailing_empty = points
            .points
            .iter()
            .rev()
            .take_while(|p| p.avg_latency_ms.is_none())
            .count();
        assert!(
            trailing_empty > 0,
            "silence since the last result must stay on the chart"
        );
        let last = points.points.last().unwrap().timestamp;
        assert!(
            last > now - 4 * hour,
            "the chart still reaches the present: ends {}h early",
            (now - last) / hour
        );
    }

    #[test]
    fn latency_series_never_invents_zero_readings() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        // A check with nothing in the window must not produce a flat line of
        // zeroes, which would read as "responded instantly" rather than "never
        // ran".
        let points = s
            .latency_series(cid("absent"), now - 6 * 3_600_000, now, 6)
            .unwrap()
            .points;
        assert_eq!(
            points.len(),
            6,
            "an empty window still preserves its requested grid"
        );
        assert!(
            points.iter().all(|p| p.avg_latency_ms.is_none()),
            "no data must yield no measurements, got {points:?}"
        );
    }

    #[test]
    fn latency_series_preserves_the_requested_grid_for_sparse_timestamps() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let min = 60_000i64;

        // A short window probed every few minutes still gets its requested
        // grid; unobserved segments are explicitly empty, not 0ms readings.
        s.insert_results(&[
            (result("x", now - 12 * min, 40), Status::Up),
            (result("x", now - 8 * min, 50), Status::Up),
            (result("x", now - 4 * min, 60), Status::Up),
            (result("x", now, 70), Status::Up),
        ])
        .unwrap();

        let points = s
            .latency_series(cid("x"), now - 16 * min, now, 120)
            .unwrap()
            .points;
        assert_eq!(
            points.len(),
            120,
            "the requested grid is never capped to data"
        );
        assert_eq!(points[29].avg_latency_ms, Some(40.0));
        assert_eq!(points[59].avg_latency_ms, Some(50.0));
        assert_eq!(points[89].avg_latency_ms, Some(60.0));
        assert_eq!(points[119].avg_latency_ms, Some(70.0));
        assert_eq!(
            points.iter().filter(|p| p.avg_latency_ms.is_none()).count(),
            116,
            "unobserved segments remain empty rather than reading as zero latency"
        );
    }

    #[test]
    fn latency_series_splits_into_sub_hour_buckets() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let min = 60_000i64;

        // Four instants over the last hour, so the requested 4 buckets (15 min
        // each) survive the resolution cap. Two land at the edges, leaving the
        // middle two genuinely empty.
        s.insert_results(&[
            (result("x", now - min, 40), Status::Up), // bucket 3 (last 15 min)
            (result("x", now - 2 * min, 20), Status::Up), // bucket 3 as well
            (result("x", now - 50 * min, 80), Status::Up), // bucket 0
            (result("x", now - 55 * min, 100), Status::Up), // bucket 0 as well
        ])
        .unwrap();

        let points = s
            .latency_series(cid("x"), now - 60 * min, now, 4)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "exactly `buckets` points");
        assert_eq!(points[3].avg_latency_ms, Some(30.0), "avg of 40 and 20");
        assert_eq!(points[3].attempts, 2);
        assert_eq!(points[3].up, 2);
        assert_eq!(points[0].avg_latency_ms, Some(90.0), "avg of 80 and 100");
        assert_eq!(points[0].attempts, 2);
        assert_eq!(points[0].up, 2);
        assert_eq!(points[1].attempts, 0, "middle segment is unobserved");
        assert_eq!(points[1].up, 0);
        assert_eq!(points[1].avg_latency_ms, None, "middle segment empty");
        assert_eq!(points[2].avg_latency_ms, None, "middle segment empty");
    }

    #[test]
    fn latency_series_attributes_a_historical_window_per_agent() {
        // What the day-detail modal asks for: a window that ended long before
        // `now`, with two agents disagreeing inside it. Sampling the newest rows
        // could not answer this.
        let s = Storage::open(":memory:").unwrap();
        let hour = 3_600_000i64;
        let now = 1_000_000_000_000i64;
        let from = now - 10 * hour;
        let to = from + 4 * hour;

        let at = |agent: &str, ts: i64, latency: u64, status: Status| {
            let mut r = result("web", ts, latency);
            r.agent_id = agent.to_string();
            (r, status)
        };
        s.insert_results(&[
            at("agent-a", from + hour, 10, Status::Up),
            at("agent-a", from + 2 * hour, 30, Status::Up),
            // agent-b times out inside the window: 5000ms is how long it waited,
            // not a measurement, so it must not become the max.
            at("agent-b", from + hour, 5000, Status::Down),
            at("agent-b", from + 2 * hour, 20, Status::Up),
            // Outside the window on both sides, and a decoy check.
            at("agent-a", from - hour, 999, Status::Up),
            at("agent-a", to + hour, 888, Status::Up),
            (result("other", from + hour, 777), Status::Up),
        ])
        .unwrap();

        // Two distinct instants in the window, so ask for the two segments the
        // data can actually support.
        let series = s.latency_series(cid("web"), from, to, 2).unwrap();

        assert_eq!(series.overall.attempts, 4, "only rows inside the window");
        assert_eq!(series.overall.up, 3);
        assert_eq!(series.overall.min_ms, Some(10));
        assert_eq!(
            series.overall.max_ms,
            Some(30),
            "the 5000ms timeout must not set the max"
        );
        assert_eq!(series.overall.avg_ms(), Some(20.0), "(10+30+20)/3");

        let by_agent: Vec<_> = series
            .agents
            .iter()
            .map(|a| (a.agent_id.as_str(), a.band.attempts, a.band.up))
            .collect();
        assert_eq!(
            by_agent,
            vec![("agent-a", 2, 2), ("agent-b", 2, 1)],
            "the failing agent is identifiable"
        );

        // Point counts retain failed evidence separately from latency. Both
        // recorded instants land in the first half of this four-hour window;
        // the timeout is observed but excluded from the latency average.
        assert_eq!(series.points.len(), 2);
        assert_eq!(series.points[0].attempts, 4);
        assert_eq!(series.points[0].up, 3);
        assert_eq!(series.points[0].avg_latency_ms, Some(20.0));
        assert_eq!(series.points[1].attempts, 0);
        assert_eq!(series.points[1].up, 0);
        // The series covers the window the caller asked for.
        assert_eq!(
            series.points[0].timestamp, from,
            "oldest segment starts at the requested bound"
        );
        // Silence after the last result means the check stopped reporting,
        // which must stay visible rather than being trimmed away.
        assert_eq!(
            series.points[1].avg_latency_ms, None,
            "the empty half must not read as 0ms"
        );
        assert!(
            series.points[1].timestamp < to,
            "the requested end is still charted"
        );
    }

    #[test]
    fn list_agents_reports_each_reporting_agent_once() {
        // Agents aren't registered anywhere — they exist because results carry
        // their agent_id, which is what makes the dashboard's agent list truthful.
        let s = Storage::open(":memory:").unwrap();
        let now = 100_000i64;
        let day = 86_400_000i64;

        let with_agent = |check: &str, agent: &str, ts: i64| {
            let mut r = result(check, ts, 10);
            r.agent_id = agent.into();
            (r, Status::Up)
        };

        s.insert_results(&[
            // agent-a reports two distinct checks in-window.
            with_agent("web", "agent-a", now),
            with_agent("api", "agent-a", now - 500),
            // agent-b reports one check twice: 1 distinct check, 2 results.
            with_agent("web", "agent-b", now - 1000),
            with_agent("web", "agent-b", now - 2000),
            // agent-old last reported over a day ago: still listed, but its
            // result falls outside the 24h window.
            with_agent("web", "agent-old", now - day - 1000),
        ])
        .unwrap();

        let agents = s.list_agents(now).unwrap();
        let find = |id: &str| agents.iter().find(|a| a.agent_id == id).unwrap().clone();

        assert_eq!(agents.len(), 3, "one row per distinct agent_id");
        assert_eq!(agents[0].agent_id, "agent-a", "ordered by last_seen DESC");

        let a = find("agent-a");
        assert_eq!(a.last_seen, now);
        assert_eq!(a.checks_reporting, 2, "distinct checks, not result count");
        assert_eq!(a.results_24h, 2);

        let b = find("agent-b");
        assert_eq!(b.checks_reporting, 1, "same check twice is still one check");
        assert_eq!(b.results_24h, 2, "but two results");

        let old = find("agent-old");
        assert_eq!(
            old.results_24h, 0,
            "stale results fall outside the 24h window"
        );
        assert_eq!(old.last_seen, now - day - 1000, "last_seen is not windowed");
    }
    #[test]
    fn history_distinguishes_agents_reporting_the_same_millisecond() {
        // Regression: several agents probe one check, so two rows can share a
        // timestamp. The UI keys rows by (timestamp, agent_id); without agent_id
        // the keys collide and the whole detail view fails to render.
        let s = Storage::open(":memory:").unwrap();
        let ts = 4_000i64;

        let mut a = result("web", ts, 10);
        a.agent_id = "agent-a".into();
        let mut b = result("web", ts, 20);
        b.agent_id = "agent-b".into();

        s.insert_results(&[(a, Status::Up), (b, Status::Up)])
            .unwrap();

        let hist = s.history(cid("web"), 10, None, None, None).unwrap();
        assert_eq!(hist.len(), 2, "both agents' results are kept");
        assert_eq!(
            hist[0].timestamp, hist[1].timestamp,
            "same millisecond, so timestamp alone cannot be a unique key"
        );

        let mut agents: Vec<_> = hist.iter().map(|h| h.agent_id.as_str()).collect();
        agents.sort();
        assert_eq!(
            agents,
            vec!["agent-a", "agent-b"],
            "each row names its agent"
        );

        // Ordering is deterministic (ts DESC, agent_id ASC) so the list is stable.
        assert_eq!(hist[0].agent_id, "agent-a");
        assert_eq!(hist[0].latency_ms, 10, "latency stays with its own agent");
        assert_eq!(hist[1].latency_ms, 20);
    }

    #[test]
    fn status_filter_finds_failures_a_newest_n_slice_would_miss() {
        // Regression: a busy day holds thousands of results. The day-detail view
        // used to read the newest 100 rows and so showed all-green for a day the
        // uptime bar had marked degraded, because the failures were older than
        // the slice. Filtering by status finds them regardless of position.
        let s = Storage::open(":memory:").unwrap();
        let day_start = 1_000_000i64;
        let mut rows = Vec::new();
        // Two early failures...
        for (i, agent) in ["agent-a", "agent-b"].iter().enumerate() {
            let mut r = result("web", day_start + i as i64, 0);
            r.agent_id = (*agent).into();
            r.status_code = None;
            r.error = Some("timeout".into());
            rows.push((r, Status::Down));
        }
        // ...buried under 300 later successes.
        for i in 0..300 {
            rows.push((result("web", day_start + 10_000 + i, 12), Status::Up));
        }
        s.insert_results(&rows).unwrap();

        // The newest 100 rows contain no failures at all — the original bug.
        let newest = s.history(cid("web"), 100, None, None, None).unwrap();
        assert_eq!(newest.len(), 100);
        assert!(
            newest.iter().all(|r| r.status == Status::Up),
            "the newest slice hides the failures, which is why the filter exists"
        );

        // Filtering by status surfaces exactly the two failures, with reasons.
        let bad = s
            .history(cid("web"), 500, None, None, Some(Status::Down))
            .unwrap();
        assert_eq!(bad.len(), 2, "both failures found regardless of recency");
        assert!(bad.iter().all(|r| r.error.as_deref() == Some("timeout")));

        // And the filter is a filter, not a truncation: successes still queryable.
        let good = s
            .history(cid("web"), 500, None, None, Some(Status::Up))
            .unwrap();
        assert_eq!(good.len(), 300);
    }

    /// Retention keeps open incidents at any age — an active outage is not old
    /// history — and takes a pruned incident's narrative and affects rows with
    /// it, since DuckDB has no cascade to do that for us.
    #[test]
    fn prune_removes_rows_older_than_the_retention_window() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        // One recent result and one far outside the 30-day results window.
        s.insert_results(&[
            (result("a", now - 1000, 1), Status::Up),
            (result("a", now - 40 * day, 2), Status::Up),
        ])
        .unwrap();
        // One recent alert and one older than the 90-day alerts window.
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            now - 1000,
            None,
        )
        .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Down),
            NodeState::Up,
            now - 100 * day,
            None,
        )
        .unwrap();
        // An open incident (any age survives) and a resolved one older than
        // the alerts window (pruned, with its update and its affects row).
        let open = |id: &str, started: i64, check: &str| {
            s.create_incident(
                id,
                "outage",
                IncidentState::Detected,
                started,
                false,
                Some(cid(check)),
                &[IncidentCheckRow {
                    check_id: cid(check),
                    source: AffectSource::Origin,
                }],
                Some(("down", None)),
            )
            .unwrap()
        };
        open("open", now - 10 * day, "a");
        open("old", now - 200 * day, "b");
        s.add_update(
            "old",
            IncidentState::Resolved,
            "recovered",
            None,
            now - 100 * day,
        )
        .unwrap();
        // Two undelivered notifications, both older than the window: the one
        // given up on is history, the one still waiting is work in progress and
        // pruning it would silently un-page an outage.
        let queued = |check: &str, attempts: i32, next_attempt: Option<i64>| QueuedRow {
            id: format!("queued-{check}"),
            notifier: "oncall".into(),
            check_id: cid(check),
            previous_state: Some(NodeState::Up),
            current_state: NodeState::Down,
            attempts,
            next_attempt,
            last_error: "the endpoint answered 500".into(),
            queued_at: now - 100 * day,
        };
        s.enqueue_notification(&queued("a", 6, None)).unwrap();
        s.enqueue_notification(&queued("b", 2, Some(now + 1000)))
            .unwrap();

        let removed = s.prune(30, 90, now).unwrap();
        assert_eq!(
            removed, 7,
            "old result + old alert + the resolved incident, its 2 updates and its \
             affects row + the given-up notification"
        );
        let left = s.undelivered_notifications(10).unwrap();
        assert_eq!(left.len(), 1, "the one still being retried survives");
        assert_eq!(left[0].check_id, cid("b"));

        // The recent result and alert survive.
        let status = s.list_status(now).unwrap();
        assert_eq!(status.len(), 1, "only the recent result's check remains");
        assert_eq!(status[0].check_id, cid("a"));
        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
        // The open incident survives whole; the resolved one is gone entirely.
        let incidents = s.list_incidents(10, None, None).unwrap();
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].id, "open");
        assert_eq!(incidents[0].state, IncidentState::Detected);
        assert_eq!(
            incidents[0].checks.len(),
            1,
            "the surviving incident keeps its affects row"
        );
        assert!(
            s.updates_for_incident("old").unwrap().is_empty(),
            "a pruned incident leaves no orphaned narrative behind"
        );
    }

    #[test]
    fn prune_metadata_rolls_back_dependent_deletes_when_watermark_rebuild_fails() {
        let s = Storage::open(":memory:").unwrap();
        let day = 86_400_000i64;
        let now = 2 * day;
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            1,
            None,
        )
        .unwrap();
        s.create_incident(
            "old",
            "outage",
            IncidentState::Detected,
            0,
            false,
            Some(cid("a")),
            &[IncidentCheckRow {
                check_id: cid("a"),
                source: AffectSource::Origin,
            }],
            Some(("down", None)),
        )
        .unwrap();
        s.add_update("old", IncidentState::Resolved, "recovered", None, 0)
            .unwrap();

        // The pre-prune stream maxima differ. After all old metadata is deleted,
        // rebuilding writes three zero-max rows and this index rejects the
        // second one, after the dependent deletes have run.
        s.conn
            .execute_batch(
                "CREATE UNIQUE INDEX fail_metadata_watermark_rebuild
                 ON replication_stream_watermarks (max_millis)",
            )
            .unwrap();
        assert!(s.prune_metadata(1, 1, now).is_err());

        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
        let incident = s.incident("old").unwrap().expect("incident rolled back");
        assert_eq!(incident.checks.len(), 1, "affects row rolled back");
        assert_eq!(s.updates_for_incident("old").unwrap().len(), 2);

        s.conn
            .execute_batch("DROP INDEX fail_metadata_watermark_rebuild")
            .unwrap();
        assert!(s.prune_metadata(1, 1, now).unwrap() > 0);
        assert!(s.incident("old").unwrap().is_none());
    }

    #[test]
    fn prune_metadata_is_idempotent_after_restart() {
        let path = std::env::temp_dir().join(format!(
            "warden-prune-metadata-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let now = 2 * 86_400_000i64;
        {
            let s = Storage::open(&path_string).unwrap();
            s.insert_alert(
                AlertScope::Check,
                "a",
                Some(NodeState::Up),
                NodeState::Down,
                1,
                None,
            )
            .unwrap();
            assert_eq!(s.prune_metadata(1, 1, now).unwrap(), 1);
            assert_eq!(s.stream_digest("alerts").unwrap().count, 0);
        }

        let restarted = Storage::open(&path_string).unwrap();
        assert_eq!(restarted.prune_metadata(1, 1, now).unwrap(), 0);
        assert_eq!(restarted.recent_alerts(10).unwrap().len(), 0);
        assert_eq!(restarted.stream_digest("alerts").unwrap().count, 0);
        drop(restarted);
        std::fs::remove_file(path).unwrap();
    }

    /// Small-scale, always-on regression cover for TODO.md's "restart/recovery
    /// tests using a persistent production-sized database" — see
    /// `benchmark_restart_recovery_at_production_scale` below for the scaled
    /// version this mirrors. Fast enough to run on every `cargo test`, so a
    /// regression in WAL replay, watermark rebuilds, or the backup/restore
    /// round trip fails CI rather than waiting for someone to run the
    /// `#[ignore]`d benchmark by hand.
    ///
    /// `std::mem::forget` skips `Connection`'s `Drop` rather than calling it,
    /// which is the closest thing to an unclean shutdown reachable safely
    /// from within one test process — an actual killed process is exercised
    /// manually against the built binary (see SCALING.md's backup section)
    /// but is not something a unit test can safely automate. It is a real
    /// difference from a clean `drop`, not a no-op: a clean close lets DuckDB
    /// release its file lock and finish any pending bookkeeping on the way
    /// out, and skipping that is exactly what durability across restarts is
    /// supposed to tolerate.
    #[test]
    fn restart_recovers_committed_rows_and_backup_restore_preserves_them() {
        let path = std::env::temp_dir().join(format!(
            "warden-restart-recovery-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let checks = 20u64;
        let observations_per_check = 500u64;

        {
            let store = Storage::open(&path_string).unwrap();
            store
                .conn
                .execute_batch(&format!(
                    "INSERT INTO check_results
                     SELECT md5('check-' || c::VARCHAR)::UUID, 'agent-a', o * 1000,
                            'up', 12, 200, NULL, 'zone', 'region', NULL
                     FROM range({checks}) t(c) CROSS JOIN range({observations_per_check}) u(o);"
                ))
                .unwrap();
            store.rebuild_result_watermarks().unwrap();
            store.rebuild_latest_check_results().unwrap();
            store.rebuild_latency_rollups().unwrap();
            store
                .update_current_state(cid("check-0"), NodeState::Up, None, 1_000)
                .unwrap();
            // No explicit checkpoint and no clean drop: the WAL, not the main
            // file, is what has to carry every one of these commits forward.
            std::mem::forget(store);
        }

        let expected_rows = checks * observations_per_check;
        let recovery_started = std::time::Instant::now();
        let restarted = Storage::open(&path_string).unwrap();
        let recovery_elapsed = recovery_started.elapsed();
        assert!(
            recovery_elapsed < std::time::Duration::from_secs(5),
            "recovery of {expected_rows} rows took {recovery_elapsed:?}, which would be a \
             regression worth investigating even at this small a scale"
        );
        assert_eq!(restarted.count_results().unwrap(), expected_rows);
        assert_eq!(restarted.list_current_state().unwrap().len(), 1);
        let watermarks = restarted.digest(0).unwrap();
        assert_eq!(watermarks.checks.len() as u64, checks);
        assert_eq!(
            watermarks
                .checks
                .iter()
                .map(|c| c.count as u64)
                .sum::<u64>(),
            expected_rows
        );
        let minute_rollups = restarted
            .rollup_digest("check_latency_minute_rollups")
            .unwrap();
        let hour_rollups = restarted
            .rollup_digest("check_latency_hour_rollups")
            .unwrap();
        assert!(minute_rollups.count > 0);
        assert!(hour_rollups.count > 0);

        let backup_path = std::env::temp_dir().join(format!(
            "warden-restart-recovery-backup-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let restore_path = std::env::temp_dir().join(format!(
            "warden-restart-recovery-restored-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        crate::coordinator::backup::create(&path_string, &backup_path).unwrap();
        let report = crate::coordinator::backup::restore(&backup_path, &restore_path).unwrap();
        assert!(report.checksum_verified);
        assert_eq!(report.raw_results, expected_rows);
        assert_eq!(report.current_states, 1);

        let restored = Storage::open(restore_path.to_str().unwrap()).unwrap();
        assert_eq!(restored.count_results().unwrap(), expected_rows);
        let restored_watermarks = restored.digest(0).unwrap();
        assert_eq!(restored_watermarks.checks.len(), watermarks.checks.len());
        assert_eq!(
            restored_watermarks
                .checks
                .iter()
                .map(|c| c.count)
                .sum::<i64>(),
            watermarks.checks.iter().map(|c| c.count).sum::<i64>()
        );
        assert_eq!(
            restored
                .rollup_digest("check_latency_minute_rollups")
                .unwrap()
                .count,
            minute_rollups.count
        );
        assert_eq!(
            restored
                .rollup_digest("check_latency_hour_rollups")
                .unwrap()
                .count,
            hour_rollups.count
        );

        drop(restored);
        let _ = std::fs::remove_file(format!("{path_string}.wal"));
        std::fs::remove_file(&path).unwrap();
        for extra in [
            &backup_path,
            &restore_path,
            &std::path::PathBuf::from(format!("{}.manifest.json", backup_path.display())),
        ] {
            let _ = std::fs::remove_file(extra);
        }
    }

    // ---- Clustering: data-plane replication ----

    fn check_config(name: &str, target: &str) -> CheckConfig {
        CheckConfig {
            name: name.into(),
            target: target.into(),
            ..Default::default()
        }
    }

    fn setting_entry(id: &str, millis: i64, node: &str, value: &str) -> ConfigEntry {
        ConfigEntry {
            kind: ConfigKind::Setting,
            id: id.into(),
            hlc: Hlc::new(millis, node),
            document: Some(serde_json::to_string(&value.to_string()).unwrap()),
        }
    }

    #[test]
    fn insert_results_dedup_skips_rows_already_present_but_inserts_new_ones() {
        let s = Storage::open(":memory:").unwrap();
        let now = 10_000i64;
        let r1 = result("a", now, 5);
        let r2 = result("a", now - 1000, 6);

        let inserted = s
            .insert_results_dedup(&[(r1.clone(), Status::Up), (r2.clone(), Status::Up)])
            .unwrap();
        assert_eq!(inserted, 2, "neither row existed yet");

        // Replaying the exact same batch — what anti-entropy retry does — must
        // be a complete no-op.
        let replayed = s
            .insert_results_dedup(&[(r1.clone(), Status::Up), (r2.clone(), Status::Up)])
            .unwrap();
        assert_eq!(replayed, 0, "replaying a batch must not duplicate it");
        assert_eq!(s.history(cid("a"), 10, None, None, None).unwrap().len(), 2);

        // A batch mixing an old row with a genuinely new one only adds the new one.
        let r3 = result("a", now - 2000, 7);
        let mixed = s
            .insert_results_dedup(&[(r1, Status::Up), (r3, Status::Up)])
            .unwrap();
        assert_eq!(
            mixed, 1,
            "only the row this node did not already have is inserted"
        );
        assert_eq!(s.history(cid("a"), 10, None, None, None).unwrap().len(), 3);
    }

    #[test]
    fn insert_results_dedup_key_includes_agent_so_concurrent_agents_dont_collide() {
        let s = Storage::open(":memory:").unwrap();
        let now = 10_000i64;
        let mut a = result("x", now, 1);
        a.agent_id = "agent-a".into();
        let mut b = result("x", now, 1);
        b.agent_id = "agent-b".into();

        let inserted = s
            .insert_results_dedup(&[(a, Status::Up), (b, Status::Up)])
            .unwrap();
        assert_eq!(
            inserted, 2,
            "same check and millisecond, different agents, must not collide"
        );
        assert_eq!(s.history(cid("x"), 10, None, None, None).unwrap().len(), 2);
    }

    #[test]
    fn upsert_alert_replays_identical_rows_but_rejects_collisions() {
        let s = Storage::open(":memory:").unwrap();
        let a = AlertRow {
            id: "fixed-id".into(),
            scope: AlertScope::Check,
            node_id: "web".into(),
            previous_state: Some(NodeState::Up),
            current_state: NodeState::Down,
            timestamp: 1000,
            incident_id: Some("inc-1".into()),
        };
        assert_eq!(s.upsert_alert(&a).unwrap(), AlertApply::Inserted);
        assert_eq!(
            s.upsert_alert(&a).unwrap(),
            AlertApply::Replay,
            "an identical replay is a no-op"
        );
        let mut collision = a.clone();
        collision.current_state = NodeState::Up;
        assert_eq!(
            s.upsert_alert(&collision).unwrap(),
            AlertApply::Conflict,
            "a reused id must not replace an event"
        );
        assert_eq!(s.recent_alerts(10).unwrap(), vec![a]);
    }

    // ---- Clustering: config replication ----

    #[test]
    fn apply_config_entry_higher_hlc_wins_equal_or_lower_is_rejected() {
        let s = Storage::open(":memory:").unwrap();
        let first = setting_entry("k", 100, "a", "first");
        assert!(s.apply_config_entry(&first).unwrap());
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("first"));

        let lower = setting_entry("k", 50, "z", "stale");
        assert!(
            !s.apply_config_entry(&lower).unwrap(),
            "an older stamp must not win, even from a node named after 'z'"
        );
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("first"));

        assert!(
            !s.apply_config_entry(&first).unwrap(),
            "an identical stamp changes nothing — it is not newer than itself"
        );
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("first"));

        let higher = setting_entry("k", 200, "a", "second");
        assert!(s.apply_config_entry(&higher).unwrap());
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("second"));
    }

    #[test]
    fn apply_config_entry_tombstone_higher_deletes_lower_does_not() {
        let s = Storage::open(":memory:").unwrap();
        s.apply_config_entry(&setting_entry("k", 100, "a", "value"))
            .unwrap();

        let low_tomb = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(50, "z"),
            document: None,
        };
        assert!(
            !s.apply_config_entry(&low_tomb).unwrap(),
            "an older delete must not win"
        );
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("value"));

        let high_tomb = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(200, "a"),
            document: None,
        };
        assert!(s.apply_config_entry(&high_tomb).unwrap());
        assert_eq!(s.get_setting("k").unwrap(), None, "the row is gone");
    }

    #[test]
    fn apply_config_entry_upsert_after_tombstone_resurrects_and_clears_it() {
        let s = Storage::open(":memory:").unwrap();
        s.apply_config_entry(&setting_entry("k", 100, "a", "value"))
            .unwrap();
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(200, "a"),
            document: None,
        })
        .unwrap();
        assert_eq!(s.get_setting("k").unwrap(), None);

        let resurrect = setting_entry("k", 300, "a", "back");
        assert!(s.apply_config_entry(&resurrect).unwrap());
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("back"));

        // The tombstone must be cleared, or the next digest sweep would see a
        // row that exists alongside a tombstone that says it does not.
        let tombstones: Vec<_> = s
            .config_entries(ConfigKind::Setting)
            .unwrap()
            .into_iter()
            .filter(|e| e.id == "k" && e.is_tombstone())
            .collect();
        assert!(tombstones.is_empty());
    }

    #[test]
    fn apply_config_entry_a_null_stamped_local_row_loses_to_anything() {
        let s = Storage::open(":memory:").unwrap();
        // Written through the pre-clustering path: no HLC stamp at all.
        s.put_setting("k", "legacy").unwrap();

        let replicated = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(1, "z"),
            document: Some(serde_json::to_string("replicated").unwrap()),
        };
        assert!(
            s.apply_config_entry(&replicated).unwrap(),
            "an unstamped row must lose to any real stamp"
        );
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("replicated"));
    }

    #[test]
    fn concurrent_edits_converge_to_the_same_winner_regardless_of_apply_order() {
        let a = Storage::open(":memory:").unwrap();
        let b = Storage::open(":memory:").unwrap();

        // Same millisecond, different nodes: the tie breaks on node_id, which
        // is a total order, so it must not matter which side applies first.
        let from_x = setting_entry("k", 100, "x", "from-x");
        let from_y = setting_entry("k", 100, "y", "from-y");

        a.apply_config_entry(&from_x).unwrap();
        a.apply_config_entry(&from_y).unwrap();

        b.apply_config_entry(&from_y).unwrap();
        b.apply_config_entry(&from_x).unwrap();

        assert_eq!(
            a.get_setting("k").unwrap(),
            b.get_setting("k").unwrap(),
            "both replicas must converge on the same value"
        );
        assert_eq!(
            a.get_setting("k").unwrap().as_deref(),
            Some("from-y"),
            "'y' beats 'x' as a node id, so it wins the tie either way"
        );
    }

    fn rollup_policy_entry(
        id: &str,
        millis: i64,
        node: &str,
        node_type: NodeType,
        policy: crate::rollup::RollupPolicy,
    ) -> ConfigEntry {
        ConfigEntry {
            kind: ConfigKind::RollupPolicy,
            id: id.into(),
            hlc: Hlc::new(millis, node),
            document: Some(serde_json::to_string(&RollupPolicyDoc { node_type, policy }).unwrap()),
        }
    }

    #[test]
    fn rollup_policy_config_replicates_with_stable_document() {
        use crate::rollup::{RollupPolicy, Threshold};

        let source = Storage::open(":memory:").unwrap();
        let replica = Storage::open(":memory:").unwrap();
        let entry = rollup_policy_entry(
            "region:eu",
            100,
            "source",
            NodeType::Region,
            RollupPolicy::new(Threshold::Any, Threshold::Quorum(2)),
        );
        assert!(source.apply_config_entry(&entry).unwrap());
        let wire = source
            .config_entry(ConfigKind::RollupPolicy, "region:eu")
            .unwrap()
            .unwrap();
        assert_eq!(
            wire.document.as_deref(),
            Some(
                r#"{"node_type":"region","policy":{"degraded_when":"any","down_when":{"quorum":2}}}"#
            )
        );

        assert!(replica.apply_config_entry(&wire).unwrap());
        assert_eq!(
            replica.list_rollup_policies().unwrap()[0].policy,
            entry
                .document
                .as_deref()
                .map(|doc| serde_json::from_str::<RollupPolicyDoc>(doc).unwrap().policy)
                .unwrap()
        );
    }

    #[test]
    fn rollup_policy_config_newer_write_wins() {
        use crate::rollup::{RollupPolicy, Threshold};

        let s = Storage::open(":memory:").unwrap();
        let newer = rollup_policy_entry(
            "zone:r:z",
            200,
            "b",
            NodeType::Zone,
            RollupPolicy::new(Threshold::Majority, Threshold::All),
        );
        let older = rollup_policy_entry(
            "zone:r:z",
            100,
            "a",
            NodeType::Zone,
            RollupPolicy::new(Threshold::Any, Threshold::All),
        );
        assert!(s.apply_config_entry(&newer).unwrap());
        assert!(!s.apply_config_entry(&older).unwrap());
        assert_eq!(
            s.list_rollup_policies().unwrap()[0].policy,
            RollupPolicy::new(Threshold::Majority, Threshold::All)
        );
    }

    #[test]
    fn rollup_policy_config_tombstone_deletes_and_blocks_older_write() {
        use crate::rollup::{RollupPolicy, Threshold};

        let s = Storage::open(":memory:").unwrap();
        let live = rollup_policy_entry(
            "global",
            100,
            "a",
            NodeType::Global,
            RollupPolicy::new(Threshold::Any, Threshold::All),
        );
        assert!(s.apply_config_entry(&live).unwrap());
        let tombstone = ConfigEntry {
            kind: ConfigKind::RollupPolicy,
            id: "global".into(),
            hlc: Hlc::new(200, "b"),
            document: None,
        };
        assert!(s.apply_config_entry(&tombstone).unwrap());
        assert!(s.list_rollup_policies().unwrap().is_empty());
        assert!(!s.apply_config_entry(&live).unwrap());
        assert_eq!(
            s.config_entries(ConfigKind::RollupPolicy).unwrap(),
            vec![tombstone]
        );
    }

    /// Arbitration is per entry, not per batch. Two edits to one id travelling
    /// together must resolve by HLC exactly as they would arriving separately:
    /// a batch is a transport detail, never a rule about who wins.
    #[test]
    fn a_batch_carrying_two_edits_to_one_id_still_resolves_by_hlc() {
        let s = Storage::open(":memory:").unwrap();
        let newer = setting_entry("theme", 2_000, "n1", "dark");
        let older = setting_entry("theme", 1_000, "n1", "light");

        let applied = s
            .apply_config_entries(&[newer.clone(), older.clone()])
            .unwrap();
        assert_eq!(
            applied,
            vec![true, false],
            "the older edit must lose even though it is last in the batch"
        );
        let back = s
            .config_entry(ConfigKind::Setting, "theme")
            .unwrap()
            .unwrap();
        assert_eq!(back.hlc, newer.hlc);

        // And the same pair in the other order still converges on the newer.
        let s = Storage::open(":memory:").unwrap();
        let applied = s.apply_config_entries(&[older, newer.clone()]).unwrap();
        assert_eq!(applied, vec![true, true]);
        let back = s
            .config_entry(ConfigKind::Setting, "theme")
            .unwrap()
            .unwrap();
        assert_eq!(back.hlc, newer.hlc);
    }

    #[test]
    fn config_batch_rolls_back_when_a_later_document_is_malformed() {
        let s = Storage::open(":memory:").unwrap();
        let valid = setting_entry("theme", 1_000, "n1", "dark");
        let malformed = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "broken".into(),
            hlc: Hlc::new(1_000, "n1"),
            document: Some("not JSON".into()),
        };

        assert!(s.apply_config_entries(&[valid, malformed]).is_err());
        assert!(s
            .config_entry(ConfigKind::Setting, "theme")
            .unwrap()
            .is_none());
    }

    /// Manual provisioning benchmark for the config write path. Run with
    /// `cargo test benchmark_apply_config_entries_10k -- --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark"]
    fn benchmark_apply_config_entries_10k() {
        let s = Storage::open(":memory:").unwrap();
        let entries = (0..10_000)
            .map(|i| setting_entry(&format!("setting-{i}"), i, "n1", "value"))
            .collect::<Vec<_>>();
        let start = std::time::Instant::now();
        let applied = s.apply_config_entries(&entries).unwrap();
        eprintln!(
            "applied {} config entries in {:?}",
            applied.iter().filter(|applied| **applied).count(),
            start.elapsed()
        );
        assert!(applied.iter().all(|applied| *applied));
    }

    /// The prune keeps each check's newest span regardless of age, because
    /// that span is not history — it is what the check reads as right now.
    #[test]
    fn state_history_prune_keeps_each_checks_newest_span_however_old() {
        let s = Storage::open(":memory:").unwrap();
        let day = 86_400_000i64;
        let stale = crate::models::new_check_id();
        let straddling = crate::models::new_check_id();
        let single = crate::models::new_check_id();
        // `record_state` only writes a span when the state actually changes,
        // so these alternate.
        for (check, since, state) in [
            (stale, day, NodeState::Up),
            (stale, 2 * day, NodeState::Down),
            (stale, 3 * day, NodeState::Up),
            (straddling, 2 * day, NodeState::Up),
            (straddling, 9 * day, NodeState::Down),
            (straddling, 10 * day, NodeState::Up),
            (single, day, NodeState::Up),
        ] {
            s.record_state(check, state, since).unwrap();
        }
        // Horizon at day 8: `now` is day 15 with a 7-day result window.
        s.prune_metadata(7, 90, 15 * day).unwrap();

        let spans = |check| {
            s.state_spans(check)
                .unwrap()
                .into_iter()
                .map(|span| span.since)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            spans(stale),
            vec![3 * day],
            "every span predates the horizon"
        );
        assert_eq!(spans(straddling), vec![9 * day, 10 * day]);
        assert_eq!(spans(single), vec![day]);
    }

    /// Raw results are the only copy of an observation until a rollup covers
    /// it. Retention deleting on a clock alone trusts a write it never
    /// checked, and the window it would lose is indistinguishable afterwards
    /// from a window in which nothing was measured.
    #[test]
    fn retention_will_not_delete_raw_results_no_rollup_covers() {
        let s = Storage::open(":memory:").unwrap();
        let minute = MILLIS_PER_MINUTE;
        let rows: Vec<_> = (1..=5)
            .map(|i| (result("covered", i * minute, 10), Status::Up))
            .collect();
        s.insert_results_dedup(&rows).unwrap();
        assert_eq!(s.count_results().unwrap(), 5);

        // Simulate the rollup for minute 2 never having been written: a failed
        // write, or a restore that carried raw rows without their summaries.
        s.conn
            .execute(
                "DELETE FROM check_latency_minute_rollups WHERE bucket_start = ?",
                duckdb::params![2 * minute],
            )
            .unwrap();

        // Asked to drop everything below minute 5, retention stops at the gap.
        s.prune_results(5 * minute).unwrap();
        assert_eq!(
            s.count_results().unwrap(),
            4,
            "only the fully summarized minute below the gap may go"
        );

        // Once the summary exists, the same request completes.
        s.conn
            .execute(
                "INSERT INTO check_latency_minute_rollups
                 VALUES (?, 'agent', ?, 1, 1, 10, 10, 10)",
                duckdb::params![cid("covered"), 2 * minute],
            )
            .unwrap();
        s.prune_results(5 * minute).unwrap();
        assert_eq!(s.count_results().unwrap(), 1, "only minute 5 is retained");
    }

    /// Manual benchmark for the state-history prune. Run with
    /// `cargo test benchmark_state_history_prune -- --ignored --nocapture`.
    /// Reports the shipped anti-join against the correlated form it replaced,
    /// on the same seeded table, so the rewrite is justified by measurement.
    #[test]
    #[ignore = "benchmark"]
    fn benchmark_state_history_prune() {
        const CORRELATED: &str = "DELETE FROM state_history
             WHERE since < ?
               AND since <> (SELECT MAX(since) FROM state_history h
                             WHERE h.check_id = state_history.check_id)";
        const ANTI_JOIN: &str = "DELETE FROM state_history
             WHERE since < ?
               AND NOT EXISTS (
                   SELECT 1 FROM (
                       SELECT check_id, MAX(since) AS newest
                       FROM state_history GROUP BY check_id
                   ) latest
                   WHERE latest.check_id = state_history.check_id
                     AND latest.newest = state_history.since)";

        let checks: i64 = std::env::var("WARDEN_BENCH_CHECKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);
        let spans: i64 = std::env::var("WARDEN_BENCH_SPANS_PER_CHECK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        let horizon = spans / 2 * 60_000;
        let seed = |store: &Storage| {
            store
                .conn
                .execute_batch(&format!(
                    "DELETE FROM state_history;
                     INSERT INTO state_history
                     SELECT (uuid_extract_timestamp(uuid())::VARCHAR || c.i::VARCHAR)::VARCHAR,
                            'up', s.i * 60000
                     FROM range(0, {checks}) c(i), range(0, {spans}) s(i);"
                ))
                .or_else(|_| {
                    store.conn.execute_batch(&format!(
                        "DELETE FROM state_history;
                         INSERT INTO state_history
                         SELECT ('00000000-0000-7000-8000-' || lpad(c.i::VARCHAR, 12, '0'))::UUID,
                                'up', s.i * 60000
                         FROM range(0, {checks}) c(i), range(0, {spans}) s(i);"
                    ))
                })
                .unwrap();
        };

        let mut report = Vec::new();
        for (label, sql) in [("anti-join", ANTI_JOIN), ("correlated", CORRELATED)] {
            let store = Storage::open(":memory:").unwrap();
            seed(&store);
            let start = std::time::Instant::now();
            let removed = store.conn.execute(sql, duckdb::params![horizon]).unwrap();
            report.push((label, removed, start.elapsed()));
        }
        for (label, removed, elapsed) in &report {
            eprintln!(
                "{label}: pruned {removed} of {} rows in {elapsed:?}",
                checks * spans
            );
        }
        assert_eq!(
            report[0].1, report[1].1,
            "both forms must remove exactly the same rows"
        );
    }

    #[test]
    fn config_entry_round_trips_each_kind_through_apply_and_read_back() {
        let s = Storage::open(":memory:").unwrap();
        let hlc = Hlc::new(1_000, "n1");

        let check = check_config("web", "https://example.com");
        let e = ConfigEntry {
            kind: ConfigKind::Check,
            id: check.id.to_string(),
            hlc: hlc.clone(),
            document: Some(serde_json::to_string(&check).unwrap()),
        };
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::Check, &check.id.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(back.hlc, hlc);
        let round: CheckConfig = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, check);

        let section = Section {
            name: "core".into(),
            position: 3,
            collapsed: true,
            display: SectionDisplay::Page,
        };
        let e = ConfigEntry {
            kind: ConfigKind::Section,
            id: "core".into(),
            hlc: hlc.clone(),
            document: Some(serde_json::to_string(&section).unwrap()),
        };
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::Section, "core")
            .unwrap()
            .unwrap();
        let round: Section = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, section);

        let e = setting_entry("theme", hlc.millis, &hlc.node_id, "dark");
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::Setting, "theme")
            .unwrap()
            .unwrap();
        let round: String = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, "dark");

        let cred = AgentCredential {
            id: "cred-1".into(),
            zone: "z1".into(),
            region: "r1".into(),
            secret_hash: "hash".into(),
            salt: "salt".into(),
            created_at: 5,
            revoked_at: Some(9),
        };
        let e = ConfigEntry {
            kind: ConfigKind::Credential,
            id: "cred-1".into(),
            hlc: hlc.clone(),
            document: Some(serde_json::to_string(&CredentialDoc::from(&cred)).unwrap()),
        };
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::Credential, "cred-1")
            .unwrap()
            .unwrap();
        let round: CredentialDoc = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, CredentialDoc::from(&cred));

        let policy = crate::rollup::RollupPolicy::new(
            crate::rollup::Threshold::Any,
            crate::rollup::Threshold::All,
        );
        let e = rollup_policy_entry(
            "region:test",
            hlc.millis,
            &hlc.node_id,
            NodeType::Region,
            policy,
        );
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::RollupPolicy, "region:test")
            .unwrap()
            .unwrap();
        let round: RollupPolicyDoc = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(
            round,
            RollupPolicyDoc {
                node_type: NodeType::Region,
                policy
            }
        );

        // Every kind now has exactly its one applied entry, no tombstones.
        for kind in ConfigKind::ALL {
            let entries = s.config_entries(kind).unwrap();
            assert_eq!(entries.len(), 1, "{kind:?} should have exactly one entry");
            assert!(!entries[0].is_tombstone());
        }

        // A missing id reads back as absent, not as an empty document.
        assert_eq!(s.config_entry(ConfigKind::Check, "nope").unwrap(), None);
    }

    #[test]
    fn config_entries_includes_tombstones_alongside_live_rows() {
        let s = Storage::open(":memory:").unwrap();
        s.apply_config_entry(&setting_entry("live", 100, "a", "v"))
            .unwrap();
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "dead".into(),
            hlc: Hlc::new(100, "a"),
            document: None,
        })
        .unwrap();

        let entries = s.config_entries(ConfigKind::Setting).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|e| e.id == "live" && !e.is_tombstone()));
        assert!(entries.iter().any(|e| e.id == "dead" && e.is_tombstone()));
    }

    #[test]
    fn config_entries_page_orders_and_bounds_live_rows_and_tombstones() {
        let s = Storage::open(":memory:").unwrap();
        for id in ["alpha", "charlie", "delta"] {
            s.apply_config_entry(&setting_entry(id, 100, "a", id))
                .unwrap();
        }
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "bravo".into(),
            hlc: Hlc::new(100, "a"),
            document: None,
        })
        .unwrap();

        let first = s.config_entries_page(ConfigKind::Setting, None, 2).unwrap();
        assert_eq!(
            first
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "bravo"]
        );
        assert!(!first[0].is_tombstone());
        assert!(first[1].is_tombstone());

        let second = s
            .config_entries_page(ConfigKind::Setting, Some("bravo"), 2)
            .unwrap();
        assert_eq!(
            second
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["charlie", "delta"]
        );
        assert!(s
            .config_entries_page(ConfigKind::Setting, Some("delta"), 2)
            .unwrap()
            .is_empty());
    }

    // ---- Placement-scoped configuration, Phase 2 (SCALING.md) ----

    fn placed_check(name: &str, regions: &[&str]) -> CheckConfig {
        CheckConfig {
            placement: crate::placement::Placement {
                regions: regions.iter().map(|r| r.to_string()).collect(),
                ..Default::default()
            },
            ..check_config(name, "https://example.com")
        }
    }

    #[test]
    fn check_ids_for_region_admits_unplaced_and_named_regions_only() {
        let s = Storage::open(":memory:").unwrap();
        let everywhere = placed_check("everywhere", &[]);
        let us_only = placed_check("us-only", &["us"]);
        let tw_only = placed_check("tw-only", &["tw"]);
        for c in [&everywhere, &us_only, &tw_only] {
            s.upsert_check(c, 1).unwrap();
        }

        let us_ids = s.check_ids_for_region("us").unwrap();
        assert_eq!(
            us_ids,
            {
                let mut v = vec![everywhere.id, us_only.id];
                v.sort();
                v
            },
            "us gets the unscoped check and the one explicitly placed there"
        );

        let tw_ids = s.check_ids_for_region("tw").unwrap();
        assert_eq!(
            tw_ids,
            {
                let mut v = vec![everywhere.id, tw_only.id];
                v.sort();
                v
            },
            "tw must not see the us-only check"
        );
    }

    #[test]
    fn upsert_check_rewrites_the_placement_projection_on_every_edit() {
        let s = Storage::open(":memory:").unwrap();
        let mut check = placed_check("moving", &["us"]);
        s.upsert_check(&check, 1).unwrap();
        assert_eq!(s.check_ids_for_region("us").unwrap(), vec![check.id]);
        assert!(s.check_ids_for_region("tw").unwrap().is_empty());

        // Re-placed to tw only: the old row in `check_placement_regions` must
        // not linger and leave the check reachable from both regions.
        check.placement.regions = vec!["tw".into()];
        s.upsert_check(&check, 2).unwrap();
        assert!(s.check_ids_for_region("us").unwrap().is_empty());
        assert_eq!(s.check_ids_for_region("tw").unwrap(), vec![check.id]);
    }

    /// The comparison Phase 2 reconciliation relies on: a region's
    /// placement-scoped digest must equal the whole-table digest the region
    /// itself reports once it holds exactly that scoped subset — otherwise
    /// the two sides could never agree "converged" without paging documents
    /// to check. Tombstones are deliberately unscoped (a delete reaches
    /// every region), so they count identically in both digests too.
    #[test]
    fn config_digest_for_region_matches_a_region_holding_exactly_its_subset() {
        let global = Storage::open(":memory:").unwrap();
        let region = Storage::open(":memory:").unwrap();
        let us_only = placed_check("us-only", &["us"]);
        let tw_only = placed_check("tw-only", &["tw"]);
        global.upsert_check(&us_only, 10).unwrap();
        global.upsert_check(&tw_only, 20).unwrap();
        // The region only ever received what its placement admits — the
        // steady state Phase 2 reconciliation converges every region to.
        region.upsert_check(&us_only, 10).unwrap();

        let scoped = global.config_digest_for_region("us").unwrap();
        let whole = region.config_digest_for(ConfigKind::Check).unwrap();
        assert_eq!(
            scoped, whole,
            "a converged region's whole-table digest must equal the global \
             tier's placement-scoped one, or every sweep would look behind"
        );

        // A whole-catalogue comparison, the pre-Phase-2 behaviour, would
        // disagree forever even though the region is fully converged.
        let unscoped = global.config_digest_for(ConfigKind::Check).unwrap();
        assert_ne!(
            unscoped, whole,
            "the whole catalogue must differ from a converged subset region \
             — this is exactly the comparison Phase 2 replaces"
        );
    }

    #[test]
    fn config_digest_for_region_counts_tombstones_unscoped() {
        let s = Storage::open(":memory:").unwrap();
        let us_only = placed_check("us-only", &["us"]);
        s.upsert_check(&us_only, 10).unwrap();
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Check,
            id: crate::models::new_check_id().to_string(),
            hlc: Hlc::new(20, "global"),
            document: None,
        })
        .unwrap();

        // A region that never held the deleted check, and whose placement
        // admits nothing else here, still must count the tombstone: deletes
        // reach every region regardless of placement.
        let tw_digest = s.config_digest_for_region("tw").unwrap();
        assert_eq!(tw_digest.count, 1, "the tombstone alone");
        let us_digest = s.config_digest_for_region("us").unwrap();
        assert_eq!(us_digest.count, 2, "the placed check plus the tombstone");
    }

    #[test]
    fn unplace_and_delete_clear_the_placement_projection() {
        let s = Storage::open(":memory:").unwrap();
        let a = placed_check("a", &["us"]);
        let b = placed_check("b", &["us"]);
        s.upsert_check(&a, 1).unwrap();
        s.upsert_check(&b, 1).unwrap();
        assert_eq!(s.check_ids_for_region("us").unwrap().len(), 2);

        assert!(s.unplace_check_local(a.id).unwrap());
        assert_eq!(
            s.check_ids_for_region("us").unwrap(),
            vec![b.id],
            "unplace must drop the projection row, or a later differently-\
             placed check reusing no id could still misreport membership"
        );

        assert!(s.delete_check(b.id).unwrap());
        assert!(s.check_ids_for_region("us").unwrap().is_empty());
    }

    /// A replicated tombstone applies through a different code path
    /// (`apply_config_entries`'s prepared per-kind statement) than
    /// `unplace_check_local`/`delete_check`, so it needs its own assertion
    /// that the projection is still cleaned up rather than left to rot.
    #[test]
    fn a_replicated_check_tombstone_clears_the_placement_projection() {
        let s = Storage::open(":memory:").unwrap();
        let check = placed_check("doomed", &["us"]);
        s.upsert_check(&check, 1).unwrap();
        assert_eq!(s.check_ids_for_region("us").unwrap(), vec![check.id]);

        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Check,
            id: check.id.to_string(),
            hlc: Hlc::new(2, "peer"),
            document: None,
        })
        .unwrap();
        assert!(s.check_ids_for_region("us").unwrap().is_empty());
    }

    #[test]
    fn stamp_config_restamps_without_changing_the_row() {
        let s = Storage::open(":memory:").unwrap();
        s.put_setting("k", "value").unwrap();
        let hlc = Hlc::new(500, "n1");
        s.stamp_config(ConfigKind::Setting, "k", &hlc).unwrap();
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("value"));
        let entry = s.config_entry(ConfigKind::Setting, "k").unwrap().unwrap();
        assert_eq!(entry.hlc, hlc);
    }

    #[test]
    fn prune_tombstones_removes_old_ones_and_keeps_recent_ones() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000i64;
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "old".into(),
            hlc: Hlc::new(now - 100_000, "a"),
            document: None,
        })
        .unwrap();
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "recent".into(),
            hlc: Hlc::new(now - 10, "a"),
            document: None,
        })
        .unwrap();

        let removed = s.prune_tombstones(now - 50_000).unwrap();
        assert_eq!(removed, 1, "only the old tombstone is removed");

        let remaining = s.config_entries(ConfigKind::Setting).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, "recent");
    }

    // ---- Clustering: incidents ----

    fn sample_replicated_incident() -> ReplicatedIncident {
        ReplicatedIncident {
            id: "inc-1".into(),
            version: Hlc::new(100, "a"),
            title: "Outage".into(),
            state: IncidentState::Detected,
            started_at: 100,
            resolved_at: None,
            global: false,
            origin_check: Some(cid("a")),
            checks: vec![(cid("a"), AffectSource::Origin)],
            updates: vec![IncidentUpdateRow {
                id: "inc-1-u1".into(),
                incident_id: "inc-1".into(),
                state: IncidentState::Detected,
                body: "down".into(),
                author: None,
                created_at: 100,
            }],
        }
    }

    #[test]
    fn upsert_incident_is_idempotent_and_updates_on_a_changed_title() {
        let s = Storage::open(":memory:").unwrap();
        let i = sample_replicated_incident();
        s.upsert_incident(&i).unwrap();
        s.upsert_incident(&i).unwrap();

        let stored = s.incident("inc-1").unwrap().unwrap();
        assert_eq!(stored.checks.len(), 1, "no duplicated attachment");
        assert_eq!(
            s.updates_for_incident("inc-1").unwrap().len(),
            1,
            "no duplicated update"
        );

        let mut renamed = i.clone();
        renamed.version = Hlc::new(101, "a");
        renamed.title = "Major outage".into();
        s.upsert_incident(&renamed).unwrap();
        let stored = s.incident("inc-1").unwrap().unwrap();
        assert_eq!(stored.title, "Major outage");
        assert_eq!(
            s.list_incidents(10, None, None).unwrap().len(),
            1,
            "still one incident row, not a second one"
        );
    }

    #[test]
    fn upsert_incident_replays_identically_and_rejects_update_id_collisions() {
        let s = Storage::open(":memory:").unwrap();
        let incident = sample_replicated_incident();
        assert!(s.upsert_incident(&incident).unwrap());
        assert!(
            !s.upsert_incident(&incident).unwrap(),
            "an identical replay is a no-op"
        );

        let mut collision = incident.clone();
        collision.id = "inc-2".into();
        collision.updates[0].incident_id = collision.id.clone();
        collision.updates[0].body = "different event with reused id".into();
        assert!(s.upsert_incident(&collision).is_err());
        assert_eq!(s.replicated_incident("inc-1").unwrap(), Some(incident));
        assert_eq!(s.replicated_incident("inc-2").unwrap(), None);
    }

    #[test]
    fn replicated_incident_round_trips_through_upsert() {
        let s = Storage::open(":memory:").unwrap();
        let i = sample_replicated_incident();
        s.upsert_incident(&i).unwrap();
        let back = s.replicated_incident("inc-1").unwrap().unwrap();
        assert_eq!(back, i);
        assert_eq!(s.replicated_incident("missing").unwrap(), None);
    }

    #[test]
    fn equal_version_incident_conflicts_converge_independent_of_delivery_order() {
        let a = Storage::open(":memory:").unwrap();
        let b = Storage::open(":memory:").unwrap();
        let first = sample_replicated_incident();
        let mut second = first.clone();
        second.title = "Conflicting title".into();
        let expected = if serde_json::to_vec(&first).unwrap() > serde_json::to_vec(&second).unwrap()
        {
            first.clone()
        } else {
            second.clone()
        };

        a.upsert_incident(&first).unwrap();
        a.upsert_incident(&second).unwrap();
        b.upsert_incident(&second).unwrap();
        b.upsert_incident(&first).unwrap();

        assert_eq!(
            a.replicated_incident("inc-1").unwrap(),
            Some(expected.clone())
        );
        assert_eq!(b.replicated_incident("inc-1").unwrap(), Some(expected));
    }

    #[test]
    fn incident_tombstone_cursor_and_digest_include_node_id_before_id() {
        let s = Storage::open(":memory:").unwrap();
        for (id, node_id) in [("z", "a"), ("a", "z")] {
            s.apply_incident_tombstone(&IncidentTombstone {
                id: id.into(),
                deleted_at: 101,
                node_id: node_id.into(),
            })
            .unwrap();
        }

        let digest = s.digest(0).unwrap();
        assert_eq!(digest.incident_tombstones.max_millis, 101);
        assert_eq!(digest.incident_tombstones.max_id, "z\0a");
        assert_eq!(
            s.incident_tombstones_since(101, "a", "z", 10).unwrap(),
            vec![IncidentTombstone {
                id: "a".into(),
                deleted_at: 101,
                node_id: "z".into(),
            }]
        );
    }

    #[test]
    fn incident_rows_and_tombstones_obey_one_lww_order_and_exact_digests() {
        let s = Storage::open(":memory:").unwrap();
        let mut incident = sample_replicated_incident();
        assert!(s.upsert_incident(&incident).unwrap());
        assert!(!s
            .apply_incident_tombstone(&IncidentTombstone {
                id: incident.id.clone(),
                deleted_at: 99,
                node_id: "z".into(),
            })
            .unwrap());
        assert!(s.incident(&incident.id).unwrap().is_some());

        assert!(s
            .apply_incident_tombstone(&IncidentTombstone {
                id: incident.id.clone(),
                deleted_at: 101,
                node_id: "a".into(),
            })
            .unwrap());
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.incidents.count, 0);
        assert_eq!(digest.incidents.max_millis, 0);
        assert_eq!(digest.incident_tombstones.count, 1);

        incident.version = Hlc::new(102, "a");
        incident.title = "recreated".into();
        assert!(s.upsert_incident(&incident).unwrap());
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.incidents.count, 1);
        assert_eq!(digest.incidents.max_millis, 102);
        assert_eq!(digest.incident_tombstones.count, 0);
        assert!(!s
            .apply_incident_tombstone(&IncidentTombstone {
                id: incident.id.clone(),
                deleted_at: 101,
                node_id: "z".into(),
            })
            .unwrap());
        assert_eq!(
            s.incident(&incident.id).unwrap().unwrap().title,
            "recreated"
        );
    }

    #[test]
    fn incident_check_mutations_advance_versions_but_noops_do_not() {
        let s = Storage::open(":memory:").unwrap();
        s.set_incident_node_id("coordinator-a");
        s.create_incident(
            "inc",
            "Outage",
            IncidentState::Detected,
            100,
            false,
            Some(cid("a")),
            &[IncidentCheckRow {
                check_id: cid("a"),
                source: AffectSource::Origin,
            }],
            None,
        )
        .unwrap();
        let version = || s.replicated_incident("inc").unwrap().unwrap().version;
        let initial = version();
        assert_eq!(initial.node_id, "coordinator-a");

        s.attach_check("inc", cid("b"), AffectSource::Dependency)
            .unwrap();
        let attached = version();
        assert!(attached > initial);
        s.attach_check("inc", cid("b"), AffectSource::Dependency)
            .unwrap();
        assert_eq!(version(), attached);

        s.detach_check("inc", cid("b"), Some(AffectSource::Dependency))
            .unwrap();
        let detached = version();
        assert!(detached > attached);
        s.detach_check("inc", cid("b"), Some(AffectSource::Dependency))
            .unwrap();
        assert_eq!(version(), detached);

        s.set_manual_checks("inc", &[cid("c")]).unwrap();
        let manual = version();
        assert!(manual > detached);
        s.set_manual_checks("inc", &[cid("c"), cid("c")]).unwrap();
        assert_eq!(version(), manual);

        s.purge_check_everything(cid("c"), 1_000, "coordinator-a")
            .unwrap();
        assert!(
            version() > manual,
            "purge membership removal must replicate"
        );
    }

    // ---- Clustering: paged backfill ----

    #[test]
    fn since_readers_page_oldest_first_and_exclude_the_cursor() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_results(&[
            (result("a", 1000, 1), Status::Up),
            (result("a", 2000, 2), Status::Down),
        ])
        .unwrap();
        let page = s.results_since(cid("a"), 1000, None, 5000, 10).unwrap();
        assert_eq!(page.len(), 1, "since is exclusive of the cursor");
        assert_eq!(page[0].0.timestamp.timestamp_millis(), 2000);
        assert_eq!(page[0].1, Status::Down);
        // Fields never persisted in `check_results` read back empty/None
        // rather than being invented.
        assert_eq!(page[0].0.body, None);
        assert_eq!(page[0].0.headers, None);

        let mut same_ts = result("a", 2000, 3);
        same_ts.agent_id = "z-agent".into();
        s.insert_results(&[(same_ts, Status::Up)]).unwrap();
        let page = s
            .results_since(cid("a"), 2000, Some("agent"), 5000, 10)
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0.agent_id, "z-agent");

        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            2000,
            None,
        )
        .unwrap();
        let alerts = s.alerts_since(1000, "", 10).unwrap();
        assert_eq!(alerts.len(), 1);

        s.create_incident(
            "inc-1",
            "t",
            IncidentState::Detected,
            2000,
            false,
            Some(cid("a")),
            &[IncidentCheckRow {
                check_id: cid("a"),
                source: AffectSource::Origin,
            }],
            None,
        )
        .unwrap();
        let incidents = s.incidents_since(1000, "", 10).unwrap();
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].id, "inc-1");
    }

    #[test]
    fn result_backfill_pages_all_rows_sharing_a_timestamp() {
        let s = Storage::open(":memory:").unwrap();
        let rows = (0..501)
            .map(|i| {
                let mut row = result("a", 2000, i);
                row.agent_id = format!("agent-{i:04}");
                (row, Status::Up)
            })
            .collect::<Vec<_>>();
        s.insert_results(&rows).unwrap();

        let first = s.results_since(cid("a"), 1000, None, 2000, 500).unwrap();
        assert_eq!(first.len(), 500);
        let last = first.last().unwrap();
        let second = s
            .results_since(
                cid("a"),
                last.0.timestamp.timestamp_millis(),
                Some(&last.0.agent_id),
                2000,
                500,
            )
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].0.agent_id, "agent-0500");
    }

    #[test]
    fn global_result_backfill_cursor_pages_shared_timestamp_across_checks() {
        let s = Storage::open(":memory:").unwrap();
        let rows = (0..501)
            .map(|i| {
                let mut row = result(&format!("check-{i:04}"), 2_000, i);
                row.agent_id = "same-agent".into();
                (row, Status::Up)
            })
            .collect::<Vec<_>>();
        s.insert_results(&rows).unwrap();

        let first = s.results_after(i64::MIN, None, None, 500).unwrap();
        assert_eq!(first.len(), 500);
        let cursor = &first[499].0;
        let second = s
            .results_after(
                cursor.timestamp.timestamp_millis(),
                Some(cursor.check_id),
                Some(&cursor.agent_id),
                500,
            )
            .unwrap();
        assert_eq!(second.len(), 1);
        assert!(first
            .iter()
            .all(|(row, _)| row.timestamp.timestamp_millis() == 2_000));
        assert_eq!(second[0].0.timestamp.timestamp_millis(), 2_000);
        assert_ne!(first.last().unwrap().0.check_id, second[0].0.check_id);
    }

    // ---- Clustering: digest ----

    #[test]
    fn full_digest_uses_write_time_watermarks() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_results(&[
            (result("a", 1000, 1), Status::Up),
            (result("a", 2000, 2), Status::Down),
            (result("b", 1500, 3), Status::Up),
        ])
        .unwrap();

        s.conn.execute("DELETE FROM check_results", []).unwrap();

        let digest = s.digest(0).unwrap();
        let a = digest
            .checks
            .iter()
            .find(|entry| entry.check_id == cid("a"))
            .unwrap();
        assert_eq!(a.max_ts, 2000);
        assert_eq!(a.count, 2);
        assert_eq!(digest.checks.len(), 2);
    }

    #[test]
    #[ignore = "production-scale benchmark; requires substantial time and disk"]
    fn benchmark_full_digest_with_production_retained_history() {
        let check_count = std::env::var("WARDEN_BENCH_CHECKS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(10_000);
        let observations_per_check = std::env::var("WARDEN_BENCH_OBSERVATIONS_PER_CHECK")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(30 * 24 * 60 * 2);
        let max_digest_ms = std::env::var("WARDEN_BENCH_MAX_DIGEST_MS")
            .ok()
            .and_then(|value| value.parse::<u128>().ok())
            .unwrap_or(2_000);
        let sample_count = 20usize;
        let path = std::env::temp_dir().join(format!(
            "warden-anti-entropy-bench-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let store = Storage::open(&path_string).unwrap();

        let seed_started = std::time::Instant::now();
        store
            .conn
            .execute_batch(&format!(
                "INSERT INTO check_results
                 SELECT md5('check-' || check_number::VARCHAR)::UUID,
                        'agent-' || (observation_number % 3)::VARCHAR,
                        observation_number,
                        'up', 1, 200, NULL, NULL, 'zone', 'region'
                 FROM range({check_count}) checks(check_number)
                 CROSS JOIN range({observations_per_check}) observations(observation_number);
                 INSERT INTO result_replication_watermarks
                 SELECT md5('check-' || check_number::VARCHAR)::UUID,
                        {observations_per_check} - 1,
                        {observations_per_check}
                 FROM range({check_count}) checks(check_number);
                 CHECKPOINT;"
            ))
            .unwrap();
        let seed_elapsed = seed_started.elapsed();
        let retained_rows = check_count * observations_per_check;
        let database_bytes = std::fs::metadata(&path).unwrap().len();

        let mut samples = Vec::with_capacity(sample_count);
        for _ in 0..sample_count {
            let started = std::time::Instant::now();
            let digest = store.digest(0).unwrap();
            samples.push(started.elapsed());
            assert_eq!(digest.checks.len() as u64, check_count);
        }
        samples.sort_unstable();
        let p50 = samples[sample_count / 2];
        let p95 = samples[(sample_count * 95 / 100).min(sample_count - 1)];
        let max = samples[sample_count - 1];

        println!(
            "checks={check_count} retained_rows={retained_rows} database_bytes={database_bytes} seed_ms={} digest_p50_ms={} digest_p95_ms={} digest_max_ms={}",
            seed_elapsed.as_millis(),
            p50.as_millis(),
            p95.as_millis(),
            max.as_millis()
        );
        assert!(
            p95.as_millis() <= max_digest_ms,
            "full digest p95 {p95:?} exceeded {max_digest_ms}ms with {retained_rows} retained rows"
        );

        drop(store);
        std::fs::remove_file(path).unwrap();
    }

    /// Retention must cost what it *removes*, not what it keeps. Seeds a table
    /// spanning two windows, prunes the older one, and reports the delete
    /// beside the full rebuilds it replaced — the rebuilds are timed here only
    /// to keep the comparison honest, they are no longer on the path.
    ///
    /// `WARDEN_BENCH_PRUNE_ROWS` (default 20,000,000) sets the retained rows.
    #[test]
    fn latency_rollups_preserve_domains_dedup_and_raw_handoff() {
        let s = Storage::open(":memory:").unwrap();
        let now = 100 * 24 * MILLIS_PER_HOUR;
        let to = now;
        let handoff = to - RAW_RETENTION_MILLIS;
        let minute = handoff - MILLIS_PER_MINUTE;
        let mut a = result("latency", minute + 10_000, 10);
        a.agent_id = "domain-a".into();
        let mut b = result("latency", minute + 20_000, 30);
        b.agent_id = "domain-b".into();
        let mut raw = result("latency", handoff + 1, 50);
        raw.agent_id = "domain-a".into();
        s.insert_results_dedup(&[(a.clone(), Status::Up), (b, Status::Up), (raw, Status::Up)])
            .unwrap();
        assert_eq!(s.insert_results_dedup(&[(a, Status::Up)]).unwrap(), 0);
        s.prune_results(handoff).unwrap();

        let series = s
            .latency_series(
                cid("latency"),
                minute,
                to,
                (to - minute) as usize / MILLIS_PER_MINUTE as usize,
            )
            .unwrap();
        assert_eq!(
            series.points[0].attempts, 2,
            "both domains survive in minute rollup"
        );
        assert_eq!(series.points[0].avg_latency_ms, Some(20.0));
        let raw_point = &series.points[1];
        assert_eq!(raw_point.attempts, 1, "raw starts strictly after handoff");
        assert_eq!(raw_point.avg_latency_ms, Some(50.0));
        assert_eq!(
            series.overall.attempts, 3,
            "replay must not inflate rollups"
        );
    }

    #[test]
    fn latency_rollup_retention_and_restart_keep_long_range_queries() {
        let path =
            std::env::temp_dir().join(format!("warden-rollups-{}.duckdb", uuid::Uuid::now_v7()));
        let now = 500 * 24 * MILLIS_PER_HOUR;
        // Must be more than 90 days old (minute tier) but still inside the
        // 13-month hour tier selected relative to this query's end.
        let old_hour = now - 300 * 24 * MILLIS_PER_HOUR;
        let expired_minute = now - MINUTE_ROLLUP_RETENTION_MILLIS - MILLIS_PER_MINUTE;
        let expired_hour = now - HOUR_ROLLUP_RETENTION_MILLIS - MILLIS_PER_HOUR;
        {
            let s = Storage::open(path.to_str().unwrap()).unwrap();
            s.insert_results(&[
                (result("latency", old_hour + 1, 40), Status::Up),
                (result("latency", expired_minute + 1, 60), Status::Up),
                (result("latency", expired_hour + 1, 80), Status::Up),
            ])
            .unwrap();
            s.prune(30, 90, now).unwrap();
        }
        let s = Storage::open(path.to_str().unwrap()).unwrap();
        let rollup_count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM check_latency_hour_rollups WHERE check_id = ?",
                duckdb::params![cid("latency")],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rollup_count, 2);
        let series = s
            .latency_series(
                cid("latency"),
                old_hour - MILLIS_PER_HOUR,
                now,
                ((now - old_hour) / MILLIS_PER_HOUR + 1) as usize,
            )
            .unwrap();
        assert_eq!(
            series.points[1].avg_latency_ms,
            Some(40.0),
            "hour rollup survives restart"
        );
        let minute_count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM check_latency_minute_rollups WHERE bucket_start <= ?",
                duckdb::params![expired_minute],
                |r| r.get(0),
            )
            .unwrap();
        let hour_count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM check_latency_hour_rollups WHERE bucket_start <= ?",
                duckdb::params![expired_hour],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(minute_count, 0, "minute retention is ninety days");
        assert_eq!(hour_count, 0, "hour retention is thirteen months");
        drop(s);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    #[ignore = "capacity baseline; configure WARDEN_BENCH_DEDUP_ROWS for the intended scale"]
    fn benchmark_replicated_dedup_against_retained_history() {
        let retained: u64 = std::env::var("WARDEN_BENCH_DEDUP_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000_000);
        let path =
            std::env::temp_dir().join(format!("warden-dedup-{}.duckdb", uuid::Uuid::now_v7()));
        let store = Storage::open(path.to_str().unwrap()).unwrap();
        let check_key = "dedup-scaling-check";
        let check = cid(check_key);

        // Rows land in time order, as live ingest writes them.
        let seeded = std::time::Instant::now();
        store
            .conn
            .execute_batch(&format!(
                "INSERT INTO check_results
                 SELECT '{check}'::UUID, 'agent-a', i, 'up', 10, 200, NULL, 'z', 'r', NULL
                 FROM generate_series(1, {retained}) t(i);"
            ))
            .unwrap();
        // Seeded straight into the table, so restore the invariant every real
        // writer maintains: the benchmark measures dedup, not a corrupt store.
        store.rebuild_result_watermarks().unwrap();
        store.rebuild_latest_check_results().unwrap();
        let seed_elapsed = seeded.elapsed();

        // A live batch: 500 fresh rows, all newer than everything retained.
        let rows: Vec<(CheckResult, Status)> = (0..500)
            .map(|i| {
                let mut r = result(check_key, retained as i64 + i, 10);
                r.agent_id = "agent-b".into();
                (r, Status::Up)
            })
            .collect();

        let mut samples = Vec::new();
        for round in 0..5 {
            let rows: Vec<(CheckResult, Status)> = rows
                .iter()
                .map(|(r, s)| {
                    let mut r = r.clone();
                    r.timestamp = chrono::DateTime::from_timestamp_millis(
                        r.timestamp.timestamp_millis() + round * 1_000,
                    )
                    .unwrap();
                    (r, *s)
                })
                .collect();
            let started = std::time::Instant::now();
            let inserted = store.insert_results_dedup(&rows).unwrap();
            samples.push(started.elapsed());
            assert_eq!(inserted, 500);
        }
        samples.sort();
        let p50 = samples[samples.len() / 2];
        let max = *samples.last().unwrap();

        // A replay of the same batch must still be recognised as a replay:
        // the bound narrows the probe, it must not lose a duplicate.
        let replay = store.insert_results_dedup(&rows).unwrap();

        println!(
            "replicated_dedup retained_rows={retained} seed_ms={} batch_rows=500 p50_ms={} max_ms={} replay_inserted={replay}",
            seed_elapsed.as_millis(),
            p50.as_millis(),
            max.as_millis()
        );
        assert_eq!(replay, 0, "an exact replay must insert nothing");
        let bounded = max.as_millis() <= 500;
        drop(store);
        let _ = std::fs::remove_file(&path);
        assert!(
            bounded,
            "a 500-row batch took {max:?} against {retained} retained rows; \
             dedup is scaling with history rather than with the batch"
        );
    }

    #[test]
    #[ignore = "capacity baseline; configure WARDEN_BENCH_* for the intended scale"]
    fn benchmark_representative_storage_capacity_baseline() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn env_u128(name: &str) -> Option<u128> {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
        }
        fn percentile(samples: &mut [std::time::Duration], percentile: usize) -> u128 {
            samples.sort_unstable();
            samples[(samples.len() * percentile / 100).min(samples.len() - 1)].as_millis()
        }
        fn bytes(path: &std::path::Path) -> u64 {
            std::fs::metadata(path)
                .map(|metadata| metadata.len())
                .unwrap_or(0)
        }

        let check_count = env_u64("WARDEN_BENCH_CHECKS", 100);
        let days = env_u64("WARDEN_BENCH_DAYS", 2);
        let observation_secs = env_u64("WARDEN_BENCH_OBSERVATION_SECS", 30);
        let domains = env_u64("WARDEN_BENCH_DOMAINS", 1);
        assert!(check_count > 0 && days > 0 && observation_secs > 0 && domains > 0);
        let observations_per_check = days * 86_400 / observation_secs;
        let rows = check_count * observations_per_check * domains;
        let path = std::env::temp_dir().join(format!(
            "warden-capacity-bench-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
        let backup_path = path.with_extension("backup.duckdb");
        let path_string = path.to_string_lossy().into_owned();
        let store = Storage::open(&path_string).unwrap();
        let now = days as i64 * 86_400_000;
        let step_ms = observation_secs as i64 * 1_000;

        let seed_started = std::time::Instant::now();
        store
            .conn
            .execute_batch(&format!(
                "INSERT INTO check_results
                 SELECT md5('check-' || check_number::VARCHAR)::UUID,
                        'agent-' || domain_number::VARCHAR || '-' || (check_number % 3)::VARCHAR,
                        observation_number * {step_ms} + domain_number,
                        CASE WHEN (observation_number + check_number * 17 + domain_number * 31) % 97 = 0 THEN 'down' ELSE 'up' END,
                        15 + ((observation_number * 37 + check_number * 13 + domain_number * 19) % 2_000),
                        CASE WHEN (observation_number + check_number * 17 + domain_number * 31) % 97 = 0 THEN 503 ELSE 200 + ((check_number + domain_number) % 5) END,
                        CASE WHEN (observation_number + check_number * 17 + domain_number * 31) % 97 = 0 THEN 'timeout-' || check_number::VARCHAR || '-' || observation_number::VARCHAR ELSE NULL END,
                        CASE WHEN (observation_number + check_number * 17 + domain_number * 31) % 97 = 0 THEN '\"timeout\"' ELSE NULL END,
                        'zone-' || domain_number::VARCHAR,
                        'region-' || domain_number::VARCHAR
                 FROM range({check_count}) checks(check_number)
                 CROSS JOIN range({observations_per_check}) observations(observation_number)
                 CROSS JOIN range({domains}) domain(domain_number);"
            ))
            .unwrap();
        store.rebuild_result_watermarks().unwrap();
        store.rebuild_latest_check_results().unwrap();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let seed_ms = seed_started.elapsed().as_millis();
        assert_eq!(store.count_results().unwrap(), rows);
        let database_before_prune = bytes(&path);
        let wal_before_prune = bytes(&wal_path);

        drop(store);
        let backup_started = std::time::Instant::now();
        std::fs::copy(&path, &backup_path).unwrap();
        let backup_ms = backup_started.elapsed().as_millis();
        let backup_bytes = bytes(&backup_path);
        let restore_started = std::time::Instant::now();
        let restored = Storage::open(backup_path.to_str().unwrap()).unwrap();
        assert_eq!(restored.count_results().unwrap(), rows);
        let restore_ms = restore_started.elapsed().as_millis();
        drop(restored);
        let store = Storage::open(&path_string).unwrap();

        let check_id: CheckId = store
            .conn
            .query_row("SELECT check_id FROM check_results LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let mut history_samples = Vec::new();
        let mut latency_samples = Vec::new();
        for _ in 0..10 {
            let started = std::time::Instant::now();
            let history = store
                .history(check_id, 100, Some(0), Some(now), None)
                .unwrap();
            history_samples.push(started.elapsed());
            assert_eq!(history.len(), 100);
            let started = std::time::Instant::now();
            let series = store.latency_series(check_id, 0, now, 120).unwrap();
            latency_samples.push(started.elapsed());
            assert!(series.overall.attempts > 0);
        }

        let prune_started = std::time::Instant::now();
        let pruned = store.prune((days - 1).max(1), 90, now).unwrap();
        let prune_ms = prune_started.elapsed().as_millis();
        assert!(pruned > 0, "benchmark must exercise raw retention");
        // Measured before any CHECKPOINT runs, so this answers the actual
        // question behind TODO's "add checkpoint maintenance if benchmarks
        // show it is needed": does a delete alone ever shrink the file, or is
        // an explicit checkpoint the only thing that can?
        let database_after_prune_no_checkpoint = bytes(&path);
        let wal_after_prune_no_checkpoint = bytes(&wal_path);
        let checkpoint_started = std::time::Instant::now();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let checkpoint_ms = checkpoint_started.elapsed().as_millis();
        let database_after_prune = bytes(&path);
        let wal_after_prune = bytes(&wal_path);

        let history_p95_ms = percentile(&mut history_samples, 95);
        let latency_p95_ms = percentile(&mut latency_samples, 95);
        println!(
            "checks={check_count} domains={domains} retained_rows={rows} observation_secs={observation_secs} seed_ms={seed_ms} database_before_prune_bytes={database_before_prune} wal_before_prune_bytes={wal_before_prune} backup_bytes={backup_bytes} backup_ms={backup_ms} restore_ms={restore_ms} history_p95_ms={history_p95_ms} latency_p95_ms={latency_p95_ms} pruned_rows={pruned} prune_ms={prune_ms} database_after_prune_no_checkpoint_bytes={database_after_prune_no_checkpoint} wal_after_prune_no_checkpoint_bytes={wal_after_prune_no_checkpoint} checkpoint_ms={checkpoint_ms} database_after_prune_bytes={database_after_prune} wal_after_prune_bytes={wal_after_prune}"
        );

        for (name, value) in [
            ("WARDEN_BENCH_MAX_HISTORY_P95_MS", history_p95_ms),
            ("WARDEN_BENCH_MAX_LATENCY_P95_MS", latency_p95_ms),
            ("WARDEN_BENCH_MAX_PRUNE_MS", prune_ms),
            ("WARDEN_BENCH_MAX_CHECKPOINT_MS", checkpoint_ms),
            ("WARDEN_BENCH_MAX_BACKUP_MS", backup_ms),
            ("WARDEN_BENCH_MAX_RESTORE_MS", restore_ms),
            (
                "WARDEN_BENCH_MAX_DATABASE_BYTES",
                database_before_prune as u128,
            ),
        ] {
            if let Some(limit) = env_u128(name) {
                assert!(value <= limit, "{name}: measured {value}, limit {limit}");
            }
        }

        drop(store);
        let _ = std::fs::remove_file(wal_path);
        std::fs::remove_file(backup_path).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    /// TODO.md: "add explicit checkpoint/file-size maintenance after large
    /// pruning operations if benchmarks show it is needed." It is not
    /// needed, and this benchmark is why.
    ///
    /// Two things have to both be true for an explicit `CHECKPOINT` after
    /// `prune_in_slices` to earn its cost: DuckDB has to leave meaningful
    /// work undone until asked, and doing that work has to shrink the file.
    /// Neither holds. `wal_autocheckpoint` is pinned high here specifically
    /// to *stop* DuckDB self-checkpointing mid-prune, so the work an explicit
    /// call would do is visible in isolation rather than already applied —
    /// with the pragma left at its default, a prune this size crosses
    /// DuckDB's own threshold before `prune_results` even returns, and a
    /// caller-issued CHECKPOINT afterward finds nothing left to flush. Run
    /// with `WARDEN_BENCH_CHECKS`/`WARDEN_BENCH_DAYS`/
    /// `WARDEN_BENCH_OBSERVATION_SECS` for other scales; the numbers recorded
    /// in TODO.md/SCALING.md are the defaults below.
    #[test]
    #[ignore = "capacity baseline; configure WARDEN_BENCH_* for the intended scale"]
    fn benchmark_checkpoint_after_prune() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn bytes(path: &std::path::Path) -> u64 {
            std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
        }

        let check_count = env_u64("WARDEN_BENCH_CHECKS", 1_000);
        let days = env_u64("WARDEN_BENCH_DAYS", 30);
        let observation_secs = env_u64("WARDEN_BENCH_OBSERVATION_SECS", 30);
        let observations_per_check = days * 86_400 / observation_secs;
        let step_ms = observation_secs as i64 * 1_000;
        let path = std::env::temp_dir().join(format!(
            "warden-checkpoint-bench-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
        let store = Storage::open(path.to_str().unwrap()).unwrap();

        store
            .conn
            .execute_batch(&format!(
                "INSERT INTO check_results
                 SELECT md5('check-' || check_number::VARCHAR)::UUID,
                        'agent-' || (check_number % 3)::VARCHAR,
                        observation_number * {step_ms},
                        'up', 15, 200, NULL, 'zone', 'region', NULL
                 FROM range({check_count}) checks(check_number)
                 CROSS JOIN range({observations_per_check}) observations(observation_number);"
            ))
            .unwrap();
        store.rebuild_result_watermarks().unwrap();
        store.rebuild_latest_check_results().unwrap();
        // Real ingest maintains rollups incrementally with every write; this
        // bulk seed does not, so backfill once here rather than have
        // `prune_results` hold the horizon back for a gap that only exists
        // because of how the benchmark seeds data.
        store.rebuild_latency_rollups().unwrap();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let seeded_bytes = bytes(&path);

        // Hold DuckDB's own automatic checkpoint off so the prune's WAL
        // growth and an explicit CHECKPOINT's cost are each visible on their
        // own, instead of the two being indistinguishable the way they are
        // with the default threshold (see the doc comment above).
        store
            .conn
            .execute_batch("PRAGMA wal_autocheckpoint='1TB';")
            .unwrap();

        let cutoff = step_ms * (observations_per_check as i64 / 30);
        let prune_started = std::time::Instant::now();
        let removed = store.prune_results(cutoff).unwrap();
        let prune_ms = prune_started.elapsed().as_millis();
        assert!(removed > 0, "benchmark must exercise raw retention");
        let database_after_prune_bytes = bytes(&path);
        let wal_after_prune_bytes = bytes(&wal_path);

        let checkpoint_started = std::time::Instant::now();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let checkpoint_ms = checkpoint_started.elapsed().as_millis();
        let database_after_checkpoint_bytes = bytes(&path);

        // A second CHECKPOINT with nothing pending: if the file were ever
        // going to shrink from reclaiming the deleted rows' space, one of
        // these two calls would have done it.
        let second_checkpoint_started = std::time::Instant::now();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let second_checkpoint_ms = second_checkpoint_started.elapsed().as_millis();
        let database_after_second_checkpoint_bytes = bytes(&path);

        println!(
            "checks={check_count} days={days} observation_secs={observation_secs} removed_rows={removed} seeded_bytes={seeded_bytes} prune_ms={prune_ms} database_after_prune_bytes={database_after_prune_bytes} wal_after_prune_bytes={wal_after_prune_bytes} checkpoint_ms={checkpoint_ms} database_after_checkpoint_bytes={database_after_checkpoint_bytes} second_checkpoint_ms={second_checkpoint_ms} database_after_second_checkpoint_bytes={database_after_second_checkpoint_bytes}"
        );

        // The file must never shrink by itself, or this benchmark's own
        // premise — that CHECKPOINT does not reclaim space — is wrong and
        // needs to be re-examined before touching the prune path.
        assert!(
            database_after_checkpoint_bytes >= database_after_prune_bytes,
            "a checkpoint should never shrink the file if this decision is right"
        );
        assert_eq!(
            database_after_second_checkpoint_bytes, database_after_checkpoint_bytes,
            "a checkpoint with nothing pending must be a byte-for-byte no-op"
        );

        drop(store);
        let _ = std::fs::remove_file(&wal_path);
        let _ = std::fs::remove_file(&path);
    }

    /// TODO.md: "add restart/recovery tests using a persistent
    /// production-sized database." Production-scale sibling of
    /// `restart_recovers_committed_rows_and_backup_restore_preserves_them`
    /// above; that test is the fast, always-on regression cover for the same
    /// three invariants — (a) recovery duration, (b) no data loss of
    /// committed rows, (c) a backup/restore round trip preserving row
    /// counts, watermarks, and rollups — at a scale CI cannot afford to run
    /// on every commit. Configure `WARDEN_BENCH_CHECKS`/`WARDEN_BENCH_DAYS`/
    /// `WARDEN_BENCH_OBSERVATION_SECS` for the intended scale; see that test's
    /// doc comment for what "closed uncleanly" does and does not simulate
    /// here.
    #[test]
    #[ignore = "capacity baseline; configure WARDEN_BENCH_* for the intended scale"]
    fn benchmark_restart_recovery_at_production_scale() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn env_u128(name: &str) -> Option<u128> {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
        }

        let check_count = env_u64("WARDEN_BENCH_CHECKS", 1_000);
        let days = env_u64("WARDEN_BENCH_DAYS", 30);
        let observation_secs = env_u64("WARDEN_BENCH_OBSERVATION_SECS", 30);
        let observations_per_check = days * 86_400 / observation_secs;
        let step_ms = observation_secs as i64 * 1_000;
        let expected_rows = check_count * observations_per_check;
        let path = std::env::temp_dir().join(format!(
            "warden-restart-bench-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path_string = path.to_string_lossy().into_owned();

        {
            let store = Storage::open(&path_string).unwrap();
            store
                .conn
                .execute_batch(&format!(
                    "INSERT INTO check_results
                     SELECT md5('check-' || check_number::VARCHAR)::UUID,
                            'agent-' || (check_number % 3)::VARCHAR,
                            observation_number * {step_ms},
                            'up', 15, 200, NULL, 'zone', 'region', NULL
                     FROM range({check_count}) checks(check_number)
                     CROSS JOIN range({observations_per_check}) observations(observation_number);"
                ))
                .unwrap();
            store.rebuild_result_watermarks().unwrap();
            store.rebuild_latest_check_results().unwrap();
            store.rebuild_latency_rollups().unwrap();
            // No checkpoint, no clean drop: every one of these rows has to be
            // recovered from the WAL, the way a killed process would leave it.
            std::mem::forget(store);
        }

        let recovery_started = std::time::Instant::now();
        let store = Storage::open(&path_string).unwrap();
        let recovery_ms = recovery_started.elapsed().as_millis();
        let recovered_rows = store.count_results().unwrap();
        assert_eq!(
            recovered_rows, expected_rows,
            "committed rows must not be lost across an unclean close"
        );
        let watermarks = store.digest(0).unwrap();
        let minute_rollups = store.rollup_digest("check_latency_minute_rollups").unwrap();
        let hour_rollups = store.rollup_digest("check_latency_hour_rollups").unwrap();

        let backup_path = path.with_extension("backup.duckdb");
        let restore_path = path.with_extension("restore.duckdb");
        let backup_started = std::time::Instant::now();
        crate::coordinator::backup::create(&path_string, &backup_path).unwrap();
        let backup_ms = backup_started.elapsed().as_millis();
        let restore_started = std::time::Instant::now();
        let report = crate::coordinator::backup::restore(&backup_path, &restore_path).unwrap();
        let restore_ms = restore_started.elapsed().as_millis();
        assert!(report.checksum_verified);
        assert_eq!(report.raw_results, expected_rows);

        let restored = Storage::open(restore_path.to_str().unwrap()).unwrap();
        assert_eq!(restored.count_results().unwrap(), expected_rows);
        let restored_watermarks = restored.digest(0).unwrap();
        assert_eq!(restored_watermarks.checks.len(), watermarks.checks.len());
        assert_eq!(
            restored_watermarks
                .checks
                .iter()
                .map(|c| c.count)
                .sum::<i64>(),
            watermarks.checks.iter().map(|c| c.count).sum::<i64>(),
            "backup/restore must preserve replication watermarks exactly"
        );
        assert_eq!(
            restored
                .rollup_digest("check_latency_minute_rollups")
                .unwrap()
                .count,
            minute_rollups.count,
            "backup/restore must preserve minute rollups exactly"
        );
        assert_eq!(
            restored
                .rollup_digest("check_latency_hour_rollups")
                .unwrap()
                .count,
            hour_rollups.count,
            "backup/restore must preserve hour rollups exactly"
        );

        println!(
            "checks={check_count} days={days} observation_secs={observation_secs} rows={expected_rows} recovery_ms={recovery_ms} backup_ms={backup_ms} restore_ms={restore_ms} minute_rollups={} hour_rollups={}",
            minute_rollups.count, hour_rollups.count
        );
        for (name, value) in [
            ("WARDEN_BENCH_MAX_RECOVERY_MS", recovery_ms),
            ("WARDEN_BENCH_MAX_BACKUP_MS", backup_ms),
            ("WARDEN_BENCH_MAX_RESTORE_MS", restore_ms),
        ] {
            if let Some(limit) = env_u128(name) {
                assert!(value <= limit, "{name}: measured {value}, limit {limit}");
            }
        }

        drop(store);
        drop(restored);
        let _ = std::fs::remove_file(format!("{path_string}.wal"));
        let _ = std::fs::remove_file(&path);
        for extra in [
            &backup_path,
            &restore_path,
            &std::path::PathBuf::from(format!("{}.manifest.json", backup_path.display())),
        ] {
            let _ = std::fs::remove_file(extra);
        }
    }

    #[test]
    fn digest_reports_max_and_count_and_excludes_rows_before_window_start() {
        let s = Storage::open(":memory:").unwrap();
        let now = 100_000i64;

        s.insert_results(&[
            (result("a", now - 5000, 1), Status::Up),
            (result("a", now - 1000, 2), Status::Down),
            (result("b", now - 50_000, 3), Status::Up), // outside window
        ])
        .unwrap();

        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            now - 1000,
            None,
        )
        .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            None,
            NodeState::Up,
            now - 90_000,
            None,
        )
        .unwrap();

        s.create_incident(
            "inc-1",
            "t",
            IncidentState::Detected,
            now - 2000,
            false,
            Some(cid("a")),
            &[IncidentCheckRow {
                check_id: cid("a"),
                source: AffectSource::Origin,
            }],
            None,
        )
        .unwrap();
        s.create_incident(
            "inc-2",
            "t",
            IncidentState::Detected,
            now - 90_000,
            false,
            Some(cid("b")),
            &[],
            None,
        )
        .unwrap();

        let window_start = now - 10_000;
        let d = s.digest(window_start).unwrap();

        let a_digest = d.checks.iter().find(|c| c.check_id == cid("a")).unwrap();
        assert_eq!(a_digest.count, 2);
        assert_eq!(a_digest.max_ts, now - 1000);
        assert!(
            d.checks.iter().all(|c| c.check_id != cid("b")),
            "the out-of-window check is excluded entirely"
        );

        assert_eq!(
            d.alerts.max_millis,
            now - 1000,
            "the older alert is outside the window"
        );
        assert_eq!(
            d.incidents.max_millis,
            now - 1999,
            "the older incident is outside the window"
        );
    }

    // ---- Clustering: fleet health ----

    fn poll(agent_id: &str, zone: &str, region: &str, at: i64, window_ms: i64) -> AgentPollRow {
        AgentPollRow {
            agent_id: agent_id.into(),
            zone: zone.into(),
            region: region.into(),
            at,
            window_ms,
            rtt_ms: None,
            build: None,
            kinds: None,
        }
    }

    #[test]
    fn agent_polls_are_recorded_per_agent_and_replace_on_re_poll() {
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll(&AgentPollRow {
            rtt_ms: Some(20),
            ..poll("agent-a", "z1", "r1", 1000, 90_000)
        })
        .unwrap();
        s.record_agent_poll(&AgentPollRow {
            rtt_ms: Some(15),
            ..poll("agent-a", "z1", "r1", 2000, 90_000)
        })
        .unwrap();
        s.record_agent_poll(&poll("agent-b", "z2", "r2", 1500, 30_000))
            .unwrap();

        let mut polls = s.list_agent_polls().unwrap();
        polls.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        assert_eq!(polls.len(), 2, "one row per agent, not one per poll");
        assert_eq!(polls[0].agent_id, "agent-a");
        assert_eq!(polls[0].at, 2000, "the newer poll replaced the older one");
        assert_eq!(polls[0].rtt_ms, Some(15));
        assert_eq!(polls[0].window_ms, 90_000);
        assert_eq!(polls[1].agent_id, "agent-b");
        assert_eq!(polls[1].window_ms, 30_000, "the window is per agent");
        assert_eq!(polls[1].rtt_ms, None);
    }

    #[test]
    fn an_older_agent_poll_never_overwrites_a_newer_one() {
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll(&AgentPollRow {
            rtt_ms: Some(11),
            build: Some("0.2.0".into()),
            ..poll("agent-a", "z1", "r1", 5000, 90_000)
        })
        .unwrap();
        // A peer's batch arriving late. Replication is unordered, so this is
        // the normal case rather than an edge one.
        s.record_agent_poll(&AgentPollRow {
            rtt_ms: Some(99),
            build: Some("0.1.0".into()),
            ..poll("agent-a", "z1", "r1", 1000, 90_000)
        })
        .unwrap();

        let polls = s.list_agent_polls().unwrap();
        assert_eq!(polls.len(), 1);
        assert_eq!(polls[0].at, 5000, "the stale row lost");
        assert_eq!(polls[0].rtt_ms, Some(11));
        assert_eq!(
            polls[0].build.as_deref(),
            Some("0.2.0"),
            "a stale row must not roll the reported build backwards either"
        );
    }

    #[test]
    fn a_declaration_survives_the_round_trip_unparsed() {
        // Stored as the agent sent it, so a kind this node has never heard of
        // still reaches the fleet view rather than being narrowed away to the
        // kinds this particular binary happens to know.
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll(&AgentPollRow {
            build: Some("0.3.0 (abc123, built 2026-08-31 00:00:00 UTC)".into()),
            kinds: Some("http,tcp,warp-drive".into()),
            ..poll("agent-a", "z1", "r1", 1000, 90_000)
        })
        .unwrap();

        let polls = s.list_agent_polls().unwrap();
        assert_eq!(polls[0].kinds.as_deref(), Some("http,tcp,warp-drive"));
        assert_eq!(
            polls[0].build.as_deref(),
            Some("0.3.0 (abc123, built 2026-08-31 00:00:00 UTC)")
        );
    }

    #[test]
    fn purging_an_agent_drops_its_poll_row() {
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll(&poll("agent-a", "z1", "r1", 1000, 90_000))
            .unwrap();
        s.purge_agent("agent-a").unwrap();
        assert!(
            s.list_agent_polls().unwrap().is_empty(),
            "a purged agent that keeps its poll row walks back onto the status page"
        );
    }

    // ---- Incident deletion ----

    /// The incident a delete test needs: one row, one update, one affects
    /// entry and one alert stamped with its id.
    fn seeded_incident(s: &Storage, id: &str, check: &str) {
        s.create_incident(
            id,
            "Outage",
            IncidentState::Detected,
            1000,
            false,
            Some(cid(check)),
            &[IncidentCheckRow {
                check_id: cid(check),
                source: AffectSource::Origin,
            }],
            Some(("down", None)),
        )
        .unwrap();
        s.upsert_alert(&AlertRow {
            id: format!("{id}-a1"),
            scope: AlertScope::Check,
            node_id: check.into(),
            previous_state: Some(NodeState::Up),
            current_state: NodeState::Down,
            timestamp: 1000,
            incident_id: Some(id.into()),
        })
        .unwrap();
    }

    #[test]
    fn purging_short_incidents_removes_their_stamped_alerts() {
        let s = Storage::open(":memory:").unwrap();
        seeded_incident(&s, "short", "web");
        s.add_update("short", IncidentState::Resolved, "recovered", None, 1_500)
            .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "api",
            Some(NodeState::Up),
            NodeState::Down,
            2_000,
            None,
        )
        .unwrap();

        assert_eq!(s.purge_short_incidents(1).unwrap(), 5);
        assert!(s.incident("short").unwrap().is_none());
        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
    }

    #[test]
    fn deleting_an_incident_removes_it_and_unlinks_its_alerts() {
        let s = Storage::open(":memory:").unwrap();
        seeded_incident(&s, "inc-1", "web");

        assert!(s.delete_incident("inc-1", 2000, "node-a").unwrap());
        assert!(s.incident("inc-1").unwrap().is_none());
        assert!(s.updates_for_incident("inc-1").unwrap().is_empty());

        // The alert survives. A transition is a fact; the incident was only the
        // story told about it, and deleting the story must not rewrite history.
        let alerts = s.recent_alerts(10).unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].incident_id, None);

        assert!(
            !s.delete_incident("inc-1", 3000, "node-a").unwrap(),
            "a second delete reports nothing removed"
        );
    }

    #[test]
    fn a_tombstoned_incident_cannot_be_replicated_back_in() {
        let s = Storage::open(":memory:").unwrap();
        seeded_incident(&s, "inc-1", "web");
        s.delete_incident("inc-1", 2000, "node-a").unwrap();

        s.upsert_incident(&ReplicatedIncident {
            id: "inc-1".into(),
            version: Hlc::new(1000, "a"),
            title: "Outage".into(),
            state: IncidentState::Detected,
            started_at: 1000,
            resolved_at: None,
            global: false,
            origin_check: Some(cid("web")),
            checks: vec![(cid("web"), AffectSource::Origin)],
            updates: Vec::new(),
        })
        .unwrap();

        assert!(
            s.incident("inc-1").unwrap().is_none(),
            "a peer's push resurrected a deleted incident"
        );
    }

    #[test]
    fn erasing_a_check_leaves_other_checks_incidents_intact() {
        let s = Storage::open(":memory:").unwrap();
        seeded_incident(&s, "inc-web", "web");
        seeded_incident(&s, "inc-api", "api");
        // `web` merely joined the api incident, behind a dependency.
        s.attach_check("inc-api", cid("web"), AffectSource::Dependency)
            .unwrap();
        s.insert_results(&[(result("web", 1000, 5), Status::Up)])
            .unwrap();

        let summary = s
            .purge_check_everything(cid("web"), 4000, "node-a")
            .unwrap();
        assert_eq!(summary.results, 1);
        assert_eq!(summary.incidents, 1);
        assert_eq!(summary.deleted_incidents, vec!["inc-web".to_string()]);

        assert!(s.incident("inc-web").unwrap().is_none());
        let api = s.incident("inc-api").unwrap().expect("api's own incident");
        assert_eq!(
            api.checks.len(),
            1,
            "the erased check left the incident it had joined, and nothing else did"
        );
        assert_eq!(api.checks[0].check_id, cid("api"));
    }

    #[test]
    fn incident_tombstones_are_pruned_on_the_longer_window() {
        let s = Storage::open(":memory:").unwrap();
        let now = 10 * 86_400_000;
        s.delete_incident("old", now - 9 * 86_400_000, "node-a")
            .unwrap();
        s.delete_incident("recent", now - 1000, "node-a").unwrap();

        s.prune(1, 7, now).unwrap();
        let left = s.list_incident_tombstones().unwrap();
        assert_eq!(
            left,
            vec!["recent".to_string()],
            "tombstones age out on max(results_days, alerts_days), not the shorter one"
        );
    }

    // ---- Clustering: wire form ----

    #[test]
    fn config_entry_wire_form_is_stable() {
        let e = ConfigEntry {
            kind: ConfigKind::Check,
            id: "web".into(),
            hlc: Hlc::new(42, "n1"),
            document: Some("{}".into()),
        };
        assert_eq!(
            serde_json::to_string(&e).unwrap(),
            r#"{"kind":"check","id":"web","hlc":{"millis":42,"node_id":"n1"},"document":"{}"}"#
        );

        let tomb = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(1, "a"),
            document: None,
        };
        assert_eq!(
            serde_json::to_string(&tomb).unwrap(),
            r#"{"kind":"setting","id":"k","hlc":{"millis":1,"node_id":"a"},"document":null}"#
        );
    }

    #[test]
    fn replicated_incident_wire_form_is_stable() {
        let web = cid("web");
        let i = ReplicatedIncident {
            id: "inc-1".into(),
            version: Hlc::new(10, "a"),
            title: "Outage".into(),
            state: IncidentState::Investigating,
            started_at: 10,
            resolved_at: None,
            global: false,
            origin_check: Some(web),
            checks: vec![(web, AffectSource::Origin)],
            updates: vec![IncidentUpdateRow {
                id: "inc-1-u10".into(),
                incident_id: "inc-1".into(),
                state: IncidentState::Detected,
                body: "down".into(),
                author: None,
                created_at: 10,
            }],
        };
        assert_eq!(
            serde_json::to_string(&i).unwrap(),
            format!(
                r#"{{"id":"inc-1","version":{{"millis":10,"node_id":"a"}},"title":"Outage","state":"investigating","started_at":10,"resolved_at":null,"global":false,"origin_check":"{web}","checks":[["{web}","origin"]],"updates":[{{"id":"inc-1-u10","incident_id":"inc-1","state":"detected","body":"down","author":null,"created_at":10}}]}}"#
            )
        );
    }

    #[test]
    fn section_display_mode_persists_across_writes() {
        let store = Storage::open(":memory:").unwrap();
        store
            .create_section("edge", false, SectionDisplay::Page)
            .unwrap();
        store
            .create_section("core", true, SectionDisplay::Inline)
            .unwrap();

        let mode = |store: &Storage, name: &str| {
            store
                .list_sections()
                .unwrap()
                .into_iter()
                .find(|s| s.name == name)
                .map(|s| s.display)
        };
        assert_eq!(mode(&store, "edge"), Some(SectionDisplay::Page));
        assert_eq!(mode(&store, "core"), Some(SectionDisplay::Inline));

        store
            .set_section_display("core", SectionDisplay::Page)
            .unwrap();
        assert_eq!(mode(&store, "core"), Some(SectionDisplay::Page));

        // A rename carries the mode with the section. Losing it here would flip
        // a paged section back to inline as a side effect of an edit that was
        // only ever about its name, re-expanding the thing the operator paged
        // precisely because it was too big to render.
        store.rename_section("core", "core-eu", 0).unwrap();
        assert_eq!(mode(&store, "core-eu"), Some(SectionDisplay::Page));

        // `collapsed` and `display` are independent: a paged section's stored
        // `collapsed` is meaningless but must not be destroyed, so toggling one
        // never rewrites the other.
        store.set_section_collapsed("core-eu", false).unwrap();
        assert_eq!(mode(&store, "core-eu"), Some(SectionDisplay::Page));
    }

    #[test]
    fn document_encodings_are_pinned() {
        let section = Section {
            name: "core".into(),
            position: 2,
            collapsed: true,
            display: SectionDisplay::Page,
        };
        assert_eq!(
            serde_json::to_string(&section).unwrap(),
            r#"{"name":"core","position":2,"collapsed":true,"display":"page"}"#
        );
        // The compatibility that matters in both directions. A document from a
        // peer that predates the mode has no `display` and must still parse,
        // reading as inline; a peer that predates it parses a document that has
        // one by ignoring the field, which is why it was added rather than
        // replacing `collapsed`.
        let legacy: Section =
            serde_json::from_str(r#"{"name":"core","position":2,"collapsed":true}"#).unwrap();
        assert_eq!(legacy.display, SectionDisplay::Inline);

        let value = "dark".to_string();
        assert_eq!(serde_json::to_string(&value).unwrap(), r#""dark""#);

        // The check document is `CheckConfig` verbatim — `checks.config` is
        // already authoritative — so the contract pinned here is the round
        // trip, not a hand-copied field list config.rs already owns.
        let check = check_config("web", "https://example.com");
        let doc = serde_json::to_string(&check).unwrap();
        let round: CheckConfig = serde_json::from_str(&doc).unwrap();
        assert_eq!(round, check);
    }

    #[test]
    fn credential_doc_round_trip_preserves_every_field() {
        let cred = AgentCredential {
            id: "cred-1".into(),
            zone: "z1".into(),
            region: "r1".into(),
            secret_hash: "hash".into(),
            salt: "salt".into(),
            created_at: 111,
            revoked_at: Some(222),
        };
        let doc = CredentialDoc::from(&cred);
        let json = serde_json::to_string(&doc).unwrap();
        assert_eq!(
            json,
            r#"{"id":"cred-1","zone":"z1","region":"r1","secret_hash":"hash","salt":"salt","created_at":111,"revoked_at":222}"#
        );
        let back: CredentialDoc = serde_json::from_str(&json).unwrap();
        // Whole-value comparison, not field by field: `From<&AgentCredential>`
        // destructures exhaustively, so a field added to `AgentCredential`
        // without a matching one here fails to compile rather than silently
        // dropping off the wire.
        assert_eq!(doc, back);
    }
}

#[cfg(test)]
mod segment_tests {
    use super::tests::cid;
    use super::*;
    use crate::models::CheckKind;
    use chrono::DateTime;

    fn up_at(check_id: &str, ts_millis: i64) -> (CheckResult, Status) {
        (
            CheckResult {
                zone: "test-zone".into(),
                region: "test-region".into(),
                body: None,
                headers: None,
                check_id: cid(check_id),
                agent_id: "agent".into(),
                kind: CheckKind::Http,
                timestamp: DateTime::from_timestamp_millis(ts_millis).unwrap(),
                latency_ms: 10,
                status_code: Some(200),
                body_matched: None,
                error: None,
                failure: None,
                metric: None,
                metrics: None,
            },
            Status::Up,
        )
    }

    #[test]
    fn segment_count_is_constant_and_width_follows_the_window() {
        // The strip keeps the same number of bars at every range; a shorter
        // window just means finer segments. 7 days over 90 segments is ~1h52m.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        const DAY: i64 = 86_400_000;

        for days in [7usize, 30, 90] {
            let b = s.uptime_buckets(cid("x"), days, 90, now, false).unwrap();
            assert_eq!(b.len(), 90, "{days}d must still yield 90 segments");

            let width = b[0].end - b[0].start;
            assert_eq!(
                width,
                days as i64 * DAY / 90,
                "{days}d segment width is the window split 90 ways"
            );
            // Segments tile with no gaps and the last one ends at `now`.
            assert_eq!(b[1].start, b[0].end, "segments are contiguous");
            assert_eq!(b[89].end, now, "the newest segment ends at now");
        }
    }

    #[test]
    fn raw_results_create_counts_but_not_uptime_data() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        s.insert_results(&[up_at("x", now - 1)]).unwrap();

        let buckets = s.uptime_buckets(cid("x"), 7, 90, now, false).unwrap();
        assert!(buckets.iter().all(|bucket| bucket.uptime.is_none()));
        assert_eq!(buckets.iter().map(|bucket| bucket.total).sum::<u64>(), 1);
        assert_eq!(buckets.iter().map(|bucket| bucket.up).sum::<u64>(), 1);
    }
}

#[cfg(test)]
mod migration_tests {
    use super::tests::cid;
    use super::*;

    /// Helper: open an incident the way the coordinator does, with the check as
    /// its origin and an opening update.
    fn open_incident(s: &Storage, check: &str, at: i64) -> IncidentRow {
        s.create_incident(
            &format!("{check}-{at}"),
            &format!("{check} is down"),
            IncidentState::Detected,
            at,
            false,
            Some(cid(check)),
            &[IncidentCheckRow {
                check_id: cid(check),
                source: AffectSource::Origin,
            }],
            Some(("connection refused", None)),
        )
        .unwrap()
    }

    #[test]
    fn agent_leaf_topology_is_persisted_monotonically() {
        let s = Storage::open(":memory:").unwrap();
        s.update_agent_check_state(
            cid("svc"),
            "agent-a",
            "zone-a",
            "region-a",
            Status::Up,
            0,
            2_000,
        )
        .unwrap();
        // A delayed report must not rewind either status/timestamp or topology.
        s.update_agent_check_state(
            cid("svc"),
            "agent-a",
            "old-zone",
            "old-region",
            Status::Down,
            3,
            1_000,
        )
        .unwrap();

        assert_eq!(
            s.list_agent_check_state().unwrap(),
            vec![AgentCheckStateRow {
                check_id: cid("svc"),
                agent_id: "agent-a".into(),
                zone: "zone-a".into(),
                region: "region-a".into(),
                status: Status::Up,
                consecutive_failures: 0,
                last_check: 2_000,
            }]
        );
    }

    #[test]
    fn incident_lifecycle_open_and_resolve() {
        let s = Storage::open(":memory:").unwrap();

        assert_eq!(s.open_incident_for(cid("svc")).unwrap(), None);

        let opened = open_incident(&s, "svc", 1_000);
        assert_eq!(opened.state, IncidentState::Detected);
        assert_eq!(opened.resolved_at, None);
        assert_eq!(opened.id, "svc-1000");
        // The cause lives in the narrative, not in a column beside it.
        let updates = s.updates_for_incident("svc-1000").unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].body, "connection refused");
        assert!(updates[0].author.is_none());

        // The check is attached, so a lookup by check finds the open incident.
        assert_eq!(
            s.open_incident_for(cid("svc")).unwrap().map(|i| i.id),
            Some("svc-1000".to_string())
        );

        // Resolving is an update like any other, and it records when.
        s.add_update("svc-1000", IncidentState::Resolved, "back", None, 2_000)
            .unwrap();
        let resolved = s.incident("svc-1000").unwrap().unwrap();
        assert_eq!(resolved.state, IncidentState::Resolved);
        assert_eq!(resolved.resolved_at, Some(2_000));
        assert_eq!(
            s.open_incident_for(cid("svc")).unwrap(),
            None,
            "a resolved incident is not open"
        );

        // After resolution a new outage opens a fresh incident.
        let second = open_incident(&s, "svc", 3_000);
        assert_eq!(second.id, "svc-3000");
    }

    /// Reopening: an operator posting a non-resolved update on a closed
    /// incident must clear `resolved_at`, or it would read as open and finished
    /// at the same time.
    #[test]
    fn local_incident_updates_in_one_millisecond_are_not_lost() {
        let s = Storage::open(":memory:").unwrap();
        s.create_incident(
            "inc-1",
            "Outage",
            IncidentState::Detected,
            1_000,
            false,
            None,
            &[],
            None,
        )
        .unwrap();
        let first = s
            .add_update(
                "inc-1",
                IncidentState::Investigating,
                "investigating",
                None,
                2_000,
            )
            .unwrap();
        let second = s
            .add_update(
                "inc-1",
                IncidentState::Identified,
                "identified",
                Some("admin"),
                2_000,
            )
            .unwrap();

        assert_ne!(
            first.id, second.id,
            "distinct local updates need distinct replication ids"
        );
        let updates = s.updates_for_incident("inc-1").unwrap();
        assert_eq!(updates.len(), 2, "neither same-ms update may be lost");
        assert_eq!(
            updates
                .iter()
                .map(|u| u.id.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn reopening_an_incident_clears_its_resolution_time() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "svc", 1_000);
        s.add_update("svc-1000", IncidentState::Resolved, "back", None, 2_000)
            .unwrap();
        assert_eq!(
            s.incident("svc-1000").unwrap().unwrap().resolved_at,
            Some(2_000)
        );

        s.add_update(
            "svc-1000",
            IncidentState::Monitoring,
            "spoke too soon",
            Some("admin"),
            3_000,
        )
        .unwrap();
        let row = s.incident("svc-1000").unwrap().unwrap();
        assert_eq!(row.state, IncidentState::Monitoring);
        assert_eq!(
            row.resolved_at, None,
            "a reopened incident is not still finished"
        );
    }

    /// Acknowledgement is derived from the narrative, never stored. It becomes
    /// true on the first authored update whatever state that update announced,
    /// so an operator who jumps straight to `identified` still counts.
    #[test]
    fn acknowledgement_is_the_first_authored_update() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "svc", 1_000);
        assert!(
            !s.is_acknowledged("svc-1000").unwrap(),
            "the coordinator's own opening entry is not an acknowledgement"
        );

        s.add_update(
            "svc-1000",
            IncidentState::Identified,
            "bad deploy",
            Some("admin"),
            2_000,
        )
        .unwrap();
        assert!(
            s.is_acknowledged("svc-1000").unwrap(),
            "skipping investigating still counts as a human looking"
        );
    }

    /// `source` is what lets reconciliation retract its own attachments without
    /// disturbing an operator's, and an origin is never reclassified.
    #[test]
    fn attachments_record_how_each_check_got_there() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "db", 1_000);
        s.attach_check("db-1000", cid("api"), AffectSource::Dependency)
            .unwrap();
        s.attach_check("db-1000", cid("web"), AffectSource::Manual)
            .unwrap();

        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .map(|c| (c.check_id, c.source))
                .collect::<Vec<_>>(),
            vec![
                (cid("api"), AffectSource::Dependency),
                (cid("db"), AffectSource::Origin),
                (cid("web"), AffectSource::Manual),
            ]
        );

        // Only the origin and the dependency gate auto-resolve.
        assert_eq!(
            row.gating_checks().collect::<Vec<_>>(),
            vec![cid("api"), cid("db")]
        );

        // Re-attaching the origin as something else must not demote it.
        s.attach_check("db-1000", cid("db"), AffectSource::Manual)
            .unwrap();
        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .find(|c| c.check_id == cid("db"))
                .map(|c| c.source),
            Some(AffectSource::Origin),
            "the cause of the incident cannot be reclassified"
        );

        // Replacing the manual set leaves origin and dependency alone.
        s.set_manual_checks("db-1000", &[cid("cdn")]).unwrap();
        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .map(|c| (c.check_id, c.source))
                .collect::<Vec<_>>(),
            vec![
                (cid("api"), AffectSource::Dependency),
                (cid("cdn"), AffectSource::Manual),
                (cid("db"), AffectSource::Origin),
            ],
            "only the manual attachment was replaced"
        );
    }

    #[test]
    fn incidents_are_listed_newest_first_and_filterable() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "a", 1_000);
        open_incident(&s, "b", 2_000);
        s.add_update("a-1000", IncidentState::Resolved, "back", None, 3_500)
            .unwrap();

        let all = s.list_incidents(100, None, None).unwrap();
        let order: Vec<String> = all.iter().map(|i| i.id.clone()).collect();
        assert_eq!(order, vec!["b-2000", "a-1000"]);

        let open = s.list_incidents(100, Some(true), None).unwrap();
        assert_eq!(
            open.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["b-2000"],
            "only b is still open"
        );

        let resolved = s.list_incidents(100, Some(false), None).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].id, "a-1000");
        assert_eq!(
            s.list_incidents(100, None, Some(3_501))
                .unwrap()
                .iter()
                .map(|i| i.id.as_str())
                .collect::<Vec<_>>(),
            vec!["b-2000"],
            "old resolved incidents are hidden, even when an old outage is still open"
        );
        assert_eq!(s.list_incidents(100, None, Some(3_500)).unwrap().len(), 2);

        let for_a = s.incidents_for_check(cid("a"), 100).unwrap();
        assert_eq!(
            for_a.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["a-1000"]
        );

        // A check attached as a dependency is found by the same lookup: the
        // blast radius is what "this check's incidents" means, not the origin.
        s.attach_check("b-2000", cid("a"), AffectSource::Dependency)
            .unwrap();
        let for_a = s.incidents_for_check(cid("a"), 100).unwrap();
        assert_eq!(
            for_a.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["b-2000", "a-1000"]
        );
    }
}

#[cfg(test)]
mod rollup_replication_tests {
    use super::tests::cid;
    use super::*;

    fn row(
        check_id: &str,
        agent_id: &str,
        bucket_start: i64,
        attempts: i64,
        up: i64,
        latency_sum: i64,
    ) -> RollupRow {
        RollupRow {
            check_id: cid(check_id),
            agent_id: agent_id.into(),
            bucket_start,
            attempts,
            up,
            latency_sum,
            latency_min: Some(10),
            latency_max: Some(10),
        }
    }

    /// The scenario item A's merge is built for: a page of rollup rows this
    /// node never derived from its own raw observations, applied for the
    /// first time.
    #[test]
    fn a_backfilled_rollup_page_is_applied_on_first_receipt() {
        let s = Storage::open(":memory:").unwrap();
        let page = vec![row("a", "agent-1", 60_000, 5, 4, 40)];

        s.apply_minute_rollups(&page).unwrap();

        let stored = s.minute_rollups_since(i64::MIN, None, None, 10).unwrap();
        assert_eq!(stored, page);
    }

    /// The hardest requirement on the backfill merge: a retried or replayed
    /// page must land exactly once. `rollup_merge_from_backfill_sql` uses
    /// `GREATEST` rather than addition precisely so this holds.
    #[test]
    fn applying_the_same_backfilled_rollup_page_twice_does_not_double_count() {
        let s = Storage::open(":memory:").unwrap();
        let page = vec![row("a", "agent-1", 60_000, 3, 2, 20)];

        s.apply_minute_rollups(&page).unwrap();
        s.apply_minute_rollups(&page).unwrap();

        let stored = s.minute_rollups_since(i64::MIN, None, None, 10).unwrap();
        assert_eq!(
            stored.len(),
            1,
            "a replay must merge into the same row, not add another"
        );
        assert_eq!(
            stored[0].attempts, 3,
            "a replayed page must not inflate attempts"
        );
        assert_eq!(stored[0].up, 2);
        assert_eq!(stored[0].latency_sum, 20);

        // The digest's checksum is maintained by the same statement batch and
        // must be exactly as replay-safe as the row it summarizes.
        let digest = s.digest(0).unwrap().latency_minute_rollups;
        assert_eq!(digest.count, 1);
        assert_eq!(
            digest.attempts_sum, 3,
            "the digest checksum must not double-count a replay either"
        );
        assert_eq!(digest.up_sum, 2);
        assert_eq!(digest.latency_sum_sum, 20);
    }

    /// A backfilled total only ever grows what is already held — it never
    /// erases local progress a peer has not caught up to yet.
    #[test]
    fn a_backfilled_rollup_never_shrinks_a_larger_local_total() {
        let s = Storage::open(":memory:").unwrap();
        s.apply_minute_rollups(&[row("a", "agent-1", 60_000, 10, 8, 80)])
            .unwrap();
        // A peer's page for the same bucket, smaller in every field — as if
        // it were still mid-catch-up itself.
        s.apply_minute_rollups(&[row("a", "agent-1", 60_000, 4, 4, 40)])
            .unwrap();

        let stored = s.minute_rollups_since(i64::MIN, None, None, 10).unwrap();
        assert_eq!(stored[0].attempts, 10);
        assert_eq!(stored[0].up, 8);
        assert_eq!(stored[0].latency_sum, 80);
    }

    /// `StreamDigest`'s shape (max position plus a count) would call these two
    /// replicas converged: same bucket count, same newest bucket. Only the
    /// counter totals `RollupDigest` adds tell them apart.
    #[test]
    fn rollup_digest_reflects_counter_totals_not_just_count_and_max() {
        let behind = Storage::open(":memory:").unwrap();
        let ahead = Storage::open(":memory:").unwrap();
        behind
            .apply_minute_rollups(&[row("a", "agent-1", 60_000, 3, 1, 10)])
            .unwrap();
        ahead
            .apply_minute_rollups(&[row("a", "agent-1", 60_000, 5, 5, 50)])
            .unwrap();

        let behind_digest = behind.digest(0).unwrap().latency_minute_rollups;
        let ahead_digest = ahead.digest(0).unwrap().latency_minute_rollups;
        assert_eq!(behind_digest.count, ahead_digest.count);
        assert_eq!(
            behind_digest.max_bucket_start,
            ahead_digest.max_bucket_start
        );
        assert_ne!(
            behind_digest.attempts_sum, ahead_digest.attempts_sum,
            "count and max agree, but the counters do not, and the digest must show it"
        );
    }

    /// Item B: a detected gap is durable metadata, not an in-memory signal —
    /// it must be there for an operator to read after any restart, not only
    /// until the next one.
    #[test]
    fn a_raw_data_gap_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "warden-raw-data-gap-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path = path.to_string_lossy().into_owned();
        let gap = RawDataGapRow {
            check_id: cid("a"),
            peer: "http://peer".into(),
            from_ts: 1_000,
            to_ts: 5_000,
            observed_at: 9_000,
        };
        {
            let s = Storage::open(&path).unwrap();
            s.record_raw_data_gap(&gap).unwrap();
        }

        let reopened = Storage::open(&path).unwrap();
        assert_eq!(reopened.list_raw_data_gaps().unwrap(), vec![gap.clone()]);

        // The same standing gap, redetected on a later sweep with a fresher
        // `observed_at`, must refresh the one row rather than accumulate a
        // second one keyed the same way.
        let refreshed = RawDataGapRow {
            observed_at: 9_500,
            ..gap
        };
        reopened.record_raw_data_gap(&refreshed).unwrap();
        assert_eq!(
            reopened.list_raw_data_gaps().unwrap(),
            vec![refreshed],
            "redetecting a standing gap must update it in place"
        );
    }

    /// TODO.md: "evaluate an index or storage strategy for
    /// `check_results(check_id, ts_millis)`." This is the measurement that
    /// answers it, not an assumption either way.
    ///
    /// `check_results` has no index today beyond DuckDB's automatic per-row-group
    /// zone maps: `ts_millis` is monotonically appended, so a time-bounded scan
    /// already skips most row groups for free, but `check_id` has no locality —
    /// every ingest batch interleaves every check — so a single-check read has
    /// to evaluate `check_id = ?` against every row in whatever `ts_millis`
    /// window survives pruning. An ART index on `(check_id, ts_millis)` is the
    /// textbook fix for exactly that access pattern (`Storage::history`,
    /// `Storage::latency_series`'s per-check filter). The question is whether it
    /// is worth what DuckDB's own docs warn it costs: every insert into an
    /// indexed table pays ART maintenance, and an index the buffer manager
    /// cannot evict grows the resident memory floor for as long as any part of
    /// it has been touched.
    ///
    /// Seeds the same representative shape as
    /// `benchmark_representative_storage_capacity_baseline` twice — once with
    /// the candidate index created before any data lands (the worst case: every
    /// row ever written pays the maintenance cost, matching what putting it in
    /// `Storage::open`'s DDL would mean), once without — and reports the read
    /// and write sides side by side. Run with `WARDEN_BENCH_CHECKS`/
    /// `WARDEN_BENCH_DAYS`/`WARDEN_BENCH_OBSERVATION_SECS`/
    /// `WARDEN_BENCH_INGEST_ROUNDS` for other scales; the numbers recorded in
    /// SCALING.md are the defaults below.
    #[test]
    #[ignore = "index tradeoff benchmark; configure WARDEN_BENCH_* for the intended scale"]
    fn benchmark_check_results_check_id_ts_index_tradeoff() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn percentile(samples: &mut [std::time::Duration], percentile: usize) -> u128 {
            samples.sort_unstable();
            samples[(samples.len() * percentile / 100).min(samples.len() - 1)].as_millis()
        }
        fn bytes(path: &std::path::Path) -> u64 {
            std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
        }

        let check_count = env_u64("WARDEN_BENCH_CHECKS", 1_000);
        let days = env_u64("WARDEN_BENCH_DAYS", 7);
        let observation_secs = env_u64("WARDEN_BENCH_OBSERVATION_SECS", 30);
        let ingest_rounds = env_u64("WARDEN_BENCH_INGEST_ROUNDS", 20);
        let observations_per_check = days * 86_400 / observation_secs;
        let rows = check_count * observations_per_check;
        let step_ms = observation_secs as i64 * 1_000;
        let now = days as i64 * 86_400_000;

        struct Report {
            label: &'static str,
            seed_ms: u128,
            database_bytes: u64,
            reopen_ms: u128,
            history_p50_ms: u128,
            history_p95_ms: u128,
            latency_p50_ms: u128,
            latency_p95_ms: u128,
            ingest_rows_per_sec: f64,
        }
        let mut reports = Vec::new();

        for (label, with_index) in [("no_index", false), ("with_index", true)] {
            let path = std::env::temp_dir().join(format!(
                "warden-check-results-index-bench-{label}-{}-{}.duckdb",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
            let path_string = path.to_string_lossy().into_owned();
            let store = Storage::open(&path_string).unwrap();
            if with_index {
                // Created immediately after the table, before any row lands —
                // the same position a DDL-resident index would occupy in
                // `Storage::open`, so seeding below pays maintenance on every
                // row rather than a one-time bulk-build at the end.
                store
                    .conn
                    .execute_batch(
                        "CREATE INDEX bench_check_results_check_ts
                         ON check_results (check_id, ts_millis)",
                    )
                    .unwrap();
            }

            let seed_started = std::time::Instant::now();
            store
                .conn
                .execute_batch(&format!(
                    "INSERT INTO check_results
                     SELECT md5('check-' || check_number::VARCHAR)::UUID,
                            'agent-' || (check_number % 3)::VARCHAR,
                            observation_number * {step_ms},
                            CASE WHEN (observation_number + check_number * 17) % 97 = 0
                                 THEN 'down' ELSE 'up' END,
                            15 + ((observation_number * 37 + check_number * 13) % 2_000),
                            CASE WHEN (observation_number + check_number * 17) % 97 = 0
                                 THEN 503 ELSE 200 END,
                            NULL, NULL, 'zone', 'region'
                     FROM range({check_count}) checks(check_number)
                     CROSS JOIN range({observations_per_check}) observations(observation_number);"
                ))
                .unwrap();
            store.rebuild_result_watermarks().unwrap();
            store.rebuild_latest_check_results().unwrap();
            // This bulk seed bypasses ordinary ingest, so latency rollups are
            // backfilled here for the same reason `benchmark_checkpoint_after_prune`
            // does it: leaving them empty would make the reopen below trigger
            // `Storage::open`'s one-time rollup backfill instead of measuring
            // an ordinary restart.
            store.rebuild_latency_rollups().unwrap();
            store.conn.execute_batch("CHECKPOINT").unwrap();
            let seed_ms = seed_started.elapsed().as_millis();
            assert_eq!(store.count_results().unwrap(), rows);
            let database_bytes = bytes(&path);
            drop(store);

            // Reopen: what an index the docs call lazily-deserialized should
            // cost at startup is "nothing more than the base table load" — this
            // is the number that confirms or refutes that for the bundled
            // DuckDB build rather than trusting the docs unverified.
            let reopen_started = std::time::Instant::now();
            let store = Storage::open(&path_string).unwrap();
            let reopen_ms = reopen_started.elapsed().as_millis();

            let check_ids: Vec<CheckId> = {
                let mut stmt = store
                    .conn
                    .prepare("SELECT DISTINCT check_id FROM check_results")
                    .unwrap();
                stmt.query_map([], |r| r.get::<_, CheckId>(0))
                    .unwrap()
                    .collect::<std::result::Result<_, _>>()
                    .unwrap()
            };
            assert_eq!(check_ids.len() as u64, check_count);
            let hot_check_id = check_ids[0];

            let mut history_samples = Vec::new();
            let mut latency_samples = Vec::new();
            for _ in 0..20 {
                let started = std::time::Instant::now();
                let history = store
                    .history(hot_check_id, 100, Some(0), Some(now), None)
                    .unwrap();
                history_samples.push(started.elapsed());
                assert_eq!(history.len(), 100);
                let started = std::time::Instant::now();
                let series = store.latency_series(hot_check_id, 0, now, 120).unwrap();
                latency_samples.push(started.elapsed());
                assert!(series.overall.attempts > 0);
            }

            // Sustained ingest landing on top of the seeded backlog: the shape
            // that shows an index's steady-state per-row write cost, as
            // opposed to seed_ms above which mixes that cost with the one-time
            // bulk build. Every batch reuses the same check ids the backlog
            // already holds millions of rows for, so this is an insert into an
            // ART node that is already large, not an empty one.
            let ingest_started = std::time::Instant::now();
            for round in 0..ingest_rounds {
                let ts = now + (round as i64 + 1) * step_ms;
                let batch: Vec<(CheckResult, Status)> = check_ids
                    .iter()
                    .map(|&check_id| {
                        (
                            CheckResult {
                                zone: "zone".into(),
                                region: "region".into(),
                                body: None,
                                headers: None,
                                check_id,
                                agent_id: "agent-0".into(),
                                kind: CheckKind::Http,
                                timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
                                latency_ms: 20,
                                status_code: Some(200),
                                body_matched: None,
                                error: None,
                                failure: None,
                                metric: None,
                                metrics: None,
                            },
                            Status::Up,
                        )
                    })
                    .collect();
                store.persist_ingest(&batch, &[], &[]).unwrap();
            }
            let ingest_elapsed = ingest_started.elapsed();
            let ingest_rows_per_sec =
                (check_count * ingest_rounds) as f64 / ingest_elapsed.as_secs_f64();

            reports.push(Report {
                label,
                seed_ms,
                database_bytes,
                reopen_ms,
                history_p50_ms: percentile(&mut history_samples, 50),
                history_p95_ms: percentile(&mut history_samples, 95),
                latency_p50_ms: percentile(&mut latency_samples, 50),
                latency_p95_ms: percentile(&mut latency_samples, 95),
                ingest_rows_per_sec,
            });

            drop(store);
            let _ = std::fs::remove_file(wal_path);
            let _ = std::fs::remove_file(path);
        }

        eprintln!(
            "checks={check_count} days={days} observation_secs={observation_secs} \
             retained_rows={rows} ingest_rounds={ingest_rounds}"
        );
        for r in &reports {
            eprintln!(
                "{label}: seed_ms={seed_ms} database_bytes={database_bytes} reopen_ms={reopen_ms} \
                 history_p50_ms={history_p50_ms} history_p95_ms={history_p95_ms} \
                 latency_p50_ms={latency_p50_ms} latency_p95_ms={latency_p95_ms} \
                 ingest_rows_per_sec={ingest_rows_per_sec:.0}",
                label = r.label,
                seed_ms = r.seed_ms,
                database_bytes = r.database_bytes,
                reopen_ms = r.reopen_ms,
                history_p50_ms = r.history_p50_ms,
                history_p95_ms = r.history_p95_ms,
                latency_p50_ms = r.latency_p50_ms,
                latency_p95_ms = r.latency_p95_ms,
                ingest_rows_per_sec = r.ingest_rows_per_sec,
            );
        }
    }

    /// TODO.md: "evaluate indexes for alert, incident, and state-history lookup
    /// patterns." State-history is not benchmarked here: every lookup this
    /// codebase makes against it (`record_state`, `state_spans`, the prune
    /// anti-join) leads with `check_id`, which is already the first column of
    /// its declared `PRIMARY KEY (check_id, since)` — DuckDB builds an ART for
    /// every PRIMARY KEY automatically, so that table already has the index
    /// this TODO item would otherwise add. `list_status` and `uptime_strips`
    /// scan the whole table unfiltered by check_id (they want every check at
    /// once), which a `check_id`-leading index cannot help regardless.
    ///
    /// What is unindexed is the *reverse* lookups: `alerts` is scanned by
    /// `ts_millis` (`recent_alerts`, `alerts_since`, the retention delete) with
    /// only an `id` primary key, and `incident_checks` is scanned by
    /// `check_id` (`open_incident_for`, `incidents_for_check`) with only a
    /// `(incident_id, check_id)` primary key that does not lead with the
    /// column being filtered on. Both tables are bounded by `alerts_days`
    /// retention rather than by raw observation volume, so they stay orders of
    /// magnitude smaller than `check_results` — seeded here at a deliberately
    /// pessimistic upper bound: every check flapping roughly daily for the
    /// full 90-day default retention.
    #[test]
    #[ignore = "index tradeoff benchmark; configure WARDEN_BENCH_* for the intended scale"]
    fn benchmark_alert_incident_lookup_index_tradeoff() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn percentile(samples: &mut [std::time::Duration], percentile: usize) -> u128 {
            samples.sort_unstable();
            samples[(samples.len() * percentile / 100).min(samples.len() - 1)].as_millis()
        }
        fn bytes(path: &std::path::Path) -> u64 {
            std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
        }

        let alert_count = env_u64("WARDEN_BENCH_ALERTS", 900_000);
        let incident_count = env_u64("WARDEN_BENCH_INCIDENTS", 45_000);
        let check_pool = env_u64("WARDEN_BENCH_CHECK_POOL", 10_000);
        let write_rounds = env_u64("WARDEN_BENCH_ALERT_WRITE_ROUNDS", 2_000);

        struct Report {
            label: &'static str,
            seed_ms: u128,
            database_bytes: u64,
            reopen_ms: u128,
            recent_alerts_p95_ms: u128,
            alerts_since_p95_ms: u128,
            open_incident_for_p95_ms: u128,
            incidents_for_check_p95_ms: u128,
            insert_alert_rows_per_sec: f64,
        }
        let mut reports = Vec::new();

        for (label, with_index) in [("no_index", false), ("with_index", true)] {
            let path = std::env::temp_dir().join(format!(
                "warden-alert-incident-index-bench-{label}-{}-{}.duckdb",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
            let path_string = path.to_string_lossy().into_owned();
            let store = Storage::open(&path_string).unwrap();
            if with_index {
                store
                    .conn
                    .execute_batch(
                        "CREATE INDEX bench_alerts_ts ON alerts (ts_millis);
                         CREATE INDEX bench_incident_checks_check ON incident_checks (check_id)",
                    )
                    .unwrap();
            }

            let seed_started = std::time::Instant::now();
            store
                .conn
                .execute_batch(&format!(
                    "INSERT INTO incidents
                         (id, title, state, started_at, resolved_at, global, origin_check,
                          version_millis, version_node)
                     SELECT 'incident-' || i::VARCHAR, 'Down',
                            CASE WHEN i % 20 = 0 THEN 'detected' ELSE 'resolved' END,
                            i * 60_000,
                            CASE WHEN i % 20 = 0 THEN NULL ELSE i * 60_000 + 30_000 END,
                            FALSE,
                            md5('check-' || (i % {check_pool})::VARCHAR)::UUID,
                            i * 60_000, 'bench-node'
                     FROM range({incident_count}) t(i);
                     INSERT INTO incident_checks (incident_id, check_id, source)
                     SELECT 'incident-' || i::VARCHAR,
                            md5('check-' || (i % {check_pool})::VARCHAR)::UUID, 'origin'
                     FROM range({incident_count}) t(i);
                     INSERT INTO alerts
                         (id, scope, node_id, previous_state, current_state, ts_millis, incident_id)
                     SELECT 'alert-' || i::VARCHAR, 'check',
                            md5('check-' || (i % {check_pool})::VARCHAR)::VARCHAR,
                            'up', CASE WHEN i % 2 = 0 THEN 'down' ELSE 'up' END, i * 1_000,
                            'incident-' || (i % {incident_count})::VARCHAR
                     FROM range({alert_count}) t(i);"
                ))
                .unwrap();
            store.conn.execute_batch("CHECKPOINT").unwrap();
            let seed_ms = seed_started.elapsed().as_millis();
            let database_bytes = bytes(&path);
            drop(store);

            let reopen_started = std::time::Instant::now();
            let store = Storage::open(&path_string).unwrap();
            let reopen_ms = reopen_started.elapsed().as_millis();

            let hot_check_id: CheckId = store
                .conn
                .query_row(
                    "SELECT check_id FROM incident_checks
                     GROUP BY check_id ORDER BY COUNT(*) DESC LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();

            let mut recent_alerts_samples = Vec::new();
            let mut alerts_since_samples = Vec::new();
            let mut open_incident_for_samples = Vec::new();
            let mut incidents_for_check_samples = Vec::new();
            for _ in 0..20 {
                let started = std::time::Instant::now();
                let recent = store.recent_alerts(50).unwrap();
                recent_alerts_samples.push(started.elapsed());
                assert_eq!(recent.len(), 50);

                let started = std::time::Instant::now();
                let since = store
                    .alerts_since(alert_count as i64 * 1_000 / 2, "", 50)
                    .unwrap();
                alerts_since_samples.push(started.elapsed());
                assert_eq!(since.len(), 50);

                let started = std::time::Instant::now();
                store.open_incident_for(hot_check_id).unwrap();
                open_incident_for_samples.push(started.elapsed());

                let started = std::time::Instant::now();
                let incidents = store.incidents_for_check(hot_check_id, 50).unwrap();
                incidents_for_check_samples.push(started.elapsed());
                assert!(!incidents.is_empty());
            }

            // Alerts are inserted one confirmed transition at a time, never in
            // bulk, so the write-cost question here is per-row `INSERT` cost
            // against an already-large table, not a bulk-load rate.
            let write_started = std::time::Instant::now();
            for i in 0..write_rounds {
                store
                    .insert_alert(
                        AlertScope::Check,
                        &format!("write-check-{i}"),
                        Some(NodeState::Up),
                        NodeState::Down,
                        alert_count as i64 * 1_000 + i as i64,
                        None,
                    )
                    .unwrap();
            }
            let write_elapsed = write_started.elapsed();
            let insert_alert_rows_per_sec = write_rounds as f64 / write_elapsed.as_secs_f64();

            reports.push(Report {
                label,
                seed_ms,
                database_bytes,
                reopen_ms,
                recent_alerts_p95_ms: percentile(&mut recent_alerts_samples, 95),
                alerts_since_p95_ms: percentile(&mut alerts_since_samples, 95),
                open_incident_for_p95_ms: percentile(&mut open_incident_for_samples, 95),
                incidents_for_check_p95_ms: percentile(&mut incidents_for_check_samples, 95),
                insert_alert_rows_per_sec,
            });

            drop(store);
            let _ = std::fs::remove_file(wal_path);
            let _ = std::fs::remove_file(path);
        }

        eprintln!(
            "alerts={alert_count} incidents={incident_count} check_pool={check_pool} \
             write_rounds={write_rounds}"
        );
        for r in &reports {
            eprintln!(
                "{label}: seed_ms={seed_ms} database_bytes={database_bytes} reopen_ms={reopen_ms} \
                 recent_alerts_p95_ms={recent_alerts_p95_ms} alerts_since_p95_ms={alerts_since_p95_ms} \
                 open_incident_for_p95_ms={open_incident_for_p95_ms} \
                 incidents_for_check_p95_ms={incidents_for_check_p95_ms} \
                 insert_alert_rows_per_sec={insert_alert_rows_per_sec:.0}",
                label = r.label,
                seed_ms = r.seed_ms,
                database_bytes = r.database_bytes,
                reopen_ms = r.reopen_ms,
                recent_alerts_p95_ms = r.recent_alerts_p95_ms,
                alerts_since_p95_ms = r.alerts_since_p95_ms,
                open_incident_for_p95_ms = r.open_incident_for_p95_ms,
                incidents_for_check_p95_ms = r.incidents_for_check_p95_ms,
                insert_alert_rows_per_sec = r.insert_alert_rows_per_sec,
            );
        }
    }
}
