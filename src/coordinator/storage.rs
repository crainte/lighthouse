use crate::config::{CheckConfig, Expect};
use crate::coordinator::hlc::Hlc;
use crate::credential::AgentCredential;
use crate::models::{CheckKind, CheckResult, Status};
use anyhow::Result;
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// DuckDB-backed store for check results and materialized current state.
/// Single connection guarded by the caller; DuckDB is single-writer.
pub struct Storage {
    conn: Connection,
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
    pub check_id: String,
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
    pub check_id: Option<String>,
    pub agent_id: Option<String>,
    pub status: Option<Status>,
    pub region: Option<String>,
    pub zone: Option<String>,
}

/// Current status of a check plus recent uptime, for the status grid.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusRow {
    pub check_id: String,
    pub status: Status,
    pub latency_ms: u64,
    pub last_check: i64, // unix millis of latest result
    pub uptime_24h: f64, // fraction 0.0..=1.0 of UP results in the 24h before now
    /// Status code from the most recent result.
    pub status_code: Option<u16>,
    /// Failure reason from the most recent result, so the dashboard can say why
    /// a check is down rather than only that it is. Never publish it verbatim.
    pub error: Option<String>,
    /// That same failure as a category, safe to publish.
    pub failure: Option<crate::models::FailureKind>,
}

/// The coordinator's CONFIRMED decision per check, materialized so the status
/// page and a restarted process agree with the in-memory tracker.
#[derive(Debug, Clone, PartialEq)]
pub struct CurrentStateRow {
    pub check_id: String,
    pub status: Status,
    /// Consecutive DOWN observations toward `failures_before_down`.
    pub consecutive_failures: u32,
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
    /// What kind of thing moved. Only `Check` is written today; the region
    /// tree (Phase 5/6) is what will start writing the others.
    pub scope: AlertScope,
    /// The check, agent or zone that moved.
    pub node_id: String,
    /// `None` for a first-ever observation, which has nothing to move from.
    pub previous_state: Option<Status>,
    pub current_state: Status,
    pub timestamp: i64,
    /// The incident this transition belongs to, when it belongs to one.
    pub incident_id: Option<String>,
}

/// One notification that has not been delivered. The transition it carries is
/// everything a retry needs, so the row is self-contained: no join, and nothing
/// to dangle when the alert it came from is pruned.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedRow {
    /// The routing *name*, resolved against the live notifier list at each
    /// attempt. Editing a backend therefore fixes its queued notifications;
    /// deleting it drops them, because there is no longer anyone to page.
    pub notifier: String,
    pub check_id: String,
    pub previous_state: Option<Status>,
    pub current_state: Status,
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
    pub check_id: String,
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
    pub origin_check: Option<String>,
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
    pub fn gating_checks(&self) -> impl Iterator<Item = &str> {
        self.checks
            .iter()
            .filter(|c| c.source.gates_resolution())
            .map(|c| c.check_id.as_str())
    }
}

/// One segment of the uptime strip. Not necessarily a day: the window is split
/// into a fixed number of segments, so a 7-day view has ~2-hour segments.
#[derive(Debug, Clone, PartialEq)]
pub struct UptimeBucket {
    /// Exclusive lower bound of the segment, unix millis.
    pub start: i64,
    /// Inclusive upper bound, unix millis. Handed to the UI so it can query the
    /// exact window a bar covers without re-deriving the segment width.
    pub end: i64,
    /// Fraction 0.0..=1.0, or None when the segment has no data at all.
    pub uptime: Option<f64>,
    pub up: u32,
    pub total: u32,
}

/// A single latency data point for the sparkline chart.
#[derive(Debug, Clone, PartialEq)]
pub struct LatencyPoint {
    pub timestamp: i64, // unix millis of the window start
    /// Mean latency across the segment, or `None` when nothing was recorded in
    /// it. A segment with no attempts is not a 0ms response, and collapsing the
    /// two paints an imaginary flat line along the axis.
    pub avg_latency_ms: Option<f64>,
}

/// A named, ordered group on the status page. Stored separately from checks so
/// it can exist while empty and be renamed without touching what is in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Section {
    pub name: String,
    pub position: i32,
    /// Whether the public page renders this section collapsed on first load.
    pub collapsed: bool,
}

/// Express a plain interval as the equivalent cron, for migrating rows written
/// when checks had an `interval_secs` instead of a schedule.
fn seconds_to_cron(secs: u64) -> String {
    match secs {
        0..=59 => format!("*/{} * * * * *", secs.max(1)),
        s if s % 3600 == 0 && s < 86_400 => format!("0 0 */{} * * *", s / 3600),
        s if s % 60 == 0 && s < 3600 => format!("0 */{} * * * *", s / 60),
        // Anything unusual keeps its cadence approximately, in minutes.
        s => format!("0 */{} * * * *", (s / 60).clamp(1, 59)),
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
    fn observe(&mut self, latency_ms: u64, up: bool) {
        self.attempts += 1;
        if !up {
            return;
        }
        self.up += 1;
        self.sum_ms += latency_ms as f64;
        self.min_ms = Some(self.min_ms.map_or(latency_ms, |m| m.min(latency_ms)));
        self.max_ms = Some(self.max_ms.map_or(latency_ms, |m| m.max(latency_ms)));
    }

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
    pub title: String,
    pub state: IncidentState,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
    pub global: bool,
    pub origin_check: Option<String>,
    pub checks: Vec<(String, AffectSource)>,
    pub updates: Vec<IncidentUpdateRow>,
}

/// One check's contribution to a results digest: what a peer needs to know to
/// decide whether it is missing anything for this check, without shipping the
/// rows themselves.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckDigest {
    pub check_id: String,
    pub max_ts: i64,
    pub count: i64,
}

/// Which config table an entry belongs to. `kind` on `ConfigEntry` and
/// `config_tombstones`, and the discriminant a digest groups by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigKind {
    Check,
    Section,
    Setting,
    Credential,
}

impl ConfigKind {
    /// Every kind, for the callers that must sweep all of them — an untagged
    /// `GET /internal/config`, and config anti-entropy. Adding a variant above
    /// without adding it here would silently stop replicating that table, so
    /// the exhaustive match in `as_str` is the reminder to come back.
    pub const ALL: [ConfigKind; 4] = [
        ConfigKind::Check,
        ConfigKind::Section,
        ConfigKind::Setting,
        ConfigKind::Credential,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigKind::Check => "check",
            ConfigKind::Section => "section",
            ConfigKind::Setting => "setting",
            ConfigKind::Credential => "credential",
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
            _ => None,
        }
    }

    /// The table this kind's rows live in, and the column that is its primary
    /// key — the one place that mapping is written, so every reader and
    /// writer of a config row agrees on it.
    fn table_and_pk(&self) -> (&'static str, &'static str) {
        match self {
            ConfigKind::Check => ("checks", "name"),
            ConfigKind::Section => ("sections", "name"),
            ConfigKind::Setting => ("settings", "key"),
            ConfigKind::Credential => ("agent_credentials", "id"),
        }
    }
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
    pub max_alert_ts: i64,
    pub max_incident_started_at: i64,
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
    /// credential id.
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
}

impl Storage {
    /// Open (or create) the database at `path` and ensure the schema exists.
    /// `path` may be ":memory:" for tests.
    pub fn open(path: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };
        // The one schema change this project makes by recreation rather than by
        // ALTER, because `agent_polls` shipped with a column missing and with
        // no writer: there is no row anywhere to migrate. Conditional so a
        // deployment that already holds the new shape keeps its rows across a
        // restart, which is half the reason the table exists at all.
        {
            let cols: Vec<String> = {
                let mut stmt = conn.prepare(
                    "SELECT column_name FROM information_schema.columns
                     WHERE table_name = 'agent_polls'",
                )?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            if !cols.is_empty() && !cols.iter().any(|c| c == "window_ms") {
                conn.execute_batch("DROP TABLE agent_polls")?;
            }
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS check_results (
                check_id   VARCHAR NOT NULL,
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
            -- The event log: one row per confirmed state transition.
            -- `incident_id` is stamped as the row is written, because
            -- membership is a fact the coordinator knows at that moment and a
            -- reader cannot re-derive it — a time-window join cannot tell two
            -- overlapping causes apart, and it would silently absorb unrelated
            -- history the moment a check joined an incident's affects list.
            -- No `alert_type` column: StateChange/Degradation/Recovery is a
            -- function of the two state columns, so storing it invites drift.
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
                origin_check VARCHAR
            );
            -- Which checks an incident affects. `source` (origin, dependency,
            -- manual) exists so reconciliation can retract what it added
            -- without disturbing what an operator added by hand.
            CREATE TABLE IF NOT EXISTS incident_checks (
                incident_id VARCHAR NOT NULL,
                check_id    VARCHAR NOT NULL,
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
            CREATE TABLE IF NOT EXISTS current_state (
                check_id   VARCHAR PRIMARY KEY,
                status     VARCHAR NOT NULL,
                last_check BIGINT  NOT NULL,
                consecutive_failures INTEGER NOT NULL DEFAULT 0
            );
            -- Check definitions, editable at runtime from the admin UI. Seeded
            -- from the config file on first boot; the table is authoritative
            -- afterwards, so an edit isn't silently reverted by a restart.
            -- `group` is a SQL keyword, hence group_name.
            CREATE TABLE IF NOT EXISTS checks (
                name        VARCHAR PRIMARY KEY,
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
                expect_status INTEGER,
                expect_body_contains VARCHAR,
                -- (updated_at, updated_by) is the HLC this row was last written
                -- at. It is the whole conflict-resolution story for clustered
                -- config: a higher stamp wins, ties break on the node id, and
                -- every node therefore picks the same winner without asking.
                updated_at  BIGINT  NOT NULL,
                updated_by  VARCHAR
            );
            -- Status-page sections. A row here, rather than just a string on
            -- each check, so a section can be created empty, renamed, and
            -- ordered independently of what is in it.
            CREATE TABLE IF NOT EXISTS sections (
                name     VARCHAR PRIMARY KEY,
                position INTEGER NOT NULL,
                collapsed BOOLEAN DEFAULT FALSE,
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
            -- id either — (notifier, check, queued_at) is unique, since one
            -- transition enqueues at most once per notifier.
            --
            -- `next_attempt` NULL means given up: attempts are exhausted and
            -- nobody was paged. That is derived state made explicit, rather than
            -- a `status` column that can disagree with `attempts`.
            CREATE TABLE IF NOT EXISTS notification_queue (
                notifier       VARCHAR NOT NULL,
                check_id       VARCHAR NOT NULL,
                previous_state VARCHAR,
                current_state  VARCHAR NOT NULL,
                attempts       INTEGER NOT NULL,
                next_attempt   BIGINT,
                last_error     VARCHAR NOT NULL,
                queued_at      BIGINT  NOT NULL,
                PRIMARY KEY (notifier, check_id, queued_at)
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
                kind       VARCHAR NOT NULL,   -- check | section | setting | credential
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
            --
            -- `window_ms` was added after the table existed but before
            -- anything ever wrote to it, so the stale shape is dropped above
            -- rather than altered. Not dropped unconditionally: this table is
            -- now written to, and a `DROP TABLE IF EXISTS` in the DDL would
            -- empty the fleet view on every restart — the exact thing
            -- persisting it was for.
            CREATE TABLE IF NOT EXISTS agent_polls (
                agent_id  VARCHAR PRIMARY KEY,
                zone      VARCHAR NOT NULL,
                region    VARCHAR NOT NULL,
                -- `at` is a reserved word in DuckDB's SQL grammar, hence the
                -- column name differing from the Rust field it backs.
                polled_at BIGINT  NOT NULL,
                window_ms BIGINT  NOT NULL,
                rtt_ms    BIGINT
            );
            -- An incident delete is replicated state for the same reason a
            -- config delete is: an absence alone cannot be told apart from
            -- never having heard of it, so a peer's next push or the
            -- reconciler would hand the deleted incident straight back.
            CREATE TABLE IF NOT EXISTS incident_tombstones (
                id         VARCHAR PRIMARY KEY,
                deleted_at BIGINT  NOT NULL,
                node_id    VARCHAR NOT NULL
            );",
        )?;
        // Columns added after the first release. DuckDB has no
        // ADD COLUMN IF NOT EXISTS, so ask what already exists and add only
        // what's missing. Errors are NOT swallowed: an earlier version of this
        // blanket-ignored them and silently half-migrated, because DuckDB
        // rejects NOT NULL on ADD COLUMN — the coordinator then crash-looped on
        // a column it thought it had added.
        let existing: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'checks'",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        for (column, ddl) in [
            (
                "visible",
                "ALTER TABLE checks ADD COLUMN visible BOOLEAN DEFAULT TRUE",
            ),
            ("eval", "ALTER TABLE checks ADD COLUMN eval VARCHAR"),
            (
                "position",
                "ALTER TABLE checks ADD COLUMN position INTEGER DEFAULT 0",
            ),
            // JSON rather than a comma-joined string: an agent id is arbitrary
            // config text and a separator inside one would silently split it.
            ("agents", "ALTER TABLE checks ADD COLUMN agents VARCHAR"),
            // The whole definition, serialized. A check now carries per-type
            // option blocks, headers and schedules; a column per field was a
            // migration treadmill, so the document is authoritative and the
            // flat columns below survive only for SQL to filter and order by.
            ("config", "ALTER TABLE checks ADD COLUMN config VARCHAR"),
        ] {
            if !existing.iter().any(|c| c == column) {
                conn.execute_batch(ddl)?;
            }
        }
        let section_columns: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'sections'",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let result_columns: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'check_results'",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        if !result_columns.iter().any(|c| c == "failure") {
            conn.execute_batch("ALTER TABLE check_results ADD COLUMN failure VARCHAR")?;
        }
        for column in ["zone", "region"] {
            if !result_columns.iter().any(|c| c == column) {
                conn.execute_batch(&format!(
                    "ALTER TABLE check_results ADD COLUMN {column} VARCHAR"
                ))?;
            }
        }
        if !section_columns.iter().any(|c| c == "collapsed") {
            conn.execute_batch("ALTER TABLE sections ADD COLUMN collapsed BOOLEAN DEFAULT FALSE")?;
        }
        // The added columns are nullable (DuckDB won't take NOT NULL here), so
        // give pre-existing rows the intended value rather than letting a NULL
        // reach a non-optional read.
        conn.execute_batch(
            "UPDATE checks SET visible = TRUE WHERE visible IS NULL;
             UPDATE checks SET position = 0 WHERE position IS NULL;",
        )?;
        let store = Self { conn };
        store.backfill_configs()?;
        Ok(store)
    }

    /// Give rows written before the `config` document a document, built from the
    /// flat columns they do have. Without this an older database would read back
    /// as checks with default schedules and no targets.
    fn backfill_configs(&self) -> Result<()> {
        let legacy: Vec<CheckConfig> = {
            let mut stmt = self.conn.prepare(
                "SELECT name, kind, target, interval_secs, timeout_secs, group_name,
                        latency_graph, enabled, expect_status, expect_body_contains,
                        visible, eval, position, agents
                 FROM checks WHERE config IS NULL",
            )?;
            let rows = stmt.query_map([], |row| {
                let interval = row.get::<_, i64>(3)?.max(5);
                Ok(CheckConfig {
                    name: row.get(0)?,
                    kind: CheckKind::from_db(&row.get::<_, String>(1)?),
                    target: row.get(2)?,
                    // An interval becomes the equivalent cron, so an upgrade
                    // doesn't silently change how often anything runs.
                    cron: seconds_to_cron(interval as u64),
                    timeout_secs: row.get::<_, i64>(4)? as u64,
                    group: row.get::<_, Option<String>>(5)?,
                    latency_graph: row.get(6)?,
                    enabled: row.get(7)?,
                    expect: Expect {
                        status: row.get::<_, Option<i32>>(8)?.map(|s| s as u16),
                        body_contains: row.get(9)?,
                    },
                    visible: row.get(10)?,
                    eval: row.get::<_, Option<String>>(11)?,
                    position: row.get::<_, Option<i32>>(12)?.unwrap_or(0),
                    agents: row
                        .get::<_, Option<String>>(13)?
                        .and_then(|raw| serde_json::from_str(&raw).ok())
                        .unwrap_or_default(),
                    ..Default::default()
                })
            })?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        for check in legacy {
            let json = serde_json::to_string(&check)?;
            self.conn.execute(
                "UPDATE checks SET config = ? WHERE name = ?",
                duckdb::params![json, check.name],
            )?;
        }
        Ok(())
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

    /// Create or replace a check by name.
    ///
    /// `config` is the definition; the other columns are a projection of it so
    /// SQL can order and filter without parsing every document.
    pub fn upsert_check(&self, c: &CheckConfig, now_millis: i64) -> Result<()> {
        // DuckDB's ON CONFLICT needs the same column list twice; deleting first
        // is shorter and the two statements are cheap at this table's size.
        self.conn
            .execute("DELETE FROM checks WHERE name = ?", duckdb::params![c.name])?;
        self.conn.execute(
            "INSERT INTO checks
                (name, kind, target, interval_secs, timeout_secs, group_name,
                 latency_graph, enabled, expect_status, expect_body_contains,
                 visible, eval, position, agents, config, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
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

    /// Apply an explicit display order. Names not listed keep their relative
    /// order after the listed ones.
    pub fn reorder_checks(&self, order: &[String], now_millis: i64) -> Result<()> {
        // Rewrite whole rows rather than UPDATE-ing the position column: the
        // document is authoritative, so touching only the column would reorder
        // the index and leave every reader seeing the old order.
        let mut checks = self.list_checks()?;
        let rank = |name: &str| {
            order
                .iter()
                .position(|n| n == name)
                // Names the caller didn't mention keep their relative order
                // after the ones it did.
                .unwrap_or(usize::MAX)
        };
        checks.sort_by(|a, b| {
            rank(&a.name)
                .cmp(&rank(&b.name))
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
        // The fleet view is the union of results and poll rows, so leaving the
        // poll row behind would purge an agent that then reappeared, healthy.
        self.delete_agent_poll(agent_id)?;
        Ok(rows as u64)
    }

    /// Delete a check's recorded results and alerts, keeping its definition.
    /// Returns how many result rows went, so the UI can say what it did.
    pub fn purge_check_data(&self, name: &str) -> Result<u64> {
        let rows = self.conn.execute(
            "DELETE FROM check_results WHERE check_id = ?",
            duckdb::params![name],
        )?;
        self.conn.execute(
            "DELETE FROM alerts WHERE node_id = ?",
            duckdb::params![name],
        )?;
        Ok(rows as u64)
    }

    /// Returns whether a row was actually removed, so the handler can 404
    /// instead of reporting success for a name that never existed.
    pub fn delete_check(&self, name: &str) -> Result<bool> {
        let n = self
            .conn
            .execute("DELETE FROM checks WHERE name = ?", duckdb::params![name])?;
        Ok(n > 0)
    }

    /// Adopt the config file's checks the first time we run against an empty
    /// table. Existing deployments keep their checks; afterwards the table wins,
    /// so an admin edit survives a restart instead of being overwritten by the
    /// file it came from.
    pub fn seed_checks_if_empty(&self, checks: &[CheckConfig], now_millis: i64) -> Result<bool> {
        let existing: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM checks", [], |r| r.get(0))?;
        if existing > 0 || checks.is_empty() {
            return Ok(false);
        }
        for c in checks {
            self.upsert_check(c, now_millis)?;
        }
        Ok(true)
    }

    /// Sections in display order.
    pub fn list_sections(&self) -> Result<Vec<Section>> {
        let mut stmt = self
            .conn
            .prepare("SELECT name, position, collapsed FROM sections ORDER BY position, name")?;
        let rows = stmt.query_map([], |row| {
            Ok(Section {
                name: row.get(0)?,
                position: row.get::<_, i32>(1)?,
                collapsed: row.get::<_, Option<bool>>(2)?.unwrap_or(false),
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Create a section at the end. Returns false when the name is taken.
    pub fn create_section(&self, name: &str, collapsed: bool) -> Result<bool> {
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
            "INSERT INTO sections (name, position, collapsed) VALUES (?, ?, ?)",
            duckdb::params![name, next, collapsed],
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
            self.create_section(&name, false)?;
        }
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

    /// Return history rows for a check, newest first, optionally within a time
    /// range and optionally restricted to one evaluated status.
    ///
    /// The status filter matters: a day can hold thousands of results, so the
    /// failures worth reading about are not necessarily in the newest `limit`.
    pub fn history(
        &self,
        check_id: &str,
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
            vec![duckdb::types::Value::Text(check_id.to_owned())];
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
            params.push(duckdb::types::Value::Text(c.clone()));
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
                    check_id: row.get::<_, String>(0)?,
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

    /// Persist an alert: one confirmed state transition. `incident_id` is
    /// stamped here, by the caller that just decided which incident this
    /// transition belongs to.
    pub fn insert_alert(
        &self,
        scope: AlertScope,
        node_id: &str,
        previous_state: Option<Status>,
        current_state: Status,
        ts_millis: i64,
        incident_id: Option<&str>,
    ) -> Result<String> {
        let id = format!("{node_id}-{ts_millis}-{}", current_state.as_str());
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

    /// Record a notification that could not be delivered, due for another
    /// attempt at `next_attempt`.
    ///
    /// Enqueued on failure, not before the first attempt: the happy path stays
    /// one HTTP request with no database write. ponytail: the cost is a crash
    /// between the failed send and this insert losing the retry — the same
    /// window an unqueued alert has today, and closing it means writing every
    /// notification twice.
    pub fn enqueue_notification(&self, row: &QueuedRow) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO notification_queue
                 (notifier, check_id, previous_state, current_state,
                  attempts, next_attempt, last_error, queued_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
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
            "SELECT notifier, check_id, previous_state, current_state,
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
            "SELECT notifier, check_id, previous_state, current_state,
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

    pub fn delete_notification(
        &self,
        notifier: &str,
        check_id: &str,
        queued_at: i64,
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM notification_queue
             WHERE notifier = ? AND check_id = ? AND queued_at = ?",
            duckdb::params![notifier, check_id, queued_at],
        )?;
        Ok(())
    }

    fn queued_row(row: &duckdb::Row) -> duckdb::Result<QueuedRow> {
        Ok(QueuedRow {
            notifier: row.get::<_, String>(0)?,
            check_id: row.get::<_, String>(1)?,
            previous_state: row
                .get::<_, Option<String>>(2)?
                .as_deref()
                .map(Status::from_db),
            current_state: Status::from_db(&row.get::<_, String>(3)?),
            attempts: row.get::<_, i32>(4)?,
            next_attempt: row.get::<_, Option<i64>>(5)?,
            last_error: row.get::<_, String>(6)?,
            queued_at: row.get::<_, i64>(7)?,
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
                .map(Status::from_db),
            current_state: Status::from_db(&row.get::<_, String>(4)?),
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
    pub fn open_incident_for(&self, check_id: &str) -> Result<Option<IncidentRow>> {
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
        origin_check: Option<&str>,
        checks: &[IncidentCheckRow],
        first_update: Option<(&str, Option<&str>)>,
    ) -> Result<IncidentRow> {
        self.conn.execute(
            "INSERT INTO incidents (id, title, state, started_at, global, origin_check)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![id, title, state.as_str(), started_at, global, origin_check],
        )?;
        for c in checks {
            self.attach_check(id, &c.check_id, c.source)?;
        }
        if let Some((body, author)) = first_update {
            self.add_update(id, state, body, author, started_at)?;
        }
        self.incident(id)?
            .ok_or_else(|| anyhow::anyhow!("incident {id} vanished immediately after insert"))
    }

    /// Attach a check to an incident. Idempotent on (incident, check): a repeat
    /// only upgrades how it got there, and never downgrades an `origin`.
    pub fn attach_check(
        &self,
        incident_id: &str,
        check_id: &str,
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
                Ok(())
            }
            None => {
                self.conn.execute(
                    "INSERT INTO incident_checks (incident_id, check_id, source)
                     VALUES (?, ?, ?)",
                    duckdb::params![incident_id, check_id, source.as_str()],
                )?;
                Ok(())
            }
        }
    }

    /// Detach a check. `only_source` restricts it to attachments of one kind,
    /// so reconciliation can retract a `dependency` without touching what an
    /// operator added by hand.
    pub fn detach_check(
        &self,
        incident_id: &str,
        check_id: &str,
        only_source: Option<AffectSource>,
    ) -> Result<()> {
        match only_source {
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
        Ok(())
    }

    /// Replace exactly the manual attachments, leaving origin and dependency
    /// rows alone. The affects editor saves through this.
    pub fn set_manual_checks(&self, incident_id: &str, checks: &[String]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM incident_checks WHERE incident_id = ? AND source = 'manual'",
            duckdb::params![incident_id],
        )?;
        for check_id in checks {
            self.attach_check(incident_id, check_id, AffectSource::Manual)?;
        }
        Ok(())
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
        let id = format!("{incident_id}-u{now_millis}");
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
        let changed = self.conn.execute(
            "UPDATE incidents SET title = ?, started_at = ?, global = ? WHERE id = ?",
            duckdb::params![title, started_at, global, id],
        )?;
        if changed == 0 {
            return Ok(None);
        }
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
                    check_id: row.get::<_, String>(0)?,
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
        sql.push_str(" ORDER BY started_at DESC LIMIT ?");
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt
            .query_map(duckdb::params![limit as i64], Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for row in &mut rows {
            row.checks = self.checks_for_incident(&row.id)?;
        }
        Ok(rows)
    }

    /// A check's incidents, newest first — every incident it was attached to,
    /// however it got there.
    pub fn incidents_for_check(&self, check_id: &str, limit: usize) -> Result<Vec<IncidentRow>> {
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

    /// Whether a check's last `runs` observations were all UP — the up-side
    /// mirror of `failures_before_down`, and what gates auto-resolve. Reading
    /// it from the results rather than keeping a counter means it is correct
    /// across a restart with nothing to seed or resync.
    ///
    /// A check with fewer than `runs` results has not cleared the bar yet.
    pub fn recovered_for(&self, check_id: &str, runs: u32) -> Result<bool> {
        let runs = runs.max(1);
        let mut stmt = self.conn.prepare(
            "SELECT status FROM check_results
             WHERE check_id = ?
             ORDER BY ts_millis DESC
             LIMIT ?",
        )?;
        let statuses = stmt
            .query_map(duckdb::params![check_id, runs as i64], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(statuses.len() == runs as usize
            && statuses.iter().all(|s| Status::from_db(s) == Status::Up))
    }

    fn incident_row(row: &duckdb::Row) -> duckdb::Result<IncidentRow> {
        Ok(IncidentRow {
            id: row.get::<_, String>(0)?,
            title: row.get::<_, String>(1)?,
            state: IncidentState::from_db(&row.get::<_, String>(2)?),
            started_at: row.get::<_, i64>(3)?,
            resolved_at: row.get::<_, Option<i64>>(4)?,
            global: row.get::<_, bool>(5)?,
            origin_check: row.get::<_, Option<String>>(6)?,
            checks: Vec::new(),
        })
    }

    /// One StatusRow per check_id: the latest result's status/latency/code/error,
    /// plus the fraction of UP results in the 24h window `(now-24h, now]`.
    /// A check with no rows at all is absent; uptime is over in-window rows only.
    pub fn list_status(&self, now_millis: i64) -> Result<Vec<StatusRow>> {
        let window_start = now_millis - 86_400_000;
        // Every "latest" field is read from a single row, picked by row_number.
        // Per-column `arg_max` is wrong here: it skips NULLs, so a recovered
        // check would keep reporting the stale error of its last failure.
        //
        // The displayed status is the coordinator's CONFIRMED state
        // (`current_state`), not the raw last observation: with
        // `failures_before_down` > 1 a check stays UP through its first failed
        // probes, and that decision must survive a restart. Checks with no
        // row yet (pre-upgrade data) fall back to the raw latest status.
        let mut stmt = self.conn.prepare(
            "SELECT
                 latest.check_id,
                 COALESCE(cs.status, latest.status) AS status,
                 latest.latency_ms,
                 latest.ts_millis,
                 win.uptime,
                 latest.status_code,
                 latest.error,
                 latest.failure
             FROM (
                 SELECT check_id, status, latency_ms, ts_millis, status_code, error, failure,
                        row_number() OVER (PARTITION BY check_id ORDER BY ts_millis DESC) AS rn
                 FROM check_results
             ) AS latest
             JOIN (
                 SELECT check_id,
                     CAST(SUM(CASE WHEN status = 'up' AND ts_millis > ? AND ts_millis <= ? THEN 1 ELSE 0 END) AS DOUBLE)
                       / NULLIF(SUM(CASE WHEN ts_millis > ? AND ts_millis <= ? THEN 1 ELSE 0 END), 0) AS uptime
                 FROM check_results
                 GROUP BY check_id
             ) AS win ON win.check_id = latest.check_id
             LEFT JOIN current_state cs ON cs.check_id = latest.check_id
             WHERE latest.rn = 1",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![window_start, now_millis, window_start, now_millis],
                |row| {
                    Ok(StatusRow {
                        check_id: row.get::<_, String>(0)?,
                        status: Status::from_db(&row.get::<_, String>(1)?),
                        latency_ms: row.get::<_, i64>(2)? as u64,
                        last_check: row.get::<_, i64>(3)?,
                        uptime_24h: row.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
                        status_code: row.get::<_, Option<i32>>(5)?.map(|c| c as u16),
                        error: row.get::<_, Option<String>>(6)?,
                        failure: row
                            .get::<_, Option<String>>(7)?
                            .and_then(|f| serde_json::from_str(&f).ok()),
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Persist what the coordinator decided a check reads as now: the confirmed
    /// status, the in-flight DOWN count toward its threshold, and when it was
    /// last observed. Written on every observation; read back at startup so the
    /// in-memory tracker resumes confirmations exactly where it left off.
    pub fn update_current_state(
        &self,
        check_id: &str,
        status: Status,
        consecutive_failures: u32,
        last_check: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO current_state (check_id, status, last_check, consecutive_failures)
             VALUES (?, ?, ?, ?)",
            duckdb::params![check_id, status.as_str(), last_check, consecutive_failures as i32],
        )?;
        Ok(())
    }

    /// All persisted confirmed states, for seeding the tracker on startup.
    pub fn list_current_state(&self) -> Result<Vec<CurrentStateRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT check_id, status, consecutive_failures FROM current_state")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(CurrentStateRow {
                    check_id: row.get::<_, String>(0)?,
                    status: Status::from_db(&row.get::<_, String>(1)?),
                    consecutive_failures: row.get::<_, i32>(2)? as u32,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
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
    pub fn prune(&self, results_days: u64, alerts_days: u64, now_millis: i64) -> Result<u64> {
        let results_before = now_millis - (results_days as i64) * 86_400_000;
        let alerts_before = now_millis - (alerts_days as i64) * 86_400_000;
        let mut removed = 0u64;
        removed += self.conn.execute(
            "DELETE FROM check_results WHERE ts_millis < ?",
            duckdb::params![results_before],
        )? as u64;
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
        let tombstone_days = results_days.max(alerts_days);
        let tombstone_before = now_millis - (tombstone_days as i64) * 86_400_000;
        removed += self.prune_tombstones(tombstone_before)? as u64;
        removed += self.prune_incident_tombstones(tombstone_before)? as u64;
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
        check_id: &str,
        days: usize,
        buckets: usize,
        now_millis: i64,
    ) -> Result<Vec<UptimeBucket>> {
        const DAY: i64 = 86_400_000;
        let buckets = buckets.max(1);
        let window = days as i64 * DAY;
        let bucket_ms = (window / buckets as i64).max(1);
        // Recompute the span from the bucket width so segment edges tile exactly:
        // integer division would otherwise leave a sliver unaccounted for.
        let span = bucket_ms * buckets as i64;
        let lower = now_millis - span; // exclusive

        let mut up = vec![0u32; buckets];
        let mut total = vec![0u32; buckets];

        let mut stmt = self.conn.prepare(
            "SELECT ts_millis, status FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
        )?;
        let rows = stmt.query_map(duckdb::params![check_id, lower, now_millis], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for r in rows {
            let (ts, status) = r?;
            // Segments ago: 0 = the one ending at `now`. The range guarantees
            // 0 <= d < buckets.
            let d = ((now_millis - ts) / bucket_ms) as usize;
            let i = buckets - 1 - d;
            total[i] += 1;
            if Status::from_db(&status) == Status::Up {
                up[i] += 1;
            }
        }

        let out = (0..buckets)
            .map(|i| {
                let t = total[i];
                let start = now_millis - (buckets as i64 - i as i64) * bucket_ms;
                UptimeBucket {
                    start,
                    end: start + bucket_ms,
                    up: up[i],
                    total: t,
                    uptime: if t > 0 {
                        Some(up[i] as f64 / t as f64)
                    } else {
                        None
                    },
                }
            })
            .collect();
        Ok(out)
    }

    /// Average latency for a check over an explicit window, split into equal
    /// segments (oldest first), plus exact per-agent attribution over the same
    /// window. Segments with no attempts carry `None`, not zero.
    ///
    /// Returns at most `buckets` points: the split is capped at one segment per
    /// recorded instant, because slicing finer than the data only manufactures
    /// empty segments.
    ///
    /// The window is explicit rather than `now`-anchored so a historical
    /// segment (a clicked uptime bar) can be charted truthfully instead of
    /// being approximated by the newest N rows.
    pub fn latency_series(
        &self,
        check_id: &str,
        from: i64,
        to: i64,
        buckets: usize,
    ) -> Result<LatencySeries> {
        // The window is exactly what the caller asked for. An earlier attempt
        // narrowed it to the data inside it, which made a young check readable
        // but silently broke the range selector: every range shorter than the
        // check's history rendered identically, so the control did nothing.
        // The selector is the viewer's instrument for choosing a span, and a
        // 30-day view of a check that is five hours old *should* look mostly
        // empty — that emptiness is the check's age, honestly drawn.
        //
        // Slicing finer than the recorded instants is a different matter: it
        // manufactures empty segments between real ones, a picket fence of
        // imaginary 0ms readings. One segment per instant is the finest split
        // that carries information.
        let instants: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT ts_millis) FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
            duckdb::params![check_id, from, to],
            |row| row.get(0),
        )?;
        if instants == 0 {
            // Nothing recorded in the window: an empty series, not a row of
            // imaginary 0ms readings.
            return Ok(LatencySeries::default());
        }
        let buckets = buckets.max(1).min(instants as usize);
        let bucket_ms = ((to - from) / buckets as i64).max(1);

        let mut sum = vec![0.0f64; buckets];
        let mut count = vec![0u32; buckets];
        let mut agents: BTreeMap<String, LatencyBand> = BTreeMap::new();

        let mut stmt = self.conn.prepare(
            "SELECT ts_millis, latency_ms, agent_id, status FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
        )?;
        let rows = stmt.query_map(duckdb::params![check_id, from, to], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for r in rows {
            let (ts, lat, agent_id, status) = r?;
            // Lower bound is exclusive, so shift before dividing: a timestamp
            // landing exactly on a boundary belongs to the segment below it.
            let i = (((ts - from - 1).max(0) / bucket_ms) as usize).min(buckets - 1);
            sum[i] += lat as f64;
            count[i] += 1;
            agents
                .entry(agent_id)
                .or_default()
                .observe(lat as u64, status == "up");
        }

        let points = (0..buckets)
            .map(|i| LatencyPoint {
                timestamp: from + i as i64 * bucket_ms,
                avg_latency_ms: (count[i] > 0).then(|| sum[i] / count[i] as f64),
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
                        check_id: name.to_string(),
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
        self.insert_results(&rows)?;

        // Matching incidents at the start of each bad day and its recovery, so
        // the incidents list mirrors the bars. Seeded through the real
        // lifecycle — an incident with its opening and resolving updates, and
        // alerts stamped with its id — so the demo exercises the same shapes
        // the coordinator writes.
        let day_start_ts = |day: usize| now_millis - (days - 1 - day) as i64 * DAY;
        let outage = |name: &str, down_day: usize| -> Result<()> {
            let started = day_start_ts(down_day);
            let ended = day_start_ts(down_day + 1);
            let id = format!("{name}-{started}");
            self.create_incident(
                &id,
                &format!("{name} unreachable"),
                IncidentState::Detected,
                started,
                false,
                Some(name),
                &[IncidentCheckRow {
                    check_id: name.to_owned(),
                    source: AffectSource::Origin,
                }],
                Some(("connection refused", None)),
            )?;
            self.insert_alert(
                AlertScope::Check,
                name,
                Some(Status::Up),
                Status::Down,
                started,
                Some(&id),
            )?;
            self.insert_alert(
                AlertScope::Check,
                name,
                Some(Status::Down),
                Status::Up,
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
    /// anti-join on `(check_id, agent_id, ts_millis)` done per row with a
    /// `WHERE NOT EXISTS`. Replaying the same batch is then a guaranteed
    /// no-op, which is what anti-entropy depends on. ponytail: if the
    /// anti-join ever costs too much against a large table, the fallback is a
    /// `UNIQUE(check_id, agent_id, ts_millis)` index added in the same DDL
    /// batch, with `INSERT OR IGNORE` replacing the subquery.
    pub fn insert_results_dedup(&self, rows: &[(CheckResult, Status)]) -> Result<usize> {
        self.conn.execute_batch("BEGIN")?;
        let insert = || -> Result<usize> {
            let mut stmt = self.conn.prepare(
                "INSERT INTO check_results
                    (check_id, agent_id, ts_millis, status, latency_ms, status_code,
                     error, failure, zone, region)
                 SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                 WHERE NOT EXISTS (
                     SELECT 1 FROM check_results
                     WHERE check_id = ? AND agent_id = ? AND ts_millis = ?
                 )",
            )?;
            let mut inserted = 0usize;
            for (r, status) in rows {
                let ts = r.timestamp.timestamp_millis();
                let n = stmt.execute(duckdb::params![
                    r.check_id,
                    r.agent_id,
                    ts,
                    status.as_str(),
                    r.latency_ms as i64,
                    r.status_code.map(|c| c as i32),
                    r.error,
                    r.failure.as_ref().map(serde_json::to_string).transpose()?,
                    non_empty(&r.zone),
                    non_empty(&r.region),
                    r.check_id,
                    r.agent_id,
                    ts,
                ])?;
                inserted += n;
            }
            Ok(inserted)
        };
        match insert() {
            Ok(n) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(n)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Idempotent by id — a replicated alert that arrives twice must not
    /// become two rows in the event log. Local writes still go through
    /// `insert_alert`, which mints the id from node/time/state; this is for a
    /// row arriving with that id already decided by whoever wrote it first.
    pub fn upsert_alert(&self, a: &AlertRow) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO alerts
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
        Ok(())
    }

    /// Idempotent by id — replaces the incident row and its affects set, and
    /// inserts any update whose id is new without ever duplicating one that
    /// already landed. What closes the `ponytail:` at the gossip handler: a
    /// replicated alert can be stamped with the *real* incident id once this
    /// has run, instead of staying unstamped or naming a local-only one.
    pub fn upsert_incident(&self, i: &ReplicatedIncident) -> Result<()> {
        // A deleted incident stays deleted. Without this a peer's push, or the
        // reconciler replaying an incident it still holds, walks it straight
        // back in — and the operator who deleted it watches it reappear.
        if self.incident_tombstoned(&i.id)? {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN")?;
        let write = || -> Result<()> {
            self.conn
                .execute("DELETE FROM incidents WHERE id = ?", duckdb::params![i.id])?;
            self.conn.execute(
                "INSERT INTO incidents
                     (id, title, state, started_at, resolved_at, global, origin_check)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    i.id,
                    i.title,
                    i.state.as_str(),
                    i.started_at,
                    i.resolved_at,
                    i.global,
                    i.origin_check,
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
            let existing_updates: std::collections::HashSet<String> = {
                let mut stmt = self
                    .conn
                    .prepare("SELECT id FROM incident_updates WHERE incident_id = ?")?;
                let rows = stmt.query_map(duckdb::params![i.id], |r| r.get::<_, String>(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            for u in &i.updates {
                if existing_updates.contains(&u.id) {
                    continue;
                }
                self.conn.execute(
                    "INSERT INTO incident_updates
                         (id, incident_id, state, body, author, created_at)
                     VALUES (?, ?, ?, ?, ?, ?)",
                    duckdb::params![u.id, i.id, u.state.as_str(), u.body, u.author, u.created_at],
                )?;
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

    /// One incident in the shape it replicates as. `None` when it does not
    /// exist locally.
    pub fn replicated_incident(&self, id: &str) -> Result<Option<ReplicatedIncident>> {
        let Some(row) = self.incident(id)? else {
            return Ok(None);
        };
        let updates = self.updates_for_incident(id)?;
        Ok(Some(ReplicatedIncident {
            id: row.id,
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
    /// against its own. One query per component, each bounded so it stays
    /// cheap on a large table: `window_start` excludes anything older than
    /// the anti-entropy sweep cares about.
    pub fn digest(&self, window_start: i64) -> Result<Digest> {
        let checks = {
            let mut stmt = self.conn.prepare(
                "SELECT check_id, MAX(ts_millis), COUNT(*) FROM check_results
                 WHERE ts_millis >= ?
                 GROUP BY check_id",
            )?;
            let rows = stmt.query_map(duckdb::params![window_start], |row| {
                Ok(CheckDigest {
                    check_id: row.get(0)?,
                    max_ts: row.get(1)?,
                    count: row.get(2)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let max_alert_ts: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(ts_millis), 0) FROM alerts WHERE ts_millis >= ?",
            duckdb::params![window_start],
            |r| r.get(0),
        )?;
        let max_incident_started_at: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(started_at), 0) FROM incidents WHERE started_at >= ?",
            duckdb::params![window_start],
            |r| r.get(0),
        )?;
        let config = [
            ConfigKind::Check,
            ConfigKind::Section,
            ConfigKind::Setting,
            ConfigKind::Credential,
        ]
        .into_iter()
        .map(|k| self.config_digest_for(k))
        .collect::<Result<Vec<_>>>()?;
        Ok(Digest {
            checks,
            window_start,
            max_alert_ts,
            max_incident_started_at,
            config,
        })
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
        let rows = stmt.query_map(duckdb::params![kind.as_str()], |r| {
            Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        let mut count = 0i64;
        let mut max_hlc: Option<Hlc> = None;
        for row in rows {
            let (millis, node) = row?;
            count += 1;
            // NULL here is a row written before stamps existed: the oldest
            // possible HLC, so it never wins a max() against a real one.
            let hlc = Hlc::new(millis.unwrap_or(0), node.unwrap_or_default());
            max_hlc = Some(match max_hlc {
                Some(m) if m >= hlc => m,
                _ => hlc,
            });
        }
        Ok(ConfigDigest {
            kind,
            max_hlc,
            count,
        })
    }

    /// Paged raw rows for backfill. `since` is exclusive and acts as the
    /// cursor; rows come back oldest-first so the caller can advance it.
    ///
    /// `kind`, `body_matched`, `body`, `headers`, `metric` and `metrics` are
    /// never in `check_results` — the table keeps the coordinator's verdict,
    /// not the probe's raw observation — so they read back as their defaults.
    /// The replica gets what was persisted, not what was probed, and that is
    /// the whole point of replicating the observer's verdict rather than
    /// re-evaluating it on every node.
    pub fn results_since(
        &self,
        check_id: &str,
        since: i64,
        until: i64,
        limit: usize,
    ) -> Result<Vec<(CheckResult, Status)>> {
        let mut stmt = self.conn.prepare(
            "SELECT agent_id, ts_millis, status, latency_ms, status_code, error,
                    failure, zone, region
             FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?
             ORDER BY ts_millis ASC, agent_id ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![check_id, since, until, limit as i64],
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
                check_id: check_id.to_owned(),
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

    /// Alerts stamped after `since_ts`, oldest first — the paged backfill
    /// counterpart to `results_since`.
    pub fn alerts_since(&self, since_ts: i64, limit: usize) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts
             WHERE ts_millis > ?
             ORDER BY ts_millis ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![since_ts, limit as i64], Self::alert_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Incidents opened after `since_started_at`, oldest first, each with its
    /// checks and updates — the paged backfill counterpart to
    /// `results_since`.
    pub fn incidents_since(
        &self,
        since_started_at: i64,
        limit: usize,
    ) -> Result<Vec<ReplicatedIncident>> {
        let ids: Vec<String> = {
            let mut stmt = self.conn.prepare(
                "SELECT id FROM incidents WHERE started_at > ?
                 ORDER BY started_at ASC LIMIT ?",
            )?;
            stmt.query_map(duckdb::params![since_started_at, limit as i64], |r| {
                r.get::<_, String>(0)
            })?
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

    // ---- Config replication ----

    /// The stamp a row and/or its tombstone currently carry, whichever is
    /// greater. `None` when neither exists — the id is entirely unknown here.
    fn local_config_stamp(&self, kind: ConfigKind, id: &str) -> Result<Option<Hlc>> {
        let (table, pk) = kind.table_and_pk();
        let row_hlc: Option<Hlc> = {
            let sql = format!("SELECT updated_at, updated_by FROM {table} WHERE {pk} = ?");
            let mut stmt = self.conn.prepare(&sql)?;
            let mut rows = stmt.query_map(duckdb::params![id], |r| {
                Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<String>>(1)?))
            })?;
            // A row whose `updated_at` is NULL was written before stamps
            // existed, or by a path this project hasn't converted yet. It
            // counts as the oldest possible HLC — (0, "") — so anything
            // replicated beats it rather than the merge failing to compare.
            rows.next()
                .transpose()?
                .map(|(millis, node)| Hlc::new(millis.unwrap_or(0), node.unwrap_or_default()))
        };
        let tomb_hlc: Option<Hlc> = {
            let mut stmt = self.conn.prepare(
                "SELECT deleted_at, node_id FROM config_tombstones WHERE kind = ? AND id = ?",
            )?;
            let mut rows = stmt.query_map(duckdb::params![kind.as_str(), id], |r| {
                Ok(Hlc::new(r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?;
            rows.next().transpose()?
        };
        Ok(match (row_hlc, tomb_hlc) {
            (Some(a), Some(b)) => Some(if a >= b { a } else { b }),
            (a, b) => a.or(b),
        })
    }

    fn delete_config_row(&self, kind: ConfigKind, id: &str) -> Result<()> {
        let (table, pk) = kind.table_and_pk();
        self.conn.execute(
            &format!("DELETE FROM {table} WHERE {pk} = ?"),
            duckdb::params![id],
        )?;
        Ok(())
    }

    /// Write a document over the row named `id`, stamped at `hlc`. The only
    /// place any config table is written from a parsed document, so the four
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
                self.upsert_check(&config, hlc.millis)?;
                self.conn.execute(
                    "UPDATE checks SET updated_by = ? WHERE name = ?",
                    duckdb::params![hlc.node_id, id],
                )?;
            }
            ConfigKind::Section => {
                let s: Section = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed section document for {id}: {e}"))?;
                self.conn
                    .execute("DELETE FROM sections WHERE name = ?", duckdb::params![id])?;
                self.conn.execute(
                    "INSERT INTO sections (name, position, collapsed, updated_at, updated_by)
                     VALUES (?, ?, ?, ?, ?)",
                    duckdb::params![s.name, s.position, s.collapsed, hlc.millis, hlc.node_id],
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
        if let Some(local) = self.local_config_stamp(e.kind, &e.id)? {
            if e.hlc <= local {
                return Ok(false);
            }
        }
        self.conn.execute_batch("BEGIN")?;
        let write = || -> Result<()> {
            if e.is_tombstone() {
                self.delete_config_row(e.kind, &e.id)?;
                self.conn.execute(
                    "DELETE FROM config_tombstones WHERE kind = ? AND id = ?",
                    duckdb::params![e.kind.as_str(), e.id],
                )?;
                self.conn.execute(
                    "INSERT INTO config_tombstones (kind, id, deleted_at, node_id)
                     VALUES (?, ?, ?, ?)",
                    duckdb::params![e.kind.as_str(), e.id, e.hlc.millis, e.hlc.node_id],
                )?;
            } else {
                // A malformed document is an error, not a silent skip: the
                // caller logs it, but this must never half-apply a document
                // over a good local row.
                let doc = e.document.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "config entry for {} has no document and is not a tombstone",
                        e.id
                    )
                })?;
                self.apply_config_document(e.kind, &e.id, doc, &e.hlc)?;
                // The edit supersedes any tombstone for this id — leaving one
                // behind would make the next digest sweep look inconsistent
                // (a row that exists, with a tombstone that says it doesn't).
                self.conn.execute(
                    "DELETE FROM config_tombstones WHERE kind = ? AND id = ?",
                    duckdb::params![e.kind.as_str(), e.id],
                )?;
            }
            Ok(())
        };
        match write() {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(true)
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    /// Read back the entry a row would replicate as, for pushing to peers
    /// after a local write. `None` when the row does not exist.
    pub fn config_entry(&self, kind: ConfigKind, id: &str) -> Result<Option<ConfigEntry>> {
        let doc: Option<(String, Option<i64>, Option<String>)> = match kind {
            ConfigKind::Check => {
                let mut stmt = self
                    .conn
                    .prepare("SELECT config, updated_at, updated_by FROM checks WHERE name = ?")?;
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
                    "SELECT name, position, collapsed, updated_at, updated_by
                     FROM sections WHERE name = ?",
                )?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    let section = Section {
                        name: r.get::<_, String>(0)?,
                        position: r.get::<_, i32>(1)?,
                        collapsed: r.get::<_, Option<bool>>(2)?.unwrap_or(false),
                    };
                    Ok((
                        section,
                        r.get::<_, Option<i64>>(3)?,
                        r.get::<_, Option<String>>(4)?,
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

    /// Every entry of a kind, tombstones included, for an anti-entropy sweep.
    pub fn config_entries(&self, kind: ConfigKind) -> Result<Vec<ConfigEntry>> {
        let (table, pk) = kind.table_and_pk();
        let ids: Vec<String> = {
            let sql = format!("SELECT {pk} FROM {table}");
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
        self.conn.execute(
            &format!("UPDATE {table} SET updated_at = ?, updated_by = ? WHERE {pk} = ?"),
            duckdb::params![hlc.millis, hlc.node_id, id],
        )?;
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
    pub fn record_agent_poll(
        &self,
        agent_id: &str,
        zone: &str,
        region: &str,
        at: i64,
        window_ms: i64,
        rtt_ms: Option<i64>,
    ) -> Result<()> {
        let newer_stored: i64 = self.conn.query_row(
            "SELECT count(*) FROM agent_polls WHERE agent_id = ? AND polled_at >= ?",
            duckdb::params![agent_id, at],
            |r| r.get(0),
        )?;
        if newer_stored > 0 {
            return Ok(());
        }
        self.conn.execute(
            "INSERT OR REPLACE INTO agent_polls
                 (agent_id, zone, region, polled_at, window_ms, rtt_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![agent_id, zone, region, at, window_ms, rtt_ms],
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
            "SELECT agent_id, zone, region, polled_at, window_ms, rtt_ms FROM agent_polls",
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
        self.conn.execute_batch("BEGIN")?;
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
                duckdb::params![id, now_millis, node_id],
            )?;
            Ok(removed > 0)
        };
        match write() {
            Ok(removed) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(removed)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Whether an incident id has been deleted anywhere in the cluster.
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
        name: &str,
        now_millis: i64,
        node_id: &str,
    ) -> Result<PurgeSummary> {
        let results = self.purge_check_data(name)?;
        let owned: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT id FROM incidents WHERE origin_check = ?")?;
            stmt.query_map(duckdb::params![name], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let mut incidents = 0u64;
        for id in &owned {
            if self.delete_incident(id, now_millis, node_id)? {
                incidents += 1;
            }
        }
        // Incidents this check merely joined belong to whoever opened them, so
        // they survive; only this check's membership of them goes.
        self.conn.execute(
            "DELETE FROM incident_checks WHERE check_id = ?",
            duckdb::params![name],
        )?;
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

    pub(super) fn result(check_id: &str, ts_millis: i64, latency_ms: u64) -> CheckResult {
        CheckResult {
            zone: "test-zone".into(),
            region: "test-region".into(),
            body: None,
            headers: None,
            check_id: check_id.into(),
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
        let hist = s.history("a", 10, None, None, None).unwrap();
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
        let a = rows.iter().find(|r| r.check_id == "a").unwrap();
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
        assert_eq!(all[0].check_id, "ddns-wa", "1s ago is the newest");

        // Per-column filters narrow correctly.
        let by_check = s
            .recent_results(
                &ResultsFilter {
                    check_id: Some("web".into()),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(by_check.len(), 2);
        assert!(by_check.iter().all(|r| r.check_id == "web"));

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
                    check_id: Some("web".into()),
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
        assert!(capped.iter().all(|r| r.check_id == "ddns-wa"));
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
        let a = rows.iter().find(|r| r.check_id == "a").unwrap();
        assert_eq!(a.status, Status::Up, "latest row is the success");
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
        let hist = s.history("a", 10, None, None, None).unwrap();
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
        let capped = s.history("a", 2, None, None, None).unwrap();
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
                Some(Status::Up),
                Status::Down,
                1000,
                Some("inc-1"),
            )
            .unwrap();
        let b1 = s
            .insert_alert(
                AlertScope::Check,
                "b",
                Some(Status::Down),
                Status::Up,
                2000,
                None,
            )
            .unwrap();
        // A first-ever observation has nothing to move from.
        let a2 = s
            .insert_alert(AlertScope::Check, "a", None, Status::Unknown, 3000, None)
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
                    current_state: Status::Unknown,
                    timestamp: 3000,
                    incident_id: None,
                },
                AlertRow {
                    id: b1,
                    scope: AlertScope::Check,
                    node_id: "b".into(),
                    previous_state: Some(Status::Down),
                    current_state: Status::Up,
                    timestamp: 2000,
                    incident_id: None,
                },
                AlertRow {
                    id: a1,
                    scope: AlertScope::Check,
                    node_id: "a".into(),
                    previous_state: Some(Status::Up),
                    current_state: Status::Down,
                    timestamp: 1000,
                    incident_id: Some("inc-1".into()),
                },
            ]
        );
    }

    #[test]
    fn recent_alerts_caps_to_the_newest_limit() {
        let s = Storage::open(":memory:").unwrap();
        for (node, ts) in [("a", 1000), ("b", 2000), ("a", 3000)] {
            s.insert_alert(
                AlertScope::Check,
                node,
                Some(Status::Up),
                Status::Down,
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
            Some(Status::Up),
            Status::Down,
            1000,
            Some("inc-1"),
        )
        .unwrap();
        // Same instant, different incident: proves membership is the stamp and
        // not the clock.
        s.insert_alert(
            AlertScope::Check,
            "b",
            Some(Status::Up),
            Status::Down,
            1000,
            Some("inc-2"),
        )
        .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(Status::Down),
            Status::Up,
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

        let rows = s.list_status(now).unwrap();
        assert_eq!(rows.len(), 2, "one row per distinct check_id");
        let find = |id: &str| rows.iter().find(|r| r.check_id == id).unwrap().clone();

        let a = find("a");
        assert_eq!(a.status, Status::Down, "latest row wins");
        assert_eq!(a.latency_ms, 50, "latest row's latency");
        assert_eq!(a.last_check, now, "latest row's timestamp");
        assert!(
            (a.uptime_24h - 0.75).abs() < 1e-9,
            "3 UP / 4 in-window, got {}",
            a.uptime_24h
        );

        let b = find("b");
        assert_eq!(b.status, Status::Up);
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
    fn uptime_buckets_windows_filters_and_boundary() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        s.insert_results(&[
            // Bucket 2 (today, (now-DAY, now]): 2 UP + 1 DOWN -> up=2, total=3.
            (result("x", now, 10), Status::Up),
            (result("x", now - 1000, 11), Status::Up),
            (result("x", now - 2000, 12), Status::Down),
            // Bucket 1 ((now-2*DAY, now-DAY]): nothing -> up=0, total=0, uptime=None.
            // Bucket 0 ((now-3*DAY, now-2*DAY]): 1 UP -> up=1, total=1, uptime=Some(1.0).
            (result("x", now - 2 * day - 1000, 13), Status::Up),
            // Boundary: exactly at bucket 0's exclusive lower bound now-3*DAY -> excluded.
            (result("x", now - 3 * day, 14), Status::Up),
            // Decoy check "y" at now: must never count toward "x".
            (result("y", now, 99), Status::Up),
        ])
        .unwrap();

        let buckets = s.uptime_buckets("x", 3, 3, now).unwrap();
        assert_eq!(buckets.len(), 3, "exactly `days` buckets, oldest-first");

        // Bucket 0: oldest day; boundary row at now-3*DAY excluded, one UP remains.
        assert_eq!(buckets[0].start, now - 3 * day);
        assert_eq!(buckets[0].up, 1);
        assert_eq!(
            buckets[0].total, 1,
            "row at exclusive lower bound now-3*DAY excluded"
        );
        assert_eq!(buckets[0].uptime, Some(1.0));

        // Bucket 1: empty middle day.
        assert_eq!(buckets[1].start, now - 2 * day);
        assert_eq!(buckets[1].up, 0);
        assert_eq!(buckets[1].total, 0);
        assert_eq!(buckets[1].uptime, None, "no rows -> uptime None, not 0.0");

        // Bucket 2: today; 2 UP + 1 DOWN; decoy "y" excluded.
        assert_eq!(buckets[2].start, now - day);
        assert_eq!(buckets[2].up, 2);
        assert_eq!(buckets[2].total, 3, "decoy check \"y\" must not count");
        let uptime = buckets[2].uptime.expect("today has data");
        assert!(
            (uptime - 2.0 / 3.0).abs() < 1e-9,
            "2 UP / 3 total, got {uptime}"
        );
    }

    #[test]
    fn uptime_buckets_no_rows_returns_empty_segments() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        let buckets = s.uptime_buckets("absent", 4, 4, now).unwrap();
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
            .latency_series("x", now - 4 * hour, now, 4)
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
            .latency_series("y", now - 4 * hour, now, 4)
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
                .latency_series("young", now - window, now, 120)
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

        let series = s.latency_series("young", now - month, now, 120).unwrap();
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

        let points = s.latency_series("quiet", now - 24 * hour, now, 12).unwrap();
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
            .latency_series("absent", now - 6 * 3_600_000, now, 6)
            .unwrap()
            .points;
        assert!(
            points.iter().all(|p| p.avg_latency_ms.is_none()),
            "no data must yield no measurements, got {points:?}"
        );
    }

    #[test]
    fn latency_series_caps_buckets_at_the_data_resolution() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let min = 60_000i64;

        // The case behind the picket-fence bug: a short window probed every few
        // minutes, asked for far more buckets than there are attempts.
        s.insert_results(&[
            (result("x", now - 12 * min, 40), Status::Up),
            (result("x", now - 8 * min, 50), Status::Up),
            (result("x", now - 4 * min, 60), Status::Up),
            (result("x", now, 70), Status::Up),
        ])
        .unwrap();

        let points = s
            .latency_series("x", now - 16 * min, now, 120)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "one segment per recorded instant, not 120");
        assert!(
            points.iter().all(|p| p.avg_latency_ms.is_some()),
            "capping to the data leaves no empty segments, got {points:?}"
        );
        assert_eq!(
            points
                .iter()
                .filter_map(|p| p.avg_latency_ms)
                .collect::<Vec<_>>(),
            vec![40.0, 50.0, 60.0, 70.0],
            "oldest first, one attempt each"
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
            .latency_series("x", now - 60 * min, now, 4)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "exactly `buckets` points");
        assert_eq!(points[3].avg_latency_ms, Some(30.0), "avg of 40 and 20");
        assert_eq!(points[0].avg_latency_ms, Some(90.0), "avg of 80 and 100");
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
        let series = s.latency_series("web", from, to, 2).unwrap();

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

        // The chart still buckets every attempt, so an outage stays visible as a
        // spike rather than vanishing from the shape.
        assert_eq!(series.points.len(), 2);
        // The series covers the window the caller asked for.
        assert_eq!(
            series.points[0].timestamp, from,
            "oldest segment starts at the requested bound"
        );
        assert_eq!(
            series.points[0].avg_latency_ms,
            Some(1265.0),
            "averages 10, 30, 20 and the 5000ms timeout"
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

        let hist = s.history("web", 10, None, None, None).unwrap();
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
        let newest = s.history("web", 100, None, None, None).unwrap();
        assert_eq!(newest.len(), 100);
        assert!(
            newest.iter().all(|r| r.status == Status::Up),
            "the newest slice hides the failures, which is why the filter exists"
        );

        // Filtering by status surfaces exactly the two failures, with reasons.
        let bad = s
            .history("web", 500, None, None, Some(Status::Down))
            .unwrap();
        assert_eq!(bad.len(), 2, "both failures found regardless of recency");
        assert!(bad.iter().all(|r| r.error.as_deref() == Some("timeout")));

        // And the filter is a filter, not a truncation: successes still queryable.
        let good = s.history("web", 500, None, None, Some(Status::Up)).unwrap();
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
            Some(Status::Up),
            Status::Down,
            now - 1000,
            None,
        )
        .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(Status::Down),
            Status::Up,
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
                Some(check),
                &[IncidentCheckRow {
                    check_id: check.into(),
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
            notifier: "oncall".into(),
            check_id: check.into(),
            previous_state: Some(Status::Up),
            current_state: Status::Down,
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
        assert_eq!(left[0].check_id, "b");

        // The recent result and alert survive.
        let status = s.list_status(now).unwrap();
        assert_eq!(status.len(), 1, "only the recent result's check remains");
        assert_eq!(status[0].check_id, "a");
        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
        // The open incident survives whole; the resolved one is gone entirely.
        let incidents = s.list_incidents(10, None).unwrap();
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
        assert_eq!(s.history("a", 10, None, None, None).unwrap().len(), 2);

        // A batch mixing an old row with a genuinely new one only adds the new one.
        let r3 = result("a", now - 2000, 7);
        let mixed = s
            .insert_results_dedup(&[(r1, Status::Up), (r3, Status::Up)])
            .unwrap();
        assert_eq!(
            mixed, 1,
            "only the row this node did not already have is inserted"
        );
        assert_eq!(s.history("a", 10, None, None, None).unwrap().len(), 3);
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
        assert_eq!(s.history("x", 10, None, None, None).unwrap().len(), 2);
    }

    #[test]
    fn upsert_alert_is_idempotent_by_id() {
        let s = Storage::open(":memory:").unwrap();
        let a = AlertRow {
            id: "fixed-id".into(),
            scope: AlertScope::Check,
            node_id: "web".into(),
            previous_state: Some(Status::Up),
            current_state: Status::Down,
            timestamp: 1000,
            incident_id: Some("inc-1".into()),
        };
        s.upsert_alert(&a).unwrap();
        s.upsert_alert(&a).unwrap();
        assert_eq!(
            s.recent_alerts(10).unwrap().len(),
            1,
            "a replicated alert arriving twice is one row, not two"
        );
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

    #[test]
    fn config_entry_round_trips_each_kind_through_apply_and_read_back() {
        let s = Storage::open(":memory:").unwrap();
        let hlc = Hlc::new(1_000, "n1");

        let check = check_config("web", "https://example.com");
        let e = ConfigEntry {
            kind: ConfigKind::Check,
            id: "web".into(),
            hlc: hlc.clone(),
            document: Some(serde_json::to_string(&check).unwrap()),
        };
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s.config_entry(ConfigKind::Check, "web").unwrap().unwrap();
        assert_eq!(back.hlc, hlc);
        let round: CheckConfig = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, check);

        let section = Section {
            name: "core".into(),
            position: 3,
            collapsed: true,
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

        // Every kind now has exactly its one applied entry, no tombstones.
        for kind in [
            ConfigKind::Check,
            ConfigKind::Section,
            ConfigKind::Setting,
            ConfigKind::Credential,
        ] {
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
            title: "Outage".into(),
            state: IncidentState::Detected,
            started_at: 100,
            resolved_at: None,
            global: false,
            origin_check: Some("a".into()),
            checks: vec![("a".into(), AffectSource::Origin)],
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
        renamed.title = "Major outage".into();
        s.upsert_incident(&renamed).unwrap();
        let stored = s.incident("inc-1").unwrap().unwrap();
        assert_eq!(stored.title, "Major outage");
        assert_eq!(
            s.list_incidents(10, None).unwrap().len(),
            1,
            "still one incident row, not a second one"
        );
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

    // ---- Clustering: paged backfill ----

    #[test]
    fn since_readers_page_oldest_first_and_exclude_the_cursor() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_results(&[
            (result("a", 1000, 1), Status::Up),
            (result("a", 2000, 2), Status::Down),
        ])
        .unwrap();
        let page = s.results_since("a", 1000, 5000, 10).unwrap();
        assert_eq!(page.len(), 1, "since is exclusive of the cursor");
        assert_eq!(page[0].0.timestamp.timestamp_millis(), 2000);
        assert_eq!(page[0].1, Status::Down);
        // Fields never persisted in `check_results` read back empty/None
        // rather than being invented.
        assert_eq!(page[0].0.body, None);
        assert_eq!(page[0].0.headers, None);

        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(Status::Up),
            Status::Down,
            2000,
            None,
        )
        .unwrap();
        let alerts = s.alerts_since(1000, 10).unwrap();
        assert_eq!(alerts.len(), 1);

        s.create_incident(
            "inc-1",
            "t",
            IncidentState::Detected,
            2000,
            false,
            Some("a"),
            &[IncidentCheckRow {
                check_id: "a".into(),
                source: AffectSource::Origin,
            }],
            None,
        )
        .unwrap();
        let incidents = s.incidents_since(1000, 10).unwrap();
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].id, "inc-1");
    }

    // ---- Clustering: digest ----

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
            Some(Status::Up),
            Status::Down,
            now - 1000,
            None,
        )
        .unwrap();
        s.insert_alert(AlertScope::Check, "a", None, Status::Up, now - 90_000, None)
            .unwrap();

        s.create_incident(
            "inc-1",
            "t",
            IncidentState::Detected,
            now - 2000,
            false,
            Some("a"),
            &[IncidentCheckRow {
                check_id: "a".into(),
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
            Some("b"),
            &[],
            None,
        )
        .unwrap();

        let window_start = now - 10_000;
        let d = s.digest(window_start).unwrap();

        let a_digest = d.checks.iter().find(|c| c.check_id == "a").unwrap();
        assert_eq!(a_digest.count, 2);
        assert_eq!(a_digest.max_ts, now - 1000);
        assert!(
            d.checks.iter().all(|c| c.check_id != "b"),
            "the out-of-window check is excluded entirely"
        );

        assert_eq!(
            d.max_alert_ts,
            now - 1000,
            "the older alert is outside the window"
        );
        assert_eq!(
            d.max_incident_started_at,
            now - 2000,
            "the older incident is outside the window"
        );
    }

    // ---- Clustering: fleet health ----

    #[test]
    fn agent_polls_are_recorded_per_agent_and_replace_on_re_poll() {
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll("agent-a", "z1", "r1", 1000, 90_000, Some(20))
            .unwrap();
        s.record_agent_poll("agent-a", "z1", "r1", 2000, 90_000, Some(15))
            .unwrap();
        s.record_agent_poll("agent-b", "z2", "r2", 1500, 30_000, None)
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
        s.record_agent_poll("agent-a", "z1", "r1", 5000, 90_000, Some(11))
            .unwrap();
        // A peer's batch arriving late. Replication is unordered, so this is
        // the normal case rather than an edge one.
        s.record_agent_poll("agent-a", "z1", "r1", 1000, 90_000, Some(99))
            .unwrap();

        let polls = s.list_agent_polls().unwrap();
        assert_eq!(polls.len(), 1);
        assert_eq!(polls[0].at, 5000, "the stale row lost");
        assert_eq!(polls[0].rtt_ms, Some(11));
    }

    #[test]
    fn purging_an_agent_drops_its_poll_row() {
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll("agent-a", "z1", "r1", 1000, 90_000, None)
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
            Some(check),
            &[IncidentCheckRow {
                check_id: check.into(),
                source: AffectSource::Origin,
            }],
            Some(("down", None)),
        )
        .unwrap();
        s.upsert_alert(&AlertRow {
            id: format!("{id}-a1"),
            scope: AlertScope::Check,
            node_id: check.into(),
            previous_state: Some(Status::Up),
            current_state: Status::Down,
            timestamp: 1000,
            incident_id: Some(id.into()),
        })
        .unwrap();
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
            title: "Outage".into(),
            state: IncidentState::Detected,
            started_at: 1000,
            resolved_at: None,
            global: false,
            origin_check: Some("web".into()),
            checks: vec![("web".into(), AffectSource::Origin)],
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
        s.attach_check("inc-api", "web", AffectSource::Dependency)
            .unwrap();
        s.insert_results(&[(result("web", 1000, 5), Status::Up)])
            .unwrap();

        let summary = s.purge_check_everything("web", 4000, "node-a").unwrap();
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
        assert_eq!(api.checks[0].check_id, "api");
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
        let i = ReplicatedIncident {
            id: "inc-1".into(),
            title: "Outage".into(),
            state: IncidentState::Investigating,
            started_at: 10,
            resolved_at: None,
            global: false,
            origin_check: Some("web".into()),
            checks: vec![("web".into(), AffectSource::Origin)],
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
            r#"{"id":"inc-1","title":"Outage","state":"investigating","started_at":10,"resolved_at":null,"global":false,"origin_check":"web","checks":[["web","origin"]],"updates":[{"id":"inc-1-u10","incident_id":"inc-1","state":"detected","body":"down","author":null,"created_at":10}]}"#
        );
    }

    #[test]
    fn document_encodings_are_pinned() {
        let section = Section {
            name: "core".into(),
            position: 2,
            collapsed: true,
        };
        assert_eq!(
            serde_json::to_string(&section).unwrap(),
            r#"{"name":"core","position":2,"collapsed":true}"#
        );

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
                check_id: check_id.into(),
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
            let b = s.uptime_buckets("x", days, 90, now).unwrap();
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
    fn a_result_lands_in_the_segment_covering_its_timestamp() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        const DAY: i64 = 86_400_000;
        // 7 days / 90 segments => 6720000 ms each.
        let width = 7 * DAY / 90;

        // One result inside the newest segment, one inside the oldest.
        s.insert_results(&[up_at("x", now - 1), up_at("x", now - 90 * width + 1)])
            .unwrap();

        let b = s.uptime_buckets("x", 7, 90, now).unwrap();
        assert_eq!(b[89].total, 1, "newest segment holds the recent result");
        assert_eq!(b[0].total, 1, "oldest segment holds the old result");
        let empty = b[1..89].iter().map(|x| x.total).sum::<u32>();
        assert_eq!(empty, 0, "nothing leaks into the segments between");
        assert_eq!(b[45].uptime, None, "an empty segment is no-data, not 0%");
    }
}

#[cfg(test)]
mod migration_tests {
    use super::tests::result;
    use super::*;

    /// Opening a database written by an older build must migrate it, not
    /// crash-loop. `:memory:` can never catch this: a fresh database is created
    /// with the current schema, so the ALTER path is only reachable against a
    /// file that predates the new columns. This is exactly how a half-applied
    /// migration reached a running deployment.
    #[test]
    fn a_database_from_before_visible_and_eval_is_migrated_on_open() {
        let dir = std::env::temp_dir().join(format!("warden-migration-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("old.duckdb");
        let path_str = path.to_str().unwrap().to_string();
        let _ = std::fs::remove_file(&path);

        // The `checks` table exactly as the previous release wrote it.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE checks (
                    name        VARCHAR PRIMARY KEY,
                    kind        VARCHAR NOT NULL,
                    target      VARCHAR NOT NULL,
                    interval_secs BIGINT NOT NULL,
                    timeout_secs  BIGINT NOT NULL,
                    group_name  VARCHAR,
                    latency_graph BOOLEAN NOT NULL,
                    enabled     BOOLEAN NOT NULL,
                    expect_status INTEGER,
                    expect_body_contains VARCHAR,
                    updated_at  BIGINT  NOT NULL
                );
                INSERT INTO checks VALUES
                    ('legacy', 'http', 'https://legacy.test', 30, 10, 'Old', true, true, 200, NULL, 0);",
            )
            .unwrap();
        }

        let store = Storage::open(&path_str).expect("opening an older database must migrate it");
        let checks = store.list_checks().expect("reading migrated rows");
        let legacy = checks
            .iter()
            .find(|c| c.name == "legacy")
            .expect("row survives");
        assert!(
            legacy.visible,
            "a row that predates `visible` must default to shown, not hidden"
        );
        assert_eq!(legacy.eval, None);
        assert_eq!(legacy.group.as_deref(), Some("Old"));

        // Sections are new too, and must be adopted from existing group names.
        store.seed_sections_from_checks().unwrap();
        let sections = store.list_sections().unwrap();
        assert!(
            sections.iter().any(|s| s.name == "Old"),
            "existing group names should become sections, got {sections:?}"
        );

        // Opening again must be a no-op rather than a duplicate-column error.
        drop(store);
        Storage::open(&path_str).expect("re-opening a migrated database must succeed");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Helper: open an incident the way the coordinator does, with the check as
    /// its origin and an opening update.
    fn open_incident(s: &Storage, check: &str, at: i64) -> IncidentRow {
        s.create_incident(
            &format!("{check}-{at}"),
            &format!("{check} is down"),
            IncidentState::Detected,
            at,
            false,
            Some(check),
            &[IncidentCheckRow {
                check_id: check.into(),
                source: AffectSource::Origin,
            }],
            Some(("connection refused", None)),
        )
        .unwrap()
    }

    #[test]
    fn incident_lifecycle_open_and_resolve() {
        let s = Storage::open(":memory:").unwrap();

        assert_eq!(s.open_incident_for("svc").unwrap(), None);

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
            s.open_incident_for("svc").unwrap().map(|i| i.id),
            Some("svc-1000".to_string())
        );

        // Resolving is an update like any other, and it records when.
        s.add_update("svc-1000", IncidentState::Resolved, "back", None, 2_000)
            .unwrap();
        let resolved = s.incident("svc-1000").unwrap().unwrap();
        assert_eq!(resolved.state, IncidentState::Resolved);
        assert_eq!(resolved.resolved_at, Some(2_000));
        assert_eq!(
            s.open_incident_for("svc").unwrap(),
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
        s.attach_check("db-1000", "api", AffectSource::Dependency)
            .unwrap();
        s.attach_check("db-1000", "web", AffectSource::Manual)
            .unwrap();

        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .map(|c| (c.check_id.as_str(), c.source))
                .collect::<Vec<_>>(),
            vec![
                ("api", AffectSource::Dependency),
                ("db", AffectSource::Origin),
                ("web", AffectSource::Manual),
            ]
        );

        // Only the origin and the dependency gate auto-resolve.
        assert_eq!(row.gating_checks().collect::<Vec<_>>(), vec!["api", "db"]);

        // Re-attaching the origin as something else must not demote it.
        s.attach_check("db-1000", "db", AffectSource::Manual)
            .unwrap();
        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .find(|c| c.check_id == "db")
                .map(|c| c.source),
            Some(AffectSource::Origin),
            "the cause of the incident cannot be reclassified"
        );

        // Replacing the manual set leaves origin and dependency alone.
        s.set_manual_checks("db-1000", &["cdn".to_string()])
            .unwrap();
        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .map(|c| (c.check_id.as_str(), c.source))
                .collect::<Vec<_>>(),
            vec![
                ("api", AffectSource::Dependency),
                ("cdn", AffectSource::Manual),
                ("db", AffectSource::Origin),
            ],
            "only the manual attachment was replaced"
        );
    }

    /// The up-side mirror of `failures_before_down`. A check needs a full run
    /// of UP observations, so a flapping check never clears the bar and stays
    /// inside one incident.
    #[test]
    fn recovered_for_requires_a_full_run_of_up_observations() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_results(&[(result("svc", 1_000, 1), Status::Down)])
            .unwrap();
        assert!(!s.recovered_for("svc", 1).unwrap(), "still down");

        s.insert_results(&[(result("svc", 2_000, 1), Status::Up)])
            .unwrap();
        assert!(
            s.recovered_for("svc", 1).unwrap(),
            "one UP clears a run of 1"
        );
        assert!(
            !s.recovered_for("svc", 3).unwrap(),
            "one UP does not clear a run of 3"
        );

        s.insert_results(&[
            (result("svc", 3_000, 1), Status::Up),
            (result("svc", 4_000, 1), Status::Up),
        ])
        .unwrap();
        assert!(
            s.recovered_for("svc", 3).unwrap(),
            "three in a row clears it"
        );

        // A flap inside the window puts it back: the run has to be unbroken.
        s.insert_results(&[(result("svc", 5_000, 1), Status::Down)])
            .unwrap();
        assert!(
            !s.recovered_for("svc", 3).unwrap(),
            "a DOWN inside the window breaks the run"
        );
    }

    #[test]
    fn incidents_are_listed_newest_first_and_filterable() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "a", 1_000);
        open_incident(&s, "b", 2_000);
        s.add_update("a-1000", IncidentState::Resolved, "back", None, 3_500)
            .unwrap();

        let all = s.list_incidents(100, None).unwrap();
        let order: Vec<String> = all.iter().map(|i| i.id.clone()).collect();
        assert_eq!(order, vec!["b-2000", "a-1000"]);

        let open = s.list_incidents(100, Some(true)).unwrap();
        assert_eq!(
            open.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["b-2000"],
            "only b is still open"
        );

        let resolved = s.list_incidents(100, Some(false)).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].id, "a-1000");

        let for_a = s.incidents_for_check("a", 100).unwrap();
        assert_eq!(
            for_a.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["a-1000"]
        );

        // A check attached as a dependency is found by the same lookup: the
        // blast radius is what "this check's incidents" means, not the origin.
        s.attach_check("b-2000", "a", AffectSource::Dependency)
            .unwrap();
        let for_a = s.incidents_for_check("a", 100).unwrap();
        assert_eq!(
            for_a.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["b-2000", "a-1000"]
        );
    }
}
