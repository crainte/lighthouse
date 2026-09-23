pub mod alert;
pub mod backup;
pub mod cluster;
pub mod database;
mod fleet;
pub mod gossip;
pub mod hlc;
mod replication_out;
mod routes;
pub mod state;
pub mod storage;

use crate::config::{CheckConfig, CoordinatorConfig};
use crate::configplane::{self, ConfigBatch};
use crate::credential::AgentCredential;
use crate::models::{eval, CheckKind, CheckResult, NodeState, Status};
use crate::rollup::{rollup, RollupPolicy};
use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use database::Database;
use fleet::{fleet_rows, unreachable_after_ms, FleetMemberDto, FleetRowDto};
use futures_util::{stream, StreamExt};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use routes::agents::{
    agent_checks_handler, agents_handler, ingest_handler, list_agent_credentials_handler,
    mint_agent_credential_handler, purge_agent_handler, revoke_agent_credential_handler,
};
use routes::checks::{
    create_check_handler, delete_check_handler, eval_test_handler, heartbeat_handler,
    list_checks_handler, purge_check_data_handler, purge_check_everything_handler,
    reorder_checks_handler, unplace_check_handler, update_check_handler, CheckResponse,
};
use routes::incidents::{
    check_incidents_handler, create_incident_handler, delete_incident_handler,
    edit_incident_handler, incident_checks_handler, incident_handler, incident_update_handler,
    incidents_handler, purge_results_handler, purge_short_incidents_handler, results_count_handler,
};
pub use routes::incidents::{IncidentCheckDto, IncidentDto, IncidentUpdateDto};
use routes::peer::{
    alerts_backfill_handler, backfill_handler, cluster_ping_handler, cluster_view_handler,
    config_entries_handler, config_handler, digest_handler, global_check_ids_handler,
    global_config_digest_handler, global_config_handler, gossip_handler,
    incident_tombstones_backfill_handler, incidents_backfill_handler,
    latency_hour_backfill_handler, latency_minute_backfill_handler, raw_data_gaps_handler,
    read_config_entry_for_replication, replicate_handler, results_backfill_handler, RollupBatch,
    RollupRowDto,
};
pub use routes::peer::{
    CheckDigestDto, DigestResponse, ReplicatedAlert, ReplicatedBelief, ReplicatedResult,
    ReplicationBatch, RollupDigestDto, StreamDigestDto,
};
use routes::policies::{
    delete_rollup_policy_handler, list_rollup_policies_handler, region_tree_handler,
    upsert_rollup_policy_handler,
};
use routes::public::{
    alerts_handler, history_handler, latency_points_handler, latency_sparkline_handler,
    results_handler, status_handler, uptime_handler, uptime_strips_handler,
};
use routes::sections::{
    create_section_handler, delete_section_handler, list_sections_handler, public_sections_handler,
    rename_section_handler, reorder_sections_handler,
};
use routes::session::{login_handler, logout_handler, session_handler};
use routes::settings::{
    agents_display_handler, maintenance_handler, notifiers_handler, test_notifier_handler,
    update_agents_display_handler, update_maintenance_handler, update_notifiers_handler,
};
use serde::Serialize;
use state::StateTracker;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use storage::Storage;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// Stay well below `/internal/config`'s 8 MiB body limit so JSON framing and a
/// future field cannot turn a bulk edit into a rejected replication request.
const MAX_CONFIG_BATCH_BYTES: usize = 6 * 1024 * 1024;
const CONFIG_APPLY_CHUNK: usize = 1_000;
const TIMING_REPORT_EVERY: u64 = 128;

#[derive(Default)]
struct TimingCounter {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl TimingCounter {
    fn observe(
        &self,
        component: &'static str,
        operation: &'static str,
        elapsed: std::time::Duration,
    ) {
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.total_us.fetch_add(micros, Ordering::Relaxed);
        self.max_us.fetch_max(micros, Ordering::Relaxed);
        let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(TIMING_REPORT_EVERY) {
            tracing::info!(
                target: "warden_bench",
                component,
                operation,
                samples = count,
                total_us = self.total_us.load(Ordering::Relaxed),
                max_us = self.max_us.load(Ordering::Relaxed),
                "aggregated timing"
            );
        }
    }
}

struct Timings {
    enabled: bool,
    prune_duration_us: AtomicU64,
    local_state_wait: TimingCounter,
    local_state_hold: TimingCounter,
    replicated_state_wait: TimingCounter,
    replicated_state_hold: TimingCounter,
    anti_entropy_page: TimingCounter,
    broadcast_network: TimingCounter,
    broadcast_failures: TimingCounter,
    /// How long a coalesced batch waited behind the peer's in-flight request.
    broadcast_queue_wait: TimingCounter,
}

impl Timings {
    fn new() -> Self {
        Self {
            enabled: std::env::var_os("WARDEN_BENCH_TIMINGS").as_deref()
                == Some(std::ffi::OsStr::new("1"))
                || std::env::var("RUST_LOG").is_ok_and(|filter| filter.contains("warden_bench")),
            prune_duration_us: AtomicU64::new(0),
            local_state_wait: TimingCounter::default(),
            local_state_hold: TimingCounter::default(),
            replicated_state_wait: TimingCounter::default(),
            replicated_state_hold: TimingCounter::default(),
            anti_entropy_page: TimingCounter::default(),
            broadcast_network: TimingCounter::default(),
            broadcast_failures: TimingCounter::default(),
            broadcast_queue_wait: TimingCounter::default(),
        }
    }
}

/// A live event broadcast to WebSocket clients. Two families: plain state
/// changes (matching the persisted alert shape) and incident lifecycle events,
/// so the UI can keep both in sync without polling. Serialized form matches the
/// dashboard API contract.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsEvent {
    StateChange {
        check_id: crate::models::CheckId,
        from: NodeState,
        to: NodeState,
        timestamp: i64,
    },
    /// A check confirmed DOWN and a new incident opened.
    IncidentOpened { incident: IncidentDto },
    /// An incident changed in any way after opening — a check joined it, an
    /// operator posted an update, it resolved. One event rather than one per
    /// verb: the payload is the whole incident, so the UI replaces its copy
    /// and does not have to model each transition twice.
    IncidentUpdated { incident: IncidentDto },
    /// An incident was deleted, here or on a peer. Carries the id alone rather
    /// than the incident: there is nothing left to send, and every consumer
    /// only needs to drop the row it already has.
    IncidentDeleted { id: String },
    /// A peer coordinator's believed liveness changed, as decided by
    /// `Cluster::sweep`. Fleet-health infrastructure, not a check transition —
    /// distinct from `StateChange` so the UI never has to guess which kind of
    /// node an id names.
    ClusterChange {
        node_id: String,
        state: cluster::MemberState,
    },
}

/// Shared coordinator state for axum handlers.
#[derive(Clone)]
pub struct Coordinator {
    inner: Arc<Inner>,
}

/// Test-only control over notification dispatch. Holding this guard prevents a
/// queued wake-up from scanning the durable notification outbox.
#[doc(hidden)]
pub struct NotificationDispatchGuard<'a> {
    coordinator: &'a Coordinator,
    _guard: tokio::sync::MutexGuard<'a, ()>,
}

impl NotificationDispatchGuard<'_> {
    /// Scan the outbox while its normal dispatcher is paused.
    #[doc(hidden)]
    pub async fn retry_due(&self, now_millis: i64) -> usize {
        self.coordinator
            .retry_due_unlocked(now_millis, &reqwest::Client::new())
            .await
    }
}

/// Configured metadata for a check, looked up by its immutable id. The store
/// only knows check ids, so anything declared in config (grouping,
/// expectations, probe type) has to be joined back in here.
#[derive(Clone)]
struct CheckMeta {
    /// The mutable display name, carried here because everything that needs
    /// human-facing text (incident titles, notification bodies) only has the
    /// id to look the check up by.
    name: String,
    kind: crate::models::CheckKind,
    group: Option<String>,
    latency_graph: bool,
    expect_status: Option<u16>,
    /// When the probe runs, as cron. The target lives in storage and is served
    /// only to authenticated admins and agents, never on the public endpoint.
    cron: String,
    /// Published on the public status page.
    visible: bool,
    /// Lua deciding UP/DOWN, replacing the built-in rule when present.
    eval: Option<String>,
    /// Display order within the section.
    position: i32,
    /// What the check reads as before its first result lands.
    default_status: Status,
    /// How many consecutive DOWN observations confirm an outage. Fewer than
    /// this leaves the check reading as it did before, so a single dropped
    /// probe does not page anyone.
    failures_before_down: u32,
    /// Probing at all. A disabled check has no agent expectation to violate.
    enabled: bool,
    /// Whether a confirmed state change on this check reaches the notifiers.
    notify: bool,
    /// Notifiers by name. Empty routes to whichever are marked default.
    notifiers: Vec<String>,
    /// Agents named explicitly. Empty means every agent.
    agents: Vec<String>,
    /// Upstream checks. A DOWN here joins an upstream's open incident rather
    /// than opening its own. One hop only — see `CheckConfig::depends_on`.
    depends_on: Vec<crate::models::CheckId>,
    rollup: crate::config::RollupOverrides,
    min_incident_secs: u64,
    min_recovery_secs: u64,
    notify_degraded: bool,
    // Retained as the effective per-check SLA interpretation for consumers that
    // do not have the original CheckConfig override available.
    #[allow(dead_code)]
    degraded_is_downtime: bool,
}

/// Apply a check's Lua script to one result.
///
/// A broken script must not silently invent a verdict: it is reported as
/// `Unknown` with the reason attached to the result, so the dashboard shows
/// "why" instead of an unexplained outage.
fn script_verdict(source: &str, r: &mut CheckResult) -> Status {
    let input = crate::script::EvalInput {
        status: r.status_code.unwrap_or(0),
        latency_ms: r.latency_ms,
        body: r.body.clone(),
        headers: r.headers.clone().unwrap_or_default(),
        metric: r.metric,
        metrics: r.metrics.clone().unwrap_or_default(),
    };
    match crate::script::evaluate(source, &input) {
        Ok(outcome) => {
            if let Some(override_ms) = outcome.latency_ms {
                r.latency_ms = override_ms;
            }
            if outcome.up {
                Status::Up
            } else {
                Status::Down
            }
        }
        Err(e) => {
            warn!(check = %r.check_id, error = %e, "eval script failed");
            r.error = Some(format!("eval script failed: {e}"));
            r.failure = Some(crate::models::FailureKind::ScriptFailed);
            Status::Unknown
        }
    }
}

/// What an incident records as its cause: the failure kind when one is known,
/// otherwise the probe's own words.
fn incident_details(r: &CheckResult) -> Option<String> {
    r.failure.map(|f| f.summary()).or_else(|| r.error.clone())
}

fn incident_title(
    check_id: &str,
    state: NodeState,
    failing_regions: &std::collections::BTreeSet<String>,
) -> String {
    if state == NodeState::Degraded {
        let regions = failing_regions
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        if regions.is_empty() {
            format!("{check_id} partial outage")
        } else {
            format!("{check_id} partial outage in {regions}")
        }
    } else {
        format!("{check_id} is {}", state.as_str())
    }
}

fn region_change_narrative(
    previous: &std::collections::BTreeSet<String>,
    current: &std::collections::BTreeSet<String>,
) -> String {
    let list = |regions: &std::collections::BTreeSet<String>| {
        if regions.is_empty() {
            "none".to_string()
        } else {
            regions.iter().cloned().collect::<Vec<_>>().join(", ")
        }
    };
    format!(
        "Affected failing regions changed from {} to {}.",
        list(previous),
        list(current)
    )
}

/// A span of milliseconds as human prose, for the auto-written resolve update.
/// Coarse on purpose: "4 minutes" is what a status page says, not "4m 12s".
fn format_duration(ms: i64) -> String {
    let mins = ms.max(0) / 60_000;
    match mins {
        0 => "less than a minute".to_string(),
        1 => "1 minute".to_string(),
        m if m < 60 => format!("{m} minutes"),
        m if m < 120 => format!("1 hour {} minutes", m % 60),
        m if m < 1440 => format!("{} hours {} minutes", m / 60, m % 60),
        m => format!("{} days {} hours", m / 1440, (m % 1440) / 60),
    }
}

/// Derive the in-memory fields consumers need from one persisted check.
fn check_meta(c: &CheckConfig, defaults: crate::config::RollupDefaults) -> CheckMeta {
    CheckMeta {
        name: c.name.clone(),
        kind: c.kind,
        group: c.group.clone(),
        latency_graph: c.latency_graph,
        expect_status: c.expect.status,
        cron: c.cron.clone(),
        visible: c.visible,
        eval: c.eval.clone(),
        position: c.position,
        failures_before_down: c.failures_before_down,
        enabled: c.enabled,
        notify: c.notify,
        notifiers: c.notifiers.clone(),
        agents: c.agents.clone(),
        depends_on: c.depends_on.clone(),
        default_status: match c.default_status {
            crate::config::DefaultStatus::Up => Status::Up,
            crate::config::DefaultStatus::Down => Status::Down,
            crate::config::DefaultStatus::Unknown => Status::Unknown,
        },
        rollup: c.rollup,
        min_incident_secs: c.min_incident_secs.unwrap_or(defaults.min_incident_secs),
        min_recovery_secs: c.min_recovery_secs.unwrap_or(defaults.min_recovery_secs),
        notify_degraded: c.notify_degraded.unwrap_or(defaults.notify_degraded),
        degraded_is_downtime: c
            .degraded_is_downtime
            .unwrap_or(defaults.degraded_is_downtime),
    }
}

/// Index config rows by check id for the lookups ingest and the status page do.
fn meta_by_id(
    checks: &[CheckConfig],
    defaults: crate::config::RollupDefaults,
) -> std::collections::HashMap<crate::models::CheckId, CheckMeta> {
    checks
        .iter()
        .map(|check| (check.id, check_meta(check, defaults)))
        .collect()
}

#[derive(Debug)]
struct RollupCandidate {
    root: NodeState,
    intermediates: Vec<IntermediateState>,
    failing_regions: std::collections::BTreeSet<String>,
}

#[derive(Debug)]
pub(in crate::coordinator) struct RollupTree {
    root: NodeState,
    global_policy: RollupPolicy,
    regions: Vec<RollupRegion>,
}

#[derive(Debug)]
pub(in crate::coordinator) struct RollupRegion {
    name: String,
    state: NodeState,
    policy: RollupPolicy,
    zones: Vec<RollupZone>,
}

#[derive(Debug)]
pub(in crate::coordinator) struct RollupZone {
    name: String,
    state: NodeState,
    policy: RollupPolicy,
    agents: Vec<RollupAgent>,
}

#[derive(Debug)]
pub(in crate::coordinator) struct RollupAgent {
    id: String,
    state: NodeState,
    staleness_ms: i64,
}

#[derive(Debug)]
struct IntermediateState {
    scope: storage::AlertScope,
    node_id: String,
    state: NodeState,
}

struct StartupData {
    reported_fleet: Vec<storage::AgentRow>,
    status_agents: crate::config::AgentsDisplay,
    notifiers: Vec<crate::notify::Notifier>,
    maintenance: Vec<crate::maintenance::MaintenanceWindow>,
    checks: Vec<CheckConfig>,
    node_id: String,
    session_key: Vec<u8>,
    cluster_members: Vec<cluster::Member>,
    credentials: Vec<AgentCredential>,
    agent_states: Vec<storage::AgentCheckStateRow>,
    current_states: Vec<storage::CurrentStateRow>,
    rollup_policies: Vec<storage::RollupPolicyRow>,
}

/// A position in a peer's ordered alert/incident/tombstone stream, plus
/// whether the walk that produced it reached the peer's tail.
///
/// The distinction exists because a stream can fail to converge for reasons
/// backfill cannot fix — an immutable row that differs between replicas is
/// refused rather than overwritten, so the digests disagree permanently. A
/// cursor discarded on reaching the tail would restart that stream from the
/// beginning on every sweep and replay the peer's entire history forever.
/// Keeping the tail costs one empty page instead. The full rescan that would
/// find a row inserted *below* the tail still happens, at
/// `STREAM_RESCAN_INTERVAL` rather than at the sweep interval.
#[derive(Clone)]
struct StreamCursor {
    position: (i64, String),
    completed_at: Option<Instant>,
}

impl StreamCursor {
    fn walking(position: (i64, String)) -> Self {
        Self {
            position,
            completed_at: None,
        }
    }

    fn completed(position: (i64, String)) -> Self {
        Self {
            position,
            completed_at: Some(Instant::now()),
        }
    }

    /// A completed walk is worth repeating from the beginning only once the
    /// backoff has elapsed; until then, resume from the tail.
    fn stale(&self) -> bool {
        self.completed_at
            .is_some_and(|at| at.elapsed() >= STREAM_RESCAN_INTERVAL)
    }
}

/// Whether `theirs` might hold a rollup bucket, or a fuller total for one,
/// that `ours` does not.
///
/// A rollup digest has no single scalar position to compare the way an
/// append-only stream's `(max_millis, max_id)` does — see
/// `storage::RollupDigest` — so there is no one field whose ordering settles
/// the question. Instead: if any field of `theirs` exceeds the same field of
/// `ours`, this side is missing something (a whole bucket if `count` is
/// ahead, more of a bucket's raw detail otherwise) and a pull is worth
/// making. A false positive here — pulling when nothing was actually
/// missing — costs one page that `apply_minute_rollups`/`apply_hour_rollups`
/// merge as a no-op; a false negative would leave a real gap unrecovered, so
/// this leans toward pulling.
fn rollup_behind(theirs: &RollupDigestDto, ours: &RollupDigestDto) -> bool {
    theirs.count > ours.count
        || theirs.max_bucket_start > ours.max_bucket_start
        || theirs.attempts_sum > ours.attempts_sum
        || theirs.up_sum > ours.up_sum
        || theirs.latency_sum_sum > ours.latency_sum_sum
}

#[derive(Default)]
struct RepairCursors {
    /// Cursors are meaningful only for the peer that produced them. Switching
    /// peers restarts from the beginning: a cursor into one peer's ordered
    /// history can skip rows unique to another peer.
    peer: Option<String>,
    results: Option<(i64, String, String)>,
    alerts: Option<StreamCursor>,
    incidents: Option<StreamCursor>,
    incident_tombstones: Option<StreamCursor>,
    /// Rollup backfill cursors. The position packs `check_id` and `agent_id`
    /// into the `String` half of `StreamCursor::position` as `"id\0agent"`,
    /// the same trick `incident_tombstones` uses for its own two-part id — a
    /// rollup bucket's real key is `(bucket_start, check_id, agent_id)`, and
    /// this avoids giving `StreamCursor` a third field just for these two
    /// streams.
    latency_minute_rollups: Option<StreamCursor>,
    latency_hour_rollups: Option<StreamCursor>,
    /// Digest observed when the cursors were last used. Retention can make a
    /// cursor point past a peer's new history, so a lower snapshot restarts it.
    checks_snapshot: Vec<CheckDigestDto>,
    alerts_snapshot: Option<(i64, String, i64)>,
    incidents_snapshot: Option<(i64, String, i64)>,
    tombstones_snapshot: Option<(i64, String, i64)>,
    latency_minute_snapshot: Option<(i64, i64, i64, i64, i64)>,
    latency_hour_snapshot: Option<(i64, i64, i64, i64, i64)>,
}

impl RepairCursors {
    fn for_peer(&mut self, peer: &str) {
        if self.peer.as_deref() != Some(peer) {
            *self = Self {
                peer: Some(peer.to_string()),
                ..Self::default()
            };
        }
    }

    /// Whether a repair is mid-walk, which pins the next sweep to the same
    /// peer. A cursor parked at a peer's tail is explicitly not in progress:
    /// pinning on one would stop the sweep rotating between peers for as long
    /// as a stream stayed divergent.
    fn in_progress(&self) -> bool {
        self.results.is_some()
            || [
                &self.alerts,
                &self.incidents,
                &self.incident_tombstones,
                &self.latency_minute_rollups,
                &self.latency_hour_rollups,
            ]
            .into_iter()
            .any(|c| c.as_ref().is_some_and(|c| c.completed_at.is_none()))
    }

    fn refresh_digest(&mut self, digest: &DigestResponse) {
        let stream = |d: &StreamDigestDto| (d.max_millis, d.max_id.clone(), d.count);
        let decreased = |before: &(i64, String, i64), now: &(i64, String, i64)| {
            now.0 < before.0 || now.2 < before.2
        };
        if self
            .alerts_snapshot
            .as_ref()
            .is_some_and(|before| decreased(before, &stream(&digest.alerts)))
        {
            self.alerts = None;
        }
        if self
            .incidents_snapshot
            .as_ref()
            .is_some_and(|before| decreased(before, &stream(&digest.incidents)))
        {
            self.incidents = None;
        }
        if self
            .tombstones_snapshot
            .as_ref()
            .is_some_and(|before| decreased(before, &stream(&digest.incident_tombstones)))
        {
            self.incident_tombstones = None;
        }
        if self.checks_snapshot.iter().any(|before| {
            digest
                .checks
                .iter()
                .find(|now| now.check_id == before.check_id)
                .is_none_or(|now| now.max_ts < before.max_ts || now.count < before.count)
        }) {
            self.results = None;
        }
        // A rollup digest has no single "position" to compare against the
        // way an append-only stream's `(max_millis, max_id)` does — see
        // `storage::RollupDigest` — so regression here means any one of its
        // fields going backwards, most plausibly retention pruning the
        // peer's oldest buckets out from under an in-progress walk.
        let rollup = |d: &RollupDigestDto| {
            (
                d.max_bucket_start,
                d.count,
                d.attempts_sum,
                d.up_sum,
                d.latency_sum_sum,
            )
        };
        let rollup_decreased = |before: &(i64, i64, i64, i64, i64),
                                now: &(i64, i64, i64, i64, i64)| {
            now.0 < before.0
                || now.1 < before.1
                || now.2 < before.2
                || now.3 < before.3
                || now.4 < before.4
        };
        if self
            .latency_minute_snapshot
            .as_ref()
            .is_some_and(|before| rollup_decreased(before, &rollup(&digest.latency_minute_rollups)))
        {
            self.latency_minute_rollups = None;
        }
        if self
            .latency_hour_snapshot
            .as_ref()
            .is_some_and(|before| rollup_decreased(before, &rollup(&digest.latency_hour_rollups)))
        {
            self.latency_hour_rollups = None;
        }
        self.checks_snapshot = digest.checks.clone();
        self.alerts_snapshot = Some(stream(&digest.alerts));
        self.incidents_snapshot = Some(stream(&digest.incidents));
        self.tombstones_snapshot = Some(stream(&digest.incident_tombstones));
        self.latency_minute_snapshot = Some(rollup(&digest.latency_minute_rollups));
        self.latency_hour_snapshot = Some(rollup(&digest.latency_hour_rollups));
    }
}

struct Inner {
    database: Database,
    timings: Arc<Timings>,
    /// Immutable alert IDs that arrived with a different payload than the local
    /// event. The local row always wins; this makes legacy divergence visible
    /// without turning anti-entropy retries into destructive repair.
    alert_identity_conflicts: AtomicU64,
    /// Raw observations a peer requested before that peer's retention horizon.
    /// This is deliberately ephemeral like repair cursors: it is an operational
    /// signal, never repair state, and a later sweep re-detects any gap.
    replication_raw_data_gaps: AtomicU64,
    /// One bounded, coalescing queue per peer for `/internal/replicate`, so a
    /// busy node never aims more than one concurrent request at a receiver's
    /// single write path.
    replication_out: replication_out::Outbox,
    tracker: Mutex<StateTracker>,
    state_write: tokio::sync::Mutex<()>,
    /// Progress for bounded anti-entropy pages. Kept in memory because a
    /// restart safely begins another idempotent replay rather than risking a
    /// persisted cursor outliving retention or a peer's history.
    repair_cursors: Mutex<RepairCursors>,
    /// Last observed zone and region rollups. The first observation establishes
    /// a silent baseline; only subsequent changes become persisted alerts.
    intermediate_states:
        Mutex<std::collections::HashMap<(crate::models::CheckId, String), NodeState>>,
    /// How long history is kept. Copied here so the retention job does not
    /// need to re-read config.
    retention: crate::config::RetentionConfig,
    /// Wakes the dispatcher after a transaction commits delivery intentions.
    /// The database outbox is authoritative; a full channel merely delays a
    /// scan and can never discard an alert.
    notify_tx: tokio::sync::mpsc::Sender<()>,
    /// Serializes queue scans. A notifier completion and the periodic retry
    /// loop may overlap, but a queued delivery must only be attempted once at
    /// a time by this coordinator.
    notification_dispatch: tokio::sync::Mutex<()>,
    /// Notification backends, edited in the admin page. Shared with the
    /// dispatcher task, which re-reads them per alert rather than capturing a
    /// copy at boot.
    notifiers: Arc<parking_lot::RwLock<Vec<crate::notify::Notifier>>>,
    events: broadcast::Sender<WsEvent>,
    /// Checks absent from this map fall back to defaults: no group, and the
    /// standard 2xx-3xx rule. Behind a lock because the admin API edits checks
    /// at runtime; ingest reads it per batch, so it stays a cached map rather
    /// than a query.
    checks: parking_lot::RwLock<std::collections::HashMap<crate::models::CheckId, CheckMeta>>,
    /// Complete, ordered check definitions for agent polls. The database remains
    /// authoritative, but polling agents must not queue a database job merely
    /// to rediscover this config snapshot.
    check_catalogue: parking_lot::RwLock<Vec<CheckConfig>>,
    rollup_defaults: crate::config::RollupDefaults,
    rollup_policies:
        parking_lot::RwLock<std::collections::HashMap<String, crate::rollup::RollupPolicy>>,
    /// Agents that have made contact recently, whether or not they have
    /// reported anything yet.
    ///
    /// Liveness elsewhere is derived from submitted results, which is truthful
    /// but blind at exactly the wrong moment: a freshly booted agent has no
    /// results, so it would be absent from the fleet, so placement would hand
    /// it every check — and twenty agents booting together would each take the
    /// whole workload. Polling for work is contact too, and it happens before
    /// the first result exists.
    ///
    /// In memory on purpose. It is a cache of who is talking to us, rebuilt
    /// within one poll interval, so there is no schema and nothing to reconcile
    /// after a restart; it is seeded from reported results at startup so a
    /// restart does not briefly forget the whole fleet.
    seen: parking_lot::RwLock<std::collections::HashMap<String, SeenAgent>>,
    auth: crate::auth::Auth,
    secure_cookies: bool,
    /// Reverse proxies trusted to supply `X-Forwarded-For` for login rate
    /// limiting. Empty means every peer is trusted for nothing, which is
    /// today's behaviour: rate limiting keys on the raw connecting address.
    trusted_proxies: crate::security::TrustedProxies,
    /// Minted agent credentials by id. Cached because every ingest matches
    /// against them; rebuilt on mint and revoke so a revocation bites at once.
    credentials: parking_lot::RwLock<std::collections::HashMap<String, AgentCredential>>,
    /// Secret peers present on `/internal/gossip`. `None` means gossip is
    /// refused outright rather than accepted unauthenticated.
    peer_token: Option<String>,
    /// Peer coordinators to replicate transitions to. `None` when none are
    /// configured, mirroring `notify_tx`: nothing is handed to a fan-out that
    /// does not exist.
    gossip: Option<Arc<gossip::Peers>>,
    /// Consecutive polls an agent may miss before it is treated as gone.
    missed_polls: u32,
    /// How the fleet is summarised on the public status page. Behind a lock
    /// because the admin page edits it at runtime and every status request
    /// reads it.
    status_agents: parking_lot::RwLock<crate::config::AgentsDisplay>,
    /// Scheduled windows during which a check's transitions page nobody.
    /// Behind a lock for the same reason the notifier list is: the admin page
    /// edits it at runtime, and a window that only took effect at the next
    /// restart would be useless for work starting in five minutes.
    maintenance: parking_lot::RwLock<Vec<crate::maintenance::MaintenanceWindow>>,
    /// Stable identity of this coordinator within the cluster. Config-resolved
    /// when set; otherwise generated once and persisted in `settings`, so a
    /// restart rejoins as itself rather than as a stranger — see
    /// `CoordinatorConfig::resolved_node_id`.
    node_id: String,
    /// Hybrid-logical clock for config replication: every config entry is
    /// stamped with it, and every entry received from a peer is observed by
    /// it, so last-writer-wins has a total order to work from.
    clock: hlc::Clock,
    /// Membership and data-plane replication to peer coordinators. `None`
    /// when this node has neither a peer token nor an advertise URL
    /// configured — clustering was never turned on, so there is nothing to
    /// track or replicate to.
    cluster: Option<Arc<cluster::Cluster>>,
    /// How often `spawn_cluster`'s heartbeat loop pings every peer and sweeps
    /// for state transitions. Copied from config so the loop does not need to
    /// re-read it.
    gossip_interval_secs: u64,
    /// How often `spawn_cluster`'s anti-entropy loop reconciles with a random
    /// live peer. Copied from config for the same reason.
    anti_entropy_secs: u64,
    /// Which region this coordinator belongs to, used to label the state
    /// summaries it pushes to a global coordinator (Phase 6). Only meaningful
    /// when `global_push` is set.
    region: String,
    /// Where this node advertises itself, so a state push tells the global node
    /// a live endpoint to route drill-down reads back to. `None` outside a
    /// cluster; the push still works, the global node just cannot proxy to a
    /// node it was never told how to reach.
    advertise_url: Option<String>,
    /// Global-coordinator push target: `(base_url, token, push_secs)`. `None`
    /// when no `[coordinator.global]` block is configured — the push loop is a
    /// no-op, and a Stage A deployment is untouched.
    global_push: Option<(String, String, u64)>,
    /// Shared cross-region secret the global coordinator presents as a `Bearer`
    /// to drive admin operations here (add/edit checks, notifiers, etc.) on
    /// behalf of an operator signed in at the global dashboard. Same value as
    /// the global push token: a region that trusts the global node with its
    /// state also trusts it to relay admin writes. `None` outside Phase 6, so a
    /// Stage A coordinator accepts admin only via a signed session as before.
    admin_trust_token: Option<String>,
}

/// An agent observed making contact: where it said it was, when, and how long
/// its silence may run before it counts as gone.
#[derive(Clone)]
struct SeenAgent {
    zone: String,
    region: String,
    at: i64,
    /// `missed_polls × poll_secs`, from the cadence the agent declared on this
    /// poll. Per agent rather than a constant because the window is only
    /// meaningful against the cadence it is judging: a fixed 90s reads an agent
    /// polling every two minutes as unreachable while it is polling happily.
    window_ms: i64,
    /// Last poll's round-trip as the agent measured it, for the admin fleet
    /// view. `None` until the agent has completed one poll, or for an entry
    /// seeded from stored results at boot.
    rtt_ms: Option<i64>,
    /// Which check kinds this agent says it can probe.
    ///
    /// `None` means it never said — an agent predating capability negotiation,
    /// or an entry seeded from stored results at boot. Read as "anything",
    /// because the alternative is that upgrading the coordinator makes every
    /// existing agent ineligible for everything and the fleet goes dark.
    kinds: Option<Vec<CheckKind>>,
    /// The build it reported, verbatim, for the fleet view. Self-asserted and
    /// display-only: it answers "why is this agent behaving differently?", and
    /// never decides placement — `kinds` does, and a lie there costs the liar
    /// only its own assignments.
    build: Option<String>,
}

impl Coordinator {
    /// Every database job in the coordinator goes through here, so the slow-job
    /// log would name this one line for all of them unless the real call site
    /// is forwarded. Synchronous-returning-a-future for the same reason
    /// `Database::call` is: `#[track_caller]` does not cross an `async fn`.
    #[track_caller]
    fn db<T, F>(&self, operation: F) -> impl std::future::Future<Output = Result<T>> + Send + '_
    where
        T: Send + 'static,
        F: FnOnce(&Storage) -> Result<T> + Send + 'static,
    {
        self.inner
            .database
            .call_at(operation, std::panic::Location::caller())
    }

    pub fn mark_embedded_agent_live(&self, agent: &crate::config::AgentConfig) {
        self.inner.seen.write().insert(
            agent.agent_id.clone(),
            SeenAgent {
                zone: agent.zone.clone(),
                region: agent.region.clone(),
                at: chrono::Utc::now().timestamp_millis(),
                window_ms: unreachable_after_ms(self.inner.missed_polls, Some(agent.poll_secs)),
                rtt_ms: Some(0),
                kinds: None,
                build: None,
            },
        );
    }

    /// Check definitions are persisted and authoritative. Runtime edits survive
    /// restarts; process configuration never seeds application resources.
    pub async fn new(cfg: &CoordinatorConfig) -> Result<Self> {
        let database = Database::open(cfg.db_path.clone()).await?;
        let now = chrono::Utc::now().timestamp_millis();
        let status_agents_seed = cfg.status_page.agents.clone();
        let notifiers_seed = cfg.notifiers.clone();
        let configured_node_id = cfg.resolved_node_id();
        let startup_peer_token = cfg.resolved_peer_token();
        let startup = database
            .call(move |storage| {
                let reported_fleet = storage.list_agents(now).unwrap_or_default();
                let status_agents =
                    configplane::load_or_seed(storage, AGENTS_DISPLAY_KEY, &status_agents_seed);
                let notifiers = configplane::load_or_seed(storage, NOTIFIERS_KEY, &notifiers_seed);
                let maintenance = configplane::load_or_seed(storage, MAINTENANCE_KEY, &Vec::new());
                storage.seed_sections_from_checks()?;
                let checks = storage.list_checks()?;
                let node_id = configured_node_id
                    .unwrap_or_else(|| configplane::load_or_generate(storage, NODE_ID_KEY, 16));
                storage.set_incident_node_id(&node_id);
                let session_key = match startup_peer_token {
                    Some(token) => token.into_bytes(),
                    None => configplane::load_or_generate(storage, SESSION_KEY_KEY, 32).into_bytes(),
                };
                let cluster_members = storage.list_cluster_members().unwrap_or_default();
                let credentials = storage.list_agent_credentials().unwrap_or_default();
                let agent_states = storage.list_agent_check_state().unwrap_or_default();
                let current_states = storage.list_current_state().unwrap_or_default();
                for row in &current_states {
                    if let Err(error) = storage.record_state(row.check_id, row.state, now) {
                        warn!(error = %error, check = %row.check_id, "could not seed state history baseline");
                    }
                }
                let rollup_policies = storage.list_rollup_policies().unwrap_or_default();
                Ok(StartupData {
                    reported_fleet,
                    status_agents,
                    notifiers,
                    maintenance,
                    checks,
                    node_id,
                    session_key,
                    cluster_members,
                    credentials,
                    agent_states,
                    current_states,
                    rollup_policies,
                })
            })
            .await?;
        let notifiers = Arc::new(parking_lot::RwLock::new(startup.notifiers));

        // The channel is only a wake-up hint. Every delivery intention is in
        // the transactional outbox before this is sent, so saturation cannot
        // lose it; the periodic retry scan will pick up a missed wake-up.
        let (notify_tx, rx) = tokio::sync::mpsc::channel::<()>(1024);
        let auth = crate::auth::Auth::with_signing_key(
            cfg.resolved_admin_password(),
            &startup.session_key,
        );
        if !auth.admin_enabled() {
            warn!("no admin password set (WARDEN_ADMIN_PASSWORD); the check editor is disabled");
        }
        // `Some` whenever this node is cluster-capable — a peer token and an
        // advertise URL are both present — regardless of whether it currently
        // has any seeds: a node with zero peers still answers pings and shows
        // its own membership view, it just knows nobody yet.
        let cluster = match (cfg.resolved_peer_token(), cfg.resolved_advertise_url()) {
            (Some(token), Some(url)) => {
                let c = cluster::Cluster::new(
                    startup.node_id.clone(),
                    url,
                    now,
                    cfg.peers.clone(),
                    Some(token),
                    cfg.peer_timeout_secs,
                );
                c.restore(startup.cluster_members.clone());
                Some(Arc::new(c))
            }
            _ => None,
        };
        let clock = hlc::Clock::new(startup.node_id.clone());
        // Resolve the global-push target up front: a present-but-tokenless
        // block is a hard error, matching how a half-configured cluster
        // refuses to start rather than pushing into a 401.
        let global_push = cfg.resolved_global_push()?;
        if let Some((url, _, secs)) = &global_push {
            info!(url = %url, push_secs = secs, region = %cfg.region, "global push enabled");
        }
        let trusted_proxies = crate::security::TrustedProxies::parse(&cfg.trusted_proxies)
            .map_err(|e| anyhow::anyhow!("coordinator.trusted_proxies: {e}"))?;
        let credentials = startup.credentials;
        let live = credentials.iter().filter(|c| !c.revoked()).count();
        if live == 0 {
            warn!(
                "no agent credentials exist; agent endpoints will refuse every \
                 request until one is minted from Admin > Credentials"
            );
        } else {
            info!(count = live, "agent credentials loaded");
        }
        // Resume every per-agent belief and the published root, including a
        // hold-down timer in flight. Seed a history baseline as well; record_state
        // is idempotent when a span already exists.
        let mut tracker = StateTracker::new();
        {
            let n = startup.agent_states.len();
            for row in startup.agent_states {
                tracker.seed_agent(
                    row.check_id,
                    &row.agent_id,
                    &row.zone,
                    &row.region,
                    row.status,
                    row.consecutive_failures,
                    row.last_check,
                );
            }
            if n > 0 {
                info!(leaves = n, "seeded agent beliefs");
            }
        }
        for row in &startup.current_states {
            tracker.seed_published(row.check_id, row.state, row.pending);
        }
        if !startup.current_states.is_empty() {
            info!(
                checks = startup.current_states.len(),
                "seeded published state"
            );
        }
        let rollup_policies = startup
            .rollup_policies
            .into_iter()
            .map(|row| (row.node_id, row.policy))
            .collect();
        let (events, _) = broadcast::channel(256);
        let coord = Self {
            inner: Arc::new(Inner {
                database,
                timings: Arc::new(Timings::new()),
                alert_identity_conflicts: AtomicU64::new(0),
                replication_raw_data_gaps: AtomicU64::new(0),
                replication_out: replication_out::Outbox::default(),
                tracker: Mutex::new(tracker),
                state_write: tokio::sync::Mutex::new(()),
                repair_cursors: Mutex::new(RepairCursors::default()),
                intermediate_states: Mutex::new(std::collections::HashMap::new()),
                retention: cfg.retention.clone(),
                notify_tx,
                notification_dispatch: tokio::sync::Mutex::new(()),
                notifiers,
                events,
                checks: parking_lot::RwLock::new(meta_by_id(&startup.checks, cfg.rollup_defaults)),
                check_catalogue: parking_lot::RwLock::new(startup.checks.clone()),
                rollup_defaults: cfg.rollup_defaults,
                rollup_policies: parking_lot::RwLock::new(rollup_policies),
                seen: parking_lot::RwLock::new(
                    startup
                        .reported_fleet
                        .into_iter()
                        .map(|a| {
                            (
                                a.agent_id,
                                SeenAgent {
                                    zone: a.zone,
                                    region: a.region,
                                    at: a.last_seen,
                                    window_ms: unreachable_after_ms(cfg.missed_polls, None),
                                    rtt_ms: None,
                                    // Seeded from stored results, which carry no
                                    // capabilities. It says what it can run on
                                    // its next poll; until then it is eligible
                                    // for everything, exactly as before.
                                    kinds: None,
                                    build: None,
                                },
                            )
                        })
                        .collect(),
                ),
                auth,
                secure_cookies: cfg.secure_cookies,
                trusted_proxies,
                credentials: parking_lot::RwLock::new(
                    credentials.into_iter().map(|c| (c.id.clone(), c)).collect(),
                ),
                peer_token: cfg.resolved_peer_token(),
                gossip: gossip::Peers::new(&cfg.peers, cfg.resolved_peer_token().as_deref())
                    .map(Arc::new),
                missed_polls: cfg.missed_polls,
                status_agents: parking_lot::RwLock::new(startup.status_agents),
                maintenance: parking_lot::RwLock::new(startup.maintenance),
                node_id: startup.node_id,
                clock,
                cluster,
                gossip_interval_secs: cfg.gossip_interval_secs,
                anti_entropy_secs: cfg.anti_entropy_secs,
                region: cfg.region.clone(),
                advertise_url: cfg.resolved_advertise_url(),
                admin_trust_token: global_push.as_ref().map(|(_, token, _)| token.clone()),
                global_push,
            }),
        };
        coord.spawn_dispatcher(rx);
        coord.apply_provisioned_credentials(&cfg.provision).await?;
        Ok(coord)
    }

    /// Apply the `[[coordinator.provision]]` blocks: credentials whose token
    /// was chosen before this node had ever run, so the agents presenting them
    /// could be configured in the same file that starts the coordinator.
    ///
    /// **An id that already exists is skipped entirely.** That is the whole
    /// idempotency story, and it is load-bearing three times over:
    ///
    /// * `credential::provision` invents a fresh random salt per call, so
    ///   re-provisioning would write a *different* record every boot — bumping
    ///   the HLC and pushing a config write to every peer on every restart.
    /// * revocation keeps the row and sets `revoked_at` (see `credential.rs`),
    ///   so an existence check by id also means a restart cannot resurrect a
    ///   credential an operator revoked. Revoked rows are deliberately not
    ///   filtered out of the check.
    /// * a peer may have replicated the credential in before this node got to
    ///   it. Same id, nothing to do.
    ///
    /// A token that cannot be resolved, or is not shaped like one of ours, is a
    /// hard startup error rather than a warning. The operator who wrote the
    /// block asked for the credential; skipping it silently produces a fleet
    /// that authenticates against nothing, and "no results" is the failure mode
    /// this project treats as the dangerous one — it reads exactly like "not
    /// scheduled yet".
    ///
    /// Cluster nuance: if two nodes provision the same token concurrently on a
    /// cold cluster they write records with different salts and the config
    /// plane's LWW picks one. Both verify the same secret, so the outcome is
    /// correct either way and converges after one anti-entropy sweep. Declaring
    /// the blocks on a single node sidesteps it entirely — the others receive
    /// the credentials over the config plane.
    async fn apply_provisioned_credentials(
        &self,
        blocks: &[crate::config::ProvisionedCredential],
    ) -> Result<()> {
        for block in blocks {
            let zone = block.zone.trim();
            let region = block.region.trim();
            // Same refusal as the mint endpoint: a blank location binds the
            // credential to nothing and places its agents in the bucket
            // credentials exist to replace.
            if zone.is_empty() || region.is_empty() {
                anyhow::bail!(
                    "coordinator.provision entries need both a zone and a region; \
                     got zone={:?} region={:?}",
                    block.zone,
                    block.region
                );
            }
            let token = block
                .resolved_token()
                .ok_or_else(|| anyhow::anyhow!(block.missing_token_error()))?;
            let (id, _) = crate::credential::split(&token).ok_or_else(|| {
                anyhow::anyhow!(
                    "coordinator.provision for zone {zone} has a malformed token: \
                     expected the `wac_<id>.<secret>` form Admin > Credentials mints"
                )
            })?;
            if self.inner.credentials.read().contains_key(id) {
                continue;
            }
            self.provision_agent_credential(&token, zone, region)
                .await?;
            info!(%id, zone, region, "provisioned agent credential from config");
        }
        Ok(())
    }

    /// Drain durable-outbox wake-ups. The database is the queue, not this
    /// bounded channel, so a process crash or saturated wake-up channel leaves
    /// the committed delivery intention available to the next scan.
    fn spawn_dispatcher(&self, mut rx: tokio::sync::mpsc::Receiver<()>) {
        let coord = self.clone();
        tokio::spawn(async move {
            // A wedged peer must not pin this sweep (or the periodic task that
            // owns it) indefinitely. Every digest and backfill request shares the
            // bound, while independent later sweeps can proceed normally.
            let client = match reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
            {
                Ok(client) => client,
                Err(error) => {
                    tracing::warn!(%error, "anti-entropy: failed to build HTTP client");
                    return;
                }
            };
            while rx.recv().await.is_some() {
                // `retry_due` scans the durable outbox and bounds concurrent
                // sends, so one slow backend cannot head-of-line block others.
                coord
                    .retry_due_with_client(chrono::Utc::now().timestamp_millis(), &client)
                    .await;
            }
        });
    }

    /// Hold notification delivery while an integration test fills the bounded
    /// wake-up channel. The database outbox remains writable while held.
    #[doc(hidden)]
    pub async fn pause_notification_dispatch_for_test(&self) -> NotificationDispatchGuard<'_> {
        NotificationDispatchGuard {
            coordinator: self,
            _guard: self.inner.notification_dispatch.lock().await,
        }
    }

    /// Fill the lossy wake-up hint. This deliberately does not touch the
    /// durable database outbox, letting integration tests exercise saturation.
    #[doc(hidden)]
    pub fn saturate_notification_wake_for_test(&self) {
        for _ in 0..=1024 {
            let _ = self.inner.notify_tx.try_send(());
        }
    }

    /// Retry every queued notification that is due, returning how many were
    /// delivered.
    ///
    /// `now_millis` is a parameter rather than read inside, exactly as `prune`
    /// takes one: a test must be able to reach the next attempt without waiting
    /// out the backoff.
    pub async fn retry_due(&self, now_millis: i64) -> usize {
        self.retry_due_with_client(now_millis, &reqwest::Client::new())
            .await
    }

    async fn retry_due_with_client(&self, now_millis: i64, client: &reqwest::Client) -> usize {
        let _dispatch = self.inner.notification_dispatch.lock().await;
        self.retry_due_unlocked(now_millis, client).await
    }

    async fn retry_due_unlocked(&self, now_millis: i64, client: &reqwest::Client) -> usize {
        let due = match self
            .db(move |storage| storage.due_notifications(now_millis, RETRY_BATCH))
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(error = %e, "failed to read the notification queue");
                return 0;
            }
        };
        if due.is_empty() {
            return 0;
        }
        let delivered = stream::iter(due)
            .map(|row| async move { self.deliver_queued(client, row, now_millis).await })
            .buffer_unordered(16)
            .filter_map(|delivered| async move { delivered.then_some(()) })
            .count()
            .await;
        delivered
    }

    async fn deliver_queued(
        &self,
        client: &reqwest::Client,
        row: storage::QueuedRow,
        now_millis: i64,
    ) -> bool {
        // Resolved by name against the live list, so fixing a wrong URL in
        // the admin page fixes the queued notifications too. A notifier that
        // has been deleted drops them: there is nobody to page, and a row
        // that can never succeed is not a retry, it is a leak.
        let notifier = self
            .inner
            .notifiers
            .read()
            .iter()
            .find(|n| n.name == row.notifier)
            .cloned();
        let Some(notifier) = notifier else {
            warn!(
                check = %row.check_id,
                notifier = %row.notifier,
                "dropping a queued notification: its notifier no longer exists"
            );
            self.drop_queued(&row).await;
            return false;
        };
        let t = alert::Transition {
            check_id: row.check_id,
            from: row.previous_state.unwrap_or(NodeState::Unknown),
            to: row.current_state,
        };
        let name = self.check_name(t.check_id);
        match crate::notify::send(client, &notifier, &t, &name).await {
            Ok(()) => {
                info!(
                    check = %row.check_id,
                    notifier = %row.notifier,
                    attempts = row.attempts + 1,
                    "queued notification delivered"
                );
                self.drop_queued(&row).await;
                return true;
            }
            Err(e) => {
                let attempts = row.attempts + 1;
                // Out of attempts: keep the row with no next attempt, which
                // is what the admin panel reads as "nobody was paged".
                let next =
                    (attempts < MAX_DELIVERY_ATTEMPTS).then(|| now_millis + backoff_ms(attempts));
                if next.is_none() {
                    warn!(
                        check = %row.check_id,
                        notifier = %row.notifier,
                        attempts,
                        error = %e,
                        "giving up on a notification; nobody was paged for this transition"
                    );
                }
                let next_row = storage::QueuedRow {
                    attempts,
                    next_attempt: next,
                    last_error: e,
                    ..row
                };
                if let Err(e) = self
                    .db(move |storage| storage.enqueue_notification(&next_row))
                    .await
                {
                    tracing::error!(error = %e, "failed to reschedule a notification");
                }
            }
        }
        false
    }

    async fn drop_queued(&self, row: &storage::QueuedRow) {
        let id = row.id.clone();
        if let Err(e) = self
            .db(move |storage| storage.delete_notification(&id))
            .await
        {
            tracing::error!(error = %e, "failed to clear a delivered notification");
        }
    }

    /// Start the retry job: every `RETRY_TICK`, attempt whatever is due.
    ///
    /// Spawned from `serve`, like retention, because it is a background job of
    /// the running server rather than part of building a coordinator — a test
    /// drives `retry_due` directly instead of waiting on a clock.
    pub fn spawn_retries(&self) {
        let coord = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(RETRY_TICK);
            loop {
                tick.tick().await;
                coord.retry_due(chrono::Utc::now().timestamp_millis()).await;
            }
        });
    }

    /// Starts the background membership heartbeat and anti-entropy loops.
    /// A no-op when clustering is not configured (`self.inner.cluster` is
    /// `None`): a solo node has no peers to ping or reconcile against.
    pub fn spawn_cluster(&self) {
        let Some(cluster) = self.inner.cluster.clone() else {
            return;
        };
        // Heartbeat: ping every known peer, then sweep for state transitions
        // (alive -> suspect -> dead, or a resurrection) driven by silence
        // rather than an explicit signal. Implemented here rather than
        // reusing `cluster::spawn_heartbeat` because a transition discovered
        // on a tick must reach connected admins over the websocket at once,
        // not wait for the next `GET /api/v1/cluster` poll to notice it.
        let heartbeat_coord = self.clone();
        let heartbeat_cluster = cluster.clone();
        let gossip_interval = self.inner.gossip_interval_secs;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(gossip_interval));
            loop {
                tick.tick().await;
                let now = chrono::Utc::now().timestamp_millis();
                heartbeat_cluster.ping_all(now).await;
                for (node_id, state) in
                    heartbeat_cluster.sweep(chrono::Utc::now().timestamp_millis())
                {
                    let _ = heartbeat_coord
                        .inner
                        .events
                        .send(WsEvent::ClusterChange { node_id, state });
                }
            }
        });

        let retry_cluster = cluster.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(
                gossip_interval.saturating_mul(3).max(1),
            ));
            loop {
                tick.tick().await;
                retry_cluster
                    .retry_discovery(chrono::Utc::now().timestamp_millis())
                    .await;
            }
        });

        // Anti-entropy: periodically reconcile with a random live peer,
        // catching up anything gossip missed — a dropped message, a peer that
        // was down during the original transition, or a late joiner with no
        // history at all.
        let anti_entropy_coord = self.clone();
        let anti_entropy_secs = self.inner.anti_entropy_secs;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(anti_entropy_secs));
            loop {
                tick.tick().await;
                anti_entropy_coord.anti_entropy_sweep().await;
            }
        });
    }

    /// Push this coordinator's state summary to the global coordinator on a
    /// timer. A no-op when no `[coordinator.global]` block is configured, so
    /// this runs harmlessly in every mode that serves — like `spawn_cluster`.
    ///
    /// Best-effort like gossip: a failed push is logged and the next tick tries
    /// again. The global node keeps last-writer-wins, so a missed push costs at
    /// most one interval of staleness, never correctness.
    pub fn spawn_global_push(&self) {
        let Some((url, token, push_secs)) = self.inner.global_push.clone() else {
            return;
        };
        let coord = self.clone();
        let client = reqwest::Client::new();
        let endpoint = format!("{}/internal/global/state", url.trim_end_matches('/'));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(push_secs));
            loop {
                tick.tick().await;
                let push = coord.build_state_push().await;
                match client
                    .post(&endpoint)
                    .bearer_auth(&token)
                    .json(&push)
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => {}
                    Ok(r) => warn!(status = %r.status(), "global push rejected"),
                    Err(e) => warn!(error = %e, "global push failed"),
                }
            }
        });
    }

    /// Build the summary of every configured, visible check for a global push.
    /// Reuses the same status source the public `/api/v1/status` reads, so the
    /// global dashboard shows exactly what a regional dashboard would.
    async fn build_state_push(&self) -> crate::global::wire::RegionStatePush {
        use crate::global::cache::worst;
        use crate::global::wire::{CheckSummary, RegionStatePush};

        let now = chrono::Utc::now().timestamp_millis();
        let (rows, confirmed, polls) = self
            .db(move |storage| {
                Ok((
                    storage.list_status(now),
                    storage.list_current_state(),
                    storage.list_agent_polls().unwrap_or_default(),
                ))
            })
            .await
            .unwrap_or_else(|error| {
                warn!(%error, "global push database read failed");
                (Ok(Vec::new()), Ok(Vec::new()), Vec::new())
            });
        let confirmed: std::collections::HashMap<crate::models::CheckId, NodeState> = confirmed
            .unwrap_or_default()
            .into_iter()
            .map(|r| (r.check_id, r.state))
            .collect();
        let by_id = self.inner.checks.read();
        let mut summaries: Vec<CheckSummary> = Vec::new();
        let mut reported: std::collections::HashSet<crate::models::CheckId> =
            std::collections::HashSet::new();
        for r in rows.unwrap_or_default() {
            let Some(meta) = by_id.get(&r.check_id) else {
                continue;
            };
            if !meta.visible {
                continue;
            }
            reported.insert(r.check_id);
            summaries.push(CheckSummary {
                check_id: r.check_id,
                name: meta.name.clone(),
                state: r.state,
                latency_ms: r.latency_ms,
                last_check: r.last_check,
                uptime_24h: Some(r.uptime_24h),
                status_code: r.status_code,
                // The raw error is redacted from the public page; the global
                // push carries only the safe category the failure implies.
                error: r.failure.map(|f| f.summary()),
                kind: Some(meta.kind),
                group: meta.group.clone(),
                latency_graph: meta.latency_graph,
                position: meta.position,
                depends_on: meta.depends_on.clone(),
            });
        }
        // Configured-but-unreported checks still belong on the global page,
        // carrying whatever confirmed state gossip supplied, else their default.
        for (id, meta) in by_id.iter() {
            if !meta.visible || reported.contains(id) {
                continue;
            }
            summaries.push(CheckSummary {
                check_id: *id,
                name: meta.name.clone(),
                state: confirmed
                    .get(id)
                    .copied()
                    .unwrap_or_else(|| meta.default_status.into()),
                latency_ms: 0,
                last_check: 0,
                uptime_24h: None,
                status_code: None,
                error: None,
                kind: Some(meta.kind),
                group: meta.group.clone(),
                latency_graph: meta.latency_graph,
                position: meta.position,
                depends_on: meta.depends_on.clone(),
            });
        }
        let region_health = summaries
            .iter()
            .fold(NodeState::Unknown, |acc, c| worst(acc, c.state));
        // Truthful agent counts for this region: an agent is healthy when its
        // last contact is inside its own silence window. The global status page
        // shows these instead of pretending a region is one agent.
        //
        // Local `seen` alone undercounts in a cluster — an agent polling a peer
        // coordinator is invisible here until fleet gossip lands it in the
        // persisted `agent_polls` twin. Union both sources (an agent healthy in
        // either is healthy), matching how `fleet_health` builds the same view,
        // so every US coordinator reports the whole region's 3 agents, not just
        // the ones it personally serves.
        let (agents_healthy, agents_total) = {
            let region = self.inner.region.as_str();
            let mut merged: std::collections::HashMap<String, bool> =
                std::collections::HashMap::new();
            {
                let seen = self.inner.seen.read();
                for (id, a) in seen.iter() {
                    if a.region != region {
                        continue;
                    }
                    let healthy = now.saturating_sub(a.at) <= a.window_ms;
                    merged
                        .entry(id.clone())
                        .and_modify(|h| *h = *h || healthy)
                        .or_insert(healthy);
                }
            }
            {
                for p in polls {
                    if p.region != region {
                        continue;
                    }
                    let healthy = now.saturating_sub(p.at) <= p.window_ms;
                    merged
                        .entry(p.agent_id)
                        .and_modify(|h| *h = *h || healthy)
                        .or_insert(healthy);
                }
            }
            let total = merged.len() as u32;
            let healthy = merged.values().filter(|h| **h).count() as u32;
            (healthy, total)
        };
        RegionStatePush {
            region: self.inner.region.clone(),
            origin: self.inner.node_id.clone(),
            advertise_url: self.inner.advertise_url.clone(),
            pushed_at: now,
            region_health,
            agents_healthy,
            agents_total,
            checks: summaries,
        }
    }

    async fn apply_config_entries(
        &self,
        entries: Vec<storage::ConfigEntry>,
    ) -> Result<(Vec<storage::ConfigEntry>, bool, bool, Vec<String>)> {
        let entries: Vec<_> = entries
            .into_iter()
            .inspect(|entry| self.inner.clock.observe(&entry.hlc))
            .collect();
        self.db(move |store| {
            let entries: Vec<_> = entries
                .into_iter()
                .map(|mut entry| {
                    if entry.kind == storage::ConfigKind::Setting {
                        if let Some(document) = entry.document.take() {
                            entry.document =
                                Some(serde_json::to_string(&document).unwrap_or(document));
                        }
                    }
                    entry
                })
                .collect();
            // Keep the peer API's fault isolation: a malformed entry must not
            // reject unrelated entries sharing its bounded wire batch. Validate
            // check identity before the transaction; the storage write still
            // owns LWW arbitration and commits every accepted entry together.
            let entries: Vec<_> = entries
                .into_iter()
                .filter(|entry| {
                    let valid = entry.kind != storage::ConfigKind::Check
                        || entry.is_tombstone()
                        || entry.document.as_deref().is_some_and(|document| {
                            serde_json::from_str::<CheckConfig>(document)
                                .ok()
                                .is_some_and(|check| check.id.to_string() == entry.id)
                        });
                    if !valid {
                        warn!(kind = entry.kind.as_str(), id = %entry.id, "config: malformed entry skipped");
                    }
                    valid
                })
                .collect();
            let applied = store.apply_config_entries(&entries)?;
            let mut checks = Vec::new();
            let mut credentials = false;
            let mut policies = false;
            let mut settings = Vec::new();
            for (entry, applied) in entries.into_iter().zip(applied) {
                if !applied {
                    continue;
                }
                match entry.kind {
                    storage::ConfigKind::Check => checks.push(entry),
                    storage::ConfigKind::Credential => credentials = true,
                    storage::ConfigKind::RollupPolicy => policies = true,
                    storage::ConfigKind::Setting => settings.push(entry.id),
                    storage::ConfigKind::Section => {}
                }
            }
            Ok((checks, credentials, policies, settings))
        })
        .await
    }

    /// Replace the published fleet summary, through the config-replication
    /// path so a peer picks it up too. `write_config` persists before
    /// `apply_config_batch` swaps the live cache, so a failed write cannot
    /// leave a setting live that a restart would forget.
    async fn save_agents_display(&self, next: crate::config::AgentsDisplay) -> Result<()> {
        let json = serde_json::to_string(&next)?;
        let entry = self
            .write_config(storage::ConfigKind::Setting, AGENTS_DISPLAY_KEY, Some(json))
            .await?;
        self.replicate_config(vec![entry]);
        Ok(())
    }

    /// Replace the notifier list, through the config-replication path. A
    /// failed write cannot leave the dispatcher sending to something no
    /// restart would remember, for the same reason as any other config edit.
    async fn save_notifiers(&self, next: Vec<crate::notify::Notifier>) -> Result<()> {
        let json = serde_json::to_string(&next)?;
        let entry = self
            .write_config(storage::ConfigKind::Setting, NOTIFIERS_KEY, Some(json))
            .await?;
        self.replicate_config(vec![entry]);
        Ok(())
    }

    /// Replace the maintenance windows, through the config-replication path:
    /// a window that only suppressed paging on the node it was created on
    /// would silently under-cover the rest of the fleet.
    async fn save_maintenance(
        &self,
        next: Vec<crate::maintenance::MaintenanceWindow>,
    ) -> Result<()> {
        let json = serde_json::to_string(&next)?;
        let entry = self
            .write_config(storage::ConfigKind::Setting, MAINTENANCE_KEY, Some(json))
            .await?;
        self.replicate_config(vec![entry]);
        Ok(())
    }

    /// Which notifiers a confirmed transition on this check should reach, or
    /// `None` when it should reach nobody.
    ///
    /// An empty list is not "nobody": it is "whoever is marked default", which
    /// is how a check needs no routing until two audiences exist. A check the
    /// cache has never heard of routes to the defaults — an unknown id means
    /// the metadata has not caught up, and silence is the wrong default for an
    /// alert. Opting out is a deliberate per-check setting, and a maintenance
    /// window is that same opt-out on a timer — judged against `at`, the
    /// transition's own timestamp, so a batch posted late is read by when it
    /// happened rather than by when it arrived.
    fn notify_targets(
        &self,
        check_id: crate::models::CheckId,
        state: NodeState,
        at: i64,
    ) -> Option<Vec<String>> {
        if self
            .inner
            .maintenance
            .read()
            .iter()
            .any(|w| w.covers(check_id, at))
        {
            return None;
        }
        match self.inner.checks.read().get(&check_id) {
            Some(m) if !m.notify || (state == NodeState::Degraded && !m.notify_degraded) => None,
            Some(m) => Some(m.notifiers.clone()),
            None => Some(Vec::new()),
        }
    }

    /// Persist a credential, make it live immediately, and push it to every
    /// peer so an operator mints once for the fleet rather than once per
    /// coordinator.
    async fn store_credential(&self, record: &AgentCredential) -> Result<()> {
        let persisted = record.clone();
        self.db(move |storage| storage.insert_agent_credential(&persisted))
            .await?;
        self.reload_credentials().await?;
        self.restamp_and_push(storage::ConfigKind::Credential, &record.id)
            .await;
        Ok(())
    }

    /// Provision a credential whose token is already known — for a zone whose
    /// agents have to be configured before this coordinator first runs.
    pub async fn provision_agent_credential(
        &self,
        token: &str,
        zone: &str,
        region: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let record = crate::credential::provision(token, zone, region, now)
            .ok_or_else(|| anyhow::anyhow!("malformed agent credential token"))?;
        self.store_credential(&record).await
    }

    /// Re-read credentials after a mint or revoke, so a revocation takes effect
    /// on the next request instead of at the next restart.
    async fn reload_credentials(&self) -> Result<()> {
        let rows = self.db(|storage| storage.list_agent_credentials()).await?;
        *self.inner.credentials.write() = rows.into_iter().map(|c| (c.id.clone(), c)).collect();
        Ok(())
    }

    async fn reload_rollup_policies(&self) -> Result<()> {
        let rows = self.db(|storage| storage.list_rollup_policies()).await?;
        *self.inner.rollup_policies.write() = rows
            .into_iter()
            .map(|row| (row.node_id, row.policy))
            .collect();
        Ok(())
    }

    /// Re-read every check definition after a bulk SQL mutation (such as a
    /// section rename or reorder) so ingest and the status page pick it up.
    /// Normal config entries update only their affected metadata below.
    async fn reload_checks(&self) -> Result<Vec<CheckConfig>> {
        let stored = self.db(|storage| storage.list_checks()).await?;
        *self.inner.checks.write() = meta_by_id(&stored, self.inner.rollup_defaults);
        *self.inner.check_catalogue.write() = stored.clone();
        Ok(stored)
    }

    /// Re-read one `Setting`-kind config row after `apply_config_entry` lands
    /// it, for the three settings this coordinator also keeps a live copy
    /// of. `save_notifiers`/`save_maintenance`/`save_agents_display` update
    /// their cache directly because they already hold the new value; this is
    /// the same update for the path that only has the row's id, having just
    /// written it (or a tombstone of it) through the shared config path
    /// instead. A missing or unparsable row — including a tombstone, which
    /// leaves no row at all — falls back to the type's default rather than
    /// leaving the previous value live, the same "recoverable beats stuck"
    /// choice `load_or_seed` makes at boot.
    ///
    /// Any other setting id applies to the table and stops there: there is
    /// nothing further in `Inner` to keep in step with it.
    async fn reload_setting_cache(&self, id: &str) -> Result<()> {
        let key = id.to_string();
        let value = self.db(move |storage| storage.get_setting(&key)).await?;
        match id {
            NOTIFIERS_KEY => {
                *self.inner.notifiers.write() = value
                    .and_then(|json| serde_json::from_str(&json).ok())
                    .unwrap_or_default();
            }
            MAINTENANCE_KEY => {
                *self.inner.maintenance.write() = value
                    .and_then(|json| serde_json::from_str(&json).ok())
                    .unwrap_or_default();
            }
            AGENTS_DISPLAY_KEY => {
                *self.inner.status_agents.write() = value
                    .and_then(|json| serde_json::from_str(&json).ok())
                    .unwrap_or_default();
            }
            _ => {}
        }
        Ok(())
    }

    /// Apply a batch of config entries through the one HLC-arbitrated write path
    /// (`Storage::apply_config_entry`), then refresh whichever in-memory
    /// caches that path bypasses. Check entries replace or remove only their
    /// own metadata; this keeps singleton edits O(1) at large cardinalities.
    /// The single point both `config_handler`
    /// (an entry that arrived over the wire) and `Coordinator::write_config`
    /// (a local admin edit) go through, so the two can never disagree about
    /// what "applied" means.
    ///
    /// `document`, per entry, is the wire form: `Setting`'s is the caller's
    /// raw value (e.g. a notifier list's own JSON), not yet wrapped as the
    /// string `Storage::apply_config_entry` expects that table's document to
    /// be — see `Storage::config_entry`'s doc for why that extra layer
    /// exists. Every other kind's document already matches its own Rust type
    /// and passes through untouched. Normalizing here, once, rather than at
    /// each caller is what lets `write_config` hand the exact same entry to
    /// both the local apply and `replicate_config`'s push.
    ///
    /// Deliberately never touches `notify_tx`, `tracker`, gossip or the
    /// cluster: config has no observer and takes effect locally by
    /// definition. A single malformed entry is logged and skipped rather
    /// than failing the whole batch, so one bad row cannot block every other
    /// edit riding along with it.
    async fn apply_config_batch(&self, entries: &[storage::ConfigEntry]) -> Result<()> {
        let (checks, credentials, policies, settings) =
            self.apply_config_entries(entries.to_vec()).await?;
        if !checks.is_empty() {
            let mut cached = self.inner.checks.write();
            let mut catalogue = self.inner.check_catalogue.write();
            for entry in checks {
                if entry.is_tombstone() {
                    if let Ok(id) = entry.id.parse() {
                        cached.remove(&id);
                        catalogue.retain(|check| check.id != id);
                    }
                    continue;
                }
                match entry
                    .document
                    .as_deref()
                    .and_then(|document| serde_json::from_str::<CheckConfig>(document).ok())
                {
                    Some(check) => {
                        cached.insert(check.id, check_meta(&check, self.inner.rollup_defaults));
                        if let Some(existing) = catalogue.iter_mut().find(|c| c.id == check.id) {
                            *existing = check;
                        } else {
                            catalogue.push(check);
                        }
                    }
                    None => {
                        warn!(id = %entry.id, "config: applied check has invalid metadata document")
                    }
                }
            }
            catalogue.sort_by(|a, b| {
                a.position
                    .cmp(&b.position)
                    .then_with(|| a.name.cmp(&b.name))
            });
        }
        if credentials {
            self.reload_credentials().await?;
        }
        if policies {
            self.reload_rollup_policies().await?;
        }
        for id in settings {
            self.reload_setting_cache(&id).await?;
        }
        Ok(())
    }

    /// Apply one local config write through `apply_config_batch` — the same
    /// path a replicated entry takes — stamped with this node's clock, which
    /// is always ahead of any earlier local stamp, so a local edit always
    /// wins the merge against what it replaces. `document` is the wire form
    /// (see `apply_config_batch`'s doc); returned unchanged so the caller can
    /// hand the exact entry it just applied to `replicate_config` without
    /// re-deriving it.
    async fn write_config(
        &self,
        kind: storage::ConfigKind,
        id: &str,
        document: Option<String>,
    ) -> Result<storage::ConfigEntry> {
        let entry = storage::ConfigEntry {
            kind,
            id: id.to_string(),
            hlc: self.inner.clock.now(),
            document,
        };
        self.apply_config_batch(std::slice::from_ref(&entry))
            .await?;
        Ok(entry)
    }

    pub(in crate::coordinator) async fn save_rollup_policy(
        &self,
        node_id: &str,
        node_type: storage::NodeType,
        policy: RollupPolicy,
    ) -> Result<()> {
        policy.validate().map_err(anyhow::Error::msg)?;
        let document = serde_json::to_string(&storage::RollupPolicyDoc { node_type, policy })?;
        let entry = self
            .write_config(storage::ConfigKind::RollupPolicy, node_id, Some(document))
            .await?;
        self.replicate_config(vec![entry]);
        Ok(())
    }

    pub(in crate::coordinator) async fn delete_rollup_policy(&self, node_id: &str) -> Result<()> {
        let entry = self
            .write_config(storage::ConfigKind::RollupPolicy, node_id, None)
            .await?;
        self.replicate_config(vec![entry]);
        Ok(())
    }

    /// Push a batch of just-applied config entries to every live peer,
    /// mirroring `ingest`'s replication broadcast: spawned, never awaited, so
    /// an admin edit costs a wedged or slow peer replication lag rather than
    /// blocking on it. A no-op when this node runs without a cluster, or no
    /// peer currently answers.
    fn replicate_config(&self, entries: Vec<storage::ConfigEntry>) {
        if entries.is_empty() {
            return;
        }
        let Some(cluster) = &self.inner.cluster else {
            return;
        };
        if cluster.live_urls().is_empty() {
            return;
        }
        let origin = self.inner.node_id.clone();
        let cluster = Arc::clone(cluster);
        tokio::spawn(async move {
            for entries in Self::config_batches(entries) {
                cluster
                    .broadcast_json(
                        "/internal/config",
                        &ConfigBatch {
                            origin: origin.clone(),
                            entries,
                        },
                    )
                    .await;
            }
        });
    }

    fn config_batches(entries: Vec<storage::ConfigEntry>) -> Vec<Vec<storage::ConfigEntry>> {
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        let mut bytes = 0;
        for entry in entries {
            let entry_bytes =
                serde_json::to_vec(&entry).map_or(MAX_CONFIG_BATCH_BYTES, |v| v.len());
            if !batch.is_empty()
                && (batch.len() == CONFIG_APPLY_CHUNK
                    || bytes + entry_bytes > MAX_CONFIG_BATCH_BYTES)
            {
                batches.push(std::mem::take(&mut batch));
                bytes = 0;
            }
            bytes += entry_bytes;
            batch.push(entry);
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
        batches
    }

    /// Stamp and push whatever row `id` currently holds, for a write that
    /// went through an older helper not aware of replication —
    /// `create_section`, the checks a section rename or delete cascades
    /// into, and a credential mint or revoke. Reads the row back and
    /// re-applies it under a fresh HLC through `write_config` — a
    /// same-document no-op locally, since nothing about the row itself
    /// changed, but it is what gives the row a stamp comparable to a peer's
    /// and something for `replicate_config` to push. A row gone by the time
    /// this runs (deleted out from under the caller) is silently skipped:
    /// there is nothing left to stamp.
    ///
    /// Not used for `Setting` rows: `Storage::config_entry` reads those back
    /// already wrapped for `apply_config_entry`'s own consumption (see
    /// `apply_config_batch`'s doc), one layer more than `write_config`
    /// expects here, so a setting is always pushed by its own handler, which
    /// already holds the raw value it just wrote.
    async fn restamp_and_push(&self, kind: storage::ConfigKind, id: &str) {
        let owned_id = id.to_string();
        let existing = self
            .db(move |storage| Ok(read_config_entry_for_replication(storage, kind, &owned_id)))
            .await
            .ok()
            .flatten();
        let Some(existing) = existing else {
            return;
        };
        match self.write_config(kind, id, existing.document).await {
            Ok(entry) => self.replicate_config(vec![entry]),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    kind = kind.as_str(),
                    %id,
                    "config: failed to restamp a row for replication"
                );
            }
        }
    }

    /// Tell the peers an incident is gone. Spawned rather than awaited, for the
    /// same reason ingest's replication is: an admin action must not block on a
    /// wedged peer, and the local delete has already happened.
    ///
    /// A no-op without a cluster, and a no-op with no live peer — the tombstone
    /// is already written locally, so a peer that is down learns about the
    /// delete when it next tries to push the incident and `upsert_incident`
    /// refuses it.
    fn replicate_incident_deletions(&self, ids: Vec<String>) {
        if ids.is_empty() {
            return;
        }
        let Some(cluster) = self.inner.cluster.clone() else {
            return;
        };
        if cluster.live_urls().is_empty() {
            return;
        }
        let coord = self.clone();
        tokio::spawn(async move {
            let tombstones: Vec<storage::IncidentTombstone> = coord
                .db(move |storage| {
                    ids.into_iter()
                        .map(|id| storage.incident_tombstone(&id))
                        .collect::<Result<Vec<_>>>()
                        .map(|rows| rows.into_iter().flatten().collect())
                })
                .await
                .unwrap_or_default();
            if tombstones.is_empty() {
                return;
            }
            let batch = ReplicationBatch {
                origin: coord.inner.node_id.clone(),
                deleted_incidents: tombstones,
                ..Default::default()
            };
            cluster.broadcast_json("/internal/replicate", &batch).await;
        });
    }

    /// Persist who is polling this node, and tell the peers.
    ///
    /// `Inner::seen` is a node-local map written only by `agent_checks_handler`,
    /// so before this existed each coordinator's fleet view contained only the
    /// agents polling *it* — a three-node cluster reported two of its three
    /// regions down on every node. The persisted twin (`agent_polls`) is what
    /// the status page and Admin > Agents actually read now.
    ///
    /// Runs on **every** coordinator, not only a clustered one: writing `seen`
    /// down is also what carries the fleet across a restart on a single node.
    /// Placement is deliberately *not* changed — `agent_checks_handler` still
    /// decides from local `seen`, so "one probe per zone" still holds and a
    /// peer's agents are never handed this node's checks.
    ///
    /// A snapshot per tick rather than a message per poll: the traffic is
    /// bounded by peers × tick, not by how many agents the fleet has.
    pub fn spawn_fleet_sync(&self) {
        let coord = self.clone();
        let interval = self.inner.gossip_interval_secs;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval));
            loop {
                tick.tick().await;
                let snapshot: Vec<storage::AgentPollRow> = coord
                    .inner
                    .seen
                    .read()
                    .iter()
                    .map(|(agent_id, s)| storage::AgentPollRow {
                        agent_id: agent_id.clone(),
                        zone: s.zone.clone(),
                        region: s.region.clone(),
                        at: s.at,
                        window_ms: s.window_ms,
                        rtt_ms: s.rtt_ms,
                        build: s.build.clone(),
                        // Back to the wire form it arrived as: peers must see
                        // the kinds the agent named, not the subset this
                        // binary happens to recognise.
                        kinds: s
                            .kinds
                            .as_ref()
                            .map(|ks| ks.iter().map(|k| k.as_db()).collect::<Vec<_>>().join(",")),
                    })
                    .collect();
                if snapshot.is_empty() {
                    continue;
                }
                let persisted = snapshot.clone();
                if let Err(error) = coord
                    .db(move |storage| {
                        for poll in &persisted {
                            storage.record_agent_poll(poll)?;
                        }
                        Ok(())
                    })
                    .await
                {
                    tracing::warn!(%error, "could not persist agent polls");
                }
                let Some(cluster) = coord.inner.cluster.clone() else {
                    continue;
                };
                if cluster.live_urls().is_empty() {
                    continue;
                }
                let batch = ReplicationBatch {
                    origin: coord.inner.node_id.clone(),
                    polls: snapshot,
                    ..Default::default()
                };
                cluster.broadcast_json("/internal/replicate", &batch).await;
            }
        });
    }

    /// Start the retention job: prune once at startup, then once a day. Runs
    /// beside ingest on its own task; a failed prune is logged and the
    /// schedule continues rather than taking the server down.
    /// How many jobs a day's raw-result delete is cut into. The database worker
    /// runs one job at a time, so the whole deletion in one job is the whole
    /// deletion in one stall: every probe result and every dashboard read waits
    /// behind it. Slicing does not make retention cheaper, it makes it
    /// interruptible — the other work in the queue runs between slices.
    const RETENTION_SLICES: i64 = 24;

    /// Raw results first, in slices, then everything else in one job. Each
    /// slice is a complete, committed deletion of a time range, so an
    /// interrupted run leaves a consistent database that simply has more left
    /// to remove tomorrow.
    async fn prune_in_slices(
        &self,
        results_days: u64,
        alerts_days: u64,
        now_millis: i64,
    ) -> Result<u64> {
        let started = Instant::now();
        let cutoff = now_millis - (results_days as i64) * 86_400_000;
        let mut removed = 0u64;
        if let Some(oldest) = self.db(|storage| storage.oldest_result_ts()).await? {
            if oldest < cutoff {
                let step = ((cutoff - oldest) / Self::RETENTION_SLICES).max(3_600_000);
                let mut edge = oldest.saturating_add(step);
                while edge < cutoff {
                    removed += self.db(move |storage| storage.prune_results(edge)).await?;
                    edge = edge.saturating_add(step);
                }
                removed += self
                    .db(move |storage| storage.prune_results(cutoff))
                    .await?;
            }
        }
        // The rest is bounded by the number of alerts, incidents and
        // tombstones, not by raw history, so it stays one job.
        removed += self
            .db(move |storage| storage.prune_metadata(results_days, alerts_days, now_millis))
            .await?;
        self.inner.database.observe_removed_logical_rows(removed);
        self.inner.timings.prune_duration_us.fetch_add(
            started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        Ok(removed)
    }

    pub fn spawn_retention(&self) {
        let retention = self.inner.retention.clone();
        let coord = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(24 * 60 * 60));
            loop {
                tick.tick().await;
                let now = chrono::Utc::now().timestamp_millis();
                let results_days = retention.results_days;
                let alerts_days = retention.alerts_days;
                match coord.prune_in_slices(results_days, alerts_days, now).await {
                    Ok(0) => {}
                    Ok(removed) => {
                        tracing::info!(removed, "retention prune removed old rows");
                    }
                    Err(e) => tracing::error!(error = %e, "retention prune failed"),
                }
            }
        });
    }

    /// Attach a confirmed-DOWN check to an incident, opening one if it needs
    /// to. Returns the incident id and whether this call created it.
    ///
    /// Resolution order:
    /// 1. Already in an open incident — nothing to do, a further DOWN rides it.
    /// 2. An upstream named in `depends_on` has an open incident — join that
    ///    one as a `dependency`, so a fan-out of checks behind one failure is
    ///    one entry on the status page rather than a row per symptom.
    /// 3. Otherwise open a new incident with this check as its `origin`.
    ///
    /// The upstream lookup is one hop: it never follows an upstream's own
    /// `depends_on`. That keeps it cycle-safe with no seen-set — a mutual
    /// `depends_on` pair simply gives the incident to whichever fell first.
    fn attach_or_open(
        &self,
        store: &storage::Storage,
        r: &CheckResult,
        state: NodeState,
        failing_regions: &std::collections::BTreeSet<String>,
        ts: i64,
    ) -> anyhow::Result<(String, bool, bool)> {
        let name = self.check_name(r.check_id);
        if let Some(existing) = store.open_incident_for(r.check_id)? {
            let title = incident_title(&name, state, failing_regions);
            let changed = existing.title != title;
            if changed {
                store.edit_incident(&existing.id, &title, existing.started_at, existing.global)?;
            }
            return Ok((existing.id, false, changed));
        }
        let upstreams: Vec<crate::models::CheckId> = {
            let by_id = self.inner.checks.read();
            by_id
                .get(&r.check_id)
                .map(|m| m.depends_on.clone())
                .unwrap_or_default()
        };
        for up in upstreams {
            if let Some(inc) = store.open_incident_for(up)? {
                store.attach_check(&inc.id, r.check_id, storage::AffectSource::Dependency)?;
                return Ok((inc.id, false, false));
            }
        }
        let id = format!("{}-{ts}", r.check_id);
        let cause = incident_details(r).unwrap_or_else(|| format!("check is {}", state.as_str()));
        store.create_incident(
            &id,
            &incident_title(&name, state, failing_regions),
            storage::IncidentState::Detected,
            ts,
            false,
            Some(r.check_id),
            &[storage::IncidentCheckRow {
                check_id: r.check_id,
                source: storage::AffectSource::Origin,
            }],
            Some((&cause, None)),
        )?;
        Ok((id, true, false))
    }

    /// The mutable display name for a check id, or the id itself (rendered)
    /// when the coordinator has no config metadata for it \u2014 a check deleted
    /// between the observation and this lookup, or one this node has not
    /// replicated config for yet. Never fails: this is for human-facing text,
    /// not identity.
    fn check_name(&self, check_id: crate::models::CheckId) -> String {
        self.inner
            .checks
            .read()
            .get(&check_id)
            .map(|m| m.name.clone())
            .unwrap_or_else(|| check_id.to_string())
    }

    fn alert_name(&self, scope: storage::AlertScope, node_id: &str) -> String {
        let check_name = |id| {
            self.inner
                .checks
                .read()
                .get(&id)
                .map(|meta| meta.name.clone())
                .unwrap_or_else(|| "Deleted check".to_string())
        };

        if scope == storage::AlertScope::Check {
            return node_id
                .parse()
                .map(check_name)
                .unwrap_or_else(|_| node_id.to_string());
        }

        let Some(rest) = node_id.strip_prefix("check:") else {
            return node_id.to_string();
        };
        let Some((check_id, location)) = rest.split_once('/') else {
            return node_id.to_string();
        };
        let Ok(check_id) = check_id.parse() else {
            return node_id.to_string();
        };
        format!(
            "{} / {}",
            check_name(check_id),
            location.replace('/', " / ")
        )
    }

    /// Resolve an incident if every gating check has recovered. Returns whether
    /// it did.
    ///
    /// "Recovered" is the up-side mirror of `failures_before_down`: a check
    /// that needs three failures before anyone believes it is down needs three
    /// successes before anyone believes it is back. At the default of 1 this is
    /// the classic behaviour — resolve on the first UP.
    ///
    /// Manual attachments are excluded: an operator naming a check they suspect
    /// must not be able to hold an incident open forever.
    ///
    /// **Caller invariant: `inc` must be an incident the coordinator owns.**
    /// The loop below falls through on an empty gating set and resolves, so an
    /// incident whose attachments are all `manual` would close itself the
    /// moment any check reported UP. `open_incident_for` is what upholds this,
    /// by returning only incidents with an `origin_check` — and an incident
    /// with an origin always carries the matching `Origin` attachment, so the
    /// set is never empty here. A future caller that sources incidents any
    /// other way has to re-establish that, or check for an empty set itself.
    fn try_resolve(
        &self,
        store: &storage::Storage,
        inc: &storage::IncidentRow,
        ts: i64,
    ) -> anyhow::Result<bool> {
        let current: std::collections::HashMap<_, _> = store
            .list_current_state()?
            .into_iter()
            .map(|row| (row.check_id, row.state))
            .collect();
        for check_id in inc.gating_checks() {
            if current.get(&check_id) != Some(&NodeState::Up) {
                return Ok(false);
            }
        }
        let downtime = crate::coordinator::format_duration(ts - inc.started_at);
        store.add_update(
            &inc.id,
            storage::IncidentState::Resolved,
            &format!("Recovered automatically. Total downtime: {downtime}."),
            None,
            ts,
        )?;
        Ok(true)
    }

    fn reconcile_open_incidents(
        &self,
        store: &storage::Storage,
        ts: i64,
    ) -> anyhow::Result<Vec<storage::IncidentRow>> {
        let mut resolved = Vec::new();
        for inc in store.list_incidents(1000, Some(true), None)? {
            if inc.origin_check.is_some() && self.try_resolve(store, &inc, ts)? {
                if let Some(row) = store.incident(&inc.id)? {
                    resolved.push(row);
                }
            }
        }
        Ok(resolved)
    }

    pub(in crate::coordinator) fn rollup_tree(
        &self,
        meta: Option<&CheckMeta>,
        leaves: Vec<(String, state::Belief)>,
        now: i64,
    ) -> RollupTree {
        use std::collections::BTreeMap;

        let policies = self.inner.rollup_policies.read();
        let policy = |override_policy: Option<RollupPolicy>, key: &str, built_in| {
            override_policy
                .or_else(|| policies.get(key).copied())
                .unwrap_or(built_in)
        };

        // Credentials, not self-declared beliefs, define which locations exist.
        // Keeping an Unknown leaf for each credential makes newly configured
        // locations visible before their first agent reports, without affecting
        // rollup thresholds (Unknown is excluded by `rollup`).
        let mut topology: BTreeMap<String, BTreeMap<String, BTreeMap<String, RollupAgent>>> =
            BTreeMap::new();
        for credential in self.inner.credentials.read().values() {
            if credential.revoked() {
                continue;
            }
            topology
                .entry(credential.region.clone())
                .or_default()
                .entry(credential.zone.clone())
                .or_default();
        }

        let credential_topology = !topology.is_empty();
        let seen = self.inner.seen.read();
        for (agent_id, belief) in leaves {
            if !credential_topology {
                topology
                    .entry(belief.region.clone())
                    .or_default()
                    .entry(belief.zone.clone())
                    .or_default();
            }
            let Some(agents) = topology
                .get_mut(&belief.region)
                .and_then(|zones| zones.get_mut(&belief.zone))
            else {
                continue;
            };
            let (staleness_ms, live) = seen.get(&agent_id).map_or_else(
                || {
                    let age = now.saturating_sub(belief.last_check);
                    (
                        age,
                        age <= unreachable_after_ms(self.inner.missed_polls, None),
                    )
                },
                |agent| {
                    let age = now.saturating_sub(agent.at.max(belief.last_check));
                    (age, age <= agent.window_ms)
                },
            );
            agents.insert(
                agent_id.clone(),
                RollupAgent {
                    id: agent_id,
                    state: if live {
                        belief.status.into()
                    } else {
                        NodeState::Unknown
                    },
                    staleness_ms,
                },
            );
        }
        drop(seen);

        let mut regions = Vec::with_capacity(topology.len());
        for (region, zone_map) in topology {
            let mut zones = Vec::with_capacity(zone_map.len());
            for (zone, agent_map) in zone_map {
                let agents: Vec<_> = agent_map.into_values().collect();
                let zone_policy = policy(
                    meta.and_then(|m| m.rollup.zone),
                    &format!("zone:{region}/{zone}"),
                    RollupPolicy::ZONE_DEFAULT,
                );
                let state = rollup(
                    &zone_policy,
                    &agents.iter().map(|agent| agent.state).collect::<Vec<_>>(),
                );
                zones.push(RollupZone {
                    name: zone,
                    state,
                    policy: zone_policy,
                    agents,
                });
            }
            let region_policy = policy(
                meta.and_then(|m| m.rollup.region),
                &format!("region:{region}"),
                RollupPolicy::REGION_DEFAULT,
            );
            let state = rollup(
                &region_policy,
                &zones.iter().map(|zone| zone.state).collect::<Vec<_>>(),
            );
            regions.push(RollupRegion {
                name: region,
                state,
                policy: region_policy,
                zones,
            });
        }
        let global_policy = policy(
            meta.and_then(|m| m.rollup.global),
            "global",
            RollupPolicy::CHECK_DEFAULT,
        );
        let candidate = rollup(
            &global_policy,
            &regions
                .iter()
                .map(|region| region.state)
                .collect::<Vec<_>>(),
        );
        RollupTree {
            root: if candidate == NodeState::Unknown {
                meta.map(|m| m.default_status.into())
                    .unwrap_or(NodeState::Unknown)
            } else {
                candidate
            },
            global_policy,
            regions,
        }
    }

    fn rollup_candidate(
        &self,
        check_id: &str,
        meta: Option<&CheckMeta>,
        leaves: Vec<(String, state::Belief)>,
        now: i64,
    ) -> RollupCandidate {
        let tree = self.rollup_tree(meta, leaves, now);
        let mut intermediates = Vec::new();
        let mut failing_regions = std::collections::BTreeSet::new();
        for region in &tree.regions {
            for zone in &region.zones {
                intermediates.push(IntermediateState {
                    scope: storage::AlertScope::Zone,
                    node_id: format!("check:{check_id}/region:{}/zone:{}", region.name, zone.name),
                    state: zone.state,
                });
            }
            if region.state.is_failing() {
                failing_regions.insert(region.name.clone());
            }
            intermediates.push(IntermediateState {
                scope: storage::AlertScope::Region,
                node_id: format!("check:{check_id}/region:{}", region.name),
                state: region.state,
            });
        }
        RollupCandidate {
            root: tree.root,
            intermediates,
            failing_regions,
        }
    }

    /// Evaluate and persist observations, then publish one rolled-up root per
    /// affected check. Everything downstream is driven solely by root changes.
    pub async fn ingest(&self, mut results: Vec<CheckResult>) {
        let state_wait_started = Instant::now();
        let _state_write = self.inner.state_write.lock().await;
        if self.inner.timings.enabled {
            self.inner.timings.local_state_wait.observe(
                "state_write",
                "local_wait",
                state_wait_started.elapsed(),
            );
        }
        let state_hold_started = Instant::now();
        let check_ids: std::collections::HashSet<_> = results.iter().map(|r| r.check_id).collect();
        let mut staged = self.inner.tracker.lock().snapshot_checks(&check_ids);
        let mut intermediate_cache: std::collections::HashMap<_, _> = self
            .inner
            .intermediate_states
            .lock()
            .iter()
            .filter(|((id, _), _)| check_ids.contains(id))
            .map(|(key, value)| (key.clone(), *value))
            .collect();
        results.sort_by(|a, b| (&a.agent_id, a.timestamp).cmp(&(&b.agent_id, b.timestamp)));
        let mut rows = Vec::with_capacity(results.len());
        let mut affected: std::collections::HashMap<crate::models::CheckId, (CheckResult, i64)> =
            std::collections::HashMap::new();
        let mut leaf_updates = Vec::with_capacity(results.len());

        {
            let tracker = &mut staged;
            for mut r in results {
                let meta = self.inner.checks.read().get(&r.check_id).cloned();
                let status = match meta.as_ref().and_then(|m| m.eval.as_deref()) {
                    Some(src) => script_verdict(src, &mut r),
                    None => eval(&r, meta.as_ref().and_then(|m| m.expect_status)),
                };
                let ts = r.timestamp.timestamp_millis();
                let confirmed = tracker.observe(
                    r.check_id,
                    &r.agent_id,
                    &r.zone,
                    &r.region,
                    status,
                    meta.as_ref().map(|m| m.failures_before_down).unwrap_or(1),
                    ts,
                );
                leaf_updates.push(storage::AgentCheckStateRow {
                    check_id: r.check_id,
                    agent_id: r.agent_id.clone(),
                    zone: r.zone.clone(),
                    region: r.region.clone(),
                    status: confirmed,
                    consecutive_failures: tracker.failures(r.check_id, &r.agent_id),
                    last_check: ts,
                });
                let replace = affected
                    .get(&r.check_id)
                    .map(|(_, prior)| ts >= *prior)
                    .unwrap_or(true);
                if replace {
                    affected.insert(r.check_id, (r.clone(), ts));
                }
                r.body = None;
                r.headers = None;
                rows.push((r, status));
            }
        }

        let mut transitions = Vec::new();
        let mut intermediate_transitions = Vec::new();
        let mut region_set_changes = Vec::new();
        let mut root_regions = std::collections::HashMap::new();
        let mut published_states = std::collections::HashMap::new();
        let mut baseline_failures = Vec::new();
        let mut state_updates = Vec::with_capacity(affected.len());
        for (check_id, (_, ts)) in &affected {
            let check_id = *check_id;
            let meta = self.inner.checks.read().get(&check_id).cloned();
            let leaves = staged.children(check_id);
            let candidate =
                self.rollup_candidate(&check_id.to_string(), meta.as_ref(), leaves, *ts);
            let (transition, state, pending, was_unpublished) = {
                let tracker = &mut staged;
                let was_unpublished = tracker.state(check_id).is_none();
                let transition = tracker.publish(
                    check_id,
                    candidate.root,
                    *ts,
                    meta.as_ref().map(|m| m.min_incident_secs).unwrap_or(0),
                    meta.as_ref().map(|m| m.min_recovery_secs).unwrap_or(0),
                );
                (
                    transition,
                    tracker.state(check_id).unwrap_or(candidate.root),
                    tracker.pending(check_id),
                    was_unpublished,
                )
            };
            // Intermediate transitions are downstream evidence just like the root
            // alert, so do not publish them while the root is held down. Otherwise
            // a swallowed blip still leaks zone/region alerts to the UI.
            if was_unpublished || transition.is_some() {
                let previous_regions = {
                    let cache = &mut intermediate_cache;
                    let mut previous = std::collections::BTreeSet::new();
                    let mut had_baseline = false;
                    for intermediate in &candidate.intermediates {
                        let key = (check_id, intermediate.node_id.clone());
                        if let Some(prior) = cache.insert(key, intermediate.state) {
                            had_baseline = true;
                            if intermediate.scope == storage::AlertScope::Region
                                && prior.is_failing()
                            {
                                previous.insert(
                                    intermediate
                                        .node_id
                                        .rsplit("region:")
                                        .next()
                                        .unwrap_or_default()
                                        .to_string(),
                                );
                            }
                            if prior != intermediate.state {
                                intermediate_transitions.push((
                                    intermediate.scope,
                                    intermediate.node_id.clone(),
                                    prior,
                                    intermediate.state,
                                    *ts,
                                ));
                            }
                        }
                    }
                    had_baseline.then_some(previous)
                };
                if let Some(previous) = previous_regions {
                    if previous != candidate.failing_regions {
                        region_set_changes.push((
                            check_id,
                            previous,
                            candidate.failing_regions.clone(),
                            *ts,
                        ));
                    }
                }
            }
            root_regions.insert(check_id, candidate.failing_regions);
            if let Some(t) = transition {
                transitions.push((t, *ts));
            } else if was_unpublished && state.is_failing() {
                baseline_failures.push((check_id, state, *ts));
            }
            // Persist every candidate, including pending hold-down state.
            published_states.insert(check_id, state);
            state_updates.push(storage::PublishedStateRow {
                check_id,
                state,
                pending,
                last_check: *ts,
            });
        }

        // Resolve targets before persistence, then put one row per intended
        // backend in the same transaction as the alert. The outbox is durable
        // before any wake-up or network I/O can occur.
        let all_notifiers = self.inner.notifiers.read().clone();
        let notification_outbox: Vec<_> = transitions
            .iter()
            .filter(|(t, _)| alert::should_alert(t))
            .filter_map(|(t, ts)| {
                let routed_to = self.notify_targets(t.check_id, t.to, *ts)?;
                let targets = crate::notify::targets(&all_notifiers, &routed_to);
                if targets.is_empty() {
                    warn!(check = %t.check_id, routed_to = ?routed_to, "alert matched no notifier");
                }
                Some(
                    targets
                        .into_iter()
                        .map(|notifier| storage::QueuedRow {
                            id: uuid::Uuid::now_v7().to_string(),
                            notifier: notifier.name.clone(),
                            check_id: t.check_id,
                            previous_state: Some(t.from),
                            current_state: t.to,
                            attempts: 0,
                            next_attempt: Some(*ts),
                            last_error: String::new(),
                            queued_at: *ts,
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect();
        let rows_for_replication = rows.clone();
        let leaf_updates_for_replication = leaf_updates.clone();
        let transitions_for_publish = transitions.clone();
        let coord = self.clone();
        let persisted = self
            .db(move |store| {
                let mut incident_events = Vec::new();
                let mut incident_for =
                    std::collections::HashMap::<crate::models::CheckId, String>::new();
                let mut replicated_alerts = Vec::<ReplicatedAlert>::new();
                let mut replicated_incidents = Vec::<storage::ReplicatedIncident>::new();
                store.prepare_ingest()?;
                store.transaction(|store| {
                    store.persist_ingest_uncommitted(&rows, &leaf_updates, &state_updates)?;

                    for (check_id, state, ts) in &baseline_failures {
                        let Some((result, _)) = affected.get(check_id) else {
                            continue;
                        };
                        let regions = root_regions.get(check_id).cloned().unwrap_or_default();
                        match coord.attach_or_open(store, result, *state, &regions, *ts) {
                            Ok((id, opened, changed)) => {
                                if opened || changed {
                                    if let Some(i) = store.incident(&id)? {
                                        let incident = IncidentDto::from_row(i, *ts, false);
                                        incident_events.push(if opened {
                                            WsEvent::IncidentOpened { incident }
                                        } else {
                                            WsEvent::IncidentUpdated { incident }
                                        });
                                    }
                                }
                                incident_for.insert(*check_id, id);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    for (t, ts) in &transitions {
                        let Some((result, _)) = affected.get(&t.check_id) else {
                            continue;
                        };
                        if t.to.is_failing() {
                            let regions =
                                root_regions.get(&t.check_id).cloned().unwrap_or_default();
                            match coord.attach_or_open(store, result, t.to, &regions, *ts) {
                                Ok((id, opened, changed)) => {
                                    if opened || changed {
                                        if let Some(i) = store.incident(&id)? {
                                            let incident = IncidentDto::from_row(i, *ts, false);
                                            incident_events.push(if opened {
                                                WsEvent::IncidentOpened { incident }
                                            } else {
                                                WsEvent::IncidentUpdated { incident }
                                            });
                                        }
                                    }
                                    incident_for.insert(t.check_id, id);
                                }
                                Err(e) => return Err(e),
                            }
                        } else if t.to == NodeState::Up {
                            match store.open_incident_for(t.check_id) {
                                Ok(Some(inc)) => {
                                    incident_for.insert(t.check_id, inc.id.clone());
                                    match coord.try_resolve(store, &inc, *ts) {
                                        Ok(true) => {
                                            if let Some(i) = store.incident(&inc.id)? {
                                                let ack = store.is_acknowledged(&inc.id)?;
                                                incident_events.push(WsEvent::IncidentUpdated {
                                                    incident: IncidentDto::from_row(i, *ts, ack),
                                                });
                                            }
                                        }
                                        Ok(false) => {}
                                        Err(e) => return Err(e),
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => return Err(e),
                            }
                        }
                    }

                    let reconcile_ts = affected
                        .values()
                        .map(|(_, ts)| *ts)
                        .max()
                        .unwrap_or_default();
                    match coord.reconcile_open_incidents(store, reconcile_ts) {
                        Ok(rows) => {
                            for row in rows {
                                let ack = store.is_acknowledged(&row.id)?;
                                if let Some(origin) = row.origin_check {
                                    incident_for.insert(origin, row.id.clone());
                                }
                                incident_events.push(WsEvent::IncidentUpdated {
                                    incident: IncidentDto::from_row(row, reconcile_ts, ack),
                                });
                            }
                        }
                        Err(e) => return Err(e),
                    }

                    for (check_id, previous, current, ts) in &region_set_changes {
                        if published_states.get(check_id) != Some(&NodeState::Degraded) {
                            continue;
                        }
                        match store.open_incident_for(*check_id) {
                            Ok(Some(inc)) => {
                                store.add_update(
                                    &inc.id,
                                    inc.state,
                                    &region_change_narrative(previous, current),
                                    None,
                                    *ts,
                                )?;
                                let title = incident_title(
                                    &coord.check_name(*check_id),
                                    NodeState::Degraded,
                                    current,
                                );
                                if inc.title != title {
                                    store.edit_incident(
                                        &inc.id,
                                        &title,
                                        inc.started_at,
                                        inc.global,
                                    )?;
                                }
                                incident_for.insert(*check_id, inc.id.clone());
                                if let Some(row) = store.incident(&inc.id)? {
                                    let ack = store.is_acknowledged(&inc.id)?;
                                    incident_events.push(WsEvent::IncidentUpdated {
                                        incident: IncidentDto::from_row(row, *ts, ack),
                                    });
                                }
                            }
                            Ok(None) => {}
                            Err(e) => return Err(e),
                        }
                    }

                    for (scope, node_id, previous, current, ts) in &intermediate_transitions {
                        match store.insert_alert(
                            *scope,
                            node_id,
                            Some(*previous),
                            *current,
                            *ts,
                            None,
                        ) {
                            Ok(id) => replicated_alerts.push(ReplicatedAlert {
                                id,
                                scope: scope.as_str().to_string(),
                                node_id: node_id.clone(),
                                previous_state: Some(*previous),
                                current_state: *current,
                                timestamp: *ts,
                                incident_id: None,
                            }),
                            Err(e) => return Err(e),
                        }
                    }

                    for (t, ts) in transitions.iter().filter(|(t, _)| alert::should_alert(t)) {
                        let incident_id = incident_for.get(&t.check_id).cloned();
                        match store.insert_alert(
                            storage::AlertScope::Check,
                            &t.check_id.to_string(),
                            Some(t.from),
                            t.to,
                            *ts,
                            incident_id.as_deref(),
                        ) {
                            Ok(id) => replicated_alerts.push(ReplicatedAlert {
                                id,
                                scope: storage::AlertScope::Check.as_str().to_string(),
                                node_id: t.check_id.to_string(),
                                previous_state: Some(t.from),
                                current_state: t.to,
                                timestamp: *ts,
                                incident_id,
                            }),
                            Err(e) => return Err(e),
                        }
                    }
                    for delivery in &notification_outbox {
                        store.enqueue_notification(delivery)?;
                    }
                    let mut seen_incidents = std::collections::HashSet::new();
                    for id in incident_for.values() {
                        if seen_incidents.insert(id.as_str()) {
                            if let Some(row) = store.replicated_incident(id)? {
                                replicated_incidents.push(row);
                            }
                        }
                    }
                    Ok((
                        incident_events,
                        incident_for,
                        replicated_alerts,
                        replicated_incidents,
                    ))
                })
            })
            .await;
        let (incident_events, incident_for, replicated_alerts, replicated_incidents) =
            match persisted {
                Ok(value) => value,
                Err(error) => {
                    if self.inner.timings.enabled {
                        self.inner.timings.local_state_hold.observe(
                            "state_write",
                            "local_hold",
                            state_hold_started.elapsed(),
                        );
                    }
                    tracing::error!(%error, "failed to persist ingest batch");
                    return;
                }
            };
        self.inner
            .database
            .observe_logical_rows(rows_for_replication.len() as u64);
        self.inner.tracker.lock().install_checks(staged, &check_ids);
        {
            let mut cache = self.inner.intermediate_states.lock();
            cache.retain(|(id, _), _| !check_ids.contains(id));
            cache.extend(intermediate_cache);
        }
        let rows = rows_for_replication;
        let transitions = transitions_for_publish;

        if let Some(peers) = &self.inner.gossip {
            if !transitions.is_empty() {
                let peers = Arc::clone(peers);
                let messages: Vec<_> = transitions
                    .iter()
                    .map(|(t, ts)| {
                        let alert = replicated_alerts.iter().find(|alert| {
                            alert.scope == storage::AlertScope::Check.as_str()
                                && alert.node_id == t.check_id.to_string()
                                && alert.timestamp == *ts
                                && alert.previous_state == Some(t.from)
                                && alert.current_state == t.to
                        });
                        gossip::Message::new(
                            t,
                            *ts,
                            alert.cloned(),
                            incident_for.get(&t.check_id).cloned(),
                        )
                    })
                    .collect();
                tokio::spawn(async move {
                    for message in &messages {
                        peers.broadcast(message).await;
                    }
                });
            }
        }
        if let Some(cluster) = &self.inner.cluster {
            if !rows.is_empty() || !replicated_alerts.is_empty() || !replicated_incidents.is_empty()
            {
                let live = cluster.live_urls();
                if !live.is_empty() {
                    let batch = ReplicationBatch {
                        origin: self.inner.node_id.clone(),
                        results: rows
                            .into_iter()
                            .map(|(result, status)| ReplicatedResult { result, status })
                            .collect(),
                        // The same leaves just written to `agent_check_state`,
                        // so a peer that never saw these probes still folds the
                        // whole zone rather than the slice that reports to it.
                        beliefs: leaf_updates_for_replication
                            .iter()
                            .map(|state| ReplicatedBelief {
                                check_id: state.check_id,
                                agent_id: state.agent_id.clone(),
                                zone: state.zone.clone(),
                                region: state.region.clone(),
                                status: state.status,
                                consecutive_failures: state.consecutive_failures,
                                last_check: state.last_check,
                            })
                            .collect(),
                        alerts: replicated_alerts,
                        incidents: replicated_incidents,
                        ..Default::default()
                    };
                    self.inner
                        .replication_out
                        .broadcast(cluster, &self.inner.timings, &batch);
                }
            }
        }

        for (t, ts) in transitions
            .into_iter()
            .filter(|(t, _)| alert::should_alert(t))
        {
            let _ = self.inner.events.send(WsEvent::StateChange {
                check_id: t.check_id,
                from: t.from,
                to: t.to,
                timestamp: ts,
            });
            // The outbox row was committed with the alert. A full wake-up
            // channel is harmless: `spawn_retries` will scan it later.
            let _ = self.inner.notify_tx.try_send(());
        }
        for event in incident_events {
            let _ = self.inner.events.send(event);
        }
        if self.inner.timings.enabled {
            self.inner.timings.local_state_hold.observe(
                "state_write",
                "local_hold",
                state_hold_started.elapsed(),
            );
        }
    }

    /// Apply a peer's replication batch: incidents, then alerts, then raw
    /// results, then this node's own confirmed-state view of every check the
    /// batch touched. Mirrors what local `ingest` writes to storage and
    /// nothing else it does — no `eval`, no `notify_tx`, no re-broadcast to
    /// the cluster or to `self.inner.gossip` — because the sender already
    /// decided all of that and owns paging for it.
    async fn apply_replicated(&self, batch: ReplicationBatch) -> anyhow::Result<()> {
        let state_wait_started = Instant::now();
        let _state_write = self.inner.state_write.lock().await;
        if self.inner.timings.enabled {
            self.inner.timings.replicated_state_wait.observe(
                "state_write",
                "replicated_wait",
                state_wait_started.elapsed(),
            );
        }
        self.apply_replicated_locked(batch).await
    }

    /// Repair must never join the state-write wait queue ahead of fresh agent
    /// observations. If ingest owns or is waiting for it, this page waits for
    /// the next sweep; idempotent cursored replay makes that safe.
    async fn apply_replicated_repair(&self, batch: ReplicationBatch) -> anyhow::Result<bool> {
        let Ok(_state_write) = self.inner.state_write.try_lock() else {
            return Ok(false);
        };
        self.apply_replicated_locked(batch).await?;
        Ok(true)
    }

    /// Called only while `state_write` is held. Keeping tracker and database
    /// updates in this one transactional path preserves the atomicity shared
    /// by ingest and ordinary replication.
    async fn apply_replicated_locked(&self, batch: ReplicationBatch) -> anyhow::Result<()> {
        let state_hold_started = Instant::now();
        let beliefs = batch.beliefs.clone();
        let origin = batch.origin.clone();
        let persisted = self
            .db(move |store| {
                store.transaction(|store| {
                    let mut incident_events = Vec::new();
                    let mut alert_identity_conflicts = Vec::new();
                    let mut state_changes: Vec<(
                        crate::models::CheckId,
                        Option<NodeState>,
                        NodeState,
                        i64,
                    )> = Vec::new();
                    let now = chrono::Utc::now().timestamp_millis();
                    // Deletes are applied before upserts, so a batch that carries both
                    // an incident and its deletion (a reconciler replaying a peer's
                    // whole state) converges on deleted rather than on ordering luck.
                    for tombstone in &batch.deleted_incidents {
                        if store.apply_incident_tombstone_uncommitted(tombstone)? {
                            incident_events.push(WsEvent::IncidentDeleted {
                                id: tombstone.id.clone(),
                            });
                        }
                    }
                    // Poll rows are a merge on `polled_at`, so a stale batch cannot
                    // un-see a live agent and applying the same batch twice is a no-op.
                    for p in &batch.polls {
                        store.record_agent_poll(p)?;
                    }
                    for i in &batch.incidents {
                        let existed = matches!(store.incident(&i.id), Ok(Some(_)));
                        let changed = store.upsert_incident_uncommitted(i)?;
                        if changed {
                            if let Some(row) = store.incident(&i.id)? {
                                let ack = store.is_acknowledged(&i.id)?;
                                let dto = IncidentDto::from_row(row, now, ack);
                                incident_events.push(if existed {
                                    WsEvent::IncidentUpdated { incident: dto }
                                } else {
                                    WsEvent::IncidentOpened { incident: dto }
                                });
                            }
                        }
                    }
                    for a in &batch.alerts {
                        let row = a.clone().into_row();
                        let applied = store.upsert_alert(&row)?;
                        if applied == storage::AlertApply::Conflict {
                            // One disagreeing event must not sink the batch it
                            // arrived in: the local event stands, and every
                            // other row here still applies.
                            //
                            // Never overwrite immutable conflicting rows. Record
                            // the committed conflict below, outside this
                            // transaction, so failed batches do not inflate the
                            // health signal.
                            alert_identity_conflicts.push(row.id.clone());
                        }
                        let inserted = applied == storage::AlertApply::Inserted;
                        // Zone and region alerts are topology narratives only. They
                        // replicate for the alert feed, but never masquerade as check
                        // roots in current state or websocket notifications.
                        if inserted && row.scope == storage::AlertScope::Check {
                            let check_id = a.node_id.parse::<crate::models::CheckId>()?;
                            state_changes.push((
                                check_id,
                                a.previous_state,
                                a.current_state,
                                a.timestamp,
                            ));
                        }
                    }
                    if !batch.results.is_empty() {
                        let rows: Vec<(CheckResult, Status)> = batch
                            .results
                            .into_iter()
                            .map(|r| (r.result, r.status))
                            .collect();
                        store.insert_results_dedup_uncommitted(&rows)?;
                    }
                    for (check_id, _, current, ts) in &state_changes {
                        store.update_current_state(*check_id, *current, None, *ts)?;
                        store.record_state(*check_id, *current, *ts)?;
                    }
                    let leaves: Vec<storage::AgentCheckStateRow> = batch
                        .beliefs
                        .iter()
                        .map(|belief| storage::AgentCheckStateRow {
                            check_id: belief.check_id,
                            agent_id: belief.agent_id.clone(),
                            zone: belief.zone.clone(),
                            region: belief.region.clone(),
                            status: belief.status,
                            consecutive_failures: belief.consecutive_failures,
                            last_check: belief.last_check,
                        })
                        .collect();
                    store.update_agent_check_states_uncommitted(&leaves)?;
                    Ok((incident_events, state_changes, alert_identity_conflicts))
                })
            })
            .await;
        let (incident_events, state_changes, alert_identity_conflicts) = match persisted {
            Ok(value) => value,
            Err(error) => {
                if self.inner.timings.enabled {
                    self.inner.timings.replicated_state_hold.observe(
                        "state_write",
                        "replicated_hold",
                        state_hold_started.elapsed(),
                    );
                }
                tracing::warn!(%error, "replicate: database worker failed");
                return Err(error);
            }
        };
        if !alert_identity_conflicts.is_empty() {
            let count = alert_identity_conflicts.len() as u64;
            let previous = self
                .inner
                .alert_identity_conflicts
                .fetch_add(count, Ordering::Relaxed);
            if previous == 0 {
                warn!(
                    origin = %origin,
                    alerts = ?alert_identity_conflicts,
                    "replicate: alert identity conflict detected; keeping immutable local rows"
                );
            } else {
                tracing::debug!(
                    origin = %origin,
                    count,
                    "replicate: alert identity conflicts persist; keeping immutable local rows"
                );
            }
        }
        {
            // The failure count resets, same as `gossip_handler`: this node
            // is not mid-confirmation for something the observer already
            // decided.
            let mut tracker = self.inner.tracker.lock();
            for (check_id, _, current, _) in &state_changes {
                tracker.seed_published(*check_id, *current, None);
            }
            // Leaves from a peer's tracker. `merge_agent` keeps the newer
            // observation, so an out-of-order or replayed batch cannot rewind a
            // live agent, and only what actually won is persisted below.
            for b in beliefs {
                tracker.merge_agent(
                    b.check_id,
                    &b.agent_id,
                    &b.zone,
                    &b.region,
                    b.status,
                    b.consecutive_failures,
                    b.last_check,
                );
            }
        }
        for (check_id, from, to, timestamp) in state_changes {
            let _ = self.inner.events.send(WsEvent::StateChange {
                check_id,
                from: from.unwrap_or(NodeState::Unknown),
                to,
                timestamp,
            });
        }
        for ev in incident_events {
            let _ = self.inner.events.send(ev);
        }
        if self.inner.timings.enabled {
            self.inner.timings.replicated_state_hold.observe(
                "state_write",
                "replicated_hold",
                state_hold_started.elapsed(),
            );
        }
        Ok(())
    }

    /// One anti-entropy cycle: pick a live peer at random, compare digests,
    /// and pull whatever it has that this node's `check_results` does not —
    /// the correctness backstop for a dropped or missed `/internal/replicate`
    /// push, not a substitute for it. Only results are reconciled; alerts and
    /// incidents ride along inside `apply_replicated` when a page happens to
    /// carry them, but nothing here chases a dropped alert on its own.
    /// Config is reconciled here too, by a different rule: results are
    /// compared per check on a timestamp cursor, config per kind on an HLC
    /// and a row count, because config rows are updated in place and a pure
    /// high-water mark cannot see a row this node never received.
    pub async fn anti_entropy_sweep(&self) {
        let Some(cluster) = self.inner.cluster.clone() else {
            return;
        };
        let live = cluster.live_urls();
        let peer = {
            let cursors = self.inner.repair_cursors.lock();
            cursors
                .peer
                .as_ref()
                .filter(|peer| cursors.in_progress() && live.contains(peer))
                .cloned()
                .or_else(|| live.choose(&mut rand::thread_rng()).cloned())
        };
        let Some(peer) = peer else {
            return;
        };
        let client = reqwest::Client::new();
        let Some(their) = self.fetch_digest(&client, &peer).await else {
            return;
        };
        let node_id = self.inner.node_id.clone();
        let our = match self
            .db(move |storage| {
                storage
                    .digest(0)
                    .map(|digest| DigestResponse::new(&node_id, digest))
            })
            .await
        {
            Ok(digest) => digest,
            Err(error) => {
                tracing::warn!(%error, "anti-entropy: failed to read local digest");
                return;
            }
        };
        {
            let mut cursors = self.inner.repair_cursors.lock();
            cursors.for_peer(&peer);
            cursors.refresh_digest(&their);
        }
        let results_differ = their.checks.iter().any(|theirs| {
            match our
                .checks
                .binary_search_by(|ours| ours.check_id.cmp(&theirs.check_id))
            {
                Ok(i) => {
                    theirs.max_ts > our.checks[i].max_ts || theirs.count != our.checks[i].count
                }
                Err(_) => true,
            }
        });
        for theirs in &their.checks {
            let local_max_ts = our
                .checks
                .binary_search_by(|ours| ours.check_id.cmp(&theirs.check_id))
                .ok()
                .map(|i| our.checks[i].max_ts);
            if local_max_ts.is_none_or(|max_ts| theirs.min_ts > max_ts) {
                self.inner
                    .replication_raw_data_gaps
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    peer,
                    check_id = %theirs.check_id,
                    peer_retained_min_ts = theirs.min_ts,
                    ?local_max_ts,
                    "anti-entropy: peer is behind the source raw-data retention horizon; backfill is incomplete"
                );
                // Durable, not just a counter and a log line: an operator
                // needs to know *which* checks have holes and over what
                // range, and a restart must not lose that a gap was ever
                // seen. `record_raw_data_gap` upserts on `(check_id, peer)`,
                // so a standing gap redetected every sweep does not grow this
                // table without bound.
                if let (Ok(check_id), from_ts, to_ts, observed_at) = (
                    theirs.check_id.parse::<crate::models::CheckId>(),
                    local_max_ts.unwrap_or(0),
                    theirs.min_ts,
                    chrono::Utc::now().timestamp_millis(),
                ) {
                    let peer = peer.clone();
                    let gap = storage::RawDataGapRow {
                        check_id,
                        peer,
                        from_ts,
                        to_ts,
                        observed_at,
                    };
                    if let Err(error) = self.db(move |s| s.record_raw_data_gap(&gap)).await {
                        tracing::warn!(%error, "anti-entropy: failed to record a raw-data gap");
                    }
                }
            }
        }
        let stream_pages = [
            (&their.alerts, &our.alerts),
            (&their.incidents, &our.incidents),
            (&their.incident_tombstones, &our.incident_tombstones),
        ]
        .into_iter()
        .filter(|(theirs, ours)| {
            theirs.count != ours.count
                || (theirs.max_millis, &theirs.max_id) > (ours.max_millis, &ours.max_id)
        })
        .count();
        if !results_differ {
            self.set_results_repair_cursor(&peer, None);
        }
        let mut pages = ANTI_ENTROPY_PAGES_PER_SWEEP;
        // Keep one page for alerts/incidents/tombstones: a large result repair
        // must not indefinitely delay lifecycle convergence.
        if results_differ {
            let mut result_pages = pages.saturating_sub(stream_pages);
            self.backfill_results(&client, &peer, &mut result_pages)
                .await;
            pages -= pages.saturating_sub(stream_pages) - result_pages;
        }
        self.reconcile_streams(&client, &peer, &their, &our, &mut pages)
            .await;
        // Rollup anti-entropy (item A) deliberately gets no reserved share of
        // `pages`, only whatever `reconcile_streams` above left behind.
        // Alerts/incidents/tombstones are reserved a page each because they
        // are everyday, bounded-size streams that must keep converging even
        // while a large result repair is running. A rollup catch-up is the
        // opposite shape: it is exactly as large as raw-result catch-up can
        // be (see `storage::RollupDigest`'s doc comment on table size) and it
        // only exists at all for the rare case of a peer already behind raw
        // retention — the common case is that `derived_results_sql` already
        // keeps rollups converged as a side effect of ordinary result
        // replication, so this path finding nothing to do costs nothing.
        // Reserving pages for it ahead of results would let a large rollup
        // catch-up starve the everyday result/alert/incident convergence this
        // sweep exists for first.
        self.reconcile_rollup_streams(&client, &peer, &their, &our, &mut pages)
            .await;
        // Config is bounded by configured objects rather than retained event
        // history and is applied in one atomic config transaction.
        self.reconcile_config(&client, &peer, &their, &our).await;
    }

    async fn reconcile_streams(
        &self,
        client: &reqwest::Client,
        peer: &str,
        their: &DigestResponse,
        our: &DigestResponse,
        pages: &mut usize,
    ) {
        for (path, theirs, ours) in [
            ("alerts", &their.alerts, &our.alerts),
            ("incidents", &their.incidents, &our.incidents),
            (
                "incident-tombstones",
                &their.incident_tombstones,
                &our.incident_tombstones,
            ),
        ] {
            if *pages == 0 {
                return;
            }
            if theirs.count == ours.count
                && (theirs.max_millis, &theirs.max_id) <= (ours.max_millis, &ours.max_id)
            {
                self.set_stream_repair_cursor(peer, path, None);
                continue;
            }
            // Still divergent after a completed walk. Resuming from the tail
            // finds anything the peer has appended since; only a full rescan
            // finds a row inserted below it, so that is what the backoff buys
            // back — once per interval rather than once per sweep.
            if self
                .stream_repair_cursor(peer, path)
                .is_some_and(|c| c.stale())
            {
                self.set_stream_repair_cursor(peer, path, None);
            }
            self.backfill_stream(client, peer, path, pages).await;
        }
    }

    async fn backfill_stream(
        &self,
        client: &reqwest::Client,
        peer: &str,
        path: &str,
        pages: &mut usize,
    ) {
        let url = cluster::Cluster::endpoint(peer, &format!("/internal/backfill/{path}"));
        let mut cursor = self
            .stream_repair_cursor(peer, path)
            .map(|c| c.position)
            .unwrap_or((i64::MIN, String::new()));
        while *pages > 0 {
            let mut query = vec![
                ("since", cursor.0.to_string()),
                ("since_id", cursor.1.clone()),
                ("limit", ANTI_ENTROPY_PAGE.to_string()),
            ];
            if path == "incident-tombstones" {
                let (node_id, id) = cursor.1.split_once('\0').unwrap_or(("", &cursor.1));
                query.push(("since_node_id", node_id.to_string()));
                query[1].1 = id.to_string();
            }
            let mut req = client.get(&url).query(&query);
            if let Some(t) = &self.inner.peer_token {
                req = req.bearer_auth(t);
            }
            let batch: ReplicationBatch = match req.send().await {
                Ok(r) if r.status().is_success() => match r.json().await {
                    Ok(b) => b,
                    Err(_) => return,
                },
                _ => return,
            };
            let next = match path {
                "alerts" => batch.alerts.last().map(|r| (r.timestamp, r.id.clone())),
                "incidents" => batch
                    .incidents
                    .last()
                    .map(|r| (r.version.millis, format!("{}\0{}", r.version.node_id, r.id))),
                _ => batch
                    .deleted_incidents
                    .last()
                    .map(|r| (r.deleted_at, format!("{}\0{}", r.node_id, r.id))),
            };
            let len = match path {
                "alerts" => batch.alerts.len(),
                "incidents" => batch.incidents.len(),
                _ => batch.deleted_incidents.len(),
            };
            if len == 0 {
                self.set_stream_repair_cursor(peer, path, Some(StreamCursor::completed(cursor)));
                return;
            }
            let page_started = Instant::now();
            match self.apply_replicated_repair(batch).await {
                Ok(true) => {}
                Ok(false) | Err(_) => {
                    self.set_stream_repair_cursor(peer, path, Some(StreamCursor::walking(cursor)));
                    return;
                }
            }
            *pages -= 1;
            if self.inner.timings.enabled {
                self.inner.timings.anti_entropy_page.observe(
                    "anti_entropy",
                    "stream_page",
                    page_started.elapsed(),
                );
            }
            if len < ANTI_ENTROPY_PAGE {
                self.set_stream_repair_cursor(
                    peer,
                    path,
                    Some(StreamCursor::completed(next.unwrap_or(cursor))),
                );
                return;
            }
            let Some(next) = next else {
                self.set_stream_repair_cursor(peer, path, Some(StreamCursor::walking(cursor)));
                return;
            };
            if next <= cursor {
                tracing::warn!(%peer, path, "anti-entropy: stream backfill cursor did not advance");
                self.set_stream_repair_cursor(peer, path, None);
                return;
            }
            cursor = next;
            self.set_stream_repair_cursor(peer, path, Some(StreamCursor::walking(cursor.clone())));
            tokio::task::yield_now().await;
        }
        self.set_stream_repair_cursor(peer, path, Some(StreamCursor::walking(cursor)));
    }

    fn set_stream_repair_cursor(&self, peer: &str, path: &str, cursor: Option<StreamCursor>) {
        let mut cursors = self.inner.repair_cursors.lock();
        cursors.for_peer(peer);
        match path {
            "alerts" => cursors.alerts = cursor,
            "incidents" => cursors.incidents = cursor,
            _ => cursors.incident_tombstones = cursor,
        }
    }

    fn stream_repair_cursor(&self, peer: &str, path: &str) -> Option<StreamCursor> {
        let mut cursors = self.inner.repair_cursors.lock();
        cursors.for_peer(peer);
        match path {
            "alerts" => cursors.alerts.clone(),
            "incidents" => cursors.incidents.clone(),
            _ => cursors.incident_tombstones.clone(),
        }
    }

    /// Rollup anti-entropy (item A). Structured like `reconcile_streams`
    /// rather than folded into it: a rollup digest is compared with
    /// `rollup_behind` instead of stream ordering, and its wire batch is
    /// `RollupBatch` rather than `ReplicationBatch`, so sharing the loop body
    /// would mean branching on `path` for the comparison too, not just for
    /// routing — at that point two short loops read more plainly than one
    /// with two shapes folded into it.
    async fn reconcile_rollup_streams(
        &self,
        client: &reqwest::Client,
        peer: &str,
        their: &DigestResponse,
        our: &DigestResponse,
        pages: &mut usize,
    ) {
        for (path, theirs, ours) in [
            (
                "latency-minute",
                &their.latency_minute_rollups,
                &our.latency_minute_rollups,
            ),
            (
                "latency-hour",
                &their.latency_hour_rollups,
                &our.latency_hour_rollups,
            ),
        ] {
            if *pages == 0 {
                return;
            }
            if !rollup_behind(theirs, ours) {
                self.set_rollup_repair_cursor(peer, path, None);
                continue;
            }
            // Same backoff as `reconcile_streams`: a completed walk resumes
            // from the tail every sweep, and only rescans from the beginning
            // once `STREAM_RESCAN_INTERVAL` has passed, so a peer whose
            // digest keeps looking ahead (its live bucket keeps growing)
            // does not pay for a full walk on every single sweep.
            if self
                .rollup_repair_cursor(peer, path)
                .is_some_and(|c| c.stale())
            {
                self.set_rollup_repair_cursor(peer, path, None);
            }
            self.backfill_rollup_stream(client, peer, path, pages).await;
        }
    }

    async fn backfill_rollup_stream(
        &self,
        client: &reqwest::Client,
        peer: &str,
        path: &str,
        pages: &mut usize,
    ) {
        let url = cluster::Cluster::endpoint(peer, &format!("/internal/backfill/{path}"));
        let mut cursor = self
            .rollup_repair_cursor(peer, path)
            .map(|c| c.position)
            .unwrap_or((i64::MIN, String::new()));
        while *pages > 0 {
            let (since_check_id, since_agent_id) =
                cursor.1.split_once('\0').unwrap_or(("", cursor.1.as_str()));
            let query = [
                ("since", cursor.0.to_string()),
                ("since_check_id", since_check_id.to_string()),
                ("since_agent_id", since_agent_id.to_string()),
                ("limit", ANTI_ENTROPY_PAGE.to_string()),
            ];
            let mut req = client.get(&url).query(&query);
            if let Some(t) = &self.inner.peer_token {
                req = req.bearer_auth(t);
            }
            let batch: RollupBatch = match req.send().await {
                Ok(r) if r.status().is_success() => match r.json().await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(%peer, path, error = %e, "anti-entropy: malformed rollup backfill response");
                        return;
                    }
                },
                Ok(r) => {
                    tracing::warn!(%peer, path, status = %r.status(), "anti-entropy: rollup backfill request rejected");
                    return;
                }
                Err(e) => {
                    tracing::warn!(%peer, path, error = %e, "anti-entropy: rollup backfill request failed");
                    return;
                }
            };
            let len = batch.rows.len();
            if len == 0 {
                self.set_rollup_repair_cursor(peer, path, Some(StreamCursor::completed(cursor)));
                return;
            }
            let next = batch
                .rows
                .last()
                .map(|r| (r.bucket_start, format!("{}\0{}", r.check_id, r.agent_id)));
            let page_started = Instant::now();
            if let Err(error) = self.apply_rollup_backfill(path, batch).await {
                tracing::warn!(%peer, path, %error, "anti-entropy: failed to apply a rollup backfill page");
                self.set_rollup_repair_cursor(peer, path, Some(StreamCursor::walking(cursor)));
                return;
            }
            *pages -= 1;
            if self.inner.timings.enabled {
                self.inner.timings.anti_entropy_page.observe(
                    "anti_entropy",
                    "rollup_page",
                    page_started.elapsed(),
                );
            }
            if len < ANTI_ENTROPY_PAGE {
                self.set_rollup_repair_cursor(
                    peer,
                    path,
                    Some(StreamCursor::completed(next.unwrap_or(cursor))),
                );
                return;
            }
            let Some(next) = next else {
                self.set_rollup_repair_cursor(peer, path, Some(StreamCursor::walking(cursor)));
                return;
            };
            if next <= cursor {
                tracing::warn!(%peer, path, "anti-entropy: rollup backfill cursor did not advance");
                self.set_rollup_repair_cursor(peer, path, None);
                return;
            }
            cursor = next;
            self.set_rollup_repair_cursor(peer, path, Some(StreamCursor::walking(cursor.clone())));
            tokio::task::yield_now().await;
        }
        self.set_rollup_repair_cursor(peer, path, Some(StreamCursor::walking(cursor)));
    }

    /// Merge one backfilled rollup page. Never routed through
    /// `apply_replicated`/`apply_replicated_repair`: those hold
    /// `state_write` because they touch the tracker, current state and
    /// alerts, none of which a rollup row is. Rollups are pure storage, so
    /// this only ever takes the database worker.
    async fn apply_rollup_backfill(&self, path: &str, batch: RollupBatch) -> anyhow::Result<()> {
        let rows = batch
            .rows
            .into_iter()
            .map(RollupRowDto::into_row)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let minute = path == "latency-minute";
        self.db(move |storage| {
            if minute {
                storage.apply_minute_rollups(&rows)
            } else {
                storage.apply_hour_rollups(&rows)
            }
        })
        .await
    }

    fn set_rollup_repair_cursor(&self, peer: &str, path: &str, cursor: Option<StreamCursor>) {
        let mut cursors = self.inner.repair_cursors.lock();
        cursors.for_peer(peer);
        match path {
            "latency-minute" => cursors.latency_minute_rollups = cursor,
            _ => cursors.latency_hour_rollups = cursor,
        }
    }

    fn rollup_repair_cursor(&self, peer: &str, path: &str) -> Option<StreamCursor> {
        let mut cursors = self.inner.repair_cursors.lock();
        cursors.for_peer(peer);
        match path {
            "latency-minute" => cursors.latency_minute_rollups.clone(),
            _ => cursors.latency_hour_rollups.clone(),
        }
    }

    async fn reconcile_config(
        &self,
        client: &reqwest::Client,
        peer: &str,
        their: &DigestResponse,
        our: &DigestResponse,
    ) {
        for kind in configplane::kinds_behind(&their.config, &our.config) {
            self.backfill_config(client, peer, kind).await;
        }
    }

    /// Pulls one config kind from `peer` until its tail, applying each page
    /// through `apply_config_batch` — the walk itself is `backfill_config_kind`,
    /// shared with the global tier's own peer anti-entropy; what differs here
    /// is the apply, which also refreshes in-memory registries.
    async fn backfill_config(
        &self,
        client: &reqwest::Client,
        peer: &str,
        kind: storage::ConfigKind,
    ) {
        let url = cluster::Cluster::endpoint(peer, "/internal/config");
        let peer_token = self.inner.peer_token.clone();
        let result = configplane::backfill_config_kind(
            client,
            &url,
            peer_token.as_deref(),
            kind,
            |entries| async move { self.apply_config_batch(&entries).await },
        )
        .await;
        match result {
            Ok(0) => {}
            Ok(total) => {
                tracing::info!(%peer, kind = kind.as_str(), entries = total, "anti-entropy: config reconciled");
            }
            Err(e) => {
                tracing::warn!(%peer, kind = kind.as_str(), error = %e, "anti-entropy: config pull failed");
            }
        }
    }

    async fn backfill_results(&self, client: &reqwest::Client, peer: &str, pages: &mut usize) {
        let url = cluster::Cluster::endpoint(peer, "/internal/backfill/results");
        let mut cursor = {
            let mut cursors = self.inner.repair_cursors.lock();
            cursors.for_peer(peer);
            cursors
                .results
                .clone()
                .unwrap_or((i64::MIN, String::new(), String::new()))
        };
        while *pages > 0 {
            let mut req = client.get(&url).query(&[
                ("since", cursor.0.to_string()),
                ("since_check_id", cursor.1.clone()),
                ("since_agent_id", cursor.2.clone()),
                ("limit", ANTI_ENTROPY_PAGE.to_string()),
            ]);
            if let Some(t) = &self.inner.peer_token {
                req = req.bearer_auth(t);
            }
            let batch: ReplicationBatch = match req.send().await {
                Ok(r) if r.status().is_success() => match r.json().await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(%peer, error = %e, "anti-entropy: malformed results backfill response");
                        return;
                    }
                },
                Ok(r) => {
                    tracing::warn!(%peer, status = %r.status(), "anti-entropy: results backfill request rejected");
                    return;
                }
                Err(e) => {
                    tracing::warn!(%peer, error = %e, "anti-entropy: results backfill request failed");
                    return;
                }
            };
            let page_len = batch.results.len();
            if page_len == 0 {
                self.set_results_repair_cursor(peer, None);
                return;
            }
            let next = batch.results.last().map(|row| {
                (
                    row.result.timestamp.timestamp_millis(),
                    row.result.check_id.to_string(),
                    row.result.agent_id.clone(),
                )
            });
            let page_started = Instant::now();
            match self.apply_replicated_repair(batch).await {
                Ok(true) => {}
                Ok(false) | Err(_) => {
                    self.set_results_repair_cursor(peer, Some(cursor));
                    return;
                }
            }
            *pages -= 1;
            if self.inner.timings.enabled {
                self.inner.timings.anti_entropy_page.observe(
                    "anti_entropy",
                    "results_page",
                    page_started.elapsed(),
                );
            }
            if page_len < ANTI_ENTROPY_PAGE {
                self.set_results_repair_cursor(peer, None);
                return;
            }
            let Some(next) = next else {
                self.set_results_repair_cursor(peer, Some(cursor));
                return;
            };
            if next <= cursor {
                tracing::warn!(%peer, "anti-entropy: results backfill cursor did not advance");
                self.set_results_repair_cursor(peer, None);
                return;
            }
            cursor = next;
            self.set_results_repair_cursor(peer, Some(cursor.clone()));
            tokio::task::yield_now().await;
        }
        self.set_results_repair_cursor(peer, Some(cursor));
    }

    fn set_results_repair_cursor(&self, peer: &str, cursor: Option<(i64, String, String)>) {
        let mut cursors = self.inner.repair_cursors.lock();
        cursors.for_peer(peer);
        cursors.results = cursor;
    }

    /// `GET /internal/digest` on `peer`, or `None` on any failure — a peer
    /// that is unreachable or rejects the call simply waits for the next
    /// sweep, the same as a dropped replication push does.
    async fn fetch_digest(&self, client: &reqwest::Client, peer: &str) -> Option<DigestResponse> {
        let url = cluster::Cluster::endpoint(peer, "/internal/digest");
        let mut req = client.get(&url);
        if let Some(t) = &self.inner.peer_token {
            req = req.bearer_auth(t);
        }
        match req.send().await {
            Ok(r) if r.status().is_success() => r.json().await.ok(),
            Ok(r) => {
                tracing::warn!(%peer, status = %r.status(), "anti-entropy: digest request rejected");
                None
            }
            Err(e) => {
                tracing::warn!(%peer, error = %e, "anti-entropy: digest request failed");
                None
            }
        }
    }

    /// Page `check_id`'s history from `peer` between `since` (exclusive on
    /// the first page, then advanced with the last row's agent id) and `until`
    /// (inclusive), applying each page as it arrives and yielding between
    /// pages so a large catch-up cannot starve `ingest`.
    #[allow(dead_code)] // Kept for the single-check endpoint's compatibility path.
    async fn backfill_check(
        &self,
        client: &reqwest::Client,
        peer: &str,
        check_id: &str,
        since: i64,
        until: i64,
    ) {
        let url = cluster::Cluster::endpoint(peer, "/internal/backfill");
        let mut cursor = (since, None::<String>);
        loop {
            let mut query = vec![
                ("check_id", check_id.to_string()),
                ("since", cursor.0.to_string()),
                ("until", until.to_string()),
                ("limit", ANTI_ENTROPY_PAGE.to_string()),
            ];
            if let Some(agent_id) = &cursor.1 {
                query.push(("since_agent_id", agent_id.clone()));
            }
            let mut req = client.get(&url).query(&query);
            if let Some(t) = &self.inner.peer_token {
                req = req.bearer_auth(t);
            }
            let batch: ReplicationBatch = match req.send().await {
                Ok(r) if r.status().is_success() => match r.json().await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(%peer, %check_id, error = %e, "anti-entropy: malformed backfill response");
                        return;
                    }
                },
                Ok(r) => {
                    tracing::warn!(%peer, %check_id, status = %r.status(), "anti-entropy: backfill request rejected");
                    return;
                }
                Err(e) => {
                    tracing::warn!(%peer, %check_id, error = %e, "anti-entropy: backfill request failed");
                    return;
                }
            };
            if batch.results.is_empty() {
                return;
            }
            let page_len = batch.results.len();
            let next_cursor = batch.results.last().map(|row| {
                (
                    row.result.timestamp.timestamp_millis(),
                    Some(row.result.agent_id.clone()),
                )
            });
            let page_started = Instant::now();
            if self.apply_replicated(batch).await.is_err() {
                return;
            }
            if self.inner.timings.enabled {
                self.inner.timings.anti_entropy_page.observe(
                    "anti_entropy",
                    "check_page",
                    page_started.elapsed(),
                );
            }
            if page_len < ANTI_ENTROPY_PAGE {
                return;
            }
            let Some(next_cursor) = next_cursor else {
                return;
            };
            if next_cursor <= cursor {
                tracing::warn!(%peer, %check_id, "anti-entropy: backfill cursor did not advance");
                return;
            }
            cursor = next_cursor;
            tokio::task::yield_now().await;
        }
    }

    /// Count distinct checks observed by each requested agent since `since`.
    /// This is a bounded aggregate for fleet-convergence checks; it does not
    /// materialize raw history rows.
    pub async fn result_coverage(
        &self,
        check_ids: Vec<crate::models::CheckId>,
        agent_ids: Vec<String>,
        since: i64,
    ) -> Result<Vec<(String, u64)>> {
        self.db(move |storage| storage.result_coverage(&check_ids, &agent_ids, since))
            .await
    }

    /// Read recent history for a check (newest first), optionally within a time
    /// range and optionally restricted to one evaluated status.
    pub async fn history(
        &self,
        check_id: crate::models::CheckId,
        limit: usize,
        from: Option<i64>,
        to: Option<i64>,
        status: Option<Status>,
    ) -> Result<Vec<storage::HistoryRow>> {
        self.db(move |storage| storage.history(check_id, limit, from, to, status))
            .await
    }

    /// Effective SLA interpretation for a check. This comes from `CheckMeta`,
    /// where inheritance from coordinator defaults has already been resolved.
    pub(crate) fn degraded_is_downtime(&self, check_id: crate::models::CheckId) -> bool {
        self.inner
            .checks
            .read()
            .get(&check_id)
            .map(|meta| meta.degraded_is_downtime)
            .unwrap_or(false)
    }

    /// Recent raw probe results across all checks and agents (newest first),
    /// narrowed by `filter`. Admin-only at the HTTP layer: rows carry the raw
    /// error.
    pub async fn recent_results(
        &self,
        filter: storage::ResultsFilter,
        limit: usize,
    ) -> Result<Vec<storage::ResultRow>> {
        self.db(move |storage| storage.recent_results(&filter, limit))
            .await
    }

    /// Remove a check from this coordinator only. This must not go through the
    /// config plane: an absent local placement is not a replicated deletion.
    /// Raw results and their replication watermarks remain intact so replacing
    /// the same id restores its history, while all local published-state caches
    /// are cleared so an unplaced check cannot remain visible or influence a
    /// later placement.
    pub(in crate::coordinator) async fn unplace_check_local(
        &self,
        check_id: crate::models::CheckId,
    ) -> Result<bool> {
        let state_wait_started = Instant::now();
        let _state_write = self.inner.state_write.lock().await;
        if self.inner.timings.enabled {
            self.inner.timings.local_state_wait.observe(
                "state_write",
                "unplace_wait",
                state_wait_started.elapsed(),
            );
        }
        let removed = self
            .db(move |storage| storage.unplace_check_local(check_id))
            .await?;
        if !removed {
            return Ok(false);
        }
        self.inner.checks.write().remove(&check_id);
        self.inner
            .check_catalogue
            .write()
            .retain(|check| check.id != check_id);
        self.inner.tracker.lock().install_checks(
            StateTracker::new(),
            &std::collections::HashSet::from([check_id]),
        );
        self.inner
            .intermediate_states
            .lock()
            .retain(|(id, _), _| *id != check_id);
        Ok(true)
    }

    /// Persist one check, through the config-replication path so a peer picks
    /// it up too, and refresh the cached metadata. Shared by create and
    /// update so both paths reload identically.
    async fn save_check(&self, check: CheckConfig) -> Response {
        let document = match serde_json::to_string(&check) {
            Ok(d) => d,
            Err(e) => return internal_error(e.into()),
        };
        let entry = match self
            .write_config(
                storage::ConfigKind::Check,
                &check.id.to_string(),
                Some(document),
            )
            .await
        {
            Ok(e) => e,
            Err(e) => return internal_error(e),
        };
        self.replicate_config(vec![entry]);
        info!(check = %check.name, "check saved");
        Json(CheckResponse { check }).into_response()
    }

    /// Persist a check straight through storage, for tests that want one
    /// configured without spinning up the admin API's session and HTTP
    /// layers — creating a check is an admin's job, not what most of these
    /// tests exercise. `Coordinator::new` deliberately never seeds checks
    /// from process configuration (see its doc), so a test-only coordinator
    /// that wants an already-configured check has to put it there itself,
    /// the same way the admin API would once a session existed. Returns the
    /// id it persisted under, which is always `check.id` — this never mints
    /// a fresh identity the way the unauthenticated create path does.
    pub async fn prune_results_for_test(&self, before: i64) -> Result<u64> {
        self.db(move |storage| storage.prune_results(before)).await
    }

    /// Every minute rollup this node holds, for asserting on rollup
    /// anti-entropy directly rather than only through digest equality.
    pub async fn minute_rollups_for_test(&self) -> Result<Vec<storage::RollupRow>> {
        self.db(|storage| storage.minute_rollups_since(i64::MIN, None, None, 10_000))
            .await
    }

    pub async fn create_check_for_test(&self, check: CheckConfig) -> crate::models::CheckId {
        let id = check.id;
        self.save_check(check).await;
        id
    }

    /// The whole catalogue this coordinator currently holds, for asserting
    /// convergence from outside — a global node's reconciliation landed, a
    /// peer's `/internal/config` push applied — without standing up the
    /// admin API's session and HTTP layers just to read a list back.
    pub async fn list_checks_for_test(&self) -> Vec<CheckConfig> {
        self.db(|storage| storage.list_checks())
            .await
            .unwrap_or_default()
    }

    /// This node's own config digest, for asserting an unplace left no
    /// tombstone behind — a tombstone would show up here as the Check kind's
    /// count staying at 1 rather than falling to 0.
    pub async fn config_digest_for_test(&self) -> Vec<storage::ConfigDigest> {
        self.db(|storage| storage.config_digest())
            .await
            .unwrap_or_default()
    }

    /// Test-only bulk provisioning through the production LWW/config replication
    /// path. Entries commit in bounded transactions and are sent as bounded
    /// `/internal/config` batches, rather than turning N checks into N broadcasts.
    pub async fn create_checks_for_test(
        &self,
        checks: Vec<CheckConfig>,
    ) -> Result<Vec<crate::models::CheckId>> {
        let ids: Vec<_> = checks.iter().map(|check| check.id).collect();
        let entries: Result<Vec<_>> = checks
            .into_iter()
            .map(|check| {
                Ok(storage::ConfigEntry {
                    kind: storage::ConfigKind::Check,
                    id: check.id.to_string(),
                    hlc: self.inner.clock.now(),
                    document: Some(serde_json::to_string(&check)?),
                })
            })
            .collect();
        let entries = entries?;
        for batch in Self::config_batches(entries.clone()) {
            self.apply_config_batch(&batch).await?;
        }
        self.replicate_config(entries);
        Ok(ids)
    }

    /// Direct cache count for bounded provisioning waits; public status remains
    /// asserted separately by the caller.
    pub fn check_count_for_test(&self) -> usize {
        self.inner.checks.read().len()
    }

    /// Persisted alerts, for tests that assert dispatch did not lose one.
    pub async fn alerts_for_test(
        &self,
        limit: usize,
    ) -> Vec<crate::coordinator::storage::AlertRow> {
        self.db(move |storage| storage.recent_alerts(limit))
            .await
            .unwrap_or_default()
    }

    /// Persisted incidents, for tests that assert the open/resolve lifecycle.
    pub async fn incidents_for_test(
        &self,
        limit: usize,
    ) -> Vec<crate::coordinator::storage::IncidentRow> {
        self.db(move |storage| storage.list_incidents(limit, None, None))
            .await
            .unwrap_or_default()
    }

    /// An incident's narrative, for tests that assert the timeline the
    /// coordinator wrote rather than only the state it left behind.
    pub async fn updates_for_test(
        &self,
        incident_id: &str,
    ) -> Vec<crate::coordinator::storage::IncidentUpdateRow> {
        let incident_id = incident_id.to_string();
        self.db(move |storage| storage.updates_for_incident(&incident_id))
            .await
            .unwrap_or_default()
    }

    /// The silence window the coordinator recorded for an agent on its last
    /// poll, so a test can assert the declared cadence was actually used rather
    /// than waiting minutes for the window to expire.
    pub fn agent_window_ms_for_test(&self, agent_id: &str) -> Option<i64> {
        self.inner.seen.read().get(agent_id).map(|s| s.window_ms)
    }

    /// Snapshot only atomic counters and file metadata; it never sends a query
    /// to the single DuckDB worker and therefore cannot add a scan to ingestion.
    pub fn metrics(&self) -> Vec<crate::openmetrics::MetricSample> {
        let database = self.inner.database.metrics_snapshot();
        vec![
            crate::openmetrics::MetricSample::new(
                "warden_database_queries_total",
                vec![],
                database.queries_total as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "warden_database_query_errors_total",
                vec![],
                database.query_errors_total as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "warden_database_query_duration_seconds_total",
                vec![],
                database.query_duration_us as f64 / 1_000_000.0,
            ),
            crate::openmetrics::MetricSample::new(
                "warden_database_logical_rows",
                vec![],
                database.logical_rows as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "warden_database_file_bytes",
                vec![],
                database.database_bytes as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "warden_database_wal_bytes",
                vec![],
                database.wal_bytes as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "warden_retention_prune_duration_seconds_total",
                vec![],
                self.inner.timings.prune_duration_us.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            ),
            crate::openmetrics::MetricSample::new(
                "warden_replication_alert_identity_conflicts_total",
                vec![],
                self.inner.alert_identity_conflicts.load(Ordering::Relaxed) as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "warden_replication_raw_data_gaps_total",
                vec![],
                self.inner.replication_raw_data_gaps.load(Ordering::Relaxed) as f64,
            ),
        ]
    }

    pub fn router(self) -> Router {
        Router::new()
            .route(
                "/api/v1/results",
                post(ingest_handler).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
            )
            .route(
                "/metrics",
                get(|State(coord): State<Coordinator>| async move {
                    (
                        [(
                            header::CONTENT_TYPE,
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )],
                        crate::openmetrics::render(&coord.metrics()),
                    )
                }),
            )
            .route("/api/v1/status", get(status_handler))
            .route("/api/v1/checks/:id/history", get(history_handler))
            .route("/api/v1/checks/:id/uptime", get(uptime_handler))
            .route(
                "/api/v1/checks/:id/latency-sparkline",
                get(latency_sparkline_handler),
            )
            // Batched twins of the two routes above. The status page draws a
            // strip and a sparkline per check, so fetching them per check cost
            // one request and one database job each — 1 + N + M for a page load.
            .route("/api/v1/uptime", get(uptime_strips_handler))
            .route("/api/v1/latency-points", get(latency_points_handler))
            .route("/api/v1/alerts", get(alerts_handler))
            // Public reads: the list and the per-incident permalink. The
            // narrative is the point of writing it, so an anonymous visitor
            // gets the same record the operator sees.
            .route("/api/v1/incidents", get(incidents_handler))
            .route("/api/v1/incidents/:id", get(incident_handler))
            .route("/api/v1/checks/:id/incidents", get(check_incidents_handler))
            // Writes are admin-gated inside each handler, like the check
            // editor. There is no acknowledge route: an authored update on a
            // `detected` incident is the acknowledgement.
            .route("/api/v1/admin/incidents", post(create_incident_handler))
            .route(
                "/api/v1/admin/incidents/purge-short",
                post(purge_short_incidents_handler),
            )
            .route(
                "/api/v1/admin/incidents/:id",
                axum::routing::patch(edit_incident_handler).delete(delete_incident_handler),
            )
            .route(
                "/api/v1/admin/incidents/:id/checks",
                axum::routing::put(incident_checks_handler),
            )
            .route(
                "/api/v1/admin/incidents/:id/updates",
                post(incident_update_handler),
            )
            .route("/api/v1/agents", get(agents_handler))
            // The raw probe-results explorer. Admin-only inside the handler:
            // it returns un-redacted errors that quote resolved addresses and
            // expected values, unlike the public per-check history endpoint.
            .route("/api/v1/admin/results", get(results_handler))
            .route("/api/v1/admin/results/count", get(results_count_handler))
            .route("/api/v1/admin/results/purge", post(purge_results_handler))
            .route(
                "/api/v1/admin/rollup-policies",
                get(list_rollup_policies_handler)
                    .put(upsert_rollup_policy_handler)
                    .delete(delete_rollup_policy_handler),
            )
            .route("/api/v1/admin/region-tree", get(region_tree_handler))
            .route("/api/v1/admin/agents/:id", delete(purge_agent_handler))
            // Section names and their display flags. Public because the status
            // page needs them to render groups; it carries no targets.
            .route("/api/v1/sections", get(public_sections_handler))
            .route("/api/v1/ws", get(ws_handler))
            // Auth. `session` is deliberately unauthenticated and never 401s —
            // it is how the page decides whether to offer the admin UI at all.
            .route(
                "/api/v1/login",
                post(login_handler).layer(DefaultBodyLimit::max(16 * 1024)),
            )
            .route("/api/v1/logout", post(logout_handler))
            .route("/api/v1/session", get(session_handler))
            // Check editor. Every route here requires an admin session.
            // Deadman switch: something external pings this, and the check goes
            // DOWN when the pings stop. Unauthenticated by design — the URL is
            // the credential, and it is only ever shown to an admin.
            .route(
                "/api/v1/heartbeat/:id",
                get(heartbeat_handler).post(heartbeat_handler),
            )
            .route(
                "/api/v1/admin/checks/:id/data",
                delete(purge_check_data_handler),
            )
            // The whole erase: the check AND everything it ever produced.
            // Ordered before the `:id` route for the same reason
            // `checks/order` is — a suffix must never be read as a check id.
            .route(
                "/api/v1/admin/checks/:id/everything",
                delete(purge_check_everything_handler),
            )
            // Unplacing is regional: it removes only this coordinator's
            // definition and derived state, never a replicated config tombstone.
            .route(
                "/api/v1/admin/checks/:id/unplace",
                delete(unplace_check_handler),
            )
            // Ordering is its own route, ahead of the `:id` route so "order"
            // is never taken for a check id. Reordering is a bulk operation
            // over ids, not an edit to any one check.
            .route("/api/v1/admin/checks/order", put(reorder_checks_handler))
            .route(
                "/api/v1/admin/checks",
                get(list_checks_handler).post(create_check_handler),
            )
            .route(
                "/api/v1/admin/checks/:id",
                put(update_check_handler).delete(delete_check_handler),
            )
            // Status-page presentation: sections exist independently of checks,
            // so they get their own routes rather than riding on a check edit.
            .route(
                "/api/v1/admin/sections",
                get(list_sections_handler)
                    .post(create_section_handler)
                    .put(reorder_sections_handler),
            )
            .route(
                "/api/v1/admin/sections/:name",
                put(rename_section_handler).delete(delete_section_handler),
            )
            // Which agents the public page names, and how. Admin-only in both
            // directions: `pinned` is a list of agent ids.
            .route(
                "/api/v1/admin/status-page/agents",
                get(agents_display_handler).put(update_agents_display_handler),
            )
            // Where alerts go. Admin-only: a webhook URL is the whole
            // credential for posting into that channel.
            .route(
                "/api/v1/admin/notifiers",
                get(notifiers_handler).put(update_notifiers_handler),
            )
            .route("/api/v1/admin/notifiers/test", post(test_notifier_handler))
            // Scheduled windows that suppress paging. Admin-only: a window is a
            // licence to go quiet, so an anonymous writer could silence the
            // whole fleet.
            .route(
                "/api/v1/admin/maintenance",
                get(maintenance_handler).put(update_maintenance_handler),
            )
            // Try a script before saving it: a typo would otherwise mark a
            // service down every interval until someone noticed.
            .route("/api/v1/admin/eval-test", post(eval_test_handler))
            // What agents probe. Carries targets, so it sits behind the agent
            // token rather than being public like /status.
            .route("/api/v1/agent/checks", get(agent_checks_handler))
            // Per-zone agent credentials. Minting returns the only copy of the
            // secret, so these are admin-gated like the check editor.
            .route(
                "/api/v1/admin/agent-credentials",
                get(list_agent_credentials_handler).post(mint_agent_credential_handler),
            )
            .route(
                "/api/v1/admin/agent-credentials/:id",
                delete(revoke_agent_credential_handler),
            )
            // Peer replication. Agent-gated rather than public: it mutates
            // published state, so an anonymous POST could mark every check down.
            .route("/internal/gossip", post(gossip_handler))
            // Cluster-based data-plane replication: results, alerts and
            // incidents together, in place of the transition-only push
            // above wherever clustering is configured.
            .route(
                "/internal/replicate",
                post(replicate_handler).layer(DefaultBodyLimit::max(32 * 1024 * 1024)),
            )
            // Anti-entropy: a peer's fixed-cost summary, and the paged pull
            // that backfills whatever it says this node is missing.
            .route("/internal/digest", get(digest_handler))
            .route("/internal/backfill", get(backfill_handler))
            .route("/internal/backfill/results", get(results_backfill_handler))
            .route("/internal/backfill/alerts", get(alerts_backfill_handler))
            .route(
                "/internal/backfill/incidents",
                get(incidents_backfill_handler),
            )
            .route(
                "/internal/backfill/incident-tombstones",
                get(incident_tombstones_backfill_handler),
            )
            // Rollup anti-entropy (item A): the only way a peer recovers
            // minute/hour latency summaries for a window it no longer holds
            // raw observations for. See `storage::RollupDigest`.
            .route(
                "/internal/backfill/latency-minute",
                get(latency_minute_backfill_handler),
            )
            .route(
                "/internal/backfill/latency-hour",
                get(latency_hour_backfill_handler),
            )
            // Config-plane push: checks, sections, settings, credentials —
            // last-writer-wins by HLC, applied locally, never observer-owned
            // the way a result is. See `config_handler`.
            .route(
                "/internal/config",
                post(config_handler)
                    .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
                    .get(config_entries_handler),
            )
            // The global tier's own push of the catalogue, gated by the same
            // shared secret a `[coordinator.global]` block already presents
            // for admin fan-out — a region that trusts the global node to
            // drive an admin write trusts it to durably distribute one too.
            // See `global::reconcile`: this is what a global node retries
            // against instead of failing an operator's write when this
            // region was briefly unreachable.
            .route(
                "/internal/global/config",
                post(global_config_handler).layer(DefaultBodyLimit::max(8 * 1024 * 1024)),
            )
            .route(
                "/internal/global/config-digest",
                get(global_config_digest_handler),
            )
            // Just the ids this region holds for the Check kind — the
            // scoped-digest repair path's "what do you actually have" read,
            // made only when a mismatch is already suspected. See
            // `global::reconcile::reconcile_checks`.
            .route("/internal/global/check-ids", get(global_check_ids_handler))
            // Cluster membership. Peer-gated like every other /internal/*
            // route: pinging is how a peer rewrites what this node believes
            // about liveness, so an agent must never be able to call it.
            .route("/internal/cluster/ping", post(cluster_ping_handler))
            // The admin-facing membership view. Admin-gated, not peer-gated:
            // this is read by a human in the browser, not by another node.
            .route("/api/v1/cluster", get(cluster_view_handler))
            // Retention-aware peer recovery (item B): standing raw-data gaps
            // this node has recorded, so an operator can see which checks
            // have holes and over what range instead of only a counter.
            .route("/api/v1/admin/raw-data-gaps", get(raw_data_gaps_handler))
            // Admin JSON is deliberately bounded even when unauthenticated: the
            // limit is enforced before handlers parse or authenticate a body.
            .layer(DefaultBodyLimit::max(1024 * 1024))
            .layer(axum::middleware::from_fn(crate::security::security_headers))
            .with_state(self)
            .fallback(static_handler)
    }
}

// ---- API response DTOs. The consumer is frontend/src/lib/api.ts; keep the two
// in step when changing a shape here. ----

// ---- Handlers ----

/// Keys under which admin-editable settings live. One JSON document each.
const AGENTS_DISPLAY_KEY: &str = "status_page.agents";
const NOTIFIERS_KEY: &str = "notifiers";
const MAINTENANCE_KEY: &str = "maintenance.windows";
/// Keys under which values generated once and reused forever live — not seeded
/// from the config file the way the settings above are, but from randomness,
/// via `load_or_generate`.
pub(crate) const NODE_ID_KEY: &str = "cluster.node_id";
const SESSION_KEY_KEY: &str = "cluster.session_key";
/// Rows per `/internal/backfill` page during an anti-entropy sweep. Bounded
/// so one very-behind check cannot hold `apply_replicated` — and the yield
/// between pages — for an unbounded stretch.
const ANTI_ENTROPY_PAGE: usize = 500;
/// A sweep gets a small, fixed repair budget shared by results and the three
/// lifecycle streams. Local ingest takes the state-write lock before every
/// repair page, so it wins whenever both are ready.
const ANTI_ENTROPY_PAGES_PER_SWEEP: usize = 4;
/// How long a stream walked to its peer's tail keeps that tail before starting
/// over from the beginning. Only a full walk can find a row a peer acquired
/// *below* the position we already passed, so the restart has to keep
/// happening; doing it every sweep is what turns a stream that cannot converge
/// into a permanent replay of the peer's whole history.
const STREAM_RESCAN_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// How often the retry job looks for due notifications. Also the finest
/// backoff granularity there is, which is why the first retry waits exactly
/// this long.
const RETRY_TICK: std::time::Duration = std::time::Duration::from_secs(30);
/// Attempts per notification, the first one included: 30s, 1m, 2m, 4m, 8m,
/// 16m — a bit over half an hour of patience. Long enough to ride out a
/// deploy or a rate limit, short enough that the queue does not become a
/// place where notifications go to be forgotten.
const MAX_DELIVERY_ATTEMPTS: i32 = 6;
/// Retried per tick. A cap so one wedged notifier cannot hold the job for
/// minutes; the rest are simply still due on the next tick.
const RETRY_BATCH: usize = 100;

/// Exponential backoff on the number of failures so far. ponytail: no jitter —
/// one coordinator retrying its own queue has nothing to thunder against, and
/// jitter is the fix when many senders share a target.
fn backoff_ms(attempts: i32) -> i64 {
    let steps = attempts.clamp(1, MAX_DELIVERY_ATTEMPTS) - 1;
    RETRY_TICK.as_millis() as i64 * (1 << steps)
}

/// Fleet liveness for the public status page.
async fn fleet_health(coord: &Coordinator, now: i64) -> Vec<FleetRowDto> {
    let cfg = coord.inner.status_agents.read().clone();
    // Nothing placed by hand and nothing automatic: the section is off, and
    // the aggregate query can be skipped entirely. This rides the busiest
    // public endpoint there is.
    if cfg.mode == crate::config::AgentsMode::Off && cfg.rows.is_empty() {
        return Vec::new();
    }
    // One lock for both reads: they are two halves of the same answer, and
    // taking it twice on the busiest public endpoint buys nothing.
    let (reported, polls) = coord
        .db(move |storage| Ok((storage.list_agents(now)?, storage.list_agent_polls()?)))
        .await
        .unwrap_or_default();
    fleet_rows(now, &coord.inner.seen.read(), &polls, &reported, &cfg)
}

/// Everyone the coordinator knows about, live pollers and past reporters alike,
/// sorted so the picker does not reshuffle between loads.
async fn fleet_members(coord: &Coordinator, now: i64) -> Vec<FleetMemberDto> {
    let mut members: std::collections::HashMap<String, (String, bool)> = coord
        .inner
        .seen
        .read()
        .iter()
        .map(|(id, s)| (id.clone(), (s.region.clone(), now - s.at <= s.window_ms)))
        .collect();
    let (reported, polls) = coord
        .db(move |storage| Ok((storage.list_agents(now)?, storage.list_agent_polls()?)))
        .await
        .unwrap_or_default();
    // Merged the same way the status page merges, so pinning a row on one node
    // offers the same fleet as pinning it on another.
    for p in polls {
        let healthy = now - p.at <= p.window_ms;
        members
            .entry(p.agent_id)
            .and_modify(|e| e.1 = e.1 || healthy)
            .or_insert((p.region, healthy));
    }
    for r in reported {
        members.entry(r.agent_id).or_insert((r.region, false));
    }
    let mut fleet: Vec<FleetMemberDto> = members
        .into_iter()
        .map(|(agent_id, (region, healthy))| FleetMemberDto {
            agent_id,
            region,
            healthy,
        })
        .collect();
    fleet.sort_by(|a, b| (&a.region, &a.agent_id).cmp(&(&b.region, &b.agent_id)));
    fleet
}

async fn ws_handler(State(coord): State<Coordinator>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| handle_ws(socket, coord))
}

async fn handle_ws(mut socket: WebSocket, coord: Coordinator) {
    let mut rx = coord.inner.events.subscribe();
    loop {
        match rx.recv().await {
            Ok(ev) => {
                let txt = match serde_json::to_string(&ev) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                if socket.send(Message::Text(txt)).await.is_err() {
                    break; // client gone
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn internal_error(e: anyhow::Error) -> Response {
    tracing::error!(error = %e, "request failed");
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "internal error",
    )
        .into_response()
}

// ---- Embedded dashboard (built Svelte SPA under frontend/dist) ----

#[derive(rust_embed::RustEmbed)]
#[folder = "frontend/dist"]
struct Assets;

/// Serve the embedded SPA. Unknown non-API paths fall back to index.html so
/// client-side routing works. Unmatched /api paths return 404 (not the SPA).
async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    if path.starts_with("api/") {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }

    let lookup = if path.is_empty() { "index.html" } else { path };
    if let Some(content) = Assets::get(lookup) {
        let mime = mime_guess::from_path(lookup).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response();
    }

    // SPA fallback / unbuilt-frontend placeholder.
    match Assets::get("index.html") {
        Some(content) => Html(content.data).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "dashboard not built — run `bun run build` in frontend/",
        )
            .into_response(),
    }
}

// ---- Auth and the check editor ----

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

    /// Slicing is a scheduling change, never a semantic one: a sliced run must
    /// remove exactly what a single-shot prune removes, and leave the same
    /// rows behind. Runs a day's worth of history through both.
    #[tokio::test]
    async fn a_sliced_prune_removes_exactly_what_one_pass_would() {
        let now = 30 * 86_400_000i64;
        let cfg = CoordinatorConfig {
            db_path: ":memory:".into(),
            retention: crate::config::RetentionConfig {
                results_days: 7,
                ..Default::default()
            },
            ..Default::default()
        };
        let coord = Coordinator::new(&cfg).await.unwrap();
        let check = crate::models::new_check_id();
        let rows: Vec<(crate::models::CheckResult, crate::models::Status)> = (0..30)
            .map(|day| {
                let mut r = crate::models::CheckResult {
                    check_id: check,
                    agent_id: "a".into(),
                    zone: "z".into(),
                    region: "r".into(),
                    kind: crate::models::CheckKind::Http,
                    timestamp: chrono::DateTime::from_timestamp_millis(day * 86_400_000).unwrap(),
                    latency_ms: 1,
                    status_code: Some(200),
                    body_matched: None,
                    error: None,
                    failure: None,
                    body: None,
                    headers: None,
                    metric: None,
                    metrics: None,
                };
                r.agent_id = "a".into();
                (r, crate::models::Status::Up)
            })
            .collect();
        coord
            .db(move |storage| storage.insert_results(&rows))
            .await
            .unwrap();

        let removed = coord.prune_in_slices(7, 90, now).await.unwrap();
        assert_eq!(removed, 23, "every day older than the retention window");
        let left = coord
            .db(move |storage| storage.history(check, 100, None, None, None))
            .await
            .unwrap();
        assert_eq!(left.len(), 7, "the retention window itself survives");
        // Idempotent: a second run has nothing left to take.
        assert_eq!(coord.prune_in_slices(7, 90, now).await.unwrap(), 0);
        let metrics = coord.metrics();
        assert_eq!(
            crate::openmetrics::select(&metrics, "warden_database_logical_rows", &[])
                .unwrap()
                .value,
            0.0
        );
        assert!(
            crate::openmetrics::select(
                &metrics,
                "warden_retention_prune_duration_seconds_total",
                &[]
            )
            .unwrap()
            .value
                > 0.0
        );
    }

    #[tokio::test]
    async fn replicated_alert_identity_conflicts_are_visible_in_metrics() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let check_id = crate::models::new_check_id().to_string();
        let alert = |current_state| ReplicatedAlert {
            id: "shared-alert-id".into(),
            scope: "check".into(),
            node_id: check_id.clone(),
            previous_state: Some(NodeState::Up),
            current_state,
            timestamp: 1_000,
            incident_id: None,
        };
        coord
            .apply_replicated(ReplicationBatch {
                origin: "first-peer".into(),
                alerts: vec![alert(NodeState::Down)],
                ..Default::default()
            })
            .await
            .unwrap();
        coord
            .apply_replicated(ReplicationBatch {
                origin: "conflicting-peer".into(),
                alerts: vec![alert(NodeState::Degraded)],
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(
            crate::openmetrics::select(
                &coord.metrics(),
                "warden_replication_alert_identity_conflicts_total",
                &[]
            )
            .unwrap()
            .value,
            1.0
        );
    }

    #[tokio::test]
    async fn metrics_snapshot_does_not_enqueue_database_work() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let before = coord.metrics();
        let rendered = crate::openmetrics::render(&before);
        let after = crate::openmetrics::parse(&rendered).unwrap();
        assert_eq!(
            crate::openmetrics::select(&before, "warden_database_queries_total", &[])
                .unwrap()
                .value,
            crate::openmetrics::select(&after, "warden_database_queries_total", &[])
                .unwrap()
                .value,
        );
    }
    use super::{incident_title, region_change_narrative, Coordinator};
    use crate::{
        config::{CheckConfig, CoordinatorConfig, GlobalConfig},
        coordinator::{
            routes::peer::{ReplicatedAlert, ReplicationBatch},
            storage::{AffectSource, IncidentCheckRow, IncidentState},
        },
        global::GlobalNode,
        models::{CheckKind, CheckResult, NodeState},
    };
    use std::{collections::BTreeSet, net::SocketAddr, time::Instant};
    use uuid::Uuid;

    fn regions(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    async fn serve(app: axum::Router) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    }

    fn percentile(samples: &mut [u128], percentile: usize) -> u128 {
        samples.sort_unstable();
        samples[(samples.len() * percentile).div_ceil(100).saturating_sub(1)]
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn assert_ceiling(name: &str, actual: u128) {
        if let Ok(value) = std::env::var(name) {
            let ceiling = value
                .parse::<u128>()
                .expect("benchmark ceiling must be an integer");
            assert!(actual <= ceiling, "{name}: {actual} exceeded {ceiling}");
        }
    }

    #[tokio::test]
    async fn applied_check_entries_update_only_their_cached_metadata() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let first = CheckConfig {
            name: "first".into(),
            target: "https://first.invalid".into(),
            ..Default::default()
        };
        let second = CheckConfig {
            name: "second".into(),
            target: "https://second.invalid".into(),
            ..Default::default()
        };
        let entry = |check: &CheckConfig, millis| super::storage::ConfigEntry {
            kind: super::storage::ConfigKind::Check,
            id: check.id.to_string(),
            hlc: super::hlc::Hlc::new(millis, "peer"),
            document: Some(serde_json::to_string(check).unwrap()),
        };
        coord
            .apply_config_batch(&[entry(&first, 1), entry(&second, 1)])
            .await
            .unwrap();

        let mut replacement = first.clone();
        replacement.name = "renamed".into();
        replacement.enabled = false;
        replacement.notify = false;
        replacement.notifiers = vec!["primary".into()];
        replacement.agents = vec!["agent-a".into()];
        replacement.depends_on = vec![second.id];
        coord
            .apply_config_batch(&[entry(&replacement, 2)])
            .await
            .unwrap();

        {
            let checks = coord.inner.checks.read();
            let updated = checks.get(&first.id).unwrap();
            assert_eq!(updated.name, "renamed");
            assert!(!updated.enabled);
            assert!(!updated.notify);
            assert_eq!(updated.notifiers, ["primary"]);
            assert_eq!(updated.agents, ["agent-a"]);
            assert_eq!(updated.depends_on, [second.id]);
            assert_eq!(checks.get(&second.id).unwrap().name, "second");
        }

        coord
            .apply_config_batch(&[super::storage::ConfigEntry {
                kind: super::storage::ConfigKind::Check,
                id: first.id.to_string(),
                hlc: super::hlc::Hlc::new(3, "peer"),
                document: None,
            }])
            .await
            .unwrap();
        let checks = coord.inner.checks.read();
        assert!(!checks.contains_key(&first.id));
        assert_eq!(checks.get(&second.id).unwrap().name, "second");
        let catalogue = coord.inner.check_catalogue.read();
        assert_eq!(catalogue.len(), 1);
        assert_eq!(catalogue[0].id, second.id);
    }

    /// SCALING.md "History survives unplacement": unplace drops the local
    /// definition and clears published state, but raw results are keyed by
    /// id and untouched. Re-placing the same id — exactly what a global
    /// reconciliation push does, `Storage::upsert_check` under a fresh
    /// document — must bring the check back with its history intact rather
    /// than starting a fresh, empty one.
    #[tokio::test]
    async fn unplace_then_replace_restores_history_by_id() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let check = coord
            .create_check_for_test(CheckConfig {
                name: "web".into(),
                target: "https://example.invalid".into(),
                ..Default::default()
            })
            .await;
        coord
            .ingest(vec![CheckResult {
                check_id: check,
                agent_id: "agent".into(),
                zone: "zone".into(),
                region: "region".into(),
                kind: CheckKind::Http,
                timestamp: chrono::DateTime::from_timestamp_millis(1_000).unwrap(),
                latency_ms: 5,
                status_code: Some(200),
                body_matched: None,
                error: None,
                failure: None,
                body: None,
                headers: None,
                metric: None,
                metrics: None,
            }])
            .await;
        let before = coord
            .recent_results(
                super::storage::ResultsFilter {
                    check_id: Some(check),
                    ..Default::default()
                },
                10,
            )
            .await
            .unwrap();
        assert_eq!(before.len(), 1, "the observation landed");

        assert!(coord.unplace_check_local(check).await.unwrap());
        assert!(
            coord.list_checks_for_test().await.is_empty(),
            "the definition is gone locally"
        );
        let during = coord
            .recent_results(
                super::storage::ResultsFilter {
                    check_id: Some(check),
                    ..Default::default()
                },
                10,
            )
            .await
            .unwrap();
        assert_eq!(during.len(), 1, "raw history is untouched by unplacing");

        // Re-placement, same id, exactly the shape a global reconciliation
        // push applies.
        coord
            .create_checks_for_test(vec![CheckConfig {
                id: check,
                name: "web".into(),
                target: "https://example.invalid".into(),
                ..Default::default()
            }])
            .await
            .unwrap();
        assert_eq!(coord.list_checks_for_test().await.len(), 1);
        let after = coord
            .recent_results(
                super::storage::ResultsFilter {
                    check_id: Some(check),
                    ..Default::default()
                },
                10,
            )
            .await
            .unwrap();
        assert_eq!(
            after.len(),
            1,
            "history reconnects by id with no plumbing on re-placement"
        );
        assert_eq!(after[0].status_code, Some(200));
    }

    #[tokio::test]
    async fn failed_ingest_does_not_publish_uncommitted_state() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let check = coord
            .create_check_for_test(CheckConfig {
                name: "transaction-test".into(),
                target: "https://example.invalid".into(),
                ..Default::default()
            })
            .await;
        let result = |code, millis| CheckResult {
            check_id: check,
            agent_id: "agent".into(),
            zone: "zone".into(),
            region: "region".into(),
            kind: CheckKind::Http,
            timestamp: chrono::DateTime::from_timestamp_millis(millis).unwrap(),
            latency_ms: 1,
            status_code: Some(code),
            body_matched: None,
            error: None,
            failure: None,
            body: None,
            headers: None,
            metric: None,
            metrics: None,
        };
        coord.ingest(vec![result(200, 1_000)]).await;
        let intermediate_before = coord.inner.intermediate_states.lock().clone();
        coord
            .db(|store| {
                store.insert_alert(
                    super::storage::AlertScope::Check,
                    "other",
                    None,
                    NodeState::Down,
                    1,
                    None,
                )?;
                store
                    .execute_test_sql("CREATE UNIQUE INDEX fail_down ON alerts (current_state)")?;
                Ok(())
            })
            .await
            .unwrap();
        coord.ingest(vec![result(500, 2_000)]).await;
        assert_eq!(coord.inner.tracker.lock().state(check), Some(NodeState::Up));
        assert_eq!(coord.inner.tracker.lock().failures(check, "agent"), 0);
        assert_eq!(*coord.inner.intermediate_states.lock(), intermediate_before);
        assert_eq!(coord.db(|store| store.count_results()).await.unwrap(), 1);
        coord
            .db(|store| {
                store.execute_test_sql("DROP INDEX fail_down")?;
                Ok(())
            })
            .await
            .unwrap();
        coord.ingest(vec![result(500, 2_000)]).await;
        assert_eq!(
            coord.inner.tracker.lock().state(check),
            Some(NodeState::Down)
        );
        assert_eq!(coord.db(|store| store.count_results()).await.unwrap(), 2);
        assert_eq!(
            coord
                .alerts_for_test(10)
                .await
                .iter()
                .filter(|a| a.node_id == check.to_string())
                .count(),
            1
        );
    }

    #[tokio::test]
    #[ignore = "sustained-ingest benchmark; configure WARDEN_BENCH_* for the intended scale"]
    async fn benchmark_sustained_coordinator_ingest() {
        let check_count = env_usize("WARDEN_BENCH_CHECKS", 10_000);
        let domain_count = env_usize("WARDEN_BENCH_DOMAINS", 1);
        let batch_count = env_usize("WARDEN_BENCH_BATCHES", 20).max(1);
        assert!(check_count > 0 && domain_count > 0);

        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "benchmark-region".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let checks: Vec<_> = (0..check_count)
            .map(|index| CheckConfig {
                id: Uuid::new_v5(&Uuid::NAMESPACE_OID, index.to_string().as_bytes()),
                name: format!("check-{index}"),
                target: "https://example.invalid".into(),
                notify: false,
                position: index as i32,
                ..Default::default()
            })
            .collect();
        let stored = checks.clone();
        coord
            .db(move |storage| {
                for check in &stored {
                    storage.upsert_check(check, 1)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        coord.reload_checks().await.unwrap();

        let rows_per_batch = check_count * domain_count;
        let mut batch_ms = Vec::with_capacity(batch_count);
        let benchmark_started = Instant::now();
        for batch in 0..batch_count {
            let timestamp = chrono::Utc::now() + chrono::Duration::milliseconds(batch as i64);
            let mut results = Vec::with_capacity(rows_per_batch);
            for domain in 0..domain_count {
                for check in &checks {
                    results.push(CheckResult {
                        check_id: check.id,
                        agent_id: format!("agent-{domain}-{}", check.position % 3),
                        zone: format!("zone-{domain}"),
                        region: format!("region-{domain}"),
                        kind: CheckKind::Http,
                        timestamp,
                        latency_ms: 10 + (check.position as u64 % 200),
                        status_code: Some(200),
                        body_matched: None,
                        error: None,
                        failure: None,
                        body: None,
                        headers: None,
                        metric: None,
                        metrics: None,
                    });
                }
            }
            let started = Instant::now();
            coord.ingest(results).await;
            batch_ms.push(started.elapsed().as_millis());
        }
        let elapsed = benchmark_started.elapsed();
        let total_rows = rows_per_batch * batch_count;
        let rows_per_second = total_rows as f64 / elapsed.as_secs_f64();
        let stored_rows = coord.db(|storage| storage.count_results()).await.unwrap() as usize;
        assert_eq!(stored_rows, total_rows);

        let batch_p50 = percentile(&mut batch_ms, 50);
        let batch_p95 = percentile(&mut batch_ms, 95);
        let batch_p99 = percentile(&mut batch_ms, 99);
        let batch_max = *batch_ms.last().unwrap();
        assert_ceiling("WARDEN_BENCH_MAX_INGEST_BATCH_P95_MS", batch_p95);
        if let Ok(value) = std::env::var("WARDEN_BENCH_MIN_INGEST_ROWS_PER_SECOND") {
            let minimum = value
                .parse::<f64>()
                .expect("benchmark floor must be a number");
            assert!(
                rows_per_second >= minimum,
                "WARDEN_BENCH_MIN_INGEST_ROWS_PER_SECOND: {rows_per_second:.2} below {minimum:.2}"
            );
        }
        println!(
            "checks={check_count} domains={domain_count} batches={batch_count} rows_per_batch={rows_per_batch} total_rows={total_rows} ingest_rows_per_second={rows_per_second:.2} batch_p50_ms={batch_p50} batch_p95_ms={batch_p95} batch_p99_ms={batch_p99} batch_max_ms={batch_max}"
        );
    }

    #[tokio::test]
    #[ignore = "production-cardinality benchmark; configure WARDEN_BENCH_* for the intended scale"]
    async fn benchmark_status_and_global_push_production_cardinality() {
        let check_count = env_usize("WARDEN_BENCH_CHECKS", 10_000);
        let region_count = env_usize("WARDEN_BENCH_REGIONS", 3);
        let sample_count = env_usize("WARDEN_BENCH_SAMPLES", 20).max(1);
        let token = "benchmark-global-token";
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "region-0".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let checks: Vec<_> = (0..check_count)
            .map(|index| CheckConfig {
                id: Uuid::new_v5(&Uuid::NAMESPACE_OID, index.to_string().as_bytes()),
                name: format!("check-{index}"),
                target: "https://example.invalid".into(),
                notify: false,
                latency_graph: true,
                position: index as i32,
                ..Default::default()
            })
            .collect();
        let stored = checks.clone();
        coord
            .db(move |storage| {
                for check in &stored {
                    storage.upsert_check(check, 1)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        coord.reload_checks().await.unwrap();
        let now = chrono::Utc::now();
        coord
            .ingest(
                checks
                    .iter()
                    .map(|check| CheckResult {
                        check_id: check.id,
                        agent_id: "benchmark-agent".into(),
                        zone: "benchmark-zone".into(),
                        region: "region-0".into(),
                        kind: CheckKind::Http,
                        timestamp: now,
                        latency_ms: 10,
                        status_code: Some(200),
                        body_matched: None,
                        error: None,
                        failure: None,
                        body: None,
                        headers: None,
                        metric: None,
                        metrics: None,
                    })
                    .collect(),
            )
            .await;

        let regional_addr = serve(coord.clone().router()).await;
        let global_addr = serve(
            GlobalNode::new(&GlobalConfig {
                token: Some(token.into()),
                db_path: ":memory:".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .router(),
        )
        .await;
        let client = reqwest::Client::new();
        // `DEFAULT_PAGE_SIZE` mirrors the frontend's own `STATUS_PAGE_SIZE`
        // (`frontend/src/App.svelte`): this is the page a real dashboard load
        // asks for, not the raw endpoint's ceiling. The `_max` URLs separately
        // measure that ceiling — `crate::page::MAX_PAGE_SIZE` — so the report
        // carries both "what a browser does" and "what a single request can
        // still be made to cost".
        const DEFAULT_PAGE_SIZE: usize = 100;
        let regional_url = format!("http://{regional_addr}/api/v1/status");
        let uptime_url = format!("http://{regional_addr}/api/v1/uptime?days=30&buckets=90");
        let latency_url =
            format!("http://{regional_addr}/api/v1/latency-points?hours=720&buckets=90");
        let regional_max_url = format!(
            "http://{regional_addr}/api/v1/status?limit={}",
            crate::page::MAX_PAGE_SIZE
        );
        let uptime_max_url = format!(
            "http://{regional_addr}/api/v1/uptime?days=30&buckets=90&limit={}",
            crate::page::MAX_PAGE_SIZE
        );
        let latency_max_url = format!(
            "http://{regional_addr}/api/v1/latency-points?hours=720&buckets=90&limit={}",
            crate::page::MAX_PAGE_SIZE
        );
        let global_url = format!("http://{global_addr}/api/v1/status");
        let push_url = format!("http://{global_addr}/internal/global/state");
        let page_len = check_count.min(DEFAULT_PAGE_SIZE);
        let max_page_len = check_count.min(crate::page::MAX_PAGE_SIZE);

        let warm = client.get(&regional_url).send().await.unwrap();
        assert_eq!(warm.status(), reqwest::StatusCode::OK);
        let mut regional_ms = Vec::with_capacity(sample_count);
        let mut regional_bytes = 0;
        for _ in 0..sample_count {
            let started = Instant::now();
            let response = client.get(&regional_url).send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let body = response.bytes().await.unwrap();
            regional_ms.push(started.elapsed().as_millis());
            regional_bytes = body.len();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["checks"].as_array().unwrap().len(), page_len);
            assert_eq!(
                value["total_checks"].as_u64().unwrap() as usize,
                check_count
            );
        }

        let warm = tokio::join!(
            client.get(&regional_url).send(),
            client.get(&uptime_url).send(),
            client.get(&latency_url).send(),
        );
        assert!(warm.0.unwrap().status().is_success());
        assert!(warm.1.unwrap().status().is_success());
        assert!(warm.2.unwrap().status().is_success());
        let mut page_ms = Vec::with_capacity(sample_count);
        let mut page_bytes = 0;
        for _ in 0..sample_count {
            let started = Instant::now();
            let (status, uptime, latency) = tokio::join!(
                client.get(&regional_url).send(),
                client.get(&uptime_url).send(),
                client.get(&latency_url).send(),
            );
            let (status, uptime, latency) = (status.unwrap(), uptime.unwrap(), latency.unwrap());
            assert!(status.status().is_success());
            assert!(uptime.status().is_success());
            assert!(latency.status().is_success());
            let (status, uptime, latency) =
                tokio::join!(status.bytes(), uptime.bytes(), latency.bytes());
            let (status, uptime, latency) = (status.unwrap(), uptime.unwrap(), latency.unwrap());
            page_ms.push(started.elapsed().as_millis());
            page_bytes = status.len() + uptime.len() + latency.len();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&status).unwrap()["total_checks"]
                    .as_u64()
                    .unwrap() as usize,
                check_count
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&uptime).unwrap()["checks"]
                    .as_array()
                    .unwrap()
                    .len(),
                page_len
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&latency).unwrap()["checks"]
                    .as_array()
                    .unwrap()
                    .len(),
                page_len
            );
        }

        // The ceiling case: a client that explicitly asks for the largest page
        // the server will still hand out (`MAX_PAGE_SIZE`), which is what the
        // old unbounded read effectively did for every check at once. This is
        // not what the dashboard requests, but it is the worst a single
        // request can still cost, and that number belongs in the same report.
        let warm = tokio::join!(
            client.get(&regional_max_url).send(),
            client.get(&uptime_max_url).send(),
            client.get(&latency_max_url).send(),
        );
        assert!(warm.0.unwrap().status().is_success());
        assert!(warm.1.unwrap().status().is_success());
        assert!(warm.2.unwrap().status().is_success());
        let mut page_max_ms = Vec::with_capacity(sample_count);
        let mut page_max_bytes = 0;
        for _ in 0..sample_count {
            let started = Instant::now();
            let (status, uptime, latency) = tokio::join!(
                client.get(&regional_max_url).send(),
                client.get(&uptime_max_url).send(),
                client.get(&latency_max_url).send(),
            );
            let (status, uptime, latency) = (status.unwrap(), uptime.unwrap(), latency.unwrap());
            assert!(status.status().is_success());
            assert!(uptime.status().is_success());
            assert!(latency.status().is_success());
            let (status, uptime, latency) =
                tokio::join!(status.bytes(), uptime.bytes(), latency.bytes());
            let (status, uptime, latency) = (status.unwrap(), uptime.unwrap(), latency.unwrap());
            page_max_ms.push(started.elapsed().as_millis());
            page_max_bytes = status.len() + uptime.len() + latency.len();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&uptime).unwrap()["checks"]
                    .as_array()
                    .unwrap()
                    .len(),
                max_page_len
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&latency).unwrap()["checks"]
                    .as_array()
                    .unwrap()
                    .len(),
                max_page_len
            );
        }

        let mut push_ms = Vec::with_capacity(sample_count * region_count);
        let mut push_bytes = 0;
        for sample in 0..sample_count {
            for region in 0..region_count {
                let started = Instant::now();
                let mut push = coord.build_state_push().await;
                push.region = format!("region-{region}");
                push.origin = format!("node-{region}-{sample}");
                push.pushed_at += sample as i64 + 1;
                push_bytes = serde_json::to_vec(&push).unwrap().len();
                let response = client
                    .post(&push_url)
                    .bearer_auth(token)
                    .json(&push)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), reqwest::StatusCode::OK);
                push_ms.push(started.elapsed().as_millis());
            }
        }

        let warm = client.get(&global_url).send().await.unwrap();
        assert_eq!(warm.status(), reqwest::StatusCode::OK);
        let mut global_ms = Vec::with_capacity(sample_count);
        let mut global_bytes = 0;
        for _ in 0..sample_count {
            let started = Instant::now();
            let response = client.get(&global_url).send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let body = response.bytes().await.unwrap();
            global_ms.push(started.elapsed().as_millis());
            global_bytes = body.len();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["checks"].as_array().unwrap().len(), page_len);
            assert_eq!(
                value["total_checks"].as_u64().unwrap() as usize,
                check_count
            );
            assert_eq!(value["agents"].as_array().unwrap().len(), region_count);
        }

        let regional_p95 = percentile(&mut regional_ms, 95);
        let page_p95 = percentile(&mut page_ms, 95);
        let page_max_p95 = percentile(&mut page_max_ms, 95);
        let push_p95 = percentile(&mut push_ms, 95);
        let global_p95 = percentile(&mut global_ms, 95);
        assert_ceiling("WARDEN_BENCH_MAX_REGIONAL_STATUS_P95_MS", regional_p95);
        assert_ceiling("WARDEN_BENCH_MAX_STATUS_PAGE_P95_MS", page_p95);
        assert_ceiling("WARDEN_BENCH_MAX_STATUS_PAGE_BYTES", page_bytes as u128);
        assert_ceiling("WARDEN_BENCH_MAX_STATUS_PAGE_MAX_P95_MS", page_max_p95);
        assert_ceiling(
            "WARDEN_BENCH_MAX_STATUS_PAGE_MAX_BYTES",
            page_max_bytes as u128,
        );
        assert_ceiling("WARDEN_BENCH_MAX_GLOBAL_PUSH_P95_MS", push_p95);
        assert_ceiling("WARDEN_BENCH_MAX_GLOBAL_STATUS_P95_MS", global_p95);
        assert_ceiling("WARDEN_BENCH_MAX_PUSH_BYTES", push_bytes as u128);
        println!(
            "checks={check_count} regions={region_count} samples={sample_count} page_size={page_len} regional_status_p95_ms={regional_p95} regional_status_bytes={regional_bytes} status_page_p95_ms={page_p95} status_page_bytes={page_bytes} status_page_requests=3 status_page_max_p95_ms={page_max_p95} status_page_max_bytes={page_max_bytes} status_page_max_size={max_page_len} global_push_p95_ms={push_p95} push_bytes={push_bytes} global_status_p95_ms={global_p95} global_status_bytes={global_bytes}"
        );
    }

    #[test]
    fn degraded_title_names_sorted_failing_regions() {
        assert_eq!(
            incident_title(
                "api",
                NodeState::Degraded,
                &regions(&["us-west", "eu-central"]),
            ),
            "api partial outage in eu-central, us-west"
        );
    }

    #[test]
    fn down_title_remains_plain_and_sensible() {
        assert_eq!(
            incident_title("api", NodeState::Down, &regions(&["eu-central"])),
            "api is down"
        );
    }

    #[test]
    fn region_change_narrative_records_old_and_new_sets() {
        assert_eq!(
            region_change_narrative(&regions(&["eu"]), &regions(&["us", "apac"])),
            "Affected failing regions changed from eu to apac, us."
        );
    }

    #[tokio::test]
    async fn reconciliation_closes_a_stale_open_incident_for_an_up_check() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let check_id = Uuid::new_v4();
        let test_coord = coord.clone();
        let resolved = coord
            .db(move |store| {
                store.update_current_state(check_id, NodeState::Up, None, 2_000)?;
                store.create_incident(
                    "stale",
                    "upstream is down",
                    IncidentState::Detected,
                    1_000,
                    false,
                    Some(check_id),
                    &[IncidentCheckRow {
                        check_id,
                        source: AffectSource::Origin,
                    }],
                    Some(("timeout", None)),
                )?;
                test_coord.reconcile_open_incidents(store, 3_000)
            })
            .await
            .unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].state, IncidentState::Resolved);
        assert_eq!(resolved[0].resolved_at, Some(3_000));
        let test_coord = coord.clone();
        assert!(coord
            .db(move |store| test_coord.reconcile_open_incidents(store, 4_000))
            .await
            .unwrap()
            .is_empty());
    }
}

#[derive(Serialize)]
struct DeletedResponse {
    deleted: String,
}
