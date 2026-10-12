pub mod alert;
pub mod backup;
pub mod cluster;
pub mod database;
pub mod db_limits;
mod dispatch;
mod fleet;
mod global_push;
pub mod gossip;
pub mod hlc;
mod ingest;
mod replication;
mod replication_out;
mod rollup_tree;
mod router;
mod routes;
pub mod state;
pub mod storage;

use crate::config::{CheckConfig, CoordinatorConfig};
use crate::configplane::{self};
use crate::credential::AgentCredential;
use crate::models::{CheckKind, NodeState, Status};
use crate::rollup::RollupPolicy;
use anyhow::Result;
use axum::response::{IntoResponse, Response};
use axum::Json;
use database::Database;
use fleet::unreachable_after_ms;
use ingest::{meta_by_id, CheckMeta};
use parking_lot::Mutex;
use replication::RepairCursors;
use router::internal_error;
use routes::checks::CheckResponse;
pub use routes::incidents::{IncidentCheckDto, IncidentDto, IncidentUpdateDto};
pub use routes::peer::{
    CheckDigestDto, DigestResponse, ReplicatedAlert, ReplicatedBelief, ReplicatedResult,
    ReplicatedState, ReplicationBatch, RollupDigestDto, StreamDigestDto,
};
use serde::Serialize;
use state::StateTracker;
use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use storage::Storage;
use tokio::sync::broadcast;
use tracing::{info, warn};

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
                target: "lighthouse_bench",
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
            enabled: std::env::var_os("LIGHTHOUSE_BENCH_TIMINGS").as_deref()
                == Some(std::ffi::OsStr::new("1"))
                || std::env::var("RUST_LOG")
                    .is_ok_and(|filter| filter.contains("lighthouse_bench")),
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
    /// Anti-entropy sweeps that aborted before reaching reconciliation: no
    /// client, an unreachable or malformed-response peer, or a failed local
    /// digest read. Distinct from `replication_raw_data_gaps`, which fires
    /// only once a sweep *did* complete a comparison and found one peer
    /// behind retention.
    replication_anti_entropy_failures: AtomicU64,
    /// Epoch milliseconds of the last anti-entropy sweep that reached
    /// reconciliation (got both digests and started comparing), so an
    /// operator can alert on reconciliation lag directly rather than
    /// inferring it from the absence of other signals. `0` until the first
    /// sweep succeeds.
    replication_anti_entropy_last_success_ms: AtomicI64,
    /// Rows removed by the retention job, lifetime. Paired with
    /// `timings.prune_duration_us` for "how long, how much" on each sweep.
    retention_rows_removed: AtomicU64,
    /// Queued notifications rescheduled for another attempt, and rows that
    /// exhausted `MAX_DELIVERY_ATTEMPTS` with nobody paged.
    notification_retries: AtomicU64,
    notification_giveups: AtomicU64,
    /// Concurrent `ingest` calls admitted right now, and batches shed because
    /// `MAX_INGEST_INFLIGHT` was already reached. Bounding this protects the
    /// shared database worker queue (see `database::Database`'s doc) from an
    /// unbounded pile-up of callers all awaiting the same serialized write
    /// path; a caller sees its batch dropped rather than queued indefinitely.
    ingest_inflight: AtomicU64,
    ingest_rejected: AtomicU64,
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
    /// The `Bearer` the global tier presents to drive admin operations here on
    /// behalf of an operator signed in at the global dashboard: the relay
    /// token derived from this region's registration key
    /// (`credential::relay_token`), so it is specific to this region. `None`
    /// without `[coordinator.global]`, so a Stage A coordinator accepts admin
    /// only via a signed session as before.
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

impl crate::http::AdminGate for Coordinator {
    fn is_admin_request(&self, headers: &axum::http::HeaderMap) -> bool {
        routes::public::is_admin(self, headers)
    }
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
        let database = Database::open(
            cfg.db_path.clone(),
            db_limits::DatabaseLimits::resolve(&cfg.database)?,
        )
        .await?;
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
            warn!(
                "no admin password set (LIGHTHOUSE_ADMIN_PASSWORD); the check editor is disabled"
            );
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
        if global_push.is_some() && cfg.resolved_advertise_url().is_none() {
            warn!(
                "[coordinator.global] is set but advertise_url is not: the global node learns \
                 where to reach this region from its pushes, so it cannot relay or distribute \
                 the catalogue here"
            );
        }
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
                replication_anti_entropy_failures: AtomicU64::new(0),
                replication_anti_entropy_last_success_ms: AtomicI64::new(0),
                retention_rows_removed: AtomicU64::new(0),
                notification_retries: AtomicU64::new(0),
                notification_giveups: AtomicU64::new(0),
                ingest_inflight: AtomicU64::new(0),
                ingest_rejected: AtomicU64::new(0),
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
                // The global tier calls back with a token derived from this
                // region's key, never the key itself; see `regions`.
                admin_trust_token: global_push
                    .as_ref()
                    .map(|(_, key, _)| crate::credential::relay_token(key)),
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
            let mut persisted = Vec::new();
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
                cluster::persist_membership(
                    &heartbeat_cluster,
                    &heartbeat_coord.inner.database,
                    &mut persisted,
                )
                .await;
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
            let mut sent = std::collections::HashMap::<String, storage::AgentPollRow>::new();
            loop {
                tick.tick().await;
                let current: Vec<storage::AgentPollRow> = coord
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
                        coordinator: Some(coord.inner.node_id.clone()),
                    })
                    .collect();
                let present: std::collections::HashSet<&str> =
                    current.iter().map(|row| row.agent_id.as_str()).collect();
                sent.retain(|agent_id, _| present.contains(agent_id.as_str()));
                let snapshot: Vec<storage::AgentPollRow> = current
                    .into_iter()
                    .filter(|row| {
                        sent.get(&row.agent_id)
                            .is_none_or(|prev| row.supersedes(prev))
                    })
                    .collect();
                if snapshot.is_empty() {
                    continue;
                }
                sent.extend(
                    snapshot
                        .iter()
                        .map(|row| (row.agent_id.clone(), row.clone())),
                );
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
    /// The time range one raw-result delete job covers. The database worker
    /// runs one job at a time, so a whole day's deletion in one job is one
    /// stall every probe result and dashboard read waits behind. Slicing does
    /// not make retention cheaper, it makes it interruptible. Five minutes
    /// keeps a slice near DuckDB's measured ~76 ns per deleted row × 3M rows
    /// ≈ 230 ms even at 100k checks × 3 domains × 30 s cadence (SCALING.md
    /// "Raw-result storage evolution"), and costs only 288 small jobs a day.
    const RETENTION_SLICE_MS: i64 = 5 * 60_000;

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
                let step = Self::RETENTION_SLICE_MS;
                let mut edge = oldest.saturating_add(step);
                while edge < cutoff {
                    let sliced = self.db(move |storage| storage.prune_results(edge)).await?;
                    removed += sliced;
                    // An empty slice means a gap in history: jump to the next
                    // retained row rather than walking empty five-minute steps.
                    let next = if sliced == 0 {
                        self.db(|storage| storage.oldest_result_ts())
                            .await?
                            .map_or(cutoff, |oldest| oldest.max(edge))
                    } else {
                        edge
                    };
                    edge = next.saturating_add(step);
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
        self.inner
            .retention_rows_removed
            .fetch_add(removed, Ordering::Relaxed);
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

    /// Every incident this coordinator holds, for asserting from outside that
    /// a write relayed by the global node landed here exactly once.
    pub async fn list_incidents_for_test(&self) -> Vec<storage::IncidentRow> {
        self.db(|storage| storage.list_incidents(10_000, None, None))
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
        let mut samples = vec![
            crate::openmetrics::MetricSample::new(
                "lighthouse_database_queries_total",
                vec![],
                database.queries_total as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_database_query_errors_total",
                vec![],
                database.query_errors_total as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_database_query_duration_seconds_total",
                vec![],
                database.query_duration_us as f64 / 1_000_000.0,
            ),
            // Lock/queue wait vs execution: both counted against the same
            // `queries_total` denominator, so an operator can see whether a
            // slow scrape's cost is jobs waiting behind a busy worker or the
            // queries themselves getting slower.
            crate::openmetrics::MetricSample::new(
                "lighthouse_database_queue_wait_seconds_total",
                vec![],
                database.queue_wait_us as f64 / 1_000_000.0,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_database_queue_depth",
                vec![],
                database.queue_depth as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_database_logical_rows",
                vec![],
                database.logical_rows as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_database_file_bytes",
                vec![],
                database.database_bytes as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_database_wal_bytes",
                vec![],
                database.wal_bytes as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_retention_prune_duration_seconds_total",
                vec![],
                self.inner.timings.prune_duration_us.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_retention_rows_removed_total",
                vec![],
                self.inner.retention_rows_removed.load(Ordering::Relaxed) as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_replication_alert_identity_conflicts_total",
                vec![],
                self.inner.alert_identity_conflicts.load(Ordering::Relaxed) as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_replication_raw_data_gaps_total",
                vec![],
                self.inner.replication_raw_data_gaps.load(Ordering::Relaxed) as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_replication_anti_entropy_failures_total",
                vec![],
                self.inner
                    .replication_anti_entropy_failures
                    .load(Ordering::Relaxed) as f64,
            ),
            // Absolute timestamp rather than a server-computed age: an age
            // goes stale between scrapes, while `time() - this` in the
            // alerting rule is always current. `0` (unix epoch) until the
            // first sweep succeeds, which reads as "always overdue" rather
            // than a false "just happened".
            crate::openmetrics::MetricSample::new(
                "lighthouse_replication_anti_entropy_last_success_timestamp_seconds",
                vec![],
                self.inner
                    .replication_anti_entropy_last_success_ms
                    .load(Ordering::Relaxed) as f64
                    / 1_000.0,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_notification_retries_total",
                vec![],
                self.inner.notification_retries.load(Ordering::Relaxed) as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_notification_giveups_total",
                vec![],
                self.inner.notification_giveups.load(Ordering::Relaxed) as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_ingest_inflight",
                vec![],
                self.inner.ingest_inflight.load(Ordering::Relaxed) as f64,
            ),
            crate::openmetrics::MetricSample::new(
                "lighthouse_ingest_rejected_total",
                vec![],
                self.inner.ingest_rejected.load(Ordering::Relaxed) as f64,
            ),
        ];
        if let Some(cluster) = &self.inner.cluster {
            samples.extend(cluster.metrics());
        }
        samples
    }
}

/// Keys under which admin-editable settings live. One JSON document each.
const AGENTS_DISPLAY_KEY: &str = "status_page.agents";
const NOTIFIERS_KEY: &str = "notifiers";
const MAINTENANCE_KEY: &str = "maintenance.windows";
/// Keys under which values generated once and reused forever live — not seeded
/// from the config file the way the settings above are, but from randomness,
/// via `load_or_generate`.
pub(crate) const NODE_ID_KEY: &str = "cluster.node_id";
const SESSION_KEY_KEY: &str = "cluster.session_key";

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
            crate::openmetrics::select(&metrics, "lighthouse_database_logical_rows", &[])
                .unwrap()
                .value,
            0.0
        );
        assert!(
            crate::openmetrics::select(
                &metrics,
                "lighthouse_retention_prune_duration_seconds_total",
                &[]
            )
            .unwrap()
            .value
                > 0.0
        );
        assert_eq!(
            crate::openmetrics::select(&metrics, "lighthouse_retention_rows_removed_total", &[])
                .unwrap()
                .value,
            23.0,
            "lifetime rows removed must track the actual prune, not just exist"
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
            crate::openmetrics::select(&before, "lighthouse_database_queries_total", &[])
                .unwrap()
                .value,
            crate::openmetrics::select(&after, "lighthouse_database_queries_total", &[])
                .unwrap()
                .value,
        );
    }

    use super::Coordinator;
    use crate::config::CoordinatorConfig;
}
