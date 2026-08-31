pub mod alert;
pub mod cluster;
mod fleet;
pub mod gossip;
pub mod hlc;
mod routes;
pub mod state;
pub mod storage;

use crate::config::{CheckConfig, CoordinatorConfig};
use crate::credential::AgentCredential;
use crate::models::{eval, CheckKind, CheckResult, Status};
use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use fleet::{fleet_rows, unreachable_after_ms, FleetMemberDto, FleetRowDto};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::RngCore;
use routes::agents::{
    agent_checks_handler, agents_handler, ingest_handler, list_agent_credentials_handler,
    mint_agent_credential_handler, purge_agent_handler, revoke_agent_credential_handler,
};
use routes::checks::{
    create_check_handler, delete_check_handler, eval_test_handler, heartbeat_handler,
    list_checks_handler, purge_check_data_handler, purge_check_everything_handler,
    reorder_checks_handler, update_check_handler, CheckResponse,
};
use routes::incidents::{
    check_incidents_handler, create_incident_handler, delete_incident_handler,
    edit_incident_handler, incident_checks_handler, incident_handler, incident_update_handler,
    incidents_handler,
};
pub use routes::incidents::{IncidentCheckDto, IncidentDto, IncidentUpdateDto};
use routes::peer::{
    backfill_handler, cluster_ping_handler, cluster_view_handler, config_entries_handler,
    config_handler, digest_handler, gossip_handler, read_config_entry_for_replication,
    replicate_handler, ConfigBatch,
};
pub use routes::peer::{
    CheckDigestDto, ConfigDigestDto, DigestResponse, ReplicatedAlert, ReplicatedResult,
    ReplicationBatch,
};
use routes::public::{
    alerts_handler, history_handler, latency_sparkline_handler, results_handler, status_handler,
    uptime_handler,
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
use std::sync::Arc;
use storage::Storage;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// A live event broadcast to WebSocket clients. Two families: plain state
/// changes (matching the persisted alert shape) and incident lifecycle events,
/// so the UI can keep both in sync without polling. Serialized form matches the
/// dashboard API contract.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsEvent {
    StateChange {
        check_id: String,
        from: Status,
        to: Status,
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

/// Configured metadata for a check, looked up by check_id. The store only knows
/// check_ids, so anything declared in config (grouping, expectations, probe type)
/// has to be joined back in here.
struct CheckMeta {
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
    depends_on: Vec<String>,
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

/// Index config rows by check name for the lookups ingest and the status page do.
fn meta_by_name(checks: &[CheckConfig]) -> std::collections::HashMap<String, CheckMeta> {
    checks
        .iter()
        .map(|c| {
            (
                c.name.clone(),
                CheckMeta {
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
                },
            )
        })
        .collect()
}

struct Inner {
    storage: Mutex<Storage>,
    tracker: Mutex<StateTracker>,
    /// How long history is kept. Copied here so the retention job does not
    /// need to re-read config.
    retention: crate::config::RetentionConfig,
    /// Alerts leave through here, never inline. Always present: the notifier
    /// list is editable at runtime, so a channel that existed only when the
    /// config file named one would mean the first notifier added in the admin
    /// page never fired until a restart.
    /// The name list is what the check asked for, resolved against the live
    /// notifiers at send time: editing a backend must not change the meaning
    /// of an alert already in the queue.
    notify_tx: tokio::sync::mpsc::Sender<(alert::Transition, Vec<String>)>,
    /// Notification backends, edited in the admin page. Shared with the
    /// dispatcher task, which re-reads them per alert rather than capturing a
    /// copy at boot.
    notifiers: Arc<parking_lot::RwLock<Vec<crate::notify::Notifier>>>,
    events: broadcast::Sender<WsEvent>,
    /// Checks absent from this map fall back to defaults: no group, and the
    /// standard 2xx-3xx rule. Behind a lock because the admin API edits checks
    /// at runtime; ingest reads it per batch, so it stays a cached map rather
    /// than a query.
    checks: parking_lot::RwLock<std::collections::HashMap<String, CheckMeta>>,
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
    /// `checks` are the config-file checks. They seed the store the first time
    /// it runs against an empty `checks` table; afterwards the store is
    /// authoritative so runtime edits survive a restart.
    ///
    /// When no checks exist at all the demo seed runs: a real deployment must
    /// not have fake monitors in its store.
    pub fn new(cfg: &CoordinatorConfig, checks: &[CheckConfig]) -> Result<Self> {
        let storage = Storage::open(&cfg.db_path)?;
        let now = chrono::Utc::now().timestamp_millis();
        // Who was reporting before this process started, so a restart does not
        // briefly forget the fleet and over-assign every check.
        let reported_fleet = storage.list_agents(now).unwrap_or_default();

        // Both seeded from the config file the first time only; after that the
        // store is authoritative, or restarting would silently revert the admin
        // page's edits. Same contract as checks.
        let status_agents = load_or_seed(&storage, AGENTS_DISPLAY_KEY, &cfg.status_page.agents);
        let notifiers = Arc::new(parking_lot::RwLock::new(load_or_seed(
            &storage,
            NOTIFIERS_KEY,
            &cfg.notifiers,
        )));
        // Store-only, with no config-file counterpart: a window is booked at a
        // moment, and nobody redeploys to schedule one.
        let maintenance: Vec<crate::maintenance::MaintenanceWindow> =
            load_or_seed(&storage, MAINTENANCE_KEY, &Vec::new());

        // The dispatcher runs beside ingest, not inside it. Bounded so a wedged
        // endpoint cannot grow an unbounded backlog in memory; overflow drops
        // the notification rather than the results behind it.
        //
        // Spawned below, once `Self` exists: a failed send is persisted to the
        // retry queue, so the dispatcher needs the store.
        let (notify_tx, rx) = tokio::sync::mpsc::channel::<(alert::Transition, Vec<String>)>(1024);
        if storage.seed_checks_if_empty(checks, now)? {
            info!(count = checks.len(), "seeded checks from config file");
        }
        // A deployment that predates the sections table has group names only on
        // its checks; adopt them so the admin page isn't empty on first open.
        storage.seed_sections_from_checks()?;
        let stored = storage.list_checks()?;
        // No checks anywhere — seed demo data so the dashboard isn't blank.
        //
        // Never in a cluster. A node with `peers` set has an empty store
        // because its config is *on its way*, not because nobody has
        // configured anything, so the emptiness that means "new install, show
        // them something" on a single node means "wait a moment" here. And the
        // demo rows do not stay local: results replicate, so every node ends up
        // showing a fake `seed` agent alongside the real fleet, and purging it
        // anywhere is undone by the next anti-entropy sweep pulling it back
        // from a peer that still has it — result deletes have no tombstone.
        // Skipping also saves each joining node 18-26s of inserts at boot.
        if stored.is_empty() && cfg.peers.is_empty() && storage.seed_mock(now)? {
            info!("no checks configured, seeded demo data");
        }
        // Stable across restarts either way: config-resolved when set, else
        // generated once and persisted so a restart rejoins as itself instead
        // of appearing as a new member.
        let node_id = cfg
            .resolved_node_id()
            .unwrap_or_else(|| load_or_generate(&storage, NODE_ID_KEY, 16));
        // The session signing key must be identical on every node so any node
        // can validate a token any other node issued (see auth.rs). `peer_token`
        // is already shared and stable, so it doubles as that material when
        // clustering is on; standalone falls back to a persisted random key so
        // a restart still does not force every admin to log back in.
        let session_key = match cfg.resolved_peer_token() {
            Some(token) => token.into_bytes(),
            None => load_or_generate(&storage, SESSION_KEY_KEY, 32).into_bytes(),
        };
        let auth = crate::auth::Auth::with_signing_key(cfg.resolved_admin_password(), &session_key);
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
                    node_id.clone(),
                    url,
                    now,
                    cfg.peers.clone(),
                    Some(token),
                    cfg.peer_timeout_secs,
                );
                match storage.list_cluster_members() {
                    Ok(members) => c.restore(members),
                    Err(e) => warn!(error = %e, "could not load persisted cluster members"),
                }
                Some(Arc::new(c))
            }
            _ => None,
        };
        let clock = hlc::Clock::new(node_id.clone());
        let credentials = storage.list_agent_credentials().unwrap_or_default();
        let live = credentials.iter().filter(|c| !c.revoked()).count();
        if live == 0 {
            warn!(
                "no agent credentials exist; agent endpoints will refuse every \
                 request until one is minted from Admin > Credentials"
            );
        } else {
            info!(count = live, "agent credentials loaded");
        }
        // Resume confirmations in flight. The confirmed status and the failure
        // count persist in `current_state`, so a check two failures into a
        // three-failure confirmation picks up where it left off after a restart
        // instead of starting over — or alerting on the first result.
        let mut tracker = StateTracker::new();
        if let Ok(state) = storage.list_current_state() {
            let n = state.len();
            for row in state {
                tracker.seed(&row.check_id, row.status, row.consecutive_failures);
            }
            if n > 0 {
                info!(checks = n, "seeded state tracker from current_state");
            }
        }
        let (events, _) = broadcast::channel(256);
        let coord = Self {
            inner: Arc::new(Inner {
                storage: Mutex::new(storage),
                tracker: Mutex::new(tracker),
                retention: cfg.retention.clone(),
                notify_tx,
                notifiers,
                events,
                checks: parking_lot::RwLock::new(meta_by_name(&stored)),
                seen: parking_lot::RwLock::new(
                    reported_fleet
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
                credentials: parking_lot::RwLock::new(
                    credentials.into_iter().map(|c| (c.id.clone(), c)).collect(),
                ),
                peer_token: cfg.resolved_peer_token(),
                gossip: gossip::Peers::new(&cfg.peers, cfg.resolved_peer_token().as_deref())
                    .map(Arc::new),
                missed_polls: cfg.missed_polls,
                status_agents: parking_lot::RwLock::new(status_agents),
                maintenance: parking_lot::RwLock::new(maintenance),
                node_id,
                clock,
                cluster,
                gossip_interval_secs: cfg.gossip_interval_secs,
                anti_entropy_secs: cfg.anti_entropy_secs,
            }),
        };
        coord.spawn_dispatcher(rx);
        coord.apply_provisioned_credentials(&cfg.provision)?;
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
    fn apply_provisioned_credentials(
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
            self.provision_agent_credential(&token, zone, region)?;
            info!(%id, zone, region, "provisioned agent credential from config");
        }
        Ok(())
    }

    /// Drain the alert channel, sending each transition to the notifiers the
    /// check routed it to. A failure is persisted rather than logged and
    /// forgotten: a 502 from Slack used to lose the page outright.
    fn spawn_dispatcher(
        &self,
        mut rx: tokio::sync::mpsc::Receiver<(alert::Transition, Vec<String>)>,
    ) {
        let coord = self.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            while let Some((t, routed_to)) = rx.recv().await {
                // Cloned per alert: the list is edited at runtime, and a lock
                // must not be held across the send.
                let all = coord.inner.notifiers.read().clone();
                let targets = crate::notify::targets(&all, &routed_to);
                if targets.is_empty() {
                    // Either nothing is marked default, or the check names
                    // notifiers that no longer exist. Both are silent
                    // otherwise, and both mean nobody was told. Deliberately
                    // not queued: there is nobody for a retry to reach.
                    warn!(
                        check = %t.check_id,
                        routed_to = ?routed_to,
                        "alert matched no notifier"
                    );
                }
                for notifier in targets {
                    if let Err(e) = crate::notify::send(&client, notifier, &t).await {
                        warn!(error = %e, check = %t.check_id, notifier = %notifier.name, "notification failed, queued for retry");
                        coord.enqueue_retry(&notifier.name, &t, &e);
                    }
                }
            }
        });
    }

    /// Persist a failed notification for another attempt.
    fn enqueue_retry(&self, notifier: &str, t: &alert::Transition, error: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        let row = storage::QueuedRow {
            notifier: notifier.to_string(),
            check_id: t.check_id.clone(),
            previous_state: Some(t.from),
            current_state: t.to,
            attempts: 1,
            next_attempt: Some(now + backoff_ms(1)),
            last_error: error.to_string(),
            queued_at: now,
        };
        if let Err(e) = self.inner.storage.lock().enqueue_notification(&row) {
            // Nothing left to fall back to; say so rather than implying the
            // retry exists.
            tracing::error!(error = %e, check = %t.check_id, notifier = %notifier, "failed to queue notification for retry");
        }
    }

    /// Retry every queued notification that is due, returning how many were
    /// delivered.
    ///
    /// `now_millis` is a parameter rather than read inside, exactly as `prune`
    /// takes one: a test must be able to reach the next attempt without waiting
    /// out the backoff.
    pub async fn retry_due(&self, now_millis: i64) -> usize {
        let due = match self
            .inner
            .storage
            .lock()
            .due_notifications(now_millis, RETRY_BATCH)
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
        let client = reqwest::Client::new();
        let mut delivered = 0;
        for row in due {
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
                self.drop_queued(&row);
                continue;
            };
            let t = alert::Transition {
                check_id: row.check_id.clone(),
                from: row.previous_state.unwrap_or(Status::Unknown),
                to: row.current_state,
            };
            match crate::notify::send(&client, &notifier, &t).await {
                Ok(()) => {
                    info!(
                        check = %row.check_id,
                        notifier = %row.notifier,
                        attempts = row.attempts + 1,
                        "queued notification delivered"
                    );
                    self.drop_queued(&row);
                    delivered += 1;
                }
                Err(e) => {
                    let attempts = row.attempts + 1;
                    // Out of attempts: keep the row with no next attempt, which
                    // is what the admin panel reads as "nobody was paged".
                    let next = (attempts < MAX_DELIVERY_ATTEMPTS)
                        .then(|| now_millis + backoff_ms(attempts));
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
                    if let Err(e) = self.inner.storage.lock().enqueue_notification(&next_row) {
                        tracing::error!(error = %e, "failed to reschedule a notification");
                    }
                }
            }
        }
        delivered
    }

    fn drop_queued(&self, row: &storage::QueuedRow) {
        if let Err(e) = self.inner.storage.lock().delete_notification(
            &row.notifier,
            &row.check_id,
            row.queued_at,
        ) {
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
                for (node_id, state) in heartbeat_cluster.sweep(now) {
                    let _ = heartbeat_coord
                        .inner
                        .events
                        .send(WsEvent::ClusterChange { node_id, state });
                }
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

    /// Replace the published fleet summary, through the config-replication
    /// path so a peer picks it up too. `write_config` persists before
    /// `apply_config_batch` swaps the live cache, so a failed write cannot
    /// leave a setting live that a restart would forget.
    fn save_agents_display(&self, next: crate::config::AgentsDisplay) -> Result<()> {
        let json = serde_json::to_string(&next)?;
        let entry =
            self.write_config(storage::ConfigKind::Setting, AGENTS_DISPLAY_KEY, Some(json))?;
        self.replicate_config(vec![entry]);
        Ok(())
    }

    /// Replace the notifier list, through the config-replication path. A
    /// failed write cannot leave the dispatcher sending to something no
    /// restart would remember, for the same reason as any other config edit.
    fn save_notifiers(&self, next: Vec<crate::notify::Notifier>) -> Result<()> {
        let json = serde_json::to_string(&next)?;
        let entry = self.write_config(storage::ConfigKind::Setting, NOTIFIERS_KEY, Some(json))?;
        self.replicate_config(vec![entry]);
        Ok(())
    }

    /// Replace the maintenance windows, through the config-replication path:
    /// a window that only suppressed paging on the node it was created on
    /// would silently under-cover the rest of the fleet.
    fn save_maintenance(&self, next: Vec<crate::maintenance::MaintenanceWindow>) -> Result<()> {
        let json = serde_json::to_string(&next)?;
        let entry = self.write_config(storage::ConfigKind::Setting, MAINTENANCE_KEY, Some(json))?;
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
    fn notify_targets(&self, check_id: &str, at: i64) -> Option<Vec<String>> {
        if self
            .inner
            .maintenance
            .read()
            .iter()
            .any(|w| w.covers(check_id, at))
        {
            return None;
        }
        match self.inner.checks.read().get(check_id) {
            Some(m) if !m.notify => None,
            Some(m) => Some(m.notifiers.clone()),
            None => Some(Vec::new()),
        }
    }

    /// Persist a credential, make it live immediately, and push it to every
    /// peer so an operator mints once for the fleet rather than once per
    /// coordinator.
    fn store_credential(&self, record: &AgentCredential) -> Result<()> {
        self.inner.storage.lock().insert_agent_credential(record)?;
        self.reload_credentials()?;
        self.restamp_and_push(storage::ConfigKind::Credential, &record.id);
        Ok(())
    }

    /// Provision a credential whose token is already known — for a zone whose
    /// agents have to be configured before this coordinator first runs.
    pub fn provision_agent_credential(&self, token: &str, zone: &str, region: &str) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let record = crate::credential::provision(token, zone, region, now)
            .ok_or_else(|| anyhow::anyhow!("malformed agent credential token"))?;
        self.store_credential(&record)
    }

    /// Re-read credentials after a mint or revoke, so a revocation takes effect
    /// on the next request instead of at the next restart.
    fn reload_credentials(&self) -> Result<()> {
        let rows = self.inner.storage.lock().list_agent_credentials()?;
        *self.inner.credentials.write() = rows.into_iter().map(|c| (c.id.clone(), c)).collect();
        Ok(())
    }

    /// Re-read check definitions after an admin edit so ingest and the status
    /// page pick them up without a restart.
    fn reload_checks(&self) -> Result<Vec<CheckConfig>> {
        let stored = self.inner.storage.lock().list_checks()?;
        *self.inner.checks.write() = meta_by_name(&stored);
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
    fn reload_setting_cache(&self, id: &str) {
        fn read<T: serde::de::DeserializeOwned + Default>(store: &Storage, key: &str) -> T {
            store
                .get_setting(key)
                .ok()
                .flatten()
                .and_then(|json| serde_json::from_str(&json).ok())
                .unwrap_or_default()
        }
        match id {
            NOTIFIERS_KEY => {
                let next = read(&self.inner.storage.lock(), NOTIFIERS_KEY);
                *self.inner.notifiers.write() = next;
            }
            MAINTENANCE_KEY => {
                let next = read(&self.inner.storage.lock(), MAINTENANCE_KEY);
                *self.inner.maintenance.write() = next;
            }
            AGENTS_DISPLAY_KEY => {
                let next = read(&self.inner.storage.lock(), AGENTS_DISPLAY_KEY);
                *self.inner.status_agents.write() = next;
            }
            _ => {}
        }
    }

    /// Apply a batch of config entries — checks, sections, settings,
    /// credentials — through the one HLC-arbitrated write path
    /// (`Storage::apply_config_entry`), then refresh whichever in-memory
    /// caches that path bypasses. The single point both `config_handler`
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
    fn apply_config_batch(&self, entries: &[storage::ConfigEntry]) -> Result<()> {
        let mut touched_checks = false;
        let mut touched_credentials = false;
        let mut touched_settings: Vec<String> = Vec::new();
        {
            let store = self.inner.storage.lock();
            for raw in entries {
                // Every stamp this node hears, local or remote, is observed —
                // without it a node whose clock lags would keep losing every
                // merge, local edits included, and could never overwrite what
                // it is replacing.
                self.inner.clock.observe(&raw.hlc);
                let mut e = raw.clone();
                if e.kind == storage::ConfigKind::Setting {
                    if let Some(doc) = e.document.take() {
                        e.document = Some(serde_json::to_string(&doc).unwrap_or(doc));
                    }
                }
                match store.apply_config_entry(&e) {
                    Ok(true) => match e.kind {
                        storage::ConfigKind::Check => touched_checks = true,
                        storage::ConfigKind::Credential => touched_credentials = true,
                        storage::ConfigKind::Setting => touched_settings.push(e.id.clone()),
                        // Sections have no in-memory cache of their own —
                        // every reader queries the table directly.
                        storage::ConfigKind::Section => {}
                    },
                    // Lost the merge: a local or peer stamp already at or
                    // ahead of this one. Not an error — the row is exactly
                    // as it should be.
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            kind = e.kind.as_str(),
                            id = %e.id,
                            "config: failed to apply an entry, skipping it"
                        );
                    }
                }
            }
        } // drop the storage guard before any reload below re-locks it
        if touched_checks {
            self.reload_checks()?;
        }
        if touched_credentials {
            self.reload_credentials()?;
        }
        for id in touched_settings {
            self.reload_setting_cache(&id);
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
    fn write_config(
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
        self.apply_config_batch(std::slice::from_ref(&entry))?;
        Ok(entry)
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
        let live = cluster.live_urls();
        if live.is_empty() {
            return;
        }
        let batch = ConfigBatch {
            origin: self.inner.node_id.clone(),
            entries,
        };
        let cluster = Arc::clone(cluster);
        tokio::spawn(async move {
            cluster.broadcast_json("/internal/config", &batch).await;
        });
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
    fn restamp_and_push(&self, kind: storage::ConfigKind, id: &str) {
        let existing = {
            let store = self.inner.storage.lock();
            read_config_entry_for_replication(&store, kind, id)
        };
        let Some(existing) = existing else {
            return;
        };
        match self.write_config(kind, id, existing.document) {
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
        let batch = ReplicationBatch {
            origin: self.inner.node_id.clone(),
            deleted_incidents: ids,
            ..Default::default()
        };
        tokio::spawn(async move {
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
                    })
                    .collect();
                if snapshot.is_empty() {
                    continue;
                }
                {
                    let store = coord.inner.storage.lock();
                    for p in &snapshot {
                        if let Err(e) = store.record_agent_poll(
                            &p.agent_id,
                            &p.zone,
                            &p.region,
                            p.at,
                            p.window_ms,
                            p.rtt_ms,
                        ) {
                            tracing::warn!(error = %e, agent = %p.agent_id, "could not persist agent poll");
                        }
                    }
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
    pub fn spawn_retention(&self) {
        let retention = self.inner.retention.clone();
        let coord = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(24 * 60 * 60));
            loop {
                tick.tick().await;
                let now = chrono::Utc::now().timestamp_millis();
                match coord.inner.storage.lock().prune(
                    retention.results_days,
                    retention.alerts_days,
                    now,
                ) {
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
        ts: i64,
    ) -> anyhow::Result<(String, bool)> {
        if let Some(existing) = store.open_incident_for(&r.check_id)? {
            return Ok((existing.id, false));
        }
        let upstreams: Vec<String> = {
            let by_name = self.inner.checks.read();
            by_name
                .get(&r.check_id)
                .map(|m| m.depends_on.clone())
                .unwrap_or_default()
        };
        for up in upstreams {
            if let Some(inc) = store.open_incident_for(&up)? {
                store.attach_check(&inc.id, &r.check_id, storage::AffectSource::Dependency)?;
                return Ok((inc.id, false));
            }
        }
        let id = format!("{}-{ts}", r.check_id);
        let cause = incident_details(r).unwrap_or_else(|| "check is failing".into());
        store.create_incident(
            &id,
            &format!("{} is down", r.check_id),
            storage::IncidentState::Detected,
            ts,
            false,
            Some(&r.check_id),
            &[storage::IncidentCheckRow {
                check_id: r.check_id.clone(),
                source: storage::AffectSource::Origin,
            }],
            Some((&cause, None)),
        )?;
        Ok((id, true))
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
        for check_id in inc.gating_checks() {
            let runs = {
                let by_name = self.inner.checks.read();
                by_name
                    .get(check_id)
                    .map(|m| m.failures_before_down)
                    .unwrap_or(1)
            };
            if !store.recovered_for(check_id, runs)? {
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

    /// Core ingest path: eval each result, detect transitions, persist, alert,
    /// and broadcast alert-worthy transitions to live WebSocket clients.
    pub async fn ingest(&self, results: Vec<CheckResult>) {
        let mut rows = Vec::with_capacity(results.len());
        let mut transitions = Vec::new();
        // Confirmed status per check after this batch, so incident opens are
        // gated on the threshold: a DOWN still inside its confirmation window
        // is not an outage yet.
        let mut confirmed: std::collections::HashMap<String, Status> =
            std::collections::HashMap::new();
        // (check_id, confirmed status, failure count, last check ts). Built
        // under the tracker lock and applied to storage afterwards, because the
        // two guards must never nest.
        let mut state_updates = Vec::with_capacity(results.len());

        {
            let mut tracker = self.inner.tracker.lock();
            for mut r in results {
                let (expect, script, threshold, default_status) = {
                    let by_name = self.inner.checks.read();
                    let meta = by_name.get(&r.check_id);
                    (
                        meta.and_then(|m| m.expect_status),
                        meta.and_then(|m| m.eval.clone()),
                        meta.map(|m| m.failures_before_down).unwrap_or(1),
                        meta.map(|m| m.default_status).unwrap_or(Status::Unknown),
                    )
                };
                let status = match script.as_deref() {
                    Some(src) => script_verdict(src, &mut r),
                    None => eval(&r, expect),
                };
                if let Some(t) = tracker.observe(&r.check_id, status, threshold) {
                    let ts = r.timestamp.timestamp_millis();
                    transitions.push((t, ts));
                }
                // A check still inside its confirmation window has not flipped;
                // it reads as it did before its first failed probe.
                let confirmed_status = tracker.current(&r.check_id).unwrap_or(default_status);
                confirmed.insert(r.check_id.clone(), confirmed_status);
                let ts = r.timestamp.timestamp_millis();
                state_updates.push((
                    r.check_id.clone(),
                    confirmed_status,
                    tracker.failures(&r.check_id),
                    ts,
                ));
                // The body is only here to feed the script. Dropping it before
                // persistence keeps 64KB per result out of the database.
                r.body = None;
                r.headers = None;
                rows.push((r, status));
            }
        } // drop tracker guard before any await
          // Incidents are driven by observations, not transitions, so a restart
          // (which forgets the tracker's baseline) cannot strand a check in the
          // wrong incident state. The open incident is the source of truth.
          //
          // Order matters here: incidents are reconciled BEFORE alerts are
          // written, because an alert is stamped with the incident it belongs to
          // and that is only knowable once this batch's opens and joins have
          // happened.
        let mut incident_events = Vec::new();
        // check_id -> the incident it belonged to during this batch. Captured
        // even when the incident then resolves, so the recovery alert is
        // stamped with the outage it ended rather than with nothing.
        let mut incident_for: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        // Collected alongside the local writes below, for the cluster-based
        // replication push after the storage guard drops. Empty when
        // clustering is not configured — nobody reads them in that case.
        let mut replicated_alerts: Vec<ReplicatedAlert> = Vec::new();
        let mut replicated_incidents: Vec<storage::ReplicatedIncident> = Vec::new();
        {
            let store = self.inner.storage.lock();
            if let Err(e) = store.insert_results(&rows) {
                tracing::error!(error = %e, "failed to persist results");
            }
            for (check_id, cs_status, failures, ts) in &state_updates {
                if let Err(e) = store.update_current_state(check_id, *cs_status, *failures, *ts) {
                    tracing::error!(error = %e, check = %check_id, "failed to persist current state");
                }
            }
            for (r, status) in &rows {
                let ts = r.timestamp.timestamp_millis();
                match status {
                    Status::Down => {
                        // A DOWN that has not yet confirmed (fewer than
                        // `failures_before_down` consecutive probes) is not an
                        // outage: no incident, and a later recovery cannot be
                        // misread as a fix for something that never opened.
                        if confirmed.get(&r.check_id) != Some(&Status::Down) {
                            continue;
                        }
                        match self.attach_or_open(&store, r, ts) {
                            Ok((id, opened)) => {
                                if opened {
                                    if let Ok(Some(i)) = store.incident(&id) {
                                        incident_events.push(WsEvent::IncidentOpened {
                                            incident: IncidentDto::from_row(i, ts, false),
                                        });
                                    }
                                }
                                incident_for.insert(r.check_id.clone(), id);
                            }
                            Err(e) => {
                                tracing::error!(error = %e, check = %r.check_id, "failed to reconcile incident");
                            }
                        }
                    }
                    // A recovery does not resolve on sight: every gating check
                    // has to clear its own `failures_before_down` worth of UP
                    // observations first, so a flapping check stays inside one
                    // incident instead of opening and closing twenty.
                    Status::Up => match store.open_incident_for(&r.check_id) {
                        Ok(Some(inc)) => {
                            incident_for.insert(r.check_id.clone(), inc.id.clone());
                            match self.try_resolve(&store, &inc, ts) {
                                Ok(true) => {
                                    if let Ok(Some(i)) = store.incident(&inc.id) {
                                        let ack = store.is_acknowledged(&inc.id).unwrap_or(false);
                                        incident_events.push(WsEvent::IncidentUpdated {
                                            incident: IncidentDto::from_row(i, ts, ack),
                                        });
                                    }
                                }
                                Ok(false) => {}
                                Err(e) => {
                                    tracing::error!(error = %e, incident = %inc.id, "failed to resolve incident");
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::error!(error = %e, check = %r.check_id, "failed to look up incident");
                        }
                    },
                    // An un-evaluable probe is not a confirmed recovery, so it
                    // leaves whatever is open exactly as it is.
                    Status::Unknown => {}
                }
            }
            for (t, ts) in transitions.iter().filter(|(t, _)| alert::should_alert(t)) {
                let incident_id = incident_for.get(&t.check_id).cloned();
                match store.insert_alert(
                    storage::AlertScope::Check,
                    &t.check_id,
                    Some(t.from),
                    t.to,
                    *ts,
                    incident_id.as_deref(),
                ) {
                    Ok(id) => replicated_alerts.push(ReplicatedAlert {
                        id,
                        scope: storage::AlertScope::Check.as_str().to_string(),
                        node_id: t.check_id.clone(),
                        previous_state: Some(t.from),
                        current_state: t.to,
                        timestamp: *ts,
                        incident_id,
                    }),
                    Err(e) => tracing::error!(error = %e, "failed to persist alert"),
                }
            }
            // Every incident touched this batch, whole, so a peer learns the
            // real id instead of guessing one on its own — see
            // `ReplicatedIncident` and the fix for the gossip ponytail.
            let mut seen_incidents: std::collections::HashSet<&str> =
                std::collections::HashSet::new();
            for id in incident_for.values() {
                if seen_incidents.insert(id.as_str()) {
                    if let Ok(Some(ri)) = store.replicated_incident(id) {
                        replicated_incidents.push(ri);
                    }
                }
            }
        } // drop storage guard before any await

        // Replicate before the loop below consumes `transitions`, and replicate
        // every confirmed flip rather than only the alert-worthy ones: a peer
        // whose status page disagrees with ours is the divergence this exists
        // to prevent.
        //
        // Spawned, never awaited, for the same reason the notifier is handed
        // off: ingest is the fleet's write path, so a wedged peer must cost
        // replication lag rather than throughput.
        if let Some(peers) = &self.inner.gossip {
            if !transitions.is_empty() {
                let peers = Arc::clone(peers);
                let msgs: Vec<_> = transitions
                    .iter()
                    .map(|(t, ts)| {
                        gossip::Message::new(t, *ts, incident_for.get(&t.check_id).cloned())
                    })
                    .collect();
                tokio::spawn(async move {
                    for m in &msgs {
                        peers.broadcast(m).await;
                    }
                });
            }
        }

        // Cluster-based replication: the richer successor to the
        // transition-only push above, carrying raw results, confirmed alerts
        // and full incident rows so a peer's history, alert feed and
        // incident detail all agree with the observer's, not only its
        // flips. `self.inner.gossip` and `self.inner.cluster` are
        // independent — a deployment may run either, both, or neither.
        //
        // Spawned, never awaited, for the same reason: ingest is the fleet's
        // write path and must not cost throughput for a wedged or slow peer.
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
                        alerts: replicated_alerts,
                        incidents: replicated_incidents,
                        ..Default::default()
                    };
                    let cluster = Arc::clone(cluster);
                    tokio::spawn(async move {
                        cluster.broadcast_json("/internal/replicate", &batch).await;
                    });
                }
            }
        }

        for (t, ts) in transitions
            .into_iter()
            .filter(|(t, _)| alert::should_alert(t))
        {
            // Broadcast to WS clients (ignore if no subscribers).
            let _ = self.inner.events.send(WsEvent::StateChange {
                check_id: t.check_id.clone(),
                from: t.from,
                to: t.to,
                timestamp: ts,
            });
            // Hand off rather than dispatch. Ingest is the path every agent in
            // the fleet POSTs into, so it must never wait on a third party:
            // sending here once blocked forever on an endpoint that accepted
            // the connection and never replied.
            //
            // `try_send`, not `send`. If the notifier has fallen behind, the
            // alert is dropped with a warning instead of applying backpressure
            // to results — the transition is already persisted and visible in
            // the UI, so a lost notification costs less than lost measurements.
            //
            // Some checks are just checks, and some outages are scheduled: the
            // transition is still confirmed, still recorded as an alert and
            // still opens an incident — it just does not page. Both the
            // per-check switch and the maintenance window are read here rather
            // than inside the dispatcher, where a later edit would silently
            // change the meaning of something already queued.
            if let Some(routed_to) = self.notify_targets(&t.check_id, ts) {
                if self
                    .inner
                    .notify_tx
                    .try_send((t.clone(), routed_to))
                    .is_err()
                {
                    tracing::warn!(
                        check = %t.check_id,
                        "notification queue full, alert not dispatched"
                    );
                }
            }
        }
        for ev in incident_events {
            let _ = self.inner.events.send(ev);
        }
    }

    /// Apply a peer's replication batch: incidents, then alerts, then raw
    /// results, then this node's own confirmed-state view of every check the
    /// batch touched. Mirrors what local `ingest` writes to storage and
    /// nothing else it does — no `eval`, no `notify_tx`, no re-broadcast to
    /// the cluster or to `self.inner.gossip` — because the sender already
    /// decided all of that and owns paging for it.
    fn apply_replicated(&self, batch: ReplicationBatch) {
        let mut incident_events = Vec::new();
        // (check_id, previous, current, timestamp), derived from the
        // alerts in the batch — the same "only a confirmed flip moves the
        // published state" rule `gossip_handler` applies, generalized to a
        // whole batch instead of one transition.
        let mut state_changes: Vec<(String, Option<Status>, Status, i64)> = Vec::new();
        {
            let store = self.inner.storage.lock();
            let now = chrono::Utc::now().timestamp_millis();
            // Deletes are applied before upserts, so a batch that carries both
            // an incident and its deletion (a reconciler replaying a peer's
            // whole state) converges on deleted rather than on ordering luck.
            for id in &batch.deleted_incidents {
                match store.delete_incident(id, now, &batch.origin) {
                    // Only announce a delete that actually removed something:
                    // a tombstone for an incident this node never had is not
                    // news any dashboard can act on.
                    Ok(true) => incident_events.push(WsEvent::IncidentDeleted { id: id.clone() }),
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, incident = %id, "replicate: failed to delete incident")
                    }
                }
            }
            // Poll rows are a merge on `polled_at`, so a stale batch cannot
            // un-see a live agent and applying the same batch twice is a no-op.
            for p in &batch.polls {
                if let Err(e) = store.record_agent_poll(
                    &p.agent_id,
                    &p.zone,
                    &p.region,
                    p.at,
                    p.window_ms,
                    p.rtt_ms,
                ) {
                    tracing::warn!(error = %e, agent = %p.agent_id, "replicate: failed to persist agent poll");
                }
            }
            for i in &batch.incidents {
                let existed = matches!(store.incident(&i.id), Ok(Some(_)));
                if let Err(e) = store.upsert_incident(i) {
                    tracing::warn!(error = %e, incident = %i.id, "replicate: failed to persist incident");
                    continue;
                }
                if let Ok(Some(row)) = store.incident(&i.id) {
                    let ack = store.is_acknowledged(&i.id).unwrap_or(false);
                    let dto = IncidentDto::from_row(row, now, ack);
                    incident_events.push(if existed {
                        WsEvent::IncidentUpdated { incident: dto }
                    } else {
                        WsEvent::IncidentOpened { incident: dto }
                    });
                }
            }
            for a in &batch.alerts {
                let row = a.clone().into_row();
                if let Err(e) = store.upsert_alert(&row) {
                    tracing::warn!(error = %e, alert = %a.id, "replicate: failed to persist alert");
                    continue;
                }
                state_changes.push((
                    a.node_id.clone(),
                    a.previous_state,
                    a.current_state,
                    a.timestamp,
                ));
            }
            if !batch.results.is_empty() {
                let rows: Vec<(CheckResult, Status)> = batch
                    .results
                    .into_iter()
                    .map(|r| (r.result, r.status))
                    .collect();
                if let Err(e) = store.insert_results_dedup(&rows) {
                    tracing::warn!(error = %e, "replicate: failed to persist results");
                }
            }
            for (check_id, _, current, ts) in &state_changes {
                if let Err(e) = store.update_current_state(check_id, *current, 0, *ts) {
                    tracing::warn!(error = %e, check = %check_id, "replicate: failed to persist state");
                }
            }
        } // drop storage guard before touching the tracker or broadcasting
        {
            // The failure count resets, same as `gossip_handler`: this node
            // is not mid-confirmation for something the observer already
            // decided.
            let mut tracker = self.inner.tracker.lock();
            for (check_id, _, current, _) in &state_changes {
                tracker.seed(check_id, *current, 0);
            }
        }
        for (check_id, from, to, timestamp) in state_changes {
            let _ = self.inner.events.send(WsEvent::StateChange {
                check_id,
                from: from.unwrap_or(Status::Unknown),
                to,
                timestamp,
            });
        }
        for ev in incident_events {
            let _ = self.inner.events.send(ev);
        }
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
        let Some(peer) = live.choose(&mut rand::thread_rng()).cloned() else {
            return;
        };
        let client = reqwest::Client::new();
        let Some(their) = self.fetch_digest(&client, &peer).await else {
            return;
        };
        let our = {
            let store = self.inner.storage.lock();
            match store.digest(0) {
                Ok(d) => DigestResponse::new(&self.inner.node_id, d),
                Err(e) => {
                    tracing::warn!(error = %e, "anti-entropy: failed to read local digest");
                    return;
                }
            }
        };
        for their_check in &their.checks {
            let our_max_ts = our
                .checks
                .iter()
                .find(|c| c.check_id == their_check.check_id)
                .map(|c| c.max_ts)
                .unwrap_or(0);
            if their_check.max_ts <= our_max_ts {
                continue;
            }
            self.backfill_check(
                &client,
                &peer,
                &their_check.check_id,
                our_max_ts,
                their_check.max_ts,
            )
            .await;
        }
        self.reconcile_config(&client, &peer, &their, &our).await;
    }

    /// Pull any config kind where `peer` may hold something this node does
    /// not. The push in `replicate_config` is the fast path; this is what
    /// closes the hole when a node was down, or unreachable, at the moment an
    /// admin saved an edit — nothing would ever mention that row again.
    ///
    /// A kind is pulled when the peer's newest stamp is ahead of ours *or*
    /// the counts disagree. The count half is not redundant: a peer can hold
    /// a row we lack while our own max HLC is higher because of some other
    /// row, and a stamp comparison alone would call that converged forever.
    ///
    /// Pulling is safe to do more often than strictly needed — every entry
    /// goes through `apply_config_entry`, which is last-writer-wins and drops
    /// anything not newer than what is already stored, so a redundant pull
    /// converges to exactly the same state.
    async fn reconcile_config(
        &self,
        client: &reqwest::Client,
        peer: &str,
        their: &DigestResponse,
        our: &DigestResponse,
    ) {
        for theirs in &their.config {
            let ours = our.config.iter().find(|c| c.kind == theirs.kind);
            let (our_hlc, our_count) = match ours {
                Some(c) => (c.max_hlc.clone(), c.count),
                None => (None, 0),
            };
            let ahead = match (&theirs.max_hlc, &our_hlc) {
                (Some(t), Some(o)) => t > o,
                (Some(_), None) => true,
                (None, _) => false,
            };
            if !ahead && theirs.count == our_count {
                continue;
            }
            self.backfill_config(client, peer, theirs.kind).await;
        }
    }

    /// `GET /internal/config?kind=` on `peer`, applied through the same
    /// `apply_config_batch` the push path uses — so a pulled entry reloads
    /// the in-memory caches, and fires no notifier, exactly as a pushed one
    /// does. Unpaged, for the reason given on `config_entries_handler`.
    async fn backfill_config(
        &self,
        client: &reqwest::Client,
        peer: &str,
        kind: storage::ConfigKind,
    ) {
        let url = cluster::Cluster::endpoint(peer, "/internal/config");
        let mut req = client.get(&url).query(&[("kind", kind.as_str())]);
        if let Some(t) = &self.inner.peer_token {
            req = req.bearer_auth(t);
        }
        let batch: ConfigBatch = match req.send().await {
            Ok(r) if r.status().is_success() => match r.json().await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(%peer, kind = kind.as_str(), error = %e, "anti-entropy: malformed config response");
                    return;
                }
            },
            Ok(r) => {
                tracing::warn!(%peer, kind = kind.as_str(), status = %r.status(), "anti-entropy: config request rejected");
                return;
            }
            Err(e) => {
                tracing::warn!(%peer, kind = kind.as_str(), error = %e, "anti-entropy: config request failed");
                return;
            }
        };
        if batch.entries.is_empty() {
            return;
        }
        let n = batch.entries.len();
        if let Err(e) = self.apply_config_batch(&batch.entries) {
            tracing::warn!(%peer, kind = kind.as_str(), error = %e, "anti-entropy: failed to apply config");
            return;
        }
        tracing::info!(%peer, kind = kind.as_str(), entries = n, "anti-entropy: config reconciled");
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
    /// the first page, then advanced to the last row applied) and `until`
    /// (inclusive), applying each page as it arrives and yielding between
    /// pages so a large catch-up cannot starve `ingest`.
    async fn backfill_check(
        &self,
        client: &reqwest::Client,
        peer: &str,
        check_id: &str,
        since: i64,
        until: i64,
    ) {
        let url = cluster::Cluster::endpoint(peer, "/internal/backfill");
        let mut cursor = since;
        loop {
            let mut req = client.get(&url).query(&[
                ("check_id", check_id.to_string()),
                ("since", cursor.to_string()),
                ("until", until.to_string()),
                ("limit", ANTI_ENTROPY_PAGE.to_string()),
            ]);
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
            let max_ts = batch
                .results
                .iter()
                .map(|r| r.result.timestamp.timestamp_millis())
                .max()
                .unwrap_or(cursor);
            self.apply_replicated(batch);
            // Either the page was not full (nothing left to fetch) or the
            // cursor failed to advance (every row shares `max_ts`, which a
            // strictly-exclusive `since` can never get past) — both mean
            // stop rather than repeat the same request forever.
            if page_len < ANTI_ENTROPY_PAGE || max_ts <= cursor {
                return;
            }
            cursor = max_ts;
            tokio::task::yield_now().await;
        }
    }

    /// Read recent history for a check (newest first), optionally within a time
    /// range and optionally restricted to one evaluated status.
    pub fn history(
        &self,
        check_id: &str,
        limit: usize,
        from: Option<i64>,
        to: Option<i64>,
        status: Option<Status>,
    ) -> Result<Vec<storage::HistoryRow>> {
        self.inner
            .storage
            .lock()
            .history(check_id, limit, from, to, status)
    }

    /// Recent raw probe results across all checks and agents (newest first),
    /// narrowed by `filter`. Admin-only at the HTTP layer: rows carry the raw
    /// error.
    pub fn recent_results(
        &self,
        filter: &storage::ResultsFilter,
        limit: usize,
    ) -> Result<Vec<storage::ResultRow>> {
        self.inner.storage.lock().recent_results(filter, limit)
    }

    /// Persist one check, through the config-replication path so a peer picks
    /// it up too, and refresh the cached metadata. Shared by create and
    /// update so both paths reload identically.
    fn save_check(&self, check: CheckConfig) -> Response {
        let document = match serde_json::to_string(&check) {
            Ok(d) => d,
            Err(e) => return internal_error(e.into()),
        };
        let entry = match self.write_config(storage::ConfigKind::Check, &check.name, Some(document))
        {
            Ok(e) => e,
            Err(e) => return internal_error(e),
        };
        self.replicate_config(vec![entry]);
        info!(check = %check.name, "check saved");
        Json(CheckResponse { check }).into_response()
    }

    /// Persisted alerts, for tests that assert dispatch did not lose one.
    pub fn alerts_for_test(&self, limit: usize) -> Vec<crate::coordinator::storage::AlertRow> {
        self.inner
            .storage
            .lock()
            .recent_alerts(limit)
            .unwrap_or_default()
    }

    /// Persisted incidents, for tests that assert the open/resolve lifecycle.
    pub fn incidents_for_test(
        &self,
        limit: usize,
    ) -> Vec<crate::coordinator::storage::IncidentRow> {
        self.inner
            .storage
            .lock()
            .list_incidents(limit, None)
            .unwrap_or_default()
    }

    /// An incident's narrative, for tests that assert the timeline the
    /// coordinator wrote rather than only the state it left behind.
    pub fn updates_for_test(
        &self,
        incident_id: &str,
    ) -> Vec<crate::coordinator::storage::IncidentUpdateRow> {
        self.inner
            .storage
            .lock()
            .updates_for_incident(incident_id)
            .unwrap_or_default()
    }

    /// The silence window the coordinator recorded for an agent on its last
    /// poll, so a test can assert the declared cadence was actually used rather
    /// than waiting minutes for the window to expire.
    pub fn agent_window_ms_for_test(&self, agent_id: &str) -> Option<i64> {
        self.inner.seen.read().get(agent_id).map(|s| s.window_ms)
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/api/v1/results", post(ingest_handler))
            .route("/api/v1/status", get(status_handler))
            .route("/api/v1/checks/:id/history", get(history_handler))
            .route("/api/v1/checks/:id/uptime", get(uptime_handler))
            .route(
                "/api/v1/checks/:id/latency-sparkline",
                get(latency_sparkline_handler),
            )
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
            .route("/api/v1/admin/agents/:id", delete(purge_agent_handler))
            // Section names and their display flags. Public because the status
            // page needs them to render groups; it carries no targets.
            .route("/api/v1/sections", get(public_sections_handler))
            .route("/api/v1/ws", get(ws_handler))
            // Auth. `session` is deliberately unauthenticated and never 401s —
            // it is how the page decides whether to offer the admin UI at all.
            .route("/api/v1/login", post(login_handler))
            .route("/api/v1/logout", post(logout_handler))
            .route("/api/v1/session", get(session_handler))
            // Check editor. Every route here requires an admin session.
            // Deadman switch: something external pings this, and the check goes
            // DOWN when the pings stop. Unauthenticated by design — the URL is
            // the credential, and it is only ever shown to an admin.
            .route(
                "/api/v1/heartbeat/:name",
                get(heartbeat_handler).post(heartbeat_handler),
            )
            .route(
                "/api/v1/admin/checks/:name/data",
                delete(purge_check_data_handler),
            )
            // The whole erase: the check AND everything it ever produced.
            // Ordered before the `:name` route for the same reason
            // `checks/order` is — a suffix must never be read as a check name.
            .route(
                "/api/v1/admin/checks/:name/everything",
                delete(purge_check_everything_handler),
            )
            // Ordering is its own route, ahead of the `:name` route so "order"
            // is never taken for a check called "order". Reordering is a bulk
            // operation over names, not an edit to any one check.
            .route("/api/v1/admin/checks/order", put(reorder_checks_handler))
            .route(
                "/api/v1/admin/checks",
                get(list_checks_handler).post(create_check_handler),
            )
            .route(
                "/api/v1/admin/checks/:name",
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
            .route("/internal/replicate", post(replicate_handler))
            // Anti-entropy: a peer's fixed-cost summary, and the paged pull
            // that backfills whatever it says this node is missing.
            .route("/internal/digest", get(digest_handler))
            .route("/internal/backfill", get(backfill_handler))
            // Config-plane push: checks, sections, settings, credentials —
            // last-writer-wins by HLC, applied locally, never observer-owned
            // the way a result is. See `config_handler`.
            .route(
                "/internal/config",
                post(config_handler).get(config_entries_handler),
            )
            // Cluster membership. Peer-gated like every other /internal/*
            // route: pinging is how a peer rewrites what this node believes
            // about liveness, so an agent must never be able to call it.
            .route("/internal/cluster/ping", post(cluster_ping_handler))
            // The admin-facing membership view. Admin-gated, not peer-gated:
            // this is read by a human in the browser, not by another node.
            .route("/api/v1/cluster", get(cluster_view_handler))
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
const NODE_ID_KEY: &str = "cluster.node_id";
const SESSION_KEY_KEY: &str = "cluster.session_key";
/// Rows per `/internal/backfill` page during an anti-entropy sweep. Bounded
/// so one very-behind check cannot hold `apply_replicated` — and the yield
/// between pages — for an unbounded stretch.
const ANTI_ENTROPY_PAGE: usize = 500;

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

/// A value with no config-file counterpart to seed from — its seed is
/// randomness itself. Generated once, persisted as hex, and reused on every
/// subsequent boot, so identity (`NODE_ID_KEY`) and the session signing key
/// (`SESSION_KEY_KEY`) are both stable across a restart without an operator
/// having to set anything.
fn load_or_generate(storage: &Storage, key: &str, len: usize) -> String {
    if let Ok(Some(v)) = storage.get_setting(key) {
        if !v.trim().is_empty() {
            return v;
        }
    }
    let mut buf = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut buf);
    let generated = crate::auth::hex(&buf);
    if let Err(e) = storage.put_setting(key, &generated) {
        warn!(error = %e, key, "could not persist generated value");
    }
    generated
}

/// A stored setting, seeded from the config file the first time it is missing.
///
/// A stored document that no longer parses — hand-edited, or a field removed in
/// a later version — falls back to the file rather than refusing to boot:
/// losing a preference is recoverable, a coordinator that will not start is not.
fn load_or_seed<T>(storage: &Storage, key: &str, from_file: &T) -> T
where
    T: Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    match storage.get_setting(key) {
        Ok(Some(json)) => match serde_json::from_str(&json) {
            Ok(stored) => return stored,
            Err(e) => warn!(error = %e, key, "stored setting is unreadable, using the config file"),
        },
        Ok(None) => {
            if let Ok(json) = serde_json::to_string(from_file) {
                if let Err(e) = storage.put_setting(key, &json) {
                    warn!(error = %e, key, "could not seed setting");
                }
            }
        }
        Err(e) => warn!(error = %e, key, "could not read setting"),
    }
    from_file.clone()
}

/// Fleet liveness for the public status page.
fn fleet_health(coord: &Coordinator, now: i64) -> Vec<FleetRowDto> {
    let cfg = coord.inner.status_agents.read().clone();
    // Nothing placed by hand and nothing automatic: the section is off, and
    // the aggregate query can be skipped entirely. This rides the busiest
    // public endpoint there is.
    if cfg.mode == crate::config::AgentsMode::Off && cfg.rows.is_empty() {
        return Vec::new();
    }
    // One lock for both reads: they are two halves of the same answer, and
    // taking it twice on the busiest public endpoint buys nothing.
    let (reported, polls) = {
        let store = coord.inner.storage.lock();
        (
            store.list_agents(now).unwrap_or_default(),
            store.list_agent_polls().unwrap_or_default(),
        )
    };
    fleet_rows(now, &coord.inner.seen.read(), &polls, &reported, &cfg)
}

/// Everyone the coordinator knows about, live pollers and past reporters alike,
/// sorted so the picker does not reshuffle between loads.
fn fleet_members(coord: &Coordinator, now: i64) -> Vec<FleetMemberDto> {
    let mut members: std::collections::HashMap<String, (String, bool)> = coord
        .inner
        .seen
        .read()
        .iter()
        .map(|(id, s)| (id.clone(), (s.region.clone(), now - s.at <= s.window_ms)))
        .collect();
    let (reported, polls) = {
        let store = coord.inner.storage.lock();
        (
            store.list_agents(now).unwrap_or_default(),
            store.list_agent_polls().unwrap_or_default(),
        )
    };
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

#[derive(Serialize)]
struct DeletedResponse {
    deleted: String,
}
