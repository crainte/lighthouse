pub mod alert;
pub mod cluster;
pub mod gossip;
pub mod hlc;
pub mod state;
pub mod storage;

use crate::config::{CheckConfig, CoordinatorConfig};
use crate::credential::AgentCredential;
use crate::models::{eval, CheckResult, ResultBatch, Status};
use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::RngCore;
use serde::{Deserialize, Serialize};
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

/// A check attached to an incident, as the UI reads it.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentCheckDto {
    pub check_id: String,
    pub source: &'static str,
}

/// One entry in an incident's narrative.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentUpdateDto {
    pub id: String,
    pub state: &'static str,
    pub body: String,
    pub author: Option<String>,
    pub created_at: i64,
}

impl From<storage::IncidentUpdateRow> for IncidentUpdateDto {
    fn from(u: storage::IncidentUpdateRow) -> Self {
        Self {
            id: u.id,
            state: u.state.as_str(),
            body: u.body,
            author: u.author,
            created_at: u.created_at,
        }
    }
}

/// The shape of an incident as the UI reads it. Also carried inside WS events.
///
/// `duration_ms` and `acknowledged` are computed here rather than stored:
/// duration is a subtraction, and an acknowledgement is the existence of an
/// authored update. Sending them saves every caller from re-deriving the same
/// two facts, without either becoming a column that can contradict its source.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentDto {
    pub id: String,
    pub title: String,
    pub state: &'static str,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
    pub duration_ms: i64,
    pub global: bool,
    pub origin_check: Option<String>,
    pub checks: Vec<IncidentCheckDto>,
    pub acknowledged: bool,
    /// Present on the single-incident read; omitted from list responses, which
    /// would otherwise carry every narrative on the page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updates: Option<Vec<IncidentUpdateDto>>,
}

impl IncidentDto {
    /// Build the list shape: no narrative, acknowledgement resolved by the
    /// caller (which holds the storage lock and can answer it cheaply).
    fn from_row(i: storage::IncidentRow, now: i64, acknowledged: bool) -> Self {
        Self {
            duration_ms: i.duration_ms(now),
            id: i.id,
            title: i.title,
            state: i.state.as_str(),
            started_at: i.started_at,
            resolved_at: i.resolved_at,
            global: i.global,
            origin_check: i.origin_check,
            checks: i
                .checks
                .into_iter()
                .map(|c| IncidentCheckDto {
                    check_id: c.check_id,
                    source: c.source.as_str(),
                })
                .collect(),
            acknowledged,
            updates: None,
        }
    }
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

/// One row of the public agent section: one agent, or a whole region.
#[derive(Serialize, Debug)]
struct FleetRowDto {
    /// What the row counts. Also what keeps row keys unique on the client: a
    /// region and an agent are allowed to share a name, and two rows keyed the
    /// same hang a Svelte `{#each}` rather than looking wrong.
    kind: RowKind,
    name: String,
    status: Health,
    /// Agents polling now, out of the agents the row covers. An agent row is
    /// 1/1 or 0/1, so the frontend needs no special case.
    healthy: u32,
    total: u32,
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum RowKind {
    Agent,
    Region,
}

/// Agent liveness — three states, not a check's UP/DOWN. An agent answering is
/// healthy even if every check it probes is down.
///
/// Declared worst-first: `Ord` is how two readings of one agent are merged, and
/// the better one wins.
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
enum Health {
    /// Long enough gone to be a decommission rather than an incident.
    Down,
    /// Not polling, but its results are recent enough to be an outage.
    Degraded,
    /// Polling within its own window.
    Up,
}

#[derive(Serialize)]
struct CheckStatusDto {
    check_id: String,
    status: Status,
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
    check_id: String,
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
    check_id: String,
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
    alert_type: &'static str,
    previous_state: Option<Status>,
    current_state: Status,
    timestamp: i64,
    incident_id: Option<String>,
}

impl From<storage::AlertRow> for AlertDto {
    fn from(r: storage::AlertRow) -> Self {
        let alert_type = match (r.previous_state, r.current_state) {
            (_, Status::Unknown) => "degradation",
            (Some(Status::Down), Status::Up) => "recovery",
            _ => "state_change",
        };
        Self {
            id: r.id,
            scope: r.scope.as_str(),
            node_id: r.node_id,
            alert_type,
            previous_state: r.previous_state,
            current_state: r.current_state,
            timestamp: r.timestamp,
            incident_id: r.incident_id,
        }
    }
}

#[derive(Serialize)]
struct IncidentsResponse {
    incidents: Vec<IncidentDto>,
}

/// Query for the incidents endpoints. `status` mirrors the incident's
/// lifecycle, not a check's UP/DOWN/UNKNOWN: `?status=open` lists only current
/// outages, which is any state other than resolved.
#[derive(Deserialize)]
struct IncidentQuery {
    limit: Option<usize>,
    status: Option<String>,
}

/// Open an incident by hand. Everything is optional but the title: a
/// maintenance notice needs no affected checks and no start time.
#[derive(Deserialize)]
struct CreateIncidentRequest {
    title: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    global: Option<bool>,
    #[serde(default)]
    checks: Vec<String>,
    /// The opening update's body.
    #[serde(default)]
    body: Option<String>,
}

/// Edit the operator-owned fields. No `state`: that moves only by posting an
/// update.
#[derive(Deserialize)]
struct EditIncidentRequest {
    title: String,
    started_at: i64,
    global: bool,
}

#[derive(Deserialize)]
struct IncidentChecksRequest {
    checks: Vec<String>,
}

#[derive(Deserialize)]
struct IncidentUpdateRequest {
    state: String,
    #[serde(default)]
    body: String,
}

#[derive(Serialize)]
struct AgentsResponse {
    agents: Vec<AgentDto>,
    /// Checks pinned only to agents that are not reporting. They are configured
    /// and enabled but nothing is running them, which is otherwise silent.
    checks_with_no_live_agent: Vec<String>,
}

#[derive(Serialize)]
struct AgentDto {
    agent_id: String,
    /// Where it reports from. The fleet is grouped by these, so an operator can
    /// see "eu-west has no live agents" rather than reading twenty agent ids.
    zone: String,
    region: String,
    last_seen: i64,
    checks_reporting: u32,
    results_24h: u32,
    /// Silent long enough to be treated as decommissioned. Its history is kept.
    retired: bool,
    /// How many checks name this agent explicitly. Purging one that is still
    /// named would stop those checks running.
    assigned_checks: u32,
    /// Derived liveness: an agent that hasn't reported recently is stale.
    status: Status,
    /// Round-trip of its last poll, ms, as the agent measured it. `None` for an
    /// agent seen only through reported results, never a live poll.
    poll_rtt_ms: Option<i64>,
}

#[derive(Deserialize)]
struct LimitQuery {
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
struct ResultsQuery {
    check: Option<String>,
    agent: Option<String>,
    /// `up` | `down` | `unknown`. An unrecognised value filters nothing.
    status: Option<String>,
    region: Option<String>,
    zone: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct DaysQuery {
    days: Option<usize>,
    /// How many segments to split the window into; the strip keeps a constant
    /// bar count across ranges.
    buckets: Option<usize>,
}

#[derive(Serialize)]
struct UptimeResponse {
    check_id: String,
    days: usize,
    overall_uptime: Option<f64>,
    buckets: Vec<UptimeBucketDto>,
}

#[derive(Serialize)]
struct UptimeBucketDto {
    /// Exclusive lower bound of the segment. Not a day boundary unless the
    /// window happens to divide into whole days.
    start: i64,
    end: i64,
    uptime: Option<f64>,
    up: u32,
    total: u32,
}

#[derive(Serialize)]
struct LatencySparklineResponse {
    check_id: String,
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
    /// `null` when the segment holds no attempts — not the same as a 0ms reply.
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
struct HoursQuery {
    hours: Option<usize>,
    buckets: Option<usize>,
    /// Explicit window, overriding `hours`. Lets a clicked uptime bar chart the
    /// segment it actually covers instead of a `now`-anchored guess.
    from: Option<i64>,
    to: Option<i64>,
}

// ---- Handlers ----

async fn ingest_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(mut batch): Json<ResultBatch>,
) -> Response {
    let caller = match authenticate_agent(&coord, &headers) {
        Ok(c) => c,
        Err(denied) => return *denied,
    };
    // An agent does not get to say where it is. Zone and region decide placement
    // and are shown as the location an outage was seen from, so a caller able to
    // name its own could answer for a zone it is not in.
    for r in &mut batch.results {
        r.zone.clone_from(&caller.zone);
        r.region.clone_from(&caller.region);
    }
    let n = batch.results.len();
    coord.ingest(batch.results).await;
    info!(accepted = n, "ingested results");
    StatusCode::ACCEPTED.into_response()
}

/// Apply a transition a peer coordinator observed.
///
/// Deliberately does three of the things local ingest does — mirror the
/// confirmed state, record the alert row, tell live dashboards — and two it
/// must not:
///
/// * **It does not notify.** The coordinator that observed the flip already
///   paged; paging again per peer is how a three-node cluster wakes someone
///   three times for one outage.
/// * **It does not re-gossip.** That is what keeps the fan-out acyclic without
///   a message id or a TTL — see the module docs.
async fn gossip_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(msg): Json<gossip::Message>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let t = msg.transition();
    // The peer sends only confirmed flips, so the failure count resets: this
    // node is not mid-confirmation for a check somebody else already decided.
    coord.inner.tracker.lock().seed(&t.check_id, t.to, 0);
    {
        let store = coord.inner.storage.lock();
        if let Err(e) = store.update_current_state(&t.check_id, t.to, 0, msg.timestamp) {
            tracing::warn!(error = %e, check = %t.check_id, "gossip: failed to persist state");
        }
        // Same filter the local path applies, so the two alert feeds agree.
        if alert::should_alert(&t) {
            // The sender's real incident id, carried on the message since
            // `gossip::Message` grew `incident_id`. Stamped verbatim even
            // though the incident row itself has not arrived here yet — it
            // lands separately, via `/internal/replicate` or an
            // anti-entropy backfill, and `upsert_incident` is keyed by this
            // same id, so the alert is already pointing at the right row the
            // moment it does. Only a sender with no incident_id at all
            // (never opened one, or an older binary) falls back to whatever
            // this node happens to have open locally.
            let incident_id = msg.incident_id.clone().or_else(|| {
                store
                    .open_incident_for(&t.check_id)
                    .ok()
                    .flatten()
                    .map(|i| i.id)
            });
            if let Err(e) = store.insert_alert(
                storage::AlertScope::Check,
                &t.check_id,
                Some(t.from),
                t.to,
                msg.timestamp,
                incident_id.as_deref(),
            ) {
                tracing::warn!(error = %e, check = %t.check_id, "gossip: failed to persist alert");
            }
        }
    } // drop storage guard before the broadcast
    let _ = coord.inner.events.send(WsEvent::StateChange {
        check_id: t.check_id,
        from: t.from,
        to: t.to,
        timestamp: msg.timestamp,
    });
    StatusCode::NO_CONTENT.into_response()
}

/// One probe observation as it replicates: the coordinator's verdict, not the
/// raw probe. `body`/`headers` are already stripped by the time this is built
/// (see `ingest`), so nothing sensitive crosses the wire twice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedResult {
    pub result: CheckResult,
    pub status: Status,
}

/// One confirmed transition as it replicates. Mirrors `storage::AlertRow`
/// field-for-field, but on the wire — `AlertRow` itself has no `Serialize`,
/// being owned by a different concern, and `AlertScope` travels as its
/// string form so this type does not need `storage` to grow a derive it
/// otherwise has no use for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedAlert {
    pub id: String,
    pub scope: String,
    pub node_id: String,
    pub previous_state: Option<Status>,
    pub current_state: Status,
    pub timestamp: i64,
    pub incident_id: Option<String>,
}

impl From<&storage::AlertRow> for ReplicatedAlert {
    fn from(a: &storage::AlertRow) -> Self {
        ReplicatedAlert {
            id: a.id.clone(),
            scope: a.scope.as_str().to_string(),
            node_id: a.node_id.clone(),
            previous_state: a.previous_state,
            current_state: a.current_state,
            timestamp: a.timestamp,
            incident_id: a.incident_id.clone(),
        }
    }
}

impl ReplicatedAlert {
    fn into_row(self) -> storage::AlertRow {
        storage::AlertRow {
            id: self.id,
            scope: storage::AlertScope::from_db(&self.scope),
            node_id: self.node_id,
            previous_state: self.previous_state,
            current_state: self.current_state,
            timestamp: self.timestamp,
            incident_id: self.incident_id,
        }
    }
}

/// The data-plane replication batch: `POST /internal/replicate`'s body, and
/// `GET /internal/backfill`'s response — the same shape both ways, so
/// anti-entropy can hand a backfilled batch straight to a peer's
/// `/internal/replicate` without reshaping it. `origin` is informational
/// only today (logging, future loop-avoidance); nothing here re-broadcasts.
///
/// Every field past `origin` is `#[serde(default)]`, which is what makes a
/// rolling upgrade safe: a node on an older build silently ignores a field it
/// does not know, degrading to the previous behaviour instead of rejecting the
/// whole batch.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicationBatch {
    pub origin: String,
    #[serde(default)]
    pub results: Vec<ReplicatedResult>,
    #[serde(default)]
    pub alerts: Vec<ReplicatedAlert>,
    #[serde(default)]
    pub incidents: Vec<storage::ReplicatedIncident>,
    /// Who is polling the sender, so every node in the cluster shows the same
    /// fleet rather than only the agents that happen to poll it.
    #[serde(default)]
    pub polls: Vec<storage::AgentPollRow>,
    /// Incident ids deleted on the sender. Applied as a delete *plus a local
    /// tombstone*, so this node will not hand the incident back later either.
    #[serde(default)]
    pub deleted_incidents: Vec<String>,
}

/// `POST /internal/replicate`: the data-plane analog of `/internal/gossip`,
/// carrying raw results, confirmed alerts and whole incident rows in one
/// batch rather than just a transition. Peer-gated, same as every
/// `/internal/*` route — an agent credential must never rewrite published
/// state.
async fn replicate_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(batch): Json<ReplicationBatch>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    coord.apply_replicated(batch);
    StatusCode::NO_CONTENT.into_response()
}

/// The config plane's push, `POST /internal/config`'s body: a batch of
/// last-writer-wins entries, from either a peer's own admin edit
/// (`Coordinator::replicate_config`) or a reconciler replaying what it
/// knows a peer wrote. `origin` is informational only, mirroring
/// `ReplicationBatch::origin` — nothing here re-broadcasts.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigBatch {
    origin: String,
    entries: Vec<storage::ConfigEntry>,
}

/// `POST /internal/config`: applies a batch of config entries — checks,
/// sections, settings, credentials — through the one HLC-arbitrated write
/// path (`Storage::apply_config_entry`), then refreshes whichever in-memory
/// caches that path bypasses. Peer-gated like every other `/internal/*`
/// route.
///
/// Deliberately never touches `notify_tx`, `tracker`, gossip or the cluster:
/// config has no observer and takes effect locally by definition — see the
/// module-level note on the plane boundary. A single malformed entry is
/// logged and skipped rather than failing the whole batch, so one bad row
/// cannot block every other edit riding along with it.
async fn config_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(batch): Json<ConfigBatch>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    match coord.apply_config_batch(&batch.entries) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => internal_error(e),
    }
}

/// Read back one id's replicated form for `Coordinator::replicate_config`.
/// `Storage::config_entry` only ever sees the live table, so it reads back
/// `None` for an id whose latest write was a delete — the tombstone that
/// delete left behind lives in a different table entirely. Falling back to a
/// scan of `config_entries` for that one tombstone is what lets deletes and
/// upserts share `replicate_config` as one path instead of the delete
/// handlers needing a second, parallel push of their own.
fn read_config_entry_for_replication(
    store: &storage::Storage,
    kind: storage::ConfigKind,
    id: &str,
) -> Option<storage::ConfigEntry> {
    match store.config_entry(kind, id) {
        Ok(Some(e)) => Some(e),
        Ok(None) => match store.config_entries(kind) {
            Ok(all) => all.into_iter().find(|e| e.is_tombstone() && e.id == id),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    kind = kind.as_str(),
                    %id,
                    "config: failed to scan for a tombstone to replicate"
                );
                None
            }
        },
        Err(e) => {
            tracing::warn!(
                error = %e,
                kind = kind.as_str(),
                %id,
                "config: failed to read back an entry to replicate"
            );
            None
        }
    }
}

/// One config kind's contribution to a digest, on the wire. Mirrors
/// `storage::ConfigDigest`. `max_hlc` is `None` for a kind with no rows and
/// no tombstones, which is distinct from a kind whose newest stamp is zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDigestDto {
    pub kind: storage::ConfigKind,
    pub max_hlc: Option<hlc::Hlc>,
    pub count: i64,
}

impl From<&storage::ConfigDigest> for ConfigDigestDto {
    fn from(d: &storage::ConfigDigest) -> Self {
        ConfigDigestDto {
            kind: d.kind,
            max_hlc: d.max_hlc.clone(),
            count: d.count,
        }
    }
}

/// One check's contribution to a digest, on the wire. Mirrors
/// `storage::CheckDigest`, which has no `Serialize` of its own.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckDigestDto {
    pub check_id: String,
    pub max_ts: i64,
    pub count: i64,
}

impl From<&storage::CheckDigest> for CheckDigestDto {
    fn from(d: &storage::CheckDigest) -> Self {
        CheckDigestDto {
            check_id: d.check_id.clone(),
            max_ts: d.max_ts,
            count: d.count,
        }
    }
}

/// `storage::Digest`, on the wire. `config` travels as a count and newest
/// stamp per kind, which is all `reconcile_config` needs to decide whether to
/// pull a kind — the entries themselves come from `/internal/config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DigestResponse {
    /// Who answered. Informational, but it is the first thing anyone wants
    /// when a sweep is pulling from the wrong node.
    pub node_id: String,
    pub checks: Vec<CheckDigestDto>,
    pub window_start: i64,
    pub max_alert_ts: i64,
    pub max_incident_started_at: i64,
    /// The config plane's half. Without this a node that was down through an
    /// edit never learns about it: the immediate push already happened and
    /// nothing else would ever mention the row again.
    pub config: Vec<ConfigDigestDto>,
}

impl DigestResponse {
    fn new(node_id: &str, d: storage::Digest) -> Self {
        DigestResponse {
            node_id: node_id.to_string(),
            checks: d.checks.iter().map(CheckDigestDto::from).collect(),
            window_start: d.window_start,
            max_alert_ts: d.max_alert_ts,
            max_incident_started_at: d.max_incident_started_at,
            config: d.config.iter().map(ConfigDigestDto::from).collect(),
        }
    }
}

/// `GET /internal/digest`: a fixed-cost summary of everything this node has
/// replicated, for a peer's anti-entropy sweep to compare against its own.
/// Unwindowed (`window_start` 0) — the sweep this backs runs over a
/// dataset's whole retained history, not a recent slice, so a peer that has
/// been gone for days is still found and caught up.
async fn digest_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let digest = { coord.inner.storage.lock().digest(0) };
    match digest {
        Ok(d) => Json(DigestResponse::new(&coord.inner.node_id, d)).into_response(),
        Err(e) => internal_error(e),
    }
}

/// Which config kind a reconciler is asking for. Absent means every kind,
/// which is what a node rejoining after a long absence wants.
#[derive(Debug, Deserialize)]
struct ConfigEntriesQuery {
    kind: Option<storage::ConfigKind>,
}

/// `GET /internal/config`: every config entry of a kind, tombstones included,
/// as the pull half of the config plane. The POST on this path is the push
/// half; this is what a peer calls when a digest tells it the push never
/// arrived.
///
/// Unpaged, unlike `/internal/backfill`. Config is bounded by how many checks
/// an operator has written, not by how long the cluster has been running, so
/// there is no growth here for a cursor to protect against — and a partial
/// config is worse than a slow one.
async fn config_entries_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<ConfigEntriesQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let kinds: Vec<storage::ConfigKind> = match q.kind {
        Some(k) => vec![k],
        None => storage::ConfigKind::ALL.to_vec(),
    };
    let entries = {
        let store = coord.inner.storage.lock();
        let mut out = Vec::new();
        for kind in kinds {
            match store.config_entries(kind) {
                Ok(mut e) => out.append(&mut e),
                Err(e) => return internal_error(e),
            }
        }
        out
    };
    Json(ConfigBatch {
        origin: coord.inner.node_id.clone(),
        entries,
    })
    .into_response()
}

/// Query for `GET /internal/backfill`.
#[derive(Debug, Deserialize)]
struct BackfillQuery {
    check_id: String,
    since: i64,
    until: i64,
    limit: usize,
}

/// `GET /internal/backfill`: paged raw rows for one check, `since` exclusive
/// — the pull counterpart to `/internal/replicate`'s push, and the same
/// `ReplicationBatch` shape, so a caller can hand the response straight to a
/// peer's `/internal/replicate` without reshaping it.
async fn backfill_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<BackfillQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let rows = {
        coord
            .inner
            .storage
            .lock()
            .results_since(&q.check_id, q.since, q.until, q.limit)
    };
    match rows {
        Ok(rows) => {
            let batch = ReplicationBatch {
                origin: coord.inner.node_id.clone(),
                results: rows
                    .into_iter()
                    .map(|(result, status)| ReplicatedResult { result, status })
                    .collect(),
                ..Default::default()
            };
            Json(batch).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// `POST /internal/cluster/ping`: a thin wrapper over `Cluster::handle_ping`.
/// Push-pull membership — the body carries the sender's whole view, the reply
/// carries this node's, so one round trip does the work of two broadcasts.
/// Peer-gated like `/internal/gossip`: an agent credential must never be able
/// to rewrite membership.
async fn cluster_ping_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<cluster::PingBody>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let Some(cluster) = coord.inner.cluster.as_ref() else {
        return error_response(
            StatusCode::NOT_FOUND,
            "clustering is not configured on this node",
        );
    };
    let now = chrono::Utc::now().timestamp_millis();
    Json(cluster.handle_ping(&body, now)).into_response()
}

/// `GET /api/v1/cluster`: this node's own membership view, for the admin UI.
#[derive(Serialize)]
struct ClusterViewResponse {
    #[serde(rename = "self")]
    me: cluster::Member,
    members: Vec<cluster::Member>,
    now: i64,
}

async fn cluster_view_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    // A node with clustering unconfigured still has an identity worth
    // showing, just no membership beyond itself.
    let (me, members) = match coord.inner.cluster.as_ref() {
        Some(cluster) => {
            // Lazily swept here too, not only by the background heartbeat
            // loop: a view requested between two heartbeat ticks must still
            // reflect a peer that has actually gone silent, and a solo admin
            // checking on a cluster with no traffic yet must not wait on a
            // tick to see it.
            for (node_id, state) in cluster.sweep(now) {
                let _ = coord
                    .inner
                    .events
                    .send(WsEvent::ClusterChange { node_id, state });
            }
            (cluster.self_member(now), cluster.snapshot())
        }
        None => (
            cluster::Member {
                node_id: coord.inner.node_id.clone(),
                url: String::new(),
                generation: 0,
                last_seen: now,
                state: cluster::MemberState::Alive,
            },
            Vec::new(),
        ),
    };
    Json(ClusterViewResponse { me, members, now }).into_response()
}

async fn status_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
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
    let confirmed: std::collections::HashMap<String, Status> = confirmed
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.check_id, r.status))
        .collect();
    match rows {
        Ok(rows) => {
            let by_name = coord.inner.checks.read();
            let mut checks: Vec<CheckStatusDto> = rows
                .into_iter()
                // Only currently-configured, published checks. A row with no
                // config was deleted; its history stays queryable, but a status
                // page claiming "blackhole — Down" for something nobody is
                // monitoring any more is worse than saying nothing.
                .filter(|r| by_name.get(&r.check_id).is_some_and(|m| m.visible))
                .map(|r| {
                    let meta = by_name.get(&r.check_id);
                    CheckStatusDto {
                        kind: meta.map(|m| m.kind),
                        group: meta.and_then(|m| m.group.clone()),
                        latency_graph: meta.is_some_and(|m| m.latency_graph),
                        cron: meta.map(|m| m.cron.clone()),
                        position: meta.map_or(i32::MAX, |m| m.position),
                        check_id: r.check_id,
                        status: r.status,
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
            let reported: std::collections::HashSet<&str> =
                checks.iter().map(|c| c.check_id.as_str()).collect();
            let pending: Vec<CheckStatusDto> = by_name
                .iter()
                .filter(|(name, m)| m.visible && !reported.contains(name.as_str()))
                .map(|(name, m)| CheckStatusDto {
                    check_id: name.clone(),
                    // A check with no measurements here still has a confirmed
                    // state if a peer gossiped one to us. Prefer it over the
                    // configured default: a cluster that knows a service is
                    // DOWN must not read "unknown" on the node that heard it
                    // second-hand. Falls back to `default_status`, which is what
                    // a check reads as before its first result.
                    status: confirmed.get(name).copied().unwrap_or(m.default_status),
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
async fn history_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
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
    match coord.history(&id, limit, q.from, q.to, status) {
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
async fn results_handler(
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
    let filter = storage::ResultsFilter {
        check_id: q.check,
        agent_id: q.agent,
        status,
        region: q.region,
        zone: q.zone,
    };
    match coord.recent_results(&filter, limit) {
        Ok(rows) => {
            let results = rows
                .into_iter()
                .map(|r| ResultDto {
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
fn is_admin(coord: &Coordinator, headers: &HeaderMap) -> bool {
    let now = chrono::Utc::now().timestamp_millis();
    session_token(headers).is_some_and(|t| coord.inner.auth.validate(t, now))
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

async fn uptime_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    Query(q): Query<DaysQuery>,
) -> Response {
    let days = q.days.unwrap_or(90).clamp(1, 365);
    // The strip keeps a constant bar count; a shorter window just means finer
    // segments rather than fewer bars.
    let buckets = q.buckets.unwrap_or(90).clamp(1, 500);
    let now = chrono::Utc::now().timestamp_millis();
    match coord
        .inner
        .storage
        .lock()
        .uptime_buckets(&id, days, buckets, now)
    {
        Ok(rows) => {
            // Overall uptime = total UP across segments / total samples.
            let up: u32 = rows.iter().map(|b| b.up).sum();
            let total: u32 = rows.iter().map(|b| b.total).sum();
            let overall_uptime = if total > 0 {
                Some(up as f64 / total as f64)
            } else {
                None
            };
            let buckets = rows
                .into_iter()
                .map(|b| UptimeBucketDto {
                    start: b.start,
                    end: b.end,
                    uptime: b.uptime,
                    up: b.up,
                    total: b.total,
                })
                .collect();
            Json(UptimeResponse {
                check_id: id,
                days,
                overall_uptime,
                buckets,
            })
            .into_response()
        }
        Err(e) => internal_error(e),
    }
}

async fn latency_sparkline_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
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
        .latency_series(&id, from, to, buckets)
    {
        Ok(series) => {
            let points = series
                .points
                .into_iter()
                .map(|p| LatencyPointDto {
                    timestamp: p.timestamp,
                    avg_latency_ms: p.avg_latency_ms,
                })
                .collect();
            Json(LatencySparklineResponse {
                check_id: id,
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

async fn alerts_handler(State(coord): State<Coordinator>, Query(q): Query<LimitQuery>) -> Response {
    let limit = q.limit.unwrap_or(50).min(1000);
    match coord.inner.storage.lock().recent_alerts(limit) {
        Ok(rows) => {
            let alerts = rows.into_iter().map(AlertDto::from).collect();
            Json(AlertsResponse { alerts }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// Parse the `status` query. `open` means anything not resolved, since state is
/// the only lifecycle field; anything else that is not exactly `resolved` means
/// "both".
fn incident_open_filter(raw: Option<&str>) -> Option<bool> {
    match raw {
        Some("open") => Some(true),
        Some("resolved") => Some(false),
        _ => None,
    }
}

/// Recent incidents, newest first. Public like `/api/v1/alerts`: the status
/// page renders the open ones as current outages.
async fn incidents_handler(
    State(coord): State<Coordinator>,
    Query(q): Query<IncidentQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).min(1000);
    let open_only = incident_open_filter(q.status.as_deref());
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    match store.list_incidents(limit, open_only) {
        Ok(rows) => {
            let incidents = rows
                .into_iter()
                .map(|r| {
                    let ack = store.is_acknowledged(&r.id).unwrap_or(false);
                    IncidentDto::from_row(r, now, ack)
                })
                .collect();
            Json(IncidentsResponse { incidents }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// One incident with its full narrative. Public: the update timeline is the
/// entire point of writing it, so the permalink an anonymous visitor follows
/// serves the same record the operator sees.
async fn incident_handler(State(coord): State<Coordinator>, Path(id): Path<String>) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    match store.incident(&id) {
        Ok(Some(row)) => {
            let ack = store.is_acknowledged(&row.id).unwrap_or(false);
            let updates = store.updates_for_incident(&row.id).unwrap_or_default();
            let mut dto = IncidentDto::from_row(row, now, ack);
            dto.updates = Some(updates.into_iter().map(IncidentUpdateDto::from).collect());
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no incident with that id"),
        Err(e) => internal_error(e),
    }
}

/// Open an incident by hand — a maintenance notice, or something no probe can
/// see. `origin_check` is left unset, which is what keeps reconciliation away
/// from it: nothing auto-attaches and nothing auto-resolves.
async fn create_incident_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(req): Json<CreateIncidentRequest>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let title = req.title.trim();
    if title.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "title is required");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let started_at = req.started_at.unwrap_or(now);
    let state = parse_incident_state(req.state.as_deref())
        .unwrap_or(crate::coordinator::storage::IncidentState::Investigating);
    let id = format!("manual-{now}");
    let checks: Vec<crate::coordinator::storage::IncidentCheckRow> = req
        .checks
        .iter()
        .map(|c| crate::coordinator::storage::IncidentCheckRow {
            check_id: c.clone(),
            source: crate::coordinator::storage::AffectSource::Manual,
        })
        .collect();
    let body = req.body.unwrap_or_default();
    let store = coord.inner.storage.lock();
    match store.create_incident(
        &id,
        title,
        state,
        started_at,
        req.global.unwrap_or(false),
        None,
        &checks,
        Some((&body, Some("admin"))),
    ) {
        Ok(row) => {
            let dto = IncidentDto::from_row(row, now, true);
            let _ = coord.inner.events.send(WsEvent::IncidentOpened {
                incident: dto.clone(),
            });
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// Edit the operator-owned fields. Deliberately cannot move `state`: that only
/// happens by posting an update, so the timeline always explains the state.
async fn edit_incident_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<EditIncidentRequest>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let title = req.title.trim();
    if title.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "title is required");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    match store.edit_incident(&id, title, req.started_at, req.global) {
        Ok(Some(row)) => {
            let ack = store.is_acknowledged(&row.id).unwrap_or(false);
            let dto = IncidentDto::from_row(row, now, ack);
            let _ = coord.inner.events.send(WsEvent::IncidentUpdated {
                incident: dto.clone(),
            });
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no incident with that id"),
        Err(e) => internal_error(e),
    }
}

/// Delete an incident outright: the row, its narrative and its affects list.
///
/// Allowed while the incident is still open. The reconciler will simply open a
/// fresh one on the next confirmed DOWN if the check is still failing, which
/// is honest — the confirm copy in the editor says so rather than the server
/// refusing on the operator's behalf.
///
/// Linked alerts survive with a null `incident_id`: an alert records that a
/// check flipped, and that stays true whatever happens to the story told about
/// it.
async fn delete_incident_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let node_id = coord.inner.node_id.clone();
    let deleted = {
        let store = coord.inner.storage.lock();
        match store.delete_incident(&id, now, &node_id) {
            Ok(deleted) => deleted,
            Err(e) => return internal_error(e),
        }
    };
    if !deleted {
        return error_response(StatusCode::NOT_FOUND, "no incident with that id");
    }
    coord.replicate_incident_deletions(vec![id.clone()]);
    let _ = coord
        .inner
        .events
        .send(WsEvent::IncidentDeleted { id: id.clone() });
    info!(incident = %id, "incident deleted");
    Json(DeletedResponse { deleted: id }).into_response()
}

/// Replace the manually attached checks. Origin and dependency attachments are
/// untouched — those belong to reconciliation, and an operator editing this
/// list must not be able to delete the outage's own cause from it.
async fn incident_checks_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<IncidentChecksRequest>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    if let Err(e) = store.set_manual_checks(&id, &req.checks) {
        return internal_error(e);
    }
    match store.incident(&id) {
        Ok(Some(row)) => {
            let ack = store.is_acknowledged(&row.id).unwrap_or(false);
            let dto = IncidentDto::from_row(row, now, ack);
            let _ = coord.inner.events.send(WsEvent::IncidentUpdated {
                incident: dto.clone(),
            });
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no incident with that id"),
        Err(e) => internal_error(e),
    }
}

/// Post an update. The only way an incident changes state, and the only way it
/// gets acknowledged: an authored update on a `detected` incident IS the ack,
/// which is why there is no separate acknowledge route.
async fn incident_update_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<IncidentUpdateRequest>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let Some(state) = parse_incident_state(Some(&req.state)) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "state must be detected, investigating, identified, monitoring or resolved",
        );
    };
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    if store.incident(&id).ok().flatten().is_none() {
        return error_response(StatusCode::NOT_FOUND, "no incident with that id");
    }
    // Logins are password-only: there is no per-user identity to record, so the
    // byline is the role. What matters is that it is non-NULL — that is what
    // makes this an acknowledgement rather than a coordinator entry.
    if let Err(e) = store.add_update(&id, state, req.body.trim(), Some("admin"), now) {
        return internal_error(e);
    }
    match store.incident(&id) {
        Ok(Some(row)) => {
            let updates = store.updates_for_incident(&row.id).unwrap_or_default();
            let mut dto = IncidentDto::from_row(row, now, true);
            dto.updates = Some(updates.into_iter().map(IncidentUpdateDto::from).collect());
            let _ = coord.inner.events.send(WsEvent::IncidentUpdated {
                incident: dto.clone(),
            });
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no incident with that id"),
        Err(e) => internal_error(e),
    }
}

fn parse_incident_state(raw: Option<&str>) -> Option<crate::coordinator::storage::IncidentState> {
    use crate::coordinator::storage::IncidentState as S;
    match raw {
        Some("detected") => Some(S::Detected),
        Some("investigating") => Some(S::Investigating),
        Some("identified") => Some(S::Identified),
        Some("monitoring") => Some(S::Monitoring),
        Some("resolved") => Some(S::Resolved),
        _ => None,
    }
}

/// A single check's incident history, for the check detail view.
async fn check_incidents_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    Query(q): Query<IncidentQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(20).min(1000);
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    match store.incidents_for_check(&id, limit) {
        Ok(rows) => {
            let incidents = rows
                .into_iter()
                .map(|r| {
                    let ack = store.is_acknowledged(&r.id).unwrap_or(false);
                    IncidentDto::from_row(r, now, ack)
                })
                .collect();
            Json(IncidentsResponse { incidents }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// How long an agent may be silent before it reads as DOWN. With no registry,
/// silence is the only failure signal there is.
const AGENT_STALE_AFTER_MS: i64 = 5 * 60 * 1000;
/// How long before a silent agent is treated as retired: still in the store,
/// still owning its history, but out of the way. Deliberately generous — an
/// agent down for an afternoon is an incident, not a decommission.
const AGENT_RETIRED_AFTER_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// How long a *poll* may be missed before an agent stops counting toward
/// placement, derived from the cadence the agent declared.
///
/// Deliberately much tighter than `AGENT_STALE_AFTER_MS`, because the two
/// measure different clocks. Reporting cadence is the check's — a five-minute
/// cron check produces a result every five minutes, so judging a prober by its
/// results needs a generous window. Polling cadence is the agent's own, and it
/// asks for work every `poll_secs`, so missing several polls already means
/// gone.
///
/// Derived rather than fixed because a constant silently assumes one cadence:
/// 90s reads an agent on `poll_secs = 120` as unreachable while it is polling
/// happily, and holding a place is exclusive — the checks assigned to an agent
/// wrongly kept in the fleet are probed by nobody. As a product, detection is
/// the same number of missed cycles at any cadence.
///
/// `poll_secs` is `None` for an agent too old to declare one, and for entries
/// seeded from stored results at boot. Those keep the window this used to be
/// fixed at: the derived one would be *tighter* than what they were running
/// against, so a coordinator upgraded ahead of its fleet would start evicting
/// agents that had not changed anything.
const AGENT_UNDECLARED_CADENCE_MS: i64 = 90 * 1000;

fn unreachable_after_ms(missed_polls: u32, poll_secs: Option<u64>) -> i64 {
    let Some(secs) = poll_secs else {
        return AGENT_UNDECLARED_CADENCE_MS;
    };
    // Clamped so a malformed or hostile `?poll_secs=0` cannot make the window
    // zero — an agent that fell out of the fleet on every poll would have its
    // checks handed round the remaining agents forever.
    i64::from(missed_polls.max(1)) * secs.clamp(1, 3600) as i64 * 1000
}

/// Liveness of one agent from how long it has been quiet.
///
/// Never `Up`: only a poll makes an agent healthy. Reporting results is a
/// trailing signal — a check on a five-minute cron says nothing about the last
/// four minutes — and an idle agent with no checks at all would read dead.
fn silence_health(age_ms: i64) -> Health {
    if age_ms <= AGENT_RETIRED_AFTER_MS {
        Health::Degraded
    } else {
        Health::Down
    }
}

/// A region row's colour from the share of its agents that are healthy.
fn row_status(healthy: u32, total: u32, cfg: &crate::config::AgentsDisplay) -> Health {
    if total == 0 {
        return Health::Down;
    }
    let pct = f64::from(healthy) * 100.0 / f64::from(total);
    if pct < cfg.down_below {
        Health::Down
    } else if pct < cfg.warn_below {
        Health::Degraded
    } else {
        Health::Up
    }
}

/// The public agent section: the rows an operator placed, in their order,
/// followed by whatever the explicit rows did not already cover.
///
/// The fleet is the union of who is polling this node (in memory, so an agent
/// that has not reported yet still counts), who is polling *any* node in the
/// cluster (`polls`, replicated), and who has reported (persisted, so a restart
/// does not empty the section). An agent in several is read at its best:
/// polling is a live answer, results are a trailing one.
///
/// The replicated poll rows are what stop a three-node cluster from showing
/// two of its three regions down on every node: `seen` only ever holds the
/// agents polling the node being asked.
///
/// Pure so the composition and the thresholds can be tested without a server;
/// `fleet_health` is the reads that feed it.
fn fleet_rows(
    now: i64,
    seen: &std::collections::HashMap<String, SeenAgent>,
    polls: &[storage::AgentPollRow],
    reported: &[storage::AgentRow],
    cfg: &crate::config::AgentsDisplay,
) -> Vec<FleetRowDto> {
    use crate::config::{AgentsMode, FleetEntry};
    let mut fleet: std::collections::HashMap<&str, (&str, Health)> =
        std::collections::HashMap::new();
    for (id, s) in seen {
        let health = if now - s.at <= s.window_ms {
            Health::Up
        } else {
            silence_health(now - s.at)
        };
        fleet.insert(id, (&s.region, health));
    }
    // The window travels with the row: it is the cadence the agent declared to
    // whichever node it polled, and judging someone else's agent by this
    // node's idea of a cadence would read a slow poller as unreachable.
    for p in polls {
        let health = if now - p.at <= p.window_ms {
            Health::Up
        } else {
            silence_health(now - p.at)
        };
        fleet
            .entry(&p.agent_id)
            .and_modify(|e| e.1 = e.1.max(health))
            .or_insert((&p.region, health));
    }
    for r in reported {
        let health = silence_health(now - r.last_seen);
        fleet
            .entry(&r.agent_id)
            .and_modify(|e| e.1 = e.1.max(health))
            .or_insert((&r.region, health));
    }

    /// One agent's own row. Its three-state health is shown directly rather
    /// than through the region thresholds: 0 of 1 healthy is "silent", not
    /// "this region is 0% up".
    fn agent_row(name: &str, health: Health) -> FleetRowDto {
        FleetRowDto {
            kind: RowKind::Agent,
            name: name.to_string(),
            status: health,
            healthy: u32::from(health == Health::Up),
            total: 1,
        }
    }

    // Explicit rows first, in the order they were placed: a status page has to
    // be diffable across reloads, so nothing here is ordered by hash.
    let mut covered: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut rows: Vec<FleetRowDto> = Vec::with_capacity(cfg.rows.len());
    for entry in &cfg.rows {
        match entry {
            FleetEntry::Agent { name } => {
                // An agent absent from the fleet entirely reads down rather
                // than disappearing — "the box you asked about is not there"
                // is the answer, and a vanished row is not.
                let health = fleet.get(name.as_str()).map_or(Health::Down, |(_, h)| *h);
                covered.insert(name.as_str());
                rows.push(agent_row(name, health));
            }
            FleetEntry::Region { name } => {
                let mut healthy = 0;
                let mut total = 0;
                for (id, (region, health)) in &fleet {
                    if *region == name.as_str() {
                        // Counted even when the agent also has a row of its
                        // own above: the label says the whole region.
                        covered.insert(id);
                        total += 1;
                        healthy += u32::from(*health == Health::Up);
                    }
                }
                rows.push(FleetRowDto {
                    kind: RowKind::Region,
                    name: name.clone(),
                    status: row_status(healthy, total, cfg),
                    healthy,
                    total,
                });
            }
        }
    }

    // Then the remainder. `Off` means "nothing but what I placed", which is
    // also how the whole section is hidden: no explicit rows and nothing
    // automatic leaves nothing at all.
    let rest = fleet.iter().filter(|(id, _)| !covered.contains(*id));
    let mut auto: Vec<FleetRowDto> = match cfg.mode {
        AgentsMode::Off => Vec::new(),
        AgentsMode::Flat => rest
            .map(|(id, (_, health))| agent_row(id, *health))
            .collect(),
        AgentsMode::Regions => {
            let mut by_region: std::collections::HashMap<&str, (u32, u32)> =
                std::collections::HashMap::new();
            for (region, health) in rest.map(|(_, v)| v) {
                let e = by_region.entry(region).or_default();
                e.1 += 1;
                e.0 += u32::from(*health == Health::Up);
            }
            by_region
                .into_iter()
                .map(|(region, (healthy, total))| FleetRowDto {
                    kind: RowKind::Region,
                    name: region.to_string(),
                    status: row_status(healthy, total, cfg),
                    healthy,
                    total,
                })
                .collect()
        }
    };
    auto.sort_by(|a, b| a.name.cmp(&b.name));
    rows.extend(auto);
    rows
}

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

/// The fleet summary as the admin page edits it, plus the agent ids it can
/// choose from.
///
/// One call rather than two: the picker offers the agents and regions the rows
/// can name, so a settings response without the fleet is never useful alone.
#[derive(Serialize)]
struct AgentsDisplayResponse {
    agents: crate::config::AgentsDisplay,
    fleet: Vec<FleetMemberDto>,
}

#[derive(Serialize)]
struct FleetMemberDto {
    agent_id: String,
    region: String,
    /// Polling right now. Shown so an operator pinning a row can see they are
    /// about to publish something that is already silent.
    healthy: bool,
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

async fn agents_display_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    Json(AgentsDisplayResponse {
        agents: coord.inner.status_agents.read().clone(),
        fleet: fleet_members(&coord, now),
    })
    .into_response()
}

/// Replace the fleet summary. Admin-only: it decides what an anonymous visitor
/// is told about where monitoring runs from.
async fn update_agents_display_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<crate::config::AgentsDisplay>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let next = body.sanitized();
    if let Err(reason) = next.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    if let Err(e) = coord.save_agents_display(next) {
        return internal_error(e);
    }
    let now = chrono::Utc::now().timestamp_millis();
    info!("agent display settings saved");
    Json(AgentsDisplayResponse {
        agents: coord.inner.status_agents.read().clone(),
        fleet: fleet_members(&coord, now),
    })
    .into_response()
}

// ---- Notifiers ----

#[derive(Deserialize)]
struct NotifiersRequest {
    notifiers: Vec<crate::notify::Notifier>,
}

#[derive(Serialize)]
struct NotifiersResponse {
    notifiers: Vec<crate::notify::Notifier>,
    /// Checks routed to a notifier that no longer exists. They are enabled and
    /// notifying and reach nobody, which is otherwise completely silent — the
    /// same failure `checks_with_no_live_agent` exists to surface for agents.
    /// Usually the aftermath of renaming a notifier.
    unrouted_checks: Vec<String>,
    /// Notifications that have not been delivered: still being retried, or
    /// given up on. Newest first and capped — the panel needs to say "nobody
    /// was paged for this", not paginate a mailbox.
    undelivered: Vec<UndeliveredDto>,
}

#[derive(Serialize)]
struct UndeliveredDto {
    notifier: String,
    check_id: String,
    to: &'static str,
    /// Failed attempts so far.
    attempts: i32,
    /// True once the attempts are exhausted: this transition reached nobody and
    /// nothing further will be tried.
    given_up: bool,
    /// When the next attempt is due. Absent once given up.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_attempt: Option<i64>,
    error: String,
    queued_at: i64,
}

/// How many undelivered rows the panel is shown. Enough to read the shape of a
/// broken notifier; the counts beside it are exact.
const UNDELIVERED_SHOWN: usize = 20;

/// The saved list, the checks whose routing no longer resolves, and whatever
/// could not be delivered.
fn notifiers_response(coord: &Coordinator) -> Response {
    let notifiers = coord.inner.notifiers.read().clone();
    let known: std::collections::HashSet<&str> =
        notifiers.iter().map(|n| n.name.as_str()).collect();
    let mut unrouted: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| {
            m.enabled
                && m.notify
                && !m.notifiers.is_empty()
                && !m.notifiers.iter().any(|n| known.contains(n.as_str()))
        })
        .map(|(name, _)| name.clone())
        .collect();
    unrouted.sort();
    let undelivered = coord
        .inner
        .storage
        .lock()
        .undelivered_notifications(UNDELIVERED_SHOWN)
        .unwrap_or_default()
        .into_iter()
        .map(|q| UndeliveredDto {
            notifier: q.notifier,
            check_id: q.check_id,
            to: q.current_state.as_str(),
            attempts: q.attempts,
            given_up: q.next_attempt.is_none(),
            next_attempt: q.next_attempt,
            error: q.last_error,
            queued_at: q.queued_at,
        })
        .collect();
    Json(NotifiersResponse {
        notifiers,
        unrouted_checks: unrouted,
        undelivered,
    })
    .into_response()
}

/// Admin-only: notifier URLs are credentials in practice. A Slack webhook URL
/// is the entire authentication for posting to that channel.
async fn notifiers_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    notifiers_response(&coord)
}

/// Replace the whole list. The list is small and always edited as a whole in
/// the form, so there is no per-notifier id to address: the name is the
/// identity, which is exactly why it has to be unique.
async fn update_notifiers_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<NotifiersRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let next: Vec<crate::notify::Notifier> = body
        .notifiers
        .into_iter()
        .map(crate::notify::Notifier::sanitized)
        .collect();
    let mut names = std::collections::HashSet::new();
    for n in &next {
        if let Err(reason) = n.validate() {
            return error_response(StatusCode::BAD_REQUEST, &reason);
        }
        // Checks route by name, so two notifiers sharing one would make
        // "send this to oncall" ambiguous.
        if !names.insert(n.name.as_str()) {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!(
                    "two notifiers are called {} — names are what checks route to",
                    n.name
                ),
            );
        }
    }
    if let Err(e) = coord.save_notifiers(next) {
        return internal_error(e);
    }
    info!(
        count = coord.inner.notifiers.read().len(),
        "notifiers saved"
    );
    notifiers_response(&coord)
}

// ---- Maintenance windows ----

#[derive(Deserialize)]
struct MaintenanceRequest {
    windows: Vec<crate::maintenance::MaintenanceWindow>,
}

#[derive(Serialize)]
struct MaintenanceResponse {
    windows: Vec<crate::maintenance::MaintenanceWindow>,
    /// Every configured check, so the window editor's picker is one request —
    /// same reason the agent-display settings carry the fleet.
    checks: Vec<String>,
}

/// What an anonymous visitor is told about a window: the operator's title and
/// when it is over. Not the affected checks — a window naming eight checks and
/// the eight monitor rows beneath it would read as eight separate outages, the
/// same argument §7 makes for a global incident.
#[derive(Serialize)]
struct MaintenanceNoticeDto {
    title: String,
    starts_at: i64,
    ends_at: i64,
}

/// Announced windows running right now, soonest to end first — the one the
/// visitor is waiting out is the one that matters.
fn announced_maintenance(coord: &Coordinator, now: i64) -> Vec<MaintenanceNoticeDto> {
    let mut notices: Vec<MaintenanceNoticeDto> = coord
        .inner
        .maintenance
        .read()
        .iter()
        .filter(|w| w.announce && w.active_at(now))
        .map(|w| MaintenanceNoticeDto {
            title: w.title.clone(),
            starts_at: w.starts_at,
            ends_at: w.ends_at,
        })
        .collect();
    notices.sort_by_key(|n| n.ends_at);
    notices
}

fn maintenance_response(coord: &Coordinator) -> Response {
    let mut checks: Vec<String> = coord.inner.checks.read().keys().cloned().collect();
    checks.sort();
    Json(MaintenanceResponse {
        windows: coord.inner.maintenance.read().clone(),
        checks,
    })
    .into_response()
}

async fn maintenance_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    maintenance_response(&coord)
}

/// Replace the whole list, like the notifiers: it is small, always edited as a
/// whole, and a window has no identity beyond its span.
async fn update_maintenance_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<MaintenanceRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let next: Vec<crate::maintenance::MaintenanceWindow> = body
        .windows
        .into_iter()
        .map(crate::maintenance::MaintenanceWindow::sanitized)
        .collect();
    for w in &next {
        if let Err(reason) = w.validate() {
            return error_response(StatusCode::BAD_REQUEST, &reason);
        }
    }
    if let Err(e) = coord.save_maintenance(next) {
        return internal_error(e);
    }
    info!(
        count = coord.inner.maintenance.read().len(),
        "maintenance windows saved"
    );
    maintenance_response(&coord)
}

#[derive(Serialize)]
struct NotifierTestResponse {
    delivered: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Send one notification now, to a notifier that need not be saved yet.
///
/// A failed delivery is a 200 carrying `delivered: false`: the request worked,
/// the third party did not, and the form has to tell those apart. Without this
/// a wrong ntfy topic is completely silent — the POST succeeds, ntfy answers
/// 404, and nothing ever looks.
async fn test_notifier_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(notifier): Json<crate::notify::Notifier>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let notifier = notifier.sanitized();
    if let Err(reason) = notifier.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    // A recovery rather than an outage: a test that pages people with a fake
    // DOWN is a test nobody runs twice.
    let sample = alert::Transition {
        check_id: "warden-test".into(),
        from: Status::Unknown,
        to: Status::Up,
    };
    let result = crate::notify::send(&reqwest::Client::new(), &notifier, &sample).await;
    Json(NotifierTestResponse {
        delivered: result.is_ok(),
        error: result.err(),
    })
    .into_response()
}

/// Agents that have reported results.
///
/// There is no registration: an agent exists because it sent something. That
/// keeps deployment to "run another one", but it means an agent can only ever
/// *stop* existing by falling silent — so retirement is derived from
/// `last_seen` rather than being a state anyone has to maintain.
///
/// Admin-only. The fleet is infrastructure detail — agent ids, zones, regions
/// and reporting cadence together map where monitoring runs from, which is not
/// something an anonymous visitor to the status page should be able to read.
async fn agents_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let rows = match coord.inner.storage.lock().list_agents(now) {
        Ok(rows) => rows,
        Err(e) => return internal_error(e),
    };

    // A check pinned to agents that are all silent never runs, and nothing
    // else would say so: the check simply stops producing results, which looks
    // identical to a check nobody has got to yet.
    let live: std::collections::HashSet<String> = rows
        .iter()
        .filter(|r| now - r.last_seen <= AGENT_STALE_AFTER_MS)
        .map(|r| r.agent_id.clone())
        .collect();
    let orphaned: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| {
            m.enabled
                && m.kind.is_agent_probed()
                && !m.agents.is_empty()
                && !m.agents.iter().any(|a| live.contains(a))
        })
        .map(|(name, _)| name.clone())
        .collect();

    // Poll RTT is not on the persisted agent rows, so join it in by id.
    // Snapshot once rather than lock per row. The replicated poll rows come
    // first and local `seen` overrides them: an agent polling a peer would
    // otherwise show a blank RTT here, which reads as "never answered".
    let mut rtt_by_id: std::collections::HashMap<String, Option<i64>> = coord
        .inner
        .storage
        .lock()
        .list_agent_polls()
        .unwrap_or_default()
        .into_iter()
        .map(|p| (p.agent_id, p.rtt_ms))
        .collect();
    rtt_by_id.extend(
        coord
            .inner
            .seen
            .read()
            .iter()
            .map(|(id, s)| (id.clone(), s.rtt_ms)),
    );
    let agents = rows
        .into_iter()
        .map(|r| {
            let silence = now - r.last_seen;
            let poll_rtt_ms = rtt_by_id.get(&r.agent_id).copied().flatten();
            AgentDto {
                zone: r.zone.clone(),
                region: r.region.clone(),
                status: if silence <= AGENT_STALE_AFTER_MS {
                    Status::Up
                } else {
                    Status::Down
                },
                retired: silence > AGENT_RETIRED_AFTER_MS,
                assigned_checks: coord
                    .inner
                    .checks
                    .read()
                    .values()
                    .filter(|m| m.agents.contains(&r.agent_id))
                    .count() as u32,
                agent_id: r.agent_id,
                last_seen: r.last_seen,
                checks_reporting: r.checks_reporting,
                results_24h: r.results_24h,
                poll_rtt_ms,
            }
        })
        .collect();
    Json(AgentsResponse {
        agents,
        checks_with_no_live_agent: orphaned,
    })
    .into_response()
}

/// Delete every result an agent ever reported, which also removes it from the
/// agent list — the list is derived from those rows.
///
/// This is the only way an agent can be made to disappear promptly, and it
/// destroys history, so it is deliberately separate from an agent simply
/// falling silent.
async fn purge_agent_handler(
    State(coord): State<Coordinator>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let still_assigned: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| m.agents.contains(&agent_id))
        .map(|(name, _)| name.clone())
        .collect();
    // Purging an agent that checks are still pinned to would leave those checks
    // silently unrunnable. Name them and refuse.
    if !still_assigned.is_empty() {
        return error_response(
            StatusCode::CONFLICT,
            &format!(
                "still assigned to {}: unassign it there first, or those checks would stop running",
                still_assigned.join(", ")
            ),
        );
    }
    match coord.inner.storage.lock().purge_agent(&agent_id) {
        Ok(rows) => {
            info!(agent = %agent_id, rows, "purged agent history");
            Json(PurgedResponse { purged: rows }).into_response()
        }
        Err(e) => internal_error(e),
    }
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

#[derive(Deserialize)]
struct LoginRequest {
    password: String,
}

#[derive(Serialize)]
struct SessionResponse {
    authenticated: bool,
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

/// Trim the strings a form submits and drop the blanks, so an untouched field
/// means "unset" rather than a group literally named "" or an agent id of "".
fn normalise(mut c: CheckConfig) -> CheckConfig {
    let blank_to_none = |s: Option<String>| s.filter(|v: &String| !v.trim().is_empty());
    c.name = c.name.trim().to_string();
    c.target = c.target.trim().to_string();
    c.cron = c.cron.trim().to_string();
    c.group = blank_to_none(c.group);
    c.eval = blank_to_none(c.eval);
    c.description = blank_to_none(c.description);
    c.image = blank_to_none(c.image);
    c.tag = blank_to_none(c.tag);
    c.expect.body_contains = blank_to_none(c.expect.body_contains);
    c.dns.nameserver = blank_to_none(c.dns.nameserver);
    c.hosts = c
        .hosts
        .into_iter()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .collect();
    c.agents = c
        .agents
        .into_iter()
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty())
        .collect();
    c.dns.expected = c
        .dns
        .expected
        .into_iter()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    c.headers.retain(|h| !h.name.trim().is_empty());
    c
}

/// The admin wire type is `CheckConfig` itself. A parallel DTO duplicated
/// twenty-odd fields and two conversions for no gain — and a field added to one
/// and forgotten in the other is exactly the drift that would silently drop a
/// setting on save.
///
/// It carries the target, so it must only ever be produced behind authentication.
#[derive(Serialize)]
struct AdminChecksResponse {
    checks: Vec<CheckConfig>,
}

#[derive(Serialize)]
struct CheckResponse {
    check: CheckConfig,
}

#[derive(Serialize)]
struct DeletedResponse {
    deleted: String,
}

/// The session token presented by a request, if any.
fn session_token(headers: &HeaderMap) -> Option<&str> {
    let cookies = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    crate::auth::cookie_value(cookies, crate::auth::SESSION_COOKIE)
}

/// Gate for every `/admin` route. Returns the 401 body the frontend expects.
fn require_admin(coord: &Coordinator, headers: &HeaderMap) -> Option<Response> {
    (!is_admin(coord, headers))
        .then(|| error_response(StatusCode::UNAUTHORIZED, "authentication required"))
}

/// The bearer credential on a request, if it carries one.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Where an authenticated agent reports from. Read off its credential, never
/// off the request, because placement acts on it.
struct AgentLocation {
    zone: String,
    region: String,
}

/// Gate for endpoints that agents call.
///
/// A minted credential is the only way in. There is no fleet-wide fallback and
/// no open mode: an ingest endpoint that accepts anonymous results lets anyone
/// post for checks they do not run, and a monitor that can be told what it saw
/// is worse than no monitor, because it is believed.
///
/// A coordinator with no credentials accepts nothing from agents until an
/// operator mints one. That is the intended first-run state, not a failure.
// Boxed error: this is the ingest hot path, and an unboxed `Response` makes
// every returned `Result` 128+ bytes to move on the success path too.
fn authenticate_agent(
    coord: &Coordinator,
    headers: &HeaderMap,
) -> Result<AgentLocation, Box<Response>> {
    bearer(headers)
        .and_then(crate::credential::split)
        .and_then(|(id, secret)| {
            let creds = coord.inner.credentials.read();
            creds
                .get(id)
                .filter(|c| crate::credential::verify(c, secret))
                .map(|c| AgentLocation {
                    zone: c.zone.clone(),
                    region: c.region.clone(),
                })
        })
        .ok_or_else(|| {
            Box::new(error_response(
                StatusCode::UNAUTHORIZED,
                "a valid agent credential is required",
            ))
        })
}

/// Gate for `/internal/gossip`. Peers are infrastructure rather than agents, so
/// they carry their own secret: once agents hold minted credentials the shared
/// agent token is no longer handed out. Refuses when none is configured — the
/// endpoint rewrites published state.
fn require_peer(coord: &Coordinator, headers: &HeaderMap) -> Option<Response> {
    use subtle::ConstantTimeEq;
    let ok = match (coord.inner.peer_token.as_deref(), bearer(headers)) {
        (Some(expected), Some(got)) => bool::from(got.as_bytes().ct_eq(expected.as_bytes())),
        _ => false,
    };
    (!ok).then(|| error_response(StatusCode::UNAUTHORIZED, "peer authentication required"))
}

async fn login_handler(
    State(coord): State<Coordinator>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    Json(body): Json<LoginRequest>,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    match coord.inner.auth.login(&body.password, peer.ip(), now) {
        Ok(token) => {
            // HttpOnly so page scripts can't read it; SameSite=Strict so another
            // origin can't ride the session. `Secure` is deliberately omitted —
            // this is commonly served over plain HTTP on a LAN, and a Secure
            // cookie would silently never be stored there.
            let cookie = format!(
                "{}={}; HttpOnly; SameSite=Strict; Path=/; Max-Age={}",
                crate::auth::SESSION_COOKIE,
                token,
                12 * 60 * 60
            );
            (
                StatusCode::OK,
                [(axum::http::header::SET_COOKIE, cookie)],
                Json(SessionResponse {
                    authenticated: true,
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

async fn logout_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(token) = session_token(&headers) {
        coord.inner.auth.logout(token);
    }
    let cleared = format!(
        "{}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0",
        crate::auth::SESSION_COOKIE
    );
    (
        StatusCode::OK,
        [(axum::http::header::SET_COOKIE, cleared)],
        Json(SessionResponse {
            authenticated: false,
        }),
    )
        .into_response()
}

async fn session_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let authenticated = session_token(&headers).is_some_and(|t| coord.inner.auth.validate(t, now));
    Json(SessionResponse { authenticated }).into_response()
}

async fn list_checks_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    match coord.inner.storage.lock().list_checks() {
        Ok(checks) => Json(AdminChecksResponse { checks }).into_response(),
        Err(e) => internal_error(e),
    }
}

async fn create_check_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<CheckConfig>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let mut check = normalise(body);
    if let Err(reason) = check.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    {
        let store = coord.inner.storage.lock();
        let existing = match store.list_checks() {
            Ok(existing) => existing,
            Err(e) => return internal_error(e),
        };
        if existing.iter().any(|c| c.name == check.name) {
            return error_response(
                StatusCode::CONFLICT,
                &format!("a check named \"{}\" already exists", check.name),
            );
        }
        // Land at the end rather than at 0, so adding a check never silently
        // reshuffles the status page.
        match store.next_check_position() {
            Ok(p) => check.position = p,
            Err(e) => return internal_error(e),
        }
    }
    coord.save_check(check)
}

async fn update_check_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<CheckConfig>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let check = normalise(body);
    // The name is the identity. Renaming via PUT would orphan the check's
    // history, so the path wins and a mismatched body is a mistake worth saying.
    if check.name != name {
        return error_response(StatusCode::BAD_REQUEST, "a check cannot be renamed");
    }
    if let Err(reason) = check.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    coord.save_check(check)
}

async fn delete_check_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    // A tombstone, not a bare row delete: see `Storage::apply_config_entry`
    // on why an absence alone is not enough for a peer to tell "deleted"
    // apart from "never heard of it".
    if !coord.inner.checks.read().contains_key(&name) {
        return error_response(StatusCode::NOT_FOUND, "no such check");
    }
    let entry = match coord.write_config(storage::ConfigKind::Check, &name, None) {
        Ok(e) => e,
        Err(e) => return internal_error(e),
    };
    coord.replicate_config(vec![entry]);
    info!(check = %name, "check deleted");
    Json(DeletedResponse { deleted: name }).into_response()
}

#[derive(Serialize)]
struct AgentChecksResponse {
    checks: Vec<CheckConfig>,
}

/// The check list an agent should probe.
///
/// Disabled checks are filtered here rather than at the agent, so "disabled"
/// means the same thing everywhere. Everything else is decided by placement:
/// the coordinator works out which agents cover each check and hands each
/// caller only its share, so agents stay dumb and no assignment is stored
/// anywhere to drift from the fleet.
async fn agent_checks_handler(
    State(coord): State<Coordinator>,
    Query(q): Query<AgentIdentity>,
    headers: HeaderMap,
) -> Response {
    let authenticated = match authenticate_agent(&coord, &headers) {
        Ok(c) => c,
        Err(denied) => return *denied,
    };
    let Some(caller) = q.agent_id.as_deref() else {
        // An agent that will not say who it is cannot be placed. Older agents
        // predate identity, so they still get everything rather than silently
        // going idle on upgrade.
        return match coord.inner.storage.lock().list_checks() {
            Ok(checks) => Json(AgentChecksResponse {
                checks: checks
                    .into_iter()
                    .filter(|c| c.enabled && c.kind.is_agent_probed())
                    .collect(),
            })
            .into_response(),
            Err(e) => internal_error(e),
        };
    };

    let now = chrono::Utc::now().timestamp_millis();

    // Asking for work is contact, and it is the only signal a freshly booted
    // agent can give. Record it before deciding, so the caller is part of the
    // fleet it is about to be placed within.
    {
        // Placement is decided from this map, so a self-declared location would
        // be a way to be handed checks for a zone you are not in and then never
        // probe them. The credential decides; `?zone=`/`?region=` are ignored.
        let (zone, region) = (authenticated.zone.clone(), authenticated.region.clone());
        let mut seen = coord.inner.seen.write();
        seen.insert(
            caller.to_string(),
            SeenAgent {
                zone,
                region,
                at: now,
                // Cadence is not location: an agent lying about how often it
                // polls only widens or narrows its own liveness window, and a
                // wider one makes it look alive longer than it earns — capped
                // at an hour by `unreachable_after_ms`.
                window_ms: unreachable_after_ms(coord.inner.missed_polls, q.poll_secs),
                rtt_ms: q.rtt_ms.map(|v| v.min(i64::MAX as u64) as i64),
            },
        );
        // Forget agents long out of contact. Without this the map is an
        // unbounded leak wherever agent ids are not stable — a Deployment's
        // pods get a fresh name on every restart, so a year of rollouts would
        // accumulate thousands of ghosts. They are already excluded from the
        // fleet by age, so retaining them buys nothing.
        seen.retain(|_, s| now - s.at <= s.window_ms);
    }

    let checks = match coord.inner.storage.lock().list_checks() {
        Ok(c) => c,
        Err(e) => return internal_error(e),
    };

    // The fleet is everyone in recent contact. Polling alone is enough: waiting
    // for a first result would mean a booting agent is invisible exactly while
    // it needs placing, and twenty agents starting together would each conclude
    // they were alone and take the entire workload.
    let fleet: Vec<crate::placement::Agent> = coord
        .inner
        .seen
        .read()
        .iter()
        .filter(|(_, s)| now - s.at <= s.window_ms)
        .map(|(agent_id, s)| crate::placement::Agent {
            agent_id: agent_id.clone(),
            zone: s.zone.clone(),
            region: s.region.clone(),
        })
        .collect();

    let mine = checks
        .into_iter()
        .filter(|c| c.enabled && c.kind.is_agent_probed())
        .filter(|c| {
            crate::placement::assign(&c.name, &c.placement, &c.agents, &fleet)
                .iter()
                .any(|id| id == caller)
        })
        .collect();
    // Serialized as `CheckConfig`, which is exactly what the agent parses — one
    // type on both ends, so the probe config can't drift from the wire.
    Json(AgentChecksResponse { checks: mine }).into_response()
}

/// Who is asking for a check list.
///
/// Only the id: location is read off the caller's credential. It used to be
/// sent on the request, which meant the value placement trusts was chosen by
/// the party being placed.
#[derive(Deserialize)]
struct AgentIdentity {
    agent_id: Option<String>,
    /// How often this agent polls, so the coordinator can size the silence
    /// window to the cadence instead of assuming the default. Absent from
    /// agents that predate it; they get the default cadence.
    poll_secs: Option<u64>,
    /// Round-trip of this agent's *previous* poll, in milliseconds, measured by
    /// the agent. Self-asserted like `poll_secs`: it only feeds display, never
    /// placement or auth, so a lie changes nothing but its own row.
    rtt_ms: Option<u64>,
}

// ---- Status-page sections ----

#[derive(Serialize)]
struct SectionDto {
    name: String,
    position: i32,
    /// Whether the public page renders this section collapsed on first load.
    collapsed: bool,
}

impl From<storage::Section> for SectionDto {
    fn from(s: storage::Section) -> Self {
        Self {
            name: s.name,
            position: s.position,
            collapsed: s.collapsed,
        }
    }
}

#[derive(Serialize)]
struct SectionsResponse {
    sections: Vec<SectionDto>,
}

#[derive(Serialize)]
struct SectionResponse {
    section: SectionDto,
}

#[derive(Deserialize)]
struct SectionNameRequest {
    name: String,
    /// Absent on rename means "leave it as it is", so a client that predates the
    /// flag can't silently expand every section it touches.
    #[serde(default)]
    collapsed: Option<bool>,
}

#[derive(Deserialize)]
struct ReorderRequest {
    order: Vec<String>,
}

/// Section names appear as headings and are matched against `checks.group_name`,
/// so an empty or absurd one is rejected at the boundary.
fn valid_section_name(name: &str) -> Result<String, String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err("section name is required".into());
    }
    if trimmed.chars().count() > 60 {
        return Err("section name must be 60 characters or fewer".into());
    }
    Ok(trimmed.to_string())
}

fn sections_response(coord: &Coordinator) -> Response {
    match coord.inner.storage.lock().list_sections() {
        Ok(sections) => Json(SectionsResponse {
            sections: sections.into_iter().map(Into::into).collect(),
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// Section metadata for the public status page. No auth: a heading name and
/// whether it starts collapsed reveal nothing a visitor cannot already see.
async fn public_sections_handler(State(coord): State<Coordinator>) -> Response {
    sections_response(&coord)
}

async fn list_sections_handler(State(coord): State<Coordinator>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    sections_response(&coord)
}

async fn create_section_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<SectionNameRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let name = match valid_section_name(&body.name) {
        Ok(n) => n,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    match coord
        .inner
        .storage
        .lock()
        .create_section(&name, body.collapsed.unwrap_or(false))
    {
        Ok(true) => {}
        Ok(false) => {
            return error_response(
                StatusCode::CONFLICT,
                &format!("a section named \"{name}\" already exists"),
            )
        }
        Err(e) => return internal_error(e),
    }
    coord.restamp_and_push(storage::ConfigKind::Section, &name);
    Json(SectionResponse {
        section: SectionDto {
            name,
            position: 0,
            collapsed: body.collapsed.unwrap_or(false),
        },
    })
    .into_response()
}

async fn rename_section_handler(
    State(coord): State<Coordinator>,
    Path(current): Path<String>,
    headers: HeaderMap,
    Json(body): Json<SectionNameRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let name = match valid_section_name(&body.name) {
        Ok(n) => n,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    if name != current {
        let taken = match coord.inner.storage.lock().list_sections() {
            Ok(existing) => existing.iter().any(|s| s.name == name),
            Err(e) => return internal_error(e),
        };
        if taken {
            return error_response(
                StatusCode::CONFLICT,
                &format!("a section named \"{name}\" already exists"),
            );
        }
    }
    match coord.inner.storage.lock().rename_section(
        &current,
        &name,
        chrono::Utc::now().timestamp_millis(),
    ) {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::NOT_FOUND, "no such section"),
        Err(e) => return internal_error(e),
    }
    // Absent means "leave it alone"; the same endpoint is used to rename and to
    // toggle how the public page first renders the section.
    if let Some(collapsed) = body.collapsed {
        if let Err(e) = coord
            .inner
            .storage
            .lock()
            .set_section_collapsed(&name, collapsed)
        {
            return internal_error(e);
        }
    }
    // Renaming moves every check in the section, so the cached metadata the
    // status page reads is now stale.
    if let Err(e) = coord.reload_checks() {
        return internal_error(e);
    }
    coord.restamp_and_push(storage::ConfigKind::Section, &name);
    // Every check `rename_section` moved gets its own stamp and push — its
    // document changed too (its `group` now names the new section), and a
    // peer that only heard about the section itself would show those checks
    // still under the old name.
    //
    // Collected into an owned `Vec` in its own statement first: a `for`
    // loop's head keeps any temporary it creates (the read guard) alive for
    // the whole loop body, not just until the `Vec` is built, and
    // `restamp_and_push` below takes a write lock on this same map via
    // `reload_checks` — holding the read guard that long would deadlock it
    // against itself.
    let moved: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| m.group.as_deref() == Some(name.as_str()))
        .map(|(n, _)| n.clone())
        .collect();
    for check_name in moved {
        coord.restamp_and_push(storage::ConfigKind::Check, &check_name);
    }
    info!(from = %current, to = %name, "section renamed");
    let collapsed = coord
        .inner
        .storage
        .lock()
        .list_sections()
        .ok()
        .and_then(|all| all.into_iter().find(|s| s.name == name))
        .map(|s| s.collapsed)
        .unwrap_or(false);
    Json(SectionResponse {
        section: SectionDto {
            name,
            position: 0,
            collapsed,
        },
    })
    .into_response()
}

async fn delete_section_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    // Captured before the delete moves them: the checks that are about to be
    // loosened, so every one of them can be stamped and pushed too — its
    // document changed (`group` is now `None`), the same as any other edit.
    let affected: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| m.group.as_deref() == Some(name.as_str()))
        .map(|(n, _)| n.clone())
        .collect();
    match coord
        .inner
        .storage
        .lock()
        .delete_section(&name, chrono::Utc::now().timestamp_millis())
    {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::NOT_FOUND, "no such section"),
        Err(e) => return internal_error(e),
    }
    if let Err(e) = coord.reload_checks() {
        return internal_error(e);
    }
    // A tombstone for the section itself, pushed the same way any other
    // delete is.
    match coord.write_config(storage::ConfigKind::Section, &name, None) {
        Ok(entry) => coord.replicate_config(vec![entry]),
        Err(e) => {
            tracing::warn!(error = %e, section = %name, "config: failed to push section tombstone")
        }
    }
    for check_name in &affected {
        coord.restamp_and_push(storage::ConfigKind::Check, check_name);
    }
    info!(section = %name, "section deleted, its checks are now loose");
    Json(DeletedResponse { deleted: name }).into_response()
}

async fn reorder_sections_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<ReorderRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let before = match coord.inner.storage.lock().list_sections() {
        Ok(s) => s,
        Err(e) => return internal_error(e),
    };
    if let Err(e) = coord.inner.storage.lock().reorder_sections(&body.order) {
        return internal_error(e);
    }
    let after = match coord.inner.storage.lock().list_sections() {
        Ok(s) => s,
        Err(e) => return internal_error(e),
    };
    // Only the sections whose position actually moved are stamped and
    // pushed: the ones the reorder rewrote are the ones a peer needs to
    // hear about, the rest already agree.
    let before_pos: std::collections::HashMap<&str, i32> = before
        .iter()
        .map(|s| (s.name.as_str(), s.position))
        .collect();
    for s in &after {
        if before_pos.get(s.name.as_str()) != Some(&s.position) {
            coord.restamp_and_push(storage::ConfigKind::Section, &s.name);
        }
    }
    sections_response(&coord)
}

// ---- Script try-it ----

#[derive(Deserialize)]
struct EvalTestRequest {
    #[serde(rename = "eval")]
    source: String,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    latency: u64,
    #[serde(default)]
    body: Option<String>,
    /// OpenMetrics eval testing: the selected sample value the script sees as
    /// `metric`, plus the same-scrape samples it sees as `metrics`. Absent for
    /// HTTP-style scripts, so existing eval-test requests are unaffected.
    #[serde(default)]
    metric: Option<f64>,
    #[serde(default)]
    metrics: Vec<crate::openmetrics::MetricSample>,
}

#[derive(Serialize)]
struct EvalTestResponse {
    verdict: &'static str,
    latency_ms: u64,
}

async fn eval_test_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<EvalTestRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let input = crate::script::EvalInput {
        status: body.status,
        latency_ms: body.latency,
        body: body.body,
        headers: Default::default(),
        metric: body.metric,
        metrics: body.metrics,
    };
    match crate::script::evaluate(&body.source, &input) {
        Ok(outcome) => Json(EvalTestResponse {
            verdict: if outcome.up { "up" } else { "down" },
            latency_ms: outcome.latency_ms.unwrap_or(body.latency),
        })
        .into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, &e),
    }
}

#[derive(Deserialize)]
struct CheckOrderRequest {
    order: Vec<String>,
}

async fn reorder_checks_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<CheckOrderRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let before_pos: std::collections::HashMap<String, i32> = coord
        .inner
        .checks
        .read()
        .iter()
        .map(|(name, m)| (name.clone(), m.position))
        .collect();
    if let Err(e) = coord
        .inner
        .storage
        .lock()
        .reorder_checks(&body.order, chrono::Utc::now().timestamp_millis())
    {
        return internal_error(e);
    }
    let checks = match coord.reload_checks() {
        Ok(checks) => checks,
        Err(e) => return internal_error(e),
    };
    // Only the checks whose position actually moved are stamped and pushed —
    // the ones the reorder rewrote are the ones a peer needs to hear about,
    // not the whole table.
    for c in &checks {
        if before_pos.get(&c.name) != Some(&c.position) {
            coord.restamp_and_push(storage::ConfigKind::Check, &c.name);
        }
    }
    Json(AdminChecksResponse { checks }).into_response()
}

/// Record a heartbeat ping. The URL is the credential, so a wrong name is a
/// plain 404 rather than an error that confirms which names exist.
async fn heartbeat_handler(State(coord): State<Coordinator>, Path(name): Path<String>) -> Response {
    let is_heartbeat = coord
        .inner
        .checks
        .read()
        .get(&name)
        .is_some_and(|m| m.kind == crate::models::CheckKind::Heartbeat);
    if !is_heartbeat {
        return error_response(StatusCode::NOT_FOUND, "no such heartbeat");
    }
    coord
        .ingest(vec![CheckResult {
            zone: crate::config::UNKNOWN_LOCATION.into(),
            region: crate::config::UNKNOWN_LOCATION.into(),
            check_id: name.clone(),
            agent_id: crate::models::HEARTBEAT_AGENT.into(),
            kind: crate::models::CheckKind::Heartbeat,
            timestamp: chrono::Utc::now(),
            latency_ms: 0,
            status_code: None,
            body_matched: None,
            error: None,
            failure: None,
            body: None,
            headers: None,
            metric: None,
            metrics: None,
        }])
        .await;
    (StatusCode::OK, "ok").into_response()
}

/// Erase a check and everything it ever produced: its definition, its results,
/// its alerts, and every incident it opened.
///
/// A third action rather than a flag on the delete, because it is the only one
/// that removes a page anonymous visitors have already read. The check goes
/// through the same config-tombstone path as a plain delete — an absence alone
/// does not survive a peer's next anti-entropy sweep — and each incident gets
/// its own tombstone for the same reason.
async fn purge_check_everything_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    if !coord.inner.checks.read().contains_key(&name) {
        return error_response(StatusCode::NOT_FOUND, "no such check");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let node_id = coord.inner.node_id.clone();
    let summary = {
        let store = coord.inner.storage.lock();
        match store.purge_check_everything(&name, now, &node_id) {
            Ok(s) => s,
            Err(e) => return internal_error(e),
        }
    };
    let entry = match coord.write_config(storage::ConfigKind::Check, &name, None) {
        Ok(e) => e,
        Err(e) => return internal_error(e),
    };
    coord.replicate_config(vec![entry]);
    coord.replicate_incident_deletions(summary.deleted_incidents.clone());
    for id in &summary.deleted_incidents {
        let _ = coord
            .inner
            .events
            .send(WsEvent::IncidentDeleted { id: id.clone() });
    }
    info!(
        check = %name,
        results = summary.results,
        incidents = summary.incidents,
        "check erased"
    );
    Json(ErasedResponse {
        deleted: name,
        results: summary.results,
        incidents: summary.incidents,
    })
    .into_response()
}

#[derive(Serialize)]
struct ErasedResponse {
    deleted: String,
    results: u64,
    incidents: u64,
}

/// Drop a check's recorded history while keeping its configuration. Separate
/// from deleting the check: "start the graph over" and "stop monitoring this"
/// are different intentions and a single button would conflate them.
async fn purge_check_data_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    match coord.inner.storage.lock().purge_check_data(&name) {
        Ok(rows) => {
            info!(check = %name, rows, "purged check history");
            Json(PurgedResponse { purged: rows }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

#[derive(Serialize)]
struct PurgedResponse {
    purged: u64,
}

// ---- Agent credentials ----

/// A credential as the admin UI reads it. The secret is absent by construction:
/// there is nothing stored that could populate it.
#[derive(Serialize)]
struct AgentCredentialDto {
    id: String,
    zone: String,
    region: String,
    created_at: i64,
    revoked: bool,
}

impl From<&AgentCredential> for AgentCredentialDto {
    fn from(c: &AgentCredential) -> Self {
        Self {
            id: c.id.clone(),
            zone: c.zone.clone(),
            region: c.region.clone(),
            created_at: c.created_at,
            revoked: c.revoked(),
        }
    }
}

#[derive(Serialize)]
struct AgentCredentialsResponse {
    credentials: Vec<AgentCredentialDto>,
}

#[derive(Deserialize)]
struct MintCredentialRequest {
    zone: String,
    region: String,
}

#[derive(Serialize)]
struct MintedCredentialResponse {
    credential: AgentCredentialDto,
    /// The only time this value exists outside the agent that will use it.
    token: String,
}

async fn list_agent_credentials_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    match coord.inner.storage.lock().list_agent_credentials() {
        Ok(rows) => Json(AgentCredentialsResponse {
            credentials: rows.iter().map(AgentCredentialDto::from).collect(),
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// Mint a credential for one zone.
///
/// The token comes back exactly once. Storing it so it could be shown again
/// would defeat hashing it, so the UI has to make the operator copy it now.
async fn mint_agent_credential_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<MintCredentialRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let zone = body.zone.trim();
    let region = body.region.trim();
    // A blank location would bind the credential to nothing and place its
    // agents in the "no location declared" bucket, which is the state minting
    // is supposed to replace.
    if zone.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "zone must not be empty");
    }
    if region.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "region must not be empty");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let (record, token) = crate::credential::mint(zone, region, now);
    if let Err(e) = coord.store_credential(&record) {
        return internal_error(e);
    }
    info!(id = %record.id, zone, region, "minted agent credential");
    (
        StatusCode::CREATED,
        Json(MintedCredentialResponse {
            credential: AgentCredentialDto::from(&record),
            token,
        }),
    )
        .into_response()
}

async fn revoke_agent_credential_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let revoked = coord.inner.storage.lock().revoke_agent_credential(&id, now);
    match revoked {
        Ok(true) => {
            if let Err(e) = coord.reload_credentials() {
                return internal_error(e);
            }
            // Revocation is security-relevant and time-sensitive — pushed
            // to peers immediately rather than waiting on anti-entropy, so
            // a credential stops working fleet-wide as close as possible to
            // the moment an operator revoked it.
            coord.restamp_and_push(storage::ConfigKind::Credential, &id);
            warn!(id = %id, "revoked agent credential");
            Json(RevokedResponse { revoked: id }).into_response()
        }
        Ok(false) => error_response(StatusCode::NOT_FOUND, "no such credential"),
        Err(e) => internal_error(e),
    }
}

#[derive(Serialize)]
struct RevokedResponse {
    revoked: String,
}

#[cfg(test)]
mod fleet_tests {
    use super::*;
    use crate::config::{AgentsDisplay, AgentsMode};

    const NOW: i64 = 1_700_000_000_000;

    fn polling(region: &str, ago_ms: i64, poll_secs: u64) -> SeenAgent {
        SeenAgent {
            zone: format!("{region}-a"),
            region: region.into(),
            at: NOW - ago_ms,
            window_ms: unreachable_after_ms(3, Some(poll_secs)),
            rtt_ms: None,
        }
    }

    fn seen(agents: &[(&str, SeenAgent)]) -> std::collections::HashMap<String, SeenAgent> {
        agents
            .iter()
            .map(|(id, s)| ((*id).to_string(), s.clone()))
            .collect()
    }

    fn reported(agent_id: &str, region: &str, ago_ms: i64) -> storage::AgentRow {
        storage::AgentRow {
            agent_id: agent_id.into(),
            zone: format!("{region}-a"),
            region: region.into(),
            last_seen: NOW - ago_ms,
            checks_reporting: 1,
            results_24h: 1,
        }
    }

    fn row<'a>(rows: &'a [FleetRowDto], name: &str) -> &'a FleetRowDto {
        rows.iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("no row named {name}"))
    }

    /// The coupling the design called out: a fixed 90s window reads an agent on
    /// a two-minute cadence as gone while it is polling happily.
    #[test]
    fn the_window_follows_the_declared_cadence() {
        assert_eq!(unreachable_after_ms(3, Some(10)), 30_000);
        assert_eq!(unreachable_after_ms(3, Some(120)), 360_000);
        // Three missed polls, whatever the cadence.
        assert_eq!(unreachable_after_ms(5, Some(120)), 600_000);
        // An agent that declares nothing keeps the window this used to be fixed
        // at, so upgrading the coordinator first does not evict the fleet.
        assert_eq!(unreachable_after_ms(3, None), 90_000);
        // A zero cadence would make every agent permanently unreachable.
        assert!(unreachable_after_ms(3, Some(0)) > 0);
        assert!(unreachable_after_ms(0, Some(10)) > 0);
    }

    #[test]
    fn a_slow_poller_within_its_own_window_is_healthy() {
        // 100s of silence: past the old fixed 90s, inside 3 x 120s.
        let rows = fleet_rows(
            NOW,
            &seen(&[("box", polling("eu", 100_000, 120))]),
            &[],
            &[],
            &AgentsDisplay::default(),
        );
        assert_eq!(row(&rows, "eu").status, Health::Up);
    }

    /// Liveness, not check health: an agent with no results at all is healthy
    /// while it answers.
    #[test]
    fn an_idle_agent_that_polls_is_healthy() {
        let rows = fleet_rows(
            NOW,
            &seen(&[("idle", polling("eu", 1_000, 10))]),
            &[],
            &[],
            &AgentsDisplay::default(),
        );
        assert_eq!(row(&rows, "eu").healthy, 1);
    }

    #[test]
    fn silence_is_three_states_not_two() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &seen(&[
                ("live", polling("eu", 1_000, 10)),
                ("quiet", polling("eu", 10 * 60_000, 10)),
                ("gone", polling("eu", 8 * 24 * 3_600_000, 10)),
            ]),
            &[],
            &[],
            &cfg,
        );
        assert_eq!(row(&rows, "live").status, Health::Up);
        assert_eq!(row(&rows, "quiet").status, Health::Degraded);
        assert_eq!(row(&rows, "gone").status, Health::Down);
    }

    /// Results are a trailing signal, so they can only lift a silent agent to
    /// degraded — never to healthy.
    #[test]
    fn reporting_without_polling_never_reads_healthy() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &Default::default(),
            &[],
            &[reported("b", "eu", 5_000)],
            &cfg,
        );
        assert_eq!(row(&rows, "b").status, Health::Degraded);
    }

    fn polled(agent_id: &str, region: &str, ago_ms: i64, poll_secs: u64) -> storage::AgentPollRow {
        storage::AgentPollRow {
            agent_id: agent_id.into(),
            zone: format!("{region}-a"),
            region: region.into(),
            at: NOW - ago_ms,
            window_ms: unreachable_after_ms(3, Some(poll_secs)),
            rtt_ms: Some(7),
        }
    }

    /// The reported bug: on a three-node cluster each coordinator's `seen` only
    /// holds the agents polling *it*, so two of the three regions rolled up
    /// 0/1 healthy and the status page called them down. A replicated poll row
    /// is a live answer wherever it was observed.
    #[test]
    fn a_peers_poll_row_alone_reads_healthy() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &Default::default(),
            &[polled("agent-b", "eu", 1_000, 10)],
            &[],
            &cfg,
        );
        assert_eq!(row(&rows, "agent-b").status, Health::Up);
    }

    /// The window travels with the row rather than being the receiver's: a peer
    /// applying its own cadence would call a slow poller unreachable.
    #[test]
    fn a_peers_poll_row_past_its_own_window_is_not_healthy() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        // 100s of silence on a 10s cadence (window 30s) is silent; the same
        // silence on a 120s cadence (window 360s) is not.
        let stale = fleet_rows(
            NOW,
            &Default::default(),
            &[polled("agent-b", "eu", 100_000, 10)],
            &[],
            &cfg,
        );
        assert_eq!(row(&stale, "agent-b").status, Health::Degraded);

        let slow = fleet_rows(
            NOW,
            &Default::default(),
            &[polled("agent-b", "eu", 100_000, 120)],
            &[],
            &cfg,
        );
        assert_eq!(row(&slow, "agent-b").status, Health::Up);
    }

    /// Both sources for one agent: the best reading wins, in either direction.
    /// A node's own `seen` is fresher than the last snapshot it broadcast, and
    /// a peer's row is fresher than a `seen` entry this node has let go stale.
    #[test]
    fn a_local_and_a_remote_row_for_one_agent_read_at_their_best() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let remote_is_better = fleet_rows(
            NOW,
            &seen(&[("a", polling("eu", 10 * 60_000, 10))]),
            &[polled("a", "eu", 1_000, 10)],
            &[],
            &cfg,
        );
        assert_eq!(remote_is_better.len(), 1, "one agent, one row");
        assert_eq!(row(&remote_is_better, "a").status, Health::Up);

        let local_is_better = fleet_rows(
            NOW,
            &seen(&[("a", polling("eu", 1_000, 10))]),
            &[polled("a", "eu", 10 * 60_000, 10)],
            &[],
            &cfg,
        );
        assert_eq!(row(&local_is_better, "a").status, Health::Up);
    }

    /// A restart empties `seen`; the persisted results keep the section honest
    /// until the fleet polls again, and the live reading wins once it does.
    #[test]
    fn the_live_reading_beats_the_stored_one() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &seen(&[("a", polling("eu", 1_000, 10))]),
            &[],
            &[reported("a", "eu", 6 * 24 * 3_600_000)],
            &cfg,
        );
        assert_eq!(rows.len(), 1, "one agent, one row: {rows:?}");
        assert_eq!(row(&rows, "a").status, Health::Up);
    }

    #[test]
    fn a_region_row_colours_on_the_share_that_is_healthy() {
        let cfg = AgentsDisplay::default();
        let half = fleet_rows(
            NOW,
            &seen(&[
                ("a", polling("eu", 1_000, 10)),
                ("b", polling("eu", 10 * 60_000, 10)),
            ]),
            &[],
            &[],
            &cfg,
        );
        // 50% healthy: under warn_below (100), not under down_below (50).
        assert_eq!(row(&half, "eu").status, Health::Degraded);
        assert_eq!((row(&half, "eu").healthy, row(&half, "eu").total), (1, 2));

        let none = fleet_rows(
            NOW,
            &seen(&[("a", polling("eu", 10 * 60_000, 10))]),
            &[],
            &[],
            &cfg,
        );
        assert_eq!(row(&none, "eu").status, Health::Down);
    }

    #[test]
    fn thresholds_are_configurable() {
        let lenient = AgentsDisplay {
            warn_below: 50.0,
            down_below: 25.0,
            ..Default::default()
        };
        // 50% healthy is green here and amber under the defaults.
        assert_eq!(row_status(1, 2, &lenient), Health::Up);
        assert_eq!(
            row_status(1, 2, &AgentsDisplay::default()),
            Health::Degraded
        );
        assert_eq!(row_status(1, 5, &lenient), Health::Down);
    }

    fn agent(name: &str) -> crate::config::FleetEntry {
        crate::config::FleetEntry::Agent { name: name.into() }
    }
    fn region(name: &str) -> crate::config::FleetEntry {
        crate::config::FleetEntry::Region { name: name.into() }
    }
    fn names(rows: &[FleetRowDto]) -> Vec<&str> {
        rows.iter().map(|r| r.name.as_str()).collect()
    }

    /// The ask this model exists for: "show agent-b, then the whole us-east
    /// region" — one agent and one region, in that order, mixed in one list.
    #[test]
    fn rows_are_published_in_the_order_they_were_placed() {
        let cfg = AgentsDisplay {
            rows: vec![agent("office-box"), region("us")],
            mode: AgentsMode::Off,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &seen(&[
                ("office-box", polling("eu", 1_000, 10)),
                ("eu-1", polling("eu", 1_000, 10)),
                ("us-1", polling("us", 1_000, 10)),
                ("us-2", polling("us", 10 * 60_000, 10)),
            ]),
            &[],
            &[],
            &cfg,
        );
        assert_eq!(
            names(&rows),
            ["office-box", "us"],
            "placed order, nothing else"
        );
        assert_eq!(rows[0].kind, RowKind::Agent);
        assert_eq!(rows[1].kind, RowKind::Region);
        // The region row counts its whole region: one of two healthy.
        assert_eq!((rows[1].healthy, rows[1].total), (1, 2));
        assert_eq!(rows[1].status, Health::Degraded);
        // eu-1 was never placed and nothing automatic is on, so it is absent.
    }

    /// A region row means the whole region even when one of its agents is also
    /// listed on its own — a row labelled `us` that quietly omitted a machine
    /// would be a lie.
    #[test]
    fn a_region_row_still_counts_an_agent_shown_individually() {
        let cfg = AgentsDisplay {
            rows: vec![agent("us-1"), region("us")],
            mode: AgentsMode::Off,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &seen(&[
                ("us-1", polling("us", 1_000, 10)),
                ("us-2", polling("us", 1_000, 10)),
            ]),
            &[],
            &[],
            &cfg,
        );
        assert_eq!(names(&rows), ["us-1", "us"]);
        assert_eq!((rows[1].healthy, rows[1].total), (2, 2), "both, not one");
    }

    /// Everything the placed rows did not cover follows them, and only that.
    #[test]
    fn the_remainder_excludes_whatever_the_placed_rows_already_showed() {
        let fleet = seen(&[
            ("office-box", polling("eu", 1_000, 10)),
            ("eu-1", polling("eu", 1_000, 10)),
            ("us-1", polling("us", 1_000, 10)),
        ]);

        // An agent row takes that agent out of its region's automatic row.
        let rows = fleet_rows(
            NOW,
            &fleet,
            &[],
            &[],
            &AgentsDisplay {
                rows: vec![agent("office-box")],
                ..Default::default()
            },
        );
        assert_eq!(names(&rows), ["office-box", "eu", "us"]);
        assert_eq!(row(&rows, "eu").total, 1, "office-box is not counted twice");

        // A region row takes every one of its agents out of the remainder.
        let rows = fleet_rows(
            NOW,
            &fleet,
            &[],
            &[],
            &AgentsDisplay {
                rows: vec![region("eu")],
                mode: AgentsMode::Flat,
                ..Default::default()
            },
        );
        assert_eq!(names(&rows), ["eu", "us-1"], "only us-1 is left over");
    }

    #[test]
    fn a_placed_row_survives_the_remainder_being_off_but_nothing_else_does() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Off,
            rows: vec![agent("office-box")],
            ..Default::default()
        };
        let fleet = seen(&[("office-box", polling("eu", 1_000, 10))]);
        let rows = fleet_rows(NOW, &fleet, &[], &[reported("b", "eu", 5_000)], &cfg);
        assert_eq!(names(&rows), ["office-box"]);

        // With nothing placed, "off" is how the whole section disappears.
        let hidden = AgentsDisplay {
            mode: AgentsMode::Off,
            ..Default::default()
        };
        assert!(fleet_rows(NOW, &fleet, &[], &[reported("b", "eu", 5_000)], &hidden).is_empty());
    }

    /// A name nobody has ever reported still gets its row: "the box you asked
    /// about is not there" is an answer, a vanished row is not.
    #[test]
    fn a_placed_row_for_something_unknown_reads_down() {
        let cfg = AgentsDisplay {
            rows: vec![agent("never-existed"), region("never-deployed")],
            mode: AgentsMode::Off,
            ..Default::default()
        };
        let rows = fleet_rows(NOW, &Default::default(), &[], &[], &cfg);
        assert_eq!(names(&rows), ["never-existed", "never-deployed"]);
        assert!(rows.iter().all(|r| r.status == Health::Down), "{rows:?}");
    }

    /// The superseded `pinned` list still parses and becomes agent rows, so a
    /// stored document or config file written before this keeps working.
    #[test]
    fn a_legacy_pinned_list_becomes_agent_rows() {
        let legacy: AgentsDisplay =
            serde_json::from_str(r#"{"mode":"regions","pinned":["box-a","box-b"]}"#).unwrap();
        let upgraded = legacy.sanitized();
        assert_eq!(upgraded.rows, vec![agent("box-a"), agent("box-b")]);
        assert!(upgraded.pinned.is_empty(), "and is not carried twice");
        // Saving it back does not re-emit the legacy field.
        let json = serde_json::to_string(&upgraded).unwrap();
        assert!(!json.contains("pinned"), "{json}");
    }

    /// Two rows keyed the same hang the page rather than looking wrong, so an
    /// exact repeat is dropped — but an agent and a region may share a name.
    #[test]
    fn repeated_rows_are_dropped_but_a_name_may_be_both() {
        let cfg = AgentsDisplay {
            rows: vec![agent("us"), region("us"), agent("us"), agent("  ")],
            ..Default::default()
        }
        .sanitized();
        assert_eq!(cfg.rows, vec![agent("us"), region("us")]);
    }
}
