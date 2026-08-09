pub mod alert;
pub mod gossip;
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
    /// A check went DOWN and a new incident opened.
    IncidentOpened { incident: IncidentDto },
    /// A check came back UP, closing its incident.
    IncidentResolved { incident: IncidentDto },
}

/// The shape of an incident as the UI reads it. Also carried inside WS events.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentDto {
    pub id: String,
    pub check_id: String,
    pub status: &'static str,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
    pub details: Option<String>,
    pub acknowledged_at: Option<i64>,
    pub acknowledged_by: Option<String>,
}

impl From<storage::IncidentRow> for IncidentDto {
    fn from(i: storage::IncidentRow) -> Self {
        Self {
            id: i.id,
            check_id: i.check_id,
            status: i.status.as_str(),
            started_at: i.started_at,
            resolved_at: i.resolved_at,
            details: i.details,
            acknowledged_at: i.acknowledged_at,
            acknowledged_by: i.acknowledged_by,
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
    /// Agents named explicitly. Empty means every agent.
    agents: Vec<String>,
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
                    agents: c.agents.clone(),
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
    /// Alerts leave through here, never inline. `None` when no channel is
    /// configured, so nothing is queued for a dispatcher that does not exist.
    notify_tx: Option<tokio::sync::mpsc::Sender<alert::Transition>>,
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
}

/// An agent observed making contact: where it said it was, and when.
#[derive(Clone)]
struct SeenAgent {
    zone: String,
    region: String,
    at: i64,
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

        // The notifier runs beside ingest, not inside it. Bounded so a wedged
        // endpoint cannot grow an unbounded backlog in memory; overflow drops
        // the notification rather than the results behind it.
        let notify_tx = if cfg.notifiers.is_empty() {
            None
        } else {
            let notifiers = cfg.notifiers.clone();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<alert::Transition>(1024);
            let client = reqwest::Client::new();
            tokio::spawn(async move {
                while let Some(t) = rx.recv().await {
                    for notifier in &notifiers {
                        crate::notify::send(&client, notifier, &t).await;
                    }
                }
            });
            Some(tx)
        };
        if storage.seed_checks_if_empty(checks, now)? {
            info!(count = checks.len(), "seeded checks from config file");
        }
        // A deployment that predates the sections table has group names only on
        // its checks; adopt them so the admin page isn't empty on first open.
        storage.seed_sections_from_checks()?;
        let stored = storage.list_checks()?;
        if stored.is_empty() {
            // No checks anywhere — seed demo data so the dashboard isn't blank.
            if storage.seed_mock(now)? {
                info!("no checks configured, seeded demo data");
            }
        }
        let auth = crate::auth::Auth::new(cfg.resolved_admin_password());
        if !auth.admin_enabled() {
            warn!("no admin password set (WARDEN_ADMIN_PASSWORD); the check editor is disabled");
        }
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
        Ok(Self {
            inner: Arc::new(Inner {
                storage: Mutex::new(storage),
                tracker: Mutex::new(tracker),
                retention: cfg.retention.clone(),
                notify_tx,
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
            }),
        })
    }

    /// Persist a credential and make it live immediately.
    fn store_credential(&self, record: &AgentCredential) -> Result<()> {
        self.inner.storage.lock().insert_agent_credential(record)?;
        self.reload_credentials()
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

        // Incidents are driven by observations, not transitions, so a
        // restart (which forgets the tracker's baseline) cannot strand a
        // check in the wrong incident state. The open incident is the
        // source of truth: a DOWN observation opens one when none is open,
        // the first UP observation resolves it. Unknown keeps it open — an
        // un-evaluable probe is not a confirmed recovery.
        let mut incident_events = Vec::new();
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
            for (t, ts) in transitions.iter().filter(|(t, _)| alert::should_alert(t)) {
                if let Err(e) = store.insert_alert(&t.check_id, t.from, t.to, *ts) {
                    tracing::error!(error = %e, "failed to persist alert");
                }
            }
            for (r, status) in &rows {
                let ts = r.timestamp.timestamp_millis();
                let ev = match status {
                    // Only broadcast an open when this observation actually
                    // opened it; a DOWN that lands while one is already open
                    // rides on the existing incident.
                    Status::Down => {
                        // A DOWN that has not yet confirmed (fewer than
                        // `failures_before_down` consecutive probes) is not an
                        // outage: no incident, and a later recovery cannot be
                        // misread as a fix for something that never opened.
                        if confirmed.get(&r.check_id) != Some(&Status::Down) {
                            None
                        } else {
                            match store.open_incident_for(&r.check_id) {
                                Ok(None) => match store.create_incident(
                                    &r.check_id,
                                    ts,
                                    incident_details(r).as_deref(),
                                ) {
                                    Ok(i) => Some(WsEvent::IncidentOpened { incident: i.into() }),
                                    Err(e) => {
                                        tracing::error!(error = %e, check = %r.check_id, "failed to open incident");
                                        None
                                    }
                                },
                                Ok(Some(_)) => None,
                                Err(e) => {
                                    tracing::error!(error = %e, check = %r.check_id, "failed to look up incident");
                                    None
                                }
                            }
                        }
                    }
                    Status::Up => match store.resolve_incident(&r.check_id, ts) {
                        Ok(Some(i)) => Some(WsEvent::IncidentResolved { incident: i.into() }),
                        Ok(None) => None,
                        Err(e) => {
                            tracing::error!(error = %e, check = %r.check_id, "failed to resolve incident");
                            None
                        }
                    },
                    Status::Unknown => None,
                };
                if let Some(ev) = ev {
                    incident_events.push(ev);
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
                    .map(|(t, ts)| gossip::Message::new(t, *ts))
                    .collect();
                tokio::spawn(async move {
                    for m in &msgs {
                        peers.broadcast(m).await;
                    }
                });
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
            if let Some(tx) = &self.inner.notify_tx {
                if tx.try_send(t.clone()).is_err() {
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

    /// Persist one check and refresh the cached metadata. Shared by create and
    /// update so both paths reload identically.
    fn save_check(&self, check: CheckConfig) -> Response {
        let now = chrono::Utc::now().timestamp_millis();
        if let Err(e) = self.inner.storage.lock().upsert_check(&check, now) {
            return internal_error(e);
        }
        if let Err(e) = self.reload_checks() {
            return internal_error(e);
        }
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
            .route("/api/v1/incidents", get(incidents_handler))
            .route(
                "/api/v1/incidents/:id/acknowledge",
                post(acknowledge_incident_handler),
            )
            .route("/api/v1/checks/:id/incidents", get(check_incidents_handler))
            .route("/api/v1/agents", get(agents_handler))
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
            .with_state(self)
            .fallback(static_handler)
    }
}

// ---- API response DTOs. The consumer is frontend/src/lib/api.ts; keep the two
// in step when changing a shape here. ----

#[derive(Serialize)]
struct StatusResponse {
    checks: Vec<CheckStatusDto>,
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
struct AlertsResponse {
    alerts: Vec<AlertDto>,
}

#[derive(Serialize)]
struct AlertDto {
    check_id: String,
    from: Status,
    to: Status,
    timestamp: i64,
}

#[derive(Serialize)]
struct IncidentsResponse {
    incidents: Vec<IncidentDto>,
}

/// Query for the incidents endpoints. `status` mirrors the row's lifecycle,
/// not a check's UP/DOWN/UNKNOWN: `?status=open` lists only current outages.
#[derive(Deserialize)]
struct IncidentQuery {
    limit: Option<usize>,
    status: Option<String>,
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
            if let Err(e) = store.insert_alert(&t.check_id, t.from, t.to, msg.timestamp) {
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
            Json(StatusResponse { checks }).into_response()
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
            let alerts = rows
                .into_iter()
                .map(|r| AlertDto {
                    check_id: r.check_id,
                    from: r.from,
                    to: r.to,
                    timestamp: r.timestamp,
                })
                .collect();
            Json(AlertsResponse { alerts }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// Parse the `status` query into an incident lifecycle filter. Anything that is
/// not exactly `open` or `resolved` means "both".
fn incident_status_filter(
    raw: Option<&str>,
) -> Option<crate::coordinator::storage::IncidentStatus> {
    match raw {
        Some("open") => Some(crate::coordinator::storage::IncidentStatus::Open),
        Some("resolved") => Some(crate::coordinator::storage::IncidentStatus::Resolved),
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
    let status = incident_status_filter(q.status.as_deref());
    match coord.inner.storage.lock().list_incidents(limit, status) {
        Ok(rows) => {
            let incidents = rows.into_iter().map(IncidentDto::from).collect();
            Json(IncidentsResponse { incidents }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// Acknowledge an open incident. Admin-only: acknowledging is an operator
/// action, so the route requires a session like the check editor does.
async fn acknowledge_incident_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    // Logins are password-only: there is no per-user identity to record, so the
    // byline is the role. The column exists for the day multi-user sessions do.
    let now = chrono::Utc::now().timestamp_millis();
    match coord
        .inner
        .storage
        .lock()
        .acknowledge_incident(&id, "admin", now)
    {
        Ok(Some(row)) => {
            Json(serde_json::json!({ "incident": IncidentDto::from(row) })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no open incident with that id"),
        Err(e) => internal_error(e),
    }
}

/// A single check's incident history, for the check detail view.
async fn check_incidents_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    Query(q): Query<IncidentQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(20).min(1000);
    match coord.inner.storage.lock().incidents_for_check(&id, limit) {
        Ok(rows) => {
            let incidents = rows.into_iter().map(IncidentDto::from).collect();
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
/// placement.
///
/// Deliberately much tighter than `AGENT_STALE_AFTER_MS`, because the two
/// measure different clocks. Reporting cadence is the check's — a five-minute
/// cron check produces a result every five minutes, so judging a prober by its
/// results needs a generous window. Polling cadence is the agent's own, and it
/// asks for work every `poll_secs` (10 by default), so missing several polls
/// already means gone.
///
/// The window matters because holding a place is exclusive: while a departed
/// agent still counts, the checks assigned to it are probed by nobody. Five
/// minutes of that is a real gap in coverage; ninety seconds is a cycle or two.
const AGENT_UNREACHABLE_AFTER_MS: i64 = 90 * 1000;

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

    let agents = rows
        .into_iter()
        .map(|r| {
            let silence = now - r.last_seen;
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
    let removed = match coord.inner.storage.lock().delete_check(&name) {
        Ok(removed) => removed,
        Err(e) => return internal_error(e),
    };
    if !removed {
        return error_response(StatusCode::NOT_FOUND, "no such check");
    }
    if let Err(e) = coord.reload_checks() {
        return internal_error(e);
    }
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
            },
        );
        // Forget agents long out of contact. Without this the map is an
        // unbounded leak wherever agent ids are not stable — a Deployment's
        // pods get a fresh name on every restart, so a year of rollouts would
        // accumulate thousands of ghosts. They are already excluded from the
        // fleet by age, so retaining them buys nothing.
        seen.retain(|_, s| now - s.at <= AGENT_UNREACHABLE_AFTER_MS);
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
        .filter(|(_, s)| now - s.at <= AGENT_UNREACHABLE_AFTER_MS)
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
    if let Err(e) = coord.inner.storage.lock().reorder_sections(&body.order) {
        return internal_error(e);
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
    if let Err(e) = coord
        .inner
        .storage
        .lock()
        .reorder_checks(&body.order, chrono::Utc::now().timestamp_millis())
    {
        return internal_error(e);
    }
    match coord.reload_checks() {
        Ok(checks) => Json(AdminChecksResponse { checks }).into_response(),
        Err(e) => internal_error(e),
    }
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
        }])
        .await;
    (StatusCode::OK, "ok").into_response()
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
