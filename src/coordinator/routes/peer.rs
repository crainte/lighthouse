//! The plane coordinators use to talk to each other, not the one agents or
//! admins use to talk to a coordinator.
//!
//! **Membership, both replication planes, and anti-entropy, together.**
//! Gossip carries a single confirmed transition; `/internal/replicate` and
//! `/internal/config` carry whole batches — results/alerts/incidents, and
//! config entries, respectively; digest and backfill are how a node that
//! missed either catches back up. Four shapes, one plane, because all four
//! answer the same question — "what does another node believe has
//! happened?" — and none of them make sense to expose to an agent, whose
//! only claim on a coordinator is the results it reports.
//!
//! **Peer-gated, because these endpoints rewrite published state.** Every
//! handler here calls `require_peer` before touching anything, with one
//! exception: `cluster_view_handler` answers the admin UI's read of
//! membership and is gated by `require_admin` instead, because it is a human
//! in a browser asking, not a peer — see its own doc comment.

use super::super::{alert, cluster, error_response, gossip, hlc, internal_error};
use super::super::{storage, Coordinator, WsEvent};
use super::session::{bearer, require_admin};
use crate::models::{CheckResult, Status};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

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
pub(in crate::coordinator) async fn gossip_handler(
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
    pub(in crate::coordinator) fn into_row(self) -> storage::AlertRow {
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
pub(in crate::coordinator) async fn replicate_handler(
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
pub(in crate::coordinator) struct ConfigBatch {
    pub(in crate::coordinator) origin: String,
    pub(in crate::coordinator) entries: Vec<storage::ConfigEntry>,
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
pub(in crate::coordinator) async fn config_handler(
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
pub(in crate::coordinator) fn read_config_entry_for_replication(
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
    pub(in crate::coordinator) fn new(node_id: &str, d: storage::Digest) -> Self {
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
pub(in crate::coordinator) async fn digest_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
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
pub(in crate::coordinator) struct ConfigEntriesQuery {
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
pub(in crate::coordinator) async fn config_entries_handler(
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
pub(in crate::coordinator) struct BackfillQuery {
    check_id: String,
    since: i64,
    until: i64,
    limit: usize,
}

/// `GET /internal/backfill`: paged raw rows for one check, `since` exclusive
/// — the pull counterpart to `/internal/replicate`'s push, and the same
/// `ReplicationBatch` shape, so a caller can hand the response straight to a
/// peer's `/internal/replicate` without reshaping it.
pub(in crate::coordinator) async fn backfill_handler(
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
pub(in crate::coordinator) async fn cluster_ping_handler(
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

pub(in crate::coordinator) async fn cluster_view_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
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
                // An unclustered node still runs a build, and this view is
                // where an operator looks for it.
                build: Some(crate::version::current().to_string()),
                protocol: Some(cluster::PROTOCOL_VERSION),
            },
            Vec::new(),
        ),
    };
    Json(ClusterViewResponse { me, members, now }).into_response()
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
