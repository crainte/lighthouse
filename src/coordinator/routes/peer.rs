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
//! membership and is gated by `crate::http::AdminSession` instead, because it
//! is a human in a browser asking, not a peer — see its own doc comment.

use super::super::router::{error_response, internal_error};
use super::super::{alert, cluster, gossip};
use super::super::{storage, Coordinator, WsEvent};
use super::session::bearer;
use crate::configplane::{ConfigBatch, ConfigDigestDto};
use crate::http::AdminSession;
use crate::models::{CheckResult, NodeState, Status};
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
    body: axum::body::Bytes,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    // Parsed only once the peer token has checked out: `Json<T>` as an
    // argument would reject a malformed body before `require_peer` above
    // ever ran, letting a caller with no peer credential — an agent's token,
    // say — learn the body's shape was wrong instead of the uniform 401
    // every other peer route gives it.
    let msg: gossip::Message = match serde_json::from_slice(&body) {
        Ok(m) => m,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let _state_write = coord.inner.state_write.lock().await;
    let t = msg.transition();
    let persisted = t.clone();
    let timestamp = msg.timestamp;
    let alert_id = msg.alert_id;
    let canonical_alert = msg.alert;
    let message_incident = msg.incident_id;
    if let Some(alert) = &canonical_alert {
        let agrees = alert_id.as_deref() == Some(alert.id.as_str())
            && alert.scope == storage::AlertScope::Check.as_str()
            && alert.node_id == persisted.check_id.to_string()
            && alert.previous_state == Some(persisted.from)
            && alert.current_state == persisted.to
            && alert.timestamp == timestamp;
        if !agrees {
            return error_response(
                StatusCode::BAD_REQUEST,
                "gossip alert does not match its transition",
            );
        }
    }
    let inserted = match coord
        .db(move |store| {
            store.transaction(|store| {
                if !alert::should_alert(&persisted) {
                    store.update_current_state(
                        persisted.check_id,
                        persisted.to,
                        None,
                        timestamp,
                    )?;
                    return Ok(true);
                }
                let incident_id = match message_incident {
                    Some(id) => Some(id),
                    None => store
                        .open_incident_for(persisted.check_id)?
                        .map(|incident| incident.id),
                };
                // Either the sender's whole immutable row, or nothing of the
                // sender's identity at all. A bare `alert_id` used to be
                // accepted and paired with this node's own `check_id`, which
                // silently produced a row whose id names one check and whose
                // body names another as soon as two nodes disagreed about a
                // check's id. `upsert_alert` treats alerts as immutable, so
                // that row could never converge and anti-entropy re-offered it
                // every sweep. Minting locally instead yields the same
                // content-derived id on every node that agrees about the
                // check, and a plainly local alert on one that does not.
                let inserted = if let Some(row) = canonical_alert {
                    let id = row.id.clone();
                    let applied = store.upsert_alert(&row.into_row())?;
                    if applied == storage::AlertApply::Conflict {
                        tracing::warn!(alert = %id, "gossip: alert id conflicts with a different local event, keeping the local one");
                    }
                    applied == storage::AlertApply::Inserted
                } else {
                    store.insert_alert(
                        storage::AlertScope::Check,
                        &persisted.check_id.to_string(),
                        Some(persisted.from),
                        persisted.to,
                        timestamp,
                        incident_id.as_deref(),
                    )?;
                    true
                };
                if inserted {
                    store.update_current_state(
                        persisted.check_id,
                        persisted.to,
                        None,
                        timestamp,
                    )?;
                    store.record_state(persisted.check_id, persisted.to, timestamp)?;
                }
                Ok(inserted)
            })
        })
        .await
    {
        Ok(inserted) => inserted,
        Err(error) => {
            tracing::warn!(%error, check = %t.check_id, "gossip: failed to persist transition");
            return internal_error(error);
        }
    };
    if inserted {
        coord
            .inner
            .tracker
            .lock()
            .seed_published(t.check_id, t.to, None);
        let _ = coord.inner.events.send(WsEvent::StateChange {
            check_id: t.check_id,
            from: t.from,
            to: t.to,
            timestamp: msg.timestamp,
        });
    }
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

/// One agent's confirmed belief about one check, as it replicates.
///
/// A zone's agents are its corroboration set, and they do not all report to the
/// same coordinator. Replicating only the observer's *conclusion* left every
/// node folding a partial tree — a two-agent zone read as two separate
/// one-agent zones, so `all/all` could be satisfied by a single flaky agent and
/// the corroboration property was unreachable in any real cluster. The leaves
/// travel so that every node folds the same tree.
///
/// This carries the tracker's *confirmed* belief, not a raw probe verdict: the
/// `failures_before_down` run belongs to the observing node and a peer must not
/// restart it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedBelief {
    pub check_id: crate::models::CheckId,
    pub agent_id: String,
    pub zone: String,
    pub region: String,
    pub status: Status,
    pub consecutive_failures: u32,
    /// When the observing coordinator last heard from this agent. Also the
    /// merge key: see `StateTracker::merge_agent`.
    pub last_check: i64,
}

/// One confirmed transition as it replicates. Mirrors `storage::AlertRow`
/// field-for-field, but on the wire — `AlertRow` itself has no `Serialize`,
/// being owned by a different concern, and `AlertScope` travels as its
/// string form so this type does not need `storage` to grow a derive it
/// otherwise has no use for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedAlert {
    pub id: String,
    pub scope: String,
    pub node_id: String,
    pub previous_state: Option<NodeState>,
    pub current_state: NodeState,
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
    /// Per-agent beliefs from the sender's tracker, so every node folds the
    /// same tree instead of only the part of the fleet it personally observed.
    #[serde(default)]
    pub beliefs: Vec<ReplicatedBelief>,
    /// Incident ids deleted on the sender. Applied as a delete *plus a local
    /// tombstone*, so this node will not hand the incident back later either.
    #[serde(default)]
    pub deleted_incidents: Vec<storage::IncidentTombstone>,
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
    match coord.apply_replicated(batch).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => internal_error(error),
    }
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
    match coord.apply_config_batch(&batch.entries).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => internal_error(e),
    }
}

/// Gate for `/internal/global/*`: the shared secret a `[coordinator.global]`
/// block presents, the same value `is_peer_request` (in `routes::checks`)
/// already accepts for admin fan-out. A region that trusts the global node to
/// drive an admin write on an operator's behalf trusts it to durably
/// distribute one too — this is not a second, weaker credential, it is the
/// same one used for a second purpose.
fn require_global_trust(coord: &Coordinator, headers: &HeaderMap) -> Option<Response> {
    use subtle::ConstantTimeEq;
    let ok = match (coord.inner.admin_trust_token.as_deref(), bearer(headers)) {
        (Some(expected), Some(got)) => bool::from(got.as_bytes().ct_eq(expected.as_bytes())),
        _ => false,
    };
    (!ok).then(|| error_response(StatusCode::UNAUTHORIZED, "global authentication required"))
}

/// `POST /internal/global/config`: the global tier's push of the catalogue it
/// holds authoritatively (SCALING.md "Placement-scoped configuration" Phase
/// one) — the same `apply_config_batch` a regional peer's `/internal/config`
/// push uses, so a document that arrives this way is indistinguishable from
/// one a regional admin edit produced. What differs is only the gate: a
/// global node is not one of this region's peers, so it authenticates with
/// the cross-region trust token instead of `peer_token`.
pub(in crate::coordinator) async fn global_config_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(batch): Json<ConfigBatch>,
) -> Response {
    if let Some(denied) = require_global_trust(&coord, &headers) {
        return denied;
    }
    match coord.apply_config_batch(&batch.entries).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => internal_error(e),
    }
}

/// `GET /internal/global/config-digest`: this region's config-only digest, for
/// a global node's reconciliation sweep to compare against its own and decide
/// which kinds are worth pushing rather than resending the whole catalogue
/// every sweep. Narrower than `/internal/digest`: a global node has no
/// results, alerts or rollups to compare, and computing them here just to
/// discard them would cost the database worker for nothing.
pub(in crate::coordinator) async fn global_config_digest_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_global_trust(&coord, &headers) {
        return denied;
    }
    match coord.db(|storage| storage.config_digest()).await {
        Ok(digests) => Json(
            digests
                .iter()
                .map(ConfigDigestDto::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// `GET /internal/global/check-ids`: every check id this region currently
/// holds. Once placement scoping means a region's Check kind is a *subset*
/// of the global catalogue (SCALING.md "Placement-scoped configuration",
/// Phase 2), a whole-kind digest mismatch no longer says which ids are
/// missing or which are excess — only that the two sides disagree. This is
/// the cheap read (primary keys only, no `config` JSON parse) a global node
/// makes to find out, and only when the digest already says a check-kind
/// mismatch exists, never on every sweep.
pub(in crate::coordinator) async fn global_check_ids_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_global_trust(&coord, &headers) {
        return denied;
    }
    match coord.db(|storage| storage.check_ids()).await {
        Ok(ids) => Json(ids).into_response(),
        Err(e) => internal_error(e),
    }
}

/// Read back one id's replicated form for `Coordinator::replicate_config`.
/// `Storage::config_entry` only ever sees the live table, so it reads back
/// `None` for an id whose latest write was a delete — the tombstone that
/// delete left behind lives in a different table entirely. Falling back to a
/// point read of that one tombstone is what lets deletes and
/// upserts share `replicate_config` as one path instead of the delete
/// handlers needing a second, parallel push of their own.
pub(in crate::coordinator) fn read_config_entry_for_replication(
    store: &storage::Storage,
    kind: storage::ConfigKind,
    id: &str,
) -> Option<storage::ConfigEntry> {
    match store.config_entry(kind, id) {
        Ok(Some(e)) => Some(e),
        Ok(None) => match store.config_tombstone(kind, id) {
            Ok(tombstone) => tombstone,
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

/// One check's contribution to a digest, on the wire. Mirrors
/// `storage::CheckDigest`, which has no `Serialize` of its own.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDigestDto {
    pub max_millis: i64,
    pub max_id: String,
    pub count: i64,
}

impl From<&storage::StreamDigest> for StreamDigestDto {
    fn from(d: &storage::StreamDigest) -> Self {
        Self {
            max_millis: d.max_millis,
            max_id: d.max_id.clone(),
            count: d.count,
        }
    }
}

/// One rollup table's contribution to a digest, on the wire. Mirrors
/// `storage::RollupDigest` — see its doc comment for why it carries more than
/// a max and a count.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RollupDigestDto {
    pub max_bucket_start: i64,
    pub count: i64,
    pub attempts_sum: i64,
    pub up_sum: i64,
    pub latency_sum_sum: i64,
}

impl From<&storage::RollupDigest> for RollupDigestDto {
    fn from(d: &storage::RollupDigest) -> Self {
        Self {
            max_bucket_start: d.max_bucket_start,
            count: d.count,
            attempts_sum: d.attempts_sum,
            up_sum: d.up_sum,
            latency_sum_sum: d.latency_sum_sum,
        }
    }
}

/// One rollup row, on the wire. Mirrors `storage::RollupRow` — the
/// `/internal/backfill/latency-minute`/`-hour` response shape, and the body
/// `apply_minute_rollups`/`apply_hour_rollups` consume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupRowDto {
    pub check_id: String,
    pub agent_id: String,
    pub bucket_start: i64,
    pub attempts: i64,
    pub up: i64,
    pub latency_sum: i64,
    pub latency_min: Option<i64>,
    pub latency_max: Option<i64>,
}

impl From<&storage::RollupRow> for RollupRowDto {
    fn from(r: &storage::RollupRow) -> Self {
        Self {
            check_id: r.check_id.to_string(),
            agent_id: r.agent_id.clone(),
            bucket_start: r.bucket_start,
            attempts: r.attempts,
            up: r.up,
            latency_sum: r.latency_sum,
            latency_min: r.latency_min,
            latency_max: r.latency_max,
        }
    }
}

impl RollupRowDto {
    pub(in crate::coordinator) fn into_row(self) -> anyhow::Result<storage::RollupRow> {
        Ok(storage::RollupRow {
            check_id: self.check_id.parse()?,
            agent_id: self.agent_id,
            bucket_start: self.bucket_start,
            attempts: self.attempts,
            up: self.up,
            latency_sum: self.latency_sum,
            latency_min: self.latency_min,
            latency_max: self.latency_max,
        })
    }
}

/// `GET /internal/backfill/latency-minute`'s and `-hour`'s response: a page
/// of rollup rows plus who answered. Its own shape rather than
/// `ReplicationBatch`'s, because rollups never travel through
/// `/internal/replicate` — they are derived locally from replicated raw
/// results (`derived_results_sql`) everywhere except the retention-gap
/// recovery this backs, so giving them a field on the general-purpose push
/// batch would invite a second, redundant path for writing the same rows.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RollupBatch {
    pub origin: String,
    pub rows: Vec<RollupRowDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckDigestDto {
    pub check_id: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub count: i64,
}

impl From<&storage::CheckDigest> for CheckDigestDto {
    fn from(d: &storage::CheckDigest) -> Self {
        CheckDigestDto {
            check_id: d.check_id.to_string(),
            min_ts: d.min_ts,
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
    pub alerts: StreamDigestDto,
    pub incidents: StreamDigestDto,
    pub incident_tombstones: StreamDigestDto,
    /// Defaulted so a node running an older build — one that predates rollup
    /// anti-entropy — still deserializes: it degrades to "this peer has
    /// nothing", which never blocks the rest of the sweep and simply means
    /// rollup recovery waits for both sides to upgrade.
    #[serde(default)]
    pub latency_minute_rollups: RollupDigestDto,
    #[serde(default)]
    pub latency_hour_rollups: RollupDigestDto,
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
            alerts: StreamDigestDto::from(&d.alerts),
            incidents: StreamDigestDto::from(&d.incidents),
            incident_tombstones: StreamDigestDto::from(&d.incident_tombstones),
            latency_minute_rollups: RollupDigestDto::from(&d.latency_minute_rollups),
            latency_hour_rollups: RollupDigestDto::from(&d.latency_hour_rollups),
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
    let digest = coord.db(|storage| storage.digest(0)).await;
    match digest {
        Ok(d) => Json(DigestResponse::new(&coord.inner.node_id, d)).into_response(),
        Err(e) => internal_error(e),
    }
}

/// Config kind to reconcile. Pagination is scoped to one kind.
#[derive(Debug, Deserialize)]
pub(in crate::coordinator) struct ConfigEntriesQuery {
    kind: Option<storage::ConfigKind>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default = "default_config_entries_limit")]
    limit: usize,
}

const CONFIG_ENTRIES_PAGE: usize = 1_000;

fn default_config_entries_limit() -> usize {
    CONFIG_ENTRIES_PAGE
}

/// `GET /internal/config`: one bounded page of config entries, tombstones
/// included, as the pull half of the config plane. Check documents are
/// projected directly from storage instead of issuing a read per id. The POST
/// on this path is the push half; this is what a peer calls when a digest tells
/// it the push never arrived.
pub(in crate::coordinator) async fn config_entries_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<ConfigEntriesQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let Some(kind) = q.kind else {
        return error_response(StatusCode::BAD_REQUEST, "config kind is required");
    };
    let limit = q.limit.clamp(1, CONFIG_ENTRIES_PAGE);
    let after = q.after;
    let entries = match coord
        .db(move |storage| storage.config_entries_page(kind, after.as_deref(), limit))
        .await
    {
        Ok(entries) => entries,
        Err(e) => return internal_error(e),
    };
    Json(ConfigBatch {
        origin: coord.inner.node_id.clone(),
        entries,
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
pub(in crate::coordinator) struct StreamBackfillQuery {
    since: i64,
    #[serde(default)]
    since_node_id: String,
    #[serde(default)]
    since_id: String,
    limit: usize,
}

pub(in crate::coordinator) async fn alerts_backfill_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<StreamBackfillQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let limit = q.limit.min(super::super::replication::ANTI_ENTROPY_PAGE);
    match coord
        .db(move |s| s.alerts_since(q.since, &q.since_id, limit))
        .await
    {
        Ok(rows) => Json(ReplicationBatch {
            origin: coord.inner.node_id.clone(),
            alerts: rows.iter().map(ReplicatedAlert::from).collect(),
            ..Default::default()
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn incidents_backfill_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<StreamBackfillQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let limit = q.limit.min(super::super::replication::ANTI_ENTROPY_PAGE);
    match coord
        .db(move |s| s.incidents_since(q.since, &q.since_id, limit))
        .await
    {
        Ok(rows) => Json(ReplicationBatch {
            origin: coord.inner.node_id.clone(),
            incidents: rows,
            ..Default::default()
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn incident_tombstones_backfill_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<StreamBackfillQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let limit = q.limit.min(super::super::replication::ANTI_ENTROPY_PAGE);
    match coord
        .db(move |s| s.incident_tombstones_since(q.since, &q.since_node_id, &q.since_id, limit))
        .await
    {
        Ok(rows) => Json(ReplicationBatch {
            origin: coord.inner.node_id.clone(),
            deleted_incidents: rows,
            ..Default::default()
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// Query for `GET /internal/backfill`.
#[derive(Debug, Deserialize)]
pub(in crate::coordinator) struct BackfillQuery {
    check_id: String,
    since: i64,
    since_agent_id: Option<String>,
    until: i64,
    limit: usize,
}

/// Query for `GET /internal/backfill/results`. Empty composite cursor fields
/// denote the beginning, allowing callers to use a stable query shape.
#[derive(Debug, Deserialize)]
pub(in crate::coordinator) struct ResultsBackfillQuery {
    since: i64,
    since_check_id: Option<String>,
    since_agent_id: Option<String>,
    limit: usize,
}

/// `GET /internal/backfill/results`: paged raw rows across all checks. The
/// composite `(timestamp, check_id, agent_id)` cursor is exclusive and fully
/// ordered, which avoids one request per divergent check during anti-entropy.
pub(in crate::coordinator) async fn results_backfill_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<ResultsBackfillQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    let since_check_id = match q.since_check_id.as_deref().filter(|id| !id.is_empty()) {
        Some(id) => match id.parse() {
            Ok(id) => Some(id),
            Err(_) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "since_check_id must be a valid UUID",
                )
            }
        },
        None => None,
    };
    let since_agent_id = q.since_agent_id.filter(|id| !id.is_empty());
    let limit = q.limit.min(super::super::replication::ANTI_ENTROPY_PAGE);
    match coord
        .db(move |storage| {
            storage.results_after(q.since, since_check_id, since_agent_id.as_deref(), limit)
        })
        .await
    {
        Ok(rows) => Json(ReplicationBatch {
            origin: coord.inner.node_id.clone(),
            results: rows
                .into_iter()
                .map(|(result, status)| ReplicatedResult { result, status })
                .collect(),
            ..Default::default()
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// Query for `GET /internal/backfill/latency-minute` and `-hour`. Same
/// composite-cursor shape as `ResultsBackfillQuery`, cursored on
/// `(bucket_start, check_id, agent_id)` instead of `(ts_millis, check_id,
/// agent_id)`.
#[derive(Debug, Deserialize)]
pub(in crate::coordinator) struct RollupBackfillQuery {
    since: i64,
    since_check_id: Option<String>,
    since_agent_id: Option<String>,
    limit: usize,
}

/// Shared by `latency_minute_backfill_handler` and
/// `latency_hour_backfill_handler`: parse the composite cursor, bound the
/// page, and load through `fetch`. The two differ only in which storage
/// method they pass as `fetch`.
async fn rollup_backfill(
    coord: &Coordinator,
    q: RollupBackfillQuery,
    fetch: impl FnOnce(
            &storage::Storage,
            i64,
            Option<crate::models::CheckId>,
            Option<&str>,
            usize,
        ) -> anyhow::Result<Vec<storage::RollupRow>>
        + Send
        + 'static,
) -> Response {
    let since_check_id = match q.since_check_id.as_deref().filter(|id| !id.is_empty()) {
        Some(id) => match id.parse() {
            Ok(id) => Some(id),
            Err(_) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "since_check_id must be a valid UUID",
                )
            }
        },
        None => None,
    };
    let since_agent_id = q.since_agent_id.filter(|id| !id.is_empty());
    let limit = q.limit.min(super::super::replication::ANTI_ENTROPY_PAGE);
    match coord
        .db(move |storage| {
            fetch(
                storage,
                q.since,
                since_check_id,
                since_agent_id.as_deref(),
                limit,
            )
        })
        .await
    {
        Ok(rows) => Json(RollupBatch {
            origin: coord.inner.node_id.clone(),
            rows: rows.iter().map(RollupRowDto::from).collect(),
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// `GET /internal/backfill/latency-minute`: paged minute-rollup rows across
/// all checks, the pull half of item A's rollup anti-entropy — see
/// `storage::RollupDigest` for why rollups need their own stream instead of
/// riding along with raw-result backfill.
pub(in crate::coordinator) async fn latency_minute_backfill_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<RollupBackfillQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    rollup_backfill(&coord, q, |s, since, id, agent, limit| {
        s.minute_rollups_since(since, id, agent, limit)
    })
    .await
}

/// `GET /internal/backfill/latency-hour`: the hour-rollup counterpart to
/// `latency_minute_backfill_handler`.
pub(in crate::coordinator) async fn latency_hour_backfill_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<RollupBackfillQuery>,
) -> Response {
    if let Some(denied) = require_peer(&coord, &headers) {
        return denied;
    }
    rollup_backfill(&coord, q, |s, since, id, agent, limit| {
        s.hour_rollups_since(since, id, agent, limit)
    })
    .await
}

/// `GET /internal/backfill`: paged raw rows for one check, with the composite
/// `(since, since_agent_id)` cursor exclusive — the pull counterpart to
/// `/internal/replicate`'s push, and the same
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
    let check_id = match q.check_id.parse() {
        Ok(id) => id,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "check_id must be a valid UUID"),
    };
    let since = q.since;
    let since_agent_id = q.since_agent_id;
    let until = q.until;
    let limit = q.limit.min(super::super::replication::ANTI_ENTROPY_PAGE);
    let rows = coord
        .db(move |storage| {
            storage.results_since(check_id, since, since_agent_id.as_deref(), until, limit)
        })
        .await;
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
    cluster.answer_ping(&body, now)
}

/// `GET /api/v1/cluster`: this node's own membership view, for the admin UI.
#[derive(Serialize)]
struct ClusterViewResponse {
    #[serde(rename = "self")]
    me: cluster::Member,
    members: Vec<cluster::Member>,
    now: i64,
    mode: &'static str,
    links: Vec<cluster::LinkObservation>,
    link_stale_after_ms: i64,
}

pub(in crate::coordinator) async fn cluster_view_handler(
    State(coord): State<Coordinator>,
    _admin: AdminSession,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    // A node with clustering unconfigured still has an identity worth
    // showing, just no membership beyond itself.
    let (me, members, links, link_stale_after_ms) = match coord.inner.cluster.as_ref() {
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
            (
                cluster.self_member(now),
                cluster.snapshot(),
                cluster.link_snapshot(),
                cluster.link_stale_after_ms(),
            )
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
            Vec::new(),
            0,
        ),
    };
    Json(ClusterViewResponse {
        me,
        members,
        now,
        mode: "coordinator",
        links,
        link_stale_after_ms,
    })
    .into_response()
}

/// `DELETE /api/v1/admin/cluster/members/:node_id`: decommission a member.
pub(in crate::coordinator) async fn decommission_member_handler(
    State(coord): State<Coordinator>,
    _admin: AdminSession,
    axum::extract::Path(node_id): axum::extract::Path<String>,
) -> Response {
    cluster::Cluster::decommission(coord.inner.cluster.as_deref(), &node_id)
}

/// One standing raw-data gap, on the wire. Mirrors `storage::RawDataGapRow`.
#[derive(Debug, Serialize)]
pub struct RawDataGapDto {
    pub check_id: String,
    pub peer: String,
    pub from_ts: i64,
    pub to_ts: i64,
    pub observed_at: i64,
}

impl From<&storage::RawDataGapRow> for RawDataGapDto {
    fn from(g: &storage::RawDataGapRow) -> Self {
        Self {
            check_id: g.check_id.to_string(),
            peer: g.peer.clone(),
            from_ts: g.from_ts,
            to_ts: g.to_ts,
            observed_at: g.observed_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(in crate::coordinator) struct RawDataGapsQuery {
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub(in crate::coordinator) struct RawDataGapsResponse {
    gaps: Vec<RawDataGapDto>,
    total: usize,
    has_more: bool,
}

/// `GET /api/v1/admin/raw-data-gaps`: every check with a standing hole in its
/// raw history that ordinary anti-entropy cannot close (`anti_entropy_sweep`
/// writes these; see `storage::RawDataGapRow`), for an operator deciding
/// whether a gap needs a snapshot restore or is small enough to accept.
///
/// Admin-gated like `cluster_view_handler`, not peer-gated: this reports what
/// *this* node has already recorded about *other* nodes falling behind, it
/// does not ask a peer for anything, so there is nothing here for a peer
/// credential to authenticate.
///
/// Once minute/hour rollups replicate on their own (item A), a row here means
/// specifically that raw probe-level detail is gone for that window — the
/// summarized latency history for it can still recover through rollup
/// anti-entropy. See SCALING.md's "Data retention tiers".
pub(in crate::coordinator) async fn raw_data_gaps_handler(
    State(coord): State<Coordinator>,
    _admin: AdminSession,
    Query(q): Query<RawDataGapsQuery>,
) -> Response {
    let limit = crate::page::bounded(q.limit, 100);
    let page = q.page.unwrap_or(0);
    match coord.db(|storage| storage.list_raw_data_gaps()).await {
        Ok(mut gaps) => {
            let (total, has_more) = crate::page::take(&mut gaps, page, limit);
            Json(RawDataGapsResponse {
                gaps: gaps.iter().map(RawDataGapDto::from).collect(),
                total,
                has_more,
            })
            .into_response()
        }
        Err(e) => internal_error(e),
    }
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
