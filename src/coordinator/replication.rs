use super::ingest::{check_meta, meta_by_id};
use super::*;
use crate::configplane::ConfigBatch;
use crate::models::{CheckResult, Status};
use rand::seq::SliceRandom;
use routes::peer::{read_config_entry_for_replication, RollupBatch, RollupRowDto};
use std::time::Duration;

/// Stay well below `/internal/config`'s 8 MiB body limit so JSON framing and a
/// future field cannot turn a bulk edit into a rejected replication request.
const MAX_CONFIG_BATCH_BYTES: usize = 6 * 1024 * 1024;
const CONFIG_APPLY_CHUNK: usize = 1_000;

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
pub(in crate::coordinator) struct RepairCursors {
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

/// Rows per `/internal/backfill` page during an anti-entropy sweep. Bounded
/// so one very-behind check cannot hold `apply_replicated` — and the yield
/// between pages — for an unbounded stretch.
pub(in crate::coordinator) const ANTI_ENTROPY_PAGE: usize = 500;
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
/// Budget for one anti-entropy request. Generous next to the control plane's
/// five seconds because a backfill page is applied inside one transaction on
/// the receiver, behind its single database worker — but finite, because the
/// sweep is a single periodic task and a peer that never answers would
/// otherwise end anti-entropy for the life of the process.
const ANTI_ENTROPY_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl Coordinator {
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
    pub(in crate::coordinator) async fn save_agents_display(
        &self,
        next: crate::config::AgentsDisplay,
    ) -> Result<()> {
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
    pub(in crate::coordinator) async fn save_notifiers(
        &self,
        next: Vec<crate::notify::Notifier>,
    ) -> Result<()> {
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
    pub(in crate::coordinator) async fn save_maintenance(
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

    /// Persist a credential, make it live immediately, and push it to every
    /// peer so an operator mints once for the fleet rather than once per
    /// coordinator.
    pub(in crate::coordinator) async fn store_credential(
        &self,
        record: &AgentCredential,
    ) -> Result<()> {
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
    pub(in crate::coordinator) async fn reload_credentials(&self) -> Result<()> {
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
    pub(in crate::coordinator) async fn reload_checks(&self) -> Result<Vec<CheckConfig>> {
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
    pub(in crate::coordinator) async fn apply_config_batch(
        &self,
        entries: &[storage::ConfigEntry],
    ) -> Result<()> {
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
    pub(in crate::coordinator) async fn write_config(
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
    pub(in crate::coordinator) fn replicate_config(&self, entries: Vec<storage::ConfigEntry>) {
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

    pub(in crate::coordinator) fn config_batches(
        entries: Vec<storage::ConfigEntry>,
    ) -> Vec<Vec<storage::ConfigEntry>> {
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
    pub(in crate::coordinator) async fn restamp_and_push(
        &self,
        kind: storage::ConfigKind,
        id: &str,
    ) {
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
    pub(in crate::coordinator) fn replicate_incident_deletions(&self, ids: Vec<String>) {
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

    /// Apply a peer's replication batch: incidents, then alerts, then raw
    /// results, then this node's own confirmed-state view of every check the
    /// batch touched. Mirrors what local `ingest` writes to storage and
    /// nothing else it does — no `eval`, no `notify_tx`, no re-broadcast to
    /// the cluster or to `self.inner.gossip` — because the sender already
    /// decided all of that and owns paging for it.
    pub(in crate::coordinator) async fn apply_replicated(
        &self,
        batch: ReplicationBatch,
    ) -> anyhow::Result<()> {
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
        // Bounded for the same reason the repair budget is bounded: a peer
        // that accepts the connection and never answers must cost this sweep a
        // request, not the sweep itself. An unbounded client here stops
        // anti-entropy permanently and silently, since the periodic task owns
        // exactly one sweep at a time.
        let client = match reqwest::Client::builder()
            .timeout(ANTI_ENTROPY_REQUEST_TIMEOUT)
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(%error, "anti-entropy: failed to build HTTP client");
                self.inner
                    .replication_anti_entropy_failures
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let Some(their) = self.fetch_digest(&client, &peer).await else {
            // `fetch_digest` already logged the specific cause (rejected or
            // unreachable); this is the one place that turns "a sweep found
            // nobody to compare against" into a signal an alert can fire on.
            self.inner
                .replication_anti_entropy_failures
                .fetch_add(1, Ordering::Relaxed);
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
                self.inner
                    .replication_anti_entropy_failures
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        // Both digests are in hand and the sweep is about to compare and
        // reconcile: "success" for reconciliation-lag purposes, independent
        // of whether an individual page below hits its own transient error —
        // those are already `replication_raw_data_gaps` or a logged warning,
        // not a sign this sweep never ran.
        self.inner
            .replication_anti_entropy_last_success_ms
            .store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CoordinatorConfig;

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
}
