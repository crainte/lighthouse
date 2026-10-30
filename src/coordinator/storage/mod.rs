//! Storage split into domain modules. `mod.rs` holds the `Storage` struct,
//! `open`, cross-domain orchestration (`prune_metadata`, `seed_mock`,
//! `checkpoint`), and helpers/types genuinely shared by more than one
//! domain file. Everything below was reachable as `crate::coordinator::
//! storage::X` before the split and must remain so — see the `pub use`
//! re-exports below.

use crate::config::CheckConfig;
use crate::coordinator::hlc::Hlc;
use crate::credential::AgentCredential;
use crate::models::{CheckId, CheckKind, CheckResult, NodeState, Status};
use anyhow::{bail, Result};
use duckdb::{Connection, OptionalExt, Statement};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Prepared statements this connection keeps compiled. DuckDB 2.0 made
/// parse/bind/plan/optimise several times dearer while execution got cheaper,
/// so for the short per-row and per-request statements that dominate here the
/// compile step is most of the cost. Sized above the number of distinct static
/// statements `CachedSql` callers use, so the LRU never evicts a hot one.
const STATEMENT_CACHE_CAPACITY: usize = 512;

/// `Connection::execute`/`query_row` that reuse a compiled statement from the
/// connection's cache instead of re-preparing `sql` on every call. Only for
/// static SQL: dynamic (`format!`-built) text would churn the cache. Same
/// signatures and error type as the uncached methods, so call sites keep
/// their `?` and `.optional()` handling unchanged.
pub(crate) trait CachedSql {
    fn execute_cached<P: duckdb::Params>(&self, sql: &str, params: P) -> duckdb::Result<usize>;

    fn query_row_cached<T, P, F>(&self, sql: &str, params: P, f: F) -> duckdb::Result<T>
    where
        P: duckdb::Params,
        F: FnOnce(&duckdb::Row<'_>) -> duckdb::Result<T>;
}

impl CachedSql for Connection {
    fn execute_cached<P: duckdb::Params>(&self, sql: &str, params: P) -> duckdb::Result<usize> {
        self.prepare_cached(sql)?.execute(params)
    }

    fn query_row_cached<T, P, F>(&self, sql: &str, params: P, f: F) -> duckdb::Result<T>
    where
        P: duckdb::Params,
        F: FnOnce(&duckdb::Row<'_>) -> duckdb::Result<T>,
    {
        self.prepare_cached(sql)?.query_row(params, f)
    }
}

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
    transaction_depth: parking_lot::Mutex<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamDigest {
    pub max_millis: i64,
    pub max_id: String,
    pub count: i64,
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
         UPDATE {table} r SET
             attempts = r.attempts + b.attempts,
             up = r.up + b.up,
             latency_sum = r.latency_sum + b.latency_sum,
             latency_min = LEAST(r.latency_min, b.latency_min),
             latency_max = GREATEST(r.latency_max, b.latency_max)
         FROM ({bucketed}) b
         WHERE r.check_id = b.check_id AND r.agent_id = b.agent_id
           AND r.bucket_start = b.bucket_start;
         INSERT INTO {table}
         SELECT b.* FROM ({bucketed}) b
         ANTI JOIN {table} r
           ON r.check_id = b.check_id AND r.agent_id = b.agent_id
          AND r.bucket_start = b.bucket_start;"
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
///
/// `staged` is collapsed to one row per key first, with the same field-wise
/// rules, so a page that repeats a key (never sent by a correct peer, but not
/// something to trust) merges deterministically rather than matching one
/// target row twice.
fn rollup_merge_from_backfill_sql(table: &str, stream: &str, staged: &str) -> String {
    let staged = format!(
        "(SELECT check_id, agent_id, bucket_start, MAX(attempts) AS attempts, MAX(up) AS up,
                 MAX(latency_sum) AS latency_sum, MIN(latency_min) AS latency_min,
                 MAX(latency_max) AS latency_max
          FROM {staged} GROUP BY check_id, agent_id, bucket_start)"
    );
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
         UPDATE {table} r SET
             attempts = GREATEST(r.attempts, s.attempts),
             up = GREATEST(r.up, s.up),
             latency_sum = GREATEST(r.latency_sum, s.latency_sum),
             latency_min = LEAST(r.latency_min, s.latency_min),
             latency_max = GREATEST(r.latency_max, s.latency_max)
         FROM {staged} s
         WHERE r.check_id = s.check_id AND r.agent_id = s.agent_id
           AND r.bucket_start = s.bucket_start;
         INSERT INTO {table}
         SELECT s.* FROM {staged} s
         ANTI JOIN {table} r
           ON r.check_id = s.check_id AND r.agent_id = s.agent_id
          AND r.bucket_start = s.bucket_start;"
    )
}

mod alerts;
mod checks;
mod config_replication;
mod fleet;
mod incidents;
mod results;
mod rollups;
mod schema;
mod status;
#[cfg(test)]
mod test_support;

pub use alerts::*;
pub use checks::*;
pub use config_replication::*;
pub use fleet::*;
pub use incidents::*;
pub use results::*;
pub use rollups::*;
use schema::{drop_rollup_primary_keys, verify_schema, SCHEMA_DDL};
pub use status::*;

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
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        conn.execute_batch(SCHEMA_DDL)?;
        let rebuilt_rollups = if path != ":memory:" {
            verify_schema(&conn)?;
            drop_rollup_primary_keys(&conn)?
        } else {
            0
        };
        let store = Self {
            conn,
            incident_node_id: parking_lot::RwLock::new("local".to_string()),
            transaction_depth: parking_lot::Mutex::new(0),
        };
        // Self-heals `replication_stream_watermarks` after a crash between an
        // alerts/incidents write and its incremental watermark update in
        // `record_stream_item` — the two are separate statements, not one
        // transaction. Cheap: the streams it covers (alerts, incidents,
        // incident_tombstones) are retention-bounded, unlike `check_results`.
        store.rebuild_stream_watermarks()?;
        if rebuilt_rollups > 0 {
            // Old databases can predate incremental rollup watermarks, and a
            // keyed-table rebuild preserves rows rather than synthesising
            // their digest. Recompute after both physical copies complete so
            // anti-entropy never sees zero counters for non-empty rollups.
            store.rebuild_rollup_watermarks()?;
        }
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
        store.prepare_config_watermarks()?;
        store.prepare_status()?;
        Ok(store)
    }

    /// Apply DuckDB resource bounds and return what DuckDB reports back, so
    /// the startup log shows the effective values rather than the request.
    pub fn apply_limits(
        &self,
        limits: &crate::coordinator::db_limits::DatabaseLimits,
    ) -> Result<crate::coordinator::db_limits::DatabaseLimits> {
        self.conn.execute_batch(&format!(
            "SET memory_limit = '{}'; SET threads = {};",
            limits.memory_limit.replace('\'', ""),
            limits.threads
        ))?;
        Ok(self.conn.query_row_cached(
            "SELECT current_setting('memory_limit'), current_setting('threads')::UINTEGER",
            [],
            |row| {
                Ok(crate::coordinator::db_limits::DatabaseLimits {
                    memory_limit: row.get(0)?,
                    threads: row.get(1)?,
                })
            },
        )?)
    }

    fn rebuild_stream_watermarks(&self) -> Result<()> {
        self.conn.execute_batch(
            "INSERT INTO replication_stream_watermarks
             SELECT 'alerts', COALESCE(MAX(ts_millis), 0), COALESCE(MAX_BY(id, (ts_millis, id)), ''), COUNT(*) FROM alerts
             UNION ALL
             SELECT 'incidents', COALESCE(MAX(version_millis), 0),
                    COALESCE(MAX_BY(version_node || chr(0) || id, (version_millis, version_node, id)), ''),
                    COUNT(*) FROM incidents
             UNION ALL
             SELECT 'incident_tombstones', COALESCE(MAX(deleted_at), 0),
                    COALESCE(MAX_BY(node_id || chr(0) || id, (deleted_at, node_id, id)), ''),
                    COUNT(*) FROM incident_tombstones
             ON CONFLICT (stream) DO UPDATE SET
                 max_millis = excluded.max_millis, max_id = excluded.max_id,
                 item_count = excluded.item_count;",
        )?;
        Ok(())
    }

    fn stream_digest(&self, stream: &str) -> Result<StreamDigest> {
        self.conn.query_row_cached(
            "SELECT max_millis, max_id, item_count FROM replication_stream_watermarks WHERE stream = ?",
            duckdb::params![stream],
            |row| Ok(StreamDigest { max_millis: row.get(0)?, max_id: row.get(1)?, count: row.get(2)? }),
        ).map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn execute_test_sql(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)?;
        Ok(())
    }

    pub(crate) fn transaction<T>(&self, operation: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        let outermost = {
            let mut depth = self.transaction_depth.lock();
            if *depth > 0 {
                false
            } else {
                *depth = 1;
                true
            }
        };
        if !outermost {
            return operation(self);
        }
        if let Err(error) = self.conn.execute_batch("BEGIN") {
            *self.transaction_depth.lock() = 0;
            return Err(error.into());
        }
        let result = operation(self);
        *self.transaction_depth.lock() = 0;
        match result {
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

    /// Return an unused stable local event id. The first event keeps the
    /// readable base; same-millisecond events receive a deterministic suffix.
    /// IDs are persisted and replicated as written, so replication retains the
    /// exact event identity rather than generating another one.
    fn record_stream_item(&self, stream: &str, millis: i64, id: &str, delta: i64) -> Result<()> {
        self.conn.execute_cached(
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
        removed += self.conn.execute_cached(
            "DELETE FROM alerts WHERE ts_millis < ?",
            duckdb::params![alerts_before],
        )? as u64;
        // Closed incidents take their narrative and their affects rows with
        // them: DuckDB has no ON DELETE CASCADE, so leaving these behind would
        // accumulate updates pointing at incidents that no longer exist.
        removed += self.conn.execute_cached(
            "DELETE FROM incident_updates WHERE incident_id IN
                 (SELECT id FROM incidents WHERE state = 'resolved' AND resolved_at < ?)",
            duckdb::params![alerts_before],
        )? as u64;
        removed += self.conn.execute_cached(
            "DELETE FROM incident_checks WHERE incident_id IN
                 (SELECT id FROM incidents WHERE state = 'resolved' AND resolved_at < ?)",
            duckdb::params![alerts_before],
        )? as u64;
        removed += self.conn.execute_cached(
            "DELETE FROM incidents WHERE state = 'resolved' AND resolved_at < ?",
            duckdb::params![alerts_before],
        )? as u64;
        // Given-up notifications are history of the same kind as an alert, so
        // they age out on the same window. Rows still waiting are current work
        // and are never pruned — dropping one silently un-pages an outage.
        removed += self.conn.execute_cached(
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
        removed += self.conn.execute_cached(
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
        removed += self.prune_tombstones_uncommitted(tombstone_before)? as u64;
        removed += self.prune_incident_tombstones(tombstone_before)? as u64;
        // A gap not redetected in an entire alert window is either fixed (a
        // snapshot restore, an extended catch-up store) or the peer that
        // reported it is gone; either way it stops being operationally
        // useful to keep past the horizon everything else here ages out on.
        removed += self.conn.execute_cached(
            "DELETE FROM raw_data_gaps WHERE observed_at < ?",
            duckdb::params![alerts_before],
        )? as u64;
        self.rebuild_stream_watermarks()?;
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

    /// Seed synthetic data spanning ~90 days before `now_millis` when the store
    /// is empty. Idempotent: a no-op when check_results already has rows.
    /// Returns true if seeding occurred.
    pub fn seed_mock(&self, now_millis: i64) -> Result<bool> {
        let count: i64 =
            self.conn
                .query_row_cached("SELECT count(*) FROM check_results", [], |r| r.get(0))?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
