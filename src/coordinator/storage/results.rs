use super::*;

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
    pub check_id: CheckId,
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
    pub check_id: Option<CheckId>,
    pub agent_id: Option<String>,
    pub status: Option<Status>,
    pub region: Option<String>,
    pub zone: Option<String>,
}

/// Current status of a check plus recent uptime, for the status grid.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusRow {
    pub check_id: CheckId,
    /// The coordinator's published conclusion, which may be Degraded.
    pub state: NodeState,
    pub latency_ms: u64,
    pub last_check: i64, // unix millis of latest result
    /// Time-weighted fraction 0.0..=1.0 of the 24h before now spent Up,
    /// computed from `state_history` spans rather than from raw result rows.
    pub uptime_24h: f64,
    /// Status code from the most recent result.
    pub status_code: Option<u16>,
    /// Failure reason from the most recent result, so the dashboard can say why
    /// a check is down rather than only that it is. Never publish it verbatim.
    pub error: Option<String>,
    /// That same failure as a category, safe to publish.
    pub failure: Option<crate::models::FailureKind>,
}

/// The coordinator's PUBLISHED decision per check, materialized so the status
/// page and a restarted process agree with the in-memory tracker.
///
/// `consecutive_failures` no longer lives here: failures are counted per agent
/// now, in `agent_check_state`. The column survives in the table for older
/// readers but the coordinator no longer means anything by it.
#[derive(Debug, Clone, PartialEq)]
pub struct CurrentStateRow {
    pub check_id: CheckId,
    pub state: NodeState,
    /// The state the hold-down timer is arguing for, and when it first said so.
    /// `None` when the rollup agrees with what is published.
    pub pending: Option<(NodeState, i64)>,
}

/// One agent's confirmed opinion of one check — a leaf of the region tree.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentCheckStateRow {
    pub check_id: CheckId,
    pub agent_id: String,
    /// Auth-stamped leaf topology, persisted so restart rollups use the same
    /// zone/region membership as the observation that established the belief.
    pub zone: String,
    pub region: String,
    pub status: Status,
    pub consecutive_failures: u32,
    pub last_check: i64,
}

/// What the coordinator publishes for one check after a rollup: the state,
/// any hold-down timer in flight, and when it was last observed.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedStateRow {
    pub check_id: CheckId,
    pub state: NodeState,
    pub pending: Option<(NodeState, i64)>,
    pub last_check: i64,
}

/// One span of published state for a check: it entered `state` at `since` and
/// stayed there until the next row's `since` (or until now, for the last one).
#[derive(Debug, Clone, PartialEq)]
pub struct StateSpan {
    pub state: NodeState,
    pub since: i64,
}

/// One segment of the uptime strip. Not necessarily a day: the window is split
/// into a fixed number of segments, so a 7-day view has ~2-hour segments.
/// `up` and `total` count probe results; uptime itself remains time-weighted.
#[derive(Debug, Clone, PartialEq)]
pub struct UptimeBucket {
    /// Exclusive lower bound of the segment, unix millis.
    pub start: i64,
    /// Inclusive upper bound, unix millis. Handed to the UI so it can query the
    /// exact window a bar covers without re-deriving the segment width.
    pub end: i64,
    /// Fraction 0.0..=1.0, or None when the segment has no data at all.
    pub uptime: Option<f64>,
    /// Successful probe attempts in this segment.
    pub up: u64,
    /// All probe attempts in this segment.
    pub total: u64,
}

/// A single latency data point for the sparkline chart.
/// One check's uptime strip plus its exact figure for the whole window. Paired
/// because every caller that draws the strip also prints the number, and
/// deriving the number from the rounded segments would disagree with it.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckUptime {
    pub buckets: Vec<UptimeBucket>,
    pub overall: Option<f64>,
}

/// The segment grid an uptime strip is drawn on. Shared by the single-check and
/// batched paths so their segment edges cannot drift apart: a strip fetched for
/// one check must tile identically to the same check's strip on the status page.
struct BucketGrid {
    lower: i64,
    bucket_ms: i64,
    buckets: usize,
}

impl BucketGrid {
    fn new(now_millis: i64, days: usize, buckets: usize) -> Self {
        const DAY: i64 = 86_400_000;
        let buckets = buckets.max(1);
        let bucket_ms = ((days as i64 * DAY) / buckets as i64).max(1);
        // Recompute the span from the bucket width so segment edges tile exactly:
        // integer division would otherwise leave a sliver unaccounted for.
        Self {
            lower: now_millis - bucket_ms * buckets as i64,
            bucket_ms,
            buckets,
        }
    }

    /// Lower bound is exclusive, so shift before dividing: a timestamp landing
    /// exactly on a boundary belongs to the segment below it.
    fn index(&self, ts: i64) -> usize {
        (((ts - self.lower - 1).max(0) / self.bucket_ms) as usize).min(self.buckets - 1)
    }
}

/// An empty location is the same as none: an agent that sent "" told us nothing,
/// and storing it would create a second spelling of unknown.
fn non_empty(s: &str) -> Option<&str> {
    (!s.trim().is_empty()).then_some(s)
}

/// Return known UP and total duration for the half-open interval `[from, to)`.
/// `Unknown` is deliberately no-data, while Degraded follows the check's
/// already-resolved SLA setting.
pub(super) fn uptime_counts(
    spans: &[StateSpan],
    from: i64,
    to: i64,
    degraded_is_downtime: bool,
) -> Option<(u64, u64)> {
    if spans.is_empty() || to <= from {
        return None;
    }

    let mut up_ms = 0u64;
    let mut known_ms = 0u64;
    for (i, span) in spans.iter().enumerate() {
        let span_end = spans.get(i + 1).map(|next| next.since).unwrap_or(i64::MAX);
        let start = span.since.max(from);
        let end = span_end.min(to);
        if end <= start || span.state == NodeState::Unknown {
            continue;
        }
        let duration = (end - start) as u64;
        known_ms = known_ms.saturating_add(duration);
        if span.state == NodeState::Up
            || (span.state == NodeState::Degraded && !degraded_is_downtime)
        {
            up_ms = up_ms.saturating_add(duration);
        }
    }
    (known_ms > 0).then_some((up_ms, known_ms))
}

impl Storage {
    pub(super) fn rebuild_result_watermarks(&self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM result_replication_watermarks w
                 WHERE NOT EXISTS (SELECT 1 FROM check_results r WHERE r.check_id = w.check_id);
             INSERT INTO result_replication_watermarks
                 SELECT check_id, MIN(ts_millis), MAX(ts_millis), COUNT(*)
                 FROM check_results GROUP BY check_id
             ON CONFLICT (check_id) DO UPDATE SET
                 min_ts = excluded.min_ts, max_ts = excluded.max_ts,
                 result_count = excluded.result_count;",
        )?;
        Ok(())
    }

    /// Recompute `latest_check_results` from raw `check_results`. Not called
    /// on the ordinary `open` path — incremental maintenance in
    /// `derived_results_sql` keeps it current on every write, and `purge_agent`
    /// calls this directly after deleting an agent's rows — but available for
    /// the same bulk-load scenarios as `rebuild_latency_rollups`.
    pub(super) fn rebuild_latest_check_results(&self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM latest_check_results l
                 WHERE NOT EXISTS (SELECT 1 FROM check_results r WHERE r.check_id = l.check_id);
             INSERT INTO latest_check_results
                 (check_id, agent_id, ts_millis, status, latency_ms, status_code, error, failure)
             SELECT check_id, agent_id, ts_millis, status, latency_ms, status_code, error, failure
             FROM check_results
             QUALIFY row_number() OVER (
                 PARTITION BY check_id ORDER BY ts_millis DESC, agent_id DESC
             ) = 1
             ON CONFLICT (check_id) DO UPDATE SET
                 agent_id = excluded.agent_id, ts_millis = excluded.ts_millis,
                 status = excluded.status, latency_ms = excluded.latency_ms,
                 status_code = excluded.status_code, error = excluded.error,
                 failure = excluded.failure;",
        )?;
        Ok(())
    }

    pub fn persist_ingest(
        &self,
        rows: &[(CheckResult, Status)],
        agent_states: &[AgentCheckStateRow],
        published_states: &[PublishedStateRow],
    ) -> Result<()> {
        self.prepare_ingest()?;
        self.transaction(|store| {
            store.persist_ingest_uncommitted(rows, agent_states, published_states)
        })
    }

    pub(crate) fn prepare_ingest(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS ingest_results_stage AS SELECT * FROM check_results WITH NO DATA;
             CREATE TEMP TABLE IF NOT EXISTS ingest_agent_stage (
                 ordinal BIGINT, check_id UUID, agent_id VARCHAR, status VARCHAR,
                 consecutive_failures INTEGER, zone VARCHAR, region VARCHAR, last_check BIGINT
             );
             CREATE TEMP TABLE IF NOT EXISTS ingest_state_stage (
                 check_id UUID, status VARCHAR, pending_state VARCHAR,
                 pending_since BIGINT, last_check BIGINT
              );
             CREATE TEMP TABLE IF NOT EXISTS replicate_new_results AS SELECT * FROM check_results WITH NO DATA;
             CREATE TEMP TABLE IF NOT EXISTS replicate_probe_stage AS SELECT * FROM check_results WITH NO DATA;
             CREATE TEMP TABLE IF NOT EXISTS prune_results_counts (check_id UUID, removed BIGINT);
             CREATE TEMP TABLE IF NOT EXISTS prune_results_min_ts (check_id UUID, min_ts BIGINT);",

        )?;
        Ok(())
    }

    pub(crate) fn persist_ingest_uncommitted(
        &self,
        rows: &[(CheckResult, Status)],
        agent_states: &[AgentCheckStateRow],
        published_states: &[PublishedStateRow],
    ) -> Result<()> {
        self.conn.execute_batch("DELETE FROM ingest_results_stage; DELETE FROM ingest_agent_stage; DELETE FROM ingest_state_stage;")?;
        self.stage_results(rows)?;
        self.stage_agent_states(agent_states)?;
        {
            let mut appender = self.conn.appender("ingest_state_stage")?;
            for state in published_states {
                appender.append_row(duckdb::params![
                    state.check_id,
                    state.state.as_str(),
                    state.pending.map(|(pending, _)| pending.as_str()),
                    state.pending.map(|(_, since)| since),
                    state.last_check,
                ])?;
            }
            appender.flush()?;
        }
        self.conn.execute_batch(&format!(
            "INSERT INTO check_results SELECT * FROM ingest_results_stage;
             {results}
             {agents}
             INSERT INTO current_state
                 (check_id, status, last_check, consecutive_failures, pending_state, pending_since)
             SELECT check_id, status, last_check, 0, pending_state, pending_since
             FROM ingest_state_stage
             ON CONFLICT (check_id) DO UPDATE SET
                 status = excluded.status,
                 last_check = excluded.last_check,
                 consecutive_failures = 0,
                 pending_state = excluded.pending_state,
                 pending_since = excluded.pending_since;
             INSERT INTO state_history
             SELECT staged.check_id, staged.status, staged.last_check
             FROM ingest_state_stage staged
             LEFT JOIN LATERAL (
                 SELECT state FROM state_history history
                 WHERE history.check_id = staged.check_id
                 ORDER BY since DESC LIMIT 1
             ) newest ON true
             WHERE newest.state IS NULL OR newest.state <> staged.status
             ON CONFLICT DO NOTHING;",
            results = derived_results_sql("ingest_results_stage"),
            agents = AGENT_STATE_MERGE_SQL,
        ))?;
        Ok(())
    }

    /// Fill `ingest_results_stage` from a batch. Shared by local ingest and
    /// replication: both write the same rows, and an appender is an order of
    /// magnitude cheaper than a prepared insert per row.
    pub(super) fn stage_results(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        let mut appender = self.conn.appender("ingest_results_stage")?;
        for (result, status) in rows {
            appender.append_row(duckdb::params![
                result.check_id,
                result.agent_id,
                result.timestamp.timestamp_millis(),
                status.as_str(),
                result.latency_ms as i64,
                result.status_code.map(|code| code as i32),
                result.error,
                non_empty(&result.zone),
                non_empty(&result.region),
                result
                    .failure
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
            ])?;
        }
        appender.flush()?;
        Ok(())
    }

    /// Fill `ingest_agent_stage`. `ordinal` preserves batch order so the merge
    /// can break `last_check` ties on arrival order rather than on scan order.
    pub(super) fn stage_agent_states(&self, states: &[AgentCheckStateRow]) -> Result<()> {
        let mut appender = self.conn.appender("ingest_agent_stage")?;
        for (ordinal, state) in states.iter().enumerate() {
            appender.append_row(duckdb::params![
                ordinal as i64,
                state.check_id,
                state.agent_id,
                state.status.as_str(),
                state.consecutive_failures as i32,
                state.zone,
                state.region,
                state.last_check,
            ])?;
        }
        appender.flush()?;
        Ok(())
    }

    /// Merge a batch of per-agent beliefs in one statement. The replication
    /// path used to run one upsert per belief, which on a coalesced batch of a
    /// few thousand checks held the single database worker for seconds and
    /// starved local ingest behind it.
    pub(crate) fn update_agent_check_states_uncommitted(
        &self,
        states: &[AgentCheckStateRow],
    ) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("DELETE FROM ingest_agent_stage;")?;
        self.stage_agent_states(states)?;
        self.conn.execute_batch(AGENT_STATE_MERGE_SQL)?;
        Ok(())
    }

    /// Persist a batch of raw results with their evaluated status.
    pub fn insert_results(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        // One transaction for the whole batch: DuckDB otherwise commits per
        // statement, which makes large seeds (100k+ rows) crawl.
        self.transaction(|_| {
            let insert = || -> Result<()> {
                let mut stmt = self.conn.prepare_cached(
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
                self.conn
                    .execute_batch("DELETE FROM ingest_results_stage;")?;
                self.stage_results(rows)?;
                self.conn
                    .execute_batch(&derived_results_sql("ingest_results_stage"))?;
                Ok(())
            };
            insert()
        })
    }

    /// Same rows as `insert_results`, loaded through DuckDB's Appender rather
    /// than a prepared statement executed once per row. For a real ingest
    /// batch the two cost about the same; for a many-thousand-row bulk load
    /// — `seed_mock`'s reason for existing — the Appender is the difference
    /// between seconds and tens of seconds, which is why it exists as its own
    /// method instead of replacing `insert_results` outright.
    pub(super) fn append_results_bulk(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        let mut appender = self.conn.appender_with_columns(
            "check_results",
            &[
                "check_id",
                "agent_id",
                "ts_millis",
                "status",
                "latency_ms",
                "status_code",
                "error",
                "failure",
                "zone",
                "region",
            ],
        )?;
        for (r, status) in rows {
            appender.append_row(duckdb::params![
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
        appender.flush()?;
        self.conn
            .execute_batch("DELETE FROM ingest_results_stage;")?;
        self.stage_results(rows)?;
        self.conn
            .execute_batch(&derived_results_sql("ingest_results_stage"))?;
        Ok(())
    }

    /// Return history rows for a check, newest first, optionally within a time
    /// range and optionally restricted to one evaluated status.
    ///
    /// The status filter matters: a day can hold thousands of results, so the
    /// failures worth reading about are not necessarily in the newest `limit`.
    pub fn result_coverage(
        &self,
        check_ids: &[CheckId],
        agent_ids: &[String],
        since: i64,
    ) -> Result<Vec<(String, u64)>> {
        if check_ids.is_empty() || agent_ids.is_empty() {
            return Ok(Vec::new());
        }
        let check_placeholders = std::iter::repeat_n("?", check_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let agent_placeholders = std::iter::repeat_n("?", agent_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT agent_id, COUNT(DISTINCT check_id)
             FROM check_results
             WHERE ts_millis >= ? AND check_id IN ({check_placeholders})
               AND agent_id IN ({agent_placeholders})
             GROUP BY agent_id"
        );
        let mut params = vec![duckdb::types::Value::BigInt(since)];
        params.extend(
            check_ids
                .iter()
                .map(|id| duckdb::types::Value::Text(id.to_string())),
        );
        params.extend(agent_ids.iter().cloned().map(duckdb::types::Value::Text));
        let param_refs: Vec<&dyn duckdb::types::ToSql> = params
            .iter()
            .map(|value| value as &dyn duckdb::types::ToSql)
            .collect();
        let mut stmt = self.conn.prepare(&sql)?;
        Ok(stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })?
            .collect::<std::result::Result<_, _>>()?)
    }

    pub fn history(
        &self,
        check_id: CheckId,
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
        // At most eight shapes (three optional clauses), so caching is bounded.
        let mut stmt = self.conn.prepare_cached(&sql)?;
        // Build params dynamically.
        let mut params: Vec<duckdb::types::Value> =
            vec![duckdb::types::Value::Text(check_id.to_string())];
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
        // At most 32 shapes (five optional clauses), so caching is bounded.
        let mut stmt = self.conn.prepare_cached(&sql)?;
        // Must be pushed in the same order the clauses were appended above.
        let mut params: Vec<duckdb::types::Value> = Vec::new();
        if let Some(c) = &filter.check_id {
            params.push(duckdb::types::Value::Text(c.to_string()));
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
                    check_id: row.get::<_, CheckId>(0)?,
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

    /// Persist what the coordinator currently publishes for a check: the
    /// rolled-up state, the hold-down timer in flight, and when it was last
    /// observed. Read back at startup so a restart resumes both the published
    /// state and any window it was part-way through.
    pub fn update_current_state(
        &self,
        check_id: CheckId,
        state: NodeState,
        pending: Option<(NodeState, i64)>,
        last_check: i64,
    ) -> Result<()> {
        self.conn.execute_cached(
            "INSERT OR REPLACE INTO current_state
                 (check_id, status, last_check, consecutive_failures, pending_state, pending_since)
             VALUES (?, ?, ?, 0, ?, ?)",
            duckdb::params![
                check_id,
                state.as_str(),
                last_check,
                pending.map(|(s, _)| s.as_str()),
                pending.map(|(_, since)| since),
            ],
        )?;
        Ok(())
    }

    /// All persisted published states, for seeding the tracker on startup.
    pub fn list_current_state(&self) -> Result<Vec<CurrentStateRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT check_id, status, pending_state, pending_since FROM current_state",
        )?;
        let rows = stmt
            .query_map([], |row| {
                let pending_state = row.get::<_, Option<String>>(2)?;
                let pending_since = row.get::<_, Option<i64>>(3)?;
                Ok(CurrentStateRow {
                    check_id: row.get::<_, CheckId>(0)?,
                    state: NodeState::from_db(&row.get::<_, String>(1)?),
                    // Both halves or neither: a `pending_state` with no
                    // timestamp cannot be resumed and a timestamp with no state
                    // cannot be matched against a candidate, so a half-written
                    // pair is discarded rather than guessed at.
                    pending: pending_state
                        .zip(pending_since)
                        .map(|(s, since)| (NodeState::from_db(&s), since)),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Persist one agent's confirmed opinion of one check.
    #[allow(clippy::too_many_arguments)]
    pub fn update_agent_check_state(
        &self,
        check_id: CheckId,
        agent_id: &str,
        zone: &str,
        region: &str,
        status: Status,
        consecutive_failures: u32,
        last_check: i64,
    ) -> Result<()> {
        self.conn.execute_cached(
            "INSERT INTO agent_check_state
                 (check_id, agent_id, status, consecutive_failures, zone, region, last_check)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (check_id, agent_id) DO UPDATE SET
                 status = excluded.status,
                 consecutive_failures = excluded.consecutive_failures,
                 zone = excluded.zone,
                 region = excluded.region,
                 last_check = excluded.last_check
             WHERE excluded.last_check >= agent_check_state.last_check",
            duckdb::params![
                check_id,
                agent_id,
                status.as_str(),
                consecutive_failures as i32,
                zone,
                region,
                last_check
            ],
        )?;
        Ok(())
    }

    /// Every persisted per-agent belief, for seeding the tracker's leaves.
    pub fn list_agent_check_state(&self) -> Result<Vec<AgentCheckStateRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT check_id, agent_id, zone, region, status, consecutive_failures, last_check
              FROM agent_check_state",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AgentCheckStateRow {
                    check_id: row.get::<_, CheckId>(0)?,
                    agent_id: row.get::<_, String>(1)?,
                    zone: row.get::<_, String>(2)?,
                    region: row.get::<_, String>(3)?,
                    status: Status::from_db(&row.get::<_, String>(4)?),
                    consecutive_failures: row.get::<_, i32>(5)? as u32,
                    last_check: row.get::<_, i64>(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ---- State history ----

    /// Open a new span for a check, closing the previous one implicitly.
    ///
    /// Idempotent on `(check_id, since)`, and a no-op when the newest span
    /// already carries this state: a coordinator that re-publishes the same
    /// conclusion must not fragment its own history into thousands of
    /// zero-length spans.
    pub fn record_state(&self, check_id: CheckId, state: NodeState, since: i64) -> Result<()> {
        let newest: Option<String> = self
            .conn
            .query_row_cached(
                "SELECT state FROM state_history WHERE check_id = ?
                 ORDER BY since DESC LIMIT 1",
                duckdb::params![check_id],
                |r| r.get(0),
            )
            .ok();
        if newest.as_deref().map(NodeState::from_db) == Some(state) {
            return Ok(());
        }
        self.conn.execute_cached(
            "INSERT OR REPLACE INTO state_history (check_id, state, since) VALUES (?, ?, ?)",
            duckdb::params![check_id, state.as_str(), since],
        )?;
        Ok(())
    }

    /// `record_state(check, current_state, since)` for every row of
    /// `current_state`, as one statement: the startup history baseline. Per
    /// row that was two autocommitted statements per check, which at 10,000
    /// checks made it most of a restart. States are normalised exactly as
    /// `NodeState::from_db` does, on both sides of the comparison, so the
    /// spans written are the ones the per-row loop would have written.
    pub fn seed_state_history_baseline(&self, since: i64) -> Result<usize> {
        Ok(self.conn.execute_cached(
            "INSERT OR REPLACE INTO state_history (check_id, state, since)
             WITH current AS (
                 SELECT check_id,
                        CASE WHEN status IN ('up', 'degraded', 'down') THEN status
                             ELSE 'unknown' END AS state
                 FROM current_state
             ),
             newest AS (
                 SELECT check_id, arg_max(state, since) AS state
                 FROM state_history GROUP BY check_id
             )
             SELECT c.check_id, c.state, ?
             FROM current c LEFT JOIN newest n ON n.check_id = c.check_id
             WHERE n.state IS NULL
                OR c.state <> CASE WHEN n.state IN ('up', 'degraded', 'down') THEN n.state
                                   ELSE 'unknown' END",
            duckdb::params![since],
        )?)
    }

    /// Record a peer's published state. Unlike `record_state`, a span is only
    /// ever appended: a replayed or out-of-order batch carrying an older state
    /// must not wedge a stale span under the history this node already has.
    pub fn record_replicated_state(
        &self,
        check_id: CheckId,
        state: NodeState,
        since: i64,
    ) -> Result<()> {
        let newest: Option<(String, i64)> = self
            .conn
            .query_row_cached(
                "SELECT state, since FROM state_history WHERE check_id = ?
                 ORDER BY since DESC LIMIT 1",
                duckdb::params![check_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
        if let Some((newest_state, newest_since)) = newest {
            if NodeState::from_db(&newest_state) == state || since <= newest_since {
                return Ok(());
            }
        }
        self.conn.execute_cached(
            "INSERT OR REPLACE INTO state_history (check_id, state, since) VALUES (?, ?, ?)",
            duckdb::params![check_id, state.as_str(), since],
        )?;
        Ok(())
    }

    /// Every span for a check, oldest first.
    pub fn state_spans(&self, check_id: CheckId) -> Result<Vec<StateSpan>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT state, since FROM state_history WHERE check_id = ? ORDER BY since ASC",
        )?;
        let rows = stmt
            .query_map(duckdb::params![check_id], |r| {
                Ok(StateSpan {
                    state: NodeState::from_db(&r.get::<_, String>(0)?),
                    since: r.get::<_, i64>(1)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Time-weighted uptime over `(from, to]`, as a fraction.
    ///
    /// Counted over spans rather than probes, which fixes three things at once:
    /// a check probed by many agents is no longer scored on how many of them
    /// were healthy, a check whose interval changed is no longer reweighted
    /// retroactively, and `degraded_is_downtime` becomes expressible at all —
    /// raw results have no Degraded to count.
    ///
    /// `Unknown` time is excluded from both numerator and denominator: a
    /// coordinator that was not running, or a check nobody was probing, did not
    /// observe downtime and must not invent uptime either. A window that is
    /// entirely Unknown returns `None` rather than 0.0, so callers can tell
    /// "perfectly broken" from "nothing known".
    pub fn uptime_between(
        &self,
        check_id: CheckId,
        from: i64,
        to: i64,
        degraded_is_downtime: bool,
    ) -> Result<Option<f64>> {
        let spans = self.state_spans(check_id)?;
        let Some((up_ms, known_ms)) = uptime_counts(&spans, from, to, degraded_is_downtime) else {
            return Ok(None);
        };
        Ok(Some(up_ms as f64 / known_ms as f64))
    }

    /// Delete rows older than the retention window. `results_days` covers raw
    /// probe results; `alerts_days` covers state-transition alerts and CLOSED
    /// incidents. An open incident is current reality, never pruned. Config
    /// tombstones use the LARGER of the two windows: a node down through the
    /// shorter one would come back, still hold a check its tombstone says is
    /// deleted, and hand it straight back on the next anti-entropy sweep —
    /// the exact resurrection tombstones exist to prevent. Returns the number
    /// of rows removed.
    /// Delete raw results older than `before`, keeping the two tables derived
    /// from them true without rebuilding either.
    ///
    /// Both rebuilds used to scan everything that *survived* the delete, which
    /// is the expensive half of retention: pruning one day out of thirty read
    /// twenty-nine. Neither is necessary.
    ///
    /// `result_replication_watermarks.max_ts` is a maximum above the cutoff by
    /// definition, so deleting below the cutoff cannot change it; only
    /// `result_count` moves, by exactly what was removed per check. A check
    /// whose count reaches zero has no rows left and loses its row, which is
    /// what a rebuild would have done.
    ///
    /// `latest_check_results` holds one row per check, the newest. If that row
    /// is at or above the cutoff it survives the delete and is still correct;
    /// if it is below the cutoff then every row for that check was below it
    /// too, so the entry must go. That is a one-row-per-check delete, not a
    /// window function over the whole history.
    pub(crate) fn prune_results(&self, before: i64) -> Result<u64> {
        let before = self.rollup_covered_horizon(before)?;
        self.conn
            .execute_batch("DELETE FROM prune_results_counts; DELETE FROM prune_results_min_ts;")?;
        self.conn.execute_cached(
            "INSERT INTO prune_results_counts
             SELECT check_id, COUNT(*) FROM check_results WHERE ts_millis < ?
             GROUP BY check_id",
            duckdb::params![before],
        )?;
        let removed = self.conn.execute_cached(
            "DELETE FROM check_results WHERE ts_millis < ?",
            duckdb::params![before],
        )? as u64;
        if removed == 0 {
            return Ok(0);
        }
        self.conn.execute_cached(
            "DELETE FROM latest_check_results WHERE ts_millis < ?",
            duckdb::params![before],
        )?;
        // One grouped query over checks touched by this prune advances their
        // retained lower bounds; it avoids issuing a history scan per check.
        self.conn.execute_batch(
            "INSERT INTO prune_results_min_ts
                 SELECT results.check_id, MIN(results.ts_millis)
                 FROM check_results results
                 JOIN prune_results_counts pruned ON pruned.check_id = results.check_id
                 GROUP BY results.check_id;
             UPDATE result_replication_watermarks
SET min_ts = COALESCE(remaining.min_ts, result_replication_watermarks.min_ts),
                     result_count = result_replication_watermarks.result_count - pruned.removed
             FROM prune_results_counts pruned
             LEFT JOIN prune_results_min_ts remaining ON remaining.check_id = pruned.check_id
             WHERE pruned.check_id = result_replication_watermarks.check_id;
             DELETE FROM result_replication_watermarks WHERE result_count <= 0;",
        )?;
        Ok(removed)
    }

    /// How far retention may actually delete, given what has been summarized.
    ///
    /// Raw results are the only copy of a latency observation until a rollup
    /// covers it. Deleting on a clock alone assumes the rollup write always
    /// succeeded, and the one time that assumption is wrong is the one time it
    /// matters: a bug or a partial restore leaves a window with no raw rows
    /// and no summary of them either, and nothing afterwards can tell the
    /// difference between that and a window where nothing was measured.
    ///
    /// So the horizon is pulled back to the start of the oldest minute that
    /// raw data claims and the rollups do not. That is deliberately
    /// conservative — it stalls retention rather than losing history, and a
    /// stall is visible in the database-size metric that is already monitored,
    /// while silent loss is visible in nothing.
    ///
    /// Only minute rollups are consulted. Hour rollups are derived from the
    /// same ingest and cover a superset of the window, so a minute gap is the
    /// first thing to appear and the strictest thing to wait for.
    pub(super) fn rollup_covered_horizon(&self, requested: i64) -> Result<i64> {
        let uncovered: Option<i64> = self.conn.query_row_cached(
            "SELECT MIN(raw.bucket_start) FROM (
                 SELECT check_id, agent_id,
                        ts_millis // ? * ? AS bucket_start
                 FROM check_results WHERE ts_millis < ?
             ) raw
             ANTI JOIN check_latency_minute_rollups rolled
               ON rolled.check_id = raw.check_id
              AND rolled.agent_id = raw.agent_id
              AND rolled.bucket_start = raw.bucket_start",
            duckdb::params![MILLIS_PER_MINUTE, MILLIS_PER_MINUTE, requested],
            |row| row.get(0),
        )?;
        match uncovered {
            Some(bucket) if bucket < requested => {
                tracing::warn!(
                    horizon = requested,
                    uncovered_from = bucket,
                    "retention: raw results are not yet covered by a minute rollup; \
                     holding the prune horizon back rather than deleting unsummarized history"
                );
                Ok(bucket)
            }
            _ => Ok(requested),
        }
    }

    /// The oldest raw result held, or `None` when there are none. Cheap: a
    /// minimum over a column written in ascending order.
    pub fn oldest_result_ts(&self) -> Result<Option<i64>> {
        let oldest: Option<i64> = self
            .conn
            .query_row_cached("SELECT MIN(ts_millis) FROM check_results", [], |row| {
                row.get(0)
            })
            .unwrap_or(None);
        Ok(oldest)
    }

    /// Deliberately issues no `CHECKPOINT` of its own. `benchmark_checkpoint_after_prune`
    /// measured one at production scale (1,000 checks, 30 raw days, a
    /// one-day/2.88M-row prune): the file never shrinks — two checkpoints
    /// back to back are byte-identical — and DuckDB has already applied the
    /// prune's ~23 MB of WAL by the time an explicit call would run, once its
    /// own automatic threshold trips. Adding one here would only spend
    /// hundreds of milliseconds (431 ms measured, past the single-worker
    /// slow-call threshold) restating work DuckDB already did, for a file
    /// size that would not change. Revisit if `wal_bytes` is ever observed
    /// growing without bound between prunes in production — that would mean
    /// DuckDB's own threshold is not tripping on this workload — not before.
    pub fn prune(&self, results_days: u64, alerts_days: u64, now_millis: i64) -> Result<u64> {
        let results_before = now_millis - (results_days as i64) * 86_400_000;
        let mut removed = self.prune_results(results_before)?;
        // Rollups outlive raw observations. Their cutoffs are independent of
        // the configurable raw window, so reducing raw retention cannot erase
        // the summaries that make old latency views meaningful.
        removed += self.prune_rollups(
            "check_latency_minute_rollups",
            MILLIS_PER_MINUTE,
            now_millis - MINUTE_ROLLUP_RETENTION_MILLIS,
        )?;
        removed += self.prune_rollups(
            "check_latency_hour_rollups",
            MILLIS_PER_HOUR,
            now_millis - HOUR_ROLLUP_RETENTION_MILLIS,
        )?;
        removed += self.prune_metadata(results_days, alerts_days, now_millis)?;
        Ok(removed)
    }

    /// How many raw results are stored. Shown before a purge, so the confirm
    /// dialog can name what it is about to destroy.
    pub fn count_results(&self) -> Result<u64> {
        let n: i64 = self
            .conn
            .query_row_cached("SELECT COUNT(*) FROM check_results", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    /// Delete every raw result, and the per-agent beliefs derived from them.
    ///
    /// Needed when the fleet's topology is relabelled. Zone and region are
    /// stamped onto each result from the agent's *credential*, so re-minting
    /// credentials under new labels leaves old rows carrying the old ones — and
    /// since the region tree derives its structure from observed locations,
    /// those rows would surface as permanently-stale single-agent regions that
    /// no agent can ever refresh or remove.
    ///
    /// `state_history` is deliberately NOT cleared: it is the coordinator's
    /// record of what it published, which relabelling does not invalidate.
    pub fn purge_results(&self) -> Result<u64> {
        let mut removed = self.conn.execute_cached("DELETE FROM check_results", [])? as u64;
        self.conn
            .execute_cached("DELETE FROM result_replication_watermarks", [])?;
        self.conn
            .execute_cached("DELETE FROM latest_check_results", [])?;
        removed += self
            .conn
            .execute_cached("DELETE FROM agent_check_state", [])? as u64;
        Ok(removed)
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
        check_id: CheckId,
        days: usize,
        buckets: usize,
        now_millis: i64,
        degraded_is_downtime: bool,
    ) -> Result<Vec<UptimeBucket>> {
        let grid = BucketGrid::new(now_millis, days, buckets);
        let buckets = grid.buckets;
        let spans = self.state_spans(check_id)?;
        let mut result_up = vec![0_u64; buckets];
        let mut result_total = vec![0_u64; buckets];
        let mut stmt = self.conn.prepare_cached(
            "SELECT ts_millis, status FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
        )?;
        let rows = stmt.query_map(duckdb::params![check_id, grid.lower, now_millis], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (ts, status) = row?;
            let index = grid.index(ts);
            result_total[index] = result_total[index].saturating_add(1);
            if status == "up" {
                result_up[index] = result_up[index].saturating_add(1);
            }
        }

        Ok((0..buckets)
            .map(|i| {
                let start = grid.lower + i as i64 * grid.bucket_ms;
                let end = start + grid.bucket_ms;
                let uptime = uptime_counts(&spans, start, end, degraded_is_downtime)
                    .map(|(up_ms, known_ms)| up_ms as f64 / known_ms as f64);
                UptimeBucket {
                    start,
                    end,
                    uptime,
                    up: result_up[i],
                    total: result_total[i],
                }
            })
            .collect())
    }

    /// Uptime strips for many checks in two queries rather than two per check.
    ///
    /// The status page draws one strip per visible check, so the per-check path
    /// turned a page load into N round trips and N scans. Here the bucketing is
    /// pushed into DuckDB, so `check_results` is scanned once and yields at most
    /// one row per check per segment.
    ///
    /// `checks` carries each check's resolved `degraded_is_downtime`, because
    /// that is a per-check decision the caller has already made and storage must
    /// not guess at.
    pub fn uptime_strips(
        &self,
        checks: &[(CheckId, bool)],
        days: usize,
        buckets: usize,
        now_millis: i64,
    ) -> Result<BTreeMap<CheckId, CheckUptime>> {
        let grid = BucketGrid::new(now_millis, days, buckets);
        self.stage_check_ids(checks)?;

        let mut spans: BTreeMap<CheckId, Vec<StateSpan>> = BTreeMap::new();
        {
            let mut stmt = self.conn.prepare_cached(
                "SELECT check_id, state, since FROM state_history
                 WHERE check_id IN (SELECT check_id FROM status_check_ids)
                 ORDER BY check_id, since",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, CheckId>(0)?,
                    NodeState::from_db(&row.get::<_, String>(1)?),
                    row.get::<_, i64>(2)?,
                ))
            })?;
            for row in rows {
                let (check_id, state, since) = row?;
                spans
                    .entry(check_id)
                    .or_default()
                    .push(StateSpan { state, since });
            }
        }

        let mut counts: BTreeMap<CheckId, (Vec<u64>, Vec<u64>)> = BTreeMap::new();
        {
            let mut stmt = self.conn.prepare_cached(
                "SELECT check_id, ((ts_millis - ? - 1) // ?)::BIGINT AS idx,
                        COUNT(*), COUNT(*) FILTER (WHERE status = 'up')
                 FROM check_results
                 WHERE ts_millis > ? AND ts_millis <= ?
                   AND check_id IN (SELECT check_id FROM status_check_ids)
                 GROUP BY 1, 2",
            )?;
            let rows = stmt.query_map(
                duckdb::params![grid.lower, grid.bucket_ms, grid.lower, now_millis],
                |row| {
                    Ok((
                        row.get::<_, CheckId>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )?;
            for row in rows {
                let (check_id, index, total, up) = row?;
                let entry = counts
                    .entry(check_id)
                    .or_insert_with(|| (vec![0; grid.buckets], vec![0; grid.buckets]));
                let index = (index.max(0) as usize).min(grid.buckets - 1);
                entry.0[index] = entry.0[index].saturating_add(up.max(0) as u64);
                entry.1[index] = entry.1[index].saturating_add(total.max(0) as u64);
            }
        }

        Ok(checks
            .iter()
            .map(|(check_id, degraded_is_downtime)| {
                let spans = spans.get(check_id).map(Vec::as_slice).unwrap_or_default();
                let counted = counts.get(check_id);
                let strip = (0..grid.buckets)
                    .map(|i| {
                        let start = grid.lower + i as i64 * grid.bucket_ms;
                        let end = start + grid.bucket_ms;
                        UptimeBucket {
                            start,
                            end,
                            uptime: uptime_counts(spans, start, end, *degraded_is_downtime)
                                .map(|(up_ms, known_ms)| up_ms as f64 / known_ms as f64),
                            up: counted.map(|c| c.0[i]).unwrap_or(0),
                            total: counted.map(|c| c.1[i]).unwrap_or(0),
                        }
                    })
                    .collect();
                let overall = uptime_counts(spans, grid.lower, now_millis, *degraded_is_downtime)
                    .map(|(up_ms, known_ms)| up_ms as f64 / known_ms as f64);
                (
                    *check_id,
                    CheckUptime {
                        buckets: strip,
                        overall,
                    },
                )
            })
            .collect())
    }

    // ---- Data-plane replication (results, alerts, incidents) ----

    /// Insert only rows this node does not already have. Returns how many
    /// were actually inserted.
    ///
    /// `check_results` carries no unique constraint — adding a primary key to
    /// a hot table is a migration this project does not do — so dedup is an
    /// anti-join on `(check_id, agent_id, ts_millis)`, done once for the whole
    /// batch through a staging table rather than once per row.
    ///
    /// Most of a batch never reaches the anti-join: rows newer than the
    /// per-check high-water mark cannot be duplicates, which is every row of an
    /// ordinary replication push. What remains — a repair replaying old rows —
    /// is probed with the scan bounded to that subset's own time range. The
    /// batch is also deduplicated against itself, so the same row arriving
    /// twice in one batch inserts once. Replaying a whole batch remains a
    /// no-op, which is what anti-entropy depends on.
    pub fn insert_results_dedup(&self, rows: &[(CheckResult, Status)]) -> Result<usize> {
        self.transaction(|store| store.insert_results_dedup_uncommitted(rows))
    }

    pub(crate) fn insert_results_dedup_uncommitted(
        &self,
        rows: &[(CheckResult, Status)],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.conn.execute_batch(
            "DELETE FROM ingest_results_stage;
             DELETE FROM replicate_new_results;
             DELETE FROM replicate_probe_stage;",
        )?;
        self.stage_results(rows)?;
        // Split the batch on this node's own high-water mark before touching
        // `check_results` at all. A row newer than the newest one held for its
        // check cannot be a duplicate of anything, which in steady-state
        // replication is every row in the batch — so the ordinary path never
        // probes the history table, and its cost stops depending on how much
        // history there is. Only repair, which replays old rows by definition,
        // pays for the anti-join.
        //
        // This rests on `result_replication_watermarks.max_ts` being the true
        // maximum for the check. Every writer of `check_results` maintains it
        // in the same statement or transaction, `Storage::open` backfills it,
        // and `prune` keeps it true by construction — deleting rows below a
        // cutoff cannot lower a maximum above it.
        self.conn.execute_batch(
            "INSERT INTO replicate_new_results
             SELECT staged.* FROM ingest_results_stage staged
             LEFT JOIN result_replication_watermarks mark
                    ON mark.check_id = staged.check_id
             WHERE mark.max_ts IS NULL OR staged.ts_millis > mark.max_ts
             QUALIFY row_number() OVER (
                 PARTITION BY staged.check_id, staged.agent_id, staged.ts_millis
                 ORDER BY staged.ts_millis
             ) = 1;
             INSERT INTO replicate_probe_stage
             SELECT staged.* FROM ingest_results_stage staged
             JOIN result_replication_watermarks mark
               ON mark.check_id = staged.check_id
             WHERE staged.ts_millis <= mark.max_ts;",
        )?;
        let probed: i64 = self.conn.query_row_cached(
            "SELECT count(*) FROM replicate_probe_stage",
            [],
            |row| row.get(0),
        )?;
        if probed > 0 {
            self.conn.execute_batch(
                "INSERT INTO replicate_new_results
                 SELECT staged.* FROM replicate_probe_stage staged
                 WHERE NOT EXISTS (
                     SELECT 1 FROM check_results existing
                     WHERE existing.check_id = staged.check_id
                       AND existing.agent_id = staged.agent_id
                       AND existing.ts_millis = staged.ts_millis
                       -- Sound because a duplicate of a staged row carries that
                       -- row's exact `ts_millis`, never below this minimum, so
                       -- nothing that could match is excluded. Results are
                       -- appended in time order, so the bound prunes by zone
                       -- map instead of probing every retained row.
                       AND existing.ts_millis >= (SELECT MIN(ts_millis) FROM replicate_probe_stage)
                 )
                 QUALIFY row_number() OVER (
                     PARTITION BY staged.check_id, staged.agent_id, staged.ts_millis
                     ORDER BY staged.ts_millis
                 ) = 1;",
            )?;
        }
        self.conn.execute_batch(&format!(
            "INSERT INTO check_results SELECT * FROM replicate_new_results;
             {derived}",
            derived = derived_results_sql("replicate_new_results"),
        ))?;
        let inserted: i64 = self.conn.query_row_cached(
            "SELECT count(*) FROM replicate_new_results",
            [],
            |row| row.get(0),
        )?;
        Ok(inserted as usize)
    }

    /// Paged raw rows for backfill. `(since, since_agent_id)` is exclusive and
    /// rows come back oldest-first so the caller can advance the stable cursor.
    /// Without an agent id, all rows at `since` are excluded for the first page.
    ///
    /// `kind`, `body_matched`, `body`, `headers`, `metric` and `metrics` are
    /// never in `check_results` — the table keeps the coordinator's verdict,
    /// not the probe's raw observation — so they read back as their defaults.
    /// The replica gets what was persisted, not what was probed, and that is
    /// the whole point of replicating the observer's verdict rather than
    /// re-evaluating it on every node.
    pub fn results_since(
        &self,
        check_id: CheckId,
        since: i64,
        since_agent_id: Option<&str>,
        until: i64,
        limit: usize,
    ) -> Result<Vec<(CheckResult, Status)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT agent_id, ts_millis, status, latency_ms, status_code, error,
                    failure, zone, region
             FROM check_results
             WHERE check_id = ? AND ts_millis <= ?
               AND (ts_millis > ? OR (? IS NOT NULL AND ts_millis = ? AND agent_id > ?))
             ORDER BY ts_millis ASC, agent_id ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![
                    check_id,
                    until,
                    since,
                    since_agent_id,
                    since,
                    since_agent_id,
                    limit as i64
                ],
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
                check_id,
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

    /// All raw result rows after the exclusive `(timestamp, check_id,
    /// agent_id)` cursor, oldest first. The complete key means a page cannot
    /// skip rows when more than one check and agent share a timestamp.
    pub fn results_after(
        &self,
        since: i64,
        since_check_id: Option<CheckId>,
        since_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(CheckResult, Status)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT check_id, agent_id, ts_millis, status, latency_ms, status_code, error,
                    failure, zone, region
             FROM check_results
             WHERE ts_millis > ? OR (ts_millis = ? AND (
                 ? IS NULL OR check_id > ? OR (check_id = ? AND agent_id > ?)
             ))
             ORDER BY ts_millis ASC, check_id ASC, agent_id ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![
                    since,
                    since,
                    since_check_id,
                    since_check_id,
                    since_check_id,
                    since_agent_id,
                    limit as i64
                ],
                |row| {
                    Ok((
                        row.get::<_, CheckId>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i32>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    check_id,
                    agent_id,
                    ts,
                    status,
                    latency_ms,
                    status_code,
                    error,
                    failure,
                    zone,
                    region,
                )| {
                    (
                        CheckResult {
                            check_id,
                            agent_id,
                            zone: zone.unwrap_or_default(),
                            region: region.unwrap_or_default(),
                            kind: CheckKind::Http,
                            timestamp: chrono::DateTime::from_timestamp_millis(ts)
                                .unwrap_or_default(),
                            latency_ms: latency_ms as u64,
                            status_code: status_code.map(|c| c as u16),
                            body_matched: None,
                            error,
                            failure: failure.and_then(|f| serde_json::from_str(&f).ok()),
                            body: None,
                            headers: None,
                            metric: None,
                            metrics: None,
                        },
                        Status::from_db(&status),
                    )
                },
            )
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::storage::test_support::*;

    /// One batched write must leave every projection exactly where the
    /// row-at-a-time path left it: newest observation wins per check, equal
    /// timestamps break ties on the greater agent, and per-agent beliefs
    /// collapse to the newest row for that agent.
    #[test]
    fn persist_ingest_projects_the_newest_observation_per_check_and_agent() {
        let s = Storage::open(":memory:").unwrap();
        let mut older = result("a", 1_000, 5);
        older.agent_id = "agent-b".into();
        let mut tie = result("a", 2_000, 7);
        tie.agent_id = "agent-a".into();
        let mut newest = result("a", 2_000, 9);
        newest.agent_id = "agent-c".into();

        s.persist_ingest(
            &[
                (older, Status::Up),
                (tie, Status::Down),
                (newest, Status::Up),
            ],
            &[
                agent_state("a", "agent-b", Status::Up, 1_000),
                agent_state("a", "agent-a", Status::Down, 2_000),
                agent_state("a", "agent-a", Status::Up, 2_500),
            ],
            &[published("a", NodeState::Up, 2_000)],
        )
        .unwrap();

        assert_eq!(s.count_results().unwrap(), 3);
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.checks[0].count, 3);
        assert_eq!(digest.checks[0].max_ts, 2_000);
        let latest = s.list_status(3_000).unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].latency_ms, 9, "greater agent wins an exact tie");

        let mut beliefs = s.list_agent_check_state().unwrap();
        beliefs.sort_by(|l, r| l.agent_id.cmp(&r.agent_id));
        assert_eq!(beliefs.len(), 2);
        assert_eq!(beliefs[0].last_check, 2_500);
        assert_eq!(beliefs[0].status, Status::Up);
        assert_eq!(beliefs[1].last_check, 1_000);
        assert_eq!(
            s.list_current_state().unwrap(),
            vec![CurrentStateRow {
                check_id: cid("a"),
                state: NodeState::Up,
                pending: None,
            }]
        );
    }

    /// A late batch must not rewind what is already published, and repeating a
    /// state must not fragment history into a span per probe.
    #[test]
    fn persist_ingest_ignores_stale_rows_and_records_only_transitions() {
        let s = Storage::open(":memory:").unwrap();
        s.persist_ingest(
            &[(result("a", 5_000, 11), Status::Up)],
            &[agent_state("a", "agent", Status::Up, 5_000)],
            &[published("a", NodeState::Up, 5_000)],
        )
        .unwrap();
        s.persist_ingest(
            &[(result("a", 1_000, 99), Status::Down)],
            &[agent_state("a", "agent", Status::Down, 1_000)],
            &[published("a", NodeState::Up, 5_100)],
        )
        .unwrap();

        assert_eq!(s.count_results().unwrap(), 2, "raw history keeps both rows");
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.checks[0].max_ts, 5_000);
        assert_eq!(digest.checks[0].count, 2);
        let latest = s.list_status(6_000).unwrap();
        assert_eq!(latest[0].latency_ms, 11, "a stale row cannot rewind latest");
        let beliefs = s.list_agent_check_state().unwrap();
        assert_eq!(beliefs[0].status, Status::Up, "stale belief is ignored");
        assert_eq!(
            s.state_spans(cid("a")).unwrap(),
            vec![StateSpan {
                state: NodeState::Up,
                since: 5_000,
            }],
            "an unchanged state opens no second span"
        );

        s.persist_ingest(
            &[(result("a", 6_000, 12), Status::Down)],
            &[agent_state("a", "agent", Status::Down, 6_000)],
            &[published("a", NodeState::Down, 6_000)],
        )
        .unwrap();
        assert_eq!(
            s.state_spans(cid("a")).unwrap().len(),
            2,
            "a changed state opens exactly one new span"
        );
    }

    /// Every projection commits together or not at all: a failing batch must
    /// not leave raw rows behind, and the connection must stay usable.
    #[test]
    fn persist_ingest_rolls_back_every_projection_on_failure() {
        let s = Storage::open(":memory:").unwrap();
        s.persist_ingest(
            &[(result("a", 1_000, 5), Status::Up)],
            &[agent_state("a", "agent", Status::Up, 1_000)],
            &[published("a", NodeState::Up, 1_000)],
        )
        .unwrap();
        // Current state is written after the raw rows, watermarks, latest
        // result, and agent belief of the same batch, so rejecting it proves
        // those earlier writes are rolled back rather than half-kept.
        s.conn
            .execute_batch("CREATE UNIQUE INDEX one_check_per_ts ON current_state (last_check)")
            .unwrap();

        let failed = s.persist_ingest(
            &[(result("b", 1_000, 6), Status::Down)],
            &[agent_state("b", "agent", Status::Down, 1_000)],
            &[published("b", NodeState::Down, 1_000)],
        );
        assert!(failed.is_err(), "a rejected projection fails the batch");
        assert_eq!(s.count_results().unwrap(), 1, "raw rows rolled back");
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.checks.len(), 1, "watermarks rolled back");
        assert_eq!(digest.checks[0].check_id, cid("a"));
        assert_eq!(s.list_status(3_000).unwrap().len(), 1);
        assert_eq!(s.list_agent_check_state().unwrap().len(), 1);
        assert_eq!(s.list_current_state().unwrap().len(), 1);
        assert!(s.state_spans(cid("b")).unwrap().is_empty());

        s.conn.execute_batch("DROP INDEX one_check_per_ts").unwrap();
        s.persist_ingest(
            &[(result("b", 1_000, 6), Status::Down)],
            &[agent_state("b", "agent", Status::Down, 1_000)],
            &[published("b", NodeState::Down, 1_000)],
        )
        .expect("the connection survives a failed batch");
        assert_eq!(s.count_results().unwrap(), 2);
    }

    #[test]
    fn ingest_transition_failure_rolls_back_all_rows_and_recovers() {
        let s = Storage::open(":memory:").unwrap();
        s.prepare_ingest().unwrap();
        let attempt = || {
            s.transaction(|store| {
                store.persist_ingest_uncommitted(
                    &[(result("a", 1_000, 5), Status::Down)],
                    &[agent_state("a", "agent", Status::Down, 1_000)],
                    &[published("a", NodeState::Down, 1_000)],
                )?;
                store.create_incident(
                    "incident-a",
                    "Down",
                    IncidentState::Detected,
                    1_000,
                    false,
                    Some(cid("a")),
                    &[IncidentCheckRow {
                        check_id: cid("a"),
                        source: AffectSource::Origin,
                    }],
                    None,
                )?;
                store.insert_alert(
                    AlertScope::Check,
                    &cid("a").to_string(),
                    Some(NodeState::Up),
                    NodeState::Down,
                    1_000,
                    Some("incident-a"),
                )?;
                Ok(())
            })
        };
        s.conn
            .execute_batch("CREATE UNIQUE INDEX reject_alert ON alerts (ts_millis)")
            .unwrap();
        s.conn.execute_batch("INSERT INTO alerts (id, scope, node_id, current_state, ts_millis) VALUES ('block', 'check', 'block', 'up', 1000)").unwrap();
        assert!(attempt().is_err());
        assert_eq!(s.count_results().unwrap(), 0);
        assert!(s.list_current_state().unwrap().is_empty());
        assert!(s.list_agent_check_state().unwrap().is_empty());
        assert!(s.state_spans(cid("a")).unwrap().is_empty());
        assert!(s.incident("incident-a").unwrap().is_none());
        s.conn
            .execute_batch("DELETE FROM alerts WHERE id = 'block'")
            .unwrap();
        attempt().unwrap();
        assert_eq!(s.count_results().unwrap(), 1);
        assert!(s.incident("incident-a").unwrap().is_some());
        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
    }

    #[test]
    fn uncommitted_ingest_is_not_visible_after_reopen() {
        let path =
            std::env::temp_dir().join(format!("lighthouse-ingest-{}.duckdb", uuid::Uuid::now_v7()));
        {
            let s = Storage::open(path.to_str().unwrap()).unwrap();
            s.prepare_ingest().unwrap();
            s.conn.execute_batch("BEGIN").unwrap();
            s.persist_ingest_uncommitted(
                &[(result("a", 1_000, 5), Status::Down)],
                &[agent_state("a", "agent", Status::Down, 1_000)],
                &[published("a", NodeState::Down, 1_000)],
            )
            .unwrap();
            s.create_incident(
                "incident-a",
                "Down",
                IncidentState::Detected,
                1_000,
                false,
                Some(cid("a")),
                &[],
                None,
            )
            .unwrap();
        }
        let s = Storage::open(path.to_str().unwrap()).unwrap();
        assert_eq!(s.count_results().unwrap(), 0);
        assert!(s.list_current_state().unwrap().is_empty());
        assert!(s.incident("incident-a").unwrap().is_none());
        drop(s);
        std::fs::remove_file(path).unwrap();
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
        let hist = s.history(cid("a"), 10, None, None, None).unwrap();
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
        let a = rows.iter().find(|r| r.check_id == cid("a")).unwrap();
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
        assert_eq!(all[0].check_id, cid("ddns-wa"), "1s ago is the newest");

        // Per-column filters narrow correctly.
        let by_check = s
            .recent_results(
                &ResultsFilter {
                    check_id: Some(cid("web")),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(by_check.len(), 2);
        assert!(by_check.iter().all(|r| r.check_id == cid("web")));

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
                    check_id: Some(cid("web")),
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
        assert!(capped.iter().all(|r| r.check_id == cid("ddns-wa")));
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
        let a = rows.iter().find(|r| r.check_id == cid("a")).unwrap();
        assert_eq!(
            a.state,
            NodeState::Up,
            "without a published state, the latest observation is lifted"
        );
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
        let hist = s.history(cid("a"), 10, None, None, None).unwrap();
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
        let capped = s.history(cid("a"), 2, None, None, None).unwrap();
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
        // Uptime is derived from published-state spans, rather than raw probe
        // counts. The three seconds Up followed by one second Down yield 75%.
        s.record_state(cid("a"), NodeState::Up, now - 4000).unwrap();
        s.record_state(cid("a"), NodeState::Down, now - 1000)
            .unwrap();

        let rows = s.list_status(now).unwrap();
        assert_eq!(rows.len(), 2, "one row per distinct check_id");
        let find = |id: &str| rows.iter().find(|r| r.check_id == cid(id)).unwrap().clone();

        let a = find("a");
        assert_eq!(
            a.state,
            NodeState::Down,
            "without a published state, the latest observation is lifted"
        );
        assert_eq!(a.latency_ms, 50, "latest row's latency");
        assert_eq!(a.last_check, now, "latest row's timestamp");
        assert!(
            (a.uptime_24h - 0.75).abs() < 1e-9,
            "3 UP / 4 in-window, got {}",
            a.uptime_24h
        );

        let b = find("b");
        assert_eq!(b.state, NodeState::Up);
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
    fn list_status_projection_handles_out_of_order_equal_timestamp_and_nulls() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let mut failure = result("a", now, 50);
        failure.agent_id = "agent-a".into();
        failure.status_code = None;
        failure.error = Some("failed".into());
        failure.failure = Some(crate::models::FailureKind::ConnectionFailed);
        let mut recovery = result("a", now, 12);
        recovery.agent_id = "agent-z".into();
        recovery.status_code = Some(204);
        let stale = result("a", now - 1, 99);

        s.insert_results(&[(failure, Status::Down), (recovery, Status::Up)])
            .unwrap();
        s.insert_results(&[(stale, Status::Down)]).unwrap();

        let row = s.list_status(now).unwrap().remove(0);
        assert_eq!(row.state, NodeState::Up);
        assert_eq!(row.latency_ms, 12);
        assert_eq!(row.status_code, Some(204));
        assert_eq!(row.error, None);
        assert_eq!(row.failure, None);
    }

    #[test]
    fn opening_repairs_an_incomplete_latest_results_projection() {
        let path = std::env::temp_dir().join(format!(
            "lighthouse-latest-results-repair-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let now = 1_000_000_000_000i64;
        {
            let s = Storage::open(&path_string).unwrap();
            s.insert_results(&[
                (result("a", now, 10), Status::Up),
                (result("b", now, 20), Status::Down),
            ])
            .unwrap();
            s.conn
                .execute(
                    "DELETE FROM latest_check_results WHERE check_id = ?",
                    duckdb::params![cid("b")],
                )
                .unwrap();
            assert_eq!(s.list_status(now).unwrap().len(), 1);
        }

        let reopened = Storage::open(&path_string).unwrap();
        assert_eq!(reopened.list_status(now).unwrap().len(), 2);
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn list_status_projection_is_rebuilt_after_agent_purge() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let mut older = result("a", now - 1, 10);
        older.agent_id = "kept".into();
        let mut latest = result("a", now, 20);
        latest.agent_id = "purged".into();
        s.insert_results(&[(older, Status::Up), (latest, Status::Down)])
            .unwrap();

        s.purge_agent("purged").unwrap();

        let row = s.list_status(now).unwrap().remove(0);
        assert_eq!(row.state, NodeState::Up);
        assert_eq!(row.latency_ms, 10);
        assert_eq!(row.last_check, now - 1);
    }

    #[test]
    fn replicated_states_only_ever_extend_the_history() {
        let s = Storage::open(":memory:").unwrap();
        let id = CheckId::from_u128(7);
        let spans = || {
            s.state_spans(id)
                .unwrap()
                .into_iter()
                .map(|span| (span.state, span.since))
                .collect::<Vec<_>>()
        };
        s.record_replicated_state(id, NodeState::Up, 1_000).unwrap();
        // Same state again, later: the open span already says so.
        s.record_replicated_state(id, NodeState::Up, 2_000).unwrap();
        s.record_replicated_state(id, NodeState::Down, 3_000)
            .unwrap();
        // A stale batch arriving late must not wedge a span under the history.
        s.record_replicated_state(id, NodeState::Up, 2_500).unwrap();
        assert_eq!(spans(), [(NodeState::Up, 1_000), (NodeState::Down, 3_000)]);
    }

    #[test]
    fn uptime_buckets_are_time_weighted_from_state_spans() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        // Raw results provide tooltip counts, but only published state spans
        // determine the time-weighted uptime ratio.
        s.insert_results(&[
            (result("x", now - 3 * day + 1, 10), Status::Down),
            (result("x", now - 2 * day + 1, 10), Status::Up),
            (result("x", now - day + 1, 10), Status::Up),
        ])
        .unwrap();
        s.record_state(cid("x"), NodeState::Up, now - 3 * day)
            .unwrap();
        s.record_state(cid("x"), NodeState::Down, now - day - day / 2)
            .unwrap();
        s.record_state(cid("x"), NodeState::Degraded, now - day / 2)
            .unwrap();

        let buckets = s.uptime_buckets(cid("x"), 3, 3, now, false).unwrap();
        assert_eq!(buckets.len(), 3, "exactly `days` buckets, oldest-first");
        assert_eq!(buckets[0].uptime, Some(1.0));
        assert_eq!(buckets[0].up, 0);
        assert_eq!(buckets[0].total, 1);
        assert_eq!(buckets[1].uptime, Some(0.5));
        assert_eq!(buckets[1].up, 1);
        assert_eq!(buckets[1].total, 1);
        assert_eq!(buckets[2].uptime, Some(0.5));
        assert_eq!(buckets[2].up, 1);
        assert_eq!(buckets[2].total, 1);
        assert!(
            buckets.iter().all(|bucket| bucket.total <= 1),
            "tooltip counts are probe attempts, not milliseconds"
        );

        let degraded_down = s.uptime_buckets(cid("x"), 3, 3, now, true).unwrap();
        assert_eq!(degraded_down[2].uptime, Some(0.0));
        assert_eq!(
            s.uptime_between(cid("x"), now - 3 * day, now, true)
                .unwrap(),
            Some(0.5)
        );
    }

    /// The batched strip replaces N per-check queries on the status page, so it
    /// must return exactly what those queries returned — same segment edges,
    /// same counts, same time-weighted fraction, and the same per-check
    /// `degraded_is_downtime` decision.
    #[test]
    fn batched_uptime_strips_agree_with_the_per_check_query() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        for (name, offset) in [("x", 0), ("y", day / 4)] {
            s.insert_results(&[
                (result(name, now - 2 * day + offset, 10), Status::Up),
                (result(name, now - day + offset, 20), Status::Down),
                (result(name, now - day / 2 + offset, 30), Status::Up),
            ])
            .unwrap();
        }
        s.record_state(cid("x"), NodeState::Up, now - 3 * day)
            .unwrap();
        s.record_state(cid("x"), NodeState::Down, now - day)
            .unwrap();
        s.record_state(cid("x"), NodeState::Up, now - day / 2)
            .unwrap();
        s.record_state(cid("y"), NodeState::Degraded, now - 3 * day)
            .unwrap();

        // `y` treats Degraded as downtime and `x` does not, so a shared flag
        // would show up as a mismatch on one of them.
        let checks = [(cid("x"), false), (cid("y"), true)];
        let batched = s.uptime_strips(&checks, 3, 12, now).unwrap();
        assert_eq!(batched.len(), 2);

        for (check_id, degraded_is_downtime) in checks {
            let expected = s
                .uptime_buckets(check_id, 3, 12, now, degraded_is_downtime)
                .unwrap();
            let got = &batched[&check_id];
            assert_eq!(got.buckets, expected, "{check_id} strip must match");
            assert_eq!(
                got.overall,
                s.uptime_between(check_id, now - 3 * day, now, degraded_is_downtime)
                    .unwrap(),
                "{check_id} overall must match the single-check figure"
            );
        }
    }

    /// A check with no rows at all must still get a full strip of empty
    /// segments, because the page renders one bar per segment regardless.
    #[test]
    fn batched_uptime_strips_cover_checks_with_no_data() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        let strips = s
            .uptime_strips(&[(cid("absent"), false)], 7, 90, now)
            .unwrap();
        let strip = &strips[&cid("absent")];
        assert_eq!(strip.buckets.len(), 90);
        assert!(strip.buckets.iter().all(|b| b.uptime.is_none()));
        assert!(strip.buckets.iter().all(|b| b.total == 0));
        assert_eq!(strip.overall, None, "nothing known is not zero uptime");
    }

    #[test]
    fn uptime_buckets_no_rows_returns_empty_segments() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        let buckets = s.uptime_buckets(cid("absent"), 4, 4, now, false).unwrap();
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

        let hist = s.history(cid("web"), 10, None, None, None).unwrap();
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
        let newest = s.history(cid("web"), 100, None, None, None).unwrap();
        assert_eq!(newest.len(), 100);
        assert!(
            newest.iter().all(|r| r.status == Status::Up),
            "the newest slice hides the failures, which is why the filter exists"
        );

        // Filtering by status surfaces exactly the two failures, with reasons.
        let bad = s
            .history(cid("web"), 500, None, None, Some(Status::Down))
            .unwrap();
        assert_eq!(bad.len(), 2, "both failures found regardless of recency");
        assert!(bad.iter().all(|r| r.error.as_deref() == Some("timeout")));

        // And the filter is a filter, not a truncation: successes still queryable.
        let good = s
            .history(cid("web"), 500, None, None, Some(Status::Up))
            .unwrap();
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
            Some(NodeState::Up),
            NodeState::Down,
            now - 1000,
            None,
        )
        .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Down),
            NodeState::Up,
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
                Some(cid(check)),
                &[IncidentCheckRow {
                    check_id: cid(check),
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
            id: format!("queued-{check}"),
            notifier: "oncall".into(),
            check_id: cid(check),
            previous_state: Some(NodeState::Up),
            current_state: NodeState::Down,
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
        assert_eq!(left[0].check_id, cid("b"));

        // The recent result and alert survive.
        let status = s.list_status(now).unwrap();
        assert_eq!(status.len(), 1, "only the recent result's check remains");
        assert_eq!(status[0].check_id, cid("a"));
        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
        // The open incident survives whole; the resolved one is gone entirely.
        let incidents = s.list_incidents(10, None, None).unwrap();
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

    #[test]
    fn prune_metadata_rolls_back_dependent_deletes_when_watermark_rebuild_fails() {
        let s = Storage::open(":memory:").unwrap();
        let day = 86_400_000i64;
        let now = 2 * day;
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            1,
            None,
        )
        .unwrap();
        s.create_incident(
            "old",
            "outage",
            IncidentState::Detected,
            0,
            false,
            Some(cid("a")),
            &[IncidentCheckRow {
                check_id: cid("a"),
                source: AffectSource::Origin,
            }],
            Some(("down", None)),
        )
        .unwrap();
        s.add_update("old", IncidentState::Resolved, "recovered", None, 0)
            .unwrap();

        // The pre-prune stream maxima differ. After all old metadata is deleted,
        // rebuilding writes three zero-max rows and this index rejects the
        // second one, after the dependent deletes have run.
        s.conn
            .execute_batch(
                "CREATE UNIQUE INDEX fail_metadata_watermark_rebuild
                 ON replication_stream_watermarks (max_millis)",
            )
            .unwrap();
        assert!(s.prune_metadata(1, 1, now).is_err());

        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
        let incident = s.incident("old").unwrap().expect("incident rolled back");
        assert_eq!(incident.checks.len(), 1, "affects row rolled back");
        assert_eq!(s.updates_for_incident("old").unwrap().len(), 2);

        s.conn
            .execute_batch("DROP INDEX fail_metadata_watermark_rebuild")
            .unwrap();
        assert!(s.prune_metadata(1, 1, now).unwrap() > 0);
        assert!(s.incident("old").unwrap().is_none());
    }

    #[test]
    fn prune_metadata_is_idempotent_after_restart() {
        let path = std::env::temp_dir().join(format!(
            "lighthouse-prune-metadata-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let now = 2 * 86_400_000i64;
        {
            let s = Storage::open(&path_string).unwrap();
            s.insert_alert(
                AlertScope::Check,
                "a",
                Some(NodeState::Up),
                NodeState::Down,
                1,
                None,
            )
            .unwrap();
            assert_eq!(s.prune_metadata(1, 1, now).unwrap(), 1);
            assert_eq!(s.stream_digest("alerts").unwrap().count, 0);
        }

        let restarted = Storage::open(&path_string).unwrap();
        assert_eq!(restarted.prune_metadata(1, 1, now).unwrap(), 0);
        assert_eq!(restarted.recent_alerts(10).unwrap().len(), 0);
        assert_eq!(restarted.stream_digest("alerts").unwrap().count, 0);
        drop(restarted);
        std::fs::remove_file(path).unwrap();
    }

    /// Small-scale, always-on regression cover for TODO.md's "restart/recovery
    /// tests using a persistent production-sized database" — see
    /// `benchmark_restart_recovery_at_production_scale` below for the scaled
    /// version this mirrors. Fast enough to run on every `cargo test`, so a
    /// regression in WAL replay, watermark rebuilds, or the backup/restore
    /// round trip fails CI rather than waiting for someone to run the
    /// `#[ignore]`d benchmark by hand.
    ///
    /// `std::mem::forget` skips `Connection`'s `Drop` rather than calling it,
    /// which is the closest thing to an unclean shutdown reachable safely
    /// from within one test process — an actual killed process is exercised
    /// manually against the built binary (see SCALING.md's backup section)
    /// but is not something a unit test can safely automate. It is a real
    /// difference from a clean `drop`, not a no-op: a clean close lets DuckDB
    /// release its file lock and finish any pending bookkeeping on the way
    /// out, and skipping that is exactly what durability across restarts is
    /// supposed to tolerate.
    #[test]
    fn restart_recovers_committed_rows_and_backup_restore_preserves_them() {
        let path = std::env::temp_dir().join(format!(
            "lighthouse-restart-recovery-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let checks = 20u64;
        let observations_per_check = 500u64;

        {
            let store = Storage::open(&path_string).unwrap();
            store
                .conn
                .execute_batch(&format!(
                    "INSERT INTO check_results
                     SELECT md5('check-' || c::VARCHAR)::UUID, 'agent-a', o * 1000,
                            'up', 12, 200, NULL, 'zone', 'region', NULL
                     FROM range({checks}) t(c) CROSS JOIN range({observations_per_check}) u(o);"
                ))
                .unwrap();
            store.rebuild_result_watermarks().unwrap();
            store.rebuild_latest_check_results().unwrap();
            store.rebuild_latency_rollups().unwrap();
            store
                .update_current_state(cid("check-0"), NodeState::Up, None, 1_000)
                .unwrap();
            // No explicit checkpoint and no clean drop: the WAL, not the main
            // file, is what has to carry every one of these commits forward.
            std::mem::forget(store);
        }

        let expected_rows = checks * observations_per_check;
        let recovery_started = std::time::Instant::now();
        let restarted = Storage::open(&path_string).unwrap();
        let recovery_elapsed = recovery_started.elapsed();
        assert!(
            recovery_elapsed < std::time::Duration::from_secs(5),
            "recovery of {expected_rows} rows took {recovery_elapsed:?}, which would be a \
             regression worth investigating even at this small a scale"
        );
        assert_eq!(restarted.count_results().unwrap(), expected_rows);
        assert_eq!(restarted.list_current_state().unwrap().len(), 1);
        let watermarks = restarted.digest(0).unwrap();
        assert_eq!(watermarks.checks.len() as u64, checks);
        assert_eq!(
            watermarks
                .checks
                .iter()
                .map(|c| c.count as u64)
                .sum::<u64>(),
            expected_rows
        );
        let minute_rollups = restarted
            .rollup_digest("check_latency_minute_rollups")
            .unwrap();
        let hour_rollups = restarted
            .rollup_digest("check_latency_hour_rollups")
            .unwrap();
        assert!(minute_rollups.count > 0);
        assert!(hour_rollups.count > 0);

        let backup_path = std::env::temp_dir().join(format!(
            "lighthouse-restart-recovery-backup-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let restore_path = std::env::temp_dir().join(format!(
            "lighthouse-restart-recovery-restored-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        crate::coordinator::backup::create(&path_string, &backup_path).unwrap();
        let report = crate::coordinator::backup::restore(&backup_path, &restore_path).unwrap();
        assert!(report.checksum_verified);
        assert_eq!(report.raw_results, expected_rows);
        assert_eq!(report.current_states, 1);

        let restored = Storage::open(restore_path.to_str().unwrap()).unwrap();
        assert_eq!(restored.count_results().unwrap(), expected_rows);
        let restored_watermarks = restored.digest(0).unwrap();
        assert_eq!(restored_watermarks.checks.len(), watermarks.checks.len());
        assert_eq!(
            restored_watermarks
                .checks
                .iter()
                .map(|c| c.count)
                .sum::<i64>(),
            watermarks.checks.iter().map(|c| c.count).sum::<i64>()
        );
        assert_eq!(
            restored
                .rollup_digest("check_latency_minute_rollups")
                .unwrap()
                .count,
            minute_rollups.count
        );
        assert_eq!(
            restored
                .rollup_digest("check_latency_hour_rollups")
                .unwrap()
                .count,
            hour_rollups.count
        );

        drop(restored);
        let _ = std::fs::remove_file(format!("{path_string}.wal"));
        std::fs::remove_file(&path).unwrap();
        for extra in [
            &backup_path,
            &restore_path,
            &std::path::PathBuf::from(format!("{}.manifest.json", backup_path.display())),
        ] {
            let _ = std::fs::remove_file(extra);
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
        assert_eq!(s.history(cid("a"), 10, None, None, None).unwrap().len(), 2);

        // A batch mixing an old row with a genuinely new one only adds the new one.
        let r3 = result("a", now - 2000, 7);
        let mixed = s
            .insert_results_dedup(&[(r1, Status::Up), (r3, Status::Up)])
            .unwrap();
        assert_eq!(
            mixed, 1,
            "only the row this node did not already have is inserted"
        );
        assert_eq!(s.history(cid("a"), 10, None, None, None).unwrap().len(), 3);
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
        assert_eq!(s.history(cid("x"), 10, None, None, None).unwrap().len(), 2);
    }

    /// The set-based startup baseline must write exactly the spans the per-row
    /// `record_state` loop it replaced did, including for values `from_db`
    /// normalises to `unknown` and for a span already sitting at `since`.
    #[test]
    fn startup_state_baseline_matches_per_check_record_state() {
        const NOW: i64 = 1_000_000;
        // (check, current_state.status as stored, existing spans oldest first)
        type Case<'a> = (&'a str, &'a str, &'a [(&'a str, i64)]);
        let cases: &[Case] = &[
            ("no-history", "down", &[]),
            ("same-state", "up", &[("up", 10)]),
            ("changed", "down", &[("up", 10)]),
            ("older-span-matches", "up", &[("up", 10), ("down", 20)]),
            ("legacy-current-no-history", "paused", &[]),
            (
                "legacy-current-matches-unknown",
                "paused",
                &[("unknown", 10)],
            ),
            (
                "legacy-history-matches-unknown",
                "unknown",
                &[("paused", 10)],
            ),
            ("legacy-history-differs", "up", &[("paused", 10)]),
            ("span-already-at-now", "down", &[("up", NOW)]),
        ];
        let seeded = || {
            let s = Storage::open(":memory:").unwrap();
            for &(check, status, existing) in cases {
                s.conn
                    .execute(
                        "INSERT INTO current_state (check_id, status, last_check) VALUES (?, ?, 0)",
                        duckdb::params![cid(check), status],
                    )
                    .unwrap();
                for &(state, since) in existing {
                    s.conn
                        .execute(
                            "INSERT INTO state_history (check_id, state, since) VALUES (?, ?, ?)",
                            duckdb::params![cid(check), state, since],
                        )
                        .unwrap();
                }
            }
            s
        };
        let spans = |s: &Storage| -> Vec<(String, String, i64)> {
            let mut stmt = s
                .conn
                .prepare("SELECT check_id::VARCHAR, state, since FROM state_history ORDER BY ALL")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap()
        };

        let per_row = seeded();
        for row in per_row.list_current_state().unwrap() {
            per_row.record_state(row.check_id, row.state, NOW).unwrap();
        }
        let set_based = seeded();
        let written = set_based.seed_state_history_baseline(NOW).unwrap();

        assert_eq!(spans(&set_based), spans(&per_row));
        // no-history, changed, older-span-matches, legacy-current-no-history,
        // legacy-history-differs, span-already-at-now.
        assert_eq!(written, 6);
    }

    /// The prune keeps each check's newest span regardless of age, because
    /// that span is not history — it is what the check reads as right now.
    #[test]
    fn state_history_prune_keeps_each_checks_newest_span_however_old() {
        let s = Storage::open(":memory:").unwrap();
        let day = 86_400_000i64;
        let stale = crate::models::new_check_id();
        let straddling = crate::models::new_check_id();
        let single = crate::models::new_check_id();
        // `record_state` only writes a span when the state actually changes,
        // so these alternate.
        for (check, since, state) in [
            (stale, day, NodeState::Up),
            (stale, 2 * day, NodeState::Down),
            (stale, 3 * day, NodeState::Up),
            (straddling, 2 * day, NodeState::Up),
            (straddling, 9 * day, NodeState::Down),
            (straddling, 10 * day, NodeState::Up),
            (single, day, NodeState::Up),
        ] {
            s.record_state(check, state, since).unwrap();
        }
        // Horizon at day 8: `now` is day 15 with a 7-day result window.
        s.prune_metadata(7, 90, 15 * day).unwrap();

        let spans = |check| {
            s.state_spans(check)
                .unwrap()
                .into_iter()
                .map(|span| span.since)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            spans(stale),
            vec![3 * day],
            "every span predates the horizon"
        );
        assert_eq!(spans(straddling), vec![9 * day, 10 * day]);
        assert_eq!(spans(single), vec![day]);
    }

    /// Raw results are the only copy of an observation until a rollup covers
    /// it. Retention deleting on a clock alone trusts a write it never
    /// checked, and the window it would lose is indistinguishable afterwards
    /// from a window in which nothing was measured.
    #[test]
    fn retention_will_not_delete_raw_results_no_rollup_covers() {
        let s = Storage::open(":memory:").unwrap();
        let minute = MILLIS_PER_MINUTE;
        let rows: Vec<_> = (1..=5)
            .map(|i| (result("covered", i * minute, 10), Status::Up))
            .collect();
        s.insert_results_dedup(&rows).unwrap();
        assert_eq!(s.count_results().unwrap(), 5);

        // Simulate the rollup for minute 2 never having been written: a failed
        // write, or a restore that carried raw rows without their summaries.
        s.conn
            .execute(
                "DELETE FROM check_latency_minute_rollups WHERE bucket_start = ?",
                duckdb::params![2 * minute],
            )
            .unwrap();

        // Asked to drop everything below minute 5, retention stops at the gap.
        s.prune_results(5 * minute).unwrap();
        assert_eq!(
            s.count_results().unwrap(),
            4,
            "only the fully summarized minute below the gap may go"
        );

        // Once the summary exists, the same request completes.
        s.conn
            .execute(
                "INSERT INTO check_latency_minute_rollups
                 VALUES (?, 'agent', ?, 1, 1, 10, 10, 10)",
                duckdb::params![cid("covered"), 2 * minute],
            )
            .unwrap();
        s.prune_results(5 * minute).unwrap();
        assert_eq!(s.count_results().unwrap(), 1, "only minute 5 is retained");
    }

    /// Manual benchmark for the state-history prune. Run with
    /// `cargo test benchmark_state_history_prune -- --ignored --nocapture`.
    /// Reports the shipped anti-join against the correlated form it replaced,
    /// on the same seeded table, so the rewrite is justified by measurement.
    #[test]
    #[ignore = "benchmark"]
    fn benchmark_state_history_prune() {
        const CORRELATED: &str = "DELETE FROM state_history
             WHERE since < ?
               AND since <> (SELECT MAX(since) FROM state_history h
                             WHERE h.check_id = state_history.check_id)";
        const ANTI_JOIN: &str = "DELETE FROM state_history
             WHERE since < ?
               AND NOT EXISTS (
                   SELECT 1 FROM (
                       SELECT check_id, MAX(since) AS newest
                       FROM state_history GROUP BY check_id
                   ) latest
                   WHERE latest.check_id = state_history.check_id
                     AND latest.newest = state_history.since)";

        let checks: i64 = std::env::var("LIGHTHOUSE_BENCH_CHECKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);
        let spans: i64 = std::env::var("LIGHTHOUSE_BENCH_SPANS_PER_CHECK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        let horizon = spans / 2 * 60_000;
        let seed = |store: &Storage| {
            store
                .conn
                .execute_batch(&format!(
                    "DELETE FROM state_history;
                     INSERT INTO state_history
                     SELECT (uuid_extract_timestamp(uuid())::VARCHAR || c.i::VARCHAR)::VARCHAR,
                            'up', s.i * 60000
                     FROM range(0, {checks}) c(i), range(0, {spans}) s(i);"
                ))
                .or_else(|_| {
                    store.conn.execute_batch(&format!(
                        "DELETE FROM state_history;
                         INSERT INTO state_history
                         SELECT ('00000000-0000-7000-8000-' || lpad(c.i::VARCHAR, 12, '0'))::UUID,
                                'up', s.i * 60000
                         FROM range(0, {checks}) c(i), range(0, {spans}) s(i);"
                    ))
                })
                .unwrap();
        };

        let mut report = Vec::new();
        for (label, sql) in [("anti-join", ANTI_JOIN), ("correlated", CORRELATED)] {
            let store = Storage::open(":memory:").unwrap();
            seed(&store);
            let start = std::time::Instant::now();
            let removed = store.conn.execute(sql, duckdb::params![horizon]).unwrap();
            report.push((label, removed, start.elapsed()));
        }
        for (label, removed, elapsed) in &report {
            eprintln!(
                "{label}: pruned {removed} of {} rows in {elapsed:?}",
                checks * spans
            );
        }
        assert_eq!(
            report[0].1, report[1].1,
            "both forms must remove exactly the same rows"
        );
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
        let page = s.results_since(cid("a"), 1000, None, 5000, 10).unwrap();
        assert_eq!(page.len(), 1, "since is exclusive of the cursor");
        assert_eq!(page[0].0.timestamp.timestamp_millis(), 2000);
        assert_eq!(page[0].1, Status::Down);
        // Fields never persisted in `check_results` read back empty/None
        // rather than being invented.
        assert_eq!(page[0].0.body, None);
        assert_eq!(page[0].0.headers, None);

        let mut same_ts = result("a", 2000, 3);
        same_ts.agent_id = "z-agent".into();
        s.insert_results(&[(same_ts, Status::Up)]).unwrap();
        let page = s
            .results_since(cid("a"), 2000, Some("agent"), 5000, 10)
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0.agent_id, "z-agent");

        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            2000,
            None,
        )
        .unwrap();
        let alerts = s.alerts_since(1000, "", 10).unwrap();
        assert_eq!(alerts.len(), 1);

        s.create_incident(
            "inc-1",
            "t",
            IncidentState::Detected,
            2000,
            false,
            Some(cid("a")),
            &[IncidentCheckRow {
                check_id: cid("a"),
                source: AffectSource::Origin,
            }],
            None,
        )
        .unwrap();
        let incidents = s.incidents_since(1000, "", 10).unwrap();
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].id, "inc-1");
    }

    #[test]
    fn result_backfill_pages_all_rows_sharing_a_timestamp() {
        let s = Storage::open(":memory:").unwrap();
        let rows = (0..501)
            .map(|i| {
                let mut row = result("a", 2000, i);
                row.agent_id = format!("agent-{i:04}");
                (row, Status::Up)
            })
            .collect::<Vec<_>>();
        s.insert_results(&rows).unwrap();

        let first = s.results_since(cid("a"), 1000, None, 2000, 500).unwrap();
        assert_eq!(first.len(), 500);
        let last = first.last().unwrap();
        let second = s
            .results_since(
                cid("a"),
                last.0.timestamp.timestamp_millis(),
                Some(&last.0.agent_id),
                2000,
                500,
            )
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].0.agent_id, "agent-0500");
    }

    #[test]
    fn global_result_backfill_cursor_pages_shared_timestamp_across_checks() {
        let s = Storage::open(":memory:").unwrap();
        let rows = (0..501)
            .map(|i| {
                let mut row = result(&format!("check-{i:04}"), 2_000, i);
                row.agent_id = "same-agent".into();
                (row, Status::Up)
            })
            .collect::<Vec<_>>();
        s.insert_results(&rows).unwrap();

        let first = s.results_after(i64::MIN, None, None, 500).unwrap();
        assert_eq!(first.len(), 500);
        let cursor = &first[499].0;
        let second = s
            .results_after(
                cursor.timestamp.timestamp_millis(),
                Some(cursor.check_id),
                Some(&cursor.agent_id),
                500,
            )
            .unwrap();
        assert_eq!(second.len(), 1);
        assert!(first
            .iter()
            .all(|(row, _)| row.timestamp.timestamp_millis() == 2_000));
        assert_eq!(second[0].0.timestamp.timestamp_millis(), 2_000);
        assert_ne!(first.last().unwrap().0.check_id, second[0].0.check_id);
    }

    #[test]
    #[ignore = "capacity baseline; configure LIGHTHOUSE_BENCH_DEDUP_ROWS for the intended scale"]
    fn benchmark_replicated_dedup_against_retained_history() {
        let retained: u64 = std::env::var("LIGHTHOUSE_BENCH_DEDUP_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000_000);
        let path =
            std::env::temp_dir().join(format!("lighthouse-dedup-{}.duckdb", uuid::Uuid::now_v7()));
        let store = Storage::open(path.to_str().unwrap()).unwrap();
        let check_key = "dedup-scaling-check";
        let check = cid(check_key);

        // Rows land in time order, as live ingest writes them.
        let seeded = std::time::Instant::now();
        store
            .conn
            .execute_batch(&format!(
                "INSERT INTO check_results
                 SELECT '{check}'::UUID, 'agent-a', i, 'up', 10, 200, NULL, 'z', 'r', NULL
                 FROM generate_series(1, {retained}) t(i);"
            ))
            .unwrap();
        // Seeded straight into the table, so restore the invariant every real
        // writer maintains: the benchmark measures dedup, not a corrupt store.
        store.rebuild_result_watermarks().unwrap();
        store.rebuild_latest_check_results().unwrap();
        let seed_elapsed = seeded.elapsed();

        // A live batch: 500 fresh rows, all newer than everything retained.
        let rows: Vec<(CheckResult, Status)> = (0..500)
            .map(|i| {
                let mut r = result(check_key, retained as i64 + i, 10);
                r.agent_id = "agent-b".into();
                (r, Status::Up)
            })
            .collect();

        let mut samples = Vec::new();
        for round in 0..5 {
            let rows: Vec<(CheckResult, Status)> = rows
                .iter()
                .map(|(r, s)| {
                    let mut r = r.clone();
                    r.timestamp = chrono::DateTime::from_timestamp_millis(
                        r.timestamp.timestamp_millis() + round * 1_000,
                    )
                    .unwrap();
                    (r, *s)
                })
                .collect();
            let started = std::time::Instant::now();
            let inserted = store.insert_results_dedup(&rows).unwrap();
            samples.push(started.elapsed());
            assert_eq!(inserted, 500);
        }
        samples.sort();
        let p50 = samples[samples.len() / 2];
        let max = *samples.last().unwrap();

        // A replay of the same batch must still be recognised as a replay:
        // the bound narrows the probe, it must not lose a duplicate.
        let replay = store.insert_results_dedup(&rows).unwrap();

        println!(
            "replicated_dedup retained_rows={retained} seed_ms={} batch_rows=500 p50_ms={} max_ms={} replay_inserted={replay}",
            seed_elapsed.as_millis(),
            p50.as_millis(),
            max.as_millis()
        );
        assert_eq!(replay, 0, "an exact replay must insert nothing");
        let bounded = max.as_millis() <= 500;
        drop(store);
        let _ = std::fs::remove_file(&path);
        assert!(
            bounded,
            "a 500-row batch took {max:?} against {retained} retained rows; \
             dedup is scaling with history rather than with the batch"
        );
    }

    #[test]
    #[ignore = "capacity baseline; configure LIGHTHOUSE_BENCH_* for the intended scale"]
    fn benchmark_representative_storage_capacity_baseline() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn env_u128(name: &str) -> Option<u128> {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
        }
        fn percentile(samples: &mut [std::time::Duration], percentile: usize) -> u128 {
            samples.sort_unstable();
            samples[(samples.len() * percentile / 100).min(samples.len() - 1)].as_millis()
        }
        fn bytes(path: &std::path::Path) -> u64 {
            std::fs::metadata(path)
                .map(|metadata| metadata.len())
                .unwrap_or(0)
        }

        let check_count = env_u64("LIGHTHOUSE_BENCH_CHECKS", 100);
        let days = env_u64("LIGHTHOUSE_BENCH_DAYS", 2);
        let observation_secs = env_u64("LIGHTHOUSE_BENCH_OBSERVATION_SECS", 30);
        let domains = env_u64("LIGHTHOUSE_BENCH_DOMAINS", 1);
        assert!(check_count > 0 && days > 0 && observation_secs > 0 && domains > 0);
        let observations_per_check = days * 86_400 / observation_secs;
        let rows = check_count * observations_per_check * domains;
        let path = std::env::temp_dir().join(format!(
            "lighthouse-capacity-bench-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
        let backup_path = path.with_extension("backup.duckdb");
        let path_string = path.to_string_lossy().into_owned();
        let store = Storage::open(&path_string).unwrap();
        let now = days as i64 * 86_400_000;
        let step_ms = observation_secs as i64 * 1_000;

        let seed_started = std::time::Instant::now();
        store
            .conn
            .execute_batch(&format!(
                "INSERT INTO check_results
                 SELECT md5('check-' || check_number::VARCHAR)::UUID,
                        'agent-' || domain_number::VARCHAR || '-' || (check_number % 3)::VARCHAR,
                        observation_number * {step_ms} + domain_number,
                        CASE WHEN (observation_number + check_number * 17 + domain_number * 31) % 97 = 0 THEN 'down' ELSE 'up' END,
                        15 + ((observation_number * 37 + check_number * 13 + domain_number * 19) % 2_000),
                        CASE WHEN (observation_number + check_number * 17 + domain_number * 31) % 97 = 0 THEN 503 ELSE 200 + ((check_number + domain_number) % 5) END,
                        CASE WHEN (observation_number + check_number * 17 + domain_number * 31) % 97 = 0 THEN 'timeout-' || check_number::VARCHAR || '-' || observation_number::VARCHAR ELSE NULL END,
                        CASE WHEN (observation_number + check_number * 17 + domain_number * 31) % 97 = 0 THEN '\"timeout\"' ELSE NULL END,
                        'zone-' || domain_number::VARCHAR,
                        'region-' || domain_number::VARCHAR
                 FROM range({check_count}) checks(check_number)
                 CROSS JOIN range({observations_per_check}) observations(observation_number)
                 CROSS JOIN range({domains}) domain(domain_number);"
            ))
            .unwrap();
        store.rebuild_result_watermarks().unwrap();
        store.rebuild_latest_check_results().unwrap();
        // Retention deletes only what rollups already summarize, so the
        // seeded raw rows need their rollups for prune to exercise anything.
        store.rebuild_latency_rollups().unwrap();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let seed_ms = seed_started.elapsed().as_millis();
        assert_eq!(store.count_results().unwrap(), rows);
        let database_before_prune = bytes(&path);
        let wal_before_prune = bytes(&wal_path);

        drop(store);
        let backup_started = std::time::Instant::now();
        std::fs::copy(&path, &backup_path).unwrap();
        let backup_ms = backup_started.elapsed().as_millis();
        let backup_bytes = bytes(&backup_path);
        let restore_started = std::time::Instant::now();
        let restored = Storage::open(backup_path.to_str().unwrap()).unwrap();
        assert_eq!(restored.count_results().unwrap(), rows);
        let restore_ms = restore_started.elapsed().as_millis();
        drop(restored);
        let store = Storage::open(&path_string).unwrap();

        let check_id: CheckId = store
            .conn
            .query_row("SELECT check_id FROM check_results LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let mut history_samples = Vec::new();
        let mut latency_samples = Vec::new();
        for _ in 0..10 {
            let started = std::time::Instant::now();
            let history = store
                .history(check_id, 100, Some(0), Some(now), None)
                .unwrap();
            history_samples.push(started.elapsed());
            assert_eq!(history.len(), 100);
            let started = std::time::Instant::now();
            let series = store.latency_series(check_id, 0, now, 120).unwrap();
            latency_samples.push(started.elapsed());
            assert!(series.overall.attempts > 0);
        }

        let prune_started = std::time::Instant::now();
        let pruned = store.prune((days - 1).max(1), 90, now).unwrap();
        let prune_ms = prune_started.elapsed().as_millis();
        assert!(pruned > 0, "benchmark must exercise raw retention");
        // Measured before any CHECKPOINT runs, so this answers the actual
        // question behind TODO's "add checkpoint maintenance if benchmarks
        // show it is needed": does a delete alone ever shrink the file, or is
        // an explicit checkpoint the only thing that can?
        let database_after_prune_no_checkpoint = bytes(&path);
        let wal_after_prune_no_checkpoint = bytes(&wal_path);
        let checkpoint_started = std::time::Instant::now();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let checkpoint_ms = checkpoint_started.elapsed().as_millis();
        let database_after_prune = bytes(&path);
        let wal_after_prune = bytes(&wal_path);

        let history_p95_ms = percentile(&mut history_samples, 95);
        let latency_p95_ms = percentile(&mut latency_samples, 95);
        println!(
            "checks={check_count} domains={domains} retained_rows={rows} observation_secs={observation_secs} seed_ms={seed_ms} database_before_prune_bytes={database_before_prune} wal_before_prune_bytes={wal_before_prune} backup_bytes={backup_bytes} backup_ms={backup_ms} restore_ms={restore_ms} history_p95_ms={history_p95_ms} latency_p95_ms={latency_p95_ms} pruned_rows={pruned} prune_ms={prune_ms} database_after_prune_no_checkpoint_bytes={database_after_prune_no_checkpoint} wal_after_prune_no_checkpoint={wal_after_prune_no_checkpoint} checkpoint_ms={checkpoint_ms} database_after_prune_bytes={database_after_prune} wal_after_prune_bytes={wal_after_prune}"
        );

        for (name, value) in [
            ("LIGHTHOUSE_BENCH_MAX_HISTORY_P95_MS", history_p95_ms),
            ("LIGHTHOUSE_BENCH_MAX_LATENCY_P95_MS", latency_p95_ms),
            ("LIGHTHOUSE_BENCH_MAX_PRUNE_MS", prune_ms),
            ("LIGHTHOUSE_BENCH_MAX_CHECKPOINT_MS", checkpoint_ms),
            ("LIGHTHOUSE_BENCH_MAX_BACKUP_MS", backup_ms),
            ("LIGHTHOUSE_BENCH_MAX_RESTORE_MS", restore_ms),
            (
                "LIGHTHOUSE_BENCH_MAX_DATABASE_BYTES",
                database_before_prune as u128,
            ),
        ] {
            if let Some(limit) = env_u128(name) {
                assert!(value <= limit, "{name}: measured {value}, limit {limit}");
            }
        }

        drop(store);
        let _ = std::fs::remove_file(wal_path);
        std::fs::remove_file(backup_path).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    /// TODO.md: "add explicit checkpoint/file-size maintenance after large
    /// pruning operations if benchmarks show it is needed." It is not
    /// needed, and this benchmark is why.
    ///
    /// Two things have to both be true for an explicit `CHECKPOINT` after
    /// `prune_in_slices` to earn its cost: DuckDB has to leave meaningful
    /// work undone until asked, and doing that work has to shrink the file.
    /// Neither holds. `wal_autocheckpoint` is pinned high here specifically
    /// to *stop* DuckDB self-checkpointing mid-prune, so the work an explicit
    /// call would do is visible in isolation rather than already applied —
    /// with the pragma left at its default, a prune this size crosses
    /// DuckDB's own threshold before `prune_results` even returns, and a
    /// caller-issued CHECKPOINT afterward finds nothing left to flush. Run
    /// with `LIGHTHOUSE_BENCH_CHECKS`/`LIGHTHOUSE_BENCH_DAYS`/
    /// `LIGHTHOUSE_BENCH_OBSERVATION_SECS` for other scales; the numbers recorded
    /// in TODO.md/SCALING.md are the defaults below.
    #[test]
    #[ignore = "capacity baseline; configure LIGHTHOUSE_BENCH_* for the intended scale"]
    fn benchmark_checkpoint_after_prune() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn bytes(path: &std::path::Path) -> u64 {
            std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
        }

        let check_count = env_u64("LIGHTHOUSE_BENCH_CHECKS", 1_000);
        let days = env_u64("LIGHTHOUSE_BENCH_DAYS", 30);
        let observation_secs = env_u64("LIGHTHOUSE_BENCH_OBSERVATION_SECS", 30);
        let observations_per_check = days * 86_400 / observation_secs;
        let step_ms = observation_secs as i64 * 1_000;
        let path = std::env::temp_dir().join(format!(
            "lighthouse-checkpoint-bench-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
        let store = Storage::open(path.to_str().unwrap()).unwrap();

        store
            .conn
            .execute_batch(&format!(
                "INSERT INTO check_results
                 SELECT md5('check-' || check_number::VARCHAR)::UUID,
                        'agent-' || (check_number % 3)::VARCHAR,
                        observation_number * {step_ms},
                        'up', 15, 200, NULL, 'zone', 'region', NULL
                 FROM range({check_count}) checks(check_number)
                 CROSS JOIN range({observations_per_check}) observations(observation_number);"
            ))
            .unwrap();
        store.rebuild_result_watermarks().unwrap();
        store.rebuild_latest_check_results().unwrap();
        // Real ingest maintains rollups incrementally with every write; this
        // bulk seed does not, so backfill once here rather than have
        // `prune_results` hold the horizon back for a gap that only exists
        // because of how the benchmark seeds data.
        store.rebuild_latency_rollups().unwrap();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let seeded_bytes = bytes(&path);

        // Hold DuckDB's own automatic checkpoint off so the prune's WAL
        // growth and an explicit CHECKPOINT's cost are each visible on their
        // own, instead of the two being indistinguishable the way they are
        // with the default threshold (see the doc comment above).
        store
            .conn
            .execute_batch("PRAGMA wal_autocheckpoint='1TB';")
            .unwrap();

        let cutoff = step_ms * (observations_per_check as i64 / 30);
        let prune_started = std::time::Instant::now();
        let removed = store.prune_results(cutoff).unwrap();
        let prune_ms = prune_started.elapsed().as_millis();
        assert!(removed > 0, "benchmark must exercise raw retention");
        let database_after_prune_bytes = bytes(&path);
        let wal_after_prune_bytes = bytes(&wal_path);

        let checkpoint_started = std::time::Instant::now();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let checkpoint_ms = checkpoint_started.elapsed().as_millis();
        let database_after_checkpoint_bytes = bytes(&path);

        // A second CHECKPOINT with nothing pending: if the file were ever
        // going to shrink from reclaiming the deleted rows' space, one of
        // these two calls would have done it.
        let second_checkpoint_started = std::time::Instant::now();
        store.conn.execute_batch("CHECKPOINT").unwrap();
        let second_checkpoint_ms = second_checkpoint_started.elapsed().as_millis();
        let database_after_second_checkpoint_bytes = bytes(&path);

        println!(
            "checks={check_count} days={days} observation_secs={observation_secs} removed_rows={removed} seeded_bytes={seeded_bytes} prune_ms={prune_ms} database_after_prune_bytes={database_after_prune_bytes} wal_after_prune_bytes={wal_after_prune_bytes} checkpoint_ms={checkpoint_ms} database_after_checkpoint_bytes={database_after_checkpoint_bytes} second_checkpoint_ms={second_checkpoint_ms} database_after_second_checkpoint_bytes={database_after_second_checkpoint_bytes}"
        );

        // The file must never shrink by itself, or this benchmark's own
        // premise — that CHECKPOINT does not reclaim space — is wrong and
        // needs to be re-examined before touching the prune path.
        assert!(
            database_after_checkpoint_bytes >= database_after_prune_bytes,
            "a checkpoint should never shrink the file if this decision is right"
        );
        assert_eq!(
            database_after_second_checkpoint_bytes, database_after_checkpoint_bytes,
            "a checkpoint with nothing pending must be a byte-for-byte no-op"
        );

        drop(store);
        let _ = std::fs::remove_file(&wal_path);
        let _ = std::fs::remove_file(&path);
    }

    /// TODO.md: "add restart/recovery tests using a persistent
    /// production-sized database." Production-scale sibling of
    /// `restart_recovers_committed_rows_and_backup_restore_preserves_them`
    /// above; that test is the fast, always-on regression cover for the same
    /// three invariants — (a) recovery duration, (b) no data loss of
    /// committed rows, (c) a backup/restore round trip preserving row
    /// counts, watermarks, and rollups — at a scale CI cannot afford to run
    /// on every commit. Configure `LIGHTHOUSE_BENCH_CHECKS`/`LIGHTHOUSE_BENCH_DAYS`/
    /// `LIGHTHOUSE_BENCH_OBSERVATION_SECS` for the intended scale; see that test's
    /// doc comment for what "closed uncleanly" does and does not simulate
    /// here.
    #[test]
    #[ignore = "capacity baseline; configure LIGHTHOUSE_BENCH_* for the intended scale"]
    fn benchmark_restart_recovery_at_production_scale() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn env_u128(name: &str) -> Option<u128> {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
        }

        let check_count = env_u64("LIGHTHOUSE_BENCH_CHECKS", 1_000);
        let days = env_u64("LIGHTHOUSE_BENCH_DAYS", 30);
        let observation_secs = env_u64("LIGHTHOUSE_BENCH_OBSERVATION_SECS", 30);
        let observations_per_check = days * 86_400 / observation_secs;
        let step_ms = observation_secs as i64 * 1_000;
        let expected_rows = check_count * observations_per_check;
        let path = std::env::temp_dir().join(format!(
            "lighthouse-restart-bench-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path_string = path.to_string_lossy().into_owned();

        {
            let store = Storage::open(&path_string).unwrap();
            store
                .conn
                .execute_batch(&format!(
                    "INSERT INTO check_results
                     SELECT md5('check-' || check_number::VARCHAR)::UUID,
                            'agent-' || (check_number % 3)::VARCHAR,
                            observation_number * {step_ms},
                            'up', 15, 200, NULL, 'zone', 'region', NULL
                     FROM range({check_count}) checks(check_number)
                     CROSS JOIN range({observations_per_check}) observations(observation_number);"
                ))
                .unwrap();
            store.rebuild_result_watermarks().unwrap();
            store.rebuild_latest_check_results().unwrap();
            store.rebuild_latency_rollups().unwrap();
            // No checkpoint, no clean drop: every one of these rows has to be
            // recovered from the WAL, the way a killed process would leave it.
            std::mem::forget(store);
        }

        let recovery_started = std::time::Instant::now();
        let store = Storage::open(&path_string).unwrap();
        let recovery_ms = recovery_started.elapsed().as_millis();
        let recovered_rows = store.count_results().unwrap();
        assert_eq!(
            recovered_rows, expected_rows,
            "committed rows must not be lost across an unclean close"
        );
        let watermarks = store.digest(0).unwrap();
        let minute_rollups = store.rollup_digest("check_latency_minute_rollups").unwrap();
        let hour_rollups = store.rollup_digest("check_latency_hour_rollups").unwrap();

        let backup_path = path.with_extension("backup.duckdb");
        let restore_path = path.with_extension("restore.duckdb");
        let backup_started = std::time::Instant::now();
        crate::coordinator::backup::create(&path_string, &backup_path).unwrap();
        let backup_ms = backup_started.elapsed().as_millis();
        let restore_started = std::time::Instant::now();
        let report = crate::coordinator::backup::restore(&backup_path, &restore_path).unwrap();
        let restore_ms = restore_started.elapsed().as_millis();
        assert!(report.checksum_verified);
        assert_eq!(report.raw_results, expected_rows);

        let restored = Storage::open(restore_path.to_str().unwrap()).unwrap();
        assert_eq!(restored.count_results().unwrap(), expected_rows);
        let restored_watermarks = restored.digest(0).unwrap();
        assert_eq!(restored_watermarks.checks.len(), watermarks.checks.len());
        assert_eq!(
            restored_watermarks
                .checks
                .iter()
                .map(|c| c.count)
                .sum::<i64>(),
            watermarks.checks.iter().map(|c| c.count).sum::<i64>(),
            "backup/restore must preserve replication watermarks exactly"
        );
        assert_eq!(
            restored
                .rollup_digest("check_latency_minute_rollups")
                .unwrap()
                .count,
            minute_rollups.count,
            "backup/restore must preserve minute rollups exactly"
        );
        assert_eq!(
            restored
                .rollup_digest("check_latency_hour_rollups")
                .unwrap()
                .count,
            hour_rollups.count,
            "backup/restore must preserve hour rollups exactly"
        );

        println!(
            "checks={check_count} days={days} observation_secs={observation_secs} rows={expected_rows} recovery_ms={recovery_ms} backup_ms={backup_ms} restore_ms={restore_ms} minute_rollups={} hour_rollups={}",
            minute_rollups.count, hour_rollups.count
        );
        for (name, value) in [
            ("LIGHTHOUSE_BENCH_MAX_RECOVERY_MS", recovery_ms),
            ("LIGHTHOUSE_BENCH_MAX_BACKUP_MS", backup_ms),
            ("LIGHTHOUSE_BENCH_MAX_RESTORE_MS", restore_ms),
        ] {
            if let Some(limit) = env_u128(name) {
                assert!(value <= limit, "{name}: measured {value}, limit {limit}");
            }
        }

        drop(store);
        drop(restored);
        let _ = std::fs::remove_file(format!("{path_string}.wal"));
        let _ = std::fs::remove_file(&path);
        for extra in [
            &backup_path,
            &restore_path,
            &std::path::PathBuf::from(format!("{}.manifest.json", backup_path.display())),
        ] {
            let _ = std::fs::remove_file(extra);
        }
    }

    #[test]
    fn segment_count_is_constant_and_width_follows_the_window() {
        // The strip keeps the same number of bars at every range; a shorter
        // window just means finer segments. 7 days over 90 segments is ~1h52m.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        const DAY: i64 = 86_400_000;

        for days in [7usize, 30, 90] {
            let b = s.uptime_buckets(cid("x"), days, 90, now, false).unwrap();
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
    fn raw_results_create_counts_but_not_uptime_data() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        s.insert_results(&[up_at("x", now - 1)]).unwrap();

        let buckets = s.uptime_buckets(cid("x"), 7, 90, now, false).unwrap();
        assert!(buckets.iter().all(|bucket| bucket.uptime.is_none()));
        assert_eq!(buckets.iter().map(|bucket| bucket.total).sum::<u64>(), 1);
        assert_eq!(buckets.iter().map(|bucket| bucket.up).sum::<u64>(), 1);
    }

    #[test]
    fn agent_leaf_topology_is_persisted_monotonically() {
        let s = Storage::open(":memory:").unwrap();
        s.update_agent_check_state(
            cid("svc"),
            "agent-a",
            "zone-a",
            "region-a",
            Status::Up,
            0,
            2_000,
        )
        .unwrap();
        // A delayed report must not rewind either status/timestamp or topology.
        s.update_agent_check_state(
            cid("svc"),
            "agent-a",
            "old-zone",
            "old-region",
            Status::Down,
            3,
            1_000,
        )
        .unwrap();

        assert_eq!(
            s.list_agent_check_state().unwrap(),
            vec![AgentCheckStateRow {
                check_id: cid("svc"),
                agent_id: "agent-a".into(),
                zone: "zone-a".into(),
                region: "region-a".into(),
                status: Status::Up,
                consecutive_failures: 0,
                last_check: 2_000,
            }]
        );
    }

    /// TODO.md: "evaluate an index or storage strategy for
    /// `check_results(check_id, ts_millis)`." This is the measurement that
    /// answers it, not an assumption either way.
    ///
    /// `check_results` has no index today beyond DuckDB's automatic per-row-group
    /// zone maps: `ts_millis` is monotonically appended, so a time-bounded scan
    /// already skips most row groups for free, but `check_id` has no locality —
    /// every ingest batch interleaves every check — so a single-check read has
    /// to evaluate `check_id = ?` against every row in whatever `ts_millis`
    /// window survives pruning. An ART index on `(check_id, ts_millis)` is the
    /// textbook fix for exactly that access pattern (`Storage::history`,
    /// `Storage::latency_series`'s per-check filter). The question is whether it
    /// is worth what DuckDB's own docs warn it costs: every insert into an
    /// indexed table pays ART maintenance, and an index the buffer manager
    /// cannot evict grows the resident memory floor for as long as any part of
    /// it has been touched.
    ///
    /// Seeds the same representative shape as
    /// `benchmark_representative_storage_capacity_baseline` twice — once with
    /// the candidate index created before any data lands (the worst case: every
    /// row ever written pays the maintenance cost, matching what putting it in
    /// `Storage::open`'s DDL would mean), once without — and reports the read
    /// and write sides side by side. Run with `LIGHTHOUSE_BENCH_CHECKS`/
    /// `LIGHTHOUSE_BENCH_DAYS`/`LIGHTHOUSE_BENCH_OBSERVATION_SECS`/
    /// `LIGHTHOUSE_BENCH_INGEST_ROUNDS` for other scales; the numbers recorded in
    /// SCALING.md are the defaults below.
    #[test]
    #[ignore = "index tradeoff benchmark; configure LIGHTHOUSE_BENCH_* for the intended scale"]
    fn benchmark_check_results_check_id_ts_index_tradeoff() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn percentile(samples: &mut [std::time::Duration], percentile: usize) -> u128 {
            samples.sort_unstable();
            samples[(samples.len() * percentile / 100).min(samples.len() - 1)].as_millis()
        }
        fn bytes(path: &std::path::Path) -> u64 {
            std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
        }

        let check_count = env_u64("LIGHTHOUSE_BENCH_CHECKS", 1_000);
        let days = env_u64("LIGHTHOUSE_BENCH_DAYS", 7);
        let observation_secs = env_u64("LIGHTHOUSE_BENCH_OBSERVATION_SECS", 30);
        let ingest_rounds = env_u64("LIGHTHOUSE_BENCH_INGEST_ROUNDS", 20);
        let observations_per_check = days * 86_400 / observation_secs;
        let rows = check_count * observations_per_check;
        let step_ms = observation_secs as i64 * 1_000;
        let now = days as i64 * 86_400_000;

        struct Report {
            label: &'static str,
            seed_ms: u128,
            database_bytes: u64,
            reopen_ms: u128,
            history_p50_ms: u128,
            history_p95_ms: u128,
            latency_p50_ms: u128,
            latency_p95_ms: u128,
            ingest_rows_per_sec: f64,
        }
        let mut reports = Vec::new();

        for (label, with_index) in [("no_index", false), ("with_index", true)] {
            let path = std::env::temp_dir().join(format!(
                "lighthouse-check-results-index-bench-{label}-{}-{}.duckdb",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
            let path_string = path.to_string_lossy().into_owned();
            let store = Storage::open(&path_string).unwrap();
            if with_index {
                // Created immediately after the table, before any row lands —
                // the same position a DDL-resident index would occupy in
                // `Storage::open`, so seeding below pays maintenance on every
                // row rather than a one-time bulk-build at the end.
                store
                    .conn
                    .execute_batch(
                        "CREATE INDEX bench_check_results_check_ts
                         ON check_results (check_id, ts_millis)",
                    )
                    .unwrap();
            }

            let seed_started = std::time::Instant::now();
            store
                .conn
                .execute_batch(&format!(
                    "INSERT INTO check_results
                     SELECT md5('check-' || check_number::VARCHAR)::UUID,
                            'agent-' || (check_number % 3)::VARCHAR,
                            observation_number * {step_ms},
                            CASE WHEN (observation_number + check_number * 17) % 97 = 0
                                 THEN 'down' ELSE 'up' END,
                            15 + ((observation_number * 37 + check_number * 13) % 2_000),
                            CASE WHEN (observation_number + check_number * 17) % 97 = 0
                                 THEN 503 ELSE 200 END,
                            NULL, NULL, 'zone', 'region'
                     FROM range({check_count}) checks(check_number)
                     CROSS JOIN range({observations_per_check}) observations(observation_number);"
                ))
                .unwrap();
            store.rebuild_result_watermarks().unwrap();
            store.rebuild_latest_check_results().unwrap();
            // This bulk seed bypasses ordinary ingest, so latency rollups are
            // backfilled here for the same reason `benchmark_checkpoint_after_prune`
            // does it: leaving them empty would make the reopen below trigger
            // `Storage::open`'s one-time rollup backfill instead of measuring
            // an ordinary restart.
            store.rebuild_latency_rollups().unwrap();
            store.conn.execute_batch("CHECKPOINT").unwrap();
            let seed_ms = seed_started.elapsed().as_millis();
            assert_eq!(store.count_results().unwrap(), rows);
            let database_bytes = bytes(&path);
            drop(store);

            // Reopen: what an index the docs call lazily-deserialized should
            // cost at startup is "nothing more than the base table load" — this
            // is the number that confirms or refutes that for the bundled
            // DuckDB build rather than trusting the docs unverified.
            let reopen_started = std::time::Instant::now();
            let store = Storage::open(&path_string).unwrap();
            let reopen_ms = reopen_started.elapsed().as_millis();

            let check_ids: Vec<CheckId> = {
                let mut stmt = store
                    .conn
                    .prepare("SELECT DISTINCT check_id FROM check_results")
                    .unwrap();
                stmt.query_map([], |r| r.get::<_, CheckId>(0))
                    .unwrap()
                    .collect::<std::result::Result<_, _>>()
                    .unwrap()
            };
            assert_eq!(check_ids.len() as u64, check_count);
            let hot_check_id = check_ids[0];

            let mut history_samples = Vec::new();
            let mut latency_samples = Vec::new();
            for _ in 0..20 {
                let started = std::time::Instant::now();
                let history = store
                    .history(hot_check_id, 100, Some(0), Some(now), None)
                    .unwrap();
                history_samples.push(started.elapsed());
                assert_eq!(history.len(), 100);
                let started = std::time::Instant::now();
                let series = store.latency_series(hot_check_id, 0, now, 120).unwrap();
                latency_samples.push(started.elapsed());
                assert!(series.overall.attempts > 0);
            }

            // Sustained ingest landing on top of the seeded backlog: the shape
            // that shows an index's steady-state per-row write cost, as
            // opposed to seed_ms above which mixes that cost with the one-time
            // bulk build. Every batch reuses the same check ids the backlog
            // already holds millions of rows for, so this is an insert into an
            // ART node that is already large, not an empty one.
            let ingest_started = std::time::Instant::now();
            for round in 0..ingest_rounds {
                let ts = now + (round as i64 + 1) * step_ms;
                let batch: Vec<(CheckResult, Status)> = check_ids
                    .iter()
                    .map(|&check_id| {
                        (
                            CheckResult {
                                zone: "zone".into(),
                                region: "region".into(),
                                body: None,
                                headers: None,
                                check_id,
                                agent_id: "agent-0".into(),
                                kind: CheckKind::Http,
                                timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
                                latency_ms: 20,
                                status_code: Some(200),
                                body_matched: None,
                                error: None,
                                failure: None,
                                metric: None,
                                metrics: None,
                            },
                            Status::Up,
                        )
                    })
                    .collect();
                store.persist_ingest(&batch, &[], &[]).unwrap();
            }
            let ingest_elapsed = ingest_started.elapsed();
            let ingest_rows_per_sec =
                (check_count * ingest_rounds) as f64 / ingest_elapsed.as_secs_f64();

            reports.push(Report {
                label,
                seed_ms,
                database_bytes,
                reopen_ms,
                history_p50_ms: percentile(&mut history_samples, 50),
                history_p95_ms: percentile(&mut history_samples, 95),
                latency_p50_ms: percentile(&mut latency_samples, 50),
                latency_p95_ms: percentile(&mut latency_samples, 95),
                ingest_rows_per_sec,
            });

            drop(store);
            let _ = std::fs::remove_file(wal_path);
            let _ = std::fs::remove_file(path);
        }

        eprintln!(
            "checks={check_count} days={days} observation_secs={observation_secs} \
             retained_rows={rows} ingest_rounds={ingest_rounds}"
        );
        for r in &reports {
            eprintln!(
                "{label}: seed_ms={seed_ms} database_bytes={database_bytes} reopen_ms={reopen_ms} \
                 history_p50_ms={history_p50_ms} history_p95_ms={history_p95_ms} \
                 latency_p50_ms={latency_p50_ms} latency_p95_ms={latency_p95_ms} \
                 ingest_rows_per_sec={ingest_rows_per_sec:.0}",
                label = r.label,
                seed_ms = r.seed_ms,
                database_bytes = r.database_bytes,
                reopen_ms = r.reopen_ms,
                history_p50_ms = r.history_p50_ms,
                history_p95_ms = r.history_p95_ms,
                latency_p50_ms = r.latency_p50_ms,
                latency_p95_ms = r.latency_p95_ms,
                ingest_rows_per_sec = r.ingest_rows_per_sec,
            );
        }
    }
}
