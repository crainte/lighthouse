use super::*;

/// One check's per-segment totals while folding the aggregated rows DuckDB
/// returned. Parallel vectors rather than a `Vec` of structs because every
/// segment exists whether or not a row landed in it, and the empty ones are the
/// common case on a sparse window.
struct SegmentCounts {
    sum_ms: Vec<f64>,
    attempts: Vec<u32>,
    up: Vec<u32>,
}

impl SegmentCounts {
    fn new(buckets: usize) -> Self {
        Self {
            sum_ms: vec![0.0; buckets],
            attempts: vec![0; buckets],
            up: vec![0; buckets],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LatencyPoint {
    pub timestamp: i64, // unix millis of the window start
    /// Every probe outcome in the segment. This is distinct from `up`: failed
    /// attempts establish that the check was observed, but do not measure its
    /// response latency.
    pub attempts: u32,
    /// Successful probe outcomes in the segment.
    pub up: u32,
    /// Mean latency across successful attempts, or `None` when none passed.
    /// A segment with no successful attempts is not a 0ms response, and
    /// collapsing the two paints an imaginary flat line along the axis.
    pub avg_latency_ms: Option<f64>,
}

/// Reliability and latency spread over one window.
///
/// `min`/`avg`/`max` cover *successful* attempts only: a failed probe's
/// recorded duration is however long it took to give up, not a measurement of
/// the service, and letting timeouts in makes the spread meaningless.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LatencyBand {
    pub attempts: u32,
    pub up: u32,
    pub min_ms: Option<u64>,
    pub max_ms: Option<u64>,
    sum_ms: f64,
}

impl LatencyBand {
    fn merge(&mut self, other: &Self) {
        self.attempts += other.attempts;
        self.up += other.up;
        self.sum_ms += other.sum_ms;
        self.min_ms = match (self.min_ms, other.min_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_ms = self.max_ms.max(other.max_ms);
    }

    /// Mean latency of the successful attempts, or `None` when none passed.
    pub fn avg_ms(&self) -> Option<f64> {
        (self.up > 0).then(|| self.sum_ms / self.up as f64)
    }
}

/// One agent's share of a window. Several agents probe the same check, so
/// "which agent was failing?" is only answerable per agent.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentBand {
    pub agent_id: String,
    pub band: LatencyBand,
}

/// Latency shape over a window plus exact per-agent attribution for it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LatencySeries {
    pub points: Vec<LatencyPoint>,
    pub overall: LatencyBand,
    pub agents: Vec<AgentBand>,
}

/// A rollup table's contribution to a digest.
///
/// `StreamDigest` works for alerts/incidents/tombstones because those rows
/// are append-only: a row is written once and its identity and payload never
/// change again, so the newest timestamp plus a count is enough to tell two
/// replicas apart. A rollup row is not append-only — it is an upsert keyed on
/// `(check_id, agent_id, bucket_start)` whose `attempts`/`up`/`latency_sum`
/// keep growing for as long as its bucket accepts new raw observations.
/// Two peers can agree on `max_bucket_start` and `count` while their
/// counters disagree (one missed a batch of raw rows that landed in a bucket
/// both already hold), and a digest that could not see that would report
/// them converged when they are not.
///
/// `attempts_sum`/`up_sum`/`latency_sum_sum` are running totals across every
/// row in the table, maintained incrementally alongside the rollup rows
/// themselves (see `rollup_merge_from_raw_sql` and
/// `rollup_merge_from_backfill_sql`) rather than recomputed by scanning the
/// table on every digest. They work as a cheap checksum because every one of
/// them is a sum of non-negative contributions: each raw observation adds a
/// fixed, positive amount to exactly one bucket's counters exactly once, so
/// the totals are monotonic and independent of how writes were batched.
/// A bucket missing on one side therefore leaves at least one total strictly
/// smaller there — there is no way for two replicas holding different data to
/// coincidentally produce the same triple the way, say, two different
/// checksums of equal length could collide by construction. It is not
/// cryptographic, and does not need to be: like every other digest in this
/// module it exists to decide whether a backfill pull is worth making, not to
/// prove equality.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RollupDigest {
    pub max_bucket_start: i64,
    pub count: i64,
    pub attempts_sum: i64,
    pub up_sum: i64,
    pub latency_sum_sum: i64,
}

/// One rollup bucket, on the wire and as the unit `apply_minute_rollups` /
/// `apply_hour_rollups` merge. Shared by the minute and hour tables — they
/// differ only in bucket width, never in shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollupRow {
    pub check_id: CheckId,
    pub agent_id: String,
    pub bucket_start: i64,
    pub attempts: i64,
    pub up: i64,
    pub latency_sum: i64,
    pub latency_min: Option<i64>,
    pub latency_max: Option<i64>,
}

/// One standing raw-data gap: this node's retained history for `check_id`
/// ends at `from_ts`, `peer`'s begins at `to_ts`, and nothing between them is
/// held by either side. `observed_at` is when the condition was last seen,
/// not when it started — the gap is redetected every sweep it persists, and
/// each detection refreshes this row rather than appending a new one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawDataGapRow {
    pub check_id: CheckId,
    pub peer: String,
    pub from_ts: i64,
    pub to_ts: i64,
    pub observed_at: i64,
}

impl Storage {
    /// Recompute both latency rollup tables from raw `check_results`.
    /// Incremental maintenance in `rollup_merge_from_raw_sql` keeps them
    /// current on every write, so this is not called on the ordinary `open`
    /// path — only by tests that bulk-seed `check_results` directly and need
    /// the rollups a real `insert_results` call would have maintained.
    #[cfg(test)]
    pub(super) fn rebuild_latency_rollups(&self) -> Result<()> {
        self.conn.execute_batch(&format!(
            "DELETE FROM check_latency_minute_rollups;
             INSERT INTO check_latency_minute_rollups
             SELECT check_id, agent_id, (ts_millis // {MILLIS_PER_MINUTE}) * {MILLIS_PER_MINUTE},
                    COUNT(*), COUNT(*) FILTER (WHERE status = 'up'),
                    COALESCE(SUM(latency_ms) FILTER (WHERE status = 'up'), 0),
                    MIN(latency_ms) FILTER (WHERE status = 'up'),
                    MAX(latency_ms) FILTER (WHERE status = 'up')
             FROM check_results GROUP BY 1, 2, 3;
             DELETE FROM check_latency_hour_rollups;
             INSERT INTO check_latency_hour_rollups
             SELECT check_id, agent_id, (ts_millis // {MILLIS_PER_HOUR}) * {MILLIS_PER_HOUR},
                    COUNT(*), COUNT(*) FILTER (WHERE status = 'up'),
                    COALESCE(SUM(latency_ms) FILTER (WHERE status = 'up'), 0),
                    MIN(latency_ms) FILTER (WHERE status = 'up'),
                    MAX(latency_ms) FILTER (WHERE status = 'up')
             FROM check_results GROUP BY 1, 2, 3;"
        ))?;
        // The rollup tables were just replaced wholesale, so whatever the
        // watermark previously held (likely all zeros — this path only runs
        // for an old database that had no rollups at all) is now wrong.
        self.rebuild_rollup_watermarks()
    }

    /// Recompute `rollup_replication_watermarks` from the rollup tables
    /// themselves. Unlike the incremental maintenance in
    /// `rollup_merge_from_raw_sql`/`rollup_merge_from_backfill_sql`, this
    /// scans both tables in full, so it is reserved for `rebuild_latency_rollups`
    /// re-seeding both tables wholesale, never every write or every restart.
    #[cfg(test)]
    pub(super) fn rebuild_rollup_watermarks(&self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM rollup_replication_watermarks
                 WHERE stream IN ('check_latency_minute_rollups', 'check_latency_hour_rollups');
             INSERT INTO rollup_replication_watermarks
             SELECT 'check_latency_minute_rollups', COALESCE(MAX(bucket_start), 0), COUNT(*),
                    COALESCE(SUM(attempts), 0), COALESCE(SUM(up), 0), COALESCE(SUM(latency_sum), 0)
             FROM check_latency_minute_rollups;
             INSERT INTO rollup_replication_watermarks
             SELECT 'check_latency_hour_rollups', COALESCE(MAX(bucket_start), 0), COUNT(*),
                    COALESCE(SUM(attempts), 0), COALESCE(SUM(up), 0), COALESCE(SUM(latency_sum), 0)
             FROM check_latency_hour_rollups;",
        )?;
        Ok(())
    }

    /// Read `rollup_replication_watermarks` for one rollup table — a point
    /// lookup, never a scan of the rollup table it summarizes, because every
    /// write maintains the watermark incrementally alongside it.
    pub(super) fn rollup_digest(&self, stream: &str) -> Result<RollupDigest> {
        self.conn
            .query_row_cached(
                "SELECT max_bucket_start, row_count, attempts_sum, up_sum, latency_sum_sum
                 FROM rollup_replication_watermarks WHERE stream = ?",
                duckdb::params![stream],
                |row| {
                    Ok(RollupDigest {
                        max_bucket_start: row.get(0)?,
                        count: row.get(1)?,
                        attempts_sum: row.get(2)?,
                        up_sum: row.get(3)?,
                        latency_sum_sum: row.get(4)?,
                    })
                },
            )
            .map_err(Into::into)
    }

    /// Delete rollup rows fully expired against `before`, taking their
    /// contribution back out of `rollup_replication_watermarks` in the same
    /// pass. `row_count`/`attempts_sum`/`up_sum`/`latency_sum_sum` are
    /// maintained incrementally everywhere else in this file specifically so
    /// a digest never has to scan the rollup tables; a prune that deleted rows
    /// without also subtracting them here would leave that watermark counting
    /// rows that no longer exist, permanently overstating this node to every
    /// peer it digests against. `max_bucket_start` is untouched: pruning only
    /// ever removes the oldest buckets, so the newest one cannot be among them.
    pub(super) fn prune_rollups(&self, table: &str, bucket_ms: i64, before: i64) -> Result<u64> {
        let (count, attempts, up, latency_sum): (i64, i64, i64, i64) = self.conn.query_row(
            &format!(
                "SELECT COUNT(*), COALESCE(SUM(attempts), 0), COALESCE(SUM(up), 0),
                        COALESCE(SUM(latency_sum), 0)
                 FROM {table} WHERE bucket_start + ? <= ?"
            ),
            duckdb::params![bucket_ms, before],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if count == 0 {
            return Ok(0);
        }
        self.conn.execute(
            &format!("DELETE FROM {table} WHERE bucket_start + ? <= ?"),
            duckdb::params![bucket_ms, before],
        )?;
        self.conn.execute_cached(
            "UPDATE rollup_replication_watermarks SET
                 row_count = row_count - ?,
                 attempts_sum = attempts_sum - ?,
                 up_sum = up_sum - ?,
                 latency_sum_sum = latency_sum_sum - ?
             WHERE stream = ?",
            duckdb::params![count, attempts, up, latency_sum, table],
        )?;
        Ok(count as u64)
    }

    /// Aggregate latency from disjoint retention tiers. Buckets ending at a
    /// handoff belong to the older tier; raw observations start strictly after
    /// that handoff. This matches the `(from, to]` public window convention and
    /// prevents a probe on a boundary from being counted twice.
    pub(super) fn latency_source_sql(from: i64, to: i64) -> String {
        let raw_start =
            (to - RAW_RETENTION_MILLIS).div_euclid(MILLIS_PER_MINUTE) * MILLIS_PER_MINUTE;
        let minute_start =
            (to - MINUTE_ROLLUP_RETENTION_MILLIS).div_euclid(MILLIS_PER_HOUR) * MILLIS_PER_HOUR;
        format!(
            "SELECT check_id, agent_id, ts_millis, 1::BIGINT attempts,
                    (status = 'up')::BIGINT up,
                    CASE WHEN status = 'up' THEN latency_ms ELSE 0 END latency_sum,
                    CASE WHEN status = 'up' THEN latency_ms END latency_min,
                    CASE WHEN status = 'up' THEN latency_ms END latency_max
             FROM check_results WHERE ts_millis > {from} AND ts_millis <= {to}
                AND ts_millis > {raw_start}
             UNION ALL
             SELECT check_id, agent_id, bucket_start + {MILLIS_PER_MINUTE}, attempts, up,
                    latency_sum, latency_min, latency_max
             FROM check_latency_minute_rollups
             WHERE bucket_start >= {minute_start} AND bucket_start < {raw_start}
               -- A rollup is indivisible: only use it when its entire bucket
               -- lies inside the requested `(from, to]` window.
               AND bucket_start >= {from}
               AND bucket_start + {MILLIS_PER_MINUTE} <= {to}
             UNION ALL
             SELECT check_id, agent_id, bucket_start + {MILLIS_PER_HOUR}, attempts, up,
                    latency_sum, latency_min, latency_max
             FROM check_latency_hour_rollups
             WHERE bucket_start < {minute_start}
               AND bucket_start >= {from}
               AND bucket_start + {MILLIS_PER_HOUR} <= {to}",
        )
    }

    /// Sparkline points for many checks in one scan.
    ///
    /// Deliberately points only: the status page's row chart reads nothing else,
    /// while the per-agent breakdown and overall band belong to the single-check
    /// detail view, which keeps using `latency_series`. Computing them here would
    /// double the scan to populate fields nobody on this page renders.
    pub fn latency_points_for(
        &self,
        checks: &[CheckId],
        from: i64,
        to: i64,
        buckets: usize,
    ) -> Result<BTreeMap<CheckId, Vec<LatencyPoint>>> {
        let buckets = buckets.max(1);
        let bucket_ms = ((to - from) / buckets as i64).max(1);
        let staged: Vec<(CheckId, bool)> = checks.iter().map(|id| (*id, false)).collect();
        self.stage_check_ids(&staged)?;

        let mut folded: BTreeMap<CheckId, SegmentCounts> = BTreeMap::new();
        let source = Self::latency_source_sql(from, to);
        let mut stmt = self.conn.prepare(&format!(
            "SELECT check_id, ((ts_millis - ? - 1) // ?)::BIGINT AS idx,
                    SUM(attempts), SUM(up), COALESCE(SUM(latency_sum), 0)
             FROM ({source}) observations
             WHERE check_id IN (SELECT check_id FROM status_check_ids)
             GROUP BY 1, 2"
        ))?;
        let rows = stmt.query_map(duckdb::params![from, bucket_ms], |row| {
            Ok((
                row.get::<_, CheckId>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        for row in rows {
            let (check_id, index, attempts, up, sum_ms) = row?;
            let entry = folded
                .entry(check_id)
                .or_insert_with(|| SegmentCounts::new(buckets));
            let index = (index.max(0) as usize).min(buckets - 1);
            entry.sum_ms[index] += sum_ms.max(0) as f64;
            entry.attempts[index] = entry.attempts[index].saturating_add(attempts.max(0) as u32);
            entry.up[index] = entry.up[index].saturating_add(up.max(0) as u32);
        }

        Ok(checks
            .iter()
            .map(|check_id| {
                let counted = folded.get(check_id);
                let points = (0..buckets)
                    .map(|i| {
                        let up = counted.map(|c| c.up[i]).unwrap_or(0);
                        LatencyPoint {
                            timestamp: from + i as i64 * bucket_ms,
                            attempts: counted.map(|c| c.attempts[i]).unwrap_or(0),
                            up,
                            avg_latency_ms: (up > 0)
                                .then(|| counted.map(|c| c.sum_ms[i]).unwrap_or(0.0) / up as f64),
                        }
                    })
                    .collect();
                (*check_id, points)
            })
            .collect())
    }

    /// Average latency for a check over an explicit window, split into the
    /// requested number of equal segments (oldest first), plus exact per-agent
    /// attribution over the same window. Segments with no attempts carry
    /// `None`, not zero, so sparse timestamps are represented faithfully.
    ///
    /// The window is explicit rather than `now`-anchored so a historical
    /// segment (a clicked uptime bar) can be charted truthfully instead of
    /// being approximated by the newest N rows.
    pub fn latency_series(
        &self,
        check_id: CheckId,
        from: i64,
        to: i64,
        buckets: usize,
    ) -> Result<LatencySeries> {
        // The window and grid are exactly what the caller asked for. A young
        // check or sparse probe schedule legitimately leaves empty segments;
        // those are `None`, never invented 0ms measurements. Keeping every
        // requested segment also lets the client align series across ranges.
        let buckets = buckets.max(1);
        let bucket_ms = ((to - from) / buckets as i64).max(1);

        let mut sum = vec![0.0f64; buckets];
        let mut attempts = vec![0u32; buckets];
        let mut up = vec![0u32; buckets];
        let mut agents: BTreeMap<String, LatencyBand> = BTreeMap::new();

        let source = Self::latency_source_sql(from, to);
        let mut stmt = self.conn.prepare(&format!(
            "SELECT ts_millis, latency_sum, agent_id, attempts, up, latency_min, latency_max
             FROM ({source}) observations WHERE check_id = ?"
        ))?;
        let rows = stmt.query_map(duckdb::params![check_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })?;
        for r in rows {
            let (ts, latency_sum, agent_id, row_attempts, row_up, latency_min, latency_max) = r?;
            // Lower bound is exclusive, so shift before dividing: a timestamp
            // landing exactly on a boundary belongs to the segment below it.
            let i = (((ts - from - 1).max(0) / bucket_ms) as usize).min(buckets - 1);
            attempts[i] = attempts[i].saturating_add(row_attempts.max(0) as u32);
            sum[i] += latency_sum.max(0) as f64;
            up[i] = up[i].saturating_add(row_up.max(0) as u32);
            let band = agents.entry(agent_id).or_default();
            band.attempts = band.attempts.saturating_add(row_attempts.max(0) as u32);
            band.up = band.up.saturating_add(row_up.max(0) as u32);
            band.sum_ms += latency_sum.max(0) as f64;
            if let Some(minimum) = latency_min {
                let minimum = minimum as u64;
                band.min_ms = Some(band.min_ms.map_or(minimum, |old| old.min(minimum)));
            }
            if let Some(maximum) = latency_max {
                band.max_ms = Some(
                    band.max_ms
                        .map_or(maximum as u64, |old| old.max(maximum as u64)),
                );
            }
        }

        let points = (0..buckets)
            .map(|i| LatencyPoint {
                timestamp: from + i as i64 * bucket_ms,
                attempts: attempts[i],
                up: up[i],
                avg_latency_ms: (up[i] > 0).then(|| sum[i] / up[i] as f64),
            })
            .collect();

        let mut overall = LatencyBand::default();
        for b in agents.values() {
            overall.merge(b);
        }
        Ok(LatencySeries {
            points,
            overall,
            agents: agents
                .into_iter()
                .map(|(agent_id, band)| AgentBand { agent_id, band })
                .collect(),
        })
    }

    /// Paged rollup rows for `/internal/backfill/latency-minute` and
    /// `-hour`, ordered oldest-bucket-first with `check_id` and `agent_id`
    /// breaking ties — the same composite shape `results_after` uses for raw
    /// rows, which is what lets one cursored sweep walk the whole table
    /// instead of one request per divergent check. `table` is the only
    /// difference between the minute and hour callers.
    pub(super) fn rollups_since(
        &self,
        table: &str,
        since_bucket: i64,
        since_check_id: Option<CheckId>,
        since_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RollupRow>> {
        let sql = format!(
            "SELECT check_id, agent_id, bucket_start, attempts, up, latency_sum,
                    latency_min, latency_max
             FROM {table}
             WHERE bucket_start > ? OR (bucket_start = ? AND (
                 ? IS NULL OR check_id > ? OR (check_id = ? AND agent_id > ?)
             ))
             ORDER BY bucket_start ASC, check_id ASC, agent_id ASC
             LIMIT ?"
        );
        // `table` is one of the two rollup tables, so caching is bounded.
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(
            duckdb::params![
                since_bucket,
                since_bucket,
                since_check_id,
                since_check_id,
                since_check_id,
                since_agent_id,
                limit as i64
            ],
            |row| {
                Ok(RollupRow {
                    check_id: row.get(0)?,
                    agent_id: row.get(1)?,
                    bucket_start: row.get(2)?,
                    attempts: row.get(3)?,
                    up: row.get(4)?,
                    latency_sum: row.get(5)?,
                    latency_min: row.get(6)?,
                    latency_max: row.get(7)?,
                })
            },
        )?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn minute_rollups_since(
        &self,
        since_bucket: i64,
        since_check_id: Option<CheckId>,
        since_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RollupRow>> {
        self.rollups_since(
            "check_latency_minute_rollups",
            since_bucket,
            since_check_id,
            since_agent_id,
            limit,
        )
    }

    pub fn hour_rollups_since(
        &self,
        since_bucket: i64,
        since_check_id: Option<CheckId>,
        since_agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RollupRow>> {
        self.rollups_since(
            "check_latency_hour_rollups",
            since_bucket,
            since_check_id,
            since_agent_id,
            limit,
        )
    }

    /// Merge a backfilled page into rollup `table` via a staging table, the
    /// same shape `stage_results` uses for raw rows: load once through an
    /// appender, then let one set-based statement do the merge instead of a
    /// prepared statement per row.
    pub(super) fn apply_rollups(&self, table: &str, rows: &[RollupRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS ingest_rollup_stage AS
                 SELECT * FROM {table} WITH NO DATA;
             DELETE FROM ingest_rollup_stage;"
        ))?;
        {
            let mut appender = self.conn.appender("ingest_rollup_stage")?;
            for row in rows {
                appender.append_row(duckdb::params![
                    row.check_id,
                    row.agent_id,
                    row.bucket_start,
                    row.attempts,
                    row.up,
                    row.latency_sum,
                    row.latency_min,
                    row.latency_max,
                ])?;
            }
            appender.flush()?;
        }
        self.conn.execute_batch(&rollup_merge_from_backfill_sql(
            table,
            table,
            "ingest_rollup_stage",
        ))?;
        Ok(())
    }

    pub fn apply_minute_rollups(&self, rows: &[RollupRow]) -> Result<()> {
        self.apply_rollups("check_latency_minute_rollups", rows)
    }

    pub fn apply_hour_rollups(&self, rows: &[RollupRow]) -> Result<()> {
        self.apply_rollups("check_latency_hour_rollups", rows)
    }

    // ---- Retention-aware peer recovery (raw-data gaps) ----

    /// Record (or refresh) a standing gap: this node's raw history for
    /// `check_id` ends at `from_ts`, `peer`'s begins at `to_ts`, and neither
    /// side holds anything between them. Keyed on `(check_id, peer)` rather
    /// than appended, because `anti_entropy_sweep` redetects a standing gap on
    /// every sweep it persists — see the table's own comment in `open`.
    pub fn record_raw_data_gap(&self, gap: &RawDataGapRow) -> Result<()> {
        self.conn.execute_cached(
            "INSERT INTO raw_data_gaps (check_id, peer, from_ts, to_ts, observed_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT (check_id, peer) DO UPDATE SET
                 from_ts = excluded.from_ts,
                 to_ts = excluded.to_ts,
                 observed_at = excluded.observed_at",
            duckdb::params![
                gap.check_id,
                gap.peer,
                gap.from_ts,
                gap.to_ts,
                gap.observed_at
            ],
        )?;
        Ok(())
    }

    /// Every standing raw-data gap, newest-observed first, for the admin
    /// route — an operator asking which checks have holes and over what
    /// range, not a paged replication cursor. Small and admin-only, so
    /// returning the whole table and letting the route apply `page::take` is
    /// simpler than a second cursor scheme for a table this size.
    pub fn list_raw_data_gaps(&self) -> Result<Vec<RawDataGapRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT check_id, peer, from_ts, to_ts, observed_at
             FROM raw_data_gaps ORDER BY observed_at DESC, check_id ASC, peer ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RawDataGapRow {
                check_id: row.get(0)?,
                peer: row.get(1)?,
                from_ts: row.get(2)?,
                to_ts: row.get(3)?,
                observed_at: row.get(4)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::storage::test_support::*;

    /// Same contract for the sparkline: one scan for many checks must produce
    /// the points the per-check series produced.
    #[test]
    fn batched_latency_points_agree_with_the_per_check_series() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        for (name, base) in [("x", 10u64), ("y", 100)] {
            let mut down = result(name, now - 3 * hour, base + 5);
            down.agent_id = "agent-b".into();
            s.insert_results(&[
                (result(name, now - 4 * hour, base), Status::Up),
                (down, Status::Down),
                (result(name, now - hour, base + 15), Status::Up),
            ])
            .unwrap();
        }

        let from = now - 6 * hour;
        let ids = [cid("x"), cid("y"), cid("absent")];
        let batched = s.latency_points_for(&ids, from, now, 6).unwrap();
        assert_eq!(batched.len(), 3);

        for check_id in ids {
            let expected = s.latency_series(check_id, from, now, 6).unwrap().points;
            assert_eq!(batched[&check_id], expected, "{check_id} points must match");
        }
        assert!(
            batched[&cid("absent")].iter().all(|p| p.attempts == 0),
            "a check with no rows still gets a full, empty series"
        );
    }

    #[test]
    fn latency_sparkline_returns_hourly_averages() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        // Four distinct instants, so a 4-bucket split is fully supported by the
        // data and isn't capped.
        s.insert_results(&[
            (result("x", now, 50), Status::Up),
            (result("x", now - 600_000, 100), Status::Up),
            (result("x", now - hour - 600_000, 75), Status::Up),
            (result("x", now - 2 * hour - 600_000, 60), Status::Up),
            // Decoy check "y" should not count.
            (result("y", now, 999), Status::Up),
        ])
        .unwrap();

        let points = s
            .latency_series(cid("x"), now - 4 * hour, now, 4)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "four instants support four buckets");

        // Segments are one hour each: the requested window divided by the
        // buckets it supports, not narrowed to the data inside it.
        assert_eq!(points[0].timestamp, now - 4 * hour, "window as requested");
        assert_eq!(points[3].timestamp, now - hour);

        // Bucket 0 spans the hour before any result exists, and must not read
        // as a 0ms response.
        assert_eq!(points[0].avg_latency_ms, None, "empty segment -> None");
        assert_eq!(points[1].avg_latency_ms, Some(60.0));
        assert_eq!(points[2].avg_latency_ms, Some(75.0));
        assert_eq!(
            points[3].avg_latency_ms,
            Some(75.0),
            "newest segment averages 50 and 100"
        );

        // Decoy "y" must not be counted.
        let y_points = s
            .latency_series(cid("y"), now - 4 * hour, now, 4)
            .unwrap()
            .points;
        assert_eq!(
            y_points.last().unwrap().avg_latency_ms,
            Some(999.0),
            "decoy 'y' has its own data"
        );
    }

    #[test]
    fn every_requested_range_charts_that_range() {
        // The 24h/7d/30d/90d selector must actually change the span, including
        // for a check younger than the range. An earlier fix narrowed the window
        // to the data inside it, which read well for a young check but made all
        // four ranges render identically once the check was younger than them —
        // the control silently stopped doing anything.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        // Five hours old, exactly the shape that triggered the report.
        let rows: Vec<_> = (0..60)
            .map(|i| {
                (
                    result("young", now - i * 300_000, 20 + i as u64),
                    Status::Up,
                )
            })
            .collect();
        s.insert_results(&rows).unwrap();

        let span_of = |window: i64| {
            let p = s
                .latency_series(cid("young"), now - window, now, 120)
                .unwrap()
                .points;
            (p.last().unwrap().timestamp - p[0].timestamp, p.len())
        };
        let (day, _) = span_of(24 * hour);
        let (week, _) = span_of(7 * 24 * hour);
        let (month, _) = span_of(30 * 24 * hour);

        assert!(
            day < week && week < month,
            "each range must chart its own span: 24h={day} 7d={week} 30d={month}"
        );
        // And each is the requested window, not the data's extent.
        let requested = (30 * 24 * hour) as f64;
        let drift = (month as f64 - requested).abs() / requested;
        assert!(drift < 0.05, "30d view spans ~30 days, got {month}ms");
    }

    #[test]
    fn a_lone_populated_segment_is_still_a_reading() {
        // The other half of the report: over a 30-day window a five-hour-old
        // check fills a single segment. That is the truth and it must be
        // returned as data — the chart drew nothing and claimed to be
        // "Collecting latency data..." while sitting on 120 results.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let month = 30 * 24 * 3_600_000i64;

        let rows: Vec<_> = (0..60)
            .map(|i| (result("young", now - i * 30_000, 40), Status::Up))
            .collect();
        s.insert_results(&rows).unwrap();

        let series = s
            .latency_series(cid("young"), now - month, now, 120)
            .unwrap();
        let populated: Vec<_> = series
            .points
            .iter()
            .filter(|p| p.avg_latency_ms.is_some())
            .collect();
        assert!(
            !populated.is_empty(),
            "a check with 60 results must report a reading over any window"
        );
        assert_eq!(
            series.overall.attempts, 60,
            "and the summary counts every attempt regardless of bucketing"
        );
    }

    #[test]
    fn a_check_that_stopped_reporting_still_shows_its_silence() {
        // Silence is data. This check reported for an hour then went quiet 20
        // hours ago; charting only the hour it spoke would hide the outage
        // entirely, so the window must keep running to the requested end.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        let rows: Vec<_> = (0..12)
            .map(|i| {
                (
                    result("quiet", now - 21 * hour + i * 300_000, 25),
                    Status::Up,
                )
            })
            .collect();
        s.insert_results(&rows).unwrap();

        let points = s
            .latency_series(cid("quiet"), now - 24 * hour, now, 12)
            .unwrap();
        let trailing_empty = points
            .points
            .iter()
            .rev()
            .take_while(|p| p.avg_latency_ms.is_none())
            .count();
        assert!(
            trailing_empty > 0,
            "silence since the last result must stay on the chart"
        );
        let last = points.points.last().unwrap().timestamp;
        assert!(
            last > now - 4 * hour,
            "the chart still reaches the present: ends {}h early",
            (now - last) / hour
        );
    }

    #[test]
    fn latency_series_never_invents_zero_readings() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        // A check with nothing in the window must not produce a flat line of
        // zeroes, which would read as "responded instantly" rather than "never
        // ran".
        let points = s
            .latency_series(cid("absent"), now - 6 * 3_600_000, now, 6)
            .unwrap()
            .points;
        assert_eq!(
            points.len(),
            6,
            "an empty window still preserves its requested grid"
        );
        assert!(
            points.iter().all(|p| p.avg_latency_ms.is_none()),
            "no data must yield no measurements, got {points:?}"
        );
    }

    #[test]
    fn latency_series_preserves_the_requested_grid_for_sparse_timestamps() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let min = 60_000i64;

        // A short window probed every few minutes still gets its requested
        // grid; unobserved segments are explicitly empty, not 0ms readings.
        s.insert_results(&[
            (result("x", now - 12 * min, 40), Status::Up),
            (result("x", now - 8 * min, 50), Status::Up),
            (result("x", now - 4 * min, 60), Status::Up),
            (result("x", now, 70), Status::Up),
        ])
        .unwrap();

        let points = s
            .latency_series(cid("x"), now - 16 * min, now, 120)
            .unwrap()
            .points;
        assert_eq!(
            points.len(),
            120,
            "the requested grid is never capped to data"
        );
        assert_eq!(points[29].avg_latency_ms, Some(40.0));
        assert_eq!(points[59].avg_latency_ms, Some(50.0));
        assert_eq!(points[89].avg_latency_ms, Some(60.0));
        assert_eq!(points[119].avg_latency_ms, Some(70.0));
        assert_eq!(
            points.iter().filter(|p| p.avg_latency_ms.is_none()).count(),
            116,
            "unobserved segments remain empty rather than reading as zero latency"
        );
    }

    #[test]
    fn latency_series_splits_into_sub_hour_buckets() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let min = 60_000i64;

        // Four instants over the last hour, so the requested 4 buckets (15 min
        // each) survive the resolution cap. Two land at the edges, leaving the
        // middle two genuinely empty.
        s.insert_results(&[
            (result("x", now - min, 40), Status::Up), // bucket 3 (last 15 min)
            (result("x", now - 2 * min, 20), Status::Up), // bucket 3 as well
            (result("x", now - 50 * min, 80), Status::Up), // bucket 0
            (result("x", now - 55 * min, 100), Status::Up), // bucket 0 as well
        ])
        .unwrap();

        let points = s
            .latency_series(cid("x"), now - 60 * min, now, 4)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "exactly `buckets` points");
        assert_eq!(points[3].avg_latency_ms, Some(30.0), "avg of 40 and 20");
        assert_eq!(points[3].attempts, 2);
        assert_eq!(points[3].up, 2);
        assert_eq!(points[0].avg_latency_ms, Some(90.0), "avg of 80 and 100");
        assert_eq!(points[0].attempts, 2);
        assert_eq!(points[0].up, 2);
        assert_eq!(points[1].attempts, 0, "middle segment is unobserved");
        assert_eq!(points[1].up, 0);
        assert_eq!(points[1].avg_latency_ms, None, "middle segment empty");
        assert_eq!(points[2].avg_latency_ms, None, "middle segment empty");
    }

    #[test]
    fn latency_series_attributes_a_historical_window_per_agent() {
        // What the day-detail modal asks for: a window that ended long before
        // `now`, with two agents disagreeing inside it. Sampling the newest rows
        // could not answer this.
        let s = Storage::open(":memory:").unwrap();
        let hour = 3_600_000i64;
        let now = 1_000_000_000_000i64;
        let from = now - 10 * hour;
        let to = from + 4 * hour;

        let at = |agent: &str, ts: i64, latency: u64, status: Status| {
            let mut r = result("web", ts, latency);
            r.agent_id = agent.to_string();
            (r, status)
        };
        s.insert_results(&[
            at("agent-a", from + hour, 10, Status::Up),
            at("agent-a", from + 2 * hour, 30, Status::Up),
            // agent-b times out inside the window: 5000ms is how long it waited,
            // not a measurement, so it must not become the max.
            at("agent-b", from + hour, 5000, Status::Down),
            at("agent-b", from + 2 * hour, 20, Status::Up),
            // Outside the window on both sides, and a decoy check.
            at("agent-a", from - hour, 999, Status::Up),
            at("agent-a", to + hour, 888, Status::Up),
            (result("other", from + hour, 777), Status::Up),
        ])
        .unwrap();

        // Two distinct instants in the window, so ask for the two segments the
        // data can actually support.
        let series = s.latency_series(cid("web"), from, to, 2).unwrap();

        assert_eq!(series.overall.attempts, 4, "only rows inside the window");
        assert_eq!(series.overall.up, 3);
        assert_eq!(series.overall.min_ms, Some(10));
        assert_eq!(
            series.overall.max_ms,
            Some(30),
            "the 5000ms timeout must not set the max"
        );
        assert_eq!(series.overall.avg_ms(), Some(20.0), "(10+30+20)/3");

        let by_agent: Vec<_> = series
            .agents
            .iter()
            .map(|a| (a.agent_id.as_str(), a.band.attempts, a.band.up))
            .collect();
        assert_eq!(
            by_agent,
            vec![("agent-a", 2, 2), ("agent-b", 2, 1)],
            "the failing agent is identifiable"
        );

        // Point counts retain failed evidence separately from latency. Both
        // recorded instants land in the first half of this four-hour window;
        // the timeout is observed but excluded from the latency average.
        assert_eq!(series.points.len(), 2);
        assert_eq!(series.points[0].attempts, 4);
        assert_eq!(series.points[0].up, 3);
        assert_eq!(series.points[0].avg_latency_ms, Some(20.0));
        assert_eq!(series.points[1].attempts, 0);
        assert_eq!(series.points[1].up, 0);
        // The series covers the window the caller asked for.
        assert_eq!(
            series.points[0].timestamp, from,
            "oldest segment starts at the requested bound"
        );
        // Silence after the last result means the check stopped reporting,
        // which must stay visible rather than being trimmed away.
        assert_eq!(
            series.points[1].avg_latency_ms, None,
            "the empty half must not read as 0ms"
        );
        assert!(
            series.points[1].timestamp < to,
            "the requested end is still charted"
        );
    }

    /// Retention must cost what it *removes*, not what it keeps. Seeds a table
    /// spanning two windows, prunes the older one, and reports the delete
    /// beside the full rebuilds it replaced — the rebuilds are timed here only
    /// to keep the comparison honest, they are no longer on the path.
    ///
    /// `LIGHTHOUSE_BENCH_PRUNE_ROWS` (default 20,000,000) sets the retained rows.
    #[test]
    fn latency_rollups_preserve_domains_dedup_and_raw_handoff() {
        let s = Storage::open(":memory:").unwrap();
        let now = 100 * 24 * MILLIS_PER_HOUR;
        let to = now;
        let handoff = to - RAW_RETENTION_MILLIS;
        let minute = handoff - MILLIS_PER_MINUTE;
        let mut a = result("latency", minute + 10_000, 10);
        a.agent_id = "domain-a".into();
        let mut b = result("latency", minute + 20_000, 30);
        b.agent_id = "domain-b".into();
        let mut raw = result("latency", handoff + 1, 50);
        raw.agent_id = "domain-a".into();
        s.insert_results_dedup(&[(a.clone(), Status::Up), (b, Status::Up), (raw, Status::Up)])
            .unwrap();
        assert_eq!(s.insert_results_dedup(&[(a, Status::Up)]).unwrap(), 0);
        s.prune_results(handoff).unwrap();

        let series = s
            .latency_series(
                cid("latency"),
                minute,
                to,
                (to - minute) as usize / MILLIS_PER_MINUTE as usize,
            )
            .unwrap();
        assert_eq!(
            series.points[0].attempts, 2,
            "both domains survive in minute rollup"
        );
        assert_eq!(series.points[0].avg_latency_ms, Some(20.0));
        let raw_point = &series.points[1];
        assert_eq!(raw_point.attempts, 1, "raw starts strictly after handoff");
        assert_eq!(raw_point.avg_latency_ms, Some(50.0));
        assert_eq!(
            series.overall.attempts, 3,
            "replay must not inflate rollups"
        );
    }

    #[test]
    fn latency_rollup_retention_and_restart_keep_long_range_queries() {
        let path = std::env::temp_dir().join(format!(
            "lighthouse-rollups-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let now = 500 * 24 * MILLIS_PER_HOUR;
        // Must be more than 90 days old (minute tier) but still inside the
        // 13-month hour tier selected relative to this query's end.
        let old_hour = now - 300 * 24 * MILLIS_PER_HOUR;
        let expired_minute = now - MINUTE_ROLLUP_RETENTION_MILLIS - MILLIS_PER_MINUTE;
        let expired_hour = now - HOUR_ROLLUP_RETENTION_MILLIS - MILLIS_PER_HOUR;
        {
            let s = Storage::open(path.to_str().unwrap()).unwrap();
            s.insert_results(&[
                (result("latency", old_hour + 1, 40), Status::Up),
                (result("latency", expired_minute + 1, 60), Status::Up),
                (result("latency", expired_hour + 1, 80), Status::Up),
            ])
            .unwrap();
            s.prune(30, 90, now).unwrap();
        }
        let s = Storage::open(path.to_str().unwrap()).unwrap();
        let rollup_count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM check_latency_hour_rollups WHERE check_id = ?",
                duckdb::params![cid("latency")],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rollup_count, 2);
        let series = s
            .latency_series(
                cid("latency"),
                old_hour - MILLIS_PER_HOUR,
                now,
                ((now - old_hour) / MILLIS_PER_HOUR + 1) as usize,
            )
            .unwrap();
        assert_eq!(
            series.points[1].avg_latency_ms,
            Some(40.0),
            "hour rollup survives restart"
        );
        let minute_count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM check_latency_minute_rollups WHERE bucket_start <= ?",
                duckdb::params![expired_minute],
                |r| r.get(0),
            )
            .unwrap();
        let hour_count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM check_latency_hour_rollups WHERE bucket_start <= ?",
                duckdb::params![expired_hour],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(minute_count, 0, "minute retention is ninety days");
        assert_eq!(hour_count, 0, "hour retention is thirteen months");
        drop(s);
        let _ = std::fs::remove_file(path);
    }

    /// The scenario item A's merge is built for: a page of rollup rows this
    /// node never derived from its own raw observations, applied for the
    /// first time.
    #[test]
    fn a_backfilled_rollup_page_is_applied_on_first_receipt() {
        let s = Storage::open(":memory:").unwrap();
        let page = vec![row("a", "agent-1", 60_000, 5, 4, 40)];

        s.apply_minute_rollups(&page).unwrap();

        let stored = s.minute_rollups_since(i64::MIN, None, None, 10).unwrap();
        assert_eq!(stored, page);
    }

    /// The hardest requirement on the backfill merge: a retried or replayed
    /// page must land exactly once. `rollup_merge_from_backfill_sql` uses
    /// `GREATEST` rather than addition precisely so this holds.
    #[test]
    fn applying_the_same_backfilled_rollup_page_twice_does_not_double_count() {
        let s = Storage::open(":memory:").unwrap();
        let page = vec![row("a", "agent-1", 60_000, 3, 2, 20)];

        s.apply_minute_rollups(&page).unwrap();
        s.apply_minute_rollups(&page).unwrap();

        let stored = s.minute_rollups_since(i64::MIN, None, None, 10).unwrap();
        assert_eq!(
            stored.len(),
            1,
            "a replay must merge into the same row, not add another"
        );
        assert_eq!(
            stored[0].attempts, 3,
            "a replayed page must not inflate attempts"
        );
        assert_eq!(stored[0].up, 2);
        assert_eq!(stored[0].latency_sum, 20);

        // The digest's checksum is maintained by the same statement batch and
        // must be exactly as replay-safe as the row it summarizes.
        let digest = s.digest(0).unwrap().latency_minute_rollups;
        assert_eq!(digest.count, 1);
        assert_eq!(
            digest.attempts_sum, 3,
            "the digest checksum must not double-count a replay either"
        );
        assert_eq!(digest.up_sum, 2);
        assert_eq!(digest.latency_sum_sum, 20);
    }

    /// A backfilled total only ever grows what is already held — it never
    /// erases local progress a peer has not caught up to yet.
    #[test]
    fn a_backfilled_rollup_never_shrinks_a_larger_local_total() {
        let s = Storage::open(":memory:").unwrap();
        s.apply_minute_rollups(&[row("a", "agent-1", 60_000, 10, 8, 80)])
            .unwrap();
        // A peer's page for the same bucket, smaller in every field — as if
        // it were still mid-catch-up itself.
        s.apply_minute_rollups(&[row("a", "agent-1", 60_000, 4, 4, 40)])
            .unwrap();

        let stored = s.minute_rollups_since(i64::MIN, None, None, 10).unwrap();
        assert_eq!(stored[0].attempts, 10);
        assert_eq!(stored[0].up, 8);
        assert_eq!(stored[0].latency_sum, 80);
    }

    /// `StreamDigest`'s shape (max position plus a count) would call these two
    /// replicas converged: same bucket count, same newest bucket. Only the
    /// counter totals `RollupDigest` adds tell them apart.
    #[test]
    fn rollup_digest_reflects_counter_totals_not_just_count_and_max() {
        let behind = Storage::open(":memory:").unwrap();
        let ahead = Storage::open(":memory:").unwrap();
        behind
            .apply_minute_rollups(&[row("a", "agent-1", 60_000, 3, 1, 10)])
            .unwrap();
        ahead
            .apply_minute_rollups(&[row("a", "agent-1", 60_000, 5, 5, 50)])
            .unwrap();

        let behind_digest = behind.digest(0).unwrap().latency_minute_rollups;
        let ahead_digest = ahead.digest(0).unwrap().latency_minute_rollups;
        assert_eq!(behind_digest.count, ahead_digest.count);
        assert_eq!(
            behind_digest.max_bucket_start,
            ahead_digest.max_bucket_start
        );
        assert_ne!(
            behind_digest.attempts_sum, ahead_digest.attempts_sum,
            "count and max agree, but the counters do not, and the digest must show it"
        );
    }

    /// Item B: a detected gap is durable metadata, not an in-memory signal —
    /// it must be there for an operator to read after any restart, not only
    /// until the next one.
    #[test]
    fn a_raw_data_gap_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "lighthouse-raw-data-gap-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path = path.to_string_lossy().into_owned();
        let gap = RawDataGapRow {
            check_id: cid("a"),
            peer: "http://peer".into(),
            from_ts: 1_000,
            to_ts: 5_000,
            observed_at: 9_000,
        };
        {
            let s = Storage::open(&path).unwrap();
            s.record_raw_data_gap(&gap).unwrap();
        }

        let reopened = Storage::open(&path).unwrap();
        assert_eq!(reopened.list_raw_data_gaps().unwrap(), vec![gap.clone()]);

        // The same standing gap, redetected on a later sweep with a fresher
        // `observed_at`, must refresh the one row rather than accumulate a
        // second one keyed the same way.
        let refreshed = RawDataGapRow {
            observed_at: 9_500,
            ..gap
        };
        reopened.record_raw_data_gap(&refreshed).unwrap();
        assert_eq!(
            reopened.list_raw_data_gaps().unwrap(),
            vec![refreshed],
            "redetecting a standing gap must update it in place"
        );
    }
}
