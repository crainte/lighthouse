use crate::models::{CheckResult, Status};
use anyhow::Result;
use duckdb::Connection;

/// DuckDB-backed store for check results and materialized current state.
/// Single connection guarded by the caller; DuckDB is single-writer.
pub struct Storage {
    conn: Connection,
}

/// One row of check history.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRow {
    pub timestamp: i64, // unix millis
    pub status: Status,
    pub latency_ms: u64,
}

/// Current status of a check plus recent uptime, for the status grid.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusRow {
    pub check_id: String,
    pub status: Status,
    pub latency_ms: u64,
    pub last_check: i64, // unix millis of latest result
    pub uptime_24h: f64, // fraction 0.0..=1.0 of UP results in the 24h before now
}

/// A persisted alert (state transition worth surfacing).
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRow {
    pub check_id: String,
    pub from: Status,
    pub to: Status,
    pub timestamp: i64, // unix millis
}

/// One day's uptime bucket for the status-page bar strip.
/// `bucket 0` is the oldest day, `bucket days-1` is today.
#[derive(Debug, Clone, PartialEq)]
pub struct DayUptime {
    pub day_start: i64,        // unix millis of the day-window start (exclusive lower bound)
    pub uptime: Option<f64>,   // fraction 0.0..=1.0, or None when the day has no data
    pub up: u32,
    pub total: u32,
}

impl Storage {
    /// Open (or create) the database at `path` and ensure the schema exists.
    /// `path` may be ":memory:" for tests.
    pub fn open(path: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS check_results (
                check_id   VARCHAR NOT NULL,
                agent_id   VARCHAR NOT NULL,
                ts_millis  BIGINT  NOT NULL,
                status     VARCHAR NOT NULL,
                latency_ms BIGINT  NOT NULL,
                status_code INTEGER,
                error      VARCHAR
            );
            CREATE TABLE IF NOT EXISTS alerts (
                check_id   VARCHAR NOT NULL,
                from_state VARCHAR NOT NULL,
                to_state   VARCHAR NOT NULL,
                ts_millis  BIGINT  NOT NULL
            );",
        )?;
        Ok(Self { conn })
    }

    /// Persist a batch of raw results with their evaluated status.
    pub fn insert_results(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "INSERT INTO check_results
                (check_id, agent_id, ts_millis, status, latency_ms, status_code, error)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
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
            ])?;
        }
        Ok(())
    }

    /// Return the most recent `limit` history rows for a check, newest first.
    pub fn history(&self, check_id: &str, limit: usize) -> Result<Vec<HistoryRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_millis, status, latency_ms
             FROM check_results
             WHERE check_id = ?
             ORDER BY ts_millis DESC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![check_id, limit as i64], |row| {
                let ts: i64 = row.get(0)?;
                let status: String = row.get(1)?;
                let latency: i64 = row.get(2)?;
                Ok(HistoryRow {
                    timestamp: ts,
                    status: Status::from_db(&status),
                    latency_ms: latency as u64,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Persist an alert (a state transition worth surfacing).
    pub fn insert_alert(&self, check_id: &str, from: Status, to: Status, ts_millis: i64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO alerts (check_id, from_state, to_state, ts_millis) VALUES (?, ?, ?, ?)",
            duckdb::params![check_id, from.as_str(), to.as_str(), ts_millis],
        )?;
        Ok(())
    }

    /// Return the most recent `limit` alerts, newest first.
    pub fn recent_alerts(&self, limit: usize) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT check_id, from_state, to_state, ts_millis
             FROM alerts
             ORDER BY ts_millis DESC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![limit as i64], |row| {
                Ok(AlertRow {
                    check_id: row.get::<_, String>(0)?,
                    from: Status::from_db(&row.get::<_, String>(1)?),
                    to: Status::from_db(&row.get::<_, String>(2)?),
                    timestamp: row.get::<_, i64>(3)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// One StatusRow per check_id: latest status/latency/timestamp, plus the
    /// fraction of UP results in the 24h window `(now-24h, now]`.
    /// A check with no rows at all is absent; uptime is over in-window rows only.
    pub fn list_status(&self, now_millis: i64) -> Result<Vec<StatusRow>> {
        let window_start = now_millis - 86_400_000;
        let mut stmt = self.conn.prepare(
            "SELECT
                 check_id,
                 arg_max(status, ts_millis)     AS latest_status,
                 arg_max(latency_ms, ts_millis) AS latest_latency,
                 max(ts_millis)                 AS last_check,
                 CAST(SUM(CASE WHEN status = 'up' AND ts_millis > ? AND ts_millis <= ? THEN 1 ELSE 0 END) AS DOUBLE)
                   / NULLIF(SUM(CASE WHEN ts_millis > ? AND ts_millis <= ? THEN 1 ELSE 0 END), 0) AS uptime
             FROM check_results
             GROUP BY check_id",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![window_start, now_millis, window_start, now_millis],
                |row| {
                    Ok(StatusRow {
                        check_id: row.get::<_, String>(0)?,
                        status: Status::from_db(&row.get::<_, String>(1)?),
                        latency_ms: row.get::<_, i64>(2)? as u64,
                        last_check: row.get::<_, i64>(3)?,
                        uptime_24h: row.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Daily uptime buckets for a check over the `days` days ending at `now_millis`,
    /// oldest-first. Bucket `i` covers `(now - (days-i)*DAY, now - (days-1-i)*DAY]`;
    /// the last bucket ends at `now`. A day with no results has `uptime == None`.
    /// Always returns exactly `days` buckets.
    pub fn daily_uptime(&self, check_id: &str, days: usize, now_millis: i64) -> Result<Vec<DayUptime>> {
        const DAY: i64 = 86_400_000;
        let days_i = days as i64;
        let lower = now_millis - days_i * DAY; // exclusive

        let mut up = vec![0u32; days];
        let mut total = vec![0u32; days];

        let mut stmt = self.conn.prepare(
            "SELECT ts_millis, status FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
        )?;
        let rows = stmt.query_map(duckdb::params![check_id, lower, now_millis], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for r in rows {
            let (ts, status) = r?;
            // days ago: 0 = today. In range guarantees 0 <= d < days.
            let d = ((now_millis - ts) / DAY) as usize;
            let i = days - 1 - d;
            total[i] += 1;
            if Status::from_db(&status) == Status::Up {
                up[i] += 1;
            }
        }

        let buckets = (0..days)
            .map(|i| {
                let t = total[i];
                DayUptime {
                    day_start: now_millis - (days_i - i as i64) * DAY,
                    up: up[i],
                    total: t,
                    uptime: if t > 0 { Some(up[i] as f64 / t as f64) } else { None },
                }
            })
            .collect();
        Ok(buckets)
    }

    /// Seed synthetic data spanning ~90 days before `now_millis` when the store
    /// is empty. Idempotent: a no-op when check_results already has rows.
    /// Returns true if seeding occurred.
    pub fn seed_mock(&self, now_millis: i64) -> Result<bool> {
        let count: i64 =
            self.conn.query_row("SELECT count(*) FROM check_results", [], |r| r.get(0))?;
        if count > 0 {
            return Ok(false);
        }

        let checks = ["api-gateway", "postgres-primary", "redis-cache", "web-frontend"];
        let days = 90usize;
        let per_day = 6usize; // one sample every 4h
        let points = days * per_day;
        let step = 24 * 60 * 60 * 1000i64 / per_day as i64; // 4h in millis
        let ts_at = |i: usize| now_millis - (points - 1 - i) as i64 * step;

        // Outage windows expressed as (check, inclusive day range) where the
        // check reports DOWN. day 0 is the oldest, day 89 is today.
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
            for i in 0..points {
                let day = i / per_day;
                let down = is_down(name, day);
                let (status, code, latency) = if down {
                    (Status::Down, Some(503u16), 0)
                } else {
                    (Status::Up, Some(200u16), 20 + ((i % 7) * 5) as i64)
                };
                rows.push((
                    CheckResult {
                        check_id: name.to_string(),
                        agent_id: "seed".to_string(),
                        timestamp: chrono::DateTime::from_timestamp_millis(ts_at(i)).unwrap(),
                        latency_ms: latency as u64,
                        status_code: code,
                        error: if down { Some("503 Service Unavailable".into()) } else { None },
                    },
                    status,
                ));
            }
        }
        self.insert_results(&rows)?;

        // Matching alerts (down at the first sample of the bad day, recovery the
        // first sample after it) so the incidents list mirrors the bars.
        let mut alert = |name: &str, down_day: usize| -> Result<()> {
            let down_ts = ts_at(down_day * per_day);
            let up_ts = ts_at((down_day + 1) * per_day);
            self.insert_alert(name, Status::Up, Status::Down, down_ts)?;
            self.insert_alert(name, Status::Down, Status::Up, up_ts)?;
            Ok(())
        };
        alert("redis-cache", 60)?;
        alert("redis-cache", 88)?;
        alert("api-gateway", 70)?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn result(check_id: &str, ts_millis: i64, latency_ms: u64) -> CheckResult {
        CheckResult {
            check_id: check_id.into(),
            agent_id: "agent".into(),
            timestamp: DateTime::from_timestamp_millis(ts_millis).unwrap(),
            latency_ms,
            status_code: Some(200),
            error: None,
        }
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
        let hist = s.history("a", 10).unwrap();
        assert_eq!(
            hist,
            vec![
                HistoryRow { timestamp: 3000, status: Status::Up, latency_ms: 30 },
                HistoryRow { timestamp: 2000, status: Status::Down, latency_ms: 20 },
                HistoryRow { timestamp: 1000, status: Status::Up, latency_ms: 10 },
            ]
        );

        // Limit caps to the N newest rows.
        let capped = s.history("a", 2).unwrap();
        assert_eq!(
            capped,
            vec![
                HistoryRow { timestamp: 3000, status: Status::Up, latency_ms: 30 },
                HistoryRow { timestamp: 2000, status: Status::Down, latency_ms: 20 },
            ]
        );
    }

    #[test]
    fn recent_alerts_round_trips_newest_first_with_exact_fields() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_alert("a", Status::Up, Status::Down, 1000).unwrap();
        s.insert_alert("b", Status::Down, Status::Up, 2000).unwrap();
        s.insert_alert("a", Status::Up, Status::Unknown, 3000).unwrap();

        let got = s.recent_alerts(10).unwrap();
        assert_eq!(
            got,
            vec![
                AlertRow { check_id: "a".into(), from: Status::Up, to: Status::Unknown, timestamp: 3000 },
                AlertRow { check_id: "b".into(), from: Status::Down, to: Status::Up, timestamp: 2000 },
                AlertRow { check_id: "a".into(), from: Status::Up, to: Status::Down, timestamp: 1000 },
            ]
        );
    }

    #[test]
    fn recent_alerts_caps_to_the_newest_limit() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_alert("a", Status::Up, Status::Down, 1000).unwrap();
        s.insert_alert("b", Status::Down, Status::Up, 2000).unwrap();
        s.insert_alert("a", Status::Up, Status::Unknown, 3000).unwrap();

        let got = s.recent_alerts(2).unwrap();
        assert_eq!(
            got,
            vec![
                AlertRow { check_id: "a".into(), from: Status::Up, to: Status::Unknown, timestamp: 3000 },
                AlertRow { check_id: "b".into(), from: Status::Down, to: Status::Up, timestamp: 2000 },
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

        let rows = s.list_status(now).unwrap();
        assert_eq!(rows.len(), 2, "one row per distinct check_id");
        let find = |id: &str| rows.iter().find(|r| r.check_id == id).unwrap().clone();

        let a = find("a");
        assert_eq!(a.status, Status::Down, "latest row wins");
        assert_eq!(a.latency_ms, 50, "latest row's latency");
        assert_eq!(a.last_check, now, "latest row's timestamp");
        assert!((a.uptime_24h - 0.75).abs() < 1e-9, "3 UP / 4 in-window, got {}", a.uptime_24h);

        let b = find("b");
        assert_eq!(b.status, Status::Up);
        assert_eq!(b.latency_ms, 21);
        assert_eq!(b.last_check, now - 4000);
        // Stale DOWN at the exclusive lower bound is filtered out, so 2 UP / 2 in-window.
        assert!((b.uptime_24h - 1.0).abs() < 1e-9, "stale row must be excluded, got {}", b.uptime_24h);
    }

    #[test]
    fn seed_mock_seeds_when_empty_then_is_idempotent() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        assert!(s.seed_mock(now).unwrap(), "seeds an empty store");
        let seeded = s.list_status(now).unwrap();
        assert!(!seeded.is_empty(), "seeded store yields at least one status");

        assert!(!s.seed_mock(now).unwrap(), "second seed is a no-op");
        let after = s.list_status(now).unwrap();
        assert_eq!(after.len(), seeded.len(), "idempotent seed adds no checks");
    }

    #[test]
    fn daily_uptime_buckets_windows_filters_and_boundary() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        s.insert_results(&[
            // Bucket 2 (today, (now-DAY, now]): 2 UP + 1 DOWN -> up=2, total=3.
            (result("x", now, 10), Status::Up),
            (result("x", now - 1000, 11), Status::Up),
            (result("x", now - 2000, 12), Status::Down),
            // Bucket 1 ((now-2*DAY, now-DAY]): nothing -> up=0, total=0, uptime=None.
            // Bucket 0 ((now-3*DAY, now-2*DAY]): 1 UP -> up=1, total=1, uptime=Some(1.0).
            (result("x", now - 2 * day - 1000, 13), Status::Up),
            // Boundary: exactly at bucket 0's exclusive lower bound now-3*DAY -> excluded.
            (result("x", now - 3 * day, 14), Status::Up),
            // Decoy check "y" at now: must never count toward "x".
            (result("y", now, 99), Status::Up),
        ])
        .unwrap();

        let buckets = s.daily_uptime("x", 3, now).unwrap();
        assert_eq!(buckets.len(), 3, "exactly `days` buckets, oldest-first");

        // Bucket 0: oldest day; boundary row at now-3*DAY excluded, one UP remains.
        assert_eq!(buckets[0].day_start, now - 3 * day);
        assert_eq!(buckets[0].up, 1);
        assert_eq!(buckets[0].total, 1, "row at exclusive lower bound now-3*DAY excluded");
        assert_eq!(buckets[0].uptime, Some(1.0));

        // Bucket 1: empty middle day.
        assert_eq!(buckets[1].day_start, now - 2 * day);
        assert_eq!(buckets[1].up, 0);
        assert_eq!(buckets[1].total, 0);
        assert_eq!(buckets[1].uptime, None, "no rows -> uptime None, not 0.0");

        // Bucket 2: today; 2 UP + 1 DOWN; decoy "y" excluded.
        assert_eq!(buckets[2].day_start, now - day);
        assert_eq!(buckets[2].up, 2);
        assert_eq!(buckets[2].total, 3, "decoy check \"y\" must not count");
        let uptime = buckets[2].uptime.expect("today has data");
        assert!((uptime - 2.0 / 3.0).abs() < 1e-9, "2 UP / 3 total, got {uptime}");
    }

    #[test]
    fn daily_uptime_no_rows_returns_days_empty_buckets() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        let buckets = s.daily_uptime("absent", 4, now).unwrap();
        assert_eq!(buckets.len(), 4, "always exactly `days` buckets even with no data");
        for (i, b) in buckets.iter().enumerate() {
            assert_eq!(b.day_start, now - (4 - i as i64) * day, "bucket {i} window start");
            assert_eq!(b.up, 0);
            assert_eq!(b.total, 0);
            assert_eq!(b.uptime, None, "bucket {i} has no data");
        }
    }
}
