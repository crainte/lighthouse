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

    /// Seed synthetic data spanning the 24h before `now_millis` when the store
    /// is empty. Idempotent: a no-op when check_results already has rows.
    /// Returns true if seeding occurred.
    pub fn seed_mock(&self, now_millis: i64) -> Result<bool> {
        let count: i64 =
            self.conn.query_row("SELECT count(*) FROM check_results", [], |r| r.get(0))?;
        if count > 0 {
            return Ok(false);
        }

        // A handful of realistic-looking checks. One ("redis-cache") suffers a
        // brief outage mid-window with matching alerts, the rest stay healthy.
        let checks = ["api-gateway", "postgres-primary", "redis-cache", "web-frontend"];
        let step = 30 * 60 * 1000i64; // one sample every 30 min
        let points = 48; // 24h of history
        let mut rows: Vec<(CheckResult, Status)> = Vec::new();

        for name in checks {
            for i in 0..points {
                let ts = now_millis - (points - 1 - i) * step;
                // redis-cache is DOWN for a 3-sample stretch near the middle.
                let down = name == "redis-cache" && (20..23).contains(&i);
                let (status, code, latency) = if down {
                    (Status::Down, Some(503u16), 0)
                } else {
                    (Status::Up, Some(200u16), 20 + (i % 7) * 5)
                };
                rows.push((
                    CheckResult {
                        check_id: name.to_string(),
                        agent_id: "seed".to_string(),
                        timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
                        latency_ms: latency as u64,
                        status_code: code,
                        error: if down { Some("503 Service Unavailable".into()) } else { None },
                    },
                    status,
                ));
            }
        }
        self.insert_results(&rows)?;

        // Alerts for the redis-cache outage: down at sample 20, recovery at 23.
        let down_ts = now_millis - (points - 1 - 20) * step;
        let up_ts = now_millis - (points - 1 - 23) * step;
        self.insert_alert("redis-cache", Status::Up, Status::Down, down_ts)?;
        self.insert_alert("redis-cache", Status::Down, Status::Up, up_ts)?;

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
}
