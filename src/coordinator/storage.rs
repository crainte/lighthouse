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
}
