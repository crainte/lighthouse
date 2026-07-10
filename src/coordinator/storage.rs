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
        let _ = path;
        todo!("implement in green phase")
    }

    /// Persist a batch of raw results with their evaluated status.
    pub fn insert_results(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        let _ = rows;
        todo!("implement in green phase")
    }

    /// Return the most recent `limit` history rows for a check, newest first.
    pub fn history(&self, check_id: &str, limit: usize) -> Result<Vec<HistoryRow>> {
        let _ = (check_id, limit);
        todo!("implement in green phase")
    }
}
