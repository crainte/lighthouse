use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Raw check result sent from agent to coordinator.
/// The agent is a dumb probe: it reports what it observed, not a verdict.
/// The coordinator runs eval logic to derive UP/DOWN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub check_id: String,
    pub agent_id: String,
    pub timestamp: DateTime<Utc>,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Evaluated status, derived by the coordinator from a CheckResult.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Up,
    Down,
    Unknown,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Up => "up",
            Status::Down => "down",
            Status::Unknown => "unknown",
        }
    }
}

/// Default eval: no error and 2xx-3xx status code = UP.
/// Coordinator applies this when a check has no custom eval script.
pub fn default_eval(result: &CheckResult) -> Status {
    let _ = result;
    todo!("implement in green phase")
}

/// Batch envelope for the ingest endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct ResultBatch {
    pub results: Vec<CheckResult>,
}
