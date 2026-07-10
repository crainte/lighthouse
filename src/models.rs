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

    /// Parse a status back from its stored string form.
    pub fn from_db(s: &str) -> Status {
        match s {
            "up" => Status::Up,
            "down" => Status::Down,
            _ => Status::Unknown,
        }
    }
}

/// Default eval: no error and 2xx-3xx status code = UP.
/// Coordinator applies this when a check has no custom eval script.
pub fn default_eval(result: &CheckResult) -> Status {
    if result.error.is_some() {
        return Status::Down;
    }
    match result.status_code {
        Some(code) if (200..400).contains(&code) => Status::Up,
        Some(_) => Status::Down,
        None => Status::Unknown,
    }
}

/// Batch envelope for the ingest endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct ResultBatch {
    pub results: Vec<CheckResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn cr(status_code: Option<u16>, error: Option<String>) -> CheckResult {
        CheckResult {
            check_id: "check".into(),
            agent_id: "agent".into(),
            timestamp: Utc::now(),
            latency_ms: 0,
            status_code,
            error,
        }
    }

    #[test]
    fn eval_maps_result_to_status() {
        let cases: [(&str, Option<u16>, Option<String>, Status); 8] = [
            ("error wins over 2xx", Some(200), Some("boom".into()), Status::Down),
            ("error with no code is down", None, Some("boom".into()), Status::Down),
            ("200 is up (lower edge)", Some(200), None, Status::Up),
            ("399 is up (upper edge)", Some(399), None, Status::Up),
            ("400 is down (just past range)", Some(400), None, Status::Down),
            ("199 is down (just below range)", Some(199), None, Status::Down),
            ("500 is down", Some(500), None, Status::Down),
            ("no code, no error is unknown", None, None, Status::Unknown),
        ];
        for (name, code, err, want) in cases {
            assert_eq!(default_eval(&cr(code, err)), want, "case: {name}");
        }
    }
}
