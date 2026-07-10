use crate::models::Status;
use std::collections::HashMap;

/// A state transition worth acting on (alerting).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub check_id: String,
    pub from: Status,
    pub to: Status,
}

/// Tracks the last known status per check and detects transitions.
/// Pure logic, no I/O — the unit worth TDD.
#[derive(Default)]
pub struct StateTracker {
    last: HashMap<String, Status>,
}

impl StateTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an observed status for a check. Returns `Some(Transition)`
    /// only when the status actually changed from a previously-known value.
    /// The first observation of a check establishes a baseline and does NOT
    /// emit a transition (nothing to alert on yet).
    pub fn observe(&mut self, check_id: &str, status: Status) -> Option<Transition> {
        let _ = (check_id, status);
        todo!("implement in green phase")
    }

    /// The most recently observed status for a check, if any.
    pub fn current(&self, check_id: &str) -> Option<Status> {
        let _ = check_id;
        todo!("implement in green phase")
    }
}
