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
        match self.last.insert(check_id.to_string(), status) {
            Some(prev) if prev != status => Some(Transition {
                check_id: check_id.to_string(),
                from: prev,
                to: status,
            }),
            _ => None,
        }
    }

    /// The most recently observed status for a check, if any.
    pub fn current(&self, check_id: &str) -> Option<Status> {
        self.last.get(check_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tracker_has_no_state() {
        let t = StateTracker::new();
        assert_eq!(t.current("a"), None);
    }

    #[test]
    fn first_observation_sets_baseline_without_transition() {
        let mut t = StateTracker::new();
        assert_eq!(t.observe("a", Status::Up), None);
        assert_eq!(t.current("a"), Some(Status::Up));
    }

    #[test]
    fn same_status_yields_no_transition() {
        let mut t = StateTracker::new();
        t.observe("a", Status::Up);
        assert_eq!(t.observe("a", Status::Up), None);
        assert_eq!(t.current("a"), Some(Status::Up));
    }

    #[test]
    fn changed_status_yields_transition() {
        let mut t = StateTracker::new();
        t.observe("a", Status::Up);
        assert_eq!(
            t.observe("a", Status::Down),
            Some(Transition {
                check_id: "a".into(),
                from: Status::Up,
                to: Status::Down,
            })
        );
        assert_eq!(t.current("a"), Some(Status::Down));
    }

    #[test]
    fn checks_are_tracked_independently() {
        let mut t = StateTracker::new();
        t.observe("a", Status::Up);
        t.observe("b", Status::Down);
        // Observing "b" must not disturb "a"'s baseline.
        assert_eq!(t.observe("a", Status::Up), None);
        assert_eq!(t.current("a"), Some(Status::Up));
        assert_eq!(t.current("b"), Some(Status::Down));
    }
}
