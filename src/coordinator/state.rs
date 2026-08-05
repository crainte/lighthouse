use crate::models::Status;
use std::collections::HashMap;

/// A state transition worth acting on (alerting).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub check_id: String,
    pub from: Status,
    pub to: Status,
}

/// Tracks the last CONFIRMED status per check and detects transitions.
///
/// "Confirmed" is the point of the tracker: a check does not flip to DOWN on
/// the first failed probe. `failures_before_down` consecutive DOWN observations
/// must land before the published state moves, so a single dropped probe or a
/// flaky hop does not page anyone. UP is never gated — one good observation
/// restores the check, because treating recovery as requiring a run of UPs
/// would make every outage last longer than it did.
///
/// Pure logic, no I/O — the unit worth TDD.
#[derive(Default)]
pub struct StateTracker {
    /// Confirmed status per check: the state the dashboard, incidents and
    /// alerts all read.
    last: HashMap<String, Status>,
    /// Consecutive DOWN observations toward the threshold. Reset by any UP;
    /// untouched by Unknown (an un-evaluable probe is neither a failure nor a
    /// recovery).
    failures: HashMap<String, u32>,
}

impl StateTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an observed status for a check. Returns `Some(Transition)` only
    /// when the CONFIRMED status changed — a DOWN below the threshold emits
    /// nothing. The first observation establishes a baseline and never emits a
    /// transition (nothing to alert on yet).
    pub fn observe(&mut self, check_id: &str, status: Status, threshold: u32) -> Option<Transition> {
        let threshold = threshold.max(1);
        match status {
            Status::Up => {
                self.failures.insert(check_id.to_string(), 0);
                self.flip(check_id, status)
            }
            Status::Down => {
                let n = self.failures.entry(check_id.to_string()).or_insert(0);
                *n = n.saturating_add(1);
                if *n < threshold {
                    // Still inside the confirmation window: the check reads as
                    // it did before, and nothing may alert or open an incident.
                    return None;
                }
                self.flip(check_id, status)
            }
            // Neither a success nor a confirmed failure: no transition, and the
            // failure counter is left alone so an Unknown between two failures
            // does not quietly reset the confirmation.
            Status::Unknown => None,
        }
    }

    /// Emit a transition when the confirmed status actually changed.
    fn flip(&mut self, check_id: &str, status: Status) -> Option<Transition> {
        match self.last.insert(check_id.to_string(), status) {
            Some(prev) if prev != status => Some(Transition {
                check_id: check_id.to_string(),
                from: prev,
                to: status,
            }),
            _ => None,
        }
    }

    /// The most recently CONFIRMED status for a check, if any.
    pub fn current(&self, check_id: &str) -> Option<Status> {
        self.last.get(check_id).copied()
    }

    /// Consecutive DOWN observations currently toward the threshold.
    pub fn failures(&self, check_id: &str) -> u32 {
        self.failures.get(check_id).copied().unwrap_or(0)
    }

    /// Restore state after a restart, from persisted `current_state`. The
    /// confirmed status and the in-flight failure count both survive, so a
    /// check two failures into a three-failure confirmation resumes where it
    /// left off instead of starting over (or alerting on the first result).
    pub fn seed(&mut self, check_id: &str, status: Status, consecutive_failures: u32) {
        self.last.insert(check_id.to_string(), status);
        self.failures.insert(check_id.to_string(), consecutive_failures);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tracker_has_no_state() {
        let mut t = StateTracker::new();
        assert_eq!(t.current("c"), None);
        assert_eq!(t.observe("c", Status::Up, 1), None);
    }

    #[test]
    fn first_observation_sets_baseline_without_transition() {
        let mut t = StateTracker::new();
        assert_eq!(t.observe("c", Status::Down, 1), None);
        assert_eq!(t.current("c"), Some(Status::Down));
    }

    #[test]
    fn same_status_yields_no_transition() {
        let mut t = StateTracker::new();
        t.observe("c", Status::Up, 1);
        assert_eq!(t.observe("c", Status::Up, 1), None);
    }

    #[test]
    fn changed_status_yields_transition() {
        let mut t = StateTracker::new();
        t.observe("c", Status::Up, 1);
        assert_eq!(
            t.observe("c", Status::Down, 1),
            Some(Transition {
                check_id: "c".into(),
                from: Status::Up,
                to: Status::Down,
            })
        );
    }

    #[test]
    fn checks_are_tracked_independently() {
        let mut t = StateTracker::new();
        t.observe("a", Status::Up, 1);
        assert_eq!(t.observe("b", Status::Down, 1), None);
        assert_eq!(t.current("a"), Some(Status::Up));
        assert_eq!(t.current("b"), Some(Status::Down));
    }

    #[test]
    fn threshold_holds_the_state_up_until_confirmed() {
        let mut t = StateTracker::new();
        t.observe("c", Status::Up, 3);
        // Two failures: still reading UP, no transition.
        assert_eq!(t.observe("c", Status::Down, 3), None);
        assert_eq!(t.current("c"), Some(Status::Up));
        assert_eq!(t.observe("c", Status::Down, 3), None);
        assert_eq!(t.current("c"), Some(Status::Up));
        // Third consecutive failure confirms it.
        assert_eq!(
            t.observe("c", Status::Down, 3),
            Some(Transition {
                check_id: "c".into(),
                from: Status::Up,
                to: Status::Down,
            })
        );
        assert_eq!(t.current("c"), Some(Status::Down));
    }

    #[test]
    fn one_up_observation_resets_the_count_and_recovers() {
        let mut t = StateTracker::new();
        t.observe("c", Status::Up, 3);
        t.observe("c", Status::Down, 3);
        t.observe("c", Status::Down, 3);
        // An UP inside the window cancels the confirmation entirely.
        assert_eq!(t.observe("c", Status::Up, 3), None);
        assert_eq!(t.failures("c"), 0);
        assert_eq!(t.observe("c", Status::Down, 3), None);
        assert_eq!(t.current("c"), Some(Status::Up));
        // The DOWN after the reset is back to being the first of a new window.
        assert_eq!(t.failures("c"), 1);
    }

    #[test]
    fn recovery_is_never_gated_by_a_threshold() {
        let mut t = StateTracker::new();
        t.observe("c", Status::Up, 5);
        for _ in 0..5 {
            t.observe("c", Status::Down, 5);
        }
        assert_eq!(t.current("c"), Some(Status::Down));
        // One UP after a confirmed DOWN restores immediately, threshold or not.
        assert_eq!(
            t.observe("c", Status::Up, 5),
            Some(Transition {
                check_id: "c".into(),
                from: Status::Down,
                to: Status::Up,
            })
        );
    }

    #[test]
    fn unknown_does_not_disturb_the_count() {
        let mut t = StateTracker::new();
        t.observe("c", Status::Up, 2);
        t.observe("c", Status::Down, 2);
        assert_eq!(t.observe("c", Status::Unknown, 2), None);
        assert_eq!(t.failures("c"), 1);
        // The unknown probe neither confirmed nor cancelled the failure.
        assert_eq!(
            t.observe("c", Status::Down, 2),
            Some(Transition {
                check_id: "c".into(),
                from: Status::Up,
                to: Status::Down,
            })
        );
    }

    #[test]
    fn seed_restores_state_and_count() {
        let mut t = StateTracker::new();
        t.seed("c", Status::Up, 2);
        assert_eq!(t.current("c"), Some(Status::Up));
        assert_eq!(t.failures("c"), 2);
        // Seeded two failures into a three-failure confirmation: one more DOWN
        // confirms, and the transition fires because the confirmed state moves.
        assert_eq!(
            t.observe("c", Status::Down, 3),
            Some(Transition {
                check_id: "c".into(),
                from: Status::Up,
                to: Status::Down,
            })
        );
        assert_eq!(t.current("c"), Some(Status::Down));
    }

    #[test]
    fn zero_threshold_reads_as_one() {
        let mut t = StateTracker::new();
        t.observe("c", Status::Up, 0);
        assert_eq!(
            t.observe("c", Status::Down, 0),
            Some(Transition {
                check_id: "c".into(),
                from: Status::Up,
                to: Status::Down,
            })
        );
    }
}
