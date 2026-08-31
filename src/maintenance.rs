//! Scheduled windows during which a check's transitions page nobody.
//!
//! Deliberately only that: the probe still runs, the result is still recorded,
//! the alert row is still written and an incident still opens — exactly the
//! contract a check with `notify = false` already has, on a timer instead of a
//! flag. See §8 of `uptime-monitor-design.md`.

use serde::{Deserialize, Serialize};

/// One span. Windows are edited as a list in **Admin > Maintenance** and stored
/// as a single settings document, so there is no per-window id: the list is
/// always replaced whole, the same argument as the notifier list.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceWindow {
    /// What the work is. Published verbatim while `announce`, so it is the
    /// operator's sentence rather than a generated one.
    pub title: String,
    /// Epoch millis, inclusive.
    pub starts_at: i64,
    /// Epoch millis, exclusive — so two back-to-back windows do not both own
    /// the instant they share.
    pub ends_at: i64,
    /// Checks it covers. Empty means every check: the "whole platform is down
    /// for twenty minutes" case, and the reason there is no separate `all` flag.
    #[serde(default)]
    pub checks: Vec<String>,
    /// Show it to anonymous visitors while it runs. Off is a silent window:
    /// suppress the pages, tell nobody.
    #[serde(default)]
    pub announce: bool,
}

impl MaintenanceWindow {
    /// Trim what the form cannot see. A check name with a stray space matches
    /// nothing, which would read as the window simply not working.
    pub fn sanitized(mut self) -> Self {
        self.title = self.title.trim().to_string();
        let mut seen = std::collections::HashSet::new();
        self.checks = self
            .checks
            .into_iter()
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty() && seen.insert(c.clone()))
            .collect();
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.title.is_empty() {
            return Err("a maintenance window needs a title".into());
        }
        if self.ends_at <= self.starts_at {
            return Err(format!("{}: the window ends before it starts", self.title));
        }
        Ok(())
    }

    /// Running at `at`.
    pub fn active_at(&self, at: i64) -> bool {
        self.starts_at <= at && at < self.ends_at
    }

    /// Running at `at` *and* covering this check — the question the notifier
    /// path asks. Answered against the transition's own timestamp, not the
    /// clock, so a batch posted late is judged by when it happened.
    pub fn covers(&self, check_id: &str, at: i64) -> bool {
        self.active_at(at) && (self.checks.is_empty() || self.checks.iter().any(|c| c == check_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(checks: &[&str]) -> MaintenanceWindow {
        MaintenanceWindow {
            title: "db upgrade".into(),
            starts_at: 1_000,
            ends_at: 2_000,
            checks: checks.iter().map(|c| (*c).to_string()).collect(),
            announce: false,
        }
    }

    /// Half-open on purpose: back-to-back windows must not both claim the
    /// boundary instant, and a window is over the moment it ends.
    #[test]
    fn the_window_includes_its_start_and_excludes_its_end() {
        let w = window(&[]);
        assert!(!w.active_at(999));
        assert!(w.active_at(1_000));
        assert!(w.active_at(1_999));
        assert!(!w.active_at(2_000));
    }

    /// An empty check list is the whole fleet, not nothing — the difference
    /// between "silence everything for twenty minutes" and a window that does
    /// nothing at all.
    #[test]
    fn an_empty_check_list_covers_every_check() {
        let w = window(&[]);
        assert!(w.covers("anything", 1_500));
        assert!(!w.covers("anything", 2_500));
    }

    #[test]
    fn a_named_window_covers_only_what_it_names() {
        let w = window(&["db", "api"]);
        assert!(w.covers("db", 1_500));
        assert!(w.covers("api", 1_500));
        assert!(!w.covers("web", 1_500));
    }

    #[test]
    fn a_window_that_ends_before_it_starts_is_refused() {
        let mut w = window(&[]);
        w.ends_at = w.starts_at;
        assert!(w.validate().is_err());
        w.title = String::new();
        assert!(w.validate().is_err());
    }

    /// A check name typed with a trailing space would match nothing, which
    /// reads as the window silently not working.
    #[test]
    fn names_are_trimmed_and_deduplicated() {
        let w = MaintenanceWindow {
            title: "  db upgrade  ".into(),
            checks: vec![" db ".into(), "db".into(), "  ".into()],
            ..window(&[])
        }
        .sanitized();
        assert_eq!(w.title, "db upgrade");
        assert_eq!(w.checks, vec!["db".to_string()]);
    }
}
