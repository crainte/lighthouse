//! A hybrid logical clock, just enough of one to order config edits.
//!
//! Config replication is last-writer-wins, so "last" has to mean something
//! across machines that disagree about the time. A wall clock alone is not
//! enough: an NTP step backwards would make a row un-writable until the clock
//! caught up, and two nodes whose clocks differ by a second would silently
//! decide the older edit won.
//!
//! So every stamp is `max(local wall clock, highest stamp ever seen + 1)`, and
//! every *received* stamp is observed before it is compared. A node with a slow
//! clock still produces increasing stamps, and a node that hears the future
//! stops living in the past.
//!
//! Ties break on `node_id`, which is arbitrary but identical everywhere, so all
//! nodes pick the same winner without exchanging a message about it.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// A stamp on a replicated config row: when, and who said so.
///
/// `Ord` is the merge rule, so it lives here rather than being re-derived at
/// each callsite — four copies of "is this newer?" is how this drifts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hlc {
    /// Milliseconds since epoch, monotonically bumped. First field so the
    /// derived `Ord` compares time before identity.
    pub millis: i64,
    pub node_id: String,
}

impl Hlc {
    pub fn new(millis: i64, node_id: impl Into<String>) -> Self {
        Self {
            millis,
            node_id: node_id.into(),
        }
    }
}

/// The clock itself: one per process, shared.
pub struct Clock {
    node_id: String,
    last: Mutex<i64>,
}

impl Clock {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            last: Mutex::new(0),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Stamp a local write. Never returns the same value twice, so two edits
    /// within one millisecond still order.
    pub fn now(&self) -> Hlc {
        self.at(chrono::Utc::now().timestamp_millis())
    }

    /// `now()` with the wall clock supplied, so a test can skew it.
    pub fn at(&self, wall_millis: i64) -> Hlc {
        let mut last = self.last.lock();
        *last = wall_millis.max(*last + 1);
        Hlc::new(*last, self.node_id.clone())
    }

    /// Take note of a stamp that arrived from a peer. Called on every received
    /// entry: without it a node whose clock is behind would keep losing every
    /// merge, and its own edits would never take effect.
    pub fn observe(&self, other: &Hlc) {
        let mut last = self.last.lock();
        *last = (*last).max(other.millis);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_increase_even_when_the_wall_clock_does_not() {
        let c = Clock::new("a");
        let first = c.at(1_000);
        // Same millisecond twice, then a clock that steps backwards: neither
        // may produce a stamp that is not greater than the last.
        let second = c.at(1_000);
        let third = c.at(500);
        assert!(second > first, "{second:?} !> {first:?}");
        assert!(third > second, "{third:?} !> {second:?}");
    }

    #[test]
    fn observing_the_future_pulls_the_clock_forward() {
        let c = Clock::new("a");
        c.at(1_000);
        let ahead = Hlc::new(9_000, "b");
        c.observe(&ahead);
        // The next local stamp must beat what we heard, or this node could
        // never overwrite the peer's row.
        assert!(c.at(1_001) > ahead);
    }

    #[test]
    fn ties_break_on_node_id_the_same_way_everywhere() {
        let a = Hlc::new(7, "a");
        let b = Hlc::new(7, "b");
        assert!(b > a);
        // Time still dominates identity, or a node named "z" would win forever.
        assert!(Hlc::new(8, "a") > b);
    }

    #[test]
    fn the_wire_form_is_stable() {
        let json = serde_json::to_string(&Hlc::new(42, "n1")).unwrap();
        assert_eq!(json, r#"{"millis":42,"node_id":"n1"}"#);
    }
}
