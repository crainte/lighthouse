pub use crate::coordinator::state::Transition;
use crate::notify::Notifier;
use reqwest::Client;

/// Decide whether a transition warrants an alert.
///
/// The rule is "any edge into or out of a failing state" — which now includes
/// Degraded, because a partial outage is a thing that happened. Note what this
/// does *not* gate: whether a notifier is reached. `notify_degraded` decides
/// that at the dispatch site, exactly as `notify` already does, so a degraded
/// transition that nobody wants paging for still leaves an alert row and an
/// entry in the feed. Silence about a partial outage is a routing preference,
/// not a reason to lose the record of it.
pub fn should_alert(t: &Transition) -> bool {
    t.to.is_failing() || t.from.is_failing()
}

/// Send a transition notification via the given notifier. Kept for callers
/// that only have a transition (not coordinator metadata); production routing
/// supplies the real mutable display name directly to `notify::send`.
pub async fn send(client: &Client, notifier: &Notifier, t: &Transition) {
    let _ = crate::notify::send(client, notifier, t, &t.check_id.to_string()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::NodeState;

    fn t(from: NodeState, to: NodeState) -> Transition {
        Transition {
            check_id: crate::models::CheckId::from_u128(1),
            from,
            to,
        }
    }

    #[test]
    fn alerts_on_any_failing_edge_only() {
        use NodeState::{Degraded, Down, Unknown, Up};
        let cases: [(&str, NodeState, NodeState, bool); 12] = [
            ("into down", Up, Down, true),
            ("out of down to up", Down, Up, true),
            ("out of down to unknown", Down, Unknown, true),
            ("into down from unknown", Unknown, Down, true),
            ("up to unknown, no failure", Up, Unknown, false),
            ("unknown to up, no failure", Unknown, Up, false),
            // The four Degraded edges. Each is a real change in what the
            // service is doing for somebody, so each gets a row.
            ("partial outage begins", Up, Degraded, true),
            ("partial outage spreads", Degraded, Down, true),
            ("outage contracts to partial", Down, Degraded, true),
            ("partial outage clears", Degraded, Up, true),
            ("degraded goes silent", Degraded, Unknown, true),
            ("degraded from silence", Unknown, Degraded, true),
        ];
        for (name, from, to, want) in cases {
            assert_eq!(should_alert(&t(from, to)), want, "case: {name}");
        }
    }
}
