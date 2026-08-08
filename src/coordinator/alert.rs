pub use crate::coordinator::state::Transition;
use crate::models::Status;
use crate::notify::Notifier;
use reqwest::Client;

/// Decide whether a transition warrants an alert.
pub fn should_alert(t: &Transition) -> bool {
    t.to == Status::Down || t.from == Status::Down
}

/// Send a transition notification via the given notifier.
pub async fn send(client: &Client, notifier: &Notifier, t: &Transition) {
    let _ = crate::notify::send(client, notifier, t).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(from: Status, to: Status) -> Transition {
        Transition {
            check_id: "c".into(),
            from,
            to,
        }
    }

    #[test]
    fn alerts_on_any_down_edge_only() {
        let cases: [(&str, Status, Status, bool); 6] = [
            ("into down", Status::Up, Status::Down, true),
            ("out of down to up", Status::Down, Status::Up, true),
            (
                "out of down to unknown",
                Status::Down,
                Status::Unknown,
                true,
            ),
            (
                "into down from unknown",
                Status::Unknown,
                Status::Down,
                true,
            ),
            ("up to unknown, no down", Status::Up, Status::Unknown, false),
            ("unknown to up, no down", Status::Unknown, Status::Up, false),
        ];
        for (name, from, to, want) in cases {
            assert_eq!(should_alert(&t(from, to)), want, "case: {name}");
        }
    }
}
