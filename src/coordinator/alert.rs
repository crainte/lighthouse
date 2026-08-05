pub use crate::coordinator::state::Transition;
use crate::models::Status;
use serde_json::json;
use tracing::warn;

/// Decide whether a transition warrants an alert.
pub fn should_alert(t: &Transition) -> bool {
    t.to == Status::Down || t.from == Status::Down
}

/// POST a simple JSON alert payload to a webhook. Best-effort.
pub async fn send_webhook(client: &reqwest::Client, url: &str, t: &Transition) {
    let payload = json!({
        "check_id": t.check_id,
        "from": t.from.as_str(),
        "to": t.to.as_str(),
    });
    // A notification endpoint that accepts the connection and never answers
    // would otherwise hold this task forever, and every alert behind it.
    const DISPATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    match tokio::time::timeout(DISPATCH_TIMEOUT, client.post(url).json(&payload).send()).await {
        Ok(Err(e)) => warn!(error = %e, check = %t.check_id, "alert webhook failed"),
        Err(_) => warn!(check = %t.check_id, "alert webhook timed out"),
        Ok(Ok(_)) => {}
    }
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
