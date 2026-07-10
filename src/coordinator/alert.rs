use crate::coordinator::state::Transition;
use crate::models::Status;
use serde_json::json;
use tracing::warn;

/// Decide whether a transition warrants an alert.
pub fn should_alert(t: &Transition) -> bool {
    let _ = t;
    todo!("implement in green phase")
}

/// POST a simple JSON alert payload to a webhook. Best-effort.
pub async fn send_webhook(client: &reqwest::Client, url: &str, t: &Transition) {
    let payload = json!({
        "check_id": t.check_id,
        "from": t.from.as_str(),
        "to": t.to.as_str(),
    });
    if let Err(e) = client.post(url).json(&payload).send().await {
        warn!(error = %e, check = %t.check_id, "alert webhook failed");
    }
}
