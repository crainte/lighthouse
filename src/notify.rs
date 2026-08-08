//! Notification backends for sending alerts.
//!
//! Supports multiple notification methods (webhook, NTFY, etc.) with a common
//! interface. Each notifier is configured in the coordinator config and processes
//! transitions independently.

use crate::coordinator::state::Transition;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// A notification backend that can send alerts for check transitions.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Notifier {
    /// Webhook: POST transition JSON to a URL.
    Webhook { url: String },
    /// NTFY: Publish transition to an NTFY topic.
    Ntfy {
        topic: String,
        #[serde(default)]
        base_url: Option<String>,
    },
}

/// Send a transition notification via the given notifier.
/// Best-effort: failures are logged but do not block.
pub async fn send(client: &Client, notifier: &Notifier, t: &Transition) {
    match notifier {
        Notifier::Webhook { url } => send_webhook(client, url, t).await,
        Notifier::Ntfy { topic, base_url } => {
            send_ntfy(
                client,
                base_url.as_deref().unwrap_or("https://ntfy.sh"),
                topic,
                t,
            )
            .await
        }
    }
}

/// POST a simple JSON alert payload to a webhook. Best-effort.
async fn send_webhook(client: &Client, url: &str, t: &Transition) {
    let payload = serde_json::json!({
        "check_id": t.check_id,
        "from": t.from.as_str(),
        "to": t.to.as_str(),
    });
    const DISPATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    match tokio::time::timeout(DISPATCH_TIMEOUT, client.post(url).json(&payload).send()).await {
        Ok(Err(e)) => warn!(error = %e, check = %t.check_id, "webhook notification failed"),
        Err(_) => warn!(check = %t.check_id, "webhook notification timed out"),
        Ok(Ok(_)) => {}
    }
}

/// POST a transition to an NTFY topic. Best-effort.
async fn send_ntfy(client: &Client, base_url: &str, topic: &str, t: &Transition) {
    let url = format!("{}/{}", base_url, topic);
    let title = format!("Check {} → {}", t.check_id, t.to.as_str());
    let body = format!(
        "Status changed from {} to {}",
        t.from.as_str(),
        t.to.as_str()
    );

    const DISPATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    match tokio::time::timeout(
        DISPATCH_TIMEOUT,
        client.post(&url).header("Title", title).body(body).send(),
    )
    .await
    {
        Ok(Err(e)) => {
            warn!(error = %e, check = %t.check_id, topic = %topic, "ntfy notification failed")
        }
        Err(_) => warn!(check = %t.check_id, topic = %topic, "ntfy notification timed out"),
        Ok(Ok(_)) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_serializes() {
        let notifier = Notifier::Webhook {
            url: "https://example.com/webhook".to_string(),
        };
        let json = serde_json::to_string(&notifier).unwrap();
        assert!(json.contains("webhook"));
        assert!(json.contains("example.com"));
    }

    #[test]
    fn ntfy_serializes_with_defaults() {
        let notifier = Notifier::Ntfy {
            topic: "my-topic".to_string(),
            base_url: None,
        };
        let json = serde_json::to_string(&notifier).unwrap();
        assert!(json.contains("ntfy"));
        assert!(json.contains("my-topic"));
    }

    #[test]
    fn ntfy_serializes_with_custom_base() {
        let notifier = Notifier::Ntfy {
            topic: "my-topic".to_string(),
            base_url: Some("https://notify.example.com".to_string()),
        };
        let json = serde_json::to_string(&notifier).unwrap();
        assert!(json.contains("notify.example.com"));
    }

    #[test]
    fn webhook_deserializes() {
        let json = r#"{"type":"webhook","url":"https://example.com/webhook"}"#;
        let notifier: Notifier = serde_json::from_str(json).unwrap();
        match notifier {
            Notifier::Webhook { url } => assert_eq!(url, "https://example.com/webhook"),
            _ => panic!("expected webhook"),
        }
    }

    #[test]
    fn ntfy_deserializes() {
        let json = r#"{"type":"ntfy","topic":"my-topic"}"#;
        let notifier: Notifier = serde_json::from_str(json).unwrap();
        match notifier {
            Notifier::Ntfy { topic, base_url } => {
                assert_eq!(topic, "my-topic");
                assert_eq!(base_url, None);
            }
            _ => panic!("expected ntfy"),
        }
    }
}
