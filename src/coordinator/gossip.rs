//! Best-effort state replication between coordinator peers.
//!
//! **One hop, deliberately.** A coordinator gossips transitions it observed
//! itself and never re-gossips one it received. That makes cycles impossible
//! without a message id, a seen-set or a TTL: with N peers cross-linked, every
//! peer still hears every transition exactly once, because the only sender is
//! the node that saw it.
//!
//! **Fire-and-forget.** Ingest is the path the whole fleet POSTs into, so it
//! must never wait on a peer. A coordinator that is down or wedged costs
//! replication lag, never ingest throughput.
//!
//! ponytail: no anti-entropy. A peer that is unreachable when a transition
//! happens misses it permanently — there is no retry queue and no reconcile
//! sweep. Add a periodic `/api/v1/status` pull between peers if silent
//! divergence ever matters more than the code it costs.

use crate::{
    coordinator::{routes::peer::ReplicatedAlert, state::Transition},
    models::NodeState,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::warn;

/// A peer that accepts the connection and never answers would otherwise hold a
/// spawned task forever, one per transition.
const GOSSIP_TIMEOUT: Duration = Duration::from_secs(5);

/// A transition on the wire.
///
/// `NodeState` already serialises lowercase, so this needs no separate encoding
/// — the JSON is `{"check_id":…,"from":"up","to":"down","timestamp":…}`.
///
/// **Widening `Status` to `NodeState` here is additive, not breaking.** A peer
/// on an older binary that receives `"degraded"` parses it through a fallback
/// that yields Unknown rather than failing, and Unknown drops out of every
/// rollup denominator. So a mixed-version cluster is briefly less informed
/// about partial outages and never wrong about total ones. Full-outage
/// transitions are unaffected in either direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    #[serde(default)]
    pub alert_id: Option<String>,
    /// The immutable event payload for `alert_id`, carried by new senders so a
    /// receiver never rebuilds the same id from its own incident assignment.
    #[serde(default)]
    pub alert: Option<ReplicatedAlert>,
    pub check_id: crate::models::CheckId,
    pub from: NodeState,
    pub to: NodeState,
    /// Milliseconds since epoch, taken from the observing coordinator's result
    /// rather than the receiver's clock, so replicated history lines up.
    pub timestamp: i64,
    /// The incident the observer stamped this transition with, if any.
    /// `#[serde(default)]` so a sender on an older binary — or a test that
    /// posts a bare transition, as `tests/gossip.rs` does — still
    /// deserialises; the receiver falls back to guessing from its own local
    /// state when this is absent.
    #[serde(default)]
    pub incident_id: Option<String>,
}

impl Message {
    pub fn new(
        t: &Transition,
        timestamp: i64,
        alert: Option<ReplicatedAlert>,
        incident_id: Option<String>,
    ) -> Self {
        Self {
            alert_id: alert.as_ref().map(|alert| alert.id.clone()),
            alert,
            check_id: t.check_id,
            from: t.from,
            to: t.to,
            timestamp,
            incident_id,
        }
    }

    pub fn transition(&self) -> Transition {
        Transition {
            check_id: self.check_id,
            from: self.from,
            to: self.to,
        }
    }
}

/// The configured peer set, plus the client and credential used to reach it.
///
/// Constructed once and shared, so a transition costs one `Arc` clone rather
/// than rebuilding a connection pool per broadcast.
pub struct Peers {
    client: reqwest::Client,
    urls: Vec<String>,
    /// Peers authenticate as agents do. Reusing that token keeps clustering to
    /// one config key instead of introducing a second shared secret to rotate.
    token: Option<String>,
}

impl Peers {
    /// `None` when no peers are configured, so "standalone" is encoded in the
    /// type and never re-checked at the callsite.
    pub fn new(urls: &[String], token: Option<&str>) -> Option<Self> {
        if urls.is_empty() {
            return None;
        }
        Some(Self {
            client: reqwest::Client::new(),
            urls: urls.to_vec(),
            token: token.map(str::to_owned),
        })
    }

    /// POST a transition to every peer. Errors are logged and abandoned: this
    /// runs detached from ingest, so there is nobody to return a failure to.
    pub async fn broadcast(&self, msg: &Message) {
        for peer in &self.urls {
            let url = endpoint(peer);
            let mut req = self.client.post(&url).json(msg);
            if let Some(t) = &self.token {
                req = req.bearer_auth(t);
            }
            match tokio::time::timeout(GOSSIP_TIMEOUT, req.send()).await {
                Ok(Ok(r)) if r.status().is_success() => {}
                Ok(Ok(r)) => {
                    warn!(%peer, status = %r.status(), check = %msg.check_id, "gossip rejected")
                }
                Ok(Err(e)) => warn!(%peer, error = %e, check = %msg.check_id, "gossip failed"),
                Err(_) => warn!(%peer, check = %msg.check_id, "gossip timed out"),
            }
        }
    }
}

/// Peers are configured as base URLs, so a trailing slash is the obvious way to
/// write one and must not produce a double slash in the path.
fn endpoint(peer: &str) -> String {
    format!("{}/internal/gossip", peer.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_urls_join_without_a_double_slash() {
        let want = "http://c2:8080/internal/gossip";
        assert_eq!(endpoint("http://c2:8080"), want);
        assert_eq!(endpoint("http://c2:8080/"), want);
        assert_eq!(endpoint("http://c2:8080///"), want);
    }

    #[test]
    fn no_peers_means_no_fan_out() {
        assert!(Peers::new(&[], None).is_none());
        assert!(Peers::new(&["http://c2:8080".into()], None).is_some());
    }

    /// The wire form is the contract between two coordinator versions, so the
    /// status encoding is pinned here rather than left to a derive.
    #[test]
    fn transition_survives_the_wire_unchanged() {
        let t = Transition {
            check_id: crate::models::CheckId::from_u128(1),
            from: NodeState::Up,
            to: NodeState::Down,
        };
        let msg = Message::new(&t, 1_700_000_000_000, None, None);

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""from":"up""#), "{json}");
        assert!(json.contains(r#""to":"down""#), "{json}");

        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
        assert_eq!(back.transition(), t);
    }

    /// Degraded is new vocabulary. Pin its encoding too, because this is the
    /// word an older peer will not recognise and the whole compatibility
    /// argument rests on how it is spelled.
    #[test]
    fn degraded_goes_on_the_wire_as_a_plain_lowercase_word() {
        let t = Transition {
            check_id: crate::models::CheckId::from_u128(1),
            from: NodeState::Up,
            to: NodeState::Degraded,
        };
        let json = serde_json::to_string(&Message::new(&t, 1, None, None)).unwrap();
        assert!(json.contains(r#""to":"degraded""#), "{json}");
        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(back.to, NodeState::Degraded);
    }
}
