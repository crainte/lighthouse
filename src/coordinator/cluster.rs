//! HTTP cluster membership and failure detection.
//!
//! **Push-pull, not push.** Every ping carries the sender's whole member view
//! and the reply carries the receiver's, so one round trip does the work of
//! two separate broadcasts and a node that only ever contacts its seeds still
//! learns about everyone the seed has heard from. A seeded with B, B knows C,
//! and A learns C on its very first ping — no separate discovery protocol.
//!
//! **Generation, not vector clocks.** Each process picks its start time as a
//! generation once, at boot, and never bumps it again. That is enough to
//! order "the same node before and after a restart" — which is the only
//! ordering membership needs — without the bookkeeping a vector clock would
//! cost for a property nothing here uses.
//!
//! **Dead members are retained, not dropped.** Forgetting a dead node would
//! make its return indistinguishable from a brand new node joining, and the
//! generation check exists precisely so a restart is recognised rather than
//! rediscovered.
//!
//! **No death notification.** A coordinator paging about its own cluster is a
//! different product surface than the uptime monitor it runs, and the most
//! likely node to want to page is the one that just went quiet. A `warn!` on
//! state change is what this module owns.

use futures_util::{stream, StreamExt};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// A peer that accepts the connection and never answers would otherwise hold a
/// spawned task forever, one per ping. Matches `gossip::GOSSIP_TIMEOUT`.
const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// The data plane's budget. A replication or config batch is applied inside one
/// transaction on the receiver, behind its `state_write` and its single
/// database worker, so it cannot share the control plane's ping budget: five
/// seconds abandons work the peer is still committing, counts a success as a
/// failure, and hands anti-entropy a repair for data that arrived anyway.
const DATA_TIMEOUT: Duration = Duration::from_secs(30);

/// Liveness as this node currently believes it, one member at a time.
///
/// Ordered worst-to-best is not needed: nothing compares states, only checks
/// equality and applies the sweep's fixed transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberState {
    Alive,
    Suspect,
    Dead,
}

impl MemberState {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemberState::Alive => "alive",
            MemberState::Suspect => "suspect",
            MemberState::Dead => "dead",
        }
    }

    pub fn from_db(s: &str) -> MemberState {
        match s {
            "suspect" => MemberState::Suspect,
            "dead" => MemberState::Dead,
            _ => MemberState::Alive,
        }
    }
}

/// One node as this coordinator currently believes it, on the wire and on
/// disk. Same shape for both: a ping response and a persisted row answer the
/// same question, "who is out there and how sure are we".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Member {
    pub node_id: String,
    pub url: String,
    pub generation: i64,
    pub last_seen: i64,
    pub state: MemberState,
    /// What that node reported it is running. Propagates with membership, so
    /// every node can show the whole cluster's builds and a half-finished
    /// rolling upgrade is visible from any one of them.
    ///
    /// `None` means not heard from since this process started — either a peer
    /// on a build that predates reporting, or an entry restored from disk that
    /// no ping has refreshed yet. Deliberately not persisted: a build read back
    /// from a table is a claim about a process that may since have been
    /// replaced, and a stale build is worse than an absent one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<String>,
    /// The wire protocol that node speaks. `None` from a peer predating it,
    /// which is read as "the protocol before this field existed".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<u32>,
}

/// Sender identity plus its whole member view. Both directions of
/// `POST /internal/cluster/ping` use this exact shape, so a single struct is
/// the request and the response — the receiver answers the question it was
/// just asked.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PingBody {
    pub node_id: String,
    pub url: String,
    pub generation: i64,
    pub members: Vec<Member>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<LinkObservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkOutcome {
    Connected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkObservation {
    pub source: String,
    pub target: String,
    pub source_generation: i64,
    pub observed_at: i64,
    pub outcome: LinkOutcome,
}

/// The peer wire protocol this build speaks.
///
/// Not a compatibility gate, and deliberately not one: every peer payload is
/// additively compatible already — new fields carry `#[serde(default)]`, so an
/// older node ignores what it does not know instead of rejecting the batch.
/// Refusing a peer on a number would break rolling upgrades that currently
/// work.
///
/// It exists for the case defaults cannot absorb: a field whose *meaning*
/// changes, where an old node applies the new value confidently and wrongly.
/// Increment it then, and only then, so that a mismatch shown in the cluster
/// view means "these two nodes disagree about what the bytes mean" rather than
/// "one of them is newer", which the build string already says.
pub const PROTOCOL_VERSION: u32 = 1;

/// The cluster as this node sees it: its own identity, everyone it has heard
/// of, and what it needs to keep asking.
pub struct Cluster {
    self_id: String,
    self_url: String,
    generation: i64,
    members: RwLock<HashMap<String, Member>>,
    links: RwLock<HashMap<(String, String), LinkObservation>>,
    /// Bootstrap addresses retried even after a member learned through them dies,
    /// so a changed address or a healed partition can restore contact.
    seeds: Vec<String>,
    client: reqwest::Client,
    /// Peers authenticate as agents do; see `CoordinatorConfig::resolved_peer_token`.
    token: Option<String>,
    peer_timeout_ms: i64,
}

impl Cluster {
    /// `generation` is the process start time in milliseconds: it only needs
    /// to be higher than whatever this node used last time, and wall-clock
    /// time at boot already guarantees that without persisting a counter.
    pub fn new(
        self_id: impl Into<String>,
        self_url: impl Into<String>,
        generation: i64,
        seeds: Vec<String>,
        token: Option<String>,
        peer_timeout_secs: u64,
    ) -> Self {
        Self {
            self_id: self_id.into(),
            self_url: self_url.into(),
            generation,
            members: RwLock::new(HashMap::new()),
            links: RwLock::new(HashMap::new()),
            seeds,
            client: reqwest::Client::new(),
            token,
            peer_timeout_ms: peer_timeout_secs as i64 * 1000,
        }
    }

    /// Seed the in-memory view from what was persisted, so a restarted node
    /// rejoins from what it already knew instead of waiting to be re-seeded by
    /// whichever peer pings it first.
    pub fn restore(&self, members: Vec<Member>) {
        let mut guard = self.members.write();
        for m in members {
            if m.node_id != self.self_id {
                guard.insert(m.node_id.clone(), m);
            }
        }
    }

    /// Base URLs may be written with a trailing slash, so joining has to be
    /// tolerant of one or several without producing a double slash in the path.
    pub fn endpoint(peer: &str, path: &str) -> String {
        format!(
            "{}/{}",
            peer.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    /// The merge rule, in exactly one place. Every path that learns about a
    /// peer — a ping response, an incoming ping — goes through this, so
    /// "which view wins" is answered once rather than re-decided per caller.
    ///
    /// A higher generation always wins, `last_seen` and `state` included: it
    /// represents a fresh process, and stale gossip about the process it
    /// replaced must not resurrect that old process's state. A lower
    /// generation is stale by definition and contributes nothing. Within the
    /// same generation, the newer `last_seen` wins and carries its `state`
    /// with it, so liveness always advances forward in time.
    ///
    /// The sender is folded in separately from its own `members` list, with
    /// `last_seen` set to now: we did not read that from the payload, we just
    /// received a live TCP connection from it.
    pub fn merge(&self, incoming: &PingBody, now_millis: i64) {
        let mut guard = self.members.write();

        // The sender's own entry travels in `members` (it pushes `self_member`
        // there), so its build is taken from that rather than duplicated onto
        // `PingBody`. Absent only if the sender predates reporting one.
        let said = incoming
            .members
            .iter()
            .find(|m| m.node_id == incoming.node_id);
        let sender = Member {
            node_id: incoming.node_id.clone(),
            url: incoming.url.clone(),
            generation: incoming.generation,
            last_seen: now_millis,
            state: MemberState::Alive,
            build: said.and_then(|m| m.build.clone()),
            protocol: said.and_then(|m| m.protocol),
        };
        Self::merge_one(&mut guard, sender, &self.self_id);

        for m in &incoming.members {
            Self::merge_one(&mut guard, m.clone(), &self.self_id);
        }
        drop(guard);
        self.merge_links(&incoming.links);
    }

    fn merge_links(&self, incoming: &[LinkObservation]) {
        let mut guard = self.links.write();
        for observation in incoming {
            let key = (observation.source.clone(), observation.target.clone());
            let replace = guard.get(&key).is_none_or(|known| {
                observation.source_generation > known.source_generation
                    || (observation.source_generation == known.source_generation
                        && observation.observed_at > known.observed_at)
            });
            if replace {
                guard.insert(key, observation.clone());
            }
        }
    }

    fn observe_link(&self, target: String, observed_at: i64, outcome: LinkOutcome) {
        self.merge_links(&[LinkObservation {
            source: self.self_id.clone(),
            target,
            source_generation: self.generation,
            observed_at,
            outcome,
        }]);
    }

    fn merge_one(guard: &mut HashMap<String, Member>, incoming: Member, self_id: &str) {
        if incoming.node_id == self_id {
            return;
        }
        match guard.get(&incoming.node_id) {
            None => {
                guard.insert(incoming.node_id.clone(), incoming);
            }
            Some(known) if incoming.generation > known.generation => {
                guard.insert(incoming.node_id.clone(), incoming);
            }
            Some(known)
                if incoming.generation == known.generation
                    && incoming.last_seen > known.last_seen =>
            {
                guard.insert(incoming.node_id.clone(), incoming);
            }
            // Lower generation: stale gossip about a process that no longer
            // exists. Ignored entirely, current entry untouched.
            _ => {}
        }
    }

    /// Handle an inbound ping: merge the sender's view, answer with ours. The
    /// HTTP layer is a thin wrapper over this, so the merge rule is testable
    /// without standing up a server.
    pub fn handle_ping(&self, incoming: &PingBody, now_millis: i64) -> PingBody {
        self.merge(incoming, now_millis);
        self.ping_body(now_millis)
    }

    fn ping_body(&self, now_millis: i64) -> PingBody {
        let mut members: Vec<Member> = self.members.read().values().cloned().collect();
        members.push(self.self_member(now_millis));
        PingBody {
            node_id: self.self_id.clone(),
            url: self.self_url.clone(),
            generation: self.generation,
            members,
            links: self.link_snapshot(),
        }
    }

    /// This node's own entry, as of `now_millis` — it is always alive to
    /// itself, and its own last-seen is simply "now".
    pub fn self_member(&self, now_millis: i64) -> Member {
        Member {
            node_id: self.self_id.clone(),
            url: self.self_url.clone(),
            generation: self.generation,
            last_seen: now_millis,
            state: MemberState::Alive,
            build: Some(crate::version::current().to_string()),
            protocol: Some(PROTOCOL_VERSION),
        }
    }

    /// Ping live and suspect members on the normal heartbeat cadence.
    pub async fn ping_all(&self, now_millis: i64) {
        let targets = self
            .members
            .read()
            .values()
            .filter(|m| m.state != MemberState::Dead)
            .map(|m| (m.url.clone(), Some(m.node_id.clone())))
            .collect();
        self.ping_targets(targets, now_millis).await;
    }

    /// Retry dead members and seeds less often than the normal heartbeat. A
    /// seed may now point to a different member, so keep trying its address
    /// even if the old member has moved or died.
    pub async fn retry_discovery(&self, now_millis: i64) {
        let targets = {
            let guard = self.members.read();
            let mut targets: HashMap<String, Option<String>> = guard
                .values()
                .filter(|m| m.state == MemberState::Dead)
                .map(|m| (m.url.clone(), Some(m.node_id.clone())))
                .collect();
            for seed in &self.seeds {
                if seed.trim_end_matches('/') != self.self_url.trim_end_matches('/')
                    && !guard.values().any(|m| {
                        m.state != MemberState::Dead
                            && m.url.trim_end_matches('/') == seed.trim_end_matches('/')
                    })
                {
                    targets.entry(seed.clone()).or_insert(None);
                }
            }
            targets.into_iter().collect()
        };
        self.ping_targets(targets, now_millis).await;
    }

    async fn ping_targets(&self, targets: Vec<(String, Option<String>)>, now_millis: i64) {
        let body = self.ping_body(now_millis);
        stream::iter(targets)
            .for_each_concurrent(32, |(peer, known_id)| {
                let body = &body;
                async move {
                    let url = Self::endpoint(&peer, "internal/cluster/ping");
                    let mut req = self.client.post(&url).json(body);
                    if let Some(t) = &self.token {
                        req = req.bearer_auth(t);
                    }
                    match tokio::time::timeout(PING_TIMEOUT, async {
                        let response = req.send().await?;
                        let status = response.status();
                        if !status.is_success() {
                            return Ok(Err(status));
                        }
                        response.json::<PingBody>().await.map(Ok)
                    })
                    .await
                    {
                        Ok(Ok(Ok(reply))) => {
                            let received_at = chrono::Utc::now().timestamp_millis();
                            self.observe_link(
                                reply.node_id.clone(),
                                received_at,
                                LinkOutcome::Connected,
                            );
                            self.merge(&reply, received_at);
                        }
                        Ok(Ok(Err(status))) => {
                            if let Some(target) = known_id.clone() {
                                self.observe_link(target, now_millis, LinkOutcome::Failed);
                            }
                            warn!(%peer, %status, "cluster ping rejected");
                        }
                        Ok(Err(e)) => {
                            if let Some(target) = known_id.clone() {
                                self.observe_link(target, now_millis, LinkOutcome::Failed);
                            }
                            warn!(%peer, error = %e, "cluster ping failed");
                        }
                        Err(_) => {
                            if let Some(target) = known_id {
                                self.observe_link(target, now_millis, LinkOutcome::Failed);
                            }
                            warn!(%peer, "cluster ping timed out");
                        }
                    }
                }
            })
            .await;
    }

    /// Advance liveness by elapsed silence: `Alive` -> `Suspect` past one
    /// timeout, `Suspect` -> `Dead` past two. Returns the nodes whose state
    /// changed, so the caller can log; nothing here notifies anyone, because a
    /// coordinator paging about its own cluster is a different concern than
    /// the outages it watches for.
    pub fn sweep(&self, now_millis: i64) -> Vec<(String, MemberState)> {
        let mut changed = Vec::new();
        let mut guard = self.members.write();
        for m in guard.values_mut() {
            let silence = now_millis - m.last_seen;
            let next = match m.state {
                MemberState::Alive if silence > self.peer_timeout_ms => Some(MemberState::Suspect),
                MemberState::Suspect if silence > 2 * self.peer_timeout_ms => {
                    Some(MemberState::Dead)
                }
                _ => None,
            };
            if let Some(next) = next {
                m.state = next;
                changed.push((m.node_id.clone(), next));
            }
        }
        for (node_id, state) in &changed {
            warn!(
                node_id,
                state = state.as_str(),
                "cluster member state changed"
            );
        }
        changed
    }

    /// URLs of members currently believed alive — the broadcast target list
    /// for replication, so a dead node stops costing a timeout per message
    /// once it has been noticed.
    pub fn live_urls(&self) -> Vec<String> {
        self.members
            .read()
            .values()
            .filter(|m| m.state == MemberState::Alive)
            .map(|m| m.url.clone())
            .collect()
    }

    /// The full member view, for persistence or the admin API.
    pub fn snapshot(&self) -> Vec<Member> {
        self.members.read().values().cloned().collect()
    }

    pub fn link_snapshot(&self) -> Vec<LinkObservation> {
        let mut links: Vec<_> = self.links.read().values().cloned().collect();
        links.sort_by(|a, b| (&a.source, &a.target).cmp(&(&b.source, &b.target)));
        links
    }

    pub fn link_stale_after_ms(&self) -> i64 {
        self.peer_timeout_ms
    }

    /// POST a JSON body to one peer at `path`, returning whether it accepted.
    /// General on purpose: later replication and config-propagation slices
    /// reuse this rather than each rolling their own client call.
    ///
    /// Runs on `DATA_TIMEOUT`, not the ping budget: every caller here posts a
    /// batch the peer has to commit before it can answer.
    pub async fn post_json<T: Serialize>(&self, peer: &str, path: &str, body: &T) -> bool {
        let url = Self::endpoint(peer, path);
        let mut req = self.client.post(&url).json(body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        match tokio::time::timeout(DATA_TIMEOUT, req.send()).await {
            Ok(Ok(r)) if r.status().is_success() => true,
            Ok(Ok(r)) => {
                warn!(%peer, %path, status = %r.status(), "peer rejected");
                false
            }
            Ok(Err(e)) => {
                warn!(%peer, %path, error = %e, "peer request failed");
                false
            }
            Err(_) => {
                warn!(%peer, %path, "peer request timed out");
                false
            }
        }
    }

    /// Fire-and-forget to every live member. Dead and suspect members are
    /// skipped: a suspect member costs one wasted send if it turns out to
    /// still be there, which is cheaper than every caller re-deriving the
    /// same "who's alive" filter.
    pub async fn broadcast_json<T: Serialize>(&self, path: &str, body: &T) {
        let targets = self.live_urls();
        for peer in targets {
            self.post_json(&peer, path, body).await;
        }
    }
}

/// Ticks `ping_all` then `sweep` on `interval_secs`, forever. Wiring this into
/// startup — deciding when a cluster is configured at all — is the
/// integrator's call, not this module's: it only knows how to run one cycle.
pub fn spawn_heartbeat(cluster: Arc<Cluster>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            tick.tick().await;
            let now = chrono::Utc::now().timestamp_millis();
            cluster.ping_all(now).await;
            cluster.sweep(chrono::Utc::now().timestamp_millis());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, url: &str, generation: i64, last_seen: i64, state: MemberState) -> Member {
        Member {
            node_id: id.into(),
            url: url.into(),
            generation,
            last_seen,
            state,
            // Liveness and merge order are what these tests are about; a build
            // string never participates in either.
            build: None,
            protocol: None,
        }
    }

    fn cluster(id: &str, url: &str, generation: i64) -> Cluster {
        Cluster::new(id, url, generation, vec![], None, 15)
    }

    #[tokio::test]
    async fn partitions_heal_without_restarting_members() {
        use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
        use std::sync::atomic::{AtomicBool, Ordering};

        for count in [2, 4] {
            let split = Arc::new(AtomicBool::new(false));
            let mut listeners = Vec::new();
            for _ in 0..count {
                listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
            }
            let urls: Vec<String> = listeners
                .iter()
                .map(|listener| format!("http://{}", listener.local_addr().unwrap()))
                .collect();
            let mut nodes = Vec::new();
            for (i, listener) in listeners.into_iter().enumerate() {
                let seeds = if i == 0 {
                    vec![]
                } else {
                    vec![urls[i - 1].clone()]
                };
                let node = Arc::new(Cluster::new(i.to_string(), &urls[i], 1, seeds, None, 1));
                let gate = split.clone();
                let app =
                    Router::new()
                        .route(
                            "/internal/cluster/ping",
                            post(
                                move |State(node): State<Arc<Cluster>>,
                                      Json(body): Json<PingBody>| async move {
                                    let from: usize = body.node_id.parse().unwrap();
                                    if gate.load(Ordering::Relaxed)
                                        && (from < count / 2) != (i < count / 2)
                                    {
                                        return Err(StatusCode::SERVICE_UNAVAILABLE);
                                    }
                                    Ok(Json(
                                        node.handle_ping(
                                            &body,
                                            chrono::Utc::now().timestamp_millis(),
                                        ),
                                    ))
                                },
                            ),
                        )
                        .with_state(node.clone());
                tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
                nodes.push(node);
            }
            for _ in 0..count + 2 {
                for node in &nodes {
                    node.retry_discovery(chrono::Utc::now().timestamp_millis())
                        .await;
                    node.ping_all(chrono::Utc::now().timestamp_millis()).await;
                }
            }
            assert!(nodes.iter().all(|node| node.snapshot().len() == count - 1));

            split.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(2100)).await;
            for node in &nodes {
                node.ping_all(chrono::Utc::now().timestamp_millis()).await;
            }
            for node in &nodes {
                node.sweep(chrono::Utc::now().timestamp_millis());
                node.sweep(chrono::Utc::now().timestamp_millis());
                assert_eq!(
                    node.snapshot()
                        .iter()
                        .filter(|m| m.state == MemberState::Dead)
                        .count(),
                    count / 2
                );
            }
            split.store(false, Ordering::Relaxed);
            for node in &nodes {
                node.retry_discovery(chrono::Utc::now().timestamp_millis())
                    .await;
            }
            for node in &nodes {
                node.ping_all(chrono::Utc::now().timestamp_millis()).await;
                assert!(node
                    .snapshot()
                    .iter()
                    .all(|m| m.state == MemberState::Alive));
            }
        }
    }

    // -- transitive discovery --

    #[test]
    fn merge_learns_a_node_only_the_sender_knew_about() {
        let a = cluster("a", "http://a", 1);
        let incoming = PingBody {
            node_id: "b".into(),
            url: "http://b".into(),
            generation: 1,
            members: vec![member("c", "http://c", 1, 100, MemberState::Alive)],
            links: vec![],
        };
        a.merge(&incoming, 200);

        let snap = a.snapshot();
        assert!(
            snap.iter().any(|m| m.node_id == "b"),
            "sender itself learned"
        );
        assert!(
            snap.iter().any(|m| m.node_id == "c"),
            "transitively learned"
        );
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn merge_learns_nothing_from_an_empty_view() {
        let a = cluster("a", "http://a", 1);
        let incoming = PingBody {
            node_id: "b".into(),
            url: "http://b".into(),
            generation: 1,
            members: vec![],
            links: vec![],
        };
        a.merge(&incoming, 200);

        let snap = a.snapshot();
        // Only the sender itself, no phantom third member conjured up.
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].node_id, "b");
    }

    // -- generation precedence --

    #[test]
    fn higher_generation_supersedes_stale_state() {
        let a = cluster("a", "http://a", 1);
        a.restore(vec![member("b", "http://b", 1, 100, MemberState::Dead)]);

        let incoming = PingBody {
            node_id: "x".into(),
            url: "http://x".into(),
            generation: 1,
            members: vec![member("b", "http://b", 2, 50, MemberState::Alive)],
            links: vec![],
        };
        a.merge(&incoming, 200);

        let b = a.snapshot().into_iter().find(|m| m.node_id == "b").unwrap();
        assert_eq!(b.generation, 2);
        assert_eq!(b.state, MemberState::Alive);
        assert_eq!(
            b.last_seen, 50,
            "restart's own last_seen replaces ours wholesale"
        );
    }

    #[test]
    fn lower_generation_is_ignored_entirely() {
        let a = cluster("a", "http://a", 1);
        a.restore(vec![member("b", "http://b", 5, 100, MemberState::Alive)]);

        let incoming = PingBody {
            node_id: "x".into(),
            url: "http://x".into(),
            generation: 1,
            members: vec![member("b", "http://b", 3, 999, MemberState::Alive)],
            links: vec![],
        };
        a.merge(&incoming, 200);

        let b = a.snapshot().into_iter().find(|m| m.node_id == "b").unwrap();
        assert_eq!(b.generation, 5);
        assert_eq!(
            b.last_seen, 100,
            "stale generation must not overwrite a newer view"
        );
    }

    // -- suspect/dead timing --

    #[test]
    fn silence_walks_alive_through_suspect_to_dead_at_the_boundaries() {
        let c = cluster("a", "http://a", 1);
        c.restore(vec![member("b", "http://b", 1, 0, MemberState::Alive)]);

        // Exactly at the timeout: still alive, `>` not `>=`.
        assert!(c.sweep(15_000).is_empty());
        // Just past it: suspect.
        let changed = c.sweep(15_001);
        assert_eq!(changed, vec![("b".into(), MemberState::Suspect)]);

        // Exactly at 2x from the original last_seen: still suspect.
        assert!(c.sweep(30_000).is_empty());
        // Just past it: dead.
        let changed = c.sweep(30_001);
        assert_eq!(changed, vec![("b".into(), MemberState::Dead)]);
    }

    #[test]
    fn a_ping_before_the_timeout_prevents_any_transition() {
        let c = cluster("a", "http://a", 1);
        c.restore(vec![member("b", "http://b", 1, 0, MemberState::Alive)]);

        let incoming = PingBody {
            node_id: "b".into(),
            url: "http://b".into(),
            generation: 1,
            members: vec![],
            links: vec![],
        };
        c.merge(&incoming, 10_000);

        assert!(
            c.sweep(15_001).is_empty(),
            "a heard-from-recently peer must not suspect"
        );
    }

    // -- dead retention and resurrection --

    #[test]
    fn a_dead_member_is_retained_and_returns_to_alive_on_higher_generation() {
        let c = cluster("a", "http://a", 1);
        c.restore(vec![member("b", "http://b", 1, 0, MemberState::Dead)]);
        assert_eq!(c.snapshot().len(), 1);

        let incoming = PingBody {
            node_id: "x".into(),
            url: "http://x".into(),
            generation: 1,
            members: vec![member("b", "http://b", 2, 100, MemberState::Alive)],
            links: vec![],
        };
        c.merge(&incoming, 200);

        let snap = c.snapshot();
        assert_eq!(
            snap.len(),
            2,
            "b and the sender x, not a phantom fourth member"
        );
        let b = snap.into_iter().find(|m| m.node_id == "b").unwrap();
        assert_eq!(b.state, MemberState::Alive);
    }

    #[test]
    fn a_dead_member_is_not_silently_dropped_by_the_sweep() {
        let c = cluster("a", "http://a", 1);
        c.restore(vec![member("b", "http://b", 1, 0, MemberState::Dead)]);
        c.sweep(999_999);
        assert_eq!(c.snapshot().len(), 1, "dead members stay in the view");
    }

    // -- endpoint joining --

    #[test]
    fn a_peers_build_travels_with_its_membership() {
        // A rolling upgrade is only diagnosable if any node can name what the
        // others are running. The sender's build rides its own member entry,
        // so it reaches every node the view reaches.
        let c = cluster("a", "http://a", 1);
        let mut sender = member("b", "http://b", 5, 5, MemberState::Alive);
        sender.build = Some("0.2.0 (deadbeef, built 2026-08-31 00:00:00 UTC)".into());
        sender.protocol = Some(PROTOCOL_VERSION);

        c.merge(
            &PingBody {
                node_id: "b".into(),
                url: "http://b".into(),
                generation: 5,
                members: vec![sender],
                links: vec![],
            },
            10,
        );

        let known = c.snapshot();
        let b = known.iter().find(|m| m.node_id == "b").expect("merged");
        assert_eq!(
            b.build.as_deref(),
            Some("0.2.0 (deadbeef, built 2026-08-31 00:00:00 UTC)")
        );
        assert_eq!(b.protocol, Some(PROTOCOL_VERSION));
    }

    #[test]
    fn a_peer_that_reports_no_build_is_still_a_member() {
        // The other half of the upgrade: a node too old to report one must
        // merge exactly as before rather than being dropped or defaulted into
        // claiming a build it is not running.
        let c = cluster("a", "http://a", 1);
        c.merge(
            &PingBody {
                node_id: "b".into(),
                url: "http://b".into(),
                generation: 5,
                members: vec![member("b", "http://b", 5, 5, MemberState::Alive)],
                links: vec![],
            },
            10,
        );

        let known = c.snapshot();
        let b = known.iter().find(|m| m.node_id == "b").expect("merged");
        assert_eq!(b.build, None);
        assert_eq!(b.protocol, None);
    }

    #[test]
    fn a_member_with_no_build_omits_the_field_entirely() {
        // `skip_serializing_if` keeps the old wire shape byte-identical for a
        // node that has nothing to say, so a peer parsing strictly on some
        // other field is not handed a null it never saw before.
        let json = serde_json::to_string(&member("b", "http://b", 1, 2, MemberState::Alive))
            .expect("serialises");
        assert!(!json.contains("build"), "{json}");
        assert!(!json.contains("protocol"), "{json}");
    }

    #[test]
    fn endpoint_joins_without_a_double_slash() {
        let want = "http://c2:8080/internal/cluster/ping";
        assert_eq!(
            Cluster::endpoint("http://c2:8080", "internal/cluster/ping"),
            want
        );
        assert_eq!(
            Cluster::endpoint("http://c2:8080/", "internal/cluster/ping"),
            want
        );
        assert_eq!(
            Cluster::endpoint("http://c2:8080/", "/internal/cluster/ping"),
            want
        );
    }

    #[test]
    fn endpoint_does_not_swallow_a_meaningful_path_segment() {
        // A trailing slash strip must not eat the leading slash of the path
        // itself, or "cluster/ping" and "/cluster/ping" would collide wrongly
        // with something that legitimately starts the same way.
        assert_ne!(
            Cluster::endpoint("http://c2:8080", "internal/cluster/ping"),
            Cluster::endpoint("http://c2:8080", "internal/other/ping")
        );
    }

    // -- live_urls excludes dead members --

    #[test]
    fn live_urls_excludes_dead_and_suspect_members() {
        let c = cluster("a", "http://a", 1);
        c.restore(vec![
            member("b", "http://b", 1, 0, MemberState::Alive),
            member("d", "http://d", 1, 0, MemberState::Dead),
            member("s", "http://s", 1, 0, MemberState::Suspect),
        ]);
        assert_eq!(c.live_urls(), vec!["http://b".to_string()]);
    }

    #[test]
    fn live_urls_is_empty_with_no_alive_members() {
        let c = cluster("a", "http://a", 1);
        c.restore(vec![member("d", "http://d", 1, 0, MemberState::Dead)]);
        assert!(c.live_urls().is_empty());
    }

    // -- wire form contract --

    #[test]
    fn ping_body_survives_the_wire_unchanged() {
        let body = PingBody {
            node_id: "a".into(),
            url: "http://a:8080".into(),
            generation: 1_700_000_000_000,
            members: vec![member(
                "b",
                "http://b:8080",
                1_700_000_000_001,
                1_700_000_000_002,
                MemberState::Suspect,
            )],
            links: vec![],
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains(r#""node_id":"a""#), "{json}");
        assert!(json.contains(r#""state":"suspect""#), "{json}");

        let back: PingBody = serde_json::from_str(&json).unwrap();
        assert_eq!(back, body);
    }

    #[test]
    fn an_old_ping_body_defaults_links_to_empty() {
        let body: PingBody =
            serde_json::from_str(r#"{"node_id":"a","url":"http://a","generation":1,"members":[]}"#)
                .unwrap();
        assert!(body.links.is_empty());
    }

    #[test]
    fn link_observations_merge_by_source_generation_then_time() {
        let c = cluster("a", "http://a", 1);
        let observation = |source_generation, observed_at, outcome| LinkObservation {
            source: "b".into(),
            target: "c".into(),
            source_generation,
            observed_at,
            outcome,
        };
        for link in [
            observation(2, 100, LinkOutcome::Connected),
            observation(1, 999, LinkOutcome::Failed),
            observation(2, 90, LinkOutcome::Failed),
            observation(2, 110, LinkOutcome::Failed),
        ] {
            c.merge_links(&[link]);
        }
        assert_eq!(
            c.link_snapshot(),
            vec![observation(2, 110, LinkOutcome::Failed)]
        );
    }

    #[test]
    fn opposite_link_directions_are_independent() {
        let c = cluster("a", "http://a", 1);
        c.merge_links(&[
            LinkObservation {
                source: "b".into(),
                target: "c".into(),
                source_generation: 1,
                observed_at: 100,
                outcome: LinkOutcome::Connected,
            },
            LinkObservation {
                source: "c".into(),
                target: "b".into(),
                source_generation: 1,
                observed_at: 100,
                outcome: LinkOutcome::Failed,
            },
        ]);
        assert_eq!(c.link_snapshot().len(), 2);
    }

    #[test]
    fn an_unknown_state_string_does_not_round_trip_as_a_third_kind() {
        // The wire form uses the same lowercase spelling as the merge rule, so
        // a receiver on a mismatched build fails loudly on an unknown variant
        // rather than silently reading it as something else.
        let err = serde_json::from_str::<MemberState>(r#""zombie""#);
        assert!(err.is_err());
    }
}
