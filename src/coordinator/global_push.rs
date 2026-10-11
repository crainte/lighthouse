//! Region-to-global state publication: one deterministic publisher per region
//! cluster sends incremental check-summary deltas against the last push the
//! global node acknowledged, with a periodic full snapshot as the repair path
//! (and the refresh for per-probe fields deltas deliberately skip).

use super::storage::{CheckStatus, StatusScope};
use super::Coordinator;
use crate::global::wire::{CheckSummary, PushAck, RegionStatePush};
use crate::http::ZstdJson;
use crate::models::{CheckId, NodeState};
use std::collections::HashMap;
use tracing::warn;

/// Every Nth push is a full snapshot even when deltas are flowing, bounding
/// how stale `latency_ms`/`last_check` can be globally and how long any
/// divergence the chain cannot see can survive. 12 × the default 5s push
/// interval is one minute.
const SNAPSHOT_EVERY: u32 = 12;

/// The sender's view of its push chain against one global endpoint.
pub(super) struct PushChain {
    epoch: i64,
    seq: u64,
    /// The last acknowledged push: its sequence number and the summaries the
    /// receiver now holds. `None` until a snapshot is acknowledged, and again
    /// after any failure or resync, so the next push is a snapshot.
    acked: Option<(u64, HashMap<CheckId, CheckSummary>)>,
    since_snapshot: u32,
}

impl PushChain {
    pub(super) fn new(epoch: i64) -> Self {
        Self {
            epoch,
            seq: 0,
            acked: None,
            since_snapshot: 0,
        }
    }

    /// Turn a complete snapshot into what to send: itself, or a delta
    /// against the acknowledged state. Returns the push and the full state it
    /// represents, to remember once acknowledged.
    pub(super) fn next(
        &mut self,
        mut push: RegionStatePush,
    ) -> (RegionStatePush, HashMap<CheckId, CheckSummary>) {
        self.seq += 1;
        push.epoch = self.epoch;
        push.seq = self.seq;
        let full: HashMap<CheckId, CheckSummary> = push
            .checks
            .iter()
            .map(|c| (c.check_id, c.clone()))
            .collect();
        match &self.acked {
            Some((base, acked)) if self.since_snapshot + 1 < SNAPSHOT_EVERY => {
                self.since_snapshot += 1;
                push.delta_base = Some(*base);
                push.checks.retain(|c| {
                    acked
                        .get(&c.check_id)
                        .is_none_or(|previous| c.materially_differs(previous))
                });
                push.removed = acked
                    .keys()
                    .filter(|id| !full.contains_key(id))
                    .copied()
                    .collect();
            }
            _ => self.since_snapshot = 0,
        }
        (push, full)
    }

    /// Record the receiver's answer to push `seq`. Anything but an applied
    /// push restarts the chain from a snapshot.
    pub(super) fn settle(
        &mut self,
        seq: u64,
        full: HashMap<CheckId, CheckSummary>,
        ack: Option<PushAck>,
    ) {
        self.acked = match ack {
            Some(PushAck { accepted: true, .. }) => Some((seq, full)),
            _ => None,
        };
    }
}

impl Coordinator {
    /// Push this region's state summary to the global coordinator on a timer.
    /// A no-op when no `[coordinator.global]` block is configured.
    ///
    /// Only the lowest-id live member of a region cluster publishes, so three
    /// coordinators do not send three equivalent streams; when it dies,
    /// membership promotes the next one, whose first push is a snapshot.
    /// Best-effort like gossip: a failed push restarts the chain with a
    /// snapshot on the next tick.
    pub fn spawn_global_push(&self) {
        let Some((url, token, push_secs)) = self.inner.global_push.clone() else {
            return;
        };
        let coord = self.clone();
        // A global node that accepts the connection and never answers would
        // otherwise hold this loop forever and the region would silently go
        // dark. Bound the attempt to under the push interval.
        let attempt_timeout = std::time::Duration::from_secs(push_secs).mul_f32(0.8);
        let client = match reqwest::Client::builder().timeout(attempt_timeout).build() {
            Ok(client) => client,
            Err(error) => {
                warn!(%error, "global push: failed to build HTTP client");
                return;
            }
        };
        let endpoint = format!("{}/internal/global/state", url.trim_end_matches('/'));
        tokio::spawn(async move {
            let mut chain = PushChain::new(chrono::Utc::now().timestamp_millis());
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(push_secs));
            // A slow push must delay the next tick, not queue a burst.
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if !coord.is_publisher() {
                    // A standby restarts from a snapshot if it is promoted.
                    chain = PushChain::new(chrono::Utc::now().timestamp_millis());
                    continue;
                }
                let (push, full) = chain.next(coord.build_state_push().await);
                let seq = push.seq;
                let ack = match client
                    .post(&endpoint)
                    .bearer_auth(&token)
                    .zstd_json(&push)
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => r.json::<PushAck>().await.ok(),
                    Ok(r) => {
                        warn!(status = %r.status(), "global push rejected");
                        None
                    }
                    Err(e) => {
                        warn!(error = %e, "global push failed");
                        None
                    }
                };
                chain.settle(seq, full, ack);
            }
        });
    }

    fn is_publisher(&self) -> bool {
        self.inner
            .cluster
            .as_ref()
            .is_none_or(|cluster| cluster.is_lowest_live())
    }

    /// A complete summary of every configured, visible check. Reads the same
    /// status source as the public `/api/v1/status`, so the global dashboard
    /// shows exactly what a regional dashboard would.
    pub(in crate::coordinator) async fn build_state_push(&self) -> RegionStatePush {
        use crate::global::cache::worst;

        let now = chrono::Utc::now().timestamp_millis();
        let reader = self.clone();
        let (statuses, polls) = self
            .db(move |storage| {
                let (checks, _) = super::routes::public::check_page(
                    &reader,
                    storage,
                    &StatusScope::All,
                    false,
                    0,
                    usize::MAX,
                )?;
                Ok((
                    storage.status_for(&checks, now)?,
                    storage.list_agent_polls().unwrap_or_default(),
                ))
            })
            .await
            .unwrap_or_else(|error| {
                warn!(%error, "global push database read failed");
                (Vec::new(), Vec::new())
            });
        let summaries: Vec<CheckSummary> = {
            let by_id = self.inner.checks.read();
            statuses
                .into_iter()
                .filter_map(|status: CheckStatus| {
                    let check_id = status.check_id();
                    let meta = by_id.get(&check_id)?;
                    let r = status.resolve(meta.default_status.into());
                    Some(CheckSummary {
                        check_id,
                        name: meta.name.clone(),
                        state: r.state,
                        latency_ms: r.latency_ms,
                        last_check: r.last_check,
                        uptime_24h: r.uptime_24h,
                        status_code: r.status_code,
                        // The raw error is redacted from the public page; the
                        // global push carries only the safe category.
                        error: r.failure.map(|f| f.summary()),
                        kind: Some(meta.kind),
                        group: meta.group.clone(),
                        latency_graph: meta.latency_graph,
                        position: meta.position,
                        depends_on: meta.depends_on.clone(),
                    })
                })
                .collect()
        };
        let region_health = summaries
            .iter()
            .fold(NodeState::Unknown, |acc, c| worst(acc, c.state));
        // Truthful agent counts for this region: an agent is healthy when its
        // last contact is inside its own silence window. Local `seen` alone
        // undercounts in a cluster, so union it with the persisted
        // `agent_polls` twin, matching how `fleet_health` builds the view.
        let (agents_healthy, agents_total) = {
            let region = self.inner.region.as_str();
            let mut merged: HashMap<String, bool> = HashMap::new();
            let mut note = |id: &str, healthy: bool| {
                merged
                    .entry(id.to_string())
                    .and_modify(|h| *h = *h || healthy)
                    .or_insert(healthy);
            };
            for (id, a) in self.inner.seen.read().iter() {
                if a.region == region {
                    note(id, now.saturating_sub(a.at) <= a.window_ms);
                }
            }
            for p in polls {
                if p.region == region {
                    note(&p.agent_id, now.saturating_sub(p.at) <= p.window_ms);
                }
            }
            let total = merged.len() as u32;
            let healthy = merged.values().filter(|h| **h).count() as u32;
            (healthy, total)
        };
        RegionStatePush {
            region: self.inner.region.clone(),
            origin: self.inner.node_id.clone(),
            advertise_url: self.inner.advertise_url.clone(),
            pushed_at: now,
            region_health,
            agents_healthy,
            agents_total,
            checks: summaries,
            epoch: 0,
            seq: 0,
            delta_base: None,
            removed: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(name: &str, state: NodeState, latency_ms: u64) -> CheckSummary {
        CheckSummary {
            check_id: CheckId::new_v5(&CheckId::NAMESPACE_OID, name.as_bytes()),
            name: name.into(),
            state,
            latency_ms,
            last_check: latency_ms as i64,
            uptime_24h: Some(1.0),
            status_code: None,
            error: None,
            kind: None,
            group: None,
            latency_graph: false,
            position: 0,
            depends_on: Vec::new(),
        }
    }

    fn snapshot(checks: Vec<CheckSummary>) -> RegionStatePush {
        RegionStatePush {
            region: "us".into(),
            origin: "a".into(),
            advertise_url: None,
            pushed_at: 1,
            region_health: NodeState::Up,
            agents_healthy: 1,
            agents_total: 1,
            checks,
            epoch: 0,
            seq: 0,
            delta_base: None,
            removed: Vec::new(),
        }
    }

    fn accepted() -> Option<PushAck> {
        Some(PushAck {
            accepted: true,
            resync: false,
        })
    }

    #[test]
    fn deltas_carry_only_material_changes_and_removals_until_the_next_snapshot() {
        let mut chain = PushChain::new(7);
        let (first, full) = chain.next(snapshot(vec![
            summary("api", NodeState::Up, 10),
            summary("db", NodeState::Up, 10),
        ]));
        assert_eq!((first.epoch, first.seq, first.delta_base), (7, 1, None));
        assert_eq!(first.checks.len(), 2);
        chain.settle(first.seq, full, accepted());

        // Latency moved on both, api went down, db disappeared, web is new.
        let (delta, full) = chain.next(snapshot(vec![
            summary("api", NodeState::Down, 99),
            summary("web", NodeState::Up, 99),
        ]));
        assert_eq!(delta.delta_base, Some(1));
        let names: Vec<&str> = delta.checks.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["api", "web"]);
        assert_eq!(
            delta.removed,
            vec![summary("db", NodeState::Up, 0).check_id]
        );
        chain.settle(delta.seq, full, accepted());

        // Only per-probe fields changed: an empty delta.
        let (quiet, full) = chain.next(snapshot(vec![
            summary("api", NodeState::Down, 5),
            summary("web", NodeState::Up, 5),
        ]));
        assert_eq!(quiet.delta_base, Some(2));
        assert!(quiet.checks.is_empty() && quiet.removed.is_empty());
        chain.settle(quiet.seq, full, accepted());

        for _ in 3..SNAPSHOT_EVERY {
            let (push, full) = chain.next(snapshot(vec![summary("api", NodeState::Down, 1)]));
            assert!(push.delta_base.is_some());
            chain.settle(push.seq, full, accepted());
        }
        let (periodic, _) = chain.next(snapshot(vec![summary("api", NodeState::Down, 1)]));
        assert_eq!(periodic.delta_base, None, "every Nth push is a snapshot");
    }

    #[test]
    fn a_resync_or_failed_push_restarts_from_a_snapshot() {
        let mut chain = PushChain::new(1);
        let (push, full) = chain.next(snapshot(vec![summary("api", NodeState::Up, 1)]));
        chain.settle(push.seq, full, accepted());
        let (push, full) = chain.next(snapshot(vec![summary("api", NodeState::Down, 1)]));
        assert!(push.delta_base.is_some());
        chain.settle(
            push.seq,
            full,
            Some(PushAck {
                accepted: false,
                resync: true,
            }),
        );
        let (push, full) = chain.next(snapshot(vec![summary("api", NodeState::Down, 1)]));
        assert_eq!(push.delta_base, None);
        assert_eq!(push.checks.len(), 1);
        chain.settle(push.seq, full, None);
        let (push, _) = chain.next(snapshot(vec![summary("api", NodeState::Down, 1)]));
        assert_eq!(push.delta_base, None, "a transport failure also resyncs");
    }
}
