//! Outbound data-plane replication, one bounded queue per peer.
//!
//! Ingest used to spawn a task per batch that posted to every live peer, so a
//! busy node aimed an unbounded number of concurrent `/internal/replicate`
//! requests at the same receiver. Every one of those requests contends for the
//! receiver's single `state_write` and its single database worker, so past a
//! certain rate they all queue behind each other, time out together, and hand
//! the resulting divergence to anti-entropy — which then competes with the live
//! traffic that caused it.
//!
//! This module replaces that fan-out with one in-flight request per peer and a
//! single merged batch waiting behind it. Backpressure becomes coalescing
//! rather than queueing: while a post is in flight, everything produced for
//! that peer folds into one batch, so the receiver sees fewer, larger
//! transactions instead of many small contending ones. Ordering per peer is
//! preserved (a single worker posts sequentially), and application is
//! idempotent, so a dropped or failed batch is still repaired by anti-entropy.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::cluster::Cluster;
use super::routes::peer::ReplicationBatch;
use super::Timings;

/// Ceiling on results held for one unreachable peer. Past this, the oldest
/// results are dropped rather than growing memory without bound: anti-entropy
/// backfills exactly this case, and dropping the oldest keeps the freshest
/// state moving.
const MAX_PENDING_RESULTS: usize = 100_000;

/// Same ceiling, applied to the lifecycle streams that ride along.
const MAX_PENDING_LIFECYCLE: usize = 20_000;

#[derive(Default)]
struct Pending {
    batch: Option<ReplicationBatch>,
    /// When the oldest un-posted contribution was enqueued, for queue-wait
    /// timing.
    since: Option<Instant>,
}

struct PeerQueue {
    pending: Mutex<Pending>,
    wake: Notify,
}

/// Per-peer outbound queues, created on first use and reused for the life of
/// the process. Keyed by the peer's base URL, which is what `Cluster` hands out
/// and what `post_json` needs.
#[derive(Default)]
pub(super) struct Outbox {
    peers: Mutex<HashMap<String, Arc<PeerQueue>>>,
}

impl Outbox {
    /// Hand `batch` to every live peer's queue. Returns immediately: the
    /// per-peer worker owns the network call, so ingest never waits on a peer.
    pub(super) fn broadcast(
        &self,
        cluster: &Arc<Cluster>,
        timings: &Arc<Timings>,
        batch: &ReplicationBatch,
    ) {
        for peer in cluster.live_urls() {
            self.enqueue(cluster, timings, &peer, batch.clone());
        }
    }

    fn enqueue(
        &self,
        cluster: &Arc<Cluster>,
        timings: &Arc<Timings>,
        peer: &str,
        batch: ReplicationBatch,
    ) {
        let queue = {
            let mut peers = self.peers.lock();
            match peers.get(peer) {
                Some(queue) => Arc::clone(queue),
                None => {
                    let queue = Arc::new(PeerQueue {
                        pending: Mutex::new(Pending::default()),
                        wake: Notify::new(),
                    });
                    peers.insert(peer.to_string(), Arc::clone(&queue));
                    spawn_worker(
                        Arc::clone(cluster),
                        Arc::clone(timings),
                        peer.to_string(),
                        Arc::clone(&queue),
                    );
                    queue
                }
            }
        };
        {
            let mut pending = queue.pending.lock();
            pending.since.get_or_insert_with(Instant::now);
            match &mut pending.batch {
                Some(existing) => merge(existing, batch),
                None => pending.batch = Some(batch),
            }
        }
        queue.wake.notify_one();
    }
}

/// One worker per peer: take everything pending, post it, repeat. Nothing else
/// posts to that peer, so exactly one request is ever in flight to it and the
/// receiver's write path is never asked to serve this node concurrently.
fn spawn_worker(cluster: Arc<Cluster>, timings: Arc<Timings>, peer: String, queue: Arc<PeerQueue>) {
    tokio::spawn(async move {
        loop {
            let taken = {
                let mut pending = queue.pending.lock();
                pending.batch.take().map(|batch| {
                    let since = pending.since.take();
                    (batch, since)
                })
            };
            let Some((batch, since)) = taken else {
                queue.wake.notified().await;
                continue;
            };
            if timings.enabled {
                if let Some(since) = since {
                    timings.broadcast_queue_wait.observe(
                        "replication_broadcast",
                        "queue_wait",
                        since.elapsed(),
                    );
                }
            }
            let started = Instant::now();
            let accepted = cluster
                .post_json(&peer, "/internal/replicate", &batch)
                .await;
            if timings.enabled {
                timings.broadcast_network.observe(
                    "replication_broadcast",
                    "network",
                    started.elapsed(),
                );
                if !accepted {
                    timings.broadcast_failures.observe(
                        "replication_broadcast",
                        "failure",
                        Duration::ZERO,
                    );
                }
            }
        }
    });
}

/// Fold `next` into `existing`, oldest-first, then trim to the pending
/// ceilings. Beliefs collapse per `(check_id, agent_id)` because only the
/// latest one carries meaning — the receiver overwrites rather than appends —
/// which is what keeps a coalesced batch from growing with time instead of with
/// the number of distinct checks.
fn merge(existing: &mut ReplicationBatch, next: ReplicationBatch) {
    existing.results.extend(next.results);
    existing.alerts.extend(next.alerts);
    existing.incidents.extend(next.incidents);
    existing.polls.extend(next.polls);
    existing.beliefs.extend(next.beliefs);
    existing.deleted_incidents.extend(next.deleted_incidents);

    let mut seen = std::collections::HashSet::new();
    let mut collapsed = Vec::with_capacity(existing.beliefs.len());
    for belief in existing.beliefs.drain(..).rev() {
        if seen.insert((belief.check_id, belief.agent_id.clone())) {
            collapsed.push(belief);
        }
    }
    collapsed.reverse();
    existing.beliefs = collapsed;

    trim_front(&mut existing.results, MAX_PENDING_RESULTS);
    trim_front(&mut existing.alerts, MAX_PENDING_LIFECYCLE);
    trim_front(&mut existing.incidents, MAX_PENDING_LIFECYCLE);
    trim_front(&mut existing.polls, MAX_PENDING_LIFECYCLE);
    trim_front(&mut existing.deleted_incidents, MAX_PENDING_LIFECYCLE);
}

/// Drop from the front, keeping the newest `max`. Anti-entropy replays what is
/// dropped; falling behind on the newest state would not be repaired as
/// promptly.
fn trim_front<T>(rows: &mut Vec<T>, max: usize) {
    if rows.len() > max {
        rows.drain(..rows.len() - max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::routes::peer::{ReplicatedBelief, ReplicatedResult};
    use crate::models::{new_check_id, CheckId, CheckKind, CheckResult, Status};

    fn result(check: CheckId, ts: i64) -> ReplicatedResult {
        ReplicatedResult {
            result: CheckResult {
                check_id: check,
                agent_id: "a".into(),
                zone: "z".into(),
                region: "r".into(),
                kind: CheckKind::Http,
                timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
                latency_ms: 10,
                status_code: Some(200),
                body_matched: None,
                error: None,
                failure: None,
                body: None,
                headers: None,
                metric: None,
                metrics: None,
            },
            status: Status::Up,
        }
    }

    fn belief(check: CheckId, agent: &str, failures: u32) -> ReplicatedBelief {
        ReplicatedBelief {
            check_id: check,
            agent_id: agent.into(),
            zone: "z".into(),
            region: "r".into(),
            status: Status::Up,
            consecutive_failures: failures,
            last_check: 0,
        }
    }

    fn millis(r: &ReplicatedResult) -> i64 {
        r.result.timestamp.timestamp_millis()
    }

    #[test]
    fn merge_keeps_every_result_in_order() {
        let id = new_check_id();
        let mut first = ReplicationBatch {
            results: vec![result(id, 1)],
            ..Default::default()
        };
        merge(
            &mut first,
            ReplicationBatch {
                results: vec![result(id, 2)],
                ..Default::default()
            },
        );
        let stamps: Vec<i64> = first.results.iter().map(millis).collect();
        assert_eq!(stamps, vec![1, 2]);
    }

    #[test]
    fn merge_collapses_beliefs_to_the_latest_per_agent() {
        let id = new_check_id();
        let mut first = ReplicationBatch {
            beliefs: vec![belief(id, "a", 1), belief(id, "b", 1)],
            ..Default::default()
        };
        merge(
            &mut first,
            ReplicationBatch {
                beliefs: vec![belief(id, "a", 2)],
                ..Default::default()
            },
        );
        assert_eq!(first.beliefs.len(), 2);
        let a = first
            .beliefs
            .iter()
            .find(|b| b.agent_id == "a")
            .expect("agent a belief");
        assert_eq!(a.consecutive_failures, 2);
    }

    #[test]
    fn merge_trims_the_oldest_results_past_the_ceiling() {
        let id = new_check_id();
        let mut first = ReplicationBatch {
            results: (0..MAX_PENDING_RESULTS as i64)
                .map(|ts| result(id, ts))
                .collect(),
            ..Default::default()
        };
        merge(
            &mut first,
            ReplicationBatch {
                results: vec![result(id, MAX_PENDING_RESULTS as i64)],
                ..Default::default()
            },
        );
        assert_eq!(first.results.len(), MAX_PENDING_RESULTS);
        assert_eq!(millis(&first.results[0]), 1);
        assert_eq!(
            millis(&first.results[MAX_PENDING_RESULTS - 1]),
            MAX_PENDING_RESULTS as i64
        );
    }
}
