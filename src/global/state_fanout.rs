//! Global-to-global replication of received region state.
//!
//! A region publishes to exactly one global node (whichever one its
//! connection lands on), and that node's in-memory `GlobalCache` was the only
//! place the region's state lived. Behind a load balancer every other global
//! node then served an empty dashboard. Here the receiving node forwards each
//! push it applied to every peer global node, which applies it through the
//! same `GlobalCache::apply_push` the region's own push went through.
//!
//! Forwarding is ordered per peer (one worker task and queue each), so a
//! peer sees a region's deltas in the order they were applied here. A peer
//! that cannot apply a delta (it restarted, missed one, or never had the
//! region) answers `resync`, and the worker sends a full snapshot of this
//! node's cached state for that region instead — `GlobalCache::export_region`
//! keeps the origin's chain position, so the origin's next delta applies on
//! the peer too. A failed or dropped forward marks the region dirty for that
//! peer, so the next forward for it is a snapshot. A peer receiving a
//! forward never forwards it again: there are no loops.
//!
//! Targets are `Cluster::forward_targets`, not `live_urls`: a node that just
//! started and has not finished discovering its peers must still pass on what
//! it receives, and an unready peer must still be fed so it can catch up.

use super::wire::{PushAck, RegionStatePush};
use super::Inner;
use crate::http::ZstdJson;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

/// Per-peer queue depth. A peer that falls this far behind gets a snapshot
/// once it drains instead of every intermediate delta.
const QUEUE: usize = 16;

/// Bound on one forward, under the default 5s push interval so a hung peer
/// cannot hold its queue across many pushes.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(4);

pub(super) const PEER_STATE_PATH: &str = "/internal/global/peer-state";

struct Worker {
    tx: mpsc::Sender<Arc<RegionStatePush>>,
    dirty: Arc<Mutex<HashSet<String>>>,
}

#[derive(Default)]
pub(super) struct PeerStats {
    pub failures: AtomicU64,
    pub resyncs: AtomicU64,
}

pub(super) struct StateFanout {
    client: reqwest::Client,
    workers: Mutex<HashMap<String, Worker>>,
    stats: Mutex<HashMap<String, Arc<PeerStats>>>,
}

impl Default for StateFanout {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(FORWARD_TIMEOUT)
                .build()
                .unwrap_or_default(),
            workers: Mutex::new(HashMap::new()),
            stats: Mutex::new(HashMap::new()),
        }
    }
}

impl StateFanout {
    /// Queue `push` (already applied locally) for every target peer. Never
    /// blocks: a full queue marks the region dirty for that peer instead.
    pub(super) fn forward(&self, inner: &Arc<Inner>, targets: Vec<String>, push: RegionStatePush) {
        if targets.is_empty() {
            return;
        }
        let push = Arc::new(push);
        let mut workers = self.workers.lock();
        workers.retain(|peer, _| targets.contains(peer));
        for peer in targets {
            let worker = workers
                .entry(peer.clone())
                .or_insert_with(|| self.spawn_worker(Arc::downgrade(inner), peer.clone()));
            if worker.tx.try_send(push.clone()).is_err() {
                worker.dirty.lock().insert(push.region.clone());
            }
        }
    }

    fn peer_stats(&self, peer: &str) -> Arc<PeerStats> {
        self.stats
            .lock()
            .entry(peer.to_string())
            .or_default()
            .clone()
    }

    /// `(peer, failures, resyncs)` for every peer ever forwarded to.
    pub(super) fn stats(&self) -> Vec<(String, u64, u64)> {
        let mut rows: Vec<_> = self
            .stats
            .lock()
            .iter()
            .map(|(peer, s)| {
                (
                    peer.clone(),
                    s.failures.load(Ordering::Relaxed),
                    s.resyncs.load(Ordering::Relaxed),
                )
            })
            .collect();
        rows.sort();
        rows
    }

    fn spawn_worker(&self, inner: Weak<Inner>, peer: String) -> Worker {
        let (tx, mut rx) = mpsc::channel::<Arc<RegionStatePush>>(QUEUE);
        let dirty: Arc<Mutex<HashSet<String>>> = Arc::default();
        let worker_dirty = dirty.clone();
        let client = self.client.clone();
        let stats = self.peer_stats(&peer);
        tokio::spawn(async move {
            let url = crate::coordinator::cluster::Cluster::endpoint(&peer, PEER_STATE_PATH);
            while let Some(push) = rx.recv().await {
                // The node is gone (tests drop nodes; a real process exits):
                // nothing left to forward from.
                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let region = push.region.clone();
                let token = inner.peer_token.clone().unwrap_or_default();
                let send = |body: &RegionStatePush| {
                    client.post(&url).bearer_auth(&token).zstd_json(body).send()
                };
                let snapshot_first = worker_dirty.lock().remove(&region);
                let outcome = if snapshot_first {
                    match inner.cache.export_region(&region) {
                        Some(snapshot) => ack_of(send(&snapshot).await).await,
                        None => Err("region no longer cached".to_string()),
                    }
                } else {
                    ack_of(send(&push).await).await
                };
                let outcome = match outcome {
                    Ok(ack) if ack.resync => {
                        stats.resyncs.fetch_add(1, Ordering::Relaxed);
                        match inner.cache.export_region(&region) {
                            Some(snapshot) => ack_of(send(&snapshot).await).await,
                            None => Err("region no longer cached".to_string()),
                        }
                    }
                    other => other,
                };
                match outcome {
                    Ok(ack) if ack.accepted => {}
                    // Superseded: the peer already holds something at least as
                    // new. Nothing to repair.
                    Ok(ack) if !ack.resync => {}
                    Ok(_) => {
                        worker_dirty.lock().insert(region);
                    }
                    Err(error) => {
                        stats.failures.fetch_add(1, Ordering::Relaxed);
                        warn!(%peer, region = %region, %error, "global: region state forward failed");
                        worker_dirty.lock().insert(region);
                    }
                }
            }
        });
        Worker { tx, dirty }
    }
}

async fn ack_of(response: reqwest::Result<reqwest::Response>) -> Result<PushAck, String> {
    match response {
        Ok(r) if r.status().is_success() => r.json::<PushAck>().await.map_err(|e| e.to_string()),
        Ok(r) => Err(format!("status {}", r.status())),
        Err(e) => Err(e.to_string()),
    }
}
