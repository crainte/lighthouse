//! Whether this global node is in sync with its peers — its readiness.
//!
//! A global node that has just started holds an empty region cache, may not
//! know its peers yet, and may be missing catalogue entries (including the
//! durable alert firing set) its peers have. Serving the dashboard from it, or
//! letting it own alerting, shows or pages on a view that is simply wrong. So
//! one notion — "in sync" — gates all three:
//!
//! 1. **membership**: one full discovery round (seeds and dead members
//!    pinged) has completed, and a peer timeout has elapsed since startup, so
//!    every peer restored from disk is either confirmed alive or swept;
//! 2. **config**: a catalogue reconcile against every live peer has
//!    completed since startup (waived only when membership has settled and no
//!    peer is alive — otherwise losing both peers would silence alerting for
//!    good);
//! 3. **state**: every registered region has delivered state since startup,
//!    directly or forwarded by a peer, or the staleness window has passed
//!    (that region really is dark).
//!
//! The result is `/readyz` (Kubernetes routes the dashboard only to synced
//! pods), the `ready` flag this node advertises in membership pings (peers do
//! not hand it alerting until it is ready), and the alerting gate itself.

use super::GlobalNode;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SyncStatus {
    pub membership: bool,
    pub config: bool,
    pub state: bool,
}

impl SyncStatus {
    pub fn ready(&self) -> bool {
        self.membership && self.config && self.state
    }

    fn reasons(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if !self.membership {
            out.push("membership");
        }
        if !self.config {
            out.push("config");
        }
        if !self.state {
            out.push("state");
        }
        out
    }
}

pub(super) struct SyncState {
    started_ms: i64,
    discovery_done: AtomicBool,
    /// When a catalogue reconcile against every live peer last completed.
    config_synced_ms: AtomicI64,
    /// When this node last became the lowest-id candidate for alerting. A
    /// fresh reconcile must complete after this before it pages.
    promoted_ms: AtomicI64,
    was_candidate: AtomicBool,
    /// When this node last became ready; 0 while unready.
    ready_since_ms: AtomicI64,
}

impl SyncState {
    pub(super) fn new(started_ms: i64) -> Self {
        Self {
            started_ms,
            discovery_done: AtomicBool::new(false),
            config_synced_ms: AtomicI64::new(0),
            promoted_ms: AtomicI64::new(0),
            was_candidate: AtomicBool::new(false),
            ready_since_ms: AtomicI64::new(0),
        }
    }
}

impl GlobalNode {
    pub(super) fn mark_discovery_round(&self) {
        self.inner
            .sync
            .discovery_done
            .store(true, Ordering::Relaxed);
    }

    pub fn sync_status(&self, now: i64) -> SyncStatus {
        let sync = &self.inner.sync;
        let elapsed = now.saturating_sub(sync.started_ms);
        let (membership, config) = match &self.inner.cluster {
            None => (true, true),
            Some(cluster) => (
                sync.discovery_done.load(Ordering::Relaxed)
                    && elapsed >= cluster.link_stale_after_ms(),
                sync.config_synced_ms.load(Ordering::Relaxed) > 0,
            ),
        };
        let (_, never_received, _) = self.inner.cache.staleness_snapshot(now);
        let state = never_received == 0 || elapsed >= self.inner.cache.staleness_ms();
        SyncStatus {
            membership,
            config,
            state,
        }
    }

    /// Whether this node may evaluate and send global alerts right now:
    /// in sync, the lowest-id *ready* member, re-reconciled since it became
    /// that, and ready long enough that the previous owner has heard so and
    /// stood down (two heartbeats). Conditions that change while this is
    /// false are not lost: the next owner diffs against the durable set.
    pub(super) fn owns_alerting(&self, now: i64) -> bool {
        if !self.sync_status(now).ready() {
            return false;
        }
        let Some(cluster) = &self.inner.cluster else {
            return true;
        };
        let sync = &self.inner.sync;
        if !cluster.is_lowest_ready() {
            return false;
        }
        if sync.config_synced_ms.load(Ordering::Relaxed) < sync.promoted_ms.load(Ordering::Relaxed)
        {
            return false;
        }
        let handoff_ms = (self.inner.gossip_interval_secs.max(1) as i64) * 2_000;
        let ready_since = sync.ready_since_ms.load(Ordering::Relaxed);
        !cluster.has_live_peers() || (ready_since > 0 && now - ready_since >= handoff_ms)
    }

    /// Evaluate global alerts if this node owns them, and replicate the
    /// firing-set changes to peers.
    pub(super) async fn evaluate_alerts(&self, now: i64) {
        if !self.inner.alerter.active() || !self.owns_alerting(now) {
            return;
        }
        let entries = self
            .inner
            .alerter
            .evaluate(&self.inner.cache, &self.inner.store, now)
            .await;
        self.broadcast_to_peers(entries);
    }

    /// Reconcile the catalogue against every live peer; on success, stamp
    /// config as in sync. With no live peer, only a settled membership view
    /// can conclude there is nobody to sync with.
    pub(super) async fn config_sync_round(&self, now: i64) -> bool {
        let Some(cluster) = &self.inner.cluster else {
            return true;
        };
        let peers = cluster.live_urls();
        let ok = if peers.is_empty() {
            self.sync_status(now).membership
        } else {
            futures_util::future::join_all(
                peers
                    .iter()
                    .map(|peer| super::cluster::reconcile_with_peer(self, peer)),
            )
            .await
            .into_iter()
            .all(|ok| ok)
        };
        if ok {
            self.inner
                .sync
                .config_synced_ms
                .store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
        }
        ok
    }

    /// One readiness tick: notice promotion, catch the catalogue up when
    /// needed, publish readiness, and evaluate alerts (so a region going dark
    /// is noticed even when no region is pushing at all).
    pub(super) async fn sync_tick(&self) {
        let now = chrono::Utc::now().timestamp_millis();
        let sync = &self.inner.sync;
        if let Some(cluster) = &self.inner.cluster {
            let candidate = cluster.is_lowest_among_ready_peers();
            if candidate && !sync.was_candidate.swap(true, Ordering::Relaxed) {
                sync.promoted_ms.store(now, Ordering::Relaxed);
                info!("global: this node is next in line to own alerting; re-syncing first");
            } else if !candidate {
                sync.was_candidate.store(false, Ordering::Relaxed);
            }
            let synced = sync.config_synced_ms.load(Ordering::Relaxed);
            if synced == 0 || synced < sync.promoted_ms.load(Ordering::Relaxed) {
                self.config_sync_round(now).await;
            }
        }
        let now = chrono::Utc::now().timestamp_millis();
        self.publish_readiness(now);
        self.evaluate_alerts(now).await;
    }

    fn publish_readiness(&self, now: i64) {
        let status = self.sync_status(now);
        let ready = status.ready();
        let sync = &self.inner.sync;
        let was_ready = sync.ready_since_ms.load(Ordering::Relaxed) > 0;
        if ready != was_ready {
            if ready {
                sync.ready_since_ms.store(now, Ordering::Relaxed);
                info!("global: in sync with peers; ready");
            } else {
                sync.ready_since_ms.store(0, Ordering::Relaxed);
                warn!(waiting_on = ?status.reasons(), "global: not in sync; unready");
            }
        }
        if let Some(cluster) = &self.inner.cluster {
            cluster.set_ready(ready);
        }
    }

    pub(super) fn spawn_sync_loop(&self) {
        let node = self.clone();
        let interval = self.inner.gossip_interval_secs.max(1);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                node.sync_tick().await;
            }
        });
    }
}

#[cfg(test)]
impl GlobalNode {
    /// Pretend this node became ready `ms` ago, past the handoff delay.
    pub(super) fn backdate_ready_for_test(&self, ms: i64) {
        let since = &self.inner.sync.ready_since_ms;
        let current = since.load(Ordering::Relaxed);
        if current > 0 {
            since.store(current - ms, Ordering::Relaxed);
        }
    }
}

pub(super) async fn readyz_handler(
    axum::extract::State(node): axum::extract::State<GlobalNode>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let status = node.sync_status(chrono::Utc::now().timestamp_millis());
    let code = if status.ready() {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        axum::Json(serde_json::json!({
            "ready": status.ready(),
            "membership": status.membership,
            "config": status.config,
            "state": status.state,
        })),
    )
        .into_response()
}
