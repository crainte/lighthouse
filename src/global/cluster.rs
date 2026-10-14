//! Global-to-global membership and config anti-entropy (SCALING.md
//! "Placement-scoped configuration", Phase 4: "run the global as three
//! nodes").
//!
//! Reuses `coordinator::cluster::Cluster` unchanged — it was already generic
//! over who runs it, nothing in it assumes a coordinator — for membership,
//! and `configplane`'s config digest/backfill walk for the catalogue itself.
//! What is new here is only the HTTP surface: three routes shaped exactly
//! like a coordinator's own `/internal/cluster/ping`, `/internal/config` and
//! `/internal/digest`, wired to `store::GlobalStore` instead of
//! `coordinator::storage::Storage` directly, and gated by `global.peer_token`
//! instead of a region's.
//!
//! A global node replicates its catalogue to its peers here. It never
//! replicates results, because it never has any — see `store`'s module doc.

use super::GlobalNode;
use crate::configplane::{self, ConfigBatch, ConfigDigestDto};
use crate::coordinator::cluster::{Cluster, PingBody};
use crate::coordinator::storage::ConfigKind;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::time::Duration;

/// Mirrors `coordinator::routes::peer::require_peer`: a global node's own
/// peers authenticate with `global.peer_token`, never the cross-region trust
/// token `global::admin` and `reconcile` use to talk to a coordinator.
fn require_peer(node: &GlobalNode, headers: &HeaderMap) -> Option<Response> {
    use subtle::ConstantTimeEq;
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let ok = match (node.inner.peer_token.as_deref(), bearer) {
        (Some(expected), Some(got)) => bool::from(got.as_bytes().ct_eq(expected.as_bytes())),
        _ => false,
    };
    (!ok).then(|| super::error_response(StatusCode::UNAUTHORIZED, "peer authentication required"))
}

pub(super) async fn ping_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Json(body): Json<PingBody>,
) -> Response {
    if let Some(denied) = require_peer(&node, &headers) {
        return denied;
    }
    let Some(cluster) = &node.inner.cluster else {
        return super::error_response(StatusCode::NOT_FOUND, "clustering is not configured");
    };
    let now = chrono::Utc::now().timestamp_millis();
    cluster.answer_ping(&body, now)
}

pub(super) async fn config_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Json(batch): Json<ConfigBatch>,
) -> Response {
    if let Some(denied) = require_peer(&node, &headers) {
        return denied;
    }
    match node.apply_peer_entries(batch.entries).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => super::error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

#[derive(Deserialize)]
pub(super) struct ConfigEntriesQuery {
    kind: Option<ConfigKind>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    configplane::CONFIG_BACKFILL_PAGE
}

pub(super) async fn config_entries_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Query(q): Query<ConfigEntriesQuery>,
) -> Response {
    if let Some(denied) = require_peer(&node, &headers) {
        return denied;
    }
    let Some(kind) = q.kind else {
        return super::error_response(StatusCode::BAD_REQUEST, "config kind is required");
    };
    let limit = q.limit.clamp(1, configplane::CONFIG_BACKFILL_PAGE);
    match node
        .inner
        .store
        .config_entries_page(kind, q.after, limit)
        .await
    {
        Ok(entries) => Json(ConfigBatch {
            origin: node.inner.store.node_id().to_string(),
            entries,
        })
        .into_response(),
        Err(e) => super::error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

pub(super) async fn digest_handler(State(node): State<GlobalNode>, headers: HeaderMap) -> Response {
    if let Some(denied) = require_peer(&node, &headers) {
        return denied;
    }
    match node.inner.store.config_digest().await {
        Ok(d) => Json(d.iter().map(ConfigDigestDto::from).collect::<Vec<_>>()).into_response(),
        Err(e) => super::error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// Starts membership heartbeat and catalogue anti-entropy against peer global
/// nodes. A no-op when `global.peers` is not configured.
pub(super) fn spawn(node: &GlobalNode) {
    let Some(cluster) = node.inner.cluster.clone() else {
        return;
    };
    let heartbeat_cluster = cluster.clone();
    let heartbeat_node = node.clone();
    let gossip_interval = node.inner.gossip_interval_secs;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(gossip_interval));
        let mut persisted = Vec::new();
        loop {
            tick.tick().await;
            let now = chrono::Utc::now().timestamp_millis();
            heartbeat_cluster.ping_all(now).await;
            heartbeat_cluster.sweep(now);
            crate::coordinator::cluster::persist_membership(
                &heartbeat_cluster,
                heartbeat_node.inner.store.database(),
                &mut persisted,
            )
            .await;
        }
    });

    let retry_cluster = cluster.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(
            gossip_interval.saturating_mul(3).max(1),
        ));
        loop {
            tick.tick().await;
            retry_cluster
                .retry_discovery(chrono::Utc::now().timestamp_millis())
                .await;
        }
    });

    let anti_entropy_node = node.clone();
    let anti_entropy_secs = node.inner.anti_entropy_secs;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(anti_entropy_secs));
        loop {
            tick.tick().await;
            anti_entropy_sweep(&anti_entropy_node).await;
        }
    });
}

/// Reconciles this node's catalogue with one live peer global node, chosen at
/// random the same way a coordinator's own anti-entropy sweep picks a peer —
/// spreading load and rotating who repairs whom rather than always picking
/// the first entry in the member list.
async fn anti_entropy_sweep(node: &GlobalNode) {
    let Some(cluster) = &node.inner.cluster else {
        return;
    };
    let peer = {
        use rand::seq::IndexedRandom;
        let live = cluster.live_urls();
        let Some(peer) = live.choose(&mut rand::rng()).cloned() else {
            return;
        };
        peer
    };
    reconcile_with_peer(node, &peer).await;
}

/// `pub(super)`: exercised directly by tests in `global::mod::tests` to drive
/// one anti-entropy pass without waiting on the timer.
pub(super) async fn reconcile_with_peer(node: &GlobalNode, peer: &str) {
    let our = match node.inner.store.config_digest().await {
        Ok(d) => d.iter().map(ConfigDigestDto::from).collect::<Vec<_>>(),
        Err(e) => {
            tracing::warn!(%peer, error = %e, "global cluster: could not read local config digest");
            node.inner
                .peer_reconcile_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };
    let url = Cluster::endpoint(peer, "/internal/digest");
    let their: Vec<ConfigDigestDto> = match node
        .inner
        .client
        .get(&url)
        .bearer_auth(node.inner.peer_token.as_deref().unwrap_or_default())
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(%peer, error = %e, "global cluster: malformed digest response");
                node.inner
                    .peer_reconcile_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        },
        Ok(r) => {
            tracing::warn!(%peer, status = %r.status(), "global cluster: digest request rejected");
            node.inner
                .peer_reconcile_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        Err(e) => {
            tracing::warn!(%peer, error = %e, "global cluster: digest request failed");
            node.inner
                .peer_reconcile_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };
    let kinds_behind = configplane::kinds_behind(&their, &our);
    node.inner.peer_reconcile_last_success_ms.store(
        chrono::Utc::now().timestamp_millis(),
        std::sync::atomic::Ordering::Relaxed,
    );
    for kind in kinds_behind {
        let config_url = Cluster::endpoint(peer, "/internal/config");
        let peer_token = node.inner.peer_token.clone();
        let result = configplane::backfill_config_kind(
            &node.inner.client,
            &config_url,
            peer_token.as_deref(),
            kind,
            |entries| node.apply_peer_entries(entries),
        )
        .await;
        match result {
            Ok(0) => {}
            Ok(total) => {
                tracing::info!(%peer, kind = kind.as_str(), entries = total, "global cluster: catalogue reconciled");
            }
            Err(e) => {
                tracing::warn!(%peer, kind = kind.as_str(), error = %e, "global cluster: config pull failed");
                node.inner
                    .peer_reconcile_repair_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        }
    }
}
