//! Distributes the global catalogue to regions, and repairs whatever a
//! region misses or should no longer hold — the other half of SCALING.md
//! "Placement-scoped configuration" Phase 1's fix for `fan_out_write`'s old
//! gap: a write used to be applied region by region and reported as failed,
//! but already partially applied, the moment one region did not answer.
//! Since `admin` writes to `store::GlobalStore` first, that failure mode
//! does not exist any more — a write is one local, durable operation — and
//! this module is only about *distributing* what is already durable, which
//! is allowed to take retries.
//!
//! Two paths:
//! - [`push_now`]: best-effort, immediate. Spawned right after a write
//!   commits, to every region currently believed live. Catches the common
//!   case at push latency instead of waiting for the sweep below.
//! - [`spawn_region_reconciler`]: periodic, digest-driven. For every region
//!   this node knows of, pulls its config digest and pushes whatever this
//!   node's own catalogue has that the region's does not. This is the
//!   backstop: what converges a region that was down for `push_now`, or one
//!   this global node has never reached before, and it costs nothing when a
//!   region is already caught up — the digest says so before any document
//!   moves.
//!
//! **The Check kind is placement-scoped; nothing else is (Phase 2).** A
//! check's `placement.regions` names where it should be *distributed*, not
//! only where it runs, so both paths above filter check documents against
//! it before a region ever sees them. Sections carry no such per-check
//! rule — a section is a status-page grouping, not a probe target, and
//! deriving "does this region hold any check in this section" on every
//! sweep would mean the same per-check placement walk this module exists to
//! avoid, for a kind two orders of magnitude smaller than checks and (since
//! Phase 0) carrying no secret either way. Sections stay on the old
//! whole-kind push.

use super::GlobalNode;
use crate::configplane::{self, ConfigBatch, ConfigDigestDto};
use crate::coordinator::storage::{ConfigEntry, ConfigKind};
use crate::models::CheckId;
use std::collections::HashSet;
use std::time::Duration;

/// The two kinds the global tier is authoritative for in Phase 1. See
/// `store`'s module doc for why the rest stay off this path.
const AUTHORITATIVE_KINDS: [ConfigKind; 2] = [ConfigKind::Check, ConfigKind::Section];

/// How often the reconciliation sweep runs against every known region.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// Push freshly written entries to every region this node currently believes
/// is live, best-effort. Spawned by the caller (`global::admin`) so a
/// write's HTTP response never waits on a region — a region's own failure
/// here is silent by design, logged and left for the sweep to retry.
pub fn push_now(node: &GlobalNode, entries: Vec<ConfigEntry>) {
    if entries.is_empty() {
        return;
    }
    let node = node.clone();
    tokio::spawn(async move {
        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            let scoped = entries_for_region(&entries, &region);
            if scoped.is_empty() {
                // Every entry in this batch was a check whose placement
                // does not admit this region — nothing here belongs on its
                // wire, and posting an empty batch would just be a wasted
                // round trip.
                continue;
            }
            if let Err(e) = push_batch(&node, &endpoints, scoped).await {
                tracing::warn!(
                    %region, error = %e,
                    "global: optimistic config push failed, the reconciliation sweep will retry"
                );
            }
        }
    });
}

/// Which of `entries` belong on `region`'s wire. Only `ConfigKind::Check`
/// documents are filtered — see the module doc for why sections are not —
/// and a tombstone is never filtered even for a check: an outright delete
/// must reach every region that might be holding the id, and nothing here
/// records which regions that was, so scoping a delete by the *current*
/// (now-absent) placement could strand a deleted check in a region.
fn entries_for_region(entries: &[ConfigEntry], region: &str) -> Vec<ConfigEntry> {
    entries
        .iter()
        .filter(|e| e.kind != ConfigKind::Check || e.is_tombstone() || check_admits(e, region))
        .cloned()
        .collect()
}

/// Reads a check document's placement and asks whether it admits `region`.
/// Failing to parse a document this node just wrote itself should not
/// happen — `global::store` validates before persisting — but this fails
/// open (push anyway) rather than silently withholding a check from where
/// it belongs: the worst case of failing open is one region briefly holding
/// a check its placement will unplace on the next sweep, which is far
/// cheaper than a check an operator placed somewhere never arriving.
fn check_admits(entry: &ConfigEntry, region: &str) -> bool {
    let Some(doc) = entry.document.as_deref() else {
        return true;
    };
    match serde_json::from_str::<crate::config::CheckConfig>(doc) {
        Ok(check) => check.placement.admits_region(region),
        Err(e) => {
            tracing::warn!(
                id = %entry.id, error = %e,
                "global: could not read placement from a check document, pushing to every region"
            );
            true
        }
    }
}

/// POST a batch to the first endpoint in `endpoints` that answers — the same
/// try-in-order failover every other region call in this crate uses.
async fn push_batch(
    node: &GlobalNode,
    endpoints: &[String],
    entries: Vec<ConfigEntry>,
) -> anyhow::Result<()> {
    let Some(token) = node.inner.token.as_deref() else {
        anyhow::bail!("no global token configured");
    };
    let batch = ConfigBatch {
        origin: node.inner.store.node_id().to_string(),
        entries,
    };
    let mut last_err = anyhow::anyhow!("no reachable coordinator");
    for base in endpoints {
        let url = format!("{}/internal/global/config", base.trim_end_matches('/'));
        match node
            .inner
            .client
            .post(&url)
            .bearer_auth(token)
            .json(&batch)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => last_err = anyhow::anyhow!("HTTP {}", r.status()),
            Err(e) => last_err = e.into(),
        }
    }
    Err(last_err)
}

/// Starts the periodic reconciliation sweep. A no-op loop when no region is
/// ever known — it simply finds nothing to do each tick — so this is safe to
/// call unconditionally from `GlobalNode::spawn_background`.
pub fn spawn_region_reconciler(node: GlobalNode) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(RECONCILE_INTERVAL);
        loop {
            tick.tick().await;
            let now = chrono::Utc::now().timestamp_millis();
            for (region, endpoints) in node.inner.cache.region_endpoints(now) {
                reconcile_region(&node, &region, &endpoints).await;
            }
        }
    });
}

/// `pub(super)`: exercised directly by tests in `global::mod::tests` to drive
/// one reconciliation pass without waiting on `RECONCILE_INTERVAL`.
pub(super) async fn reconcile_region(node: &GlobalNode, region: &str, endpoints: &[String]) {
    let Some(token) = node.inner.token.as_deref() else {
        return;
    };
    let our = match our_digest(node, region).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(%region, error = %e, "global: could not read local config digest");
            return;
        }
    };
    for base in endpoints {
        let url = format!(
            "{}/internal/global/config-digest",
            base.trim_end_matches('/')
        );
        let resp = node.inner.client.get(&url).bearer_auth(token).send().await;
        let their: Vec<ConfigDigestDto> = match resp {
            Ok(r) if r.status().is_success() => match r.json::<Vec<ConfigDigestDto>>().await {
                Ok(d) => scoped(&d),
                Err(e) => {
                    tracing::warn!(%region, %base, error = %e, "global: malformed config-digest response");
                    continue;
                }
            },
            Ok(r) => {
                tracing::warn!(%region, %base, status = %r.status(), "global: config-digest request rejected");
                continue;
            }
            Err(e) => {
                tracing::warn!(%region, %base, error = %e, "global: config-digest request failed");
                continue;
            }
        };
        // `our` is `their` from `kinds_behind`'s point of view: it answers
        // "which of the first argument's kinds are ahead of the second's",
        // and here the region is the one that might be behind.
        for kind in configplane::kinds_behind(&our, &their) {
            match kind {
                // Checks converge by id-set diff against a placement-scoped
                // desired set, never a whole-table repush — see
                // `reconcile_checks`'s doc for why.
                ConfigKind::Check => reconcile_checks(node, region, base, token).await,
                _ => push_kind(node, region, base, kind).await,
            }
        }
        // One endpoint answering is enough for this region this sweep.
        return;
    }
}

/// This node's digest as compared against `region`: every other
/// authoritative kind uses the whole-catalogue digest `store::config_digest`
/// already computes, but the Check entry is swapped for
/// `config_digest_for_region`'s placement-scoped one before comparing.
///
/// Comparing a *whole-catalogue* check digest against a region that
/// legitimately holds only its placed subset would report "behind" forever
/// — the subset's count and max stamp almost never equal the full
/// catalogue's — and re-page the entire catalogue every sweep regardless of
/// how converged the region actually is. That is the exact amplification
/// this project has already removed twice elsewhere (the replicated-dedup
/// high-water mark, and `STREAM_RESCAN_INTERVAL`), and scoping the digest
/// itself is what keeps a converged region's steady-state cost at one
/// digest comparison rather than one full catalogue read, per sweep.
async fn our_digest(node: &GlobalNode, region: &str) -> anyhow::Result<Vec<ConfigDigestDto>> {
    let whole = node.inner.store.config_digest().await?;
    let mut digests = scoped(&whole.iter().map(ConfigDigestDto::from).collect::<Vec<_>>());
    let check_scoped =
        ConfigDigestDto::from(&node.inner.store.config_digest_for_region(region).await?);
    match digests.iter_mut().find(|d| d.kind == ConfigKind::Check) {
        Some(slot) => *slot = check_scoped,
        None => digests.push(check_scoped),
    }
    Ok(digests)
}

fn scoped(digests: &[ConfigDigestDto]) -> Vec<ConfigDigestDto> {
    digests
        .iter()
        .filter(|d| AUTHORITATIVE_KINDS.contains(&d.kind))
        .cloned()
        .collect()
}

/// Reconciles the Check kind specifically. What a region should hold is a
/// *subset* of the catalogue (its placement), so unlike every other kind
/// this cannot repair by resending the whole table when the digest
/// disagrees — that would still page the full catalogue on every drift, just
/// less often than every sweep. Instead this diffs id sets, each read
/// cheaply (primary keys only, no document parse — see
/// `Storage::check_ids_for_region` and the `/internal/global/check-ids`
/// route), and converges in both directions:
///
/// - ids this region's placement calls for but the region does not have are
///   pushed;
/// - ids the region holds but placement no longer admits are **unplaced**,
///   never deleted — SCALING.md's "Unplace is disable-shaped, not
///   delete-shaped": a tombstone would replicate and delete the check in a
///   region that still probes it.
///
/// Only runs when the digest comparison already flagged the Check kind as
/// mismatched, so a converged region never pays for this — the id-set reads
/// below are the *repair* cost, proportional to how much actually drifted,
/// not a steady-state one.
async fn reconcile_checks(node: &GlobalNode, region: &str, base: &str, token: &str) {
    let our_ids: HashSet<CheckId> = match node.inner.store.check_ids_for_region(region).await {
        Ok(ids) => ids.into_iter().collect(),
        Err(e) => {
            tracing::warn!(%region, error = %e, "global: could not read local placement-scoped check ids");
            return;
        }
    };
    let their_ids: HashSet<CheckId> = match fetch_check_ids(node, base, token).await {
        Ok(ids) => ids.into_iter().collect(),
        Err(e) => {
            tracing::warn!(%region, %base, error = %e, "global: could not read region's check ids");
            return;
        }
    };
    let missing: Vec<CheckId> = our_ids.difference(&their_ids).copied().collect();
    let excess: Vec<CheckId> = their_ids.difference(&our_ids).copied().collect();
    if !missing.is_empty() {
        push_check_ids(node, region, base, token, &missing).await;
    }
    if !excess.is_empty() {
        unplace_checks(node, region, base, token, &excess).await;
    }
}

async fn fetch_check_ids(
    node: &GlobalNode,
    base: &str,
    token: &str,
) -> anyhow::Result<Vec<CheckId>> {
    let url = format!("{}/internal/global/check-ids", base.trim_end_matches('/'));
    let resp = node
        .inner
        .client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?;
    Ok(resp.error_for_status()?.json().await?)
}

/// Pushes exactly `ids`' current documents — the mirror image of
/// `push_kind`'s whole-table page, scoped to a caller-computed set instead
/// of everything of one kind, chunked the same way so one region's repair
/// stays a bounded request size regardless of how many ids are missing.
async fn push_check_ids(node: &GlobalNode, region: &str, base: &str, token: &str, ids: &[CheckId]) {
    let url = format!("{}/internal/global/config", base.trim_end_matches('/'));
    let mut total = 0usize;
    for chunk in ids.chunks(configplane::CONFIG_BACKFILL_PAGE) {
        let mut entries = Vec::with_capacity(chunk.len());
        for id in chunk {
            match node
                .inner
                .store
                .config_entry(ConfigKind::Check, &id.to_string())
                .await
            {
                Ok(Some(entry)) => entries.push(entry),
                // Raced with a delete between listing ids and reading the
                // entry back; the tombstone push (unscoped, see
                // `entries_for_region`) will reach this region on its own.
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(%region, %id, error = %e, "global: could not read a check to push");
                }
            }
        }
        if entries.is_empty() {
            continue;
        }
        let n = entries.len();
        let batch = ConfigBatch {
            origin: node.inner.store.node_id().to_string(),
            entries,
        };
        match node
            .inner
            .client
            .post(&url)
            .bearer_auth(token)
            .json(&batch)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => total += n,
            Ok(r) => {
                tracing::warn!(%region, status = %r.status(), "global: scoped check push rejected");
                return;
            }
            Err(e) => {
                tracing::warn!(%region, error = %e, "global: scoped check push failed");
                return;
            }
        }
    }
    if total > 0 {
        tracing::info!(%region, checks = total, "global: pushed placement-scoped checks to region");
    }
}

/// Removes ids `region` holds but its placement no longer admits, through
/// the existing local-only unplace route — never a delete, which would
/// write a replicated tombstone and remove the check everywhere, including
/// a region that still probes it. The handler is idempotent (404 on an id
/// already gone), so a race with the region unplacing it by some other path
/// is silent rather than a warning.
async fn unplace_checks(node: &GlobalNode, region: &str, base: &str, token: &str, ids: &[CheckId]) {
    let mut total = 0usize;
    for id in ids {
        let url = format!(
            "{}/api/v1/admin/checks/{id}/unplace",
            base.trim_end_matches('/')
        );
        match node
            .inner
            .client
            .delete(&url)
            .bearer_auth(token)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() || r.status() == reqwest::StatusCode::NOT_FOUND => {
                total += 1;
            }
            Ok(r) => {
                tracing::warn!(%region, %id, status = %r.status(), "global: unplace rejected")
            }
            Err(e) => tracing::warn!(%region, %id, error = %e, "global: unplace request failed"),
        }
    }
    if total > 0 {
        tracing::info!(
            %region, checks = total,
            "global: unplaced checks the region's placement no longer admits"
        );
    }
}

/// Pages this node's own store for `kind` and pushes every page to `base`,
/// the mirror image of `configplane::backfill_config_kind`'s pull: the
/// source is local storage rather than an HTTP GET, because this side is
/// always authoritative for `kind` and never needs to ask the region for it.
///
/// Used for `ConfigKind::Section` only now that `ConfigKind::Check` has its
/// own placement-scoped repair in `reconcile_checks` — see the module doc.
async fn push_kind(node: &GlobalNode, region: &str, base: &str, kind: ConfigKind) {
    let Some(token) = node.inner.token.as_deref() else {
        return;
    };
    let url = format!("{}/internal/global/config", base.trim_end_matches('/'));
    let mut after: Option<String> = None;
    let mut total = 0usize;
    loop {
        let page = match node
            .inner
            .store
            .config_entries_page(kind, after.clone(), configplane::CONFIG_BACKFILL_PAGE)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(%region, kind = kind.as_str(), error = %e, "global: could not page local config");
                return;
            }
        };
        let n = page.len();
        if n == 0 {
            break;
        }
        after = page.iter().map(|e| e.id.clone()).max();
        let batch = ConfigBatch {
            origin: node.inner.store.node_id().to_string(),
            entries: page,
        };
        match node
            .inner
            .client
            .post(&url)
            .bearer_auth(token)
            .json(&batch)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => {
                tracing::warn!(%region, kind = kind.as_str(), status = %r.status(), "global: config push rejected");
                return;
            }
            Err(e) => {
                tracing::warn!(%region, kind = kind.as_str(), error = %e, "global: config push failed");
                return;
            }
        }
        total += n;
        if n < configplane::CONFIG_BACKFILL_PAGE {
            break;
        }
        tokio::task::yield_now().await;
    }
    if total > 0 {
        tracing::info!(%region, kind = kind.as_str(), entries = total, "global: catalogue reconciled to region");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CheckConfig;
    use crate::coordinator::hlc::Hlc;
    use crate::placement::Placement;

    fn check_entry(id: uuid::Uuid, regions: &[&str]) -> ConfigEntry {
        let check = CheckConfig {
            id,
            placement: Placement {
                regions: regions.iter().map(|r| r.to_string()).collect(),
                ..Placement::default()
            },
            ..CheckConfig::default()
        };
        ConfigEntry {
            kind: ConfigKind::Check,
            id: id.to_string(),
            hlc: Hlc::new(1, "global"),
            document: Some(serde_json::to_string(&check).unwrap()),
        }
    }

    #[test]
    fn entries_for_region_keeps_only_checks_placed_there() {
        let us_only = check_entry(uuid::Uuid::new_v4(), &["us"]);
        let everywhere = check_entry(uuid::Uuid::new_v4(), &[]);
        let entries = vec![us_only.clone(), everywhere.clone()];

        let for_us = entries_for_region(&entries, "us");
        assert_eq!(for_us.len(), 2, "both admit us");

        let for_tw = entries_for_region(&entries, "tw");
        assert_eq!(
            for_tw,
            vec![everywhere],
            "the us-only check must not reach tw"
        );
    }

    #[test]
    fn entries_for_region_never_scopes_a_tombstone() {
        let tombstone = ConfigEntry {
            kind: ConfigKind::Check,
            id: uuid::Uuid::new_v4().to_string(),
            hlc: Hlc::new(1, "global"),
            document: None,
        };
        let entries = vec![tombstone.clone()];
        assert_eq!(
            entries_for_region(&entries, "anywhere"),
            vec![tombstone],
            "a delete must reach every region regardless of the last known placement"
        );
    }

    #[test]
    fn entries_for_region_never_scopes_a_section() {
        let section = ConfigEntry {
            kind: ConfigKind::Section,
            id: "infra".to_string(),
            hlc: Hlc::new(1, "global"),
            document: Some("{}".to_string()),
        };
        let entries = vec![section.clone()];
        assert_eq!(
            entries_for_region(&entries, "anywhere"),
            vec![section],
            "sections are not placement-scoped"
        );
    }

    // End-to-end reconciliation (id-set convergence, unplace-then-replace,
    // the tw-never-holds-a-us-only-check disclosure test, and the
    // no-further-paging-once-converged cost test) live in
    // `global::mod::tests`, which has the running coordinators this module
    // only ever talks to over HTTP.
}
