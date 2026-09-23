//! The config plane's wire shapes and reconciliation walk, shared by two
//! callers: a coordinator's intra-region peer anti-entropy
//! (`coordinator::mod::reconcile_config`/`backfill_config`) and the global
//! tier's own peer anti-entropy (`global::cluster`), which replicates the
//! same `ConfigEntry`/`ConfigKind` rows between global nodes for the same
//! reason (SCALING.md "Placement-scoped configuration", Phases 1 and 4).
//!
//! Both sides hold config in a `coordinator::storage::Storage` — a global
//! node's is simply one with no results ever written to it — so the wire
//! format and the pull-and-apply loop are identical. This module is the one
//! place they are written, so the two do not drift apart the way two
//! hand-copied implementations eventually would.
//!
//! What is deliberately *not* here: which kinds a caller reconciles (a
//! coordinator does all five; a global node does the subset it holds — see
//! `global::store`), and how an applied batch updates in-memory caches, which
//! differs enough between the two callers that sharing it would mean passing
//! in most of the behaviour anyway.

use crate::coordinator::hlc::Hlc;
use crate::coordinator::storage::{ConfigDigest, ConfigEntry, ConfigKind};
use serde::{Deserialize, Serialize};

/// One config kind's digest on the wire: how many entries it has, and the
/// highest stamp among them. Mirrors `storage::ConfigDigest`, which is not
/// itself `Serialize` — it is a storage-layer type, and keeping the wire copy
/// separate is what lets a column be added to one without silently changing
/// the other's JSON shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDigestDto {
    pub kind: ConfigKind,
    pub max_hlc: Option<Hlc>,
    pub count: i64,
}

impl From<&ConfigDigest> for ConfigDigestDto {
    fn from(d: &ConfigDigest) -> Self {
        ConfigDigestDto {
            kind: d.kind,
            max_hlc: d.max_hlc.clone(),
            count: d.count,
        }
    }
}

/// The config plane's push, and the pull's response: a batch of
/// last-writer-wins entries. `origin` is informational only, mirroring
/// `ReplicationBatch::origin` — nothing here re-broadcasts.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigBatch {
    pub origin: String,
    pub entries: Vec<ConfigEntry>,
}

/// Page size for one config-kind's pull, shared so a page is a consistent
/// bounded HTTP body cost wherever this walk runs.
pub const CONFIG_BACKFILL_PAGE: usize = 1_000;

/// A value with no config-file counterpart to seed from — its seed is
/// randomness itself. Generated once, persisted as hex, and reused on every
/// subsequent boot, so identity survives a restart without an operator
/// having to set anything. Used by both a coordinator's `cluster.node_id`
/// and a global node's own.
pub fn load_or_generate(
    storage: &crate::coordinator::storage::Storage,
    key: &str,
    len: usize,
) -> String {
    if let Ok(Some(v)) = storage.get_setting(key) {
        if !v.trim().is_empty() {
            return v;
        }
    }
    use rand::RngCore;
    let mut buf = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut buf);
    let generated = crate::auth::hex(&buf);
    if let Err(e) = storage.put_setting(key, &generated) {
        tracing::warn!(error = %e, key, "could not persist generated value");
    }
    generated
}

/// A stored setting, seeded from the config file the first time it is
/// missing. A stored document that no longer parses falls back to the file
/// rather than refusing to boot: losing a preference is recoverable, a node
/// that will not start is not.
pub fn load_or_seed<T>(
    storage: &crate::coordinator::storage::Storage,
    key: &str,
    from_file: &T,
) -> T
where
    T: Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    match storage.get_setting(key) {
        Ok(Some(json)) => match serde_json::from_str(&json) {
            Ok(stored) => return stored,
            Err(e) => {
                tracing::warn!(error = %e, key, "stored setting is unreadable, using the config file")
            }
        },
        Ok(None) => {
            if let Ok(json) = serde_json::to_string(from_file) {
                if let Err(e) = storage.put_setting(key, &json) {
                    tracing::warn!(error = %e, key, "could not seed setting");
                }
            }
        }
        Err(e) => tracing::warn!(error = %e, key, "could not read setting"),
    }
    from_file.clone()
}

/// Which kinds `their` digest claims to be ahead on relative to `our`. A kind
/// is behind when the peer's highest stamp is newer, or its count differs at
/// all — a peer can be ahead on count alone if it holds a tombstone this side
/// has not observed, which does not move `max_hlc` on this side to notice.
pub fn kinds_behind(their: &[ConfigDigestDto], our: &[ConfigDigestDto]) -> Vec<ConfigKind> {
    their
        .iter()
        .filter(|theirs| {
            let ours = our.iter().find(|c| c.kind == theirs.kind);
            let (our_hlc, our_count) = match ours {
                Some(c) => (c.max_hlc.clone(), c.count),
                None => (None, 0),
            };
            let ahead = match (&theirs.max_hlc, &our_hlc) {
                (Some(t), Some(o)) => t > o,
                (Some(_), None) => true,
                (None, _) => false,
            };
            ahead || theirs.count != our_count
        })
        .map(|d| d.kind)
        .collect()
}

/// Pulls one config kind from `peer_base_url`'s `GET config_path` until the
/// peer's tail, handing each page to `apply` before requesting the next.
/// Callers differ in what `apply` does after storage — a coordinator also
/// refreshes in-memory registries the config affects, a global node persists
/// and stops there — and in what `config_path` is, since a global node's
/// pull from a region uses a different route than one global node's pull
/// from another.
pub async fn backfill_config_kind<F, Fut>(
    client: &reqwest::Client,
    config_url: &str,
    peer_token: Option<&str>,
    kind: ConfigKind,
    mut apply: F,
) -> anyhow::Result<usize>
where
    F: FnMut(Vec<ConfigEntry>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut after: Option<String> = None;
    let mut total = 0usize;
    loop {
        let mut req = client.get(config_url).query(&[
            ("kind", kind.as_str().to_string()),
            ("limit", CONFIG_BACKFILL_PAGE.to_string()),
            ("after", after.clone().unwrap_or_default()),
        ]);
        if let Some(t) = peer_token {
            req = req.bearer_auth(t);
        }
        let batch: ConfigBatch = req.send().await?.error_for_status()?.json().await?;
        let n = batch.entries.len();
        if n == 0 {
            break;
        }
        after = batch.entries.iter().map(|e| e.id.clone()).max();
        total += n;
        apply(batch.entries).await?;
        if n < CONFIG_BACKFILL_PAGE {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(kind: ConfigKind, millis: i64, count: i64) -> ConfigDigestDto {
        ConfigDigestDto {
            kind,
            max_hlc: Some(Hlc::new(millis, "n")),
            count,
        }
    }

    #[test]
    fn kinds_behind_flags_a_newer_stamp() {
        let their = vec![digest(ConfigKind::Check, 100, 1)];
        let our = vec![digest(ConfigKind::Check, 50, 1)];
        assert_eq!(kinds_behind(&their, &our), vec![ConfigKind::Check]);
    }

    #[test]
    fn kinds_behind_flags_a_missing_kind() {
        let their = vec![digest(ConfigKind::Section, 100, 1)];
        assert_eq!(kinds_behind(&their, &[]), vec![ConfigKind::Section]);
    }

    #[test]
    fn kinds_behind_ignores_a_caught_up_kind() {
        let their = vec![digest(ConfigKind::Check, 100, 3)];
        let our = vec![digest(ConfigKind::Check, 100, 3)];
        assert!(kinds_behind(&their, &our).is_empty());
    }

    #[test]
    fn kinds_behind_flags_a_count_only_difference() {
        // Same max stamp, different count: a tombstone the other side hasn't
        // observed does not move `max_hlc`, so count is what catches it.
        let their = vec![digest(ConfigKind::Check, 100, 4)];
        let our = vec![digest(ConfigKind::Check, 100, 3)];
        assert_eq!(kinds_behind(&their, &our), vec![ConfigKind::Check]);
    }
}
