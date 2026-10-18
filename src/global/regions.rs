//! Region registration: per-region credentials, discovery and removal.
//!
//! A region attaches itself. It holds one registration key, minted here
//! (Admin > Regions) or provisioned with `[[global.provision]]`, and its first
//! authenticated push makes it known — nothing declares regions up front. The
//! key is bound to one region name exactly as an agent credential is bound to
//! a zone (`credential.rs`), so a region can only ever publish as itself, and
//! only a salted hash of it is stored.
//!
//! The reverse direction uses `credential::relay_token(key)`: every regional
//! coordinator derives it from the key it already holds, and this tier learns
//! it from the region's authenticated push. It authorises calls into that one
//! region and nothing else, so a compromised region cannot impersonate the
//! global tier to its neighbours.
//!
//! Each region is one record — credential, the endpoints its pushes
//! advertised, and the relay token — persisted as a `Setting` under
//! `region:<name>`. That rides the existing global-to-global config plane, so
//! every global node can authenticate and reach every region, and a removal is
//! that entry's tombstone: LWW-ordered, so neither stale anti-entropy nor a
//! returning coordinator (whose key no longer verifies) can bring it back.

use super::GlobalNode;
use crate::configplane::ConfigBatch;
use crate::coordinator::storage::{ConfigEntry, ConfigKind};
use crate::credential::{self, AgentCredential};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Setting key prefix for a region record.
pub const KEY_PREFIX: &str = "region:";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegionRecord {
    pub name: String,
    pub credential_id: String,
    pub secret_hash: String,
    pub salt: String,
    pub created_at: i64,
    /// Coordinator base URLs learned from this region's pushes.
    #[serde(default)]
    pub endpoints: Vec<String>,
    /// Bearer for calls into this region; `None` until its first push.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_token: Option<String>,
}

impl RegionRecord {
    pub fn new(credential: AgentCredential) -> Self {
        Self {
            name: credential.region,
            credential_id: credential.id,
            secret_hash: credential.secret_hash,
            salt: credential.salt,
            created_at: credential.created_at,
            endpoints: Vec::new(),
            relay_token: None,
        }
    }

    fn verify(&self, secret: &str) -> bool {
        credential::verify(
            &AgentCredential {
                id: self.credential_id.clone(),
                zone: String::new(),
                region: self.name.clone(),
                secret_hash: self.secret_hash.clone(),
                salt: self.salt.clone(),
                created_at: self.created_at,
                revoked_at: None,
            },
            secret,
        )
    }

    /// Fold in what an authenticated push taught. `true` when the record
    /// changed and must be persisted — rare: a first push, a new advertised
    /// address, or a rotated key.
    fn learn(&mut self, relay_token: String, advertise_url: Option<&str>) -> bool {
        let mut changed = false;
        if self.relay_token.as_deref() != Some(relay_token.as_str()) {
            self.relay_token = Some(relay_token);
            changed = true;
        }
        if let Some(url) = advertise_url
            .map(|u| u.trim().trim_end_matches('/'))
            .filter(|u| !u.is_empty())
        {
            if !self.endpoints.iter().any(|e| e == url) {
                self.endpoints.push(url.to_string());
                changed = true;
            }
        }
        changed
    }

    fn key(&self) -> String {
        format!("{KEY_PREFIX}{}", self.name)
    }
}

/// Region names travel in URLs, composite incident ids (`region:id`) and
/// placement, so they stay plain identifiers.
pub fn valid_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 63 {
        return Err("region name must be 1-63 characters".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(
            "region name may only contain letters, digits, dash, underscore and dot".into(),
        );
    }
    Ok(())
}

/// Every registered region, in memory: pushes authenticate against it on
/// every interval, so it is never a database read.
#[derive(Default)]
pub struct Registry {
    records: RwLock<HashMap<String, RegionRecord>>,
}

impl Registry {
    /// The record a presented key is bound to, if it verifies.
    pub fn authenticate(&self, bearer: &str) -> Option<RegionRecord> {
        let (id, secret) = credential::split(bearer)?;
        self.records
            .read()
            .values()
            .find(|r| r.credential_id == id)
            .filter(|r| r.verify(secret))
            .cloned()
    }

    pub fn relay_token(&self, region: &str) -> Option<String> {
        self.records.read().get(region)?.relay_token.clone()
    }

    pub fn get(&self, region: &str) -> Option<RegionRecord> {
        self.records.read().get(region).cloned()
    }

    pub fn list(&self) -> Vec<RegionRecord> {
        let mut out: Vec<_> = self.records.read().values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn replace_all(&self, records: Vec<RegionRecord>) {
        *self.records.write() = records.into_iter().map(|r| (r.name.clone(), r)).collect();
    }

    fn upsert(&self, record: RegionRecord) {
        self.records.write().insert(record.name.clone(), record);
    }

    fn remove(&self, region: &str) -> bool {
        self.records.write().remove(region).is_some()
    }
}

/// Why a push was refused.
pub enum PushDenied {
    Unauthenticated,
    WrongRegion(String),
}

impl GlobalNode {
    /// Rebuild the registry from the catalogue and make the cache agree: a
    /// registered region is routable before its first push, and one removed
    /// elsewhere stops being served. Cheap — one prefix read of a handful of
    /// rows — and run only at boot and when a peer's batch touched a region.
    pub(super) async fn load_regions(&self) -> anyhow::Result<()> {
        let records = self.inner.store.regions().await?;
        let cache = &self.inner.cache;
        cache.retain_regions(|name| records.iter().any(|r| r.name == name));
        for r in &records {
            cache.seed(&r.name, &r.endpoints);
        }
        self.inner.regions.replace_all(records);
        Ok(())
    }

    /// `[[global.provision]]`, once at boot. A region already registered or
    /// deliberately removed is left alone, so a restart neither churns the
    /// config plane nor resurrects a removal.
    pub(super) async fn provision_regions(
        &self,
        blocks: &[crate::config::ProvisionedCredential],
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        for block in blocks {
            if self.inner.regions.get(&block.region).is_some()
                || self.inner.store.region_removed(&block.region).await?
            {
                continue;
            }
            let token = block
                .resolved_token()
                .ok_or_else(|| anyhow::anyhow!(block.missing_token_error()))?;
            let credential = credential::provision(&token, "", &block.region, now)
                .ok_or_else(|| anyhow::anyhow!("malformed key for region {}", block.region))?;
            self.save_region(RegionRecord::new(credential)).await?;
            tracing::info!(region = %block.region, "provisioned region registration key");
        }
        Ok(())
    }

    /// Authenticate a push and learn from it. The region is the one the key
    /// is bound to; a push naming any other is refused.
    pub(super) fn admit_push(
        &self,
        bearer: Option<&str>,
        region: &str,
        advertise_url: Option<&str>,
    ) -> Result<(), PushDenied> {
        let bearer = bearer.ok_or(PushDenied::Unauthenticated)?;
        let mut record = self
            .inner
            .regions
            .authenticate(bearer)
            .ok_or(PushDenied::Unauthenticated)?;
        if record.name != region {
            return Err(PushDenied::WrongRegion(record.name));
        }
        if record.learn(credential::relay_token(bearer), advertise_url) {
            self.inner.regions.upsert(record.clone());
            let node = self.clone();
            tokio::spawn(async move {
                if let Err(e) = node.save_region(record).await {
                    tracing::warn!(error = %e, "global: could not persist region registration");
                }
            });
        }
        Ok(())
    }

    /// Mint (or rotate) a region's key. Rotation clears the relay token: it
    /// derives from the old key, and the next push relearns it.
    pub(super) async fn mint_region(&self, name: &str) -> anyhow::Result<(RegionRecord, String)> {
        let now = chrono::Utc::now().timestamp_millis();
        let (credential, token) = credential::mint("", name, now);
        let mut record = RegionRecord::new(credential);
        if let Some(existing) = self.inner.regions.get(name) {
            record.endpoints = existing.endpoints;
        }
        self.save_region(record.clone()).await?;
        Ok((record, token))
    }

    /// Remove a region: tombstone its record (which revokes its key on every
    /// global node), drop it from routing and the cache, and strip it from
    /// every check's placement. A check placed only there is disabled rather
    /// than left with an empty placement, which would mean "every region".
    /// The region's own database is not touched — it may be gone.
    pub(super) async fn remove_region(&self, name: &str) -> anyhow::Result<Option<usize>> {
        if !self.inner.regions.remove(name) {
            return Ok(None);
        }
        let tombstone = self.inner.store.delete_region(name).await?;
        self.inner.cache.retain_regions(|r| r != name);
        self.broadcast_to_peers(vec![tombstone]);
        let checks = self.inner.store.strip_region(name).await?;
        let count = checks.len();
        super::reconcile::push_now(self, checks.clone());
        self.broadcast_to_peers(checks);
        tracing::warn!(region = name, checks = count, "region removed");
        Ok(Some(count))
    }

    /// Apply a peer global node's batch, then refresh the registry when it
    /// carried a region record.
    pub(super) async fn apply_peer_entries(&self, entries: Vec<ConfigEntry>) -> anyhow::Result<()> {
        let touches_regions = entries
            .iter()
            .any(|e| e.kind == ConfigKind::Setting && e.id.starts_with(KEY_PREFIX));
        self.inner.store.apply_entries(entries).await?;
        if touches_regions {
            self.load_regions().await?;
        }
        Ok(())
    }

    /// The relay token for whichever region `base` belongs to.
    pub(super) fn relay_token_for(&self, base: &str) -> Option<String> {
        let region = self.inner.cache.region_of_endpoint(base)?;
        self.inner.regions.relay_token(&region)
    }

    async fn save_region(&self, record: RegionRecord) -> anyhow::Result<()> {
        let entry = self.inner.store.put_region(&record.key(), &record).await?;
        self.inner.cache.seed(&record.name, &record.endpoints);
        self.inner.regions.upsert(record);
        self.broadcast_to_peers(vec![entry]);
        Ok(())
    }

    /// Best-effort immediate push to peer global nodes; anti-entropy is the
    /// backstop for any that miss it.
    pub(super) fn broadcast_to_peers(&self, entries: Vec<ConfigEntry>) {
        let Some(cluster) = self.inner.cluster.clone() else {
            return;
        };
        if entries.is_empty() {
            return;
        }
        let batch = ConfigBatch {
            origin: self.inner.store.node_id().to_string(),
            entries,
        };
        tokio::spawn(async move { cluster.broadcast_json("/internal/config", &batch).await });
    }
}

#[cfg(test)]
impl GlobalNode {
    /// Register `region` as attached with `key` at `endpoints`, as its first
    /// push would, without a database round trip.
    pub(super) fn attach_for_test(&self, region: &str, key: &str, endpoints: &[String]) {
        let mut record = RegionRecord::new(credential::provision(key, "", region, 0).unwrap());
        record.relay_token = Some(credential::relay_token(key));
        record.endpoints = endpoints.to_vec();
        self.inner.cache.seed(region, endpoints);
        self.inner.regions.upsert(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_authenticates_only_its_own_region() {
        let (c, token) = credential::mint("", "us", 1);
        let registry = Registry::default();
        registry.upsert(RegionRecord::new(c));
        assert_eq!(registry.authenticate(&token).unwrap().name, "us");
        let (id, _) = credential::split(&token).unwrap();
        assert!(registry.authenticate(&format!("{id}.wrong")).is_none());
        assert!(registry.authenticate("shared-secret").is_none());
        assert!(registry.remove("us"));
        assert!(registry.authenticate(&token).is_none(), "removal revokes");
    }

    #[test]
    fn learning_is_idempotent_so_steady_pushes_never_write() {
        let (c, token) = credential::mint("", "us", 1);
        let mut r = RegionRecord::new(c);
        let relay = credential::relay_token(&token);
        assert!(r.learn(relay.clone(), Some("http://us-a:8080/")));
        assert!(!r.learn(relay.clone(), Some("http://us-a:8080")));
        assert!(r.learn(relay, Some("http://us-b:8080")));
        assert_eq!(r.endpoints, ["http://us-a:8080", "http://us-b:8080"]);
        assert_ne!(r.relay_token.as_deref(), Some(token.as_str()));
    }

    #[test]
    fn names_stay_plain_identifiers() {
        assert!(valid_name("us-east_1.a").is_ok());
        for bad in ["", "us:east", "us/east", "us east"] {
            assert!(valid_name(bad).is_err(), "{bad:?}");
        }
    }
}
