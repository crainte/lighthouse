//! The global tier's own durable catalogue (SCALING.md "Placement-scoped
//! configuration", Phase 1).
//!
//! A global node is close to *a coordinator that holds the catalogue but no
//! results* — see SCALING.md's "The shape" — so this reuses the
//! coordinator's config plane wholesale rather than inventing a parallel
//! one: the same `coordinator::database::Database` worker, the same
//! `coordinator::storage::Storage`, the same `ConfigEntry`/`ConfigKind`
//! HLC-arbitrated write path. `Storage::open` still creates every table a
//! coordinator has, results and rollups included — it is the same schema —
//! but nothing here ever writes a result, a rollup, or an incident; only
//! `checks` and `sections` are ever touched, which is deliberately a subset
//! of what `ConfigKind` covers (see the module doc on scope, below).
//!
//! **Scope of Phase 1's authoritative fast path: checks and sections only.**
//! Those two are "the catalogue" in the sense SCALING.md's example is about —
//! whole check documents and how they are grouped on the status page — and
//! neither carries a secret that must stay in one region (Phase 0 already
//! moved targets and headers to references). `Setting` (notifiers, the fleet
//! display), `Credential` and `RollupPolicy` stay on the pre-existing
//! region-by-region fan-out in `global::mod`:
//! - A notifier or the fleet display is one JSON document with no identity
//!   or position to reconcile, but reading it back also means merging
//!   per-region computed context (undelivered notifications, a region's own
//!   fleet roster) that this phase does not attempt to unify.
//! - A credential is minted, not authored: the secret exists only once, at
//!   the region that generates it, and holding a copy globally would be
//!   exactly the boundary-crossing Phase 0 exists to prevent.
//! - A rollup policy is keyed by a region/zone node id a global node has no
//!   independent view of.
//!
//! Widening this list is Phase 2/3 work, not a Phase 1 gap: the mechanism
//! below (write locally, stamp with this node's clock, hand the entry to a
//! reconciler) works unchanged for any `ConfigKind`.

use crate::config::CheckConfig;
use crate::coordinator::database::Database;
use crate::coordinator::hlc::{self, Hlc};
use crate::coordinator::storage::{ConfigDigest, ConfigEntry, ConfigKind, Section, SectionDisplay};
use crate::models::CheckId;
use anyhow::Result;

/// Setting key under which this node's own identity is persisted, once
/// generated — see `configplane::load_or_generate`. Distinct from a
/// coordinator's `cluster.node_id` only by living in a different database.
const NODE_ID_KEY: &str = "cluster.node_id";

/// Why an admin write against the catalogue was refused, carrying enough to
/// answer with the right HTTP status: a malformed or invalid document (400),
/// a name collision (409), an id that does not exist (404), or something
/// that went wrong underneath (500).
pub enum AdminError {
    BadRequest(String),
    Conflict(String),
    NotFound,
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for AdminError {
    fn from(e: anyhow::Error) -> Self {
        AdminError::Internal(e)
    }
}

/// The global tier's catalogue: a dedicated database worker plus the clock
/// that stamps every local edit. Held once, behind the `Arc` in
/// `global::Inner`, and reached through `&self` everywhere — there is
/// nothing here to clone independently of the node that owns it.
pub struct GlobalStore {
    database: Database,
    clock: hlc::Clock,
    node_id: String,
}

impl GlobalStore {
    /// Opens (or creates) the catalogue at `db_path` and resolves this node's
    /// identity: config-supplied, or generated once and persisted so a
    /// restart rejoins as itself — the same rule a coordinator's `node_id`
    /// follows, via the same `configplane::load_or_generate`.
    pub async fn open(db_path: String, configured_node_id: Option<String>) -> Result<Self> {
        let database = Database::open(db_path).await?;
        let node_id = database
            .call(move |storage| {
                Ok(match configured_node_id {
                    Some(id) => id,
                    None => crate::configplane::load_or_generate(storage, NODE_ID_KEY, 16),
                })
            })
            .await?;
        let clock = hlc::Clock::new(node_id.clone());
        Ok(Self {
            database,
            clock,
            node_id,
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    #[track_caller]
    fn db<T, F>(&self, operation: F) -> impl std::future::Future<Output = Result<T>> + Send + '_
    where
        T: Send + 'static,
        F: FnOnce(&crate::coordinator::storage::Storage) -> Result<T> + Send + 'static,
    {
        self.database
            .call_at(operation, std::panic::Location::caller())
    }

    /// Observe a stamp seen on the wire, so this node's own clock never
    /// issues one a peer (global or region) would read as older than
    /// something it already holds — the same rule `Coordinator::clock`
    /// follows for every replicated entry.
    pub fn observe(&self, hlc: &Hlc) {
        self.clock.observe(hlc);
    }

    // ---- Generic config-plane access, for `global::cluster` anti-entropy
    //      and reconciliation against regions ----

    pub async fn config_digest(&self) -> Result<Vec<ConfigDigest>> {
        self.db(|storage| storage.config_digest()).await
    }

    pub async fn config_entries_page(
        &self,
        kind: ConfigKind,
        after: Option<String>,
        limit: usize,
    ) -> Result<Vec<ConfigEntry>> {
        self.db(move |storage| storage.config_entries_page(kind, after.as_deref(), limit))
            .await
    }

    /// One id's current replicated form, for pushing exactly the checks a
    /// region's placement is missing rather than repaging the whole kind —
    /// see `global::reconcile::reconcile_checks`.
    pub async fn config_entry(&self, kind: ConfigKind, id: &str) -> Result<Option<ConfigEntry>> {
        let id = id.to_string();
        self.db(move |storage| storage.config_entry(kind, &id))
            .await
    }

    /// Ids `region`'s placement admits, computed locally against the
    /// `check_placement_regions` projection — see
    /// `Storage::check_ids_for_region`.
    pub async fn check_ids_for_region(&self, region: &str) -> Result<Vec<CheckId>> {
        let region = region.to_string();
        self.db(move |storage| storage.check_ids_for_region(&region))
            .await
    }

    /// This region's placement-scoped contribution to a Check-kind digest —
    /// see `Storage::config_digest_for_region` for why a whole-catalogue
    /// digest cannot answer "is this region converged" once regions hold
    /// different subsets.
    pub async fn config_digest_for_region(&self, region: &str) -> Result<ConfigDigest> {
        let region = region.to_string();
        self.db(move |storage| storage.config_digest_for_region(&region))
            .await
    }

    /// Apply a batch received from a peer global node or a reconciliation
    /// pull. Pure LWW arbitration, exactly `Storage::apply_config_entries` —
    /// there are no in-memory registries here to refresh afterwards, unlike
    /// a coordinator's `apply_config_batch`.
    pub async fn apply_entries(&self, entries: Vec<ConfigEntry>) -> Result<Vec<bool>> {
        for entry in &entries {
            self.observe(&entry.hlc);
        }
        self.db(move |storage| storage.apply_config_entries(&entries))
            .await
    }

    /// Stamp a document with this node's clock and apply it through the same
    /// LWW path a replicated entry takes, so a local edit and one that
    /// arrived from a peer can never disagree about what wins. Mirrors
    /// `Coordinator::write_config`.
    async fn write_config(
        &self,
        kind: ConfigKind,
        id: &str,
        document: Option<String>,
    ) -> Result<ConfigEntry> {
        let entry = ConfigEntry {
            kind,
            id: id.to_string(),
            hlc: self.clock.now(),
            document,
        };
        let applying = entry.clone();
        self.db(move |storage| storage.apply_config_entries(&[applying]).map(|_| ()))
            .await?;
        Ok(entry)
    }

    /// Re-read whatever `id` currently holds and reapply it under a fresh
    /// stamp — a same-document no-op locally, but what gives a row mutated
    /// through a bulk storage method (reorder, section rename cascade) a
    /// stamp worth pushing to a region. Mirrors `Coordinator::restamp_and_push`
    /// minus the push, which `global::reconcile` does with the returned entry.
    async fn restamp(&self, kind: ConfigKind, id: &str) -> Option<ConfigEntry> {
        let owned_id = id.to_string();
        let existing = self
            .db(move |storage| storage.config_entry(kind, &owned_id))
            .await
            .ok()
            .flatten()?;
        self.write_config(kind, id, existing.document).await.ok()
    }

    // ---- Checks ----

    pub async fn list_checks(
        &self,
        page: usize,
        limit: usize,
    ) -> Result<(Vec<CheckConfig>, usize, bool)> {
        let mut checks = self.db(|storage| storage.list_checks()).await?;
        let (total, has_more) = crate::page::take(&mut checks, page, limit);
        Ok((checks, total, has_more))
    }

    pub async fn create_check(
        &self,
        body: serde_json::Value,
    ) -> Result<(CheckConfig, ConfigEntry), AdminError> {
        let mut check = CheckConfig::from_submitted(body, true)
            .map_err(AdminError::BadRequest)?
            .normalised();
        // Global always mints identity for a browser-facing create: there is
        // no "trusted caller who already minted one" here the way a region
        // accepts from the global relay today — global itself is now that
        // authority, so it always chooses.
        check.id = crate::models::new_check_id();
        check.validate().map_err(AdminError::BadRequest)?;
        let name = check.name.clone();
        let (existing, position) = self
            .db(|storage| Ok((storage.list_checks()?, storage.next_check_position()?)))
            .await?;
        if existing.iter().any(|c| c.name == name) {
            return Err(AdminError::Conflict(format!(
                "a check named \"{name}\" already exists"
            )));
        }
        check.position = position;
        let entry = self.write_check(&check).await?;
        Ok((check, entry))
    }

    pub async fn update_check(
        &self,
        id: CheckId,
        body: serde_json::Value,
    ) -> Result<(CheckConfig, ConfigEntry), AdminError> {
        let mut check = CheckConfig::from_submitted(body, false)
            .map_err(AdminError::BadRequest)?
            .normalised();
        check.id = id;
        check.validate().map_err(AdminError::BadRequest)?;
        let existing = self.db(|storage| storage.list_checks()).await?;
        if !existing.iter().any(|c| c.id == id) {
            return Err(AdminError::NotFound);
        }
        if existing.iter().any(|c| c.id != id && c.name == check.name) {
            return Err(AdminError::Conflict(format!(
                "a check named \"{}\" already exists",
                check.name
            )));
        }
        let entry = self.write_check(&check).await?;
        Ok((check, entry))
    }

    async fn write_check(&self, check: &CheckConfig) -> Result<ConfigEntry> {
        let document = serde_json::to_string(check)?;
        self.write_config(ConfigKind::Check, &check.id.to_string(), Some(document))
            .await
    }

    pub async fn delete_check(&self, id: CheckId) -> Result<ConfigEntry, AdminError> {
        let existing = self.db(|storage| storage.list_checks()).await?;
        if !existing.iter().any(|c| c.id == id) {
            return Err(AdminError::NotFound);
        }
        Ok(self
            .write_config(ConfigKind::Check, &id.to_string(), None)
            .await?)
    }

    /// Apply an explicit display order, restamping only the checks whose
    /// position actually moved — the ones a region needs to hear about.
    /// Mirrors `reorder_checks_handler`.
    pub async fn reorder_checks(
        &self,
        order: Vec<CheckId>,
    ) -> Result<(Vec<CheckConfig>, Vec<ConfigEntry>)> {
        let now = chrono::Utc::now().timestamp_millis();
        let before: std::collections::HashMap<CheckId, i32> = self
            .db(|storage| {
                Ok(storage
                    .list_checks()?
                    .into_iter()
                    .map(|c| (c.id, c.position))
                    .collect())
            })
            .await?;
        self.db(move |storage| storage.reorder_checks(&order, now))
            .await?;
        let checks = self.db(|storage| storage.list_checks()).await?;
        let mut entries = Vec::new();
        for c in &checks {
            if before.get(&c.id) != Some(&c.position) {
                if let Some(entry) = self.restamp(ConfigKind::Check, &c.id.to_string()).await {
                    entries.push(entry);
                }
            }
        }
        Ok((checks, entries))
    }

    // ---- Sections ----

    pub async fn list_sections(&self) -> Result<Vec<Section>> {
        self.db(|storage| storage.list_sections()).await
    }

    pub async fn create_section(
        &self,
        name: &str,
        collapsed: bool,
        display: SectionDisplay,
    ) -> Result<(Section, ConfigEntry), AdminError> {
        let name = Section::valid_name(name).map_err(AdminError::BadRequest)?;
        let create_name = name.clone();
        let created = self
            .db(move |storage| storage.create_section(&create_name, collapsed, display))
            .await?;
        if !created {
            return Err(AdminError::Conflict(format!(
                "a section named \"{name}\" already exists"
            )));
        }
        let entry = self
            .restamp(ConfigKind::Section, &name)
            .await
            .ok_or(AdminError::NotFound)?;
        Ok((
            Section {
                name,
                position: 0,
                collapsed,
                display,
            },
            entry,
        ))
    }

    /// Renames a section, carrying its checks with it. Returns the section's
    /// own entry plus one for every check `Storage::rename_section` moved —
    /// its `group` changed too, and a region that heard only about the
    /// section would show those checks still under the old name.
    pub async fn rename_section(
        &self,
        current: &str,
        name: &str,
        collapsed: Option<bool>,
        display: Option<SectionDisplay>,
    ) -> Result<(Section, Vec<ConfigEntry>), AdminError> {
        let name = Section::valid_name(name).map_err(AdminError::BadRequest)?;
        if name != current {
            let taken = self
                .db(|storage| storage.list_sections())
                .await?
                .iter()
                .any(|s| s.name == name);
            if taken {
                return Err(AdminError::Conflict(format!(
                    "a section named \"{name}\" already exists"
                )));
            }
        }
        let now = chrono::Utc::now().timestamp_millis();
        let (from, to) = (current.to_string(), name.clone());
        let renamed = self
            .db(move |storage| {
                let renamed = storage.rename_section(&from, &to, now)?;
                if renamed {
                    if let Some(collapsed) = collapsed {
                        storage.set_section_collapsed(&to, collapsed)?;
                    }
                    if let Some(display) = display {
                        storage.set_section_display(&to, display)?;
                    }
                }
                Ok(renamed)
            })
            .await?;
        if !renamed {
            return Err(AdminError::NotFound);
        }
        let moved_checks: Vec<CheckId> = self
            .db({
                let name = name.clone();
                move |storage| {
                    Ok(storage
                        .list_checks()?
                        .into_iter()
                        .filter(|c| c.group.as_deref() == Some(name.as_str()))
                        .map(|c| c.id)
                        .collect())
                }
            })
            .await?;
        let mut entries = Vec::new();
        if let Some(entry) = self.restamp(ConfigKind::Section, &name).await {
            entries.push(entry);
        }
        for check_id in moved_checks {
            if let Some(entry) = self.restamp(ConfigKind::Check, &check_id.to_string()).await {
                entries.push(entry);
            }
        }
        let stored = self
            .db(|storage| storage.list_sections())
            .await?
            .into_iter()
            .find(|s| s.name == name)
            .unwrap_or(Section {
                name: name.clone(),
                position: 0,
                collapsed: collapsed.unwrap_or(false),
                display: display.unwrap_or_default(),
            });
        Ok((stored, entries))
    }

    /// Deletes a section, loosening (not deleting) the checks in it. Returns
    /// the tombstone plus one entry per loosened check, whose `group` is now
    /// `None`.
    pub async fn delete_section(&self, name: &str) -> Result<Vec<ConfigEntry>, AdminError> {
        let affected: Vec<CheckId> = self
            .db({
                let name = name.to_string();
                move |storage| {
                    Ok(storage
                        .list_checks()?
                        .into_iter()
                        .filter(|c| c.group.as_deref() == Some(name.as_str()))
                        .map(|c| c.id)
                        .collect())
                }
            })
            .await?;
        let now = chrono::Utc::now().timestamp_millis();
        let delete_name = name.to_string();
        let deleted = self
            .db(move |storage| storage.delete_section(&delete_name, now))
            .await?;
        if !deleted {
            return Err(AdminError::NotFound);
        }
        let mut entries = vec![self.write_config(ConfigKind::Section, name, None).await?];
        for check_id in affected {
            if let Some(entry) = self.restamp(ConfigKind::Check, &check_id.to_string()).await {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Applies an explicit section order, restamping only the sections whose
    /// position actually moved.
    pub async fn reorder_sections(
        &self,
        order: Vec<String>,
    ) -> Result<(Vec<Section>, Vec<ConfigEntry>)> {
        let before = self.db(|storage| storage.list_sections()).await?;
        self.db(move |storage| storage.reorder_sections(&order))
            .await?;
        let after = self.db(|storage| storage.list_sections()).await?;
        let before_pos: std::collections::HashMap<&str, i32> = before
            .iter()
            .map(|s| (s.name.as_str(), s.position))
            .collect();
        let mut entries = Vec::new();
        for s in &after {
            if before_pos.get(s.name.as_str()) != Some(&s.position) {
                if let Some(entry) = self.restamp(ConfigKind::Section, &s.name).await {
                    entries.push(entry);
                }
            }
        }
        Ok((after, entries))
    }
}
