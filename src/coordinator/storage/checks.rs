use super::*;

/// A stored rollup policy for one node of the tree.
#[derive(Debug, Clone, PartialEq)]
pub struct RollupPolicyRow {
    /// Namespaced: `global`, `region:us-east`, `zone:us-east-1a`.
    pub node_id: String,
    pub node_type: NodeType,
    pub policy: crate::rollup::RollupPolicy,
    pub updated_at: i64,
    pub updated_by: Option<String>,
}

/// Which level of the tree a node sits at.
///
/// Unrelated to `Incident.global`, which means "shown page-wide" and is about
/// presentation. Two different words called global; do not conflate them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    Global,
    Region,
    Zone,
}

impl NodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeType::Global => "global",
            NodeType::Region => "region",
            NodeType::Zone => "zone",
        }
    }

    pub fn from_db(s: &str) -> NodeType {
        match s {
            "region" => NodeType::Region,
            "zone" => NodeType::Zone,
            _ => NodeType::Global,
        }
    }
}

/// How the public page renders a section. An operator choice per section, not a
/// global rule: a three-check section is better inline, and forcing a click for
/// it would be worse than a flat page. A large one is unreadable inline and
/// costs a strip per check to render, which is what `Page` bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SectionDisplay {
    /// Render the section's rows on the primary page, as it always has.
    #[default]
    Inline,
    /// Render a summary tile on the primary page linking to the section's own
    /// page, where its rows and their charts are loaded on demand.
    Page,
}

impl SectionDisplay {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Page => "page",
        }
    }

    /// Anything unrecognised reads as `Inline`. A row written by a newer peer
    /// with a mode this build has never heard of must still render its checks;
    /// hiding the section instead would hide live monitoring.
    pub fn from_db(s: &str) -> Self {
        match s {
            "page" => Self::Page,
            _ => Self::Inline,
        }
    }
}

/// A named, ordered group on the status page. Stored separately from checks so
/// it can exist while empty and be renamed without touching what is in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Section {
    pub name: String,
    pub position: i32,
    /// Whether the public page renders this section collapsed on first load.
    /// The initial render state of an *inline* section, and meaningless for a
    /// paged one — a tile has nothing to collapse.
    pub collapsed: bool,
    /// Defaulted so a section document written by a peer that predates the mode
    /// still parses, rendering as it does today rather than failing the batch.
    #[serde(default)]
    pub display: SectionDisplay,
}

impl Section {
    /// Section names appear as headings and are matched against
    /// `checks.group_name`, so an empty or absurd one is rejected at the
    /// boundary. Shared by the coordinator's own admin API (`routes::sections`)
    /// and the global tier's (`global::store`): both accept the same
    /// submission from the same admin console.
    pub fn valid_name(name: &str) -> Result<String, String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err("section name is required".into());
        }
        if trimmed.chars().count() > 60 {
            return Err("section name must be 60 characters or fewer".into());
        }
        Ok(trimmed.to_string())
    }
}

/// Approximate gap between runs, for the legacy `interval_secs` column only.
/// Nothing reads it back; it exists so a rolled-back binary isn't left with a
/// zero interval.
fn cron_to_seconds(cron: &str) -> u64 {
    let Ok(schedule) = CheckConfig::parse_cron(cron) else {
        return 30;
    };
    let mut it = schedule.upcoming(chrono::Utc);
    match (it.next(), it.next()) {
        (Some(a), Some(b)) => (b - a).num_seconds().clamp(1, 86_400) as u64,
        _ => 30,
    }
}

/// What an erase actually removed, so the UI can say it rather than claiming
/// something generic. `deleted_incidents` is also what the caller replicates.
#[derive(Debug, Clone, PartialEq)]
pub struct PurgeSummary {
    pub results: u64,
    pub incidents: u64,
    pub deleted_incidents: Vec<String>,
}

impl Storage {
    /// Every configured check, by name. Includes disabled ones: the admin UI has
    /// to show them to re-enable them.
    pub fn list_checks(&self) -> Result<Vec<CheckConfig>> {
        let mut stmt = self.conn.prepare(
            "SELECT config FROM checks WHERE config IS NOT NULL ORDER BY position, name",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            let raw = row?;
            // A row that won't parse is skipped rather than taking down every
            // read: one bad document must not hide the rest of the checks.
            match serde_json::from_str::<CheckConfig>(&raw) {
                Ok(c) => out.push(c),
                Err(e) => tracing::error!(error = %e, "skipping unreadable check row"),
            }
        }
        Ok(out)
    }

    /// Every id `checks` currently holds. Reads only the primary key — no
    /// `config` JSON parse — so a peer's "what do you have" reconciliation
    /// query (`global::reconcile::reconcile_checks`) costs nothing like
    /// `list_checks()`'s per-document scan, and stays cheap however many
    /// checks are held.
    pub fn check_ids(&self) -> Result<Vec<CheckId>> {
        let mut stmt = self.conn.prepare("SELECT id FROM checks ORDER BY id")?;
        let rows = stmt.query_map([], |r| r.get::<_, CheckId>(0))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Ids `region`'s placement admits — the desired set a global node
    /// computes locally, then diffs against what the region itself reports
    /// holding (`check_ids`, fetched over `/internal/global/check-ids`) to
    /// find what to push and what to unplace. See `check_placement_regions`'s
    /// table comment for the "no rows means every region" rule this mirrors.
    pub fn check_ids_for_region(&self, region: &str) -> Result<Vec<CheckId>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.id FROM checks c
             WHERE NOT EXISTS (
                       SELECT 1 FROM check_placement_regions r WHERE r.check_id = c.id
                   )
                OR EXISTS (
                       SELECT 1 FROM check_placement_regions r
                       WHERE r.check_id = c.id AND r.region = ?
                   )
             ORDER BY c.id",
        )?;
        let rows = stmt.query_map(duckdb::params![region], |r| r.get::<_, CheckId>(0))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Create or replace a check by id — the immutable identity, never the
    /// mutable `name`, so an edit that only renames a check still updates the
    /// same row instead of leaving a stale one behind under the old name.
    ///
    /// `config` is the definition; the other columns are a projection of it so
    /// SQL can order and filter without parsing every document.
    pub fn upsert_check(&self, c: &CheckConfig, now_millis: i64) -> Result<()> {
        self.transaction(|store| {
            store.upsert_check_uncommitted(c, now_millis)?;
            store.sync_config_watermark(ConfigKind::Check, &c.id.to_string())
        })
    }

    pub(super) fn upsert_check_uncommitted(&self, c: &CheckConfig, now_millis: i64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO checks
                (id, name, kind, target, interval_secs, timeout_secs, group_name,
                 latency_graph, enabled, expect_status, expect_body_contains,
                 visible, eval, position, agents, config, updated_at, updated_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
             ON CONFLICT (id) DO UPDATE SET
                 name = excluded.name, kind = excluded.kind, target = excluded.target,
                 interval_secs = excluded.interval_secs, timeout_secs = excluded.timeout_secs,
                 group_name = excluded.group_name, latency_graph = excluded.latency_graph,
                 enabled = excluded.enabled, expect_status = excluded.expect_status,
                 expect_body_contains = excluded.expect_body_contains,
                 visible = excluded.visible, eval = excluded.eval, position = excluded.position,
                 agents = excluded.agents, config = excluded.config,
                 updated_at = excluded.updated_at, updated_by = excluded.updated_by",
            duckdb::params![
                c.id,
                c.name,
                c.kind.as_db(),
                c.target,
                // Legacy column, kept populated so an older binary rolled back
                // onto this database still finds a usable interval.
                cron_to_seconds(&c.cron) as i64,
                c.timeout_secs as i64,
                c.group,
                c.latency_graph,
                c.enabled,
                c.expect.status.map(|s| s as i32),
                c.expect.body_contains,
                c.visible,
                c.eval,
                c.position,
                serde_json::to_string(&c.agents).unwrap_or_else(|_| "[]".into()),
                serde_json::to_string(c)?,
                now_millis,
            ],
        )?;
        self.project_placement_regions(c.id, &c.placement.regions)?;
        Ok(())
    }

    /// Rewrites `check_placement_regions` for one check from its placement's
    /// `regions` list: regions no longer named are deleted and the rest are
    /// inserted idempotently, so no key is deleted and re-inserted within one
    /// transaction (a DuckDB ART-index fault pattern). An empty list leaves no rows behind, matching
    /// `Placement`'s "empty means every region" rule so a reader never has to
    /// special-case "no rows yet" against "explicitly no regions".
    pub(super) fn project_placement_regions(
        &self,
        check_id: CheckId,
        regions: &[String],
    ) -> Result<()> {
        use std::collections::BTreeSet;
        let wanted: BTreeSet<&str> = regions.iter().map(String::as_str).collect();
        let existing: BTreeSet<String> = self
            .conn
            .prepare("SELECT region FROM check_placement_regions WHERE check_id = ?")?
            .query_map(duckdb::params![check_id], |row| row.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        for stale in existing.iter().filter(|r| !wanted.contains(r.as_str())) {
            self.conn.execute(
                "DELETE FROM check_placement_regions WHERE check_id = ? AND region = ?",
                duckdb::params![check_id, stale],
            )?;
        }
        for added in wanted.into_iter().filter(|r| !existing.contains(*r)) {
            self.conn.execute(
                "INSERT INTO check_placement_regions (check_id, region) VALUES (?, ?)",
                duckdb::params![check_id, added],
            )?;
        }
        Ok(())
    }

    /// Position a new check after everything else, so creating one never
    /// silently reshuffles the status page.
    pub fn next_check_position(&self) -> Result<i32> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM checks",
            [],
            |r| r.get(0),
        )?)
    }

    /// Apply an explicit display order, by id. Ids not listed keep their
    /// relative order after the listed ones.
    pub fn reorder_checks(&self, order: &[CheckId], now_millis: i64) -> Result<()> {
        self.transaction(|store| {
            // Rewrite whole rows rather than UPDATE-ing the position column: the
            // document is authoritative, so touching only the column would reorder
            // the index and leave every reader seeing the old order.
            let mut ranks = std::collections::HashMap::new();
            for (rank, id) in order.iter().copied().enumerate() {
                ranks.entry(id).or_insert(rank);
            }
            let mut checks = store.list_checks()?;
            checks.sort_by(|a, b| {
                ranks
                    .get(&a.id)
                    .unwrap_or(&usize::MAX)
                    .cmp(ranks.get(&b.id).unwrap_or(&usize::MAX))
                    .then_with(|| a.position.cmp(&b.position))
                    .then_with(|| a.name.cmp(&b.name))
            });
            for (position, mut check) in checks.into_iter().enumerate() {
                let position = position as i32;
                if check.position != position {
                    check.position = position;
                    store.upsert_check(&check, now_millis)?;
                }
            }
            Ok(())
        })
    }

    /// Delete a check's recorded results and alerts, keeping its definition.
    /// Returns how many result rows went, so the UI can say what it did.
    pub fn purge_check_data(&self, check_id: CheckId) -> Result<u64> {
        self.transaction(|store| {
            let rows = store.conn.execute(
                "DELETE FROM check_results WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            store.conn.execute(
                "DELETE FROM result_replication_watermarks WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            store.conn.execute(
                "DELETE FROM latest_check_results WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            store.conn.execute(
                "DELETE FROM alerts WHERE node_id = ?",
                duckdb::params![check_id.to_string()],
            )?;
            store.rebuild_stream_watermarks()?;
            Ok(rows as u64)
        })
    }

    /// Remove a check definition and the state derived from its local placement,
    /// without touching raw results or the config tombstone stream. A later
    /// placement with the same immutable id therefore reconnects its history.
    pub fn unplace_check_local(&self, check_id: CheckId) -> Result<bool> {
        self.transaction(|store| {
            let removed = store
                .conn
                .execute("DELETE FROM checks WHERE id = ?", duckdb::params![check_id])?;
            if removed == 0 {
                return Ok(false);
            }
            store.conn.execute(
                "DELETE FROM current_state WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            store.conn.execute(
                "DELETE FROM agent_check_state WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            store.conn.execute(
                "DELETE FROM check_placement_regions WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            store.sync_config_watermark(ConfigKind::Check, &check_id.to_string())?;
            Ok(true)
        })
    }

    /// Returns whether a row was actually removed, so the handler can 404
    /// instead of reporting success for an id that never existed.
    pub fn delete_check(&self, check_id: CheckId) -> Result<bool> {
        self.transaction(|store| {
            let n = store
                .conn
                .execute("DELETE FROM checks WHERE id = ?", duckdb::params![check_id])?;
            store.conn.execute(
                "DELETE FROM check_placement_regions WHERE check_id = ?",
                duckdb::params![check_id],
            )?;
            if n > 0 {
                store.sync_config_watermark(ConfigKind::Check, &check_id.to_string())?;
            }
            Ok(n > 0)
        })
    }

    /// Sections in display order.
    pub fn list_sections(&self) -> Result<Vec<Section>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, position, collapsed, display FROM sections ORDER BY position, name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Section {
                name: row.get(0)?,
                position: row.get::<_, i32>(1)?,
                collapsed: row.get::<_, Option<bool>>(2)?.unwrap_or(false),
                display: row
                    .get::<_, Option<String>>(3)?
                    .as_deref()
                    .map_or(SectionDisplay::Inline, SectionDisplay::from_db),
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Create a section at the end. Returns false when the name is taken.
    pub fn create_section(
        &self,
        name: &str,
        collapsed: bool,
        display: SectionDisplay,
    ) -> Result<bool> {
        self.transaction(|store| {
            let exists: i64 = store.conn.query_row(
                "SELECT COUNT(*) FROM sections WHERE name = ?",
                duckdb::params![name],
                |r| r.get(0),
            )?;
            if exists > 0 {
                return Ok(false);
            }
            let next: i32 = store.conn.query_row(
                "SELECT COALESCE(MAX(position), -1) + 1 FROM sections",
                [],
                |r| r.get(0),
            )?;
            store.conn.execute(
                "INSERT INTO sections (name, position, collapsed, display) VALUES (?, ?, ?, ?)",
                duckdb::params![name, next, collapsed, display.as_str()],
            )?;
            store.sync_config_watermark(ConfigKind::Section, name)?;
            Ok(true)
        })
    }

    /// Rename a section, carrying its checks with it. Without the second update
    /// every check in the section would silently become loose.
    pub fn rename_section(&self, from: &str, to: &str, now_millis: i64) -> Result<bool> {
        self.transaction(|store| {
            let n = store.conn.execute(
                "UPDATE sections SET name = ? WHERE name = ?",
                duckdb::params![to, from],
            )?;
            if n == 0 {
                return Ok(false);
            }
            store.sync_config_watermark(ConfigKind::Section, from)?;
            store.sync_config_watermark(ConfigKind::Section, to)?;
            // Each affected check is rewritten whole: its group lives in the
            // document, so updating only the column would leave readers on the old
            // name and the check apparently orphaned.
            for mut check in store.list_checks()? {
                if check.group.as_deref() == Some(from) {
                    check.group = Some(to.to_string());
                    store.upsert_check(&check, now_millis)?;
                }
            }
            Ok(true)
        })
    }

    /// Set whether a section starts collapsed on the public page.
    pub fn set_section_collapsed(&self, name: &str, collapsed: bool) -> Result<bool> {
        self.transaction(|store| {
            let n = store.conn.execute(
                "UPDATE sections SET collapsed = ? WHERE name = ?",
                duckdb::params![collapsed, name],
            )?;
            if n > 0 {
                store.sync_config_watermark(ConfigKind::Section, name)?;
            }
            Ok(n > 0)
        })
    }

    /// Set whether the public page renders a section inline or as its own page.
    pub fn set_section_display(&self, name: &str, display: SectionDisplay) -> Result<bool> {
        self.transaction(|store| {
            let n = store.conn.execute(
                "UPDATE sections SET display = ? WHERE name = ?",
                duckdb::params![display.as_str(), name],
            )?;
            if n > 0 {
                store.sync_config_watermark(ConfigKind::Section, name)?;
            }
            Ok(n > 0)
        })
    }

    /// Delete a section. Its checks are kept and become loose — deleting a
    /// heading must never delete the monitoring underneath it.
    pub fn delete_section(&self, name: &str, now_millis: i64) -> Result<bool> {
        self.transaction(|store| {
            let n = store
                .conn
                .execute("DELETE FROM sections WHERE name = ?", duckdb::params![name])?;
            if n == 0 {
                return Ok(false);
            }
            store.sync_config_watermark(ConfigKind::Section, name)?;
            for mut check in store.list_checks()? {
                if check.group.as_deref() == Some(name) {
                    check.group = None;
                    store.upsert_check(&check, now_millis)?;
                }
            }
            Ok(true)
        })
    }

    /// Apply an explicit order. Names not listed keep their relative order after
    /// the listed ones, so a stale client can't silently drop a section.
    pub fn reorder_sections(&self, order: &[String]) -> Result<()> {
        self.transaction(|store| {
            let listed: std::collections::HashSet<&str> =
                order.iter().map(String::as_str).collect();
            let mut position = 0i32;
            for name in order {
                let changed = store.conn.execute(
                    "UPDATE sections SET position = ? WHERE name = ? AND position <> ?",
                    duckdb::params![position, name, position],
                )?;
                if changed > 0 {
                    store.sync_config_watermark(ConfigKind::Section, name)?;
                }
                position += 1;
            }
            let remaining: Vec<String> = store
                .list_sections()?
                .into_iter()
                .map(|section| section.name)
                .filter(|name| !listed.contains(name.as_str()))
                .collect();
            for name in remaining {
                let changed = store.conn.execute(
                    "UPDATE sections SET position = ? WHERE name = ? AND position <> ?",
                    duckdb::params![position, name, position],
                )?;
                if changed > 0 {
                    store.sync_config_watermark(ConfigKind::Section, &name)?;
                }
                position += 1;
            }
            Ok(())
        })
    }

    /// Adopt the section names already present on checks, so a deployment that
    /// predates the sections table doesn't start with an empty list.
    pub fn seed_sections_from_checks(&self) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT group_name FROM checks
             WHERE group_name IS NOT NULL
               AND group_name NOT IN (SELECT name FROM sections)
             ORDER BY group_name",
        )?;
        let names: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<_, _>>()?;
        for name in names {
            self.create_section(&name, false, SectionDisplay::Inline)?;
        }
        Ok(())
    }

    // ---- Rollup policy ----

    /// Every stored policy. Nodes without a row use their level's default, so
    /// this is usually short and is cached by the caller.
    pub fn list_rollup_policies(&self) -> Result<Vec<RollupPolicyRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT node_id, node_type, degraded_when, down_when, config, updated_at, updated_by
             FROM rollup_policy ORDER BY node_id",
        )?;
        let rows = stmt
            .query_map([], Self::rollup_policy_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub(super) fn rollup_policy_row(row: &duckdb::Row) -> duckdb::Result<RollupPolicyRow> {
        let config: Option<String> = row.get(4)?;
        let quorums: serde_json::Value = config
            .as_deref()
            .and_then(|c| serde_json::from_str(c).ok())
            .unwrap_or(serde_json::Value::Null);
        let q = |key: &str| quorums.get(key).and_then(|v| v.as_u64()).map(|v| v as u32);
        Ok(RollupPolicyRow {
            node_id: row.get::<_, String>(0)?,
            node_type: NodeType::from_db(&row.get::<_, String>(1)?),
            policy: crate::rollup::RollupPolicy {
                degraded_when: crate::rollup::Threshold::from_db(
                    &row.get::<_, String>(2)?,
                    q("degraded_q"),
                ),
                down_when: crate::rollup::Threshold::from_db(
                    &row.get::<_, String>(3)?,
                    q("down_q"),
                ),
            },
            updated_at: row.get::<_, i64>(5)?,
            updated_by: row.get::<_, Option<String>>(6)?,
        })
    }

    /// Write a node's policy, last-writer-wins on the HLC like every other
    /// replicated config row.
    pub fn upsert_rollup_policy(
        &self,
        node_id: &str,
        node_type: NodeType,
        policy: &crate::rollup::RollupPolicy,
        hlc: &Hlc,
    ) -> Result<()> {
        self.transaction(|store| {
            store.upsert_rollup_policy_uncommitted(node_id, node_type, policy, hlc)?;
            store.sync_config_watermark(ConfigKind::RollupPolicy, node_id)
        })
    }

    pub(super) fn upsert_rollup_policy_uncommitted(
        &self,
        node_id: &str,
        node_type: NodeType,
        policy: &crate::rollup::RollupPolicy,
        hlc: &Hlc,
    ) -> Result<()> {
        use crate::rollup::Threshold;
        let mut config = serde_json::Map::new();
        if let Threshold::Quorum(q) = policy.degraded_when {
            config.insert("degraded_q".into(), q.into());
        }
        if let Threshold::Quorum(q) = policy.down_when {
            config.insert("down_q".into(), q.into());
        }
        let config = if config.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(config).to_string())
        };
        self.conn.execute(
            "INSERT INTO rollup_policy
                 (node_id, node_type, degraded_when, down_when, config, updated_at, updated_by)
              VALUES (?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT (node_id) DO UPDATE SET
                  node_type = excluded.node_type, degraded_when = excluded.degraded_when,
                  down_when = excluded.down_when, config = excluded.config,
                  updated_at = excluded.updated_at, updated_by = excluded.updated_by",
            duckdb::params![
                node_id,
                node_type.as_str(),
                policy.degraded_when.as_str(),
                policy.down_when.as_str(),
                config,
                hlc.millis,
                hlc.node_id,
            ],
        )?;
        Ok(())
    }

    /// Drop a node's policy, returning it to its level's default.
    pub fn delete_rollup_policy(&self, node_id: &str) -> Result<bool> {
        self.transaction(|store| {
            let n = store.conn.execute(
                "DELETE FROM rollup_policy WHERE node_id = ?",
                duckdb::params![node_id],
            )?;
            if n > 0 {
                store.sync_config_watermark(ConfigKind::RollupPolicy, node_id)?;
            }
            Ok(n > 0)
        })
    }

    /// Erase everything a check ever produced: its results, its alerts, every
    /// incident it opened, and its rows inside incidents another check owns.
    ///
    /// Deliberately separate from `purge_check_data`, which keeps the incident
    /// record — "reset the graph" and "this never happened" are different
    /// intentions, and only one of them removes a page visitors have read.
    /// Incidents go through `delete_incident` one at a time rather than a bulk
    /// DELETE, so each leaves a tombstone and the erase survives replication.
    pub fn purge_check_everything(
        &self,
        check_id: CheckId,
        now_millis: i64,
        node_id: &str,
    ) -> Result<PurgeSummary> {
        let results = self.purge_check_data(check_id)?;
        let owned: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT id FROM incidents WHERE origin_check = ?")?;
            stmt.query_map(duckdb::params![check_id], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let mut incidents = 0u64;
        for id in &owned {
            if self.delete_incident(id, now_millis, node_id)? {
                incidents += 1;
            }
        }
        // Incidents this check merely joined belong to whoever opened them, so
        // they survive; only this check's membership of them goes. Route each
        // removal through the normal mutator so its replication version moves.
        let memberships: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT incident_id FROM incident_checks WHERE check_id = ?")?;
            stmt.query_map(duckdb::params![check_id], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        for incident_id in memberships {
            self.detach_check(&incident_id, check_id, None)?;
        }
        Ok(PurgeSummary {
            results,
            incidents,
            deleted_incidents: owned,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::storage::test_support::*;

    #[test]
    fn fresh_schema_marker_never_rewrites_new_random_check_ids() {
        let path = std::env::temp_dir().join(format!(
            "lighthouse-storage-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path = path.to_string_lossy().into_owned();
        let id = CheckId::now_v7();
        let check = CheckConfig {
            id,
            name: "new-check".into(),
            target: "https://example.test".into(),
            ..Default::default()
        };
        Storage::open(&path)
            .unwrap()
            .upsert_check(&check, 1)
            .unwrap();
        let reopened = Storage::open(&path).unwrap();
        assert_eq!(reopened.list_checks().unwrap()[0].id, id);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn check_ids_for_region_admits_unplaced_and_named_regions_only() {
        let s = Storage::open(":memory:").unwrap();
        let everywhere = placed_check("everywhere", &[]);
        let us_only = placed_check("us-only", &["us"]);
        let tw_only = placed_check("tw-only", &["tw"]);
        for c in [&everywhere, &us_only, &tw_only] {
            s.upsert_check(c, 1).unwrap();
        }

        let us_ids = s.check_ids_for_region("us").unwrap();
        assert_eq!(
            us_ids,
            {
                let mut v = vec![everywhere.id, us_only.id];
                v.sort();
                v
            },
            "us gets the unscoped check and the one explicitly placed there"
        );

        let tw_ids = s.check_ids_for_region("tw").unwrap();
        assert_eq!(
            tw_ids,
            {
                let mut v = vec![everywhere.id, tw_only.id];
                v.sort();
                v
            },
            "tw must not see the us-only check"
        );
    }

    /// Same-key rewrites inside one transaction are the shape that used to
    /// be delete-then-insert; as upserts they must leave exactly the last
    /// definition, renamed and re-placed, with nothing stale behind.
    #[test]
    fn repeated_same_key_upserts_in_one_transaction_keep_the_last_definition() {
        let s = Storage::open(":memory:").unwrap();
        let mut check = placed_check("first", &["us", "eu"]);
        s.transaction(|store| {
            for (n, regions) in [&["us", "eu"][..], &["eu", "tw"], &["tw"]]
                .iter()
                .enumerate()
            {
                check.name = format!("name-{n}");
                check.placement.regions = regions.iter().map(|r| r.to_string()).collect();
                store.upsert_check(&check, n as i64 + 1)?;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(s.list_checks().unwrap(), vec![check.clone()]);
        assert!(s.check_ids_for_region("us").unwrap().is_empty());
        assert!(s.check_ids_for_region("eu").unwrap().is_empty());
        assert_eq!(s.check_ids_for_region("tw").unwrap(), vec![check.id]);
    }

    #[test]
    fn upsert_check_rewrites_the_placement_projection_on_every_edit() {
        let s = Storage::open(":memory:").unwrap();
        let mut check = placed_check("moving", &["us"]);
        s.upsert_check(&check, 1).unwrap();
        assert_eq!(s.check_ids_for_region("us").unwrap(), vec![check.id]);
        assert!(s.check_ids_for_region("tw").unwrap().is_empty());

        // Re-placed to tw only: the old row in `check_placement_regions` must
        // not linger and leave the check reachable from both regions.
        check.placement.regions = vec!["tw".into()];
        s.upsert_check(&check, 2).unwrap();
        assert!(s.check_ids_for_region("us").unwrap().is_empty());
        assert_eq!(s.check_ids_for_region("tw").unwrap(), vec![check.id]);
    }

    #[test]
    fn unplace_and_delete_clear_the_placement_projection() {
        let s = Storage::open(":memory:").unwrap();
        let a = placed_check("a", &["us"]);
        let b = placed_check("b", &["us"]);
        s.upsert_check(&a, 1).unwrap();
        s.upsert_check(&b, 1).unwrap();
        assert_eq!(s.check_ids_for_region("us").unwrap().len(), 2);

        assert!(s.unplace_check_local(a.id).unwrap());
        assert_eq!(
            s.check_ids_for_region("us").unwrap(),
            vec![b.id],
            "unplace must drop the projection row, or a later differently-\
             placed check reusing no id could still misreport membership"
        );

        assert!(s.delete_check(b.id).unwrap());
        assert!(s.check_ids_for_region("us").unwrap().is_empty());
    }

    /// A replicated tombstone applies through a different code path
    /// (`apply_config_entries`'s prepared per-kind statement) than
    /// `unplace_check_local`/`delete_check`, so it needs its own assertion
    /// that the projection is still cleaned up rather than left to rot.
    #[test]
    fn a_replicated_check_tombstone_clears_the_placement_projection() {
        let s = Storage::open(":memory:").unwrap();
        let check = placed_check("doomed", &["us"]);
        s.upsert_check(&check, 1).unwrap();
        assert_eq!(s.check_ids_for_region("us").unwrap(), vec![check.id]);

        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Check,
            id: check.id.to_string(),
            hlc: Hlc::new(2, "peer"),
            document: None,
        })
        .unwrap();
        assert!(s.check_ids_for_region("us").unwrap().is_empty());
    }

    #[test]
    fn erasing_a_check_leaves_other_checks_incidents_intact() {
        let s = Storage::open(":memory:").unwrap();
        seeded_incident(&s, "inc-web", "web");
        seeded_incident(&s, "inc-api", "api");
        // `web` merely joined the api incident, behind a dependency.
        s.attach_check("inc-api", cid("web"), AffectSource::Dependency)
            .unwrap();
        s.insert_results(&[(result("web", 1000, 5), Status::Up)])
            .unwrap();

        let summary = s
            .purge_check_everything(cid("web"), 4000, "node-a")
            .unwrap();
        assert_eq!(summary.results, 1);
        assert_eq!(summary.incidents, 1);
        assert_eq!(summary.deleted_incidents, vec!["inc-web".to_string()]);

        assert!(s.incident("inc-web").unwrap().is_none());
        let api = s.incident("inc-api").unwrap().expect("api's own incident");
        assert_eq!(
            api.checks.len(),
            1,
            "the erased check left the incident it had joined, and nothing else did"
        );
        assert_eq!(api.checks[0].check_id, cid("api"));
    }

    #[test]
    fn section_display_mode_persists_across_writes() {
        let store = Storage::open(":memory:").unwrap();
        store
            .create_section("edge", false, SectionDisplay::Page)
            .unwrap();
        store
            .create_section("core", true, SectionDisplay::Inline)
            .unwrap();

        let mode = |store: &Storage, name: &str| {
            store
                .list_sections()
                .unwrap()
                .into_iter()
                .find(|s| s.name == name)
                .map(|s| s.display)
        };
        assert_eq!(mode(&store, "edge"), Some(SectionDisplay::Page));
        assert_eq!(mode(&store, "core"), Some(SectionDisplay::Inline));

        store
            .set_section_display("core", SectionDisplay::Page)
            .unwrap();
        assert_eq!(mode(&store, "core"), Some(SectionDisplay::Page));

        // A rename carries the mode with the section. Losing it here would flip
        // a paged section back to inline as a side effect of an edit that was
        // only ever about its name, re-expanding the thing the operator paged
        // precisely because it was too big to render.
        store.rename_section("core", "core-eu", 0).unwrap();
        assert_eq!(mode(&store, "core-eu"), Some(SectionDisplay::Page));

        // `collapsed` and `display` are independent: a paged section's stored
        // `collapsed` is meaningless but must not be destroyed, so toggling one
        // never rewrites the other.
        store.set_section_collapsed("core-eu", false).unwrap();
        assert_eq!(mode(&store, "core-eu"), Some(SectionDisplay::Page));
    }

    #[test]
    fn document_encodings_are_pinned() {
        let section = Section {
            name: "core".into(),
            position: 2,
            collapsed: true,
            display: SectionDisplay::Page,
        };
        assert_eq!(
            serde_json::to_string(&section).unwrap(),
            r#"{"name":"core","position":2,"collapsed":true,"display":"page"}"#
        );
        // The compatibility that matters in both directions. A document from a
        // peer that predates the mode has no `display` and must still parse,
        // reading as inline; a peer that predates it parses a document that has
        // one by ignoring the field, which is why it was added rather than
        // replacing `collapsed`.
        let legacy: Section =
            serde_json::from_str(r#"{"name":"core","position":2,"collapsed":true}"#).unwrap();
        assert_eq!(legacy.display, SectionDisplay::Inline);

        let value = "dark".to_string();
        assert_eq!(serde_json::to_string(&value).unwrap(), r#""dark""#);

        // The check document is `CheckConfig` verbatim — `checks.config` is
        // already authoritative — so the contract pinned here is the round
        // trip, not a hand-copied field list config.rs already owns.
        let check = check_config("web", "https://example.com");
        let doc = serde_json::to_string(&check).unwrap();
        let round: CheckConfig = serde_json::from_str(&doc).unwrap();
        assert_eq!(round, check);
    }
}
