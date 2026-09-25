use super::*;

/// One check's contribution to a results digest: what a peer needs to know to
/// decide whether it is missing anything for this check, without shipping the
/// rows themselves.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckDigest {
    pub check_id: CheckId,
    /// Oldest raw observation still retained for this check. This is maintained
    /// alongside the replication high-water mark, so digesting it never scans
    /// raw history.
    pub min_ts: i64,
    pub max_ts: i64,
    pub count: i64,
}

/// Which config table an entry belongs to. `kind` on `ConfigEntry` and
/// `config_tombstones`, and the discriminant a digest groups by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigKind {
    Check,
    Section,
    Setting,
    Credential,
    RollupPolicy,
}

impl ConfigKind {
    /// Every kind, for the callers that must sweep all of them — an untagged
    /// `GET /internal/config`, and config anti-entropy. Adding a variant above
    /// without adding it here would silently stop replicating that table, so
    /// the exhaustive match in `as_str` is the reminder to come back.
    pub const ALL: [ConfigKind; 5] = [
        ConfigKind::Check,
        ConfigKind::Section,
        ConfigKind::Setting,
        ConfigKind::Credential,
        ConfigKind::RollupPolicy,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigKind::Check => "check",
            ConfigKind::Section => "section",
            ConfigKind::Setting => "setting",
            ConfigKind::Credential => "credential",
            ConfigKind::RollupPolicy => "rollup_policy",
        }
    }

    /// `None` on anything unrecognised, unlike the other `from_db`s here: a
    /// config kind this binary doesn't know about must not be silently folded
    /// into one it does, or a tombstone could be applied against the wrong
    /// table.
    pub fn from_db(s: &str) -> Option<Self> {
        match s {
            "check" => Some(ConfigKind::Check),
            "section" => Some(ConfigKind::Section),
            "setting" => Some(ConfigKind::Setting),
            "credential" => Some(ConfigKind::Credential),
            "rollup_policy" => Some(ConfigKind::RollupPolicy),
            _ => None,
        }
    }

    /// The table this kind's rows live in, and the column that is its primary
    /// key — the one place that mapping is written, so every reader and
    /// writer of a config row agrees on it.
    fn table_and_pk(&self) -> (&'static str, &'static str) {
        match self {
            ConfigKind::Check => ("checks", "id"),
            ConfigKind::Section => ("sections", "name"),
            ConfigKind::Setting => ("settings", "key"),
            ConfigKind::Credential => ("agent_credentials", "id"),
            ConfigKind::RollupPolicy => ("rollup_policy", "node_id"),
        }
    }
}

/// A row's contribution to a digest: enough to tell a peer whether it is
/// behind, without shipping every document. `max_hlc` covers rows and
/// tombstones together, since a peer missing a delete is just as behind as
/// one missing an edit.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigDigest {
    pub kind: ConfigKind,
    pub max_hlc: Option<Hlc>,
    pub count: i64,
}

/// A fixed-cost summary of everything replicated, for a peer to compare
/// against its own and ask only for what differs. `window_start` bounds the
/// results digest so it stays cheap against a table that can hold millions of
/// rows; alerts and incidents are windowed the same way for the same reason.
#[derive(Debug, Clone, PartialEq)]
pub struct Digest {
    pub checks: Vec<CheckDigest>,
    pub window_start: i64,
    pub alerts: StreamDigest,
    pub incidents: StreamDigest,
    pub incident_tombstones: StreamDigest,
    pub latency_minute_rollups: RollupDigest,
    pub latency_hour_rollups: RollupDigest,
    pub config: Vec<ConfigDigest>,
}

/// One replicated config row, on the wire and as the unit `apply_config_entry`
/// decides about. `document` is the whole row as JSON — never a hand-picked
/// subset of fields, so a column this binary doesn't know about yet still
/// round-trips through a node that does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub kind: ConfigKind,
    /// The row's primary key: check name, section name, setting key,
    /// credential id, or rollup node id.
    pub id: String,
    pub hlc: Hlc,
    /// The whole row as a JSON document. `None` iff this is a tombstone.
    pub document: Option<String>,
}

impl ConfigEntry {
    pub fn is_tombstone(&self) -> bool {
        self.document.is_none()
    }
}

/// What a batch of config entries is arbitrated against: the newest stamp each
/// `(kind, id)` currently carries here, and which of them carry a tombstone at
/// all — a delete that is not there does not need clearing.
#[derive(Default)]
struct ConfigStamps {
    newest: std::collections::HashMap<(ConfigKind, String), Hlc>,
    tombstoned: std::collections::HashSet<(ConfigKind, String)>,
}

impl Storage {
    /// A settings document, or `None` when the key has never been written.
    /// `None` is what makes the config file a seed rather than a default that
    /// silently overwrites an admin's edit on every restart.
    pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM settings WHERE key = ?")?;
        let mut rows = stmt.query_map(duckdb::params![key], |r| r.get::<_, String>(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn put_setting(&self, key: &str, value: &str) -> Result<()> {
        // Delete-then-insert, like `upsert_check`: DuckDB's ON CONFLICT wants
        // the column list twice and this table has one row per page.
        self.conn
            .execute("DELETE FROM settings WHERE key = ?", duckdb::params![key])?;
        self.conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            duckdb::params![key, value],
        )?;
        Ok(())
    }

    // ---- Anti-entropy ----

    /// A fixed-cost summary of everything replicated, for a peer to compare
    /// against its own. Full retained-history digests read compact metadata
    /// maintained on write; explicitly windowed digests scan only that window.
    pub fn digest(&self, window_start: i64) -> Result<Digest> {
        let checks = {
            let (sql, params): (&str, Vec<i64>) = if window_start == 0 {
                (
                    "SELECT check_id, min_ts, max_ts, result_count
                      FROM result_replication_watermarks
                     ORDER BY check_id",
                    Vec::new(),
                )
            } else {
                (
                    "SELECT check_id, MIN(ts_millis), MAX(ts_millis), COUNT(*) FROM check_results
                      WHERE ts_millis >= ? GROUP BY check_id ORDER BY check_id",
                    vec![window_start],
                )
            };
            let mut stmt = self.conn.prepare(sql)?;
            let rows = stmt.query_map(duckdb::params_from_iter(params), |row| {
                Ok(CheckDigest {
                    check_id: row.get(0)?,
                    min_ts: row.get(1)?,
                    max_ts: row.get(2)?,
                    count: row.get(3)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let alerts = self.stream_digest("alerts")?;
        let incidents = self.stream_digest("incidents")?;
        let incident_tombstones = self.stream_digest("incident_tombstones")?;
        let latency_minute_rollups = self.rollup_digest("check_latency_minute_rollups")?;
        let latency_hour_rollups = self.rollup_digest("check_latency_hour_rollups")?;
        let config = self.config_digest()?;
        Ok(Digest {
            checks,
            window_start,
            alerts,
            incidents,
            incident_tombstones,
            latency_minute_rollups,
            latency_hour_rollups,
            config,
        })
    }

    /// Every config kind's digest, with no results/alerts/rollup scan riding
    /// along — what `digest()` computes for its own `config` field, exposed on
    /// its own for a store that holds only the config plane. The global tier
    /// (`global::store::GlobalStore`) is the reason this exists: it has no
    /// results to digest and would otherwise have to call the much more
    /// expensive `digest()` and throw away everything but this part.
    pub fn config_digest(&self) -> Result<Vec<ConfigDigest>> {
        ConfigKind::ALL
            .into_iter()
            .map(|k| self.config_digest_for(k))
            .collect()
    }

    /// One kind's contribution to a digest: how many entries it has, and the
    /// highest stamp among them. Rows and tombstones are unioned into the same
    /// scan, because a peer missing a delete is exactly as behind as one
    /// missing an edit — the digest must not be able to look "caught up" while
    /// still missing a tombstone.
    pub(super) fn config_digest_for(&self, kind: ConfigKind) -> Result<ConfigDigest> {
        let (table, _) = kind.table_and_pk();
        let sql = format!(
            "SELECT millis, node FROM (
                 SELECT updated_at AS millis, updated_by AS node FROM {table}
                 UNION ALL
                 SELECT deleted_at AS millis, node_id AS node FROM config_tombstones
                 WHERE kind = ?
             )"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(duckdb::params![kind.as_str()], |r| {
                Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<String>>(1)?))
            })?
            .collect::<duckdb::Result<Vec<_>>>()?;
        Ok(Self::digest_from_stamp_rows(kind, rows))
    }

    /// `region`'s placement-scoped contribution to a Check-kind digest: the
    /// same shape `config_digest_for` produces for the whole table, but the
    /// live-row half is restricted to checks `region`'s placement admits (via
    /// `check_placement_regions`, never a `config` JSON parse). Tombstones
    /// stay unscoped — an outright delete propagates to every region
    /// regardless of placement, since nothing here tracks a check's *past*
    /// placement to scope a tombstone against.
    ///
    /// This is what makes Phase 2 reconciliation subset-correct instead of
    /// amplifying: comparing a *whole-catalogue* check digest against a
    /// region that legitimately holds only its placed subset would never
    /// converge, and would re-page the full catalogue every sweep — the
    /// exact cost this project has already removed twice elsewhere (see
    /// `STREAM_RESCAN_INTERVAL` and the replicated-dedup high-water mark).
    /// A region that genuinely holds what its placement calls for produces a
    /// digest that matches this one, so a converged region costs one HTTP
    /// digest comparison per sweep and nothing else.
    pub fn config_digest_for_region(&self, region: &str) -> Result<ConfigDigest> {
        let sql = "
            SELECT millis, node FROM (
                SELECT c.updated_at AS millis, c.updated_by AS node
                FROM checks c
                WHERE NOT EXISTS (
                          SELECT 1 FROM check_placement_regions r WHERE r.check_id = c.id
                      )
                   OR EXISTS (
                          SELECT 1 FROM check_placement_regions r
                          WHERE r.check_id = c.id AND r.region = ?
                      )
                UNION ALL
                SELECT deleted_at AS millis, node_id AS node
                FROM config_tombstones WHERE kind = 'check'
            )";
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt
            .query_map(duckdb::params![region], |r| {
                Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<String>>(1)?))
            })?
            .collect::<duckdb::Result<Vec<_>>>()?;
        Ok(Self::digest_from_stamp_rows(ConfigKind::Check, rows))
    }

    /// Folds `(millis, node)` stamp pairs — a row or tombstone's contribution
    /// — into the count/max-stamp shape every digest query above produces.
    /// Shared so the two queries above cannot silently compute "caught up"
    /// differently from one another.
    pub(super) fn digest_from_stamp_rows(
        kind: ConfigKind,
        rows: Vec<(Option<i64>, Option<String>)>,
    ) -> ConfigDigest {
        let mut count = 0i64;
        let mut max_hlc: Option<Hlc> = None;
        for (millis, node) in rows {
            count += 1;
            // NULL here is a row written before stamps existed: the oldest
            // possible HLC, so it never wins a max() against a real one.
            let hlc = Hlc::new(millis.unwrap_or(0), node.unwrap_or_default());
            max_hlc = Some(match max_hlc {
                Some(m) if m >= hlc => m,
                _ => hlc,
            });
        }
        ConfigDigest {
            kind,
            max_hlc,
            count,
        }
    }

    // ---- Config replication ----

    /// The stamps a batch of entries is arbitrated against, loaded in one query
    /// per kind rather than two per entry. A bulk config push — provisioning
    /// ten thousand checks, or a peer replaying its whole config — is otherwise
    /// thousands of point lookups on the one database worker, and every read
    /// and write on this node queues behind them.
    fn local_config_stamps(&self, entries: &[ConfigEntry]) -> Result<ConfigStamps> {
        let mut stamps = ConfigStamps::default();
        let mut by_kind: std::collections::HashMap<ConfigKind, Vec<&str>> =
            std::collections::HashMap::new();
        for entry in entries {
            by_kind.entry(entry.kind).or_default().push(&entry.id);
        }
        for (kind, ids) in by_kind {
            let (table, pk) = kind.table_and_pk();
            let placeholders = vec!["?"; ids.len()].join(", ");
            let params: Vec<&dyn duckdb::types::ToSql> = ids
                .iter()
                .map(|id| id as &dyn duckdb::types::ToSql)
                .collect();
            let sql = format!(
                "SELECT CAST({pk} AS VARCHAR), updated_at, updated_by
                 FROM {table} WHERE CAST({pk} AS VARCHAR) IN ({placeholders})"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params.as_slice(), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            })?;
            for row in rows {
                let (id, millis, node) = row?;
                // A row whose `updated_at` is NULL was written before stamps
                // existed, or by a path this project hasn't converted yet. It
                // counts as the oldest possible HLC — (0, "") — so anything
                // replicated beats it rather than the merge failing to compare.
                stamps.newest.insert(
                    (kind, id),
                    Hlc::new(millis.unwrap_or(0), node.unwrap_or_default()),
                );
            }

            let sql = format!(
                "SELECT id, deleted_at, node_id FROM config_tombstones
                 WHERE kind = ? AND id IN ({placeholders})"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let kind_str = kind.as_str();
            let mut tomb_params: Vec<&dyn duckdb::types::ToSql> =
                vec![&kind_str as &dyn duckdb::types::ToSql];
            tomb_params.extend(params);
            let rows = stmt.query_map(tomb_params.as_slice(), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    Hlc::new(r.get::<_, i64>(1)?, r.get::<_, String>(2)?),
                ))
            })?;
            for row in rows {
                let (id, hlc) = row?;
                let key = (kind, id);
                stamps.tombstoned.insert(key.clone());
                match stamps.newest.get(&key) {
                    Some(existing) if *existing >= hlc => {}
                    _ => {
                        stamps.newest.insert(key, hlc);
                    }
                }
            }
        }
        Ok(stamps)
    }

    pub(super) fn prepare_config_delete_statements(&self) -> Result<Vec<Statement<'_>>> {
        [
            ConfigKind::Check,
            ConfigKind::Section,
            ConfigKind::Setting,
            ConfigKind::Credential,
            ConfigKind::RollupPolicy,
        ]
        .into_iter()
        .map(|kind| {
            let (table, pk) = kind.table_and_pk();
            self.conn
                .prepare(&format!("DELETE FROM {table} WHERE {pk} = ?"))
        })
        .collect::<duckdb::Result<_>>()
        .map_err(Into::into)
    }

    /// Write a document over the row named `id`, stamped at `hlc`. The only
    /// place any config table is written from a parsed document, so the
    /// encodings live here and nowhere else.
    pub(super) fn apply_config_document(
        &self,
        kind: ConfigKind,
        id: &str,
        doc: &str,
        hlc: &Hlc,
    ) -> Result<()> {
        match kind {
            ConfigKind::Check => {
                // `doc` IS `checks.config` verbatim — the whole `CheckConfig`,
                // authoritative already. `upsert_check` is the one place that
                // projects it into the flat columns SQL filters and orders by,
                // so reusing it is what keeps this from becoming a second,
                // hand-maintained field list that drifts from the first.
                let config: CheckConfig = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed check document for {id}: {e}"))?;
                if config.id.to_string() != id {
                    bail!(
                        "check document id {} does not match config key {id}",
                        config.id
                    );
                }
                self.upsert_check(&config, hlc.millis)?;
                self.conn.execute(
                    "UPDATE checks SET updated_by = ? WHERE id = ?",
                    duckdb::params![hlc.node_id, id],
                )?;
            }
            ConfigKind::Section => {
                let s: Section = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed section document for {id}: {e}"))?;
                self.conn
                    .execute("DELETE FROM sections WHERE name = ?", duckdb::params![id])?;
                self.conn.execute(
                    "INSERT INTO sections (name, position, collapsed, display, updated_at, updated_by)
                     VALUES (?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        s.name,
                        s.position,
                        s.collapsed,
                        s.display.as_str(),
                        hlc.millis,
                        hlc.node_id
                    ],
                )?;
            }
            ConfigKind::Setting => {
                let value: String = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed setting document for {id}: {e}"))?;
                self.conn
                    .execute("DELETE FROM settings WHERE key = ?", duckdb::params![id])?;
                self.conn.execute(
                    "INSERT INTO settings (key, value, updated_at, updated_by)
                     VALUES (?, ?, ?, ?)",
                    duckdb::params![id, value, hlc.millis, hlc.node_id],
                )?;
            }
            ConfigKind::Credential => {
                let c: CredentialDoc = serde_json::from_str(doc)
                    .map_err(|e| anyhow::anyhow!("malformed credential document for {id}: {e}"))?;
                self.conn.execute(
                    "DELETE FROM agent_credentials WHERE id = ?",
                    duckdb::params![id],
                )?;
                self.conn.execute(
                    "INSERT INTO agent_credentials
                         (id, zone, region, secret_hash, salt, created_at, revoked_at,
                          updated_at, updated_by)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        c.id,
                        c.zone,
                        c.region,
                        c.secret_hash,
                        c.salt,
                        c.created_at,
                        c.revoked_at,
                        hlc.millis,
                        hlc.node_id,
                    ],
                )?;
            }
            ConfigKind::RollupPolicy => {
                let doc: RollupPolicyDoc = serde_json::from_str(doc).map_err(|e| {
                    anyhow::anyhow!("malformed rollup policy document for {id}: {e}")
                })?;
                doc.policy
                    .validate()
                    .map_err(|e| anyhow::anyhow!("invalid rollup policy for {id}: {e}"))?;
                self.upsert_rollup_policy(id, doc.node_type, &doc.policy, hlc)?;
            }
        }
        Ok(())
    }

    /// The ONLY place "is this newer?" is decided for replicated config.
    /// Local writes are meant to go through this same function — the caller
    /// stamps a fresh `Hlc` and calls it exactly like a replicated entry
    /// would — so a local edit and one that arrived over the wire share one
    /// code path and can never disagree about what wins.
    ///
    /// Returns whether `e` was applied. `false` means the local stamp was
    /// already greater than or equal to `e.hlc`, so nothing changed —
    /// comparison uses `Hlc`'s derived `Ord`, never re-implemented here.
    pub fn apply_config_entry(&self, e: &ConfigEntry) -> Result<bool> {
        self.apply_config_entries(std::slice::from_ref(e))
            .map(|applied| applied.into_iter().next().unwrap_or(false))
    }

    /// Apply a bounded config batch in one transaction. LWW arbitration remains
    /// per entry, but a crash can never expose only a prefix of accepted writes.
    /// Callers use the returned flags to update only caches whose rows changed.
    pub fn apply_config_entries(&self, entries: &[ConfigEntry]) -> Result<Vec<bool>> {
        self.conn.execute_batch("BEGIN")?;
        let write = || -> Result<Vec<bool>> {
            let mut stamps = self.local_config_stamps(entries)?;
            let mut deletes = self.prepare_config_delete_statements()?;
            let mut tombstone_deletes = self
                .conn
                .prepare("DELETE FROM config_tombstones WHERE kind = ? AND id = ?")?;
            let mut tombstone_inserts = self.conn.prepare(
                "INSERT INTO config_tombstones (kind, id, deleted_at, node_id)
                 VALUES (?, ?, ?, ?)",
            )?;
            entries
                .iter()
                .map(|entry| {
                    self.apply_config_entry_uncommitted(
                        entry,
                        &mut stamps,
                        &mut deletes[entry.kind as usize],
                        &mut tombstone_deletes,
                        &mut tombstone_inserts,
                    )
                })
                .collect()
        };
        match write() {
            Ok(applied) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(applied)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn apply_config_entry_uncommitted(
        &self,
        e: &ConfigEntry,
        stamps: &mut ConfigStamps,
        delete: &mut Statement<'_>,
        tombstone_delete: &mut Statement<'_>,
        tombstone_insert: &mut Statement<'_>,
    ) -> Result<bool> {
        let key = (e.kind, e.id.clone());
        if let Some(local) = stamps.newest.get(&key) {
            if e.hlc <= *local {
                return Ok(false);
            }
        }
        let tombstoned = stamps.tombstoned.contains(&key);
        // The snapshot has to track what this batch itself writes. Two entries
        // for one id in a single batch must still resolve by HLC, exactly as
        // they would arriving separately — otherwise the last one in the vector
        // wins and an older edit can overwrite a newer one.
        stamps.newest.insert(key.clone(), e.hlc.clone());
        if e.is_tombstone() {
            stamps.tombstoned.insert(key.clone());
        } else {
            stamps.tombstoned.remove(&key);
        }
        if e.is_tombstone() {
            if e.kind != ConfigKind::Check {
                delete.execute(duckdb::params![e.id])?;
            } else if let Ok(check_id) = e.id.parse::<CheckId>() {
                delete.execute(duckdb::params![e.id])?;
                // Bypasses `delete_check`, which does this too, because a
                // replicated tombstone applies straight through the prepared
                // per-kind statement above rather than that helper.
                self.conn.execute(
                    "DELETE FROM check_placement_regions WHERE check_id = ?",
                    duckdb::params![check_id],
                )?;
            }
            if tombstoned {
                tombstone_delete.execute(duckdb::params![e.kind.as_str(), e.id])?;
            }
            tombstone_insert.execute(duckdb::params![
                e.kind.as_str(),
                e.id,
                e.hlc.millis,
                e.hlc.node_id
            ])?;
        } else {
            // A malformed document is an error, not a silent skip: the caller
            // logs it, but this must never half-apply a document over a good
            // local row.
            let doc = e.document.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "config entry for {} has no document and is not a tombstone",
                    e.id
                )
            })?;
            self.apply_config_document(e.kind, &e.id, doc, &e.hlc)?;
            // The edit supersedes any tombstone for this id — leaving one
            // behind would make the next digest sweep look inconsistent.
            if tombstoned {
                tombstone_delete.execute(duckdb::params![e.kind.as_str(), e.id])?;
            }
        }
        Ok(true)
    }

    /// Read back the entry a row would replicate as, for pushing to peers
    /// after a local write. `None` when the row does not exist.
    pub fn config_entry(&self, kind: ConfigKind, id: &str) -> Result<Option<ConfigEntry>> {
        let doc: Option<(String, Option<i64>, Option<String>)> = match kind {
            ConfigKind::Check => {
                // A caller (or a peer on an older binary) can name an id that
                // is not a UUID at all. That is not an error here: nothing in
                // this table can match it, which is exactly "no such row".
                let Ok(id) = id.parse::<CheckId>() else {
                    return Ok(None);
                };
                let mut stmt = self
                    .conn
                    .prepare("SELECT config, updated_at, updated_by FROM checks WHERE id = ?")?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                })?;
                rows.next()
                    .transpose()?
                    .and_then(|(doc, at, by)| doc.map(|doc| (doc, at, by)))
            }
            ConfigKind::Section => {
                let mut stmt = self.conn.prepare(
                    "SELECT name, position, collapsed, display, updated_at, updated_by
                     FROM sections WHERE name = ?",
                )?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    let section = Section {
                        name: r.get::<_, String>(0)?,
                        position: r.get::<_, i32>(1)?,
                        collapsed: r.get::<_, Option<bool>>(2)?.unwrap_or(false),
                        display: r
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map_or(SectionDisplay::Inline, SectionDisplay::from_db),
                    };
                    Ok((
                        section,
                        r.get::<_, Option<i64>>(4)?,
                        r.get::<_, Option<String>>(5)?,
                    ))
                })?;
                match rows.next().transpose()? {
                    Some((section, at, by)) => Some((serde_json::to_string(&section)?, at, by)),
                    None => None,
                }
            }
            ConfigKind::Setting => {
                let mut stmt = self
                    .conn
                    .prepare("SELECT value, updated_at, updated_by FROM settings WHERE key = ?")?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                })?;
                match rows.next().transpose()? {
                    Some((value, at, by)) => Some((serde_json::to_string(&value)?, at, by)),
                    None => None,
                }
            }
            ConfigKind::Credential => {
                let mut stmt = self.conn.prepare(
                    "SELECT id, zone, region, secret_hash, salt, created_at, revoked_at,
                            updated_at, updated_by
                     FROM agent_credentials WHERE id = ?",
                )?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    let c = CredentialDoc {
                        id: r.get(0)?,
                        zone: r.get(1)?,
                        region: r.get(2)?,
                        secret_hash: r.get(3)?,
                        salt: r.get(4)?,
                        created_at: r.get(5)?,
                        revoked_at: r.get(6)?,
                    };
                    Ok((
                        c,
                        r.get::<_, Option<i64>>(7)?,
                        r.get::<_, Option<String>>(8)?,
                    ))
                })?;
                match rows.next().transpose()? {
                    Some((c, at, by)) => Some((serde_json::to_string(&c)?, at, by)),
                    None => None,
                }
            }
            ConfigKind::RollupPolicy => {
                let mut stmt = self.conn.prepare(
                    "SELECT node_id, node_type, degraded_when, down_when, config,
                            updated_at, updated_by
                     FROM rollup_policy WHERE node_id = ?",
                )?;
                let mut rows = stmt.query_map(duckdb::params![id], |r| {
                    let policy = Self::rollup_policy_row(r)?;
                    Ok((
                        RollupPolicyDoc {
                            node_type: policy.node_type,
                            policy: policy.policy,
                        },
                        Some(policy.updated_at),
                        policy.updated_by,
                    ))
                })?;
                match rows.next().transpose()? {
                    Some((doc, at, by)) => Some((serde_json::to_string(&doc)?, at, by)),
                    None => None,
                }
            }
        };
        Ok(doc.map(|(document, at, by)| ConfigEntry {
            kind,
            id: id.to_owned(),
            // Same NULL-as-oldest-HLC reading as the merge rule, so a row a
            // peer has never heard a stamp for still replicates as *something*
            // comparable rather than being unrepresentable.
            hlc: Hlc::new(at.unwrap_or(0), by.unwrap_or_default()),
            document: Some(document),
        }))
    }

    /// Read one ordered, bounded page of entries for a config kind. Both the
    /// candidate-id scan and the document reads are bounded, so a peer pull
    /// never materializes the full config table before applying its page size.
    pub fn config_entries_page(
        &self,
        kind: ConfigKind,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConfigEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let (table, pk) = kind.table_and_pk();
        // Select only a page of ids before projecting documents. The union
        // keeps live rows and tombstones in the same cursor order.
        let sql = format!(
            "SELECT id, deleted_at, node_id FROM (\
                 SELECT {pk}::VARCHAR AS id, NULL::BIGINT AS deleted_at, NULL::VARCHAR AS node_id \
                 FROM {table} \
                 UNION ALL \
                 SELECT id, deleted_at, node_id FROM config_tombstones WHERE kind = ?\
             ) entries \
             WHERE ? IS NULL OR id > ? \
             ORDER BY id \
             LIMIT ?"
        );
        let rows = self
            .conn
            .prepare(&sql)?
            .query_map(
                duckdb::params![kind.as_str(), after, after, limit as i64],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut entries = Vec::with_capacity(rows.len());
        for (id, deleted_at, node_id) in rows {
            if let Some(deleted_at) = deleted_at {
                entries.push(ConfigEntry {
                    kind,
                    id,
                    hlc: Hlc::new(deleted_at, node_id.unwrap_or_default()),
                    document: None,
                });
            } else if let Some(entry) = self.config_entry(kind, &id)? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Every entry of a kind, tombstones included, for an anti-entropy sweep.
    pub fn config_entries(&self, kind: ConfigKind) -> Result<Vec<ConfigEntry>> {
        let (table, pk) = kind.table_and_pk();
        let ids: Vec<String> = {
            // Cast explicitly: `pk` is a native `UUID` column for checks and
            // `VARCHAR` for everything else, and `FromSql for String` does not
            // accept a UUID column directly.
            let sql = format!("SELECT {pk}::VARCHAR FROM {table}");
            let mut stmt = self.conn.prepare(&sql)?;
            stmt.query_map([], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(e) = self.config_entry(kind, &id)? {
                out.push(e);
            }
        }
        let mut stmt = self
            .conn
            .prepare("SELECT id, deleted_at, node_id FROM config_tombstones WHERE kind = ?")?;
        let tombstones = stmt
            .query_map(duckdb::params![kind.as_str()], |r| {
                Ok(ConfigEntry {
                    kind,
                    id: r.get::<_, String>(0)?,
                    hlc: Hlc::new(r.get::<_, i64>(1)?, r.get::<_, String>(2)?),
                    document: None,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        out.extend(tombstones);
        Ok(out)
    }

    /// Stamp an existing row without changing it — used by paths that mutate
    /// a row through an older helper that doesn't know about replication.
    pub fn stamp_config(&self, kind: ConfigKind, id: &str, hlc: &Hlc) -> Result<()> {
        let (table, pk) = kind.table_and_pk();
        if kind == ConfigKind::Check {
            let Ok(id) = id.parse::<CheckId>() else {
                return Ok(());
            };
            self.conn.execute(
                &format!("UPDATE {table} SET updated_at = ?, updated_by = ? WHERE {pk} = ?"),
                duckdb::params![hlc.millis, hlc.node_id, id],
            )?;
        } else {
            self.conn.execute(
                &format!("UPDATE {table} SET updated_at = ?, updated_by = ? WHERE {pk} = ?"),
                duckdb::params![hlc.millis, hlc.node_id, id],
            )?;
        }
        Ok(())
    }

    /// Delete tombstones older than `before_millis`. Pruned separately from
    /// `prune()`'s other windows and on a longer one — the caller passes the
    /// larger of `results_days`/`alerts_days` because a node that was down for
    /// the *shorter* window would come back, still hold a check its tombstone
    /// says is deleted, and hand it straight back on the next anti-entropy
    /// sweep. That resurrection is exactly what a tombstone exists to
    /// prevent, so it must outlive every window a node could plausibly be
    /// offline for.
    pub fn prune_tombstones(&self, before_millis: i64) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM config_tombstones WHERE deleted_at < ?",
            duckdb::params![before_millis],
        )?;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::storage::test_support::*;

    // ---- Clustering: config replication ----

    #[test]
    fn apply_config_entry_higher_hlc_wins_equal_or_lower_is_rejected() {
        let s = Storage::open(":memory:").unwrap();
        let first = setting_entry("k", 100, "a", "first");
        assert!(s.apply_config_entry(&first).unwrap());
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("first"));

        let lower = setting_entry("k", 50, "z", "stale");
        assert!(
            !s.apply_config_entry(&lower).unwrap(),
            "an older stamp must not win, even from a node named after 'z'"
        );
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("first"));

        assert!(
            !s.apply_config_entry(&first).unwrap(),
            "an identical stamp changes nothing — it is not newer than itself"
        );
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("first"));

        let higher = setting_entry("k", 200, "a", "second");
        assert!(s.apply_config_entry(&higher).unwrap());
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("second"));
    }

    #[test]
    fn apply_config_entry_tombstone_higher_deletes_lower_does_not() {
        let s = Storage::open(":memory:").unwrap();
        s.apply_config_entry(&setting_entry("k", 100, "a", "value"))
            .unwrap();

        let low_tomb = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(50, "z"),
            document: None,
        };
        assert!(
            !s.apply_config_entry(&low_tomb).unwrap(),
            "an older delete must not win"
        );
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("value"));

        let high_tomb = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(200, "a"),
            document: None,
        };
        assert!(s.apply_config_entry(&high_tomb).unwrap());
        assert_eq!(s.get_setting("k").unwrap(), None, "the row is gone");
    }

    #[test]
    fn apply_config_entry_upsert_after_tombstone_resurrects_and_clears_it() {
        let s = Storage::open(":memory:").unwrap();
        s.apply_config_entry(&setting_entry("k", 100, "a", "value"))
            .unwrap();
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(200, "a"),
            document: None,
        })
        .unwrap();
        assert_eq!(s.get_setting("k").unwrap(), None);

        let resurrect = setting_entry("k", 300, "a", "back");
        assert!(s.apply_config_entry(&resurrect).unwrap());
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("back"));

        // The tombstone must be cleared, or the next digest sweep would see a
        // row that exists alongside a tombstone that says it does not.
        let tombstones: Vec<_> = s
            .config_entries(ConfigKind::Setting)
            .unwrap()
            .into_iter()
            .filter(|e| e.id == "k" && e.is_tombstone())
            .collect();
        assert!(tombstones.is_empty());
    }

    #[test]
    fn apply_config_entry_a_null_stamped_local_row_loses_to_anything() {
        let s = Storage::open(":memory:").unwrap();
        // Written through the pre-clustering path: no HLC stamp at all.
        s.put_setting("k", "legacy").unwrap();

        let replicated = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(1, "z"),
            document: Some(serde_json::to_string("replicated").unwrap()),
        };
        assert!(
            s.apply_config_entry(&replicated).unwrap(),
            "an unstamped row must lose to any real stamp"
        );
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("replicated"));
    }

    #[test]
    fn concurrent_edits_converge_to_the_same_winner_regardless_of_apply_order() {
        let a = Storage::open(":memory:").unwrap();
        let b = Storage::open(":memory:").unwrap();

        // Same millisecond, different nodes: the tie breaks on node_id, which
        // is a total order, so it must not matter which side applies first.
        let from_x = setting_entry("k", 100, "x", "from-x");
        let from_y = setting_entry("k", 100, "y", "from-y");

        a.apply_config_entry(&from_x).unwrap();
        a.apply_config_entry(&from_y).unwrap();

        b.apply_config_entry(&from_y).unwrap();
        b.apply_config_entry(&from_x).unwrap();

        assert_eq!(
            a.get_setting("k").unwrap(),
            b.get_setting("k").unwrap(),
            "both replicas must converge on the same value"
        );
        assert_eq!(
            a.get_setting("k").unwrap().as_deref(),
            Some("from-y"),
            "'y' beats 'x' as a node id, so it wins the tie either way"
        );
    }

    #[test]
    fn rollup_policy_config_replicates_with_stable_document() {
        use crate::rollup::{RollupPolicy, Threshold};

        let source = Storage::open(":memory:").unwrap();
        let replica = Storage::open(":memory:").unwrap();
        let entry = rollup_policy_entry(
            "region:eu",
            100,
            "source",
            NodeType::Region,
            RollupPolicy::new(Threshold::Any, Threshold::Quorum(2)),
        );
        assert!(source.apply_config_entry(&entry).unwrap());
        let wire = source
            .config_entry(ConfigKind::RollupPolicy, "region:eu")
            .unwrap()
            .unwrap();
        assert_eq!(
            wire.document.as_deref(),
            Some(
                r#"{"node_type":"region","policy":{"degraded_when":"any","down_when":{"quorum":2}}}"#
            )
        );

        assert!(replica.apply_config_entry(&wire).unwrap());
        assert_eq!(
            replica.list_rollup_policies().unwrap()[0].policy,
            entry
                .document
                .as_deref()
                .map(|doc| serde_json::from_str::<RollupPolicyDoc>(doc).unwrap().policy)
                .unwrap()
        );
    }

    #[test]
    fn rollup_policy_config_newer_write_wins() {
        use crate::rollup::{RollupPolicy, Threshold};

        let s = Storage::open(":memory:").unwrap();
        let newer = rollup_policy_entry(
            "zone:r:z",
            200,
            "b",
            NodeType::Zone,
            RollupPolicy::new(Threshold::Majority, Threshold::All),
        );
        let older = rollup_policy_entry(
            "zone:r:z",
            100,
            "a",
            NodeType::Zone,
            RollupPolicy::new(Threshold::Any, Threshold::All),
        );
        assert!(s.apply_config_entry(&newer).unwrap());
        assert!(!s.apply_config_entry(&older).unwrap());
        assert_eq!(
            s.list_rollup_policies().unwrap()[0].policy,
            RollupPolicy::new(Threshold::Majority, Threshold::All)
        );
    }

    #[test]
    fn rollup_policy_config_tombstone_deletes_and_blocks_older_write() {
        use crate::rollup::{RollupPolicy, Threshold};

        let s = Storage::open(":memory:").unwrap();
        let live = rollup_policy_entry(
            "global",
            100,
            "a",
            NodeType::Global,
            RollupPolicy::new(Threshold::Any, Threshold::All),
        );
        assert!(s.apply_config_entry(&live).unwrap());
        let tombstone = ConfigEntry {
            kind: ConfigKind::RollupPolicy,
            id: "global".into(),
            hlc: Hlc::new(200, "b"),
            document: None,
        };
        assert!(s.apply_config_entry(&tombstone).unwrap());
        assert!(s.list_rollup_policies().unwrap().is_empty());
        assert!(!s.apply_config_entry(&live).unwrap());
        assert_eq!(
            s.config_entries(ConfigKind::RollupPolicy).unwrap(),
            vec![tombstone]
        );
    }

    /// Arbitration is per entry, not per batch. Two edits to one id travelling
    /// together must resolve by HLC exactly as they would arriving separately:
    /// a batch is a transport detail, never a rule about who wins.
    #[test]
    fn a_batch_carrying_two_edits_to_one_id_still_resolves_by_hlc() {
        let s = Storage::open(":memory:").unwrap();
        let newer = setting_entry("theme", 2_000, "n1", "dark");
        let older = setting_entry("theme", 1_000, "n1", "light");

        let applied = s
            .apply_config_entries(&[newer.clone(), older.clone()])
            .unwrap();
        assert_eq!(
            applied,
            vec![true, false],
            "the older edit must lose even though it is last in the batch"
        );
        let back = s
            .config_entry(ConfigKind::Setting, "theme")
            .unwrap()
            .unwrap();
        assert_eq!(back.hlc, newer.hlc);

        // And the same pair in the other order still converges on the newer.
        let s = Storage::open(":memory:").unwrap();
        let applied = s.apply_config_entries(&[older, newer.clone()]).unwrap();
        assert_eq!(applied, vec![true, true]);
        let back = s
            .config_entry(ConfigKind::Setting, "theme")
            .unwrap()
            .unwrap();
        assert_eq!(back.hlc, newer.hlc);
    }

    #[test]
    fn config_batch_rolls_back_when_a_later_document_is_malformed() {
        let s = Storage::open(":memory:").unwrap();
        let valid = setting_entry("theme", 1_000, "n1", "dark");
        let malformed = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "broken".into(),
            hlc: Hlc::new(1_000, "n1"),
            document: Some("not JSON".into()),
        };

        assert!(s.apply_config_entries(&[valid, malformed]).is_err());
        assert!(s
            .config_entry(ConfigKind::Setting, "theme")
            .unwrap()
            .is_none());
    }

    /// Manual provisioning benchmark for the config write path. Run with
    /// `cargo test benchmark_apply_config_entries_10k -- --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark"]
    fn benchmark_apply_config_entries_10k() {
        let s = Storage::open(":memory:").unwrap();
        let entries = (0..10_000)
            .map(|i| setting_entry(&format!("setting-{i}"), i, "n1", "value"))
            .collect::<Vec<_>>();
        let start = std::time::Instant::now();
        let applied = s.apply_config_entries(&entries).unwrap();
        eprintln!(
            "applied {} config entries in {:?}",
            applied.iter().filter(|applied| **applied).count(),
            start.elapsed()
        );
        assert!(applied.iter().all(|applied| *applied));
    }

    #[test]
    fn config_entry_round_trips_each_kind_through_apply_and_read_back() {
        let s = Storage::open(":memory:").unwrap();
        let hlc = Hlc::new(1_000, "n1");

        let check = check_config("web", "https://example.com");
        let e = ConfigEntry {
            kind: ConfigKind::Check,
            id: check.id.to_string(),
            hlc: hlc.clone(),
            document: Some(serde_json::to_string(&check).unwrap()),
        };
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::Check, &check.id.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(back.hlc, hlc);
        let round: CheckConfig = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, check);

        let section = Section {
            name: "core".into(),
            position: 3,
            collapsed: true,
            display: SectionDisplay::Page,
        };
        let e = ConfigEntry {
            kind: ConfigKind::Section,
            id: "core".into(),
            hlc: hlc.clone(),
            document: Some(serde_json::to_string(&section).unwrap()),
        };
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::Section, "core")
            .unwrap()
            .unwrap();
        let round: Section = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, section);

        let e = setting_entry("theme", hlc.millis, &hlc.node_id, "dark");
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::Setting, "theme")
            .unwrap()
            .unwrap();
        let round: String = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, "dark");

        let cred = AgentCredential {
            id: "cred-1".into(),
            zone: "z1".into(),
            region: "r1".into(),
            secret_hash: "hash".into(),
            salt: "salt".into(),
            created_at: 5,
            revoked_at: Some(9),
        };
        let e = ConfigEntry {
            kind: ConfigKind::Credential,
            id: "cred-1".into(),
            hlc: hlc.clone(),
            document: Some(serde_json::to_string(&CredentialDoc::from(&cred)).unwrap()),
        };
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::Credential, "cred-1")
            .unwrap()
            .unwrap();
        let round: CredentialDoc = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(round, CredentialDoc::from(&cred));

        let policy = crate::rollup::RollupPolicy::new(
            crate::rollup::Threshold::Any,
            crate::rollup::Threshold::All,
        );
        let e = rollup_policy_entry(
            "region:test",
            hlc.millis,
            &hlc.node_id,
            NodeType::Region,
            policy,
        );
        assert!(s.apply_config_entry(&e).unwrap());
        let back = s
            .config_entry(ConfigKind::RollupPolicy, "region:test")
            .unwrap()
            .unwrap();
        let round: RollupPolicyDoc = serde_json::from_str(back.document.as_ref().unwrap()).unwrap();
        assert_eq!(
            round,
            RollupPolicyDoc {
                node_type: NodeType::Region,
                policy
            }
        );

        // Every kind now has exactly its one applied entry, no tombstones.
        for kind in ConfigKind::ALL {
            let entries = s.config_entries(kind).unwrap();
            assert_eq!(entries.len(), 1, "{kind:?} should have exactly one entry");
            assert!(!entries[0].is_tombstone());
        }

        // A missing id reads back as absent, not as an empty document.
        assert_eq!(s.config_entry(ConfigKind::Check, "nope").unwrap(), None);
    }

    #[test]
    fn config_entries_includes_tombstones_alongside_live_rows() {
        let s = Storage::open(":memory:").unwrap();
        s.apply_config_entry(&setting_entry("live", 100, "a", "v"))
            .unwrap();
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "dead".into(),
            hlc: Hlc::new(100, "a"),
            document: None,
        })
        .unwrap();

        let entries = s.config_entries(ConfigKind::Setting).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|e| e.id == "live" && !e.is_tombstone()));
        assert!(entries.iter().any(|e| e.id == "dead" && e.is_tombstone()));
    }

    #[test]
    fn config_entries_page_orders_and_bounds_live_rows_and_tombstones() {
        let s = Storage::open(":memory:").unwrap();
        for id in ["alpha", "charlie", "delta"] {
            s.apply_config_entry(&setting_entry(id, 100, "a", id))
                .unwrap();
        }
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "bravo".into(),
            hlc: Hlc::new(100, "a"),
            document: None,
        })
        .unwrap();

        let first = s.config_entries_page(ConfigKind::Setting, None, 2).unwrap();
        assert_eq!(
            first
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "bravo"]
        );
        assert!(!first[0].is_tombstone());
        assert!(first[1].is_tombstone());

        let second = s
            .config_entries_page(ConfigKind::Setting, Some("bravo"), 2)
            .unwrap();
        assert_eq!(
            second
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["charlie", "delta"]
        );
        assert!(s
            .config_entries_page(ConfigKind::Setting, Some("delta"), 2)
            .unwrap()
            .is_empty());
    }

    /// The comparison Phase 2 reconciliation relies on: a region's
    /// placement-scoped digest must equal the whole-table digest the region
    /// itself reports once it holds exactly that scoped subset — otherwise
    /// the two sides could never agree "converged" without paging documents
    /// to check. Tombstones are deliberately unscoped (a delete reaches
    /// every region), so they count identically in both digests too.
    #[test]
    fn config_digest_for_region_matches_a_region_holding_exactly_its_subset() {
        let global = Storage::open(":memory:").unwrap();
        let region = Storage::open(":memory:").unwrap();
        let us_only = placed_check("us-only", &["us"]);
        let tw_only = placed_check("tw-only", &["tw"]);
        global.upsert_check(&us_only, 10).unwrap();
        global.upsert_check(&tw_only, 20).unwrap();
        // The region only ever received what its placement admits — the
        // steady state Phase 2 reconciliation converges every region to.
        region.upsert_check(&us_only, 10).unwrap();

        let scoped = global.config_digest_for_region("us").unwrap();
        let whole = region.config_digest_for(ConfigKind::Check).unwrap();
        assert_eq!(
            scoped, whole,
            "a converged region's whole-table digest must equal the global \
             tier's placement-scoped one, or every sweep would look behind"
        );

        // A whole-catalogue comparison, the pre-Phase-2 behaviour, would
        // disagree forever even though the region is fully converged.
        let unscoped = global.config_digest_for(ConfigKind::Check).unwrap();
        assert_ne!(
            unscoped, whole,
            "the whole catalogue must differ from a converged subset region \
             — this is exactly the comparison Phase 2 replaces"
        );
    }

    #[test]
    fn config_digest_for_region_counts_tombstones_unscoped() {
        let s = Storage::open(":memory:").unwrap();
        let us_only = placed_check("us-only", &["us"]);
        s.upsert_check(&us_only, 10).unwrap();
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Check,
            id: crate::models::new_check_id().to_string(),
            hlc: Hlc::new(20, "global"),
            document: None,
        })
        .unwrap();

        // A region that never held the deleted check, and whose placement
        // admits nothing else here, still must count the tombstone: deletes
        // reach every region regardless of placement.
        let tw_digest = s.config_digest_for_region("tw").unwrap();
        assert_eq!(tw_digest.count, 1, "the tombstone alone");
        let us_digest = s.config_digest_for_region("us").unwrap();
        assert_eq!(us_digest.count, 2, "the placed check plus the tombstone");
    }

    #[test]
    fn stamp_config_restamps_without_changing_the_row() {
        let s = Storage::open(":memory:").unwrap();
        s.put_setting("k", "value").unwrap();
        let hlc = Hlc::new(500, "n1");
        s.stamp_config(ConfigKind::Setting, "k", &hlc).unwrap();
        assert_eq!(s.get_setting("k").unwrap().as_deref(), Some("value"));
        let entry = s.config_entry(ConfigKind::Setting, "k").unwrap().unwrap();
        assert_eq!(entry.hlc, hlc);
    }

    #[test]
    fn prune_tombstones_removes_old_ones_and_keeps_recent_ones() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000i64;
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "old".into(),
            hlc: Hlc::new(now - 100_000, "a"),
            document: None,
        })
        .unwrap();
        s.apply_config_entry(&ConfigEntry {
            kind: ConfigKind::Setting,
            id: "recent".into(),
            hlc: Hlc::new(now - 10, "a"),
            document: None,
        })
        .unwrap();

        let removed = s.prune_tombstones(now - 50_000).unwrap();
        assert_eq!(removed, 1, "only the old tombstone is removed");

        let remaining = s.config_entries(ConfigKind::Setting).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, "recent");
    }

    // ---- Clustering: digest ----

    #[test]
    fn full_digest_uses_write_time_watermarks() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_results(&[
            (result("a", 1000, 1), Status::Up),
            (result("a", 2000, 2), Status::Down),
            (result("b", 1500, 3), Status::Up),
        ])
        .unwrap();

        s.conn.execute("DELETE FROM check_results", []).unwrap();

        let digest = s.digest(0).unwrap();
        let a = digest
            .checks
            .iter()
            .find(|entry| entry.check_id == cid("a"))
            .unwrap();
        assert_eq!(a.max_ts, 2000);
        assert_eq!(a.count, 2);
        assert_eq!(digest.checks.len(), 2);
    }

    #[test]
    #[ignore = "production-scale benchmark; requires substantial time and disk"]
    fn benchmark_full_digest_with_production_retained_history() {
        let check_count = std::env::var("WARDEN_BENCH_CHECKS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(10_000);
        let observations_per_check = std::env::var("WARDEN_BENCH_OBSERVATIONS_PER_CHECK")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(30 * 24 * 60 * 2);
        let max_digest_ms = std::env::var("WARDEN_BENCH_MAX_DIGEST_MS")
            .ok()
            .and_then(|value| value.parse::<u128>().ok())
            .unwrap_or(2_000);
        let sample_count = 20usize;
        let path = std::env::temp_dir().join(format!(
            "warden-anti-entropy-bench-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let store = Storage::open(&path_string).unwrap();

        let seed_started = std::time::Instant::now();
        store
            .conn
            .execute_batch(&format!(
                "INSERT INTO check_results
                 SELECT md5('check-' || check_number::VARCHAR)::UUID,
                        'agent-' || (observation_number % 3)::VARCHAR,
                        observation_number,
                        'up', 1, 200, NULL, NULL, 'zone', 'region'
                 FROM range({check_count}) checks(check_number)
                 CROSS JOIN range({observations_per_check}) observations(observation_number);
                 INSERT INTO result_replication_watermarks
                 SELECT md5('check-' || check_number::VARCHAR)::UUID,
                        {observations_per_check} - 1,
                        {observations_per_check}
                 FROM range({check_count}) checks(check_number);
                 CHECKPOINT;"
            ))
            .unwrap();
        let seed_elapsed = seed_started.elapsed();
        let retained_rows = check_count * observations_per_check;
        let database_bytes = std::fs::metadata(&path).unwrap().len();

        let mut samples = Vec::with_capacity(sample_count);
        for _ in 0..sample_count {
            let started = std::time::Instant::now();
            let digest = store.digest(0).unwrap();
            samples.push(started.elapsed());
            assert_eq!(digest.checks.len() as u64, check_count);
        }
        samples.sort_unstable();
        let p50 = samples[sample_count / 2];
        let p95 = samples[(sample_count * 95 / 100).min(sample_count - 1)];
        let max = samples[sample_count - 1];

        println!(
            "checks={check_count} retained_rows={retained_rows} database_bytes={database_bytes} seed_ms={} digest_p50_ms={} digest_p95_ms={} digest_max_ms={}",
            seed_elapsed.as_millis(),
            p50.as_millis(),
            p95.as_millis(),
            max.as_millis()
        );
        assert!(
            p95.as_millis() <= max_digest_ms,
            "full digest p95 {p95:?} exceeded {max_digest_ms}ms with {retained_rows} retained rows"
        );

        drop(store);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn digest_reports_max_and_count_and_excludes_rows_before_window_start() {
        let s = Storage::open(":memory:").unwrap();
        let now = 100_000i64;

        s.insert_results(&[
            (result("a", now - 5000, 1), Status::Up),
            (result("a", now - 1000, 2), Status::Down),
            (result("b", now - 50_000, 3), Status::Up), // outside window
        ])
        .unwrap();

        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            now - 1000,
            None,
        )
        .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            None,
            NodeState::Up,
            now - 90_000,
            None,
        )
        .unwrap();

        s.create_incident(
            "inc-1",
            "t",
            IncidentState::Detected,
            now - 2000,
            false,
            Some(cid("a")),
            &[IncidentCheckRow {
                check_id: cid("a"),
                source: AffectSource::Origin,
            }],
            None,
        )
        .unwrap();
        s.create_incident(
            "inc-2",
            "t",
            IncidentState::Detected,
            now - 90_000,
            false,
            Some(cid("b")),
            &[],
            None,
        )
        .unwrap();

        let window_start = now - 10_000;
        let d = s.digest(window_start).unwrap();

        let a_digest = d.checks.iter().find(|c| c.check_id == cid("a")).unwrap();
        assert_eq!(a_digest.count, 2);
        assert_eq!(a_digest.max_ts, now - 1000);
        assert!(
            d.checks.iter().all(|c| c.check_id != cid("b")),
            "the out-of-window check is excluded entirely"
        );

        assert_eq!(
            d.alerts.max_millis,
            now - 1000,
            "the older alert is outside the window"
        );
        assert_eq!(
            d.incidents.max_millis,
            now - 1999,
            "the older incident is outside the window"
        );
    }

    // ---- Clustering: wire form ----

    #[test]
    fn config_entry_wire_form_is_stable() {
        let e = ConfigEntry {
            kind: ConfigKind::Check,
            id: "web".into(),
            hlc: Hlc::new(42, "n1"),
            document: Some("{}".into()),
        };
        assert_eq!(
            serde_json::to_string(&e).unwrap(),
            r#"{"kind":"check","id":"web","hlc":{"millis":42,"node_id":"n1"},"document":"{}"}"#
        );

        let tomb = ConfigEntry {
            kind: ConfigKind::Setting,
            id: "k".into(),
            hlc: Hlc::new(1, "a"),
            document: None,
        };
        assert_eq!(
            serde_json::to_string(&tomb).unwrap(),
            r#"{"kind":"setting","id":"k","hlc":{"millis":1,"node_id":"a"},"document":null}"#
        );
    }
}
