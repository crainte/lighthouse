use super::*;

/// Where an incident is in its life. The single lifecycle field: "open" means
/// anything other than `Resolved`, so there is no second status column to fall
/// out of sync with it.
///
/// `Detected` is deliberately not `Investigating`: a probe noticing an outage
/// is not a human looking at one, and on a public page the difference is the
/// expensive kind of lie. Acknowledging is the `Detected` -> `Investigating`
/// edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentState {
    Detected,
    Investigating,
    Identified,
    Monitoring,
    Resolved,
}

impl IncidentState {
    pub fn as_str(&self) -> &'static str {
        match self {
            IncidentState::Detected => "detected",
            IncidentState::Investigating => "investigating",
            IncidentState::Identified => "identified",
            IncidentState::Monitoring => "monitoring",
            IncidentState::Resolved => "resolved",
        }
    }

    /// Anything unrecognised reads as `Detected` rather than failing the row:
    /// a state nobody has acted on is the safe reading of a value we cannot
    /// interpret.
    pub fn from_db(s: &str) -> IncidentState {
        match s {
            "investigating" => IncidentState::Investigating,
            "identified" => IncidentState::Identified,
            "monitoring" => IncidentState::Monitoring,
            "resolved" => IncidentState::Resolved,
            _ => IncidentState::Detected,
        }
    }

    pub fn is_resolved(&self) -> bool {
        matches!(self, IncidentState::Resolved)
    }
}

/// How a check came to be attached to an incident. Recorded so reconciliation
/// can retract what it added without touching what an operator added by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AffectSource {
    /// The check whose DOWN opened the incident.
    Origin,
    /// Attached by `depends_on` reconciliation.
    Dependency,
    /// Attached by an operator in the editor.
    Manual,
}

impl AffectSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            AffectSource::Origin => "origin",
            AffectSource::Dependency => "dependency",
            AffectSource::Manual => "manual",
        }
    }

    pub fn from_db(s: &str) -> AffectSource {
        match s {
            "origin" => AffectSource::Origin,
            "dependency" => AffectSource::Dependency,
            _ => AffectSource::Manual,
        }
    }

    /// Whether this attachment gates auto-resolve. Manual ones do not: an
    /// operator naming a check they suspect must not be able to wedge an
    /// incident open forever.
    pub fn gates_resolution(&self) -> bool {
        !matches!(self, AffectSource::Manual)
    }
}

/// A check attached to an incident, and how it got there.
#[derive(Debug, Clone, PartialEq)]
pub struct IncidentCheckRow {
    pub check_id: CheckId,
    pub source: AffectSource,
}

/// One entry in an incident's narrative. Every state change is one of these,
/// including the two the coordinator writes itself on auto-open and
/// auto-resolve, so the timeline can never disagree with the state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentUpdateRow {
    pub id: String,
    pub incident_id: String,
    /// The state this update announced.
    pub state: IncidentState,
    pub body: String,
    /// `None` when the coordinator wrote it. A non-None author is what makes
    /// an update an acknowledgement; there is no separate ack column.
    pub author: Option<String>,
    pub created_at: i64,
}

/// The operator-facing record of an outage.
#[derive(Debug, Clone, PartialEq)]
pub struct IncidentRow {
    pub id: String,
    pub title: String,
    pub state: IncidentState,
    /// When the outage began, unix millis. Editable: an operator may move it
    /// back to when customers noticed rather than when a probe did. Moving it
    /// does not re-own alerts — those carry their own stamp.
    pub started_at: i64,
    /// When it was declared over; `None` while open.
    pub resolved_at: Option<i64>,
    /// Rendered page-wide rather than against its checks. A global incident
    /// with no checks at all is the maintenance-notice case.
    pub global: bool,
    /// The check whose DOWN opened it. `None` for a manually opened incident,
    /// which reconciliation therefore ignores entirely.
    pub origin_check: Option<CheckId>,
    /// Every check this incident affects, with how each was attached.
    pub checks: Vec<IncidentCheckRow>,
}

impl IncidentRow {
    /// How long the outage has run: to its resolution, or to now while open.
    pub fn duration_ms(&self, now_millis: i64) -> i64 {
        self.resolved_at.unwrap_or(now_millis) - self.started_at
    }

    /// The checks whose recovery gates auto-resolve — everything except what
    /// an operator attached by hand.
    pub fn gating_checks(&self) -> impl Iterator<Item = CheckId> + '_ {
        self.checks
            .iter()
            .filter(|c| c.source.gates_resolution())
            .map(|c| c.check_id)
    }
}

/// An incident as it replicates: the row plus everything that hangs off it.
/// Shipped whole rather than as three separate wire messages, because
/// `upsert_incident` has to apply the checks and updates in the same
/// transaction as the row or a reader could see an incident with no checks
/// yet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicatedIncident {
    pub id: String,
    pub version: Hlc,
    pub title: String,
    pub state: IncidentState,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
    pub global: bool,
    pub origin_check: Option<CheckId>,
    pub checks: Vec<(CheckId, AffectSource)>,
    pub updates: Vec<IncidentUpdateRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncidentTombstone {
    pub id: String,
    pub deleted_at: i64,
    pub node_id: String,
}

impl Storage {
    pub fn set_incident_node_id(&self, node_id: &str) {
        *self.incident_node_id.write() = node_id.to_string();
    }

    pub(super) fn incident_node_id(&self) -> String {
        self.incident_node_id.read().clone()
    }

    pub(super) fn touch_incident(&self, id: &str, millis: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE incidents SET version_millis = GREATEST(version_millis + 1, ?), version_node = ?
             WHERE id = ?",
            duckdb::params![millis, self.incident_node_id(), id],
        )?;
        let version: Option<i64> = self
            .conn
            .query_row(
                "SELECT version_millis FROM incidents WHERE id = ?",
                duckdb::params![id],
                |row| row.get(0),
            )
            .ok();
        if let Some(version) = version {
            let cursor_id = format!("{}\0{id}", self.incident_node_id());
            self.record_stream_item("incidents", version, &cursor_id, 0)?;
        }
        Ok(())
    }

    /// The open incident **the coordinator owns** for a check: one it opened
    /// from a probe, identified by having an `origin_check`. A check is in at
    /// most one of these, because reconciliation joins an existing incident
    /// rather than opening a second.
    ///
    /// Manually created incidents are excluded on purpose. An operator's
    /// incident has no origin check, so the reconciler has nothing to follow —
    /// it must not adopt dependents into it, must not let a failing check ride
    /// it instead of opening a real one, and above all must not resolve it. A
    /// maintenance notice is over when a human says so.
    pub fn open_incident_for(&self, check_id: CheckId) -> Result<Option<IncidentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT i.id
             FROM incidents i
             JOIN incident_checks c ON c.incident_id = i.id
             WHERE c.check_id = ?
               AND i.state <> 'resolved'
               AND i.origin_check IS NOT NULL
             ORDER BY i.started_at DESC
             LIMIT 1",
        )?;
        let mut ids = stmt
            .query_map(duckdb::params![check_id], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        match ids.pop() {
            Some(id) => self.incident(&id),
            None => Ok(None),
        }
    }

    /// Open an incident. `origin_check` is `None` for one an operator created,
    /// which reconciliation then ignores entirely. The opening update is
    /// written in the same call, so an incident never exists without the entry
    /// explaining it.
    #[allow(clippy::too_many_arguments)]
    pub fn create_incident(
        &self,
        id: &str,
        title: &str,
        state: IncidentState,
        started_at: i64,
        global: bool,
        origin_check: Option<CheckId>,
        checks: &[IncidentCheckRow],
        first_update: Option<(&str, Option<&str>)>,
    ) -> Result<IncidentRow> {
        self.conn.execute(
            "INSERT INTO incidents
                 (id, title, state, started_at, global, origin_check, version_millis, version_node)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                id,
                title,
                state.as_str(),
                started_at,
                global,
                origin_check,
                started_at,
                self.incident_node_id()
            ],
        )?;
        for c in checks {
            self.conn.execute(
                "INSERT INTO incident_checks (incident_id, check_id, source) VALUES (?, ?, ?)",
                duckdb::params![id, c.check_id, c.source.as_str()],
            )?;
        }
        if let Some((body, author)) = first_update {
            self.add_update(id, state, body, author, started_at)?;
        }
        self.touch_incident(id, started_at)?;
        let cursor_id = format!("{}\0{id}", self.incident_node_id());
        self.record_stream_item("incidents", started_at, &cursor_id, 1)?;
        self.incident(id)?
            .ok_or_else(|| anyhow::anyhow!("incident {id} vanished immediately after insert"))
    }

    /// Attach a check to an incident. Idempotent on (incident, check): a repeat
    /// only upgrades how it got there, and never downgrades an `origin`.
    pub fn attach_check(
        &self,
        incident_id: &str,
        check_id: CheckId,
        source: AffectSource,
    ) -> Result<()> {
        let existing: Option<String> = {
            let mut stmt = self.conn.prepare(
                "SELECT source FROM incident_checks WHERE incident_id = ? AND check_id = ?",
            )?;
            let mut rows = stmt
                .query_map(duckdb::params![incident_id, check_id], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows.pop()
        };
        match existing.as_deref().map(AffectSource::from_db) {
            // The origin is how the incident came to exist; nothing reclassifies it.
            Some(AffectSource::Origin) => Ok(()),
            Some(current) if current == source => Ok(()),
            Some(_) => {
                self.conn.execute(
                    "UPDATE incident_checks SET source = ?
                     WHERE incident_id = ? AND check_id = ?",
                    duckdb::params![source.as_str(), incident_id, check_id],
                )?;
                self.touch_incident(incident_id, chrono::Utc::now().timestamp_millis())
            }
            None => {
                self.conn.execute(
                    "INSERT INTO incident_checks (incident_id, check_id, source)
                     VALUES (?, ?, ?)",
                    duckdb::params![incident_id, check_id, source.as_str()],
                )?;
                self.touch_incident(incident_id, chrono::Utc::now().timestamp_millis())
            }
        }
    }

    /// Detach a check. `only_source` restricts it to attachments of one kind,
    /// so reconciliation can retract a `dependency` without touching what an
    /// operator added by hand.
    pub fn detach_check(
        &self,
        incident_id: &str,
        check_id: CheckId,
        only_source: Option<AffectSource>,
    ) -> Result<()> {
        let changed = match only_source {
            Some(s) => self.conn.execute(
                "DELETE FROM incident_checks
                 WHERE incident_id = ? AND check_id = ? AND source = ?",
                duckdb::params![incident_id, check_id, s.as_str()],
            )?,
            None => self.conn.execute(
                "DELETE FROM incident_checks WHERE incident_id = ? AND check_id = ?",
                duckdb::params![incident_id, check_id],
            )?,
        };
        if changed != 0 {
            self.touch_incident(incident_id, chrono::Utc::now().timestamp_millis())?;
        }
        Ok(())
    }

    /// Replace exactly the manual attachments, leaving origin and dependency
    /// rows alone. The affects editor saves through this.
    pub fn set_manual_checks(&self, incident_id: &str, checks: &[CheckId]) -> Result<()> {
        let current: std::collections::BTreeSet<CheckId> = {
            let mut stmt = self.conn.prepare(
                "SELECT check_id FROM incident_checks WHERE incident_id = ? AND source = 'manual'",
            )?;
            stmt.query_map(duckdb::params![incident_id], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?
        };
        let desired: std::collections::BTreeSet<CheckId> = checks.iter().copied().collect();
        if current == desired {
            return Ok(());
        }
        self.conn.execute(
            "DELETE FROM incident_checks WHERE incident_id = ? AND source = 'manual'",
            duckdb::params![incident_id],
        )?;
        let mut stmt = self.conn.prepare(
            "INSERT INTO incident_checks (incident_id, check_id, source) VALUES (?, ?, 'manual')",
        )?;
        for check_id in desired {
            stmt.execute(duckdb::params![incident_id, check_id])?;
        }
        self.touch_incident(incident_id, chrono::Utc::now().timestamp_millis())
    }

    /// Add an update, moving the incident to the state it announces. This is
    /// the only way an incident changes state — operator posts, acknowledgement
    /// and the coordinator's own auto-open/auto-resolve entries all land here,
    /// so the timeline can never disagree with the state.
    ///
    /// `author` is `None` when the coordinator wrote it; a non-None author on a
    /// `detected` incident is what constitutes an acknowledgement.
    pub fn add_update(
        &self,
        incident_id: &str,
        state: IncidentState,
        body: &str,
        author: Option<&str>,
        now_millis: i64,
    ) -> Result<IncidentUpdateRow> {
        let id = self.next_event_id("incident_updates", &format!("{incident_id}-u{now_millis}"))?;
        self.conn.execute(
            "INSERT INTO incident_updates (id, incident_id, state, body, author, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![id, incident_id, state.as_str(), body, author, now_millis],
        )?;
        // resolved_at tracks the state rather than being set beside it: moving
        // back out of `resolved` (an operator reopening) must clear it, or the
        // incident would read as both open and finished.
        if state.is_resolved() {
            self.conn.execute(
                "UPDATE incidents SET state = ?, resolved_at = ? WHERE id = ?",
                duckdb::params![state.as_str(), now_millis, incident_id],
            )?;
        } else {
            self.conn.execute(
                "UPDATE incidents SET state = ?, resolved_at = NULL WHERE id = ?",
                duckdb::params![state.as_str(), incident_id],
            )?;
        }
        self.touch_incident(incident_id, now_millis)?;
        Ok(IncidentUpdateRow {
            id,
            incident_id: incident_id.to_owned(),
            state,
            body: body.to_owned(),
            author: author.map(str::to_owned),
            created_at: now_millis,
        })
    }

    /// An incident's narrative, oldest first.
    pub fn updates_for_incident(&self, incident_id: &str) -> Result<Vec<IncidentUpdateRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, incident_id, state, body, author, created_at
             FROM incident_updates
             WHERE incident_id = ?
             ORDER BY created_at ASC",
        )?;
        let rows = stmt
            .query_map(duckdb::params![incident_id], |row| {
                Ok(IncidentUpdateRow {
                    id: row.get::<_, String>(0)?,
                    incident_id: row.get::<_, String>(1)?,
                    state: IncidentState::from_db(&row.get::<_, String>(2)?),
                    body: row.get::<_, String>(3)?,
                    author: row.get::<_, Option<String>>(4)?,
                    created_at: row.get::<_, i64>(5)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Whether a human has touched this incident. Derived from the updates
    /// rather than stored: the acknowledgement IS the first authored update, so
    /// a column beside it would be a second copy of the same fact. Stays true
    /// when an operator skipped straight past `investigating`.
    pub fn is_acknowledged(&self, incident_id: &str) -> Result<bool> {
        let mut stmt = self.conn.prepare(
            "SELECT 1 FROM incident_updates
             WHERE incident_id = ? AND author IS NOT NULL LIMIT 1",
        )?;
        let mut rows = stmt
            .query_map(duckdb::params![incident_id], |r| r.get::<_, i32>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows.pop().is_some())
    }

    /// Edit the operator-owned fields. Deliberately cannot touch `state` —
    /// that moves only by posting an update.
    pub fn edit_incident(
        &self,
        id: &str,
        title: &str,
        started_at: i64,
        global: bool,
    ) -> Result<Option<IncidentRow>> {
        let Some(existing) = self.incident(id)? else {
            return Ok(None);
        };
        if existing.title == title && existing.started_at == started_at && existing.global == global
        {
            return Ok(Some(existing));
        }
        self.conn.execute(
            "UPDATE incidents SET title = ?, started_at = ?, global = ? WHERE id = ?",
            duckdb::params![title, started_at, global, id],
        )?;
        self.touch_incident(id, chrono::Utc::now().timestamp_millis())?;
        self.incident(id)
    }

    /// One incident with its affected checks.
    pub fn incident(&self, id: &str) -> Result<Option<IncidentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, title, state, started_at, resolved_at, global, origin_check
             FROM incidents WHERE id = ?",
        )?;
        let mut rows = stmt
            .query_map(duckdb::params![id], Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        match rows.pop() {
            Some(mut row) => {
                row.checks = self.checks_for_incident(&row.id)?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    pub(super) fn checks_for_incident(&self, incident_id: &str) -> Result<Vec<IncidentCheckRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT check_id, source FROM incident_checks
             WHERE incident_id = ? ORDER BY check_id",
        )?;
        let rows = stmt
            .query_map(duckdb::params![incident_id], |row| {
                Ok(IncidentCheckRow {
                    check_id: row.get::<_, CheckId>(0)?,
                    source: AffectSource::from_db(&row.get::<_, String>(1)?),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Recent incidents, newest first. `open_only` filters to current outages —
    /// which is `state <> 'resolved'`, since state is the only lifecycle field.
    pub fn list_incidents(
        &self,
        limit: usize,
        open_only: Option<bool>,
        since: Option<i64>,
    ) -> Result<Vec<IncidentRow>> {
        let mut sql = String::from(
            "SELECT id, title, state, started_at, resolved_at, global, origin_check
             FROM incidents WHERE TRUE",
        );
        match open_only {
            Some(true) => sql.push_str(" AND state <> 'resolved'"),
            Some(false) => sql.push_str(" AND state = 'resolved'"),
            None => {}
        }
        if since.is_some() {
            sql.push_str(" AND (state <> 'resolved' OR resolved_at >= ?)");
        }
        sql.push_str(" ORDER BY started_at DESC LIMIT ?");
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = if let Some(since) = since {
            stmt.query_map(duckdb::params![since, limit as i64], Self::incident_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(duckdb::params![limit as i64], Self::incident_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for row in &mut rows {
            row.checks = self.checks_for_incident(&row.id)?;
        }
        Ok(rows)
    }

    /// A check's incidents, newest first — every incident it was attached to,
    /// however it got there.
    pub fn incidents_for_check(&self, check_id: CheckId, limit: usize) -> Result<Vec<IncidentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT i.id, i.title, i.state, i.started_at, i.resolved_at, i.global, i.origin_check
             FROM incidents i
             JOIN incident_checks c ON c.incident_id = i.id
             WHERE c.check_id = ?
             ORDER BY i.started_at DESC
             LIMIT ?",
        )?;
        let mut rows = stmt
            .query_map(duckdb::params![check_id, limit as i64], Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for row in &mut rows {
            row.checks = self.checks_for_incident(&row.id)?;
        }
        Ok(rows)
    }

    pub(super) fn incident_row(row: &duckdb::Row) -> duckdb::Result<IncidentRow> {
        Ok(IncidentRow {
            id: row.get::<_, String>(0)?,
            title: row.get::<_, String>(1)?,
            state: IncidentState::from_db(&row.get::<_, String>(2)?),
            started_at: row.get::<_, i64>(3)?,
            resolved_at: row.get::<_, Option<i64>>(4)?,
            global: row.get::<_, bool>(5)?,
            origin_check: row.get::<_, Option<CheckId>>(6)?,
            checks: Vec::new(),
        })
    }

    /// Delete resolved incidents shorter than `max_secs`, with everything
    /// stamped as belonging to them.
    ///
    /// This exists to clean up after the bug the region tree fixes: with
    /// `failures_before_down = 1` and no hold-down, one agent's single failed
    /// probe opened an incident that the next probe closed 50 seconds later.
    /// Those rows are not history anybody wants, and leaving them makes the
    /// incident list unreadable for exactly the checks with the most agents.
    ///
    /// Only *resolved* incidents are touched. An open incident has no duration
    /// yet, and a short-lived open one is simply a new one.
    pub fn purge_short_incidents(&self, max_secs: u64) -> Result<u64> {
        let max_ms = (max_secs as i64).saturating_mul(1000);
        // DuckDB has no ON DELETE CASCADE, so remove every dependent row before
        // its incident. Alerts are deliberately included: this cleanup erases a
        // known false-positive outage, not merely its operator narrative.
        let selector = "SELECT id FROM incidents
             WHERE state = 'resolved' AND resolved_at IS NOT NULL
                AND resolved_at - started_at < ?";
        let mut removed = 0u64;
        for table in ["incident_updates", "incident_checks", "alerts"] {
            removed += self.conn.execute(
                &format!("DELETE FROM {table} WHERE incident_id IN ({selector})"),
                duckdb::params![max_ms],
            )? as u64;
        }

        removed += self.conn.execute(
            "DELETE FROM incidents
             WHERE state = 'resolved' AND resolved_at IS NOT NULL
               AND resolved_at - started_at < ?",
            duckdb::params![max_ms],
        )? as u64;
        if removed != 0 {
            self.rebuild_stream_watermarks()?;
        }
        Ok(removed)
    }

    /// Apply a replicated incident by id. Exact replays are no-ops; an update
    /// id may be reused only for its identical immutable payload, never to
    /// overwrite a different incident's narrative.
    pub fn upsert_incident(&self, i: &ReplicatedIncident) -> Result<bool> {
        self.transaction(|store| store.upsert_incident_uncommitted(i))
    }

    pub(crate) fn upsert_incident_uncommitted(&self, i: &ReplicatedIncident) -> Result<bool> {
        // Rows and tombstones share one total order. A newer incident may
        // intentionally recreate a deleted id; an equal/newer tombstone wins,
        // while a stale delete cannot erase a newer row.
        let superseded_tombstone = match self.incident_tombstone(&i.id)? {
            Some(t) => {
                let tombstone_version = Hlc::new(t.deleted_at, t.node_id);
                if tombstone_version >= i.version {
                    return Ok(false);
                }
                true
            }
            None => false,
        };
        let existed = if let Some(existing) = self.replicated_incident(&i.id)? {
            if existing.version > i.version || existing == *i {
                return Ok(false);
            }
            if existing.version == i.version
                && serde_json::to_vec(&existing)? >= serde_json::to_vec(i)?
            {
                return Ok(false);
            }
            true
        } else {
            false
        };
        for update in &i.updates {
            if update.incident_id != i.id {
                bail!(
                    "replicated update {} belongs to {}, not {}",
                    update.id,
                    update.incident_id,
                    i.id
                );
            }
            let existing = self
                .conn
                .query_row(
                    "SELECT id, incident_id, state, body, author, created_at
                 FROM incident_updates WHERE id = ?",
                    duckdb::params![update.id],
                    |row| {
                        Ok(IncidentUpdateRow {
                            id: row.get(0)?,
                            incident_id: row.get(1)?,
                            state: IncidentState::from_db(&row.get::<_, String>(2)?),
                            body: row.get(3)?,
                            author: row.get(4)?,
                            created_at: row.get(5)?,
                        })
                    },
                )
                .ok();
            if let Some(existing) = existing {
                if existing.incident_id != i.id {
                    bail!(
                        "replicated incident update id {} belongs to another incident",
                        update.id
                    );
                }
            }
        }
        let write = || -> Result<()> {
            self.conn
                .execute("DELETE FROM incidents WHERE id = ?", duckdb::params![i.id])?;
            if superseded_tombstone {
                self.conn.execute(
                    "DELETE FROM incident_tombstones WHERE id = ?",
                    duckdb::params![i.id],
                )?;
            }
            self.conn.execute(
                "INSERT INTO incidents
                     (id, title, state, started_at, resolved_at, global, origin_check,
                      version_millis, version_node)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    i.id,
                    i.title,
                    i.state.as_str(),
                    i.started_at,
                    i.resolved_at,
                    i.global,
                    i.origin_check,
                    i.version.millis,
                    i.version.node_id,
                ],
            )?;
            self.conn.execute(
                "DELETE FROM incident_checks WHERE incident_id = ?",
                duckdb::params![i.id],
            )?;
            for (check_id, source) in &i.checks {
                self.conn.execute(
                    "INSERT INTO incident_checks (incident_id, check_id, source)
                     VALUES (?, ?, ?)",
                    duckdb::params![i.id, check_id, source.as_str()],
                )?;
            }
            self.conn.execute(
                "DELETE FROM incident_updates WHERE incident_id = ?",
                duckdb::params![i.id],
            )?;
            for u in &i.updates {
                self.conn.execute(
                    "INSERT INTO incident_updates
                         (id, incident_id, state, body, author, created_at)
                      VALUES (?, ?, ?, ?, ?, ?)",
                    duckdb::params![u.id, i.id, u.state.as_str(), u.body, u.author, u.created_at],
                )?;
            }
            let cursor_id = format!("{}\0{}", i.version.node_id, i.id);
            self.record_stream_item(
                "incidents",
                i.version.millis,
                &cursor_id,
                i64::from(!existed),
            )?;
            if superseded_tombstone {
                self.rebuild_stream_watermarks()?;
            }
            Ok(())
        };
        write()?;
        Ok(true)
    }

    /// One incident in the shape it replicates as. `None` when it does not
    /// exist locally.
    pub fn replicated_incident(&self, id: &str) -> Result<Option<ReplicatedIncident>> {
        let Some(row) = self.incident(id)? else {
            return Ok(None);
        };
        let updates = self.updates_for_incident(id)?;
        Ok(Some(ReplicatedIncident {
            id: row.id,
            version: self.conn.query_row(
                "SELECT version_millis, version_node FROM incidents WHERE id = ?",
                duckdb::params![id],
                |r| Ok(Hlc::new(r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )?,
            title: row.title,
            state: row.state,
            started_at: row.started_at,
            resolved_at: row.resolved_at,
            global: row.global,
            origin_check: row.origin_check,
            checks: row
                .checks
                .into_iter()
                .map(|c| (c.check_id, c.source))
                .collect(),
            updates,
        }))
    }

    /// Incidents changed after the exclusive stable mutation cursor.
    pub fn incidents_since(
        &self,
        since_version: i64,
        since_id: &str,
        limit: usize,
    ) -> Result<Vec<ReplicatedIncident>> {
        let (since_node, since_incident_id) = since_id.split_once('\0').unwrap_or(("", since_id));
        let ids: Vec<String> = {
            let mut stmt = self.conn.prepare(
                "SELECT id FROM incidents
                 WHERE version_millis > ?
                    OR (version_millis = ? AND (version_node > ?
                        OR (version_node = ? AND id > ?)))
                 ORDER BY version_millis ASC, version_node ASC, id ASC LIMIT ?",
            )?;
            stmt.query_map(
                duckdb::params![
                    since_version,
                    since_version,
                    since_node,
                    since_node,
                    since_incident_id,
                    limit as i64
                ],
                |r| r.get::<_, String>(0),
            )?
            .collect::<std::result::Result<_, _>>()?
        };
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(i) = self.replicated_incident(&id)? {
                out.push(i);
            }
        }
        Ok(out)
    }

    pub fn incident_tombstones_since(
        &self,
        since_deleted_at: i64,
        since_node_id: &str,
        since_id: &str,
        limit: usize,
    ) -> Result<Vec<IncidentTombstone>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, deleted_at, node_id FROM incident_tombstones
              WHERE deleted_at > ?
                 OR (deleted_at = ? AND node_id > ?)
                 OR (deleted_at = ? AND node_id = ? AND id > ?)
              ORDER BY deleted_at ASC, node_id ASC, id ASC LIMIT ?",
        )?;
        stmt.query_map(
            duckdb::params![
                since_deleted_at,
                since_deleted_at,
                since_node_id,
                since_deleted_at,
                since_node_id,
                since_id,
                limit as i64
            ],
            |row| {
                Ok(IncidentTombstone {
                    id: row.get(0)?,
                    deleted_at: row.get(1)?,
                    node_id: row.get(2)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
    }

    // ---- Incident deletion ----

    /// Erase an incident: its narrative, its affects rows, and the incident
    /// itself, leaving a tombstone so a peer's push cannot resurrect it.
    ///
    /// Linked alerts are **unlinked, not deleted**. An alert is the raw record
    /// that a check flipped; an incident is the narrative written around it.
    /// Deleting the narrative must not erase the fact, or the alert feed and
    /// the uptime figure would start disagreeing about the same outage.
    ///
    /// Returns false when there was no such incident, so a handler can 404
    /// rather than report success for an id that never existed. A repeat
    /// delete of an already-tombstoned id is still false, and still harmless.
    pub fn delete_incident(&self, id: &str, now_millis: i64, node_id: &str) -> Result<bool> {
        let deleted_at = self
            .replicated_incident(id)?
            .map(|incident| now_millis.max(incident.version.millis + 1))
            .unwrap_or(now_millis);
        self.apply_incident_tombstone(&IncidentTombstone {
            id: id.to_string(),
            deleted_at,
            node_id: node_id.to_string(),
        })
    }

    pub fn apply_incident_tombstone(&self, tombstone: &IncidentTombstone) -> Result<bool> {
        self.transaction(|store| store.apply_incident_tombstone_uncommitted(tombstone))
    }

    pub(crate) fn apply_incident_tombstone_uncommitted(
        &self,
        tombstone: &IncidentTombstone,
    ) -> Result<bool> {
        let id = &tombstone.id;
        let incoming_version = Hlc::new(tombstone.deleted_at, tombstone.node_id.clone());
        if let Some(incident) = self.replicated_incident(id)? {
            if incident.version > incoming_version {
                return Ok(false);
            }
        }
        let tombstone_existed = if let Some(existing) = self.incident_tombstone(id)? {
            if Hlc::new(existing.deleted_at, existing.node_id) >= incoming_version {
                return Ok(false);
            }
            true
        } else {
            false
        };
        let write = || -> Result<bool> {
            self.conn.execute(
                "DELETE FROM incident_updates WHERE incident_id = ?",
                duckdb::params![id],
            )?;
            self.conn.execute(
                "DELETE FROM incident_checks WHERE incident_id = ?",
                duckdb::params![id],
            )?;
            self.conn.execute(
                "UPDATE alerts SET incident_id = NULL WHERE incident_id = ?",
                duckdb::params![id],
            )?;
            let removed = self
                .conn
                .execute("DELETE FROM incidents WHERE id = ?", duckdb::params![id])?;
            // The tombstone is written even when the row was already gone: a
            // peer that deletes an incident this node never received still has
            // to be able to stop it arriving afterwards.
            self.conn.execute(
                "INSERT OR REPLACE INTO incident_tombstones (id, deleted_at, node_id)
                 VALUES (?, ?, ?)",
                duckdb::params![id, tombstone.deleted_at, tombstone.node_id],
            )?;
            let cursor_id = format!("{}\0{id}", tombstone.node_id);
            self.record_stream_item(
                "incident_tombstones",
                tombstone.deleted_at,
                &cursor_id,
                i64::from(!tombstone_existed),
            )?;
            // Replacement and deletion can invalidate a stream's previous max,
            // not only its count. Rare deletes favor exact metadata over an
            // incremental update that cannot know the next-highest row.
            self.rebuild_stream_watermarks()?;
            Ok(removed > 0)
        };
        write()
    }

    /// Whether an incident id has been deleted anywhere in the cluster.
    pub fn incident_tombstone(&self, id: &str) -> Result<Option<IncidentTombstone>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, deleted_at, node_id FROM incident_tombstones WHERE id = ?")?;
        let mut rows = stmt.query_map(duckdb::params![id], |row| {
            Ok(IncidentTombstone {
                id: row.get(0)?,
                deleted_at: row.get(1)?,
                node_id: row.get(2)?,
            })
        })?;
        rows.next().transpose().map_err(Into::into)
    }

    pub fn incident_tombstoned(&self, id: &str) -> Result<bool> {
        let found: i64 = self.conn.query_row(
            "SELECT count(*) FROM incident_tombstones WHERE id = ?",
            duckdb::params![id],
            |r| r.get(0),
        )?;
        Ok(found > 0)
    }

    /// Every incident id this node knows to be deleted, for replicating the
    /// deletion to a peer that has not heard about it yet.
    pub fn list_incident_tombstones(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT id FROM incident_tombstones")?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<_, _>>()?;
        Ok(rows)
    }

    /// Delete incident tombstones older than `before_millis`. Same window and
    /// same reasoning as `prune_tombstones`: a tombstone has to outlive the
    /// longest outage a peer could plausibly have been down for, or that peer
    /// comes back and pushes the incident again.
    pub fn prune_incident_tombstones(&self, before_millis: i64) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM incident_tombstones WHERE deleted_at < ?",
            duckdb::params![before_millis],
        )?;
        if n != 0 {
            self.rebuild_stream_watermarks()?;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::storage::test_support::*;

    #[test]
    fn upsert_incident_is_idempotent_and_updates_on_a_changed_title() {
        let s = Storage::open(":memory:").unwrap();
        let i = sample_replicated_incident();
        s.upsert_incident(&i).unwrap();
        s.upsert_incident(&i).unwrap();

        let stored = s.incident("inc-1").unwrap().unwrap();
        assert_eq!(stored.checks.len(), 1, "no duplicated attachment");
        assert_eq!(
            s.updates_for_incident("inc-1").unwrap().len(),
            1,
            "no duplicated update"
        );

        let mut renamed = i.clone();
        renamed.version = Hlc::new(101, "a");
        renamed.title = "Major outage".into();
        s.upsert_incident(&renamed).unwrap();
        let stored = s.incident("inc-1").unwrap().unwrap();
        assert_eq!(stored.title, "Major outage");
        assert_eq!(
            s.list_incidents(10, None, None).unwrap().len(),
            1,
            "still one incident row, not a second one"
        );
    }

    #[test]
    fn upsert_incident_replays_identically_and_rejects_update_id_collisions() {
        let s = Storage::open(":memory:").unwrap();
        let incident = sample_replicated_incident();
        assert!(s.upsert_incident(&incident).unwrap());
        assert!(
            !s.upsert_incident(&incident).unwrap(),
            "an identical replay is a no-op"
        );

        let mut collision = incident.clone();
        collision.id = "inc-2".into();
        collision.updates[0].incident_id = collision.id.clone();
        collision.updates[0].body = "different event with reused id".into();
        assert!(s.upsert_incident(&collision).is_err());
        assert_eq!(s.replicated_incident("inc-1").unwrap(), Some(incident));
        assert_eq!(s.replicated_incident("inc-2").unwrap(), None);
    }

    #[test]
    fn replicated_incident_round_trips_through_upsert() {
        let s = Storage::open(":memory:").unwrap();
        let i = sample_replicated_incident();
        s.upsert_incident(&i).unwrap();
        let back = s.replicated_incident("inc-1").unwrap().unwrap();
        assert_eq!(back, i);
        assert_eq!(s.replicated_incident("missing").unwrap(), None);
    }

    #[test]
    fn equal_version_incident_conflicts_converge_independent_of_delivery_order() {
        let a = Storage::open(":memory:").unwrap();
        let b = Storage::open(":memory:").unwrap();
        let first = sample_replicated_incident();
        let mut second = first.clone();
        second.title = "Conflicting title".into();
        let expected = if serde_json::to_vec(&first).unwrap() > serde_json::to_vec(&second).unwrap()
        {
            first.clone()
        } else {
            second.clone()
        };

        a.upsert_incident(&first).unwrap();
        a.upsert_incident(&second).unwrap();
        b.upsert_incident(&second).unwrap();
        b.upsert_incident(&first).unwrap();

        assert_eq!(
            a.replicated_incident("inc-1").unwrap(),
            Some(expected.clone())
        );
        assert_eq!(b.replicated_incident("inc-1").unwrap(), Some(expected));
    }

    #[test]
    fn incident_tombstone_cursor_and_digest_include_node_id_before_id() {
        let s = Storage::open(":memory:").unwrap();
        for (id, node_id) in [("z", "a"), ("a", "z")] {
            s.apply_incident_tombstone(&IncidentTombstone {
                id: id.into(),
                deleted_at: 101,
                node_id: node_id.into(),
            })
            .unwrap();
        }

        let digest = s.digest(0).unwrap();
        assert_eq!(digest.incident_tombstones.max_millis, 101);
        assert_eq!(digest.incident_tombstones.max_id, "z\0a");
        assert_eq!(
            s.incident_tombstones_since(101, "a", "z", 10).unwrap(),
            vec![IncidentTombstone {
                id: "a".into(),
                deleted_at: 101,
                node_id: "z".into(),
            }]
        );
    }

    #[test]
    fn incident_rows_and_tombstones_obey_one_lww_order_and_exact_digests() {
        let s = Storage::open(":memory:").unwrap();
        let mut incident = sample_replicated_incident();
        assert!(s.upsert_incident(&incident).unwrap());
        assert!(!s
            .apply_incident_tombstone(&IncidentTombstone {
                id: incident.id.clone(),
                deleted_at: 99,
                node_id: "z".into(),
            })
            .unwrap());
        assert!(s.incident(&incident.id).unwrap().is_some());

        assert!(s
            .apply_incident_tombstone(&IncidentTombstone {
                id: incident.id.clone(),
                deleted_at: 101,
                node_id: "a".into(),
            })
            .unwrap());
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.incidents.count, 0);
        assert_eq!(digest.incidents.max_millis, 0);
        assert_eq!(digest.incident_tombstones.count, 1);

        incident.version = Hlc::new(102, "a");
        incident.title = "recreated".into();
        assert!(s.upsert_incident(&incident).unwrap());
        let digest = s.digest(0).unwrap();
        assert_eq!(digest.incidents.count, 1);
        assert_eq!(digest.incidents.max_millis, 102);
        assert_eq!(digest.incident_tombstones.count, 0);
        assert!(!s
            .apply_incident_tombstone(&IncidentTombstone {
                id: incident.id.clone(),
                deleted_at: 101,
                node_id: "z".into(),
            })
            .unwrap());
        assert_eq!(
            s.incident(&incident.id).unwrap().unwrap().title,
            "recreated"
        );
    }

    #[test]
    fn incident_check_mutations_advance_versions_but_noops_do_not() {
        let s = Storage::open(":memory:").unwrap();
        s.set_incident_node_id("coordinator-a");
        s.create_incident(
            "inc",
            "Outage",
            IncidentState::Detected,
            100,
            false,
            Some(cid("a")),
            &[IncidentCheckRow {
                check_id: cid("a"),
                source: AffectSource::Origin,
            }],
            None,
        )
        .unwrap();
        let version = || s.replicated_incident("inc").unwrap().unwrap().version;
        let initial = version();
        assert_eq!(initial.node_id, "coordinator-a");

        s.attach_check("inc", cid("b"), AffectSource::Dependency)
            .unwrap();
        let attached = version();
        assert!(attached > initial);
        s.attach_check("inc", cid("b"), AffectSource::Dependency)
            .unwrap();
        assert_eq!(version(), attached);

        s.detach_check("inc", cid("b"), Some(AffectSource::Dependency))
            .unwrap();
        let detached = version();
        assert!(detached > attached);
        s.detach_check("inc", cid("b"), Some(AffectSource::Dependency))
            .unwrap();
        assert_eq!(version(), detached);

        s.set_manual_checks("inc", &[cid("c")]).unwrap();
        let manual = version();
        assert!(manual > detached);
        s.set_manual_checks("inc", &[cid("c"), cid("c")]).unwrap();
        assert_eq!(version(), manual);

        s.purge_check_everything(cid("c"), 1_000, "coordinator-a")
            .unwrap();
        assert!(
            version() > manual,
            "purge membership removal must replicate"
        );
    }

    #[test]
    fn purging_short_incidents_removes_their_stamped_alerts() {
        let s = Storage::open(":memory:").unwrap();
        seeded_incident(&s, "short", "web");
        s.add_update("short", IncidentState::Resolved, "recovered", None, 1_500)
            .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "api",
            Some(NodeState::Up),
            NodeState::Down,
            2_000,
            None,
        )
        .unwrap();

        assert_eq!(s.purge_short_incidents(1).unwrap(), 5);
        assert!(s.incident("short").unwrap().is_none());
        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
    }

    #[test]
    fn deleting_an_incident_removes_it_and_unlinks_its_alerts() {
        let s = Storage::open(":memory:").unwrap();
        seeded_incident(&s, "inc-1", "web");

        assert!(s.delete_incident("inc-1", 2000, "node-a").unwrap());
        assert!(s.incident("inc-1").unwrap().is_none());
        assert!(s.updates_for_incident("inc-1").unwrap().is_empty());

        // The alert survives. A transition is a fact; the incident was only the
        // story told about it, and deleting the story must not rewrite history.
        let alerts = s.recent_alerts(10).unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].incident_id, None);

        assert!(
            !s.delete_incident("inc-1", 3000, "node-a").unwrap(),
            "a second delete reports nothing removed"
        );
    }

    #[test]
    fn a_tombstoned_incident_cannot_be_replicated_back_in() {
        let s = Storage::open(":memory:").unwrap();
        seeded_incident(&s, "inc-1", "web");
        s.delete_incident("inc-1", 2000, "node-a").unwrap();

        s.upsert_incident(&ReplicatedIncident {
            id: "inc-1".into(),
            version: Hlc::new(1000, "a"),
            title: "Outage".into(),
            state: IncidentState::Detected,
            started_at: 1000,
            resolved_at: None,
            global: false,
            origin_check: Some(cid("web")),
            checks: vec![(cid("web"), AffectSource::Origin)],
            updates: Vec::new(),
        })
        .unwrap();

        assert!(
            s.incident("inc-1").unwrap().is_none(),
            "a peer's push resurrected a deleted incident"
        );
    }

    #[test]
    fn incident_tombstones_are_pruned_on_the_longer_window() {
        let s = Storage::open(":memory:").unwrap();
        let now = 10 * 86_400_000;
        s.delete_incident("old", now - 9 * 86_400_000, "node-a")
            .unwrap();
        s.delete_incident("recent", now - 1000, "node-a").unwrap();

        s.prune(1, 7, now).unwrap();
        let left = s.list_incident_tombstones().unwrap();
        assert_eq!(
            left,
            vec!["recent".to_string()],
            "tombstones age out on max(results_days, alerts_days), not the shorter one"
        );
    }

    #[test]
    fn replicated_incident_wire_form_is_stable() {
        let web = cid("web");
        let i = ReplicatedIncident {
            id: "inc-1".into(),
            version: Hlc::new(10, "a"),
            title: "Outage".into(),
            state: IncidentState::Investigating,
            started_at: 10,
            resolved_at: None,
            global: false,
            origin_check: Some(web),
            checks: vec![(web, AffectSource::Origin)],
            updates: vec![IncidentUpdateRow {
                id: "inc-1-u10".into(),
                incident_id: "inc-1".into(),
                state: IncidentState::Detected,
                body: "down".into(),
                author: None,
                created_at: 10,
            }],
        };
        assert_eq!(
            serde_json::to_string(&i).unwrap(),
            format!(
                r#"{{"id":"inc-1","version":{{"millis":10,"node_id":"a"}},"title":"Outage","state":"investigating","started_at":10,"resolved_at":null,"global":false,"origin_check":"{web}","checks":[["{web}","origin"]],"updates":[{{"id":"inc-1-u10","incident_id":"inc-1","state":"detected","body":"down","author":null,"created_at":10}}]}}"#
            )
        );
    }

    #[test]
    fn incident_lifecycle_open_and_resolve() {
        let s = Storage::open(":memory:").unwrap();

        assert_eq!(s.open_incident_for(cid("svc")).unwrap(), None);

        let opened = open_incident(&s, "svc", 1_000);
        assert_eq!(opened.state, IncidentState::Detected);
        assert_eq!(opened.resolved_at, None);
        assert_eq!(opened.id, "svc-1000");
        // The cause lives in the narrative, not in a column beside it.
        let updates = s.updates_for_incident("svc-1000").unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].body, "connection refused");
        assert!(updates[0].author.is_none());

        // The check is attached, so a lookup by check finds the open incident.
        assert_eq!(
            s.open_incident_for(cid("svc")).unwrap().map(|i| i.id),
            Some("svc-1000".to_string())
        );

        // Resolving is an update like any other, and it records when.
        s.add_update("svc-1000", IncidentState::Resolved, "back", None, 2_000)
            .unwrap();
        let resolved = s.incident("svc-1000").unwrap().unwrap();
        assert_eq!(resolved.state, IncidentState::Resolved);
        assert_eq!(resolved.resolved_at, Some(2_000));
        assert_eq!(
            s.open_incident_for(cid("svc")).unwrap(),
            None,
            "a resolved incident is not open"
        );

        // After resolution a new outage opens a fresh incident.
        let second = open_incident(&s, "svc", 3_000);
        assert_eq!(second.id, "svc-3000");
    }

    /// Reopening: an operator posting a non-resolved update on a closed
    /// incident must clear `resolved_at`, or it would read as open and finished
    /// at the same time.
    #[test]
    fn local_incident_updates_in_one_millisecond_are_not_lost() {
        let s = Storage::open(":memory:").unwrap();
        s.create_incident(
            "inc-1",
            "Outage",
            IncidentState::Detected,
            1_000,
            false,
            None,
            &[],
            None,
        )
        .unwrap();
        let first = s
            .add_update(
                "inc-1",
                IncidentState::Investigating,
                "investigating",
                None,
                2_000,
            )
            .unwrap();
        let second = s
            .add_update(
                "inc-1",
                IncidentState::Identified,
                "identified",
                Some("admin"),
                2_000,
            )
            .unwrap();

        assert_ne!(
            first.id, second.id,
            "distinct local updates need distinct replication ids"
        );
        let updates = s.updates_for_incident("inc-1").unwrap();
        assert_eq!(updates.len(), 2, "neither same-ms update may be lost");
        assert_eq!(
            updates
                .iter()
                .map(|u| u.id.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn reopening_an_incident_clears_its_resolution_time() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "svc", 1_000);
        s.add_update("svc-1000", IncidentState::Resolved, "back", None, 2_000)
            .unwrap();
        assert_eq!(
            s.incident("svc-1000").unwrap().unwrap().resolved_at,
            Some(2_000)
        );

        s.add_update(
            "svc-1000",
            IncidentState::Monitoring,
            "spoke too soon",
            Some("admin"),
            3_000,
        )
        .unwrap();
        let row = s.incident("svc-1000").unwrap().unwrap();
        assert_eq!(row.state, IncidentState::Monitoring);
        assert_eq!(
            row.resolved_at, None,
            "a reopened incident is not still finished"
        );
    }

    /// Acknowledgement is derived from the narrative, never stored. It becomes
    /// true on the first authored update whatever state that update announced,
    /// so an operator who jumps straight to `identified` still counts.
    #[test]
    fn acknowledgement_is_the_first_authored_update() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "svc", 1_000);
        assert!(
            !s.is_acknowledged("svc-1000").unwrap(),
            "the coordinator's own opening entry is not an acknowledgement"
        );

        s.add_update(
            "svc-1000",
            IncidentState::Identified,
            "bad deploy",
            Some("admin"),
            2_000,
        )
        .unwrap();
        assert!(
            s.is_acknowledged("svc-1000").unwrap(),
            "skipping investigating still counts as a human looking"
        );
    }

    /// `source` is what lets reconciliation retract its own attachments without
    /// disturbing an operator's, and an origin is never reclassified.
    #[test]
    fn attachments_record_how_each_check_got_there() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "db", 1_000);
        s.attach_check("db-1000", cid("api"), AffectSource::Dependency)
            .unwrap();
        s.attach_check("db-1000", cid("web"), AffectSource::Manual)
            .unwrap();

        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .map(|c| (c.check_id, c.source))
                .collect::<Vec<_>>(),
            vec![
                (cid("api"), AffectSource::Dependency),
                (cid("db"), AffectSource::Origin),
                (cid("web"), AffectSource::Manual),
            ]
        );

        // Only the origin and the dependency gate auto-resolve.
        assert_eq!(
            row.gating_checks().collect::<Vec<_>>(),
            vec![cid("api"), cid("db")]
        );

        // Re-attaching the origin as something else must not demote it.
        s.attach_check("db-1000", cid("db"), AffectSource::Manual)
            .unwrap();
        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .find(|c| c.check_id == cid("db"))
                .map(|c| c.source),
            Some(AffectSource::Origin),
            "the cause of the incident cannot be reclassified"
        );

        // Replacing the manual set leaves origin and dependency alone.
        s.set_manual_checks("db-1000", &[cid("cdn")]).unwrap();
        let row = s.incident("db-1000").unwrap().unwrap();
        assert_eq!(
            row.checks
                .iter()
                .map(|c| (c.check_id, c.source))
                .collect::<Vec<_>>(),
            vec![
                (cid("api"), AffectSource::Dependency),
                (cid("cdn"), AffectSource::Manual),
                (cid("db"), AffectSource::Origin),
            ],
            "only the manual attachment was replaced"
        );
    }

    #[test]
    fn incidents_are_listed_newest_first_and_filterable() {
        let s = Storage::open(":memory:").unwrap();
        open_incident(&s, "a", 1_000);
        open_incident(&s, "b", 2_000);
        s.add_update("a-1000", IncidentState::Resolved, "back", None, 3_500)
            .unwrap();

        let all = s.list_incidents(100, None, None).unwrap();
        let order: Vec<String> = all.iter().map(|i| i.id.clone()).collect();
        assert_eq!(order, vec!["b-2000", "a-1000"]);

        let open = s.list_incidents(100, Some(true), None).unwrap();
        assert_eq!(
            open.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["b-2000"],
            "only b is still open"
        );

        let resolved = s.list_incidents(100, Some(false), None).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].id, "a-1000");
        assert_eq!(
            s.list_incidents(100, None, Some(3_501))
                .unwrap()
                .iter()
                .map(|i| i.id.as_str())
                .collect::<Vec<_>>(),
            vec!["b-2000"],
            "old resolved incidents are hidden, even when an old outage is still open"
        );
        assert_eq!(s.list_incidents(100, None, Some(3_500)).unwrap().len(), 2);

        let for_a = s.incidents_for_check(cid("a"), 100).unwrap();
        assert_eq!(
            for_a.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["a-1000"]
        );

        // A check attached as a dependency is found by the same lookup: the
        // blast radius is what "this check's incidents" means, not the origin.
        s.attach_check("b-2000", cid("a"), AffectSource::Dependency)
            .unwrap();
        let for_a = s.incidents_for_check(cid("a"), 100).unwrap();
        assert_eq!(
            for_a.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["b-2000", "a-1000"]
        );
    }

    /// TODO.md: "evaluate indexes for alert, incident, and state-history lookup
    /// patterns." State-history is not benchmarked here: every lookup this
    /// codebase makes against it (`record_state`, `state_spans`, the prune
    /// anti-join) leads with `check_id`, which is already the first column of
    /// its declared `PRIMARY KEY (check_id, since)` — DuckDB builds an ART for
    /// every PRIMARY KEY automatically, so that table already has the index
    /// this TODO item would otherwise add. `list_status` and `uptime_strips`
    /// scan the whole table unfiltered by check_id (they want every check at
    /// once), which a `check_id`-leading index cannot help regardless.
    ///
    /// What is unindexed is the *reverse* lookups: `alerts` is scanned by
    /// `ts_millis` (`recent_alerts`, `alerts_since`, the retention delete) with
    /// only an `id` primary key, and `incident_checks` is scanned by
    /// `check_id` (`open_incident_for`, `incidents_for_check`) with only a
    /// `(incident_id, check_id)` primary key that does not lead with the
    /// column being filtered on. Both tables are bounded by `alerts_days`
    /// retention rather than by raw observation volume, so they stay orders of
    /// magnitude smaller than `check_results` — seeded here at a deliberately
    /// pessimistic upper bound: every check flapping roughly daily for the
    /// full 90-day default retention.
    #[test]
    #[ignore = "index tradeoff benchmark; configure LIGHTHOUSE_BENCH_* for the intended scale"]
    fn benchmark_alert_incident_lookup_index_tradeoff() {
        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn percentile(samples: &mut [std::time::Duration], percentile: usize) -> u128 {
            samples.sort_unstable();
            samples[(samples.len() * percentile / 100).min(samples.len() - 1)].as_millis()
        }
        fn bytes(path: &std::path::Path) -> u64 {
            std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
        }

        let alert_count = env_u64("LIGHTHOUSE_BENCH_ALERTS", 900_000);
        let incident_count = env_u64("LIGHTHOUSE_BENCH_INCIDENTS", 45_000);
        let check_pool = env_u64("LIGHTHOUSE_BENCH_CHECK_POOL", 10_000);
        let write_rounds = env_u64("LIGHTHOUSE_BENCH_ALERT_WRITE_ROUNDS", 2_000);

        struct Report {
            label: &'static str,
            seed_ms: u128,
            database_bytes: u64,
            reopen_ms: u128,
            recent_alerts_p95_ms: u128,
            alerts_since_p95_ms: u128,
            open_incident_for_p95_ms: u128,
            incidents_for_check_p95_ms: u128,
            insert_alert_rows_per_sec: f64,
        }
        let mut reports = Vec::new();

        for (label, with_index) in [("no_index", false), ("with_index", true)] {
            let path = std::env::temp_dir().join(format!(
                "lighthouse-alert-incident-index-bench-{label}-{}-{}.duckdb",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
            let path_string = path.to_string_lossy().into_owned();
            let store = Storage::open(&path_string).unwrap();
            if with_index {
                store
                    .conn
                    .execute_batch(
                        "CREATE INDEX bench_alerts_ts ON alerts (ts_millis);
                         CREATE INDEX bench_incident_checks_check ON incident_checks (check_id)",
                    )
                    .unwrap();
            }

            let seed_started = std::time::Instant::now();
            store
                .conn
                .execute_batch(&format!(
                    "INSERT INTO incidents
                         (id, title, state, started_at, resolved_at, global, origin_check,
                          version_millis, version_node)
                     SELECT 'incident-' || i::VARCHAR, 'Down',
                            CASE WHEN i % 20 = 0 THEN 'detected' ELSE 'resolved' END,
                            i * 60_000,
                            CASE WHEN i % 20 = 0 THEN NULL ELSE i * 60_000 + 30_000 END,
                            FALSE,
                            md5('check-' || (i % {check_pool})::VARCHAR)::UUID,
                            i * 60_000, 'bench-node'
                     FROM range({incident_count}) t(i);
                     INSERT INTO incident_checks (incident_id, check_id, source)
                     SELECT 'incident-' || i::VARCHAR,
                            md5('check-' || (i % {check_pool})::VARCHAR)::UUID, 'origin'
                     FROM range({incident_count}) t(i);
                     INSERT INTO alerts
                         (id, scope, node_id, previous_state, current_state, ts_millis, incident_id)
                     SELECT 'alert-' || i::VARCHAR, 'check',
                            md5('check-' || (i % {check_pool})::VARCHAR)::VARCHAR,
                            'up', CASE WHEN i % 2 = 0 THEN 'down' ELSE 'up' END, i * 1_000,
                            'incident-' || (i % {incident_count})::VARCHAR
                     FROM range({alert_count}) t(i);"
                ))
                .unwrap();
            store.conn.execute_batch("CHECKPOINT").unwrap();
            let seed_ms = seed_started.elapsed().as_millis();
            let database_bytes = bytes(&path);
            drop(store);

            let reopen_started = std::time::Instant::now();
            let store = Storage::open(&path_string).unwrap();
            let reopen_ms = reopen_started.elapsed().as_millis();

            let hot_check_id: CheckId = store
                .conn
                .query_row(
                    "SELECT check_id FROM incident_checks
                     GROUP BY check_id ORDER BY COUNT(*) DESC LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();

            let mut recent_alerts_samples = Vec::new();
            let mut alerts_since_samples = Vec::new();
            let mut open_incident_for_samples = Vec::new();
            let mut incidents_for_check_samples = Vec::new();
            for _ in 0..20 {
                let started = std::time::Instant::now();
                let recent = store.recent_alerts(50).unwrap();
                recent_alerts_samples.push(started.elapsed());
                assert_eq!(recent.len(), 50);

                let started = std::time::Instant::now();
                let since = store
                    .alerts_since(alert_count as i64 * 1_000 / 2, "", 50)
                    .unwrap();
                alerts_since_samples.push(started.elapsed());
                assert_eq!(since.len(), 50);

                let started = std::time::Instant::now();
                store.open_incident_for(hot_check_id).unwrap();
                open_incident_for_samples.push(started.elapsed());

                let started = std::time::Instant::now();
                let incidents = store.incidents_for_check(hot_check_id, 50).unwrap();
                incidents_for_check_samples.push(started.elapsed());
                assert!(!incidents.is_empty());
            }

            // Alerts are inserted one confirmed transition at a time, never in
            // bulk, so the write-cost question here is per-row `INSERT` cost
            // against an already-large table, not a bulk-load rate.
            let write_started = std::time::Instant::now();
            for i in 0..write_rounds {
                store
                    .insert_alert(
                        AlertScope::Check,
                        &format!("write-check-{i}"),
                        Some(NodeState::Up),
                        NodeState::Down,
                        alert_count as i64 * 1_000 + i as i64,
                        None,
                    )
                    .unwrap();
            }
            let write_elapsed = write_started.elapsed();
            let insert_alert_rows_per_sec = write_rounds as f64 / write_elapsed.as_secs_f64();

            reports.push(Report {
                label,
                seed_ms,
                database_bytes,
                reopen_ms,
                recent_alerts_p95_ms: percentile(&mut recent_alerts_samples, 95),
                alerts_since_p95_ms: percentile(&mut alerts_since_samples, 95),
                open_incident_for_p95_ms: percentile(&mut open_incident_for_samples, 95),
                incidents_for_check_p95_ms: percentile(&mut incidents_for_check_samples, 95),
                insert_alert_rows_per_sec,
            });

            drop(store);
            let _ = std::fs::remove_file(wal_path);
            let _ = std::fs::remove_file(path);
        }

        eprintln!(
            "alerts={alert_count} incidents={incident_count} check_pool={check_pool} \
             write_rounds={write_rounds}"
        );
        for r in &reports {
            eprintln!(
                "{label}: seed_ms={seed_ms} database_bytes={database_bytes} reopen_ms={reopen_ms} \
                 recent_alerts_p95_ms={recent_alerts_p95_ms} alerts_since_p95_ms={alerts_since_p95_ms} \
                 open_incident_for_p95_ms={open_incident_for_p95_ms} \
                 incidents_for_check_p95_ms={incidents_for_check_p95_ms} \
                 insert_alert_rows_per_sec={insert_alert_rows_per_sec:.0}",
                label = r.label,
                seed_ms = r.seed_ms,
                database_bytes = r.database_bytes,
                reopen_ms = r.reopen_ms,
                recent_alerts_p95_ms = r.recent_alerts_p95_ms,
                alerts_since_p95_ms = r.alerts_since_p95_ms,
                open_incident_for_p95_ms = r.open_incident_for_p95_ms,
                incidents_for_check_p95_ms = r.incidents_for_check_p95_ms,
                insert_alert_rows_per_sec = r.insert_alert_rows_per_sec,
            );
        }
    }
}
