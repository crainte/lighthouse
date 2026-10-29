//! Status reads bounded by the requested page: scope, order and paging run in
//! SQL over `checks`, and per-check work (latest row, published state, uptime
//! spans) touches only the staged page, so a request costs O(page) rather
//! than O(checks).

use super::*;

/// Which visible checks a status-shaped read covers. `Primary` is the main
/// page: loose checks plus inline sections (a group with no section row
/// counts as inline). `Section` is exactly one section's checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusScope {
    Primary,
    Section(String),
    All,
}

impl StatusScope {
    pub fn from_section(section: Option<String>) -> Self {
        section.map_or(Self::Primary, Self::Section)
    }
}

/// One page of visible check ids in display order, plus the scope's total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckPage {
    pub ids: Vec<CheckId>,
    pub total: usize,
}

/// A check's status: measured, or not yet reported (carrying the published
/// state a peer may have gossiped; the caller falls back to its default).
#[derive(Debug, Clone, PartialEq)]
pub enum CheckStatus {
    Reported(StatusRow),
    Pending {
        check_id: CheckId,
        published: Option<NodeState>,
    },
}

impl CheckStatus {
    pub fn check_id(&self) -> CheckId {
        match self {
            Self::Reported(row) => row.check_id,
            Self::Pending { check_id, .. } => *check_id,
        }
    }

    /// Flatten into what every reader renders. A pending check reads as its
    /// published state, else `default`, with no measurement and no uptime.
    pub fn resolve(self, default: NodeState) -> ResolvedStatus {
        match self {
            Self::Reported(row) => ResolvedStatus {
                state: row.state,
                latency_ms: row.latency_ms,
                last_check: row.last_check,
                uptime_24h: Some(row.uptime_24h),
                status_code: row.status_code,
                error: row.error,
                failure: row.failure,
            },
            Self::Pending { published, .. } => ResolvedStatus {
                state: published.unwrap_or(default),
                latency_ms: 0,
                last_check: 0,
                uptime_24h: None,
                status_code: None,
                error: None,
                failure: None,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedStatus {
    pub state: NodeState,
    pub latency_ms: u64,
    pub last_check: i64,
    pub uptime_24h: Option<f64>,
    pub status_code: Option<u16>,
    pub error: Option<String>,
    pub failure: Option<crate::models::FailureKind>,
}

/// State counts for one section, over every visible check in it. `pending`
/// lists checks with neither a published nor a measured state, whose
/// configured default only the caller knows.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SectionStates {
    pub states: Vec<(NodeState, usize)>,
    pub pending: Vec<CheckId>,
}

impl Storage {
    pub(super) fn prepare_status(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS status_check_ids (
                 ordinal INTEGER, check_id UUID, degraded_is_downtime BOOLEAN
             );",
        )?;
        Ok(())
    }

    /// Replace `status_check_ids` with `checks`, preserving their order.
    pub(super) fn stage_check_ids(&self, checks: &[(CheckId, bool)]) -> Result<()> {
        self.conn.execute_batch("DELETE FROM status_check_ids;")?;
        let mut appender = self.conn.appender("status_check_ids")?;
        for (ordinal, (check_id, degraded)) in checks.iter().enumerate() {
            appender.append_row(duckdb::params![ordinal as i32, check_id, degraded])?;
        }
        appender.flush()?;
        Ok(())
    }

    /// Visible checks in `scope`, ordered as the status page renders them
    /// (`position`, `name`, `id`), paged in SQL.
    pub fn visible_check_page(
        &self,
        scope: &StatusScope,
        latency_graph_only: bool,
        page: usize,
        limit: usize,
    ) -> Result<CheckPage> {
        let (scope_sql, section) = match scope {
            StatusScope::All => ("TRUE", None),
            StatusScope::Primary => (
                "(c.group_name IS NULL OR COALESCE(s.display, 'inline') = 'inline')",
                None,
            ),
            StatusScope::Section(name) => ("c.group_name = ?", Some(name.as_str())),
        };
        let from = format!(
            "FROM checks c LEFT JOIN sections s ON s.name = c.group_name
             WHERE c.visible AND (c.latency_graph OR NOT {latency_graph_only}) AND {scope_sql}"
        );
        // Six shapes (scope x latency_graph_only), with paging bound rather
        // than inlined, so both statements stay in the connection's cache.
        let mut params: Vec<&dyn duckdb::ToSql> = section.iter().map(|s| s as _).collect();
        let total: i64 = self.conn.query_row_cached(
            &format!("SELECT COUNT(*) {from}"),
            params.as_slice(),
            |row| row.get(0),
        )?;
        let limit = limit.min(i64::MAX as usize);
        let offset = i64::try_from(page.saturating_mul(limit)).unwrap_or(i64::MAX);
        let limit = limit as i64;
        params.push(&limit);
        params.push(&offset);
        let ids = self
            .conn
            .prepare_cached(&format!(
                "SELECT c.id {from} ORDER BY COALESCE(c.position, 0), c.name, c.id
                 LIMIT ? OFFSET ?"
            ))?
            .query_map(params.as_slice(), |row| row.get::<_, CheckId>(0))?
            .collect::<std::result::Result<_, _>>()?;
        Ok(CheckPage {
            ids,
            total: total as usize,
        })
    }

    /// Status for exactly `checks`, in the given order. Each entry carries the
    /// check's resolved `degraded_is_downtime`, which decides whether Degraded
    /// time counts against its 24h uptime.
    ///
    /// The displayed state is the published `current_state`, falling back to
    /// the raw latest status; uptime is time-weighted over `state_history`
    /// spans in `(now-24h, now]`, reading spans for the staged checks only.
    pub fn status_for(
        &self,
        checks: &[(CheckId, bool)],
        now_millis: i64,
    ) -> Result<Vec<CheckStatus>> {
        let window_start = now_millis - 86_400_000;
        self.stage_check_ids(checks)?;
        let mut spans_by_check = BTreeMap::<CheckId, Vec<StateSpan>>::new();
        {
            let mut stmt = self.conn.prepare_cached(
                "SELECT h.check_id, h.state, h.since
                 FROM state_history h SEMI JOIN status_check_ids i ON i.check_id = h.check_id
                 WHERE h.since >= ?
                 UNION ALL
                 SELECT h.check_id, arg_max(h.state, h.since), MAX(h.since)
                 FROM state_history h SEMI JOIN status_check_ids i ON i.check_id = h.check_id
                 WHERE h.since < ?
                 GROUP BY h.check_id
                 ORDER BY 1, 3",
            )?;
            let spans = stmt.query_map(duckdb::params![window_start, window_start], |row| {
                Ok((
                    row.get::<_, CheckId>(0)?,
                    StateSpan {
                        state: NodeState::from_db(&row.get::<_, String>(1)?),
                        since: row.get::<_, i64>(2)?,
                    },
                ))
            })?;
            for span in spans {
                let (check_id, span) = span?;
                spans_by_check.entry(check_id).or_default().push(span);
            }
        }
        let mut stmt = self.conn.prepare_cached(
            "SELECT i.check_id, i.degraded_is_downtime, cs.status, latest.status,
                    latest.latency_ms, latest.ts_millis, latest.status_code,
                    latest.error, latest.failure
             FROM status_check_ids i
             LEFT JOIN latest_check_results latest ON latest.check_id = i.check_id
             LEFT JOIN current_state cs ON cs.check_id = i.check_id
             ORDER BY i.ordinal",
        )?;
        let rows = stmt.query_map([], |row| {
            let check_id = row.get::<_, CheckId>(0)?;
            let degraded_counts = row.get::<_, bool>(1)?;
            let published = row.get::<_, Option<String>>(2)?;
            let Some(latest_status) = row.get::<_, Option<String>>(3)? else {
                return Ok(CheckStatus::Pending {
                    check_id,
                    published: published.as_deref().map(NodeState::from_db),
                });
            };
            Ok(CheckStatus::Reported(StatusRow {
                check_id,
                state: NodeState::from_db(published.as_deref().unwrap_or(&latest_status)),
                latency_ms: row.get::<_, i64>(4)? as u64,
                last_check: row.get::<_, i64>(5)?,
                // No spans in the window reads as fully up: an absence of
                // recorded outage is not an outage.
                uptime_24h: spans_by_check
                    .get(&check_id)
                    .and_then(|spans| {
                        uptime_counts(spans, window_start, now_millis, degraded_counts)
                    })
                    .map_or(1.0, |(up_ms, known_ms)| up_ms as f64 / known_ms as f64),
                status_code: row.get::<_, Option<i32>>(6)?.map(|c| c as u16),
                error: row.get::<_, Option<String>>(7)?,
                failure: row
                    .get::<_, Option<String>>(8)?
                    .and_then(|f| serde_json::from_str(&f).ok()),
            }))
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Per-section state counts over every visible check, aggregated in SQL so
    /// section tiles cost one grouped scan of the catalogue, not a status
    /// build per check.
    pub fn section_states(&self) -> Result<BTreeMap<String, SectionStates>> {
        let mut sections = BTreeMap::<String, SectionStates>::new();
        let mut stmt = self.conn.prepare_cached(
            "SELECT c.group_name, COALESCE(cs.status, latest.status) AS state, COUNT(*),
                    CASE WHEN COALESCE(cs.status, latest.status) IS NULL
                         THEN string_agg(c.id::VARCHAR, ',') END
             FROM checks c
             LEFT JOIN current_state cs ON cs.check_id = c.id
             LEFT JOIN latest_check_results latest ON latest.check_id = c.id
             WHERE c.visible AND c.group_name IS NOT NULL
             GROUP BY 1, 2",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (section, state, count, pending) = row?;
            let entry = sections.entry(section).or_default();
            match state {
                Some(state) => entry
                    .states
                    .push((NodeState::from_db(&state), count as usize)),
                None => entry.pending.extend(
                    pending
                        .unwrap_or_default()
                        .split(',')
                        .filter_map(|id| id.trim().parse::<CheckId>().ok()),
                ),
            }
        }
        Ok(sections)
    }

    /// Every check with a measurement, using each check's stored
    /// `degraded_is_downtime`. Tests read status through this; the routes
    /// page through `visible_check_page` + `status_for`.
    #[cfg(test)]
    pub fn list_status(&self, now_millis: i64) -> Result<Vec<StatusRow>> {
        let degraded: std::collections::HashSet<CheckId> = self
            .list_checks()?
            .into_iter()
            .filter(|c| c.degraded_is_downtime.unwrap_or(false))
            .map(|c| c.id)
            .collect();
        let ids: Vec<(CheckId, bool)> = self
            .conn
            .prepare_cached("SELECT check_id FROM latest_check_results ORDER BY check_id")?
            .query_map([], |row| row.get::<_, CheckId>(0))?
            .map(|id| id.map(|id| (id, degraded.contains(&id))))
            .collect::<std::result::Result<_, _>>()?;
        Ok(self
            .status_for(&ids, now_millis)?
            .into_iter()
            .filter_map(|status| match status {
                CheckStatus::Reported(row) => Some(row),
                CheckStatus::Pending { .. } => None,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::storage::test_support::{cid, result};

    fn check(name: &str, position: i32, group: Option<&str>, visible: bool) -> CheckConfig {
        CheckConfig {
            id: cid(name),
            name: name.into(),
            target: "https://example.test".into(),
            position,
            group: group.map(str::to_string),
            visible,
            ..Default::default()
        }
    }

    #[test]
    fn visible_check_page_scopes_orders_and_pages_in_sql() {
        let s = Storage::open(":memory:").unwrap();
        s.create_section("inline", false, SectionDisplay::Inline)
            .unwrap();
        s.create_section("paged", false, SectionDisplay::Page)
            .unwrap();
        let checks = [
            check("b", 1, None, true),
            check("a", 1, None, true),
            check("c", 0, Some("inline"), true),
            check("d", 2, Some("paged"), true),
            check("e", 3, Some("orphan"), true),
            check("hidden", 0, None, false),
        ];
        for c in &checks {
            s.upsert_check(c, 1).unwrap();
        }
        let ids = |names: &[&str]| -> Vec<CheckId> {
            names
                .iter()
                .map(|n| checks.iter().find(|c| c.name == *n).unwrap().id)
                .collect()
        };

        let first = s
            .visible_check_page(&StatusScope::Primary, false, 0, 2)
            .unwrap();
        assert_eq!(
            first,
            CheckPage {
                ids: ids(&["c", "a"]),
                total: 4
            }
        );
        let second = s
            .visible_check_page(&StatusScope::Primary, false, 1, 2)
            .unwrap();
        assert_eq!(
            second,
            CheckPage {
                ids: ids(&["b", "e"]),
                total: 4
            }
        );
        let past = s
            .visible_check_page(&StatusScope::Primary, false, 9, 2)
            .unwrap();
        assert_eq!(
            past,
            CheckPage {
                ids: vec![],
                total: 4
            }
        );

        let paged = s
            .visible_check_page(&StatusScope::Section("paged".into()), false, 0, 10)
            .unwrap();
        assert_eq!(
            paged,
            CheckPage {
                ids: ids(&["d"]),
                total: 1
            }
        );
        assert_eq!(
            s.visible_check_page(&StatusScope::All, false, 0, 10)
                .unwrap()
                .total,
            5
        );
    }

    #[test]
    fn status_for_returns_requested_checks_in_order_with_pending_entries() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let reported = check("reported", 0, None, true);
        let gossiped = check("gossiped", 1, None, true);
        let silent = check("silent", 2, None, true);
        for c in [&reported, &gossiped, &silent] {
            s.upsert_check(c, 1).unwrap();
        }
        s.insert_results(&[(result("reported", now - 1_000, 12), Status::Up)])
            .unwrap();
        s.update_current_state(gossiped.id, NodeState::Down, None, now)
            .unwrap();

        let statuses = s
            .status_for(
                &[
                    (silent.id, false),
                    (gossiped.id, false),
                    (reported.id, false),
                ],
                now,
            )
            .unwrap();
        assert_eq!(statuses.len(), 3);
        assert_eq!(
            statuses[0],
            CheckStatus::Pending {
                check_id: silent.id,
                published: None
            }
        );
        assert_eq!(
            statuses[1],
            CheckStatus::Pending {
                check_id: gossiped.id,
                published: Some(NodeState::Down)
            }
        );
        match &statuses[2] {
            CheckStatus::Reported(row) => {
                assert_eq!(
                    (row.check_id, row.state, row.latency_ms),
                    (reported.id, NodeState::Up, 12)
                );
            }
            other => panic!("expected a reported status, got {other:?}"),
        }
    }

    #[test]
    fn section_states_count_every_visible_check_and_list_pending_ones() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let down = check("down", 0, Some("web"), true);
        let up = check("up", 1, Some("web"), true);
        let pending = check("pending", 2, Some("web"), true);
        let hidden = check("hidden", 3, Some("web"), false);
        for c in [&down, &up, &pending, &hidden] {
            s.upsert_check(c, 1).unwrap();
        }
        s.update_current_state(down.id, NodeState::Down, None, now)
            .unwrap();
        s.update_current_state(up.id, NodeState::Up, None, now)
            .unwrap();
        s.update_current_state(hidden.id, NodeState::Down, None, now)
            .unwrap();

        let mut web = s.section_states().unwrap().remove("web").unwrap();
        web.states.sort_by_key(|(state, _)| state.as_str());
        assert_eq!(
            web,
            SectionStates {
                states: vec![(NodeState::Down, 1), (NodeState::Up, 1)],
                pending: vec![pending.id],
            }
        );
    }
}
