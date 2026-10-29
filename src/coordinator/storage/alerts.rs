use super::*;

/// A persisted alert: one confirmed state transition. The event log, distinct
/// from an incident, which is the operator-facing record of an outage. An
/// alert names the incident it belongs to, stamped as the row is written —
/// membership is a fact the coordinator knows at that moment and no reader can
/// re-derive from a time window.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRow {
    pub id: String,
    /// What kind of thing moved. `Check` is what pages; `Zone` and `Region`
    /// rows are the event log behind it, written so the fleet view can show
    /// "us-east lost reach to X" as an event rather than as an inference.
    pub scope: AlertScope,
    /// The check, agent or zone that moved.
    pub node_id: String,
    /// `None` for a first-ever observation, which has nothing to move from.
    pub previous_state: Option<NodeState>,
    pub current_state: NodeState,
    pub timestamp: i64,
    /// The incident this transition belongs to, when it belongs to one.
    pub incident_id: Option<String>,
}

/// One notification that has not been delivered. The transition it carries is
/// everything a retry needs, so the row is self-contained: no join, and nothing
/// to dangle when the alert it came from is pruned.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedRow {
    /// Immutable delivery identity. It distinguishes transitions that share a
    /// notifier, check, and millisecond, and stays stable across retries.
    pub id: String,
    /// The routing *name*, resolved against the live notifier list at each
    /// attempt. Editing a backend therefore fixes its queued notifications;
    /// deleting it drops them, because there is no longer anyone to page.
    pub notifier: String,
    pub check_id: CheckId,
    pub previous_state: Option<NodeState>,
    pub current_state: NodeState,
    /// Failed attempts so far, first included.
    pub attempts: i32,
    /// When to try next. `None` means given up — attempts exhausted.
    pub next_attempt: Option<i64>,
    pub last_error: String,
    pub queued_at: i64,
}

/// What an alert is about. Stored rather than derived — it is not a function
/// of anything else on the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertScope {
    Check,
    Agent,
    Zone,
    Region,
}

impl AlertScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertScope::Check => "check",
            AlertScope::Agent => "agent",
            AlertScope::Zone => "zone",
            AlertScope::Region => "region",
        }
    }

    pub fn from_db(s: &str) -> AlertScope {
        match s {
            "agent" => AlertScope::Agent,
            "zone" => AlertScope::Zone,
            "region" => AlertScope::Region,
            _ => AlertScope::Check,
        }
    }
}

/// What a replicated alert did on arrival: a new event, a replay of one already
/// held, or an id whose payload disagrees with the local event of the same id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertApply {
    Inserted,
    Replay,
    Conflict,
}

impl Storage {
    /// Persist an alert: one confirmed state transition. `incident_id` is
    /// stamped here, by the caller that just decided which incident this
    /// transition belongs to.
    pub fn insert_alert(
        &self,
        scope: AlertScope,
        node_id: &str,
        previous_state: Option<NodeState>,
        current_state: NodeState,
        ts_millis: i64,
        incident_id: Option<&str>,
    ) -> Result<String> {
        let id = self.next_event_id(
            "alerts",
            &format!("{node_id}-{ts_millis}-{}", current_state.as_str()),
        )?;
        self.conn.execute_cached(
            "INSERT INTO alerts
                 (id, scope, node_id, previous_state, current_state, ts_millis, incident_id)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                id,
                scope.as_str(),
                node_id,
                previous_state.map(|s| s.as_str()),
                current_state.as_str(),
                ts_millis,
                incident_id,
            ],
        )?;
        self.record_stream_item("alerts", ts_millis, &id, 1)?;
        Ok(id)
    }

    /// Return the most recent `limit` alerts, newest first.
    pub fn recent_alerts(&self, limit: usize) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts
             ORDER BY ts_millis DESC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![limit as i64], Self::alert_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The alerts stamped as belonging to one incident, oldest first — the
    /// machine evidence behind the operator's narrative.
    pub fn alerts_for_incident(&self, incident_id: &str) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts
             WHERE incident_id = ?
             ORDER BY ts_millis ASC",
        )?;
        let rows = stmt
            .query_map(duckdb::params![incident_id], Self::alert_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Record a notification delivery intention or update its retry state.
    /// Callers creating an alert do this inside the alert transaction; a crash
    /// after commit therefore leaves an owed delivery for the next dispatcher.
    pub fn enqueue_notification(&self, row: &QueuedRow) -> Result<()> {
        self.conn.execute_cached(
            "INSERT INTO notification_queue
                 (id, notifier, check_id, previous_state, current_state,
                  attempts, next_attempt, last_error, queued_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (id) DO UPDATE SET
                 attempts = excluded.attempts,
                 next_attempt = excluded.next_attempt,
                 last_error = excluded.last_error",
            duckdb::params![
                row.id,
                row.notifier,
                row.check_id,
                row.previous_state.map(|s| s.as_str()),
                row.current_state.as_str(),
                row.attempts,
                row.next_attempt,
                row.last_error,
                row.queued_at,
            ],
        )?;
        Ok(())
    }

    /// Queued notifications whose next attempt is due, oldest first — an outage
    /// should be paged in the order it happened.
    pub fn due_notifications(&self, now_millis: i64, limit: usize) -> Result<Vec<QueuedRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, notifier, check_id, previous_state, current_state,
                    attempts, next_attempt, last_error, queued_at
             FROM notification_queue
             WHERE next_attempt IS NOT NULL AND next_attempt <= ?
             ORDER BY queued_at ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![now_millis, limit as i64], Self::queued_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Everything still undelivered, newest first, whether waiting or given up.
    /// Read by the notifier admin panel: "nobody was paged" is exactly the kind
    /// of failure this project keeps refusing to leave silent.
    pub fn undelivered_notifications(&self, limit: usize) -> Result<Vec<QueuedRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, notifier, check_id, previous_state, current_state,
                    attempts, next_attempt, last_error, queued_at
             FROM notification_queue
             ORDER BY queued_at DESC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![limit as i64], Self::queued_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn delete_notification(&self, id: &str) -> Result<()> {
        self.conn.execute_cached(
            "DELETE FROM notification_queue WHERE id = ?",
            duckdb::params![id],
        )?;
        Ok(())
    }

    pub(super) fn queued_row(row: &duckdb::Row) -> duckdb::Result<QueuedRow> {
        Ok(QueuedRow {
            id: row.get::<_, String>(0)?,
            notifier: row.get::<_, String>(1)?,
            check_id: row.get::<_, CheckId>(2)?,
            previous_state: row
                .get::<_, Option<String>>(3)?
                .as_deref()
                .map(NodeState::from_db),
            current_state: NodeState::from_db(&row.get::<_, String>(4)?),
            attempts: row.get::<_, i32>(5)?,
            next_attempt: row.get::<_, Option<i64>>(6)?,
            last_error: row.get::<_, String>(7)?,
            queued_at: row.get::<_, i64>(8)?,
        })
    }

    pub(super) fn alert_row(row: &duckdb::Row) -> duckdb::Result<AlertRow> {
        Ok(AlertRow {
            id: row.get::<_, String>(0)?,
            scope: AlertScope::from_db(&row.get::<_, String>(1)?),
            node_id: row.get::<_, String>(2)?,
            previous_state: row
                .get::<_, Option<String>>(3)?
                .as_deref()
                .map(NodeState::from_db),
            current_state: NodeState::from_db(&row.get::<_, String>(4)?),
            timestamp: row.get::<_, i64>(5)?,
            incident_id: row.get::<_, Option<String>>(6)?,
        })
    }

    /// Insert a replicated alert exactly once. An existing id is a replay only
    /// when its complete immutable payload agrees; replacing a different event
    /// would silently corrupt the event log after an id collision, so a
    /// disagreeing payload is reported as `Conflict` and nothing is written.
    ///
    /// Reported rather than raised because the caller is applying a batch:
    /// failing the transaction would make one disagreeing row permanently block
    /// every other row travelling with it, and anti-entropy would replay that
    /// same doomed batch forever.
    pub fn upsert_alert(&self, a: &AlertRow) -> Result<AlertApply> {
        let existing = self
            .conn
            .query_row_cached(
                "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts WHERE id = ?",
                duckdb::params![a.id],
                Self::alert_row,
            )
            .ok();
        if let Some(existing) = existing {
            return Ok(if existing == *a {
                AlertApply::Replay
            } else {
                AlertApply::Conflict
            });
        }
        self.conn.execute_cached(
            "INSERT INTO alerts
                 (id, scope, node_id, previous_state, current_state, ts_millis, incident_id)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                a.id,
                a.scope.as_str(),
                a.node_id,
                a.previous_state.map(|s| s.as_str()),
                a.current_state.as_str(),
                a.timestamp,
                a.incident_id,
            ],
        )?;
        self.record_stream_item("alerts", a.timestamp, &a.id, 1)?;
        Ok(AlertApply::Inserted)
    }

    /// Alerts after the exclusive `(timestamp, id)` cursor, oldest first.
    pub fn alerts_since(
        &self,
        since_ts: i64,
        since_id: &str,
        limit: usize,
    ) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, scope, node_id, previous_state, current_state, ts_millis, incident_id
             FROM alerts
             WHERE ts_millis > ? OR (ts_millis = ? AND id > ?)
             ORDER BY ts_millis ASC, id ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![since_ts, since_ts, since_id, limit as i64],
                Self::alert_row,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Three transitions, one of them stamped with an incident. Asserts the
    /// exact row shape rather than a count, so a column silently dropped from
    /// the SELECT fails here instead of surfacing as a blank cell in the UI.
    #[test]
    fn recent_alerts_round_trips_newest_first_with_exact_fields() {
        let s = Storage::open(":memory:").unwrap();
        let a1 = s
            .insert_alert(
                AlertScope::Check,
                "a",
                Some(NodeState::Up),
                NodeState::Down,
                1000,
                Some("inc-1"),
            )
            .unwrap();
        let b1 = s
            .insert_alert(
                AlertScope::Check,
                "b",
                Some(NodeState::Down),
                NodeState::Up,
                2000,
                None,
            )
            .unwrap();
        // A first-ever observation has nothing to move from.
        let a2 = s
            .insert_alert(AlertScope::Check, "a", None, NodeState::Unknown, 3000, None)
            .unwrap();

        let got = s.recent_alerts(10).unwrap();
        assert_eq!(
            got,
            vec![
                AlertRow {
                    id: a2,
                    scope: AlertScope::Check,
                    node_id: "a".into(),
                    previous_state: None,
                    current_state: NodeState::Unknown,
                    timestamp: 3000,
                    incident_id: None,
                },
                AlertRow {
                    id: b1,
                    scope: AlertScope::Check,
                    node_id: "b".into(),
                    previous_state: Some(NodeState::Down),
                    current_state: NodeState::Up,
                    timestamp: 2000,
                    incident_id: None,
                },
                AlertRow {
                    id: a1,
                    scope: AlertScope::Check,
                    node_id: "a".into(),
                    previous_state: Some(NodeState::Up),
                    current_state: NodeState::Down,
                    timestamp: 1000,
                    incident_id: Some("inc-1".into()),
                },
            ]
        );
    }

    #[test]
    fn local_alerts_with_identical_payloads_in_one_millisecond_are_not_lost() {
        let s = Storage::open(":memory:").unwrap();
        let first = s
            .insert_alert(
                AlertScope::Check,
                "web",
                Some(NodeState::Up),
                NodeState::Down,
                1_000,
                None,
            )
            .unwrap();
        let second = s
            .insert_alert(
                AlertScope::Check,
                "web",
                Some(NodeState::Up),
                NodeState::Down,
                1_000,
                None,
            )
            .unwrap();

        assert_ne!(
            first, second,
            "distinct local events need distinct replication ids"
        );
        let alerts = s.recent_alerts(10).unwrap();
        assert_eq!(alerts.len(), 2, "neither same-ms alert may be lost");
        assert_eq!(
            alerts
                .iter()
                .map(|a| a.id.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn recent_alerts_caps_to_the_newest_limit() {
        let s = Storage::open(":memory:").unwrap();
        for (node, ts) in [("a", 1000), ("b", 2000), ("a", 3000)] {
            s.insert_alert(
                AlertScope::Check,
                node,
                Some(NodeState::Up),
                NodeState::Down,
                ts,
                None,
            )
            .unwrap();
        }

        let got = s.recent_alerts(2).unwrap();
        assert_eq!(
            got.iter().map(|a| a.timestamp).collect::<Vec<_>>(),
            vec![3000, 2000],
            "the newest two, newest first"
        );
    }

    /// The evidence join: only the alerts stamped with this incident come back,
    /// oldest first. A time-window join would have swept up the unrelated one.
    #[test]
    fn alerts_for_incident_returns_only_its_own_stamped_transitions() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Up),
            NodeState::Down,
            1000,
            Some("inc-1"),
        )
        .unwrap();
        // Same instant, different incident: proves membership is the stamp and
        // not the clock.
        s.insert_alert(
            AlertScope::Check,
            "b",
            Some(NodeState::Up),
            NodeState::Down,
            1000,
            Some("inc-2"),
        )
        .unwrap();
        s.insert_alert(
            AlertScope::Check,
            "a",
            Some(NodeState::Down),
            NodeState::Up,
            2000,
            Some("inc-1"),
        )
        .unwrap();

        let got = s.alerts_for_incident("inc-1").unwrap();
        assert_eq!(
            got.iter()
                .map(|a| (a.node_id.as_str(), a.timestamp))
                .collect::<Vec<_>>(),
            vec![("a", 1000), ("a", 2000)],
            "only inc-1's transitions, oldest first"
        );
    }

    #[test]
    fn upsert_alert_replays_identical_rows_but_rejects_collisions() {
        let s = Storage::open(":memory:").unwrap();
        let a = AlertRow {
            id: "fixed-id".into(),
            scope: AlertScope::Check,
            node_id: "web".into(),
            previous_state: Some(NodeState::Up),
            current_state: NodeState::Down,
            timestamp: 1000,
            incident_id: Some("inc-1".into()),
        };
        assert_eq!(s.upsert_alert(&a).unwrap(), AlertApply::Inserted);
        assert_eq!(
            s.upsert_alert(&a).unwrap(),
            AlertApply::Replay,
            "an identical replay is a no-op"
        );
        let mut collision = a.clone();
        collision.current_state = NodeState::Up;
        assert_eq!(
            s.upsert_alert(&collision).unwrap(),
            AlertApply::Conflict,
            "a reused id must not replace an event"
        );
        assert_eq!(s.recent_alerts(10).unwrap(), vec![a]);
    }
}
