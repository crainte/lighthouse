use crate::config::{CheckConfig, Expect};
use crate::models::{CheckKind, CheckResult, Status};
use anyhow::Result;
use duckdb::Connection;
use std::collections::BTreeMap;

/// DuckDB-backed store for check results and materialized current state.
/// Single connection guarded by the caller; DuckDB is single-writer.
pub struct Storage {
    conn: Connection,
}

/// One row of check history. Multiple agents probe the same check, so rows are
/// only unique by (timestamp, agent_id) — timestamp alone collides.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRow {
    pub timestamp: i64, // unix millis
    pub agent_id: String,
    pub status: Status,
    pub latency_ms: u64,
    /// HTTP status code observed, when the probe was HTTP.
    pub status_code: Option<u16>,
    /// Why the probe failed, verbatim from the agent. `None` on success.
    /// Never publish this: it quotes what the probe saw.
    pub error: Option<String>,
    /// The same failure as a category, safe to publish. `None` for rows written
    /// before probes classified themselves.
    pub failure: Option<crate::models::FailureKind>,
}

/// Current status of a check plus recent uptime, for the status grid.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusRow {
    pub check_id: String,
    pub status: Status,
    pub latency_ms: u64,
    pub last_check: i64, // unix millis of latest result
    pub uptime_24h: f64, // fraction 0.0..=1.0 of UP results in the 24h before now
    /// Status code from the most recent result.
    pub status_code: Option<u16>,
    /// Failure reason from the most recent result, so the dashboard can say why
    /// a check is down rather than only that it is. Never publish it verbatim.
    pub error: Option<String>,
    /// That same failure as a category, safe to publish.
    pub failure: Option<crate::models::FailureKind>,
}

/// The coordinator's CONFIRMED decision per check, materialized so the status
/// page and a restarted process agree with the in-memory tracker.
#[derive(Debug, Clone, PartialEq)]
pub struct CurrentStateRow {
    pub check_id: String,
    pub status: Status,
    /// Consecutive DOWN observations toward `failures_before_down`.
    pub consecutive_failures: u32,
}

/// A probe agent that has reported results, derived from observed `agent_id`s.
/// There is no agent registry — an agent exists because it sent data.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRow {
    pub agent_id: String,
    /// Where it last reported from. Read from its most recent result rather
    /// than a registry, for the same reason the agent list itself is: an agent
    /// that has been moved should be shown where it actually is now.
    pub zone: String,
    pub region: String,
    pub last_seen: i64, // unix millis of its most recent result
    pub checks_reporting: u32,
    /// Results it submitted in the last 24h, as a rough liveness signal.
    pub results_24h: u32,
}

/// A persisted alert (state transition worth surfacing).
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRow {
    pub check_id: String,
    pub from: Status,
    pub to: Status,
    pub timestamp: i64,
}

/// One outage for a check: opened when the check first reads DOWN, resolved
/// when it reads UP again. A check has at most one open incident at a time;
/// further DOWN observations while it is open extend nothing, the `alerts`
/// table keeps the per-transition detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncidentStatus {
    Open,
    Resolved,
}

impl IncidentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            IncidentStatus::Open => "open",
            IncidentStatus::Resolved => "resolved",
        }
    }

    pub fn from_db(s: &str) -> IncidentStatus {
        match s {
            "resolved" => IncidentStatus::Resolved,
            _ => IncidentStatus::Open,
        }
    }
}

/// The span of an outage, as the UI reads it back.
#[derive(Debug, Clone, PartialEq)]
pub struct IncidentRow {
    pub id: String,
    pub check_id: String,
    pub status: IncidentStatus,
    /// When the check first read DOWN, unix millis.
    pub started_at: i64,
    /// When it read UP again; `None` while the incident is open.
    pub resolved_at: Option<i64>,
    /// Why it went down: the failure kind when one is known, else the probe's
    /// own words. Recorded once, at open.
    pub details: Option<String>,
    /// When an admin acknowledged the outage, unix millis. `None` until then.
    pub acknowledged_at: Option<i64>,
    /// The admin session name that acknowledged it, if one was recorded.
    pub acknowledged_by: Option<String>,
}

/// One segment of the uptime strip. Not necessarily a day: the window is split
/// into a fixed number of segments, so a 7-day view has ~2-hour segments.
#[derive(Debug, Clone, PartialEq)]
pub struct UptimeBucket {
    /// Exclusive lower bound of the segment, unix millis.
    pub start: i64,
    /// Inclusive upper bound, unix millis. Handed to the UI so it can query the
    /// exact window a bar covers without re-deriving the segment width.
    pub end: i64,
    /// Fraction 0.0..=1.0, or None when the segment has no data at all.
    pub uptime: Option<f64>,
    pub up: u32,
    pub total: u32,
}

/// A single latency data point for the sparkline chart.
#[derive(Debug, Clone, PartialEq)]
pub struct LatencyPoint {
    pub timestamp: i64, // unix millis of the window start
    /// Mean latency across the segment, or `None` when nothing was recorded in
    /// it. A segment with no attempts is not a 0ms response, and collapsing the
    /// two paints an imaginary flat line along the axis.
    pub avg_latency_ms: Option<f64>,
}

/// A named, ordered group on the status page. Stored separately from checks so
/// it can exist while empty and be renamed without touching what is in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    pub name: String,
    pub position: i32,
    /// Whether the public page renders this section collapsed on first load.
    pub collapsed: bool,
}

/// Express a plain interval as the equivalent cron, for migrating rows written
/// when checks had an `interval_secs` instead of a schedule.
fn seconds_to_cron(secs: u64) -> String {
    match secs {
        0..=59 => format!("*/{} * * * * *", secs.max(1)),
        s if s % 3600 == 0 && s < 86_400 => format!("0 0 */{} * * *", s / 3600),
        s if s % 60 == 0 && s < 3600 => format!("0 */{} * * * *", s / 60),
        // Anything unusual keeps its cadence approximately, in minutes.
        s => format!("0 */{} * * * *", (s / 60).clamp(1, 59)),
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

/// Reliability and latency spread over one window.
///
/// `min`/`avg`/`max` cover *successful* attempts only: a failed probe's
/// recorded duration is however long it took to give up, not a measurement of
/// the service, and letting timeouts in makes the spread meaningless.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LatencyBand {
    pub attempts: u32,
    pub up: u32,
    pub min_ms: Option<u64>,
    pub max_ms: Option<u64>,
    sum_ms: f64,
}

impl LatencyBand {
    fn observe(&mut self, latency_ms: u64, up: bool) {
        self.attempts += 1;
        if !up {
            return;
        }
        self.up += 1;
        self.sum_ms += latency_ms as f64;
        self.min_ms = Some(self.min_ms.map_or(latency_ms, |m| m.min(latency_ms)));
        self.max_ms = Some(self.max_ms.map_or(latency_ms, |m| m.max(latency_ms)));
    }

    fn merge(&mut self, other: &Self) {
        self.attempts += other.attempts;
        self.up += other.up;
        self.sum_ms += other.sum_ms;
        self.min_ms = match (self.min_ms, other.min_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_ms = self.max_ms.max(other.max_ms);
    }

    /// Mean latency of the successful attempts, or `None` when none passed.
    pub fn avg_ms(&self) -> Option<f64> {
        (self.up > 0).then(|| self.sum_ms / self.up as f64)
    }
}

/// One agent's share of a window. Several agents probe the same check, so
/// "which agent was failing?" is only answerable per agent.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentBand {
    pub agent_id: String,
    pub band: LatencyBand,
}

/// An empty location is the same as none: an agent that sent "" told us nothing,
/// and storing it would create a second spelling of unknown.
fn non_empty(s: &str) -> Option<&str> {
    (!s.trim().is_empty()).then_some(s)
}

/// Latency shape over a window plus exact per-agent attribution for it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LatencySeries {
    pub points: Vec<LatencyPoint>,
    pub overall: LatencyBand,
    pub agents: Vec<AgentBand>,
}

impl Storage {
    /// Open (or create) the database at `path` and ensure the schema exists.
    /// `path` may be ":memory:" for tests.
    pub fn open(path: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS check_results (
                check_id   VARCHAR NOT NULL,
                agent_id   VARCHAR NOT NULL,
                ts_millis  BIGINT  NOT NULL,
                status     VARCHAR NOT NULL,
                latency_ms BIGINT  NOT NULL,
                status_code INTEGER,
                error      VARCHAR,
                -- Where the probe ran. Nullable: results recorded before agents
                -- declared a location read as unknown rather than being guessed.
                zone       VARCHAR,
                region     VARCHAR,
                -- Why it failed, as a value. The public page renders this rather
                -- than the free text, which quotes whatever the probe saw.
                failure    VARCHAR
            );
            CREATE TABLE IF NOT EXISTS alerts (
                check_id   VARCHAR NOT NULL,
                from_state VARCHAR NOT NULL,
                to_state   VARCHAR NOT NULL,
                ts_millis  BIGINT  NOT NULL
            );
            -- Outage lifecycle, one row per down-period. Opened when a check
            -- first reads DOWN, resolved when it reads UP again. Distinct from
            -- `alerts`: an alert is a single state transition; an incident is
            -- the span those transitions belong to, so the UI can ask whether
            -- a check is down right now and how long it was down.
            CREATE TABLE IF NOT EXISTS incidents (
                id          VARCHAR PRIMARY KEY,
                check_id    VARCHAR NOT NULL,
                status      VARCHAR NOT NULL,
                started_at  BIGINT  NOT NULL,
                resolved_at BIGINT,
                details     VARCHAR,
                acknowledged_at BIGINT,
                acknowledged_by VARCHAR
            );
            -- The CONFIRMED state per check, materialized for fast reads. The
            -- raw `check_results` table keeps every observation; this table
            -- holds what the coordinator decided they mean — a check with
            -- `failures_before_down` = 3 stays 'up' here through its first two
            -- failed probes. Read by the status page, seeded into the in-memory
            -- tracker on startup so a restart resumes a confirmation in flight.
            CREATE TABLE IF NOT EXISTS current_state (
                check_id   VARCHAR PRIMARY KEY,
                status     VARCHAR NOT NULL,
                last_check BIGINT  NOT NULL,
                consecutive_failures INTEGER NOT NULL DEFAULT 0
            );
            -- Check definitions, editable at runtime from the admin UI. Seeded
            -- from the config file on first boot; the table is authoritative
            -- afterwards, so an edit isn't silently reverted by a restart.
            -- `group` is a SQL keyword, hence group_name.
            CREATE TABLE IF NOT EXISTS checks (
                name        VARCHAR PRIMARY KEY,
                kind        VARCHAR NOT NULL,
                target      VARCHAR NOT NULL,
                interval_secs BIGINT NOT NULL,
                timeout_secs  BIGINT NOT NULL,
                group_name  VARCHAR,
                latency_graph BOOLEAN NOT NULL,
                enabled     BOOLEAN NOT NULL,
                -- Published on the public status page. Separate from `enabled`:
                -- a check can be probed and alerted on without being shown.
                visible     BOOLEAN NOT NULL DEFAULT TRUE,
                -- Lua deciding UP/DOWN. NULL means the built-in rule.
                eval        VARCHAR,
                expect_status INTEGER,
                expect_body_contains VARCHAR,
                updated_at  BIGINT  NOT NULL
            );
            -- Status-page sections. A row here, rather than just a string on
            -- each check, so a section can be created empty, renamed, and
            -- ordered independently of what is in it.
            CREATE TABLE IF NOT EXISTS sections (
                name     VARCHAR PRIMARY KEY,
                position INTEGER NOT NULL,
                collapsed BOOLEAN DEFAULT FALSE
            );",
        )?;
        // Columns added after the first release. DuckDB has no
        // ADD COLUMN IF NOT EXISTS, so ask what already exists and add only
        // what's missing. Errors are NOT swallowed: an earlier version of this
        // blanket-ignored them and silently half-migrated, because DuckDB
        // rejects NOT NULL on ADD COLUMN — the coordinator then crash-looped on
        // a column it thought it had added.
        let existing: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'checks'",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        for (column, ddl) in [
            (
                "visible",
                "ALTER TABLE checks ADD COLUMN visible BOOLEAN DEFAULT TRUE",
            ),
            ("eval", "ALTER TABLE checks ADD COLUMN eval VARCHAR"),
            (
                "position",
                "ALTER TABLE checks ADD COLUMN position INTEGER DEFAULT 0",
            ),
            // JSON rather than a comma-joined string: an agent id is arbitrary
            // config text and a separator inside one would silently split it.
            ("agents", "ALTER TABLE checks ADD COLUMN agents VARCHAR"),
            // The whole definition, serialized. A check now carries per-type
            // option blocks, headers and schedules; a column per field was a
            // migration treadmill, so the document is authoritative and the
            // flat columns below survive only for SQL to filter and order by.
            ("config", "ALTER TABLE checks ADD COLUMN config VARCHAR"),
        ] {
            if !existing.iter().any(|c| c == column) {
                conn.execute_batch(ddl)?;
            }
        }
        let section_columns: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'sections'",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let result_columns: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'check_results'",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        if !result_columns.iter().any(|c| c == "failure") {
            conn.execute_batch("ALTER TABLE check_results ADD COLUMN failure VARCHAR")?;
        }
        for column in ["zone", "region"] {
            if !result_columns.iter().any(|c| c == column) {
                conn.execute_batch(&format!(
                    "ALTER TABLE check_results ADD COLUMN {column} VARCHAR"
                ))?;
            }
        }
        if !section_columns.iter().any(|c| c == "collapsed") {
            conn.execute_batch("ALTER TABLE sections ADD COLUMN collapsed BOOLEAN DEFAULT FALSE")?;
        }
        // Acknowledgment was added after the first release; old databases get
        // the columns with NULL meaning "not acknowledged".
        let incident_columns: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'incidents'",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        if !incident_columns.iter().any(|c| c == "acknowledged_at") {
            // Nullable BIGINT is fine here (DuckDB only rejects NOT NULL on
            // ADD COLUMN); NULL reads as "not acknowledged".
            conn.execute_batch("ALTER TABLE incidents ADD COLUMN acknowledged_at BIGINT")?;
        }
        if !incident_columns.iter().any(|c| c == "acknowledged_by") {
            conn.execute_batch("ALTER TABLE incidents ADD COLUMN acknowledged_by VARCHAR")?;
        }
        // The added columns are nullable (DuckDB won't take NOT NULL here), so
        // give pre-existing rows the intended value rather than letting a NULL
        // reach a non-optional read.
        conn.execute_batch(
            "UPDATE checks SET visible = TRUE WHERE visible IS NULL;
             UPDATE checks SET position = 0 WHERE position IS NULL;",
        )?;
        let store = Self { conn };
        store.backfill_configs()?;
        Ok(store)
    }

    /// Give rows written before the `config` document a document, built from the
    /// flat columns they do have. Without this an older database would read back
    /// as checks with default schedules and no targets.
    fn backfill_configs(&self) -> Result<()> {
        let legacy: Vec<CheckConfig> = {
            let mut stmt = self.conn.prepare(
                "SELECT name, kind, target, interval_secs, timeout_secs, group_name,
                        latency_graph, enabled, expect_status, expect_body_contains,
                        visible, eval, position, agents
                 FROM checks WHERE config IS NULL",
            )?;
            let rows = stmt.query_map([], |row| {
                let interval = row.get::<_, i64>(3)?.max(5);
                Ok(CheckConfig {
                    name: row.get(0)?,
                    kind: CheckKind::from_db(&row.get::<_, String>(1)?),
                    target: row.get(2)?,
                    // An interval becomes the equivalent cron, so an upgrade
                    // doesn't silently change how often anything runs.
                    cron: seconds_to_cron(interval as u64),
                    timeout_secs: row.get::<_, i64>(4)? as u64,
                    group: row.get::<_, Option<String>>(5)?,
                    latency_graph: row.get(6)?,
                    enabled: row.get(7)?,
                    expect: Expect {
                        status: row.get::<_, Option<i32>>(8)?.map(|s| s as u16),
                        body_contains: row.get(9)?,
                    },
                    visible: row.get(10)?,
                    eval: row.get::<_, Option<String>>(11)?,
                    position: row.get::<_, Option<i32>>(12)?.unwrap_or(0),
                    agents: row
                        .get::<_, Option<String>>(13)?
                        .and_then(|raw| serde_json::from_str(&raw).ok())
                        .unwrap_or_default(),
                    ..Default::default()
                })
            })?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        for check in legacy {
            let json = serde_json::to_string(&check)?;
            self.conn.execute(
                "UPDATE checks SET config = ? WHERE name = ?",
                duckdb::params![json, check.name],
            )?;
        }
        Ok(())
    }

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

    /// Create or replace a check by name.
    ///
    /// `config` is the definition; the other columns are a projection of it so
    /// SQL can order and filter without parsing every document.
    pub fn upsert_check(&self, c: &CheckConfig, now_millis: i64) -> Result<()> {
        // DuckDB's ON CONFLICT needs the same column list twice; deleting first
        // is shorter and the two statements are cheap at this table's size.
        self.conn
            .execute("DELETE FROM checks WHERE name = ?", duckdb::params![c.name])?;
        self.conn.execute(
            "INSERT INTO checks
                (name, kind, target, interval_secs, timeout_secs, group_name,
                 latency_graph, enabled, expect_status, expect_body_contains,
                 visible, eval, position, agents, config, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
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

    /// Apply an explicit display order. Names not listed keep their relative
    /// order after the listed ones.
    pub fn reorder_checks(&self, order: &[String], now_millis: i64) -> Result<()> {
        // Rewrite whole rows rather than UPDATE-ing the position column: the
        // document is authoritative, so touching only the column would reorder
        // the index and leave every reader seeing the old order.
        let mut checks = self.list_checks()?;
        let rank = |name: &str| {
            order
                .iter()
                .position(|n| n == name)
                // Names the caller didn't mention keep their relative order
                // after the ones it did.
                .unwrap_or(usize::MAX)
        };
        checks.sort_by(|a, b| {
            rank(&a.name)
                .cmp(&rank(&b.name))
                .then_with(|| a.position.cmp(&b.position))
                .then_with(|| a.name.cmp(&b.name))
        });
        for (i, mut check) in checks.into_iter().enumerate() {
            check.position = i as i32;
            self.upsert_check(&check, now_millis)?;
        }
        Ok(())
    }

    /// Delete every result an agent reported. The agent list is derived from
    /// these rows, so this is what makes an agent disappear.
    pub fn purge_agent(&self, agent_id: &str) -> Result<u64> {
        let rows = self.conn.execute(
            "DELETE FROM check_results WHERE agent_id = ?",
            duckdb::params![agent_id],
        )?;
        Ok(rows as u64)
    }

    /// Delete a check's recorded results and alerts, keeping its definition.
    /// Returns how many result rows went, so the UI can say what it did.
    pub fn purge_check_data(&self, name: &str) -> Result<u64> {
        let rows = self.conn.execute(
            "DELETE FROM check_results WHERE check_id = ?",
            duckdb::params![name],
        )?;
        self.conn.execute(
            "DELETE FROM alerts WHERE check_id = ?",
            duckdb::params![name],
        )?;
        Ok(rows as u64)
    }

    /// Returns whether a row was actually removed, so the handler can 404
    /// instead of reporting success for a name that never existed.
    pub fn delete_check(&self, name: &str) -> Result<bool> {
        let n = self
            .conn
            .execute("DELETE FROM checks WHERE name = ?", duckdb::params![name])?;
        Ok(n > 0)
    }

    /// Adopt the config file's checks the first time we run against an empty
    /// table. Existing deployments keep their checks; afterwards the table wins,
    /// so an admin edit survives a restart instead of being overwritten by the
    /// file it came from.
    pub fn seed_checks_if_empty(&self, checks: &[CheckConfig], now_millis: i64) -> Result<bool> {
        let existing: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM checks", [], |r| r.get(0))?;
        if existing > 0 || checks.is_empty() {
            return Ok(false);
        }
        for c in checks {
            self.upsert_check(c, now_millis)?;
        }
        Ok(true)
    }

    /// Sections in display order.
    pub fn list_sections(&self) -> Result<Vec<Section>> {
        let mut stmt = self
            .conn
            .prepare("SELECT name, position, collapsed FROM sections ORDER BY position, name")?;
        let rows = stmt.query_map([], |row| {
            Ok(Section {
                name: row.get(0)?,
                position: row.get::<_, i32>(1)?,
                collapsed: row.get::<_, Option<bool>>(2)?.unwrap_or(false),
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Create a section at the end. Returns false when the name is taken.
    pub fn create_section(&self, name: &str, collapsed: bool) -> Result<bool> {
        let exists: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sections WHERE name = ?",
            duckdb::params![name],
            |r| r.get(0),
        )?;
        if exists > 0 {
            return Ok(false);
        }
        let next: i32 = self.conn.query_row(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM sections",
            [],
            |r| r.get(0),
        )?;
        self.conn.execute(
            "INSERT INTO sections (name, position, collapsed) VALUES (?, ?, ?)",
            duckdb::params![name, next, collapsed],
        )?;
        Ok(true)
    }

    /// Rename a section, carrying its checks with it. Without the second update
    /// every check in the section would silently become loose.
    pub fn rename_section(&self, from: &str, to: &str, now_millis: i64) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE sections SET name = ? WHERE name = ?",
            duckdb::params![to, from],
        )?;
        if n == 0 {
            return Ok(false);
        }
        // Each affected check is rewritten whole: its group lives in the
        // document, so updating only the column would leave readers on the old
        // name and the check apparently orphaned.
        for mut check in self.list_checks()? {
            if check.group.as_deref() == Some(from) {
                check.group = Some(to.to_string());
                self.upsert_check(&check, now_millis)?;
            }
        }
        Ok(true)
    }

    /// Set whether a section starts collapsed on the public page.
    pub fn set_section_collapsed(&self, name: &str, collapsed: bool) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE sections SET collapsed = ? WHERE name = ?",
            duckdb::params![collapsed, name],
        )?;
        Ok(n > 0)
    }

    /// Delete a section. Its checks are kept and become loose — deleting a
    /// heading must never delete the monitoring underneath it.
    pub fn delete_section(&self, name: &str, now_millis: i64) -> Result<bool> {
        let n = self
            .conn
            .execute("DELETE FROM sections WHERE name = ?", duckdb::params![name])?;
        if n == 0 {
            return Ok(false);
        }
        for mut check in self.list_checks()? {
            if check.group.as_deref() == Some(name) {
                check.group = None;
                self.upsert_check(&check, now_millis)?;
            }
        }
        Ok(true)
    }

    /// Apply an explicit order. Names not listed keep their relative order after
    /// the listed ones, so a stale client can't silently drop a section.
    pub fn reorder_sections(&self, order: &[String]) -> Result<()> {
        let mut position = 0i32;
        for name in order {
            self.conn.execute(
                "UPDATE sections SET position = ? WHERE name = ?",
                duckdb::params![position, name],
            )?;
            position += 1;
        }
        // Anything the caller didn't mention is appended in its existing order,
        // so a client working from a stale list can't strand a section at
        // position 0 alongside the ones it did name.
        let remaining: Vec<String> = self
            .list_sections()?
            .into_iter()
            .map(|s| s.name)
            .filter(|n| !order.contains(n))
            .collect();
        for name in remaining {
            self.conn.execute(
                "UPDATE sections SET position = ? WHERE name = ?",
                duckdb::params![position, name],
            )?;
            position += 1;
        }
        Ok(())
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
            self.create_section(&name, false)?;
        }
        Ok(())
    }

    /// Persist a batch of raw results with their evaluated status.
    pub fn insert_results(&self, rows: &[(CheckResult, Status)]) -> Result<()> {
        // One transaction for the whole batch: DuckDB otherwise commits per
        // statement, which makes large seeds (100k+ rows) crawl.
        self.conn.execute_batch("BEGIN")?;
        let insert = || -> Result<()> {
            let mut stmt = self.conn.prepare(
                "INSERT INTO check_results
                    (check_id, agent_id, ts_millis, status, latency_ms, status_code,
                     error, failure, zone, region)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for (r, status) in rows {
                stmt.execute(duckdb::params![
                    r.check_id,
                    r.agent_id,
                    r.timestamp.timestamp_millis(),
                    status.as_str(),
                    r.latency_ms as i64,
                    r.status_code.map(|c| c as i32),
                    r.error,
                    r.failure.as_ref().map(serde_json::to_string).transpose()?,
                    non_empty(&r.zone),
                    non_empty(&r.region),
                ])?;
            }
            Ok(())
        };
        match insert() {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Return history rows for a check, newest first, optionally within a time
    /// range and optionally restricted to one evaluated status.
    ///
    /// The status filter matters: a day can hold thousands of results, so the
    /// failures worth reading about are not necessarily in the newest `limit`.
    pub fn history(
        &self,
        check_id: &str,
        limit: usize,
        from: Option<i64>,
        to: Option<i64>,
        status: Option<Status>,
    ) -> Result<Vec<HistoryRow>> {
        let mut sql = String::from(
            "SELECT ts_millis, agent_id, status, latency_ms, status_code, error, failure
             FROM check_results WHERE check_id = ?",
        );
        if from.is_some() {
            sql.push_str(" AND ts_millis >= ?");
        }
        if to.is_some() {
            sql.push_str(" AND ts_millis < ?");
        }
        if status.is_some() {
            sql.push_str(" AND status = ?");
        }
        // Deterministic order when several agents report the same millisecond.
        sql.push_str(" ORDER BY ts_millis DESC, agent_id ASC LIMIT ?");
        let mut stmt = self.conn.prepare(&sql)?;
        // Build params dynamically.
        let mut params: Vec<duckdb::types::Value> =
            vec![duckdb::types::Value::Text(check_id.to_owned())];
        if let Some(f) = from {
            params.push(duckdb::types::Value::BigInt(f));
        }
        if let Some(t) = to {
            params.push(duckdb::types::Value::BigInt(t));
        }
        // Must be pushed in the same order the clauses were appended above.
        if let Some(s) = status {
            params.push(duckdb::types::Value::Text(s.as_str().to_owned()));
        }
        params.push(duckdb::types::Value::BigInt(limit as i64));
        let param_refs: Vec<&dyn duckdb::types::ToSql> = params
            .iter()
            .map(|v| v as &dyn duckdb::types::ToSql)
            .collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(HistoryRow {
                    timestamp: row.get::<_, i64>(0)?,
                    agent_id: row.get::<_, String>(1)?,
                    status: Status::from_db(&row.get::<_, String>(2)?),
                    latency_ms: row.get::<_, i64>(3)? as u64,
                    status_code: row.get::<_, Option<i32>>(4)?.map(|c| c as u16),
                    error: row.get::<_, Option<String>>(5)?,
                    // Rows written before probes classified themselves have no
                    // value here; readers fall back to the neutral summary.
                    failure: row
                        .get::<_, Option<String>>(6)?
                        .and_then(|f| serde_json::from_str(&f).ok()),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Persist an alert (a state transition worth surfacing).
    pub fn insert_alert(
        &self,
        check_id: &str,
        from: Status,
        to: Status,
        ts_millis: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO alerts (check_id, from_state, to_state, ts_millis) VALUES (?, ?, ?, ?)",
            duckdb::params![check_id, from.as_str(), to.as_str(), ts_millis],
        )?;
        Ok(())
    }

    /// Return the most recent `limit` alerts, newest first.
    pub fn recent_alerts(&self, limit: usize) -> Result<Vec<AlertRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT check_id, from_state, to_state, ts_millis
             FROM alerts
             ORDER BY ts_millis DESC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![limit as i64], |row| {
                Ok(AlertRow {
                    check_id: row.get::<_, String>(0)?,
                    from: Status::from_db(&row.get::<_, String>(1)?),
                    to: Status::from_db(&row.get::<_, String>(2)?),
                    timestamp: row.get::<_, i64>(3)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The open incident for a check, if any. A check never has more than one:
    /// while it stays DOWN the same row remains open.
    pub fn open_incident_for(&self, check_id: &str) -> Result<Option<IncidentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, check_id, status, started_at, resolved_at, details,
                    acknowledged_at, acknowledged_by
             FROM incidents
             WHERE check_id = ? AND status = 'open'
             ORDER BY started_at DESC
             LIMIT 1",
        )?;
        let mut rows = stmt
            .query_map(duckdb::params![check_id], Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows.pop())
    }

    /// Open a new incident for a check. A check never carries two open
    /// incidents, so if one is already open it is returned unchanged and
    /// nothing is written — a further DOWN observation rides on the existing
    /// row. The id is the check plus its start time, unique because a new
    /// incident only opens after the previous one closed.
    pub fn create_incident(
        &self,
        check_id: &str,
        started_at: i64,
        details: Option<&str>,
    ) -> Result<IncidentRow> {
        if let Some(existing) = self.open_incident_for(check_id)? {
            return Ok(existing);
        }
        let id = format!("{check_id}-{started_at}");
        self.conn.execute(
            "INSERT INTO incidents (id, check_id, status, started_at, details)
             VALUES (?, ?, 'open', ?, ?)",
            duckdb::params![id, check_id, started_at, details],
        )?;
        Ok(IncidentRow {
            id,
            check_id: check_id.into(),
            status: IncidentStatus::Open,
            started_at,
            resolved_at: None,
            details: details.map(str::to_owned),
            acknowledged_at: None,
            acknowledged_by: None,
        })
    }

    /// Close the open incident for a check, recording when. Returns it (now
    /// resolved), or `None` when nothing was open.
    pub fn resolve_incident(
        &self,
        check_id: &str,
        resolved_at: i64,
    ) -> Result<Option<IncidentRow>> {
        let Some(open) = self.open_incident_for(check_id)? else {
            return Ok(None);
        };
        self.conn.execute(
            "UPDATE incidents SET status = 'resolved', resolved_at = ? WHERE id = ?",
            duckdb::params![resolved_at, open.id],
        )?;
        Ok(Some(IncidentRow {
            status: IncidentStatus::Resolved,
            resolved_at: Some(resolved_at),
            ..open
        }))
    }

    /// Mark an open incident acknowledged — an operator has seen it. Returns the
    /// updated row, or `None` when the id matches no open incident (already
    /// resolved, or never existed).
    pub fn acknowledge_incident(
        &self,
        id: &str,
        acknowledged_by: &str,
        now_millis: i64,
    ) -> Result<Option<IncidentRow>> {
        let changed = self.conn.execute(
            "UPDATE incidents
             SET acknowledged_at = ?, acknowledged_by = ?
             WHERE id = ? AND status = 'open'",
            duckdb::params![now_millis, acknowledged_by, id],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        let mut stmt = self.conn.prepare(
            "SELECT id, check_id, status, started_at, resolved_at, details,
                    acknowledged_at, acknowledged_by
             FROM incidents WHERE id = ?",
        )?;
        let mut rows = stmt
            .query_map(duckdb::params![id], Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows.pop())
    }

    /// Recent incidents, newest first. `status` filters to 'open' or 'resolved'
    /// when given.
    pub fn list_incidents(
        &self,
        limit: usize,
        status: Option<IncidentStatus>,
    ) -> Result<Vec<IncidentRow>> {
        let mut sql = String::from(
            "SELECT id, check_id, status, started_at, resolved_at, details,
                    acknowledged_at, acknowledged_by
             FROM incidents WHERE TRUE",
        );
        let mut params: Vec<duckdb::types::Value> = Vec::new();
        if let Some(s) = status {
            sql.push_str(" AND status = ?");
            params.push(duckdb::types::Value::Text(s.as_str().to_owned()));
        }
        sql.push_str(" ORDER BY started_at DESC LIMIT ?");
        params.push(duckdb::types::Value::BigInt(limit as i64));
        let param_refs: Vec<&dyn duckdb::types::ToSql> = params
            .iter()
            .map(|v| v as &dyn duckdb::types::ToSql)
            .collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(param_refs.as_slice(), Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// A check's incidents, newest first.
    pub fn incidents_for_check(&self, check_id: &str, limit: usize) -> Result<Vec<IncidentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, check_id, status, started_at, resolved_at, details,
                    acknowledged_at, acknowledged_by
             FROM incidents
             WHERE check_id = ?
             ORDER BY started_at DESC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(duckdb::params![check_id, limit as i64], Self::incident_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn incident_row(row: &duckdb::Row) -> duckdb::Result<IncidentRow> {
        Ok(IncidentRow {
            id: row.get::<_, String>(0)?,
            check_id: row.get::<_, String>(1)?,
            status: IncidentStatus::from_db(&row.get::<_, String>(2)?),
            started_at: row.get::<_, i64>(3)?,
            resolved_at: row.get::<_, Option<i64>>(4)?,
            details: row.get::<_, Option<String>>(5)?,
            acknowledged_at: row.get::<_, Option<i64>>(6)?,
            acknowledged_by: row.get::<_, Option<String>>(7)?,
        })
    }

    /// One StatusRow per check_id: the latest result's status/latency/code/error,
    /// plus the fraction of UP results in the 24h window `(now-24h, now]`.
    /// A check with no rows at all is absent; uptime is over in-window rows only.
    pub fn list_status(&self, now_millis: i64) -> Result<Vec<StatusRow>> {
        let window_start = now_millis - 86_400_000;
        // Every "latest" field is read from a single row, picked by row_number.
        // Per-column `arg_max` is wrong here: it skips NULLs, so a recovered
        // check would keep reporting the stale error of its last failure.
        //
        // The displayed status is the coordinator's CONFIRMED state
        // (`current_state`), not the raw last observation: with
        // `failures_before_down` > 1 a check stays UP through its first failed
        // probes, and that decision must survive a restart. Checks with no
        // row yet (pre-upgrade data) fall back to the raw latest status.
        let mut stmt = self.conn.prepare(
            "SELECT
                 latest.check_id,
                 COALESCE(cs.status, latest.status) AS status,
                 latest.latency_ms,
                 latest.ts_millis,
                 win.uptime,
                 latest.status_code,
                 latest.error,
                 latest.failure
             FROM (
                 SELECT check_id, status, latency_ms, ts_millis, status_code, error, failure,
                        row_number() OVER (PARTITION BY check_id ORDER BY ts_millis DESC) AS rn
                 FROM check_results
             ) AS latest
             JOIN (
                 SELECT check_id,
                     CAST(SUM(CASE WHEN status = 'up' AND ts_millis > ? AND ts_millis <= ? THEN 1 ELSE 0 END) AS DOUBLE)
                       / NULLIF(SUM(CASE WHEN ts_millis > ? AND ts_millis <= ? THEN 1 ELSE 0 END), 0) AS uptime
                 FROM check_results
                 GROUP BY check_id
             ) AS win ON win.check_id = latest.check_id
             LEFT JOIN current_state cs ON cs.check_id = latest.check_id
             WHERE latest.rn = 1",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![window_start, now_millis, window_start, now_millis],
                |row| {
                    Ok(StatusRow {
                        check_id: row.get::<_, String>(0)?,
                        status: Status::from_db(&row.get::<_, String>(1)?),
                        latency_ms: row.get::<_, i64>(2)? as u64,
                        last_check: row.get::<_, i64>(3)?,
                        uptime_24h: row.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
                        status_code: row.get::<_, Option<i32>>(5)?.map(|c| c as u16),
                        error: row.get::<_, Option<String>>(6)?,
                        failure: row
                            .get::<_, Option<String>>(7)?
                            .and_then(|f| serde_json::from_str(&f).ok()),
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Persist what the coordinator decided a check reads as now: the confirmed
    /// status, the in-flight DOWN count toward its threshold, and when it was
    /// last observed. Written on every observation; read back at startup so the
    /// in-memory tracker resumes confirmations exactly where it left off.
    pub fn update_current_state(
        &self,
        check_id: &str,
        status: Status,
        consecutive_failures: u32,
        last_check: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO current_state (check_id, status, last_check, consecutive_failures)
             VALUES (?, ?, ?, ?)",
            duckdb::params![check_id, status.as_str(), last_check, consecutive_failures as i32],
        )?;
        Ok(())
    }

    /// All persisted confirmed states, for seeding the tracker on startup.
    pub fn list_current_state(&self) -> Result<Vec<CurrentStateRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT check_id, status, consecutive_failures FROM current_state")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(CurrentStateRow {
                    check_id: row.get::<_, String>(0)?,
                    status: Status::from_db(&row.get::<_, String>(1)?),
                    consecutive_failures: row.get::<_, i32>(2)? as u32,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Delete rows older than the retention window. `results_days` covers raw
    /// probe results; `alerts_days` covers state-transition alerts and CLOSED
    /// incidents. An open incident is current reality, never pruned. Returns
    /// the number of rows removed.
    pub fn prune(&self, results_days: u64, alerts_days: u64, now_millis: i64) -> Result<u64> {
        let results_before = now_millis - (results_days as i64) * 86_400_000;
        let alerts_before = now_millis - (alerts_days as i64) * 86_400_000;
        let mut removed = 0u64;
        removed += self.conn.execute(
            "DELETE FROM check_results WHERE ts_millis < ?",
            duckdb::params![results_before],
        )? as u64;
        removed += self.conn.execute(
            "DELETE FROM alerts WHERE ts_millis < ?",
            duckdb::params![alerts_before],
        )? as u64;
        removed += self.conn.execute(
            "DELETE FROM incidents WHERE status = 'resolved' AND resolved_at < ?",
            duckdb::params![alerts_before],
        )? as u64;
        Ok(removed)
    }

    /// Every agent that has ever reported, newest-seen first. Agents are not
    /// registered anywhere — they exist because results carry their `agent_id`,
    /// so this reflects what is actually reporting rather than what is configured.
    ///
    /// Heartbeat pings are written under a reserved id because they arrive over
    /// HTTP with no agent behind them. Listing it would invent a monitoring
    /// agent that nobody deployed and that no pod corresponds to.
    pub fn list_agents(&self, now_millis: i64) -> Result<Vec<AgentRow>> {
        let window_start = now_millis - 86_400_000;
        let mut stmt = self.conn.prepare(
            "SELECT
                 agent_id,
                 max(ts_millis)                  AS last_seen,
                 count(DISTINCT check_id)        AS checks_reporting,
                 SUM(CASE WHEN ts_millis > ? AND ts_millis <= ? THEN 1 ELSE 0 END) AS results_24h,
                 -- Its location as of its newest result, so relocating an agent
                 -- is reflected without any registry to update.
                 arg_max(zone, ts_millis)        AS zone,
                 arg_max(region, ts_millis)      AS region
             FROM check_results
             WHERE agent_id != ?
             GROUP BY agent_id
             ORDER BY last_seen DESC",
        )?;
        let rows = stmt
            .query_map(
                duckdb::params![window_start, now_millis, crate::models::HEARTBEAT_AGENT],
                |row| {
                    Ok(AgentRow {
                        agent_id: row.get::<_, String>(0)?,
                        last_seen: row.get::<_, i64>(1)?,
                        zone: row
                            .get::<_, Option<String>>(4)?
                            .unwrap_or_else(|| crate::config::UNKNOWN_LOCATION.into()),
                        region: row
                            .get::<_, Option<String>>(5)?
                            .unwrap_or_else(|| crate::config::UNKNOWN_LOCATION.into()),
                        checks_reporting: row.get::<_, i64>(2)? as u32,
                        results_24h: row.get::<_, Option<i64>>(3)?.unwrap_or(0) as u32,
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Uptime split into `buckets` equal segments over the `days` days ending at
    /// `now_millis`, oldest-first. Segments are `days*DAY/buckets` wide, so the
    /// strip keeps a constant number of bars while a shorter window simply gives
    /// finer resolution (7 days over 90 segments is ~1h52m each).
    ///
    /// A segment with no results has `uptime == None`, which the UI renders as
    /// "no data" rather than as an outage. Always returns exactly `buckets`.
    pub fn uptime_buckets(
        &self,
        check_id: &str,
        days: usize,
        buckets: usize,
        now_millis: i64,
    ) -> Result<Vec<UptimeBucket>> {
        const DAY: i64 = 86_400_000;
        let buckets = buckets.max(1);
        let window = days as i64 * DAY;
        let bucket_ms = (window / buckets as i64).max(1);
        // Recompute the span from the bucket width so segment edges tile exactly:
        // integer division would otherwise leave a sliver unaccounted for.
        let span = bucket_ms * buckets as i64;
        let lower = now_millis - span; // exclusive

        let mut up = vec![0u32; buckets];
        let mut total = vec![0u32; buckets];

        let mut stmt = self.conn.prepare(
            "SELECT ts_millis, status FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
        )?;
        let rows = stmt.query_map(duckdb::params![check_id, lower, now_millis], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for r in rows {
            let (ts, status) = r?;
            // Segments ago: 0 = the one ending at `now`. The range guarantees
            // 0 <= d < buckets.
            let d = ((now_millis - ts) / bucket_ms) as usize;
            let i = buckets - 1 - d;
            total[i] += 1;
            if Status::from_db(&status) == Status::Up {
                up[i] += 1;
            }
        }

        let out = (0..buckets)
            .map(|i| {
                let t = total[i];
                let start = now_millis - (buckets as i64 - i as i64) * bucket_ms;
                UptimeBucket {
                    start,
                    end: start + bucket_ms,
                    up: up[i],
                    total: t,
                    uptime: if t > 0 {
                        Some(up[i] as f64 / t as f64)
                    } else {
                        None
                    },
                }
            })
            .collect();
        Ok(out)
    }

    /// Average latency for a check over an explicit window, split into equal
    /// segments (oldest first), plus exact per-agent attribution over the same
    /// window. Segments with no attempts carry `None`, not zero.
    ///
    /// Returns at most `buckets` points: the split is capped at one segment per
    /// recorded instant, because slicing finer than the data only manufactures
    /// empty segments.
    ///
    /// The window is explicit rather than `now`-anchored so a historical
    /// segment (a clicked uptime bar) can be charted truthfully instead of
    /// being approximated by the newest N rows.
    pub fn latency_series(
        &self,
        check_id: &str,
        from: i64,
        to: i64,
        buckets: usize,
    ) -> Result<LatencySeries> {
        // The window is exactly what the caller asked for. An earlier attempt
        // narrowed it to the data inside it, which made a young check readable
        // but silently broke the range selector: every range shorter than the
        // check's history rendered identically, so the control did nothing.
        // The selector is the viewer's instrument for choosing a span, and a
        // 30-day view of a check that is five hours old *should* look mostly
        // empty — that emptiness is the check's age, honestly drawn.
        //
        // Slicing finer than the recorded instants is a different matter: it
        // manufactures empty segments between real ones, a picket fence of
        // imaginary 0ms readings. One segment per instant is the finest split
        // that carries information.
        let instants: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT ts_millis) FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
            duckdb::params![check_id, from, to],
            |row| row.get(0),
        )?;
        if instants == 0 {
            // Nothing recorded in the window: an empty series, not a row of
            // imaginary 0ms readings.
            return Ok(LatencySeries::default());
        }
        let buckets = buckets.max(1).min(instants as usize);
        let bucket_ms = ((to - from) / buckets as i64).max(1);

        let mut sum = vec![0.0f64; buckets];
        let mut count = vec![0u32; buckets];
        let mut agents: BTreeMap<String, LatencyBand> = BTreeMap::new();

        let mut stmt = self.conn.prepare(
            "SELECT ts_millis, latency_ms, agent_id, status FROM check_results
             WHERE check_id = ? AND ts_millis > ? AND ts_millis <= ?",
        )?;
        let rows = stmt.query_map(duckdb::params![check_id, from, to], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for r in rows {
            let (ts, lat, agent_id, status) = r?;
            // Lower bound is exclusive, so shift before dividing: a timestamp
            // landing exactly on a boundary belongs to the segment below it.
            let i = (((ts - from - 1).max(0) / bucket_ms) as usize).min(buckets - 1);
            sum[i] += lat as f64;
            count[i] += 1;
            agents
                .entry(agent_id)
                .or_default()
                .observe(lat as u64, status == "up");
        }

        let points = (0..buckets)
            .map(|i| LatencyPoint {
                timestamp: from + i as i64 * bucket_ms,
                avg_latency_ms: (count[i] > 0).then(|| sum[i] / count[i] as f64),
            })
            .collect();

        let mut overall = LatencyBand::default();
        for b in agents.values() {
            overall.merge(b);
        }
        Ok(LatencySeries {
            points,
            overall,
            agents: agents
                .into_iter()
                .map(|(agent_id, band)| AgentBand { agent_id, band })
                .collect(),
        })
    }

    /// Seed synthetic data spanning ~90 days before `now_millis` when the store
    /// is empty. Idempotent: a no-op when check_results already has rows.
    /// Returns true if seeding occurred.
    pub fn seed_mock(&self, now_millis: i64) -> Result<bool> {
        let count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM check_results", [], |r| r.get(0))?;
        if count > 0 {
            return Ok(false);
        }

        let checks = [
            "api-gateway",
            "postgres-primary",
            "redis-cache",
            "web-frontend",
        ];
        let days = 90usize;
        const DAY: i64 = 86_400_000;
        // Sample timestamps oldest-first. Older days are sparse (30-min); the
        // most recent 24h is dense (5-min) so the latency sparkline is detailed
        // without seeding 90 days at high resolution.
        let recent_step = 5 * 60 * 1000i64; // 5 min
        let sparse_step = 30 * 60 * 1000i64; // 30 min
                                             // day 0 = oldest, day (days-1) = today. `day_of(ts)` maps a timestamp to
                                             // its day index for outage lookup and alert placement.
        let day_of = |ts: i64| -> usize {
            let ago = ((now_millis - ts) / DAY) as usize;
            (days - 1).saturating_sub(ago)
        };
        let mut samples: Vec<i64> = Vec::new();
        // Sparse span: from 90 days ago up to 24h ago.
        let mut ts = now_millis - days as i64 * DAY;
        while ts <= now_millis - DAY {
            samples.push(ts);
            ts += sparse_step;
        }
        // Dense span: last 24h.
        ts = now_millis - DAY + recent_step;
        while ts <= now_millis {
            samples.push(ts);
            ts += recent_step;
        }

        // Outage windows expressed as (check, inclusive day range) where the
        // check reports DOWN. day 0 is the oldest, day (days-1) is today.
        let is_down = |name: &str, day: usize| -> bool {
            match name {
                // A full bad day ~30 days ago plus a recent blip.
                "redis-cache" => day == 60 || day == 88,
                // One brief outage ~20 days ago.
                "api-gateway" => day == 70,
                _ => false,
            }
        };

        let mut rows: Vec<(CheckResult, Status)> = Vec::new();
        for name in checks {
            for (i, &ts) in samples.iter().enumerate() {
                let down = is_down(name, day_of(ts));
                let (status, code, latency) = if down {
                    (Status::Down, Some(503u16), 0)
                } else {
                    (Status::Up, Some(200u16), 20 + ((i % 7) * 5) as i64)
                };
                rows.push((
                    CheckResult {
                        body: None,
                        headers: None,
                        zone: "seed".into(),
                        region: "seed".into(),
                        check_id: name.to_string(),
                        agent_id: "seed".to_string(),
                        kind: crate::models::CheckKind::Http,
                        timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
                        latency_ms: latency as u64,
                        status_code: code,
                        body_matched: None,
                        error: if down {
                            Some("503 Service Unavailable".into())
                        } else {
                            None
                        },
                        failure: down.then_some(crate::models::FailureKind::UnexpectedStatus),
                    },
                    status,
                ));
            }
        }
        self.insert_results(&rows)?;

        // Matching alerts at the start of each bad day and its recovery, so the
        // incidents list mirrors the bars.
        let day_start_ts = |day: usize| now_millis - (days - 1 - day) as i64 * DAY;
        let alert = |name: &str, down_day: usize| -> Result<()> {
            self.insert_alert(name, Status::Up, Status::Down, day_start_ts(down_day))?;
            self.insert_alert(name, Status::Down, Status::Up, day_start_ts(down_day + 1))?;
            Ok(())
        };
        alert("redis-cache", 60)?;
        alert("redis-cache", 88)?;
        alert("api-gateway", 70)?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn result(check_id: &str, ts_millis: i64, latency_ms: u64) -> CheckResult {
        CheckResult {
            zone: "test-zone".into(),
            region: "test-region".into(),
            body: None,
            headers: None,
            check_id: check_id.into(),
            agent_id: "agent".into(),
            kind: crate::models::CheckKind::Http,
            timestamp: DateTime::from_timestamp_millis(ts_millis).unwrap(),
            latency_ms,
            status_code: Some(200),
            body_matched: None,
            error: None,
            failure: None,
        }
    }

    #[test]
    fn failure_reason_survives_the_round_trip_to_readers() {
        // An operator needs to know *why* a check is down, not just that it is.
        // The reason is written by the agent and must reach both readers.
        let s = Storage::open(":memory:").unwrap();
        let now = 5_000i64;
        let mut refused = result("a", now, 0);
        refused.status_code = None;
        refused.error = Some("connection refused".into());

        let mut wrong_code = result("a", now - 1000, 12);
        wrong_code.status_code = Some(404);

        s.insert_results(&[(wrong_code, Status::Down), (refused, Status::Down)])
            .unwrap();

        // history() surfaces the per-attempt reason.
        let hist = s.history("a", 10, None, None, None).unwrap();
        assert_eq!(hist[0].error.as_deref(), Some("connection refused"));
        assert_eq!(
            hist[0].status_code, None,
            "a refused connection has no HTTP code"
        );
        assert_eq!(
            hist[1].error, None,
            "a 404 is a response, not a transport error"
        );
        assert_eq!(
            hist[1].status_code,
            Some(404),
            "the unexpected code is preserved"
        );

        // list_status() surfaces the reason for the *latest* attempt only.
        let rows = s.list_status(now).unwrap();
        let a = rows.iter().find(|r| r.check_id == "a").unwrap();
        assert_eq!(
            a.error.as_deref(),
            Some("connection refused"),
            "dashboard shows the newest failure reason"
        );
    }

    #[test]
    fn recovered_check_does_not_report_a_stale_failure_reason() {
        // Regression: per-column arg_max() skips NULLs, so a check that failed and
        // then recovered kept surfacing the old error next to an "up" status.
        let s = Storage::open(":memory:").unwrap();
        let now = 10_000i64;

        let mut failed = result("a", now - 1000, 5);
        failed.status_code = None;
        failed.error = Some("connection refused".into());

        // The newest row succeeded: status_code 200, error None.
        let recovered = result("a", now, 11);

        s.insert_results(&[(failed, Status::Down), (recovered, Status::Up)])
            .unwrap();

        let rows = s.list_status(now).unwrap();
        let a = rows.iter().find(|r| r.check_id == "a").unwrap();
        assert_eq!(a.status, Status::Up, "latest row is the success");
        assert_eq!(
            a.error, None,
            "a recovered check must not carry the old error"
        );
        assert_eq!(
            a.status_code,
            Some(200),
            "code comes from the same latest row"
        );
        assert_eq!(a.latency_ms, 11, "latency comes from the same latest row");
    }

    #[test]
    fn history_round_trips_newest_first_with_limit_and_check_filter() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_results(&[
            (result("a", 1000, 10), Status::Up),
            (result("a", 2000, 20), Status::Down),
            (result("a", 3000, 30), Status::Up),
            (result("b", 1500, 99), Status::Down),
        ])
        .unwrap();

        // Full history for "a": newest first, and "b"'s row (latency 99) excluded.
        let hist = s.history("a", 10, None, None, None).unwrap();
        assert_eq!(
            hist,
            vec![
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 3000,
                    status: Status::Up,
                    latency_ms: 30,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 2000,
                    status: Status::Down,
                    latency_ms: 20,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 1000,
                    status: Status::Up,
                    latency_ms: 10,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
            ]
        );

        // Limit caps to the N newest rows.
        let capped = s.history("a", 2, None, None, None).unwrap();
        assert_eq!(
            capped,
            vec![
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 3000,
                    status: Status::Up,
                    latency_ms: 30,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
                HistoryRow {
                    agent_id: "agent".into(),
                    timestamp: 2000,
                    status: Status::Down,
                    latency_ms: 20,
                    status_code: Some(200),
                    error: None,
                    failure: None,
                },
            ]
        );
    }

    #[test]
    fn recent_alerts_round_trips_newest_first_with_exact_fields() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_alert("a", Status::Up, Status::Down, 1000).unwrap();
        s.insert_alert("b", Status::Down, Status::Up, 2000).unwrap();
        s.insert_alert("a", Status::Up, Status::Unknown, 3000)
            .unwrap();

        let got = s.recent_alerts(10).unwrap();
        assert_eq!(
            got,
            vec![
                AlertRow {
                    check_id: "a".into(),
                    from: Status::Up,
                    to: Status::Unknown,
                    timestamp: 3000
                },
                AlertRow {
                    check_id: "b".into(),
                    from: Status::Down,
                    to: Status::Up,
                    timestamp: 2000
                },
                AlertRow {
                    check_id: "a".into(),
                    from: Status::Up,
                    to: Status::Down,
                    timestamp: 1000
                },
            ]
        );
    }

    #[test]
    fn recent_alerts_caps_to_the_newest_limit() {
        let s = Storage::open(":memory:").unwrap();
        s.insert_alert("a", Status::Up, Status::Down, 1000).unwrap();
        s.insert_alert("b", Status::Down, Status::Up, 2000).unwrap();
        s.insert_alert("a", Status::Up, Status::Unknown, 3000)
            .unwrap();

        let got = s.recent_alerts(2).unwrap();
        assert_eq!(
            got,
            vec![
                AlertRow {
                    check_id: "a".into(),
                    from: Status::Up,
                    to: Status::Unknown,
                    timestamp: 3000
                },
                AlertRow {
                    check_id: "b".into(),
                    from: Status::Down,
                    to: Status::Up,
                    timestamp: 2000
                },
            ]
        );
    }

    #[test]
    fn list_status_reports_latest_per_check_and_windowed_uptime() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        s.insert_results(&[
            // Check "a": 3 UP + 1 DOWN in-window; latest (greatest ts) is the DOWN.
            (result("a", now - 3000, 10), Status::Up),
            (result("a", now - 2000, 11), Status::Up),
            (result("a", now - 1000, 12), Status::Up),
            (result("a", now, 50), Status::Down),
            // Check "b": all UP in-window ...
            (result("b", now - 5000, 20), Status::Up),
            (result("b", now - 4000, 21), Status::Up),
            // ... plus one stale DOWN exactly on the exclusive lower bound: must be excluded.
            (result("b", now - day, 99), Status::Down),
        ])
        .unwrap();

        let rows = s.list_status(now).unwrap();
        assert_eq!(rows.len(), 2, "one row per distinct check_id");
        let find = |id: &str| rows.iter().find(|r| r.check_id == id).unwrap().clone();

        let a = find("a");
        assert_eq!(a.status, Status::Down, "latest row wins");
        assert_eq!(a.latency_ms, 50, "latest row's latency");
        assert_eq!(a.last_check, now, "latest row's timestamp");
        assert!(
            (a.uptime_24h - 0.75).abs() < 1e-9,
            "3 UP / 4 in-window, got {}",
            a.uptime_24h
        );

        let b = find("b");
        assert_eq!(b.status, Status::Up);
        assert_eq!(b.latency_ms, 21);
        assert_eq!(b.last_check, now - 4000);
        // Stale DOWN at the exclusive lower bound is filtered out, so 2 UP / 2 in-window.
        assert!(
            (b.uptime_24h - 1.0).abs() < 1e-9,
            "stale row must be excluded, got {}",
            b.uptime_24h
        );
    }

    #[test]
    fn seed_mock_seeds_when_empty_then_is_idempotent() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        assert!(s.seed_mock(now).unwrap(), "seeds an empty store");
        let seeded = s.list_status(now).unwrap();
        assert!(
            !seeded.is_empty(),
            "seeded store yields at least one status"
        );

        assert!(!s.seed_mock(now).unwrap(), "second seed is a no-op");
        let after = s.list_status(now).unwrap();
        assert_eq!(after.len(), seeded.len(), "idempotent seed adds no checks");
    }

    #[test]
    fn uptime_buckets_windows_filters_and_boundary() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        s.insert_results(&[
            // Bucket 2 (today, (now-DAY, now]): 2 UP + 1 DOWN -> up=2, total=3.
            (result("x", now, 10), Status::Up),
            (result("x", now - 1000, 11), Status::Up),
            (result("x", now - 2000, 12), Status::Down),
            // Bucket 1 ((now-2*DAY, now-DAY]): nothing -> up=0, total=0, uptime=None.
            // Bucket 0 ((now-3*DAY, now-2*DAY]): 1 UP -> up=1, total=1, uptime=Some(1.0).
            (result("x", now - 2 * day - 1000, 13), Status::Up),
            // Boundary: exactly at bucket 0's exclusive lower bound now-3*DAY -> excluded.
            (result("x", now - 3 * day, 14), Status::Up),
            // Decoy check "y" at now: must never count toward "x".
            (result("y", now, 99), Status::Up),
        ])
        .unwrap();

        let buckets = s.uptime_buckets("x", 3, 3, now).unwrap();
        assert_eq!(buckets.len(), 3, "exactly `days` buckets, oldest-first");

        // Bucket 0: oldest day; boundary row at now-3*DAY excluded, one UP remains.
        assert_eq!(buckets[0].start, now - 3 * day);
        assert_eq!(buckets[0].up, 1);
        assert_eq!(
            buckets[0].total, 1,
            "row at exclusive lower bound now-3*DAY excluded"
        );
        assert_eq!(buckets[0].uptime, Some(1.0));

        // Bucket 1: empty middle day.
        assert_eq!(buckets[1].start, now - 2 * day);
        assert_eq!(buckets[1].up, 0);
        assert_eq!(buckets[1].total, 0);
        assert_eq!(buckets[1].uptime, None, "no rows -> uptime None, not 0.0");

        // Bucket 2: today; 2 UP + 1 DOWN; decoy "y" excluded.
        assert_eq!(buckets[2].start, now - day);
        assert_eq!(buckets[2].up, 2);
        assert_eq!(buckets[2].total, 3, "decoy check \"y\" must not count");
        let uptime = buckets[2].uptime.expect("today has data");
        assert!(
            (uptime - 2.0 / 3.0).abs() < 1e-9,
            "2 UP / 3 total, got {uptime}"
        );
    }

    #[test]
    fn uptime_buckets_no_rows_returns_empty_segments() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        let buckets = s.uptime_buckets("absent", 4, 4, now).unwrap();
        assert_eq!(
            buckets.len(),
            4,
            "always exactly `days` buckets even with no data"
        );
        for (i, b) in buckets.iter().enumerate() {
            assert_eq!(
                b.start,
                now - (4 - i as i64) * day,
                "bucket {i} window start"
            );
            assert_eq!(b.up, 0);
            assert_eq!(b.total, 0);
            assert_eq!(b.uptime, None, "bucket {i} has no data");
        }
    }

    #[test]
    fn latency_sparkline_returns_hourly_averages() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        // Four distinct instants, so a 4-bucket split is fully supported by the
        // data and isn't capped.
        s.insert_results(&[
            (result("x", now, 50), Status::Up),
            (result("x", now - 600_000, 100), Status::Up),
            (result("x", now - hour - 600_000, 75), Status::Up),
            (result("x", now - 2 * hour - 600_000, 60), Status::Up),
            // Decoy check "y" should not count.
            (result("y", now, 999), Status::Up),
        ])
        .unwrap();

        let points = s
            .latency_series("x", now - 4 * hour, now, 4)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "four instants support four buckets");

        // Segments are one hour each: the requested window divided by the
        // buckets it supports, not narrowed to the data inside it.
        assert_eq!(points[0].timestamp, now - 4 * hour, "window as requested");
        assert_eq!(points[3].timestamp, now - hour);

        // Bucket 0 spans the hour before any result exists, and must not read
        // as a 0ms response.
        assert_eq!(points[0].avg_latency_ms, None, "empty segment -> None");
        assert_eq!(points[1].avg_latency_ms, Some(60.0));
        assert_eq!(points[2].avg_latency_ms, Some(75.0));
        assert_eq!(
            points[3].avg_latency_ms,
            Some(75.0),
            "newest segment averages 50 and 100"
        );

        // Decoy "y" must not be counted.
        let y_points = s
            .latency_series("y", now - 4 * hour, now, 4)
            .unwrap()
            .points;
        assert_eq!(
            y_points.last().unwrap().avg_latency_ms,
            Some(999.0),
            "decoy 'y' has its own data"
        );
    }

    #[test]
    fn every_requested_range_charts_that_range() {
        // The 24h/7d/30d/90d selector must actually change the span, including
        // for a check younger than the range. An earlier fix narrowed the window
        // to the data inside it, which read well for a young check but made all
        // four ranges render identically once the check was younger than them —
        // the control silently stopped doing anything.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        // Five hours old, exactly the shape that triggered the report.
        let rows: Vec<_> = (0..60)
            .map(|i| {
                (
                    result("young", now - i * 300_000, 20 + i as u64),
                    Status::Up,
                )
            })
            .collect();
        s.insert_results(&rows).unwrap();

        let span_of = |window: i64| {
            let p = s
                .latency_series("young", now - window, now, 120)
                .unwrap()
                .points;
            (p.last().unwrap().timestamp - p[0].timestamp, p.len())
        };
        let (day, _) = span_of(24 * hour);
        let (week, _) = span_of(7 * 24 * hour);
        let (month, _) = span_of(30 * 24 * hour);

        assert!(
            day < week && week < month,
            "each range must chart its own span: 24h={day} 7d={week} 30d={month}"
        );
        // And each is the requested window, not the data's extent.
        let requested = (30 * 24 * hour) as f64;
        let drift = (month as f64 - requested).abs() / requested;
        assert!(drift < 0.05, "30d view spans ~30 days, got {month}ms");
    }

    #[test]
    fn a_lone_populated_segment_is_still_a_reading() {
        // The other half of the report: over a 30-day window a five-hour-old
        // check fills a single segment. That is the truth and it must be
        // returned as data — the chart drew nothing and claimed to be
        // "Collecting latency data..." while sitting on 120 results.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let month = 30 * 24 * 3_600_000i64;

        let rows: Vec<_> = (0..60)
            .map(|i| (result("young", now - i * 30_000, 40), Status::Up))
            .collect();
        s.insert_results(&rows).unwrap();

        let series = s.latency_series("young", now - month, now, 120).unwrap();
        let populated: Vec<_> = series
            .points
            .iter()
            .filter(|p| p.avg_latency_ms.is_some())
            .collect();
        assert!(
            !populated.is_empty(),
            "a check with 60 results must report a reading over any window"
        );
        assert_eq!(
            series.overall.attempts, 60,
            "and the summary counts every attempt regardless of bucketing"
        );
    }

    #[test]
    fn a_check_that_stopped_reporting_still_shows_its_silence() {
        // Silence is data. This check reported for an hour then went quiet 20
        // hours ago; charting only the hour it spoke would hide the outage
        // entirely, so the window must keep running to the requested end.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let hour = 3_600_000i64;

        let rows: Vec<_> = (0..12)
            .map(|i| {
                (
                    result("quiet", now - 21 * hour + i * 300_000, 25),
                    Status::Up,
                )
            })
            .collect();
        s.insert_results(&rows).unwrap();

        let points = s.latency_series("quiet", now - 24 * hour, now, 12).unwrap();
        let trailing_empty = points
            .points
            .iter()
            .rev()
            .take_while(|p| p.avg_latency_ms.is_none())
            .count();
        assert!(
            trailing_empty > 0,
            "silence since the last result must stay on the chart"
        );
        let last = points.points.last().unwrap().timestamp;
        assert!(
            last > now - 4 * hour,
            "the chart still reaches the present: ends {}h early",
            (now - last) / hour
        );
    }

    #[test]
    fn latency_series_never_invents_zero_readings() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;

        // A check with nothing in the window must not produce a flat line of
        // zeroes, which would read as "responded instantly" rather than "never
        // ran".
        let points = s
            .latency_series("absent", now - 6 * 3_600_000, now, 6)
            .unwrap()
            .points;
        assert!(
            points.iter().all(|p| p.avg_latency_ms.is_none()),
            "no data must yield no measurements, got {points:?}"
        );
    }

    #[test]
    fn latency_series_caps_buckets_at_the_data_resolution() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let min = 60_000i64;

        // The case behind the picket-fence bug: a short window probed every few
        // minutes, asked for far more buckets than there are attempts.
        s.insert_results(&[
            (result("x", now - 12 * min, 40), Status::Up),
            (result("x", now - 8 * min, 50), Status::Up),
            (result("x", now - 4 * min, 60), Status::Up),
            (result("x", now, 70), Status::Up),
        ])
        .unwrap();

        let points = s
            .latency_series("x", now - 16 * min, now, 120)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "one segment per recorded instant, not 120");
        assert!(
            points.iter().all(|p| p.avg_latency_ms.is_some()),
            "capping to the data leaves no empty segments, got {points:?}"
        );
        assert_eq!(
            points
                .iter()
                .filter_map(|p| p.avg_latency_ms)
                .collect::<Vec<_>>(),
            vec![40.0, 50.0, 60.0, 70.0],
            "oldest first, one attempt each"
        );
    }

    #[test]
    fn latency_series_splits_into_sub_hour_buckets() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let min = 60_000i64;

        // Four instants over the last hour, so the requested 4 buckets (15 min
        // each) survive the resolution cap. Two land at the edges, leaving the
        // middle two genuinely empty.
        s.insert_results(&[
            (result("x", now - min, 40), Status::Up), // bucket 3 (last 15 min)
            (result("x", now - 2 * min, 20), Status::Up), // bucket 3 as well
            (result("x", now - 50 * min, 80), Status::Up), // bucket 0
            (result("x", now - 55 * min, 100), Status::Up), // bucket 0 as well
        ])
        .unwrap();

        let points = s
            .latency_series("x", now - 60 * min, now, 4)
            .unwrap()
            .points;
        assert_eq!(points.len(), 4, "exactly `buckets` points");
        assert_eq!(points[3].avg_latency_ms, Some(30.0), "avg of 40 and 20");
        assert_eq!(points[0].avg_latency_ms, Some(90.0), "avg of 80 and 100");
        assert_eq!(points[1].avg_latency_ms, None, "middle segment empty");
        assert_eq!(points[2].avg_latency_ms, None, "middle segment empty");
    }

    #[test]
    fn latency_series_attributes_a_historical_window_per_agent() {
        // What the day-detail modal asks for: a window that ended long before
        // `now`, with two agents disagreeing inside it. Sampling the newest rows
        // could not answer this.
        let s = Storage::open(":memory:").unwrap();
        let hour = 3_600_000i64;
        let now = 1_000_000_000_000i64;
        let from = now - 10 * hour;
        let to = from + 4 * hour;

        let at = |agent: &str, ts: i64, latency: u64, status: Status| {
            let mut r = result("web", ts, latency);
            r.agent_id = agent.to_string();
            (r, status)
        };
        s.insert_results(&[
            at("agent-a", from + hour, 10, Status::Up),
            at("agent-a", from + 2 * hour, 30, Status::Up),
            // agent-b times out inside the window: 5000ms is how long it waited,
            // not a measurement, so it must not become the max.
            at("agent-b", from + hour, 5000, Status::Down),
            at("agent-b", from + 2 * hour, 20, Status::Up),
            // Outside the window on both sides, and a decoy check.
            at("agent-a", from - hour, 999, Status::Up),
            at("agent-a", to + hour, 888, Status::Up),
            (result("other", from + hour, 777), Status::Up),
        ])
        .unwrap();

        // Two distinct instants in the window, so ask for the two segments the
        // data can actually support.
        let series = s.latency_series("web", from, to, 2).unwrap();

        assert_eq!(series.overall.attempts, 4, "only rows inside the window");
        assert_eq!(series.overall.up, 3);
        assert_eq!(series.overall.min_ms, Some(10));
        assert_eq!(
            series.overall.max_ms,
            Some(30),
            "the 5000ms timeout must not set the max"
        );
        assert_eq!(series.overall.avg_ms(), Some(20.0), "(10+30+20)/3");

        let by_agent: Vec<_> = series
            .agents
            .iter()
            .map(|a| (a.agent_id.as_str(), a.band.attempts, a.band.up))
            .collect();
        assert_eq!(
            by_agent,
            vec![("agent-a", 2, 2), ("agent-b", 2, 1)],
            "the failing agent is identifiable"
        );

        // The chart still buckets every attempt, so an outage stays visible as a
        // spike rather than vanishing from the shape.
        assert_eq!(series.points.len(), 2);
        // The series covers the window the caller asked for.
        assert_eq!(
            series.points[0].timestamp, from,
            "oldest segment starts at the requested bound"
        );
        assert_eq!(
            series.points[0].avg_latency_ms,
            Some(1265.0),
            "averages 10, 30, 20 and the 5000ms timeout"
        );
        // Silence after the last result means the check stopped reporting,
        // which must stay visible rather than being trimmed away.
        assert_eq!(
            series.points[1].avg_latency_ms, None,
            "the empty half must not read as 0ms"
        );
        assert!(
            series.points[1].timestamp < to,
            "the requested end is still charted"
        );
    }

    #[test]
    fn list_agents_reports_each_reporting_agent_once() {
        // Agents aren't registered anywhere — they exist because results carry
        // their agent_id, which is what makes the dashboard's agent list truthful.
        let s = Storage::open(":memory:").unwrap();
        let now = 100_000i64;
        let day = 86_400_000i64;

        let with_agent = |check: &str, agent: &str, ts: i64| {
            let mut r = result(check, ts, 10);
            r.agent_id = agent.into();
            (r, Status::Up)
        };

        s.insert_results(&[
            // agent-a reports two distinct checks in-window.
            with_agent("web", "agent-a", now),
            with_agent("api", "agent-a", now - 500),
            // agent-b reports one check twice: 1 distinct check, 2 results.
            with_agent("web", "agent-b", now - 1000),
            with_agent("web", "agent-b", now - 2000),
            // agent-old last reported over a day ago: still listed, but its
            // result falls outside the 24h window.
            with_agent("web", "agent-old", now - day - 1000),
        ])
        .unwrap();

        let agents = s.list_agents(now).unwrap();
        let find = |id: &str| agents.iter().find(|a| a.agent_id == id).unwrap().clone();

        assert_eq!(agents.len(), 3, "one row per distinct agent_id");
        assert_eq!(agents[0].agent_id, "agent-a", "ordered by last_seen DESC");

        let a = find("agent-a");
        assert_eq!(a.last_seen, now);
        assert_eq!(a.checks_reporting, 2, "distinct checks, not result count");
        assert_eq!(a.results_24h, 2);

        let b = find("agent-b");
        assert_eq!(b.checks_reporting, 1, "same check twice is still one check");
        assert_eq!(b.results_24h, 2, "but two results");

        let old = find("agent-old");
        assert_eq!(
            old.results_24h, 0,
            "stale results fall outside the 24h window"
        );
        assert_eq!(old.last_seen, now - day - 1000, "last_seen is not windowed");
    }
    #[test]
    fn history_distinguishes_agents_reporting_the_same_millisecond() {
        // Regression: several agents probe one check, so two rows can share a
        // timestamp. The UI keys rows by (timestamp, agent_id); without agent_id
        // the keys collide and the whole detail view fails to render.
        let s = Storage::open(":memory:").unwrap();
        let ts = 4_000i64;

        let mut a = result("web", ts, 10);
        a.agent_id = "agent-a".into();
        let mut b = result("web", ts, 20);
        b.agent_id = "agent-b".into();

        s.insert_results(&[(a, Status::Up), (b, Status::Up)])
            .unwrap();

        let hist = s.history("web", 10, None, None, None).unwrap();
        assert_eq!(hist.len(), 2, "both agents' results are kept");
        assert_eq!(
            hist[0].timestamp, hist[1].timestamp,
            "same millisecond, so timestamp alone cannot be a unique key"
        );

        let mut agents: Vec<_> = hist.iter().map(|h| h.agent_id.as_str()).collect();
        agents.sort();
        assert_eq!(
            agents,
            vec!["agent-a", "agent-b"],
            "each row names its agent"
        );

        // Ordering is deterministic (ts DESC, agent_id ASC) so the list is stable.
        assert_eq!(hist[0].agent_id, "agent-a");
        assert_eq!(hist[0].latency_ms, 10, "latency stays with its own agent");
        assert_eq!(hist[1].latency_ms, 20);
    }

    #[test]
    fn status_filter_finds_failures_a_newest_n_slice_would_miss() {
        // Regression: a busy day holds thousands of results. The day-detail view
        // used to read the newest 100 rows and so showed all-green for a day the
        // uptime bar had marked degraded, because the failures were older than
        // the slice. Filtering by status finds them regardless of position.
        let s = Storage::open(":memory:").unwrap();
        let day_start = 1_000_000i64;

        let mut rows = Vec::new();
        // Two early failures...
        for (i, agent) in ["agent-a", "agent-b"].iter().enumerate() {
            let mut r = result("web", day_start + i as i64, 0);
            r.agent_id = (*agent).into();
            r.status_code = None;
            r.error = Some("timeout".into());
            rows.push((r, Status::Down));
        }
        // ...buried under 300 later successes.
        for i in 0..300 {
            rows.push((result("web", day_start + 10_000 + i, 12), Status::Up));
        }
        s.insert_results(&rows).unwrap();

        // The newest 100 rows contain no failures at all — the original bug.
        let newest = s.history("web", 100, None, None, None).unwrap();
        assert_eq!(newest.len(), 100);
        assert!(
            newest.iter().all(|r| r.status == Status::Up),
            "the newest slice hides the failures, which is why the filter exists"
        );

        // Filtering by status surfaces exactly the two failures, with reasons.
        let bad = s
            .history("web", 500, None, None, Some(Status::Down))
            .unwrap();
        assert_eq!(bad.len(), 2, "both failures found regardless of recency");
        assert!(bad.iter().all(|r| r.error.as_deref() == Some("timeout")));

        // And the filter is a filter, not a truncation: successes still queryable.
        let good = s.history("web", 500, None, None, Some(Status::Up)).unwrap();
        assert_eq!(good.len(), 300);
    }

    #[test]
    fn prune_removes_rows_older_than_the_retention_window() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        let day = 86_400_000i64;

        // One recent result and one far outside the 30-day results window.
        s.insert_results(&[
            (result("a", now - 1000, 1), Status::Up),
            (result("a", now - 40 * day, 2), Status::Up),
        ])
        .unwrap();
        // One recent alert and one older than the 90-day alerts window.
        s.insert_alert("a", Status::Up, Status::Down, now - 1000)
            .unwrap();
        s.insert_alert("a", Status::Down, Status::Up, now - 100 * day)
            .unwrap();
        // An open incident (any age survives) and a resolved one older than
        // the alerts window (pruned).
        s.create_incident("open", now - 10 * day, Some("still down"))
            .unwrap();
        s.create_incident("old", now - 200 * day, Some("ancient"))
            .unwrap();
        s.resolve_incident("old", now - 100 * day).unwrap();

        let removed = s.prune(30, 90, now).unwrap();
        assert_eq!(removed, 3, "old result + old alert + old resolved incident");

        // The recent result and alert survive.
        let status = s.list_status(now).unwrap();
        assert_eq!(status.len(), 1, "only the recent result's check remains");
        assert_eq!(status[0].check_id, "a");
        assert_eq!(s.recent_alerts(10).unwrap().len(), 1);
        // The open incident survives; the resolved one is gone.
        let incidents = s.list_incidents(10, None).unwrap();
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].check_id, "open");
        assert_eq!(incidents[0].status, IncidentStatus::Open);
    }
}

#[cfg(test)]
mod segment_tests {
    use super::*;
    use crate::models::CheckKind;
    use chrono::DateTime;

    fn up_at(check_id: &str, ts_millis: i64) -> (CheckResult, Status) {
        (
            CheckResult {
                zone: "test-zone".into(),
                region: "test-region".into(),
                body: None,
                headers: None,
                check_id: check_id.into(),
                agent_id: "agent".into(),
                kind: CheckKind::Http,
                timestamp: DateTime::from_timestamp_millis(ts_millis).unwrap(),
                latency_ms: 10,
                status_code: Some(200),
                body_matched: None,
                error: None,
                failure: None,
            },
            Status::Up,
        )
    }

    #[test]
    fn segment_count_is_constant_and_width_follows_the_window() {
        // The strip keeps the same number of bars at every range; a shorter
        // window just means finer segments. 7 days over 90 segments is ~1h52m.
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        const DAY: i64 = 86_400_000;

        for days in [7usize, 30, 90] {
            let b = s.uptime_buckets("x", days, 90, now).unwrap();
            assert_eq!(b.len(), 90, "{days}d must still yield 90 segments");

            let width = b[0].end - b[0].start;
            assert_eq!(
                width,
                days as i64 * DAY / 90,
                "{days}d segment width is the window split 90 ways"
            );
            // Segments tile with no gaps and the last one ends at `now`.
            assert_eq!(b[1].start, b[0].end, "segments are contiguous");
            assert_eq!(b[89].end, now, "the newest segment ends at now");
        }
    }

    #[test]
    fn a_result_lands_in_the_segment_covering_its_timestamp() {
        let s = Storage::open(":memory:").unwrap();
        let now = 1_000_000_000_000i64;
        const DAY: i64 = 86_400_000;
        // 7 days / 90 segments => 6720000 ms each.
        let width = 7 * DAY / 90;

        // One result inside the newest segment, one inside the oldest.
        s.insert_results(&[up_at("x", now - 1), up_at("x", now - 90 * width + 1)])
            .unwrap();

        let b = s.uptime_buckets("x", 7, 90, now).unwrap();
        assert_eq!(b[89].total, 1, "newest segment holds the recent result");
        assert_eq!(b[0].total, 1, "oldest segment holds the old result");
        let empty = b[1..89].iter().map(|x| x.total).sum::<u32>();
        assert_eq!(empty, 0, "nothing leaks into the segments between");
        assert_eq!(b[45].uptime, None, "an empty segment is no-data, not 0%");
    }
}

#[cfg(test)]
mod migration_tests {
    use super::*;

    /// Opening a database written by an older build must migrate it, not
    /// crash-loop. `:memory:` can never catch this: a fresh database is created
    /// with the current schema, so the ALTER path is only reachable against a
    /// file that predates the new columns. This is exactly how a half-applied
    /// migration reached a running deployment.
    #[test]
    fn a_database_from_before_visible_and_eval_is_migrated_on_open() {
        let dir = std::env::temp_dir().join(format!("warden-migration-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("old.duckdb");
        let path_str = path.to_str().unwrap().to_string();
        let _ = std::fs::remove_file(&path);

        // The `checks` table exactly as the previous release wrote it.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE checks (
                    name        VARCHAR PRIMARY KEY,
                    kind        VARCHAR NOT NULL,
                    target      VARCHAR NOT NULL,
                    interval_secs BIGINT NOT NULL,
                    timeout_secs  BIGINT NOT NULL,
                    group_name  VARCHAR,
                    latency_graph BOOLEAN NOT NULL,
                    enabled     BOOLEAN NOT NULL,
                    expect_status INTEGER,
                    expect_body_contains VARCHAR,
                    updated_at  BIGINT  NOT NULL
                );
                INSERT INTO checks VALUES
                    ('legacy', 'http', 'https://legacy.test', 30, 10, 'Old', true, true, 200, NULL, 0);",
            )
            .unwrap();
        }

        let store = Storage::open(&path_str).expect("opening an older database must migrate it");
        let checks = store.list_checks().expect("reading migrated rows");
        let legacy = checks
            .iter()
            .find(|c| c.name == "legacy")
            .expect("row survives");
        assert!(
            legacy.visible,
            "a row that predates `visible` must default to shown, not hidden"
        );
        assert_eq!(legacy.eval, None);
        assert_eq!(legacy.group.as_deref(), Some("Old"));

        // Sections are new too, and must be adopted from existing group names.
        store.seed_sections_from_checks().unwrap();
        let sections = store.list_sections().unwrap();
        assert!(
            sections.iter().any(|s| s.name == "Old"),
            "existing group names should become sections, got {sections:?}"
        );

        // Opening again must be a no-op rather than a duplicate-column error.
        drop(store);
        Storage::open(&path_str).expect("re-opening a migrated database must succeed");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incident_lifecycle_open_and_resolve() {
        let s = Storage::open(":memory:").unwrap();

        // Nothing open yet, and opening when none is open fails cleanly.
        assert_eq!(s.open_incident_for("svc").unwrap(), None);
        assert_eq!(s.resolve_incident("svc", 2_000).unwrap(), None);

        let opened = s
            .create_incident("svc", 1_000, Some("connection refused"))
            .unwrap();
        assert_eq!(opened.status, IncidentStatus::Open);
        assert_eq!(opened.resolved_at, None);
        assert_eq!(opened.details.as_deref(), Some("connection refused"));
        assert_eq!(opened.id, "svc-1000");

        // A second open while one is open rides on the existing incident.
        let again = s.create_incident("svc", 1_500, None).unwrap();
        assert_eq!(again.id, "svc-1000", "the original incident stays open");
        assert_eq!(s.list_incidents(100, None).unwrap().len(), 1);

        // Resolving closes it and records when.
        let resolved = s.resolve_incident("svc", 2_000).unwrap().unwrap();
        assert_eq!(resolved.status, IncidentStatus::Resolved);
        assert_eq!(resolved.resolved_at, Some(2_000));
        assert_eq!(s.open_incident_for("svc").unwrap(), None);

        // After resolution a new outage opens a fresh incident.
        let second = s.create_incident("svc", 3_000, None).unwrap();
        assert_eq!(second.id, "svc-3000");
    }

    #[test]
    fn incidents_are_listed_newest_first_and_filterable() {
        let s = Storage::open(":memory:").unwrap();
        s.create_incident("a", 1_000, None).unwrap();
        s.create_incident("b", 2_000, None).unwrap();
        // "a" is still open, so this rides on the 1000 incident rather than
        // opening a second one.
        s.create_incident("a", 3_000, None).unwrap();
        s.resolve_incident("a", 3_500).unwrap();

        let all = s.list_incidents(100, None).unwrap();
        let order: Vec<String> = all.iter().map(|i| i.id.clone()).collect();
        assert_eq!(order, vec!["b-2000", "a-1000"]);

        let open = s.list_incidents(100, Some(IncidentStatus::Open)).unwrap();
        assert_eq!(
            open.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["b-2000"],
            "only b is still open"
        );

        let resolved = s
            .list_incidents(100, Some(IncidentStatus::Resolved))
            .unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].id, "a-1000");

        let for_a = s.incidents_for_check("a", 100).unwrap();
        assert_eq!(
            for_a.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            vec!["a-1000"]
        );
        assert_eq!(
            s.incidents_for_check("a", 1).unwrap().len(),
            1,
            "limit applies"
        );
    }
}
