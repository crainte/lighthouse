use super::*;

/// Column names DuckDB currently has for `table`, used by `verify_schema` to
/// diff an opened database against the schema the DDL constant declares.
fn table_column_names(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT column_name FROM information_schema.columns WHERE table_name = ?")?;
    let rows = stmt.query_map(duckdb::params![table], |r| r.get::<_, String>(0))?;
    rows.collect::<std::result::Result<_, _>>()
        .map_err(Into::into)
}

/// Compare an opened database's tables and columns against what `SCHEMA_DDL`
/// declares, by applying the same DDL to a scratch in-memory connection and
/// diffing `information_schema.columns` rather than hand-maintaining a second
/// list that could drift from the DDL itself.
///
/// A missing table or column fails clearly: this project runs no in-place
/// migrations before 1.0 (see SCALING.md's "Pre-1.0 database policy"), so a
/// database that predates a column this binary requires must be rebuilt, not
/// silently read as though the column were there. Extra columns — a rollback
/// to an older binary reopening a newer database — are tolerated.
pub(super) fn verify_schema(conn: &Connection) -> Result<()> {
    let reference = Connection::open_in_memory()?;
    reference.execute_batch(SCHEMA_DDL)?;
    let tables: Vec<String> = {
        let mut stmt = reference.prepare(
            "SELECT table_name FROM information_schema.tables
             WHERE table_schema = 'main' AND table_type = 'BASE TABLE'",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    for table in tables {
        let expected = table_column_names(&reference, &table)?;
        let actual = table_column_names(conn, &table)?;
        for column in &expected {
            if !actual.iter().any(|c| c == column) {
                bail!(
                    "database schema is missing column `{table}.{column}`: this database \
                     predates the supported schema and cannot be opened. See SCALING.md's \
                     \"Pre-1.0 database policy\" for the rebuild procedure."
                );
            }
        }
    }
    Ok(())
}

/// The complete table schema, applied verbatim to every opened database.
/// `CREATE TABLE IF NOT EXISTS` is a no-op wherever a table already exists,
/// so this changes nothing on a currently-running production database: every
/// column below was, at some point, added by an `ALTER TABLE` this project
/// used to run on `open`, and any database that has been opened by a build
/// carrying that `ALTER` already has the column on disk. What's new is that
/// a database that predates one of these columns is no longer silently
/// patched up — see `verify_schema`, and SCALING.md's "Pre-1.0 database
/// policy".
pub(super) const SCHEMA_DDL: &str = "CREATE TABLE IF NOT EXISTS check_results (
    check_id   UUID    NOT NULL,
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
-- One row per check, probe domain, and UTC minute/hour. `agent_id`
-- is the independent observation domain: replicas in a domain must
-- deduplicate, while distinct domains must never be averaged away.
CREATE TABLE IF NOT EXISTS check_latency_minute_rollups (
    check_id UUID NOT NULL,
    agent_id VARCHAR NOT NULL,
    bucket_start BIGINT NOT NULL,
    attempts BIGINT NOT NULL,
    up BIGINT NOT NULL,
    latency_sum BIGINT NOT NULL,
    latency_min BIGINT,
    latency_max BIGINT,
    PRIMARY KEY (check_id, agent_id, bucket_start)
);
CREATE TABLE IF NOT EXISTS check_latency_hour_rollups (
    check_id UUID NOT NULL,
    agent_id VARCHAR NOT NULL,
    bucket_start BIGINT NOT NULL,
    attempts BIGINT NOT NULL,
    up BIGINT NOT NULL,
    latency_sum BIGINT NOT NULL,
    latency_min BIGINT,
    latency_max BIGINT,
    PRIMARY KEY (check_id, agent_id, bucket_start)
);
CREATE TABLE IF NOT EXISTS latest_check_results (
    check_id    UUID PRIMARY KEY,
    agent_id    VARCHAR NOT NULL,
    ts_millis   BIGINT NOT NULL,
    status      VARCHAR NOT NULL,
    latency_ms  BIGINT NOT NULL,
    status_code INTEGER,
    error       VARCHAR,
    failure     VARCHAR
);
CREATE TABLE IF NOT EXISTS result_replication_watermarks (
    check_id     UUID PRIMARY KEY,
    min_ts       BIGINT NOT NULL,
    max_ts       BIGINT NOT NULL,
    result_count BIGINT NOT NULL
);
-- One row per rollup table ('check_latency_minute_rollups' or
-- '..._hour_rollups'), maintained on every write instead of scanned
-- on every digest — the same reason `result_replication_watermarks`
-- exists instead of aggregating `check_results` per sweep, and more
-- pointed here: a rollup bucket lives for the whole retention tier
-- (90 days/13 months), so the table this summarizes is not small.
-- See `RollupDigest` for why a rollup needs more than max/count to
-- be compared safely.
CREATE TABLE IF NOT EXISTS rollup_replication_watermarks (
    stream           VARCHAR PRIMARY KEY,
    max_bucket_start BIGINT NOT NULL,
    row_count        BIGINT NOT NULL,
    attempts_sum     BIGINT NOT NULL,
    up_sum           BIGINT NOT NULL,
    latency_sum_sum  BIGINT NOT NULL
);
-- `rollup_digest` is a point lookup, not a scan, so both rows must exist
-- from the first open onward rather than only once something has been
-- rolled up. Harmless to repeat: an existing row (real counters, on every
-- database that has ever ingested a result) is left untouched.
INSERT INTO rollup_replication_watermarks
    (stream, max_bucket_start, row_count, attempts_sum, up_sum, latency_sum_sum)
VALUES ('check_latency_minute_rollups', 0, 0, 0, 0, 0),
       ('check_latency_hour_rollups', 0, 0, 0, 0, 0)
ON CONFLICT (stream) DO NOTHING;
-- A durable record of `anti_entropy_sweep` finding a peer whose
-- retained raw history starts after ours ends: nothing anti-entropy
-- can do closes that hole in raw detail, so it is written down
-- rather than only logged and counted. `(check_id, peer)` is the
-- key rather than an append-only id, because the same standing gap
-- is redetected every sweep it persists and a row per sweep would
-- grow without bound; each detection instead refreshes the one row
-- for that pairing. See SCALING.md's Data retention tiers section for
-- what recovery does and does not mean once minute/hour rollups
-- replicate on their own (item A alongside this table).
CREATE TABLE IF NOT EXISTS raw_data_gaps (
    check_id    UUID    NOT NULL,
    peer        VARCHAR NOT NULL,
    from_ts     BIGINT  NOT NULL,
    to_ts       BIGINT  NOT NULL,
    observed_at BIGINT  NOT NULL,
    PRIMARY KEY (check_id, peer)
);
-- The event log: one row per confirmed state transition.
-- `incident_id` is stamped as the row is written, because
-- membership is a fact the coordinator knows at that moment and a
-- reader cannot re-derive it — a time-window join cannot tell two
-- overlapping causes apart, and it would silently absorb unrelated
-- history the moment a check joined an incident's affects list.
-- No `alert_type` column: StateChange/Degradation/Recovery is a
-- function of the two state columns, so storing it invites drift.
CREATE TABLE IF NOT EXISTS replication_stream_watermarks (
    stream      VARCHAR PRIMARY KEY,
    max_millis  BIGINT NOT NULL,
    max_id      VARCHAR NOT NULL,
    item_count  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts (
    id          VARCHAR PRIMARY KEY,
    scope       VARCHAR NOT NULL,
    node_id     VARCHAR NOT NULL,
    -- NULL for a first-ever observation: nothing to move from.
    previous_state VARCHAR,
    current_state  VARCHAR NOT NULL,
    ts_millis   BIGINT  NOT NULL,
    incident_id VARCHAR
);
-- The operator-facing record of an outage. `state` is the ONLY
-- lifecycle field: open means state <> 'resolved'. Duration is
-- derived, never stored, and so is acknowledgement: that is the
-- existence of an incident_updates row with an author, not a
-- column here.
CREATE TABLE IF NOT EXISTS incidents (
    id          VARCHAR PRIMARY KEY,
    title       VARCHAR NOT NULL,
    -- detected, investigating, identified, monitoring, resolved.
    -- Automatic incidents start at 'detected': a probe noticing is
    -- not a human investigating.
    state       VARCHAR NOT NULL,
    started_at  BIGINT  NOT NULL,
    resolved_at BIGINT,
    global      BOOLEAN NOT NULL DEFAULT FALSE,
    -- NULL for a manually opened incident, which reconciliation
    -- therefore never touches.
    origin_check UUID,
    version_millis BIGINT NOT NULL,
    version_node VARCHAR NOT NULL
);
-- Which checks an incident affects. `source` (origin, dependency,
-- manual) exists so reconciliation can retract what it added
-- without disturbing what an operator added by hand.
CREATE TABLE IF NOT EXISTS incident_checks (
    incident_id VARCHAR NOT NULL,
    check_id    UUID    NOT NULL,
    source      VARCHAR NOT NULL,
    PRIMARY KEY (incident_id, check_id)
);
-- The narrative. Every state change is one of these, including the
-- two the coordinator writes itself on auto-open and auto-resolve,
-- so the timeline can never disagree with the state. A non-NULL
-- author is what makes an update an acknowledgement.
CREATE TABLE IF NOT EXISTS incident_updates (
    id          VARCHAR PRIMARY KEY,
    incident_id VARCHAR NOT NULL,
    state       VARCHAR NOT NULL,
    body        VARCHAR NOT NULL,
    author      VARCHAR,
    created_at  BIGINT  NOT NULL
);
-- The CONFIRMED state per check, materialized for fast reads. The
-- raw `check_results` table keeps every observation; this table
-- holds what the coordinator decided they mean — a check with
-- `failures_before_down` = 3 stays 'up' here through its first two
-- failed probes. Read by the status page, seeded into the in-memory
-- tracker on startup so a restart resumes a confirmation in flight.
--
-- `pending_state`/`pending_since` are the hold-down timer, persisted
-- so it survives a restart: without `pending_since` a node rebooting
-- mid-window would restart the window, and a coordinator restarting
-- more often than the hold-down could suppress an outage indefinitely.
-- `pending_state` sits beside it because \"something has been pending
-- for 90 seconds\" alone can't say whether a newly-arrived Down
-- inherits that clock or restarts it.
CREATE TABLE IF NOT EXISTS current_state (
    check_id   UUID PRIMARY KEY,
    status     VARCHAR NOT NULL,
    last_check BIGINT  NOT NULL,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    pending_state VARCHAR,
    pending_since BIGINT
);
-- One agent's confirmed opinion of one check: the leaves of the
-- region tree. `current_state` above is the rolled-up conclusion;
-- this is the evidence it was rolled up from.
--
-- Persisted for the same reason `current_state` is — so a restart
-- resumes confirmations rather than restarting them — but *per
-- agent*, because that is now where `failures_before_down` is
-- counted. Without this, a restart would reset every agent's
-- in-flight confirmation at once and a real outage would take
-- another full threshold to re-confirm.
CREATE TABLE IF NOT EXISTS agent_check_state (
    check_id   UUID    NOT NULL,
    agent_id   VARCHAR NOT NULL,
     status     VARCHAR NOT NULL,
     consecutive_failures INTEGER NOT NULL DEFAULT 0,
     zone       VARCHAR NOT NULL DEFAULT 'unknown',
     region     VARCHAR NOT NULL DEFAULT 'unknown',
     last_check BIGINT  NOT NULL,
    PRIMARY KEY (check_id, agent_id)
);
-- Rollup policy per tree node. Deliberately NOT named `region_tree`
-- (design doc :1348), because it does not store the tree: structure
-- is derived from agent credentials, which cannot drift from the
-- real fleet the way a declared topology can. Only the policy — a
-- genuine operator decision that nothing else can supply — is
-- stored here.
--
-- `node_id` is a namespaced key: 'global', 'region:us-east',
-- 'zone:us-east-1a'. A node with no row uses the built-in default
-- for its level.
--
-- (updated_at, updated_by) makes this a citizen of the HLC
-- last-writer-wins config stream alongside checks, sections,
-- settings and credentials. A policy that existed only on whichever
-- coordinator an admin happened to hit would make two nodes
-- disagree about whether a service is down, which is precisely the
-- disagreement clustering exists to prevent.
CREATE TABLE IF NOT EXISTS rollup_policy (
    node_id       VARCHAR PRIMARY KEY,
    node_type     VARCHAR NOT NULL,
    degraded_when VARCHAR NOT NULL,
    down_when     VARCHAR NOT NULL,
    -- JSON, e.g. {\"degraded_q\":2,\"down_q\":3}. Quorum counts live
    -- here rather than in the threshold strings so those stay a
    -- closed vocabulary.
    config        VARCHAR,
    updated_at    BIGINT  NOT NULL,
    updated_by    VARCHAR
);
-- One row per published transition: the state a check was in, and
-- when it entered it. The open span is the row with the greatest
-- `since`.
--
-- This exists because uptime was being computed as the UP fraction
-- of raw result rows, which is wrong twice over: it is agent-blind
-- (a check probed by four agents with one permanently broken reads
-- 75% forever) and it is count-weighted, so changing a check's
-- interval silently reweights its history. Spans are
-- time-weighted, agree with what the status page actually showed,
-- and can express whether Degraded counted as downtime.
CREATE TABLE IF NOT EXISTS state_history (
    check_id UUID    NOT NULL,
    state    VARCHAR NOT NULL,
    since    BIGINT  NOT NULL,
    PRIMARY KEY (check_id, since)
);
-- Check definitions, editable at runtime from the admin UI. Seeded
-- from the config file on first boot; the table is authoritative
-- afterwards, so an edit isn't silently reverted by a restart.
-- `id` is the immutable identity — a UUIDv7 minted once, by the
-- application, and never reassigned; `name` is the mutable,
-- initially-unique display value everything else here is keyed
-- against. `group` is a SQL keyword, hence group_name.
CREATE TABLE IF NOT EXISTS checks (
    id          UUID PRIMARY KEY,
    name        VARCHAR NOT NULL UNIQUE,
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
    -- Display order on the status page.
    position    INTEGER DEFAULT 0,
    -- JSON rather than a comma-joined string: an agent id is arbitrary
    -- config text and a separator inside one would silently split it.
    agents      VARCHAR,
    -- The whole definition, serialized. A check now carries per-type
    -- option blocks, headers and schedules; a column per field was a
    -- migration treadmill, so the document is authoritative and the
    -- flat columns here survive only for SQL to filter and order by.
    config      VARCHAR,
    expect_status INTEGER,
    expect_body_contains VARCHAR,
    -- (updated_at, updated_by) is the HLC this row was last written
    -- at. It is the whole conflict-resolution story for clustered
    -- config: a higher stamp wins, ties break on the node id, and
    -- every node therefore picks the same winner without asking.
    updated_at  BIGINT  NOT NULL,
    updated_by  VARCHAR
);
-- One row per region a check's `placement.regions` explicitly
-- names, projected from `checks.config` the same way the flat
-- columns above are — so deciding which checks a region's
-- placement admits is a join, never a per-check JSON parse. A
-- check with no rows here has an empty `placement.regions`,
-- which `Placement` already treats as every region; queries
-- follow that same rule. See SCALING.md's Placement-scoped
-- configuration section, Phase 2: this is what lets a region's
-- desired set be computed without reading every check document
-- on every reconciliation sweep.
CREATE TABLE IF NOT EXISTS check_placement_regions (
    check_id UUID    NOT NULL,
    region   VARCHAR NOT NULL,
    PRIMARY KEY (check_id, region)
);
-- Status-page sections. A row here, rather than just a string on
-- each check, so a section can be created empty, renamed, and
-- ordered independently of what is in it.
CREATE TABLE IF NOT EXISTS sections (
    name     VARCHAR PRIMARY KEY,
    position INTEGER NOT NULL,
    collapsed BOOLEAN DEFAULT FALSE,
    -- 'inline' or 'page'. Stored as text rather than a boolean so
    -- a third mode does not need a second column.
    display VARCHAR DEFAULT 'inline',
    -- HLC, as on `checks`.
    updated_at BIGINT,
    updated_by VARCHAR
);
-- Per-zone agent credentials. Only the hash is kept: the plaintext
-- is returned once at mint and is not reconstructible from this.
-- `zone`/`region` live here rather than being asserted by the agent,
-- because placement trusts an agent's claimed location.
CREATE TABLE IF NOT EXISTS agent_credentials (
    id          VARCHAR PRIMARY KEY,
    zone        VARCHAR NOT NULL,
    region      VARCHAR NOT NULL,
    secret_hash VARCHAR NOT NULL,
    salt        VARCHAR NOT NULL,
    created_at  BIGINT  NOT NULL,
    -- Kept after revocation so an operator can still see that the
    -- credential existed and when it stopped working.
    revoked_at  BIGINT,
    -- HLC. Revocation replicates as an ordinary LWW update, which
    -- is what makes a revoked credential stop working fleet-wide
    -- rather than only on the node it was revoked from.
    updated_at  BIGINT,
    updated_by  VARCHAR
);
-- Page-level settings edited in the admin UI. One JSON document per
-- key: they are small, always read whole, and never filtered on
-- their internals, so a column per field would buy nothing but
-- migrations. The config file only seeds a key that is absent.
CREATE TABLE IF NOT EXISTS settings (
    key   VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL,
    -- HLC, as on `checks`. Whole-document LWW per key, which is
    -- what these already were locally — now across nodes.
    updated_at BIGINT,
    updated_by VARCHAR
);
-- Notifications that have not been delivered yet. A row exists
-- *because* a send failed: success deletes it, so an empty table is
-- the healthy state and there is no 'sent' row to prune.
--
-- It stores the transition to re-send rather than an alert id: the
-- retry needs check and states, nothing else, and a foreign key into
-- a table with its own retention window would dangle. No surrogate
-- Every delivery has an immutable id, so distinct transitions at
-- the same millisecond cannot replace one another.
--
-- `next_attempt` NULL means given up: attempts are exhausted and
-- nobody was paged. That is derived state made explicit, rather than
-- a `status` column that can disagree with `attempts`.
CREATE TABLE IF NOT EXISTS notification_queue (
    id             VARCHAR PRIMARY KEY,
    notifier       VARCHAR NOT NULL,
    check_id       UUID    NOT NULL,
    previous_state VARCHAR,
    current_state  VARCHAR NOT NULL,
    attempts       INTEGER NOT NULL,
    next_attempt   BIGINT,
    last_error     VARCHAR NOT NULL,
    queued_at      BIGINT  NOT NULL
);
-- Who else is in the cluster. Persisted rather than kept in memory
-- so a restarted node rejoins from what it already knew instead of
-- waiting to be re-seeded by whichever peer pings it first.
CREATE TABLE IF NOT EXISTS cluster_members (
    node_id    VARCHAR PRIMARY KEY,
    url        VARCHAR NOT NULL,
    -- Process start time. A restart supersedes stale gossip about
    -- the old process, which is what stops a returning node from
    -- flapping between alive and dead as old views arrive.
    generation BIGINT  NOT NULL,
    last_seen  BIGINT  NOT NULL,
    state      VARCHAR NOT NULL    -- alive | suspect | dead
);
-- Config deletes are themselves replicated state. Without this row
-- a peer's anti-entropy sweep sees a check we do not have, decides
-- we are behind, and hands the deleted check straight back.
-- `kind` names the config table, `id` its primary key.
CREATE TABLE IF NOT EXISTS config_tombstones (
    kind       VARCHAR NOT NULL,   -- check | section | setting | credential | rollup_policy
    id         VARCHAR NOT NULL,
    deleted_at BIGINT  NOT NULL,   -- HLC millis
    node_id    VARCHAR NOT NULL,   -- HLC tiebreak
    PRIMARY KEY (kind, id)
);
-- Who last polled which agent, and how fast. This used to live only
-- in `Inner::seen`, in memory, so two clustered coordinators
-- disagreed about who was actually polling; persisting it here
-- means replication carries fleet health for free instead of each
-- node needing its own gossip message for it.
CREATE TABLE IF NOT EXISTS agent_polls (
    agent_id  VARCHAR PRIMARY KEY,
    zone      VARCHAR NOT NULL,
    region    VARCHAR NOT NULL,
    -- `at` is a reserved word in DuckDB's SQL grammar, hence the
    -- column name differing from the Rust field it backs.
    polled_at BIGINT  NOT NULL,
    window_ms BIGINT  NOT NULL,
    rtt_ms    BIGINT,
    -- What the agent said it is, on its last poll to whichever node
    -- it polls. Nullable: an agent predating either declaration
    -- reports neither, and absent must not read as none-at-all.
    build     VARCHAR,
    -- Comma-separated wire names, stored unparsed so a kind this
    -- node has never heard of survives the round trip.
    declared_kinds VARCHAR,
    -- Node id of the coordinator polled, for the agent -> coordinator
    -- link in the fleet view. Nullable: a peer predating it omits it.
    coordinator VARCHAR
);
-- An incident delete is replicated state for the same reason a
-- config delete is: an absence alone cannot be told apart from
-- never having heard of it, so a peer's next push or the
-- reconciler would hand the deleted incident straight back.
CREATE TABLE IF NOT EXISTS incident_tombstones (
    id         VARCHAR PRIMARY KEY,
    deleted_at BIGINT  NOT NULL,
    node_id    VARCHAR NOT NULL
);";

#[cfg(test)]
mod tests {
    use super::*;
    /// A database missing a column the current schema requires must fail
    /// `Storage::open` clearly, not silently read the check back with a
    /// default it never had. Regression test for `verify_schema`.
    #[test]
    fn opening_a_database_missing_a_current_column_fails_clearly() {
        let path = std::env::temp_dir().join(format!(
            "lighthouse-missing-column-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path = path.to_string_lossy().into_owned();
        {
            let conn = Connection::open(&path).unwrap();
            // Shaped like the current `incidents` table but missing
            // `version_millis`/`version_node`, added after the first release.
            conn.execute_batch(
                "CREATE TABLE incidents (
                    id VARCHAR PRIMARY KEY,
                    title VARCHAR NOT NULL,
                    state VARCHAR NOT NULL,
                    started_at BIGINT NOT NULL,
                    resolved_at BIGINT,
                    global BOOLEAN NOT NULL DEFAULT FALSE,
                    origin_check UUID
                );",
            )
            .unwrap();
        }

        let message = match Storage::open(&path) {
            Ok(_) => panic!("a database missing a required column must not open"),
            Err(err) => err.to_string(),
        };
        assert!(
            message.contains("incidents.version_millis")
                || message.contains("incidents.version_node"),
            "error should name the missing column, got: {message}"
        );
        assert!(
            message.contains("Pre-1.0 database policy"),
            "error should point at the rebuild procedure, got: {message}"
        );
        std::fs::remove_file(path).unwrap();
    }

    /// A fresh file database reopens without error and keeps its rows —
    /// the ordinary, non-degraded path `verify_schema` must never reject.
    #[test]
    fn a_fresh_file_database_reopens_and_keeps_its_data() {
        let path = std::env::temp_dir().join(format!(
            "lighthouse-fresh-reopen-{}.duckdb",
            uuid::Uuid::now_v7()
        ));
        let path = path.to_string_lossy().into_owned();
        let check = CheckConfig {
            id: CheckId::now_v7(),
            name: "fresh".into(),
            target: "https://example.test".into(),
            ..Default::default()
        };
        {
            let s = Storage::open(&path).unwrap();
            s.upsert_check(&check, 1).unwrap();
        }

        let reopened = Storage::open(&path).expect("a fresh database must reopen cleanly");
        assert_eq!(reopened.list_checks().unwrap()[0].id, check.id);
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
}
