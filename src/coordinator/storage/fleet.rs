use super::*;

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

/// Mirrors `AgentCredential` for the wire only. `credential.rs` does not
/// derive `Serialize` — it is owned by a different concern and adding one
/// there is out of scope here — so this is the envelope's own copy of the
/// shape. It carries hashes and salts, never a plaintext secret, because
/// there is no plaintext left after mint to leak. Pinned by a round-trip test
/// that fails the moment the two struct shapes drift apart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(in crate::coordinator) struct RollupPolicyDoc {
    pub node_type: NodeType,
    pub policy: crate::rollup::RollupPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct CredentialDoc {
    pub(super) id: String,
    pub(super) zone: String,
    pub(super) region: String,
    pub(super) secret_hash: String,
    pub(super) salt: String,
    pub(super) created_at: i64,
    pub(super) revoked_at: Option<i64>,
}

impl From<&AgentCredential> for CredentialDoc {
    // Destructured exhaustively, with no `..`: a field added to
    // `AgentCredential` makes this fail to compile instead of silently
    // vanishing off the wire the next time a credential replicates.
    fn from(c: &AgentCredential) -> Self {
        let AgentCredential {
            id,
            zone,
            region,
            secret_hash,
            salt,
            created_at,
            revoked_at,
        } = c.clone();
        CredentialDoc {
            id,
            zone,
            region,
            secret_hash,
            salt,
            created_at,
            revoked_at,
        }
    }
}

impl From<CredentialDoc> for AgentCredential {
    fn from(c: CredentialDoc) -> Self {
        let CredentialDoc {
            id,
            zone,
            region,
            secret_hash,
            salt,
            created_at,
            revoked_at,
        } = c;
        AgentCredential {
            id,
            zone,
            region,
            secret_hash,
            salt,
            created_at,
            revoked_at,
        }
    }
}

/// Who last polled an agent, and how fast — the persisted twin of
/// `Inner::seen`, so a cluster of coordinators agrees on fleet health instead
/// of each holding its own view in memory.
///
/// `window_ms` travels with the row rather than being applied by the reader:
/// liveness is `now - at <= window_ms`, and the window is the cadence the
/// agent declared to *whichever* node it polled. A peer applying its own
/// `missed_polls` to someone else's agent would read an agent polling every
/// two minutes as unreachable while it is polling happily.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentPollRow {
    pub agent_id: String,
    pub zone: String,
    pub region: String,
    pub at: i64,
    pub window_ms: i64,
    pub rtt_ms: Option<i64>,
    /// What the agent reported it is running, for the same reason the rest of
    /// this row travels: an operator on any node should see the whole fleet,
    /// not only the agents that happen to poll the node they opened.
    #[serde(default)]
    pub build: Option<String>,
    /// Check kinds the agent declared, comma-separated as it sent them.
    ///
    /// `None` means it never declared — read as "anything", never as
    /// "nothing". Stored as the wire string rather than parsed: a peer may
    /// legitimately name a kind this node has never heard of, and reducing that
    /// to the kinds this binary knows would make an agent look less capable on
    /// one node than on another for no reason but build order.
    #[serde(default)]
    pub kinds: Option<String>,
    /// The node id of the coordinator the agent polled, so the fleet view can
    /// draw which coordinator each agent actually talks to. `None` from a peer
    /// predating it.
    #[serde(default)]
    pub coordinator: Option<String>,
}

impl AgentPollRow {
    /// Whether this row must be persisted and sent to peers when `previous`
    /// is what was last sent. A poll advances `at`/`rtt_ms` on every request,
    /// so an unchanged fleet is resent only once `at` has moved a third of the
    /// agent's own silence window: often enough that no peer sees a live
    /// agent go stale, rarely enough that a steady fleet costs nothing per tick.
    pub fn supersedes(&self, previous: &Self) -> bool {
        let presence = |row: &Self| Self {
            at: 0,
            rtt_ms: None,
            ..row.clone()
        };
        self.at.saturating_sub(previous.at) >= (self.window_ms / 3).max(1)
            || presence(self) != presence(previous)
    }
}

impl Storage {
    /// Delete every result an agent reported. The agent list is derived from
    /// these rows, so this is what makes an agent disappear.
    pub fn purge_agent(&self, agent_id: &str) -> Result<u64> {
        self.transaction(|store| {
            let rows = store.conn.execute(
                "DELETE FROM check_results WHERE agent_id = ?",
                duckdb::params![agent_id],
            )?;
            store.rebuild_result_watermarks()?;
            store.rebuild_latest_check_results()?;
            // The fleet view is the union of results and poll rows, so leaving the
            // poll row behind would purge an agent that then reappeared, healthy.
            store.delete_agent_poll(agent_id)?;
            Ok(rows as u64)
        })
    }

    // ---- Agent credentials ----

    /// Every minted credential, revoked ones included. The caller caches these
    /// and matches against them per request, so this is read at boot and after
    /// each mint or revoke rather than on the ingest path.
    pub fn list_agent_credentials(&self) -> Result<Vec<AgentCredential>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, zone, region, secret_hash, salt, created_at, revoked_at
             FROM agent_credentials ORDER BY region, zone, created_at",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AgentCredential {
                    id: row.get::<_, String>(0)?,
                    zone: row.get::<_, String>(1)?,
                    region: row.get::<_, String>(2)?,
                    secret_hash: row.get::<_, String>(3)?,
                    salt: row.get::<_, String>(4)?,
                    created_at: row.get::<_, i64>(5)?,
                    revoked_at: row.get::<_, Option<i64>>(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn insert_agent_credential(&self, c: &AgentCredential) -> Result<()> {
        self.transaction(|store| {
            store.conn.execute(
                "INSERT INTO agent_credentials
                     (id, zone, region, secret_hash, salt, created_at, revoked_at)
                  VALUES (?, ?, ?, ?, ?, ?, NULL)",
                duckdb::params![c.id, c.zone, c.region, c.secret_hash, c.salt, c.created_at],
            )?;
            store.sync_config_watermark(ConfigKind::Credential, &c.id)
        })
    }

    /// Returns whether a live credential was actually revoked, so the handler
    /// can 404 rather than report success for an id that never existed — or
    /// report success twice for one already revoked.
    pub fn revoke_agent_credential(&self, id: &str, now: i64) -> Result<bool> {
        self.transaction(|store| {
            let n = store.conn.execute(
                "UPDATE agent_credentials SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
                duckdb::params![now, id],
            )?;
            if n > 0 {
                store.sync_config_watermark(ConfigKind::Credential, id)?;
            }
            Ok(n > 0)
        })
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

    // ---- Cluster membership ----

    /// Persist the member view so a restarted node rejoins from what it knew.
    /// Delete-then-insert the whole set in one transaction: DuckDB commits per
    /// statement otherwise, which is the difference between one fsync and one
    /// per member on every heartbeat.
    pub fn save_cluster_members(
        &self,
        members: &[crate::coordinator::cluster::Member],
    ) -> Result<()> {
        self.transaction(|_| {
            let write = || -> Result<()> {
                self.conn.execute_batch("DELETE FROM cluster_members")?;
                let mut stmt = self.conn.prepare(
                    "INSERT INTO cluster_members (node_id, url, generation, last_seen, state)
                 VALUES (?, ?, ?, ?, ?)",
                )?;
                for m in members {
                    stmt.execute(duckdb::params![
                        m.node_id,
                        m.url,
                        m.generation,
                        m.last_seen,
                        m.state.as_str(),
                    ])?;
                }
                Ok(())
            };
            write()
        })
    }

    /// Drop per-peer state kept for decommissioned members' addresses.
    pub fn forget_peer_state(&self, urls: &[String]) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("DELETE FROM raw_data_gaps WHERE peer = ?")?;
        for url in urls {
            stmt.execute(duckdb::params![url])?;
        }
        Ok(())
    }

    /// Every persisted member, for rejoining the cluster on restart.
    pub fn list_cluster_members(&self) -> Result<Vec<crate::coordinator::cluster::Member>> {
        let mut stmt = self
            .conn
            .prepare("SELECT node_id, url, generation, last_seen, state FROM cluster_members")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::coordinator::cluster::Member {
                    node_id: row.get::<_, String>(0)?,
                    url: row.get::<_, String>(1)?,
                    generation: row.get::<_, i64>(2)?,
                    last_seen: row.get::<_, i64>(3)?,
                    state: crate::coordinator::cluster::MemberState::from_db(
                        &row.get::<_, String>(4)?,
                    ),
                    // Not stored: see `Member::build`. A restored row says who
                    // was out there, not what they are running now; the first
                    // ping after boot fills this in.
                    build: None,
                    protocol: None,
                })
            })?
            .collect::<std::result::Result<_, _>>()?;
        Ok(rows)
    }

    // ---- Fleet health ----

    /// Record when an agent was last polled and how long it took to answer.
    /// One row per agent: only the latest poll matters, so this replaces
    /// rather than accumulates.
    ///
    /// A **merge**, not a blind replace: a row is only overwritten by a
    /// strictly newer `at`. That is what makes applying a peer's rows
    /// idempotent and order-independent — batches cross the wire out of order,
    /// and a stale one arriving late must not un-see a live agent. Read then
    /// write is safe without a transaction because DuckDB is single-writer and
    /// the caller already holds the storage mutex.
    pub fn record_agent_poll(&self, row: &AgentPollRow) -> Result<()> {
        let newer_stored: i64 = self.conn.query_row(
            "SELECT count(*) FROM agent_polls WHERE agent_id = ? AND polled_at >= ?",
            duckdb::params![row.agent_id, row.at],
            |r| r.get(0),
        )?;
        if newer_stored > 0 {
            return Ok(());
        }
        self.conn.execute(
            "INSERT OR REPLACE INTO agent_polls
                 (agent_id, zone, region, polled_at, window_ms, rtt_ms, build, declared_kinds,
                  coordinator)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                row.agent_id,
                row.zone,
                row.region,
                row.at,
                row.window_ms,
                row.rtt_ms,
                row.build,
                row.kinds,
                row.coordinator,
            ],
        )?;
        Ok(())
    }

    /// Forget an agent's poll row. Called from `purge_agent`: the fleet view
    /// reads this table as well as the results, so a purge that left the row
    /// behind would put the agent straight back on the status page.
    pub fn delete_agent_poll(&self, agent_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM agent_polls WHERE agent_id = ?",
            duckdb::params![agent_id],
        )?;
        Ok(())
    }

    /// Every agent's last-known poll, for the fleet health view and for
    /// replicating it to peers.
    pub fn list_agent_polls(&self) -> Result<Vec<AgentPollRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT agent_id, zone, region, polled_at, window_ms, rtt_ms, build, declared_kinds,
                    coordinator
             FROM agent_polls",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AgentPollRow {
                    agent_id: row.get(0)?,
                    zone: row.get(1)?,
                    region: row.get(2)?,
                    at: row.get(3)?,
                    window_ms: row.get(4)?,
                    rtt_ms: row.get(5)?,
                    build: row.get(6)?,
                    kinds: row.get(7)?,
                    coordinator: row.get(8)?,
                })
            })?
            .collect::<std::result::Result<_, _>>()?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::storage::test_support::*;

    #[test]
    fn presence_is_resent_only_on_change_or_a_third_of_the_window() {
        let sent = poll("a", "z", "r", 1_000, 30_000);
        let mut polled = AgentPollRow {
            at: 10_999,
            rtt_ms: Some(4),
            ..sent.clone()
        };
        assert!(!polled.supersedes(&sent), "a routine poll is not news");
        polled.at = 11_000;
        assert!(
            polled.supersedes(&sent),
            "refresh before peers see it stale"
        );
        let moved = AgentPollRow {
            zone: "z2".into(),
            at: 1_001,
            ..sent.clone()
        };
        assert!(moved.supersedes(&sent), "a material change is sent at once");
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
    fn agent_polls_are_recorded_per_agent_and_replace_on_re_poll() {
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll(&AgentPollRow {
            rtt_ms: Some(20),
            ..poll("agent-a", "z1", "r1", 1000, 90_000)
        })
        .unwrap();
        s.record_agent_poll(&AgentPollRow {
            rtt_ms: Some(15),
            ..poll("agent-a", "z1", "r1", 2000, 90_000)
        })
        .unwrap();
        s.record_agent_poll(&poll("agent-b", "z2", "r2", 1500, 30_000))
            .unwrap();

        let mut polls = s.list_agent_polls().unwrap();
        polls.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        assert_eq!(polls.len(), 2, "one row per agent, not one per poll");
        assert_eq!(polls[0].agent_id, "agent-a");
        assert_eq!(polls[0].at, 2000, "the newer poll replaced the older one");
        assert_eq!(polls[0].rtt_ms, Some(15));
        assert_eq!(polls[0].window_ms, 90_000);
        assert_eq!(polls[1].agent_id, "agent-b");
        assert_eq!(polls[1].window_ms, 30_000, "the window is per agent");
        assert_eq!(polls[1].rtt_ms, None);
    }

    #[test]
    fn an_older_agent_poll_never_overwrites_a_newer_one() {
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll(&AgentPollRow {
            rtt_ms: Some(11),
            build: Some("0.2.0".into()),
            ..poll("agent-a", "z1", "r1", 5000, 90_000)
        })
        .unwrap();
        // A peer's batch arriving late. Replication is unordered, so this is
        // the normal case rather than an edge one.
        s.record_agent_poll(&AgentPollRow {
            rtt_ms: Some(99),
            build: Some("0.1.0".into()),
            ..poll("agent-a", "z1", "r1", 1000, 90_000)
        })
        .unwrap();

        let polls = s.list_agent_polls().unwrap();
        assert_eq!(polls.len(), 1);
        assert_eq!(polls[0].at, 5000, "the stale row lost");
        assert_eq!(polls[0].rtt_ms, Some(11));
        assert_eq!(
            polls[0].build.as_deref(),
            Some("0.2.0"),
            "a stale row must not roll the reported build backwards either"
        );
    }

    #[test]
    fn a_declaration_survives_the_round_trip_unparsed() {
        // Stored as the agent sent it, so a kind this node has never heard of
        // still reaches the fleet view rather than being narrowed away to the
        // kinds this particular binary happens to know.
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll(&AgentPollRow {
            build: Some("0.3.0 (abc123, built 2026-08-31 00:00:00 UTC)".into()),
            kinds: Some("http,tcp,warp-drive".into()),
            coordinator: Some("coord-a".into()),
            ..poll("agent-a", "z1", "r1", 1000, 90_000)
        })
        .unwrap();

        let polls = s.list_agent_polls().unwrap();
        assert_eq!(polls[0].coordinator.as_deref(), Some("coord-a"));
        assert_eq!(polls[0].kinds.as_deref(), Some("http,tcp,warp-drive"));
        assert_eq!(
            polls[0].build.as_deref(),
            Some("0.3.0 (abc123, built 2026-08-31 00:00:00 UTC)")
        );
    }

    #[test]
    fn purging_an_agent_drops_its_poll_row() {
        let s = Storage::open(":memory:").unwrap();
        s.record_agent_poll(&poll("agent-a", "z1", "r1", 1000, 90_000))
            .unwrap();
        s.purge_agent("agent-a").unwrap();
        assert!(
            s.list_agent_polls().unwrap().is_empty(),
            "a purged agent that keeps its poll row walks back onto the status page"
        );
    }

    #[test]
    fn credential_doc_round_trip_preserves_every_field() {
        let cred = AgentCredential {
            id: "cred-1".into(),
            zone: "z1".into(),
            region: "r1".into(),
            secret_hash: "hash".into(),
            salt: "salt".into(),
            created_at: 111,
            revoked_at: Some(222),
        };
        let doc = CredentialDoc::from(&cred);
        let json = serde_json::to_string(&doc).unwrap();
        assert_eq!(
            json,
            r#"{"id":"cred-1","zone":"z1","region":"r1","secret_hash":"hash","salt":"salt","created_at":111,"revoked_at":222}"#
        );
        let back: CredentialDoc = serde_json::from_str(&json).unwrap();
        // Whole-value comparison, not field by field: `From<&AgentCredential>`
        // destructures exhaustively, so a field added to `AgentCredential`
        // without a matching one here fails to compile rather than silently
        // dropping off the wire.
        assert_eq!(doc, back);
    }
}
