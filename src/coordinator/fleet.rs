//! Fleet liveness: which agents are up, judged by silence rather than a
//! registry.
//!
//! **Union of three sources.** An agent counts if it is polling this node (in
//! memory, so one that has not reported yet still shows), polling any node in
//! the cluster (replicated poll rows, so a three-node cluster does not read
//! two of its three regions down on every node just because `seen` only ever
//! holds who is polling *it*), or has reported results in the past
//! (persisted, so a restart does not empty the section). An agent seen by
//! more than one source is read at its best: polling is a live answer,
//! results are a trailing one.
//!
//! **Pure composition, not reads.** Everything here takes its inputs as plain
//! values — a map, some slices, a config — and returns rows or a verdict. The
//! lock-holding, store-querying half of the picture stays in `fleet_health`
//! and `fleet_members` back in `mod.rs`, which is what keeps the composition
//! and the thresholds testable without spinning up a server.

use super::storage;
use super::SeenAgent;
use serde::Serialize;

/// One row of the public agent section: one agent, or a whole region.
#[derive(Serialize, Debug)]
pub(super) struct FleetRowDto {
    /// What the row counts. Also what keeps row keys unique on the client: a
    /// region and an agent are allowed to share a name, and two rows keyed the
    /// same hang a Svelte `{#each}` rather than looking wrong.
    kind: RowKind,
    name: String,
    status: Health,
    /// Agents polling now, out of the agents the row covers. An agent row is
    /// 1/1 or 0/1, so the frontend needs no special case.
    healthy: u32,
    total: u32,
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum RowKind {
    Agent,
    Region,
}

/// Agent liveness — three states, not a check's UP/DOWN. An agent answering is
/// healthy even if every check it probes is down.
///
/// Declared worst-first: `Ord` is how two readings of one agent are merged, and
/// the better one wins.
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
enum Health {
    /// Long enough gone to be a decommission rather than an incident.
    Down,
    /// Not polling, but its results are recent enough to be an outage.
    Degraded,
    /// Polling within its own window.
    Up,
}

/// How long an agent may be silent before it reads as DOWN. With no registry,
/// silence is the only failure signal there is.
pub(super) const AGENT_STALE_AFTER_MS: i64 = 5 * 60 * 1000;
/// How long before a silent agent is treated as retired: still in the store,
/// still owning its history, but out of the way. Deliberately generous — an
/// agent down for an afternoon is an incident, not a decommission.
pub(super) const AGENT_RETIRED_AFTER_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// How long a *poll* may be missed before an agent stops counting toward
/// placement, derived from the cadence the agent declared.
///
/// Deliberately much tighter than `AGENT_STALE_AFTER_MS`, because the two
/// measure different clocks. Reporting cadence is the check's — a five-minute
/// cron check produces a result every five minutes, so judging a prober by its
/// results needs a generous window. Polling cadence is the agent's own, and it
/// asks for work every `poll_secs`, so missing several polls already means
/// gone.
///
/// Derived rather than fixed because a constant silently assumes one cadence:
/// 90s reads an agent on `poll_secs = 120` as unreachable while it is polling
/// happily, and holding a place is exclusive — the checks assigned to an agent
/// wrongly kept in the fleet are probed by nobody. As a product, detection is
/// the same number of missed cycles at any cadence.
///
/// `poll_secs` is `None` for an agent too old to declare one, and for entries
/// seeded from stored results at boot. Those keep the window this used to be
/// fixed at: the derived one would be *tighter* than what they were running
/// against, so a coordinator upgraded ahead of its fleet would start evicting
/// agents that had not changed anything.
const AGENT_UNDECLARED_CADENCE_MS: i64 = 90 * 1000;

pub(super) fn unreachable_after_ms(missed_polls: u32, poll_secs: Option<u64>) -> i64 {
    let Some(secs) = poll_secs else {
        return AGENT_UNDECLARED_CADENCE_MS;
    };
    // Clamped so a malformed or hostile `?poll_secs=0` cannot make the window
    // zero — an agent that fell out of the fleet on every poll would have its
    // checks handed round the remaining agents forever.
    i64::from(missed_polls.max(1)) * secs.clamp(1, 3600) as i64 * 1000
}

/// Liveness of one agent from how long it has been quiet.
///
/// Never `Up`: only a poll makes an agent healthy. Reporting results is a
/// trailing signal — a check on a five-minute cron says nothing about the last
/// four minutes — and an idle agent with no checks at all would read dead.
fn silence_health(age_ms: i64) -> Health {
    if age_ms <= AGENT_RETIRED_AFTER_MS {
        Health::Degraded
    } else {
        Health::Down
    }
}

/// A region row's colour from the share of its agents that are healthy.
fn row_status(healthy: u32, total: u32, cfg: &crate::config::AgentsDisplay) -> Health {
    if total == 0 {
        return Health::Down;
    }
    let pct = f64::from(healthy) * 100.0 / f64::from(total);
    if pct < cfg.down_below {
        Health::Down
    } else if pct < cfg.warn_below {
        Health::Degraded
    } else {
        Health::Up
    }
}

/// The public agent section: the rows an operator placed, in their order,
/// followed by whatever the explicit rows did not already cover.
///
/// The fleet is the union of who is polling this node (in memory, so an agent
/// that has not reported yet still counts), who is polling *any* node in the
/// cluster (`polls`, replicated), and who has reported (persisted, so a restart
/// does not empty the section). An agent in several is read at its best:
/// polling is a live answer, results are a trailing one.
///
/// The replicated poll rows are what stop a three-node cluster from showing
/// two of its three regions down on every node: `seen` only ever holds the
/// agents polling the node being asked.
///
/// Pure so the composition and the thresholds can be tested without a server;
/// `fleet_health` is the reads that feed it.
pub(super) fn fleet_rows(
    now: i64,
    seen: &std::collections::HashMap<String, SeenAgent>,
    polls: &[storage::AgentPollRow],
    reported: &[storage::AgentRow],
    cfg: &crate::config::AgentsDisplay,
) -> Vec<FleetRowDto> {
    use crate::config::{AgentsMode, FleetEntry};
    let mut fleet: std::collections::HashMap<&str, (&str, Health)> =
        std::collections::HashMap::new();
    for (id, s) in seen {
        let health = if now - s.at <= s.window_ms {
            Health::Up
        } else {
            silence_health(now - s.at)
        };
        fleet.insert(id, (&s.region, health));
    }
    // The window travels with the row: it is the cadence the agent declared to
    // whichever node it polled, and judging someone else's agent by this
    // node's idea of a cadence would read a slow poller as unreachable.
    for p in polls {
        let health = if now - p.at <= p.window_ms {
            Health::Up
        } else {
            silence_health(now - p.at)
        };
        fleet
            .entry(&p.agent_id)
            .and_modify(|e| e.1 = e.1.max(health))
            .or_insert((&p.region, health));
    }
    for r in reported {
        let health = silence_health(now - r.last_seen);
        fleet
            .entry(&r.agent_id)
            .and_modify(|e| e.1 = e.1.max(health))
            .or_insert((&r.region, health));
    }

    /// One agent's own row. Its three-state health is shown directly rather
    /// than through the region thresholds: 0 of 1 healthy is "silent", not
    /// "this region is 0% up".
    fn agent_row(name: &str, health: Health) -> FleetRowDto {
        FleetRowDto {
            kind: RowKind::Agent,
            name: name.to_string(),
            status: health,
            healthy: u32::from(health == Health::Up),
            total: 1,
        }
    }

    // Explicit rows first, in the order they were placed: a status page has to
    // be diffable across reloads, so nothing here is ordered by hash.
    let mut covered: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut rows: Vec<FleetRowDto> = Vec::with_capacity(cfg.rows.len());
    for entry in &cfg.rows {
        match entry {
            FleetEntry::Agent { name } => {
                // An agent absent from the fleet entirely reads down rather
                // than disappearing — "the box you asked about is not there"
                // is the answer, and a vanished row is not.
                let health = fleet.get(name.as_str()).map_or(Health::Down, |(_, h)| *h);
                covered.insert(name.as_str());
                rows.push(agent_row(name, health));
            }
            FleetEntry::Region { name } => {
                let mut healthy = 0;
                let mut total = 0;
                for (id, (region, health)) in &fleet {
                    if *region == name.as_str() {
                        // Counted even when the agent also has a row of its
                        // own above: the label says the whole region.
                        covered.insert(id);
                        total += 1;
                        healthy += u32::from(*health == Health::Up);
                    }
                }
                rows.push(FleetRowDto {
                    kind: RowKind::Region,
                    name: name.clone(),
                    status: row_status(healthy, total, cfg),
                    healthy,
                    total,
                });
            }
        }
    }

    // Then the remainder. `Off` means "nothing but what I placed", which is
    // also how the whole section is hidden: no explicit rows and nothing
    // automatic leaves nothing at all.
    let rest = fleet.iter().filter(|(id, _)| !covered.contains(*id));
    let mut auto: Vec<FleetRowDto> = match cfg.mode {
        AgentsMode::Off => Vec::new(),
        AgentsMode::Flat => rest
            .map(|(id, (_, health))| agent_row(id, *health))
            .collect(),
        AgentsMode::Regions => {
            let mut by_region: std::collections::HashMap<&str, (u32, u32)> =
                std::collections::HashMap::new();
            for (region, health) in rest.map(|(_, v)| v) {
                let e = by_region.entry(region).or_default();
                e.1 += 1;
                e.0 += u32::from(*health == Health::Up);
            }
            by_region
                .into_iter()
                .map(|(region, (healthy, total))| FleetRowDto {
                    kind: RowKind::Region,
                    name: region.to_string(),
                    status: row_status(healthy, total, cfg),
                    healthy,
                    total,
                })
                .collect()
        }
    };
    auto.sort_by(|a, b| a.name.cmp(&b.name));
    rows.extend(auto);
    rows
}

#[derive(Serialize)]
pub(super) struct FleetMemberDto {
    pub(super) agent_id: String,
    pub(super) region: String,
    /// Polling right now. Shown so an operator pinning a row can see they are
    /// about to publish something that is already silent.
    pub(super) healthy: bool,
}

#[cfg(test)]
mod fleet_tests {
    use super::*;
    use crate::config::{AgentsDisplay, AgentsMode};

    const NOW: i64 = 1_700_000_000_000;

    fn polling(region: &str, ago_ms: i64, poll_secs: u64) -> SeenAgent {
        SeenAgent {
            zone: format!("{region}-a"),
            region: region.into(),
            at: NOW - ago_ms,
            window_ms: unreachable_after_ms(3, Some(poll_secs)),
            rtt_ms: None,
            kinds: None,
            build: None,
        }
    }

    fn seen(agents: &[(&str, SeenAgent)]) -> std::collections::HashMap<String, SeenAgent> {
        agents
            .iter()
            .map(|(id, s)| ((*id).to_string(), s.clone()))
            .collect()
    }

    fn reported(agent_id: &str, region: &str, ago_ms: i64) -> storage::AgentRow {
        storage::AgentRow {
            agent_id: agent_id.into(),
            zone: format!("{region}-a"),
            region: region.into(),
            last_seen: NOW - ago_ms,
            checks_reporting: 1,
            results_24h: 1,
        }
    }

    fn row<'a>(rows: &'a [FleetRowDto], name: &str) -> &'a FleetRowDto {
        rows.iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("no row named {name}"))
    }

    /// The coupling the design called out: a fixed 90s window reads an agent on
    /// a two-minute cadence as gone while it is polling happily.
    #[test]
    fn the_window_follows_the_declared_cadence() {
        assert_eq!(unreachable_after_ms(3, Some(10)), 30_000);
        assert_eq!(unreachable_after_ms(3, Some(120)), 360_000);
        // Three missed polls, whatever the cadence.
        assert_eq!(unreachable_after_ms(5, Some(120)), 600_000);
        // An agent that declares nothing keeps the window this used to be fixed
        // at, so upgrading the coordinator first does not evict the fleet.
        assert_eq!(unreachable_after_ms(3, None), 90_000);
        // A zero cadence would make every agent permanently unreachable.
        assert!(unreachable_after_ms(3, Some(0)) > 0);
        assert!(unreachable_after_ms(0, Some(10)) > 0);
    }

    #[test]
    fn a_slow_poller_within_its_own_window_is_healthy() {
        // 100s of silence: past the old fixed 90s, inside 3 x 120s.
        let rows = fleet_rows(
            NOW,
            &seen(&[("box", polling("eu", 100_000, 120))]),
            &[],
            &[],
            &AgentsDisplay::default(),
        );
        assert_eq!(row(&rows, "eu").status, Health::Up);
    }

    /// Liveness, not check health: an agent with no results at all is healthy
    /// while it answers.
    #[test]
    fn an_idle_agent_that_polls_is_healthy() {
        let rows = fleet_rows(
            NOW,
            &seen(&[("idle", polling("eu", 1_000, 10))]),
            &[],
            &[],
            &AgentsDisplay::default(),
        );
        assert_eq!(row(&rows, "eu").healthy, 1);
    }

    #[test]
    fn silence_is_three_states_not_two() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &seen(&[
                ("live", polling("eu", 1_000, 10)),
                ("quiet", polling("eu", 10 * 60_000, 10)),
                ("gone", polling("eu", 8 * 24 * 3_600_000, 10)),
            ]),
            &[],
            &[],
            &cfg,
        );
        assert_eq!(row(&rows, "live").status, Health::Up);
        assert_eq!(row(&rows, "quiet").status, Health::Degraded);
        assert_eq!(row(&rows, "gone").status, Health::Down);
    }

    /// Results are a trailing signal, so they can only lift a silent agent to
    /// degraded — never to healthy.
    #[test]
    fn reporting_without_polling_never_reads_healthy() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &Default::default(),
            &[],
            &[reported("b", "eu", 5_000)],
            &cfg,
        );
        assert_eq!(row(&rows, "b").status, Health::Degraded);
    }

    fn polled(agent_id: &str, region: &str, ago_ms: i64, poll_secs: u64) -> storage::AgentPollRow {
        storage::AgentPollRow {
            agent_id: agent_id.into(),
            zone: format!("{region}-a"),
            region: region.into(),
            at: NOW - ago_ms,
            window_ms: unreachable_after_ms(3, Some(poll_secs)),
            rtt_ms: Some(7),
            // Liveness roll-up is what these tests are about; what an agent can
            // probe never enters it.
            build: None,
            kinds: None,
            coordinator: None,
        }
    }

    /// The reported bug: on a three-node cluster each coordinator's `seen` only
    /// holds the agents polling *it*, so two of the three regions rolled up
    /// 0/1 healthy and the status page called them down. A replicated poll row
    /// is a live answer wherever it was observed.
    #[test]
    fn a_peers_poll_row_alone_reads_healthy() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &Default::default(),
            &[polled("agent-b", "eu", 1_000, 10)],
            &[],
            &cfg,
        );
        assert_eq!(row(&rows, "agent-b").status, Health::Up);
    }

    /// The window travels with the row rather than being the receiver's: a peer
    /// applying its own cadence would call a slow poller unreachable.
    #[test]
    fn a_peers_poll_row_past_its_own_window_is_not_healthy() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        // 100s of silence on a 10s cadence (window 30s) is silent; the same
        // silence on a 120s cadence (window 360s) is not.
        let stale = fleet_rows(
            NOW,
            &Default::default(),
            &[polled("agent-b", "eu", 100_000, 10)],
            &[],
            &cfg,
        );
        assert_eq!(row(&stale, "agent-b").status, Health::Degraded);

        let slow = fleet_rows(
            NOW,
            &Default::default(),
            &[polled("agent-b", "eu", 100_000, 120)],
            &[],
            &cfg,
        );
        assert_eq!(row(&slow, "agent-b").status, Health::Up);
    }

    /// Both sources for one agent: the best reading wins, in either direction.
    /// A node's own `seen` is fresher than the last snapshot it broadcast, and
    /// a peer's row is fresher than a `seen` entry this node has let go stale.
    #[test]
    fn a_local_and_a_remote_row_for_one_agent_read_at_their_best() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let remote_is_better = fleet_rows(
            NOW,
            &seen(&[("a", polling("eu", 10 * 60_000, 10))]),
            &[polled("a", "eu", 1_000, 10)],
            &[],
            &cfg,
        );
        assert_eq!(remote_is_better.len(), 1, "one agent, one row");
        assert_eq!(row(&remote_is_better, "a").status, Health::Up);

        let local_is_better = fleet_rows(
            NOW,
            &seen(&[("a", polling("eu", 1_000, 10))]),
            &[polled("a", "eu", 10 * 60_000, 10)],
            &[],
            &cfg,
        );
        assert_eq!(row(&local_is_better, "a").status, Health::Up);
    }

    /// A restart empties `seen`; the persisted results keep the section honest
    /// until the fleet polls again, and the live reading wins once it does.
    #[test]
    fn the_live_reading_beats_the_stored_one() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Flat,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &seen(&[("a", polling("eu", 1_000, 10))]),
            &[],
            &[reported("a", "eu", 6 * 24 * 3_600_000)],
            &cfg,
        );
        assert_eq!(rows.len(), 1, "one agent, one row: {rows:?}");
        assert_eq!(row(&rows, "a").status, Health::Up);
    }

    #[test]
    fn a_region_row_colours_on_the_share_that_is_healthy() {
        let cfg = AgentsDisplay::default();
        let half = fleet_rows(
            NOW,
            &seen(&[
                ("a", polling("eu", 1_000, 10)),
                ("b", polling("eu", 10 * 60_000, 10)),
            ]),
            &[],
            &[],
            &cfg,
        );
        // 50% healthy: under warn_below (100), not under down_below (50).
        assert_eq!(row(&half, "eu").status, Health::Degraded);
        assert_eq!((row(&half, "eu").healthy, row(&half, "eu").total), (1, 2));

        let none = fleet_rows(
            NOW,
            &seen(&[("a", polling("eu", 10 * 60_000, 10))]),
            &[],
            &[],
            &cfg,
        );
        assert_eq!(row(&none, "eu").status, Health::Down);
    }

    #[test]
    fn thresholds_are_configurable() {
        let lenient = AgentsDisplay {
            warn_below: 50.0,
            down_below: 25.0,
            ..Default::default()
        };
        // 50% healthy is green here and amber under the defaults.
        assert_eq!(row_status(1, 2, &lenient), Health::Up);
        assert_eq!(
            row_status(1, 2, &AgentsDisplay::default()),
            Health::Degraded
        );
        assert_eq!(row_status(1, 5, &lenient), Health::Down);
    }

    fn agent(name: &str) -> crate::config::FleetEntry {
        crate::config::FleetEntry::Agent { name: name.into() }
    }
    fn region(name: &str) -> crate::config::FleetEntry {
        crate::config::FleetEntry::Region { name: name.into() }
    }
    fn names(rows: &[FleetRowDto]) -> Vec<&str> {
        rows.iter().map(|r| r.name.as_str()).collect()
    }

    /// The ask this model exists for: "show agent-b, then the whole us-east
    /// region" — one agent and one region, in that order, mixed in one list.
    #[test]
    fn rows_are_published_in_the_order_they_were_placed() {
        let cfg = AgentsDisplay {
            rows: vec![agent("office-box"), region("us")],
            mode: AgentsMode::Off,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &seen(&[
                ("office-box", polling("eu", 1_000, 10)),
                ("eu-1", polling("eu", 1_000, 10)),
                ("us-1", polling("us", 1_000, 10)),
                ("us-2", polling("us", 10 * 60_000, 10)),
            ]),
            &[],
            &[],
            &cfg,
        );
        assert_eq!(
            names(&rows),
            ["office-box", "us"],
            "placed order, nothing else"
        );
        assert_eq!(rows[0].kind, RowKind::Agent);
        assert_eq!(rows[1].kind, RowKind::Region);
        // The region row counts its whole region: one of two healthy.
        assert_eq!((rows[1].healthy, rows[1].total), (1, 2));
        assert_eq!(rows[1].status, Health::Degraded);
        // eu-1 was never placed and nothing automatic is on, so it is absent.
    }

    /// A region row means the whole region even when one of its agents is also
    /// listed on its own — a row labelled `us` that quietly omitted a machine
    /// would be a lie.
    #[test]
    fn a_region_row_still_counts_an_agent_shown_individually() {
        let cfg = AgentsDisplay {
            rows: vec![agent("us-1"), region("us")],
            mode: AgentsMode::Off,
            ..Default::default()
        };
        let rows = fleet_rows(
            NOW,
            &seen(&[
                ("us-1", polling("us", 1_000, 10)),
                ("us-2", polling("us", 1_000, 10)),
            ]),
            &[],
            &[],
            &cfg,
        );
        assert_eq!(names(&rows), ["us-1", "us"]);
        assert_eq!((rows[1].healthy, rows[1].total), (2, 2), "both, not one");
    }

    /// Everything the placed rows did not cover follows them, and only that.
    #[test]
    fn the_remainder_excludes_whatever_the_placed_rows_already_showed() {
        let fleet = seen(&[
            ("office-box", polling("eu", 1_000, 10)),
            ("eu-1", polling("eu", 1_000, 10)),
            ("us-1", polling("us", 1_000, 10)),
        ]);

        // An agent row takes that agent out of its region's automatic row.
        let rows = fleet_rows(
            NOW,
            &fleet,
            &[],
            &[],
            &AgentsDisplay {
                rows: vec![agent("office-box")],
                ..Default::default()
            },
        );
        assert_eq!(names(&rows), ["office-box", "eu", "us"]);
        assert_eq!(row(&rows, "eu").total, 1, "office-box is not counted twice");

        // A region row takes every one of its agents out of the remainder.
        let rows = fleet_rows(
            NOW,
            &fleet,
            &[],
            &[],
            &AgentsDisplay {
                rows: vec![region("eu")],
                mode: AgentsMode::Flat,
                ..Default::default()
            },
        );
        assert_eq!(names(&rows), ["eu", "us-1"], "only us-1 is left over");
    }

    #[test]
    fn a_placed_row_survives_the_remainder_being_off_but_nothing_else_does() {
        let cfg = AgentsDisplay {
            mode: AgentsMode::Off,
            rows: vec![agent("office-box")],
            ..Default::default()
        };
        let fleet = seen(&[("office-box", polling("eu", 1_000, 10))]);
        let rows = fleet_rows(NOW, &fleet, &[], &[reported("b", "eu", 5_000)], &cfg);
        assert_eq!(names(&rows), ["office-box"]);

        // With nothing placed, "off" is how the whole section disappears.
        let hidden = AgentsDisplay {
            mode: AgentsMode::Off,
            ..Default::default()
        };
        assert!(fleet_rows(NOW, &fleet, &[], &[reported("b", "eu", 5_000)], &hidden).is_empty());
    }

    /// A name nobody has ever reported still gets its row: "the box you asked
    /// about is not there" is an answer, a vanished row is not.
    #[test]
    fn a_placed_row_for_something_unknown_reads_down() {
        let cfg = AgentsDisplay {
            rows: vec![agent("never-existed"), region("never-deployed")],
            mode: AgentsMode::Off,
            ..Default::default()
        };
        let rows = fleet_rows(NOW, &Default::default(), &[], &[], &cfg);
        assert_eq!(names(&rows), ["never-existed", "never-deployed"]);
        assert!(rows.iter().all(|r| r.status == Health::Down), "{rows:?}");
    }

    /// The superseded `pinned` list still parses and becomes agent rows, so a
    /// stored document or config file written before this keeps working.
    #[test]
    fn a_legacy_pinned_list_becomes_agent_rows() {
        let legacy: AgentsDisplay =
            serde_json::from_str(r#"{"mode":"regions","pinned":["box-a","box-b"]}"#).unwrap();
        let upgraded = legacy.sanitized();
        assert_eq!(upgraded.rows, vec![agent("box-a"), agent("box-b")]);
        assert!(upgraded.pinned.is_empty(), "and is not carried twice");
        // Saving it back does not re-emit the legacy field.
        let json = serde_json::to_string(&upgraded).unwrap();
        assert!(!json.contains("pinned"), "{json}");
    }

    /// Two rows keyed the same hang the page rather than looking wrong, so an
    /// exact repeat is dropped — but an agent and a region may share a name.
    #[test]
    fn repeated_rows_are_dropped_but_a_name_may_be_both() {
        let cfg = AgentsDisplay {
            rows: vec![agent("us"), region("us"), agent("us"), agent("  ")],
            ..Default::default()
        }
        .sanitized();
        assert_eq!(cfg.rows, vec![agent("us"), region("us")]);
    }
}
