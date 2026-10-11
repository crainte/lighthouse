//! The global node's in-memory cache of per-region state, plus the aggregation
//! that turns many regions' summaries into the single cross-region view the
//! dashboard renders.
//!
//! There is no database here. A region's state lives only as long as it keeps
//! pushing; a region that goes silent past the staleness window reads as
//! `Unknown` and drops out of aggregation, which is exactly how a dark region
//! should look on a status page — not stale-but-green.

use crate::global::wire::{CheckSummary, RegionStatePush};
use crate::models::{CheckId, NodeState};
use parking_lot::RwLock;
use std::collections::{BTreeSet, HashMap};

/// One region's last known state, as cached from its most recent push.
#[derive(Debug, Clone)]
pub struct RegionState {
    pub health: NodeState,
    /// Healthy and total probe-agent counts the region last reported, so the
    /// global status page shows a real fraction rather than a fabricated 1/1.
    pub agents_healthy: u32,
    pub agents_total: u32,
    pub checks: HashMap<CheckId, CheckSummary>,
    /// Endpoints to route drill-down reads to, seeded from config and refreshed
    /// from each push's `advertise_url`.
    pub endpoints: Vec<String>,
    pub origin: String,
    /// The origin's push chain position, so a delta applies only on top of
    /// exactly the push it was computed against.
    pub epoch: i64,
    pub seq: u64,
    /// Wall-clock millis carried on the push that produced this entry.
    pub pushed_at: i64,
    /// Local receive time, used for staleness so a sender with a skewed clock
    /// cannot look perpetually fresh or perpetually dark.
    pub received_at: i64,
}

impl RegionState {
    /// A region is dark when nothing has arrived within the staleness window,
    /// judged by local receive time.
    pub fn is_stale(&self, now: i64, staleness_ms: i64) -> bool {
        now.saturating_sub(self.received_at) > staleness_ms
    }

    /// Health accounting for staleness: a dark region is `Unknown`, whatever it
    /// last claimed.
    pub fn effective_health(&self, now: i64, staleness_ms: i64) -> NodeState {
        if self.is_stale(now, staleness_ms) {
            NodeState::Unknown
        } else {
            self.health
        }
    }
}

/// What `apply_push` did with a push.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Applied,
    /// A snapshot older than what is cached; dropped.
    Superseded,
    /// A delta that does not extend the cached chain; send a snapshot.
    Resync,
}

/// The cache proper: region name -> its cached state. Guarded by an `RwLock`
/// because pushes (writes) are rare relative to dashboard reads.
#[derive(Default)]
pub struct GlobalCache {
    regions: RwLock<HashMap<String, RegionState>>,
    staleness_ms: i64,
}

/// One check as aggregated across every reporting region.
#[derive(Debug, Clone)]
pub struct AggregatedCheck {
    pub check_id: CheckId,
    /// Mutable display name from the freshest reporting region.
    pub name: String,
    /// Cross-region rolled-up state: the worst state any live region reports.
    pub state: NodeState,
    /// Per-region state for this check, region name -> state, live regions only.
    pub per_region: HashMap<String, NodeState>,
    /// The freshest region's presentation fields, so the dashboard has a
    /// latency, a code and a label to show for the check as a whole.
    pub latency_ms: u64,
    pub last_check: i64,
    pub uptime_24h: Option<f64>,
    pub status_code: Option<u16>,
    pub error: Option<String>,
    pub kind: Option<crate::models::CheckKind>,
    pub group: Option<String>,
    pub latency_graph: bool,
    pub position: i32,
}

/// Per-check facts the incident grouping reads; see `GlobalCache::check_facts`.
#[derive(Debug, Clone, Default)]
pub struct CheckFacts {
    pub name: Option<String>,
    pub depends_on: Vec<CheckId>,
    pub regions: BTreeSet<String>,
}

/// A region as summarised for the roster view (stands in for the cluster panel).
#[derive(Debug, Clone)]
pub struct RegionSummary {
    pub region: String,
    pub health: NodeState,
    pub origin: String,
    pub check_count: usize,
    pub stale: bool,
    /// Millis since the last push landed locally.
    pub age_ms: i64,
    /// Probe-agent health the region last reported, for a real fraction.
    pub agents_healthy: u32,
    pub agents_total: u32,
    pub endpoints: Vec<String>,
}

impl GlobalCache {
    pub fn new(staleness_secs: u64, seed: &[crate::config::GlobalRegion]) -> Self {
        let regions = seed
            .iter()
            .map(|r| {
                (
                    r.name.clone(),
                    RegionState {
                        health: NodeState::Unknown,
                        // Seeded regions have not reported agents yet.
                        agents_healthy: 0,
                        agents_total: 0,
                        checks: HashMap::new(),
                        endpoints: r.endpoints.clone(),
                        origin: String::new(),
                        epoch: 0,
                        seq: 0,
                        pushed_at: 0,
                        // Seeded regions start dark: config declares they exist
                        // and where to reach them, not that they are healthy.
                        received_at: 0,
                    },
                )
            })
            .collect();
        Self {
            regions: RwLock::new(regions),
            staleness_ms: (staleness_secs.max(1) as i64) * 1000,
        }
    }

    pub fn staleness_ms(&self) -> i64 {
        self.staleness_ms
    }

    pub fn staleness_snapshot(&self, now: i64) -> (u64, u64, i64) {
        let guard = self.regions.read();
        let mut stale = 0;
        let mut never_received = 0;
        let mut oldest_received_at = 0;
        for state in guard.values() {
            if state.received_at == 0 {
                never_received += 1;
            } else {
                if state.is_stale(now, self.staleness_ms) {
                    stale += 1;
                }
                if oldest_received_at == 0 || state.received_at < oldest_received_at {
                    oldest_received_at = state.received_at;
                }
            }
        }
        (stale, never_received, oldest_received_at)
    }

    /// Apply one push. A snapshot is last-writer-wins by `pushed_at`, `origin`
    /// breaking a tie, so a delayed push never overwrites a newer one. A delta
    /// applies only on top of exactly the push it names (same origin, epoch
    /// and `seq == delta_base`); anything else asks the sender to resync with
    /// a snapshot. This is the single mutation entrypoint.
    pub fn apply_push(&self, push: RegionStatePush, now: i64) -> PushOutcome {
        let mut guard = self.regions.write();
        if let Some(base) = push.delta_base {
            let Some(cur) = guard.get_mut(&push.region) else {
                return PushOutcome::Resync;
            };
            if cur.received_at == 0
                || cur.origin != push.origin
                || cur.epoch != push.epoch
                || cur.seq != base
            {
                return PushOutcome::Resync;
            }
            for id in &push.removed {
                cur.checks.remove(id);
            }
            cur.checks
                .extend(push.checks.into_iter().map(|c| (c.check_id, c)));
            merge_endpoint(&mut cur.endpoints, push.advertise_url.as_deref());
            cur.health = push.region_health;
            cur.agents_healthy = push.agents_healthy;
            cur.agents_total = push.agents_total;
            cur.seq = push.seq;
            cur.pushed_at = push.pushed_at;
            cur.received_at = now;
            return PushOutcome::Applied;
        }
        let endpoints = match guard.get(&push.region) {
            Some(cur) => {
                let newer = push.pushed_at > cur.pushed_at
                    || (push.pushed_at == cur.pushed_at && push.origin >= cur.origin)
                    || cur.received_at == 0;
                if !newer {
                    return PushOutcome::Superseded;
                }
                cur.endpoints.clone()
            }
            None => Vec::new(),
        };
        let mut endpoints = endpoints;
        merge_endpoint(&mut endpoints, push.advertise_url.as_deref());
        guard.insert(
            push.region.clone(),
            Self::state_from_push(push, endpoints, now),
        );
        PushOutcome::Applied
    }

    fn state_from_push(push: RegionStatePush, endpoints: Vec<String>, now: i64) -> RegionState {
        let checks = push.checks.into_iter().map(|c| (c.check_id, c)).collect();
        RegionState {
            health: push.region_health,
            agents_healthy: push.agents_healthy,
            agents_total: push.agents_total,
            checks,
            endpoints,
            origin: push.origin,
            epoch: push.epoch,
            seq: push.seq,
            pushed_at: push.pushed_at,
            received_at: now,
        }
    }

    /// The region roster, sorted by name for a stable render.
    pub fn roster(&self, now: i64) -> Vec<RegionSummary> {
        let guard = self.regions.read();
        let mut rows: Vec<RegionSummary> = guard
            .iter()
            .map(|(name, s)| {
                let stale = s.is_stale(now, self.staleness_ms);
                RegionSummary {
                    region: name.clone(),
                    health: s.effective_health(now, self.staleness_ms),
                    origin: s.origin.clone(),
                    check_count: s.checks.len(),
                    agents_healthy: s.agents_healthy,
                    agents_total: s.agents_total,
                    stale,
                    age_ms: if s.received_at == 0 {
                        -1
                    } else {
                        now.saturating_sub(s.received_at)
                    },
                    endpoints: s.endpoints.clone(),
                }
            })
            .collect();
        rows.sort_by(|a, b| a.region.cmp(&b.region));
        rows
    }

    /// Aggregate every live region's checks into a single cross-region view.
    /// A region past the staleness window contributes nothing — its checks are
    /// dropped rather than shown stale.
    pub fn aggregate(&self, now: i64) -> Vec<AggregatedCheck> {
        let guard = self.regions.read();
        let mut merged: HashMap<CheckId, AggregatedCheck> = HashMap::new();
        // Iterate regions in name order so the "freshest presentation fields"
        // tie-break is deterministic when two regions share a last_check.
        let mut regions: Vec<(&String, &RegionState)> = guard.iter().collect();
        regions.sort_by(|a, b| a.0.cmp(b.0));
        for (region_name, region) in &regions {
            if region.is_stale(now, self.staleness_ms) {
                continue;
            }
            for (check_id, summary) in &region.checks {
                let agg = merged.entry(*check_id).or_insert_with(|| AggregatedCheck {
                    check_id: *check_id,
                    name: summary.name.clone(),
                    state: NodeState::Unknown,
                    per_region: HashMap::new(),
                    latency_ms: 0,
                    last_check: 0,
                    uptime_24h: None,
                    status_code: None,
                    error: None,
                    kind: None,
                    group: None,
                    latency_graph: false,
                    position: i32::MAX,
                });
                agg.per_region.insert((*region_name).clone(), summary.state);
                agg.state = worst(agg.state, summary.state);
                agg.latency_graph = agg.latency_graph || summary.latency_graph;
                if summary.position != i32::MAX {
                    agg.position = agg.position.min(summary.position);
                }
                if agg.name.is_empty() && !summary.name.is_empty() {
                    agg.name = summary.name.clone();
                }
                agg.kind = agg.kind.or(summary.kind);
                if agg.group.is_none() {
                    agg.group = summary.group.clone();
                }
            }
        }

        // The visible evidence must explain the aggregate state. Select the
        // freshest contributor whose state equals the already-computed worst
        // state, rather than allowing a newer successful observation to erase
        // the error which made the global check Down or Degraded.
        for agg in merged.values_mut() {
            let evidence = regions
                .iter()
                .filter(|(_, region)| !region.is_stale(now, self.staleness_ms))
                .filter_map(|(_, region)| region.checks.get(&agg.check_id))
                .filter(|summary| summary.state == agg.state)
                .max_by_key(|summary| summary.last_check);
            if let Some(summary) = evidence {
                agg.last_check = summary.last_check;
                if !summary.name.is_empty() {
                    agg.name = summary.name.clone();
                }
                agg.latency_ms = summary.latency_ms;
                agg.uptime_24h = summary.uptime_24h;
                agg.status_code = summary.status_code;
                agg.error = summary.error.clone();
            }
        }

        let mut out: Vec<AggregatedCheck> = merged.into_values().collect();
        out.sort_by(|a, b| {
            a.position
                .cmp(&b.position)
                .then(a.check_id.cmp(&b.check_id))
        });
        out
    }

    /// Endpoints of the region that owns a check — the regions reporting it,
    /// in name order, live first. Used to route a drill-down read. When no
    /// region claims the check (deleted, or only known from config), every
    /// configured endpoint is a candidate.
    pub fn owner_endpoints(&self, check_id: CheckId, now: i64) -> Vec<String> {
        self.owner_region_endpoints(check_id, now)
            .into_iter()
            .flat_map(|(_, endpoints)| endpoints)
            .collect()
    }

    /// One reachable-coordinator candidate set for every region reporting a
    /// check. Unlike `owner_endpoints`, this preserves the regional boundary so
    /// global queries can collect and merge each region's raw response.
    pub fn owner_region_endpoints(
        &self,
        check_id: CheckId,
        now: i64,
    ) -> Vec<(String, Vec<String>)> {
        let guard = self.regions.read();
        let mut owners: Vec<(&String, &RegionState, bool)> = guard
            .iter()
            .filter(|(_, s)| s.checks.contains_key(&check_id))
            .map(|(name, s)| (name, s, !s.is_stale(now, self.staleness_ms)))
            .collect();
        owners.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(b.0)));
        let regions: Vec<(String, Vec<String>)> = owners
            .into_iter()
            .filter_map(|(name, state, _)| {
                let endpoints = dedup_preserve(state.endpoints.clone());
                (!endpoints.is_empty()).then(|| (name.clone(), endpoints))
            })
            .collect();
        if regions.is_empty() {
            let mut fallback: Vec<(String, Vec<String>)> = guard
                .iter()
                .filter_map(|(name, state)| {
                    let endpoints = dedup_preserve(state.endpoints.clone());
                    (!endpoints.is_empty()).then(|| (name.clone(), endpoints))
                })
                .collect();
            fallback.sort_by(|a, b| a.0.cmp(&b.0));
            fallback
        } else {
            regions
        }
    }

    /// What the global tier's incident presentation needs to know about each
    /// check, gathered in one pass over the region cache.
    ///
    /// `name` and `depends_on` are `CheckConfig` fields, identical wherever a
    /// check is placed, so any one live region states them accurately; a stale
    /// region contributes neither, matching `aggregate`. `regions` is different:
    /// it is where the check is placed, and a region that has gone quiet is
    /// still placed. Counting only live regions would let one dark region turn
    /// "partial outage" into "is down" merely by no longer being heard from.
    pub fn check_facts(&self, now: i64) -> HashMap<CheckId, CheckFacts> {
        let guard = self.regions.read();
        let mut out: HashMap<CheckId, CheckFacts> = HashMap::new();
        for (region_name, region) in guard.iter() {
            let live = !region.is_stale(now, self.staleness_ms);
            for (check_id, summary) in &region.checks {
                let facts = out.entry(*check_id).or_default();
                facts.regions.insert(region_name.clone());
                if !live {
                    continue;
                }
                if facts.name.is_none() && !summary.name.is_empty() {
                    facts.name = Some(summary.name.clone());
                }
                if facts.depends_on.is_empty() {
                    facts.depends_on = summary.depends_on.clone();
                }
            }
        }
        out
    }

    /// The cross-region rolled-up state per check, as a map — a cheap snapshot
    /// the push handler diffs before and after applying a push to decide which
    /// checks changed and thus which `state_change` events to broadcast to live
    /// dashboards.
    pub fn state_snapshot(&self, now: i64) -> HashMap<CheckId, NodeState> {
        self.aggregate(now)
            .into_iter()
            .map(|c| (c.check_id, c.state))
            .collect()
    }

    /// One entry per known region: its name and its coordinator endpoints,
    /// live regions first. This is the fan-out target for an admin write, which
    /// must reach *every* region exactly once (write to any one live
    /// coordinator per region — the cluster replicates it internally), unlike a
    /// drill-down read that only needs a single answer.
    pub fn region_endpoints(&self, now: i64) -> Vec<(String, Vec<String>)> {
        let guard = self.regions.read();
        let mut regions: Vec<(&String, &RegionState)> = guard.iter().collect();
        regions.sort_by(|a, b| {
            let a_live = !a.1.is_stale(now, self.staleness_ms);
            let b_live = !b.1.is_stale(now, self.staleness_ms);
            b_live.cmp(&a_live).then(a.0.cmp(b.0))
        });
        regions
            .into_iter()
            .filter(|(_, s)| !s.endpoints.is_empty())
            .map(|(name, s)| (name.clone(), dedup_preserve(s.endpoints.clone())))
            .collect()
    }

    /// Every known coordinator endpoint, live regions first — the fallback fan
    /// target for a read that names no check.
    pub fn all_endpoints(&self, now: i64) -> Vec<String> {
        let guard = self.regions.read();
        let mut regions: Vec<&RegionState> = guard.values().collect();
        regions.sort_by(|a, b| {
            let a_live = !a.is_stale(now, self.staleness_ms);
            let b_live = !b.is_stale(now, self.staleness_ms);
            b_live.cmp(&a_live)
        });
        dedup_preserve(
            regions
                .into_iter()
                .flat_map(|s| s.endpoints.clone())
                .collect(),
        )
    }
}

/// `NodeState` has no ordering that means "worse", so spell it out. The rule
/// matches how a region rollup reads a mix: any Down makes the whole thing
/// Down, else any Degraded degrades it, else Up if anything is Up, else nothing
/// is known.
pub fn worst(a: NodeState, b: NodeState) -> NodeState {
    use NodeState::{Degraded, Down, Unknown, Up};
    match (a, b) {
        (Down, _) | (_, Down) => Down,
        (Degraded, _) | (_, Degraded) => Degraded,
        (Up, _) | (_, Up) => Up,
        (Unknown, Unknown) => Unknown,
    }
}

fn merge_endpoint(endpoints: &mut Vec<String>, url: Option<&str>) {
    if let Some(u) = url {
        let u = u.trim().trim_end_matches('/').to_string();
        if !u.is_empty() && !endpoints.contains(&u) {
            endpoints.push(u);
        }
    }
}

fn dedup_preserve(items: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    items
        .into_iter()
        .filter(|s| !s.is_empty() && seen.insert(s.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global::wire::RegionStatePush;
    use crate::models::{CheckId, NodeState};

    fn id(name: &str) -> CheckId {
        CheckId::new_v5(&CheckId::NAMESPACE_OID, name.as_bytes())
    }

    fn push(
        region: &str,
        origin: &str,
        at: i64,
        health: NodeState,
        checks: &[(&str, NodeState)],
    ) -> RegionStatePush {
        RegionStatePush {
            region: region.into(),
            origin: origin.into(),
            advertise_url: Some(format!("http://{origin}:8080")),
            pushed_at: at,
            region_health: health,
            agents_healthy: 1,
            agents_total: 1,
            checks: checks
                .iter()
                .map(|(name, st)| CheckSummary {
                    check_id: id(name),
                    name: (*name).to_string(),
                    state: *st,
                    latency_ms: 10,
                    last_check: at,
                    uptime_24h: Some(100.0),
                    status_code: Some(200),
                    error: None,
                    kind: None,
                    group: None,
                    latency_graph: false,
                    position: 0,
                    depends_on: Vec::new(),
                })
                .collect(),
            epoch: 1,
            seq: at as u64,
            delta_base: None,
            removed: Vec::new(),
        }
    }

    #[test]
    fn deltas_apply_only_on_top_of_the_push_they_extend() {
        let cache = GlobalCache::new(20, &[]);
        let mut delta = push("us", "a", 6, NodeState::Down, &[("api", NodeState::Down)]);
        delta.delta_base = Some(5);
        delta.removed = vec![id("gone")];
        assert_eq!(cache.apply_push(delta.clone(), 1_000), PushOutcome::Resync);

        let snapshot = push(
            "us",
            "a",
            5,
            NodeState::Up,
            &[
                ("api", NodeState::Up),
                ("gone", NodeState::Up),
                ("kept", NodeState::Up),
            ],
        );
        assert_eq!(cache.apply_push(snapshot, 1_000), PushOutcome::Applied);
        assert_eq!(cache.apply_push(delta.clone(), 1_001), PushOutcome::Applied);
        let mut states: Vec<(String, NodeState)> = cache
            .aggregate(1_001)
            .into_iter()
            .map(|c| (c.name, c.state))
            .collect();
        states.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            states,
            vec![
                ("api".into(), NodeState::Down),
                ("kept".into(), NodeState::Up)
            ]
        );

        // Replaying the same delta, one from another origin, or one from a
        // restarted sender (new epoch) cannot extend the chain.
        assert_eq!(cache.apply_push(delta.clone(), 1_002), PushOutcome::Resync);
        let mut other = delta.clone();
        other.origin = "b".into();
        other.delta_base = Some(6);
        assert_eq!(cache.apply_push(other, 1_002), PushOutcome::Resync);
        let mut restarted = delta;
        restarted.epoch = 2;
        restarted.delta_base = Some(6);
        assert_eq!(cache.apply_push(restarted, 1_002), PushOutcome::Resync);
    }

    #[test]
    fn staleness_snapshot_counts_seeded_received_and_expired_regions() {
        use crate::config::GlobalRegion;
        let cache = GlobalCache::new(
            20,
            &[GlobalRegion {
                name: "seeded".into(),
                endpoints: vec![],
            }],
        );
        assert_eq!(cache.staleness_snapshot(1_000), (0, 1, 0));
        cache.apply_push(push("fresh", "a", 1, NodeState::Up, &[]), 1_000);
        cache.apply_push(push("expired", "b", 1, NodeState::Up, &[]), 1);
        assert_eq!(cache.staleness_snapshot(1_000), (0, 1, 1));
        assert_eq!(cache.staleness_snapshot(21_001), (2, 1, 1));
    }

    #[test]
    fn seeded_endpoints_survive_a_push_with_no_advertise_url() {
        use crate::config::GlobalRegion;
        let seed = vec![GlobalRegion {
            name: "tw".to_string(),
            endpoints: vec!["http://coordinator-tw:8080".to_string()],
        }];
        let cache = GlobalCache::new(20, &seed);
        let mut p = push("tw", "coordinator-tw", 1000, NodeState::Up, &[]);
        p.advertise_url = None; // a single-node region advertises nothing
        cache.apply_push(p, 1000);
        let regions = cache.region_endpoints(2000);
        let tw = regions.iter().find(|(n, _)| n == "tw");
        assert!(tw.is_some(), "tw dropped from region_endpoints after push");
        assert_eq!(
            tw.unwrap().1,
            vec!["http://coordinator-tw:8080".to_string()],
            "seeded endpoint lost when push carried no advertise_url"
        );
    }

    #[test]
    fn last_writer_wins_by_pushed_at() {
        let cache = GlobalCache::new(20, &[]);
        cache.apply_push(
            push("us", "a", 100, NodeState::Up, &[("x", NodeState::Up)]),
            1000,
        );
        // An older push from a peer must not overwrite the newer state.
        cache.apply_push(
            push("us", "b", 50, NodeState::Down, &[("x", NodeState::Down)]),
            1001,
        );
        let agg = cache.aggregate(1002);
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].state, NodeState::Up);
    }

    #[test]
    fn cross_region_rollup_takes_the_worst_live_region() {
        let cache = GlobalCache::new(20, &[]);
        cache.apply_push(
            push("us", "a", 1, NodeState::Up, &[("api", NodeState::Up)]),
            1000,
        );
        cache.apply_push(
            push("tw", "t", 1, NodeState::Down, &[("api", NodeState::Down)]),
            1000,
        );
        let agg = cache.aggregate(1000);
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].state, NodeState::Down);
        assert_eq!(agg[0].per_region.len(), 2);
    }

    #[test]
    fn a_stale_region_drops_out_of_aggregation() {
        let cache = GlobalCache::new(20, &[]); // 20_000 ms window
        cache.apply_push(
            push("us", "a", 1, NodeState::Up, &[("api", NodeState::Up)]),
            1_000,
        );
        // 30s later, nothing new: the region is dark and contributes nothing.
        let agg = cache.aggregate(31_000);
        assert!(agg.is_empty());
        let roster = cache.roster(31_000);
        assert_eq!(roster.len(), 1);
        assert!(roster[0].stale);
        assert_eq!(roster[0].health, NodeState::Unknown);
    }

    #[test]
    fn failure_evidence_comes_from_the_freshest_worst_contributor() {
        let cache = GlobalCache::new(20, &[]);
        let mut failing = push("us", "a", 10, NodeState::Down, &[("api", NodeState::Down)]);
        failing.checks[0].error = Some("connection refused".into());
        failing.checks[0].status_code = Some(503);
        cache.apply_push(failing, 1_000);
        cache.apply_push(
            push("tw", "b", 20, NodeState::Up, &[("api", NodeState::Up)]),
            1_000,
        );

        let check = cache.aggregate(1_000).pop().unwrap();
        assert_eq!(check.state, NodeState::Down);
        assert_eq!(check.last_check, 10);
        assert_eq!(check.error.as_deref(), Some("connection refused"));
        assert_eq!(check.status_code, Some(503));
    }

    #[test]
    fn owner_endpoints_prefers_the_reporting_region() {
        let cache = GlobalCache::new(20, &[]);
        cache.apply_push(
            push("us", "a", 1, NodeState::Up, &[("api", NodeState::Up)]),
            1000,
        );
        cache.apply_push(
            push("tw", "t", 1, NodeState::Up, &[("dns", NodeState::Up)]),
            1000,
        );
        let eps = cache.owner_endpoints(id("dns"), 1000);
        assert_eq!(eps, vec!["http://t:8080".to_string()]);
    }
}
