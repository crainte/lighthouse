//! Deciding which agents probe which checks.
//!
//! At two agents it hardly matters: an unassigned check runs on both and the
//! duplication is invisible. At twenty it is the dominant cost — 100 checks on
//! a 30s cadence become 67 probes/s and 5.8M rows a day, nearly all of them the
//! same observation repeated. The fix is not to pin agent ids onto checks (that
//! makes the fleet un-editable: every new agent means revisiting every check)
//! but to describe *where* a check should run and let the coordinator pick.
//!
//! Assignment is computed, never stored. A stored assignment table is a second
//! source of truth that drifts from the fleet the moment an agent dies, and
//! something has to notice and repair it. Deriving the answer from the live
//! agent list means there is nothing to repair.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// An agent as the placement logic sees it: an identity and a location.
///
/// Capability is carried but not consulted here. `assign` answers one question
/// — given these agents, which of them take this check — and the caller decides
/// which agents were eligible to be offered it in the first place. Folding both
/// into `assign` would make its distribution tests depend on kind eligibility
/// they are not about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent {
    pub agent_id: String,
    pub zone: String,
    pub region: String,
    /// Check kinds this agent can probe, or `None` when it has not said —
    /// an agent predating capability negotiation, which runs anything.
    pub kinds: Option<Vec<crate::models::CheckKind>>,
}

/// Where a check should run.
///
/// The default — no zones, no regions, one agent per zone — means "probe this
/// from everywhere, once per zone". That is the behaviour a fleet actually
/// wants, and it is what turns the old 20x duplication into one probe per
/// failure domain without anyone configuring anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Placement {
    /// Zones allowed to run this. Empty means every zone.
    pub zones: Vec<String>,
    /// Regions allowed to run this. Empty means every region.
    pub regions: Vec<String>,
    /// The failure-domain at which `count` is applied. Older documents omit
    /// this and retain their per-zone behaviour.
    pub mode: PlacementMode,
    /// How many agents in each matched `mode` domain probe this check.
    /// `None` keeps the legacy `per_zone` value, so stored check documents
    /// written before mode/count existed keep exactly their fan-out.
    pub count: Option<u32>,
    /// Legacy spelling for the per-zone count. Kept on the wire so an older
    /// admin client can round-trip a check; new clients send `mode`/`count`.
    pub per_zone: u32,
}

/// The domain in which placement chooses independent observers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PlacementMode {
    #[default]
    Zone,
    Region,
}

impl Default for Placement {
    fn default() -> Self {
        Self {
            zones: Vec::new(),
            regions: Vec::new(),
            mode: PlacementMode::Zone,
            count: None,
            per_zone: 1,
        }
    }
}

impl Placement {
    /// The requested fan-out, retaining documents written with `per_zone`.
    fn count(&self) -> u32 {
        self.count.unwrap_or(self.per_zone).max(1)
    }

    /// Whether an agent's location is eligible at all, before fan-out.
    fn admits(&self, agent: &Agent) -> bool {
        let zone_ok = self.zones.is_empty() || self.zones.contains(&agent.zone);
        zone_ok && self.admits_region(&agent.region)
    }

    /// The region half of `admits`, standing on its own because deciding
    /// where a check's *definition* should be distributed
    /// (`global::reconcile`) only ever has a region to ask about — an agent,
    /// let alone its zone, does not exist yet at that point. Kept here rather
    /// than duplicated so the two questions ("does this region get the
    /// document" and "does this agent get to run it") can never disagree
    /// about what "every region" means.
    pub fn admits_region(&self, region: &str) -> bool {
        self.regions.is_empty() || self.regions.iter().any(|r| r == region)
    }
}

/// Score an (agent, check) pair. Rendezvous hashing: each agent scores itself
/// for the check, highest wins.
///
/// The property that matters is stability under fleet change. Losing an agent
/// only reassigns the checks *it* held — every other pairing keeps its score
/// and its winner — so one agent dying does not reshuffle the fleet and set off
/// a stampede of restarts across every probe loop.
fn score(check: crate::models::CheckId, agent_id: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    check.hash(&mut h);
    // Separator: without it ("ab","c") and ("a","bc") hash alike, and two
    // checks could collapse onto the same agent for no reason.
    0xffu8.hash(&mut h);
    agent_id.hash(&mut h);
    h.finish()
}

/// Which of `live` should probe `check`.
///
/// `pinned` is the explicit escape hatch: when a check names agents outright,
/// that wins and placement is ignored. Some checks genuinely belong to one box
/// — a probe of a machine's own loopback port cannot be run from anywhere else.
///
/// Returns agent ids, sorted, so callers can compare results without caring
/// about iteration order.
///
/// `check` is the check's immutable id, never its name: hashing on the name
/// would reshuffle every assignment the moment an operator renamed a check,
/// restarting probe loops fleet-wide for a purely cosmetic edit.
pub fn assign(
    check: crate::models::CheckId,
    placement: &Placement,
    pinned: &[String],
    live: &[Agent],
) -> Vec<String> {
    if !pinned.is_empty() {
        // Pinning names agents that may not currently be live. That is
        // deliberate: the operator asked for those and nobody else, so a silent
        // agent means the check does not run rather than quietly migrating to a
        // machine that cannot answer for it. The agents panel reports this.
        let mut out: Vec<String> = pinned.to_vec();
        out.sort();
        out.dedup();
        return out;
    }

    // Group by the selected failure domain. Per-zone is the historic default;
    // per-region deliberately lets a region choose a small representative set
    // across all of its zones.
    let mut by_domain: BTreeMap<&str, Vec<&Agent>> = BTreeMap::new();
    for a in live.iter().filter(|a| placement.admits(a)) {
        let domain = match placement.mode {
            PlacementMode::Zone => a.zone.as_str(),
            PlacementMode::Region => a.region.as_str(),
        };
        by_domain.entry(domain).or_default().push(a);
    }

    let mut chosen: Vec<String> = Vec::new();
    for (_, mut agents) in by_domain {
        // Highest score first; agent_id breaks ties so the result is total and
        // reproducible across coordinator replicas.
        agents.sort_by(|x, y| {
            score(check, &y.agent_id)
                .cmp(&score(check, &x.agent_id))
                .then_with(|| x.agent_id.cmp(&y.agent_id))
        });
        chosen.extend(
            agents
                .into_iter()
                .take(placement.count() as usize)
                .map(|a| a.agent_id.clone()),
        );
    }
    chosen.sort();
    chosen
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic check id for a human-readable test name, so test bodies
    /// stay readable while placement itself only ever sees ids.
    fn cid(name: &str) -> crate::models::CheckId {
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, name.as_bytes())
    }

    fn fleet(spec: &[(&str, &str, &str)]) -> Vec<Agent> {
        spec.iter()
            .map(|(id, zone, region)| Agent {
                agent_id: (*id).into(),
                zone: (*zone).into(),
                region: (*region).into(),
                // Distribution is what these tests are about; eligibility is
                // decided before `assign` is ever called.
                kinds: None,
            })
            .collect()
    }

    /// Three regions, two zones each, three agents per zone: the shape the
    /// 20-agent target actually takes.
    fn big_fleet() -> Vec<Agent> {
        let mut v = Vec::new();
        for region in ["us-east", "eu-west", "ap-south"] {
            for zone in ["a", "b"] {
                for n in 0..3 {
                    v.push(Agent {
                        agent_id: format!("{region}-{zone}-{n}"),
                        zone: format!("{region}-{zone}"),
                        region: region.into(),
                        kinds: None,
                    });
                }
            }
        }
        v
    }

    #[test]
    fn the_default_is_one_probe_per_zone_not_one_per_agent() {
        // The whole point. Eighteen agents across six zones must produce six
        // probes, not eighteen: that ratio is the difference between 5.8M and
        // 0.9M rows a day.
        let live = big_fleet();
        let got = assign(cid("api"), &Placement::default(), &[], &live);
        assert_eq!(got.len(), 6, "one per zone, got {got:?}");

        let zones: std::collections::BTreeSet<&str> = got
            .iter()
            .map(|id| {
                live.iter()
                    .find(|a| a.agent_id == *id)
                    .unwrap()
                    .zone
                    .as_str()
            })
            .collect();
        assert_eq!(zones.len(), 6, "every zone covered exactly once");
    }

    #[test]
    fn losing_an_agent_moves_only_its_own_checks() {
        // Why rendezvous hashing rather than "sort and take the first N": a
        // departing agent must not renumber everyone else. Each reassignment
        // restarts a probe loop, so a reshuffle during an incident — exactly
        // when agents go quiet — would be a self-inflicted second outage.
        let live = big_fleet();
        let checks: Vec<crate::models::CheckId> =
            (0..200).map(|i| cid(&format!("check-{i}"))).collect();

        let before: Vec<Vec<String>> = checks
            .iter()
            .map(|c| assign(*c, &Placement::default(), &[], &live))
            .collect();

        let casualty = &live[0].agent_id.clone();
        let smaller: Vec<Agent> = live
            .iter()
            .filter(|a| a.agent_id != *casualty)
            .cloned()
            .collect();
        let after: Vec<Vec<String>> = checks
            .iter()
            .map(|c| assign(*c, &Placement::default(), &[], &smaller))
            .collect();

        let mut moved = 0;
        for (b, a) in before.iter().zip(&after) {
            if b != a {
                moved += 1;
                assert!(
                    b.contains(casualty),
                    "only checks held by the lost agent may move: {b:?} -> {a:?}"
                );
            }
        }
        assert!(moved > 0, "the lost agent's checks must go somewhere");
    }

    #[test]
    fn a_silent_agents_checks_are_taken_over() {
        // Failover with no registry: the caller passes only live agents, so a
        // departed one simply is not in the input and its work is re-derived.
        let live = fleet(&[("a1", "z1", "r1"), ("a2", "z1", "r1")]);
        let held = assign(cid("api"), &Placement::default(), &[], &live);
        assert_eq!(held.len(), 1);

        let survivor: Vec<Agent> = live
            .iter()
            .filter(|a| a.agent_id != held[0])
            .cloned()
            .collect();
        let after = assign(cid("api"), &Placement::default(), &[], &survivor);
        assert_eq!(after.len(), 1, "the zone is still covered");
        assert_ne!(after, held, "by the other agent");
    }

    #[test]
    fn placement_restricts_to_the_named_zones_and_regions() {
        let live = big_fleet();

        let eu = Placement {
            regions: vec!["eu-west".into()],
            ..Default::default()
        };
        let got = assign(cid("api"), &eu, &[], &live);
        assert_eq!(got.len(), 2, "eu-west has two zones: {got:?}");
        assert!(got.iter().all(|id| id.starts_with("eu-west")));

        let one_zone = Placement {
            zones: vec!["ap-south-a".into()],
            ..Default::default()
        };
        let got = assign(cid("api"), &one_zone, &[], &live);
        assert_eq!(got.len(), 1, "{got:?}");
        assert!(got[0].starts_with("ap-south-a"));
    }

    #[test]
    fn legacy_per_zone_documents_keep_their_fanout() {
        let live = big_fleet();
        let legacy: Placement =
            serde_json::from_str(r#"{"regions":["eu-west"],"zones":[],"per_zone":2}"#).unwrap();
        assert_eq!(legacy.mode, PlacementMode::Zone);
        assert_eq!(assign(cid("api"), &legacy, &[], &live).len(), 4);
    }

    #[test]
    fn per_region_count_chooses_across_the_whole_region() {
        let live = big_fleet();
        let placement = Placement {
            regions: vec!["eu-west".into()],
            mode: PlacementMode::Region,
            count: Some(2),
            ..Default::default()
        };
        let got = assign(cid("api"), &placement, &[], &live);
        assert_eq!(got.len(), 2, "two observers total in eu-west: {got:?}");
        assert!(got.iter().all(|id| id.starts_with("eu-west")));
    }

    #[test]
    fn per_zone_buys_corroboration() {
        let live = big_fleet();
        let two = Placement {
            zones: vec!["us-east-a".into()],
            per_zone: 2,
            ..Default::default()
        };
        let got = assign(cid("api"), &two, &[], &live);
        assert_eq!(got.len(), 2, "two independent observers in one zone");

        // Asking for more agents than a zone holds gets the whole zone, not an
        // error: coverage degrades quietly rather than the check not running.
        let greedy = Placement {
            zones: vec!["us-east-a".into()],
            per_zone: 99,
            ..Default::default()
        };
        assert_eq!(assign(cid("api"), &greedy, &[], &live).len(), 3);
    }

    #[test]
    fn pinning_overrides_placement_entirely() {
        // A check on a box's own loopback cannot run anywhere else, so an
        // explicit list must not be second-guessed by zone matching.
        let live = big_fleet();
        let got = assign(
            cid("loopback"),
            &Placement {
                regions: vec!["eu-west".into()],
                ..Default::default()
            },
            &["us-east-a-0".into()],
            &live,
        );
        assert_eq!(got, vec!["us-east-a-0".to_string()]);
    }

    #[test]
    fn an_unmatched_placement_selects_nobody() {
        // Silence is the honest answer: a check scoped to a region with no live
        // agents is not running, and inventing a substitute elsewhere would
        // report on something the operator never asked to measure.
        let live = big_fleet();
        let nowhere = Placement {
            regions: vec!["sa-east".into()],
            ..Default::default()
        };
        assert!(assign(cid("api"), &nowhere, &[], &live).is_empty());
    }

    #[test]
    fn assignment_is_stable_and_replica_independent() {
        // Two coordinators must agree without talking to each other, and
        // repeated calls must not flap: any instability here restarts probe
        // loops forever.
        let live = big_fleet();
        let shuffled: Vec<Agent> = live.iter().rev().cloned().collect();
        for name in ["api", "db", "cdn"] {
            let c = cid(name);
            let a = assign(c, &Placement::default(), &[], &live);
            let b = assign(c, &Placement::default(), &[], &shuffled);
            assert_eq!(a, b, "input order must not change the outcome for {name}");
        }
    }

    #[test]
    fn work_spreads_across_agents_within_a_zone() {
        // One agent per zone is chosen per check, but across many checks the
        // choice must spread — otherwise one machine in each zone does all the
        // probing while its siblings idle.
        let live = big_fleet();
        let mut load: BTreeMap<String, usize> = BTreeMap::new();
        for i in 0..600 {
            for id in assign(
                cid(&format!("check-{i}")),
                &Placement::default(),
                &[],
                &live,
            ) {
                *load.entry(id).or_default() += 1;
            }
        }
        let counts: Vec<usize> = load.values().copied().collect();
        let (min, max) = (*counts.iter().min().unwrap(), *counts.iter().max().unwrap());
        assert_eq!(load.len(), 18, "every agent gets work");
        assert!(
            max < min * 2,
            "load should be roughly even, got min={min} max={max}"
        );
    }

    #[test]
    fn admits_region_treats_an_empty_list_as_every_region() {
        let p = Placement::default();
        assert!(p.admits_region("us"));
        assert!(p.admits_region("tw"));
    }

    #[test]
    fn admits_region_narrows_to_the_named_regions() {
        let p = Placement {
            regions: vec!["us".into()],
            ..Placement::default()
        };
        assert!(p.admits_region("us"));
        assert!(!p.admits_region("tw"));
    }
}
