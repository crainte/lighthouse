//! Folding many opinions into one state.
//!
//! This is the consumer that `placement.rs` promised and never got. Its
//! docstring (`placement.rs:49-51`) describes `per_zone` fan-out as
//! *"corroboration — a second opinion before calling a service down"*, but
//! until now every opinion landed in a single shared counter, so a second agent
//! made flapping *more* likely rather than less. `rollup` is where the second
//! opinion is actually consulted.
//!
//! **Level-agnostic on purpose.** [`rollup`] takes a slice of child states and
//! knows nothing about where they came from: per-agent states at the zone
//! level, per-zone states at the region level, per-region states at the check
//! level — and, once regional coordinators push summaries upward, per-region
//! states observed by an entirely different node. That signature is the seam
//! the global coordinator attaches to without this file changing.

use serde::{Deserialize, Serialize};

use crate::models::NodeState;

/// How many failing children it takes to satisfy a rule.
///
/// Counts, not percentages: `Majority` of two is two, not one, and the
/// arithmetic stays integral so there is no rounding to argue about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Threshold {
    /// One is enough.
    Any,
    /// Strictly more than half.
    Majority,
    /// Every child, unanimously.
    All,
    /// An explicit count.
    Quorum(u32),
}

impl Threshold {
    pub fn as_str(&self) -> &'static str {
        match self {
            Threshold::Any => "any",
            Threshold::Majority => "majority",
            Threshold::All => "all",
            Threshold::Quorum(_) => "quorum",
        }
    }

    /// Parse the stored string form. The quorum count lives in the policy's
    /// `config` JSON rather than in this string, so a bare `"quorum"` needs a
    /// count supplied alongside it.
    pub fn from_db(s: &str, quorum: Option<u32>) -> Threshold {
        match s {
            "any" => Threshold::Any,
            "all" => Threshold::All,
            "quorum" => Threshold::Quorum(quorum.unwrap_or(1)),
            _ => Threshold::Majority,
        }
    }

    /// Is `k` of `n` enough?
    ///
    /// `n == 0` is never satisfied by anything — with no opinions there is
    /// nothing to be unanimous *about*, and an `All` that returned true on an
    /// empty set would call every unobserved check down. [`rollup`] returns
    /// `Unknown` before reaching here, but the guard stays because this is a
    /// public predicate.
    pub fn satisfied(&self, k: usize, n: usize) -> bool {
        if n == 0 {
            return false;
        }
        match self {
            Threshold::Any => k >= 1,
            Threshold::Majority => k * 2 > n,
            Threshold::All => k == n,
            // Clamped into `1..=n`: a quorum of 3 configured against a fleet
            // that has since shrunk to two agents degrades to "both" rather
            // than becoming permanently unsatisfiable, which would silently
            // disable alerting on that node.
            Threshold::Quorum(q) => {
                let q = (*q).clamp(1, n as u32) as usize;
                k >= q
            }
        }
    }

    /// Ordering by how hard each is to satisfy at a given fleet size, used to
    /// validate that a node cannot be Down without also being Degraded.
    fn strictness(&self, n: usize) -> usize {
        match self {
            Threshold::Any => 1,
            Threshold::Majority => n / 2 + 1,
            Threshold::All => n,
            Threshold::Quorum(q) => (*q).clamp(1, n as u32) as usize,
        }
    }
}

/// Two thresholds, because one cannot express what this tree needs.
///
/// The design doc's `All` (`uptime-monitor-design.md:571-580`) is *pessimistic*:
/// "Up only if all children Up; Down if any child Down" — one failure
/// propagates upward. Corroboration needs the exact opposite: *all* agents must
/// agree before a zone is down. A single-threshold enum has to pick one of
/// those readings, and either choice is wrong at one of the levels. So the
/// policy carries both edges explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollupPolicy {
    /// How many failing children (degraded *or* down) make this node Degraded.
    pub degraded_when: Threshold,
    /// How many hard-Down children make this node Down.
    pub down_when: Threshold,
}

impl RollupPolicy {
    pub const fn new(degraded_when: Threshold, down_when: Threshold) -> Self {
        Self {
            degraded_when,
            down_when,
        }
    }

    /// Agents → zone. Redundant agents probe the same target, so disagreement
    /// between them means a broken *agent*, not a broken service. A zone is
    /// therefore never Degraded — only Up or Down, and Down only unanimously.
    /// This is the corroboration property, and the whole point of Stage A.
    pub const ZONE_DEFAULT: RollupPolicy = RollupPolicy::new(Threshold::All, Threshold::All);

    /// Zones → region. A zone is already a corroborated unit, so one zone
    /// losing reach is real signal worth surfacing; the region is only Down
    /// when every zone in it agrees.
    pub const REGION_DEFAULT: RollupPolicy = RollupPolicy::new(Threshold::Any, Threshold::All);

    /// Regions → check. One region down is a partial outage; most regions down
    /// is an outage.
    pub const CHECK_DEFAULT: RollupPolicy = RollupPolicy::new(Threshold::Any, Threshold::Majority);

    /// `down_when` must be no easier to satisfy than `degraded_when`, or a node
    /// could be Down without ever having been Degraded — the state machine
    /// would skip an edge the alerting code assumes exists.
    ///
    /// Checked across a range of plausible fleet sizes rather than one, because
    /// `Majority` and `Quorum` change places as `n` moves and a policy that is
    /// coherent for three agents and incoherent for two is still a bug.
    pub fn validate(&self) -> Result<(), String> {
        if let Threshold::Quorum(0) = self.degraded_when {
            return Err("degraded_when quorum must be at least 1".into());
        }
        if let Threshold::Quorum(0) = self.down_when {
            return Err("down_when quorum must be at least 1".into());
        }
        for n in 1..=9usize {
            if self.down_when.strictness(n) < self.degraded_when.strictness(n) {
                return Err(format!(
                    "down_when ({}) is easier to satisfy than degraded_when ({}) \
                     with {n} children: a node would go Down without being Degraded",
                    self.down_when.as_str(),
                    self.degraded_when.as_str()
                ));
            }
        }
        Ok(())
    }
}

/// Fold child states into this node's state.
///
/// `children` are per-agent states at the zone level, per-zone states at the
/// region level, and per-region states at the check level.
pub fn rollup(policy: &RollupPolicy, children: &[NodeState]) -> NodeState {
    rollup_counts(policy, children.iter().map(|state| (*state, 1)))
}

/// `rollup` over `(state, how many children are in it)` pairs, so a caller
/// holding aggregated counts never expands them into one entry per child.
pub fn rollup_counts(
    policy: &RollupPolicy,
    children: impl IntoIterator<Item = (NodeState, usize)>,
) -> NodeState {
    // Unknown children are excluded from the denominator entirely, not counted
    // as healthy and not counted as failing. This is the single most important
    // property in this file: a silent agent, a decommissioned one, or an
    // unreachable region must never manufacture a phantom outage — nor mask a
    // real one by inflating the healthy count.
    let (mut n, mut hard_down, mut failing) = (0, 0, 0);
    for (state, count) in children {
        if state == NodeState::Unknown {
            continue;
        }
        n += count;
        if state == NodeState::Down {
            hard_down += count;
        }
        if state.is_failing() {
            failing += count;
        }
    }
    if n == 0 {
        // Nobody has an opinion. The check reads as its `default_status`,
        // exactly as it does today before the first result arrives.
        return NodeState::Unknown;
    }

    if policy.down_when.satisfied(hard_down, n) {
        NodeState::Down
    } else if policy.degraded_when.satisfied(failing, n) {
        NodeState::Degraded
    } else {
        NodeState::Up
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use NodeState::{Degraded, Down, Unknown, Up};

    #[test]
    fn thresholds_count_correctly() {
        // (threshold, k, n, satisfied)
        let cases: &[(Threshold, usize, usize, bool)] = &[
            (Threshold::Any, 0, 3, false),
            (Threshold::Any, 1, 3, true),
            (Threshold::Majority, 1, 2, false),
            (Threshold::Majority, 2, 2, true),
            (Threshold::Majority, 2, 3, true),
            (Threshold::Majority, 1, 3, false),
            (Threshold::All, 2, 3, false),
            (Threshold::All, 3, 3, true),
            (Threshold::Quorum(2), 1, 3, false),
            (Threshold::Quorum(2), 2, 3, true),
            // Clamped down: a quorum larger than the fleet becomes "all".
            (Threshold::Quorum(9), 2, 2, true),
            (Threshold::Quorum(9), 1, 2, false),
            // Nothing is satisfiable with no children.
            (Threshold::Any, 0, 0, false),
            (Threshold::All, 0, 0, false),
        ];
        for (t, k, n, want) in cases {
            assert_eq!(t.satisfied(*k, *n), *want, "{t:?} with {k} of {n}");
        }
    }

    #[test]
    fn no_opinions_is_unknown_not_down() {
        assert_eq!(rollup(&RollupPolicy::ZONE_DEFAULT, &[]), Unknown);
        // ...and a fleet that is entirely silent is the same case: every child
        // filtered out leaves nothing to judge.
        assert_eq!(
            rollup(&RollupPolicy::ZONE_DEFAULT, &[Unknown, Unknown]),
            Unknown
        );
    }

    #[test]
    fn silent_children_leave_the_denominator() {
        // Two agents, one silent, one down. Under `All` the surviving opinion is
        // unanimous among those who have one, so the zone is Down — the silent
        // agent neither vetoes the outage nor votes it into existence.
        assert_eq!(
            rollup(&RollupPolicy::ZONE_DEFAULT, &[Down, Unknown]),
            Down,
            "a silent agent must not veto a real outage"
        );
        // And the mirror: one up, one silent, is simply up.
        assert_eq!(rollup(&RollupPolicy::ZONE_DEFAULT, &[Up, Unknown]), Up);
    }

    #[test]
    fn a_single_flaky_agent_never_takes_its_zone_down() {
        // The `ntfy-remote` bug, at the level where it is fixed. Two agents
        // probing the same target; one fails. `All` is unsatisfied, so the zone
        // stays Up and nothing propagates upward — no transition, no alert row,
        // no incident.
        assert_eq!(
            rollup(&RollupPolicy::ZONE_DEFAULT, &[Down, Up]),
            Up,
            "one dissenting agent is a broken agent, not a broken service"
        );
        assert_eq!(rollup(&RollupPolicy::ZONE_DEFAULT, &[Up, Down]), Up);
        // Zones are never Degraded: there is no middle reading for "my two
        // agents disagree", because that is not a fact about the service.
        assert_ne!(rollup(&RollupPolicy::ZONE_DEFAULT, &[Down, Up]), Degraded);
    }

    #[test]
    fn a_whole_zone_agreeing_is_down() {
        assert_eq!(rollup(&RollupPolicy::ZONE_DEFAULT, &[Down, Down]), Down);
        assert_eq!(
            rollup(&RollupPolicy::ZONE_DEFAULT, &[Down, Down, Down]),
            Down
        );
    }

    #[test]
    fn one_zone_down_degrades_its_region_but_does_not_down_it() {
        // Zones → region: `Any` degrades, `All` downs.
        assert_eq!(
            rollup(&RollupPolicy::REGION_DEFAULT, &[Down, Up]),
            Degraded,
            "a zone losing reach is real signal, but the region is still serving"
        );
        assert_eq!(rollup(&RollupPolicy::REGION_DEFAULT, &[Down, Down]), Down);
        assert_eq!(rollup(&RollupPolicy::REGION_DEFAULT, &[Up, Up]), Up);
    }

    #[test]
    fn the_reported_scenario_reads_as_a_partial_outage() {
        // us-east down, eu up. Under the old flat majority this published UP
        // and the outage vanished; that is the third defect in A1.
        assert_eq!(
            rollup(&RollupPolicy::CHECK_DEFAULT, &[Down, Up]),
            Degraded,
            "a regional outage must not be rounded away to healthy"
        );
        // Three regions, two down: majority satisfied, the check is Down.
        assert_eq!(
            rollup(&RollupPolicy::CHECK_DEFAULT, &[Down, Down, Up]),
            Down
        );
        // Three regions, one down: degraded.
        assert_eq!(
            rollup(&RollupPolicy::CHECK_DEFAULT, &[Down, Up, Up]),
            Degraded
        );
    }

    #[test]
    fn degraded_children_count_toward_degraded_but_never_toward_down() {
        // A region that is itself only partially failing must not be counted as
        // a hard Down vote by the level above it, or partial outages would
        // compound into a full one.
        assert_eq!(
            rollup(&RollupPolicy::CHECK_DEFAULT, &[Degraded, Degraded]),
            Degraded,
            "two partial outages are still a partial outage"
        );
        assert_eq!(
            rollup(
                &RollupPolicy::new(Threshold::Any, Threshold::Any),
                &[Degraded]
            ),
            Degraded,
            "even `down_when: Any` needs a hard Down to fire"
        );
    }

    #[test]
    fn a_single_child_is_a_pass_through_at_every_level() {
        // The standalone deployment: one region, one zone, one agent. `All` and
        // `Any` over a single child both *are* that child, so the tree changes
        // nothing for a single-instance install — which is why hold-down, not
        // corroboration, is the fix that reaches those users.
        for policy in [
            RollupPolicy::ZONE_DEFAULT,
            RollupPolicy::REGION_DEFAULT,
            RollupPolicy::CHECK_DEFAULT,
        ] {
            assert_eq!(rollup(&policy, &[Up]), Up, "{policy:?}");
            assert_eq!(rollup(&policy, &[Down]), Down, "{policy:?}");
            assert_eq!(rollup(&policy, &[Unknown]), Unknown, "{policy:?}");
        }
    }

    #[test]
    fn the_default_policies_are_internally_coherent() {
        for policy in [
            RollupPolicy::ZONE_DEFAULT,
            RollupPolicy::REGION_DEFAULT,
            RollupPolicy::CHECK_DEFAULT,
        ] {
            assert!(policy.validate().is_ok(), "{policy:?}");
        }
    }

    #[test]
    fn validation_rejects_a_policy_that_could_skip_degraded() {
        // Down on any one child, Degraded only when all of them fail: a
        // two-child node would jump Up → Down without passing through Degraded.
        let bad = RollupPolicy::new(Threshold::All, Threshold::Any);
        assert!(bad.validate().is_err());

        let bad = RollupPolicy::new(Threshold::Majority, Threshold::Any);
        assert!(bad.validate().is_err());

        // A zero quorum is unsatisfiable-by-nonsense rather than by clamping:
        // it would mean "no failing children make this node down", which reads
        // as a typo for `Any` and should be rejected rather than guessed at.
        assert!(RollupPolicy::new(Threshold::Quorum(0), Threshold::All)
            .validate()
            .is_err());
        assert!(RollupPolicy::new(Threshold::Any, Threshold::Quorum(0))
            .validate()
            .is_err());

        // Equal is fine — that is the zone default.
        assert!(RollupPolicy::new(Threshold::All, Threshold::All)
            .validate()
            .is_ok());
    }

    #[test]
    fn thresholds_round_trip_through_their_stored_form() {
        for t in [
            Threshold::Any,
            Threshold::Majority,
            Threshold::All,
            Threshold::Quorum(3),
        ] {
            let quorum = match t {
                Threshold::Quorum(q) => Some(q),
                _ => None,
            };
            assert_eq!(Threshold::from_db(t.as_str(), quorum), t, "{t:?}");
        }
        // Unrecognised falls back to the least surprising middle ground rather
        // than to `Any`, which would make an unknown policy trigger-happy.
        assert_eq!(Threshold::from_db("nonsense", None), Threshold::Majority);
    }
}
