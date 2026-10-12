use crate::models::{CheckId, NodeState, Status};
use std::collections::HashMap;

/// A state transition worth acting on (alerting).
///
/// `from`/`to` are [`NodeState`], not [`Status`]: a transition describes the
/// *published* state of a check, which can be Degraded, and never describes a
/// single probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub check_id: CheckId,
    pub from: NodeState,
    pub to: NodeState,
}

/// One agent's confirmed opinion about one check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Belief {
    pub status: Status,
    pub consecutive_failures: u32,
    /// Auth-stamped topology for this leaf. Kept with the belief so a restart
    /// reconstructs the same region/zone tree rather than inferring it later.
    pub zone: String,
    pub region: String,
    /// When this agent last reported on this check, in epoch millis. Staleness
    /// is judged by the caller, which knows each agent's declared cadence.
    pub last_check: i64,
}

/// What the coordinator currently tells the world about a check, plus the
/// hold-down timer working toward the next answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Published {
    pub state: NodeState,
    /// The state the rollup is currently arguing for, and when it first said
    /// so. `None` when the rollup agrees with what is already published.
    pub pending: Option<(NodeState, i64)>,
}

/// Tracks per-agent confirmation, then the published state of each check.
///
/// **Keyed by `(check_id, agent_id)`, and that is the whole point.** Before
/// this, every agent's result mutated one shared counter per check, in the
/// order batches happened to arrive rather than in timestamp order. With
/// `failures_before_down = 1` any single agent's single failed probe flipped
/// the check DOWN globally; with a higher threshold the "consecutive" failures
/// could come from different agents while one healthy agent's UP reset the run,
/// so a genuinely dead service could fail to confirm at all. Neither is a
/// threshold doing what its name says.
///
/// Now `failures_before_down` means what it reads as: N consecutive failures
/// *as seen by one agent*. Combining those opinions is [`crate::rollup`]'s job,
/// and the tracker only holds the leaves and the published answer.
///
/// Pure logic, no I/O — the unit worth TDD.
#[derive(Default)]
pub struct StateTracker {
    /// Per-agent confirmed belief, keyed by (check, agent).
    agents: HashMap<(CheckId, String), Belief>,
    /// Published state per check, with its hold-down timer.
    published: HashMap<CheckId, Published>,
}

impl StateTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot_checks(&self, checks: &std::collections::HashSet<CheckId>) -> Self {
        Self {
            agents: self
                .agents
                .iter()
                .filter(|((id, _), _)| checks.contains(id))
                .map(|(key, belief)| (key.clone(), belief.clone()))
                .collect(),
            published: self
                .published
                .iter()
                .filter(|(id, _)| checks.contains(id))
                .map(|(id, state)| (*id, *state))
                .collect(),
        }
    }

    pub fn install_checks(&mut self, staged: Self, checks: &std::collections::HashSet<CheckId>) {
        self.agents.retain(|(id, _), _| !checks.contains(id));
        self.published.retain(|id, _| !checks.contains(id));
        self.agents.extend(staged.agents);
        self.published.extend(staged.published);
    }

    /// Record one agent's observation of one check. Returns that agent's
    /// confirmed status afterwards.
    ///
    /// No transition comes out of here any more: one agent changing its mind is
    /// not news on its own, and deciding what it means is the rollup's job.
    /// UP is still never gated — one good observation restores that agent's
    /// opinion, because requiring a run of UPs would make every outage read as
    /// longer than it was.
    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &mut self,
        check_id: CheckId,
        agent_id: &str,
        zone: &str,
        region: &str,
        status: Status,
        threshold: u32,
        ts: i64,
    ) -> Status {
        let threshold = threshold.max(1);
        let key = (check_id, agent_id.to_string());
        let belief = self.agents.entry(key).or_insert(Belief {
            status: Status::Unknown,
            consecutive_failures: 0,
            zone: zone.to_string(),
            region: region.to_string(),
            last_check: ts,
        });
        // A late flush must not replace newer topology or make an agent look
        // fresher than its newest result already did.
        if ts >= belief.last_check {
            belief.zone = zone.to_string();
            belief.region = region.to_string();
            belief.last_check = ts;
        }
        match status {
            Status::Up => {
                belief.consecutive_failures = 0;
                belief.status = Status::Up;
            }
            Status::Down => {
                belief.consecutive_failures = belief.consecutive_failures.saturating_add(1);
                if belief.consecutive_failures >= threshold {
                    belief.status = Status::Down;
                }
                // Below the threshold the agent keeps saying whatever it said
                // before: still inside its confirmation window.
            }
            // Neither a success nor a failure. The counter is left alone so an
            // un-evaluable probe between two failures does not quietly cancel a
            // confirmation in progress.
            Status::Unknown => {}
        }
        belief.status
    }

    /// Every agent with an opinion about this check, for the rollup to fold.
    ///
    /// Returns beliefs as recorded, including stale ones: the caller knows each
    /// agent's cadence and decides which have gone silent. Staleness is not
    /// judged here because the tracker has no clock and no fleet view, and a
    /// second definition of "silent" living in two places is how they drift.
    pub fn children(&self, check_id: CheckId) -> Vec<(String, Belief)> {
        self.agents
            .iter()
            .filter(|((c, _), _)| *c == check_id)
            .map(|((_, a), b)| (a.clone(), b.clone()))
            .collect()
    }

    /// One agent's confirmed opinion, if it has one.
    pub fn current(&self, check_id: CheckId, agent_id: &str) -> Option<Status> {
        self.agents
            .get(&(check_id, agent_id.to_string()))
            .map(|b| b.status)
    }

    /// Consecutive failures this agent has seen toward its threshold.
    pub fn failures(&self, check_id: CheckId, agent_id: &str) -> u32 {
        self.agents
            .get(&(check_id, agent_id.to_string()))
            .map(|b| b.consecutive_failures)
            .unwrap_or(0)
    }

    /// This agent's non-zero failure runs among `checks`: the hint a restarted
    /// agent uses to resume retry/down cadence instead of waiting out a long
    /// schedule. One lookup per assigned check, so the cost is the caller's
    /// share rather than the whole tracker.
    pub fn failing(
        &self,
        agent_id: &str,
        checks: impl IntoIterator<Item = CheckId>,
    ) -> HashMap<CheckId, u32> {
        let mut key = (CheckId::nil(), agent_id.to_string());
        checks
            .into_iter()
            .filter_map(|id| {
                key.0 = id;
                let n = self.agents.get(&key)?.consecutive_failures;
                (n > 0).then_some((id, n))
            })
            .collect()
    }

    /// Restore one agent's belief after a restart, so a confirmation in flight
    /// resumes per agent instead of starting over.
    #[allow(clippy::too_many_arguments)]
    pub fn seed_agent(
        &mut self,
        check_id: CheckId,
        agent_id: &str,
        zone: &str,
        region: &str,
        status: Status,
        consecutive_failures: u32,
        last_check: i64,
    ) {
        self.agents.insert(
            (check_id, agent_id.to_string()),
            Belief {
                status,
                consecutive_failures,
                zone: zone.to_string(),
                region: region.to_string(),
                last_check,
            },
        );
    }

    /// Install a belief observed by *another* coordinator, keeping whichever
    /// observation is newer. Returns whether this one won.
    ///
    /// Regional coordinators each see only the agents that report to them, but
    /// a zone's agents are the corroboration set — `all/all` is meaningless if
    /// one node holds one opinion and its peer holds the other. So leaves
    /// replicate, and every node folds the same tree.
    ///
    /// **The timestamp guard is the whole safety argument.** `/internal/replicate`
    /// has no ordering guarantee and `anti_entropy_sweep` replays historical
    /// pages; an unconditional insert would let a late or replayed batch rewind
    /// a live agent to a belief it held minutes ago, and a rewound leaf is an
    /// invented outage. Comparing `last_check` makes application idempotent and
    /// order-independent, the same rule the poll rows already use.
    #[allow(clippy::too_many_arguments)]
    pub fn merge_agent(
        &mut self,
        check_id: CheckId,
        agent_id: &str,
        zone: &str,
        region: &str,
        status: Status,
        consecutive_failures: u32,
        last_check: i64,
    ) -> bool {
        let key = (check_id, agent_id.to_string());
        // `>=`, not `>`: re-applying the same observation must be a no-op, so a
        // duplicate delivery cannot resurrect a belief a newer local probe has
        // already replaced at the same millisecond.
        if self
            .agents
            .get(&key)
            .is_some_and(|existing| existing.last_check >= last_check)
        {
            return false;
        }
        self.agents.insert(
            key,
            Belief {
                status,
                consecutive_failures,
                zone: zone.to_string(),
                region: region.to_string(),
                last_check,
            },
        );
        true
    }

    /// The state currently published for a check.
    pub fn state(&self, check_id: CheckId) -> Option<NodeState> {
        self.published.get(&check_id).map(|p| p.state)
    }

    /// The hold-down timer's current argument, if one is running.
    pub fn pending(&self, check_id: CheckId) -> Option<(NodeState, i64)> {
        self.published.get(&check_id).and_then(|p| p.pending)
    }

    /// Restore the published state and any hold-down in flight.
    ///
    /// `pending_since` matters across restarts: without it a node that reboots
    /// during a two-minute hold-down would start the two minutes again, so a
    /// flapping coordinator could suppress an outage indefinitely.
    pub fn seed_published(
        &mut self,
        check_id: CheckId,
        state: NodeState,
        pending: Option<(NodeState, i64)>,
    ) {
        self.published.insert(
            check_id,
            Published {
                state,
                pending: pending.filter(|(s, _)| *s != state),
            },
        );
    }

    /// Offer the rollup's conclusion as the check's new state, subject to
    /// hold-down. Returns `Some(Transition)` only when the published state
    /// actually moves.
    ///
    /// Hold-down lives here, inside the publication step, rather than at the
    /// incident site. Everything downstream — alert rows, notifications, WS
    /// events, incidents, gossip — reads the published state, so gating it once
    /// here means a blip that never survives its window produces *no alert row
    /// at all*, not merely no incident. Gating `attach_or_open` instead would
    /// still page.
    ///
    /// `min_incident_secs` gates any move away from Up; `min_recovery_secs`
    /// gates the move back to it.
    pub fn publish(
        &mut self,
        check_id: CheckId,
        candidate: NodeState,
        ts: i64,
        min_incident_secs: u64,
        min_recovery_secs: u64,
    ) -> Option<Transition> {
        let Some(entry) = self.published.get_mut(&check_id) else {
            // First conclusion for this check establishes the baseline. Nothing
            // to alert on yet, and nothing to hold down against.
            self.published.insert(
                check_id,
                Published {
                    state: candidate,
                    pending: None,
                },
            );
            return None;
        };

        if candidate == entry.state {
            // Any return to the published state cancels the timer, so the
            // window has to be survived *unbroken* rather than merely on
            // balance. A check that alternates every probe never announces.
            entry.pending = None;
            return None;
        }

        let hold = if candidate == NodeState::Up {
            min_recovery_secs
        } else {
            min_incident_secs
        };

        let since = match entry.pending {
            // A different candidate restarts the clock. If a pending Degraded is
            // overtaken by Down, the operator asked for N seconds of *Down*
            // before being told about Down — not N seconds of "something".
            Some((state, since)) if state == candidate => since,
            _ => ts,
        };

        if hold > 0 && ts.saturating_sub(since) < (hold as i64).saturating_mul(1000) {
            entry.pending = Some((candidate, since));
            return None;
        }

        let from = entry.state;
        entry.state = candidate;
        entry.pending = None;
        Some(Transition {
            check_id,
            from,
            to: candidate,
        })
    }

    /// Drop every per-agent belief while retaining published roots and their
    /// hold-down state. Used after raw results are purged: the next observations
    /// must establish fresh leaves without making each check look unpublished.
    pub fn clear_leaf_beliefs(&mut self) {
        self.agents.clear();
    }

    /// Forget a check entirely — every agent's belief and its published state.
    /// Used when a check is deleted, so a later check reusing the name does not
    /// inherit a stranded baseline.
    pub fn forget(&mut self, check_id: CheckId) {
        self.agents.retain(|(c, _), _| *c != check_id);
        self.published.remove(&check_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Most tests care about confirmation, not about time.
    const T: i64 = 1_000_000;

    /// Fixed check ids so test bodies stay readable while the tracker only
    /// ever sees `CheckId`.
    fn c() -> CheckId {
        CheckId::from_u128(1)
    }
    fn other() -> CheckId {
        CheckId::from_u128(2)
    }

    #[test]
    fn new_tracker_has_no_state() {
        let t = StateTracker::new();
        assert_eq!(t.current(c(), "a"), None);
        assert_eq!(t.state(c()), None);
        assert!(t.children(c()).is_empty());
    }

    #[test]
    fn one_agents_failures_confirm_independently_of_another() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "unknown", "unknown", Status::Up, 3, T);
        t.observe(c(), "b", "unknown", "unknown", Status::Up, 3, T);

        // Agent A fails twice — not yet confirmed for A, and B is untouched.
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T),
            Status::Up
        );
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T),
            Status::Up
        );
        assert_eq!(t.failures(c(), "a"), 2);
        assert_eq!(t.failures(c(), "b"), 0);
        assert_eq!(t.current(c(), "b"), Some(Status::Up));

        // A third failure confirms it for A alone.
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T),
            Status::Down
        );
        assert_eq!(t.current(c(), "b"), Some(Status::Up));
    }

    #[test]
    fn a_healthy_agent_no_longer_resets_another_agents_run() {
        // The second defect in A1, at its source. Alternating A-down/B-up used
        // to reset one shared counter forever, so a genuinely dead target seen
        // by A could never confirm. Now B's successes are B's business.
        let mut t = StateTracker::new();
        for _ in 0..3 {
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T);
            t.observe(c(), "b", "unknown", "unknown", Status::Up, 3, T);
        }
        assert_eq!(t.current(c(), "a"), Some(Status::Down));
        assert_eq!(t.current(c(), "b"), Some(Status::Up));
        assert_eq!(t.failures(c(), "a"), 3);
    }

    #[test]
    fn threshold_holds_an_agents_opinion_until_confirmed() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "unknown", "unknown", Status::Up, 3, T);
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T),
            Status::Up
        );
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T),
            Status::Up
        );
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T),
            Status::Down
        );
    }

    #[test]
    fn one_up_observation_resets_the_count_and_recovers() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "unknown", "unknown", Status::Up, 3, T);
        t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T);
        t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T);
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Up, 3, T),
            Status::Up
        );
        assert_eq!(t.failures(c(), "a"), 0);
        t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T);
        assert_eq!(t.failures(c(), "a"), 1);
        assert_eq!(t.current(c(), "a"), Some(Status::Up));
    }

    #[test]
    fn recovery_is_never_gated_by_a_threshold() {
        let mut t = StateTracker::new();
        for _ in 0..5 {
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 5, T);
        }
        assert_eq!(t.current(c(), "a"), Some(Status::Down));
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Up, 5, T),
            Status::Up
        );
    }

    #[test]
    fn unknown_does_not_disturb_the_count() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "unknown", "unknown", Status::Up, 2, T);
        t.observe(c(), "a", "unknown", "unknown", Status::Down, 2, T);
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Unknown, 2, T),
            Status::Up
        );
        assert_eq!(t.failures(c(), "a"), 1);
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 2, T),
            Status::Down
        );
    }

    #[test]
    fn zero_threshold_reads_as_one() {
        let mut t = StateTracker::new();
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 0, T),
            Status::Down
        );
    }

    #[test]
    fn children_lists_every_agent_with_an_opinion() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "unknown", "unknown", Status::Up, 1, T);
        t.observe(c(), "b", "unknown", "unknown", Status::Down, 1, T);
        t.observe(other(), "a", "unknown", "unknown", Status::Up, 1, T);

        let mut kids = t.children(c());
        kids.sort_by(|x, y| x.0.cmp(&y.0));
        assert_eq!(kids.len(), 2);
        assert_eq!(kids[0].0, "a");
        assert_eq!(kids[0].1.status, Status::Up);
        assert_eq!(kids[0].1.zone, "unknown");
        assert_eq!(kids[0].1.region, "unknown");
        assert_eq!(kids[1].0, "b");
        assert_eq!(kids[1].1.status, Status::Down);
        // Checks do not leak into each other.
        assert_eq!(t.children(other()).len(), 1);
    }

    #[test]
    fn last_check_never_moves_backwards() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "unknown", "unknown", Status::Up, 1, T);
        // A late flush carrying an older probe must not make the agent look
        // stale, nor make an already-newer agent look fresher than it is.
        t.observe(c(), "a", "unknown", "unknown", Status::Up, 1, T - 60_000);
        assert_eq!(t.children(c())[0].1.last_check, T);
    }

    #[test]
    fn topology_tracks_the_newest_observation() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "zone-a", "region-a", Status::Up, 1, T);
        t.observe(c(), "a", "old-zone", "old-region", Status::Up, 1, T - 1);

        let belief = t.children(c()).pop().unwrap().1;
        assert_eq!(belief.zone, "zone-a");
        assert_eq!(belief.region, "region-a");
        assert_eq!(belief.last_check, T);
    }

    #[test]
    fn seed_restores_an_agents_belief_and_count() {
        let mut t = StateTracker::new();
        t.seed_agent(c(), "a", "unknown", "unknown", Status::Up, 2, T);
        assert_eq!(t.current(c(), "a"), Some(Status::Up));
        assert_eq!(t.failures(c(), "a"), 2);
        let belief = t.children(c()).pop().unwrap().1;
        assert_eq!(belief.zone, "unknown");
        assert_eq!(belief.region, "unknown");
        // Two failures into a three-failure confirmation: one more confirms.
        assert_eq!(
            t.observe(c(), "a", "unknown", "unknown", Status::Down, 3, T),
            Status::Down
        );
    }

    #[test]
    fn a_replicated_belief_lands_when_this_node_has_never_seen_the_agent() {
        // The whole point of replicating leaves: a coordinator that no agent
        // in `us-east-1a` reports to must still hold that zone's full
        // corroboration set, or `all/all` is satisfied by one flaky agent.
        let mut t = StateTracker::new();
        assert!(t.merge_agent(c(), "a2", "us-east-1a", "us-east", Status::Up, 0, T));
        assert_eq!(t.current(c(), "a2"), Some(Status::Up));
        let belief = t.children(c()).pop().unwrap().1;
        assert_eq!(belief.zone, "us-east-1a");
        assert_eq!(belief.region, "us-east");
    }

    #[test]
    fn a_replayed_or_late_batch_never_rewinds_a_belief() {
        // `/internal/replicate` has no ordering guarantee and anti-entropy
        // replays historical pages. An unconditional insert would let a batch
        // from a minute ago put a recovered agent back down — an outage this
        // cluster invented rather than observed.
        let mut t = StateTracker::new();
        assert!(t.merge_agent(c(), "a", "z", "r", Status::Down, 3, T));
        assert!(!t.merge_agent(c(), "a", "z", "r", Status::Up, 0, T - 1));
        assert_eq!(t.current(c(), "a"), Some(Status::Down));
        assert_eq!(t.failures(c(), "a"), 3);

        // Newer still wins, so this is a guard and not a freeze.
        assert!(t.merge_agent(c(), "a", "z", "r", Status::Up, 0, T + 1));
        assert_eq!(t.current(c(), "a"), Some(Status::Up));
        assert_eq!(t.failures(c(), "a"), 0);
    }

    #[test]
    fn re_applying_the_same_observation_is_a_no_op() {
        // Duplicate delivery is normal — push and anti-entropy both carry the
        // same batch. `>=` rather than `>` is what makes application
        // idempotent: at an equal timestamp the belief already installed wins,
        // so a duplicate cannot displace a local probe from the same
        // millisecond.
        let mut t = StateTracker::new();
        t.observe(c(), "a", "z", "r", Status::Up, 3, T);
        assert!(!t.merge_agent(c(), "a", "z", "r", Status::Down, 3, T));
        assert_eq!(t.current(c(), "a"), Some(Status::Up));
    }

    #[test]
    fn merging_carries_the_observers_confirmation_run_rather_than_restarting_it() {
        // The failure count travels with the belief. If a peer re-ran
        // `failures_before_down` from zero, a two-node cluster would take twice
        // as long to confirm a real outage on every node but the observer.
        let mut t = StateTracker::new();
        assert!(t.merge_agent(c(), "a", "z", "r", Status::Up, 2, T));
        assert_eq!(t.failures(c(), "a"), 2);
        assert_eq!(
            t.observe(c(), "a", "z", "r", Status::Down, 3, T + 1),
            Status::Down,
            "one more failure on top of the replicated run should confirm"
        );
    }

    #[test]
    fn merged_leaves_from_two_coordinators_fold_into_one_zone() {
        // The deployment bug this exists for: agents `a1` and `a2` are the
        // redundant pair in one zone but report to different coordinators. Both
        // leaves have to be present on one node before `children` can hand
        // `rollup` a two-agent zone.
        let mut t = StateTracker::new();
        t.observe(c(), "a1", "us-east-1a", "us-east", Status::Up, 3, T);
        assert!(t.merge_agent(c(), "a2", "us-east-1a", "us-east", Status::Down, 3, T));
        let mut leaves = t.children(c());
        leaves.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            leaves.len(),
            2,
            "the zone's corroboration set is both agents"
        );
        assert_eq!(leaves[0].1.status, Status::Up);
        assert_eq!(leaves[1].1.status, Status::Down);
    }

    #[test]
    fn forget_clears_every_trace_of_a_check() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "unknown", "unknown", Status::Down, 1, T);
        t.publish(c(), NodeState::Down, T, 0, 0);
        t.forget(c());
        assert!(t.children(c()).is_empty());
        assert_eq!(t.state(c()), None);
    }

    // --- publication and hold-down ---

    #[test]
    fn first_conclusion_sets_a_baseline_without_a_transition() {
        let mut t = StateTracker::new();
        assert_eq!(t.publish(c(), NodeState::Down, T, 0, 0), None);
        assert_eq!(t.state(c()), Some(NodeState::Down));
    }

    #[test]
    fn an_unchanged_conclusion_is_not_a_transition() {
        let mut t = StateTracker::new();
        t.publish(c(), NodeState::Up, T, 0, 0);
        assert_eq!(t.publish(c(), NodeState::Up, T + 1000, 0, 0), None);
    }

    #[test]
    fn zero_hold_down_publishes_immediately() {
        // The default, and the guarantee that upgrading changes nothing until
        // an operator asks for it.
        let mut t = StateTracker::new();
        t.publish(c(), NodeState::Up, T, 0, 0);
        assert_eq!(
            t.publish(c(), NodeState::Down, T, 0, 0),
            Some(Transition {
                check_id: c(),
                from: NodeState::Up,
                to: NodeState::Down,
            })
        );
    }

    #[test]
    fn hold_down_suppresses_a_blip_entirely() {
        let mut t = StateTracker::new();
        t.publish(c(), NodeState::Up, T, 0, 0);
        // 120s hold-down, 60s interval: two probes down, then recovery.
        assert_eq!(t.publish(c(), NodeState::Down, T, 120, 0), None);
        assert_eq!(t.publish(c(), NodeState::Down, T + 60_000, 120, 0), None);
        // Recovered before the window elapsed: nothing was ever published, so
        // there is no alert row and no incident to explain away.
        assert_eq!(t.publish(c(), NodeState::Up, T + 90_000, 120, 0), None);
        assert_eq!(t.state(c()), Some(NodeState::Up));
        assert_eq!(t.pending(c()), None);
    }

    #[test]
    fn hold_down_announces_once_the_window_is_survived() {
        let mut t = StateTracker::new();
        t.publish(c(), NodeState::Up, T, 0, 0);
        assert_eq!(t.publish(c(), NodeState::Down, T, 120, 0), None);
        assert_eq!(t.publish(c(), NodeState::Down, T + 60_000, 120, 0), None);
        assert_eq!(
            t.publish(c(), NodeState::Down, T + 120_000, 120, 0),
            Some(Transition {
                check_id: c(),
                from: NodeState::Up,
                to: NodeState::Down,
            })
        );
        assert_eq!(t.pending(c()), None);
    }

    #[test]
    fn a_return_to_the_published_state_cancels_the_window() {
        // The window must be survived unbroken. An alternating check that is
        // down half the time never accumulates enough continuous badness.
        let mut t = StateTracker::new();
        t.publish(c(), NodeState::Up, T, 0, 0);
        for i in 0..10 {
            let base = T + i * 120_000;
            assert_eq!(t.publish(c(), NodeState::Down, base, 120, 0), None);
            assert_eq!(t.publish(c(), NodeState::Up, base + 60_000, 120, 0), None);
        }
        assert_eq!(t.state(c()), Some(NodeState::Up));
    }

    #[test]
    fn a_worse_candidate_restarts_the_window_rather_than_inheriting_it() {
        // Pending Degraded for 90s, then Down arrives. The operator asked for
        // 120 seconds of *Down* before hearing about Down, so the clock starts
        // again rather than firing 30 seconds later.
        let mut t = StateTracker::new();
        t.publish(c(), NodeState::Up, T, 0, 0);
        assert_eq!(t.publish(c(), NodeState::Degraded, T, 120, 0), None);
        assert_eq!(t.publish(c(), NodeState::Down, T + 90_000, 120, 0), None);
        assert_eq!(t.pending(c()), Some((NodeState::Down, T + 90_000)));
        assert_eq!(t.publish(c(), NodeState::Down, T + 180_000, 120, 0), None);
        assert!(t
            .publish(c(), NodeState::Down, T + 210_000, 120, 0)
            .is_some());
    }

    #[test]
    fn recovery_has_its_own_window() {
        let mut t = StateTracker::new();
        t.publish(c(), NodeState::Down, T, 0, 0);
        // min_incident_secs of 0 must not let a recovery through early.
        assert_eq!(t.publish(c(), NodeState::Up, T, 0, 300), None);
        assert_eq!(t.publish(c(), NodeState::Up, T + 299_000, 0, 300), None);
        assert!(t.publish(c(), NodeState::Up, T + 300_000, 0, 300).is_some());
    }

    #[test]
    fn hold_down_survives_a_restart_through_pending_since() {
        // A node that reboots mid-window resumes it. Without this, a
        // coordinator restarting more often than the hold-down would suppress
        // an outage forever.
        let mut t = StateTracker::new();
        t.seed_published(c(), NodeState::Up, Some((NodeState::Down, T)));
        assert_eq!(t.pending(c()), Some((NodeState::Down, T)));
        assert!(t
            .publish(c(), NodeState::Down, T + 120_000, 120, 0)
            .is_some());
    }

    #[test]
    fn a_seeded_pending_matching_the_published_state_is_discarded() {
        // Defensive: a stale `pending_since` for the state we already publish
        // would otherwise sit in the map forever, never matching a candidate
        // and never being cleared.
        let mut t = StateTracker::new();
        t.seed_published(c(), NodeState::Up, Some((NodeState::Up, T)));
        assert_eq!(t.pending(c()), None);
    }

    #[test]
    fn clearing_leaf_beliefs_keeps_published_roots() {
        let mut t = StateTracker::new();
        t.observe(c(), "a", "zone", "region", Status::Up, 1, T);
        t.seed_published(c(), NodeState::Down, Some((NodeState::Up, T)));

        t.clear_leaf_beliefs();

        assert!(t.children(c()).is_empty());
        assert_eq!(t.state(c()), Some(NodeState::Down));
        assert_eq!(t.pending(c()), Some((NodeState::Up, T)));
    }

    #[test]
    fn degraded_is_a_first_class_published_state() {
        let mut t = StateTracker::new();
        t.publish(c(), NodeState::Up, T, 0, 0);
        assert_eq!(
            t.publish(c(), NodeState::Degraded, T, 0, 0),
            Some(Transition {
                check_id: c(),
                from: NodeState::Up,
                to: NodeState::Degraded,
            })
        );
        assert_eq!(
            t.publish(c(), NodeState::Down, T, 0, 0),
            Some(Transition {
                check_id: c(),
                from: NodeState::Degraded,
                to: NodeState::Down,
            })
        );
    }
}
