use super::*;
use crate::rollup::rollup;

#[derive(Debug)]
pub(in crate::coordinator) struct RollupCandidate {
    pub(in crate::coordinator) root: NodeState,
    pub(in crate::coordinator) intermediates: Vec<IntermediateState>,
    pub(in crate::coordinator) failing_regions: std::collections::BTreeSet<String>,
}

#[derive(Debug)]
pub(in crate::coordinator) struct RollupTree {
    pub(in crate::coordinator) root: NodeState,
    pub(in crate::coordinator) global_policy: RollupPolicy,
    pub(in crate::coordinator) regions: Vec<RollupRegion>,
}

#[derive(Debug)]
pub(in crate::coordinator) struct RollupRegion {
    pub(in crate::coordinator) name: String,
    pub(in crate::coordinator) state: NodeState,
    pub(in crate::coordinator) policy: RollupPolicy,
    pub(in crate::coordinator) zones: Vec<RollupZone>,
}

#[derive(Debug)]
pub(in crate::coordinator) struct RollupZone {
    pub(in crate::coordinator) name: String,
    pub(in crate::coordinator) state: NodeState,
    pub(in crate::coordinator) policy: RollupPolicy,
    pub(in crate::coordinator) agents: Vec<RollupAgent>,
}

#[derive(Debug)]
pub(in crate::coordinator) struct RollupAgent {
    pub(in crate::coordinator) id: String,
    pub(in crate::coordinator) state: NodeState,
    pub(in crate::coordinator) staleness_ms: i64,
}

#[derive(Debug)]
pub(in crate::coordinator) struct IntermediateState {
    pub(in crate::coordinator) scope: storage::AlertScope,
    pub(in crate::coordinator) node_id: String,
    pub(in crate::coordinator) state: NodeState,
}

impl Coordinator {
    pub(in crate::coordinator) fn rollup_tree(
        &self,
        meta: Option<&CheckMeta>,
        leaves: Vec<(String, state::Belief)>,
        now: i64,
    ) -> RollupTree {
        use std::collections::BTreeMap;

        let policies = self.inner.rollup_policies.read();
        let policy = |override_policy: Option<RollupPolicy>, key: &str, built_in| {
            override_policy
                .or_else(|| policies.get(key).copied())
                .unwrap_or(built_in)
        };

        // Credentials, not self-declared beliefs, define which locations exist.
        // Keeping an Unknown leaf for each credential makes newly configured
        // locations visible before their first agent reports, without affecting
        // rollup thresholds (Unknown is excluded by `rollup`).
        let mut topology: BTreeMap<String, BTreeMap<String, BTreeMap<String, RollupAgent>>> =
            BTreeMap::new();
        for credential in self.inner.credentials.read().values() {
            if credential.revoked() {
                continue;
            }
            topology
                .entry(credential.region.clone())
                .or_default()
                .entry(credential.zone.clone())
                .or_default();
        }

        let credential_topology = !topology.is_empty();
        let seen = self.inner.seen.read();
        for (agent_id, belief) in leaves {
            if !credential_topology {
                topology
                    .entry(belief.region.clone())
                    .or_default()
                    .entry(belief.zone.clone())
                    .or_default();
            }
            let Some(agents) = topology
                .get_mut(&belief.region)
                .and_then(|zones| zones.get_mut(&belief.zone))
            else {
                continue;
            };
            let (staleness_ms, live) = seen.get(&agent_id).map_or_else(
                || {
                    let age = now.saturating_sub(belief.last_check);
                    (
                        age,
                        age <= unreachable_after_ms(self.inner.missed_polls, None),
                    )
                },
                |agent| {
                    let age = now.saturating_sub(agent.at.max(belief.last_check));
                    (age, age <= agent.window_ms)
                },
            );
            agents.insert(
                agent_id.clone(),
                RollupAgent {
                    id: agent_id,
                    state: if live {
                        belief.status.into()
                    } else {
                        NodeState::Unknown
                    },
                    staleness_ms,
                },
            );
        }
        drop(seen);

        let mut regions = Vec::with_capacity(topology.len());
        for (region, zone_map) in topology {
            let mut zones = Vec::with_capacity(zone_map.len());
            for (zone, agent_map) in zone_map {
                let agents: Vec<_> = agent_map.into_values().collect();
                let zone_policy = policy(
                    meta.and_then(|m| m.rollup.zone),
                    &format!("zone:{region}/{zone}"),
                    RollupPolicy::ZONE_DEFAULT,
                );
                let state = rollup(
                    &zone_policy,
                    &agents.iter().map(|agent| agent.state).collect::<Vec<_>>(),
                );
                zones.push(RollupZone {
                    name: zone,
                    state,
                    policy: zone_policy,
                    agents,
                });
            }
            let region_policy = policy(
                meta.and_then(|m| m.rollup.region),
                &format!("region:{region}"),
                RollupPolicy::REGION_DEFAULT,
            );
            let state = rollup(
                &region_policy,
                &zones.iter().map(|zone| zone.state).collect::<Vec<_>>(),
            );
            regions.push(RollupRegion {
                name: region,
                state,
                policy: region_policy,
                zones,
            });
        }
        let global_policy = policy(
            meta.and_then(|m| m.rollup.global),
            "global",
            RollupPolicy::CHECK_DEFAULT,
        );
        let candidate = rollup(
            &global_policy,
            &regions
                .iter()
                .map(|region| region.state)
                .collect::<Vec<_>>(),
        );
        RollupTree {
            root: if candidate == NodeState::Unknown {
                meta.map(|m| m.default_status.into())
                    .unwrap_or(NodeState::Unknown)
            } else {
                candidate
            },
            global_policy,
            regions,
        }
    }

    pub(in crate::coordinator) fn rollup_candidate(
        &self,
        check_id: &str,
        meta: Option<&CheckMeta>,
        leaves: Vec<(String, state::Belief)>,
        now: i64,
    ) -> RollupCandidate {
        let tree = self.rollup_tree(meta, leaves, now);
        let mut intermediates = Vec::new();
        let mut failing_regions = std::collections::BTreeSet::new();
        for region in &tree.regions {
            for zone in &region.zones {
                intermediates.push(IntermediateState {
                    scope: storage::AlertScope::Zone,
                    node_id: format!("check:{check_id}/region:{}/zone:{}", region.name, zone.name),
                    state: zone.state,
                });
            }
            if region.state.is_failing() {
                failing_regions.insert(region.name.clone());
            }
            intermediates.push(IntermediateState {
                scope: storage::AlertScope::Region,
                node_id: format!("check:{check_id}/region:{}", region.name),
                state: region.state,
            });
        }
        RollupCandidate {
            root: tree.root,
            intermediates,
            failing_regions,
        }
    }
}
