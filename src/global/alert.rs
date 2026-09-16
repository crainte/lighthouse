//! Cross-region alerting for the global coordinator.
//!
//! The regions already page on their own outages, so the global node must not
//! re-page every transition — that is the double-paging trap. It fires only on
//! conditions no single region can see:
//!
//! - a check that is Down in **every** region currently reporting it, and
//! - a region that has gone **dark** (no push within the staleness window).
//!
//! Both are edge-triggered against a small in-memory map of last-fired states,
//! so a condition pages once when it becomes true and once more (recovery) when
//! it clears — never every tick it persists. Silent entirely when no notifiers
//! are configured, which is the default.

use crate::global::cache::{AggregatedCheck, GlobalCache};
use crate::models::{CheckId, NodeState};
use crate::notify::Notifier;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

/// A global alert condition, named so the debounce map has a stable key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Condition {
    /// A check is down in every region reporting it.
    CheckDownEverywhere(CheckId, String),
    /// A region stopped pushing.
    RegionDark(String),
}

impl Condition {
    fn check_id(&self) -> Option<CheckId> {
        match self {
            Condition::CheckDownEverywhere(id, _) => Some(*id),
            Condition::RegionDark(_) => None,
        }
    }

    fn title(&self) -> String {
        match self {
            Condition::CheckDownEverywhere(_, name) => {
                format!("check {name} is down in every region")
            }
            Condition::RegionDark(region) => format!("region {region} has gone dark"),
        }
    }
}

/// Evaluates and dispatches global alerts. Holds the notifier list and the set
/// of conditions currently firing.
pub struct GlobalAlerter {
    client: reqwest::Client,
    notifiers: Vec<Notifier>,
    firing: Mutex<HashSet<Condition>>,
}

impl GlobalAlerter {
    pub fn new(notifiers: Vec<Notifier>) -> Self {
        Self {
            client: reqwest::Client::new(),
            notifiers,
            firing: Mutex::new(HashSet::new()),
        }
    }

    /// Whether any notifier is configured. When false, `evaluate` is a no-op
    /// and the caller can skip building the inputs entirely.
    pub fn active(&self) -> bool {
        !self.notifiers.is_empty()
    }

    /// Recompute conditions from the current aggregate and roster, then page on
    /// each edge — a newly-true condition, and a newly-cleared one.
    pub async fn evaluate(&self, cache: &GlobalCache, now: i64) {
        if !self.active() {
            return;
        }
        let aggregate = cache.aggregate(now);
        let roster = cache.roster(now);

        let mut current: HashSet<Condition> = HashSet::new();
        for check in &aggregate {
            if is_down_everywhere(check) {
                current.insert(Condition::CheckDownEverywhere(
                    check.check_id,
                    check.name.clone(),
                ));
            }
        }
        for region in &roster {
            // A region only counts as "dark" once it has ever pushed: a region
            // declared in config but never yet heard from is not an outage.
            if region.stale && region.age_ms >= 0 {
                current.insert(Condition::RegionDark(region.region.clone()));
            }
        }

        // Diff against what was firing, under the lock, so two overlapping
        // ticks cannot both decide to open the same alert.
        let (opened, cleared) = {
            let mut firing = self.firing.lock();
            let opened: Vec<Condition> = current.difference(&firing).cloned().collect();
            let cleared: Vec<Condition> = firing.difference(&current).cloned().collect();
            *firing = current;
            (opened, cleared)
        };

        for c in opened {
            self.page(&c, NodeState::Down).await;
        }
        for c in cleared {
            self.page(&c, NodeState::Up).await;
        }
    }

    /// Send one transition to every configured notifier. `to` is Down when the
    /// condition opened, Up when it cleared, so a webhook sees a normal
    /// UP/DOWN pair rather than a bespoke event type.
    async fn page(&self, condition: &Condition, to: NodeState) {
        let transition = crate::coordinator::state::Transition {
            // Region-dark conditions have no check identity. Use a stable
            // sentinel only for notifier transport; check conditions retain
            // their native UUID end-to-end.
            check_id: condition.check_id().unwrap_or_else(CheckId::nil),
            from: if to == NodeState::Down {
                NodeState::Up
            } else {
                NodeState::Down
            },
            to,
        };
        info!(
            condition = %condition.title(),
            check = ?condition.check_id(),
            to = to.as_str(),
            "global alert"
        );
        for notifier in &self.notifiers {
            if let Err(e) =
                crate::notify::send(&self.client, notifier, &transition, &condition.title()).await
            {
                warn!(notifier = %notifier.name, error = %e, "global alert delivery failed");
            }
        }
    }
}

/// A check is down everywhere when it is reported by at least one region and
/// every region reporting it reads Down. A single Up or Degraded region means
/// the service is reachable from somewhere, which the regions handle alone.
fn is_down_everywhere(check: &AggregatedCheck) -> bool {
    let per_region: &HashMap<String, NodeState> = &check.per_region;
    !per_region.is_empty() && per_region.values().all(|s| *s == NodeState::Down)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic per-name check id, not random, so the id an assertion
    /// checks against is the same one the fixture built.
    fn cid(name: &str) -> CheckId {
        let mut bytes = [0u8; 16];
        let src = name.as_bytes();
        let n = src.len().min(16);
        bytes[..n].copy_from_slice(&src[..n]);
        CheckId::from_u128(u128::from_be_bytes(bytes))
    }

    fn check(id: &str, regions: &[(&str, NodeState)]) -> AggregatedCheck {
        AggregatedCheck {
            check_id: cid(id),
            name: id.into(),
            state: NodeState::Down,
            per_region: regions.iter().map(|(r, s)| ((*r).into(), *s)).collect(),
            latency_ms: 0,
            last_check: 0,
            uptime_24h: 0.0,
            status_code: None,
            error: None,
            kind: None,
            group: None,
            latency_graph: false,
            position: 0,
        }
    }

    #[test]
    fn down_everywhere_needs_all_regions_down() {
        assert!(is_down_everywhere(&check(
            "api",
            &[("us", NodeState::Down), ("tw", NodeState::Down)]
        )));
        assert!(!is_down_everywhere(&check(
            "api",
            &[("us", NodeState::Down), ("tw", NodeState::Up)]
        )));
        assert!(!is_down_everywhere(&check("api", &[])));
    }
}
