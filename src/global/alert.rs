//! Cross-region alerting for the global coordinator.
//!
//! The regions already page on their own outages, so the global node must not
//! re-page every transition — that is the double-paging trap. It fires only on
//! conditions no single region can see:
//!
//! - a check that is Down in **every** region currently reporting it, and
//! - a region that has gone **dark** (no push within the staleness window).
//!
//! Both are edge-triggered: a condition pages once when it becomes true and
//! once more (recovery) when it clears — never every tick it persists.
//!
//! **The firing set is durable and shared.** Each firing condition is a
//! global `Setting` under `global-alert:…`, so it survives a restart and rides
//! the global-to-global config plane to every peer. Whichever node owns
//! alerting next (see `GlobalNode::owns_alerting`) diffs against the same set
//! and so neither re-pages an ongoing condition nor sends a false recovery.
//! The record is written *before* the notification is sent: a crash between
//! the two loses that one page rather than repeating it. Right before acting
//! on an edge, the record is re-read, so a node that briefly overlaps with the
//! previous owner skips an edge that owner already recorded.
//!
//! Silent entirely when no notifiers are configured, which is the default.

use crate::coordinator::storage::ConfigEntry;
use crate::global::cache::{AggregatedCheck, GlobalCache};
use crate::global::store::GlobalStore;
use crate::models::{CheckId, NodeState};
use crate::notify::Notifier;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

/// Setting-key prefix for the durable firing set.
pub const KEY_PREFIX: &str = "global-alert:";

/// A global alert condition, keyed by identity only: a check's display name
/// is carried in the record for the message, so renaming a check that is
/// already down does not look like a new condition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Condition {
    /// A check is down in every region reporting it.
    CheckDownEverywhere(CheckId),
    /// A region stopped pushing.
    RegionDark(String),
}

impl Condition {
    fn check_id(&self) -> Option<CheckId> {
        match self {
            Condition::CheckDownEverywhere(id) => Some(*id),
            Condition::RegionDark(_) => None,
        }
    }

    pub(crate) fn key(&self) -> String {
        match self {
            Condition::CheckDownEverywhere(id) => format!("{KEY_PREFIX}check:{id}"),
            Condition::RegionDark(region) => format!("{KEY_PREFIX}region:{region}"),
        }
    }

    fn from_key(key: &str) -> Option<Self> {
        let rest = key.strip_prefix(KEY_PREFIX)?;
        if let Some(id) = rest.strip_prefix("check:") {
            id.parse().ok().map(Condition::CheckDownEverywhere)
        } else {
            rest.strip_prefix("region:")
                .map(|r| Condition::RegionDark(r.to_string()))
        }
    }
}

fn check_title(name: &str) -> String {
    format!("check {name} is down in every region")
}

fn region_title(region: &str) -> String {
    format!("region {region} has gone dark")
}

/// What is stored per firing condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FiringRecord {
    opened_at: i64,
    /// The message the opening page carried, reused for the recovery so the
    /// pair reads the same even if the check was renamed in between.
    title: String,
}

/// Evaluates and dispatches global alerts.
pub struct GlobalAlerter {
    client: reqwest::Client,
    notifiers: Vec<Notifier>,
    /// Serialises evaluations on this node: the diff and its writes must not
    /// interleave with another tick's.
    evaluating: tokio::sync::Mutex<()>,
    #[cfg(test)]
    pub(crate) sent: parking_lot::Mutex<Vec<(String, NodeState)>>,
}

impl GlobalAlerter {
    pub fn new(notifiers: Vec<Notifier>) -> Self {
        Self {
            client: reqwest::Client::new(),
            notifiers,
            evaluating: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            sent: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Whether any notifier is configured. When false, `evaluate` is a no-op
    /// and the caller can skip building the inputs entirely.
    pub fn active(&self) -> bool {
        !self.notifiers.is_empty()
    }

    /// Recompute conditions from the cache, diff them against the durable
    /// firing set, record each edge, then page it. Returns the config entries
    /// written, for the caller to broadcast to peer global nodes.
    ///
    /// The caller decides *whether* this node may evaluate at all (owner,
    /// in sync); this only does the edge detection.
    pub async fn evaluate(
        &self,
        cache: &GlobalCache,
        store: &GlobalStore,
        now: i64,
    ) -> Vec<ConfigEntry> {
        if !self.active() {
            return Vec::new();
        }
        let _guard = self.evaluating.lock().await;
        let aggregate = cache.aggregate(now);
        let roster = cache.roster(now);

        let mut current: HashMap<Condition, String> = HashMap::new();
        for check in &aggregate {
            if is_down_everywhere(check) {
                current.insert(
                    Condition::CheckDownEverywhere(check.check_id),
                    check_title(&check.name),
                );
            }
        }
        for region in &roster {
            // A region only counts as "dark" once it has ever pushed: a region
            // registered but never yet heard from is not an outage.
            if region.stale && region.age_ms >= 0 {
                current.insert(
                    Condition::RegionDark(region.region.clone()),
                    region_title(&region.region),
                );
            }
        }
        let known_regions: HashSet<&str> = roster.iter().map(|r| r.region.as_str()).collect();

        let firing: HashMap<Condition, FiringRecord> = match store.firing_alerts().await {
            Ok(rows) => rows
                .into_iter()
                .filter_map(|(key, value)| {
                    let condition = Condition::from_key(&key)?;
                    let record = serde_json::from_str(&value).unwrap_or(FiringRecord {
                        opened_at: 0,
                        title: key.clone(),
                    });
                    Some((condition, record))
                })
                .collect(),
            Err(error) => {
                // Without the firing set every edge is a guess; skip the tick
                // rather than risk a repeat page.
                warn!(%error, "global alert: could not read firing set, skipping evaluation");
                return Vec::new();
            }
        };

        let mut entries = Vec::new();
        let mut opened: Vec<(&Condition, &String)> = current
            .iter()
            .filter(|(c, _)| !firing.contains_key(*c))
            .collect();
        opened.sort_by_key(|(c, _)| c.key());
        for (condition, title) in opened {
            let key = condition.key();
            // Another owner may have recorded this edge since the read above.
            if store.alert_firing(&key).await.unwrap_or(false) {
                continue;
            }
            let record = FiringRecord {
                opened_at: now,
                title: title.clone(),
            };
            let Ok(json) = serde_json::to_string(&record) else {
                continue;
            };
            match store.put_alert(&key, &json).await {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    warn!(%key, %error, "global alert: could not record opening, not paging");
                    continue;
                }
            }
            self.page(condition, title, NodeState::Down).await;
        }

        let mut cleared: Vec<(&Condition, &FiringRecord)> = firing
            .iter()
            .filter(|(c, _)| !current.contains_key(*c))
            .collect();
        cleared.sort_by_key(|(c, _)| c.key());
        for (condition, record) in cleared {
            let key = condition.key();
            if !store.alert_firing(&key).await.unwrap_or(true) {
                continue;
            }
            match store.clear_alert(&key).await {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    warn!(%key, %error, "global alert: could not record recovery, not paging");
                    continue;
                }
            }
            // A removed region did not recover; it stopped existing. Drop
            // its record without paging.
            if let Condition::RegionDark(region) = condition {
                if !known_regions.contains(region.as_str()) {
                    continue;
                }
            }
            self.page(condition, &record.title, NodeState::Up).await;
        }
        entries
    }

    /// Send one transition to every configured notifier. `to` is Down when the
    /// condition opened, Up when it cleared, so a webhook sees a normal
    /// UP/DOWN pair rather than a bespoke event type.
    async fn page(&self, condition: &Condition, title: &str, to: NodeState) {
        #[cfg(test)]
        self.sent.lock().push((condition.key(), to));
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
            condition = %title,
            check = ?condition.check_id(),
            to = to.as_str(),
            "global alert"
        );
        for notifier in &self.notifiers {
            if let Err(e) = crate::notify::send(&self.client, notifier, &transition, title).await {
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
            uptime_24h: None,
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
