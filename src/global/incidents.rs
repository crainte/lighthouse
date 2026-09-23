//! Global-tier presentation over the incidents the region relay already
//! merges: folding one outage's per-region rows into a single group, and
//! attributing a dependent's incident to a cross-region upstream.
//!
//! Both are read-side only. Nothing here writes anywhere — a regional
//! incident record is exactly what it was before this module ever runs, per
//! SCALING.md's "Cross-region dependency belongs to the global tier": relaying
//! a "join this incident" instruction down would make incident membership a
//! two-writer problem, and a region cannot act on an upstream it does not
//! probe anyway. This module only reshapes what an operator reading the
//! global dashboard is shown.

use crate::models::CheckId;
use serde::Serialize;
use std::collections::HashMap;

/// One incident as read off a region's answer to `/api/v1/incidents`, reduced
/// to the fields grouping and attribution need. `id` is the composite
/// `"<region>:<local>"` id the merge already stamped on it, which is also
/// where `region` comes from — the merge never carries the region alongside
/// the item separately, so the composite id is the one place both are joined.
#[derive(Debug, Clone)]
pub struct RawIncident {
    pub id: String,
    pub region: String,
    pub origin_check: Option<CheckId>,
    pub title: String,
    pub state: String,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
}

impl RawIncident {
    /// Parse one merged incident. `None` on anything unparseable — a
    /// same-region-elsewhere id, or a response shape from a build old enough
    /// to be missing a field — rather than failing the whole page over one
    /// bad row.
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        let id = value.get("id")?.as_str()?.to_string();
        let (region, _local) = super::parse_composite_incident_id(&id)?;
        Some(Self {
            id,
            region,
            origin_check: value
                .get("origin_check")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            title: value.get("title")?.as_str()?.to_string(),
            state: value.get("state")?.as_str()?.to_string(),
            started_at: value.get("started_at")?.as_i64()?,
            resolved_at: value.get("resolved_at").and_then(|v| v.as_i64()),
        })
    }

    /// The window this incident covers for overlap purposes: detection to
    /// resolution, or to `now` while still open. An open incident's window
    /// therefore always reaches the instant grouping runs — see
    /// `group_and_attribute` for why that is exactly the rule this needs.
    fn window(&self, now: i64) -> (i64, i64) {
        (self.started_at, self.resolved_at.unwrap_or(now))
    }
}

/// One region's incident inside a group, preserved in full so folding rows
/// together never hides *which* regions were affected, when each detected it,
/// or that one recovered while another did not.
#[derive(Debug, Clone, Serialize)]
pub struct RegionIncident {
    pub region: String,
    pub id: String,
    pub title: String,
    pub state: String,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
}

/// A hint that a dependent group's outage is probably explained by an
/// upstream's, surfaced for the operator rather than folded into the
/// dependent's own row — the dependent keeps its own regions and its own id.
#[derive(Debug, Clone, Serialize)]
pub struct Attribution {
    pub check_id: String,
    pub region: String,
    pub incident_id: String,
}

/// One outage of one check, folded from however many regions' incidents
/// describe it. See `group_and_attribute` for the grouping and attribution
/// rules.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentGroup {
    /// Stable across repeated calls over the same inputs: the earliest
    /// member's own composite id, so the group's identity does not depend on
    /// map iteration order and a group can be linked to from elsewhere.
    pub id: String,
    pub origin_check: Option<String>,
    pub title: String,
    pub state: String,
    pub started_at: i64,
    /// `Some` only once every member has resolved. A region still down keeps
    /// the whole group open — the honest summary of "this outage is not over
    /// everywhere yet", which is the second half of the fan-out problem this
    /// phase exists to surface (TODO.md: "and that `tw` never recovered").
    pub resolved_at: Option<i64>,
    pub regions: Vec<RegionIncident>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributed_to: Option<Attribution>,
}

/// Group per-region incidents of the same check into one row, then attribute
/// each dependent's group to an overlapping cross-region upstream outage.
///
/// **Grouping window.** Two incidents sharing an `origin_check` are folded
/// into one group when their windows — `[started_at, resolved_at ?? now]` —
/// overlap. An open incident's window always reaches `now`, so two regions
/// that are *currently* down for the same check always land in one group no
/// matter how many seconds apart they were each confirmed — the
/// `…-1790119380023` (us) / `…-1790119440028` (tw) case TODO.md records.
/// Once a region's incident resolves, its window closes at that instant, so a
/// *later*, unrelated outage of the same check — whose own window starts only
/// after every prior member's window has closed — opens a fresh group rather
/// than being absorbed into the old one. With three or more members this
/// applies transitively (a sorted interval-merge, not each member compared
/// only against the first), so a chain of overlapping windows collapses to
/// one group even where the first and last member do not themselves overlap.
///
/// Incidents with no `origin_check` (opened by hand, e.g. a maintenance
/// notice) never merge with one another: they carry no check identity to
/// group on, and two unrelated hand-opened incidents happening to overlap in
/// time is not the fan-out this exists to fold away. Each is its own group of
/// one.
///
/// **Attribution.** A group's `origin_check` may declare upstreams via
/// `depends_on`. When an upstream has its own group open in a *different*
/// region whose window overlaps this one, the dependent group is attributed
/// to it. This is a presentation hint ("probably caused by X"), not a merge —
/// the dependent keeps its own row, its own id, and its own regions. A
/// same-region upstream is deliberately not attributed here: the region
/// already joined it locally as a `dependency` attachment
/// (`Coordinator::attach_or_open`), so it never shows up as a same-region
/// `origin_check` incident for global to re-derive.
pub fn group_and_attribute(
    raw: &[RawIncident],
    depends_on: &HashMap<CheckId, Vec<CheckId>>,
    now: i64,
) -> Vec<IncidentGroup> {
    let mut groups = group(raw, now);
    attribute(&mut groups, depends_on);
    groups
}

fn group(raw: &[RawIncident], now: i64) -> Vec<IncidentGroup> {
    let mut by_check: HashMap<CheckId, Vec<&RawIncident>> = HashMap::new();
    let mut unowned: Vec<&RawIncident> = Vec::new();
    for incident in raw {
        match incident.origin_check {
            Some(check_id) => by_check.entry(check_id).or_default().push(incident),
            None => unowned.push(incident),
        }
    }

    let mut groups = Vec::new();
    for (_check_id, mut members) in by_check {
        members.sort_by_key(|m| m.started_at);
        let mut current: Vec<&RawIncident> = Vec::new();
        let mut current_end = i64::MIN;
        for member in members {
            let (start, end) = member.window(now);
            if current.is_empty() || start <= current_end {
                current.push(member);
                current_end = current_end.max(end);
            } else {
                groups.push(build_group(&current));
                current = vec![member];
                current_end = end;
            }
        }
        if !current.is_empty() {
            groups.push(build_group(&current));
        }
    }
    for incident in unowned {
        groups.push(build_group(&[incident]));
    }

    groups.sort_by_key(|g| std::cmp::Reverse(g.started_at));
    groups
}

fn build_group(members: &[&RawIncident]) -> IncidentGroup {
    let earliest = members
        .iter()
        .min_by_key(|m| m.started_at)
        .expect("build_group is never called with an empty slice");
    let started_at = earliest.started_at;
    let resolved_at = members
        .iter()
        .all(|m| m.resolved_at.is_some())
        .then(|| members.iter().filter_map(|m| m.resolved_at).max())
        .flatten();
    // The headline favours a still-open member over a resolved one: an
    // operator opening a group that is still affecting one region wants to
    // know what is happening now, not what a region that has since recovered
    // called it first.
    let headline = members
        .iter()
        .find(|m| m.resolved_at.is_none())
        .copied()
        .unwrap_or(earliest);
    let mut regions: Vec<RegionIncident> = members
        .iter()
        .map(|m| RegionIncident {
            region: m.region.clone(),
            id: m.id.clone(),
            title: m.title.clone(),
            state: m.state.clone(),
            started_at: m.started_at,
            resolved_at: m.resolved_at,
        })
        .collect();
    regions.sort_by(|a, b| a.region.cmp(&b.region));
    IncidentGroup {
        id: earliest.id.clone(),
        origin_check: earliest.origin_check.map(|c| c.to_string()),
        title: headline.title.clone(),
        state: headline.state.clone(),
        started_at,
        resolved_at,
        regions,
        attributed_to: None,
    }
}

fn attribute(groups: &mut [IncidentGroup], depends_on: &HashMap<CheckId, Vec<CheckId>>) {
    struct Snapshot {
        check_id: Option<CheckId>,
        start: i64,
        end: i64,
        regions: Vec<String>,
    }
    // A snapshot up front so deciding one group's attribution never sees
    // another group mutated mid-pass — attribution reads every group but
    // writes only its own `attributed_to`.
    let snapshot: Vec<Snapshot> = groups
        .iter()
        .map(|g| Snapshot {
            check_id: g.origin_check.as_deref().and_then(|s| s.parse().ok()),
            start: g.started_at,
            // An open group's window reaches "now" for grouping, but no
            // single shared "now" survives into this snapshot; `resolved_at
            // ?? i64::MAX` is equivalent for an overlap test, since an open
            // group overlaps anything that has not ended before it started.
            end: g.resolved_at.unwrap_or(i64::MAX),
            regions: g.regions.iter().map(|r| r.region.clone()).collect(),
        })
        .collect();

    let mut decisions: Vec<Option<Attribution>> = vec![None; groups.len()];
    for (idx, dependent) in snapshot.iter().enumerate() {
        let Some(check_id) = dependent.check_id else {
            continue;
        };
        let Some(upstreams) = depends_on.get(&check_id) else {
            continue;
        };
        // First upstream with an overlapping, cross-region group wins — the
        // same one-hop, first-match spirit as the local `attach_or_open`,
        // just re-derived at the tier where both ends are visible.
        let attribution = upstreams.iter().find_map(|upstream| {
            snapshot.iter().enumerate().find_map(|(other_idx, other)| {
                if other_idx == idx || other.check_id != Some(*upstream) {
                    return None;
                }
                let overlaps = other.start <= dependent.end && dependent.start <= other.end;
                if !overlaps {
                    return None;
                }
                let foreign_region = other
                    .regions
                    .iter()
                    .find(|r| !dependent.regions.contains(r))?;
                Some(Attribution {
                    check_id: upstream.to_string(),
                    region: foreign_region.clone(),
                    incident_id: groups[other_idx].id.clone(),
                })
            })
        });
        decisions[idx] = attribution;
    }

    for (group, decision) in groups.iter_mut().zip(decisions) {
        group.attributed_to = decision;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incident(
        region: &str,
        check: CheckId,
        started_at: i64,
        resolved_at: Option<i64>,
    ) -> RawIncident {
        RawIncident {
            id: format!("{region}:{check}-{started_at}"),
            region: region.to_string(),
            origin_check: Some(check),
            title: "x is down".to_string(),
            state: if resolved_at.is_some() {
                "resolved".to_string()
            } else {
                "detected".to_string()
            },
            started_at,
            resolved_at,
        }
    }

    fn cid(n: u128) -> CheckId {
        CheckId::from_u128(n)
    }

    /// The exact TODO.md case: `dns` detected DOWN in `us` at 16:23:00 and in
    /// `tw` a minute later, both still open. One group, both regions present.
    #[test]
    fn two_regions_seconds_apart_become_one_group() {
        let dns = cid(1);
        let raw = vec![
            incident("us", dns, 1_790_119_380_023, None),
            incident("tw", dns, 1_790_119_440_028, None),
        ];
        let groups = group_and_attribute(&raw, &HashMap::new(), 1_790_119_500_000);
        assert_eq!(groups.len(), 1, "one outage, one group");
        let group = &groups[0];
        assert_eq!(group.regions.len(), 2);
        assert!(group.resolved_at.is_none(), "still open in tw");
        let tw_region = "tw is never absent";
        assert!(
            group.regions.iter().any(|r| r.region == "tw"),
            "{tw_region}"
        );
        assert!(group.regions.iter().any(|r| r.region == "us"));
    }

    /// `tw` recovers, `us` does not: the group stays open, and each region's
    /// own recovery state survives inside it — folding rows must not erase
    /// "tw never recovered".
    #[test]
    fn group_stays_open_while_any_member_is_open() {
        let dns = cid(1);
        let raw = vec![
            incident("us", dns, 1_000, None),
            incident("tw", dns, 1_010, Some(2_000)),
        ];
        let groups = group_and_attribute(&raw, &HashMap::new(), 3_000);
        assert_eq!(groups.len(), 1);
        assert!(groups[0].resolved_at.is_none());
        let tw = groups[0].regions.iter().find(|r| r.region == "tw").unwrap();
        assert_eq!(tw.resolved_at, Some(2_000), "tw's own recovery is kept");
        let us = groups[0].regions.iter().find(|r| r.region == "us").unwrap();
        assert!(us.resolved_at.is_none(), "us's own outage is kept");
    }

    /// A later, disjoint outage of the same check must not be absorbed into
    /// the first: both regions recovered from the first before the second
    /// began, so their windows never overlap.
    #[test]
    fn a_later_separate_outage_stays_a_separate_group() {
        let dns = cid(1);
        let raw = vec![
            incident("us", dns, 1_000, Some(1_100)),
            incident("tw", dns, 1_010, Some(1_110)),
            // A wholly different outage of the same check, well after both
            // regions recovered from the first.
            incident("us", dns, 5_000, Some(5_100)),
        ];
        let groups = group_and_attribute(&raw, &HashMap::new(), 6_000);
        assert_eq!(groups.len(), 2, "the later outage is its own group");
        let earlier = groups.iter().find(|g| g.started_at == 1_000).unwrap();
        assert_eq!(earlier.regions.len(), 2);
        let later = groups.iter().find(|g| g.started_at == 5_000).unwrap();
        assert_eq!(later.regions.len(), 1);
    }

    /// Incidents opened by hand (no `origin_check`) never merge with one
    /// another, even if their windows happen to overlap: there is no shared
    /// check identity to group on.
    #[test]
    fn manually_opened_incidents_never_merge() {
        let raw = vec![
            RawIncident {
                id: "us:maint-1".to_string(),
                region: "us".to_string(),
                origin_check: None,
                title: "maintenance".to_string(),
                state: "detected".to_string(),
                started_at: 1_000,
                resolved_at: None,
            },
            RawIncident {
                id: "tw:maint-2".to_string(),
                region: "tw".to_string(),
                origin_check: None,
                title: "unrelated notice".to_string(),
                state: "detected".to_string(),
                started_at: 1_005,
                resolved_at: None,
            },
        ];
        let groups = group_and_attribute(&raw, &HashMap::new(), 2_000);
        assert_eq!(groups.len(), 2);
    }

    /// A dependent's incident, opened independently in its own region because
    /// its upstream is probed only elsewhere, is attributed to the upstream's
    /// overlapping cross-region incident.
    #[test]
    fn dependent_attributed_to_overlapping_cross_region_upstream() {
        let upstream = cid(1);
        let dependent = cid(2);
        let raw = vec![
            incident("us", upstream, 1_000, None),
            incident("tw", dependent, 1_005, None),
        ];
        let mut depends_on = HashMap::new();
        depends_on.insert(dependent, vec![upstream]);
        let groups = group_and_attribute(&raw, &depends_on, 2_000);
        let dependent_group = groups
            .iter()
            .find(|g| g.origin_check.as_deref() == Some(&dependent.to_string()))
            .unwrap();
        let attribution = dependent_group
            .attributed_to
            .as_ref()
            .expect("overlapping cross-region upstream should attribute");
        assert_eq!(attribution.check_id, upstream.to_string());
        assert_eq!(attribution.region, "us");
    }

    /// The upstream's outage ended well before the dependent's began: no
    /// overlap, so no attribution is offered.
    #[test]
    fn dependent_not_attributed_when_windows_do_not_overlap() {
        let upstream = cid(1);
        let dependent = cid(2);
        let raw = vec![
            incident("us", upstream, 1_000, Some(1_100)),
            incident("tw", dependent, 5_000, None),
        ];
        let mut depends_on = HashMap::new();
        depends_on.insert(dependent, vec![upstream]);
        let groups = group_and_attribute(&raw, &depends_on, 6_000);
        let dependent_group = groups
            .iter()
            .find(|g| g.origin_check.as_deref() == Some(&dependent.to_string()))
            .unwrap();
        assert!(dependent_group.attributed_to.is_none());
    }

    /// A same-region upstream is not attributed here: the region already
    /// joined it locally, so it never appears as a same-region `origin_check`
    /// incident for global to re-derive. This guards against attributing to a
    /// coincidentally-overlapping same-region row if one somehow appeared.
    #[test]
    fn same_region_upstream_is_not_attributed() {
        let upstream = cid(1);
        let dependent = cid(2);
        let raw = vec![
            incident("us", upstream, 1_000, None),
            incident("us", dependent, 1_005, None),
        ];
        let mut depends_on = HashMap::new();
        depends_on.insert(dependent, vec![upstream]);
        let groups = group_and_attribute(&raw, &depends_on, 2_000);
        let dependent_group = groups
            .iter()
            .find(|g| g.origin_check.as_deref() == Some(&dependent.to_string()))
            .unwrap();
        assert!(dependent_group.attributed_to.is_none());
    }
}
