//! Test fixtures shared across every domain's test module. `#[cfg(test)]`
//! only; not part of the storage split's public surface.
use super::*;
use chrono::DateTime;

/// Deterministic per-name check id so test bodies stay readable while
/// storage only ever sees `CheckId`. Packing the name's bytes (rather than
/// hashing) also keeps `Ord` on the id agreeing with `Ord` on the name for
/// these short ASCII fixtures, which several tests rely on for ordering.
pub(crate) fn cid(name: &str) -> CheckId {
    let mut bytes = [0u8; 16];
    let src = name.as_bytes();
    let n = src.len().min(16);
    bytes[..n].copy_from_slice(&src[..n]);
    CheckId::from_u128(u128::from_be_bytes(bytes))
}
pub(crate) fn result(check_id: &str, ts_millis: i64, latency_ms: u64) -> CheckResult {
    CheckResult {
        zone: "test-zone".into(),
        region: "test-region".into(),
        body: None,
        headers: None,
        check_id: cid(check_id),
        agent_id: "agent".into(),
        kind: crate::models::CheckKind::Http,
        timestamp: DateTime::from_timestamp_millis(ts_millis).unwrap(),
        latency_ms,
        status_code: Some(200),
        body_matched: None,
        error: None,
        failure: None,
        metric: None,
        metrics: None,
    }
}
pub(crate) fn agent_state(
    check_id: &str,
    agent_id: &str,
    status: Status,
    ts: i64,
) -> AgentCheckStateRow {
    AgentCheckStateRow {
        check_id: cid(check_id),
        agent_id: agent_id.into(),
        zone: "test-zone".into(),
        region: "test-region".into(),
        status,
        consecutive_failures: 0,
        last_check: ts,
    }
}
pub(crate) fn published(check_id: &str, state: NodeState, ts: i64) -> PublishedStateRow {
    PublishedStateRow {
        check_id: cid(check_id),
        state,
        pending: None,
        last_check: ts,
    }
}
// ---- Clustering: data-plane replication ----

pub(crate) fn check_config(name: &str, target: &str) -> CheckConfig {
    CheckConfig {
        name: name.into(),
        target: target.into(),
        ..Default::default()
    }
}
pub(crate) fn setting_entry(id: &str, millis: i64, node: &str, value: &str) -> ConfigEntry {
    ConfigEntry {
        kind: ConfigKind::Setting,
        id: id.into(),
        hlc: Hlc::new(millis, node),
        document: Some(serde_json::to_string(&value.to_string()).unwrap()),
    }
}
pub(crate) fn rollup_policy_entry(
    id: &str,
    millis: i64,
    node: &str,
    node_type: NodeType,
    policy: crate::rollup::RollupPolicy,
) -> ConfigEntry {
    ConfigEntry {
        kind: ConfigKind::RollupPolicy,
        id: id.into(),
        hlc: Hlc::new(millis, node),
        document: Some(serde_json::to_string(&RollupPolicyDoc { node_type, policy }).unwrap()),
    }
}
// ---- Placement-scoped configuration, Phase 2 (SCALING.md) ----

pub(crate) fn placed_check(name: &str, regions: &[&str]) -> CheckConfig {
    CheckConfig {
        placement: crate::placement::Placement {
            regions: regions.iter().map(|r| r.to_string()).collect(),
            ..Default::default()
        },
        ..check_config(name, "https://example.com")
    }
}
// ---- Clustering: incidents ----

pub(crate) fn sample_replicated_incident() -> ReplicatedIncident {
    ReplicatedIncident {
        id: "inc-1".into(),
        version: Hlc::new(100, "a"),
        title: "Outage".into(),
        state: IncidentState::Detected,
        started_at: 100,
        resolved_at: None,
        global: false,
        origin_check: Some(cid("a")),
        checks: vec![(cid("a"), AffectSource::Origin)],
        updates: vec![IncidentUpdateRow {
            id: "inc-1-u1".into(),
            incident_id: "inc-1".into(),
            state: IncidentState::Detected,
            body: "down".into(),
            author: None,
            created_at: 100,
        }],
    }
}
// ---- Clustering: fleet health ----

pub(crate) fn poll(
    agent_id: &str,
    zone: &str,
    region: &str,
    at: i64,
    window_ms: i64,
) -> AgentPollRow {
    AgentPollRow {
        agent_id: agent_id.into(),
        zone: zone.into(),
        region: region.into(),
        at,
        window_ms,
        rtt_ms: None,
        build: None,
        kinds: None,
        coordinator: None,
    }
}
// ---- Incident deletion ----

/// The incident a delete test needs: one row, one update, one affects
/// entry and one alert stamped with its id.
pub(crate) fn seeded_incident(s: &Storage, id: &str, check: &str) {
    s.create_incident(
        id,
        "Outage",
        IncidentState::Detected,
        1000,
        false,
        Some(cid(check)),
        &[IncidentCheckRow {
            check_id: cid(check),
            source: AffectSource::Origin,
        }],
        Some(("down", None)),
    )
    .unwrap();
    s.upsert_alert(&AlertRow {
        id: format!("{id}-a1"),
        scope: AlertScope::Check,
        node_id: check.into(),
        previous_state: Some(NodeState::Up),
        current_state: NodeState::Down,
        timestamp: 1000,
        incident_id: Some(id.into()),
    })
    .unwrap();
}
pub(crate) fn up_at(check_id: &str, ts_millis: i64) -> (CheckResult, Status) {
    (
        CheckResult {
            zone: "test-zone".into(),
            region: "test-region".into(),
            body: None,
            headers: None,
            check_id: cid(check_id),
            agent_id: "agent".into(),
            kind: CheckKind::Http,
            timestamp: DateTime::from_timestamp_millis(ts_millis).unwrap(),
            latency_ms: 10,
            status_code: Some(200),
            body_matched: None,
            error: None,
            failure: None,
            metric: None,
            metrics: None,
        },
        Status::Up,
    )
}
/// Helper: open an incident the way the coordinator does, with the check as
/// its origin and an opening update.
pub(crate) fn open_incident(s: &Storage, check: &str, at: i64) -> IncidentRow {
    s.create_incident(
        &format!("{check}-{at}"),
        &format!("{check} is down"),
        IncidentState::Detected,
        at,
        false,
        Some(cid(check)),
        &[IncidentCheckRow {
            check_id: cid(check),
            source: AffectSource::Origin,
        }],
        Some(("connection refused", None)),
    )
    .unwrap()
}
pub(crate) fn row(
    check_id: &str,
    agent_id: &str,
    bucket_start: i64,
    attempts: i64,
    up: i64,
    latency_sum: i64,
) -> RollupRow {
    RollupRow {
        check_id: cid(check_id),
        agent_id: agent_id.into(),
        bucket_start,
        attempts,
        up,
        latency_sum,
        latency_min: Some(10),
        latency_max: Some(10),
    }
}
