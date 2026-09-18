//! Admin handlers for per-node rollup-policy overrides.
//!
//! Node ids are namespaced structural keys rather than user-facing labels:
//! `global`, `region:<region>`, and `zone:<region>/<zone>`. Keeping the node
//! type beside the key in the request lets us reject a policy that would never
//! be selected by the rollup tree.

use super::super::{error_response, internal_error, storage, Coordinator, DeletedResponse};
use super::session::require_admin;
use crate::models::NodeState;
use crate::rollup::RollupPolicy;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::info;

/// A policy override for one rollup-tree node.
///
/// This is both the upsert request and the item returned by the list endpoint;
/// there is no separate database-shaped DTO with replication metadata that an
/// admin cannot edit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::coordinator) struct RollupPolicyDto {
    pub(in crate::coordinator) node_id: String,
    pub(in crate::coordinator) node_type: storage::NodeType,
    pub(in crate::coordinator) policy: RollupPolicy,
}

#[derive(Serialize)]
struct RollupPoliciesResponse {
    policies: Vec<RollupPolicyDto>,
}

/// The identity carried by a delete request. A body is deliberate: zone node
/// ids contain `/`, which is part of their identity and should not be split by
/// Axum path routing.
#[derive(Debug, Deserialize)]
pub(in crate::coordinator) struct RollupPolicyDeleteRequest {
    pub(in crate::coordinator) node_id: String,
    pub(in crate::coordinator) node_type: storage::NodeType,
}

impl From<storage::RollupPolicyRow> for RollupPolicyDto {
    fn from(row: storage::RollupPolicyRow) -> Self {
        Self {
            node_id: row.node_id,
            node_type: row.node_type,
            policy: row.policy,
        }
    }
}

#[derive(Debug, Serialize)]
struct RegionTreeResponse {
    checks: Vec<CheckTreeDto>,
}

#[derive(Debug, Serialize)]
struct CheckTreeDto {
    check_id: crate::models::CheckId,
    name: String,
    state: NodeState,
    effective_policy: RollupPolicy,
    regions: Vec<RegionDto>,
}

#[derive(Debug, Serialize)]
struct RegionDto {
    region: String,
    state: NodeState,
    effective_policy: RollupPolicy,
    zones: Vec<ZoneDto>,
}

#[derive(Debug, Serialize)]
struct ZoneDto {
    zone: String,
    state: NodeState,
    effective_policy: RollupPolicy,
    agents: Vec<AgentStateDto>,
}

#[derive(Debug, Serialize)]
struct AgentStateDto {
    id: String,
    state: NodeState,
    staleness_ms: i64,
}

/// Snapshot the effective credential-authoritative rollup tree for every check.
pub(in crate::coordinator) async fn region_tree_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }

    let now = chrono::Utc::now().timestamp_millis();
    let checks = coord.inner.checks.read().clone();
    let mut check_ids: Vec<_> = checks.keys().cloned().collect();
    check_ids.sort();
    let snapshots = check_ids
        .into_iter()
        .map(|check_id| {
            let leaves = coord.inner.tracker.lock().children(check_id);
            let meta = checks.get(&check_id);
            let tree = coord.rollup_tree(meta, leaves, now);
            CheckTreeDto {
                check_id,
                name: meta.map_or_else(|| check_id.to_string(), |m| m.name.clone()),
                state: tree.root,
                effective_policy: tree.global_policy,
                regions: tree
                    .regions
                    .into_iter()
                    .map(|region| RegionDto {
                        region: region.name,
                        state: region.state,
                        effective_policy: region.policy,
                        zones: region
                            .zones
                            .into_iter()
                            .map(|zone| ZoneDto {
                                zone: zone.name,
                                state: zone.state,
                                effective_policy: zone.policy,
                                agents: zone
                                    .agents
                                    .into_iter()
                                    .map(|agent| AgentStateDto {
                                        id: agent.id,
                                        state: agent.state,
                                        staleness_ms: agent.staleness_ms,
                                    })
                                    .collect(),
                            })
                            .collect(),
                    })
                    .collect(),
            }
        })
        .collect();
    Json(RegionTreeResponse { checks: snapshots }).into_response()
}

/// Ensure a node id names precisely the node type the client claims to edit.
fn validate_node_id(node_id: &str, node_type: storage::NodeType) -> Result<(), &'static str> {
    if node_id != node_id.trim() {
        return Err("node_id must not have leading or trailing whitespace");
    }

    match node_type {
        storage::NodeType::Global if node_id == "global" => Ok(()),
        storage::NodeType::Global => Err("a global policy must use node_id \"global\""),
        storage::NodeType::Region => match node_id.strip_prefix("region:") {
            Some(region) if !region.is_empty() && !region.contains('/') => Ok(()),
            _ => Err("a region policy node_id must be \"region:<region>\""),
        },
        storage::NodeType::Zone => match node_id.strip_prefix("zone:") {
            Some(location) => match location.split_once('/') {
                Some((region, zone))
                    if !region.is_empty() && !zone.is_empty() && !zone.contains('/') =>
                {
                    Ok(())
                }
                _ => Err("a zone policy node_id must be \"zone:<region>/<zone>\""),
            },
            None => Err("a zone policy node_id must be \"zone:<region>/<zone>\""),
        },
    }
}

pub(in crate::coordinator) async fn list_rollup_policies_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }

    match coord.db(|storage| storage.list_rollup_policies()).await {
        Ok(rows) => Json(RollupPoliciesResponse {
            policies: rows.into_iter().map(Into::into).collect(),
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn upsert_rollup_policy_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<RollupPolicyDto>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    if let Err(reason) = validate_node_id(&body.node_id, body.node_type) {
        return error_response(StatusCode::BAD_REQUEST, reason);
    }
    if let Err(reason) = body.policy.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    if let Err(e) = coord
        .save_rollup_policy(&body.node_id, body.node_type, body.policy)
        .await
    {
        return internal_error(e);
    }

    info!(node_id = %body.node_id, node_type = body.node_type.as_str(), "rollup policy saved");
    Json(body).into_response()
}

pub(in crate::coordinator) async fn delete_rollup_policy_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<RollupPolicyDeleteRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    if let Err(reason) = validate_node_id(&body.node_id, body.node_type) {
        return error_response(StatusCode::BAD_REQUEST, reason);
    }

    let stored_type = match coord.db(|storage| storage.list_rollup_policies()).await {
        Ok(rows) => rows
            .into_iter()
            .find(|row| row.node_id == body.node_id)
            .map(|row| row.node_type),
        Err(e) => return internal_error(e),
    };
    match stored_type {
        None => return error_response(StatusCode::NOT_FOUND, "no such rollup policy"),
        Some(stored_type) if stored_type != body.node_type => {
            return error_response(StatusCode::BAD_REQUEST, "node_id does not match node_type")
        }
        Some(_) => {}
    }
    if let Err(e) = coord.delete_rollup_policy(&body.node_id).await {
        return internal_error(e);
    }

    info!(node_id = %body.node_id, "rollup policy deleted");
    Json(DeletedResponse {
        deleted: body.node_id,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_must_match_its_type() {
        assert!(validate_node_id("global", storage::NodeType::Global).is_ok());
        assert!(validate_node_id("region:eu", storage::NodeType::Region).is_ok());
        assert!(validate_node_id("zone:eu/a", storage::NodeType::Zone).is_ok());
        assert!(validate_node_id("region:eu", storage::NodeType::Zone).is_err());
        assert!(validate_node_id("zone:eu/a/extra", storage::NodeType::Zone).is_err());
        assert!(validate_node_id("zone:eu/a", storage::NodeType::Region).is_err());
        assert!(validate_node_id("global", storage::NodeType::Region).is_err());
    }

    #[test]
    fn region_tree_wire_shape_contains_no_credentials() {
        let response = RegionTreeResponse {
            checks: vec![CheckTreeDto {
                check_id: crate::models::CheckId::from_u128(1),
                name: "api".into(),
                state: NodeState::Unknown,
                effective_policy: RollupPolicy::CHECK_DEFAULT,
                regions: vec![RegionDto {
                    region: "eu".into(),
                    state: NodeState::Unknown,
                    effective_policy: RollupPolicy::REGION_DEFAULT,
                    zones: vec![ZoneDto {
                        zone: "a".into(),
                        state: NodeState::Unknown,
                        effective_policy: RollupPolicy::ZONE_DEFAULT,
                        agents: Vec::new(),
                    }],
                }],
            }],
        };
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(
            json["checks"][0]["check_id"],
            crate::models::CheckId::from_u128(1).to_string()
        );
        assert_eq!(json["checks"][0]["name"], "api");
        assert_eq!(
            json["checks"][0]["regions"][0]["zones"][0]["state"],
            "unknown"
        );
        let encoded = serde_json::to_string(&json).unwrap();
        assert!(!encoded.contains("token"));
        assert!(!encoded.contains("secret"));
        assert!(!encoded.contains("hash"));
    }
}
