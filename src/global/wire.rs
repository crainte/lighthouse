//! The wire format between a regional coordinator and the global coordinator.
//!
//! One direction only: a coordinator POSTs a [`RegionStatePush`] to the global
//! node every few seconds. The payload is a *summary* — current per-check state,
//! nothing raw — because the global node stores no results (see the design
//! document, §3 Global Coordinator: "No raw data storage"). Keeping the DTOs in
//! their own module lets both the sender (`coordinator`) and the receiver
//! (`global`) depend on the same shapes without either reaching into the other.

use crate::models::{CheckId, CheckKind, NodeState};
use serde::{Deserialize, Serialize};

/// A region's whole current state, as one coordinator sees it.
///
/// Every coordinator in a region cluster holds the same replicated state, so
/// any of them can push and the global node keeps the most recent by
/// `pushed_at` (last-writer-wins, `origin` breaking a rare tie). That is why a
/// three-node region does not need to elect a pusher: three identical pushes
/// collapse to one cached entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStatePush {
    /// Which region these checks belong to.
    pub region: String,
    /// node_id of the coordinator that produced this push. Tie-breaks equal
    /// `pushed_at` and lets the global node name who last spoke for a region.
    pub origin: String,
    /// Where the origin coordinator can be reached, so the global node learns
    /// a live endpoint to route drill-down reads back to without config.
    pub advertise_url: Option<String>,
    /// Wall-clock millis at the sender when the summary was taken.
    pub pushed_at: i64,
    /// The region-wide rolled-up health across all its checks. Precomputed by
    /// the sender so the global node need not know the region's topology.
    pub region_health: NodeState,
    /// How many probe agents the region currently counts as healthy, and how
    /// many it knows of. Lets the global status page show a truthful "us 3/3"
    /// instead of pretending a region is a single agent. Defaulted for
    /// back-compat with a coordinator that predates the field.
    #[serde(default)]
    pub agents_healthy: u32,
    #[serde(default)]
    pub agents_total: u32,
    /// One entry per configured, visible check.
    pub checks: Vec<CheckSummary>,
}

/// A single check's current state within a region. Fields mirror the public
/// `CheckStatusDto` the dashboard already renders, so the global node can serve
/// an aggregated `/api/v1/status` without inventing a new frontend contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckSummary {
    /// Immutable identity. Stable across regions: a check folded into a
    /// global check keeps its id everywhere, which is what lets the global
    /// tier aggregate by identity instead of by mutable display name.
    pub check_id: CheckId,
    /// Mutable display name, carried alongside the id so the global page
    /// never has to render a UUID as a label.
    #[serde(default)]
    pub name: String,
    /// This region's rolled-up state for the check.
    pub state: NodeState,
    pub latency_ms: u64,
    pub last_check: i64,
    /// `None` when this region measured nothing in the window. Skipped on the
    /// wire when absent, so a node on an older build reads the field as missing
    /// and defaults it exactly as it did before this was optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uptime_24h: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<CheckKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default)]
    pub latency_graph: bool,
    #[serde(default)]
    pub position: i32,
}

/// The global node's acknowledgement of a push. Trivial today; a struct so the
/// wire format can carry back "and here is who else I know" for global HA later
/// without changing the sender's expectation of a JSON body.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PushAck {
    pub accepted: bool,
}
