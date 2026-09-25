//! The check editor: creating, editing, deleting, reordering, and probing a
//! check definition by hand.
//!
//! **A check definition is authoritative as a JSON document, not a set of
//! columns.** `write_config` persists the whole `CheckConfig` and stamps it
//! for replication in one step, so there is no partial-field update to get
//! half-applied on this node or half-propagated to a peer. Parsing a
//! submission and trimming it — `CheckConfig::from_submitted` and
//! `normalised` — live on `CheckConfig` itself rather than here, because the
//! global tier's own admin API (`global::store`) accepts the exact same JSON
//! shape from the exact same console and must parse it identically.
//!
//! **Identity is the path id, never the body.** `update_check_handler` always
//! overwrites `body.id` with the path parameter, so a client cannot move an
//! edit onto a different check by sending a different id. `name` is a mutable
//! display value — not identity — so renaming through the same PUT is exactly
//! what this allows; history, alerts and incidents are keyed on the immutable
//! id and are unaffected by it.
//!
//! **Unplacing, deleting, and erasing are three distinct actions.**
//! `unplace_check_handler` removes a definition only from this region without
//! writing or replicating a tombstone, preserving raw results under the same
//! immutable id for a later placement. `delete_check_handler` is the normal
//! replicated delete. `purge_check_everything_handler` removes a check and
//! every trace it left, while `purge_check_data_handler` keeps its definition
//! and only resets history. These operations stay separate because their
//! replication and retention consequences are deliberately different.

use super::super::router::{error_response, internal_error, DeletedResponse};
use super::super::{storage, Coordinator, WsEvent};
use super::session::{bearer, require_admin};
use crate::config::CheckConfig;
use crate::models::{CheckId, CheckResult};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

/// Whether the caller authenticated with a cross-node secret rather than an
/// admin session — the global tier relaying a write it already decided, or
/// another coordinator in this region.
fn is_peer_request(coord: &Coordinator, headers: &HeaderMap) -> bool {
    use subtle::ConstantTimeEq;
    let Some(got) = bearer(headers) else {
        return false;
    };
    [
        coord.inner.peer_token.as_deref(),
        coord.inner.admin_trust_token.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|expected| bool::from(got.as_bytes().ct_eq(expected.as_bytes())))
}

/// Parse a path id after auth has already been checked, never before.
/// `Path<CheckId>` would reject a non-UUID id at extraction time — before the
/// handler body, and so before `require_admin` ever runs — which would let
/// an anonymous caller learn "that wasn't even a valid id" (400) instead of
/// the uniform "authentication required" (401) every other admin route gives
/// them. Taking the id as a plain `String` and parsing it here keeps auth the
/// first thing every admin-gated handler decides.
fn parse_check_id(id: &str) -> Result<CheckId, Box<Response>> {
    id.parse()
        .map_err(|_| Box::new(error_response(StatusCode::NOT_FOUND, "no such check")))
}

#[derive(Serialize)]
struct AdminChecksResponse {
    checks: Vec<CheckConfig>,
    /// Checks in the catalogue, not in this page. The console's dependency
    /// pickers and section editors want the whole list, so they page until
    /// `has_more` is false rather than being handed all of it at once.
    total: usize,
    has_more: bool,
}

impl AdminChecksResponse {
    /// The whole catalogue, for the write paths that answer with the list they
    /// just rewrote. These are bounded by what an operator can edit in one
    /// request, not by how many checks exist.
    fn whole(checks: Vec<CheckConfig>) -> Self {
        Self {
            total: checks.len(),
            has_more: false,
            checks,
        }
    }
}

/// Paging for the admin catalogue. The default is the shared ceiling rather
/// than a screenful: this list is a lookup table for half the console, and a
/// small default would turn every consumer into a paging loop for the sake of
/// deployments that have no pages. The ceiling is the point — one request must
/// not be able to ask the single database worker for ten thousand documents.
#[derive(Deserialize)]
pub(in crate::coordinator) struct AdminChecksQuery {
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(Serialize)]
pub(in crate::coordinator) struct CheckResponse {
    pub(in crate::coordinator) check: CheckConfig,
}

pub(in crate::coordinator) async fn list_checks_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<AdminChecksQuery>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let (page, limit) = (
        q.page.unwrap_or(0),
        crate::page::bounded(q.limit, crate::page::MAX_PAGE_SIZE),
    );
    match coord.db(|storage| storage.list_checks()).await {
        Ok(mut checks) => {
            let (total, has_more) = crate::page::take(&mut checks, page, limit);
            Json(AdminChecksResponse {
                checks,
                total,
                has_more,
            })
            .into_response()
        }
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn create_check_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let trusted_global = is_peer_request(&coord, &headers);
    let mut check = match CheckConfig::from_submitted(body, !trusted_global) {
        Ok(check) => check.normalised(),
        Err(reason) => return error_response(StatusCode::BAD_REQUEST, &reason),
    };
    // Browser callers never choose identity. The global coordinator uses the
    // cluster credential and has already minted exactly one UUIDv7 before its
    // regional fan-out, so each region persists that same identity.
    if !trusted_global {
        check.id = crate::models::new_check_id();
    }
    if let Err(reason) = check.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    {
        let (existing, position) = match coord
            .db(|storage| Ok((storage.list_checks()?, storage.next_check_position()?)))
            .await
        {
            Ok(value) => value,
            Err(e) => return internal_error(e),
        };
        if existing.iter().any(|c| c.name == check.name) {
            return error_response(
                StatusCode::CONFLICT,
                &format!("a check named \"{}\" already exists", check.name),
            );
        }
        // Land at the end rather than at 0, so adding a check never silently
        // reshuffles the status page.
        check.position = position;
    }
    coord.save_check(check).await
}

pub(in crate::coordinator) async fn update_check_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let id = match parse_check_id(&id) {
        Ok(id) => id,
        Err(denied) => return *denied,
    };
    let mut check = match CheckConfig::from_submitted(body, false) {
        Ok(check) => check.normalised(),
        Err(reason) => return error_response(StatusCode::BAD_REQUEST, &reason),
    };
    // The id is the identity, never the body's. A client cannot move an edit
    // onto a different check by sending a different id than the path names,
    // and a rename is exactly what this allows: `name` is a mutable display
    // value, so changing it here is not "a different check", unlike a
    // mismatched id would be.
    check.id = id;
    if let Err(reason) = check.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    {
        let existing = match coord.db(|storage| storage.list_checks()).await {
            Ok(existing) => existing,
            Err(e) => return internal_error(e),
        };
        if !existing.iter().any(|c| c.id == id) {
            return error_response(StatusCode::NOT_FOUND, "no such check");
        }
        if existing.iter().any(|c| c.id != id && c.name == check.name) {
            return error_response(
                StatusCode::CONFLICT,
                &format!("a check named \"{}\" already exists", check.name),
            );
        }
    }
    coord.save_check(check).await
}

/// Remove this check from this region only.
///
/// This deliberately does not use `write_config`: that path creates a
/// replication tombstone, which would delete the definition in regions where
/// the check remains placed. The coordinator operation removes the local
/// definition and derived state but leaves `check_results` untouched, allowing
/// the same immutable id to reconnect its history when it is placed here again.
pub(in crate::coordinator) async fn unplace_check_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let id = match parse_check_id(&id) {
        Ok(id) => id,
        Err(denied) => return *denied,
    };
    match coord.unplace_check_local(id).await {
        Ok(true) => {
            let id = id.to_string();
            info!(check = %id, "check unplaced locally");
            Json(UnplacedResponse { unplaced: id }).into_response()
        }
        Ok(false) => error_response(StatusCode::NOT_FOUND, "no such check"),
        Err(e) => internal_error(e),
    }
}

#[derive(Serialize)]
pub(in crate::coordinator) struct UnplacedResponse {
    pub(in crate::coordinator) unplaced: String,
}

pub(in crate::coordinator) async fn delete_check_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let id = match parse_check_id(&id) {
        Ok(id) => id,
        Err(denied) => return *denied,
    };
    // A tombstone, not a bare row delete: see `Storage::apply_config_entry`
    // on why an absence alone is not enough for a peer to tell "deleted"
    // apart from "never heard of it".
    if !coord.inner.checks.read().contains_key(&id) {
        return error_response(StatusCode::NOT_FOUND, "no such check");
    }
    let id_str = id.to_string();
    let entry = match coord
        .write_config(storage::ConfigKind::Check, &id_str, None)
        .await
    {
        Ok(e) => e,
        Err(e) => return internal_error(e),
    };
    coord.replicate_config(vec![entry]);
    info!(check = %id, "check deleted");
    Json(DeletedResponse { deleted: id_str }).into_response()
}

// ---- Script try-it ----

#[derive(Deserialize)]
pub(in crate::coordinator) struct EvalTestRequest {
    #[serde(rename = "eval")]
    source: String,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    latency: u64,
    #[serde(default)]
    body: Option<String>,
    /// OpenMetrics eval testing: the selected sample value the script sees as
    /// `metric`, plus the same-scrape samples it sees as `metrics`. Absent for
    /// HTTP-style scripts, so existing eval-test requests are unaffected.
    #[serde(default)]
    metric: Option<f64>,
    #[serde(default)]
    metrics: Vec<crate::openmetrics::MetricSample>,
}

#[derive(Serialize)]
struct EvalTestResponse {
    verdict: &'static str,
    latency_ms: u64,
}

pub(in crate::coordinator) async fn eval_test_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<EvalTestRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let input = crate::script::EvalInput {
        status: body.status,
        latency_ms: body.latency,
        body: body.body,
        headers: Default::default(),
        metric: body.metric,
        metrics: body.metrics,
    };
    match crate::script::evaluate(&body.source, &input) {
        Ok(outcome) => Json(EvalTestResponse {
            verdict: if outcome.up { "up" } else { "down" },
            latency_ms: outcome.latency_ms.unwrap_or(body.latency),
        })
        .into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, &e),
    }
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct CheckOrderRequest {
    order: Vec<CheckId>,
}

pub(in crate::coordinator) async fn reorder_checks_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<CheckOrderRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let before_pos: std::collections::HashMap<CheckId, i32> = coord
        .inner
        .checks
        .read()
        .iter()
        .map(|(id, m)| (*id, m.position))
        .collect();
    let order = body.order;
    if let Err(e) = coord
        .db(move |storage| storage.reorder_checks(&order, chrono::Utc::now().timestamp_millis()))
        .await
    {
        return internal_error(e);
    }
    let checks = match coord.reload_checks().await {
        Ok(checks) => checks,
        Err(e) => return internal_error(e),
    };
    // Only the checks whose position actually moved are stamped and pushed —
    // the ones the reorder rewrote are the ones a peer needs to hear about,
    // not the whole table.
    for c in &checks {
        if before_pos.get(&c.id) != Some(&c.position) {
            coord
                .restamp_and_push(storage::ConfigKind::Check, &c.id.to_string())
                .await;
        }
    }
    Json(AdminChecksResponse::whole(checks)).into_response()
}

/// Record a heartbeat ping. The immutable UUID keeps this URL stable when the
/// operator renames the check; an unknown id — including one that was never
/// even a valid UUID — remains a plain 404, not a hint that it was malformed.
pub(in crate::coordinator) async fn heartbeat_handler(
    State(coord): State<Coordinator>,
    Path(check_id): Path<String>,
) -> Response {
    let Ok(check_id) = check_id.parse::<CheckId>() else {
        return error_response(StatusCode::NOT_FOUND, "no such heartbeat");
    };
    let is_heartbeat = coord
        .inner
        .checks
        .read()
        .get(&check_id)
        .is_some_and(|m| m.kind == crate::models::CheckKind::Heartbeat);
    if !is_heartbeat {
        return error_response(StatusCode::NOT_FOUND, "no such heartbeat");
    }
    coord
        .ingest(vec![CheckResult {
            zone: crate::config::UNKNOWN_LOCATION.into(),
            region: crate::config::UNKNOWN_LOCATION.into(),
            check_id,
            agent_id: crate::models::HEARTBEAT_AGENT.into(),
            kind: crate::models::CheckKind::Heartbeat,
            timestamp: chrono::Utc::now(),
            latency_ms: 0,
            status_code: None,
            body_matched: None,
            error: None,
            failure: None,
            body: None,
            headers: None,
            metric: None,
            metrics: None,
        }])
        .await;
    (StatusCode::OK, "ok").into_response()
}

/// Erase a check and everything it ever produced: its definition, its results,
/// its alerts, and every incident it opened.
///
/// A third action rather than a flag on the delete, because it is the only one
/// that removes a page anonymous visitors have already read. The check goes
/// through the same config-tombstone path as a plain delete — an absence alone
/// does not survive a peer's next anti-entropy sweep — and each incident gets
/// its own tombstone for the same reason.
pub(in crate::coordinator) async fn purge_check_everything_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let id = match parse_check_id(&id) {
        Ok(id) => id,
        Err(denied) => return *denied,
    };
    if !coord.inner.checks.read().contains_key(&id) {
        return error_response(StatusCode::NOT_FOUND, "no such check");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let node_id = coord.inner.node_id.clone();
    let summary = match coord
        .db(move |storage| storage.purge_check_everything(id, now, &node_id))
        .await
    {
        Ok(summary) => summary,
        Err(e) => return internal_error(e),
    };
    let id_str = id.to_string();
    let entry = match coord
        .write_config(storage::ConfigKind::Check, &id_str, None)
        .await
    {
        Ok(e) => e,
        Err(e) => return internal_error(e),
    };
    coord.replicate_config(vec![entry]);
    coord.replicate_incident_deletions(summary.deleted_incidents.clone());
    for incident_id in &summary.deleted_incidents {
        let _ = coord.inner.events.send(WsEvent::IncidentDeleted {
            id: incident_id.clone(),
        });
    }
    info!(
        check = %id,
        results = summary.results,
        incidents = summary.incidents,
        "check erased"
    );
    Json(ErasedResponse {
        deleted: id_str,
        results: summary.results,
        incidents: summary.incidents,
    })
    .into_response()
}

#[derive(Serialize)]
struct ErasedResponse {
    deleted: String,
    results: u64,
    incidents: u64,
}

/// Drop a check's recorded history while keeping its configuration. Separate
/// from deleting the check: "start the graph over" and "stop monitoring this"
/// are different intentions and a single button would conflate them.
pub(in crate::coordinator) async fn purge_check_data_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let id = match parse_check_id(&id) {
        Ok(id) => id,
        Err(denied) => return *denied,
    };
    match coord.db(move |storage| storage.purge_check_data(id)).await {
        Ok(rows) => {
            info!(check = %id, rows, "purged check history");
            Json(PurgedResponse { purged: rows }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

#[derive(Serialize)]
pub(in crate::coordinator) struct PurgedResponse {
    pub(in crate::coordinator) purged: u64,
}
