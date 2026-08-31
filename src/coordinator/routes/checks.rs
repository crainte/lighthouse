//! The check editor: creating, editing, deleting, reordering, and probing a
//! check definition by hand.
//!
//! **A check definition is authoritative as a JSON document, not a set of
//! columns.** `write_config` persists the whole `CheckConfig` and stamps it
//! for replication in one step, so there is no partial-field update to get
//! half-applied on this node or half-propagated to a peer. That is also why
//! `normalise` — trimming form input and dropping blanks — lives next to the
//! handlers that accept an edit rather than in `storage` or in `CheckConfig`
//! itself: it is about what makes a submitted document fit to save, the same
//! job `validate` does immediately after.
//!
//! **Renaming is refused, not merged.** `update_check_handler` rejects a body
//! whose name disagrees with the path, because the name is the identity a
//! check's history, alerts, and incidents are all keyed on — silently
//! adopting a new one would orphan all three.
//!
//! **Erasing is a different action from deleting.**
//! `purge_check_everything_handler` removes a check's definition and every
//! trace it left, including pages an anonymous visitor may already have
//! read; `purge_check_data_handler` keeps the definition and only resets its
//! history. Conflating the two behind one button would make "start the graph
//! over" as destructive as "stop monitoring this", so they stay separate
//! endpoints with separate names.

use super::super::{
    error_response, internal_error, storage, Coordinator, DeletedResponse, WsEvent,
};
use super::session::require_admin;
use crate::config::CheckConfig;
use crate::models::CheckResult;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Trim the strings a form submits and drop the blanks, so an untouched field
/// means "unset" rather than a group literally named "" or an agent id of "".
fn normalise(mut c: CheckConfig) -> CheckConfig {
    let blank_to_none = |s: Option<String>| s.filter(|v: &String| !v.trim().is_empty());
    c.name = c.name.trim().to_string();
    c.target = c.target.trim().to_string();
    c.cron = c.cron.trim().to_string();
    c.group = blank_to_none(c.group);
    c.eval = blank_to_none(c.eval);
    c.description = blank_to_none(c.description);
    c.image = blank_to_none(c.image);
    c.tag = blank_to_none(c.tag);
    c.expect.body_contains = blank_to_none(c.expect.body_contains);
    c.dns.nameserver = blank_to_none(c.dns.nameserver);
    c.hosts = c
        .hosts
        .into_iter()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .collect();
    c.agents = c
        .agents
        .into_iter()
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty())
        .collect();
    c.dns.expected = c
        .dns
        .expected
        .into_iter()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    c.headers.retain(|h| !h.name.trim().is_empty());
    c
}

/// The admin wire type is `CheckConfig` itself. A parallel DTO duplicated
/// twenty-odd fields and two conversions for no gain — and a field added to one
/// and forgotten in the other is exactly the drift that would silently drop a
/// setting on save.
///
/// It carries the target, so it must only ever be produced behind authentication.
#[derive(Serialize)]
struct AdminChecksResponse {
    checks: Vec<CheckConfig>,
}

#[derive(Serialize)]
pub(in crate::coordinator) struct CheckResponse {
    pub(in crate::coordinator) check: CheckConfig,
}

pub(in crate::coordinator) async fn list_checks_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    match coord.inner.storage.lock().list_checks() {
        Ok(checks) => Json(AdminChecksResponse { checks }).into_response(),
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn create_check_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<CheckConfig>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let mut check = normalise(body);
    if let Err(reason) = check.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    {
        let store = coord.inner.storage.lock();
        let existing = match store.list_checks() {
            Ok(existing) => existing,
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
        match store.next_check_position() {
            Ok(p) => check.position = p,
            Err(e) => return internal_error(e),
        }
    }
    coord.save_check(check)
}

pub(in crate::coordinator) async fn update_check_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<CheckConfig>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let check = normalise(body);
    // The name is the identity. Renaming via PUT would orphan the check's
    // history, so the path wins and a mismatched body is a mistake worth saying.
    if check.name != name {
        return error_response(StatusCode::BAD_REQUEST, "a check cannot be renamed");
    }
    if let Err(reason) = check.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    coord.save_check(check)
}

pub(in crate::coordinator) async fn delete_check_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    // A tombstone, not a bare row delete: see `Storage::apply_config_entry`
    // on why an absence alone is not enough for a peer to tell "deleted"
    // apart from "never heard of it".
    if !coord.inner.checks.read().contains_key(&name) {
        return error_response(StatusCode::NOT_FOUND, "no such check");
    }
    let entry = match coord.write_config(storage::ConfigKind::Check, &name, None) {
        Ok(e) => e,
        Err(e) => return internal_error(e),
    };
    coord.replicate_config(vec![entry]);
    info!(check = %name, "check deleted");
    Json(DeletedResponse { deleted: name }).into_response()
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
    order: Vec<String>,
}

pub(in crate::coordinator) async fn reorder_checks_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<CheckOrderRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let before_pos: std::collections::HashMap<String, i32> = coord
        .inner
        .checks
        .read()
        .iter()
        .map(|(name, m)| (name.clone(), m.position))
        .collect();
    if let Err(e) = coord
        .inner
        .storage
        .lock()
        .reorder_checks(&body.order, chrono::Utc::now().timestamp_millis())
    {
        return internal_error(e);
    }
    let checks = match coord.reload_checks() {
        Ok(checks) => checks,
        Err(e) => return internal_error(e),
    };
    // Only the checks whose position actually moved are stamped and pushed —
    // the ones the reorder rewrote are the ones a peer needs to hear about,
    // not the whole table.
    for c in &checks {
        if before_pos.get(&c.name) != Some(&c.position) {
            coord.restamp_and_push(storage::ConfigKind::Check, &c.name);
        }
    }
    Json(AdminChecksResponse { checks }).into_response()
}

/// Record a heartbeat ping. The URL is the credential, so a wrong name is a
/// plain 404 rather than an error that confirms which names exist.
pub(in crate::coordinator) async fn heartbeat_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
) -> Response {
    let is_heartbeat = coord
        .inner
        .checks
        .read()
        .get(&name)
        .is_some_and(|m| m.kind == crate::models::CheckKind::Heartbeat);
    if !is_heartbeat {
        return error_response(StatusCode::NOT_FOUND, "no such heartbeat");
    }
    coord
        .ingest(vec![CheckResult {
            zone: crate::config::UNKNOWN_LOCATION.into(),
            region: crate::config::UNKNOWN_LOCATION.into(),
            check_id: name.clone(),
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
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    if !coord.inner.checks.read().contains_key(&name) {
        return error_response(StatusCode::NOT_FOUND, "no such check");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let node_id = coord.inner.node_id.clone();
    let summary = {
        let store = coord.inner.storage.lock();
        match store.purge_check_everything(&name, now, &node_id) {
            Ok(s) => s,
            Err(e) => return internal_error(e),
        }
    };
    let entry = match coord.write_config(storage::ConfigKind::Check, &name, None) {
        Ok(e) => e,
        Err(e) => return internal_error(e),
    };
    coord.replicate_config(vec![entry]);
    coord.replicate_incident_deletions(summary.deleted_incidents.clone());
    for id in &summary.deleted_incidents {
        let _ = coord
            .inner
            .events
            .send(WsEvent::IncidentDeleted { id: id.clone() });
    }
    info!(
        check = %name,
        results = summary.results,
        incidents = summary.incidents,
        "check erased"
    );
    Json(ErasedResponse {
        deleted: name,
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
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    match coord.inner.storage.lock().purge_check_data(&name) {
        Ok(rows) => {
            info!(check = %name, rows, "purged check history");
            Json(PurgedResponse { purged: rows }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

#[derive(Serialize)]
pub(in crate::coordinator) struct PurgedResponse {
    pub(in crate::coordinator) purged: u64,
}
