//! Incident reads and writes, in one place, because they read and write the
//! same shape.
//!
//! **One DTO family serves both directions.** `IncidentDto` is what the
//! public timeline renders and what an admin write echoes straight back
//! after mutating a row — `IncidentDto::from_row` is the one function that
//! turns a storage row into either. Splitting public reads from admin writes
//! would either duplicate that shape in two modules or force one of them to
//! reach across a boundary for it, so the two live together instead.
//!
//! **Public narrative, gated edits.** Anyone can read an incident and its
//! whole update history — the story is the point of writing one down — but
//! opening, editing, retitling, attaching a check, or posting an update all
//! require an admin session. Each write handler checks `require_admin` for
//! itself, the same way the check editor does, rather than the router gating
//! a whole path prefix.
//!
//! **An incident's id belongs to whichever node opened it.** Local
//! confirmation mints an id once; gossip, `/internal/replicate`, and
//! anti-entropy all carry that same id to every peer rather than letting each
//! one mint its own. The handlers here never mint an id on a peer's behalf —
//! `create_incident_handler` mints one only for an incident opened by hand,
//! on the node the admin happened to be talking to.

use super::super::{
    error_response, internal_error, require_admin, storage, Coordinator, DeletedResponse, WsEvent,
};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::info;

/// A check attached to an incident, as the UI reads it.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentCheckDto {
    pub check_id: String,
    pub source: &'static str,
}

/// One entry in an incident's narrative.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentUpdateDto {
    pub id: String,
    pub state: &'static str,
    pub body: String,
    pub author: Option<String>,
    pub created_at: i64,
}

impl From<storage::IncidentUpdateRow> for IncidentUpdateDto {
    fn from(u: storage::IncidentUpdateRow) -> Self {
        Self {
            id: u.id,
            state: u.state.as_str(),
            body: u.body,
            author: u.author,
            created_at: u.created_at,
        }
    }
}

/// The shape of an incident as the UI reads it. Also carried inside WS events.
///
/// `duration_ms` and `acknowledged` are computed here rather than stored:
/// duration is a subtraction, and an acknowledgement is the existence of an
/// authored update. Sending them saves every caller from re-deriving the same
/// two facts, without either becoming a column that can contradict its source.
#[derive(Debug, Clone, Serialize)]
pub struct IncidentDto {
    pub id: String,
    pub title: String,
    pub state: &'static str,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
    pub duration_ms: i64,
    pub global: bool,
    pub origin_check: Option<String>,
    pub checks: Vec<IncidentCheckDto>,
    pub acknowledged: bool,
    /// Present on the single-incident read; omitted from list responses, which
    /// would otherwise carry every narrative on the page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updates: Option<Vec<IncidentUpdateDto>>,
}

impl IncidentDto {
    /// Build the list shape: no narrative, acknowledgement resolved by the
    /// caller (which holds the storage lock and can answer it cheaply).
    pub(in crate::coordinator) fn from_row(
        i: storage::IncidentRow,
        now: i64,
        acknowledged: bool,
    ) -> Self {
        Self {
            duration_ms: i.duration_ms(now),
            id: i.id,
            title: i.title,
            state: i.state.as_str(),
            started_at: i.started_at,
            resolved_at: i.resolved_at,
            global: i.global,
            origin_check: i.origin_check,
            checks: i
                .checks
                .into_iter()
                .map(|c| IncidentCheckDto {
                    check_id: c.check_id,
                    source: c.source.as_str(),
                })
                .collect(),
            acknowledged,
            updates: None,
        }
    }
}

#[derive(Serialize)]
struct IncidentsResponse {
    incidents: Vec<IncidentDto>,
}

/// Query for the incidents endpoints. `status` mirrors the incident's
/// lifecycle, not a check's UP/DOWN/UNKNOWN: `?status=open` lists only current
/// outages, which is any state other than resolved.
#[derive(Deserialize)]
pub(in crate::coordinator) struct IncidentQuery {
    limit: Option<usize>,
    status: Option<String>,
}

/// Open an incident by hand. Everything is optional but the title: a
/// maintenance notice needs no affected checks and no start time.
#[derive(Deserialize)]
pub(in crate::coordinator) struct CreateIncidentRequest {
    title: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    global: Option<bool>,
    #[serde(default)]
    checks: Vec<String>,
    /// The opening update's body.
    #[serde(default)]
    body: Option<String>,
}

/// Edit the operator-owned fields. No `state`: that moves only by posting an
/// update.
#[derive(Deserialize)]
pub(in crate::coordinator) struct EditIncidentRequest {
    title: String,
    started_at: i64,
    global: bool,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct IncidentChecksRequest {
    checks: Vec<String>,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct IncidentUpdateRequest {
    state: String,
    #[serde(default)]
    body: String,
}

/// Parse the `status` query. `open` means anything not resolved, since state is
/// the only lifecycle field; anything else that is not exactly `resolved` means
/// "both".
fn incident_open_filter(raw: Option<&str>) -> Option<bool> {
    match raw {
        Some("open") => Some(true),
        Some("resolved") => Some(false),
        _ => None,
    }
}

/// Recent incidents, newest first. Public like `/api/v1/alerts`: the status
/// page renders the open ones as current outages.
pub(in crate::coordinator) async fn incidents_handler(
    State(coord): State<Coordinator>,
    Query(q): Query<IncidentQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).min(1000);
    let open_only = incident_open_filter(q.status.as_deref());
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    match store.list_incidents(limit, open_only) {
        Ok(rows) => {
            let incidents = rows
                .into_iter()
                .map(|r| {
                    let ack = store.is_acknowledged(&r.id).unwrap_or(false);
                    IncidentDto::from_row(r, now, ack)
                })
                .collect();
            Json(IncidentsResponse { incidents }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// One incident with its full narrative. Public: the update timeline is the
/// entire point of writing it, so the permalink an anonymous visitor follows
/// serves the same record the operator sees.
pub(in crate::coordinator) async fn incident_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    match store.incident(&id) {
        Ok(Some(row)) => {
            let ack = store.is_acknowledged(&row.id).unwrap_or(false);
            let updates = store.updates_for_incident(&row.id).unwrap_or_default();
            let mut dto = IncidentDto::from_row(row, now, ack);
            dto.updates = Some(updates.into_iter().map(IncidentUpdateDto::from).collect());
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no incident with that id"),
        Err(e) => internal_error(e),
    }
}

/// Open an incident by hand — a maintenance notice, or something no probe can
/// see. `origin_check` is left unset, which is what keeps reconciliation away
/// from it: nothing auto-attaches and nothing auto-resolves.
pub(in crate::coordinator) async fn create_incident_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(req): Json<CreateIncidentRequest>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let title = req.title.trim();
    if title.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "title is required");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let started_at = req.started_at.unwrap_or(now);
    let state = parse_incident_state(req.state.as_deref())
        .unwrap_or(crate::coordinator::storage::IncidentState::Investigating);
    let id = format!("manual-{now}");
    let checks: Vec<crate::coordinator::storage::IncidentCheckRow> = req
        .checks
        .iter()
        .map(|c| crate::coordinator::storage::IncidentCheckRow {
            check_id: c.clone(),
            source: crate::coordinator::storage::AffectSource::Manual,
        })
        .collect();
    let body = req.body.unwrap_or_default();
    let store = coord.inner.storage.lock();
    match store.create_incident(
        &id,
        title,
        state,
        started_at,
        req.global.unwrap_or(false),
        None,
        &checks,
        Some((&body, Some("admin"))),
    ) {
        Ok(row) => {
            let dto = IncidentDto::from_row(row, now, true);
            let _ = coord.inner.events.send(WsEvent::IncidentOpened {
                incident: dto.clone(),
            });
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// Edit the operator-owned fields. Deliberately cannot move `state`: that only
/// happens by posting an update, so the timeline always explains the state.
pub(in crate::coordinator) async fn edit_incident_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<EditIncidentRequest>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let title = req.title.trim();
    if title.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "title is required");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    match store.edit_incident(&id, title, req.started_at, req.global) {
        Ok(Some(row)) => {
            let ack = store.is_acknowledged(&row.id).unwrap_or(false);
            let dto = IncidentDto::from_row(row, now, ack);
            let _ = coord.inner.events.send(WsEvent::IncidentUpdated {
                incident: dto.clone(),
            });
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no incident with that id"),
        Err(e) => internal_error(e),
    }
}

/// Delete an incident outright: the row, its narrative and its affects list.
///
/// Allowed while the incident is still open. The reconciler will simply open a
/// fresh one on the next confirmed DOWN if the check is still failing, which
/// is honest — the confirm copy in the editor says so rather than the server
/// refusing on the operator's behalf.
///
/// Linked alerts survive with a null `incident_id`: an alert records that a
/// check flipped, and that stays true whatever happens to the story told about
/// it.
pub(in crate::coordinator) async fn delete_incident_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let node_id = coord.inner.node_id.clone();
    let deleted = {
        let store = coord.inner.storage.lock();
        match store.delete_incident(&id, now, &node_id) {
            Ok(deleted) => deleted,
            Err(e) => return internal_error(e),
        }
    };
    if !deleted {
        return error_response(StatusCode::NOT_FOUND, "no incident with that id");
    }
    coord.replicate_incident_deletions(vec![id.clone()]);
    let _ = coord
        .inner
        .events
        .send(WsEvent::IncidentDeleted { id: id.clone() });
    info!(incident = %id, "incident deleted");
    Json(DeletedResponse { deleted: id }).into_response()
}

/// Replace the manually attached checks. Origin and dependency attachments are
/// untouched — those belong to reconciliation, and an operator editing this
/// list must not be able to delete the outage's own cause from it.
pub(in crate::coordinator) async fn incident_checks_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<IncidentChecksRequest>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    if let Err(e) = store.set_manual_checks(&id, &req.checks) {
        return internal_error(e);
    }
    match store.incident(&id) {
        Ok(Some(row)) => {
            let ack = store.is_acknowledged(&row.id).unwrap_or(false);
            let dto = IncidentDto::from_row(row, now, ack);
            let _ = coord.inner.events.send(WsEvent::IncidentUpdated {
                incident: dto.clone(),
            });
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no incident with that id"),
        Err(e) => internal_error(e),
    }
}

/// Post an update. The only way an incident changes state, and the only way it
/// gets acknowledged: an authored update on a `detected` incident IS the ack,
/// which is why there is no separate acknowledge route.
pub(in crate::coordinator) async fn incident_update_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<IncidentUpdateRequest>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let Some(state) = parse_incident_state(Some(&req.state)) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "state must be detected, investigating, identified, monitoring or resolved",
        );
    };
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    if store.incident(&id).ok().flatten().is_none() {
        return error_response(StatusCode::NOT_FOUND, "no incident with that id");
    }
    // Logins are password-only: there is no per-user identity to record, so the
    // byline is the role. What matters is that it is non-NULL — that is what
    // makes this an acknowledgement rather than a coordinator entry.
    if let Err(e) = store.add_update(&id, state, req.body.trim(), Some("admin"), now) {
        return internal_error(e);
    }
    match store.incident(&id) {
        Ok(Some(row)) => {
            let updates = store.updates_for_incident(&row.id).unwrap_or_default();
            let mut dto = IncidentDto::from_row(row, now, true);
            dto.updates = Some(updates.into_iter().map(IncidentUpdateDto::from).collect());
            let _ = coord.inner.events.send(WsEvent::IncidentUpdated {
                incident: dto.clone(),
            });
            Json(serde_json::json!({ "incident": dto })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no incident with that id"),
        Err(e) => internal_error(e),
    }
}

fn parse_incident_state(raw: Option<&str>) -> Option<crate::coordinator::storage::IncidentState> {
    use crate::coordinator::storage::IncidentState as S;
    match raw {
        Some("detected") => Some(S::Detected),
        Some("investigating") => Some(S::Investigating),
        Some("identified") => Some(S::Identified),
        Some("monitoring") => Some(S::Monitoring),
        Some("resolved") => Some(S::Resolved),
        _ => None,
    }
}

/// A single check's incident history, for the check detail view.
pub(in crate::coordinator) async fn check_incidents_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    Query(q): Query<IncidentQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(20).min(1000);
    let now = chrono::Utc::now().timestamp_millis();
    let store = coord.inner.storage.lock();
    match store.incidents_for_check(&id, limit) {
        Ok(rows) => {
            let incidents = rows
                .into_iter()
                .map(|r| {
                    let ack = store.is_acknowledged(&r.id).unwrap_or(false);
                    IncidentDto::from_row(r, now, ack)
                })
                .collect();
            Json(IncidentsResponse { incidents }).into_response()
        }
        Err(e) => internal_error(e),
    }
}
