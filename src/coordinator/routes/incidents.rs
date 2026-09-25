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
    error_response, internal_error, storage, Coordinator, DeletedResponse, WsEvent,
};
use super::session::require_admin;
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
            origin_check: i.origin_check.map(|id| id.to_string()),
            checks: i
                .checks
                .into_iter()
                .map(|c| IncidentCheckDto {
                    check_id: c.check_id.to_string(),
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
    /// Epoch milliseconds. This bounds resolved incidents only; open outages
    /// remain visible even when they began before the selected status window.
    since: Option<i64>,
}

/// Bounds the destructive short-incident cleanup to the transient outages it
/// exists to remove. A caller must deliberately choose a larger threshold.
#[derive(Deserialize)]
pub(in crate::coordinator) struct ShortIncidentPurgeQuery {
    max_secs: Option<u64>,
}

#[derive(Serialize)]
struct ShortIncidentPurgeResponse {
    deleted: u64,
}

#[derive(Serialize)]
struct ResultsCountResponse {
    raw_results: u64,
}

#[derive(Serialize)]
struct ResultsPurgeResponse {
    deleted: u64,
}

const DEFAULT_SHORT_INCIDENT_MAX_SECS: u64 = 60;
const MAX_SHORT_INCIDENT_MAX_SECS: u64 = 3_600;

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
    checks: Vec<crate::models::CheckId>,
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
    checks: Vec<crate::models::CheckId>,
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
    let limit = crate::page::bounded(q.limit, 50);
    let open_only = incident_open_filter(q.status.as_deref());
    let now = chrono::Utc::now().timestamp_millis();
    match coord
        .db(move |storage| {
            storage
                .list_incidents(limit, open_only, q.since)
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| {
                            let ack = storage.is_acknowledged(&row.id).unwrap_or(false);
                            IncidentDto::from_row(row, now, ack)
                        })
                        .collect::<Vec<_>>()
                })
        })
        .await
    {
        Ok(mut incidents) => {
            // Titles are resolved against the live catalogue here rather than
            // trusted from when they were written, so a name learned after the
            // incident opened — or changed since — is the one shown.
            coord.resolve_incident_titles(&mut incidents);
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
    let lookup_id = id.clone();
    match coord
        .db(move |storage| {
            let Some(row) = storage.incident(&lookup_id)? else {
                return Ok(None);
            };
            let ack = storage.is_acknowledged(&row.id).unwrap_or(false);
            let updates = storage.updates_for_incident(&row.id).unwrap_or_default();
            let mut dto = IncidentDto::from_row(row, now, ack);
            dto.updates = Some(updates.into_iter().map(IncidentUpdateDto::from).collect());
            Ok(Some(dto))
        })
        .await
    {
        Ok(Some(mut dto)) => {
            coord.resolve_incident_titles(std::slice::from_mut(&mut dto));
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
    body: axum::body::Bytes,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    // Parsed only once the session has checked out: `Json<T>` as an argument
    // would reject a body shaped wrong before this handler's own code — and
    // so `require_admin` above — ever ran, letting an anonymous caller learn
    // the body was malformed instead of the uniform "authentication
    // required" every other admin route gives them.
    let req: CreateIncidentRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
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
            check_id: *c,
            source: crate::coordinator::storage::AffectSource::Manual,
        })
        .collect();
    let body = req.body.unwrap_or_default();
    let title = title.to_string();
    let create_id = id.clone();
    match coord
        .db(move |storage| {
            storage.create_incident(
                &create_id,
                &title,
                state,
                started_at,
                req.global.unwrap_or(false),
                None,
                &checks,
                Some((&body, Some("admin"))),
            )
        })
        .await
    {
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
    let title = title.to_string();
    let edit_id = id.clone();
    match coord
        .db(move |storage| {
            let row = storage.edit_incident(&edit_id, &title, req.started_at, req.global)?;
            Ok(row.map(|row| {
                let ack = storage.is_acknowledged(&row.id).unwrap_or(false);
                IncidentDto::from_row(row, now, ack)
            }))
        })
        .await
    {
        Ok(Some(dto)) => {
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
    let delete_id = id.clone();
    let deleted = match coord
        .db(move |storage| storage.delete_incident(&delete_id, now, &node_id))
        .await
    {
        Ok(deleted) => deleted,
        Err(e) => return internal_error(e),
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

/// Return the exact raw-result count shown in the destructive purge confirm.
/// This is separate from the explorer's filtered result list.
pub(in crate::coordinator) async fn results_count_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    match coord.db(|storage| storage.count_results()).await {
        Ok(raw_results) => Json(ResultsCountResponse { raw_results }).into_response(),
        Err(e) => internal_error(e),
    }
}

/// Purge all raw results and their in-memory leaf beliefs. Published roots stay
/// intact because they represent the coordinator's last announced state.
pub(in crate::coordinator) async fn purge_results_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let deleted = match coord.db(|storage| storage.purge_results()).await {
        Ok(deleted) => deleted,
        Err(e) => return internal_error(e),
    };
    coord.inner.tracker.lock().clear_leaf_beliefs();
    info!(deleted, "purged raw results");
    Json(ResultsPurgeResponse { deleted }).into_response()
}

/// Delete resolved, transient incidents created by a brief probe failure.
///
/// This is deliberately admin-only and refuses zero or broad thresholds: it is
/// cleanup for a known short-incident bug, not a general history-retention API.
pub(in crate::coordinator) async fn purge_short_incidents_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Query(q): Query<ShortIncidentPurgeQuery>,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    let max_secs = q.max_secs.unwrap_or(DEFAULT_SHORT_INCIDENT_MAX_SECS);
    if !(1..=MAX_SHORT_INCIDENT_MAX_SECS).contains(&max_secs) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "max_secs must be between 1 and 3600",
        );
    }
    match coord
        .db(move |storage| storage.purge_short_incidents(max_secs))
        .await
    {
        Ok(deleted) => {
            info!(max_secs, deleted, "purged short incidents");
            Json(ShortIncidentPurgeResponse { deleted }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// Replace the manually attached checks. Origin and dependency attachments are
/// untouched — those belong to reconciliation, and an operator editing this
/// list must not be able to delete the outage's own cause from it.
pub(in crate::coordinator) async fn incident_checks_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Some(rejection) = require_admin(&coord, &headers) {
        return rejection;
    }
    // See `create_incident_handler`: parsed after auth, not before, so a
    // caller with no session learns nothing about whether the body would
    // even have been valid.
    let req: IncidentChecksRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let now = chrono::Utc::now().timestamp_millis();
    let update_id = id.clone();
    match coord
        .db(move |storage| {
            storage.set_manual_checks(&update_id, &req.checks)?;
            let row = storage.incident(&update_id)?;
            Ok(row.map(|row| {
                let ack = storage.is_acknowledged(&row.id).unwrap_or(false);
                IncidentDto::from_row(row, now, ack)
            }))
        })
        .await
    {
        Ok(Some(dto)) => {
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
    let update_id = id.clone();
    let update_body = req.body.trim().to_string();
    match coord
        .db(move |storage| {
            let Some(current) = storage.incident(&update_id)? else {
                return Ok(None);
            };
            // An empty-bodied update that does not move the state carries no
            // information, and the acknowledgement it would have recorded is
            // already on the record. Writing one anyway lets a repeated "Ack"
            // append the same silent row forever — which then replicates to
            // every peer and pads a public narrative with nothing. Answer with
            // the incident as it stands instead.
            let redundant = update_body.is_empty()
                && current.state == state
                && storage.is_acknowledged(&update_id)?;
            if !redundant {
                storage.add_update(&update_id, state, &update_body, Some("admin"), now)?;
            }
            let row = storage
                .incident(&update_id)?
                .expect("incident checked above");
            let updates = storage.updates_for_incident(&row.id).unwrap_or_default();
            let mut dto = IncidentDto::from_row(row, now, true);
            dto.updates = Some(updates.into_iter().map(IncidentUpdateDto::from).collect());
            Ok(Some(dto))
        })
        .await
    {
        Ok(Some(dto)) => {
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
    Path(id): Path<crate::models::CheckId>,
    Query(q): Query<IncidentQuery>,
) -> Response {
    let limit = crate::page::bounded(q.limit, 20);
    let now = chrono::Utc::now().timestamp_millis();
    match coord
        .db(move |storage| {
            storage.incidents_for_check(id, limit).map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        let ack = storage.is_acknowledged(&row.id).unwrap_or(false);
                        IncidentDto::from_row(row, now, ack)
                    })
                    .collect()
            })
        })
        .await
    {
        Ok(incidents) => Json(IncidentsResponse { incidents }).into_response(),
        Err(e) => internal_error(e),
    }
}
