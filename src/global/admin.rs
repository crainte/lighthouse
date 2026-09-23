//! Admin handlers for the two catalogue kinds the global tier holds
//! authoritatively in Phase 1: checks and sections. See `store`'s module doc
//! for why the rest of the admin surface (notifiers, credentials, rollup
//! policies, incidents, maintenance…) is not here — those still fall through
//! `relay_handler` to a region, unchanged from before this phase.
//!
//! Every handler here follows the same shape: authenticate, ask
//! `GlobalStore` to do the whole thing (validate, persist, stamp), and on
//! success hand the entries it returns to `reconcile::push_now` so regions
//! converge without the operator's request waiting on any of them. A region
//! that is unreachable right now costs nothing here — `reconcile`'s periodic
//! sweep is what catches it up once it returns.
//!
//! Response shapes are deliberately the same JSON the coordinator's own
//! `routes::checks`/`routes::sections` return (`{check: ...}`,
//! `{checks: ..., has_more: ...}`, `{section: ...}`, `{sections: ...}`): the
//! embedded SPA talks to both a coordinator and a global node with the same
//! client code, and a shape drift here would be invisible until a browser
//! hit it.

use super::store::AdminError;
use super::{error_response, reconcile, require_admin, GlobalNode};
use crate::coordinator::storage::{Section, SectionDisplay};
use crate::models::CheckId;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

fn admin_error(e: AdminError) -> Response {
    match e {
        AdminError::BadRequest(msg) => error_response(StatusCode::BAD_REQUEST, &msg),
        AdminError::Conflict(msg) => error_response(StatusCode::CONFLICT, &msg),
        AdminError::NotFound => error_response(StatusCode::NOT_FOUND, "no such check"),
        AdminError::Internal(e) => {
            tracing::error!(error = %e, "global: catalogue write failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

fn parse_check_id(id: &str) -> Result<CheckId, Box<Response>> {
    id.parse()
        .map_err(|_| Box::new(error_response(StatusCode::NOT_FOUND, "no such check")))
}

// ---- Checks ----

#[derive(Deserialize)]
pub(super) struct ChecksQuery {
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct ChecksResponse {
    checks: Vec<crate::config::CheckConfig>,
    total: usize,
    has_more: bool,
}

#[derive(Serialize)]
struct CheckResponse {
    check: crate::config::CheckConfig,
}

pub(super) async fn list_checks_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Query(q): Query<ChecksQuery>,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    let (page, limit) = (
        q.page.unwrap_or(0),
        crate::page::bounded(q.limit, crate::page::MAX_PAGE_SIZE),
    );
    match node.inner.store.list_checks(page, limit).await {
        Ok((checks, total, has_more)) => Json(ChecksResponse {
            checks,
            total,
            has_more,
        })
        .into_response(),
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

pub(super) async fn create_check_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    match node.inner.store.create_check(body).await {
        Ok((check, entry)) => {
            reconcile::push_now(&node, vec![entry]);
            Json(CheckResponse { check }).into_response()
        }
        Err(e) => admin_error(e),
    }
}

pub(super) async fn update_check_handler(
    State(node): State<GlobalNode>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    let id = match parse_check_id(&id) {
        Ok(id) => id,
        Err(denied) => return *denied,
    };
    match node.inner.store.update_check(id, body).await {
        Ok((check, entry)) => {
            reconcile::push_now(&node, vec![entry]);
            Json(CheckResponse { check }).into_response()
        }
        Err(e) => admin_error(e),
    }
}

pub(super) async fn delete_check_handler(
    State(node): State<GlobalNode>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    let id = match parse_check_id(&id) {
        Ok(id) => id,
        Err(denied) => return *denied,
    };
    match node.inner.store.delete_check(id).await {
        Ok(entry) => {
            reconcile::push_now(&node, vec![entry]);
            Json(serde_json::json!({ "deleted": id.to_string() })).into_response()
        }
        Err(e) => admin_error(e),
    }
}

#[derive(Deserialize)]
pub(super) struct CheckOrderRequest {
    order: Vec<CheckId>,
}

pub(super) async fn reorder_checks_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Json(body): Json<CheckOrderRequest>,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    match node.inner.store.reorder_checks(body.order).await {
        Ok((checks, entries)) => {
            let total = checks.len();
            reconcile::push_now(&node, entries);
            Json(ChecksResponse {
                checks,
                total,
                has_more: false,
            })
            .into_response()
        }
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

// ---- Sections ----

#[derive(Serialize)]
struct SectionsResponse {
    sections: Vec<Section>,
}

#[derive(Serialize)]
struct SectionResponse {
    section: Section,
}

async fn sections_response(node: &GlobalNode) -> Response {
    match node.inner.store.list_sections().await {
        Ok(sections) => Json(SectionsResponse { sections }).into_response(),
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

/// No auth: a section name and whether it starts collapsed reveal nothing a
/// visitor cannot already see, matching the coordinator's own
/// `public_sections_handler`.
pub(super) async fn public_sections_handler(State(node): State<GlobalNode>) -> Response {
    sections_response(&node).await
}

pub(super) async fn list_sections_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    sections_response(&node).await
}

#[derive(Deserialize)]
pub(super) struct SectionNameRequest {
    name: String,
    collapsed: Option<bool>,
    display: Option<SectionDisplay>,
}

pub(super) async fn create_section_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Json(body): Json<SectionNameRequest>,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    let collapsed = body.collapsed.unwrap_or(false);
    let display = body.display.unwrap_or_default();
    match node
        .inner
        .store
        .create_section(&body.name, collapsed, display)
        .await
    {
        Ok((section, entry)) => {
            reconcile::push_now(&node, vec![entry]);
            Json(SectionResponse { section }).into_response()
        }
        Err(e) => admin_error(e),
    }
}

pub(super) async fn rename_section_handler(
    State(node): State<GlobalNode>,
    Path(current): Path<String>,
    headers: HeaderMap,
    Json(body): Json<SectionNameRequest>,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    match node
        .inner
        .store
        .rename_section(&current, &body.name, body.collapsed, body.display)
        .await
    {
        Ok((section, entries)) => {
            reconcile::push_now(&node, entries);
            Json(SectionResponse { section }).into_response()
        }
        Err(e) => admin_error(e),
    }
}

pub(super) async fn delete_section_handler(
    State(node): State<GlobalNode>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    match node.inner.store.delete_section(&name).await {
        Ok(entries) => {
            reconcile::push_now(&node, entries);
            Json(serde_json::json!({ "deleted": name })).into_response()
        }
        Err(e) => admin_error(e),
    }
}

#[derive(Deserialize)]
pub(super) struct SectionOrderRequest {
    order: Vec<String>,
}

pub(super) async fn reorder_sections_handler(
    State(node): State<GlobalNode>,
    headers: HeaderMap,
    Json(body): Json<SectionOrderRequest>,
) -> Response {
    if let Some(denied) = require_admin(&node, &headers) {
        return denied;
    }
    match node.inner.store.reorder_sections(body.order).await {
        Ok((sections, entries)) => {
            reconcile::push_now(&node, entries);
            Json(SectionsResponse { sections }).into_response()
        }
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}
