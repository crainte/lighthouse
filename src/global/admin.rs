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
use super::{error_response, reconcile, GlobalNode};
use crate::coordinator::storage::{Section, SectionDisplay};
use crate::http::{AdminJson, AdminSession};
use crate::models::CheckId;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
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
    _admin: AdminSession,
    Query(q): Query<ChecksQuery>,
) -> Response {
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
    AdminJson(body): AdminJson<serde_json::Value>,
) -> Response {
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
    AdminJson(body): AdminJson<serde_json::Value>,
) -> Response {
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
    _admin: AdminSession,
) -> Response {
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
    AdminJson(body): AdminJson<CheckOrderRequest>,
) -> Response {
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
    _admin: AdminSession,
) -> Response {
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
    AdminJson(body): AdminJson<SectionNameRequest>,
) -> Response {
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
    AdminJson(body): AdminJson<SectionNameRequest>,
) -> Response {
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
    _admin: AdminSession,
) -> Response {
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
    AdminJson(body): AdminJson<SectionOrderRequest>,
) -> Response {
    match node.inner.store.reorder_sections(body.order).await {
        Ok((sections, entries)) => {
            reconcile::push_now(&node, entries);
            Json(SectionsResponse { sections }).into_response()
        }
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

// ---- Regions (see `regions`) ----

/// A registered region for Admin > Regions. Never carries a secret: the key
/// is shown once, by `mint_region_handler`, and the relay token not at all.
#[derive(Serialize)]
struct RegionDto {
    name: String,
    credential_id: String,
    created_at: i64,
    endpoints: Vec<String>,
    /// Whether the region has pushed with this key yet.
    attached: bool,
    stale: bool,
    age_ms: i64,
    check_count: usize,
}

pub(super) async fn list_regions_handler(
    State(node): State<GlobalNode>,
    _admin: AdminSession,
) -> Response {
    let roster = node
        .inner
        .cache
        .roster(chrono::Utc::now().timestamp_millis());
    let regions: Vec<RegionDto> = node
        .inner
        .regions
        .list()
        .into_iter()
        .map(|r| {
            let live = roster.iter().find(|s| s.region == r.name);
            RegionDto {
                attached: r.relay_token.is_some(),
                stale: live.is_none_or(|s| s.stale),
                age_ms: live.map_or(-1, |s| s.age_ms),
                check_count: live.map_or(0, |s| s.check_count),
                name: r.name,
                credential_id: r.credential_id,
                created_at: r.created_at,
                endpoints: r.endpoints,
            }
        })
        .collect();
    Json(serde_json::json!({ "regions": regions })).into_response()
}

#[derive(Deserialize)]
pub(super) struct MintRegionRequest {
    name: String,
}

/// Register a region, or rotate its key. The key is returned exactly once.
pub(super) async fn mint_region_handler(
    State(node): State<GlobalNode>,
    AdminJson(body): AdminJson<MintRegionRequest>,
) -> Response {
    let name = body.name.trim();
    if let Err(e) = super::regions::valid_name(name) {
        return error_response(StatusCode::BAD_REQUEST, &e);
    }
    match node.mint_region(name).await {
        Ok((record, token)) => Json(serde_json::json!({
            "region": record.name,
            "credential_id": record.credential_id,
            "token": token,
        }))
        .into_response(),
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

pub(super) async fn remove_region_handler(
    State(node): State<GlobalNode>,
    Path(name): Path<String>,
    _admin: AdminSession,
) -> Response {
    match node.remove_region(&name).await {
        Ok(Some(checks)) => {
            Json(serde_json::json!({ "removed": name, "checks_updated": checks })).into_response()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no such region"),
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

/// Decommission a peer global node; see `Cluster::decommission`.
pub(super) async fn decommission_member_handler(
    State(node): State<GlobalNode>,
    Path(node_id): Path<String>,
    _admin: AdminSession,
) -> Response {
    crate::coordinator::cluster::Cluster::decommission(node.inner.cluster.as_deref(), &node_id)
}
