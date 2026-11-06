//! Admin handlers for the two catalogue kinds the global tier holds
//! authoritatively in Phase 1: checks and sections. See `store`'s module doc
//! for why the rest of the admin surface (notifiers, credentials, rollup
//! policies, incidents…) is not here — those still fall through
//! `relay_handler` to a region, unchanged from before this phase. Maintenance
//! is the one hybrid: its windows still go through the relay, but the check
//! picker is filled from this catalogue, since a region holds only the checks
//! its placement admits.
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
use super::{error_response, reconcile, relay, GlobalNode};
use crate::coordinator::storage::{Section, SectionDisplay};
use crate::http::{AdminBytes, AdminJson, AdminSession};
use crate::models::CheckId;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{Method, StatusCode, Uri};
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

// ---- Maintenance ----

/// Replaces the `checks` picker list in a region's maintenance response with
/// the global catalogue's. `windows` is left as the region returned it. A body
/// that is not an object carrying a `checks` field is returned unchanged.
fn with_catalogue_checks(
    mut body: serde_json::Value,
    options: &[(CheckId, String)],
) -> serde_json::Value {
    if let Some(checks) = body.as_object_mut().filter(|o| o.contains_key("checks")) {
        checks.insert(
            "checks".to_string(),
            options
                .iter()
                .map(|(id, name)| serde_json::json!({ "id": id.to_string(), "name": name }))
                .collect(),
        );
    }
    body
}

/// Swaps in the catalogue's check list on a successful regional answer; every
/// other response, including errors and unparseable bodies, passes through.
async fn maintenance_with_catalogue(node: &GlobalNode, resp: Response) -> Response {
    if !resp.status().is_success() {
        return resp;
    }
    let (parts, body) = resp.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, usize::MAX).await else {
        return error_response(StatusCode::BAD_GATEWAY, "region response was unreadable");
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Response::from_parts(parts, axum::body::Body::from(bytes));
    };
    match node.inner.store.check_options().await {
        Ok(options) => Json(with_catalogue_checks(value, &options)).into_response(),
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

pub(super) async fn maintenance_handler(
    State(node): State<GlobalNode>,
    _admin: AdminSession,
    uri: Uri,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let pq = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path());
    let resp = relay::relay_single_path(
        &node,
        &Method::GET,
        pq,
        &Bytes::new(),
        &node.inner.cache.all_endpoints(now),
        true,
    )
    .await;
    maintenance_with_catalogue(&node, resp).await
}

pub(super) async fn update_maintenance_handler(
    State(node): State<GlobalNode>,
    uri: Uri,
    AdminBytes(body): AdminBytes,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let resp = relay::fan_out_write(&node, &Method::PUT, &uri, &body, now).await;
    maintenance_with_catalogue(&node, resp).await
}

// ---- Sections ----

pub(super) async fn public_spacers_handler(State(node): State<GlobalNode>) -> Response {
    match node.inner.store.homepage_spacers().await {
        Ok(spacers) => Json(spacers).into_response(),
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

pub(super) async fn update_spacers_handler(
    State(node): State<GlobalNode>,
    AdminJson(body): AdminJson<crate::homepage::Spacers>,
) -> Response {
    if let Err(reason) = body.validate() {
        return error_response(StatusCode::BAD_REQUEST, reason);
    }
    match node.inner.store.put_homepage_spacers(&body).await {
        Ok(entry) => {
            reconcile::push_now(&node, vec![entry]);
            Json(body).into_response()
        }
        Err(e) => admin_error(AdminError::Internal(e)),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn id(n: u128) -> CheckId {
        CheckId::from_u128(n)
    }

    #[test]
    fn regional_picker_list_is_replaced_by_the_catalogue_and_windows_are_kept() {
        let region = json!({
            "windows": [{"title": "w", "checks": [id(2).to_string()]}],
            "checks": [{"id": id(1).to_string(), "name": "api"}],
        });
        let options = vec![
            (id(1), "api".to_string()),
            (id(2), "unraid-ping".to_string()),
        ];
        let out = with_catalogue_checks(region.clone(), &options);
        assert_eq!(out["windows"], region["windows"]);
        assert_eq!(
            out["checks"],
            json!([
                {"id": id(1).to_string(), "name": "api"},
                {"id": id(2).to_string(), "name": "unraid-ping"},
            ])
        );
    }

    #[test]
    fn bodies_without_a_checks_field_pass_through() {
        let options = vec![(id(1), "api".to_string())];
        for body in [json!({"error": "nope"}), json!([1, 2]), json!("x")] {
            assert_eq!(with_catalogue_checks(body.clone(), &options), body);
        }
    }
}
