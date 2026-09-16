//! Status-page sections: pure presentation over checks, not a storage
//! concept of their own.
//!
//! **Rows exist to be a heading, not a target.** A section groups checks for
//! display and nothing else — it carries no notifier, no schedule, no
//! expectation. That is why `create_section_handler` and friends live behind
//! the check editor's own admin gate rather than a separate permission: the
//! only thing a section write can do is relabel or reorder what the public
//! page shows.
//!
//! **Name validation and ordering rules live here, not in storage,** because
//! they are about what makes a heading fit to render — length, emptiness,
//! position — not about how a row is persisted. `storage` stores whatever
//! name it is given; this module is what refuses to store a bad one.

use super::super::{error_response, internal_error, storage, Coordinator, DeletedResponse};
use super::session::require_admin;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Serialize)]
struct SectionDto {
    name: String,
    position: i32,
    /// Whether the public page renders this section collapsed on first load.
    collapsed: bool,
}

impl From<storage::Section> for SectionDto {
    fn from(s: storage::Section) -> Self {
        Self {
            name: s.name,
            position: s.position,
            collapsed: s.collapsed,
        }
    }
}

#[derive(Serialize)]
struct SectionsResponse {
    sections: Vec<SectionDto>,
}

#[derive(Serialize)]
struct SectionResponse {
    section: SectionDto,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct SectionNameRequest {
    name: String,
    /// Absent on rename means "leave it as it is", so a client that predates the
    /// flag can't silently expand every section it touches.
    #[serde(default)]
    collapsed: Option<bool>,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct ReorderRequest {
    order: Vec<String>,
}

/// Section names appear as headings and are matched against `checks.group_name`,
/// so an empty or absurd one is rejected at the boundary.
fn valid_section_name(name: &str) -> Result<String, String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err("section name is required".into());
    }
    if trimmed.chars().count() > 60 {
        return Err("section name must be 60 characters or fewer".into());
    }
    Ok(trimmed.to_string())
}

fn sections_response(coord: &Coordinator) -> Response {
    match coord.inner.storage.lock().list_sections() {
        Ok(sections) => Json(SectionsResponse {
            sections: sections.into_iter().map(Into::into).collect(),
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// Section metadata for the public status page. No auth: a heading name and
/// whether it starts collapsed reveal nothing a visitor cannot already see.
pub(in crate::coordinator) async fn public_sections_handler(
    State(coord): State<Coordinator>,
) -> Response {
    sections_response(&coord)
}

pub(in crate::coordinator) async fn list_sections_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    sections_response(&coord)
}

pub(in crate::coordinator) async fn create_section_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<SectionNameRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let name = match valid_section_name(&body.name) {
        Ok(n) => n,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    match coord
        .inner
        .storage
        .lock()
        .create_section(&name, body.collapsed.unwrap_or(false))
    {
        Ok(true) => {}
        Ok(false) => {
            return error_response(
                StatusCode::CONFLICT,
                &format!("a section named \"{name}\" already exists"),
            )
        }
        Err(e) => return internal_error(e),
    }
    coord.restamp_and_push(storage::ConfigKind::Section, &name);
    Json(SectionResponse {
        section: SectionDto {
            name,
            position: 0,
            collapsed: body.collapsed.unwrap_or(false),
        },
    })
    .into_response()
}

pub(in crate::coordinator) async fn rename_section_handler(
    State(coord): State<Coordinator>,
    Path(current): Path<String>,
    headers: HeaderMap,
    Json(body): Json<SectionNameRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let name = match valid_section_name(&body.name) {
        Ok(n) => n,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    if name != current {
        let taken = match coord.inner.storage.lock().list_sections() {
            Ok(existing) => existing.iter().any(|s| s.name == name),
            Err(e) => return internal_error(e),
        };
        if taken {
            return error_response(
                StatusCode::CONFLICT,
                &format!("a section named \"{name}\" already exists"),
            );
        }
    }
    match coord.inner.storage.lock().rename_section(
        &current,
        &name,
        chrono::Utc::now().timestamp_millis(),
    ) {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::NOT_FOUND, "no such section"),
        Err(e) => return internal_error(e),
    }
    // Absent means "leave it alone"; the same endpoint is used to rename and to
    // toggle how the public page first renders the section.
    if let Some(collapsed) = body.collapsed {
        if let Err(e) = coord
            .inner
            .storage
            .lock()
            .set_section_collapsed(&name, collapsed)
        {
            return internal_error(e);
        }
    }
    // Renaming moves every check in the section, so the cached metadata the
    // status page reads is now stale.
    if let Err(e) = coord.reload_checks() {
        return internal_error(e);
    }
    coord.restamp_and_push(storage::ConfigKind::Section, &name);
    // Every check `rename_section` moved gets its own stamp and push — its
    // document changed too (its `group` now names the new section), and a
    // peer that only heard about the section itself would show those checks
    // still under the old name.
    //
    // Collected into an owned `Vec` in its own statement first: a `for`
    // loop's head keeps any temporary it creates (the read guard) alive for
    // the whole loop body, not just until the `Vec` is built, and
    // `restamp_and_push` below takes a write lock on this same map via
    // `reload_checks` — holding the read guard that long would deadlock it
    // against itself.
    let moved: Vec<crate::models::CheckId> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| m.group.as_deref() == Some(name.as_str()))
        .map(|(id, _)| *id)
        .collect();
    for check_id in moved {
        coord.restamp_and_push(storage::ConfigKind::Check, &check_id.to_string());
    }
    info!(from = %current, to = %name, "section renamed");
    let collapsed = coord
        .inner
        .storage
        .lock()
        .list_sections()
        .ok()
        .and_then(|all| all.into_iter().find(|s| s.name == name))
        .map(|s| s.collapsed)
        .unwrap_or(false);
    Json(SectionResponse {
        section: SectionDto {
            name,
            position: 0,
            collapsed,
        },
    })
    .into_response()
}

pub(in crate::coordinator) async fn delete_section_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    // Captured before the delete moves them: the checks that are about to be
    // loosened, so every one of them can be stamped and pushed too — its
    // document changed (`group` is now `None`), the same as any other edit.
    let affected: Vec<crate::models::CheckId> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| m.group.as_deref() == Some(name.as_str()))
        .map(|(id, _)| *id)
        .collect();
    match coord
        .inner
        .storage
        .lock()
        .delete_section(&name, chrono::Utc::now().timestamp_millis())
    {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::NOT_FOUND, "no such section"),
        Err(e) => return internal_error(e),
    }
    if let Err(e) = coord.reload_checks() {
        return internal_error(e);
    }
    // A tombstone for the section itself, pushed the same way any other
    // delete is.
    match coord.write_config(storage::ConfigKind::Section, &name, None) {
        Ok(entry) => coord.replicate_config(vec![entry]),
        Err(e) => {
            tracing::warn!(error = %e, section = %name, "config: failed to push section tombstone")
        }
    }
    for check_id in &affected {
        coord.restamp_and_push(storage::ConfigKind::Check, &check_id.to_string());
    }
    info!(section = %name, "section deleted, its checks are now loose");
    Json(DeletedResponse { deleted: name }).into_response()
}

pub(in crate::coordinator) async fn reorder_sections_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<ReorderRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let before = match coord.inner.storage.lock().list_sections() {
        Ok(s) => s,
        Err(e) => return internal_error(e),
    };
    if let Err(e) = coord.inner.storage.lock().reorder_sections(&body.order) {
        return internal_error(e);
    }
    let after = match coord.inner.storage.lock().list_sections() {
        Ok(s) => s,
        Err(e) => return internal_error(e),
    };
    // Only the sections whose position actually moved are stamped and
    // pushed: the ones the reorder rewrote are the ones a peer needs to
    // hear about, the rest already agree.
    let before_pos: std::collections::HashMap<&str, i32> = before
        .iter()
        .map(|s| (s.name.as_str(), s.position))
        .collect();
    for s in &after {
        if before_pos.get(s.name.as_str()) != Some(&s.position) {
            coord.restamp_and_push(storage::ConfigKind::Section, &s.name);
        }
    }
    sections_response(&coord)
}
