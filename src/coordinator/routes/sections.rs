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

use super::super::router::{error_response, internal_error, DeletedResponse};
use super::super::{storage, Coordinator};
use crate::http::{AdminJson, AdminSession};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::info;

async fn spacers_response(coord: &Coordinator) -> Response {
    match coord
        .db(|storage| storage.get_setting(crate::homepage::KEY))
        .await
    {
        Ok(Some(value)) => match serde_json::from_str::<crate::homepage::Spacers>(&value) {
            Ok(spacers) => Json(spacers).into_response(),
            Err(e) => internal_error(e.into()),
        },
        Ok(None) => Json(crate::homepage::Spacers::default()).into_response(),
        Err(e) => internal_error(e),
    }
}

pub(in crate::coordinator) async fn public_spacers_handler(
    State(coord): State<Coordinator>,
) -> Response {
    spacers_response(&coord).await
}

pub(in crate::coordinator) async fn update_spacers_handler(
    State(coord): State<Coordinator>,
    AdminJson(body): AdminJson<crate::homepage::Spacers>,
) -> Response {
    if let Err(reason) = body.validate() {
        return error_response(StatusCode::BAD_REQUEST, reason);
    }
    let value = match serde_json::to_string(&body) {
        Ok(value) => value,
        Err(e) => return internal_error(e.into()),
    };
    match coord
        .write_config(
            storage::ConfigKind::Setting,
            crate::homepage::KEY,
            Some(value),
        )
        .await
    {
        Ok(entry) => {
            coord.replicate_config(vec![entry]);
            Json(body).into_response()
        }
        Err(e) => internal_error(e),
    }
}

#[derive(Serialize)]
struct SectionsResponse {
    sections: Vec<storage::Section>,
}

#[derive(Serialize)]
struct SectionResponse {
    section: storage::Section,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct SectionNameRequest {
    name: String,
    /// Absent on rename means "leave it as it is", so a client that predates the
    /// flag can't silently expand every section it touches.
    #[serde(default)]
    collapsed: Option<bool>,
    /// Absent means "leave it as it is", for the same reason: an older admin
    /// client editing a name must not reset a paged section to inline.
    #[serde(default)]
    display: Option<storage::SectionDisplay>,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct ReorderRequest {
    order: Vec<String>,
}

async fn sections_response(coord: &Coordinator) -> Response {
    match coord.db(|storage| storage.list_sections()).await {
        Ok(sections) => Json(SectionsResponse { sections }).into_response(),
        Err(e) => internal_error(e),
    }
}

/// Section metadata for the public status page. No auth: a heading name and
/// whether it starts collapsed reveal nothing a visitor cannot already see.
pub(in crate::coordinator) async fn public_sections_handler(
    State(coord): State<Coordinator>,
) -> Response {
    sections_response(&coord).await
}

pub(in crate::coordinator) async fn list_sections_handler(
    State(coord): State<Coordinator>,
    _admin: AdminSession,
) -> Response {
    sections_response(&coord).await
}

pub(in crate::coordinator) async fn create_section_handler(
    State(coord): State<Coordinator>,
    AdminJson(body): AdminJson<SectionNameRequest>,
) -> Response {
    let name = match storage::Section::valid_name(&body.name) {
        Ok(n) => n,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    let create_name = name.clone();
    let collapsed = body.collapsed.unwrap_or(false);
    let display = body.display.unwrap_or_default();
    match coord
        .db(move |storage| storage.create_section(&create_name, collapsed, display))
        .await
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
    coord
        .restamp_and_push(storage::ConfigKind::Section, &name)
        .await;
    Json(SectionResponse {
        section: storage::Section {
            name,
            position: 0,
            collapsed,
            display,
        },
    })
    .into_response()
}

pub(in crate::coordinator) async fn rename_section_handler(
    State(coord): State<Coordinator>,
    Path(current): Path<String>,
    AdminJson(body): AdminJson<SectionNameRequest>,
) -> Response {
    let name = match storage::Section::valid_name(&body.name) {
        Ok(n) => n,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    if name != current {
        let taken = match coord.db(|storage| storage.list_sections()).await {
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
    let rename_from = current.clone();
    let rename_to = name.clone();
    let collapsed = body.collapsed;
    let display = body.display;
    match coord
        .db(move |storage| {
            let renamed = storage.rename_section(
                &rename_from,
                &rename_to,
                chrono::Utc::now().timestamp_millis(),
            )?;
            if renamed {
                if let Some(collapsed) = collapsed {
                    storage.set_section_collapsed(&rename_to, collapsed)?;
                }
                if let Some(display) = display {
                    storage.set_section_display(&rename_to, display)?;
                }
            }
            Ok(renamed)
        })
        .await
    {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::NOT_FOUND, "no such section"),
        Err(e) => return internal_error(e),
    }
    // Renaming moves every check in the section, so the cached metadata the
    // status page reads is now stale.
    if let Err(e) = coord.reload_checks().await {
        return internal_error(e);
    }
    coord
        .restamp_and_push(storage::ConfigKind::Section, &name)
        .await;
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
        coord
            .restamp_and_push(storage::ConfigKind::Check, &check_id.to_string())
            .await;
    }
    info!(from = %current, to = %name, "section renamed");
    // Read back rather than echo the request: `collapsed` and `display` are both
    // optional, so what was stored is the only thing that knows the new state.
    let stored = coord
        .db(|storage| storage.list_sections())
        .await
        .ok()
        .and_then(|all| all.into_iter().find(|s| s.name == name));
    Json(SectionResponse {
        section: storage::Section {
            name,
            position: 0,
            collapsed: stored.as_ref().is_some_and(|s| s.collapsed),
            display: stored.map(|s| s.display).unwrap_or_default(),
        },
    })
    .into_response()
}

pub(in crate::coordinator) async fn delete_section_handler(
    State(coord): State<Coordinator>,
    Path(name): Path<String>,
    _admin: AdminSession,
) -> Response {
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
    let delete_name = name.clone();
    match coord
        .db(move |storage| {
            storage.delete_section(&delete_name, chrono::Utc::now().timestamp_millis())
        })
        .await
    {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::NOT_FOUND, "no such section"),
        Err(e) => return internal_error(e),
    }
    if let Err(e) = coord.reload_checks().await {
        return internal_error(e);
    }
    // A tombstone for the section itself, pushed the same way any other
    // delete is.
    match coord
        .write_config(storage::ConfigKind::Section, &name, None)
        .await
    {
        Ok(entry) => coord.replicate_config(vec![entry]),
        Err(e) => {
            tracing::warn!(error = %e, section = %name, "config: failed to push section tombstone")
        }
    }
    for check_id in &affected {
        coord
            .restamp_and_push(storage::ConfigKind::Check, &check_id.to_string())
            .await;
    }
    info!(section = %name, "section deleted, its checks are now loose");
    Json(DeletedResponse { deleted: name }).into_response()
}

pub(in crate::coordinator) async fn reorder_sections_handler(
    State(coord): State<Coordinator>,
    AdminJson(body): AdminJson<ReorderRequest>,
) -> Response {
    let order = body.order;
    let (before, after) = match coord
        .db(move |storage| {
            let before = storage.list_sections()?;
            storage.reorder_sections(&order)?;
            let after = storage.list_sections()?;
            Ok((before, after))
        })
        .await
    {
        Ok(value) => value,
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
            coord
                .restamp_and_push(storage::ConfigKind::Section, &s.name)
                .await;
        }
    }
    sections_response(&coord).await
}
