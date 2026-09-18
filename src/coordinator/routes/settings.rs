//! Admin-editable settings: the fleet display, the notifier list, and the
//! maintenance calendar.
//!
//! **Each is one JSON document in the `settings` table, read through
//! `load_or_seed` and written through `write_config`/`replicate_config`.**
//! `Coordinator::save_agents_display`, `save_notifiers`, and
//! `save_maintenance` all persist the document and stamp it for replication
//! *before* `apply_config_batch` swaps the in-memory copy a handler here
//! reads next — so a failed write never leaves a setting live that a restart
//! would forget, and a peer never learns of a change this node has not
//! itself durably recorded.
//!
//! **Grouped by document, not by concern.** A notifier list and a
//! maintenance calendar have nothing to do with each other, but each pairs a
//! read handler, a replace-the-whole-document write handler, and the
//! validation that write must pass — the same shape three times over. That
//! shape, not the subject matter, is why they share a file instead of each
//! getting one of its own.
//!
//! **Every write here replaces the whole document.** A notifier list and a
//! window calendar are both small and always edited as a whole in the admin
//! form, so neither has a per-item id to address; the alternative would be
//! inventing one for no reader that needs it.

use super::super::{
    alert, error_response, fleet_members, internal_error, Coordinator, FleetMemberDto,
};
use super::session::require_admin;
use crate::models::NodeState;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::info;

/// The fleet summary as the admin page edits it, plus the agent ids it can
/// choose from.
///
/// One call rather than two: the picker offers the agents and regions the rows
/// can name, so a settings response without the fleet is never useful alone.
#[derive(Serialize)]
struct AgentsDisplayResponse {
    agents: crate::config::AgentsDisplay,
    fleet: Vec<FleetMemberDto>,
}

pub(in crate::coordinator) async fn agents_display_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let agents = coord.inner.status_agents.read().clone();
    let fleet = fleet_members(&coord, now).await;
    Json(AgentsDisplayResponse { agents, fleet }).into_response()
}

/// Replace the fleet summary. Admin-only: it decides what an anonymous visitor
/// is told about where monitoring runs from.
pub(in crate::coordinator) async fn update_agents_display_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<crate::config::AgentsDisplay>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let next = body.sanitized();
    if let Err(reason) = next.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    if let Err(e) = coord.save_agents_display(next).await {
        return internal_error(e);
    }
    let now = chrono::Utc::now().timestamp_millis();
    info!("agent display settings saved");
    let agents = coord.inner.status_agents.read().clone();
    let fleet = fleet_members(&coord, now).await;
    Json(AgentsDisplayResponse { agents, fleet }).into_response()
}

// ---- Notifiers ----

#[derive(Deserialize)]
pub(in crate::coordinator) struct NotifiersRequest {
    notifiers: Vec<crate::notify::Notifier>,
}

#[derive(Serialize)]
struct NotifiersResponse {
    notifiers: Vec<crate::notify::Notifier>,
    /// Checks routed to a notifier that no longer exists. They are enabled and
    /// notifying and reach nobody, which is otherwise completely silent — the
    /// same failure `checks_with_no_live_agent` exists to surface for agents.
    /// Usually the aftermath of renaming a notifier.
    unrouted_checks: Vec<String>,
    /// Notifications that have not been delivered: still being retried, or
    /// given up on. Newest first and capped — the panel needs to say "nobody
    /// was paged for this", not paginate a mailbox.
    undelivered: Vec<UndeliveredDto>,
}

#[derive(Serialize)]
struct UndeliveredDto {
    notifier: String,
    check_id: crate::models::CheckId,
    name: String,
    to: &'static str,
    /// Failed attempts so far.
    attempts: i32,
    /// True once the attempts are exhausted: this transition reached nobody and
    /// nothing further will be tried.
    given_up: bool,
    /// When the next attempt is due. Absent once given up.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_attempt: Option<i64>,
    error: String,
    queued_at: i64,
}

/// How many undelivered rows the panel is shown. Enough to read the shape of a
/// broken notifier; the counts beside it are exact.
const UNDELIVERED_SHOWN: usize = 20;

/// The saved list, the checks whose routing no longer resolves, and whatever
/// could not be delivered.
async fn notifiers_response(coord: &Coordinator) -> Response {
    let notifiers = coord.inner.notifiers.read().clone();
    let known: std::collections::HashSet<&str> =
        notifiers.iter().map(|n| n.name.as_str()).collect();
    let checks: std::collections::HashMap<_, _> = coord
        .inner
        .checks
        .read()
        .iter()
        .map(|(id, meta)| {
            (
                *id,
                (
                    meta.name.clone(),
                    meta.enabled,
                    meta.notify,
                    meta.notifiers.clone(),
                ),
            )
        })
        .collect();
    let mut unrouted: Vec<String> = checks
        .values()
        .filter(|(_, enabled, notify, routed)| {
            *enabled
                && *notify
                && !routed.is_empty()
                && !routed.iter().any(|n| known.contains(n.as_str()))
        })
        .map(|(name, _, _, _)| name.clone())
        .collect();
    unrouted.sort();
    let undelivered = coord
        .db(|storage| storage.undelivered_notifications(UNDELIVERED_SHOWN))
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|q| UndeliveredDto {
            notifier: q.notifier,
            name: checks
                .get(&q.check_id)
                .map_or_else(|| q.check_id.to_string(), |m| m.0.clone()),
            check_id: q.check_id,
            to: q.current_state.as_str(),
            attempts: q.attempts,
            given_up: q.next_attempt.is_none(),
            next_attempt: q.next_attempt,
            error: q.last_error,
            queued_at: q.queued_at,
        })
        .collect();
    Json(NotifiersResponse {
        notifiers,
        unrouted_checks: unrouted,
        undelivered,
    })
    .into_response()
}

/// Admin-only: notifier URLs are credentials in practice. A Slack webhook URL
/// is the entire authentication for posting to that channel.
pub(in crate::coordinator) async fn notifiers_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    notifiers_response(&coord).await
}

/// Replace the whole list. The list is small and always edited as a whole in
/// the form, so there is no per-notifier id to address: the name is the
/// identity, which is exactly why it has to be unique.
pub(in crate::coordinator) async fn update_notifiers_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<NotifiersRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let next: Vec<crate::notify::Notifier> = body
        .notifiers
        .into_iter()
        .map(crate::notify::Notifier::sanitized)
        .collect();
    let mut names = std::collections::HashSet::new();
    for n in &next {
        if let Err(reason) = n.validate() {
            return error_response(StatusCode::BAD_REQUEST, &reason);
        }
        // Checks route by name, so two notifiers sharing one would make
        // "send this to oncall" ambiguous.
        if !names.insert(n.name.as_str()) {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!(
                    "two notifiers are called {} — names are what checks route to",
                    n.name
                ),
            );
        }
    }
    if let Err(e) = coord.save_notifiers(next).await {
        return internal_error(e);
    }
    info!(
        count = coord.inner.notifiers.read().len(),
        "notifiers saved"
    );
    notifiers_response(&coord).await
}

// ---- Maintenance windows ----

#[derive(Deserialize)]
pub(in crate::coordinator) struct MaintenanceRequest {
    windows: Vec<crate::maintenance::MaintenanceWindow>,
}

#[derive(Serialize)]
struct MaintenanceResponse {
    windows: Vec<crate::maintenance::MaintenanceWindow>,
    /// Every configured check, so the window editor's picker is one request —
    /// same reason the agent-display settings carry the fleet. Windows store
    /// checks by id; this is what lets the picker show a name for each.
    checks: Vec<CheckOptionDto>,
}

#[derive(Serialize)]
struct CheckOptionDto {
    id: crate::models::CheckId,
    name: String,
}

/// What an anonymous visitor is told about a window: the operator's title and
/// when it is over. Not the affected checks — a window naming eight checks and
/// the eight monitor rows beneath it would read as eight separate outages, the
/// same argument §7 makes for a global incident.
#[derive(Serialize)]
pub(in crate::coordinator) struct MaintenanceNoticeDto {
    title: String,
    starts_at: i64,
    ends_at: i64,
}

/// Announced windows running right now, soonest to end first — the one the
/// visitor is waiting out is the one that matters.
pub(in crate::coordinator) fn announced_maintenance(
    coord: &Coordinator,
    now: i64,
) -> Vec<MaintenanceNoticeDto> {
    let mut notices: Vec<MaintenanceNoticeDto> = coord
        .inner
        .maintenance
        .read()
        .iter()
        .filter(|w| w.announce && w.active_at(now))
        .map(|w| MaintenanceNoticeDto {
            title: w.title.clone(),
            starts_at: w.starts_at,
            ends_at: w.ends_at,
        })
        .collect();
    notices.sort_by_key(|n| n.ends_at);
    notices
}

fn maintenance_response(coord: &Coordinator) -> Response {
    let mut checks: Vec<CheckOptionDto> = coord
        .inner
        .checks
        .read()
        .iter()
        .map(|(id, m)| CheckOptionDto {
            id: *id,
            name: m.name.clone(),
        })
        .collect();
    checks.sort_by(|a, b| a.name.cmp(&b.name));
    Json(MaintenanceResponse {
        windows: coord.inner.maintenance.read().clone(),
        checks,
    })
    .into_response()
}

pub(in crate::coordinator) async fn maintenance_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    maintenance_response(&coord)
}

/// Replace the whole list, like the notifiers: it is small, always edited as a
/// whole, and a window has no identity beyond its span.
pub(in crate::coordinator) async fn update_maintenance_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(body): Json<MaintenanceRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let next: Vec<crate::maintenance::MaintenanceWindow> = body
        .windows
        .into_iter()
        .map(crate::maintenance::MaintenanceWindow::sanitized)
        .collect();
    for w in &next {
        if let Err(reason) = w.validate() {
            return error_response(StatusCode::BAD_REQUEST, &reason);
        }
    }
    if let Err(e) = coord.save_maintenance(next).await {
        return internal_error(e);
    }
    info!(
        count = coord.inner.maintenance.read().len(),
        "maintenance windows saved"
    );
    maintenance_response(&coord)
}

#[derive(Serialize)]
struct NotifierTestResponse {
    delivered: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Send one notification now, to a notifier that need not be saved yet.
///
/// A failed delivery is a 200 carrying `delivered: false`: the request worked,
/// the third party did not, and the form has to tell those apart. Without this
/// a wrong ntfy topic is completely silent — the POST succeeds, ntfy answers
/// 404, and nothing ever looks.
pub(in crate::coordinator) async fn test_notifier_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(notifier): Json<crate::notify::Notifier>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let notifier = notifier.sanitized();
    if let Err(reason) = notifier.validate() {
        return error_response(StatusCode::BAD_REQUEST, &reason);
    }
    // A recovery rather than an outage: a test that pages people with a fake
    // DOWN is a test nobody runs twice.
    let sample = alert::Transition {
        check_id: crate::models::new_check_id(),
        from: NodeState::Unknown,
        to: NodeState::Up,
    };
    let result =
        crate::notify::send(&reqwest::Client::new(), &notifier, &sample, "Warden test").await;
    Json(NotifierTestResponse {
        delivered: result.is_ok(),
        error: result.err(),
    })
    .into_response()
}
