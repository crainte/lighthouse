pub mod alert;
pub mod state;
pub mod storage;

use crate::config::CoordinatorConfig;
use crate::models::{default_eval, CheckResult, ResultBatch, Status};
use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use state::StateTracker;
use std::sync::Arc;
use storage::Storage;
use tokio::sync::broadcast;
use tracing::info;

/// A live state-change event, broadcast to WebSocket clients and matching the
/// persisted alert shape. Serialized form matches the dashboard API contract.
#[derive(Debug, Clone, Serialize)]
pub struct StateEvent {
    #[serde(rename = "type")]
    pub kind: &'static str, // always "state_change"
    pub check_id: String,
    pub from: Status,
    pub to: Status,
    pub timestamp: i64,
}

/// Shared coordinator state for axum handlers.
#[derive(Clone)]
pub struct Coordinator {
    inner: Arc<Inner>,
}

struct Inner {
    storage: Mutex<Storage>,
    tracker: Mutex<StateTracker>,
    http: reqwest::Client,
    alert_webhook: Option<String>,
    events: broadcast::Sender<StateEvent>,
}

impl Coordinator {
    pub fn new(cfg: &CoordinatorConfig) -> Result<Self> {
        let storage = Storage::open(&cfg.db_path)?;
        // Seed mock data so the dashboard has something to show on first run.
        let now = chrono::Utc::now().timestamp_millis();
        if storage.seed_mock(now)? {
            info!("seeded mock data (empty store)");
        }
        let (events, _) = broadcast::channel(256);
        Ok(Self {
            inner: Arc::new(Inner {
                storage: Mutex::new(storage),
                tracker: Mutex::new(StateTracker::new()),
                http: reqwest::Client::new(),
                alert_webhook: cfg.alert_webhook.clone(),
                events,
            }),
        })
    }

    /// Core ingest path: eval each result, detect transitions, persist, alert,
    /// and broadcast alert-worthy transitions to live WebSocket clients.
    pub async fn ingest(&self, results: Vec<CheckResult>) {
        let mut rows = Vec::with_capacity(results.len());
        let mut transitions = Vec::new();

        {
            let mut tracker = self.inner.tracker.lock();
            for r in results {
                let status = default_eval(&r);
                if let Some(t) = tracker.observe(&r.check_id, status) {
                    let ts = r.timestamp.timestamp_millis();
                    transitions.push((t, ts));
                }
                rows.push((r, status));
            }
        } // drop tracker guard before any await

        {
            let store = self.inner.storage.lock();
            if let Err(e) = store.insert_results(&rows) {
                tracing::error!(error = %e, "failed to persist results");
            }
            for (t, ts) in transitions.iter().filter(|(t, _)| alert::should_alert(t)) {
                if let Err(e) = store.insert_alert(&t.check_id, t.from, t.to, *ts) {
                    tracing::error!(error = %e, "failed to persist alert");
                }
            }
        } // drop storage guard before any await

        for (t, ts) in transitions.into_iter().filter(|(t, _)| alert::should_alert(t)) {
            // Broadcast to WS clients (ignore if no subscribers).
            let _ = self.inner.events.send(StateEvent {
                kind: "state_change",
                check_id: t.check_id.clone(),
                from: t.from,
                to: t.to,
                timestamp: ts,
            });
            if let Some(url) = &self.inner.alert_webhook {
                alert::send_webhook(&self.inner.http, url, &t).await;
            }
        }
    }

    /// Read recent history for a check (newest first).
    pub fn history(&self, check_id: &str, limit: usize) -> Result<Vec<storage::HistoryRow>> {
        self.inner.storage.lock().history(check_id, limit)
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/api/v1/results", post(ingest_handler))
            .route("/api/v1/status", get(status_handler))
            .route("/api/v1/checks/:id/history", get(history_handler))
            .route("/api/v1/alerts", get(alerts_handler))
            .route("/api/v1/ws", get(ws_handler))
            .with_state(self)
    }
}

// ---- API response DTOs (match warden-api-contract.md) ----

#[derive(Serialize)]
struct StatusResponse {
    checks: Vec<CheckStatusDto>,
}

#[derive(Serialize)]
struct CheckStatusDto {
    check_id: String,
    status: Status,
    latency_ms: u64,
    last_check: i64,
    uptime_24h: f64,
}

#[derive(Serialize)]
struct HistoryResponse {
    check_id: String,
    history: Vec<HistoryDto>,
}

#[derive(Serialize)]
struct HistoryDto {
    timestamp: i64,
    status: Status,
    latency_ms: u64,
}

#[derive(Serialize)]
struct AlertsResponse {
    alerts: Vec<AlertDto>,
}

#[derive(Serialize)]
struct AlertDto {
    check_id: String,
    from: Status,
    to: Status,
    timestamp: i64,
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<usize>,
}

// ---- Handlers ----

async fn ingest_handler(State(coord): State<Coordinator>, Json(batch): Json<ResultBatch>) {
    let n = batch.results.len();
    coord.ingest(batch.results).await;
    info!(accepted = n, "ingested results");
}

async fn status_handler(State(coord): State<Coordinator>) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let rows = coord.inner.storage.lock().list_status(now);
    match rows {
        Ok(rows) => {
            let checks = rows
                .into_iter()
                .map(|r| CheckStatusDto {
                    check_id: r.check_id,
                    status: r.status,
                    latency_ms: r.latency_ms,
                    last_check: r.last_check,
                    uptime_24h: r.uptime_24h,
                })
                .collect();
            Json(StatusResponse { checks }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

async fn history_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(100).min(1000);
    match coord.history(&id, limit) {
        Ok(rows) => {
            let history = rows
                .into_iter()
                .map(|r| HistoryDto {
                    timestamp: r.timestamp,
                    status: r.status,
                    latency_ms: r.latency_ms,
                })
                .collect();
            Json(HistoryResponse { check_id: id, history }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

async fn alerts_handler(State(coord): State<Coordinator>, Query(q): Query<LimitQuery>) -> Response {
    let limit = q.limit.unwrap_or(50).min(1000);
    match coord.inner.storage.lock().recent_alerts(limit) {
        Ok(rows) => {
            let alerts = rows
                .into_iter()
                .map(|r| AlertDto {
                    check_id: r.check_id,
                    from: r.from,
                    to: r.to,
                    timestamp: r.timestamp,
                })
                .collect();
            Json(AlertsResponse { alerts }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

async fn ws_handler(State(coord): State<Coordinator>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| handle_ws(socket, coord))
}

async fn handle_ws(mut socket: WebSocket, coord: Coordinator) {
    let mut rx = coord.inner.events.subscribe();
    loop {
        match rx.recv().await {
            Ok(ev) => {
                let txt = match serde_json::to_string(&ev) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                if socket.send(Message::Text(txt)).await.is_err() {
                    break; // client gone
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn internal_error(e: anyhow::Error) -> Response {
    tracing::error!(error = %e, "request failed");
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}
