pub mod alert;
pub mod state;
pub mod storage;

use crate::config::CoordinatorConfig;
use crate::models::{default_eval, CheckResult, ResultBatch};
use anyhow::Result;
use axum::{extract::State, routing::post, Json, Router};
use parking_lot::Mutex;
use state::StateTracker;
use std::sync::Arc;
use storage::Storage;
use tracing::info;

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
}

impl Coordinator {
    pub fn new(cfg: &CoordinatorConfig) -> Result<Self> {
        let storage = Storage::open(&cfg.db_path)?;
        Ok(Self {
            inner: Arc::new(Inner {
                storage: Mutex::new(storage),
                tracker: Mutex::new(StateTracker::new()),
                http: reqwest::Client::new(),
                alert_webhook: cfg.alert_webhook.clone(),
            }),
        })
    }

    /// Core ingest path: eval each result, detect transitions, persist, alert.
    /// Used by both the HTTP endpoint and the in-process standalone wiring.
    pub async fn ingest(&self, results: Vec<CheckResult>) {
        let mut rows = Vec::with_capacity(results.len());
        let mut transitions = Vec::new();

        {
            let mut tracker = self.inner.tracker.lock();
            for r in results {
                let status = default_eval(&r);
                if let Some(t) = tracker.observe(&r.check_id, status) {
                    transitions.push(t);
                }
                rows.push((r, status));
            }
        } // drop tracker guard before any await

        if let Err(e) = self.inner.storage.lock().insert_results(&rows) {
            tracing::error!(error = %e, "failed to persist results");
        }

        if let Some(url) = &self.inner.alert_webhook {
            for t in transitions.iter().filter(|t| alert::should_alert(t)) {
                alert::send_webhook(&self.inner.http, url, t).await;
            }
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/api/v1/results", post(ingest_handler))
            .with_state(self)
    }
}

async fn ingest_handler(State(coord): State<Coordinator>, Json(batch): Json<ResultBatch>) {
    let n = batch.results.len();
    coord.ingest(batch.results).await;
    info!(accepted = n, "ingested results");
}
