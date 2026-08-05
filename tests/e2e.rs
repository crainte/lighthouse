//! End-to-end smoke test for the Warden pipeline.
//!
//! Exercises the real code paths against live localhost servers:
//!   agent::execute_check (real HTTP GET)
//!     -> Coordinator::ingest (eval -> transition detect -> persist -> alert)
//!       -> Coordinator::history (DuckDB read-back)
//!       -> webhook POST (real HTTP)
//!
//! No internal types are mocked; only the two localhost HTTP servers are
//! stand-ins for a real target and a real alert sink.

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{extract::State, Json, Router};
use parking_lot::Mutex;
use serde_json::Value;

use warden::agent::{execute_check, Probes};
use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::models::{CheckKind, Status};

/// Bind an ephemeral 127.0.0.1 port and serve `app`. Returns the bound addr.
/// The listener is bound before we hand it to `serve`, so the OS accepts
/// connections into the backlog immediately — no readiness sleep required.
async fn serve(app: Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// Webhook capture handler: record each posted JSON body.
async fn capture(
    State(sink): State<Arc<Mutex<Vec<Value>>>>,
    Json(body): Json<Value>,
) -> StatusCode {
    sink.lock().push(body);
    StatusCode::OK
}

/// Poll the capture sink until at least one alert lands (or time out).
/// The webhook POST is awaited inside `ingest`, so this usually returns on the
/// first iteration; the loop just guards against scheduler jitter without a
/// fixed long sleep.
async fn wait_for_alerts(sink: &Arc<Mutex<Vec<Value>>>) -> Vec<Value> {
    for _ in 0..200 {
        {
            let g = sink.lock();
            if !g.is_empty() {
                return g.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("no alert captured within timeout");
}

fn check(target: String) -> CheckConfig {
    // Same `name` for both routes => same check_id => transition is detectable.
    CheckConfig {
        name: "web".into(),
        kind: CheckKind::Http,
        target,
        cron: "0 * * * * *".into(),
        timeout_secs: 5,
        ..Default::default()
    }
}

/// A single-agent test fleet: identity plus the location every result carries.
fn origin() -> warden::agent::Origin<'static> {
    warden::agent::Origin {
        agent_id: "agent-01",
        zone: "test-zone",
        region: "test-region",
    }
}

#[tokio::test]
async fn agent_check_through_coordinator_fires_alert_on_up_to_down() {
    // (a) Target server: 200 on /ok, 500 on /err.
    let target_addr = serve(
        Router::new()
            .route("/ok", get(|| async { StatusCode::OK }))
            .route("/err", get(|| async { StatusCode::INTERNAL_SERVER_ERROR })),
    )
    .await;

    // (b) Webhook capture server: records POST bodies into a shared vec.
    let alerts: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let webhook_addr = serve(
        Router::new()
            .route("/", post(capture))
            .with_state(alerts.clone()),
    )
    .await;

    let coord = Coordinator::new(
        &CoordinatorConfig {
            admin_password: None,
            agent_token: None,
            listen: "127.0.0.1:0".into(),
            db_path: ":memory:".into(),
            alert_webhook: Some(format!("http://{webhook_addr}/")),
        },
        &[],
    )
    .unwrap();

    let probes = Probes::new().unwrap();

    // --- Contract 1: 200 route maps to status_code=Some(200), error=None ---
    let up = execute_check(
        &probes,
        &origin(),
        &check(format!("http://{target_addr}/ok")),
    )
    .await;
    assert_eq!(
        up.status_code,
        Some(200),
        "healthy route must report HTTP 200"
    );
    assert_eq!(up.error, None, "healthy route must not surface an error");

    // Baseline ingest: evals to Up, first observation => no transition => silent.
    coord.ingest(vec![up]).await;

    // 2ms gap so the 500 result gets a strictly-later ts_millis, making the
    // DESC history ordering deterministic. This is a clock-separation guarantee,
    // not a "wait for it to settle" race.
    tokio::time::sleep(Duration::from_millis(2)).await;

    // --- Contract 2: 500 route maps to status_code=Some(500) ---
    let down = execute_check(
        &probes,
        &origin(),
        &check(format!("http://{target_addr}/err")),
    )
    .await;
    assert_eq!(
        down.status_code,
        Some(500),
        "failing route must report HTTP 500"
    );

    // Transition ingest: evals to Down, Up->Down edge => alert must fire.
    coord.ingest(vec![down]).await;

    // --- Contract 3: both results persisted, newest-first [Down, Up] ---
    let hist = coord.history("web", 10, None, None, None).unwrap();
    assert_eq!(hist.len(), 2, "both ingested results must be persisted");
    assert_eq!(
        hist[0].status,
        Status::Down,
        "newest row is the 500 => Down"
    );
    assert_eq!(hist[1].status, Status::Up, "older row is the 200 => Up");

    // --- Contract 4: exactly one alert for the Up->Down edge, carrying check_id ---
    let got = wait_for_alerts(&alerts).await;
    assert_eq!(
        got.len(),
        1,
        "baseline is silent; only the Up->Down edge alerts"
    );
    assert_eq!(
        got[0]["check_id"], "web",
        "alert payload references the check_id"
    );
    assert_eq!(got[0]["from"], "up", "transition edge starts at up");
    assert_eq!(got[0]["to"], "down", "transition edge lands on down");
}
