//! Integration test for the dashboard live path: a real ingested state
//! transition must (a) broadcast to a connected WebSocket client and (b)
//! surface in the read APIs (/api/v1/alerts, /api/v1/status).
//!
//! Everything runs against the real Coordinator router over a real localhost
//! socket — only the network boundary is localhost, no internal types are
//! mocked or reached into. Request bodies are hand-built JSON; responses are
//! navigated as serde_json::Value, so the test is coupled to the wire contract,
//! not to Rust internals.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_tungstenite::connect_async;

use warden::config::CoordinatorConfig;
use warden::coordinator::Coordinator;

/// Bind an ephemeral 127.0.0.1 port, read the port BEFORE serving, then serve
/// the router in a spawned task. The listener is bound before `serve`, so the
/// OS accepts into the backlog immediately.
fn spawn_server(coord: Coordinator) -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let app = coord.router();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// A request JSON for the ingest endpoint carrying one CheckResult.
/// `status_code` drives default_eval: 200 -> up, 500 -> down.
fn result_body(check_id: &str, ts: &str, status_code: u16) -> Value {
    json!({
        "results": [{
            "check_id": check_id,
            "agent_id": "agent-test",
            "timestamp": ts,
            "latency_ms": 10,
            "status_code": status_code,
        }]
    })
}

#[tokio::test]
async fn transition_broadcasts_over_ws_and_surfaces_in_read_api() {
    // Real server, in-memory store. `Coordinator::new` seeds mock data on an
    // empty store; we assert only on a fresh check_id ("test-check") so seed
    // data (api-gateway/redis-cache/...) never interferes.
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        alert_webhook: None,
    })
    .unwrap();
    let addr = spawn_server(coord);

    let http = reqwest::Client::new();

    // --- 1. Subscribe a WS client BEFORE ingesting (retry the very first
    // connect while the accept loop schedules). ---
    let ws_url = format!("ws://{addr}/api/v1/ws");
    let mut ws = {
        let mut connected = None;
        for _ in 0..50 {
            match connect_async(&ws_url).await {
                Ok((stream, _resp)) => {
                    connected = Some(stream);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
        connected.expect("ws client failed to connect")
    };

    // ponytail: subscription readiness barrier. `handle_ws` calls
    // `events.subscribe()` inside axum's post-upgrade task, which runs AFTER
    // connect_async returns. There is no server-side hello to wait on and the
    // impl is off-limits, so a short sleep is the only available barrier. A
    // broadcast sent before subscribe() lands would be silently dropped.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // --- 2. Baseline: 200 -> up. First observation => no transition =>
    // must NOT broadcast and must NOT create an alert. ---
    let r = http
        .post(format!("http://{addr}/api/v1/results"))
        .json(&result_body("test-check", "2026-07-11T00:00:00Z", 200))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "baseline ingest should succeed");

    // --- 3. Transition: 500 -> down. Up->Down edge => broadcast + alert. ---
    let r = http
        .post(format!("http://{addr}/api/v1/results"))
        .json(&result_body("test-check", "2026-07-11T00:00:01Z", 500))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "transition ingest should succeed");

    // --- 4. Exactly one state_change over WS: test-check up->down. ---
    // If the baseline wrongly emitted, that event was sent first and this read
    // would surface it (to=="up"), failing the assertions below.
    let event: Value = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let msg = ws
                .next()
                .await
                .expect("ws stream closed before any state_change")
                .expect("ws read error");
            if msg.is_text() {
                let txt = msg.into_text().unwrap();
                break serde_json::from_str::<Value>(&txt).expect("state_change is valid JSON");
            }
            // ignore protocol frames (ping/pong)
        }
    })
    .await
    .expect("timed out waiting for a state_change broadcast");

    assert_eq!(event["type"], "state_change", "event type");
    assert_eq!(event["check_id"], "test-check", "event check_id");
    assert_eq!(event["from"], "up", "transition starts at up");
    assert_eq!(event["to"], "down", "transition lands on down");

    // No second broadcast: proves the baseline was silent and the transition
    // fired exactly once. A text frame arriving here fails the test.
    let extra = tokio::time::timeout(Duration::from_millis(400), async {
        loop {
            match ws.next().await {
                Some(Ok(m)) if m.is_text() => break Some(m.into_text().unwrap()),
                Some(Ok(_)) => continue, // ignore ping/pong
                _ => break None,         // stream closed
            }
        }
    })
    .await;
    assert!(
        extra.is_err(),
        "expected no further ws message (baseline must be silent, transition fires once); got {extra:?}"
    );

    // --- 5. GET /api/v1/alerts: exactly one alert for test-check, up->down. ---
    let alerts: Value = http
        .get(format!("http://{addr}/api/v1/alerts?limit=50"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mine: Vec<&Value> = alerts["alerts"]
        .as_array()
        .expect("alerts is an array")
        .iter()
        .filter(|a| a["check_id"] == "test-check")
        .collect();
    assert_eq!(
        mine.len(),
        1,
        "exactly one alert for test-check (baseline must not alert)"
    );
    assert_eq!(mine[0]["from"], "up", "alert from state");
    assert_eq!(mine[0]["to"], "down", "alert to state");

    // --- 6. GET /api/v1/status: test-check reflects the transition (down). ---
    let status: Value = http
        .get(format!("http://{addr}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entry = status["checks"]
        .as_array()
        .expect("checks is an array")
        .iter()
        .find(|c| c["check_id"] == "test-check")
        .expect("test-check present in status");
    assert_eq!(entry["status"], "down", "latest status is down");
}
