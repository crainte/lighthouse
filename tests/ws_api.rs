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
    // Real server, in-memory store. "test-check" is configured rather than
    // conjured by ingest alone: the status page only publishes checks that
    // currently exist, and this test is about transitions surfacing, not about
    // how orphaned history is treated.
    // `agent_token: None` does not mean "no token": `Coordinator::new` resolves
    // it from `WARDEN_AGENT_TOKEN`, so on any machine that exports it — which the
    // docker-compose workflow encourages via `.env` — an unauthenticated ingest
    // is rejected and this test fails for a reason unrelated to what it checks.
    // Remove it for this process so the config's `None` is honoured. This test
    // binary holds a single test, so there is no other thread to race with.
    std::env::remove_var("WARDEN_AGENT_TOKEN");
    let coord = Coordinator::new(
        &CoordinatorConfig {
            admin_password: None,
            agent_token: None,
            listen: "127.0.0.1:0".into(),
            db_path: ":memory:".into(),
            alert_webhook: None,
            retention: warden::config::RetentionConfig::default(),
        },
        &[warden::config::CheckConfig {
            name: "test-check".into(),
            target: "https://test-check.invalid".into(),
            ..Default::default()
        }],
    )
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

    // The same ingest that fired the transition also opened an incident, and
    // that lifecycle event is broadcast next. Reading it also proves the
    // baseline was silent — had the baseline wrongly emitted, it would have
    // arrived before the state_change above.
    let opened: Value = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let msg = ws
                .next()
                .await
                .expect("ws stream closed before incident_opened")
                .expect("ws read error");
            if msg.is_text() {
                let txt = msg.into_text().unwrap();
                break serde_json::from_str::<Value>(&txt).expect("incident event is valid JSON");
            }
        }
    })
    .await
    .expect("timed out waiting for incident_opened");
    assert_eq!(opened["type"], "incident_opened", "event type");
    assert_eq!(
        opened["incident"]["check_id"], "test-check",
        "incident check"
    );
    assert_eq!(opened["incident"]["status"], "open", "incident opens");
    assert_eq!(
        opened["incident"]["resolved_at"],
        Value::Null,
        "not yet resolved"
    );
    let started_at = opened["incident"]["started_at"]
        .as_i64()
        .expect("started_at is a timestamp");

    // --- 5. Read APIs: one alert (up->down) and one open incident. ---
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

    let incidents: Value = http
        .get(format!("http://{addr}/api/v1/incidents?limit=50"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mine_inc: Vec<&Value> = incidents["incidents"]
        .as_array()
        .expect("incidents is an array")
        .iter()
        .filter(|i| i["check_id"] == "test-check")
        .collect();
    assert_eq!(mine_inc.len(), 1, "exactly one incident for test-check");
    assert_eq!(mine_inc[0]["status"], "open", "incident is open");
    assert_eq!(
        mine_inc[0]["started_at"], started_at,
        "started_at matches the WS event"
    );

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

    // --- 7. Recovery: 200 -> up closes the incident. Broadcast order for the
    // same ingest is state_change first, then the lifecycle event. ---
    let r = http
        .post(format!("http://{addr}/api/v1/results"))
        .json(&result_body("test-check", "2026-07-11T00:00:02Z", 200))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "recovery ingest should succeed");

    let recovered: Value = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let msg = ws
                .next()
                .await
                .expect("ws stream closed before state_change")
                .expect("ws read error");
            if msg.is_text() {
                let txt = msg.into_text().unwrap();
                break serde_json::from_str::<Value>(&txt).expect("state_change is valid JSON");
            }
        }
    })
    .await
    .expect("timed out waiting for recovery state_change");
    assert_eq!(recovered["type"], "state_change", "event type");
    assert_eq!(recovered["from"], "down", "transition starts at down");
    assert_eq!(recovered["to"], "up", "transition lands on up");

    let resolved: Value = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let msg = ws
                .next()
                .await
                .expect("ws stream closed before incident_resolved")
                .expect("ws read error");
            if msg.is_text() {
                let txt = msg.into_text().unwrap();
                break serde_json::from_str::<Value>(&txt).expect("incident event is valid JSON");
            }
        }
    })
    .await
    .expect("timed out waiting for incident_resolved");
    assert_eq!(resolved["type"], "incident_resolved", "event type");
    assert_eq!(
        resolved["incident"]["check_id"], "test-check",
        "incident check"
    );
    assert_eq!(
        resolved["incident"]["status"], "resolved",
        "incident closes"
    );
    assert_eq!(
        resolved["incident"]["resolved_at"].as_i64(),
        Some(1_783_728_002_000),
        "resolved_at is the recovery observation time"
    );

    // --- 8. Read APIs after recovery: same incident, now resolved. ---
    let incidents: Value = http
        .get(format!(
            "http://{addr}/api/v1/incidents?limit=50&status=resolved"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mine_inc: Vec<&Value> = incidents["incidents"]
        .as_array()
        .expect("incidents is an array")
        .iter()
        .filter(|i| i["check_id"] == "test-check")
        .collect();
    assert_eq!(mine_inc.len(), 1, "the resolved incident is listed");
    assert_eq!(
        mine_inc[0]["started_at"], started_at,
        "same incident, not a new one"
    );
    assert!(
        mine_inc[0]["resolved_at"].as_i64().unwrap() > started_at,
        "closed after it opened"
    );

    // Open-only filter excludes it.
    let open_only: Value = http
        .get(format!(
            "http://{addr}/api/v1/incidents?limit=50&status=open"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let still_open: Vec<&Value> = open_only["incidents"]
        .as_array()
        .expect("incidents is an array")
        .iter()
        .filter(|i| i["check_id"] == "test-check")
        .collect();
    assert!(still_open.is_empty(), "no open incident after recovery");

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
    assert_eq!(mine.len(), 2, "recovery is a second alert, down->up");
    assert_eq!(mine[0]["from"], "down", "newest alert from state");
    assert_eq!(mine[0]["to"], "up", "newest alert to state");

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
    assert_eq!(entry["status"], "up", "latest status is up");
}
