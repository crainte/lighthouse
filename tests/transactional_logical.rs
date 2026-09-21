//! Crash and write-failure boundaries for externally visible logical state.

use std::{net::SocketAddr, time::Duration};

use duckdb::Connection;
use uuid::Uuid;
use warden::{
    config::{CheckConfig, CoordinatorConfig},
    coordinator::Coordinator,
    models::{CheckId, CheckKind, CheckResult, NodeState},
};

const PEER_TOKEN: &str = "transactional-peer-secret";

fn check_id() -> CheckId {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"transactional-logical-check")
}

fn config(path: &str) -> CoordinatorConfig {
    CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: path.into(),
        peer_token: Some(PEER_TOKEN.into()),
        ..Default::default()
    }
}

async fn open(path: &str) -> Coordinator {
    let coord = Coordinator::new(&config(path)).await.unwrap();
    coord
        .create_check_for_test(CheckConfig {
            id: check_id(),
            name: "transactional-logical-check".into(),
            kind: CheckKind::Http,
            target: "https://example.invalid".into(),
            cron: "0 * * * * *".into(),
            timeout_secs: 5,
            ..Default::default()
        })
        .await;
    coord
}

fn result(code: u16, timestamp: i64) -> CheckResult {
    CheckResult {
        check_id: check_id(),
        agent_id: "agent".into(),
        zone: "zone".into(),
        region: "region".into(),
        kind: CheckKind::Http,
        timestamp: chrono::DateTime::from_timestamp_millis(timestamp).unwrap(),
        latency_ms: 1,
        status_code: Some(code),
        body_matched: None,
        error: None,
        failure: None,
        body: None,
        headers: None,
        metric: None,
        metrics: None,
    }
}

async fn serve(coord: Coordinator) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, coord.router()).await;
    });
    (addr, handle)
}

async fn close(handle: tokio::task::JoinHandle<()>, coord: Coordinator) {
    handle.abort();
    drop(coord);
    // The server owns a Coordinator clone until its aborted task unwinds.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

fn database_path() -> String {
    std::env::temp_dir()
        .join(format!("warden-transactional-{}.duckdb", Uuid::now_v7()))
        .to_string_lossy()
        .into_owned()
}

#[tokio::test]
async fn uncommitted_close_reopen_preserves_only_committed_logical_state() {
    let path = database_path();
    let coord = open(&path).await;
    coord.ingest(vec![result(200, 1_000)]).await;
    drop(coord);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Model a process dying between logical writes and COMMIT. These changes
    // deliberately cover both the transition feed and its materialized state.
    let db = Connection::open(&path).unwrap();
    db.execute_batch(&format!(
        "BEGIN;
         INSERT INTO alerts (id, scope, node_id, previous_state, current_state, ts_millis)
         VALUES ('interrupted', 'check', '{id}', 'up', 'down', 2000);
         UPDATE current_state SET status = 'down', last_check = 2000
         WHERE check_id = '{id}';",
        id = check_id()
    ))
    .unwrap();
    drop(db);

    let reopened = open(&path).await;
    assert!(reopened.alerts_for_test(10).await.is_empty());
    reopened.ingest(vec![result(500, 2_000)]).await;
    let alerts = reopened.alerts_for_test(10).await;
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].previous_state, Some(NodeState::Up));
    assert_eq!(alerts[0].current_state, NodeState::Down);
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn gossip_write_failure_does_not_publish_and_can_retry_after_reopen() {
    let path = database_path();
    let coord = open(&path).await;
    coord.ingest(vec![result(200, 1_000)]).await;
    drop(coord);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let db = Connection::open(&path).unwrap();
    db.execute_batch(
        "CREATE UNIQUE INDEX reject_down_alert ON alerts (current_state);
         INSERT INTO alerts (id, scope, node_id, current_state, ts_millis)
         VALUES ('block-down', 'check', 'block', 'down', 1);",
    )
    .unwrap();
    drop(db);

    let coord = open(&path).await;
    let (addr, handle) = serve(coord.clone()).await;
    let body = serde_json::json!({
        "check_id": check_id(), "from": "up", "to": "down", "timestamp": 2_000,
    });
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/internal/gossip"))
        .bearer_auth(PEER_TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        coord.alerts_for_test(10).await.len(),
        1,
        "only the blocker exists"
    );
    close(handle, coord).await;

    let db = Connection::open(&path).unwrap();
    db.execute_batch("DROP INDEX reject_down_alert; DELETE FROM alerts WHERE id = 'block-down';")
        .unwrap();
    drop(db);

    let reopened = open(&path).await;
    let (addr, handle) = serve(reopened.clone()).await;
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/internal/gossip"))
        .bearer_auth(PEER_TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let alerts = reopened.alerts_for_test(10).await;
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].previous_state, Some(NodeState::Up));
    assert_eq!(alerts[0].current_state, NodeState::Down);
    close(handle, reopened).await;
    std::fs::remove_file(path).unwrap();
}
