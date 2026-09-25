//! Shared fixtures for the admin-API integration tests split by feature area:
//! `admin_checks.rs`, `admin_incidents.rs`, `admin_sections.rs`, and
//! `admin_settings.rs`. Originally one file (`admin_api.rs`); kept as fixtures
//! here instead of duplicated per file so the four suites stay in sync.
#![allow(dead_code)] // not every helper is used by every split file

use std::net::SocketAddr;

use serde_json::{json, Value};
use uuid::Uuid;

use warden::config::CoordinatorConfig;
use warden::coordinator::Coordinator;
use warden::models::CheckId;

pub const PASSWORD: &str = "correct horse battery staple";
/// A provisioned agent credential. Agents authenticate only with these now —
/// there is no fleet-wide token — so the fixture mints one up front and every
/// agent-facing call below presents it.
pub const AGENT_TOKEN: &str = "wac_test.agent-shared-secret";

pub fn cid(name: &str) -> CheckId {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes())
}

pub fn spawn_server(coord: Coordinator) -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    // Login rate limiting needs the peer address, so the test serves the router
    // the same way main.rs does.
    let app = coord
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// Provision one credential per zone and hand back the token for each.
///
/// This is the real provisioning model — location is bound to the credential —
/// so a test that wants a multi-zone fleet has to hold a token per zone rather
/// than declaring `?zone=` on the request.
pub async fn zone_tokens(
    coord: &Coordinator,
    zones: &[(&str, &str)],
) -> std::collections::HashMap<String, String> {
    let mut tokens = std::collections::HashMap::new();
    for (zone, region) in zones {
        let token = format!("wac_{}.secret", zone.replace('-', "_"));
        coord
            .provision_agent_credential(&token, zone, region)
            .await
            .unwrap();
        tokens.insert((*zone).to_string(), token);
    }
    tokens
}

pub async fn coordinator() -> Coordinator {
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        admin_password: Some(PASSWORD.into()),
        peer_token: Some("test-peer-token".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    coord
        .provision_agent_credential(AGENT_TOKEN, "test-zone", "test-region")
        .await
        .unwrap();
    coord
}

pub fn check_body(name: &str, target: &str) -> Value {
    json!({
        "name": name,
        "type": "http",
        "target": target,
        "cron": "*/30 * * * * *",
        "timeout_secs": 5,
        "group": "Probes",
        "latency_graph": true,
        "enabled": true,
        "failures_before_down": 1,
    })
}

/// Log in and return the `Cookie:` header value for the session.
///
/// Sent explicitly rather than via a cookie jar so the test exercises the exact
/// header a browser would send, and so a missing `Set-Cookie` fails here.
pub async fn login(base: &str) -> String {
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&json!({ "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "login should succeed");
    let set = res
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .expect("login must set a session cookie")
        .to_str()
        .unwrap();
    assert!(
        set.contains("HttpOnly"),
        "the session cookie must be HttpOnly so page scripts cannot read it: {set}"
    );
    assert!(
        set.contains("SameSite=Strict"),
        "the session cookie must be SameSite=Strict: {set}"
    );
    set.split(';').next().unwrap().to_string()
}

/// Build a coordinator whose checks carry `depends_on` / `failures_before_down`
/// as configured, so the reconciler has a real graph to read.
pub async fn coordinator_with(checks: Vec<warden::config::CheckConfig>) -> Coordinator {
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        admin_password: Some(PASSWORD.into()),
        ..Default::default()
    })
    .await
    .unwrap();
    for check in checks {
        coord.create_check_for_test(check).await;
    }
    coord
        .provision_agent_credential(AGENT_TOKEN, "test-zone", "test-region")
        .await
        .unwrap();
    coord
}

pub fn http_check(name: &str) -> warden::config::CheckConfig {
    warden::config::CheckConfig {
        id: cid(name),
        name: name.into(),
        target: format!("http://{name}.test/"),
        ..Default::default()
    }
}

pub fn probe(check: &str, ts: i64, code: u16) -> warden::models::CheckResult {
    warden::models::CheckResult {
        check_id: cid(check),
        agent_id: "agent-1".into(),
        zone: "test-zone".into(),
        region: "test-region".into(),
        kind: warden::models::CheckKind::Http,
        timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
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

/// Open one incident by hand and hand back its id. Manual rather than
/// probe-driven because the delete path does not care how it was opened, and
/// driving it through ingest would make the test about reconciliation instead.
pub async fn create_incident(base: &str, session: &str, title: &str, checks: &[String]) -> String {
    let res: Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/incidents"))
        .header(reqwest::header::COOKIE, session)
        .json(&json!({ "title": title, "checks": checks, "body": "opened by hand" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    res["incident"]["id"].as_str().unwrap().to_string()
}

pub async fn public_incident_ids(base: &str) -> Vec<String> {
    let listed: Value = reqwest::get(format!("{base}/api/v1/incidents?limit=100"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    listed["incidents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap().to_string())
        .collect()
}
