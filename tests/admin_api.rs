//! Integration test for the check editor and the auth around it.
//!
//! The security claim being defended is narrow and load-bearing: the admin API
//! mutates what gets probed and exposes probe targets, so it must be unusable
//! without a session — and probe targets must never appear on the public status
//! endpoint. Everything runs against the real router over a real localhost
//! socket, driven as JSON over the wire rather than through Rust internals.

use std::net::SocketAddr;

use serde_json::{json, Value};
use uuid::Uuid;

use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::models::{CheckId, NodeState};
use warden::notify::{Notifier, NotifierKind};

const PASSWORD: &str = "correct horse battery staple";
/// A provisioned agent credential. Agents authenticate only with these now —
/// there is no fleet-wide token — so the fixture mints one up front and every
/// agent-facing call below presents it.
const AGENT_TOKEN: &str = "wac_test.agent-shared-secret";

fn cid(name: &str) -> CheckId {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes())
}

fn spawn_server(coord: Coordinator) -> SocketAddr {
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
async fn zone_tokens(
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

async fn coordinator() -> Coordinator {
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

fn check_body(name: &str, target: &str) -> Value {
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
async fn login(base: &str) -> String {
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

/// "Nothing was measured" and "nothing was up" must not be the same answer.
/// A check configured here but never observed — placed in another region, or
/// simply new — reports `uptime_24h: null`; one that *was* observed reports a
/// number. Reporting 0.0 for the first case ranks a check nobody was asked to
/// watch alongside a total outage.
#[tokio::test]
async fn an_unobserved_check_reports_null_uptime_rather_than_zero() {
    let coord = coordinator().await;
    let addr = spawn_server(coord);
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let session = login(&base).await;

    let created = client
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("never-probed", "https://example.com/a"))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let observed = client
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("probed", "https://example.com/b"))
        .send()
        .await
        .unwrap();
    let observed_id = observed.json::<Value>().await.unwrap()["check"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&json!({"results": [{
            "check_id": observed_id,
            "agent_id": "agent-test",
            "timestamp": "2026-01-01T00:00:00Z",
            "latency_ms": 12,
            "status_code": 200,
        }]}))
        .send()
        .await
        .unwrap();

    let status: Value = client
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let by_name = |name: &str| -> Value {
        status["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("{name} should be on the status page: {status}"))
            .clone()
    };
    assert!(
        by_name("never-probed")["uptime_24h"].is_null(),
        "an unobserved check must report null uptime: {status}"
    );
    assert!(
        by_name("probed")["uptime_24h"].is_f64(),
        "an observed check must report a number: {status}"
    );
}

#[tokio::test]
async fn http_security_headers_and_cookie_policy_are_applied() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let status = client
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap();
    let headers = status.headers();
    assert_eq!(
        headers["content-security-policy"],
        "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self' ws: wss:"
    );
    assert_eq!(headers["x-frame-options"], "DENY");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert_eq!(
        headers["referrer-policy"],
        "strict-origin-when-cross-origin"
    );

    // A client can forge this header; it must not make cookies Secure.
    let login = client
        .post(format!("{base}/api/v1/login"))
        .header("X-Forwarded-Proto", "https")
        .json(&json!({ "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert!(!login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .contains("Secure"));

    let secure = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        admin_password: Some(PASSWORD.into()),
        secure_cookies: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let secure_addr = spawn_server(secure);
    let secure_login = client
        .post(format!("http://{secure_addr}/api/v1/login"))
        .json(&json!({ "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert!(secure_login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .contains("Secure"));
}

#[tokio::test]
async fn oversized_login_and_admin_json_are_rejected_before_and_after_authentication() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let login_oversized = client
        .post(format!("{base}/api/v1/login"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(vec![b'x'; 16 * 1024 + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(login_oversized.status(), 413);

    let oversized = vec![b'x'; 1024 * 1024 + 1];
    let anonymous = client
        .post(format!("{base}/api/v1/admin/eval-test"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(oversized.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 413);

    let session = login(&base).await;
    let authenticated = client
        .post(format!("{base}/api/v1/admin/eval-test"))
        .header(reqwest::header::COOKIE, session)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(oversized)
        .send()
        .await
        .unwrap();
    assert_eq!(authenticated.status(), 413);
}

#[tokio::test]
async fn the_check_editor_is_unusable_without_a_session() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let anon = reqwest::Client::new();

    // --- Every admin route refuses an anonymous caller. ---
    for (method, path) in [
        ("GET", "/api/v1/admin/checks"),
        ("POST", "/api/v1/admin/checks"),
        ("PUT", "/api/v1/admin/checks/whatever"),
        ("DELETE", "/api/v1/admin/checks/whatever"),
    ] {
        let req = match method {
            "GET" => anon.get(format!("{base}{path}")),
            "POST" => anon
                .post(format!("{base}{path}"))
                .json(&check_body("x", "https://x.test")),
            "PUT" => anon
                .put(format!("{base}{path}"))
                .json(&check_body("x", "https://x.test")),
            _ => anon.delete(format!("{base}{path}")),
        };
        let res = req.send().await.unwrap();
        assert_eq!(res.status(), 401, "{method} {path} must require a session");
        let body: Value = res.json().await.unwrap();
        assert_eq!(body["error"], "authentication required");
    }

    // Session probing is public and must never 401 — the page uses it to decide
    // whether to offer the admin UI at all.
    let res = anon
        .get(format!("{base}/api/v1/session"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.json::<Value>().await.unwrap()["authenticated"], false);

    // --- A wrong password grants nothing. ---
    let res = anon
        .post(format!("{base}/api/v1/login"))
        .json(&json!({ "password": "hunter2" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
    let res = anon
        .get(format!("{base}/api/v1/admin/checks"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401, "a failed login must not leave a session");
}

#[tokio::test]
async fn an_authenticated_admin_can_round_trip_a_check() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    // --- Create. ---
    let res = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("docs-site", "https://docs.test/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let created: Value = res.json().await.unwrap();
    assert_eq!(created["check"]["target"], "https://docs.test/health");
    let check_id = created["check"]["id"].as_str().unwrap();

    // A duplicate name is a conflict, not a silent overwrite.
    let res = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("docs-site", "https://other.test"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 409);

    // --- Update. ---
    let mut edited = check_body("docs-site", "https://docs.test/healthz");
    edited["enabled"] = json!(false);
    let res = admin
        .put(format!("{base}/api/v1/admin/checks/{check_id}"))
        .header(reqwest::header::COOKIE, &session)
        .json(&edited)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let listed: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = listed["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "docs-site")
        .expect("the edited check is listed");
    assert_eq!(row["target"], "https://docs.test/healthz");
    assert_eq!(
        row["enabled"], false,
        "a disabled check is kept, not dropped"
    );

    // --- Agents receive targets, but only the checks they should run. ---
    let agent = reqwest::Client::new();
    let feed: Value = agent
        .get(format!("{base}/api/v1/agent/checks"))
        .bearer_auth(AGENT_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !feed["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["name"] == "docs-site"),
        "a disabled check must not be handed to agents"
    );

    // --- Delete. ---
    let res = admin
        .delete(format!("{base}/api/v1/admin/checks/{check_id}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let res = admin
        .delete(format!("{base}/api/v1/admin/checks/{check_id}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404, "deleting twice is not a success");

    // --- Logging out revokes immediately. ---
    admin
        .post(format!("{base}/api/v1/logout"))
        .send()
        .await
        .unwrap();
    let res = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
}

#[tokio::test]
async fn unplacing_a_check_keeps_history_for_replacement_and_clears_local_state() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let created: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("regional-web", "https://regional.test/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let check_id = created["check"]["id"].as_str().unwrap().to_string();

    let result = |timestamp: &str| {
        json!({"results": [{
            "check_id": check_id,
            "agent_id": "agent-test",
            "timestamp": timestamp,
            "latency_ms": 12,
            "status_code": 200,
        }]})
    };
    let res = admin
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&result("2026-01-01T00:00:00Z"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);

    let res = admin
        .delete(format!("{base}/api/v1/admin/checks/{check_id}/unplace"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.json::<Value>().await.unwrap()["unplaced"], check_id);

    let checks: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(checks["checks"].as_array().unwrap().is_empty());
    let status: Value = reqwest::get(format!("{base}/api/v1/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(status["checks"].as_array().unwrap().is_empty());

    let mut replacement = check_body("regional-web", "https://regional.test/health");
    replacement["id"] = json!(check_id);
    let res = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .bearer_auth("test-peer-token")
        .json(&replacement)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.json::<Value>().await.unwrap()["check"]["id"], check_id);

    let history: Value = reqwest::get(format!("{base}/api/v1/checks/{check_id}/history?limit=10"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(history["history"].as_array().unwrap().len(), 1);

    let res = admin
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&result("2026-01-01T00:01:00Z"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    let status: Value = reqwest::get(format!("{base}/api/v1/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["checks"].as_array().unwrap().len(), 1);
    assert_eq!(status["checks"][0]["check_id"], check_id);
}

#[tokio::test]
async fn invalid_checks_are_refused_with_a_readable_reason() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    // Each of these would produce a check that cannot work, so it must be caught
    // at the boundary rather than failing silently every interval forever.
    let cases = [
        (
            json!({"name": "", "type": "http", "target": "https://a.test", "cron": "*/30 * * * * *", "timeout_secs": 5}),
            "name",
        ),
        (
            json!({"name": "bad/name", "type": "http", "target": "https://a.test", "cron": "*/30 * * * * *", "timeout_secs": 5}),
            "slash",
        ),
        (
            json!({"name": "a", "type": "http", "target": "", "cron": "*/30 * * * * *", "timeout_secs": 5}),
            "target",
        ),
        (
            json!({"name": "a", "type": "http", "target": "example.com", "cron": "*/30 * * * * *", "timeout_secs": 5}),
            "http://",
        ),
        (
            json!({"name": "a", "type": "tcp", "target": "hostonly", "cron": "*/30 * * * * *", "timeout_secs": 5}),
            "host:port",
        ),
        // A one-second schedule is a load test, not monitoring.
        (
            json!({"name": "a", "type": "http", "target": "https://a.test", "cron": "* * * * * *", "timeout_secs": 1}),
            "5 seconds",
        ),
        (
            json!({"name": "a", "type": "http", "target": "https://a.test", "cron": "*/30 * * * * *", "timeout_secs": 60}),
            "timeout",
        ),
        (
            json!({"name": "a", "type": "dns", "target": "https://a.test/x", "cron": "*/30 * * * * *", "timeout_secs": 5}),
            "hostname",
        ),
    ];
    for (body, expected_fragment) in cases {
        let res = admin
            .post(format!("{base}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &session)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 400, "should reject {body}");
        let err = res.json::<Value>().await.unwrap()["error"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            err.contains(expected_fragment),
            "error {err:?} should mention {expected_fragment:?} for {body}"
        );
    }

    // Names are mutable labels; the UUID route preserves the check's history.
    let keeper: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("keeper", "https://keeper.test"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let keeper_id = keeper["check"]["id"].as_str().unwrap();
    let res = admin
        .put(format!("{base}/api/v1/admin/checks/{keeper_id}"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("renamed", "https://keeper.test"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.json::<Value>().await.unwrap()["check"]["name"],
        "renamed"
    );
}

#[tokio::test]
async fn probe_targets_never_reach_the_public_api() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;
    let created: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body(
            "internal",
            "https://vault.internal.example/health",
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let internal_id = &created["check"]["id"];

    // Report a result so the check appears on the status page at all.
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&json!({"results": [{
            "check_id": internal_id,
            "agent_id": "agent-test",
            "timestamp": "2026-01-01T00:00:00Z",
            "latency_ms": 12,
            "status_code": 200,
        }]}))
        .send()
        .await
        .unwrap();

    let public = reqwest::Client::new()
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        public.contains("internal"),
        "the check itself should be public: {public}"
    );
    assert!(
        !public.contains("vault.internal.example"),
        "the probe target must not appear on the public status endpoint: {public}"
    );
}

#[tokio::test]
async fn agents_must_present_the_token_for_targets_and_ingest() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let anon = reqwest::Client::new();

    // The check feed carries targets, so it is not public.
    let res = anon
        .get(format!("{base}/api/v1/agent/checks"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
    let res = anon
        .get(format!("{base}/api/v1/agent/checks"))
        .bearer_auth("wrong-token")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);

    // Unauthenticated ingest would let anyone forge uptime.
    let forged = json!({"results": [{
        "check_id": Uuid::now_v7(),
        "agent_id": "impostor",
        "timestamp": "2026-01-01T00:00:00Z",
        "latency_ms": 1,
        "status_code": 200,
    }]});
    let res = anon
        .post(format!("{base}/api/v1/results"))
        .json(&forged)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401, "results must be authenticated");

    let res = anon
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&forged)
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success(), "a valid token is accepted");
}

#[tokio::test]
async fn sections_are_ordered_renamed_and_deleted_without_losing_checks() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let post = |path: &str, body: Value| {
        let (admin, session, base) = (admin.clone(), session.clone(), base.clone());
        let path = path.to_string();
        async move {
            admin
                .post(format!("{base}{path}"))
                .header(reqwest::header::COOKIE, &session)
                .json(&body)
                .send()
                .await
                .unwrap()
        }
    };

    for name in ["Storage", "API", "CDN"] {
        let res = post("/api/v1/admin/sections", json!({ "name": name })).await;
        assert_eq!(res.status(), 200, "creating {name}");
    }
    // A section can exist before anything is in it, which is the whole point of
    // storing them separately from checks.
    let res = post("/api/v1/admin/sections", json!({ "name": "API" })).await;
    assert_eq!(res.status(), 409, "duplicate section names are refused");

    let names = |v: &Value| {
        v["sections"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };

    // Explicit order, and a name left out must not be stranded ahead of the
    // listed ones.
    let res = admin
        .put(format!("{base}/api/v1/admin/sections"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "order": ["CDN", "API"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let listed: Value = res.json().await.unwrap();
    assert_eq!(names(&listed), vec!["CDN", "API", "Storage"]);

    // A check in a section follows it through a rename.
    let mut check = check_body("edge-cache", "https://edge.test");
    check["group"] = json!("CDN");
    post("/api/v1/admin/checks", check).await;

    let res = admin
        .put(format!("{base}/api/v1/admin/sections/CDN"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "name": "Edge" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let checks: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = checks["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "edge-cache")
        .unwrap();
    assert_eq!(
        row["group"], "Edge",
        "renaming a section must carry its checks, not orphan them"
    );

    // Deleting a section must not delete the monitoring inside it.
    let res = admin
        .delete(format!("{base}/api/v1/admin/sections/Edge"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let checks: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = checks["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "edge-cache")
        .expect("the check survives its section being deleted");
    assert!(
        row.get("group").is_none() || row["group"].is_null(),
        "the check should be loose, got {row}"
    );
}

#[tokio::test]
async fn a_lua_script_decides_the_verdict_end_to_end() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    // 500 is DOWN under the built-in rule; this script inverts that, so the
    // recorded status proves the script ran rather than the default.
    let mut check = check_body("scripted", "https://scripted.test");
    check["eval"] = json!(r#"if status == 500 then return "UP" end return "DOWN""#);
    let res = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let check_id = res.json::<Value>().await.unwrap()["check"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let agent = reqwest::Client::new();
    let report = |status: u16| {
        let check_id = check_id.clone();
        let (agent, base) = (agent.clone(), base.clone());
        async move {
            agent
                .post(format!("{base}/api/v1/results"))
                .bearer_auth(AGENT_TOKEN)
                .json(&json!({"results": [{
                    "check_id": check_id,
                    "agent_id": "agent-test",
                    "type": "http",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "latency_ms": 11,
                    "status_code": status,
                }]}))
                .send()
                .await
                .unwrap()
        }
    };
    report(500).await;
    report(200).await;

    let history: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/checks/{check_id}/history?limit=10"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let by_code: Vec<(u64, String)> = history["history"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            (
                h["status_code"].as_u64().unwrap_or(0),
                h["status"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert!(
        by_code.contains(&(500, "up".to_string())),
        "the script should make 500 UP, got {by_code:?}"
    );
    assert!(
        by_code.contains(&(200, "down".to_string())),
        "the script should make 200 DOWN, got {by_code:?}"
    );
}

#[tokio::test]
async fn a_broken_script_is_refused_at_save_time() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    // Saving this would otherwise mark the service down every interval forever.
    let mut check = check_body("broken", "https://broken.test");
    check["eval"] = json!("this is not lua");
    let res = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
    let err = res.json::<Value>().await.unwrap()["error"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(err.to_lowercase().contains("lua"), "got {err}");
}

#[tokio::test]
async fn the_eval_test_endpoint_reports_verdicts_and_errors() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let try_it = |body: Value| {
        let (admin, session, base) = (admin.clone(), session.clone(), base.clone());
        async move {
            admin
                .post(format!("{base}/api/v1/admin/eval-test"))
                .header(reqwest::header::COOKIE, &session)
                .json(&body)
                .send()
                .await
                .unwrap()
        }
    };

    let res = try_it(json!({
        "eval": r#"if status == 429 then return "UP", 5 end return "DOWN""#,
        "status": 429,
        "latency": 99,
    }))
    .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["verdict"], "up");
    assert_eq!(body["latency_ms"], 5, "the script's override is reported");

    // The whole point of the endpoint: a mistake is visible before saving.
    let res = try_it(json!({ "eval": "return nil + 1", "status": 200 })).await;
    assert_eq!(res.status(), 400);

    // And it is not a public code-execution endpoint.
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/eval-test"))
        .json(&json!({ "eval": "return \"UP\"" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401, "running scripts must require a session");
}

#[tokio::test]
async fn checks_keep_an_explicit_display_order() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let mut ids = std::collections::HashMap::new();
    for name in ["alpha", "bravo", "charlie"] {
        let created: Value = admin
            .post(format!("{base}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &session)
            .json(&check_body(name, &format!("https://{name}.test")))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        ids.insert(name, created["check"]["id"].as_str().unwrap().to_string());
    }

    let names = |v: &Value| {
        v["checks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };

    // Creation order is preserved rather than alphabetical-by-accident, and a
    // new check lands last instead of reshuffling the page.
    let listed: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(names(&listed), vec!["alpha", "bravo", "charlie"]);

    let res = admin
        .put(format!("{base}/api/v1/admin/checks/order"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "order": [ids["charlie"], ids["alpha"]] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    // "bravo" wasn't listed, so it follows the ones that were.
    assert_eq!(
        names(&res.json().await.unwrap()),
        vec!["charlie", "alpha", "bravo"]
    );

    // The public page has to agree, otherwise arranging the order does nothing
    // where it matters.
    for name in ["alpha", "bravo", "charlie"] {
        reqwest::Client::new()
            .post(format!("{base}/api/v1/results"))
            .bearer_auth(AGENT_TOKEN)
            .json(&json!({"results": [{
                "check_id": ids[name],
                "agent_id": "agent-test",
                "timestamp": "2026-01-01T00:00:00Z",
                "latency_ms": 5,
                "status_code": 200,
            }]}))
            .send()
            .await
            .unwrap();
    }
    let public: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut ours: Vec<(i64, String)> = public["checks"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| {
            let id = c["check_id"].as_str().unwrap();
            ["alpha", "bravo", "charlie"]
                .into_iter()
                .find(|name| ids[*name] == id)
                .map(|name| (c["position"].as_i64().unwrap(), name.to_string()))
        })
        .collect();
    ours.sort();
    assert_eq!(
        ours.into_iter().map(|(_, n)| n).collect::<Vec<_>>(),
        vec!["charlie", "alpha", "bravo"],
        "the public page must expose the arranged order"
    );
}

#[tokio::test]
async fn a_check_can_be_restricted_to_particular_agents() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let mut everywhere = check_body("everywhere", "https://all.test");
    everywhere["agents"] = json!([]);
    let mut pinned = check_body("pinned", "https://pinned.test");
    pinned["agents"] = json!(["agent-a"]);
    for body in [everywhere, pinned] {
        let res = admin
            .post(format!("{base}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &session)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    let feed_for = |agent: Option<&str>| {
        let base = base.clone();
        let q = agent.map(|a| format!("?agent_id={a}")).unwrap_or_default();
        async move {
            let v: Value = reqwest::Client::new()
                .get(format!("{base}/api/v1/agent/checks{q}"))
                .bearer_auth(AGENT_TOKEN)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            v["checks"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c["name"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        }
    };

    // Both agents are unplaced, so they share the default zone — and an
    // unplaced check runs once per zone, not once per agent. That single
    // change is what stops a 20-agent fleet probing everything twenty times.
    // Coverage is not lost: if the holder falls silent the other takes over at
    // its next poll, redundancy by failover rather than by duplication.
    // The first pair registers both agents; poll again after membership has
    // converged before asserting placement.
    feed_for(Some("agent-a")).await;
    feed_for(Some("agent-b")).await;
    let a = feed_for(Some("agent-a")).await;
    let b = feed_for(Some("agent-b")).await;

    // Exactly one agent owns the unplaced check in the zone.
    let holders = [&a, &b]
        .iter()
        .filter(|f| f.contains(&"everywhere".to_string()))
        .count();
    assert_eq!(
        holders, 1,
        "one agent per zone probes it, got a={a:?} b={b:?}"
    );

    // Pinning still names machines outright and overrides placement.
    assert!(
        a.contains(&"pinned".to_string()),
        "agent-a owns the pinned check, got {a:?}"
    );
    assert!(
        !b.contains(&"pinned".to_string()),
        "agent-b must not be handed a check pinned to agent-a, got {b:?}"
    );

    // An agent that doesn't say who it is predates the feature; it keeps
    // working rather than silently going idle.
    let anon = feed_for(None).await;
    assert!(anon.contains(&"pinned".to_string()));
}

#[tokio::test]
async fn agent_polls_use_the_refreshed_catalogue_without_changing_assignment() {
    let coord = coordinator().await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let client = reqwest::Client::new();

    let created: Value = client
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("catalogued", "https://catalogue.test"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["check"]["id"].as_str().unwrap();

    // Register both agents before checking placement, so stable assignment—not
    // cold-start ownership—is asserted.
    for agent in ["agent-a", "agent-b"] {
        client
            .get(format!("{base}/api/v1/agent/checks?agent_id={agent}"))
            .bearer_auth(AGENT_TOKEN)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    let feeds = futures_util::future::join_all(["agent-a", "agent-b"].map(|agent| {
        let client = client.clone();
        let base = base.clone();
        async move {
            client
                .get(format!("{base}/api/v1/agent/checks?agent_id={agent}"))
                .bearer_auth(AGENT_TOKEN)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    }))
    .await;
    assert_eq!(
        feeds
            .iter()
            .filter(|feed| feed["checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["id"] == id))
            .count(),
        1,
        "the cached catalogue must retain one-per-zone assignment"
    );

    let mut updated = created["check"].clone();
    updated["enabled"] = Value::Bool(false);
    client
        .put(format!("{base}/api/v1/admin/checks/{id}"))
        .header(reqwest::header::COOKIE, &session)
        .json(&updated)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    for agent in ["agent-a", "agent-b"] {
        let feed: Value = client
            .get(format!("{base}/api/v1/agent/checks?agent_id={agent}"))
            .bearer_auth(AGENT_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(feed["checks"].as_array().unwrap().is_empty());
    }
}

#[tokio::test]
async fn hosts_combine_by_any_or_all() {
    // A pool where one live member is enough, versus a set where each must be
    // up. Getting this backwards either hides an outage or invents one, so both
    // directions are pinned.
    use warden::config::{CheckConfig, MatchMode};

    let pool = CheckConfig {
        host_match: MatchMode::Any,
        ..Default::default()
    };
    assert!(
        pool.host_match.satisfied(1, 3),
        "any: one live member is up"
    );
    assert!(!pool.host_match.satisfied(0, 3), "any: none live is down");

    let set = CheckConfig::default();
    assert_eq!(set.host_match, MatchMode::All, "all is the safe default");
    assert!(set.host_match.satisfied(3, 3));
    assert!(!set.host_match.satisfied(2, 3), "all: one missing is down");
}

#[tokio::test]
async fn a_heartbeat_goes_up_when_pinged_and_is_not_handed_to_agents() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let res = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({
            "name": "nightly-backup",
            "type": "heartbeat",
            "target": "",
            "cron": "*/30 * * * * *",
            "timeout_secs": 5,
            "grace_secs": 3600,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "a heartbeat needs no target");
    let heartbeat_id = res.json::<Value>().await.unwrap()["check"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Nothing probes a heartbeat; handing it to an agent would have it probe
    // an empty target forever.
    let feed: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/agent/checks?agent_id=agent-a"))
        .bearer_auth(AGENT_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !feed["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["name"] == "nightly-backup"),
        "a heartbeat must not be handed to agents"
    );

    // The URL is the credential and needs no session — that is the point of a
    // deadman switch that cron or a backup script can curl.
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/heartbeat/{heartbeat_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let history: Value = reqwest::Client::new()
        .get(format!(
            "{base}/api/v1/checks/{heartbeat_id}/history?limit=5"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rows = history["history"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "the ping was recorded");
    assert_eq!(rows[0]["status"], "up");

    // An unknown name must not reveal whether it exists.
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/heartbeat/not-a-check"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);

    // A normal check must not be pingable — that would let anyone forge uptime.
    let normal: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("normal", "https://normal.test"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/heartbeat/{}", normal["check"]["id"]))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404, "only heartbeats accept pings");
}

#[tokio::test]
async fn purging_data_keeps_the_check() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let noisy: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("noisy", "https://noisy.test"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let noisy_id = noisy["check"]["id"].as_str().unwrap().to_string();
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&json!({"results": [{
            "check_id": noisy_id,
            "agent_id": "agent-test",
            "timestamp": "2026-01-01T00:00:00Z",
            "latency_ms": 3,
            "status_code": 200,
        }]}))
        .send()
        .await
        .unwrap();

    let res = admin
        .delete(format!("{base}/api/v1/admin/checks/{noisy_id}/data"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.json::<Value>().await.unwrap()["purged"], 1);

    // History is gone but the check itself survives — the two are different
    // intentions and must not be conflated.
    let history: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/checks/{noisy_id}/history?limit=5"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(history["history"].as_array().unwrap().is_empty());
    let listed: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        listed["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["name"] == "noisy"),
        "purging data must not delete the check"
    );
}

#[tokio::test]
async fn a_configured_check_appears_before_it_has_ever_reported() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    // A five-minute check is invisible for its first five minutes if the page
    // only lists checks that already have results — which is exactly when
    // someone is watching for it to show up.
    let mut slow = check_body("slow-dns", "example.test");
    slow["type"] = json!("dns");
    slow["cron"] = json!("0 */5 * * * *");
    let created = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&slow)
        .send()
        .await
        .unwrap();
    let code = created.status();
    let body: Value = created.json().await.unwrap();
    assert_eq!(code, 200, "create failed: {body}");
    let slow_id = body["check"]["id"].clone();

    let public: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = public["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["check_id"] == slow_id)
        .expect("a configured check belongs on the page before its first result");
    assert_eq!(row["status"], "unknown", "no result yet is not an outage");
    assert!(
        row["uptime_24h"].is_null(),
        "and for the same reason it is not 0% uptime either: {row}"
    );
    assert_eq!(row["type"], "dns");

    // A hidden check still stays off the page.
    let mut hidden = check_body("hidden-one", "https://hidden.test");
    hidden["visible"] = json!(false);
    let hidden: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&hidden)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let hidden_id = hidden["check"]["id"].clone();
    let public: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !public["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["check_id"] == hidden_id),
        "listing pending checks must not leak hidden ones"
    );
}

#[tokio::test]
async fn heartbeat_pings_do_not_invent_a_monitoring_agent() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let cronjob: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({
            "name": "cronjob", "type": "heartbeat", "target": "",
            "cron": "*/30 * * * * *", "timeout_secs": 5, "grace_secs": 600,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    reqwest::Client::new()
        .post(format!(
            "{base}/api/v1/heartbeat/{}",
            cronjob["check"]["id"]
        ))
        .send()
        .await
        .unwrap();

    let agents: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids: Vec<&str> = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["agent_id"].as_str().unwrap())
        .collect();
    assert!(
        !ids.contains(&"heartbeat"),
        "a heartbeat ping has no agent behind it and must not be listed as one, got {ids:?}"
    );
}

#[tokio::test]
async fn a_check_pinned_to_a_dead_agent_is_reported_not_silent() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    // An agent that reported once and then stopped.
    let seed: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("seed", "https://seed.test"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let seed_id = seed["check"]["id"].as_str().unwrap().to_string();
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&json!({"results": [{
            "check_id": seed_id, "agent_id": "agent-ghost",
            "timestamp": "2020-01-01T00:00:00Z", "latency_ms": 1, "status_code": 200,
        }]}))
        .send()
        .await
        .unwrap();

    let mut pinned = check_body("pinned-to-ghost", "https://ghost.test");
    pinned["agents"] = json!(["agent-ghost"]);
    let pinned: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&pinned)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let pinned_id = pinned["check"]["id"].as_str().unwrap().to_string();

    let agents: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // The dangerous case: the check is configured and enabled, but every agent
    // it names is silent, so nothing runs it and nothing else would say so.
    let orphans = agents["checks_with_no_live_agent"].as_array().unwrap();
    assert!(
        orphans.iter().any(|c| c == "pinned-to-ghost"),
        "a check whose only agent is dead must be reported, got {orphans:?}"
    );

    let ghost = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["agent_id"] == "agent-ghost")
        .expect("a silent agent is still listed, with its history");
    assert_eq!(ghost["status"], "down");
    assert_eq!(ghost["retired"], true, "silent for years reads as retired");
    assert_eq!(ghost["assigned_checks"], 1);

    // Purging it while a check still names it would stop that check running.
    let res = admin
        .delete(format!("{base}/api/v1/admin/agents/agent-ghost"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 409);
    let err = res.json::<Value>().await.unwrap()["error"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        err.contains("pinned-to-ghost"),
        "the refusal names the check: {err}"
    );

    // Unassign, then purging is allowed and the agent disappears.
    let mut freed = check_body("pinned-to-ghost", "https://ghost.test");
    freed["agents"] = json!([]);
    let res = admin
        .put(format!("{base}/api/v1/admin/checks/{pinned_id}"))
        .header(reqwest::header::COOKIE, &session)
        .json(&freed)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let res = admin
        .delete(format!("{base}/api/v1/admin/agents/agent-ghost"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let agents: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !agents["agents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["agent_id"] == "agent-ghost"),
        "a purged agent is gone from the list"
    );

    // Purging must require a session — it destroys history.
    let res = reqwest::Client::new()
        .delete(format!("{base}/api/v1/admin/agents/agent-a"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
}

#[tokio::test]
async fn public_pages_never_republish_what_a_probe_observed() {
    // The reported leak. A DNS check with one mistyped digit failed with
    // "resolved to 73.109.204.164, expected one of [...]" — the operator's home
    // address, published to anyone loading the status page. A status page says
    // whether a service is up; it must not narrate the infrastructure it
    // watches back to the internet.
    let coord = coordinator().await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let client = reqwest::Client::new();

    let created: Value = client
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({
            "name": "ddns", "type": "dns", "target": "home.example.com",
            "cron": "*/30 * * * * *", "timeout_secs": 5, "enabled": true,
            "dns": { "record": "A", "expected": ["203.0.113.9"] },
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .expect("check created")
        .json()
        .await
        .unwrap();
    let ddns_id: CheckId = created["check"]["id"].as_str().unwrap().parse().unwrap();

    let secret = "198.51.100.77";
    coord
        .ingest(vec![warden::models::CheckResult {
            check_id: ddns_id,
            agent_id: "agent-a".into(),
            zone: "us-east-a".into(),
            region: "us-east".into(),
            kind: warden::models::CheckKind::Dns,
            timestamp: chrono::Utc::now(),
            latency_ms: 12,
            status_code: None,
            body_matched: None,
            error: Some(format!(
                "resolved to {secret}, expected one of [\"203.0.113.9\"]"
            )),
            failure: Some(warden::models::FailureKind::UnexpectedDnsAnswer),
            body: None,
            headers: None,
            metric: None,
            metrics: None,
        }])
        .await;

    // Both public endpoints the status page and the check page read from.
    for path in [
        "/api/v1/status".to_string(),
        format!("/api/v1/checks/{ddns_id}/history?limit=10"),
    ] {
        let anon = client
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            !anon.contains(secret),
            "logged out, {path} leaked the resolved address: {anon}"
        );
        assert!(
            anon.contains("Unexpected DNS answer"),
            "the visitor still learns why it is down, {path}: {anon}"
        );

        // An admin is the one audience entitled to the detail — without it the
        // check is undebuggable, and hiding it from them would be a worse bug.
        let admin = client
            .get(format!("{base}{path}"))
            .header(reqwest::header::COOKIE, &session)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            admin.contains(secret),
            "an authenticated admin must still see the real error, {path}"
        );
    }
}

#[tokio::test]
async fn certificate_expiry_days_survive_into_the_public_summary() {
    // Days-to-expiry is in the certificate the server already presents to every
    // visitor, so publishing the count reveals nothing — and it is the single
    // most useful thing a status page can say about a TLS failure. The names on
    // the certificate are what must not travel.
    let expired = warden::models::FailureKind::CertificateExpired { days: 4129 };
    assert_eq!(expired.summary(), "Certificate expired 4129 days ago");

    let soon = warden::models::FailureKind::CertificateExpiring { days: 6 };
    assert_eq!(soon.summary(), "Certificate expires in 6 days");

    // Singular and same-day read as English, not as "1 days".
    assert_eq!(
        warden::models::FailureKind::CertificateExpiring { days: 1 }.summary(),
        "Certificate expires in 1 day"
    );
    assert_eq!(
        warden::models::FailureKind::CertificateExpired { days: 0 }.summary(),
        "Certificate expired today"
    );

    // No summary may quote an observed value.
    for kind in [
        warden::models::FailureKind::UnexpectedDnsAnswer,
        warden::models::FailureKind::UnexpectedContent,
        warden::models::FailureKind::NameResolution,
        warden::models::FailureKind::TlsError,
        warden::models::FailureKind::Other,
    ] {
        let s = kind.summary();
        assert!(
            !s.contains('"') && !s.contains('.'),
            "{kind:?} summary must not quote or address anything: {s}"
        );
    }
}
#[tokio::test]
async fn a_twenty_agent_fleet_probes_each_check_once_per_zone() {
    // The scaling contract, end to end over HTTP. Twenty agents across three
    // regions and six zones ask for their work; each check must come back
    // assigned to exactly one agent per zone. Left unplaced it would go to all
    // twenty — 100 checks at 30s is 67 probes/s and 5.8M rows a day, almost
    // all of it the same observation repeated.
    let coord = coordinator().await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let client = reqwest::Client::new();

    for n in 0..6 {
        client
            .post(format!("{base}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &session)
            .json(&check_body(&format!("svc-{n}"), "https://example.com"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .expect("check created");
    }

    // Six zones, three agents each: none of them has ever reported, so this
    // also exercises the cold-start path where liveness cannot come from
    // history.
    let mut fleet = Vec::new();
    let mut zones = Vec::new();
    for region in ["us-east", "eu-west", "ap-south"] {
        for z in ["a", "b"] {
            zones.push((format!("{region}-{z}"), region));
            for n in 0..3 {
                fleet.push((format!("{region}-{z}-{n}"), format!("{region}-{z}"), region));
            }
        }
    }
    assert_eq!(fleet.len(), 18);
    let zone_refs: Vec<(&str, &str)> = zones.iter().map(|(z, r)| (z.as_str(), *r)).collect();
    let tokens = zone_tokens(&coord, &zone_refs).await;

    // The fleet is discovered by contact, so the first agent to poll is briefly
    // the only one the coordinator knows about and takes everything. That is
    // self-correcting rather than wrong — but it means steady state is what the
    // guarantee applies to, so the fleet introduces itself first.
    for (agent_id, zone, _region) in &fleet {
        client
            .get(format!("{base}/api/v1/agent/checks?agent_id={agent_id}"))
            .bearer_auth(&tokens[zone])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .expect("feed served");
    }

    let mut assigned: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for (agent_id, zone, _region) in &fleet {
        let body: Value = client
            .get(format!("{base}/api/v1/agent/checks?agent_id={agent_id}"))
            .bearer_auth(&tokens[zone])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        for c in body["checks"].as_array().unwrap() {
            assigned
                .entry(c["name"].as_str().unwrap().to_string())
                .or_default()
                .push(agent_id.clone());
        }
    }

    assert_eq!(assigned.len(), 6, "every check is covered by someone");
    for (check, holders) in &assigned {
        assert_eq!(
            holders.len(),
            6,
            "{check} must run once per zone, not once per agent: {holders:?}"
        );
        let zones: std::collections::BTreeSet<&str> = holders
            .iter()
            .map(|h| h.rsplit_once('-').unwrap().0)
            .collect();
        assert_eq!(zones.len(), 6, "{check} spread across all six zones");
    }

    // Total probe volume is what actually matters: 36, not 108.
    let total: usize = assigned.values().map(|v| v.len()).sum();
    assert_eq!(total, 36, "6 checks x 6 zones");
}

#[tokio::test]
async fn scoping_a_check_to_a_region_keeps_it_out_of_the_others() {
    // A check on an internal EU service must not be probed from Singapore: it
    // would fail there for reasons that say nothing about the service.
    let coord = coordinator().await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let client = reqwest::Client::new();

    let mut body = check_body("eu-only", "https://eu.example.com");
    body["placement"] = json!({ "regions": ["eu-west"], "zones": [], "per_zone": 1 });
    client
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .expect("check created");

    let tokens = zone_tokens(
        &coord,
        &[("eu-west-a", "eu-west"), ("ap-south-a", "ap-south")],
    )
    .await;
    let ask = |agent: &'static str, zone: &'static str, _region: &'static str| {
        let client = client.clone();
        let base = base.clone();
        let token = tokens[zone].clone();
        async move {
            let v: Value = client
                .get(format!("{base}/api/v1/agent/checks?agent_id={agent}"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            v["checks"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c["name"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        }
    };

    assert!(
        ask("eu-1", "eu-west-a", "eu-west")
            .await
            .contains(&"eu-only".to_string()),
        "an eu-west agent must probe it"
    );
    assert!(
        !ask("ap-1", "ap-south-a", "ap-south")
            .await
            .contains(&"eu-only".to_string()),
        "an ap-south agent must not"
    );

    // Placement survives the round trip through storage, which keeps checks as
    // JSON documents — a projected-column write would have dropped it.
    let stored: Value = client
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let c = stored["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "eu-only")
        .expect("check present");
    assert_eq!(c["placement"]["regions"][0], "eu-west");
}
/// Admin reads must never disclose values that authenticate a notification
/// delivery, while writing the response back must leave those values intact.
#[tokio::test]
async fn notifier_admin_reads_redact_secrets_and_put_accepts_placeholders() {
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        admin_password: Some(PASSWORD.into()),
        notifiers: vec![
            Notifier {
                name: "hook".into(),
                kind: NotifierKind::Webhook {
                    url: "https://hooks.example.test/webhook-secret".into(),
                },
                default: true,
            },
            Notifier {
                name: "slack".into(),
                kind: NotifierKind::Slack {
                    webhook_url: "https://hooks.example.test/slack-secret".into(),
                },
                default: false,
            },
            Notifier {
                name: "pager".into(),
                kind: NotifierKind::Pagerduty {
                    routing_key: "pager-secret".into(),
                },
                default: false,
            },
            Notifier {
                name: "mail".into(),
                kind: NotifierKind::Email(warden::notify::MailConfig {
                    host: "smtp.example.test".into(),
                    port: None,
                    username: Some("warden".into()),
                    password: Some("smtp-secret".into()),
                    from: "warden@example.test".into(),
                    to: "ops@example.test".into(),
                    tls: warden::notify::MailTls::Starttls,
                }),
                default: false,
            },
        ],
        ..Default::default()
    })
    .await
    .unwrap();
    let addr = spawn_server(coord);
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let session = login(&base).await;

    let read: Value = client
        .get(format!("{base}/api/v1/admin/notifiers"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let encoded = read.to_string();
    for secret in [
        "webhook-secret",
        "slack-secret",
        "pager-secret",
        "smtp-secret",
    ] {
        assert!(!encoded.contains(secret), "admin GET leaked {secret}");
    }
    assert_eq!(read["notifiers"][0]["url"], "********");
    assert_eq!(read["notifiers"][1]["webhook_url"], "********");
    assert_eq!(read["notifiers"][2]["routing_key"], "********");
    assert_eq!(read["notifiers"][3]["password"], "********");

    let saved: Value = client
        .put(format!("{base}/api/v1/admin/notifiers"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "notifiers": read["notifiers"] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(saved["notifiers"], read["notifiers"]);
}

#[tokio::test]
async fn a_hanging_notification_endpoint_cannot_stall_ingest() {
    // The design has the decision engine hand alerts to a notifier over a
    // channel; the implementation awaited the webhook inside `ingest`. That
    // difference is a compounding outage: ingest is the hot path every agent
    // POSTs into, so one unresponsive Slack endpoint blocks results from the
    // whole fleet — and agents drop a batch when their POST times out. The
    // moment it bites is an incident, when transitions and notifications are
    // most frequent and the results matter most.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    // Accepts the connection, then never answers: the realistic failure, and
    // worse than a refusal because it consumes the full client timeout.
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = listener.accept().await {
            held.push(s);
        }
    });

    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        notifiers: vec![Notifier {
            name: "alerts".into(),
            default: true,
            kind: NotifierKind::Webhook {
                url: format!("http://{sink_addr}/"),
            },
        }],
        admin_password: Some(PASSWORD.into()),
        ..Default::default()
    })
    .await
    .unwrap();
    coord
        .create_check_for_test(CheckConfig {
            id: cid("svc"),
            name: "svc".into(),
            target: "https://svc.test/".into(),
            ..Default::default()
        })
        .await;

    let result = |status: u16, ts: i64| warden::models::CheckResult {
        check_id: cid("svc"),
        agent_id: "agent-a".into(),
        zone: "test-zone".into(),
        region: "test-region".into(),
        kind: warden::models::CheckKind::Http,
        timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
        latency_ms: 1,
        status_code: Some(status),
        body_matched: None,
        error: None,
        failure: None,
        body: None,
        headers: None,
        metric: None,
        metrics: None,
    };

    // First observation is a baseline and never alerts.
    coord.ingest(vec![result(200, 1_000)]).await;

    // This one transitions UP -> DOWN, so it fires a notification.
    let started = std::time::Instant::now();
    coord.ingest(vec![result(500, 2_000)]).await;
    let blocked = started.elapsed();

    assert!(
        blocked < std::time::Duration::from_secs(2),
        "ingest waited {blocked:?} on an unresponsive notification endpoint; \
         at 20 agents that backpressures the whole fleet"
    );

    // Decoupling must not lose the alert: it is still recorded for the UI.
    let mine: Vec<_> = coord
        .alerts_for_test(100)
        .await
        .into_iter()
        .filter(|a| a.node_id == cid("svc").to_string())
        .collect();
    assert_eq!(mine.len(), 1, "the transition is still persisted");
}
/// The incident lifecycle is driven by observations, not transitions: the open
/// incident is the source of truth. A check whose first-ever observation is
/// DOWN still opens an incident (the outage is real even though the alerts feed
/// treats that first observation as a baseline), further DOWN observations ride
/// the same open incident, UNKNOWN does not confirm a recovery, and — at the
/// default `failures_before_down` of 1 — the first UP observation resolves it.
///
/// Also pins the shape of an auto-opened incident: it starts at `Detected`, not
/// `Investigating`, because no human has looked at it yet.
#[tokio::test]
async fn incidents_open_on_down_and_resolve_on_up() {
    use warden::coordinator::storage::IncidentState;
    let coord = coordinator_with(vec![http_check("api")]).await;
    let now = chrono::Utc::now().timestamp_millis();

    // `status_code: None` makes the default eval read the probe as Unknown;
    // `Some(code)` drives Up (2xx-3xx) or Down otherwise.
    let result =
        |ts: i64, status_code: Option<u16>, error: Option<String>| warden::models::CheckResult {
            check_id: cid("api"),
            agent_id: "agent-1".into(),
            zone: "test-zone".into(),
            region: "test-region".into(),
            kind: warden::models::CheckKind::Http,
            timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
            latency_ms: 1,
            status_code,
            body_matched: None,
            error,
            failure: None,
            body: None,
            headers: None,
            metric: None,
            metrics: None,
        };

    // The demo seed opens its own incidents, so every assertion below counts
    // only this check's — a global count would be measuring the fixture.
    async fn mine(c: &Coordinator) -> Vec<warden::coordinator::storage::IncidentRow> {
        c.incidents_for_test(100)
            .await
            .into_iter()
            .filter(|i| i.origin_check == Some(cid("api")))
            .collect()
    }

    // First-ever observation is DOWN. No alert (baseline is silent), but the
    // outage is real: an incident must open.
    coord
        .ingest(vec![result(
            now,
            Some(500),
            Some("connection refused".into()),
        )])
        .await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "first-ever DOWN opens an incident");
    assert_eq!(
        incidents[0].state,
        IncidentState::Detected,
        "a probe noticing is not a human investigating"
    );
    assert_eq!(
        incidents[0].origin_check,
        Some(cid("api")),
        "the check that fell is the origin"
    );
    assert_eq!(
        incidents[0]
            .checks
            .iter()
            .map(|c| (c.check_id, c.source))
            .collect::<Vec<_>>(),
        vec![(
            cid("api"),
            warden::coordinator::storage::AffectSource::Origin
        )],
        "the origin is attached as an affected check"
    );
    // The cause is the first entry in the narrative, not a column beside it.
    let updates = coord.updates_for_test(&incidents[0].id).await;
    assert_eq!(updates.len(), 1, "opening writes exactly one update");
    assert_eq!(updates[0].body, "connection refused");
    assert_eq!(updates[0].state, IncidentState::Detected);
    assert!(
        updates[0].author.is_none(),
        "the coordinator wrote it, so it is not an acknowledgement"
    );
    assert!(
        coord
            .alerts_for_test(100)
            .await
            .into_iter()
            .filter(|a| a.node_id == cid("api").to_string())
            .collect::<Vec<_>>()
            .is_empty(),
        "baseline still does not alert"
    );

    // Further DOWN observations while it is open ride on the same incident.
    coord
        .ingest(vec![result(now + 5_000, Some(500), None)])
        .await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "no second incident while one is open");

    // UNKNOWN is not a confirmed recovery, so it must not close the incident.
    coord.ingest(vec![result(now + 10_000, None, None)]).await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "UNKNOWN keeps the incident open");
    assert_eq!(incidents[0].state, IncidentState::Detected);

    // Recovery. `failures_before_down` defaults to 1, so one UP is one full
    // up-side run and the incident resolves immediately.
    coord
        .ingest(vec![result(now + 20_000, Some(200), None)])
        .await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "same incident, now resolved");
    assert_eq!(incidents[0].state, IncidentState::Resolved);
    assert_eq!(incidents[0].resolved_at, Some(now + 20_000));
    // Resolving writes its own entry, so the timeline explains the state.
    let updates = coord.updates_for_test(&incidents[0].id).await;
    assert_eq!(updates.len(), 2, "open and resolve each wrote an update");
    assert_eq!(updates[1].state, IncidentState::Resolved);
    assert!(
        updates[1].body.contains("Total downtime"),
        "the resolve entry states the downtime, got {:?}",
        updates[1].body
    );

    // The recovery alert is stamped with the incident it ended, so the evidence
    // stays attached to the outage rather than floating loose.
    let recovery = coord
        .alerts_for_test(100)
        .await
        .into_iter()
        .find(|a| a.node_id == cid("api").to_string() && a.current_state == NodeState::Up)
        .expect("recovery alert recorded");
    assert_eq!(
        recovery.incident_id.as_deref(),
        Some(incidents[0].id.as_str()),
        "the recovery transition names the incident it closed"
    );

    // And the next outage is a fresh incident.
    coord
        .ingest(vec![result(now + 30_000, Some(500), None)])
        .await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 2, "a new outage opens a second incident");
    assert_eq!(
        incidents[0].state,
        IncidentState::Detected,
        "the newest is the open one"
    );
}

/// The fleet listing is admin-only. Agent ids, zones, regions and reporting
/// cadence together map where monitoring runs from; an anonymous visitor to the
/// status page must not be able to read it.
///
/// This also keeps the admin UI honest: the Agents panel is the only screen
/// backed by this endpoint, so if it answered anonymously an expired session
/// would keep rendering it with live data.
#[tokio::test]
async fn the_fleet_listing_is_not_public() {
    let coord = coordinator().await;
    let addr = spawn_server(coord);
    let base = format!("http://{addr}");

    let anon = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        anon.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "the fleet must not be readable without a session"
    );

    let session = login(&base).await;
    let ok = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(
        ok.status(),
        200,
        "an admin session must still be able to read it"
    );
}

/// Build a coordinator whose checks carry `depends_on` / `failures_before_down`
/// as configured, so the reconciler has a real graph to read.
async fn coordinator_with(checks: Vec<warden::config::CheckConfig>) -> Coordinator {
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

fn http_check(name: &str) -> warden::config::CheckConfig {
    warden::config::CheckConfig {
        id: cid(name),
        name: name.into(),
        target: format!("http://{name}.test/"),
        ..Default::default()
    }
}

fn probe(check: &str, ts: i64, code: u16) -> warden::models::CheckResult {
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

/// A check that declares `depends_on` joins its upstream's open incident
/// instead of opening its own: one outage, one entry on the status page, rather
/// than a row per symptom.
///
/// The dependent still records its own alert — grouping is not suppression —
/// and the attachment is marked `dependency` so an operator can see the machine
/// put it there.
#[tokio::test]
async fn a_dependent_check_joins_its_upstreams_incident() {
    use warden::coordinator::storage::AffectSource;
    let mut api = http_check("api");
    api.depends_on = vec![cid("db")];
    let coord = coordinator_with(vec![http_check("db"), api]).await;
    let now = chrono::Utc::now().timestamp_millis();

    // A baseline UP for the dependent: a first-ever observation never alerts,
    // so without this the "grouping is not suppression" assertion below would
    // pass or fail for the wrong reason.
    coord.ingest(vec![probe("api", now - 1_000, 200)]).await;

    // The upstream falls first and opens the incident.
    coord.ingest(vec![probe("db", now, 500)]).await;
    let opened: Vec<_> = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .filter(|i| i.origin_check == Some(cid("db")))
        .collect();
    assert_eq!(opened.len(), 1, "the upstream opened one incident");
    let incident_id = opened[0].id.clone();

    // The dependent falls next. It must NOT open a second incident.
    coord.ingest(vec![probe("api", now + 1_000, 500)]).await;
    assert!(
        coord
            .incidents_for_test(100)
            .await
            .iter()
            .all(|i| i.origin_check != Some(cid("api"))),
        "a dependent must not open its own incident while its upstream is down"
    );

    let inc = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == incident_id)
        .expect("the upstream incident is still there");
    assert_eq!(
        inc.checks
            .iter()
            .map(|c| (c.check_id, c.source))
            .collect::<Vec<_>>(),
        vec![
            (cid("api"), AffectSource::Dependency),
            (cid("db"), AffectSource::Origin),
        ],
        "the dependent joined the upstream's incident, marked as automatic"
    );

    // Grouping is not suppression: the dependent's own transition is still
    // recorded, and stamped with the incident it belongs to.
    let api_alert = coord
        .alerts_for_test(100)
        .await
        .into_iter()
        .find(|a| a.node_id == cid("api").to_string())
        .expect("the dependent still records its own alert");
    assert_eq!(api_alert.incident_id.as_deref(), Some(incident_id.as_str()));
}

/// Without a declared dependency the same two outages stay separate. This is
/// the counterpart to the test above: it is what stops the grouping rule from
/// quietly merging unrelated failures that happen to coincide, which is exactly
/// what a time-window correlation would have done.
#[tokio::test]
async fn unrelated_checks_falling_together_stay_separate_incidents() {
    let coord = coordinator_with(vec![http_check("db"), http_check("ping")]).await;
    let now = chrono::Utc::now().timestamp_millis();

    coord
        .ingest(vec![probe("db", now, 500), probe("ping", now, 500)])
        .await;

    let origins: Vec<String> = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .filter_map(|i| i.origin_check)
        .filter(|c| *c == cid("db") || *c == cid("ping"))
        .map(|c| c.to_string())
        .collect();
    assert_eq!(
        origins.len(),
        2,
        "two unrelated outages are two incidents, got {origins:?}"
    );
}

/// Auto-resolve waits out the flapping. With `failures_before_down = 3` a check
/// needs three consecutive UP observations before the incident closes, so a
/// check bouncing DOWN/UP stays inside ONE incident instead of opening and
/// closing a new one on every edge.
#[tokio::test]
async fn a_flapping_check_stays_inside_one_incident() {
    use warden::coordinator::storage::IncidentState;
    let mut flappy = http_check("flappy");
    flappy.failures_before_down = 3;
    let coord = coordinator_with(vec![flappy]).await;
    let now = chrono::Utc::now().timestamp_millis();
    async fn mine(c: &Coordinator) -> Vec<warden::coordinator::storage::IncidentRow> {
        c.incidents_for_test(100)
            .await
            .into_iter()
            .filter(|i| i.origin_check == Some(cid("flappy")))
            .collect()
    }

    // Three failures confirm the DOWN and open the incident.
    for i in 0..3 {
        coord
            .ingest(vec![probe("flappy", now + i * 1_000, 500)])
            .await;
    }
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "confirmed DOWN opened one incident");
    let id = incidents[0].id.clone();

    // Flap: UP, DOWN, UP, DOWN. No single UP is a full run of three, so the
    // incident must never close and never be superseded by a second one.
    for (i, code) in [200u16, 500, 200, 500].into_iter().enumerate() {
        coord
            .ingest(vec![probe("flappy", now + 10_000 + i as i64 * 1_000, code)])
            .await;
        let incidents = mine(&coord).await;
        assert_eq!(
            incidents.len(),
            1,
            "flapping must not spray incidents; saw {} after code {code}",
            incidents.len()
        );
        assert_eq!(
            incidents[0].id, id,
            "still the same incident, not a replacement"
        );
    }

    // The two earlier UP observations remain part of the recovery confirmation
    // run; a DOWN that is already confirmed does not reset it. Two more UPs
    // therefore complete the required three successes and close the incident.
    for i in 0..2 {
        coord
            .ingest(vec![probe("flappy", now + 20_000 + i * 1_000, 200)])
            .await;
    }
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "still one incident");
    assert_eq!(
        incidents[0].state,
        IncidentState::Resolved,
        "the accumulated recovery run resolves it"
    );
}

/// Auto-resolve fires whether or not a human has taken the incident over: an
/// operator mid-narrative does not block the machine from recording that the
/// checks came back.
#[tokio::test]
async fn auto_resolve_fires_even_after_a_human_has_acknowledged() {
    use warden::coordinator::storage::IncidentState;
    let coord = coordinator_with(vec![http_check("svc")]).await;
    let now = chrono::Utc::now().timestamp_millis();
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;

    coord.ingest(vec![probe("svc", now, 500)]).await;
    let id = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.origin_check == Some(cid("svc")))
        .expect("incident opened")
        .id;

    // Acknowledge by posting an update — there is no acknowledge endpoint.
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/incidents/{id}/updates"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "state": "investigating", "body": "looking" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let acked = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == id)
        .unwrap();
    assert_eq!(acked.state, IncidentState::Investigating);

    coord.ingest(vec![probe("svc", now + 5_000, 200)]).await;
    let resolved = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == id)
        .unwrap();
    assert_eq!(
        resolved.state,
        IncidentState::Resolved,
        "recovery resolves it even though a human had taken it over"
    );
}

/// Writing to an incident requires a session. The narrative is published on the
/// public status page, so an open update endpoint would let anyone post text to
/// it — and an open create endpoint would let anyone invent an outage.
#[tokio::test]
async fn incident_writes_are_admin_only_while_reads_are_public() {
    let coord = coordinator_with(vec![http_check("svc")]).await;
    let now = chrono::Utc::now().timestamp_millis();
    coord.ingest(vec![probe("svc", now, 500)]).await;
    let id = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.origin_check == Some(cid("svc")))
        .expect("incident opened")
        .id;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let anon = reqwest::Client::new();

    for (method, url, body) in [
        (
            reqwest::Method::POST,
            format!("{base}/api/v1/admin/incidents"),
            json!({ "title": "invented" }),
        ),
        (
            reqwest::Method::PATCH,
            format!("{base}/api/v1/admin/incidents/{id}"),
            json!({ "title": "hijacked", "started_at": now, "global": true }),
        ),
        (
            reqwest::Method::PUT,
            format!("{base}/api/v1/admin/incidents/{id}/checks"),
            json!({ "checks": [cid("svc").to_string()] }),
        ),
        (
            reqwest::Method::POST,
            format!("{base}/api/v1/admin/incidents/{id}/updates"),
            json!({ "state": "resolved", "body": "nothing to see here" }),
        ),
    ] {
        let res = anon
            .request(method.clone(), &url)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{method} {url} must require a session"
        );
    }

    // Nothing above took effect.
    let after = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == id)
        .unwrap();
    assert_eq!(after.title, "svc is down", "the title was not rewritten");
    assert!(!after.global, "it was not promoted to a global incident");
    assert!(
        !after.state.is_resolved(),
        "an anonymous caller could not close the outage"
    );

    // Reads stay public: the permalink is what the status page links to.
    let res = anon
        .get(format!("{base}/api/v1/incidents/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["incident"]["id"], id);
    assert!(
        !body["incident"]["updates"].as_array().unwrap().is_empty(),
        "the permalink carries the narrative"
    );
}

/// A manually created incident is the operator's, and the reconciler must not
/// touch it. It has no `origin_check`, so there is nothing for the machine to
/// follow — a maintenance notice is over when a human says so.
///
/// This is a regression test. Both halves shipped broken and were caught by a
/// live smoke test, not by the suite:
///   1. `open_incident_for` returned manual incidents, so a passing check
///      pulled one into the resolve path at all.
///   2. `try_resolve` iterated the gating checks and fell through on an empty
///      set, so an incident whose attachments are all `manual` resolved
///      vacuously — the loop had nothing to refuse.
#[tokio::test]
async fn a_manual_incident_is_never_auto_resolved() {
    use warden::coordinator::storage::IncidentState;
    let coord = coordinator_with(vec![http_check("db")]).await;
    let now = chrono::Utc::now().timestamp_millis();
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;

    // A maintenance notice naming a check that is perfectly healthy.
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/incidents"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({
            "title": "Scheduled database maintenance",
            "state": "monitoring",
            "global": true,
            "checks": [cid("db").to_string()],
            "body": "Planned failover window.",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let created: Value = res.json().await.unwrap();
    let id = created["incident"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        created["incident"]["origin_check"],
        Value::Null,
        "a manual incident has no origin for the reconciler to follow"
    );

    // The named check keeps passing. That must change nothing.
    coord
        .ingest(vec![probe("db", now, 200), probe("db", now + 1_000, 200)])
        .await;
    let after = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == id)
        .expect("the manual incident still exists");
    assert_eq!(
        after.state,
        IncidentState::Monitoring,
        "a healthy check must not close an operator's incident"
    );
    assert_eq!(after.resolved_at, None);

    // And the named check failing must open its OWN incident rather than
    // riding the manual one, which would give the outage no origin.
    coord.ingest(vec![probe("db", now + 2_000, 500)]).await;
    let own: Vec<_> = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .filter(|i| i.origin_check == Some(cid("db")))
        .collect();
    assert_eq!(
        own.len(),
        1,
        "the failure opened a real incident of its own, not a ride on the notice"
    );
    assert_ne!(own[0].id, id);
}

/// End-to-end for the OpenMetrics check: a scrape's selected value and its
/// same-scrape samples reach the coordinator's Lua evaluator and drive the
/// confirmed status, and a missing selection is a real failure rather than a
/// silent zero.
///
/// This exercises the ingest -> script_verdict -> status path with the transient
/// `metric`/`metrics` fields, which are never persisted, so the only way to
/// observe them is through the verdict they produce.
#[tokio::test]
async fn openmetrics_metric_and_metrics_drive_the_verdict() {
    use warden::config::{CheckConfig, MetricLabel, MetricOptions};
    use warden::models::{CheckKind, CheckResult};
    use warden::openmetrics::MetricSample;

    // A metric check whose script compares two same-scrape series (used/size)
    // and treats an absent selection as DOWN.
    let check = CheckConfig {
        id: cid("queue"),
        name: "queue".into(),
        kind: CheckKind::Openmetrics,
        target: "http://exporter.test:9100/metrics".into(),
        metric: MetricOptions {
            name: "queue_used".into(),
            labels: vec![MetricLabel {
                name: "queue".into(),
                value: "jobs".into(),
            }],
        },
        eval: Some(
            r#"
              if metric == nil then return "DOWN" end
              local used = metrics.queue_used[1].value
              local size = metrics.queue_size[1].value
              if used / size > 0.9 then return "DOWN" end
              return "UP"
            "#
            .into(),
        ),
        ..Default::default()
    };
    let coord = coordinator_with(vec![check]).await;
    let now = chrono::Utc::now().timestamp_millis();

    let scrape = |ts: i64, metric: Option<f64>, metrics: Vec<MetricSample>| CheckResult {
        check_id: cid("queue"),
        agent_id: "agent-1".into(),
        zone: "test-zone".into(),
        region: "test-region".into(),
        kind: CheckKind::Openmetrics,
        timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
        latency_ms: 3,
        status_code: Some(200),
        body_matched: None,
        error: None,
        failure: None,
        body: None,
        headers: None,
        metric,
        metrics: Some(metrics),
    };

    let healthy = || {
        vec![
            MetricSample::new("queue_used", vec![("queue".into(), "jobs".into())], 10.0),
            MetricSample::new("queue_size", vec![("queue".into(), "jobs".into())], 100.0),
        ]
    };

    // Baseline: used/size = 0.1, well under the threshold -> UP. A first-ever
    // observation never alerts, so no incident yet.
    coord.ingest(vec![scrape(now, Some(10.0), healthy())]).await;
    assert!(
        coord
            .incidents_for_test(100)
            .await
            .iter()
            .all(|i| i.origin_check != Some(cid("queue"))),
        "a healthy metric check opens no incident"
    );

    // The same-scrape ratio crosses the threshold: used=95, size=100 -> DOWN,
    // which must open an incident. This can only be true if BOTH samples from
    // the same scrape reached the script.
    let hot = vec![
        MetricSample::new("queue_used", vec![("queue".into(), "jobs".into())], 95.0),
        MetricSample::new("queue_size", vec![("queue".into(), "jobs".into())], 100.0),
    ];
    coord
        .ingest(vec![scrape(now + 1_000, Some(95.0), hot)])
        .await;
    let down: Vec<_> = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .filter(|i| i.origin_check == Some(cid("queue")))
        .collect();
    assert_eq!(
        down.len(),
        1,
        "a same-scrape ratio over threshold drives the check DOWN via metrics[]"
    );

    // Recovery, then a MISSING selection: the exporter stops emitting the
    // selected series (metric = nil). The script returns DOWN for nil, proving a
    // missing sample is a failure and not a zero that happens to pass.
    coord
        .ingest(vec![scrape(now + 2_000, Some(10.0), healthy())])
        .await;
    coord.ingest(vec![scrape(now + 3_000, None, vec![])]).await;
    let reopened: Vec<_> = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .filter(|i| i.origin_check == Some(cid("queue")) && i.resolved_at.is_none())
        .collect();
    assert_eq!(
        reopened.len(),
        1,
        "a missing selected metric is DOWN, not a silent healthy zero"
    );
}

/// The raw-results explorer is admin-only and un-redacted: an anonymous caller
/// is refused, and an authenticated one gets the verbatim probe error — the
/// ddns-style line quoting the resolved IP that the public API deliberately
/// summarises away.
#[tokio::test]
async fn the_results_explorer_is_admin_only_and_returns_raw_errors() {
    let coord = coordinator().await;
    coord
        .create_check_for_test(CheckConfig {
            id: cid("web"),
            name: "web".into(),
            ..Default::default()
        })
        .await;
    coord
        .create_check_for_test(CheckConfig {
            id: cid("ddns-wa"),
            name: "ddns-wa".into(),
            kind: warden::models::CheckKind::Dns,
            ..Default::default()
        })
        .await;

    // The exact leak the public path redacts: a resolved address and the
    // expected values, verbatim from the agent.
    let leaked = r#"resolved to 97.7.61.14, expected one of ["7.…"]"#;
    let now = chrono::Utc::now().timestamp_millis();
    let result =
        |check: &str, agent: &str, ts: i64, region: &str, zone: &str, error: Option<String>| {
            warden::models::CheckResult {
                check_id: cid(check),
                agent_id: agent.into(),
                zone: zone.into(),
                region: region.into(),
                kind: warden::models::CheckKind::Dns,
                timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
                latency_ms: 3,
                status_code: None,
                body_matched: None,
                error,
                failure: None,
                body: None,
                headers: None,
                metric: None,
                metrics: None,
            }
        };
    coord
        .ingest(vec![
            result("web", "agent-a", now, "us-east", "us-east-1a", None),
            result(
                "ddns-wa",
                "agent-b",
                now + 1_000,
                "eu-west",
                "eu-west-1a",
                Some(leaked.into()),
            ),
        ])
        .await;

    let addr = spawn_server(coord);
    let base = format!("http://{addr}");

    // Anonymous: refused. This endpoint returns raw errors, so it must never be
    // reachable without a session.
    let anon = reqwest::Client::new()
        .get(format!("{base}/api/v1/admin/results"))
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), 401, "the explorer must require a session");

    let session = login(&base).await;
    let admin = reqwest::Client::new();

    // Authed, filtered to the ddns check: the demo seed also lands rows, so the
    // server-side `check` filter is what isolates this test's data.
    let body: Value = admin
        .get(format!(
            "{base}/api/v1/admin/results?check={}",
            cid("ddns-wa")
        ))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rows = body["results"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "the check filter isolates the ddns row");
    assert_eq!(rows[0]["check_id"], cid("ddns-wa").to_string());
    assert_eq!(
        rows[0]["error"], leaked,
        "the admin path returns the error verbatim, un-redacted"
    );
    assert_eq!(rows[0]["region"], "eu-west");
    assert_eq!(rows[0]["zone"], "eu-west-1a");

    // Combined filters AND together: same check, filtered to the other agent
    // that never touched it, returns nothing.
    let empty: Value = admin
        .get(format!(
            "{base}/api/v1/admin/results?check={}&agent=agent-a",
            cid("ddns-wa")
        ))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        empty["results"].as_array().unwrap().len(),
        0,
        "an agent that never probed this check narrows to nothing"
    );

    // The `web` result carried no error, so its row omits the field entirely.
    let web: Value = admin
        .get(format!("{base}/api/v1/admin/results?check={}", cid("web")))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rows = web["results"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].get("error").is_none(),
        "a successful probe has no error field"
    );
}

// ---- Deleting incidents, and erasing a check outright ----

/// Open one incident by hand and hand back its id. Manual rather than
/// probe-driven because the delete path does not care how it was opened, and
/// driving it through ingest would make the test about reconciliation instead.
async fn create_incident(base: &str, session: &str, title: &str, checks: &[String]) -> String {
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

/// Acknowledging is posting an empty-bodied `investigating` update, and the
/// console offers it on every render of a `detected` row. A double click, a
/// stale list, or a relay that answers from a lagging region therefore posts it
/// twice — and the second one recorded another silent row against the public
/// narrative, forever, one per click. The first ack is the acknowledgement; an
/// empty update that moves nothing afterwards must record nothing.
#[tokio::test]
async fn acknowledging_twice_records_one_update() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;
    let id = create_incident(&base, &session, "svc is down", &[]).await;

    let ack = || async {
        admin
            .post(format!("{base}/api/v1/admin/incidents/{id}/updates"))
            .header(reqwest::header::COOKIE, session.clone())
            .json(&json!({ "state": "investigating", "body": "" }))
            .send()
            .await
            .unwrap()
    };

    assert_eq!(ack().await.status(), reqwest::StatusCode::OK);
    let after_first: Value = reqwest::get(format!("{base}/api/v1/incidents/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let first = after_first["incident"]["updates"].as_array().unwrap().len();
    assert_eq!(after_first["incident"]["state"], "investigating");
    assert_eq!(after_first["incident"]["acknowledged"], true);

    // Three more clicks. Each is accepted — the operator did nothing wrong —
    // but none of them may leave a trace.
    for _ in 0..3 {
        assert_eq!(ack().await.status(), reqwest::StatusCode::OK);
    }
    let after_repeat: Value = reqwest::get(format!("{base}/api/v1/incidents/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        after_repeat["incident"]["updates"]
            .as_array()
            .unwrap()
            .len(),
        first,
        "a repeated acknowledgement must not append another empty update"
    );
    assert_eq!(after_repeat["incident"]["acknowledged"], true);

    // A real update still lands: the guard is about empty no-ops, not about
    // refusing to let an operator narrate an incident they already acked.
    let res = admin
        .post(format!("{base}/api/v1/admin/incidents/{id}/updates"))
        .header(reqwest::header::COOKIE, session.clone())
        .json(&json!({ "state": "investigating", "body": "nginx upstream missing" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let after_body: Value = reqwest::get(format!("{base}/api/v1/incidents/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        after_body["incident"]["updates"].as_array().unwrap().len(),
        first + 1,
        "an authored update is not a no-op even at the same state"
    );
}

async fn public_incident_ids(base: &str) -> Vec<String> {
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

#[tokio::test]
async fn deleting_an_incident_needs_a_session_and_a_real_id() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;
    let id = create_incident(&base, &session, "Router flap", &[]).await;

    // An anonymous caller must not be able to erase what visitors are reading.
    let anon = reqwest::Client::new()
        .delete(format!("{base}/api/v1/admin/incidents/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), 401);
    assert!(
        public_incident_ids(&base).await.contains(&id),
        "a rejected delete must not have deleted anything"
    );

    let unknown = admin
        .delete(format!("{base}/api/v1/admin/incidents/never-existed"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(
        unknown.status(),
        404,
        "an id nobody opened is not a success"
    );

    let res = admin
        .delete(format!("{base}/api/v1/admin/incidents/{id}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.json::<Value>().await.unwrap()["deleted"], id);
    assert!(
        !public_incident_ids(&base).await.contains(&id),
        "the incident is gone from the public list"
    );

    // A second delete of the same id is a 404, not a silent success: the
    // tombstone remembers, but there is nothing left to remove.
    let again = admin
        .delete(format!("{base}/api/v1/admin/incidents/{id}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 404);
}

/// The alert log is the raw record that a check flipped. Deleting the narrative
/// written around it must not erase the fact, so the alert survives with a null
/// `incident_id` rather than going with the incident.
#[tokio::test]
async fn deleting_an_incident_unlinks_its_alerts_instead_of_deleting_them() {
    let coord = coordinator().await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let web: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("web", "https://web.test"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let web_id: CheckId = web["check"]["id"].as_str().unwrap().parse().unwrap();
    // An UP first, so the DOWN below is a *transition*: a check's first-ever
    // observation opens an incident but writes no alert, and this test is
    // about what happens to the alert.
    for (ts, code) in [("2026-01-01T00:00:00Z", 200), ("2026-01-01T00:01:00Z", 500)] {
        reqwest::Client::new()
            .post(format!("{base}/api/v1/results"))
            .bearer_auth(AGENT_TOKEN)
            .json(&json!({"results": [{
            "check_id": web_id,
                "agent_id": "agent-test",
                "timestamp": ts,
                "latency_ms": 3,
                "status_code": code,
            }]}))
            .send()
            .await
            .unwrap();
    }

    let opened = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.origin_check == Some(web_id))
        .expect("a confirmed DOWN opens an incident");

    admin
        .delete(format!("{base}/api/v1/admin/incidents/{}", opened.id))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();

    let alerts: Value = reqwest::get(format!("{base}/api/v1/alerts?limit=50"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mine: Vec<&Value> = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["node_id"] == web_id.to_string())
        .collect();
    assert_eq!(mine.len(), 1, "the transition is still on the record");
    assert!(
        mine[0]["incident_id"].is_null(),
        "the alert should be unlinked, not deleted: {:?}",
        mine[0]
    );
}

/// The third danger-zone action: the check, its results, its alerts and every
/// incident it opened, all gone. Incidents owned by *other* checks survive.
#[tokio::test]
async fn erasing_a_check_removes_everything_it_produced() {
    let coord = coordinator().await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let mut ids = std::collections::HashMap::new();
    for name in ["web", "api"] {
        let created: Value = admin
            .post(format!("{base}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &session)
            .json(&check_body(name, &format!("https://{name}.test")))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        ids.insert(name, created["check"]["id"].as_str().unwrap().to_string());
    }
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&json!({"results": [{
            "check_id": ids["web"],
            "agent_id": "agent-test",
            "timestamp": "2026-01-01T00:00:00Z",
            "latency_ms": 3,
            "status_code": 500,
        }]}))
        .send()
        .await
        .unwrap();
    // An operator's incident about a different check, which `web` merely
    // appears on. It must outlive the erase.
    let others = create_incident(
        &base,
        &session,
        "Datacentre work",
        &[ids["api"].clone(), ids["web"].clone()],
    )
    .await;

    let unknown = admin
        .delete(format!("{base}/api/v1/admin/checks/nope/everything"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);

    let res = admin
        .delete(format!(
            "{base}/api/v1/admin/checks/{}/everything",
            ids["web"]
        ))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["deleted"], ids["web"]);
    assert_eq!(body["results"], 1);
    assert_eq!(body["incidents"], 1, "the incident web opened, and only it");

    let listed: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = listed["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(
        !names.contains(&"web"),
        "the check itself is gone: {names:?}"
    );
    assert!(names.contains(&"api"), "and nothing else went with it");

    assert!(
        coord
            .incidents_for_test(100)
            .await
            .into_iter()
            .all(|i| i.origin_check != Some(cid("web"))),
        "every incident web opened is gone"
    );
    let surviving = public_incident_ids(&base).await;
    assert!(
        surviving.contains(&others),
        "an incident another check owns is not the erased check's to delete"
    );

    let history: Value = reqwest::get(format!(
        "{base}/api/v1/checks/{}/history?limit=5",
        ids["web"]
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert!(history["history"].as_array().unwrap().is_empty());
}
