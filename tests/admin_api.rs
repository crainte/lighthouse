//! Integration test for the check editor and the auth around it.
//!
//! The security claim being defended is narrow and load-bearing: the admin API
//! mutates what gets probed and exposes probe targets, so it must be unusable
//! without a session — and probe targets must never appear on the public status
//! endpoint. Everything runs against the real router over a real localhost
//! socket, driven as JSON over the wire rather than through Rust internals.

use std::net::SocketAddr;

use serde_json::{json, Value};

use warden::config::CoordinatorConfig;
use warden::coordinator::Coordinator;
use warden::notify::Notifier;

const PASSWORD: &str = "correct horse battery staple";
const AGENT_TOKEN: &str = "agent-shared-secret";

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

fn coordinator() -> Coordinator {
    Coordinator::new(
        &CoordinatorConfig {
            listen: "127.0.0.1:0".into(),
            db_path: ":memory:".into(),
            notifiers: Vec::new(),
            retention: warden::config::RetentionConfig::default(),
            admin_password: Some(PASSWORD.into()),
            agent_token: Some(AGENT_TOKEN.into()),
        },
        &[],
    )
    .unwrap()
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

#[tokio::test]
async fn the_check_editor_is_unusable_without_a_session() {
    let addr = spawn_server(coordinator());
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
    let addr = spawn_server(coordinator());
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
    assert_eq!(
        res.json::<Value>().await.unwrap()["check"]["target"],
        "https://docs.test/health"
    );

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
        .put(format!("{base}/api/v1/admin/checks/docs-site"))
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
        .delete(format!("{base}/api/v1/admin/checks/docs-site"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let res = admin
        .delete(format!("{base}/api/v1/admin/checks/docs-site"))
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
async fn invalid_checks_are_refused_with_a_readable_reason() {
    let addr = spawn_server(coordinator());
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
            json!({"name": "bad name", "type": "http", "target": "https://a.test", "cron": "*/30 * * * * *", "timeout_secs": 5}),
            "name",
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

    // Renaming through PUT would orphan the check's history.
    admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("keeper", "https://keeper.test"))
        .send()
        .await
        .unwrap();
    let res = admin
        .put(format!("{base}/api/v1/admin/checks/keeper"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("renamed", "https://keeper.test"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn probe_targets_never_reach_the_public_api() {
    let addr = spawn_server(coordinator());
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;
    admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body(
            "internal",
            "https://vault.internal.example/health",
        ))
        .send()
        .await
        .unwrap();

    // Report a result so the check appears on the status page at all.
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&json!({"results": [{
            "check_id": "internal",
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
    let addr = spawn_server(coordinator());
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
        "check_id": "anything",
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
    let addr = spawn_server(coordinator());
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
    let addr = spawn_server(coordinator());
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

    let agent = reqwest::Client::new();
    let report = |status: u16| {
        let (agent, base) = (agent.clone(), base.clone());
        async move {
            agent
                .post(format!("{base}/api/v1/results"))
                .bearer_auth(AGENT_TOKEN)
                .json(&json!({"results": [{
                    "check_id": "scripted",
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
        .get(format!("{base}/api/v1/checks/scripted/history?limit=10"))
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
    let addr = spawn_server(coordinator());
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
    let addr = spawn_server(coordinator());
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
    let addr = spawn_server(coordinator());
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    for name in ["alpha", "bravo", "charlie"] {
        admin
            .post(format!("{base}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &session)
            .json(&check_body(name, &format!("https://{name}.test")))
            .send()
            .await
            .unwrap();
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
        .json(&json!({ "order": ["charlie", "alpha"] }))
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
                "check_id": name,
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
            let n = c["check_id"].as_str().unwrap().to_string();
            ["alpha", "bravo", "charlie"]
                .contains(&n.as_str())
                .then(|| (c["position"].as_i64().unwrap(), n))
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
    let addr = spawn_server(coordinator());
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
    let a = feed_for(Some("agent-a")).await;
    let b = feed_for(Some("agent-b")).await;

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
    let addr = spawn_server(coordinator());
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
        .post(format!("{base}/api/v1/heartbeat/nightly-backup"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let history: Value = reqwest::Client::new()
        .get(format!(
            "{base}/api/v1/checks/nightly-backup/history?limit=5"
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
    admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("normal", "https://normal.test"))
        .send()
        .await
        .unwrap();
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/heartbeat/normal"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404, "only heartbeats accept pings");
}

#[tokio::test]
async fn purging_data_keeps_the_check() {
    let addr = spawn_server(coordinator());
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("noisy", "https://noisy.test"))
        .send()
        .await
        .unwrap();
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&json!({"results": [{
            "check_id": "noisy",
            "agent_id": "agent-test",
            "timestamp": "2026-01-01T00:00:00Z",
            "latency_ms": 3,
            "status_code": 200,
        }]}))
        .send()
        .await
        .unwrap();

    let res = admin
        .delete(format!("{base}/api/v1/admin/checks/noisy/data"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.json::<Value>().await.unwrap()["purged"], 1);

    // History is gone but the check itself survives — the two are different
    // intentions and must not be conflated.
    let history: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/checks/noisy/history?limit=5"))
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
    let addr = spawn_server(coordinator());
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
    let body = created.text().await.unwrap();
    assert_eq!(code, 200, "create failed: {body}");

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
        .find(|c| c["check_id"] == "slow-dns")
        .expect("a configured check belongs on the page before its first result");
    assert_eq!(row["status"], "unknown", "no result yet is not an outage");
    assert_eq!(row["uptime_24h"], 0.0);
    assert_eq!(row["type"], "dns");

    // A hidden check still stays off the page.
    let mut hidden = check_body("hidden-one", "https://hidden.test");
    hidden["visible"] = json!(false);
    admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&hidden)
        .send()
        .await
        .unwrap();
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
            .any(|c| c["check_id"] == "hidden-one"),
        "listing pending checks must not leak hidden ones"
    );
}

#[tokio::test]
async fn heartbeat_pings_do_not_invent_a_monitoring_agent() {
    let addr = spawn_server(coordinator());
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({
            "name": "cronjob", "type": "heartbeat", "target": "",
            "cron": "*/30 * * * * *", "timeout_secs": 5, "grace_secs": 600,
        }))
        .send()
        .await
        .unwrap();
    reqwest::Client::new()
        .post(format!("{base}/api/v1/heartbeat/cronjob"))
        .send()
        .await
        .unwrap();

    let agents: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
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
    let addr = spawn_server(coordinator());
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    // An agent that reported once and then stopped.
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(AGENT_TOKEN)
        .json(&json!({"results": [{
            "check_id": "seed", "agent_id": "agent-ghost",
            "timestamp": "2020-01-01T00:00:00Z", "latency_ms": 1, "status_code": 200,
        }]}))
        .send()
        .await
        .unwrap();

    let mut pinned = check_body("pinned-to-ghost", "https://ghost.test");
    pinned["agents"] = json!(["agent-ghost"]);
    admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&pinned)
        .send()
        .await
        .unwrap();

    let agents: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
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
    admin
        .put(format!("{base}/api/v1/admin/checks/pinned-to-ghost"))
        .header(reqwest::header::COOKIE, &session)
        .json(&freed)
        .send()
        .await
        .unwrap();
    let res = admin
        .delete(format!("{base}/api/v1/admin/agents/agent-ghost"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let agents: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
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
    let coord = coordinator();
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let client = reqwest::Client::new();

    client
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
        .expect("check created");

    let secret = "198.51.100.77";
    coord
        .ingest(vec![warden::models::CheckResult {
            check_id: "ddns".into(),
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
        }])
        .await;

    // Both public endpoints the status page and the check page read from.
    for path in ["/api/v1/status", "/api/v1/checks/ddns/history?limit=10"] {
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
    let coord = coordinator();
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
    for region in ["us-east", "eu-west", "ap-south"] {
        for z in ["a", "b"] {
            for n in 0..3 {
                fleet.push((format!("{region}-{z}-{n}"), format!("{region}-{z}"), region));
            }
        }
    }
    assert_eq!(fleet.len(), 18);

    // The fleet is discovered by contact, so the first agent to poll is briefly
    // the only one the coordinator knows about and takes everything. That is
    // self-correcting rather than wrong — but it means steady state is what the
    // guarantee applies to, so the fleet introduces itself first.
    for (agent_id, zone, region) in &fleet {
        client
            .get(format!(
                "{base}/api/v1/agent/checks?agent_id={agent_id}&zone={zone}&region={region}"
            ))
            .header("Authorization", format!("Bearer {AGENT_TOKEN}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .expect("feed served");
    }

    let mut assigned: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for (agent_id, zone, region) in &fleet {
        let body: Value = client
            .get(format!(
                "{base}/api/v1/agent/checks?agent_id={agent_id}&zone={zone}&region={region}"
            ))
            .header("Authorization", format!("Bearer {AGENT_TOKEN}"))
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
    let coord = coordinator();
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

    let ask = |agent: &'static str, zone: &'static str, region: &'static str| {
        let client = client.clone();
        let base = base.clone();
        async move {
            let v: Value = client
                .get(format!(
                    "{base}/api/v1/agent/checks?agent_id={agent}&zone={zone}&region={region}"
                ))
                .header("Authorization", format!("Bearer {AGENT_TOKEN}"))
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

    let coord = Coordinator::new(
        &CoordinatorConfig {
            listen: "127.0.0.1:0".into(),
            db_path: ":memory:".into(),
            notifiers: vec![Notifier::Webhook {
                url: format!("http://{sink_addr}/"),
            }],
            retention: warden::config::RetentionConfig::default(),
            admin_password: Some(PASSWORD.into()),
            agent_token: Some(AGENT_TOKEN.into()),
        },
        &[],
    )
    .unwrap();

    let result = |status: u16, ts: i64| warden::models::CheckResult {
        check_id: "svc".into(),
        agent_id: "agent-a".into(),
        zone: "us-east-a".into(),
        region: "us-east".into(),
        kind: warden::models::CheckKind::Http,
        timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
        latency_ms: 1,
        status_code: Some(status),
        body_matched: None,
        error: None,
        failure: None,
        body: None,
        headers: None,
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
        .into_iter()
        .filter(|a| a.check_id == "svc")
        .collect();
    assert_eq!(mine.len(), 1, "the transition is still persisted");
}
/// The incident lifecycle is driven by observations, not transitions: the
/// open incident row is the source of truth. A check whose first-ever
/// observation is DOWN still opens an incident (the outage is real even though
/// the alerts feed treats that first observation as a baseline), further DOWN
/// observations ride the same open incident, UNKNOWN does not confirm a
/// recovery, and the first UP observation resolves it.
#[tokio::test]
async fn incidents_open_on_down_and_resolve_on_up() {
    let coord = coordinator();
    let now = 1_783_728_000_000i64; // 2026-07-11T00:00:00Z

    // `status_code: None` makes the default eval read the probe as Unknown;
    // `Some(code)` drives Up (2xx-3xx) or Down otherwise.
    let result =
        |ts: i64, status_code: Option<u16>, error: Option<String>| warden::models::CheckResult {
            check_id: "api".into(),
            agent_id: "agent-a".into(),
            zone: "us-east-a".into(),
            region: "us-east".into(),
            kind: warden::models::CheckKind::Http,
            timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
            latency_ms: 1,
            status_code,
            body_matched: None,
            error,
            failure: None,
            body: None,
            headers: None,
        };

    // First-ever observation is DOWN. No alert (baseline is silent), but the
    // outage is real: an incident must open.
    coord
        .ingest(vec![result(
            now,
            Some(500),
            Some("connection refused".into()),
        )])
        .await;
    let incidents = coord.incidents_for_test(100);
    assert_eq!(incidents.len(), 1, "first-ever DOWN opens an incident");
    assert_eq!(
        incidents[0].status,
        warden::coordinator::storage::IncidentStatus::Open
    );
    assert_eq!(incidents[0].details.as_deref(), Some("connection refused"));
    assert!(
        coord
            .alerts_for_test(100)
            .into_iter()
            .filter(|a| a.check_id == "api")
            .collect::<Vec<_>>()
            .is_empty(),
        "baseline still does not alert"
    );

    // Further DOWN observations while it is open ride on the same incident.
    coord
        .ingest(vec![result(now + 5_000, Some(500), None)])
        .await;
    let incidents = coord.incidents_for_test(100);
    assert_eq!(incidents.len(), 1, "no second incident while one is open");

    // UNKNOWN is not a confirmed recovery, so it must not close the incident.
    coord.ingest(vec![result(now + 10_000, None, None)]).await;
    let incidents = coord.incidents_for_test(100);
    assert_eq!(incidents.len(), 1, "UNKNOWN keeps the incident open");
    assert_eq!(
        incidents[0].status,
        warden::coordinator::storage::IncidentStatus::Open
    );

    // Recovery: the first UP observation resolves it.
    coord
        .ingest(vec![result(now + 20_000, Some(200), None)])
        .await;
    let incidents = coord.incidents_for_test(100);
    assert_eq!(incidents.len(), 1, "same incident, now resolved");
    assert_eq!(
        incidents[0].status,
        warden::coordinator::storage::IncidentStatus::Resolved
    );
    assert_eq!(incidents[0].resolved_at, Some(now + 20_000));

    // And the next outage is a fresh incident.
    coord
        .ingest(vec![result(now + 30_000, Some(500), None)])
        .await;
    let incidents = coord.incidents_for_test(100);
    assert_eq!(incidents.len(), 2, "a new outage opens a second incident");
    assert_eq!(
        incidents[0].status,
        warden::coordinator::storage::IncidentStatus::Open,
        "the newest is the open one"
    );
}
