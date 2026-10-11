//! Admin API integration tests for check CRUD, catalogue paging, ordering,
//! placement/scoping, agent assignment, probe-target confidentiality, Lua
//! verdict scripts, OpenMetrics-driven checks, and the results explorer.
//!
//! Split from the original `admin_api.rs`; see `tests/common/mod.rs` for the
//! shared fixtures (`coordinator`, `spawn_server`, `login`, `check_body`, ...).

mod common;

use serde_json::{json, Value};

use common::*;
use lighthouse::config::CheckConfig;
use lighthouse::models::CheckId;
use uuid::Uuid;

/// `limit` arrives from the network and the database is a single worker, so a
/// caller must not be able to opt out of the page bound. The console still
/// gets the whole catalogue; it pays for it one bounded request at a time.
#[tokio::test]
async fn the_admin_check_list_caps_its_page_and_pages_to_the_end() {
    let coord = coordinator().await;
    let addr = spawn_server(coord);
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let session = login(&base).await;
    for i in 0..5 {
        client
            .post(format!("{base}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &session)
            .json(&check_body(
                &format!("check-{i}"),
                &format!("https://example.com/{i}"),
            ))
            .send()
            .await
            .unwrap();
    }
    let page = |query: &str| {
        let (client, base, session) = (client.clone(), base.clone(), session.clone());
        let query = query.to_string();
        async move {
            client
                .get(format!("{base}/api/v1/admin/checks?{query}"))
                .header(reqwest::header::COOKIE, session)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };

    let first = page("page=0&limit=2").await;
    assert_eq!(first["checks"].as_array().unwrap().len(), 2);
    assert_eq!(first["total"], 5);
    assert_eq!(first["has_more"], true);

    let last = page("page=2&limit=2").await;
    assert_eq!(last["checks"].as_array().unwrap().len(), 1);
    assert_eq!(last["has_more"], false);

    let past_end = page("page=99&limit=2").await;
    assert!(past_end["checks"].as_array().unwrap().is_empty());
    assert_eq!(past_end["total"], 5, "paging past the end is not an error");

    // An over-cap request is clamped rather than refused: the caller gets a
    // legal page, not a 400 telling it a number it never chose was too big.
    let huge = page("limit=100000").await;
    assert_eq!(huge["checks"].as_array().unwrap().len(), 5);
    assert_eq!(huge["has_more"], false);
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
    use lighthouse::config::{CheckConfig, MatchMode};

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
        .ingest(vec![lighthouse::models::CheckResult {
            check_id: ddns_id,
            agent_id: "agent-a".into(),
            zone: "us-east-a".into(),
            region: "us-east".into(),
            kind: lighthouse::models::CheckKind::Dns,
            timestamp: chrono::Utc::now(),
            latency_ms: 12,
            status_code: None,
            body_matched: None,
            error: Some(format!(
                "resolved to {secret}, expected one of [\"203.0.113.9\"]"
            )),
            failure: Some(lighthouse::models::FailureKind::UnexpectedDnsAnswer),
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
    let expired = lighthouse::models::FailureKind::CertificateExpired { days: 4129 };
    assert_eq!(expired.summary(), "Certificate expired 4129 days ago");

    let soon = lighthouse::models::FailureKind::CertificateExpiring { days: 6 };
    assert_eq!(soon.summary(), "Certificate expires in 6 days");

    // Singular and same-day read as English, not as "1 days".
    assert_eq!(
        lighthouse::models::FailureKind::CertificateExpiring { days: 1 }.summary(),
        "Certificate expires in 1 day"
    );
    assert_eq!(
        lighthouse::models::FailureKind::CertificateExpired { days: 0 }.summary(),
        "Certificate expired today"
    );

    // No summary may quote an observed value.
    for kind in [
        lighthouse::models::FailureKind::UnexpectedDnsAnswer,
        lighthouse::models::FailureKind::UnexpectedContent,
        lighthouse::models::FailureKind::NameResolution,
        lighthouse::models::FailureKind::TlsError,
        lighthouse::models::FailureKind::Other,
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
    use lighthouse::config::{CheckConfig, MetricLabel, MetricOptions};
    use lighthouse::models::{CheckKind, CheckResult};
    use lighthouse::openmetrics::MetricSample;

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
            kind: lighthouse::models::CheckKind::Dns,
            ..Default::default()
        })
        .await;

    // The exact leak the public path redacts: a resolved address and the
    // expected values, verbatim from the agent.
    let leaked = r#"resolved to 97.7.61.14, expected one of ["7.…"]"#;
    let now = chrono::Utc::now().timestamp_millis();
    let result =
        |check: &str, agent: &str, ts: i64, region: &str, zone: &str, error: Option<String>| {
            lighthouse::models::CheckResult {
                check_id: cid(check),
                agent_id: agent.into(),
                zone: zone.into(),
                region: region.into(),
                kind: lighthouse::models::CheckKind::Dns,
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
