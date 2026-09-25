//! Admin API integration tests for session/auth boundaries, HTTP security
//! headers, request-body size limits, notifier secret redaction, the
//! notification dispatcher's isolation from ingest, fleet-listing privacy,
//! and trusted-proxy `X-Forwarded-For` handling.
//!
//! Split from the original `admin_api.rs`; see `tests/common/mod.rs` for the
//! shared fixtures.

mod common;

use serde_json::{json, Value};

use common::*;
use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::notify::{Notifier, NotifierKind};

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

/// Login rate limiting keys on the raw peer address unless a proxy is
/// explicitly trusted — see `crate::security::client_ip`. This is the
/// end-to-end version of `security::tests`: the point is not the header
/// parser, it is that the handler wires it up at all.
#[tokio::test]
async fn a_spoofed_forwarded_for_header_is_ignored_without_a_trusted_proxy() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Five failed attempts, each claiming a different attacker-chosen
    // client IP. Nothing here is a configured trusted proxy, so every
    // attempt is still billed to the real peer (127.0.0.1), and it locks out
    // regardless of what the header says.
    for i in 0..5 {
        let res = client
            .post(format!("{base}/api/v1/login"))
            .header("X-Forwarded-For", format!("203.0.113.{i}"))
            .json(&json!({ "password": "wrong" }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 401);
    }
    let res = client
        .post(format!("{base}/api/v1/login"))
        .header("X-Forwarded-For", "203.0.113.99")
        .json(&json!({ "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        429,
        "the raw peer is rate limited no matter what X-Forwarded-For claims"
    );
}

#[tokio::test]
async fn a_trusted_proxys_forwarded_for_is_honoured_per_client() {
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        admin_password: Some(PASSWORD.into()),
        trusted_proxies: vec!["127.0.0.1".into()],
        ..Default::default()
    })
    .await
    .unwrap();
    let addr = spawn_server(coord);
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Five failed attempts from one claimed client behind the trusted proxy
    // locks that client out...
    for _ in 0..5 {
        let res = client
            .post(format!("{base}/api/v1/login"))
            .header("X-Forwarded-For", "203.0.113.10")
            .json(&json!({ "password": "wrong" }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 401);
    }
    let locked_out = client
        .post(format!("{base}/api/v1/login"))
        .header("X-Forwarded-For", "203.0.113.10")
        .json(&json!({ "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(locked_out.status(), 429);

    // ...but a different client is a separate bucket and logs in on the
    // first correct attempt, even arriving through an extra hop (127.0.0.1
    // again) that is itself trusted and so is skipped rather than mistaken
    // for the client.
    let other_client = client
        .post(format!("{base}/api/v1/login"))
        .header("X-Forwarded-For", "203.0.113.11, 127.0.0.1")
        .json(&json!({ "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(other_client.status(), 200);
}
