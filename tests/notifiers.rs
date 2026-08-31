//! Where alerts go, and which checks are allowed to send them.
//!
//! Two properties carry this feature. The first is that the notifier list is
//! *live*: it used to be read once at boot into a spawned dispatcher, so a list
//! edited anywhere but the config file changed nothing until a restart — and
//! with an empty config there was no dispatcher at all, so the first notifier
//! added could never fire. The second is that a check may opt out: some checks
//! are just checks, and their outage should be recorded without paging anyone.
//!
//! The stand-ins are the webhook sinks playing real alert endpoints. They are
//! what makes "who was paged?" observable rather than inferred from a log line.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::post;
use axum::{extract::State, Json, Router};
use parking_lot::Mutex;
use serde_json::{json, Value};

use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::models::{CheckKind, CheckResult};

const PASSWORD: &str = "correct horse battery staple";
const LOUD: &str = "payments-api";
/// A check worth probing and charting, not worth waking anyone for.
const QUIET: &str = "batch-importer";
/// Routed to one named notifier each, not to the defaults.
const TO_A: &str = "payments-db";
const TO_B: &str = "search-index";

type Sink = Arc<Mutex<Vec<Value>>>;

/// Served with `ConnectInfo` because the login route rate-limits per peer
/// address and 500s without it, exactly as `main.rs` serves the router.
async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = app.into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

/// An alert endpoint that records what it was sent.
async fn alert_sink() -> (Sink, SocketAddr) {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/",
            post(
                |State(s): State<Sink>, Json(body): Json<Value>| async move {
                    s.lock().push(body);
                    StatusCode::OK
                },
            ),
        )
        .with_state(sink.clone());
    let addr = serve(app).await;
    (sink, addr)
}

/// An endpoint that refuses everything, the way ntfy answers for a topic you
/// may not publish to.
async fn refusing_sink() -> SocketAddr {
    serve(Router::new().route("/", post(|| async { StatusCode::NOT_FOUND }))).await
}

/// No notifiers in the config on purpose: the list starts empty and is built
/// through the admin API, which is the path an operator uses.
///
/// Four checks: one on the default route, one muted, and two routed to a
/// notifier by name — which is the shape "some to A, some to B, some to a
/// default" needs to be testable at all.
fn coordinator() -> Coordinator {
    coordinator_at(":memory:")
}

fn coordinator_at(db_path: &str) -> Coordinator {
    let check = |name: &str, notify: bool, notifiers: &[&str]| CheckConfig {
        name: name.into(),
        kind: CheckKind::Http,
        target: "https://example.invalid".into(),
        cron: "0 * * * * *".into(),
        timeout_secs: 5,
        notify,
        notifiers: notifiers.iter().map(|s| (*s).to_string()).collect(),
        ..Default::default()
    };
    Coordinator::new(
        &CoordinatorConfig {
            listen: "127.0.0.1:0".into(),
            db_path: db_path.into(),
            admin_password: Some(PASSWORD.into()),
            ..Default::default()
        },
        &[
            check(LOUD, true, &[]),
            check(QUIET, false, &[]),
            check(TO_A, true, &["team-a"]),
            check(TO_B, true, &["team-b"]),
        ],
    )
    .unwrap()
}

/// An endpoint that fails its first `fail_first` requests and accepts the rest,
/// counting every one. A transient 500 is the case the retry queue exists for:
/// the alert used to be logged once and lost.
async fn flaky_sink(fail_first: usize) -> (Sink, Arc<Mutex<usize>>, SocketAddr) {
    type Flaky = (Sink, Arc<Mutex<usize>>, usize);
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let hits = Arc::new(Mutex::new(0usize));
    let app = Router::new()
        .route(
            "/",
            post(
                |State((s, h, fail)): State<Flaky>, Json(body): Json<Value>| async move {
                    let n = {
                        let mut h = h.lock();
                        *h += 1;
                        *h
                    };
                    if n <= fail {
                        StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        s.lock().push(body);
                        StatusCode::OK
                    }
                },
            ),
        )
        .with_state((sink.clone(), hits.clone(), fail_first));
    let addr = serve(app).await;
    (sink, hits, addr)
}

/// Reopen the store the way `agent_health.rs` does: DuckDB is single-writer, so
/// the dropped coordinator's file lock may outlive the drop by a moment.
fn reopen(db_path: &str) -> Coordinator {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match std::panic::catch_unwind(|| coordinator_at(db_path)) {
            Ok(coord) => return coord,
            Err(e) if std::time::Instant::now() >= deadline => std::panic::resume_unwind(e),
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }
}

async fn undelivered(base: &str, session: &str) -> Vec<Value> {
    let res = reqwest::Client::new()
        .get(format!("{base}/api/v1/admin/notifiers"))
        .header(reqwest::header::COOKIE, session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json::<Value>().await.unwrap()["undelivered"]
        .as_array()
        .cloned()
        .unwrap()
}

/// A notifier as the admin API takes it. `default` decides whether a check
/// that names nobody reaches it.
fn webhook(name: &str, addr: SocketAddr, default: bool) -> Value {
    json!({
        "name": name,
        "type": "webhook",
        "url": format!("http://{addr}/"),
        "default": default,
    })
}

fn result(check: &str, status_code: u16, ts_millis: i64) -> CheckResult {
    CheckResult {
        check_id: check.into(),
        agent_id: "agent-test".into(),
        zone: "z".into(),
        region: "r".into(),
        kind: CheckKind::Http,
        timestamp: chrono::DateTime::from_timestamp_millis(ts_millis).unwrap(),
        latency_ms: 10,
        status_code: Some(status_code),
        body_matched: None,
        error: None,
        failure: None,
        body: None,
        headers: None,
        metric: None,
        metrics: None,
    }
}

/// Drive one confirmed UP -> DOWN transition for `check`.
async fn take_down(coord: &Coordinator, check: &str) {
    coord.ingest(vec![result(check, 200, 1_000)]).await;
    coord.ingest(vec![result(check, 500, 2_000)]).await;
}

/// Dispatch is spawned, not awaited, so the sink fills a moment after ingest.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

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
        .unwrap()
        .to_str()
        .unwrap();
    set.split(';').next().unwrap().to_string()
}

async fn put_notifiers(base: &str, session: &str, notifiers: Value) -> (u16, Value) {
    let res = reqwest::Client::new()
        .put(format!("{base}/api/v1/admin/notifiers"))
        .header(reqwest::header::COOKIE, session)
        .json(&json!({ "notifiers": notifiers }))
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    (status, res.json().await.unwrap())
}

/// The bug this feature exists to fix: with no notifier in the config file
/// there was no dispatcher at all, so the first one added could never fire —
/// and no amount of saving would change that without a restart.
#[tokio::test]
async fn a_notifier_added_at_runtime_pages_without_a_restart() {
    let (sink, sink_addr) = alert_sink().await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    // Nothing configured yet: an outage is recorded and paged to nobody.
    take_down(&coord, LOUD).await;
    settle().await;
    assert!(sink.lock().is_empty(), "no notifier, no notification");
    assert!(
        !coord.alerts_for_test(10).is_empty(),
        "the alert itself is still recorded"
    );

    let (code, body) =
        put_notifiers(&base, &session, json!([webhook("oncall", sink_addr, true)])).await;
    assert_eq!(code, 200, "{body}");

    // Recovery is a transition too, and pages: the sink should see the recovery
    // and then the fresh outage, in that order.
    coord.ingest(vec![result(LOUD, 200, 3_000)]).await;
    settle().await;
    assert_eq!(
        sink.lock().len(),
        1,
        "the notifier is live from the moment it was saved"
    );

    coord.ingest(vec![result(LOUD, 500, 4_000)]).await;
    settle().await;

    let got = sink.lock().clone();
    let states: Vec<&str> = got.iter().map(|g| g["to"].as_str().unwrap()).collect();
    assert_eq!(states, ["up", "down"], "{got:?}");
    assert!(got.iter().all(|g| g["check_id"] == LOUD));
}

/// "Some checks are just checks." The incident is still real; nobody is woken.
#[tokio::test]
async fn a_check_with_notify_off_records_its_outage_without_paging() {
    let (sink, sink_addr) = alert_sink().await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let (code, _) =
        put_notifiers(&base, &session, json!([webhook("oncall", sink_addr, true)])).await;
    assert_eq!(code, 200);

    take_down(&coord, QUIET).await;
    settle().await;
    assert!(
        sink.lock().is_empty(),
        "a quiet check must not page: {:?}",
        sink.lock().clone()
    );
    // But it is still an outage everywhere else in the product.
    assert!(
        coord.alerts_for_test(10).iter().any(|a| a.node_id == QUIET),
        "the alert row is still written"
    );
    assert!(
        coord
            .incidents_for_test(10)
            .iter()
            .any(|i| i.checks.iter().any(|c| c.check_id == QUIET)),
        "the incident is still opened"
    );

    // And the switch is per check, not a global mute.
    take_down(&coord, LOUD).await;
    settle().await;
    assert_eq!(sink.lock().len(), 1, "the loud check still pages");
}

/// A wrong ntfy topic used to be perfectly silent: the POST succeeded, the
/// server answered 404, and nothing looked at the status.
#[tokio::test]
async fn a_test_send_reports_a_refusal_instead_of_claiming_success() {
    let refusing = refusing_sink().await;
    let (sink, working) = alert_sink().await;
    let addr = serve(coordinator().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    let test = |notifier: Value| {
        let base = base.clone();
        let session = session.clone();
        async move {
            let res = reqwest::Client::new()
                .post(format!("{base}/api/v1/admin/notifiers/test"))
                .header(reqwest::header::COOKIE, session)
                .json(&notifier)
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "a refused delivery is not a failed call");
            res.json::<Value>().await.unwrap()
        }
    };

    let bad = test(webhook("oncall", refusing, true)).await;
    assert_eq!(bad["delivered"], false, "404 is not delivery: {bad}");
    assert!(
        bad["error"].as_str().unwrap().contains("404"),
        "the reason names what the endpoint said: {bad}"
    );

    let good = test(webhook("oncall", working, true)).await;
    assert_eq!(good["delivered"], true, "{good}");
    assert_eq!(sink.lock().len(), 1, "the test actually sent something");
    assert_eq!(
        sink.lock()[0]["check_id"],
        "warden-test",
        "and it is recognisable as a test"
    );

    // A notifier that cannot work is rejected before anything is sent.
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/notifiers/test"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "name": "alerts", "type": "ntfy", "topic": "" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn the_notifier_list_is_admin_only() {
    let addr = serve(coordinator().router()).await;
    let base = format!("http://{addr}");
    let anon = reqwest::Client::new();
    let body = json!({ "notifiers": [{ "name": "oncall", "type": "webhook", "url": "http://example.invalid/" }] });

    for (what, res) in [
        (
            "read",
            anon.get(format!("{base}/api/v1/admin/notifiers"))
                .send()
                .await
                .unwrap(),
        ),
        (
            "write",
            anon.put(format!("{base}/api/v1/admin/notifiers"))
                .json(&body)
                .send()
                .await
                .unwrap(),
        ),
        (
            "test-send",
            anon.post(format!("{base}/api/v1/admin/notifiers/test"))
                .json(&json!({ "name": "oncall", "type": "webhook", "url": "http://example.invalid/" }))
                .send()
                .await
                .unwrap(),
        ),
    ] {
        // A webhook URL is the entire credential for posting into that channel,
        // and test-send would otherwise be an anonymous request generator.
        assert_eq!(res.status(), 401, "{what} must require a session");
    }
}

#[tokio::test]
async fn a_notifier_that_cannot_work_is_refused() {
    let addr = serve(coordinator().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    for (bad, expected) in [
        (
            json!([{ "name": "oncall", "type": "webhook", "url": "" }]),
            "URL",
        ),
        (
            json!([{ "name": " ", "type": "webhook", "url": "http://x.invalid/" }]),
            "name",
        ),
        (
            json!([{ "name": "oncall", "type": "webhook", "url": "example.com/hook" }]),
            "http://",
        ),
        (
            json!([{ "name": "alerts", "type": "ntfy", "topic": "  " }]),
            "topic",
        ),
        (
            json!([{ "name": "alerts", "type": "ntfy", "topic": "alerts/oops" }]),
            "slash",
        ),
    ] {
        let (code, body) = put_notifiers(&base, &session, bad.clone()).await;
        assert_eq!(code, 400, "{bad} should be refused");
        assert!(
            body["error"].as_str().unwrap().contains(expected),
            "reason should mention {expected}: {body}"
        );
    }

    // Nothing was stored by any of those.
    let (code, body) = put_notifiers(&base, &session, json!([])).await;
    assert_eq!(code, 200);
    assert_eq!(body["notifiers"], json!([]));
}

/// Whitespace in a URL is invisible in the form and fails at delivery time.
#[tokio::test]
async fn notifier_fields_are_trimmed_before_they_are_stored() {
    let addr = serve(coordinator().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    let (code, body) = put_notifiers(
        &base,
        &session,
        json!([
            { "name": "  oncall  ", "type": "webhook", "url": "  https://hooks.example.com/x  " },
            { "name": "alerts", "type": "ntfy", "topic": " alerts ", "base_url": "   " },
        ]),
    )
    .await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(body["notifiers"][0]["url"], "https://hooks.example.com/x");
    assert_eq!(
        body["notifiers"][0]["name"], "oncall",
        "the routing key too"
    );
    assert_eq!(body["notifiers"][1]["topic"], "alerts");
    assert!(
        body["notifiers"][1]["base_url"].is_null(),
        "a blank server means the default, not an empty URL: {body}"
    );
}

/// The ask: some checks to A, some to B, and the rest to a default.
#[tokio::test]
async fn checks_reach_the_notifier_they_name_and_nothing_else() {
    let (a, a_addr) = alert_sink().await;
    let (b, b_addr) = alert_sink().await;
    let (fallback, fallback_addr) = alert_sink().await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    // Only `oncall` is a default; the two teams are named routes.
    let (code, body) = put_notifiers(
        &base,
        &session,
        json!([
            webhook("team-a", a_addr, false),
            webhook("team-b", b_addr, false),
            webhook("oncall", fallback_addr, true),
        ]),
    )
    .await;
    assert_eq!(code, 200, "{body}");

    take_down(&coord, TO_A).await;
    take_down(&coord, TO_B).await;
    take_down(&coord, LOUD).await;
    settle().await;

    let names = |s: &Sink| {
        s.lock()
            .iter()
            .map(|v| v["check_id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&a), [TO_A], "team-a hears only its own check");
    assert_eq!(names(&b), [TO_B], "team-b hears only its own check");
    assert_eq!(
        names(&fallback),
        [LOUD],
        "the default hears the check that named nobody — and not the routed ones"
    );
}

/// Renaming a notifier silently unroutes every check that named it. Nothing
/// else would say so: the check keeps probing and simply stops paging.
#[tokio::test]
async fn checks_left_pointing_at_a_missing_notifier_are_reported() {
    let (_sink, sink_addr) = alert_sink().await;
    let addr = serve(coordinator().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    let (_, body) = put_notifiers(
        &base,
        &session,
        json!([
            webhook("team-a", sink_addr, false),
            webhook("oncall", sink_addr, true)
        ]),
    )
    .await;
    assert_eq!(
        body["unrouted_checks"],
        json!([TO_B]),
        "only the check naming a notifier nobody answers to: {body}"
    );

    // Rename team-a away and its check joins the orphans.
    let (_, body) = put_notifiers(
        &base,
        &session,
        json!([
            webhook("team-a-renamed", sink_addr, false),
            webhook("oncall", sink_addr, true)
        ]),
    )
    .await;
    assert_eq!(body["unrouted_checks"], json!([TO_A, TO_B]), "{body}");
}

#[tokio::test]
async fn two_notifiers_cannot_share_a_name() {
    let (_sink, sink_addr) = alert_sink().await;
    let addr = serve(coordinator().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    let (code, body) = put_notifiers(
        &base,
        &session,
        json!([
            webhook("oncall", sink_addr, true),
            webhook("oncall", sink_addr, false)
        ]),
    )
    .await;
    assert_eq!(code, 400, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("oncall"),
        "the reason names the collision: {body}"
    );
}

/// A one-shot SMTP server: just enough protocol for a real SMTP client to
/// complete a session, and it keeps what was in the DATA phase.
///
/// It advertises no extensions at all, which is deliberate — no STARTTLS, no
/// AUTH — so a notifier that quietly needed either would fail here rather than
/// pass by accident.
async fn smtp_sink() -> (Arc<Mutex<Vec<String>>>, SocketAddr) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured = seen.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let captured = captured.clone();
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut lines = BufReader::new(read).lines();
                write.write_all(b"220 sink ESMTP\r\n").await.ok();
                let mut body = String::new();
                let mut in_data = false;
                while let Ok(Some(line)) = lines.next_line().await {
                    if in_data {
                        if line == "." {
                            in_data = false;
                            captured.lock().push(std::mem::take(&mut body));
                            write.write_all(b"250 queued\r\n").await.ok();
                        } else {
                            body.push_str(&line);
                            body.push('\n');
                        }
                        continue;
                    }
                    let verb = line.split_whitespace().next().unwrap_or("").to_uppercase();
                    let reply: &[u8] = match verb.as_str() {
                        "EHLO" | "HELO" => b"250 sink\r\n",
                        "DATA" => {
                            in_data = true;
                            b"354 go ahead\r\n"
                        }
                        "QUIT" => b"221 bye\r\n",
                        _ => b"250 ok\r\n",
                    };
                    write.write_all(reply).await.ok();
                    if verb == "QUIT" {
                        return;
                    }
                }
            });
        }
    });
    (seen, addr)
}

/// Email is the only backend that is not an HTTP POST, so it is the only one
/// whose delivery path nothing else exercises.
#[tokio::test]
async fn an_email_notifier_delivers_over_smtp() {
    let (mailbox, smtp) = smtp_sink().await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    let (code, body) = put_notifiers(
        &base,
        &session,
        json!([{
            "name": "ops-mail",
            "type": "email",
            "host": smtp.ip().to_string(),
            "port": smtp.port(),
            "tls": "none",
            "from": "warden@example.com",
            "to": "ops@example.com",
            "default": true,
        }]),
    )
    .await;
    assert_eq!(code, 200, "{body}");

    take_down(&coord, LOUD).await;
    settle().await;

    let mail = mailbox.lock().clone();
    assert_eq!(mail.len(), 1, "one confirmed transition, one mail");
    assert!(
        mail[0].contains("ops@example.com") && mail[0].contains(LOUD) && mail[0].contains("down"),
        "the mail names the recipient, the check and where it went: {}",
        mail[0]
    );

    // And a check that pages nobody sends no mail either — the mute is not
    // per-backend.
    take_down(&coord, QUIET).await;
    settle().await;
    assert_eq!(mailbox.lock().len(), 1, "the muted check stays quiet");
}

/// Slack answers 400 `invalid_payload` for JSON without `text`, so pointing the
/// generic webhook at a Slack URL looks configured and delivers nothing. That
/// difference in payload is the entire reason `slack` is its own kind.
#[tokio::test]
async fn a_slack_notifier_sends_the_payload_slack_accepts() {
    let (sink, slack) = alert_sink().await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    let (code, body) = put_notifiers(
        &base,
        &session,
        json!([{
            "name": "slack-ops",
            "type": "slack",
            "webhook_url": format!("http://{slack}/"),
            "default": true,
        }]),
    )
    .await;
    assert_eq!(code, 200, "{body}");

    take_down(&coord, LOUD).await;
    settle().await;

    let got = sink.lock().clone();
    assert_eq!(got.len(), 1, "one transition, one message");
    let text = got[0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("Slack needs a `text` field, got {}", got[0]));
    assert!(
        text.contains(LOUD) && text.contains("down"),
        "the message names the check and its new state: {text}"
    );
    assert!(
        got[0].get("check_id").is_none(),
        "not the webhook shape, which Slack refuses: {}",
        got[0]
    );
}

/// The gap this closes: a notifier that answers 500 once used to lose the page
/// entirely — one `warn!` and the transition was never sent to anyone.
#[tokio::test]
async fn a_notification_that_fails_is_queued_and_retried_until_it_lands() {
    let (sink, hits, flaky) = flaky_sink(1).await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let (code, body) =
        put_notifiers(&base, &session, json!([webhook("oncall", flaky, true)])).await;
    assert_eq!(code, 200, "{body}");

    take_down(&coord, LOUD).await;
    settle().await;
    assert_eq!(*hits.lock(), 1, "one attempt was made");
    assert!(sink.lock().is_empty(), "and it was refused");

    let queued = undelivered(&base, &session).await;
    assert_eq!(queued.len(), 1, "the refusal is queued: {queued:?}");
    assert_eq!(queued[0]["check_id"], LOUD);
    assert_eq!(queued[0]["notifier"], "oncall");
    assert_eq!(queued[0]["to"], "down");
    assert_eq!(queued[0]["attempts"], 1);
    assert_eq!(queued[0]["given_up"], false);
    assert!(
        queued[0]["error"].as_str().unwrap().contains("500"),
        "and it says what the endpoint answered: {}",
        queued[0]
    );

    // Nothing is due yet — the backoff is real, not a busy loop.
    let now = chrono::Utc::now().timestamp_millis();
    assert_eq!(coord.retry_due(now).await, 0, "not due yet");
    assert_eq!(*hits.lock(), 1, "so nothing was sent");

    // Past the backoff it goes out, and the queue empties.
    assert_eq!(coord.retry_due(now + 60_000).await, 1);
    assert_eq!(*hits.lock(), 2);
    assert_eq!(sink.lock().len(), 1, "the alert finally reached the sink");
    assert_eq!(sink.lock()[0]["check_id"], LOUD);
    assert!(
        undelivered(&base, &session).await.is_empty(),
        "delivered means gone: there is no 'sent' row to accumulate"
    );
}

/// Persistence is the whole point of the word "queue" here: a coordinator
/// restarted mid-outage still owes those pages.
#[tokio::test]
async fn a_queued_notification_outlives_a_restart_and_follows_the_edited_notifier() {
    let dir = std::env::temp_dir().join(format!("warden-retry-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("retry.duckdb");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("duckdb.wal"));
    let db = path.to_str().unwrap().to_string();

    let refusing = refusing_sink().await;
    let (sink, hits, working) = flaky_sink(0).await;

    {
        let coord = coordinator_at(&db);
        let addr = serve(coord.clone().router()).await;
        let base = format!("http://{addr}");
        let session = login(&base).await;
        let (code, _) =
            put_notifiers(&base, &session, json!([webhook("oncall", refusing, true)])).await;
        assert_eq!(code, 200);
        take_down(&coord, LOUD).await;
        settle().await;
        assert_eq!(
            undelivered(&base, &session).await.len(),
            1,
            "queued before the restart"
        );
    }

    let coord = reopen(&db);
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let queued = undelivered(&base, &session).await;
    assert_eq!(queued.len(), 1, "still owed after the restart: {queued:?}");
    assert_eq!(queued[0]["attempts"], 1, "and it did not lose its attempts");

    // Repointing the notifier fixes its queued notifications too: the row holds
    // the routing name, and the backend behind it is resolved at each attempt.
    let (code, _) = put_notifiers(&base, &session, json!([webhook("oncall", working, true)])).await;
    assert_eq!(code, 200);
    let now = chrono::Utc::now().timestamp_millis();
    assert_eq!(coord.retry_due(now + 60_000).await, 1);
    assert_eq!(*hits.lock(), 1);
    assert_eq!(sink.lock()[0]["check_id"], LOUD);
    assert!(undelivered(&base, &session).await.is_empty());

    std::fs::remove_file(&path).ok();
    std::fs::remove_file(path.with_extension("duckdb.wal")).ok();
}

/// An endpoint that is simply broken must stop being retried, and must not go
/// quiet about it: "nobody was paged for this outage" is the fact the panel has
/// to be able to state.
#[tokio::test]
async fn attempts_run_out_and_the_queue_records_that_nobody_was_paged() {
    let refusing = refusing_sink().await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let (code, _) =
        put_notifiers(&base, &session, json!([webhook("oncall", refusing, true)])).await;
    assert_eq!(code, 200);

    take_down(&coord, LOUD).await;
    settle().await;

    // Six attempts total, the first one included; each retry is due only after
    // its own doubling backoff, so walk the clock forward generously.
    let mut now = chrono::Utc::now().timestamp_millis();
    for _ in 0..10 {
        now += 60 * 60 * 1000;
        assert_eq!(coord.retry_due(now).await, 0, "it never succeeds");
    }

    let queued = undelivered(&base, &session).await;
    assert_eq!(queued.len(), 1, "the row stays, as the record of a miss");
    assert_eq!(
        queued[0]["attempts"], 6,
        "and stops at the limit: {queued:?}"
    );
    assert_eq!(queued[0]["given_up"], true);
    assert!(
        queued[0]["next_attempt"].is_null(),
        "given up means no next attempt: {}",
        queued[0]
    );

    // A given-up row is not picked up again, however far the clock moves.
    assert_eq!(coord.retry_due(now + 86_400_000).await, 0);
    assert_eq!(
        undelivered(&base, &session).await[0]["attempts"],
        6,
        "no further attempts were made"
    );
}

// ---- Maintenance windows ----
//
// `take_down` drives the DOWN at ts 2_000, so a window is placed around that
// instant rather than around the wall clock: suppression is judged by when the
// transition happened, not by when the batch arrived.

fn window(title: &str, starts_at: i64, ends_at: i64, checks: &[&str], announce: bool) -> Value {
    json!({
        "title": title,
        "starts_at": starts_at,
        "ends_at": ends_at,
        "checks": checks,
        "announce": announce,
    })
}

async fn put_maintenance(base: &str, session: &str, windows: Value) -> (u16, Value) {
    let res = reqwest::Client::new()
        .put(format!("{base}/api/v1/admin/maintenance"))
        .header(reqwest::header::COOKIE, session)
        .json(&json!({ "windows": windows }))
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    (status, res.json().await.unwrap())
}

/// Anonymous, deliberately: the announcement is for visitors.
async fn public_status(base: &str) -> Value {
    let res = reqwest::Client::new()
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "the status page is public");
    res.json().await.unwrap()
}

/// The whole point of a window: the work was planned, so nobody is woken for
/// it. Everything else about the outage is still recorded, exactly as it is for
/// a check with `notify = false`.
#[tokio::test]
async fn a_maintenance_window_suppresses_the_page_without_hiding_the_outage() {
    let (sink, sink_addr) = alert_sink().await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let (code, _) =
        put_notifiers(&base, &session, json!([webhook("oncall", sink_addr, true)])).await;
    assert_eq!(code, 200);
    let (code, body) = put_maintenance(
        &base,
        &session,
        json!([window("db upgrade", 1_500, 3_000, &[LOUD], true)]),
    )
    .await;
    assert_eq!(code, 200, "{body}");

    take_down(&coord, LOUD).await;
    settle().await;
    assert!(
        sink.lock().is_empty(),
        "a scheduled outage must not page: {:?}",
        sink.lock().clone()
    );
    assert!(
        coord.alerts_for_test(10).iter().any(|a| a.node_id == LOUD),
        "the alert row is still written"
    );
    assert!(
        coord
            .incidents_for_test(10)
            .iter()
            .any(|i| i.checks.iter().any(|c| c.check_id == LOUD)),
        "the incident is still opened"
    );

    // A window that is over announces nothing, so the notice is read at a
    // moment the window covers.
    let notices = public_status(&base).await["maintenance"].clone();
    assert!(
        notices.as_array().is_some_and(|n| n.is_empty()),
        "the window ended in 1970, so nothing is announced now: {notices}"
    );
}

/// The mutation guard for the other test: suppression has to be bounded by the
/// window, or a single booking silences the fleet forever.
#[tokio::test]
async fn a_transition_outside_the_window_still_pages() {
    let (sink, sink_addr) = alert_sink().await;
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let (code, _) =
        put_notifiers(&base, &session, json!([webhook("oncall", sink_addr, true)])).await;
    assert_eq!(code, 200);
    // Starts after the transition this test drives, and names the check that
    // falls — so only the timing can be what lets the page through.
    let (code, body) = put_maintenance(
        &base,
        &session,
        json!([window("db upgrade", 5_000, 6_000, &[LOUD], false)]),
    )
    .await;
    assert_eq!(code, 200, "{body}");

    take_down(&coord, LOUD).await;
    settle().await;
    assert_eq!(sink.lock().len(), 1, "an unscheduled outage still pages");
}

/// An announced window is the operator's sentence, published to anonymous
/// visitors while it runs — and only while it runs.
#[tokio::test]
async fn an_announced_window_is_published_while_it_runs() {
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let now = chrono::Utc::now().timestamp_millis();
    let (code, body) = put_maintenance(
        &base,
        &session,
        json!([
            window("db upgrade", now - 60_000, now + 600_000, &[], true),
            window("silent patching", now - 60_000, now + 600_000, &[], false),
            window("last month", now - 86_400_000, now - 3_600_000, &[], true),
        ]),
    )
    .await;
    assert_eq!(code, 200, "{body}");

    let notices = public_status(&base).await["maintenance"].clone();
    let titles: Vec<&str> = notices
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["title"].as_str().unwrap())
        .collect();
    assert_eq!(
        titles,
        vec!["db upgrade"],
        "only the announced, running window: {notices}"
    );
}

/// A window is a licence to go quiet, so an anonymous writer could silence the
/// whole fleet with one PUT.
#[tokio::test]
async fn maintenance_windows_are_admin_only() {
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let res = client
        .get(format!("{base}/api/v1/admin/maintenance"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401, "reading the schedule needs a session");
    let res = client
        .put(format!("{base}/api/v1/admin/maintenance"))
        .json(&json!({ "windows": [window("hush", 0, 9_999_999_999_999, &[], false)] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401, "and so does booking one");

    // The refused PUT changed nothing: the check still pages.
    let (sink, sink_addr) = alert_sink().await;
    let session = login(&base).await;
    let (code, _) =
        put_notifiers(&base, &session, json!([webhook("oncall", sink_addr, true)])).await;
    assert_eq!(code, 200);
    take_down(&coord, LOUD).await;
    settle().await;
    assert_eq!(sink.lock().len(), 1, "no window was stored");
}

/// A window that ends before it starts covers nothing, so it would look like a
/// window that simply does not work.
#[tokio::test]
async fn a_window_that_ends_before_it_starts_is_refused() {
    let coord = coordinator();
    let addr = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let (code, body) = put_maintenance(
        &base,
        &session,
        json!([window("backwards", 3_000, 1_500, &[], false)]),
    )
    .await;
    assert_eq!(code, 400, "{body}");
    let (code, body) = put_maintenance(
        &base,
        &session,
        json!([window("", 1_000, 2_000, &[], false)]),
    )
    .await;
    assert_eq!(code, 400, "a window with no title says nothing: {body}");
}
