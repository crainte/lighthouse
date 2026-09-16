//! Peer replication between two live coordinators.
//!
//! Two real `Coordinator`s talking over a real localhost socket: node A
//! observes a transition, node B learns it by gossip. Nothing internal is
//! mocked — the only stand-ins are the webhook sinks playing the part of real
//! alert endpoints, and they are what makes "who paged?" directly observable.
//!
//! The three properties under test are the ones that make clustering safe
//! rather than merely present:
//!
//!   1. the peer's published state actually converges,
//!   2. the peer does **not** page a second time for one outage,
//!   3. the endpoint is not anonymously writable.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::post;
use axum::{extract::State, Json, Router};
use parking_lot::Mutex;
use serde_json::Value;

use uuid::Uuid;
use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::models::{CheckId, CheckKind, CheckResult};
use warden::notify::{Notifier, NotifierKind};

/// Peers authenticate with their own secret, never an agent's.
const PEER_TOKEN: &str = "cluster-peer-secret";
const CHECK: &str = "web";

type Sink = Arc<Mutex<Vec<Value>>>;
/// Captured gossip POSTs: the Authorization header and the body.
type Seen = Arc<Mutex<Vec<(Option<String>, Value)>>>;

/// Bind an ephemeral port and serve `app`. Bound before `serve`, so the OS
/// accepts into the backlog immediately and no readiness sleep is needed.
async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
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

fn cid(name: &str) -> CheckId {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes())
}

fn coordinator(alerts_to: SocketAddr, peers: Vec<String>) -> Coordinator {
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        notifiers: vec![Notifier {
            name: "alerts".into(),
            default: true,
            kind: NotifierKind::Webhook {
                url: format!("http://{alerts_to}/"),
            },
        }],
        peer_token: Some(PEER_TOKEN.into()),
        peers,
        ..Default::default()
    })
    .unwrap();
    coord.create_check_for_test(CheckConfig {
        id: cid(CHECK),
        name: CHECK.into(),
        kind: CheckKind::Http,
        target: "https://example.invalid".into(),
        cron: "0 * * * * *".into(),
        timeout_secs: 5,
        ..Default::default()
    });
    coord
}

/// A probe observation. `status_code` drives the default eval: 200 -> UP,
/// 500 -> DOWN.
fn result(status_code: u16, ts_millis: i64) -> CheckResult {
    CheckResult {
        check_id: cid(CHECK),
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

/// Poll until `f` returns `Some`, or give up. Gossip is spawned rather than
/// awaited, so the peer converges a moment after `ingest` returns.
async fn eventually<T, F, Fut>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    for _ in 0..400 {
        if let Some(v) = f().await {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn get_json(url: &str) -> Value {
    reqwest::get(url).await.unwrap().json().await.unwrap()
}

#[tokio::test]
async fn a_transition_reaches_the_peer_without_paging_twice() {
    let (sink_a, addr_sink_a) = alert_sink().await;
    let (sink_b, addr_sink_b) = alert_sink().await;

    // Peer B is a plain coordinator with no peers of its own: it receives.
    let coord_b = coordinator(addr_sink_b, vec![]);
    let addr_b = serve(coord_b.clone().router()).await;

    // Node A replicates to B.
    let coord_a = coordinator(addr_sink_a, vec![format!("http://{addr_b}")]);

    // The first observation only establishes a baseline; the second flips it.
    coord_a.ingest(vec![result(200, 1_000)]).await;
    coord_a.ingest(vec![result(500, 2_000)]).await;

    // 1. The observing node pages locally. Asserted first: if this fails the
    //    fixture never produced a transition, and every peer assertion below
    //    would be vacuous rather than wrong.
    let paged_a = eventually("node A to page", || async {
        let g = sink_a.lock();
        (!g.is_empty()).then(|| g.len())
    })
    .await;
    assert_eq!(paged_a, 1, "the observing node pages once");

    // 1. B's published state converges on A's verdict.
    let status = eventually("peer status to go down", || async {
        let v = get_json(&format!("http://{addr_b}/api/v1/status")).await;
        let c = v["checks"]
            .as_array()?
            .iter()
            .find(|c| c["check_id"] == cid(CHECK).to_string())?
            .clone();
        (c["status"] == "down").then_some(c)
    })
    .await;
    assert_eq!(status["status"], "down");

    // ...and the alert feed agrees, carrying A's timestamp rather than B's clock.
    let alerts = get_json(&format!("http://{addr_b}/api/v1/alerts")).await;
    let replicated = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["node_id"] == cid(CHECK).to_string())
        .expect("peer should have recorded the replicated alert");
    assert_eq!(replicated["previous_state"], "up");
    assert_eq!(replicated["current_state"], "down");
    assert_eq!(replicated["timestamp"], 2_000);

    // 2. Exactly one page for one outage. A observed it, so A pages; B learned
    //    it second-hand and must stay quiet, or a three-node cluster wakes
    //    somebody three times.
    let paged_a = eventually("node A to page", || async {
        let g = sink_a.lock();
        (!g.is_empty()).then(|| g.len())
    })
    .await;
    assert_eq!(paged_a, 1, "the observing node pages once");
    assert!(
        sink_b.lock().is_empty(),
        "peer must not page for a transition it only heard about"
    );
}

#[tokio::test]
async fn gossip_is_not_anonymously_writable() {
    let (_sink, addr_sink) = alert_sink().await;
    let coord = coordinator(addr_sink, vec![]);
    let addr = serve(coord.clone().router()).await;

    let forged = serde_json::json!({
        "check_id": cid(CHECK),
        "from": "up",
        "to": "down",
        "timestamp": 9_999,
    });

    let unauthenticated = reqwest::Client::new()
        .post(format!("http://{addr}/internal/gossip"))
        .json(&forged)
        .send()
        .await
        .unwrap();
    assert_eq!(
        unauthenticated.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "an anonymous POST could otherwise mark every check down"
    );

    // The forged transition must not have landed.
    let v = get_json(&format!("http://{addr}/api/v1/alerts")).await;
    assert!(
        v["alerts"].as_array().unwrap().is_empty(),
        "rejected gossip must not be applied"
    );

    // The same body with the token is accepted, so the 401 above is really
    // about authentication and not a malformed request.
    let authenticated = reqwest::Client::new()
        .post(format!("http://{addr}/internal/gossip"))
        .bearer_auth(PEER_TOKEN)
        .json(&forged)
        .send()
        .await
        .unwrap();
    assert!(authenticated.status().is_success());

    let v = get_json(&format!("http://{addr}/api/v1/alerts")).await;
    assert_eq!(v["alerts"].as_array().unwrap().len(), 1);
}

/// A check `Status` round-trips through the peer unchanged. Recovery matters as
/// much as failure: a cluster that replicates DOWN but not UP leaves every peer
/// showing a permanent outage.
#[tokio::test]
async fn recovery_replicates_too() {
    let (_sink_a, addr_sink_a) = alert_sink().await;
    let (_sink_b, addr_sink_b) = alert_sink().await;

    let coord_b = coordinator(addr_sink_b, vec![]);
    let addr_b = serve(coord_b.clone().router()).await;
    let coord_a = coordinator(addr_sink_a, vec![format!("http://{addr_b}")]);

    coord_a.ingest(vec![result(200, 1_000)]).await;
    coord_a.ingest(vec![result(500, 2_000)]).await;
    coord_a.ingest(vec![result(200, 3_000)]).await;

    let status = eventually("peer to recover", || async {
        let v = get_json(&format!("http://{addr_b}/api/v1/status")).await;
        let c = v["checks"]
            .as_array()?
            .iter()
            .find(|c| c["check_id"] == cid(CHECK).to_string())?
            .clone();
        (c["status"] == "up").then_some(c)
    })
    .await;
    assert_eq!(status["status"], "up");

    // Both legs of the flip replicated, newest first.
    let v = get_json(&format!("http://{addr_b}/api/v1/alerts")).await;
    let legs: Vec<(&str, &str)> = v["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            (
                a["previous_state"].as_str().unwrap(),
                a["current_state"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(legs, vec![("down", "up"), ("up", "down")]);
}

/// POST a transition to a coordinator's gossip endpoint, authenticated.
async fn gossip_to(addr: SocketAddr, from: &str, to: &str, ts: i64) {
    let r = reqwest::Client::new()
        .post(format!("http://{addr}/internal/gossip"))
        .bearer_auth(PEER_TOKEN)
        .json(&serde_json::json!({
            "check_id": cid(CHECK), "from": from, "to": to, "timestamp": ts,
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "gossip rejected: {}", r.status());
}

/// The check's published status on a coordinator.
async fn peer_status(addr: SocketAddr) -> String {
    let v = get_json(&format!("http://{addr}/api/v1/status")).await;
    v["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["check_id"] == cid(CHECK).to_string())
        .expect("check should be listed")["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// `Status` crosses the wire in its own serde form rather than the hand-rolled
/// string match the first attempt used, so an un-evaluable verdict stays
/// UNKNOWN instead of being coerced to UP or DOWN.
#[tokio::test]
async fn unknown_is_carried_faithfully() {
    let (_sink, addr_sink) = alert_sink().await;
    let coord = coordinator(addr_sink, vec![]);
    let addr = serve(coord.clone().router()).await;

    // Move the check off its default first. UNKNOWN *is* the default, so
    // asserting it directly would pass even if gossip did nothing at all.
    gossip_to(addr, "up", "down", 4_000).await;
    assert_eq!(
        peer_status(addr).await,
        "down",
        "baseline must not be default"
    );

    gossip_to(addr, "down", "unknown", 4_242).await;
    assert_eq!(peer_status(addr).await, "unknown");
}

/// The peer credential is actually presented on the wire. Without this, a
/// token-protected cluster would silently stop replicating: every POST would
/// 401 and the only symptom is a peer that quietly drifts.
#[tokio::test]
async fn broadcast_presents_the_agent_token() {
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let app =
        Router::new()
            .route(
                "/internal/gossip",
                post(
                    |State(s): State<Seen>,
                     headers: axum::http::HeaderMap,
                     Json(body): Json<Value>| async move {
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_owned);
                        s.lock().push((auth, body));
                        StatusCode::NO_CONTENT
                    },
                ),
            )
            .with_state(seen.clone());
    let addr = serve(app).await;

    let (_sink, addr_sink) = alert_sink().await;
    let coord = coordinator(addr_sink, vec![format!("http://{addr}")]);
    coord.ingest(vec![result(200, 1_000)]).await;
    coord.ingest(vec![result(500, 2_000)]).await;

    let got = eventually("peer to receive a gossip POST", || async {
        let g = seen.lock();
        (!g.is_empty()).then(|| g[0].clone())
    })
    .await;

    // Against the const, not a copy of it: the literal here was left behind by
    // the rename to a peer-only token and asserted a secret nothing used.
    assert_eq!(got.0, Some(format!("Bearer {PEER_TOKEN}")));
    assert_eq!(got.1["check_id"], cid(CHECK).to_string());
    assert_eq!(got.1["to"], "down");
}
