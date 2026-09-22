//! Config-plane replication: checks, settings and credentials.
//!
//! Real `Coordinator`s over real localhost sockets, same harness style as
//! `tests/cluster.rs` and `tests/gossip.rs`. Config replication is a second,
//! separate plane from result replication — last-writer-wins by HLC, applied
//! locally on every node, never observer-owned the way a probe result is —
//! and these tests are chosen to defend exactly that separation:
//!
//!   1. an edit made on one node actually takes effect on its peer, including
//!      the in-memory path an agent reads rather than only the admin table —
//!      a stale cache is the most likely first bug here, and a table-only
//!      assertion would not catch it;
//!   2. a delete is a tombstone, not merely an absence: replaying a stale,
//!      older create must not resurrect it;
//!   3. two nodes editing the same row converge on the higher HLC regardless
//!      of the order the edits arrive in — that is the entire reason to use
//!      an HLC instead of "whoever we heard from last";
//!   4. the same LWW rule applies to settings, not only checks;
//!   5. a credential minted on one node works, and stops working once
//!      revoked, on every other node — an operator should not have to mint
//!      once per coordinator;
//!   6. a session survives crossing to a different node, because both share
//!      the signing key derived from `peer_token`;
//!   7. **a replicated config entry must never fire a notifier.** Config
//!      takes effect locally on its own; only an observed result transition
//!      may page. Blurring the two planes would mean editing a check pages
//!      somebody.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::{json, Value};

use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::models::CheckKind;
use warden::notify::{Notifier, NotifierKind};

const PEER_TOKEN: &str = "cluster-peer-secret";
const ADMIN_PASSWORD: &str = "correct horse battery staple";
const CHECK: &str = "web";

type Sink = Arc<Mutex<Vec<Value>>>;

async fn alert_sink() -> (Sink, SocketAddr) {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let app = axum::Router::new()
        .route(
            "/",
            axum::routing::post(
                |axum::extract::State(s): axum::extract::State<Sink>,
                 axum::Json(body): axum::Json<Value>| async move {
                    s.lock().push(body);
                    axum::http::StatusCode::OK
                },
            ),
        )
        .with_state(sink.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (sink, addr)
}

struct Node {
    coord: Coordinator,
    addr: SocketAddr,
    sink: Sink,
}

/// Build and serve one coordinator seeded with `checks`. `checks` is
/// deliberately a parameter rather than a constant: the check-replication
/// tests want each node to start with *nothing*, so the check they observe
/// on the peer can only have arrived by replication.
async fn spawn_node(node_id: &str, peers: Vec<String>, checks: Vec<CheckConfig>) -> Node {
    let (sink, addr_sink) = alert_sink().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        admin_password: Some(ADMIN_PASSWORD.into()),
        notifiers: vec![Notifier {
            name: "alerts".into(),
            default: true,
            kind: NotifierKind::Webhook {
                url: format!("http://{addr_sink}/"),
            },
        }],
        peer_token: Some(PEER_TOKEN.into()),
        node_id: Some(node_id.into()),
        advertise_url: Some(format!("http://{addr}")),
        peers,
        // Generous: none of these tests exercise failure detection, and a
        // spurious suspect/dead transition mid-test must not be able to
        // affect a config-plane assertion.
        peer_timeout_secs: 7_200,
        ..Default::default()
    })
    .await
    .unwrap();
    for check in checks {
        coord.create_check_for_test(check).await;
    }
    let app = coord
        .clone()
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Node { coord, addr, sink }
}

fn check_cfg(name: &str) -> CheckConfig {
    CheckConfig {
        name: name.into(),
        kind: CheckKind::Http,
        target: "https://example.invalid".into(),
        cron: "0 * * * * *".into(),
        timeout_secs: 5,
        ..Default::default()
    }
}

/// Poll until `f` returns `Some`, or fail naming what never arrived.
async fn eventually<T, F, Fut>(what: &str, deadline: Duration, interval: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = tokio::time::Instant::now();
    loop {
        if let Some(v) = f().await {
            return v;
        }
        if start.elapsed() >= deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(interval).await;
    }
}

async fn login(addr: SocketAddr) -> String {
    let res = reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/login"))
        .json(&json!({ "password": ADMIN_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "login should succeed");
    res.headers()
        .get(reqwest::header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

async fn cluster_view(addr: SocketAddr, session: &str) -> Value {
    reqwest::Client::new()
        .get(format!("http://{addr}/api/v1/cluster"))
        .header(reqwest::header::COOKIE, session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn post_ping(addr: SocketAddr, body: &Value) -> Value {
    let res = reqwest::Client::new()
        .post(format!("http://{addr}/internal/cluster/ping"))
        .bearer_auth(PEER_TOKEN)
        .json(body)
        .send()
        .await
        .unwrap();
    assert!(
        res.status().is_success(),
        "ping to {addr} was rejected: {}",
        res.status()
    );
    res.json().await.unwrap()
}

/// One push-pull membership round, driven directly rather than waiting on
/// the heartbeat timer — see `tests/cluster.rs::gossip_step` for the full
/// explanation. Config replication only broadcasts to members a node
/// currently believes are alive, so this has to run before an edit for that
/// edit to reach anyone.
async fn gossip_step(from: SocketAddr, from_session: &str, to: SocketAddr) {
    let from_view = cluster_view(from, from_session).await;
    let ping = json!({
        "node_id": from_view["self"]["node_id"],
        "url": from_view["self"]["url"],
        "generation": from_view["self"]["generation"],
        "members": from_view["members"],
    });
    let reply = post_ping(to, &ping).await;
    post_ping(from, &reply).await;
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

fn check_document(id: &str, name: &str, target: &str) -> String {
    let mut body = check_body(name, target);
    body["id"] = json!(id);
    body.to_string()
}

fn config_entry(
    kind: &str,
    id: &str,
    millis: i64,
    node_id: &str,
    document: Option<String>,
) -> Value {
    json!({
        "kind": kind,
        "id": id,
        "hlc": { "millis": millis, "node_id": node_id },
        "document": document,
    })
}

/// `POST /internal/config`, authenticated. Asserts 204 so a rejected batch
/// fails loudly at the call site rather than as a confusing state mismatch
/// three lines later.
async fn apply_config(addr: SocketAddr, origin: &str, entries: Vec<Value>) {
    let res = reqwest::Client::new()
        .post(format!("http://{addr}/internal/config"))
        .bearer_auth(PEER_TOKEN)
        .json(&json!({ "origin": origin, "entries": entries }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        reqwest::StatusCode::NO_CONTENT,
        "a well-formed /internal/config batch should be accepted, got {}",
        res.status()
    );
}

async fn admin_checks(addr: SocketAddr, session: &str) -> Value {
    reqwest::Client::new()
        .get(format!("http://{addr}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn has_check(addr: SocketAddr, session: &str, name: &str) -> bool {
    admin_checks(addr, session)
        .await
        .get("checks")
        .and_then(|c| c.as_array())
        .unwrap()
        .iter()
        .any(|c| c["name"] == name)
}

async fn check_target(addr: SocketAddr, session: &str, name: &str) -> String {
    admin_checks(addr, session)
        .await
        .get("checks")
        .and_then(|c| c.as_array())
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("{name} should exist on {addr}"))["target"]
        .as_str()
        .unwrap()
        .to_string()
}

/// The check feed an agent actually reads. No `agent_id` query param, which
/// is the "declare nothing" fallback that returns every enabled check
/// unfiltered by placement — the simplest possible reader of the in-memory
/// check set, and the one a stale-cache bug is most likely to miss.
async fn agent_checks(addr: SocketAddr, token: &str) -> Value {
    reqwest::Client::new()
        .get(format!("http://{addr}/api/v1/agent/checks"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn post_result(addr: SocketAddr, token: &str, agent_id: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/results"))
        .bearer_auth(token)
        .json(&json!({ "results": [{
            "check_id": uuid::Uuid::now_v7(),
            "agent_id": agent_id,
            "zone": "z",
            "region": "r",
            "timestamp": "2026-07-11T00:00:00Z",
            "latency_ms": 12,
            "status_code": 200,
        }]}))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

// ---------------------------------------------------------------------------
// 9. Edit propagates
// ---------------------------------------------------------------------------

/// A check created through A's admin API appears on B — in the admin table
/// *and* in the in-memory set an agent actually reads. Asserting only the
/// table would pass even if the agent-facing cache never refreshed, which is
/// the most likely first bug in a second, separately-cached read path.
#[tokio::test]
async fn a_check_created_on_one_node_appears_on_its_peer_including_the_agent_feed() {
    let b = spawn_node("b", vec![], vec![]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], vec![]).await;
    let session_a = login(a.addr).await;
    let session_b = login(b.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    let agent_token = "wac_test.config-repl-secret";
    b.coord
        .provision_agent_credential(agent_token, "z", "r")
        .await
        .unwrap();

    let res = reqwest::Client::new()
        .post(format!("http://{}/api/v1/admin/checks", a.addr))
        .header(reqwest::header::COOKIE, &session_a)
        .json(&check_body(CHECK, "https://docs.test/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "creating the check on A should succeed");

    // The table view.
    let listed = eventually(
        "B's admin check list to include the new check",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            admin_checks(b.addr, &session_b)
                .await
                .get("checks")
                .and_then(|c| c.as_array())
                .unwrap()
                .iter()
                .find(|c| c["name"] == CHECK)
                .cloned()
        },
    )
    .await;
    assert_eq!(listed["target"], "https://docs.test/health");

    // The in-memory path an agent actually polls.
    let feed = eventually(
        "B's agent-visible check feed to include the new check",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            agent_checks(b.addr, agent_token)
                .await
                .get("checks")
                .and_then(|c| c.as_array())
                .unwrap()
                .iter()
                .find(|c| c["name"] == CHECK)
                .cloned()
        },
    )
    .await;
    assert_eq!(
        feed["target"], "https://docs.test/health",
        "the agent feed must reflect the replicated check, not a stale in-memory cache"
    );
}

// ---------------------------------------------------------------------------
// 10. Unplace is local, then delete does not resurrect (the tombstone test)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unplacing_a_check_is_local_and_does_not_create_a_replication_tombstone() {
    let b = spawn_node("b", vec![], vec![]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], vec![]).await;
    let session_a = login(a.addr).await;
    let session_b = login(b.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    let check_id = uuid::Uuid::now_v7().to_string();
    let entry = config_entry(
        "check",
        &check_id,
        1_000,
        "a",
        Some(check_document(&check_id, CHECK, "https://docs.test/health")),
    );
    apply_config(a.addr, "a", vec![entry.clone()]).await;
    apply_config(b.addr, "a", vec![entry.clone()]).await;

    let res = reqwest::Client::new()
        .delete(format!(
            "http://{}/api/v1/admin/checks/{check_id}/unplace",
            a.addr
        ))
        .header(reqwest::header::COOKIE, &session_a)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert!(
        !has_check(a.addr, &session_a, CHECK).await,
        "unplace removes the check from its local coordinator"
    );
    assert!(
        has_check(b.addr, &session_b, CHECK).await,
        "unplace must not delete the check in another region"
    );

    apply_config(a.addr, "b", vec![entry]).await;
    assert!(
        has_check(a.addr, &session_a, CHECK).await,
        "a retained peer config entry can place the same immutable id again"
    );
    assert!(has_check(b.addr, &session_b, CHECK).await);
}

// ---------------------------------------------------------------------------
// 11. Delete does not resurrect (the tombstone test)
// ---------------------------------------------------------------------------

/// Deleting a check is a tombstone with an HLC, not merely a row disappearing:
/// replaying an older create — a stale retransmit, or a reconciler that ran
/// before the delete propagated — must not bring it back on either node.
///
/// The spec pins a way to *push* config (`POST /internal/config`) and a way
/// to *detect* divergence (`GET /internal/digest`), but no endpoint to pull a
/// peer's config rows. So this test plays the reconciler itself: it applies
/// the tombstone it knows was written, directly, over the one write path that
/// is pinned — which is exactly what a real anti-entropy sweep would end up
/// doing after using the digest to notice the mismatch.
#[tokio::test]
async fn deleting_a_check_is_a_tombstone_a_stale_replay_cannot_undo() {
    let b = spawn_node("b", vec![], vec![]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], vec![]).await;
    let session_a = login(a.addr).await;
    let session_b = login(b.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    // Seed the check on both nodes through the config-replication path
    // itself, with an HLC this test controls, rather than through the admin
    // API — so the "stale replay" step later is unambiguously older.
    let create_hlc = 1_000_i64;
    let check_id = uuid::Uuid::now_v7().to_string();
    let doc = check_document(&check_id, CHECK, "https://docs.test/health");
    let create_entry = config_entry("check", &check_id, create_hlc, "a", Some(doc));
    apply_config(a.addr, "a", vec![create_entry.clone()]).await;
    apply_config(b.addr, "a", vec![create_entry.clone()]).await;
    assert!(
        has_check(a.addr, &session_a, CHECK).await,
        "setup: the check should exist on A before deleting it"
    );
    assert!(
        has_check(b.addr, &session_b, CHECK).await,
        "setup: the check should exist on B before deleting it"
    );

    // Delete on A through the real admin path — this is the write an
    // operator actually makes.
    let res = reqwest::Client::new()
        .delete(format!("http://{}/api/v1/admin/checks/{check_id}", a.addr))
        .header(reqwest::header::COOKIE, &session_a)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "deleting the check on A should succeed");
    assert!(
        !has_check(a.addr, &session_a, CHECK).await,
        "the check must be gone on A immediately after deleting it"
    );

    // Push a tombstone to B, newer than the create, standing in for the
    // anti-entropy sweep this test is driving by hand.
    let tombstone = config_entry("check", &check_id, create_hlc + 1, "a", None);
    apply_config(b.addr, "a", vec![tombstone]).await;
    assert!(
        !has_check(b.addr, &session_b, CHECK).await,
        "the tombstone should delete the check on B"
    );

    // The tombstone test itself: replaying the *original create* — an older
    // HLC than the tombstone — must not bring the check back on either node.
    apply_config(a.addr, "a", vec![create_entry.clone()]).await;
    apply_config(b.addr, "a", vec![create_entry]).await;
    assert!(
        !has_check(a.addr, &session_a, CHECK).await,
        "a stale, older create must not resurrect a tombstoned check on A"
    );
    assert!(
        !has_check(b.addr, &session_b, CHECK).await,
        "a stale, older create must not resurrect a tombstoned check on B"
    );
}

// ---------------------------------------------------------------------------
// 11. Concurrent edit converges
// ---------------------------------------------------------------------------

/// Two edits to the same check, applied to both nodes in an order that
/// contradicts their HLCs (the higher-HLC edit lands first everywhere, the
/// lower-HLC edit second), still converge on the higher-HLC document on both
/// nodes. If arrival order decided the winner instead of the HLC, this would
/// converge on the *lower*-HLC edit here, which is the point of driving the
/// skew explicitly rather than relying on real wall-clock timing.
#[tokio::test]
async fn concurrent_edits_from_both_nodes_converge_on_the_higher_hlc() {
    const NAME: &str = "concurrent-check";
    let b = spawn_node("b", vec![], vec![]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], vec![]).await;
    let session_a = login(a.addr).await;
    let session_b = login(b.addr).await;

    let check_id = uuid::Uuid::now_v7().to_string();
    let edit_a = config_entry(
        "check",
        &check_id,
        9_000,
        "a",
        Some(check_document(&check_id, NAME, "https://a.test/health")),
    );
    let edit_b = config_entry(
        "check",
        &check_id,
        5_000,
        "b",
        Some(check_document(&check_id, NAME, "https://b.test/health")),
    );

    // A's higher-HLC edit is applied everywhere FIRST; B's lower-HLC edit
    // arrives SECOND. A naive "last write wins by arrival" would end up on
    // B's edit; the HLC must keep A's.
    apply_config(a.addr, "a", vec![edit_a.clone()]).await;
    apply_config(b.addr, "a", vec![edit_a]).await;
    apply_config(b.addr, "b", vec![edit_b.clone()]).await;
    apply_config(a.addr, "b", vec![edit_b]).await;

    let target_a = check_target(a.addr, &session_a, NAME).await;
    let target_b = check_target(b.addr, &session_b, NAME).await;
    assert_eq!(
        target_a, "https://a.test/health",
        "A should keep its own higher-HLC edit despite it arriving first and being overwritten by a later, lower-HLC message"
    );
    assert_eq!(
        target_b, "https://a.test/health",
        "B should converge on A's higher-HLC edit rather than keeping its own more-recently-arrived one"
    );
    assert_eq!(
        target_a, target_b,
        "both nodes must agree on the same definition"
    );
}

// ---------------------------------------------------------------------------
// 12. Settings LWW
// ---------------------------------------------------------------------------

/// The same last-writer-wins rule applies to settings, not only to checks: a
/// notifier list edited on both nodes converges to one list rather than a
/// union or a per-node split brain.
#[tokio::test]
async fn notifier_edits_from_both_nodes_converge_to_one_list() {
    let b = spawn_node("b", vec![], vec![]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], vec![]).await;
    let session_a = login(a.addr).await;
    let session_b = login(b.addr).await;

    let notifiers_a =
        json!([{"name":"from-a","default":true,"type":"webhook","url":"http://a.invalid/"}]);
    let notifiers_b =
        json!([{"name":"from-b","default":true,"type":"webhook","url":"http://b.invalid/"}]);

    let edit_a = config_entry(
        "setting",
        "notifiers",
        1_000,
        "a",
        Some(notifiers_a.to_string()),
    );
    let edit_b = config_entry(
        "setting",
        "notifiers",
        4_000,
        "b",
        Some(notifiers_b.to_string()),
    );

    apply_config(a.addr, "a", vec![edit_a.clone()]).await;
    apply_config(b.addr, "a", vec![edit_a]).await;
    apply_config(b.addr, "b", vec![edit_b.clone()]).await;
    apply_config(a.addr, "b", vec![edit_b]).await;

    for (addr, session, who) in [(a.addr, &session_a, "A"), (b.addr, &session_b, "B")] {
        let v: Value = reqwest::Client::new()
            .get(format!("http://{addr}/api/v1/admin/notifiers"))
            .header(reqwest::header::COOKIE, session)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let names: Vec<&str> = v["notifiers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            vec!["from-b"],
            "{who} should converge on B's higher-HLC notifier list rather than merging or keeping its own: {v}"
        );
    }
}

// ---------------------------------------------------------------------------
// 13. Credential works fleet-wide
// ---------------------------------------------------------------------------

/// A credential minted on A works when presented to B, and stops working on
/// B the moment it is revoked on A — an operator mints once for the fleet,
/// not once per coordinator.
#[tokio::test]
async fn a_credential_minted_on_one_node_works_on_its_peer_until_revoked() {
    let b = spawn_node("b", vec![], vec![check_cfg(CHECK)]).await;
    let a = spawn_node(
        "a",
        vec![format!("http://{}", b.addr)],
        vec![check_cfg(CHECK)],
    )
    .await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    let res = reqwest::Client::new()
        .post(format!("http://{}/api/v1/admin/agent-credentials", a.addr))
        .header(reqwest::header::COOKIE, &session_a)
        .json(&json!({"zone":"eu-west-1a","region":"eu-west"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "minting on A should succeed");
    let minted: Value = res.json().await.unwrap();
    let token = minted["token"].as_str().unwrap().to_string();
    let id = token.split('.').next().unwrap().to_string();

    eventually(
        "B to accept a credential minted on A",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            let status = post_result(b.addr, &token, "fleet-agent").await;
            (status == 202).then_some(())
        },
    )
    .await;

    let res = reqwest::Client::new()
        .delete(format!(
            "http://{}/api/v1/admin/agent-credentials/{id}",
            a.addr
        ))
        .header(reqwest::header::COOKIE, &session_a)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "revoking on A should succeed");

    eventually(
        "B to stop accepting a credential revoked on A",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            let status = post_result(b.addr, &token, "fleet-agent").await;
            (status == 401).then_some(())
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// 14. Session portability
// ---------------------------------------------------------------------------

/// A session minted on A is valid when presented to B, because both share the
/// signing key derived from `peer_token` (see src/auth.rs). Without this, a
/// rolling restart or a load balancer in front of a clustered coordinator
/// would randomly 401 an otherwise-valid admin depending on which node
/// happened to answer. A tampered token is refused by both.
#[tokio::test]
async fn a_session_from_one_node_is_valid_on_its_peer() {
    let b = spawn_node("b", vec![], vec![]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], vec![]).await;
    let session = login(a.addr).await;

    let res = reqwest::Client::new()
        .get(format!("http://{}/api/v1/admin/checks", b.addr))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        200,
        "a session minted on A must be accepted by B: both are configured with the same peer_token, which is what makes the signing key identical"
    );

    // Tamper with the token: flip the last character of the cookie value.
    let (name, value) = session
        .split_once('=')
        .expect("cookie has a name=value form");
    let mut tampered_value = value.to_string();
    let last = tampered_value.pop().expect("cookie value is non-empty");
    tampered_value.push(if last == 'a' { 'b' } else { 'a' });
    let tampered = format!("{name}={tampered_value}");

    for (addr, who) in [(a.addr, "A"), (b.addr, "B")] {
        let res = reqwest::Client::new()
            .get(format!("http://{addr}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &tampered)
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            401,
            "{who} must refuse a tampered session token"
        );
    }
}

// ---------------------------------------------------------------------------
// 15. Config is not a result
// ---------------------------------------------------------------------------

/// A replicated config entry fires no notifier on either node. This is the
/// plane boundary: config is last-writer-wins and takes effect locally;
/// results are observer-owned, and only an observed transition may page. If
/// this ever fails, editing a check would page somebody.
#[tokio::test]
async fn replicated_config_does_not_fire_a_notifier() {
    let b = spawn_node("b", vec![], vec![]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], vec![]).await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    let entry = config_entry(
        "check",
        CHECK,
        1_000,
        "a",
        Some(check_body(CHECK, "https://docs.test/health").to_string()),
    );
    apply_config(a.addr, "a", vec![entry.clone()]).await;
    apply_config(b.addr, "a", vec![entry]).await;

    // Give any (wrongly wired) notification path a moment to fire before
    // checking both sinks are still empty.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        a.sink.lock().is_empty(),
        "A must not notify for a config change, only for an observed transition"
    );
    assert!(
        b.sink.lock().is_empty(),
        "B must not notify for a replicated config change either"
    );
}

// ---------------------------------------------------------------------------
// 16. Anti-entropy recovers a missed config push
// ---------------------------------------------------------------------------

/// The push in `replicate_config` only reaches members a node currently
/// believes are alive, so an edit saved while a peer is unreachable reaches
/// nobody — and, unlike a probe result, nothing will ever mention that row
/// again. Anti-entropy is the only thing that closes that hole.
///
/// The unreachable peer is simulated by editing *before* the membership
/// handshake, which is precisely the state a node that was down through the
/// edit ends up in: the push had no one to go to.
#[tokio::test]
async fn a_config_edit_missed_while_a_peer_was_unreachable_is_recovered_by_anti_entropy() {
    let a = spawn_node("a", vec![], vec![]).await;
    let b = spawn_node("b", vec![], vec![]).await;
    let session_a = login(a.addr).await;
    let session_b = login(b.addr).await;

    // Create on A while B is not a known member — the push goes nowhere.
    let res = reqwest::Client::new()
        .post(format!("http://{}/api/v1/admin/checks", a.addr))
        .header(reqwest::header::COOKIE, &session_a)
        .json(&check_body(CHECK, "https://docs.test/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "creating the check on A should succeed");
    assert!(
        !has_check(b.addr, &session_b, CHECK).await,
        "setup: B must not have the check yet — if it does, the push reached \
         it and this test is no longer exercising anti-entropy"
    );

    // B comes back: membership is established, but the create is long past.
    gossip_step(a.addr, &session_a, b.addr).await;
    b.coord.anti_entropy_sweep().await;

    assert!(
        has_check(b.addr, &session_b, CHECK).await,
        "anti-entropy should have pulled the check B missed while unreachable"
    );
    assert_eq!(
        check_target(b.addr, &session_b, CHECK).await,
        "https://docs.test/health",
        "the recovered check should carry the document A actually stored"
    );
}

// ---------------------------------------------------------------------------
// 17. Anti-entropy is still last-writer-wins
// ---------------------------------------------------------------------------

/// The negative half of the pair above: a sweep pulls whole config kinds, not
/// individual rows, so it routinely fetches entries the puller already has —
/// including ones it has since edited. Those must lose to the newer local
/// stamp exactly as a push would.
///
/// The pull is forced by giving A a second check B lacks, so the row counts
/// disagree and the kind is fetched. Without that, `reconcile_config` would
/// correctly skip the kind entirely and the assertion below would pass
/// without the LWW rule ever being consulted.
#[tokio::test]
async fn an_anti_entropy_pull_does_not_overwrite_a_newer_local_edit() {
    const OTHER: &str = "only-on-a";
    let a = spawn_node("a", vec![], vec![]).await;
    let b = spawn_node("b", vec![], vec![]).await;
    let session_a = login(a.addr).await;
    let session_b = login(b.addr).await;

    // A holds an old edit of CHECK, plus a second check B has never seen.
    let check_id = uuid::Uuid::now_v7().to_string();
    let other_id = uuid::Uuid::now_v7().to_string();
    let stale = config_entry(
        "check",
        &check_id,
        1_000,
        "a",
        Some(check_document(
            &check_id,
            CHECK,
            "https://stale.test/health",
        )),
    );
    let other = config_entry(
        "check",
        &other_id,
        1_000,
        "a",
        Some(check_document(
            &other_id,
            OTHER,
            "https://other.test/health",
        )),
    );
    apply_config(a.addr, "a", vec![stale, other]).await;

    // B holds a strictly newer edit of the same check.
    let fresh = config_entry(
        "check",
        &check_id,
        9_000,
        "b",
        Some(check_document(
            &check_id,
            CHECK,
            "https://fresh.test/health",
        )),
    );
    apply_config(b.addr, "b", vec![fresh]).await;

    gossip_step(a.addr, &session_a, b.addr).await;
    b.coord.anti_entropy_sweep().await;

    assert_eq!(
        check_target(b.addr, &session_b, OTHER).await,
        "https://other.test/health",
        "the sweep should still have pulled the row B was genuinely missing \
         — otherwise this test proves nothing about the row it kept"
    );
    assert_eq!(
        check_target(b.addr, &session_b, CHECK).await,
        "https://fresh.test/health",
        "a pulled entry with an older HLC must lose to the newer local edit"
    );
}

/// Replication carries result history and gets 32 MiB; config transfers carry
/// documents and get 8 MiB. Both must override the coordinator-wide 1 MiB
/// default while still rejecting the byte immediately above their own ceiling.
#[tokio::test]
async fn peer_replication_and_config_transfer_enforce_their_route_specific_limits() {
    let node = spawn_node("limits", vec![], vec![]).await;

    for (path, limit) in [
        ("/internal/replicate", 32 * 1024 * 1024),
        ("/internal/config", 8 * 1024 * 1024),
    ] {
        let client = reqwest::Client::new();
        let within_route_limit = client
            .post(format!("http://{}{path}", node.addr))
            .bearer_auth(PEER_TOKEN)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(vec![b'x'; 1024 * 1024 + 1])
            .send()
            .await
            .unwrap();
        assert_eq!(
            within_route_limit.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{path} must pass bodies above the global limit to its handler"
        );

        let oversized = client
            .post(format!("http://{}{path}", node.addr))
            .bearer_auth(PEER_TOKEN)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(vec![b'x'; limit + 1])
            .send()
            .await
            .unwrap();
        assert_eq!(
            oversized.status(),
            reqwest::StatusCode::PAYLOAD_TOO_LARGE,
            "{path} must enforce its dedicated {limit}-byte limit"
        );
    }
}
