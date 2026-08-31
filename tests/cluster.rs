//! Coordinator clustering: membership, failure detection and replication.
//!
//! Real `Coordinator`s over real localhost sockets, exactly like
//! `tests/gossip.rs`. The only stand-ins are the webhook sinks playing the
//! part of real alert endpoints — they are what makes "who paged?" directly
//! observable — and, where the spec gives no pull endpoint for it, the test
//! itself playing the role of the anti-entropy reconciler over the endpoints
//! that *are* pinned.
//!
//! The properties under test are the ones that make a cluster safe to run
//! rather than merely present:
//!
//!   1. membership actually converges, transitively, from a chain of single
//!      peers — and a node nobody ever contacts stays alone;
//!   2. failure detection follows the documented boundaries, and a returning
//!      process is recognised by generation rather than added as a stranger;
//!   3. results replicate, exactly once, with no duplication on replay;
//!   4. **the observing node owns evaluation and paging — a peer that only
//!      hears about a transition must never notify or re-derive it.** This is
//!      the same invariant `tests/gossip.rs` defends for the transition-only
//!      predecessor design; here it has to hold for the richer replication
//!      batch too.
//!   5. a late joiner can catch up by digest and backfill, not only by
//!      whatever happened to be pushed while it was down;
//!   6. a replicated incident keeps the id it was opened with, closing the
//!      `ponytail:` in `gossip_handler` (src/coordinator/mod.rs) that admits
//!      the old transition-only path never received the peer's real id;
//!   7. a deleted incident stays deleted: the tombstone survives a peer
//!      pushing the row straight back, which is what anti-entropy and an
//!      un-converged peer both look like on the wire;
//!   8. an agent polling one node is in the fleet every node publishes —
//!      **without** entering any other node's placement map, so visibility
//!      replicates and authority does not;
//!   9. every `/internal/*` endpoint refuses both an anonymous caller and a
//!      valid *agent* credential — clustering is peer infrastructure, and an
//!      agent must never be able to rewrite published state or edit config.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::{json, Value};

use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::models::{CheckKind, CheckResult};
use warden::notify::{Notifier, NotifierKind};

/// Peers authenticate with their own secret, never an agent's — see
/// `require_peer` in src/coordinator/mod.rs.
const PEER_TOKEN: &str = "cluster-peer-secret";
const ADMIN_PASSWORD: &str = "correct horse battery staple";
const CHECK: &str = "web";

type Sink = Arc<Mutex<Vec<Value>>>;

/// An alert endpoint that records what it was sent. Copied from
/// `tests/gossip.rs`.
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

/// A live coordinator plus what a test needs to talk to it: its address, its
/// alert sink, and the `Coordinator` handle itself for the same
/// direct-ingest shortcut `tests/gossip.rs` uses (posting results is an
/// agent's job, not what clustering is testing).
struct Node {
    coord: Coordinator,
    addr: SocketAddr,
    sink: Sink,
}

/// Build and serve one coordinator. `advertise_url` is derived from a
/// listener bound before the coordinator is constructed, because a peer has
/// to be told a real, dialable address — there is no way to know the
/// ephemeral port in advance the way a fixed deployment would.
async fn spawn_node(node_id: &str, peers: Vec<String>, peer_timeout_secs: u64) -> Node {
    let (sink, addr_sink) = alert_sink().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let coord = Coordinator::new(
        &CoordinatorConfig {
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
            peer_timeout_secs,
            ..Default::default()
        },
        &[CheckConfig {
            name: CHECK.into(),
            kind: CheckKind::Http,
            target: "https://example.invalid".into(),
            cron: "0 * * * * *".into(),
            timeout_secs: 5,
            ..Default::default()
        }],
    )
    .unwrap();
    let app = coord
        .clone()
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Node { coord, addr, sink }
}

/// Two hours: long enough that no test which does not deliberately shorten it
/// ever sees a spurious suspect/dead transition just from wall-clock time
/// passing while other assertions run.
const NO_TIMEOUT: u64 = 7_200;

/// A probe observation. `status_code` drives the default eval: 200 -> UP,
/// 500 -> DOWN. Copied from `tests/gossip.rs`.
fn result(status_code: u16, ts_millis: i64) -> CheckResult {
    CheckResult {
        check_id: CHECK.into(),
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

/// Poll until `f` returns `Some`, or fail naming what never arrived. Used
/// only where a genuine wait is unavoidable: replication is fire-and-forget,
/// and failure detection is a real wall-clock boundary. Never a substitute
/// for a direct call.
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

async fn get_json(url: &str) -> Value {
    reqwest::get(url).await.unwrap().json().await.unwrap()
}

/// Log in and return the `Cookie:` header value, exactly as
/// `tests/admin_api.rs` does.
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

/// `GET /api/v1/cluster`: this node's own membership view.
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

/// `POST /internal/cluster/ping`, authenticated, returning the reply body.
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

/// One push-pull gossip round, driven directly by the test instead of
/// waiting on `gossip_interval_secs`: `from`'s current view is sent to `to`
/// (which merges it and replies with its own view), and that reply is then
/// fed back into `from` — exactly what `Cluster::ping_all` does over the
/// wire in one round trip, just called on the test's own schedule.
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

/// `POST /internal/replicate`, authenticated. Returns the status so callers
/// can assert both "accepted" and "no duplicate landed".
async fn replicate(addr: SocketAddr, body: &Value) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("http://{addr}/internal/replicate"))
        .bearer_auth(PEER_TOKEN)
        .json(body)
        .send()
        .await
        .unwrap()
        .status()
}

fn replicate_batch(origin: &str, results: Vec<Value>) -> Value {
    json!({ "origin": origin, "results": results })
}

fn replicated_result(r: CheckResult, status: &str) -> Value {
    json!({ "result": serde_json::to_value(r).unwrap(), "status": status })
}

async fn history(addr: SocketAddr, check_id: &str) -> Vec<Value> {
    get_json(&format!("http://{addr}/api/v1/checks/{check_id}/history")).await["history"]
        .as_array()
        .unwrap()
        .clone()
}

async fn digest(addr: SocketAddr) -> Value {
    reqwest::Client::new()
        .get(format!("http://{addr}/internal/digest"))
        .bearer_auth(PEER_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn backfill(addr: SocketAddr, check_id: &str, since: i64, until: i64, limit: u32) -> Value {
    reqwest::Client::new()
        .get(format!(
            "http://{addr}/internal/backfill?check_id={check_id}&since={since}&until={until}&limit={limit}"
        ))
        .bearer_auth(PEER_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// All member ids this node currently knows about, including itself. A set
/// rather than a count, so a wrong-but-same-size membership does not read as
/// convergence.
fn known_ids(view: &Value) -> std::collections::BTreeSet<String> {
    std::iter::once(view["self"]["node_id"].as_str().unwrap().to_string())
        .chain(
            view["members"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["node_id"].as_str().unwrap().to_string()),
        )
        .collect()
}

// ---------------------------------------------------------------------------
// 1. Discovery
// ---------------------------------------------------------------------------

/// Each node is seeded with exactly one peer, forming a chain (A -> B, B ->
/// C) rather than a full mesh. Push-pull discovery means one direction of
/// contact is enough to learn everyone transitively — A never configures C
/// at all — so this is the case that actually exercises transitivity, not a
/// relaxation of a full-mesh test.
#[tokio::test]
async fn discovery_converges_through_a_chain_of_single_peers() {
    let c = spawn_node("c", vec![], NO_TIMEOUT).await;
    let b = spawn_node("b", vec![format!("http://{}", c.addr)], NO_TIMEOUT).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], NO_TIMEOUT).await;

    let session_a = login(a.addr).await;
    let session_b = login(b.addr).await;
    let session_c = login(c.addr).await;

    // Two rounds is enough for a 3-node chain: round one gives B knowledge of
    // C (and A knowledge of B); round two carries C onward to A via B's now
    //-richer view. A third is added purely as slack against ordering.
    for _ in 0..3 {
        gossip_step(a.addr, &session_a, b.addr).await;
        gossip_step(b.addr, &session_b, c.addr).await;
    }

    for (who, addr, session) in [
        ("A", a.addr, &session_a),
        ("B", b.addr, &session_b),
        ("C", c.addr, &session_c),
    ] {
        let view = cluster_view(addr, session).await;
        let ids = known_ids(&view);
        let expect: std::collections::BTreeSet<String> =
            ["a", "b", "c"].into_iter().map(String::from).collect();
        assert_eq!(
            ids, expect,
            "{who} should have converged on all three members through the chain, saw {ids:?}: {view}"
        );
    }
}

/// A node that nobody ever contacts, and which has no seeds of its own, must
/// not spontaneously discover anyone. Gossip only travels along edges someone
/// actually dials.
#[tokio::test]
async fn a_node_with_no_seeds_stays_alone() {
    let solo = spawn_node("solo", vec![], NO_TIMEOUT).await;
    let session = login(solo.addr).await;
    let view = cluster_view(solo.addr, &session).await;
    assert!(
        view["members"].as_array().unwrap().is_empty(),
        "an unseeded, never-contacted node must have no members: {view}"
    );
    assert_eq!(view["self"]["node_id"], "solo");
}

// ---------------------------------------------------------------------------
// 2. Failure detection
// ---------------------------------------------------------------------------

/// Liveness follows the documented boundaries — `Alive` -> `Suspect` past one
/// `peer_timeout_secs` of silence, `Suspect` -> `Dead` past two — and a
/// process that comes back with a higher generation is recognised as the same
/// member returning, not admitted as a new one. A stale relay at the *same*
/// generation must not undo a dead verdict: only a fresher observation, or a
/// genuinely new process, earns that.
#[tokio::test]
async fn suspect_then_dead_then_recovers_by_generation_not_by_stale_gossip() {
    const TIMEOUT_SECS: u64 = 1;
    let b = spawn_node("b", vec![], TIMEOUT_SECS).await;
    let a = spawn_node("a", vec![], TIMEOUT_SECS).await;
    let session_a = login(a.addr).await;

    // Establish B as a member of A once, then go silent: nothing pings A
    // about B again until the test says so.
    gossip_step(a.addr, &session_a, b.addr).await;
    let member = |view: &Value| -> Option<Value> {
        view["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["node_id"] == "b")
            .cloned()
    };

    let view = cluster_view(a.addr, &session_a).await;
    assert_eq!(
        member(&view).expect("B should be a member of A")["state"],
        "alive",
        "B must start alive: {view}"
    );

    let suspect = eventually(
        "B to go suspect",
        Duration::from_secs(8),
        Duration::from_millis(50),
        || async {
            member(&cluster_view(a.addr, &session_a).await).filter(|m| m["state"] == "suspect")
        },
    )
    .await;
    assert_eq!(suspect["state"], "suspect");

    let dead = eventually(
        "B to go dead",
        Duration::from_secs(8),
        Duration::from_millis(50),
        || async {
            member(&cluster_view(a.addr, &session_a).await).filter(|m| m["state"] == "dead")
        },
    )
    .await;
    assert_eq!(dead["state"], "dead");
    let generation_when_dead = dead["generation"].as_i64().unwrap();
    let last_seen_when_dead = dead["last_seen"].as_i64().unwrap();

    // Negative half first, while B is still dead: gossip about B relayed by a
    // third party, at the SAME generation but an OLDER last_seen than what A
    // already has, must be dropped rather than resurrecting it. A real
    // delayed retransmission looks exactly like this.
    let stale_relay = json!({
        "node_id": "relay", "url": "http://relay.invalid",
        "generation": 1,
        "members": [{
            "node_id": "b",
            "url": format!("http://{}", b.addr),
            "generation": generation_when_dead,
            "last_seen": last_seen_when_dead - 10_000,
            "state": "alive",
        }],
    });
    post_ping(a.addr, &stale_relay).await;
    let still_dead = cluster_view(a.addr, &session_a).await;
    assert_eq!(
        member(&still_dead).unwrap()["state"],
        "dead",
        "a stale same-generation relay must not resurrect a dead member: {still_dead}"
    );

    // Positive half: B "restarts" — same node_id and url, a strictly higher
    // generation — and pings A directly. This must update B's existing entry
    // in place, not add a second entry for what is actually one physical node
    // returning.
    let members_before = still_dead;
    let members_before_count = members_before["members"].as_array().unwrap().len();
    let resurrection = json!({
        "node_id": "b", "url": format!("http://{}", b.addr),
        "generation": generation_when_dead + 1_000,
        "members": [],
    });
    post_ping(a.addr, &resurrection).await;

    let alive_again = cluster_view(a.addr, &session_a).await;
    let members_after_count = alive_again["members"].as_array().unwrap().len();
    assert_eq!(
        members_after_count, members_before_count,
        "a higher-generation return must update B's existing entry, not add a second member: {alive_again}"
    );
    assert_eq!(
        member(&alive_again).unwrap()["state"],
        "alive",
        "a higher generation must read as alive again: {alive_again}"
    );
}

// ---------------------------------------------------------------------------
// 3. Replication
// ---------------------------------------------------------------------------

/// A result ingested on A is visible on B's public history feed. A must have
/// B as a believed-live member before ingest, or the push never fires.
#[tokio::test]
async fn results_ingested_on_one_node_are_visible_on_its_peer() {
    let b = spawn_node("b", vec![], NO_TIMEOUT).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], NO_TIMEOUT).await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    a.coord.ingest(vec![result(200, 1_000)]).await;

    let rows = eventually(
        "B to see A's result",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            let rows = history(b.addr, CHECK).await;
            (!rows.is_empty()).then_some(rows)
        },
    )
    .await;
    assert_eq!(
        rows.len(),
        1,
        "B should have exactly the one result A observed: {rows:?}"
    );
    assert_eq!(rows[0]["status"], "up");
}

// ---------------------------------------------------------------------------
// 4. Idempotency
// ---------------------------------------------------------------------------

/// Replaying an identical `/internal/replicate` batch must not duplicate
/// history; a batch with one genuinely new observation must add exactly one
/// row. Driven directly at B rather than through A's push, so this is a
/// clean test of the receiving side alone.
#[tokio::test]
async fn replaying_a_replicate_batch_does_not_duplicate_history() {
    let b = spawn_node("b", vec![], NO_TIMEOUT).await;
    let batch_at = |ts: i64| replicate_batch("a", vec![replicated_result(result(200, ts), "up")]);

    assert_eq!(
        replicate(b.addr, &batch_at(1_000)).await,
        reqwest::StatusCode::NO_CONTENT
    );
    assert_eq!(history(b.addr, CHECK).await.len(), 1);

    // Exact replay of the same batch: nothing new should land.
    assert_eq!(
        replicate(b.addr, &batch_at(1_000)).await,
        reqwest::StatusCode::NO_CONTENT
    );
    assert_eq!(
        history(b.addr, CHECK).await.len(),
        1,
        "replaying an identical batch must not duplicate the row"
    );

    // A batch with one genuinely new observation adds exactly one row.
    assert_eq!(
        replicate(b.addr, &batch_at(2_000)).await,
        reqwest::StatusCode::NO_CONTENT
    );
    assert_eq!(
        history(b.addr, CHECK).await.len(),
        2,
        "a batch with one new observation should add exactly one row"
    );
}

// ---------------------------------------------------------------------------
// 5. No double paging — THE INVARIANT TEST
// ---------------------------------------------------------------------------

/// **The invariant that makes clustering safe rather than merely present.**
/// A observes the transition and pages once. B learns the same transition by
/// replication and must persist it — its published state and alert feed must
/// converge — without ever notifying: the observing node owns evaluation and
/// alerting, and a peer that re-evaluated or re-notified would mean a
/// three-node cluster pages somebody three times for one outage.
///
/// This is `tests/gossip.rs::a_transition_reaches_the_peer_without_paging_twice`
/// re-asserted against the richer `/internal/replicate` batch that replaces
/// the transition-only `/internal/gossip` path.
#[tokio::test]
async fn only_the_observing_node_pages_for_a_replicated_transition() {
    let b = spawn_node("b", vec![], NO_TIMEOUT).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], NO_TIMEOUT).await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    // The first observation only establishes a baseline; the second flips it.
    a.coord.ingest(vec![result(200, 1_000)]).await;
    a.coord.ingest(vec![result(500, 2_000)]).await;

    // Asserted first: if A never paged, the fixture produced no transition
    // and every assertion about B below would be vacuous rather than wrong.
    let paged_a = eventually(
        "A to page",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            let n = a.sink.lock().len();
            (n > 0).then_some(n)
        },
    )
    .await;
    assert_eq!(paged_a, 1, "the observing node pages exactly once");

    // B's published state must actually converge — a peer that silently
    // dropped the replicated batch would also leave B's sink empty, which
    // would make the "no page" assertion below true for the wrong reason.
    let status = eventually(
        "B to converge on down",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            let v = get_json(&format!("http://{}/api/v1/status", b.addr)).await;
            let c = v["checks"]
                .as_array()?
                .iter()
                .find(|c| c["check_id"] == CHECK)?
                .clone();
            (c["status"] == "down").then_some(c)
        },
    )
    .await;
    assert_eq!(status["status"], "down");

    assert_eq!(
        a.sink.lock().len(),
        1,
        "A must still have paged exactly once"
    );
    assert!(
        b.sink.lock().is_empty(),
        "the peer must never page for a transition it only learned about secondhand"
    );
}

// ---------------------------------------------------------------------------
// 6. Anti-entropy
// ---------------------------------------------------------------------------

/// A joins-late scenario: B becomes a member only after A already holds
/// history B never saw pushed. B must be able to catch up by pulling A's
/// digest and backfill rather than only by whatever push happened to be live
/// at ingest time — this is the correctness backstop for a dropped push.
#[tokio::test]
async fn a_late_joiner_backfills_history_via_digest_and_backfill() {
    let a = spawn_node("a", vec![], NO_TIMEOUT).await;
    // A accumulates history entirely on its own, before B exists as a peer at
    // all — so nothing here could have arrived by a live push.
    a.coord
        .ingest(vec![
            result(200, 1_000),
            result(500, 2_000),
            result(200, 3_000),
        ])
        .await;

    let b = spawn_node("b", vec![], NO_TIMEOUT).await;
    let session_a = login(a.addr).await;
    // Membership only, no push: B joins after the fact.
    gossip_step(a.addr, &session_a, b.addr).await;

    let dig = digest(a.addr).await;
    let entry = dig["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["check_id"] == CHECK)
        .cloned()
        .expect("A's digest should list the check it has history for");
    assert_eq!(
        entry["count"], 3,
        "A's digest should report all three observations: {dig}"
    );

    let until = entry["max_ts"].as_i64().unwrap() + 1;
    let batch = backfill(a.addr, CHECK, 0, until, 100).await;
    assert_eq!(
        batch["results"].as_array().unwrap().len(),
        3,
        "backfill since the beginning of time should hand back everything: {batch}"
    );

    assert_eq!(
        replicate(b.addr, &batch).await,
        reqwest::StatusCode::NO_CONTENT
    );

    let b_history = history(b.addr, CHECK).await;
    assert_eq!(
        b_history.len(),
        3,
        "B should match A's history after applying the backfilled batch: {b_history:?}"
    );

    // Negative half: once the two agree, a fresh digest comparison finds
    // nothing left to request.
    let dig_b = digest(b.addr).await;
    let entry_b = dig_b["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["check_id"] == CHECK)
        .cloned()
        .expect("B's digest should list the check now that it has caught up");
    assert_eq!(
        entry_b["count"], entry["count"],
        "once B has caught up its digest must agree with A's, so anti-entropy has nothing left to pull: {dig_b}"
    );
    assert_eq!(entry_b["max_ts"], entry["max_ts"]);
}

// ---------------------------------------------------------------------------
// 7. Incident replication
// ---------------------------------------------------------------------------

/// A replicated incident keeps the id it was opened with on the node that
/// observed the outage.
///
/// Closes an explicit gap: `gossip_handler` in src/coordinator/mod.rs carries
/// a `ponytail:` admitting the old transition-only path never receives the
/// peer's incident row, so it stamps a replicated alert with whichever
/// incident this node happens to have open locally — or none. The
/// `/internal/replicate` `incidents` array exists precisely so a peer is told
/// the real id instead of guessing, so this test asserts on the id itself,
/// not merely that *an* incident exists on B.
#[tokio::test]
async fn a_replicated_incident_keeps_its_originating_id() {
    let b = spawn_node("b", vec![], NO_TIMEOUT).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], NO_TIMEOUT).await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    a.coord.ingest(vec![result(200, 1_000)]).await;
    a.coord.ingest(vec![result(500, 2_000)]).await; // confirms DOWN, opens an incident

    let incident_id = a
        .coord
        .incidents_for_test(1)
        .first()
        .expect("A should have opened an incident for the confirmed outage")
        .id
        .clone();

    let incident_json = eventually(
        "B to receive A's incident",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            let res = reqwest::Client::new()
                .get(format!("http://{}/api/v1/incidents/{incident_id}", b.addr))
                .send()
                .await
                .unwrap();
            if res.status() != reqwest::StatusCode::OK {
                return None;
            }
            Some(res.json::<Value>().await.unwrap())
        },
    )
    .await;
    assert_eq!(
        incident_json["incident"]["id"], incident_id,
        "B's copy of the incident must carry A's id verbatim, not one B minted itself: {incident_json}"
    );

    let alerts = get_json(&format!("http://{}/api/v1/alerts", b.addr)).await;
    let replicated_alert = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|al| al["node_id"] == CHECK)
        .expect("B should have recorded the replicated alert");
    assert_eq!(
        replicated_alert["incident_id"], incident_id,
        "B's alert row must be stamped with A's incident id, not a locally minted one: {replicated_alert}"
    );
}

/// Deleting an incident on the node that opened it removes it cluster-wide,
/// and a peer that still has the row cannot hand it back.
///
/// The second half is the whole reason `incident_tombstones` exists. Deletion
/// is a *local* row disappearing; anti-entropy and the ordinary replication
/// push both work by resending what a node has, so without a tombstone the
/// next batch from any peer — or the operator's own node re-pushing before it
/// converged — silently resurrects the incident an operator just removed. The
/// test therefore pushes the incident straight back at B over
/// `/internal/replicate` after the delete and asserts it stays gone.
#[tokio::test]
async fn a_deleted_incident_stays_deleted_when_a_peer_pushes_it_back() {
    let b = spawn_node("b", vec![], NO_TIMEOUT).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], NO_TIMEOUT).await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    a.coord.ingest(vec![result(200, 1_000)]).await;
    a.coord.ingest(vec![result(500, 2_000)]).await; // confirms DOWN, opens an incident

    let incident = a
        .coord
        .incidents_for_test(1)
        .first()
        .expect("A should have opened an incident for the confirmed outage")
        .clone();
    let incident_id = incident.id.clone();

    let incident_url = |addr: SocketAddr| format!("http://{addr}/api/v1/incidents/{incident_id}");
    let incident_status = |addr: SocketAddr| {
        let url = incident_url(addr);
        async move { reqwest::get(&url).await.unwrap().status() }
    };

    // B must actually have it first, or "gone" afterwards proves nothing.
    eventually(
        "B to receive A's incident",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async { (incident_status(b.addr).await == reqwest::StatusCode::OK).then_some(()) },
    )
    .await;

    let deleted = reqwest::Client::new()
        .delete(format!(
            "http://{}/api/v1/admin/incidents/{incident_id}",
            a.addr
        ))
        .header(reqwest::header::COOKIE, &session_a)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 200, "the delete on A should succeed");
    assert_eq!(
        incident_status(a.addr).await,
        reqwest::StatusCode::NOT_FOUND,
        "the incident must be gone on the node it was deleted from"
    );

    eventually(
        "B to drop the deleted incident",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            (incident_status(b.addr).await == reqwest::StatusCode::NOT_FOUND).then_some(())
        },
    )
    .await;

    // The resurrection attempt: exactly the batch a peer that had not yet
    // heard about the delete — or an anti-entropy sweep — would send.
    let push_back = json!({
        "origin": "a",
        "incidents": [{
            "id": incident_id,
            "title": incident.title,
            "state": incident.state,
            "started_at": incident.started_at,
            "resolved_at": incident.resolved_at,
            "global": incident.global,
            "origin_check": incident.origin_check,
            "checks": [],
            "updates": [],
        }],
    });
    assert_eq!(
        replicate(b.addr, &push_back).await,
        reqwest::StatusCode::NO_CONTENT,
        "the batch itself is well-formed and must be accepted"
    );
    assert_eq!(
        incident_status(b.addr).await,
        reqwest::StatusCode::NOT_FOUND,
        "a tombstoned incident must not be resurrected by a peer still holding the row"
    );

    // And the alert survives: a transition is a fact that happened, the
    // operator-facing narrative around it is not.
    let alerts = get_json(&format!("http://{}/api/v1/alerts", a.addr)).await;
    let alert = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|al| al["node_id"] == CHECK)
        .expect("deleting the incident must not delete the transition it described");
    assert!(
        alert["incident_id"].is_null(),
        "the surviving alert must be unlinked, not left pointing at a deleted incident: {alert}"
    );
}

// ---------------------------------------------------------------------------
// 8. Fleet liveness
// ---------------------------------------------------------------------------

/// An agent polling A is part of the fleet every node reports, not just A's.
///
/// `seen` is per-node by construction — an agent only ever polls the
/// coordinator it was pointed at — so before poll rows replicated, a
/// three-node cluster showed two of its three regions down on every node.
/// This asserts the fix from the other side: B is never contacted by the
/// agent at all, and must still publish its region as up.
///
/// Placement is deliberately *not* part of this: B learning that the agent
/// exists must not make B start handing it checks.
#[tokio::test]
async fn an_agent_polling_one_node_shows_up_in_its_peers_fleet() {
    const AGENT: &str = "probe-1";
    const REGION: &str = "ap-south-1";
    let b = spawn_node("b", vec![], NO_TIMEOUT).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], NO_TIMEOUT).await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    let agent_token = "wac_fleet.poll-secret";
    a.coord
        .provision_agent_credential(agent_token, "ap-south-1a", REGION)
        .unwrap();

    // The poll itself: asking for work is the contact that puts an agent in
    // `seen`, which is what the sync loop then writes down and broadcasts.
    let res = reqwest::Client::new()
        .get(format!(
            "http://{}/api/v1/agent/checks?agent_id={AGENT}&poll_secs=30",
            a.addr
        ))
        .bearer_auth(agent_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "the agent's poll should be accepted");

    // `tokio::time::interval` fires its first tick immediately, so this does
    // not wait out a gossip period before the first snapshot goes out.
    a.coord.spawn_fleet_sync();

    let region_row = eventually(
        "B to publish A's agent in its fleet",
        Duration::from_secs(5),
        Duration::from_millis(20),
        || async {
            let status = get_json(&format!("http://{}/api/v1/status", b.addr)).await;
            status["agents"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .find(|r| r["name"] == REGION)
                .cloned()
        },
    )
    .await;
    assert_eq!(
        region_row["healthy"], 1,
        "B must count the agent polling A as up, judged by the window that \
         travelled with the row: {region_row}"
    );
    assert_eq!(region_row["total"], 1);

    // Visibility is not placement. A replicated poll row is a display fact:
    // it must not enter B's `seen`, which is the map `agent_checks_handler`
    // decides "one probe per zone" from. If it did, both nodes would believe
    // they own the same agent and hand it the same checks twice.
    assert!(
        b.coord.agent_window_ms_for_test(AGENT).is_none(),
        "B published the agent without ever being polled by it, so the row \
         must be display-only and stay out of B's placement map"
    );
    assert!(
        a.coord.agent_window_ms_for_test(AGENT).is_some(),
        "A is the node actually being polled and must still own placement"
    );
}

// ---------------------------------------------------------------------------
// 9. Auth
// ---------------------------------------------------------------------------

/// Every `/internal/*` endpoint refuses an anonymous caller and a valid
/// *agent* credential, and accepts the peer token. The agent half matters as
/// much as the anonymous half: these endpoints rewrite published state and
/// edit configuration, and an agent is not infrastructure — it is the party
/// clustering exists to keep honest, not a peer.
#[tokio::test]
async fn internal_endpoints_refuse_anonymous_and_agent_callers() {
    let node = spawn_node("a", vec![], NO_TIMEOUT).await;
    let agent_token = "wac_test.cluster-auth-secret";
    node.coord
        .provision_agent_credential(agent_token, "z", "r")
        .unwrap();

    // Ping is checked on its own: the contract pins its request/response
    // shape but not a specific success code the way it pins 204 for
    // replicate/config and (implicitly, by pairing with digest) 200 for the
    // GET endpoints.
    let ping_body =
        json!({"node_id":"ghost","url":"http://ghost.invalid","generation":1,"members":[]});
    let anon = reqwest::Client::new()
        .post(format!("http://{}/internal/cluster/ping", node.addr))
        .json(&ping_body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        anon.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "ping must refuse an anonymous caller"
    );
    let agent = reqwest::Client::new()
        .post(format!("http://{}/internal/cluster/ping", node.addr))
        .bearer_auth(agent_token)
        .json(&ping_body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        agent.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "ping must refuse a valid agent credential: an agent is not a cluster peer"
    );
    let peer = reqwest::Client::new()
        .post(format!("http://{}/internal/cluster/ping", node.addr))
        .bearer_auth(PEER_TOKEN)
        .json(&ping_body)
        .send()
        .await
        .unwrap();
    assert!(
        peer.status().is_success(),
        "ping should accept the correct peer token, got {}",
        peer.status()
    );

    let cases: Vec<(&str, String, Option<Value>, reqwest::StatusCode)> = vec![
        (
            "POST",
            "/internal/replicate".to_string(),
            Some(json!({"origin":"ghost","results":[]})),
            reqwest::StatusCode::NO_CONTENT,
        ),
        (
            "POST",
            "/internal/config".to_string(),
            Some(json!({"origin":"ghost","entries":[]})),
            reqwest::StatusCode::NO_CONTENT,
        ),
        (
            "GET",
            "/internal/digest".to_string(),
            None,
            reqwest::StatusCode::OK,
        ),
        (
            "GET",
            format!("/internal/backfill?check_id={CHECK}&since=0&until=9999999999999&limit=10"),
            None,
            reqwest::StatusCode::OK,
        ),
    ];

    for (method, path, body, want) in cases {
        let build = |token: Option<&str>| {
            let mut req = match method {
                "GET" => reqwest::Client::new().get(format!("http://{}{path}", node.addr)),
                "POST" => reqwest::Client::new().post(format!("http://{}{path}", node.addr)),
                other => panic!("unsupported method {other}"),
            };
            if let Some(t) = token {
                req = req.bearer_auth(t);
            }
            if let Some(b) = &body {
                req = req.json(b);
            }
            req
        };

        let res = build(None).send().await.unwrap();
        assert_eq!(
            res.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{method} {path} must refuse an anonymous caller, got {}",
            res.status()
        );

        let res = build(Some(agent_token)).send().await.unwrap();
        assert_eq!(
            res.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{method} {path} must refuse a valid agent credential: an agent must not be able to rewrite published state or edit config, got {}",
            res.status()
        );

        let res = build(Some(PEER_TOKEN)).send().await.unwrap();
        assert_eq!(
            res.status(),
            want,
            "{method} {path} should accept the correct peer token, got {}",
            res.status()
        );
    }
}

// ---------------------------------------------------------------------------
// The demo seed is a single-node affordance
// ---------------------------------------------------------------------------

/// An empty store means two different things, and the difference is `peers`.
///
/// On a single node it means "nobody has configured anything yet", and demo
/// data is friendlier than a blank dashboard. On a node that has just joined a
/// cluster it means "the config is on its way" — and the demo rows do not stay
/// local. Results replicate, so one joining node seeds a fake `seed` agent onto
/// every node in the cluster, including the ones that were configured properly.
/// Removing it afterwards does not work either: a result delete has no
/// tombstone, so the next anti-entropy sweep pulls the rows back from a peer
/// that still has them.
#[tokio::test]
async fn a_joining_node_does_not_seed_demo_data_over_the_whole_cluster() {
    async fn status_agents(peers: Vec<String>) -> Vec<serde_json::Value> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let coord = Coordinator::new(
            &CoordinatorConfig {
                db_path: ":memory:".into(),
                peer_token: Some(PEER_TOKEN.into()),
                advertise_url: Some(format!("http://{addr}")),
                peers,
                ..Default::default()
            },
            // No checks: the state that triggers the seed.
            &[],
        )
        .unwrap();
        let app = coord
            .router()
            .into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let body: Value = reqwest::Client::new()
            .get(format!("http://{addr}/api/v1/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        body["agents"].as_array().cloned().unwrap_or_default()
    }

    let standalone = status_agents(vec![]).await;
    assert!(
        standalone.iter().any(|a| a["name"] == "seed"),
        "a standalone node with no checks should still seed demo data, or this \
         test proves nothing about the clustered case: {standalone:?}"
    );

    let joining = status_agents(vec!["http://127.0.0.1:9/".into()]).await;
    assert!(
        joining.is_empty(),
        "a node with peers configured must wait for its config instead of \
         inventing a fleet and replicating it: {joining:?}"
    );
}
