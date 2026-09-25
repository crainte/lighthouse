//! Chaos and recovery scenarios: peer unavailability during and across a
//! check's DOWN/recovery lifecycle, network partitions and bisections,
//! agent failover between coordinators, and coordinator restart mid-ingest.
//!
//! Built on the same in-process, real-socket pattern as `tests/cluster.rs`
//! and `tests/gossip.rs` — real `Coordinator`s over real localhost sockets,
//! nothing mocked below HTTP. "Unavailable" is simulated by aborting a
//! node's server task, which frees its port (so a peer's request gets a real
//! connection refusal), and "recovery" by rebinding the *same* address, the
//! way a restarted process behind a stable peer URL would.
//!
//! Convergence is always asserted the same way: identical current state and
//! incident lifecycle on every node once reconciliation has run, exactly one
//! incident per outage, and no re-paging for a transition a node only
//! learned about secondhand.
//!
//! ## Already covered elsewhere (referenced, not duplicated)
//!
//!   - **Peer unavailable through an entire DOWN-and-recovery cycle**:
//!     `tests/cluster.rs::anti_entropy_recovers_a_complete_missed_incident_lifecycle`
//!     (a peer that does not exist yet for the whole lifecycle, then joins
//!     and recovers the complete history — open, updates, and resolution —
//!     via anti-entropy).
//!   - **Exact-replay duplicate delivery** of a `/internal/replicate` batch:
//!     `tests/cluster.rs::replaying_a_replicate_batch_does_not_duplicate_history`.
//!   - **Reordered delivery** between the two replication transports for the
//!     same transition (gossip arriving before and after `/internal/replicate`):
//!     `tests/gossip.rs::gossip_and_batch_replication_are_idempotent_in_either_order`.
//!   - **A replayed or late belief never rewinds a tracker's state**, at the
//!     unit level: `src/coordinator/state.rs::a_replayed_or_late_batch_never_rewinds_a_belief`.
//!   - **Restart with a persistent database, committed-only state**:
//!     `tests/transactional_logical.rs::uncommitted_close_reopen_preserves_only_committed_logical_state`
//!     and `::gossip_write_failure_does_not_publish_and_can_retry_after_reopen`
//!     (crash boundaries around one write), plus
//!     `src/coordinator/storage/results.rs::restart_recovers_committed_rows_and_backup_restore_preserves_them`
//!     (row/watermark/rollup survival at scale). None of the three assert on
//!     the *incident* lifecycle across a restart, which is what
//!     `coordinator_restart_mid_incident_lifecycle_preserves_exactly_one_incident`
//!     below adds.
//!   - **A credential provisioned on one node is usable on its peer**:
//!     `tests/config_replication.rs::a_credential_minted_on_one_node_works_on_its_peer_until_revoked`
//!     — the mechanism `agent_failover_between_coordinators_converges_on_one_incident`
//!     below relies on (provisioned directly on both nodes here, to keep the
//!     failover scenario itself the focus rather than credential replication
//!     timing).

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde_json::{json, Value};

use uuid::Uuid;
use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::models::{CheckId, CheckKind, CheckResult};
use warden::notify::{Notifier, NotifierKind};

const PEER_TOKEN: &str = "chaos-peer-secret";
const ADMIN_PASSWORD: &str = "correct horse battery staple";
const CHECK: &str = "web";
const CHECK2: &str = "db";

/// Two hours: long enough that no test here sees a spurious suspect/dead
/// membership transition just from wall-clock time passing while other
/// assertions run — membership state is not what these tests exercise.
const NO_TIMEOUT: u64 = 7_200;

type Sink = Arc<Mutex<Vec<Value>>>;

fn cid(name: &str) -> CheckId {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes())
}

fn cid_str(name: &str) -> String {
    cid(name).to_string()
}

/// An alert endpoint that records what it was sent. Same shape as
/// `tests/cluster.rs::alert_sink`.
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

/// A live coordinator plus what a test needs to talk to it and to take it
/// down: its address, alert sink, direct `Coordinator` handle, and the
/// server task's `JoinHandle` so a test can abort it to simulate the node
/// becoming unreachable.
struct Node {
    coord: Coordinator,
    addr: SocketAddr,
    sink: Sink,
    handle: tokio::task::JoinHandle<()>,
}

async fn spawn_node(node_id: &str, peers: Vec<String>, checks: &[&str]) -> Node {
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
        peer_timeout_secs: NO_TIMEOUT,
        ..Default::default()
    })
    .await
    .unwrap();
    for name in checks {
        coord
            .create_check_for_test(CheckConfig {
                id: cid(name),
                name: (*name).into(),
                kind: CheckKind::Http,
                target: format!("https://{name}.example.invalid"),
                cron: "0 * * * * *".into(),
                timeout_secs: 5,
                ..Default::default()
            })
            .await;
    }
    let handle = serve(coord.clone(), listener);
    Node {
        coord,
        addr,
        sink,
        handle,
    }
}

fn serve(coord: Coordinator, listener: tokio::net::TcpListener) -> tokio::task::JoinHandle<()> {
    let app = coord
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    })
}

/// Abort a node's server task and wait until its port genuinely refuses
/// connections, so a caller's next request is a real connection failure
/// rather than a race against the task unwinding. Hands back the still-live
/// `Coordinator` and its address so the test can drive it directly while
/// "down" and later rebind the same address to bring it back.
async fn take_down(node: Node) -> (Coordinator, SocketAddr, Sink) {
    node.handle.abort();
    let start = Instant::now();
    loop {
        if tokio::net::TcpStream::connect(node.addr).await.is_err() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "{} never became unreachable after being taken down",
            node.addr
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (node.coord, node.addr, node.sink)
}

/// Rebind the exact address a node was serving on before `take_down`, the
/// way a restarted process behind a stable peer URL comes back. Retries
/// briefly in case the OS has not yet released the port.
async fn bring_back_up(coord: Coordinator, addr: SocketAddr, sink: Sink) -> Node {
    let start = Instant::now();
    let listener = loop {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => break listener,
            Err(error) => {
                assert!(
                    start.elapsed() < Duration::from_secs(5),
                    "failed to rebind {addr}: {error}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };
    let handle = serve(coord.clone(), listener);
    Node {
        coord,
        addr,
        sink,
        handle,
    }
}

fn result(check: &str, status_code: u16, ts_millis: i64) -> CheckResult {
    CheckResult {
        check_id: cid(check),
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

/// Poll until `f` returns `Some`, or fail naming what never arrived. Never a
/// substitute for a direct call — used only where a genuine wait is
/// unavoidable (replication is fire-and-forget; recovery is a real I/O
/// boundary).
async fn eventually<T, F, Fut>(what: &str, deadline: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(start.elapsed() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn get_json(url: &str) -> Value {
    reqwest::get(url).await.unwrap().json().await.unwrap()
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

/// One push-pull gossip round, exactly as `tests/cluster.rs::gossip_step`.
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

/// Every check's published status, keyed by name, from one node's status
/// page — the shape used to assert cross-node convergence.
async fn published_states(addr: SocketAddr) -> std::collections::BTreeMap<String, String> {
    let status = get_json(&format!("http://{addr}/api/v1/status")).await;
    status["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap().to_string(),
                c["status"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// Every incident id and whether it is resolved, a node currently holds, as
/// a set — used to assert two nodes converged on exactly the same incidents
/// rather than merely the same count. Boolean rather than the raw lifecycle
/// string (`detected`/`investigating`/.../`resolved`) because what these
/// tests assert on is convergence and open-vs-closed, not which of the
/// open sub-states an incident happens to be in.
async fn incident_states(coord: &Coordinator) -> std::collections::BTreeSet<(String, bool)> {
    coord
        .incidents_for_test(1_000)
        .await
        .into_iter()
        .map(|i| (i.id, i.resolved_at.is_some()))
        .collect()
}

fn open_incident(id: &str) -> (String, bool) {
    (id.to_string(), false)
}

fn resolved_incident(id: &str) -> (String, bool) {
    (id.to_string(), true)
}

// ---------------------------------------------------------------------------
// 1. Peer unavailable during the DOWN transition
// ---------------------------------------------------------------------------

/// B is an established cluster member — not a late joiner — when A confirms
/// a DOWN transition. B's port is unreachable at exactly that moment, so the
/// live push goes nowhere; B recovers only once it returns and reconciles.
/// Distinct from the already-covered "unreachable through the whole
/// lifecycle" case (`cluster.rs::anti_entropy_recovers_a_complete_missed_incident_lifecycle`):
/// there B never exists until after both transitions, so nothing is ever
/// "pushed and dropped" — here membership and a live push attempt exist and
/// the drop is real. Also folds in the duplicate-delivery angle: the sweep
/// that recovers B is run twice, so a reconciliation that has nothing left
/// to do must still be a safe no-op.
#[tokio::test]
async fn a_peer_unreachable_during_the_down_transition_recovers_without_duplicating() {
    let b = spawn_node("b", vec![], &[CHECK]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], &[CHECK]).await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    let (b_coord, b_addr, b_sink) = take_down(b).await;

    // A confirms the outage while B cannot be reached at all.
    a.coord.ingest(vec![result(CHECK, 200, 1_000)]).await;
    a.coord.ingest(vec![result(CHECK, 500, 2_000)]).await;

    let incident_id = eventually(
        "A to open an incident for the outage",
        Duration::from_secs(5),
        || async { a.coord.incidents_for_test(1).await.into_iter().next() },
    )
    .await
    .id;
    let paged = eventually("A to page", Duration::from_secs(5), || async {
        let n = a.sink.lock().len();
        (n > 0).then_some(n)
    })
    .await;
    assert_eq!(
        paged, 1,
        "A must have paged exactly once despite B being unreachable"
    );

    let b = bring_back_up(b_coord, b_addr, b_sink).await;
    b.coord.anti_entropy_sweep().await;
    // A second, immediately-following sweep must be a no-op: nothing left to
    // reconcile, and reconciling it twice must not double anything.
    b.coord.anti_entropy_sweep().await;

    let converged = incident_states(&b.coord).await;
    assert_eq!(
        converged,
        std::collections::BTreeSet::from([open_incident(&incident_id)]),
        "B must converge on exactly A's incident, not invent or duplicate one"
    );
    assert!(
        b.sink.lock().is_empty(),
        "B only learned about the transition secondhand and must never page for it"
    );
    assert_eq!(
        a.sink.lock().len(),
        1,
        "A's own page count must be unaffected by B's later reconciliation"
    );
}

// ---------------------------------------------------------------------------
// 2. Two-node partition healing
// ---------------------------------------------------------------------------

/// A and B are peers, then both directions go dark at once — neither can
/// reach the other, the way a real network split looks from either side —
/// rather than only one side's inbound port dropping. Each keeps observing
/// its own probes independently while split; healing must converge both on
/// one incident with no resurrection and no re-paging for the side that
/// only learns about it afterward.
#[tokio::test]
async fn two_node_partition_heals_with_exactly_one_incident_and_no_resurrection() {
    let b = spawn_node("b", vec![], &[CHECK]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], &[CHECK]).await;
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    let (a_coord, a_addr, a_sink) = take_down(a).await;
    let (b_coord, b_addr, b_sink) = take_down(b).await;

    // A observes the whole outage and recovery, unreachable throughout.
    a_coord.ingest(vec![result(CHECK, 200, 1_000)]).await;
    a_coord.ingest(vec![result(CHECK, 500, 2_000)]).await;
    a_coord.ingest(vec![result(CHECK, 200, 3_000)]).await;
    let incident_id = a_coord
        .incidents_for_test(1)
        .await
        .into_iter()
        .next()
        .expect("A should have opened an incident while partitioned")
        .id;

    // Heal: both sides come back on their original addresses.
    let a = bring_back_up(a_coord, a_addr, a_sink).await;
    let b = bring_back_up(b_coord, b_addr, b_sink).await;
    b.coord.anti_entropy_sweep().await;

    let a_states = incident_states(&a.coord).await;
    let b_states = incident_states(&b.coord).await;
    assert_eq!(
        a_states,
        std::collections::BTreeSet::from([resolved_incident(&incident_id)])
    );
    assert_eq!(
        a_states, b_states,
        "both sides of a healed partition must agree on exactly the same incident set"
    );
    assert_eq!(
        published_states(a.addr).await,
        published_states(b.addr).await,
        "both sides must publish the same current state per check once healed"
    );

    // Healing must not resurrect the outage: running the sweep again finds
    // nothing new and must not reopen or duplicate anything.
    b.coord.anti_entropy_sweep().await;
    assert_eq!(incident_states(&b.coord).await, b_states, "no resurrection");
    assert!(
        b.sink.lock().is_empty(),
        "B never observed the transition itself and must not page for it after healing"
    );
}

// ---------------------------------------------------------------------------
// 3. Full cluster bisection healing
// ---------------------------------------------------------------------------

/// A three-node mesh (A, B, C all peers of each other) is bisected by taking
/// only A down: A ends up isolated on one side, B and C stay connected to
/// each other on the other. While bisected, A observes its own outage and
/// B observes a *different* check's outage that still replicates live to C
/// — proving the still-connected side keeps functioning, not merely
/// waiting. Healing must converge all three nodes on both incidents with no
/// duplication.
#[tokio::test]
async fn full_cluster_bisection_heals_and_all_three_nodes_converge() {
    let checks = [CHECK, CHECK2];
    let c = spawn_node("c", vec![], &checks).await;
    let b = spawn_node("b", vec![format!("http://{}", c.addr)], &checks).await;
    let a = spawn_node(
        "a",
        vec![format!("http://{}", b.addr), format!("http://{}", c.addr)],
        &checks,
    )
    .await;
    let (session_a, session_b) = (login(a.addr).await, login(b.addr).await);
    gossip_step(a.addr, &session_a, b.addr).await;
    gossip_step(a.addr, &session_a, c.addr).await;
    gossip_step(b.addr, &session_b, c.addr).await;

    let (a_coord, a_addr, a_sink) = take_down(a).await;

    // A's own outage, isolated.
    a_coord.ingest(vec![result(CHECK, 200, 1_000)]).await;
    a_coord.ingest(vec![result(CHECK, 500, 2_000)]).await;
    let a_incident = a_coord
        .incidents_for_test(1)
        .await
        .into_iter()
        .next()
        .expect("A should open an incident while isolated")
        .id;

    // B and C are still connected to each other: B's outage must still
    // replicate live to C without needing A or a restart. Selected by
    // `origin_check` rather than "whichever incident is first": A's own
    // outbound pushes are not blocked by taking down its *inbound* listener
    // (a real process behind a dead link would be blocked both ways; an
    // aborted listener here only stops others from reaching it), so A's
    // incident can legitimately reach B first and this must not be
    // mistaken for B's own.
    b.coord.ingest(vec![result(CHECK2, 200, 1_000)]).await;
    b.coord.ingest(vec![result(CHECK2, 500, 2_000)]).await;
    let b_incident = eventually(
        "B to open an incident for CHECK2",
        Duration::from_secs(5),
        || async {
            b.coord
                .incidents_for_test(10)
                .await
                .into_iter()
                .find(|i| i.origin_check == Some(cid(CHECK2)))
        },
    )
    .await
    .id;
    eventually(
        "C to converge on B's incident across the still-live B-C link",
        Duration::from_secs(5),
        || async {
            incident_states(&c.coord)
                .await
                .contains(&open_incident(&b_incident))
                .then_some(())
        },
    )
    .await;

    // Heal: A returns on its original address.
    let a = bring_back_up(a_coord, a_addr, a_sink).await;
    a.coord.anti_entropy_sweep().await;
    c.coord.anti_entropy_sweep().await;
    // A second round so both directions of the mesh have had a chance to
    // pull from whichever peer actually holds what they are missing.
    a.coord.anti_entropy_sweep().await;
    b.coord.anti_entropy_sweep().await;

    let expected =
        std::collections::BTreeSet::from([open_incident(&a_incident), open_incident(&b_incident)]);
    for (name, coord) in [("a", &a.coord), ("b", &b.coord), ("c", &c.coord)] {
        let got = incident_states(coord).await;
        assert_eq!(
            got, expected,
            "node {name} must converge on both incidents after the bisection heals, got {got:?}"
        );
    }
    assert_eq!(
        published_states(a.addr).await,
        published_states(b.addr).await
    );
    assert_eq!(
        published_states(b.addr).await,
        published_states(c.addr).await
    );
    let paged = eventually("A to page", Duration::from_secs(5), || async {
        let n = a.sink.lock().len();
        (n > 0).then_some(n)
    })
    .await;
    assert_eq!(
        paged, 1,
        "A must still have paged exactly once for its own outage"
    );
}

// ---------------------------------------------------------------------------
// 4. Agent failover between coordinators
// ---------------------------------------------------------------------------

/// The same credential is provisioned on two peer coordinators (the
/// replication of a credential minted on one node is covered by
/// `tests/config_replication.rs::a_credential_minted_on_one_node_works_on_its_peer_until_revoked`;
/// provisioning both directly here keeps this test about the failover
/// itself). An agent normally reports to A; A becomes unreachable mid-poll
/// cycle, so the agent fails over and posts its next observation to B
/// instead. B, as the actual observer of the DOWN transition, opens the
/// incident and pages. Once A returns and reconciles, the agent's *recovery*
/// observation lands back on A — which must resolve the same incident B
/// opened, not invent a second one.
#[tokio::test]
async fn agent_failover_between_coordinators_converges_on_one_incident() {
    const AGENT_TOKEN: &str = "wac_chaos.failover-secret";
    let b = spawn_node("b", vec![], &[CHECK]).await;
    let a = spawn_node("a", vec![format!("http://{}", b.addr)], &[CHECK]).await;
    for node in [&a, &b] {
        node.coord
            .provision_agent_credential(AGENT_TOKEN, "zone", "region")
            .await
            .unwrap();
    }
    let session_a = login(a.addr).await;
    gossip_step(a.addr, &session_a, b.addr).await;

    let post_result = |addr: SocketAddr, code: u16, ts: &str| {
        let body = json!({"results": [{
            "check_id": cid_str(CHECK),
            "agent_id": "agent-1",
            "timestamp": ts,
            "latency_ms": 5,
            "status_code": code,
        }]});
        async move {
            reqwest::Client::new()
                .post(format!("http://{addr}/api/v1/results"))
                .bearer_auth(AGENT_TOKEN)
                .json(&body)
                .send()
                .await
                .unwrap()
        }
    };

    // Baseline. The agent normally only reports to the primary, but a
    // first-ever observation only seeds a tracker's baseline rather than
    // producing an alert (`state.rs::first_conclusion_sets_a_baseline_without_a_transition`),
    // and a no-transition ingest has no alert to replicate — so the
    // secondary is seeded directly here, the way it would be once caught up
    // on ordinary traffic, to keep this test about failover semantics
    // rather than replication propagation timing.
    let res = post_result(a.addr, 200, "2026-01-01T00:00:00Z").await;
    assert_eq!(res.status(), 202);
    b.coord.ingest(vec![result(CHECK, 200, 1_000)]).await;

    let (a_coord, a_addr, a_sink) = take_down(a).await;

    // Failover: the agent's next observation goes to the secondary, which
    // confirms DOWN and owns the page.
    let res = post_result(b.addr, 500, "2026-01-01T00:01:00Z").await;
    assert_eq!(
        res.status(),
        202,
        "the secondary must accept the failed-over agent"
    );
    let incident_id = eventually(
        "B to open an incident for the failed-over observation",
        Duration::from_secs(5),
        || async { b.coord.incidents_for_test(1).await.into_iter().next() },
    )
    .await
    .id;
    let paged = eventually("B to page", Duration::from_secs(5), || async {
        let n = b.sink.lock().len();
        (n > 0).then_some(n)
    })
    .await;
    assert_eq!(paged, 1, "B must page exactly once");

    // The primary returns and reconciles before the agent fails back to it.
    let a = bring_back_up(a_coord, a_addr, a_sink).await;
    a.coord.anti_entropy_sweep().await;
    eventually(
        "A to converge on B's incident before the recovery observation arrives",
        Duration::from_secs(5),
        || async {
            incident_states(&a.coord)
                .await
                .contains(&open_incident(&incident_id))
                .then_some(())
        },
    )
    .await;

    // Fail back: the recovery observation lands on the primary again.
    let res = post_result(a.addr, 200, "2026-01-01T00:02:00Z").await;
    assert_eq!(res.status(), 202);
    eventually(
        "A to resolve the incident it converged on, not open a second one",
        Duration::from_secs(5),
        || async {
            let states = incident_states(&a.coord).await;
            states
                .contains(&resolved_incident(&incident_id))
                .then_some(())
        },
    )
    .await;
    assert_eq!(
        a.coord.incidents_for_test(10).await.len(),
        1,
        "the failover-and-back cycle must produce exactly one incident on the primary"
    );

    b.coord.anti_entropy_sweep().await;
    let b_states = incident_states(&b.coord).await;
    assert_eq!(
        b_states,
        std::collections::BTreeSet::from([resolved_incident(&incident_id)]),
        "the secondary must converge on the same single resolved incident"
    );
    let a_paged = eventually(
        "A to page the resolution",
        Duration::from_secs(5),
        || async {
            let n = a.sink.lock().len();
            (n > 0).then_some(n)
        },
    )
    .await;
    assert_eq!(
        a_paged, 1,
        "A pages exactly once, for the resolution it observed"
    );
    assert_eq!(
        b.sink.lock().len(),
        1,
        "B's page count from the DOWN transition must be unaffected by the later resolution on A"
    );
}

// ---------------------------------------------------------------------------
// 5. Coordinator restart during ingest
// ---------------------------------------------------------------------------

/// A single coordinator, backed by a real on-disk database, is confirmed
/// DOWN over real HTTP ingest, then "restarts" — `drop`ping the coordinator
/// and reopening the same file, the same crash boundary
/// `tests/transactional_logical.rs` uses — before its recovery observation
/// arrives. The already-covered tests at that boundary assert on raw rows
/// and alert counts; this one is the incident-lifecycle-level assertion
/// neither makes: exactly one incident, correctly resolved, with nothing
/// lost or duplicated by the restart landing in between.
#[tokio::test]
async fn coordinator_restart_mid_incident_lifecycle_preserves_exactly_one_incident() {
    const AGENT_TOKEN: &str = "wac_chaos.restart-secret";
    let path = std::env::temp_dir()
        .join(format!("warden-chaos-restart-{}.duckdb", Uuid::now_v7()))
        .to_string_lossy()
        .into_owned();

    async fn open(path: &str) -> Coordinator {
        let coord = Coordinator::new(&CoordinatorConfig {
            listen: "127.0.0.1:0".into(),
            db_path: path.into(),
            peer_token: Some(PEER_TOKEN.into()),
            ..Default::default()
        })
        .await
        .unwrap();
        coord
            .create_check_for_test(CheckConfig {
                id: cid(CHECK),
                name: CHECK.into(),
                kind: CheckKind::Http,
                target: "https://example.invalid".into(),
                cron: "0 * * * * *".into(),
                timeout_secs: 5,
                ..Default::default()
            })
            .await;
        coord
    }

    let coord = open(&path).await;
    coord
        .provision_agent_credential(AGENT_TOKEN, "zone", "region")
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = serve(coord.clone(), listener);

    let post = |addr: SocketAddr, code: u16, ts: &str| {
        let body = json!({"results": [{
            "check_id": cid_str(CHECK),
            "agent_id": "agent-1",
            "timestamp": ts,
            "latency_ms": 5,
            "status_code": code,
        }]});
        async move {
            reqwest::Client::new()
                .post(format!("http://{addr}/api/v1/results"))
                .bearer_auth(AGENT_TOKEN)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status()
        }
    };

    assert_eq!(post(addr, 200, "2026-01-01T00:00:00Z").await, 202);
    assert_eq!(post(addr, 500, "2026-01-01T00:01:00Z").await, 202);
    let incident_id = eventually(
        "the DOWN transition to open an incident before the crash",
        Duration::from_secs(5),
        || async { coord.incidents_for_test(1).await.into_iter().next() },
    )
    .await
    .id;

    // Crash: the process dies right after committing the DOWN transition.
    // Same boundary as `transactional_logical.rs::close`: abort the server
    // task and drop the coordinator handle, then wait for DuckDB's file lock
    // to actually release before reopening it.
    handle.abort();
    drop(coord);
    let start = Instant::now();
    loop {
        match duckdb::Connection::open(&path) {
            Ok(_) => break,
            Err(_) if start.elapsed() < Duration::from_secs(5) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("database file never became reopenable: {error}"),
        }
    }

    // The credential was already committed before the crash; re-provisioning
    // the same token now would try to insert a duplicate primary key.
    let reopened = open(&path).await;
    assert_eq!(
        reopened.incidents_for_test(10).await.len(),
        1,
        "the committed incident must survive the restart, not be lost"
    );
    assert_eq!(
        reopened.incidents_for_test(1).await[0].id,
        incident_id,
        "the surviving incident must be the same one, not a re-derived duplicate"
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = serve(reopened.clone(), listener);
    assert_eq!(post(addr, 200, "2026-01-01T00:02:00Z").await, 202);
    eventually(
        "the post-restart recovery observation to resolve the same incident",
        Duration::from_secs(5),
        || async {
            let incidents = reopened.incidents_for_test(10).await;
            (incidents.len() == 1
                && incidents[0].id == incident_id
                && incidents[0].state.as_str() == "resolved")
                .then_some(())
        },
    )
    .await;
    assert_eq!(
        reopened.incidents_for_test(10).await.len(),
        1,
        "no duplicate incident from ingest continuing after the restart"
    );

    handle.abort();
    drop(reopened);
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::remove_file(&path).unwrap();
}
