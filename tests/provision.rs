//! `[[coordinator.provision]]`: agent credentials whose token was chosen
//! before the coordinator ever ran.
//!
//! Minting in the admin UI is right when an operator is standing at a browser.
//! It is impossible when the agents and the coordinator start from the same
//! file — a compose stack, a Kubernetes manifest, a fixture — because the
//! agents need the token before there is a UI to mint it in.
//!
//! The properties defended here are the ones that make that safe to leave in a
//! config file forever, rather than only being right on the first boot:
//!
//!   1. the credential exists and pins location, exactly as a minted one does;
//!   2. a *second* boot changes nothing — no new salt, no new HLC stamp, and
//!      therefore no config-plane write pushed to every peer on every restart.
//!      That cost is invisible on a single node, which is why it needs a test;
//!   3. a revoked credential is not resurrected by the block that created it,
//!      which is the same property stated as a security one;
//!   4. a token that cannot be resolved stops the boot instead of being
//!      skipped — a fleet authenticating against nothing looks exactly like a
//!      fleet that has not been scheduled work yet;
//!   5. one node provisioning is enough for a cluster: the credential reaches
//!      the others over the config plane.

use std::net::SocketAddr;

use serde_json::{json, Value};
use uuid::Uuid;
use warden::config::{CheckConfig, CoordinatorConfig, ProvisionedCredential};
use warden::coordinator::Coordinator;
use warden::models::{CheckId, CheckKind};

const PASSWORD: &str = "correct horse battery staple";
const PEER_TOKEN: &str = "peer-only-secret";
const CHECK: &str = "payments-api";
/// Shaped like a minted token (`wac_<id>.<secret>`) because that is all the
/// coordinator will accept — the point of provisioning is choosing the value,
/// not inventing a second token format.
const TOKEN: &str = "wac_provisioned.a-secret-chosen-in-advance";

fn block(
    zone: &str,
    region: &str,
    token: Option<&str>,
    env: Option<&str>,
) -> ProvisionedCredential {
    ProvisionedCredential {
        zone: zone.into(),
        region: region.into(),
        token: token.map(str::to_owned),
        token_env: env.map(str::to_owned),
    }
}

fn config(db_path: &str, provision: Vec<ProvisionedCredential>) -> CoordinatorConfig {
    CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: db_path.into(),
        admin_password: Some(PASSWORD.into()),
        // Only so the tests can read `/internal/config` back. With no
        // `advertise_url` this node is still standalone.
        peer_token: Some(PEER_TOKEN.into()),
        provision,
        ..Default::default()
    }
}

fn cid(name: &str) -> CheckId {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes())
}

fn checks() -> Vec<CheckConfig> {
    vec![CheckConfig {
        id: cid(CHECK),
        name: CHECK.into(),
        kind: CheckKind::Http,
        target: "https://example.invalid".into(),
        cron: "0 * * * * *".into(),
        timeout_secs: 5,
        ..Default::default()
    }]
}

async fn boot(db_path: &str, provision: Vec<ProvisionedCredential>) -> anyhow::Result<Coordinator> {
    let coord = Coordinator::new(&config(db_path, provision)).await?;
    for check in checks() {
        coord.create_check_for_test(check).await;
    }
    Ok(coord)
}

/// The refusal message, or a failure naming what booted when it should not
/// have. `Coordinator` is not `Debug`, so this stands in for `expect_err`.
async fn boot_error(provision: Vec<ProvisionedCredential>, why: &str) -> String {
    match boot(":memory:", provision).await {
        Ok(_) => panic!("{why}"),
        Err(e) => e.to_string(),
    }
}

/// Serve, keeping the handle so a restart test can stop this node before the
/// next one opens the same database file.
async fn serve(coord: Coordinator) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = coord
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, handle)
}

async fn login(base: &str) -> String {
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&json!({ "password": PASSWORD }))
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

/// Post one result claiming a location the credential does not grant.
async fn post_result(base: &str, token: &str, agent_id: &str, zone: &str, region: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(token)
        .json(&json!({ "results": [{
            "check_id": cid(CHECK),
            "agent_id": agent_id,
            "zone": zone,
            "region": region,
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

/// Where the coordinator believes an agent is, read back through the admin API.
async fn located(base: &str, session: &str, agent_id: &str) -> (String, String) {
    let v: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/agents"))
        .header(reqwest::header::COOKIE, session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let a = v["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["agent_id"] == agent_id)
        .unwrap_or_else(|| panic!("agent {agent_id} should be listed: {v}"))
        .clone();
    (
        a["zone"].as_str().unwrap().to_owned(),
        a["region"].as_str().unwrap().to_owned(),
    )
}

/// The replicated view of the credential rows: the stored document (salt and
/// `created_at` included) together with the HLC stamp a peer would order it
/// by. Both halves matter for the idempotency test — a re-provision would
/// change the salt, and *any* rewrite would change the stamp and be pushed.
async fn credential_entries(addr: SocketAddr) -> Vec<Value> {
    let v: Value = reqwest::Client::new()
        .get(format!("http://{addr}/internal/config?kind=credential"))
        .bearer_auth(PEER_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["entries"].as_array().cloned().unwrap_or_default()
}

async fn stop(handle: tokio::task::JoinHandle<()>) {
    handle.abort();
    let _ = handle.await;
}

/// A private directory per test, since two tests running in parallel must not
/// share a database file.
fn db_file(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("warden-provision-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.duckdb"));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("duckdb.wal"));
    path.to_str().unwrap().to_string()
}

// ---------------------------------------------------------------------------
// 1. It provisions, and the credential behaves like a minted one
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_configured_credential_works_and_pins_the_zone_it_declares() {
    let coord = boot(
        ":memory:",
        vec![block("us-east-1a", "us-east", Some(TOKEN), None)],
    )
    .await
    .unwrap();
    let (addr, handle) = serve(coord).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    assert_eq!(
        post_result(&base, TOKEN, "agent-a", "attacker-zone", "attacker-region").await,
        202,
        "a credential provisioned from config must be accepted like a minted one"
    );
    assert_eq!(
        located(&base, &session, "agent-a").await,
        ("us-east-1a".to_string(), "us-east".to_string()),
        "location comes from the credential the config declared, never from \
         what the agent claimed"
    );
    stop(handle).await;
}

/// `token_env` is the form a deployment uses: the block names a variable and
/// the secret stays out of the committed file.
#[tokio::test]
async fn the_token_may_come_from_the_environment() {
    const VAR: &str = "WARDEN_TEST_PROVISION_TOKEN_FROM_ENV";
    std::env::set_var(VAR, format!("  {TOKEN}  "));
    let coord = boot(
        ":memory:",
        vec![block("eu-west-1a", "eu-west", None, Some(VAR))],
    )
    .await
    .unwrap();
    let (addr, handle) = serve(coord).await;
    let base = format!("http://{addr}");

    assert_eq!(
        post_result(&base, TOKEN, "agent-env", "eu-west-1a", "eu-west").await,
        202,
        "the token should be read from the environment, trimmed"
    );
    std::env::remove_var(VAR);
    stop(handle).await;
}

// ---------------------------------------------------------------------------
// 2. A second boot is a no-op
// ---------------------------------------------------------------------------

/// The block stays in the file forever, so it is applied on every boot. If it
/// re-provisioned, `credential::provision` would invent a fresh salt, write a
/// different record and stamp a new HLC — a config-plane write pushed to every
/// peer, every restart, for a credential nobody changed.
///
/// Invisible on one node. This is the test that fails if someone "simplifies"
/// the existence check away.
#[tokio::test]
async fn a_second_boot_rewrites_nothing() {
    let db = db_file("second-boot");
    let first = {
        let (addr, handle) = serve(
            boot(&db, vec![block("us-east-1a", "us-east", Some(TOKEN), None)])
                .await
                .unwrap(),
        )
        .await;
        let entries = credential_entries(addr).await;
        stop(handle).await;
        entries
    };
    assert_eq!(
        first.len(),
        1,
        "the first boot should provision one: {first:?}"
    );

    let (addr, handle) = serve(
        boot(&db, vec![block("us-east-1a", "us-east", Some(TOKEN), None)])
            .await
            .unwrap(),
    )
    .await;
    let second = credential_entries(addr).await;
    stop(handle).await;

    assert_eq!(
        second, first,
        "a second boot with the same block must leave the row byte-identical: \
         same salt, same created_at, same HLC stamp, nothing to replicate"
    );
    let _ = std::fs::remove_file(&db);
}

/// The same skip, but reached because a *peer* got there first: the id is
/// already in the map, so there is nothing to write even though this node has
/// never provisioned anything itself.
#[tokio::test]
async fn a_credential_already_replicated_in_is_left_alone() {
    let db = db_file("already-present");
    let replicated = {
        let (addr, handle) = serve(
            boot(
                &db,
                vec![block("ap-south-1a", "ap-south", Some(TOKEN), None)],
            )
            .await
            .unwrap(),
        )
        .await;
        let entries = credential_entries(addr).await;
        stop(handle).await;
        entries
    };

    // Same store, but this boot declares the *same token* under a different
    // location. The id already exists, so the row must not move: rewriting it
    // would relocate a fleet on a restart.
    let (addr, handle) = serve(
        boot(
            &db,
            vec![block("elsewhere-1a", "elsewhere", Some(TOKEN), None)],
        )
        .await
        .unwrap(),
    )
    .await;
    let after = credential_entries(addr).await;
    stop(handle).await;

    assert_eq!(
        after, replicated,
        "an existing id is skipped entirely, whoever wrote it first"
    );
    let _ = std::fs::remove_file(&db);
}

// ---------------------------------------------------------------------------
// 3. Revocation survives a restart
// ---------------------------------------------------------------------------

/// Revocation keeps the row and sets `revoked_at`, so the existence check by
/// id is also what stops a restart from handing a revoked fleet its access
/// back. A skip that filtered revoked rows out would undo an operator's
/// security action on the next deploy.
#[tokio::test]
async fn a_revoked_credential_is_not_resurrected_by_its_own_config_block() {
    let db = db_file("revoked");
    let id = {
        let (addr, handle) = serve(
            boot(&db, vec![block("us-east-1a", "us-east", Some(TOKEN), None)])
                .await
                .unwrap(),
        )
        .await;
        let base = format!("http://{addr}");
        let session = login(&base).await;
        let listed: Value = reqwest::Client::new()
            .get(format!("{base}/api/v1/admin/agent-credentials"))
            .header(reqwest::header::COOKIE, &session)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let id = listed["credentials"][0]["id"].as_str().unwrap().to_owned();
        let res = reqwest::Client::new()
            .delete(format!("{base}/api/v1/admin/agent-credentials/{id}"))
            .header(reqwest::header::COOKIE, &session)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "revocation should succeed");
        assert_eq!(
            post_result(&base, TOKEN, "agent-a", "us-east-1a", "us-east").await,
            401,
            "revoked means refused, before any restart is involved"
        );
        stop(handle).await;
        id
    };

    let (addr, handle) = serve(
        boot(&db, vec![block("us-east-1a", "us-east", Some(TOKEN), None)])
            .await
            .unwrap(),
    )
    .await;
    let base = format!("http://{addr}");
    assert_eq!(
        post_result(&base, TOKEN, "agent-a", "us-east-1a", "us-east").await,
        401,
        "restarting with the block still present must not un-revoke {id}"
    );
    stop(handle).await;
    let _ = std::fs::remove_file(&db);
}

// ---------------------------------------------------------------------------
// 4. An unusable block stops the boot
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unresolvable_token_fails_startup_naming_the_zone_and_the_variable() {
    const VAR: &str = "WARDEN_TEST_PROVISION_TOKEN_NEVER_SET";
    let err = boot_error(
        vec![block("us-east-1a", "us-east", None, Some(VAR))],
        "a block whose token resolves to nothing must not boot silently",
    )
    .await;
    assert!(err.contains("us-east-1a"), "should name the zone: {err}");
    assert!(err.contains(VAR), "should name the variable: {err}");
}

#[tokio::test]
async fn a_malformed_token_fails_startup() {
    let err = boot_error(
        vec![block(
            "us-east-1a",
            "us-east",
            Some("not-a-warden-token"),
            None,
        )],
        "a token that is not shaped like a credential must not boot",
    )
    .await;
    assert!(err.contains("us-east-1a"), "should name the zone: {err}");
    assert!(
        err.contains("malformed"),
        "should say what is wrong with it: {err}"
    );
}

#[tokio::test]
async fn a_blank_location_fails_startup() {
    let err = boot_error(
        vec![block("  ", "us-east", Some(TOKEN), None)],
        "a credential bound to no zone is the state credentials replace",
    )
    .await;
    assert!(err.contains("zone"), "should say what is missing: {err}");
}

// ---------------------------------------------------------------------------
// 5. One node provisioning is enough for a cluster
// ---------------------------------------------------------------------------

/// What the compose cluster relies on: the blocks are declared on one
/// coordinator, and the agents pointed at the *other* nodes authenticate
/// against a credential those nodes were never configured with.
///
/// The boot-time push reaches nobody — A has no members yet when it
/// provisions — so this is anti-entropy's job, driven directly here rather
/// than waiting out the sweep timer.
#[tokio::test]
async fn a_credential_provisioned_on_one_node_is_accepted_by_its_peer() {
    let a = spawn_peer("a", vec![block("us-east-1a", "us-east", Some(TOKEN), None)]).await;
    let b = spawn_peer("b", vec![]).await;
    let base_b = format!("http://{}", b.addr);

    assert_eq!(
        post_result(&base_b, TOKEN, "agent-a", "us-east-1a", "us-east").await,
        401,
        "setup: B must not know the credential yet, or this proves nothing"
    );

    let session_a = login(&format!("http://{}", a.addr)).await;
    gossip_step(a.addr, &session_a, b.addr).await;
    b.coord.anti_entropy_sweep().await;

    assert_eq!(
        post_result(&base_b, TOKEN, "agent-a", "us-east-1a", "us-east").await,
        202,
        "a credential provisioned on A must work on B: mint once for the \
         fleet, not once per coordinator"
    );
    let session_b = login(&base_b).await;
    assert_eq!(
        located(&base_b, &session_b, "agent-a").await,
        ("us-east-1a".to_string(), "us-east".to_string()),
        "and it must arrive carrying its location, not a default"
    );
}

struct Peer {
    coord: Coordinator,
    addr: SocketAddr,
}

/// A cluster-capable node: `peer_token` plus an `advertise_url`, so membership
/// and the config plane are both live. No seeds — the tests drive gossip by
/// hand, as `tests/config_replication.rs` does.
async fn spawn_peer(node_id: &str, provision: Vec<ProvisionedCredential>) -> Peer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let coord = Coordinator::new(&CoordinatorConfig {
        node_id: Some(node_id.into()),
        advertise_url: Some(format!("http://{addr}")),
        // Generous: nothing here exercises failure detection, and a
        // spurious suspect mid-test must not drop a config push.
        peer_timeout_secs: 7_200,
        ..config(":memory:", provision)
    })
    .await
    .unwrap();
    for check in checks() {
        coord.create_check_for_test(check).await;
    }
    let app = coord
        .clone()
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Peer { coord, addr }
}

/// One push-pull membership round, driven directly rather than waiting on the
/// heartbeat timer — see `tests/cluster.rs::gossip_step`.
async fn gossip_step(from: SocketAddr, from_session: &str, to: SocketAddr) {
    let from_view: Value = reqwest::Client::new()
        .get(format!("http://{from}/api/v1/cluster"))
        .header(reqwest::header::COOKIE, from_session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ping = json!({
        "node_id": from_view["self"]["node_id"],
        "url": from_view["self"]["url"],
        "generation": from_view["self"]["generation"],
        "members": from_view["members"],
    });
    let reply = post_ping(to, &ping).await;
    post_ping(from, &reply).await;
}

async fn post_ping(addr: SocketAddr, body: &Value) -> Value {
    let res = reqwest::Client::new()
        .post(format!("http://{addr}/internal/cluster/ping"))
        .bearer_auth(PEER_TOKEN)
        .json(body)
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success(), "ping to {addr} was rejected");
    res.json().await.unwrap()
}
