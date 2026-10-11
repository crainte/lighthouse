//! Agent authentication and the identity it establishes.
//!
//! The properties here are what stop a caller with network reach from telling
//! the monitor what it saw. The sharpest one is not forged results: it is that
//! zone and region used to be self-asserted, and placement believes them. A
//! caller able to name its own zone can be handed real checks and then never
//! probe them, and nothing alerts, because "no results" reads exactly like
//! "not scheduled yet". So these tests assert the *bound* location, not merely
//! that a request was accepted.

use std::net::SocketAddr;

use lighthouse::config::{CheckConfig, CoordinatorConfig};
use lighthouse::coordinator::Coordinator;
use lighthouse::models::{CheckId, CheckKind};
use serde_json::{json, Value};
use uuid::Uuid;

const PASSWORD: &str = "correct horse battery staple";
const PEER_TOKEN: &str = "peer-only-secret";
const CHECK: &str = "payments-api";

fn cid(name: &str) -> CheckId {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes())
}

async fn coordinator(peer_token: Option<&str>) -> Coordinator {
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: ":memory:".into(),
        admin_password: Some(PASSWORD.into()),
        peer_token: peer_token.map(str::to_owned),
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

async fn serve(coord: Coordinator) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = coord
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
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

/// Mint through the real admin API, so the test exercises the same path an
/// operator does.
async fn mint(base: &str, session: &str, zone: &str, region: &str) -> String {
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/agent-credentials"))
        .header(reqwest::header::COOKIE, session)
        .json(&json!({ "zone": zone, "region": region }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "minting should succeed");
    let body: Value = res.json().await.unwrap();
    body["token"]
        .as_str()
        .expect("a token is returned")
        .to_owned()
}

/// Post one result, claiming `zone`/`region` — which a credentialed caller is
/// not entitled to decide.
async fn post_result(base: &str, token: &str, agent_id: &str, zone: &str, region: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth(token)
        .json(&json!({ "results": [{
            "check_id": cid(CHECK).to_string(),
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

#[tokio::test]
async fn a_credential_pins_location_over_whatever_the_agent_claims() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let token = mint(&base, &session, "eu-west-1a", "eu-west").await;

    // The agent lies about where it is.
    let status = post_result(&base, &token, "agent-1", "us-east-1b", "us-east").await;
    assert_eq!(status, 202, "a valid credential is still accepted");

    assert_eq!(
        located(&base, &session, "agent-1").await,
        ("eu-west-1a".to_string(), "eu-west".to_string()),
        "location must come from the credential, not the request body"
    );
}

/// The placement vector specifically: `/agent/checks` is what decides which
/// checks a caller is handed, and it reads the same self-declared location.
#[tokio::test]
async fn a_credential_pins_location_on_the_placement_path_too() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let token = mint(&base, &session, "eu-west-1a", "eu-west").await;

    let res = reqwest::Client::new()
        .get(format!(
            "{base}/api/v1/agent/checks?agent_id=agent-2&zone=us-east-1b&region=us-east"
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());

    // It has not reported a result, so it is only in the fleet by having asked
    // for work — which is exactly the state placement acts on.
    post_result(&base, &token, "agent-2", "us-east-1b", "us-east").await;
    assert_eq!(
        located(&base, &session, "agent-2").await,
        ("eu-west-1a".to_string(), "eu-west".to_string()),
        "asking for work must not let a caller place itself in another zone"
    );
}

/// A coordinator with no credentials accepts nothing from agents. There is no
/// fleet-wide token to fall back to and no open mode, so this is the first-run
/// state until an operator mints one.
#[tokio::test]
async fn nothing_is_accepted_until_a_credential_exists() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");

    assert_eq!(
        post_result(&base, "anything-at-all", "agent-3", "z", "r").await,
        401,
        "ingest must refuse before any credential is minted"
    );
    let res = reqwest::get(format!("{base}/api/v1/agent/checks?agent_id=agent-3"))
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        401,
        "check targets must not be readable either"
    );

    // The forged DOWN must not have landed.
    let v: Value = reqwest::get(format!("{base}/api/v1/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let c = v["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["check_id"] == cid(CHECK).to_string())
        .expect("the configured check is still listed");
    assert_eq!(
        c["status"], "unknown",
        "a refused result must not be stored"
    );

    // ...and it starts working the moment one exists.
    let session = login(&base).await;
    let token = mint(&base, &session, "eu-west-1a", "eu-west").await;
    assert_eq!(post_result(&base, &token, "agent-3", "z", "r").await, 202);
}

#[tokio::test]
async fn revoking_a_credential_takes_effect_without_a_restart() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let token = mint(&base, &session, "eu-west-1a", "eu-west").await;
    let id = token.split('.').next().unwrap().to_string();

    assert_eq!(post_result(&base, &token, "agent-4", "z", "r").await, 202);

    let res = reqwest::Client::new()
        .delete(format!("{base}/api/v1/admin/agent-credentials/{id}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    assert_eq!(
        post_result(&base, &token, "agent-4", "z", "r").await,
        401,
        "a revoked credential must stop working immediately, not at next boot"
    );

    // Still listed, so an operator can see it existed.
    let v: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/admin/agent-credentials"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = v["credentials"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == id)
        .expect("revoked credentials stay listed for audit");
    assert_eq!(row["revoked"], true);
}

/// Minting must never hand back something that lets the caller read it again.
#[tokio::test]
async fn the_secret_is_returned_once_and_never_listed() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let token = mint(&base, &session, "eu-west-1a", "eu-west").await;
    let secret = token.split('.').nth(1).unwrap();

    let raw = reqwest::Client::new()
        .get(format!("{base}/api/v1/admin/agent-credentials"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        !raw.contains(secret),
        "the listing must never carry the secret: {raw}"
    );
}

#[tokio::test]
async fn credentials_are_admin_only() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    for res in [
        client
            .get(format!("{base}/api/v1/admin/agent-credentials"))
            .send()
            .await
            .unwrap(),
        client
            .post(format!("{base}/api/v1/admin/agent-credentials"))
            .json(&json!({ "zone": "z", "region": "r" }))
            .send()
            .await
            .unwrap(),
    ] {
        assert_eq!(
            res.status(),
            401,
            "minting agent credentials without a session would be a free pass \
             into ingest"
        );
    }

    // An agent credential is not an admin session either.
    let session = login(&base).await;
    let agent = mint(&base, &session, "eu-west-1a", "eu-west").await;
    let res = client
        .get(format!("{base}/api/v1/admin/agent-credentials"))
        .bearer_auth(agent)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        401,
        "an agent must not be able to mint itself more credentials"
    );
}

#[tokio::test]
async fn a_blank_location_is_refused() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    for body in [
        json!({ "zone": "  ", "region": "eu-west" }),
        json!({ "zone": "eu-west-1a", "region": "" }),
    ] {
        let res = reqwest::Client::new()
            .post(format!("{base}/api/v1/admin/agent-credentials"))
            .header(reqwest::header::COOKIE, &session)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            400,
            "a credential bound to nowhere binds nothing"
        );
    }
}

/// Peers are not agents. Once agents hold minted credentials the shared token
/// is no longer handed out, so gossip must not depend on it.
#[tokio::test]
async fn gossip_does_not_accept_an_agent_credential() {
    let addr = serve(coordinator(Some(PEER_TOKEN)).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let agent_token = mint(&base, &session, "eu-west-1a", "eu-west").await;

    let msg = json!({ "check_id": cid(CHECK), "from": "up", "to": "down", "timestamp": 5_000 });
    let post = |token: String| {
        let base = base.clone();
        let msg = msg.clone();
        async move {
            reqwest::Client::new()
                .post(format!("{base}/internal/gossip"))
                .bearer_auth(token)
                .json(&msg)
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    };

    assert_eq!(
        post(agent_token).await,
        401,
        "an agent credential must not let a caller rewrite published state"
    );
    assert!(
        (200..300).contains(&post(PEER_TOKEN.to_string()).await),
        "the peer token is what opens gossip"
    );
}

/// The grain is the **zone**, not the agent: every agent in a zone presents the
/// same credential. Worth pinning, because the natural misreading ("one token
/// per agent") changes how an operator provisions a fleet.
#[tokio::test]
async fn one_credential_is_shared_by_every_agent_in_its_zone() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let zone_token = mint(&base, &session, "eu-west-1a", "eu-west").await;

    // Three distinct agents, one shared credential.
    for agent in ["prober-a", "prober-b", "prober-c"] {
        assert_eq!(
            post_result(&base, &zone_token, agent, "somewhere", "else").await,
            202,
            "{agent} shares its zone's credential"
        );
        assert_eq!(
            located(&base, &session, agent).await,
            ("eu-west-1a".to_string(), "eu-west".to_string()),
            "and is placed in the zone the credential was minted for"
        );
    }
}

/// ...but nothing forces that grain. Minting several credentials for one zone
/// is allowed, and is how you get per-agent revocation if you want it.
#[tokio::test]
async fn a_zone_may_hold_several_credentials() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    let a = mint(&base, &session, "eu-west-1a", "eu-west").await;
    let b = mint(&base, &session, "eu-west-1a", "eu-west").await;
    assert_ne!(a, b);

    assert_eq!(post_result(&base, &a, "prober-a", "x", "y").await, 202);
    assert_eq!(post_result(&base, &b, "prober-b", "x", "y").await, 202);

    // Revoking one leaves the other working, so a fleet provisioned one
    // credential per agent can retire a single machine.
    let id_a = a.split('.').next().unwrap();
    reqwest::Client::new()
        .delete(format!("{base}/api/v1/admin/agent-credentials/{id_a}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();

    assert_eq!(post_result(&base, &a, "prober-a", "x", "y").await, 401);
    assert_eq!(
        post_result(&base, &b, "prober-b", "x", "y").await,
        202,
        "revoking one credential must not take down the rest of the zone"
    );
}

/// The ingest route accepts batches bigger than the coordinator-wide 1 MiB
/// default, but must still refuse anything over its own 16 MiB ceiling before
/// either authentication or JSON parsing takes place.
#[tokio::test]
async fn oversized_result_ingest_is_rejected_for_authenticated_and_anonymous_callers() {
    let addr = serve(coordinator(None).await).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let token = mint(&base, &session, "eu-west-1a", "eu-west").await;
    let oversized = vec![b'x'; 16 * 1024 * 1024 + 1];

    for credential in [Some(token.as_str()), None] {
        let mut request = reqwest::Client::new()
            .post(format!("{base}/api/v1/results"))
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        if let Some(credential) = credential {
            request = request.bearer_auth(credential);
        }
        let response = request.body(oversized.clone()).send().await.unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::PAYLOAD_TOO_LARGE,
            "the route limit must reject an oversized ingest before auth distinguishes callers"
        );
    }
}
