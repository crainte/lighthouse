//! Ingest refuses when nothing is configured.
//!
//! This used to be the opposite: with no agent token set, `agent_ok` returned
//! `true` and any caller who could reach the coordinator could post results for
//! checks they do not run, and read every check target. A monitor that can be
//! told what it saw is worse than no monitor, because it is believed.
//!
//! Its own test binary on purpose. The assertion needs `WARDEN_AGENT_TOKEN`
//! genuinely absent, and removing a process-global env var races with anything
//! running beside it. One test per binary, no other thread to race with.

use std::net::SocketAddr;

use serde_json::{json, Value};
use warden::config::{CheckConfig, CoordinatorConfig};
use warden::coordinator::Coordinator;
use warden::models::CheckKind;

const CHECK: &str = "payments-api";

#[tokio::test]
async fn ingest_refuses_when_no_credential_and_no_token_exist() {
    // The docker-compose workflow encourages exporting this via `.env`, so the
    // config's `None` only means "unset" once the environment agrees.
    std::env::remove_var("WARDEN_AGENT_TOKEN");

    let coord = Coordinator::new(
        &CoordinatorConfig {
            listen: "127.0.0.1:0".into(),
            db_path: ":memory:".into(),
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = coord
        .clone()
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");

    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .json(&json!({ "results": [{
            "check_id": CHECK,
            "agent_id": "anonymous",
            "timestamp": "2026-07-11T00:00:00Z",
            "latency_ms": 1,
            "status_code": 500,
        }]}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        401,
        "an unconfigured coordinator must refuse results, not accept them"
    );

    // Refusing has to mean nothing was written. A 401 that still ingested would
    // be the same hole with a worse error message.
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
        .find(|c| c["check_id"] == CHECK)
        .expect("the configured check is still listed");
    assert_eq!(
        c["status"], "unknown",
        "the forged DOWN must not have landed"
    );

    // Check targets are readable through the agent endpoint, so it closes too.
    let res = reqwest::get(format!("{base}/api/v1/agent/checks?agent_id=anonymous"))
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        401,
        "check targets must not be readable by an unauthenticated caller"
    );
}
