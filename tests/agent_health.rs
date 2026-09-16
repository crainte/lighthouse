//! Agent health on the public status page.
//!
//! Two claims are load-bearing here. The first is that an anonymous visitor can
//! read whether monitoring itself is healthy — that used to require
//! `/api/v1/agents`, which is admin-gated, so the homepage's `Promise.all`
//! rejected on the 401 and the whole page failed to render. The second is that
//! opening it up did not publish the fleet: a region row says "3 of 3 healthy",
//! it does not name the agents, zones or cadences that map where monitoring
//! runs from.
//!
//! Everything runs against the real router over a real localhost socket.

use std::net::SocketAddr;

use serde_json::{json, Value};

use uuid::Uuid;
use warden::config::{AgentsDisplay, AgentsMode, CheckConfig, CoordinatorConfig, StatusPageConfig};
use warden::coordinator::Coordinator;
use warden::models::{CheckId, CheckKind};

const PASSWORD: &str = "correct horse battery staple";

fn coordinator(agents: AgentsDisplay) -> Coordinator {
    let coord = coordinator_at(":memory:", agents);
    coord
        .provision_agent_credential("wac_eu.secret", "eu-west-1a", "eu-west")
        .unwrap();
    coord
        .provision_agent_credential("wac_us.secret", "us-east-1a", "us-east")
        .unwrap();
    coord
}

fn cid(name: &str) -> CheckId {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes())
}

fn coordinator_at(db_path: &str, agents: AgentsDisplay) -> Coordinator {
    let coord = Coordinator::new(&CoordinatorConfig {
        listen: "127.0.0.1:0".into(),
        db_path: db_path.into(),
        admin_password: Some(PASSWORD.into()),
        status_page: StatusPageConfig { agents },
        ..Default::default()
    })
    .unwrap();
    coord.create_check_for_test(CheckConfig {
        id: cid("payments-api"),
        name: "payments-api".into(),
        kind: CheckKind::Http,
        target: "https://example.invalid".into(),
        cron: "0 * * * * *".into(),
        timeout_secs: 5,
        ..Default::default()
    });
    coord
}

/// Open the same database file again after the previous coordinator was shut
/// down. DuckDB holds a file lock until its handle actually drops, and an
/// aborted server task unwinds a moment after `abort()` returns, so this waits
/// for the lock rather than racing it.
fn reopen(db_path: &str) -> Coordinator {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match std::panic::catch_unwind(|| coordinator_at(db_path, AgentsDisplay::default())) {
            Ok(coord) => return coord,
            Err(e) if std::time::Instant::now() >= deadline => std::panic::resume_unwind(e),
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }
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

/// Serve on a handle the caller can abort, so a test can shut the coordinator
/// down and reopen its database file.
async fn serve_abortable(coord: Coordinator) -> (SocketAddr, tokio::task::JoinHandle<()>) {
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
        .json(&serde_json::json!({ "password": PASSWORD }))
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

const DISPLAY: &str = "/api/v1/admin/status-page/agents";

/// PUT the display settings, returning the status and body so a test can assert
/// on a rejection's wording as well as its code.
async fn put_display(base: &str, session: &str, body: Value) -> (u16, Value) {
    let res = reqwest::Client::new()
        .put(format!("{base}{DISPLAY}"))
        .header(reqwest::header::COOKIE, session)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    (status, res.json().await.unwrap())
}

/// The wire shape of the display settings. `rows` mixes agents and regions in
/// display order; `mode` decides what happens to everything they don't cover.
fn display(mode: &str, rows: Value) -> Value {
    serde_json::json!({
        "mode": mode,
        "rows": rows,
        "warn_below": 100.0,
        "down_below": 50.0,
    })
}

fn agent_row(name: &str) -> Value {
    serde_json::json!({ "kind": "agent", "name": name })
}

fn region_row(name: &str) -> Value {
    serde_json::json!({ "kind": "region", "name": name })
}

/// One poll for work — the heartbeat the health section is derived from.
async fn poll(base: &str, token: &str, agent_id: &str, poll_secs: Option<u64>) -> u16 {
    let mut url = format!("{base}/api/v1/agent/checks?agent_id={agent_id}");
    if let Some(s) = poll_secs {
        url.push_str(&format!("&poll_secs={s}"));
    }
    reqwest::Client::new()
        .get(url)
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn anonymous_status(base: &str) -> Value {
    let res = reqwest::get(format!("{base}/api/v1/status")).await.unwrap();
    assert_eq!(res.status(), 200, "the status page is public");
    res.json().await.unwrap()
}

#[tokio::test]
async fn the_public_page_reads_fleet_health_without_a_session() {
    let coord = coordinator(AgentsDisplay::default());
    let addr = serve(coord).await;
    let base = format!("http://{addr}");

    assert_eq!(
        poll(&base, "wac_eu.secret", "eu-probe-1", Some(10)).await,
        200
    );

    let body = anonymous_status(&base).await;
    let rows = body["agents"].as_array().expect("agents[] is present");
    assert_eq!(rows.len(), 1, "one region is polling: {body}");
    assert_eq!(rows[0]["name"], "eu-west");
    assert_eq!(rows[0]["status"], "up");
    assert_eq!(rows[0]["healthy"], 1);
    assert_eq!(rows[0]["total"], 1);

    // The fleet itself stays admin-only, and the region row must not smuggle it
    // out: no agent id, no zone.
    let serialized = body.to_string();
    assert!(
        !serialized.contains("eu-probe-1") && !serialized.contains("eu-west-1a"),
        "the public page must not name agents or zones: {serialized}"
    );
    let agents = reqwest::get(format!("{base}/api/v1/agents")).await.unwrap();
    assert_eq!(
        agents.status(),
        401,
        "the per-agent list is still admin-only"
    );
}

/// The homepage renders the section from whatever the coordinator publishes, so
/// `mode = "off"` has to be answered by the backend rather than by the client
/// choosing not to look.
#[tokio::test]
async fn off_publishes_no_fleet_at_all() {
    let coord = coordinator(AgentsDisplay {
        mode: AgentsMode::Off,
        ..Default::default()
    });
    let addr = serve(coord).await;
    let base = format!("http://{addr}");

    assert_eq!(
        poll(&base, "wac_eu.secret", "eu-probe-1", Some(10)).await,
        200
    );

    let body = anonymous_status(&base).await;
    assert!(
        body["agents"].as_array().expect("still a list").is_empty(),
        "mode=off publishes nothing: {body}"
    );
}

/// The composition the row model exists for: one named agent, then a whole
/// region, in that order, with nothing else on the page.
#[tokio::test]
async fn placed_rows_publish_an_agent_and_a_whole_region_in_order() {
    let coord = coordinator(AgentsDisplay::default());
    let addr = serve(coord).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    for (token, id) in [
        ("wac_eu.secret", "eu-probe-1"),
        ("wac_us.secret", "us-probe-1"),
        ("wac_us.secret", "us-probe-2"),
    ] {
        assert_eq!(poll(&base, token, id, Some(10)).await, 200);
    }

    let (code, body) = put_display(
        &base,
        &session,
        display(
            "off",
            json!([agent_row("eu-probe-1"), region_row("us-east")]),
        ),
    )
    .await;
    assert_eq!(code, 200, "{body}");

    let rows = anonymous_status(&base).await;
    let listed: Vec<(&str, &str)> = rows["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["kind"].as_str().unwrap(), r["name"].as_str().unwrap()))
        .collect();
    assert_eq!(
        listed,
        [("agent", "eu-probe-1"), ("region", "us-east")],
        "placed order, and nothing the rows did not ask for: {rows}"
    );
    assert_eq!(rows["agents"][1]["total"], 2, "the whole region: {rows}");

    // The remainder can be switched back on without touching the placed rows.
    let (code, _) = put_display(
        &base,
        &session,
        display(
            "regions",
            json!([agent_row("eu-probe-1"), region_row("us-east")]),
        ),
    )
    .await;
    assert_eq!(code, 200);
    let rows = anonymous_status(&base).await;
    let names: Vec<&str> = rows["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["eu-probe-1", "us-east"],
        "eu-probe-1 was the only agent in eu-west, so no automatic row is left: {rows}"
    );
}

/// The coupling the design flagged: the silence window used to be a hardcoded
/// 90s that silently assumed a 10s poll, so an agent on a slower cadence read as
/// unreachable while it was polling happily — and its checks were handed to
/// nobody. Asserted on the recorded window rather than by waiting minutes.
#[tokio::test]
async fn the_window_is_sized_from_the_cadence_the_agent_declares() {
    let coord = coordinator(AgentsDisplay::default());
    let probe = coord.clone();
    let addr = serve(coord).await;
    let base = format!("http://{addr}");

    assert_eq!(
        poll(&base, "wac_eu.secret", "slow", Some(120)).await,
        200,
        "a slow poller is still a valid agent"
    );
    assert_eq!(
        probe.agent_window_ms_for_test("slow"),
        Some(360_000),
        "3 missed polls at 120s, not the old fixed 90s"
    );
    // An agent too old to declare a cadence keeps the window the coordinator
    // used to apply to everyone, so upgrading the coordinator ahead of the
    // fleet does not start evicting agents that changed nothing.
    assert_eq!(poll(&base, "wac_us.secret", "legacy", None).await, 200);
    assert_eq!(probe.agent_window_ms_for_test("legacy"), Some(90_000));
}

// ---- The admin surface. What the page shows is a runtime setting, not a
// config-file decision, so these assert the edit path end to end. ----

#[tokio::test]
async fn the_display_settings_are_admin_only() {
    let addr = serve(coordinator(AgentsDisplay::default())).await;
    let base = format!("http://{addr}");
    let anon = reqwest::Client::new();

    assert_eq!(
        anon.get(format!("{base}{DISPLAY}"))
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "reading the settings lists pinned agent ids"
    );
    assert_eq!(
        anon.put(format!("{base}{DISPLAY}"))
            .json(&display("flat", json!([])))
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "an anonymous caller must not choose what the public page publishes"
    );
}

/// The point of the whole change: an operator switches the mode and the public
/// page follows, with no restart and no config file edit.
#[tokio::test]
async fn saving_a_mode_changes_the_public_page_at_once() {
    let addr = serve(coordinator(AgentsDisplay::default())).await;
    let base = format!("http://{addr}");
    assert_eq!(
        poll(&base, "wac_eu.secret", "eu-probe-1", Some(10)).await,
        200
    );
    let session = login(&base).await;

    assert_eq!(
        anonymous_status(&base).await["agents"][0]["name"],
        "eu-west"
    );

    // Flat names the agent itself.
    let (code, body) = put_display(&base, &session, display("flat", json!([]))).await;
    assert_eq!(code, 200, "{body}");
    let rows = anonymous_status(&base).await;
    assert_eq!(rows["agents"][0]["name"], "eu-probe-1", "{rows}");

    // Off publishes nothing at all.
    let (code, body) = put_display(&base, &session, display("off", json!([]))).await;
    assert_eq!(code, 200, "{body}");
    let rows = anonymous_status(&base).await;
    assert!(
        rows["agents"].as_array().unwrap().is_empty(),
        "hidden means hidden: {rows}"
    );
}

#[tokio::test]
async fn the_picker_offers_the_fleet_it_can_pin() {
    let addr = serve(coordinator(AgentsDisplay::default())).await;
    let base = format!("http://{addr}");
    assert_eq!(
        poll(&base, "wac_eu.secret", "eu-probe-1", Some(10)).await,
        200
    );
    let session = login(&base).await;

    let body: Value = reqwest::Client::new()
        .get(format!("{base}{DISPLAY}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["fleet"][0]["agent_id"], "eu-probe-1", "{body}");
    assert_eq!(body["fleet"][0]["region"], "eu-west");
    assert_eq!(body["fleet"][0]["healthy"], true, "it just polled");
    assert_eq!(body["agents"]["mode"], "regions");
}

/// Repeated rows would be keyed the same, which hangs a Svelte `{#each}`
/// rather than looking wrong — so they are dropped before they can be stored,
/// not just before they are rendered.
#[tokio::test]
async fn duplicate_and_blank_rows_are_dropped() {
    let addr = serve(coordinator(AgentsDisplay::default())).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    let (code, body) = put_display(
        &base,
        &session,
        display(
            "regions",
            json!([
                agent_row("box"),
                agent_row(" box "),
                agent_row(""),
                region_row("  "),
                agent_row("other"),
            ]),
        ),
    )
    .await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(
        body["agents"]["rows"],
        json!([agent_row("box"), agent_row("other")])
    );
    assert!(
        body["agents"].get("pinned").is_none(),
        "the superseded field is not written back: {body}"
    );
}

#[tokio::test]
async fn thresholds_that_cannot_be_satisfied_are_refused() {
    let addr = serve(coordinator(AgentsDisplay::default())).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    // Red above amber leaves no band in which a region reads merely degraded.
    let inverted = serde_json::json!({
        "mode": "regions", "pinned": [], "warn_below": 40.0, "down_below": 90.0
    });
    let (code, body) = put_display(&base, &session, inverted).await;
    assert_eq!(code, 400, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("degraded"),
        "the reason has to be readable in the form: {body}"
    );

    let out_of_range = serde_json::json!({
        "mode": "regions", "pinned": [], "warn_below": 150.0, "down_below": 50.0
    });
    let (code, body) = put_display(&base, &session, out_of_range).await;
    assert_eq!(code, 400, "{body}");
    assert!(body["error"].as_str().unwrap().contains("0-100"), "{body}");

    // And nothing was published from a rejected save.
    let (_, current) = put_display(&base, &session, display("regions", json!([]))).await;
    assert_eq!(current["agents"]["warn_below"], 100.0);
}

/// The trap this whole shape exists to avoid: an edit that a restart silently
/// reverts because the config file is still treated as the authority.
#[tokio::test]
async fn a_saved_setting_outlives_a_restart_and_the_config_file_does_not_revert_it() {
    let dir = std::env::temp_dir().join(format!("warden-display-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("display.duckdb");
    let _ = std::fs::remove_file(&path);
    let db = path.to_str().unwrap().to_string();

    {
        let (addr, handle) = serve_abortable(coordinator_at(&db, AgentsDisplay::default())).await;
        let base = format!("http://{addr}");
        let session = login(&base).await;
        // A pinned id nobody has ever reported still gets a row, so the second
        // boot has something unambiguous to prove: an empty section would also
        // be what a reverted setting produced on an empty fleet.
        let (code, body) = put_display(
            &base,
            &session,
            display("regions", json!([agent_row("ghost-box")])),
        )
        .await;
        assert_eq!(code, 200, "{body}");
        handle.abort();
        let _ = handle.await;
    }

    // Same file, same config file — which still says "no pinned agents".
    let (addr, handle) = serve_abortable(reopen(&db)).await;
    let rows = anonymous_status(&format!("http://{addr}")).await;
    assert_eq!(
        rows["agents"][0]["name"], "ghost-box",
        "the store is the authority after first boot: {rows}"
    );
    handle.abort();
    let _ = std::fs::remove_file(&path);
}

/// Poll round-trip is reported by the agent on its next poll and surfaces on
/// the admin fleet list, joined in from the in-memory poll map. It is display
/// only — a made-up value moves nothing but its own row.
#[tokio::test]
async fn poll_rtt_surfaces_on_the_admin_agent_list() {
    let coord = coordinator(AgentsDisplay::default());
    let addr = serve(coord).await;
    let base = format!("http://{addr}");
    let session = login(&base).await;

    // The agent has to have reported at least once to appear in the list at
    // all — the list is derived from result rows, RTT is joined onto them.
    reqwest::Client::new()
        .post(format!("{base}/api/v1/results"))
        .bearer_auth("wac_eu.secret")
        .json(&serde_json::json!({"results": [{
            "check_id": cid("payments-api"),
            "agent_id": "rtt-agent",
            "timestamp": "2026-01-01T00:00:00Z",
            "latency_ms": 12,
            "status_code": 200,
        }]}))
        .send()
        .await
        .unwrap();

    async fn agents(base: &str, session: &str) -> Value {
        reqwest::Client::new()
            .get(format!("{base}/api/v1/agents"))
            .header(reqwest::header::COOKIE, session)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    }
    let find = |v: &Value| -> Value {
        v["agents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["agent_id"] == "rtt-agent")
            .cloned()
            .unwrap()
    };

    // Before any poll declares one, the field is present and null.
    let before = agents(&base, &session).await;
    assert!(
        find(&before)["poll_rtt_ms"].is_null(),
        "no poll yet means no RTT: {before}"
    );

    // A poll carrying rtt_ms is recorded and shows up.
    let code = reqwest::Client::new()
        .get(format!(
            "{base}/api/v1/agent/checks?agent_id=rtt-agent&poll_secs=10&rtt_ms=42"
        ))
        .bearer_auth("wac_eu.secret")
        .send()
        .await
        .unwrap()
        .status()
        .as_u16();
    assert_eq!(code, 200);

    let after = agents(&base, &session).await;
    assert_eq!(
        find(&after)["poll_rtt_ms"],
        42,
        "the RTT the agent reported should surface: {after}"
    );
}
