//! Admin API integration tests for incident lifecycle: open-on-down,
//! resolve-on-up, dependency grouping, flap containment, acknowledgement,
//! auto-resolve interaction with manual incidents, and deletion semantics.
//!
//! Split from the original `admin_api.rs`; see `tests/common/mod.rs` for the
//! shared fixtures (including `coordinator_with`, `http_check`, `probe`,
//! `create_incident`, and `public_incident_ids`, used only here and by the
//! `erasing_a_check_removes_everything_it_produced` test in
//! `admin_checks.rs`).

mod common;

use serde_json::{json, Value};

use common::*;
use lighthouse::coordinator::Coordinator;
use lighthouse::models::{CheckId, NodeState};

/// Pausing an unhealthy monitor ends its machine-owned incident without
/// inventing a recovery; an operator's incident remains their responsibility.
#[tokio::test]
async fn pausing_down_check_closes_only_automatic_incident_without_claiming_recovery() {
    use lighthouse::coordinator::storage::IncidentState;

    let coord = coordinator_with(vec![http_check("db")]).await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let manual_id =
        create_incident(&base, &session, "Planned work", &[cid("db").to_string()]).await;
    let now = chrono::Utc::now().timestamp_millis();
    coord.ingest(vec![probe("db", now, 500)]).await;
    let automatic = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.origin_check == Some(cid("db")))
        .expect("DOWN opened incident");
    assert!(automatic.resolved_at.is_none());

    let mut body = check_body("db", "http://db.test/");
    body["enabled"] = json!(false);
    let response = reqwest::Client::new()
        .put(format!("{base}/api/v1/admin/checks/{}", cid("db")))
        .header(reqwest::header::COOKIE, &session)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );

    let after = coord.incidents_for_test(100).await;
    let automatic_after = after.iter().find(|i| i.id == automatic.id).unwrap();
    assert_eq!(automatic_after.state, IncidentState::Resolved);
    assert!(automatic_after.resolved_at.is_some());
    let updates = coord.updates_for_test(&automatic.id).await;
    assert_eq!(updates.len(), 2, "preserve detection and explain closure");
    assert!(updates[1].body.to_lowercase().contains("monitoring paused"));
    assert!(!updates[1].body.to_lowercase().contains("recover"));
    let manual = after.iter().find(|i| i.id == manual_id).unwrap();
    assert!(manual.resolved_at.is_none(), "manual incident stays open");
}

/// The incident lifecycle is driven by observations, not transitions: the open
/// incident is the source of truth. A check whose first-ever observation is
/// DOWN still opens an incident (the outage is real even though the alerts feed
/// treats that first observation as a baseline), further DOWN observations ride
/// the same open incident, UNKNOWN does not confirm a recovery, and — at the
/// default `failures_before_down` of 1 — the first UP observation resolves it.
///
/// Also pins the shape of an auto-opened incident: it starts at `Detected`, not
/// `Investigating`, because no human has looked at it yet.
#[tokio::test]
async fn incidents_open_on_down_and_resolve_on_up() {
    use lighthouse::coordinator::storage::IncidentState;
    let coord = coordinator_with(vec![http_check("api")]).await;
    let now = chrono::Utc::now().timestamp_millis();

    // `status_code: None` makes the default eval read the probe as Unknown;
    // `Some(code)` drives Up (2xx-3xx) or Down otherwise.
    let result = |ts: i64, status_code: Option<u16>, error: Option<String>| {
        lighthouse::models::CheckResult {
            check_id: cid("api"),
            agent_id: "agent-1".into(),
            zone: "test-zone".into(),
            region: "test-region".into(),
            kind: lighthouse::models::CheckKind::Http,
            timestamp: chrono::DateTime::from_timestamp_millis(ts).unwrap(),
            latency_ms: 1,
            status_code,
            body_matched: None,
            error,
            failure: None,
            body: None,
            headers: None,
            metric: None,
            metrics: None,
        }
    };

    // The demo seed opens its own incidents, so every assertion below counts
    // only this check's — a global count would be measuring the fixture.
    async fn mine(c: &Coordinator) -> Vec<lighthouse::coordinator::storage::IncidentRow> {
        c.incidents_for_test(100)
            .await
            .into_iter()
            .filter(|i| i.origin_check == Some(cid("api")))
            .collect()
    }

    // First-ever observation is DOWN. No alert (baseline is silent), but the
    // outage is real: an incident must open.
    coord
        .ingest(vec![result(
            now,
            Some(500),
            Some("connection refused".into()),
        )])
        .await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "first-ever DOWN opens an incident");
    assert_eq!(
        incidents[0].state,
        IncidentState::Detected,
        "a probe noticing is not a human investigating"
    );
    assert_eq!(
        incidents[0].origin_check,
        Some(cid("api")),
        "the check that fell is the origin"
    );
    assert_eq!(
        incidents[0]
            .checks
            .iter()
            .map(|c| (c.check_id, c.source))
            .collect::<Vec<_>>(),
        vec![(
            cid("api"),
            lighthouse::coordinator::storage::AffectSource::Origin
        )],
        "the origin is attached as an affected check"
    );
    // The cause is the first entry in the narrative, not a column beside it.
    let updates = coord.updates_for_test(&incidents[0].id).await;
    assert_eq!(updates.len(), 1, "opening writes exactly one update");
    assert_eq!(updates[0].body, "connection refused");
    assert_eq!(updates[0].state, IncidentState::Detected);
    assert!(
        updates[0].author.is_none(),
        "the coordinator wrote it, so it is not an acknowledgement"
    );
    assert!(
        coord
            .alerts_for_test(100)
            .await
            .into_iter()
            .filter(|a| a.node_id == cid("api").to_string())
            .collect::<Vec<_>>()
            .is_empty(),
        "baseline still does not alert"
    );

    // Further DOWN observations while it is open ride on the same incident.
    coord
        .ingest(vec![result(now + 5_000, Some(500), None)])
        .await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "no second incident while one is open");

    // UNKNOWN is not a confirmed recovery, so it must not close the incident.
    coord.ingest(vec![result(now + 10_000, None, None)]).await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "UNKNOWN keeps the incident open");
    assert_eq!(incidents[0].state, IncidentState::Detected);

    // Recovery. `failures_before_down` defaults to 1, so one UP is one full
    // up-side run and the incident resolves immediately.
    coord
        .ingest(vec![result(now + 20_000, Some(200), None)])
        .await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "same incident, now resolved");
    assert_eq!(incidents[0].state, IncidentState::Resolved);
    assert_eq!(incidents[0].resolved_at, Some(now + 20_000));
    // Resolving writes its own entry, so the timeline explains the state.
    let updates = coord.updates_for_test(&incidents[0].id).await;
    assert_eq!(updates.len(), 2, "open and resolve each wrote an update");
    assert_eq!(updates[1].state, IncidentState::Resolved);
    assert!(
        updates[1].body.contains("Total downtime"),
        "the resolve entry states the downtime, got {:?}",
        updates[1].body
    );

    // The recovery alert is stamped with the incident it ended, so the evidence
    // stays attached to the outage rather than floating loose.
    let recovery = coord
        .alerts_for_test(100)
        .await
        .into_iter()
        .find(|a| a.node_id == cid("api").to_string() && a.current_state == NodeState::Up)
        .expect("recovery alert recorded");
    assert_eq!(
        recovery.incident_id.as_deref(),
        Some(incidents[0].id.as_str()),
        "the recovery transition names the incident it closed"
    );

    // And the next outage is a fresh incident.
    coord
        .ingest(vec![result(now + 30_000, Some(500), None)])
        .await;
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 2, "a new outage opens a second incident");
    assert_eq!(
        incidents[0].state,
        IncidentState::Detected,
        "the newest is the open one"
    );
}

/// A check that declares `depends_on` joins its upstream's open incident
/// instead of opening its own: one outage, one entry on the status page, rather
/// than a row per symptom.
///
/// The dependent still records its own alert — grouping is not suppression —
/// and the attachment is marked `dependency` so an operator can see the machine
/// put it there.
#[tokio::test]
async fn a_dependent_check_joins_its_upstreams_incident() {
    use lighthouse::coordinator::storage::AffectSource;
    let mut api = http_check("api");
    api.depends_on = vec![cid("db")];
    let coord = coordinator_with(vec![http_check("db"), api]).await;
    let now = chrono::Utc::now().timestamp_millis();

    // A baseline UP for the dependent: a first-ever observation never alerts,
    // so without this the "grouping is not suppression" assertion below would
    // pass or fail for the wrong reason.
    coord.ingest(vec![probe("api", now - 1_000, 200)]).await;

    // The upstream falls first and opens the incident.
    coord.ingest(vec![probe("db", now, 500)]).await;
    let opened: Vec<_> = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .filter(|i| i.origin_check == Some(cid("db")))
        .collect();
    assert_eq!(opened.len(), 1, "the upstream opened one incident");
    let incident_id = opened[0].id.clone();

    // The dependent falls next. It must NOT open a second incident.
    coord.ingest(vec![probe("api", now + 1_000, 500)]).await;
    assert!(
        coord
            .incidents_for_test(100)
            .await
            .iter()
            .all(|i| i.origin_check != Some(cid("api"))),
        "a dependent must not open its own incident while its upstream is down"
    );

    let inc = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == incident_id)
        .expect("the upstream incident is still there");
    assert_eq!(
        inc.checks
            .iter()
            .map(|c| (c.check_id, c.source))
            .collect::<Vec<_>>(),
        vec![
            (cid("api"), AffectSource::Dependency),
            (cid("db"), AffectSource::Origin),
        ],
        "the dependent joined the upstream's incident, marked as automatic"
    );

    // Grouping is not suppression: the dependent's own transition is still
    // recorded, and stamped with the incident it belongs to.
    let api_alert = coord
        .alerts_for_test(100)
        .await
        .into_iter()
        .find(|a| a.node_id == cid("api").to_string())
        .expect("the dependent still records its own alert");
    assert_eq!(api_alert.incident_id.as_deref(), Some(incident_id.as_str()));
}

/// Without a declared dependency the same two outages stay separate. This is
/// the counterpart to the test above: it is what stops the grouping rule from
/// quietly merging unrelated failures that happen to coincide, which is exactly
/// what a time-window correlation would have done.
#[tokio::test]
async fn unrelated_checks_falling_together_stay_separate_incidents() {
    let coord = coordinator_with(vec![http_check("db"), http_check("ping")]).await;
    let now = chrono::Utc::now().timestamp_millis();

    coord
        .ingest(vec![probe("db", now, 500), probe("ping", now, 500)])
        .await;

    let origins: Vec<String> = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .filter_map(|i| i.origin_check)
        .filter(|c| *c == cid("db") || *c == cid("ping"))
        .map(|c| c.to_string())
        .collect();
    assert_eq!(
        origins.len(),
        2,
        "two unrelated outages are two incidents, got {origins:?}"
    );
}

/// Auto-resolve waits out the flapping. With `failures_before_down = 3` a check
/// needs three consecutive UP observations before the incident closes, so a
/// check bouncing DOWN/UP stays inside ONE incident instead of opening and
/// closing a new one on every edge.
#[tokio::test]
async fn a_flapping_check_stays_inside_one_incident() {
    use lighthouse::coordinator::storage::IncidentState;
    let mut flappy = http_check("flappy");
    flappy.failures_before_down = 3;
    let coord = coordinator_with(vec![flappy]).await;
    let now = chrono::Utc::now().timestamp_millis();
    async fn mine(c: &Coordinator) -> Vec<lighthouse::coordinator::storage::IncidentRow> {
        c.incidents_for_test(100)
            .await
            .into_iter()
            .filter(|i| i.origin_check == Some(cid("flappy")))
            .collect()
    }

    // Three failures confirm the DOWN and open the incident.
    for i in 0..3 {
        coord
            .ingest(vec![probe("flappy", now + i * 1_000, 500)])
            .await;
    }
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "confirmed DOWN opened one incident");
    let id = incidents[0].id.clone();

    // Flap: UP, DOWN, UP, DOWN. No single UP is a full run of three, so the
    // incident must never close and never be superseded by a second one.
    for (i, code) in [200u16, 500, 200, 500].into_iter().enumerate() {
        coord
            .ingest(vec![probe("flappy", now + 10_000 + i as i64 * 1_000, code)])
            .await;
        let incidents = mine(&coord).await;
        assert_eq!(
            incidents.len(),
            1,
            "flapping must not spray incidents; saw {} after code {code}",
            incidents.len()
        );
        assert_eq!(
            incidents[0].id, id,
            "still the same incident, not a replacement"
        );
    }

    // The two earlier UP observations remain part of the recovery confirmation
    // run; a DOWN that is already confirmed does not reset it. Two more UPs
    // therefore complete the required three successes and close the incident.
    for i in 0..2 {
        coord
            .ingest(vec![probe("flappy", now + 20_000 + i * 1_000, 200)])
            .await;
    }
    let incidents = mine(&coord).await;
    assert_eq!(incidents.len(), 1, "still one incident");
    assert_eq!(
        incidents[0].state,
        IncidentState::Resolved,
        "the accumulated recovery run resolves it"
    );
}

/// Auto-resolve fires whether or not a human has taken the incident over: an
/// operator mid-narrative does not block the machine from recording that the
/// checks came back.
#[tokio::test]
async fn auto_resolve_fires_even_after_a_human_has_acknowledged() {
    use lighthouse::coordinator::storage::IncidentState;
    let coord = coordinator_with(vec![http_check("svc")]).await;
    let now = chrono::Utc::now().timestamp_millis();
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;

    coord.ingest(vec![probe("svc", now, 500)]).await;
    let id = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.origin_check == Some(cid("svc")))
        .expect("incident opened")
        .id;

    // Acknowledge by posting an update — there is no acknowledge endpoint.
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/incidents/{id}/updates"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "state": "investigating", "body": "looking" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let acked = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == id)
        .unwrap();
    assert_eq!(acked.state, IncidentState::Investigating);

    coord.ingest(vec![probe("svc", now + 5_000, 200)]).await;
    let resolved = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == id)
        .unwrap();
    assert_eq!(
        resolved.state,
        IncidentState::Resolved,
        "recovery resolves it even though a human had taken it over"
    );
}

/// Writing to an incident requires a session. The narrative is published on the
/// public status page, so an open update endpoint would let anyone post text to
/// it — and an open create endpoint would let anyone invent an outage.
#[tokio::test]
async fn incident_writes_are_admin_only_while_reads_are_public() {
    let coord = coordinator_with(vec![http_check("svc")]).await;
    let now = chrono::Utc::now().timestamp_millis();
    coord.ingest(vec![probe("svc", now, 500)]).await;
    let id = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.origin_check == Some(cid("svc")))
        .expect("incident opened")
        .id;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let anon = reqwest::Client::new();

    for (method, url, body) in [
        (
            reqwest::Method::POST,
            format!("{base}/api/v1/admin/incidents"),
            json!({ "title": "invented" }),
        ),
        (
            reqwest::Method::PATCH,
            format!("{base}/api/v1/admin/incidents/{id}"),
            json!({ "title": "hijacked", "started_at": now, "global": true }),
        ),
        (
            reqwest::Method::PUT,
            format!("{base}/api/v1/admin/incidents/{id}/checks"),
            json!({ "checks": [cid("svc").to_string()] }),
        ),
        (
            reqwest::Method::POST,
            format!("{base}/api/v1/admin/incidents/{id}/updates"),
            json!({ "state": "resolved", "body": "nothing to see here" }),
        ),
    ] {
        let res = anon
            .request(method.clone(), &url)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{method} {url} must require a session"
        );
    }

    // Nothing above took effect.
    let after = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == id)
        .unwrap();
    assert_eq!(after.title, "svc is down", "the title was not rewritten");
    assert!(!after.global, "it was not promoted to a global incident");
    assert!(
        !after.state.is_resolved(),
        "an anonymous caller could not close the outage"
    );

    // Reads stay public: the permalink is what the status page links to.
    let res = anon
        .get(format!("{base}/api/v1/incidents/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["incident"]["id"], id);
    assert!(
        !body["incident"]["updates"].as_array().unwrap().is_empty(),
        "the permalink carries the narrative"
    );
}

/// A manually created incident is the operator's, and the reconciler must not
/// touch it. It has no `origin_check`, so there is nothing for the machine to
/// follow — a maintenance notice is over when a human says so.
///
/// This is a regression test. Both halves shipped broken and were caught by a
/// live smoke test, not by the suite:
///   1. `open_incident_for` returned manual incidents, so a passing check
///      pulled one into the resolve path at all.
///   2. `try_resolve` iterated the gating checks and fell through on an empty
///      set, so an incident whose attachments are all `manual` resolved
///      vacuously — the loop had nothing to refuse.
#[tokio::test]
async fn a_manual_incident_is_never_auto_resolved() {
    use lighthouse::coordinator::storage::IncidentState;
    let coord = coordinator_with(vec![http_check("db")]).await;
    let now = chrono::Utc::now().timestamp_millis();
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;

    // A maintenance notice naming a check that is perfectly healthy.
    let res = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/incidents"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({
            "title": "Scheduled database maintenance",
            "state": "monitoring",
            "global": true,
            "checks": [cid("db").to_string()],
            "body": "Planned failover window.",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let created: Value = res.json().await.unwrap();
    let id = created["incident"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        created["incident"]["origin_check"],
        Value::Null,
        "a manual incident has no origin for the reconciler to follow"
    );

    // The named check keeps passing. That must change nothing.
    coord
        .ingest(vec![probe("db", now, 200), probe("db", now + 1_000, 200)])
        .await;
    let after = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.id == id)
        .expect("the manual incident still exists");
    assert_eq!(
        after.state,
        IncidentState::Monitoring,
        "a healthy check must not close an operator's incident"
    );
    assert_eq!(after.resolved_at, None);

    // And the named check failing must open its OWN incident rather than
    // riding the manual one, which would give the outage no origin.
    coord.ingest(vec![probe("db", now + 2_000, 500)]).await;
    let own: Vec<_> = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .filter(|i| i.origin_check == Some(cid("db")))
        .collect();
    assert_eq!(
        own.len(),
        1,
        "the failure opened a real incident of its own, not a ride on the notice"
    );
    assert_ne!(own[0].id, id);
}

/// The lifecycle an operator drives: `detected` is the coordinator's word for
/// "nobody has looked yet", so a human can never post it; `resolved` ends the
/// incident, so resolving it again is refused rather than appended as a second
/// ending; and a reopen is a non-resolved update, which clears the resolution.
#[tokio::test]
async fn an_operator_cannot_resolve_twice_or_post_detected() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let client = reqwest::Client::new();
    let id = create_incident(&base, &session, "Planned work", &[]).await;
    let post = |state: &'static str| {
        client
            .post(format!("{base}/api/v1/admin/incidents/{id}/updates"))
            .header(reqwest::header::COOKIE, &session)
            .json(&json!({ "state": state, "body": "note" }))
            .send()
    };

    assert_eq!(post("detected").await.unwrap().status(), 400);
    assert_eq!(post("resolved").await.unwrap().status(), 200);
    let again = post("resolved").await.unwrap();
    assert_eq!(again.status(), 409, "an ended incident cannot end again");

    let detail: Value = client
        .get(format!("{base}/api/v1/incidents/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let states: Vec<&str> = detail["incident"]["updates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["state"].as_str().unwrap())
        .collect();
    assert_eq!(states.iter().filter(|s| **s == "resolved").count(), 1);

    let reopened: Value = post("investigating").await.unwrap().json().await.unwrap();
    assert_eq!(reopened["incident"]["state"], "investigating");
    assert_eq!(reopened["incident"]["resolved_at"], Value::Null);
}

/// An automatic resolve is stamped with the probe's timestamp, which an agent
/// takes before the batch reaches the coordinator. An operator update landing
/// in that gap would otherwise sort *after* the resolve, and the timeline would
/// end on "investigating" for an incident that is resolved.
#[tokio::test]
async fn an_automatic_resolve_never_sorts_before_the_update_it_follows() {
    let coord = coordinator_with(vec![http_check("db")]).await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let session = login(&base).await;
    let now = chrono::Utc::now().timestamp_millis();

    coord.ingest(vec![probe("db", now - 60_000, 500)]).await;
    let id = coord
        .incidents_for_test(10)
        .await
        .into_iter()
        .find(|i| i.origin_check == Some(cid("db")))
        .expect("the failure opened an incident")
        .id;
    let posted = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/incidents/{id}/updates"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "state": "investigating", "body": "looking" }))
        .send()
        .await
        .unwrap();
    assert_eq!(posted.status(), 200);

    // Probed before the operator's update was written, delivered after.
    coord.ingest(vec![probe("db", now - 30_000, 200)]).await;

    let detail: Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/incidents/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["incident"]["state"], "resolved");
    let updates = detail["incident"]["updates"].as_array().unwrap();
    assert_eq!(
        updates.last().unwrap()["state"],
        "resolved",
        "the timeline ends where the incident does: {updates:?}"
    );
}

/// Acknowledging is posting an empty-bodied `investigating` update, and the
/// console offers it on every render of a `detected` row. A double click, a
/// stale list, or a relay that answers from a lagging region therefore posts it
/// twice — and the second one recorded another silent row against the public
/// narrative, forever, one per click. The first ack is the acknowledgement; an
/// empty update that moves nothing afterwards must record nothing.
#[tokio::test]
async fn acknowledging_twice_records_one_update() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;
    let id = create_incident(&base, &session, "svc is down", &[]).await;

    let ack = || async {
        admin
            .post(format!("{base}/api/v1/admin/incidents/{id}/updates"))
            .header(reqwest::header::COOKIE, session.clone())
            .json(&json!({ "state": "investigating", "body": "" }))
            .send()
            .await
            .unwrap()
    };

    assert_eq!(ack().await.status(), reqwest::StatusCode::OK);
    let after_first: Value = reqwest::get(format!("{base}/api/v1/incidents/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let first = after_first["incident"]["updates"].as_array().unwrap().len();
    assert_eq!(after_first["incident"]["state"], "investigating");
    assert_eq!(after_first["incident"]["acknowledged"], true);

    // Three more clicks. Each is accepted — the operator did nothing wrong —
    // but none of them may leave a trace.
    for _ in 0..3 {
        assert_eq!(ack().await.status(), reqwest::StatusCode::OK);
    }
    let after_repeat: Value = reqwest::get(format!("{base}/api/v1/incidents/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        after_repeat["incident"]["updates"]
            .as_array()
            .unwrap()
            .len(),
        first,
        "a repeated acknowledgement must not append another empty update"
    );
    assert_eq!(after_repeat["incident"]["acknowledged"], true);

    // A real update still lands: the guard is about empty no-ops, not about
    // refusing to let an operator narrate an incident they already acked.
    let res = admin
        .post(format!("{base}/api/v1/admin/incidents/{id}/updates"))
        .header(reqwest::header::COOKIE, session.clone())
        .json(&json!({ "state": "investigating", "body": "nginx upstream missing" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let after_body: Value = reqwest::get(format!("{base}/api/v1/incidents/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        after_body["incident"]["updates"].as_array().unwrap().len(),
        first + 1,
        "an authored update is not a no-op even at the same state"
    );
}

#[tokio::test]
async fn deleting_an_incident_needs_a_session_and_a_real_id() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;
    let id = create_incident(&base, &session, "Router flap", &[]).await;

    // An anonymous caller must not be able to erase what visitors are reading.
    let anon = reqwest::Client::new()
        .delete(format!("{base}/api/v1/admin/incidents/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), 401);
    assert!(
        public_incident_ids(&base).await.contains(&id),
        "a rejected delete must not have deleted anything"
    );

    let unknown = admin
        .delete(format!("{base}/api/v1/admin/incidents/never-existed"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(
        unknown.status(),
        404,
        "an id nobody opened is not a success"
    );

    let res = admin
        .delete(format!("{base}/api/v1/admin/incidents/{id}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.json::<Value>().await.unwrap()["deleted"], id);
    assert!(
        !public_incident_ids(&base).await.contains(&id),
        "the incident is gone from the public list"
    );

    // A second delete of the same id is a 404, not a silent success: the
    // tombstone remembers, but there is nothing left to remove.
    let again = admin
        .delete(format!("{base}/api/v1/admin/incidents/{id}"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 404);
}

/// The alert log is the raw record that a check flipped. Deleting the narrative
/// written around it must not erase the fact, so the alert survives with a null
/// `incident_id` rather than going with the incident.
#[tokio::test]
async fn deleting_an_incident_unlinks_its_alerts_instead_of_deleting_them() {
    let coord = coordinator().await;
    let addr = spawn_server(coord.clone());
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let web: Value = admin
        .post(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .json(&check_body("web", "https://web.test"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let web_id: CheckId = web["check"]["id"].as_str().unwrap().parse().unwrap();
    // An UP first, so the DOWN below is a *transition*: a check's first-ever
    // observation opens an incident but writes no alert, and this test is
    // about what happens to the alert.
    for (ts, code) in [("2026-01-01T00:00:00Z", 200), ("2026-01-01T00:01:00Z", 500)] {
        reqwest::Client::new()
            .post(format!("{base}/api/v1/results"))
            .bearer_auth(AGENT_TOKEN)
            .json(&json!({"results": [{
            "check_id": web_id,
                "agent_id": "agent-test",
                "timestamp": ts,
                "latency_ms": 3,
                "status_code": code,
            }]}))
            .send()
            .await
            .unwrap();
    }

    let opened = coord
        .incidents_for_test(100)
        .await
        .into_iter()
        .find(|i| i.origin_check == Some(web_id))
        .expect("a confirmed DOWN opens an incident");

    admin
        .delete(format!("{base}/api/v1/admin/incidents/{}", opened.id))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();

    let alerts: Value = reqwest::get(format!("{base}/api/v1/alerts?limit=50"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mine: Vec<&Value> = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["node_id"] == web_id.to_string())
        .collect();
    assert_eq!(mine.len(), 1, "the transition is still on the record");
    assert!(
        mine[0]["incident_id"].is_null(),
        "the alert should be unlinked, not deleted: {:?}",
        mine[0]
    );
}
