use super::*;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use fleet::{fleet_rows, FleetMemberDto, FleetRowDto};
use routes::agents::{
    agent_checks_handler, agents_handler, ingest_handler, list_agent_credentials_handler,
    mint_agent_credential_handler, purge_agent_handler, revoke_agent_credential_handler,
};
use routes::checks::{
    create_check_handler, delete_check_handler, eval_test_handler, heartbeat_handler,
    list_checks_handler, purge_check_data_handler, purge_check_everything_handler,
    reorder_checks_handler, unplace_check_handler, update_check_handler,
};
use routes::incidents::{
    check_incidents_handler, create_incident_handler, delete_incident_handler,
    edit_incident_handler, incident_checks_handler, incident_handler, incident_update_handler,
    incidents_handler, purge_results_handler, purge_short_incidents_handler, results_count_handler,
};
use routes::peer::{
    alerts_backfill_handler, backfill_handler, cluster_ping_handler, cluster_view_handler,
    config_entries_handler, config_handler, digest_handler, global_check_ids_handler,
    global_config_digest_handler, global_config_handler, gossip_handler,
    incident_tombstones_backfill_handler, incidents_backfill_handler,
    latency_hour_backfill_handler, latency_minute_backfill_handler, raw_data_gaps_handler,
    replicate_handler, results_backfill_handler,
};
use routes::policies::{
    delete_rollup_policy_handler, list_rollup_policies_handler, region_tree_handler,
    upsert_rollup_policy_handler,
};
use routes::public::{
    alerts_handler, history_handler, latency_points_handler, latency_sparkline_handler,
    results_handler, status_handler, uptime_handler, uptime_strips_handler,
};
use routes::sections::{
    create_section_handler, delete_section_handler, list_sections_handler, public_sections_handler,
    rename_section_handler, reorder_sections_handler,
};
use routes::session::{login_handler, logout_handler, session_handler};
use routes::settings::{
    agents_display_handler, maintenance_handler, notifiers_handler, test_notifier_handler,
    update_agents_display_handler, update_maintenance_handler, update_notifiers_handler,
};
use tokio::sync::broadcast;

/// Fleet liveness for the public status page.
pub(in crate::coordinator) async fn fleet_health(
    coord: &Coordinator,
    now: i64,
) -> Vec<FleetRowDto> {
    let cfg = coord.inner.status_agents.read().clone();
    // Nothing placed by hand and nothing automatic: the section is off, and
    // the aggregate query can be skipped entirely. This rides the busiest
    // public endpoint there is.
    if cfg.mode == crate::config::AgentsMode::Off && cfg.rows.is_empty() {
        return Vec::new();
    }
    // One lock for both reads: they are two halves of the same answer, and
    // taking it twice on the busiest public endpoint buys nothing.
    let (reported, polls) = coord
        .db(move |storage| Ok((storage.list_agents(now)?, storage.list_agent_polls()?)))
        .await
        .unwrap_or_default();
    fleet_rows(now, &coord.inner.seen.read(), &polls, &reported, &cfg)
}

/// Everyone the coordinator knows about, live pollers and past reporters alike,
/// sorted so the picker does not reshuffle between loads.
pub(in crate::coordinator) async fn fleet_members(
    coord: &Coordinator,
    now: i64,
) -> Vec<FleetMemberDto> {
    let mut members: std::collections::HashMap<String, (String, bool)> = coord
        .inner
        .seen
        .read()
        .iter()
        .map(|(id, s)| (id.clone(), (s.region.clone(), now - s.at <= s.window_ms)))
        .collect();
    let (reported, polls) = coord
        .db(move |storage| Ok((storage.list_agents(now)?, storage.list_agent_polls()?)))
        .await
        .unwrap_or_default();
    // Merged the same way the status page merges, so pinning a row on one node
    // offers the same fleet as pinning it on another.
    for p in polls {
        let healthy = now - p.at <= p.window_ms;
        members
            .entry(p.agent_id)
            .and_modify(|e| e.1 = e.1 || healthy)
            .or_insert((p.region, healthy));
    }
    for r in reported {
        members.entry(r.agent_id).or_insert((r.region, false));
    }
    let mut fleet: Vec<FleetMemberDto> = members
        .into_iter()
        .map(|(agent_id, (region, healthy))| FleetMemberDto {
            agent_id,
            region,
            healthy,
        })
        .collect();
    fleet.sort_by(|a, b| (&a.region, &a.agent_id).cmp(&(&b.region, &b.agent_id)));
    fleet
}

async fn ws_handler(State(coord): State<Coordinator>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| handle_ws(socket, coord))
}

async fn handle_ws(mut socket: WebSocket, coord: Coordinator) {
    let mut rx = coord.inner.events.subscribe();
    loop {
        match rx.recv().await {
            Ok(ev) => {
                let txt = match serde_json::to_string(&ev) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                if socket.send(Message::Text(txt)).await.is_err() {
                    break; // client gone
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

pub(in crate::coordinator) fn internal_error(e: anyhow::Error) -> Response {
    tracing::error!(error = %e, "request failed");
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "internal error",
    )
        .into_response()
}

// ---- Embedded dashboard (built Svelte SPA under frontend/dist) ----

#[derive(rust_embed::RustEmbed)]
#[folder = "frontend/dist"]
struct Assets;

/// Serve the embedded SPA. Unknown non-API paths fall back to index.html so
/// client-side routing works. Unmatched /api paths return 404 (not the SPA).
async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    if path.starts_with("api/") {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }

    let lookup = if path.is_empty() { "index.html" } else { path };
    if let Some(content) = Assets::get(lookup) {
        let mime = mime_guess::from_path(lookup).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response();
    }

    // SPA fallback / unbuilt-frontend placeholder.
    match Assets::get("index.html") {
        Some(content) => Html(content.data).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "dashboard not built — run `bun run build` in frontend/",
        )
            .into_response(),
    }
}

// ---- Auth and the check editor ----

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

pub(in crate::coordinator) fn error_response(code: StatusCode, message: &str) -> Response {
    (
        code,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
        .into_response()
}

#[derive(Serialize)]
pub(in crate::coordinator) struct DeletedResponse {
    pub(in crate::coordinator) deleted: String,
}

impl Coordinator {
    pub fn router(self) -> Router {
        Router::new()
            .route(
                "/api/v1/results",
                post(ingest_handler).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
            )
            .route(
                "/metrics",
                get(|State(coord): State<Coordinator>| async move {
                    (
                        [(
                            header::CONTENT_TYPE,
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )],
                        crate::openmetrics::render(&coord.metrics()),
                    )
                }),
            )
            .route("/api/v1/status", get(status_handler))
            .route("/api/v1/checks/:id/history", get(history_handler))
            .route("/api/v1/checks/:id/uptime", get(uptime_handler))
            .route(
                "/api/v1/checks/:id/latency-sparkline",
                get(latency_sparkline_handler),
            )
            // Batched twins of the two routes above. The status page draws a
            // strip and a sparkline per check, so fetching them per check cost
            // one request and one database job each — 1 + N + M for a page load.
            .route("/api/v1/uptime", get(uptime_strips_handler))
            .route("/api/v1/latency-points", get(latency_points_handler))
            .route("/api/v1/alerts", get(alerts_handler))
            // Public reads: the list and the per-incident permalink. The
            // narrative is the point of writing it, so an anonymous visitor
            // gets the same record the operator sees.
            .route("/api/v1/incidents", get(incidents_handler))
            .route("/api/v1/incidents/:id", get(incident_handler))
            .route("/api/v1/checks/:id/incidents", get(check_incidents_handler))
            // Writes are admin-gated inside each handler, like the check
            // editor. There is no acknowledge route: an authored update on a
            // `detected` incident is the acknowledgement.
            .route("/api/v1/admin/incidents", post(create_incident_handler))
            .route(
                "/api/v1/admin/incidents/purge-short",
                post(purge_short_incidents_handler),
            )
            .route(
                "/api/v1/admin/incidents/:id",
                axum::routing::patch(edit_incident_handler).delete(delete_incident_handler),
            )
            .route(
                "/api/v1/admin/incidents/:id/checks",
                axum::routing::put(incident_checks_handler),
            )
            .route(
                "/api/v1/admin/incidents/:id/updates",
                post(incident_update_handler),
            )
            .route("/api/v1/agents", get(agents_handler))
            // The raw probe-results explorer. Admin-only inside the handler:
            // it returns un-redacted errors that quote resolved addresses and
            // expected values, unlike the public per-check history endpoint.
            .route("/api/v1/admin/results", get(results_handler))
            .route("/api/v1/admin/results/count", get(results_count_handler))
            .route("/api/v1/admin/results/purge", post(purge_results_handler))
            .route(
                "/api/v1/admin/rollup-policies",
                get(list_rollup_policies_handler)
                    .put(upsert_rollup_policy_handler)
                    .delete(delete_rollup_policy_handler),
            )
            .route("/api/v1/admin/region-tree", get(region_tree_handler))
            .route("/api/v1/admin/agents/:id", delete(purge_agent_handler))
            // Section names and their display flags. Public because the status
            // page needs them to render groups; it carries no targets.
            .route("/api/v1/sections", get(public_sections_handler))
            .route("/api/v1/ws", get(ws_handler))
            // Auth. `session` is deliberately unauthenticated and never 401s —
            // it is how the page decides whether to offer the admin UI at all.
            .route(
                "/api/v1/login",
                post(login_handler).layer(DefaultBodyLimit::max(16 * 1024)),
            )
            .route("/api/v1/logout", post(logout_handler))
            .route("/api/v1/session", get(session_handler))
            // Check editor. Every route here requires an admin session.
            // Deadman switch: something external pings this, and the check goes
            // DOWN when the pings stop. Unauthenticated by design — the URL is
            // the credential, and it is only ever shown to an admin.
            .route(
                "/api/v1/heartbeat/:id",
                get(heartbeat_handler).post(heartbeat_handler),
            )
            .route(
                "/api/v1/admin/checks/:id/data",
                delete(purge_check_data_handler),
            )
            // The whole erase: the check AND everything it ever produced.
            // Ordered before the `:id` route for the same reason
            // `checks/order` is — a suffix must never be read as a check id.
            .route(
                "/api/v1/admin/checks/:id/everything",
                delete(purge_check_everything_handler),
            )
            // Unplacing is regional: it removes only this coordinator's
            // definition and derived state, never a replicated config tombstone.
            .route(
                "/api/v1/admin/checks/:id/unplace",
                delete(unplace_check_handler),
            )
            // Ordering is its own route, ahead of the `:id` route so "order"
            // is never taken for a check id. Reordering is a bulk operation
            // over ids, not an edit to any one check.
            .route("/api/v1/admin/checks/order", put(reorder_checks_handler))
            .route(
                "/api/v1/admin/checks",
                get(list_checks_handler).post(create_check_handler),
            )
            .route(
                "/api/v1/admin/checks/:id",
                put(update_check_handler).delete(delete_check_handler),
            )
            // Status-page presentation: sections exist independently of checks,
            // so they get their own routes rather than riding on a check edit.
            .route(
                "/api/v1/admin/sections",
                get(list_sections_handler)
                    .post(create_section_handler)
                    .put(reorder_sections_handler),
            )
            .route(
                "/api/v1/admin/sections/:name",
                put(rename_section_handler).delete(delete_section_handler),
            )
            // Which agents the public page names, and how. Admin-only in both
            // directions: `pinned` is a list of agent ids.
            .route(
                "/api/v1/admin/status-page/agents",
                get(agents_display_handler).put(update_agents_display_handler),
            )
            // Where alerts go. Admin-only: a webhook URL is the whole
            // credential for posting into that channel.
            .route(
                "/api/v1/admin/notifiers",
                get(notifiers_handler).put(update_notifiers_handler),
            )
            .route("/api/v1/admin/notifiers/test", post(test_notifier_handler))
            // Scheduled windows that suppress paging. Admin-only: a window is a
            // licence to go quiet, so an anonymous writer could silence the
            // whole fleet.
            .route(
                "/api/v1/admin/maintenance",
                get(maintenance_handler).put(update_maintenance_handler),
            )
            // Try a script before saving it: a typo would otherwise mark a
            // service down every interval until someone noticed.
            .route("/api/v1/admin/eval-test", post(eval_test_handler))
            // What agents probe. Carries targets, so it sits behind the agent
            // token rather than being public like /status.
            .route("/api/v1/agent/checks", get(agent_checks_handler))
            // Per-zone agent credentials. Minting returns the only copy of the
            // secret, so these are admin-gated like the check editor.
            .route(
                "/api/v1/admin/agent-credentials",
                get(list_agent_credentials_handler).post(mint_agent_credential_handler),
            )
            .route(
                "/api/v1/admin/agent-credentials/:id",
                delete(revoke_agent_credential_handler),
            )
            // Peer replication. Agent-gated rather than public: it mutates
            // published state, so an anonymous POST could mark every check down.
            .route("/internal/gossip", post(gossip_handler))
            // Cluster-based data-plane replication: results, alerts and
            // incidents together, in place of the transition-only push
            // above wherever clustering is configured.
            .route(
                "/internal/replicate",
                post(replicate_handler).layer(DefaultBodyLimit::max(32 * 1024 * 1024)),
            )
            // Anti-entropy: a peer's fixed-cost summary, and the paged pull
            // that backfills whatever it says this node is missing.
            .route("/internal/digest", get(digest_handler))
            .route("/internal/backfill", get(backfill_handler))
            .route("/internal/backfill/results", get(results_backfill_handler))
            .route("/internal/backfill/alerts", get(alerts_backfill_handler))
            .route(
                "/internal/backfill/incidents",
                get(incidents_backfill_handler),
            )
            .route(
                "/internal/backfill/incident-tombstones",
                get(incident_tombstones_backfill_handler),
            )
            // Rollup anti-entropy (item A): the only way a peer recovers
            // minute/hour latency summaries for a window it no longer holds
            // raw observations for. See `storage::RollupDigest`.
            .route(
                "/internal/backfill/latency-minute",
                get(latency_minute_backfill_handler),
            )
            .route(
                "/internal/backfill/latency-hour",
                get(latency_hour_backfill_handler),
            )
            // Config-plane push: checks, sections, settings, credentials —
            // last-writer-wins by HLC, applied locally, never observer-owned
            // the way a result is. See `config_handler`.
            .route(
                "/internal/config",
                post(config_handler)
                    .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
                    .get(config_entries_handler),
            )
            // The global tier's own push of the catalogue, gated by the same
            // shared secret a `[coordinator.global]` block already presents
            // for admin fan-out — a region that trusts the global node to
            // drive an admin write trusts it to durably distribute one too.
            // See `global::reconcile`: this is what a global node retries
            // against instead of failing an operator's write when this
            // region was briefly unreachable.
            .route(
                "/internal/global/config",
                post(global_config_handler).layer(DefaultBodyLimit::max(8 * 1024 * 1024)),
            )
            .route(
                "/internal/global/config-digest",
                get(global_config_digest_handler),
            )
            // Just the ids this region holds for the Check kind — the
            // scoped-digest repair path's "what do you actually have" read,
            // made only when a mismatch is already suspected. See
            // `global::reconcile::reconcile_checks`.
            .route("/internal/global/check-ids", get(global_check_ids_handler))
            // Cluster membership. Peer-gated like every other /internal/*
            // route: pinging is how a peer rewrites what this node believes
            // about liveness, so an agent must never be able to call it.
            .route("/internal/cluster/ping", post(cluster_ping_handler))
            // The admin-facing membership view. Admin-gated, not peer-gated:
            // this is read by a human in the browser, not by another node.
            .route("/api/v1/cluster", get(cluster_view_handler))
            // Retention-aware peer recovery (item B): standing raw-data gaps
            // this node has recorded, so an operator can see which checks
            // have holes and over what range instead of only a counter.
            .route("/api/v1/admin/raw-data-gaps", get(raw_data_gaps_handler))
            // Admin JSON is deliberately bounded even when unauthenticated: the
            // limit is enforced before handlers parse or authenticate a body.
            .layer(DefaultBodyLimit::max(1024 * 1024))
            .layer(axum::middleware::from_fn(crate::security::security_headers))
            .with_state(self)
            .fallback(static_handler)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{CheckConfig, CoordinatorConfig, GlobalConfig},
        global::GlobalNode,
        models::{CheckKind, CheckResult},
    };
    use std::{net::SocketAddr, time::Instant};
    use uuid::Uuid;

    async fn serve(app: axum::Router) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    }

    fn percentile(samples: &mut [u128], percentile: usize) -> u128 {
        samples.sort_unstable();
        samples[(samples.len() * percentile).div_ceil(100).saturating_sub(1)]
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn assert_ceiling(name: &str, actual: u128) {
        if let Ok(value) = std::env::var(name) {
            let ceiling = value
                .parse::<u128>()
                .expect("benchmark ceiling must be an integer");
            assert!(actual <= ceiling, "{name}: {actual} exceeded {ceiling}");
        }
    }

    #[tokio::test]
    #[ignore = "production-cardinality benchmark; configure WARDEN_BENCH_* for the intended scale"]
    async fn benchmark_status_and_global_push_production_cardinality() {
        let check_count = env_usize("WARDEN_BENCH_CHECKS", 10_000);
        let region_count = env_usize("WARDEN_BENCH_REGIONS", 3);
        let sample_count = env_usize("WARDEN_BENCH_SAMPLES", 20).max(1);
        let token = "benchmark-global-token";
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "region-0".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let checks: Vec<_> = (0..check_count)
            .map(|index| CheckConfig {
                id: Uuid::new_v5(&Uuid::NAMESPACE_OID, index.to_string().as_bytes()),
                name: format!("check-{index}"),
                target: "https://example.invalid".into(),
                notify: false,
                latency_graph: true,
                position: index as i32,
                ..Default::default()
            })
            .collect();
        let stored = checks.clone();
        coord
            .db(move |storage| {
                for check in &stored {
                    storage.upsert_check(check, 1)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        coord.reload_checks().await.unwrap();
        let now = chrono::Utc::now();
        coord
            .ingest(
                checks
                    .iter()
                    .map(|check| CheckResult {
                        check_id: check.id,
                        agent_id: "benchmark-agent".into(),
                        zone: "benchmark-zone".into(),
                        region: "region-0".into(),
                        kind: CheckKind::Http,
                        timestamp: now,
                        latency_ms: 10,
                        status_code: Some(200),
                        body_matched: None,
                        error: None,
                        failure: None,
                        body: None,
                        headers: None,
                        metric: None,
                        metrics: None,
                    })
                    .collect(),
            )
            .await;

        let regional_addr = serve(coord.clone().router()).await;
        let global_addr = serve(
            GlobalNode::new(&GlobalConfig {
                token: Some(token.into()),
                db_path: ":memory:".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .router(),
        )
        .await;
        let client = reqwest::Client::new();
        // `DEFAULT_PAGE_SIZE` mirrors the frontend's own `STATUS_PAGE_SIZE`
        // (`frontend/src/App.svelte`): this is the page a real dashboard load
        // asks for, not the raw endpoint's ceiling. The `_max` URLs separately
        // measure that ceiling — `crate::page::MAX_PAGE_SIZE` — so the report
        // carries both "what a browser does" and "what a single request can
        // still be made to cost".
        const DEFAULT_PAGE_SIZE: usize = 100;
        let regional_url = format!("http://{regional_addr}/api/v1/status");
        let uptime_url = format!("http://{regional_addr}/api/v1/uptime?days=30&buckets=90");
        let latency_url =
            format!("http://{regional_addr}/api/v1/latency-points?hours=720&buckets=90");
        let regional_max_url = format!(
            "http://{regional_addr}/api/v1/status?limit={}",
            crate::page::MAX_PAGE_SIZE
        );
        let uptime_max_url = format!(
            "http://{regional_addr}/api/v1/uptime?days=30&buckets=90&limit={}",
            crate::page::MAX_PAGE_SIZE
        );
        let latency_max_url = format!(
            "http://{regional_addr}/api/v1/latency-points?hours=720&buckets=90&limit={}",
            crate::page::MAX_PAGE_SIZE
        );
        let global_url = format!("http://{global_addr}/api/v1/status");
        let push_url = format!("http://{global_addr}/internal/global/state");
        let page_len = check_count.min(DEFAULT_PAGE_SIZE);
        let max_page_len = check_count.min(crate::page::MAX_PAGE_SIZE);

        let warm = client.get(&regional_url).send().await.unwrap();
        assert_eq!(warm.status(), reqwest::StatusCode::OK);
        let mut regional_ms = Vec::with_capacity(sample_count);
        let mut regional_bytes = 0;
        for _ in 0..sample_count {
            let started = Instant::now();
            let response = client.get(&regional_url).send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let body = response.bytes().await.unwrap();
            regional_ms.push(started.elapsed().as_millis());
            regional_bytes = body.len();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["checks"].as_array().unwrap().len(), page_len);
            assert_eq!(
                value["total_checks"].as_u64().unwrap() as usize,
                check_count
            );
        }

        let warm = tokio::join!(
            client.get(&regional_url).send(),
            client.get(&uptime_url).send(),
            client.get(&latency_url).send(),
        );
        assert!(warm.0.unwrap().status().is_success());
        assert!(warm.1.unwrap().status().is_success());
        assert!(warm.2.unwrap().status().is_success());
        let mut page_ms = Vec::with_capacity(sample_count);
        let mut page_bytes = 0;
        for _ in 0..sample_count {
            let started = Instant::now();
            let (status, uptime, latency) = tokio::join!(
                client.get(&regional_url).send(),
                client.get(&uptime_url).send(),
                client.get(&latency_url).send(),
            );
            let (status, uptime, latency) = (status.unwrap(), uptime.unwrap(), latency.unwrap());
            assert!(status.status().is_success());
            assert!(uptime.status().is_success());
            assert!(latency.status().is_success());
            let (status, uptime, latency) =
                tokio::join!(status.bytes(), uptime.bytes(), latency.bytes());
            let (status, uptime, latency) = (status.unwrap(), uptime.unwrap(), latency.unwrap());
            page_ms.push(started.elapsed().as_millis());
            page_bytes = status.len() + uptime.len() + latency.len();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&status).unwrap()["total_checks"]
                    .as_u64()
                    .unwrap() as usize,
                check_count
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&uptime).unwrap()["checks"]
                    .as_array()
                    .unwrap()
                    .len(),
                page_len
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&latency).unwrap()["checks"]
                    .as_array()
                    .unwrap()
                    .len(),
                page_len
            );
        }

        // The ceiling case: a client that explicitly asks for the largest page
        // the server will still hand out (`MAX_PAGE_SIZE`), which is what the
        // old unbounded read effectively did for every check at once. This is
        // not what the dashboard requests, but it is the worst a single
        // request can still cost, and that number belongs in the same report.
        let warm = tokio::join!(
            client.get(&regional_max_url).send(),
            client.get(&uptime_max_url).send(),
            client.get(&latency_max_url).send(),
        );
        assert!(warm.0.unwrap().status().is_success());
        assert!(warm.1.unwrap().status().is_success());
        assert!(warm.2.unwrap().status().is_success());
        let mut page_max_ms = Vec::with_capacity(sample_count);
        let mut page_max_bytes = 0;
        for _ in 0..sample_count {
            let started = Instant::now();
            let (status, uptime, latency) = tokio::join!(
                client.get(&regional_max_url).send(),
                client.get(&uptime_max_url).send(),
                client.get(&latency_max_url).send(),
            );
            let (status, uptime, latency) = (status.unwrap(), uptime.unwrap(), latency.unwrap());
            assert!(status.status().is_success());
            assert!(uptime.status().is_success());
            assert!(latency.status().is_success());
            let (status, uptime, latency) =
                tokio::join!(status.bytes(), uptime.bytes(), latency.bytes());
            let (status, uptime, latency) = (status.unwrap(), uptime.unwrap(), latency.unwrap());
            page_max_ms.push(started.elapsed().as_millis());
            page_max_bytes = status.len() + uptime.len() + latency.len();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&uptime).unwrap()["checks"]
                    .as_array()
                    .unwrap()
                    .len(),
                max_page_len
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&latency).unwrap()["checks"]
                    .as_array()
                    .unwrap()
                    .len(),
                max_page_len
            );
        }

        let mut push_ms = Vec::with_capacity(sample_count * region_count);
        let mut push_bytes = 0;
        for sample in 0..sample_count {
            for region in 0..region_count {
                let started = Instant::now();
                let mut push = coord.build_state_push().await;
                push.region = format!("region-{region}");
                push.origin = format!("node-{region}-{sample}");
                push.pushed_at += sample as i64 + 1;
                push_bytes = serde_json::to_vec(&push).unwrap().len();
                let response = client
                    .post(&push_url)
                    .bearer_auth(token)
                    .json(&push)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), reqwest::StatusCode::OK);
                push_ms.push(started.elapsed().as_millis());
            }
        }

        let warm = client.get(&global_url).send().await.unwrap();
        assert_eq!(warm.status(), reqwest::StatusCode::OK);
        let mut global_ms = Vec::with_capacity(sample_count);
        let mut global_bytes = 0;
        for _ in 0..sample_count {
            let started = Instant::now();
            let response = client.get(&global_url).send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let body = response.bytes().await.unwrap();
            global_ms.push(started.elapsed().as_millis());
            global_bytes = body.len();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["checks"].as_array().unwrap().len(), page_len);
            assert_eq!(
                value["total_checks"].as_u64().unwrap() as usize,
                check_count
            );
            assert_eq!(value["agents"].as_array().unwrap().len(), region_count);
        }

        let regional_p95 = percentile(&mut regional_ms, 95);
        let page_p95 = percentile(&mut page_ms, 95);
        let page_max_p95 = percentile(&mut page_max_ms, 95);
        let push_p95 = percentile(&mut push_ms, 95);
        let global_p95 = percentile(&mut global_ms, 95);
        assert_ceiling("WARDEN_BENCH_MAX_REGIONAL_STATUS_P95_MS", regional_p95);
        assert_ceiling("WARDEN_BENCH_MAX_STATUS_PAGE_P95_MS", page_p95);
        assert_ceiling("WARDEN_BENCH_MAX_STATUS_PAGE_BYTES", page_bytes as u128);
        assert_ceiling("WARDEN_BENCH_MAX_STATUS_PAGE_MAX_P95_MS", page_max_p95);
        assert_ceiling(
            "WARDEN_BENCH_MAX_STATUS_PAGE_MAX_BYTES",
            page_max_bytes as u128,
        );
        assert_ceiling("WARDEN_BENCH_MAX_GLOBAL_PUSH_P95_MS", push_p95);
        assert_ceiling("WARDEN_BENCH_MAX_GLOBAL_STATUS_P95_MS", global_p95);
        assert_ceiling("WARDEN_BENCH_MAX_PUSH_BYTES", push_bytes as u128);
        println!(
            "checks={check_count} regions={region_count} samples={sample_count} page_size={page_len} regional_status_p95_ms={regional_p95} regional_status_bytes={regional_bytes} status_page_p95_ms={page_p95} status_page_bytes={page_bytes} status_page_requests=3 status_page_max_p95_ms={page_max_p95} status_page_max_bytes={page_max_bytes} status_page_max_size={max_page_len} global_push_p95_ms={push_p95} push_bytes={push_bytes} global_status_p95_ms={global_p95} global_status_bytes={global_bytes}"
        );
    }
}
