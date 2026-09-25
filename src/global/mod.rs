//! The global coordinator (`warden global`), Stage B of the design.
//!
//! It holds **no raw data**. Regional coordinators push a state summary every
//! few seconds; the global node caches the latest per region ([`cache`]),
//! serves an aggregated dashboard over the same public API the coordinator
//! does — so the embedded SPA renders unchanged — and reverse-proxies any read
//! that needs raw history back to a coordinator in the owning region. A single
//! node today; the push/cache split is shaped so global-to-global replication
//! can be added later without changing the wire format (see [`wire`]).
//!
//! Split across sibling modules by concern: [`routes`] builds the router and
//! serves the handlers answered from local cache/state; [`relay`] proxies
//! everything else to regional coordinators; [`merge`] folds the resulting
//! per-region answers into one; [`assets`] serves the embedded SPA.

mod admin;
pub mod alert;
mod assets;
pub mod cache;
pub mod cluster;
pub mod incidents;
mod merge;
pub mod reconcile;
mod relay;
mod routes;
pub mod store;
pub mod wire;

use crate::config::GlobalConfig;
use alert::GlobalAlerter;
use axum::Router;
use cache::GlobalCache;
use routes::GlobalWsEvent;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::warn;

pub(crate) use relay::parse_composite_incident_id;
pub(crate) use routes::{error_response, require_admin};

/// Shared global-node state for axum handlers.
#[derive(Clone)]
pub struct GlobalNode {
    inner: Arc<Inner>,
}

struct Inner {
    cache: GlobalCache,
    alerter: GlobalAlerter,
    auth: crate::auth::Auth,
    /// Secret regional coordinators present on `/internal/global/*`. Also the
    /// credential the global node presents *back* to a region as a `Bearer` to
    /// relay admin operations an operator drives from the global dashboard,
    /// and as the push destination for the durable catalogue in `store`.
    token: Option<String>,
    secure_cookies: bool,
    /// Reverse proxies trusted to supply `X-Forwarded-For` for login rate
    /// limiting, parsed by `GlobalConfig::validate`. `new` is not fallible,
    /// so a config that bypassed `validate` (only possible from a direct,
    /// non-`main` construction, e.g. a test) falls back to trusting nothing.
    trusted_proxies: crate::security::TrustedProxies,
    /// Client for proxying drill-down reads and admin writes to regions.
    client: reqwest::Client,
    /// Fan-out for dashboard WebSocket clients. A `state_change` is published
    /// here whenever an incoming push moves a check's cross-region rollup.
    events: broadcast::Sender<GlobalWsEvent>,
    /// The durable catalogue (SCALING.md "Placement-scoped configuration",
    /// Phase 1). Authoritative for checks and sections; everything else the
    /// global dashboard edits still goes straight to regions the way it
    /// always has. See `store`'s module doc.
    store: store::GlobalStore,
    /// Membership and config anti-entropy against other global nodes
    /// (Phase 4). `None` when `global.peers` is not configured — a single
    /// global node is a complete deployment on its own.
    cluster: Option<Arc<crate::coordinator::cluster::Cluster>>,
    /// Secret other global nodes present on `/internal/*`. Kept alongside
    /// `cluster` rather than read out of it: a coordinator's own
    /// `require_peer` gates the same way, against its own copy rather than
    /// reaching into `Cluster`, which has no public accessor for it.
    peer_token: Option<String>,
    /// How often `spawn_background`'s heartbeat loop pings every global peer.
    gossip_interval_secs: u64,
    /// How often `spawn_background`'s anti-entropy loop reconciles the
    /// catalogue with a peer global node and with every known region.
    anti_entropy_secs: u64,
}
impl GlobalNode {
    /// Opens the durable catalogue and, when `global.peers` is configured,
    /// this node's own cluster membership — the two things a global node
    /// needs that a cache-only one did not. Everything else is unchanged
    /// from before Phase 1.
    pub async fn new(cfg: &GlobalConfig) -> anyhow::Result<Self> {
        let cache = GlobalCache::new(cfg.region_staleness_secs, &cfg.regions);
        // Signing key from the shared push token when present, so a restart
        // does not invalidate every admin session — the same trick the
        // coordinator uses with its peer token.
        let auth = match cfg.resolved_token() {
            Some(token) => {
                crate::auth::Auth::with_signing_key(cfg.resolved_admin_password(), token.as_bytes())
            }
            None => crate::auth::Auth::new(cfg.resolved_admin_password()),
        };
        if !auth.admin_enabled() {
            warn!("no admin password set (WARDEN_ADMIN_PASSWORD); the admin UI is disabled");
        }
        let store = store::GlobalStore::open(cfg.db_path.clone(), cfg.resolved_node_id()).await?;
        let node_id = store.node_id().to_string();
        let cluster = match (cfg.resolved_peer_token(), cfg.resolved_advertise_url()) {
            (Some(token), Some(url)) => {
                let now = chrono::Utc::now().timestamp_millis();
                let c = crate::coordinator::cluster::Cluster::new(
                    node_id,
                    url,
                    now,
                    cfg.peers.clone(),
                    Some(token),
                    cfg.peer_timeout_secs,
                );
                Some(Arc::new(c))
            }
            _ => None,
        };
        let peer_token = cfg.resolved_peer_token();
        let (events, _) = broadcast::channel(256);
        Ok(Self {
            inner: Arc::new(Inner {
                cache,
                alerter: GlobalAlerter::new(cfg.notifiers.clone()),
                auth,
                token: cfg.resolved_token(),
                secure_cookies: cfg.secure_cookies,
                trusted_proxies: crate::security::TrustedProxies::parse(&cfg.trusted_proxies)
                    .unwrap_or_default(),
                // Bounded: this client drives the periodic region
                // reconciler as well as dashboard relays. A region that
                // accepts the connection and never answers would otherwise
                // hold the reconcile loop forever, and the only symptom
                // would be regions quietly ceasing to converge.
                client: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(15))
                    .build()
                    .unwrap_or_default(),
                events,
                store,
                cluster,
                peer_token,
                gossip_interval_secs: cfg.gossip_interval_secs,
                anti_entropy_secs: cfg.anti_entropy_secs,
            }),
        })
    }
    pub fn spawn_background(&self) {
        cluster::spawn(self);
        reconcile::spawn_region_reconciler(self.clone());
    }

    pub fn router(self) -> Router {
        routes::router(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::Response;

    // ---- Placement-scoped configuration, Phase 1 & 4 (SCALING.md) ----

    use crate::config::{CoordinatorConfig, GlobalPushConfig, GlobalRegion};
    use crate::coordinator::Coordinator;

    async fn serve_router(router: Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        addr
    }

    fn admin_cookie(node: &GlobalNode, password: &str) -> String {
        let token = node
            .inner
            .auth
            .login(
                password,
                "127.0.0.1".parse().unwrap(),
                chrono::Utc::now().timestamp_millis(),
            )
            .unwrap();
        format!("{}={}", crate::auth::GLOBAL_SESSION_COOKIE, token)
    }

    /// Phase 1's central claim: a catalogue write commits to the global store
    /// as one local operation, so it is unaffected by a region being down —
    /// no 502, no partial fan-out — and the down region converges on its own
    /// once it is reachable again, with no repeat of the write from the
    /// operator.
    #[tokio::test]
    async fn admin_write_survives_an_unreachable_region_and_converges_once_it_returns() {
        let global_token = "global-token";
        let admin_password = "secret";
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let coord_addr = serve_router(coord.clone().router()).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            admin_password: Some(admin_password.into()),
            db_path: ":memory:".into(),
            regions: vec![GlobalRegion {
                name: "us".into(),
                // Port 1 is a reserved, never-listening port: an admin write
                // must not depend on this ever answering.
                endpoints: vec!["http://127.0.0.1:1".into()],
            }],
            ..Default::default()
        })
        .await
        .unwrap();
        let cookie = admin_cookie(&node, admin_password);
        let global_addr = serve_router(node.clone().router()).await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("http://{global_addr}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &cookie)
            .json(&serde_json::json!({"name": "web", "type": "http", "target": "https://example.com"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "the write itself must not fail"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        let check_name = body["check"]["name"].as_str().unwrap().to_string();
        assert_eq!(check_name, "web");

        // The catalogue is authoritative on the global store regardless of
        // whether any region ever answers.
        let (checks, total, _) = node.inner.store.list_checks(0, 10).await.unwrap();
        assert_eq!(total, 1);
        assert_eq!(checks[0].name, "web");

        // The region has not seen it yet — its only endpoint never answers.
        assert!(coord.list_checks_for_test().await.is_empty());

        // The region "returns": it pushes a fresh state summary naming its
        // real, reachable address, exactly as `Coordinator::spawn_global_push`
        // would on its own timer.
        let push = wire::RegionStatePush {
            region: "us".into(),
            origin: "coordinator-us".into(),
            advertise_url: Some(format!("http://{coord_addr}")),
            pushed_at: chrono::Utc::now().timestamp_millis(),
            region_health: crate::models::NodeState::Up,
            agents_healthy: 1,
            agents_total: 1,
            checks: Vec::new(),
        };
        client
            .post(format!("http://{global_addr}/internal/global/state"))
            .bearer_auth(global_token)
            .json(&push)
            .send()
            .await
            .unwrap();

        // Drive one reconciliation pass directly rather than waiting on
        // `reconcile::RECONCILE_INTERVAL`.
        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }

        let checks = coord.list_checks_for_test().await;
        assert_eq!(
            checks.len(),
            1,
            "the region converges without a repeated write"
        );
        assert_eq!(checks[0].name, "web");
    }

    /// Phase 4: two global nodes reconcile the catalogue between themselves —
    /// a write applied only to one converges onto the other through the same
    /// digest-and-backfill anti-entropy a coordinator's peers already use.
    #[tokio::test]
    async fn two_global_nodes_converge_through_anti_entropy() {
        let global_token = "global-token";
        let peer_token = "global-peer-token";

        let node_a = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            node_id: Some("global-a".into()),
            peer_token: Some(peer_token.into()),
            advertise_url: Some("http://global-a.invalid".into()),
            ..Default::default()
        })
        .await
        .unwrap();
        let addr_a = serve_router(node_a.clone().router()).await;

        // A write that only ever reaches node A — node B never hears it any
        // other way in this test, standing in for "B was down when A wrote".
        node_a
            .inner
            .store
            .create_check(
                serde_json::json!({"name": "web", "type": "http", "target": "https://example.com"}),
            )
            .await
            .ok()
            .unwrap();

        let node_b = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            node_id: Some("global-b".into()),
            peer_token: Some(peer_token.into()),
            advertise_url: Some("http://global-b.invalid".into()),
            peers: vec![format!("http://{addr_a}")],
            ..Default::default()
        })
        .await
        .unwrap();

        assert!(node_b
            .inner
            .store
            .list_checks(0, 10)
            .await
            .unwrap()
            .0
            .is_empty());

        cluster::reconcile_with_peer(&node_b, &format!("http://{addr_a}")).await;

        let (checks, total, _) = node_b.inner.store.list_checks(0, 10).await.unwrap();
        assert_eq!(total, 1);
        assert_eq!(checks[0].name, "web");
    }

    /// Phase 1: the catalogue is durable, not merely cached — a restart with
    /// the same `db_path` reopens the same checks rather than starting empty.
    #[tokio::test]
    async fn global_node_restart_preserves_the_catalogue() {
        let dir = std::env::temp_dir().join(format!(
            "warden-global-restart-test-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("global.duckdb").to_string_lossy().to_string();
        let cfg = GlobalConfig {
            token: Some("global-token".into()),
            db_path: db_path.clone(),
            ..Default::default()
        };

        let node = GlobalNode::new(&cfg).await.unwrap();
        node.inner
            .store
            .create_check(
                serde_json::json!({"name": "web", "type": "http", "target": "https://example.com"}),
            )
            .await
            .ok()
            .unwrap();
        drop(node);

        let reopened = GlobalNode::new(&cfg).await.unwrap();
        let (checks, total, _) = reopened.inner.store.list_checks(0, 10).await.unwrap();
        assert_eq!(total, 1, "the catalogue survives a restart");
        assert_eq!(checks[0].name, "web");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The data plane stays regional: a coordinator keeps serving status with
    /// no global node reachable at all — not merely tolerant of a slow one,
    /// but functionally independent of it. `spawn_global_push` is started to
    /// show its failure against an address nothing listens on costs nothing
    /// beyond a logged warning.
    /// A refused connection fails fast; a blackholed one does not. A global
    /// node that accepts the socket and never answers — a half-created
    /// container, a dropped route — used to hold the push loop on its first
    /// iteration for the life of the process, with no warning and no retry.
    /// The region simply stopped appearing on the global dashboard, which is
    /// indistinguishable from a region that has not pushed yet. This is the
    /// failure mode this project treats as the dangerous one, so the push
    /// must be bounded and must keep ticking.
    #[tokio::test]
    async fn a_global_node_that_never_answers_does_not_wedge_the_push_loop() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::Relaxed);
                // Hold the socket open and never answer.
                held.push(socket);
            }
        });

        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: format!("http://{addr}"),
                token: Some("global-token".into()),
                token_env: None,
                push_secs: 1,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        coord.spawn_global_push();

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        let tries = accepted.load(Ordering::Relaxed);
        assert!(
            tries >= 2,
            "a silent global must cost one bounded attempt per tick, not one forever: {tries}"
        );
    }

    #[tokio::test]
    async fn regions_keep_serving_status_with_the_global_tier_entirely_down() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://127.0.0.1:1".into(),
                token: Some("global-token".into()),
                token_env: None,
                push_secs: 1,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        coord.spawn_global_push();
        coord
            .create_check_for_test(crate::config::CheckConfig {
                name: "web".into(),
                kind: crate::models::CheckKind::Http,
                target: "https://example.com".into(),
                ..Default::default()
            })
            .await;
        let coord_addr = serve_router(coord.router()).await;

        let resp = reqwest::get(format!("http://{coord_addr}/api/v1/status"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["checks"][0]["name"], "web");
    }

    // ---- Placement-scoped configuration, Phase 2 & 3 (SCALING.md) ----

    /// SCALING.md's concrete disclosure case: `unifi`/`upstream`, placed to
    /// `["us"]` only, still had their full definitions — one with a push
    /// credential in its target — sitting in `coordinator-tw`'s database.
    /// This is the test that Phase 2 reconciliation actually closes that
    /// gap: a check whose placement admits only `us` must never reach
    /// `tw`'s store at all.
    #[tokio::test]
    async fn a_check_placed_to_one_region_never_reaches_another() {
        let global_token = "global-token";
        let admin_password = "secret";
        let us = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let us_addr = serve_router(us.clone().router()).await;

        let tw = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "tw".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let tw_addr = serve_router(tw.clone().router()).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            admin_password: Some(admin_password.into()),
            db_path: ":memory:".into(),
            regions: vec![
                GlobalRegion {
                    name: "us".into(),
                    endpoints: vec![format!("http://{us_addr}")],
                },
                GlobalRegion {
                    name: "tw".into(),
                    endpoints: vec![format!("http://{tw_addr}")],
                },
            ],
            ..Default::default()
        })
        .await
        .unwrap();
        let cookie = admin_cookie(&node, admin_password);
        let global_addr = serve_router(node.clone().router()).await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("http://{global_addr}/api/v1/admin/checks"))
            .header(reqwest::header::COOKIE, &cookie)
            .json(&serde_json::json!({
                "name": "unifi",
                "type": "http",
                // A referenced secret, per Phase 0 ("Secrets are referenced,
                // not carried") — a literal credential in a query value is
                // rejected outright, which is a separate control from this
                // test's placement-scoping claim.
                "target": "https://192.168.1.1/push?token=${PUSH_TOKEN}",
                "placement": {"regions": ["us"]},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Drive reconciliation for both regions directly rather than waiting
        // on `push_now`'s spawned task or `RECONCILE_INTERVAL`.
        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }

        let us_checks = us.list_checks_for_test().await;
        assert_eq!(us_checks.len(), 1, "the placed region receives it");
        assert_eq!(us_checks[0].name, "unifi");

        let tw_checks = tw.list_checks_for_test().await;
        assert!(
            tw_checks.is_empty(),
            "a region never placed must never hold the definition, secret included: {tw_checks:?}"
        );
    }

    /// Narrowing a check's placement away from a region must unplace it
    /// there — never delete it, which would write a `config_tombstone` that
    /// replicates and deletes the check in the region still probing it
    /// (SCALING.md "Unplace is disable-shaped, not delete-shaped"). Checked
    /// directly against the region's own digest: a tombstone would leave
    /// `config_digest`'s Check-kind count at 1 (the tombstone row), not 0.
    #[tokio::test]
    async fn unplace_writes_no_tombstone_and_the_holding_region_keeps_the_check() {
        let global_token = "global-token";
        let us = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let us_addr = serve_router(us.clone().router()).await;

        let tw = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "tw".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let tw_addr = serve_router(tw.clone().router()).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            regions: vec![
                GlobalRegion {
                    name: "us".into(),
                    endpoints: vec![format!("http://{us_addr}")],
                },
                GlobalRegion {
                    name: "tw".into(),
                    endpoints: vec![format!("http://{tw_addr}")],
                },
            ],
            ..Default::default()
        })
        .await
        .unwrap();

        // No placement named: every region admits it, so both get it.
        let (_, entry) = node
            .inner
            .store
            .create_check(serde_json::json!({
                "name": "dns", "type": "http", "target": "https://example.com",
            }))
            .await
            .ok()
            .unwrap();
        let check_id: crate::models::CheckId = entry.id.parse().unwrap();

        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }
        assert_eq!(us.list_checks_for_test().await.len(), 1, "us receives it");
        assert_eq!(
            tw.list_checks_for_test().await.len(),
            1,
            "tw receives it too"
        );

        // Narrow placement to `us` only.
        node.inner
            .store
            .update_check(
                check_id,
                serde_json::json!({
                    "name": "dns", "type": "http", "target": "https://example.com",
                    "placement": {"regions": ["us"]},
                }),
            )
            .await
            .ok()
            .unwrap();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }

        assert_eq!(
            us.list_checks_for_test().await.len(),
            1,
            "the still-placed region keeps it"
        );
        assert!(
            tw.list_checks_for_test().await.is_empty(),
            "tw's placement no longer admits it"
        );

        let tw_digest = tw.config_digest_for_test().await;
        let check_digest = tw_digest
            .iter()
            .find(|d| d.kind == crate::coordinator::storage::ConfigKind::Check)
            .unwrap();
        assert_eq!(
            check_digest.count, 0,
            "unplace must leave no tombstone behind — a tombstone here would \
             replicate and delete the check in us too"
        );
    }

    /// Once a region has converged, a further sweep must cost exactly one
    /// digest comparison — no full-table push, no id-set diff, no unplace
    /// call. A placement-scoped digest that still amplified would defeat the
    /// whole point of Phase 2, the same way a whole-catalogue digest against
    /// a subset region already did before this change.
    #[tokio::test]
    async fn converged_region_costs_no_further_paging_once_reconciled() {
        async fn log_requests(
            axum::extract::State(log): axum::extract::State<RequestLog>,
            req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> Response {
            log.lock()
                .unwrap()
                .push((req.method().to_string(), req.uri().path().to_string()));
            next.run(req).await
        }
        type RequestLog = std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>;

        let global_token = "global-token";
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://ignored.invalid".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let log: RequestLog = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let router = coord
            .clone()
            .router()
            .layer(axum::middleware::from_fn_with_state(
                log.clone(),
                log_requests,
            ));
        let coord_addr = serve_router(router).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            regions: vec![GlobalRegion {
                name: "us".into(),
                endpoints: vec![format!("http://{coord_addr}")],
            }],
            ..Default::default()
        })
        .await
        .unwrap();
        node.inner
            .store
            .create_check(serde_json::json!({
                "name": "web", "type": "http", "target": "https://example.com",
            }))
            .await
            .ok()
            .unwrap();

        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }
        assert_eq!(
            coord.list_checks_for_test().await.len(),
            1,
            "first sweep converges the region"
        );

        log.lock().unwrap().clear();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }
        let seen = log.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![(
                "GET".to_string(),
                "/internal/global/config-digest".to_string()
            )],
            "a converged region must cost exactly one digest read per sweep, not a full page"
        );
    }

    /// The data plane's independence claim, specifically for a check that
    /// arrived *through* placement-scoped reconciliation rather than a
    /// direct write: once placed, a region that never hears from global
    /// again keeps probing and serving status for it. Nothing regional ever
    /// unplaces a check on its own — that verb is only ever driven by an
    /// incoming call from the global tier — so losing contact with global
    /// must read as "no further instructions", never as "unplace
    /// everything".
    #[tokio::test]
    async fn a_region_cut_off_from_global_keeps_what_it_was_placed() {
        let global_token = "global-token";
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "us".into(),
            global: Some(GlobalPushConfig {
                url: "http://127.0.0.1:1".into(),
                token: Some(global_token.into()),
                token_env: None,
                push_secs: 3600,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        let coord_addr = serve_router(coord.clone().router()).await;

        let node = GlobalNode::new(&GlobalConfig {
            token: Some(global_token.into()),
            db_path: ":memory:".into(),
            regions: vec![GlobalRegion {
                name: "us".into(),
                endpoints: vec![format!("http://{coord_addr}")],
            }],
            ..Default::default()
        })
        .await
        .unwrap();
        node.inner
            .store
            .create_check(serde_json::json!({
                "name": "web", "type": "http", "target": "https://example.com",
            }))
            .await
            .ok()
            .unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        for (region, endpoints) in node.inner.cache.region_endpoints(now) {
            reconcile::reconcile_region(&node, &region, &endpoints).await;
        }
        assert_eq!(
            coord.list_checks_for_test().await.len(),
            1,
            "reconciliation placed it"
        );

        // The global node vanishes entirely; no further sweep ever reaches
        // this region again.
        drop(node);

        let resp = reqwest::get(format!("http://{coord_addr}/api/v1/status"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["checks"][0]["name"], "web");
        assert_eq!(
            coord.list_checks_for_test().await.len(),
            1,
            "still placed with no global reachable at all"
        );
    }
}
