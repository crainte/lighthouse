//! Opt-in release-mode benchmark. It uses only localhost services and a unique
//! temporary DuckDB directory; run it explicitly with the command below.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use duckdb::Connection;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{routing::get, Router};
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;
use warden::agent;
use warden::config::{
    AgentConfig, CheckConfig, CoordinatorConfig, GlobalConfig, GlobalPushConfig, RetentionConfig,
};
use warden::coordinator::Coordinator;
use warden::credential;
use warden::global::GlobalNode;
use warden::models::{CheckId, CheckKind, CheckResult};
use warden::placement::Placement;

const DEFAULT_CHECKS: usize = 10_000;
const DEFAULT_SECS: u64 = 45;
const DEFAULT_MAX_DISK_MB: u64 = 512;
const DEFAULT_CRON: &str = "*/30 * * * * *";
const PEERS: usize = 3;
const DIAGNOSTIC_LIMIT: usize = 20;

struct TempDb {
    dir: PathBuf,
    retain: bool,
}

impl TempDb {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "warden-release-bench-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir, retain: false }
    }

    fn retain(&mut self) {
        self.retain = true;
    }

    fn path(&self, name: &str) -> String {
        self.dir
            .join(format!("{name}.duckdb"))
            .display()
            .to_string()
    }

    fn assert_below(&self, max_bytes: u64) {
        let bytes = std::fs::read_dir(&self.dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| entry.metadata().ok())
            .map(|metadata| metadata.len())
            .sum::<u64>();
        assert!(
            bytes <= max_bytes,
            "benchmark DB used {} MiB, exceeding configured {} MiB safety cap",
            bytes / 1024 / 1024,
            max_bytes / 1024 / 1024
        );
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        if !self.retain {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

struct Regional {
    coord: Coordinator,
    addr: SocketAddr,
    server: JoinHandle<()>,
}

async fn serve(app: Router) -> (SocketAddr, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, server)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_secs(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn benchmark_check(id: CheckId, number: usize, target: String, cron: &str) -> CheckConfig {
    CheckConfig {
        id,
        name: format!("localhost-live-http-{number}"),
        kind: CheckKind::Http,
        target,
        cron: cron.into(),
        timeout_secs: 2,
        latency_graph: true,
        placement: Placement {
            zones: (0..PEERS)
                .map(|index| format!("bench-zone-{index}"))
                .collect(),
            regions: vec!["bench-region".into()],
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn wait_until<T>(what: &str, timeout: Duration, mut test: impl FnMut() -> T)
where
    T: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if test().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn status_checks(client: &reqwest::Client, base: &str) -> usize {
    client
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["checks"]
        .as_array()
        .unwrap()
        .len()
}

async fn coverage(
    coord: &Coordinator,
    ids: &[CheckId],
    agent_ids: &[String],
    began: chrono::DateTime<chrono::Utc>,
) -> Vec<(String, u64)> {
    coord
        .result_coverage(ids.to_vec(), agent_ids.to_vec(), began.timestamp_millis())
        .await
        .unwrap()
}

#[allow(dead_code)]
#[derive(Debug)]
struct ResultStats {
    count: i64,
    max_ts: i64,
    watermark_checks: i64,
    watermark_count: i64,
    watermark_max_ts: i64,
    collision_groups: i64,
    collision_rows: i64,
}

fn result_stats(
    conn: &Connection,
    ids: &[CheckId],
    agent_id: &str,
    since: i64,
) -> duckdb::Result<ResultStats> {
    let check_ids = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    let placeholders = std::iter::repeat_n("?", check_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut result_params = vec![duckdb::types::Value::BigInt(since)];
    result_params.extend(check_ids.iter().cloned().map(duckdb::types::Value::Text));
    result_params.push(duckdb::types::Value::Text(agent_id.into()));
    let (count, max_ts) = conn.query_row(
        &format!(
            "SELECT COUNT(*), COALESCE(MAX(ts_millis), 0) FROM check_results \
             WHERE ts_millis >= ? AND check_id IN ({placeholders}) AND agent_id = ?"
        ),
        duckdb::params_from_iter(result_params),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let watermark_params = check_ids
        .iter()
        .cloned()
        .map(duckdb::types::Value::Text)
        .collect::<Vec<_>>();
    let (watermark_checks, watermark_count, watermark_max_ts) = conn.query_row(
        &format!(
            "SELECT COUNT(*), COALESCE(SUM(result_count), 0), COALESCE(MAX(max_ts), 0) \
             FROM result_replication_watermarks WHERE check_id IN ({placeholders})"
        ),
        duckdb::params_from_iter(watermark_params),
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let mut collision_params = vec![duckdb::types::Value::BigInt(since)];
    collision_params.extend(check_ids.iter().cloned().map(duckdb::types::Value::Text));
    collision_params.push(duckdb::types::Value::Text(agent_id.into()));
    let (collision_groups, collision_rows) = conn.query_row(
        &format!(
            "SELECT COUNT(*), COALESCE(SUM(rows - 1), 0) FROM ( \
                 SELECT COUNT(*) AS rows FROM check_results \
                 WHERE ts_millis >= ? AND check_id IN ({placeholders}) AND agent_id = ? \
                 GROUP BY check_id, agent_id, ts_millis HAVING COUNT(*) > 1 \
             )"
        ),
        duckdb::params_from_iter(collision_params),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(ResultStats {
        count,
        max_ts,
        watermark_checks,
        watermark_count,
        watermark_max_ts,
        collision_groups,
        collision_rows,
    })
}

fn source_agent_diagnostics(
    temp: &TempDb,
    source_index: usize,
    ids: &[CheckId],
    agent_id: &str,
    since: i64,
    loops: usize,
) -> Result<String, String> {
    let source = Connection::open(temp.path(&format!("peer-{source_index}")))
        .map_err(|error| error.to_string())?;
    let stats = result_stats(&source, ids, agent_id, since).map_err(|error| error.to_string())?;
    let distinct_coverage: i64 = source
        .query_row(
            "SELECT COUNT(DISTINCT check_id) FROM check_results \
             WHERE ts_millis >= ? AND agent_id = ?",
            duckdb::params![since, agent_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let poll: Option<(String, String, i64)> = source
        .query_row(
            "SELECT zone, region, polled_at FROM agent_polls WHERE agent_id = ?",
            duckdb::params![agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .ok();
    Ok(format!(
        "source_agent={agent_id} loops={loops} distinct_coverage={distinct_coverage}/{} raw_rows={} max_ts={} agent_poll={poll:?}",
        ids.len(), stats.count, stats.max_ts,
    ))
}

async fn source_agent_assignment(
    client: &reqwest::Client,
    addr: SocketAddr,
    agent_id: &str,
    token: &str,
) -> String {
    match client
        .get(format!(
            "http://{addr}/api/v1/agent/checks?agent_id={agent_id}&poll_secs=1"
        ))
        .bearer_auth(token)
        .send()
        .await
    {
        Ok(response) => match response.error_for_status() {
            Ok(response) => match response.json::<serde_json::Value>().await {
                Ok(body) => {
                    let checks = body["checks"].as_array().map_or(&[][..], Vec::as_slice);
                    let sample = checks
                        .iter()
                        .take(DIAGNOSTIC_LIMIT)
                        .filter_map(|check| check["id"].as_str())
                        .collect::<Vec<_>>();
                    format!(
                        "agent_checks_assigned={} sample_ids={sample:?}",
                        checks.len()
                    )
                }
                Err(error) => format!("agent_checks_response_error={error}"),
            },
            Err(error) => format!("agent_checks_status_error={error}"),
        },
        Err(error) => format!("agent_checks_request_error={error}"),
    }
}

fn watermark_map(
    conn: &Connection,
    ids: &[CheckId],
) -> duckdb::Result<HashMap<CheckId, (i64, i64)>> {
    let check_ids = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    let placeholders = std::iter::repeat_n("?", check_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn.prepare(&format!(
        "SELECT check_id, max_ts, result_count FROM result_replication_watermarks \
         WHERE check_id IN ({placeholders})"
    ))?;
    let rows = stmt.query_map(
        duckdb::params_from_iter(check_ids.into_iter().map(duckdb::types::Value::Text)),
        |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))),
    )?;
    rows.collect()
}

fn convergence_diagnostics(
    temp: &TempDb,
    target_index: usize,
    ids: &[CheckId],
    agent_ids: &[String],
    since: i64,
) -> Result<String, String> {
    let target =
        Connection::open(temp.path(&format!("peer-{target_index}"))).map_err(|e| e.to_string())?;
    let target_watermarks = watermark_map(&target, ids).map_err(|e| e.to_string())?;
    let mut lines = Vec::new();
    for (source_index, agent_id) in agent_ids.iter().enumerate() {
        let source = Connection::open(temp.path(&format!("peer-{source_index}")))
            .map_err(|e| e.to_string())?;
        let source_watermarks = watermark_map(&source, ids).map_err(|e| e.to_string())?;
        let source_stats =
            result_stats(&source, ids, agent_id, since).map_err(|e| e.to_string())?;
        let target_stats =
            result_stats(&target, ids, agent_id, since).map_err(|e| e.to_string())?;
        let mut missing = Vec::new();
        let mut digest_differences = Vec::new();
        for id in ids {
            let source_digest = source_watermarks.get(id).copied().unwrap_or((0, 0));
            let target_digest = target_watermarks.get(id).copied().unwrap_or((0, 0));
            if source_digest != target_digest && digest_differences.len() < DIAGNOSTIC_LIMIT {
                digest_differences.push(format!("{id}:{source_digest:?}->{target_digest:?}"));
            }
            if missing.len() < DIAGNOSTIC_LIMIT
                && !target
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM check_results WHERE check_id = ? AND agent_id = ? AND ts_millis >= ?)",
                        duckdb::params![id, agent_id, since],
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(|e| e.to_string())?
            {
                missing.push(id.to_string());
            }
        }
        lines.push(format!(
            "agent={agent_id} source={source_stats:?} target={target_stats:?} missing_check_ids={missing:?} digest_differences={digest_differences:?}"
        ));
    }
    Ok(lines.join("\n"))
}

/// A realistic but bounded release-mode load test. Environment knobs:
/// `WARDEN_BENCH_CHECKS` (default 10000), `WARDEN_BENCH_SECS` (45, maximum
/// 300), `WARDEN_BENCH_CRON` (default every 30 seconds),
/// `WARDEN_BENCH_MAX_DISK_MB` (512), and `WARDEN_BENCH_ANTI_ENTROPY` (default
/// `on`; set to `off` to skip `spawn_cluster`). The latter also disables that
/// method's heartbeat and discovery loops, so it is an isolation mode rather
/// than an equivalent clustered run. It is ignored so ordinary test runs never
/// start servers or write a database.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "release benchmark; run explicitly with cargo test --release --test release_benchmark -- --ignored --nocapture"]
async fn release_benchmark_three_peers_one_region_global_live_agents_and_retention() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
    let checks = env_usize("WARDEN_BENCH_CHECKS", DEFAULT_CHECKS).clamp(100, 100_000);
    let seconds = env_secs("WARDEN_BENCH_SECS", DEFAULT_SECS).clamp(1, 300);
    let cron = std::env::var("WARDEN_BENCH_CRON").unwrap_or_else(|_| DEFAULT_CRON.into());
    let probe_limit = env_usize("WARDEN_BENCH_MAX_IN_FLIGHT_PROBES", 128);
    assert!(probe_limit > 0, "probe concurrency must be positive");
    let anti_entropy = std::env::var("WARDEN_BENCH_ANTI_ENTROPY")
        .map(|value| value != "off")
        .unwrap_or(true);
    let max_disk =
        env_secs("WARDEN_BENCH_MAX_DISK_MB", DEFAULT_MAX_DISK_MB).clamp(32, 4_096) * 1024 * 1024;
    assert!(
        checks as u64 * PEERS as u64 * PEERS as u64 * 2_048 <= max_disk,
        "check count exceeds disk safety budget for three live agents and three replicas"
    );

    let metrics = agent::benchmark_metrics();
    agent::reset_benchmark_metrics();
    let mut temp = TempDb::new();
    let client = reqwest::Client::new();
    let (probe_addr, probe_server) =
        serve(Router::new().route("/health", get(|| async { "ok" }))).await;
    let probe_url = format!("http://{probe_addr}/health");
    let global_token = format!("bench-global-{}", Uuid::now_v7());
    let (global_addr, global_server) = serve(
        GlobalNode::new(&GlobalConfig {
            listen: "127.0.0.1:0".into(),
            token: Some(global_token.clone()),
            regions: Vec::new(),
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .router(),
    )
    .await;
    let global_base = format!("http://{global_addr}");

    let listeners = futures_util::future::join_all(
        (0..PEERS).map(|_| tokio::net::TcpListener::bind("127.0.0.1:0")),
    )
    .await
    .into_iter()
    .map(Result::unwrap)
    .collect::<Vec<_>>();
    let addresses: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect();
    let peer_token = format!("bench-peer-{}", Uuid::now_v7());
    let mut regions = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let addr = addresses[index];
        let peers = addresses
            .iter()
            .copied()
            .filter(|peer| *peer != addr)
            .map(|peer| format!("http://{peer}"))
            .collect();
        let coord = Coordinator::new(&CoordinatorConfig {
            listen: "127.0.0.1:0".into(),
            db_path: temp.path(&format!("peer-{index}")),
            peer_token: Some(peer_token.clone()),
            node_id: Some(format!("bench-peer-{index}")),
            advertise_url: Some(format!("http://{addr}")),
            peers,
            gossip_interval_secs: 1,
            anti_entropy_secs: 1,
            retention: RetentionConfig {
                results_days: 1,
                alerts_days: 1,
            },
            // These are coordinator peers in one region, not three regions.
            region: "bench-region".into(),
            global: Some(GlobalPushConfig {
                url: global_base.clone(),
                token: Some(global_token.clone()),
                token_env: None,
                push_secs: 1,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        if anti_entropy {
            coord.spawn_cluster();
        }
        coord.spawn_global_push();
        regions.push(Regional {
            coord: coord.clone(),
            addr,
            server: tokio::spawn(
                async move { axum::serve(listener, coord.router()).await.unwrap() },
            ),
        });
    }

    // Provision once through the production HLC/LWW path. The helper commits
    // and replicates bounded `/internal/config` batches, rather than issuing
    // one broadcast per check.
    let ids: Vec<_> = (0..checks).map(|_| Uuid::now_v7()).collect();
    regions[0]
        .coord
        .create_checks_for_test(
            ids.iter()
                .enumerate()
                .map(|(number, id)| benchmark_check(*id, number, probe_url.clone(), &cron))
                .collect(),
        )
        .await
        .unwrap();
    for region in &regions {
        wait_until(
            "direct check configuration replication",
            Duration::from_secs(30),
            || {
                let coord = region.coord.clone();
                async move { coord.check_count_for_test() == checks }
            },
        )
        .await;
    }
    // Status rendering is intentionally not the provisioning poll: it is more
    // expensive at 10k checks, but must eventually expose the replicated set.
    for region in &regions {
        let base = format!("http://{}", region.addr);
        wait_until("public check status", Duration::from_secs(30), || {
            let client = client.clone();
            let base = base.clone();
            async move { status_checks(&client, &base).await == checks }
        })
        .await;
    }

    // One properly provisioned agent per zone, all in the same region. Explicit
    // placement above means every distinct check is observed once per zone.
    let mut agents = Vec::new();
    let agent_ids: Vec<_> = (0..PEERS)
        .map(|index| format!("live-agent-{index}"))
        .collect();
    let mut agent_tokens = Vec::new();
    for (index, region) in regions.iter().enumerate() {
        let zone = format!("bench-zone-{index}");
        let (_, token) =
            credential::mint(&zone, "bench-region", chrono::Utc::now().timestamp_millis());
        region
            .coord
            .provision_agent_credential(&token, &zone, "bench-region")
            .await
            .unwrap();
        agent_tokens.push(token.clone());
        let cfg: AgentConfig = toml::from_str(&format!(
             "coordinator_url = \"http://{}\"\nagent_id = \"{}\"\nzone = \"{zone}\"\nregion = \"bench-region\"\npoll_secs = 1\nflush_secs = 1\nmax_in_flight_probes = {probe_limit}\ntoken = \"{token}\"\n",
             region.addr, agent_ids[index]
        ))
        .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(16_384);
        agents.push(tokio::spawn(agent::sync_loop(cfg.clone(), tx, Vec::new())));
        agents.push(tokio::spawn(agent::poster_loop(cfg, rx)));
    }

    // Establish one observation for every assigned (check, agent) pair before
    // the concurrent retention/read window begins.
    let observation_started = Instant::now();
    let began = chrono::Utc::now();
    for (index, region) in regions.iter().enumerate() {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let agent_id = &agent_ids[index];
        let mut loops = 0;
        loop {
            loops += 1;
            let observed =
                coverage(&region.coord, &ids, std::slice::from_ref(agent_id), began).await;
            if observed == vec![(agent_id.clone(), checks as u64)] {
                break;
            }
            if Instant::now() >= deadline {
                temp.retain();
                let diagnostics = source_agent_diagnostics(
                    &temp,
                    index,
                    &ids,
                    agent_id,
                    began.timestamp_millis(),
                    loops,
                )
                .unwrap_or_else(|error| format!("source diagnostics unavailable: {error}"));
                let assignment =
                    source_agent_assignment(&client, region.addr, agent_id, &agent_tokens[index])
                        .await;
                if let Some(metrics) = &metrics {
                    println!("agent_benchmark_metrics {metrics:?}");
                }
                panic!(
                    "source-agent observation coverage timed out at {}: observed={observed:?}\n{diagnostics}\n{assignment}\ndb_dir={}",
                    region.addr,
                    temp.dir.display(),
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    wait_until("non-empty global summary", Duration::from_secs(30), || {
        let client = client.clone();
        let global_base = global_base.clone();
        async move { status_checks(&client, &global_base).await == checks }
    })
    .await;

    // Exercise public reads while retention and all agents remain live.
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let read_duration = Duration::from_secs(seconds.min(10));
    let history_id = ids[0];
    let paths = [
        "/api/v1/status".to_string(),
        format!("/api/v1/checks/{history_id}/history?limit=100"),
        "/api/v1/latency-points?hours=1&buckets=60".to_string(),
    ];
    let readers = paths.into_iter().map(|path| {
        let client = client.clone();
        let global_base = global_base.clone();
        let latencies = latencies.clone();
        tokio::spawn(async move {
            let read_started = Instant::now();
            let mut reads = 0;
            while read_started.elapsed() < read_duration {
                let started = Instant::now();
                let response = client
                    .get(format!("{global_base}{path}"))
                    .send()
                    .await
                    .unwrap();
                assert!(
                    response.status().is_success(),
                    "reader {path} returned {}",
                    response.status()
                );
                let body: serde_json::Value = response.json().await.unwrap();
                let non_empty = if path.contains("history") {
                    body["history"]
                        .as_array()
                        .is_some_and(|rows| !rows.is_empty())
                } else if path.contains("latency-points") {
                    body["checks"]
                        .as_array()
                        .is_some_and(|rows| !rows.is_empty())
                } else {
                    body["checks"]
                        .as_array()
                        .is_some_and(|rows| rows.len() == checks)
                };
                assert!(
                    non_empty,
                    "reader {path} returned no benchmark coverage: {body}"
                );
                latencies.lock().push(started.elapsed());
                reads += 1;
            }
            reads
        })
    });

    // Retention is verified against a separate old row, not inferred from the
    // job starting successfully.
    let sentinel = Uuid::now_v7();
    let (_, retention_token) = credential::mint(
        "retention-zone",
        "bench-region",
        chrono::Utc::now().timestamp_millis(),
    );
    regions[0]
        .coord
        .provision_agent_credential(&retention_token, "retention-zone", "bench-region")
        .await
        .unwrap();
    let old_result = CheckResult {
        check_id: sentinel,
        agent_id: "retention-sentinel".into(),
        zone: "retention-zone".into(),
        region: "bench-region".into(),
        kind: CheckKind::Http,
        timestamp: chrono::Utc::now() - chrono::Duration::days(2),
        latency_ms: 1,
        status_code: Some(200),
        body_matched: None,
        error: None,
        failure: None,
        body: None,
        headers: None,
        metric: None,
        metrics: None,
    };
    let response = client
        .post(format!("http://{}/api/v1/results", regions[0].addr))
        .bearer_auth(&retention_token)
        .json(&serde_json::json!({"results": [old_result]}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    regions[0].coord.spawn_retention();
    wait_until(
        "retention sentinel removal",
        Duration::from_secs(60),
        || {
            let coord = regions[0].coord.clone();
            async move {
                coord
                    .history(sentinel, 10, None, None, None)
                    .await
                    .unwrap()
                    .is_empty()
            }
        },
    )
    .await;

    // Convergence is every coordinator holding every expected (check, agent)
    // pair, not equality with one source's raw count (which can include extra
    // schedules while the agents are intentionally still running).
    for (target_index, region) in regions.iter().enumerate() {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let observed = coverage(&region.coord, &ids, &agent_ids, began).await;
            if observed.len() == agent_ids.len()
                && observed.iter().all(|(_, count)| *count == checks as u64)
            {
                break;
            }
            if Instant::now() >= deadline {
                temp.retain();
                let diagnostics = convergence_diagnostics(
                    &temp,
                    target_index,
                    &ids,
                    &agent_ids,
                    began.timestamp_millis(),
                )
                .unwrap_or_else(|error| format!("diagnostics unavailable: {error}"));
                if let Some(metrics) = &metrics {
                    println!("agent_benchmark_metrics {metrics:?}");
                }
                panic!(
                    "result replication missing at {}: {observed:?}\n{diagnostics}\ndb_dir={}",
                    region.addr,
                    temp.dir.display(),
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    temp.assert_below(max_disk);

    let read_counts = futures_util::future::join_all(readers)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    assert!(
        read_counts.iter().all(|count| *count > 0),
        "reader coverage missing: {read_counts:?}"
    );

    let mut samples = latencies.lock().clone();
    samples.sort_unstable();
    let percentile = |p: f64| samples[((samples.len() - 1) as f64 * p).round() as usize];
    let observation_elapsed = observation_started.elapsed().as_secs_f64();
    let read_elapsed = read_duration.as_secs_f64();
    let expected_results = checks * PEERS;
    if let Some(metrics) = &metrics {
        println!("agent_benchmark_metrics {metrics:?}");
    }
    println!(
        "release_benchmark checks={checks} probe_limit={probe_limit} cron={cron:?} anti_entropy={} expected_distinct_results={expected_results} result_rate_per_sec={:.2} global_checks={checks} read_requests={} read_rate_per_sec={:.2} p50_ms={} p95_ms={} db_dir={}",
        if anti_entropy { "on" } else { "off (cluster loops skipped)" },
        expected_results as f64 / observation_elapsed,
        samples.len(),
        samples.len() as f64 / read_elapsed,
        percentile(0.50).as_millis(),
        percentile(0.95).as_millis(),
        temp.dir.display(),
    );

    for agent in &agents {
        agent.abort();
    }
    for region in &regions {
        region.server.abort();
    }
    global_server.abort();
    probe_server.abort();
}
