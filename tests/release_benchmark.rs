//! Opt-in release-mode benchmark. It uses only localhost services and a unique
//! temporary DuckDB directory; run it explicitly with the command below.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use duckdb::Connection;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{routing::get, Router};
use lighthouse::agent;
use lighthouse::config::{
    AgentConfig, CheckConfig, CoordinatorConfig, GlobalConfig, GlobalPushConfig, RetentionConfig,
};
use lighthouse::coordinator::Coordinator;
use lighthouse::credential;
use lighthouse::global::GlobalNode;
use lighthouse::models::{CheckId, CheckKind, CheckResult};
use lighthouse::placement::Placement;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

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
            "lighthouse-release-bench-{}-{nonce}",
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

async fn status_page(
    client: &reqwest::Client,
    base: &str,
    page: usize,
    limit: usize,
) -> serde_json::Value {
    client
        .get(format!("{base}/api/v1/status?page={page}&limit={limit}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn status_page_summary(status: &serde_json::Value) -> String {
    format!(
        "total_checks={} page={} limit={} returned={} has_more={}",
        status["total_checks"],
        status["page"],
        status["limit"],
        status["checks"].as_array().map_or(0, Vec::len),
        status["has_more"],
    )
}

async fn wait_for_status_page(client: &reqwest::Client, base: &str, expected: usize, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last: String;
    loop {
        match client
            .get(format!("{base}/api/v1/status?page=0&limit=100"))
            .send()
            .await
        {
            Ok(response) => {
                let response_status = response.status();
                match response.json::<serde_json::Value>().await {
                    Ok(status) => {
                        last = format!(
                            "http_status={response_status} {}",
                            status_page_summary(&status)
                        );
                        if status["total_checks"] == expected
                            && status["page"] == 0
                            && status["limit"] == 100
                            && status["checks"]
                                .as_array()
                                .is_some_and(|rows| rows.len() == expected.min(100))
                            && status["has_more"] == (expected > 100)
                        {
                            return;
                        }
                    }
                    Err(error) => {
                        last = format!("http_status={response_status} decode_error={error}")
                    }
                }
            }
            Err(error) => last = format!("request_error={error}"),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {last}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn status_ids(client: &reqwest::Client, base: &str, expected: usize) -> HashSet<CheckId> {
    let mut ids = HashSet::with_capacity(expected);
    let mut page = 0;
    loop {
        let status = status_page(client, base, page, 1_000).await;
        assert_eq!(status["page"], page, "{}", status_page_summary(&status));
        assert_eq!(status["limit"], 1_000, "{}", status_page_summary(&status));
        assert_eq!(
            status["total_checks"],
            expected,
            "{}",
            status_page_summary(&status)
        );
        let checks = status["checks"].as_array().unwrap();
        assert!(
            !checks.is_empty() || !status["has_more"].as_bool().unwrap(),
            "{}",
            status_page_summary(&status)
        );
        for check in checks {
            ids.insert(check["check_id"].as_str().unwrap().parse().unwrap());
        }
        if !status["has_more"].as_bool().unwrap() {
            break;
        }
        page += 1;
    }
    ids
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
/// `LIGHTHOUSE_BENCH_CHECKS` (default 10000), `LIGHTHOUSE_BENCH_SECS` (45, maximum
/// 300), `LIGHTHOUSE_BENCH_CRON` (default every 30 seconds),
/// `LIGHTHOUSE_BENCH_MAX_DISK_MB` (512), and `LIGHTHOUSE_BENCH_ANTI_ENTROPY` (default
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
    let checks = env_usize("LIGHTHOUSE_BENCH_CHECKS", DEFAULT_CHECKS).clamp(100, 100_000);
    let seconds = env_secs("LIGHTHOUSE_BENCH_SECS", DEFAULT_SECS).clamp(1, 300);
    let cron = std::env::var("LIGHTHOUSE_BENCH_CRON").unwrap_or_else(|_| DEFAULT_CRON.into());
    let probe_limit = env_usize("LIGHTHOUSE_BENCH_MAX_IN_FLIGHT_PROBES", 128);
    assert!(probe_limit > 0, "probe concurrency must be positive");
    let anti_entropy = std::env::var("LIGHTHOUSE_BENCH_ANTI_ENTROPY")
        .map(|value| value != "off")
        .unwrap_or(true);
    let max_disk = env_secs("LIGHTHOUSE_BENCH_MAX_DISK_MB", DEFAULT_MAX_DISK_MB).clamp(32, 4_096)
        * 1024
        * 1024;
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
    let global_token = format!("wac_bench.{}", Uuid::now_v7().simple());
    let (global_addr, global_server) = serve(
        GlobalNode::new(&GlobalConfig {
            listen: "127.0.0.1:0".into(),
            provision: vec![lighthouse::config::ProvisionedCredential {
                zone: String::new(),
                region: "bench-region".into(),
                token: Some(global_token.clone()),
                token_env: None,
            }],
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
        wait_for_status_page(&client, &base, checks, "regional public check status").await;
        let found = status_ids(&client, &base, checks).await;
        assert_eq!(found, ids.iter().copied().collect());
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
        agents.push(tokio::spawn(agent::sync_loop(cfg.clone(), tx)));
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

    wait_for_status_page(&client, &global_base, checks, "global public check status").await;
    let global_ids = status_ids(&client, &global_base, checks).await;
    assert_eq!(global_ids, ids.iter().copied().collect());

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
                    body["total_checks"] == checks
                        && body["page"] == 0
                        && body["limit"] == 100
                        && body["checks"]
                            .as_array()
                            .is_some_and(|rows| rows.len() == checks.min(100))
                        && body["has_more"] == (checks > 100)
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

// ---------------------------------------------------------------------------
// Production-scale benchmarks with hard, env-overridable regression
// thresholds.
//
// The benchmark above measures the same production-cardinality shape (10,000
// checks, three live-probing peers) but only prints its numbers; nothing
// fails the run if a change regresses them. The five benchmarks below add the
// missing regression gate for the areas named in TODO.md's P2 "production
// operations and validation" item, each as its own focused, independently
// runnable `#[ignore]`d test rather than one further-overloaded scenario:
//
//   - `benchmark_sustained_ingest_throughput_threshold` — results/s.
//   - `benchmark_status_uptime_history_latency_query_thresholds` — public
//     status, 24h uptime, single-check history, and latency-points query
//     p50/p99, all measured against one shared 10,000-check/24h-history
//     fixture rather than four separate ones.
//   - `benchmark_anti_entropy_reconciliation_threshold` — wall-clock time for
//     a peer that joined after the fact to fully converge by anti-entropy
//     alone.
//   - `benchmark_retention_cleanup_threshold` — `prune_results` duration over
//     a realistic multi-day raw-result backlog.
//   - `benchmark_startup_recovery_threshold` — reopen duration for a
//     persistent, on-disk database sized like the fixture above.
//
// Every threshold is a hard default (see `assert_ceiling`/`assert_floor`
// below), not merely checked when a `LIGHTHOUSE_BENCH_*` variable happens to be
// set — a silent regression must not pass just because nobody configured a
// ceiling. Defaults are set at roughly 2x the number measured against this
// workstation (see SCALING.md's "10,000-check production gate" section for
// the measured numbers and the exact command used); override any of them to
// tune for different hardware or a stricter release gate.
//
// Run explicitly, one at a time or all together:
//   cargo test --release --test release_benchmark -- --ignored --nocapture

fn env_u128(name: &str, default: u128) -> u128 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `samples` sorted in place; the smallest value whose rank is at least `p`
/// percent of the way through the set. Same formula as
/// `coordinator::router::tests::percentile`, duplicated rather than shared
/// because that one is private to an in-crate `#[cfg(test)]` module an
/// external integration test cannot reach.
fn percentile(samples: &mut [u128], p: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() * p).div_ceil(100).saturating_sub(1)]
}

/// Asserts `actual` is at most a ceiling that defaults to `default` and can
/// be overridden via the environment variable `name`. Unlike
/// `coordinator::router::tests::assert_ceiling`, which only checks anything
/// when the variable happens to be set, this default is hard: a regression
/// fails the benchmark whether or not anyone configured a ceiling.
fn assert_ceiling(name: &str, default: u128, actual: u128) {
    let ceiling = env_u128(name, default);
    assert!(
        actual <= ceiling,
        "{name}: {actual} exceeded the ceiling of {ceiling} (default {default}; override via {name})"
    );
}

/// The throughput-side twin of `assert_ceiling`: `actual` must be at least a
/// floor that defaults to `default` and can be overridden via `name`.
fn assert_floor(name: &str, default: u128, actual: u128) {
    let floor = env_u128(name, default);
    assert!(
        actual >= floor,
        "{name}: {actual} fell below the floor of {floor} (default {default}; override via {name})"
    );
}

/// A check with no placement restriction and no real probe target, for the
/// single-coordinator benchmarks below — unlike `benchmark_check`, which is
/// shaped for the three-zone live-agent scenario above, these seed results
/// directly through `Coordinator::ingest` and never actually probe anything.
fn bench_check(id: CheckId, number: usize) -> CheckConfig {
    CheckConfig {
        id,
        name: format!("bench-check-{number}"),
        kind: CheckKind::Http,
        target: "https://example.invalid".into(),
        cron: DEFAULT_CRON.into(),
        timeout_secs: 5,
        latency_graph: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn status_pagination_covers_over_100_checks_regionally_and_globally() {
    let checks = 101;
    let token = "wac_pagination.global-key";
    let coord = Coordinator::new(&CoordinatorConfig {
        db_path: ":memory:".into(),
        region: "status-pagination-region".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let ids = seed_checks(&coord, checks).await;
    let (regional_addr, regional_server) = serve(coord.router()).await;
    let (global_addr, global_server) = serve(
        GlobalNode::new(&GlobalConfig {
            provision: vec![lighthouse::config::ProvisionedCredential {
                zone: String::new(),
                region: "status-pagination-region".into(),
                token: Some(token.into()),
                token_env: None,
            }],
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .router(),
    )
    .await;
    let client = reqwest::Client::new();
    let regional_base = format!("http://{regional_addr}");
    let global_base = format!("http://{global_addr}");
    wait_for_status_page(&client, &regional_base, checks, "regional paginated status").await;
    let regional_ids = status_ids(&client, &regional_base, checks).await;
    assert_eq!(regional_ids, ids.iter().copied().collect());

    let now = chrono::Utc::now().timestamp_millis();
    let push = lighthouse::global::wire::RegionStatePush {
        region: "status-pagination-region".into(),
        origin: "status-pagination-node".into(),
        advertise_url: Some(regional_base),
        pushed_at: now,
        region_health: lighthouse::models::NodeState::Unknown,
        agents_healthy: 0,
        agents_total: 0,
        checks: ids
            .iter()
            .enumerate()
            .map(
                |(number, check_id)| lighthouse::global::wire::CheckSummary {
                    check_id: *check_id,
                    name: format!("bench-check-{number}"),
                    enabled: true,
                    state: lighthouse::models::NodeState::Unknown,
                    latency_ms: 0,
                    last_check: 0,
                    uptime_24h: None,
                    status_code: None,
                    error: None,
                    kind: Some(CheckKind::Http),
                    group: None,
                    latency_graph: true,
                    position: number as i32,
                    depends_on: Vec::new(),
                },
            )
            .collect(),
        epoch: 1,
        seq: 1,
        delta_base: None,
        removed: Vec::new(),
    };
    let response = client
        .post(format!("{global_base}/internal/global/state"))
        .bearer_auth(token)
        .json(&push)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    wait_for_status_page(&client, &global_base, checks, "global paginated status").await;
    let global_ids = status_ids(&client, &global_base, checks).await;
    assert_eq!(global_ids, ids.into_iter().collect());
    regional_server.abort();
    global_server.abort();
}

fn bench_result(id: CheckId, ts_millis: i64) -> CheckResult {
    CheckResult {
        check_id: id,
        agent_id: "bench-agent".into(),
        zone: "bench-zone".into(),
        region: "bench-region".into(),
        kind: CheckKind::Http,
        timestamp: chrono::DateTime::from_timestamp_millis(ts_millis).unwrap(),
        latency_ms: 12,
        status_code: Some(200),
        body_matched: None,
        error: None,
        failure: None,
        body: None,
        headers: None,
        metric: None,
        metrics: None,
    }
}

/// Create `checks` checks through the same batched HLC/config path
/// `create_checks_for_test` already uses in the benchmark above, then wait
/// for every one of them to be locally queryable before returning — a
/// caller measuring what happens *after* the catalogue exists must not
/// accidentally include provisioning time in the number.
async fn seed_checks(coord: &Coordinator, checks: usize) -> Vec<CheckId> {
    let ids: Vec<CheckId> = (0..checks).map(|_| Uuid::now_v7()).collect();
    coord
        .create_checks_for_test(
            ids.iter()
                .enumerate()
                .map(|(number, id)| bench_check(*id, number))
                .collect(),
        )
        .await
        .unwrap();
    wait_until(
        "seeded checks to be locally queryable",
        Duration::from_secs(60),
        || {
            let coord = coord.clone();
            async move { coord.check_count_for_test() == checks }
        },
    )
    .await;
    ids
}

/// Sustained result ingest throughput, in results/s, against
/// `Coordinator::ingest` directly — the same call the HTTP result-ingest
/// route makes once a caller's credential has checked out, so this measures
/// the coordinator's own ingest cost without also paying for `reqwest`
/// round trips on both sides.
///
/// `LIGHTHOUSE_BENCH_CHECKS` (default 10,000), `LIGHTHOUSE_BENCH_INGEST_SECS`
/// (default 10, so this stays fast enough to run alongside the others),
/// `LIGHTHOUSE_BENCH_MIN_INGEST_RESULTS_PER_SEC` (hard floor).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "production-scale benchmark; run explicitly with cargo test --release --test release_benchmark -- --ignored --nocapture"]
async fn benchmark_sustained_ingest_throughput_threshold() {
    let checks = env_usize("LIGHTHOUSE_BENCH_CHECKS", DEFAULT_CHECKS).clamp(100, 100_000);
    let seconds = env_secs("LIGHTHOUSE_BENCH_INGEST_SECS", 10).clamp(1, 120);
    let coord = Coordinator::new(&CoordinatorConfig {
        db_path: ":memory:".into(),
        region: "bench-region".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let ids = seed_checks(&coord, checks).await;

    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut round: i64 = 0;
    let mut total_results: u64 = 0;
    let base_ts = chrono::Utc::now().timestamp_millis();
    let started = Instant::now();
    while Instant::now() < deadline {
        let ts = base_ts + round;
        let batch: Vec<CheckResult> = ids.iter().map(|id| bench_result(*id, ts)).collect();
        total_results += batch.len() as u64;
        coord.ingest(batch).await;
        round += 1;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let rate = total_results as f64 / elapsed;
    // ~2x below the ~29,800 results/s measured at 10,000 checks in
    // SCALING.md's "10,000-check production gate" section.
    assert_floor(
        "LIGHTHOUSE_BENCH_MIN_INGEST_RESULTS_PER_SEC",
        14_000,
        rate.floor() as u128,
    );
    println!(
        "benchmark_sustained_ingest checks={checks} rounds={round} total_results={total_results} elapsed_s={elapsed:.2} results_per_sec={rate:.2}"
    );
}

/// Public status, 24h uptime, single-check history, and latency-points query
/// latency, all over real HTTP against one shared 10,000-check fixture with
/// 24h of history per check.
///
/// `LIGHTHOUSE_BENCH_CHECKS` (default 10,000), `LIGHTHOUSE_BENCH_HISTORY_PER_CHECK`
/// (default 12, i.e. one observation every two hours), `LIGHTHOUSE_BENCH_QUERY_SAMPLES`
/// (default 50), and one `LIGHTHOUSE_BENCH_MAX_*_P50_MS` / `_P99_MS` pair per
/// endpoint (hard ceilings, all overridable).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "production-scale benchmark; run explicitly with cargo test --release --test release_benchmark -- --ignored --nocapture"]
async fn benchmark_status_uptime_history_latency_query_thresholds() {
    let checks = env_usize("LIGHTHOUSE_BENCH_CHECKS", DEFAULT_CHECKS).clamp(100, 100_000);
    let history_per_check = env_usize("LIGHTHOUSE_BENCH_HISTORY_PER_CHECK", 12).clamp(1, 1_000);
    let samples = env_usize("LIGHTHOUSE_BENCH_QUERY_SAMPLES", 50).clamp(1, 1_000);
    let coord = Coordinator::new(&CoordinatorConfig {
        db_path: ":memory:".into(),
        region: "bench-region".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let ids = seed_checks(&coord, checks).await;

    let now = chrono::Utc::now().timestamp_millis();
    let day_ms = 24 * 60 * 60 * 1000;
    let step = day_ms / history_per_check as i64;
    // Oldest first, so the last result ingested per check — the one that
    // decides its published current state — is also the most recent.
    for sample in 0..history_per_check {
        let ts = now - day_ms + (sample as i64 + 1) * step;
        let batch: Vec<CheckResult> = ids.iter().map(|id| bench_result(*id, ts)).collect();
        coord.ingest(batch).await;
    }

    let (addr, server) = serve(coord.clone().router()).await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    wait_for_status_page(
        &client,
        &base,
        checks,
        "seeded history to publish on the status page",
    )
    .await;

    async fn sample_ms(client: &reqwest::Client, url: &str, samples: usize) -> (u128, u128) {
        // One unmeasured warm-up request, matching
        // `benchmark_status_and_global_push_production_cardinality`: the
        // first hit after ingest pays for whatever caches or query plans are
        // not yet warm, which is not the steady-state cost this measures.
        let warm = client.get(url).send().await.unwrap();
        assert!(warm.status().is_success(), "{url}: {}", warm.status());
        let mut ms = Vec::with_capacity(samples);
        for _ in 0..samples {
            let started = Instant::now();
            let response = client.get(url).send().await.unwrap();
            assert!(
                response.status().is_success(),
                "{url}: {}",
                response.status()
            );
            let _ = response.bytes().await.unwrap();
            ms.push(started.elapsed().as_millis());
        }
        (percentile(&mut ms, 50), percentile(&mut ms, 99))
    }

    let status_url = format!("{base}/api/v1/status");
    let uptime_url = format!("{base}/api/v1/uptime?days=1&buckets=24");
    let history_url = format!("{base}/api/v1/checks/{}/history?limit=100", ids[0]);
    let latency_url = format!("{base}/api/v1/latency-points?hours=24&buckets=24");

    let (status_p50, status_p99) = sample_ms(&client, &status_url, samples).await;
    let (uptime_p50, uptime_p99) = sample_ms(&client, &uptime_url, samples).await;
    let (history_p50, history_p99) = sample_ms(&client, &history_url, samples).await;
    let (latency_p50, latency_p99) = sample_ms(&client, &latency_url, samples).await;

    // Defaults are ~2x what this benchmark measured at 10,000 checks / 24h
    // history on the machine SCALING.md's numbers were taken on; see its
    // "10,000-check production gate" section for the measured values.
    assert_ceiling("LIGHTHOUSE_BENCH_MAX_STATUS_P50_MS", 120, status_p50);
    assert_ceiling("LIGHTHOUSE_BENCH_MAX_STATUS_P99_MS", 150, status_p99);
    assert_ceiling("LIGHTHOUSE_BENCH_MAX_UPTIME_P50_MS", 80, uptime_p50);
    assert_ceiling("LIGHTHOUSE_BENCH_MAX_UPTIME_P99_MS", 120, uptime_p99);
    assert_ceiling("LIGHTHOUSE_BENCH_MAX_HISTORY_P50_MS", 20, history_p50);
    assert_ceiling("LIGHTHOUSE_BENCH_MAX_HISTORY_P99_MS", 30, history_p99);
    assert_ceiling(
        "LIGHTHOUSE_BENCH_MAX_LATENCY_POINTS_P50_MS",
        100,
        latency_p50,
    );
    assert_ceiling(
        "LIGHTHOUSE_BENCH_MAX_LATENCY_POINTS_P99_MS",
        150,
        latency_p99,
    );

    println!(
        "benchmark_query_latency checks={checks} history_per_check={history_per_check} samples={samples} status_p50_ms={status_p50} status_p99_ms={status_p99} uptime_p50_ms={uptime_p50} uptime_p99_ms={uptime_p99} history_p50_ms={history_p50} history_p99_ms={history_p99} latency_points_p50_ms={latency_p50} latency_points_p99_ms={latency_p99}"
    );
    server.abort();
}

/// Wall-clock time for a peer that missed a coordinator's whole result
/// history to fully converge by anti-entropy alone — the same recovery path
/// `cluster.rs::anti_entropy_recovers_a_complete_missed_incident_lifecycle`
/// and `chaos.rs`'s peer-unavailable scenarios exercise for correctness;
/// this measures its cost at production cardinality instead.
///
/// `LIGHTHOUSE_BENCH_CHECKS` (default 10,000), `LIGHTHOUSE_BENCH_ANTI_ENTROPY_MAX_SECS`
/// (hard ceiling on total sweep time to full convergence).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "production-scale benchmark; run explicitly with cargo test --release --test release_benchmark -- --ignored --nocapture"]
async fn benchmark_anti_entropy_reconciliation_threshold() {
    let checks = env_usize("LIGHTHOUSE_BENCH_CHECKS", DEFAULT_CHECKS).clamp(100, 100_000);
    let peer_token = "bench-anti-entropy-peer-token";

    async fn node(peer_token: &str, peers: Vec<String>) -> (Coordinator, SocketAddr) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let coord = Coordinator::new(&CoordinatorConfig {
            listen: "127.0.0.1:0".into(),
            db_path: ":memory:".into(),
            peer_token: Some(peer_token.into()),
            advertise_url: Some(format!("http://{addr}")),
            region: "bench-region".into(),
            peers,
            peer_timeout_secs: 7_200,
            ..Default::default()
        })
        .await
        .unwrap();
        let app = coord
            .clone()
            .router()
            .into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (coord, addr)
    }

    let (source, source_addr) = node(peer_token, vec![]).await;
    let ids = seed_checks(&source, checks).await;
    let ts = chrono::Utc::now().timestamp_millis();
    source
        .ingest(ids.iter().map(|id| bench_result(*id, ts)).collect())
        .await;

    // The target joins only after the source already holds the whole
    // catalogue and its results — a late joiner, exactly like
    // `cluster.rs::a_late_joiner_backfills_history_via_digest_and_backfill`
    // — so nothing but anti-entropy can catch it up.
    let (target, target_addr) = node(peer_token, vec![format!("http://{source_addr}")]).await;
    // A real push-pull gossip round, as `cluster.rs::gossip_step` and
    // `chaos.rs::gossip_step` both do: the target's view goes to the source
    // (which merges it and replies with its own), and that reply feeds back
    // into the target — anti-entropy needs the target to know the source is
    // live, not just the reverse.
    async fn ping(addr: SocketAddr, token: &str, body: &serde_json::Value) -> serde_json::Value {
        let response = reqwest::Client::new()
            .post(format!("http://{addr}/internal/cluster/ping"))
            .bearer_auth(token)
            .json(body)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        response.json().await.unwrap()
    }
    let target_ping = serde_json::json!({
        "node_id": "bench-target", "url": format!("http://{target_addr}"), "generation": 1, "members": [],
    });
    let source_view = ping(source_addr, peer_token, &target_ping).await;
    ping(target_addr, peer_token, &source_view).await;

    let started = Instant::now();
    loop {
        target.anti_entropy_sweep().await;
        if target.check_count_for_test() == checks {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "anti-entropy never converged the target's catalogue within 5 minutes"
        );
    }
    let catalogue_converged = started.elapsed();
    // Checked through `result_coverage` (the same call the giant benchmark
    // above polls for its own convergence) rather than the public status
    // page: that page is bounded to `crate::page::MAX_PAGE_SIZE`, so an
    // array-length check against a `checks` count above the page size would
    // never converge regardless of what anti-entropy actually did.
    let began = chrono::DateTime::from_timestamp_millis(ts).unwrap();
    let agent = vec!["bench-agent".to_string()];
    loop {
        target.anti_entropy_sweep().await;
        let observed = coverage(&target, &ids, &agent, began).await;
        if observed == vec![("bench-agent".to_string(), checks as u64)] {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "anti-entropy never converged the target's results within 5 minutes: {observed:?}"
        );
    }
    let elapsed = started.elapsed();
    // ~2x the measured 61s full catalogue-plus-results convergence for a
    // 10,000-check late joiner (SCALING.md's "10,000-check production gate").
    assert_ceiling(
        "LIGHTHOUSE_BENCH_ANTI_ENTROPY_MAX_SECS",
        130,
        elapsed.as_secs() as u128,
    );
    println!(
        "benchmark_anti_entropy_reconciliation checks={checks} catalogue_converged_ms={} full_converged_ms={}",
        catalogue_converged.as_millis(),
        elapsed.as_millis()
    );
}

/// `Storage::prune_results` duration (via the `prune_results_for_test` hook
/// every restart/recovery test already uses) over a realistic multi-day raw
/// backlog: `LIGHTHOUSE_BENCH_CHECKS` (default 10,000) checks, each with
/// `LIGHTHOUSE_BENCH_RETENTION_DAYS` (default 3) days of hourly results, pruning
/// everything older than one day.
///
/// `LIGHTHOUSE_BENCH_RETENTION_MAX_MS` is the hard ceiling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "production-scale benchmark; run explicitly with cargo test --release --test release_benchmark -- --ignored --nocapture"]
async fn benchmark_retention_cleanup_threshold() {
    let checks = env_usize("LIGHTHOUSE_BENCH_CHECKS", DEFAULT_CHECKS).clamp(100, 100_000);
    let days = env_usize("LIGHTHOUSE_BENCH_RETENTION_DAYS", 3).clamp(1, 30);
    let coord = Coordinator::new(&CoordinatorConfig {
        db_path: ":memory:".into(),
        region: "bench-region".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let ids = seed_checks(&coord, checks).await;

    let now = chrono::Utc::now().timestamp_millis();
    let hour_ms = 60 * 60 * 1000;
    let hours = days * 24;
    let cutoff = now - hour_ms; // keep only the most recent hour
    for hour in 0..hours {
        let ts = now - (hours - hour) as i64 * hour_ms;
        let batch: Vec<CheckResult> = ids.iter().map(|id| bench_result(*id, ts)).collect();
        coord.ingest(batch).await;
    }
    let seeded_rows = checks as u64 * hours as u64;

    let started = Instant::now();
    let removed = coord.prune_results_for_test(cutoff).await.unwrap();
    let elapsed = started.elapsed();
    // ~5x (not 2x) the measured 80ms: a raw-result delete's cost is I/O-bound
    // and the least alike-hardware-portable of these numbers.
    assert_ceiling(
        "LIGHTHOUSE_BENCH_RETENTION_MAX_MS",
        400,
        elapsed.as_millis(),
    );
    assert!(
        removed > 0 && removed < seeded_rows,
        "the prune should have removed the seeded backlog but kept the most recent hour: \
         removed={removed} seeded={seeded_rows}"
    );
    println!(
        "benchmark_retention_cleanup checks={checks} days={days} seeded_rows={seeded_rows} removed_rows={removed} elapsed_ms={}",
        elapsed.as_millis()
    );
}

/// Reopen duration for a persistent, on-disk database sized like the query
/// fixture above (10,000 checks x 24h of history by default) — the
/// integration-level, HTTP-serving analog of
/// `storage::results::tests::restart_recovers_committed_rows_and_backup_restore_preserves_them`,
/// which measures the same WAL-replay boundary directly against `Storage`
/// without a `Coordinator` or router around it.
///
/// `LIGHTHOUSE_BENCH_CHECKS` (default 10,000), `LIGHTHOUSE_BENCH_HISTORY_PER_CHECK`
/// (default 12), `LIGHTHOUSE_BENCH_STARTUP_MAX_MS` (hard ceiling).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "production-scale benchmark; run explicitly with cargo test --release --test release_benchmark -- --ignored --nocapture"]
async fn benchmark_startup_recovery_threshold() {
    let checks = env_usize("LIGHTHOUSE_BENCH_CHECKS", DEFAULT_CHECKS).clamp(100, 100_000);
    let history_per_check = env_usize("LIGHTHOUSE_BENCH_HISTORY_PER_CHECK", 12).clamp(1, 1_000);
    let temp = TempDb::new();
    let path = temp.path("startup-recovery");

    let coord = Coordinator::new(&CoordinatorConfig {
        db_path: path.clone(),
        region: "bench-region".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let ids = seed_checks(&coord, checks).await;
    let now = chrono::Utc::now().timestamp_millis();
    let day_ms = 24 * 60 * 60 * 1000;
    let step = day_ms / history_per_check as i64;
    for sample in 0..history_per_check {
        let ts = now - day_ms + (sample as i64 + 1) * step;
        let batch: Vec<CheckResult> = ids.iter().map(|id| bench_result(*id, ts)).collect();
        coord.ingest(batch).await;
    }
    let seeded_rows = checks as u64 * history_per_check as u64;

    // Model a process restart rather than a clean shutdown: drop the handle
    // without an orderly close, the same crash boundary
    // `transactional_logical.rs::close` and the `chaos.rs` restart test use.
    drop(coord);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = Instant::now();
    let reopened = Coordinator::new(&CoordinatorConfig {
        db_path: path,
        region: "bench-region".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(
        reopened.check_count_for_test(),
        checks,
        "the reopened catalogue must match what was seeded before the restart"
    );
    // Set at ~2x the 9.6s reopen first measured at 10,000 checks / 24h history.
    // That time was mostly the per-check state-history baseline in
    // `Coordinator::new`, not WAL replay; it is one statement now and the
    // reopen measures ~0.3s (SCALING.md's "DuckDB 2.0" section).
    assert_ceiling(
        "LIGHTHOUSE_BENCH_STARTUP_MAX_MS",
        20_000,
        elapsed.as_millis(),
    );
    println!(
        "benchmark_startup_recovery checks={checks} history_per_check={history_per_check} seeded_rows={seeded_rows} reopen_ms={}",
        elapsed.as_millis()
    );
    drop(reopened);
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(temp);
}
