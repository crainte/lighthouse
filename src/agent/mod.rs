mod dns;
mod http;
mod icmp;
mod ntp;
mod openmetrics;
mod ssl;
mod tcp;

use crate::config::{AgentConfig, CheckConfig};
use crate::http::ZstdJson;
use crate::models::{CheckId, CheckKind, CheckResult, ResultBatch, Status};
use anyhow::{Context, Result};
use chrono::Utc;
use hickory_resolver::config::{NameServerConfig, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::{Resolver, TokioResolver};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

/// What a probe observed: an optional HTTP status code, whether the configured
/// content expectation matched, and an error string when the probe failed.
/// `body`/`headers` are populated only for checks carrying a Lua script.
/// `metric`/`metrics` are populated only for OpenMetrics checks, mirroring the
/// same transient-only treatment as `body`.
#[derive(Default)]
struct Observation {
    status_code: Option<u16>,
    body_matched: Option<bool>,
    error: Option<String>,
    failure: Option<crate::models::FailureKind>,
    body: Option<String>,
    headers: Option<std::collections::HashMap<String, String>>,
    metric: Option<f64>,
    metrics: Option<Vec<crate::openmetrics::MetricSample>>,
}

impl Observation {
    /// A failure always carries its cause: the probe knows it here, and
    /// nothing downstream can recover it from the text without guessing.
    fn failed(kind: crate::models::FailureKind, msg: impl Into<String>) -> Self {
        Self {
            error: Some(msg.into()),
            failure: Some(kind),
            ..Default::default()
        }
    }
    fn ok() -> Self {
        Self::default()
    }
}

/// Who observed a result and from where. Carried on every probe so an outage
/// can be attributed to a place, not merely to a process: "slow" and "slow from
/// Frankfurt" are different findings, and only the agent knows which it is.
pub struct Origin<'a> {
    pub agent_id: &'a str,
    pub zone: &'a str,
    pub region: &'a str,
}

/// Owned form, because each probe loop is spawned and outlives the config
/// borrow it was built from.
struct OwnedOrigin {
    agent_id: String,
    zone: String,
    region: String,
}

impl OwnedOrigin {
    fn from(cfg: &AgentConfig) -> Self {
        Self {
            agent_id: cfg.agent_id.clone(),
            zone: cfg.zone.clone(),
            region: cfg.region.clone(),
        }
    }

    fn borrow(&self) -> Origin<'_> {
        Origin {
            agent_id: &self.agent_id,
            zone: &self.zone,
            region: &self.region,
        }
    }
}

/// Run one check, producing a raw CheckResult.
/// The agent only reports observations — the coordinator derives UP/DOWN.
///
/// A check may carry several hosts, combined by `host_match`:
/// `all` means every host must pass (a set of things that should each be up),
/// `any` means one passing is enough (a redundant pool). Every host is probed
/// either way, so the error names all of what actually failed rather than
/// stopping at the first.
pub async fn execute_check(probes: &Probes, who: &Origin<'_>, check: &CheckConfig) -> CheckResult {
    let start = Instant::now();
    let targets = check.all_targets();
    let single = targets.len() <= 1;

    let mut passed = 0usize;
    let mut failures: Vec<String> = Vec::new();
    let mut failure_kind: Option<crate::models::FailureKind> = None;
    let mut last = Observation::ok();
    for target in &targets {
        let obs = probe_one(probes, check, target).await;
        match &obs.error {
            None => passed += 1,
            Some(e) => {
                failure_kind = failure_kind.or(obs.failure);
                failures.push(if single {
                    e.clone()
                } else {
                    // Say which host failed; a pool check reporting a bare error
                    // gives no indication of where to look.
                    format!("{target}: {e}")
                })
            }
        }
        last = obs;
    }

    let satisfied = check.host_match.satisfied(passed, targets.len());
    let (error, failure) = if satisfied {
        (None, None)
    } else if failures.is_empty() {
        (
            Some("no hosts to probe".to_string()),
            Some(crate::models::FailureKind::Other),
        )
    } else {
        (
            Some(failures.join("; ")),
            failure_kind.or(Some(crate::models::FailureKind::Other)),
        )
    };

    CheckResult {
        check_id: check.id,
        agent_id: who.agent_id.to_string(),
        zone: who.zone.to_string(),
        region: who.region.to_string(),
        kind: check.kind,
        timestamp: Utc::now(),
        latency_ms: start.elapsed().as_millis() as u64,
        status_code: last.status_code,
        body_matched: last.body_matched,
        error,
        failure,
        body: last.body,
        headers: last.headers,
        metric: last.metric,
        metrics: last.metrics,
    }
}

async fn probe_one(probes: &Probes, check: &CheckConfig, target: &str) -> Observation {
    match check.kind {
        CheckKind::Http => http::probe_http(&probes.http, check, target).await,
        CheckKind::Tcp => tcp::probe_tcp(check, target).await,
        CheckKind::Ntp => ntp::probe_ntp(check, target).await,
        CheckKind::Dns => {
            dns::probe_dns(&probes.resolver, &probes.system_nameservers, check, target).await
        }
        CheckKind::Icmp => icmp::probe_icmp(&probes.resolver, check, target).await,
        CheckKind::Ssl => ssl::probe_ssl(check, target).await,
        // Each OpenMetrics host is scraped and selected independently, exactly
        // like every other probe kind here; `execute_check` combines the
        // per-target outcomes with `host_match`. That means a metric missing
        // on one host of a multi-host check surfaces as a failure for that
        // host rather than being masked by whichever target happened to run
        // last — the same reasoning that already applies to HTTP/TCP/DNS.
        CheckKind::Openmetrics => openmetrics::probe_openmetrics(&probes.http, check, target).await,
        // Nothing to probe: the coordinator decides a heartbeat from when it was
        // last pinged, so an agent should never have been handed one.
        CheckKind::Heartbeat => Observation::failed(
            crate::models::FailureKind::Other,
            "heartbeat checks are not agent-probed",
        ),
    }
}

/// Shared, cheaply-cloned probe dependencies. Built once per process:
/// the HTTP client pools connections and the DNS resolver is `Arc`-backed
/// internally, so cloning per check loop shares one instance.
#[derive(Clone)]
pub struct Probes {
    http: reqwest::Client,
    resolver: TokioResolver,
    /// The servers `resolver` was built from, in order. DNSSEC checks speak to
    /// them directly, because they must read the AD bit off a raw response.
    system_nameservers: Arc<[std::net::SocketAddr]>,
}

pub(super) fn resolver_for(addr: std::net::SocketAddr) -> Result<TokioResolver, String> {
    let mut server = NameServerConfig::udp_and_tcp(addr.ip());
    for connection in &mut server.connections {
        connection.port = addr.port();
    }
    let config = ResolverConfig::from_parts(None, Vec::new(), vec![server]);
    let mut opts = ResolverOpts::default();
    opts.cache_size = 0;
    opts.attempts = 2;
    opts.timeout = Duration::from_secs(3);
    opts.try_tcp_on_error = true;
    Resolver::builder_with_config(config, TokioRuntimeProvider::default())
        .with_options(opts)
        .build()
        .map_err(|e| e.to_string())
}

/// One address per configured server, in resolv.conf order, deduplicated —
/// hickory lists a server once per transport, both on the same port.
fn nameserver_addrs(config: &ResolverConfig) -> Arc<[std::net::SocketAddr]> {
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();
    for server in config.name_servers() {
        let port = server.connections.first().map_or(53, |c| c.port);
        let addr = std::net::SocketAddr::new(server.ip, port);
        if !addrs.contains(&addr) {
            addrs.push(addr);
        }
    }
    addrs.into()
}

impl Probes {
    /// Build the shared probe clients.
    ///
    /// DNS is configured for monitoring rather than for speed-at-any-cost:
    /// caching is off so a probe can never report a stale success during a real
    /// outage, and a dropped UDP packet is retried instead of being reported as
    /// an outage. NXDOMAIN is a real answer and is deliberately not retried.
    pub fn new() -> Result<Self> {
        const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);

        let mut opts = ResolverOpts::default();
        opts.cache_size = 0; // never serve a cached answer
        opts.attempts = 2; // 2 retries => 3 sends, worst case 9s
        opts.timeout = ATTEMPT_TIMEOUT; // per attempt, and the pool-loop deadline
        opts.try_tcp_on_error = true; // fall back to TCP on loss/truncation

        // Prefer the host's resolvers; fall back to public ones when
        // /etc/resolv.conf is missing or empty (possible in minimal containers),
        // which would otherwise fail resolver construction outright.
        let config = match hickory_resolver::system_conf::read_system_conf() {
            Ok((config, _)) if !config.name_servers().is_empty() => config,
            Ok(_) | Err(_) => {
                warn!("no system resolver config, falling back to public DNS");
                let servers = ["1.1.1.1", "8.8.8.8"]
                    .iter()
                    .map(|ip| NameServerConfig::udp_and_tcp(ip.parse().expect("literal IP")))
                    .collect();
                ResolverConfig::from_parts(None, Vec::new(), servers)
            }
        };
        let system_nameservers = nameserver_addrs(&config);
        let resolver = Resolver::builder_with_config(config, TokioRuntimeProvider::default())
            .with_options(opts)
            .build()
            .context("building DNS resolver for checks")?;

        Ok(Self {
            http: reqwest::Client::new(),
            resolver,
            system_nameservers,
        })
    }
}

/// A bounded, opt-in aggregate for the release benchmark. No samples, check
/// labels, or metric registry are retained; production avoids every atomic
/// update unless `LIGHTHOUSE_BENCH_AGENT_METRICS=1` was set before first use.
#[derive(Debug, Default)]
pub struct AgentBenchmarkMetrics {
    pub scheduled: AtomicU64,
    pub schedule_lag_ns: AtomicU64,
    pub schedule_lag_max_ns: AtomicU64,
    pub permit_wait_ns: AtomicU64,
    pub permit_wait_max_ns: AtomicU64,
    pub execute_ns: AtomicU64,
    pub execute_max_ns: AtomicU64,
    pub in_flight: AtomicU64,
    pub in_flight_max: AtomicU64,
    pub queued: AtomicU64,
    pub queued_max: AtomicU64,
    pub post_batches: AtomicU64,
    pub post_results: AtomicU64,
    pub post_batch_max: AtomicU64,
    pub post_latency_ns: AtomicU64,
    pub post_latency_max_ns: AtomicU64,
    pub post_queue_max: AtomicU64,
    pub dropped_results: AtomicU64,
    pub post_success: AtomicU64,
    pub post_client_errors: AtomicU64,
    pub post_server_errors: AtomicU64,
    pub post_transport_errors: AtomicU64,
}

impl AgentBenchmarkMetrics {
    fn observe_max(value: &AtomicU64, sample: u64) {
        value.fetch_max(sample, Ordering::Relaxed);
    }

    fn observe_duration(total: &AtomicU64, max: &AtomicU64, elapsed: Duration) {
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        total.fetch_add(nanos, Ordering::Relaxed);
        Self::observe_max(max, nanos);
    }

    fn observe_queue(&self, len: usize) {
        Self::observe_max(&self.post_queue_max, len as u64);
    }
}

static BENCHMARK_METRICS: OnceLock<Option<Arc<AgentBenchmarkMetrics>>> = OnceLock::new();

/// Return aggregate agent benchmark counters when explicitly enabled by
/// `LIGHTHOUSE_BENCH_AGENT_METRICS=1`. The returned counters are process-wide and
/// bounded; benchmark callers may clear them with [`reset_benchmark_metrics`].
pub fn benchmark_metrics() -> Option<Arc<AgentBenchmarkMetrics>> {
    BENCHMARK_METRICS
        .get_or_init(|| {
            (std::env::var("LIGHTHOUSE_BENCH_AGENT_METRICS")
                .ok()
                .as_deref()
                == Some("1"))
            .then(|| Arc::new(AgentBenchmarkMetrics::default()))
        })
        .clone()
}

/// Clear the opt-in benchmark aggregate without allocating it when disabled.
pub fn reset_benchmark_metrics() {
    let Some(Some(metrics)) = BENCHMARK_METRICS.get() else {
        return;
    };
    for counter in [
        &metrics.scheduled,
        &metrics.schedule_lag_ns,
        &metrics.schedule_lag_max_ns,
        &metrics.permit_wait_ns,
        &metrics.permit_wait_max_ns,
        &metrics.execute_ns,
        &metrics.execute_max_ns,
        &metrics.in_flight,
        &metrics.in_flight_max,
        &metrics.queued,
        &metrics.queued_max,
        &metrics.post_batches,
        &metrics.post_results,
        &metrics.post_batch_max,
        &metrics.post_latency_ns,
        &metrics.post_latency_max_ns,
        &metrics.post_queue_max,
        &metrics.dropped_results,
        &metrics.post_success,
        &metrics.post_client_errors,
        &metrics.post_server_errors,
        &metrics.post_transport_errors,
    ] {
        counter.store(0, Ordering::Relaxed);
    }
}

/// Wait for the agent-wide probe budget immediately before a probe executes.
/// Tokio's semaphore is fair: a check already queued gets a permit before later
/// arrivals, preventing a busy schedule from starving an older wake-up.
async fn acquire_probe_permit(
    permits: Arc<Semaphore>,
    metrics: Option<&AgentBenchmarkMetrics>,
) -> OwnedSemaphorePermit {
    let started = metrics.map(|_| Instant::now());
    if let Some(metrics) = metrics {
        let queued = metrics.queued.fetch_add(1, Ordering::Relaxed) + 1;
        AgentBenchmarkMetrics::observe_max(&metrics.queued_max, queued);
    }
    let permit = permits
        .acquire_owned()
        .await
        .expect("probe semaphore is held by check loops");
    if let Some(metrics) = metrics {
        metrics.queued.fetch_sub(1, Ordering::Relaxed);
        AgentBenchmarkMetrics::observe_duration(
            &metrics.permit_wait_ns,
            &metrics.permit_wait_max_ns,
            started.expect("metrics start time").elapsed(),
        );
    }
    permit
}

/// Spawn one loop per check, each on its own interval so a slow check never
/// delays a sibling. Results flow into `tx`. Handles are returned so a caller
/// that re-syncs its check list can replace the whole set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeCadence {
    Scheduled,
    Retry,
    Down,
}

fn scheduling_status(check: &CheckConfig, result: &CheckResult) -> Status {
    let Some(source) = check.eval.as_deref().filter(|s| !s.trim().is_empty()) else {
        return crate::models::eval(result, check.expect.status);
    };
    let input = crate::script::EvalInput {
        status: result.status_code.unwrap_or(0),
        latency_ms: result.latency_ms,
        body: result.body.clone(),
        headers: result.headers.clone().unwrap_or_default(),
        metric: result.metric,
        metrics: result.metrics.clone().unwrap_or_default(),
    };
    match crate::script::evaluate(source, &input) {
        Ok(outcome) if outcome.up => Status::Up,
        Ok(_) => Status::Down,
        Err(_) => Status::Unknown,
    }
}

/// Consecutive failures per check, owned by `sync_loop` and shared with every
/// check loop. A respawn (any check-list change) or a restarted agent's
/// coordinator hint resumes retry/down cadence from here instead of falling
/// back to the normal schedule, which for an hourly check would hide a
/// recovery for up to an hour. Every probed check has an entry, zero included,
/// so a lagging hint never overrides a fresher local observation.
type FailureMemory = Arc<parking_lot::Mutex<HashMap<CheckId, u32>>>;

fn cadence_for(failures: u32, threshold: u32) -> ProbeCadence {
    if failures == 0 {
        ProbeCadence::Scheduled
    } else if failures >= threshold.max(1) {
        ProbeCadence::Down
    } else {
        ProbeCadence::Retry
    }
}

/// Unknown leaves the run alone: an un-evaluable probe between two failures
/// must not cancel a confirmation in progress, and mirrors `StateTracker`.
fn next_cadence(status: Status, failures: &mut u32, threshold: u32) -> ProbeCadence {
    match status {
        Status::Down => *failures = failures.saturating_add(1),
        Status::Up => *failures = 0,
        Status::Unknown => {}
    }
    cadence_for(*failures, threshold)
}

fn spawn_check_loops(
    cfg: &AgentConfig,
    checks: Vec<CheckConfig>,
    tx: mpsc::Sender<CheckResult>,
    permits: Arc<Semaphore>,
    memory: FailureMemory,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    let probes = Probes::new()?;
    let mut handles = Vec::with_capacity(checks.len());
    for check in checks {
        // A schedule that won't parse is refused at save time, so reaching here
        // means a hand-edited store. Skip that check loudly rather than
        // defaulting it to some cadence nobody asked for.
        let schedule = match check.schedule() {
            Ok(s) => s,
            Err(e) => {
                warn!(check = %check.name, error = %e, "skipping check with an unusable schedule");
                continue;
            }
        };
        let retry_interval = check.retry_interval().map_err(anyhow::Error::msg)?;
        let down_interval = check.down_interval().map_err(anyhow::Error::msg)?;
        let probes = probes.clone();
        let origin = OwnedOrigin::from(cfg);
        let tx = tx.clone();
        let permits = permits.clone();
        let metrics = benchmark_metrics();
        let memory = memory.clone();
        handles.push(tokio::spawn(async move {
            let mut failures = memory.lock().get(&check.id).copied().unwrap_or(0);
            let mut cadence = cadence_for(failures, check.failures_before_down);
            loop {
                let wait = match cadence {
                    ProbeCadence::Scheduled => {
                        // Sleep until the next scheduled instant rather than ticking a
                        // fixed interval, so "every hour on the hour" lands on the hour
                        // instead of an hour after the agent happened to start.
                        let Some(next) = schedule.upcoming(chrono::Utc).next() else {
                            warn!(check = %check.name, "schedule has no future runs, stopping");
                            return;
                        };
                        (next - Utc::now()).to_std().unwrap_or(Duration::ZERO)
                    }
                    ProbeCadence::Retry => retry_interval,
                    ProbeCadence::Down => down_interval,
                };
                let scheduled = Instant::now();
                tokio::time::sleep(wait).await;
                if let Some(metrics) = &metrics {
                    metrics.scheduled.fetch_add(1, Ordering::Relaxed);
                    let lag = scheduled.elapsed().saturating_sub(wait);
                    AgentBenchmarkMetrics::observe_duration(
                        &metrics.schedule_lag_ns,
                        &metrics.schedule_lag_max_ns,
                        lag,
                    );
                }

                // Wait after the scheduled wake-up, not before it. The queue
                // therefore bounds only execution/socket use, never schedule
                // calculation or a check loop's ability to rejoin the queue.
                let permit = acquire_probe_permit(permits.clone(), metrics.as_deref()).await;
                if let Some(metrics) = &metrics {
                    let in_flight = metrics.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
                    AgentBenchmarkMetrics::observe_max(&metrics.in_flight_max, in_flight);
                }
                let execution = metrics.as_ref().map(|_| Instant::now());
                let result = execute_check(&probes, &origin.borrow(), &check).await;
                if let Some(metrics) = &metrics {
                    metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
                    AgentBenchmarkMetrics::observe_duration(
                        &metrics.execute_ns,
                        &metrics.execute_max_ns,
                        execution.expect("metrics start time").elapsed(),
                    );
                }
                drop(permit);
                cadence = next_cadence(
                    scheduling_status(&check, &result),
                    &mut failures,
                    check.failures_before_down,
                );
                memory.lock().insert(check.id, failures);
                debug!(check = %check.name, latency = result.latency_ms, ?cadence, "check done");
                if tx.send(result).await.is_err() {
                    break; // receiver gone, coordinator shutting down
                }
            }
        }));
    }
    Ok(handles)
}

/// Percent-encode a query value. Agent ids are tame config identifiers, so this
/// only needs to cover the characters that would break a query string rather
/// than being a general-purpose encoder.
fn urlencode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            other => other
                .to_string()
                .as_bytes()
                .iter()
                .map(|b| format!("%{b:02X}"))
                .collect(),
        })
        .collect()
}

/// Poll the coordinator for the checks this agent should run, replacing the
/// running loops whenever the set changes.
///
/// The whole set is torn down and respawned rather than diffed: at this scale
/// the difference is a handful of tasks, and an aborted probe simply re-runs on
/// the next tick. Diffing would buy nothing and could silently keep a stale
/// interval running.
pub async fn sync_loop(cfg: AgentConfig, tx: mpsc::Sender<CheckResult>) {
    let client = reqwest::Client::new();
    // Identify ourselves so the coordinator can hand back only the checks this
    // agent is assigned, rather than every check it knows about.
    //
    // Location is deliberately not sent. The coordinator reads it off this
    // agent's credential, because placement acts on it and the party being
    // placed should not get to choose.
    //
    // Cadence is sent because the coordinator sizes this agent's silence window
    // from it. A fixed window on the coordinator assumes one cadence and reads
    // a slow poller as gone.
    // Base URL; `rtt_ms` for the *previous* poll is appended per request below.
    // Reporting last poll's round-trip is what lets the coordinator show agent
    // reachability without a synthetic self-check — the poll is already the
    // heartbeat, this just times it.
    //
    // Capabilities are sent so the coordinator can place around this agent
    // instead of at it. Dropping a check it cannot represent keeps this agent
    // running, but the check still counts as covered and never gets probed;
    // saying up front which kinds it can run is what lets the coordinator give
    // the work to an agent that can, or report that nobody can.
    //
    // Rebuilt per tick rather than hoisted, because the coordinator in use can
    // change under failover.
    let query = format!(
        "agent_id={}&poll_secs={}&kinds={}&version={}",
        urlencode(&cfg.agent_id),
        cfg.poll_secs.max(1),
        urlencode(&probeable_kinds().join(",")),
        urlencode(&crate::version::current().to_string()),
    );
    let token = cfg.resolved_token();

    let permits = Arc::new(Semaphore::new(cfg.max_in_flight_probes));
    let memory = FailureMemory::default();
    let mut current: Vec<CheckConfig> = Vec::new();
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // The first tick fires immediately: the coordinator owns the check list.
    let mut poll = tokio::time::interval(std::time::Duration::from_secs(cfg.poll_secs.max(1)));
    let mut last_rtt_ms: Option<u128> = None;
    loop {
        poll.tick().await;
        let coordinator = cfg.coordinator().to_string();
        let url = match last_rtt_ms {
            Some(rtt) => format!("{coordinator}/api/v1/agent/checks?{query}&rtt_ms={rtt}"),
            None => format!("{coordinator}/api/v1/agent/checks?{query}"),
        };
        let mut req = client.get(&url);
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        let started = Instant::now();
        let sent = req.send().await;
        last_rtt_ms = Some(started.elapsed().as_millis());
        let (fetched, failing) = match sent {
            Ok(r) if r.status().is_success() => match r.json::<AgentChecksResponse>().await {
                Ok(body) => {
                    let (usable, skipped) = usable_checks(body.checks);
                    if !skipped.is_empty() {
                        // Loud, because the operator assigned these and they are
                        // not running: the coordinator is ahead of this agent and
                        // the fix is to upgrade it.
                        warn!(
                            checks = %skipped.join(", "),
                            "skipping checks this agent is too old to run; upgrade it"
                        );
                    }
                    (usable, body.failing)
                }
                Err(e) => {
                    warn!(error = %e, "malformed check list from coordinator");
                    continue;
                }
            },
            Ok(r) => {
                warn!(status = %r.status(), "coordinator refused the check list");
                continue;
            }
            // A coordinator that is down or unreachable must not stop the checks
            // already running: keep probing with the set we have.
            Err(e) => {
                debug!(error = %e, coordinator = %coordinator, "could not refresh check list, keeping current set");
                cfg.rotate_coordinator();
                continue;
            }
        };

        let hinted = adopt_failure_hints(&memory, failing);
        if !hinted && same_checks(&current, &fetched) {
            continue;
        }
        info!(
            count = fetched.len(),
            "check list changed, restarting probes"
        );
        for h in handles.drain(..) {
            h.abort();
        }
        current = fetched;
        let ids: std::collections::HashSet<CheckId> = current.iter().map(|c| c.id).collect();
        memory.lock().retain(|id, _| ids.contains(id));
        handles = match spawn_check_loops(
            &cfg,
            current.clone(),
            tx.clone(),
            permits.clone(),
            memory.clone(),
        ) {
            Ok(h) => h,
            Err(e) => {
                warn!(error = %e, "failed to restart check loops");
                return;
            }
        };
    }
}

/// Whether the agent's running set already matches what the coordinator serves.
///
/// This compares whole checks. It used to list the fields thought to affect
/// probing — name, kind, target, cron, hosts, timeout, expected status and
/// body — which silently went stale every time a check gained an option.
/// Correcting a DNS check's expected answers changed nothing in that list, so
/// the agent kept probing the old value indefinitely and the status page
/// reported a failure the operator had already fixed. Nothing announced it.
///
/// A whole-struct comparison cannot drift. Its cost is a restart for edits that
/// only affect presentation, which is a cancelled and respawned timer per
/// check — far cheaper than a probe that is quietly wrong.
/// Take the coordinator's failure runs for checks this process has not probed
/// yet — a cold start, or a check newly assigned after failover. Returns
/// whether any was adopted, since running loops only read memory at spawn.
fn adopt_failure_hints(memory: &FailureMemory, failing: HashMap<CheckId, u32>) -> bool {
    let mut memory = memory.lock();
    let mut adopted = false;
    for (id, n) in failing {
        if n > 0 && !memory.contains_key(&id) {
            memory.insert(id, n);
            adopted = true;
        }
    }
    adopted
}

fn same_checks(a: &[CheckConfig], b: &[CheckConfig]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    // The feed's order is the operator's display order, which is not a reason
    // to restart, so compare as sets keyed by the immutable id.
    fn sorted(cs: &[CheckConfig]) -> Vec<&CheckConfig> {
        let mut v: Vec<&CheckConfig> = cs.iter().collect();
        v.sort_by_key(|c| c.id);
        v
    }
    sorted(a) == sorted(b)
}

/// Wire shape of `/api/v1/agent/checks`.
///
/// Checks stay raw until each one is known to parse, because a coordinator is
/// upgraded before its fleet is. A newer coordinator can legitimately describe
/// a check kind this binary has never heard of, and deserialising the list as
/// `Vec<CheckConfig>` made that one unknown kind reject the *whole document* —
/// serde has no per-element recovery. The agent then kept its previous set
/// forever and silently ignored every later edit, while still probing and
/// reporting, so the fleet looked healthy the entire time.
///
/// Parsing element by element costs exactly the checks this binary cannot
/// represent, and nothing else.
#[derive(serde::Deserialize)]
struct AgentChecksResponse {
    checks: Vec<serde_json::Value>,
    #[serde(default)]
    failing: HashMap<CheckId, u32>,
}

/// The check kinds this binary can probe, as wire names.
///
/// Derived from `CheckKind::ALL` rather than written out, so a kind added to
/// the enum is claimed by every agent built afterwards without anyone
/// remembering to update a second list. Non-probed kinds are excluded: a
/// heartbeat is driven by an inbound ping, so claiming it would invite work
/// that no agent performs.
fn probeable_kinds() -> Vec<&'static str> {
    CheckKind::ALL
        .iter()
        .filter(|k| k.is_agent_probed())
        .map(|k| k.as_db())
        .collect()
}

/// The checks this agent can actually run, and a name for each one it had to
/// drop. A dropped check is not an error: it is this agent being older than the
/// coordinator, which is the normal state during a rolling upgrade.
fn usable_checks(raw: Vec<serde_json::Value>) -> (Vec<CheckConfig>, Vec<String>) {
    let mut usable = Vec::with_capacity(raw.len());
    let mut skipped = Vec::new();
    for value in raw {
        // Named before parsing, so the warning can say which check was dropped
        // even though the thing that failed to parse is the check itself.
        let name = value
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("<unnamed>")
            .to_string();
        match serde_json::from_value::<CheckConfig>(value) {
            Ok(c) => usable.push(c),
            Err(e) => {
                debug!(check = %name, error = %e, "cannot represent this check");
                skipped.push(name);
            }
        }
    }
    (usable, skipped)
}

// A full batch is sent immediately, rather than waiting for the next periodic
// flush. At the default five-second interval this keeps a 10k-results/30s agent
// (333 results/s) bounded and lossless while its coordinator is healthy.
const MAX_RESULTS_PER_BATCH: usize = 500;
// Thirty seconds of that expected peak rate. This is a result count, not a
// batch count, so the memory limit remains meaningful if batch sizing changes.
const MAX_PENDING_RESULTS: usize = 10_000;

/// FIFO retry buffer whose bound is expressed in results rather than batches.
#[derive(Default)]
struct PendingResults {
    batches: std::collections::VecDeque<Vec<CheckResult>>,
    len: usize,
}

impl PendingResults {
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Retain the oldest observations when the coordinator is unavailable:
    /// replacing them with newer ones would violate retry FIFO.
    fn push_back(&mut self, batch: Vec<CheckResult>) -> bool {
        debug_assert!(!batch.is_empty() && batch.len() <= MAX_RESULTS_PER_BATCH);
        if self.len + batch.len() > MAX_PENDING_RESULTS {
            return false;
        }
        self.len += batch.len();
        self.batches.push_back(batch);
        true
    }

    fn pop_front(&mut self) -> Option<Vec<CheckResult>> {
        let batch = self.batches.pop_front()?;
        self.len -= batch.len();
        Some(batch)
    }

    fn push_front(&mut self, batch: Vec<CheckResult>) {
        self.len += batch.len();
        self.batches.push_front(batch);
    }
}

fn queue_current(
    current: &mut Vec<CheckResult>,
    pending: &mut PendingResults,
    metrics: Option<&AgentBenchmarkMetrics>,
) {
    if current.is_empty() {
        return;
    }
    let batch_len = current.len();
    if !pending.push_back(std::mem::take(current)) {
        if let Some(metrics) = metrics {
            metrics
                .dropped_results
                .fetch_add(batch_len as u64, Ordering::Relaxed);
        }
        warn!(
            buffered = pending.len,
            cap = MAX_PENDING_RESULTS,
            "result buffer full, dropping newest batch"
        );
    }
    if let Some(metrics) = metrics {
        metrics.observe_queue(pending.len);
    }
}

async fn post_pending(
    cfg: &AgentConfig,
    client: &reqwest::Client,
    token: &Option<String>,
    pending: &mut PendingResults,
    metrics: Option<&AgentBenchmarkMetrics>,
) {
    // Retry oldest first, so history reaches the coordinator in order and a
    // recovered queue never reorders around a gap.
    while let Some(front) = pending.pop_front() {
        let batch_len = front.len();
        let body = ResultBatch { results: front };
        let started = metrics.map(|_| Instant::now());
        // Read per batch: the sync loop shares this cursor, so a failover it
        // decided applies to results already queued.
        let coordinator = cfg.coordinator().to_string();
        let mut req = client
            .post(format!("{coordinator}/api/v1/results"))
            .zstd_json(&body);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        match req.send().await {
            Ok(r) if r.status().is_success() => {
                if let Some(metrics) = metrics {
                    metrics.post_success.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(r) => {
                warn!(status = %r.status(), "coordinator rejected results");
                // A 4xx will never succeed on retry; dropping it avoids a
                // per-flush hot loop. A 5xx is transient.
                if !r.status().is_server_error() {
                    if let Some(metrics) = metrics {
                        metrics.post_client_errors.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    if let Some(metrics) = metrics {
                        metrics.post_server_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    pending.push_front(body.results);
                }
            }
            Err(e) => {
                if let Some(metrics) = metrics {
                    metrics
                        .post_transport_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                warn!(error = %e, coordinator = %coordinator, "failed to post results, keeping batch for retry");
                // The batch is kept, so failing over costs nothing: the next
                // flush replays it at the next coordinator and coordinator-side
                // deduplication makes a double delivery a no-op.
                cfg.rotate_coordinator();
                pending.push_front(body.results);
            }
        }
        if let Some(metrics) = metrics {
            metrics.post_batches.fetch_add(1, Ordering::Relaxed);
            metrics
                .post_results
                .fetch_add(batch_len as u64, Ordering::Relaxed);
            AgentBenchmarkMetrics::observe_max(&metrics.post_batch_max, batch_len as u64);
            AgentBenchmarkMetrics::observe_duration(
                &metrics.post_latency_ns,
                &metrics.post_latency_max_ns,
                started.expect("metrics start time").elapsed(),
            );
            metrics.observe_queue(pending.len);
        }
        if !pending.is_empty() {
            break;
        }
    }
}

/// Batch results and POST them to a remote coordinator (agent mode).
/// Standalone mode skips this and consumes the channel directly.
pub async fn poster_loop(cfg: AgentConfig, mut rx: mpsc::Receiver<CheckResult>) {
    let client = reqwest::Client::new();
    let token = cfg.resolved_token();
    let mut current = Vec::with_capacity(MAX_RESULTS_PER_BATCH);
    let mut pending = PendingResults::default();
    let metrics = benchmark_metrics();
    let mut flush = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(cfg.flush_secs),
        Duration::from_secs(cfg.flush_secs),
    );
    let mut receiver_open = true;

    loop {
        tokio::select! {
            maybe = rx.recv(), if receiver_open => match maybe {
                Some(r) => {
                    current.push(r);
                    if current.len() == MAX_RESULTS_PER_BATCH {
                        let was_empty = pending.is_empty();
                        queue_current(&mut current, &mut pending, metrics.as_deref());
                        // A healthy coordinator never accumulates a queue: send
                        // full batches immediately. Once a retry is queued,
                        // wait for the periodic flush before retrying it.
                        if was_empty {
                            post_pending(&cfg, &client, &token, &mut pending, metrics.as_deref()).await;
                        }
                    }
                }
                None => {
                    queue_current(&mut current, &mut pending, metrics.as_deref());
                    post_pending(&cfg, &client, &token, &mut pending, metrics.as_deref()).await;
                    if pending.is_empty() {
                        return;
                    }
                    receiver_open = false;
                }
            },
            _ = flush.tick() => {
                queue_current(&mut current, &mut pending, metrics.as_deref());
                post_pending(&cfg, &client, &token, &mut pending, metrics.as_deref()).await;
                if !receiver_open && pending.is_empty() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod sync_tests {
    use super::{
        acquire_probe_permit, adopt_failure_hints, next_cadence, poster_loop, same_checks,
        spawn_check_loops, usable_checks, FailureMemory, ProbeCadence,
    };
    use crate::config::{AgentConfig, CheckConfig};
    use crate::models::{CheckId, CheckKind, CheckResult, ResultBatch, Status};
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use chrono::Utc;
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };
    use tokio::sync::{mpsc, watch, Semaphore};
    use tokio::time::timeout;

    fn dns_check() -> CheckConfig {
        CheckConfig {
            name: "ddns".into(),
            kind: crate::models::CheckKind::Dns,
            target: "home.example.com".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn probe_permits_bound_execution_and_queued_checks_progress_fairly() {
        let permits = Arc::new(Semaphore::new(3));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (started_tx, mut started_rx) = mpsc::channel(12);
        let (release_tx, release_rx) = watch::channel(false);
        let mut workers = Vec::new();
        for _ in 0..12 {
            let permits = permits.clone();
            let active = active.clone();
            let peak = peak.clone();
            let started_tx = started_tx.clone();
            let mut release_rx = release_rx.clone();
            workers.push(tokio::spawn(async move {
                let _permit = acquire_probe_permit(permits, None).await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                started_tx.send(()).await.unwrap();
                release_rx.wait_for(|released| *released).await.unwrap();
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        drop(started_tx);

        for _ in 0..3 {
            timeout(Duration::from_secs(1), started_rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 3);
        release_tx.send(true).unwrap();
        for worker in workers {
            timeout(Duration::from_secs(1), worker)
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            started_rx.recv().await.is_some(),
            "queued checks must eventually run"
        );

        let permits = Arc::new(Semaphore::new(1));
        let held = permits.clone().acquire_owned().await.unwrap();
        let (order_tx, mut order_rx) = mpsc::channel(2);
        let (queued_tx, mut queued_rx) = mpsc::channel(2);
        for check in ["older", "newer"] {
            let permits = permits.clone();
            let order_tx = order_tx.clone();
            let queued_tx = queued_tx.clone();
            tokio::spawn(async move {
                queued_tx.send(()).await.unwrap();
                let _permit = acquire_probe_permit(permits, None).await;
                order_tx.send(check).await.unwrap();
            });
            timeout(Duration::from_secs(1), queued_rx.recv())
                .await
                .unwrap()
                .unwrap();
            tokio::task::yield_now().await;
        }
        drop(held);
        assert_eq!(
            timeout(Duration::from_secs(1), order_rx.recv())
                .await
                .unwrap(),
            Some("older")
        );
    }

    fn result(sequence: u128) -> CheckResult {
        CheckResult {
            check_id: CheckId::from_u128(sequence),
            agent_id: "batch-test".into(),
            zone: "test-zone".into(),
            region: "test-region".into(),
            kind: CheckKind::Http,
            timestamp: Utc::now(),
            latency_ms: 0,
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

    type SinkState = (Arc<Mutex<Vec<Vec<u128>>>>, Arc<AtomicBool>);

    async fn start_result_sink(reject_first: bool) -> (String, Arc<Mutex<Vec<Vec<u128>>>>) {
        let received = Arc::new(Mutex::new(Vec::new()));
        let reject = Arc::new(AtomicBool::new(reject_first));
        let app = Router::new()
            .route(
                "/api/v1/results",
                post(
                    |State((received, reject)): State<SinkState>,
                     Json(batch): Json<ResultBatch>| async move {
                        let ids = batch
                            .results
                            .into_iter()
                            .map(|r| r.check_id.as_u128())
                            .collect();
                        received.lock().unwrap().push(ids);
                        if reject.swap(false, Ordering::SeqCst) {
                            StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            StatusCode::OK
                        }
                    },
                ),
            )
            .with_state((received.clone(), reject));
        let app = crate::http::compressed(app);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (base, received)
    }

    #[tokio::test]
    async fn poster_splits_sustained_bursts_into_bounded_batches() {
        let (base, received) = start_result_sink(false).await;
        let (tx, rx) = mpsc::channel(1_100);
        let mut cfg = AgentConfig::default();
        cfg.coordinator_url = vec![base];
        cfg.flush_secs = 60;
        let poster = tokio::spawn(poster_loop(cfg, rx));
        for sequence in 0..1_001 {
            tx.send(result(sequence)).await.unwrap();
        }
        drop(tx);
        timeout(Duration::from_secs(5), poster)
            .await
            .unwrap()
            .unwrap();

        let received = received.lock().unwrap().clone();
        assert!(
            received
                .iter()
                .all(|batch| batch.len() <= super::MAX_RESULTS_PER_BATCH),
            "each POST must be capped"
        );
        assert_eq!(
            received.into_iter().flatten().collect::<Vec<_>>(),
            (0..1_001).collect::<Vec<_>>(),
            "healthy bursts must neither drop nor reorder results"
        );
    }

    #[tokio::test]
    async fn poster_retries_failed_batches_before_later_results() {
        let (base, received) = start_result_sink(true).await;
        let (tx, rx) = mpsc::channel(4);
        let mut cfg = AgentConfig::default();
        cfg.coordinator_url = vec![base];
        cfg.flush_secs = 1;
        let poster = tokio::spawn(poster_loop(cfg, rx));
        tx.send(result(1)).await.unwrap();
        tx.send(result(2)).await.unwrap();
        drop(tx);
        timeout(Duration::from_secs(5), poster)
            .await
            .unwrap()
            .unwrap();

        let received = received.lock().unwrap().clone();
        assert_eq!(received, vec![vec![1, 2], vec![1, 2]]);
    }

    #[test]
    fn failures_retry_then_use_down_cadence_until_recovery() {
        let mut failures = 0;
        assert_eq!(
            next_cadence(Status::Down, &mut failures, 2),
            ProbeCadence::Retry
        );
        assert_eq!(
            next_cadence(Status::Unknown, &mut failures, 2),
            ProbeCadence::Retry,
            "an unknown retry must not silently cancel confirmation"
        );
        assert_eq!(
            next_cadence(Status::Down, &mut failures, 2),
            ProbeCadence::Down
        );
        assert_eq!(
            next_cadence(Status::Down, &mut failures, 2),
            ProbeCadence::Down
        );
        assert_eq!(
            next_cadence(Status::Up, &mut failures, 2),
            ProbeCadence::Scheduled
        );
        assert_eq!(failures, 0);
        assert_eq!(
            next_cadence(Status::Unknown, &mut failures, 2),
            ProbeCadence::Scheduled,
            "unknown with no failure run keeps the normal schedule"
        );
    }

    /// A down check on a long schedule must not wait out that schedule to
    /// notice recovery just because its loop was respawned (a config edit) or
    /// the agent restarted and took the coordinator's hint.
    #[tokio::test]
    async fn a_respawned_loop_resumes_down_cadence_from_failure_memory() {
        let check = CheckConfig {
            name: "yearly".into(),
            kind: CheckKind::Tcp,
            target: "127.0.0.1:1".into(),
            cron: "0 0 0 1 1 *".into(),
            timeout_secs: 1,
            failures_before_down: 2,
            down_interval_secs: Some(1),
            ..Default::default()
        };
        let cfg = AgentConfig::default();
        let spawn = |memory: FailureMemory| {
            let (tx, rx) = mpsc::channel(4);
            let handles = spawn_check_loops(
                &cfg,
                vec![check.clone()],
                tx,
                Arc::new(Semaphore::new(1)),
                memory,
            )
            .unwrap();
            (handles, rx)
        };

        let (handles, mut rx) = spawn(FailureMemory::default());
        assert!(
            timeout(Duration::from_millis(2500), rx.recv())
                .await
                .is_err(),
            "a check with no failure run waits for its schedule"
        );
        handles.iter().for_each(|h| h.abort());

        let memory = FailureMemory::default();
        assert!(adopt_failure_hints(
            &memory,
            [(check.id, 2), (CheckId::from_u128(9), 0)].into()
        ));
        assert!(
            !adopt_failure_hints(&memory, [(check.id, 5)].into()),
            "local memory wins over a later hint"
        );
        let (handles, mut rx) = spawn(memory.clone());
        let result = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("down cadence probes within the down interval")
            .unwrap();
        assert_eq!(result.check_id, check.id);
        assert_eq!(memory.lock().get(&check.id), Some(&3));
        handles.iter().for_each(|h| h.abort());
    }

    #[test]
    fn correcting_expected_dns_answers_restarts_the_probe() {
        // The reported bug. An operator fixes a mistyped address in the admin
        // UI; the agent compares its running set against the feed, sees no
        // difference, and keeps probing the old value. The check goes on
        // failing for a fix that was already made, and nothing says why.
        let mut fixed = dns_check();
        fixed.dns.expected = vec!["203.0.113.9".into()];
        let mut typo = dns_check();
        typo.dns.expected = vec!["203.0.113.8".into()];

        assert!(
            !same_checks(&[typo], &[fixed.clone()]),
            "a corrected expected answer must reach the agent"
        );
        assert!(
            same_checks(&[fixed.clone()], &[fixed]),
            "no edit, no restart"
        );
    }

    #[test]
    fn no_probe_affecting_field_can_be_forgotten() {
        // The real defect was structural, not one missing field: the comparison
        // listed fields by hand, so every option added since drifted out of it.
        // Mutating each probe-affecting field in turn is what keeps the next
        // option from going the same way.
        let base = dns_check();
        type Mutation = (&'static str, Box<dyn Fn(&mut CheckConfig)>);
        let mutations: Vec<Mutation> = vec![
            (
                "target",
                Box::new(|c| c.target = "other.example.com".into()),
            ),
            ("cron", Box::new(|c| c.cron = "0 */7 * * * *".into())),
            ("timeout", Box::new(|c| c.timeout_secs = 99)),
            ("hosts", Box::new(|c| c.hosts = vec!["1.2.3.4".into()])),
            (
                "dns.expected",
                Box::new(|c| c.dns.expected = vec!["9.9.9.9".into()]),
            ),
            ("dns.record", Box::new(|c| c.dns.record = "AAAA".into())),
            (
                "dns.match_type",
                Box::new(|c| c.dns.match_type = crate::config::MatchMode::Any),
            ),
            (
                "host_match",
                Box::new(|c| c.host_match = crate::config::MatchMode::Any),
            ),
            ("expect.status", Box::new(|c| c.expect.status = Some(418))),
            (
                "expect.body",
                Box::new(|c| c.expect.body_contains = Some("hi".into())),
            ),
            ("ping.count", Box::new(|c| c.ping.count = 7)),
            ("cert_warning_days", Box::new(|c| c.cert_warning_days = 3)),
            (
                "allow_self_signed",
                Box::new(|c| c.allow_self_signed = !c.allow_self_signed),
            ),
            ("eval", Box::new(|c| c.eval = Some("return true".into()))),
            ("enabled", Box::new(|c| c.enabled = !c.enabled)),
            ("agents", Box::new(|c| c.agents = vec!["agent-b".into()])),
            (
                "headers",
                Box::new(|c| {
                    c.headers = vec![crate::config::Header {
                        name: "X-Probe".into(),
                        value: "1".into(),
                    }]
                }),
            ),
        ];

        for (field, mutate) in mutations {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert!(
                !same_checks(std::slice::from_ref(&base), &[changed]),
                "editing {field} must reach the agent"
            );
        }
    }

    #[test]
    fn reordering_the_feed_is_not_a_change() {
        // Display order is the operator's arrangement, not probe behaviour.
        // Restarting every loop when someone drags a row would be gratuitous.
        let mut a = dns_check();
        a.name = "a".into();
        let mut b = dns_check();
        b.name = "b".into();
        assert!(same_checks(&[a.clone(), b.clone()], &[b, a]));
    }

    #[test]
    fn an_unknown_kind_costs_only_its_own_check() {
        // The reported failure. A coordinator that learned a new probe type
        // hands it to an agent that predates it. Deserialising the list as a
        // whole rejected every check over the one it could not name, and the
        // agent then ran its previous set forever without saying so.
        let raw = vec![
            serde_json::json!({ "name": "api", "type": "http", "target": "https://example.com" }),
            serde_json::json!({ "name": "exporter", "type": "openmetrics", "target": "http://x/metrics" }),
            serde_json::json!({ "name": "db", "type": "tcp", "target": "db:5432" }),
        ];

        let (usable, skipped) = usable_checks(raw);

        assert_eq!(
            usable.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["api", "exporter", "db"],
            "every kind this binary knows must survive the poll"
        );
        assert!(skipped.is_empty());
    }

    #[test]
    fn a_kind_from_the_future_is_dropped_by_name() {
        // What a rolling upgrade actually looks like from the old side: the
        // coordinator is ahead, one check cannot be represented, and the
        // operator needs it named because they assigned it and it is not
        // running.
        let raw = vec![
            serde_json::json!({ "name": "api", "type": "http", "target": "https://example.com" }),
            serde_json::json!({ "name": "warehouse", "type": "not-a-kind-yet", "target": "pg:5432" }),
        ];

        let (usable, skipped) = usable_checks(raw);

        assert_eq!(usable.len(), 1);
        assert_eq!(usable[0].name, "api");
        assert_eq!(skipped, ["warehouse"], "the drop must be attributable");
    }

    #[test]
    fn a_check_too_broken_to_name_still_reports() {
        // Not every unparseable element is a future kind; a truncated or
        // mangled document has no name to blame. Silence would be worse than
        // an anonymous warning, so it is still counted.
        let raw = vec![serde_json::json!({ "type": "http" }), serde_json::json!(7)];

        let (usable, skipped) = usable_checks(raw);

        assert!(usable.is_empty());
        assert_eq!(skipped, ["<unnamed>", "<unnamed>"]);
    }
}
