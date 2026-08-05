use crate::config::{AgentConfig, CheckConfig};
use crate::models::{CheckKind, CheckResult, ResultBatch};
use anyhow::{Context, Result};
use chrono::Utc;
use hickory_resolver::config::{NameServerConfig, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::{Resolver, TokioResolver};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// What a probe observed: an optional HTTP status code, whether the configured
/// content expectation matched, and an error string when the probe failed.
/// `body`/`headers` are populated only for checks carrying a Lua script.
#[derive(Default)]
struct Observation {
    status_code: Option<u16>,
    body_matched: Option<bool>,
    error: Option<String>,
    failure: Option<crate::models::FailureKind>,
    body: Option<String>,
    headers: Option<std::collections::HashMap<String, String>>,
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
        check_id: check.name.clone(),
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
    }
}

async fn probe_one(probes: &Probes, check: &CheckConfig, target: &str) -> Observation {
    match check.kind {
        CheckKind::Http => probe_http(&probes.http, check, target).await,
        CheckKind::Tcp => probe_tcp(check, target).await,
        CheckKind::Dns => probe_dns(&probes.resolver, check, target).await,
        CheckKind::Icmp => probe_icmp(&probes.resolver, check, target).await,
        CheckKind::Ssl => probe_ssl(check, target).await,
        // Nothing to probe: the coordinator decides a heartbeat from when it was
        // last pinged, so an agent should never have been handed one.
        CheckKind::Heartbeat => Observation::failed(
            crate::models::FailureKind::Other,
            "heartbeat checks are not agent-probed",
        ),
    }
}

/// HTTP GET. Reads the body when a content expectation is configured, or when a
/// Lua script might inspect it.
async fn probe_http(client: &reqwest::Client, check: &CheckConfig, target: &str) -> Observation {
    let mut req = client.get(target).timeout(check.timeout());
    for header in &check.headers {
        req = req.header(header.name.trim(), header.value.as_str());
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            return if e.is_timeout() {
                Observation::failed(crate::models::FailureKind::Timeout, "timeout")
            } else {
                Observation::failed(crate::models::FailureKind::ConnectionFailed, e.to_string())
            }
        }
    };
    let status_code = Some(resp.status().as_u16());
    let scripted = check.eval.as_deref().is_some_and(|s| !s.trim().is_empty());
    // Headers are cheap; the body is not, so only read it when something will
    // actually look at it.
    let headers = scripted.then(|| {
        resp.headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_lowercase(),
                    v.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect()
    });
    let needle = check.expect.body_contains.as_deref();
    if needle.is_none() && !scripted {
        return Observation {
            status_code,
            headers,
            ..Default::default()
        };
    }
    match resp.text().await {
        Ok(body) => {
            let matched = needle.map(|n| body.contains(n));
            Observation {
                status_code,
                body_matched: matched,
                // Spell out the mismatch: otherwise a 200-with-wrong-body shows
                // up as "HTTP 200" and reads like a healthy response.
                error: match (needle, matched) {
                    (Some(n), Some(false)) => Some(format!("body did not contain {n:?}")),
                    _ => None,
                },
                failure: matches!((needle, matched), (Some(_), Some(false)))
                    .then_some(crate::models::FailureKind::UnexpectedContent),
                body: scripted.then(|| truncate_body(body)),
                headers,
            }
        }
        Err(e) => Observation {
            status_code,
            error: Some(format!("reading body: {e}")),
            failure: Some(crate::models::FailureKind::ConnectionFailed),
            headers,
            ..Default::default()
        },
    }
}

/// Cap what travels to the coordinator, on a char boundary so the JSON stays
/// valid UTF-8.
fn truncate_body(mut body: String) -> String {
    if body.len() > crate::script::MAX_BODY_BYTES {
        let mut cut = crate::script::MAX_BODY_BYTES;
        while cut > 0 && !body.is_char_boundary(cut) {
            cut -= 1;
        }
        body.truncate(cut);
    }
    body
}

/// TCP connect to "host:port". Connecting at all is a success; the coordinator
/// treats any error as DOWN.
async fn probe_tcp(check: &CheckConfig, target: &str) -> Observation {
    match tokio::time::timeout(check.timeout(), tokio::net::TcpStream::connect(target)).await {
        Ok(Ok(_stream)) => Observation::ok(),
        Ok(Err(e)) => {
            Observation::failed(crate::models::FailureKind::ConnectionFailed, e.to_string())
        }
        Err(_) => Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
    }
}

/// DNS lookup of the configured record type. A dropped UDP packet is retried by
/// the resolver (see `Probes::new`) rather than counted as an outage — the
/// failure mode that makes naive DNS monitors flap. NXDOMAIN comes back as an
/// error, which is correct: the name genuinely does not resolve.
///
/// The target is queried as a fully-qualified name (trailing dot). Without this,
/// a resolver with `search example.local` silently answers for
/// `target.example.local` instead — so the check would pass while testing a name
/// nobody configured, and a wildcard/sinkhole answer would read as healthy.
///
/// Expected values assert what the record resolves to, under `any` or `all`
/// matching.
async fn probe_dns(resolver: &TokioResolver, check: &CheckConfig, target: &str) -> Observation {
    let fqdn = if target.ends_with('.') {
        target.to_string()
    } else {
        format!("{target}.")
    };
    let answers = match tokio::time::timeout(
        check.timeout(),
        lookup_record(resolver, &fqdn, &check.dns.record),
    )
    .await
    {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => return Observation::failed(crate::models::FailureKind::NameResolution, e),
        Err(_) => return Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
    };
    if answers.is_empty() {
        return Observation::failed(
            crate::models::FailureKind::UnexpectedDnsAnswer,
            format!("no {} records for {target}", check.dns.record),
        );
    }

    // `expected` is the modern form; `expect.body_contains` predates it and is
    // still honoured so existing configs keep asserting what they always did.
    let mut wanted: Vec<String> = check
        .dns
        .expected
        .iter()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    if let Some(legacy) = check.expect.body_contains.as_deref() {
        wanted.push(legacy.to_string());
    }
    if wanted.is_empty() {
        return Observation::ok();
    }

    let joined = answers.join(",");
    let hit = |needle: &String| answers.iter().any(|a| a.contains(needle.as_str()));
    let hits = wanted.iter().filter(|w| hit(w)).count();
    let matched = check.dns.match_type.satisfied(hits, wanted.len());
    Observation {
        body_matched: Some(matched),
        // Name the actual answer, so a pinned-record failure is self-explanatory.
        failure: (!matched).then_some(crate::models::FailureKind::UnexpectedDnsAnswer),
        error: (!matched).then(|| {
            let mode = match check.dns.match_type {
                crate::config::MatchMode::All => "all of",
                crate::config::MatchMode::Any => "one of",
            };
            format!("resolved to {joined}, expected {mode} {wanted:?}")
        }),
        ..Default::default()
    }
}

/// Look up one record type, returning the answers as text.
async fn lookup_record(
    resolver: &TokioResolver,
    fqdn: &str,
    record: &str,
) -> Result<Vec<String>, String> {
    use hickory_resolver::proto::rr::RecordType;
    let rtype = match record.to_ascii_uppercase().as_str() {
        "AAAA" => RecordType::AAAA,
        "CNAME" => RecordType::CNAME,
        "MX" => RecordType::MX,
        "TXT" => RecordType::TXT,
        "NS" => RecordType::NS,
        _ => RecordType::A,
    };
    match resolver.lookup(fqdn, rtype).await {
        // `answers()` is the response section; rendering the rdata gives the
        // value ("1.2.3.4", "mail.example.com.") rather than a whole record line.
        Ok(answer) => Ok(answer
            .answers()
            .iter()
            .map(|r| r.data.to_string())
            .collect()),
        // "No records" is the common, expected failure — report it plainly rather
        // than leaking the resolver's Debug-formatted Query internals.
        Err(e) if e.is_no_records_found() => Err(format!("no {record} records for {fqdn}")),
        Err(e) => Err(e.to_string()),
    }
}

/// ICMP echo. Resolves the target, then sends `count` echoes; one reply is
/// enough, because a single dropped packet is not an outage.
///
/// ponytail: needs CAP_NET_RAW (or a permissive `net.ipv4.ping_group_range`).
/// Without it the probe reports a permission error rather than silently passing.
async fn probe_icmp(resolver: &TokioResolver, check: &CheckConfig, target: &str) -> Observation {
    let addr = match resolve_one(resolver, target, &check.ping.ip_type).await {
        Ok(a) => a,
        Err(e) => return Observation::failed(crate::models::FailureKind::NameResolution, e),
    };
    let mut last_error = String::from("no reply");
    let mut last_kind = crate::models::FailureKind::ConnectionFailed;
    for _ in 0..check.ping.count.max(1) {
        match tokio::time::timeout(check.timeout(), surge_ping::ping(addr, &[0; 32])).await {
            Ok(Ok(_)) => return Observation::ok(),
            Ok(Err(e)) => {
                last_error = e.to_string();
                last_kind = crate::models::FailureKind::ConnectionFailed;
            }
            Err(_) => {
                last_error = "timeout".to_string();
                last_kind = crate::models::FailureKind::Timeout;
            }
        }
    }
    Observation::failed(last_kind, last_error)
}

/// Captures the leaf certificate during the handshake instead of validating it.
///
/// An SSL *check* has to report on whatever the server actually presents,
/// including a chain that fails validation — refusing the handshake would tell
/// us nothing about the expiry we came to read. The connection is used solely
/// to inspect the certificate and is dropped immediately, so nothing is ever
/// sent over it.
#[derive(Debug, Default)]
struct CertCapture {
    expiry: parking_lot::Mutex<Option<chrono::DateTime<Utc>>>,
}

impl CertCapture {
    fn expiry(&self) -> Option<chrono::DateTime<Utc>> {
        *self.expiry.lock()
    }
}

impl rustls::client::danger::ServerCertVerifier for CertCapture {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use x509_parser::prelude::FromDer;
        if let Ok((_, cert)) = x509_parser::prelude::X509Certificate::from_der(end_entity) {
            let secs = cert.validity().not_after.timestamp();
            *self.expiry.lock() = chrono::DateTime::from_timestamp(secs, 0);
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// TLS certificate validity for a host, and how long is left on it.
///
/// Expiry is a failure the moment it happens, so the check reports DOWN once the
/// certificate is inside its warning window rather than waiting for the outage
/// the expiry will cause.
async fn probe_ssl(check: &CheckConfig, target: &str) -> Observation {
    // Accept "host" or "host:port"; 443 unless told otherwise.
    let (host, port) = match target.rsplit_once(':') {
        Some((h, p)) if p.parse::<u16>().is_ok() => (h.to_string(), p.parse::<u16>().unwrap()),
        _ => (target.to_string(), 443u16),
    };
    let addr = format!("{host}:{port}");
    let stream =
        match tokio::time::timeout(check.timeout(), tokio::net::TcpStream::connect(&addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Observation::failed(
                    crate::models::FailureKind::ConnectionFailed,
                    format!("connecting to {addr}: {e}"),
                )
            }
            Err(_) => return Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
        };

    let verifier = std::sync::Arc::new(CertCapture::default());
    let mut cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier.clone())
        .with_no_client_auth();
    cfg.alpn_protocols.clear();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg));
    let server_name = match rustls::pki_types::ServerName::try_from(host.clone()) {
        Ok(n) => n,
        Err(_) => {
            return Observation::failed(
                crate::models::FailureKind::TlsError,
                format!("{host} is not a valid TLS server name"),
            )
        }
    };
    if let Err(e) =
        tokio::time::timeout(check.timeout(), connector.connect(server_name, stream)).await
    {
        let _ = e;
        return Observation::failed(
            crate::models::FailureKind::Timeout,
            "timeout during TLS handshake",
        );
    }

    let Some(expiry) = verifier.expiry() else {
        return Observation::failed(
            crate::models::FailureKind::TlsError,
            "server presented no certificate",
        );
    };
    let days_left = (expiry - Utc::now()).num_days();
    if days_left < 0 {
        return Observation::failed(
            crate::models::FailureKind::CertificateExpired { days: -days_left },
            format!("certificate expired {} days ago", -days_left),
        );
    }
    if days_left <= check.cert_warning_days as i64 {
        return Observation::failed(
            crate::models::FailureKind::CertificateExpiring { days: days_left },
            format!("certificate expires in {days_left} days"),
        );
    }
    Observation::ok()
}

/// Resolve a bare host (or pass through a literal IP) to a single address,
/// honouring the configured address family. Queried as an FQDN for the same
/// reason as `probe_dns`: search-domain expansion would ping a different host.
async fn resolve_one(
    resolver: &TokioResolver,
    target: &str,
    ip_type: &str,
) -> Result<std::net::IpAddr, String> {
    if let Ok(ip) = target.parse::<std::net::IpAddr>() {
        return Ok(ip);
    }
    let fqdn = if target.ends_with('.') {
        target.to_string()
    } else {
        format!("{target}.")
    };
    let answer = resolver
        .lookup_ip(fqdn.as_str())
        .await
        .map_err(|e| format!("resolving {target}: {e}"))?;
    let wanted = ip_type.to_ascii_lowercase();
    answer
        .iter()
        .find(|ip| match wanted.as_str() {
            "ipv4" => ip.is_ipv4(),
            "ipv6" => ip.is_ipv6(),
            _ => true,
        })
        .ok_or_else(|| match wanted.as_str() {
            "ipv4" | "ipv6" => format!("{target} has no {wanted} address"),
            _ => format!("{target} resolved to no addresses"),
        })
}

/// Shared, cheaply-cloned probe dependencies. Built once per process:
/// the HTTP client pools connections and the DNS resolver is `Arc`-backed
/// internally, so cloning per check loop shares one instance.
#[derive(Clone)]
pub struct Probes {
    http: reqwest::Client,
    resolver: TokioResolver,
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
        let builder = match TokioResolver::builder_tokio() {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "no system resolver config, falling back to public DNS");
                let servers = ["1.1.1.1", "8.8.8.8"]
                    .iter()
                    .map(|ip| NameServerConfig::udp_and_tcp(ip.parse().expect("literal IP")))
                    .collect();
                let config = ResolverConfig::from_parts(None, Vec::new(), servers);
                Resolver::builder_with_config(config, TokioRuntimeProvider::default())
            }
        };
        let resolver = builder
            .with_options(opts)
            .build()
            .context("building DNS resolver for checks")?;

        Ok(Self {
            http: reqwest::Client::new(),
            resolver,
        })
    }
}

/// Spawn one loop per check, each on its own interval so a slow check never
/// delays a sibling. Results flow into `tx`. Handles are returned so a caller
/// that re-syncs its check list can replace the whole set.
pub fn spawn_check_loops(
    cfg: &AgentConfig,
    checks: Vec<CheckConfig>,
    tx: mpsc::Sender<CheckResult>,
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
        let probes = probes.clone();
        let origin = OwnedOrigin::from(cfg);
        let tx = tx.clone();
        handles.push(tokio::spawn(async move {
            loop {
                // Sleep until the next scheduled instant rather than ticking a
                // fixed interval, so "every hour on the hour" lands on the hour
                // instead of an hour after the agent happened to start.
                let Some(next) = schedule.upcoming(chrono::Utc).next() else {
                    warn!(check = %check.name, "schedule has no future runs, stopping");
                    return;
                };
                let wait = (next - Utc::now()).to_std().unwrap_or(Duration::ZERO);
                tokio::time::sleep(wait).await;

                let result = execute_check(&probes, &origin.borrow(), &check).await;
                debug!(check = %check.name, latency = result.latency_ms, "check done");
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
pub async fn sync_loop(cfg: AgentConfig, tx: mpsc::Sender<CheckResult>, initial: Vec<CheckConfig>) {
    let client = reqwest::Client::new();
    // Identify ourselves so the coordinator can hand back only the checks this
    // agent is assigned, rather than every check it knows about.
    // Location travels with the request: a first-boot agent has no stored
    // history for the coordinator to look it up from, and that is precisely
    // when it needs placing.
    let url = format!(
        "{}/api/v1/agent/checks?agent_id={}&zone={}&region={}",
        cfg.coordinator_url,
        urlencode(&cfg.agent_id),
        urlencode(&cfg.zone),
        urlencode(&cfg.region),
    );
    let token = cfg.resolved_token();

    let mut current = initial;
    let mut handles = match spawn_check_loops(&cfg, current.clone(), tx.clone()) {
        Ok(h) => h,
        Err(e) => {
            warn!(error = %e, "failed to start check loops");
            return;
        }
    };

    let mut poll = tokio::time::interval(std::time::Duration::from_secs(cfg.poll_secs.max(1)));
    // The first tick fires immediately, which is what we want: the config file
    // is only a cold-start fallback and the coordinator owns the real list.
    loop {
        poll.tick().await;
        let mut req = client.get(&url);
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        let fetched = match req.send().await {
            Ok(r) if r.status().is_success() => match r.json::<AgentChecksResponse>().await {
                Ok(body) => body.checks,
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
                debug!(error = %e, "could not refresh check list, keeping current set");
                continue;
            }
        };

        if same_checks(&current, &fetched) {
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
        handles = match spawn_check_loops(&cfg, current.clone(), tx.clone()) {
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
fn same_checks(a: &[CheckConfig], b: &[CheckConfig]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    // The feed's order is the operator's display order, which is not a reason
    // to restart, so compare as sets keyed by the unique name.
    fn sorted(cs: &[CheckConfig]) -> Vec<&CheckConfig> {
        let mut v: Vec<&CheckConfig> = cs.iter().collect();
        v.sort_by(|x, y| x.name.cmp(&y.name));
        v
    }
    sorted(a) == sorted(b)
}

/// Wire shape of `/api/v1/agent/checks`.
#[derive(serde::Deserialize)]
struct AgentChecksResponse {
    checks: Vec<CheckConfig>,
}

/// Batch results and POST them to a remote coordinator (agent mode).
/// Standalone mode skips this and consumes the channel directly.
pub async fn poster_loop(cfg: AgentConfig, mut rx: mpsc::Receiver<CheckResult>) {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/results", cfg.coordinator_url);
    let token = cfg.resolved_token();
    let mut current = Vec::new();
    // Batches whose POST failed, oldest first. A failed POST keeps its batch
    // and retries on the next flush instead of dropping it, so a coordinator
    // that blips for a minute does not silently eat the results that would
    // have confirmed (or recovered) an outage. Bounded so an unreachable
    // coordinator cannot grow an unbounded backlog in memory; when the cap is
    // hit the OLDEST results are dropped, because the freshest observation is
    // what the status page and the confirmation threshold act on.
    let mut pending: std::collections::VecDeque<Vec<CheckResult>> = std::collections::VecDeque::new();
    const MAX_PENDING_BATCHES: usize = 32;
    let mut flush = tokio::time::interval(std::time::Duration::from_secs(cfg.flush_secs));

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(r) => current.push(r),
                None => break, // producers gone
            },
            _ = flush.tick() => {
                if current.is_empty() && pending.is_empty() {
                    continue;
                }
                if !current.is_empty() {
                    pending.push_back(std::mem::take(&mut current));
                    while pending.len() > MAX_PENDING_BATCHES {
                        warn!("result buffer full, dropping oldest batch");
                        pending.pop_front();
                    }
                }
                // Retry oldest first, so history reaches the coordinator in
                // order and a recovered queue never reorders around a gap.
                while let Some(front) = pending.pop_front() {
                    let body = ResultBatch { results: front };
                    let mut req = client.post(&url).json(&body);
                    if let Some(t) = &token {
                        req = req.bearer_auth(t);
                    }
                    match req.send().await {
                        Ok(r) if r.status().is_success() => {}
                        // A rejected batch is worth shouting about: a token
                        // mismatch otherwise looks exactly like a check that
                        // stopped reporting.
                        Ok(r) => {
                            warn!(status = %r.status(), "coordinator rejected results");
                            // A 4xx will never succeed on retry; dropping it
                            // avoids a per-flush hot loop. A 5xx is transient.
                            if !r.status().is_server_error() {
                                continue;
                            }
                            pending.push_front(body.results);
                            break;
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to post results, keeping batch for retry");
                            pending.push_front(body.results);
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod sync_tests {
    use super::same_checks;
    use crate::config::CheckConfig;

    fn dns_check() -> CheckConfig {
        CheckConfig {
            name: "ddns".into(),
            kind: crate::models::CheckKind::Dns,
            target: "home.example.com".into(),
            ..Default::default()
        }
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
}
