use crate::models::CheckKind;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub checks: Vec<CheckConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    /// Where to POST results. Ignored in standalone (writes in-process).
    #[serde(default = "default_coordinator_url")]
    pub coordinator_url: String,
    #[serde(default = "default_agent_id")]
    pub agent_id: String,
    /// Where this agent runs. Declared, never inferred: only the operator knows
    /// which rack, datacentre or cloud AZ a process sits in, and a probe's
    /// result is meaningless without knowing where it was observed from.
    ///
    /// A zone is the failure domain a probe shares (one AZ, one office link); a
    /// region groups zones for rollups. Both default to "unknown" so a
    /// single-site install needs no configuration at all.
    #[serde(default = "default_location")]
    pub zone: String,
    #[serde(default = "default_location")]
    pub region: String,
    /// Flush interval for batched result posting.
    #[serde(default = "default_flush_secs")]
    pub flush_secs: u64,
    /// How often to ask the coordinator for this agent's checks.
    ///
    /// This interval is also the worst-case delay before a newly created or
    /// edited check takes effect, which is the number an operator actually
    /// feels. It is affordable to keep short: the feed costs ~1.5ms and reads
    /// no result history, so twenty agents at ten seconds is 2 req/s. Agents on
    /// a metered or high-latency link can raise it.
    #[serde(default = "default_poll_secs")]
    pub poll_secs: u64,
    /// Shared secret presented to the coordinator, falling back to
    /// `WARDEN_AGENT_TOKEN`. Must match the coordinator's `agent_token`.
    #[serde(default)]
    pub token: Option<String>,
}

impl AgentConfig {
    pub fn resolved_token(&self) -> Option<String> {
        self.token
            .clone()
            .or_else(|| std::env::var("WARDEN_AGENT_TOKEN").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoordinatorConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    /// Webhook URL for alerts (Phase 1: webhook only).
    #[serde(default)]
    pub alert_webhook: Option<String>,
    /// Password for the admin UI. Read from `WARDEN_ADMIN_PASSWORD` when unset
    /// here, which is how it should normally be supplied — a password in a
    /// config file tends to end up in version control.
    ///
    /// With neither set, every mutating endpoint refuses: an uptime monitor
    /// that silently accepts anonymous edits is worse than one you can't edit.
    #[serde(default)]
    pub admin_password: Option<String>,
    /// Shared secret agents present to post results and fetch their check list.
    /// Falls back to `WARDEN_AGENT_TOKEN`. Unset means those endpoints stay open,
    /// preserving existing deployments — but check targets are then readable by
    /// anyone who can reach the coordinator, so it is logged loudly at boot.
    #[serde(default)]
    pub agent_token: Option<String>,
}

impl CoordinatorConfig {
    /// Config file first, environment second. Returns `None` when neither is
    /// set or the value is blank.
    fn secret(inline: &Option<String>, env_key: &str) -> Option<String> {
        inline
            .clone()
            .or_else(|| std::env::var(env_key).ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    pub fn resolved_admin_password(&self) -> Option<String> {
        Self::secret(&self.admin_password, "WARDEN_ADMIN_PASSWORD")
    }

    pub fn resolved_agent_token(&self) -> Option<String> {
        Self::secret(&self.agent_token, "WARDEN_AGENT_TOKEN")
    }
}

/// Per-check assertions. Mirrors the design document's nested `expect` block:
///
/// ```toml
/// [checks.expect]
/// status = 200
/// body_contains = "ok"
/// ```
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct Expect {
    /// HTTP only: require this exact status code instead of any 2xx-3xx.
    #[serde(default)]
    pub status: Option<u16>,
    /// Require this substring in the response body (HTTP) or among the resolved
    /// answers (DNS). Absent means no content assertion.
    #[serde(default)]
    pub body_contains: Option<String>,
}

/// HTTP request headers sent with the probe, as ordered pairs so the editor can
/// round-trip them without reordering on every save.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct Header {
    pub name: String,
    pub value: String,
}

/// Whether a set of assertions all have to hold, or just one of them.
/// Shared by DNS expected values and multi-host checks so "any/all" means the
/// same thing wherever it appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchMode {
    /// One passing is enough.
    Any,
    #[default]
    All,
}

impl MatchMode {
    pub fn satisfied(&self, passed: usize, total: usize) -> bool {
        match self {
            MatchMode::Any => passed > 0,
            MatchMode::All => passed == total,
        }
    }
}

/// What a DNS probe asks for and what counts as a correct answer.
///
/// `Default` is written out rather than derived: a derived one would give
/// `record: ""`, which disagrees with the serde field default and made every
/// DNS check that didn't name a record fail validation.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct DnsOptions {
    /// Resolver to query. Absent uses the system resolver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nameserver: Option<String>,
    /// Record type: A, AAAA, CNAME, MX, TXT, NS.
    #[serde(default = "default_record")]
    pub record: String,
    /// Whether every expected value must appear, or just one of them.
    #[serde(default)]
    pub match_type: MatchMode,
    /// Answers the lookup must contain. Empty means "resolving at all is enough".
    #[serde(default)]
    pub expected: Vec<String>,
}

impl Default for DnsOptions {
    fn default() -> Self {
        Self {
            nameserver: None,
            record: default_record(),
            match_type: MatchMode::default(),
            expected: Vec::new(),
        }
    }
}

fn default_record() -> String {
    "A".into()
}

/// ICMP specifics. `count` echoes are sent and the check passes if any reply.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct PingOptions {
    /// "any", "ipv4" or "ipv6" — which address family to resolve the host to.
    #[serde(default = "default_ip_type")]
    pub ip_type: String,
    #[serde(default = "default_ping_count")]
    pub count: u8,
}

impl Default for PingOptions {
    fn default() -> Self {
        Self {
            ip_type: default_ip_type(),
            count: default_ping_count(),
        }
    }
}

fn default_ip_type() -> String {
    "any".into()
}
fn default_ping_count() -> u8 {
    1
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct CheckConfig {
    pub name: String,
    /// Probe type. An unknown value fails config load — a check that silently
    /// probes the wrong protocol is worse than one that refuses to start.
    #[serde(default, rename = "type")]
    pub kind: CheckKind,
    /// HTTP: a URL. TCP: "host:port". DNS/ICMP/SSL: a hostname. Heartbeat: unused.
    #[serde(default)]
    pub target: String,
    /// Extra targets probed by the same check, for "add another host".
    #[serde(default)]
    pub hosts: Vec<String>,
    /// How the hosts combine. `all` (default) means every host must pass, so the
    /// check watches a set of things that should each be up. `any` means one
    /// passing is enough, which is what you want for a redundant pool.
    #[serde(default)]
    pub host_match: MatchMode,
    /// When to run, as cron. Six fields (seconds first) so sub-minute schedules
    /// stay expressible; a five-field expression is accepted and read as
    /// "at second zero".
    #[serde(default = "default_cron")]
    pub cron: String,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Collapsible group this check belongs to on the status page. Checks with
    /// no group are listed loose, above the groups.
    #[serde(default)]
    pub group: Option<String>,
    /// Show the 24h latency graph for this check on the status page. Off by
    /// default: latency is only interesting for some checks, and a graph per
    /// check makes the page very long.
    #[serde(default)]
    pub latency_graph: bool,
    #[serde(default)]
    pub expect: Expect,
    /// Stop probing without discarding the check or its history. Editable from
    /// the admin UI; defaults on so existing config files behave unchanged.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Show this check on the public status page. Distinct from `enabled`: a
    /// check can be probed and alerted on without being published.
    #[serde(default = "default_enabled")]
    pub visible: bool,
    /// Lua deciding UP/DOWN, overriding the built-in rule. See `crate::script`.
    /// Absent means the standard 2xx-3xx logic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eval: Option<String>,
    /// Display order on the status page, within this check's section. Assigned
    /// by the store on create; the config file rarely sets it.
    #[serde(default)]
    pub position: i32,
    /// Where this check runs. Describes zones and regions rather than naming
    /// machines, so growing the fleet does not mean revisiting every check.
    #[serde(default)]
    pub placement: crate::placement::Placement,
    /// Which agents probe this check. Empty means every agent — the default and
    /// the common case. A non-empty list restricts it to those agents.
    #[serde(default)]
    pub agents: Vec<String>,
    /// Free text shown on the status page under the name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Icon for the monitor: an emoji or an image URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Short label for filtering and grouping in the admin list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// What the check reads as before it has ever reported. Without this a new
    /// check shows as an outage until its first result lands.
    #[serde(default)]
    pub default_status: DefaultStatus,
    /// Headers sent with an HTTP probe.
    #[serde(default)]
    pub headers: Vec<Header>,
    /// Accept an untrusted TLS certificate. Off by default: silently trusting a
    /// bad certificate would make an HTTPS check meaningless.
    #[serde(default)]
    pub allow_self_signed: bool,
    #[serde(default)]
    pub dns: DnsOptions,
    #[serde(default)]
    pub ping: PingOptions,
    /// Warn this many days before a TLS certificate expires (SSL checks).
    #[serde(default = "default_cert_warning_days")]
    pub cert_warning_days: u32,
    /// Heartbeat only: how long without a ping before the check is DOWN.
    #[serde(default = "default_grace_secs")]
    pub grace_secs: u64,
}

/// What a check reads as before its first result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DefaultStatus {
    Up,
    Down,
    #[default]
    Unknown,
}

fn default_cron() -> String {
    "*/30 * * * * *".into()
}
fn default_cert_warning_days() -> u32 {
    14
}
fn default_grace_secs() -> u64 {
    300
}

/// Defaults match the serde defaults, so a check built in code behaves like one
/// parsed from an empty TOML table rather than getting a blank schedule.
impl Default for CheckConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            kind: CheckKind::default(),
            target: String::new(),
            hosts: Vec::new(),
            host_match: MatchMode::default(),
            cron: default_cron(),
            timeout_secs: default_timeout_secs(),
            group: None,
            latency_graph: false,
            expect: Expect::default(),
            enabled: true,
            visible: true,
            eval: None,
            position: 0,
            placement: crate::placement::Placement::default(),
            agents: Vec::new(),
            description: None,
            image: None,
            tag: None,
            default_status: DefaultStatus::default(),
            headers: Vec::new(),
            allow_self_signed: false,
            dns: DnsOptions::default(),
            ping: PingOptions::default(),
            cert_warning_days: default_cert_warning_days(),
            grace_secs: default_grace_secs(),
        }
    }
}

impl CheckConfig {
    /// Parse the schedule. A five-field expression is padded to six so the
    /// familiar `*/5 * * * *` keeps working alongside `*/30 * * * * *`.
    pub fn schedule(&self) -> Result<cron::Schedule, String> {
        Self::parse_cron(&self.cron)
    }

    pub fn parse_cron(expr: &str) -> Result<cron::Schedule, String> {
        let trimmed = expr.trim();
        if trimmed.is_empty() {
            return Err("schedule is required".into());
        }
        let padded = match trimmed.split_whitespace().count() {
            5 => format!("0 {trimmed}"),
            _ => trimmed.to_string(),
        };
        use std::str::FromStr;
        cron::Schedule::from_str(&padded).map_err(|e| format!("invalid cron expression: {e}"))
    }

    /// Every target this check probes: the primary plus any extras.
    pub fn all_targets(&self) -> Vec<&str> {
        std::iter::once(self.target.as_str())
            .chain(self.hosts.iter().map(String::as_str))
            .filter(|t| !t.trim().is_empty())
            .collect()
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }

    /// Reject a check that could not work, before it reaches storage. The admin
    /// UI can submit anything, so the rules live with the type rather than in
    /// one handler.
    ///
    /// Returns a message written for whoever is looking at the form.
    pub fn validate(&self) -> Result<(), String> {
        let name = self.name.trim();
        if name.is_empty() {
            return Err("name is required".into());
        }
        // The name is a URL path segment and the storage key, so keep it to
        // characters that survive both without escaping surprises.
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        {
            return Err("name may only contain letters, digits, dash, underscore and dot".into());
        }
        // A heartbeat is pinged rather than probed, so it is the one kind with
        // nothing to point at.
        if self.kind != CheckKind::Heartbeat && self.target.trim().is_empty() {
            return Err("target is required".into());
        }
        let schedule = Self::parse_cron(&self.cron)?;
        // A schedule that never fires is a check that silently never runs.
        let mut upcoming = schedule.upcoming(chrono::Utc);
        let (Some(first), Some(second)) = (upcoming.next(), upcoming.next()) else {
            return Err("that schedule never runs".into());
        };
        let gap = (second - first).num_seconds();
        if gap < 5 {
            return Err("schedule must leave at least 5 seconds between runs".into());
        }
        if self.timeout_secs < 1 {
            return Err("timeout must be at least 1 second".into());
        }
        if self.timeout_secs as i64 > gap {
            return Err("timeout must not exceed the gap between runs".into());
        }
        if let Some(status) = self.expect.status {
            if self.kind != CheckKind::Http {
                return Err("expected status code only applies to HTTP checks".into());
            }
            if !(100..=599).contains(&status) {
                return Err("expected status must be between 100 and 599".into());
            }
        }
        for header in &self.headers {
            if header.name.trim().is_empty() {
                return Err("a header with no name cannot be sent".into());
            }
        }
        // Every target has to satisfy the kind's shape, not just the first —
        // otherwise an added host silently fails forever.
        for target in self.all_targets() {
            match self.kind {
                CheckKind::Http => {
                    if !(target.starts_with("http://") || target.starts_with("https://")) {
                        return Err("HTTP target must start with http:// or https://".into());
                    }
                }
                // "host:port" — a TCP probe with no port has nothing to connect to.
                CheckKind::Tcp => match target.rsplit_once(':') {
                    Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => {}
                    _ => return Err("TCP target must be host:port".into()),
                },
                CheckKind::Dns | CheckKind::Icmp | CheckKind::Ssl => {
                    if target.contains("://") || target.contains('/') {
                        return Err("target must be a hostname or IP, not a URL".into());
                    }
                }
                CheckKind::Heartbeat => {}
            }
        }
        if self.kind == CheckKind::Dns
            && !matches!(
                self.dns.record.to_ascii_uppercase().as_str(),
                "A" | "AAAA" | "CNAME" | "MX" | "TXT" | "NS"
            )
        {
            return Err("lookup record must be one of A, AAAA, CNAME, MX, TXT, NS".into());
        }
        if self.kind == CheckKind::Icmp && self.ping.count == 0 {
            return Err("ping count must be at least 1".into());
        }
        if self.kind == CheckKind::Heartbeat && self.grace_secs < 10 {
            return Err("heartbeat grace must be at least 10 seconds".into());
        }
        // A script that doesn't compile would mark the service down every
        // interval forever, so it must never reach storage.
        if let Some(src) = self
            .eval
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            crate::script::check_syntax(src)?;
        }
        Ok(())
    }
}

impl Config {
    /// Load config from `path`. If the file does not exist, fall back to
    /// built-in defaults (coordinator on 0.0.0.0:8080, no checks) so the binary
    /// runs out of the box — e.g. `docker run` with no mounted config.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            tracing::info!(path = %path.display(), "config not found, using defaults");
            return Ok(Config::default());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config = toml::from_str(&text).context("parsing config TOML")?;
        Ok(cfg)
    }
}

fn default_coordinator_url() -> String {
    "http://127.0.0.1:8080".into()
}
/// Unplaced rather than misplaced. An agent that never says where it is must
/// not be silently filed under someone else's zone.
pub const UNKNOWN_LOCATION: &str = "unknown";

pub fn default_location() -> String {
    UNKNOWN_LOCATION.into()
}

fn default_agent_id() -> String {
    "agent-01".into()
}
fn default_poll_secs() -> u64 {
    10
}

fn default_flush_secs() -> u64 {
    5
}
fn default_listen() -> String {
    "0.0.0.0:8080".into()
}
fn default_db_path() -> String {
    "warden.duckdb".into()
}
fn default_timeout_secs() -> u64 {
    10
}
fn default_enabled() -> bool {
    true
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            coordinator_url: default_coordinator_url(),
            agent_id: default_agent_id(),
            zone: default_location(),
            region: default_location(),
            flush_secs: default_flush_secs(),
            poll_secs: default_poll_secs(),
            token: None,
        }
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            db_path: default_db_path(),
            alert_webhook: None,
            admin_password: None,
            agent_token: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_check_kinds_and_nested_expect() {
        let cfg: Config = toml::from_str(
            r#"
            [[checks]]
            name = "api"
            type = "http"
            target = "https://example.com/health"
            [checks.expect]
            status = 200
            body_contains = "ok"

            [[checks]]
            name = "db"
            type = "tcp"
            target = "db.internal:5432"

            [[checks]]
            name = "resolve"
            type = "dns"
            target = "example.com"

            [[checks]]
            name = "gateway"
            type = "icmp"
            target = "192.0.2.1"
            "#,
        )
        .expect("config parses");

        let kinds: Vec<_> = cfg.checks.iter().map(|c| c.kind).collect();
        assert_eq!(
            kinds,
            vec![
                CheckKind::Http,
                CheckKind::Tcp,
                CheckKind::Dns,
                CheckKind::Icmp
            ]
        );

        let api = &cfg.checks[0];
        assert_eq!(api.expect.status, Some(200));
        assert_eq!(api.expect.body_contains.as_deref(), Some("ok"));
        // Checks without an [expect] table assert nothing.
        assert_eq!(cfg.checks[1].expect.status, None);
        assert_eq!(cfg.checks[1].expect.body_contains, None);
    }

    #[test]
    fn group_drives_dashboard_sections_and_defaults_to_none() {
        // `group` is what the status page renders its collapsible sections from,
        // so the UI always reflects the config rather than a hardcoded list.
        let cfg: Config = toml::from_str(
            r#"
            [[checks]]
            name = "web"
            target = "https://example.com"
            group = "External"

            [[checks]]
            name = "db"
            type = "tcp"
            target = "db.internal:5432"
            group = "Core Services"

            [[checks]]
            name = "loose"
            target = "https://example.org"
            "#,
        )
        .expect("config parses");

        assert_eq!(cfg.checks[0].group.as_deref(), Some("External"));
        assert_eq!(cfg.checks[1].group.as_deref(), Some("Core Services"));
        assert_eq!(
            cfg.checks[2].group, None,
            "a check with no group is listed loose, never hidden"
        );
    }

    #[test]
    fn unknown_check_type_is_rejected() {
        // A typo must fail loudly rather than silently probing HTTP.
        let err = toml::from_str::<Config>(
            r#"
            [[checks]]
            name = "oops"
            type = "gopher"
            target = "example.com"
            "#,
        )
        .expect_err("unknown type must be rejected");
        assert!(
            err.to_string().contains("gopher"),
            "error names the bad type: {err}"
        );
    }

    #[test]
    fn check_type_defaults_to_http() {
        let cfg: Config = toml::from_str(
            r#"
            [[checks]]
            name = "web"
            target = "https://example.com"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.checks[0].kind, CheckKind::Http);
    }
}
