use crate::models::CheckKind;
use crate::notify::Notifier;
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
    ///
    /// Authoritative in `standalone` mode only. In `agent` mode the coordinator
    /// takes location from the agent's credential and overwrites these, because
    /// placement acts on it and the party being placed must not choose it.
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
    /// List of notification backends for alerts (webhook, ntfy, etc.).
    #[serde(default)]
    pub notifiers: Vec<Notifier>,
    /// Password for the admin UI. Read from `WARDEN_ADMIN_PASSWORD` when unset
    /// here, which is how it should normally be supplied — a password in a
    /// config file tends to end up in version control.
    ///
    /// With neither set, every mutating endpoint refuses: an uptime monitor
    /// that silently accepts anonymous edits is worse than one you can't edit.
    #[serde(default)]
    pub admin_password: Option<String>,
    /// How long result, alert and incident history is kept. A nightly prune
    /// deletes rows older than this; raising the values keeps more history.
    #[serde(default)]
    pub retention: RetentionConfig,
    /// Coordinators to *contact first*, as base URLs
    /// (`http://coordinator-b:8080`). Empty means standalone.
    ///
    /// This is a seed list, not the membership: a node pings a seed, gets that
    /// node's whole member view back, and learns everyone else transitively.
    /// One reachable seed is therefore enough to join a cluster of any size,
    /// and a seed that is down costs nothing once the cluster is formed.
    ///
    /// Peers authenticate with `peer_token`. See `coordinator::cluster`.
    #[serde(default)]
    pub peers: Vec<String>,
    /// Shared secret peers present to each other on `/internal/*`. Unset means
    /// clustering is refused rather than accepted: those endpoints rewrite
    /// published state and edit configuration.
    #[serde(default)]
    pub peer_token: Option<String>,
    /// Stable identity of this coordinator within the cluster.
    ///
    /// Unset means one is generated on first boot and persisted in `settings`,
    /// so a restart rejoins as itself rather than appearing as a new member and
    /// leaving a ghost behind. It also breaks HLC ties, so it must be unique.
    #[serde(default)]
    pub node_id: Option<String>,
    /// How peers reach *this* node, as a base URL. Required when `peers` is
    /// non-empty: a node that cannot be dialled back can be discovered by
    /// nobody, and would replicate outward while receiving nothing.
    #[serde(default)]
    pub advertise_url: Option<String>,
    /// How often to ping every known member. Also the granularity of failure
    /// detection: nothing is noticed faster than this.
    #[serde(default = "default_gossip_interval_secs")]
    pub gossip_interval_secs: u64,
    /// Silence after which a member becomes `suspect`, and twice which it
    /// becomes `dead`. Comfortably above `gossip_interval_secs`, or a single
    /// dropped ping reads as a failure.
    #[serde(default = "default_peer_timeout_secs")]
    pub peer_timeout_secs: u64,
    /// How often to reconcile with a peer by digest exchange. This bounds how
    /// long a divergence caused by a dropped push can last, so it is a
    /// correctness backstop rather than the main replication path.
    #[serde(default = "default_anti_entropy_secs")]
    pub anti_entropy_secs: u64,
    /// How many consecutive polls an agent may miss before it stops counting
    /// toward placement and reads as unhealthy. The silence window is
    /// `missed_polls × poll_secs`, the agent's own cadence, so detection costs
    /// the same number of cycles however often it polls.
    #[serde(default = "default_missed_polls")]
    pub missed_polls: u32,
    /// What the public status page shows.
    #[serde(default)]
    pub status_page: StatusPageConfig,
    /// Credentials whose token is chosen in advance rather than minted in the
    /// UI — for a fleet whose agents must be configured before this coordinator
    /// has ever run (a compose stack, a Kubernetes manifest, a test fixture).
    ///
    /// Applied once, at boot: an id that already exists is left exactly as it
    /// is, so this neither churns the config plane on every restart nor
    /// resurrects a credential an operator revoked. In a cluster one node
    /// declaring the blocks is enough — the rest receive them over the config
    /// plane.
    #[serde(default)]
    pub provision: Vec<ProvisionedCredential>,
}

/// One `[[coordinator.provision]]` block: a credential the operator already
/// knows the token of, because the agents that will present it were configured
/// before this coordinator first ran.
///
/// Zone and region are declared here, not by the agent, for the same reason
/// they are bound to a minted credential (see `credential.rs`): placement acts
/// on location, and the party being placed must not choose it.
#[derive(Debug, Clone, Deserialize)]
pub struct ProvisionedCredential {
    pub zone: String,
    pub region: String,
    /// The token itself. Present for a fixture; a deployment should use
    /// `token_env` instead and keep the secret out of a committed file.
    #[serde(default)]
    pub token: Option<String>,
    /// Name of the environment variable holding the token.
    #[serde(default)]
    pub token_env: Option<String>,
}

impl ProvisionedCredential {
    /// Inline value first, environment second, like every other secret here.
    /// Blank is treated as absent: an unset variable expands to an empty string
    /// in most of the places these are written (compose, shell), and an empty
    /// token is not a credential.
    pub fn resolved_token(&self) -> Option<String> {
        self.token
            .clone()
            .or_else(|| {
                self.token_env
                    .as_deref()
                    .and_then(|key| std::env::var(key).ok())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// What to say when there is no token to provision. Names the variable and
    /// the zone, because the alternative failure is a fleet authenticating
    /// against nothing, and "no results" is the mode this project treats as
    /// dangerous precisely because it reads like "not scheduled yet".
    pub fn missing_token_error(&self) -> String {
        match self.token_env.as_deref() {
            Some(key) => format!(
                "coordinator.provision for zone {zone} has no token: neither an \
                 inline `token` nor the environment variable {key} is set",
                zone = self.zone
            ),
            None => format!(
                "coordinator.provision for zone {zone} has no token: set `token` \
                 for a fixture, or `token_env` naming the variable that holds it",
                zone = self.zone
            ),
        }
    }
}

/// History retention: rows older than the window are pruned periodically.
#[derive(Debug, Clone, Deserialize)]
pub struct RetentionConfig {
    /// Keep raw probe results this many days.
    #[serde(default = "default_results_days")]
    pub results_days: u64,
    /// Keep state-transition alerts and closed incidents this many days. Open
    /// incidents are never pruned — an active outage is not old history.
    #[serde(default = "default_alerts_days")]
    pub alerts_days: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            results_days: default_results_days(),
            alerts_days: default_alerts_days(),
        }
    }
}

fn default_results_days() -> u64 {
    30
}

fn default_alerts_days() -> u64 {
    90
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StatusPageConfig {
    #[serde(default)]
    pub agents: AgentsDisplay,
}

/// `[status_page.agents]` — how the fleet is summarised for anonymous readers.
///
/// The per-agent list stays admin-only; what is public is liveness, aggregated.
///
/// Also the admin wire type, so the editor and the config file cannot disagree
/// about what a setting means. The file seeds this on first boot and the store
/// is authoritative afterwards, exactly like checks.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct AgentsDisplay {
    /// Rows the operator placed by hand, in the order they appear. An entry is
    /// one agent or one whole region, so "this box, then all of us-east" is
    /// expressible — which a list of agent ids was not.
    #[serde(default)]
    pub rows: Vec<FleetEntry>,
    /// What happens to agents no explicit row already covers.
    #[serde(default)]
    pub mode: AgentsMode,
    /// Superseded by `rows`, which can name a region as well as an agent.
    /// Still read so a config file or a stored document written before the
    /// change keeps working; folded into `rows` by `sanitized` and never
    /// written back.
    #[serde(default, skip_serializing)]
    pub pinned: Vec<String>,
    /// Percent of a region's agents that must be healthy for a green row.
    /// Default 100: one silent agent is worth an amber row.
    #[serde(default = "default_warn_below")]
    pub warn_below: f64,
    /// Below this percent the region row reads down.
    #[serde(default = "default_down_below")]
    pub down_below: f64,
}

/// One row an operator placed on the public page.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum FleetEntry {
    /// One named agent, on a row of its own.
    Agent { name: String },
    /// Every agent in a region, aggregated — including any also shown
    /// individually above it. "The whole region" is read literally, because a
    /// row labelled `us-east` that quietly omitted a machine would be a lie.
    Region { name: String },
}

impl FleetEntry {
    pub fn name(&self) -> &str {
        match self {
            FleetEntry::Agent { name } | FleetEntry::Region { name } => name,
        }
    }

    fn trimmed(self) -> Self {
        match self {
            FleetEntry::Agent { name } => FleetEntry::Agent {
                name: name.trim().to_string(),
            },
            FleetEntry::Region { name } => FleetEntry::Region {
                name: name.trim().to_string(),
            },
        }
    }
}

/// What to do with agents the explicit rows do not already cover.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentsMode {
    /// Fold them into a row per region.
    #[default]
    Regions,
    /// One row each. Publishes the fleet's agent ids, so it is opt-in.
    Flat,
    /// Show only the explicit rows. With none, the section disappears.
    Off,
}

impl Default for AgentsDisplay {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            mode: AgentsMode::default(),
            pinned: Vec::new(),
            warn_below: default_warn_below(),
            down_below: default_down_below(),
        }
    }
}

impl AgentsDisplay {
    /// Trim the strings a form submits, drop the blanks and the duplicates, and
    /// absorb the superseded `pinned` list.
    ///
    /// Deduping is not cosmetic: the status page keys its rows, so a repeated
    /// entry would render two rows with the same key — which hangs a Svelte
    /// `{#each}` rather than looking wrong. An agent and a region may share a
    /// name, so identity is the pair, not the string.
    pub fn sanitized(mut self) -> Self {
        let migrated = std::mem::take(&mut self.pinned)
            .into_iter()
            .map(|name| FleetEntry::Agent { name });
        let mut seen = std::collections::HashSet::new();
        self.rows = self
            .rows
            .into_iter()
            .chain(migrated)
            .map(FleetEntry::trimmed)
            .filter(|e| !e.name().is_empty() && seen.insert(e.clone()))
            .collect();
        self
    }

    /// Rejections are read by whoever is looking at the form, so they say what
    /// to do rather than which field failed a predicate.
    pub fn validate(&self) -> Result<(), String> {
        for (name, v) in [
            ("warn_below", self.warn_below),
            ("down_below", self.down_below),
        ] {
            if !(0.0..=100.0).contains(&v) || v.is_nan() {
                return Err(format!("{name} is a percentage, so it has to be 0-100"));
            }
        }
        if self.down_below > self.warn_below {
            return Err(
                "the down threshold has to be at or below the warning threshold, \
                 otherwise a region can never read as merely degraded"
                    .into(),
            );
        }
        Ok(())
    }
}

fn default_missed_polls() -> u32 {
    3
}

fn default_gossip_interval_secs() -> u64 {
    5
}

/// Three missed pings, matching `missed_polls` for agents: one dropped packet
/// must not suspect a healthy peer.
fn default_peer_timeout_secs() -> u64 {
    15
}

fn default_anti_entropy_secs() -> u64 {
    60
}

fn default_warn_below() -> f64 {
    100.0
}

fn default_down_below() -> f64 {
    50.0
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

    /// Secret peers present to each other on `/internal/gossip`. Its own value,
    /// not shared with agents: an agent credential must never open an endpoint
    /// that rewrites published state.
    pub fn resolved_peer_token(&self) -> Option<String> {
        Self::secret(&self.peer_token, "WARDEN_PEER_TOKEN")
    }

    /// Where peers should dial this node, environment second like the secrets.
    pub fn resolved_advertise_url(&self) -> Option<String> {
        self.advertise_url
            .clone()
            .or_else(|| std::env::var("WARDEN_ADVERTISE_URL").ok())
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
    }

    pub fn resolved_node_id(&self) -> Option<String> {
        self.node_id
            .clone()
            .or_else(|| std::env::var("WARDEN_NODE_ID").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Refuse to start a half-configured cluster.
    ///
    /// Both of these otherwise fail silently and late: without a `peer_token`
    /// the node gossips outward and every receiver returns 401, and without an
    /// `advertise_url` it discovers peers that can never dial it back. Either
    /// looks exactly like a network problem for as long as it takes someone to
    /// read the logs on the *other* machine.
    pub fn validate_cluster(&self) -> Result<()> {
        if self.peers.is_empty() {
            return Ok(());
        }
        if self.resolved_peer_token().is_none() {
            anyhow::bail!(
                "coordinator.peers is set but peer_token is not: peers would refuse \
                 every message. Set peer_token (or WARDEN_PEER_TOKEN) to the same \
                 value on every coordinator."
            );
        }
        if self.resolved_advertise_url().is_none() {
            anyhow::bail!(
                "coordinator.peers is set but advertise_url is not: peers would have \
                 no address to reach this node on. Set advertise_url (or \
                 WARDEN_ADVERTISE_URL) to this node's base URL, e.g. \
                 http://coordinator-a:8080"
            );
        }
        Ok(())
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

/// A single label constraint for OpenMetrics sample selection. Exact match on
/// both name and value; a sample is selected only when its label set equals the
/// configured constraints (see `crate::openmetrics::select`).
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct MetricLabel {
    pub name: String,
    pub value: String,
}

/// OpenMetrics specifics: which sample to pull out of the scrape. The metric
/// family name is required; labels narrow an ambiguous family to one series.
/// Missing or ambiguous selection is a failed observation, never a silent zero.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct MetricOptions {
    /// The metric family name to select, e.g. `up` or `http_requests_total`.
    #[serde(default)]
    pub name: String,
    /// Exact label constraints. Empty selects the family's single unlabeled
    /// series; a family with multiple series and no labels here is ambiguous.
    #[serde(default)]
    pub labels: Vec<MetricLabel>,
}

impl MetricOptions {
    /// The labels as the `(name, value)` pairs the selector expects.
    pub fn label_pairs(&self) -> Vec<(String, String)> {
        self.labels
            .iter()
            .map(|l| (l.name.clone(), l.value.clone()))
            .collect()
    }
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
    /// Whether a state change on this check reaches the notifiers. Distinct
    /// from `enabled` and `visible`: some checks are just checks — worth
    /// probing and charting, not worth waking anyone for. The incident and the
    /// alert row are recorded either way; only the notification is withheld.
    #[serde(default = "default_enabled")]
    pub notify: bool,
    /// Which notifiers this check's alerts go to, by name. Empty means the
    /// ones marked as defaults, so a check needs no routing at all until two
    /// audiences exist. Ignored entirely when `notify` is off.
    ///
    /// A name no notifier has any more routes nowhere; the notifier admin page
    /// lists those checks rather than letting them go quietly unrouted.
    #[serde(default)]
    pub notifiers: Vec<String>,
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
    /// OpenMetrics sample selection. Only meaningful for the `openmetrics` kind.
    #[serde(default)]
    pub metric: MetricOptions,
    /// Warn this many days before a TLS certificate expires (SSL checks).
    #[serde(default = "default_cert_warning_days")]
    pub cert_warning_days: u32,
    /// Heartbeat only: how long without a ping before the check is DOWN.
    #[serde(default = "default_grace_secs")]
    pub grace_secs: u64,
    /// How many consecutive failed probes confirm a DOWN. One failed probe does
    /// not flip the state: the check reads DOWN (and alerts, and opens an
    /// incident) only after this many failures in a row, so a single dropped
    /// probe or a flaky hop does not page anyone. 1 is the classic behaviour.
    #[serde(default = "default_failures_before_down")]
    pub failures_before_down: u32,
    /// Checks this one sits behind. When this check confirms DOWN and any
    /// upstream named here already has an open incident, this check joins that
    /// incident instead of opening its own — one outage, one entry on the
    /// status page, rather than a row per symptom.
    ///
    /// The lookup is one hop, deliberately: `web -> api -> db` with all three
    /// down gives two incidents, not one. Transitive grouping needs a cycle
    /// guard and a tie-break for a check reachable from two roots, and one hop
    /// covers the case this exists for — a fan-out of checks behind a single
    /// dependency. It is also cycle-safe by construction, so a mutual
    /// `depends_on` pair simply gives the incident to whichever fell first.
    ///
    /// Grouping does not change who gets paged: the dependent still records its
    /// own alert and still notifies per its own routing.
    #[serde(default)]
    pub depends_on: Vec<String>,
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
fn default_failures_before_down() -> u32 {
    1
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
            notify: true,
            notifiers: Vec::new(),
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
            metric: MetricOptions::default(),
            cert_warning_days: default_cert_warning_days(),
            grace_secs: default_grace_secs(),
            failures_before_down: default_failures_before_down(),
            depends_on: Vec::new(),
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
                // The exporter is scraped over HTTP, so the target is a URL.
                CheckKind::Openmetrics => {
                    if !(target.starts_with("http://") || target.starts_with("https://")) {
                        return Err("OpenMetrics target must start with http:// or https://".into());
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
        if self.kind == CheckKind::Openmetrics {
            // One scrape selects one sample; aggregating across several exporters
            // has no defined meaning here, and silently probing only the last
            // host would hide the rest. Reject extra hosts rather than guess.
            if !self.hosts.is_empty() {
                return Err(
                    "OpenMetrics checks probe a single target; extra hosts are not supported"
                        .into(),
                );
            }
            if self.metric.name.trim().is_empty() {
                return Err("metric name is required for OpenMetrics checks".into());
            }
            for label in &self.metric.labels {
                if label.name.trim().is_empty() {
                    return Err("a metric label with no name is invalid".into());
                }
            }
        }
        if self.kind == CheckKind::Icmp && self.ping.count == 0 {
            return Err("ping count must be at least 1".into());
        }
        if self.kind == CheckKind::Heartbeat && self.grace_secs < 10 {
            return Err("heartbeat grace must be at least 10 seconds".into());
        }
        // 0 failures "before" down would mean down already: the confirmation
        // threshold exists to swallow a dropped probe, not to distrust every
        // result. Treat 0 as 1 so a hand-typed config cannot create a check
        // that never flips.
        if self.failures_before_down == 0 {
            return Err("failures_before_down must be at least 1".into());
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
            notifiers: Vec::new(),
            admin_password: None,
            retention: RetentionConfig::default(),
            peers: Vec::new(),
            peer_token: None,
            node_id: None,
            advertise_url: None,
            gossip_interval_secs: default_gossip_interval_secs(),
            peer_timeout_secs: default_peer_timeout_secs(),
            anti_entropy_secs: default_anti_entropy_secs(),
            missed_polls: default_missed_polls(),
            status_page: StatusPageConfig::default(),
            provision: Vec::new(),
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

    fn openmetrics_check() -> CheckConfig {
        CheckConfig {
            name: "exporter".into(),
            kind: CheckKind::Openmetrics,
            target: "http://exporter.internal:9100/metrics".into(),
            metric: MetricOptions {
                name: "up".into(),
                labels: vec![MetricLabel {
                    name: "job".into(),
                    value: "node".into(),
                }],
            },
            ..Default::default()
        }
    }

    #[test]
    fn valid_openmetrics_check_passes_validation() {
        openmetrics_check().validate().expect("should validate");
    }

    #[test]
    fn openmetrics_requires_metric_name() {
        let mut c = openmetrics_check();
        c.metric.name = "  ".into();
        assert!(c
            .validate()
            .unwrap_err()
            .contains("metric name is required"));
    }

    #[test]
    fn openmetrics_rejects_empty_label_name() {
        let mut c = openmetrics_check();
        c.metric.labels[0].name = "".into();
        assert!(c
            .validate()
            .unwrap_err()
            .contains("metric label with no name"));
    }

    #[test]
    fn openmetrics_target_must_be_http_url() {
        let mut c = openmetrics_check();
        c.target = "exporter.internal:9100".into();
        assert!(c
            .validate()
            .unwrap_err()
            .contains("OpenMetrics target must start with http"));
    }

    #[test]
    fn openmetrics_rejects_extra_hosts() {
        let mut c = openmetrics_check();
        c.hosts = vec!["http://other:9100/metrics".into()];
        assert!(c.validate().unwrap_err().contains("single target"));
    }

    #[test]
    fn prometheus_alias_deserializes_to_openmetrics() {
        let cfg: Config = toml::from_str(
            r#"
            [[checks]]
            name = "exp"
            type = "prometheus"
            target = "http://exp:9100/metrics"
            [checks.metric]
            name = "up"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.checks[0].kind, CheckKind::Openmetrics);
        assert_eq!(cfg.checks[0].metric.name, "up");
    }
}
