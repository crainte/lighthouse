use crate::models::CheckKind;
use crate::notify::Notifier;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub coordinator: CoordinatorConfig,
    /// Global-coordinator settings. Only consulted in `global` mode; a regional
    /// coordinator ignores it, and a regional deployment need not write it.
    #[serde(default)]
    pub global: GlobalConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    /// Where to POST results and poll for work. Ignored in standalone (writes
    /// in-process).
    ///
    /// Accepts one URL or a list of them, so an existing
    /// `coordinator_url = "http://coordinator-a:8080"` keeps working unchanged.
    /// A list is failover, not sharding: the coordinators in a region are
    /// interchangeable for an agent — config replicates through the config
    /// plane and per-agent beliefs replicate through the data plane, so it does
    /// not matter which one is answering. Pinning an agent to a single
    /// coordinator would make that coordinator a single point of failure for
    /// everything the agent probes, which is the opposite of why the region has
    /// three of them.
    ///
    /// Order is preference, not priority: whichever entry answered last is the
    /// one still in use, and there is no failback. Flapping between endpoints
    /// on every blip would move placement around for no benefit.
    #[serde(default = "default_coordinator_url", deserialize_with = "one_or_many")]
    pub coordinator_url: Vec<String>,
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
    /// At most this many probes may execute concurrently on this agent.
    /// Shared across every running check loop, including loop generations after a
    /// coordinator sync. Must be positive; no upper limit is imposed.
    #[serde(default = "default_max_in_flight_probes")]
    pub max_in_flight_probes: usize,
    /// Shared secret presented to the coordinator, falling back to
    /// `LIGHTHOUSE_AGENT_TOKEN`. Must match the coordinator's `agent_token`.
    #[serde(default)]
    pub token: Option<String>,
    /// Which entry of `coordinator_url` is currently believed reachable.
    ///
    /// Shared rather than per-loop: `AgentConfig` is cloned into the sync loop
    /// and the poster loop, and cloning an `Arc` shares it, so both loops fail
    /// over together. Split cursors would leave the agent polling one
    /// coordinator for work while posting results to another — legal, since
    /// both replicate, but an operator reading two sets of logs to answer "who
    /// is this agent talking to?" is a needless puzzle.
    #[serde(skip)]
    cursor: Arc<AtomicUsize>,
}

impl AgentConfig {
    /// The coordinator currently in use, without a trailing slash.
    pub fn coordinator(&self) -> &str {
        let urls = &self.coordinator_url;
        let url = urls
            .get(self.cursor.load(Ordering::Relaxed) % urls.len().max(1))
            .map_or("", String::as_str);
        url.trim_end_matches('/')
    }

    /// Give up on the current coordinator and move to the next.
    ///
    /// Called only when a request did not get an answer at all. A coordinator
    /// that *rejects* a request is up and has an opinion — rotating away from a
    /// 401 would just spread a bad token across the region and turn one clear
    /// error into three confusing ones.
    ///
    /// Reports the move itself, rather than leaving each caller to. Both agent
    /// loops share the cursor and race to notice an outage; the first one to
    /// rotate leaves the other succeeding against the new endpoint with nothing
    /// to report, so a per-caller log prints only when the *slower* loop
    /// happens to win. Failing over is exactly the event an operator needs in
    /// the log, and it was silent in the common case.
    pub fn rotate_coordinator(&self) {
        if self.coordinator_url.len() < 2 {
            return;
        }
        let from = self.coordinator().to_string();
        self.cursor.fetch_add(1, Ordering::Relaxed);
        tracing::info!(from = %from, to = %self.coordinator(), "failing over to another coordinator");
    }

    pub fn resolved_token(&self) -> Option<String> {
        self.token
            .clone()
            .or_else(|| std::env::var("LIGHTHOUSE_AGENT_TOKEN").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Refuse a zero-sized execution budget rather than starting check loops
    /// that can never acquire a permit.
    pub fn validate(&self) -> Result<()> {
        if self.max_in_flight_probes == 0 {
            anyhow::bail!("agent.max_in_flight_probes must be positive");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoordinatorConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default)]
    pub database: DatabaseConfig,
    /// List of notification backends for alerts (webhook, ntfy, etc.).
    #[serde(default)]
    pub notifiers: Vec<Notifier>,
    /// Password for the admin UI. Read from `LIGHTHOUSE_ADMIN_PASSWORD` when unset
    /// here, which is how it should normally be supplied — a password in a
    /// config file tends to end up in version control.
    ///
    /// With neither set, every mutating endpoint refuses: an uptime monitor
    /// that silently accepts anonymous edits is worse than one you can't edit.
    #[serde(default)]
    pub admin_password: Option<String>,
    /// Mark admin session cookies `Secure`. Enable this when TLS terminates at
    /// this server or at a trusted proxy; it is never inferred from forwarded
    /// request headers, which untrusted clients can forge.
    #[serde(default)]
    pub secure_cookies: bool,
    /// Reverse proxies (or load balancers) allowed to supply `X-Forwarded-For`,
    /// as bare IPs or CIDR blocks (`"10.0.0.0/8"`).
    ///
    /// Empty is the default and the safe one: login rate limiting keys on the
    /// raw peer address exactly as it always has, and a header a client set
    /// itself is never consulted. Populating this list is what lets a
    /// deployment behind a reverse proxy rate-limit per real client instead
    /// of per shared proxy bucket — see `crate::security::client_ip`.
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
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
    /// Fleet-wide fallbacks for the per-check rollup knobs.
    ///
    /// A check that sets none of `min_incident_secs`, `min_recovery_secs`,
    /// `notify_degraded` or `degraded_is_downtime` inherits these. This is the
    /// lever that turns hold-down on for a whole deployment at once, which is
    /// how a single-instance install gets protection from phantom incidents
    /// without editing every check.
    #[serde(default)]
    pub rollup_defaults: RollupDefaults,
    /// Which region this coordinator belongs to, used only to label the state
    /// summaries it pushes to a global coordinator (Stage B / Phase 6). A
    /// single-region deployment never reads this; it defaults to `"default"`
    /// and changes nothing until a `[coordinator.global]` push target is set.
    #[serde(default = "default_region")]
    pub region: String,
    /// Where and how to push this coordinator's state summary to a global
    /// coordinator. Absent (the common case) means no push happens — every
    /// completed Stage A deployment behaves exactly as before.
    #[serde(default)]
    pub global: Option<GlobalPushConfig>,
}

/// `[coordinator.global]` — a regional coordinator's link up to the global
/// coordinator. Presence of this block is what turns the push loop on.
///
/// Give every coordinator of a region the same block: only the lowest-id
/// live member publishes, and the others take over only when it dies, so a
/// member without the block would leave its region unpublished while it is
/// the lowest id.
#[derive(Debug, Clone, Deserialize)]
pub struct GlobalPushConfig {
    /// Base URL of the global coordinator, e.g. `http://global:8080`.
    pub url: String,
    /// This region's registration key (`wac_<id>.<secret>`), minted in the
    /// global Admin > Regions or provisioned with `[[global.provision]]`.
    /// The global node binds it to this coordinator's `region`, so it attaches
    /// the region rather than authenticating a shared secret, and the global
    /// tier calls back with a token derived from it
    /// (`credential::relay_token`) that only this region accepts.
    #[serde(default)]
    pub token: Option<String>,
    /// Environment variable holding the token, when it is not inline.
    #[serde(default)]
    pub token_env: Option<String>,
    /// How often to push, in seconds. The design allows 5-10s; 5 keeps the
    /// global dashboard within a probe interval of the regions.
    #[serde(default = "default_push_secs")]
    pub push_secs: u64,
}

impl GlobalPushConfig {
    /// Inline value first, environment second, blank treated as absent —
    /// exactly like every other secret here.
    pub fn resolved_token(&self) -> Option<String> {
        self.token
            .clone()
            .or_else(|| {
                self.token_env
                    .as_deref()
                    .and_then(|key| std::env::var(key).ok())
            })
            .or_else(|| std::env::var("LIGHTHOUSE_REGION_KEY").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }
}

/// `[global]` — the global coordinator itself (`lighthouse global`). Caches state
/// summaries pushed by regional coordinators, serves an aggregated dashboard,
/// and routes drill-down reads back to the owning region. Holds no raw data.
///
/// Unknown keys are refused so a leftover `[[global.region]]` or `token` from
/// before per-region registration fails loudly instead of being ignored.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    #[serde(default = "default_global_listen")]
    pub listen: String,
    /// Admin password for the aggregated dashboard, `LIGHTHOUSE_ADMIN_PASSWORD`
    /// when unset — same contract as a coordinator.
    #[serde(default)]
    pub admin_password: Option<String>,
    /// Mark admin session cookies `Secure`; explicitly enable when this node is
    /// served over TLS or behind a trusted TLS-terminating proxy.
    #[serde(default)]
    pub secure_cookies: bool,
    /// Reverse proxies allowed to supply `X-Forwarded-For` for this node's
    /// own login rate limiting. See `coordinator.trusted_proxies`; the same
    /// contract, kept as a second field because it names a different
    /// deployment's proxies rather than sharing this project's only other
    /// notion of trust.
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
    /// Seconds without a push after which a region reads as dark (its health
    /// goes Unknown and, if notifiers are set, a region-down alert fires).
    #[serde(default = "default_region_staleness_secs")]
    pub region_staleness_secs: u64,
    /// Cross-region alert backends. Empty means the global node never pages —
    /// the regions already alert, and double-paging is the failure mode to
    /// avoid. Populated, it pages only on conditions a single region cannot
    /// see: a check down in every reporting region, or a region going dark.
    #[serde(default)]
    pub notifiers: Vec<Notifier>,
    /// Region registration keys chosen in advance, for a deployment whose
    /// regional coordinators are configured before this node first runs (a
    /// compose stack, a manifest). `zone` is not used. Applied once at boot: a
    /// region already registered, or one an operator removed, is left alone.
    /// Regions themselves are not declared — one appears when its first
    /// authenticated push arrives.
    #[serde(default)]
    pub provision: Vec<ProvisionedCredential>,
    /// Where the global tier's own catalogue lives (SCALING.md "Placement-scoped
    /// configuration", Phase 1). Unlike a coordinator this holds no results or
    /// rollups — `Storage::open` still creates those tables because it is the
    /// same schema, but nothing ever writes a row into them from here; only
    /// `checks` and `sections` (see `global::store`) are ever touched.
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default)]
    pub database: DatabaseConfig,
    /// Other global nodes to *contact first*, as base URLs. Same contract as
    /// `coordinator.peers`: a seed list, not the membership, and peers
    /// authenticate with `peer_token`. Empty means a standalone global node,
    /// which is a complete Phase 1 deployment on its own — Phase 4 is running
    /// three of them with this set.
    #[serde(default)]
    pub peers: Vec<String>,
    /// Shared secret global nodes present to each other on `/internal/*`.
    /// Unset means clustering is refused, exactly like `coordinator.peer_token`.
    #[serde(default)]
    pub peer_token: Option<String>,
    /// Stable identity of this global node within its own cluster. Unset means
    /// one is generated on first boot and persisted in `settings`, so a restart
    /// rejoins as itself. Also breaks HLC ties for the catalogue's clock.
    #[serde(default)]
    pub node_id: Option<String>,
    /// How peers reach *this* node, as a base URL. Required when `peers` is
    /// non-empty, for the same reason as a coordinator's.
    #[serde(default)]
    pub advertise_url: Option<String>,
    /// How often this node pings every known global peer.
    #[serde(default = "default_gossip_interval_secs")]
    pub gossip_interval_secs: u64,
    /// Silence after which a peer becomes `suspect`, then `dead`.
    #[serde(default = "default_peer_timeout_secs")]
    pub peer_timeout_secs: u64,
    /// How often this node reconciles its catalogue with a peer global node by
    /// digest, and with every region it knows of.
    #[serde(default = "default_anti_entropy_secs")]
    pub anti_entropy_secs: u64,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            listen: default_global_listen(),
            admin_password: None,
            secure_cookies: false,
            trusted_proxies: Vec::new(),
            region_staleness_secs: default_region_staleness_secs(),
            notifiers: Vec::new(),
            provision: Vec::new(),
            db_path: default_db_path(),
            database: DatabaseConfig::default(),
            peers: Vec::new(),
            peer_token: None,
            node_id: None,
            advertise_url: None,
            gossip_interval_secs: default_gossip_interval_secs(),
            peer_timeout_secs: default_peer_timeout_secs(),
            anti_entropy_secs: default_anti_entropy_secs(),
        }
    }
}

impl GlobalConfig {
    pub fn resolved_admin_password(&self) -> Option<String> {
        self.admin_password
            .clone()
            .or_else(|| std::env::var("LIGHTHOUSE_ADMIN_PASSWORD").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Secret global peers present to each other on `/internal/*`. Same
    /// environment fallback name as a coordinator's: a process is always
    /// exactly one of the two, so the two never collide.
    pub fn resolved_peer_token(&self) -> Option<String> {
        self.peer_token
            .clone()
            .or_else(|| std::env::var("LIGHTHOUSE_PEER_TOKEN").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    pub fn resolved_advertise_url(&self) -> Option<String> {
        self.advertise_url
            .clone()
            .or_else(|| std::env::var("LIGHTHOUSE_ADVERTISE_URL").ok())
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
    }

    pub fn resolved_node_id(&self) -> Option<String> {
        self.node_id
            .clone()
            .or_else(|| std::env::var("LIGHTHOUSE_NODE_ID").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Refuse to start with a provision block that could never authenticate,
    /// or a half-configured global-to-global cluster — same reasoning as
    /// `CoordinatorConfig::validate_cluster`.
    pub fn validate(&self) -> Result<()> {
        for block in &self.provision {
            crate::global::regions::valid_name(&block.region)
                .map_err(|e| anyhow::anyhow!("global.provision region {:?}: {e}", block.region))?;
            let token = block
                .resolved_token()
                .ok_or_else(|| anyhow::anyhow!(block.missing_token_error()))?;
            if crate::credential::split(&token).is_none() {
                anyhow::bail!(
                    "global.provision for region {} has a malformed token: expected \
                     the `wac_<id>.<secret>` form Admin > Regions mints",
                    block.region
                );
            }
        }
        crate::security::TrustedProxies::parse(&self.trusted_proxies)
            .map_err(|e| anyhow::anyhow!("global.trusted_proxies: {e}"))?;
        if !self.peers.is_empty() {
            if self.resolved_peer_token().is_none() {
                anyhow::bail!(
                    "global.peers is set but peer_token is not: peers would refuse \
                     every message. Set peer_token (or LIGHTHOUSE_PEER_TOKEN) to the \
                     same value on every global node."
                );
            }
            if self.resolved_advertise_url().is_none() {
                anyhow::bail!(
                    "global.peers is set but advertise_url is not: peers would have \
                     no address to reach this node on. Set advertise_url (or \
                     LIGHTHOUSE_ADVERTISE_URL) to this node's base URL, e.g. \
                     http://global-a:8080"
                );
            }
        }
        Ok(())
    }
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
    /// Required for `[[coordinator.provision]]`; unused by `[[global.provision]]`,
    /// whose key binds a whole region.
    #[serde(default)]
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
        let scope = if self.zone.is_empty() {
            format!("global.provision for region {}", self.region)
        } else {
            format!("coordinator.provision for zone {}", self.zone)
        };
        match self.token_env.as_deref() {
            Some(key) => format!(
                "{scope} has no token: neither an inline `token` nor the \
                 environment variable {key} is set"
            ),
            None => format!(
                "{scope} has no token: set `token` for a fixture, or `token_env` \
                 naming the variable that holds it"
            ),
        }
    }
}

/// `[coordinator.database]` / `[global.database]` — DuckDB resource bounds.
/// Unset values are derived from the container's cgroup limits (see
/// `coordinator::db_limits`), so a pod's `resources.limits` is respected
/// without configuration; set them to pin the values explicitly.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DatabaseConfig {
    /// DuckDB `memory_limit`, e.g. `"2GiB"`. Env: `LIGHTHOUSE_DB_MEMORY_LIMIT`.
    #[serde(default)]
    pub memory_limit: Option<String>,
    /// DuckDB worker threads. Env: `LIGHTHOUSE_DB_THREADS`.
    #[serde(default)]
    pub threads: Option<u32>,
}

impl DatabaseConfig {
    pub fn resolved_memory_limit(&self) -> Option<String> {
        self.memory_limit
            .clone()
            .or_else(|| std::env::var("LIGHTHOUSE_DB_MEMORY_LIMIT").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    pub fn resolved_threads(&self) -> Result<Option<u32>> {
        if let Some(threads) = self.threads {
            return Ok(Some(threads));
        }
        match std::env::var("LIGHTHOUSE_DB_THREADS") {
            Ok(value) if !value.trim().is_empty() => {
                value.trim().parse().map(Some).map_err(|_| {
                    anyhow::anyhow!("LIGHTHOUSE_DB_THREADS must be a positive integer")
                })
            }
            _ => Ok(None),
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

fn default_region() -> String {
    "default".to_string()
}

fn default_push_secs() -> u64 {
    5
}

fn default_global_listen() -> String {
    "0.0.0.0:8080".to_string()
}

/// Four push intervals of silence, mirroring the peer/agent "three misses plus
/// slack" rule: one dropped push must not blank a healthy region.
fn default_region_staleness_secs() -> u64 {
    30
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
        Self::secret(&self.admin_password, "LIGHTHOUSE_ADMIN_PASSWORD")
    }

    /// Secret peers present to each other on `/internal/gossip`. Its own value,
    /// not shared with agents: an agent credential must never open an endpoint
    /// that rewrites published state.
    pub fn resolved_peer_token(&self) -> Option<String> {
        Self::secret(&self.peer_token, "LIGHTHOUSE_PEER_TOKEN")
    }

    /// Where peers should dial this node, environment second like the secrets.
    pub fn resolved_advertise_url(&self) -> Option<String> {
        self.advertise_url
            .clone()
            .or_else(|| std::env::var("LIGHTHOUSE_ADVERTISE_URL").ok())
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
    }

    pub fn resolved_node_id(&self) -> Option<String> {
        self.node_id
            .clone()
            .or_else(|| std::env::var("LIGHTHOUSE_NODE_ID").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// The global push target and its resolved token, when both are set.
    /// Returns `None` — push disabled — when no `[coordinator.global]` block
    /// exists, so a regional-only deployment is untouched. Errors when the
    /// block is present but its token cannot be resolved, because a push that
    /// every receiver 401s is the same silent-failure trap as a tokenless
    /// cluster.
    pub fn resolved_global_push(&self) -> Result<Option<(String, String, u64)>> {
        let Some(g) = &self.global else {
            return Ok(None);
        };
        let url = g.url.trim().trim_end_matches('/').to_string();
        if url.is_empty() {
            anyhow::bail!("coordinator.global.url is set but empty");
        }
        let token = g.resolved_token().ok_or_else(|| {
            anyhow::anyhow!(
                "coordinator.global is set but its token is not: set \
                 [coordinator.global] token (or token_env, or LIGHTHOUSE_REGION_KEY) \
                 to this region's registration key from the global Admin > Regions."
            )
        })?;
        if crate::credential::split(&token).is_none() {
            anyhow::bail!(
                "coordinator.global token is not a region registration key: expected \
                 the `wac_<id>.<secret>` form the global Admin > Regions mints"
            );
        }
        Ok(Some((url, token, g.push_secs.max(1))))
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
                 every message. Set peer_token (or LIGHTHOUSE_PEER_TOKEN) to the same \
                 value on every coordinator."
            );
        }
        if self.resolved_advertise_url().is_none() {
            anyhow::bail!(
                "coordinator.peers is set but advertise_url is not: peers would have \
                 no address to reach this node on. Set advertise_url (or \
                 LIGHTHOUSE_ADVERTISE_URL) to this node's base URL, e.g. \
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

/// Expand `${NAME}` references from this coordinator's environment. Check
/// documents carry names, not credentials: values are deliberately resolved
/// only immediately before a regional coordinator hands work to its agents.
///
/// The resolver reports the missing *name*, never a value or the surrounding
/// target/header, so the error is safe to expose as a configuration problem.
pub fn resolve_secret_references(
    value: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<String, String> {
    let mut resolved = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        resolved.push_str(&rest[..start]);
        let after_open = &rest[start + 2..];
        let Some(end) = after_open.find('}') else {
            return Err("invalid secret reference; expected ${NAME}".into());
        };
        let name = &after_open[..end];
        if name.is_empty()
            || !name.chars().enumerate().all(|(i, c)| {
                if i == 0 {
                    c == '_' || c.is_ascii_alphabetic()
                } else {
                    c == '_' || c.is_ascii_alphanumeric()
                }
            })
        {
            return Err("invalid secret reference; expected ${NAME}".into());
        }
        let Some(secret) = lookup(name) else {
            return Err(format!(
                "missing environment variable {name} for check secret reference"
            ));
        };
        resolved.push_str(&secret);
        rest = &after_open[end + 1..];
    }
    resolved.push_str(rest);
    Ok(resolved)
}

fn has_secret_reference(value: &str) -> bool {
    value.contains("${")
}

fn target_has_url_userinfo(target: &str) -> bool {
    let Some(scheme) = target.find("://") else {
        return false;
    };
    let authority = &target[scheme + 3..];
    authority
        .split(['/', '?', '#'])
        .next()
        .is_some_and(|authority| authority.contains('@'))
}

fn has_sensitive_query_value(target: &str) -> bool {
    let Some((_, query_and_fragment)) = target.split_once('?') else {
        return false;
    };
    let query = query_and_fragment.split('#').next().unwrap_or_default();
    query.split('&').any(|part| {
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        matches!(
            key.to_ascii_lowercase().as_str(),
            "access_token"
                | "api_key"
                | "apikey"
                | "auth"
                | "key"
                | "password"
                | "passwd"
                | "secret"
                | "token"
        ) && !has_secret_reference(value)
    })
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

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DnsMode {
    #[default]
    Standard,
    Dnssec,
}

/// What a DNS probe asks for and what counts as a correct answer.
///
/// `Default` is written out rather than derived: a derived one would give
/// `record: ""`, which disagrees with the serde field default and made every
/// DNS check that didn't name a record fail validation.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct DnsOptions {
    #[serde(default)]
    pub mode: DnsMode,
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
            mode: DnsMode::default(),
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

pub(crate) fn dns_nameserver(value: &str) -> std::result::Result<SocketAddr, String> {
    let value = value.trim();
    if let Ok(addr) = value.parse::<SocketAddr>() {
        return Ok(addr);
    }
    value
        .parse::<IpAddr>()
        .map(|ip| SocketAddr::new(ip, 53))
        .map_err(|_| "nameserver must be an IP address, optionally with a port".into())
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
    /// Immutable identity, generated once at creation as a UUIDv7 and never
    /// reassigned. `name` is the mutable display value; every stored
    /// relationship (results, current state, agent beliefs, incidents,
    /// notification queues, dependencies, maintenance) keys on this instead.
    ///
    /// The default here is a safety net for a document that predates this
    /// field (or a struct literal built without setting it) rather than the
    /// normal path: public admin creation always mints a fresh one and
    /// ignores whatever a client sent (see `create_check_handler`), and a
    /// trusted internal caller — regional creation folded into a global
    /// check, replication, fanout — supplies an existing id explicitly.
    #[serde(default = "crate::models::new_check_id")]
    pub id: crate::models::CheckId,
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
    /// Headers sent with an HTTP probe. Values may contain `${NAME}` references,
    /// resolved only by the regional coordinator when assigning an agent.
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
    /// Seconds between confirmation attempts after a failure, before DOWN is
    /// confirmed. When omitted, derived from the normal schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_interval_secs: Option<u64>,
    /// Seconds between probes while DOWN. A successful probe immediately
    /// returns the check to its normal cron schedule. When omitted, derived
    /// from the normal schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub down_interval_secs: Option<u64>,
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
    pub depends_on: Vec<crate::models::CheckId>,
    /// Per-level overrides of the region tree's node policies. `None` at a
    /// level means that node's own stored policy, or the built-in default for
    /// the level.
    ///
    /// Overrides exist because a policy is a statement about *this check*,
    /// not only about the fleet: a service that is genuinely served from one
    /// region should not be called degraded by a rule written for globally
    /// anycast ones.
    #[serde(default)]
    pub rollup: RollupOverrides,
    /// How long the rollup must continuously argue for a non-Up state before
    /// the published state moves. `0` — the default — is today's behaviour, so
    /// upgrading changes nothing until an operator asks for it.
    ///
    /// This is the fix that reaches single-instance deployments. With one
    /// agent there is nobody to corroborate with, so the tree is a pass-through
    /// and time is the only evidence available that a failure is real.
    ///
    /// Mind the interaction with the schedule: on a 60s interval, a 120s
    /// hold-down announces a genuine outage roughly three minutes after it
    /// starts. That is the trade being bought, not a bug.
    ///
    /// `None` falls back to `[coordinator.rollup_defaults]`, then to 0.
    /// Optional rather than a bare `u64` so that "unset" and "explicitly zero"
    /// stay distinguishable — otherwise a fleet-wide hold-down could never be
    /// switched *off* for one noisy check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_incident_secs: Option<u64>,
    /// The up-side mirror: how long a recovery must hold before the check is
    /// published as Up again. Useful for a service that comes back, falls over,
    /// and comes back again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_recovery_secs: Option<u64>,
    /// Whether a Degraded transition reaches the notifiers. Down always does,
    /// subject to `notify`.
    ///
    /// On by default: a partial regional outage is real enough to wake someone.
    /// Off is for checks where one region dropping out is routine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notify_degraded: Option<bool>,
    /// Whether Degraded minutes count against the 24h uptime figure.
    ///
    /// Off by default, and the asymmetry with `notify_degraded` is deliberate:
    /// the outage is worth telling someone about, but the service *was*
    /// reachable, so the SLA number should say so rather than quietly charging
    /// a full outage for a partial one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degraded_is_downtime: Option<bool>,
}

/// Fleet-wide fallbacks for the per-check region-tree settings, so an operator
/// who wants a 120-second hold-down everywhere says it once.
///
/// Every field here is a plain value with a built-in default; the *check* is
/// what carries options. That way the fallback chain has exactly one place it
/// can bottom out, and reading a check's effective setting is a single
/// `unwrap_or`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct RollupDefaults {
    #[serde(default)]
    pub min_incident_secs: u64,
    #[serde(default)]
    pub min_recovery_secs: u64,
    #[serde(default = "default_enabled")]
    pub notify_degraded: bool,
    #[serde(default)]
    pub degraded_is_downtime: bool,
}

impl Default for RollupDefaults {
    fn default() -> Self {
        Self {
            // Zero, so upgrading to the region tree changes nothing about
            // when a deployment pages until someone asks it to.
            min_incident_secs: 0,
            min_recovery_secs: 0,
            notify_degraded: true,
            degraded_is_downtime: false,
        }
    }
}

/// Per-check overrides of the region tree's rollup policies, one per level.
///
/// A level left `None` falls through to that node's stored policy, and then to
/// the built-in default — so a check that cares about only one level says only
/// that, rather than having to restate the whole tree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct RollupOverrides {
    /// Agents → zone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<crate::rollup::RollupPolicy>,
    /// Zones → region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<crate::rollup::RollupPolicy>,
    /// Regions → the check itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global: Option<crate::rollup::RollupPolicy>,
}

impl RollupOverrides {
    fn validate(&self) -> Result<(), String> {
        for (level, policy) in [
            ("zone", self.zone),
            ("region", self.region),
            ("global", self.global),
        ] {
            if let Some(p) = policy {
                p.validate().map_err(|e| format!("rollup.{level}: {e}"))?;
            }
        }
        Ok(())
    }
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

pub const NEW_CHECK_FAILURES_BEFORE_DOWN: u32 = 2;

/// Defaults match the serde defaults, so a check built in code behaves like one
/// parsed from an empty TOML table rather than getting a blank schedule.
impl Default for CheckConfig {
    fn default() -> Self {
        Self {
            id: crate::models::new_check_id(),
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
            retry_interval_secs: None,
            down_interval_secs: None,
            depends_on: Vec::new(),
            rollup: RollupOverrides::default(),
            min_incident_secs: None,
            min_recovery_secs: None,
            notify_degraded: None,
            degraded_is_downtime: None,
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

    /// Resolve this document for one region's agents. The stored/admin form
    /// remains references, preventing credentials from crossing regions or
    /// returning through the admin API.
    pub fn resolve_secret_references(
        &self,
        lookup: impl Fn(&str) -> Option<String> + Copy,
    ) -> Result<Self, String> {
        let mut resolved = self.clone();
        resolved.target = resolve_secret_references(&self.target, lookup)?;
        resolved.hosts = self
            .hosts
            .iter()
            .map(|host| resolve_secret_references(host, lookup))
            .collect::<Result<_, _>>()?;
        resolved.headers = self
            .headers
            .iter()
            .map(|header| {
                Ok(Header {
                    name: header.name.clone(),
                    value: resolve_secret_references(&header.value, lookup)?,
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(resolved)
    }

    /// Resolve references against this region's coordinator process environment
    /// at agent hand-off. Kept separate from storage and admin serialization.
    pub fn resolve_for_agent(&self) -> Result<Self, String> {
        self.resolve_secret_references(|name| std::env::var(name).ok())
    }

    fn schedule_gap_secs(&self) -> Result<u64, String> {
        let schedule = self.schedule()?;
        let mut upcoming = schedule.upcoming(chrono::Utc);
        let (Some(first), Some(second)) = (upcoming.next(), upcoming.next()) else {
            return Err("that schedule never runs".into());
        };
        u64::try_from((second - first).num_seconds())
            .map_err(|_| "schedule must move forward".into())
    }

    pub fn retry_interval(&self) -> Result<Duration, String> {
        let normal = self.schedule_gap_secs()?;
        let derived = (normal / 10).clamp(10, 60).max(self.timeout_secs);
        Ok(Duration::from_secs(
            self.retry_interval_secs.unwrap_or(derived),
        ))
    }

    pub fn down_interval(&self) -> Result<Duration, String> {
        let normal = self.schedule_gap_secs()?;
        let derived = (normal / 4).clamp(30, 300).max(self.timeout_secs);
        Ok(Duration::from_secs(
            self.down_interval_secs.unwrap_or(derived),
        ))
    }

    /// Parse an admin-submitted document. The wire type is `CheckConfig`
    /// itself — a parallel DTO duplicated twenty-odd fields and two
    /// conversions for no gain, and a field added to one and forgotten in the
    /// other is exactly the drift that would silently drop a setting on save.
    ///
    /// Shared by the coordinator's own admin API (`routes::checks`) and the
    /// global tier's (`global::store`): both accept the same JSON shape from
    /// the same admin console, so both parse it the same way. It carries the
    /// target, so the result must only ever be produced behind authentication.
    pub fn from_submitted(body: serde_json::Value, new_default: bool) -> Result<Self, String> {
        let has_failure_threshold = body.get("failures_before_down").is_some();
        let mut check: CheckConfig = serde_json::from_value(body).map_err(|e| e.to_string())?;
        if new_default && !has_failure_threshold {
            check.failures_before_down = NEW_CHECK_FAILURES_BEFORE_DOWN;
        }
        Ok(check)
    }

    /// Trim the strings a form submits and drop the blanks, so an untouched
    /// field means "unset" rather than a group literally named "". Shared for
    /// the same reason `from_submitted` is.
    pub fn normalised(mut self) -> Self {
        let blank_to_none = |s: Option<String>| s.filter(|v: &String| !v.trim().is_empty());
        self.name = self.name.trim().to_string();
        self.target = self.target.trim().to_string();
        self.cron = self.cron.trim().to_string();
        self.group = blank_to_none(self.group);
        self.eval = blank_to_none(self.eval);
        self.description = blank_to_none(self.description);
        self.image = blank_to_none(self.image);
        self.tag = blank_to_none(self.tag);
        self.expect.body_contains = blank_to_none(self.expect.body_contains);
        self.dns.nameserver = blank_to_none(self.dns.nameserver);
        self.hosts = self
            .hosts
            .into_iter()
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
            .collect();
        self.agents = self
            .agents
            .into_iter()
            .map(|a| a.trim().to_string())
            .filter(|a| !a.is_empty())
            .collect();
        self.dns.expected = self
            .dns
            .expected
            .into_iter()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect();
        self.headers.retain(|h| !h.name.trim().is_empty());
        self
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
        if name.contains('/') || name.chars().any(char::is_control) {
            return Err("name may not contain a slash or control character".into());
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
            if matches!(
                header.name.to_ascii_lowercase().as_str(),
                "authorization"
                    | "proxy-authorization"
                    | "x-api-key"
                    | "x-auth-token"
                    | "x-access-token"
            ) && !has_secret_reference(&header.value)
            {
                return Err(
                    "authentication header values must use a ${NAME} secret reference".into(),
                );
            }
        }
        // Every target has to satisfy the kind's shape, not just the first —
        // otherwise an added host silently fails forever.
        for target in self.all_targets() {
            if target_has_url_userinfo(target) {
                return Err(
                    "target URL userinfo must use a ${NAME} secret reference in a header".into(),
                );
            }
            if has_sensitive_query_value(target) {
                return Err(
                    "sensitive target query values must use a ${NAME} secret reference".into(),
                );
            }
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
            && self.dns.mode == DnsMode::Standard
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
        if self.kind != CheckKind::Heartbeat {
            for (name, interval) in [
                ("retry interval", self.retry_interval_secs),
                ("down interval", self.down_interval_secs),
            ] {
                if let Some(seconds) = interval {
                    if seconds < self.timeout_secs {
                        return Err(format!("{name} must not be shorter than the timeout"));
                    }
                }
            }
        }
        // A policy whose `down_when` is easier to satisfy than its
        // `degraded_when` would let a check jump straight from Up to Down
        // without passing through Degraded, skipping an edge the alerting code
        // assumes exists. Rejected here rather than clamped, because either
        // repair would be a guess at which of the two the operator meant.
        self.rollup.validate()?;
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

fn default_coordinator_url() -> Vec<String> {
    vec!["http://127.0.0.1:8080".into()]
}

/// Accept either a bare string or a list of them for the same key.
///
/// A single coordinator is the common case and should not have to be written as
/// a one-element array; a region with three is the deployed case and must not
/// need a second key that can disagree with the first.
fn one_or_many<'de, D>(de: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    let urls = match OneOrMany::deserialize(de)? {
        OneOrMany::One(u) => vec![u],
        OneOrMany::Many(u) => u,
    };
    let urls: Vec<String> = urls
        .into_iter()
        .map(|u| u.trim().to_string())
        .filter(|u| !u.is_empty())
        .collect();
    if urls.is_empty() {
        // Falling back to the default here would silently point an agent whose
        // config says `coordinator_url = []` at localhost.
        return Err(serde::de::Error::custom(
            "coordinator_url must name at least one coordinator",
        ));
    }
    Ok(urls)
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
fn default_max_in_flight_probes() -> usize {
    128
}
fn default_listen() -> String {
    "0.0.0.0:8080".into()
}
fn default_db_path() -> String {
    "lighthouse.duckdb".into()
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
            max_in_flight_probes: default_max_in_flight_probes(),
            token: None,
            cursor: Arc::default(),
        }
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            db_path: default_db_path(),
            database: DatabaseConfig::default(),
            notifiers: Vec::new(),
            admin_password: None,
            secure_cookies: false,
            trusted_proxies: Vec::new(),
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
            rollup_defaults: RollupDefaults::default(),
            region: default_region(),
            global: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_options_default_to_standard_and_nameservers_accept_ip_or_port() {
        let dns: DnsOptions =
            serde_json::from_value(serde_json::json!({})).expect("old DNS documents still parse");
        assert_eq!(dns.mode, DnsMode::Standard);
        assert_eq!(dns_nameserver("192.0.2.1").unwrap().port(), 53);
        assert_eq!(dns_nameserver("192.0.2.1:5353").unwrap().port(), 5353);
        assert_eq!(dns_nameserver("[2001:db8::1]:5353").unwrap().port(), 5353);
        assert!(dns_nameserver("resolver.example").is_err());
    }

    #[test]
    fn dnssec_checks_default_to_the_system_resolver() {
        let mut check = CheckConfig {
            name: "resolver DNSSEC".into(),
            kind: CheckKind::Dns,
            target: ".".into(),
            dns: DnsOptions {
                mode: DnsMode::Dnssec,
                ..Default::default()
            },
            ..Default::default()
        };
        check
            .validate()
            .expect("no nameserver means the system resolver");
        check.dns.nameserver = Some("192.0.2.1:53".into());
        check.validate().expect("explicit resolver is valid");
    }

    #[test]
    fn regions_are_provisioned_not_declared() {
        let cfg: Config = toml::from_str(
            r#"
            [global]
            [[global.provision]]
            region = "tw"
            token = "wac_tw.secret"
            "#,
        )
        .expect("[[global.provision]] parses");
        cfg.global.validate().expect("a shaped key is valid");
        assert_eq!(cfg.global.provision[0].region, "tw");

        for stale in [
            "[global]\ntoken = \"t\"",
            "[[global.region]]\nname = \"tw\"",
        ] {
            assert!(
                toml::from_str::<Config>(stale).is_err(),
                "a pre-registration key must fail loudly: {stale}"
            );
        }
        let bad: Config =
            toml::from_str("[[global.provision]]\nregion = \"tw\"\ntoken = \"shared\"").unwrap();
        assert!(bad.global.validate().is_err(), "an unshaped key is refused");
    }

    #[test]
    fn agent_max_in_flight_probes_defaults_to_128() {
        let cfg: Config = toml::from_str("[agent]").expect("agent config parses");
        assert_eq!(cfg.agent.max_in_flight_probes, 128);
        cfg.agent.validate().expect("default is valid");
    }

    #[test]
    fn agent_max_in_flight_probes_accepts_any_positive_value() {
        let cfg: Config = toml::from_str("[agent]\nmax_in_flight_probes = 1000000")
            .expect("positive budget parses");
        cfg.agent.validate().expect("positive budget is valid");
    }

    #[test]
    fn agent_max_in_flight_probes_rejects_zero() {
        let cfg: Config = toml::from_str("[agent]\nmax_in_flight_probes = 0")
            .expect("zero parses so startup can explain the error");
        let err = cfg.agent.validate().expect_err("zero budget is invalid");
        assert!(err
            .to_string()
            .contains("max_in_flight_probes must be positive"));
    }

    #[test]
    fn coordinator_url_accepts_one_url_or_a_list() {
        let one: Config = toml::from_str(
            r#"
            [agent]
            coordinator_url = "http://coordinator-a:8080/"
            "#,
        )
        .expect("a bare string is still valid");
        assert_eq!(one.agent.coordinator_url.len(), 1);
        // The trailing slash is trimmed at use, not at parse: what the operator
        // wrote is what `--print-config` should show back.
        assert_eq!(one.agent.coordinator(), "http://coordinator-a:8080");

        let many: Config = toml::from_str(
            r#"
            [agent]
            coordinator_url = ["http://coordinator-a:8080", "http://coordinator-b:8080"]
            "#,
        )
        .expect("a list is valid");
        assert_eq!(many.agent.coordinator_url.len(), 2);
        assert_eq!(many.agent.coordinator(), "http://coordinator-a:8080");
    }

    #[test]
    fn an_empty_coordinator_list_is_rejected_rather_than_defaulted() {
        // Defaulting would point the agent at localhost, where in a container
        // there is nothing — a silent misconfiguration that looks like every
        // check being down.
        let err = toml::from_str::<Config>(
            r#"
            [agent]
            coordinator_url = []
            "#,
        )
        .expect_err("an empty list is not a coordinator");
        assert!(
            err.to_string().contains("at least one coordinator"),
            "the error should say what is missing: {err}"
        );
    }

    #[test]
    fn rotation_walks_the_list_and_wraps_without_failing_back() {
        let cfg = AgentConfig {
            coordinator_url: vec!["http://a:8080".into(), "http://b:8080".into()],
            ..Default::default()
        };
        assert_eq!(cfg.coordinator(), "http://a:8080");
        cfg.rotate_coordinator();
        assert_eq!(cfg.coordinator(), "http://b:8080");
        // Wrapping, not stopping: the first coordinator may well be back.
        cfg.rotate_coordinator();
        assert_eq!(cfg.coordinator(), "http://a:8080");
    }

    #[test]
    fn a_clone_shares_the_cursor_so_both_agent_loops_fail_over_together() {
        // `sync_loop` and `poster_loop` each take an `AgentConfig` by value.
        // If the cursor were copied rather than shared, the agent would poll
        // one coordinator for work while posting results to another.
        let cfg = AgentConfig {
            coordinator_url: vec!["http://a:8080".into(), "http://b:8080".into()],
            ..Default::default()
        };
        let poster = cfg.clone();
        cfg.rotate_coordinator();
        assert_eq!(poster.coordinator(), "http://b:8080");
    }

    #[test]
    fn a_single_coordinator_never_rotates_away_from_itself() {
        let cfg = AgentConfig::default();
        let before = cfg.coordinator().to_string();
        cfg.rotate_coordinator();
        assert_eq!(cfg.coordinator(), before);
    }

    #[test]
    fn file_seeded_checks_are_rejected() {
        // Checks are created only through the authenticated control plane now
        // (see SCALING.md: "File-seeded checks are removed"). `Config` has no
        // `checks` field any more, and `deny_unknown_fields` turns a leftover
        // `[[checks]]` table into a load error instead of a silent no-op.
        let err = toml::from_str::<Config>(
            r#"
            [[checks]]
            name = "web"
            type = "http"
            target = "https://example.com"
            "#,
        )
        .expect_err("a config-file check table must not parse");
        assert!(
            err.to_string().contains("checks"),
            "the error should name the rejected field: {err}"
        );
    }

    #[test]
    fn check_secret_references_resolve_without_mutating_document() {
        let check = CheckConfig {
            name: "private API".into(),
            target: "https://${API_HOST}/health?token=${API_TOKEN}".into(),
            headers: vec![Header {
                name: "Authorization".into(),
                value: "Bearer ${API_TOKEN}".into(),
            }],
            ..Default::default()
        };
        let resolved = check
            .resolve_secret_references(|name| match name {
                "API_HOST" => Some("api.internal".into()),
                "API_TOKEN" => Some("credential".into()),
                _ => None,
            })
            .expect("all references resolve");
        assert_eq!(
            resolved.target,
            "https://api.internal/health?token=credential"
        );
        assert_eq!(resolved.headers[0].value, "Bearer credential");
        assert!(
            check.target.contains("${API_TOKEN}"),
            "stored document stays a reference"
        );
    }

    #[test]
    fn missing_check_secret_names_the_environment_variable_not_a_value() {
        let err = resolve_secret_references("Bearer ${MISSING_TOKEN}", |_| None)
            .expect_err("missing reference must fail hand-off");
        assert_eq!(
            err,
            "missing environment variable MISSING_TOKEN for check secret reference"
        );
        assert!(!err.contains("Bearer"));
    }

    #[test]
    fn check_validation_rejects_inline_sensitive_values() {
        let mut check = CheckConfig {
            name: "private API".into(),
            target: "https://user:password@example.com/health".into(),
            ..Default::default()
        };
        assert!(check.validate().unwrap_err().contains("userinfo"));

        check.target = "https://example.com/health?api_key=literal".into();
        assert!(check.validate().unwrap_err().contains("query values"));

        check.target = "https://example.com/health".into();
        check.headers = vec![Header {
            name: "Authorization".into(),
            value: "Bearer literal".into(),
        }];
        assert!(check.validate().unwrap_err().contains("secret reference"));

        check.headers[0].value = "Bearer ${API_TOKEN}".into();
        check.validate().expect("reference is safe to store");
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
    fn adaptive_intervals_derive_from_the_normal_schedule() {
        let mut check = CheckConfig {
            name: "hourly".into(),
            target: "https://example.com".into(),
            cron: "0 0 * * * *".into(),
            ..Default::default()
        };
        assert_eq!(check.retry_interval().unwrap(), Duration::from_secs(60));
        assert_eq!(check.down_interval().unwrap(), Duration::from_secs(300));

        check.cron = "*/30 * * * * *".into();
        assert_eq!(check.retry_interval().unwrap(), Duration::from_secs(10));
        assert_eq!(check.down_interval().unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn explicit_adaptive_intervals_must_fit_the_timeout() {
        let mut check = CheckConfig {
            name: "web".into(),
            target: "https://example.com".into(),
            timeout_secs: 10,
            retry_interval_secs: Some(9),
            ..Default::default()
        };
        assert!(check.validate().unwrap_err().contains("retry interval"));

        check.retry_interval_secs = Some(10);
        check.down_interval_secs = Some(9);
        assert!(check.validate().unwrap_err().contains("down interval"));
    }

    #[test]
    fn old_documents_keep_immediate_failure_semantics() {
        let old: CheckConfig = serde_json::from_value(serde_json::json!({
            "name": "old",
            "type": "http",
            "target": "https://example.com"
        }))
        .unwrap();
        assert_eq!(old.failures_before_down, 1);
        assert_eq!(old.retry_interval_secs, None);
        assert_eq!(old.down_interval_secs, None);
        assert_eq!(CheckConfig::default().failures_before_down, 1);
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
}
