use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Immutable check identity. Generated once, UUIDv7 so it sorts by creation
/// order and stores natively as DuckDB's `UUID` column type. Every table that
/// references a check — results, current state, agent beliefs, state
/// history, incident attachments, notification queues, dependencies,
/// maintenance associations — keys on this, never on the mutable display
/// `name`. See `uptime-monitor-design.md` and `SCALING.md` for the identity
/// migration rationale.
pub type CheckId = uuid::Uuid;

/// Mint a fresh check identity. The application always generates this; a
/// caller-selected ID from an untrusted client must never reach here (public
/// admin creation ignores any `id` in the request body). Trusted internal
/// paths — regional creation folded into a global check, replication,
/// fanout — pass an existing ID through instead of calling this again.
pub fn new_check_id() -> CheckId {
    uuid::Uuid::now_v7()
}

/// What kind of probe a check performs. Deserialized from the config `type` key;
/// an unrecognized value fails config load rather than silently probing HTTP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckKind {
    #[default]
    Http,
    Tcp,
    Dns,
    Icmp,
    /// TLS certificate validity and expiry for a host.
    Ssl,
    /// Inverted check: nothing is probed. Something external pings a generated
    /// URL, and the check goes DOWN when that stops happening.
    Heartbeat,
    /// Scrape a Prometheus/OpenMetrics exporter, select one sample by metric
    /// name and exact labels, and evaluate it (optionally via Lua). The raw
    /// exporter body is never persisted; only the selected value survives.
    #[serde(alias = "prometheus")]
    Openmetrics,
    // ponytail: no Database kind yet. Postgres/MySQL/SQLite would be a `sqlx`
    // feature away, but Oracle and SQL Server need proprietary client libraries
    // that cannot be vendored into the distroless image — so adding "database"
    // as one kind would either ship a broken subset or abandon the 80MB
    // single-binary deployment. Revisit as sqlx-backed pg/mysql/sqlite first,
    // with the other two behind an opt-in image variant.
}

impl CheckKind {
    /// Every kind this binary knows. An agent declares its subset of these to
    /// the coordinator, so exhaustiveness matters: a kind missing here is a
    /// kind no agent ever claims to run.
    pub const ALL: [CheckKind; 7] = [
        CheckKind::Http,
        CheckKind::Tcp,
        CheckKind::Dns,
        CheckKind::Icmp,
        CheckKind::Ssl,
        CheckKind::Heartbeat,
        CheckKind::Openmetrics,
    ];

    pub fn as_db(&self) -> &'static str {
        match self {
            CheckKind::Http => "http",
            CheckKind::Tcp => "tcp",
            CheckKind::Dns => "dns",
            CheckKind::Icmp => "icmp",
            CheckKind::Ssl => "ssl",
            CheckKind::Heartbeat => "heartbeat",
            CheckKind::Openmetrics => "openmetrics",
        }
    }

    /// Strict inverse of `as_db`. `None` for anything else, which is what a
    /// newer peer naming a kind this binary predates looks like — the caller
    /// decides whether that is a fallback or a refusal, because guessing is
    /// only safe in one of those two places.
    pub fn parse(s: &str) -> Option<CheckKind> {
        Some(match s {
            "http" => CheckKind::Http,
            "tcp" => CheckKind::Tcp,
            "dns" => CheckKind::Dns,
            "icmp" => CheckKind::Icmp,
            "ssl" => CheckKind::Ssl,
            "heartbeat" => CheckKind::Heartbeat,
            "openmetrics" | "prometheus" => CheckKind::Openmetrics,
            _ => return None,
        })
    }

    /// Unknown values fall back to HTTP only because the column is written
    /// exclusively by `as_db` — a stored value outside this set means the row
    /// was hand-edited.
    pub fn from_db(s: &str) -> CheckKind {
        CheckKind::parse(s).unwrap_or(CheckKind::Http)
    }

    /// Whether an agent probes this kind at all. A heartbeat is driven by an
    /// inbound ping, so handing it to agents would have them probe nothing.
    pub fn is_agent_probed(&self) -> bool {
        !matches!(self, CheckKind::Heartbeat)
    }
}

/// Reserved `agent_id` for heartbeat pings. They arrive over HTTP from whatever
/// job is checking in, so there is no agent behind them and none should be
/// listed.
pub const HEARTBEAT_AGENT: &str = "heartbeat";

/// What a check id resolves to for display when no name is known — a check
/// deleted after the alert or incident that names it, or, transiently, one
/// whose config reached a node before its name did. Shown in incident and
/// alert titles in place of the raw UUID, which a user should never see where
/// a name belongs. Centralised so both tiers and every title path agree.
pub const DELETED_CHECK_LABEL: &str = "Deleted check";

/// Raw check result sent from agent to coordinator.
/// The agent is a dumb probe: it reports what it observed, not a verdict.
/// The coordinator runs eval logic to derive UP/DOWN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub check_id: CheckId,
    pub agent_id: String,
    /// Where the probe ran. Stamped by the agent because only it knows, and
    /// carried on every result because "the API was slow" and "the API was slow
    /// from Frankfurt" are different findings. Absent on results from agents
    /// predating this field, which read as "unknown" rather than being guessed.
    #[serde(default)]
    pub zone: String,
    #[serde(default)]
    pub region: String,
    /// Which probe produced this, so the coordinator can evaluate it correctly:
    /// only HTTP carries status-code semantics.
    #[serde(default, rename = "type")]
    pub kind: CheckKind,
    pub timestamp: DateTime<Utc>,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    /// Whether the configured body/answer substring was found. `None` when the
    /// check has no content expectation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_matched: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Why it failed, classified by the probe that saw it. Present whenever
    /// `error` is; the pair is detail plus category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureKind>,
    /// Response body, sent ONLY when the check has a Lua script that might read
    /// it, and truncated. Used transiently for evaluation and never stored:
    /// keeping a body per result per agent per interval would dwarf everything
    /// else in the database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Response headers, same conditions as `body`. Keys are lowercased so a
    /// script can index them predictably.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<std::collections::HashMap<String, String>>,
    /// The value of the configured selected sample from an OpenMetrics scrape.
    /// `None` when no sample matched the selection (a failed observation) or
    /// for non-metric checks. Transient: passed into Lua at ingest and never
    /// written to `check_results`, history, or gossip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<f64>,
    /// Every sample from the same scrape, so a script can compare samples that
    /// share the metric's freshness and source. Transient like `metric` and
    /// `body`; omitted when absent and never persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Vec<crate::openmetrics::MetricSample>>,
}

/// Evaluated status, derived by the coordinator from a CheckResult.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Up,
    Down,
    Unknown,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Up => "up",
            Status::Down => "down",
            Status::Unknown => "unknown",
        }
    }

    /// Parse a status back from its stored string form.
    pub fn from_db(s: &str) -> Status {
        match s {
            "up" => Status::Up,
            "down" => Status::Down,
            _ => Status::Unknown,
        }
    }
}

/// The rolled-up state of a *node* in the region tree — an agent, a zone, a
/// region, or the check itself.
///
/// Deliberately a separate type from [`Status`], which stays three-state.
/// `Status` is an *observation*: what one probe saw. `NodeState` is a
/// *conclusion*: what a group of observations means. `Degraded` is only ever
/// meaningful for the latter — a single probe cannot be partially up — and
/// keeping the types apart makes it impossible for `eval()` or a Lua script to
/// invent one. See `uptime-monitor-design.md:520-526`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    Up,
    /// Some part of the tree is failing, but not enough of it to call the whole
    /// node down. A regional outage of a globally-served check lands here.
    Degraded,
    Down,
    Unknown,
}

impl NodeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeState::Up => "up",
            NodeState::Degraded => "degraded",
            NodeState::Down => "down",
            NodeState::Unknown => "unknown",
        }
    }

    /// Parse back from the stored string form.
    ///
    /// Unrecognised values read as `Unknown` rather than failing, exactly as
    /// [`Status::from_db`] does. That is what lets `degraded` reach an older
    /// peer over gossip without breaking it: it reads the state as Unknown,
    /// which drops out of every rollup denominator, so a mixed-version cluster
    /// is briefly less informed but never wrong.
    pub fn from_db(s: &str) -> NodeState {
        match s {
            "up" => NodeState::Up,
            "degraded" => NodeState::Degraded,
            "down" => NodeState::Down,
            _ => NodeState::Unknown,
        }
    }

    /// Anything that is not affirmatively Up. The incident-open gate.
    pub fn is_failing(&self) -> bool {
        matches!(self, NodeState::Degraded | NodeState::Down)
    }
}

/// The leaf boundary: one agent's observation becomes that agent's node state.
/// There is no inverse, because collapsing Degraded back onto a probe result
/// would be a lie about what was measured.
impl From<Status> for NodeState {
    fn from(s: Status) -> NodeState {
        match s {
            Status::Up => NodeState::Up,
            Status::Down => NodeState::Down,
            Status::Unknown => NodeState::Unknown,
        }
    }
}

/// Derive UP/DOWN from an observation plus the check's optional status
/// expectation. A probe error, or a failed content match, is always DOWN.
///
/// HTTP: `expect_status` must match exactly when set, otherwise any 2xx-3xx is UP.
/// TCP/DNS/ICMP carry no status code — connecting/resolving/replying at all is UP,
/// since any failure is reported as an `error`.
pub fn eval(result: &CheckResult, expect_status: Option<u16>) -> Status {
    if result.error.is_some() {
        return Status::Down;
    }
    if result.body_matched == Some(false) {
        return Status::Down;
    }
    match result.kind {
        CheckKind::Http => match (expect_status, result.status_code) {
            (Some(want), Some(got)) => {
                if got == want {
                    Status::Up
                } else {
                    Status::Down
                }
            }
            (None, Some(code)) if (200..400).contains(&code) => Status::Up,
            (None, Some(_)) => Status::Down,
            // Responded without a code: nothing to judge.
            (_, None) => Status::Unknown,
        },
        // These report failure through `error`, handled above, so reaching here
        // means the probe did what it was asked. A heartbeat never comes from an
        // agent at all — the coordinator synthesises its result.
        CheckKind::Tcp
        | CheckKind::Dns
        | CheckKind::Icmp
        | CheckKind::Ssl
        | CheckKind::Heartbeat => Status::Up,
        // A metric check with no custom Lua is UP when a sample was selected.
        // Transport, parse, and selection failures all surface as `error` and
        // are caught above as DOWN, so reaching here means a sample was found.
        // Guard on `metric` all the same: a result carrying neither an error
        // nor a selected value has nothing to judge and reads as Unknown rather
        // than a silent UP.
        CheckKind::Openmetrics => match result.metric {
            Some(_) => Status::Up,
            None => Status::Unknown,
        },
    }
}

/// Batch envelope for the ingest endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct ResultBatch {
    pub results: Vec<CheckResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn cr(status_code: Option<u16>, error: Option<String>) -> CheckResult {
        CheckResult {
            zone: "test-zone".into(),
            region: "test-region".into(),
            body: None,
            headers: None,
            check_id: CheckId::from_u128(1),
            agent_id: "agent".into(),
            kind: CheckKind::Http,
            timestamp: Utc::now(),
            latency_ms: 0,
            status_code,
            body_matched: None,
            failure: error.is_some().then_some(FailureKind::Other),
            error,
            metric: None,
            metrics: None,
        }
    }

    #[test]
    fn node_state_round_trips_through_the_db_form() {
        for s in [
            NodeState::Up,
            NodeState::Degraded,
            NodeState::Down,
            NodeState::Unknown,
        ] {
            assert_eq!(NodeState::from_db(s.as_str()), s, "{s:?}");
        }
        // An older peer's vocabulary, and a future one's: both read as Unknown
        // rather than panicking or defaulting to Up.
        assert_eq!(NodeState::from_db("partial"), NodeState::Unknown);
        assert_eq!(NodeState::from_db(""), NodeState::Unknown);
    }

    #[test]
    fn status_lifts_into_node_state_without_inventing_degraded() {
        assert_eq!(NodeState::from(Status::Up), NodeState::Up);
        assert_eq!(NodeState::from(Status::Down), NodeState::Down);
        assert_eq!(NodeState::from(Status::Unknown), NodeState::Unknown);
    }

    #[test]
    fn eval_maps_result_to_status() {
        let cases: [(&str, Option<u16>, Option<String>, Status); 8] = [
            (
                "error wins over 2xx",
                Some(200),
                Some("boom".into()),
                Status::Down,
            ),
            (
                "error with no code is down",
                None,
                Some("boom".into()),
                Status::Down,
            ),
            ("200 is up (lower edge)", Some(200), None, Status::Up),
            ("399 is up (upper edge)", Some(399), None, Status::Up),
            (
                "400 is down (just past range)",
                Some(400),
                None,
                Status::Down,
            ),
            (
                "199 is down (just below range)",
                Some(199),
                None,
                Status::Down,
            ),
            ("500 is down", Some(500), None, Status::Down),
            ("no code, no error is unknown", None, None, Status::Unknown),
        ];
        for (name, code, err, want) in cases {
            assert_eq!(eval(&cr(code, err), None), want, "case: {name}");
        }
    }

    #[test]
    fn expect_status_requires_an_exact_match() {
        // A 401 that the check explicitly expects is UP, even though the default
        // 2xx-3xx rule would call it DOWN.
        assert_eq!(eval(&cr(Some(401), None), Some(401)), Status::Up);
        // ...and a 200 is DOWN when 401 was expected.
        assert_eq!(eval(&cr(Some(200), None), Some(401)), Status::Down);
    }

    #[test]
    fn failed_body_match_is_down_even_on_2xx() {
        let mut r = cr(Some(200), None);
        r.body_matched = Some(false);
        assert_eq!(
            eval(&r, None),
            Status::Down,
            "200 with wrong body must be DOWN"
        );
        r.body_matched = Some(true);
        assert_eq!(eval(&r, None), Status::Up);
    }

    #[test]
    fn non_http_probes_are_up_when_they_report_no_error() {
        // TCP/DNS/ICMP carry no status code; reaching the target at all is UP,
        // and any failure arrives as an `error` instead.
        for kind in [CheckKind::Tcp, CheckKind::Dns, CheckKind::Icmp] {
            let mut r = cr(None, None);
            r.kind = kind;
            assert_eq!(eval(&r, None), Status::Up, "{kind:?} success");

            let mut failed = cr(None, Some("refused".into()));
            failed.kind = kind;
            assert_eq!(eval(&failed, None), Status::Down, "{kind:?} failure");
        }
    }
}

/// Why a probe failed, as a value rather than prose.
///
/// The probe knows the cause at the moment it fails, so it says so. Recovering
/// it later by matching on our own error text would be guesswork that silently
/// mis-classifies the day someone rewords a message.
///
/// `summary` is what the *public* status page may show. Raw failure text quotes
/// whatever the probe saw — resolved addresses, expected DNS values, request
/// URLs, pool member hostnames — and a public page must not republish the
/// infrastructure it is watching. One mistyped DNS expectation was enough to
/// broadcast a home IP address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Timeout,
    ConnectionFailed,
    NameResolution,
    UnexpectedDnsAnswer,
    UnexpectedContent,
    UnexpectedStatus,
    /// Days elapsed since expiry. A certificate's validity window is published
    /// in the certificate itself, so the count reveals nothing the visitor
    /// could not read from the handshake — unlike the names it is issued for.
    CertificateExpired {
        days: i64,
    },
    /// Days remaining. Same reasoning, and the number is the whole point.
    CertificateExpiring {
        days: i64,
    },
    TlsError,
    ScriptFailed,
    Other,
}

impl FailureKind {
    /// Safe to publish. Says what went wrong, never what was observed.
    pub fn summary(&self) -> String {
        match self {
            FailureKind::Timeout => "Timed out".into(),
            FailureKind::ConnectionFailed => "Connection failed".into(),
            FailureKind::NameResolution => "Name resolution failed".into(),
            FailureKind::UnexpectedDnsAnswer => "Unexpected DNS answer".into(),
            FailureKind::UnexpectedContent => "Unexpected response content".into(),
            FailureKind::UnexpectedStatus => "Unexpected status code".into(),
            FailureKind::CertificateExpired { days } => match days {
                0 => "Certificate expired today".into(),
                1 => "Certificate expired 1 day ago".into(),
                d => format!("Certificate expired {d} days ago"),
            },
            FailureKind::CertificateExpiring { days } => match days {
                0 => "Certificate expires today".into(),
                1 => "Certificate expires in 1 day".into(),
                d => format!("Certificate expires in {d} days"),
            },
            FailureKind::TlsError => "TLS error".into(),
            FailureKind::ScriptFailed => "Check script failed".into(),
            FailureKind::Other => "Check failed".into(),
        }
    }
}

/// What to publish for a failure recorded before probes classified themselves.
///
/// Existing rows carry text only. Rather than guess at their meaning by matching
/// on our own wording, they all read as a plain failure: a summary that says too
/// little is recoverable, one that leaks is not.
pub const LEGACY_FAILURE_SUMMARY: &str = "Check failed";
