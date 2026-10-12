//! Everything an agent talks to, plus the admin views and controls over
//! agents and their credentials.
//!
//! **An agent is a dumb probe.** It has no opinion about what it should run
//! or where it is — it only asks `/api/v1/agent/checks` for work and posts
//! what it saw to `/api/v1/results`. Every handler in this file is one half
//! of that exchange: hand out work, or accept an observation. Nothing here
//! computes a verdict or decides policy; that lives in `Coordinator::ingest`
//! and `crate::placement`, which this module calls into rather than
//! reimplements.
//!
//! **The credential decides location, never the caller.** `authenticate_agent`
//! reads zone and region off the credential that was minted for that agent,
//! and both `ingest_handler` and `agent_checks_handler` overwrite whatever the
//! request claimed with that value. A self-declared location would let an
//! agent be handed checks for a zone it is not actually in — or claim credit
//! for an outage it never observed — so the one fact placement and the
//! outage record both depend on is never taken on the agent's word.
//!
//! **Credential administration lives here too**, not in a separate file,
//! because minting and revoking are the admin-side counterpart to the same
//! secret `authenticate_agent` checks: the two halves of one mechanism.

use super::super::fleet::{unreachable_after_ms, AGENT_RETIRED_AFTER_MS, AGENT_STALE_AFTER_MS};
use super::super::router::{error_response, internal_error};
use super::super::{storage, Coordinator, SeenAgent};
use super::checks::PurgedResponse;
use super::session::bearer;
use crate::config::CheckConfig;
use crate::credential::AgentCredential;
use crate::http::{AdminJson, AdminSession};
use crate::models::{ResultBatch, Status};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

#[derive(Serialize)]
struct AgentsResponse {
    agents: Vec<AgentDto>,
    /// Checks pinned only to agents that are not reporting. They are configured
    /// and enabled but nothing is running them, which is otherwise silent.
    checks_with_no_live_agent: Vec<String>,
    /// Checks whose every eligible agent is present and reporting but has said
    /// it cannot probe that kind — a fleet older than the coordinator that
    /// configured them. Distinct from the list above because the remedy is
    /// different: those agents are not missing, they are out of date.
    checks_no_agent_can_run: Vec<String>,
    /// How many checks each list found, which is not how many it names.
    ///
    /// The agent roster is bounded by the fleet, but these two are bounded by
    /// the catalogue: a region that loses its agents orphans every check it
    /// holds, and naming ten thousand of them in a diagnostic banner helps
    /// nobody and costs the same as the status page it is warning about. The
    /// count is the signal; the names are a sample.
    checks_with_no_live_agent_total: usize,
    checks_no_agent_can_run_total: usize,
}

/// Names carried by the two fleet-health lists before they become a count.
/// Enough to recognise a pattern — one zone, one kind — without becoming a
/// second copy of the check list.
const FLEET_DIAGNOSTIC_SAMPLE: usize = 50;

fn sample(mut names: Vec<String>) -> Vec<String> {
    names.sort_unstable();
    names.truncate(FLEET_DIAGNOSTIC_SAMPLE);
    names
}

#[derive(Serialize)]
struct AgentDto {
    agent_id: String,
    /// Where it reports from. The fleet is grouped by these, so an operator can
    /// see "eu-west has no live agents" rather than reading twenty agent ids.
    zone: String,
    region: String,
    last_seen: i64,
    checks_reporting: u32,
    results_24h: u32,
    /// Silent long enough to be treated as decommissioned. Its history is kept.
    retired: bool,
    /// How many checks name this agent explicitly. Purging one that is still
    /// named would stop those checks running.
    assigned_checks: u32,
    /// Derived liveness: an agent that hasn't reported recently is stale.
    status: Status,
    /// Round-trip of its last poll, ms, as the agent measured it. `None` for an
    /// agent seen only through reported results, never a live poll.
    poll_rtt_ms: Option<i64>,
    /// The build it reported on its last poll, to whichever coordinator it
    /// polls: peers replicate poll rows. `None` for an agent that predates
    /// build reporting, or one seen only through results.
    version: Option<String>,
    /// Node id of the coordinator it last polled. `None` when that poll came
    /// from a peer predating the field, or it was seen only through results.
    coordinator: Option<String>,
    /// Check kinds it says it can probe. `None` means it never said, which is
    /// read as "anything" — not as "nothing".
    kinds: Option<Vec<String>>,
}

/// What one agent last said about itself, from either half of the fleet view:
/// a local poll, or a peer's replicated poll row.
struct Declared {
    at: i64,
    rtt_ms: Option<i64>,
    build: Option<String>,
    /// Wire names as sent, not parsed — a kind this node has never heard of
    /// must survive being displayed and compared.
    kinds: Option<Vec<String>>,
    coordinator: Option<String>,
}

/// What each agent said about itself on its last poll, wherever it polled.
///
/// Choose the newest declaration per agent, with local `seen` winning only
/// timestamp ties. Without replicated rows an operator would see builds and
/// capabilities only for agents that poll the node they opened.
///
/// Kinds stay as the wire names the agent sent, unparsed. A peer may
/// legitimately name a kind this node has never heard of, and narrowing to
/// the kinds this binary knows would make an agent look less capable here
/// than on the node it actually polls.
fn declarations(
    polls: Vec<storage::AgentPollRow>,
    seen: &std::collections::HashMap<String, SeenAgent>,
    me: &str,
) -> std::collections::HashMap<String, Declared> {
    let mut declared: std::collections::HashMap<String, Declared> = polls
        .into_iter()
        .map(|p| {
            (
                p.agent_id,
                Declared {
                    at: p.at,
                    rtt_ms: p.rtt_ms,
                    build: p.build,
                    kinds: p.kinds.map(|k| split_kinds(&k)),
                    coordinator: p.coordinator,
                },
            )
        })
        .collect();
    for (id, s) in seen {
        if declared
            .get(id)
            .is_none_or(|replicated| s.at >= replicated.at)
        {
            declared.insert(
                id.clone(),
                Declared {
                    at: s.at,
                    rtt_ms: s.rtt_ms,
                    build: s.build.clone(),
                    kinds: s
                        .kinds
                        .as_ref()
                        .map(|ks| ks.iter().map(|k| k.as_db().to_string()).collect()),
                    coordinator: Some(me.to_string()),
                },
            );
        }
    }
    declared
}

/// Split a declared kind list into its wire names.
///
/// An empty string yields an empty list, which is meaningful: the agent
/// answered the question with "nothing". Only an absent parameter means
/// "never said", and that is the caller's `Option` to hold.
fn split_kinds(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

pub(in crate::coordinator) async fn ingest_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let caller = match authenticate_agent(&coord, &headers) {
        Ok(c) => c,
        Err(denied) => return *denied,
    };
    // The body is only parsed once a credential has checked out. `Json<T>`
    // as an extractor argument would parse before this handler's body ever
    // runs, so a malformed or stale-shaped payload (like a check identified
    // by name rather than the UUID it must carry now) would reject with 422
    // even from a caller presenting no credential at all — a caller must
    // never learn anything about the payload's shape before it has proven
    // who it is.
    let mut batch: ResultBatch = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    // An agent does not get to say where it is. Zone and region decide placement
    // and are shown as the location an outage was seen from, so a caller able to
    // name its own could answer for a zone it is not in.
    for r in &mut batch.results {
        r.zone.clone_from(&caller.zone);
        r.region.clone_from(&caller.region);
    }
    let n = batch.results.len();
    if let Err(e) = coord.try_ingest(batch.results).await {
        warn!(error = %e, rejected = n, "ingest refused; agent will retry");
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "ingest unavailable, retry");
    }
    info!(accepted = n, "ingested results");
    StatusCode::ACCEPTED.into_response()
}

/// Agents that have reported results.
///
/// There is no registration: an agent exists because it sent something. That
/// keeps deployment to "run another one", but it means an agent can only ever
/// *stop* existing by falling silent — so retirement is derived from
/// `last_seen` rather than being a state anyone has to maintain.
///
/// Admin-only. The fleet is infrastructure detail — agent ids, zones, regions
/// and reporting cadence together map where monitoring runs from, which is not
/// something an anonymous visitor to the status page should be able to read.
pub(in crate::coordinator) async fn agents_handler(
    State(coord): State<Coordinator>,
    _admin: AdminSession,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let (rows, polls) = match coord
        .db(move |storage| Ok((storage.list_agents(now)?, storage.list_agent_polls()?)))
        .await
    {
        Ok(rows) => rows,
        Err(e) => return internal_error(e),
    };

    // A check pinned to agents that are all silent never runs, and nothing
    // else would say so: the check simply stops producing results, which looks
    // identical to a check nobody has got to yet.
    let live: std::collections::HashSet<String> = rows
        .iter()
        .filter(|r| now - r.last_seen <= AGENT_STALE_AFTER_MS)
        .map(|r| r.agent_id.clone())
        .collect();
    let orphaned: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| {
            m.enabled
                && m.kind.is_agent_probed()
                && !m.agents.is_empty()
                && !m.agents.iter().any(|a| live.contains(a))
        })
        .map(|(_, m)| m.name.clone())
        .collect();

    let declared = declarations(polls, &coord.inner.seen.read(), &coord.inner.node_id);

    let unrunnable: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| {
            if !m.enabled || !m.kind.is_agent_probed() {
                return false;
            }
            let eligible: Vec<&String> = live
                .iter()
                .filter(|id| m.agents.is_empty() || m.agents.contains(id))
                .collect();
            // `all` over an empty set is vacuously true, and "nobody is here"
            // is the other list's finding, not this one's.
            !eligible.is_empty()
                && eligible.iter().all(|id| {
                    declared
                        .get(*id)
                        .and_then(|d| d.kinds.as_ref())
                        // Silence is not a refusal: an agent that never
                        // declared must not be counted as declining, or a
                        // fleet that has said nothing would report every
                        // check unrunnable.
                        .is_some_and(|kinds| !kinds.iter().any(|k| k == m.kind.as_db()))
                })
        })
        .map(|(_, m)| m.name.clone())
        .collect();

    let agents = rows
        .into_iter()
        .map(|r| {
            let silence = now - r.last_seen;
            let said = declared.get(&r.agent_id);
            let poll_rtt_ms = said.and_then(|d| d.rtt_ms);
            AgentDto {
                zone: r.zone.clone(),
                region: r.region.clone(),
                status: if silence <= AGENT_STALE_AFTER_MS {
                    Status::Up
                } else {
                    Status::Down
                },
                retired: silence > AGENT_RETIRED_AFTER_MS,
                assigned_checks: coord
                    .inner
                    .checks
                    .read()
                    .values()
                    .filter(|m| m.agents.contains(&r.agent_id))
                    .count() as u32,
                agent_id: r.agent_id,
                last_seen: r.last_seen,
                checks_reporting: r.checks_reporting,
                results_24h: r.results_24h,
                poll_rtt_ms,
                version: said.and_then(|d| d.build.clone()),
                kinds: said.and_then(|d| d.kinds.clone()),
                coordinator: said.and_then(|d| d.coordinator.clone()),
            }
        })
        .collect();
    Json(AgentsResponse {
        agents,
        checks_with_no_live_agent_total: orphaned.len(),
        checks_no_agent_can_run_total: unrunnable.len(),
        checks_with_no_live_agent: sample(orphaned),
        checks_no_agent_can_run: sample(unrunnable),
    })
    .into_response()
}

/// Delete every result an agent ever reported, which also removes it from the
/// agent list — the list is derived from those rows.
///
/// This is the only way an agent can be made to disappear promptly, and it
/// destroys history, so it is deliberately separate from an agent simply
/// falling silent.
pub(in crate::coordinator) async fn purge_agent_handler(
    State(coord): State<Coordinator>,
    Path(agent_id): Path<String>,
    _admin: AdminSession,
) -> Response {
    let still_assigned: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| m.agents.contains(&agent_id))
        .map(|(_, m)| m.name.clone())
        .collect();
    // Purging an agent that checks are still pinned to would leave those checks
    // silently unrunnable. Name them and refuse.
    if !still_assigned.is_empty() {
        return error_response(
            StatusCode::CONFLICT,
            &format!(
                "still assigned to {}: unassign it there first, or those checks would stop running",
                still_assigned.join(", ")
            ),
        );
    }
    let purge_id = agent_id.clone();
    match coord
        .db(move |storage| storage.purge_agent(&purge_id))
        .await
    {
        Ok(rows) => {
            info!(agent = %agent_id, rows, "purged agent history");
            Json(PurgedResponse { purged: rows }).into_response()
        }
        Err(e) => internal_error(e),
    }
}

/// Where an authenticated agent reports from. Read off its credential, never
/// off the request, because placement acts on it.
struct AgentLocation {
    zone: String,
    region: String,
}

/// Gate for endpoints that agents call.
///
/// A minted credential is the only way in. There is no fleet-wide fallback and
/// no open mode: an ingest endpoint that accepts anonymous results lets anyone
/// post for checks they do not run, and a monitor that can be told what it saw
/// is worse than no monitor, because it is believed.
///
/// A coordinator with no credentials accepts nothing from agents until an
/// operator mints one. That is the intended first-run state, not a failure.
// Boxed error: this is the ingest hot path, and an unboxed `Response` makes
// every returned `Result` 128+ bytes to move on the success path too.
fn authenticate_agent(
    coord: &Coordinator,
    headers: &HeaderMap,
) -> Result<AgentLocation, Box<Response>> {
    bearer(headers)
        .and_then(crate::credential::split)
        .and_then(|(id, secret)| {
            let creds = coord.inner.credentials.read();
            creds
                .get(id)
                .filter(|c| crate::credential::verify(c, secret))
                .map(|c| AgentLocation {
                    zone: c.zone.clone(),
                    region: c.region.clone(),
                })
        })
        .ok_or_else(|| {
            Box::new(error_response(
                StatusCode::UNAUTHORIZED,
                "a valid agent credential is required",
            ))
        })
}

#[derive(Serialize)]
struct AgentChecksResponse {
    checks: Vec<CheckConfig>,
    /// Checks omitted because this coordinator cannot resolve their secret
    /// references. Kept alongside the successful assignments so one regional
    /// configuration mistake does not make an entire fleet retain stale work.
    config_errors: Vec<AgentCheckConfigError>,
    /// The caller's current failure run per assigned check, non-zero only, so
    /// a restarted agent resumes retry/down cadence rather than waiting a
    /// whole schedule to notice recovery.
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    failing: std::collections::HashMap<crate::models::CheckId, u32>,
}

/// A configuration error for an omitted check, naming the missing reference
/// but never carrying the secret value or target.
#[derive(Serialize, Debug, PartialEq, Eq)]
struct AgentCheckConfigError {
    name: String,
    error: String,
}

fn resolve_checks_for_agent<'a>(
    checks: impl IntoIterator<Item = &'a CheckConfig>,
    lookup: impl Fn(&str) -> Option<String> + Copy,
) -> (Vec<CheckConfig>, Vec<AgentCheckConfigError>) {
    let mut resolved = Vec::new();
    let mut config_errors = Vec::new();
    for check in checks {
        match check.resolve_secret_references(lookup) {
            Ok(check) => resolved.push(check),
            Err(error) => config_errors.push(AgentCheckConfigError {
                name: check.name.clone(),
                error,
            }),
        }
    }
    (resolved, config_errors)
}

/// The check list an agent should probe.
///
/// Disabled checks are filtered here rather than at the agent, so "disabled"
/// means the same thing everywhere. Everything else is decided by placement:
/// the coordinator works out which agents cover each check and hands each
/// caller only its share, so agents stay dumb and no assignment is stored
/// anywhere to drift from the fleet.
pub(in crate::coordinator) async fn agent_checks_handler(
    State(coord): State<Coordinator>,
    Query(q): Query<AgentIdentity>,
    headers: HeaderMap,
) -> Response {
    let authenticated = match authenticate_agent(&coord, &headers) {
        Ok(c) => c,
        Err(denied) => return *denied,
    };
    let Some(caller) = q.agent_id.as_deref() else {
        // An agent that will not say who it is cannot be placed. Older agents
        // predate identity, so they still get everything rather than silently
        // going idle on upgrade. The catalogue is refreshed atomically with
        // every accepted check config change.
        let checks = coord.inner.check_catalogue.read();
        let (checks, config_errors) = resolve_checks_for_agent(
            checks
                .iter()
                .filter(|c| c.enabled && c.kind.is_agent_probed()),
            |name| std::env::var(name).ok(),
        );
        return Json(AgentChecksResponse {
            checks,
            config_errors,
            failing: Default::default(),
        })
        .into_response();
    };

    let now = chrono::Utc::now().timestamp_millis();

    // Asking for work is contact, and it is the only signal a freshly booted
    // agent can give. Record it before deciding, so the caller is part of the
    // fleet it is about to be placed within.
    {
        // Placement is decided from this map, so a self-declared location would
        // be a way to be handed checks for a zone you are not in and then never
        // probe them. The credential decides; `?zone=`/`?region=` are ignored.
        let (zone, region) = (authenticated.zone.clone(), authenticated.region.clone());
        let mut seen = coord.inner.seen.write();
        seen.insert(
            caller.to_string(),
            SeenAgent {
                zone,
                region,
                at: now,
                // Cadence is not location: an agent lying about how often it
                // polls only widens or narrows its own liveness window, and a
                // wider one makes it look alive longer than it earns — capped
                // at an hour by `unreachable_after_ms`.
                window_ms: unreachable_after_ms(coord.inner.missed_polls, q.poll_secs),
                rtt_ms: q.rtt_ms.map(|v| v.min(i64::MAX as u64) as i64),
                kinds: declared_kinds(q.kinds.as_deref()),
                build: q.version.clone().filter(|v| !v.is_empty()),
            },
        );
        // Forget agents long out of contact. Without this the map is an
        // unbounded leak wherever agent ids are not stable — a Deployment's
        // pods get a fresh name on every restart, so a year of rollouts would
        // accumulate thousands of ghosts. They are already excluded from the
        // fleet by age, so retaining them buys nothing.
        seen.retain(|_, s| now - s.at <= s.window_ms);
    }

    // Check config is an in-memory snapshot refreshed with every accepted
    // config entry. Only the cluster-wide agent poll rows still require a
    // database job for placement.
    let checks = coord.inner.check_catalogue.read().clone();
    let polls = match coord.db(|storage| storage.list_agent_polls()).await {
        Ok(polls) => polls,
        Err(e) => return internal_error(e),
    };

    // Placement needs the cluster-wide fresh union, not just this node's
    // callers: an agent can fail over to a peer between polls without losing
    // its assignment here. For each agent, select the newest live observation;
    // local `seen` wins only when timestamps tie. The selected observation
    // supplies location and capabilities together.
    let fleet = placement_fleet(now, &coord.inner.seen.read(), &polls);

    let mine: Vec<crate::config::CheckConfig> = checks
        .into_iter()
        .filter(|c| c.enabled && c.kind.is_agent_probed())
        .filter(|c| {
            // Placement runs against the agents that can actually run *this*
            // kind, not the whole fleet. An agent too old for a kind would
            // otherwise be handed the check, drop it on parse, and leave it
            // covered on paper and unprobed in fact — the same silence this
            // negotiation exists to end. Excluding it here lets the next
            // eligible agent take the work instead.
            let capable = fleet_for(&fleet, c.kind);
            crate::placement::assign(c.id, &c.placement, &c.agents, &capable)
                .iter()
                .any(|id| id == caller)
        })
        .collect();
    let failing = coord
        .inner
        .tracker
        .lock()
        .failing(caller, mine.iter().map(|c| c.id));
    let (checks, config_errors) = resolve_checks_for_agent(&mine, |name| std::env::var(name).ok());
    Json(AgentChecksResponse {
        checks,
        config_errors,
        failing,
    })
    .into_response()
}

/// Who is asking for a check list.
///
/// Only the id: location is read off the caller's credential. It used to be
/// sent on the request, which meant the value placement trusts was chosen by
/// the party being placed.
#[derive(Deserialize)]
pub(in crate::coordinator) struct AgentIdentity {
    agent_id: Option<String>,
    /// How often this agent polls, so the coordinator can size the silence
    /// window to the cadence instead of assuming the default. Absent from
    /// agents that predate it; they get the default cadence.
    poll_secs: Option<u64>,
    /// Round-trip of this agent's *previous* poll, in milliseconds, measured by
    /// the agent. Self-asserted like `poll_secs`: it only feeds display, never
    /// placement or auth, so a lie changes nothing but its own row.
    rtt_ms: Option<u64>,
    /// Comma-separated check kinds this agent can probe. Absent from agents
    /// predating capability negotiation, which are taken to run anything.
    ///
    /// Understating it is the only lie available and it is self-punishing: the
    /// work goes to another agent, or to nobody and gets reported as uncovered.
    kinds: Option<String>,
    /// The agent's build, for the fleet view. Display only.
    version: Option<String>,
}

/// What an agent claims it can probe.
///
/// `None` — the parameter absent entirely — means "never said", which is an
/// agent older than this negotiation and stays eligible for everything.
/// Present-but-empty is different and is honoured as "nothing": an agent that
/// answers the question with an empty list has answered it.
///
/// Kinds this coordinator does not recognise are dropped rather than rejected,
/// because an agent *newer* than its coordinator is a legitimate ordering too —
/// it can simply never be assigned a kind this node has no checks of.
/// Build placement's live per-agent fleet from local and replicated polls.
///
/// A replicated observation supersedes an older local one; local wins an equal
/// timestamp so a coordinator keeps the exact record it just received.
fn placement_fleet(
    now: i64,
    seen: &std::collections::HashMap<String, SeenAgent>,
    polls: &[storage::AgentPollRow],
) -> Vec<crate::placement::Agent> {
    let mut freshest: std::collections::HashMap<String, (i64, crate::placement::Agent)> = polls
        .iter()
        .filter(|p| now - p.at <= p.window_ms)
        .map(|p| {
            (
                p.agent_id.clone(),
                (
                    p.at,
                    crate::placement::Agent {
                        agent_id: p.agent_id.clone(),
                        zone: p.zone.clone(),
                        region: p.region.clone(),
                        kinds: declared_kinds(p.kinds.as_deref()),
                    },
                ),
            )
        })
        .collect();
    for (agent_id, s) in seen {
        if now - s.at > s.window_ms {
            continue;
        }
        let local = (
            s.at,
            crate::placement::Agent {
                agent_id: agent_id.clone(),
                zone: s.zone.clone(),
                region: s.region.clone(),
                kinds: s.kinds.clone(),
            },
        );
        match freshest.entry(agent_id.clone()) {
            std::collections::hash_map::Entry::Occupied(mut entry) if s.at >= entry.get().0 => {
                entry.insert(local);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(local);
            }
            _ => {}
        }
    }
    freshest.into_values().map(|(_, agent)| agent).collect()
}

fn declared_kinds(raw: Option<&str>) -> Option<Vec<crate::models::CheckKind>> {
    Some(
        split_kinds(raw?)
            .iter()
            .filter_map(|k| crate::models::CheckKind::parse(k))
            .collect(),
    )
}

/// The agents eligible to probe one kind.
///
/// Separate from `assign` so placement keeps its single question — given these
/// agents, which take this check — and eligibility stays the caller's.
fn fleet_for(
    fleet: &[crate::placement::Agent],
    kind: crate::models::CheckKind,
) -> Vec<crate::placement::Agent> {
    fleet
        .iter()
        .filter(|a| a.kinds.as_ref().is_none_or(|ks| ks.contains(&kind)))
        .cloned()
        .collect()
}

// ---- Agent credentials ----

/// A credential as the admin UI reads it. The secret is absent by construction:
/// there is nothing stored that could populate it.
#[derive(Serialize)]
struct AgentCredentialDto {
    id: String,
    zone: String,
    region: String,
    created_at: i64,
    revoked: bool,
}

impl From<&AgentCredential> for AgentCredentialDto {
    fn from(c: &AgentCredential) -> Self {
        Self {
            id: c.id.clone(),
            zone: c.zone.clone(),
            region: c.region.clone(),
            created_at: c.created_at,
            revoked: c.revoked(),
        }
    }
}

#[derive(Serialize)]
struct AgentCredentialsResponse {
    credentials: Vec<AgentCredentialDto>,
}

#[derive(Deserialize)]
pub(in crate::coordinator) struct MintCredentialRequest {
    zone: String,
    region: String,
}

#[derive(Serialize)]
struct MintedCredentialResponse {
    credential: AgentCredentialDto,
    /// The only time this value exists outside the agent that will use it.
    token: String,
}

pub(in crate::coordinator) async fn list_agent_credentials_handler(
    State(coord): State<Coordinator>,
    _admin: AdminSession,
) -> Response {
    match coord.db(|storage| storage.list_agent_credentials()).await {
        Ok(rows) => Json(AgentCredentialsResponse {
            credentials: rows.iter().map(AgentCredentialDto::from).collect(),
        })
        .into_response(),
        Err(e) => internal_error(e),
    }
}

/// Mint a credential for one zone.
///
/// The token comes back exactly once. Storing it so it could be shown again
/// would defeat hashing it, so the UI has to make the operator copy it now.
pub(in crate::coordinator) async fn mint_agent_credential_handler(
    State(coord): State<Coordinator>,
    AdminJson(body): AdminJson<MintCredentialRequest>,
) -> Response {
    let zone = body.zone.trim();
    let region = body.region.trim();
    // A blank location would bind the credential to nothing and place its
    // agents in the "no location declared" bucket, which is the state minting
    // is supposed to replace.
    if zone.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "zone must not be empty");
    }
    if region.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "region must not be empty");
    }
    let now = chrono::Utc::now().timestamp_millis();
    let (record, token) = crate::credential::mint(zone, region, now);
    if let Err(e) = coord.store_credential(&record).await {
        return internal_error(e);
    }
    info!(id = %record.id, zone, region, "minted agent credential");
    (
        StatusCode::CREATED,
        Json(MintedCredentialResponse {
            credential: AgentCredentialDto::from(&record),
            token,
        }),
    )
        .into_response()
}

pub(in crate::coordinator) async fn revoke_agent_credential_handler(
    State(coord): State<Coordinator>,
    Path(id): Path<String>,
    _admin: AdminSession,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let revoke_id = id.clone();
    let revoked = coord
        .db(move |storage| storage.revoke_agent_credential(&revoke_id, now))
        .await;
    match revoked {
        Ok(true) => {
            if let Err(e) = coord.reload_credentials().await {
                return internal_error(e);
            }
            // Revocation is security-relevant and time-sensitive — pushed
            // to peers immediately rather than waiting on anti-entropy, so
            // a credential stops working fleet-wide as close as possible to
            // the moment an operator revoked it.
            coord
                .restamp_and_push(storage::ConfigKind::Credential, &id)
                .await;
            warn!(id = %id, "revoked agent credential");
            Json(RevokedResponse { revoked: id }).into_response()
        }
        Ok(false) => error_response(StatusCode::NOT_FOUND, "no such credential"),
        Err(e) => internal_error(e),
    }
}

#[derive(Serialize)]
struct RevokedResponse {
    revoked: String,
}

#[cfg(test)]
mod tests {
    use super::{
        declarations, declared_kinds, fleet_for, placement_fleet, resolve_checks_for_agent,
        SeenAgent,
    };
    use crate::config::CheckConfig;
    use crate::coordinator::storage::AgentPollRow;
    use crate::models::CheckKind;
    use crate::placement::Agent;
    use std::collections::HashMap;

    fn agent(id: &str, kinds: Option<&[CheckKind]>) -> Agent {
        Agent {
            agent_id: id.into(),
            zone: "eu-west-a".into(),
            region: "eu-west".into(),
            kinds: kinds.map(|k| k.to_vec()),
        }
    }

    #[test]
    fn missing_secret_omits_only_the_affected_check() {
        let good = CheckConfig {
            name: "good".into(),
            target: "https://good.test/".into(),
            ..Default::default()
        };
        let missing = CheckConfig {
            name: "needs-token".into(),
            target: "https://missing.test/".into(),
            headers: vec![crate::config::Header {
                name: "Authorization".into(),
                value: "Bearer ${MISSING_TOKEN}".into(),
            }],
            ..Default::default()
        };

        let (checks, errors) = resolve_checks_for_agent([&good, &missing], |_| None);

        assert_eq!(checks, vec![good]);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].name, "needs-token");
        assert_eq!(
            errors[0].error,
            "missing environment variable MISSING_TOKEN for check secret reference"
        );
        assert!(!errors[0].error.contains("Bearer"));
        assert!(!errors[0].error.contains("missing.test"));
    }

    #[test]
    fn placement_fleet_selects_newest_record_and_local_on_ties() {
        let mut seen = HashMap::new();
        seen.insert(
            "agent".into(),
            SeenAgent {
                zone: "local-zone".into(),
                region: "local-region".into(),
                at: 100,
                window_ms: 100,
                rtt_ms: None,
                kinds: Some(vec![CheckKind::Http]),
                build: None,
            },
        );
        let replicated = AgentPollRow {
            agent_id: "agent".into(),
            zone: "peer-zone".into(),
            region: "peer-region".into(),
            at: 101,
            window_ms: 100,
            rtt_ms: None,
            build: None,
            kinds: Some("tcp".into()),
            coordinator: None,
        };

        let fleet = placement_fleet(101, &seen, std::slice::from_ref(&replicated));
        assert_eq!(fleet[0].zone, "peer-zone");
        assert_eq!(fleet[0].region, "peer-region");
        assert_eq!(fleet[0].kinds, Some(vec![CheckKind::Tcp]));

        let fleet = placement_fleet(
            100,
            &seen,
            &[AgentPollRow {
                at: 100,
                ..replicated
            }],
        );
        assert_eq!(fleet[0].zone, "local-zone");
        assert_eq!(fleet[0].region, "local-region");
        assert_eq!(fleet[0].kinds, Some(vec![CheckKind::Http]));
    }

    #[test]
    fn an_agent_polling_a_peer_still_reports_its_build_and_coordinator() {
        let local = SeenAgent {
            zone: "z".into(),
            region: "r".into(),
            at: 100,
            window_ms: 100,
            rtt_ms: None,
            kinds: None,
            build: Some("local".into()),
        };
        let seen = HashMap::from([("here".to_string(), local)]);
        let peer = |agent: &str, at: i64| AgentPollRow {
            agent_id: agent.into(),
            zone: "z".into(),
            region: "r".into(),
            at,
            window_ms: 100,
            rtt_ms: None,
            build: Some("peer".into()),
            kinds: None,
            coordinator: Some("coord-b".into()),
        };
        let declared = declarations(vec![peer("there", 90), peer("here", 50)], &seen, "coord-a");
        assert_eq!(declared["there"].build.as_deref(), Some("peer"));
        assert_eq!(declared["there"].coordinator.as_deref(), Some("coord-b"));
        // The newer local poll wins, and names this node.
        assert_eq!(declared["here"].build.as_deref(), Some("local"));
        assert_eq!(declared["here"].coordinator.as_deref(), Some("coord-a"));
    }

    #[test]
    fn an_agent_that_never_declared_stays_eligible_for_everything() {
        // The upgrade path. Every agent in an existing fleet predates this
        // parameter, so treating silence as "runs nothing" would make the
        // coordinator upgrade turn monitoring off.
        assert_eq!(declared_kinds(None), None);

        let fleet = vec![agent("old", None)];
        assert_eq!(fleet_for(&fleet, CheckKind::Openmetrics).len(), 1);
    }

    #[test]
    fn an_agent_that_declared_nothing_means_nothing() {
        // Distinct from never declaring: this agent answered the question.
        // Reading an explicit empty list as "anything" would make the one
        // declaration a coordinator cannot act on the one it must trust most.
        assert_eq!(declared_kinds(Some("")), Some(vec![]));

        let fleet = vec![agent("mute", Some(&[]))];
        assert!(fleet_for(&fleet, CheckKind::Http).is_empty());
    }

    #[test]
    fn a_kind_the_coordinator_does_not_know_is_dropped_not_rejected() {
        // An agent newer than its coordinator is a legitimate ordering. The
        // kinds it names that this node has never heard of are simply kinds it
        // will never be assigned, not grounds to discard the whole declaration
        // and mistake a capable agent for one that can do nothing.
        assert_eq!(
            declared_kinds(Some("http, tcp ,quantum-ping,")),
            Some(vec![CheckKind::Http, CheckKind::Tcp]),
            "known kinds survive an unknown neighbour"
        );
    }

    #[test]
    fn placement_only_sees_agents_that_can_run_the_kind() {
        // The reported failure, from the coordinator's side. An old agent was
        // handed an openmetrics check, dropped it on parse, and the check stayed
        // covered on paper while never being probed. Narrowing the fleet first
        // means the assignment lands on an agent that will actually run it.
        let fleet = vec![
            agent("old", Some(&[CheckKind::Http, CheckKind::Tcp])),
            agent("new", Some(&[CheckKind::Http, CheckKind::Openmetrics])),
        ];

        let capable = fleet_for(&fleet, CheckKind::Openmetrics);
        assert_eq!(
            capable
                .iter()
                .map(|a| a.agent_id.as_str())
                .collect::<Vec<_>>(),
            ["new"]
        );
        // And a kind both can run still reaches both, so narrowing costs
        // nothing when the fleet is uniform.
        assert_eq!(fleet_for(&fleet, CheckKind::Http).len(), 2);
    }
}
