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
use super::super::{error_response, internal_error, storage, Coordinator, SeenAgent};
use super::checks::PurgedResponse;
use super::session::{bearer, require_admin};
use crate::config::CheckConfig;
use crate::credential::AgentCredential;
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
    /// The build it reported on its last poll here. `None` for an agent that
    /// predates build reporting, or one polling a peer rather than this node.
    version: Option<String>,
    /// Check kinds it says it can probe. `None` means it never said, which is
    /// read as "anything" — not as "nothing".
    kinds: Option<Vec<String>>,
}

pub(in crate::coordinator) async fn ingest_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
    Json(mut batch): Json<ResultBatch>,
) -> Response {
    let caller = match authenticate_agent(&coord, &headers) {
        Ok(c) => c,
        Err(denied) => return *denied,
    };
    // An agent does not get to say where it is. Zone and region decide placement
    // and are shown as the location an outage was seen from, so a caller able to
    // name its own could answer for a zone it is not in.
    for r in &mut batch.results {
        r.zone.clone_from(&caller.zone);
        r.region.clone_from(&caller.region);
    }
    let n = batch.results.len();
    coord.ingest(batch.results).await;
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
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let rows = match coord.inner.storage.lock().list_agents(now) {
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
        .map(|(name, _)| name.clone())
        .collect();

    // What each agent said about itself on its last poll here. Snapshot once
    // rather than lock per row.
    //
    // Capability is local knowledge: it arrives on a poll, and a poll goes to
    // one coordinator. An agent polling a peer has no declaration here and is
    // read as capable of anything — the same assumption made of an agent that
    // predates declaring, so it under-reports rather than inventing a fault.
    let declared = coord.inner.seen.read().clone();

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
                        .and_then(|s| s.kinds.as_ref())
                        // Silence is not a refusal: an agent that never
                        // declared must not be counted as declining, or a
                        // fleet that has said nothing would report every
                        // check unrunnable.
                        .is_some_and(|kinds| !kinds.contains(&m.kind))
                })
        })
        .map(|(name, _)| name.clone())
        .collect();

    // Poll RTT is not on the persisted agent rows, so join it in by id.
    // The replicated poll rows come first and local `seen` overrides them: an
    // agent polling a peer would otherwise show a blank RTT here, which reads
    // as "never answered".
    let mut rtt_by_id: std::collections::HashMap<String, Option<i64>> = coord
        .inner
        .storage
        .lock()
        .list_agent_polls()
        .unwrap_or_default()
        .into_iter()
        .map(|p| (p.agent_id, p.rtt_ms))
        .collect();
    rtt_by_id.extend(declared.iter().map(|(id, s)| (id.clone(), s.rtt_ms)));
    let agents = rows
        .into_iter()
        .map(|r| {
            let silence = now - r.last_seen;
            let poll_rtt_ms = rtt_by_id.get(&r.agent_id).copied().flatten();
            let said = declared.get(&r.agent_id);
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
                version: said.and_then(|s| s.build.clone()),
                kinds: said.and_then(|s| {
                    s.kinds
                        .as_ref()
                        .map(|ks| ks.iter().map(|k| k.as_db().to_string()).collect())
                }),
            }
        })
        .collect();
    Json(AgentsResponse {
        agents,
        checks_with_no_live_agent: orphaned,
        checks_no_agent_can_run: unrunnable,
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
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let still_assigned: Vec<String> = coord
        .inner
        .checks
        .read()
        .iter()
        .filter(|(_, m)| m.agents.contains(&agent_id))
        .map(|(name, _)| name.clone())
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
    match coord.inner.storage.lock().purge_agent(&agent_id) {
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
        // going idle on upgrade.
        return match coord.inner.storage.lock().list_checks() {
            Ok(checks) => Json(AgentChecksResponse {
                checks: checks
                    .into_iter()
                    .filter(|c| c.enabled && c.kind.is_agent_probed())
                    .collect(),
            })
            .into_response(),
            Err(e) => internal_error(e),
        };
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

    let checks = match coord.inner.storage.lock().list_checks() {
        Ok(c) => c,
        Err(e) => return internal_error(e),
    };

    // The fleet is everyone in recent contact. Polling alone is enough: waiting
    // for a first result would mean a booting agent is invisible exactly while
    // it needs placing, and twenty agents starting together would each conclude
    // they were alone and take the entire workload.
    let fleet: Vec<crate::placement::Agent> = coord
        .inner
        .seen
        .read()
        .iter()
        .filter(|(_, s)| now - s.at <= s.window_ms)
        .map(|(agent_id, s)| crate::placement::Agent {
            agent_id: agent_id.clone(),
            zone: s.zone.clone(),
            region: s.region.clone(),
            kinds: s.kinds.clone(),
        })
        .collect();

    let mine = checks
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
            crate::placement::assign(&c.name, &c.placement, &c.agents, &capable)
                .iter()
                .any(|id| id == caller)
        })
        .collect();
    // Serialized as `CheckConfig`, which is exactly what the agent parses — one
    // type on both ends, so the probe config can't drift from the wire.
    Json(AgentChecksResponse { checks: mine }).into_response()
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
fn declared_kinds(raw: Option<&str>) -> Option<Vec<crate::models::CheckKind>> {
    let raw = raw?;
    Some(
        raw.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .filter_map(crate::models::CheckKind::parse)
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
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    match coord.inner.storage.lock().list_agent_credentials() {
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
    headers: HeaderMap,
    Json(body): Json<MintCredentialRequest>,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
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
    if let Err(e) = coord.store_credential(&record) {
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
    headers: HeaderMap,
) -> Response {
    if let Some(denied) = require_admin(&coord, &headers) {
        return denied;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let revoked = coord.inner.storage.lock().revoke_agent_credential(&id, now);
    match revoked {
        Ok(true) => {
            if let Err(e) = coord.reload_credentials() {
                return internal_error(e);
            }
            // Revocation is security-relevant and time-sensitive — pushed
            // to peers immediately rather than waiting on anti-entropy, so
            // a credential stops working fleet-wide as close as possible to
            // the moment an operator revoked it.
            coord.restamp_and_push(storage::ConfigKind::Credential, &id);
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
    use super::{declared_kinds, fleet_for};
    use crate::models::CheckKind;
    use crate::placement::Agent;

    fn agent(id: &str, kinds: Option<&[CheckKind]>) -> Agent {
        Agent {
            agent_id: id.into(),
            zone: "eu-west-a".into(),
            region: "eu-west".into(),
            kinds: kinds.map(|k| k.to_vec()),
        }
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
