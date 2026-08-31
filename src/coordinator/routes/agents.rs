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

    // Poll RTT is not on the persisted agent rows, so join it in by id.
    // Snapshot once rather than lock per row. The replicated poll rows come
    // first and local `seen` overrides them: an agent polling a peer would
    // otherwise show a blank RTT here, which reads as "never answered".
    let mut rtt_by_id: std::collections::HashMap<String, Option<i64>> = coord
        .inner
        .storage
        .lock()
        .list_agent_polls()
        .unwrap_or_default()
        .into_iter()
        .map(|p| (p.agent_id, p.rtt_ms))
        .collect();
    rtt_by_id.extend(
        coord
            .inner
            .seen
            .read()
            .iter()
            .map(|(id, s)| (id.clone(), s.rtt_ms)),
    );
    let agents = rows
        .into_iter()
        .map(|r| {
            let silence = now - r.last_seen;
            let poll_rtt_ms = rtt_by_id.get(&r.agent_id).copied().flatten();
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
            }
        })
        .collect();
    Json(AgentsResponse {
        agents,
        checks_with_no_live_agent: orphaned,
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
        })
        .collect();

    let mine = checks
        .into_iter()
        .filter(|c| c.enabled && c.kind.is_agent_probed())
        .filter(|c| {
            crate::placement::assign(&c.name, &c.placement, &c.agents, &fleet)
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
