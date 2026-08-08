# Warden — session handoff

Repo: `~/code/uptime`. Working tree clean apart from `uptime-monitor-design.md`, which is yours.

## Landed this session

| Commit | What |
|---|---|
| `33a368f` | One-hop gossip replication between coordinator peers |
| `0763999` | Admin panels no longer honour an expired session |
| _(this one)_ | Per-zone agent credentials, fail-closed ingest, Agents UI rebuilt for scale, Credentials admin panel |

### Gossip (`src/coordinator/gossip.rs`)
Cycles are prevented structurally: a node gossips only transitions it observed
itself and never re-gossips one it received, so there is no message id, seen-set
or TTL. Receiving a transition mirrors state, records the alert and updates live
dashboards, but deliberately does **not** notify — the observing node already
paged.

Found and fixed along the way: `list_status` is rooted in `check_results`, so a
node with no local measurements produced no row and read `unknown`. Gossip could
never have moved a peer's status page. Latency and uptime stay honestly empty on
a node that measured nothing rather than being fabricated.

### Agent credentials (`src/credential.rs`)
`Authorization: Bearer <id>.<secret>`; only a salted SHA-256 of the secret is
stored, and the plaintext exists once, in the mint response.

**The grain is the zone, not the agent.** One credential is minted per zone and
every agent in that zone carries the same value in `[agent] token`. Nothing
forces that grain — several credentials may exist for one zone, which is how you
get per-agent revocation if you want it — but the default provisioning story is
one token per zone. Consequences: revoking takes down the whole zone at once, and
an agent can still impersonate a *sibling in its own zone* (`agent_id` remains
self-asserted). What it cannot do is claim a different zone.

**The credential carries the agent's location, and that is the point.** Zone and
region used to be self-asserted and placement believes them, so anyone holding
the fleet-wide token could register agents into a zone, have real checks placed
on them, and never probe. Nothing alerts on that: "no results" reads exactly
like "not scheduled yet", so monitoring goes quiet without a signal.

Auth ladder on `/api/v1/results` and `/api/v1/agent/checks`:
1. Any live credential exists → a credential is required, location comes from it.
2. Otherwise `agent_token` (legacy, fleet-wide, self-asserted location).
3. Neither → **401**. This used to accept.

Minting the first credential retires the shared token, so the weaker path cannot
linger. `/internal/gossip` now takes `peer_token` (falls back to `agent_token`),
because once agents hold minted credentials the shared token is no longer handed
out.

## Ground rules worth keeping

- Never weaken a test to get green.
- Mutation-test anything security-shaped: break the implementation deliberately
  and confirm the right test fails. Three gossip mutations and two credential
  mutations were each caught by exactly one test. A test that cannot fail is
  worse than no test — `unknown_is_carried_faithfully` passed vacuously at first
  because UNKNOWN is also the default.
- Verify against **live JSON**, not the TS types; `api.ts` and the Rust DTOs drift.
- Re-check the served bundle hash against `frontend/dist` after a rebuild;
  `rust-embed` does not reliably re-embed. If stale: `cargo clean -p warden`.
- Svelte `{#each}` keys must be unique — duplicates silently hang the page.
- `innerText` reflects CSS `text-transform`. Asserting `'Sign in required'`
  against a panel that renders uppercase gives a false negative.
- Coordinator sessions are in-memory, so **restarting the server is how you
  expire one** for testing.
- Tests that need an env var genuinely absent get their own test binary;
  `remove_var` races with anything running beside it (`agent_auth_closed.rs`).
- `cd` is aliased oddly in this shell — use the tool's `cwd` argument.

## Outstanding work

### 1. Incidents — full status-page model _(agreed scope, not started)_
Today `incidents` is `(id, check_id, status, started_at, resolved_at, details,
acknowledged_at, acknowledged_by)` and Acknowledge is the only verb. Wanted:
- `incident_updates` table (incident_id, author, body, created_at) + endpoints.
- Lifecycle states: Investigating → Identified → Monitoring → Resolved.
- Manual incident creation, with no failing check behind it.
- Severity, and an owner per incident.
- Updates rendered on the **public** status page, not just in admin.

### 2. Notifier admin UI _(not started)_
`notifiers` is config-file only and read **once** in `Coordinator::new` into a
spawned dispatcher. A UI means moving them into DuckDB like checks, and
restarting the dispatcher on change. Also wanted: a **test-send** button (a
wrong ntfy topic is currently silent — it warns to the log and nowhere else),
and per-check/per-section routing, since every notifier currently gets every
alert.

### 3. Maintenance panel
Still `AdminPlaceholder`, no backend at all. Needs scheduled windows and alert
suppression; disabling a check is currently the only way to stop probing.

### 4. Credential follow-ups
- **Rotation without downtime**: two live credentials per zone, so a zone can
  roll without a window. Today revoking is immediate and total.
- **`last_used_at`**: deliberately not stored — it would be a DB write per
  ingest. Without it there is no safe way to tell whether a credential is idle
  before revoking it. Sample it periodically rather than per request.
- **Per-agent credentials**: current grain is per-zone, so an agent can still
  impersonate a sibling *within its own zone*. Only worth it if agents run on
  hosts you do not control.

### 5. Smaller, known
- `ponytail:` in `gossip.rs` — no anti-entropy. A peer unreachable when a
  transition fires misses it permanently. Upgrade path is a periodic
  `/api/v1/status` pull between peers.
- The public check-detail page still exposes `agent_id` per latency band. It is
  a deliberate feature, but it is fleet information on an unauthenticated page.
- `fetchAgents()` in `api.ts` throws a plain `Error`, not `AdminError`, so the
  editor shows a generic message instead of "your session expired" when a
  session dies mid-edit. `AgentsAdmin` works around it by matching on `401`.
- Agents UI filters client-side over the full fleet. Fine into the hundreds;
  past ~1000 the endpoint needs server-side paging.
- `docker-compose.yaml` runs a single coordinator; there is no multi-coordinator
  gossip setup to exercise clustering under compose.
- `git stash` holds an earlier AgentsAdmin experiment of yours
  ("superseded by scale rebuild") — `git stash pop` if you want it back,
  `git stash drop` to bin it.

## Verification gates

Run with `WARDEN_AGENT_TOKEN` **exported** — the point is that tests are not
sensitive to it.

| Gate | Expected |
|---|---|
| `cargo test` | 88 + 24 + 8 + 1 + 1 + 5 + 1 = 128 passed, 0 failed |
| `cargo fmt --check` | clean |
| `cargo clippy --all-targets` | 0 warnings |
| `cd frontend && npx svelte-check --threshold error` | 213 files, 0 errors |
| `cd frontend && npx vitest run` | 22 passed |
| served bundle vs `frontend/dist` | hashes match |

Wait **≥55 s** after `docker compose up`; `seed_mock` does 18–26 s of DuckDB
inserts and shorter readiness probes report false failures.
