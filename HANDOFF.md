# Warden — session handoff

Repo: `~/code/uptime`. The working tree contains the completed Stage A region
tree and deployment cleanup, uncommitted.

There are exactly two supported Compose topologies. `docker-compose.yaml` runs
one all-in-one `standalone` process on `:9000`; `docker-compose.cluster.yaml`
runs three regional coordinators on `:9001`–`:9003` plus three agents, all in
one `us-east` region. `:9004` and `:9005` are unused and reserved for the Stage
B multi-region/global deployment. The old `deploy/*.toml`,
`docker-compose.agents.yaml`, and root `warden.toml` have been removed; both
stacks now carry their config as Compose configs.

**Security-relevant behaviour change, read this before an incident makes you
find it the hard way:** admin sessions are now stateless HMAC-signed tokens
instead of an in-memory table (Phase 5 clustering, **§9 Clustering** in the
design doc), so that any coordinator in a cluster can validate a session a
different node created. The consequence outside clustering too: **a
coordinator restart no longer logs everyone out**, which it did before this
landed. Logging a token out revokes it into a small in-memory set until its
own TTL expires; that revocation is pushed to peers best-effort, so the worst
case of a dropped push is a logged-out token staying valid on one other node
until it would have expired anyway.

## Landed this session

| Commit | What |
|---|---|
| `33a368f` | One-hop gossip replication between coordinator peers |
| `0763999` | Admin panels no longer honour an expired session |
| `88c909e` | Per-zone agent credentials, fail-closed ingest, Credentials panel |
| _(this one)_ | Completed Stage A region tree: per-agent beliefs, credential-derived topology, four-state/two-threshold rollups, hold-down, state history, replicated policy config, and admin tree UI |
| _(this one)_ | Deployment cleanup: exactly two Compose topologies on `:9000` and `:9001`–`:9003`; old deploy configs, agents Compose file, and root config removed |
| _(this one)_ | Agent health on the public status page; notifier management with test-send and a per-check opt-out; the homepage no longer 401s |
| _(also this one)_ | Slack, PagerDuty and email notifier kinds, each with a delivery test; persistent retry queue for failed notifications |
| _(also this one)_ | Maintenance windows: scheduled spans that suppress paging, optionally announced on the public page |
| _(also this one)_ | Region Tree admin rewritten as one canonical policy tree with an inline editor, a TS mirror of `rollup()` for live preview, and per-check shadowing surfaced |
| _(also this one)_ | Per-agent beliefs replicate, so every coordinator in a region folds the whole regional fleet; agents take a coordinator *list* and fail over |

### Notifications (`Admin > Notifications`, `NotifiersAdmin.svelte`)
The backends always existed; nothing could reach them. `cfg.notifiers` was read
once in `Coordinator::new` and moved into a spawned dispatcher, and `notify_tx`
was `None` when the list was empty — so with no notifier in the config file
there was no dispatcher at all and the first one added could never fire.

Now: the list is a stored setting (same `settings` table and `load_or_seed` as
the agent display), the channel is always created, and the dispatcher clones the
list **per alert** rather than capturing it at boot. A save is live on the next
transition. `GET`/`PUT /api/v1/admin/notifiers` and
`POST /api/v1/admin/notifiers/test` are admin-gated — a webhook URL is the whole
credential for posting into that channel, and an open test-send endpoint is an
anonymous request generator.

**Delivery is now falsifiable.** `notify::send` returns `Result<(), String>` and
a non-2xx counts as a failure, so a wrong ntfy topic (404) is reported instead
of silently succeeding. Test-send returns HTTP 200 with `delivered: false` and
the reason: the request worked, the third party did not, and the form has to
tell those apart.

**Five backends** (`NotifierKind`, flat on the wire, tagged by `type`):
`webhook`, `ntfy`, `slack`, `pagerduty`, `email`. All share the routing, the
test-send and the per-check `notify` opt-out below; only the payload differs.

- **`slack` is not `webhook` aimed at a Slack URL.** Slack rejects JSON without
  `text` with 400 `invalid_payload`, so the generic webhook there looks
  configured and delivers nothing. Not pinned to `hooks.slack.com`: Mattermost
  and Discord's compat endpoint take the same shape, and `tests/notifiers.rs`
  points one at a local sink for exactly that reason.
- **`pagerduty`** is Events API v2 with `dedup_key = check_id`, so the UP
  `resolve`s the incident the DOWN `trigger`ed instead of opening a second one,
  and a flapping check reuses one PagerDuty incident. UNKNOWN triggers at
  `warning`. The payload is built by `pagerduty_event`, unit-tested — the real
  endpoint is not mockable and is not worth pretending otherwise.
- **`email`** is the only backend that is not an HTTP POST (adds `lettre`). TLS
  mode is declared (`starttls` default / `implicit` / `none`), never guessed
  from the port, and STARTTLS is *required* rather than opportunistic. `port`
  absent means 587/465/25 for the mode. One recipient — an alias is the mail
  server's job. A fresh connection per notification, no pool.
  `an_email_notifier_delivers_over_smtp` drives a real SMTP session against a
  ~40-line sink in the test file; flipping its `tls` to `starttls` makes it fail
  (the sink advertises no extensions), which is what proves the mode is
  load-bearing rather than decorative.

`Notifier::describe()` was deleted rather than extended — nothing called it but
its own test.

**Failed deliveries are queued, not lost** (`notification_queue`,
`Coordinator::retry_due` / `spawn_retries`). Dispatch tries once inline; a
failure writes the transition and the notifier *name* to DuckDB and a 30s job
retries with doubling backoff — six attempts including the first, ≈32 minutes.
Success deletes the row; exhausted attempts leave it with `next_attempt` NULL,
which is what the panel renders as "reached nobody". Given-up rows age out on
`alerts_days`; waiting rows are never pruned.

- The row holds the **name**, resolved against the live list per attempt, so
  correcting a URL in the panel fixes what is queued for it and deleting a
  notifier drops its queue. Same rule as routing.
- `retry_due(now_millis)` takes the clock as a parameter, exactly like `prune`,
  so a test reaches the next attempt without sleeping out the backoff. The
  ticking job is spawned from `serve`, not `Coordinator::new` — tests drive the
  method directly.
- No `status` column: pending vs given up is `next_attempt IS NULL`. Same
  argument as `alert_type` and `IncidentState`.
- Enqueued on failure only, so the happy path is still one HTTP request and no
  write. A crash between the failed send and the insert loses the retry —
  marked `ponytail:` in `storage.rs`; closing it means writing every
  notification twice.
- `GET /api/v1/admin/notifiers` now also returns `undelivered` (newest 20:
  check, state, notifier, attempts, `given_up`, error), rendered under the
  unrouted-checks warning.

**Routing.** A notifier has a `name` (the routing key, unique) and
`default: bool`. A check carries `notifiers: Vec<String>`:

| check | goes to |
|---|---|
| `notify = false` | nobody |
| `notifiers = []` | every notifier with `default = true` |
| `notifiers = ["team-a"]` | exactly `team-a`, default flag irrelevant |

A name nothing answers to reaches **nobody** — deliberately not a silent fall
back to the defaults, which would page the wrong people. That failure is
surfaced instead: the notifiers GET returns `unrouted_checks`, and the panel
renders it, the same way `checks_with_no_live_agent` surfaces unprobed checks.
Renaming a notifier is a routing change, so that list is how you see what it
broke.

**Several notifiers may be default at once, on purpose** — an unrouted alert
fans out to all of them, which is what you want for Slack *and* PagerDuty. It
was reported as surprising, and fairly: "default" reads as if it were exclusive.
Kept as a fan-out rather than made a radio, because exclusivity removes a real
capability and invents an awkward zero-default state; instead the panel now
states the consequence outright — *"A check that names no notifier goes to all
of these: oncall, slack"* — and warns when nothing is default. Make it exclusive
only if someone actually wants one-and-only-one; it is a `type="radio"` and a
one-line guard.

Targets are resolved in the **dispatcher** against the live list, from the name
list the check asked for at enqueue: editing a backend never changes the meaning
of an alert already queued. `notify = false` is still resolved at enqueue
(`notify_targets` returns `None`), so a queued alert is not re-judged.

Two upgrade defaults are load-bearing: `Notifier::name` defaults to empty and
`default` defaults to **true**, so a notifier written before routing keeps
paging instead of going quiet on upgrade. Checks are stored as JSON documents,
so neither field needed a migration.

### Agent health (`fleet_rows` / `fleet_health` in `coordinator/mod.rs`)
`/api/v1/status` carries an `agents` array beside `checks`: `{kind, name,
status, healthy, total}` where `kind` is `agent` or `region` and status is
`up`/`degraded`/`down`. The homepage renders it and no longer calls
`/api/v1/agents`, which is what made an anonymous load fail outright — the
endpoint is admin-gated and the `Promise.all` rejected on the 401.

**Rows are composed, not moded.** `AgentsDisplay.rows` is an ordered list whose
entries are `{kind: agent|region, name}`, so "this box, then all of us-east" is
one list. `mode` only decides what happens to agents no row covered: fold into
region rows, one row each, or nothing. `off` with rows still shows the rows —
`off` plus no rows is how the section disappears. A region row counts **every**
agent in it, including one shown individually above, because a row labelled
`us-east` that quietly omitted a machine would be a lie. The superseded `pinned`
list still parses and is folded into `rows` by `sanitized`.

**Liveness, not check health.** An agent that answers is healthy even if every
check it probes is down. Only a *poll* makes it healthy; results can lift a
silent agent to `degraded` at best, because a five-minute cron says nothing
about the last four minutes and an agent with no checks would otherwise read
dead. The fleet is the union of the in-memory `seen` map and the persisted
result rows, and an agent in both is read at its best — that is what keeps the
section honest across a restart.

**The window follows the cadence.** `AGENT_UNREACHABLE_AFTER_MS` is gone. The
agent declares `?poll_secs=` on every poll and the coordinator stores
`missed_polls × poll_secs` per agent (`missed_polls = 3`, configurable), so
detection is three missed cycles at any cadence. An agent that declares nothing
— an older binary, or an entry seeded from stored results at boot — keeps the
old fixed 90s, deliberately: the derived window is *tighter*, so a coordinator
upgraded ahead of its fleet would otherwise start evicting agents that had
changed nothing.

**The display is a setting, not a config-file decision.** `settings` is a new
table of `(key, JSON document)` — one row, `status_page.agents`. The config
block seeds it on first boot and the store wins from then on, exactly like
checks, so a restart cannot revert what an operator chose. Edited in
**Admin > Status page** (`AgentDisplayAdmin.svelte`) through `GET`/`PUT
/api/v1/admin/status-page/agents`; the PUT validates (0–100, `down_below ≤
warn_below`), trims and dedupes `rows`, persists, then swaps the in-memory
copy, so the public page changes on the next request with no restart. The GET
returns the fleet alongside the settings because a picker without the fleet is
not useful; the editor derives the region list from it.

`AgentsDisplay` is both the TOML type and the admin wire type — one shape, so
the file and the form cannot disagree about what a setting means. Note the word
collision: "Pinned" in the Agents panel means *checks pinned to an agent*, so
the row editor says "Rows, in order" and never uses the word.

**The editor is built for a big fleet.** Verified against a throwaway
coordinator holding 20 regions and 120 agents, not only against the small Compose
demo. The picker is a search box over regions and agents that mounts at most 8
of each and says how many it is hiding (`+112 more — narrow the search`); a
chip per fleet member was fine at two agents and unusable at 120. Rows reorder
by native drag (grip, drop-edge shadow) exactly like the sections list above
them, with ▲▼ as the keyboard path — puppeteer needs
`page.setDragInterception(true)` and `handle.dragAndDrop()` to exercise that;
synthetic mouse moves do not fire HTML5 drag events, and a test written with
them passes vacuously.

At that size the *public* page is 20-odd region rows, which is the point of the
remainder modes: curating it down means placing the handful of rows that matter
and setting everything else to "Not shown".

Also fixed: `broadcast_presents_the_agent_token` in `tests/gossip.rs` asserted
a literal `cluster-shared-secret` that the credentials refactor renamed out of
existence. It now asserts against the `PEER_TOKEN` const, which is what stops
it drifting again.

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

**A credential is the only way in.** No fleet-wide token, no open mode: a
coordinator with no credentials refuses every agent request until an operator
mints one. That is the intended first-run state, and it is why the compose files
are split — the coordinator starts, you mint per zone, then agents start.

**`[[coordinator.provision]]` is the unattended path** (`config.rs`,
`Coordinator::apply_provisioned_credentials`). A block declares zone, region
and a token chosen in advance (`token`, or `token_env` naming the variable
holding it), for a fleet that must be configured before the coordinator has
ever run. Everything downstream is identical to a minted credential — location
still comes from the credential, never from the agent.

Two properties carry the whole design, both covered in `tests/provision.rs`:

- **An id that already exists is skipped, revoked ones included.** So a second
  boot writes nothing (a re-provision would invent a fresh salt, restamp the
  HLC and push a config write to every peer on every restart — invisible on one
  node), and a restart cannot resurrect a credential someone revoked.
- **An unresolvable or malformed token fails startup, loudly, naming the zone**
  rather than being skipped. Skipping silently yields a fleet authenticating
  against nothing, and "no results" is the failure mode this project treats as
  the dangerous one.

In a cluster, declare the blocks on one node; the rest receive them over the
config plane.

`/internal/gossip` takes its own `peer_token`. An agent credential must never
open an endpoint that rewrites published state.

Agent `[agent] zone`/`region` are authoritative in **standalone** mode only. In
agent mode the coordinator overwrites them from the credential, and the agent no
longer sends them on the check-list request at all.

### Running locally

Standalone is one command and serves `:9000`:

```bash
docker compose up -d --build
```

The Stage A regional cluster requires explicit secrets—there are no committed
token defaults—and serves `:9001`, `:9002`, and `:9003`:

```bash
WARDEN_PEER_TOKEN=... WARDEN_AGENT_TOKEN_A=... WARDEN_AGENT_TOKEN_B=... \
  docker compose -f docker-compose.cluster.yaml up -d --build
```

The cluster is intentionally one `us-east` region: `agent-a1` and `agent-a2`
corroborate `us-east-1a` using credential A, while `agent-b1` observes
`us-east-1b` using credential B. Coordinator seeds form `a → b → c → a`.
This validates regional Stage A only; no global coordinator or second region is
present, and `:9004` / `:9005` remain reserved for Stage B.

**The demo seed no longer runs on a node with `peers` set.** An empty store
means "nobody has configured anything yet" standalone and "the config is on its
way" when joining, and the difference matters because results replicate: one
joining node used to seed a fake `seed` agent onto every dashboard in the
cluster, and it could not be removed — a result delete has no tombstone, so
`purge_agent` is node-local and the next anti-entropy sweep pulls the rows back
from a peer. Covered by
`a_joining_node_does_not_seed_demo_data_over_the_whole_cluster`
(`tests/cluster.rs`), which asserts the standalone half too so it cannot pass
vacuously.

The cluster file's header gives the current matrix. Verify transitive
membership from the seed chain, checks and credentials converging from
coordinator A, all nodes showing both `us-east` zones, and policy edits
converging through the config plane. Brief first-boot 401s are possible while
provisioned credentials replicate.

Drop the volumes (`down -v`) before a rebuild: no migrations, so a stale volume
fails on missing HLC columns.

### Stage A region tree (Phase 5 consumer complete)

Phase 5 was reopened because clustering had replicated observations but the
placement model still had no consumer: redundant agents advanced one shared
failure counter rather than corroborating one another. That consumer is now
complete.

- `NodeState` is exactly `Up | Degraded | Down | Unknown`. Per-agent confirmed
  beliefs and counters live in `agent_check_state`; stale leaves become
  `Unknown`.
- Active credentials derive the region/zone topology. Agent-submitted locations
  cannot create nodes, and empty credential-backed locations remain visible as
  `Unknown` until an agent reports.
- Every policy has `degraded_when` and `down_when`, each `any`, `majority`,
  `all`, or `quorum(n)`. These count failure evidence; `all` therefore means
  unanimous corroboration. `Unknown` is excluded from both numerator and
  denominator.
- Defaults are agents→zone `all/all`, zones→region `any/all`, and
  regions→check `any/majority`. Per-check overrides beat stored node policies,
  which beat built-ins. Stored policy writes/deletes are HLC/LWW config entries
  and replicate/anti-entropy with the rest of the config plane.
- `min_incident_secs` and `min_recovery_secs` hold candidate transitions before
  publication (both default zero). Degraded alerts default on; Degraded time
  defaults to uptime, not downtime.
- `state_history` records published root spans and is the source for
  time-weighted uptime. Zone/region alerts describe topology transitions; only
  root transitions drive check incidents and notifications.
- Mixed coordinator versions fail soft: old nodes decode `degraded` as
  `Unknown`, so a rolling upgrade may temporarily hide partial-outage detail but
  does not invent or suppress total-outage transitions.

Stage B / Phase 6 is implemented: regional-to-global state push
(`Coordinator::spawn_global_push` → `/internal/global/state`), an in-memory
global cache with cross-region aggregation and query routing (`src/global/`),
the `global` CLI mode, and `docker-compose.global.yaml` (a 3-node `us` cluster
on `:9001`–`:9003`, a single-node `tw` region on `:9004`, and the global node on
`:9005`). The global node holds no raw data; drill-down reads reverse-proxy to
the owning region. Cross-region alerting is off unless `[global] notifiers` is
set and pages only on all-regions-down or a dark region. Not done (deliberately
deferred): global-to-global HA replication of the cache — the wire format and
the single `apply_push` mutation point are shaped for it, but multiple global
nodes do not yet sync.

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
- Coordinator sessions are now stateless HMAC-signed tokens (§9 Clustering),
  not an in-memory table, so **a restart no longer expires one** — the
  opposite of how this used to work. To force one out for testing, use
  logout (which revokes it) or wait out its TTL.
- Tests that need an env var genuinely absent get their own test binary;
  `remove_var` races with anything running beside it (`agent_auth_closed.rs`).
- `cd` is aliased oddly in this shell — use the tool's `cwd` argument.

## Outstanding work

### 1. Incidents — full status-page model _(landed)_
Replaced outright, no migration (pre-release). Model and reasoning live in
**`uptime-monitor-design.md` §7 Alerts & Incidents**, which is now the record;
what follows is only what a reader needs to navigate the code.

`alerts` is the event log, built out to the documented model: `id`, `scope`,
`node_id`, `previous_state`/`current_state`, and `incident_id` **stamped as the
row is written**. Membership is a fact the writer knows; a time-window join was
drafted and thrown out because it cannot separate overlapping causes and would
absorb unrelated history the moment a check joined an affects list.
`alert_type` is derived in the DTO — storing a summary of two columns beside
them is drift waiting to happen.

`incidents` is the operator-facing record: `title`, `state`, editable
`started_at`, `global`, `origin_check`, plus `incident_checks` (blast radius,
with `source` = origin/dependency/manual) and `incident_updates` (the
narrative). No `status`, no `details`, no `acknowledged_at/by` — all three were
second copies of something else.

- **`detected` is where automatic incidents start.** `investigating` claims a
  human is on it, which is a lie for the first minutes of every outage and an
  expensive one on a public page.
- **Every state change is an update**, including the two the coordinator writes
  itself. Acknowledging is just the first authored update, which is why there
  is no acknowledge endpoint and no ack columns: "has anyone looked?" is
  `EXISTS(incident_updates WHERE author IS NOT NULL)`, and it stays right when
  an operator jumps straight to `identified`.
- **`depends_on` (new per-check field) groups the blast radius.** A dependent
  confirming DOWN joins its upstream's open incident as `dependency` instead of
  opening its own. **One hop, not transitive** — cycle-safe with no seen-set,
  and `web→api→db` all down is deliberately two incidents. Grouping is not
  suppression: the dependent still alerts and still notifies. Edited in the
  check editor ("Depends on", beside Confirm DOWN after); checks are stored as
  JSON documents, so no schema change was needed to carry it.
- **Auto-resolve reuses `failures_before_down` as the up-side run.** A check
  needing three failures to be believed down needs three successes to be
  believed back, so a flapping check stays inside ONE incident instead of
  opening and closing twenty. At the default of 1 this is the old behaviour.
  Only incident resolution reads it — `current_state` still flips UP on the
  first good probe, so uptime maths and the grid are untouched.
- Auto-resolve fires whether or not a human has acknowledged.

**Two bugs a live smoke test caught that the suite did not** (both now have a
regression test, `a_manual_incident_is_never_auto_resolved`):
`open_incident_for` returned manually created incidents, so a passing check
pulled one into the resolve path; and `try_resolve` iterated the gating checks
and fell through on an empty set, so an incident whose attachments are all
`manual` resolved vacuously. The fix is the `origin_check IS NOT NULL` filter —
mutation-verified. The empty-set guard was deliberately **not** kept: with the
filter in place it is unreachable, so it could only ever be untested code. The
invariant is recorded on `try_resolve`'s doc comment instead.

Public: `GET /api/v1/incidents`, `GET /api/v1/incidents/:id` (carries the
narrative), and `#/incident/<id>` in the SPA. Writes are all under
`/api/v1/admin/incidents…` and admin-gated, covered by
`incident_writes_are_admin_only_while_reads_are_public`.

Still deliberately out: severity, owner per incident, transitive dependencies,
and suppressing a dependent's notification.

**Deploying a no-migration schema change onto an existing volume.** "No
migrations" is fine for the code and a trap for deployment: the Compose config
sets `db_path = /data/warden.duckdb` on its named volume, so old
`alerts`/`incidents` tables survive a rebuild and
`CREATE TABLE IF NOT EXISTS` silently no-ops on them. The new binary then boots
cleanly and 500s on every incident and alert read:

```
ERROR retention prune failed error=Binder Error: Referenced column "state" not found
GET /api/v1/incidents -> 500
```

Do **not** answer that by deleting the volume. It holds the real checks
(`homeassistant`, `unifi-certificate`, `ddns-*`, …), ~350k results, the
status-page settings and **both agent credentials** — losing those means
re-minting and rewriting `.env` before the fleet reports again. Drop only the
tables whose shape changed and everything else survives:

1. Prove the incompatibility against a **copy** first (`docker cp` the active
   service's `/data/warden.duckdb` to `/tmp/...`, then point a local coordinator
   at it). Never diagnose on the live file.
2. Stop the active service, then copy **both** `warden.duckdb` *and*
   `warden.duckdb.wal` out. **Stopping the container does not checkpoint the
   WAL** — there were 2.3 MB of committed rows in it, and taking only the main
   file loses them *and* leaves a mismatched WAL behind. Opening the pair with
   DuckDB replays and folds the WAL in (results went 342,013 → 350,155, which is
   how you know it mattered); closing leaves one consolidated file.
3. `DROP TABLE IF EXISTS incident_updates, incident_checks, incidents, alerts`
   — anything with no `duckdb` CLI on the host can borrow the crate's bundled
   engine from a throwaway test in `tests/`.
4. Copy the single file back, delete the stale `.wal`, and restart that service.

Keep a backup of the untouched pair until the new stack is verified. Service and
volume names differ between the standalone and cluster topologies; inspect the
selected Compose file rather than relying on old `uptime-*` container names.

### 2. Routing by section _(per-check routing landed this session)_
A check names its notifiers and there is a default set, so "payments pages the
DB team, everything else pages oncall" works today. What is still missing is
routing a whole **section** at once: with fifty checks in one group, an
audience change is fifty edits. The shape would be a section-level default that
a check inherits unless it names its own — the resolution order already exists
in `notify_targets`, so it is one more lookup rather than a new model.

Also unbuilt: escalation (page B only if A is not acknowledged), and any
schedule, which overlaps the maintenance panel below.

### 3. Maintenance panel _(landed)_
`Admin > Maintenance` is a real panel now (`MaintenanceAdmin.svelte`), and
`AdminPlaceholder.svelte` is deleted — it had no other user. Model and reasoning
are in **`uptime-monitor-design.md` §8 Maintenance Windows**; what follows is
what a reader needs to navigate the code.

A window (`src/maintenance.rs`) is `title`, `starts_at`, `ends_at`, `checks`
(empty = every check) and `announce`. Suppression is **one arm in
`notify_targets`**, which now takes the transition's timestamp: a window is the
per-check `notify = false` switch on a timer, so probing, results, alert rows
and incidents all continue exactly as they do for a muted check. That is the
whole implementation — there is no second suppression path to keep in step.

- Judged against the transition's own timestamp, not the clock, so a batch
  posted late is read by when it happened. Known consequence: an outage that
  starts inside a window and recovers after it ends pages the recovery having
  never paged the failure. Fixing that means remembering per check what was
  suppressed, which is state to persist for a cosmetic gain.
- Stored, not configured: one `settings` document (`maintenance.windows`) via
  the same `load_or_seed` as the notifiers, with **no** `[[maintenance]]` block
  to seed it — a window is booked at a moment and nobody redeploys to schedule
  one. Replaced as a whole by the PUT, so a window has no id.
- `announce` publishes the operator's title under the overall-status banner
  while the window runs (`maintenance` on `/api/v1/status`, `.mbanner` in
  `App.svelte`). It names no checks and is not clickable: a window has no
  updates and no permalink, which is what separates it from an incident.
  Advance notice is deliberately out — that is a manually opened incident.
- Both `/api/v1/admin/maintenance` verbs are admin-gated. A window is a licence
  to go quiet, so an anonymous PUT could silence the fleet;
  `maintenance_windows_are_admin_only` asserts the 401 *and* that the refused
  PUT stored nothing.
- The editor is a `<select multiple>` of check names from the same GET, and two
  `datetime-local` inputs. Native controls on purpose: the picker, the calendar
  and the timezone handling are the browser's, and the panel is ~120 lines.

Mutation-verified, per the rule below: dropping the `starts_at` bound from
`active_at` fails only `a_transition_outside_the_window_still_pages`, and making
`covers` always false fails only
`a_maintenance_window_suppresses_the_page_without_hiding_the_outage`.

Still open beside it: alert suppression by *section*, and escalation — both
listed under item 2 above.

### 4. Credential follow-ups
- **Rotation without downtime**: two live credentials per zone, so a zone can
  roll without a window. Today revoking is immediate and total.
- **`last_used_at`**: deliberately not stored — it would be a DB write per
  ingest. Without it there is no safe way to tell whether a credential is idle
  before revoking it. Sample it periodically rather than per request.
- **Optional one-token-per-agent hardening**: credentials currently derive
  topology at region/zone grain, and the demo intentionally shares credential A
  between two corroborating agents in `us-east-1a`. Minting one credential per
  agent would narrow revocation/impersonation scope but needs no new credential
  primitive.
- **Config seeding** _(landed)_: `[[coordinator.provision]]` gives
  `provision_agent_credential` a config surface, so an unattended first boot
  works — see the credentials section above and `docker-compose.cluster.yaml`.

### 5. Smaller, known
- Closed: anti-entropy now exists. Every `anti_entropy_secs` a node pulls one
  random alive peer's digest (`GET /internal/digest` — per-check max-ts/count
  over a bounded window, plus high-water marks for alerts/incidents and
  config) and backfills exactly the ranges it is behind on
  (`GET /internal/backfill`, paged so a catch-up cannot starve ingest under
  the storage mutex). A peer unreachable when a write happens is caught up on
  the next sweep instead of missing it permanently. The same sweep reconciles
  the config plane, pulling a kind from `GET /internal/config` when the peer's
  stamp is ahead *or* the row counts disagree — config rows are updated in
  place, so a high-water mark alone cannot see a row this node never received.
  See **§9 Clustering**.
- The public check-detail page still exposes `agent_id` per latency band. It is
  a deliberate feature, but it is fleet information on an unauthenticated page.
- `fetchAgents()` in `api.ts` throws a plain `Error`, not `AdminError`, so the
  editor shows a generic message instead of "your session expired" when a
  session dies mid-edit. `AgentsAdmin` works around it by matching on `401`.
- Agents UI filters client-side over the full fleet. Fine into the hundreds;
  past ~1000 the endpoint needs server-side paging.
- Closed: fleet health now agrees across the cluster. Agent poll time is
  persisted as a column instead of living only in the in-memory `seen` map,
  so two coordinators serving the same status page read the same answer for
  who is polling rather than disagreeing based on whichever of them happened
  to receive the last poll.
- The demo seed reports as an agent (`agent_id = "seed"`), so a coordinator with
  no real checks shows a `seed` region on the public page. Harmless, and it
  disappears the moment a real check exists — but it is the demo data leaking
  into a new surface.
- The retry queue stays per coordinator, in its own DuckDB — and that is now
  known to be permanently correct, not just correct today. The clustering
  invariant (**§9 Clustering**: the observing coordinator owns evaluation and
  alerting) means a peer never dispatches on a replicated alert, so there is
  exactly one coordinator ever trying to send any given notification, however
  large the cluster gets. Nothing here needed to change.
- Notifiers are replaced as a whole list, so two admins editing the panel at
  once is last-write-wins. Fine for a list of two; it is the reason there is no
  per-notifier id.
- Test-send builds its own `reqwest::Client` per call rather than sharing the
  dispatcher's. One TLS handshake on a button press, against not threading a
  client through the handler.
- Closed: a zone's agents no longer split across coordinators. Per-agent
  beliefs replicate in `ReplicationBatch.beliefs`, keyed on `last_check` by
  `StateTracker::merge_agent`, so every node folds the same tree. Before this,
  `agent-a1` and `agent-a2` — the redundant pair in `us-east-1a`, pointed at
  different coordinators — read as two separate one-agent zones on each node,
  and the zone's `all/all` policy was satisfied by whichever single agent
  reported locally. The unit test for that property passed the whole time; it
  was unreachable in any real deployment. `tests/cluster.rs`
  (`a_zones_agents_fold_into_one_zone_on_every_coordinator`) now pins it over
  real sockets.
- Closed: `agent.coordinator_url` takes a list, so a coordinator going down no
  longer stops an agent. Rotation is on transport failure only — a 401 is an
  answer, and rotating away from it would spread a bad token across the region.
  The cursor is an `Arc` inside `AgentConfig`, so the sync loop and the poster
  loop fail over together rather than drifting onto different coordinators.
  There is deliberately no failback: flapping on every blip would move
  placement around for nothing.
- This was regional HA for *reads and probes*. Stage B is now built on top of
  it: `CoordinatorConfig` has a `region` field (used only to label pushes), a
  `[coordinator.global]` push block, and the `global` CLI mode exists. `:9004`
  (tw region) and `:9005` (global node) are now used by
  `docker-compose.global.yaml`. A coordinator still mints a credential for any
  region it is asked for — placement, not the global tier, owns that.
- Closed: deployment now has exactly two Compose files. The standalone file is
  one process on `:9000`; the Stage A cluster file is three coordinators on
  `:9001`–`:9003` with three agents in one `us-east` region. Boot order does
  not matter—seeds are retried—and each coordinator has its own volume. Do not
  reintroduce `deploy/*.toml`, `docker-compose.agents.yaml`, or root
  `warden.toml`; reserve `:9004` / `:9005` for Stage B.
- `git stash` holds an earlier AgentsAdmin experiment of yours
  ("superseded by scale rebuild") — `git stash pop` if you want it back,
  `git stash drop` to bin it.

## Verification gates

Run with `WARDEN_AGENT_TOKEN` **exported** — the point is that tests are not
sensitive to it.

| Gate | Expected |
|---|---|
| `cargo test` | 363 passed, 0 failed across 13 suites |
| `cargo fmt --check` | clean |
| `cargo clippy --all-targets` | 0 warnings |
| `npm run build` (in `frontend/`) | clean |
| `npx svelte-check --threshold error` (in `frontend/`) | 0 errors, 0 warnings |
| `npx vitest run` (in `frontend/`) | 59 passed across 4 files |
| `docker compose config -q` (both Compose files) | clean |

Incident work is covered by seven tests in `tests/admin_api.rs`
(`incidents_open_on_down_and_resolve_on_up`,
`a_dependent_check_joins_its_upstreams_incident`,
`unrelated_checks_falling_together_stay_separate_incidents`,
`failure_confirmation_does_not_invent_a_recovery_window`,
`auto_resolve_fires_even_after_a_human_has_acknowledged`,
`a_manual_incident_is_never_auto_resolved`,
`incident_writes_are_admin_only_while_reads_are_public`) plus the wire-shape
assertions in `tests/ws_api.rs`. The two lifecycle bugs above were found by a
**live** run, not by the suite — a standalone coordinator on `:8077` with a
`depends_on` chain in its config is worth ten minutes before believing any
reconciliation change.

Maintenance windows: five unit tests in `src/maintenance.rs` for the predicate,
and five in `tests/notifiers.rs`
(`a_maintenance_window_suppresses_the_page_without_hiding_the_outage`,
`a_transition_outside_the_window_still_pages`,
`an_announced_window_is_published_while_it_runs`,
`maintenance_windows_are_admin_only`,
`a_window_that_ends_before_it_starts_is_refused`). Live-verified too, the same
way: a standalone coordinator on `:8077` plus `vite --port 5174`, booking a
window through the panel, then reading it back on the anonymous `/api/v1/status`
and again after a coordinator restart — which is what proves the settings
document, not just the in-memory copy.

Wait **≥55 s** after `docker compose up`; `seed_mock` does 18–26 s of DuckDB
inserts and shorter readiness probes report false failures.
