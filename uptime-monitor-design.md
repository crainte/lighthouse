# Uptime Monitor - Design Document

> A Rust-based, self-hosted uptime monitoring tool with hierarchical region support — replacing Uptime Kuma, Kener, and simple Prometheus setups with a single binary.

## Project Goals

**Why build this?**
- Replace three tools (Uptime Kuma, Kener, Prometheus) with one binary
- Frustration with Uptime Kuma migration issues and Kener's slowness
- Learning Rust through a practical project
- Self-hosted first: no mandatory external dependencies

**Core Philosophy:**
- Single binary deployments (agent and coordinator modes)
- Zero external dependencies for basic operation
- DuckDB for persistence (embedded, columnar, fast analytics)
- Async-first with Tokio
---

## Architecture Overview

Single binary currently has three modes:

```
warden agent        # Runs checks, posts to regional coordinators
warden coordinator  # Regional coordinator, clusterable
warden standalone   # All-in-one for small deployments
warden global       # Cross-region aggregator (Stage B / Phase 6)
```

The `global` mode is implemented (Phase 6). See `docker-compose.global.yaml`.

### Full Deployment (Stage B)

The diagram below is the Phase 6 topology, now implemented. Regional clusters
push state summaries to a global coordinator. It caches only those summaries;
data-bearing global reads fan out to the regions and merge their responses at
request time, without retaining raw regional data.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         GLOBAL COORDINATOR                              │
│                            (1-3 nodes)                                  │
│                                                                         │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                   │
│  │  Global-1   │◄─►│  Global-2   │◄─►│  Global-3   │  (gossip)         │
│  │  (cached    │   │  (cached    │   │  (cached    │                   │
│  │   state)    │   │   state)    │   │   state)    │                   │
│  └──────▲──────┘   └──────▲──────┘   └──────▲──────┘                   │
│         │                 │                 │                           │
│         └─────────────────┼─────────────────┘                           │
│                           │                                             │
│  Serves: Dashboard, global alerts, cross-region status                  │
│  Stores: Cached state summaries only (no raw data)                     │
└───────────────────────────▲─────────────────────────────────────────────┘
                            │
              state push (every 5-10s)
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
┌───────┴───────┐   ┌───────┴───────┐   ┌───────┴───────┐
│   US-EAST     │   │   EU-WEST     │   │   AP-SOUTH    │
│   REGION      │   │   REGION      │   │   REGION      │
│               │   │               │   │               │
│  (3-5 coords) │   │  (3-5 coords) │   │  (3-5 coords) │
└───────▲───────┘   └───────▲───────┘   └───────▲───────┘
        │                   │                   │
   ┌────┴────┐         ┌────┴────┐         ┌────┴────┐
   │ Agents  │         │ Agents  │         │ Agents  │
   └─────────┘         └─────────┘         └─────────┘
```

### Regional Coordinator Cluster (3-5 nodes)

Full replication within region. All coordinators have all data.

```
┌─────────────────────────────────────────────────────────────────┐
│                  REGIONAL COORDINATOR CLUSTER                   │
│                                                                  │
│  ┌───────────┐      ┌───────────┐      ┌───────────┐            │
│  │  Coord-1  │◄────►│  Coord-2  │◄────►│  Coord-3  │  (gossip)  │
│  │  DuckDB   │      │  DuckDB   │      │  DuckDB   │            │
│  │  (full)   │      │  (full)   │      │  (full)   │            │
│  └─────▲─────┘      └─────▲─────┘      └─────▲─────┘            │
│        │                  │                  │                   │
└────────┼──────────────────┼──────────────────┼───────────────────┘
         │                  │                  │
    POST to any ───────────►│◄─────── broadcast to peers
         │                  
    ┌────┴────┐        
    │  Agent  │        
    └─────────┘        
```

**Write path:**
1. Agent POSTs to any regional coordinator (load balanced)
2. Receiving coordinator writes to local DuckDB
3. Receiving coordinator broadcasts to all peers via gossip
4. All peers write to their local DuckDB
5. All nodes have complete data

**Read path:**
- Query any node — it has full data, no fan-out needed
- Node down? Query another, no data loss

**Consistency:** Eventual (small window). For uptime data, this is acceptable — a missed check result doesn't matter.

### Data Distribution

| Data | Storage | Replication |
|------|---------|-------------|
| Check results (raw) | Regional DuckDB | Full (all nodes in region) |
| Current state, per-agent beliefs, state history | Regional DuckDB | Full (all nodes in region) |
| Config (including rollup policies), alerts | Regional DuckDB | Full (all nodes in region) |
| State summaries (Stage B) | Global coordinator | Pushed from regions; cached (no raw data) |

### Query Routing (Stage B)

Phase 6 serves dashboard status and region health from the global cache. The
cache contains state summaries only. Global data-bearing reads fan out to the
relevant regional coordinators and merge responses for that request: logs,
uptime, latency/sparklines, alerts, and incidents. No raw result, log, alert,
or incident data is persisted globally. A check-specific read targets every
live region that owns that check; fleet-wide alerts and incidents target every
live region.

| Query | Where | Latency |
|-------|-------|---------|
| Dashboard (all checks status) | Global state-summary cache | Local |
| Region health | Global state-summary cache | Local |
| Logs, uptime, latency, alerts, incidents | Global → regional fan-out and request-time merge | Slowest responding region |

**Example:** You're in us-west, querying Taiwan check history:
- Dashboard load: instant (global cached summary)
- Taiwan history: routed to its owning AP region
- A cross-region uptime, latency, alert, or incident view: merged from the
  responding regions for that request, then discarded by the global node

---

## Component Design

### 1. Agent

**Responsibilities:**
- Poll a regional coordinator for assigned checks, using any configured
  coordinator URL as failover
- Execute assigned health checks (HTTP, TCP, DNS, ICMP, custom scripts)
- Report results to coordinator
- Run independently with minimal configuration

**Placement:** By default, each enabled agent-probed check is assigned to one
live agent in every zone. Placement is deterministic rendezvous hashing, so if
the selected agent disappears the next-ranked live agent in that domain takes
over without reshuffling unrelated assignments. Explicit `mode = "region"` and
`count = 1` instead assigns one live agent per region across its zones; `count`
is always per selected zone or region, not fleet-wide. Optional region and zone
allow-lists constrain eligible agents.

**Scheduling:** Each assigned check has its own task, so one slow or failing
probe does not block another. The implementation evaluates six-field (seconds
first) cron schedules against wall-clock time, rather than using startup-relative
interval timers. Consequently, aligned expressions such as `*/30 * * * * *` can
synchronize many probes at seconds 00 and 30. Deterministic spreading and bounded
probe concurrency are **not implemented**; they are planned scaling work. See
[SCALING.md](SCALING.md) for the phased 10,000-check plan and its scheduler
requirements.

**Architecture:**
```rust
// One async task per assigned check provides failure isolation.
// Each loop sleeps until the next wall-clock cron instant.
struct Agent {
    config: AgentConfig,
    checks: Vec<CheckDefinition>,
    result_tx: mpsc::Sender<CheckResult>,
}

async fn check_loop(check: CheckDefinition, tx: mpsc::Sender<CheckResult>) {
    let schedule = check.schedule().expect("validated before assignment");
    loop {
        let Some(next) = schedule.upcoming(Utc).next() else { return };
        let wait = (next - Utc::now()).to_std().unwrap_or(Duration::ZERO);
        tokio::time::sleep(wait).await;
        let result = execute_check(&check).await;
        tx.send(result).await.ok(); // Backpressure via channel capacity
    }
}

// Batched poster collects results and sends to coordinator
async fn poster_loop(rx: mpsc::Receiver<CheckResult>, coordinator_url: String) {
    let mut batch = Vec::new();
    let mut flush_interval = tokio::time::interval(Duration::from_secs(5));
    
    loop {
        tokio::select! {
            Some(result) = rx.recv() => batch.push(result),
            _ = flush_interval.tick() => {
                if !batch.is_empty() {
                    post_results(&coordinator_url, &batch).await;
                    batch.clear();
                }
            }
        }
    }
}
```

**Check Types (MVP):**
| Type | Implementation | Response |
|------|----------------|----------|
| HTTP | `reqwest` | Status code, latency, optional body match |
| TCP | `TcpStream::connect` | Connection success, latency |
| DNS | `trust-dns-resolver` | Resolution success, records |
| ICMP | `surge-ping` or raw sockets | Ping success, RTT |
| OpenMetrics | `reqwest` + text exposition parser | Selected sample value, all same-scrape samples |

**Custom Eval (Lua):**

For flexible UP/DOWN logic beyond simple status code checks, monitors can define a Lua eval function:

```yaml
checks:
  - name: api-health
    type: http
    target: https://api.example.com/health
    interval: 30s
    timeout: 10s
    eval: |
      -- Lua eval: return "UP" or "DOWN", optional latency override
      local code = math.floor(status / 100)
      if status == 429 or (code >= 2 and code <= 3) then
        return "UP", latency
      end
      return "DOWN", latency
```

Variables available in eval:
- `status` — HTTP status code (or 0 for connection failure)
- `latency` — response time in milliseconds
- `body` — response body (string, truncated to 64KB)
- `headers` — response headers (table)
- `metric` — OpenMetrics only: the selected sample's value, or `nil` when no
  sample matched the configured selection. `nil` (not `0`) marks a failed
  selection so a stopped exporter does not read as a healthy zero.
- `metrics` — OpenMetrics only: every sample from the same scrape, keyed by
  family name: `metrics[family]` is an array of `{ labels = { name = value, ... }, value = n }`
  records. This lets a script compare series that share the scrape's freshness
  and source (e.g. `used / size`).

No eval = default logic. For HTTP that is 2xx = UP; for OpenMetrics it is
"a sample was selected" = UP (a transport, parse, or selection failure is DOWN).

**OpenMetrics / Prometheus checks:**

An `openmetrics` check HTTP-scrapes an exporter, parses the Prometheus/OpenMetrics
text exposition format on the agent, and selects one sample by metric family name
plus an exact label set. Only the selected value and the same-scrape samples
travel to the coordinator for evaluation — the raw exporter body is never stored
in history or gossip.

```yaml
checks:
  - name: node-up
    type: openmetrics        # `prometheus` is accepted as an alias
    target: http://exporter.internal:9100/metrics
    interval: 30s
    timeout: 10s
    metric:
      name: up               # required: the metric family to select
      labels:                # exact match; narrows a family to one series
        - { name: job, value: node }
    # Optional: default is UP when a sample is selected. Custom Lua overrides it.
    eval: |
      -- selected value plus a same-scrape comparison
      if metric == nil then return "DOWN" end
      local used = metrics.queue_used[1].value
      local size = metrics.queue_size[1].value
      if used / size > 0.95 then return "DOWN" end
      return "UP"
```

Semantics and limits:
- **Selection is exact.** A metric name plus the exact label set identifies at
  most one sample. A missing or ambiguous selection is a *failed observation*
  (DOWN/failure), never a silent zero.
- **Single target.** OpenMetrics checks probe one exporter URL; extra `hosts`
  are rejected rather than silently scraping only the last.
- **Bounded body.** The exporter response is capped (1 MiB) before parsing; an
  oversized body fails the check rather than consuming unbounded memory.
- **Instantaneous values only.** Counters are exposed as their raw current
  value. Rate/increase/quantile math is out of scope — this is not PromQL.
- **No persistence of samples.** The selected value evaluates the check but is
  not written to `check_results`, history, status DTOs, or the DB schema.
- **Non-goal: cross-check references.** A script sees only this scrape's own
  samples. There is no `check.{other}` API and no last-value lookup across
  checks: same-scrape relationships are safe because their freshness and source
  are identical, while cross-check relationships need a later value-store and
  freshness/agent-selection design.

**Agent Configuration (YAML):**
```yaml
coordinator: https://coordinator.example.com
agent_id: us-east-1a-agent-01
zone: us-east-1a
region: us-east
```

Agents do not load or seed checks from configuration files. Operators create,
edit, and delete checks through the authenticated Admin/API control plane; the
coordinator distributes the assigned definitions to agents. A check has an
immutable UUIDv7 identity and a mutable Unicode display name. The definition
examples above describe the fields submitted through that control plane, not
file-seeded `[[checks]]` entries.

---

### 2. Regional Coordinator

**Responsibilities:**
- Receive results from agents
- Replicate to peer coordinators (gossip)
- Store in local DuckDB
- Compute region state rollup
- Push state summaries to global coordinator
- Serve regional API/UI
- Dispatch notifications for regional alerts

**Clustering:**
```rust
struct RegionalCoordinator {
    node_id: String,
    peers: Arc<RwLock<HashSet<NodeId>>>,  // Discovered via gossip
    db: DuckDbConnection,
    global_url: Option<String>,           // Push state here
}

impl RegionalCoordinator {
    async fn handle_results(&self, results: Vec<CheckResult>) {
        // 1. Write locally
        self.db.insert_results(&results).await;
        
        // 2. Update in-memory state
        self.state.update(&results);
        
        // 3. Broadcast to peers (async, best-effort)
        let peers = self.peers.read().await;
        for peer in peers.iter() {
            tokio::spawn(self.replicate_to(peer, &results));
        }
    }
    
    async fn push_state_to_global(&self) {
        // Called every 5-10 seconds
        let summary = StateSummary {
            region: self.region.clone(),
            checks: self.state.current_states(),
            region_health: self.state.compute_rollup(),
            timestamp: Utc::now(),
        };
        self.global_client.push_state(summary).await;
    }
}
```

**Gossip (via `chitchat` or similar):**
- Node discovery (who's in my cluster?)
- Failure detection (heartbeat timeout)
- Lightweight protocol, no leader election needed

---

### 3. Global Coordinator

**Responsibilities:**
- Receive state pushes from regional coordinators
- Cache current state for all regions
- Serve global dashboard/API
- Run global alert rules
- Route drill-down queries to regional coordinators

**No raw data storage.** Global coordinator only caches state summaries. When an
operator creates a global check through the authenticated control plane, the
global coordinator mints one UUIDv7 before distributing the definition; regions
must use that UUID rather than minting per-region identities.

```rust
struct GlobalCoordinator {
    regions: HashMap<RegionId, RegionState>,
    region_endpoints: HashMap<RegionId, Vec<String>>,  // For routing
}

struct RegionState {
    health: NodeState,           // UP/DOWN/DEGRADED
    checks: HashMap<CheckId, CheckState>,
    last_update: DateTime<Utc>,
}

impl GlobalCoordinator {
    async fn handle_state_push(&self, summary: StateSummary) {
        // Update cached state
        self.regions.insert(summary.region, summary.into());
        
        // Check global alert conditions
        if let Some(alert) = self.check_global_alerts() {
            self.notify(alert).await;
        }
    }
    
    async fn query_check_data(&self, check_id: &str) -> Results {
        // Fan out to live owner regions and merge this response only.
        self.merge_region_responses(check_id).await
    }
}
```

**HA for global coordinator:**
Same pattern — 1-3 nodes with gossip, all receive state pushes, all have same cached data.

---

### 4. Gate (Decision Engine)

Runs on regional coordinators. Processes incoming results and triggers alerts.

```rust
struct Gate {
    state: Arc<RwLock<RegionTree>>,
    db: duckdb::Connection,
    notify_tx: mpsc::Sender<Alert>,
}

impl Gate {
    async fn process_result(&self, result: CheckResult) {
        // Update state tree
        let mut tree = self.state.write().await;
        tree.update_check(result.check_id, result.status, result.timestamp);
        
        // Recompute rollup from leaves to root
        tree.recompute_rollup();
        
        // Check if state transition warrants alert
        if let Some(alert) = tree.check_alert_conditions() {
            self.notify_tx.send(alert).await.ok();
        }
        
        // Persist to DuckDB
        self.db.execute("INSERT INTO check_results ...", params![...]);
    }
}
```

---

### 5. Notifier

```rust
struct Notifier {
    /// Routing key. A check names the notifiers it wants, so this is identity:
    /// renaming one is a routing change, and the admin API reports the checks
    /// it orphaned rather than silently falling back to the defaults.
    name: String,
    kind: NotifierKind,
    /// Receives every alert from a check that names no notifier at all.
    default: bool,
}

enum NotifierKind {
    Webhook { url: String },
    Ntfy { topic: String, base_url: Option<String> },
    Slack { webhook_url: String },
    Pagerduty { routing_key: String },
    Email(MailConfig),
}
```

**Slack is its own kind, not the webhook pointed at a Slack URL.** Slack rejects
a payload without `text` with 400 `invalid_payload`, so the generic webhook aimed
at `hooks.slack.com` looks configured and delivers nothing on every alert. The
kind is deliberately *not* pinned to `hooks.slack.com`: Mattermost and Discord's
compatibility endpoint take the same shape.

**PagerDuty dedups on the check id.** Events API v2, `event_action = trigger` on
the way down and `resolve` on the way up, both carrying `dedup_key = check_id`
— so a recovery closes the incident the outage opened, and a flapping check
reuses one PagerDuty incident rather than spraying twenty. UNKNOWN triggers at
`warning` rather than `error`: a probe that could not decide is not an outage.

**Email declares its TLS mode rather than having it guessed from the port**
(`starttls` default, `implicit`, `none`, with the port optional and defaulting
to 587/465/25 respectively). Guessing is wrong in both directions — a plaintext
password, or a handshake that can never succeed — and a relay on a
non-standard port is ordinary. STARTTLS is *required*, never opportunistic, so a
relay that does not offer it fails instead of leaking the login. One recipient:
a distribution list is the mail server's job.

**A failed notification is queued, not logged and lost.** Dispatch tries once
inline; a failure persists the transition and the notifier's *name* to
`notification_queue`, and a background job retries it with exponential backoff —
30s, 1m, 2m, 4m, 8m, 16m, six attempts including the first, a bit over half an
hour of patience. Success deletes the row, so an empty table is the healthy
state and there is no `sent` row to accumulate or prune.

Enqueued **on failure**, not before the first attempt: the happy path stays one
HTTP request with no database write. The cost is a crash in the window between
a failed send and the insert, which is the same window an unqueued alert already
had; closing it means writing every notification twice.

The row stores the **name**, and the backend behind it is resolved at each
attempt — so correcting a wrong URL in the admin page also fixes everything
queued for it, and deleting a notifier drops its queue, because there is no
longer anybody for a retry to reach. Same principle as routing: the queue holds
what the check asked for, never a snapshot of the fleet of backends.

**Running out of attempts is a fact, not a deletion.** The row stays with
`next_attempt` NULL, which is what the notifier panel reads as "this transition
reached nobody" — the alert is still recorded and the incident still opened, but
nothing was paged, and that is precisely the failure this project refuses to
leave silent. Those rows age out on the `alerts_days` window; rows still waiting
are current work and are never pruned.

`status` is deliberately absent, for the same reason `alert_type` is: pending /
given-up is a function of `next_attempt`, and a stored copy is a column that can
disagree with the one it summarises.

---

### 6. Region Tree (Hierarchical State)

Stage A is complete. Each check is evaluated as a tree of **per-agent beliefs →
zones → regions → check root**. `NodeState` has exactly four values:

```rust
enum NodeState {
    Up,       // no configured failure threshold is satisfied
    Degraded, // partial failure: degraded threshold met, down threshold not met
    Down,     // down threshold met
    Unknown,  // no current opinion
}
```

A leaf is one agent's confirmed opinion of one check. Confirmation counters are
therefore per `(check_id, agent_id)` in `agent_check_state`; results from two
agents never advance one shared failure counter. Silent/stale agents become
`Unknown` rather than `Down`.

Leaves are cluster-wide, not node-local. Every coordinator in a region folds
the same tree over the whole regional fleet, because the leaves themselves
replicate (**§9**, `POST /internal/replicate`). A node that no agent in a given
zone reports to still holds that zone's full corroboration set.

**Topology is derived from active credentials, not agent claims.** Each
credential contributes its authenticated region and zone, including an empty
`Unknown` location before an agent first reports. Observed per-agent leaves are
admitted only beneath a credential-defined location. Revoked credentials no
longer define topology. This prevents self-asserted location from manufacturing
a region or zone that placement and rollup then trust.

**Policies have two independent failure thresholds:**

```rust
enum Threshold { Any, Majority, All, Quorum(u32) }
struct RollupPolicy {
    degraded_when: Threshold, // failing children: Degraded or Down
    down_when: Threshold,     // hard-Down children only
}
```

`down_when` must be at least as strict as `degraded_when`. `All` always means
**all known children corroborate that failure threshold**; it does not mean the
old, ambiguous "UP only if all children are UP." `Unknown` children are excluded
from both numerator and denominator. With no known children the rollup is
`Unknown`, and only the check root then falls back to `default_status`.

Built-in defaults encode the Stage A intent:

| Fold | `degraded_when` | `down_when` | Result |
|---|---|---|---|
| agents → zone | `all` | `all` | redundant agents must unanimously corroborate an outage |
| zones → region | `any` | `all` | one zone is partial failure; all zones is regional outage |
| regions → check | `any` | `majority` | one region is partial failure; a majority is outage |

A check may override each level. Otherwise the node's stored `rollup_policy`
row wins, then the built-in default. Policy edits and tombstones are HLC/LWW
config entries, replicated and reconciled through the config plane just like
checks, settings, sections, and credentials.

The candidate root does not publish immediately when a hold-down is configured.
`min_incident_secs` requires a non-UP candidate to remain continuous before the
published transition; `min_recovery_secs` does the same for recovery. Both
default to zero and can be set fleet-wide in `[coordinator.rollup_defaults]` or
overridden per check. Degraded transitions alert by default
(`notify_degraded = true`), while Degraded time counts as uptime by default
(`degraded_is_downtime = false`): partial reachability is actionable without
silently charging it as total downtime.

Every published root transition is appended to `state_history`. Uptime is
computed from those time-weighted state spans—not raw, agent-count-weighted
probe rows—so hold-down and the chosen Degraded interpretation match what the
status page actually showed. Zone and region transitions are also alert events
for topology history, but only check-root transitions drive check state,
incidents, and notification routing.

---

### 7. Alerts & Incidents

**Two objects, deliberately not one.**

- An **alert** is an *event*: one confirmed state transition, with a scope and a
  timestamp. It is what a notifier sends and what the retry queue keys on.
- An **incident** is a *record*: a titled, operator-editable span with affected
  checks and a narrative of updates. It is what the public status page shows.

Collapsing the two was considered and rejected. A single table needs
`previous_state`/`current_state` on a span covering many flaps, where the pair
is meaningless, and it hangs a title and an Updates list off every UP→DOWN
edge. Alerts stay the log; incidents own the story.

**An alert names its incident, stamped at write time.** The coordinator has
just decided which incident this transition belongs to — it opened it, or found
it via `depends_on` — so it records that, rather than leaving it to be guessed
later. Nothing is attached by hand: the DNS check falling writes both the alert
and the incident, already linked.

Reconstructing the link from a time window was drafted and thrown out. Note it
is not flapping that breaks it — a check bouncing forty times inside one
outage *should* hand all forty edges to that incident, and both designs do.
What breaks it is ambiguity it cannot resolve: a check covered by two
overlapping incidents is claimed by both, with no tiebreak, and adding a check
to the affects list retroactively rewrites which past events "belong" to the
incident — so an unrelated ping outage from that morning is silently absorbed.
Membership is a fact the writer knows and the reader cannot re-derive.

`started_at` stays editable and deliberately does **not** re-own anything.
Moving it back an hour says "the outage began before we detected it" — a
narrative correction to what the page tells visitors. It is not a claim that
machine events which happened before detection were caused by this incident.
The window is the story; the stamp is the evidence.

What exists today is neither model: `alerts` is a four-column transition log
with no id, and `incidents` is a one-check span whose only verb is Acknowledge.
Both are replaced outright — this is pre-release, so there is no migration and
no compatibility shim.

**Alert**
```rust
struct Alert {
    id: String,
    scope: AlertScope,              // check | agent | zone | region
    node_id: String,                // the check, agent or zone that moved
    previous_state: Option<Status>, // None for a first-ever observation
    current_state: Status,
    timestamp: i64,
    /// The incident this transition belongs to. Stamped by the coordinator
    /// when the alert is recorded; `None` for a transition that opened no
    /// incident (a recovery on a check nothing was tracking).
    incident_id: Option<String>,
}
```

`alert_type` from the original model is **not stored**. `StateChange` /
`Degradation` / `Recovery` is a pure function of the state pair, and a stored
copy is a column that can disagree with the two fields it summarises; it is
derived in the DTO. `scope` *is* stored — it is not derivable. The completed
Stage A region tree writes zone and region alerts as well as check-root alerts.

**Incident**
```rust
struct Incident {
    id: String,
    title: String,
    state: IncidentState,          // detected | investigating | identified | monitoring | resolved
    started_at: i64,               // editable; first confirmed DOWN when automatic
    resolved_at: Option<i64>,
    global: bool,                  // shown page-wide, not just on its checks
    origin_check: Option<String>,  // None when a human opened it
}
```

`state` is the **only** lifecycle field: "open" means `state != resolved`.
There is no second `status` column to drift out of sync with it, which is what
today's `open`/`resolved` pair would become the moment a human state existed
beside it. Duration is derived — `(resolved_at ?? now) - started_at` — never
stored.

**`detected` is the machine's state, and it is where every automatic incident
starts.** `investigating` claims a human is on it, which is a lie for the first
however-many minutes of every outage — and on a public page it is the
expensive kind of lie, because a visitor reads it as "someone is awake". A
probe noticing is not an investigation.

Rejected names: `alert` collides with the event object, `notification` claims a
message was sent (a check with `notify = false` opens an incident and pages
nobody), and `anomaly` undersells a hard DOWN.

| State | Means | Set by |
|---|---|---|
| `detected` | a probe saw it; nobody has looked | the coordinator, on auto-open |
| `investigating` | a human has it and is looking | acknowledging, or posting any update |
| `identified` | cause known | an operator |
| `monitoring` | fix applied, watching | an operator |
| `resolved` | over | an operator, or auto-resolve |

**Acknowledging is the `detected` → `investigating` edge**, not a flag beside
the state. That is why there are no `acknowledged_at` / `acknowledged_by`
columns: the ack *is* the first update a human wrote, so its timestamp and
author already exist on that row. Two columns holding a copy of the earliest
human update is precisely the drift this model keeps designing out. "Has anyone
looked?" is `EXISTS (SELECT 1 FROM incident_updates WHERE incident_id = ? AND
author IS NOT NULL)`, and it stays true if the operator jumped straight to
`identified`.

**Every state change is an update.** An incident's state is only ever moved by
posting an update, exactly as Kener does it: a body plus the state it announces.
That is one write path for the operator's "Add Update", for acknowledging, for
auto-open and for auto-resolve, so the timeline can never disagree with the
state.

| Trigger | Update written |
|---|---|
| Check confirms DOWN | `detected` — body is the failure summary, the probe's own words as a fallback. `author` NULL |
| Operator acknowledges | `investigating`, body optional. `author` set — this is what makes it an ack |
| Operator posts | whichever state they chose |
| Affected checks all read UP for `failures_before_down` observations | `resolved` — body states the total downtime. `author` NULL |

The auto-written updates are why the current `incidents.details` column
disappears: the cause is the first entry in the narrative, not a field beside it.

**Affects.** An incident covers a set of checks, and how each got there is
recorded, because the reconciler must never undo an operator's edit:

| `source` | Added by | Removed by |
|---|---|---|
| `origin` | the check whose DOWN opened the incident | nothing |
| `dependency` | the reconciler, via `depends_on` | the reconciler, when it recovers |
| `manual` | an operator in the editor | an operator |

**Dependent checks (`depends_on`).** A new per-check field naming upstream
checks. When check `C` confirms DOWN:

1. If any check in `C.depends_on` has an open incident, `C` joins **that**
   incident with `source = dependency`, and opens none of its own.
2. Otherwise `C` opens its own incident, with itself as `origin`.

The lookup is **one hop, not transitive**. `web → api → db` with all three down
gives two incidents, not one. Transitive resolution needs a cycle guard and a
tie-break for a check reachable from two roots, and one hop covers the case the
feature exists for: a fan-out of checks behind one dependency. It is also
cycle-safe by construction — `A depends_on B, B depends_on A` terminates,
whoever falls first wins the incident.

Chosen over host-matching, section-matching and time-window correlation because
those merge unrelated outages that happen to coincide, and an incident that
claims the wrong blast radius is worse on a status page than two honest ones.
The cost is that the graph has to be declared.

**Dependency grouping does not change who gets paged.** A dependent still
records its own alert and still notifies per its own routing. Suppressing the
downstream page is the obvious next move, but it is escalation policy, and
silently muting a check because someone edited an unrelated check's
`depends_on` is the wrong default to arrive at by accident.

**Auto-resolve waits out the flapping, and the check already says how long to
wait.** It fires when every `origin` and `dependency` check has read UP for
`failures_before_down` consecutive observations. No new setting: a check that
needs three failures before anyone believes it is down needs three successes
before anyone believes it is back. The number is per check and already tuned
per check, which a single global window could never be — a 10s HTTP probe and
a 5-minute cron want very different patience, and they each already declare it.

That the field is named for one direction is the cost of not adding a second
one. It reads as an asymmetry knob and is now a symmetry knob; the comment on
it says so.

The first UP is otherwise not enough, and that is the point: a check bouncing
DOWN/UP every thirty seconds would open and close twenty incidents in ten
minutes, spraying the page with noise and scattering one outage across twenty
permalinks. All forty edges belong inside **one** incident, which is what an
operator means by "DNS was flapping this morning".

At the default of `1` this is exactly today's behaviour — resolve on the first
UP — which is correct rather than a gap. A check configured to believe a single
failure is a check configured to believe a single success. Flap damping is
opt-in by raising the number, and raising it was always the answer to a flapping
check.

**Only incident resolution reads the up-side count; the check's own state does
not.** `current_state` still flips UP on the first good probe, so the status
page goes green immediately and uptime maths is untouched. Recovery staying
immediate is a deliberate decision recorded in Phase 3, and inverting it here
would silently change uptime percentages, notification timing and the public
grid. The visible consequence is a check reading green beneath an incident
still marked open, for `failures_before_down - 1` probes. That is accurate:
recovered is not the same as declared over.

No new counter is stored for this. The up-side run is read from
`check_results` — the last N rows for the check, all UP — so it is correct
across a restart with nothing to seed or resync.

**Auto-resolve always fires**, acknowledged or not. An operator mid-narrative
who wants the last word raises the check's `failures_before_down`, which is the
same knob and one they already understand.

`manual` attachments are narrative and do not gate resolution — an operator
naming a check they suspect must not be able to wedge an incident open forever.
UNKNOWN is not a recovery, as today.

Evaluation is lazy — on each ingest batch, only for incidents whose affected
checks appear in that batch — so there is no sweep timer. Consequence worth
knowing: an incident whose checks all stop reporting entirely (disabled, or the
last agent probing them retired) never auto-resolves. It stays open until an
operator resolves it, which is the honest outcome, since nothing can currently
attest that it recovered.

**Global incidents** render page-wide instead of against their checks, and a
global incident with no checks at all is the maintenance-notice case: a
manually opened incident that no probe will ever resolve.

That distinction is the flag's whole job, so the two placements are the two
surfaces an open incident gets on the public page, above the archive:

| | non-global | global |
|---|---|---|
| live surface | a marker on each affected monitor row, and the incident list on that check's detail page | a clickable notice stacked under the overall-status banner |
| Past Incidents | listed | listed, tagged `global` |

A global is deliberately **not** repeated on the rows of the checks it
affects — a page-wide notice plus the same title on eight monitors makes one
outage look like nine. A maintenance notice has no row to sit on at all, which
is the case the banner exists for.

**Past Incidents stays complete either way.** It is history, not a live
surface, and hiding a resolved outage from it because the outage was scoped to
one monitor would make the page less honest, not more. Both surfaces are driven
by the same websocket-fed list the archive reads, so an incident opening or
resolving appears and retracts without a reload.

The row marker is informational rather than a link: the row is already a button
to the check's detail page, where the incident is a link to its permalink, and
a button inside a button is invalid markup.

**Manual incidents** have `origin_check = None`, so the reconciler ignores them
entirely: nothing auto-attaches to them and nothing auto-resolves them.

**UI.** Admin lists Title / Duration / State / Affects / Edit. The editor
carries title, start time, the Global toggle, the affected-check list
(dependency rows marked as such and read-only) and the Updates timeline with
Add Update. The public page gets a per-incident permalink, `#/incident/<id>`,
rendering title, state, the update timeline and the affected monitors — the
updates are public, which is the entire point of writing them — plus the two
live surfaces above: the banner and the per-monitor marker both link into it.

**Deliberately not included**, each a column and a form field when wanted:
severity, an owner per incident, incident templates, subscriber notification on
update, and per-section notifier routing (already tracked separately).

**Deleting incidents.** `DELETE /api/v1/admin/incidents/:id` is admin-gated
and answers 404 for an id nothing recognises — a delete is a claim about a
specific record, and reporting success on a phantom one would be a lie the
caller has no way to catch.

`incident_tombstones` exists for the same reason `config_tombstones` does: an
absence is not a fact a cluster can rely on. A peer's next
`/internal/replicate` push, or the reconciler re-pushing an incident it still
holds locally, hands the deleted row straight back the moment nothing records
that the delete was on purpose — so `upsert_incident` checks
`incident_tombstoned` first, on every node including the one that issued the
delete, and refuses to write a tombstoned id. Tombstones are pruned on
`max(results_days, alerts_days)`, the longer of the two retention windows, for
the same reason config tombstones are: pruning them sooner than the data they
guard is kept would let a node that was down through the whole prune window
come back, find nothing, and hand the incident straight back to a cluster
that already forgot it.

**Linked alerts are unlinked, not deleted** — `UPDATE alerts SET incident_id
= NULL`, never a `DELETE FROM alerts`. An alert is the raw record that a
check flipped state; an incident is the narrative written around it.
Deleting the story must not erase the fact, or the alert feed and the uptime
figure start disagreeing about an outage that plainly happened.

**Deleting an OPEN incident is allowed.** The reconciler needs nothing but a
check still failing to open a fresh incident on the next confirmed DOWN — the
old id being gone changes nothing about what it decides. That is the honest
outcome, so the editor's confirm copy says as much rather than the server
second-guessing the operator by refusing on their behalf.

`WsEvent::IncidentDeleted { id }` carries the id alone: there is nothing left
on the row worth sending, and every consumer's whole job is to drop what it
already has. It fires from the local handler and from `apply_replicated`
alike, whenever a delete actually removed a row — a tombstone for an incident
this node never held is not news any dashboard can act on, so that case stays
silent.

**The check-level cascade, `DELETE /api/v1/admin/checks/:name/everything`, is
a third Danger-zone action, not a checkbox bolted onto the existing delete.**
It is the only destructive action here that removes a page anonymous visitors
have already read, and that consequence earns it its own button and its own
confirm copy rather than hiding behind a modifier on "delete check". It
removes the check itself through the same config-tombstone path a plain
delete uses, and on top of that erases every result the check ever produced,
every alert it wrote, and every incident it opened — each incident going
through `delete_incident` individually so each gets its own tombstone, rather
than a bulk delete that would leave the cluster nothing to converge on. An
incident the check merely joined (`dependency` or `manual`) is left standing:
only this check's row inside `incident_checks` goes, never the incident
another check still owns.

**Known limitation, documented rather than fixed: raw results still have no
tombstone.** Erasing a check while a peer is down removes the rows here, but
that peer's next anti-entropy sweep sees results it still holds and this node
no longer does, decides it is behind, and hands them straight back — the same
same class of anti-entropy hazard as any un-tombstoned replicated row. The
check's *definition* and its *incidents* do not come back, because both are
tombstoned; only the raw result rows can reappear, orphaned under an immutable
check UUID whose definition no longer exists.

---

### 8. Maintenance Windows

A window is a scheduled span during which a check's transitions **page
nobody**. Nothing else changes: the probe still runs, the result is still
recorded, the alert row is still written and an incident still opens. That is
exactly the existing per-check `notify = false` contract on a timer instead of
a flag, which is why it lands as one more arm in `notify_targets` rather than a
new suppression layer.

```rust
struct MaintenanceWindow {
    title: String,
    starts_at: i64,        // epoch millis, inclusive
    ends_at: i64,          // exclusive
    /// Checks it covers. Empty = every check — the "whole platform is down for
    /// twenty minutes" case, and the reason there is no separate `all` flag.
    checks: Vec<String>,
    /// Publish it on the status page while it runs.
    announce: bool,
}
```

Suppression is `starts_at <= t < ends_at` against the **transition's** own
timestamp, plus membership. Consequence worth naming: an outage that begins
inside a window and recovers after it ends pages the recovery having never
paged the failure. Left as is — the alternative is remembering per check what
was suppressed, which is state to persist and reconcile for a cosmetic gain.

**Not a pause on probing.** The grid, the latency chart and the uptime
percentage all keep showing what really happened, so "was it actually down for
the whole window?" stays answerable. Disabling the check is already the way to
stop probing, and it is the wrong thing to do for maintenance.

**Announcing is opt-in per window, and only while it runs.** An announced
window renders as a notice under the overall-status banner — the operator's own
title, and when it is over — because "we are upgrading the database" is not
derivable from a time span. A silent window suppresses the pages and tells
nobody, which is the right shape for work no visitor cares about.

The notice deliberately does **not** name the affected checks. A window naming
eight of them, above the eight monitor rows it covers, reads as eight separate
outages: the same argument §7 makes for keeping a global incident off the rows
it affects. It is also not clickable — a window has no updates and no
permalink, and that is exactly what separates it from an incident. An outage
someone wants to narrate is an incident; a window is a schedule.

Advance notice is out: a window booked for next quarter would sit on the page
all quarter, and bounding that means a lead-time setting nobody has asked for.
A notice before the work starts is a manually opened incident, as §7 describes.

**Stored, not configured.** One `settings` document (`maintenance.windows`),
like the notifier list and the agent display, and no `[[maintenance]]` block:
a window is booked at a moment and nobody redeploys to schedule one. The list
is replaced as a whole by the PUT, so there is no per-window id — the panel is
the only editor and it always sends everything, same argument as notifiers.

```
GET  /api/v1/admin/maintenance   → windows, plus the check names one can name
PUT  /api/v1/admin/maintenance   → replace the list
```

Both admin-gated: a window is a licence to go quiet, so an anonymous writer
could silence the whole fleet.

Deliberately out, each a field and a form row when wanted: recurrence (weekly
patching is the obvious first ask), suppressing incident creation as well,
advance notice before a window opens, notifying that one started, and
auto-deleting expired windows — an ended window is one line of history an
operator removes.

---

### 9. Clustering

A regional coordinator cluster (§2) needs three things a standalone coordinator
does not: to know who else is in it, to keep every node's DuckDB in sync, and
to notice when a peer stops answering. All three ride the HTTP server every
coordinator already runs — same axum port, same `peer_token`, same compose and
TLS story as agent traffic. No second protocol, no UDP port to add to the
compose files or a certificate.

**Two planes, replicated by different rules, and the difference is the whole
design.**

- **Data** — raw results, current state, alerts, incidents — is *observed*:
  exactly one coordinator's agents produced it, and that coordinator already
  decided what it means. A peer that receives replicated data persists it and
  stops; it does not re-evaluate, does not notify, and does not re-gossip it.
  This is the governing invariant, and it is what keeps a three-node cluster
  paging once for one outage instead of three times, and what keeps two
  coordinators from disagreeing about a check whose Lua eval is not
  deterministic — only the coordinator that actually ran it gets to decide.
- **Config** — checks, sections, settings, agent credentials, rollup policies — has no
  observer. No evaluation makes one edit more authoritative than another
  written on a different node a second later, so it is last-writer-wins on a
  per-row HLC stamp, and *a replicated config write takes effect locally* —
  that is the entire point of editing from any node.

The two planes share the transport — HTTP, `peer_token` — and nothing else.
Routing a config write down the data plane's replicate-and-persist path, or,
worse, evaluating and notifying on a replicated result the way a locally
observed one is evaluated, is the change most likely to quietly reintroduce
triple paging or a fleet that disagrees with itself. Keeping the split
explicit here is what stops someone "simplifying" it away later.

#### Node identity

Every coordinator has a `node_id`, stable across restarts: configured, or
generated once on first boot and persisted in `settings`, so the second boot
finds the same one rather than rejoining as a stranger and leaving a ghost
member behind. It also breaks HLC ties (below), so it must be unique.

`generation` is the process's own start timestamp, exchanged on every ping. A
restarted node presents a new generation, which is what lets a node's own
re-announcement beat old, still-in-flight gossip about its previous process —
otherwise a returning node would flap between `alive` and `dead` as stale
views kept arriving from other peers.

#### Membership: `POST /internal/cluster/ping`

Membership is push-pull, one round trip: the sender presents its own identity
plus its whole member view, and the receiver answers with its identity plus
its own view.

```rust
struct Member {
    node_id: String,
    url: String,       // advertise_url
    generation: i64,
    last_seen: i64,
    state: String,      // alive | suspect | dead
}

struct PingEnvelope {
    from: Member,       // the sender, as it sees itself
    view: Vec<Member>,  // everyone the sender currently knows about
}
// response is a PingEnvelope too, `from` the receiver's own identity
```

Both sides merge the other's view into their own, entry by entry: for a given
`node_id`, the higher `generation` wins outright, and within a generation the
newer `last_seen` wins. One ping therefore does three things at once —
announces the sender, refreshes both sides' liveness clock for it, and
exchanges everyone else either side has heard of.

**`peers` is a seed list, not the membership.** It names who to ping *first*;
the running member set is whatever `cluster_members` holds, persisted so a
restart rejoins from what it already knew instead of waiting to be re-seeded
by whichever peer happens to ping it first. Transitive discovery falls out of
the merge for free: a node seeded only with B learns C the first time it pings
B and receives C in the returned view, with no separate discovery round. One
reachable seed is therefore enough to join a cluster of any size, and a seed
that is down once the cluster has formed costs nothing — the running members
already know each other.

#### Failure detection

```
alive --(silent for peer_timeout_secs)--> suspect --(silent for another peer_timeout_secs)--> dead
  ^                                                                                              |
  └────────────────────────────── any successful ping ─────────────────────────────────────────┘
```

| State | Meaning |
|---|---|
| `alive` | pinged successfully within `peer_timeout_secs` |
| `suspect` | silent for one `peer_timeout_secs`; still pinged, not yet excluded |
| `dead` | silent for two `peer_timeout_secs` |

Dead members are **retained, not dropped**. Losing the row would make a
returning node look new instead of recognised, and a `node_id` that reappears
with a low `generation` is exactly the signal that tells a network blip apart
from a process restart.

**Deliberately no notification on a peer's death.** A WARN log, a row in the
admin UI, and a `ClusterChange` websocket event — nothing pages. This is a
choice, not a gap: a coordinator alerting about the health of its own cluster
is a different product surface from a coordinator alerting about the things it
monitors, and the obvious failure mode is worse than silence — the node whose
death would justify the page may be the one that just died, and cannot send
it. An operator watching the fleet is the backstop.

**Heartbeat fan-out is unsubsetted.** Every alive member pings every other
alive member on each `gossip_interval_secs` tick — no SWIM-style indirect
probing, no gossiping to a random subset. At 3-5 nodes that is at most twenty
pings a cycle; subsetting exists to keep an O(N²) heartbeat affordable at
cluster sizes this project does not have, and the code it would take is not
worth carrying for a fan-out this small.

#### Replicating the data plane: `POST /internal/replicate`

```
POST /internal/replicate
  { results: [CheckResult], beliefs: [ReplicatedBelief], polls: [AgentPollRow],
    alerts: [Alert], incidents: [Incident], deleted_incidents: [String] }
  → whatever the observing coordinator just wrote locally, replayed verbatim.
    Receiver persists it and stops: no re-evaluation, no notification, no
    further replication.
```

**Leaves replicate, not just conclusions.** `beliefs` carries the observing
node's per-agent tracker entries, because a zone's agents are its corroboration
set and they do not all report to the same coordinator — the deployment
topology is a coordinator cluster per region serving every agent in that
region, not one coordinator per agent. Replicating only the folded conclusion
left each node holding a partial tree: a two-agent zone read as two separate
one-agent zones, so `all/all` was satisfied by a single flaky agent and the
corroboration property was unreachable in any real cluster.

A belief is the tracker's *confirmed* status plus its `consecutive_failures`,
never a raw probe verdict — the `failures_before_down` run belongs to the
observer and a peer must not restart it. Application is keyed on `last_check`
(`StateTracker::merge_agent`): an older or equal observation loses. That is what
makes it order-independent and replay-safe, which matters because
`/internal/replicate` has no ordering guarantee and anti-entropy replays
historical pages. A rewound leaf would be an outage the cluster invented rather
than observed.

`check_results` has no unique constraint — it never needed one for a single
writer — so a replicated insert cannot rely on a primary key to no-op a
duplicate. Instead it is an anti-join on `(check_id, agent_id, ts_millis)`:
insert only the rows the receiver does not already have. That makes
replication **replay-safe** — sending the same batch twice is a no-op — which
is exactly the property anti-entropy (below) depends on to backfill without
double-counting.

#### Replicating the config plane: `POST /internal/config`

```
POST /internal/config
  { checks / sections / settings / credentials / rollup policies: [(row, Hlc)],
    tombstones: [(kind, id, Hlc)] }
  → per-row LWW merge against the local (updated_at, updated_by) stamp;
    whichever value wins is applied — including this write, if it wins.
```

Rollup policies are first-class config entries (`kind = rollup_policy`), so node
policy edits and deletes converge through both push replication and anti-entropy;
they are not node-local UI preferences.

The merge rule is the HLC's own `Ord`: higher `millis` wins, ties break on
`node_id`, so every node picks the same winner without a message about it. A
node whose wall clock is slow still produces increasing stamps — every local
stamp is `max(wall clock, last stamp + 1)` — and every stamp it receives,
local or replicated, is observed, so a lagging clock can never leave a node
stuck losing every merge it takes part in.

**Deletes are tombstones**, in `config_tombstones` (`kind`, `id`, `deleted_at`,
`node_id`), not row removal — a bare delete leaves nothing for anti-entropy to
compare against, so a peer that missed it sees a check it does not have and
helpfully hands it straight back. Tombstones are pruned on the *retention*
schedule rather than a fixed constant, on purpose: pruning them sooner than
the data they guard is kept would let a node that was down through the whole
prune window come back, find no tombstone, and resurrect a check every other
node already forgot.

#### Anti-entropy

Replication above is fire-and-forget: a peer unreachable when a write happens
misses it, with nothing retried. Anti-entropy is the backstop, run every
`anti_entropy_secs` against one random alive peer:

```
GET /internal/digest
  → { node_id,
      checks: [(check_id, min_ts, max_ts, count)],  // from a maintained
                                                      // watermark, not a raw scan
      alerts / incidents / incident_tombstones:
        { max_millis, max_id, count },                // append-only stream digests
      latency_minute_rollups / latency_hour_rollups:
        { max_bucket_start, count, attempts_sum, up_sum, latency_sum_sum },
      config: [(kind, max_hlc, count)] }              // one row per config kind

GET /internal/backfill/{results,alerts,incidents,incident-tombstones,
                        latency-minute,latency-hour}?since=&limit=
  → one page for that stream; the response's tail is the cursor for the next page

GET /internal/config?kind=&after=&limit=
  → entries of that kind, tombstones included; paged, 1,000 per page
```

A node compares the digest to its own and asks for exactly the streams and
ranges it is behind on, not a full resync. Each stream backfills from its own
paged, cursored endpoint rather than one streamed dump: DuckDB is
single-writer, and a long backfill held under the storage mutex would starve
the ingest path it shares it with. Every sweep spends a small, fixed page
budget shared across results and the alert/incident/tombstone streams, so one
very-behind check or stream cannot stall the others; rollup catch-up gets
whatever pages that leaves, since `derived_results_sql` already keeps rollups
converged as a side effect of ordinary result replication and this path only
matters once a peer is behind raw retention. A raw-result gap that reaches
back past the peer's own retention horizon cannot be repaired by anti-entropy
at all; it is recorded durably as a `raw_data_gaps` row (`GET
/api/v1/admin/raw-data-gaps`) rather than retried forever. The minute/hour
latency rollups replicate on their own digest/backfill pair too, so a peer
that lost the raw window can still recover the *summarized* latency history
for it even though the individual probes are gone. See `storage::RollupDigest`
and `replication::ANTI_ENTROPY_PAGES_PER_SWEEP`.

The config plane is reconciled in the same sweep but by a different rule. A
kind is pulled when the peer's `max_hlc` is ahead of ours **or** the counts
disagree. The count half is not redundant: config rows are updated in place,
so a peer can hold a row we never received while our own `max_hlc` is higher
because of some unrelated row — a high-water mark alone would call that pair
converged forever. The pull is paged (1,000 entries per request) but applied
per page rather than deferred until the walk completes, because config is
bounded by how many checks an operator has written rather than by uptime, and
a partially applied config is worse than a slow one — and safely so, since
each page is independently idempotent. Over-pulling is harmless: every entry
goes through the same last-writer-wins `apply_config_entry` the push path
uses, so an entry older than what is already stored loses, and a redundant
pull converges on identical state.

#### Admin sessions

A consequence of clustering worth stating here, since it is a behaviour change
and not just an implementation detail: admin sessions become stateless
HMAC-signed tokens instead of an in-memory table, so any node can validate a
session a different node created. The signing key derives from `peer_token`
when one is set — identical on every node by construction, stable across a
restart — or a random key persisted in `settings` when it is not.
**A coordinator restart no longer logs everyone out**, which it does today.
Logout keeps a small in-memory revocation set until the token's own expiry,
pushed to peers best-effort; the worst case of a dropped push is a logged-out
token staying valid on one *other* node until its TTL, which bounds the
exposure to the session lifetime rather than leaving it open-ended.

#### Startup validation

`peers` non-empty with `peer_token` or `advertise_url` unset fails coordinator
startup rather than starting half-configured. Both failure modes are silent
otherwise: no `peer_token` means every ping is rejected with 401 at the
*receiver*, and no `advertise_url` means this node discovers peers that have
no address to dial it back on. Either looks exactly like a network problem for
as long as it takes someone to read the logs on the other machine — refusing
to start is cheaper than that hour.

#### Fleet liveness across nodes

`Inner::seen` is node-local, written only by `agent_checks_handler` on
whichever node an agent happens to be polling. That was the entire fleet view
before this landed: in a three-node cluster, one node reported only the agents
polling it as healthy while another showed the mirror image. Each node was
completely confident and wrong about its own cluster. Admin > Agents
told a third story, healthy everywhere, because it derives from replicated
*results* over a rolling five-minute window rather than from `seen` at all —
not the same signal as a poll heartbeat, just one that happens to also claim
to answer "is this agent alive". Two liveness clocks, disagreeing, and only
one of them ever left the node that kept it.

`agent_polls` is the persisted twin of `seen`: agent id, zone, region, when it
last polled, the window it declared, its RTT — a row instead of a map entry,
so it can outlive the process and cross the wire to a peer.

`Coordinator::spawn_fleet_sync` is what moves it. Every `gossip_interval_secs`
tick it snapshots `seen`, writes the snapshot into `agent_polls`, and — if
this node is clustered and has a live peer — broadcasts it to
`/internal/replicate` as `ReplicationBatch::polls`. It runs on **every**
coordinator, not only a clustered one, because writing `seen` down is also
what carries the fleet across a restart on a single node; clustering only adds
the broadcast half. A snapshot per tick rather than a message per poll is the
deliberate shape: traffic is bounded by peers × tick, not by how many agents
the fleet has, so a five-hundred-agent fleet costs a cluster the same wire
traffic as a five-agent one.

The write is a **merge**, not a blind replace: `record_agent_poll` only
overwrites a row when the incoming `at` is strictly newer than what is
stored. That is what makes applying a peer's batch idempotent — the same
snapshot landing twice is a no-op — and order-independent — a stale batch
arriving late behind a fresher one cannot un-see a live agent. A reader (the
status page, Admin > Agents, the fleet picker) takes the best of both sources
at once: local `seen` for whoever is polling this node right now, `agent_polls`
for everyone a peer has reported, the healthier reading winning where both
exist.

`window_ms` travels with the row rather than being reapplied by the reader,
because it is the poll cadence the agent declared to *whichever* node it
polled — not this node's. A peer judging that row against its own
`missed_polls` setting would read an agent polling every two minutes as
unreachable purely because this node's own window happens to be shorter.
Liveness is `now - at <= window_ms`, computed with the window the row already
carries, not the one the reader would have chosen.

**Placement consumes the cluster-wide fresh union.** `agent_checks_handler`
combines local `seen` with replicated `agent_polls` before assigning checks, so
an agent failing over to another coordinator remains eligible everywhere. This
makes the poll input cluster-wide rather than display-only: every coordinator
makes the same placement decision from the same fresh fleet, while local `seen`
wins ties at an equal or newer timestamp.

`agent_polls` is the one table this project recreates rather than alters. It
shipped with `window_ms` missing from the column list and, worse, with
nothing writing to it at all — there was no row anywhere to migrate, so
`Storage::open` drops and recreates the table instead of running an `ALTER`.
That drop is conditional, keyed on the column being absent: an unconditional
`DROP TABLE IF EXISTS` in the DDL would empty the fleet view on every restart,
which is the exact problem persisting `seen` here was supposed to solve. It is
this project's no-migrations convention applied honestly — recreated because
there was truly nothing stored to lose, and never again once there is.

---

### 10. Data Models

**Check Result (Agent → Coordinator):**
```rust
#[derive(Serialize, Deserialize)]
struct CheckResult {
    check_id: Uuid,        // immutable UUIDv7 identity
    agent_id: String,
    zone: String,
    region: String,
    timestamp: DateTime<Utc>,
    status: CheckStatus,
    latency_ms: Option<u64>,
    details: Option<String>,
}

#[derive(Serialize, Deserialize)]
enum CheckStatus {
    Up,
    Down { reason: String },
    Timeout,
    Error { message: String },
}
```

**Alert:**
```rust
struct Alert {
    id: String,
    scope: AlertScope,              // check | agent | zone | region
    node_id: String,
    previous_state: Option<NodeState>,
    current_state: NodeState,
    timestamp: i64,
    incident_id: Option<String>,
}
```

The event kind is derived from the state pair rather than stored. Stage A emits
check, zone, and region transitions; global scope remains Stage B work.

---

### 11. DuckDB Schema

DuckDB is used for check result history and analytics. Its columnar storage provides efficient compression and fast aggregation queries.

```sql
-- Monitors/checks configuration
CREATE TABLE monitors (
    id UUID PRIMARY KEY,              -- immutable UUIDv7 identity
    name VARCHAR NOT NULL UNIQUE,      -- mutable Unicode display name,
    type VARCHAR NOT NULL,  -- http, tcp, dns, icmp
    target VARCHAR NOT NULL,
    interval_secs INTEGER NOT NULL DEFAULT 60,
    timeout_secs INTEGER NOT NULL DEFAULT 10,
    config JSON,  -- type-specific config (including eval script)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Per-agent confirmed leaves. Credential-authenticated zone/region are stored
-- with the belief so a restart reconstructs the same tree.
CREATE TABLE agent_check_state (
    check_id UUID NOT NULL,
    agent_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    consecutive_failures INTEGER NOT NULL,
    zone VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    last_check BIGINT NOT NULL,
    PRIMARY KEY (check_id, agent_id)
);

-- Per-node two-threshold policy. Node ids are `global`, `region:<region>`, or
-- `zone:<region>/<zone>`; quorum counts live in `config`.
CREATE TABLE rollup_policy (
    node_id VARCHAR PRIMARY KEY,
    node_type VARCHAR NOT NULL,
    degraded_when VARCHAR NOT NULL,
    down_when VARCHAR NOT NULL,
    config JSON,
    updated_at BIGINT NOT NULL,
    updated_by VARCHAR
);

-- Check results history (main time-series table)
CREATE TABLE check_results (
    check_id UUID NOT NULL,
    agent_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,  -- up, down, timeout, error
    latency_ms INTEGER,
    details VARCHAR,
    timestamp TIMESTAMP NOT NULL
);

-- Current state (materialized for fast reads)
CREATE TABLE current_state (
    check_id UUID PRIMARY KEY,
    state VARCHAR NOT NULL,  -- up, down, degraded, unknown
    pending_state VARCHAR,   -- hold-down candidate, if any
    pending_since BIGINT,
    last_check BIGINT NOT NULL
);

-- Published check-root spans used for time-weighted uptime.
CREATE TABLE state_history (
    check_id UUID NOT NULL,
    state VARCHAR NOT NULL,
    since BIGINT NOT NULL,
    PRIMARY KEY (check_id, since)
);

-- Alert log: one row per confirmed state transition. `incident_id` is stamped
-- by the coordinator as the row is written — membership is a fact the writer
-- knows, not something a reader can re-derive from a time window. NULL when
-- the transition opened no incident. `alert_type` is deliberately absent: it
-- is a function of the two state columns, so storing it invites drift.
CREATE TABLE alerts (
    id VARCHAR PRIMARY KEY,
    scope VARCHAR NOT NULL,          -- check, agent, zone, region
    node_id VARCHAR NOT NULL,
    previous_state VARCHAR,          -- NULL for a first-ever observation
    current_state VARCHAR NOT NULL,
    ts_millis BIGINT NOT NULL,
    incident_id VARCHAR
);

-- Operator-facing outage record. `state` is the only lifecycle field; open
-- means state <> 'resolved'. Duration is derived, never stored, and so is
-- "acknowledged": it is the existence of an incident_updates row with an
-- author, not a column here.
CREATE TABLE incidents (
    id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    state VARCHAR NOT NULL,          -- detected, investigating, identified, monitoring, resolved
    started_at BIGINT NOT NULL,
    resolved_at BIGINT,
    global BOOLEAN NOT NULL DEFAULT FALSE,
    origin_check UUID                -- NULL for a manually opened incident
);

-- Which checks an incident affects, and how each was attached. `source`
-- exists so reconciliation can retract what it added without touching what an
-- operator added by hand.
CREATE TABLE incident_checks (
    incident_id VARCHAR NOT NULL,
    check_id UUID NOT NULL,
    source VARCHAR NOT NULL,         -- origin, dependency, manual
    PRIMARY KEY (incident_id, check_id)
);

-- The narrative. Every state change is one of these, including the two the
-- coordinator writes itself on auto-open and auto-resolve.
CREATE TABLE incident_updates (
    id VARCHAR PRIMARY KEY,
    incident_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,          -- the state this update announced
    body VARCHAR NOT NULL,
    author VARCHAR,                  -- NULL when the coordinator wrote it
    created_at BIGINT NOT NULL
);

-- Notifications that have not been delivered. A row exists *because* a send
-- failed; success deletes it. It carries the transition to re-send rather than
-- an alert id: a retry needs the check and its states, nothing else, and a
-- foreign key into a table with its own retention window would dangle. No
-- surrogate id either — one transition enqueues at most once per notifier, so
-- (notifier, check, queued_at) is already unique.
--
-- `next_attempt` NULL means given up: attempts exhausted, nobody paged. There
-- is no `status` column, which could only ever disagree with `attempts`.
CREATE TABLE notification_queue (
    notifier       VARCHAR NOT NULL,   -- the routing name, resolved per attempt
    check_id       UUID    NOT NULL,
    previous_state VARCHAR,
    current_state  VARCHAR NOT NULL,
    attempts       INTEGER NOT NULL,   -- failed attempts, the first included
    next_attempt   BIGINT,             -- NULL = given up
    last_error     VARCHAR NOT NULL,
    queued_at      BIGINT  NOT NULL,
    PRIMARY KEY (notifier, check_id, queued_at)
);
```

**Example Queries:**

```sql
-- Average latency over last hour
SELECT avg(latency_ms), percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95
FROM check_results
WHERE check_id = '018f4e30-0000-7000-8000-000000000001'::UUID
  AND timestamp > now() - INTERVAL '1 hour';

-- Uptime percentage over last 24h
SELECT 
    check_id,
    count(*) FILTER (WHERE status = 'up') * 100.0 / count(*) as uptime_pct
FROM check_results
WHERE timestamp > now() - INTERVAL '24 hours'
GROUP BY check_id;

-- Latency histogram buckets
SELECT 
    width_bucket(latency_ms, 0, 1000, 10) as bucket,
    count(*) as count
FROM check_results
WHERE check_id = '018f4e30-0000-7000-8000-000000000001'::UUID
  AND timestamp > now() - INTERVAL '1 hour'
GROUP BY bucket
ORDER BY bucket;
```

---

## API Design

### Agent → Coordinator

Heartbeat checks are invoked at `GET` or `POST /api/v1/heartbeat/{check_id}`, where
`check_id` is the immutable UUIDv7 (for example,
`/api/v1/heartbeat/018f4e30-0000-7000-8000-000000000001`). Renaming the display
name does not change this URL.

```
POST /api/v1/results
Content-Type: application/json
X-Agent-ID: us-east-1a-agent-01
X-Agent-Secret: <shared-secret>

{
  "results": [
    {
      "check_id": "018f4e30-0000-7000-8000-000000000001",
      "timestamp": "2024-01-15T10:30:00Z",
      "status": "up",
      "latency_ms": 45
    },
    {
      "check_id": "018f4e30-0000-7000-8000-000000000002",
      "timestamp": "2024-01-15T10:30:05Z",
      "status": "down",
      "details": "Connection refused"
    }
  ]
}

Agent results identify checks only by UUID; coordinator/UI responses add the
current display name where a human reads the result.

Response: 200 OK
{
  "accepted": 2,
  "config_version": "abc123"  // For config sync
}
```

### UI/External API

```
GET /api/v1/status
→ Current state tree

GET /api/v1/monitors
→ List all monitors; each response carries UUID `check_id` and display `name`

GET /api/v1/monitors/{id}/history?from=&to=&limit=
→ Check history

GET /api/v1/alerts?limit=
→ Transition log, newest first

GET /api/v1/incidents?status=open|resolved&limit=
→ Incident list with affected checks (public)

GET /api/v1/incidents/{id}
→ One incident with its full update timeline (public)

POST /api/v1/admin/incidents
→ Open one by hand: title, start time, global, affected checks

PATCH /api/v1/admin/incidents/{id}
→ Edit title, start time, global

PUT /api/v1/admin/incidents/{id}/checks
→ Replace the manual affected-check set

POST /api/v1/admin/incidents/{id}/updates
→ Add an update: state + optional body. The only way an incident changes
  state, and the only way it gets acknowledged — an authored update on a
  `detected` incident IS the ack. There is no separate acknowledge endpoint;
  the Ack button posts `{state: "investigating"}` with an empty body.

POST /api/v1/admin/checks
PUT  /api/v1/admin/checks/{check_id}
DELETE /api/v1/admin/checks/{check_id}
→ Authenticated check lifecycle. IDs are UUIDv7; names are mutable Unicode
  display values. The global control plane mints one UUID before distributing a
  global check, so every region retains the same identity.

GET  /api/v1/admin/maintenance
PUT  /api/v1/admin/maintenance
→ Scheduled windows. Admin-gated both ways: a window is a licence to go quiet.
  Announced windows running now ride `/api/v1/status` for anonymous readers.

GET /api/v1/regions
→ Region tree structure

WebSocket /api/v1/ws/events
→ Real-time state updates
```

---

## Configuration

### Coordinator Config (TOML)

```toml
[coordinator]
listen = "0.0.0.0:8080"
db_path = "/var/lib/warden/warden.duckdb"
# admin_password = "..."  # Prefer WARDEN_ADMIN_PASSWORD.
# peer_token = "..."      # Prefer WARDEN_PEER_TOKEN.
# peers = ["http://coordinator-b:8080", "http://coordinator-c:8080"]

[coordinator.rollup_defaults]
min_incident_secs = 60
min_recovery_secs = 30
notify_degraded = true
degraded_is_downtime = false
```

Runtime resources are not sourced from this file. Checks are created only via
the authenticated Admin UI/API and persisted in DuckDB. Agent credentials are
minted or provisioned through the credential control plane, and notifications,
maintenance windows, sections, and display settings become store-authoritative
after their supported initialization path. See `warden.example.toml` for the
complete deployment configuration.
---

## Scaling

### Horizontal Scaling

| Component | How to scale | Limit |
|-----------|--------------|-------|
| Agents | Add more, anywhere | Unlimited |
| Regional coordinators | Add nodes (3-5 per region) | Gossip mesh ~10 nodes |
| Global coordinators | Add nodes (1-3) | Same |
| Regions | Add new regions | Unlimited |

### Write Throughput

| Scale | Checks | Interval | Writes/sec | Nodes |
|-------|--------|----------|------------|-------|
| Small | 100 | 30s | 3 | 1 (standalone) |
| Medium | 1,000 | 10s | 100 | 3 regional |
| Large | 5,000 | 10s | 500 | 3-5 regional |

At 500 writes/sec with 5 nodes, internal replication = 2,000 messages/sec. Trivial for modern networking.

### Storage

The estimates above are not a claim of 10,000-check readiness. **[SCALING.md](SCALING.md)** is the source of truth for the phased 10,000-check scale plan, including scheduler spreading, bounded concurrency, storage, and protocol work; none of that planned scaling work should be inferred as implemented here.

Full replication means 3-5x storage per region. For check results:
- 1,000 checks × 10s interval × 30 days = ~260M rows
- DuckDB with compression: ~5-10 GB per node
- 5 nodes = 25-50 GB total

Acceptable trade-off for query simplicity.

---

## Implementation Roadmap

### Phase 1: MVP (Uptime Kuma Replacement)
- [x] Agent: HTTP check execution
- [x] Agent: Result batching and posting
- [x] Coordinator: HTTP endpoint for results
- [x] Coordinator: DuckDB persistence
- [x] Coordinator: Basic state tracking
- [x] CLI: `--standalone` mode
- [x] Basic alerting (webhook)

### Phase 2: UI
- [x] Status dashboard
  - Check detail closes with a **probe grid** (Kener's squares) rather than a
    paged log of rows: one square per recorded probe, oldest to newest, hover
    for timestamp, latency and failure reason. Deliberately *per probe*,
    not per minute — a fixed one-minute cell would render a 5-minute check as
    80% empty squares implying gaps that never existed, and two agents probing
    the same minute would have to be collapsed into one cell, hiding a
    single-region failure. The panel says so, and claims a probe count rather
    than a span of time. Bucketed-by-time squares are the strip above it, which
    already exists.
  - The grid is **one labelled block per agent**, 60 squares to a row, each
    agent's own newest probes trimmed to whole rows (capped at 4 rows, so one
    chatty agent cannot stretch the panel). Wrapping one chronological grid was
    the first cut and is wrong twice: it left a ragged part-row hanging off the
    bottom, and with ten agents interleaved a single probe location's outage
    reads as everything flapping instead of one row going red. Rows are
    therefore *not* a shared time axis — hence the per-agent label and the note
    saying so. Squares are `1fr` grid tracks with `aspect-ratio: 1`, so they
    narrow on a small viewport rather than reflowing into a short final row.
- [x] Monitor configuration — **Admin > Checks**: create, edit, enable/disable,
      reorder, per-check schedule, expectations, agents, routing and Lua.
- [ ] Alert management
- [x] Real-time updates (WebSocket)
- [x] Incidents: full status-page model — `detected` → investigating →
      identified → monitoring → resolved, moved only by posting an update;
      editable title/start/global; multi-check blast radius with `depends_on`
      grouping; manual creation; public permalink. Replaced the original
      one-check span outright. See **§7 Alerts & Incidents**.
- [x] Alerts: built out to the documented model — `id`, `scope`, previous/current
      state, and the `incident_id` stamped at write time. `alert_type` is
      derived in the DTO rather than stored.

### Phase 3: Check Types & Eval
- [x] TCP checks
- [x] DNS checks — FQDN-forced, caching disabled, UDP retry + TCP fallback
- [x] ICMP ping — requires `CAP_NET_RAW`
- [x] Declarative `expect` (status code, body/answer substring)
- [x] Lua eval for custom check logic — sandboxed in `src/script.rs` (pure
      libraries only, instruction-budget hook, fresh VM per evaluation), run on
      the coordinator, syntax-checked on save, with a dry-run endpoint
      (`POST /api/v1/admin/eval-test`) so a script is falsifiable before it is
      trusted. The agent sends a body/headers only when a script exists.

Deferred from Phase 3, tracked here so it isn't lost:
- [x] Confirmation threshold before DOWN (`failures_before_down`) — a check
      flips DOWN only after N consecutive failed probes; recovery stays immediate
- [x] A check whose first-ever observation is DOWN does not alert
      (`StateTracker` treats the first observation as a baseline)
- [x] Agent result batches are retained and retried when the coordinator POST
      fails — bounded buffer, oldest dropped on overflow
- [x] `zone` / `region` on `CheckResult` — stamped by the agent, since only it
      knows where it ran; absent on results from older agents rather than guessed

### Phase 4: Notifications & Status Page
- [x] Slack, Email, PagerDuty integrations — three more `NotifierKind`s beside
      webhook and ntfy, sharing the same routing, test-send and per-check
      `notify` opt-out. Slack is separate from the generic webhook because it
      needs a `text` payload; PagerDuty dedups on the check id so a recovery
      resolves the incident the outage opened; email declares its TLS mode
      instead of having it guessed from the port. See **§5 Notifier**.
- [x] Retry queue with persistence — a failed send is queued in DuckDB and
      retried with exponential backoff (six attempts, ~32 min), surviving a
      restart; success deletes the row and exhausted attempts leave it as the
      record that nobody was paged, surfaced in **Admin > Notifications**. The
      queued row names its notifier, so fixing a wrong URL fixes what is queued
      for it. See **§5 Notifier**.
- [x] Notifier management — the backends existed from the start but were
      config-file only, read **once** into a spawned dispatcher, with no
      dispatcher at all when the list was empty: the first notifier added
      anywhere but the file could never fire. Now a stored setting like the
      checks are, edited in **Admin > Notifications**, with the dispatcher
      re-reading the list per alert so a save takes effect immediately.
  - Test-send (`POST /api/v1/admin/notifiers/test`) sends one recovery-shaped
      notification to a notifier that need not be saved yet. It exists because
      delivery used to be unfalsifiable: a wrong ntfy topic answered 404 and
      nothing looked at the status code. A non-2xx is now a failure everywhere,
      including in the dispatcher's log.
  - Per-check opt-out (`notify`, default on): some checks are just checks. The
      transition is still confirmed, the alert row still written and the
      incident still opened — only the notification is withheld.
  - Routing: a notifier has a unique `name` and a `default` flag; a check
      carries `notifiers = [names]`. Empty means the defaults, so nothing needs
      routing until two audiences exist; naming one or more means exactly those.
      A name nothing answers to reaches nobody rather than falling back to the
      defaults — a silent fallback pages the wrong people — and the notifier
      panel lists the checks it orphaned, the way the agents panel lists checks
      no live agent probes. Chosen over a selector on each notifier because the
      ask is per check; a section-level default is the next step, not a
      different model.
- [x] Public status page (Kener replacement) — incident list plus a
      per-incident permalink (`#/incident/<id>`) rendering the update timeline
      and affected monitors to anonymous visitors, with dependency-attached
      monitors marked as such. Writes stay admin-gated; reads are public.
  - Open incidents also render live, not only in the archive: a global one as a
    clickable notice under the overall-status banner, a scoped one as a marker
    on each affected monitor row plus an incident list on the check's detail
    page. Both follow the websocket, so they appear and retract without a
    reload. Past Incidents keeps listing everything — it is history.
- [x] Agent health on status page — liveness, not check health: a responding
      agent is healthy even if every check it probes is down. Three states
      (healthy / warning / unhealthy) from silence thresholds; region rows
      aggregate % healthy with configurable color thresholds; public page shows
      regions only, per-agent detail stays admin-only.
  - Health clock: no new storage, as decided. The poll heartbeat lives in the
    in-memory `seen` map (`coord.inner.seen`), seeded from persisted results at
    boot and updated by every poll. Classification: healthy = polling inside
    its own window (so an idle zero-check agent reads healthy); warning = not
    polling but seen within 7d; unhealthy = retired (≥ 7d). Results alone never
    read healthy — a five-minute cron says nothing about the last four minutes.
    Persistence is done: `agent_polls` is the persisted, replicated twin of
    `seen`, so a clustered coordinator's fleet view is no longer scoped to the
    agents polling it. See §9, "Fleet liveness across nodes".
  - Config: `[coordinator.status_page.agents]` — `rows` (ordered entries, each
    `{kind = "agent"|"region", name}`) and `mode` (`regions` | `flat` | `off`)
    for whatever the rows do not cover, plus `warn_below` / `down_below`
    (region % thresholds). `rows` replaces the original `pinned` list, which
    could only prepend agents: "show this box, then the whole us-east region"
    is one ordered list of mixed entries, not a mode. A region row counts every
    agent in it, including one shown individually above — a row labelled
    `us-east` that quietly omitted a machine would be a lie. `off` still shows
    placed rows; `off` with none is how the section disappears, so `show` was
    never needed. Display is deterministic: rows in configured order, the
    remainder sorted by name. Legacy `pinned` documents are folded into `rows`.
    - Edited in **Admin > Status page**, not the config file. The block above is a
      first-boot settings seed; unlike checks, it has no `[[checks]]` counterpart.
      After that the store is authoritative (a `settings` table holding one JSON document per key) and
    a restart cannot revert an operator's choice. `GET`/`PUT
    /api/v1/admin/status-page/agents` are both admin-gated, the PUT validates
    (0–100, `down_below ≤ warn_below`) and dedupes `rows` — a repeated entry
    would render two rows with the same key, which hangs the page rather than
    looking wrong; identity is the (kind, name) pair, so a region and an agent
    may share a name. The settings response carries the fleet the rows can
    name, so the picker is one request.
  - Coupling fixed as designed: the window is `missed_polls × poll_secs`
    (`missed_polls = 3` on the coordinator, `poll_secs` declared by the agent on
    every poll and clamped to 1s–1h), so detection is three missed cycles at any
    cadence and the old hardcoded 90s is gone. An agent too old to declare a
    cadence gets the default one.
  - Public homepage no longer 401s: region health rides `/api/v1/status`, the
    anonymous page never calls `/api/v1/agents`, and that endpoint stays
    admin-gated.
- [x] Maintenance windows — a scheduled span during which the checks it covers
      page nobody, edited in **Admin > Maintenance**. Probing, results, alerts
      and incidents all continue: it is the per-check `notify` switch on a
      timer, not a pause, so the grid and the uptime figure still show what
      happened. Empty check list means the whole fleet; `announce` publishes the
      operator's title under the overall-status banner while the window runs.
      Stored as one `settings` document with no config-file counterpart. See
      **§8 Maintenance Windows**.

### Phase 5: Regional Clustering and Stage A Region Tree
- [x] Gossip membership — push-pull over the same HTTP port as everything
      else, `POST /internal/cluster/ping`: `peers` is only a seed list, and
      every other member is discovered transitively through the exchanged
      view. No `chitchat`, no second protocol. See **§9 Clustering**.
- [x] Write replication to peers — split into two streams, because they
      need opposite rules: an observer-owned data stream
      (`POST /internal/replicate` — results, state, alerts, incidents) that a
      receiver persists and never re-evaluates or re-notifies, and an HLC
      last-writer-wins config stream       (`POST /internal/config` — checks,
      sections, settings, credentials, rollup policies) that a receiver applies
      immediately.

      A digest/backfill anti-entropy sweep bounds how long a dropped push can
      leave a node behind. See **§9 Clustering**.
- [x] Failure detection — `alive` → `suspect` → `dead` on silence past
      `peer_timeout_secs`, retained rather than dropped so a returning node
      is recognised instead of read as new. Logged and shown in the admin UI;
      deliberately never paged — see **§9 Clustering** for why.
- [x] CLI: `coordinator` mode — an empty `peers` list is the unclustered shape,
      and a non-empty one turns on membership, replication and anti-entropy.
      Startup refuses a half-configured cluster (`peers` set without
      `peer_token` or `advertise_url`) instead of silently gossiping into a wall.
- [x] **Reopened Phase 5 consumer:** Stage A region-tree evaluation now consumes
      placement's redundant observations instead of feeding them into one shared
      counter. It persists per-agent state, derives credential-backed topology,
      rolls the four-state tree with two-threshold corroboration policies,
      publishes through configurable hold-downs, records `state_history`, and
      exposes/administers the tree. Rollup policy config replicates through the
      HLC config plane. See **§6 Region Tree**.
- [x] Mixed-version behavior is fail-soft: an older coordinator parses a
      `degraded` transition as `Unknown`, which is excluded from rollup
      denominators. During a rolling upgrade it can be temporarily less informed
      about partial outages, but total-outage transitions remain compatible.
- [x] Deployment surface reduced to exactly two Compose topologies:
      `docker-compose.yaml` starts one `standalone` process on `:9000` with one
      command; `docker-compose.cluster.yaml` starts three coordinators on
      `:9001`–`:9003` and three agents in one `us-east` region. The old
      `deploy/*.toml`, `docker-compose.agents.yaml`, and root `warden.toml` were
      removed; configuration now lives in Compose configs. Ports `:9004` and
      `:9005` are reserved for the Stage B multi-region/global topology and are
      intentionally unused in Stage A.

### Phase 6: Global Coordinator (Stage B) — done
- [x] Add at least a second regional cluster. `docker-compose.global.yaml` runs
      a 3-node `us` cluster (`:9001`–`:9003`) alongside a single-node `tw`
      region (`:9004`), leaving the Stage A Compose files untouched.
- [x] State push from regional coordinators to a global coordinator. Any
      coordinator with a `[coordinator.global]` block POSTs a summary (no raw
      data) to `/internal/global/state` every `push_secs`; see
      `Coordinator::spawn_global_push` and `global::wire::RegionStatePush`.
- [x] Global state cache and cross-region tree consumption. `global::cache`
      keeps the latest push per region (last-writer-wins), aggregates checks
      cross-region (worst live region wins), and drops regions past the
      staleness window. It backs `/api/v1/status`, `/api/v1/cluster` (region
      roster), and `/api/v1/admin/region-tree` so the existing SPA renders it.
- [x] Cluster-wide poll input for placement. Coordinators replicate fresh
      `agent_polls`; assignment uses the union with local polls, preserving
      default one-per-zone placement and rendezvous failover. Explicit
      `mode = "region", count = 1` selects one observer per region.
- [x] Global request-time merged reads. Logs, uptime, latency/sparklines,
      alerts, and incidents fan out to their regional owners and merge only in
      the response; the global cache retains state summaries, never raw data.
- [x] CLI: `global` mode (`warden global`), no raw storage, no probing.
- [x] Stage B Compose topology on reserved ports `:9004` (tw) / `:9005`
      (global), without changing either completed Stage A topology.
- [x] Cross-region alerting, off by default: the global node pages only when a
      check is down in every reporting region, or a region goes dark, and only
      when `[global] notifiers` is set. Avoids double-paging the regions.


## Dependencies (Rust Crates)

```toml
[dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# HTTP
axum = "0.7"
reqwest = { version = "0.12", features = ["json"] }

# Database
duckdb = "1.0"

# Clustering: no new dependency. Membership (POST /internal/cluster/ping),
# both replication streams and anti-entropy all ride the axum server and
# reqwest client every coordinator already runs. `chitchat` (SWIM over UDP)
# was in the original plan and is dropped: it would add a second protocol, a
# UDP port and a second authentication story to the compose and TLS setup,
# to buy indirect failure-detection probing that a 3-5 node cluster does not
# need. See "Why gossip over consensus" below and **§9 Clustering**.

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"

# Time
chrono = { version = "0.4", features = ["serde"] }

# Utilities
uuid = { version = "1", features = ["v4", "serde"] }
tracing = "0.1"
tracing-subscriber = "0.3"
clap = { version = "4", features = ["derive"] }

# Lua scripting (for custom eval)
mlua = { version = "0.9", features = ["lua54", "vendored"] }

# DNS checks
trust-dns-resolver = "0.23"

# Ping
surge-ping = "0.8"

# Email notifier (SMTP). The only backend that is not an HTTP POST, so it is
# the only one needing a client; rustls to match reqwest.
lettre = { version = "0.11", default-features = false, features = [
  "tokio1", "tokio1-rustls-tls", "smtp-transport", "builder",
] }
```

---

## Design Decisions

### Why DuckDB?

- **Columnar storage** — efficient compression for time-series
- **Fast analytics** — aggregations, percentiles native
- **SQL** — no need to implement PromQL
- **Embedded** — single file, no server

Trade-off: Single-writer. Solved by full replication — each node writes to its own DuckDB.

### Why full replication?

Options considered:
1. **Sharded by check_id** — efficient storage, but dashboard queries require fan-out
2. **Full replication** — 3-5x storage, but all queries are local

For 3-5 nodes and GB-scale data, storage cost is acceptable. Query simplicity wins.

### Why gossip over consensus (Raft)?

- No leader election needed — all nodes are equal
- Simpler failure detection
- Write replication is best-effort (eventual consistency OK for metrics)
- Raft adds complexity for strong consistency we don't need

**Why push-pull over HTTP beats SWIM here.** `chitchat` and other SWIM
implementations exist to make failure detection and dissemination cheap at
scale — indirect probing so one bad path doesn't wrongly suspect a live node,
gossip fan-out so a rumor spreads in `O(log N)` hops instead of every node
telling every node. None of that pays for itself at 3-5 nodes: direct probing
of every member is already cheap (§9's unsubsetted heartbeat is at most twenty
pings a cycle), and one-hop dissemination is not slower than SWIM's gossip
when there are only four other nodes to reach. What SWIM would cost here is
real — a second protocol over UDP, a new port to open in every compose file
and firewall rule, and a second authentication story alongside `peer_token` —
against a benefit (indirect probing, epidemic fan-out) that only shows up past
cluster sizes this project doesn't target. An HTTP server every coordinator
already runs, already behind the same TLS and the same bearer token as agent
traffic, is the cheaper failure detector at this N.

### Why Lua for custom eval?

- **Small** — `mlua` adds ~500KB
- **Sandboxed** — safe to run user code
- **Simple** — easier than JS for config-file scripting
- **Flexible** — matches Kener's custom eval capability

---

## Open Questions

1. **Config sync**: Should agents pull config from coordinator, or push-only?
2. **Multi-tenancy**: Single-org only, or support multiple tenants?
3. **Status page**: How much of Kener's theming to support?
4. **Retention**: Per-region retention policies, or global?

## References

- [Uptime Kuma](https://github.com/louislam/uptime-kuma) - Inspiration (what we're replacing)
- [Kener](https://github.com/rajnandan1/kener) - Another alternative (too complex)
- [Gatus](https://github.com/TwiN/gatus) - Go-based, good reference for check types

---

*Design drafted: May 2026*
*Status: Ready for implementation*
