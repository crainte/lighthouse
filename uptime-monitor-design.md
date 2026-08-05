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

Single binary with three modes:

```
uptime --agent        # Runs checks, posts to regional coordinators
uptime --coordinator  # Regional coordinator, clusterable
uptime --global       # Global coordinator, receives state from regions
uptime --standalone   # All-in-one for small deployments
```

### Full Deployment

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
| Current state | Regional DuckDB | Full (all nodes in region) |
| Config, alerts | Regional DuckDB | Full (all nodes in region) |
| State summaries | Global coordinator | Pushed from regions, cached |

### Query Routing

| Query | Where | Latency |
|-------|-------|---------|
| Dashboard (all checks status) | Global (cached) | Local |
| Region health | Global (cached) | Local |
| Check history | Regional coordinator | Local to region |
| Cross-region analytics | Global → fan-out to regions | Slowest region |

**Example:** You're in us-west, querying Taiwan check history:
- Dashboard load: instant (global has cached state)
- Drill into Taiwan check: ~250ms (routes to AP region)

---

## Component Design

### 1. Agent

**Responsibilities:**
- Execute health checks (HTTP, TCP, DNS, ICMP, custom scripts)
- Report results to coordinator
- Run independently with minimal configuration

**Architecture:**
```rust
// One async task per check, independent timers
struct Agent {
    config: AgentConfig,
    checks: Vec<CheckDefinition>,
    result_tx: mpsc::Sender<CheckResult>,
}

// Each check runs in its own task
async fn check_loop(check: CheckDefinition, tx: mpsc::Sender<CheckResult>) {
    let mut interval = tokio::time::interval(check.interval);
    loop {
        interval.tick().await;
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

No eval = default logic (2xx = UP, else DOWN).

**Agent Configuration (YAML):**
```yaml
coordinator: https://coordinator.example.com
agent_id: us-east-1a-agent-01
zone: us-east-1a
region: us-east

checks:
  - name: api-health
    type: http
    target: https://api.example.com/health
    interval: 30s
    timeout: 10s
    expect:
      status: 200
      body_contains: "ok"
  
  - name: database
    type: tcp
    target: db.internal:5432
    interval: 60s
    timeout: 5s
```

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

**No raw data storage.** Global coordinator only caches state summaries.

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
    
    async fn query_check_history(&self, check_id: &str, region: &str) -> Results {
        // Route to regional coordinator
        let endpoint = self.region_endpoints.get(region).unwrap();
        self.proxy_to_region(endpoint, check_id).await
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
    channels: Vec<NotificationChannel>,
    db: duckdb::Connection, // For retry persistence
}

enum NotificationChannel {
    Slack { webhook_url: String },
    Email { smtp_config: SmtpConfig },
    Webhook { url: String, headers: HashMap<String, String> },
    PagerDuty { routing_key: String },
}

impl Notifier {
    async fn dispatch(&self, alert: Alert) {
        for channel in &self.channels {
            match channel.send(&alert).await {
                Ok(_) => self.db.mark_sent(alert.id, channel.id()).await,
                Err(e) => self.db.queue_retry(alert.id, channel.id(), e).await,
            }
        }
    }
}
```

---

### 6. Region Tree (Hierarchical State)

**Structure:**
```
Global
├── us (Region)
│   ├── us-east-1 (Zone)
│   │   ├── us-east-1a-agent-01
│   │   └── us-east-1b-agent-02
│   └── us-west-2 (Zone)
│       └── us-west-2a-agent-01
├── eu (Region)
│   └── eu-west-1 (Zone)
│       └── eu-west-1a-agent-01
└── ap (Region)
    └── ap-southeast-1 (Zone)
        └── ap-southeast-1a-agent-01
```

**Node State:**
```rust
#[derive(Clone, Copy, PartialEq)]
enum NodeState {
    Up,       // All children healthy (or policy satisfied)
    Degraded, // Some children failing, but policy still satisfied
    Down,     // Policy violated
    Unknown,  // No recent data
}

struct TreeNode {
    id: String,
    node_type: NodeType, // Global, Region, Zone, Agent
    state: NodeState,
    policy: RollupPolicy,
    children: Vec<TreeNode>,
    last_update: DateTime<Utc>,
}

#[derive(Clone)]
enum RollupPolicy {
    Any,                    // UP if any child is UP
    Majority,               // UP if >50% of children are UP
    All,                    // UP only if all children are UP
    Quorum(usize),          // UP if N children are UP
    Weighted(Vec<(String, u32)>), // Weighted voting
}
```

**Rollup Logic:**
```rust
impl TreeNode {
    fn compute_state(&self) -> NodeState {
        if self.children.is_empty() {
            return self.state; // Leaf node (agent) - direct state
        }
        
        let child_states: Vec<_> = self.children.iter()
            .map(|c| c.compute_state())
            .collect();
        
        match &self.policy {
            RollupPolicy::Any => {
                if child_states.iter().any(|s| *s == NodeState::Up) {
                    NodeState::Up
                } else if child_states.iter().all(|s| *s == NodeState::Down) {
                    NodeState::Down
                } else {
                    NodeState::Degraded
                }
            }
            RollupPolicy::All => {
                if child_states.iter().all(|s| *s == NodeState::Up) {
                    NodeState::Up
                } else if child_states.iter().any(|s| *s == NodeState::Down) {
                    NodeState::Down
                } else {
                    NodeState::Degraded
                }
            }
            RollupPolicy::Majority => {
                let up_count = child_states.iter()
                    .filter(|s| **s == NodeState::Up)
                    .count();
                if up_count > child_states.len() / 2 {
                    NodeState::Up
                } else if up_count == 0 {
                    NodeState::Down
                } else {
                    NodeState::Degraded
                }
            }
            // ... other policies
        }
    }
}
```

---

### 4. Data Models

**Check Result (Agent → Coordinator):**
```rust
#[derive(Serialize, Deserialize)]
struct CheckResult {
    check_id: String,
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
#[derive(Serialize, Deserialize)]
struct Alert {
    id: Uuid,
    alert_type: AlertType,
    scope: AlertScope,
    node_id: String,
    previous_state: NodeState,
    current_state: NodeState,
    timestamp: DateTime<Utc>,
    details: AlertDetails,
}

enum AlertType {
    StateChange,    // UP → DOWN, DOWN → UP
    Degradation,    // Partial failure
    Recovery,       // DOWN → UP
    Flapping,       // Rapid state changes
}

enum AlertScope {
    Check { check_id: String },
    Agent { agent_id: String },
    Zone { zone_id: String },
    Region { region_id: String },
    Global,
}
```

---

### 5. DuckDB Schema

DuckDB is used for check result history and analytics. Its columnar storage provides efficient compression and fast aggregation queries.

```sql
-- Monitors/checks configuration
CREATE TABLE monitors (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,  -- http, tcp, dns, icmp
    target VARCHAR NOT NULL,
    interval_secs INTEGER NOT NULL DEFAULT 60,
    timeout_secs INTEGER NOT NULL DEFAULT 10,
    config JSON,  -- type-specific config (including eval script)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Region tree structure
CREATE TABLE region_tree (
    id VARCHAR PRIMARY KEY,
    parent_id VARCHAR REFERENCES region_tree(id),
    node_type VARCHAR NOT NULL,  -- global, region, zone, agent
    name VARCHAR NOT NULL,
    policy VARCHAR NOT NULL DEFAULT 'majority',
    policy_config JSON
);

-- Check results history (main time-series table)
CREATE TABLE check_results (
    check_id VARCHAR NOT NULL,
    agent_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,  -- up, down, timeout, error
    latency_ms INTEGER,
    details VARCHAR,
    timestamp TIMESTAMP NOT NULL
);

-- Current state (materialized for fast reads)
CREATE TABLE current_state (
    node_id VARCHAR PRIMARY KEY,
    state VARCHAR NOT NULL,  -- up, down, degraded, unknown
    last_check TIMESTAMP,
    last_state_change TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0
);

-- Alert history
CREATE TABLE alerts (
    id VARCHAR PRIMARY KEY,
    alert_type VARCHAR NOT NULL,
    scope VARCHAR NOT NULL,
    node_id VARCHAR NOT NULL,
    previous_state VARCHAR,
    current_state VARCHAR NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    acknowledged_at TIMESTAMP,
    acknowledged_by VARCHAR
);

-- Notification queue (for retries)
CREATE TABLE notification_queue (
    id INTEGER PRIMARY KEY,
    alert_id VARCHAR NOT NULL,
    channel_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,  -- pending, sent, failed
    attempts INTEGER DEFAULT 0,
    last_attempt TIMESTAMP,
    next_attempt TIMESTAMP,
    error VARCHAR
);
```

**Example Queries:**

```sql
-- Average latency over last hour
SELECT avg(latency_ms), percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95
FROM check_results
WHERE check_id = 'api-health'
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
WHERE check_id = 'api-health'
  AND timestamp > now() - INTERVAL '1 hour'
GROUP BY bucket
ORDER BY bucket;
```

---

## API Design

### Agent → Coordinator

```
POST /api/v1/results
Content-Type: application/json
X-Agent-ID: us-east-1a-agent-01
X-Agent-Secret: <shared-secret>

{
  "results": [
    {
      "check_id": "api-health",
      "timestamp": "2024-01-15T10:30:00Z",
      "status": "up",
      "latency_ms": 45
    },
    {
      "check_id": "database",
      "timestamp": "2024-01-15T10:30:05Z",
      "status": "down",
      "details": "Connection refused"
    }
  ]
}

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
→ List all monitors

GET /api/v1/monitors/{id}/history?from=&to=&limit=
→ Check history

GET /api/v1/alerts?status=open&limit=
→ Alert list

POST /api/v1/alerts/{id}/acknowledge
→ Acknowledge alert

GET /api/v1/regions
→ Region tree structure

WebSocket /api/v1/ws/events
→ Real-time state updates
```

---

## Configuration

### Coordinator Config (YAML)

```yaml
server:
  listen: 0.0.0.0:8080
  tls:
    cert: /etc/uptime/cert.pem
    key: /etc/uptime/key.pem

database:
  path: /var/lib/uptime/data.duckdb
  
auth:
  agents:
    - id: us-east-1a-agent-01
      secret: ${AGENT_SECRET_1}
    - id: eu-west-1a-agent-01
      secret: ${AGENT_SECRET_2}

notifications:
  channels:
    - id: slack-ops
      type: slack
      webhook_url: ${SLACK_WEBHOOK}
      
    - id: pagerduty-critical
      type: pagerduty
      routing_key: ${PD_KEY}
      
  routing:
    - scope: global
      state: down
      channels: [slack-ops, pagerduty-critical]
    - scope: zone
      state: degraded
      channels: [slack-ops]

alerting:
  flap_threshold: 3        # State changes within window to detect flapping
  flap_window_secs: 300    # 5 minute window
  recovery_alerts: true    # Send alert when recovering from DOWN
  
retention:
  results_days: 30
  alerts_days: 90
```
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
- [ ] Monitor configuration
- [ ] Alert management
- [x] Real-time updates (WebSocket)
- [x] Incidents: auto-open on DOWN, auto-resolve on UP, REST + WebSocket events + admin panel

### Phase 3: Check Types & Eval
- [x] TCP checks
- [x] DNS checks — FQDN-forced, caching disabled, UDP retry + TCP fallback
- [x] ICMP ping — requires `CAP_NET_RAW`
- [x] Declarative `expect` (status code, body/answer substring)
- [ ] Lua eval for custom check logic

Deferred from Phase 3, tracked here so it isn't lost:
- [x] Confirmation threshold before DOWN (`failures_before_down`) — a check
      flips DOWN only after N consecutive failed probes; recovery stays immediate
- [x] A check whose first-ever observation is DOWN does not alert
      (`StateTracker` treats the first observation as a baseline)
- [x] Agent result batches are retained and retried when the coordinator POST
      fails — bounded buffer, oldest dropped on overflow
- [ ] `zone` / `region` on `CheckResult` (needed by the Phase 5/6 region tree)

### Phase 4: Notifications & Status Page
- [ ] Slack, Email, PagerDuty integrations
- [ ] Retry queue with persistence
- [ ] Public status page (Kener replacement)
- [ ] Maintenance windows

### Phase 5: Regional Clustering
- [ ] Gossip membership (`chitchat`)
- [ ] Write replication to peers
- [ ] Failure detection
- [ ] CLI: `--coordinator` mode

### Phase 6: Global Coordinator
- [ ] State push from regional to global
- [ ] Global state cache
- [ ] Query routing to regions
- [ ] CLI: `--global` mode


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

# Clustering
chitchat = "0.8"          # Gossip protocol

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
