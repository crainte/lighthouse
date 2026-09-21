# Production Readiness TODO

Prioritized action items from the production-readiness review. Existing items from `TODO` are included under **Existing product issues** so they are not lost.

## P0 — Production blockers

### Storage and runtime scalability

- [x] Move all synchronous DuckDB work off Tokio worker threads.
  - [x] Introduce a dedicated blocking database executor or worker.
  - [x] Remove direct `parking_lot::Mutex<Storage>` contention from async handlers and background tasks.
  - [x] Add load tests proving slow queries do not stall probes, gossip, WebSockets, or unrelated API requests.
- [x] Replace the full-history anti-entropy digest scan.
  - [x] Stop scanning full history for `digest(0)` every anti-entropy interval.
  - [x] Maintain compact per-check replication high-water metadata.
  - [x] Benchmark anti-entropy with production-scale retained history.
- [x] Optimize the public status hot path.
  - [x] Stop scanning all `check_results` with `row_number()` to find the latest row.
  - [x] Use materialized latest-observation/current-state data.
  - [x] Replace per-check uptime queries with one batched query or pre-aggregated time buckets.
  - [x] Benchmark `/api/v1/status` and global state pushes at expected production cardinality.
  - [x] Remove the per-check fan-out from the status **page load**, not just `/api/v1/status`.
    The endpoint is one batched query, but `App.svelte` then issues one `fetchUptime` per
    check and one `fetchLatencySparkline` per graph-enabled check, so a page load costs
    `1 + N + M` requests and as many database-worker jobs. At 10,000 checks that is ~20,000
    jobs against a 256-slot queue, and the overflow backpressures result ingest.
    - [x] Add set-based `uptime_strips` and `latency_points_for` storage methods that push
      bucketing into DuckDB and scan `check_results` once.
    - [x] Add batched public routes and collapse the `App.svelte` fan-out onto them.
    - [x] Benchmark a full status **page load** at production cardinality, not the endpoint alone.
- [x] Define and document a supported capacity envelope for DuckDB (offline single-replica envelope and the live three-peer 10,000-check gate both measured; see `SCALING.md`).
  - [x] Add a representative-data single-table baseline benchmark that reports DuckDB/WAL growth, history and latency p95, retention pruning, and checkpoint duration.
  - [x] Measure sustained ingest throughput, query latency, retention duration, row counts, database growth, and backup/restore with production-scale multi-domain data.
    - [x] Add an ignored benchmark for sustained real coordinator ingest with multi-domain rows/s and batch latency measurements.
    - [x] Add checkpointed offline backup/restore timing and size to the representative storage baseline.
    - [x] Batch ingest persistence so 10,000 checks × 3 domains sustain production ingest rates.
    - [x] Record the measured single-replica envelope and its linear retention projection in `SCALING.md`.
    - [x] Re-measure with live probe traffic, network replication, and concurrent query load during retention (`tests/release_benchmark.rs`; 10,000 checks passed three consecutive runs at 785–1,577 results/s with read p95 3 ms).
  - [x] Define raw, minute-rollup, hourly-rollup, state-transition, WAL, and backup retention policy plus per-domain/per-replica capacity estimates.
  - [x] Decide when hourly aggregation, time partitioning, or a separate raw-observation store is required.

### Replication and cluster correctness

- [x] Fix anti-entropy result pagination for rows sharing a timestamp.
  - [x] Replace the timestamp-only cursor with a stable composite cursor such as `(ts_millis, agent_id)` or the complete deduplication key.
  - [x] Add a test with more than one page of results sharing the same timestamp.
- [x] Add alert and incident anti-entropy recovery.
  - [x] Compare alert and incident high-water information during reconciliation.
  - [x] Add bounded, cursor-based backfill for alerts, incidents, incident updates, and incident tombstones.
  - [x] Verify a peer recovers after missing an entire outage and recovery lifecycle.
- [x] Make transition replication idempotent across gossip and cluster replication.
  - [x] Carry the originating alert ID through gossip.
  - [x] Use the same content-aware upsert path for both replication mechanisms.
  - [x] Add race-order tests where gossip arrives before and after `/internal/replicate`.
  - [x] Retain the legacy gossip transport for configured non-membership peers and rolling upgrades; upgraded senders now carry the same alert identity on both paths.
- [x] Ensure dead cluster members can rejoin without a process restart.
  - [x] Periodically retry dead members and configured seeds at a slower cadence.
  - [x] Test recovery after a two-node partition and a full multi-node network bisection.
- [x] Make each logical ingest/state transition transactional.
  - [x] Local ingest, replicated batches (including peer beliefs), and gossip transitions commit their storage changes together.
  - [x] Ingest, replication, and gossip publish after commit; keep local ingest's in-memory tracker state unchanged on write failure.
  - [x] Local ingest constraint-failure rollback/retry, replicated batch rollback, uncommitted-close/reopen, gossip write-failure, and process-interruption coverage.

### Release security and dependency hygiene

- [x] Upgrade frontend dependencies until `bun audit` is clean or every remaining advisory is explicitly accepted.
  - [x] Upgrade vulnerable Vite, Vitest, PostCSS, NanoID, esbuild, and related transitive packages.
  - [x] Rebuild and rerun all frontend tests after upgrades.
- [x] Add Rust dependency auditing to CI.
  - [x] Install and run `cargo-audit` or an equivalent advisory scanner.
  - [x] Fail release builds on unaccepted high/critical advisories.
- [x] Add explicit request-body limits.
  - [x] Set route-specific limits for result ingest, peer replication, config transfer, login, and admin JSON bodies.
  - [x] Add tests for oversized authenticated and unauthenticated requests.
- [x] Add production HTTP security headers.
  - [x] Add a Content Security Policy.
  - [x] Prevent framing with `frame-ancestors` and/or `X-Frame-Options`.
  - [x] Add `X-Content-Type-Options: nosniff` and a suitable referrer policy.
- [x] Make secure session-cookie behavior configurable for TLS deployments.
  - [x] Support the `Secure` cookie attribute without breaking intentional plain-HTTP LAN deployments.
  - [x] Do not trust forwarded-protocol headers unless the proxy is explicitly trusted.

## P1 — Reliability and data integrity

### Config-plane provisioning cost

Measured during the passing 10,000-check gate; provisioning-time, not steady-state.

- [ ] Batch `apply_config_entries` writes instead of one upsert per entry (up to ~4.6 s on the database worker per batch while provisioning 10,000 checks).
- [ ] Page or project `config_entries` for peers; a full read of 10,000 check documents costs ~2.2 s on the worker.

### Alert identity

- [ ] Fix alert ids that name a different check on different nodes. Observed in the global Compose stack: id `01a08215-de63-…-1788912120134-down` is stored against check `01a08215-de63-…` on `coordinator-us-b`/`-us-c` and against check `018f0000-…` (`upstream`) on `coordinator-us-a`. Both nodes believe their own row, so `upsert_alert` reports a conflict, the alert stream never converges, and anti-entropy re-offers the same page every sweep. Likely the originating-alert-id pass-through combined with dependency/upstream incident joining; a conflicting alert is now skipped rather than failing the whole replication batch, which contains the damage but does not resolve it.

### Limits measured during the 10,000-check gate

- [x] Replicated dedup no longer scales with retained history: splitting the batch on the per-check high-water mark keeps the live path off the history table entirely (17 ms at 20M rows, 16 ms at 40M). Only repair reaches the anti-join.
- [x] Retention costs what it removes rather than what it keeps: the two full rebuilds after the delete are replaced by targeted maintenance (176 ms versus 7,552 ms on a daily window at 40M rows), and the delete is sliced across 24 jobs so the worker interleaves.
- [x] The global dashboard showed "Collecting latency data…" for every check: `/api/v1/uptime` and `/api/v1/latency-points` are fleet-wide *data* reads, but the global relay treated them as singleton config reads and answered from one region, which holds no results for checks its own agents do not probe. Both now fan out to every region and merge per check.
- [ ] The full status page is 134.7 MB over 3 requests at 10,000 checks (p95 509 ms). Paginate or filter the dashboard; the monitoring cardinality is supportable, the single-page rendering of it is not.
- [ ] `agent_checks_handler` runs `list_checks()` on the database worker for every agent poll, though the coordinator already caches check metadata in memory. Cost is `agents / poll_secs × O(checks)` on the one worker.
- [ ] The notification dispatcher is a single serial loop with a 10 s per-send timeout, fed by a 1,024-slot channel whose `try_send` drops on overflow — a slow notifier delays every other alert, and an overflow loses alerts with no retry row written.

### Storage maintenance

- [ ] Review and add appropriate physical organization/indexing for common queries.
  - [ ] Evaluate an index or storage strategy for `check_results(check_id, ts_millis)`.
  - [ ] Evaluate indexes for alert, incident, and state-history lookup patterns.
  - [ ] Confirm index behavior and write cost with the bundled DuckDB version.
- [ ] Make retention and cleanup operations transactional where partial completion can expose inconsistent state.
- [ ] Implement minute/hour latency rollups, query resolution switching, and raw/rollup boundary semantics before reducing raw retention.
- [ ] Replace monolithic raw-result deletes with a benchmarked, crash-safe time-partition lifecycle that drops expired partitions only after rollup coverage is complete.
- [ ] Define retention-aware peer recovery for nodes behind raw retention: snapshot restore, explicit raw-data gaps, or extended catch-up storage.
- [x] Run large retention deletes outside Tokio worker threads and avoid holding the main storage lock for the full operation (sliced across 24 database jobs; the worker already runs off the Tokio threads).
- [ ] Add explicit checkpoint/file-size maintenance after large pruning operations if benchmarks show it is needed.
- [ ] Monitor logical row counts, DuckDB file size, prune duration, query duration, and database errors.
- [ ] Cap route/list page sizes consistently; the internal anti-entropy backfill endpoint is capped, but user-facing route pagination still needs explicit limits.
- [ ] Replace the correlated state-history prune query with a bounded/windowed approach if production benchmarks show poor scaling.
- [ ] Create and test documented backup and restore procedures.
- [ ] Add restart/recovery tests using a persistent production-sized database.

### TLS and probe behavior

- [ ] Resolve the dead `allow_self_signed` option.
  - [ ] Either implement a per-check HTTP TLS client honoring the option or remove the option from config and UI.
  - [ ] Add tests for trusted, self-signed, expired, and hostname-mismatched certificates.
- [ ] Document SSL certificate-check semantics.
  - [ ] Explain that the expiry probe captures certificate dates without normal chain validation.
  - [ ] Decide whether certificate trust and hostname validity should be independently reportable failures.
- [ ] Document that monitor targets and notifier webhooks provide intentional server-side network access.
  - [ ] State that admin access is network-level privilege.
  - [ ] Consider optional private-network/allow-list policies for multi-tenant or hostile deployments.

### Authentication and secrets

- [ ] Redact notifier secrets in admin read responses.
  - [ ] Return placeholders or presence flags for SMTP passwords, routing keys, and webhook secrets.
  - [ ] Preserve existing secrets unless an update explicitly replaces them.
- [ ] Add explicit trusted-proxy support if per-client login rate limiting is required behind a reverse proxy.
  - [ ] Keep the current raw peer-IP behavior as the safe default.
  - [ ] Document shared proxy rate-limit buckets until trusted-proxy support exists.
- [ ] Document TLS termination and transport-security requirements for production deployments.
- [ ] Confirm operational secret rotation procedures for admin, peer, global, and agent credentials.

### Frontend correctness

- [ ] Prevent stale filter responses in `ProbeResultsAdmin.svelte`.
  - [ ] Add cancellation or monotonically increasing request IDs.
- [ ] Prevent overlapping/out-of-order polling in `ClusterAdmin.svelte`.
  - [ ] Add an in-flight guard or schedule the next poll after the current one finishes.
- [ ] Consolidate duplicate initial session checks in `App.svelte`.
- [ ] Show acknowledgement failures to the operator instead of only logging them to the console.
- [ ] Add an `aria-live` announcement for important live status changes.
- [ ] Make "Past Incidents" respect the status page's selected time window.
  - [ ] Symptom: selecting `24h` still lists incidents from 11 days ago. The range switch
        only drives the charts — `setDays()` in `App.svelte` calls `loadCharts()` and
        nothing else, and `fetchIncidents()` sends only `?limit=`.
  - [ ] Add a `since` (epoch ms) parameter to `IncidentQuery` in
        `coordinator/routes/incidents.rs` and a bound to `Storage::list_incidents`, so the
        window is applied in SQL rather than by shipping full history to the browser. The
        global relay forwards the query string as-is, so it needs no change.
  - [ ] Keep unresolved incidents visible regardless of `since`: the window bounds resolved
        incidents only, so an ongoing outage older than the window is never hidden from the
        status page.
  - [ ] Refetch incidents from `setDays()`, and keep the admin console's list unwindowed —
        `App.svelte` currently feeds one `incidents` array to both `IncidentsList` (admin)
        and `PastIncidents` (public), so a windowed shared fetch would truncate the console
        too.
  - [ ] Cover it with a test that a resolved incident outside the window is absent while an
        open one outside it is still returned.

### Status sub-pages

Batching bounds the number of *requests* a status page makes; it does not bound the
amount of work behind them. A flat page still computes and ships every visible check's
90-segment strip, which is unreadable for an operator and wasteful for the server long
before 10,000 checks. Sub-pages bound both.

Display mode is a per-section admin choice, not a global migration. Both behaviours stay
supported: a small section is better inline, and forcing a click for three checks would
be worse than what exists today.

- [x] Add a per-section display mode: expand inline (today's behaviour) or link to its
      own status page.
  - [x] Add the mode to `Section` next to `name`/`position`/`collapsed`, defaulting to
        inline so existing deployments render unchanged. `collapsed` stays the initial
        render state for inline sections and is meaningless for paged ones.
  - [x] Carry it through the section config-replication path with `#[serde(default)]`,
        so a peer on an older build still parses the document.
  - [x] Add the control to `SectionsAdmin.svelte`.
- [x] Add the paged view.
  - [x] Add a `#/group/<name>` view to `viewFromHash()` in `App.svelte`, alongside the
        existing `#/check/`, `#/incident/` and `#/admin/` routes.
  - [x] Render a paged section on the primary page as a summary tile (green/yellow/red)
        instead of its rows.
  - [x] Derive a tile's colour from `crate::rollup::RollupPolicy` (`Threshold`/quorum),
        which already expresses "this node is degraded when N children fail" for zones
        and regions — a section is another node set, so it must not get a second,
        inconsistent rule. Computed by `section_rollups` on the coordinator under the
        override key `section:<name>`, falling back to `CHECK_DEFAULT`, and shipped on
        `/api/v1/status`. The browser never folds a section state itself.
  - [x] Fetch strips and sparklines only for the checks actually rendered, so a paged
        section costs nothing until it is opened. `/api/v1/uptime` and
        `/api/v1/latency-points` take `?section=<name>`; absent means the primary page,
        which the server reads as loose checks plus inline sections.
  - [x] Decide explicitly whether a tile shows its own history strip. **It does not.**
        Colour and counts only ("12 checks · 1 down"), both of which the rollup already
        carries, so a paged section adds no query to the hot path at all. A strip would
        have been one folded group-level query over state spans — cheap per section, but
        paid by every visitor on every load to decorate a tile most never open.
  - [x] Keep deep links working: a check page must remain reachable without loading its
        section's page first. `#/check/<id>` matches independently of `#/group/`.
- [x] Surface the cost of leaving a large section inline.
  - [x] Because the mode is an operator choice, an all-inline deployment keeps the flat
        page's cost — the bound is available, not automatic. `SectionsAdmin` warns above
        `INLINE_WARN_AT` checks rather than silently overriding the choice.

Follow-ups, deliberately not done here:

- [ ] Navigating from a section page into a check and back refetches that section's
      charts. Two bounded requests, not a fan-out, so it is a fair price for maps that
      hold exactly the current scope at the current window and nothing stale — but a
      last-loaded-scope guard would remove it if the round trip is ever felt.
- [ ] A section page has no rollup banner of its own; it just repeats the primary page's
      per-check rows under a heading. The state is already computed and shipped, so this
      is presentation only.

## P2 — Maintainability and code reduction

### Split oversized backend modules

- [ ] Split `src/coordinator/storage.rs` into focused modules.
  - [ ] `storage/schema.rs`
  - [ ] `storage/results.rs`
  - [ ] `storage/checks.rs`
  - [ ] `storage/incidents.rs`
  - [ ] `storage/alerts.rs`
  - [ ] `storage/config_replication.rs`
  - [ ] `storage/fleet.rs`
  - [ ] Move tests to their corresponding domains.
- [ ] Split `src/coordinator/mod.rs` into focused modules.
  - [ ] `coordinator/ingest.rs`
  - [ ] `coordinator/replication.rs`
  - [ ] `coordinator/dispatch.rs`
  - [ ] `coordinator/rollup_tree.rs`
  - [ ] `coordinator/router.rs`
- [ ] Split `src/global/mod.rs` into routes, relay, merge, and asset-serving modules.
- [ ] Split `src/config.rs` by runtime role if configuration continues to grow.
- [ ] Split `src/agent.rs` into protocol-specific probe modules.
- [ ] Extract shared coordinator/global HTTP utilities.
  - [ ] Share error response types and helpers.
  - [ ] Share static embedded-asset serving.
  - [ ] Reconcile duplicated fleet/member DTOs where their semantics match.
- [ ] Replace repeated route-level `require_admin` boilerplate with an Axum request-parts extractor.

### Split oversized frontend modules

- [ ] Split `CheckEditor.svelte` into an orchestrator and focused subcomponents.
  - [ ] HTTP fields
  - [ ] TCP fields
  - [ ] DNS fields
  - [ ] ICMP/ping fields
  - [ ] SSL fields
  - [ ] Heartbeat fields
  - [ ] OpenMetrics fields
  - [ ] Headers editor
  - [ ] Placement editor
  - [ ] Rollup-policy editor
  - [ ] Dependency editor
  - [ ] Evaluation tester
- [ ] Split `AgentsAdmin.svelte` into focused panels/components.
- [ ] Extract routing, session, and WebSocket/resync orchestration from `App.svelte`.
- [ ] Split `frontend/src/lib/admin.ts` into per-domain API/type modules.
- [ ] Review `RegionTreeAdmin.svelte`, `DayDetailModal.svelte`, `SectionsAdmin.svelte`, and `CheckDetail.svelte` for natural component boundaries.

### Dead and compatibility code

- [ ] Remove or use the currently unused `sortChecks` helper and its supporting types/tests.
- [ ] Remove the stale `#[allow(dead_code)]` from `CheckMeta::degraded_is_downtime` if it remains used.
- [ ] Decide and document the pre-1.0 database compatibility policy.
  - [ ] If existing pre-UUID databases must be supported, move the UUID migration subsystem into a dedicated migration module and retain upgrade tests.
  - [ ] If they do not need support, delete the legacy migration subsystem and document the required one-time rebuild procedure.
  - [ ] Avoid mixing implicit `ALTER TABLE` migrations with undocumented drop/rebuild expectations.
- [ ] Review comments and design documentation for outdated anti-entropy guarantees.
- [ ] Split large integration test files by feature area, especially `tests/admin_api.rs`.

## P2 — Production operations and validation

- [ ] Add production-scale benchmarks and regression thresholds.
  - [ ] Sustained result ingest
  - [ ] Public status latency
  - [ ] 24-hour uptime calculation
  - [ ] History and latency queries
  - [ ] Anti-entropy reconciliation
  - [ ] Retention cleanup
  - [ ] Startup and recovery with a large persistent database
- [ ] Add chaos and recovery tests.
  - [ ] Peer unavailable during DOWN transition
  - [ ] Peer unavailable through DOWN and recovery
  - [ ] Duplicate/reordered replication delivery
  - [ ] Coordinator restart during ingest
  - [ ] Two-node partition healing
  - [ ] Full cluster bisection healing
  - [ ] Agent failover between coordinators
- [ ] Add production observability.
  - [ ] Database operation latency and lock wait time
  - [ ] Ingest queue/backpressure
  - [ ] Replication failures and reconciliation lag
  - [ ] Notification retry/give-up counts
  - [ ] Cluster membership transitions
  - [ ] Retention duration and rows removed
- [ ] Establish release gates.
  - [ ] Clean `cargo check --all-targets`.
  - [ ] Clean `cargo clippy --all-targets -- -D warnings`.
  - [ ] Passing Rust and frontend tests.
  - [ ] Passing frontend and Rust dependency audits.
  - [ ] Successful production frontend and container builds.
  - [ ] Backup/restore smoke test.
  - [ ] Clean or intentionally documented working tree.
- [ ] Break the current large working-tree change set into reviewable commits before release.

## Existing product issues

- [ ] Convert global coordinator credentials to per-region credentials used to attach regional coordinators to the global coordinator.
- [ ] Add a supported way to decommission a cluster member.
  - [ ] Remove it from persistent membership and configured seeds on every node; stop retries and replication to it.
  - [ ] Define and test how removal propagates without stale gossip or a returning node resurrecting membership.
- [ ] Add a supported way to remove a region.
  - [ ] Define removal behavior for cached state, credentials, checks, incidents, and stale regional coordinators.
  - [ ] Propagate deletion safely and prevent a removed region from being resurrected by stale state.
- [ ] Relax or redesign check-name validation so names such as `h@h` and `home assistant` are supported.
  - [ ] Keep immutable UUID identity separate from human-readable display names.
  - [ ] Audit URLs, selectors, storage keys, and replication code so they do not depend on restricted names.
- [ ] Verify and fix downtime accuracy for long-interval checks.
  - [ ] Confirm failing checks use Kubernetes-style retry/failure probing rather than waiting for the normal interval.
  - [ ] Confirm recovery is detected promptly while a check is down.
  - [ ] Add tests for `failures_before_down`, long schedules, retry cadence, recovery cadence, hold-down, and restart behavior.
  - [ ] Remove the old `TODO` item if current retry/down cadence already solves the issue.

## Confirmed strengths to preserve

These are not action items, but changes above should retain them:

- Constant-time authentication and credential comparisons.
- HMAC-signed stateless admin sessions.
- Random agent credentials hashed at rest.
- Credential-bound agent region and zone identity.
- Peer-token protection on internal routes.
- Parameterized request-path SQL.
- Restricted Lua standard libraries and instruction limits.
- No raw HTML rendering in the frontend.
- Distroless production runtime image.
- Existing Rust and frontend test coverage.
