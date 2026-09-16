# Production Readiness TODO

Prioritized action items from the production-readiness review. Existing items from `TODO` are included under **Existing product issues** so they are not lost.

## P0 — Production blockers

### Storage and runtime scalability

- [ ] Move all synchronous DuckDB work off Tokio worker threads.
  - [ ] Introduce a dedicated blocking database executor or worker.
  - [ ] Remove direct `parking_lot::Mutex<Storage>` contention from async handlers and background tasks.
  - [ ] Add load tests proving slow queries do not stall probes, gossip, WebSockets, or unrelated API requests.
- [ ] Replace the full-history anti-entropy digest scan.
  - [ ] Stop calling `digest(0)` every anti-entropy interval.
  - [ ] Maintain compact per-check replication high-water metadata, or use a bounded recent window plus infrequent full reconciliation.
  - [ ] Benchmark anti-entropy with production-scale retained history.
- [ ] Optimize the public status hot path.
  - [ ] Stop scanning all `check_results` with `row_number()` to find the latest row.
  - [ ] Use materialized latest-observation/current-state data.
  - [ ] Replace per-check uptime queries with one batched query or pre-aggregated time buckets.
  - [ ] Benchmark `/api/v1/status` and global state pushes at expected production cardinality.
- [ ] Define and document a supported capacity envelope for DuckDB.
  - [ ] Measure ingest throughput, query latency, retention duration, row counts, and database growth.
  - [ ] Decide when hourly aggregation, time partitioning, or a separate raw-observation store is required.

### Replication and cluster correctness

- [ ] Fix anti-entropy result pagination for rows sharing a timestamp.
  - [ ] Replace the timestamp-only cursor with a stable composite cursor such as `(ts_millis, agent_id)` or the complete deduplication key.
  - [ ] Add a test with more than one page of results sharing the same timestamp.
- [ ] Add alert and incident anti-entropy recovery.
  - [ ] Compare alert and incident high-water information during reconciliation.
  - [ ] Add bounded, cursor-based backfill for alerts, incidents, incident updates, and incident tombstones.
  - [ ] Verify a peer recovers after missing an entire outage and recovery lifecycle.
- [ ] Make transition replication idempotent across gossip and cluster replication.
  - [ ] Carry the originating alert ID through gossip.
  - [ ] Use the same content-aware upsert path for both replication mechanisms.
  - [ ] Add race-order tests where gossip arrives before and after `/internal/replicate`.
  - [ ] Decide whether the legacy gossip transition path can be removed in favor of one replication plane.
- [ ] Ensure dead cluster members can rejoin without a process restart.
  - [ ] Periodically retry dead members and configured seeds at a slower cadence.
  - [ ] Test recovery after a two-node partition and a full multi-node network bisection.
- [ ] Make each logical ingest/state transition transactional.
  - [ ] Atomically persist raw results, agent beliefs, current state, state history, alerts, and incident changes where appropriate.
  - [ ] Do not publish replication, WebSocket events, or notifications until the transaction commits.
  - [ ] Add failure-injection tests for partial database errors and process interruption.

### Release security and dependency hygiene

- [ ] Upgrade frontend dependencies until `bun audit` is clean or every remaining advisory is explicitly accepted.
  - [ ] Upgrade vulnerable Vite, Vitest, PostCSS, NanoID, esbuild, and related transitive packages.
  - [ ] Rebuild and rerun all frontend tests after upgrades.
- [ ] Add Rust dependency auditing to CI.
  - [ ] Install and run `cargo-audit` or an equivalent advisory scanner.
  - [ ] Fail release builds on unaccepted high/critical advisories.
- [ ] Add explicit request-body limits.
  - [ ] Set route-specific limits for result ingest, peer replication, config transfer, login, and admin JSON bodies.
  - [ ] Add tests for oversized authenticated and unauthenticated requests.
- [ ] Add production HTTP security headers.
  - [ ] Add a Content Security Policy.
  - [ ] Prevent framing with `frame-ancestors` and/or `X-Frame-Options`.
  - [ ] Add `X-Content-Type-Options: nosniff` and a suitable referrer policy.
- [ ] Make secure session-cookie behavior configurable for TLS deployments.
  - [ ] Support the `Secure` cookie attribute without breaking intentional plain-HTTP LAN deployments.
  - [ ] Do not trust forwarded-protocol headers unless the proxy is explicitly trusted.

## P1 — Reliability and data integrity

### Storage maintenance

- [ ] Review and add appropriate physical organization/indexing for common queries.
  - [ ] Evaluate an index or storage strategy for `check_results(check_id, ts_millis)`.
  - [ ] Evaluate indexes for alert, incident, and state-history lookup patterns.
  - [ ] Confirm index behavior and write cost with the bundled DuckDB version.
- [ ] Make retention and cleanup operations transactional where partial completion can expose inconsistent state.
- [ ] Run large retention deletes outside Tokio worker threads and avoid holding the main storage lock for the full operation.
- [ ] Add explicit checkpoint/file-size maintenance after large pruning operations if benchmarks show it is needed.
- [ ] Monitor logical row counts, DuckDB file size, prune duration, query duration, and database errors.
- [ ] Cap the internal backfill endpoint's requested page size.
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
