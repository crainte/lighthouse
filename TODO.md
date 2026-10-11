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

### Placement-scoped configuration

Design settled in `SCALING.md`. All five phases are implemented.

- [x] **Phase 1** — global holds the catalogue and placement durably, replicated between global nodes, reusing the coordinator's config plane (HLC entries, per-kind digests, cursored backfill, cluster membership). Fixes the existing gap that `fan_out_write` applies partially and nothing reconciles it. Scope: checks and sections are authoritative on the global store; notifiers, credentials and rollup policies stay on the pre-existing all-region fan-out until Phase 2/3 widen it — see `global::...
- [x] **Phase 2** — per-region reconciliation against the set placement implies, with an `unplace` verb that drops the local definition, keeps the check id, and never writes a tombstone. A tombstone would replicate and delete the check in the region still probing it. `global::reconcile::reconcile_checks` diffs placement-scoped id sets (`Storage::check_ids_for_region` against `/internal/global/check-ids`) rather than re-paging the whole kind, so a converged region costs one digest comparison pe...
- [x] **Phase 3** — deliver probe detail only to placed regions; regional status pages list only what the region holds. Closes the disclosure (a target with an embedded credential currently sits in every region) and cuts per-region config cost. Fell out of Phase 2: a region's status page and admin check list already read from what it locally holds, which reconciliation now scopes to placement; the global dashboard is unaffected since it merges per-region pushes keyed by check id.
- [x] **Phase 4** — run the global as three nodes.
- [x] **Phase 5** — cross-region dependency attribution at the global tier, the only place both ends of the edge are visible. Distinct from `RollupPolicy`, which describes state folding rather than causation.
  - [x] Group a multi-region check's per-region incidents on the global tier. Observed in the global Compose stack: `dns` and `ntfy` place to every region, so `us` and `tw` each open their own incident for one outage — `…-1790119380023` (us, 16:23:00) beside `…-1790119440028` (tw, 16:24:00), both titled "dns is down". `unifi` and `upstream`, pinned to `us`, never duplicate, which is what identifies the cause: an incident id is `<check_id>-<detection_ts>`, and two regions detecting seconds ...
- [x] Clear a check's published state on unplace so a region stops asserting a state it no longer observes; keep raw results, which reconnect by id on re-placement.
- [x] **Phase 0** — reference secrets in check documents rather than carrying them, following the existing `token_env` convention: the region resolves a named credential at hand-off to its own agents, the admin UI renders the reference, and an unresolved reference reads as a configuration error naming it. This is what lets the global tier hold and move whole documents while values stay in the region that probes with them, and it turns placement-scoped distribution from a security control into ...

### Config-plane provisioning cost

Measured during the passing 10,000-check gate; provisioning-time, not steady-state.

- [x] Batch `apply_config_entries` writes instead of one upsert per entry (up to ~4.6 s on the database worker per batch while provisioning 10,000 checks).
- [x] Page or project `config_entries` for peers; a full read of 10,000 check documents costs ~2.2 s on the worker.

### Alert identity

- [x] Fix alert ids that name a different check on different nodes. Root-caused against the
      live stack rather than guessed: `canonicalize_random_legacy_check_ids` rewrote
      `alerts.node_id` but not `alerts.id`, which embeds the check id, and it ran per node
      inside a replicated cluster — self-marking complete on an empty `checks` table. One
      replica rewrote half of a derived identity pair and its peers did not. Measured: 127
      alert rows across `us-a`/`us-b`/`us-c` sharing an id with disagreeing bodies, which
      `upsert_alert` refuses forever because an alert is immutable.
  - [x] Delete the legacy check-identity migration subsystem (871 lines). Identity is
        cluster-shared state and a node may not rewrite it locally; the project is
        unreleased, so the supported answer is a rebuild. Recorded as the pre-1.0 database
        policy in `SCALING.md`.
  - [x] Stop the gossip fallback adopting a bare `alert_id` and pairing it with a locally
        resolved `check_id`. A peer now sends the whole immutable row or nothing, and the
        receiver mints its own content-derived id instead.
  - [x] Bound the amplification: a stream walked to its peer's tail keeps that tail rather
        than restarting from the beginning every sweep. A permanently divergent stream is a
        health alarm, not a reason to replay a peer's whole history — measured at 4 replays
        per 4 sweeps before, 1 after.

### Limits measured during the 10,000-check gate

- [x] Replicated dedup no longer scales with retained history: splitting the batch on the per-check high-water mark keeps the live path off the history table entirely (17 ms at 20M rows, 16 ms at 40M). Only repair reaches the anti-join.
- [x] Retention costs what it removes rather than what it keeps: the two full rebuilds after the delete are replaced by targeted maintenance (176 ms versus 7,552 ms on a daily window at 40M rows), and the delete is sliced across 24 jobs so the worker interleaves.
- [x] The global dashboard showed "Collecting latency data…" for every check: `/api/v1/uptime` and `/api/v1/latency-points` are fleet-wide *data* reads, but the global relay treated them as singleton config reads and answered from one region, which holds no results for checks its own agents do not probe. Both now fan out to every region and merge per check.
- [x] The full status page is 134.7 MB over 3 requests at 10,000 checks (p95 509 ms). Paginate or filter the dashboard; the monitoring cardinality is supportable, the single-page rendering of it is not. Fixed: `/api/v1/status`, `/api/v1/uptime`, and `/api/v1/latency-points` are bounded through `src/page.rs` (`MAX_PAGE_SIZE = 1,000`, shared by regional and global handlers), and `frontend/src/App.svelte` requests a 100-check page by default. Re-measured with `benchmark_status_and_global_push_produ...
- [x] `agent_checks_handler` runs `list_checks()` on the database worker for every agent poll, though the coordinator already caches check metadata in memory. Cost is `agents / poll_secs × O(checks)` on the one worker.
- [x] The notification dispatcher is a single serial loop with a 10 s per-send timeout, fed by a 1,024-slot channel whose `try_send` drops on overflow — a slow notifier delays every other alert, and an overflow loses alerts with no retry row written.

### Storage maintenance

- [x] Review and add appropriate physical organization/indexing for common queries. Evaluated, not assumed: benchmarked with DuckDB v1.5.4 (bundled by `duckdb = "1.10504.0"`) rather than adopting the "ART rebuilds at startup" folklore unread — reopen time is measured identical with or without an index, matching DuckDB's persisted/lazy-loaded ART storage. Decision for every candidate is not to add it; see SCALING.md "Indexing evaluated and rejected at measured scale" for the numbers and the thr...
  - [x] Evaluate an index or storage strategy for `check_results(check_id, ts_millis)`. Measured (`benchmark_check_results_check_id_ts_index_tradeoff`) at 80.64M rows: 1.7–2.6× the storage and 1.7–2.1× slower bulk writes, no reproducible `history`/`latency_series` benefit — DuckDB's automatic zone map on the monotonically-appended `ts_millis` already prunes the table before `check_id` is evaluated. Not added.
  - [x] Evaluate indexes for alert, incident, and state-history lookup patterns. `alerts(ts_millis)` and `incident_checks(check_id)` measured (`benchmark_alert_incident_lookup_index_tradeoff`) at a pessimistic 900K-alert/45K-incident seed: 1.5–2.1× slower bulk writes, no reproducible read benefit at that (already small, retention-bounded) scale. Neither added. `state_history` needs nothing added: its `PRIMARY KEY (check_id, since)` already leads with the only column every lookup against it fi...
  - [x] Confirm index behavior and write cost with the bundled DuckDB version. Confirmed empirically rather than assumed: reopen duration is unaffected by the presence of an index (lazy ART deserialization), while bulk-load and per-row write cost are measurably and reproducibly higher with one, on both benchmarks above.
- [x] Make retention and cleanup operations transactional where partial completion can expose inconsistent state.
- [x] Implement minute/hour latency rollups, query resolution switching, and raw/rollup boundary semantics before reducing raw retention. Rollups also now replicate on their own anti-entropy stream (digest + cursored backfill), so a peer behind raw retention can still recover summarized latency history for a window it holds no raw rows for.
- [x] Gate raw-result deletes on rollup coverage. Retention now pulls its horizon back to
      the start of the oldest minute that raw data claims and the rollups do not, so a
      failed rollup write stalls retention instead of destroying history nothing summarized.
      A stall is visible in the monitored database-size metric; silent loss is visible in
      nothing.
- [ ] Physical time-partitioning of `check_results` is deliberately **not** built. SCALING.md's
      own gate requires it only "when a full-scale retention run cannot drop one expiry unit
      within the maintenance budget", and the measured run is 176 ms sliced across 24 jobs.
      DuckDB has no native partitioning, so this would mean table-per-day behind a union view,
      touching every ingest, dedup, digest, backfill and query path. Promote this when a daily
      retention slice exceeds the maintenance budget or requires excessive temporary space.
- [x] Define retention-aware peer recovery for nodes behind raw retention: a standing gap is recorded durably (`raw_data_gaps`, admin-readable at `GET /api/v1/admin/raw-data-gaps`) rather than only logged and counted. No snapshot-restore tooling exists yet, so recovery today is the explicit-gap outcome, bounded to raw probe-level detail — the same window's rollups recover through their own replication (see above).
- [x] Run large retention deletes outside Tokio worker threads and avoid holding the main storage lock for the full operation (sliced across 24 database jobs; the worker already runs off the Tokio threads).
- [x] Add explicit checkpoint/file-size maintenance after large pruning operations if benchmarks show it is needed. Benchmarked (`benchmark_checkpoint_after_prune`) instead of assumed: at 1,000 checks/30 raw days, pruning one day (2.88M of 86.4M rows) leaves ~23 MB of WAL; an explicit `CHECKPOINT` costs 431 ms (over the single-worker slow-call threshold) and shrinks nothing — the file never gets smaller, and a second checkpoint is byte-identical to the first. DuckDB already applies the WAL aut...
- [x] Monitor logical row counts, DuckDB file size, prune duration, query duration, and database errors.
- [x] Cap route/list page sizes consistently. Three separate copies of the same 1,000 ceiling
      (`bounded_page_size`, `MAX_INCIDENT_PAGE_SIZE`, an ad-hoc `.min(1_000)` on the global
      relay) are now one `src/page.rs`. Defaults stay with the routes, since a status page and
      an incident list disagree about a sensible first screen; the ceiling does not.
  - [x] `/api/v1/admin/checks` and the agents route were genuinely unbounded. The check list is
        paged and the console pages to the end, so the array stays the contract while no single
        request can ask the one database worker for ten thousand documents.
  - [x] The agents route's orphaned/unrunnable lists are bounded by the catalogue, not the
        fleet: they now carry a count plus a capped sample.
- [x] Replace the correlated state-history prune query with an anti-join over one grouped pass.
      Measured first, and the premise turned out to be wrong: DuckDB decorrelates the old form,
      so on 3M spans the two measure 2.4 s against 2.2 s whether the rows are spread over 10,000
      checks or concentrated in 100 flapping ones. Kept for saying what it means, not for the
      10%. See `benchmark_state_history_prune`.
- [x] Create and test documented backup and restore procedures: `lighthouse backup --output <path>` and `lighthouse restore --from <backup> --into <path>` (`src/coordinator/backup.rs`, wired into `main.rs` beside the other run modes). Backup checkpoints the source, copies it plus its manifest, and reopens the copy to prove it is queryable before calling the job done; restore verifies the manifest checksum, refuses to write over an existing path, and reopens the result. Documented in SCALING.md's "Checkp...
- [x] Add restart/recovery tests using a persistent production-sized database. `restart_recovers_committed_rows_and_backup_restore_preserves_them` (fast, always-on) and `benchmark_restart_recovery_at_production_scale` (`#[ignore]`d, `LIGHTHOUSE_BENCH_*`-sized) in `src/coordinator/storage.rs` both close the database uncleanly via `std::mem::forget` — skipping DuckDB's own connection shutdown rather than a graceful `drop`, the closest an in-process test can safely get to a killed process — then re...

### TLS and probe behavior

- [x] Resolve the dead `allow_self_signed` option.
  - [x] Either implement a per-check HTTP TLS client honoring the option or remove the option from config and UI.
  - [x] Add tests for trusted, self-signed, expired, and hostname-mismatched certificates.
- [x] Document SSL certificate-check semantics.
  - [x] Explain that the expiry probe captures certificate dates without normal chain validation.
  - [x] Decide whether certificate trust and hostname validity should be independently reportable failures.
- [x] Document that monitor targets and notifier webhooks provide intentional server-side network access.
  - [x] State that admin access is network-level privilege.
  - [x] Consider optional private-network/allow-list policies for multi-tenant or hostile deployments.

### Authentication and secrets

- [x] Redact notifier secrets in admin read responses.
  - [x] Return placeholders or presence flags for SMTP passwords, routing keys, and webhook secrets.
  - [x] Preserve existing secrets unless an update explicitly replaces them.
- [x] Add explicit trusted-proxy support if per-client login rate limiting is required behind a reverse proxy. `trusted_proxies` (`[coordinator]`/`[global]`, bare IPs or CIDRs, empty by default) in `src/config.rs`; resolution lives in `crate::security::TrustedProxies`/`client_ip` (`src/security.rs`) rather than duplicating the trust notion `secure_cookies` already reserves for a forwarded-protocol decision this project has deliberately not added. When the immediate peer is trusted, `client_ip` r...
  - [x] Keep the current raw peer-IP behavior as the safe default.
  - [x] Document shared proxy rate-limit buckets until trusted-proxy support exists.
- [x] Document TLS termination and transport-security requirements for production deployments.
- [x] Confirm operational secret rotation procedures for admin, peer, global, and agent credentials.

### Frontend correctness

- [x] Prevent stale filter responses in `ProbeResultsAdmin.svelte`.
  - [x] Add cancellation or monotonically increasing request IDs.
- [x] Prevent overlapping/out-of-order polling in `ClusterAdmin.svelte`.
  - [x] Add an in-flight guard or schedule the next poll after the current one finishes.
- [x] Consolidate duplicate initial session checks in `App.svelte`.
- [x] Show acknowledgement failures to the operator instead of only logging them to the console.
- [x] Add an `aria-live` announcement for important live status changes.
- [x] Make "Past Incidents" respect the status page's selected time window.
  - [x] Symptom: selecting `24h` still lists incidents from 11 days ago. The range switch
        only drives the charts — `setDays()` in `App.svelte` calls `loadCharts()` and
        nothing else, and `fetchIncidents()` sends only `?limit=`.
  - [x] Add a `since` (epoch ms) parameter to `IncidentQuery` in
        `coordinator/routes/incidents.rs` and a bound to `Storage::list_incidents`, so the
        window is applied in SQL rather than by shipping full history to the browser. The
        global relay forwards the query string as-is, so it needs no change.
  - [x] Keep unresolved incidents visible regardless of `since`: the window bounds resolved
        incidents only, so an ongoing outage older than the window is never hidden from the
        status page.
  - [x] Refetch incidents from `setDays()`, and keep the admin console's list unwindowed —
        `App.svelte` currently feeds one `incidents` array to both `IncidentsList` (admin)
        and `PastIncidents` (public), so a windowed shared fetch would truncate the console
        too.
  - [x] Cover it with a test that a resolved incident outside the window is absent while an
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

Follow-ups implemented in P1:

- [x] Navigating from a section page into a check and back refetches that section's
      charts. Two bounded requests, not a fan-out, so it is a fair price for maps that
      hold exactly the current scope at the current window and nothing stale — but a
      last-loaded-scope guard would remove it if the round trip is ever felt.
- [x] A section page has no rollup banner of its own; it just repeats the primary page's
      per-check rows under a heading. The state is already computed and shipped, so this
      is presentation only.

## P2 — Maintainability and code reduction

### Split oversized backend modules

- [x] Split `src/coordinator/storage.rs` into focused modules.
  - [x] `storage/schema.rs`
  - [x] `storage/results.rs`
  - [x] `storage/checks.rs`
  - [x] `storage/incidents.rs`
  - [x] `storage/alerts.rs`
  - [x] `storage/config_replication.rs`
  - [x] `storage/fleet.rs`
  - [x] Move tests to their corresponding domains.
- [x] Split `src/coordinator/mod.rs` into focused modules.
  - [x] `coordinator/ingest.rs`
  - [x] `coordinator/replication.rs`
  - [x] `coordinator/dispatch.rs`
  - [x] `coordinator/rollup_tree.rs`
  - [x] `coordinator/router.rs`
- [x] Split `src/global/mod.rs` into routes, relay, merge, and asset-serving modules.
- [x] Split `src/config.rs` by runtime role if configuration continues to grow.
  Evaluated, not split: most of it is the shared `CheckConfig` wire format used by every role, and the role structs embed each other.
- [x] Split `src/agent.rs` into protocol-specific probe modules.
- [x] Extract shared coordinator/global HTTP utilities.
  - [x] Share error response types and helpers.
  - [x] Share static embedded-asset serving.
  - [x] Reconcile duplicated fleet/member DTOs where their semantics match.
    `SectionDto` merged into `storage::Section`. The fleet DTOs stay separate: the coordinator's version carries merge-ordered enums that the global tier does not need.
- [x] Replace repeated route-level `require_admin` boilerplate with an Axum request-parts extractor.

### Split oversized frontend modules

- [x] Split `CheckEditor.svelte` into an orchestrator and focused subcomponents.
  - [x] HTTP fields
  - [x] TCP fields
  - [x] DNS fields
  - [x] ICMP/ping fields
  - [x] SSL fields
  - [x] Heartbeat fields
  - [x] OpenMetrics fields
  - [x] Headers editor
  - [x] Placement editor
  - [x] Rollup-policy editor
  - [x] Dependency editor
  - [x] Evaluation tester
- [x] Split `AgentsAdmin.svelte` into focused panels/components.
- [x] Extract routing, session, and WebSocket/resync orchestration from `App.svelte`.
- [x] Split `frontend/src/lib/admin.ts` into per-domain API/type modules.
- [x] Review `RegionTreeAdmin.svelte`, `DayDetailModal.svelte`, `SectionsAdmin.svelte`, and `CheckDetail.svelte` for natural component boundaries.
  SectionsAdmin was split, and the probe-grid logic shared by CheckDetail and DayDetailModal now lives in `lib/probeGrid.ts`. The rest are single cohesive views.

### Dead and compatibility code

- [x] Remove or use the currently unused `sortChecks` helper and its supporting types/tests.
- [x] Remove the stale `#[allow(dead_code)]` from `CheckMeta::degraded_is_downtime` if it remains used.
- [x] Decide and document the pre-1.0 database compatibility policy.
  - [x] ~~If existing pre-UUID databases must be supported, …~~ Not supported.
  - [x] If they do not need support, delete the legacy migration subsystem and document the required one-time rebuild procedure.
  - [x] Avoid mixing implicit `ALTER TABLE` migrations with undocumented drop/rebuild expectations.
- [x] Review comments and design documentation for outdated anti-entropy guarantees.
- [x] Split large integration test files by feature area, especially `tests/admin_api.rs`.

## P2 — Production operations and validation

- [x] Add production-scale benchmarks and regression thresholds (`tests/release_benchmark.rs`, thresholds in SCALING.md).
  - [x] Sustained result ingest
  - [x] Public status latency
  - [x] 24-hour uptime calculation
  - [x] History and latency queries
  - [x] Anti-entropy reconciliation
  - [x] Retention cleanup
  - [x] Startup and recovery with a large persistent database
- [x] Add chaos and recovery tests (`tests/chaos.rs`; scenarios already covered elsewhere are referenced there, not duplicated).
  - [x] Peer unavailable during DOWN transition
  - [x] Peer unavailable through DOWN and recovery
  - [x] Duplicate/reordered replication delivery
  - [x] Coordinator restart during ingest
  - [x] Two-node partition healing
  - [x] Full cluster bisection healing
  - [x] Agent failover between coordinators
- [x] Add production observability.
  - [x] Database operation latency and lock wait time
  - [x] Ingest queue/backpressure
  - [x] Replication failures and reconciliation lag
  - [x] Notification retry/give-up counts
  - [x] Cluster membership transitions
  - [x] Retention duration and rows removed
- [x] Establish release gates: `scripts/release-gate.sh` (CI: `.github/workflows/release-gate.yml`, fast subset in `hk.pkl`).
  - [x] Clean `cargo check --all-targets`.
  - [x] Clean `cargo clippy --all-targets -- -D warnings`.
  - [x] Passing Rust and frontend tests.
  - [x] Passing frontend and Rust dependency audits.
  - [x] Successful production frontend and container builds.
  - [x] Backup/restore smoke test.
  - [x] Clean or intentionally documented working tree.
- [x] Break the current large working-tree change set into reviewable commits before release.

### P2 follow-ups

- [x] The config digest still full-scans the config tables on every sweep; back it with watermarks like the results/stream digests.
  Connection-local watermarks rebuilt on open and maintained transactionally per touched key; whole-kind and placement-scoped check digests are point reads.
- [x] Instrument global-tier reconciliation and region-push staleness (the coordinator-side metrics exist; the global tier exposes only DB and membership metrics).
- [x] `release_benchmark_three_peers_one_region_global_live_agents_and_retention` times out waiting for public check status on this machine; it predates P2.
  The benchmark expected 10,000 rows from a paginated endpoint; it now checks `total_checks` and pages for full coverage. Passes twice at 10,000 checks in Docker with `nofile=65536`.

## P3 — Storage robustness and scaling past 10,000 checks

Estimated per-region ceilings (SCALING.md capacity model, extrapolated beyond the measured 10,000): status reads are the first wall at roughly 30–50k checks, the full config read the second. Prove each item with the existing benchmarks at `LIGHTHOUSE_BENCH_CHECKS=30000` and `100000`.

### DuckDB operational safety

- [x] Exit on database invalidation. A DuckDB fatal error ("database has been invalidated") currently leaves the coordinator serving 500s indefinitely; detect it in `Database` and exit non-zero so the restart policy recovers and peers backfill the gap.
  `database::is_invalidated` matches `FATAL Error`/`database has been invalidated` on any job error; the worker logs and exits 1.
- [x] Bound DuckDB memory and threads, container-aware.
  - [x] Derive defaults from the cgroup limit (v2 `memory.max`/`cpu.max`, v1 `memory.limit_in_bytes`/`cpu.cfs_quota_us`), falling back to host totals; apply a fraction of memory to `memory_limit`, leaving headroom for the process itself.
    Half of `min(cgroup, MemTotal)`; threads = quota rounded up, else `available_parallelism` (`coordinator::db_limits`).
  - [x] Allow an explicit override in config and env so Kubernetes manifests can pin it alongside `resources.limits`.
    `[coordinator.database]`/`[global.database]` `memory_limit`/`threads`, or `LIGHTHOUSE_DB_MEMORY_LIMIT`/`LIGHTHOUSE_DB_THREADS`.
  - [x] Log the effective values at startup; test the cgroup parsing against fixture files.
- [x] Replace delete-then-reinsert upserts (`upsert_check`, `put_setting`, rollup policies, watermark tables) with `INSERT … ON CONFLICT DO UPDATE`, avoiding DuckDB's history of ART-index faults on same-key delete/insert within one transaction.
  Also config documents, tombstones, replicated incidents, placement regions and the watermark rebuilds; set-valued children delete only vanished keys.

### Read path

- [x] Page and filter status in SQL. The status handler builds every check's status before slicing the page (`routes/public.rs`), so cost is O(checks) per request regardless of page size.
  Scope/order/paging in SQL; per-check work joins only the staged page (status, strips, latency points); section tiles are one `GROUP BY`. Regional status p95: 157 → 21 ms at 30k checks, 43 ms at 100k.
- [x] Page the full config read (`config_entries`) for provisioning and repair so it never holds the database worker for seconds at 100k checks.
  The pull was paged; check documents are now projected in the page query, a delete's tombstone is a point read, and the unpaged read is test-only.
- [x] Add read-only connections alongside the single writer so reads stop queueing behind ingest.
  - [x] Prerequisite: config watermarks are connection-local TEMP tables (P2). Either keep digest reads on the writer connection or move watermarks to persistent derived tables first.
  - [x] Measure after the two items above; skip if SQL-side paging alone removes the ceiling.
    Skipped by measurement: 43 ms p95 status and 101 ms full page at 100k checks (SCALING.md "Read path").

### Transport

- [x] Compress inter-node HTTP (replication, anti-entropy, global push, agent batches). `tower-http` and `flate2` are already in `Cargo.lock`; prefer zstd or gzip negotiated by `Content-Encoding`, and measure bytes and CPU at 10k checks.
  zstd both directions (`http::compressed`, `ZstdJson`). 10k-check snapshot 2.30 MB → 277 KB.
- [x] Make the region-to-global push incremental: send changed check summaries since the last acknowledged push, with a periodic full snapshot as the repair path. Today every push resends ~2 MB per region at 10k checks.
  Lowest-id live publisher per region; `epoch`/`seq`/`delta_base` chain with receiver `resync`; snapshot every 12th push. Steady-state delta: ~170 B.
- [x] Stop resending unchanged fleet-presence snapshots.
  Sent/persisted only on a material change or after a third of the agent's silence window.
- [x] Re-evaluate protobuf/gRPC only after compression and deltas, and only if transport is still measured as material (SCALING.md "Scaling milestones").
  Not material (~170 B per region per push in steady state); not adopted.

### Raw-result storage evolution

- [ ] Move sealed days of raw results to per-day Parquet files read through DuckDB, so retention becomes deleting a file instead of a large `DELETE`, and current-day writes stay in the live table.
  Measured, not required yet: ~76 ns per deleted row, and five-minute retention slices stay ≈230 ms at 100k checks × 3 domains. Volume (~390 GB per replica at that scale) is the binding cost, and Parquet would not change it materially (SCALING.md "Raw-result storage evolution").
- [ ] Optional: ship sealed Parquet days to S3-compatible storage as the archive instead of backing up the live database.
  Skipped with the Parquet item it depends on.
- [x] Reframe backup around what is irreplaceable: the config catalogue (checks, sections, settings, credentials, policies). Raw metrics are not backed up; the existing `lighthouse backup` CLI remains for full-node recovery.
  `lighthouse backup --catalogue --output <json>` / `restore --catalogue --from <json>` over the paged config plane, online, HLC-applied.

### P3 follow-ups

- [ ] The global node aggregates every region's checks per `/api/v1/status` request: 241 ms p95 at 100k checks × 3 regions.
- [ ] Building the global push summary is an O(checks) database job per push interval on the publisher: 396 ms every 5 s at 100k checks.

## Existing product issues

- [ ] Convert global coordinator credentials to per-region credentials used to attach regional
      coordinators to the global coordinator. Today a region is static `[[global.region]]` config
      on the global node and a shared token; it cannot check in. The target is the agent model:
      a region holds a registration key and attaches itself to whichever global node exists,
      so regions are discovered rather than declared. Phase 1's durable global store is the
      prerequisite that now exists — a registering region has somewhere authoritative to land.
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