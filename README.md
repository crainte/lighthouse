<h1 align="center">Lighthouse</h1>

<p align="center">
  <b>A self-hosted, hierarchical uptime monitor in one Rust binary.</b><br>
  Distributed probes. DuckDB storage. Lua-scripted checks. An embedded Svelte admin UI.
</p>

---

Lighthouse probes your services from agents in as many regions as you like,
evaluates the results in regional coordinators, and rolls the state up to a
global view. It is meant to replace a stack of Uptime Kuma, Kener and a
Prometheus blackbox setup with a single artifact that has no mandatory external
dependencies: storage is embedded DuckDB, and the management UI,
**Lightkeeper**, is compiled into the binary.

It is pre-1.0. Read [Status and limitations](#status-and-limitations) before you
depend on it.

## What it does

**Checks**
- HTTP, TCP, DNS, ICMP and TLS certificate (SSL) expiry probes
- **Heartbeat** checks, inverted: something external pings a generated URL, and the check goes DOWN when the pings stop
- **OpenMetrics** checks: scrape a Prometheus/OpenMetrics exporter, select one sample by name and labels, and evaluate it
- Optional **Lua** evaluation of a probe result to decide up or down
- Placement policies decide which agents, zones and regions run each check

**Alerting**
- Alerts and incidents with a publication delay so a single failed probe does not page anyone
- Maintenance windows
- Notifications by webhook, ntfy, Slack, PagerDuty and email (SMTP, with STARTTLS or implicit TLS)

**Visibility**
- Public status pages with uptime strips and latency sparklines
- A region tree (global, region, zone, agent) with rolled-up state at every level
- An OpenMetrics endpoint so Prometheus can scrape Lighthouse itself

**Operations**
- Regional coordinator clusters that gossip membership and replicate configuration and results, with agent failover between coordinators
- A global tier that aggregates state across regions and holds no raw data
- Checkpointed backup and restore of the DuckDB file
- Build stamping: `lighthouse --version` reports the git sha and build time, so a stale deployment is easy to spot

## Architecture

```
                     ┌─────────────────────────────┐
                     │   Global coordinators       │  aggregates region state,
                     │   (lighthouse global)       │  serves the global dashboard
                     └──────────────▲──────────────┘
                                    │ state pushes
          ┌─────────────────────────┴─────────────────────────┐
┌─────────┴──────────┐                              ┌─────────┴──────────┐
│ Regional cluster   │                              │ Regional cluster   │
│ coordinator × 3    │  gossip + replication        │ coordinator × 3    │
│ DuckDB, alerting,  │  between peers               │                    │
│ Lightkeeper UI     │                              │                    │
└─────────▲──────────┘                              └─────────▲──────────┘
          │ results ↑   checks ↓                              │
   ┌──────┴──────┐                                     ┌──────┴──────┐
   │ agents      │  probe HTTP / TCP / DNS / ICMP / …  │ agents      │
   └─────────────┘                                     └─────────────┘
```

**Agents** are deliberately simple. They poll a coordinator for their checks,
run the probes, and post raw results back. They hold no state worth keeping.

**Coordinators** own a region. They ingest results, store them in DuckDB,
evaluate them (including Lua), run the alert and incident logic, send
notifications and serve the admin UI and API. A region normally runs three
peers that replicate to each other.

**Global coordinators** receive summaries from each region, serve the combined
dashboard, and route drill-downs to the owning region.

One binary does all of it. The first argument selects the mode:

| Mode | What it runs |
|---|---|
| `standalone` | Coordinator and local agent in one process. The small-deployment default. |
| `agent` | Probes only; posts to one or more remote coordinators. |
| `coordinator` | Ingest, storage, alerting, API and UI for a region. |
| `global` | Cross-region aggregator. Holds no raw data. |
| `backup` | Checkpoints `coordinator.db_path` and copies it to `--output`. Run against a stopped node. |
| `restore` | Restores a backup into a fresh path with `--from` and `--into`. Never restores over a live database. |

`backup` and `restore` also take `--catalogue` to export and import a running
node's configuration (checks, sections, settings, credentials, policies) as JSON.

## Quick start

### Docker Compose

```sh
LIGHTHOUSE_ADMIN_PASSWORD=change-me docker compose up --build
```

Open <http://localhost:9000> (override with `LIGHTHOUSE_PORT`). This runs a
single standalone node. Without `LIGHTHOUSE_ADMIN_PASSWORD` the check editor is
disabled rather than left open.

### From source

Lightkeeper is embedded into the binary, so build the frontend first:

```sh
cd frontend && bun install --frozen-lockfile && bun run build && cd ..
cargo build --release
```

Then run it with a config file:

```sh
cp lighthouse.example.toml lighthouse.toml
LIGHTHOUSE_ADMIN_PASSWORD=change-me \
  ./target/release/lighthouse standalone --config lighthouse.toml
```

Checks are not seeded from the config file. Create them in Lightkeeper or
through the admin API; they are stored in the database. Secrets belong in
`LIGHTHOUSE_*` environment variables, not in the TOML. [`lighthouse.example.toml`](lighthouse.example.toml)
documents every setting.

ICMP probes need `CAP_NET_RAW` (the compose files add it).

## Deployment examples

| File | Topology |
|---|---|
| [`docker-compose.yaml`](docker-compose.yaml) | One standalone node. |
| [`docker-compose.cluster.yaml`](docker-compose.cluster.yaml) | A three-coordinator regional cluster with agents. |
| [`docker-compose.global.yaml`](docker-compose.global.yaml) | Several regions plus a three-node global tier. |

The cluster and global examples need the `LIGHTHOUSE_*` token variables
described at the top of each file. Released images are published to
`ghcr.io/crainte/lighthouse`.

## Development

Tools are pinned in [`mise.toml`](mise.toml); quality gates are defined once in
[`hk.pkl`](hk.pkl).

```sh
mise install            # hk, git-cliff, actionlint, bun
hk install              # git hooks: fast checks on commit, full Rust tests on push
hk check                # fmt, clippy, unit tests, svelte-check, vitest
hk check --all --slow   # everything CI runs
```

Commit messages must be [conventional commits](https://www.conventionalcommits.org);
the `commit-msg` hook enforces it, and the changelog and version are derived from them.

The frontend lives in `frontend/` (Svelte, Vite, Vitest). `bun run dev` serves it
with hot reload.

A backup/restore smoke test that drives the real binary is available locally
and is not part of CI:

```sh
hk check -p smoke -S backup-restore-smoke
```

### CI and releases

[`ci.yml`](.github/workflows/ci.yml) runs one job per gate, each a single step
from `hk.pkl`, so a red check names its cause: `fmt`, `clippy`, `test`,
`svelte-check`, `vitest`, `frontend-build`, `cargo-audit`, `bun-audit`,
`docker-build` and `actionlint`. To reproduce one locally:

```sh
hk check --all --slow -S clippy
```

Releases are two steps:

1. Run the **Release PR** workflow. git-cliff derives the next version from the
   conventional commits (or you pick one), the workflow bumps `Cargo.toml`,
   `Cargo.lock` and `frontend/package.json`, regenerates `CHANGELOG.md`, and opens a PR.
2. Merge it. The **Release** workflow sees a changelog entry with no tag, runs CI,
   creates the GitHub release and publishes the image to GHCR.

### Layout

```
src/
  agent/         probes (http, tcp, dns, icmp, ssl, openmetrics) and the agent loop
  coordinator/   ingest, storage, replication, gossip, clustering, backup, routing
  global/        cross-region aggregation, incidents, region tree
  notify.rs      webhook, ntfy, Slack, PagerDuty, email
  script.rs      Lua evaluation
  config.rs      configuration and LIGHTHOUSE_* environment handling
frontend/        Lightkeeper (Svelte), embedded into the binary at build time
tests/           integration suites: cluster, gossip, replication, chaos, benchmarks
```

## Status and limitations

- **Pre-1.0.** There are no in-place schema or identity migrations: a database
  written by an older build is rebuilt, not migrated. Wire formats and storage
  shapes can still change.
- **Single DuckDB writer per node.** Scale comes from regional clusters and
  agents, not from one big database. The measured capacity envelope for a
  10,000-check deployment is in [`SCALING.md`](SCALING.md).
- **Backups are offline.** `backup` expects a stopped coordinator or an idle replica.
- **Raw results live in the main database.** Time-partitioned or Parquet-archived
  raw storage is not built yet.
- **At very large scale** (100k+ checks) the global tier recomputes the status
  summary per request and regional publishers rebuild theirs per interval; both
  are known and measured, not fixed.

The open work is tracked in [`TODO.md`](TODO.md).

## Further reading

- [`uptime-monitor-design.md`](uptime-monitor-design.md): architecture, components, data model and API
- [`SCALING.md`](SCALING.md): capacity model, replication behaviour and benchmark results
- [`lighthouse.example.toml`](lighthouse.example.toml): every configuration option
- [`CHANGELOG.md`](CHANGELOG.md)
