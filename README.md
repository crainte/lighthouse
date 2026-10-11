# Lighthouse

Lighthouse is a distributed uptime monitor. It runs probes through regional
agents, stores and evaluates results in regional coordinators, and can combine
regional state through a global coordinator. **Lightkeeper** is Lighthouse's
embedded management UI for administering checks, credentials, notifications,
and status views.

## Modes

The `lighthouse` binary supports these deployment modes:

- `standalone`: a coordinator and local agent in one process for a small deployment.
- `agent`: probes checks and posts results to regional coordinators.
- `coordinator`: serves a regional coordinator, API, and Lightkeeper.
- `global`: aggregates state from multiple regional coordinators.
- `backup` and `restore`: create and verify checkpointed DuckDB backups.

## Quick start

Build and run a standalone instance:

```sh
cargo build --release --bin lighthouse
./target/release/lighthouse standalone --config lighthouse.example.toml
```

The example configuration uses `lighthouse.duckdb` by default. Copy it before
using it for an installation, and supply secrets through `LIGHTHOUSE_*`
environment variables rather than committing them to configuration.

```sh
cp lighthouse.example.toml lighthouse.toml
LIGHTHOUSE_ADMIN_PASSWORD=change-me \
  lighthouse standalone --config lighthouse.toml
```

For a containerized standalone deployment:

```sh
docker compose up --build
```

Use `docker-compose.cluster.yaml` for the Stage A regional cluster example and
`docker-compose.global.yaml` for the Stage B multi-region/global example.
Those examples require their documented `LIGHTHOUSE_*` token variables.

## Further reading

- [Configuration example](lighthouse.example.toml)
- [Architecture and API design](uptime-monitor-design.md)
- [Scaling and operations](SCALING.md)
- [Production readiness work](TODO.md)
- [Development handoff](HANDOFF.md)
