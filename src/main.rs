use anyhow::Result;
use clap::Parser;
use lighthouse::agent;
use lighthouse::config::Config;
use lighthouse::coordinator::Coordinator;
use lighthouse::global::GlobalNode;
use lighthouse::version;
use std::path::PathBuf;
use tokio::sync::mpsc;

/// Lighthouse — self-hosted uptime monitor.
#[derive(Parser)]
// `--version` carries the sha and build time too, because the version alone
// cannot distinguish two deployments of an unreleased crate.
#[command(name = "lighthouse", version, long_version = build_banner())]
struct Cli {
    /// Run mode.
    #[arg(value_enum)]
    mode: Mode,
    /// Path to config file (TOML). Read for every mode but `restore`, which
    /// takes its paths explicitly because the whole point is never to touch
    /// a configured, possibly-live database.
    #[arg(short, long, default_value = "lighthouse.toml")]
    config: PathBuf,
    /// `backup` only: where to write the checkpointed snapshot. Must not
    /// already exist — a backup generation is immutable once written.
    #[arg(long)]
    output: Option<PathBuf>,
    /// `restore` only: the backup file to restore from.
    #[arg(long)]
    from: Option<PathBuf>,
    /// `restore` only: the fresh path to restore into. Must not already
    /// exist; restore into a replacement location, never over a live database.
    #[arg(long)]
    into: Option<PathBuf>,
    /// `backup`/`restore`: operate on the config catalogue (checks, sections,
    /// settings, credentials, policies) of a *running* node over its peer
    /// API instead of on the database file. `backup --catalogue --output
    /// <file.json>`; `restore --catalogue --from <file.json>`.
    #[arg(long)]
    catalogue: bool,
    /// `--catalogue` only: the node's base URL. Defaults to
    /// `coordinator.advertise_url`, else `http://127.0.0.1:<listen port>`.
    #[arg(long)]
    url: Option<String>,
}

/// `--version` output. Leaked because clap wants a `&'static str` and the
/// value lives as long as the process anyway.
fn build_banner() -> &'static str {
    Box::leak(version::current().to_string().into_boxed_str())
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Mode {
    /// Agent + coordinator in one process.
    Standalone,
    /// Checks only; posts to a remote coordinator.
    Agent,
    /// Ingest + storage + alerting only.
    Coordinator,
    /// Cross-region aggregator: caches region state pushes, serves the global
    /// dashboard, routes drill-downs to regions. Holds no raw data.
    Global,
    /// Checkpoint `coordinator.db_path` and copy it to `--output`. Run this
    /// against a stopped coordinator (or a replica not otherwise in use):
    /// DuckDB allows one writer, so this either gets an exclusive view of the
    /// file or fails rather than racing live writes.
    Backup,
    /// Copy `--from` into `--into` and verify it opens and reads cleanly.
    /// Never writes over an existing path.
    Restore,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Standalone => "standalone",
            Mode::Agent => "agent",
            Mode::Coordinator => "coordinator",
            Mode::Global => "global",
            Mode::Backup => "backup",
            Mode::Restore => "restore",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    // First line of every log: the question asked of a misbehaving node is
    // almost always "which build is that?", and the answer should already be
    // in the logs the operator is holding.
    let build = version::current();
    tracing::info!(
        version = %build.version,
        git_sha = %build.git_sha,
        built_at = %build.built_at.to_rfc3339(),
        mode = cli.mode.as_str(),
        "lighthouse starting"
    );
    let cfg = Config::load(&cli.config)?;

    match cli.mode {
        Mode::Standalone => run_standalone(cfg).await,
        Mode::Agent => run_agent(cfg).await,
        Mode::Coordinator => run_coordinator(cfg).await,
        Mode::Global => run_global(cfg).await,
        Mode::Backup if cli.catalogue => run_catalogue_backup(&cfg, cli.url, cli.output).await,
        Mode::Restore if cli.catalogue => run_catalogue_restore(&cfg, cli.url, cli.from).await,
        Mode::Backup => run_backup(&cfg, cli.output),
        Mode::Restore => run_restore(cli.from, cli.into),
    }
}

/// `lighthouse backup --output <path>`: checkpoint `coordinator.db_path` and
/// copy it there. Synchronous — there is no server to run alongside it, and
/// the whole operation is expected to complete and exit.
fn run_backup(cfg: &Config, output: Option<PathBuf>) -> Result<()> {
    let output = output.ok_or_else(|| anyhow::anyhow!("backup mode requires --output <path>"))?;
    let manifest = lighthouse::coordinator::backup::create(&cfg.coordinator.db_path, &output)?;
    tracing::info!(
        source = %manifest.source_path,
        source_node = %manifest.source_node,
        destination = %output.display(),
        database_bytes = manifest.database_bytes,
        sha256 = %manifest.sha256,
        "backup complete"
    );
    println!(
        "backup written to {} ({} bytes, sha256 {})",
        output.display(),
        manifest.database_bytes,
        manifest.sha256
    );
    Ok(())
}

/// `lighthouse restore --from <backup> --into <fresh path>`: copy and verify,
/// never opening or touching a live database. Reports what it found so an
/// operator can compare it against SCALING.md's restore checklist before
/// putting the result into service.
fn run_restore(from: Option<PathBuf>, into: Option<PathBuf>) -> Result<()> {
    let from = from.ok_or_else(|| anyhow::anyhow!("restore mode requires --from <backup path>"))?;
    let into = into.ok_or_else(|| anyhow::anyhow!("restore mode requires --into <fresh path>"))?;
    let report = lighthouse::coordinator::backup::restore(&from, &into)?;
    tracing::info!(
        destination = %into.display(),
        checksum_verified = report.checksum_verified,
        raw_results = report.raw_results,
        current_states = report.current_states,
        oldest_result_ts = ?report.oldest_result_ts,
        "restore complete"
    );
    println!(
        "restored to {} (checksum_verified={}, raw_results={}, current_states={}, oldest_result_ts={:?})",
        into.display(),
        report.checksum_verified,
        report.raw_results,
        report.current_states,
        report.oldest_result_ts
    );
    Ok(())
}

/// The running node a catalogue operation talks to, and the peer token it
/// presents.
fn catalogue_target(cfg: &Config, url: Option<String>) -> (String, Option<String>) {
    let url = url
        .or_else(|| cfg.coordinator.resolved_advertise_url())
        .unwrap_or_else(|| {
            let port = cfg.coordinator.listen.rsplit(':').next().unwrap_or("8080");
            format!("http://127.0.0.1:{port}")
        });
    (url, cfg.coordinator.resolved_peer_token())
}

/// `lighthouse backup --catalogue --output <file.json>`: export the config
/// catalogue from a running node, with no downtime.
async fn run_catalogue_backup(
    cfg: &Config,
    url: Option<String>,
    output: Option<PathBuf>,
) -> Result<()> {
    let output = output.ok_or_else(|| anyhow::anyhow!("backup mode requires --output <path>"))?;
    let (url, token) = catalogue_target(cfg, url);
    let catalogue =
        lighthouse::coordinator::backup::export_catalogue(&url, token.as_deref()).await?;
    lighthouse::coordinator::backup::write_catalogue(&catalogue, &output)?;
    println!(
        "catalogue from {url} written to {} ({} entries)",
        output.display(),
        catalogue.entries.len()
    );
    Ok(())
}

/// `lighthouse restore --catalogue --from <file.json>`: apply a catalogue to
/// a running node by HLC; its peers converge through anti-entropy.
async fn run_catalogue_restore(
    cfg: &Config,
    url: Option<String>,
    from: Option<PathBuf>,
) -> Result<()> {
    let from =
        from.ok_or_else(|| anyhow::anyhow!("restore mode requires --from <catalogue path>"))?;
    let (url, token) = catalogue_target(cfg, url);
    let catalogue = lighthouse::coordinator::backup::read_catalogue(&from)?;
    let applied =
        lighthouse::coordinator::backup::import_catalogue(&url, token.as_deref(), &catalogue)
            .await?;
    println!(
        "catalogue {} applied to {url} ({applied} entries)",
        from.display()
    );
    Ok(())
}

/// Standalone: agent produces results into an in-process channel that
/// the coordinator drains directly — no HTTP hop.
async fn run_standalone(mut cfg: Config) -> Result<()> {
    cfg.agent.validate()?;
    cfg.coordinator.validate_cluster()?;
    let coord = Coordinator::new(&cfg.coordinator).await?;
    let (tx, mut rx) = mpsc::channel(1024);

    let ingest = coord.clone();
    tokio::spawn(async move {
        while let Some(result) = rx.recv().await {
            ingest.ingest(vec![result]).await;
        }
    });

    let heartbeat = coord.clone();
    let embedded_agent = cfg.agent.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            embedded_agent.poll_secs.max(1),
        ));
        loop {
            interval.tick().await;
            heartbeat.mark_embedded_agent_live(&embedded_agent);
        }
    });

    let port = cfg
        .coordinator
        .listen
        .rsplit_once(':')
        .map_or("8080", |(_, port)| port);
    cfg.agent.coordinator_url = vec![format!("http://127.0.0.1:{port}")];
    tokio::spawn(agent::sync_loop(cfg.agent.clone(), tx));
    serve(coord, &cfg.coordinator.listen).await
}

async fn run_agent(cfg: Config) -> Result<()> {
    cfg.agent.validate()?;
    let (tx, rx) = mpsc::channel(1024);
    tokio::spawn(agent::sync_loop(cfg.agent.clone(), tx));
    agent::poster_loop(cfg.agent, rx).await;
    Ok(())
}

async fn run_coordinator(cfg: Config) -> Result<()> {
    cfg.coordinator.validate_cluster()?;
    let coord = Coordinator::new(&cfg.coordinator).await?;
    serve(coord, &cfg.coordinator.listen).await
}

/// Global: no probing. Holds the catalogue durably (SCALING.md
/// "Placement-scoped configuration") and caches state summaries pushed by
/// regional coordinators; serves the aggregated dashboard.
async fn run_global(cfg: Config) -> Result<()> {
    cfg.global.validate()?;
    let node = GlobalNode::new(&cfg.global).await?;
    // Membership heartbeat and catalogue anti-entropy against peer global
    // nodes, plus catalogue reconciliation against every known region. A
    // no-op loop where clustering or regions are not configured, like the
    // coordinator's own `spawn_cluster`.
    node.spawn_background();
    let listen = cfg.global.listen.clone();
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(%listen, "global coordinator listening");
    axum::serve(
        listener,
        node.router()
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn serve(coord: Coordinator, listen: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "coordinator listening");
    // History pruning runs beside the server: once at startup, then daily.
    coord.spawn_retention();
    // Notifications that failed on the first try are retried on their own
    // schedule; without this the queue would fill and never drain.
    coord.spawn_retries();
    // Membership heartbeat and anti-entropy against peer coordinators. A
    // no-op when clustering is not configured.
    coord.spawn_cluster();
    // Push this region's state summary to the global coordinator, when one is
    // configured. A no-op otherwise, like the cluster loops above.
    coord.spawn_global_push();
    // Persist who is polling this node and share it with the peers, so every
    // coordinator reports the same fleet. Runs standalone too: it is also what
    // makes the fleet view survive a restart.
    coord.spawn_fleet_sync();
    // Peer address is needed to rate-limit login attempts per client.
    axum::serve(
        listener,
        coord
            .router()
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
