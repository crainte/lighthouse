use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tokio::sync::mpsc;
use warden::agent;
use warden::config::Config;
use warden::coordinator::Coordinator;
use warden::global::GlobalNode;
use warden::version;

/// Warden — self-hosted uptime monitor.
#[derive(Parser)]
// `--version` carries the sha and build time too, because the version alone
// cannot distinguish two deployments of an unreleased crate.
#[command(name = "warden", version, long_version = build_banner())]
struct Cli {
    /// Run mode.
    #[arg(value_enum)]
    mode: Mode,
    /// Path to config file (TOML).
    #[arg(short, long, default_value = "warden.toml")]
    config: PathBuf,
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
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Standalone => "standalone",
            Mode::Agent => "agent",
            Mode::Coordinator => "coordinator",
            Mode::Global => "global",
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
        "warden starting"
    );
    let cfg = Config::load(&cli.config)?;

    match cli.mode {
        Mode::Standalone => run_standalone(cfg).await,
        Mode::Agent => run_agent(cfg).await,
        Mode::Coordinator => run_coordinator(cfg).await,
        Mode::Global => run_global(cfg).await,
    }
}

/// Standalone: agent produces results into an in-process channel that
/// the coordinator drains directly — no HTTP hop.
async fn run_standalone(mut cfg: Config) -> Result<()> {
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
    tokio::spawn(agent::sync_loop(cfg.agent.clone(), tx, Vec::new()));
    serve(coord, &cfg.coordinator.listen).await
}

async fn run_agent(cfg: Config) -> Result<()> {
    let (tx, rx) = mpsc::channel(1024);
    tokio::spawn(agent::sync_loop(cfg.agent.clone(), tx, Vec::new()));
    agent::poster_loop(cfg.agent, rx).await;
    Ok(())
}

async fn run_coordinator(cfg: Config) -> Result<()> {
    cfg.coordinator.validate_cluster()?;
    let coord = Coordinator::new(&cfg.coordinator).await?;
    serve(coord, &cfg.coordinator.listen).await
}

/// Global: no storage, no probing. Caches state summaries pushed by regional
/// coordinators and serves the aggregated dashboard.
async fn run_global(cfg: Config) -> Result<()> {
    cfg.global.validate()?;
    let node = GlobalNode::new(&cfg.global);
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
