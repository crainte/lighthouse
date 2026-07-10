use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tokio::sync::mpsc;
use warden::agent;
use warden::config::Config;
use warden::coordinator::Coordinator;

/// Warden — self-hosted uptime monitor.
#[derive(Parser)]
#[command(name = "warden", version)]
struct Cli {
    /// Run mode.
    #[arg(value_enum)]
    mode: Mode,
    /// Path to config file (TOML).
    #[arg(short, long, default_value = "warden.toml")]
    config: PathBuf,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Mode {
    /// Agent + coordinator in one process.
    Standalone,
    /// Checks only; posts to a remote coordinator.
    Agent,
    /// Ingest + storage + alerting only.
    Coordinator,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;

    match cli.mode {
        Mode::Standalone => run_standalone(cfg).await,
        Mode::Agent => run_agent(cfg).await,
        Mode::Coordinator => run_coordinator(cfg).await,
    }
}

/// Standalone: agent produces results into an in-process channel that
/// the coordinator drains directly — no HTTP hop.
async fn run_standalone(cfg: Config) -> Result<()> {
    let coord = Coordinator::new(&cfg.coordinator)?;
    let (tx, mut rx) = mpsc::channel(1024);
    agent::spawn_check_loops(&cfg.agent, cfg.checks, tx);

    let ingest = coord.clone();
    tokio::spawn(async move {
        while let Some(result) = rx.recv().await {
            ingest.ingest(vec![result]).await;
        }
    });

    serve(coord, &cfg.coordinator.listen).await
}

async fn run_agent(cfg: Config) -> Result<()> {
    let (tx, rx) = mpsc::channel(1024);
    agent::spawn_check_loops(&cfg.agent, cfg.checks, tx);
    agent::poster_loop(cfg.agent, rx).await;
    Ok(())
}

async fn run_coordinator(cfg: Config) -> Result<()> {
    let coord = Coordinator::new(&cfg.coordinator)?;
    serve(coord, &cfg.coordinator.listen).await
}

async fn serve(coord: Coordinator, listen: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "coordinator listening");
    axum::serve(listener, coord.router()).await?;
    Ok(())
}
