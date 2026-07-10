use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub checks: Vec<CheckConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    /// Where to POST results. Ignored in standalone (writes in-process).
    #[serde(default = "default_coordinator_url")]
    pub coordinator_url: String,
    #[serde(default = "default_agent_id")]
    pub agent_id: String,
    /// Flush interval for batched result posting.
    #[serde(default = "default_flush_secs")]
    pub flush_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoordinatorConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    /// Webhook URL for alerts (Phase 1: webhook only).
    #[serde(default)]
    pub alert_webhook: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CheckConfig {
    pub name: String,
    #[serde(default = "default_check_type")]
    pub r#type: String,
    pub target: String,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

impl CheckConfig {
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs)
    }
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config = toml::from_str(&text).context("parsing config TOML")?;
        Ok(cfg)
    }
}

fn default_coordinator_url() -> String {
    "http://127.0.0.1:8080".into()
}
fn default_agent_id() -> String {
    "agent-01".into()
}
fn default_flush_secs() -> u64 {
    5
}
fn default_listen() -> String {
    "0.0.0.0:8080".into()
}
fn default_db_path() -> String {
    "warden.duckdb".into()
}
fn default_check_type() -> String {
    "http".into()
}
fn default_interval_secs() -> u64 {
    60
}
fn default_timeout_secs() -> u64 {
    10
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            coordinator_url: default_coordinator_url(),
            agent_id: default_agent_id(),
            flush_secs: default_flush_secs(),
        }
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            db_path: default_db_path(),
            alert_webhook: None,
        }
    }
}
