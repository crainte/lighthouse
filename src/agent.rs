use crate::config::{AgentConfig, CheckConfig};
use crate::models::{CheckResult, ResultBatch};
use chrono::Utc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Run one check against its target, producing a raw CheckResult.
/// Phase 1: HTTP only. TCP/DNS/ICMP land in Phase 3.
pub async fn execute_check(
    client: &reqwest::Client,
    agent_id: &str,
    check: &CheckConfig,
) -> CheckResult {
    let start = Instant::now();
    let (status_code, error) = match client
        .get(&check.target)
        .timeout(check.timeout())
        .send()
        .await
    {
        Ok(resp) => (Some(resp.status().as_u16()), None),
        Err(e) => {
            let msg = if e.is_timeout() {
                "timeout".to_string()
            } else {
                e.to_string()
            };
            (None, Some(msg))
        }
    };

    CheckResult {
        check_id: check.name.clone(),
        agent_id: agent_id.to_string(),
        timestamp: Utc::now(),
        latency_ms: start.elapsed().as_millis() as u64,
        status_code,
        error,
    }
}

/// Spawn one loop per check, each on its own interval so a slow check
/// never delays a sibling. Results flow into `tx`.
pub fn spawn_check_loops(
    cfg: &AgentConfig,
    checks: Vec<CheckConfig>,
    tx: mpsc::Sender<CheckResult>,
) {
    let client = reqwest::Client::new();
    for check in checks {
        let client = client.clone();
        let agent_id = cfg.agent_id.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check.interval());
            loop {
                interval.tick().await;
                let result = execute_check(&client, &agent_id, &check).await;
                debug!(check = %check.name, latency = result.latency_ms, "check done");
                if tx.send(result).await.is_err() {
                    break; // receiver gone, coordinator shutting down
                }
            }
        });
    }
}

/// Batch results and POST them to a remote coordinator (agent mode).
/// Standalone mode skips this and consumes the channel directly.
pub async fn poster_loop(cfg: AgentConfig, mut rx: mpsc::Receiver<CheckResult>) {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/results", cfg.coordinator_url);
    let mut batch = Vec::new();
    let mut flush = tokio::time::interval(std::time::Duration::from_secs(cfg.flush_secs));

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(r) => batch.push(r),
                None => break, // producers gone
            },
            _ = flush.tick() => {
                if batch.is_empty() { continue; }
                let body = ResultBatch { results: std::mem::take(&mut batch) };
                if let Err(e) = client.post(&url).json(&body).send().await {
                    warn!(error = %e, "failed to post results, dropping batch");
                }
            }
        }
    }
}
