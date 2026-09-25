use super::Observation;
use crate::config::CheckConfig;

/// TCP connect to "host:port". Connecting at all is a success; the coordinator
/// treats any error as DOWN.
pub(super) async fn probe_tcp(check: &CheckConfig, target: &str) -> Observation {
    match tokio::time::timeout(check.timeout(), tokio::net::TcpStream::connect(target)).await {
        Ok(Ok(_stream)) => Observation::ok(),
        Ok(Err(e)) => {
            Observation::failed(crate::models::FailureKind::ConnectionFailed, e.to_string())
        }
        Err(_) => Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
    }
}
