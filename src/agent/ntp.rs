use super::Observation;
use crate::config::CheckConfig;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) async fn probe_ntp(check: &CheckConfig, target: &str) -> Observation {
    match tokio::time::timeout(check.timeout(), query_ntp(target)).await {
        Ok(Ok(())) => Observation::ok(),
        Ok(Err(error)) => Observation::failed(crate::models::FailureKind::UnexpectedContent, error),
        Err(_) => Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
    }
}

async fn query_ntp(target: &str) -> Result<(), String> {
    let addr = tokio::net::lookup_host(target)
        .await
        .map_err(|error| format!("could not resolve NTP target: {error}"))?
        .next()
        .ok_or_else(|| "could not resolve NTP target".to_string())?;
    let bind = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = tokio::net::UdpSocket::bind(bind)
        .await
        .map_err(|error| format!("could not bind NTP socket: {error}"))?;
    socket
        .connect(addr)
        .await
        .map_err(|error| format!("could not connect NTP socket: {error}"))?;

    let mut request = [0_u8; 48];
    request[0] = 0x23;
    let timestamp = ntp_timestamp()?;
    request[40..48].copy_from_slice(&timestamp);
    socket
        .send(&request)
        .await
        .map_err(|error| format!("could not send NTP request: {error}"))?;

    let mut response = [0_u8; 48];
    let len = socket
        .recv(&mut response)
        .await
        .map_err(|error| format!("could not read NTP response: {error}"))?;
    validate_response(&response[..len], &timestamp)
}

fn ntp_timestamp() -> Result<[u8; 8], String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before the Unix epoch: {error}"))?;
    let seconds = elapsed.as_secs().wrapping_add(2_208_988_800);
    let fraction = ((u64::from(elapsed.subsec_nanos())) << 32) / 1_000_000_000;
    Ok(((seconds << 32) | fraction).to_be_bytes())
}

fn validate_response(response: &[u8], request_timestamp: &[u8; 8]) -> Result<(), String> {
    if response.len() < 48 {
        return Err("NTP response is shorter than 48 bytes".into());
    }
    if response[0] >> 6 == 3 {
        return Err("NTP server is unsynchronized".into());
    }
    if response[0] & 7 != 4 {
        return Err("NTP response has an invalid mode".into());
    }
    if !(1..=15).contains(&response[1]) {
        return Err("NTP response has an invalid stratum".into());
    }
    if response[24..32] != *request_timestamp {
        return Err("NTP response does not match the request".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn check(target: String) -> CheckConfig {
        CheckConfig {
            target,
            timeout_secs: 1,
            ..Default::default()
        }
    }

    async fn server(response: [u8; 48]) -> String {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = socket.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut request = [0_u8; 48];
            let (_, peer) = socket.recv_from(&mut request).await.unwrap();
            let mut response = response;
            response[24..32].copy_from_slice(&request[40..48]);
            socket.send_to(&response, peer).await.unwrap();
        });
        target
    }

    fn response() -> [u8; 48] {
        let mut response = [0_u8; 48];
        response[0] = 0x24;
        response[1] = 1;
        response
    }

    #[tokio::test]
    async fn accepts_valid_ntp_response() {
        let target = server(response()).await;
        let check = check(target);
        assert!(probe_ntp(&check, &check.target).await.error.is_none());
    }

    #[tokio::test]
    async fn rejects_invalid_unsynchronized_and_mismatched_responses() {
        for response in [
            {
                let mut response = response();
                response[0] = 0xe4;
                response
            },
            {
                let mut response = response();
                response[0] = 0x23;
                response
            },
            {
                let mut response = response();
                response[1] = 16;
                response
            },
        ] {
            let target = server(response).await;
            let check = check(target);
            let observation = probe_ntp(&check, &check.target).await;
            assert_eq!(
                observation.failure,
                Some(crate::models::FailureKind::UnexpectedContent)
            );
        }

        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = socket.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut request = [0_u8; 48];
            let (_, peer) = socket.recv_from(&mut request).await.unwrap();
            socket.send_to(&response(), peer).await.unwrap();
        });
        let check = check(target);
        let observation = probe_ntp(&check, &check.target).await;
        assert_eq!(
            observation.failure,
            Some(crate::models::FailureKind::UnexpectedContent)
        );
    }

    #[tokio::test]
    async fn times_out_when_server_does_not_respond() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = socket.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut request = [0_u8; 48];
            socket.recv_from(&mut request).await.unwrap();
        });
        let mut check = check(target);
        check.timeout_secs = 1;
        let observation = probe_ntp(&check, &check.target).await;
        assert_eq!(
            observation.failure,
            Some(crate::models::FailureKind::Timeout)
        );
        assert_eq!(observation.error.as_deref(), Some("timeout"));
    }
}
