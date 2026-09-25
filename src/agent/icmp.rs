use super::Observation;
use crate::config::CheckConfig;
use hickory_resolver::TokioResolver;

/// ICMP echo. Resolves the target, then sends `count` echoes; one reply is
/// enough, because a single dropped packet is not an outage.
///
/// ponytail: needs CAP_NET_RAW (or a permissive `net.ipv4.ping_group_range`).
/// Without it the probe reports a permission error rather than silently passing.
pub(super) async fn probe_icmp(
    resolver: &TokioResolver,
    check: &CheckConfig,
    target: &str,
) -> Observation {
    let addr = match resolve_one(resolver, target, &check.ping.ip_type).await {
        Ok(a) => a,
        Err(e) => return Observation::failed(crate::models::FailureKind::NameResolution, e),
    };
    let mut last_error = String::from("no reply");
    let mut last_kind = crate::models::FailureKind::ConnectionFailed;
    for _ in 0..check.ping.count.max(1) {
        match tokio::time::timeout(check.timeout(), surge_ping::ping(addr, &[0; 32])).await {
            Ok(Ok(_)) => return Observation::ok(),
            Ok(Err(e)) => {
                last_error = e.to_string();
                last_kind = crate::models::FailureKind::ConnectionFailed;
            }
            Err(_) => {
                last_error = "timeout".to_string();
                last_kind = crate::models::FailureKind::Timeout;
            }
        }
    }
    Observation::failed(last_kind, last_error)
}

/// Resolve a bare host (or pass through a literal IP) to a single address,
/// honouring the configured address family. Queried as an FQDN for the same
/// reason as `probe_dns`: search-domain expansion would ping a different host.
async fn resolve_one(
    resolver: &TokioResolver,
    target: &str,
    ip_type: &str,
) -> Result<std::net::IpAddr, String> {
    if let Ok(ip) = target.parse::<std::net::IpAddr>() {
        return Ok(ip);
    }
    let fqdn = if target.ends_with('.') {
        target.to_string()
    } else {
        format!("{target}.")
    };
    let answer = resolver
        .lookup_ip(fqdn.as_str())
        .await
        .map_err(|e| format!("resolving {target}: {e}"))?;
    let wanted = ip_type.to_ascii_lowercase();
    answer
        .iter()
        .find(|ip| match wanted.as_str() {
            "ipv4" => ip.is_ipv4(),
            "ipv6" => ip.is_ipv6(),
            _ => true,
        })
        .ok_or_else(|| match wanted.as_str() {
            "ipv4" | "ipv6" => format!("{target} has no {wanted} address"),
            _ => format!("{target} resolved to no addresses"),
        })
}
