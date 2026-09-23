//! HTTP response hardening shared by coordinator and global routers.
//!
//! This module is also where "how far do we trust the network in front of
//! us" lives. That question has one answer, not two: `TrustedProxies` is the
//! same list a `Secure`-cookie decision would consult if this project ever
//! inferred one from `X-Forwarded-Proto` (it does not — see
//! `secure_cookies` in `config.rs` — but if it did, it must read this list
//! rather than invent a second one) and the list `client_ip` uses to decide
//! whether `X-Forwarded-For` is a proxy's testimony or a stranger's guess.

use axum::http::{header, HeaderMap, HeaderValue, Request};
use axum::middleware::Next;
use axum::response::Response;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Add browser-facing protections without relying on proxy-supplied headers.
pub async fn security_headers(request: Request<axum::body::Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    // The SPA uses dynamic inline style attributes for charts and status colors.
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self' ws: wss:",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    response
}

const X_FORWARDED_FOR: &str = "x-forwarded-for";

/// A configured set of IPs or CIDR blocks whose `X-Forwarded-For` claims are
/// believed — a bare reverse proxy in front of this node, not a client.
///
/// Empty by construction unless an operator opts in via `trusted_proxies`,
/// which is also what makes empty the correct default: nothing in this
/// project infers trust from the network, only from configuration.
#[derive(Debug, Clone, Default)]
pub struct TrustedProxies {
    // Each entry is a network address already masked to its own prefix, so
    // `contains` is one comparison per entry rather than a mask-then-compare.
    networks: Vec<(IpAddr, u8)>,
}

impl TrustedProxies {
    /// Parse `host` or `host/prefix` entries. A bare address is treated as a
    /// `/32` (or `/128` for IPv6) — one trusted host, not a network.
    pub fn parse(raw: &[String]) -> Result<Self, String> {
        let mut networks = Vec::with_capacity(raw.len());
        for entry in raw {
            let (host, prefix) = match entry.split_once('/') {
                Some((host, prefix)) => (host, Some(prefix)),
                None => (entry.as_str(), None),
            };
            let addr: IpAddr = host
                .trim()
                .parse()
                .map_err(|_| format!("invalid trusted_proxies entry {entry:?}: not an IP"))?;
            let max_prefix = match addr {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };
            let prefix = match prefix {
                Some(p) => p
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| format!("invalid trusted_proxies entry {entry:?}: bad prefix"))?,
                None => max_prefix,
            };
            if prefix > max_prefix {
                return Err(format!(
                    "invalid trusted_proxies entry {entry:?}: prefix exceeds {max_prefix}"
                ));
            }
            networks.push((mask(addr, prefix), prefix));
        }
        Ok(Self { networks })
    }

    fn contains(&self, ip: IpAddr) -> bool {
        self.networks
            .iter()
            .any(|(net, prefix)| mask(ip, *prefix) == *net && same_family(ip, *net))
    }
}

fn same_family(a: IpAddr, b: IpAddr) -> bool {
    matches!(
        (a, b),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

/// Zero the host bits beyond `prefix`. `prefix` at the address's own width
/// (32 for IPv4, 128 for IPv6) is the identity mask; guarding the shift
/// amount at 0 avoids the overflow a shift-by-width would otherwise panic on.
fn mask(addr: IpAddr, prefix: u8) -> IpAddr {
    match addr {
        IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            IpAddr::V4(Ipv4Addr::from(bits & mask))
        }
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            IpAddr::V6(Ipv6Addr::from(bits & mask))
        }
    }
}

/// Resolve the address a request should be attributed to for per-client
/// controls such as login rate limiting.
///
/// `peer` is what the TCP connection actually reports — never spoofable, but
/// behind a reverse proxy it is always the proxy's own address, so every
/// client shares one identity. `X-Forwarded-For` fixes that only when `peer`
/// is a proxy this deployment configured; otherwise the header is exactly as
/// trustworthy as any other client-supplied string, which is to say not at
/// all, and is ignored outright.
///
/// When `peer` is trusted, the header is read from the right: each proxy in
/// a chain is expected to append the address it saw connecting to *it*, so
/// the rightmost entry is the closest hop's testimony. Entries that are
/// themselves trusted proxies are skipped — they are further hops in
/// infrastructure this deployment already trusts — and the first entry that
/// is not is taken as the client. This is the classic direction to parse in:
/// reading left-to-right lets a client prepend any address it likes ahead of
/// what the real chain appended, and trusting that string is the spoof this
/// function exists to close. A hop that fails to parse as an address stops
/// the walk and falls back to `peer` rather than guessing past it.
pub fn client_ip(peer: IpAddr, headers: &HeaderMap, trusted: &TrustedProxies) -> IpAddr {
    if !trusted.contains(peer) {
        return peer;
    }
    let Some(value) = headers.get(X_FORWARDED_FOR).and_then(|v| v.to_str().ok()) else {
        return peer;
    };
    for hop in value.split(',').rev() {
        let Ok(candidate) = hop.trim().parse::<IpAddr>() else {
            return peer;
        };
        if !trusted.contains(candidate) {
            return candidate;
        }
    }
    // Every hop named a trusted proxy — there is no claim beyond our own
    // trust boundary to prefer over the peer that reached us directly.
    peer
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxies(entries: &[&str]) -> TrustedProxies {
        TrustedProxies::parse(&entries.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap()
    }

    fn headers_with_xff(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(X_FORWARDED_FOR, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn empty_config_ignores_the_header_entirely() {
        let trusted = TrustedProxies::default();
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let headers = headers_with_xff("1.2.3.4");
        assert_eq!(client_ip(peer, &headers, &trusted), peer);
    }

    #[test]
    fn a_spoofed_header_from_an_untrusted_peer_is_ignored() {
        let trusted = proxies(&["10.0.0.1"]);
        let attacker: IpAddr = "6.6.6.6".parse().unwrap();
        let headers = headers_with_xff("1.2.3.4");
        assert_eq!(client_ip(attacker, &headers, &trusted), attacker);
    }

    #[test]
    fn a_header_from_a_trusted_proxy_is_honoured() {
        let trusted = proxies(&["10.0.0.1"]);
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let headers = headers_with_xff("1.2.3.4");
        assert_eq!(
            client_ip(peer, &headers, &trusted),
            "1.2.3.4".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn a_trusted_proxy_with_no_header_falls_back_to_the_peer() {
        let trusted = proxies(&["10.0.0.1"]);
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(client_ip(peer, &headers, &trusted), peer);
    }

    #[test]
    fn a_multi_hop_chain_picks_the_rightmost_untrusted_hop() {
        // client -> proxy1 (10.0.0.1) -> proxy2 (10.0.0.2, our peer). Both
        // hops are trusted infrastructure; the client is not.
        let trusted = proxies(&["10.0.0.1", "10.0.0.2"]);
        let peer: IpAddr = "10.0.0.2".parse().unwrap();
        let headers = headers_with_xff("1.2.3.4, 10.0.0.1");
        assert_eq!(
            client_ip(peer, &headers, &trusted),
            "1.2.3.4".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn a_client_cannot_smuggle_a_fake_hop_ahead_of_the_real_chain() {
        // The attacker sets their own X-Forwarded-For before it ever reaches
        // the trusted proxy; the proxy appends the address it actually saw.
        // Only the rightmost (proxy-appended) entry is believed.
        let trusted = proxies(&["10.0.0.1"]);
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let headers = headers_with_xff("8.8.8.8, 9.9.9.9");
        assert_eq!(
            client_ip(peer, &headers, &trusted),
            "9.9.9.9".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn a_chain_of_only_trusted_hops_falls_back_to_the_peer() {
        let trusted = proxies(&["10.0.0.1", "10.0.0.2"]);
        let peer: IpAddr = "10.0.0.2".parse().unwrap();
        let headers = headers_with_xff("10.0.0.1");
        assert_eq!(client_ip(peer, &headers, &trusted), peer);
    }

    #[test]
    fn a_malformed_hop_falls_back_to_the_peer_rather_than_guessing() {
        let trusted = proxies(&["10.0.0.1"]);
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let headers = headers_with_xff("not-an-ip");
        assert_eq!(client_ip(peer, &headers, &trusted), peer);
    }

    #[test]
    fn cidr_ranges_match_by_prefix() {
        let trusted = proxies(&["10.0.0.0/8"]);
        assert!(trusted.contains("10.5.6.7".parse().unwrap()));
        assert!(!trusted.contains("11.0.0.1".parse().unwrap()));
    }

    #[test]
    fn ipv6_and_ipv4_are_not_conflated() {
        // A masked IPv4-mapped bit pattern must not accidentally match a
        // differently-typed trusted entry; family must match too.
        let trusted = proxies(&["0.0.0.0/0"]);
        assert!(!trusted.contains("::1".parse().unwrap()));
    }

    #[test]
    fn parse_rejects_malformed_entries() {
        assert!(TrustedProxies::parse(&["not-an-ip".to_string()]).is_err());
        assert!(TrustedProxies::parse(&["10.0.0.0/40".to_string()]).is_err());
    }
}
