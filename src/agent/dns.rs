use super::Observation;
use crate::config::{CheckConfig, DnsMode};
use hickory_resolver::proto::{
    op::{Edns, Message, Query, ResponseCode},
    rr::{Name, RecordType},
};
use hickory_resolver::TokioResolver;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) async fn probe_dns(
    resolver: &TokioResolver,
    system_nameservers: &[SocketAddr],
    check: &CheckConfig,
    target: &str,
) -> Observation {
    if check.dns.mode == DnsMode::Dnssec {
        return probe_dnssec(check, system_nameservers).await;
    }

    let fqdn = if target.ends_with('.') {
        target.to_string()
    } else {
        format!("{target}.")
    };
    let owned_resolver;
    let resolver = match check.dns.nameserver.as_deref() {
        Some(value) => match crate::config::dns_nameserver(value) {
            Ok(addr) => {
                owned_resolver = match super::resolver_for(addr) {
                    Ok(resolver) => resolver,
                    Err(e) => {
                        return Observation::failed(crate::models::FailureKind::NameResolution, e)
                    }
                };
                &owned_resolver
            }
            Err(e) => return Observation::failed(crate::models::FailureKind::NameResolution, e),
        },
        None => resolver,
    };
    let answers = match tokio::time::timeout(
        check.timeout(),
        lookup_record(resolver, &fqdn, &check.dns.record),
    )
    .await
    {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => return Observation::failed(crate::models::FailureKind::NameResolution, e),
        Err(_) => return Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
    };
    if answers.is_empty() {
        return Observation::failed(
            crate::models::FailureKind::UnexpectedDnsAnswer,
            format!("no {} records for {target}", check.dns.record),
        );
    }

    let mut wanted: Vec<String> = check
        .dns
        .expected
        .iter()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    if let Some(legacy) = check.expect.body_contains.as_deref() {
        wanted.push(legacy.to_string());
    }
    if wanted.is_empty() {
        return Observation::ok();
    }

    let joined = answers.join(",");
    let hit = |needle: &String| answers.iter().any(|a| a.contains(needle.as_str()));
    let hits = wanted.iter().filter(|w| hit(w)).count();
    let matched = check.dns.match_type.satisfied(hits, wanted.len());
    Observation {
        body_matched: Some(matched),
        failure: (!matched).then_some(crate::models::FailureKind::UnexpectedDnsAnswer),
        error: (!matched).then(|| {
            let mode = match check.dns.match_type {
                crate::config::MatchMode::All => "all of",
                crate::config::MatchMode::Any => "one of",
            };
            format!("resolved to {joined}, expected {mode} {wanted:?}")
        }),
        ..Default::default()
    }
}

/// Ask the configured resolver, or else the agent's system resolvers, whether
/// it can authenticate the root DNSKEY set.
///
/// System resolvers are tried in order, but only an unreachable or silent one
/// hands over to the next. An answer — even SERVFAIL or one without AD — is
/// the verdict: skipping past a resolver that fails validation would hide
/// exactly the fault this check exists to report.
async fn probe_dnssec(check: &CheckConfig, system: &[SocketAddr]) -> Observation {
    let servers = match check.dns.nameserver.as_deref() {
        Some(value) => match crate::config::dns_nameserver(value) {
            Ok(addr) => vec![addr],
            Err(e) => return Observation::failed(crate::models::FailureKind::DnssecValidation, e),
        },
        None => system.to_vec(),
    };
    if servers.is_empty() {
        return Observation::failed(
            crate::models::FailureKind::DnssecValidation,
            "no resolver available: none configured and the system has none",
        );
    }
    let per_server = (check.timeout() / servers.len() as u32).max(Duration::from_secs(1));
    match tokio::time::timeout(check.timeout(), dnssec_first_answer(&servers, per_server)).await {
        Err(_) => Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
        Ok(Err(e)) => Observation::failed(crate::models::FailureKind::DnssecValidation, e),
        Ok(Ok((addr, message))) => evaluate_dnssec(addr, &message),
    }
}

/// The first response any server gives, with the server that gave it.
async fn dnssec_first_answer(
    servers: &[SocketAddr],
    per_server: Duration,
) -> Result<(SocketAddr, Message), String> {
    let mut errors = Vec::new();
    for &addr in servers {
        match tokio::time::timeout(per_server, dnssec_query(addr)).await {
            Ok(Ok(message)) => return Ok((addr, message)),
            Ok(Err(e)) => errors.push(format!("{addr}: {e}")),
            Err(_) => errors.push(format!("{addr}: no response")),
        }
    }
    Err(errors.join("; "))
}

async fn dnssec_query(addr: SocketAddr) -> Result<Message, String> {
    let mut request = Message::query();
    request.metadata.recursion_desired = true;
    request.add_query(Query::query(Name::root(), RecordType::DNSKEY));
    let mut edns = Edns::new();
    edns.set_dnssec_ok(true).set_max_payload(1232);
    request.edns = Some(edns);
    let request_id = request.metadata.id;
    let wire = request.to_vec().map_err(|e| e.to_string())?;

    let bind = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = tokio::net::UdpSocket::bind(bind)
        .await
        .map_err(|e| format!("could not bind DNS socket: {e}"))?;
    socket
        .send_to(&wire, addr)
        .await
        .map_err(|e| format!("could not query resolver: {e}"))?;
    let mut response = vec![0; 65535];
    let (len, source) = socket
        .recv_from(&mut response)
        .await
        .map_err(|e| format!("could not read resolver response: {e}"))?;
    if source != addr {
        return Err("DNS response came from an unexpected address".into());
    }
    response.truncate(len);
    let message = Message::from_vec(&response).map_err(|e| format!("invalid DNS response: {e}"))?;
    if message.metadata.id != request_id {
        return Err("DNS response had an unexpected transaction ID".into());
    }
    if message.metadata.truncation {
        return dnssec_query_tcp(addr, &wire, request_id).await;
    }
    Ok(message)
}

async fn dnssec_query_tcp(
    addr: SocketAddr,
    wire: &[u8],
    request_id: u16,
) -> Result<Message, String> {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("could not connect to resolver over TCP: {e}"))?;
    let len = u16::try_from(wire.len()).map_err(|_| "DNS query is too large".to_string())?;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| format!("could not query resolver over TCP: {e}"))?;
    stream
        .write_all(wire)
        .await
        .map_err(|e| format!("could not query resolver over TCP: {e}"))?;
    let response_len = stream
        .read_u16()
        .await
        .map_err(|e| format!("could not read resolver response over TCP: {e}"))?;
    let mut response = vec![0; usize::from(response_len)];
    stream
        .read_exact(&mut response)
        .await
        .map_err(|e| format!("could not read resolver response over TCP: {e}"))?;
    let message = Message::from_vec(&response).map_err(|e| format!("invalid DNS response: {e}"))?;
    if message.metadata.id != request_id {
        return Err("DNS response had an unexpected transaction ID".into());
    }
    Ok(message)
}

fn evaluate_dnssec(addr: SocketAddr, message: &Message) -> Observation {
    if message.metadata.response_code != ResponseCode::NoError {
        return Observation::failed(
            crate::models::FailureKind::DnssecValidation,
            format!(
                "resolver {addr} returned {} for the root DNSKEY query",
                message.metadata.response_code
            ),
        );
    }
    if !message.metadata.authentic_data {
        return Observation::failed(
            crate::models::FailureKind::DnssecValidation,
            format!("resolver {addr} did not authenticate the root DNSKEY response"),
        );
    }
    if !message
        .answers
        .iter()
        .any(|record| record.record_type() == RecordType::DNSKEY)
    {
        return Observation::failed(
            crate::models::FailureKind::DnssecValidation,
            format!("resolver {addr} sent an authenticated response with no root DNSKEY records"),
        );
    }
    Observation::ok()
}

async fn lookup_record(
    resolver: &TokioResolver,
    fqdn: &str,
    record: &str,
) -> Result<Vec<String>, String> {
    let rtype = match record.to_ascii_uppercase().as_str() {
        "AAAA" => RecordType::AAAA,
        "CNAME" => RecordType::CNAME,
        "MX" => RecordType::MX,
        "TXT" => RecordType::TXT,
        "NS" => RecordType::NS,
        _ => RecordType::A,
    };
    match resolver.lookup(fqdn, rtype).await {
        Ok(answer) => Ok(answer
            .answers()
            .iter()
            .map(|r| r.data.to_string())
            .collect()),
        Err(e) if e.is_no_records_found() => Err(format!("no {record} records for {fqdn}")),
        Err(e) => Err(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_resolver::proto::op::OpCode;

    const ADDR: SocketAddr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 53)),
        53,
    );

    fn response(code: ResponseCode, authenticated: bool, dnskey: bool) -> Message {
        let mut message = Message::response(1, OpCode::Query);
        message.metadata.response_code = code;
        message.metadata.authentic_data = authenticated;
        if dnskey {
            let wire = vec![
                0, 1, 0x81, 0xa0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 48, 0, 1, 0xc0, 0x0c, 0, 48, 0, 1,
                0, 0, 0, 60, 0, 7, 1, 1, 3, 8, 1, 2, 3,
            ];
            return Message::from_vec(&wire).expect("DNSKEY response parses");
        }
        message
    }

    async fn serve_once(success: bool) -> SocketAddr {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("test DNS server binds");
        let addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut query_wire = vec![0; 2048];
            let (len, peer) = socket.recv_from(&mut query_wire).await.unwrap();
            query_wire.truncate(len);
            let query = Message::from_vec(&query_wire).expect("query parses");
            assert_eq!(query.queries[0].query_type(), RecordType::DNSKEY);
            assert_eq!(query.queries[0].name(), &Name::root());
            assert!(query.edns.unwrap().flags().dnssec_ok);

            let mut response_wire = if success {
                vec![
                    0, 1, 0x81, 0xa0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 48, 0, 1, 0xc0, 0x0c, 0, 48, 0,
                    1, 0, 0, 0, 60, 0, 7, 1, 1, 3, 8, 1, 2, 3,
                ]
            } else {
                Message::error_msg(query.metadata.id, OpCode::Query, ResponseCode::ServFail)
                    .to_vec()
                    .unwrap()
            };
            response_wire[0..2].copy_from_slice(&query.metadata.id.to_be_bytes());
            socket.send_to(&response_wire, peer).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn dnssec_query_requests_dnskey_with_do_and_preserves_servfail() {
        let addr = serve_once(true).await;
        let success = dnssec_query(addr).await.unwrap();
        assert!(evaluate_dnssec(addr, &success).error.is_none());

        let addr = serve_once(false).await;
        let servfail = dnssec_query(addr).await.unwrap();
        assert_eq!(servfail.metadata.response_code, ResponseCode::ServFail);
        let result = evaluate_dnssec(addr, &servfail);
        assert_eq!(
            result.failure,
            Some(crate::models::FailureKind::DnssecValidation)
        );
        assert!(result.error.unwrap().contains(&addr.to_string()));
    }

    #[tokio::test]
    async fn a_silent_system_resolver_fails_over_to_the_next() {
        // Bound but never answers: the shape of an unreachable resolver.
        let silent = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let live = serve_once(true).await;
        let (addr, message) = dnssec_first_answer(
            &[silent.local_addr().unwrap(), live],
            Duration::from_millis(200),
        )
        .await
        .expect("the second resolver answers");
        assert_eq!(addr, live);
        assert!(evaluate_dnssec(addr, &message).error.is_none());
    }

    #[tokio::test]
    async fn a_servfail_is_a_verdict_not_a_reason_to_fail_over() {
        let failing = serve_once(false).await;
        let healthy = serve_once(true).await;
        let (addr, message) = dnssec_first_answer(&[failing, healthy], Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(addr, failing);
        assert_eq!(message.metadata.response_code, ResponseCode::ServFail);
    }

    #[tokio::test]
    async fn dnssec_without_any_resolver_fails_plainly() {
        let check = CheckConfig {
            kind: crate::models::CheckKind::Dns,
            dns: crate::config::DnsOptions {
                mode: DnsMode::Dnssec,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = probe_dnssec(&check, &[]).await;
        assert_eq!(
            result.failure,
            Some(crate::models::FailureKind::DnssecValidation)
        );
        assert!(result.error.unwrap().contains("no resolver"));
    }

    #[test]
    fn dnssec_requires_success_authenticated_data_and_a_dnskey() {
        assert!(
            evaluate_dnssec(ADDR, &response(ResponseCode::NoError, true, true))
                .error
                .is_none()
        );
        for message in [
            response(ResponseCode::ServFail, true, false),
            response(ResponseCode::NoError, false, false),
            response(ResponseCode::NoError, true, false),
        ] {
            let result = evaluate_dnssec(ADDR, &message);
            assert_eq!(
                result.failure,
                Some(crate::models::FailureKind::DnssecValidation)
            );
            assert!(result.error.is_some());
        }
    }
}
