use super::Observation;
use crate::config::{CheckConfig, DnsMode};
use hickory_resolver::proto::{
    op::{Edns, Message, Query, ResponseCode},
    rr::{Name, RecordType},
};
use hickory_resolver::TokioResolver;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) async fn probe_dns(
    resolver: &TokioResolver,
    check: &CheckConfig,
    target: &str,
) -> Observation {
    if check.dns.mode == DnsMode::Dnssec {
        return probe_dnssec(check).await;
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

async fn probe_dnssec(check: &CheckConfig) -> Observation {
    let Some(value) = check.dns.nameserver.as_deref() else {
        return Observation::failed(
            crate::models::FailureKind::DnssecValidation,
            "DNSSEC validation check has no nameserver",
        );
    };
    let addr = match crate::config::dns_nameserver(value) {
        Ok(addr) => addr,
        Err(e) => return Observation::failed(crate::models::FailureKind::DnssecValidation, e),
    };
    match tokio::time::timeout(check.timeout(), dnssec_query(addr)).await {
        Err(_) => Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
        Ok(Err(e)) => Observation::failed(crate::models::FailureKind::DnssecValidation, e),
        Ok(Ok(message)) => evaluate_dnssec(&message),
    }
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

fn evaluate_dnssec(message: &Message) -> Observation {
    if message.metadata.response_code != ResponseCode::NoError {
        return Observation::failed(
            crate::models::FailureKind::DnssecValidation,
            format!(
                "resolver returned {} for the root DNSKEY query",
                message.metadata.response_code
            ),
        );
    }
    if !message.metadata.authentic_data {
        return Observation::failed(
            crate::models::FailureKind::DnssecValidation,
            "resolver did not authenticate the root DNSKEY response",
        );
    }
    if !message
        .answers
        .iter()
        .any(|record| record.record_type() == RecordType::DNSKEY)
    {
        return Observation::failed(
            crate::models::FailureKind::DnssecValidation,
            "authenticated response contained no root DNSKEY records",
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
        let success = dnssec_query(serve_once(true).await).await.unwrap();
        assert!(evaluate_dnssec(&success).error.is_none());

        let servfail = dnssec_query(serve_once(false).await).await.unwrap();
        assert_eq!(servfail.metadata.response_code, ResponseCode::ServFail);
        assert_eq!(
            evaluate_dnssec(&servfail).failure,
            Some(crate::models::FailureKind::DnssecValidation)
        );
    }

    #[test]
    fn dnssec_requires_success_authenticated_data_and_a_dnskey() {
        assert!(
            evaluate_dnssec(&response(ResponseCode::NoError, true, true))
                .error
                .is_none()
        );
        for message in [
            response(ResponseCode::ServFail, true, false),
            response(ResponseCode::NoError, false, false),
            response(ResponseCode::NoError, true, false),
        ] {
            let result = evaluate_dnssec(&message);
            assert_eq!(
                result.failure,
                Some(crate::models::FailureKind::DnssecValidation)
            );
            assert!(result.error.is_some());
        }
    }
}
