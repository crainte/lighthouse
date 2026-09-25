use super::Observation;
use crate::config::CheckConfig;
use hickory_resolver::TokioResolver;

/// DNS lookup of the configured record type. A dropped UDP packet is retried by
/// the resolver (see `Probes::new`) rather than counted as an outage — the
/// failure mode that makes naive DNS monitors flap. NXDOMAIN comes back as an
/// error, which is correct: the name genuinely does not resolve.
///
/// The target is queried as a fully-qualified name (trailing dot). Without this,
/// a resolver with `search example.local` silently answers for
/// `target.example.local` instead — so the check would pass while testing a name
/// nobody configured, and a wildcard/sinkhole answer would read as healthy.
///
/// Expected values assert what the record resolves to, under `any` or `all`
/// matching.
pub(super) async fn probe_dns(
    resolver: &TokioResolver,
    check: &CheckConfig,
    target: &str,
) -> Observation {
    let fqdn = if target.ends_with('.') {
        target.to_string()
    } else {
        format!("{target}.")
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

    // `expected` is the modern form; `expect.body_contains` predates it and is
    // still honoured so existing configs keep asserting what they always did.
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
        // Name the actual answer, so a pinned-record failure is self-explanatory.
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

/// Look up one record type, returning the answers as text.
async fn lookup_record(
    resolver: &TokioResolver,
    fqdn: &str,
    record: &str,
) -> Result<Vec<String>, String> {
    use hickory_resolver::proto::rr::RecordType;
    let rtype = match record.to_ascii_uppercase().as_str() {
        "AAAA" => RecordType::AAAA,
        "CNAME" => RecordType::CNAME,
        "MX" => RecordType::MX,
        "TXT" => RecordType::TXT,
        "NS" => RecordType::NS,
        _ => RecordType::A,
    };
    match resolver.lookup(fqdn, rtype).await {
        // `answers()` is the response section; rendering the rdata gives the
        // value ("1.2.3.4", "mail.example.com.") rather than a whole record line.
        Ok(answer) => Ok(answer
            .answers()
            .iter()
            .map(|r| r.data.to_string())
            .collect()),
        // "No records" is the common, expected failure — report it plainly rather
        // than leaking the resolver's Debug-formatted Query internals.
        Err(e) if e.is_no_records_found() => Err(format!("no {record} records for {fqdn}")),
        Err(e) => Err(e.to_string()),
    }
}
