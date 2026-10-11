use super::Observation;
use crate::config::CheckConfig;
use anyhow::{Context, Result};

/// HTTP GET. Reads the body when a content expectation is configured, or when a
/// Lua script might inspect it.
pub(super) async fn probe_http(
    client: &reqwest::Client,
    check: &CheckConfig,
    target: &str,
) -> Observation {
    let client = if check.allow_self_signed && target.starts_with("https://") {
        match self_signed_http_client() {
            Ok(client) => client,
            Err(e) => {
                return Observation::failed(crate::models::FailureKind::TlsError, e.to_string())
            }
        }
    } else {
        client.clone()
    };
    let mut req = client.get(target).timeout(check.timeout());
    for header in &check.headers {
        req = req.header(header.name.trim(), header.value.as_str());
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            return if e.is_timeout() {
                Observation::failed(crate::models::FailureKind::Timeout, "timeout")
            } else {
                Observation::failed(crate::models::FailureKind::ConnectionFailed, e.to_string())
            }
        }
    };
    let status_code = Some(resp.status().as_u16());
    let scripted = check.eval.as_deref().is_some_and(|s| !s.trim().is_empty());
    // Headers are cheap; the body is not, so only read it when something will
    // actually look at it.
    let headers = scripted.then(|| {
        resp.headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_lowercase(),
                    v.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect()
    });
    let needle = check.expect.body_contains.as_deref();
    if needle.is_none() && !scripted {
        return Observation {
            status_code,
            headers,
            ..Default::default()
        };
    }
    match resp.text().await {
        Ok(body) => {
            let matched = needle.map(|n| body.contains(n));
            Observation {
                status_code,
                body_matched: matched,
                // Spell out the mismatch: otherwise a 200-with-wrong-body shows
                // up as "HTTP 200" and reads like a healthy response.
                error: match (needle, matched) {
                    (Some(n), Some(false)) => Some(format!("body did not contain {n:?}")),
                    _ => None,
                },
                failure: matches!((needle, matched), (Some(_), Some(false)))
                    .then_some(crate::models::FailureKind::UnexpectedContent),
                body: scripted.then(|| truncate_body(body)),
                headers,
                ..Default::default()
            }
        }
        Err(e) => Observation {
            status_code,
            error: Some(format!("reading body: {e}")),
            failure: Some(crate::models::FailureKind::ConnectionFailed),
            headers,
            ..Default::default()
        },
    }
}

/// Cap what travels to the coordinator, on a char boundary so the JSON stays
/// valid UTF-8.
fn truncate_body(mut body: String) -> String {
    if body.len() > crate::script::MAX_BODY_BYTES {
        let mut cut = crate::script::MAX_BODY_BYTES;
        while cut > 0 && !body.is_char_boundary(cut) {
            cut -= 1;
        }
        body.truncate(cut);
    }
    body
}

/// Build an HTTPS client that preserves normal certificate validity and hostname
/// checks, but permits a self-signed leaf certificate without disabling TLS
/// verification for any other per-check failure.
fn self_signed_http_client() -> Result<reqwest::Client> {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let verifier = SelfSignedVerifier {
        trusted: rustls::client::WebPkiServerVerifier::builder(std::sync::Arc::new(roots))
            .build()
            .context("building TLS certificate verifier")?,
    };
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(std::sync::Arc::new(verifier))
        .with_no_client_auth();
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .build()
        .context("building HTTPS probe client")
}

#[derive(Debug)]
struct SelfSignedVerifier {
    trusted: std::sync::Arc<rustls::client::WebPkiServerVerifier>,
}

impl rustls::client::danger::ServerCertVerifier for SelfSignedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        match self
            .trusted
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Ok(verified) => Ok(verified),
            Err(rustls::Error::InvalidCertificate(error))
                if intermediates.is_empty() && is_chain_error(&error) =>
            {
                verify_self_signed_leaf(end_entity, server_name, now)
            }
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.trusted.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.trusted.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.trusted.supported_verify_schemes()
    }
}

/// Whether a certificate validation error is solely a failure to establish a
/// chain to a configured trust anchor. The CA leaf form is customary for a
/// self-signed certificate and rustls reports it separately from UnknownIssuer.
fn is_chain_error(error: &rustls::CertificateError) -> bool {
    match error {
        rustls::CertificateError::UnknownIssuer => true,
        rustls::CertificateError::Other(other) => other.to_string().contains("CaUsedAsEndEntity"),
        _ => false,
    }
}

/// Verify the narrow self-signed exception with the same webpki verifier used
/// for normal TLS. A matching issuer/subject alone is not enough: use that
/// exact certificate as the sole trust anchor, so webpki verifies the leaf's
/// validity, server name, key usage, and extensions. Presented chains never
/// qualify for this exception.
fn verify_self_signed_leaf(
    end_entity: &rustls::pki_types::CertificateDer<'_>,
    server_name: &rustls::pki_types::ServerName<'_>,
    now: rustls::pki_types::UnixTime,
) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
    use rustls::client::danger::ServerCertVerifier;
    use x509_parser::prelude::{FromDer, X509Certificate};

    let (_, cert) = X509Certificate::from_der(end_entity)
        .map_err(|e| rustls::Error::General(format!("parsing self-signed certificate: {e}")))?;
    if cert.subject() != cert.issuer() {
        return Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::UnknownIssuer,
        ));
    }
    let mut roots = rustls::RootCertStore::empty();
    roots.add(end_entity.clone().into_owned())?;
    let verifier = rustls::client::WebPkiServerVerifier::builder(std::sync::Arc::new(roots))
        .build()
        .map_err(|e| {
            rustls::Error::General(format!("building self-signed certificate verifier: {e}"))
        })?;
    verifier.verify_server_cert(end_entity, &[], server_name, &[], now)
}

#[cfg(test)]
mod tests {
    use super::SelfSignedVerifier;
    use rustls::client::danger::ServerCertVerifier;
    use std::{
        fs,
        process::Command,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn self_signed_cert() -> rustls::pki_types::CertificateDer<'static> {
        let dir = std::env::temp_dir().join(format!(
            "lighthouse-tls-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&dir).unwrap();
        let cert = dir.join("cert.der");
        let key = dir.join("key.pem");
        let status = Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(&key)
            .args([
                "-out",
                "cert.der",
                "-outform",
                "DER",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-addext",
                "keyUsage=critical,digitalSignature,keyEncipherment",
                "-days",
                "1",
            ])
            .current_dir(&dir)
            .status()
            .unwrap();
        assert!(status.success());
        let cert = rustls::pki_types::CertificateDer::from(fs::read(cert).unwrap());
        fs::remove_dir_all(dir).unwrap();
        cert
    }

    fn verifier_with_roots(roots: rustls::RootCertStore) -> SelfSignedVerifier {
        SelfSignedVerifier {
            trusted: rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
                .build()
                .unwrap(),
        }
    }

    fn verifier() -> SelfSignedVerifier {
        verifier_with_roots(rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        })
    }

    #[test]
    fn trusted_certificate_is_accepted_without_the_exception() {
        let cert = self_signed_cert();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.clone()).unwrap();
        let verifier = verifier_with_roots(roots);
        let localhost = rustls::pki_types::ServerName::try_from("localhost").unwrap();

        assert!(verifier
            .verify_server_cert(
                &cert,
                &[],
                &localhost,
                &[],
                rustls::pki_types::UnixTime::now()
            )
            .is_ok());
    }

    #[test]
    fn self_signed_fallback_keeps_hostname_verification() {
        let cert = self_signed_cert();
        let verifier = verifier();
        let now = rustls::pki_types::UnixTime::now();
        let localhost = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        assert!(verifier
            .verify_server_cert(&cert, &[], &localhost, &[], now)
            .is_ok());

        let wrong_name = rustls::pki_types::ServerName::try_from("wrong.example").unwrap();
        assert!(verifier
            .verify_server_cert(&cert, &[], &wrong_name, &[], now)
            .is_err());

        let expired = rustls::pki_types::UnixTime::since_unix_epoch(
            std::time::Duration::from_secs(now.as_secs() + 2 * 24 * 60 * 60),
        );
        assert!(verifier
            .verify_server_cert(&cert, &[], &localhost, &[], expired)
            .is_err());
    }

    #[test]
    fn forged_self_signed_certificate_is_rejected() {
        let cert = self_signed_cert();
        let mut forged = cert.as_ref().to_vec();
        *forged.last_mut().unwrap() ^= 1;
        let forged = rustls::pki_types::CertificateDer::from(forged);
        let verifier = verifier();
        let localhost = rustls::pki_types::ServerName::try_from("localhost").unwrap();

        assert!(verifier
            .verify_server_cert(
                &forged,
                &[],
                &localhost,
                &[],
                rustls::pki_types::UnixTime::now(),
            )
            .is_err());
    }
}
