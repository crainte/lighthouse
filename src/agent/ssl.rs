use super::Observation;
use crate::config::CheckConfig;
use chrono::Utc;

/// Captures the leaf certificate during the handshake instead of validating it.
///
/// An SSL *check* has to report on whatever the server actually presents,
/// including a chain that fails validation — refusing the handshake would tell
/// us nothing about the expiry we came to read. The connection is used solely
/// to inspect the certificate and is dropped immediately, so nothing is ever
/// sent over it.
#[derive(Debug, Default)]
struct CertCapture {
    expiry: parking_lot::Mutex<Option<chrono::DateTime<Utc>>>,
}

impl CertCapture {
    fn expiry(&self) -> Option<chrono::DateTime<Utc>> {
        *self.expiry.lock()
    }
}

impl rustls::client::danger::ServerCertVerifier for CertCapture {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use x509_parser::prelude::FromDer;
        if let Ok((_, cert)) = x509_parser::prelude::X509Certificate::from_der(end_entity) {
            let secs = cert.validity().not_after.timestamp();
            *self.expiry.lock() = chrono::DateTime::from_timestamp(secs, 0);
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// TLS certificate validity for a host, and how long is left on it.
///
/// Expiry is a failure the moment it happens, so the check reports DOWN once the
/// certificate is inside its warning window rather than waiting for the outage
/// the expiry will cause.
pub(super) async fn probe_ssl(check: &CheckConfig, target: &str) -> Observation {
    // Accept "host" or "host:port"; 443 unless told otherwise.
    let (host, port) = match target.rsplit_once(':') {
        Some((h, p)) if p.parse::<u16>().is_ok() => (h.to_string(), p.parse::<u16>().unwrap()),
        _ => (target.to_string(), 443u16),
    };
    let addr = format!("{host}:{port}");
    let stream =
        match tokio::time::timeout(check.timeout(), tokio::net::TcpStream::connect(&addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Observation::failed(
                    crate::models::FailureKind::ConnectionFailed,
                    format!("connecting to {addr}: {e}"),
                )
            }
            Err(_) => return Observation::failed(crate::models::FailureKind::Timeout, "timeout"),
        };

    let verifier = std::sync::Arc::new(CertCapture::default());
    let mut cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier.clone())
        .with_no_client_auth();
    cfg.alpn_protocols.clear();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg));
    let server_name = match rustls::pki_types::ServerName::try_from(host.clone()) {
        Ok(n) => n,
        Err(_) => {
            return Observation::failed(
                crate::models::FailureKind::TlsError,
                format!("{host} is not a valid TLS server name"),
            )
        }
    };
    if let Err(e) =
        tokio::time::timeout(check.timeout(), connector.connect(server_name, stream)).await
    {
        let _ = e;
        return Observation::failed(
            crate::models::FailureKind::Timeout,
            "timeout during TLS handshake",
        );
    }

    let Some(expiry) = verifier.expiry() else {
        return Observation::failed(
            crate::models::FailureKind::TlsError,
            "server presented no certificate",
        );
    };
    let days_left = (expiry - Utc::now()).num_days();
    if days_left < 0 {
        return Observation::failed(
            crate::models::FailureKind::CertificateExpired { days: -days_left },
            format!("certificate expired {} days ago", -days_left),
        );
    }
    if days_left <= check.cert_warning_days as i64 {
        return Observation::failed(
            crate::models::FailureKind::CertificateExpiring { days: days_left },
            format!("certificate expires in {days_left} days"),
        );
    }
    Observation::ok()
}
