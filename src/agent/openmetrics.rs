use super::Observation;
use crate::config::CheckConfig;

/// Upper bound on an OpenMetrics exporter body. Exporters are meant to answer
/// with a bounded set of series, but a misconfigured or hostile target could
/// stream indefinitely; without a cap a single check would let it hold memory
/// and bandwidth open for as long as the exporter cared to keep writing.
const MAX_METRICS_BODY_BYTES: usize = 4_194_304;

/// Scrape a Prometheus/OpenMetrics exporter and select one sample.
///
/// The body is always read here (unlike `probe_http`, which skips it when
/// nothing will inspect it) because parsing it is the entire point of this
/// check kind — but it is never kept on the `Observation`: only the selected
/// value and the parsed samples travel onward, per the type's contract that
/// the raw exposition text is never persisted.
pub(super) async fn probe_openmetrics(
    client: &reqwest::Client,
    check: &CheckConfig,
    target: &str,
) -> Observation {
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
    let status = resp.status();
    // An error page is not metrics: reject before spending any effort parsing
    // whatever body came with it.
    if !status.is_success() {
        return Observation::failed(
            crate::models::FailureKind::UnexpectedStatus,
            format!("unexpected status {}", status.as_u16()),
        );
    }
    // Reject an oversized body before reading it when the exporter is honest
    // about `Content-Length`; this catches the common case without a read.
    if resp
        .content_length()
        .is_some_and(|len| len > MAX_METRICS_BODY_BYTES as u64)
    {
        return Observation::failed(
            crate::models::FailureKind::Other,
            format!("exporter body exceeded {MAX_METRICS_BODY_BYTES} bytes"),
        );
    }
    let status_code = Some(status.as_u16());
    let mut resp = resp;
    let mut body = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if chunk.len() > MAX_METRICS_BODY_BYTES - body.len() {
                    return Observation::failed(
                        crate::models::FailureKind::Other,
                        format!("exporter body exceeded {MAX_METRICS_BODY_BYTES} bytes"),
                    );
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(e) => {
                return Observation {
                    status_code,
                    error: Some(format!("reading body: {e}")),
                    failure: Some(crate::models::FailureKind::ConnectionFailed),
                    ..Default::default()
                }
            }
        }
    }
    let body = match std::str::from_utf8(&body) {
        Ok(body) => body,
        Err(e) => {
            return Observation::failed(
                crate::models::FailureKind::UnexpectedContent,
                format!("exporter body is not UTF-8: {e}"),
            )
        }
    };
    let samples = match crate::openmetrics::parse(body) {
        Ok(s) => s,
        Err(e) => {
            return Observation::failed(
                crate::models::FailureKind::UnexpectedContent,
                format!("parse exposition: {e}"),
            )
        }
    };
    let selected = match crate::openmetrics::select(
        &samples,
        check.metric.name.trim(),
        &check.metric.label_pairs(),
    ) {
        Ok(s) => s,
        // Missing or ambiguous selection is a failed observation, never a
        // silent zero — a check that reads "0" because nothing matched would
        // hide the real problem (the exporter renamed a label, say) behind a
        // number that looks like a legitimate reading.
        Err(e) => {
            return Observation::failed(
                crate::models::FailureKind::UnexpectedContent,
                e.to_string(),
            )
        }
    };
    let value = selected.value;
    Observation {
        status_code,
        metric: Some(value),
        metrics: Some(samples),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    // A raw scrape of a small two-series family, used across the success and
    // selection tests below: one job matches, one does not, so selection by
    // label is actually exercised rather than trivially picking the only
    // sample.
    use super::probe_openmetrics;
    use crate::config::{CheckConfig, MetricLabel, MetricOptions};
    use crate::models::{CheckKind, FailureKind};
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::Router;
    use std::net::SocketAddr;

    const EXPOSITION: &str = "up{job=\"a\"} 1\nup{job=\"b\"} 0\n# EOF\n";

    /// Serve one fixed body at `/metrics` with the given status, on a random
    /// local port, for the lifetime of the test process.
    async fn serve(body: &'static str, status: StatusCode) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/metrics", get(move || async move { (status, body) }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    fn check(name: &str, labels: &[(&str, &str)]) -> CheckConfig {
        CheckConfig {
            name: "openmetrics-test".into(),
            kind: CheckKind::Openmetrics,
            metric: MetricOptions {
                name: name.into(),
                labels: labels
                    .iter()
                    .map(|(n, v)| MetricLabel {
                        name: (*n).into(),
                        value: (*v).into(),
                    })
                    .collect(),
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn selects_matching_sample_and_carries_the_whole_scrape() {
        let addr = serve(EXPOSITION, StatusCode::OK).await;
        let client = reqwest::Client::new();
        let target = format!("http://{addr}/metrics");
        let obs = probe_openmetrics(&client, &check("up", &[("job", "a")]), &target).await;

        assert_eq!(obs.error, None, "unexpected failure: {:?}", obs.error);
        assert_eq!(obs.metric, Some(1.0));
        // Both series travel onward, not just the selected one, so the
        // coordinator's Lua can compare same-scrape samples.
        assert_eq!(obs.metrics.as_ref().map(Vec::len), Some(2));
    }

    #[tokio::test]
    async fn label_set_picks_the_matching_series_not_the_first_one() {
        let addr = serve(EXPOSITION, StatusCode::OK).await;
        let client = reqwest::Client::new();
        let target = format!("http://{addr}/metrics");
        let obs = probe_openmetrics(&client, &check("up", &[("job", "b")]), &target).await;

        assert_eq!(obs.metric, Some(0.0));
    }

    #[tokio::test]
    async fn missing_metric_is_a_failed_observation_not_a_silent_zero() {
        let addr = serve(EXPOSITION, StatusCode::OK).await;
        let client = reqwest::Client::new();
        let target = format!("http://{addr}/metrics");
        let obs = probe_openmetrics(&client, &check("does_not_exist", &[]), &target).await;

        assert_eq!(obs.metric, None);
        assert_eq!(obs.failure, Some(FailureKind::UnexpectedContent));
        assert!(obs.error.is_some());
    }

    #[tokio::test]
    async fn malformed_exposition_fails_before_selection() {
        let addr = serve("up not_a_number\n", StatusCode::OK).await;
        let client = reqwest::Client::new();
        let target = format!("http://{addr}/metrics");
        let obs = probe_openmetrics(&client, &check("up", &[]), &target).await;

        assert_eq!(obs.metric, None);
        assert_eq!(obs.failure, Some(FailureKind::UnexpectedContent));
    }

    #[tokio::test]
    async fn accepts_exporter_bodies_larger_than_one_megabyte() {
        let mut body = String::from("gateway_cpu 0.32\n");
        for i in 0..50_000 {
            body.push_str(&format!("other_metric{{device=\"{i}\"}} 1\n"));
        }
        assert!(body.len() > 1_048_576);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/metrics", get(move || async move { body }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let obs = probe_openmetrics(
            &reqwest::Client::new(),
            &check("gateway_cpu", &[]),
            &format!("http://{addr}/metrics"),
        )
        .await;
        assert_eq!(obs.error, None);
        assert_eq!(obs.metric, Some(0.32));
    }

    #[tokio::test]
    async fn non_2xx_status_fails_before_parsing_the_body() {
        // A 500 page is not metrics, even if its body happened to look like
        // some: the status must be checked first.
        let addr = serve("up 1\n", StatusCode::INTERNAL_SERVER_ERROR).await;
        let client = reqwest::Client::new();
        let target = format!("http://{addr}/metrics");
        let obs = probe_openmetrics(&client, &check("up", &[]), &target).await;

        assert_eq!(obs.metric, None);
        assert_eq!(obs.failure, Some(FailureKind::UnexpectedStatus));
    }
}
