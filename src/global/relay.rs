//! Proxying (relaying) requests to regional coordinators.

use super::merge::{attach_incident_groups, chart_relay_path, filter_chart_page, merge_responses};
use super::routes::{error_response, is_admin};
use super::GlobalNode;
use crate::models::CheckId;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, CONTROLS};
use tracing::warn;

/// Everything under `/api/v1` the global node does not serve from its own
/// cache or its own catalogue (`global::admin` — checks and sections,
/// intercepted by explicit routes before this fallback is ever reached) is
/// relayed to regional coordinators. Three shapes:
///
/// * **Public reads** (`GET`, no `/admin/`) — a drill-down: route to the region
///   that owns the named check, take the first answer.
/// * **Admin reads** (`GET /api/v1/admin/...`) — same single-answer relay, but
///   the operator's global session is translated into the shared trust token so
///   the region authorizes it. De-duplication of list results is left to the
///   region: they run identical config, so any one region's list is the fleet's.
/// * **Admin writes** (`POST`/`PUT`/`PATCH`/`DELETE`) not covered by
///   `global::admin` — notifiers, credentials, rollup policies, maintenance,
///   incidents — are fanned out to *every* region so the fleet stays uniform.
///   The operator sees the first region's response; a failure on any region is
///   surfaced so a partial fan-out is never silently reported as success. This
///   is the pre-Phase-1 mechanism, unreconciled if a region misses it; see
///   `fan_out_write`'s doc for why the catalogue no longer uses it.
///
/// Non-API paths serve the embedded SPA.
pub(super) async fn relay_handler(
    State(node): State<GlobalNode>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().trim_start_matches('/');
    if !path.starts_with("api/") {
        return crate::http::serve_asset(&uri);
    }
    let now = chrono::Utc::now().timestamp_millis();
    let is_admin_path = path.starts_with("api/v1/admin/");
    let is_write = !matches!(method, Method::GET | Method::HEAD);
    // A signed-in operator here relays as admin to the regions. This gates
    // writes and admin paths, and — for reads — decides whether the region is
    // asked as admin (full detail, and some "public" reads like `/agents` are
    // in fact admin-gated) or anonymously.
    let operator = is_admin(&node, &headers);

    // Writes and admin paths require a signed-in operator; the region only ever
    // sees the trust token, never the operator's session.
    if (is_admin_path || is_write) && !operator {
        return error_response(StatusCode::UNAUTHORIZED, "authentication required");
    }

    let composite = extract_incident_id(path)
        .as_deref()
        .and_then(parse_composite_incident_id);
    if is_write {
        // Regional incidents are not replicated between regions. Once an
        // incident has been returned from a merged list its composite id is its
        // routing key, so edits must go only to that owner, with the local id.
        if let Some((region, local_id)) = composite {
            let Some(endpoints) = node
                .inner
                .cache
                .region_endpoints(now)
                .into_iter()
                .find_map(|(name, endpoints)| (name == region).then_some(endpoints))
            else {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "incident owner region is unavailable",
                );
            };
            let pq = rewrite_incident_path(&uri, &local_id);
            return relay_single_path(&node, &method, &pq, &body, &endpoints, true).await;
        }
        return fan_out_write(&node, &method, &uri, &body, now).await;
    }

    if let Some((region, local_id)) = composite {
        let Some(endpoints) = node
            .inner
            .cache
            .region_endpoints(now)
            .into_iter()
            .find_map(|(name, endpoints)| (name == region).then_some(endpoints))
        else {
            return error_response(
                StatusCode::NOT_FOUND,
                "incident owner region is unavailable",
            );
        };
        let pq = rewrite_incident_path(&uri, &local_id);
        return relay_incident_detail(&node, &method, &pq, &body, &endpoints, operator, &region)
            .await;
    }

    // Data-bearing reads need one answer from every owner region. Global keeps
    // no storage: it merges only these transient responses.
    let region_endpoints = match extract_check_id(path) {
        Some(id) => node.inner.cache.owner_region_endpoints(id, now),
        None if path == "api/v1/alerts"
            || path == "api/v1/incidents"
            || path == "api/v1/admin/results"
            // The status page's batched series. Fleet-wide *data*, not config:
            // a check is observed only by the agents of the region holding it,
            // so one region answers with empty buckets for every check it does
            // not probe — which the page renders as "Collecting latency data…"
            // for checks that have plenty of it elsewhere.
            || path == "api/v1/uptime"
            || path == "api/v1/latency-points" =>
        {
            node.inner.cache.region_endpoints(now)
        }
        None => Vec::new(),
    };
    if !region_endpoints.is_empty() {
        return relay_merge(&node, &method, &uri, &body, &region_endpoints, operator).await;
    }

    // Config and other singleton reads are identical in every region.
    relay_single(
        &node,
        &method,
        &uri,
        &body,
        &node.inner.cache.all_endpoints(now),
        operator,
    )
    .await
}
const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS.add(b'/').add(b'%').add(b'?').add(b'#');

fn decode_path_segment(segment: &str) -> Option<String> {
    percent_decode_str(segment)
        .decode_utf8()
        .ok()
        .map(Into::into)
}

fn extract_check_id(path: &str) -> Option<CheckId> {
    let rest = path.strip_prefix("api/v1/checks/")?;
    let id = rest.split('/').next()?;
    decode_path_segment(id)?.parse().ok()
}

fn extract_incident_id(path: &str) -> Option<String> {
    let path = path.strip_prefix("api/v1/")?;
    let path = path.strip_prefix("admin/").unwrap_or(path);
    let rest = path.strip_prefix("incidents/")?;
    let id = rest
        .split('/')
        .next()
        .filter(|id| !id.is_empty() && *id != "purge-short")?;
    decode_path_segment(id)
}

/// A regional incident id is namespaced at the global boundary. The separator
/// is deliberately a character regional ids do not mint (`:`), making the
/// mapping reversible without global storage.
pub(super) fn composite_incident_id(region: &str, id: &str) -> String {
    format!("{region}:{id}")
}

pub(crate) fn parse_composite_incident_id(id: &str) -> Option<(String, String)> {
    let (region, local) = id.split_once(':')?;
    (!region.is_empty() && !local.is_empty()).then(|| (region.to_string(), local.to_string()))
}
fn rewrite_incident_path(uri: &Uri, local_id: &str) -> String {
    let pq = uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or(uri.path());
    let (path, query) = pq.split_once('?').unwrap_or((pq, ""));
    let marker = "/incidents/";
    let rewritten = path.find(marker).map_or_else(
        || path.to_string(),
        |start| {
            let id_start = start + marker.len();
            let suffix = path[id_start..]
                .find('/')
                .map(|i| &path[id_start + i..])
                .unwrap_or("");
            format!(
                "{}{}{}",
                &path[..id_start],
                utf8_percent_encode(local_id, PATH_SEGMENT_ENCODE_SET),
                suffix
            )
        },
    );
    if query.is_empty() {
        rewritten
    } else {
        format!("{rewritten}?{query}")
    }
}
/// Build a region-bound request, attaching the trust token when the operation
/// is admin so the region authorizes it as the global node acting for an
/// operator. Non-admin reads carry no credential — they hit public endpoints.
pub(super) fn region_request(
    node: &GlobalNode,
    method: &Method,
    base: &str,
    pq: &str,
    body: &Bytes,
    authed: bool,
) -> reqwest::RequestBuilder {
    let url = format!("{}{}", base.trim_end_matches('/'), pq);
    let mut req = node
        .inner
        .client
        .request(method.clone(), &url)
        .body(body.clone());
    if authed {
        if let Some(token) = node.inner.token.as_deref() {
            req = req.bearer_auth(token);
        }
    }
    // A body from the SPA is always JSON; tell the region so it parses it.
    if !body.is_empty() {
        req = req.header(header::CONTENT_TYPE, "application/json");
    }
    req
}
/// Relay a read to the first endpoint that answers, returning its response
/// verbatim. `authed` attaches the trust token (admin reads).
async fn relay_single_path(
    node: &GlobalNode,
    method: &Method,
    pq: &str,
    body: &Bytes,
    endpoints: &[String],
    authed: bool,
) -> Response {
    if endpoints.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to serve this query yet",
        );
    }
    for base in endpoints {
        match region_request(node, method, base, pq, body, authed)
            .send()
            .await
        {
            Ok(resp) => return passthrough(resp).await,
            Err(e) => warn!(base = %base, error = %e, "region relay failed, trying next"),
        }
    }
    error_response(
        StatusCode::BAD_GATEWAY,
        "no region coordinator answered the query",
    )
}
/// Read one coordinator from every owner region concurrently. A merged response
/// is authoritative only when every owner contributed; partial history is more
/// misleading than a visible outage.
pub(super) async fn relay_merge(
    node: &GlobalNode,
    method: &Method,
    uri: &Uri,
    body: &Bytes,
    regions: &[(String, Vec<String>)],
    authed: bool,
) -> Response {
    // Batched chart pages are selected from the global aggregate below. Do not
    // let every region apply its own page first: regional order and membership
    // differ, so that double pagination can omit every globally selected id.
    let pq = if matches!(uri.path(), "/api/v1/uptime" | "/api/v1/latency-points") {
        chart_relay_path(uri)
    } else {
        uri.path_and_query()
            .map(|p| p.as_str())
            .unwrap_or(uri.path())
            .to_string()
    };
    if regions.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to serve this query yet",
        );
    }

    let mut requests = tokio::task::JoinSet::new();
    for (region, endpoints) in regions {
        let node = node.clone();
        let method = method.clone();
        let pq = pq.clone();
        let body = body.clone();
        let region = region.clone();
        let endpoints = endpoints.clone();
        requests.spawn(async move {
            let result = fetch_region_json(&node, &method, &pq, &body, &endpoints, authed).await;
            (region, result)
        });
    }

    let mut merged = Vec::with_capacity(regions.len());
    while let Some(result) = requests.join_next().await {
        match result {
            Ok((region, Ok(value))) => merged.push((region, value)),
            Ok((region, Err(message))) => {
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("region '{region}' is unavailable: {message}"),
                );
            }
            Err(error) => {
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("region relay task failed: {error}"),
                )
            }
        }
    }
    match merge_responses(uri.path(), uri.query(), merged) {
        Ok(mut response) => {
            if matches!(uri.path(), "/api/v1/uptime" | "/api/v1/latency-points") {
                filter_chart_page(node, uri, &mut response.0);
            }
            if uri.path().ends_with("/incidents") {
                attach_incident_groups(node, &mut response.0);
            }
            response.into_response()
        }
        Err(message) => error_response(StatusCode::BAD_GATEWAY, &message),
    }
}
async fn fetch_region_json(
    node: &GlobalNode,
    method: &Method,
    pq: &str,
    body: &Bytes,
    endpoints: &[String],
    authed: bool,
) -> Result<serde_json::Value, String> {
    let mut last_error = String::from("no reachable coordinator");
    for base in endpoints {
        match region_request(node, method, base, pq, body, authed)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => match response.json().await {
                Ok(value) => return Ok(value),
                Err(error) => last_error = format!("invalid JSON: {error}"),
            },
            // A coordinator answered authoritatively. Retrying a peer could
            // mask authorization/client failures or return inconsistent data.
            Ok(response) => return Err(format!("HTTP {}", response.status())),
            Err(error) => last_error = error.to_string(),
        }
    }
    Err(last_error)
}
async fn relay_incident_detail(
    node: &GlobalNode,
    method: &Method,
    pq: &str,
    body: &Bytes,
    endpoints: &[String],
    authed: bool,
    region: &str,
) -> Response {
    let response = relay_single_path(node, method, pq, body, endpoints, authed).await;
    let status = response.status();
    if !status.is_success() {
        return response;
    }
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(mut value) => {
            if let Some(incident) = value.get_mut("incident") {
                rewrite_incident_value(region, incident);
            }
            Json(value).into_response()
        }
        Err(_) => (status, bytes).into_response(),
    }
}
pub(super) fn rewrite_incident_value(region: &str, incident: &mut serde_json::Value) {
    if let Some(id) = incident.get("id").and_then(|value| value.as_str()) {
        incident["id"] = serde_json::Value::String(composite_incident_id(region, id));
    }
    if let Some(origin_check) = incident
        .get("origin_check")
        .and_then(|value| value.as_str())
    {
        incident["origin_check"] = serde_json::Value::String(origin_check.to_string());
    }
    if let Some(updates) = incident
        .get_mut("updates")
        .and_then(|value| value.as_array_mut())
    {
        for update in updates {
            if let Some(id) = update.get("id").and_then(|value| value.as_str()) {
                update["id"] = serde_json::Value::String(composite_incident_id(region, id));
            }
        }
    }
}
async fn relay_single(
    node: &GlobalNode,
    method: &Method,
    uri: &Uri,
    body: &Bytes,
    endpoints: &[String],
    authed: bool,
) -> Response {
    let pq = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path());
    if endpoints.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to serve this query yet",
        );
    }
    for base in endpoints {
        match region_request(node, method, base, pq, body, authed)
            .send()
            .await
        {
            Ok(resp) => return passthrough(resp).await,
            Err(e) => warn!(base = %base, error = %e, "region relay failed, trying next"),
        }
    }
    error_response(
        StatusCode::BAD_GATEWAY,
        "no region coordinator answered the query",
    )
}
/// Fan an admin write out to one live coordinator in every region, for
/// everything the global tier is *not* authoritative for (`global::admin`
/// intercepts checks and sections before this is ever reached — see its
/// module doc). Regions run identical config, so the write must land
/// everywhere or the fleet diverges. The first region's response is returned
/// to the operator; if any region errors (transport or a non-2xx), that
/// failure is surfaced instead so a partial apply is never reported as
/// success.
///
/// This is the old, all-or-nothing mechanism SCALING.md's "Placement-scoped
/// configuration" describes the gap in: a region that is merely stale drops
/// out of `region_endpoints` and misses the write silently, and nothing
/// reconciles it afterwards. Phase 1 closes that gap for the catalogue by
/// routing it through `store::GlobalStore` and `reconcile` instead; widening
/// the fix to notifiers, credentials, rollup policies, maintenance and
/// incidents is Phase 2/3 work (see `store`'s module doc for why each of
/// those is not simply added to the authoritative list today).
async fn fan_out_write(
    node: &GlobalNode,
    method: &Method,
    uri: &Uri,
    body: &Bytes,
    now: i64,
) -> Response {
    let pq = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path());
    let regions = node.inner.cache.region_endpoints(now);
    if regions.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no region is available to apply this change yet",
        );
    }
    let mut first_ok: Option<Response> = None;
    for (region, endpoints) in &regions {
        match apply_to_region(node, method, endpoints, pq, body).await {
            Ok(resp) => {
                if first_ok.is_none() {
                    first_ok = Some(resp);
                }
            }
            Err(msg) => {
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("change failed in region '{region}': {msg}"),
                );
            }
        }
    }
    first_ok.unwrap_or_else(|| {
        error_response(
            StatusCode::BAD_GATEWAY,
            "no region coordinator applied the change",
        )
    })
}
/// Apply a write to a region by trying its coordinators until one answers.
/// A non-2xx from a reachable coordinator is a real rejection (validation, a
/// duplicate name) — return it as the region's error rather than trying the
/// next coordinator, which would only apply the same write twice.
///
/// A `DELETE` that 404s is the exception: delete is idempotent, so a region
/// that is already missing the target has the state the operator asked for.
/// Treating that as failure would make a single drifted region wedge every
/// delete, which is exactly the drift fan-out exists to heal.
async fn apply_to_region(
    node: &GlobalNode,
    method: &Method,
    endpoints: &[String],
    pq: &str,
    body: &Bytes,
) -> Result<Response, String> {
    let mut last_transport_err = String::from("no reachable coordinator");
    for base in endpoints {
        match region_request(node, method, base, pq, body, true)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success()
                    || (method == Method::DELETE && status == StatusCode::NOT_FOUND)
                {
                    return Ok(passthrough(resp).await);
                }
                let text = resp.text().await.unwrap_or_default();
                return Err(format!("HTTP {} {}", status.as_u16(), text));
            }
            Err(e) => last_transport_err = e.to_string(),
        }
    }
    Err(last_transport_err)
}
/// Turn a region's `reqwest::Response` into our `Response`, preserving status
/// and content type so the SPA sees exactly what the coordinator returned.
async fn passthrough(resp: reqwest::Response) -> Response {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let body = resp.bytes().await.unwrap_or_default();
    (status, [(header::CONTENT_TYPE, ct)], body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_path_ids_are_decoded_for_routing_and_reencoded_for_relay() {
        // Checks are addressed by UUID now, not by a percent-encoded name, so
        // the routing path only needs to parse the id back out.
        let id = CheckId::from_u128(0x0d0e_ad10);
        assert_eq!(
            extract_check_id(&format!("api/v1/checks/{id}/history")),
            Some(id)
        );
        let id = extract_incident_id("api/v1/admin/incidents/us%3Aupstream-1").unwrap();
        assert_eq!(
            parse_composite_incident_id(&id),
            Some(("us".into(), "upstream-1".into()))
        );
        let uri = "/api/v1/admin/incidents/us%3Aupstream-1?x=1"
            .parse()
            .unwrap();
        assert_eq!(
            rewrite_incident_path(&uri, "upstream-1"),
            "/api/v1/admin/incidents/upstream-1?x=1"
        );
    }

    #[test]
    fn incident_detail_rewrites_incident_and_update_ids() {
        let mut incident = serde_json::json!({"id": "i1", "updates": [{"id": "u1"}]});
        rewrite_incident_value("us", &mut incident);
        assert_eq!(incident["id"], "us:i1");
        assert_eq!(incident["updates"][0]["id"], "us:u1");
    }
}
