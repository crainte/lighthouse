//! Merging regional status, incident and fleet views into one response.

use super::relay::{composite_incident_id, rewrite_incident_value};
use super::{cache, incidents, GlobalNode};
use axum::http::Uri;
use axum::Json;
use percent_encoding::percent_decode_str;
use std::collections::HashMap;

/// Add a `"groups"` array alongside the merged `"incidents"` list: the same
/// per-region rows, folded by same-check overlapping windows and attributed
/// to a cross-region upstream where one applies. Presentation only — the
/// flat `"incidents"` array this reads is left exactly as `merge_responses`
/// built it, so a caller keyed to individual incident ids (the admin editor,
/// `IncidentEditor`) is unaffected. See `incidents::group_and_attribute` for
/// the rules.
pub(super) fn attach_incident_groups(node: &GlobalNode, response: &mut serde_json::Value) {
    let now = chrono::Utc::now().timestamp_millis();
    // Repair any incident whose title still names its origin check by raw UUID,
    // which happens when a region opened the incident before that check's name
    // had reached its in-memory catalogue. The global tier holds every live
    // region's name for the check, so it can put the display name back before
    // anyone sees it — in the flat list a regional dashboard would fall back
    // to, and in the groups derived from it below. A title that already carries
    // a name is left untouched: only a leading raw id is swapped.
    let facts = node.inner.cache.check_facts(now);
    if let Some(items) = response.get_mut("incidents").and_then(|v| v.as_array_mut()) {
        for item in items.iter_mut() {
            resolve_incident_title(item, &facts);
        }
    }

    let Some(items) = response.get("incidents").and_then(|v| v.as_array()) else {
        return;
    };
    let raw: Vec<incidents::RawIncident> = items
        .iter()
        .filter_map(incidents::RawIncident::from_json)
        .collect();
    if raw.is_empty() {
        return;
    }
    let groups = incidents::group_and_attribute(&raw, &facts, now);
    response["groups"] =
        serde_json::to_value(groups).unwrap_or_else(|_| serde_json::Value::Array(Vec::new()));
}

/// Swap a leading raw origin-check id in an incident title for the check's
/// display name. Only the exact `<origin_check> ...` prefix is replaced, so a
/// title that already reads "dns is down" — or one a human wrote — is left
/// exactly as it is. A check the global tier has never heard a name for (every
/// region holding it is stale) keeps the id, which is still better than
/// inventing one.
fn resolve_incident_title(
    incident: &mut serde_json::Value,
    facts: &std::collections::HashMap<crate::models::CheckId, cache::CheckFacts>,
) {
    let Some(origin) = incident.get("origin_check").and_then(|v| v.as_str()) else {
        return;
    };
    let Ok(id) = origin.parse::<crate::models::CheckId>() else {
        return;
    };
    let Some(title) = incident.get("title").and_then(|v| v.as_str()) else {
        return;
    };
    // Only a title that still leads with the raw id is rewritten, so a title
    // that already carries a name — or one a human typed — is left exactly as
    // it is. An id the global tier has no live name for resolves to the shared
    // "deleted check" label rather than staying a UUID.
    if let Some(rest) = title.strip_prefix(origin) {
        let name = facts
            .get(&id)
            .and_then(|f| f.name.clone())
            .unwrap_or_else(|| crate::models::DELETED_CHECK_LABEL.to_string());
        incident["title"] = serde_json::Value::String(format!("{name}{rest}"));
    }
}

/// Remove status-page selectors before fanning a batched chart request out.
/// The global aggregate supplies the one authoritative page after merging.
pub(super) fn chart_relay_path(uri: &Uri) -> String {
    let query = uri.query().map(|query| {
        query
            .split('&')
            .filter(|pair| {
                !matches!(
                    pair.split_once('=').map(|(key, _)| key),
                    Some("page" | "limit" | "section")
                )
            })
            .collect::<Vec<_>>()
            .join("&")
    });
    // A global page is capped at 1,000 rows. Ask each region for that full
    // candidate set so a regional default page (100) cannot hide ids selected
    // by a later global page.
    match query.as_deref() {
        Some("") | None => format!("{}?limit=1000", uri.path()),
        Some(query) => format!("{}?{query}&limit=1000", uri.path()),
    }
}

/// Keep chart entries to precisely the ids rendered by the global status page.
pub(super) fn filter_chart_page(node: &GlobalNode, uri: &Uri, response: &mut serde_json::Value) {
    let page = query_usize(uri.query(), "page").unwrap_or(0);
    let limit = crate::page::bounded(query_usize(uri.query(), "limit"), 100);
    let section = query_value(uri.query(), "section");
    let mut checks = node
        .inner
        .cache
        .aggregate(chrono::Utc::now().timestamp_millis());
    checks.retain(|check| {
        section
            .as_deref()
            .is_none_or(|want| check.group.as_deref() == Some(want))
    });
    checks.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.check_id.cmp(&b.check_id))
    });
    let start = page.saturating_mul(limit).min(checks.len());
    let end = start.saturating_add(limit).min(checks.len());
    let wanted: Vec<String> = checks[start..end]
        .iter()
        .map(|check| check.check_id.to_string())
        .collect();
    filter_chart_entries(response, &wanted);
}

fn filter_chart_entries(response: &mut serde_json::Value, wanted: &[String]) {
    if let Some(entries) = response
        .get_mut("checks")
        .and_then(|value| value.as_array_mut())
    {
        entries.retain(|entry| {
            entry
                .get("check_id")
                .and_then(|id| id.as_str())
                .is_some_and(|id| wanted.iter().any(|wanted| wanted == id))
        });
        entries.sort_by_key(|entry| {
            entry
                .get("check_id")
                .and_then(|id| id.as_str())
                .and_then(|id| wanted.iter().position(|wanted| wanted == id))
                .unwrap_or(usize::MAX)
        });
    }
}

pub(super) fn query_usize(query: Option<&str>, name: &str) -> Option<usize> {
    query_value(query, name)?.parse().ok()
}

pub(super) fn query_value(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| percent_decode_str(value).decode_utf8_lossy().into_owned())
    })
}

pub(super) fn merge_responses(
    path: &str,
    query: Option<&str>,
    responses: Vec<(String, serde_json::Value)>,
) -> Result<Json<serde_json::Value>, String> {
    let first = responses
        .first()
        .map(|(_, value)| value)
        .cloned()
        .unwrap_or_default();
    // Before the suffix rules: `/api/v1/uptime` also ends with `/uptime`, and
    // the batched shape is a list of the single-check one, not the thing itself.
    if path == "/api/v1/uptime" || path == "/api/v1/latency-points" {
        return merge_check_series(path, responses).map(Json);
    }
    if path.ends_with("/uptime") {
        return merge_uptime(&first, &responses).map(Json);
    }
    if path.ends_with("/latency-sparkline") {
        return merge_latency(&first, &responses).map(Json);
    }

    let key = if path.ends_with("/history") {
        "history"
    } else if path.ends_with("/results") {
        "results"
    } else if path.ends_with("/alerts") {
        "alerts"
    } else {
        "incidents"
    };
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
    for (region, value) in responses {
        for mut item in value
            .get(key)
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let dedup_key = merged_item_key(key, &region, &item);
            if !seen.insert(dedup_key) {
                continue;
            }
            rewrite_merged_identity(key, &region, &mut item);
            items.push(item);
        }
    }
    items.sort_by_key(|item| std::cmp::Reverse(item_timestamp(item)));
    if let Some(limit) = query_limit(query) {
        items.truncate(limit);
    }
    Ok(Json(serde_json::json!({ key: items })))
}

fn merged_item_key(key: &str, region: &str, item: &serde_json::Value) -> String {
    let id = item
        .get("id")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    if key == "alerts" || key == "incidents" {
        return format!("{region}:{id}");
    }
    format!(
        "{}:{}:{}",
        item.get("check_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default(),
        item.get("agent_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default(),
        item_timestamp(item)
    )
}

fn rewrite_merged_identity(key: &str, region: &str, item: &mut serde_json::Value) {
    if key == "incidents" {
        rewrite_incident_value(region, item);
    }
    if key == "alerts" {
        if let Some(id) = item.get("id").and_then(|value| value.as_str()) {
            item["id"] = serde_json::Value::String(composite_incident_id(region, id));
        }
        if let Some(id) = item.get("incident_id").and_then(|value| value.as_str()) {
            item["incident_id"] = serde_json::Value::String(composite_incident_id(region, id));
        }
    }
}

fn item_timestamp(item: &serde_json::Value) -> i64 {
    item.get("timestamp")
        .or_else(|| item.get("started_at"))
        .and_then(|value| value.as_i64())
        .unwrap_or_default()
}

fn query_limit(query: Option<&str>) -> Option<usize> {
    query?.split('&').find_map(|pair| {
        pair.split_once('=')
            .and_then(|(key, value)| (key == "limit").then(|| value.parse().ok()).flatten())
    })
}

/// Merge a batched series — one response per region, each carrying every check
/// that region knows — by grouping on `check_id` and reusing the single-check
/// merges. A check observed in two regions has its buckets summed exactly as
/// the drill-down endpoints already sum them; a check observed in one is
/// carried through unchanged.
fn merge_check_series(
    path: &str,
    responses: Vec<(String, serde_json::Value)>,
) -> Result<serde_json::Value, String> {
    let latency = path.ends_with("latency-points");
    let mut order: Vec<String> = Vec::new();
    let mut by_check: HashMap<String, Vec<(String, serde_json::Value)>> = HashMap::new();
    for (region, value) in &responses {
        for entry in value["checks"].as_array().into_iter().flatten() {
            let Some(id) = entry.get("check_id").and_then(|value| value.as_str()) else {
                continue;
            };
            by_check
                .entry(id.to_string())
                .or_insert_with(|| {
                    order.push(id.to_string());
                    Vec::new()
                })
                .push((region.clone(), entry.clone()));
        }
    }

    let mut checks = Vec::with_capacity(order.len());
    for id in order {
        let entries = by_check.remove(&id).unwrap_or_default();
        let first = entries
            .first()
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        let mut merged = if latency {
            let mut merged = merge_latency(&first, &entries)?;
            // The single-check endpoint answers with a per-agent breakdown, an
            // overall band and its own window; the batched shape carries none
            // of them, and the page would parse them once per check on it.
            if let Some(object) = merged.as_object_mut() {
                object.remove("overall");
                object.remove("agents");
                object.remove("from");
                object.remove("to");
                object.remove("buckets");
            }
            merged
        } else {
            merge_uptime(&first, &entries)?
        };
        // Display name is config, identical in every region, and not something
        // either single-check merge carries.
        if let Some(name) = first.get("name") {
            merged["name"] = name.clone();
        }
        checks.push(merged);
    }

    let first = responses
        .first()
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    let mut out = serde_json::json!({ "checks": checks });
    for key in ["from", "to", "days", "buckets"] {
        if let Some(value) = first.get(key) {
            out[key] = value.clone();
        }
    }
    Ok(out)
}

fn merge_uptime(
    first: &serde_json::Value,
    responses: &[(String, serde_json::Value)],
) -> Result<serde_json::Value, String> {
    let grids = bucket_grids(responses, "buckets", |bucket| {
        Some((bucket.get("start")?.as_i64()?, bucket.get("end")?.as_i64()?))
    })?;
    let mut totals = vec![(0_u64, 0_u64, 0.0_f64, 0_u64); grids.first().map_or(0, Vec::len)];
    for (_, value) in responses {
        for (index, bucket) in value["buckets"].as_array().unwrap().iter().enumerate() {
            totals[index].0 += bucket["up"].as_u64().unwrap_or(0);
            totals[index].1 += bucket["total"].as_u64().unwrap_or(0);
            if let Some(uptime) = bucket["uptime"].as_f64() {
                totals[index].2 += uptime;
                totals[index].3 += 1;
            }
        }
    }
    let buckets = first["buckets"].as_array().unwrap().iter().zip(totals).map(|(bucket, (up, total, uptime_sum, uptime_regions))| {
        serde_json::json!({"start": bucket["start"], "end": bucket["end"], "up": up, "total": total, "uptime": (uptime_regions > 0).then(|| uptime_sum / uptime_regions as f64)})
    }).collect::<Vec<_>>();
    let overall = responses
        .iter()
        .filter_map(|(_, value)| value["overall_uptime"].as_f64())
        .fold((0.0_f64, 0_u64), |(sum, count), uptime| {
            (sum + uptime, count + 1)
        });
    Ok(serde_json::json!({
        "check_id": first["check_id"],
        "days": first["days"],
        "overall_uptime": (overall.1 > 0).then(|| overall.0 / overall.1 as f64),
        "buckets": buckets
    }))
}

fn bucket_grids<F>(
    responses: &[(String, serde_json::Value)],
    key: &str,
    mut bounds: F,
) -> Result<Vec<Vec<(i64, i64)>>, String>
where
    F: FnMut(&serde_json::Value) -> Option<(i64, i64)>,
{
    let mut grids = Vec::with_capacity(responses.len());
    let mut expected_len = None;
    for (region, value) in responses {
        let buckets = value[key]
            .as_array()
            .ok_or_else(|| format!("region '{region}' returned no {key} array"))?;
        if let Some(length) = expected_len {
            if buckets.len() != length {
                return Err(format!(
                    "incompatible {key} grids: region '{region}' has {} buckets, expected {length}",
                    buckets.len()
                ));
            }
        } else {
            expected_len = Some(buckets.len());
        }
        let grid = buckets.iter().enumerate().map(|(index, bucket)| {
            bounds(bucket).ok_or_else(|| format!("incompatible {key} grid: region '{region}' bucket {index} has invalid bounds"))
        }).collect::<Result<Vec<_>, _>>()?;
        if grid.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(format!(
                "incompatible {key} grid: region '{region}' buckets are not ordered"
            ));
        }
        grids.push(grid);
    }
    Ok(grids)
}

fn merge_latency(
    first: &serde_json::Value,
    responses: &[(String, serde_json::Value)],
) -> Result<serde_json::Value, String> {
    let grids = bucket_grids(responses, "points", |point| {
        point
            .get("timestamp")?
            .as_i64()
            .map(|timestamp| (timestamp, timestamp))
    })?;
    let mut points = vec![(0_u64, 0_u64, 0.0); grids.first().map_or(0, Vec::len)];
    let mut overall = (0_u64, 0_u64, None, 0.0, 0_u64);
    let mut agents: HashMap<String, (u64, u64, Option<u64>, f64, u64)> = HashMap::new();
    for (_, value) in responses {
        for (index, point) in value["points"].as_array().unwrap().iter().enumerate() {
            let up = point["up"].as_u64().unwrap_or(0);
            points[index].0 += point["attempts"].as_u64().unwrap_or(0);
            points[index].1 += up;
            points[index].2 += point["avg_latency_ms"].as_f64().unwrap_or(0.0) * up as f64;
        }
        merge_latency_band(&mut overall, value.get("overall"));
        for agent in value["agents"].as_array().into_iter().flatten() {
            let Some(id) = agent.get("agent_id").and_then(|value| value.as_str()) else {
                continue;
            };
            merge_latency_band(
                agents.entry(id.to_string()).or_insert((0, 0, None, 0.0, 0)),
                Some(agent),
            );
        }
    }
    let points = first["points"].as_array().unwrap().iter().zip(points).map(|(point, (attempts, up, sum))| {
        serde_json::json!({"timestamp": point["timestamp"], "attempts": attempts, "up": up, "avg_latency_ms": (up > 0).then(|| sum / up as f64)})
    }).collect::<Vec<_>>();
    let agents = agents
        .into_iter()
        .map(|(agent_id, band)| {
            let mut value = latency_band_json(band);
            value["agent_id"] = serde_json::Value::String(agent_id);
            value
        })
        .collect::<Vec<_>>();
    Ok(
        serde_json::json!({"check_id": first["check_id"], "from": first["from"], "to": first["to"], "buckets": points.len(), "points": points, "overall": latency_band_json(overall), "agents": agents}),
    )
}

fn merge_latency_band(
    target: &mut (u64, u64, Option<u64>, f64, u64),
    band: Option<&serde_json::Value>,
) {
    let Some(band) = band else { return };
    let attempts = band
        .get("attempts")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let up = band.get("up").and_then(|value| value.as_u64()).unwrap_or(0);
    target.0 += attempts;
    target.1 += up;
    if let Some(min) = band.get("min_ms").and_then(|value| value.as_u64()) {
        target.2 = Some(target.2.map_or(min, |current| current.min(min)));
    }
    if let Some(avg) = band.get("avg_ms").and_then(|value| value.as_f64()) {
        target.3 += avg * up as f64;
    }
    if let Some(max) = band.get("max_ms").and_then(|value| value.as_u64()) {
        target.4 = target.4.max(max);
    }
}

fn latency_band_json(
    (attempts, up, min, sum, max): (u64, u64, Option<u64>, f64, u64),
) -> serde_json::Value {
    serde_json::json!({"attempts": attempts, "up": up, "min_ms": min, "avg_ms": (up > 0).then(|| sum / up as f64), "max_ms": (up > 0).then_some(max)})
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A region can bake a raw check id into an incident title when it opens
    /// the incident before the check's name has reached its catalogue. The
    /// global tier holds every live region's name for the check, so it puts the
    /// name back before anyone sees it — and a title that already carries a
    /// name, or one a human wrote, is left untouched.
    #[test]
    fn incident_titles_leading_with_a_raw_id_are_resolved_to_the_name() {
        let id = crate::models::new_check_id();
        let names = std::collections::HashMap::from([(
            id,
            cache::CheckFacts {
                name: Some("dns".to_string()),
                ..Default::default()
            },
        )]);

        // The bug: title is `<id> is down`.
        let mut baked = serde_json::json!({
            "origin_check": id.to_string(),
            "title": format!("{id} is down"),
        });
        resolve_incident_title(&mut baked, &names);
        assert_eq!(baked["title"], "dns is down");

        // A correct title is not touched.
        let mut good = serde_json::json!({
            "origin_check": id.to_string(),
            "title": "dns is down",
        });
        resolve_incident_title(&mut good, &names);
        assert_eq!(good["title"], "dns is down");

        // A degraded suffix is preserved — only the leading id is swapped.
        let mut degraded = serde_json::json!({
            "origin_check": id.to_string(),
            "title": format!("{id} partial outage in us, tw"),
        });
        resolve_incident_title(&mut degraded, &names);
        assert_eq!(degraded["title"], "dns partial outage in us, tw");

        // An id no live region names resolves to the shared label, never the
        // raw UUID.
        let orphan = crate::models::new_check_id();
        let mut deleted = serde_json::json!({
            "origin_check": orphan.to_string(),
            "title": format!("{orphan} is down"),
        });
        resolve_incident_title(&mut deleted, &names);
        assert_eq!(
            deleted["title"],
            format!("{} is down", crate::models::DELETED_CHECK_LABEL)
        );

        // A hand-opened incident (no origin check) keeps its human title.
        let mut manual = serde_json::json!({
            "origin_check": serde_json::Value::Null,
            "title": "Scheduled maintenance",
        });
        resolve_incident_title(&mut manual, &names);
        assert_eq!(manual["title"], "Scheduled maintenance");
    }

    /// The status page asks one question for the whole estate, but a check is
    /// only observed where its agents are. Relaying that question to a single
    /// region answers "no data" for every check that region does not probe,
    /// which the page renders as "Collecting latency data…" — so the batched
    /// series must be asked of every region and merged per check.
    #[test]
    fn batched_latency_points_merge_every_region_per_check() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({
                    "from": 0, "to": 100, "buckets": 2,
                    "checks": [
                        {"check_id": "probed-in-us", "name": "web", "points": [
                            {"timestamp": 0, "attempts": 4, "up": 4, "avg_latency_ms": 100.0},
                            {"timestamp": 50, "attempts": 4, "up": 2, "avg_latency_ms": 200.0}
                        ]},
                        {"check_id": "probed-in-both", "points": [
                            {"timestamp": 0, "attempts": 2, "up": 2, "avg_latency_ms": 10.0},
                            {"timestamp": 50, "attempts": 2, "up": 2, "avg_latency_ms": 10.0}
                        ]}
                    ]
                }),
            ),
            (
                "tw".into(),
                serde_json::json!({
                    "from": 0, "to": 100, "buckets": 2,
                    "checks": [
                        {"check_id": "probed-in-us", "name": "web", "points": [
                            {"timestamp": 0, "attempts": 0, "up": 0, "avg_latency_ms": null},
                            {"timestamp": 50, "attempts": 0, "up": 0, "avg_latency_ms": null}
                        ]},
                        {"check_id": "probed-in-both", "points": [
                            {"timestamp": 0, "attempts": 2, "up": 2, "avg_latency_ms": 30.0},
                            {"timestamp": 50, "attempts": 2, "up": 0, "avg_latency_ms": null}
                        ]}
                    ]
                }),
            ),
        ];
        let Json(merged) = merge_responses("/api/v1/latency-points", None, responses).unwrap();
        let checks = merged["checks"].as_array().unwrap();
        assert_eq!(checks.len(), 2, "every check, once");

        let us = &checks[0];
        assert_eq!(us["check_id"], "probed-in-us");
        assert_eq!(us["name"], "web", "display name survives the merge");
        assert_eq!(
            us["points"][0]["attempts"], 4,
            "a region that does not probe the check must not erase the one that does"
        );

        let both = &checks[1];
        assert_eq!(both["points"][0]["attempts"], 4, "2 + 2 attempts");
        assert_eq!(
            both["points"][0]["avg_latency_ms"], 20.0,
            "latency is weighted by successful attempts, not averaged twice"
        );
        assert_eq!(both["points"][1]["attempts"], 4);
        assert_eq!(
            both["points"][1]["avg_latency_ms"], 10.0,
            "a region with no successes contributes none of the mean"
        );
    }

    /// `/api/v1/uptime` also ends with `/uptime`, so the batched shape must be
    /// recognised before the single-check rule that would read its fields.
    #[test]
    fn batched_uptime_merges_per_check_rather_than_as_one_check() {
        let bucket = |up: u64, total: u64| {
            serde_json::json!({"start": 0, "end": 10, "up": up, "total": total,
                               "uptime": up as f64 / total as f64})
        };
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"days": 1, "checks": [
                    {"check_id": "c1", "name": "web", "days": 1, "overall_uptime": 1.0,
                     "buckets": [bucket(10, 10)]}
                ]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"days": 1, "checks": [
                    {"check_id": "c1", "name": "web", "days": 1, "overall_uptime": 0.5,
                     "buckets": [bucket(5, 10)]}
                ]}),
            ),
        ];
        let Json(merged) = merge_responses("/api/v1/uptime", None, responses).unwrap();
        let checks = merged["checks"].as_array().unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0]["buckets"][0]["up"], 15);
        assert_eq!(checks[0]["buckets"][0]["total"], 20);
        assert_eq!(checks[0]["name"], "web");
        assert_eq!(merged["days"], 1);
    }

    #[test]
    fn chart_page_filter_keeps_the_global_page_after_multi_region_merge() {
        // More than one regional page: ids 100..199 would never be returned if
        // each region were sent page=1 before the global merge. The selected
        // page is instead applied once to the merged global ids.
        let wanted: Vec<String> = (100..200).map(|id| format!("check-{id:03}")).collect();
        let mut merged = serde_json::json!({"checks": (0..202).rev().map(|id| {
            serde_json::json!({"check_id": format!("check-{id:03}")})
        }).collect::<Vec<_>>()});
        filter_chart_entries(&mut merged, &wanted);
        let checks = merged["checks"].as_array().unwrap();
        assert_eq!(checks.len(), 100);
        assert_eq!(checks.first().unwrap()["check_id"], "check-100");
        assert_eq!(checks.last().unwrap()["check_id"], "check-199");
    }

    #[test]
    fn chart_relays_do_not_forward_regional_page_selectors() {
        let uri: Uri = "/api/v1/uptime?days=30&page=1&limit=100&section=edge&buckets=90"
            .parse()
            .unwrap();
        assert_eq!(
            chart_relay_path(&uri),
            "/api/v1/uptime?days=30&buckets=90&limit=1000"
        );
    }

    #[test]
    fn merged_lists_deduplicate_and_apply_the_requested_limit() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"alerts": [{"id": "a", "timestamp": 10}, {"id": "b", "timestamp": 30}]}),
            ),
            (
                "us".into(),
                serde_json::json!({"alerts": [{"id": "a", "timestamp": 10}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"alerts": [{"id": "a", "timestamp": 20}]}),
            ),
        ];
        let Json(merged) = merge_responses("/api/v1/alerts", Some("limit=2"), responses).unwrap();
        let alerts = merged["alerts"].as_array().unwrap();
        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[0]["id"], "us:b");
        assert_eq!(alerts[1]["id"], "tw:a");
    }

    #[test]
    fn latency_merge_weights_bands_and_preserves_agents() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "from": 0, "to": 10, "points": [{"timestamp": 0, "attempts": 2, "up": 1, "avg_latency_ms": 100.0}], "overall": {"attempts": 2, "up": 1, "min_ms": 100, "avg_ms": 100.0, "max_ms": 100}, "agents": [{"agent_id": "a", "attempts": 2, "up": 1, "min_ms": 100, "avg_ms": 100.0, "max_ms": 100}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "from": 0, "to": 10, "points": [{"timestamp": 0, "attempts": 3, "up": 2, "avg_latency_ms": 200.0}], "overall": {"attempts": 3, "up": 2, "min_ms": 150, "avg_ms": 200.0, "max_ms": 250}, "agents": [{"agent_id": "b", "attempts": 3, "up": 2, "min_ms": 150, "avg_ms": 200.0, "max_ms": 250}]}),
            ),
        ];
        let merged = merge_latency(&responses[0].1, &responses).unwrap();
        assert_eq!(merged["points"][0]["avg_latency_ms"], 166.66666666666666);
        assert_eq!(merged["overall"]["avg_ms"], 166.66666666666666);
        let agents = merged["agents"].as_array().unwrap();
        assert_eq!(agents.len(), 2);
        assert!(agents.iter().all(|agent| agent.get("band").is_none()));
        assert!(agents.iter().all(|agent| agent["attempts"].is_number()));
        assert!(agents.iter().all(|agent| agent["avg_ms"].is_number()));
    }

    #[test]
    fn bucket_merges_use_first_region_grid_despite_millisecond_skew() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "days": 1, "buckets": [{"start": 1_000, "end": 2_000, "up": 2, "total": 2}, {"start": 2_000, "end": 3_000, "up": 1, "total": 2}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "days": 1, "buckets": [{"start": 1_003, "end": 2_003, "up": 3, "total": 3}, {"start": 2_003, "end": 3_003, "up": 2, "total": 2}]}),
            ),
        ];
        let merged = merge_uptime(&responses[0].1, &responses).unwrap();
        assert_eq!(merged["buckets"][0]["start"], 1_000);
        assert_eq!(merged["buckets"][0]["end"], 2_000);
        assert_eq!(merged["buckets"][0]["up"], 5);
        assert_eq!(merged["buckets"][0]["total"], 5);

        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "from": 10, "to": 20, "points": [{"timestamp": 10_000, "attempts": 2, "up": 2, "avg_latency_ms": 100.0}], "overall": {}}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "from": 13, "to": 23, "points": [{"timestamp": 10_003, "attempts": 3, "up": 3, "avg_latency_ms": 200.0}], "overall": {}}),
            ),
        ];
        let merged = merge_latency(&responses[0].1, &responses).unwrap();
        assert_eq!(merged["from"], 10);
        assert_eq!(merged["to"], 20);
        assert_eq!(merged["points"][0]["timestamp"], 10_000);
        assert_eq!(merged["points"][0]["attempts"], 5);
    }

    #[test]
    fn uptime_merge_keeps_time_weighted_state_for_empty_attempt_buckets() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "days": 1, "overall_uptime": 1.0, "buckets": [{"start": 1, "end": 2, "up": 0, "total": 0, "uptime": 1.0}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "days": 1, "overall_uptime": 0.5, "buckets": [{"start": 1, "end": 2, "up": 0, "total": 0, "uptime": 0.5}]}),
            ),
        ];

        let merged = merge_uptime(&responses[0].1, &responses).unwrap();

        assert_eq!(merged["buckets"][0]["total"], 0);
        assert_eq!(merged["buckets"][0]["uptime"], 0.75);
        assert_eq!(merged["overall_uptime"], 0.75);
    }

    #[test]
    fn incompatible_bucket_grids_return_clear_errors() {
        let responses = vec![
            (
                "us".into(),
                serde_json::json!({"check_id": "api", "days": 1, "buckets": [{"start": 1, "end": 2, "up": 1, "total": 1}]}),
            ),
            (
                "tw".into(),
                serde_json::json!({"check_id": "api", "days": 1, "buckets": []}),
            ),
        ];
        assert_eq!(
            merge_uptime(&responses[0].1, &responses).unwrap_err(),
            "incompatible buckets grids: region 'tw' has 0 buckets, expected 1"
        );
    }
}
