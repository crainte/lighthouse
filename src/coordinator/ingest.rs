use super::*;
use crate::models::{eval, CheckResult, Status};

/// Configured metadata for a check, looked up by its immutable id. The store
/// only knows check ids, so anything declared in config (grouping,
/// expectations, probe type) has to be joined back in here.
#[derive(Clone)]
pub(in crate::coordinator) struct CheckMeta {
    /// The mutable display name, carried here because everything that needs
    /// human-facing text (incident titles, notification bodies) only has the
    /// id to look the check up by.
    pub(in crate::coordinator) name: String,
    pub(in crate::coordinator) kind: crate::models::CheckKind,
    pub(in crate::coordinator) group: Option<String>,
    pub(in crate::coordinator) latency_graph: bool,
    pub(in crate::coordinator) expect_status: Option<u16>,
    /// When the probe runs, as cron. The target lives in storage and is served
    /// only to authenticated admins and agents, never on the public endpoint.
    pub(in crate::coordinator) cron: String,
    /// Published on the public status page.
    pub(in crate::coordinator) visible: bool,
    /// Lua deciding UP/DOWN, replacing the built-in rule when present.
    pub(in crate::coordinator) eval: Option<String>,
    /// Display order within the section.
    pub(in crate::coordinator) position: i32,
    /// What the check reads as before its first result lands.
    pub(in crate::coordinator) default_status: Status,
    /// How many consecutive DOWN observations confirm an outage. Fewer than
    /// this leaves the check reading as it did before, so a single dropped
    /// probe does not page anyone.
    pub(in crate::coordinator) failures_before_down: u32,
    /// Probing at all. A disabled check has no agent expectation to violate.
    pub(in crate::coordinator) enabled: bool,
    /// Whether a confirmed state change on this check reaches the notifiers.
    pub(in crate::coordinator) notify: bool,
    /// Notifiers by name. Empty routes to whichever are marked default.
    pub(in crate::coordinator) notifiers: Vec<String>,
    /// Agents named explicitly. Empty means every agent.
    pub(in crate::coordinator) agents: Vec<String>,
    /// Upstream checks. A DOWN here joins an upstream's open incident rather
    /// than opening its own. One hop only — see `CheckConfig::depends_on`.
    pub(in crate::coordinator) depends_on: Vec<crate::models::CheckId>,
    pub(in crate::coordinator) rollup: crate::config::RollupOverrides,
    pub(in crate::coordinator) min_incident_secs: u64,
    pub(in crate::coordinator) min_recovery_secs: u64,
    pub(in crate::coordinator) notify_degraded: bool,
    // Retained as the effective per-check SLA interpretation for consumers that
    // do not have the original CheckConfig override available.
    #[allow(dead_code)]
    pub(in crate::coordinator) degraded_is_downtime: bool,
}

/// Apply a check's Lua script to one result.
///
/// A broken script must not silently invent a verdict: it is reported as
/// `Unknown` with the reason attached to the result, so the dashboard shows
/// "why" instead of an unexplained outage.
fn script_verdict(source: &str, r: &mut CheckResult) -> Status {
    let input = crate::script::EvalInput {
        status: r.status_code.unwrap_or(0),
        latency_ms: r.latency_ms,
        body: r.body.clone(),
        headers: r.headers.clone().unwrap_or_default(),
        metric: r.metric,
        metrics: r.metrics.clone().unwrap_or_default(),
    };
    match crate::script::evaluate(source, &input) {
        Ok(outcome) => {
            if let Some(override_ms) = outcome.latency_ms {
                r.latency_ms = override_ms;
            }
            if outcome.up {
                Status::Up
            } else {
                Status::Down
            }
        }
        Err(e) => {
            warn!(check = %r.check_id, error = %e, "eval script failed");
            r.error = Some(format!("eval script failed: {e}"));
            r.failure = Some(crate::models::FailureKind::ScriptFailed);
            Status::Unknown
        }
    }
}

/// What an incident records as its cause: the failure kind when one is known,
/// otherwise the probe's own words.
fn incident_details(r: &CheckResult) -> Option<String> {
    r.failure.map(|f| f.summary()).or_else(|| r.error.clone())
}

fn incident_title(
    check_id: &str,
    state: NodeState,
    failing_regions: &std::collections::BTreeSet<String>,
) -> String {
    if state == NodeState::Degraded {
        let regions = failing_regions
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        if regions.is_empty() {
            format!("{check_id} partial outage")
        } else {
            format!("{check_id} partial outage in {regions}")
        }
    } else {
        format!("{check_id} is {}", state.as_str())
    }
}

fn region_change_narrative(
    previous: &std::collections::BTreeSet<String>,
    current: &std::collections::BTreeSet<String>,
) -> String {
    let list = |regions: &std::collections::BTreeSet<String>| {
        if regions.is_empty() {
            "none".to_string()
        } else {
            regions.iter().cloned().collect::<Vec<_>>().join(", ")
        }
    };
    format!(
        "Affected failing regions changed from {} to {}.",
        list(previous),
        list(current)
    )
}

/// A span of milliseconds as human prose, for the auto-written resolve update.
/// Coarse on purpose: "4 minutes" is what a status page says, not "4m 12s".
fn format_duration(ms: i64) -> String {
    let mins = ms.max(0) / 60_000;
    match mins {
        0 => "less than a minute".to_string(),
        1 => "1 minute".to_string(),
        m if m < 60 => format!("{m} minutes"),
        m if m < 120 => format!("1 hour {} minutes", m % 60),
        m if m < 1440 => format!("{} hours {} minutes", m / 60, m % 60),
        m => format!("{} days {} hours", m / 1440, (m % 1440) / 60),
    }
}

/// Derive the in-memory fields consumers need from one persisted check.
pub(in crate::coordinator) fn check_meta(
    c: &CheckConfig,
    defaults: crate::config::RollupDefaults,
) -> CheckMeta {
    CheckMeta {
        name: c.name.clone(),
        kind: c.kind,
        group: c.group.clone(),
        latency_graph: c.latency_graph,
        expect_status: c.expect.status,
        cron: c.cron.clone(),
        visible: c.visible,
        eval: c.eval.clone(),
        position: c.position,
        failures_before_down: c.failures_before_down,
        enabled: c.enabled,
        notify: c.notify,
        notifiers: c.notifiers.clone(),
        agents: c.agents.clone(),
        depends_on: c.depends_on.clone(),
        default_status: match c.default_status {
            crate::config::DefaultStatus::Up => Status::Up,
            crate::config::DefaultStatus::Down => Status::Down,
            crate::config::DefaultStatus::Unknown => Status::Unknown,
        },
        rollup: c.rollup,
        min_incident_secs: c.min_incident_secs.unwrap_or(defaults.min_incident_secs),
        min_recovery_secs: c.min_recovery_secs.unwrap_or(defaults.min_recovery_secs),
        notify_degraded: c.notify_degraded.unwrap_or(defaults.notify_degraded),
        degraded_is_downtime: c
            .degraded_is_downtime
            .unwrap_or(defaults.degraded_is_downtime),
    }
}

/// Index config rows by check id for the lookups ingest and the status page do.
pub(in crate::coordinator) fn meta_by_id(
    checks: &[CheckConfig],
    defaults: crate::config::RollupDefaults,
) -> std::collections::HashMap<crate::models::CheckId, CheckMeta> {
    checks
        .iter()
        .map(|check| (check.id, check_meta(check, defaults)))
        .collect()
}

impl Coordinator {
    fn notify_targets(
        &self,
        check_id: crate::models::CheckId,
        state: NodeState,
        at: i64,
    ) -> Option<Vec<String>> {
        if self
            .inner
            .maintenance
            .read()
            .iter()
            .any(|w| w.covers(check_id, at))
        {
            return None;
        }
        match self.inner.checks.read().get(&check_id) {
            Some(m) if !m.notify || (state == NodeState::Degraded && !m.notify_degraded) => None,
            Some(m) => Some(m.notifiers.clone()),
            None => Some(Vec::new()),
        }
    }

    fn attach_or_open(
        &self,
        store: &storage::Storage,
        r: &CheckResult,
        state: NodeState,
        failing_regions: &std::collections::BTreeSet<String>,
        ts: i64,
    ) -> anyhow::Result<(String, bool, bool)> {
        let name = self.check_name(r.check_id);
        if let Some(existing) = store.open_incident_for(r.check_id)? {
            let title = incident_title(&name, state, failing_regions);
            let changed = existing.title != title;
            if changed {
                store.edit_incident(&existing.id, &title, existing.started_at, existing.global)?;
            }
            return Ok((existing.id, false, changed));
        }
        let upstreams: Vec<crate::models::CheckId> = {
            let by_id = self.inner.checks.read();
            by_id
                .get(&r.check_id)
                .map(|m| m.depends_on.clone())
                .unwrap_or_default()
        };
        for up in upstreams {
            if let Some(inc) = store.open_incident_for(up)? {
                store.attach_check(&inc.id, r.check_id, storage::AffectSource::Dependency)?;
                return Ok((inc.id, false, false));
            }
        }
        let id = format!("{}-{ts}", r.check_id);
        let cause = incident_details(r).unwrap_or_else(|| format!("check is {}", state.as_str()));
        store.create_incident(
            &id,
            &incident_title(&name, state, failing_regions),
            storage::IncidentState::Detected,
            ts,
            false,
            Some(r.check_id),
            &[storage::IncidentCheckRow {
                check_id: r.check_id,
                source: storage::AffectSource::Origin,
            }],
            Some((&cause, None)),
        )?;
        Ok((id, true, false))
    }

    /// The mutable display name for a check id, or the id itself (rendered)
    /// when the coordinator has no config metadata for it \u2014 a check deleted
    /// between the observation and this lookup, or one this node has not
    /// replicated config for yet. Never fails: this is for human-facing text,
    /// not identity.
    /// A check's display name, or `DELETED_CHECK_LABEL` when the id resolves to
    /// nothing. It never falls back to the raw UUID: this feeds incident and
    /// notification titles, and a user should never be shown an id where a name
    /// belongs — the same choice `alert_name` already makes. A missing name is
    /// almost always a check deleted after the event it names; the one other
    /// cause, a config-reconciliation window where the id is probeable before
    /// its name has reached this map, heals on the next read now that titles
    /// are resolved at read time rather than trusted from when they were baked.
    pub(in crate::coordinator) fn check_name(&self, check_id: crate::models::CheckId) -> String {
        self.inner
            .checks
            .read()
            .get(&check_id)
            .map(|m| m.name.clone())
            .unwrap_or_else(|| crate::models::DELETED_CHECK_LABEL.to_string())
    }

    /// Rewrite incident DTO titles that still lead with their origin check's
    /// raw id, putting the current display name back. Read-time rather than
    /// stored, so a title baked during a config-reconciliation window (id known
    /// before name) heals the moment the name is present, and a check renamed
    /// after an incident opened shows its new name if the stored title had
    /// fallen back to the id. A title that already carries a name, or one a
    /// human wrote, never leads with the id and is left untouched. Mirrors the
    /// global tier's `resolve_incident_title`.
    pub(in crate::coordinator) fn resolve_incident_titles(
        &self,
        incidents: &mut [routes::incidents::IncidentDto],
    ) {
        for dto in incidents.iter_mut() {
            let Some(origin) = dto.origin_check.as_deref() else {
                continue;
            };
            let Some(rest) = dto.title.strip_prefix(origin) else {
                continue;
            };
            let name = origin
                .parse()
                .map(|id| self.check_name(id))
                .unwrap_or_else(|_| crate::models::DELETED_CHECK_LABEL.to_string());
            dto.title = format!("{name}{rest}");
        }
    }

    pub(in crate::coordinator) fn alert_name(
        &self,
        scope: storage::AlertScope,
        node_id: &str,
    ) -> String {
        let check_name = |id| {
            self.inner
                .checks
                .read()
                .get(&id)
                .map(|meta| meta.name.clone())
                .unwrap_or_else(|| crate::models::DELETED_CHECK_LABEL.to_string())
        };

        if scope == storage::AlertScope::Check {
            return node_id
                .parse()
                .map(check_name)
                .unwrap_or_else(|_| node_id.to_string());
        }

        let Some(rest) = node_id.strip_prefix("check:") else {
            return node_id.to_string();
        };
        let Some((check_id, location)) = rest.split_once('/') else {
            return node_id.to_string();
        };
        let Ok(check_id) = check_id.parse() else {
            return node_id.to_string();
        };
        format!(
            "{} / {}",
            check_name(check_id),
            location.replace('/', " / ")
        )
    }

    /// Resolve an incident if every gating check has recovered. Returns whether
    /// it did.
    ///
    /// "Recovered" is the up-side mirror of `failures_before_down`: a check
    /// that needs three failures before anyone believes it is down needs three
    /// successes before anyone believes it is back. At the default of 1 this is
    /// the classic behaviour — resolve on the first UP.
    ///
    /// Manual attachments are excluded: an operator naming a check they suspect
    /// must not be able to hold an incident open forever.
    ///
    /// **Caller invariant: `inc` must be an incident the coordinator owns.**
    /// The loop below falls through on an empty gating set and resolves, so an
    /// incident whose attachments are all `manual` would close itself the
    /// moment any check reported UP. `open_incident_for` is what upholds this,
    /// by returning only incidents with an `origin_check` — and an incident
    /// with an origin always carries the matching `Origin` attachment, so the
    /// set is never empty here. A future caller that sources incidents any
    /// other way has to re-establish that, or check for an empty set itself.
    fn try_resolve(
        &self,
        store: &storage::Storage,
        inc: &storage::IncidentRow,
        ts: i64,
    ) -> anyhow::Result<bool> {
        let current: std::collections::HashMap<_, _> = store
            .list_current_state()?
            .into_iter()
            .map(|row| (row.check_id, row.state))
            .collect();
        for check_id in inc.gating_checks() {
            if current.get(&check_id) != Some(&NodeState::Up) {
                return Ok(false);
            }
        }
        let downtime = format_duration(ts - inc.started_at);
        store.add_update(
            &inc.id,
            storage::IncidentState::Resolved,
            &format!("Recovered automatically. Total downtime: {downtime}."),
            None,
            ts,
        )?;
        Ok(true)
    }

    fn reconcile_open_incidents(
        &self,
        store: &storage::Storage,
        ts: i64,
    ) -> anyhow::Result<Vec<storage::IncidentRow>> {
        let mut resolved = Vec::new();
        for inc in store.list_incidents(1000, Some(true), None)? {
            if inc.origin_check.is_some() && self.try_resolve(store, &inc, ts)? {
                if let Some(row) = store.incident(&inc.id)? {
                    resolved.push(row);
                }
            }
        }
        Ok(resolved)
    }

    /// Evaluate and persist observations, then publish one rolled-up root per
    /// affected check. Everything downstream is driven solely by root changes.
    pub async fn ingest(&self, mut results: Vec<CheckResult>) {
        let state_wait_started = Instant::now();
        let _state_write = self.inner.state_write.lock().await;
        if self.inner.timings.enabled {
            self.inner.timings.local_state_wait.observe(
                "state_write",
                "local_wait",
                state_wait_started.elapsed(),
            );
        }
        let state_hold_started = Instant::now();
        let check_ids: std::collections::HashSet<_> = results.iter().map(|r| r.check_id).collect();
        let mut staged = self.inner.tracker.lock().snapshot_checks(&check_ids);
        let mut intermediate_cache: std::collections::HashMap<_, _> = self
            .inner
            .intermediate_states
            .lock()
            .iter()
            .filter(|((id, _), _)| check_ids.contains(id))
            .map(|(key, value)| (key.clone(), *value))
            .collect();
        results.sort_by(|a, b| (&a.agent_id, a.timestamp).cmp(&(&b.agent_id, b.timestamp)));
        let mut rows = Vec::with_capacity(results.len());
        let mut affected: std::collections::HashMap<crate::models::CheckId, (CheckResult, i64)> =
            std::collections::HashMap::new();
        let mut leaf_updates = Vec::with_capacity(results.len());

        {
            let tracker = &mut staged;
            for mut r in results {
                let meta = self.inner.checks.read().get(&r.check_id).cloned();
                let status = match meta.as_ref().and_then(|m| m.eval.as_deref()) {
                    Some(src) => script_verdict(src, &mut r),
                    None => eval(&r, meta.as_ref().and_then(|m| m.expect_status)),
                };
                let ts = r.timestamp.timestamp_millis();
                let confirmed = tracker.observe(
                    r.check_id,
                    &r.agent_id,
                    &r.zone,
                    &r.region,
                    status,
                    meta.as_ref().map(|m| m.failures_before_down).unwrap_or(1),
                    ts,
                );
                leaf_updates.push(storage::AgentCheckStateRow {
                    check_id: r.check_id,
                    agent_id: r.agent_id.clone(),
                    zone: r.zone.clone(),
                    region: r.region.clone(),
                    status: confirmed,
                    consecutive_failures: tracker.failures(r.check_id, &r.agent_id),
                    last_check: ts,
                });
                let replace = affected
                    .get(&r.check_id)
                    .map(|(_, prior)| ts >= *prior)
                    .unwrap_or(true);
                if replace {
                    affected.insert(r.check_id, (r.clone(), ts));
                }
                r.body = None;
                r.headers = None;
                rows.push((r, status));
            }
        }

        let mut transitions = Vec::new();
        let mut intermediate_transitions = Vec::new();
        let mut region_set_changes = Vec::new();
        let mut root_regions = std::collections::HashMap::new();
        let mut published_states = std::collections::HashMap::new();
        let mut baseline_failures = Vec::new();
        let mut state_updates = Vec::with_capacity(affected.len());
        for (check_id, (_, ts)) in &affected {
            let check_id = *check_id;
            let meta = self.inner.checks.read().get(&check_id).cloned();
            let leaves = staged.children(check_id);
            let candidate =
                self.rollup_candidate(&check_id.to_string(), meta.as_ref(), leaves, *ts);
            let (transition, state, pending, was_unpublished) = {
                let tracker = &mut staged;
                let was_unpublished = tracker.state(check_id).is_none();
                let transition = tracker.publish(
                    check_id,
                    candidate.root,
                    *ts,
                    meta.as_ref().map(|m| m.min_incident_secs).unwrap_or(0),
                    meta.as_ref().map(|m| m.min_recovery_secs).unwrap_or(0),
                );
                (
                    transition,
                    tracker.state(check_id).unwrap_or(candidate.root),
                    tracker.pending(check_id),
                    was_unpublished,
                )
            };
            // Intermediate transitions are downstream evidence just like the root
            // alert, so do not publish them while the root is held down. Otherwise
            // a swallowed blip still leaks zone/region alerts to the UI.
            if was_unpublished || transition.is_some() {
                let previous_regions = {
                    let cache = &mut intermediate_cache;
                    let mut previous = std::collections::BTreeSet::new();
                    let mut had_baseline = false;
                    for intermediate in &candidate.intermediates {
                        let key = (check_id, intermediate.node_id.clone());
                        if let Some(prior) = cache.insert(key, intermediate.state) {
                            had_baseline = true;
                            if intermediate.scope == storage::AlertScope::Region
                                && prior.is_failing()
                            {
                                previous.insert(
                                    intermediate
                                        .node_id
                                        .rsplit("region:")
                                        .next()
                                        .unwrap_or_default()
                                        .to_string(),
                                );
                            }
                            if prior != intermediate.state {
                                intermediate_transitions.push((
                                    intermediate.scope,
                                    intermediate.node_id.clone(),
                                    prior,
                                    intermediate.state,
                                    *ts,
                                ));
                            }
                        }
                    }
                    had_baseline.then_some(previous)
                };
                if let Some(previous) = previous_regions {
                    if previous != candidate.failing_regions {
                        region_set_changes.push((
                            check_id,
                            previous,
                            candidate.failing_regions.clone(),
                            *ts,
                        ));
                    }
                }
            }
            root_regions.insert(check_id, candidate.failing_regions);
            if let Some(t) = transition {
                transitions.push((t, *ts));
            } else if was_unpublished && state.is_failing() {
                baseline_failures.push((check_id, state, *ts));
            }
            // Persist every candidate, including pending hold-down state.
            published_states.insert(check_id, state);
            state_updates.push(storage::PublishedStateRow {
                check_id,
                state,
                pending,
                last_check: *ts,
            });
        }

        // Resolve targets before persistence, then put one row per intended
        // backend in the same transaction as the alert. The outbox is durable
        // before any wake-up or network I/O can occur.
        let all_notifiers = self.inner.notifiers.read().clone();
        let notification_outbox: Vec<_> = transitions
            .iter()
            .filter(|(t, _)| alert::should_alert(t))
            .filter_map(|(t, ts)| {
                let routed_to = self.notify_targets(t.check_id, t.to, *ts)?;
                let targets = crate::notify::targets(&all_notifiers, &routed_to);
                if targets.is_empty() {
                    warn!(check = %t.check_id, routed_to = ?routed_to, "alert matched no notifier");
                }
                Some(
                    targets
                        .into_iter()
                        .map(|notifier| storage::QueuedRow {
                            id: uuid::Uuid::now_v7().to_string(),
                            notifier: notifier.name.clone(),
                            check_id: t.check_id,
                            previous_state: Some(t.from),
                            current_state: t.to,
                            attempts: 0,
                            next_attempt: Some(*ts),
                            last_error: String::new(),
                            queued_at: *ts,
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect();
        let rows_for_replication = rows.clone();
        let leaf_updates_for_replication = leaf_updates.clone();
        let transitions_for_publish = transitions.clone();
        let coord = self.clone();
        let persisted = self
            .db(move |store| {
                let mut incident_events = Vec::new();
                let mut incident_for =
                    std::collections::HashMap::<crate::models::CheckId, String>::new();
                let mut replicated_alerts = Vec::<ReplicatedAlert>::new();
                let mut replicated_incidents = Vec::<storage::ReplicatedIncident>::new();
                store.prepare_ingest()?;
                store.transaction(|store| {
                    store.persist_ingest_uncommitted(&rows, &leaf_updates, &state_updates)?;

                    for (check_id, state, ts) in &baseline_failures {
                        let Some((result, _)) = affected.get(check_id) else {
                            continue;
                        };
                        let regions = root_regions.get(check_id).cloned().unwrap_or_default();
                        match coord.attach_or_open(store, result, *state, &regions, *ts) {
                            Ok((id, opened, changed)) => {
                                if opened || changed {
                                    if let Some(i) = store.incident(&id)? {
                                        let incident = IncidentDto::from_row(i, *ts, false);
                                        incident_events.push(if opened {
                                            WsEvent::IncidentOpened { incident }
                                        } else {
                                            WsEvent::IncidentUpdated { incident }
                                        });
                                    }
                                }
                                incident_for.insert(*check_id, id);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    for (t, ts) in &transitions {
                        let Some((result, _)) = affected.get(&t.check_id) else {
                            continue;
                        };
                        if t.to.is_failing() {
                            let regions =
                                root_regions.get(&t.check_id).cloned().unwrap_or_default();
                            match coord.attach_or_open(store, result, t.to, &regions, *ts) {
                                Ok((id, opened, changed)) => {
                                    if opened || changed {
                                        if let Some(i) = store.incident(&id)? {
                                            let incident = IncidentDto::from_row(i, *ts, false);
                                            incident_events.push(if opened {
                                                WsEvent::IncidentOpened { incident }
                                            } else {
                                                WsEvent::IncidentUpdated { incident }
                                            });
                                        }
                                    }
                                    incident_for.insert(t.check_id, id);
                                }
                                Err(e) => return Err(e),
                            }
                        } else if t.to == NodeState::Up {
                            match store.open_incident_for(t.check_id) {
                                Ok(Some(inc)) => {
                                    incident_for.insert(t.check_id, inc.id.clone());
                                    match coord.try_resolve(store, &inc, *ts) {
                                        Ok(true) => {
                                            if let Some(i) = store.incident(&inc.id)? {
                                                let ack = store.is_acknowledged(&inc.id)?;
                                                incident_events.push(WsEvent::IncidentUpdated {
                                                    incident: IncidentDto::from_row(i, *ts, ack),
                                                });
                                            }
                                        }
                                        Ok(false) => {}
                                        Err(e) => return Err(e),
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => return Err(e),
                            }
                        }
                    }

                    let reconcile_ts = affected
                        .values()
                        .map(|(_, ts)| *ts)
                        .max()
                        .unwrap_or_default();
                    match coord.reconcile_open_incidents(store, reconcile_ts) {
                        Ok(rows) => {
                            for row in rows {
                                let ack = store.is_acknowledged(&row.id)?;
                                if let Some(origin) = row.origin_check {
                                    incident_for.insert(origin, row.id.clone());
                                }
                                incident_events.push(WsEvent::IncidentUpdated {
                                    incident: IncidentDto::from_row(row, reconcile_ts, ack),
                                });
                            }
                        }
                        Err(e) => return Err(e),
                    }

                    for (check_id, previous, current, ts) in &region_set_changes {
                        if published_states.get(check_id) != Some(&NodeState::Degraded) {
                            continue;
                        }
                        match store.open_incident_for(*check_id) {
                            Ok(Some(inc)) => {
                                store.add_update(
                                    &inc.id,
                                    inc.state,
                                    &region_change_narrative(previous, current),
                                    None,
                                    *ts,
                                )?;
                                let title = incident_title(
                                    &coord.check_name(*check_id),
                                    NodeState::Degraded,
                                    current,
                                );
                                if inc.title != title {
                                    store.edit_incident(
                                        &inc.id,
                                        &title,
                                        inc.started_at,
                                        inc.global,
                                    )?;
                                }
                                incident_for.insert(*check_id, inc.id.clone());
                                if let Some(row) = store.incident(&inc.id)? {
                                    let ack = store.is_acknowledged(&inc.id)?;
                                    incident_events.push(WsEvent::IncidentUpdated {
                                        incident: IncidentDto::from_row(row, *ts, ack),
                                    });
                                }
                            }
                            Ok(None) => {}
                            Err(e) => return Err(e),
                        }
                    }

                    for (scope, node_id, previous, current, ts) in &intermediate_transitions {
                        match store.insert_alert(
                            *scope,
                            node_id,
                            Some(*previous),
                            *current,
                            *ts,
                            None,
                        ) {
                            Ok(id) => replicated_alerts.push(ReplicatedAlert {
                                id,
                                scope: scope.as_str().to_string(),
                                node_id: node_id.clone(),
                                previous_state: Some(*previous),
                                current_state: *current,
                                timestamp: *ts,
                                incident_id: None,
                            }),
                            Err(e) => return Err(e),
                        }
                    }

                    for (t, ts) in transitions.iter().filter(|(t, _)| alert::should_alert(t)) {
                        let incident_id = incident_for.get(&t.check_id).cloned();
                        match store.insert_alert(
                            storage::AlertScope::Check,
                            &t.check_id.to_string(),
                            Some(t.from),
                            t.to,
                            *ts,
                            incident_id.as_deref(),
                        ) {
                            Ok(id) => replicated_alerts.push(ReplicatedAlert {
                                id,
                                scope: storage::AlertScope::Check.as_str().to_string(),
                                node_id: t.check_id.to_string(),
                                previous_state: Some(t.from),
                                current_state: t.to,
                                timestamp: *ts,
                                incident_id,
                            }),
                            Err(e) => return Err(e),
                        }
                    }
                    for delivery in &notification_outbox {
                        store.enqueue_notification(delivery)?;
                    }
                    let mut seen_incidents = std::collections::HashSet::new();
                    for id in incident_for.values() {
                        if seen_incidents.insert(id.as_str()) {
                            if let Some(row) = store.replicated_incident(id)? {
                                replicated_incidents.push(row);
                            }
                        }
                    }
                    Ok((
                        incident_events,
                        incident_for,
                        replicated_alerts,
                        replicated_incidents,
                    ))
                })
            })
            .await;
        let (incident_events, incident_for, replicated_alerts, replicated_incidents) =
            match persisted {
                Ok(value) => value,
                Err(error) => {
                    if self.inner.timings.enabled {
                        self.inner.timings.local_state_hold.observe(
                            "state_write",
                            "local_hold",
                            state_hold_started.elapsed(),
                        );
                    }
                    tracing::error!(%error, "failed to persist ingest batch");
                    return;
                }
            };
        self.inner
            .database
            .observe_logical_rows(rows_for_replication.len() as u64);
        self.inner.tracker.lock().install_checks(staged, &check_ids);
        {
            let mut cache = self.inner.intermediate_states.lock();
            cache.retain(|(id, _), _| !check_ids.contains(id));
            cache.extend(intermediate_cache);
        }
        let rows = rows_for_replication;
        let transitions = transitions_for_publish;

        if let Some(peers) = &self.inner.gossip {
            if !transitions.is_empty() {
                let peers = Arc::clone(peers);
                let messages: Vec<_> = transitions
                    .iter()
                    .map(|(t, ts)| {
                        let alert = replicated_alerts.iter().find(|alert| {
                            alert.scope == storage::AlertScope::Check.as_str()
                                && alert.node_id == t.check_id.to_string()
                                && alert.timestamp == *ts
                                && alert.previous_state == Some(t.from)
                                && alert.current_state == t.to
                        });
                        gossip::Message::new(
                            t,
                            *ts,
                            alert.cloned(),
                            incident_for.get(&t.check_id).cloned(),
                        )
                    })
                    .collect();
                tokio::spawn(async move {
                    for message in &messages {
                        peers.broadcast(message).await;
                    }
                });
            }
        }
        if let Some(cluster) = &self.inner.cluster {
            if !rows.is_empty() || !replicated_alerts.is_empty() || !replicated_incidents.is_empty()
            {
                let live = cluster.live_urls();
                if !live.is_empty() {
                    let batch = ReplicationBatch {
                        origin: self.inner.node_id.clone(),
                        results: rows
                            .into_iter()
                            .map(|(result, status)| ReplicatedResult { result, status })
                            .collect(),
                        // The same leaves just written to `agent_check_state`,
                        // so a peer that never saw these probes still folds the
                        // whole zone rather than the slice that reports to it.
                        beliefs: leaf_updates_for_replication
                            .iter()
                            .map(|state| ReplicatedBelief {
                                check_id: state.check_id,
                                agent_id: state.agent_id.clone(),
                                zone: state.zone.clone(),
                                region: state.region.clone(),
                                status: state.status,
                                consecutive_failures: state.consecutive_failures,
                                last_check: state.last_check,
                            })
                            .collect(),
                        alerts: replicated_alerts,
                        incidents: replicated_incidents,
                        ..Default::default()
                    };
                    self.inner
                        .replication_out
                        .broadcast(cluster, &self.inner.timings, &batch);
                }
            }
        }

        for (t, ts) in transitions
            .into_iter()
            .filter(|(t, _)| alert::should_alert(t))
        {
            let _ = self.inner.events.send(WsEvent::StateChange {
                check_id: t.check_id,
                from: t.from,
                to: t.to,
                timestamp: ts,
            });
            // The outbox row was committed with the alert. A full wake-up
            // channel is harmless: `spawn_retries` will scan it later.
            let _ = self.inner.notify_tx.try_send(());
        }
        for event in incident_events {
            let _ = self.inner.events.send(event);
        }
        if self.inner.timings.enabled {
            self.inner.timings.local_state_hold.observe(
                "state_write",
                "local_hold",
                state_hold_started.elapsed(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CoordinatorConfig;
    use std::collections::BTreeSet;
    use std::time::Instant;
    use storage::{AffectSource, IncidentCheckRow, IncidentState};
    use uuid::Uuid;

    fn regions(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn percentile(samples: &mut [u128], percentile: usize) -> u128 {
        samples.sort_unstable();
        samples[(samples.len() * percentile).div_ceil(100).saturating_sub(1)]
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn assert_ceiling(name: &str, actual: u128) {
        if let Ok(value) = std::env::var(name) {
            let ceiling = value
                .parse::<u128>()
                .expect("benchmark ceiling must be an integer");
            assert!(actual <= ceiling, "{name}: {actual} exceeded {ceiling}");
        }
    }

    #[tokio::test]
    async fn failed_ingest_does_not_publish_uncommitted_state() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let check = coord
            .create_check_for_test(CheckConfig {
                name: "transaction-test".into(),
                target: "https://example.invalid".into(),
                ..Default::default()
            })
            .await;
        let result = |code, millis| CheckResult {
            check_id: check,
            agent_id: "agent".into(),
            zone: "zone".into(),
            region: "region".into(),
            kind: CheckKind::Http,
            timestamp: chrono::DateTime::from_timestamp_millis(millis).unwrap(),
            latency_ms: 1,
            status_code: Some(code),
            body_matched: None,
            error: None,
            failure: None,
            body: None,
            headers: None,
            metric: None,
            metrics: None,
        };
        coord.ingest(vec![result(200, 1_000)]).await;
        let intermediate_before = coord.inner.intermediate_states.lock().clone();
        coord
            .db(|store| {
                store.insert_alert(
                    super::storage::AlertScope::Check,
                    "other",
                    None,
                    NodeState::Down,
                    1,
                    None,
                )?;
                store
                    .execute_test_sql("CREATE UNIQUE INDEX fail_down ON alerts (current_state)")?;
                Ok(())
            })
            .await
            .unwrap();
        coord.ingest(vec![result(500, 2_000)]).await;
        assert_eq!(coord.inner.tracker.lock().state(check), Some(NodeState::Up));
        assert_eq!(coord.inner.tracker.lock().failures(check, "agent"), 0);
        assert_eq!(*coord.inner.intermediate_states.lock(), intermediate_before);
        assert_eq!(coord.db(|store| store.count_results()).await.unwrap(), 1);
        coord
            .db(|store| {
                store.execute_test_sql("DROP INDEX fail_down")?;
                Ok(())
            })
            .await
            .unwrap();
        coord.ingest(vec![result(500, 2_000)]).await;
        assert_eq!(
            coord.inner.tracker.lock().state(check),
            Some(NodeState::Down)
        );
        assert_eq!(coord.db(|store| store.count_results()).await.unwrap(), 2);
        assert_eq!(
            coord
                .alerts_for_test(10)
                .await
                .iter()
                .filter(|a| a.node_id == check.to_string())
                .count(),
            1
        );
    }

    #[tokio::test]
    #[ignore = "sustained-ingest benchmark; configure WARDEN_BENCH_* for the intended scale"]
    async fn benchmark_sustained_coordinator_ingest() {
        let check_count = env_usize("WARDEN_BENCH_CHECKS", 10_000);
        let domain_count = env_usize("WARDEN_BENCH_DOMAINS", 1);
        let batch_count = env_usize("WARDEN_BENCH_BATCHES", 20).max(1);
        assert!(check_count > 0 && domain_count > 0);

        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            region: "benchmark-region".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let checks: Vec<_> = (0..check_count)
            .map(|index| CheckConfig {
                id: Uuid::new_v5(&Uuid::NAMESPACE_OID, index.to_string().as_bytes()),
                name: format!("check-{index}"),
                target: "https://example.invalid".into(),
                notify: false,
                position: index as i32,
                ..Default::default()
            })
            .collect();
        let stored = checks.clone();
        coord
            .db(move |storage| {
                for check in &stored {
                    storage.upsert_check(check, 1)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        coord.reload_checks().await.unwrap();

        let rows_per_batch = check_count * domain_count;
        let mut batch_ms = Vec::with_capacity(batch_count);
        let benchmark_started = Instant::now();
        for batch in 0..batch_count {
            let timestamp = chrono::Utc::now() + chrono::Duration::milliseconds(batch as i64);
            let mut results = Vec::with_capacity(rows_per_batch);
            for domain in 0..domain_count {
                for check in &checks {
                    results.push(CheckResult {
                        check_id: check.id,
                        agent_id: format!("agent-{domain}-{}", check.position % 3),
                        zone: format!("zone-{domain}"),
                        region: format!("region-{domain}"),
                        kind: CheckKind::Http,
                        timestamp,
                        latency_ms: 10 + (check.position as u64 % 200),
                        status_code: Some(200),
                        body_matched: None,
                        error: None,
                        failure: None,
                        body: None,
                        headers: None,
                        metric: None,
                        metrics: None,
                    });
                }
            }
            let started = Instant::now();
            coord.ingest(results).await;
            batch_ms.push(started.elapsed().as_millis());
        }
        let elapsed = benchmark_started.elapsed();
        let total_rows = rows_per_batch * batch_count;
        let rows_per_second = total_rows as f64 / elapsed.as_secs_f64();
        let stored_rows = coord.db(|storage| storage.count_results()).await.unwrap() as usize;
        assert_eq!(stored_rows, total_rows);

        let batch_p50 = percentile(&mut batch_ms, 50);
        let batch_p95 = percentile(&mut batch_ms, 95);
        let batch_p99 = percentile(&mut batch_ms, 99);
        let batch_max = *batch_ms.last().unwrap();
        assert_ceiling("WARDEN_BENCH_MAX_INGEST_BATCH_P95_MS", batch_p95);
        if let Ok(value) = std::env::var("WARDEN_BENCH_MIN_INGEST_ROWS_PER_SECOND") {
            let minimum = value
                .parse::<f64>()
                .expect("benchmark floor must be a number");
            assert!(
                rows_per_second >= minimum,
                "WARDEN_BENCH_MIN_INGEST_ROWS_PER_SECOND: {rows_per_second:.2} below {minimum:.2}"
            );
        }
        println!(
            "checks={check_count} domains={domain_count} batches={batch_count} rows_per_batch={rows_per_batch} total_rows={total_rows} ingest_rows_per_second={rows_per_second:.2} batch_p50_ms={batch_p50} batch_p95_ms={batch_p95} batch_p99_ms={batch_p99} batch_max_ms={batch_max}"
        );
    }

    #[test]
    fn degraded_title_names_sorted_failing_regions() {
        assert_eq!(
            incident_title(
                "api",
                NodeState::Degraded,
                &regions(&["us-west", "eu-central"]),
            ),
            "api partial outage in eu-central, us-west"
        );
    }

    #[test]
    fn down_title_remains_plain_and_sensible() {
        assert_eq!(
            incident_title("api", NodeState::Down, &regions(&["eu-central"])),
            "api is down"
        );
    }

    #[test]
    fn region_change_narrative_records_old_and_new_sets() {
        assert_eq!(
            region_change_narrative(&regions(&["eu"]), &regions(&["us", "apac"])),
            "Affected failing regions changed from eu to apac, us."
        );
    }

    #[tokio::test]
    async fn reconciliation_closes_a_stale_open_incident_for_an_up_check() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let check_id = Uuid::new_v4();
        let test_coord = coord.clone();
        let resolved = coord
            .db(move |store| {
                store.update_current_state(check_id, NodeState::Up, None, 2_000)?;
                store.create_incident(
                    "stale",
                    "upstream is down",
                    IncidentState::Detected,
                    1_000,
                    false,
                    Some(check_id),
                    &[IncidentCheckRow {
                        check_id,
                        source: AffectSource::Origin,
                    }],
                    Some(("timeout", None)),
                )?;
                test_coord.reconcile_open_incidents(store, 3_000)
            })
            .await
            .unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].state, IncidentState::Resolved);
        assert_eq!(resolved[0].resolved_at, Some(3_000));
        let test_coord = coord.clone();
        assert!(coord
            .db(move |store| test_coord.reconcile_open_incidents(store, 4_000))
            .await
            .unwrap()
            .is_empty());
    }

    /// A title stored with the origin check's raw id — what a region bakes when
    /// it opens an incident before the check's name has reached its catalogue —
    /// is resolved to the current name at read time, and follows a rename. A
    /// title that already names its check, and a hand-opened incident's human
    /// title, are left untouched. An id with no known check resolves to the
    /// shared label, never a UUID.
    #[tokio::test]
    async fn incident_titles_resolve_to_the_current_name_at_read_time() {
        let coord = Coordinator::new(&CoordinatorConfig {
            db_path: ":memory:".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let id = coord
            .create_check_for_test(CheckConfig {
                name: "dns".into(),
                target: "https://example.invalid".into(),
                ..Default::default()
            })
            .await;

        let dto = |origin: Option<String>, title: &str| IncidentDto {
            id: "i-1".into(),
            title: title.into(),
            state: "detected",
            started_at: 0,
            resolved_at: None,
            duration_ms: 0,
            global: false,
            origin_check: origin,
            checks: Vec::new(),
            acknowledged: false,
            updates: None,
        };
        let title_of = |coord: &Coordinator, d: IncidentDto| {
            let mut one = [d];
            coord.resolve_incident_titles(&mut one);
            one[0].title.clone()
        };

        // The bug: a raw id where the name belongs.
        assert_eq!(
            title_of(&coord, dto(Some(id.to_string()), &format!("{id} is down"))),
            "dns is down"
        );
        // A correct title is untouched.
        assert_eq!(
            title_of(&coord, dto(Some(id.to_string()), "dns is down")),
            "dns is down"
        );
        // The name follows a rename, because it is resolved at read time.
        let mut renamed = coord
            .db(move |store| {
                Ok(store
                    .list_checks()?
                    .into_iter()
                    .find(|c| c.id == id)
                    .unwrap())
            })
            .await
            .unwrap();
        renamed.name = "domain".into();
        coord
            .db(move |store| store.upsert_check(&renamed, 1))
            .await
            .unwrap();
        coord.reload_checks().await.unwrap();
        assert_eq!(
            title_of(&coord, dto(Some(id.to_string()), &format!("{id} is down"))),
            "domain is down"
        );
        // An unknown id never leaks as a UUID.
        let orphan = crate::models::new_check_id();
        assert_eq!(
            title_of(
                &coord,
                dto(Some(orphan.to_string()), &format!("{orphan} is down"))
            ),
            format!("{} is down", crate::models::DELETED_CHECK_LABEL)
        );
        // A hand-opened incident keeps its human title.
        assert_eq!(
            title_of(&coord, dto(None, "Scheduled maintenance")),
            "Scheduled maintenance"
        );
    }
}
