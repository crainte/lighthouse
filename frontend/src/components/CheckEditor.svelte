<script lang="ts">
  import type { Agent, HistoryPoint } from '../lib/api';
  import { fetchAgents, fetchHistory } from '../lib/api';
  import { failureReason, formatLatency, formatRelativeTime, statusColor } from '../lib/format';
  import type { CheckPayload, Section } from '../lib/admin';
  import {
    AdminError,
    createCheck,
    deleteCheck,
    fetchAdminChecks,
    fetchNotifiers,
    fetchSections,
    purgeCheckData,
    purgeCheckEverything,
    updateCheck,
  } from '../lib/admin';
  import NumberField from './NumberField.svelte';
  import type { Draft } from './check-editor/draft';
  import {
    KINDS,
    DEFAULT_STATUSES,
    blank,
    fromCheck,
    whole,
    validPolicy,
    policyValue,
  } from './check-editor/draft';
  import HttpFields from './check-editor/HttpFields.svelte';
  import TcpFields from './check-editor/TcpFields.svelte';
  import IcmpFields from './check-editor/IcmpFields.svelte';
  import DnsFields from './check-editor/DnsFields.svelte';
  import SslFields from './check-editor/SslFields.svelte';
  import OpenMetricsFields from './check-editor/OpenMetricsFields.svelte';
  import HeartbeatFields from './check-editor/HeartbeatFields.svelte';
  import PlacementEditor from './check-editor/PlacementEditor.svelte';
  import RollupPolicyEditor from './check-editor/RollupPolicyEditor.svelte';
  import DependencyEditor from './check-editor/DependencyEditor.svelte';
  import EvaluationTester from './check-editor/EvaluationTester.svelte';

  let {
    id,
    onsaved,
    onclose,
  }: { id: string | null; onsaved: () => void; onclose: () => void } = $props();

  const LOG_LIMIT = 50;

  let draft = $state<Draft>(blank());
  /** Server-owned ordering, round-tripped untouched. Absent on create. */
  let position = $state<number | undefined>(undefined);
  let sections = $state<Section[]>([]);
  /** Every agent the coordinator has ever heard from, with its liveness — the
   *  picker needs both the ids and which of them are still reporting. */
  let agentRows = $state<Agent[]>([]);
  /** Notifier names this check can route to, and which of them are defaults —
   *  the picker has to say what "none selected" actually means. */
  let notifierNames = $state<string[]>([]);
  let defaultNotifiers = $state<string[]>([]);
  /** Every other check's id and name, for the `depends_on` picker. Excludes
   *  this one: a check depending on itself would look up its own open
   *  incident and join it, which is a no-op dressed up as configuration. */
  let allChecks = $state<{ id: string; name: string }[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let saving = $state(false);
  let saveError = $state<string | null>(null);
  let savedNote = $state<string | null>(null);

  let logs = $state<HistoryPoint[]>([]);
  let logsError = $state<string | null>(null);
  let logsLoading = $state(false);

  // Each destructive action confirms on its own, so arming one never arms the
  // other — they destroy very different things.
  let pending = $state<'purge' | 'delete' | 'erase' | null>(null);
  let dangerBusy = $state(false);
  let dangerError = $state<string | null>(null);
  let dangerNote = $state<string | null>(null);

  // This is deliberately initialized, rather than derived: after a new check
  // is saved, closing Probe remains the editor's own persistent choice.
  // svelte-ignore state_referenced_locally
  let probeOpen = $state(id === null);

  const kind = $derived(draft.type);
  const creating = $derived(id === null);

  let savedName = $state<string | null>(null);

  const heartbeatUrl = $derived(
    id === null ? null : `${location.origin}/api/v1/heartbeat/${encodeURIComponent(id)}`,
  );

  // A check may sit in a section the list doesn't know about (renamed
  // elsewhere, stale tab). Offering it keeps the select from silently
  // reassigning it.
  const sectionOptions = $derived.by(() => {
    const names = sections.map((s) => s.name);
    return draft.group && !names.includes(draft.group) ? [...names, draft.group] : names;
  });

  // Same reason as sections: an agent assigned to this check may not have
  // reported yet, and dropping it from the picker would silently unassign it.
  // Set-deduped so the keyed each below can never see a repeated id.
  const agentOptions = $derived([
    ...new Set([...agentRows.map((a) => a.agent_id), ...draft.agents]),
  ]);

  /** Dependency candidates: every other check, plus anything already named on
   *  this one so a stale entry stays visible instead of vanishing silently.
   *  Deduped by id, since the picker below is keyed on it. A dependency whose
   *  check has since been deleted has no name to show, so it falls back to
   *  its raw id rather than disappearing from the list unexplained. */
  const otherChecks = $derived.by(() => {
    const byId = new Map(allChecks.map((c) => [c.id, c.name]));
    const ids = new Set([...allChecks.map((c) => c.id), ...draft.depends_on]);
    ids.delete(id ?? '');
    return [...ids].map((depId) => ({ id: depId, name: byId.get(depId) ?? depId }));
  });

  // An id absent here is either silent or has never reported at all. Both mean
  // the same thing to someone ticking the box: nothing would probe this check.
  const reporting = $derived(
    new Set(agentRows.filter((a) => a.status === 'up').map((a) => a.agent_id)),
  );

  /** Live agents only: the backend computes placement against the agents it has
   *  heard from in the last five minutes, which is exactly `status === 'up'`.
   *  A silent agent's zone is not coverage. */
  const live = $derived(agentRows.filter((a) => a.status === 'up'));

  /** Locations the fleet actually reports, plus anything already on this check.
   *  A zone can legitimately be named before an agent shows up in it, so the
   *  draft's own values are always offered — dropping one would silently
   *  rewrite the placement on the next save. Set-deduped: the pickers below are
   *  keyed by these strings. */
  const zoneOptions = $derived([
    ...new Set([...live.map((a) => a.zone), ...draft.placement.zones]),
  ]);
  const regionOptions = $derived([
    ...new Set([...live.map((a) => a.region), ...draft.placement.regions]),
  ]);

  /** Pinning is an override, not a filter: when it is on, placement is dead
   *  config and saying otherwise would be a lie the editor tells. */
  const pinned = $derived(draft.agents.length > 0);

  /** The number that stops a 20x fan-out. Same rule the coordinator applies:
   *  eligible live agents grouped by the chosen zone or region, with `count`
   *  taken in each domain — never as a fleet-wide total. Which specific agents
   *  win is rendezvous hashing and irrelevant here; how many and where is the
   *  whole question. */
  const effect = $derived.by(() => {
    const p = draft.placement;
    const count = Math.max(1, p.count ?? 1);
    const counts = new Map<string, number>();
    for (const a of live) {
      if (p.zones.length > 0 && !p.zones.includes(a.zone)) continue;
      if (p.regions.length > 0 && !p.regions.includes(a.region)) continue;
      const domain = p.mode === 'region' ? a.region : a.zone;
      counts.set(domain, (counts.get(domain) ?? 0) + 1);
    }
    const domains = [...counts]
      .map(([domain, available]) => ({ domain, probes: Math.min(available, count), available }))
      .sort((x, y) => x.domain.localeCompare(y.domain));
    return { domains, total: domains.reduce((n, d) => n + d.probes, 0), count };
  });

  const cronHint = $derived(describeCron(draft.cron));
  const retryIntervalHint = $derived(describeInterval(draft.retry_interval_secs));
  const downIntervalHint = $derived(describeInterval(draft.down_interval_secs));

  /* ---- Section summaries ----
     A shut panel still has to answer its own question, or collapsing the form
     has only hidden the crowding rather than fixed it. Each note is the one
     line that would otherwise make you open the section to check. */

  const identityNote = $derived(
    [draft.name.trim() || 'unnamed', draft.tag.trim()].filter(Boolean).join(' · '),
  );

  /** Scheme and trailing slash carry no information in a one-line summary. */
  const shortTarget = $derived(draft.target.trim().replace(/^[a-z]+:\/\//, '').replace(/\/$/, ''));

  /** Heartbeat ignores both, so it never appears there — only on the kinds
   *  the retry/down cadence actually governs. */
  const cadenceNote = $derived(
    kind === 'heartbeat'
      ? ''
      : [
          draft.retry_interval_secs !== null ? `retry ${draft.retry_interval_secs}s` : '',
          draft.down_interval_secs !== null ? `down ${draft.down_interval_secs}s` : '',
        ]
          .filter(Boolean)
          .join(', '),
  );

  const probeNote = $derived(
    [
      kind,
      shortTarget || (kind === 'heartbeat' ? 'inbound ping' : 'no target'),
      cronHint,
      cadenceNote,
    ]
      .filter(Boolean)
      .join(' · '),
  );

  const placementNote = $derived.by(() => {
    if (pinned)
      return `pinned to ${draft.agents.length} ${draft.agents.length === 1 ? 'agent' : 'agents'}`;
    if (effect.total === 0) return 'nothing probes this';
    const domains = effect.domains.length;
    return `${effect.total} ${effect.total === 1 ? 'probe' : 'probes'} across ${domains} ${draft.placement.mode}${domains === 1 ? '' : 's'}`;
  });

  /** Counts only what departs from the coordinator's own defaults, so the badge
   *  reads as "things you changed", not "fields that exist". */
  const verdictOverrides = $derived(
    (draft.rollup.zone ? 1 : 0) +
      (draft.rollup.region ? 1 : 0) +
      (draft.rollup.global ? 1 : 0) +
      (draft.min_incident_secs !== null ? 1 : 0) +
      (draft.min_recovery_secs !== null ? 1 : 0),
  );

  const verdictNote = $derived(
    [
      `DOWN after ${draft.failures_before_down ?? '?'}`,
      draft.depends_on.length > 0 ? `${draft.depends_on.length} upstream` : '',
      verdictOverrides > 0
        ? `${verdictOverrides} ${verdictOverrides === 1 ? 'override' : 'overrides'}`
        : '',
    ]
      .filter(Boolean)
      .join(' · '),
  );

  const alertNote = $derived.by(() => {
    if (!draft.notify) return 'silent';
    if (draft.notifiers.length > 0) return `→ ${draft.notifiers.join(', ')}`;
    return defaultNotifiers.length > 0 ? `→ defaults (${defaultNotifiers.join(', ')})` : '→ nobody';
  });

  const statusPageNote = $derived(
    [
      draft.visible ? 'visible' : 'hidden',
      draft.group ? `in ${draft.group}` : '',
      draft.latency_graph ? 'latency graph' : '',
    ]
      .filter(Boolean)
      .join(' · '),
  );

  /* Advanced disclosure. Set from the loaded check in load(), then owned by
     whoever is typing. */
  let verdictAdvanced = $state(false);

  /** Plain-language gloss for the shapes people actually type. Anything else
   *  says it cannot read the expression rather than guessing — a wrong reading
   *  here is worse than none, because it is the only place the schedule is
   *  stated in words. */
  function describeCron(expr: string): string {
    const fields = expr.trim().split(/\s+/).filter(Boolean);
    // A five-field expression is minutes-first; the server pads it with "0".
    const six = fields.length === 5 ? ['0', ...fields] : fields;
    if (six.length !== 6) return 'Six fields, seconds first. Five are read as “at second zero”.';
    const [sec, min, hr, ...date] = six;
    if (date.some((f) => f !== '*' && f !== '?')) return 'Runs on a specific day or month.';
    const step = (v: string) => (/^\*\/[1-9]\d*$/.test(v) ? Number(v.slice(2)) : null);
    const at = (v: string) => (/^\d+$/.test(v) ? v : null);
    const every = (n: number, unit: string) => `every ${n === 1 ? unit : `${n} ${unit}s`}`;
    if (min === '*' && hr === '*') {
      const s = step(sec);
      if (s) return every(s, 'second');
      if (at(sec)) return `every minute, at second ${Number(sec)}`;
    }
    if (hr === '*' && at(sec)) {
      const m = step(min);
      if (m) return every(m, 'minute');
      if (at(min)) return `every hour, at :${min.padStart(2, '0')}`;
    }
    if (at(sec) && at(min)) {
      const h = step(hr);
      if (h) return every(h, 'hour');
      if (at(hr)) return `every day, at ${hr.padStart(2, '0')}:${min.padStart(2, '0')}`;
    }
    return 'Lighthouse reads this schedule; this hint cannot put it in words.';
  }

  function describeInterval(seconds: number | null): string {
    if (seconds === null) return 'Automatic — derived from the cron schedule.';
    if (!Number.isInteger(seconds) || seconds < 1) return 'Enter a whole number of seconds.';

    const parts: string[] = [];
    const units: [number, string][] = [
      [86_400, 'day'],
      [3_600, 'hour'],
      [60, 'minute'],
      [1, 'second'],
    ];
    let remaining = seconds;
    for (const [size, name] of units) {
      const count = Math.floor(remaining / size);
      if (count === 0) continue;
      parts.push(`${count} ${name}${count === 1 ? '' : 's'}`);
      remaining %= size;
    }
    return `every ${parts.join(', ')}`;
  }

  function fail(e: unknown): string {
    return e instanceof Error ? e.message : String(e);
  }

  async function load() {
    loading = true;
    loadError = null;
    try {
      // The pickers would silently offer the wrong choices on a partial load,
      // so they come together or the failure is reported.
      const [checks, secs, ag, notifiers] = await Promise.all([
        fetchAdminChecks(),
        fetchSections(),
        fetchAgents(),
        fetchNotifiers(),
      ]);
      sections = secs;
      agentRows = ag.agents;
      allChecks = checks.map((c) => ({ id: c.id, name: c.name }));
      notifierNames = notifiers.notifiers.map((n) => n.name).filter(Boolean);
      defaultNotifiers = notifiers.notifiers
        .filter((n) => n.default)
        .map((n) => n.name)
        .filter(Boolean);
      if (id !== null) {
        const found = checks.find((c) => c.id === id);
        if (!found) throw new Error('This check could not be found. It may have been deleted.');
        draft = fromCheck(found);
        position = found.position;
        savedName = found.name;
        // Read once, here, rather than reactively: a saved check that already
        // carries overrides must arrive with them on screen, but clearing the
        // last one must not then slam the panel shut under the cursor.
        verdictAdvanced = verdictOverrides > 0;
      }
    } catch (e) {
      loadError =
        e instanceof AdminError && e.unauthorized
          ? `${e.message} — your session expired. Sign in again from the header.`
          : fail(e);
    } finally {
      loading = false;
    }
  }

  async function loadLogs() {
    if (id === null) return;
    logsLoading = true;
    logsError = null;
    try {
      const r = await fetchHistory(id, LOG_LIMIT);
      logs = r.history;
    } catch (e) {
      logsError = fail(e);
    } finally {
      logsLoading = false;
    }
  }

  load();
  loadLogs();

  function secretReferenceError(target: string, headers: { name: string; value: string }[]): string | null {
    const hasReference = (value: string) => value.includes('${');
    const authority = target.match(/^[a-z]+:\/\/([^/?#]*)/i)?.[1] ?? '';
    if (authority.includes('@')) return 'URL userinfo is not allowed; use an authentication header with a ${NAME} reference.';
    const query = target.split('?')[1]?.split('#')[0] ?? '';
    for (const part of query.split('&')) {
      const [key, value = ''] = part.split('=', 2);
      if (/^(access_token|api_key|apikey|auth|key|password|passwd|secret|token)$/i.test(key) && !hasReference(value))
        return 'Sensitive query values must use a ${NAME} reference.';
    }
    for (const header of headers) {
      if (/^(authorization|proxy-authorization|x-api-key|x-auth-token|x-access-token)$/i.test(header.name.trim()) && !hasReference(header.value))
        return 'Authentication header values must use a ${NAME} reference.';
    }
    return null;
  }

  async function save() {
    if (saving) return;
    savedNote = null;
    const n = draft.name.trim();
    if (!n) return (saveError = 'Name is required.');
    const k = draft.type;
    if (k !== 'heartbeat' && !draft.target.trim()) return (saveError = 'Target is required.');
    const secretError = secretReferenceError(draft.target.trim(), k === 'http' ? draft.headers : []);
    if (secretError) return (saveError = secretError);
    if (k !== 'heartbeat' && !whole(draft.timeout_secs, 1))
      return (saveError = 'Timeout must be a whole number of seconds, at least 1.');
    if (k === 'icmp' && !whole(draft.ping.count, 1))
      return (saveError = 'Ping count must be a whole number, at least 1.');
    if (k === 'ssl' && !whole(draft.cert_warning_days, 0))
      return (saveError = 'Certificate warning must be a whole number of days, at least 0.');
    if (k === 'openmetrics' && !draft.metric.name.trim())
      return (saveError = 'Metric name is required.');
      if (k === 'heartbeat' && !whole(draft.grace_secs, 10))
        return (saveError = 'Grace must be a whole number of seconds, at least 10.');
    if (!whole(draft.failures_before_down, 1))
      return (saveError = 'Confirmation must be a whole number of probes, at least 1.');
    // Heartbeat never reads either: nothing is retried between pings, so the
    // fields are off-screen for it and the draft's stale value (from an
    // earlier kind) is simply never validated or sent.
    if (
      k !== 'heartbeat' &&
      draft.retry_interval_secs !== null &&
      !whole(draft.retry_interval_secs, draft.timeout_secs ?? 1)
    )
      return (saveError = 'Retry interval must be a whole number of seconds, at least the timeout.');
    if (
      k !== 'heartbeat' &&
      draft.down_interval_secs !== null &&
      !whole(draft.down_interval_secs, draft.timeout_secs ?? 1)
    )
      return (saveError = 'Down interval must be a whole number of seconds, at least the timeout.');
    if (draft.min_incident_secs !== null && !whole(draft.min_incident_secs, 0))
      return (saveError = 'Incident hold-down must be a whole number of seconds, at least 0.');
    if (draft.min_recovery_secs !== null && !whole(draft.min_recovery_secs, 0))
      return (saveError = 'Recovery hold-down must be a whole number of seconds, at least 0.');
    if (!validPolicy(draft.rollup.zone) || !validPolicy(draft.rollup.region) || !validPolicy(draft.rollup.global))
      return (saveError = 'Rollup quorum values must be whole numbers, at least 1.');
    if (!whole(draft.placement.count, 1))
      return (saveError = 'Placement count must be a whole number, at least 1.');

    const trimmed = (v: string[]) => v.map((s) => s.trim()).filter(Boolean);
    const multiHost = k === 'tcp' || k === 'icmp';
    const payload: CheckPayload = {
      name: n,
      type: k,
      // A heartbeat is pinged rather than probed, so it is the one kind with
      // nothing to point at.
      target: k === 'heartbeat' ? '' : draft.target.trim(),
      hosts: multiHost ? trimmed(draft.hosts) : [],
      host_match: draft.host_match,
      cron: draft.cron.trim(),
      // Nothing is probed for a heartbeat, so a timeout has no meaning — but the
      // server still requires one that fits inside the schedule gap, and the
      // field is deliberately not on screen for this type. 1s always fits.
      timeout_secs: k === 'heartbeat' ? 1 : (draft.timeout_secs ?? 10),
      latency_graph: draft.latency_graph,
      expect: {},
      enabled: draft.enabled,
      visible: draft.visible,
      notify: draft.notify,
      notifiers: [...draft.notifiers],
      // Ids selected via checkbox, not free text, but trimmed for consistency
      // with every other list on this payload.
      depends_on: trimmed(draft.depends_on),
      agents: [...draft.agents],
      default_status: draft.default_status,
      // Config a type cannot act on is config the server has to reject or
      // silently ignore, so only the fields this type actually uses go on the
      // wire — an expected status on a TCP check is a hard validation error.
      headers: k === 'http' ? draft.headers.filter((h) => h.name.trim()) : [],
      allow_self_signed: k === 'http' || k === 'ssl' ? draft.allow_self_signed : false,
      dns: {
        record: draft.dns.record,
        match_type: draft.dns.match_type,
        expected: k === 'dns' ? trimmed(draft.dns.expected) : [],
      },
      ping: { ip_type: draft.ping.ip_type, count: draft.ping.count ?? 1 },
      metric:
        k === 'openmetrics'
          ? {
              name: draft.metric.name.trim(),
              labels: draft.metric.labels
                .filter((l) => l.name.trim())
                .map((l) => ({ name: l.name.trim(), value: l.value })),
            }
          : { name: '', labels: [] },
      cert_warning_days: draft.cert_warning_days ?? 14,
      grace_secs: draft.grace_secs ?? 300,
      failures_before_down: draft.failures_before_down ?? 2,
      rollup: {},
placement: {
zones: trimmed(draft.placement.zones),
regions: trimmed(draft.placement.regions),
mode: draft.placement.mode,
count: draft.placement.count ?? 1,
// Send the legacy field too: an older coordinator preserves the historic
// per-zone value, while a current one reads mode/count.
per_zone: draft.placement.count ?? 1,
},
    };
    // Ordering is the server's: an edit round-trips the position it handed us,
    // a create has none until the server assigns one.
    if (position !== undefined) payload.position = position;
    for (const level of ['zone', 'region', 'global'] as const) {
      const policy = draft.rollup[level];
      if (policy) payload.rollup[level] = policyValue(policy);
    }
    // Preserve inheritance: blank means omitted, while zero and false are sent.
    // Heartbeat ignores both, so a stale value left over from a previous kind
    // never reaches the wire for one.
    if (k !== 'heartbeat' && draft.retry_interval_secs !== null)
      payload.retry_interval_secs = draft.retry_interval_secs;
    if (k !== 'heartbeat' && draft.down_interval_secs !== null)
      payload.down_interval_secs = draft.down_interval_secs;
    if (draft.min_incident_secs !== null) payload.min_incident_secs = draft.min_incident_secs;
    if (draft.min_recovery_secs !== null) payload.min_recovery_secs = draft.min_recovery_secs;
    if (draft.notify_degraded !== '') payload.notify_degraded = draft.notify_degraded === 'true';
    if (draft.degraded_is_downtime !== '')
      payload.degraded_is_downtime = draft.degraded_is_downtime === 'true';
    // An untouched optional field means "unset", not a tag literally named "".
    if (draft.group.trim()) payload.group = draft.group.trim();
    if (draft.eval.trim()) payload.eval = draft.eval.trim();
    if (draft.description.trim()) payload.description = draft.description.trim();
    if (draft.image.trim()) payload.image = draft.image.trim();
    if (draft.tag.trim()) payload.tag = draft.tag.trim();
    if (k === 'http') {
      if (draft.expect_status !== null) payload.expect.status = draft.expect_status;
      if (draft.expect_body.trim()) payload.expect.body_contains = draft.expect_body.trim();
    }
    if (k === 'dns' && draft.dns.nameserver.trim())
      payload.dns.nameserver = draft.dns.nameserver.trim();

    saving = true;
    saveError = null;
    try {
      if (id === null) {
        await createCheck(payload);
        onsaved();
        // A created check's identity is now fixed; the table is where you pick
        // it up again, and it proves the row landed.
        onclose();
        return;
      }
      const updated = await updateCheck(id, payload);
      // Round-trip what the server stored, so trimmed and defaulted values are
      // what you are looking at rather than what you typed.
      draft = fromCheck(updated);
      position = updated.position;
      savedName = updated.name;
      savedNote = 'Saved.';
      onsaved();
    } catch (e) {
      saveError = fail(e);
    } finally {
      saving = false;
    }
  }

  async function confirmPurge() {
    if (id === null || dangerBusy) return;
    dangerBusy = true;
    dangerError = null;
    dangerNote = null;
    try {
      const rows = await purgeCheckData(id);
      pending = null;
      dangerNote = `Deleted ${rows} recorded ${rows === 1 ? 'attempt' : 'attempts'}. The check is still configured and still probing.`;
      logs = [];
      await loadLogs();
      onsaved();
    } catch (e) {
      dangerError = fail(e);
    } finally {
      dangerBusy = false;
    }
  }

  async function confirmDelete() {
    if (id === null || dangerBusy) return;
    dangerBusy = true;
    dangerError = null;
    try {
      await deleteCheck(id);
      onsaved();
      onclose();
    } catch (e) {
      dangerError = fail(e);
      dangerBusy = false;
    }
  }

  // The only one of the three that also takes the recorded history and the
  // incidents with it — plain delete leaves those orphaned behind the id.
  async function confirmErase() {
    if (id === null || dangerBusy) return;
    dangerBusy = true;
    dangerError = null;
    try {
      const eraseName = savedName ?? (draft.name || id);
      const { results, incidents } = await purgeCheckEverything(id);
      dangerNote = `Erased ${eraseName}: ${results} recorded ${results === 1 ? 'attempt' : 'attempts'} and ${incidents} ${incidents === 1 ? 'incident' : 'incidents'} removed, along with the check itself.`;
      onsaved();
      onclose();
    } catch (e) {
      dangerError = fail(e);
      dangerBusy = false;
    }
  }
</script>

{#if loading}
  <div class="admin-panel"><p class="admin-muted">Loading…</p></div>
{:else if loadError}
  <div class="admin-panel">
    <p class="admin-error">{loadError}</p>
    <div class="form-actions">
      <button class="btn" onclick={onclose}>Back to checks</button>
    </div>
  </div>
{:else}
  <!-- Enabled lives up here rather than inside a section: it governs the whole
       check, and a switch you have to go looking for is one you forget is off.
       With every panel shut, the header still says so. -->
  <div class="area-bar" class:off={!draft.enabled}>
    <div class="title-line">
      <span class="panel-title no-gap mono">{creating ? 'New check' : draft.name || 'unnamed'}</span>
      <span class="kind-pill">{kind}</span>
      {#if !draft.enabled}<span class="off-pill">not probing</span>{/if}
    </div>
    <div class="range-switch head-acts">
      <label class="toggle enable-toggle">
        <input type="checkbox" bind:checked={draft.enabled} />
        <span>Enabled</span>
      </label>
      <button class="range-btn" onclick={onclose}>← All checks</button>
    </div>
  </div>

  <!-- Sections are ordered as the decisions are actually made: what it is, how
       it is probed, where from, what counts as down, who hears about it, and
       how it looks. Only the first two open on arrival, and the summaries carry
       enough that the rest usually stay shut. -->

  <details class="admin-panel editor-section" open>
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Identity
      <span class="admin-muted sum-note mono">{identityNote}</span>
    </summary>
    <div class="editor-body">
      <div class="form-grid">
        <label class="field">
          <span class="field-label">Name</span>
          <input class="mono" bind:value={draft.name} placeholder="api-gateway" />
          <span class="field-note">
            The mutable display label — rename freely. Anything but a slash or a control
            character; this is not the check's identity, so renaming never orphans its history,
            alerts or incidents.
          </span>
        </label>

        <label class="field">
          <span class="field-label">Tag</span>
          <input bind:value={draft.tag} placeholder="(none)" />
          <span class="field-note">Short label for scanning the checks table.</span>
        </label>

        <label class="field">
          <span class="field-label">Description</span>
          <input bind:value={draft.description} placeholder="(none)" />
          <span class="field-note">One line, shown under the name on the status page.</span>
        </label>
      </div>
    </div>
  </details>

  <details class="admin-panel editor-section" bind:open={probeOpen}>
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Probe
      <span class="admin-muted sum-note mono">{probeNote}</span>
    </summary>
    <div class="editor-body">
      <div class="form-grid">
        <label class="field">
          <span class="field-label">Type</span>
          <select bind:value={draft.type}>
            {#each KINDS as k (k)}
              <option value={k}>{k}</option>
            {/each}
          </select>
          <span class="field-note">Decides every field below.</span>
        </label>

        {#if kind === 'http'}
          <HttpFields {draft} />
        {:else if kind === 'tcp'}
          <TcpFields {draft} />
        {:else if kind === 'icmp'}
          <IcmpFields {draft} />
        {:else if kind === 'dns'}
          <DnsFields {draft} />
        {:else if kind === 'ssl'}
          <SslFields {draft} />
        {:else if kind === 'openmetrics'}
          <OpenMetricsFields {draft} />
        {:else}
          <HeartbeatFields {draft} {heartbeatUrl} />
        {/if}

        <!-- The schedule belongs with the probe: how often it runs is part of
             how it is probed, not a separate concern. -->
        <label class="field wide">
          <span class="field-label">Cron</span>
          <input class="mono" bind:value={draft.cron} placeholder="*/30 * * * * *" />
          <span class="field-note"><code>{draft.cron.trim() || '(empty)'}</code> — {cronHint}</span>
        </label>

        {#if kind !== 'heartbeat'}
          <!-- Nothing to retry between pings on a heartbeat, so these are
               absent for that kind rather than shown and ignored. -->
          <label class="field">
            <span class="field-label">Retry interval (s)</span>
            <NumberField min={draft.timeout_secs ?? 1} bind:value={draft.retry_interval_secs} placeholder="auto" />
            <span class="field-note interval-example">{retryIntervalHint}</span>
            <span class="field-note">
              How often it re-probes after a single failure, while confirming DOWN. Blank derives
              a value from the schedule; at least the timeout either way.
            </span>
          </label>

          <label class="field">
            <span class="field-label">Down interval (s)</span>
            <NumberField min={draft.timeout_secs ?? 1} bind:value={draft.down_interval_secs} placeholder="auto" />
            <span class="field-note interval-example">{downIntervalHint}</span>
            <span class="field-note">
              How often it re-probes once DOWN is confirmed. A success returns it to the cron
              schedule immediately. Blank derives a value from the schedule; at least the timeout
              either way.
            </span>
          </label>
        {/if}
      </div>

      <EvaluationTester {kind} bind:evalScript={draft.eval} />
    </div>
  </details>

  <!-- Pinning and placement answer the same question — who probes this — so
       they are one panel. Split across two, as they used to be, each had to
       apologise for the other in prose. -->
  <details class="admin-panel editor-section">
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Placement
      <span class="admin-muted sum-note mono" class:bad={!pinned && effect.total === 0}>
        {placementNote}
      </span>
    </summary>
    <div class="editor-body">
      <PlacementEditor {draft} {pinned} {effect} {zoneOptions} {regionOptions} {agentOptions} {reporting} />
    </div>
  </details>

  <details class="admin-panel editor-section">
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Verdict
      <span class="admin-muted sum-note mono">{verdictNote}</span>
    </summary>
    <div class="editor-body">
      <p class="field-note lede">
        How probe results become a status. Everything here is about reading the evidence, not
        about telling anyone — that is the Alerting section.
      </p>

      <div class="form-grid">
        <label class="field">
          <span class="field-label">Confirm DOWN after</span>
          <NumberField min={1} bind:value={draft.failures_before_down} />
          <span class="field-note">
            Consecutive failed probes before the check reads DOWN. 1 flips immediately; 3 tolerates
            a flaky hop. The time-based hold-down under Advanced applies after rollup.
          </span>
        </label>

        <label class="field">
          <span class="field-label">Default status</span>
          <select bind:value={draft.default_status}>
            {#each DEFAULT_STATUSES as s (s)}
              <option value={s}>{s}</option>
            {/each}
          </select>
          <span class="field-note">How it reads before its first result lands.</span>
        </label>

        <label class="field">
          <span class="field-label">Count DEGRADED as downtime</span>
          <select bind:value={draft.degraded_is_downtime}>
            <option value="">inherit (default: off)</option>
            <option value="true">on</option>
            <option value="false">off</option>
          </select>
          <span class="field-note">
            Whether a degraded period counts against uptime. Inherit follows the coordinator
            default; choosing off is an explicit override.
          </span>
        </label>

        <DependencyEditor {draft} {otherChecks} />
      </div>

      <details class="advanced" bind:open={verdictAdvanced}>
        <summary class="advanced-summary">
          <span class="chev" aria-hidden="true">▸</span> Advanced — hold-downs and rollup
          {#if verdictOverrides > 0}
            <span class="badge-count">
              {verdictOverrides}
              {verdictOverrides === 1 ? 'override' : 'overrides'}
            </span>
          {/if}
        </summary>
        <div class="advanced-body">
          <div class="form-grid">
            <label class="field">
              <span class="field-label">Incident hold-down (s)</span>
              <NumberField min={0} bind:value={draft.min_incident_secs} placeholder="inherit" />
              <span class="field-note">Blank inherits the coordinator default. Explicit 0 publishes failures immediately.</span>
            </label>

            <label class="field">
              <span class="field-label">Recovery hold-down (s)</span>
              <NumberField min={0} bind:value={draft.min_recovery_secs} placeholder="inherit" />
              <span class="field-note">Blank inherits. Explicit 0 publishes recovery immediately.</span>
            </label>

            <RollupPolicyEditor {draft} />
          </div>
        </div>
      </details>
    </div>
  </details>

  <details class="admin-panel editor-section">
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Alerting
      <span class="admin-muted sum-note mono">{alertNote}</span>
    </summary>
    <div class="editor-body">
      <div class="form-grid">
        <div class="field wide switches">
          <span class="field-label">Notify</span>
          <label class="toggle">
            <input type="checkbox" bind:checked={draft.notify} />
            <span
              >Send state changes to the notifiers <em
                >— off still records the incident; it just doesn't page anyone.</em
              ></span
            >
          </label>
        </div>

        {#if draft.notify}
          <div class="field wide">
            <span class="field-label">Send alerts to</span>
            {#if notifierNames.length === 0}
              <span class="field-note">
                No notifiers configured. Add one on the Notifications panel; until then every
                alert is recorded and sent nowhere.
              </span>
            {:else}
              <div class="agent-picker">
                {#each notifierNames as n (n)}
                  <label class="toggle">
                    <input type="checkbox" value={n} bind:group={draft.notifiers} />
                    <span class="agent-pill">{n}</span>
                  </label>
                {/each}
              </div>
              <span class="field-note">
                {#if draft.notifiers.length === 0}
                  None picked, so this check goes to the defaults:
                  <strong>{defaultNotifiers.join(', ') || 'nothing is marked default, so nobody'}</strong>.
                {:else}
                  Exactly these, whether or not they are defaults.
                {/if}
              </span>
            {/if}
          </div>

          <label class="field">
            <span class="field-label">Notify on DEGRADED</span>
            <select bind:value={draft.notify_degraded}>
              <option value="">inherit (default: on)</option>
              <option value="true">on</option>
              <option value="false">off</option>
            </select>
            <span class="field-note">
              Whether a partial outage pages as well as a full one. Off means only DOWN and
              recovery are sent.
            </span>
          </label>
        {/if}
      </div>
    </div>
  </details>

  <details class="admin-panel editor-section">
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Status page
      <span class="admin-muted sum-note mono">{statusPageNote}</span>
    </summary>
    <div class="editor-body">
      <div class="form-grid">
        <label class="field">
          <span class="field-label">Section</span>
          <select bind:value={draft.group}>
            <option value="">(none — listed loose)</option>
            {#each sectionOptions as s (s)}
              <option value={s}>{s}</option>
            {/each}
          </select>
          <span class="field-note">Sections are created on the Status page panel.</span>
        </label>

        <label class="field">
          <span class="field-label">Image</span>
          <input bind:value={draft.image} placeholder="🛰️ or https://…/icon.png" />
          <span class="field-note">An emoji or an image URL.</span>
        </label>

        <div class="field wide switches">
          <span class="field-label">Display</span>
          <label class="toggle">
            <input type="checkbox" checked={!draft.visible} onchange={() => (draft.visible = !draft.visible)} />
            <span>Hidden <em>— keep it off the public status page. It is still probed.</em></span>
          </label>
          <label class="toggle">
            <input type="checkbox" bind:checked={draft.latency_graph} />
            <span>Latency graph <em>— draw the sparkline under it.</em></span>
          </label>
        </div>
      </div>
    </div>
  </details>

  <!-- Neither section exists before the check does: no history to show, nothing
       to purge or delete. Absent rather than empty or disabled. -->
  {#if !creating}
    <details class="admin-panel editor-section">
      <summary class="editor-summary">
        <span class="chev" aria-hidden="true">▸</span> Recent logs
        <span class="admin-muted sum-note">newest {LOG_LIMIT}</span>
      </summary>
      <div class="editor-body">
        {#if logsLoading}
          <p class="admin-muted">Loading…</p>
        {:else if logsError}
          <p class="admin-error">{logsError}</p>
          <div class="form-actions">
            <button class="btn" onclick={loadLogs}>Retry</button>
          </div>
        {:else if logs.length === 0}
          <p class="admin-muted">No attempts recorded for this check.</p>
        {:else}
          <div class="log-table-wrap">
            <table class="log-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Agent</th>
                  <th>Latency</th>
                  <th>Status</th>
                  <th>Reason</th>
                </tr>
              </thead>
              <tbody>
                {#each logs as h, i (i)}
                  <tr>
                    <td class="dim">{formatRelativeTime(h.timestamp)}</td>
                    <td><span class="agent-pill">{h.agent_id}</span></td>
                    <td class="mono">{formatLatency(h.latency_ms)}</td>
                    <td><span class="badge" style="background:{statusColor(h.status)}">{h.status}</span></td>
                    <td class="log-reason" title={failureReason(h) ?? ''}>{failureReason(h) ?? ''}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}
      </div>
    </details>

    <details class="admin-panel editor-section danger">
      <summary class="editor-summary">
        <span class="chev" aria-hidden="true">▸</span> Danger zone
      </summary>
      <div class="editor-body">
        {#if dangerError}<p class="admin-error">{dangerError}</p>{/if}
        {#if dangerNote}<p class="admin-muted">{dangerNote}</p>{/if}

        <div class="danger-act">
          <div class="danger-title">Purge data</div>
          <p class="admin-muted">
            Deletes every recorded attempt for <span class="mono">{draft.name || 'this check'}</span> —
            history, uptime bars and latency graphs all reset to empty. The check itself stays
            exactly as configured and keeps probing. This cannot be undone.
          </p>
          <div class="form-actions">
            {#if pending === 'purge'}
              <span class="confirm">Delete all recorded history?</span>
              <button class="btn danger" onclick={confirmPurge} disabled={dangerBusy}>
                {dangerBusy ? 'Purging…' : 'Yes, purge the data'}
              </button>
              <button class="btn" onclick={() => (pending = null)} disabled={dangerBusy}>
                Cancel
              </button>
            {:else}
              <button class="btn danger" onclick={() => (pending = 'purge')}>Purge data</button>
            {/if}
          </div>
        </div>

        <div class="danger-act">
          <div class="danger-title">Delete check</div>
          <p class="admin-muted">
            Removes <span class="mono">{draft.name || 'this check'}</span>'s configuration and its
            place on the status page. Nothing will probe it again and the name becomes free to
            reuse. Its recorded history — results, alerts, incidents — is <em>not</em> touched by
            this; it stays keyed on this check's id, just with no configuration left to name it.
            Use "erase everything" below if that history should go too. This cannot be undone.
          </p>
          <div class="form-actions">
            {#if pending === 'delete'}
              <span class="confirm">Delete the check, keeping its recorded history?</span>
              <button class="btn danger" onclick={confirmDelete} disabled={dangerBusy}>
                {dangerBusy ? 'Deleting…' : 'Yes, delete the check'}
              </button>
              <button class="btn" onclick={() => (pending = null)} disabled={dangerBusy}>
                Cancel
              </button>
            {:else}
              <button class="btn danger" onclick={() => (pending = 'delete')}>Delete check</button>
            {/if}
          </div>
        </div>

        <div class="danger-act">
          <div class="danger-title">Delete check and erase everything</div>
          <p class="admin-muted">
            Removes <span class="mono">{draft.name || 'this check'}</span> and every trace it left
            behind: its configuration, every recorded result and alert, and every incident it ever
            opened or joined. Nothing is left orphaned. This is the option for a check that should
            never have existed — not one you might reuse the name for later. This cannot be undone.
          </p>
          <div class="form-actions">
            {#if pending === 'erase'}
              <span class="confirm">Delete the check and erase everything it ever recorded?</span>
              <button class="btn danger" onclick={confirmErase} disabled={dangerBusy}>
                {dangerBusy ? 'Erasing…' : 'Yes, erase everything'}
              </button>
              <button class="btn" onclick={() => (pending = null)} disabled={dangerBusy}>
                Cancel
              </button>
            {:else}
              <button class="btn danger" onclick={() => (pending = 'erase')}>
                Delete check and erase everything
              </button>
            {/if}
          </div>
        </div>
      </div>
    </details>
  {/if}

  <div class="save-bar">
    {#if saveError}<p class="admin-error">{saveError}</p>{/if}
    {#if savedNote}<p class="admin-muted">{savedNote}</p>{/if}
    <div class="form-actions">
      <button class="btn primary" onclick={save} disabled={saving}>
        {saving ? 'Saving…' : creating ? 'Create check' : 'Save changes'}
      </button>
      <button class="btn" onclick={onclose} disabled={saving}>Cancel</button>
    </div>
  </div>
{/if}

<style>
  .area-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    margin-bottom: 0.6rem;
  }
  .no-gap {
    margin-bottom: 0;
    text-transform: none;
    letter-spacing: 0;
    font-size: 0.95rem;
    color: var(--text);
  }

  .head-acts {
    align-items: center;
    gap: 0.9rem;
  }
  .enable-toggle {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
    cursor: pointer;
    white-space: nowrap;
  }
  /* A disabled check is not a broken one, so the header dims rather than
     reddens — but it must not read as normal either, with every panel shut. */
  .area-bar.off .no-gap {
    color: var(--text-dim);
  }
  .off-pill {
    font-size: 0.6rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    padding: 0.1rem 0.4rem;
    border-radius: 999px;
    border: 1px solid var(--amber);
    color: var(--amber);
  }

  /* One line of orientation at the top of a section, before the controls. */
  .lede {
    display: block;
    margin: 0 0 0.8rem;
    line-height: 1.5;
    max-width: 68ch;
  }

  /* Nested disclosure for the fields most checks never touch. Quieter than the
     section summaries above it: this is a second-order fold, not a peer. */
  .advanced {
    margin-top: 0.9rem;
    border-top: 1px solid var(--border);
  }
  .advanced-summary {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.55rem 0;
    cursor: pointer;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
    user-select: none;
    list-style: none;
  }
  .advanced-summary::-webkit-details-marker {
    display: none;
  }
  .advanced-summary:hover {
    color: var(--text);
  }
  .advanced-summary:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }
  .advanced-summary .chev {
    transition: transform 0.12s ease;
  }
  .advanced[open] > .advanced-summary .chev {
    transform: rotate(90deg);
  }
  .advanced-body {
    padding-bottom: 0.3rem;
  }
  /* Counts what departs from the defaults. Lit rather than grey: a fold that
     hides a changed value must say so on its own lid. */
  .badge-count {
    font-size: 0.62rem;
    font-weight: 600;
    letter-spacing: 0.03em;
    padding: 0.05rem 0.4rem;
    border-radius: 999px;
    border: 1px solid var(--accent);
    color: var(--accent);
    text-transform: none;
  }

  .form-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.7rem 1rem;
    align-items: start;
  }
  .field.wide {
    grid-column: span 2;
  }
  /* Same specificity as .field.wide (0,2,0) so the full-width intent wins:
     .field.wide alone would override `1 / -1` with `span 2`. */
  .field.switches {
    grid-column: 1 / -1;
    gap: 0.35rem;
  }
  .agent-picker {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 0.9rem;
  }
  .field code {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--text);
  }

  /* A check nothing probes must not read as fine when the panel is shut. */
  .sum-note.bad {
    color: var(--down);
  }
  .interval-example {
    color: var(--text);
  }

  .log-table-wrap {
    overflow-x: auto;
  }
  .log-table {
    width: 100%;
    min-width: 40rem;
    table-layout: fixed;
    border-collapse: collapse;
    font-size: 0.8rem;
  }
  .log-table th,
  .log-table td {
    width: 20%;
    padding: 0.4rem 0.75rem;
    text-align: left;
    vertical-align: middle;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  .log-table th {
    color: var(--text-dim);
    font-size: 0.7rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }
  .log-table tbody tr:last-child td {
    border-bottom: none;
  }
  .log-reason {
    color: var(--down);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* Two destructive actions in one panel: the rule between them stops "purge"
     and "delete" reading as one block with two buttons. */
  .danger-act + .danger-act {
    margin-top: 1rem;
    padding-top: 1rem;
    border-top: 1px solid rgba(248, 81, 73, 0.25);
  }
  .danger-title {
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--down);
    margin-bottom: 0.25rem;
  }
  .confirm {
    font-size: 0.75rem;
    color: var(--down);
    align-self: center;
  }

  .form-actions {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-top: 0.6rem;
    flex-wrap: wrap;
  }

  .save-bar {
    border-top: 1px solid var(--border);
    padding: 0.7rem 0 0.4rem;
  }
  .save-bar .form-actions {
    margin-top: 0;
  }

  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .dim {
    color: var(--text-dim);
    white-space: nowrap;
  }
</style>
