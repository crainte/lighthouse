<script lang="ts">
  import type { Agent, CheckKind, HistoryPoint } from '../lib/api';
  import { UNKNOWN_LOCATION, fetchAgents, fetchHistory } from '../lib/api';
  import { failureReason, formatLatency, formatRelativeTime, statusColor } from '../lib/format';
  import type {
    AdminCheck,
    CheckPayload,
    DefaultStatus,
    EvalResult,
    MatchMode,
    Section,
  } from '../lib/admin';
  import {
    AdminError,
    createCheck,
    deleteCheck,
    fetchAdminChecks,
    fetchSections,
    purgeCheckData,
    testEval,
    updateCheck,
  } from '../lib/admin';

  let {
    name,
    onsaved,
    onclose,
  }: { name: string | null; onsaved: () => void; onclose: () => void } = $props();

  // Number inputs go empty rather than to zero when cleared, so the draft holds
  // nulls and the guards below refuse them; AdminCheck itself stays honest.
  // `group` and the optional strings are non-optional here because `bind:value`
  // needs a string, and `position` is absent because the server owns ordering.
  type Draft = Omit<
    AdminCheck,
    | 'timeout_secs'
    | 'expect'
    | 'group'
    | 'eval'
    | 'description'
    | 'image'
    | 'tag'
    | 'position'
    | 'dns'
    | 'ping'
    | 'cert_warning_days'
    | 'grace_secs'
    | 'placement'
  > & {
    timeout_secs: number | null;
    expect_status: number | null;
    expect_body: string;
    group: string;
    eval: string;
    description: string;
    image: string;
    tag: string;
    dns: { nameserver: string; record: string; match_type: MatchMode; expected: string[] };
    ping: { ip_type: string; count: number | null };
    cert_warning_days: number | null;
    grace_secs: number | null;
    // Same reason as every other number here: cleared reads as empty, not zero.
    placement: { zones: string[]; regions: string[]; per_zone: number | null };
  };

  const KINDS: CheckKind[] = ['http', 'tcp', 'dns', 'icmp', 'ssl', 'heartbeat'];
  const MATCH_MODES: MatchMode[] = ['any', 'all'];
  const DEFAULT_STATUSES: DefaultStatus[] = ['up', 'down', 'unknown'];
  const DNS_RECORDS = ['A', 'AAAA', 'CNAME', 'MX', 'TXT', 'NS'];
  const IP_TYPES = ['any', 'ipv4', 'ipv6'];
  const LOG_LIMIT = 50;

  function blank(): Draft {
    return {
      name: '',
      type: 'http',
      target: '',
      hosts: [],
      host_match: 'all',
      cron: '*/30 * * * * *',
      timeout_secs: 10,
      group: '',
      latency_graph: false,
      expect_status: null,
      expect_body: '',
      enabled: true,
      visible: true,
      eval: '',
      agents: [],
      description: '',
      image: '',
      tag: '',
      default_status: 'unknown',
      headers: [],
      allow_self_signed: false,
      dns: { nameserver: '', record: 'A', match_type: 'all', expected: [] },
      ping: { ip_type: 'any', count: 1 },
      cert_warning_days: 14,
      grace_secs: 300,
      placement: { zones: [], regions: [], per_zone: 1 },
    };
  }

  // Deep-copied field by field rather than spread: the draft must not alias the
  // arrays inside the loaded check, or "cancel" would still have mutated them.
  function fromCheck(c: AdminCheck): Draft {
    const d = blank();
    return {
      ...d,
      name: c.name,
      type: c.type,
      target: c.target ?? '',
      hosts: [...(c.hosts ?? [])],
      host_match: c.host_match ?? d.host_match,
      cron: c.cron ?? d.cron,
      timeout_secs: c.timeout_secs ?? d.timeout_secs,
      group: c.group ?? '',
      latency_graph: c.latency_graph ?? false,
      expect_status: c.expect?.status ?? null,
      expect_body: c.expect?.body_contains ?? '',
      enabled: c.enabled ?? true,
      visible: c.visible ?? true,
      eval: c.eval ?? '',
      agents: [...(c.agents ?? [])],
      description: c.description ?? '',
      image: c.image ?? '',
      tag: c.tag ?? '',
      default_status: c.default_status ?? d.default_status,
      headers: (c.headers ?? []).map((h) => ({ name: h.name, value: h.value })),
      allow_self_signed: c.allow_self_signed ?? false,
      dns: {
        nameserver: c.dns?.nameserver ?? '',
        record: c.dns?.record ?? d.dns.record,
        match_type: c.dns?.match_type ?? d.dns.match_type,
        expected: [...(c.dns?.expected ?? [])],
      },
      ping: { ip_type: c.ping?.ip_type ?? d.ping.ip_type, count: c.ping?.count ?? d.ping.count },
      cert_warning_days: c.cert_warning_days ?? d.cert_warning_days,
      grace_secs: c.grace_secs ?? d.grace_secs,
      placement: {
        zones: [...(c.placement?.zones ?? [])],
        regions: [...(c.placement?.regions ?? [])],
        per_zone: c.placement?.per_zone ?? 1,
      },
    };
  }

  let draft = $state<Draft>(blank());
  /** Server-owned ordering, round-tripped untouched. Absent on create. */
  let position = $state<number | undefined>(undefined);
  let sections = $state<Section[]>([]);
  /** Every agent the coordinator has ever heard from, with its liveness — the
   *  picker needs both the ids and which of them are still reporting. */
  let agentRows = $state<Agent[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let saving = $state(false);
  let saveError = $state<string | null>(null);
  let savedNote = $state<string | null>(null);

  let logs = $state<HistoryPoint[]>([]);
  let logsError = $state<string | null>(null);
  let logsLoading = $state(false);

  let evalResult = $state<EvalResult | null>(null);
  let evalError = $state<string | null>(null);
  let testing = $state(false);
  let sample = $state<{ status: number | null; latency: number | null; body: string }>({
    status: 200,
    latency: 42,
    body: 'ok',
  });

  // Each destructive action confirms on its own, so arming one never arms the
  // other — they destroy very different things.
  let pending = $state<'purge' | 'delete' | null>(null);
  let dangerBusy = $state(false);
  let dangerError = $state<string | null>(null);
  let dangerNote = $state<string | null>(null);

  let copied = $state(false);
  let copyError = $state<string | null>(null);

  const kind = $derived(draft.type);
  const creating = $derived(name === null);

  // The URL is the credential, so it only exists once the check does; for an
  // unsaved check we say so rather than printing a 404.
  const heartbeatUrl = $derived(
    name === null ? null : `${location.origin}/api/v1/heartbeat/${encodeURIComponent(name)}`,
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
   *  eligible live agents grouped by zone, `per_zone` taken from each zone —
   *  so it is per zone, never a total. Which specific agents win is rendezvous
   *  hashing and irrelevant here; how many, and where, is the whole question. */
  const effect = $derived.by(() => {
    const p = draft.placement;
    const per = Math.max(1, p.per_zone ?? 1);
    const counts = new Map<string, number>();
    for (const a of live) {
      if (p.zones.length > 0 && !p.zones.includes(a.zone)) continue;
      if (p.regions.length > 0 && !p.regions.includes(a.region)) continue;
      counts.set(a.zone, (counts.get(a.zone) ?? 0) + 1);
    }
    const zones = [...counts]
      .map(([zone, available]) => ({ zone, probes: Math.min(available, per), available }))
      .sort((x, y) => x.zone.localeCompare(y.zone));
    return { zones, total: zones.reduce((n, z) => n + z.probes, 0), per };
  });

  /** Free-text buffer per axis, so a zone can be named before any agent in it
   *  has ever reported. Keyed by axis because the picker below is one snippet
   *  rendered twice. */
  let adding = $state<{ zones: string; regions: string }>({ zones: '', regions: '' });

  const cronHint = $derived(describeCron(draft.cron));

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
    return 'Warden reads this schedule; this hint cannot put it in words.';
  }

  function fail(e: unknown): string {
    return e instanceof Error ? e.message : String(e);
  }

  async function load() {
    loading = true;
    loadError = null;
    try {
      // The pickers would silently offer the wrong choices on a partial load,
      // so the three come together or the failure is reported.
      const [checks, secs, ag] = await Promise.all([
        fetchAdminChecks(),
        fetchSections(),
        fetchAgents(),
      ]);
      sections = secs;
      agentRows = ag.agents;
      if (name !== null) {
        const found = checks.find((c) => c.name === name);
        if (!found) throw new Error(`No check named “${name}”. It may have been deleted.`);
        draft = fromCheck(found);
        position = found.position;
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
    if (name === null) return;
    logsLoading = true;
    logsError = null;
    try {
      const r = await fetchHistory(name, LOG_LIMIT);
      logs = r.history;
    } catch (e) {
      logsError = fail(e);
    } finally {
      logsLoading = false;
    }
  }

  load();
  loadLogs();

  /** These map to unsigned integers server-side. A negative or fractional value
   *  can be typed past the `min` attribute and would fail JSON deserialization,
   *  surfacing a framework message instead of Warden's own. */
  function whole(v: number | null, min: number): boolean {
    return v !== null && Number.isInteger(v) && v >= min;
  }

  function addHost() {
    draft.hosts.push('');
  }
  function addHeader() {
    draft.headers.push({ name: '', value: '' });
  }
  function addExpected() {
    draft.dns.expected.push('');
  }

  /** Adding a location the fleet has never reported is deliberate: a zone is
   *  usually configured before the agent that will live in it boots. */
  function addLocation(axis: 'zones' | 'regions', value: string) {
    const v = value.trim();
    if (v && !draft.placement[axis].includes(v)) draft.placement[axis].push(v);
  }

  function toggleLocation(axis: 'zones' | 'regions', value: string, on: boolean) {
    const list = draft.placement[axis];
    const at = list.indexOf(value);
    if (on && at === -1) list.push(value);
    if (!on && at !== -1) list.splice(at, 1);
  }

  async function save() {
    if (saving) return;
    savedNote = null;
    const n = draft.name.trim();
    if (!n) return (saveError = 'Name is required.');
    const k = draft.type;
    if (k !== 'heartbeat' && !draft.target.trim()) return (saveError = 'Target is required.');
    if (k !== 'heartbeat' && !whole(draft.timeout_secs, 1))
      return (saveError = 'Timeout must be a whole number of seconds, at least 1.');
    if (k === 'icmp' && !whole(draft.ping.count, 1))
      return (saveError = 'Ping count must be a whole number, at least 1.');
    if (k === 'ssl' && !whole(draft.cert_warning_days, 0))
      return (saveError = 'Certificate warning must be a whole number of days, at least 0.');
    if (k === 'heartbeat' && !whole(draft.grace_secs, 10))
      return (saveError = 'Grace must be a whole number of seconds, at least 10.');
    if (!whole(draft.placement.per_zone, 1))
      return (saveError = 'Agents per zone must be a whole number, at least 1.');

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
      cert_warning_days: draft.cert_warning_days ?? 14,
      grace_secs: draft.grace_secs ?? 300,
      placement: {
        zones: trimmed(draft.placement.zones),
        regions: trimmed(draft.placement.regions),
        per_zone: draft.placement.per_zone ?? 1,
      },
    };
    // Ordering is the server's: an edit round-trips the position it handed us,
    // a create has none until the server assigns one.
    if (position !== undefined) payload.position = position;
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
      if (name === null) {
        await createCheck(payload);
        onsaved();
        // A created check's identity is now fixed; the table is where you pick
        // it up again, and it proves the row landed.
        onclose();
        return;
      }
      const updated = await updateCheck(name, payload);
      // Round-trip what the server stored, so trimmed and defaulted values are
      // what you are looking at rather than what you typed.
      draft = fromCheck(updated);
      position = updated.position;
      savedNote = 'Saved.';
      onsaved();
    } catch (e) {
      saveError = fail(e);
    } finally {
      saving = false;
    }
  }

  async function runTest() {
    if (testing) return;
    testing = true;
    evalResult = null;
    evalError = null;
    try {
      evalResult = await testEval({
        eval: draft.eval,
        status: sample.status ?? 0,
        latency: sample.latency ?? 0,
        body: sample.body,
      });
    } catch (e) {
      evalError = fail(e);
    } finally {
      testing = false;
    }
  }

  async function confirmPurge() {
    if (name === null || dangerBusy) return;
    dangerBusy = true;
    dangerError = null;
    dangerNote = null;
    try {
      const rows = await purgeCheckData(name);
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
    if (name === null || dangerBusy) return;
    dangerBusy = true;
    dangerError = null;
    try {
      await deleteCheck(name);
      onsaved();
      onclose();
    } catch (e) {
      dangerError = fail(e);
      dangerBusy = false;
    }
  }

  async function copyHeartbeat() {
    if (!heartbeatUrl) return;
    copyError = null;
    try {
      await navigator.clipboard.writeText(heartbeatUrl);
      copied = true;
      setTimeout(() => (copied = false), 1500);
    } catch {
      // Clipboard access needs a secure context, which a LAN deployment over
      // plain HTTP is not. Saying so beats a button that quietly does nothing.
      copyError = 'Clipboard unavailable here — select the URL above and copy it.';
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
  <div class="area-bar">
    <div class="title-line">
      <span class="panel-title no-gap mono">{creating ? 'New check' : name}</span>
      <span class="kind-pill">{kind}</span>
    </div>
    <div class="range-switch">
      <button class="range-btn" onclick={onclose}>← All checks</button>
    </div>
  </div>

  <details class="admin-panel editor-section" open>
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> General settings
    </summary>
    <div class="editor-body">
      <div class="form-grid">
        <label class="field">
          <span class="field-label">Name</span>
          <input
            class="mono"
            bind:value={draft.name}
            readonly={!creating}
            placeholder="api-gateway"
          />
          <span class="field-note">
            {creating
              ? 'Letters, digits, dash, underscore and dot.'
              : 'Identity — rename by creating a new check.'}
          </span>
        </label>

        <label class="field">
          <span class="field-label">Tag</span>
          <input bind:value={draft.tag} placeholder="(none)" />
          <span class="field-note">Short label for scanning the checks table.</span>
        </label>

        <label class="field">
          <span class="field-label">Image</span>
          <input bind:value={draft.image} placeholder="🛰️ or https://…/icon.png" />
          <span class="field-note">An emoji or an image URL.</span>
        </label>

        <label class="field wide">
          <span class="field-label">Description</span>
          <input bind:value={draft.description} placeholder="(none)" />
        </label>

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

        <label class="field wide">
          <span class="field-label">Cron</span>
          <input class="mono" bind:value={draft.cron} placeholder="*/30 * * * * *" />
          <span class="field-note"><code>{draft.cron.trim() || '(empty)'}</code> — {cronHint}</span>
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

        <div class="field wide switches">
          <span class="field-label">Switches</span>
          <label class="toggle">
            <input type="checkbox" bind:checked={draft.enabled} />
            <span>Enabled <em>— probe it. Off keeps the config and history.</em></span>
          </label>
          <label class="toggle">
            <input type="checkbox" checked={!draft.visible} onchange={() => (draft.visible = !draft.visible)} />
            <span>Hidden <em>— keep it off the public status page. It is still probed.</em></span>
          </label>
          <label class="toggle">
            <input type="checkbox" bind:checked={draft.latency_graph} />
            <span>Latency graph <em>— draw the sparkline under it.</em></span>
          </label>
        </div>

        <div class="field wide">
          <span class="field-label">Pin to agents</span>
          {#if agentOptions.length === 0}
            <span class="field-note">
              No agent has reported yet. Placement, in Configuration, decides who probes this check
              once some do.
            </span>
          {:else}
            <div class="agent-picker">
              {#each agentOptions as id (id)}
                <label class="toggle">
                  <input type="checkbox" value={id} bind:group={draft.agents} />
                  <span class="agent-pill">{id}</span>
                  {#if !reporting.has(id)}
                    <em
                      class="stale"
                      title="This agent has not reported recently. A check pinned only to silent agents is never probed."
                      >not reporting</em
                    >
                  {/if}
                </label>
              {/each}
            </div>
            <span class="field-note">
              None ticked is the normal case: Placement, in Configuration, decides who probes this
              check. Ticking agents pins it to exactly those and overrides placement entirely —
              for a probe that genuinely belongs to one box, such as a machine's own loopback port.
            </span>
          {/if}
        </div>
      </div>
    </div>
  </details>

  <!-- General is the only section open on arrival; the rest are one click, and
       a form that opens as four screens of fields is the crowding this page
       exists to undo. -->
  <details class="admin-panel editor-section">
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Configuration
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
        </label>

        {#if kind === 'http'}
          <label class="field wide">
            <span class="field-label">Target URL</span>
            <input class="mono" bind:value={draft.target} placeholder="https://example.com/health" />
          </label>

          <label class="field">
            <span class="field-label">Timeout (s)</span>
            <input type="number" min="1" bind:value={draft.timeout_secs} />
          </label>

          <label class="field">
            <span class="field-label">Expect status</span>
            <input type="number" min="100" max="599" bind:value={draft.expect_status} placeholder="(any 2xx–3xx)" />
          </label>

          <label class="field wide">
            <span class="field-label">Expect body contains</span>
            <input bind:value={draft.expect_body} placeholder="(any response body)" />
          </label>

          <div class="field wide">
            <span class="field-label">Headers</span>
            {#if draft.headers.length === 0}
              <span class="field-note">None. The probe sends only Warden's defaults.</span>
            {:else}
              <!-- Keyed by index: two headers may legitimately share a name, and
                   a duplicate key aborts the render of the whole page. -->
              {#each draft.headers as _header, i (i)}
                <div class="pair-row">
                  <input class="mono pair-name" bind:value={draft.headers[i].name} placeholder="Authorization" />
                  <input class="mono" bind:value={draft.headers[i].value} placeholder="Bearer …" />
                  <button class="btn danger" onclick={() => draft.headers.splice(i, 1)}>Remove</button>
                </div>
              {/each}
            {/if}
            <div class="add-row">
              <button class="btn" onclick={addHeader}>+ Add header</button>
            </div>
          </div>

          <div class="field wide">
            <span class="field-label">TLS</span>
            <label class="toggle">
              <input type="checkbox" bind:checked={draft.allow_self_signed} />
              <span>
                Allow self-signed
                <em>— accept an untrusted certificate. Off means HTTPS is verified.</em>
              </span>
            </label>
          </div>
        {:else if kind === 'tcp'}
          <label class="field wide">
            <span class="field-label">Target</span>
            <input class="mono" bind:value={draft.target} placeholder="db.internal:5432" />
            <span class="field-note">host:port — a TCP probe with no port has nothing to connect to.</span>
          </label>

          <label class="field">
            <span class="field-label">Timeout (s)</span>
            <input type="number" min="1" bind:value={draft.timeout_secs} />
          </label>

          <div class="field wide">
            <span class="field-label">Additional hosts</span>
            {#if draft.hosts.length === 0}
              <span class="field-note">None. Only the target above is probed.</span>
            {:else}
              {#each draft.hosts as _host, i (i)}
                <div class="pair-row">
                  <input class="mono" bind:value={draft.hosts[i]} placeholder="db-replica.internal:5432" />
                  <button class="btn danger" onclick={() => draft.hosts.splice(i, 1)}>Remove</button>
                </div>
              {/each}
            {/if}
            <div class="add-row">
              <button class="btn" onclick={addHost}>+ Add host</button>
            </div>
          </div>

          <label class="field">
            <span class="field-label">Host match</span>
            <select bind:value={draft.host_match}>
              {#each MATCH_MODES as m (m)}
                <option value={m}>{m}</option>
              {/each}
            </select>
            <span class="field-note">
              all = every host must connect. any = one is enough, for a redundant pool.
            </span>
          </label>
        {:else if kind === 'icmp'}
          <label class="field wide">
            <span class="field-label">Target host</span>
            <input class="mono" bind:value={draft.target} placeholder="1.1.1.1" />
            <span class="field-note">A hostname or IP, not a URL.</span>
          </label>

          <label class="field">
            <span class="field-label">Timeout (s)</span>
            <input type="number" min="1" bind:value={draft.timeout_secs} />
          </label>

          <div class="field wide">
            <span class="field-label">Additional hosts</span>
            {#if draft.hosts.length === 0}
              <span class="field-note">None. Only the target above is pinged.</span>
            {:else}
              {#each draft.hosts as _host, i (i)}
                <div class="pair-row">
                  <input class="mono" bind:value={draft.hosts[i]} placeholder="8.8.8.8" />
                  <button class="btn danger" onclick={() => draft.hosts.splice(i, 1)}>Remove</button>
                </div>
              {/each}
            {/if}
            <div class="add-row">
              <button class="btn" onclick={addHost}>+ Add host</button>
            </div>
          </div>

          <label class="field">
            <span class="field-label">Host match</span>
            <select bind:value={draft.host_match}>
              {#each MATCH_MODES as m (m)}
                <option value={m}>{m}</option>
              {/each}
            </select>
            <span class="field-note">all = every host must reply. any = one is enough.</span>
          </label>

          <label class="field">
            <span class="field-label">IP type</span>
            <select bind:value={draft.ping.ip_type}>
              {#each IP_TYPES as t (t)}
                <option value={t}>{t}</option>
              {/each}
            </select>
            <span class="field-note">Which address family the host resolves to.</span>
          </label>

          <label class="field">
            <span class="field-label">Ping count</span>
            <input type="number" min="1" bind:value={draft.ping.count} />
            <span class="field-note">Echoes sent per attempt.</span>
          </label>
        {:else if kind === 'dns'}
          <label class="field wide">
            <span class="field-label">Target host</span>
            <input class="mono" bind:value={draft.target} placeholder="example.com" />
            <span class="field-note">The name to look up — not a URL.</span>
          </label>

          <label class="field">
            <span class="field-label">Timeout (s)</span>
            <input type="number" min="1" bind:value={draft.timeout_secs} />
          </label>

          <label class="field">
            <span class="field-label">Nameserver</span>
            <input class="mono" bind:value={draft.dns.nameserver} placeholder="(system resolver)" />
          </label>

          <label class="field">
            <span class="field-label">Record</span>
            <select bind:value={draft.dns.record}>
              {#each DNS_RECORDS as r (r)}
                <option value={r}>{r}</option>
              {/each}
            </select>
          </label>

          <label class="field">
            <span class="field-label">Match type</span>
            <select bind:value={draft.dns.match_type}>
              {#each MATCH_MODES as m (m)}
                <option value={m}>{m}</option>
              {/each}
            </select>
            <span class="field-note">all = every expected value must appear. any = one of them.</span>
          </label>

          <div class="field wide">
            <span class="field-label">Expected answers</span>
            {#if draft.dns.expected.length === 0}
              <span class="field-note">None. Resolving at all is enough.</span>
            {:else}
              {#each draft.dns.expected as _value, i (i)}
                <div class="pair-row">
                  <input class="mono" bind:value={draft.dns.expected[i]} placeholder="93.184.216.34" />
                  <button class="btn danger" onclick={() => draft.dns.expected.splice(i, 1)}>
                    Remove
                  </button>
                </div>
              {/each}
            {/if}
            <div class="add-row">
              <button class="btn" onclick={addExpected}>+ Add value</button>
            </div>
          </div>
        {:else if kind === 'ssl'}
          <label class="field wide">
            <span class="field-label">Target host</span>
            <input class="mono" bind:value={draft.target} placeholder="example.com" />
            <span class="field-note">A hostname, not a URL. Port 443 is assumed.</span>
          </label>

          <label class="field">
            <span class="field-label">Timeout (s)</span>
            <input type="number" min="1" bind:value={draft.timeout_secs} />
          </label>

          <label class="field">
            <span class="field-label">Warn before expiry (days)</span>
            <input type="number" min="0" bind:value={draft.cert_warning_days} />
            <span class="field-note">DOWN once the certificate is this close to expiring.</span>
          </label>
        {:else}
          <label class="field">
            <span class="field-label">Grace (s)</span>
            <input type="number" min="10" bind:value={draft.grace_secs} />
            <span class="field-note">No ping within this window and the check goes DOWN.</span>
          </label>

          <div class="field wide">
            <span class="field-label">Ping URL</span>
            {#if heartbeatUrl}
              <div class="pair-row">
                <input class="mono" readonly value={heartbeatUrl} />
                <button class="btn" onclick={copyHeartbeat}>{copied ? 'Copied' : 'Copy'}</button>
              </div>
              <span class="field-note">
                Have the job call this URL (GET or POST) when it finishes. It needs no
                authentication — the URL itself is the credential, so treat it like one.
              </span>
              {#if copyError}<span class="admin-error">{copyError}</span>{/if}
            {:else}
              <span class="field-note">
                The ping URL is built from the check's name, so it appears here once this check
                has been saved.
              </span>
            {/if}
          </div>
        {/if}

        <div class="field wide placement">
          <span class="field-label">Placement</span>
          {#if pinned}
            <p class="field-note override">
              Not in effect. This check pins {draft.agents.length}
              {draft.agents.length === 1 ? 'agent' : 'agents'} in General settings, and an explicit
              pin wins outright — placement is ignored entirely while that list is non-empty. Clear
              it to place this check by location instead.
            </p>
          {/if}
          <p class="field-note">
            Where this check runs, described by location rather than by machine, so growing the
            fleet does not mean revisiting every check. Leave both lists empty and it runs
            everywhere — once per zone, not once per agent.
          </p>

          <!-- Declared here, rendered twice below: regions and zones are the
               same picker over a different axis. -->
          {#snippet locations(axis: 'zones' | 'regions', noun: string, options: string[])}
            <div class="field">
              <span class="field-label">{noun}s</span>
              {#if options.length > 0}
                <div class="agent-picker">
                  <!-- Set-deduped above, so no value can repeat. -->
                  {#each options as v (v)}
                    <label class="toggle">
                      <input
                        type="checkbox"
                        checked={draft.placement[axis].includes(v)}
                        onchange={(e) => toggleLocation(axis, v, e.currentTarget.checked)}
                      />
                      <span class="agent-pill">{v}</span>
                      {#if v === UNKNOWN_LOCATION}
                        <em
                          class="stale"
                          title="Agents that declare no zone or region of their own. Set them in the agent's config."
                          >undeclared</em
                        >
                      {/if}
                    </label>
                  {/each}
                </div>
              {/if}
              <div class="pair-row">
                <input class="mono" bind:value={adding[axis]} placeholder="{noun} not in the fleet yet" />
                <button
                  class="btn"
                  onclick={() => {
                    addLocation(axis, adding[axis]);
                    adding[axis] = '';
                  }}
                >
                  + Add
                </button>
              </div>
              <span class="field-note">
                {draft.placement[axis].length === 0
                  ? `None ticked — every ${noun} is allowed.`
                  : `Restricted to ${draft.placement[axis].length} ${noun}${draft.placement[axis].length === 1 ? '' : 's'}.`}
                Offered {noun}s come from the live fleet; add one by hand for a {noun} whose agents
                have not booted yet.
              </span>
            </div>
          {/snippet}

          <div class="placement-axes">
            {@render locations('regions', 'region', regionOptions)}
            {@render locations('zones', 'zone', zoneOptions)}
            <label class="field">
              <span class="field-label">Agents per zone</span>
              <input type="number" min="1" bind:value={draft.placement.per_zone} />
              <span class="field-note">
                How many agents inside <em>each</em> matched zone probe this — not a total. Raising
                it to 2 buys a second opinion before a service is called down, at the price of a
                duplicate probe and a duplicate row every time it runs.
              </span>
            </label>
          </div>

          <!-- The number that stops a 20x fan-out: what this spec does to the
               fleet as it stands right now, not what it means in the abstract. -->
          {#if pinned}
            <p class="effect muted">
              With pinning on, {draft.agents.length}
              {draft.agents.length === 1 ? 'agent probes' : 'agents probe'} this check — the spec above
              changes nothing until the pin is cleared.
            </p>
          {:else if effect.total === 0}
            <p class="effect none">
              No live agent matches this placement, so nothing probes this check. It will report
              nothing rather than fail — silence, not an error.
            </p>
          {:else}
            <p class="effect">
              Right now this places <strong>{effect.total}</strong>
              {effect.total === 1 ? 'probe' : 'probes'} across {effect.zones.length}
              {effect.zones.length === 1 ? 'zone' : 'zones'}:
              {#each effect.zones as z (z.zone)}
                <span class="effect-zone" class:short={z.available < effect.per}>
                  {z.zone} ×{z.probes}{z.available < effect.per ? ` (only ${z.available} live)` : ''}
                </span>
              {/each}
            </p>
          {/if}
        </div>
      </div>

      <div class="eval-block">
        <div class="field-label">Custom eval (Lua)</div>
        <textarea
          class="mono"
          rows="10"
          spellcheck="false"
          bind:value={draft.eval}
          placeholder={'local code = math.floor(status / 100)\nif status == 429 or (code >= 2 and code <= 3) then\n  return "UP", latency\nend\nreturn "DOWN"'}
        ></textarea>
        <p class="field-note">
          In scope: <code>status</code> (number, 0 when the probe never connected),
          <code>latency</code>
          (ms), <code>body</code> (string, truncated), <code>headers</code> (table, lowercased
          keys). Return <code>"UP"</code> or <code>"DOWN"</code>, optionally a latency override as a
          second value. Returning nothing — or leaving this empty — uses the built-in 2xx–3xx logic.
        </p>

        <div class="try-row">
          <label class="field try-num">
            <span class="field-label">status</span>
            <input type="number" bind:value={sample.status} />
          </label>
          <label class="field try-num">
            <span class="field-label">latency (ms)</span>
            <input type="number" bind:value={sample.latency} />
          </label>
          <label class="field try-body">
            <span class="field-label">body</span>
            <input class="mono" bind:value={sample.body} />
          </label>
          <button class="btn" onclick={runTest} disabled={testing}>
            {testing ? 'Testing…' : 'Test script'}
          </button>
        </div>

        {#if evalError}
          <p class="admin-error mono">{evalError}</p>
        {:else if evalResult}
          <p class="verdict">
            <span class="verdict-pill {evalResult.verdict}">{evalResult.verdict.toUpperCase()}</span>
            <span class="admin-muted">reported latency {evalResult.latency_ms} ms</span>
          </p>
        {/if}
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
          <div class="admin-rows">
            <!-- Keyed by index: several agents can report the same millisecond,
                 so (timestamp, agent) is not guaranteed unique and a duplicate
                 key aborts the render of the whole page. -->
            {#each logs as h, i (i)}
              <div class="log-row">
                <span class="dim">{formatRelativeTime(h.timestamp)}</span>
                <span class="agent-pill">{h.agent_id}</span>
                <span class="mono">{formatLatency(h.latency_ms)}</span>
                <span class="badge" style="background:{statusColor(h.status)}">{h.status}</span>
                <span class="log-reason" title={failureReason(h) ?? ''}
                  >{failureReason(h) ?? ''}</span
                >
              </div>
            {/each}
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
            Deletes every recorded attempt for <span class="mono">{name}</span> — history, uptime
            bars and latency graphs all reset to empty. The check itself stays exactly as
            configured and keeps probing. This cannot be undone.
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
            Removes <span class="mono">{name}</span> entirely: its configuration, its recorded
            history and its place on the status page. Nothing will probe it again and the name
            becomes free to reuse. This cannot be undone.
          </p>
          <div class="form-actions">
            {#if pending === 'delete'}
              <span class="confirm">Delete the check and everything it recorded?</span>
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

  .form-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.7rem 1rem;
    align-items: start;
  }
  .field.wide {
    grid-column: span 2;
  }
  .switches {
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

  /* One repeated entry: value fields stretch, the remove button never does. */
  .pair-row {
    display: flex;
    gap: 0.4rem;
    align-items: center;
  }
  .pair-row + .pair-row {
    margin-top: 0.3rem;
  }
  .pair-name {
    flex: 0 1 14rem;
  }
  .add-row {
    margin-top: 0.4rem;
  }

  /* Full width and boxed: placement is one decision made of four controls, and
     scattering them across the three-column grid reads as four unrelated ones. */
  .placement {
    grid-column: 1 / -1;
    gap: 0.45rem;
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.6rem 0.7rem;
  }
  .placement-axes {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(15rem, 1fr));
    gap: 0.7rem 1rem;
    align-items: start;
  }
  .override {
    color: var(--amber);
    line-height: 1.5;
  }
  .effect {
    margin: 0.2rem 0 0;
    font-size: 0.78rem;
    line-height: 1.6;
    color: var(--text);
  }
  .effect.none {
    color: var(--down);
  }
  .effect.muted {
    color: var(--text-dim);
  }
  .effect-zone {
    display: inline-block;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.72rem;
    padding: 0.05rem 0.4rem;
    margin: 0 0.2rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--text-dim);
  }
  /* Fewer agents in the zone than asked for: the spec is not being met, and the
     count above is the real one rather than the one that was typed. */
  .effect-zone.short {
    border-color: var(--amber);
    color: var(--amber);
  }

  .eval-block {
    margin-top: 0.9rem;
    border-top: 1px solid var(--border);
    padding-top: 0.7rem;
  }
  .eval-block textarea {
    resize: vertical;
    line-height: 1.45;
    margin-top: 0.3rem;
  }
  .eval-block code {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--text);
  }
  .eval-block .field-note {
    display: block;
    margin: 0.4rem 0 0;
    line-height: 1.5;
  }

  .try-row {
    display: flex;
    align-items: flex-end;
    gap: 0.6rem;
    margin-top: 0.7rem;
  }
  .try-num {
    width: 7rem;
    flex: none;
  }
  .try-body {
    flex: 1 1 auto;
  }

  /* A wrong verdict here is the difference between a page that alerts and one
     that lies, so it is stated at size and in colour, not as a footnote. */
  .verdict {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin: 0.6rem 0 0;
  }
  .verdict-pill {
    font-size: 0.9rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    padding: 0.15rem 0.7rem;
    border-radius: 6px;
  }
  .verdict-pill.up {
    background: rgba(63, 185, 80, 0.16);
    border: 1px solid var(--up);
    color: var(--up);
  }
  .verdict-pill.down {
    background: rgba(248, 81, 73, 0.16);
    border: 1px solid var(--down);
    color: var(--down);
  }

  .log-row {
    display: grid;
    grid-template-columns: 7rem auto 5rem 4.5rem minmax(0, 1fr);
    gap: 0.6rem;
    align-items: center;
    font-size: 0.8rem;
    padding: 0.3rem 0;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  .log-row:last-child {
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
  /* Dim, beside the id rather than under it: the warning belongs to the row you
     are ticking. */
  .stale {
    font-size: 0.68rem;
  }
</style>
