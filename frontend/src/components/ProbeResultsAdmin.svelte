<script lang="ts">
  import { onMount } from 'svelte';
  import type { ProbeStatus } from '../lib/admin';
  import {
    AdminError,
    fetchAdminChecks,
    fetchProbeResults,
    type ProbeResult,
  } from '../lib/admin';
  import { formatLatency, formatRelativeTime } from '../lib/format';
  import { formatAbsolute } from '../lib/incident';

  // `onopen` jumps to the check editor, like AgentsAdmin: a row names a check,
  // and the operator reading a failure often wants to go edit it. Takes the
  // check's immutable id, which `ProbeResult.check_id` already is.
  // `onexpired` drives the same re-login flow every panel uses.
  let {
    onopen,
    onexpired,
  }: { onopen: (id: string) => void; onexpired: () => void } = $props();

  /** How many rows to ask the server for. The server clamps to 1000. */
  const LIMITS = [100, 200, 500] as const;

  let results = $state<ProbeResult[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  // A filter request can finish after a later one. Only the newest request may
  // update the view (including its loading/error state).
  let latestLoad = 0;

  // Server-side filters: a change to any of these re-fetches. `check` is a
  // check id \u2014 the server rejects anything that does not parse as one.
  let check = $state('');
  let agent = $state('');
  let status = $state<'' | ProbeStatus>('');
  let region = $state('');
  let zone = $state('');
  let limit = $state<number>(200);

  // Client-side free-text search over the returned batch — never a param, so a
  // keystroke only re-filters the list in memory.
  let query = $state('');

  /** Every configured check's id and name, fetched once on mount so the
   *  dropdown is complete even for a check that has no recent results. Falls
   *  back to the distinct ids in the batch if this call fails. */
  let allChecks = $state<{ id: string; name: string }[]>([]);

  /** Fetch with the current server-side filters. Called on mount and whenever a
   *  server-side filter changes; typing in `query` does NOT call this. */
  async function load() {
    const request = ++latestLoad;
    loading = true;
    loadError = null;
    try {
      const nextResults = await fetchProbeResults({
        check: check || undefined,
        agent: agent || undefined,
        status: status || undefined,
        region: region || undefined,
        zone: zone || undefined,
        limit,
      });
      if (request === latestLoad) results = nextResults;
    } catch (e) {
      if (request !== latestLoad) return;
      if (e instanceof AdminError && e.unauthorized) {
        onexpired();
        return;
      }
      loadError = e instanceof Error ? e.message : String(e);
    } finally {
      if (request === latestLoad) loading = false;
    }
  }

  onMount(async () => {
    try {
      allChecks = (await fetchAdminChecks())
        .map((c) => ({ id: c.id, name: c.name }))
        .sort((a, b) => a.name.localeCompare(b.name));
    } catch (e) {
      if (e instanceof AdminError && e.unauthorized) {
        onexpired();
        return;
      }
      // A failed check list is not fatal: the dropdown falls back to the batch.
    }
    await load();
  });

  // Distinct values from the current batch, for the agent/region/zone pickers.
  // The check picker prefers the full configured list so it is complete.
  const distinct = (pick: (r: ProbeResult) => string | undefined): string[] =>
    [...new Set(results.map(pick).filter((v): v is string => !!v))].sort();

  const checkOptions = $derived(
    allChecks.length
      ? allChecks
      : [...new Map(results.map((r) => [r.check_id, r.name])).entries()]
          .map(([id, name]) => ({ id, name }))
          .sort((a, b) => a.name.localeCompare(b.name)),
  );
  const agentOptions = $derived(distinct((r) => r.agent_id));
  const regionOptions = $derived(distinct((r) => r.region));
  const zoneOptions = $derived(distinct((r) => r.zone));

  /** The free-text search: a substring match across the fields an operator
   *  types into it — the raw error, the check's name, and the agent. */
  const visible = $derived.by(() => {
    const q = query.trim().toLowerCase();
    if (!q) return results;
    return results.filter((r) =>
      [r.error, r.name, r.agent_id].some((f) => f?.toLowerCase().includes(q)),
    );
  });

  const filtersActive = $derived(
    !!(check || agent || status || region || zone || query),
  );

  function clearFilters() {
    const wasServer = !!(check || agent || status || region || zone);
    check = '';
    agent = '';
    status = '';
    region = '';
    zone = '';
    query = '';
    if (wasServer) load();
  }

  /** Severity tone from the result's status, matching AlertsList's palette. */
  function tone(s: ProbeStatus): { pill: string; bar: string; label: string } {
    if (s === 'down') return { pill: 'red', bar: 'bad', label: 'down' };
    if (s === 'unknown') return { pill: 'amber', bar: 'warn', label: 'unknown' };
    return { pill: 'green', bar: 'ok', label: 'up' };
  }

  /** Region + zone as one line, honest about the gaps: an agent that never
   *  declared a location reads as unknown rather than an invented placeholder. */
  function location(r: ProbeResult): string {
    if (r.region && r.zone) return `${r.region} · ${r.zone}`;
    return r.region ?? r.zone ?? 'unknown location';
  }
</script>

<!-- `.admin-panel` is what activates the shared input/select styling (scoped to
     that class in app.css), so the controls match every other admin page. -->
<div class="admin-panel probe-workspace">
  <p class="admin-muted lead">
    Inspect individual probe attempts, newest first. Filter by monitor, location, or outcome to
    investigate failures, including full error details omitted from the public status page.
  </p>

  <div class="probe-snapshot" aria-label="Filtered result summary">
    <div><span>Visible attempts</span><strong>{visible.length}</strong><small>Current filtered batch</small></div>
    <div><span>Successful</span><strong class="snapshot-up">{visible.filter((r) => r.status === 'up').length}</strong><small>Probe returned up</small></div>
    <div><span>Failed</span><strong class:snapshot-down={visible.some((r) => r.status === 'down')}>{visible.filter((r) => r.status === 'down').length}</strong><small>{visible.filter((r) => r.status === 'unknown').length} unknown outcomes</small></div>
    <div><span>Reporting sources</span><strong>{new Set(visible.map((r) => r.agent_id)).size}</strong><small>Distinct agents in this batch</small></div>
  </div>
  <div class="controls">
  <label class="pick">
    <span class="pick-label">Check</span>
    <select bind:value={check} onchange={load}>
      <option value="">All checks</option>
      {#each checkOptions as c (c.id)}
        <option value={c.id}>{c.name}</option>
      {/each}
    </select>
  </label>
  <label class="pick">
    <span class="pick-label">Agent</span>
    <select bind:value={agent} onchange={load}>
      <option value="">All agents</option>
      {#each agentOptions as a (a)}
        <option value={a}>{a}</option>
      {/each}
    </select>
  </label>
  <div class="range-switch" role="group" aria-label="Status filter">
    {#each [['', 'All'], ['up', 'Up'], ['down', 'Down'], ['unknown', 'Unknown']] as [val, label] (val)}
      <button
        class="range-btn"
        class:active={status === val}
        onclick={() => {
          status = val as '' | ProbeStatus;
          load();
        }}>{label}</button
      >
    {/each}
  </div>
  <label class="pick">
    <span class="pick-label">Region</span>
    <select bind:value={region} onchange={load}>
      <option value="">All regions</option>
      {#each regionOptions as r (r)}
        <option value={r}>{r}</option>
      {/each}
    </select>
  </label>
  <label class="pick">
    <span class="pick-label">Zone</span>
    <select bind:value={zone} onchange={load}>
      <option value="">All zones</option>
      {#each zoneOptions as z (z)}
        <option value={z}>{z}</option>
      {/each}
    </select>
  </label>
  <input
    class="search"
    type="search"
    bind:value={query}
    placeholder="Search error, check or agent"
    aria-label="Search results by error text, check or agent"
  />
  <label class="pick">
    <span class="pick-label">Rows</span>
    <select bind:value={limit} onchange={load}>
      {#each LIMITS as n (n)}
        <option value={n}>{n}</option>
      {/each}
    </select>
  </label>
  <button class="btn" onclick={load} disabled={loading}>Refresh</button>
</div>

<div class="count-line">
  <span class="count">{visible.length} of {results.length} results</span>
  {#if filtersActive}
    <button class="btn" onclick={clearFilters}>Clear filters</button>
  {/if}
</div>

{#if loadError}
  <div class="notice error">
    Failed to load results: {loadError}
    <div style="margin-top:0.5rem"><button class="link" onclick={load}>Retry</button></div>
  </div>
{:else if loading && results.length === 0}
  <div class="otable"><div class="ot-empty">Loading…</div></div>
{:else if results.length === 0}
  <div class="otable"><div class="ot-empty">No probe results in range.</div></div>
{:else if visible.length === 0}
  <div class="otable"><div class="ot-empty">No results match these filters.</div></div>
{:else}
  <div class="probe-ledger">
    <div class="probe-heading"><span>Monitor / agent</span><span>Outcome</span><span>Location</span><span>Latency</span><span>Observed</span></div>
    {#each visible as r (`${r.check_id}-${r.agent_id}-${r.timestamp}`)}
      {@const t = tone(r.status)}
      <article class="probe-entry">
        <div class="probe-line">
          <div class="probe-identity"><button onclick={() => onopen(r.check_id)} title="Edit this check">{r.name}</button><span>{r.agent_id}</span></div>
          <div><span class="opill {t.pill}">{t.label}</span>{#if r.status_code != null}<span class="probe-secondary">HTTP {r.status_code}</span>{/if}</div>
          <span class="probe-place">{location(r)}</span>
          <span class="probe-latency">{formatLatency(r.latency_ms)}</span>
          <time class="probe-time" datetime={new Date(r.timestamp).toISOString()} title={formatAbsolute(r.timestamp)}>{formatRelativeTime(r.timestamp)}</time>
        </div>
        {#if r.error}
          <details class="probe-error"><summary>{r.error}</summary><pre>{r.error}</pre></details>
        {/if}
      </article>
    {/each}
  </div>
{/if}
</div>

<style>
  .probe-snapshot { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin: 24px 0; }
  .probe-snapshot > div { display: grid; gap: 10px; padding: 20px; background: var(--ad-panel); border: 1px solid var(--ad-line); border-radius: 8px; }
  .probe-snapshot span, .probe-snapshot small { color: var(--ad-dim); font-size: 12px; }
  .probe-snapshot strong { font-size: 28px; font-weight: 500; }
  .snapshot-up { color: var(--ad-green); } .snapshot-down { color: var(--ad-red); }
  @media (max-width: 700px) { .probe-snapshot { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; } .probe-snapshot > div { padding: 16px; } }

  .lead {
    margin: 0 0 1rem;
  }
  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: 16px;
    margin-bottom: 24px;
  }
  .pick {
    display: flex;
    flex-direction: column;
    gap: 8px;
    flex: 1 1 9rem;
    min-width: 0;
  }
  .pick-label {
    font-size: 12px;
    text-transform: none;
    letter-spacing: 0;
    color: var(--ad-faint);
  }
  /* The global `.admin-panel input/select` rule sets width:100%; in this flex
     row each control should size to its own content instead. */
  .pick select {
    width: 100%;
    min-width: 0;
    max-width: 24rem;
  }
  .search {
    width: 16rem;
    max-width: 100%;
    align-self: flex-end;
  }
  .count-line {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
  }
  .count {
    font-size: 0.8rem;
    color: var(--ad-dim);
  }
  .probe-ledger { border: 1px solid var(--ad-line); border-radius: 8px; overflow: hidden; }
  .probe-heading, .probe-line { display: grid; grid-template-columns: minmax(0, 1.6fr) minmax(90px, .65fr) minmax(0, 1fr) minmax(70px, .5fr) minmax(90px, .7fr); align-items: center; gap: 20px; padding: 16px 20px; }
  .probe-heading { color: var(--ad-dim); font-size: 11px; background: var(--ad-panel-2); }
  .probe-entry { border-top: 1px solid var(--ad-line-soft); }
  .probe-entry:hover { background: var(--ad-panel-2); }
  .probe-line { font-size: 13px; }
  .probe-identity { min-width: 0; display: grid; gap: 6px; }
  .probe-identity button { padding: 0; background: none; border: 0; color: var(--ad-text); font: inherit; text-align: left; cursor: pointer; overflow-wrap: anywhere; }
  .probe-identity button:hover { color: var(--ad-cyan); }
  .probe-identity > span, .probe-secondary { color: var(--ad-dim); font-size: 11px; overflow-wrap: anywhere; }
  .probe-secondary { display: block; margin-top: 6px; }
  .probe-place, .probe-time { color: var(--ad-dim); font-size: 12px; overflow-wrap: anywhere; }
  .probe-latency { font-variant-numeric: tabular-nums; }
  .probe-error { padding: 0 20px 16px; color: var(--ad-red); font-size: 12px; }
  .probe-error summary { cursor: pointer; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .probe-error pre { white-space: pre-wrap; overflow-wrap: anywhere; padding: 16px; background: var(--ad-bg); border-radius: 6px; line-height: 1.6; }
  @media (max-width: 900px) { .probe-heading { display: none; } .probe-line { grid-template-columns: minmax(0, 1fr) auto; gap: 12px; } .probe-place { grid-column: 1; } .probe-time { grid-column: 1 / -1; } }
</style>
