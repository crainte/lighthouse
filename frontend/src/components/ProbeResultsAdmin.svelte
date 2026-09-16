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
    loading = true;
    loadError = null;
    try {
      results = await fetchProbeResults({
        check: check || undefined,
        agent: agent || undefined,
        status: status || undefined,
        region: region || undefined,
        zone: zone || undefined,
        limit,
      });
    } catch (e) {
      if (e instanceof AdminError && e.unauthorized) {
        onexpired();
        return;
      }
      loadError = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
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
<div class="admin-panel">
  <p class="admin-muted lead">
    The raw results agents reported, newest first — verbatim, including the error
    text the public status page deliberately summarises away. This is where you
    answer "what did the agents actually see?".
  </p>

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
  <ul class="result-feed">
    {#each visible as r (r.check_id + r.agent_id + r.timestamp)}
      {@const t = tone(r.status)}
      <li class="result-card {t.bar}">
        <div class="rc-top">
          <span class="opill {t.pill}">{t.label}</span>
          <button class="rc-check" onclick={() => onopen(r.check_id)} title="Edit this check">
            {r.name}
          </button>
          <span class="rc-agent">{r.agent_id}</span>
          <span class="rc-when field-note" title={formatAbsolute(r.timestamp)}>
            {formatRelativeTime(r.timestamp)}
          </span>
        </div>
        <div class="rc-meta">
          <span class="opill grey">{location(r)}</span>
          <span class="field-note">{formatLatency(r.latency_ms)}</span>
          {#if r.status_code != null}
            <span class="field-note">HTTP {r.status_code}</span>
          {/if}
        </div>
        {#if r.error}
          <!-- The raw probe error, verbatim: the ddns-style line an operator
               comes here to read. Monospace and wrapping so a long DNS answer
               stays legible rather than truncating. -->
          <pre class="rc-error">{r.error}</pre>
        {/if}
      </li>
    {/each}
  </ul>
{/if}
</div>

<style>
  .lead {
    margin: 0 0 1rem;
  }
  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: 0.6rem 0.9rem;
    margin-bottom: 0.8rem;
  }
  .pick {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }
  .pick-label {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--ad-faint);
  }
  /* The global `.admin-panel input/select` rule sets width:100%; in this flex
     row each control should size to its own content instead. */
  .pick select {
    width: auto;
    min-width: 9rem;
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
  .result-feed {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }
  /* Left bar carries severity, so the feed scans by colour before it is read. */
  .result-card {
    background: var(--ad-panel);
    border: 1px solid var(--ad-line);
    border-left: 3px solid var(--ad-line);
    border-radius: 8px;
    padding: 0.7rem 0.9rem 0.75rem;
  }
  .result-card.bad {
    border-left-color: var(--ad-red);
  }
  .result-card.warn {
    border-left-color: var(--ad-amber);
  }
  .result-card.ok {
    border-left-color: var(--ad-green);
  }
  .rc-top {
    display: flex;
    align-items: center;
    gap: 0.55rem;
  }
  .rc-check {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.9rem;
    color: var(--ad-cyan);
    background: none;
    border: none;
    padding: 0;
    cursor: pointer;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .rc-check:hover {
    text-decoration: underline;
  }
  .rc-agent {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.8rem;
    color: var(--ad-dim);
  }
  .rc-when {
    margin-left: auto;
  }
  .rc-meta {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
    margin-top: 0.45rem;
  }
  .rc-error {
    margin: 0.55rem 0 0;
    padding: 0.5rem 0.6rem;
    background: var(--ad-bg, rgba(0, 0, 0, 0.2));
    border-radius: 6px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.78rem;
    color: var(--ad-dim);
    white-space: pre-wrap;
    word-break: break-word;
  }
</style>
