<script lang="ts">
  import type { AgentsDisplay, AgentsMode, FleetEntry, FleetMember } from '../lib/admin';
  import { AdminError, fetchAgentsDisplay, saveAgentsDisplay } from '../lib/admin';
  import NumberField from './NumberField.svelte';

  // `onchanged` refreshes the public page behind the admin view, so a change is
  // visible without a reload.
  let { onchanged, onexpired }: { onchanged: () => void; onexpired: () => void } = $props();

  const MODES: { id: AgentsMode; label: string; hint: string }[] = [
    {
      id: 'regions',
      label: 'As region rows',
      hint: 'Anything not placed above is folded into a row for its region. No agent is named.',
    },
    {
      id: 'flat',
      label: 'One row each',
      hint: 'Anything not placed above gets its own row. This publishes your agent ids.',
    },
    {
      id: 'off',
      label: 'Not shown',
      hint: 'Only the rows above are published. With no rows, the section disappears entirely.',
    },
  ];

  /** How many candidates the picker mounts before asking for a narrower search.
   *  A fleet is hundreds of agents across tens of regions, and a wall of every
   *  one of them is not a picker. */
  const SUGGESTIONS = 8;

  let fleet = $state<FleetMember[]>([]);
  let form = $state<AgentsDisplay | null>(null);
  /** What the server last confirmed, so Save can tell edited from untouched. */
  let saved = $state<string>('');
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let saveError = $state<string | null>(null);
  let saving = $state(false);
  let expired = $state(false);
  let query = $state('');

  /** Native HTML5 drag carries no usable payload between elements, so the
   *  source index lives here. Same shape the sections list above uses. */
  let drag = $state<{ from: number; over: number | null } | null>(null);

  const dirty = $derived(form !== null && JSON.stringify(form) !== saved);

  /** Regions the fleet actually has, deduped and sorted. */
  const regions = $derived([...new Set(fleet.map((m) => m.region))].sort());

  const placed = $derived(new Set((form?.rows ?? []).map((r) => `${r.kind}:${r.name}`)));

  /** Candidates for the picker: everything not already placed, narrowed by the
   *  search box. Counts are kept so the UI can say how much it is hiding. */
  const matches = $derived.by(() => {
    const q = query.trim().toLowerCase();
    const hit = (s: string) => q === '' || s.toLowerCase().includes(q);
    const r = regions.filter((x) => !placed.has(`region:${x}`) && hit(x));
    const a = fleet
      .map((m) => m.agent_id)
      .filter((x) => !placed.has(`agent:${x}`) && hit(x));
    return {
      regions: r.slice(0, SUGGESTIONS),
      agents: a.slice(0, SUGGESTIONS),
      hiddenRegions: Math.max(0, r.length - SUGGESTIONS),
      hiddenAgents: Math.max(0, a.length - SUGGESTIONS),
      empty: r.length === 0 && a.length === 0,
    };
  });

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) expired = true;
    return e instanceof Error ? e.message : String(e);
  }

  function adopt(s: { agents: AgentsDisplay; fleet: FleetMember[] }) {
    form = s.agents;
    fleet = s.fleet;
    saved = JSON.stringify(s.agents);
  }

  async function refresh() {
    loading = true;
    loadError = null;
    try {
      adopt(await fetchAgentsDisplay());
    } catch (e) {
      loadError = fail(e);
    } finally {
      loading = false;
    }
  }

  refresh();

  function add(entry: FleetEntry) {
    if (!form) return;
    form.rows = [...form.rows, entry];
  }

  function removeAt(i: number) {
    if (!form) return;
    form.rows = form.rows.filter((_, n) => n !== i);
  }

  function reorder(from: number, to: number) {
    if (!form) return;
    const next = [...form.rows];
    const [moved] = next.splice(from, 1);
    next.splice(to, 0, moved);
    form.rows = next;
  }

  /** Arrow buttons are the keyboard path — a drag grip has none. */
  function move(i: number, by: number) {
    const to = i + by;
    if (!form || to < 0 || to >= form.rows.length) return;
    reorder(i, to);
  }

  function startDrag(e: DragEvent, from: number) {
    // Firefox refuses to start a drag unless some data is attached.
    e.dataTransfer?.setData('text/plain', String(from));
    if (e.dataTransfer) e.dataTransfer.effectAllowed = 'move';
    drag = { from, over: null };
  }

  /** Only calling preventDefault marks a target droppable. */
  function dragOver(e: DragEvent, index: number) {
    if (!drag || drag.from === index) return;
    e.preventDefault();
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
    drag.over = index;
  }

  /** Which side of the hovered row the dragged one lands on. */
  function dropEdge(index: number): 'above' | 'below' | null {
    if (!drag || drag.over !== index) return null;
    return drag.from < index ? 'below' : 'above';
  }

  function drop(index: number) {
    if (drag) reorder(drag.from, index);
    drag = null;
  }

  /** Whether a placed row still corresponds to something the fleet has. A
   *  decommissioned agent is not removed automatically — it renders as down on
   *  the public page, which is the honest answer — but the editor says so. */
  function known(entry: FleetEntry): boolean {
    return entry.kind === 'agent'
      ? fleet.some((m) => m.agent_id === entry.name)
      : regions.includes(entry.name);
  }

  async function save() {
    if (!form || saving) return;
    saving = true;
    saveError = null;
    try {
      adopt(await saveAgentsDisplay(form));
      onchanged();
    } catch (e) {
      saveError = fail(e);
    } finally {
      saving = false;
    }
  }
</script>

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{loadError ?? saveError ?? 'Session expired.'}</p>
    <p class="admin-muted">
      Your session expired. Sign in again to change what the status page shows.
    </p>
    <div class="range-switch retry">
      <button class="btn" onclick={onexpired}>Sign in again</button>
    </div>
  </div>
{:else if loading}
  <div class="admin-panel"><p class="admin-muted">Loading…</p></div>
{:else if loadError || !form}
  <div class="admin-panel">
    <p class="admin-error">{loadError}</p>
    <div class="range-switch retry">
      <button class="btn" onclick={refresh}>Retry</button>
    </div>
  </div>
{:else}
  <div class="admin-panel">
    <div class="panel-title">Monitoring agents</div>
    <p class="admin-muted lead">
      Whether the status page reports on the monitoring fleet itself, and in how much
      detail. This is liveness, not check health: an agent that answers is healthy even
      when every check it probes is down.
    </p>

    <div class="picker-head">
      <span class="flabel">Rows, in order</span>
      <span class="admin-muted">
        Place a single agent or a whole region, and drag to reorder. A region row counts
        every agent in it, including one you also show on its own.
      </span>
    </div>

    {#if form.rows.length === 0}
      <p class="admin-muted">
        No rows placed. The page shows whatever the setting below produces — which for a
        large fleet is usually what you want, with a handful of rows placed here on top.
      </p>
    {:else}
      <div class="rows" role="list">
        {#each form.rows as entry, i (entry.kind + ':' + entry.name)}
          <div
            class="rrow"
            role="listitem"
            class:dragging={drag?.from === i}
            class:drop-above={dropEdge(i) === 'above'}
            class:drop-below={dropEdge(i) === 'below'}
            ondragover={(e) => dragOver(e, i)}
            ondrop={(e) => (e.preventDefault(), drop(i))}
          >
            <span
              class="grip"
              draggable="true"
              aria-hidden="true"
              title="Drag to reorder"
              ondragstart={(e) => startDrag(e, i)}
              ondragend={() => (drag = null)}>⠿</span
            >
            <div class="movers">
              <button
                class="reorder-step"
                onclick={() => move(i, -1)}
                disabled={i === 0}
                aria-label="Move {entry.name} up"><svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 14 6-6 6 6" /></svg></button
              >
              <button
                class="reorder-step"
                onclick={() => move(i, 1)}
                disabled={i === form.rows.length - 1}
                aria-label="Move {entry.name} down"><svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 10 6 6 6-6" /></svg></button
              >
            </div>
            <span class="rkind {entry.kind}">{entry.kind}</span>
            <span class="rname">{entry.name}</span>
            {#if known(entry)}
              <span></span>
            {:else}
              <span class="unknown">not in the fleet — publishes as down</span>
            {/if}
            <button class="btn danger" onclick={() => removeAt(i)}>Remove</button>
          </div>
        {/each}
      </div>
    {/if}

    <div class="add">
      <input
        class="search"
        placeholder="Search agents and regions to add…"
        bind:value={query}
        aria-label="Search agents and regions"
      />
      {#if fleet.length === 0}
        <span class="admin-muted">
          No agents known yet. One appears here as soon as it polls for work.
        </span>
      {:else if matches.empty}
        <span class="admin-muted">
          {query.trim() ? 'Nothing matches.' : 'Everything is already placed.'}
        </span>
      {/if}
    </div>

    {#if matches.regions.length}
      <div class="chips">
        <span class="flabel">Regions</span>
        {#each matches.regions as r (r)}
          <button class="chip region" onclick={() => add({ kind: 'region', name: r })}>+ {r}</button>
        {/each}
        {#if matches.hiddenRegions}
          <span class="admin-muted more">+{matches.hiddenRegions} more — narrow the search</span>
        {/if}
      </div>
    {/if}
    {#if matches.agents.length}
      <div class="chips">
        <span class="flabel">Agents</span>
        {#each matches.agents as a (a)}
          <button class="chip" onclick={() => add({ kind: 'agent', name: a })}>+ {a}</button>
        {/each}
        {#if matches.hiddenAgents}
          <span class="admin-muted more">+{matches.hiddenAgents} more — narrow the search</span>
        {/if}
      </div>
    {/if}

    <div class="picker-head everything">
      <span class="flabel">Everything else</span>
    </div>
    <div class="range-switch">
      {#each MODES as m (m.id)}
        <button
          class="range-btn"
          class:active={form.mode === m.id}
          onclick={() => form && (form.mode = m.id)}
        >
          {m.label}
        </button>
      {/each}
    </div>
    <p class="admin-muted hint">{MODES.find((m) => m.id === form?.mode)?.hint}</p>

    <div class="field-row">
      <label class="field">
        <span class="flabel">Amber below</span>
        <NumberField min={0} max={100} step={1} bind:value={form.warn_below} />
      </label>
      <label class="field">
        <span class="flabel">Red below</span>
        <NumberField min={0} max={100} step={1} bind:value={form.down_below} />
      </label>
      <p class="admin-muted thresholds">
        Percent of a region row's agents that must be healthy. At the defaults one silent
        agent turns its region amber, and a region with no healthy agent reads red. Agent
        rows show their own state and ignore these.
      </p>
    </div>

    {#if saveError}<p class="admin-error save-error">{saveError}</p>{/if}
    <div class="actions">
      <button class="btn primary" onclick={save} disabled={!dirty || saving}>
        {saving ? 'Saving…' : 'Save'}
      </button>
      {#if dirty}<span class="admin-muted">Unsaved changes</span>{/if}
    </div>
  </div>
{/if}

<style>
  .lead {
    margin: -0.35rem 0 0.9rem;
    line-height: 1.5;
  }
  .retry {
    margin-top: 0.7rem;
  }
  .hint {
    margin: 0.45rem 0 1rem;
  }
  .picker-head {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    flex-wrap: wrap;
    margin-bottom: 0.4rem;
  }
  .picker-head.everything {
    margin-top: 1.2rem;
  }
  .flabel {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }
  .rows {
    border-top: 1px solid var(--border);
  }
  .rrow {
    display: grid;
    grid-template-columns: auto auto 4.4rem minmax(8rem, max-content) 1fr auto;
    gap: 0.5rem;
    align-items: center;
    padding: 0.34rem 0;
    font-size: 0.85rem;
    border-bottom: 1px solid var(--border);
  }
  /* Grab affordance for the native drag. Not focusable on purpose: the arrow
     buttons are the accessible path, so a screen reader gains nothing from
     announcing a control it cannot operate. */
  .grip {
    cursor: grab;
    color: var(--text-dim);
    line-height: 1;
    padding: 0 0.1rem;
    user-select: none;
  }
  .grip:active {
    cursor: grabbing;
  }
  .rrow.dragging {
    opacity: 0.45;
  }
  /* Insertion line on the side the dragged row will land. Inset shadow rather
     than a border, so the row never shifts by a pixel mid-drag. */
  .rrow.drop-above {
    box-shadow: inset 0 2px 0 0 var(--accent);
  }
  .rrow.drop-below {
    box-shadow: inset 0 -2px 0 0 var(--accent);
  }
  .movers {
    display: inline-flex;
    gap: 0.15rem;
  }
  .reorder-step {
    padding: 0.05rem 0.3rem;
    font-size: 0.62rem;
    line-height: 1.4;
  }
  .rkind {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }
  .rkind.region {
    color: var(--accent);
  }
  .rname {
    font-family: var(--mono, ui-monospace, monospace);
  }
  .unknown {
    font-size: 0.76rem;
    color: var(--amber);
  }
  .add {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
    margin-top: 0.8rem;
  }
  .search {
    max-width: 20rem;
  }
  .chips {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    flex-wrap: wrap;
    margin-top: 0.45rem;
  }
  .chip {
    background: none;
    border: 1px dashed var(--border);
    border-radius: 999px;
    color: var(--text-dim);
    font-size: 0.78rem;
    padding: 0.15rem 0.6rem;
    cursor: pointer;
  }
  .chip:hover {
    border-style: solid;
    color: var(--accent);
    border-color: var(--accent);
  }
  .chip.region {
    font-weight: 600;
  }
  .more {
    font-size: 0.76rem;
  }
  .field-row {
    display: flex;
    align-items: flex-end;
    gap: 0.7rem;
    flex-wrap: wrap;
    margin-top: 1rem;
  }
  .field {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }
  /* :global because the control is now a component; the width belongs to the
     slot it sits in, not to the component. */
  .field :global(.num) {
    width: 6rem;
  }
  .thresholds {
    flex: 1 1 16rem;
    line-height: 1.45;
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    margin-top: 1rem;
  }
  .save-error {
    margin-top: 0.8rem;
  }
</style>
