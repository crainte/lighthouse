<script lang="ts">
  import { UNKNOWN_LOCATION } from '../../lib/api';
  import type { Draft } from './draft';
  import NumberField from '../NumberField.svelte';

  let {
    draft,
    pinned,
    effect,
    zoneOptions,
    regionOptions,
    agentOptions,
    reporting,
  }: {
    draft: Draft;
    pinned: boolean;
    effect: { domains: { domain: string; probes: number; available: number }[]; total: number; count: number };
    zoneOptions: string[];
    regionOptions: string[];
    agentOptions: string[];
    reporting: Set<string>;
  } = $props();

  /** Free-text buffer per axis, so a zone can be named before any agent in it
   *  has ever reported. Keyed by axis because the picker below is one snippet
   *  rendered twice. */
  let adding = $state<{ zones: string; regions: string }>({ zones: '', regions: '' });

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
</script>

<p class="field-note lede">
  Where this check runs, described by location rather than by machine, so growing the fleet does
  not mean revisiting every check. Choose whether the count applies once per zone or once per
  region; leave both lists empty to include every location.
</p>

{#if pinned}
  <p class="field-note override">
    This check is pinned to {draft.agents.length}
    {draft.agents.length === 1 ? 'agent' : 'agents'} below, and an explicit pin wins outright —
    the regions and zones here are ignored entirely while that list is non-empty. Clear it to
    place this check by location instead.
  </p>
{/if}

<div class="placement-axes">
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
        Offered {noun}s come from the live fleet; add one by hand for a {noun} whose agents have
        not booted yet.
      </span>
    </div>
  {/snippet}

  {@render locations('regions', 'region', regionOptions)}
  {@render locations('zones', 'zone', zoneOptions)}
  <label class="field">
    <span class="field-label">Placement mode</span>
    <select bind:value={draft.placement.mode}>
      <option value="zone">Per zone</option>
      <option value="region">Per region</option>
    </select>
    <span class="field-note">The selected domain gets its own independent observer count.</span>
  </label>
  <label class="field">
    <span class="field-label">Agents per {draft.placement.mode}</span>
    <NumberField min={1} bind:value={draft.placement.count} />
    <span class="field-note">
      How many agents inside <em>each</em> matched {draft.placement.mode} probe this — not a
      fleet-wide total. Raising it buys corroboration at the cost of duplicate probes.
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
    No live agent matches this placement, so nothing probes this check. It will report nothing
    rather than fail — silence, not an error.
  </p>
{:else}
  <p class="effect">
    Right now this places <strong>{effect.total}</strong>
    {effect.total === 1 ? 'probe' : 'probes'} across {effect.domains.length}
    {draft.placement.mode}{effect.domains.length === 1 ? '' : 's'}:
    {#each effect.domains as domain (domain.domain)}
      <span class="effect-zone" class:short={domain.available < effect.count}>
        {domain.domain} ×{domain.probes}{domain.available < effect.count ? ` (only ${domain.available} live)` : ''}
      </span>
    {/each}
  </p>
{/if}

<div class="field pin-block">
  <span class="field-label">Pin to agents</span>
  {#if agentOptions.length === 0}
    <span class="field-note">
      No agent has reported yet. The placement above decides who probes this check once some do.
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
      None ticked is the normal case: the placement above decides who probes this check. Ticking
      agents pins it to exactly those and overrides placement entirely — for a probe that
      genuinely belongs to one box, such as a machine's own loopback port.
    </span>
  {/if}
</div>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .lede {
    display: block;
    margin: 0 0 0.8rem;
    line-height: 1.5;
    max-width: 68ch;
  }
  .agent-picker {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 0.9rem;
  }
  .pair-row {
    display: flex;
    gap: 0.4rem;
    align-items: center;
  }
  .pin-block {
    margin-top: 0.9rem;
    padding-top: 0.8rem;
    border-top: 1px solid var(--border);
  }
  .placement-axes {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(15rem, 1fr));
    gap: 0.7rem 1rem;
    align-items: start;
  }
  .override {
    display: block;
    color: var(--amber);
    line-height: 1.5;
    margin: 0 0 0.8rem;
    max-width: 68ch;
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
  .stale {
    font-size: 0.68rem;
  }
</style>
