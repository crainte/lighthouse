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

  // Pinning is an override, not another placement filter. The controls share
  // one draft, so hiding a strategy must also remove its effect on the payload.
  // Preserve choices locally for a switch-back before the form is saved.
  let strategy = $state<'automatic' | 'pinned'>(pinned ? 'pinned' : 'automatic');
  let previousPins = [...draft.agents];

  let scope = $state<'all' | 'selected'>(
    draft.placement.regions.length || draft.placement.zones.length ? 'selected' : 'all',
  );
  let previousFilters = {
    regions: [...draft.placement.regions],
    zones: [...draft.placement.zones],
  };
  let adding = $state<{ zones: string; regions: string }>({ zones: '', regions: '' });
  const livePinned = $derived(draft.agents.filter((id) => reporting.has(id)).length);

  function changeStrategy(next: 'automatic' | 'pinned') {
    if (next === strategy) return;
    if (next === 'automatic') {
      previousPins = [...draft.agents];
      draft.agents = [];
    } else {
      draft.agents = [...previousPins];
    }
    strategy = next;
  }

  function changeScope(next: 'all' | 'selected') {
    if (next === scope) return;
    if (next === 'all') {
      previousFilters = {
        regions: [...draft.placement.regions],
        zones: [...draft.placement.zones],
      };
      draft.placement.regions = [];
      draft.placement.zones = [];
    } else {
      draft.placement.regions = [...previousFilters.regions];
      draft.placement.zones = [...previousFilters.zones];
    }
    scope = next;
  }

  // Locations need not have booted an agent yet. An absent zone must remain an
  // editable value rather than disappearing from the picker on reload.
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

<div class="placement-editor">
  <p class="field-note intro">Choose how Lightkeeper distributes this check. Most checks can use the default location-aware placement.</p>

  <fieldset class="strategy-fieldset">
    <legend class="field-label">Placement strategy</legend>
    <div class="strategy-choices">
      <label class="strategy-option" class:active={strategy === 'automatic'}>
        <input type="radio" name="placement-strategy" checked={strategy === 'automatic'} onchange={() => changeStrategy('automatic')} />
        <span class="option-copy">
          <strong>Automatic</strong>
          <small>Choose agents by location. Adapts as your fleet changes.</small>
        </span>
      </label>
      <label class="strategy-option" class:active={strategy === 'pinned'}>
        <input type="radio" name="placement-strategy" checked={strategy === 'pinned'} onchange={() => changeStrategy('pinned')} />
        <span class="option-copy">
          <strong>Specific agents</strong>
          <small>Run only on explicitly selected agents.</small>
        </span>
      </label>
    </div>
  </fieldset>

  {#if strategy === 'automatic'}
    <div class="placement-settings">
      <fieldset class="scope-fieldset">
        <legend class="field-label">Where should this check run?</legend>
        <div class="scope-choices">
          <label class:active={scope === 'all'}><input type="radio" name="placement-scope" checked={scope === 'all'} onchange={() => changeScope('all')} /> All locations</label>
          <label class:active={scope === 'selected'}><input type="radio" name="placement-scope" checked={scope === 'selected'} onchange={() => changeScope('selected')} /> Selected locations</label>
        </div>
        {#if scope === 'all'}
          <p class="field-note">Every region and zone is eligible, including locations added later.</p>
        {/if}
      </fieldset>

      {#if scope === 'selected'}
        <div class="location-filters">
          {#snippet locations(axis: 'zones' | 'regions', noun: string, options: string[])}
            <section class="location-axis" aria-label={'Filter ' + noun + 's'}>
              <div class="axis-heading">
                <strong>{noun === 'region' ? 'Regions' : 'Zones'}</strong>
                <span>{draft.placement[axis].length === 0 ? 'All allowed' : draft.placement[axis].length + ' selected'}</span>
              </div>
              {#if options.length > 0}
                <div class="location-options">
                  {#each options as v (v)}
                    <label class="location-choice">
                      <input type="checkbox" checked={draft.placement[axis].includes(v)} onchange={(e) => toggleLocation(axis, v, e.currentTarget.checked)} />
                      <span>{v}</span>
                      {#if v === UNKNOWN_LOCATION}<em title="This agent did not declare a location">undeclared</em>{/if}
                    </label>
                  {/each}
                </div>
              {:else}
                <p class="field-note">No {noun}s reported yet.</p>
              {/if}
              <span class="field-note">{draft.placement[axis].length === 0 ? 'All ' + noun + 's allowed' : 'Only checked ' + noun + 's allowed'}</span>
            </section>
          {/snippet}
          {@render locations('regions', 'region', regionOptions)}
          {@render locations('zones', 'zone', zoneOptions)}
        </div>
        <p class="field-note filter-note">Region and zone selections are combined. Leave either group unchecked to include every location on that axis.</p>
        <details class="future-locations">
          <summary>Add a future region or zone <span>Advanced</span></summary>
          <p class="field-note">Add a location before its agents have started reporting.</p>
          <div class="future-location-fields">
            {#each [{ axis: 'regions', label: 'Region' }, { axis: 'zones', label: 'Zone' }] as item (item.axis)}
              <div class="future-location-row">
                <input class="mono" aria-label={'New ' + item.label.toLowerCase()} placeholder={'New ' + item.label.toLowerCase()} bind:value={adding[item.axis as 'zones' | 'regions']} />
                <button type="button" class="btn" disabled={!adding[item.axis as 'zones' | 'regions'].trim()} onclick={() => {
                  const axis = item.axis as 'zones' | 'regions';
                  addLocation(axis, adding[axis]);
                  adding[axis] = '';
                }}>Add</button>
              </div>
            {/each}
          </div>
        </details>
      {/if}

      <div class="observer-settings">
        <label class="field">
          <span class="field-label">Count per</span>
          <select bind:value={draft.placement.mode} aria-label="Placement mode">
            <option value="zone">Zone</option>
            <option value="region">Region</option>
          </select>
        </label>
        <div class="field">
          <label for="placement-count" class="field-label">Agents per {draft.placement.mode}</label>
          <NumberField min={1} ariaLabel={'Agents per ' + draft.placement.mode} bind:value={draft.placement.count} />
        </div>
        <p class="field-note observer-explainer">Each matched {draft.placement.mode} gets up to this many observers—not a fleet-wide total. Additional agents provide corroboration.</p>
      </div>
    </div>
  {:else}
    <div class="pinned-settings">
      <div class="pinned-heading">
        <strong>Choose the agents that own this check</strong>
        <span>{draft.agents.length} selected</span>
      </div>
      {#if agentOptions.length === 0}
        <p class="field-note">No agents have reported yet. Add an agent to the fleet before pinning this check.</p>
      {:else}
        <div class="agent-options">
          {#each agentOptions as id (id)}
            <label class="agent-choice">
              <input type="checkbox" value={id} bind:group={draft.agents} />
              <span>{id}</span>
              {#if !reporting.has(id)}<em title="An agent that is not reporting cannot run this check">Not reporting</em>{/if}
            </label>
          {/each}
        </div>
      {/if}
      {#if draft.agents.length === 0}
        <p class="selection-warning">Select at least one agent to activate pinning. Until then, location-based placement still applies.</p>
      {:else}
        <p class="field-note">Only these agents will run the check. Regions, zones, and observer counts are ignored while pinned.</p>
      {/if}
    </div>
  {/if}

  <!-- Show actual projected impact after the decision, not paragraphs between
       controls. Pinning has precedence even when the saved location filters
       remain configured in the draft. -->
  <section class="placement-preview" aria-label="Placement preview" aria-live="polite">
    <div class="preview-heading">
      <span>Current placement</span>
      {#if pinned}
        <strong class:warning={livePinned < draft.agents.length}>{livePinned} of {draft.agents.length} reporting</strong>
      {:else}
        <strong class:warning={effect.total === 0}>{effect.total} {effect.total === 1 ? 'probe' : 'probes'}</strong>
      {/if}
    </div>
    {#if pinned}
      <p>{draft.agents.length} {draft.agents.length === 1 ? 'agent is' : 'agents are'} pinned to this check. {livePinned === 0 ? 'None are currently reporting; this check will be silent.' : 'Only reporting agents can execute it.'}</p>
      <div class="preview-domains">
        {#each draft.agents as id (id)}
          <span class="preview-tag" class:short={!reporting.has(id)}>{id}{!reporting.has(id) ? ' · silent' : ''}</span>
        {/each}
      </div>
    {:else if effect.total === 0}
      <p class="warning-text">No reporting agent matches this placement. This check will be silent rather than marked down until coverage is available.</p>
    {:else}
      <p>Based on your reporting fleet, across {effect.domains.length} {draft.placement.mode}{effect.domains.length === 1 ? '' : 's'}.</p>
      <div class="preview-domains">
        {#each effect.domains as domain (domain.domain)}
          <span class="preview-tag" class:short={domain.available < effect.count}>{domain.domain} ×{domain.probes}{domain.available < effect.count ? ' · only ' + domain.available + ' live' : ''}</span>
        {/each}
      </div>
    {/if}
  </section>
</div>

<style>
  .placement-editor { display: grid; gap: 18px; color: var(--text); }
  .intro { margin: 0; max-width: 74ch; line-height: 1.6; }
  .strategy-fieldset, .scope-fieldset { min-width: 0; margin: 0; padding: 0; border: 0; }
  .strategy-fieldset legend, .scope-fieldset legend { margin-bottom: 9px; }
  .strategy-choices { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
  /* Force radio + copy into fixed/elastic grid columns. Some inherited form
     styles otherwise allow the radio to stretch and shove its text beyond
     the right edge of the selection card. */
  .strategy-option {
    display: grid;
    grid-template-columns: 16px minmax(0, 1fr);
    grid-template-rows: auto;
    gap: 12px;
    align-items: start;
    min-width: 0;
    min-height: 82px;
    padding: 15px;
    border: 1px solid var(--border);
    border-radius: 9px;
    background: #0d1b2b;
    cursor: pointer;
  }
  .strategy-option.active { background: #102538; border-color: #32718b; box-shadow: inset 0 0 0 1px rgba(91, 208, 230, .06); }
  .strategy-option input[type='radio'] {
    grid-column: 1;
    grid-row: 1;
    width: 16px;
    height: 16px;
    min-width: 16px;
    margin: 1px 0 0;
    padding: 0;
    justify-self: start;
    accent-color: var(--cyan, #6bbbd5);
  }
  .strategy-option .option-copy {
    grid-column: 2;
    grid-row: 1;
    display: grid;
    align-content: start;
    gap: 5px;
    min-width: 0;
    max-width: 100%;
    overflow-wrap: break-word;
    text-align: left;
  }
  .option-copy strong { font-size: 13px; font-weight: 600; }
  .option-copy small { font-size: 11px; line-height: 1.5; color: var(--text-dim); }
  .placement-settings { display: grid; gap: 14px; }
  .scope-choices { display: flex; flex-wrap: wrap; gap: 8px; }
  .scope-choices label { display: flex; gap: 8px; align-items: center; padding: 9px 13px; border: 1px solid var(--border); border-radius: 7px; font-size: 12px; cursor: pointer; }
  .scope-choices label.active { color: var(--text); background: #122b3b; border-color: #32627c; }
  .scope-choices input, .location-choice input, .agent-choice input { accent-color: var(--cyan, #6bbbd5); }
  .scope-fieldset > .field-note { display: block; margin: 8px 0 0; }
  .location-filters { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
  .location-axis { min-width: 0; padding: 14px; background: #0d1b2b; border: 1px solid var(--border); border-radius: 8px; }
  .axis-heading, .pinned-heading, .preview-heading { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px; }
  .axis-heading { margin-bottom: 12px; }
  .axis-heading strong { font-size: 12px; }
  .axis-heading > span, .pinned-heading > span { font-size: 11px; color: var(--text-dim); }
  .location-options, .agent-options { display: flex; flex-wrap: wrap; gap: 8px; }
  .location-choice, .agent-choice { display: inline-flex; align-items: center; gap: 7px; min-width: 0; padding: 7px 9px; border: 1px solid var(--border); border-radius: 7px; font-size: 12px; cursor: pointer; }
  .location-choice span, .agent-choice span { min-width: 0; overflow-wrap: anywhere; }
  .location-choice em, .agent-choice em { font-size: 10px; color: var(--amber); font-style: normal; }
  .location-axis > .field-note { display: block; margin-top: 12px; }
  .filter-note { margin: 0; }
  .future-locations { border-top: 1px solid var(--border); padding-top: 10px; }
  .future-locations summary { width: fit-content; color: var(--text-dim); cursor: pointer; font-size: 12px; }
  .future-locations summary span { margin-left: 6px; color: var(--text-dim); opacity: .75; font-size: 10px; }
  .future-locations > p { margin: 8px 0; }
  .future-location-fields { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
  .future-location-row { display: flex; gap: 6px; min-width: 0; }
  .future-location-row input { min-width: 0; flex: 1; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-variant-numeric: tabular-nums; }
  .observer-settings { display: grid; grid-template-columns: minmax(0, 180px) minmax(0, 170px) minmax(0, 1fr); gap: 14px; align-items: end; padding: 14px; background: #0c1a27; border: 1px solid var(--border); border-radius: 8px; }
  .observer-settings .field { margin: 0; }
  .observer-explainer { align-self: center; margin: 0; line-height: 1.5; }
  .pinned-settings { display: grid; gap: 12px; padding: 15px; background: #0d1b2b; border: 1px solid var(--border); border-radius: 8px; }
  .pinned-heading strong { font-size: 13px; }
  .pinned-settings p { margin: 0; }
  .selection-warning { padding: 9px 12px; color: var(--amber); background: rgba(171, 115, 33, .07); border: 1px solid var(--amber); border-radius: 6px; font-size: 12px; }
  .placement-preview { display: grid; gap: 9px; padding: 15px; border: 1px solid #285062; background: #0d2330; border-radius: 8px; }
  .preview-heading > span { color: var(--text-dim); font-size: 12px; }
  .preview-heading strong { font-size: 15px; font-weight: 600; color: var(--text); }
  .preview-heading .warning { color: var(--amber); }
  .placement-preview p { margin: 0; color: var(--text-dim); font-size: 12px; line-height: 1.5; }
  .placement-preview p.warning-text { color: var(--amber); }
  .preview-domains { display: flex; flex-wrap: wrap; gap: 6px; }
  .preview-tag { padding: 4px 8px; border: 1px solid #29485d; border-radius: 999px; color: var(--text-dim); font-size: 11px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  .preview-tag.short { color: var(--amber); border-color: var(--amber); }
  @media (max-width: 800px) {
    .observer-settings { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .observer-explainer { grid-column: 1 / -1; }
  }
  @media (max-width: 560px) {
    .strategy-choices, .location-filters, .future-location-fields { grid-template-columns: 1fr; }
    .observer-settings { grid-template-columns: 1fr; }
    .observer-explainer { grid-column: auto; }
  }
</style>
