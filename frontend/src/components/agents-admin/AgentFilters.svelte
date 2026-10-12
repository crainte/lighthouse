<script lang="ts">
  import { placeName } from './fleet';

  let {
    search = $bindable(),
    statusFilter = $bindable(),
    regionFilter = $bindable(),
    zoneFilter = $bindable(),
    showRetired = $bindable(),
    grouped,
    onSetGrouped,
    regionOptions,
    zoneOptions,
    retiredCount,
    matchedCount,
    agentCount,
    filtersActive,
    onClear,
    purgeNote,
  }: {
    search: string;
    statusFilter: 'all' | 'up' | 'silent';
    regionFilter: string;
    zoneFilter: string;
    showRetired: boolean;
    grouped: boolean;
    onSetGrouped: (on: boolean) => void;
    regionOptions: string[];
    zoneOptions: string[];
    retiredCount: number;
    matchedCount: number;
    agentCount: number;
    filtersActive: boolean;
    onClear: () => void;
    purgeNote: string | null;
  } = $props();

  /** The chosen zone may not exist in the new region, and a filter pair that
   *  matches nothing looks like an empty fleet. */
  function pickRegion(next: string) {
    regionFilter = next;
    zoneFilter = '';
  }
</script>

<div class="controls">
  <label class="pick search-field"><span class="pick-label">Search fleet</span>
  <input
    class="search"
    type="search"
    bind:value={search}
    placeholder="Search agent, zone or region"
    aria-label="Search agents by id, zone or region"
  />
  </label>
  <div class="status-field"><span class="pick-label">Connectivity</span>
  <div class="range-switch" role="group" aria-label="Status filter">
    <button
      class="range-btn"
      class:active={statusFilter === 'all'}
      onclick={() => (statusFilter = 'all')}>All</button
    >
    <button
      class="range-btn"
      class:active={statusFilter === 'up'}
      onclick={() => (statusFilter = 'up')}>Up</button
    >
    <button
      class="range-btn"
      class:active={statusFilter === 'silent'}
      onclick={() => (statusFilter = 'silent')}>Silent</button
    >
  </div>
  </div>
  <label class="pick">
    <span class="pick-label">Region</span>
    <select value={regionFilter} onchange={(e) => pickRegion(e.currentTarget.value)}>
      <option value="">All regions</option>
      {#each regionOptions as r (r)}
        <option value={r}>{placeName(r)}</option>
      {/each}
    </select>
  </label>
  <label class="pick">
    <span class="pick-label">Zone</span>
    <select bind:value={zoneFilter}>
      <option value="">All zones</option>
      <!-- Distinct zone names for the chosen region, so no key repeats. -->
      {#each zoneOptions as z (z)}
        <option value={z}>{placeName(z)}</option>
      {/each}
    </select>
  </label>
  {#if retiredCount > 0}
    <label class="toggle">
      <input type="checkbox" bind:checked={showRetired} />
      <span>Retired <em>({retiredCount})</em></span>
    </label>
  {/if}
  <label class="toggle">
    <input type="checkbox" checked={grouped} onchange={(e) => onSetGrouped(e.currentTarget.checked)} />
    <span>Group by location</span>
  </label>
</div>

<div class="count-line">
  <span class="count">{matchedCount} of {agentCount} agents</span>
  {#if filtersActive}
    <button class="btn" onclick={onClear}>Clear filters</button>
  {/if}
  {#if purgeNote}<span class="admin-muted note">{purgeNote}</span>{/if}
</div>

<style>
  .controls {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 180px), 1fr));
    align-items: end;
    gap: 16px;
    margin-bottom: 20px;
  }
  .search {
    width: 100%;
    max-width: 100%;
  }
  .pick {
    display: flex;
    flex-direction: column;
    align-items: stretch;
    gap: 8px;
    min-width: 0;
  }
  .pick-label {
    font-size: 0.72rem;
    text-transform: none;
    letter-spacing: 0;
    color: var(--text-dim);
  }
  .pick select {
    width: 100%;
    min-width: 0;
  }

  .status-field { display: grid; gap: 8px; }
  .status-field .range-btn { min-height: 38px; flex: 1; }
  .controls .toggle { min-height: 38px; margin: 0; }
  .count-line {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    flex-wrap: wrap;
    margin-bottom: 0.5rem;
  }
  .count {
    font-size: 0.78rem;
    color: var(--text-dim);
    font-variant-numeric: tabular-nums;
  }
  .note {
    font-size: 0.78rem;
  }
</style>
