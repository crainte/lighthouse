<script lang="ts">
  import type { ZoneStat } from './fleet';
  import { placeName } from './fleet';

  let {
    agentCount,
    fleet,
    onFocusZone,
  }: {
    agentCount: number;
    fleet: {
      up: number;
      silent: number;
      retired: number;
      zones: Map<string, unknown>;
      regions: Map<string, unknown>;
      dead: ZoneStat[];
    };
    onFocusZone: (z: ZoneStat) => void;
  } = $props();
</script>

<!-- What an operator reads before anything else: one line of counts, no cards,
     no hero number — the point is comparing six figures at a glance. -->
<div class="summary">
  <div class="tile">
    <span class="tile-value">{agentCount}</span>
    <span class="tile-label">agents</span>
  </div>
  <div class="tile">
    <span class="tile-value up" class:zero={fleet.up === 0}>{fleet.up}</span>
    <span class="tile-label">reporting</span>
  </div>
  <div class="tile">
    <span class="tile-value down" class:zero={fleet.silent === 0}>{fleet.silent}</span>
    <span class="tile-label">silent</span>
  </div>
  <div class="tile" title="Silent for over a week — counted in silent too.">
    <span class="tile-value">{fleet.retired}</span>
    <span class="tile-label">retired</span>
  </div>
  <div class="tile">
    <span class="tile-value">{fleet.zones.size}</span>
    <span class="tile-label">zones</span>
  </div>
  <div class="tile">
    <span class="tile-value">{fleet.regions.size}</span>
    <span class="tile-label">regions</span>
  </div>
</div>

{#if fleet.dead.length > 0}
  <p class="zone-alarm">
    {fleet.dead.length}
    {fleet.dead.length === 1 ? 'zone has' : 'zones have'} no live agent. Every check placed there
    is unprobed — placement selects nobody rather than quietly running it somewhere else.
  </p>
  <div class="dead-list">
    <!-- Zone names repeat across regions, so the key carries both. -->
    {#each fleet.dead.slice(0, 12) as z (`${z.region}\u0000${z.zone}`)}
      <button class="dead-chip" onclick={() => onFocusZone(z)}>
        {placeName(z.region)} / {placeName(z.zone)}
      </button>
    {/each}
    {#if fleet.dead.length > 12}
      <span class="admin-muted">+{fleet.dead.length - 12} more</span>
    {/if}
  </div>
{/if}

<style>
  .summary {
    display: flex;
    flex-wrap: wrap;
    align-items: stretch;
    gap: 1.4rem;
    padding: 0 0 0.7rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 0.8rem;
  }
  .tile {
    display: flex;
    align-items: baseline;
    gap: 0.35rem;
  }
  .tile-value {
    font-size: 1.05rem;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
  }
  .tile-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }
  .tile-value.up {
    color: var(--up);
  }
  .tile-value.down {
    color: var(--down);
  }
  /* Nothing silent is the good case; colouring a zero red would cry wolf. */
  .tile-value.zero {
    color: var(--text-dim);
  }

  .zone-alarm {
    color: var(--down);
    font-size: 0.8rem;
    line-height: 1.5;
    margin: 0 0 0.5rem;
    max-width: 46rem;
  }
  .dead-list {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem;
    margin-bottom: 0.8rem;
  }
  /* The tree used to be how you got to a dead zone; now the alarm itself is the
     way in — clicking one filters the table down to it. */
  .dead-chip {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.72rem;
    padding: 0.1rem 0.5rem;
    border: 1px solid var(--down);
    border-radius: 999px;
    background: none;
    color: var(--down);
    cursor: pointer;
  }
  .dead-chip:hover {
    background: rgba(248, 81, 73, 0.16);
  }
</style>
