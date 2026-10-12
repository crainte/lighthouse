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

<div class="fleet-pulse">
  <div class="pulse-heading"><span>Fleet connectivity</span><strong>{fleet.up}<span> / {agentCount} reporting</span></strong></div>
  <div class="pulse-track" role="img" aria-label={`${fleet.up} reporting and ${fleet.silent} silent out of ${agentCount} agents`}><span style={`width: ${agentCount ? fleet.up / agentCount * 100 : 0}%`}></span></div>
  <p>Agent connectivity, independent of the services they monitor.</p>
</div>
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
  .fleet-pulse { padding: 24px; border: 1px solid var(--ad-line); border-radius: 9px; background: var(--ad-panel); }
  .pulse-heading { display: flex; flex-wrap: wrap; align-items: baseline; justify-content: space-between; gap: 12px; font-size: 14px; }
  .pulse-heading strong { color: var(--ad-green); font-size: 28px; }
  .pulse-heading strong span { color: var(--ad-dim); font-size: 13px; font-weight: 400; }
  .pulse-track { height: 8px; background: #51422c; border-radius: 4px; overflow: hidden; margin-top: 20px; }
  .pulse-track > span { display: block; height: 100%; background: var(--ad-green); }
  .fleet-pulse p { color: var(--ad-dim); font-size: 12px; margin: 14px 0 0; }
  @media (max-width: 700px) { .summary { grid-template-columns: repeat(3, minmax(0, 1fr)); } .tile { padding: 16px; } }

  .summary {
    display: grid;
    grid-template-columns: repeat(6, minmax(0, 1fr));
    gap: 1px;
    background: var(--ad-line);
    border: 1px solid var(--ad-line);
    border-radius: 8px;
    overflow: hidden;
    margin: 16px 0 28px;
  }
  .tile {
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding: 18px 20px;
    background: var(--ad-panel);
  }
  .tile-value {
    font-size: 24px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
  }
  .tile-label {
    font-size: 0.72rem;
    text-transform: none;
    letter-spacing: 0;
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
