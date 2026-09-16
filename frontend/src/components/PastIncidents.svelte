<script lang="ts">
  import type { Incident } from '../lib/api';
  import { formatDuration, formatRelativeTime } from '../lib/format';
  import { checkLabel, checkNameMap, incidentStateColor } from '../lib/incident';

  let {
    incidents,
    checks,
    onOpen,
  }: {
    incidents: Incident[];
    /** For resolving a check's mutable name from the id each `IncidentCheck`
     *  carries. */
    checks: { check_id: string; name: string }[];
    /** Follow the incident's public permalink. */
    onOpen: (id: string) => void;
  } = $props();

  const names = $derived(checkNameMap(checks));

  /** Deliberately the status page's own card vocabulary rather than the admin
   *  console's table: this sits directly under the monitor cards, and a flat
   *  hairline grid in the middle of them reads as a different site. */
</script>

{#if incidents.length === 0}
  <p class="dim">No incidents recorded.</p>
{:else}
  <div class="components">
    {#each incidents as i (i.id)}
      <button class="component" onclick={() => onOpen(i.id)}>
        <div class="component-head">
          <span class="component-name">
            <span class="status-dot" style="background:{incidentStateColor(i.state)}"></span>
            {i.title}
            {#if i.global}<span class="glob">global</span>{/if}
          </span>
          <span class="component-meta">
            <span class="component-uptime">{formatDuration(i.duration_ms)}</span>
            <span class="component-latency">
              {i.state === 'resolved' ? 'resolved' : i.state}
            </span>
          </span>
        </div>
        <div class="inc-foot">
          <span class="when">started {formatRelativeTime(i.started_at)}</span>
          {#if i.checks.length}
            <span class="mons">
              {#each i.checks as c (c.check_id)}
                <span class="mon" class:auto={c.source === 'dependency'}>
                  {checkLabel(names, c.check_id)}
                </span>
              {/each}
            </span>
          {/if}
        </div>
      </button>
    {/each}
  </div>
{/if}

<style>
  /* Head spacing is tighter than a monitor card's: there is no uptime strip
     underneath to separate from. */
  .component-head {
    margin-bottom: 0.45rem;
  }
  .glob {
    font-size: 0.62rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-dim);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0.05rem 0.4rem;
  }
  .inc-foot {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.75rem;
    flex-wrap: wrap;
  }
  .when {
    font-size: 0.75rem;
    color: var(--text-dim);
  }
  .mons {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }
  .mon {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.7rem;
    color: var(--text-dim);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.02rem 0.34rem;
  }
  /* Attached by dependency rather than by falling over itself. Dashed so a
     reader can see the difference without being told. */
  .mon.auto {
    border-style: dashed;
  }
</style>
