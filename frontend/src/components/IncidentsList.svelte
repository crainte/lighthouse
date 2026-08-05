<script lang="ts">
  import type { Incident } from '../lib/api';
  import { formatDuration, formatRelativeTime, statusColor } from '../lib/format';

  let { incidents }: { incidents: Incident[] } = $props();
</script>

{#if incidents.length === 0}
  <p class="dim">No incidents.</p>
{:else}
  <ul class="alerts">
    {#each incidents as i (i.id)}
      <li>
        <span
          class="badge"
          style="background:{i.status === 'open' ? statusColor('down') : statusColor('up')}"
        >
          {i.status}
        </span>
        <span class="mono">{i.check_id}</span>
        <span class="dim">started {formatRelativeTime(i.started_at)}</span>
        {#if i.status === 'resolved' && i.resolved_at != null}
          <span class="dim">for {formatDuration(i.resolved_at - i.started_at)}</span>
        {:else}
          <span class="dim">ongoing</span>
        {/if}
        {#if i.details}
          <span class="dim incident-detail" title={i.details}>{i.details}</span>
        {/if}
      </li>
    {/each}
  </ul>
{/if}

<style>
  .incident-detail {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 24rem;
  }
</style>
