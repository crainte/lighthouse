<script lang="ts">
  import type { Incident } from '../lib/api';
  import { formatDuration, formatRelativeTime, statusColor } from '../lib/format';

  let {
    incidents,
    onAcknowledge,
  }: {
    incidents: Incident[];
    onAcknowledge?: (i: Incident) => void;
  } = $props();
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
        {#if i.acknowledged_at != null}
          <span class="dim ack-note">
            acknowledged {formatRelativeTime(i.acknowledged_at)}
            {#if i.acknowledged_by}by {i.acknowledged_by}{/if}
          </span>
        {:else if i.status === 'open' && onAcknowledge}
          <button class="ack" onclick={() => onAcknowledge(i)}>Acknowledge</button>
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
  .ack {
    margin-left: auto;
    font-size: 0.8rem;
    padding: 0.15rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: transparent;
    color: var(--text);
    cursor: pointer;
  }
  .ack:hover {
    border-color: var(--accent);
  }
  .ack-note {
    margin-left: auto;
  }
</style>
