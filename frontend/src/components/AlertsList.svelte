<script lang="ts">
  import type { Alert } from '../lib/api';
  import { statusColor, formatRelativeTime } from '../lib/format';

  let { alerts }: { alerts: Alert[] } = $props();
</script>

{#if alerts.length === 0}
  <p class="dim">No recent alerts.</p>
{:else}
  <ul class="alerts">
    {#each alerts as a, i (a.check_id + ':' + a.timestamp + ':' + i)}
      <li>
        <span class="badge" style="background:{statusColor(a.to)}">{a.to}</span>
        <span class="mono">{a.check_id}</span>
        <span class="transition">
          <span style="color:{statusColor(a.from)}">{a.from}</span>
          <span class="arrow">→</span>
          <span style="color:{statusColor(a.to)}">{a.to}</span>
        </span>
        <span class="dim" style="margin-left:auto">{formatRelativeTime(a.timestamp)}</span>
      </li>
    {/each}
  </ul>
{/if}
