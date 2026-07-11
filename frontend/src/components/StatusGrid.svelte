<script lang="ts">
  import type { Check } from '../lib/api';
  import { statusColor, formatUptime, formatLatency, formatRelativeTime, sortChecks } from '../lib/format';
  import type { SortKey, SortDir } from '../lib/format';

  let { checks, onselect }: { checks: Check[]; onselect: (id: string) => void } = $props();

  let sortKey = $state<SortKey>('status');
  let sortDir = $state<SortDir>('asc');

  const sorted = $derived(sortChecks(checks, sortKey, sortDir));

  function sortBy(key: SortKey) {
    if (sortKey === key) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      sortKey = key;
      sortDir = 'asc';
    }
  }

  const arrow = (key: SortKey) => (sortKey === key ? (sortDir === 'asc' ? ' ▲' : ' ▼') : '');
</script>

<table class="grid">
  <thead>
    <tr>
      <th class="sortable" onclick={() => sortBy('name')}>Check{arrow('name')}</th>
      <th class="sortable" onclick={() => sortBy('status')}>Status{arrow('status')}</th>
      <th>Latency</th>
      <th class="sortable" onclick={() => sortBy('uptime')}>Uptime 24h{arrow('uptime')}</th>
      <th>Last check</th>
    </tr>
  </thead>
  <tbody>
    {#each sorted as check (check.check_id)}
      <tr onclick={() => onselect(check.check_id)}>
        <td>{check.check_id}</td>
        <td>
          <span class="badge" style="background:{statusColor(check.status)}">{check.status}</span>
        </td>
        <td class="mono">{formatLatency(check.latency_ms)}</td>
        <td class="mono">{formatUptime(check.uptime_24h)}</td>
        <td class="dim">{formatRelativeTime(check.last_check)}</td>
      </tr>
    {/each}
  </tbody>
</table>

{#if sorted.length === 0}
  <p class="dim">No checks configured.</p>
{/if}
