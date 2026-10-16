<script lang="ts">
  import type { FleetRow } from '../lib/api';
  import ServerRow from './ServerRow.svelte';

  let { fleet }: { fleet: FleetRow[] } = $props();
  const locations = $derived(fleet.filter((row) => row.kind === 'region').length || fleet.length);
</script>

{#if fleet.length}
  <details class="section public-coverage" aria-label="Monitoring coverage">
    <summary class="public-coverage-toggle">
      <span class="public-coverage-heading">
        <strong>Monitoring coverage</strong>
        <span>Connectivity of the monitoring fleet, not the health of the services it checks.</span>
      </span>
      <span class="public-coverage-count">
        {locations} reported locations
        <span class="public-coverage-action">· <span class="public-coverage-action-label">View observers</span></span>
        <span class="public-coverage-chevron" aria-hidden="true">⌄</span>
      </span>
    </summary>
    <div class="public-coverage-observers" aria-label="Reporting locations">
      {#each fleet as row (row.kind + ':' + row.name)}
        <ServerRow name={row.name} status={row.status}
          detail={row.kind === 'region' ? row.healthy + '/' + row.total + ' agents reporting' : undefined} />
      {/each}
    </div>
  </details>
{/if}
