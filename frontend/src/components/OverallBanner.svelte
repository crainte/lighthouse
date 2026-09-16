<script lang="ts">
  import type { Check } from '../lib/api';
  import { overallStatus } from '../lib/format';

  let { checks }: { checks: Check[] } = $props();

  const state = $derived(overallStatus(checks));
  const label = $derived(
    state === 'operational'
      ? 'All Systems Operational'
      : state === 'degraded'
        ? 'Degraded System Performance'
        : state === 'partial'
          ? 'Partial System Outage'
          : 'Major Outage',
  );
</script>

<div class="banner {state}">
  <span class="banner-icon" aria-hidden="true">
    {#if state === 'operational'}✓{:else}!{/if}
  </span>
  <span class="banner-text">{label}</span>
</div>
