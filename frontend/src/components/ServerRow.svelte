<script lang="ts">
  import type { Status } from '../lib/api';
  import { statusColor } from '../lib/format';

  let {
    name,
    status = 'up',
    detail,
  }: {
    name: string;
    status?: Status;
    /** Secondary line, e.g. how many checks an agent reports. */
    detail?: string;
  } = $props();

  const statusLabel = $derived(
    status === 'up' ? 'Reporting' : status === 'down' ? 'Silent' : 'Unknown',
  );
</script>

<div class="server-row">
  <span class="server-name">
    <span class="status-dot" style="background:{statusColor(status)}"></span>
    {name}
    {#if detail}
      <span class="server-detail">{detail}</span>
    {/if}
  </span>
  <span class="server-status {status}">{statusLabel}</span>
</div>
