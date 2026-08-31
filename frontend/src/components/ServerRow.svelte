<script lang="ts">
  import type { Health } from '../lib/api';
  import { statusColor } from '../lib/format';

  let {
    name,
    status = 'up',
    detail,
  }: {
    name: string;
    /** Liveness, not check health: an agent answering is healthy even if every
     *  check it probes is down. A region row is `degraded` when only some of
     *  its agents are healthy. */
    status?: Health;
    /** Secondary line, e.g. how many of a region's agents are healthy. */
    detail?: string;
  } = $props();

  const statusLabel = $derived(
    status === 'up' ? 'Healthy' : status === 'degraded' ? 'Degraded' : 'Down',
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
