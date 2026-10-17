<script lang="ts">
  import type { Check, Incident, SectionRollup } from '../lib/api';
  import { formatRelativeTime } from '../lib/format';
  import { summarizePublicStatus } from '../lib/publicStatus';
  import scenicLighthouse from '../assets/lighthouse-fall.png';

  let {
    checks, incidents, sections = [], waitingForRegions = false,
  }: {
    checks: Check[];
    incidents: Incident[];
    sections?: SectionRollup[];
    waitingForRegions?: boolean;
  } = $props();
  const summary = $derived(summarizePublicStatus(checks, incidents, sections, waitingForRegions));
  const observedAt = $derived(Math.max(0, ...checks.map((check) => check.last_check ?? 0)));
</script>

<section
  class="public-status-hero"
  class:storm={summary.tone === 'outage' || summary.tone === 'degraded' || summary.tone === 'incident'}
  class:awaiting={waitingForRegions}
  data-health={summary.tone}
  style:--scene={'url("' + scenicLighthouse + '")'}
  aria-label="Current service status"
>
  <div class="public-hero-scene" aria-hidden="true"></div>
  <div class="public-hero-content">
    <div class="public-state-banner {summary.tone}" role="status">
      <div class="public-state-symbol" aria-hidden="true">
        {#if waitingForRegions}…{:else if summary.tone === 'operational'}✓{:else if summary.tone === 'unknown'}?{:else}!{/if}
      </div>
      <div class="public-state-copy">
        <div class="public-state-heading">
          <h1>{summary.title}</h1>
          {#if summary.incident}<span class="public-state-label">{summary.incident.state}</span>{/if}
        </div>
        <p>{summary.detail}</p>
        {#if observedAt > 0}<p class="public-state-time">Latest monitor observation {formatRelativeTime(observedAt)}</p>{/if}
      </div>
    </div>
  </div>
</section>
