<script lang="ts">
  import type { Check, Incident, UptimeResponse, UptimeBucket, LatencyPoint } from '../lib/api';
  import {
    formatUptime,
    formatLatency,
    barColor,
    statusColor,
    dayStatusText,
    failureReason,
  } from '../lib/format';
  import { stateTone } from '../lib/incident';
  import LatencyChart from './LatencyChart.svelte';

  let {
    check,
    uptime,
    sparkline,
    incident,
    onselect,
    onbarclick,
    compact = false,
  }: {
    check: Check;
    uptime: UptimeResponse | undefined;
    sparkline: LatencyPoint[] | null;
    /** The open incident this monitor is caught in, if any. Informational: the
     *  row is already a button to the check's detail page, where the incident
     *  is a link to its updates. */
    incident?: Incident | null;
    onselect: (id: string) => void;
    onbarclick?: (id: string, b: UptimeBucket) => void;
    compact?: boolean;
  } = $props();

  const placeholders = Array.from({ length: 90 }, (_, i) => i);

  const statusLabel = $derived(
    check.status === 'up'
      ? 'Operational'
      : check.status === 'degraded'
        ? 'Degraded'
        : check.status === 'down'
          ? 'Down'
          : 'Unknown',
  );

  // Why the latest attempt failed, shown under the header when there is one.
  const reason = $derived(failureReason(check));

  const dateFmt = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });

  const timeFmt = new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
  });

  const axisFmt = new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    hour12: true,
  });

  const dateTimeFmt = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });

  const DAY_MS = 86_400_000;

  // Segments are only whole days at the widest range, so label them by their
  // real width: a date alone would be ambiguous when a day holds 12 segments.
  function segFmt(ts: number): string {
    const width = uptime ? uptime.buckets[0].end - uptime.buckets[0].start : DAY_MS;
    return width >= DAY_MS ? dateFmt.format(new Date(ts)) : dateTimeFmt.format(new Date(ts));
  }

  function segLabel(b: UptimeBucket): string {
    const width = b.end - b.start;
    if (width >= DAY_MS) return dateFmt.format(new Date(b.start));
    // Sub-day segment: show the span it covers.
    return `${dateTimeFmt.format(new Date(b.start))} – ${timeFmt.format(new Date(b.end))}`;
  }

  // Hovered-bar tooltip state for the 90-day uptime strip (follows the pointer).
  let tip = $state<{ b: UptimeBucket; x: number; y: number } | null>(null);

  // Which bucket sits under the pointer, from its x offset in the strip.
  // Bars are equal-width flex items, so a simple ratio maps cleanly.
  function bucketAt(e: MouseEvent): UptimeBucket | null {
    const buckets = uptime?.buckets;
    if (!buckets?.length) return null;
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    const ratio = (e.clientX - rect.left) / rect.width;
    const i = Math.floor(ratio * buckets.length);
    return buckets[Math.max(0, Math.min(buckets.length - 1, i))] ?? null;
  }

  function onStripMove(e: MouseEvent) {
    const b = bucketAt(e);
    tip = b ? { b, x: e.clientX, y: e.clientY } : null;
  }

  function stripClicked(e: MouseEvent) {
    const b = bucketAt(e);
    if (!b) return;
    // Don't also trigger the row's own navigate-to-detail handler.
    e.stopPropagation();
    onbarclick?.(check.check_id, b);
  }
</script>

{#if compact}
  <button class="public-service-row" data-status={check.status} onclick={() => onselect(check.check_id)}>
    <span class="public-service-name">
      <span class="public-service-dot" style:background={statusColor(check.status)} aria-hidden="true"></span>
      <span><strong>{check.name}</strong><small>{check.group ?? check.type?.toUpperCase() ?? 'Service monitor'}</small></span>
    </span>
    <span class="public-service-history">
      <span class="ubars" role="presentation" onmousemove={onStripMove} onmouseleave={() => (tip = null)} onclick={stripClicked}>
        {#if uptime}
          {#each uptime.buckets as b (b.start)}
            <span class="ubar {barColor(b)}" class:hovered={tip?.b === b} role="presentation"></span>
          {/each}
        {:else}
          {#each placeholders as i (i)}
            <span class="ubar nodata" role="presentation"></span>
          {/each}
        {/if}
      </span>
      {#if reason}<small class="public-service-reason" title={reason}>{reason}</small>{/if}
      {#if incident}<small class="public-service-incident">Incident: {incident.title}</small>{/if}
    </span>
    <span class="public-service-stats">
      <strong>{formatUptime(uptime?.overall_uptime ?? null)}</strong>
      <small>{statusLabel} · {formatLatency(check.latency_ms)}</small>
    </span>
    <span class="public-service-chevron" aria-hidden="true">›</span>
  </button>
{:else}
<button class="component" onclick={() => onselect(check.check_id)}>
  <div class="component-head">
    <span class="component-name">
      <span class="status-dot" style="background:{statusColor(check.status)}"></span>
      {check.name}
    </span>
    <span class="component-meta">
      <span class="component-uptime">{formatUptime(uptime?.overall_uptime ?? null)}</span>
      <span class="component-latency">{formatLatency(check.latency_ms)} · {statusLabel}</span>
    </span>
  </div>

  {#if incident}
    <div class="component-incident">
      <span class="opill {stateTone(incident.state)}">{incident.state}</span>
      <span class="ci-title">{incident.title}</span>
    </div>
  {/if}

  {#if reason}
    <div class="component-reason {check.status}" title={reason}>{reason}</div>
  {/if}

  <!-- Hover is handled on the container, not each bar: with per-bar handlers the
       2px gaps fire mouseleave and the tooltip flickers as you sweep across. -->
  <div
    class="ubars"
    role="presentation"
    onmousemove={onStripMove}
    onmouseleave={() => (tip = null)}
    onclick={stripClicked}
  >
    {#if uptime}
      {#each uptime.buckets as b (b.start)}
        <span class="ubar {barColor(b)}" class:hovered={tip?.b === b} role="presentation"></span>
      {/each}
    {:else}
      {#each placeholders as i (i)}
        <span class="ubar nodata"></span>
      {/each}
    {/if}
  </div>

  <div class="strip-labels">
    <span>{uptime ? segFmt(uptime.buckets[0].start) : 'earliest'}</span>
    <span>Now</span>
  </div>

  {#if check.latency_graph && sparkline && sparkline.length > 1}
    <LatencyChart points={sparkline} label="latency over the selected window" />
  {/if}
</button>
{/if}

{#if tip}
  <div class="bar-tooltip" style="left:{tip.x}px; top:{tip.y}px">
    <div class="tt-date">{segLabel(tip.b)}</div>
    <div class="tt-status {barColor(tip.b)}">{dayStatusText(tip.b)}</div>
    {#if tip.b.total > 0}
      <div class="tt-detail">{tip.b.up}/{tip.b.total} checks passed</div>
    {/if}
  </div>
{/if}
