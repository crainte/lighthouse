<script lang="ts">
  import type { LatencyPoint } from '../lib/api';

  let {
    points,
    label,
    tall = false,
  }: {
    points: LatencyPoint[];
    label: string;
    /** Taller plot for the roomier day-detail view. */
    tall?: boolean;
  } = $props();

  // Bar geometry (Hyperping-style vertical bars in a 0..240 x 0..44 viewBox).
  // preserveAspectRatio="none" means these units stretch to whatever CSS height
  // the plot has, so `tall` needs no geometry changes.
  const CHART_W = 240;
  const CHART_H = 44;
  const DAY_MS = 86_400_000;

  const timeFmt = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
  const axisFmt = new Intl.DateTimeFormat(undefined, { hour: '2-digit', hour12: true });
  const dayFmt = new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric' });
  const stampFmt = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });

  // A window of days can't be labelled with clock times alone, so the axis and
  // the hover readout switch to dates once the span outgrows a couple of days.
  const span = $derived(
    points.length > 1 ? points[points.length - 1].timestamp - points[0].timestamp : 0,
  );
  const spansDays = $derived(span > 2 * DAY_MS);

  let hoverIndex = $state<number | null>(null);
  let hoverPct = $state(0);

  const maxLatency = $derived(
    Math.max(1, ...points.map((p) => p.avg_latency_ms ?? 0)),
  );

  /// Draw whenever anything was measured. This once required several populated
  /// segments, on the theory that fewer would render as slabs implying a
  /// measurement spanning days — but bars are capped to 4% of the width below,
  /// so a lone reading is already a mark at its point in time, not a slab. The
  /// guard only suppressed real data: a check with 132 results in the last five
  /// hours, viewed over 30 days, fills one segment and claimed to be
  /// "collecting" while sitting on a full history.
  // Counts were added after this endpoint existed, so fall back to the old
  // latency-only shape while a browser is talking to an older coordinator.
  const attempts = (p: LatencyPoint) => p.attempts ?? (p.avg_latency_ms === null ? 0 : 1);
  const successes = (p: LatencyPoint) => p.up ?? (p.avg_latency_ms === null ? 0 : 1);
  const enoughData = $derived(points.some((p) => attempts(p) > 0));
  const hasSuccessfulAttempt = $derived(points.some((p) => successes(p) > 0));

  // `i` is the index in `points`, kept because empty segments are dropped from
  // this list and positional index would no longer line up with the hover.
  type Bar = { i: number; x: number; y: number; w: number; h: number };
  const bars = $derived.by<Bar[]>(() => {
    if (points.length === 0) return [];
    const step = CHART_W / points.length;
    const populated = points.flatMap((p, i) => (p.avg_latency_ms === null ? [] : [i]));
    const gaps = populated.slice(1).map((i, n) => i - populated[n]).sort((a, b) => a - b);
    const cadence = gaps.length > 0 ? gaps[Math.floor(gaps.length / 2)] : 1;
    const w = Math.min(step * cadence, CHART_W * 0.08);
    // Segments with no attempts are skipped entirely — drawing them as a stub
    // would lay down a flat line of imaginary 0ms readings. A segment that *was*
    // measured always gets a visible bar, even at sub-millisecond latency, so
    // "fast" never renders identically to "no data".
    const MIN_H = CHART_H * 0.06;
    return points.flatMap((p, i) => {
      if (p.avg_latency_ms === null) return [];
      // Peaks fill only the lower ~78% so the floating readout up top stays clear.
      const h = Math.max(MIN_H, (p.avg_latency_ms / maxLatency) * (CHART_H * 0.78));
      return [{ i, x: i * step + step / 2 - w / 2, y: CHART_H - h, w, h }];
    });
  });

  const hoverPoint = $derived(hoverIndex !== null ? (points[hoverIndex] ?? null) : null);
  const scaleLabel = $derived(maxLatency > 0 ? `~${Math.round(maxLatency)}ms` : '');

  // Up to 6 evenly spaced ticks, but never more than there are points: with
  // only a handful of buckets the even spacing collapses onto the same index,
  // and duplicated keys abort the whole render.
  const axisTicks = $derived.by(() => {
    if (points.length < 2) return [];
    const count = Math.min(6, points.length);
    const seen = new Set<number>();
    const ticks: { i: number; pct: number; label: string }[] = [];
    for (let k = 0; k < count; k++) {
      const i = Math.round((k / (count - 1)) * (points.length - 1));
      if (seen.has(i)) continue;
      seen.add(i);
      const at = new Date(points[i].timestamp);
      ticks.push({
        i,
        pct: (i / (points.length - 1)) * 100,
        label: spansDays ? dayFmt.format(at) : axisFmt.format(at),
      });
    }
    return ticks;
  });

  function onMove(e: MouseEvent) {
    if (points.length === 0) return;
    const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect();
    const ratio = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    hoverIndex = Math.max(0, Math.min(points.length - 1, Math.floor(ratio * points.length)));
    hoverPct = ratio * 100;
  }
</script>

<div class="latency-graph" class:tall>
  {#if !enoughData}
    <p class="lat-empty">Collecting latency data…</p>
  {:else if !hasSuccessfulAttempt}
    <p class="lat-empty">Attempts recorded, but none succeeded yet.</p>
  {:else}
  <div class="latency-plot">
    <span class="lat-scale">{scaleLabel}</span>
    <div class="lat-inner">
      {#if hoverPoint}
        <div class="lat-readout" style="left:{hoverPct}%">
          <span class="lat-value">
            {#if attempts(hoverPoint) === 0}
              <span class="lat-nodata">no attempts</span>
            {:else if hoverPoint.avg_latency_ms === null}
              <span class="lat-nodata">Down</span>
            {:else if Math.round(hoverPoint.avg_latency_ms) === 0}
              <!-- Whole-millisecond probes: a mean that rounds to zero means
                   "under our resolution", not "instantaneous". -->
              &lt;1<span class="lat-unit">ms</span>
            {:else}
              {Math.round(hoverPoint.avg_latency_ms)}<span class="lat-unit">ms</span>
            {/if}
          </span>
          <span class="lat-time">
            {(spansDays ? stampFmt : timeFmt).format(new Date(hoverPoint.timestamp))}
            · {successes(hoverPoint)}/{attempts(hoverPoint)} succeeded
          </span>
        </div>
        <div class="lat-cursor" style="left:{hoverPct}%"></div>
      {/if}
      <svg
        class="latency-bars"
        viewBox="0 0 240 44"
        preserveAspectRatio="none"
        role="img"
        aria-label={label}
        onmousemove={onMove}
        onmouseleave={() => (hoverIndex = null)}
      >
        {#each bars as bar (bar.i)}
          <rect
            x={bar.x}
            y={bar.y}
            width={bar.w}
            height={bar.h}
            fill={bar.i === hoverIndex ? 'var(--text, #e6edf3)' : 'var(--accent, #56d4e4)'}
          />
        {/each}
      </svg>
      <div class="lat-axis">
        {#each axisTicks as t (t.i)}
          <span class="lat-tick" style="left:{t.pct}%">{t.label}</span>
        {/each}
      </div>
    </div>
  </div>
  {/if}
</div>

<style>
  /* A newly added check has a couple of readings; drawing them would imply a
     measurement spanning days rather than admitting there is nothing to show. */
  .lat-empty {
    margin: 0.75rem 0 0.25rem;
    font-size: 0.75rem;
    color: var(--text-dim, #7d8590);
  }
</style>
