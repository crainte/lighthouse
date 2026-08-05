<script lang="ts">
  import { fetchHistory, fetchUptime, fetchLatencyWindow } from '../lib/api';
  import type {
    Check,
    HistoryPoint,
    UptimeResponse,
    LatencySparklineResponse,
  } from '../lib/api';
  import { statusColor, formatLatency, formatRelativeTime, failureReason, barColor } from '../lib/format';
  import LatencyChart from './LatencyChart.svelte';

  let {
    checkId,
    check,
    onback,
  }: {
    checkId: string;
    /** Live status row, so this page reports the same figures as the list. */
    check?: Check;
    onback: () => void;
  } = $props();

  const HOUR_MS = 3_600_000;

  // Same windows as the status page, so moving between them keeps its meaning.
  const RANGES = [
    { label: '24h', hours: 24 },
    { label: '7d', hours: 24 * 7 },
    { label: '30d', hours: 24 * 30 },
    { label: '90d', hours: 24 * 90 },
  ] as const;
  const STRIP_SEGMENTS = 90;
  let hours = $state(24);

  let uptime = $state<UptimeResponse | null>(null);
  let series = $state<LatencySparklineResponse | null>(null);
  /** Every failing attempt in the window, however far back it sits. */
  let failures = $state<HistoryPoint[]>([]);
  /** Newest attempts, for the raw log at the bottom. */
  let history = $state<HistoryPoint[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);

  $effect(() => {
    const id = checkId;
    const window = hours;
    let cancelled = false;
    loading = true;
    error = null;
    const to = Date.now();
    const from = to - window * HOUR_MS;
    // Failures are fetched by status rather than taken from the newest rows: a
    // busy check buries them thousands of results deep.
    Promise.all([
      fetchUptime(id, Math.max(1, Math.round(window / 24)), STRIP_SEGMENTS),
      fetchLatencyWindow(id, from, to, 120),
      fetchHistory(id, 500, from, to, 'down'),
      fetchHistory(id, 100),
    ])
      .then(([u, s, bad, recent]) => {
        if (cancelled) return;
        uptime = u;
        series = s;
        failures = bad.history;
        history = recent.history;
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        error = e instanceof Error ? e.message : String(e);
      })
      .finally(() => {
        if (!cancelled) loading = false;
      });
    return () => {
      cancelled = true;
    };
  });

  const rangeLabel = $derived(RANGES.find((r) => r.hours === hours)?.label ?? `${hours}h`);
  const attempts = $derived(series?.overall.attempts ?? 0);
  const failed = $derived(attempts - (series?.overall.up ?? 0));

  // Three separate numbers, not one string: "7/19/109" reads as a date.
  function bandParts(
    min: number | null,
    avg: number | null,
    max: number | null,
  ): string[] | null {
    if (avg === null) return null;
    return [String(min), String(Math.round(avg)), String(max)];
  }

  // The raw list is long (every agent × every interval). Show the newest slice
  // and let the reader ask for more.
  const PAGE = 20;
  let shown = $state(PAGE);
  const visible = $derived(history.slice(0, shown));

  function setHours(next: number) {
    if (next === hours) return;
    hours = next;
    shown = PAGE;
  }
</script>

<div class="detail-head">
  <button class="link" onclick={onback}>← Back</button>
  <div class="title-line">
    <h2 class="mono">{checkId}</h2>
    {#if check?.type}<span class="kind-pill">{check.type}</span>{/if}
    {#if check}
      <span class="status-chip" style="color:{statusColor(check.status)}">
        <span class="chip-dot" style="background:{statusColor(check.status)}"></span>
        {check.status}
      </span>
    {/if}
  </div>
  {#if check?.cron || check?.group}
    <p class="probe-line">
      {#if check.cron}<span class="mono">{check.cron}</span>{/if}
      {#if check.group}<span class="dim">· {check.group}</span>{/if}
    </p>
  {/if}
</div>

<div class="section-bar">
  <div class="range-switch" role="group" aria-label="detail window">
    {#each RANGES as r (r.hours)}
      <button class="range-btn" class:active={hours === r.hours} onclick={() => setHours(r.hours)}>
        {r.label}
      </button>
    {/each}
  </div>
</div>

{#if loading}
  <div class="notice">Loading history…</div>
{:else if error}
  <div class="notice error">Failed to load history: {error}</div>
{:else}
  <div class="card">
    <div class="figures">
      <div class="figure">
        <span class="fig-value">
          {uptime?.overall_uptime != null ? `${(uptime.overall_uptime * 100).toFixed(2)}%` : '—'}
        </span>
        <span class="fig-label">uptime ({rangeLabel})</span>
      </div>
      <div class="figure">
        <span class="fig-value">{attempts}</span>
        <span class="fig-label">attempts</span>
      </div>
      <div class="figure">
        <span class="fig-value" class:bad={failed > 0}>{failed}</span>
        <span class="fig-label">failed</span>
      </div>
      {#if series}
        {@const band = bandParts(series.overall.min_ms, series.overall.avg_ms, series.overall.max_ms)}
        <div class="figure">
          <span class="fig-value">
            {#if band}
              {#each band as v, i (i)}
                {#if i > 0}<span class="fig-sep">/</span>{/if}{v}
              {/each}<span class="fig-unit">ms</span>
            {:else}
              —
            {/if}
          </span>
          <span class="fig-label">min / avg / max</span>
        </div>
      {/if}
      <div class="figure">
        <span class="fig-value">{formatLatency(check?.latency_ms ?? null)}</span>
        <span class="fig-label">latest</span>
      </div>
    </div>

    {#if uptime}
      <div class="detail-strip">
        <div class="ubars" role="presentation">
          {#each uptime.buckets as b (b.start)}
            <span class="ubar {barColor(b)}" role="presentation"></span>
          {/each}
        </div>
        <div class="strip-labels">
          <span>{rangeLabel} ago</span>
          <span>Now</span>
        </div>
      </div>
    {/if}

    {#if series && series.points.length > 1}
      <LatencyChart points={series.points} label="latency over the selected window" />
    {/if}

    <div class="detail-panel">
      <div class="panel-title" class:ok={failed === 0}>
        {#if failed === 0}
          No failures
        {:else}
          {failed} failed {failed === 1 ? 'attempt' : 'attempts'}
        {/if}
      </div>
      {#if failures.length}
        <div class="fail-list">
          {#each failures.slice(0, 50) as f (`${f.timestamp}-${f.agent_id}`)}
            <div class="fail-row">
              <span class="dim">{formatRelativeTime(f.timestamp)}</span>
              <span class="agent-pill">{f.agent_id}</span>
              <span class="fail-why" title={failureReason(f) ?? ''}>
                {failureReason(f) ?? 'failed'}
              </span>
            </div>
          {/each}
        </div>
        {#if failures.length > 50}
          <p class="panel-note">…and {failures.length - 50} more.</p>
        {/if}
      {:else if failed > 0}
        <p class="dim">Details are outside the retained window.</p>
      {:else}
        <p class="all-clear"><span class="tick" aria-hidden="true">✓</span> Every attempt in this window passed.</p>
      {/if}
    </div>

    {#if series && series.agents.length}
      <div class="detail-panel">
        <div class="panel-title">By agent</div>
        <div class="agent-grid">
          <div class="agent-head">
            <span>Agent</span><span>Attempts</span><span>Failed</span><span>min / avg / max</span>
          </div>
          {#each series.agents as a (a.agent_id)}
            {@const ab = bandParts(a.min_ms, a.avg_ms, a.max_ms)}
            <div class="agent-row">
              <span class="agent-pill">{a.agent_id}</span>
              <span class="mono">{a.attempts}</span>
              <span class="mono" class:bad={a.attempts - a.up > 0}>{a.attempts - a.up}</span>
              <span class="mono band">
                {#if ab}
                  {#each ab as v, i (i)}
                    {#if i > 0}<span class="fig-sep">/</span>{/if}<span class="band-v">{v}</span>
                  {/each}
                {:else}
                  <span class="dim">—</span>
                {/if}
              </span>
            </div>
          {/each}
        </div>
      </div>
    {/if}

    <div class="detail-panel">
      <div class="panel-title">Recent attempts</div>
      {#if visible.length}
        {#each visible as h (`${h.timestamp}-${h.agent_id}`)}
          <div class="history-row">
            <span class="badge" style="background:{statusColor(h.status)}">{h.status}</span>
            <span class="mono">{formatLatency(h.latency_ms)}</span>
            <span class="agent-pill">{h.agent_id}</span>
            {#if failureReason(h)}
              <span class="row-reason" title={failureReason(h)}>{failureReason(h)}</span>
            {/if}
            <span class="dim" style="margin-left:auto">{formatRelativeTime(h.timestamp)}</span>
          </div>
        {/each}
        {#if shown < history.length}
          <div style="margin-top:0.75rem">
            <button class="link" onclick={() => (shown = Math.min(shown + PAGE, history.length))}>
              Show {Math.min(PAGE, history.length - shown)} more
            </button>
          </div>
        {:else if history.length > PAGE}
          <div style="margin-top:0.75rem">
            <button class="link" onclick={() => (shown = PAGE)}>Collapse</button>
          </div>
        {/if}
      {:else}
        <p class="dim">No attempts recorded.</p>
      {/if}
    </div>
  </div>
{/if}

<style>
  .probe-line {
    margin: 0.35rem 0 0;
    font-size: 0.82rem;
    color: var(--text-dim, #8b949e);
    word-break: break-all;
  }
  /* The cron expression is code, so it reads as code. */
  .probe-line .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  .status-chip {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: capitalize;
  }
  .chip-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
  }

  .detail-strip {
    margin: 1.2rem 0 0.6rem;
  }

  /* Sections inside the card, matching the day-detail panels. */
  .detail-panel {
    margin-top: 1.4rem;
    padding-top: 1.1rem;
    border-top: 1px solid var(--border, #30363d);
  }
  .panel-title {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, #8b949e);
    margin-bottom: 0.7rem;
  }
  .panel-title.ok {
    color: var(--up, #3fb950);
  }
  .panel-note {
    margin: 0.6rem 0 0;
    font-size: 0.72rem;
    color: var(--text-dim, #8b949e);
  }
  .all-clear {
    margin: 0;
    font-size: 0.82rem;
    color: var(--up, #3fb950);
  }
  .tick {
    font-weight: 700;
    margin-right: 0.2rem;
  }

  .fail-list {
    max-height: 280px;
    overflow-y: auto;
    scrollbar-width: thin;
    scrollbar-color: #3d4650 transparent;
  }
  .fail-row {
    display: grid;
    grid-template-columns: 7rem auto 1fr;
    gap: 0.6rem;
    align-items: baseline;
    padding: 0.28rem 0 0.28rem 0.5rem;
    margin-bottom: 2px;
    font-size: 0.8rem;
    border-left: 2px solid var(--down, #f85149);
    background: rgba(248, 81, 73, 0.05);
  }
  .fail-why {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--down, #f85149);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .agent-grid {
    display: flex;
    flex-direction: column;
  }
  .agent-head,
  .agent-row {
    display: grid;
    grid-template-columns: 1fr 5rem 4.5rem 8rem;
    gap: 0.5rem;
    align-items: center;
    font-size: 0.8rem;
    padding: 0.35rem 0;
  }
  .agent-head {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, #8b949e);
    border-bottom: 1px solid var(--border, #30363d);
  }
  .agent-row {
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  .agent-row:last-child {
    border-bottom: none;
  }
  .agent-row .bad {
    color: var(--down, #f85149);
    font-weight: 600;
  }
</style>
