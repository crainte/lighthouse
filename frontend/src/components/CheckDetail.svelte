<script lang="ts">
  import { fetchHistory, fetchUptime, fetchLatencyWindow, fetchCheckIncidents } from '../lib/api';
  import type {
    Check,
    Incident,
    HistoryPoint,
    UptimeResponse,
    LatencySparklineResponse,
  } from '../lib/api';
  import {
    statusColor,
    formatLatency,
    formatRelativeTime,
    formatDuration,
    failureReason,
    barColor,
  } from '../lib/format';
  import { stateTone } from '../lib/incident';
  import { bandParts, computeProbeRows, probeTone } from '../lib/probeGrid';
  import LatencyChart from './LatencyChart.svelte';

  let {
    checkId,
    check,
    refreshKey = 0,
    onback,
  }: {
    checkId: string;
    /** Live status row, so this page reports the same figures as the list. */
    check?: Check;
    /** Bumped by the parent on a live state change for this check or on a
     *  websocket reconnect, to force a refetch of this page's own data. */
    refreshKey?: number;
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
  /** Newest attempts, for the probe grid at the bottom. */
  let history = $state<HistoryPoint[]>([]);
  /** Every incident this check was attached to, however it got there. The
   *  per-check endpoint rather than the page's list: that one is capped at the
   *  newest 50 site-wide, which is not this check's history. */
  let incidents = $state<Incident[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);
  // The check/window the current data belongs to, so a refreshKey bump can tell
  // a silent refresh (same check, same window) from a genuine (re)load.
  let loadedId: string | null = null;
  let loadedWindow = -1;

  $effect(() => {
    const id = checkId;
    const window = hours;
    // Depend on refreshKey so a live state change or reconnect refetches. A bump
    // is a silent refresh — no full-page spinner — since the page is already up
    // and only the numbers are moving. A change of check or window is a real
    // (re)load and does show the spinner.
    const isRefresh = refreshKey > 0 && id === loadedId && window === loadedWindow;
    void refreshKey;
    loadedId = id;
    loadedWindow = window;
    let cancelled = false;
    if (!isRefresh) loading = true;
    error = null;
    const to = Date.now();
    const from = to - window * HOUR_MS;
    // Failures are fetched by status rather than taken from the newest rows: a
    // busy check buries them thousands of results deep.
    Promise.all([
      fetchUptime(id, Math.max(1, Math.round(window / 24)), STRIP_SEGMENTS),
      fetchLatencyWindow(id, from, to, 120),
      fetchHistory(id, 500, from, to, 'down'),
      fetchHistory(id, PROBES),
      fetchCheckIncidents(id),
    ])
      .then(([u, s, bad, recent, inc]) => {
        if (cancelled) return;
        uptime = u;
        series = s;
        failures = bad.history;
        history = recent.history;
        incidents = inc.incidents;
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

  /** Squares per row. Fixed rather than "however many fit", so a block of
   *  probes is either whole rows or nothing — a wrapping flex row left a
   *  12-square tail hanging off the bottom. */
  const COLS = 60;
  /** Ceiling per agent, so one chatty agent cannot push the panel metres long. */
  const MAX_PER_AGENT = COLS * 4;
  /** Flat history to slice up. The endpoint serves the newest N across every
   *  agent, so a big fleet gets shorter rows rather than some agents none. */
  const PROBES = COLS * 12;

  /** One labelled block per agent, so ten of them turn one region's outage into
   *  scattered noise instead of a readable page. lib/probeGrid.ts also backs
   *  DayDetailModal's identically-shaped grid over one bucket's exact window. */
  const probeRows = $derived(computeProbeRows(history, COLS, MAX_PER_AGENT));
  const probeCount = $derived(probeRows.reduce((n, r) => n + r.probes.length, 0));

  const stampFmt = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });

  /** Hovered square, positioned from the square's own box rather than the
   *  pointer: the squares are 11px with gaps, and a pointer-following tooltip
   *  jitters across them. Cleared on the grid's own mouseleave, so crossing a
   *  gap doesn't flicker it. */
  let hovered = $state<{ p: HistoryPoint; x: number; y: number } | null>(null);

  function hoverSquare(e: MouseEvent, p: HistoryPoint) {
    const r = (e.currentTarget as HTMLElement).getBoundingClientRect();
    hovered = { p, x: r.left + r.width / 2, y: r.top };
  }

  function setHours(next: number) {
    if (next === hours) return;
    hours = next;
  }
</script>

<div class="detail-head">
  <button class="link" onclick={onback}>← Back</button>
  <div class="title-line">
    <h2 class="mono">{check?.name ?? checkId}</h2>
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

    {#if incidents.length}
      <div class="detail-panel">
        <div class="panel-title">Incidents</div>
        <div class="inc-list">
          {#each incidents as i (i.id)}
            <a class="inc-row" href={`#/incident/${encodeURIComponent(i.id)}`}>
              <span class="opill {stateTone(i.state)}">{i.state}</span>
              <span class="inc-name">{i.title}</span>
              <span class="mono">{formatDuration(i.duration_ms)}</span>
              <span class="dim inc-when">{formatRelativeTime(i.started_at)}</span>
            </a>
          {/each}
        </div>
      </div>
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
      <div class="panel-title">Recent probes</div>
      {#if probeRows.length}
        <!-- Hover lives on each square but is cleared on the container: a
             per-square mouseleave fires in the gutters and blinks the tooltip. -->
        <div class="sq-rows" role="presentation" onmouseleave={() => (hovered = null)}>
          {#each probeRows as row (row.agent_id)}
            <div class="sq-block">
              <span class="agent-pill">{row.agent_id}</span>
              <div class="sq-grid" style="grid-template-columns: repeat({COLS}, 1fr)">
                {#each row.probes as h (`${h.timestamp}-${h.agent_id}`)}
                  <span
                    class="sq"
                    class:hovered={hovered?.p === h}
                    style="background:{statusColor(h.status)}"
                    role="presentation"
                    onmouseenter={(e) => hoverSquare(e, h)}
                  ></span>
                {/each}
              </div>
            </div>
          {/each}
        </div>
        <p class="panel-note">
          One square per probe, oldest to newest, {COLS} to a row — {probeCount}
          {probeCount === 1 ? 'probe' : 'probes'} across {probeRows.length}
          {probeRows.length === 1 ? 'agent' : 'agents'}, not a fixed span of time. Each agent
          shows its own probes, so a column is not one moment across rows. Hover for the
          timestamp and latency.
        </p>
      {:else}
        <p class="dim">No attempts recorded.</p>
      {/if}
    </div>
  </div>
{/if}

{#if hovered}
  {@const p = hovered.p}
  <!-- Same tooltip as the uptime strip's, so a square and a bar read alike. -->
  <div class="bar-tooltip" style="left:{hovered.x}px; top:{hovered.y}px">
    <div class="tt-date">{stampFmt.format(new Date(p.timestamp))}</div>
    <div class="tt-status {probeTone(p.status)}">{p.status}</div>
    <!-- No agent here: the block this square sits in is already labelled with it. -->
    <div class="tt-detail">{formatLatency(p.latency_ms)}</div>
    {#if failureReason(p)}
      <div class="tt-detail">{failureReason(p)}</div>
    {/if}
  </div>
{/if}

<style>
  .probe-line {
    margin: 0.35rem 0 0;
    font-size: 0.82rem;
    color: var(--text-dim, #7d8590);
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
    border-top: 1px solid var(--border, #1f262b);
  }
  .panel-title {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, #7d8590);
    margin-bottom: 0.7rem;
  }
  .panel-title.ok {
    color: var(--up, #3fb950);
  }
  .panel-note {
    margin: 0.6rem 0 0;
    font-size: 0.72rem;
    color: var(--text-dim, #7d8590);
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
    scrollbar-color: #1f262b transparent;
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
    color: var(--text-dim, #7d8590);
    border-bottom: 1px solid var(--border, #1f262b);
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
  /* This check's incidents. Each is a link to its public permalink, which is
     where the update timeline lives — the row only carries enough to pick one. */
  .inc-list {
    display: flex;
    flex-direction: column;
  }
  .inc-row {
    display: grid;
    grid-template-columns: auto 1fr auto auto;
    gap: 0.6rem;
    align-items: center;
    padding: 0.4rem 0;
    font-size: 0.8rem;
    color: inherit;
    text-decoration: none;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  .inc-row:last-child {
    border-bottom: none;
  }
  .inc-row:hover .inc-name {
    text-decoration: underline;
  }
  .inc-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .inc-when {
    text-align: right;
  }
  /* One labelled block of squares per agent. */
  .sq-rows {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }
  .sq-block {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 0.3rem;
  }
  /* A fixed column count with `1fr` tracks, not wrapping flex: every row is
     exactly as wide as the block, so the squares narrow on a small viewport
     rather than reflowing into a short final row. The count comes from `COLS`
     inline, so there is one source of truth for it. */
  .sq-grid {
    display: grid;
    gap: 3px;
    width: 100%;
  }
  .sq {
    /* Square whatever the track width works out to — the Kener proportion,
       held by geometry rather than a hardcoded 11px. */
    aspect-ratio: 1;
    min-width: 0;
    border-radius: 2px;
    cursor: default;
  }
  /* Driven by state, not `:hover`, so the highlight matches whichever square
     the tooltip is describing. */
  .sq.hovered {
    filter: brightness(1.4);
    outline: 1px solid var(--text, #e6edf3);
    outline-offset: 1px;
  }
</style>
