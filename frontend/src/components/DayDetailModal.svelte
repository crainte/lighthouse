<script lang="ts">
  import type {
    UptimeBucket,
    HistoryPoint,
    HistoryResponse,
    LatencySparklineResponse,
    CheckKind,
  } from '../lib/api';
  import { fetchHistory, fetchLatencyWindow } from '../lib/api';
  import { barColor, dayStatusText, formatLatency, failureReason, statusColor } from '../lib/format';
  import LatencyChart from './LatencyChart.svelte';

  let {
    checkId,
    name,
    kind,
    bucket,
    onclose,
  }: {
    checkId: string;
    /** Mutable display name, for the heading. Falls back to the id when the
     *  caller has none handy (e.g. a check deleted since this bucket loaded). */
    name?: string;
    /** Probe type, so the panel says what it measured. Absent for checks no
     *  longer in the config. */
    kind?: CheckKind;
    bucket: UptimeBucket;
    onclose: () => void;
  } = $props();

  // Every failing attempt in the segment. A busy window holds thousands of
  // results, so the newest N rows routinely exclude the very failures this view
  // exists to explain — they have to be asked for by status.
  let failures = $state<HistoryPoint[]>([]);
  // Latency shape and exact per-agent attribution over the whole segment.
  let series = $state<LatencySparklineResponse | null>(null);
  let loading = $state(true);
  let error = $state<string | null>(null);
  // The full attempt log is opt-in: hundreds of identical "OK" rows are noise
  // until you actually want to read them.
  let showLog = $state(false);
  let log = $state<HistoryPoint[]>([]);
  let logLoading = $state(false);

  const DAY_MS = 86_400_000;

  /** Squares per row — matches CheckDetail so the two grids read alike. */
  const COLS = 60;
  /** Ceiling per agent, so one chatty agent cannot push the panel metres long. */
  const MAX_PER_AGENT = COLS * 4;

  const dateFmt = new Intl.DateTimeFormat(undefined, {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
  const timeFmt = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
  const secFmt = new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  const stampFmt = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });

  // A bar is a whole day only at the widest range; below that it's a slice, so
  // the heading has to say which slice.
  const spanLabel = $derived(
    bucket.end - bucket.start >= DAY_MS
      ? dateFmt.format(new Date(bucket.start))
      : `${dateFmt.format(new Date(bucket.start))} · ${timeFmt.format(new Date(bucket.start))} – ${timeFmt.format(new Date(bucket.end))}`,
  );

  // The bar's own counts are authoritative — they're what you clicked.
  const failed = $derived(bucket.total - bucket.up);

  $effect(() => {
    loading = true;
    error = null;
    showLog = false;
    log = [];
    const { start, end } = bucket;
    Promise.all([
      fetchHistory(checkId, 500, start, end, 'down'),
      fetchLatencyWindow(checkId, start, end, 120),
    ])
      .then(([bad, s]: [HistoryResponse, LatencySparklineResponse]) => {
        failures = bad.history;
        series = s;
        loading = false;
      })
      .catch((e: Error) => {
        error = e.message;
        loading = false;
      });
  });

  /** One labelled block per agent, oldest→newest, trimmed to whole rows of
   *  `COLS` — identical shape to CheckDetail's probe grid, but derived from this
   *  modal's exact-window `log` instead of the check's newest-N history. */
  const probeRows = $derived.by(() => {
    // The API serves newest-first, so the first N per agent are the newest.
    const byAgent = new Map<string, HistoryPoint[]>();
    for (const p of log) {
      const row = byAgent.get(p.agent_id);
      if (row === undefined) byAgent.set(p.agent_id, [p]);
      else if (row.length < MAX_PER_AGENT) row.push(p);
    }
    return [...byAgent]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([agent_id, newestFirst]) => {
        const whole =
          newestFirst.length >= COLS
            ? Math.floor(newestFirst.length / COLS) * COLS
            : newestFirst.length;
        return { agent_id, probes: newestFirst.slice(0, whole).reverse() };
      });
  });
  const probeCount = $derived(probeRows.reduce((n, r) => n + r.probes.length, 0));

  /** Hovered square, positioned from the square's own box rather than the
   *  pointer, cleared on the grid's own mouseleave. Mirrors CheckDetail. */
  let hovered = $state<{ p: HistoryPoint; x: number; y: number } | null>(null);

  function hoverSquare(e: MouseEvent, p: HistoryPoint) {
    const r = (e.currentTarget as HTMLElement).getBoundingClientRect();
    hovered = { p, x: r.left + r.width / 2, y: r.top };
  }

  /** `.tt-status` has no `unknown` tone — an unrecorded probe reads the same as
   *  an empty bucket on the strip. */
  function tone(s: HistoryPoint['status']): string {
    return s === 'up' || s === 'down' ? s : 'nodata';
  }

  function toggleLog() {
    showLog = !showLog;
    if (!showLog || log.length || logLoading) return;
    logLoading = true;
    fetchHistory(checkId, COLS * 12, bucket.start, bucket.end)
      .then((r: HistoryResponse) => {
        log = r.history;
        logLoading = false;
      })
      .catch(() => (logLoading = false));
  }

  // Three separate numbers, not one string: "7/19/109" reads as a date.
  function bandParts(
    min: number | null,
    avg: number | null,
    max: number | null,
  ): string[] | null {
    if (avg === null) return null;
    return [String(min), String(Math.round(avg)), String(max)];
  }

  function onBackdrop(e: MouseEvent) {
    if ((e.target as HTMLElement).classList.contains('modal-backdrop')) onclose();
  }

  function onKeydown(e: KeyboardEvent) {
    if (e.key === 'Escape') onclose();
  }
</script>

<svelte:window onkeydown={onKeydown} />

<!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
<div class="modal-backdrop" onclick={onBackdrop} role="dialog" aria-modal="true" tabindex="-1">
  <div class="modal">
    <div class="modal-header">
      <div>
        <div class="title-line">
          <h2>{name ?? checkId}</h2>
          {#if kind}<span class="kind-pill">{kind}</span>{/if}
        </div>
        <p class="modal-span">{spanLabel}</p>
      </div>
      <div class="header-right">
        <span class="day-status {barColor(bucket)}">{dayStatusText(bucket)}</span>
        <button class="close-btn" onclick={onclose} aria-label="Close">&times;</button>
      </div>
    </div>

    {#if loading}
      <div class="modal-body"><p class="muted">Loading…</p></div>
    {:else if error}
      <div class="modal-body"><p class="error">{error}</p></div>
    {:else}
      <div class="figures">
        <div class="figure">
          <span class="fig-value">{bucket.uptime !== null ? `${(bucket.uptime * 100).toFixed(2)}%` : '—'}</span>
          <span class="fig-label">uptime</span>
        </div>
        <div class="figure">
          <span class="fig-value">{bucket.total}</span>
          <span class="fig-label">attempts</span>
        </div>
        <div class="figure">
          <span class="fig-value" class:bad={failed > 0}>{failed}</span>
          <span class="fig-label">failed</span>
        </div>
        {#if series}
          {@const band = bandParts(
            series.overall.min_ms,
            series.overall.avg_ms,
            series.overall.max_ms,
          )}
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
      </div>

      {#if series && series.points.length > 1}
        <div class="panel">
          <div class="panel-title">Latency across the window</div>
          <LatencyChart points={series.points} label="latency across the selected window" tall />
        </div>
      {/if}

      <div class="columns">
        <div class="panel">
          <div class="panel-title" class:ok={failed === 0}>
            {#if failed === 0}
              No failures
            {:else}
              {failed} failed {failed === 1 ? 'attempt' : 'attempts'}
            {/if}
          </div>
          {#if failures.length}
            <div class="fail-list">
              {#each failures as f (`${f.timestamp}-${f.agent_id}`)}
                <div class="fail-row">
                  <span class="mono dim">{secFmt.format(new Date(f.timestamp))}</span>
                  <span class="agent-pill">{f.agent_id}</span>
                  <span class="fail-why" title={failureReason(f) ?? ''}>
                    {failureReason(f) ?? 'failed'}
                  </span>
                </div>
              {/each}
            </div>
          {:else if failed > 0}
            <p class="muted">Details are outside the retained window.</p>
          {:else}
            <p class="all-clear"><span class="tick" aria-hidden="true">✓</span> Every attempt in this window passed.</p>
          {/if}
        </div>

        {#if series && series.agents.length}
          <div class="panel">
            <div class="panel-title">By agent</div>
            <div class="agent-grid">
              <div class="agent-head">
                <span>Agent</span><span>Attempts</span><span>Failed</span><span
                  >min / avg / max</span
                >
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
            <p class="panel-note">
              Latency covers successful attempts only, so a timeout never reads as the slowest
              response.
            </p>
          </div>
        {/if}
      </div>

      <div class="panel">
        <button class="log-toggle" onclick={toggleLog}>
          {showLog ? '▾' : '▸'} Every attempt
        </button>
        {#if showLog}
          {#if logLoading}
            <p class="muted">Loading…</p>
          {:else if log.length}
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
              timestamp and latency.{#if log.length >= COLS * 12}
                Capped at {COLS * 12} of {bucket.total} attempts.{/if}
            </p>
          {:else}
            <p class="muted">No attempts recorded.</p>
          {/if}
        {/if}
      </div>
    {/if}
  </div>
</div>

{#if hovered}
  {@const p = hovered.p}
  <!-- Same tooltip as the uptime strip's, so a square and a bar read alike. -->
  <div class="bar-tooltip" style="left:{hovered.x}px; top:{hovered.y}px">
    <div class="tt-date">{stampFmt.format(new Date(p.timestamp))}</div>
    <div class="tt-status {tone(p.status)}">{p.status}</div>
    <!-- No agent here: the block this square sits in is already labelled with it. -->
    <div class="tt-detail">{formatLatency(p.latency_ms)}</div>
    {#if failureReason(p)}
      <div class="tt-detail">{failureReason(p)}</div>
    {/if}
  </div>
{/if}

<style>
  .modal-backdrop {
    position: fixed;
    inset: 0;
    z-index: 1000;
    background: rgba(1, 4, 9, 0.72);
    display: flex;
    align-items: flex-start;
    justify-content: center;
    padding: 5vh 3vw;
    overflow-y: auto;
  }

  /* Wide: the content is a time series and two tables, all of which read badly
     in a narrow column. */
  .modal {
    background: var(--bg-card, #101316);
    border: 1px solid var(--border, #1f262b);
    border-radius: 12px;
    width: min(1100px, 100%);
    position: relative;
    box-shadow: 0 16px 48px rgba(0, 0, 0, 0.6);
  }

  .modal-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 1rem;
    padding: 1.1rem 1.4rem;
    border-bottom: 1px solid var(--border, #1f262b);
  }
  .modal-header h2 {
    margin: 0;
    font-size: 1.05rem;
    font-weight: 600;
    color: var(--text, #e6edf3);
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  .modal-span {
    margin: 0.25rem 0 0;
    font-size: 0.82rem;
    color: var(--text-dim, #7d8590);
  }
  .header-right {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .close-btn {
    background: none;
    border: none;
    color: var(--text-dim, #7d8590);
    font-size: 1.5rem;
    cursor: pointer;
    padding: 0;
    line-height: 1;
  }
  .close-btn:hover {
    color: var(--text, #e6edf3);
  }

  .day-status {
    font-size: 0.8rem;
    font-weight: 500;
    padding: 0.15rem 0.6rem;
    border-radius: 4px;
    white-space: nowrap;
  }
  .day-status.up {
    background: rgba(63, 185, 80, 0.15);
    color: var(--up, #3fb950);
  }
  .day-status.degraded {
    background: rgba(210, 153, 34, 0.15);
    color: var(--amber, #d29922);
  }
  .day-status.down {
    background: rgba(248, 81, 73, 0.15);
    color: var(--down, #f85149);
  }
  .day-status.nodata {
    background: rgba(139, 148, 158, 0.12);
    color: var(--text-dim, #7d8590);
  }

  .modal-body {
    padding: 1.1rem 1.4rem;
  }

  /* Layout of the figures themselves is shared (app.css); the modal only owns
     how the strip sits in the dialog. */
  .figures {
    padding: 1.1rem 1.4rem;
    border-bottom: 1px solid var(--border, #1f262b);
  }

  .panel-title.ok {
    color: var(--up, #3fb950);
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

  .panel {
    padding: 1rem 1.4rem;
    border-bottom: 1px solid var(--border, #1f262b);
    min-width: 0;
  }
  .panel:last-child {
    border-bottom: none;
  }
  .panel-title {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, #7d8590);
    margin-bottom: 0.7rem;
  }
  .panel-note {
    margin: 0.6rem 0 0;
    font-size: 0.72rem;
    color: var(--text-dim, #7d8590);
  }

  /* Failures beside agent attribution: the two questions you ask together. */
  .columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    border-bottom: 1px solid var(--border, #1f262b);
  }
  .columns > .panel {
    border-bottom: none;
  }
  .columns > .panel:first-child {
    border-right: 1px solid var(--border, #1f262b);
  }
  @media (max-width: 820px) {
    .columns {
      grid-template-columns: 1fr;
    }
    .columns > .panel:first-child {
      border-right: none;
      border-bottom: 1px solid var(--border, #1f262b);
    }
  }

  /* Trackless scrollbar: no groove, just a pill thumb. Modern browsers use the
     standard properties; the -webkit- rules are the fallback for Safari, which
     ignores them. Chromium honours whichever it supports and skips the rest. */
  .fail-list {
    max-height: 260px;
    overflow-y: auto;
    scrollbar-width: thin;
    scrollbar-color: #1f262b transparent;
  }
  .fail-list::-webkit-scrollbar {
    width: 10px;
  }
  .fail-list::-webkit-scrollbar-track {
    background: transparent;
  }
  .fail-list::-webkit-scrollbar-thumb {
    background: #1f262b;
    border-radius: 999px;
    /* Transparent border + content-box clip insets the pill so it floats in the
       gutter instead of filling it edge to edge. */
    border: 3px solid transparent;
    background-clip: content-box;
  }
  .fail-row {
    display: grid;
    grid-template-columns: 5.5rem auto 1fr;
    gap: 0.6rem;
    align-items: baseline;
    padding: 0.28rem 0;
    font-size: 0.8rem;
    border-left: 2px solid var(--down, #f85149);
    padding-left: 0.5rem;
    background: rgba(248, 81, 73, 0.05);
    margin-bottom: 2px;
  }
  .fail-why {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--down, #f85149);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* One labelled block of squares per agent \u2014 same as CheckDetail. */
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
  .sq-grid {
    display: grid;
    gap: 3px;
    width: 100%;
  }
  .sq {
    aspect-ratio: 1;
    min-width: 0;
    border-radius: 2px;
    cursor: default;
  }
  .sq.hovered {
    filter: brightness(1.4);
    outline: 1px solid var(--text, #e6edf3);
    outline-offset: 1px;
  }

  .agent-grid {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
  }
  .agent-head,
  .agent-row {
    display: grid;
    grid-template-columns: 1fr 4.5rem 4rem 7.5rem;
    gap: 0.5rem;
    align-items: center;
    font-size: 0.8rem;
    padding: 0.3rem 0;
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

  .log-toggle {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, #7d8590);
    cursor: pointer;
  }
  .log-toggle:hover {
    color: var(--text, #e6edf3);
  }

  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .dim {
    color: var(--text-dim, #7d8590);
  }
  .muted {
    color: var(--text-dim, #7d8590);
    font-size: 0.82rem;
    margin: 0;
  }
  .error {
    color: var(--down, #f85149);
    font-size: 0.82rem;
    margin: 0;
  }
</style>
