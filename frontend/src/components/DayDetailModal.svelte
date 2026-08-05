<script lang="ts">
  import type {
    UptimeBucket,
    HistoryPoint,
    HistoryResponse,
    LatencySparklineResponse,
    CheckKind,
  } from '../lib/api';
  import { fetchHistory, fetchLatencyWindow } from '../lib/api';
  import { barColor, dayStatusText, formatLatency, failureReason } from '../lib/format';
  import LatencyChart from './LatencyChart.svelte';

  let {
    checkId,
    kind,
    bucket,
    onclose,
  }: {
    checkId: string;
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

  function toggleLog() {
    showLog = !showLog;
    if (!showLog || log.length || logLoading) return;
    logLoading = true;
    fetchHistory(checkId, 300, bucket.start, bucket.end)
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
          <h2>{checkId}</h2>
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
            <div class="log">
              {#each log as pt (`${pt.timestamp}-${pt.agent_id}`)}
                <div class="log-row" class:down={pt.status === 'down'}>
                  <span class="mono dim">{secFmt.format(new Date(pt.timestamp))}</span>
                  <span class="agent-pill">{pt.agent_id}</span>
                  <span class="mono lat">{formatLatency(pt.latency_ms)}</span>
                  <span class="verdict" class:down={pt.status === 'down'}>
                    {pt.status === 'up' ? 'OK' : pt.status === 'down' ? 'FAIL' : 'UNKN'}
                  </span>
                  <span class="fail-why" title={failureReason(pt) ?? ''}>
                    {failureReason(pt) ?? ''}
                  </span>
                </div>
              {/each}
            </div>
            {#if log.length >= 300}
              <p class="panel-note">Newest 300 of {bucket.total} attempts.</p>
            {/if}
          {:else}
            <p class="muted">No attempts recorded.</p>
          {/if}
        {/if}
      </div>
    {/if}
  </div>
</div>

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
    background: var(--bg-card, #161b22);
    border: 1px solid var(--border, #30363d);
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
    border-bottom: 1px solid var(--border, #30363d);
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
    color: var(--text-dim, #8b949e);
  }
  .header-right {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .close-btn {
    background: none;
    border: none;
    color: var(--text-dim, #8b949e);
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
    color: var(--text-dim, #8b949e);
  }

  .modal-body {
    padding: 1.1rem 1.4rem;
  }

  /* Layout of the figures themselves is shared (app.css); the modal only owns
     how the strip sits in the dialog. */
  .figures {
    padding: 1.1rem 1.4rem;
    border-bottom: 1px solid var(--border, #30363d);
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
    border-bottom: 1px solid var(--border, #30363d);
    min-width: 0;
  }
  .panel:last-child {
    border-bottom: none;
  }
  .panel-title {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, #8b949e);
    margin-bottom: 0.7rem;
  }
  .panel-note {
    margin: 0.6rem 0 0;
    font-size: 0.72rem;
    color: var(--text-dim, #8b949e);
  }

  /* Failures beside agent attribution: the two questions you ask together. */
  .columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    border-bottom: 1px solid var(--border, #30363d);
  }
  .columns > .panel {
    border-bottom: none;
  }
  .columns > .panel:first-child {
    border-right: 1px solid var(--border, #30363d);
  }
  @media (max-width: 820px) {
    .columns {
      grid-template-columns: 1fr;
    }
    .columns > .panel:first-child {
      border-right: none;
      border-bottom: 1px solid var(--border, #30363d);
    }
  }

  /* Trackless scrollbar: no groove, just a pill thumb. Modern browsers use the
     standard properties; the -webkit- rules are the fallback for Safari, which
     ignores them. Chromium honours whichever it supports and skips the rest. */
  .fail-list,
  .log {
    max-height: 260px;
    overflow-y: auto;
    scrollbar-width: thin;
    scrollbar-color: #3d4650 transparent;
  }
  .fail-list::-webkit-scrollbar,
  .log::-webkit-scrollbar {
    width: 10px;
  }
  .fail-list::-webkit-scrollbar-track,
  .log::-webkit-scrollbar-track {
    background: transparent;
  }
  .fail-list::-webkit-scrollbar-thumb,
  .log::-webkit-scrollbar-thumb {
    background: #3d4650;
    border-radius: 999px;
    /* Transparent border + content-box clip insets the pill so it floats in the
       gutter instead of filling it edge to edge. */
    border: 3px solid transparent;
    background-clip: content-box;
  }
  .fail-row,
  .log-row {
    display: grid;
    grid-template-columns: 5.5rem auto 1fr;
    gap: 0.6rem;
    align-items: baseline;
    padding: 0.28rem 0;
    font-size: 0.8rem;
  }
  .log-row {
    grid-template-columns: 5.5rem auto 3.5rem 2.6rem 1fr;
  }
  .fail-row {
    border-left: 2px solid var(--down, #f85149);
    padding-left: 0.5rem;
    background: rgba(248, 81, 73, 0.05);
    margin-bottom: 2px;
  }
  .log-row.down {
    background: rgba(248, 81, 73, 0.06);
  }
  .fail-why {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--down, #f85149);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .lat {
    text-align: right;
  }
  .verdict {
    font-weight: 600;
    font-size: 0.72rem;
    color: var(--up, #3fb950);
  }
  .verdict.down {
    color: var(--down, #f85149);
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

  .log-toggle {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, #8b949e);
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
    color: var(--text-dim, #8b949e);
  }
  .muted {
    color: var(--text-dim, #8b949e);
    font-size: 0.82rem;
    margin: 0;
  }
  .error {
    color: var(--down, #f85149);
    font-size: 0.82rem;
    margin: 0;
  }
</style>
