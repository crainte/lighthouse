<script lang="ts">
  import { fetchHistory } from '../lib/api';
  import type { HistoryPoint } from '../lib/api';
  import { statusColor, formatUptime, formatLatency, formatRelativeTime } from '../lib/format';

  let { checkId, onback }: { checkId: string; onback: () => void } = $props();

  let history = $state<HistoryPoint[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);

  $effect(() => {
    const id = checkId;
    let cancelled = false;
    loading = true;
    error = null;
    fetchHistory(id, 100)
      .then((res) => {
        if (cancelled) return;
        history = res.history;
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

  // Oldest-first for the chart; API returns newest-first.
  const chrono = $derived([...history].reverse());
  const maxLatency = $derived(Math.max(1, ...chrono.map((h) => h.latency_ms ?? 0)));
  const upCount = $derived(history.filter((h) => h.status === 'up').length);
  const uptime = $derived(history.length ? upCount / history.length : null);
</script>

<div class="detail-head">
  <button class="link" onclick={onback}>← Back</button>
  <h2 class="mono">{checkId}</h2>
</div>

{#if loading}
  <div class="notice">Loading history…</div>
{:else if error}
  <div class="notice error">Failed to load history: {error}</div>
{:else}
  <div class="card">
    <div style="margin-bottom:1rem">
      <span class="stat">
        <span class="label">Uptime ({history.length} checks)</span>
        <span class="value">{formatUptime(uptime)}</span>
      </span>
      <span class="stat">
        <span class="label">Latest latency</span>
        <span class="value">{formatLatency(chrono.at(-1)?.latency_ms ?? null)}</span>
      </span>
    </div>

    {#if chrono.length}
      <div class="bars" title="latency over time (oldest → newest)">
        {#each chrono as h (h.timestamp)}
          <div
            class="bar"
            style="height:{Math.round(((h.latency_ms ?? 0) / maxLatency) * 100)}%;background:{statusColor(h.status)}"
            title="{formatRelativeTime(h.timestamp)} · {h.status} · {formatLatency(h.latency_ms)}"
          ></div>
        {/each}
      </div>

      <div style="margin-top:1rem">
        {#each history as h (h.timestamp)}
          <div class="history-row">
            <span class="badge" style="background:{statusColor(h.status)}">{h.status}</span>
            <span class="mono">{formatLatency(h.latency_ms)}</span>
            <span class="dim" style="margin-left:auto">{formatRelativeTime(h.timestamp)}</span>
          </div>
        {/each}
      </div>
    {:else}
      <p class="dim">No history for this check.</p>
    {/if}
  </div>
{/if}
