<script lang="ts">
  import { UNKNOWN_LOCATION } from '../../lib/api';
  import { formatRelativeTime } from '../../lib/format';
  import type { Row, SortKey } from './fleet';
  import { placeName } from './fleet';

  let {
    rendered,
    sortKey,
    sortDir,
    onToggleSort,
    ariaSort,
    collapsed = $bindable(),
    pending = $bindable(),
    purging,
    purgeError,
    onArm,
    onConfirmPurge,
    capped,
    shown,
    pageSize,
    onShowMore,
    onClearFilters,
  }: {
    rendered: Row[];
    sortKey: SortKey;
    sortDir: 1 | -1;
    onToggleSort: (key: SortKey) => void;
    ariaSort: (key: SortKey) => 'none' | 'ascending' | 'descending';
    collapsed: Record<string, boolean>;
    pending: string | null;
    purging: boolean;
    purgeError: string | null;
    onArm: (id: string) => void;
    onConfirmPurge: (id: string) => void;
    capped: number;
    shown: number;
    pageSize: number;
    onShowMore: () => void;
    onClearFilters: () => void;
  } = $props();
</script>

<div class="fleet-sort" role="group" aria-label="Sort agents">
  <span>Sort by</span>
  {#each [['agent', 'Agent'], ['region', 'Region'], ['zone', 'Zone'], ['status', 'Connectivity'], ['seen', 'Last seen'], ['rtt', 'Poll RTT'], ['reporting', 'Reporting'], ['results', 'Results'], ['pinned', 'Pinned']] as [key, label] (key)}
    <button class:active={sortKey === key} aria-pressed={sortKey === key} aria-label={`${label}, ${ariaSort(key as SortKey)}`} onclick={() => onToggleSort(key as SortKey)}>{label}{sortKey === key ? (sortDir === 1 ? ' ↑' : ' ↓') : ''}</button>
  {/each}
</div>
<div class="fleet-list">
  {#if rendered.length === 0}
    <div class="fleet-empty">No agent matches these filters. <button class="btn" onclick={onClearFilters}>Clear filters</button></div>
  {/if}
  {#each rendered as row (row.key)}
    {#if row.kind === 'region'}
      <button class="location-heading region-heading" aria-expanded={!collapsed[row.key]} onclick={() => (collapsed[row.key] = !collapsed[row.key])}>
        <svg class:closed={collapsed[row.key]} width="16" height="16" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 9 6 6 6-6" /></svg>
        <strong>{row.region === UNKNOWN_LOCATION ? 'No location declared' : row.region}</strong>
        <span>{row.stat.zones} zones · {row.stat.up} reporting · {row.stat.count - row.stat.up} silent</span>
      </button>
    {:else if row.kind === 'zone'}
      <button class="location-heading zone-heading" aria-expanded={!collapsed[row.key]} onclick={() => (collapsed[row.key] = !collapsed[row.key])}>
        <svg class:closed={collapsed[row.key]} width="16" height="16" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 9 6 6 6-6" /></svg>
        <strong>{placeName(row.zone.zone)}</strong>
        <span class:silent={row.zone.up === 0}>{row.zone.up} / {row.zone.count} reporting</span>
      </button>
    {:else}
      {@const a = row.agent}
      <article class="fleet-agent" class:retired={a.retired}>
        <div class="agent-identity">
          <span class="agent-signal" class:reporting={a.status === 'up'}></span>
          <div><strong class="agent-name">{a.agent_id}</strong><span class="agent-location">{placeName(a.region)} / {placeName(a.zone)}</span></div>
        </div>
        <div class="agent-connectivity"><span class:reporting={a.status === 'up'}>{a.retired ? 'Retired' : a.status === 'up' ? 'Reporting' : 'Silent'}</span><span class="agent-caption" title={new Date(a.last_seen).toLocaleString()}>Seen {formatRelativeTime(a.last_seen)}</span></div>
        <dl class="agent-metrics">
          <div><dt>Poll RTT</dt><dd>{a.poll_rtt_ms == null ? '—' : `${a.poll_rtt_ms} ms`}</dd></div>
          <div><dt>Checks reporting</dt><dd>{a.checks_reporting}</dd></div>
          <div><dt>Results / 24h</dt><dd>{a.results_24h.toLocaleString()}</dd></div>
          <div><dt>Pinned checks</dt><dd>{a.assigned_checks}</dd></div>
        </dl>
        <details class="agent-details">
          <summary>Agent details</summary>
          <div class="agent-detail-body">
            <div><span class="agent-caption">Reported build</span><span class="agent-build">{a.version ?? 'Not reported'}</span></div>
            <div><span class="agent-caption">Probe capabilities</span><span>{a.kinds?.join(', ') ?? 'Not reported'}</span></div>
            <button class="btn danger" onclick={() => onArm(a.agent_id)} disabled={purging}>Purge agent data</button>
          </div>
        </details>
        {#if pending === a.agent_id}
          <div class="purge-confirm" role="alert">
            <p>Permanently purge {a.agent_id} and every result it has recorded? This removes probe history, including {a.results_24h} results in the last 24 hours. {a.assigned_checks} checks explicitly reference this agent. This cannot be undone.</p>
            {#if purgeError}<p class="admin-error">{purgeError}</p>{/if}
            <div class="confirm-actions"><button class="btn danger" onclick={() => onConfirmPurge(a.agent_id)} disabled={purging}>{purging ? 'Purging…' : 'Purge agent and results'}</button><button class="btn" onclick={() => (pending = null)} disabled={purging}>Cancel</button></div>
          </div>
        {/if}
      </article>
    {/if}
  {/each}
</div>
{#if capped > 0}
  <div class="fleet-more"><button class="btn" onclick={onShowMore}>Show {Math.min(pageSize, capped)} more</button><span class="admin-muted">{shown} of {shown + capped} shown</span></div>
{/if}

<style>
  .fleet-sort { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; padding: 16px 0; color: var(--ad-dim); font-size: 12px; }
  .fleet-sort button { font: inherit; color: var(--ad-dim); border: 1px solid transparent; border-radius: 5px; background: none; padding: 6px 10px; cursor: pointer; }
  .fleet-sort button.active { color: var(--ad-cyan); background: var(--ad-panel-2); border-color: var(--ad-line); }
  .fleet-list { border: 1px solid var(--ad-line); border-radius: 8px; overflow: hidden; }
  .location-heading { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; width: 100%; padding: 18px 20px; border: 0 solid var(--ad-line); border-bottom-width: 1px; background: var(--ad-panel-2); color: var(--ad-text); text-align: left; font: inherit; cursor: pointer; }
  .location-heading span { margin-left: auto; color: var(--ad-dim); font-size: 12px; }
  .location-heading svg { fill: none; stroke: currentColor; stroke-width: 1.5; flex: none; }
  .location-heading svg.closed { transform: rotate(-90deg); }
  .zone-heading { background: var(--ad-panel); padding: 12px 20px; font-size: 13px; }
  .location-heading .silent { color: var(--ad-amber); }
  .fleet-agent { display: grid; grid-template-columns: minmax(0, 1.4fr) minmax(110px, .7fr) minmax(0, 2fr); gap: 20px; padding: 20px; }
  .fleet-agent:not(:first-child) { border-top: 1px solid var(--ad-line-soft); }
  .fleet-agent.retired { opacity: .65; }
  .agent-identity { display: flex; align-items: flex-start; gap: 12px; min-width: 0; }
  .agent-name { display: block; overflow-wrap: anywhere; font-size: 14px; }
  .agent-signal { width: 7px; height: 7px; border-radius: 50%; background: var(--ad-amber); margin-top: 6px; flex: none; }
  .agent-signal.reporting { background: var(--ad-green); }
  .agent-location, .agent-caption { display: block; color: var(--ad-dim); font-size: 12px; margin-top: 6px; overflow-wrap: anywhere; }
  .agent-connectivity { font-size: 13px; color: var(--ad-amber); }
  .agent-connectivity .reporting { color: var(--ad-green); }
  .agent-metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin: 0; }
  .agent-metrics dt { font-size: 11px; color: var(--ad-dim); }
  .agent-metrics dd { margin: 8px 0 0; font-size: 14px; font-variant-numeric: tabular-nums; }
  .agent-details { grid-column: 1 / -1; color: var(--ad-dim); font-size: 12px; }
  .agent-details summary { cursor: pointer; width: fit-content; }
  .agent-detail-body { display: flex; flex-wrap: wrap; align-items: end; gap: 20px; padding-top: 16px; }
  .agent-detail-body > div { flex: 1 1 220px; min-width: 0; display: grid; gap: 8px; }
  .agent-build { font-family: var(--mono); overflow-wrap: anywhere; }
  .purge-confirm { grid-column: 1 / -1; color: var(--ad-red); font-size: 13px; line-height: 1.6; }
  .confirm-actions, .fleet-more { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
  .fleet-more { margin-top: 20px; }
  .fleet-empty { padding: 24px; color: var(--ad-dim); }
  @media (max-width: 1100px) { .fleet-agent { grid-template-columns: minmax(0, 1fr) auto; } .agent-metrics { grid-column: 1 / -1; } }
  @media (max-width: 600px) { .fleet-agent { padding: 16px; gap: 16px; } .agent-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); } .location-heading { padding: 16px; } .location-heading span { width: 100%; margin-left: 28px; } }
</style>
