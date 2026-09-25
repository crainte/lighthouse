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

{#snippet column(key: SortKey, label: string)}
  <th scope="col" aria-sort={ariaSort(key)}>
    <button class="sort" class:active={sortKey === key} onclick={() => onToggleSort(key)}>
      <span class="label">{label}</span>
      <span class="dir" aria-hidden="true">{sortKey === key ? (sortDir === 1 ? '↑' : '↓') : ''}</span>
    </button>
  </th>
{/snippet}

<!-- A real table with `table-layout: fixed`: the header cells and the body
     cells are the same columns by definition, so no template can be
     changed in one place and not the other, and a group header is one
     full-width cell rather than a second, drifting track list. -->
<div class="admin-rows table-wrap">
  <table class="fleet">
    <colgroup>
      <col />
      <col class="c-region" />
      <col class="c-zone" />
      <col class="c-status" />
      <col class="c-seen" />
      <col class="c-num" />
      <col class="c-num" />
      <col class="c-num" />
      <col class="c-num" />
      <col class="c-act" />
    </colgroup>
    <thead>
      <tr>
        {@render column('agent', 'Agent')}
        {@render column('region', 'Region')}
        {@render column('zone', 'Zone')}
        {@render column('status', 'Status')}
        {@render column('seen', 'Last seen')}
        {@render column('rtt', 'Poll RTT')}
        {@render column('reporting', 'Reporting')}
        {@render column('results', 'Results 24h')}
        {@render column('pinned', 'Pinned')}
        <th scope="col"><span class="vh">Actions</span></th>
      </tr>
    </thead>
    <tbody>
      {#if rendered.length === 0}
        <tr>
          <td class="empty" colspan="10">
            No agent matches these filters.
            <button class="btn" onclick={onClearFilters}>Clear filters</button>
          </td>
        </tr>
      {/if}
      <!-- Keyed on agent_id, which is unique fleet-wide; group rows carry
           an `r:`/`z:` prefix so they cannot collide with an id. -->
      {#each rendered as row (row.key)}
        {#if row.kind === 'region'}
          <tr class="grp region" class:unplaced={row.region === UNKNOWN_LOCATION}>
            <th colspan="10" scope="colgroup">
              <button
                class="grp-toggle"
                aria-expanded={!collapsed[row.key]}
                onclick={() => (collapsed[row.key] = !collapsed[row.key])}
              >
                <span class="chev">{collapsed[row.key] ? '▸' : '▾'}</span>
                <span class="grp-name"
                  >{row.region === UNKNOWN_LOCATION ? 'No location declared' : row.region}</span
                >
                <span class="grp-meta">
                  {row.stat.zones}
                  {row.stat.zones === 1 ? 'zone' : 'zones'} · {row.stat.up} up · {row.stat.count -
                    row.stat.up} silent · {row.stat.results} results/24h
                </span>
              </button>
            </th>
          </tr>
        {:else if row.kind === 'zone'}
          <tr class="grp zone" class:dead={row.zone.up === 0}>
            <th colspan="10" scope="colgroup">
              <button
                class="grp-toggle"
                aria-expanded={!collapsed[row.key]}
                onclick={() => (collapsed[row.key] = !collapsed[row.key])}
              >
                <span class="chev">{collapsed[row.key] ? '▸' : '▾'}</span>
                <span class="grp-name">{placeName(row.zone.zone)}</span>
                {#if row.zone.up === 0}<span class="dead-pill">no live agent</span>{/if}
                <span class="grp-meta">
                  {row.zone.up} up · {row.zone.count - row.zone.up} silent · {row.zone.results}
                  results/24h
                </span>
              </button>
            </th>
          </tr>
        {:else}
          <tr class="agent-row" class:retired={row.agent.retired}>
            <td title={row.agent.version ?? row.agent.agent_id}>
              <span class="agent-pill">{row.agent.agent_id}</span>
              <!-- The build goes under the id rather than in a column of
                   its own: it is read when one agent behaves unlike its
                   neighbours, not scanned down the table. -->
              {#if row.agent.version}
                <span class="agent-build">{row.agent.version}</span>
              {/if}
            </td>
            <td class="dim" class:amber={row.agent.region === UNKNOWN_LOCATION}>
              {placeName(row.agent.region)}
            </td>
            <td class="dim" class:amber={row.agent.zone === UNKNOWN_LOCATION}>
              {placeName(row.agent.zone)}
            </td>
            <td class="state" class:on={row.agent.status === 'up'}>
              {row.agent.retired ? 'Retired' : row.agent.status === 'up' ? 'Reporting' : 'Silent'}
            </td>
            <td class="dim" title={new Date(row.agent.last_seen).toLocaleString()}>
              {formatRelativeTime(row.agent.last_seen)}
            </td>
            <td class="mono dim">
              {row.agent.poll_rtt_ms == null ? '—' : `${row.agent.poll_rtt_ms} ms`}
            </td>
            <td class="mono">{row.agent.checks_reporting}</td>
            <td class="mono">{row.agent.results_24h}</td>
            <td class="mono">{row.agent.assigned_checks}</td>
            <td class="actions">
              <button class="btn danger" onclick={() => onArm(row.agent.agent_id)} disabled={purging}>
                Purge
              </button>
            </td>
          </tr>
          {#if pending === row.agent.agent_id}
            <!-- One full-width cell on purpose: a confirmation wide enough
                 to say what is destroyed would not fit any column. -->
            <tr class="confirm-row">
              <td colspan="9">
                <p class="confirm-copy">
                  Purge <span class="mono">{row.agent.agent_id}</span>? Every result it ever
                  recorded is deleted with it — {row.agent.results_24h} in the last 24 hours,
                  across {row.agent.checks_reporting}
                  {row.agent.checks_reporting === 1 ? 'check' : 'checks'}. Those checks lose that
                  history from their uptime bars and latency graphs.
                  {#if row.agent.assigned_checks > 0}
                    {row.agent.assigned_checks}
                    {row.agent.assigned_checks === 1 ? 'check names' : 'checks name'} this agent
                    explicitly and would be left with nothing to run
                    {row.agent.assigned_checks === 1 ? 'it' : 'them'}.
                  {/if}
                  The agent disappears from this list because the list is those rows. This cannot
                  be undone.
                </p>
                {#if purgeError}<p class="admin-error">{purgeError}</p>{/if}
                <div class="confirm-actions">
                  <button
                    class="btn danger"
                    onclick={() => onConfirmPurge(row.agent.agent_id)}
                    disabled={purging}
                  >
                    {purging ? 'Purging…' : 'Yes, purge it and its results'}
                  </button>
                  <button class="btn" onclick={() => (pending = null)} disabled={purging}>
                    Cancel
                  </button>
                </div>
              </td>
            </tr>
          {/if}
        {/if}
      {/each}
    </tbody>
  </table>
</div>

{#if capped > 0}
  <div class="more">
    <button class="btn" onclick={onShowMore}>
      Show {Math.min(pageSize, capped)} more
    </button>
    <span class="admin-muted">
      {shown} of {shown + capped} shown — the rest are held back so the table stays quick.
    </span>
  </div>
{/if}

<style>
  .vh {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }

  .table-wrap {
    overflow-x: auto;
  }
  /* `fixed` is the whole alignment guarantee: column widths come from the
     colgroup, not from cell content, so a long agent id cannot move a heading
     off the values under it at any viewport width. */
  .fleet {
    width: 100%;
    /* Every other column is a fixed width, so this floor is what the agent id —
       the one column that has to stay readable — is left with before the wrap
       starts scrolling. */
    min-width: 66rem;
    table-layout: fixed;
    border-collapse: collapse;
    font-size: 0.8rem;
  }
  .c-region {
    width: 8rem;
  }
  .c-zone {
    width: 9rem;
  }
  .c-status {
    width: 6rem;
  }
  .c-seen {
    width: 7.5rem;
  }
  .c-num {
    width: 7rem;
  }
  .c-act {
    width: 5.5rem;
  }

  .fleet th,
  .fleet td {
    text-align: left;
    font-weight: 400;
    padding: 0.34rem 0.5rem 0.34rem 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .fleet thead th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--bg-elev);
    border-bottom: 1px solid var(--border);
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }
  .sort {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    background: none;
    border: 0;
    padding: 0;
    font: inherit;
    letter-spacing: inherit;
    text-transform: inherit;
    color: inherit;
    cursor: pointer;
    max-width: 100%;
  }
  .sort:hover {
    color: var(--text);
  }
  .sort.active {
    color: var(--accent);
  }
  .sort .label {
    overflow: hidden;
    text-overflow: ellipsis;
  }
  /* Reserved whether or not this column is the sorted one, so switching sort
     never nudges a heading sideways. */
  .dir {
    display: inline-block;
    width: 0.6rem;
    flex: none;
  }

  .fleet tbody tr.agent-row td {
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  /* Shown but stepped back: a retired agent is history, not an operator. */
  .agent-row.retired td {
    opacity: 0.5;
  }
  .agent-pill {
    display: inline-block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    vertical-align: bottom;
  }
  .agent-build {
    display: block;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.7rem;
    color: var(--text-dim);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .state {
    color: var(--text-dim);
  }
  .state.on {
    color: var(--up);
  }
  .actions {
    text-align: right;
    padding-right: 0;
  }

  /* Group headers are a single spanning cell, so they share the table's columns
     without declaring any of their own. */
  .grp th {
    padding: 0;
  }
  .grp-toggle {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    width: 100%;
    background: none;
    border: 0;
    color: inherit;
    font: inherit;
    text-align: left;
    cursor: pointer;
  }
  .grp-toggle:hover {
    color: var(--text);
  }
  .chev {
    width: 0.7rem;
    flex: none;
    color: var(--text-dim);
  }
  .grp-name {
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .grp-meta {
    margin-left: auto;
    padding-left: 1rem;
    color: var(--text-dim);
    font-size: 0.72rem;
    font-variant-numeric: tabular-nums;
    text-transform: none;
    letter-spacing: 0;
    white-space: nowrap;
  }
  .grp.region th {
    padding: 0.5rem 0 0.25rem;
    border-bottom: 1px solid var(--border);
    font-size: 0.85rem;
    font-weight: 600;
    letter-spacing: 0.02em;
  }
  .grp.zone th {
    padding: 0.3rem 0 0.2rem 0.55rem;
    font-size: 0.76rem;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .grp.zone th .grp-toggle {
    border-left: 2px solid var(--border);
    padding-left: 0.4rem;
    margin-left: -0.4rem;
  }
  /* An unplaced agent is a config gap, so the bucket reads as a warning rather
     than as one more region. */
  .grp.region.unplaced th {
    color: var(--amber);
    border-bottom-color: var(--amber);
  }
  /* The one thing this panel exists to answer. A zone with nothing live means
     its checks produce no results and no error — nothing else would say so. */
  .grp.zone.dead th {
    color: var(--down);
  }
  .grp.zone.dead th .grp-toggle {
    border-left-color: var(--down);
  }
  .dead-pill {
    text-transform: none;
    letter-spacing: 0;
    font-size: 0.68rem;
    padding: 0.05rem 0.4rem;
    border: 1px solid var(--down);
    border-radius: 999px;
  }

  .empty {
    padding: 0.9rem 0;
    color: var(--text-dim);
  }
  .empty .btn {
    margin-left: 0.5rem;
  }

  .confirm-row td {
    padding: 0.5rem 0 0.7rem;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
    white-space: normal;
  }
  .confirm-copy {
    color: var(--down);
    font-size: 0.78rem;
    line-height: 1.5;
    margin: 0 0 0.5rem;
    max-width: 46rem;
  }
  .confirm-actions {
    display: flex;
    gap: 0.4rem;
  }

  .more {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    flex-wrap: wrap;
    margin-top: 0.7rem;
  }

  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .dim {
    color: var(--text-dim);
  }
  .amber {
    color: var(--amber);
  }
</style>
