<script lang="ts">
  import type { Agent } from '../lib/api';
  import { formatRelativeTime } from '../lib/format';
  import { regionFlag } from '../lib/regionFlag';
  import type { SortKey } from './agents-admin/fleet';
  import { placeName } from './agents-admin/fleet';

  let {
    agents,
    selectedId,
    onSelect,
    sortKey,
    sortDir,
    onSort,
    pending,
    purging,
    purgeError,
    onArm,
    onCancel,
    onConfirmPurge,
  }: {
    agents: Agent[];
    selectedId: string | null;
    onSelect: (id: string | null) => void;
    sortKey: SortKey;
    sortDir: 1 | -1;
    onSort: (key: SortKey) => void;
    pending: string | null;
    purging: boolean;
    purgeError: string | null;
    onArm: (id: string) => void;
    onCancel: () => void;
    onConfirmPurge: (id: string) => void;
  } = $props();

  const selected = $derived(agents.find((a) => a.agent_id === selectedId) ?? null);
  const columns: { key: SortKey; title: string }[] = [
    { key: 'agent', title: 'Agent' },
    { key: 'region', title: 'Region' },
    { key: 'zone', title: 'Zone' },
    { key: 'status', title: 'Connectivity' },
    { key: 'seen', title: 'Last seen' },
    { key: 'rtt', title: 'Poll RTT' },
    { key: 'reporting', title: 'Checks' },
    { key: 'results', title: 'Results / 24h' },
  ];
  function state(a: Agent) { return a.retired ? 'Retired' : a.status === 'up' ? 'Reporting' : 'Silent'; }
</script>

<div class="inventory-layout" class:inspect-open={selected !== null}>
  <section class="inventory-panel" aria-label="Agent inventory">
    <div class="inventory-scroll">
      <table>
        <thead>
          <tr>
            {#each columns as col (col.key)}
              <th scope="col" aria-sort={sortKey === col.key ? (sortDir === 1 ? 'ascending' : 'descending') : 'none'}>
                <button onclick={() => onSort(col.key)}>{col.title}{#if sortKey === col.key}<span aria-hidden="true">{sortDir === 1 ? ' ↑' : ' ↓'}</span>{/if}</button>
              </th>
            {/each}
            <th scope="col">Coordinator</th><th scope="col">Build</th>
          </tr>
        </thead>
        <tbody>
          {#each agents as agent (agent.agent_id)}
            <tr class:selected={selectedId === agent.agent_id} class:retired={agent.retired}>
              <td>
                <button class="agent-link" onclick={() => onSelect(agent.agent_id)} aria-pressed={selectedId === agent.agent_id}>
                  <span class="agent-cube" aria-hidden="true">⬡</span>
                  <span><strong>{agent.agent_id}</strong><small>{placeName(agent.region)} / {placeName(agent.zone)}</small></span>
                </button>
              </td>
              <td><span class="flag" aria-hidden="true">{regionFlag(agent.region) ?? '◎'}</span> {placeName(agent.region)}</td>
              <td>{placeName(agent.zone)}</td>
              <td><span class="state" class:good={agent.status === 'up' && !agent.retired} class:retired={agent.retired}><i></i>{state(agent)}</span></td>
              <td title={new Date(agent.last_seen).toLocaleString()}>{formatRelativeTime(agent.last_seen)}</td>
              <td>{agent.poll_rtt_ms === null ? '—' : agent.poll_rtt_ms + ' ms'}</td>
              <td>{agent.checks_reporting}</td>
              <td>{agent.results_24h.toLocaleString()}</td>
              <td class="mono">{agent.coordinator ?? 'Not reported'}</td>
              <td class="mono" title={agent.version ?? 'Not reported'}>{agent.version ?? '—'}</td>
            </tr>
          {:else}
            <tr><td colspan="10" class="empty">No agents match these filters.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
  {#if selected}
    <aside class="agent-inspector" aria-label={'Agent details for ' + selected.agent_id}>
      <header>
        <span class="agent-cube" aria-hidden="true">⬡</span>
        <div><h3>{selected.agent_id}</h3><p>{placeName(selected.region)} / {placeName(selected.zone)}</p></div>
        <button class="inspector-close" aria-label="Close agent details" onclick={() => onSelect(null)}>×</button>
      </header>
      <div class="inspector-section">
        <h4>Connectivity</h4>
        <span class="state" class:good={selected.status === 'up' && !selected.retired} class:retired={selected.retired}><i></i>{state(selected)}</span>
        <dl>
          <div><dt>Last seen</dt><dd title={new Date(selected.last_seen).toLocaleString()}>{formatRelativeTime(selected.last_seen)}</dd></div>
          <div><dt>Poll RTT</dt><dd>{selected.poll_rtt_ms === null ? 'Not reported' : selected.poll_rtt_ms + ' ms'}</dd></div>
          <div><dt>Checks reporting</dt><dd>{selected.checks_reporting}</dd></div>
          <div><dt>Results / 24h</dt><dd>{selected.results_24h.toLocaleString()}</dd></div>
          <div><dt>Pinned checks</dt><dd>{selected.assigned_checks}</dd></div>
        </dl>
      </div>
      <div class="inspector-section">
        <h4>Placement</h4>
        <dl>
          <div><dt>Region</dt><dd><span aria-hidden="true">{regionFlag(selected.region) ?? '◎'}</span> {placeName(selected.region)}</dd></div>
          <div><dt>Zone</dt><dd>{placeName(selected.zone)}</dd></div>
          <div><dt>Polls coordinator</dt><dd class="mono">{selected.coordinator ?? 'Not reported'}</dd></div>
        </dl>
      </div>
      <div class="inspector-section">
        <h4>Build & capabilities</h4>
        <dl><div><dt>Reported build</dt><dd class="mono">{selected.version ?? 'Not reported'}</dd></div></dl>
        <div class="capabilities">
          {#if selected.kinds === null}
            <span class="hint">Capabilities not reported (legacy agents remain eligible).</span>
          {:else if selected.kinds.length === 0}
            <span class="hint">No capabilities declared.</span>
          {:else}
            {#each selected.kinds as kind (kind)}<span class="capability">{kind}</span>{/each}
          {/if}
        </div>
      </div>
      <div class="inspector-section">
        <h4>Agent data</h4>
        <p class="hint">Purge permanently deletes this agent's recorded results. Checks explicitly pinned to it must be reassigned first.</p>
        {#if pending === selected.agent_id}
          <div role="alert" class="purge-confirm">
            <p>Delete {selected.agent_id} and its recorded results? This cannot be undone. {selected.assigned_checks} checks explicitly reference this agent.</p>
            {#if purgeError}<p class="admin-error">{purgeError}</p>{/if}
            <div class="purge-actions">
              <button class="btn danger" disabled={purging} onclick={() => onConfirmPurge(selected.agent_id)}>{purging ? 'Purging…' : 'Confirm purge'}</button>
              <button class="btn" disabled={purging} onclick={onCancel}>Cancel</button>
            </div>
          </div>
        {:else}
          <button class="btn danger" disabled={purging} onclick={() => onArm(selected.agent_id)}>Purge agent data</button>
        {/if}
      </div>
    </aside>
  {:else}
    <aside class="agent-inspector inspector-empty" aria-label="Agent details">
      <span class="empty-symbol" aria-hidden="true">⬡</span>
      <h3>Select an agent</h3>
      <p>Choose an inventory row to inspect its connectivity, placement, build and capabilities without leaving this page.</p>
    </aside>
  {/if}
</div>

<style>
  .inventory-layout {display:grid;grid-template-columns:minmax(0,1fr) 290px;gap:14px;align-items:start;}
  .inventory-panel,.agent-inspector {min-width:0;background:#101b29;border:1px solid #294056;border-radius:8px;overflow:hidden;}
  .inventory-scroll {max-width:100%;overflow-x:auto;}
  table {width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap;min-width:920px;text-align:left;}
  th {padding:0;background:#0c1928;border-bottom:1px solid #294056;color:#91a8bd;font-size:11px;font-weight:500;}
  th button {font:inherit;color:inherit;text-align:left;padding:12px 9px;width:100%;background:none;border:0;cursor:pointer;white-space:nowrap;}
  th button:hover {color:#b5e7f4;background:#142b3c;}
  th button:focus-visible,.agent-link:focus-visible {outline:2px solid #65d5e5;outline-offset:-2px;}
  th button span {color:#6ad5e5;}
  td {border-top:1px solid #22384d;padding:13px 9px;color:#bdcfe0;}
  tr.selected td {background:#143247;border-top-color:#32647a;}
  tbody tr:hover td {background:#132838;}
  tbody tr.selected:hover td {background:#143247;}
  tr.retired td {opacity:.64;}
  .agent-link {display:flex;align-items:center;gap:9px;text-align:left;border:0;background:none;color:#e9f3f8;font:inherit;padding:0;cursor:pointer;}
  .agent-link span:last-child {display:grid;gap:3px;min-width:0;}
  .agent-link strong {font-size:12px;}.agent-link small {color:#91a7bc;font-size:10px;}
  .agent-cube {display:grid;place-items:center;color:#6dd7e8;font-size:22px;line-height:1;flex:none;}
  .flag {font-size:17px;vertical-align:middle;}
  .state {display:inline-flex;align-items:center;gap:6px;color:#e2b477;background:#2f251d;border:1px solid #745639;border-radius:15px;padding:4px 8px;font-size:11px;}
  .state.good {color:#54d8ae;background:#102e29;border-color:#245b4a;}
  .state.retired {color:#a1afbd;background:#20303e;border-color:#35495d;}
  .state i {width:6px;height:6px;border-radius:50%;background:currentColor;}
  .mono {font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;}
  .empty {padding:30px;text-align:center;color:#91a8bd;}
  .agent-inspector {padding:14px;display:grid;gap:12px;position:sticky;top:14px;max-height:calc(100vh - 45px);overflow-y:auto;}
  .agent-inspector header {display:flex;align-items:center;gap:10px;}
  .agent-inspector header>div {flex:1;min-width:0;}
  h3 {font-size:14px;margin:0;overflow-wrap:anywhere;}
  .agent-inspector header p {margin:3px 0 0;color:#93a9bc;font-size:11px;}
  .inspector-close {background:none;border:0;color:#9db5c8;cursor:pointer;font-size:23px;}
  .inspector-section {border-top:1px solid #294056;padding-top:12px;}
  h4 {margin:0 0 12px;font-size:12px;color:#e1eaf2;}
  dl {display:grid;gap:11px;margin:12px 0 0;}
  dl>div {display:flex;justify-content:space-between;gap:12px;align-items:baseline;font-size:12px;}
  dt {color:#8fa6ba;} dd {margin:0;color:#dceaf4;text-align:right;overflow-wrap:anywhere;min-width:0;}
  .capabilities {display:flex;gap:5px;flex-wrap:wrap;}
  .capability {padding:4px 7px;border:1px solid #31596c;border-radius:12px;color:#8bd8e8;font-size:10px;}
  .hint {font-size:11px;line-height:1.55;color:#91a7bc;}
  .purge-confirm {font-size:11px;color:#e9a4a0;line-height:1.5;}
  .purge-actions {display:flex;flex-wrap:wrap;gap:7px;}
  .inspector-empty {place-items:center;text-align:center;gap:8px;padding:50px 18px;}
  .inspector-empty p {color:#90a8bc;font-size:12px;line-height:1.6;}
  .empty-symbol {font-size:32px;color:#4f8299;}
  @media (max-width:1100px) {.inventory-layout {grid-template-columns:minmax(0,1fr);}.agent-inspector {position:static;max-height:none;}.inspector-empty {display:none;}}
</style>
