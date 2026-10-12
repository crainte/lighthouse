<script lang="ts">
  import { onDestroy } from 'svelte';
  import type { ClusterMember, ClusterResponse } from '../lib/admin';
  import type { Agent } from '../lib/api';
  import { fetchAgents } from '../lib/api';
  import { reportedBuild } from '../lib/admin/cluster';
  import { AdminError, buildIdentity, decommissionMember, fetchCluster } from '../lib/admin';
  import RegionsAdmin from './RegionsAdmin.svelte';
  import { classifyLink } from '../lib/clusterGraph';
  import { formatRelativeTime } from '../lib/format';
  import { formatAbsolute } from '../lib/incident';

  let { onexpired }: { onexpired?: () => void } = $props();

  // Gossip rounds settle on the order of a few seconds; polling faster would
  // just repaint the same generation/last_seen pair.
  const REFRESH_MS = 5000;

  let cluster = $state<ClusterResponse | null>(null);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let expired = $state(false);
  let view = $state<'graph' | 'table'>('graph');
  // Polling must not overlap: a slow request should make an interval tick a
  // no-op rather than allowing an older response to overwrite a newer one.
  let refreshInFlight = false;

  let agents = $state<Agent[]>([]);
  let agentsError = $state<string | null>(null);
  let selectedNode = $state<string | null>(null);
  const members = $derived(cluster ? [cluster.self, ...cluster.members] : []);
  const selected = $derived(members.find((m) => m.node_id === selectedNode) ?? cluster?.self);
  const regions = $derived([...new Set(agents.map((a) => a.region))].sort());
  const liveMembers = $derived(cluster?.members.filter((m) => m.state !== 'dead' && m.state !== 'removed') ?? []);
  let confirmDecommission = $state<string | null>(null);
  let decommissionError = $state<string | null>(null);

  async function decommission(nodeId: string) {
    try {
      await decommissionMember(nodeId);
      confirmDecommission = null;
      decommissionError = null;
      await refresh();
    } catch (e) {
      decommissionError = fail(e);
    }
  }
  const comparable = $derived(liveMembers.filter((m) => reportedBuild(m.build)));
  const skewed = $derived(reportedBuild(cluster?.self.build)
    ? comparable.filter((m) => buildIdentity(reportedBuild(m.build)!) !== buildIdentity(reportedBuild(cluster?.self.build)!))
    : []);
  const unreported = $derived(liveMembers.length - comparable.length);
  function linkState(member: ClusterMember) {
    const link = cluster?.links?.find((l) => l.source === member.node_id && l.target === cluster?.self.node_id);
    return classifyLink(link, cluster?.now ?? Date.now(), cluster?.link_stale_after_ms ?? 0);
  }

  function tone(state: ClusterMember['state']): 'green' | 'amber' | 'red' | '' {
    switch (state) {
      case 'alive':
        return 'green';
      case 'suspect':
        return 'amber';
      case 'dead':
        return 'red';
      case 'removed':
        return '';
    }
  }

  function fail(e: unknown): string {
    const message = e instanceof Error ? e.message : String(e);
    if (e instanceof AdminError && e.unauthorized) {
      expired = true;
      onexpired?.();
    }
    return message;
  }

  async function refresh() {
    if (refreshInFlight) return;
    refreshInFlight = true;
    try {
      const [membership, fleet] = await Promise.allSettled([fetchCluster(), fetchAgents()]);
      if (membership.status === 'rejected') throw membership.reason;
      cluster = membership.value;
      if (fleet.status === 'fulfilled') {
        agents = fleet.value.agents;
        agentsError = null;
      } else {
        agentsError = fail(fleet.reason);
      }
      loadError = null;
    } catch (e) {
      loadError = fail(e);
    } finally {
      refreshInFlight = false;
      loading = false;
    }
  }

  refresh();
  // Cleaned up on destroy: a leaked timer here would keep hitting the
  // coordinator with an authenticated poll long after the panel closed.
  const timer = setInterval(refresh, REFRESH_MS);
  onDestroy(() => clearInterval(timer));
</script>

{#if expired}
  <p class="admin-error">Your session expired. Sign in again to view the cluster.</p>
  <button class="btn" onclick={onexpired}>Sign in again</button>
{:else if loading}
  <p class="admin-muted">Loading cluster…</p>
{:else if loadError}
  <p class="admin-error">{loadError}</p><button class="btn" onclick={refresh}>Retry</button>
{:else if cluster}
  <div class="cluster-summary">
    <div><span>Current node</span><strong>{cluster.self.node_id}</strong><small>{cluster.mode === 'global' ? 'Global coordinator' : 'Regional coordinator'}</small></div>
    <div><span>{cluster.mode === 'global' ? 'Reporting regions' : 'Coordinator members'}</span><strong>{cluster.members.length + (cluster.mode === 'global' ? 0 : 1)}</strong><small>{cluster.members.filter((m) => m.state === 'alive').length} {cluster.mode === 'global' ? 'regions connected' : 'peers alive'}</small></div>
    <div><span>Agents</span><strong>{agentsError ? '—' : agents.length}</strong><small>{agentsError ? 'Fleet unavailable' : `${agents.filter((a) => a.status === 'up').length} reporting`}</small></div>
    <div><span>Software build</span><strong class="build-label" title={reportedBuild(cluster.self.build) ?? ''}>{reportedBuild(cluster.self.build) ? buildIdentity(reportedBuild(cluster.self.build)!) : 'Not reported'}</strong><small class:mismatch={skewed.length > 0}>{skewed.length ? `${skewed.length} reported builds differ` : !reportedBuild(cluster.self.build) ? 'Current build cannot be compared' : unreported ? `${unreported} ${cluster.mode === 'global' ? 'regional' : 'peer'} builds not reported` : comparable.length ? 'Reported builds match' : 'No peer builds to compare'}</small></div>
  </div>
  <div class="topology-toolbar"><div><h3>Deployment topology</h3><p>{cluster.mode === 'global' ? 'Regional reports and agent placement, as observed by this global node.' : 'Coordinator membership and the agents reporting in this region.'}</p></div><div class="range-switch" role="group" aria-label="Cluster view"><button class="range-btn" class:active={view === 'graph'} onclick={() => (view = 'graph')}>Topology</button><button class="range-btn" class:active={view === 'table'} onclick={() => (view = 'table')}>Members</button></div></div>
  <div class="topology-layout">
    <section class="topology-map" aria-label="Deployment topology">
      {#if view === 'graph'}
        <div class="topology-root"><span class="topology-tier">{cluster.mode === 'global' ? 'Global coordinator' : 'Regional cluster'}</span><button class="node-select" class:chosen={selected?.node_id === cluster.self.node_id} onclick={() => (selectedNode = cluster!.self.node_id)}><span class="node-dot {tone(cluster.self.state)}"></span><strong>{cluster.self.node_id}</strong><span>This node</span></button></div>
        <div class="topology-branches">
          {#each cluster.members as m (m.node_id)}
            <div class="topology-branch">
              <span class="branch-relation">{cluster.mode === 'global' ? `Report ${linkState(m)}` : 'Cluster member'}</span>
              <button class="node-select" class:chosen={selected?.node_id === m.node_id} onclick={() => (selectedNode = m.node_id)}><span class="node-dot {tone(m.state)}"></span><strong>{m.node_id}</strong><span>{cluster.mode === 'global' ? 'Region' : 'Coordinator'} · {m.state}</span></button>
              {#if cluster.mode === 'global'}
                <div class="regional-agents">
                  {#each agents.filter((a) => a.region === m.node_id) as a (`${a.region}-${a.zone}-${a.agent_id}`)}
                    <div class="topology-agent"><span class="node-dot" class:green={a.status === 'up'} class:amber={a.status !== 'up'}></span><span><strong>{a.agent_id}</strong><small>{a.zone} · {a.status === 'up' ? 'reporting' : 'silent'}</small></span></div>
                  {:else}<p class="topology-note">{agentsError ? 'Agent data unavailable' : 'No agents reported for this region'}</p>{/each}
                </div>
              {/if}
            </div>
          {/each}
        </div>
        {#if cluster.mode !== 'global' || agents.some((a) => !cluster!.members.some((m) => m.node_id === a.region))}
          <div class="placement-heading">Agent placement <span>Location grouping, not individual coordinator connections</span></div>
          <div class="placement-grid">
            {#each regions.filter((region) => cluster!.mode !== 'global' || !cluster!.members.some((m) => m.node_id === region)) as region (region)}
              <div class="placement-region"><strong>{region}</strong>
                {#each agents.filter((a) => a.region === region) as a (`${a.region}-${a.zone}-${a.agent_id}`)}
                  <div class="topology-agent"><span class="node-dot" class:green={a.status === 'up'} class:amber={a.status !== 'up'}></span><span><strong>{a.agent_id}</strong><small>{a.zone} · {a.status === 'up' ? 'reporting' : 'silent'}</small></span></div>
                {/each}
              </div>
            {:else}<p class="topology-note">{agentsError ? 'Agent data unavailable' : 'No agents reported'}</p>{/each}
          </div>
        {/if}
        <p class="topology-note topology-caveat">{cluster.mode === 'global' ? 'Each region represents a reporting origin, not its full coordinator roster. Individual regional members and agent-to-coordinator connections are not exposed by this API.' : 'Agents are grouped by declared location. Their individual coordinator connections are not exposed by this API.'}</p>
      {:else}
        <div class="member-list">
          {#each members as m (m.node_id)}
            <button class="member-row" class:chosen={selected?.node_id === m.node_id} onclick={() => (selectedNode = m.node_id)}><span class="node-dot {tone(m.state)}"></span><strong>{m.node_id}</strong><span>{m.state}</span><small>{reportedBuild(m.build) ? buildIdentity(reportedBuild(m.build)!) : 'Build not reported'}</small></button>
          {/each}
        </div>
      {/if}
    </section>
    <aside class="node-inspector" aria-label="Selected node details" aria-live="polite">
      {#if selected}
        <span class="topology-tier">{selected.node_id === cluster.self.node_id ? 'Current node' : cluster.mode === 'global' ? 'Selected region' : 'Selected coordinator'}</span><h3>{selected.node_id}</h3>
        <dl><div><dt>Membership state</dt><dd><span class="opill {tone(selected.state)}">{selected.state}</span></dd></div><div><dt>Endpoint</dt><dd>{selected.url || 'Not reported'}</dd></div><div><dt>Last observed</dt><dd title={formatAbsolute(selected.last_seen)}>{formatRelativeTime(selected.last_seen, cluster.now)}</dd></div><div><dt>Build</dt><dd>{reportedBuild(selected.build) ?? 'Not reported'}</dd></div>
        {#if selected.build?.startsWith('origin ')}<div><dt>Reporting origin</dt><dd>{selected.build.slice(7)}</dd></div>{/if}
        {#if cluster.mode !== 'global'}<div><dt>Generation</dt><dd>{selected.generation}</dd></div>{/if}
        </dl>
        {#if cluster.mode !== 'global' && selected.node_id !== cluster.self.node_id && selected.state !== 'removed'}
          {#if confirmDecommission === selected.node_id}
            <p class="topology-note">Decommission {selected.node_id}? Every member stops contacting it, and it cannot rejoin under this node id.</p>
            <button class="btn danger" onclick={() => decommission(selected!.node_id)}>Decommission</button>
            <button class="btn" onclick={() => (confirmDecommission = null)}>Cancel</button>
          {:else}
            <button class="btn danger" onclick={() => (confirmDecommission = selected!.node_id)}>Decommission…</button>
          {/if}
          {#if decommissionError}<p class="admin-error">{decommissionError}</p>{/if}
        {/if}
        <h4>Observed connections</h4>
        {#each (cluster.links ?? []).filter((l) => l.source === selected!.node_id || l.target === selected!.node_id) as link (`${link.source}-${link.target}`)}
          <div class="connection-row"><span>{link.source} → {link.target}</span><strong>{classifyLink(link, cluster.now, cluster.link_stale_after_ms ?? 0)}</strong><small>{formatRelativeTime(link.observed_at, cluster.now)}</small></div>
        {:else}<p class="topology-note">No connection observations reported.</p>{/each}
      {/if}
    </aside>
  </div>
  {#if agentsError}<p class="admin-error">Could not load agent placement: {agentsError}</p>{/if}
  {#if cluster.mode === 'global'}<RegionsAdmin {onexpired} />{/if}
{/if}

<style>
  .cluster-summary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); border: 1px solid var(--ad-line); border-radius: 9px; background: var(--ad-panel); }
  .cluster-summary > div { display: grid; align-content: start; gap: 10px; padding: 20px; min-width: 0; }
  .cluster-summary > div:not(:first-child) { border-left: 1px solid var(--ad-line); }
  .cluster-summary span, .cluster-summary small { color: var(--ad-dim); font-size: 12px; line-height: 1.5; }
  .cluster-summary strong { font-size: 24px; overflow-wrap: anywhere; }
  .cluster-summary .build-label { font-size: 15px; }
  .cluster-summary .mismatch { color: var(--ad-amber); }
  .topology-toolbar { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px; margin: 28px 0 16px; }
  h3 { margin: 0; font-size: 16px; font-weight: 550; }
  .topology-toolbar p { margin: 8px 0 0; font-size: 12px; color: var(--ad-dim); }
  .topology-layout { display: grid; grid-template-columns: minmax(0, 1fr) 280px; gap: 20px; align-items: start; }
  .topology-map, .node-inspector { background: var(--ad-panel); border: 1px solid var(--ad-line); border-radius: 9px; padding: 24px; min-width: 0; }
  .topology-root { max-width: 340px; margin: 0 auto 28px; }
  .topology-tier { display: block; color: var(--ad-dim); font-size: 11px; margin-bottom: 12px; }
  .node-select { display: grid; grid-template-columns: 8px minmax(0, 1fr); align-items: center; gap: 10px 12px; text-align: left; border: 1px solid var(--ad-line); border-radius: 7px; padding: 16px; width: 100%; background: var(--ad-bg); color: var(--ad-text); cursor: pointer; }
  .node-select strong { overflow-wrap: anywhere; font-size: 14px; }
  .node-select > span:last-child { grid-column: 2; font-size: 11px; color: var(--ad-dim); }
  .chosen { border-color: var(--ad-cyan) !important; }
  .node-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--ad-faint); flex: none; }
  .node-dot.green { background: var(--ad-green); } .node-dot.amber { background: var(--ad-amber); } .node-dot.red { background: var(--ad-red); }
  .topology-branches, .placement-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 220px), 1fr)); gap: 20px; }
  .topology-branch { min-width: 0; border-top: 1px solid var(--ad-line); padding-top: 16px; }
  .branch-relation { display: block; color: var(--ad-dim); font-size: 11px; margin-bottom: 12px; }
  .regional-agents { margin: 12px 0 0 12px; padding-left: 16px; border-left: 1px solid var(--ad-line); }
  .topology-agent { display: flex; align-items: baseline; gap: 10px; padding: 12px 0; font-size: 12px; }
  .topology-agent strong { overflow-wrap: anywhere; }
  .topology-agent small { display: block; margin-top: 6px; color: var(--ad-dim); }
  .placement-heading { font-size: 13px; margin: 28px 0 16px; }
  .placement-heading span { display: block; color: var(--ad-dim); font-size: 11px; margin-top: 6px; }
  .placement-region { border: 1px solid var(--ad-line); border-radius: 7px; padding: 16px; font-size: 13px; min-width: 0; }
  .topology-note { color: var(--ad-dim); font-size: 12px; line-height: 1.65; }
  .topology-caveat { border-top: 1px solid var(--ad-line); margin: 24px 0 0; padding-top: 20px; }
  .node-inspector h3 { overflow-wrap: anywhere; }
  .node-inspector dl { margin: 24px 0; }
  .node-inspector dl > div { margin-bottom: 20px; }
  .node-inspector dt { color: var(--ad-dim); font-size: 11px; }
  .node-inspector dd { margin: 8px 0 0; font-size: 12px; overflow-wrap: anywhere; line-height: 1.6; }
  .node-inspector h4 { font-size: 13px; font-weight: 500; }
  .connection-row { display: grid; gap: 8px; padding: 12px 0; border-top: 1px solid var(--ad-line); font-size: 12px; overflow-wrap: anywhere; }
  .connection-row small { color: var(--ad-dim); }
  .member-list { display: grid; gap: 12px; }
  .member-row { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; padding: 16px; border: 1px solid var(--ad-line); border-radius: 7px; color: var(--ad-text); background: none; cursor: pointer; text-align: left; overflow-wrap: anywhere; }
  .member-row small { width: 100%; color: var(--ad-dim); }
  @media (max-width: 1100px) { .topology-layout { grid-template-columns: minmax(0, 1fr); } .cluster-summary { grid-template-columns: repeat(2, minmax(0, 1fr)); } .cluster-summary > div { border-bottom: 1px solid var(--ad-line); } .cluster-summary > div:nth-child(3) { border-left: 0; } }
  @media (max-width: 600px) { .topology-map, .node-inspector { padding: 16px; } .cluster-summary > div { padding: 16px; } .cluster-summary strong { font-size: 20px; } }
</style>
