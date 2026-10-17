<script lang="ts">
  import { onDestroy } from 'svelte';
  import type { ClusterLink, ClusterMember, ClusterResponse } from '../lib/admin';
  import type { Agent } from '../lib/api';
  import { fetchAgents } from '../lib/api';
  import { reportedBuild } from '../lib/admin/cluster';
  import { AdminError, buildIdentity, createAgentCredential, decommissionMember, fetchCluster } from '../lib/admin';
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
  let view = $state<'map' | 'members'>('map');
  // Polling must not overlap: a slow request should make an interval tick a
  // no-op rather than allowing an older response to overwrite a newer one.
  let refreshInFlight = false;

  let agents = $state<Agent[]>([]);
  let agentsError = $state<string | null>(null);
  let selectedNode = $state<string | null>(null);
  let selectedAgent = $state<string | null>(null);
  const aliveMembers = $derived(cluster?.members.filter((m) => m.state === 'alive').length ?? 0);
  // On the global node each region also reports its own coordinators.
  const rosters = $derived(cluster?.regions ?? []);
  const coordinators = $derived(rosters.flatMap((r) => r.members));
  const globalPeers = $derived(cluster?.mode === 'global'
    ? cluster.global_peers?.length ? cluster.global_peers : [cluster.self]
    : []);
  // Global peer members and regional reports are different sets in the API.
  // Deduplicate only the inspector list; never conflate them in the map.
  const members = $derived(cluster
    ? [...new Map([...globalPeers, cluster.self, ...cluster.members, ...coordinators]
      .map((member) => [member.node_id, member] as const)).values()]
    : []);
  const isRegion = (m: ClusterMember) => cluster?.mode === 'global' && cluster.members.some((r) => r.node_id === m.node_id);
  const polling = (nodeId: string) => agents.filter((a) => a.coordinator === nodeId).length;
  /** Every observed link touching a node, each read against the clock of the
   *  node that reported it. */
  function linksFor(nodeId: string): { link: ClusterLink; now: number; staleAfter: number }[] {
    if (!cluster) return [];
    const sources = [
      { links: cluster.links ?? [], now: cluster.now, staleAfter: cluster.link_stale_after_ms ?? 0 },
      { links: cluster.global_links ?? [], now: cluster.now, staleAfter: cluster.global_link_stale_after_ms ?? 0 },
      ...rosters.map((r) => ({ links: r.links, now: r.now ?? cluster!.now, staleAfter: r.link_stale_after_ms ?? 0 })),
    ];
    return sources.flatMap((s) => s.links
      .filter((l) => l.source === nodeId || l.target === nodeId)
      .map((link) => ({ link, now: s.now, staleAfter: s.staleAfter })));
  }
  const selected = $derived(members.find((m) => m.node_id === selectedNode) ?? cluster?.self);
  const agentSelection = $derived(agents.find((a) => a.agent_id === selectedAgent) ?? null);
  const lanes = $derived.by(() => {
    if (!cluster) return [];
    const names = cluster.mode === 'global'
      ? [...new Set([...cluster.members.map((m) => m.node_id), ...agents.map((a) => a.region)])]
      : [...new Set(agents.map((a) => a.region))];
    return names.sort().map((name) => {
      const regionAgents = agents.filter((a) => a.region === name);
      const zones = [...new Set(regionAgents.map((a) => a.zone))].sort().map((zone) => ({ zone, agents: regionAgents.filter((a) => a.zone === zone) }));
      return { name, member: cluster!.members.find((m) => m.node_id === name), roster: rosters.find((r) => r.region === name), agents: regionAgents, zones };
    });
  });
  let onboardingRegion = $state('');
  let onboardingZone = $state('');
  let issuing = $state(false);
  let issueError = $state<string | null>(null);
  let issued = $state<{ region: string; zone: string; token: string } | null>(null);
  // A global node mints in a region it can reach, so only reporting regions.
  const onboardingRegions = $derived(cluster?.mode === 'global'
    ? cluster.members.map((m) => m.node_id).sort()
    : [...new Set(lanes.map((l) => l.name))].sort());

  async function issueAgentToken() {
    const region = onboardingRegion.trim();
    const zone = onboardingZone.trim();
    if (!region || !zone) {
      issueError = 'Choose a region and enter a zone.';
      return;
    }
    issueError = null;
    issuing = true;
    try {
      const minted = await createAgentCredential(zone, region);
      issued = { region, zone, token: minted.token };
      onboardingZone = '';
    } catch (e) {
      issueError = fail(e);
    } finally {
      issuing = false;
    }
  }

  function selectMember(id: string) {
    selectedNode = id;
    selectedAgent = null;
  }

  function selectAgent(id: string) {
    selectedAgent = id;
  }

  function linkBetween(source: string, target: string) {
    return classifyLink(cluster?.links?.find((l) => l.source === source && l.target === target), cluster?.now ?? Date.now(), cluster?.link_stale_after_ms ?? 0);
  }
  const regions = $derived([...new Set(agents.map((a) => a.region))].sort());
  // Builds are compared per coordinator, so skew inside a region shows too.
  const liveMembers = $derived((cluster?.mode === 'global' ? coordinators : cluster?.members ?? []).filter((m) => m.state !== 'dead' && m.state !== 'removed'));
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
      case 'pending':
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
  <div class="admin-panel"><p class="admin-error">Your session expired. Sign in again to view topology.</p><button class="btn" onclick={onexpired}>Sign in again</button></div>
{:else if loading}
  <div class="admin-panel"><p class="admin-muted">Loading topology…</p></div>
{:else if loadError}
  <div class="admin-panel"><p class="admin-error">{loadError}</p><button class="btn" onclick={refresh}>Retry</button></div>
{:else if cluster}
  <section class="cluster-summary" aria-label="Topology summary">
    <div><span>Current node</span><strong>{cluster.self.node_id}</strong><small>{cluster.mode === 'global' ? 'Global coordinator' : 'Regional coordinator'}</small></div>
    <div><span>{cluster.mode === 'global' ? 'Reporting regions' : 'Coordinator members'}</span><strong>{cluster.members.length + (cluster.mode === 'global' ? 0 : 1)}</strong><small>{aliveMembers} {cluster.mode === 'global' ? (aliveMembers === 1 ? 'region connected' : 'regions connected') : (aliveMembers === 1 ? 'peer alive' : 'peers alive')}</small></div>
    <div><span>Agents</span><strong>{agentsError ? '—' : agents.length}</strong><small>{agentsError ? 'Fleet unavailable' : `${agents.filter((a) => a.status === 'up').length} reporting`}</small></div>
    <div><span>Software build</span><strong class="build-label" title={reportedBuild(cluster.self.build) ?? ''}>{reportedBuild(cluster.self.build) ? buildIdentity(reportedBuild(cluster.self.build)!) : 'Not reported'}</strong><small class:mismatch={skewed.length > 0}>{skewed.length ? `${skewed.length} reported ${skewed.length === 1 ? 'build differs' : 'builds differ'}` : !reportedBuild(cluster.self.build) ? 'Current build cannot be compared' : unreported ? `${unreported} ${cluster.mode === 'global' ? 'regional' : 'peer'} ${unreported === 1 ? 'build' : 'builds'} not reported` : comparable.length ? 'Reported builds match' : 'No peer builds to compare'}</small></div>
  </section>

  <div class="section-heading"><div><h3>Deployment map</h3><p>Live coordinator membership and declared agent placement. One Lightkeeper binary, multiple deployment shapes.</p></div><div class="range-switch" role="group" aria-label="Topology view"><button class="range-btn" class:active={view === 'map'} onclick={() => (view = 'map')}>Map</button><button class="range-btn" class:active={view === 'members'} onclick={() => (view = 'members')}>Members</button></div></div>

  <div class="topology-layout">
    <section class="topology-map" aria-label="Deployment map">
      {#if view === 'map'}
        <section class="map-root" aria-label={cluster.mode === 'global' ? 'Global control plane' : 'Regional control plane'}>
          <header class="plane-heading">
            <div class="plane-title"><span class="plane-glyph" aria-hidden="true">◇</span><div><h4>{cluster.mode === 'global' ? 'Global control plane' : 'Regional control plane'}</h4><p>{cluster.mode === 'global' ? 'Peer coordinators sharing the global catalogue' : 'Peer coordinators serving this region'}</p></div></div>
            <span class="cluster-badge">{cluster.mode === 'global' ? globalPeers.length : cluster.members.length + 1} {cluster.mode === 'global' ? (globalPeers.length === 1 ? 'node' : 'nodes') : (cluster.members.length === 0 ? 'node' : 'nodes')} · {cluster.mode === 'global' ? (globalPeers.length > 1 ? 'cluster' : 'standalone') : (cluster.members.length ? 'cluster' : 'standalone')}</span>
          </header>
          <div class="mesh-nodes">
            {#each cluster.mode === 'global' ? globalPeers : [cluster.self, ...cluster.members] as m (m.node_id)}
              <button
                class="node-select plane-peer"
                class:chosen={!agentSelection && selected?.node_id === m.node_id}
                class:polled={agentSelection?.coordinator === m.node_id}
                onclick={() => selectMember(m.node_id)}
                aria-label={m.node_id + ' Coordinator · ' + m.state + (m.node_id === cluster.self.node_id ? ' · This node' : '')}
              >
                <span class="node-dot {tone(m.state)}"></span>
                <strong>{m.node_id}</strong>
                {#if m.node_id === cluster.self.node_id}<span class="current-tag">This node</span>{/if}
                <small>{cluster.mode === 'global' ? 'Global coordinator' : 'Regional coordinator'} · {m.state}</small>
              </button>
            {/each}
          </div>
          {#if cluster.mode !== 'global' && cluster.members.length > 0}
            <div class="link-matrix" style={'--mesh-size: ' + (cluster.members.length + 1)} aria-label="Observed coordinator links">
              <span></span>{#each [cluster.self, ...cluster.members] as target (target.node_id)}<span class="matrix-label">{target.node_id}</span>{/each}
              {#each [cluster.self, ...cluster.members] as source (source.node_id)}
                <span class="matrix-label">{source.node_id}</span>
                {#each [cluster.self, ...cluster.members] as target (target.node_id)}
                  {@const state = source.node_id === target.node_id ? 'self' : linkBetween(source.node_id, target.node_id)}
                  <span class="matrix-cell {state}" title={source.node_id + ' to ' + target.node_id + ': ' + state}></span>
                {/each}
              {/each}
            </div>
          {/if}
        </section>

        <div class="region-cards">
          {#each lanes as lane (lane.name)}
            <article class="region-card">
              <header class="region-heading">
                {#if lane.member}
                  <button class="region-title" class:chosen={!agentSelection && selected?.node_id === lane.member.node_id} onclick={() => selectMember(lane.member!.node_id)}>
                    <span class="node-dot {tone(lane.member.state)}"></span><strong>{lane.name}</strong><span>Region · {lane.member.state}</span>
                  </button>
                {:else}
                  <div class="region-title"><span class="node-dot"></span><strong>{lane.name}</strong><span>Declared agent region</span></div>
                {/if}
                <div class="region-counts">
                  <span>{lane.zones.length} {lane.zones.length === 1 ? 'zone' : 'zones'}</span>
                  <span>{lane.agents.length} {lane.agents.length === 1 ? 'agent' : 'agents'}</span>
                  {#if cluster.mode === 'global' && lane.member}<span class="report-chip {linkState(lane.member)}">Report {linkState(lane.member)}</span>{/if}
                </div>
              </header>
              {#if cluster.mode === 'global'}
                <section class="region-section" aria-label={'Coordinators in ' + lane.name}>
                  <div class="region-section-heading"><h5>Coordinators</h5><span>{lane.roster?.members.length ?? 0}</span></div>
                  {#if lane.roster && !lane.roster.available}
                    <p class="topology-note">Regional coordinator roster unavailable</p>
                  {:else if lane.roster?.members.length}
                    <div class="region-coordinators">
                      {#each lane.roster?.members ?? [] as m (m.node_id)}
                        <button
                          class="coordinator-chip"
                          class:chosen={!agentSelection && selected?.node_id === m.node_id}
                          class:polled={agentSelection?.coordinator === m.node_id}
                          title={m.node_id + ' · ' + m.state}
                          onclick={() => selectMember(m.node_id)}
                        >
                          <span class="node-dot {tone(m.state)}"></span>
                          <span class="coordinator-copy"><strong>{m.node_id}</strong><small>{polling(m.node_id)} polling · {m.state}</small></span>
                        </button>
                      {/each}
                    </div>
                  {:else}
                    <p class="topology-note">No coordinator roster reported</p>
                  {/if}
                </section>
              {/if}
              <section class="region-section" aria-label={'Zones in ' + lane.name}>
                <div class="region-section-heading"><h5>Zones &amp; agents</h5><span>{lane.zones.length}</span></div>
                {#if lane.zones.length}
                  <div class="zone-grid">
                    {#each lane.zones as zone (zone.zone)}
                      <div class="zone-panel">
                        <div class="zone-heading">
                          <strong>{zone.zone}</strong>
                          <span>{zone.agents.filter((a) => a.status === 'up').length}/{zone.agents.length} reporting</span>
                        </div>
                        <div class="agent-list">
                          {#each zone.agents as a (a.region + '-' + a.zone + '-' + a.agent_id)}
                            <button
                              class="agent-node"
                              class:chosen={selectedAgent === a.agent_id}
                              onclick={() => selectAgent(a.agent_id)}
                            >
                              <span class="node-dot" class:green={a.status === 'up'} class:amber={a.status !== 'up'}></span>
                              <span class="agent-copy"><strong>{a.agent_id}</strong><small>{a.status === 'up' ? 'Reporting' : 'Silent'}</small></span>
                            </button>
                          {/each}
                        </div>
                      </div>
                    {/each}
                  </div>
                {:else}
                  <p class="topology-note">{agentsError ? 'Agent data unavailable' : 'No agents reported in this region'}</p>
                {/if}
              </section>
            </article>
          {:else}
            <p class="topology-note">No regions or agents reported. Standalone nodes require no additional tiers.</p>
          {/each}
        </div>
        <p class="topology-note topology-caveat">All roles run the same Lightkeeper binary. Global and regional control planes may be clustered or standalone; a deployment need not include every tier. Select an agent to highlight the coordinator it polls.</p>
      {:else}
        <div class="member-list">
          {#each members as m (m.node_id)}
            <button class="member-row" class:chosen={selected?.node_id === m.node_id} onclick={() => selectMember(m.node_id)}><span class="node-dot {tone(m.state)}"></span><strong>{m.node_id}</strong><span>{isRegion(m) ? 'Region' : 'Coordinator'} · {m.state}</span><small>{reportedBuild(m.build) ? buildIdentity(reportedBuild(m.build)!) : 'Build not reported'}</small></button>
          {/each}
        </div>
      {/if}
    </section>

    <aside class="node-inspector" aria-label="Selected topology details" aria-live="polite">
      {#if agentSelection}
        <span class="topology-tier">Selected agent</span><h3>{agentSelection.agent_id}</h3>
        <dl><div><dt>Connectivity</dt><dd><span class="opill {agentSelection.status === 'up' ? 'green' : 'amber'}">{agentSelection.status === 'up' ? 'reporting' : 'silent'}</span></dd></div><div><dt>Location</dt><dd>{agentSelection.region} / {agentSelection.zone}</dd></div><div><dt>Last seen</dt><dd>{formatRelativeTime(agentSelection.last_seen)}</dd></div><div><dt>Checks reporting</dt><dd>{agentSelection.checks_reporting}</dd></div><div><dt>Reported build</dt><dd>{agentSelection.version ?? 'Not reported'}</dd></div><div><dt>Polls coordinator</dt><dd>{agentSelection.coordinator ?? 'Not reported'}</dd></div></dl>
      {:else if selected}
        <span class="topology-tier">{selected.node_id === cluster.self.node_id ? 'Current node' : isRegion(selected) ? 'Selected region' : 'Selected coordinator'}</span><h3>{selected.node_id}</h3>
        <dl><div><dt>Membership state</dt><dd><span class="opill {tone(selected.state)}">{selected.state}</span></dd></div><div><dt>Endpoint</dt><dd>{selected.url || 'Not reported'}</dd></div><div><dt>Last observed</dt><dd title={formatAbsolute(selected.last_seen)}>{formatRelativeTime(selected.last_seen, cluster.now)}</dd></div><div><dt>Build</dt><dd>{reportedBuild(selected.build) ?? 'Not reported'}</dd></div>
        {#if selected.origin}<div><dt>Reporting origin</dt><dd>{selected.origin}</dd></div>{/if}
        {#if cluster.mode !== 'global' || coordinators.some((c) => c.node_id === selected!.node_id)}<div><dt>Generation</dt><dd>{selected.generation}</dd></div><div><dt>Agents polling</dt><dd>{polling(selected.node_id)}</dd></div>{/if}
        </dl>
        {#if cluster.mode !== 'global' && selected.node_id !== cluster.self.node_id && selected.state !== 'removed'}
          {#if confirmDecommission === selected.node_id}
            <p class="topology-note">Decommission {selected.node_id}? Every member stops contacting it, and it cannot rejoin under this node id.</p>
            <div class="inspector-actions"><button class="btn danger" onclick={() => decommission(selected!.node_id)}>Decommission</button><button class="btn" onclick={() => (confirmDecommission = null)}>Cancel</button></div>
          {:else}
            <button class="btn danger" onclick={() => (confirmDecommission = selected!.node_id)}>Decommission…</button>
          {/if}
          {#if decommissionError}<p class="admin-error">{decommissionError}</p>{/if}
        {/if}
        <h4>Observed connections</h4>
        {#each linksFor(selected.node_id) as { link, now, staleAfter } (`${link.source}-${link.target}`)}
          <div class="connection-row"><span>{link.source} → {link.target}</span><strong>{classifyLink(link, now, staleAfter)}</strong><small>{formatRelativeTime(link.observed_at, now)}</small></div>
        {:else}<p class="topology-note">No connection observations reported.</p>{/each}
      {/if}
    </aside>
  </div>
  {#if agentsError}<p class="admin-error">Could not load agent placement: {agentsError}</p>{/if}

  <div class="section-heading"><div><h3>Expand the deployment</h3><p>Register regional coordinators, then issue zone-scoped credentials for the agents that will report into them.</p></div></div>
  <section class="onboarding" aria-label="Deployment onboarding">
    <ol class="onboarding-steps"><li><span>1</span>Register region</li><li><span>2</span>Configure coordinator</li><li><span>3</span>Issue agent token</li><li><span>4</span>Verify reports</li></ol>
    <div class="onboarding-grid">
      <div class="onboarding-card">
        <div class="card-heading"><span class="step-number">1</span><div><h4>Region registration</h4><p>Creates the key a regional coordinator uses to report to the global tier.</p></div></div>
        {#if cluster.mode === 'global'}<RegionsAdmin {onexpired} />{:else}<p class="topology-note">Region registration is managed from the global coordinator. This regional coordinator authorizes agents for its own zones.</p>{/if}
      </div>
      <div class="onboarding-card">
        <div class="card-heading"><span class="step-number">3</span><div><h4>Agent token</h4><p>Credentials are scoped to one region and zone. The secret is shown once.</p></div></div>
        <form class="token-form" onsubmit={(e) => (e.preventDefault(), issueAgentToken())}>
          <label class="field"><span class="field-label">Region</span><select bind:value={onboardingRegion}><option value="">Choose region</option>{#each onboardingRegions as region (region)}<option value={region}>{region}</option>{/each}</select></label>
          <label class="field"><span class="field-label">Zone</span><input bind:value={onboardingZone} placeholder="e.g. us-east-1a" /></label>
          <button class="btn primary" type="submit" disabled={issuing || !onboardingRegion || !onboardingZone.trim()}>{issuing ? 'Issuing…' : 'Issue token'}</button>
        </form>
        {#if issueError}<p class="admin-error">{issueError}</p>{/if}
        {#if issued}
          <div class="issued-token" role="status"><strong>{`Copy the token for ${issued.region} / ${issued.zone} now`}</strong><pre>[agent]{'\n'}region = "{issued.region}"{'\n'}zone = "{issued.zone}"{'\n'}token = "{issued.token}"</pre><button class="btn" type="button" onclick={() => (issued = null)}>Dismiss</button></div>
        {/if}
      </div>
    </div>
  </section>
{/if}

<style>
  .cluster-summary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 1px; overflow: hidden; border: 1px solid var(--ad-line); border-radius: 9px; background: var(--ad-line); }
  .cluster-summary > div { display: grid; align-content: start; gap: 10px; min-width: 0; padding: 20px; background: var(--ad-panel); }
  .cluster-summary span, .cluster-summary small { color: var(--ad-dim); font-size: 12px; line-height: 1.5; }
  .cluster-summary strong { font-size: 24px; font-weight: 500; overflow-wrap: anywhere; }
  .cluster-summary .build-label { font-size: 15px; }
  .cluster-summary .mismatch { color: var(--ad-amber); }
  .topology-layout { display: grid; grid-template-columns: minmax(0, 1fr) 300px; gap: 20px; align-items: start; }
  .topology-map, .node-inspector, .onboarding-card { min-width: 0; padding: 24px; background: var(--ad-panel); border: 1px solid var(--ad-line); border-radius: 9px; }
  .node-inspector { position: sticky; top: 84px; }
  .map-root { position: relative; padding: 22px; background: linear-gradient(140deg, #102035 0%, #0b1520 72%); border: 1px solid #29475f; border-radius: 9px; }
  .plane-heading, .plane-title, .region-heading, .region-title, .region-counts, .region-section-heading, .zone-heading { display: flex; align-items: center; }
  .plane-heading, .region-heading, .zone-heading { justify-content: space-between; gap: 12px; }
  .plane-title { gap: 12px; min-width: 0; }
  .plane-glyph { display: grid; place-items: center; flex: none; width: 37px; height: 37px; color: var(--ad-cyan); background: #112940; border: 1px solid #284863; border-radius: 9px; font-size: 24px; }
  .plane-heading h4 { margin: 0; color: var(--ad-text); font-size: 14px; font-weight: 600; }
  .plane-heading p { margin: 5px 0 0; color: var(--ad-dim); font-size: 11px; }
  .cluster-badge { flex: none; padding: 6px 10px; color: #a4d8f1; background: #112940; border: 1px solid #294a64; border-radius: 20px; font-size: 11px; }
  .mesh-nodes { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 170px), 1fr)); gap: 12px; margin-top: 18px; }
  .node-select { display: grid; grid-template-columns: 8px minmax(0, 1fr) auto; align-items: center; gap: 6px 10px; width: 100%; padding: 14px; color: var(--ad-text); text-align: left; background: var(--ad-panel); border: 1px solid var(--ad-line); border-radius: 7px; cursor: pointer; font: inherit; }
  .plane-peer strong { min-width: 0; overflow-wrap: anywhere; font-size: 13px; }
  .plane-peer small { grid-column: 2 / -1; color: var(--ad-dim); font-size: 11px; }
  .current-tag { padding: 3px 6px; color: var(--ad-cyan); border: 1px solid #24688b; border-radius: 12px; font-size: 10px; white-space: nowrap; }
  .chosen { border-color: var(--ad-cyan) !important; box-shadow: 0 0 0 1px rgba(86, 212, 228, .18); }
  .node-dot { width: 7px; height: 7px; flex: none; border-radius: 50%; background: var(--ad-faint); }
  .node-dot.green { background: var(--ad-green); } .node-dot.amber { background: var(--ad-amber); } .node-dot.red { background: var(--ad-red); }
  .link-matrix { display: grid; grid-template-columns: minmax(70px, auto) repeat(var(--mesh-size), minmax(28px, 1fr)); gap: 6px; margin-top: 20px; overflow-x: auto; color: var(--ad-dim); font-size: 10px; }
  .matrix-label { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .matrix-cell { min-width: 28px; height: 18px; border-radius: 3px; background: var(--ad-line); }
  .matrix-cell.connected { background: #285145; } .matrix-cell.failed { background: #56323e; } .matrix-cell.stale { background: #51422c; } .matrix-cell.self { background: repeating-linear-gradient(135deg, #142538 0 4px, #0b1520 4px 8px); }
  .region-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 440px), 1fr)); gap: 16px; margin-top: 22px; }
  .region-card { min-width: 0; padding: 16px; background: #0d1a29; border: 1px solid #284158; border-radius: 9px; }
  .region-heading { align-items: flex-start; flex-wrap: wrap; padding-bottom: 15px; border-bottom: 1px solid var(--ad-line); }
  .region-title { gap: 9px; min-width: 0; padding: 4px 6px; color: var(--ad-text); border: 1px solid transparent; border-radius: 6px; background: transparent; font: inherit; }
  button.region-title { cursor: pointer; }
  .region-title strong { font-size: 15px; }
  .region-title > span:last-child { color: var(--ad-dim); font-size: 11px; }
  .region-counts { justify-content: flex-end; flex-wrap: wrap; gap: 8px; color: var(--ad-dim); font-size: 11px; }
  .region-counts > span { white-space: nowrap; }
  .report-chip { padding: 4px 7px; background: #101f2c; border: 1px solid var(--ad-line); border-radius: 12px; }
  .report-chip.connected { color: var(--ad-green); }
  .report-chip.failed { color: var(--ad-red); }
  .report-chip.stale { color: var(--ad-amber); }
  .region-section { margin-top: 16px; }
  .region-section-heading { justify-content: space-between; gap: 10px; margin-bottom: 10px; }
  .region-section-heading h5 { margin: 0; color: #9dd7ee; font-size: 12px; font-weight: 600; }
  .region-section-heading > span { padding: 2px 7px; background: #152b40; border: 1px solid #28475c; border-radius: 12px; color: var(--ad-dim); font-size: 11px; }
  .region-coordinators { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 138px), 1fr)); gap: 8px; }
  .coordinator-chip, .agent-node { display: flex; align-items: flex-start; gap: 9px; min-width: 0; width: 100%; padding: 11px; color: var(--ad-text); text-align: left; background: #12243a; border: 1px solid #2a405a; border-radius: 6px; cursor: pointer; font: inherit; }
  .coordinator-chip .node-dot, .agent-node .node-dot { margin-top: 5px; }
  .coordinator-copy, .agent-copy { display: grid; gap: 4px; min-width: 0; }
  .coordinator-copy strong, .agent-copy strong { min-width: 0; font-size: 12px; font-weight: 550; overflow-wrap: anywhere; }
  .coordinator-copy small, .agent-copy small { color: var(--ad-dim); font-size: 10px; overflow-wrap: anywhere; }
  .zone-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 205px), 1fr)); gap: 10px; }
  .zone-panel { min-width: 0; padding: 10px; background: #0c1929; border: 1px solid #284057; border-radius: 7px; }
  .zone-heading { margin-bottom: 10px; }
  .zone-heading strong { min-width: 0; overflow-wrap: anywhere; font-size: 12px; font-weight: 550; }
  .zone-heading span { color: var(--ad-dim); font-size: 10px; white-space: nowrap; }
  .agent-list { display: grid; gap: 6px; }
  .polled { border-color: var(--ad-green) !important; box-shadow: 0 0 0 1px rgba(80, 200, 140, .25); }
  .topology-note { color: var(--ad-dim); font-size: 12px; line-height: 1.65; }
  .topology-caveat { margin: 24px 0 0; padding-top: 20px; border-top: 1px solid var(--ad-line); }
  .node-inspector h3 { margin: 0; overflow-wrap: anywhere; font-size: 18px; font-weight: 550; }
  .node-inspector dl { margin: 24px 0; }
  .node-inspector dl > div { margin-bottom: 18px; }
  .node-inspector dt { color: var(--ad-dim); font-size: 11px; }
  .node-inspector dd { margin: 8px 0 0; overflow-wrap: anywhere; font-size: 12px; line-height: 1.6; }
  .node-inspector h4, .onboarding-card h4 { margin: 0; font-size: 14px; font-weight: 550; }
  .inspector-actions { display: flex; flex-wrap: wrap; gap: 8px; }
  .connection-row { display: grid; gap: 8px; padding: 12px 0; overflow-wrap: anywhere; border-top: 1px solid var(--ad-line); font-size: 12px; }
  .connection-row small { color: var(--ad-dim); }
  .member-list { display: grid; gap: 12px; }
  .member-row { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; padding: 16px; overflow-wrap: anywhere; color: var(--ad-text); text-align: left; background: none; border: 1px solid var(--ad-line); border-radius: 7px; cursor: pointer; font: inherit; }
  .member-row small { width: 100%; color: var(--ad-dim); }
  .onboarding-steps { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 1px; margin: 0 0 20px; padding: 0; overflow: hidden; list-style: none; background: var(--ad-line); border: 1px solid var(--ad-line); border-radius: 8px; }
  .onboarding-steps li { display: flex; align-items: center; gap: 10px; padding: 14px 16px; color: var(--ad-dim); background: #0d1723; font-size: 12px; }
  .onboarding-steps span, .step-number { display: grid; place-items: center; width: 24px; height: 24px; flex: none; color: var(--ad-cyan); border: 1px solid #2b475b; border-radius: 50%; font-size: 11px; }
  .onboarding-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; }
  .card-heading { display: flex; gap: 14px; margin-bottom: 20px; }
  .card-heading p { margin: 8px 0 0; color: var(--ad-dim); font-size: 12px; line-height: 1.6; }
  .token-form { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto; align-items: end; gap: 12px; margin-top: 18px; }
  .issued-token { margin-top: 20px; padding: 16px; border: 1px solid var(--ad-green); border-radius: 7px; font-size: 12px; }
  .issued-token pre { padding: 14px; overflow-wrap: anywhere; white-space: pre-wrap; background: var(--ad-bg); border-radius: 6px; user-select: all; }
  @media (max-width: 1180px) { .topology-layout, .onboarding-grid { grid-template-columns: minmax(0, 1fr); } .node-inspector { position: static; } }
  @media (max-width: 900px) { .cluster-summary { grid-template-columns: repeat(2, minmax(0, 1fr)); } .onboarding-steps { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  @media (max-width: 600px) { .topology-map, .node-inspector, .onboarding-card { padding: 16px; } .map-root { padding: 14px; } .region-card { padding: 12px; } .cluster-summary > div { padding: 16px; } .cluster-summary strong { font-size: 20px; } .token-form { grid-template-columns: 1fr; } .onboarding-steps { grid-template-columns: 1fr; } }
</style>
