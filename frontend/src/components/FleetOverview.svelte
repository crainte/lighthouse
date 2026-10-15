<script lang="ts">
  import { onDestroy } from 'svelte';
  import type { Agent } from '../lib/api';
  import { fetchAgents, UNKNOWN_LOCATION } from '../lib/api';
  import { AdminError, fetchCluster, type ClusterResponse, type ClusterMember } from '../lib/admin';
  import { formatRelativeTime } from '../lib/format';

  let { mode = 'overview', onNavigate, onexpired }: {
    mode?: 'overview' | 'regions';
    onNavigate: (tab: 'overview' | 'agents' | 'regions' | 'credentials' | 'registration') => void;
    onexpired?: () => void;
  } = $props();

  const REFRESH_MS = 15000;
  let cluster = $state<ClusterResponse | null>(null);
  let agents = $state<Agent[]>([]);
  let unavailable = $state<string[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let fleetError = $state<string | null>(null);
  let collapsed = $state(new Set<string>());
  let refreshInFlight = false;
  let showAllActivity = $state(false);

  async function refresh() {
    if (refreshInFlight) return;
    refreshInFlight = true;
    try {
      const [clusterResult, agentsResult] = await Promise.allSettled([fetchCluster(), fetchAgents()]);
      if (clusterResult.status === 'fulfilled') {
        cluster = clusterResult.value;
        loadError = null;
      } else {
        if (clusterResult.reason instanceof AdminError && clusterResult.reason.unauthorized) onexpired?.();
        loadError = clusterResult.reason instanceof Error ? clusterResult.reason.message : String(clusterResult.reason);
      }
      if (agentsResult.status === 'fulfilled') {
        agents = agentsResult.value.agents;
        unavailable = agentsResult.value.unavailable_regions ?? [];
        fleetError = null;
      } else {
        if (agentsResult.reason instanceof AdminError && agentsResult.reason.unauthorized) onexpired?.();
        fleetError = agentsResult.reason instanceof Error ? agentsResult.reason.message : String(agentsResult.reason);
      }
    } finally {
      loading = false;
      refreshInFlight = false;
    }
  }

  refresh();
  const timer = setInterval(refresh, REFRESH_MS);
  onDestroy(() => clearInterval(timer));

  const reporting = $derived(agents.filter((a) => a.status === 'up' && !a.retired));
  const silent = $derived(agents.filter((a) => a.status !== 'up' && !a.retired));
  const retired = $derived(agents.filter((a) => a.retired));
  const activeCount = $derived(reporting.length + silent.length);
  const reportingPct = $derived(activeCount ? Math.round(reporting.length / activeCount * 100) : 0);

  const peers = $derived.by((): ClusterMember[] => {
    if (!cluster) return [];
    if (cluster.mode === 'global') return cluster.global_peers?.length ? cluster.global_peers : [cluster.self];
    return [cluster.self, ...cluster.members];
  });

  const regions = $derived.by(() => {
    const names = new Set(agents.map((a) => a.region));
    if (cluster?.mode === 'global') cluster.members.forEach((m) => names.add(m.node_id));
    return [...names].sort((a,b) => a.localeCompare(b)).map((name) => {
      const within = agents.filter((a) => a.region === name);
      const zones = [...new Set(within.map((a) => a.zone))].sort().map((name) => ({
        name,
        agents: within.filter((a) => a.zone === name).sort((a,b) => a.agent_id.localeCompare(b.agent_id)),
      }));
      const link = cluster?.mode === 'global' ? cluster.members.find((m) => m.node_id === name) : undefined;
      const roster = cluster?.regions?.find((r) => r.region === name);
      return { name, agents: within, zones, link, roster };
    });
  });
  const zonesCount = $derived(regions.reduce((total, region) => total + region.zones.length, 0));
  const activity = $derived([...agents].sort((a,b) => b.last_seen - a.last_seen).slice(0, showAllActivity ? 15 : 5));

  function toggle(key: string) {
    const next = new Set(collapsed);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    collapsed = next;
  }

  function label(name: string) { return name === UNKNOWN_LOCATION ? 'Unplaced' : name; }
  function isReporting(agent: Agent) { return agent.status === 'up' && !agent.retired; }
  function regionStatus(region: (typeof regions)[number]) {
    if (region.link) return region.link.state === 'alive' ? 'connected' : region.link.state;
    const running = region.agents.some(isReporting);
    return running ? 'reporting' : region.agents.length ? 'silent' : 'no agents';
  }
</script>

{#if loading && !cluster}
  <div class="admin-panel"><p class="admin-muted">Loading fleet…</p></div>
{:else}
  {#if loadError}<div class="admin-error">Could not load control-plane membership: {loadError}</div>{/if}
  {#if fleetError}<div class="admin-error">Could not load agent inventory: {fleetError}</div>{/if}
  {#if unavailable.length}<div class="admin-error">Region data unavailable from {unavailable.join(', ')}; fleet totals and placement may be incomplete.</div>{/if}

  {#if mode === 'overview'}
    <section class="fleet-stat-grid" aria-label="Fleet summary">
      <div class="fleet-stat"><strong>{fleetError ? '—' : agents.length}</strong><span>Agents</span><small>Known to this node</small></div>
      <div class="fleet-stat good"><strong>{fleetError ? '—' : reporting.length}</strong><span>Reporting</span><small>{fleetError ? 'Inventory unavailable' : 'Currently connected'}</small></div>
      <div class="fleet-stat" class:warn={silent.length > 0}><strong>{fleetError ? '—' : silent.length}</strong><span>Silent</span><small>Active but not reporting</small></div>
      <div class="fleet-stat"><strong>{fleetError ? '—' : retired.length}</strong><span>Retired</span><small>Retained history</small></div>
      <div class="fleet-stat"><strong>{fleetError ? '—' : zonesCount}</strong><span>Zones</span><small>Observed placement</small></div>
      <div class="fleet-stat"><strong>{fleetError && !cluster ? '—' : regions.length}</strong><span>Regions</span><small>Known or reporting</small></div>
    </section>
  {/if}

  <div class="fleet-layout" class:regions-only={mode === 'regions'}>
    <div class="fleet-main">
      {#if cluster && mode === 'overview'}
        <section class="control-plane" aria-label="Control-plane membership">
          <div class="control-title">
            <span class="plane-icon" aria-hidden="true">◇</span>
            <div>
              <h3>{cluster.mode === 'global' ? 'Global control plane' : 'Regional control plane'}</h3>
              <p>{peers.length} {peers.length === 1 ? 'node' : 'nodes'} · {peers.length > 1 ? 'Clustered' : 'Standalone'} · {cluster.mode === 'global' ? 'Global role' : 'Regional role'}</p>
            </div>
            <span class="plane-badge" class:warning={peers.some((p) => p.state !== 'alive')}>{peers.every((p) => p.state === 'alive') ? 'All reporting' : 'Attention'}</span>
          </div>
          <div class="plane-peers">
            {#each peers as peer (peer.node_id)}
              <div class="plane-peer">
                <span class="live-dot" class:bad={peer.state === 'dead' || peer.state === 'removed'} class:warn={peer.state === 'suspect'}></span>
                <strong>{peer.node_id}</strong>
                {#if peer.node_id === cluster.self.node_id}<span class="self-pill">This node</span>{/if}
                <small>{peer.state}</small>
              </div>
            {/each}
          </div>
        </section>
      {/if}

      <div class="regions-topline">
        <div>
          <h3>{mode === 'overview' ? 'Regions & zones' : 'Region and zone inventory'}</h3>
          <p>{mode === 'overview' ? 'Live placement, grouped by location.' : 'Explore regions, zones, and the agents reporting from each location.'}</p>
        </div>
        {#if mode === 'overview'}<button class="quiet-link" onclick={() => onNavigate('regions')}>All regions <span aria-hidden="true">→</span></button>{/if}
      </div>

      <div class="region-list">
        {#each regions as region (region.name)}
          <section class="fleet-region" aria-label={'Region ' + label(region.name)}>
            <button class="region-toggle" onclick={() => toggle(region.name)} aria-expanded={!collapsed.has(region.name)}>
              <span class="chev" class:closed={collapsed.has(region.name)}>⌄</span>
              <span class="region-mark" class:warning={regionStatus(region) === 'silent'}>◎</span>
              <strong>{label(region.name)}</strong>
              <span class="state-chip" class:warning={regionStatus(region) !== 'connected' && regionStatus(region) !== 'reporting'}>{regionStatus(region)}</span>
              <span class="region-summary">{region.zones.length} {region.zones.length === 1 ? 'zone' : 'zones'} · {region.agents.length} {region.agents.length === 1 ? 'agent' : 'agents'}</span>
              <span class="expand-symbol" aria-hidden="true">···</span>
            </button>
            {#if !collapsed.has(region.name)}
              {#if mode === 'regions' && region.roster?.members.length}
                <div class="coordinator-list">
                  <small>Regional coordinators</small>
                  {#each region.roster.members as member (member.node_id)}
                    <span class="coordinator-pill"><span class="live-dot" class:warn={member.state !== 'alive'}></span>{member.node_id}</span>
                  {/each}
                </div>
              {/if}
              {#each region.zones as zone (zone.name)}
                <div class="zone-card">
                  <button class="zone-toggle" onclick={() => toggle(region.name + '/' + zone.name)} aria-expanded={!collapsed.has(region.name + '/' + zone.name)}>
                    <span class="zone-icon" aria-hidden="true">⌂</span>
                    <strong>{label(zone.name)}</strong>
                    <span class="state-chip" class:warning={!zone.agents.some(isReporting)}>{zone.agents.some(isReporting) ? 'Reporting' : 'No active agent'}</span>
                    <span class="zone-summary">{zone.agents.filter(isReporting).length}/{zone.agents.length} reporting</span>
                    <span class="go" aria-hidden="true">{collapsed.has(region.name + '/' + zone.name) ? '›' : '⌄'}</span>
                  </button>
                  {#if !collapsed.has(region.name + '/' + zone.name)}
                    {#each zone.agents as agent (agent.agent_id)}
                      <div class="fleet-agent">
                        <span class="live-dot" class:warn={!isReporting(agent)}></span>
                        <span class="agent-name"><strong>{agent.agent_id}</strong><small>{agent.coordinator ? 'Polls ' + agent.coordinator : 'Coordinator not reported'}</small></span>
                        <span class="agent-state" class:warning={!isReporting(agent)}>{agent.retired ? 'Retired' : isReporting(agent) ? 'Reporting' : 'Silent'} · {formatRelativeTime(agent.last_seen)}</span>
                        <span class="agent-detail">Checks <strong>{agent.checks_reporting}</strong></span>
                        <span class="agent-detail">Poll RTT <strong>{agent.poll_rtt_ms === null ? '—' : agent.poll_rtt_ms + ' ms'}</strong></span>
                        <button class="agent-drill" title={'View fleet agents including ' + agent.agent_id} aria-label={'View agent ' + agent.agent_id} onclick={() => onNavigate('agents')}>›</button>
                      </div>
                    {:else}
                      <p class="empty-zone">No agents have reported from this zone.</p>
                    {/each}
                  {/if}
                </div>
              {:else}
                <div class="empty-zone">No zones have reported yet. <button class="quiet-link" onclick={() => onNavigate('registration')}>Add a location →</button></div>
              {/each}
            {/if}
          </section>
        {:else}
          <div class="fleet-empty">
            <h3>No locations yet</h3>
            <p>Start a regional coordinator or enroll an agent. Single-node deployments don't require extra hierarchy.</p>
            <button class="btn primary" onclick={() => onNavigate('registration')}>Set up deployment</button>
          </div>
        {/each}
      </div>
    </div>

    {#if mode === 'overview'}
      <aside class="fleet-aside" aria-label="Fleet health and recent agent reports">
        <section class="aside-panel">
          <div class="aside-heading"><h3>Fleet connectivity</h3><span>Live</span></div>
          <div class="fleet-health"><strong>{fleetError ? '—' : reportingPct + '%'}</strong><span>of active agents reporting</span></div>
          <div class="health-track"><div style={'width:' + reportingPct + '%'}></div></div>
          <div class="health-lines"><span><i class="live-dot"></i>{reporting.length} reporting</span><span><i class="live-dot warn"></i>{silent.length} silent</span><span><i class="live-dot neutral"></i>{retired.length} retired</span></div>
          <p class="aside-foot">Connectivity only; monitored check health is tracked separately.</p>
        </section>
        <section class="aside-panel">
          <div class="aside-heading"><h3>Latest agent reports</h3><button class="quiet-link" onclick={() => (showAllActivity = !showAllActivity)}>{showAllActivity ? 'Show less' : 'Show more'} →</button></div>
          {#each activity as agent (agent.agent_id)}
            <div class="activity-row">
              <span class="live-dot" class:warn={!isReporting(agent)}></span>
              <div><strong>{agent.agent_id}</strong><small>{label(agent.zone)}</small></div>
              <time>{formatRelativeTime(agent.last_seen)}</time>
            </div>
          {:else}
            <p class="aside-foot">No agent reports yet.</p>
          {/each}
          <button class="aside-link" onclick={() => onNavigate('agents')}>Explore all agents <span>→</span></button>
        </section>
      </aside>
    {/if}
  </div>
{/if}

<style>
  .fleet-stat-grid { display:grid; grid-template-columns:repeat(6,minmax(0,1fr));gap:12px;margin:0 0 18px; }
  .fleet-stat { background:#101b29;border:1px solid #22364b;border-radius:9px;padding:17px;display:grid;gap:5px;min-width:0; }
  .fleet-stat strong { font-size:28px;font-weight:600;color:#e6eef6;font-variant-numeric:tabular-nums; }
  .fleet-stat.good strong { color:#54d0ab; }.fleet-stat.warn strong { color:#e9ae62; }
  .fleet-stat span { font-size:12px;color:#cedee9; }.fleet-stat small { font-size:11px;color:#8da0b5; }
  .fleet-layout { display:grid;grid-template-columns:minmax(0,1fr) 280px;gap:16px;align-items:start; }
  .fleet-layout.regions-only { display:block; }
  .fleet-main { min-width:0; }.fleet-aside { display:grid;gap:16px; }
  .control-plane,.fleet-region,.aside-panel,.fleet-empty { background:#101b29;border:1px solid #22364b;border-radius:9px; }
  .control-plane { padding:18px;margin-bottom:19px; }
  .control-title { display:flex;align-items:center;gap:12px; }
  .control-title h3,.aside-heading h3,.regions-topline h3 { color:#e6eef6;font-size:14px;margin:0;font-weight:650; }
  .control-title p,.regions-topline p { color:#8ca2b6;font-size:11px;margin:5px 0 0; }
  .plane-icon { display:grid;place-items:center;width:39px;height:39px;border:1px solid #275b71;border-radius:9px;color:#65d5e3;font-size:27px;background:#0c2738; }
  .plane-badge { margin-left:auto;font-size:11px;color:#62d8af;border-radius:20px;background:#0e302b;border:1px solid #255d4c;padding:5px 9px;white-space:nowrap; }
  .plane-badge.warning { color:#e8b578;background:#312618;border-color:#785433; }
  .plane-peers { display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,165px),1fr));gap:8px;margin-top:15px; }
  .plane-peer { display:flex;align-items:center;gap:7px;flex-wrap:wrap;padding:11px;background:#0b1725;border:1px solid #253b50;border-radius:7px;min-width:0; }
  .plane-peer strong { min-width:0;overflow-wrap:anywhere;font-size:12px; }.plane-peer small { width:100%;padding-left:15px;color:#8ca2b6;font-size:11px;text-transform:capitalize; }
  .self-pill { font-size:10px;color:#6cd5e7;border:1px solid #285d72;padding:2px 5px;border-radius:12px; }
  .regions-topline { display:flex;align-items:center;justify-content:space-between;gap:12px;margin:12px 0; }
  .quiet-link { font:inherit;border:0;padding:4px;color:#64cae2;background:transparent;cursor:pointer;font-size:12px; }
  .quiet-link:hover { color:#a7e9f1; text-decoration:underline; }
  .region-list { display:grid;gap:13px; }
  .fleet-region { overflow:hidden; }
  .region-toggle,.zone-toggle { display:flex;align-items:center;gap:10px;width:100%;text-align:left;background:transparent;border:0;color:#e7f0f6;font:inherit;cursor:pointer;min-width:0; }
  .region-toggle { min-height:60px;padding:12px 16px; }.region-toggle:hover,.zone-toggle:hover { background:#172b3c; }
  .region-toggle:focus-visible,.zone-toggle:focus-visible,.agent-drill:focus-visible { outline:2px solid #56d4e4;outline-offset:-3px; }
  .chev { color:#80a3bb;transform:rotate(0);transition:transform .15s ease; }.chev.closed { transform:rotate(-90deg); }
  .region-mark { display:grid;place-items:center;width:28px;height:28px;color:#55cae1;background:#0e293c;border:1px solid #245a6e;border-radius:7px; }.region-mark.warning { color:#eab06f; }
  .region-toggle strong { font-size:15px; }.region-summary { font-size:11px;color:#9db1c3; }
  .state-chip { font-size:10px;color:#51cfa8;background:#0b302b;border:1px solid #245c49;border-radius:14px;padding:4px 8px;white-space:nowrap; }
  .state-chip.warning { color:#e9b06f;background:#2d231c;border-color:#705237; }
  .expand-symbol { margin-left:auto;letter-spacing:2px;color:#91abc1; }
  .zone-card { margin:0 14px 10px 32px;background:#0d1928;border:1px solid #253d52;border-radius:8px;overflow:hidden; }
  .zone-toggle { min-height:46px;padding:8px 12px; }.zone-toggle strong {font-size:12px;overflow-wrap:anywhere;min-width:0;}
  .zone-icon { color:#63d8c9;background:#102e39;border-radius:6px;padding:5px 7px; }
  .zone-summary { font-size:11px;color:#95a9b9;margin-left:auto;white-space:nowrap; }.go { color:#79aec8; }
  .fleet-agent { display:flex;align-items:center;gap:10px;padding:12px 14px 12px 23px;border-top:1px solid #213349;min-width:0; }
  .agent-name { display:grid;gap:4px;flex:1;min-width:100px; }
  .agent-name strong { font-size:12px;overflow-wrap:anywhere; }.agent-name small { font-size:10px;color:#8ea4b7; }
  .agent-state { font-size:11px;color:#52cea7;white-space:nowrap; }.agent-state.warning { color:#e5b173; }
  .agent-detail { display:grid;gap:3px;font-size:10px;color:#8ea4b7;min-width:67px; }.agent-detail strong { font-size:11px;color:#d8e7f1;font-weight:500; }
  .agent-drill { display:grid;place-items:center;color:#89cce1;background:none;border:0;cursor:pointer;font-size:20px;padding:7px; }
  .coordinator-list { display:flex;flex-wrap:wrap;align-items:center;gap:8px;padding:10px 16px 12px; }
  .coordinator-list small { color:#8ea4b7; }
  .coordinator-pill { display:inline-flex;align-items:center;gap:6px;padding:6px 9px;background:#0d1e2e;border:1px solid #244057;border-radius:6px;font-size:11px; }
  .live-dot { flex:none;display:inline-block;width:7px;height:7px;background:#52cba5;border-radius:50%; }
  .live-dot.warn { background:#e5ac6c; }.live-dot.bad { background:#e27370; }.live-dot.neutral { background:#8a98a8; }
  .aside-panel { padding:17px; }.aside-heading { display:flex;justify-content:space-between;align-items:center;gap:8px; }
  .aside-heading>span { color:#75bfd1;font-size:11px; }.fleet-health { display:flex;align-items:baseline;gap:12px;margin-top:21px;flex-wrap:wrap; }
  .fleet-health strong { color:#50cea7;font-size:28px; }.fleet-health span { font-size:11px;color:#91a7b9; }
  .health-track { height:8px;background:#183248;border-radius:8px;overflow:hidden;margin:15px 0; }.health-track div { height:100%;background:#50cea7; }
  .health-lines { display:grid;gap:9px; }.health-lines>span { display:flex;align-items:center;gap:9px;font-size:12px;color:#a5b8c8; }
  .health-lines i {margin-right:2px;}.aside-foot { font-size:11px;color:#8299ad;line-height:1.6;margin:13px 0 0; }
  .activity-row { display:flex;align-items:center;gap:8px;padding:11px 0;border-bottom:1px solid #213449; }
  .activity-row>div {display:grid;gap:4px;min-width:0;flex:1;}.activity-row strong {font-size:12px;font-weight:550;overflow-wrap:anywhere;}.activity-row small {font-size:10px;color:#90a4b7;}
  .activity-row time {font-size:10px;color:#96a9bc;white-space:nowrap;}
  .aside-link { display:flex;justify-content:space-between;width:100%;padding:12px 0 0;font:inherit;font-size:12px;color:#76d4e5;border:0;background:none;cursor:pointer; }
  .empty-zone { padding:12px 22px;color:#9bb0c2;font-size:12px; }
  .fleet-empty { padding:28px; }.fleet-empty h3 {margin-top:0;}.fleet-empty p {color:#92a6b7;font-size:12px;}
  @media (max-width:1250px) { .fleet-stat-grid { grid-template-columns:repeat(3,minmax(0,1fr)); }.fleet-layout {grid-template-columns:minmax(0,1fr) 240px;} }
  @media (max-width:950px) { .fleet-layout { grid-template-columns:minmax(0,1fr); }.fleet-aside {grid-template-columns:repeat(2,minmax(0,1fr));} }
  @media (max-width:680px) { .fleet-stat-grid {grid-template-columns:repeat(2,minmax(0,1fr));}.fleet-aside{grid-template-columns:1fr;}.region-toggle{flex-wrap:wrap;}.region-summary{flex-basis:50%;}.zone-card{margin-left:14px;}.fleet-agent{flex-wrap:wrap;}.agent-state{flex-basis:45%;}.agent-detail{min-width:60px;} }
  @media (prefers-reduced-motion:reduce) { .chev { transition:none; } }
</style>
