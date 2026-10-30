<script lang="ts">
  import { onMount } from 'svelte';
  import { fetchAgents, fetchUptimeStrips } from '../lib/api';
  import type { Agent, Alert, Check, FleetRow, Incident, IncidentGroup, MaintenanceNotice, UptimeResponse } from '../lib/api';
  import { fetchCluster, fetchRegionTree, type NodeMode, type RegionTreeResponse } from '../lib/admin';
  import { barColor, failureReason, formatDuration, formatLatency, formatRelativeTime, formatUptime } from '../lib/format';
  import LighthouseHero from './LighthouseHero.svelte';
  import type { SeasonalPreferences } from '../lib/seasonalAtmosphere';
  import ConsoleIcon from './ConsoleIcon.svelte';
  import { activeIncidentCount, alertLabel, attentionChecks, incidentFeed, isUnauthorized } from './nightwatchOverview';

  let {
    checks,
    totalChecks,
    hasMore,
    incidents,
    groups,
    alerts,
    fleet,
    maintenance,
    loading,
    error,
    wsStatus,
    nodeMode,
    onOpenCheck,
    onOpenIncident,
    onPanel,
    onAddCheck,
    onRetry,
    onExpired,
    seasonalPreferences,
    seasonalNow,
    onSeasonalPreferencesChange,
  }: {
    checks: Check[];
    totalChecks: number;
    hasMore: boolean;
    incidents: Incident[];
    groups: IncidentGroup[];
    alerts: Alert[];
    fleet: FleetRow[];
    maintenance: MaintenanceNotice[];
    loading: boolean;
    error: string | null;
    wsStatus: string;
    nodeMode: NodeMode;
    onOpenCheck: (id: string) => void;
    onOpenIncident: (id: string) => void;
    onPanel: (panel: string) => void;
    onAddCheck: () => void;
    onRetry: () => void;
    onExpired: () => void;
    seasonalPreferences?: SeasonalPreferences;
    seasonalNow?: Date;
    onSeasonalPreferencesChange?: (next: SeasonalPreferences) => void;
  } = $props();

  const FEATURED = 6;
  const BUCKETS = 48;
  const SEGMENTS = 48;
  const REFRESH_MS = 60_000;
  const TYPE_LABELS: Record<string, string> = {
    http: 'HTTP',
    tcp: 'TCP',
    dns: 'DNS',
    icmp: 'ICMP',
    ssl: 'SSL / TLS',
    ntp: 'NTP',
    heartbeat: 'Heartbeat',
    openmetrics: 'OpenMetrics',
  };
  const STATE_LABELS: Record<string, string> = { up: 'Operational', down: 'Down', degraded: 'Degraded', unknown: 'Unknown' };
  const TONES: Record<string, string> = { up: 'good', down: 'bad', degraded: 'warn', unknown: 'quiet' };
  const MARKS: Record<string, string> = { up: '✓', down: '×', degraded: '△', unknown: '○' };
  const SEVERITY: Record<string, number> = { down: 0, degraded: 1, unknown: 2, up: 3 };
  const timeFmt = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
  const dateFmt = new Intl.DateTimeFormat(undefined, { day: 'numeric', month: 'short', year: 'numeric' });

  let agents = $state<Agent[] | null>(null);
  let orphanedChecks = $state(0);
  let cluster = $state<{ self: string; live: number; total: number } | null>(null);
  let regionTree = $state<RegionTreeResponse | null>(null);
  let strips = $state<Record<string, UptimeResponse>>({});
  let connectivityError = $state(false);
  let filter = $state<'all' | 'attention'>('all');
  let query = $state('');
  let monitorsPanel = $state<HTMLElement | null>(null);
  let stripsKey = '';
  let connectivityRequest = 0;
  let stripsRequest = 0;

  const attention = $derived(attentionChecks(checks));
  const activeChecks = $derived(checks.filter((check) => check.enabled !== false));
  const down = $derived(activeChecks.filter((check) => check.status === 'down').length);
  const degraded = $derived(activeChecks.filter((check) => check.status === 'degraded').length);
  const unknown = $derived(activeChecks.filter((check) => check.status === 'unknown').length);
  const up = $derived(activeChecks.length - down - degraded - unknown);
  const feed = $derived(incidentFeed(incidents, groups));
  const activeCount = $derived(activeIncidentCount(incidents, groups));
  const checkNames = $derived(new Map(checks.map((check) => [check.check_id, check.name])));
  const disconnected = $derived(!!error || wsStatus === 'closed');
  const updatedAt = $derived(activeChecks.reduce((latest, check) => Math.max(latest, check.last_check ?? 0), 0));
  const modeLabel = $derived(nodeMode === 'global' ? 'Global' : 'Coordinator');
  const lighthouseHealth = $derived.by(() => {
    if (loading || disconnected) return 'unknown' as const;
    if (down > 0 || feed.some((item) => item.state !== 'monitoring')) return 'down' as const;
    if (degraded > 0) return 'degraded' as const;
    if (!activeChecks.length || (unknown > 0 && unknown === activeChecks.length)) return 'unknown' as const;
    return 'operational' as const;
  });

  const sortedChecks = $derived(
    checks
      .map((check, index) => ({ check, index }))
      .sort((a, b) => Number(a.check.enabled === false) - Number(b.check.enabled === false) || SEVERITY[a.check.status] - SEVERITY[b.check.status] || a.index - b.index)
      .map(({ check }) => check),
  );
  const filteredChecks = $derived(
    sortedChecks
      .filter((check) => filter === 'all' || (check.enabled !== false && check.status !== 'up'))
      .filter((check) => `${check.name} ${check.type ?? ''} ${check.group ?? ''}`.toLowerCase().includes(query.trim().toLowerCase())),
  );
  const featuredChecks = $derived(filteredChecks.slice(0, FEATURED));

  const liveAgents = $derived(agents?.filter((agent) => !agent.retired) ?? null);
  const staleAgents = $derived(liveAgents?.filter((agent) => agent.status !== 'up') ?? []);
  const onlineAgents = $derived(liveAgents ? liveAgents.length - staleAgents.length : 0);

  type Station = {
    name: string;
    location: string;
    affected: number;
    failing: number;
    unknownOnly: boolean;
    agentsLive: number | null;
    agentsTotal: number | null;
    freshness: string;
    reporting: boolean;
  };

  const stations = $derived.by((): Station[] => {
    const byRegion = new Map<string, { affected: Set<string>; failing: Set<string>; known: boolean; zones: Set<string>; staleness: number | null }>();
    const pausedIds = new Set(checks.filter((check) => check.enabled === false).map((check) => check.check_id));
    const entry = (name: string) => {
      let value = byRegion.get(name);
      if (!value) {
        value = { affected: new Set(), failing: new Set(), known: false, zones: new Set(), staleness: null };
        byRegion.set(name, value);
      }
      return value;
    };
    for (const check of regionTree?.checks ?? []) {
      for (const region of check.regions) {
        const value = entry(region.region);
        if (pausedIds.has(check.check_id)) continue;
        if (region.state !== 'unknown') value.known = true;
        if (region.state === 'degraded' || region.state === 'down') value.affected.add(check.check_id);
        if (region.state === 'down') value.failing.add(check.check_id);
        for (const zone of region.zones) {
          value.zones.add(zone.zone);
          for (const agent of zone.agents) {
            value.staleness = value.staleness === null ? agent.staleness_ms : Math.min(value.staleness, agent.staleness_ms);
          }
        }
      }
    }
    const fleetRegions = fleet.filter((row) => row.kind === 'region');
    for (const row of fleetRegions) entry(row.name);
    for (const agent of liveAgents ?? []) if (agent.region) entry(agent.region);
    return [...byRegion.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([name, value]) => {
        const regionAgents = liveAgents?.filter((agent) => agent.region === name) ?? null;
        const row = fleetRegions.find((candidate) => candidate.name === name);
        const agentsTotal = regionAgents && regionAgents.length ? regionAgents.length : (row?.total ?? null);
        const agentsLive = regionAgents && regionAgents.length ? regionAgents.filter((agent) => agent.status === 'up').length : (row?.healthy ?? null);
        const freshness =
          value.staleness !== null
            ? `Last report · ${formatRelativeTime(Date.now() - value.staleness)}`
            : row
              ? row.status === 'down'
                ? 'Not reporting'
                : 'Reporting'
              : 'Reporting status unknown';
        return {
          name,
          location: value.zones.size ? `${value.zones.size} ${value.zones.size === 1 ? 'zone' : 'zones'}` : nodeMode === 'global' ? 'Regional coordinator' : 'Region',
          affected: value.affected.size,
          failing: value.failing.size,
          unknownOnly: !value.known,
          agentsLive,
          agentsTotal,
          freshness,
          reporting: (agentsLive ?? 0) > 0 || row?.status === 'up' || row?.status === 'degraded',
        };
      });
  });
  const reportingStations = $derived(stations.filter((station) => station.reporting).length);
  const affectedStations = $derived(stations.filter((station) => station.affected > 0));

  type AttentionItem = { key: string; icon: string; tone: string; title: string; body: string; mono?: string; action: string; run: () => void };

  const attentionItems = $derived.by((): AttentionItem[] => {
    const items: AttentionItem[] = attention.map((check) => {
      const incident = feed.find((item) => 'checks' in item && item.checks.some((affected) => affected.check_id === check.check_id));
      const certificate = check.type === 'ssl' && check.status !== 'unknown';
      const reason = sentence(failureReason(check) ?? `${STATE_LABELS[check.status]} on its latest evaluation`);
      const detail = certificate ? `${check.name}: ${reason}` : reason;
      return {
        key: `check:${check.check_id}`,
        icon: certificate ? 'credentials' : 'checks',
        tone: TONES[check.status],
        title: certificate ? 'Certificate warning' : check.name,
        body: incident ? `${detail} Linked to “${incident.title}”.` : detail,
        action: 'Inspect monitor →',
        run: () => onOpenCheck(check.check_id),
      };
    });
    if (staleAgents.length) {
      const single = staleAgents.length === 1 ? staleAgents[0] : null;
      items.push({
        key: 'agents:stale',
        icon: 'agents',
        tone: 'warn',
        title: single ? 'One agent stopped reporting' : `${staleAgents.length} agents stopped reporting`,
        mono: staleAgents.slice(0, 3).map((agent) => agent.agent_id).join(', '),
        body: single ? `Last seen at ${timeFmt.format(single.last_seen)} · ${formatRelativeTime(single.last_seen)}` : 'Their assigned checks may stop producing results.',
        action: single ? 'Inspect agent →' : 'Inspect agents →',
        run: () => onPanel('agents'),
      });
    }
    if (orphanedChecks > 0) {
      items.push({
        key: 'agents:orphaned',
        icon: 'alert',
        tone: 'bad',
        title: `${orphanedChecks} ${orphanedChecks === 1 ? 'check has' : 'checks have'} no live agent`,
        body: 'Nothing is probing these checks until an assigned agent reconnects.',
        action: 'Review assignments →',
        run: () => onPanel('agents'),
      });
    }
    const now = Date.now();
    for (const window of maintenance.filter((item) => item.starts_at <= now && item.ends_at > now)) {
      items.push({
        key: `maintenance:${window.title}:${window.starts_at}`,
        icon: 'maintenance',
        tone: 'muted',
        title: 'Maintenance in progress',
        body: `${window.title} · until ${timeFmt.format(window.ends_at)}`,
        action: 'View maintenance →',
        run: () => onPanel('maintenance'),
      });
    }
    return items.slice(0, 5);
  });

  const hero = $derived.by(() => {
    if (loading) return { tone: 'quiet', title: 'Establishing visibility', text: 'Loading the latest monitor states…', action: null as null | { label: string; run: () => void } };
    if (error) return { tone: 'warn', title: 'Visibility interrupted', text: 'The status feed could not be refreshed. Current health cannot be confirmed.', action: { label: 'Retry', run: onRetry } };
    if (wsStatus === 'closed') return { tone: 'warn', title: 'Visibility interrupted', text: 'Live updates are disconnected. Showing the most recent snapshot.', action: { label: 'Refresh now', run: onRetry } };
    if (!checks.length) return { tone: 'good', title: 'Your watch starts here', text: 'Add a monitor to begin tracking availability, response time, and certificate health.', action: { label: 'Add your first monitor', run: onAddCheck } };
    if (!activeChecks.length) return { tone: 'quiet', title: 'Monitoring paused', text: 'No monitors are active. Resume a monitor to confirm current health.', action: { label: 'View all monitors', run: () => onPanel('checks') } };
    if (attention.length) {
      const regions = affectedStations.map((station) => station.name);
      const where = regions.length && regions.length < stations.length ? ` in ${regions.join(', ')}` : '';
      const others = regions.length && regions.length < stations.length ? ' Your other regions are operating normally.' : '';
      const count = attention.length === 1 ? 'One monitor needs' : `${attention.length} monitors need`;
      const title = down === 0 ? 'Degraded performance' : down === activeChecks.length ? 'Major service disruption' : 'Partial service disruption';
      const action = feed[0]
        ? { label: 'Review active incident', run: () => openIncident(feed[0]) }
        : { label: 'Review affected monitors', run: showAttention };
      return { tone: down ? 'bad' : 'warn', title, text: `${count} attention${where}.${others}`, action };
    }
    const scope = hasMore ? `All ${activeChecks.length} shown active monitors are` : `All ${activeChecks.length} active ${activeChecks.length === 1 ? 'monitor is' : 'monitors are'}`;
    const regions = stations.length ? ` ${reportingStations} of ${stations.length} ${stations.length === 1 ? 'region is' : 'regions are'} reporting.` : '';
    return {
      tone: 'good',
      title: hasMore ? 'Shown monitors operational' : 'All systems operational',
      text: `${scope} passing their checks.${regions}`,
      action: stations.length ? { label: 'Explore regional health', run: () => onPanel('region-tree') } : { label: 'View all monitors', run: () => onPanel('checks') },
    };
  });

  function openIncident(item: Incident | IncidentGroup) {
    if ('regions' in item && item.regions[0]) onOpenIncident(item.regions[0].id);
    else onOpenIncident(item.id);
  }

  function incidentScope(item: Incident | IncidentGroup) {
    if ('regions' in item) return item.regions.map((region) => region.region).join(' / ') || 'All regions';
    const names = item.checks.map((affected) => checkNames.get(affected.check_id) ?? affected.check_id);
    if (!names.length) return item.global ? 'Global incident' : 'No monitors attached';
    return names.length > 3 ? `${names.slice(0, 3).join(' / ')} / +${names.length - 3} more` : names.join(' / ');
  }

  function latestUpdate(item: Incident | IncidentGroup) {
    if ('regions' in item || !item.updates?.length) return null;
    return [...item.updates].sort((a, b) => b.created_at - a.created_at)[0];
  }

  function elapsed(since: number) {
    const minutes = Math.max(0, Math.floor((Date.now() - since) / 60_000));
    if (minutes < 1) return 'just now';
    if (minutes < 60) return `${minutes} min`;
    const hours = Math.floor(minutes / 60);
    if (hours < 48) return `${hours}h ${minutes % 60}m`;
    return formatDuration(Date.now() - since);
  }

  function sentence(text: string) {
    return /[.!?]$/.test(text.trim()) ? text.trim() : `${text.trim()}.`;
  }

  function stateText(state: string) {
    return state.charAt(0).toUpperCase() + state.slice(1);
  }

  function showAttention() {
    filter = 'attention';
    monitorsPanel?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function bars(check: Check) {
    const buckets = strips[check.check_id]?.buckets;
    if (!buckets?.length) return Array.from({ length: BUCKETS }, () => 'unknown');
    return buckets.map((bucket) => {
      const color = barColor(bucket);
      return color === 'nodata' ? 'unknown' : color;
    });
  }

  function eventTone(alert: Alert) {
    return TONES[alert.current_state] ?? 'quiet';
  }

  function eventDetail(alert: Alert) {
    const scope = alert.scope === 'check' ? 'Monitor' : stateText(alert.scope);
    const from = alert.previous_state ? `${STATE_LABELS[alert.previous_state] ?? alert.previous_state} → ` : '';
    return `${scope} · ${from}${STATE_LABELS[alert.current_state] ?? alert.current_state}`;
  }

  function settled<T>(result: PromiseSettledResult<T>): T | null {
    if (result.status === 'fulfilled') return result.value;
    if (isUnauthorized(result.reason)) onExpired();
    return null;
  }

  async function loadConnectivity() {
    const request = ++connectivityRequest;
    const [agentResult, clusterResult, treeResult] = await Promise.allSettled([fetchAgents(), fetchCluster(), fetchRegionTree()]);
    if (request !== connectivityRequest) return;
    const agentData = settled(agentResult);
    const clusterData = settled(clusterResult);
    const treeData = settled(treeResult);
    agents = agentData?.agents ?? null;
    orphanedChecks = agentData ? (agentData.checks_with_no_live_agent_total ?? agentData.checks_with_no_live_agent.length) : 0;
    cluster = clusterData
      ? { self: clusterData.self.node_id, total: clusterData.members.length, live: clusterData.members.filter((member) => member.state === 'alive').length }
      : null;
    if (treeData) regionTree = treeData;
    connectivityError = !agentData || !clusterData;
  }

  async function loadStrips(force = false) {
    const key = checks.map((check) => `${check.check_id}:${check.status}`).join(',');
    if (!force && key === stripsKey) return;
    stripsKey = key;
    if (!checks.length) {
      strips = {};
      return;
    }
    const request = ++stripsRequest;
    try {
      const response = await fetchUptimeStrips(1, BUCKETS, null, 0, 100, true);
      if (request === stripsRequest) strips = Object.fromEntries(response.checks.map((strip) => [strip.check_id, strip]));
    } catch (failure) {
      if (isUnauthorized(failure)) onExpired();
    }
  }

  onMount(() => {
    void loadConnectivity();
    const timer = window.setInterval(() => {
      void loadConnectivity();
      void loadStrips(true);
    }, REFRESH_MS);
    return () => {
      window.clearInterval(timer);
      connectivityRequest += 1;
      stripsRequest += 1;
    };
  });

  $effect(() => {
    void checks;
    void loadStrips();
  });
</script>

<div class="overview">
  <section class="hero" aria-labelledby="hero-title">
    <LighthouseHero health={lighthouseHealth} {seasonalPreferences} {seasonalNow} {onSeasonalPreferencesChange} />
    <div class="hero-copy">
      <div class="hero-kicker"><span class={hero.tone} aria-hidden="true">◈</span>Infrastructure at a glance</div>
      <h1 id="hero-title">{hero.title}</h1>
      <p>{hero.text}</p>
      {#if hero.action}
        <button class="hero-link" onclick={hero.action.run}>{hero.action.label} <ConsoleIcon name="arrow" /></button>
      {/if}
    </div>
  </section>

  <section class="metric-rail" aria-label="Monitoring summary">
    <div class="metric">
      <div class="metric-label">Operational monitors</div>
      <div class="metric-value">{loading || error ? '—' : up}<small>{' / '}{loading || error ? '—' : activeChecks.length}</small></div>
      <div class="metric-meta">
        {#if loading}Loading…{:else if error}Current states unknown{:else if !checks.length}No monitors yet{:else if !activeChecks.length}<span class="quiet">No active monitors</span>{:else if !attention.length}<span class="good">✓ All operational</span>{:else}
          <span class="good">{up} operational</span>
          {#if degraded}<span class="warn">{degraded} degraded</span>{/if}
          {#if down}<span class="bad">{down} down</span>{/if}
          {#if unknown}<span class="quiet">{unknown} unknown</span>{/if}
        {/if}
        {#if hasMore && !loading}<span class="quiet">First {checks.length} of {totalChecks}</span>{/if}
      </div>
    </div>
    <div class="metric">
      <div class="metric-label">Active incidents</div>
      <div class="metric-value">{loading || error ? '—' : activeCount}</div>
      <div class="metric-meta">{loading || error ? 'Cannot confirm current count' : feed[0] ? `${stateText(feed[0].state)} · ${elapsed(feed[0].started_at)}` : 'No active incidents'}</div>
    </div>
    <div class="metric">
      <div class="metric-label">Reporting regions</div>
      <div class="metric-value">{stations.length ? reportingStations : '—'}{#if stations.length}<small>{' / '}{stations.length}</small>{/if}</div>
      <div class="metric-meta">{!stations.length ? 'No regions reporting yet' : reportingStations === stations.length ? 'Latest reports received' : `${stations.length - reportingStations} not reporting`}</div>
    </div>
    <div class="metric">
      <div class="metric-label">Live agents</div>
      <div class="metric-value">{liveAgents ? onlineAgents : '—'}{#if liveAgents}<small>{' / '}{liveAgents.length}</small>{/if}</div>
      <div class="metric-meta">
        {#if !liveAgents}{connectivityError ? 'Connectivity unknown' : 'Checking connectivity…'}{:else if !liveAgents.length}No agents have reported{:else if staleAgents.length}<span class="warn">{staleAgents.length} {staleAgents.length === 1 ? 'agent' : 'agents'} not reporting</span>{:else}All agents reporting{/if}
      </div>
    </div>
  </section>

  <div class="dashboard-grid">
    <div class="primary-column">
      <section class="panel" aria-labelledby="incidents-title">
        <div class="panel-head">
          <div class="panel-heading"><h2 id="incidents-title">Active incidents</h2><span class="counter">{loading || error ? '—' : activeCount}</span></div>
          <button class="text-link" onclick={() => onPanel('incidents')}>View all ↗</button>
        </div>
        {#if loading || error}
          <div class="state-notice">△ Incident feed unavailable</div>
          <div class="empty">Current incident state cannot be confirmed.</div>
        {:else if feed.length}
          {#each feed.slice(0, 2) as item (item.id)}
            {@const update = latestUpdate(item)}
            <div class="incident" class:quiet-incident={item.state === 'monitoring'}>
              <div class="incident-top">
                <span class="pill" class:bad-pill={item.state === 'detected'}>◈ {stateText(item.state)}</span>
                <span class="tiny quiet">Started {timeFmt.format(item.started_at)} <span aria-hidden="true">·</span> {elapsed(item.started_at)}</span>
              </div>
              <h3>{item.title}</h3>
              <p>{incidentScope(item)}</p>
              {#if update?.body}<div class="incident-update">{update.body}</div>{/if}
              <div class="incident-bottom">
                <span>Updated {timeFmt.format(update?.created_at ?? item.started_at)}</span>
                <button class="text-link" onclick={() => openIncident(item)}>View incident →</button>
              </div>
            </div>
          {/each}
        {:else}
          <div class="empty"><span class="good">✓ No active incidents</span><br />All tracked incidents are resolved.</div>
        {/if}
      </section>

      <section class="panel" aria-labelledby="regions-title">
        <div class="panel-head">
          <div class="panel-heading"><h2 id="regions-title">Regional stations</h2><span class="counter">{stations.length}</span></div>
          <button class="text-link" onclick={() => onPanel('region-tree')}>Explore regions ↗</button>
        </div>
        {#if stations.length}
          <div class="region-meta"><span>Service health by region</span><span>Reporting to <span class="mono-text">{cluster?.self ?? nodeMode}</span></span></div>
          <div class="stations" style="--stations: {Math.min(stations.length, 4)}">
            {#each stations as station (station.name)}
              <div class="station" class:affected={station.affected > 0} class:unknown={station.unknownOnly}>
                <span class="station-beacon"><ConsoleIcon name="station" /></span>
                <div>
                  <button onclick={() => onPanel('region-tree')}><span class="station-name mono-text">{station.name}</span></button>
                  <div class="station-location">{station.location}</div>
                </div>
                <div class="station-summary">
                  <div class="station-state {station.unknownOnly ? 'quiet' : station.failing ? 'bad' : station.affected ? 'warn' : 'good'}">
                    {station.unknownOnly ? '○ Unknown' : station.affected ? `${station.failing ? '×' : '△'} ${station.affected} affected` : '✓ Operational'}
                  </div>
                  <div class="station-details">
                    {station.agentsTotal === null ? '—' : `${station.agentsLive ?? 0} / ${station.agentsTotal}`} agents reporting<br />{station.freshness}
                  </div>
                </div>
              </div>
            {/each}
          </div>
          <div class="region-foot"><span>Regions report independently to global · no inter-region connection</span><span>{regionTree ? 'Region tree · refreshed every minute' : 'Region health unavailable'}</span></div>
        {:else}
          <div class="empty">Regional stations appear once agents report from a region.</div>
        {/if}
      </section>

      <section class="panel" aria-labelledby="monitors-title" bind:this={monitorsPanel}>
        <div class="panel-head">
          <div class="panel-heading"><h2 id="monitors-title">Monitor health</h2><span class="counter">{totalChecks}</span></div>
          <button class="text-link" onclick={() => onPanel('checks')}>All monitors ↗</button>
        </div>
        <div class="toolbar">
          <div class="tabs" role="group" aria-label="Monitor status filter">
            <button class="tab" aria-pressed={filter === 'all'} onclick={() => (filter = 'all')}>All</button>
            <button class="tab" aria-pressed={filter === 'attention'} onclick={() => (filter = 'attention')}>Needs attention</button>
          </div>
          <label class="search">
            <ConsoleIcon name="search" />
            <span class="sr-only">Filter monitors</span>
            <input bind:value={query} placeholder="Filter monitors…" type="search" />
          </label>
        </div>
        <div class="monitor-head" aria-hidden="true"><span>Monitor</span><span>Current state</span><span class="right">Response</span><span class="right">24h uptime</span></div>
        {#if loading}
          <div class="empty">Loading current monitor states…</div>
        {:else if !checks.length}
          <div class="empty">No monitors yet. <button class="text-link inline" onclick={onAddCheck}>Add your first monitor →</button></div>
        {:else if !featuredChecks.length}
          <div class="empty">No monitors match this filter.</div>
        {:else}
          {#each featuredChecks as check (check.check_id)}
            <button
              class="monitor-row"
              onclick={() => onOpenCheck(check.check_id)}
              aria-label={check.enabled === false ? `${check.name}, Paused. Open details` : `${check.name}, ${STATE_LABELS[check.status]}, response ${formatLatency(check.latency_ms)}, 24-hour uptime ${formatUptime(check.uptime_24h)}. Open details`}
            >
              <span>
                <span class="monitor-name">{check.name}</span>
                <span class="monitor-type">{TYPE_LABELS[check.type ?? ''] ?? 'Monitor'}{#if check.group} <span aria-hidden="true">·</span> {check.group}{/if}</span>
              </span>
              <span class="monitor-status {check.enabled === false ? 'quiet' : TONES[check.status]}"><span aria-hidden="true">{check.enabled === false ? '○' : MARKS[check.status]}</span>{check.enabled === false ? 'Paused' : STATE_LABELS[check.status]}</span>
              <span class="number"><span class="mobile-label">Response </span>{check.enabled === false ? '—' : formatLatency(check.latency_ms)}</span>
              <span class="number uptime"><span class="mobile-label">24h </span>{check.enabled === false ? '—' : formatUptime(check.uptime_24h)}</span>
              <span class="monitor-history" aria-hidden="true">
                <span class="history-label">24h ago</span>
                <span class="uptime-strip">{#each bars(check) as tone, index (index)}<span class="uptime-bar {tone}"></span>{/each}</span>
                <span class="history-label">Now</span>
              </span>
            </button>
          {/each}
        {/if}
        <div class="monitor-footer">
          <span>{featuredChecks.length} of {filteredChecks.length} shown · {totalChecks} total monitors</span>
          <span class="legend"><span><i></i>Up</span><span><i class="fail"></i>Failure</span><span><i class="degr"></i>Degraded</span><span><i class="missing"></i>No data</span></span>
        </div>
      </section>
    </div>

    <div class="secondary-column">
      <section class="panel" aria-labelledby="attention-title">
        <div class="panel-head">
          <h2 id="attention-title">Needs attention</h2>
          <span class="quiet"><ConsoleIcon name="alert" /></span>
        </div>
        {#if loading || disconnected}
          <div class="empty"><span class="warn">△ Restore visibility first</span><br />Current monitor states cannot be confirmed until the status feed reconnects.</div>
        {:else if attentionItems.length}
          {#each attentionItems as item (item.key)}
            <button class="attention-item" onclick={item.run}>
              <div class="attention-title"><ConsoleIcon name={item.icon} /><span class={item.tone}>{item.title}</span></div>
              <p>{#if item.mono}<span class="mono-text">{item.mono}</span><br />{/if}{item.body}</p>
              <div class="action">{item.action}</div>
            </button>
          {/each}
        {:else}
          <div class="empty"><span class="good">✓ Nothing needs attention</span><br />No failing monitors, stale agents, or unassigned checks.</div>
        {/if}
      </section>

      <section class="panel" aria-labelledby="connectivity-title">
        <div class="panel-head">
          <h2 id="connectivity-title">Fleet connectivity</h2>
          <button class="text-link" onclick={() => onPanel('cluster')}>Details ↗</button>
        </div>
        <div class="fleet-content">
          <div class="fleet-line">
            <span>Probe agents</span>
            <span class={!liveAgents ? 'quiet' : staleAgents.length ? 'warn' : 'good'}>{liveAgents ? `${onlineAgents} / ${liveAgents.length} online` : 'Unknown'}</span>
          </div>
          {#if liveAgents?.length}
            {@const count = Math.min(SEGMENTS, liveAgents.length)}
            {@const online = Math.round((onlineAgents / liveAgents.length) * count)}
            <div class="fleet-segments" aria-hidden="true">{#each Array.from({ length: count }, (_, index) => index) as index (index)}<span class:stale={index >= online}></span>{/each}</div>
          {/if}
          <p class="fleet-note">
            {#if !liveAgents}{connectivityError ? 'Agent connectivity could not be loaded.' : 'Checking agent connectivity…'}{:else if !liveAgents.length}No agents have reported yet.{:else if staleAgents.length}{staleAgents.length} stale · {staleAgents.slice(0, 2).map((agent) => agent.agent_id).join(', ')}{staleAgents.length > 2 ? '…' : ''}{:else}All agents are reporting.{/if}
          </p>
          <div class="fleet-divider"></div>
          <div class="fleet-line">
            <span>{nodeMode === 'global' ? 'Regional reporting' : 'Peer coordinators'}</span>
            <span class={!cluster ? 'quiet' : cluster.live < cluster.total ? 'warn' : 'good'}>{!cluster ? 'Unknown' : cluster.total ? `${cluster.live} / ${cluster.total} connected` : 'Standalone'}</span>
          </div>
          <p class="fleet-note">
            {#if nodeMode === 'global'}Global ← regional coordinators<br />Reporting freshness, not peer gossip health.{:else if cluster && !cluster.total}This coordinator runs without peers.{:else}Gossip membership as seen by this coordinator.{/if}
          </p>
        </div>
      </section>

      <section class="panel events-panel" aria-labelledby="events-title">
        <div class="panel-head">
          <h2 id="events-title">Recent activity</h2>
          <button class="text-link" onclick={() => onPanel('alerts')}>View feed ↗</button>
        </div>
        {#if alerts.length}
          <div class="activity">
            {#each alerts.slice(0, 3) as alert (alert.id)}
              <button class="event" onclick={() => (alert.incident_id ? onOpenIncident(alert.incident_id) : onPanel('alerts'))}>
                <span class="event-marker {eventTone(alert)}" aria-hidden="true">{MARKS[alert.current_state] ?? '○'}</span>
                <span><span class="event-title">{alertLabel(alert)}</span><small>{eventDetail(alert)}</small></span>
                <time datetime={new Date(alert.timestamp).toISOString()}>{timeFmt.format(alert.timestamp)}</time>
              </button>
            {/each}
          </div>
        {:else}
          <div class="empty">No recent state changes.</div>
        {/if}
        <div class="activity-foot">{dateFmt.format(Date.now())} · Local time · Recent events, not an audit log</div>
      </section>
    </div>
  </div>

  <footer class="page-footer">
    <span>Lightkeeper <span class="quiet">/</span> Calm in the storm.</span>
    <span>{modeLabel} view · {stations.length} {stations.length === 1 ? 'region' : 'regions'}{updatedAt ? ` · Latest check ${timeFmt.format(updatedAt)}` : ''}</span>
  </footer>
</div>

<style>
  .overview {
    --bg: #080f19;
    --panel: #101b29;
    --hover: #152334;
    --border: #223247;
    --text: #e8eff5;
    --muted: #a0afc0;
    --quiet: #8a9aad;
    --green: #56c9a1;
    --amber: #e7b66b;
    --red: #ee8389;
    --focus: #83b8ce;
    color: var(--text);
    font-size: 14px;
    line-height: 1.5;
    font-variant-numeric: tabular-nums;
  }
  button,
  input {
    font: inherit;
  }
  button {
    color: inherit;
    border: 0;
    background: none;
    cursor: pointer;
    transition: background-color 0.15s, color 0.15s, border-color 0.15s;
  }
  button:hover {
    background-color: var(--hover);
  }
  button:focus-visible,
  input:focus-visible {
    outline: 2px solid var(--focus);
    outline-offset: 3px;
  }
  h1,
  h2,
  h3,
  p {
    margin: 0;
  }
  h2 {
    font-size: 14px;
    font-weight: 600;
  }
  h3 {
    font-size: 14px;
  }
  .muted { color: var(--muted); }
  .quiet { color: var(--quiet); }
  .good { color: var(--green); }
  .warn { color: var(--amber); }
  .bad { color: var(--red); }
  .tiny { font-size: 12px; }
  .mono-text { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }
  .right { text-align: right; }
  .hero {
    height: 228px;
    position: relative;
    isolation: isolate;
    overflow: visible;
    z-index: 10;
    display: flex;
    align-items: center;
  }
  .hero-copy {
    position: relative;
    z-index: 2;
    max-width: 68%;
    padding-bottom: 6px;
  }
  .hero-kicker {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 15px;
    font-size: 11px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--muted);
  }
  .hero h1 {
    font-size: 32px;
    line-height: 1.16;
    letter-spacing: -1.1px;
    font-weight: 550;
    margin-bottom: 13px;
  }
  .hero p {
    font-size: 13px;
    color: var(--muted);
    max-width: 460px;
    line-height: 1.7;
  }
  .hero-link {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: var(--text);
    font-size: 12px;
    padding: 8px 0;
    margin-top: 10px;
  }
  .hero-link:hover {
    background: none;
    color: var(--focus);
  }
  .hero:after {
    content: '';
    position: absolute;
    inset: 0;
    z-index: 1;
    background: linear-gradient(90deg, var(--bg) 5%, transparent 75%);
    pointer-events: none;
  }
  .metric-rail {
    display: grid;
    grid-template-columns: 1.45fr 1fr 1fr 1fr;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: #0d1723;
    margin-bottom: 20px;
  }
  .metric {
    padding: 15px 20px;
    position: relative;
    min-width: 0;
  }
  .metric + .metric:before {
    content: '';
    position: absolute;
    left: 0;
    top: 18px;
    bottom: 18px;
    border-left: 1px solid var(--border);
  }
  .metric-label {
    font-size: 11px;
    color: var(--muted);
    margin-bottom: 6px;
    display: flex;
    align-items: center;
    gap: 7px;
  }
  .metric-value {
    font-size: 26px;
    letter-spacing: -0.8px;
    line-height: 1.2;
  }
  .metric-value small {
    font-size: 13px;
    color: var(--muted);
    letter-spacing: 0;
  }
  .metric-meta {
    font-size: 11px;
    color: var(--muted);
    margin-top: 5px;
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .dashboard-grid {
    display: grid;
    grid-template-columns: minmax(0, 1.9fr) minmax(280px, 1fr);
    gap: 16px;
    align-items: start;
  }
  .primary-column,
  .secondary-column {
    display: grid;
    gap: 16px;
    min-width: 0;
  }
  .panel {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 10px;
    overflow: hidden;
  }
  .panel-head {
    min-height: 51px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 12px 18px;
    border-bottom: 1px solid var(--border);
  }
  .panel-heading {
    display: flex;
    align-items: center;
    gap: 9px;
  }
  .counter {
    font-size: 10px;
    color: var(--muted);
    border: 1px solid var(--border);
    padding: 0 5px;
    border-radius: 4px;
  }
  .text-link {
    font-size: 11px;
    color: var(--muted);
    padding: 5px 0;
    min-height: 28px;
  }
  .text-link:hover {
    background: none;
    color: var(--text);
  }
  .text-link.inline {
    color: var(--focus);
    font-size: 12px;
  }
  .incident {
    padding: 17px 18px 15px;
    position: relative;
  }
  .incident + .incident {
    border-top: 1px solid var(--border);
  }
  .incident:before {
    content: '';
    position: absolute;
    left: 0;
    top: 18px;
    bottom: 18px;
    width: 2px;
    background: var(--red);
  }
  .incident.quiet-incident:before {
    background: var(--amber);
  }
  .incident-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 10px;
  }
  .pill {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    border-radius: 4px;
    padding: 2px 7px;
    font-size: 10px;
    background: #322b24;
    color: var(--amber);
    border: 1px solid #554633;
  }
  .pill.bad-pill {
    background: #32212b;
    color: var(--red);
    border-color: #56323e;
  }
  .incident h3 {
    font-weight: 550;
    letter-spacing: -0.1px;
    margin-bottom: 5px;
  }
  .incident p {
    font-size: 12px;
    color: var(--muted);
  }
  .incident-update {
    border-left: 1px solid #3d4b5a;
    padding-left: 10px;
    margin-top: 12px;
    line-height: 1.6;
    font-size: 11px;
    color: var(--muted);
  }
  .incident-bottom {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 12px;
    font-size: 11px;
    color: var(--quiet);
  }
  .region-meta {
    padding: 12px 18px 0;
    font-size: 10px;
    color: var(--muted);
    display: flex;
    justify-content: space-between;
    gap: 8px;
  }
  .stations {
    display: grid;
    grid-template-columns: repeat(var(--stations, 3), minmax(0, 1fr));
    padding: 19px 18px 17px;
    gap: 16px;
    position: relative;
  }
  /* Regions are siblings reporting to global, not peers in a network chain.
     Never draw a line through unrelated regional station nodes. */
  .station {
    min-width: 0;
    position: relative;
  }
  .station-beacon {
    width: 26px;
    height: 26px;
    display: grid;
    place-items: center;
    background: var(--panel);
    border: 1px solid #385d59;
    border-radius: 50%;
    color: var(--green);
    margin-bottom: 12px;
  }
  .station.affected .station-beacon {
    color: var(--amber);
    border-color: #6d5437;
  }
  .station.unknown .station-beacon {
    color: var(--quiet);
    border-color: var(--border);
  }
  .station-name {
    font-size: 12px;
    font-weight: 600;
  }
  .station-location {
    font-size: 10px;
    color: var(--quiet);
    margin-top: 1px;
  }
  .station-state {
    font-size: 11px;
    margin: 9px 0 6px;
  }
  .station-details {
    font-size: 10px;
    color: var(--muted);
    line-height: 1.9;
  }
  .station button {
    padding: 0;
    text-align: left;
  }
  .station button:hover {
    background: none;
    color: var(--focus);
  }
  .region-foot {
    border-top: 1px solid var(--border);
    padding: 9px 18px;
    font-size: 10px;
    color: var(--quiet);
    display: flex;
    justify-content: space-between;
    gap: 10px;
  }
  .toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  .tabs {
    display: flex;
    gap: 4px;
  }
  .tab {
    font-size: 11px;
    border-radius: 5px;
    padding: 5px 8px;
    color: var(--muted);
    min-height: 30px;
  }
  .tab[aria-pressed='true'] {
    background: #233344;
    color: var(--text);
  }
  .search {
    display: flex;
    align-items: center;
    gap: 6px;
    border: 1px solid var(--border);
    border-radius: 5px;
    padding: 0 8px;
    max-width: 170px;
    color: var(--quiet);
  }
  .search input {
    width: 100%;
    min-width: 0;
    background: transparent;
    border: 0;
    color: var(--text);
    font-size: 11px;
    height: 30px;
    outline: none;
  }
  .search:focus-within {
    outline: 2px solid var(--focus);
    outline-offset: 2px;
  }
  .search input::placeholder {
    color: var(--quiet);
  }
  .monitor-head,
  .monitor-row {
    display: grid;
    grid-template-columns: minmax(130px, 1.5fr) minmax(90px, 1fr) 60px 70px;
    gap: 12px;
    align-items: center;
  }
  .monitor-head {
    padding: 9px 18px;
    font-size: 10px;
    color: var(--quiet);
    background: #0e1926;
  }
  .monitor-row {
    width: 100%;
    text-align: left;
    padding: 12px 18px;
    border-top: 1px solid #1c2c3e;
    min-height: 61px;
    border-radius: 0;
  }
  .monitor-row > span:first-child {
    min-width: 0;
  }
  .monitor-name {
    font-size: 12px;
    font-weight: 550;
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .monitor-type {
    font-size: 10px;
    color: var(--quiet);
  }
  .monitor-status {
    font-size: 10px;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .number {
    text-align: right;
    font-size: 11px;
  }
  .mobile-label {
    display: none;
  }
  .monitor-history {
    grid-column: 1/-1;
    display: flex;
    align-items: center;
    gap: 10px;
    margin-top: -1px;
  }
  .uptime-strip {
    display: flex;
    gap: 2px;
    flex: 1;
    height: 12px;
    min-width: 0;
  }
  .uptime-bar {
    background: #347865;
    flex: 1;
    border-radius: 1px;
    min-width: 0;
  }
  .uptime-bar.down {
    background: var(--red);
  }
  .uptime-bar.degraded {
    background: var(--amber);
  }
  .uptime-bar.unknown {
    background: repeating-linear-gradient(135deg, #657489 0 1px, transparent 1px 3px);
    border: 1px solid #627185;
  }
  .history-label {
    font-size: 9px;
    color: var(--quiet);
    white-space: nowrap;
  }
  .monitor-footer {
    padding: 11px 18px;
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 8px;
    border-top: 1px solid var(--border);
    font-size: 10px;
    color: var(--quiet);
  }
  .legend {
    display: flex;
    gap: 9px;
  }
  .legend span {
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .legend i {
    height: 6px;
    width: 6px;
    background: #347865;
    border-radius: 1px;
  }
  .legend i.fail {
    background: var(--red);
  }
  .legend i.degr {
    background: var(--amber);
  }
  .legend i.missing {
    background: none;
    border: 1px solid var(--quiet);
  }
  .attention-item {
    display: block;
    text-align: left;
    width: 100%;
    padding: 16px 18px;
  }
  .attention-item + .attention-item {
    border-top: 1px solid var(--border);
  }
  .attention-title {
    display: flex;
    align-items: center;
    gap: 9px;
    font-size: 12px;
    font-weight: 550;
  }
  .attention-item p {
    padding-left: 27px;
    margin-top: 6px;
    color: var(--muted);
    font-size: 11px;
    line-height: 1.7;
    overflow-wrap: anywhere;
  }
  .attention-item .action {
    padding-left: 27px;
    margin-top: 8px;
    font-size: 10px;
    color: var(--focus);
  }
  .fleet-content {
    padding: 15px 18px;
  }
  .fleet-line {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 12px;
    margin-bottom: 10px;
  }
  .fleet-segments {
    display: flex;
    gap: 4px;
    height: 5px;
    margin: 12px 0 8px;
  }
  .fleet-segments span {
    flex: 1;
    border-radius: 1px;
    background: #347865;
  }
  .fleet-segments .stale {
    background: #a57b46;
  }
  .fleet-note {
    font-size: 10px;
    color: var(--muted);
  }
  .fleet-divider {
    border-top: 1px solid var(--border);
    margin: 16px 0;
  }
  .activity {
    padding: 0 18px;
  }
  .event {
    position: relative;
    display: grid;
    grid-template-columns: 10px 1fr auto;
    gap: 9px;
    width: 100%;
    text-align: left;
    padding: 15px 0;
    border-bottom: 1px solid var(--border);
    border-radius: 0;
    align-items: start;
  }
  .event:hover {
    background: none;
  }
  .event:hover .event-title {
    color: var(--focus);
  }
  .event:last-child {
    border-bottom: 0;
  }
  .event-marker {
    font-size: 12px;
    margin-top: 2px;
  }
  .event-title {
    display: block;
    font-size: 11px;
  }
  .event small {
    font-size: 10px;
    color: var(--quiet);
    display: block;
    margin-top: 4px;
  }
  .event time {
    font-size: 10px;
    color: var(--muted);
    padding-top: 2px;
  }
  .activity-foot {
    font-size: 10px;
    color: var(--quiet);
    padding: 10px 18px;
    border-top: 1px solid var(--border);
  }
  .page-footer {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    padding-top: 20px;
    color: var(--quiet);
    font-size: 10px;
  }
  .empty {
    padding: 25px 18px;
    font-size: 12px;
    color: var(--muted);
    line-height: 1.8;
  }
  .state-notice {
    padding: 12px 18px;
    background: #20232a;
    color: var(--amber);
    border-bottom: 1px solid var(--border);
    font-size: 12px;
  }
  .sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }
  @media (min-width: 768px) {
    .hero {
      height: 210px;
    }
    .metric {
      padding: 12px 18px;
    }
    .panel-head {
      min-height: 45px;
      padding: 8px 18px;
    }
    .incident {
      padding: 13px 18px;
    }
    .incident-top {
      margin-bottom: 7px;
    }
    .incident-update {
      margin-top: 8px;
    }
    .incident-bottom {
      margin-top: 6px;
    }
    .stations {
      padding: 14px 18px 12px;
      gap: 14px;
    }
    .stations:before {
      top: 27px;
    }
    .station-beacon {
      margin-bottom: 6px;
    }
    .station-location {
      line-height: 1.3;
    }
    .station-state {
      margin: 6px 0 3px;
    }
    .station-details {
      line-height: 1.6;
    }
    .region-meta {
      padding-top: 8px;
    }
    .region-foot {
      padding: 7px 18px;
    }
    .monitor-row {
      padding: 9px 18px;
      row-gap: 5px;
    }
    .monitor-type {
      display: block;
      line-height: 1.4;
    }
    .monitor-name {
      line-height: 1.5;
    }
    .monitor-head {
      padding-top: 7px;
      padding-bottom: 7px;
    }
  }
  @media (min-width: 1600px) {
    .hero h1 {
      font-size: 36px;
    }
    .hero {
      height: 245px;
    }
    .monitor-row {
      min-height: 70px;
    }
  }
  @media (max-width: 1150px) {
    .dashboard-grid {
      grid-template-columns: minmax(0, 1fr);
    }
    .secondary-column {
      grid-template-columns: 1fr 1fr;
    }
    .secondary-column .events-panel {
      grid-column: 1/-1;
    }
    .activity {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 20px;
    }
    .event {
      border: 0;
    }
    .hero h1 {
      font-size: 29px;
    }
    .metric {
      padding: 15px;
    }
    .monitor-head,
    .monitor-row {
      grid-template-columns: minmax(150px, 1.5fr) 1fr 85px 90px;
    }
  }
  @media (max-width: 767px) {
    .hero {
      height: 205px;
    }
    .hero-copy {
      max-width: 85%;
    }
    .hero h1 {
      font-size: 28px;
      max-width: 330px;
    }
    .hero p {
      font-size: 12px;
      max-width: 300px;
    }
    .hero-kicker {
      font-size: 10px;
      margin-bottom: 12px;
    }
    .metric-rail {
      grid-template-columns: 1fr 1fr;
    }
    .metric:nth-child(3),
    .metric:nth-child(4) {
      border-top: 1px solid var(--border);
    }
    .metric:nth-child(3):before {
      display: none;
    }
    .metric {
      padding: 13px 16px;
    }
    .metric-value {
      font-size: 24px;
    }
    .secondary-column {
      grid-template-columns: 1fr;
    }
    .secondary-column .events-panel {
      grid-column: auto;
    }
    .activity {
      display: block;
    }
    .event {
      border-bottom: 1px solid var(--border);
    }
    .stations {
      grid-template-columns: 1fr;
      padding: 8px 18px;
      gap: 0;
    }
    .stations:before {
      display: none;
    }
    .station {
      display: grid;
      grid-template-columns: 28px 1fr 1fr;
      gap: 10px;
      padding: 14px 0;
      align-items: start;
    }
    .station + .station {
      border-top: 1px solid var(--border);
    }
    .station-beacon {
      margin-top: 2px;
    }
    .station-state {
      margin: 0 0 4px;
    }
    .station-summary {
      grid-column: 3;
    }
    .region-meta,
    .region-foot {
      flex-direction: column;
      gap: 2px;
    }
    .panel-head {
      padding: 12px 16px;
    }
    .toolbar {
      flex-wrap: wrap;
    }
    .search {
      max-width: none;
      flex: 1;
      min-width: 120px;
    }
    .monitor-head {
      display: none;
    }
    .monitor-row {
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 7px;
      padding: 14px 16px;
    }
    .monitor-status {
      justify-content: flex-end;
    }
    .monitor-row .number {
      text-align: left;
      color: var(--muted);
    }
    .monitor-row .number.uptime {
      text-align: right;
    }
    .mobile-label {
      display: inline;
    }
    .monitor-history {
      margin-top: 4px;
    }
    .monitor-footer {
      padding: 12px 16px;
    }
    .page-footer {
      align-items: flex-start;
      flex-direction: column;
    }
    .tab,
    .text-link,
    .hero-link {
      min-height: 44px;
    }
    .incident-top {
      flex-wrap: wrap;
    }
  }
  @media (prefers-reduced-motion: reduce) {
    button {
      transition: none;
    }
  }
</style>
