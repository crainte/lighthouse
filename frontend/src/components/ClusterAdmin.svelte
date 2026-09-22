<script lang="ts">
  import { onDestroy } from 'svelte';
  import type { ClusterLink, ClusterMember, ClusterResponse } from '../lib/admin';
  import { AdminError, buildIdentity, fetchCluster } from '../lib/admin';
  import { classifyLink, graphLayout, linkPath } from '../lib/clusterGraph';
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
  let graphDetail = $state<string | null>(null);
  // Polling must not overlap: a slow request should make an interval tick a
  // no-op rather than allowing an older response to overwrite a newer one.
  let refreshInFlight = false;

  function selectGraphItem(event: KeyboardEvent | MouseEvent, detail: string) {
    if (event instanceof KeyboardEvent && event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    graphDetail = detail;
  }

  let nodes = $derived(cluster ? graphLayout(cluster.self, cluster.members) : []);
  let edges = $derived.by(() => {
    const current = cluster;
    if (!current) return [];
    const byId = new Map(nodes.map((node) => [node.id, node]));
    const links = current.links ?? [];
    const observations = new Map(links.map((link) => [`${link.source}\0${link.target}`, link]));
    const pairs: { source: string; target: string; observation?: ClusterLink }[] = [];
    if (current.mode === 'global') {
      for (const member of current.members) {
        pairs.push({
          source: member.node_id,
          target: current.self.node_id,
          observation: observations.get(`${member.node_id}\0${current.self.node_id}`),
        });
      }
    } else {
      for (const source of nodes) {
        for (const target of nodes) {
          if (source.id !== target.id) {
            pairs.push({
              source: source.id,
              target: target.id,
              observation: observations.get(`${source.id}\0${target.id}`),
            });
          }
        }
      }
    }
    return pairs.flatMap((edge) => {
      const source = byId.get(edge.source);
      const target = byId.get(edge.target);
      if (!source || !target) return [];
      return [{
        ...edge,
        sourceNode: source,
        targetNode: target,
        state: classifyLink(
          edge.observation,
          current.now,
          current.link_stale_after_ms ?? 0,
        ),
      }];
    });
  });

  function edgeLabel(edge: (typeof edges)[number]): string {
    const kind = cluster?.mode === 'global' ? 'reporting' : 'connectivity';
    const seen = edge.observation
      ? formatRelativeTime(edge.observation.observed_at, cluster!.now)
      : 'never observed';
    return `${edge.source} to ${edge.target}: ${edge.state} ${kind}, ${seen}`;
  }

  function tone(state: ClusterMember['state']): 'green' | 'amber' | 'red' {
    switch (state) {
      case 'alive':
        return 'green';
      case 'suspect':
        return 'amber';
      case 'dead':
        return 'red';
    }
  }

  // A cluster mid-upgrade is the normal state during a deploy and a fault only
  // if it stays that way, so this reports rather than alarms: peers running
  // something other than this node get marked, and nothing is blocked.
  let skewed = $derived(
    cluster
      ? cluster.members.filter(
          (m) =>
            m.state !== 'dead' &&
            m.build &&
            cluster!.self.build &&
            buildIdentity(m.build) !== buildIdentity(cluster!.self.build),
        )
      : [],
  );

  // Silence is not disagreement. A peer too old to report a build cannot be
  // compared, and counting it as skewed would flag every cluster that has not
  // finished its first upgrade to a build that reports one.
  let unreported = $derived(
    cluster ? cluster.members.filter((m) => m.state !== 'dead' && !m.build).length : 0,
  );

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
      cluster = await fetchCluster();
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
  <p class="admin-error">{loadError ?? 'Session expired.'}</p>
  <p class="admin-muted">Your session expired. Sign in again to view the cluster.</p>
  <div class="range-switch retry">
    <button class="range-btn" onclick={onexpired}>Sign in again</button>
  </div>
{:else if loading}
  <p class="admin-muted">Loading…</p>
{:else if loadError}
  <p class="admin-error">{loadError}</p>
  <div class="range-switch retry">
    <button class="range-btn" onclick={refresh}>Retry</button>
  </div>
{:else if cluster}
  <div class="stat-grid">
    <div class="stat-card">
      <div class="stat-label">This node</div>
      <div class="stat-value cluster-id">{cluster.self.node_id}</div>
      <div class="stat-sub">{cluster.self.url}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">State</div>
      <div class="stat-value {tone(cluster.self.state)}">
        {cluster.self.state}
      </div>
      <div class="stat-sub">generation {cluster.self.generation}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Peers</div>
      <div class="stat-value">{cluster.members.length}</div>
      <div class="stat-sub">
        {cluster.members.filter((m) => m.state === 'alive').length} alive
      </div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Build</div>
      <div class="stat-value cluster-id" title={cluster.self.build ?? ''}>
        {cluster.self.build ?? 'unknown'}
      </div>
      <div class="stat-sub">
        {#if skewed.length > 0}
          <span class="amber"
            >{skewed.length}
            {skewed.length === 1 ? 'peer runs' : 'peers run'} something else</span
          >
        {:else if unreported > 0}
          {unreported}
          {unreported === 1 ? 'peer does' : 'peers do'} not report one
        {:else}
          whole cluster matches
        {/if}
      </div>
    </div>
  </div>

  {#if skewed.length > 0}
    <p class="admin-muted skew-note">
      Peers are running a different build from this node. That is expected while a rolling upgrade
      is in progress and worth investigating if it persists: an agent polling an older coordinator
      can be handed a different set of checks, and a coordinator older than the one that configured
      a check may not understand every field on it.
    </p>
  {/if}

  {#if cluster.members.length === 0}
    <p class="admin-muted">Clustering is not configured — this coordinator is standalone.</p>
  {:else}
    <div class="view-bar">
      <div class="range-switch" role="group" aria-label="Cluster view">
        <button class="range-btn" class:active={view === 'graph'} onclick={() => (view = 'graph')}>Graph</button>
        <button class="range-btn" class:active={view === 'table'} onclick={() => (view = 'table')}>Table</button>
      </div>
    </div>

    {#if view === 'graph'}
      <section class="graph-panel" aria-label="Cluster connectivity graph">
        <div class="graph-copy">
          <strong>{cluster.mode === 'global' ? 'Region reporting' : 'Direct ping connectivity'}</strong>
          <span>
            {cluster.mode === 'global'
              ? 'Arrows show fresh state reports received by the global node.'
              : 'Each arrow is the source coordinator’s latest direct ping result.'}
          </span>
        </div>
        <div class="legend" aria-label="Connectivity legend">
          <span class="connected">connected</span><span class="failed">failed</span><span class="stale">stale</span><span class="unknown">unknown</span>
        </div>
        <svg id="cluster-graph" class="cluster-graph" viewBox="0 0 800 440" role="img" aria-label="Cluster connectivity">
          <defs>
            {#each ['connected', 'failed', 'stale', 'unknown'] as state}
              <marker id="arrow-{state}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" class="marker {state}"></path>
              </marker>
            {/each}
          </defs>
          {#each edges as edge (`${edge.source}-${edge.target}`)}
            <path
              class="edge {edge.state}"
              d={linkPath(edge.sourceNode, edge.targetNode, 1)}
              marker-end="url(#arrow-{edge.state})"
              role="button"
              tabindex="0"
              aria-label={edgeLabel(edge)}
              onclick={(event) => selectGraphItem(event, edgeLabel(edge))}
              onkeydown={(event) => selectGraphItem(event, edgeLabel(edge))}
            ><title>{edgeLabel(edge)}</title></path>
          {/each}
          {#each nodes as node (node.id)}
            {@const nodeLabel = `${node.id}: ${node.member.state}${node.self ? ', this node' : ''}`}
            <g
              class="graph-node"
              role="button"
              tabindex="0"
              aria-label={nodeLabel}
              onclick={(event) => selectGraphItem(event, nodeLabel)}
              onkeydown={(event) => selectGraphItem(event, nodeLabel)}
            >
              <circle cx={node.x} cy={node.y} r={node.self ? 39 : 34} class="node-ring {tone(node.member.state)}" />
              <circle cx={node.x} cy={node.y} r={node.self ? 31 : 27} class="node-fill" />
              <text x={node.x} y={node.y + 4} text-anchor="middle">{node.id}</text>
              {#if node.self}<text class="self-label" x={node.x} y={node.y + 55} text-anchor="middle">self</text>{/if}
              <title>{nodeLabel}</title>
            </g>
          {/each}
        </svg>
        {#if graphDetail}
          <p class="graph-detail" aria-live="polite">{graphDetail}</p>
        {:else if (cluster.links ?? []).length === 0}
          <p class="admin-muted graph-empty">No link observations yet. The graph will fill after gossip completes.</p>
        {/if}
      </section>
    {:else}
      {@const now = cluster.now}
      <div class="otable">
        <div class="ot-head">
          <span>Node</span><span>URL</span><span>State</span><span>Build</span><span>Last seen</span>
        </div>
        <div class="ot-row">
          <span class="node-cell"><span class="ot-id">{cluster.self.node_id}</span><span class="opill grey">self</span></span>
          <span class="field-note">{cluster.self.url}</span>
          <span><span class="opill {tone(cluster.self.state)}">{cluster.self.state}</span></span>
          <span class="field-note build-cell" title={cluster.self.build ?? ''}>{cluster.self.build ?? '—'}</span>
          <span class="field-note" title={formatAbsolute(cluster.self.last_seen)}>{formatRelativeTime(cluster.self.last_seen, now)}</span>
        </div>
        {#each cluster.members as m (m.node_id)}
          <div class="ot-row" class:flagged={m.state !== 'alive'} class:bad={m.state === 'dead'}>
            <span class="node-cell"><span class="ot-id">{m.node_id}</span></span>
            <span class="field-note">{m.url}</span>
            <span><span class="opill {tone(m.state)}">{m.state}</span></span>
            <span class="field-note build-cell" title={m.build ?? 'this peer does not report a build'}>
              {#if !m.build}—{:else if m.build === cluster.self.build}{m.build}{:else}<span class="amber">{m.build}</span>{/if}
            </span>
            <span class="field-note" title={formatAbsolute(m.last_seen)}>{formatRelativeTime(m.last_seen, now)}</span>
          </div>
        {/each}
      </div>
    {/if}
  {/if}
{/if}

<style>
  .view-bar { display: flex; justify-content: flex-end; margin: 1rem 0 .65rem; }
  .graph-panel {
    position: relative;
    border: 1px solid var(--ad-line);
    border-radius: 9px;
    background: radial-gradient(circle at 50% 45%, rgba(86, 212, 228, .06), transparent 45%), var(--ad-panel);
    overflow: hidden;
  }
  .graph-copy { display: flex; flex-direction: column; gap: .2rem; padding: .85rem 1rem 0; font-size: .78rem; color: var(--ad-dim); }
  .graph-copy strong { color: var(--ad-text); font-size: .82rem; }
  .legend { display: flex; flex-wrap: wrap; gap: .8rem; padding: .65rem 1rem 0; font-size: .68rem; color: var(--ad-dim); }
  .legend span::before { content: ''; display: inline-block; width: 1.3rem; border-top: 2px solid; margin-right: .3rem; vertical-align: middle; }
  .legend .connected { color: var(--ad-green); }
  .legend .failed { color: var(--ad-red); }
  .legend .stale { color: var(--ad-amber); }
  .legend .unknown { color: var(--ad-faint); }
  .cluster-graph { display: block; width: 100%; min-height: 300px; }
  .edge { fill: none; stroke-width: 2.2; opacity: .8; outline: none; }
  .edge:hover, .edge:focus { stroke-width: 4; opacity: 1; }
  .edge.connected { stroke: var(--ad-green); }
  .edge.failed { stroke: var(--ad-red); stroke-dasharray: 7 4; }
  .edge.stale { stroke: var(--ad-amber); stroke-dasharray: 3 5; }
  .edge.unknown { stroke: var(--ad-faint); stroke-dasharray: 2 7; opacity: .35; }
  .marker.connected { fill: var(--ad-green); }
  .marker.failed { fill: var(--ad-red); }
  .marker.stale { fill: var(--ad-amber); }
  .marker.unknown { fill: var(--ad-faint); }
  .graph-node { outline: none; }
  .graph-node:focus .node-fill, .graph-node:hover .node-fill { stroke: var(--ad-cyan); stroke-width: 2; }
  .node-ring { fill: none; stroke-width: 5; }
  .node-ring.green { stroke: var(--ad-green); }
  .node-ring.amber { stroke: var(--ad-amber); }
  .node-ring.red { stroke: var(--ad-red); }
  .node-fill { fill: var(--ad-panel-2); stroke: var(--ad-line); }
  .graph-node text { fill: var(--ad-text); font: 600 13px ui-monospace, SFMono-Regular, Menlo, monospace; pointer-events: none; }
  .graph-node .self-label { fill: var(--ad-dim); font-size: 10px; text-transform: uppercase; }
  .graph-empty, .graph-detail { position: absolute; left: 1rem; right: 1rem; bottom: .7rem; margin: 0; }
  .graph-detail { color: var(--ad-text); font-size: .75rem; }
  .otable {
    --ot-cols: minmax(8rem, 1.3fr) minmax(9rem, 1.5fr) 6.5rem minmax(9rem, 1.6fr) 8rem;
    margin-top: 1rem;
  }
  .skew-note {
    margin-top: 0.75rem;
  }
  .build-cell {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.78rem;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .cluster-id {
    font-size: 1.05rem;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .node-cell {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    min-width: 0;
  }
  @media (max-width: 820px) {
    .cluster-graph { min-height: 240px; }
    .graph-copy span { max-width: 28rem; }
    .otable {
      --ot-cols: minmax(0, 1fr);
    }
    .ot-head {
      display: none;
    }
  }
</style>
