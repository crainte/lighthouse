<script lang="ts">
  import { onDestroy } from 'svelte';
  import type { ClusterMember, ClusterResponse } from '../lib/admin';
  import { AdminError, fetchCluster } from '../lib/admin';
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
          (m) => m.state !== 'dead' && m.build && m.build !== cluster!.self.build,
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
    try {
      cluster = await fetchCluster();
      loadError = null;
    } catch (e) {
      loadError = fail(e);
    } finally {
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
    {@const now = cluster.now}
    <div class="otable">
      <div class="ot-head">
        <span>Node</span>
        <span>URL</span>
        <span>State</span>
        <span>Build</span>
        <span>Last seen</span>
      </div>
      <div class="ot-row">
        <span class="node-cell">
          <span class="ot-id">{cluster.self.node_id}</span>
          <span class="opill grey">self</span>
        </span>
        <span class="field-note">{cluster.self.url}</span>
        <span><span class="opill {tone(cluster.self.state)}">{cluster.self.state}</span></span>
        <span class="field-note build-cell" title={cluster.self.build ?? ''}>
          {cluster.self.build ?? '—'}
        </span>
        <span class="field-note" title={formatAbsolute(cluster.self.last_seen)}>
          {formatRelativeTime(cluster.self.last_seen, now)}
        </span>
      </div>
      {#each cluster.members as m (m.node_id)}
        <div class="ot-row" class:flagged={m.state !== 'alive'} class:bad={m.state === 'dead'}>
          <span class="node-cell"><span class="ot-id">{m.node_id}</span></span>
          <span class="field-note">{m.url}</span>
          <span><span class="opill {tone(m.state)}">{m.state}</span></span>
          <span
            class="field-note build-cell"
            title={m.build ?? 'this peer does not report a build'}
          >
            {#if !m.build}
              —
            {:else if m.build === cluster.self.build}
              {m.build}
            {:else}
              <span class="amber">{m.build}</span>
            {/if}
          </span>
          <span class="field-note" title={formatAbsolute(m.last_seen)}>
            {formatRelativeTime(m.last_seen, now)}
          </span>
        </div>
      {/each}
    </div>
  {/if}
{/if}

<style>
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
    .otable {
      --ot-cols: minmax(0, 1fr);
    }
    .ot-head {
      display: none;
    }
  }
</style>
