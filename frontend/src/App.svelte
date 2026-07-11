<script lang="ts">
  import { onMount } from 'svelte';
  import { fetchStatus, fetchAlerts } from './lib/api';
  import type { Check, Alert } from './lib/api';
  import { connectWs } from './lib/ws';
  import type { WsStatus } from './lib/ws';
  import StatusGrid from './components/StatusGrid.svelte';
  import CheckDetail from './components/CheckDetail.svelte';
  import AlertsList from './components/AlertsList.svelte';

  type View = { name: 'grid' } | { name: 'alerts' } | { name: 'detail'; checkId: string };

  let checks = $state<Check[]>([]);
  let alerts = $state<Alert[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let wsStatus = $state<WsStatus>('connecting');
  let view = $state<View>({ name: 'grid' });

  async function load() {
    loading = true;
    error = null;
    try {
      const [s, a] = await Promise.all([fetchStatus(), fetchAlerts()]);
      checks = s.checks;
      alerts = a.alerts;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }

  onMount(() => {
    load();
    const conn = connectWs({
      onStatus: (s) => (wsStatus = s),
      onStateChange: (msg) => {
        checks = checks.map((c) =>
          c.check_id === msg.check_id
            ? { ...c, status: msg.to, last_check: msg.timestamp }
            : c,
        );
        alerts = [
          { check_id: msg.check_id, from: msg.from, to: msg.to, timestamp: msg.timestamp },
          ...alerts,
        ];
      },
    });
    return () => conn.close();
  });
</script>

<div class="app">
  <header class="bar">
    <h1>Warden</h1>
    <nav class="tabs">
      <button
        class:active={view.name === 'grid' || view.name === 'detail'}
        onclick={() => (view = { name: 'grid' })}>Status</button
      >
      <button class:active={view.name === 'alerts'} onclick={() => (view = { name: 'alerts' })}
        >Alerts</button
      >
    </nav>
    <span class="ws-dot {wsStatus}" title="live updates: {wsStatus}">
      <span class="dot"></span>{wsStatus}
    </span>
  </header>

  {#if loading}
    <div class="notice">Loading…</div>
  {:else if error}
    <div class="notice error">
      Failed to load data: {error}
      <div style="margin-top:0.5rem"><button class="link" onclick={load}>Retry</button></div>
    </div>
  {:else if view.name === 'detail'}
    <CheckDetail checkId={view.checkId} onback={() => (view = { name: 'grid' })} />
  {:else if view.name === 'alerts'}
    <AlertsList {alerts} />
  {:else}
    <StatusGrid {checks} onselect={(id) => (view = { name: 'detail', checkId: id })} />
  {/if}
</div>
