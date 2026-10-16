<script lang="ts">
  import type { Snippet } from 'svelte';
  import ConsoleIcon from './ConsoleIcon.svelte';
  import { sceneForSeason } from '../lib/seasonalScene';
  import type { Season } from '../lib/seasonalAtmosphere';
  import { fetchCluster } from '../lib/admin';

  type NavPanel = { id: string; label: string; group: string };

  let {
    panels,
    active,
    title,
    incidentCount,
    incidentsUnacknowledged,
    wsStatus,
    nodeMode,
    authenticated,
    overview,
    season = null,
    onSelect,
    onHome,
    onSignOut,
    children,
  }: {
    panels: readonly NavPanel[];
    active: string;
    title: string;
    incidentCount: number;
    incidentsUnacknowledged: number;
    wsStatus: string;
    nodeMode: string;
    authenticated: boolean;
    overview: boolean;
    season?: Season | null;
    onSelect: (id: string) => void;
    onHome: () => void;
    onSignOut: () => void;
    children: Snippet;
  } = $props();

  let menu = $state<HTMLDialogElement | null>(null);
  let nodeId = $state<string | null>(null);

  $effect(() => {
    if (!authenticated || nodeId) return;
    let active = true;
    fetchCluster()
      .then((cluster) => {
        if (active) nodeId = cluster.self.node_id;
      })
      .catch(() => {});
    return () => {
      active = false;
    };
  });
  let menuButton = $state<HTMLButtonElement | null>(null);

  const liveLabel = $derived(
    wsStatus === 'open' ? 'Live updates connected' : wsStatus === 'connecting' ? 'Connecting to live updates' : 'Live updates disconnected',
  );
  const liveTone = $derived(wsStatus === 'open' ? 'good' : wsStatus === 'connecting' ? 'warn' : 'bad');
  const modeLabel = $derived(nodeMode === 'global' ? 'Global' : 'Coordinator');
  const sidebarScene = $derived(sceneForSeason(season));

  function choose(id: string) {
    menu?.close();
    onSelect(id);
  }

  function openMenu() {
    menu?.showModal();
  }

  function closeMenu() {
    menu?.close();
  }

  function startsGroup(index: number) {
    const group = panels[index].group;
    return group !== '' && (index === 0 || panels[index - 1].group !== group);
  }
</script>

{#snippet navigation(mobile: boolean)}
  {#each panels as panel, index (panel.id)}
    {#if startsGroup(index)}<div class="nav-section">{panel.group}</div>{/if}
    <button
      class="nav-item"
      class:active={active === panel.id}
      aria-current={active === panel.id ? 'page' : undefined}
      onclick={() => (mobile ? choose(panel.id) : onSelect(panel.id))}
    >
      <ConsoleIcon name={panel.id} />
      {panel.label}
      {#if panel.id === 'incidents' && incidentCount > 0}
        <span class="count-badge" class:quiet={incidentsUnacknowledged === 0}>{incidentCount}</span>
      {/if}
    </button>
  {/each}
{/snippet}

<div class="console">
  <aside class="sidebar" aria-label="Console navigation">
    <!-- Ambient artwork only. The brand, links and status footer remain above
         the image layer with solid contrast, even in seasonal modes. -->
    <div class="sidebar-art" aria-hidden="true" style:background-image={`url("${sidebarScene}")`}></div>
    <div class="brand">
      <svg class="brand-mark" viewBox="0 0 32 40" fill="none" aria-hidden="true">
        <path d="m10 35 3-23h6l3 23M8 35h16M11 12h10M12 7h8l-4-5ZM13 7v5m6-5v5" stroke="currentColor" stroke-width="1.5" />
        <path d="m15 16-1 15h4l-1-15" fill="currentColor" opacity=".3" />
      </svg>
      <div><strong>Lightkeeper</strong><small>LIGHTHOUSE CONSOLE</small></div>
    </div>
    <nav class="nav-scroll" aria-label="Main">
      {#if authenticated}{@render navigation(false)}{/if}
    </nav>
    <div class="nav-footer">
      <div class="connection"><span class="dot {liveTone}"></span><span>{liveLabel}</span></div>
      <div class="connection"><span class="mono-text">{nodeId ?? nodeMode}</span><span class="quiet">· {modeLabel}</span></div>
      <button class="nav-item" onclick={onHome}><ConsoleIcon name="arrow" />Public status page</button>
      {#if authenticated}
        <div class="account">
          <span class="avatar">OP</span><span>Operator <span class="quiet tiny">/ admin</span></span>
          <button onclick={onSignOut} aria-label="Log out" title="Log out">↗</button>
        </div>
      {/if}
    </div>
  </aside>

  <div class="workspace">
    <header class="topbar">
      <div class="crumb">
        <button class="menu-toggle" bind:this={menuButton} onclick={openMenu} aria-label="Open navigation" aria-haspopup="dialog">
          <ConsoleIcon name="menu" />
        </button>
        <span class="muted context">Workspace</span><span class="slash">/</span><strong>{title}</strong>
      </div>
      <div class="top-actions">
        <span class="freshness tiny muted">{modeLabel} view · {wsStatus === 'open' ? 'Live' : liveLabel}</span>
      </div>
    </header>
    <main class="content" class:page={!overview} id="main" tabindex="-1">
      {@render children()}
    </main>
  </div>

  <dialog class="mobile-dialog" bind:this={menu} aria-labelledby="console-mobile-title" onclose={() => menuButton?.focus()}>
    <div class="dialog-head">
      <h2 id="console-mobile-title">Lightkeeper</h2>
      <button class="close" onclick={closeMenu} aria-label="Close navigation">×</button>
    </div>
    <nav aria-label="Mobile main">
      {#if authenticated}{@render navigation(true)}{/if}
      <button class="nav-item" onclick={() => { closeMenu(); onHome(); }}><ConsoleIcon name="arrow" />Public status page</button>
      {#if authenticated}<button class="nav-item" onclick={() => { closeMenu(); onSignOut(); }}>Log out</button>{/if}
    </nav>
  </dialog>
</div>

<style>
  .console {
    --bg: #080f19;
    --nav: #0a121e;
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
    min-height: 100vh;
    font: 14px/1.5 -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    color: var(--text);
    background: var(--bg);
    font-variant-numeric: tabular-nums;
  }
  button {
    font: inherit;
    color: inherit;
    border: 0;
    background: none;
    cursor: pointer;
    -webkit-tap-highlight-color: transparent;
    transition: background-color 0.15s, color 0.15s, border-color 0.15s;
  }
  button:hover {
    background-color: var(--hover);
  }
  button:focus-visible {
    outline: 2px solid var(--focus);
    outline-offset: 3px;
  }
  h2 {
    margin: 0;
    font-size: 14px;
    font-weight: 600;
  }
  .muted { color: var(--muted); }
  .quiet { color: var(--quiet); }
  .good { color: var(--green); }
  .warn { color: var(--amber); }
  .bad { color: var(--red); }
  .tiny { font-size: 12px; }
  .mono-text { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }
  .sidebar {
    position: fixed;
    inset: 0 auto 0 0;
    width: 216px;
    background: var(--nav);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    padding: 26px 16px 14px;
    z-index: 5;
  }
  .sidebar-art {
    position: absolute;
    left: 0;
    right: 0;
    bottom: 110px;
    height: min(38vh, 340px);
    background-size: auto 100%;
    /* The far-left edge of the source is nearly black. Show the water and
       illuminated shore just left of the lighthouse, without framing the
       structure itself. This crop is shared by all seasonal scenes. */
    background-position: 58% center;
    background-repeat: no-repeat;
    opacity: .58;
    filter: brightness(1.22) saturate(1.08);
    pointer-events: none;
    -webkit-mask-image: linear-gradient(to bottom, transparent 0%, #000 24%, #000 66%, transparent 100%);
    mask-image: linear-gradient(to bottom, transparent 0%, #000 24%, #000 66%, transparent 100%);
  }
  .brand, .nav-scroll, .nav-footer { position: relative; z-index: 1; }
  .brand {
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 0 4px 26px;
  }
  .brand-mark {
    display: block;
    width: 30px;
    height: 38px;
    color: var(--focus);
  }
  .brand strong {
    font-weight: 700;
    font-size: 17px;
    letter-spacing: -0.5px;
  }
  .brand small {
    display: block;
    color: var(--quiet);
    font-size: 10px;
    letter-spacing: 0.06em;
  }
  .nav-scroll {
    overflow: auto;
    flex: 1;
  }
  .nav-section {
    margin: 23px 12px 7px;
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--quiet);
  }
  .nav-item {
    display: flex;
    align-items: center;
    gap: 10px;
    min-height: 36px;
    width: 100%;
    padding: 7px 12px;
    text-align: left;
    border-radius: 5px;
    color: var(--muted);
    font-size: 12px;
    margin: 2px 0;
    position: relative;
  }
  .nav-item.active {
    color: #b5e7db;
    background: #142c32;
  }
  .nav-item.active:before {
    content: '';
    position: absolute;
    left: 0;
    height: 16px;
    width: 2px;
    background: var(--green);
  }
  .count-badge {
    margin-left: auto;
    font-size: 10px;
    min-width: 18px;
    text-align: center;
    border-radius: 4px;
    background: #36242d;
    color: var(--red);
    padding: 1px 5px;
  }
  .count-badge.quiet {
    background: #1c2d3b;
    color: var(--muted);
  }
  .nav-footer {
    border-top: 1px solid var(--border);
    padding-top: 12px;
    margin-top: 18px;
  }
  .connection {
    display: flex;
    align-items: center;
    gap: 7px;
    font-size: 11px;
    color: var(--muted);
    padding: 5px 12px;
  }
  .dot {
    width: 6px;
    height: 6px;
    display: inline-block;
    border-radius: 50%;
    background: currentColor;
    flex-shrink: 0;
  }
  .account {
    display: flex;
    align-items: center;
    gap: 9px;
    border-top: 1px solid var(--border);
    margin-top: 10px;
    padding: 14px 6px 0;
    font-size: 12px;
  }
  .avatar {
    width: 28px;
    height: 28px;
    display: grid;
    place-items: center;
    background: #1c2d3b;
    border-radius: 50%;
    color: #b2c4d4;
    font-size: 10px;
  }
  .account button {
    margin-left: auto;
    min-height: 36px;
    border-radius: 5px;
  }
  .workspace {
    margin-left: 216px;
  }
  .topbar {
    height: 64px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding: 0 28px;
  }
  .crumb {
    display: flex;
    align-items: center;
    gap: 12px;
    font-size: 12px;
  }
  .crumb .slash {
    color: #61748b;
  }
  .top-actions {
    display: flex;
    align-items: center;
    gap: 18px;
  }
  .menu-toggle {
    display: none;
  }
  .content {
    padding: 0 28px 24px;
    max-width: 1560px;
    margin: auto;
  }
  .content:focus {
    outline: none;
  }
  .content.page {
    padding-top: 28px;
    padding-bottom: 48px;
    font-size: 16px;
    line-height: 1.5;
  }
  .mobile-dialog {
    margin: 0;
    height: 100dvh;
    max-height: none;
    width: 280px;
    max-width: 90vw;
    background: var(--nav);
    color: var(--text);
    border: 0;
    border-right: 1px solid var(--border);
    padding: 16px;
    font: 14px/1.5 -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  }
  .mobile-dialog::backdrop {
    background: #020810b8;
  }
  .mobile-dialog nav {
    padding-top: 8px;
  }
  .mobile-dialog .nav-item {
    min-height: 44px;
  }
  .mobile-dialog .nav-section {
    margin-top: 14px;
  }
  .dialog-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0 0 10px;
    border-bottom: 1px solid var(--border);
    gap: 14px;
  }
  .close {
    width: 44px;
    height: 44px;
    border-radius: 5px;
    font-size: 22px;
  }
  @media (max-width: 1150px) {
    .top-actions .freshness {
      display: none;
    }
  }
  @media (max-width: 767px) {
    .sidebar {
      display: none;
    }
    .workspace {
      margin: 0;
    }
    .topbar {
      height: 60px;
      padding: 0 16px;
      gap: 8px;
    }
    .menu-toggle {
      display: inline-flex;
      width: 44px;
      height: 44px;
      align-items: center;
      justify-content: center;
      border-radius: 5px;
    }
    .crumb {
      gap: 8px;
    }
    .crumb .context,
  .crumb strong {
    font-weight: 700;
  }
  .crumb .slash {
      display: none;
    }
    .top-actions {
      gap: 8px;
    }
    .content {
      padding: 0 16px 20px;
    }
    .content.page {
      padding-top: 20px;
    }
  }
  @media (prefers-reduced-motion: reduce) {
    button {
      transition: none;
    }
  }
</style>
