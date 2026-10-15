<script lang="ts">
  import FleetOverview from './FleetOverview.svelte';
  import AgentsAdmin from './AgentsAdmin.svelte';
  import AgentCredentials from './AgentCredentials.svelte';
  import RegionsAdmin from './RegionsAdmin.svelte';
  import type { FleetTab } from '../lib/router';

  let { tab = 'overview', onNavigate, onexpired, onopen, nodeMode }: {
    tab?: FleetTab;
    onNavigate: (tab: FleetTab) => void;
    onexpired: () => void;
    onopen: (id: string | null) => void;
    nodeMode: string;
  } = $props();

  const tabs: { id: FleetTab; title: string }[] = [
    { id: 'overview', title: 'Overview' },
    { id: 'agents', title: 'Agents' },
    { id: 'regions', title: 'Regions & zones' },
    { id: 'access', title: 'Access & enrollment' },
  ];
</script>

<section class="fleet-workspace" aria-label="Fleet workspace">
  <nav class="fleet-tabs" aria-label="Fleet views">
    {#each tabs as item (item.id)}
      <button class:active={tab === item.id} aria-current={tab === item.id ? 'page' : undefined} onclick={() => onNavigate(item.id)}>
        {item.title}
      </button>
    {/each}
  </nav>

  {#if tab === 'overview'}
    <FleetOverview mode="overview" {onNavigate} {onexpired} />
  {:else if tab === 'agents'}
    <div class="fleet-tab-intro"><h3>Agents</h3><p>Fleet inventory, connectivity, placement, and probe capability.</p></div>
    <AgentsAdmin {onexpired} {onopen} />
  {:else if tab === 'regions'}
    <FleetOverview mode="regions" {onNavigate} {onexpired} />
  {:else if tab === 'access'}
    <section class="enrollment-hero" aria-label="Enrollment steps">
      <span class="hero-symbol" aria-hidden="true">♧</span>
      <div class="hero-description">
        <h3>Onboard agents in two steps</h3>
        <p>{nodeMode === 'global' ? 'First register a region to create its coordinator key, then issue agent credentials for zones inside that region.' : 'Issue agent credentials for zones in this region. Regional-only and standalone deployments do not need global registration.'}</p>
      </div>
      <div class="hero-steps">
        {#if nodeMode === 'global'}
          <div><span class="step-disc">1</span><div><strong>Register region</strong><small>Generate a regional coordinator key</small></div></div>
          <span class="step-arrow">›</span>
        {/if}
        <div><span class="step-disc">2</span><div><strong>Issue agent credentials</strong><small>Generate zone-scoped agent tokens</small></div></div>
      </div>
    </section>

    {#if nodeMode === 'global'}
      <section class="access-panel" aria-label="Regional coordinator access">
        <header class="access-heading"><span class="section-number">1</span><div><h3>Region registration</h3><p>Register regions that can report to the global control plane. A key is issued only when registered or rotated.</p></div></header>
        <RegionsAdmin {onexpired} />
      </section>
    {/if}
    <section class="access-panel" aria-label="Zone agent credentials">
      <header class="access-heading"><span class="section-number">2</span><div><h3>Agent credentials</h3><p>Create zone-scoped keys after registering the region. Agents within one zone can share a credential.</p></div></header>
      <AgentCredentials {onexpired} {nodeMode} embedded />
    </section>
  {/if}
</section>

<style>
  .fleet-workspace { min-width:0; }
  .fleet-tabs {display:flex;align-items:center;gap:3px;overflow-x:auto;max-width:100%;border-bottom:1px solid #243b51;margin-bottom:20px;}
  .fleet-tabs button {flex:none;min-height:45px;padding:10px 16px;background:transparent;border:0;border-bottom:2px solid transparent;font:inherit;font-size:12px;color:#9bb0c4;cursor:pointer;}
  .fleet-tabs button:hover {background:#132839;color:#e5eef6;}
  .fleet-tabs button.active {color:#e6f4fa;border-bottom-color:#56d4e4;background:#102a3f;}
  .fleet-tabs button:focus-visible {outline:2px solid #56d4e4;outline-offset:-2px;}
  .fleet-tab-intro {margin:6px 0 18px;}
  .fleet-tab-intro h3 {font-size:16px;font-weight:600;margin:0 0 5px;}
  .fleet-tab-intro p {font-size:12px;color:#96acc0;margin:0;}
  .enrollment-hero {display:flex;align-items:center;gap:22px;flex-wrap:wrap;padding:20px;margin-bottom:16px;background:linear-gradient(100deg,#102438,#101d30);border:1px solid #2c4860;border-radius:9px;}
  .hero-symbol {display:grid;place-items:center;flex:none;width:45px;height:45px;border:1px solid #3c8197;color:#78ddeb;border-radius:10px;font-size:24px;}
  .hero-description {flex:1;min-width:230px;}
  .hero-description h3 {margin:0 0 5px;font-size:15px;font-weight:650;}
  .hero-description p {color:#a0b5c6;max-width:67ch;margin:0;font-size:12px;line-height:1.55;}
  .hero-steps {display:flex;align-items:center;flex-wrap:wrap;gap:13px;}
  .hero-steps>div {display:flex;align-items:center;gap:10px;max-width:175px;}
  .hero-steps strong {display:block;font-size:11px;}
  .hero-steps small {display:block;margin-top:3px;font-size:10px;color:#93aabe;line-height:1.4;}
  .step-disc,.section-number {display:grid;place-items:center;flex:none;width:33px;height:33px;border:1px solid #5bd1d7;border-radius:50%;color:#71dfe5;font-size:14px;}
  .step-arrow {color:#6ca9bb;font-size:24px;}
  .access-panel {min-width:0;padding:20px;margin-bottom:16px;background:#101b29;border:1px solid #284157;border-radius:9px;}
  .access-heading {display:flex;align-items:center;gap:13px;margin:0 0 18px;}
  .access-heading h3 {margin:0 0 4px;font-size:15px;font-weight:650;}
  .access-heading p {margin:0;font-size:11px;color:#a0b2c4;line-height:1.5;}
  @media (max-width:650px) {.enrollment-hero,.access-panel {padding:13px;}.hero-steps{width:100%;}.fleet-tabs button{padding:10px 12px;}}
</style>
