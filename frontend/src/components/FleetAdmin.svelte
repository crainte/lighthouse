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
    { id: 'credentials', title: 'Credentials' },
    { id: 'registration', title: 'Registration' },
  ];
</script>

<section class="fleet-workspace" aria-label="Fleet workspace">
  <div class="fleet-toolbar">
    <nav class="fleet-tabs" aria-label="Fleet views">
      {#each tabs as item (item.id)}
        <button class:active={tab === item.id} aria-current={tab === item.id ? 'page' : undefined} onclick={() => onNavigate(item.id)}>
          {item.title}
        </button>
      {/each}
    </nav>
    <div class="fleet-quick-actions">
      {#if nodeMode === 'global'}<button class="fleet-quick" onclick={() => onNavigate('registration')}>+ Register region</button>{/if}
      <button class="fleet-quick" onclick={() => onNavigate('credentials')}>+ Issue agent token</button>
    </div>
  </div>

  {#if tab === 'overview'}
    <FleetOverview mode="overview" {onNavigate} {onexpired} />
  {:else if tab === 'agents'}
    <div class="fleet-tab-intro"><h3>Agents</h3><p>Fleet inventory, connectivity, placement, and probe capability.</p></div>
    <AgentsAdmin {onexpired} {onopen} />
  {:else if tab === 'regions'}
    <FleetOverview mode="regions" {onNavigate} {onexpired} />
  {:else if tab === 'credentials'}
    <div class="fleet-tab-intro"><h3>Credentials</h3><p>Generate, rotate, and revoke the zone-scoped tokens your agents use to join the fleet.</p></div>
    <AgentCredentials {onexpired} />
  {:else if tab === 'registration'}
    <div class="fleet-tab-intro"><h3>Expand the deployment</h3><p>Onboard regions and agents without leaving the Fleet workspace.</p></div>
    <div class="setup-steps" aria-label="Deployment setup steps">
      <div><span>1</span>Register region <small>Global deployments only</small></div>
      <div><span>2</span>Configure coordinator <small>Use the issued region key</small></div>
      <div><span>3</span>Issue agent token <small>Choose region and zone</small></div>
      <div><span>4</span>Verify reporting <small>Inspect agents in Fleet</small></div>
    </div>
    {#if nodeMode === 'global'}
      <div class="registration-block"><h4>Region registration</h4><p>Create or rotate keys used by regional coordinators when they report to the global control plane.</p><RegionsAdmin {onexpired} /></div>
    {:else}
      <div class="registration-block"><h4>Regional deployment</h4><p>Global registration isn't needed in a regional-only or standalone deployment. Your agent credentials are scoped to the region and zone where those agents run.</p></div>
    {/if}
    <div class="registration-block">
      <h4>Enroll agents</h4>
      <p>Generate a zone-scoped credential, configure your agent with the issued token, then verify that it reports. Tokens are only shown once, so credential generation and revocation stay together in the Credentials view.</p>
      <div class="setup-actions">
        <button class="btn primary" onclick={() => onNavigate('credentials')}>Open credentials →</button>
        <button class="btn" onclick={() => onNavigate('agents')}>Verify agents →</button>
      </div>
    </div>
  {/if}
</section>

<style>
  .fleet-workspace { min-width:0; }
  .fleet-toolbar {display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:12px;margin-bottom:20px;border-bottom:1px solid #243b51;}
  .fleet-tabs {display:flex;align-items:center;gap:3px;overflow-x:auto;max-width:100%;}
  .fleet-tabs button {flex:none;min-height:45px;padding:10px 15px;background:transparent;border:0;border-bottom:2px solid transparent;font:inherit;font-size:12px;color:#9bb0c4;cursor:pointer;}
  .fleet-tabs button:hover {background:#132839;color:#e5eef6;}
  .fleet-tabs button.active {color:#e6f4fa;border-bottom-color:#56d4e4;background:#102a3f;}
  .fleet-tabs button:focus-visible,.fleet-quick:focus-visible {outline:2px solid #56d4e4;outline-offset:-2px;}
  .fleet-quick-actions {display:flex;gap:8px;flex-wrap:wrap;padding-bottom:8px;}
  .fleet-quick {font:inherit;font-size:12px;border:1px solid #2c607d;color:#8bd2e5;background:#0d2637;border-radius:7px;padding:8px 11px;cursor:pointer;}
  .fleet-quick:hover {background:#14394b;border-color:#56d4e4;}
  .fleet-tab-intro {margin:6px 0 18px;}
  .fleet-tab-intro h3 {font-size:16px;font-weight:600;margin:0 0 5px;}
  .fleet-tab-intro p {font-size:12px;color:#96acc0;margin:0;}
  .setup-steps {display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:18px;}
  .setup-steps>div {display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:13px;color:#d8e7f0;font-size:12px;background:#0f1d2e;border:1px solid #294054;border-radius:8px;}
  .setup-steps span {display:grid;place-items:center;width:23px;height:23px;border:1px solid #36748e;border-radius:50%;color:#72c8e0;}
  .setup-steps small {display:block;flex-basis:100%;color:#8fa4b7;margin-left:31px;}
  .registration-block {min-width:0;padding:18px;margin-bottom:16px;background:#101b29;border:1px solid #22364b;border-radius:9px;}
  .registration-block h4 {font-size:14px;margin:0 0 7px;}.registration-block>p {font-size:12px;color:#94aabe;line-height:1.5;max-width:70ch;}
  .setup-actions {display:flex;flex-wrap:wrap;gap:10px;margin-top:14px;}
  @media (max-width:950px) {.setup-steps {grid-template-columns:repeat(2,minmax(0,1fr));}}
  @media (max-width:650px) {.setup-steps {grid-template-columns:1fr;}.fleet-tabs {width:100%;}.fleet-quick-actions {padding-bottom:12px;}}
</style>
