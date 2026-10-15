<script lang="ts">
  import type { RegionRegistration } from '../lib/admin';
  import { AdminError, fetchRegions, mintRegion, removeRegion } from '../lib/admin';
  import { formatRelativeTime } from '../lib/format';
  import { regionFlag } from '../lib/regionFlag';
  import CredentialId from './CredentialId.svelte';

  let { onexpired }: { onexpired?: () => void } = $props();
  let regions = $state<RegionRegistration[]>([]);
  let loadError = $state<string | null>(null);
  let formError = $state<string | null>(null);
  let name = $state('');
  let busy = $state(false);
  let pendingRemove = $state<string | null>(null);
  let minted = $state<{ region: string; token: string; credentialId: string } | null>(null);
  let copied = $state(false);

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) onexpired?.();
    return e instanceof Error ? e.message : String(e);
  }

  async function refresh() {
    try {
      regions = await fetchRegions();
      loadError = null;
    } catch (e) {
      loadError = fail(e);
    }
  }

  async function mint(region: string) {
    if (!region.trim() || busy) {
      if (!region.trim()) formError = 'Enter a region name.';
      return;
    }
    busy = true;
    formError = null;
    try {
      const res = await mintRegion(region.trim());
      minted = { region: res.region, token: res.token, credentialId: res.credential_id };
      copied = false;
      name = '';
      await refresh();
    } catch (e) {
      formError = fail(e);
    } finally {
      busy = false;
    }
  }

  async function copyToken() {
    if (!minted) return;
    try {
      await navigator.clipboard.writeText(minted.token);
      copied = true;
    } catch {
      formError = 'Clipboard access unavailable. Select the token text and copy it manually.';
    }
  }

  async function remove(region: string) {
    busy = true;
    formError = null;
    try {
      await removeRegion(region);
      pendingRemove = null;
      await refresh();
    } catch (e) {
      formError = fail(e);
    } finally {
      busy = false;
    }
  }

  refresh();
</script>

<section class="regions-panel" aria-label="Region registration">
  <form class="regions-create" onsubmit={(e) => { e.preventDefault(); void mint(name); }}>
    <label class="field">
      <span class="field-label">Region</span>
      <input bind:value={name} placeholder="e.g. eu-west" required />
    </label>
    <button class="btn primary" type="submit" disabled={busy}>{busy ? 'Registering…' : 'Register region'}</button>
  </form>
  <p class="note">A region key authorizes a coordinator to report to the global tier. Set it as <code>[coordinator.global] token</code>. Rotating the key disconnects the region until the coordinator has been updated.</p>
  {#if formError}<p class="admin-error">{formError}</p>{/if}

  {#if minted}
    <div class="minted" role="alert">
      <strong>Copy the new key for {minted.region} now. This is the only time the secret is shown.</strong>
      <p>Credential ID: <code>{minted.credentialId}</code></p>
      <pre>[coordinator.global]{'\n'}token = "{minted.token}"</pre>
      <div class="minted-actions">
        <button class="btn primary" onclick={copyToken}>{copied ? 'Copied' : 'Copy secret token'}</button>
        <button class="btn" onclick={() => (minted = null)}>Dismiss</button>
      </div>
    </div>
  {/if}

  <div class="table-title">
    <div><h5>Registered regions</h5><p>Identifiers are shown below; existing secret keys cannot be recovered.</p></div>
    <span>{regions.length} {regions.length === 1 ? 'region' : 'regions'}</span>
  </div>
  {#if loadError}
    <p class="admin-error">{loadError}</p><button class="btn" onclick={refresh}>Retry</button>
  {:else if regions.length === 0}
    <p class="empty">No regions registered yet.</p>
  {:else}
    <div class="scroll-table">
      <div class="table" role="table" aria-label="Registered region credentials">
        <div class="table-row heading" role="row">
          <span role="columnheader">Region</span><span role="columnheader">Status</span>
          <span role="columnheader">Credential ID</span><span role="columnheader">Created</span>
          <span role="columnheader">Last report</span><span role="columnheader">Actions</span>
        </div>
        {#each regions as r (r.name)}
          <div class="table-row" role="row">
            <span class="region-name" role="cell"><span class="flag" aria-hidden="true">{regionFlag(r.name) ?? '◎'}</span><strong>{r.name}</strong></span>
            <span role="cell"><span class="state" class:stale={!r.attached || r.stale}><i></i>{!r.attached ? 'Awaiting first report' : r.stale ? 'Stale' : 'Connected'}</span></span>
            <span role="cell"><CredentialId id={r.credential_id} /></span>
            <span role="cell" title={new Date(r.created_at).toLocaleString()}>{formatRelativeTime(r.created_at)}</span>
            <span role="cell">{r.attached ? formatRelativeTime(Date.now() - r.age_ms) : 'Never'}</span>
            <span class="actions" role="cell">
              {#if pendingRemove === r.name}
                <span class="confirm">Remove {r.name}? Placed checks may be disabled.</span>
                <button class="btn danger" disabled={busy} onclick={() => remove(r.name)}>Confirm</button>
                <button class="btn" onclick={() => (pendingRemove = null)}>Cancel</button>
              {:else}
                <button class="btn" disabled={busy} onclick={() => mint(r.name)}>Rotate key</button>
                <button class="btn danger" disabled={busy} onclick={() => (pendingRemove = r.name)}>Remove</button>
              {/if}
            </span>
          </div>
        {/each}
      </div>
    </div>
  {/if}
</section>

<style>
  .regions-panel { min-width:0; }
  .regions-create { display:flex;align-items:flex-end;gap:14px;margin:4px 0 12px; }
  .regions-create .field {flex:1;max-width:550px;margin:0;}
  .regions-create .btn.primary {flex:none;}
  .note {font-size:12px;max-width:84ch;line-height:1.55;color:var(--ad-dim);margin:0 0 16px;}
  .note code {font-family:ui-monospace,SFMono-Regular,Menlo,monospace;}
  .minted {border:1px solid var(--ad-amber);border-left-width:3px;background:rgba(210,153,34,.08);padding:15px;border-radius:8px;margin:12px 0 20px;font-size:12px;}
  .minted strong {color:var(--ad-amber);}
  .minted p {color:var(--ad-dim);}
  .minted pre {background:#081522;border:1px solid var(--ad-line);padding:10px;white-space:pre-wrap;word-break:break-all;user-select:all;}
  .minted-actions {display:flex;gap:9px;flex-wrap:wrap;}
  .table-title {display:flex;align-items:center;justify-content:space-between;gap:10px;margin:20px 0 10px;}
  .table-title h5 {font-size:13px;margin:0 0 3px;}
  .table-title p,.table-title>span {font-size:11px;color:var(--ad-dim);margin:0;}
  .scroll-table {max-width:100%;overflow-x:auto;}
  .table {min-width:840px;}
  .table-row {display:grid;grid-template-columns:minmax(100px,1fr) minmax(125px,1fr) minmax(185px,1.5fr) 95px 95px minmax(160px,1.1fr);gap:12px;align-items:center;padding:12px 10px;border-bottom:1px solid #26384b;font-size:12px;}
  .table-row.heading {color:var(--ad-dim);text-transform:uppercase;letter-spacing:.035em;font-size:10px;}
  .region-name {display:flex;align-items:center;gap:8px;}.flag {font-size:19px;line-height:1;}.state {display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:15px;background:#10362e;border:1px solid #23604d;color:#62dcae;white-space:nowrap;font-size:11px;}
  .state i {width:6px;height:6px;border-radius:50%;background:currentColor;}.state.stale {color:#e5b579;background:#30271a;border-color:#785739;}
  .actions {display:flex;gap:7px;align-items:center;justify-content:flex-end;flex-wrap:wrap;}
  .confirm {font-size:11px;color:var(--ad-amber);}
  .empty {padding:20px;color:var(--ad-dim);}
  @media (max-width:650px) {.regions-create {flex-direction:column;align-items:stretch;}.regions-create .field {max-width:none;}}
</style>
