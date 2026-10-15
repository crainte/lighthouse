<script lang="ts">
  import type { AgentCredential, RegionRegistration } from '../lib/admin';
  import { regionFlag } from '../lib/regionFlag';
  import CredentialId from './CredentialId.svelte';
  import {
    AdminError,
    fetchAgentCredentials,
    createAgentCredential,
    revokeAgentCredential,
    deleteAgentCredential,
    fetchRegions,
  } from '../lib/admin';
  import { formatRelativeTime } from '../lib/format';

  let { onexpired, embedded = false, nodeMode = 'global' }: { onexpired?: () => void; embedded?: boolean; nodeMode?: string } = $props();

  let credentials = $state<AgentCredential[]>([]);
  let knownRegions = $state<RegionRegistration[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let unavailableRegions = $state<string[]>([]);
  let formError = $state<string | null>(null);
  let expired = $state(false);

  let zone = $state('');
  let region = $state('');
  let creating = $state(false);
  let pendingRevoke = $state<string | null>(null);
  let pendingDelete = $state<string | null>(null);
  let busy = $state<string | null>(null);

  /** The one-time secret, held apart from `credentials` on purpose: `refresh()`
   *  replaces that list wholesale and would otherwise take the only copy of the
   *  token with it. Cleared only by the operator's Dismiss. */
  let minted = $state<{ credential: AgentCredential; token: string } | null>(null);
  let copied = $state(false);

  const snippet = $derived(minted ? `[agent]\ntoken = "${minted.token}"` : '');

  // Region groups the fleet the same way it does everywhere else; zone orders
  // within it, so a credential sits where the operator already looks for it.
  const ordered = $derived(
    [...credentials].sort((a, b) => a.region.localeCompare(b.region) || a.zone.localeCompare(b.zone)),
  );

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) {
      expired = true;
      onexpired?.();
    }
    return e instanceof Error ? e.message : String(e);
  }

  async function refresh() {
    loading = true;
    loadError = null;
    try {
      const r = await fetchAgentCredentials();
      credentials = r.credentials;
      unavailableRegions = r.unavailable_regions ?? [];
    } catch (e) {
      loadError = fail(e);
    } finally {
      loading = false;
    }
  }

  refresh();
  if (nodeMode === 'global') {
    void fetchRegions().then((items) => (knownRegions = items)).catch(() => { /* Region suggestions are optional; manual names still work. */ });
  }

  async function create() {
    if (creating) return;
    const z = zone.trim();
    const r = region.trim();
    if (!z || !r) {
      formError = 'Zone and region are both required.';
      return;
    }
    creating = true;
    formError = null;
    try {
      minted = await createAgentCredential(z, r);
      copied = false;
      zone = '';
      region = '';
      await refresh();
    } catch (e) {
      formError = fail(e);
    } finally {
      creating = false;
    }
  }

  async function copyToken() {
    if (!minted) return;
    try {
      await navigator.clipboard.writeText(minted.token);
      copied = true;
    } catch {
      // Clipboard blocked (insecure origin, denied permission). The token is on
      // screen and selectable, so this is a downgrade, not a failure.
      copied = false;
      formError = 'Could not copy — select the token above and copy it manually.';
    }
  }

  async function confirmRevoke(id: string) {
    pendingRevoke = null;
    busy = id;
    formError = null;
    try {
      await revokeAgentCredential(id);
      await refresh();
    } catch (e) {
      formError = fail(e);
    } finally {
      busy = null;
    }
  }

  async function confirmDelete(id: string) {
    pendingDelete = null;
    busy = id;
    formError = null;
    try {
      await deleteAgentCredential(id);
      await refresh();
    } catch (e) {
      formError = fail(e);
    } finally {
      busy = null;
    }
  }
</script>

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{loadError ?? formError ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to manage agent credentials.</p>
    <div class="range-switch retry">
      <button class="btn" onclick={onexpired}>Sign in again</button>
    </div>
  </div>
{:else}
  <div class="credential-container" class:admin-panel={!embedded}>
    {#if !embedded}<div class="panel-title">Zone credentials</div>{/if}
    <p class="admin-muted lead">
      Issue credentials to agents in a region and zone. Agents within a zone can share one
      <code class="mono">[agent] token</code>. Secrets are shown only once. Deploy replacements
      before revoking existing credentials, or agents using them will disconnect.
    </p>

    <form class="create" onsubmit={(e) => (e.preventDefault(), create())}>
      <label class="field"><span class="field-label">Region</span><input bind:value={region} placeholder="Region (e.g. eu-west)" required list="registered-fleet-regions" /><datalist id="registered-fleet-regions">{#each knownRegions as r (r.name)}<option value={r.name}></option>{/each}</datalist></label>
      <label class="field"><span class="field-label">Zone</span><input bind:value={zone} placeholder="Zone (e.g. eu-west-1a)" required /></label>
      <button class="btn primary" type="submit" disabled={creating}>
        {creating ? 'Issuing…' : 'Issue agent token'}
      </button>
    </form>

    {#if formError}<p class="admin-error">{formError}</p>{/if}

    {#if minted}
      <!-- Survives the refresh above by living outside `credentials`. -->
      <div class="minted" role="alert">
        <div class="minted-head">
          Copy this token now — it is shown once and cannot be recovered. If you lose it, revoke
          this credential and generate a new one.
        </div>
        <div class="minted-meta admin-muted">
          {minted.credential.zone} · {minted.credential.region} ·
          <span class="mono">{minted.credential.id}</span>
        </div>
        <pre class="mono token">{snippet}</pre>
        <div class="minted-actions">
          <button class="btn primary" onclick={copyToken}>
            {copied ? 'Copied' : 'Copy token'}
          </button>
          <button class="btn" onclick={() => ((minted = null), (copied = false))}>Dismiss</button>
        </div>
      </div>
    {/if}

    {#if unavailableRegions.length}<p class="admin-error">Credentials not read from {unavailableRegions.join(', ')}: those regions did not answer, so their credentials are missing below.</p>{/if}
    {#if loading}
      <p class="admin-muted">Loading…</p>
    {:else if loadError}
      <p class="admin-error">{loadError}</p>
      <div class="range-switch retry">
        <button class="btn" onclick={refresh}>Retry</button>
      </div>
    {:else if ordered.length === 0}
      <p class="admin-muted">
        No agent credentials, so no agent can report yet. There is no shared token and no open
        mode: until one exists the coordinator refuses every agent request, which is the intended
        first-run state. Generate one per zone, then start the agents with the token in
        <code class="mono">[agent] token</code> — the value is shown once, here, and cannot be
        recovered afterwards.
      </p>
    {:else}
      <div class="list-heading"><strong>Issued agent credentials</strong><span>{ordered.length} credential{ordered.length === 1 ? '' : 's'} · IDs only, not recoverable secrets</span></div>
      <div class="admin-rows">
        <div class="row head">
          <span>Region</span>
          <span>Zone</span>
          <span>Credential ID</span>
          <span>Status</span>
          <span>Created</span>
          <span>Actions</span>
        </div>
        {#each ordered as c (c.id)}
          <div class="row" class:revoked={c.revoked}>
            <span class="region"><span class="flag" aria-hidden="true">{regionFlag(c.region) ?? '◎'}</span>{c.region}</span>
            <span class="zone">{c.zone}</span>
            <span class="mono id"><CredentialId id={c.id} /></span>
            <span><span class="status" class:revoked={c.revoked}>{c.revoked ? 'Revoked' : 'Active'}</span></span>
            <span title={new Date(c.created_at).toLocaleString()}>{formatRelativeTime(c.created_at)}</span>
            <span class="actions" class:confirming={pendingDelete === c.id || pendingRevoke === c.id}>
              {#if c.revoked && pendingDelete === c.id}
                <span class="confirm">Delete “{c.zone}” from this list? It is already revoked.</span>
                <button class="btn danger" onclick={() => confirmDelete(c.id)}>Delete</button>
                <button class="btn" onclick={() => (pendingDelete = null)}>Cancel</button>
              {:else if c.revoked}
                <span class="tag">Revoked</span>
                <button class="btn" disabled={busy !== null} onclick={() => (pendingDelete = c.id)}
                  >Delete</button
                >
              {:else if pendingRevoke === c.id}
                <span class="confirm">
                  Revoke “{c.zone}”? Agents in that zone stop reporting immediately, and this cannot
                  be undone.
                </span>
                <button class="btn danger" onclick={() => confirmRevoke(c.id)}>Revoke</button>
                <button class="btn" onclick={() => (pendingRevoke = null)}>Cancel</button>
              {:else}
                <button class="btn danger" disabled={busy !== null} onclick={() => (pendingRevoke = c.id)}
                  >Revoke</button
                >
              {/if}
            </span>
          </div>
        {/each}
      </div>
    {/if}
  </div>
{/if}

<style>
  .lead {
    margin: -0.35rem 0 0.8rem;
    line-height: 1.5;
  }
  .retry {
    margin-top: 0.7rem;
  }

  .create {
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    align-items: flex-end;
    margin: 24px 0;
  }
  .create .field { flex: 1 1 14rem; max-width: 24rem; }

  /* Warning-grade because the cost of scrolling past it is unrecoverable: the
     secret is gone the moment this is dismissed. */
  .minted {
    border: 1px solid var(--amber);
    border-left-width: 3px;
    border-radius: 8px;
    background: rgba(210, 153, 34, 0.08);
    padding: 0.7rem 0.8rem;
    margin-bottom: 0.9rem;
  }
  .minted-head {
    font-size: 0.82rem;
    font-weight: 600;
    color: var(--amber);
    line-height: 1.45;
  }
  .minted-meta {
    margin-top: 0.25rem;
    font-size: 0.72rem;
  }
  .token {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.5rem 0.6rem;
    margin: 0.55rem 0;
    font-size: 0.76rem;
    line-height: 1.5;
    white-space: pre-wrap;
    word-break: break-all;
    user-select: all;
  }
  .minted-actions {
    display: flex;
    gap: 0.4rem;
  }

  .row {
    display: grid;
    /* Fixed action column: rows are separate grids, so an `auto` one sized
       per row and dragged every other column out of line with the header. */
    grid-template-columns: minmax(6rem, .7fr) minmax(7rem, .9fr) minmax(11rem, 1.35fr) 6rem 6rem minmax(10rem, 1fr);
    gap: 0.6rem;
    align-items: center;
    font-size: 0.8rem;
    padding: 12px 0;
    min-width: 870px;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  .row:last-child {
    border-bottom: none;
  }
  .row.head {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
    border-bottom: 1px solid var(--border);
  }
  .zone {
    font-weight: 600;
  }
  .id {
    color: var(--text-dim);
  }
  /* Kept legible for audit, but plainly no longer usable. */
  .row.revoked > span:not(.actions),
  .row.revoked .tag {
    opacity: 0.55;
  }
  /* A confirmation needs a sentence, so it takes its own line under the row
     instead of squeezing the columns. */
  .actions.confirming {
    grid-column: 1 / -1;
  }
  .row.revoked .zone,
  .row.revoked .id {
    text-decoration: line-through;
  }
  .tag {
    font-size: 0.62rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--down);
  }
  .actions {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 0.35rem;
    flex-wrap: wrap;
  }
  .confirm {
    font-size: 0.72rem;
    color: var(--down);
    max-width: 26rem;
    text-align: right;
    line-height: 1.4;
  }

  .credential-container {min-width:0;}
  .list-heading {display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;padding:10px 0;border-bottom:1px solid var(--border);font-size:13px;}
  .list-heading span {font-size:11px;color:var(--text-dim);}
  .admin-rows {max-width:100%;overflow-x:auto;}
  .region {display:flex;align-items:center;gap:8px;}
  .flag {font-size:18px;line-height:1;}
  .status {display:inline-block;padding:4px 9px;border-radius:14px;background:#10362e;border:1px solid #245b4a;color:#64d8af;font-size:11px;}
  .status.revoked {color:#f19a98;background:#30232c;border-color:#65404b;}
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
</style>
