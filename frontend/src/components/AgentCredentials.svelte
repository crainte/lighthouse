<script lang="ts">
  import type { AgentCredential } from '../lib/admin';
  import {
    AdminError,
    fetchAgentCredentials,
    createAgentCredential,
    revokeAgentCredential,
  } from '../lib/admin';
  import { formatRelativeTime } from '../lib/format';

  let { onexpired }: { onexpired?: () => void } = $props();

  let credentials = $state<AgentCredential[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let formError = $state<string | null>(null);
  let expired = $state(false);

  let zone = $state('');
  let region = $state('');
  let creating = $state(false);
  let pendingRevoke = $state<string | null>(null);
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
      credentials = await fetchAgentCredentials();
    } catch (e) {
      loadError = fail(e);
    } finally {
      loading = false;
    }
  }

  refresh();

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
</script>

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{loadError ?? formError ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to manage agent credentials.</p>
    <div class="range-switch retry">
      <button class="range-btn" onclick={onexpired}>Sign in again</button>
    </div>
  </div>
{:else}
  <div class="admin-panel">
    <div class="panel-title">Agent credentials</div>
    <p class="admin-muted lead">
      One credential per zone, shared by every agent in that zone: each of them carries the same
      value in <code class="mono">[agent] token</code>. The credential decides the zone and region
      its agents report from, so an agent cannot place itself somewhere it was not issued for.
      Revoking is immediate and permanent, and takes down every agent in the zone at once — mint
      the replacement and roll it out first.
    </p>

    <form class="create" onsubmit={(e) => (e.preventDefault(), create())}>
      <input bind:value={zone} placeholder="Zone (e.g. eu-west-1a)" />
      <input bind:value={region} placeholder="Region (e.g. eu-west)" />
      <button class="btn primary" type="submit" disabled={creating}>
        {creating ? 'Generating…' : 'Generate'}
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

    {#if loading}
      <p class="admin-muted">Loading…</p>
    {:else if loadError}
      <p class="admin-error">{loadError}</p>
      <div class="range-switch retry">
        <button class="range-btn" onclick={refresh}>Retry</button>
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
      <div class="admin-rows">
        <div class="row head">
          <span>Zone</span>
          <span>Region</span>
          <span>ID</span>
          <span>Created</span>
          <span></span>
        </div>
        {#each ordered as c (c.id)}
          <div class="row" class:revoked={c.revoked}>
            <span class="zone">{c.zone}</span>
            <span>{c.region}</span>
            <span class="mono id">{c.id}</span>
            <span title={new Date(c.created_at).toLocaleString()}
              >{formatRelativeTime(c.created_at)}</span
            >
            <span class="actions">
              {#if c.revoked}
                <span class="tag">Revoked</span>
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
    gap: 0.5rem;
    margin-bottom: 0.8rem;
  }
  .create input {
    max-width: 14rem;
  }

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
    grid-template-columns: minmax(7rem, 1fr) minmax(6rem, 0.8fr) minmax(7rem, 0.9fr) 6rem auto;
    gap: 0.6rem;
    align-items: center;
    font-size: 0.8rem;
    padding: 0.32rem 0;
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
  .row.revoked {
    opacity: 0.55;
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

  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
</style>
