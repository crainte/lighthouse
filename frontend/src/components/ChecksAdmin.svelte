<script lang="ts">
  import type { AdminCheck } from '../lib/admin';
  import { AdminError, fetchAdminChecks } from '../lib/admin';

  // `null` opens the editor on a new check. Everything that changes a check
  // now lives on the editor page, so this component only reads.
  let {
    onopen,
    onexpired,
  }: { onopen: (id: string | null) => void; onexpired: () => void } = $props();

  let checks = $state<AdminCheck[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  // A 401 mid-session means the cookie died; nothing here will work until the
  // user signs in again, so stop pretending the table is live.
  let expired = $state(false);

  async function refresh() {
    loading = true;
    loadError = null;
    try {
      checks = await fetchAdminChecks();
    } catch (e) {
      if (e instanceof AdminError && e.unauthorized) expired = true;
      loadError = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }

  refresh();
</script>

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{loadError ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to manage checks.</p>
    <div class="range-switch expired-action">
      <button class="range-btn" onclick={onexpired}>Sign in again</button>
    </div>
  </div>
{:else}
  <div class="area-bar">
    <div class="title-line">
      <span class="panel-title no-gap">Checks</span>
      {#if !loading}<span class="kind-pill">{checks.length}</span>{/if}
    </div>
    <div class="range-switch">
      <button class="btn primary" onclick={() => onopen(null)}>＋ New check</button>
    </div>
  </div>

  <div class="admin-panel">
    {#if loading}
      <p class="admin-muted">Loading…</p>
    {:else if loadError}
      <p class="admin-error">{loadError}</p>
      <div class="range-switch expired-action">
        <button class="range-btn" onclick={refresh}>Retry</button>
      </div>
    {:else if checks.length === 0}
      <div class="checks-empty"><h3>Start monitoring your infrastructure</h3><p class="admin-muted">Create a check to track availability, response time, or certificate health.</p><button class="btn primary" onclick={() => onopen(null)}>Create your first check</button></div>
    {:else}
      <div class="admin-rows">
        <div class="row head">
          <span>Name</span>
          <span>Tag</span>
          <span>Type</span>
          <span>Status</span>
          <span>Hidden</span>
          <span>Cron</span>
          <span></span>
        </div>
        {#each checks as c (c.id)}
          <div class="row" class:off={!c.enabled}>
            <span class="mono name" title={c.name}>{c.name}</span>
            <span>{#if c.tag}<span class="tag-pill">{c.tag}</span>{:else}<span class="dim">—</span>{/if}</span>
            <span><span class="kind-pill">{c.type}</span></span>
            <span class="state" class:on={c.enabled}>{c.enabled ? 'Enabled' : 'Disabled'}</span>
            <!-- The column asks "hidden?", so a published check reads No. The
                 stored field is still `visible`; only the question is flipped. -->
            <span class="dim">{c.visible ? 'No' : 'Yes'}</span>
            <span class="mono dim">{c.cron}</span>
            <span class="actions">
              <button class="btn" onclick={() => onopen(c.id)}>Configure</button>
            </span>
          </div>
        {/each}
      </div>
    {/if}
  </div>
{/if}

<style>
  .checks-empty { padding: 32px 16px; text-align: center; }
  .checks-empty h3 { font-size: 16px; font-weight: 550; margin: 0 0 8px; }
  .checks-empty .btn { margin-top: 12px; }
  .area-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    margin-bottom: 0.6rem;
  }
  .no-gap {
    margin-bottom: 0;
  }
  .expired-action {
    margin-top: 0.7rem;
  }

  /* Header and body rows share this template, so every track must resolve to
     the same width in both. The action column is sized explicitly rather than
     `auto`: the header's cell is empty, so `auto` collapsed it to 0 there and
     handed the slack to the flexible tracks, shifting every heading right of
     the values beneath it. */
  .row {
    display: grid;
    grid-template-columns:
      minmax(8rem, 1.4fr) minmax(0, 0.8fr) 5rem 5.5rem 4.5rem
      minmax(8rem, 1fr) 6rem;
    gap: 0.6rem;
    align-items: center;
    font-size: 0.8rem;
    padding: 12px 4px;
    min-width: 760px;
    border-bottom: 1px solid var(--ad-line-soft);
  }
  .row:not(.head):hover { background: var(--ad-panel-2); }
  .row:last-child {
    border-bottom: none;
  }
  .row.head {
    font-size: 0.68rem;
    text-transform: none;
    letter-spacing: 0;
    color: var(--text-dim);
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    background: var(--bg-elev);
  }
  /* A disabled check keeps its history but stops probing; dimming says so
     without hiding it. */
  .row.off .name {
    opacity: 0.55;
  }
  .name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  /* Probing or not is the one thing in this table you act on, so it is stated
     in words and lit green rather than left as another grey cell. */
  .tag-pill {
    display: inline-block;
    max-width: 100%;
    padding: 0.08rem 0.45rem;
    overflow: hidden;
    color: var(--text-dim);
    text-overflow: ellipsis;
    white-space: nowrap;
    border: 1px solid var(--border);
    border-radius: 999px;
  }
  .state {
    color: var(--text-dim);
  }
  .state.on {
    color: var(--up);
  }
  .actions {
    display: flex;
    justify-content: flex-end;
  }

  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .dim {
    color: var(--text-dim);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
