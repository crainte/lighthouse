<script lang="ts">
  import type { RegionRegistration } from '../lib/admin';
  import { AdminError, fetchRegions, mintRegion, removeRegion } from '../lib/admin';
  import { formatRelativeTime } from '../lib/format';

  let { onexpired }: { onexpired?: () => void } = $props();

  let regions = $state<RegionRegistration[]>([]);
  let loadError = $state<string | null>(null);
  let formError = $state<string | null>(null);
  let name = $state('');
  let busy = $state(false);
  let pendingRemove = $state<string | null>(null);
  /** The key exists only in this response; dismissing it loses it for good. */
  let minted = $state<{ region: string; token: string } | null>(null);

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
    busy = true;
    formError = null;
    try {
      const res = await mintRegion(region.trim());
      minted = { region: res.region, token: res.token };
      name = '';
      await refresh();
    } catch (e) {
      formError = fail(e);
    } finally {
      busy = false;
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
  <p class="admin-muted">
    Put the key in <code class="mono">[coordinator.global] token</code>. Rotating a key disconnects the region until it is updated.
  </p>
  <form class="regions-create" onsubmit={(e) => (e.preventDefault(), mint(name))}>
    <label class="field"><span class="field-label">Region</span><input bind:value={name} placeholder="Region (e.g. eu-west)" /></label>
    <button class="btn primary" type="submit" disabled={busy || !name.trim()}>Register</button>
  </form>
  {#if formError}<p class="admin-error">{formError}</p>{/if}
  {#if minted}
    <div class="regions-minted" role="alert">
      <strong>Copy the key for {minted.region} now. It is shown only once.</strong>
      <pre class="mono">[coordinator.global]{'\n'}token = "{minted.token}"</pre>
      <button class="btn" onclick={() => (minted = null)}>Dismiss</button>
    </div>
  {/if}
  {#if loadError}
    <p class="admin-error">{loadError}</p>
  {:else if regions.length === 0}
    <p class="admin-muted">No regions registered.</p>
  {:else}
    {#each regions as r (r.name)}
      <div class="regions-row">
        <span class="node-dot" class:green={r.attached && !r.stale} class:amber={!r.attached || r.stale}></span>
        <strong>{r.name}</strong>
        <small>
          {!r.attached ? 'Waiting for first push' : r.stale ? 'Not reporting' : `${r.check_count} checks · ${formatRelativeTime(Date.now() - r.age_ms)}`}
          · <span class="mono">{r.credential_id}</span>
        </small>
        <span class="regions-actions">
          {#if pendingRemove === r.name}
            <span class="admin-muted">Remove {r.name}? Its key is revoked and checks placed only there are disabled.</span>
            <button class="btn danger" disabled={busy} onclick={() => remove(r.name)}>Remove</button>
            <button class="btn" onclick={() => (pendingRemove = null)}>Cancel</button>
          {:else}
            <button class="btn" disabled={busy} onclick={() => mint(r.name)}>Rotate key</button>
            <button class="btn danger" disabled={busy} onclick={() => (pendingRemove = r.name)}>Remove</button>
          {/if}
        </span>
      </div>
    {/each}
  {/if}
</section>

<style>
  .regions-panel { min-width: 0; }
  .regions-panel > p { margin: 0; font-size: 12px; }
  .regions-create { display: flex; gap: 16px; flex-wrap: wrap; align-items: flex-end; margin: 20px 0; }
  .regions-create .field { flex: 1 1 14rem; max-width: 24rem; }
  .regions-minted { border: 1px solid var(--ad-amber); border-radius: 7px; padding: 16px; margin-bottom: 16px; font-size: 13px; }
  .regions-minted pre { white-space: pre-wrap; word-break: break-all; user-select: all; }
  .regions-row { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; padding: 14px 0; border-top: 1px solid var(--ad-line); font-size: 13px; }
  .regions-row small { color: var(--ad-dim); }
  .regions-actions { margin-left: auto; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  .node-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--ad-faint); }
  .node-dot.green { background: var(--ad-green); }
  .node-dot.amber { background: var(--ad-amber); }
</style>
