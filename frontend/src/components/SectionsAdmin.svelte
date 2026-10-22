<script lang="ts">
  import type { AdminCheck, Section } from '../lib/admin';
  import { AdminError, fetchAdminChecks, fetchSections } from '../lib/admin';
  import { fetchHomepageSpacers } from '../lib/api';
  import type { HomepageSpacer } from '../lib/homepageSpacers';
  import SectionsList from './sections-admin/SectionsList.svelte';
  import CheckOrderList from './sections-admin/CheckOrderList.svelte';
  import HomepageSpacersAdmin from './HomepageSpacersAdmin.svelte';

  let { onchanged, onexpired }: { onchanged: () => void; onexpired: () => void } = $props();

  let sections = $state<Section[]>([]);
  let checks = $state<AdminCheck[]>([]);
  let spacers = $state<HomepageSpacer[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let expired = $state(false);
  /** Set by whichever child's write hit the 401, so the expired panel shows
   *  the same message the action itself failed with. */
  let expiredMessage = $state<string | null>(null);

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) expired = true;
    return e instanceof Error ? e.message : String(e);
  }

  async function refresh() {
    loading = true;
    loadError = null;
    try {
      [sections, checks, spacers] = await Promise.all([fetchSections(), fetchAdminChecks(), fetchHomepageSpacers()]);
    } catch (e) {
      loadError = fail(e);
    } finally {
      loading = false;
    }
  }

  refresh();

  function markExpired(message: string) {
    expired = true;
    expiredMessage = message;
  }
</script>

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{expiredMessage ?? loadError ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to configure the status page.</p>
    <div class="range-switch retry">
      <button class="btn" onclick={onexpired}>Sign in again</button>
    </div>
  </div>
{:else if loading}
  <div class="admin-panel"><p class="admin-muted">Loading…</p></div>
{:else if loadError}
  <div class="admin-panel">
    <p class="admin-error">{loadError}</p>
    <div class="range-switch retry">
      <button class="btn" onclick={refresh}>Retry</button>
    </div>
  </div>
{:else}
  <SectionsList bind:sections {checks} {onchanged} onexpired={markExpired} refreshAll={refresh} />
  <CheckOrderList bind:checks {sections} {onchanged} onexpired={markExpired} />
  <HomepageSpacersAdmin {checks} {sections} bind:spacers {onchanged} onexpired={markExpired} />
{/if}

<style>
  .retry {
    margin-top: 0.7rem;
  }
</style>
