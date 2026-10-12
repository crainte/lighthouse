<script lang="ts">
  import {
    AdminError,
    fetchResultsCount,
    purgeResults,
    purgeShortIncidents,
  } from '../lib/admin';
  import NumberField from './NumberField.svelte';

  let { onexpired }: { onexpired: () => void } = $props();

  // Null when the field is cleared; the guard in arm() rejects it like any
  // other out-of-range value rather than silently purging with a default.
  let maxSecs = $state<number | null>(60);
  let confirming = $state(false);
  let purging = $state(false);
  let resultsCount = $state<number | null>(null);
  let resultsLoading = $state(true);
  let resultsConfirming = $state(false);
  let resultsPurging = $state(false);
  let error = $state<string | null>(null);
  let note = $state<string | null>(null);
  let resultsError = $state<string | null>(null);
  let resultsNote = $state<string | null>(null);

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) onexpired();
    return e instanceof Error ? e.message : String(e);
  }

  async function loadResultsCount() {
    resultsLoading = true;
    resultsError = null;
    try {
      resultsCount = await fetchResultsCount();
    } catch (e) {
      resultsError = fail(e);
    } finally {
      resultsLoading = false;
    }
  }

  void loadResultsCount();

  function arm() {
    const value = Number(maxSecs);
    error = null;
    note = null;
    if (!Number.isInteger(value) || value < 1 || value > 3600) {
      error = 'Maximum duration must be a whole number between 1 and 3600 seconds.';
      return;
    }
    confirming = true;
  }

  async function purge() {
    if (purging) return;
    purging = true;
    error = null;
    try {
      const deleted = await purgeShortIncidents(Number(maxSecs));
      confirming = false;
      note = `Purged ${deleted} ${deleted === 1 ? 'short incident' : 'short incidents'}.`;
    } catch (e) {
      error = fail(e);
    } finally {
      purging = false;
    }
  }

  function armResultsPurge() {
    if (resultsCount === null || resultsLoading) return;
    resultsError = null;
    resultsNote = null;
    resultsConfirming = true;
  }

  async function confirmResultsPurge() {
    if (resultsPurging || resultsCount === null) return;
    resultsPurging = true;
    resultsError = null;
    try {
      const deleted = await purgeResults();
      resultsConfirming = false;
      resultsNote = `Purged ${deleted} ${deleted === 1 ? 'raw result' : 'raw results'}.`;
      await loadResultsCount();
    } catch (e) {
      resultsError = fail(e);
    } finally {
      resultsPurging = false;
    }
  }
</script>

<div class="admin-panel">
  <p class="cleanup-warning">Permanent actions. Review the scope before confirming; deleted records cannot be recovered.</p>

  <section class="results-cleanup">
    <div class="field-label">Raw probe results</div>
    {#if resultsLoading}
      <p class="admin-muted">Loading result count…</p>
    {:else if resultsCount !== null}
      <p class="field-note">{resultsCount} {resultsCount === 1 ? 'raw result is' : 'raw results are'} stored. Purging clears probe history but keeps the published status.</p>
    {/if}
    {#if resultsError}<p class="admin-error">{resultsError}</p>{/if}
    {#if resultsNote}<p class="admin-muted">{resultsNote}</p>{/if}
    <div class="form-actions">
      {#if resultsConfirming && resultsCount !== null}
        <span class="confirm">Purge exactly {resultsCount} {resultsCount === 1 ? 'raw result' : 'raw results'}?</span>
        <button class="btn danger" onclick={confirmResultsPurge} disabled={resultsPurging}>
          {resultsPurging ? 'Purging…' : `Yes, purge ${resultsCount} ${resultsCount === 1 ? 'result' : 'results'}`}
        </button>
        <button class="btn" onclick={() => (resultsConfirming = false)} disabled={resultsPurging}>Cancel</button>
      {:else}
        <button class="btn danger" onclick={armResultsPurge} disabled={resultsLoading || resultsCount === null}>Purge raw results</button>
        <button class="btn" onclick={loadResultsCount} disabled={resultsLoading || resultsPurging}>Refresh count</button>
      {/if}
    </div>
  </section>

  <div class="cleanup-form">
    <div class="panel-title">Short-lived incidents</div>
    <label class="field">
      <span class="field-label">Maximum incident duration (seconds)</span>
      <NumberField min={1} max={3600} step={1} bind:value={maxSecs} disabled={purging} />
    </label>
    <span class="field-note">Only resolved incidents lasting this long or less are removed.</span>
  </div>

  {#if error}<p class="admin-error">{error}</p>{/if}
  {#if note}<p class="admin-muted">{note}</p>{/if}

  <div class="form-actions">
    {#if confirming}
      <span class="confirm">Purge resolved incidents lasting {maxSecs} seconds or less?</span>
      <button class="btn danger" onclick={purge} disabled={purging}>
        {purging ? 'Purging…' : 'Yes, purge short incidents'}
      </button>
      <button class="btn" onclick={() => (confirming = false)} disabled={purging}>Cancel</button>
    {:else}
      <button class="btn danger" onclick={arm}>Purge short incidents</button>
    {/if}
  </div>
</div>

<style>
  .cleanup-warning { padding: 12px 16px; border: 1px solid #56323e; border-left: 2px solid var(--ad-red); border-radius: 6px; color: var(--ad-red); font-size: 13px; background: #221a25; }
  .results-cleanup {
    display: grid;
    gap: 0.3rem;
    margin-top: 0.85rem;
  }

  .cleanup-form {
    display: grid;
    gap: 0.3rem;
    margin-top: 28px;
    padding-top: 24px;
    border-top: 1px solid var(--border);
    max-width: 32rem;
  }

  .field {
    display: grid;
    gap: 0.3rem;
  }

  .field-label {
    font-size: 0.75rem;
    color: var(--text-dim);
  }

  .field-note,
  .confirm {
    font-size: 0.75rem;
    color: var(--text-dim);
  }

  .confirm {
    color: var(--down);
  }

  .form-actions {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
    margin-top: 0.85rem;
  }
</style>
