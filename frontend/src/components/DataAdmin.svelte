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

<div class="data-sections">
  <p class="cleanup-warning">Every action here is permanent. Review the scope before confirming; deleted records cannot be recovered.</p>

  <details class="admin-panel editor-section danger" open>
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Raw probe results
      <span class="admin-muted sum-note mono-text">
        {resultsLoading ? 'counting…' : resultsCount === null ? '—' : `${resultsCount.toLocaleString()} stored`}
      </span>
    </summary>
    <div class="editor-body">
      <p class="field-note lede">
        Every probe attempt agents have reported. Purging clears probe history, uptime bars and
        latency graphs; each check keeps its published status and keeps probing.
      </p>
      {#if resultsLoading}
        <p class="admin-muted">Loading result count…</p>
      {:else if resultsCount !== null}
        <p class="stored"><strong>{resultsCount.toLocaleString()}</strong> {resultsCount === 1 ? 'raw result' : 'raw results'} stored.</p>
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
    </div>
  </details>

  <details class="admin-panel editor-section danger" open>
    <summary class="editor-summary">
      <span class="chev" aria-hidden="true">▸</span> Short-lived incidents
      <span class="admin-muted sum-note mono-text">≤ {maxSecs ?? '—'} s</span>
    </summary>
    <div class="editor-body">
      <p class="field-note lede">
        Blips that opened and resolved an incident within seconds. Removing them tidies the
        incident history; open incidents and longer ones are never touched.
      </p>
      <label class="field duration">
        <span class="field-label">Maximum incident duration (seconds)</span>
        <NumberField min={1} max={3600} step={1} bind:value={maxSecs} disabled={purging} />
        <span class="field-note">Only resolved incidents lasting this long or less are removed.</span>
      </label>

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
  </details>
</div>

<style>
  /* Separate surfaces with a gap, not one panel: each section destroys a
     different kind of record and should read as its own decision. */
  .data-sections {
    display: grid;
    gap: 16px;
  }
  .data-sections > .admin-panel {
    margin: 0;
  }
  .data-sections > .editor-section[open] > .editor-summary {
    border-bottom-color: rgba(248, 81, 73, 0.45);
  }
  .editor-body > .form-actions {
    margin-top: 16px;
  }
  .cleanup-warning {
    margin: 0;
    padding: 12px 16px;
    border: 1px solid #56323e;
    border-left: 2px solid var(--ad-red);
    border-radius: 6px;
    color: var(--ad-red);
    font-size: 13px;
    background: #221a25;
  }
  .lede {
    margin: 0 0 14px;
    max-width: 68ch;
    line-height: 1.5;
  }
  .stored {
    margin: 0;
    font-size: 0.85rem;
    color: var(--text-dim);
  }
  .stored strong {
    color: var(--text);
    font-size: 1.05rem;
    font-variant-numeric: tabular-nums;
  }
  .duration {
    max-width: 22rem;
  }
  .mono-text {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  .confirm {
    font-size: 0.75rem;
    color: var(--down);
  }
</style>
