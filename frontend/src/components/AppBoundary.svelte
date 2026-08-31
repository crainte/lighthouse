<script lang="ts">
  import type { Snippet } from 'svelte';

  let { children }: { children: Snippet } = $props();

  let failure = $state<string | null>(null);
</script>

<!--
  A render error inside Svelte does not surface as a broken widget — it aborts the
  render and leaves whatever was on screen frozen, typically the loading state.
  This project has shipped `each_key_duplicate` three times and every time it
  looked like "the page never loads" rather than an error.

  The boundary turns that into something a user can report and a developer can
  act on, and keeps the failure from being mistaken for a slow network.
-->
<svelte:boundary
  onerror={(e: unknown) => {
    failure = e instanceof Error ? e.message : String(e);
  }}
>
  {@render children()}

  {#snippet failed(_error: unknown, reset: () => void)}
    <div class="boundary">
      <h2>The dashboard hit a rendering error</h2>
      <p class="boundary-detail">{failure ?? 'unknown error'}</p>
      <p class="boundary-hint">
        Monitoring is unaffected — this is the page, not your checks. Reloading usually clears it.
      </p>
      <div class="boundary-actions">
        <button class="range-btn" onclick={reset}>Try again</button>
        <button class="range-btn" onclick={() => location.reload()}>Reload</button>
      </div>
    </div>
  {/snippet}
</svelte:boundary>

<style>
  .boundary {
    max-width: 640px;
    margin: 4rem auto;
    padding: 1.4rem;
    border: 1px solid var(--down, #f85149);
    border-radius: 12px;
    background: rgba(248, 81, 73, 0.06);
  }
  .boundary h2 {
    margin: 0 0 0.6rem;
    font-size: 1rem;
    color: var(--down, #f85149);
  }
  .boundary-detail {
    margin: 0 0 0.6rem;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.8rem;
    color: var(--text, #e6edf3);
    word-break: break-word;
  }
  .boundary-hint {
    margin: 0 0 1rem;
    font-size: 0.82rem;
    color: var(--text-dim, #7d8590);
  }
  .boundary-actions {
    display: flex;
    gap: 0.5rem;
  }
</style>
