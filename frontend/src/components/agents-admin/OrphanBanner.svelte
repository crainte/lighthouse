<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    title,
    names,
    idByName,
    onopen,
    overflowNote,
    children,
  }: {
    title: string;
    names: string[];
    idByName: Map<string, string>;
    onopen: (id: string) => void;
    overflowNote: string | null;
    children: Snippet;
  } = $props();
</script>

<div class="admin-panel orphans">
  <div class="orphan-title">{title}</div>
  <p class="orphan-note">{@render children()}</p>
  <div class="orphan-list">
    <!-- Check names are map keys server-side, so they cannot repeat. -->
    {#each names as n (n)}
      <button class="orphan-link" onclick={() => onopen(idByName.get(n) ?? n)}>{n}</button>
    {/each}
  </div>
  {#if overflowNote}
    <p class="orphan-note">{overflowNote}</p>
  {/if}
</div>

<style>
  /* The whole point of the panel: a check nothing is running produces no error,
     no alert and no missing-data marker, so the warning has to carry the weight
     the absent signal does not. */
  .orphans {
    border-color: var(--down);
    background: rgba(248, 81, 73, 0.07);
  }
  .orphan-title {
    color: var(--down);
    font-weight: 600;
    font-size: 0.95rem;
    margin-bottom: 0.35rem;
  }
  .orphan-note {
    color: var(--text);
    font-size: 0.82rem;
    margin: 0 0 0.6rem;
    line-height: 1.5;
    max-width: 46rem;
  }
  .orphan-list {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }
  .orphan-link {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.78rem;
    padding: 0.15rem 0.5rem;
    border: 1px solid var(--down);
    border-radius: 999px;
    background: none;
    color: var(--down);
    cursor: pointer;
  }
  .orphan-link:hover {
    background: rgba(248, 81, 73, 0.16);
  }
</style>
