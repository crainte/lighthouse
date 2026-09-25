<script lang="ts">
  import type { Draft } from './draft';

  let {
    draft,
    otherChecks,
  }: { draft: Draft; otherChecks: { id: string; name: string }[] } = $props();
</script>

<div class="field wide">
  <span class="field-label">Depends on</span>
  {#if otherChecks.length === 0}
    <span class="field-note">No other checks to depend on yet.</span>
  {:else}
    <div class="agent-picker">
      {#each otherChecks as dep (dep.id)}
        <label class="toggle">
          <input type="checkbox" value={dep.id} bind:group={draft.depends_on} />
          <span class="agent-pill">{dep.name}</span>
        </label>
      {/each}
    </div>
  {/if}
  <span class="field-note">
    {#if draft.depends_on.length === 0}
      Nothing upstream. A failure here opens its own incident.
    {:else}
      When this check fails while one of these already has an open incident, it joins that
      incident instead of opening a second one for the same root cause. It still alerts and still
      notifies — grouping is not silence. One hop only: it does not follow an upstream's own
      dependencies.
    {/if}
  </span>
</div>

<style>
  .agent-picker {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 0.9rem;
  }
</style>
