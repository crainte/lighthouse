<script lang="ts">
  let {
    pairs,
    addLabel,
    namePlaceholder,
    valuePlaceholder,
    emptyNote,
  }: {
    pairs: { name: string; value: string }[];
    addLabel: string;
    namePlaceholder: string;
    valuePlaceholder: string;
    emptyNote: string;
  } = $props();
</script>

{#if pairs.length === 0}
  <span class="field-note">{emptyNote}</span>
{:else}
  <!-- Keyed by index: two entries may legitimately share a name, and a
       duplicate key aborts the render of the whole page. -->
  {#each pairs as _pair, i (i)}
    <div class="pair-row">
      <input class="mono pair-name" bind:value={pairs[i].name} placeholder={namePlaceholder} />
      <input class="mono" bind:value={pairs[i].value} placeholder={valuePlaceholder} />
      <button class="btn danger" onclick={() => pairs.splice(i, 1)}>Remove</button>
    </div>
  {/each}
{/if}
<div class="add-row">
  <button class="btn" onclick={() => pairs.push({ name: '', value: '' })}>{addLabel}</button>
</div>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  /* One repeated entry: value fields stretch, the remove button never does. */
  .pair-row {
    display: flex;
    gap: 0.4rem;
    align-items: center;
  }
  .pair-row + .pair-row {
    margin-top: 0.3rem;
  }
  .pair-name {
    flex: 0 1 14rem;
  }
  .add-row {
    margin-top: 0.4rem;
  }
</style>
