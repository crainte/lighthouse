<script lang="ts">
  let {
    values,
    addLabel,
    placeholder,
    emptyNote,
  }: {
    values: string[];
    addLabel: string;
    placeholder: string;
    emptyNote: string;
  } = $props();
</script>

{#if values.length === 0}
  <span class="field-note">{emptyNote}</span>
{:else}
  {#each values as _value, i (i)}
    <div class="pair-row">
      <input class="mono" bind:value={values[i]} {placeholder} />
      <button class="btn danger" onclick={() => values.splice(i, 1)}>Remove</button>
    </div>
  {/each}
{/if}
<div class="add-row">
  <button class="btn" onclick={() => values.push('')}>{addLabel}</button>
</div>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .pair-row {
    display: flex;
    gap: 0.4rem;
    align-items: center;
  }
  .pair-row + .pair-row {
    margin-top: 0.3rem;
  }
  .add-row {
    margin-top: 0.4rem;
  }
</style>
