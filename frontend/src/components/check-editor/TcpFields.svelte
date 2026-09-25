<script lang="ts">
  import type { Draft } from './draft';
  import { MATCH_MODES } from './draft';
  import NumberField from '../NumberField.svelte';
  import StringListEditor from './StringListEditor.svelte';

  let { draft }: { draft: Draft } = $props();
</script>

<label class="field wide">
  <span class="field-label">Target</span>
  <input class="mono" bind:value={draft.target} placeholder="db.internal:5432" />
  <span class="field-note">host:port — a TCP probe with no port has nothing to connect to.</span>
</label>

<label class="field">
  <span class="field-label">Timeout (s)</span>
  <NumberField min={1} bind:value={draft.timeout_secs} />
</label>

<div class="field wide">
  <span class="field-label">Additional hosts</span>
  <StringListEditor
    values={draft.hosts}
    addLabel="+ Add host"
    placeholder="db-replica.internal:5432"
    emptyNote="None. Only the target above is probed."
  />
</div>

<label class="field">
  <span class="field-label">Host match</span>
  <select bind:value={draft.host_match}>
    {#each MATCH_MODES as m (m)}
      <option value={m}>{m}</option>
    {/each}
  </select>
  <span class="field-note">
    all = every host must connect. any = one is enough, for a redundant pool.
  </span>
</label>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
</style>
