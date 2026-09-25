<script lang="ts">
  import type { Draft } from './draft';
  import { MATCH_MODES, IP_TYPES } from './draft';
  import NumberField from '../NumberField.svelte';
  import StringListEditor from './StringListEditor.svelte';

  let { draft }: { draft: Draft } = $props();
</script>

<label class="field wide">
  <span class="field-label">Target host</span>
  <input class="mono" bind:value={draft.target} placeholder="1.1.1.1" />
  <span class="field-note">A hostname or IP, not a URL.</span>
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
    placeholder="8.8.8.8"
    emptyNote="None. Only the target above is pinged."
  />
</div>

<label class="field">
  <span class="field-label">Host match</span>
  <select bind:value={draft.host_match}>
    {#each MATCH_MODES as m (m)}
      <option value={m}>{m}</option>
    {/each}
  </select>
  <span class="field-note">all = every host must reply. any = one is enough.</span>
</label>

<label class="field">
  <span class="field-label">IP type</span>
  <select bind:value={draft.ping.ip_type}>
    {#each IP_TYPES as t (t)}
      <option value={t}>{t}</option>
    {/each}
  </select>
  <span class="field-note">Which address family the host resolves to.</span>
</label>

<label class="field">
  <span class="field-label">Ping count</span>
  <NumberField min={1} bind:value={draft.ping.count} />
  <span class="field-note">Echoes sent per attempt.</span>
</label>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
</style>
