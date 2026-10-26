<script lang="ts">
  import type { Draft } from './draft';
  import NumberField from '../NumberField.svelte';

  let { draft }: { draft: Draft } = $props();
</script>

<label class="field wide">
  <span class="field-label">Target</span>
  <input class="mono" bind:value={draft.target} placeholder="time.example.com:123" />
  <span class="field-note">NTP server host:port. Port 123 is standard; include a custom port explicitly.</span>
</label>

<label class="field">
  <span class="field-label">Timeout (s)</span>
  <NumberField min={1} bind:value={draft.timeout_secs} />
</label>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
</style>
