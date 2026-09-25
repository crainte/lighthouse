<script lang="ts">
  import type { Draft } from './draft';
  import NumberField from '../NumberField.svelte';

  let { draft }: { draft: Draft } = $props();
</script>

<label class="field wide">
  <span class="field-label">Target host</span>
  <input class="mono" bind:value={draft.target} placeholder="example.com" />
  <span class="field-note">A hostname, not a URL. Port 443 is assumed.</span>
</label>

<label class="field">
  <span class="field-label">Timeout (s)</span>
  <NumberField min={1} bind:value={draft.timeout_secs} />
</label>

<label class="field">
  <span class="field-label">Warn before expiry (days)</span>
  <NumberField min={0} bind:value={draft.cert_warning_days} />
  <span class="field-note">DOWN once the certificate is this close to expiring.</span>
</label>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
</style>
