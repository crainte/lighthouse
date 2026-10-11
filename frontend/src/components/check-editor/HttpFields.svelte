<script lang="ts">
  import type { Draft } from './draft';
  import NumberField from '../NumberField.svelte';
  import HeadersEditor from './HeadersEditor.svelte';

  let { draft }: { draft: Draft } = $props();
</script>

<label class="field wide">
  <span class="field-label">Target URL</span>
  <!-- The placeholder is an expression, not a literal attribute: Svelte reads
       `{NAME}` in attribute text as an interpolation, so writing the credential
       reference inline made the editor crash on an undefined variable. -->
  <input
    class="mono"
    bind:value={draft.target}
    placeholder={'https://example.com/health?token=${API_TOKEN}'}
  />
  <span class="field-note">Use ${'{NAME}'} for credentials; it resolves only in the probing region.</span>
</label>

<label class="field">
  <span class="field-label">Timeout (s)</span>
  <NumberField min={1} bind:value={draft.timeout_secs} />
</label>

<label class="field">
  <span class="field-label">Expect status</span>
  <NumberField min={100} max={599} bind:value={draft.expect_status} placeholder="(any 2xx–3xx)" />
</label>

<label class="field wide">
  <span class="field-label">Expect body contains</span>
  <input bind:value={draft.expect_body} placeholder="(any response body)" />
</label>

<div class="field wide">
  <span class="field-label">Headers</span>
  <HeadersEditor
    pairs={draft.headers}
    addLabel="+ Add header"
    namePlaceholder="Authorization"
    valuePlaceholder="Bearer …"
    emptyNote="None. The probe sends only Lighthouse's defaults."
  />
</div>

<div class="field wide">
  <span class="field-label">TLS</span>
  <label class="toggle">
    <input type="checkbox" bind:checked={draft.allow_self_signed} />
    <span>
      Allow self-signed
      <em>— accept an untrusted certificate. Off means HTTPS is verified.</em>
    </span>
  </label>
</div>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
</style>
