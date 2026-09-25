<script lang="ts">
  import type { Draft } from './draft';
  import NumberField from '../NumberField.svelte';
  import HeadersEditor from './HeadersEditor.svelte';

  let { draft }: { draft: Draft } = $props();
</script>

<label class="field wide">
  <span class="field-label">Target URL</span>
  <input class="mono" bind:value={draft.target} placeholder="https://example.com/metrics" />
  <span class="field-note">The exporter's scrape endpoint. Only this one target is probed.</span>
</label>

<label class="field">
  <span class="field-label">Timeout (s)</span>
  <NumberField min={1} bind:value={draft.timeout_secs} />
</label>

<label class="field wide">
  <span class="field-label">Metric name</span>
  <input class="mono" bind:value={draft.metric.name} placeholder="up" />
  <span class="field-note">The metric family to read from the scrape, e.g. "up".</span>
</label>

<div class="field wide">
  <span class="field-label">Metric labels</span>
  <HeadersEditor
    pairs={draft.metric.labels}
    addLabel="+ Add label"
    namePlaceholder="path"
    valuePlaceholder="/health"
    emptyNote="None. If the family has more than one series, the first one wins — add labels to pick a specific one."
  />
  <span class="field-note">
    Narrows the metric family down to one series when several share the same name — e.g.
    <code>path="/health"</code>. All must match.
  </span>
</div>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .field code {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--text);
  }
</style>
