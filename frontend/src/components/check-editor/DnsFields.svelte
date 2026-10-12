<script lang="ts">
  import type { Draft } from './draft';
  import { MATCH_MODES, DNS_RECORDS } from './draft';
  import NumberField from '../NumberField.svelte';
  import StringListEditor from './StringListEditor.svelte';

  let { draft }: { draft: Draft } = $props();
</script>

<label class="field wide">
  <span class="field-label">Mode</span>
  <select bind:value={draft.dns.mode}>
    <option value="standard">Standard lookup</option>
    <option value="dnssec">DNSSEC validation</option>
  </select>
  {#if draft.dns.mode === 'dnssec'}
    <span class="field-note">Queries the root DNSKEY and requires a successful authenticated response. No root key is pinned.</span>
  {/if}
</label>

{#if draft.dns.mode === 'standard'}
  <label class="field wide">
    <span class="field-label">Target host</span>
    <input class="mono" bind:value={draft.target} placeholder="example.com" />
    <span class="field-note">The name to look up — not a URL.</span>
  </label>
{/if}

<label class="field">
  <span class="field-label">Timeout (s)</span>
  <NumberField min={1} bind:value={draft.timeout_secs} />
</label>

<label class="field">
  <span class="field-label">Nameserver</span>
  <input class="mono" bind:value={draft.dns.nameserver} placeholder="(system resolver)" />
  <span class="field-note">
    Optional: an IP, with a port if not 53. Blank uses the agent's system resolver(s).
    {#if draft.dns.mode === 'dnssec'}
      Set it to test a specific validating resolver — a local stub (Docker's, systemd-resolved)
      may strip the authenticated flag and read as a failure.
    {/if}
  </span>
</label>

{#if draft.dns.mode === 'standard'}
  <label class="field">
    <span class="field-label">Record</span>
    <select bind:value={draft.dns.record}>
      {#each DNS_RECORDS as r (r)}
        <option value={r}>{r}</option>
      {/each}
    </select>
  </label>

  <label class="field">
    <span class="field-label">Match type</span>
    <select bind:value={draft.dns.match_type}>
      {#each MATCH_MODES as m (m)}
        <option value={m}>{m}</option>
      {/each}
    </select>
    <span class="field-note">all = every expected value must appear. any = one of them.</span>
  </label>

  <div class="field wide">
    <span class="field-label">Expected answers</span>
    <StringListEditor
      values={draft.dns.expected}
      addLabel="+ Add value"
      placeholder="93.184.216.34"
      emptyNote="None. Resolving at all is enough."
    />
  </div>
{/if}

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
</style>
