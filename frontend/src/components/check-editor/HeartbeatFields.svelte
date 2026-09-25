<script lang="ts">
  import type { Draft } from './draft';
  import NumberField from '../NumberField.svelte';

  let { draft, heartbeatUrl }: { draft: Draft; heartbeatUrl: string | null } = $props();

  let copied = $state(false);
  let copyError = $state<string | null>(null);

  async function copyHeartbeat() {
    if (!heartbeatUrl) return;
    copyError = null;
    try {
      await navigator.clipboard.writeText(heartbeatUrl);
      copied = true;
      setTimeout(() => (copied = false), 1500);
    } catch {
      // Clipboard access needs a secure context, which a LAN deployment over
      // plain HTTP is not. Saying so beats a button that quietly does nothing.
      copyError = 'Clipboard unavailable here — select the URL above and copy it.';
    }
  }
</script>

<label class="field">
  <span class="field-label">Grace (s)</span>
  <NumberField min={10} bind:value={draft.grace_secs} />
  <span class="field-note">No ping within this window and the check goes DOWN.</span>
</label>

<div class="field wide">
  <span class="field-label">Ping URL</span>
  {#if heartbeatUrl}
    <div class="pair-row">
      <input class="mono" readonly value={heartbeatUrl} />
      <button class="btn" onclick={copyHeartbeat}>{copied ? 'Copied' : 'Copy'}</button>
    </div>
    <span class="field-note">
      Have the job call this URL (GET or POST) when it finishes. It needs no authentication —
      the URL itself is the credential, so treat it like one. It is built from the check's
      current name, so renaming and saving moves it — update the job before you save a rename.
    </span>
    {#if copyError}<span class="admin-error">{copyError}</span>{/if}
  {:else}
    <span class="field-note">
      The ping URL is built from the check's name, so it appears here once this check has been
      saved.
    </span>
  {/if}
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
</style>
