<script lang="ts">
  let { id }: { id: string } = $props();
  let copied = $state(false);
  async function copy() {
    try {
      await navigator.clipboard.writeText(id);
      copied = true;
    } catch {
      copied = false;
    }
  }
</script>

<div class="credential-id" title="Credential identifier only; the secret token is not recoverable">
  <code>{id}</code>
  <button type="button" aria-label={'Copy credential ID ' + id} title={copied ? 'Copied ID' : 'Copy ID'} onclick={copy}>{copied ? '✓' : '⧉'}</button>
</div>

<style>
  .credential-id {display:inline-flex;align-items:center;gap:8px;min-width:0;max-width:100%;padding:6px 9px;background:#0a1828;border:1px solid #28475e;border-radius:6px;}
  code {color:#a5bfd2;font-size:11px;overflow-wrap:anywhere;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;}
  button {flex:none;font:inherit;font-size:14px;color:#7ecde0;background:transparent;border:0;cursor:pointer;padding:3px;}
  button:hover {color:#c1f2fa;}
  button:focus-visible {outline:2px solid #65d5e5;outline-offset:2px;}
</style>
