<script lang="ts">
  let { id }: { id: string } = $props();
</script>

<code class="credential-id" title="Credential ID only; not a usable token. The secret cannot be recovered.">{id}</code>

<style>
  .credential-id {display:inline-block;max-width:100%;padding:6px 9px;background:#0a1828;border:1px solid #28475e;border-radius:6px;color:#a5bfd2;font-size:11px;overflow-wrap:anywhere;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;}
</style>
