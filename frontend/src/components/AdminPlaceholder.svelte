<script lang="ts">
  // A nav item that leads nowhere is worse than no nav item, so this panel says
  // plainly that nothing is wired up. No dummy rows, no dead controls.
  let { title, note }: { title: string; note: string } = $props();
</script>

<div class="admin-panel">
  <div class="panel-title">{title}</div>
  <p class="admin-muted">{note}</p>
</div>
