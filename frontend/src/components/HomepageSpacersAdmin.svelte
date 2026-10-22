<script lang="ts">
  import type { AdminCheck, Section } from '../lib/admin';
  import { AdminError, saveHomepageSpacers } from '../lib/admin';
  import type { HomepageSpacer } from '../lib/homepageSpacers';
  import { moveSpacer, placeSpacers } from '../lib/homepageSpacers';

  let {
    checks, sections, spacers = $bindable(), onchanged, onexpired,
  }: {
    checks: AdminCheck[];
    sections: Section[];
    spacers: HomepageSpacer[];
    onchanged: () => void;
    onexpired: (message: string) => void;
  } = $props();

  let busy = $state(false);
  let error = $state<string | null>(null);
  let dragging = $state<string | null>(null);
  const blocks = $derived.by(() => {
    const visible = checks.filter((check) => check.visible);
    const loose = visible.filter((check) => !check.group)
      .sort((a, b) => a.position - b.position || a.name.localeCompare(b.name));
    const known = [...sections].sort((a, b) => a.position - b.position);
    const orphanNames = [...new Set(visible.map((check) => check.group).filter((name) => Boolean(name)))]
      .filter((name) => !known.some((section) => section.name === name));
    const groups = [
      ...known,
      ...orphanNames.map((name) => ({ name: name!, display: 'inline' as const })),
    ];
    return [
      ...loose.map((check) => ({ key: `check:${check.id}`, label: check.name })),
      ...groups.filter((group) => group.display === 'page')
        .map((group) => ({ key: `section:${group.name}`, label: group.name })),
      ...groups.filter((group) => group.display === 'inline' && visible.some((check) => check.group === group.name))
        .map((group) => ({ key: `section:${group.name}`, label: group.name })),
    ];
  });
  const items = $derived(placeSpacers(blocks, spacers));

  async function save(next: HomepageSpacer[]) {
    if (busy) return;
    busy = true;
    error = null;
    try {
      spacers = await saveHomepageSpacers(next);
      onchanged();
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      error = message;
      if (e instanceof AdminError && e.unauthorized) onexpired(message);
    } finally {
      busy = false;
    }
  }

  function add() {
    save([...spacers, { id: crypto.randomUUID(), after: blocks.at(-1)?.key ?? null }]);
  }

  function drop(index: number) {
    const id = dragging;
    dragging = null;
    if (!id || busy) return;
    const from = items.findIndex((item) => item.kind === 'spacer' && item.spacer.id === id);
    if (from !== index) save(moveSpacer(blocks, spacers, id, index - from));
  }
</script>

<div class="admin-panel">
  <div class="panel-title">Homepage spacers</div>
  <p class="admin-muted">Divide the Services list with visual separators. Spacers do not affect monitoring or incidents. Drag a spacer onto a row or use its arrows to place it between services and sections.</p>
  {#if error}<p class="admin-error" role="alert">{error}</p>{/if}
  <button class="btn" onclick={add} disabled={busy}>Add spacer</button>
  {#if busy}<p class="admin-muted">Saving…</p>{/if}
  <div role="list" class="layout-list">
    {#each items as item, index (item.key)}
      <div
        role="listitem"
        class="layout-row"
        class:separator={item.kind === 'spacer'}
        ondragover={(e) => { if (dragging) e.preventDefault(); }}
        ondrop={(e) => { e.preventDefault(); drop(index); }}
      >
        {#if item.kind === 'spacer'}
          <span draggable={!busy} aria-hidden="true" title="Drag spacer to reorder"
            ondragstart={(e) => { e.dataTransfer?.setData('text/plain', item.spacer.id); dragging = item.spacer.id; }}
            ondragend={() => (dragging = null)}>⠿</span>
          <button aria-label="Move spacer {index + 1} up" disabled={busy || index === 0}
            onclick={() => save(moveSpacer(blocks, spacers, item.spacer.id, -1))}>↑</button>
          <button aria-label="Move spacer {index + 1} down" disabled={busy || index === items.length - 1}
            onclick={() => save(moveSpacer(blocks, spacers, item.spacer.id, 1))}>↓</button>
          <span>Spacer · visual divider</span>
          <button class="remove" aria-label="Remove spacer {index + 1}" disabled={busy}
            onclick={() => save(spacers.filter((spacer) => spacer.id !== item.spacer.id))}>Remove</button>
        {:else}
          <span class="admin-muted">{item.key.startsWith('section:') ? 'Section' : 'Check'}</span>
          <span>{item.block.label}</span>
        {/if}
      </div>
    {/each}
  </div>
</div>

<style>
  .layout-list { margin-top: 0.8rem; }
  .layout-row { display: flex; align-items: center; gap: 0.65rem; padding: 0.4rem 0.5rem; border-bottom: 1px solid var(--border); }
  .layout-row.separator { background: rgba(100, 160, 190, 0.08); }
  .layout-row [draggable='true'] { cursor: grab; }
  .remove { margin-left: auto; }
</style>
