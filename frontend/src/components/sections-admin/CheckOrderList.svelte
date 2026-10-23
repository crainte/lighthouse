<script lang="ts">
  import type { AdminCheck, Section } from '../../lib/admin';
  import { AdminError, reorderChecks, updateCheck, saveHomepageSpacers } from '../../lib/admin';
  import type { HomepageSpacer } from '../../lib/homepageSpacers';
  import { moveSpacer, placeSpacers, serviceBlocks } from '../../lib/homepageSpacers';
  import type { DragState } from './dragReorder';
  import { dropEdge, reorder } from './dragReorder';

  let {
    checks = $bindable(),
    spacers = $bindable(),
    sections,
    onchanged,
    onexpired,
  }: {
    checks: AdminCheck[];
    spacers: HomepageSpacer[];
    sections: Section[];
    onchanged: () => void;
    onexpired: (message: string) => void;
  } = $props();

  let busyOrder = $state(false);
  let orderError = $state<string | null>(null);
  /** `${check}|${field}` currently being written. */
  let busyCell = $state<string | null>(null);

  let drag = $state<DragState | null>(null);
  let draggingSpacer = $state<string | null>(null);
  let busySpacer = $state(false);
  let spacerError = $state<string | null>(null);

  function startDrag(e: DragEvent, list: string, from: number) {
    e.dataTransfer?.setData('text/plain', String(from));
    if (e.dataTransfer) e.dataTransfer.effectAllowed = 'move';
    drag = { list, from, over: null };
  }

  function dragOver(e: DragEvent, list: string, index: number) {
    if (!drag || drag.list !== list || drag.from === index) return;
    e.preventDefault();
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
    drag.over = index;
  }

  const ordered = $derived([...sections].sort((a, b) => a.position - b.position));

  // Public services use loose checks, paged sections, then inline sections.
  // Use the *same* pure layout helper for spacer anchors, so placement in this
  // editor agrees with what the visitor sees, including orphan sections.
  const publicBlocks = $derived(serviceBlocks(
    checks.filter((c) => c.visible),
    sections,
    (c) => c.id,
  ));
  const spacerItems = $derived(placeSpacers(publicBlocks, spacers));
  const anchors = $derived(new Set(publicBlocks.map((block) => block.key)));

  // Keep hidden checks editable but never allow them to become spacer anchors:
  // unpublished checks have no row on the public page to anchor to.
  const orderGroups = $derived.by(() => {
    const byPosition = (a: AdminCheck, b: AdminCheck) =>
      (a.position ?? 0) - (b.position ?? 0) || a.name.localeCompare(b.name);
    const sortedChecks = [...checks].sort(byPosition);
    const assigned = [...new Set(sortedChecks.map((c) => c.group).filter((g) => Boolean(g)))] as string[];
    const paged = ordered.filter((section) => section.display === 'page').map((section) => section.name);
    const inline = assigned.filter((name) => !paged.includes(name));
    const names = [...paged, ...inline];
    return [
      { key: '', label: '(listed loose)', rows: sortedChecks.filter((c) => !c.group) },
      ...names.map((name) => ({
        key: name,
        label: name,
        rows: sortedChecks.filter((c) => c.group === name),
      })),
    ].filter((group) =>
      group.rows.length > 0 || (group.key !== '' && anchors.has(`section:${group.key}`)),
    );
  });

  function fail(e: unknown): string {
    const message = e instanceof Error ? e.message : String(e);
    if (e instanceof AdminError && e.unauthorized) onexpired(message);
    return message;
  }

  async function commitChecks(ids: string[]) {
    busyOrder = true;
    orderError = null;
    try {
      checks = await reorderChecks(ids);
      onchanged();
    } catch (e) {
      orderError = fail(e);
    } finally {
      busyOrder = false;
    }
  }

  /** The server takes one flat list of ids, so a move inside a single group
   *  still ships the whole display order. */
  function checkOrderWith(groupIndex: number, from: number, to: number): string[] {
    return orderGroups
      .map((g, i) => {
        const ids = g.rows.map((c) => c.id);
        return i === groupIndex ? reorder(ids, from, to) : ids;
      })
      .flat();
  }

  /** Moves a check past its neighbour inside its own group. */
  function moveCheck(groupIndex: number, index: number, delta: number) {
    const group = orderGroups[groupIndex];
    const target = index + delta;
    if (busyOrder || !group || target < 0 || target >= group.rows.length) return;
    commitChecks(checkOrderWith(groupIndex, index, target));
  }

  function dropCheck(list: string, groupIndex: number, index: number) {
    const d = drag;
    drag = null;
    if (!d || d.list !== list || d.from === index || busyOrder) return;
    commitChecks(checkOrderWith(groupIndex, d.from, index));
  }

  async function persistSpacers(next: HomepageSpacer[]) {
    if (busySpacer || busyOrder) return;
    busySpacer = true;
    spacerError = null;
    try {
      spacers = await saveHomepageSpacers(next);
      onchanged();
    } catch (e) {
      spacerError = fail(e);
    } finally {
      busySpacer = false;
    }
  }

  function addSpacer() {
    const after = publicBlocks.at(-1)?.key ?? null;
    void persistSpacers([...spacers, { id: crypto.randomUUID(), after }]);
  }

  function stepSpacer(id: string, delta: number) {
    void persistSpacers(moveSpacer(publicBlocks, spacers, id, delta));
  }

  function removeSpacer(id: string) {
    void persistSpacers(spacers.filter((spacer) => spacer.id !== id));
  }

  function startSpacerDrag(e: DragEvent, id: string) {
    if (busySpacer) return;
    e.dataTransfer?.setData('text/plain', id);
    if (e.dataTransfer) e.dataTransfer.effectAllowed = 'move';
    draggingSpacer = id;
  }

  /** Dropping on a check or section places the spacer *after* that public row.
   *  A drag never modifies the check's group, order, or configuration. */
  function dropSpacerAfter(e: DragEvent, after: string) {
    e.preventDefault();
    const id = draggingSpacer;
    draggingSpacer = null;
    if (!id || !anchors.has(after)) return;
    const current = spacers.find((spacer) => spacer.id === id);
    if (!current || current.after === after) return;
    void persistSpacers([
      ...spacers.filter((spacer) => spacer.id !== id),
      { id, after },
    ]);
  }

  function dropOnSpacer(e: DragEvent, destinationId: string) {
    e.preventDefault();
    const id = draggingSpacer;
    draggingSpacer = null;
    if (!id || id === destinationId) return;
    const from = spacerItems.findIndex((item) => item.kind === 'spacer' && item.spacer.id === id);
    const to = spacerItems.findIndex((item) => item.kind === 'spacer' && item.spacer.id === destinationId);
    if (from >= 0 && to >= 0) stepSpacer(id, to - from);
  }

  /** Writes a check's section back through the normal check update, so the
   *  server sees the same payload the editor would send. */
  async function patch(c: AdminCheck, next: AdminCheck, cell: string) {
    if (busyCell) return;
    busyCell = cell;
    orderError = null;
    try {
      const saved = await updateCheck(c.id, next);
      checks = checks.map((x) => (x.id === c.id ? saved : x));
      onchanged();
    } catch (e) {
      orderError = fail(e);
    } finally {
      busyCell = null;
    }
  }
</script>

<div class="admin-panel">
  <div class="order-toolbar">
    <div class="panel-title">Check order</div>
    <button class="btn" onclick={addSpacer} disabled={busySpacer || busyOrder}>+ Add spacer</button>
  </div>
  <p class="admin-muted lead">
    Drag checks or use their arrows to reorder within a section. Select a different section
    to move a check there. Add a spacer here, then drag it onto a check or section to insert
    a visual gap after it, or use its arrows to move one position at a time.
    Spacers affect only the public page layout, never monitoring.
  </p>

  {#if orderError}<p class="admin-error" role="alert">{orderError}</p>{/if}
  {#if spacerError}<p class="admin-error" role="alert">{spacerError}</p>{/if}
  {#if busyOrder || busySpacer}<p class="admin-muted">Saving layout…</p>{/if}

  {#snippet spacerRow(spacer: HomepageSpacer)}
    {@const index = spacerItems.findIndex((item) => item.kind === 'spacer' && item.spacer.id === spacer.id)}
    <div class="orow spacer-row" role="listitem"
      ondragover={(e) => { if (draggingSpacer) e.preventDefault(); }}
      ondrop={(e) => dropOnSpacer(e, spacer.id)}>
      <span class="grip" draggable={!busySpacer} aria-hidden="true"
        title="Drag spacer after a check or section"
        ondragstart={(e) => startSpacerDrag(e, spacer.id)}
        ondragend={() => (draggingSpacer = null)}>⠿</span>
      <div class="movers">
        <button class="reorder-step" onclick={() => stepSpacer(spacer.id, -1)}
          disabled={busySpacer || index <= 0} aria-label="Move spacer up">↑</button>
        <button class="reorder-step" onclick={() => stepSpacer(spacer.id, 1)}
          disabled={busySpacer || index >= spacerItems.length - 1} aria-label="Move spacer down">↓</button>
      </div>
      <span class="spacer-label">Spacer <small>— visual gap on the status page</small></span>
      <button class="btn remove-spacer" onclick={() => removeSpacer(spacer.id)}
        disabled={busySpacer} aria-label="Remove spacer">Remove</button>
    </div>
  {/snippet}

  {#if orderGroups.length === 0 && spacers.length === 0}
    <p class="admin-muted">No checks configured yet. You can still add a spacer.</p>
  {:else}
    <div class="admin-rows">
      {#each spacers.filter((spacer) => spacer.after === null) as spacer (spacer.id)}
        {@render spacerRow(spacer)}
      {/each}
      {#each orderGroups as g, gi (g.key)}
        <!-- One drag list per group, so a drop on another group's row never
             matches and the browser refuses it instead of reassigning. -->
        {@const list = `check:${g.key}`}
        <div class="ogroup">
          <div class="olabel" role="group"
            ondragover={(e) => { if (draggingSpacer) e.preventDefault(); }}
            ondrop={(e) => { if (g.key) dropSpacerAfter(e, `section:${g.key}`); }}
          >{g.label}</div>
          <div role="list">
          {#each g.rows as c, i (c.id)}
            <div
              class="orow"
              role="listitem"
              class:dragging={drag?.list === list && drag.from === i}
              class:drop-above={dropEdge(drag, list, i) === 'above'}
              class:drop-below={dropEdge(drag, list, i) === 'below'}
              ondragover={(e) => {
                if (draggingSpacer) e.preventDefault();
                else dragOver(e, list, i);
              }}
              ondrop={(e) => {
                if (draggingSpacer) dropSpacerAfter(e, g.key ? `section:${g.key}` : `check:${c.id}`);
                else { e.preventDefault(); dropCheck(list, gi, i); }
              }}
            >
              <span
                class="grip"
                draggable={!busyOrder}
                aria-hidden="true"
                title="Drag to reorder within {g.label}"
                ondragstart={(e) => startDrag(e, list, i)}
                ondragend={() => (drag = null)}>⠿</span
              >
              <div class="movers">
                <button
                  class="reorder-step"
                  onclick={() => moveCheck(gi, i, -1)}
                  disabled={i === 0 || busyOrder}
                  aria-label="Move {c.name} up"><svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 14 6-6 6 6" /></svg></button
                >
                <button
                  class="reorder-step"
                  onclick={() => moveCheck(gi, i, 1)}
                  disabled={i === g.rows.length - 1 || busyOrder}
                  aria-label="Move {c.name} down"><svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 10 6 6 6-6" /></svg></button
                >
              </div>
              <span class="mono oname" class:off={!c.enabled}>{c.name}</span>
              {#if !c.visible}
                <span class="ohidden" title="not published on the status page">hidden</span>
              {/if}
              <!-- Which section a check sits in is page layout, not check
                   configuration, so it is set here — through the same update
                   call the editor uses. -->
              <select
                class="osection"
                value={c.group ?? ''}
                disabled={busyCell !== null || busyOrder}
                aria-label="Section for {c.name}"
                onchange={(e) => {
                  const group = e.currentTarget.value;
                  const next: AdminCheck = { ...c };
                  if (group) next.group = group;
                  else delete next.group;
                  patch(c, next, `${c.id}|group`);
                }}
              >
                <option value="">(none — listed loose)</option>
                {#each ordered as s (s.name)}
                  <option value={s.name}>{s.name}</option>
                {/each}
                <!-- A check can sit in a section this list never had (renamed
                     elsewhere, stale tab); offering it stops the select from
                     silently reassigning the check on first change. -->
                {#if c.group && !ordered.some((s) => s.name === c.group)}
                  <option value={c.group}>{c.group}</option>
                {/if}
              </select>
            </div>
            {#if !g.key}
              {#each spacers.filter((spacer) => spacer.after === `check:${c.id}`) as spacer (spacer.id)}
                {@render spacerRow(spacer)}
              {/each}
            {/if}
          {/each}
          </div>
          {#if g.key}
            {#each spacers.filter((spacer) => spacer.after === `section:${g.key}`) as spacer (spacer.id)}
              {@render spacerRow(spacer)}
            {/each}
          {/if}
        </div>
      {/each}
      {#each spacers.filter((spacer) => spacer.after !== null && !anchors.has(spacer.after)) as spacer (spacer.id)}
        {@render spacerRow(spacer)}
      {/each}
    </div>
  {/if}
</div>

<style>
  /* Grab affordance for the native drag. Not focusable on purpose: the arrow
     buttons are the accessible path, so a screen reader gains nothing from
     announcing a control it cannot operate. */
  .grip {
    cursor: grab;
    color: var(--text-dim);
    line-height: 1;
    padding: 0 0.1rem;
    user-select: none;
  }
  .grip:active {
    cursor: grabbing;
  }
  .grip:not([draggable='true']) {
    cursor: default;
    opacity: 0.4;
  }
  .orow.dragging {
    opacity: 0.45;
  }
  /* Insertion line on the side the dragged row will land. Inset shadow rather
     than a border, so the row never shifts by a pixel mid-drag. */
  .orow.drop-above {
    box-shadow: inset 0 2px 0 0 var(--accent);
  }
  .orow.drop-below {
    box-shadow: inset 0 -2px 0 0 var(--accent);
  }

  .lead {
    margin: 0.25rem 0 0.8rem;
    line-height: 1.5;
  }
  .order-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 1rem; }
  .spacer-row {
    margin: 1px 0;
    padding: 0.45rem 0.5rem;
    background: rgba(96, 165, 190, 0.07);
    border: 1px dashed var(--border);
    border-radius: 4px;
  }
  .spacer-label { color: var(--text); font-size: 0.8rem; }
  .spacer-label small { color: var(--text-dim); font-size: 0.7rem; }
  .remove-spacer { margin-left: auto; font-size: 0.72rem; }


  .movers {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .reorder-step {
    padding: 0 0.3rem;
    font-size: 0.55rem;
    line-height: 1.25;
  }

  .osection {
    /* Pushed to the row's trailing edge so every select lines up down the
       column even though the names either side of it vary in width. */
    margin-left: auto;
    flex: none;
    width: 13rem;
    font-size: 0.75rem;
  }

  .ogroup {
    margin-bottom: 0.5rem;
  }
  .olabel {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
    padding: 0.3rem 0;
    border-bottom: 1px solid var(--border);
  }
  .orow {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    font-size: 0.8rem;
    padding: 0.28rem 0;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  .orow:last-child {
    border-bottom: none;
  }
  .oname.off {
    opacity: 0.6;
  }
  .ohidden {
    font-size: 0.6rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }

  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
</style>
