<script lang="ts">
  import type { AdminCheck, Section } from '../../lib/admin';
  import { AdminError, reorderChecks, updateCheck } from '../../lib/admin';
  import type { DragState } from './dragReorder';
  import { dropEdge, reorder } from './dragReorder';

  let {
    checks = $bindable(),
    sections,
    onchanged,
    onexpired,
  }: {
    checks: AdminCheck[];
    sections: Section[];
    onchanged: () => void;
    onexpired: (message: string) => void;
  } = $props();

  let busyOrder = $state(false);
  let orderError = $state<string | null>(null);
  /** `${check}|${field}` currently being written. */
  let busyCell = $state<string | null>(null);

  let drag = $state<DragState | null>(null);

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

  // Mirrors the public page: loose checks above every section, sections in their
  // own order, checks inside each by position. A check parked in a section the
  // list doesn't know about still gets a group of its own — otherwise it would
  // be invisible here and unorderable. Empty sections are dropped: nothing to
  // order. Keys stay unique because the group names are Set-deduped and '' can
  // never be a section name.
  const orderGroups = $derived.by(() => {
    const byPosition = (a: AdminCheck, b: AdminCheck) =>
      (a.position ?? 0) - (b.position ?? 0) || a.name.localeCompare(b.name);
    const known = ordered.map((s) => s.name);
    const assigned = [...new Set(checks.map((c) => c.group).filter((g) => Boolean(g)))] as string[];
    const names = [...known, ...assigned.filter((n) => !known.includes(n))];
    return [
      { key: '', label: '(listed loose)', rows: checks.filter((c) => !c.group).sort(byPosition) },
      ...names.map((n) => ({
        key: n,
        label: n,
        rows: checks.filter((c) => c.group === n).sort(byPosition),
      })),
    ].filter((g) => g.rows.length > 0);
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
  <div class="panel-title">Check order</div>
  <p class="admin-muted lead">
    The order checks appear in on the public page, within their own section. Loose checks render
    above every section. Drag a row by its handle, or use the arrows — those only move a check
    inside the section it is already in. To put it in a different one, use the select on its row.
    Everything else about a check lives in its own editor.
  </p>

  {#if orderError}<p class="admin-error">{orderError}</p>{/if}
  {#if busyOrder}<p class="admin-muted">Saving order…</p>{/if}

  {#if orderGroups.length === 0}
    <p class="admin-muted">No checks configured yet.</p>
  {:else}
    <div class="admin-rows">
      {#each orderGroups as g, gi (g.key)}
        <!-- One drag list per group, so a drop on another group's row never
             matches and the browser refuses it instead of reassigning. -->
        {@const list = `check:${g.key}`}
        <div class="ogroup">
          <div class="olabel">{g.label}</div>
          <div role="list">
          {#each g.rows as c, i (c.id)}
            <div
              class="orow"
              role="listitem"
              class:dragging={drag?.list === list && drag.from === i}
              class:drop-above={dropEdge(drag, list, i) === 'above'}
              class:drop-below={dropEdge(drag, list, i) === 'below'}
              ondragover={(e) => dragOver(e, list, i)}
              ondrop={(e) => (e.preventDefault(), dropCheck(list, gi, i))}
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
          {/each}
          </div>
        </div>
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
    margin: -0.35rem 0 0.8rem;
    line-height: 1.5;
  }

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
