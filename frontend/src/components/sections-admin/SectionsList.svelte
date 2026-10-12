<script lang="ts">
  import type { AdminCheck, Section, SectionDisplay } from '../../lib/admin';
  import { AdminError, createSection, deleteSection, reorderSections, updateSection } from '../../lib/admin';
  import type { DragState } from './dragReorder';
  import { dropEdge, reorder } from './dragReorder';

  let {
    sections = $bindable(),
    checks,
    onchanged,
    onexpired,
    refreshAll,
  }: {
    sections: Section[];
    checks: AdminCheck[];
    onchanged: () => void;
    onexpired: (message: string) => void;
    /** A rename carries every check in the section with it, which this
     *  component cannot see from `sections` alone — refetches both lists,
     *  same as the initial load. */
    refreshAll: () => Promise<void>;
  } = $props();

  /** Above this many checks, rendering a section inline costs a history strip
   *  per check on every visitor's first paint. The admin is warned rather than
   *  overridden: the mode is an operator's call, and silently paging a section
   *  they chose to keep inline would be a worse surprise than a large page. */
  const INLINE_WARN_AT = 25;

  let newName = $state('');
  let creating = $state(false);
  /** Section being renamed inline, with its working value. */
  let renaming = $state<{ name: string; value: string } | null>(null);
  let pendingDelete = $state<string | null>(null);
  /** Section name with an in-flight write, or 'order' for a reorder. */
  let busySection = $state<string | null>(null);
  let sectionError = $state<string | null>(null);

  let drag = $state<DragState | null>(null);

  function startDrag(e: DragEvent, list: string, from: number) {
    // Firefox refuses to start a drag unless some data is attached.
    e.dataTransfer?.setData('text/plain', String(from));
    if (e.dataTransfer) e.dataTransfer.effectAllowed = 'move';
    drag = { list, from, over: null };
  }

  /** Only calling preventDefault marks a target droppable, so leaving it out is
   *  what refuses a cross-section drop — the cursor says "no" and no drop fires. */
  function dragOver(e: DragEvent, list: string, index: number) {
    if (!drag || drag.list !== list || drag.from === index) return;
    e.preventDefault();
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
    drag.over = index;
  }

  // Position is the server's ordering authority; the array it hands back may not
  // already be sorted by it.
  const ordered = $derived([...sections].sort((a, b) => a.position - b.position));

  function fail(e: unknown): string {
    const message = e instanceof Error ? e.message : String(e);
    if (e instanceof AdminError && e.unauthorized) onexpired(message);
    return message;
  }

  async function create() {
    const name = newName.trim();
    if (!name || creating) return;
    creating = true;
    sectionError = null;
    try {
      sections = [...sections, await createSection(name)];
      newName = '';
    } catch (e) {
      sectionError = fail(e);
    } finally {
      creating = false;
    }
  }

  async function commitRename() {
    if (!renaming || busySection) return;
    const { name, value } = renaming;
    const next = value.trim();
    if (!next || next === name) return (renaming = null);
    busySection = name;
    sectionError = null;
    try {
      await updateSection(name, { name: next });
      renaming = null;
      // The rename carries every check in the section with it, so the per-check
      // table below is stale until we reload both.
      await refreshAll();
      onchanged();
    } catch (e) {
      sectionError = fail(e);
    } finally {
      busySection = null;
    }
  }

  /** The public page's first render only — this admin list is never collapsed.
   *  The server echoes the section back with a placeholder position, so only the
   *  flag is merged in. */
  async function toggleCollapsed(s: Section) {
    if (busySection) return;
    busySection = s.name;
    sectionError = null;
    try {
      const saved = await updateSection(s.name, { collapsed: !s.collapsed });
      sections = sections.map((x) =>
        x.name === s.name ? { ...x, collapsed: saved.collapsed } : x,
      );
      onchanged();
    } catch (e) {
      sectionError = fail(e);
    } finally {
      busySection = null;
    }
  }

  /** Inline renders the section's rows on the primary page; page renders a
   *  summary tile linking to `#/group/<name>`, whose rows and charts load only
   *  when a visitor opens it. `collapsed` is deliberately left alone — a
   *  section switched to a page and back must come back the way it was. */
  async function setDisplay(s: Section, display: SectionDisplay) {
    if (busySection || s.display === display) return;
    busySection = s.name;
    sectionError = null;
    try {
      const saved = await updateSection(s.name, { display });
      sections = sections.map((x) =>
        x.name === s.name ? { ...x, display: saved.display } : x,
      );
      onchanged();
    } catch (e) {
      sectionError = fail(e);
    } finally {
      busySection = null;
    }
  }

  async function confirmDelete(name: string) {
    pendingDelete = null;
    busySection = name;
    sectionError = null;
    try {
      await deleteSection(name);
      // Its checks survive and become loose, so their `group` field just
      // changed server-side too — the same reload as a rename.
      await refreshAll();
      onchanged();
    } catch (e) {
      sectionError = fail(e);
    } finally {
      busySection = null;
    }
  }

  /** One place to send a section order, so the buttons and the drag agree. */
  async function commitSections(names: string[]) {
    busySection = 'order';
    sectionError = null;
    try {
      sections = await reorderSections(names);
      onchanged();
    } catch (e) {
      sectionError = fail(e);
    } finally {
      busySection = null;
    }
  }

  function move(index: number, delta: number) {
    const target = index + delta;
    if (busySection || target < 0 || target >= ordered.length) return;
    commitSections(reorder(ordered.map((s) => s.name), index, target));
  }

  function dropSection(index: number) {
    const d = drag;
    drag = null;
    if (!d || d.list !== 'sections' || d.from === index || busySection) return;
    commitSections(reorder(ordered.map((s) => s.name), d.from, index));
  }
</script>

<div class="admin-panel">
  <div class="panel-title">Sections</div>
  <p class="admin-muted lead">
    Sections group checks on the public page, in the order below. Checks with no section are
    listed loose above all of them. Drag a row by its handle, or use the arrows. “Collapsed on
    public page” only changes how a visitor first sees the section — they can still expand it,
    and this list is never affected.
  </p>

  <form class="create" onsubmit={(e) => (e.preventDefault(), create())}>
    <input bind:value={newName} placeholder="New section name" />
    <button class="btn primary" type="submit" disabled={creating || !newName.trim()}>
      {creating ? 'Adding…' : 'Add section'}
    </button>
  </form>

  {#if sectionError}<p class="admin-error">{sectionError}</p>{/if}
  {#if busySection === 'order'}<p class="admin-muted">Saving order…</p>{/if}

  {#if ordered.length === 0}
    <p class="admin-muted">No sections yet. Every check is listed loose.</p>
  {:else}
    <div class="slist" role="list">
      {#each ordered as s, i (s.name)}
        {@const members = checks.filter((c) => c.group === s.name).length}
        <div
          class="srow"
          role="listitem"
          class:dragging={drag?.list === 'sections' && drag.from === i}
          class:drop-above={dropEdge(drag, 'sections', i) === 'above'}
          class:drop-below={dropEdge(drag, 'sections', i) === 'below'}
          ondragover={(e) => dragOver(e, 'sections', i)}
          ondrop={(e) => (e.preventDefault(), dropSection(i))}
        >
          <!-- Only the grip is draggable: the row holds a text input while
               renaming, and a draggable row makes that text unselectable. The
               arrows beside it are the keyboard path — a grip has none. -->
          <span
            class="grip"
            draggable={busySection === null}
            aria-hidden="true"
            title="Drag to reorder"
            ondragstart={(e) => startDrag(e, 'sections', i)}
            ondragend={() => (drag = null)}>⠿</span
          >
          <div class="movers">
            <button
              class="reorder-step"
              onclick={() => move(i, -1)}
              disabled={i === 0 || busySection !== null}
              aria-label="Move {s.name} up"><svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 14 6-6 6 6" /></svg></button
            >
            <button
              class="reorder-step"
              onclick={() => move(i, 1)}
              disabled={i === ordered.length - 1 || busySection !== null}
              aria-label="Move {s.name} down"><svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 10 6 6 6-6" /></svg></button
            >
          </div>

          {#if renaming?.name === s.name}
            <input
              bind:value={renaming.value}
              onkeydown={(e) => {
                if (e.key === 'Enter') commitRename();
                if (e.key === 'Escape') renaming = null;
              }}
            />
            <span class="scount"></span>
            <span class="sactions">
              <button class="btn primary" onclick={commitRename} disabled={busySection === s.name}>
                {busySection === s.name ? 'Saving…' : 'Save'}
              </button>
              <button class="btn" onclick={() => (renaming = null)}>Cancel</button>
            </span>
          {:else}
            <span class="sname">{s.name}</span>
            <span class="scount">{members} {members === 1 ? 'check' : 'checks'}</span>
            <span class="sactions">
              <label class="toggle display-mode">
                <em>Public page</em>
                <select
                  value={s.display}
                  disabled={busySection !== null}
                  onchange={(e) => setDisplay(s, e.currentTarget.value as SectionDisplay)}
                >
                  <option value="inline">Rows inline</option>
                  <option value="page">Tile linking to its own page</option>
                </select>
              </label>
              {#if s.display === 'inline'}
                <label
                  class="toggle collapse-toggle"
                  title="How the public status page first renders this section. Visitors can still expand it."
                >
                  <input
                    type="checkbox"
                    checked={s.collapsed}
                    disabled={busySection !== null}
                    onchange={() => toggleCollapsed(s)}
                  />
                  <em>Collapsed on public page</em>
                </label>
                {#if members > INLINE_WARN_AT}
                  <span class="inline-warn">
                    {members} checks render inline on the primary page, each with its own history
                    strip. Its own page would load them only when a visitor opens it.
                  </span>
                {/if}
              {/if}
              {#if pendingDelete === s.name}
                <span class="confirm">
                  Delete “{s.name}”? The checks in it are not deleted — they stay configured and
                  probing, just unassigned and listed loose. {members} affected.
                </span>
                <button class="btn danger" onclick={() => confirmDelete(s.name)}>Delete</button>
                <button class="btn" onclick={() => (pendingDelete = null)}>Cancel</button>
              {:else}
                <button
                  class="btn"
                  onclick={() => ((pendingDelete = null), (renaming = { name: s.name, value: s.name }))}
                  >Rename</button
                >
                <button class="btn danger" onclick={() => (pendingDelete = s.name)}>Delete</button>
              {/if}
            </span>
          {/if}
        </div>
      {/each}
    </div>
  {/if}
</div>

<style>
  .lead {
    margin: -0.35rem 0 0.8rem;
    line-height: 1.5;
  }

  .create {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 0.8rem;
  }
  .create input {
    max-width: 18rem;
  }

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
  .srow.dragging {
    opacity: 0.45;
  }
  /* Insertion line on the side the dragged row will land. Inset shadow rather
     than a border, so the row never shifts by a pixel mid-drag. */
  .srow.drop-above {
    box-shadow: inset 0 2px 0 0 var(--accent);
  }
  .srow.drop-below {
    box-shadow: inset 0 -2px 0 0 var(--accent);
  }
  .collapse-toggle {
    font-size: 0.72rem;
    margin-right: 0.35rem;
  }
  .display-mode {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    font-size: 0.72rem;
    margin-right: 0.35rem;
  }
  .display-mode select {
    font-size: 0.72rem;
    padding: 0.1rem 0.2rem;
  }
  /* A caption, not an error: the operator's choice stands, and this only says
     what it costs. Sized to wrap under the controls rather than stretch the
     row, which would shift every other section's buttons. */
  .inline-warn {
    font-size: 0.7rem;
    color: var(--degraded, var(--text-dim));
    max-width: 26rem;
    text-align: right;
  }

  .srow {
    display: grid;
    /* Name sizes to its text so the member count reads as a caption on it
       rather than drifting into the middle of the row. */
    grid-template-columns: auto auto minmax(6rem, max-content) 1fr auto;
    gap: 0.6rem;
    align-items: center;
    padding: 0.34rem 0;
    font-size: 0.85rem;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  .srow:last-child {
    border-bottom: none;
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
  .sname {
    font-weight: 600;
  }
  .scount {
    font-size: 0.72rem;
    color: var(--text-dim);
  }
  .sactions {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    justify-content: flex-end;
    flex-wrap: wrap;
  }
  /* Deleting a section is not deleting its checks — the confirm has to say so
     before the destructive button is reachable. */
  .confirm {
    font-size: 0.72rem;
    color: var(--down);
    max-width: 30rem;
    text-align: right;
    line-height: 1.4;
  }
</style>
