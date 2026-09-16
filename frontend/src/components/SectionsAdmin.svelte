<script lang="ts">
  import type { AdminCheck, Section } from '../lib/admin';
  import {
    AdminError,
    fetchAdminChecks,
    fetchSections,
    createSection,
    updateSection,
    deleteSection,
    reorderSections,
    reorderChecks,
    updateCheck,
  } from '../lib/admin';

  let { onchanged, onexpired }: { onchanged: () => void; onexpired: () => void } = $props();

  let sections = $state<Section[]>([]);
  let checks = $state<AdminCheck[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let sectionError = $state<string | null>(null);
  let expired = $state(false);

  let newName = $state('');
  let creating = $state(false);
  /** Section being renamed inline, with its working value. */
  let renaming = $state<{ name: string; value: string } | null>(null);
  let pendingDelete = $state<string | null>(null);
  /** Section name with an in-flight write, or 'order' for a reorder. */
  let busySection = $state<string | null>(null);
  /** `${check}|${field}` currently being written. */
  let busyCell = $state<string | null>(null);
  /** A check-order write is in flight; the whole list freezes until it lands. */
  let busyOrder = $state(false);
  let orderError = $state<string | null>(null);

  /** Native HTML5 drag carries no usable payload between elements, so the source
   *  row lives here. `list` is 'sections' or `check:<group>`, and a drag never
   *  crosses lists: dropping a check outside its own section is refused by the
   *  browser rather than silently reassigning the check. */
  let drag = $state<{ list: string; from: number; over: number | null } | null>(null);

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

  /** Which side of the hovered row the dragged one will land on. Dragging down
   *  lands after the target, dragging up lands before it. */
  function dropEdge(list: string, index: number): 'above' | 'below' | null {
    if (!drag || drag.list !== list || drag.over !== index) return null;
    return drag.from < index ? 'below' : 'above';
  }

  function reorder(names: string[], from: number, to: number): string[] {
    const next = [...names];
    const [moved] = next.splice(from, 1);
    next.splice(to, 0, moved);
    return next;
  }

  // Position is the server's ordering authority; the array it hands back may not
  // already be sorted by it.
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
    if (e instanceof AdminError && e.unauthorized) expired = true;
    return e instanceof Error ? e.message : String(e);
  }

  async function refresh() {
    loading = true;
    loadError = null;
    try {
      [sections, checks] = await Promise.all([fetchSections(), fetchAdminChecks()]);
    } catch (e) {
      loadError = fail(e);
    } finally {
      loading = false;
    }
  }

  refresh();

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
      await refresh();
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

  async function confirmDelete(name: string) {
    pendingDelete = null;
    busySection = name;
    sectionError = null;
    try {
      await deleteSection(name);
      await refresh();
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

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{loadError ?? sectionError ?? orderError ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to configure the status page.</p>
    <div class="range-switch retry">
      <button class="range-btn" onclick={onexpired}>Sign in again</button>
    </div>
  </div>
{:else if loading}
  <div class="admin-panel"><p class="admin-muted">Loading…</p></div>
{:else if loadError}
  <div class="admin-panel">
    <p class="admin-error">{loadError}</p>
    <div class="range-switch retry">
      <button class="range-btn" onclick={refresh}>Retry</button>
    </div>
  </div>
{:else}
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
            class:drop-above={dropEdge('sections', i) === 'above'}
            class:drop-below={dropEdge('sections', i) === 'below'}
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
                class="btn nudge"
                onclick={() => move(i, -1)}
                disabled={i === 0 || busySection !== null}
                aria-label="Move {s.name} up">▲</button
              >
              <button
                class="btn nudge"
                onclick={() => move(i, 1)}
                disabled={i === ordered.length - 1 || busySection !== null}
                aria-label="Move {s.name} down">▼</button
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
                class:drop-above={dropEdge(list, i) === 'above'}
                class:drop-below={dropEdge(list, i) === 'below'}
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
                    class="btn nudge"
                    onclick={() => moveCheck(gi, i, -1)}
                    disabled={i === 0 || busyOrder}
                    aria-label="Move {c.name} up">▲</button
                  >
                  <button
                    class="btn nudge"
                    onclick={() => moveCheck(gi, i, 1)}
                    disabled={i === g.rows.length - 1 || busyOrder}
                    aria-label="Move {c.name} down">▼</button
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
{/if}

<style>
  .lead {
    margin: -0.35rem 0 0.8rem;
    line-height: 1.5;
  }
  .retry {
    margin-top: 0.7rem;
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
  .srow.dragging,
  .orow.dragging {
    opacity: 0.45;
  }
  /* Insertion line on the side the dragged row will land. Inset shadow rather
     than a border, so the row never shifts by a pixel mid-drag. */
  .srow.drop-above,
  .orow.drop-above {
    box-shadow: inset 0 2px 0 0 var(--accent);
  }
  .srow.drop-below,
  .orow.drop-below {
    box-shadow: inset 0 -2px 0 0 var(--accent);
  }
  .collapse-toggle {
    font-size: 0.72rem;
    margin-right: 0.35rem;
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
  .nudge {
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
