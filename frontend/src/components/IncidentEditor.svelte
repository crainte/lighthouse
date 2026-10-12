<script lang="ts">
  import {
    addIncidentUpdate,
    createIncident,
    editIncident,
    fetchIncident,
    setIncidentChecks,
  } from '../lib/api';
  import { untrack } from 'svelte';
  import type { Incident, IncidentState } from '../lib/api';
  import { deleteIncident, fetchRegions } from '../lib/admin';
  import { formatDuration, formatRelativeTime } from '../lib/format';
  import {
    affectSourceNote,
    checkLabel,
    checkNameMap,
    fromDatetimeLocal,
    incidentRegionChoices,
    incidentStateColor,
    postableStates,
    stateTone,
    toDatetimeLocal,
  } from '../lib/incident';

  let {
    incidentId,
    checks,
    regionScoped = false,
    onClose,
    onSaved,
  }: {
    /** `null` is the create form. Otherwise the editor loads the incident
     *  itself, so the page can be deep-linked and survives a refresh. */
    incidentId: string | null;
    /** Every check's id and mutable name, for the affects picker: the id is
     *  what `setIncidentChecks` sends, the name is what the picker shows. */
    checks: { id: string; name: string }[];
    /** On a global node incidents belong to one region, so creating one asks
     *  which. A coordinator owns its own and never asks. */
    regionScoped?: boolean;
    onClose: () => void;
    onSaved: (i: Incident) => void;
  } = $props();

  const names = $derived(checkNameMap(checks.map((c) => ({ check_id: c.id, name: c.name }))));

  // The server's answer, and the draft over it. Separate so a half-typed title
  // never leaks into the list behind the editor.
  let current = $state<Incident | null>(null);
  let title = $state('');
  let startedAt = $state(toDatetimeLocal(Date.now()));
  let global = $state(false);
  let body = $state('');
  let updateState = $state<IncidentState>('investigating');
  let updateBody = $state('');
  let checkFilter = $state('');
  let regions = $state<string[] | null>(null);
  let region = $state('');
  let error = $state<string | null>(null);
  let busy = $state(false);
  // Seeded so the first paint of an existing incident is the spinner, not an
  // empty form. The effect below owns it after that, hence `untrack`.
  let loading = $state(untrack(() => incidentId) !== null);

  // Two-step confirm, same pattern as the check editor's danger zone — armed
  // by one click, only fires on the second.
  let pendingDelete = $state(false);
  let deleteBusy = $state(false);

  const creating = $derived(current === null);
  const needsRegion = $derived(creating && regionScoped);
  const canCreate = $derived(!needsRegion || region !== '');

  $effect(() => {
    if (!needsRegion || regions !== null) return;
    let cancelled = false;
    fetchRegions()
      .then((all) => {
        if (cancelled) return;
        regions = incidentRegionChoices(all);
        region = regions[0] ?? '';
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        regions = [];
        error = e instanceof Error ? e.message : String(e);
      });
    return () => {
      cancelled = true;
    };
  });

  // Load by id. Also re-seeds the detail fields, which is what makes a save
  // followed by a reload show the saved values rather than the stale draft.
  $effect(() => {
    const id = incidentId;
    if (id === null) {
      loading = false;
      return;
    }
    let cancelled = false;
    loading = true;
    fetchIncident(id)
      .then((r) => {
        if (cancelled) return;
        current = r.incident;
        title = r.incident.title;
        startedAt = toDatetimeLocal(r.incident.started_at);
        global = r.incident.global;
      })
      .catch((e: unknown) => {
        if (!cancelled) error = e instanceof Error ? e.message : String(e);
      })
      .finally(() => {
        if (!cancelled) loading = false;
      });
    return () => {
      cancelled = true;
    };
  });

  // Default the state select to where the incident already is, except while
  // `detected` — posting is what acknowledges it, so offer the next step.
  $effect(() => {
    const s = current?.state;
    updateState = s === undefined || s === 'detected' || s === 'resolved' ? 'investigating' : s;
  });

  const manualChecks = $derived(
    (current?.checks ?? []).filter((c) => c.source === 'manual').map((c) => c.check_id),
  );
  const attached = $derived(new Set((current?.checks ?? []).map((c) => c.check_id)));
  const pickable = $derived(
    checks
      .filter((c) => !attached.has(c.id))
      .filter((c) => c.name.toLowerCase().includes(checkFilter.trim().toLowerCase()))
      .slice(0, 8),
  );

  async function run(work: () => Promise<Incident>) {
    busy = true;
    error = null;
    try {
      const saved = await work();
      current = saved;
      onSaved(saved);
      return true;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      return false;
    } finally {
      busy = false;
    }
  }

  async function saveDetails() {
    if (!title.trim()) {
      error = 'A title is required.';
      return;
    }
    const at = fromDatetimeLocal(startedAt);
    if (creating) {
      // No refetch here: saving swaps the URL to the new incident, which
      // remounts this editor against its id and loads the narrative.
      await run(() =>
        createIncident({
          title: title.trim(),
          state: 'investigating',
          started_at: at,
          global,
          checks: [],
          body: body.trim(),
          ...(needsRegion ? { region } : {}),
        }),
      );
    } else if (current) {
      const id = current.id;
      await run(() => editIncident(id, { title: title.trim(), started_at: at, global }));
    }
  }

  /** `setIncidentChecks` replaces exactly the MANUAL set, so only manual ids go
   *  over the wire. Sending the origin or a dependency would be meaningless at
   *  best and, if the endpoint ever widened, would let the editor delete the
   *  incident's own cause. */
  async function saveManual(next: string[]) {
    if (!current) return;
    const id = current.id;
    await run(() => setIncidentChecks(id, next));
  }

  async function postUpdate() {
    if (!current) return;
    const id = current.id;
    const state = updateState;
    const text = updateBody;
    const ok = await run(() => addIncidentUpdate(id, state, text.trim()));
    if (ok) updateBody = '';
  }

  // Deletes the incident outright — not a resolve, not an archive. If the
  // check is still down, nothing here stops it opening a fresh incident on
  // the next failed probe, so the confirm copy says so.
  async function confirmDeleteIncident() {
    if (!current || deleteBusy) return;
    deleteBusy = true;
    error = null;
    try {
      await deleteIncident(current.id);
      onClose();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      deleteBusy = false;
    }
  }
</script>

{#if loading}
  <div class="notice">Loading incident…</div>
{:else}
  <div class="page-head page-head-row">
    <div>
      <h2>{creating ? 'New incident' : current?.title}</h2>
      <p class="page-meta">
        {#if creating}
          Nothing auto-attaches to a hand-made incident, and nothing auto-resolves it.
        {:else if current}
          <span class="opill {stateTone(current.state)}">{current.state}</span>
          <span class="dim"
            >· {current.state === 'resolved' ? 'lasted' : 'ongoing for'}
            {formatDuration(current.duration_ms)}</span
          >
          {#if current.origin_check}
            <span class="dim">· opened by</span>
            <span class="ot-id">{checkLabel(names, current.origin_check)}</span>
          {:else}
            <span class="dim">· created by hand</span>
          {/if}
        {/if}
      </p>
    </div>
    <div class="head-actions">
      {#if current && !creating}
        <a class="btn" href={`#/incident/${encodeURIComponent(current.id)}`}>View public page</a>
        {#if pendingDelete}
          <span class="confirm">
            {current.state !== 'resolved'
              ? 'Delete it? The check is still failing, so a new incident will just open on its next probe.'
              : 'Delete this incident?'}
          </span>
          <button class="btn danger" onclick={confirmDeleteIncident} disabled={deleteBusy}>
            {deleteBusy ? 'Deleting…' : 'Yes, delete it'}
          </button>
          <button class="btn" onclick={() => (pendingDelete = false)} disabled={deleteBusy}>
            Cancel
          </button>
        {:else}
          <button class="btn danger" onclick={() => (pendingDelete = true)}>
            Delete incident
          </button>
        {/if}
      {/if}
      <button class="btn" onclick={onClose}>Close</button>
    </div>
  </div>

  {#if error}
    <p class="admin-error">{error}</p>
  {/if}

  <div class="ed-grid">
    <!-- 1. Details -->
    <div class="admin-panel">
      <div class="panel-title">Details</div>
      <fieldset class="plain" disabled={busy}>
        <label class="field">
          <span class="field-label">Title</span>
          <input type="text" bind:value={title} placeholder="What visitors will read" />
        </label>
        <label class="field">
          <span class="field-label">Started</span>
          <input type="datetime-local" bind:value={startedAt} />
          <span class="field-note">
            Moving this back says the outage began before a probe saw it. It does not change
            which transitions belong to the incident.
          </span>
        </label>
        <label class="toggle">
          <input type="checkbox" bind:checked={global} />
          <span>Global incident <em>— shown page-wide, not just on its checks.</em></span>
        </label>
        {#if needsRegion}
          <label class="field">
            <span class="field-label">Region</span>
            <select bind:value={region} disabled={!regions?.length}>
              {#if regions === null}
                <option value="">Loading regions…</option>
              {:else if regions.length === 0}
                <option value="">No region is reporting</option>
              {/if}
              {#each regions ?? [] as r (r)}
                <option value={r}>{r}</option>
              {/each}
            </select>
            <span class="field-note">The incident is kept by this region's coordinators.</span>
          </label>
        {/if}
        {#if creating}
          <label class="field">
            <span class="field-label">First update</span>
            <textarea rows="3" bind:value={body} placeholder="What is happening"></textarea>
          </label>
        {/if}
        <div class="row-end">
          <button class="btn primary" onclick={saveDetails} disabled={!canCreate}>
            {creating ? 'Open incident' : 'Save details'}
          </button>
        </div>
      </fieldset>
    </div>

    {#if current}
      <!-- 2. Affects -->
      <div class="admin-panel">
        <div class="panel-title">Affects · {current.checks.length}</div>
        <fieldset class="plain" disabled={busy}>
          {#if current.checks.length === 0}
            <p class="admin-muted">No checks attached.</p>
          {:else}
            <ul class="affect-list">
              {#each current.checks as c (c.check_id)}
                <li>
                  <span class="ot-id">{checkLabel(names, c.check_id)}</span>
                  <span
                    class="opill {c.source === 'origin'
                      ? 'cyan'
                      : c.source === 'dependency'
                        ? 'violet'
                        : 'grey'}"
                    title={affectSourceNote(c.source)}
                  >
                    {c.source === 'dependency' ? 'auto' : c.source}
                  </span>
                  {#if c.source === 'manual'}
                    <button
                      class="btn danger"
                      onclick={() => saveManual(manualChecks.filter((m) => m !== c.check_id))}
                    >
                      Remove
                    </button>
                  {:else}
                    <span class="field-note">coordinator-owned</span>
                  {/if}
                </li>
              {/each}
            </ul>
          {/if}
          <label class="field">
            <span class="field-label">Attach a check</span>
            <input type="search" bind:value={checkFilter} placeholder="Search checks" />
          </label>
          <div class="picker">
            {#each pickable as c (c.id)}
              <button class="btn" onclick={() => saveManual([...manualChecks, c.id])}>
                + {c.name}
              </button>
            {:else}
              <span class="field-note">No unattached check matches.</span>
            {/each}
          </div>
        </fieldset>
      </div>

      <!-- 3. Updates -->
      <div class="admin-panel span-2">
        <div class="panel-title">Updates · {(current.updates ?? []).length}</div>
        <fieldset class="plain" disabled={busy}>
          {#if (current.updates ?? []).length === 0}
            <p class="admin-muted">No updates yet.</p>
          {:else}
            <ol class="timeline">
              {#each current.updates ?? [] as u (u.id)}
                <li>
                  <span class="tl-dot" style="background:{incidentStateColor(u.state)}"></span>
                  <div class="tl-body">
                    <div class="tl-head">
                      <span class="opill {stateTone(u.state)}">{u.state}</span>
                      <span class="field-note">{formatRelativeTime(u.created_at)}</span>
                      <span class="field-note">· {u.author ?? 'coordinator'}</span>
                    </div>
                    {#if u.body}<p class="tl-text">{u.body}</p>{/if}
                  </div>
                </li>
              {/each}
            </ol>
          {/if}

          <div class="post-box">
            <textarea rows="2" bind:value={updateBody} placeholder="What changed"></textarea>
            <div class="post-row">
              {#if current.state === 'resolved'}<span class="field-label">Reopen as</span>{/if}
              <select bind:value={updateState}>
                {#each postableStates(current.state) as s (s)}
                  <option value={s}>{s}</option>
                {/each}
              </select>
              <button class="btn primary" onclick={postUpdate}>
                {current.state === 'resolved' ? 'Reopen incident' : 'Post update'}
              </button>
              <span class="field-note">
                {#if current.state === 'resolved'}
                  Resolved is final unless you reopen it.{#if current.origin_check}{' '}While its
                    check keeps passing, the coordinator will resolve it again on the next probe.{/if}
                {:else}
                  Posting is the only way the state moves — and the first one acknowledges it.
                {/if}
              </span>
            </div>
          </div>
        </fieldset>
      </div>
    {/if}
  </div>
{/if}

<style>
  .head-actions {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    align-items: center;
  }
  .confirm {
    font-size: 0.75rem;
    color: var(--down);
  }
  /* `a` styled as a button: leaving for the public page is navigation, not an
     action, so it should be middle-clickable. */
  a.btn {
    text-decoration: none;
    display: inline-flex;
    align-items: center;
  }
  /* Details and Affects side by side, the timeline full width beneath: the
     narrative is the thing that gets long. */
  .ed-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 320px), 1fr));
    gap: 1rem;
    align-items: start;
  }
  .span-2 {
    grid-column: 1 / -1;
  }
  /* A fieldset only for the `disabled` sweep while a save is in flight. */
  fieldset.plain {
    border: 0;
    margin: 0;
    padding: 0;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 0.8rem;
  }
  .row-end {
    display: flex;
    justify-content: flex-end;
  }
  .affect-list {
    list-style: none;
    padding: 0;
    margin: 0;
  }
  .affect-list li {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.4rem 0;
    border-bottom: 1px solid var(--ad-line-soft);
  }
  .affect-list li:last-child {
    border-bottom: none;
  }
  .affect-list .ot-id {
    flex: 1;
  }
  .picker {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
  }
  /* Timeline with a rail: the dots make the sequence readable at a glance. */
  .timeline {
    list-style: none;
    padding: 0;
    margin: 0;
  }
  .timeline li {
    position: relative;
    display: flex;
    gap: 0.7rem;
    padding: 0 0 0.9rem;
  }
  .timeline li:not(:last-child)::before {
    content: '';
    position: absolute;
    left: 4px;
    top: 1.1rem;
    bottom: 0;
    width: 1px;
    background: var(--ad-line);
  }
  .tl-dot {
    flex: none;
    width: 9px;
    height: 9px;
    border-radius: 50%;
    margin-top: 0.32rem;
  }
  .tl-body {
    min-width: 0;
  }
  .tl-head {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem;
  }
  .tl-text {
    margin: 0.25rem 0 0;
    font-size: 0.85rem;
    white-space: pre-wrap;
    line-height: 1.5;
  }
  .post-box {
    border-top: 1px solid var(--ad-line);
    padding-top: 0.8rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .post-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .post-row select {
    width: auto;
  }
  .dim {
    color: var(--ad-dim);
  }
</style>
