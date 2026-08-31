<script lang="ts">
  import type { MaintenanceResponse, MaintenanceWindow } from '../lib/admin';
  import { AdminError, fetchMaintenance, saveMaintenance } from '../lib/admin';

  let { onexpired }: { onexpired: () => void } = $props();

  /** Rows hold the times as the `datetime-local` strings the inputs bind to,
   *  and are converted to epoch millis on the way out. Keyed by their own
   *  counter for the same reason the notifier rows are: keying by index reuses
   *  the wrong input when a row is removed. */
  type Row = {
    key: number;
    title: string;
    starts: string;
    ends: string;
    checks: string[];
    announce: boolean;
  };
  let nextKey = 0;

  let rows = $state<Row[]>([]);
  let checks = $state<string[]>([]);
  let saved = $state('');
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let saveError = $state<string | null>(null);
  let saving = $state(false);
  let expired = $state(false);
  /** Re-read on every save so a row's pill is right without a timer ticking
   *  behind the panel. A window crossing its boundary while the form sits open
   *  is a stale pill, not a stale window — the backend reads the clock. */
  let now = $state(Date.now());

  /** `datetime-local` speaks local wall time with no zone, so the offset has to
   *  come out before the ISO slice and go back in when parsing. */
  function toLocal(ms: number): string {
    const d = new Date(ms - new Date(ms).getTimezoneOffset() * 60_000);
    return d.toISOString().slice(0, 16);
  }

  function toWindow(r: Row): MaintenanceWindow {
    return {
      title: r.title,
      starts_at: new Date(r.starts).getTime(),
      ends_at: new Date(r.ends).getTime(),
      checks: r.checks,
      announce: r.announce,
    };
  }

  const list = $derived(rows.map(toWindow));
  const dirty = $derived(JSON.stringify(list) !== saved);

  function phase(r: Row): { label: string; tone: string } {
    const w = toWindow(r);
    if (Number.isNaN(w.starts_at) || Number.isNaN(w.ends_at)) return { label: '—', tone: '' };
    if (now >= w.ends_at) return { label: 'ended', tone: 'done' };
    if (now >= w.starts_at) return { label: 'running', tone: 'live' };
    return { label: 'upcoming', tone: 'soon' };
  }

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) expired = true;
    return e instanceof Error ? e.message : String(e);
  }

  function adopt(r: MaintenanceResponse) {
    rows = r.windows.map((w) => ({
      key: nextKey++,
      title: w.title,
      starts: toLocal(w.starts_at),
      ends: toLocal(w.ends_at),
      checks: w.checks,
      announce: w.announce,
    }));
    checks = r.checks;
    saved = JSON.stringify(r.windows);
    now = Date.now();
  }

  async function refresh() {
    loading = true;
    loadError = null;
    try {
      adopt(await fetchMaintenance());
    } catch (e) {
      loadError = fail(e);
    } finally {
      loading = false;
    }
  }

  refresh();

  /** An hour from now, which is the shape of most planned work. Every field is
   *  editable, so this is a starting point rather than a policy. */
  function add() {
    const start = Date.now();
    rows = [
      ...rows,
      {
        key: nextKey++,
        title: '',
        starts: toLocal(start),
        ends: toLocal(start + 60 * 60 * 1000),
        checks: [],
        announce: false,
      },
    ];
  }

  function remove(key: number) {
    rows = rows.filter((r) => r.key !== key);
  }

  async function save() {
    if (saving) return;
    saving = true;
    saveError = null;
    try {
      adopt(await saveMaintenance(list));
    } catch (e) {
      saveError = fail(e);
    } finally {
      saving = false;
    }
  }
</script>

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{loadError ?? saveError ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to change the schedule.</p>
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
    <div class="panel-title">Maintenance</div>
    <p class="admin-muted lead">
      A window stops the checks it covers from paging anyone while it runs. Probing carries on, so
      the grid, the latency chart and the uptime figure still show what really happened — and the
      outage is still recorded as an alert and an incident. Announce one to say so on the public
      page; leave it silent to suppress the pages and tell nobody.
    </p>

    {#if rows.length === 0}
      <p class="admin-muted">
        Nothing scheduled. Every check pages according to its own routing.
      </p>
    {:else}
      <div class="wlist">
        {#each rows as row (row.key)}
          {@const s = phase(row)}
          <div class="wrow">
            <span class="pill {s.tone}">{s.label}</span>
            <input class="wtitle" placeholder="what the work is" bind:value={row.title} />
            <label class="when">
              <span>from</span>
              <input type="datetime-local" bind:value={row.starts} />
            </label>
            <label class="when">
              <span>to</span>
              <input type="datetime-local" bind:value={row.ends} />
            </label>
            <label class="scope" title="Nothing selected means every check">
              <span>
                {row.checks.length === 0
                  ? 'all checks'
                  : `${row.checks.length} check${row.checks.length === 1 ? '' : 's'}`}
              </span>
              <select multiple size="3" bind:value={row.checks}>
                {#each checks as name (name)}
                  <option value={name}>{name}</option>
                {/each}
              </select>
            </label>
            <label class="ann" title="Shown to anonymous visitors while the window runs">
              <input type="checkbox" bind:checked={row.announce} />
              <span>announce</span>
            </label>
            <button class="btn danger" onclick={() => remove(row.key)}>Remove</button>
          </div>
        {/each}
      </div>
    {/if}

    {#if saveError}<p class="admin-error save-error">{saveError}</p>{/if}

    <div class="actions">
      <button class="btn" onclick={add}>Add window</button>
      <span class="spacer"></span>
      {#if dirty}<span class="admin-muted">Unsaved changes</span>{/if}
      <button class="btn primary" onclick={save} disabled={!dirty || saving}>
        {saving ? 'Saving…' : 'Save'}
      </button>
    </div>
  </div>
{/if}

<style>
  .lead {
    margin-bottom: 0.7rem;
    line-height: 1.5;
  }
  .wlist {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .wrow {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: 0.4rem;
    padding: 0.5rem;
    border: 1px solid var(--border);
    border-radius: 8px;
  }
  .wtitle {
    flex: 1 1 12rem;
    min-width: 8rem;
  }
  .when,
  .scope,
  .ann {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    font-size: 0.72rem;
    color: var(--text-dim);
  }
  .scope {
    align-items: flex-start;
    flex-direction: column;
    gap: 0.15rem;
  }
  .scope select {
    min-width: 10rem;
  }
  .pill {
    font-size: 0.66rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 0.15rem 0.4rem;
    border-radius: 999px;
    border: 1px solid var(--border);
    color: var(--text-dim);
  }
  .pill.live {
    color: var(--accent);
    border-color: var(--accent);
  }
  .pill.soon {
    color: var(--text);
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    margin-top: 0.7rem;
  }
  .spacer {
    flex: 1;
  }
  .save-error {
    margin-top: 0.5rem;
  }
</style>
