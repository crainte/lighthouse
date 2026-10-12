<script lang="ts">
  import type { Notifier, NotifiersResponse, Undelivered } from '../lib/admin';
  import { AdminError, fetchNotifiers, saveNotifiers, testNotifier } from '../lib/admin';
  import NumberField from './NumberField.svelte';

  let { onexpired }: { onexpired: () => void } = $props();

  /** Rows carry a key of their own because the list is edited in place: keying
   *  an {#each} by index reuses the wrong input when a row is removed, and
   *  keying by name breaks the moment two rows are blank. */
  type Row = { key: number; notifier: Notifier };
  let nextKey = 0;

  let rows = $state<Row[]>([]);
  let unrouted = $state<string[]>([]);
  let undelivered = $state<Undelivered[]>([]);
  let saved = $state('');
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let saveError = $state<string | null>(null);
  let saving = $state(false);
  let expired = $state(false);
  /** Test result per row key. Cleared when the row changes, so a stale "sent"
   *  never sits under a URL that has since been edited. */
  let tested = $state<Record<number, { ok: boolean; message: string }>>({});
  let testing = $state<number | null>(null);

  const list = $derived(rows.map((r) => r.notifier));
  const dirty = $derived(JSON.stringify(list) !== saved);
  /** With nothing marked default, a check that names no notifier reaches
   *  nobody — silently, which is the failure mode worth shouting about. */
  const noDefault = $derived(rows.length > 0 && !rows.some((r) => r.notifier.default));
  /** Marking several is deliberate — an unrouted alert fans out to all of them
   *  — but "default" reads as if it were exclusive, so the panel spells out
   *  the consequence rather than leaving it to be inferred. */
  const defaults = $derived(
    rows.map((r) => r.notifier.name.trim()).filter((n, i) => n && rows[i].notifier.default),
  );
  /** Split rather than one count: "reached nobody" and "still trying" are
   *  different news, and an operator reads the first one differently. */
  const gaveUp = $derived(undelivered.filter((u) => u.given_up).length);
  const retrying = $derived(undelivered.length - gaveUp);

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) expired = true;
    return e instanceof Error ? e.message : String(e);
  }

  function adopt(r: NotifiersResponse) {
    rows = r.notifiers.map((notifier) => ({ key: nextKey++, notifier }));
    unrouted = r.unrouted_checks;
    undelivered = r.undelivered;
    saved = JSON.stringify(r.notifiers);
    tested = {};
  }

  async function refresh() {
    loading = true;
    loadError = null;
    try {
      adopt(await fetchNotifiers());
    } catch (e) {
      loadError = fail(e);
    } finally {
      loading = false;
    }
  }

  refresh();

  /** A new row of each kind, in the state the backend accepts once filled in.
   *  Blank optionals are read as absence server-side, so "" is safe to bind. */
  function blank(type: Notifier['type'], isDefault: boolean): Notifier {
    const base = { name: '', default: isDefault };
    switch (type) {
      case 'webhook':
        return { ...base, type, url: '' };
      case 'ntfy':
        return { ...base, type, topic: '', base_url: '' };
      case 'slack':
        return { ...base, type, webhook_url: '' };
      case 'pagerduty':
        return { ...base, type, routing_key: '' };
      case 'email':
        return {
          ...base,
          type,
          host: '',
          port: null,
          username: '',
          password: '',
          from: '',
          to: '',
          tls: 'starttls',
        };
    }
  }

  function add(type: Notifier['type']) {
    // The first notifier is a default; later ones are not, because adding a
    // second usually means splitting an audience rather than doubling it.
    rows = [...rows, { key: nextKey++, notifier: blank(type, rows.length === 0) }];
  }

  function remove(key: number) {
    rows = rows.filter((r) => r.key !== key);
    delete tested[key];
  }

  /** Any edit invalidates a previous test of that row. */
  function touched(key: number) {
    if (tested[key]) delete tested[key];
  }

  async function test(row: Row) {
    testing = row.key;
    try {
      const r = await testNotifier(row.notifier);
      tested[row.key] = {
        ok: r.delivered,
        message: r.delivered ? 'Sent' : (r.error ?? 'Not delivered'),
      };
    } catch (e) {
      tested[row.key] = { ok: false, message: fail(e) };
    } finally {
      testing = null;
    }
  }

  async function save() {
    if (saving) return;
    saving = true;
    saveError = null;
    try {
      adopt(await saveNotifiers(list));
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
    <p class="admin-muted">Your session expired. Sign in again to change where alerts go.</p>
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
    <div class="panel-title">Notifications</div>
    <p class="admin-muted lead">
      Choose where confirmed state changes are delivered. Checks use their selected channels,
      or the default channels when none are selected. Checks with notifications disabled do not send alerts.
      Test a channel before saving to verify delivery.
    </p>

    {#if unrouted.length}
      <p class="warn">
        Routed to a notifier that does not exist, so nothing is paged for them:
        <strong>{unrouted.join(', ')}</strong>. Rename a notifier back, or repoint those
        checks in the check editor.
      </p>
    {/if}
    {#if noDefault}
      <p class="warn">
        No notifier is marked default. Every check that names none of them reaches nobody.
      </p>
    {:else if defaults.length}
      <p class="admin-muted routing">
        A check that names no notifier goes to{defaults.length > 1 ? ' all of these:' : ''}
        <strong>{defaults.join(', ')}</strong>.
        {#if defaults.length > 1}
          Marking several is a fan-out, not a choice between them — untick the ones that
          should only hear the checks that name them.
        {/if}
      </p>
    {/if}

    {#if undelivered.length}
      <p class="warn">
        {#if gaveUp}
          {gaveUp} notification{gaveUp === 1 ? '' : 's'} reached nobody.
        {/if}
        {#if retrying}
          {retrying} {retrying === 1 ? 'is' : 'are'} still being retried — a queued one follows
          its notifier by name, so fixing the row below fixes the delivery too.
        {/if}
      </p>
      <ul class="undel">
        {#each undelivered as u (u.notifier + u.check_id + u.queued_at)}
          <li>
            <strong>{u.name} → {u.to}</strong> via {u.notifier || '(unnamed)'} —
            {u.error}
            <span class="admin-muted">
              {u.given_up
                ? `gave up after ${u.attempts} attempts`
                : `attempt ${u.attempts}, retrying`}
            </span>
          </li>
        {/each}
      </ul>
    {/if}

    {#if rows.length === 0}
      <p class="admin-muted">No delivery channels yet. Alerts remain available in the console; add a channel to receive notifications.</p>
    {:else}
      <div class="nlist">
        {#each rows as row (row.key)}
          <div class="nrow">
            <div class="channel-heading">
            <input
              class="nname"
               placeholder="Channel name"
               aria-label="Channel name"
              bind:value={row.notifier.name}
              oninput={() => touched(row.key)}
            />
            <span class="ntype">{row.notifier.type}</span>
            </div>
            <div class="channel-fields">
            {#if row.notifier.type === 'webhook'}
              <label class="channel-field"><span>Webhook URL</span>
              <input
                class="grow"
                placeholder="https://hooks.example.com/…"
                bind:value={row.notifier.url}
                oninput={() => touched(row.key)}
              />
              </label>
            {:else if row.notifier.type === 'slack'}
              <label class="channel-field"><span>Slack webhook URL</span>
              <input
                class="grow"
                placeholder="https://hooks.slack.com/services/…"
                bind:value={row.notifier.webhook_url}
                oninput={() => touched(row.key)}
              />
              </label>
            {:else if row.notifier.type === 'pagerduty'}
              <label class="channel-field"><span>Integration key</span>
              <input
                class="grow"
                placeholder="Events API v2 integration key"
                bind:value={row.notifier.routing_key}
                oninput={() => touched(row.key)}
              />
              </label>
            {:else if row.notifier.type === 'email'}
              <label class="channel-field"><span>SMTP host</span>
              <input
                class="topic"
                placeholder="smtp host"
                bind:value={row.notifier.host}
                oninput={() => touched(row.key)}
              />
              </label>
              <!-- A function binding rather than `bind:value`: `port` lives on
                   the email variant alone, and the union does not narrow inside
                   a binding expression the way it does inside this branch. -->
              <label class="channel-field"><span>Port</span>
              <NumberField
                min={1}
                max={65535}
                placeholder="port"
                bind:value={
                  () => (row.notifier.type === 'email' ? (row.notifier.port ?? null) : null),
                  (v) => {
                    if (row.notifier.type === 'email') row.notifier.port = v;
                  }
                }
                onchanged={() => touched(row.key)}
              />
              </label>
              <label class="channel-field"><span>Encryption</span>
              <select
                class="tls"
                bind:value={row.notifier.tls}
                onchange={() => touched(row.key)}
                title="STARTTLS is required, not opportunistic — a relay that does not offer it fails rather than sending the password in the clear"
              >
                <option value="starttls">starttls</option>
                <option value="implicit">implicit tls</option>
                <option value="none">no tls</option>
              </select>
              </label>
              <label class="channel-field"><span>From address</span>
              <input
                class="topic"
                placeholder="from address"
                bind:value={row.notifier.from}
                oninput={() => touched(row.key)}
              />
              </label>
              <label class="channel-field"><span>To address</span>
              <input
                class="topic"
                placeholder="to address"
                bind:value={row.notifier.to}
                oninput={() => touched(row.key)}
              />
              </label>
              <label class="channel-field"><span>Username (optional)</span>
              <input
                class="topic"
                placeholder="username (optional)"
                value={row.notifier.username ?? ''}
                oninput={(e) => {
                  if (row.notifier.type === 'email') row.notifier.username = e.currentTarget.value;
                  touched(row.key);
                }}
              />
              </label>
              <label class="channel-field"><span>Password</span>
              <input
                class="topic"
                type="password"
                placeholder="password"
                value={row.notifier.password ?? ''}
                oninput={(e) => {
                  if (row.notifier.type === 'email') row.notifier.password = e.currentTarget.value;
                  touched(row.key);
                }}
              />
              </label>
            {:else}
              <label class="channel-field"><span>Topic</span>
              <input
                class="topic"
                placeholder="topic"
                bind:value={row.notifier.topic}
                oninput={() => touched(row.key)}
              />
              </label>
              <label class="channel-field"><span>Server URL (optional)</span>
              <input
                class="grow"
                placeholder="https://ntfy.sh (default)"
                value={row.notifier.base_url ?? ''}
                oninput={(e) => {
                  if (row.notifier.type === 'ntfy')
                    row.notifier.base_url = e.currentTarget.value;
                  touched(row.key);
                }}
              />
              </label>
            {/if}
            </div>
            <div class="channel-actions">
            <label class="deflt" title="Receives checks that name no notifier">
              <input type="checkbox" bind:checked={row.notifier.default} />
              <span>Default channel</span>
            </label>
            <button class="btn" onclick={() => test(row)} disabled={testing === row.key}>
              {testing === row.key ? 'Sending…' : 'Test'}
            </button>
            <button class="btn danger" onclick={() => remove(row.key)}>Remove</button>
            </div>
            {#if tested[row.key]}
              <span class="result" class:ok={tested[row.key].ok}>{tested[row.key].message}</span>
            {/if}
          </div>
        {/each}
      </div>
    {/if}

    {#if saveError}<p class="admin-error save-error">{saveError}</p>{/if}

    <div class="actions">
      <button class="btn" onclick={() => add('webhook')}>Add webhook</button>
      <button class="btn" onclick={() => add('ntfy')}>Add ntfy</button>
      <button class="btn" onclick={() => add('slack')}>Add Slack</button>
      <button class="btn" onclick={() => add('pagerduty')}>Add PagerDuty</button>
      <button class="btn" onclick={() => add('email')}>Add email</button>
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
    margin: -0.35rem 0 0.9rem;
    line-height: 1.5;
  }
  .retry {
    margin-top: 0.7rem;
  }
  .routing {
    line-height: 1.5;
    margin: 0 0 0.7rem;
  }
  .warn {
    color: var(--amber);
    font-size: 0.82rem;
    line-height: 1.5;
    margin: 0 0 0.7rem;
  }
  .undel {
    margin: -0.4rem 0 0.9rem;
    padding-left: 1.1rem;
    font-size: 0.8rem;
    line-height: 1.6;
  }
  .nlist { display: grid; gap: 16px; margin: 20px 0; }
  .channel-heading, .channel-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; }
  .channel-heading { padding-bottom: 16px; border-bottom: 1px solid var(--border); }
  .channel-fields { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 240px), 1fr)); gap: 16px; }
  .channel-field { display: flex; flex-direction: column; gap: 8px; min-width: 0; font-size: 12px; color: var(--text-dim); }
  .channel-actions { padding-top: 16px; border-top: 1px solid var(--border); }
  .channel-actions .deflt { margin-right: auto; }
  .nrow {
    display: grid;
    gap: 20px;
    padding: 20px;
    border: 1px solid var(--border);
    border-radius: 8px;
  }
  .nname {
    flex: 1 1 12rem;
    max-width: 28rem;
  }
  .ntype {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
    min-width: 4.2rem;
  }
  .grow, .topic, .tls { width: 100%; min-width: 0; }
  .deflt {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    font-size: 0.78rem;
    color: var(--text-dim);
    cursor: pointer;
  }
  .result {
    font-size: 0.78rem;
    color: var(--down);
    flex-basis: 100%;
  }
  .result.ok {
    color: var(--up);
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
    margin-top: 24px;
    padding-top: 20px;
    border-top: 1px solid var(--border);
  }
  .spacer {
    flex: 1;
  }
  .save-error {
    margin-top: 0.8rem;
  }
</style>
