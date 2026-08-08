<script lang="ts">
  import { onMount } from 'svelte';
  import {
    fetchStatus,
    fetchAlerts,
    fetchIncidents,
    acknowledgeIncident,
    fetchUptime,
    fetchLatencySparkline,
    fetchAgents,
    fetchPublicSections,
  } from './lib/api';
  import type {
    Check,
    Alert,
    Incident,
    Agent,
    UptimeResponse,
    UptimeBucket,
    LatencySparklineResponse,
    PublicSection,
  } from './lib/api';
  import { connectWs } from './lib/ws';
  import type { WsStatus } from './lib/ws';
  import { worstStatus } from './lib/format';
  import OverallBanner from './components/OverallBanner.svelte';
  import ComponentRow from './components/ComponentRow.svelte';
  import AlertsList from './components/AlertsList.svelte';
  import IncidentsList from './components/IncidentsList.svelte';
  import CheckDetail from './components/CheckDetail.svelte';
  import DayDetailModal from './components/DayDetailModal.svelte';
  import CollapsibleGroup from './components/CollapsibleGroup.svelte';
  import ServerRow from './components/ServerRow.svelte';
  import LoginModal from './components/LoginModal.svelte';
  import ChecksAdmin from './components/ChecksAdmin.svelte';
  import CheckEditor from './components/CheckEditor.svelte';
  import SectionsAdmin from './components/SectionsAdmin.svelte';
  import AgentsAdmin from './components/AgentsAdmin.svelte';
  import AdminPlaceholder from './components/AdminPlaceholder.svelte';
  import { fetchSession, logout } from './lib/admin';

  // One array drives the sidebar and the content switch, so a new admin area is
  // a single line here plus its panel.
  const ADMIN_PANELS = [
    { id: 'checks', label: 'Checks' },
    { id: 'status', label: 'Status page' },
    { id: 'agents', label: 'Agents' },
    { id: 'alerts', label: 'Alerts' },
    { id: 'incidents', label: 'Incidents' },
    { id: 'maintenance', label: 'Maintenance' },
  ] as const;
  type AdminPanel = (typeof ADMIN_PANELS)[number]['id'];

  type View =
    | { name: 'home' }
    | { name: 'detail'; checkId: string }
    // `edit` present = the check editor page rather than the panel's own
    // content; `edit.name === null` is a check that does not exist yet.
    | { name: 'admin'; panel: AdminPanel; edit?: { name: string | null } };
  type DayModal = { checkId: string; bucket: UptimeBucket } | null;

  // The view is reflected in the URL hash so the browser/mouse Back button steps
  // back into the app instead of leaving it — and a check page can be linked
  // and survives a refresh.
  function viewFromHash(): View {
    // #/admin/checks/<name> — or /new — is the editor page for one check.
    // "new" is therefore a reserved name: a check actually called "new" would
    // open the create form instead. Accepted rather than paid for with a query
    // parameter hanging off every other admin URL.
    const editor = location.hash.match(/^#\/admin\/checks\/(.+?)\/?$/);
    if (editor) {
      const raw = decodeURIComponent(editor[1]);
      return { name: 'admin', panel: 'checks', edit: { name: raw === 'new' ? null : raw } };
    }
    const admin = location.hash.match(/^#\/admin(?:\/([^/]*))?\/?$/);
    if (admin) {
      // An unknown or absent id lands on the first panel rather than rendering
      // an empty content area.
      const panel = ADMIN_PANELS.find((p) => p.id === admin[1])?.id ?? ADMIN_PANELS[0].id;
      return { name: 'admin', panel };
    }
    const m = location.hash.match(/^#\/check\/(.+)$/);
    return m ? { name: 'detail', checkId: decodeURIComponent(m[1]) } : { name: 'home' };
  }

  let checks = $state<Check[]>([]);
  let alerts = $state<Alert[]>([]);
  let incidents = $state<Incident[]>([]);
  let agents = $state<Agent[]>([]);
  // Only carries the headings' metadata — /api/v1/status has none, and the
  // collapsed flag decides how a group first renders.
  let sections = $state<PublicSection[]>([]);
  let uptime = $state<Record<string, UptimeResponse>>({});
  let sparkline = $state<Record<string, LatencySparklineResponse>>({});
  let loading = $state(true);
  let error = $state<string | null>(null);
  let wsStatus = $state<WsStatus>('connecting');
  let view = $state<View>(viewFromHash());
  let dayModal = $state<DayModal>(null);
  let authenticated = $state(false);
  // Until the session probe answers, "logged out" is a guess — and on #/admin
  // acting on that guess flashes a sign-in prompt at someone already signed in.
  let sessionChecked = $state(false);
  let adminView = $state<'none' | 'login'>('none');

  // Tracks whether we pushed our own entry, so the in-app Back button never
  // walks off the site when someone deep-linked straight to a subpage.
  let pushedNav = $state(false);

  function openDetail(checkId: string) {
    history.pushState(null, '', `#/check/${encodeURIComponent(checkId)}`);
    pushedNav = true;
    view = { name: 'detail', checkId };
  }

  function openAdmin(panel: AdminPanel = ADMIN_PANELS[0].id) {
    history.pushState(null, '', `#/admin/${panel}`);
    pushedNav = true;
    view = { name: 'admin', panel };
  }

  /** Opens the check editor page. `null` is a check that does not exist yet. */
  function openCheckEditor(name: string | null) {
    history.pushState(null, '', `#/admin/checks/${name === null ? 'new' : encodeURIComponent(name)}`);
    pushedNav = true;
    view = { name: 'admin', panel: 'checks', edit: { name } };
  }

  function goHome() {
    history.replaceState(null, '', '#/');
    pushedNav = false;
    view = { name: 'home' };
  }

  /** Switches admin area and records it in the URL, so reload and back/forward
   *  land on the same panel. */
  function selectPanel(panel: AdminPanel) {
    // The editor is not a panel, so clicking the panel it belongs to has to
    // leave it — without `!view.edit` "Checks" reads as a dead nav item.
    if (view.name === 'admin' && view.panel === panel && !view.edit) return;
    openAdmin(panel);
  }

  function goBack() {
    if (pushedNav) {
      history.back();
      return;
    }
    goHome();
  }

  function signOut() {
    authenticated = false;
    adminView = 'none';
    logout();
    // Staying on an admin URL we can no longer serve would just show the
    // sign-in prompt to the person who just left.
    if (view.name === 'admin') goHome();
  }

  /** An admin call 401'd: the cookie died mid-session. */
  function onExpired() {
    authenticated = false;
    adminView = 'login';
  }

  /** Acknowledge an open incident from the admin Incidents panel. */
  async function acknowledge(incident: Incident) {
    try {
      const updated = await acknowledgeIncident(incident.id);
      incidents = incidents.map((i) => (i.id === updated.id ? updated : i));
    } catch (e) {
      console.error('acknowledge failed', e);
    }
  }

  // Shared window for both the uptime strip and the latency graph, so one
  // control drives everything on the row. The strip always renders the same
  // number of segments, so a shorter window means finer resolution, not fewer
  // bars. 30 days is the default: meaningful history without a fresh install
  // being mostly empty "no data" segments.
  const RANGES = [
    { label: '24h', days: 1 },
    { label: '7d', days: 7 },
    { label: '30d', days: 30 },
    { label: '90d', days: 90 },
  ] as const;
  const STRIP_SEGMENTS = 90;
  let days = $state(30);

  // Only the per-check charts depend on the window, so refetch those rather than
  // reloading status, alerts and agents too.
  async function setDays(next: number) {
    if (next === days) return;
    days = next;
    const [strips, lines] = await Promise.all([
      Promise.all(checks.map((c) => fetchUptime(c.check_id, next, STRIP_SEGMENTS))),
      Promise.all(
        checks
          .filter((c) => c.latency_graph)
          .map((c) => fetchLatencySparkline(c.check_id, next * 24)),
      ),
    ]);
    uptime = Object.fromEntries(strips.map((u) => [u.check_id, u]));
    sparkline = Object.fromEntries(lines.map((l) => [l.check_id, l]));
  }

  // The order an admin arranged, not alphabetical. Name breaks ties so the list
  // stays stable for checks that have never been explicitly ordered.
  const ordered = $derived(
    [...checks].sort(
      (a, b) => (a.position ?? 0) - (b.position ?? 0) || a.check_id.localeCompare(b.check_id),
    ),
  );

  // Grouping comes from the `group` field each check carries from the TOML
  // config, so the page always reflects what is actually configured. Group order
  // follows first appearance in the sorted list.
  const monitorGroups = $derived.by(() => {
    const groups = new Map<string, Check[]>();
    for (const c of ordered) {
      if (!c.group) continue;
      const existing = groups.get(c.group);
      if (existing) existing.push(c);
      else groups.set(c.group, [c]);
    }
    // A group with no matching section row — a check pointing at a name the
    // sections table never had — renders expanded rather than vanishing.
    return [...groups].map(([name, checks]) => ({
      name,
      checks,
      collapsed: sections.find((s) => s.name === name)?.collapsed ?? false,
    }));
  });

  // Everything without a group is listed loose, so a check can never be hidden
  // by a config typo.
  const generalChecks = $derived(ordered.filter((c) => !c.group));

  async function load() {
    loading = true;
    error = null;
    try {
      const [s, a, inc, ag, sec] = await Promise.all([
        fetchStatus(),
        fetchAlerts(),
        fetchIncidents(),
        fetchAgents(),
        fetchPublicSections(),
      ]);
      checks = s.checks;
      alerts = a.alerts;
      incidents = inc.incidents;
      agents = ag.agents;
      sections = sec;
      // Both per-check charts use the selected window; the latency graph is only
      // fetched for checks that opted in, so we don't pay for graphs nobody
      // renders.
      const strips = await Promise.all(
        s.checks.map((c) => fetchUptime(c.check_id, days, STRIP_SEGMENTS)),
      );
      uptime = Object.fromEntries(strips.map((u) => [u.check_id, u]));
      const lines = await Promise.all(
        s.checks
          .filter((c) => c.latency_graph)
          .map((c) => fetchLatencySparkline(c.check_id, days * 24)),
      );
      sparkline = Object.fromEntries(lines.map((l) => [l.check_id, l]));
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }

  // The session can die while the tab sits open. Panels that read public
  // endpoints (agents, alerts, incidents) or render static copy never get a
  // 401 to learn from, so `authenticated` would stay true from page load and
  // keep serving admin screens to an expired session. Re-verify on entering
  // admin and on every panel switch rather than inferring it from failures.
  $effect(() => {
    if (view.name !== 'admin') return;
    void view.panel;
    fetchSession()
      .then((ok) => {
        authenticated = ok;
        if (!ok) adminView = 'login';
      })
      .finally(() => (sessionChecked = true));
  });

  onMount(() => {
    load();
    // Cookie-only: the server is the sole authority on whether we're signed in.
    fetchSession()
      .then((ok) => (authenticated = ok))
      .finally(() => (sessionChecked = true));
    const conn = connectWs({
      onStatus: (s) => (wsStatus = s),
      onStateChange: (msg) => {
        checks = checks.map((c) =>
          c.check_id === msg.check_id ? { ...c, status: msg.to, last_check: msg.timestamp } : c,
        );
        alerts = [
          { check_id: msg.check_id, from: msg.from, to: msg.to, timestamp: msg.timestamp },
          ...alerts,
        ];
      },
      onIncident: (msg) => {
        if (msg.type === 'incident_opened') {
          incidents = [msg.incident, ...incidents];
        } else {
          incidents = incidents.map((i) => (i.id === msg.incident.id ? msg.incident : i));
        }
      },
    });
    // Back/forward (including the mouse's back button) drives the view.
    const onPop = () => {
      view = viewFromHash();
      if (view.name === 'home') pushedNav = false;
      dayModal = null;
    };
    window.addEventListener('popstate', onPop);
    return () => {
      conn.close();
      window.removeEventListener('popstate', onPop);
    };
  });
</script>

<div class="app" class:wide={view.name === 'admin'}>
  <header class="bar">
    <div class="brand">
      <h1>Warden</h1>
      <p class="subtitle">Service Status</p>
    </div>
    <div class="bar-right">
      <span class="ws-dot {wsStatus}" title="live updates: {wsStatus}">
        <span class="dot"></span>{wsStatus}
      </span>
      <div class="range-switch" role="group" aria-label="admin">
        <button
          class="range-btn"
          class:active={view.name === 'admin'}
          onclick={() => (authenticated ? openAdmin() : (adminView = 'login'))}
        >
          Admin
        </button>
        {#if authenticated}
          <button class="range-btn" onclick={signOut}>Log out</button>
        {/if}
      </div>
    </div>
  </header>

  {#if view.name === 'admin'}
    {@const panel = view.panel}
    {@const editing = view.edit ?? null}
    <div class="admin-head">
      <h2>Admin</h2>
    </div>
    <div class="admin-shell">
      <nav class="admin-nav" aria-label="admin sections">
        <!-- Deep-linking straight to #/admin leaves nothing to pop, so this
             sets the view outright instead of walking history. -->
        <button class="admin-exit" onclick={goHome}>← Status page</button>
        {#if sessionChecked && authenticated}
          <div class="admin-nav-items">
            {#each ADMIN_PANELS as p (p.id)}
              <button
                class="admin-nav-item"
                class:active={panel === p.id}
                aria-current={panel === p.id ? 'page' : undefined}
                onclick={() => selectPanel(p.id)}>{p.label}</button
              >
            {/each}
          </div>
        {/if}
      </nav>
      <div class="admin-content">
        {#if !sessionChecked}
          <div class="notice">Checking session…</div>
        {:else if !authenticated}
          <div class="admin-panel">
            <div class="panel-title">Sign in required</div>
            <p class="admin-muted">Managing checks and the status page needs an admin session.</p>
            <div class="range-switch admin-signin">
              <button class="range-btn" onclick={() => (adminView = 'login')}>Sign in</button>
            </div>
          </div>
        {:else if panel === 'checks'}
          {#if editing}
            <!-- Keyed so back/forward between two checks rebuilds the editor
                 instead of leaving one check's draft under another's name. -->
            {#key editing.name}
              <CheckEditor
                name={editing.name}
                onsaved={load}
                onclose={() => openAdmin('checks')}
              />
            {/key}
          {:else}
            <ChecksAdmin onopen={openCheckEditor} onexpired={onExpired} />
          {/if}
        {:else if panel === 'status'}
          <SectionsAdmin onchanged={load} onexpired={onExpired} />
        {:else if panel === 'agents'}
          <AgentsAdmin onopen={openCheckEditor} onexpired={onExpired} />
        {:else if panel === 'alerts'}
          <div class="panel-title">Alerts</div>
          <AlertsList {alerts} />
        {:else if panel === 'incidents'}
          <div class="panel-title">Incidents</div>
          <IncidentsList {incidents} onAcknowledge={acknowledge} />
        {:else}
          <AdminPlaceholder
            title="Maintenance"
            note="Not built yet. There is no way to schedule a maintenance window or suppress alerts for one; disabling a check on the Checks panel is the only way to stop probing."
          />
        {/if}
      </div>
    </div>
  {:else if loading}
    <div class="notice">Loading…</div>
  {:else if error}
    <div class="notice error">
      Failed to load data: {error}
      <div style="margin-top:0.5rem"><button class="link" onclick={load}>Retry</button></div>
    </div>
  {:else if view.name === 'detail'}
    <!-- Bound once: the narrowing on `view` doesn't survive into the callback below. -->
    {@const id = view.checkId}
    <CheckDetail
      checkId={id}
      check={checks.find((c) => c.check_id === id)}
      onback={goBack}
    />
  {:else}
    <OverallBanner {checks} />

    <section class="section">
      <div class="section-bar">
        <h2 class="section-title">Monitors</h2>
        <div class="range-switch" role="group" aria-label="chart window">
          {#each RANGES as r (r.days)}
            <button
              class="range-btn"
              class:active={days === r.days}
              onclick={() => setDays(r.days)}
            >
              {r.label}
            </button>
          {/each}
        </div>
      </div>
      <div class="components">
        {#each generalChecks as c (c.check_id)}
          <ComponentRow
            check={c}
            uptime={uptime[c.check_id]}
            sparkline={sparkline[c.check_id]?.points ?? null}
            onselect={openDetail}
            onbarclick={(id, b) => (dayModal = { checkId: id, bucket: b })}
          />
        {/each}

        {#each monitorGroups as g (g.name)}
          <CollapsibleGroup
            name={g.name}
            status={worstStatus(g.checks)}
            count={g.checks.length}
            collapsed={g.collapsed}
          >
            {#each g.checks as c (c.check_id)}
              <ComponentRow
                check={c}
                uptime={uptime[c.check_id]}
                sparkline={sparkline[c.check_id]?.points ?? null}
                onselect={openDetail}
                onbarclick={(id, b) => (dayModal = { checkId: id, bucket: b })}
              />
            {/each}
          </CollapsibleGroup>
        {/each}
      </div>
    </section>

    {#if agents.length}
      <section class="section">
        <h2 class="section-title">Monitoring Agents</h2>
        <div class="components">
          {#each agents as a (a.agent_id)}
            <ServerRow
              name={a.agent_id}
              status={a.status}
              detail="{a.checks_reporting} {a.checks_reporting === 1 ? 'check' : 'checks'} · {a.results_24h} results/24h"
            />
          {/each}
        </div>
      </section>
    {/if}

    <section class="section">
      <h2 class="section-title">Past Incidents</h2>
      <IncidentsList {incidents} />
    </section>
  {/if}
</div>

{#if dayModal}
  <DayDetailModal
    checkId={dayModal.checkId}
    kind={checks.find((c) => c.check_id === dayModal?.checkId)?.type}
    bucket={dayModal.bucket}
    onclose={() => (dayModal = null)}
  />
{/if}

{#if adminView === 'login'}
  <LoginModal
    onsuccess={() => {
      authenticated = true;
      adminView = 'none';
      // Already on #/admin (arrived by reload or an expired session), so don't
      // stack a second identical history entry.
      if (view.name !== 'admin') openAdmin();
    }}
    oncancel={() => (adminView = 'none')}
  />
{/if}
