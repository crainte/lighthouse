<script lang="ts">
  import { onMount } from 'svelte';
  import {
    fetchStatus,
    fetchAlerts,
    fetchIncidents,
    addIncidentUpdate,
    fetchUptime,
    fetchLatencySparkline,
    fetchPublicSections,
  } from './lib/api';
  import type {
    Check,
    Alert,
    Incident,
    FleetRow,
    MaintenanceNotice,
    UptimeResponse,
    UptimeBucket,
    LatencySparklineResponse,
    PublicSection,
  } from './lib/api';
  import { connectWs } from './lib/ws';
  import type { WsStatus } from './lib/ws';
  import { applyStateChange, prependAlert, applyIncidentEvent } from './lib/live';
  import { formatDuration, worstStatus } from './lib/format';
  import {
    formatAbsolute,
    openGlobalIncidents,
    openIncidentForCheck,
    stateTone,
  } from './lib/incident';
  import OverallBanner from './components/OverallBanner.svelte';
  import ComponentRow from './components/ComponentRow.svelte';
  import AlertsList from './components/AlertsList.svelte';
  import IncidentsList from './components/IncidentsList.svelte';
  import PastIncidents from './components/PastIncidents.svelte';
  import IncidentEditor from './components/IncidentEditor.svelte';
  import IncidentPage from './components/IncidentPage.svelte';
  import CheckDetail from './components/CheckDetail.svelte';
  import DayDetailModal from './components/DayDetailModal.svelte';
  import CollapsibleGroup from './components/CollapsibleGroup.svelte';
  import ServerRow from './components/ServerRow.svelte';
  import LoginModal from './components/LoginModal.svelte';
  import ChecksAdmin from './components/ChecksAdmin.svelte';
  import CheckEditor from './components/CheckEditor.svelte';
  import SectionsAdmin from './components/SectionsAdmin.svelte';
  import AgentsAdmin from './components/AgentsAdmin.svelte';
  import AgentDisplayAdmin from './components/AgentDisplayAdmin.svelte';
  import NotifiersAdmin from './components/NotifiersAdmin.svelte';
  import AgentCredentials from './components/AgentCredentials.svelte';
  import ProbeResultsAdmin from './components/ProbeResultsAdmin.svelte';
  import MaintenanceAdmin from './components/MaintenanceAdmin.svelte';
  import ClusterAdmin from './components/ClusterAdmin.svelte';
  import { fetchSession, logout } from './lib/admin';

  // One array drives the sidebar and the content switch, so a new admin area is
  // a single line here plus its panel. `group` buckets them under the sidebar's
  // section headings; `title`/`blurb` are the page head, so a panel never has to
  // render its own.
  const ADMIN_PANELS = [
    {
      id: 'checks',
      label: 'Checks',
      group: 'Monitor',
      title: 'Checks',
      blurb: 'What gets probed, how often, and from where.',
    },
    {
      id: 'alerts',
      label: 'Alerts',
      group: 'Monitor',
      title: 'Alerts',
      blurb: 'Every confirmed state transition, newest first.',
    },
    {
      id: 'incidents',
      label: 'Incidents',
      group: 'Monitor',
      title: 'Incidents',
      blurb: 'Outages as the status page tells them.',
    },
    {
      id: 'agents',
      label: 'Agents',
      group: 'Fleet',
      title: 'Agents',
      blurb: 'The probe fleet and what each machine is running.',
    },
    {
      id: 'credentials',
      label: 'Credentials',
      group: 'Fleet',
      title: 'Agent credentials',
      blurb: 'One per zone. The secret is shown once, at mint.',
    },
    {
      id: 'results',
      label: 'Probe results',
      group: 'Fleet',
      title: 'Probe results',
      blurb: 'Raw results agents reported — filter by check, agent, status, or location.',
    },
    {
      id: 'cluster',
      label: 'Cluster',
      group: 'Fleet',
      title: 'Cluster',
      blurb: 'The coordinators gossiping with each other, and what each one currently believes.',
    },
    {
      id: 'status',
      label: 'Status page',
      group: 'Platform',
      title: 'Status page',
      blurb: 'Sections, ordering, and how much of the fleet visitors see.',
    },
    {
      id: 'notifications',
      label: 'Notifications',
      group: 'Platform',
      title: 'Notifications',
      blurb: 'Where alerts go, and which checks route to whom.',
    },
    {
      id: 'maintenance',
      label: 'Maintenance',
      group: 'Platform',
      title: 'Maintenance',
      blurb: 'Scheduled windows and alert suppression.',
    },
  ] as const;
  type AdminPanel = (typeof ADMIN_PANELS)[number]['id'];

  /** Sidebar groups, in declaration order, so adding a panel needs no second list. */
  const NAV_GROUPS = [...new Set(ADMIN_PANELS.map((p) => p.group))];

  type View =
    | { name: 'home' }
    | { name: 'detail'; checkId: string }
    // The public permalink for one incident, which is what the status page and
    // any notification link to.
    | { name: 'incident'; incidentId: string }
    // `edit` present = the check editor page rather than the panel's own
    // content; `edit.name === null` is a check that does not exist yet.
    // `incident` present = the incident editor, open on that id (or `null` for
    // one being created).
    | {
        name: 'admin';
        panel: AdminPanel;
        edit?: { name: string | null };
        incident?: { id: string | null };
      };
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
    // #/admin/incidents/<id> — the editor for one incident, so the Alerts feed
    // can link straight to the thing an operator would act on, and the URL
    // survives a refresh. "new" is reserved the same way as for checks.
    const incEditor = location.hash.match(/^#\/admin\/incidents\/(.+?)\/?$/);
    if (incEditor) {
      const raw = decodeURIComponent(incEditor[1]);
      return {
        name: 'admin',
        panel: 'incidents',
        incident: { id: raw === 'new' ? null : raw },
      };
    }
    const admin = location.hash.match(/^#\/admin(?:\/([^/]*))?\/?$/);
    if (admin) {
      // An unknown or absent id lands on the first panel rather than rendering
      // an empty content area.
      const panel = ADMIN_PANELS.find((p) => p.id === admin[1])?.id ?? ADMIN_PANELS[0].id;
      return { name: 'admin', panel };
    }
    const inc = location.hash.match(/^#\/incident\/(.+)$/);
    if (inc) return { name: 'incident', incidentId: decodeURIComponent(inc[1]) };
    const m = location.hash.match(/^#\/check\/(.+)$/);
    return m ? { name: 'detail', checkId: decodeURIComponent(m[1]) } : { name: 'home' };
  }

  let checks = $state<Check[]>([]);
  let alerts = $state<Alert[]>([]);
  let incidents = $state<Incident[]>([]);
  // Fleet liveness as the coordinator chose to publish it: regions, or pinned
  // agents. Deliberately not `/api/v1/agents` — that needs a session, and this
  // page is anonymous, so fetching it here failed the whole load with a 401.
  let fleet = $state<FleetRow[]>([]);
  // Announced maintenance running right now. Public, and deliberately separate
  // from incidents: this one was planned.
  let maintenance = $state<MaintenanceNotice[]>([]);
  // Only carries the headings' metadata — /api/v1/status has none, and the
  // collapsed flag decides how a group first renders.
  let sections = $state<PublicSection[]>([]);
  let uptime = $state<Record<string, UptimeResponse>>({});
  let sparkline = $state<Record<string, LatencySparklineResponse>>({});
  let loading = $state(true);
  let error = $state<string | null>(null);
  let wsStatus = $state<WsStatus>('connecting');
  // Bumped to tell an open CheckDetail page to refetch its own uptime, latency,
  // history and incidents — on a live state change for the check it's showing,
  // or on any websocket reconnect. It reads public endpoints that never emit a
  // 401, so without this nudge a detail page left open goes stale.
  let detailRefresh = $state(0);
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

  function openIncident(incidentId: string) {
    history.pushState(null, '', `#/incident/${encodeURIComponent(incidentId)}`);
    pushedNav = true;
    view = { name: 'incident', incidentId };
  }

  /** Opens the admin incident editor. `null` is an incident being created.
   *
   *  A route rather than panel state, so the Alerts feed can link an operator
   *  straight to the thing they would act on, and a refresh stays put. */
  function openIncidentEditor(id: string | null) {
    history.pushState(
      null,
      '',
      `#/admin/incidents/${id === null ? 'new' : encodeURIComponent(id)}`,
    );
    pushedNav = true;
    view = { name: 'admin', panel: 'incidents', incident: { id } };
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

  /** Acknowledge from the admin Incidents list. There is no acknowledge
   *  endpoint: an authored update on a `detected` incident IS the ack, so the
   *  button posts an `investigating` update with an empty body. */
  async function acknowledge(incident: Incident) {
    try {
      const updated = await addIncidentUpdate(incident.id, 'investigating', '');
      incidents = incidents.map((i) => (i.id === updated.id ? updated : i));
    } catch (e) {
      console.error('acknowledge failed', e);
    }
  }

  /** The editor writes through the API and hands back the saved incident, so
   *  the list updates without a reload. A create lands at the front, matching
   *  the newest-first order the server serves.
   *
   *  The editor deliberately stays open: posting an update is a normal thing to
   *  do several times in a row, and closing it under the operator each time
   *  would make the timeline unusable. A create swaps the URL over to the
   *  incident it just made, so Affects and Updates become available and the
   *  page can be reloaded or linked. */
  function onIncidentSaved(saved: Incident) {
    incidents = incidents.some((i) => i.id === saved.id)
      ? incidents.map((i) => (i.id === saved.id ? saved : i))
      : [saved, ...incidents];
    if (view.name === 'admin' && view.incident?.id !== saved.id) {
      history.replaceState(null, '', `#/admin/incidents/${encodeURIComponent(saved.id)}`);
      view = { name: 'admin', panel: 'incidents', incident: { id: saved.id } };
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

  // Refetch one check's uptime strip and (if it opted in) its latency graph,
  // replacing just that check's entry. Used after a live state change so the
  // percentage and charts converge on the server's numbers rather than staying
  // frozen at page-load values.
  async function refreshCheckCharts(id: string) {
    const c = checks.find((ch) => ch.check_id === id);
    if (!c) return;
    try {
      const [u, line] = await Promise.all([
        fetchUptime(id, days, STRIP_SEGMENTS),
        c.latency_graph ? fetchLatencySparkline(id, days * 24) : Promise.resolve(null),
      ]);
      uptime = { ...uptime, [id]: u };
      if (line) sparkline = { ...sparkline, [id]: line };
    } catch {
      // A failed refresh leaves the last-known values in place; the next event
      // or a reconnect resync will try again.
    }
  }

  // Authoritative status refresh: the WS state_change payload only carries the
  // new status and timestamp, so latency, status code, error, uptime_24h and
  // fleet health are only correct after re-reading /status. Debounced so a batch
  // of transitions costs one request, not one per check.
  let statusTimer: ReturnType<typeof setTimeout> | null = null;
  function refreshStatusSoon() {
    if (statusTimer) clearTimeout(statusTimer);
    statusTimer = setTimeout(async () => {
      statusTimer = null;
      try {
        const s = await fetchStatus();
        checks = s.checks;
        fleet = s.agents;
        maintenance = s.maintenance;
      } catch {
        // Keep the optimistic in-memory state; a later event or reconnect retries.
      }
    }, 600);
  }

  // Per-check chart refresh, coalesced: several transitions on the same check in
  // quick succession trigger one refetch each after things settle.
  const chartTimers = new Map<string, ReturnType<typeof setTimeout>>();
  function refreshCheckChartsSoon(id: string) {
    const existing = chartTimers.get(id);
    if (existing) clearTimeout(existing);
    chartTimers.set(
      id,
      setTimeout(() => {
        chartTimers.delete(id);
        void refreshCheckCharts(id);
      }, 800),
    );
  }

  // Full resync after a websocket reconnect. The server lets slow clients lag,
  // so a client that dropped may have missed events entirely; re-reading status,
  // alerts, incidents and every visible chart is the only way to guarantee the
  // page isn't left stale. Reuses the initial load path without the spinner.
  async function resync() {
    try {
      const [s, a, inc] = await Promise.all([fetchStatus(), fetchAlerts(), fetchIncidents()]);
      checks = s.checks;
      alerts = a.alerts;
      incidents = inc.incidents;
      fleet = s.agents;
      maintenance = s.maintenance;
      const strips = await Promise.all(
        s.checks.map((c) => fetchUptime(c.check_id, days, STRIP_SEGMENTS)),
      );
      uptime = Object.fromEntries(strips.map((u) => [u.check_id, u]));
      const lines = await Promise.all(
        s.checks.filter((c) => c.latency_graph).map((c) => fetchLatencySparkline(c.check_id, days * 24)),
      );
      sparkline = Object.fromEntries(lines.map((l) => [l.check_id, l]));
    } catch {
      // A failed resync leaves the last-known state; the next reconnect retries.
    }
  }

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

  // The page-wide live surface. Fed by the same `incidents` array the websocket
  // keeps current, so one opening or resolving lands here without a reload.
  const openGlobals = $derived(openGlobalIncidents(incidents));

  async function load() {
    loading = true;
    error = null;
    try {
      const [s, a, inc, sec] = await Promise.all([
        fetchStatus(),
        fetchAlerts(),
        fetchIncidents(),
        fetchPublicSections(),
      ]);
      checks = s.checks;
      alerts = a.alerts;
      incidents = inc.incidents;
      fleet = s.agents;
      maintenance = s.maintenance;
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
        // Optimistic: flip the badge (and the "All Systems Operational" banner,
        // which is derived from `checks`) and prepend the alert immediately for
        // instant feedback. Reducers are pure and unit-tested in live.test.ts.
        checks = applyStateChange(checks, msg);
        alerts = prependAlert(alerts, msg);
        // Then reconcile against the server: the event carries no latency,
        // status code, error, uptime_24h or fleet health, and the charts are
        // frozen at page-load values until refetched.
        refreshStatusSoon();
        refreshCheckChartsSoon(msg.check_id);
        // If the detail page for this check is open, nudge it to refetch too.
        if (view.name === 'detail' && view.checkId === msg.check_id) detailRefresh += 1;
      },
      onIncident: (msg) => {
        incidents = applyIncidentEvent(incidents, msg);
      },
      // A reconnect may have skipped events while the socket was down, so pull
      // status, alerts, incidents and every chart back into sync — including an
      // open detail page, which fetches its own data.
      onReconnect: () => {
        void resync();
        detailRefresh += 1;
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
      if (statusTimer) clearTimeout(statusTimer);
      for (const t of chartTimers.values()) clearTimeout(t);
      chartTimers.clear();
    };
  });
</script>

<div class="app" class:wide={view.name === 'admin'} class:admin-app={view.name === 'admin'}>
  <!-- The console owns the whole viewport, brand included, so the status page
       header is suppressed there rather than stacked above a second brand. -->
  {#if view.name !== 'admin'}
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
            onclick={() => (authenticated ? openAdmin() : (adminView = 'login'))}
          >
            Admin
          </button>
        </div>
      </div>
    </header>
  {/if}

  {#if view.name === 'admin'}
    {@const panel = view.panel}
    {@const editing = view.edit ?? null}
    {@const incEditing = view.incident ?? null}
    {@const meta = ADMIN_PANELS.find((p) => p.id === panel)}
    {@const openCount = incidents.filter((i) => i.state !== 'resolved').length}
    {@const unacked = incidents.filter((i) => i.state === 'detected').length}
    <div class="admin-shell">
      <nav class="admin-nav" aria-label="admin sections">
        <div class="nav-brand">
          <span class="brand-dot"></span>
          <span class="brand-name">Warden</span>
          <span class="brand-tag">admin</span>
        </div>
        {#if sessionChecked && authenticated}
          {#each NAV_GROUPS as g (g)}
            <div class="nav-group">{g}</div>
            <div class="admin-nav-items">
              {#each ADMIN_PANELS.filter((p) => p.group === g) as p (p.id)}
                <button
                  class="admin-nav-item"
                  class:active={panel === p.id}
                  aria-current={panel === p.id ? 'page' : undefined}
                  onclick={() => selectPanel(p.id)}
                >
                  {p.label}
                  <!-- Only counts worth acting on get a badge: unacknowledged
                       outages, not every row the panel can list. -->
                  {#if p.id === 'incidents' && openCount > 0}
                    <span class="nav-badge" class:quiet={unacked === 0}>{openCount}</span>
                  {/if}
                </button>
              {/each}
            </div>
          {/each}
        {/if}
        <div class="nav-foot">
          <span class="nav-status {wsStatus}" title="live updates: {wsStatus}">
            <span class="dot"></span>{wsStatus === 'open' ? 'live' : wsStatus}
          </span>
          <!-- Deep-linking straight to #/admin leaves nothing to pop, so this
               sets the view outright instead of walking history. -->
          <button class="admin-nav-item" onclick={goHome}>← Status page</button>
          {#if authenticated}
            <button class="admin-nav-item" onclick={signOut}>Log out</button>
          {/if}
        </div>
      </nav>
      <div class="admin-content">
        {#if !sessionChecked}
          <div class="notice">Checking session…</div>
        {:else if !authenticated}
          <div class="page-head"><h2>Admin</h2></div>
          <div class="admin-panel">
            <div class="panel-title">Sign in required</div>
            <p class="admin-muted">Managing checks and the status page needs an admin session.</p>
            <div class="range-switch admin-signin">
              <button class="btn primary" onclick={() => (adminView = 'login')}>Sign in</button>
            </div>
          </div>
        {:else if panel === 'checks' && editing}
          <!-- Keyed so back/forward between two checks rebuilds the editor
               instead of leaving one check's draft under another's name. -->
          {#key editing.name}
            <CheckEditor name={editing.name} onsaved={load} onclose={() => openAdmin('checks')} />
          {/key}
        {:else if panel === 'incidents' && incEditing}
          {#key incEditing.id}
            <IncidentEditor
              incidentId={incEditing.id}
              checks={checks.map((c) => c.check_id)}
              onClose={() => openAdmin('incidents')}
              onSaved={onIncidentSaved}
            />
          {/key}
        {:else}
          <div class="page-head page-head-row">
            <div>
              <h2>{meta?.title ?? 'Admin'}</h2>
              <p class="page-meta">{meta?.blurb ?? ''}</p>
            </div>
            {#if panel === 'incidents'}
              <button class="btn primary" onclick={() => openIncidentEditor(null)}>
                New incident
              </button>
            {/if}
          </div>
          {#if panel === 'checks'}
            <ChecksAdmin onopen={openCheckEditor} onexpired={onExpired} />
          {:else if panel === 'status'}
            <SectionsAdmin onchanged={load} onexpired={onExpired} />
            <AgentDisplayAdmin onchanged={load} onexpired={onExpired} />
          {:else if panel === 'agents'}
            <AgentsAdmin onopen={openCheckEditor} onexpired={onExpired} />
          {:else if panel === 'credentials'}
            <AgentCredentials onexpired={onExpired} />
          {:else if panel === 'results'}
            <ProbeResultsAdmin onopen={openCheckEditor} onexpired={onExpired} />
          {:else if panel === 'cluster'}
            <ClusterAdmin onexpired={onExpired} />
          {:else if panel === 'alerts'}
            <AlertsList {alerts} onOpenIncident={openIncidentEditor} />
          {:else if panel === 'incidents'}
            <IncidentsList
              {incidents}
              onEdit={(i) => openIncidentEditor(i.id)}
              onAck={acknowledge}
            />
          {:else if panel === 'notifications'}
            <NotifiersAdmin onexpired={onExpired} />
          {:else}
            <MaintenanceAdmin onexpired={onExpired} />
          {/if}
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
  {:else if view.name === 'incident'}
    {@const id = view.incidentId}
    <IncidentPage incidentId={id} onBack={goBack} />
  {:else if view.name === 'detail'}
    <!-- Bound once: the narrowing on `view` doesn't survive into the callback below. -->
    {@const id = view.checkId}
    <CheckDetail
      checkId={id}
      check={checks.find((c) => c.check_id === id)}
      refreshKey={detailRefresh}
      onback={goBack}
    />
  {:else}
    <OverallBanner {checks} />

    {#if maintenance.length}
      <div class="mbanners">
        {#each maintenance as m (m.title + m.ends_at)}
          <div class="mbanner">
            <span class="mpill">maintenance</span>
            <span class="mb-title">{m.title}</span>
            <span class="mb-meta">until {formatAbsolute(m.ends_at)}</span>
          </div>
        {/each}
      </div>
    {/if}

    {#if openGlobals.length}
      <div class="gbanners">
        {#each openGlobals as i (i.id)}
          <button class="gbanner" onclick={() => openIncident(i.id)}>
            <span class="opill {stateTone(i.state)}">{i.state}</span>
            <span class="gb-title">{i.title}</span>
            <span class="gb-meta">{formatDuration(i.duration_ms)} · read updates →</span>
          </button>
        {/each}
      </div>
    {/if}

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
            incident={openIncidentForCheck(incidents, c.check_id)}
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
                incident={openIncidentForCheck(incidents, c.check_id)}
                onselect={openDetail}
                onbarclick={(id, b) => (dayModal = { checkId: id, bucket: b })}
              />
            {/each}
          </CollapsibleGroup>
        {/each}
      </div>
    </section>

    {#if fleet.length}
      <section class="section">
        <h2 class="section-title">Monitoring Agents</h2>
        <div class="components">
          {#each fleet as row (row.kind + ':' + row.name)}
            <ServerRow
              name={row.name}
              status={row.status}
              detail={row.kind === 'region'
                ? `${row.healthy}/${row.total} ${row.total === 1 ? 'agent' : 'agents'} healthy`
                : undefined}
            />
          {/each}
        </div>
      </section>
    {/if}

    <section class="section">
      <h2 class="section-title">Past Incidents</h2>
      <PastIncidents {incidents} onOpen={openIncident} />
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
