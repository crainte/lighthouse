<script lang="ts">
  import { onMount } from 'svelte';
  import {
    fetchStatus,
    fetchAlerts,
    fetchIncidents,
    addIncidentUpdate,
    fetchUptime,
    fetchLatencySparkline,
    fetchUptimeStrips,
    fetchLatencyPoints,
    fetchPublicSections,
  } from './lib/api';
  import type {
    Check,
    Alert,
    Incident,
    IncidentGroup,
    FleetRow,
    MaintenanceNotice,
    UptimeResponse,
    UptimeBucket,
    LatencyPoint,
    PublicSection,
    SectionRollup,
  } from './lib/api';
  import { connectWs } from './lib/ws';
  import type { WsStatus } from './lib/ws';
  import { applyStateChange, prependAlert, applyIncidentEvent } from './lib/live';
  import { worstStatus } from './lib/format';
  import type { View, FleetTab } from './lib/router';
  import {
    viewFromHash,
    hashForHome,
    hashForCheck,
    hashForIncident,
    hashForGroup,
    hashForAdmin,
    hashForFleet,
    hashForCheckEditor,
    hashForIncidentEditor,
  } from './lib/router';
  import { createSessionChecker } from './lib/session';
  import {
    DEFAULT_SEASONAL_PREFERENCES,
    SEASONAL_STORAGE_KEY,
    loadSeasonalPreferences,
    resolveSeason,
    saveSeasonalPreferences,
    type SeasonalPreferences,
  } from './lib/seasonalAtmosphere';
  import { debounce, debounceByKey } from './lib/debounce';
  import {
    formatAbsolute,
    openIncidentForCheck,
  } from './lib/incident';
  import NightwatchOverview from './components/NightwatchOverview.svelte';
  import ConsoleIcon from './components/ConsoleIcon.svelte';
  import ConsoleShell from './components/ConsoleShell.svelte';
  import PublicStatusHero from './components/PublicStatusHero.svelte';
  import publicLighthouseScene from './assets/lighthouse-fall.png';
  import PublicIncidentSpotlight from './components/PublicIncidentSpotlight.svelte';
  import './public-status.css';
  import ComponentRow from './components/ComponentRow.svelte';
  import AlertsList from './components/AlertsList.svelte';
  import IncidentsList from './components/IncidentsList.svelte';
  import PastIncidents from './components/PastIncidents.svelte';
  import IncidentEditor from './components/IncidentEditor.svelte';
  import IncidentPage from './components/IncidentPage.svelte';
  import CheckDetail from './components/CheckDetail.svelte';
  import DayDetailModal from './components/DayDetailModal.svelte';
  import CollapsibleGroup from './components/CollapsibleGroup.svelte';
  import SectionTile from './components/SectionTile.svelte';
  import ServerRow from './components/ServerRow.svelte';
  import LoginModal from './components/LoginModal.svelte';
  import ChecksAdmin from './components/ChecksAdmin.svelte';
  import CheckEditor from './components/CheckEditor.svelte';
  import SectionsAdmin from './components/SectionsAdmin.svelte';
  import FleetAdmin from './components/FleetAdmin.svelte';
  import AgentDisplayAdmin from './components/AgentDisplayAdmin.svelte';
  import NotifiersAdmin from './components/NotifiersAdmin.svelte';
  import ProbeResultsAdmin from './components/ProbeResultsAdmin.svelte';
  import MaintenanceAdmin from './components/MaintenanceAdmin.svelte';
  import RegionTreeAdmin from './components/RegionTreeAdmin.svelte';
  import DataAdmin from './components/DataAdmin.svelte';
  import { fetchSession, logout, type NodeMode } from './lib/admin';

  // One array drives the sidebar and the content switch. Admin page standard:
  // the shell owns location, App owns the page title/purpose/primary action, and
  // panels begin with content or section headings that never repeat the title.
  const ADMIN_PANELS = [
    { id: 'overview', label: 'Overview', group: '', title: 'Overview', blurb: 'Every signal, in sight.' },
    {
      id: 'checks',
      label: 'Checks',
      group: 'Monitor',
      title: 'Checks',
      blurb: 'Monitor definitions, probe schedules, placement, and public visibility.',
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
      blurb: 'Active and historical outages, acknowledgements, and public updates.',
    },
    {
      id: 'fleet',
      label: 'Fleet',
      group: 'Fleet',
      title: 'Fleet',
      blurb: 'Agents, regions, zones, credentials, and connectivity.',
    },
    {
      id: 'results',
      label: 'Probe results',
      group: 'Fleet',
      title: 'Probe results',
      blurb: 'Individual probe attempts with outcome, location, latency, and error details.',
    },
    {
      id: 'region-tree',
      label: 'Alert rollup',
      group: 'Fleet',
      title: 'Alert rollup',
      blurb: 'How agent observations become zone, region, and global alert states.',
    },
    {
      id: 'status',
      label: 'Status page',
      group: 'Platform',
      title: 'Status page',
      blurb: 'Public sections, monitor order, and fleet visibility.',
    },
    {
      id: 'notifications',
      label: 'Notifications',
      group: 'Platform',
      title: 'Notifications',
      blurb: 'Delivery channels, default routing, and failed notification attempts.',
    },
    {
      id: 'maintenance',
      label: 'Maintenance',
      group: 'Platform',
      title: 'Maintenance',
      blurb: 'Planned work windows that suppress notifications without pausing probes.',
    },
    {
      id: 'data',
      label: 'Data cleanup',
      group: 'Platform',
      title: 'Data cleanup',
      blurb: 'Permanent cleanup operations for stored probe and incident data.',
    },
  ] as const;
  type AdminPanel = (typeof ADMIN_PANELS)[number]['id'];

  type DayModal = { checkId: string; bucket: UptimeBucket } | null;

  // The view is reflected in the URL hash so the browser/mouse Back button steps
  // back into the app instead of leaving it — and a check page can be linked
  // and survives a refresh. Parsing and route strings live in lib/router.ts, as
  // pure functions the panel list here feeds.
  const currentView = () =>
    viewFromHash(
      location.hash,
      ADMIN_PANELS.map((p) => p.id),
      ADMIN_PANELS[0].id,
    );

  const STATUS_PAGE_SIZE = 100;
  let statusPage = $state(0);
  let statusTotal = $state(0);
  let statusHasMore = $state(false);
  let checks = $state<Check[]>([]);
  let alerts = $state<Alert[]>([]);
  // The public list follows the selected status window while the admin console
  // retains full history for incident management.
  let publicIncidents = $state<Incident[]>([]);
  let adminIncidents = $state<Incident[]>([]);
  // Only a global node's merge sets `groups` on the response; a regional
  // coordinator's answer never does, so these stay empty there and the two
  // list components fall back to their un-grouped rendering unchanged.
  let publicIncidentGroups = $state<IncidentGroup[]>([]);
  let adminIncidentGroups = $state<IncidentGroup[]>([]);
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
  // Per-section states, rolled up by the coordinator under the same policy the
  // zone and region rows use. Never recomputed here: a tile and a zone row
  // disagreeing about what "degraded" means is the bug this avoids.
  let sectionRollups = $state<SectionRollup[]>([]);
  let uptime = $state<Record<string, UptimeResponse>>({});
  // Points only: the row chart reads nothing else, and the per-agent breakdown
  // the detail view shows would double the server's scan to populate here.
  let sparkline = $state<Record<string, LatencyPoint[]>>({});
  let loading = $state(true);
  let error = $state<string | null>(null);
  let acknowledgementError = $state<string | null>(null);
  // A screen-reader-only account of websocket state transitions. The visible
  // badges update too, but those are easy to miss while reading another row.
  let liveAnnouncement = $state('');
  let wsStatus = $state<WsStatus>('connecting');
  // Bumped to tell an open CheckDetail page to refetch its own uptime, latency,
  // history and incidents — on a live state change for the check it's showing,
  // or on any websocket reconnect. It reads public endpoints that never emit a
  // 401, so without this nudge a detail page left open goes stale.
  let detailRefresh = $state(0);
  let view = $state<View<AdminPanel>>(currentView());
  let dayModal = $state<DayModal>(null);
  let authenticated = $state(false);
  // Which node the SPA is talking to. Defaults to coordinator until the
  // session probe reports otherwise.
  let nodeMode = $state<NodeMode>('coordinator');
  // Until the session probe answers, "logged out" is a guess — and on #/admin
  // acting on that guess flashes a sign-in prompt at someone already signed in.
  let sessionChecked = $state(false);
  let adminView = $state<'none' | 'login'>('none');

  // The hero controls and the persistent navigation share one seasonal choice.
  // It is local to this browser, with updates also reflected across tabs.
  let seasonalPreferences = $state<SeasonalPreferences>({ ...DEFAULT_SEASONAL_PREFERENCES });
  let seasonalNow = $state(new Date());
  const selectedSeason = $derived(resolveSeason(seasonalPreferences, seasonalNow));

  function updateSeasonalPreferences(next: SeasonalPreferences) {
    seasonalPreferences = next;
    saveSeasonalPreferences(localStorage, next);
  }

  // Tracks whether we pushed our own entry, so the in-app Back button never
  // walks off the site when someone deep-linked straight to a subpage.
  let pushedNav = $state(false);

  function openDetail(checkId: string) {
    history.pushState(null, '', `#/check/${encodeURIComponent(checkId)}`);
    pushedNav = true;
    view = { name: 'detail', checkId };
  }

  function openIncident(incidentId: string) {
    history.pushState(null, '', hashForIncident(incidentId));
    pushedNav = true;
    view = { name: 'incident', incidentId };
  }

  function openGroup(sectionName: string) {
    history.pushState(null, '', hashForGroup(sectionName));
    pushedNav = true;
    view = { name: 'group', sectionName };
  }

  /** Opens the admin incident editor. `null` is an incident being created.
   *
   *  A route rather than panel state, so the Alerts feed can link an operator
   *  straight to the thing they would act on, and a refresh stays put. */
  function openIncidentEditor(id: string | null) {
    history.pushState(null, '', hashForIncidentEditor(id));
    pushedNav = true;
    view = { name: 'admin', panel: 'incidents', incident: { id } };
  }

  function openAdmin(panel: AdminPanel = ADMIN_PANELS[0].id) {
    history.pushState(null, '', panel === 'fleet' ? hashForFleet('overview') : hashForAdmin(panel));
    pushedNav = true;
    view = panel === 'fleet' ? { name: 'admin', panel, fleetTab: 'overview' } : { name: 'admin', panel };
  }

  function openFleetTab(tab: FleetTab) {
    if (view.name === 'admin' && view.panel === 'fleet' && (view.fleetTab ?? 'overview') === tab) return;
    history.pushState(null, '', hashForFleet(tab));
    pushedNav = true;
    view = { name: 'admin', panel: 'fleet', fleetTab: tab };
  }

  /** Opens the check editor page, by immutable id. `null` is a check that does
   *  not exist yet. */
  function openCheckEditor(id: string | null) {
    history.pushState(null, '', hashForCheckEditor(id));
    pushedNav = true;
    view = { name: 'admin', panel: 'checks', edit: { id } };
  }

  function goHome() {
    history.replaceState(null, '', hashForHome());
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
    acknowledgementError = null;
    try {
      const updated = await addIncidentUpdate(incident.id, 'investigating', '');
      adminIncidents = adminIncidents.map((i) => (i.id === updated.id ? updated : i));
      publicIncidents = publicIncidents.map((i) => (i.id === updated.id ? updated : i));
    } catch (e) {
      acknowledgementError = e instanceof Error ? e.message : String(e);
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
    adminIncidents = adminIncidents.some((i) => i.id === saved.id)
      ? adminIncidents.map((i) => (i.id === saved.id ? saved : i))
      : [saved, ...adminIncidents];
    publicIncidents = publicIncidents.some((i) => i.id === saved.id)
      ? publicIncidents.map((i) => (i.id === saved.id ? saved : i))
      : [saved, ...publicIncidents];
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
  const RANGE_STORAGE_KEY = 'lighthouse.status.days';

  function storedDays(): number {
    try {
      const stored = Number(localStorage.getItem(RANGE_STORAGE_KEY));
      return RANGES.some((range) => range.days === stored) ? stored : 30;
    } catch {
      return 30;
    }
  }

  let days = $state(storedDays());

  // Every chart the current view renders, in two requests regardless of check
  // count. The server applies the same visibility rule the status list uses,
  // omits sparklines for checks that never draw one, and — given a scope —
  // returns only that scope's checks, so this asks for nothing the page will
  // not draw.
  //
  // `scope` is the section whose own page is open, or null for the primary
  // page, which excludes every paged section. The maps are REPLACED rather than
  // merged: they then always hold exactly the current view's charts at the
  // current window, so no entry can survive at a stale range after `setDays`.
  async function loadCharts(window: number, scope: string | null, page = statusPage) {
    const [strips, lines] = await Promise.all([
      fetchUptimeStrips(window, STRIP_SEGMENTS, scope, page, STATUS_PAGE_SIZE),
      fetchLatencyPoints(window * 24, STRIP_SEGMENTS, scope, page, STATUS_PAGE_SIZE),
    ]);
    // A response for a page or range already left must not replace the charts
    // now on screen.
    if (days !== window || chartScope !== scope) return;
    uptime = Object.fromEntries(strips.checks.map((u) => [u.check_id, u]));
    sparkline = Object.fromEntries(lines.checks.map((l) => [l.check_id, l.points]));
  }

  // The charts a view needs are a function of which section is open and which
  // window is selected, so that is what drives the fetch — rather than a call
  // at each of the several places either can change, where a missed one leaves
  // an opened section permanently blank.
  const chartScope = $derived(view.name === 'group' ? view.sectionName : null);
  const statusScope = $derived(view.name === 'group' ? view.sectionName : null);

  async function loadStatusPage(page: number) {
    const s = await fetchStatus(statusScope, page, STATUS_PAGE_SIZE);
    if (statusScope !== (view.name === 'group' ? view.sectionName : null)) return;
    checks = s.checks;
    statusPage = s.page;
    statusTotal = s.total_checks;
    statusHasMore = s.has_more;
    sectionRollups = s.sections;
    fleet = s.agents;
    maintenance = s.maintenance;
  }

  function setStatusPage(page: number) {
    if (page < 0 || (page === statusPage && !loading)) return;
    void loadStatusPage(page)
      .then(() => loadCharts(days, statusScope, page))
      .catch(() => {
        // Retain the currently coherent status and charts when a page fails.
      });
  }
  // Returning from a check page restores the same scope and range. Keep those
  // maps instead of repeating two bounded chart requests for that round trip.
  let lastLoadedChartScope = $state<string | null | undefined>(undefined);
  let lastLoadedChartDays = $state<number | null>(null);

  $effect(() => {
    // A check or incident page renders no list and fetches its own charts, so
    // there is nothing here for it to load.
    if (view.name !== 'home' && view.name !== 'group') return;
    const scope = chartScope;
    const window = days;
    if (lastLoadedChartScope === scope && lastLoadedChartDays === window) return;
    loadCharts(window, scope)
      .then(() => {
        if (chartScope === scope && days === window) {
          lastLoadedChartScope = scope;
          lastLoadedChartDays = window;
        }
      })
      .catch(() => {
        // Leave the last known charts up; status, alerts and incidents are
        // loaded separately and a failure here must not blank the page.
      });
  });

  // Refetch one check's uptime strip and (if it opted in) its latency graph,
  // replacing just that check's entry. Used after a live state change so the
  // percentage and charts converge on the server's numbers rather than staying
  // frozen at page-load values. Stays per-check: this is one check, not a
  // fan-out over the whole page.
  async function refreshCheckCharts(id: string) {
    const c = checks.find((ch) => ch.check_id === id);
    if (!c) return;
    try {
      const [u, line] = await Promise.all([
        fetchUptime(id, days, STRIP_SEGMENTS),
        c.latency_graph
          ? fetchLatencySparkline(id, days * 24, STRIP_SEGMENTS)
          : Promise.resolve(null),
      ]);
      uptime = { ...uptime, [id]: u };
      if (line) sparkline = { ...sparkline, [id]: line.points };
    } catch {
      // A failed refresh leaves the last-known values in place; the next event
      // or a reconnect resync will try again.
    }
  }

  // Authoritative status refresh: the WS state_change payload only carries the
  // new status and timestamp, so latency, status code, error, uptime_24h and
  // fleet health are only correct after re-reading /status. Debounced so a batch
  // of transitions costs one request, not one per check.
  const refreshStatusSoon = debounce(async () => {
    try {
      const [s, a, publicInc, adminInc] = await Promise.all([
        fetchStatus(statusScope, statusPage, STATUS_PAGE_SIZE),
        fetchAlerts(),
        fetchIncidents(50, Date.now() - days * 24 * 60 * 60 * 1000),
        fetchIncidents(),
      ]);
      checks = s.checks;
      statusPage = s.page;
      statusTotal = s.total_checks;
      statusHasMore = s.has_more;
      alerts = a.alerts;
      publicIncidents = publicInc.incidents;
      adminIncidents = adminInc.incidents;
      publicIncidentGroups = publicInc.groups ?? [];
      adminIncidentGroups = adminInc.groups ?? [];
      fleet = s.agents;
      maintenance = s.maintenance;
    } catch {
      // Keep the optimistic in-memory state; a later event or reconnect retries.
    }
  }, 600);

  // Per-check chart refresh, coalesced: several transitions on the same check in
  // quick succession trigger one refetch each after things settle.
  const refreshCheckChartsSoon = debounceByKey((id: string) => void refreshCheckCharts(id), 800);

  // Full resync after a websocket reconnect. The server lets slow clients lag,
  // so a client that dropped may have missed events entirely; re-reading status,
  // alerts, incidents and every visible chart is the only way to guarantee the
  // page isn't left stale. Reuses the initial load path without the spinner.
  async function resync() {
    try {
      const [s, a, publicInc, adminInc] = await Promise.all([
        fetchStatus(statusScope, statusPage, STATUS_PAGE_SIZE),
        fetchAlerts(),
        fetchIncidents(50, Date.now() - days * 24 * 60 * 60 * 1000),
        fetchIncidents(),
      ]);
      checks = s.checks;
      statusPage = s.page;
      statusTotal = s.total_checks;
      statusHasMore = s.has_more;
      alerts = a.alerts;
      publicIncidents = publicInc.incidents;
      adminIncidents = adminInc.incidents;
      publicIncidentGroups = publicInc.groups ?? [];
      adminIncidentGroups = adminInc.groups ?? [];
      fleet = s.agents;
      maintenance = s.maintenance;
      sectionRollups = s.sections;
      await loadCharts(days, chartScope);
    } catch {
      // A failed resync leaves the last-known state; the next reconnect retries.
    }
  }

  // The selected window bounds resolved incidents as well as the charts; open
  // incidents remain present because the server deliberately does not apply the
  // bound to them. The admin list is always fetched without this parameter.
  async function loadPublicIncidents(window: number) {
    const since = Date.now() - window * 24 * 60 * 60 * 1000;
    const fetched = await fetchIncidents(50, since);
    // A slower response for a previously selected range must not overwrite the
    // list for the range now visible on screen.
    if (days === window) {
      publicIncidents = fetched.incidents;
      publicIncidentGroups = fetched.groups ?? [];
    }
  }

  function setDays(next: number) {
    if (next === days) return;
    days = next;
    void loadPublicIncidents(next).catch(() => {
      // Keep the last known public list if its refetch fails.
    });
    try {
      localStorage.setItem(RANGE_STORAGE_KEY, String(next));
    } catch {}
  }

  // The order an admin arranged, not alphabetical. Name breaks ties so the list
  // stays stable for checks that have never been explicitly ordered.
  const ordered = $derived(
    [...checks].sort(
      (a, b) => (a.position ?? 0) - (b.position ?? 0) || a.name.localeCompare(b.name),
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
    // sections table never had — renders expanded and inline rather than
    // vanishing or becoming an empty tile.
    return [...groups].map(([name, checks]) => {
      const section = sections.find((s) => s.name === name);
      return {
        name,
        checks,
        collapsed: section?.collapsed ?? false,
        display: section?.display ?? 'inline',
      };
    });
  });

  // The primary page renders inline sections as rows and paged ones as tiles.
  // Splitting here keeps the markup from testing `display` twice and getting
  // the two branches out of step.
  const inlineGroups = $derived(monitorGroups.filter((g) => g.display === 'inline'));
  // Page sections are represented by rollups on the primary scope, not their
  // child rows. Keep their tiles here so bounded status pages do not make an
  // unopened section disappear.
  const pagedGroups = $derived(
    sections
      .filter((section) => section.display === 'page')
      .map((section) => ({ name: section.name, checks: [] as Check[], collapsed: section.collapsed })),
  );

  // The section whose own page is open, resolved against the status list. An
  // empty `checks` here is a real state — a section can be emptied while its
  // page is bookmarked — and renders as a message, not a blank page.
  const groupPage = $derived.by(() => {
    if (view.name !== 'group') return null;
    const name = view.sectionName;
    return {
      name,
      checks: ordered.filter((c) => c.group === name),
      known: sections.some((s) => s.name === name),
      rollup: sectionRollups.find((r) => r.name === name),
    };
  });

  const groupRollupBanner = $derived.by(() => {
    const rollup = groupPage?.rollup;
    if (!rollup) return null;
    const tone = rollup.status === 'up' ? 'operational' : rollup.status === 'down' ? 'major' : 'degraded';
    const label =
      rollup.status === 'up'
        ? 'All monitors operational'
        : rollup.status === 'down'
          ? 'Section outage'
          : rollup.status === 'degraded'
            ? 'Degraded section performance'
            : 'Section status unknown';
    return { ...rollup, tone, label };
  });

  // Everything without a group is listed loose, so a check can never be hidden
  // by a config typo.
  const generalChecks = $derived(ordered.filter((c) => !c.group));

  // The page-wide live surface is derived from the windowed public list, which
  // always includes open incidents even when their start predates the window.
  const activePublicIncidents = $derived(
    publicIncidents.filter((incident) => incident.state !== 'resolved').sort((a, b) => b.started_at - a.started_at),
  );
  const leadPublicIncident = $derived(activePublicIncidents[0] ?? null);
  const publicRouteIncidentId = $derived(view.name === 'incident' ? view.incidentId : null);

  // Section URLs remain deep-linkable; changing scope always starts at page one
  // and requests matching charts with the exact same scope/page tuple.
  $effect(() => {
    if (view.name !== 'home' && view.name !== 'group') return;
    const scope = statusScope;
    statusPage = 0;
    void loadStatusPage(0)
      .then(() => loadCharts(days, scope, 0))
      .catch(() => {
        // The normal load error path remains responsible for initial failures.
      });
  });

  async function load() {
    loading = true;
    error = null;
    try {
      const [s, a, publicInc, adminInc, sec] = await Promise.all([
        fetchStatus(statusScope, statusPage, STATUS_PAGE_SIZE),
        fetchAlerts(),
        fetchIncidents(50, Date.now() - days * 24 * 60 * 60 * 1000),
        fetchIncidents(),
        fetchPublicSections(),
      ]);
      checks = s.checks;
      statusPage = s.page;
      statusTotal = s.total_checks;
      statusHasMore = s.has_more;
      alerts = a.alerts;
      publicIncidents = publicInc.incidents;
      adminIncidents = adminInc.incidents;
      publicIncidentGroups = publicInc.groups ?? [];
      adminIncidentGroups = adminInc.groups ?? [];
      fleet = s.agents;
      maintenance = s.maintenance;
      sections = sec;
      sectionRollups = s.sections;
      // Charts are not awaited here. They are driven by `chartScope`/`days`,
      // and that request runs in parallel with this one rather than after it.
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
  // The mount and initial admin-route paths can overlap, so share their one
  // request rather than probing the same cookie twice — createSessionChecker
  // (lib/session.ts) is the de-duplication; only the fields to set are ours.
  const checkSession = createSessionChecker({
    fetchSession,
    onResolved: (s) => {
      authenticated = s.authenticated;
      nodeMode = s.mode;
      if (!s.authenticated && view.name === 'admin') adminView = 'login';
    },
    onSettled: () => {
      sessionChecked = true;
    },
  });

  $effect(() => {
    if (view.name !== 'admin') return;
    void view.panel;
    void checkSession();
  });

  onMount(() => {
    seasonalPreferences = loadSeasonalPreferences(localStorage);
    seasonalNow = new Date();
    // Auto mode follows the local date even when Overview isn't mounted.
    const dateInterval = window.setInterval(() => (seasonalNow = new Date()), 60_000);
    const refreshDate = () => { if (document.visibilityState === 'visible') seasonalNow = new Date(); };
    const syncFromStorage = (event: StorageEvent) => {
      if (event.key === SEASONAL_STORAGE_KEY || event.key === null)
        seasonalPreferences = loadSeasonalPreferences(localStorage);
    };
    window.addEventListener('storage', syncFromStorage);
    document.addEventListener('visibilitychange', refreshDate);
    load();
    // Cookie-only: the server is the sole authority on whether we're signed in.
    void checkSession();
    const conn = connectWs({
      onStatus: (s) => (wsStatus = s),
      onStateChange: (msg) => {
        // Optimistic: flip the badge (and the "All Systems Operational" banner,
        // which is derived from `checks`) and prepend the alert immediately for
        // instant feedback. Reducers are pure and unit-tested in live.test.ts.
        const check = checks.find((c) => c.check_id === msg.check_id);
        checks = applyStateChange(checks, msg);
        alerts = prependAlert(alerts, msg);
        liveAnnouncement = `${check?.name ?? 'Monitor'} changed from ${msg.from} to ${msg.to}.`;
        // Then reconcile against the server: the event carries no latency,
        // status code, error, uptime_24h or fleet health, and the charts are
        // frozen at page-load values until refetched.
        refreshStatusSoon();
        refreshCheckChartsSoon(msg.check_id);
        // If the detail page for this check is open, nudge it to refetch too.
        if (view.name === 'detail' && view.checkId === msg.check_id) detailRefresh += 1;
      },
      onIncident: (msg) => {
        adminIncidents = applyIncidentEvent(adminIncidents, msg);
        publicIncidents = applyIncidentEvent(publicIncidents, msg);
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
      view = currentView();
      if (view.name === 'home') pushedNav = false;
      dayModal = null;
    };
    window.addEventListener('popstate', onPop);
    return () => {
      window.clearInterval(dateInterval);
      window.removeEventListener('storage', syncFromStorage);
      document.removeEventListener('visibilitychange', refreshDate);
      conn.close();
      window.removeEventListener('popstate', onPop);
      refreshStatusSoon.cancel();
      refreshCheckChartsSoon.cancelAll();
    };
  });
</script>

<div class="app" class:wide={view.name === 'admin'} class:admin-app={view.name === 'admin'} class:public-app={view.name !== 'admin'}>
  <div
    aria-live="polite"
    aria-atomic="true"
    style="position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0"
  >
    {liveAnnouncement}
  </div>
  <!-- The console owns the whole viewport, brand included, so the status page
       header is suppressed there rather than stacked above a second brand. -->
  {#if view.name !== 'admin'}
    <header class="public-topbar" aria-label="Public status navigation">
      <div class="public-brand">
        <!-- Match the admin console's lighthouse silhouette without the
             horizontal beacon marks so the public identity stays restrained. -->
        <svg class="public-brand-icon" viewBox="0 0 32 40" fill="none" aria-hidden="true">
          <path d="m10 35 3-23h6l3 23M8 35h16M11 12h10M12 7h8l-4-5ZM13 7v5m6-5v5"
            stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" />
          <path d="m15 16-1 15h4l-1-15" fill="currentColor" opacity=".3" />
        </svg>
        <div class="public-brand-text"><h1>Lightkeeper</h1><p class="subtitle">Service status</p></div>
      </div>
      <nav class="public-nav" aria-label="Public pages">
        <button class:active={view.name === 'home'} onclick={goHome}>Status</button>
        <button onclick={() => {
          if (view.name !== 'home') goHome();
          else document.getElementById('public-incidents')?.scrollIntoView({ behavior: 'smooth' });
        }}>History</button>
        <button class="public-admin" onclick={() => (authenticated ? openAdmin() : (adminView = 'login'))}>Admin</button>
      </nav>
    </header>
    {#if view.name !== 'home'}
      <!-- The public routes retain the lighthouse identity even without the
           health verdict shown on the status homepage. Decorative only. -->
      <div
        class="public-route-masthead"
        class:storm={publicRouteIncidentId !== null && activePublicIncidents.some((incident) => incident.id === publicRouteIncidentId)}
        style:--scene={'url("' + publicLighthouseScene + '")'}
        aria-hidden="true"
      ></div>
    {/if}
  {/if}

  {#if view.name === 'admin'}
    {@const panel = view.panel}
    {@const editing = view.edit ?? null}
    {@const incEditing = view.incident ?? null}
    {@const meta = ADMIN_PANELS.find((p) => p.id === panel)}
    {@const openCount = adminIncidents.filter((i) => i.state !== 'resolved').length}
    {@const unacked = adminIncidents.filter((i) => i.state === 'detected').length}
    <ConsoleShell
      panels={ADMIN_PANELS}
      active={panel}
      title={meta?.title ?? 'Admin'}
      incidentCount={openCount}
      incidentsUnacknowledged={unacked}
      {wsStatus}
      {nodeMode}
      authenticated={sessionChecked && authenticated}
      overview={panel === 'overview' && sessionChecked && authenticated}
      season={selectedSeason}
      onSelect={(id) => selectPanel(id as AdminPanel)}
      onHome={goHome}
      onSignOut={signOut}
    >
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
        {:else if panel === 'overview'}
          <NightwatchOverview seasonalPreferences={seasonalPreferences} seasonalNow={seasonalNow} onSeasonalPreferencesChange={updateSeasonalPreferences} {checks} totalChecks={statusTotal} hasMore={statusHasMore || statusPage > 0} incidents={adminIncidents} groups={adminIncidentGroups} {alerts} {fleet} {maintenance} {loading} {error} {wsStatus} {nodeMode} onOpenCheck={openDetail} onOpenIncident={openIncidentEditor} onPanel={(id) => { const target = ADMIN_PANELS.find((p) => p.id === id); if (target) openAdmin(target.id); }} onAddCheck={() => openCheckEditor(null)} onRetry={load} onExpired={onExpired} />
        {:else if panel === 'checks' && editing}
          <!-- Keyed so back/forward between two checks rebuilds the editor
               instead of leaving one check's draft under another's id. -->
          {#key editing.id}
            <div class="admin-workspace"><CheckEditor id={editing.id} onsaved={load} onclose={() => openAdmin('checks')} /></div>
          {/key}
        {:else if panel === 'incidents' && incEditing}
          {#key incEditing.id}
              <div class="admin-workspace"><IncidentEditor

              incidentId={incEditing.id}
              checks={checks.map((c) => ({ id: c.check_id, name: c.name }))}
              regionScoped={nodeMode === 'global'}
              onClose={() => openAdmin('incidents')}
                onSaved={onIncidentSaved}
              /></div>
          {/key}
          {:else}
            <div class="admin-workspace">
            <div class="page-head page-head-row">
            <div>
              <div class="console-page-title"><span class="console-page-icon"><ConsoleIcon name={panel} size={20} /></span><h2>{meta?.title ?? 'Admin'}</h2></div>
              <p class="page-meta">{meta?.blurb ?? ''}</p>
            </div>
            <div class="page-actions">
              {#if panel === 'checks'}
                <button class="btn primary" onclick={() => openCheckEditor(null)}>New check</button>
              {:else if panel === 'incidents'}
                <button class="btn primary" onclick={() => openIncidentEditor(null)}>New incident</button>
              {/if}
            </div>
          </div>
          {#if panel === 'checks'}
            <ChecksAdmin onopen={openCheckEditor} onexpired={onExpired} />
          {:else if panel === 'status'}
            <SectionsAdmin onchanged={load} onexpired={onExpired} />
            <AgentDisplayAdmin onchanged={load} onexpired={onExpired} />
          {:else if panel === 'fleet'}
            <FleetAdmin tab={view.fleetTab ?? 'overview'} onNavigate={openFleetTab} onexpired={onExpired} onopen={openCheckEditor} {nodeMode} />
          {:else if panel === 'results'}
            <ProbeResultsAdmin onopen={openCheckEditor} onexpired={onExpired} />
          {:else if panel === 'region-tree'}
            <RegionTreeAdmin onexpired={onExpired} />
          {:else if panel === 'alerts'}
            <AlertsList {alerts} {checks} onOpenIncident={openIncidentEditor} />
          {:else if panel === 'incidents'}
            {#if acknowledgementError}
              <div class="notice error" role="alert">
                Failed to acknowledge incident: {acknowledgementError}
                <button class="link" onclick={() => (acknowledgementError = null)}>Dismiss</button>
              </div>
            {/if}
            <IncidentsList
              incidents={adminIncidents}
              groups={adminIncidentGroups}
              {checks}
              onEdit={(i) => openIncidentEditor(i.id)}
              onAck={acknowledge}
            />
          {:else if panel === 'notifications'}
            <NotifiersAdmin onexpired={onExpired} />
          {:else if panel === 'maintenance'}
            <MaintenanceAdmin onexpired={onExpired} />
          {:else}
              <DataAdmin onexpired={onExpired} />
            {/if}
            </div>
        {/if}
    </ConsoleShell>
  {:else if loading}
    <div class="notice">Loading…</div>
  {:else if error}
    <div class="notice error">
      Failed to load data: {error}
      <div style="margin-top:0.5rem"><button class="link" onclick={load}>Retry</button></div>
    </div>
  {:else if view.name === 'incident'}
    {@const id = view.incidentId}
    <div class="public-status-detail"><IncidentPage incidentId={id} {checks} onBack={goBack} /></div>
  {:else if view.name === 'detail'}
    <!-- Bound once: the narrowing on `view` doesn't survive into the callback below. -->
    {@const id = view.checkId}
    <div class="public-status-detail"><CheckDetail
      checkId={id}
      check={checks.find((c) => c.check_id === id)}
      refreshKey={detailRefresh}
      onback={goBack}
    /></div>
  {:else if view.name === 'group' && groupPage}
    <button class="link" onclick={goBack}>← Back</button>
    <section class="section">
      {#if groupRollupBanner}
        <div class="banner {groupRollupBanner.tone}">
          <span class="banner-icon" aria-hidden="true">
            {#if groupRollupBanner.status === 'up'}✓{:else}!{/if}
          </span>
          <span class="banner-text">
            {groupRollupBanner.label} · {groupRollupBanner.checks} {groupRollupBanner.checks === 1 ? 'monitor' : 'monitors'}
            {#if groupRollupBanner.down > 0} · {groupRollupBanner.down} down{/if}
            {#if groupRollupBanner.degraded > 0} · {groupRollupBanner.degraded} degraded{/if}
          </span>
        </div>
      {/if}
      <div class="section-bar">
        <h2 class="section-title">{groupPage.name}</h2>
        <div class="range-switch" role="group" aria-label="chart window">
          {#each RANGES as r (r.days)}
            <button class="range-btn" class:active={days === r.days} onclick={() => setDays(r.days)}>
              {r.label}
            </button>
          {/each}
        </div>
      </div>
      {#if groupPage.checks.length}
        <div class="components public-services">
          {#each groupPage.checks as c (c.check_id)}
            <ComponentRow
              compact
              check={c}
              uptime={uptime[c.check_id]}
              sparkline={sparkline[c.check_id] ?? null}
              incident={openIncidentForCheck(publicIncidents, c.check_id)}
              onselect={openDetail}
              onbarclick={(id, b) => (dayModal = { checkId: id, bucket: b })}
            />
          {/each}
        </div>
       {:else}
         <div class="notice">
           {groupPage.known
             ? 'This section has no checks in it.'
             : 'No such section. It may have been renamed or deleted.'}
         </div>
       {/if}
       {#if statusTotal > STATUS_PAGE_SIZE}
         <nav class="pagination" aria-label="monitor pages">
           <button class="btn" disabled={statusPage === 0} onclick={() => setStatusPage(statusPage - 1)}>Previous</button>
           <span>Showing {statusPage * STATUS_PAGE_SIZE + 1}–{Math.min((statusPage + 1) * STATUS_PAGE_SIZE, statusTotal)} of {statusTotal}</span>
           <button class="btn" disabled={!statusHasMore} onclick={() => setStatusPage(statusPage + 1)}>Next</button>
         </nav>
       {/if}
     </section>
   {:else}
     <PublicStatusHero {checks} incidents={publicIncidents} sections={sectionRollups} />
     {#if leadPublicIncident}
       <PublicIncidentSpotlight incident={leadPublicIncident} {checks} onOpen={openIncident} />
       {#if activePublicIncidents.length > 1}
         <p class="public-muted">{activePublicIncidents.length - 1} additional active {activePublicIncidents.length === 2 ? 'incident' : 'incidents'} listed in incident history below.</p>
       {/if}
     {/if}

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

    <section class="section">
      <div class="section-bar">
        <h2 class="section-title">Services</h2>
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
      <div class="components public-services">
        {#each generalChecks as c (c.check_id)}
          <ComponentRow
            compact
            check={c}
            uptime={uptime[c.check_id]}
            sparkline={sparkline[c.check_id] ?? null}
            incident={openIncidentForCheck(publicIncidents, c.check_id)}
            onselect={openDetail}
            onbarclick={(id, b) => (dayModal = { checkId: id, bucket: b })}
          />
        {/each}

        {#each pagedGroups as g (g.name)}
          <SectionTile
            name={g.name}
            rollup={sectionRollups.find((r) => r.name === g.name)}
            count={sectionRollups.find((r) => r.name === g.name)?.checks ?? 0}
            onopen={openGroup}
          />
        {/each}

        {#each inlineGroups as g (g.name)}
          <CollapsibleGroup
            name={g.name}
            status={worstStatus(g.checks)}
            count={g.checks.length}
            collapsed={g.collapsed}
          >
            {#each g.checks as c (c.check_id)}
              <ComponentRow
                compact
                check={c}
                uptime={uptime[c.check_id]}
                sparkline={sparkline[c.check_id] ?? null}
                incident={openIncidentForCheck(publicIncidents, c.check_id)}
                onselect={openDetail}
                onbarclick={(id, b) => (dayModal = { checkId: id, bucket: b })}
              />
            {/each}
          </CollapsibleGroup>
        {/each}
       </div>
       {#if statusTotal > STATUS_PAGE_SIZE}
         <nav class="pagination" aria-label="monitor pages">
           <button class="btn" disabled={statusPage === 0} onclick={() => setStatusPage(statusPage - 1)}>Previous</button>
           <span>Showing {statusPage * STATUS_PAGE_SIZE + 1}–{Math.min((statusPage + 1) * STATUS_PAGE_SIZE, statusTotal)} of {statusTotal}</span>
           <button class="btn" disabled={!statusHasMore} onclick={() => setStatusPage(statusPage + 1)}>Next</button>
         </nav>
       {/if}
      </section>

      <section class="section" id="public-incidents" aria-label="Incident history">
        <div class="section-bar"><h2 class="section-title">Recent incidents</h2></div>
        {#if publicIncidents.length === 0}
          <div class="public-incidents-empty"><span class="empty-mark" aria-hidden="true">✓</span><span>No incidents recorded in the current history window.</span></div>
        {:else}
          <PastIncidents incidents={publicIncidents} groups={publicIncidentGroups} {checks} onOpen={openIncident} />
        {/if}
      </section>

      {#if fleet.length}
        <details class="section public-coverage" aria-label="Monitoring coverage">
          <summary class="public-coverage-toggle">
            <span class="public-coverage-heading">
              <strong>Monitoring coverage</strong>
              <span>Connectivity of the monitoring fleet, not the health of the services it checks.</span>
            </span>
            <span class="public-coverage-count">
              {fleet.filter((row) => row.kind === 'region').length || fleet.length} reported locations
              <span class="public-coverage-action">· <span class="public-coverage-action-label">View observers</span></span>
              <span class="public-coverage-chevron" aria-hidden="true">⌄</span>
            </span>
          </summary>
          <div class="public-coverage-observers" aria-label="Reporting locations">
            {#each fleet as row (row.kind + ':' + row.name)}
              <ServerRow name={row.name} status={row.status}
                detail={row.kind === 'region' ? row.healthy + '/' + row.total + ' agents reporting' : undefined} />
            {/each}
          </div>
        </details>
      {/if}

  {/if}
  {#if view.name !== 'admin'}
    <footer class="public-footer">
      <span><strong>Lightkeeper</strong>　│　Calm in the storm.</span>
      <span>Status reflects published observations. Times shown in your local timezone.</span>
    </footer>
  {/if}
</div>

{#if dayModal}
  <DayDetailModal
    checkId={dayModal.checkId}
    name={checks.find((c) => c.check_id === dayModal?.checkId)?.name}
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
