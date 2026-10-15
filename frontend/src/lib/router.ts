// The URL hash is the single source of truth for navigation: it drives what
// renders, survives a refresh, and lets the browser's own Back button step
// back into the app instead of leaving it. Parsing (`viewFromHash`) and
// building (`hashFor*`) live here as pure functions so the routes themselves
// are unit-testable without mounting the app.

export type FleetTab = 'overview' | 'agents' | 'access';

export type View<Panel extends string> =
  | { name: 'home' }
  | { name: 'detail'; checkId: string }
  // The public permalink for one incident, which is what the status page and
  // any notification link to.
  | { name: 'incident'; incidentId: string }
  // `edit` present = the check editor page rather than the panel's own
  // content; `edit.id === null` is a check that does not exist yet.
  // `incident` present = the incident editor, open on that id (or `null` for
  // one being created).
  | {
      name: 'admin';
      panel: Panel;
      fleetTab?: FleetTab;
      edit?: { id: string | null };
      incident?: { id: string | null };
    }
  // One section's own page. Carries the name, not a resolved section: the
  // route must survive a deep link that lands before `/api/v1/sections` has.
  | { name: 'group'; sectionName: string };

/** Parses `location.hash` into a `View`. `panelIds`/`defaultPanel` are
 *  supplied by the caller rather than baked in here, since the set of admin
 *  panels is app configuration, not routing. */
export function viewFromHash<Panel extends string>(
  hash: string,
  panelIds: readonly Panel[],
  defaultPanel: Panel,
): View<Panel> {
  // #/admin/checks/<id> — or /new — is the editor page for one check. "new"
  // is therefore a reserved id: no check ever has the literal id "new", since
  // ids are server-minted UUIDs. Accepted rather than paid for with a query
  // parameter hanging off every other admin URL.
  const editor = hash.match(/^#\/admin\/checks\/(.+?)\/?$/);
  if (editor) {
    const raw = decodeURIComponent(editor[1]);
    return { name: 'admin', panel: 'checks' as Panel, edit: { id: raw === 'new' ? null : raw } };
  }
  // #/admin/incidents/<id> — the editor for one incident, so the Alerts feed
  // can link straight to the thing an operator would act on, and the URL
  // survives a refresh. "new" is reserved the same way as for checks.
  const incEditor = hash.match(/^#\/admin\/incidents\/(.+?)\/?$/);
  if (incEditor) {
    const raw = decodeURIComponent(incEditor[1]);
    return {
      name: 'admin',
      panel: 'incidents' as Panel,
      incident: { id: raw === 'new' ? null : raw },
    };
  }
  // Fleet tabs have their own permalinks and Back/Forward history.
  const fleet = hash.match(/^#\/admin\/fleet(?:\/(overview|agents|regions|access|credentials|registration))?\/?$/);
  if (fleet) {
    const oldTab = fleet[1] === 'credentials' || fleet[1] === 'registration';
    const tab: FleetTab = oldTab ? 'access' : fleet[1] === 'agents' ? 'agents' : 'overview';
    return { name: 'admin', panel: 'fleet' as Panel, fleetTab: tab };
  }

  // Preserve old bookmarked admin pages after the Fleet consolidation.
  const oldFleet = hash.match(/^#\/admin\/(agents|credentials|cluster)\/?$/);
  if (oldFleet) {
    const tab: FleetTab = oldFleet[1] === 'agents' ? 'agents' : oldFleet[1] === 'credentials' ? 'access' : 'overview';
    return { name: 'admin', panel: 'fleet' as Panel, fleetTab: tab };
  }
  const admin = hash.match(/^#\/admin(?:\/([^/]*))?\/?$/);
  if (admin) {
    // An unknown or absent id lands on the first panel rather than rendering
    // an empty content area.
    const panel = panelIds.find((p) => p === admin[1]) ?? defaultPanel;
    return { name: 'admin', panel };
  }
  const inc = hash.match(/^#\/incident\/(.+)$/);
  if (inc) return { name: 'incident', incidentId: decodeURIComponent(inc[1]) };
  // #/group/<name> — a section an admin set to render as its own page. A
  // check page stays reachable at #/check/<id> without passing through here,
  // so a deep link into a paged section's check still works.
  const grp = hash.match(/^#\/group\/(.+)$/);
  if (grp) return { name: 'group', sectionName: decodeURIComponent(grp[1]) };
  const m = hash.match(/^#\/check\/(.+)$/);
  return m ? { name: 'detail', checkId: decodeURIComponent(m[1]) } : { name: 'home' };
}

export const hashForHome = () => '#/';
export const hashForCheck = (id: string) => `#/check/${encodeURIComponent(id)}`;
export const hashForIncident = (id: string) => `#/incident/${encodeURIComponent(id)}`;
export const hashForGroup = (name: string) => `#/group/${encodeURIComponent(name)}`;
export const hashForAdmin = (panel: string) => `#/admin/${panel}`;
export const hashForFleet = (tab: FleetTab) => `#/admin/fleet/${tab}`;
export const hashForCheckEditor = (id: string | null) =>
  `#/admin/checks/${id === null ? 'new' : encodeURIComponent(id)}`;
export const hashForIncidentEditor = (id: string | null) =>
  `#/admin/incidents/${id === null ? 'new' : encodeURIComponent(id)}`;
