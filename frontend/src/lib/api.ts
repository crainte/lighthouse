// Types + fetch wrappers for the Lighthouse coordinator HTTP API.
// Same-origin in production; /api is proxied to the backend during `vite dev`.

/** A single probe attempt remains three-state; degradation is derived only
 * when the coordinator rolls several attempts up for publication. */
export type ProbeStatus = "up" | "down" | "unknown";

/** Published rollup state. A check can be degraded when only part of its
 * configured probe fleet is healthy. */
export type Status = ProbeStatus | "degraded";

export type CheckKind =
  "http" | "tcp" | "dns" | "icmp" | "ssl" | "heartbeat" | "openmetrics";

export interface Check {
  /** Immutable identity. Every route, key and lookup uses this — never `name`. */
  check_id: string;
  /** Mutable display name. Render this in labels; renaming does not change
   *  `check_id` or orphan any history, alert or incident. */
  name: string;
  status: Status;
  latency_ms: number | null;
  last_check: number;
  /** `null` when nothing was measured in the window — a check configured on
   *  this coordinator but placed in another region, or one probed for the first
   *  time moments ago. Distinct from `0`, which is a real outage. */
  uptime_24h: number | null;
  /** HTTP status code from the latest attempt, when the probe was HTTP. */
  status_code?: number;
  /** Why the latest attempt failed. Absent when it succeeded. */
  error?: string;
  /** Probe type, from config. Absent for checks not in the current config. */
  type?: CheckKind;
  /** Configured group name. Absent means the check is listed loose. */
  group?: string;
  /** Whether this check opted into a latency graph on the status page. */
  latency_graph?: boolean;
  /** When the probe runs, as cron. The target is deliberately not public. */
  cron?: string;
  /** Display order within the section, as arranged in the admin page. */
  position?: number;
}

/** What the coordinator reports for an agent that declares no zone or region.
 *  A real value the backend writes, not a frontend placeholder — matching on it
 *  is how the unplaced bucket is found. */
export const UNKNOWN_LOCATION = "unknown";

export interface Agent {
  agent_id: string;
  /** Failure domain the agent declares for itself — `zone` is one AZ or one
   *  office link, `region` groups zones. Both are `"unknown"` when the agent's
   *  own config names neither. */
  zone: string;
  region: string;
  last_seen: number;
  checks_reporting: number;
  results_24h: number;
  status: ProbeStatus;
  /** Silent long enough to read as decommissioned. Still listed and still owns
   *  its history — retirement is derived from silence, not a state anyone sets. */
  retired: boolean;
  /** How many checks name this agent explicitly. Purging one that is still
   *  named would leave those checks with nothing to run them. */
  assigned_checks: number;
  /** Round-trip of the agent's last poll, ms, as it measured it. `null` for an
   *  agent seen only via reported results, never a live poll. */
  poll_rtt_ms: number | null;
  /** The build it reported on its last poll, to whichever coordinator it
   *  polls. `null` for an agent that predates build reporting, or one seen
   *  only via reported results. */
  version: string | null;
  /** Node id of the coordinator it last polled. `null` when unknown. */
  coordinator: string | null;
  /** Check kinds it says it can probe. `null` means it never said, which is
   *  read as "anything" rather than "nothing": every agent predating capability
   *  negotiation reports nothing, and treating that as incapable would empty
   *  the fleet on upgrade. */
  kinds: string[] | null;
}

export interface AgentsResponse {
  agents: Agent[];
  /** Enabled checks pinned only to agents that have gone silent. Nothing runs
   *  them and nothing else reports it, so this is the one place it surfaces. */
  checks_with_no_live_agent: string[];
  /** Enabled checks whose every eligible agent is present and reporting but has
   *  said it cannot probe that kind. Distinct from the list above because the
   *  remedy differs: those agents are not missing, they are out of date. */
  checks_no_agent_can_run: string[];
  /** How many checks each list found. The arrays above are a sample capped
   *  server-side: losing a region's agents orphans every check it holds, and
   *  naming all of them costs as much as the page being warned about. */
  checks_with_no_live_agent_total: number;
  checks_no_agent_can_run_total: number;
  /** Global node only: regions whose fleet could not be read, so the lists
   *  above are missing them. */
  unavailable_regions?: string[];
}

/** Agent liveness on the public page. An agent answering is healthy even if
 *  every check it probes is down. */
export type Health = "up" | "degraded" | "down";

/** One row of the public agent section: a single agent, or a whole region. */
export interface FleetRow {
  /** Also what keeps keys unique: a region and an agent may share a name. */
  kind: "agent" | "region";
  name: string;
  status: Health;
  healthy: number;
  total: number;
}

/** A scheduled window the operator chose to announce, while it runs. Windows
 *  that were not announced suppress paging silently and never appear here. */
export interface MaintenanceNotice {
  title: string;
  starts_at: number;
  ends_at: number;
}

export interface StatusResponse {
  checks: Check[];
  /** Bounded status page echoed by chart routes. */
  page: number;
  limit: number;
  total_checks: number;
  has_more: boolean;
  /** Empty when `[status_page.agents] mode = "off"`. */
  agents: FleetRow[];
  /** Announced windows running right now, soonest to end first. */
  maintenance: MaintenanceNotice[];
  /** One entry per section that has checks, rolled up by the coordinator. The
   *  rule lives there so a section tile and a zone row cannot disagree; do not
   *  recompute a state from `checks` here. */
  sections: SectionRollup[];
}

export interface SectionRollup {
  name: string;
  status: Status;
  /** Caption counts. A tile shows no history strip, so these are all it says. */
  checks: number;
  degraded: number;
  down: number;
}

export interface HistoryPoint {
  timestamp: number;
  /** Several agents probe the same check, so (timestamp, agent_id) is the key. */
  agent_id: string;
  status: ProbeStatus;
  latency_ms: number | null;
  status_code?: number;
  error?: string;
}

export interface HistoryResponse {
  check_id: string;
  name: string;
  history: HistoryPoint[];
}

/** One confirmed state transition. The event log — distinct from `Incident`,
 *  which is the operator-facing record of an outage. `alert_type` is derived
 *  by the server from the two state fields, never stored. */
export interface Alert {
  id: string;
  scope: "check" | "agent" | "zone" | "region";
  node_id: string;
  /** Human-readable check or rollup label; identity remains in node_id. */
  name?: string;
  alert_type: "state_change" | "degradation" | "recovery";
  previous_state: Status | null;
  current_state: Status;
  timestamp: number;
  /** The incident this transition belongs to, stamped when it was written. */
  incident_id: string | null;
}

export interface AlertsResponse {
  alerts: Alert[];
}

export interface StateChange {
  type: "state_change";
  check_id: string;
  from: Status;
  to: Status;
  timestamp: number;
}

/** WS lifecycle events for incidents, broadcast alongside state changes.
 *  `incident_updated` covers everything after the open — a check joining, an
 *  operator posting, auto-resolve — because the payload is the whole incident
 *  and the UI just replaces its copy. `incident_deleted` carries only the id —
 *  the incident is gone, there is nothing left to replace it with — and fires
 *  both on a local admin delete and on a peer's replicated one, so every node
 *  drops it the same way. */
export type IncidentEvent =
  | { type: "incident_opened" | "incident_updated"; incident: Incident }
  | { type: "incident_deleted"; id: string };

export interface UptimeBucket {
  /** Exclusive lower bound of the segment. Only a day boundary at the 90d range. */
  start: number;
  end: number;
  uptime: number | null;
  up: number;
  total: number;
}

export interface UptimeResponse {
  check_id: string;
  name: string;
  days: number;
  overall_uptime: number | null;
  buckets: UptimeBucket[];
}

export interface LatencyPoint {
  timestamp: number;
  /** All outcomes observed in the segment, including failures. Optional while
   * talking to an older coordinator which did not publish bucket counts. */
  attempts?: number;
  /** Successful outcomes in the segment. Only these contribute latency. */
  up?: number;
  /** `null` when no attempt passed. Not the same as a 0ms reply, and drawing
   *  it as one paints an imaginary flat line along the axis. */
  avg_latency_ms: number | null;
}

/** Reliability and latency spread over a window. `min`/`avg`/`max` cover
 *  successful attempts only, so a timeout never becomes the "slowest" reading. */
export interface LatencyBand {
  attempts: number;
  up: number;
  min_ms: number | null;
  avg_ms: number | null;
  max_ms: number | null;
}

export interface AgentBand extends LatencyBand {
  agent_id: string;
}

export interface LatencySparklineResponse {
  check_id: string;
  name: string;
  /** Window actually charted; echoed back so the client never assumes it. */
  from: number;
  to: number;
  buckets: number;
  points: LatencyPoint[];
  overall: LatencyBand;
  agents: AgentBand[];
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} -> ${res.status} ${res.statusText}`);
  return res.json();
}

export function fetchStatus(
  section: string | null = null,
  page = 0,
  limit = 100,
  all = false,
): Promise<StatusResponse> {
  return getJson<StatusResponse>(
    `/api/v1/status?page=${page}&limit=${limit}${sectionParam(section)}${all ? "&all=true" : ""}`,
  );
}

export function fetchHistory(
  id: string,
  limit = 100,
  from?: number,
  to?: number,
  status?: ProbeStatus,
): Promise<HistoryResponse> {
  let url = `/api/v1/checks/${encodeURIComponent(id)}/history?limit=${limit}`;
  if (from !== undefined) url += `&from=${from}`;
  if (to !== undefined) url += `&to=${to}`;
  if (status !== undefined) url += `&status=${status}`;
  return getJson<HistoryResponse>(url);
}

export function fetchAlerts(limit = 50): Promise<AlertsResponse> {
  return getJson<AlertsResponse>(`/api/v1/alerts?limit=${limit}`);
}

/** Where an incident is in its life. The single lifecycle field: "open" means
 *  anything other than `resolved`.
 *
 *  `detected` is the machine's state and where every automatic incident
 *  starts — a probe noticing an outage is not a human investigating one.
 *  Acknowledging is the `detected` -> `investigating` edge. */
export type IncidentState =
  "detected" | "investigating" | "identified" | "monitoring" | "resolved";

export const INCIDENT_STATES: IncidentState[] = [
  "detected",
  "investigating",
  "identified",
  "monitoring",
  "resolved",
];

/** How a check came to be attached. `origin` is the check whose DOWN opened
 *  the incident, `dependency` was attached by `depends_on` reconciliation, and
 *  only `manual` rows are editable — the other two belong to the coordinator. */
export type AffectSource = "origin" | "dependency" | "manual";

export interface IncidentCheck {
  check_id: string;
  source: AffectSource;
}

export interface IncidentUpdate {
  id: string;
  state: IncidentState;
  body: string;
  /** `null` when the coordinator wrote it. A non-null author is what makes an
   *  update an acknowledgement. */
  author: string | null;
  created_at: number;
}

export interface Incident {
  id: string;
  title: string;
  state: IncidentState;
  started_at: number;
  resolved_at: number | null;
  /** Server-computed: to resolution, or to now while open. */
  duration_ms: number;
  /** Rendered page-wide rather than against its checks. */
  global: boolean;
  /** The check whose DOWN opened it; null when a human did. */
  origin_check: string | null;
  checks: IncidentCheck[];
  /** Whether any human has posted an update. Derived server-side from the
   *  narrative, not a stored flag. */
  acknowledged: boolean;
  /** Only present on the single-incident read. */
  updates?: IncidentUpdate[];
}

/** One region's incident inside a group, preserved in full — which region,
 *  when it detected the outage, and whether it has recovered there. Only the
 *  global node's merge produces these; a regional coordinator's own
 *  `/api/v1/incidents` answer never sets `groups` at all. */
export interface IncidentGroupRegion {
  region: string;
  id: string;
  title: string;
  state: IncidentState;
  started_at: number;
  resolved_at: number | null;
}

/** A hint, not a merge: the dependent keeps its own row and regions. */
export interface IncidentAttribution {
  check_id: string;
  region: string;
  incident_id: string;
}

/** One outage of one check, folded from however many regions' incidents
 *  describe it — the global tier's read-side fix for a multi-region check
 *  opening one incident per probing region. See `SCALING.md`, "Cross-region
 *  dependency belongs to the global tier". */
export interface IncidentGroup {
  id: string;
  origin_check: string | null;
  title: string;
  state: IncidentState;
  started_at: number;
  /** `null` while any region in the group is still down. */
  resolved_at: number | null;
  regions: IncidentGroupRegion[];
  attributed_to?: IncidentAttribution;
}

export interface IncidentsResponse {
  incidents: Incident[];
  /** Present only on a global node's merged answer. */
  groups?: IncidentGroup[];
}

export function fetchIncidents(limit = 50, since?: number): Promise<IncidentsResponse> {
  let url = `/api/v1/incidents?limit=${limit}`;
  if (since !== undefined) url += `&since=${since}`;
  return getJson<IncidentsResponse>(url);
}

/** One incident with its full update timeline. Public, like the list. */
export function fetchIncident(id: string): Promise<{ incident: Incident }> {
  return getJson<{ incident: Incident }>(
    `/api/v1/incidents/${encodeURIComponent(id)}`,
  );
}

async function incidentWrite(
  url: string,
  method: string,
  body: unknown,
): Promise<Incident> {
  const res = await fetch(url, {
    method,
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `${res.status} ${res.statusText}`);
  }
  return (await res.json()).incident;
}

/** Open an incident by hand — a maintenance notice, or anything no probe can
 *  see. It carries no origin check, so reconciliation never touches it. */
export function createIncident(body: {
  title: string;
  state?: IncidentState;
  started_at?: number;
  global?: boolean;
  checks?: string[];
  body?: string;
  /** Global node only: the region that will own the incident. */
  region?: string;
}): Promise<Incident> {
  return incidentWrite("/api/v1/admin/incidents", "POST", body);
}

/** Edit the operator-owned fields. Deliberately cannot move `state` — post an
 *  update for that, so the timeline always explains the state. */
export function editIncident(
  id: string,
  body: { title: string; started_at: number; global: boolean },
): Promise<Incident> {
  return incidentWrite(
    `/api/v1/admin/incidents/${encodeURIComponent(id)}`,
    "PATCH",
    body,
  );
}

/** Replace the manually attached checks. Origin and dependency attachments are
 *  untouched by this. */
export function setIncidentChecks(
  id: string,
  checks: string[],
): Promise<Incident> {
  return incidentWrite(
    `/api/v1/admin/incidents/${encodeURIComponent(id)}/checks`,
    "PUT",
    {
      checks,
    },
  );
}

/** Post an update. The only way an incident changes state, and the only way it
 *  gets acknowledged. Returns the incident with its refreshed timeline. */
export function addIncidentUpdate(
  id: string,
  state: IncidentState,
  body: string,
): Promise<Incident> {
  return incidentWrite(
    `/api/v1/admin/incidents/${encodeURIComponent(id)}/updates`,
    "POST",
    {
      state,
      body,
    },
  );
}

export function fetchCheckIncidents(
  id: string,
  limit = 20,
): Promise<IncidentsResponse> {
  return getJson<IncidentsResponse>(
    `/api/v1/checks/${encodeURIComponent(id)}/incidents?limit=${limit}`,
  );
}

export function fetchAgents(): Promise<AgentsResponse> {
  return getJson<AgentsResponse>("/api/v1/agents");
}

export function fetchUptime(
  id: string,
  days = 90,
  buckets = 90,
): Promise<UptimeResponse> {
  return getJson<UptimeResponse>(
    `/api/v1/checks/${encodeURIComponent(id)}/uptime?days=${days}&buckets=${buckets}`,
  );
}

export function fetchLatencySparkline(
  id: string,
  hours = 24,
  buckets = 120,
): Promise<LatencySparklineResponse> {
  return getJson<LatencySparklineResponse>(
    `/api/v1/checks/${encodeURIComponent(id)}/latency-sparkline?hours=${hours}&buckets=${buckets}`,
  );
}

/** Every visible check's strip in one request.
 *
 *  The status page draws one per check, so fetching them individually made a
 *  page load cost a request and a database job per check. */
export interface UptimeStripsResponse {
  days: number;
  checks: UptimeResponse[];
}

export function fetchUptimeStrips(
  days = 90,
  buckets = 90,
  section: string | null = null,
  page = 0,
  limit = 100,
): Promise<UptimeStripsResponse> {
  return getJson<UptimeStripsResponse>(
    `/api/v1/uptime?days=${days}&buckets=${buckets}&page=${page}&limit=${limit}${sectionParam(section)}`,
  );
}

/** Scope a batched chart request. Null is the primary page, which the server
 *  reads as "loose checks and inline sections" — a section rendered as its own
 *  page is excluded until that page asks for it by name. */
function sectionParam(section: string | null): string {
  return section === null ? "" : `&section=${encodeURIComponent(section)}`;
}

/** Sparkline points for every check that draws one.
 *
 *  Only `points`: the row chart reads nothing else, and the per-agent breakdown
 *  belongs to the single-check detail view. */
export interface CheckLatencyPoints {
  check_id: string;
  points: LatencyPoint[];
}

export interface LatencyPointsResponse {
  from: number;
  to: number;
  buckets: number;
  checks: CheckLatencyPoints[];
}

export function fetchLatencyPoints(
  hours = 24,
  buckets = 120,
  section: string | null = null,
  page = 0,
  limit = 100,
): Promise<LatencyPointsResponse> {
  return getJson<LatencyPointsResponse>(
    `/api/v1/latency-points?hours=${hours}&buckets=${buckets}&page=${page}&limit=${limit}${sectionParam(section)}`,
  );
}

/** Same series over an explicit window, so a clicked uptime bar can chart the
 *  segment it actually covers instead of the newest rows. */
export function fetchLatencyWindow(
  id: string,
  from: number,
  to: number,
  buckets = 120,
): Promise<LatencySparklineResponse> {
  return getJson<LatencySparklineResponse>(
    `/api/v1/checks/${encodeURIComponent(id)}/latency-sparkline?from=${from}&to=${to}&buckets=${buckets}`,
  );
}

/** Section metadata for the public page. `/api/v1/status` carries none, so the
 *  initial collapsed state has to come from here. Unauthenticated. */
export interface PublicSection {
  name: string;
  position: number;
  collapsed: boolean;
  /** `inline` renders the section's rows here; `page` renders a tile linking to
   *  `#/group/<name>`, where its rows and charts load on demand. */
  display: SectionDisplay;
}

export type SectionDisplay = "inline" | "page";

export function fetchPublicSections(): Promise<PublicSection[]> {
  return getJson<{ sections: PublicSection[] }>("/api/v1/sections").then(
    (r) => r.sections,
  );
}
