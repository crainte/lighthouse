// Types + fetch wrappers for the Warden coordinator HTTP API.
// Same-origin in production; /api is proxied to the backend during `vite dev`.

export type Status = 'up' | 'down' | 'unknown';

export type CheckKind = 'http' | 'tcp' | 'dns' | 'icmp' | 'ssl' | 'heartbeat';

export interface Check {
  check_id: string;
  status: Status;
  latency_ms: number | null;
  last_check: number;
  uptime_24h: number;
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
export const UNKNOWN_LOCATION = 'unknown';

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
  status: Status;
  /** Silent long enough to read as decommissioned. Still listed and still owns
   *  its history — retirement is derived from silence, not a state anyone sets. */
  retired: boolean;
  /** How many checks name this agent explicitly. Purging one that is still
   *  named would leave those checks with nothing to run them. */
  assigned_checks: number;
}

export interface AgentsResponse {
  agents: Agent[];
  /** Enabled checks pinned only to agents that have gone silent. Nothing runs
   *  them and nothing else reports it, so this is the one place it surfaces. */
  checks_with_no_live_agent: string[];
}

export interface StatusResponse {
  checks: Check[];
}

export interface HistoryPoint {
  timestamp: number;
  /** Several agents probe the same check, so (timestamp, agent_id) is the key. */
  agent_id: string;
  status: Status;
  latency_ms: number | null;
  status_code?: number;
  error?: string;
}

export interface HistoryResponse {
  check_id: string;
  history: HistoryPoint[];
}

export interface Alert {
  check_id: string;
  from: Status;
  to: Status;
  timestamp: number;
}

export interface AlertsResponse {
  alerts: Alert[];
}

export interface StateChange {
  type: 'state_change';
  check_id: string;
  from: Status;
  to: Status;
  timestamp: number;
}

/** WS lifecycle events for incidents, broadcast alongside state changes. */
export interface IncidentEvent {
  type: 'incident_opened' | 'incident_resolved';
  incident: Incident;
}

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
  days: number;
  overall_uptime: number | null;
  buckets: UptimeBucket[];
}

export interface LatencyPoint {
  timestamp: number;
  /** `null` when the segment holds no attempts. Not the same as a 0ms reply,
   *  and drawing it as one paints an imaginary flat line along the axis. */
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

export function fetchStatus(): Promise<StatusResponse> {
  return getJson<StatusResponse>('/api/v1/status');
}

export function fetchHistory(
  id: string,
  limit = 100,
  from?: number,
  to?: number,
  status?: Status,
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

/** Lifecycle of an outage: opened when a check reads DOWN, resolved when it
 *  reads UP again. Distinct from `Alert`, which is one state transition. */
export type IncidentStatus = 'open' | 'resolved';

export interface Incident {
  id: string;
  check_id: string;
  status: IncidentStatus;
  started_at: number;
  resolved_at: number | null;
  /** Why it went down, recorded at open. */
  details: string | null;
  /** When an admin acknowledged it. `null` until then. */
  acknowledged_at: number | null;
  acknowledged_by: string | null;
}

export interface IncidentsResponse {
  incidents: Incident[];
}

export function fetchIncidents(limit = 50): Promise<IncidentsResponse> {
  return getJson<IncidentsResponse>(`/api/v1/incidents?limit=${limit}`);
}

/** Acknowledge an open incident. Requires an admin session (cookie auth). */
export async function acknowledgeIncident(id: string): Promise<Incident> {
  const res = await fetch(`/api/v1/incidents/${encodeURIComponent(id)}/acknowledge`, {
    method: 'POST',
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()).incident;
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
  return getJson<AgentsResponse>('/api/v1/agents');
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
}

export function fetchPublicSections(): Promise<PublicSection[]> {
  return getJson<{ sections: PublicSection[] }>('/api/v1/sections').then((r) => r.sections);
}
