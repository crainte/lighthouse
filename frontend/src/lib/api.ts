// Types + fetch wrappers for the Warden coordinator HTTP API.
// Same-origin in production; /api is proxied to the backend during `vite dev`.

export type Status = 'up' | 'down' | 'unknown';

export interface Check {
  check_id: string;
  status: Status;
  latency_ms: number | null;
  last_check: number;
  uptime_24h: number;
}

export interface StatusResponse {
  checks: Check[];
}

export interface HistoryPoint {
  timestamp: number;
  status: Status;
  latency_ms: number | null;
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

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} -> ${res.status} ${res.statusText}`);
  return res.json();
}

export function fetchStatus(): Promise<StatusResponse> {
  return getJson<StatusResponse>('/api/v1/status');
}

export function fetchHistory(id: string, limit = 100): Promise<HistoryResponse> {
  return getJson<HistoryResponse>(
    `/api/v1/checks/${encodeURIComponent(id)}/history?limit=${limit}`,
  );
}

export function fetchAlerts(limit = 50): Promise<AlertsResponse> {
  return getJson<AlertsResponse>(`/api/v1/alerts?limit=${limit}`);
}
