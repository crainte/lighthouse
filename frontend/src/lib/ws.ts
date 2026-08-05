// Live WebSocket client for /api/v1/ws with exponential reconnect backoff.
import type { IncidentEvent, IncidentStatus, StateChange, Status } from './api';

const STATUSES: Status[] = ['up', 'down', 'unknown'];
const INCIDENT_STATUSES: IncidentStatus[] = ['open', 'resolved'];

function isStatus(v: unknown): v is Status {
  return typeof v === 'string' && (STATUSES as string[]).includes(v);
}

function parseStateChange(data: unknown): StateChange | null {
  if (typeof data !== 'object' || data === null) return null;
  const m = data as Record<string, unknown>;
  if (m.type !== 'state_change') return null;
  if (typeof m.check_id !== 'string') return null;
  if (!isStatus(m.from) || !isStatus(m.to)) return null;
  if (typeof m.timestamp !== 'number') return null;
  return {
    type: 'state_change',
    check_id: m.check_id,
    from: m.from,
    to: m.to,
    timestamp: m.timestamp,
  };
}

function parseIncident(data: unknown): IncidentEvent | null {
  if (typeof data !== 'object' || data === null) return null;
  const m = data as Record<string, unknown>;
  if (m.type !== 'incident_opened' && m.type !== 'incident_resolved') return null;
  const i = m.incident as Record<string, unknown> | undefined;
  if (typeof i !== 'object' || i === null) return null;
  if (typeof i.id !== 'string' || typeof i.check_id !== 'string') return null;
  if (typeof i.status !== 'string' || !(INCIDENT_STATUSES as string[]).includes(i.status)) {
    return null;
  }
  if (typeof i.started_at !== 'number') return null;
  if (i.resolved_at !== null && typeof i.resolved_at !== 'number') return null;
  if (i.details !== null && typeof i.details !== 'string') return null;
  return {
    type: m.type,
    incident: {
      id: i.id,
      check_id: i.check_id,
      status: i.status as IncidentStatus,
      started_at: i.started_at,
      resolved_at: i.resolved_at as number | null,
      details: i.details as string | null,
    },
  };
}

export type WsStatus = 'connecting' | 'open' | 'closed';

export interface WsHandlers {
  onStateChange: (msg: StateChange) => void;
  onIncident?: (msg: IncidentEvent) => void;
  onStatus?: (status: WsStatus) => void;
}

export interface WsConnection {
  close: () => void;
}

export function connectWs(handlers: WsHandlers): WsConnection {
  let ws: WebSocket | null = null;
  let retry = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;

  const setStatus = (s: WsStatus) => handlers.onStatus?.(s);

  const open = () => {
    if (stopped) return;
    setStatus('connecting');
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${proto}//${location.host}/api/v1/ws`);

    ws.onopen = () => {
      retry = 0;
      setStatus('open');
    };

    ws.onmessage = (ev) => {
      let data: unknown;
      try {
        data = JSON.parse(ev.data);
      } catch {
        return;
      }
      const msg = parseStateChange(data);
      if (msg) handlers.onStateChange(msg);
      const inc = parseIncident(data);
      if (inc) handlers.onIncident?.(inc);
    };

    ws.onclose = () => {
      setStatus('closed');
      if (stopped) return;
      // Exponential backoff capped at 10s.
      const delay = Math.min(1000 * 2 ** retry, 10_000);
      retry += 1;
      timer = setTimeout(open, delay);
    };

    ws.onerror = () => ws?.close();
  };

  open();

  return {
    close() {
      stopped = true;
      if (timer) clearTimeout(timer);
      ws?.close();
    },
  };
}
