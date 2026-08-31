// Live WebSocket client for /api/v1/ws with exponential reconnect backoff.
import type {
  AffectSource,
  IncidentCheck,
  IncidentEvent,
  IncidentState,
  StateChange,
  Status,
} from "./api";
import { INCIDENT_STATES } from "./api";

const STATUSES: Status[] = ["up", "down", "unknown"];
const AFFECT_SOURCES: AffectSource[] = ["origin", "dependency", "manual"];

function isStatus(v: unknown): v is Status {
  return typeof v === "string" && (STATUSES as string[]).includes(v);
}

function parseStateChange(data: unknown): StateChange | null {
  if (typeof data !== "object" || data === null) return null;
  const m = data as Record<string, unknown>;
  if (m.type !== "state_change") return null;
  if (typeof m.check_id !== "string") return null;
  if (!isStatus(m.from) || !isStatus(m.to)) return null;
  if (typeof m.timestamp !== "number") return null;
  return {
    type: "state_change",
    check_id: m.check_id,
    from: m.from,
    to: m.to,
    timestamp: m.timestamp,
  };
}

function parseIncidentChecks(v: unknown): IncidentCheck[] | null {
  if (!Array.isArray(v)) return null;
  const out: IncidentCheck[] = [];
  for (const raw of v) {
    if (typeof raw !== "object" || raw === null) return null;
    const c = raw as Record<string, unknown>;
    if (typeof c.check_id !== "string") return null;
    if (
      typeof c.source !== "string" ||
      !(AFFECT_SOURCES as string[]).includes(c.source)
    ) {
      return null;
    }
    out.push({ check_id: c.check_id, source: c.source as AffectSource });
  }
  return out;
}

function parseIncident(data: unknown): IncidentEvent | null {
  if (typeof data !== "object" || data === null) return null;
  const m = data as Record<string, unknown>;
  if (m.type === "incident_deleted") {
    // Bare-id payload — reject anything that also looks like an incident so a
    // malformed or mistyped event fails loud (null) instead of being coerced.
    if (typeof m.id !== "string") return null;
    return { type: "incident_deleted", id: m.id };
  }
  if (m.type !== "incident_opened" && m.type !== "incident_updated")
    return null;
  const i = m.incident as Record<string, unknown> | undefined;
  if (typeof i !== "object" || i === null) return null;
  if (typeof i.id !== "string" || typeof i.title !== "string") return null;
  if (
    typeof i.state !== "string" ||
    !(INCIDENT_STATES as string[]).includes(i.state)
  ) {
    return null;
  }
  if (typeof i.started_at !== "number") return null;
  if (i.resolved_at !== null && typeof i.resolved_at !== "number") return null;
  if (typeof i.duration_ms !== "number") return null;
  if (typeof i.global !== "boolean" || typeof i.acknowledged !== "boolean")
    return null;
  if (i.origin_check !== null && typeof i.origin_check !== "string")
    return null;
  const checks = parseIncidentChecks(i.checks);
  if (checks === null) return null;
  return {
    type: m.type,
    incident: {
      id: i.id,
      title: i.title,
      state: i.state as IncidentState,
      started_at: i.started_at,
      resolved_at: i.resolved_at as number | null,
      duration_ms: i.duration_ms,
      global: i.global,
      origin_check: i.origin_check as string | null,
      checks,
      acknowledged: i.acknowledged,
    },
  };
}

export type WsStatus = "connecting" | "open" | "closed";

export interface WsHandlers {
  onStateChange: (msg: StateChange) => void;
  onIncident?: (msg: IncidentEvent) => void;
  onStatus?: (status: WsStatus) => void;
  /** Fired when the socket reopens after a drop — never on the first connect.
   *  The server lets slow clients lag (RecvError::Lagged is skipped), so a
   *  client that was disconnected may have missed events entirely. A resync of
   *  status, incidents and charts on reconnect is the only way to guarantee the
   *  page is not left stale. */
  onReconnect?: () => void;
}

export interface WsConnection {
  close: () => void;
}

export function connectWs(handlers: WsHandlers): WsConnection {
  let ws: WebSocket | null = null;
  let retry = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;
  // The first onopen is the initial connect, not a reconnect. Only opens after
  // a drop should trigger a resync, so the initial page load isn't fetched twice.
  let everOpened = false;

  const setStatus = (s: WsStatus) => handlers.onStatus?.(s);

  const open = () => {
    if (stopped) return;
    setStatus("connecting");
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${proto}//${location.host}/api/v1/ws`);

    ws.onopen = () => {
      retry = 0;
      setStatus("open");
      if (everOpened) handlers.onReconnect?.();
      everOpened = true;
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
      setStatus("closed");
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
