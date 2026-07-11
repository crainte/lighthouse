// Live WebSocket client for /api/v1/ws with exponential reconnect backoff.
import type { StateChange, Status } from './api';

const STATUSES: Status[] = ['up', 'down', 'unknown'];

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

export type WsStatus = 'connecting' | 'open' | 'closed';

export interface WsHandlers {
  onStateChange: (msg: StateChange) => void;
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
