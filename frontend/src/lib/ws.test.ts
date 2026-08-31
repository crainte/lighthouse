import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { connectWs } from "./ws";
import type { WsStatus } from "./ws";

// A minimal fake WebSocket that lets a test drive open/message/close by hand.
// connectWs reaches for the global `WebSocket` and `location`, so both are
// injected here — the test env is `node`, with no DOM.
class FakeWebSocket {
  static instances: FakeWebSocket[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  sent: unknown[] = [];
  closed = false;

  constructor(public url: string) {
    FakeWebSocket.instances.push(this);
  }

  // Test drivers.
  emitOpen() {
    this.onopen?.();
  }
  emitMessage(obj: unknown) {
    this.onmessage?.({ data: JSON.stringify(obj) });
  }
  // Simulate the server/network dropping the connection.
  drop() {
    this.closed = true;
    this.onclose?.();
  }
  close() {
    this.closed = true;
  }
}

const originalWebSocket = globalThis.WebSocket;
const originalLocation = (globalThis as { location?: unknown }).location;

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
  (globalThis as unknown as { WebSocket: unknown }).WebSocket = FakeWebSocket;
  (globalThis as unknown as { location: unknown }).location = {
    protocol: "http:",
    host: "example.test",
  };
});

afterEach(() => {
  vi.useRealTimers();
  (globalThis as unknown as { WebSocket: unknown }).WebSocket =
    originalWebSocket;
  (globalThis as unknown as { location: unknown }).location = originalLocation;
});

function latest(): FakeWebSocket {
  return FakeWebSocket.instances[FakeWebSocket.instances.length - 1];
}

describe("connectWs", () => {
  it("connects to the ws:// endpoint under an http origin", () => {
    connectWs({ onStateChange: () => {} });
    expect(latest().url).toBe("ws://example.test/api/v1/ws");
  });

  it("uses wss:// under an https origin", () => {
    (globalThis as unknown as { location: unknown }).location = {
      protocol: "https:",
      host: "secure.test",
    };
    connectWs({ onStateChange: () => {} });
    expect(latest().url).toBe("wss://secure.test/api/v1/ws");
  });

  it("reports status transitions: connecting → open → closed", () => {
    const statuses: WsStatus[] = [];
    connectWs({ onStateChange: () => {}, onStatus: (s) => statuses.push(s) });
    expect(statuses).toEqual(["connecting"]);
    latest().emitOpen();
    expect(statuses).toEqual(["connecting", "open"]);
    latest().drop();
    expect(statuses.at(-1)).toBe("closed");
  });

  it("does NOT fire onReconnect on the first open", () => {
    const onReconnect = vi.fn();
    connectWs({ onStateChange: () => {}, onReconnect });
    latest().emitOpen();
    expect(onReconnect).not.toHaveBeenCalled();
  });

  it("fires onReconnect when the socket reopens after a drop", () => {
    const onReconnect = vi.fn();
    connectWs({ onStateChange: () => {}, onReconnect });
    latest().emitOpen(); // initial connect
    latest().drop(); // network drop → schedules a reconnect

    // Backoff timer elapses and a new socket is created.
    vi.advanceTimersByTime(10_000);
    expect(FakeWebSocket.instances.length).toBe(2);

    latest().emitOpen(); // the reconnect
    expect(onReconnect).toHaveBeenCalledTimes(1);
  });

  it("fires onReconnect again on a second reconnect", () => {
    const onReconnect = vi.fn();
    connectWs({ onStateChange: () => {}, onReconnect });
    latest().emitOpen();
    latest().drop();
    vi.advanceTimersByTime(10_000);
    latest().emitOpen();
    latest().drop();
    vi.advanceTimersByTime(10_000);
    latest().emitOpen();
    expect(onReconnect).toHaveBeenCalledTimes(2);
  });

  it("routes a state_change message to onStateChange", () => {
    const onStateChange = vi.fn();
    connectWs({ onStateChange });
    latest().emitOpen();
    latest().emitMessage({
      type: "state_change",
      check_id: "api",
      from: "up",
      to: "down",
      timestamp: 5,
    });
    expect(onStateChange).toHaveBeenCalledWith({
      type: "state_change",
      check_id: "api",
      from: "up",
      to: "down",
      timestamp: 5,
    });
  });

  it("does not reconnect after close() is called", () => {
    const conn = connectWs({ onStateChange: () => {} });
    latest().emitOpen();
    conn.close();
    latest().drop();
    vi.advanceTimersByTime(10_000);
    // No new socket created after an intentional close.
    expect(FakeWebSocket.instances.length).toBe(1);
  });

  it("routes an incident_deleted message to onIncident with just the id", () => {
    const onIncident = vi.fn();
    connectWs({ onStateChange: () => {}, onIncident });
    latest().emitOpen();
    latest().emitMessage({ type: "incident_deleted", id: "i1" });
    expect(onIncident).toHaveBeenCalledWith({
      type: "incident_deleted",
      id: "i1",
    });
  });

  it("rejects an incident_deleted message with no string id", () => {
    const onIncident = vi.fn();
    connectWs({ onStateChange: () => {}, onIncident });
    latest().emitOpen();
    latest().emitMessage({ type: "incident_deleted" });
    latest().emitMessage({ type: "incident_deleted", id: 42 });
    expect(onIncident).not.toHaveBeenCalled();
  });

  it("rejects an incident_opened message carrying only an id", () => {
    const onIncident = vi.fn();
    connectWs({ onStateChange: () => {}, onIncident });
    latest().emitOpen();
    latest().emitMessage({ type: "incident_opened", id: "i1" });
    expect(onIncident).not.toHaveBeenCalled();
  });
});
