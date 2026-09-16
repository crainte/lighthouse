// Pure reducers for the live status page. The status page mutates three arrays
// in response to websocket events — checks, alerts and incidents — and every
// one of those mutations is a pure function of (previous array, event). Keeping
// them here rather than inline in App.svelte's handlers means the convergence
// rules (a down→up recovery lands on the banner, the alert feed and the
// incident list without a reload) can be unit-tested without mounting a
// component or standing up a DOM. No side effects, no fetches.
import type { Alert, Check, Incident, IncidentEvent, StateChange } from "./api";

// Optimistically apply a live state change to the checks list: flip the status
// and stamp the check as just-seen. Everything else (latency, uptime_24h,
// status code, error) is left alone — the event doesn't carry it, so an
// authoritative /status refresh reconciles those afterwards. A change for a
// check we don't know about is ignored rather than invented.
export function applyStateChange(checks: Check[], msg: StateChange): Check[] {
  return checks.map((c) =>
    c.check_id === msg.check_id
      ? { ...c, status: msg.to, last_check: msg.timestamp }
      : c,
  );
}

// The synthetic alert a live state change prepends to the feed, so the "recent
// alerts" list moves the instant a check flips instead of waiting for the next
// full load. It mirrors the row the server will persist, but carries no
// incident_id: that is stamped server-side and only arrives on a later refresh.
export function alertForStateChange(msg: StateChange): Alert {
  return {
    id: `${msg.check_id}-${msg.timestamp}-${msg.to}`,
    scope: "check",
    node_id: msg.check_id,
    alert_type:
      msg.to === "degraded"
        ? "degradation"
        : msg.to === "up" && (msg.from === "down" || msg.from === "degraded")
          ? "recovery"
          : "state_change",
    previous_state: msg.from,
    current_state: msg.to,
    timestamp: msg.timestamp,
    incident_id: null,
  };
}

export function prependAlert(alerts: Alert[], msg: StateChange): Alert[] {
  return [alertForStateChange(msg), ...alerts];
}

// Fold an incident lifecycle event into the list. `incident_opened` is new and
// goes to the front; `incident_updated` covers everything after the open — a
// check joining, an operator posting, auto-resolve — so it is a replace-by-id.
// An update for an id we've never seen is treated as an open so a client that
// missed the original open (a lagged socket) still shows it rather than dropping
// it; a reconnect resync will re-sort the list authoritatively. `incident_deleted`
// drops it by id — a no-op if it was never in the list, since a peer's
// replicated delete and a local delete both fire the same event and either one
// may race a resync that already dropped it.
export function applyIncidentEvent(
  incidents: Incident[],
  msg: IncidentEvent,
): Incident[] {
  if (msg.type === "incident_deleted") {
    return incidents.filter((i) => i.id !== msg.id);
  }
  if (msg.type === "incident_opened") {
    return [msg.incident, ...incidents.filter((i) => i.id !== msg.incident.id)];
  }
  const known = incidents.some((i) => i.id === msg.incident.id);
  if (!known) return [msg.incident, ...incidents];
  return incidents.map((i) => (i.id === msg.incident.id ? msg.incident : i));
}
