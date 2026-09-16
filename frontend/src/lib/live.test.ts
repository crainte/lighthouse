import { describe, it, expect } from "vitest";
import {
  applyStateChange,
  alertForStateChange,
  prependAlert,
  applyIncidentEvent,
} from "./live";
import { overallStatus } from "./format";
import type { Check, Incident, IncidentEvent, StateChange } from "./api";

function check(id: string, status: Check["status"]): Check {
  return {
    check_id: id,
    name: id,
    status,
    latency_ms: 5,
    last_check: 1000,
    uptime_24h: 1,
  };
}

function stateChange(partial: Partial<StateChange>): StateChange {
  return {
    type: "state_change",
    check_id: "api",
    from: "up",
    to: "down",
    timestamp: 2000,
    ...partial,
  };
}

function incident(
  id: string,
  state: Incident["state"],
  extra: Partial<Incident> = {},
): Incident {
  return {
    id,
    title: `Incident ${id}`,
    state,
    started_at: 1000,
    resolved_at: state === "resolved" ? 2000 : null,
    duration_ms: 1000,
    global: false,
    origin_check: "api",
    checks: [{ check_id: "api", source: "origin" }],
    acknowledged: false,
    ...extra,
  };
}

describe("applyStateChange", () => {
  it("flips the matching check status and stamps last_check", () => {
    const before = [check("api", "up"), check("db", "up")];
    const after = applyStateChange(
      before,
      stateChange({ check_id: "api", to: "down", timestamp: 2000 }),
    );
    expect(after.find((c) => c.check_id === "api")?.status).toBe("down");
    expect(after.find((c) => c.check_id === "api")?.last_check).toBe(2000);
    expect(after.find((c) => c.check_id === "db")?.status).toBe("up");
  });

  it("leaves non-status fields untouched for later reconciliation", () => {
    const before = [check("api", "up")];
    const after = applyStateChange(before, stateChange({ to: "down" }));
    // latency_ms and uptime_24h are not in the event, so they must survive.
    expect(after[0].latency_ms).toBe(5);
    expect(after[0].uptime_24h).toBe(1);
  });

  it("ignores a change for an unknown check", () => {
    const before = [check("api", "up")];
    const after = applyStateChange(
      before,
      stateChange({ check_id: "ghost", to: "down" }),
    );
    expect(after).toEqual(before);
  });

  it("does not mutate the input array", () => {
    const before = [check("api", "up")];
    applyStateChange(before, stateChange({ to: "down" }));
    expect(before[0].status).toBe("up");
  });

  it("drives the overall banner through degraded, partial, and recovery", () => {
    let checks = [check("api", "up"), check("db", "up")];
    expect(overallStatus(checks)).toBe("operational");
    checks = applyStateChange(
      checks,
      stateChange({ check_id: "api", from: "up", to: "degraded" }),
    );
    expect(overallStatus(checks)).toBe("degraded");
    checks = applyStateChange(
      checks,
      stateChange({ check_id: "api", from: "degraded", to: "down" }),
    );
    expect(overallStatus(checks)).toBe("partial");
    checks = applyStateChange(
      checks,
      stateChange({ check_id: "api", from: "down", to: "up" }),
    );
    expect(overallStatus(checks)).toBe("operational");
  });
});

describe("alertForStateChange", () => {
  it("labels down/degraded→up transitions as recoveries", () => {
    expect(
      alertForStateChange(stateChange({ from: "down", to: "up" })).alert_type,
    ).toBe("recovery");
    expect(
      alertForStateChange(stateChange({ from: "degraded", to: "up" })).alert_type,
    ).toBe("recovery");
  });

  it("labels transitions into degraded as degradation", () => {
    expect(
      alertForStateChange(stateChange({ from: "up", to: "degraded" })).alert_type,
    ).toBe("degradation");
  });

  it("labels anything else a state_change", () => {
    expect(
      alertForStateChange(stateChange({ from: "up", to: "down" })).alert_type,
    ).toBe("state_change");
    expect(
      alertForStateChange(stateChange({ from: "unknown", to: "up" }))
        .alert_type,
    ).toBe("state_change");
  });

  it("carries no incident_id — that is stamped server-side", () => {
    expect(alertForStateChange(stateChange({})).incident_id).toBeNull();
  });

  it("prepends the new alert, newest first", () => {
    const existing = prependAlert([], stateChange({ timestamp: 1000 }));
    const next = prependAlert(existing, stateChange({ timestamp: 2000 }));
    expect(next).toHaveLength(2);
    expect(next[0].timestamp).toBe(2000);
  });
});

describe("applyIncidentEvent", () => {
  it("prepends a newly opened incident", () => {
    const opened: IncidentEvent = {
      type: "incident_opened",
      incident: incident("i1", "detected"),
    };
    const after = applyIncidentEvent([], opened);
    expect(after).toHaveLength(1);
    expect(after[0].id).toBe("i1");
  });

  it("does not duplicate an open for an id already present", () => {
    const existing = [incident("i1", "detected")];
    const opened: IncidentEvent = {
      type: "incident_opened",
      incident: incident("i1", "investigating"),
    };
    const after = applyIncidentEvent(existing, opened);
    expect(after).toHaveLength(1);
    expect(after[0].state).toBe("investigating");
  });

  it("replaces by id on update, keeping list order", () => {
    const existing = [incident("i1", "detected"), incident("i2", "monitoring")];
    const updated: IncidentEvent = {
      type: "incident_updated",
      incident: incident("i1", "resolved"),
    };
    const after = applyIncidentEvent(existing, updated);
    expect(after.map((i) => i.id)).toEqual(["i1", "i2"]);
    expect(after.find((i) => i.id === "i1")?.state).toBe("resolved");
  });

  it("treats an update for an unknown id as an open (lagged socket recovery)", () => {
    const updated: IncidentEvent = {
      type: "incident_updated",
      incident: incident("i9", "monitoring"),
    };
    const after = applyIncidentEvent([incident("i1", "detected")], updated);
    expect(after.map((i) => i.id)).toEqual(["i9", "i1"]);
  });

  it("an auto-resolve arrives as an update and flips state to resolved without a reload", () => {
    let incidents = [incident("i1", "detected")];
    incidents = applyIncidentEvent(incidents, {
      type: "incident_updated",
      incident: incident("i1", "resolved"),
    });
    expect(incidents[0].state).toBe("resolved");
    expect(incidents[0].resolved_at).not.toBeNull();
  });

  it("drops the incident by id on delete", () => {
    const existing = [incident("i1", "detected"), incident("i2", "monitoring")];
    const deleted: IncidentEvent = { type: "incident_deleted", id: "i1" };
    const after = applyIncidentEvent(existing, deleted);
    expect(after.map((i) => i.id)).toEqual(["i2"]);
  });

  it("is a no-op deleting an unknown id", () => {
    const existing = [incident("i1", "detected")];
    const deleted: IncidentEvent = { type: "incident_deleted", id: "i9" };
    const after = applyIncidentEvent(existing, deleted);
    expect(after).toEqual(existing);
  });
});
