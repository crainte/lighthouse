import { describe, expect, it } from 'vitest';
import type { Incident } from './api';
import { summarizePublicStatus } from './publicStatus';

const incident = (overrides: Partial<Incident> = {}): Incident => ({
  id: 'incident-1', title: 'DNS service disruption',
  state: 'investigating', started_at: 123, resolved_at: null,
  duration_ms: 10000, global: false, origin_check: 'dns', checks: [{ check_id: 'dns', source: 'origin' }],
  acknowledged: true, ...overrides,
});

describe('public status summary', () => {
  it('never calls an empty global-only deployment healthy or an outage', () => {
    const waiting = summarizePublicStatus([], [], [], true);
    expect(waiting).toEqual({
      tone: 'unknown',
      title: 'Waiting for regional reports',
      detail: expect.stringContaining('no regional coordinators are reporting'),
      incident: null,
    });
    // Regular coordinators with no checks have a distinct, generic no-data state.
    expect(summarizePublicStatus([], []).title).toBe('No status data available');
  });

  it('reports healthy services only when every observation is healthy', () => {
    expect(summarizePublicStatus([{ status: 'up' }, { status: 'up' }], []).tone).toBe('operational');
    expect(summarizePublicStatus([{ status: 'up' }, { status: 'unknown' }], []).tone).toBe('unknown');
    expect(summarizePublicStatus([], []).tone).toBe('unknown');
  });

  it('ignores deliberately disabled monitors, including stale failures and unknowns', () => {
    const status = summarizePublicStatus([
      { status: 'up', enabled: true },
      { status: 'unknown', enabled: false },
      { status: 'down', enabled: false },
    ], []);
    expect(status.tone).toBe('operational');
    expect(status.title).toBe('All systems operational');
  });

  it('does not invent an all-clear when every visible monitor is disabled', () => {
    const status = summarizePublicStatus([{ status: 'unknown', enabled: false }], []);
    expect(status.tone).toBe('unknown');
    expect(status.title).toBe('No status data available');
  });

  it('distinguishes degraded service from partial and complete outages', () => {
    expect(summarizePublicStatus([{ status: 'up' }, { status: 'degraded' }], []).tone).toBe('degraded');
    expect(summarizePublicStatus([{ status: 'up' }, { status: 'down' }], []).title).toBe('Partial service outage');
    expect(summarizePublicStatus([{ status: 'down' }, { status: 'down' }], []).title).toBe('Major service outage');
  });

  it('includes server-published sections hidden behind their own pages', () => {
    expect(summarizePublicStatus([{ status: 'up' }], [], [{ status: 'down' }]).tone).toBe('outage');
    expect(summarizePublicStatus([{ status: 'up' }], [], [{ status: 'degraded' }]).tone).toBe('degraded');
    expect(summarizePublicStatus([{ status: 'up' }], [], [{ status: 'unknown' }]).tone).toBe('unknown');
  });

  it('ignores a section whose members are all paused', () => {
    const status = summarizePublicStatus([{ status: 'up' }], [], [
      { status: 'unknown', checks: 3, paused: 3 },
      { status: 'down', checks: 4, paused: 0 },
    ]);
    expect(status.tone).toBe('outage');
    expect(summarizePublicStatus([{ status: 'up' }], [], [{ status: 'unknown', checks: 3, paused: 3 }]).tone).toBe('operational');
  });

  it('gives actual open incidents priority and does not fabricate an ETA', () => {
    const summary = summarizePublicStatus([{ status: 'up' }], [incident()]);
    expect(summary.title).toBe('DNS service disruption');
    expect(summary.tone).toBe('incident');
    expect(summary.incident?.state).toBe('investigating');
    expect(summary.detail).not.toMatch(/ETA|resolved by|estimated recovery/i);
  });

  it('does not treat a resolved incident as ongoing', () => {
    const summary = summarizePublicStatus([{ status: 'up' }], [
      incident({ state: 'resolved', resolved_at: 145 }),
    ]);
    expect(summary.tone).toBe('operational');
    expect(summary.incident).toBeNull();
  });
});
