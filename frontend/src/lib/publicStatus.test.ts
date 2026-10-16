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
  it('reports healthy services only when every observation is healthy', () => {
    expect(summarizePublicStatus([{ status: 'up' }, { status: 'up' }], []).tone).toBe('operational');
    expect(summarizePublicStatus([{ status: 'up' }, { status: 'unknown' }], []).tone).toBe('unknown');
    expect(summarizePublicStatus([], []).tone).toBe('unknown');
  });

  it('distinguishes degraded service from partial and complete outages', () => {
    expect(summarizePublicStatus([{ status: 'up' }, { status: 'degraded' }], []).tone).toBe('degraded');
    expect(summarizePublicStatus([{ status: 'up' }, { status: 'down' }], []).title).toBe('Partial service outage');
    expect(summarizePublicStatus([{ status: 'down' }, { status: 'down' }], []).title).toBe('Major service outage');
  });

  it('gives actual open incidents priority and does not fabricate an ETA', () => {
    const summary = summarizePublicStatus([{ status: 'up' }], [incident()]);
    expect(summary.title).toBe('DNS service disruption');
    expect(summary.tone).toBe('degraded');
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
