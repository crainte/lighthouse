import { describe, expect, it } from 'vitest';
import type { Alert, Check, Incident, IncidentGroup } from '../lib/api';
import {
  activeIncidentCount,
  alertLabel,
  attentionChecks,
  incidentFeed,
  isUnauthorized,
  observedLabel,
} from './nightwatchOverview';

const check = (status: Check['status']): Check => ({
  check_id: status,
  name: status,
  status,
  latency_ms: null,
  last_check: 0,
  uptime_24h: null,
});

const incident = (id: string, state: Incident['state'], started_at = 1): Incident => ({
  id,
  title: id,
  state,
  started_at,
  resolved_at: null,
  duration_ms: 0,
  global: false,
  origin_check: null,
  checks: [],
  acknowledged: false,
});

describe('nightwatch overview helpers', () => {
  it('selects only checks needing attention', () => {
    expect(attentionChecks([check('up'), check('degraded'), check('down'), check('unknown')]).map((c) => c.status)).toEqual([
      'degraded',
      'down',
      'unknown',
    ]);
  });

  it('labels a partial check page as an observed subset', () => {
    expect(observedLabel(40, 20, true)).toBe('Observed subset: 20 of 40 monitors');
    expect(observedLabel(20, 20, false)).toBe('20 monitored checks');
  });

  it('deduplicates grouped incident regions from the incident feed', () => {
    const group: IncidentGroup = {
      id: 'group-1',
      title: 'Regional issue',
      state: 'investigating',
      started_at: 3,
      resolved_at: null,
      origin_check: null,
      regions: [{ id: 'incident-1', title: 'Local issue', state: 'investigating', started_at: 2, resolved_at: null, region: 'eu' }],
    };
    expect(incidentFeed([incident('incident-1', 'investigating'), incident('incident-2', 'detected')], [group]).map((item) => item.id)).toEqual([
      'group-1',
      'incident-2',
    ]);
    expect(activeIncidentCount([incident('incident-1', 'investigating')], [group])).toBe(1);
  });

  it('formats alert names without inventing detail', () => {
    const alert: Alert = {
      id: 'a', scope: 'check', node_id: 'fallback', name: 'API', alert_type: 'recovery', previous_state: 'down', current_state: 'up', timestamp: 0, incident_id: null,
    };
    expect(alertLabel(alert)).toBe('API recovered');
  });

  it('recognizes only explicit unauthorized errors', () => {
    expect(isUnauthorized({ status: 401 })).toBe(true);
    expect(isUnauthorized({ status: 500 })).toBe(false);
    expect(isUnauthorized(new Error('401'))).toBe(false);
    expect(isUnauthorized(new Error('/api/v1/agents -> 401 Unauthorized'))).toBe(true);
    expect(isUnauthorized(new Error('/api/v1/agents -> 4010 Unknown'))).toBe(false);
  });
});
