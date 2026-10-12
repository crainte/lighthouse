import { describe, it, expect } from 'vitest';
import { checkLabel, checkNameMap, incidentRegionChoices, openGlobalIncidents, openIncidentForCheck } from './incident';
import type { Incident, IncidentState } from './api';

function inc(over: Partial<Incident> & { id: string }): Incident {
  return {
    title: over.id,
    state: 'detected' as IncidentState,
    started_at: 1_000,
    resolved_at: null,
    duration_ms: 60_000,
    global: false,
    origin_check: null,
    checks: [],
    acknowledged: false,
    ...over,
  };
}

describe('openGlobalIncidents', () => {
  it('keeps only unresolved globals', () => {
    const rows = [
      inc({ id: 'open-global', global: true }),
      inc({ id: 'done-global', global: true, state: 'resolved' }),
      inc({ id: 'open-scoped' }),
    ];
    expect(openGlobalIncidents(rows).map((i) => i.id)).toEqual(['open-global']);
  });

  it('keeps a global with no checks — the maintenance notice has no row to sit on', () => {
    const rows = [inc({ id: 'maintenance', global: true, checks: [] })];
    expect(openGlobalIncidents(rows).map((i) => i.id)).toEqual(['maintenance']);
  });
});

describe('openIncidentForCheck', () => {
  const attached = (id: string, over: Partial<Incident> = {}) =>
    inc({ id, checks: [{ check_id: 'api', source: 'origin' }], ...over });

  it('finds the open incident a check is attached to, whatever attached it', () => {
    const rows = [inc({ id: 'dep', checks: [{ check_id: 'api', source: 'dependency' }] })];
    expect(openIncidentForCheck(rows, 'api')?.id).toBe('dep');
  });

  it('ignores resolved incidents and other checks', () => {
    const rows = [
      attached('done', { state: 'resolved' }),
      inc({ id: 'elsewhere', checks: [{ check_id: 'db', source: 'origin' }] }),
    ];
    expect(openIncidentForCheck(rows, 'api')).toBeNull();
  });

  it('ignores globals — they already render page-wide', () => {
    expect(openIncidentForCheck([attached('g', { global: true })], 'api')).toBeNull();
  });

  it('takes the newest by started_at, not array order', () => {
    const rows = [
      attached('older', { started_at: 1_000 }),
      attached('newer', { started_at: 5_000 }),
    ];
    expect(openIncidentForCheck(rows, 'api')?.id).toBe('newer');
    expect(openIncidentForCheck([...rows].reverse(), 'api')?.id).toBe('newer');
  });
});

describe('checkNameMap / checkLabel', () => {
  it('resolves a mutable name from an immutable id', () => {
    const names = checkNameMap([
      { check_id: 'uuid-1', name: 'API' },
      { check_id: 'uuid-2', name: 'こんにちは' },
    ]);
    expect(checkLabel(names, 'uuid-1')).toBe('API');
    expect(checkLabel(names, 'uuid-2')).toBe('こんにちは');
  });

  it('falls back to the raw id for a check no longer in the list', () => {
    const names = checkNameMap([{ check_id: 'uuid-1', name: 'API' }]);
    expect(checkLabel(names, 'uuid-deleted')).toBe('uuid-deleted');
  });
});

describe('incidentRegionChoices', () => {
  const reg = (name: string, attached = true, stale = false) => ({ name, attached, stale });

  it('offers only regions that are reporting, in name order', () => {
    expect(
      incidentRegionChoices([reg('us'), reg('eu', false), reg('tw'), reg('ap', true, true)]),
    ).toEqual(['tw', 'us']);
  });

  it('is empty when nothing has reported yet', () => {
    expect(incidentRegionChoices([reg('us', false)])).toEqual([]);
  });
});
