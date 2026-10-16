import { describe, expect, it } from 'vitest';
import { blank, fromCheck, normaliseLegacyCron } from './draft';
import type { AdminCheck } from '../../lib/admin';

function check(dns: AdminCheck['dns']): AdminCheck {
  return {
    id: '018f0000-0000-7000-8000-000000000001',
    name: 'resolver',
    type: 'dns',
    target: '.',
    hosts: [],
    host_match: 'all',
    cron: '*/30 * * * * *',
    timeout_secs: 10,
    latency_graph: false,
    expect: {},
    enabled: true,
    visible: true,
    notify: true,
    notifiers: [],
    position: 0,
    placement: { zones: [], regions: [], per_zone: 1 },
    agents: [],
    default_status: 'unknown',
    headers: [],
    allow_self_signed: false,
    dns,
    ping: { ip_type: 'any', count: 1 },
    metric: { name: '', labels: [] },
    cert_warning_days: 14,
    grace_secs: 300,
    failures_before_down: 1,
    rollup: {},
  };
}

describe('DNS check drafts', () => {
  it('defaults new checks to ordinary DNS lookups', () => {
    expect(blank().dns.mode).toBe('standard');
  });

  it('round-trips DNSSEC mode and nameserver', () => {
    const draft = fromCheck(
      check({ mode: 'dnssec', nameserver: '10.0.0.53:5353', record: 'A', match_type: 'all', expected: [] }),
    );
    expect(draft.dns.mode).toBe('dnssec');
    expect(draft.dns.nameserver).toBe('10.0.0.53:5353');
  });

  it('defaults documents without a mode for compatibility', () => {
    const legacy = check({ mode: 'standard', record: 'A', match_type: 'all', expected: [] });
    delete (legacy.dns as Partial<AdminCheck['dns']>).mode;
    expect(fromCheck(legacy).dns.mode).toBe('standard');
  });
});

describe('legacy cron compatibility', () => {
  it('repairs exactly the invalid 60-second and 60-minute increments', () => {
    expect(normaliseLegacyCron('*/60 * * * * *')).toBe('0 * * * * *');
    expect(normaliseLegacyCron('0 */60 * * * *')).toBe('0 0 * * * *');
    expect(normaliseLegacyCron('*/60 * * * *')).toBe('0 * * * *');
    expect(normaliseLegacyCron('*/61 * * * * *')).toBe('*/61 * * * * *');
  });

  it('repairs legacy saved checks on edit without modifying other cron schedules', () => {
    const existing = check({ mode: 'standard', record: 'A', match_type: 'all', expected: [] });
    existing.cron = '0 */60 * * * *';
    expect(fromCheck(existing).cron).toBe('0 0 * * * *');
    existing.cron = '*/30 * * * * *';
    expect(fromCheck(existing).cron).toBe('*/30 * * * * *');
  });
});

