import { describe, it, expect } from 'vitest';
import {
  statusColor,
  formatUptime,
  formatLatency,
  formatRelativeTime,
  sortChecks,
  overallStatus,
  worstStatus,
  barColor,
  dayStatusText,
} from './format';
import type { Check } from './api';

describe('statusColor', () => {
  it('maps each status to a distinct color', () => {
    expect(statusColor('up')).toBe('#3fb950');
    expect(statusColor('down')).toBe('#f85149');
    expect(statusColor('unknown')).toBe('#8b949e');
    expect(statusColor('degraded')).toBe('#d29922');
  });
});

describe('formatUptime', () => {
  it('renders at most 2 decimals, trimming trailing zeros', () => {
    expect(formatUptime(0.987)).toBe('98.7%');
    expect(formatUptime(0.9894)).toBe('98.94%');
    expect(formatUptime(0.9999)).toBe('99.99%');
    expect(formatUptime(0.5)).toBe('50%');
    expect(formatUptime(0)).toBe('0%');
  });
  it('renders exactly "100%" when full', () => {
    expect(formatUptime(1)).toBe('100%');
  });
  it('renders a dash for missing values', () => {
    expect(formatUptime(null)).toBe('—');
    expect(formatUptime(undefined)).toBe('—');
  });
});

describe('overallStatus', () => {
  it('is operational when no checks or none down', () => {
    expect(overallStatus([])).toBe('operational');
    expect(overallStatus([{ status: 'up' }, { status: 'unknown' }])).toBe('operational');
  });
  it('is partial when some but not all are down', () => {
    expect(overallStatus([{ status: 'up' }, { status: 'down' }])).toBe('partial');
  });
  it('is degraded when any check is degraded and none are down', () => {
    expect(overallStatus([{ status: 'up' }, { status: 'degraded' }])).toBe('degraded');
    expect(overallStatus([{ status: 'degraded' }, { status: 'degraded' }])).toBe('degraded');
  });
  it('is major when all are down', () => {
    expect(overallStatus([{ status: 'down' }, { status: 'down' }])).toBe('major');
  });
});

describe('worstStatus', () => {
  it('ranks down above degraded above unknown above up', () => {
    expect(worstStatus([])).toBe('up');
    expect(worstStatus([{ status: 'up' }, { status: 'unknown' }])).toBe('unknown');
    expect(worstStatus([{ status: 'unknown' }, { status: 'degraded' }])).toBe('degraded');
    expect(worstStatus([{ status: 'degraded' }, { status: 'down' }])).toBe('down');
  });
});

describe('barColor', () => {
  it('is nodata for a null-uptime day', () => {
    expect(barColor({ uptime: null })).toBe('nodata');
  });
  it('is up only for a full day', () => {
    expect(barColor({ uptime: 1 })).toBe('up');
    expect(barColor({ uptime: 0.9999 })).toBe('up');
  });
  it('is degraded for partial downtime, not a full outage', () => {
    // 2 failures out of ~1000 checks must not look like a total outage.
    expect(barColor({ uptime: 0.9981 })).toBe('degraded');
    expect(barColor({ uptime: 0.5 })).toBe('degraded');
  });
  it('is down only when nothing passed', () => {
    expect(barColor({ uptime: 0 })).toBe('down');
  });
});

describe('dayStatusText', () => {
  it('labels a day by its uptime', () => {
    expect(dayStatusText({ uptime: null, total: 0 })).toBe('No data');
    expect(dayStatusText({ uptime: 1, total: 6 })).toBe('Operational');
    expect(dayStatusText({ uptime: 0, total: 6 })).toBe('Major outage');
    expect(dayStatusText({ uptime: 0.5, total: 6 })).toBe('Degraded · 50%');
  });
});

describe('formatLatency', () => {
  it('renders sub-second latency in ms', () => {
    expect(formatLatency(42)).toBe('42 ms');
    expect(formatLatency(999)).toBe('999 ms');
  });
  it('reports a truncated sub-millisecond reading as below the resolution', () => {
    // Probes record whole milliseconds, so 0 means "faster than we can measure",
    // not "took no time at all".
    expect(formatLatency(0)).toBe('<1 ms');
  });
  it('renders second-scale latency in seconds', () => {
    expect(formatLatency(1500)).toBe('1.50 s');
    expect(formatLatency(1000)).toBe('1.00 s');
  });
  it('renders a dash for missing values', () => {
    expect(formatLatency(null)).toBe('—');
    expect(formatLatency(undefined)).toBe('—');
  });
});

describe('formatRelativeTime', () => {
  const now = 1_720_000_000_000;
  it('renders recent times as "just now"', () => {
    expect(formatRelativeTime(now, now)).toBe('just now');
    expect(formatRelativeTime(now - 5_000, now)).toBe('just now');
  });
  it('renders seconds, minutes, hours, days', () => {
    expect(formatRelativeTime(now - 30_000, now)).toBe('30s ago');
    expect(formatRelativeTime(now - 5 * 60_000, now)).toBe('5m ago');
    expect(formatRelativeTime(now - 3 * 3_600_000, now)).toBe('3h ago');
    expect(formatRelativeTime(now - 2 * 86_400_000, now)).toBe('2d ago');
  });
  it('treats future timestamps as "just now"', () => {
    expect(formatRelativeTime(now + 10_000, now)).toBe('just now');
  });
});

describe('sortChecks', () => {
  // check_id is the immutable identity (opaque, unrelated to display order);
  // `name` is the mutable label sorting actually reads.
  const checks: Check[] = [
    { check_id: 'id-2', name: 'beta', status: 'up', latency_ms: 10, last_check: 3, uptime_24h: 0.9 },
    { check_id: 'id-4', name: 'alpha', status: 'down', latency_ms: 20, last_check: 1, uptime_24h: 0.5 },
    { check_id: 'id-1', name: 'gamma', status: 'unknown', latency_ms: 30, last_check: 2, uptime_24h: 0.99 },
    { check_id: 'id-3', name: 'delta', status: 'degraded', latency_ms: 15, last_check: 4, uptime_24h: 0.8 },
  ];

  it('sorts by name ascending and descending without mutating input', () => {
    const asc = sortChecks(checks, 'name', 'asc');
    expect(asc.map((c) => c.name)).toEqual(['alpha', 'beta', 'delta', 'gamma']);
    const desc = sortChecks(checks, 'name', 'desc');
    expect(desc.map((c) => c.name)).toEqual(['gamma', 'delta', 'beta', 'alpha']);
    // input untouched
    expect(checks.map((c) => c.name)).toEqual(['beta', 'alpha', 'gamma', 'delta']);
  });

  it('sorts by status rank up < unknown < degraded < down ascending', () => {
    const asc = sortChecks(checks, 'status', 'asc');
    expect(asc.map((c) => c.status)).toEqual(['up', 'unknown', 'degraded', 'down']);
    const desc = sortChecks(checks, 'status', 'desc');
    expect(desc.map((c) => c.status)).toEqual(['down', 'degraded', 'unknown', 'up']);
  });

  it('sorts by uptime numerically', () => {
    const asc = sortChecks(checks, 'uptime', 'asc');
    expect(asc.map((c) => c.uptime_24h)).toEqual([0.5, 0.8, 0.9, 0.99]);
    const desc = sortChecks(checks, 'uptime', 'desc');
    expect(desc.map((c) => c.uptime_24h)).toEqual([0.99, 0.9, 0.8, 0.5]);
  });

  it('ranks unmeasured checks last in both directions', () => {
    const list = [
      { check_id: 'measured-bad', name: 'a', status: 'down', latency_ms: 1, last_check: 1, uptime_24h: 0 },
      { check_id: 'unmeasured', name: 'b', status: 'up', latency_ms: 0, last_check: 0, uptime_24h: null },
      { check_id: 'measured-good', name: 'c', status: 'up', latency_ms: 1, last_check: 1, uptime_24h: 1 },
    ] as Parameters<typeof sortChecks>[0];

    expect(sortChecks(list, 'uptime', 'asc').map((c) => c.check_id)).toEqual([
      'measured-bad',
      'measured-good',
      'unmeasured',
    ]);
    expect(sortChecks(list, 'uptime', 'desc').map((c) => c.check_id)).toEqual([
      'measured-good',
      'measured-bad',
      'unmeasured',
    ]);
  });
});
