import { describe, it, expect } from 'vitest';
import {
  statusColor,
  formatUptime,
  formatLatency,
  formatRelativeTime,
  overallStatus,
  worstStatus,
  barColor,
  dayStatusText,
} from './format';

describe('statusColor', () => {
  it('maps each status to a distinct color', () => {
    expect(statusColor('up')).toBe('var(--up)');
    expect(statusColor('down')).toBe('var(--down)');
    expect(statusColor('unknown')).toBe('var(--unknown)');
    expect(statusColor('degraded')).toBe('var(--amber)');
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
  it('ignores disabled checks even when their last state was down', () => {
    expect(overallStatus([{ status: 'up', enabled: true }, { status: 'down', enabled: false }])).toBe('operational');
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
  it('ignores paused checks when summarizing a group', () => {
    expect(worstStatus([{ status: 'up', enabled: true }, { status: 'unknown', enabled: false }])).toBe('up');
    expect(worstStatus([{ status: 'up', enabled: true }, { status: 'down', enabled: false }])).toBe('up');
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
