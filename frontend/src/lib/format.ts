// Pure display/logic helpers. No DOM, no side effects — unit-tested in format.test.ts.
import type { Status, Health, Check, UptimeBucket } from './api';

// Degraded checks and partially healthy fleet rows share the same amber used
// by partial-outage history bars.
const STATUS_COLORS: Record<Status | Health, string> = {
  up: '#3fb950',
  down: '#f85149',
  unknown: '#8b949e',
  degraded: '#d29922',
};

export function statusColor(status: Status | Health): string {
  return STATUS_COLORS[status] ?? STATUS_COLORS.unknown;
}

// Uptime as a percent with at most 2 decimals; exactly "100%" when full.
// Trailing zeros are trimmed (0.987 -> "98.7%", 0.9894 -> "98.94%").
export function formatUptime(fraction: number | null | undefined): string {
  if (fraction == null || Number.isNaN(fraction)) return '—';
  const pct = fraction * 100;
  if (pct >= 100) return '100%';
  return `${parseFloat(pct.toFixed(2))}%`;
}

// Overall system state from all checks, for the status banner.
export type Overall = 'operational' | 'degraded' | 'partial' | 'major';

export function overallStatus(checks: Pick<Check, 'status'>[]): Overall {
  if (checks.length === 0) return 'operational';
  const down = checks.filter((c) => c.status === 'down').length;
  if (down === checks.length) return 'major';
  if (down > 0) return 'partial';
  if (checks.some((c) => c.status === 'degraded')) return 'degraded';
  return 'operational';
}

// Worst status across a set of checks, for a group header: DOWN wins, then
// DEGRADED, then UNKNOWN, else UP. An empty set reads as UP.
export function worstStatus(checks: Pick<Check, 'status'>[]): Status {
  if (checks.some((c) => c.status === 'down')) return 'down';
  if (checks.some((c) => c.status === 'degraded')) return 'degraded';
  if (checks.some((c) => c.status === 'unknown')) return 'unknown';
  return 'up';
}

// Colour class for a single day's uptime bar. `degraded` exists so a day that
// lost a couple of checks out of thousands doesn't look identical to a total
// outage — it matches the wording `dayStatusText` already used.
export type BarKind = 'up' | 'degraded' | 'down' | 'nodata';

export function barColor(bucket: Pick<UptimeBucket, 'uptime'>): BarKind {
  if (bucket.uptime == null) return 'nodata';
  if (bucket.uptime >= 0.9999) return 'up';
  return bucket.uptime > 0 ? 'degraded' : 'down';
}

// Human-readable status for a single day's uptime bucket, for hover tooltips.
export function dayStatusText(
  bucket: Pick<UptimeBucket, 'uptime' | 'total'>,
): string {
  if (bucket.uptime == null || bucket.total === 0) return 'No data';
  if (bucket.uptime >= 0.9999) return 'Operational';
  if (bucket.uptime <= 0) return 'Major outage';
  return `Degraded · ${formatUptime(bucket.uptime)}`;
}

export function formatLatency(ms: number | null | undefined): string {
  if (ms == null || Number.isNaN(ms)) return '—';
  // Probes record whole milliseconds, so a sub-millisecond reply truncates to
  // zero. Saying "0 ms" implies a measurement that never happened; the honest
  // reading is "faster than we can resolve".
  if (ms === 0) return '<1 ms';
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

export function formatRelativeTime(ts: number, now: number = Date.now()): string {
  const diff = now - ts;
  if (diff < 10_000) return 'just now';
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

// A span of milliseconds as "1h 5m" — seconds only while under a minute, days
// once past 24h. Used for incident durations.
export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '—';
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

// A short human explanation of why an attempt failed, or null when there is
// nothing to explain. `error` is the agent's own words; a bare unexpected
// status code is the fallback.
export function failureReason(r: {
  status: Status;
  status_code?: number;
  error?: string;
}): string | null {
  if (r.status === 'up') return null;
  if (r.error) return r.error;
  if (r.status_code != null) return `HTTP ${r.status_code}`;
  return null;
}
