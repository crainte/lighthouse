// Pure display/logic helpers. No DOM, no side effects — unit-tested in format.test.ts.
import type { Status, Check } from './api';

export type SortKey = 'name' | 'status' | 'uptime';
export type SortDir = 'asc' | 'desc';

const STATUS_COLORS: Record<Status, string> = {
  up: '#3fb950',
  down: '#f85149',
  unknown: '#8b949e',
};

export function statusColor(status: Status): string {
  return STATUS_COLORS[status] ?? STATUS_COLORS.unknown;
}

export function formatUptime(fraction: number | null | undefined): string {
  if (fraction == null || Number.isNaN(fraction)) return '—';
  return `${(fraction * 100).toFixed(1)}%`;
}

export function formatLatency(ms: number | null | undefined): string {
  if (ms == null || Number.isNaN(ms)) return '—';
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

// Lower rank sorts first ascending: healthy first, problems last.
const STATUS_RANK: Record<Status, number> = { up: 0, unknown: 1, down: 2 };

export function sortChecks(list: Check[], key: SortKey, dir: SortDir): Check[] {
  const sign = dir === 'desc' ? -1 : 1;
  return [...list].sort((a, b) => {
    let cmp: number;
    if (key === 'name') cmp = a.check_id.localeCompare(b.check_id);
    else if (key === 'status') cmp = STATUS_RANK[a.status] - STATUS_RANK[b.status];
    else cmp = a.uptime_24h - b.uptime_24h;
    return cmp * sign;
  });
}
