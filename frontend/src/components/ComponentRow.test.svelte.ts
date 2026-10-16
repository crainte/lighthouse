import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/svelte';
import ComponentRow from './ComponentRow.svelte';
import type { Check, LatencyPoint, UptimeResponse } from '../lib/api';

afterEach(() => cleanup());

const mkCheck = (enabled: boolean): Check => ({
  check_id: 'dns', name: 'DNS', status: 'up', latency_ms: 12,
  last_check: Date.now(), uptime_24h: 1, latency_graph: enabled, type: 'dns',
});

const uptime: UptimeResponse = {
  check_id: 'dns', name: 'DNS', days: 1, overall_uptime: 1,
  buckets: [
    { start: 1000, end: 2000, uptime: 1, up: 2, total: 2 },
    { start: 2000, end: 3000, uptime: 1, up: 3, total: 3 },
  ],
};
const sparkline: LatencyPoint[] = [
  { timestamp: 1000, avg_latency_ms: 12, attempts: 1, up: 1 },
  { timestamp: 2000, avg_latency_ms: 20, attempts: 1, up: 1 },
  { timestamp: 3000, avg_latency_ms: 14, attempts: 1, up: 1 },
];

function mount(enabled: boolean, points: LatencyPoint[] | null = sparkline) {
  const onselect = vi.fn();
  const onbarclick = vi.fn();
  const rendered = render(ComponentRow, {
    props: {
      compact: true, check: mkCheck(enabled), uptime, sparkline: points,
      onselect, onbarclick,
    },
  });
  return { ...rendered, onselect, onbarclick };
}

describe('compact public service rows', () => {
  it('renders the original latency chart when enabled', () => {
    const { container } = mount(true);
    expect(screen.getByRole('img', { name: 'Latency history for DNS' })).toBeInTheDocument();
    expect(container.querySelector('.public-service-latency')).toBeInTheDocument();
  });

  it('keeps latency hidden when disabled, even when sparkline samples exist', () => {
    mount(false);
    expect(screen.queryByRole('img', { name: /Latency history/ })).not.toBeInTheDocument();
  });

  it('does not fabricate a graph if fewer than two samples are available', () => {
    mount(true, sparkline.slice(0, 1));
    expect(screen.queryByRole('img', { name: /Latency history/ })).not.toBeInTheDocument();
  });

  it('retains navigation and uptime-bucket selection', async () => {
    const { onselect, onbarclick, container } = mount(true);
    await fireEvent.click(screen.getByRole('button', { name: /DNS/ }));
    expect(onselect).toHaveBeenCalledWith('dns');
    await fireEvent.click(container.querySelector('.public-service-history .ubars')!);
    expect(onbarclick).toHaveBeenCalledWith('dns', expect.objectContaining({ uptime: 1 }));
  });
});
