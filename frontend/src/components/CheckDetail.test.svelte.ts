import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen } from '@testing-library/svelte';
import CheckDetail from './CheckDetail.svelte';
import { fetchCheckIncidents, fetchHistory, fetchLatencyWindow, fetchUptime } from '../lib/api';
import type { Check, HistoryPoint } from '../lib/api';

vi.mock('../lib/api', () => ({
  fetchCheckIncidents: vi.fn(),
  fetchHistory: vi.fn(),
  fetchLatencyWindow: vi.fn(),
  fetchUptime: vi.fn(),
}));

afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});

describe('public check detail', () => {
  it('shows a neutral Paused chip instead of a disabled check’s last status while retaining its history', async () => {
    const check: Check = {
      check_id: 'dns', name: 'DNS', status: 'down', enabled: false,
      latency_ms: null, last_check: 1_000, uptime_24h: 0,
    };
    const failure: HistoryPoint = {
      timestamp: Date.now() - 60_000, agent_id: 'agent-1', status: 'down',
      latency_ms: null, error: 'historical timeout',
    };
    vi.mocked(fetchUptime).mockResolvedValue({
      check_id: 'dns', name: 'DNS', days: 1, overall_uptime: 0,
      buckets: [{ start: 1, end: 2, uptime: 0, up: 0, total: 1 }],
    });
    vi.mocked(fetchLatencyWindow).mockResolvedValue({
      check_id: 'dns', name: 'DNS', from: 1, to: 2, buckets: 1, points: [],
      overall: { attempts: 1, up: 0, min_ms: null, avg_ms: null, max_ms: null },
      agents: [],
    });
    vi.mocked(fetchHistory).mockResolvedValue({ check_id: 'dns', name: 'DNS', history: [failure] });
    vi.mocked(fetchCheckIncidents).mockResolvedValue({ incidents: [] });

    const { container } = render(CheckDetail, { props: { checkId: 'dns', check, onback: vi.fn() } });
    const chip = container.querySelector('.detail-head .status-chip');
    expect(chip).toHaveTextContent('Paused');
    expect(chip).not.toHaveTextContent('down');
    expect(chip).toHaveAttribute('style', 'color: var(--unknown);');
    expect(chip?.querySelector('.chip-dot')).toHaveAttribute('style', 'background: var(--unknown);');
    expect(await screen.findByText('historical timeout')).toBeInTheDocument();
    expect(screen.getByText('uptime (24h)')).toBeInTheDocument();
  });
});
