import { fireEvent, render, screen, within } from '@testing-library/svelte';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fetchAgents, type Check } from '../lib/api';
import { fetchCluster, fetchRegionTree } from '../lib/admin';
import NightwatchOverview from './NightwatchOverview.svelte';

beforeEach(() => {
  vi.stubGlobal('matchMedia', vi.fn(() => ({ matches: true, addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.mocked(fetchRegionTree).mockImplementation(() => new Promise(() => {}));
});

vi.mock('../lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../lib/api')>()),
  fetchAgents: vi.fn(() => new Promise(() => {})),
  fetchUptimeStrips: vi.fn(() => new Promise(() => {})),
}));
vi.mock('../lib/admin', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../lib/admin')>()),
  fetchCluster: vi.fn(() => new Promise(() => {})),
  fetchRegionTree: vi.fn(() => new Promise(() => {})),
}));

const check = (name: string, status: Check['status'], enabled?: boolean): Check => ({
  check_id: name, name, status, enabled, type: 'http', latency_ms: 123, last_check: 1, uptime_24h: 0.98,
});
const props = (checks: Check[]) => ({
  checks, totalChecks: checks.length, hasMore: false, incidents: [], groups: [], alerts: [], fleet: [], maintenance: [],
  loading: false, error: null, wsStatus: 'open', nodeMode: 'coordinator' as const,
  onOpenCheck: vi.fn(), onOpenIncident: vi.fn(), onPanel: vi.fn(), onAddCheck: vi.fn(), onRetry: vi.fn(), onExpired: vi.fn(),
});

describe('paused monitors in the admin overview', () => {
  it('counts only active observations and keeps paused inventory rows neutral with history', async () => {
    const { container } = render(NightwatchOverview, { props: props([
      check('API', 'up'), check('Old outage', 'down', false), check('Old slowdown', 'degraded', false), check('Old unknown', 'unknown', false),
    ]) });
    expect(screen.getByRole('heading', { name: 'All systems operational' })).toBeInTheDocument();
    expect(container.querySelector('.landscape.operational')).not.toBeNull();
    const summary = screen.getByRole('region', { name: 'Monitoring summary' });
    expect(within(summary).getByText('1', { selector: '.metric-value' })).toHaveTextContent('1 / 1');
    expect(summary).not.toHaveTextContent('down');
    expect(summary).not.toHaveTextContent('degraded');
    for (const name of ['Old outage', 'Old slowdown', 'Old unknown']) {
      const row = screen.getByRole('button', { name: new RegExp(`^${name}, Paused\\. Open details$`) });
      expect(row).toHaveTextContent('Paused');
      expect(row).not.toHaveTextContent('123');
      expect(row).not.toHaveTextContent('98');
      expect(row.querySelector('.monitor-status.quiet')).not.toBeNull();
      expect(row.querySelector('.uptime-strip')).not.toBeNull();
    }
    await fireEvent.click(screen.getByRole('button', { name: 'Needs attention' }));
    expect(container.querySelectorAll('.monitor-row')).toHaveLength(0);
  });

  it('never presents an all-clear when all inventory monitors are paused', () => {
    const { container } = render(NightwatchOverview, { props: props([check('Old outage', 'down', false)]) });
    expect(screen.queryByRole('heading', { name: 'All systems operational' })).not.toBeInTheDocument();
    expect(screen.queryByText('✓ All operational')).not.toBeInTheDocument();
    expect(container.querySelector('.landscape.unknown')).not.toBeNull();
    expect(screen.getByRole('button', { name: /^Old outage, Paused\. Open details$/ })).toBeInTheDocument();
  });

  it('classifies an active outage against active checks, not paused inventory', () => {
    render(NightwatchOverview, { props: props([check('API', 'down'), check('Dormant', 'up', false)]) });
    expect(screen.getByRole('heading', { name: 'Major service disruption' })).toBeInTheDocument();
  });

  it('does not count paused region-tree states as affected stations', async () => {
    vi.mocked(fetchAgents).mockRejectedValueOnce(new Error('offline'));
    vi.mocked(fetchCluster).mockRejectedValueOnce(new Error('offline'));
    vi.mocked(fetchRegionTree).mockResolvedValue({ checks: [{
      check_id: 'Dormant', name: 'Dormant', state: 'down', effective_policy: { degraded_when: 'any', down_when: 'any' },
      regions: [{ region: 'east', state: 'down', effective_policy: null, zones: [] }],
    }] });
    const { container } = render(NightwatchOverview, { props: props([check('Dormant', 'down', false), check('API', 'up')]) });
    expect(await screen.findByText('east')).toBeInTheDocument();
    const station = container.querySelector('.station');
    expect(station).not.toBeNull();
    expect(station).not.toHaveClass('affected');
    expect(station).toHaveClass('unknown');
    expect(station).not.toHaveTextContent('1 affected');
  });
});
