import { fireEvent, render, screen } from '@testing-library/svelte';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import NightwatchOverview from './NightwatchOverview.svelte';
import type { Check } from '../lib/api';

beforeEach(() => {
  vi.stubGlobal('matchMedia', vi.fn(() => ({ matches: true, addEventListener: vi.fn(), removeEventListener: vi.fn() })));
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

const check = (name: string, status: Check['status'] = 'up'): Check => ({ check_id: name, name, status, type: 'http', latency_ms: 12, last_check: 1, uptime_24h: 1 });
const callbacks = () => ({ onOpenCheck: vi.fn(), onOpenIncident: vi.fn(), onPanel: vi.fn(), onAddCheck: vi.fn(), onRetry: vi.fn(), onExpired: vi.fn() });
const props = (overrides = {}) => ({ checks: [], totalChecks: 0, hasMore: false, incidents: [], groups: [], alerts: [], fleet: [], maintenance: [], loading: false, error: null, wsStatus: 'open', nodeMode: 'coordinator' as const, ...callbacks(), ...overrides });

describe('NightwatchOverview', () => {
  it('does not claim operational health while loading', () => {
    const { container } = render(NightwatchOverview, { props: props({ loading: true }) });
    expect(screen.getByRole('heading', { name: 'Establishing visibility' })).toBeInTheDocument();
    expect(screen.getByText('Loading current monitor states…')).toBeInTheDocument();
    expect(screen.queryByText('All systems operational')).not.toBeInTheDocument();
    expect(container.querySelector('.landscape.unknown')).not.toBeNull();
  });

  it('invites the first monitor when none exist', async () => {
    const onAddCheck = vi.fn();
    render(NightwatchOverview, { props: props({ onAddCheck }) });
    expect(screen.getByRole('heading', { name: 'Your watch starts here' })).toBeInTheDocument();
    await fireEvent.click(screen.getAllByRole('button', { name: /Add your first monitor/ })[0]);
    expect(onAddCheck).toHaveBeenCalledOnce();
  });

  it('does not claim passing monitors when live updates disconnect', () => {
    render(NightwatchOverview, { props: props({ checks: [check('API')], totalChecks: 1, wsStatus: 'closed' }) });
    expect(screen.getByRole('heading', { name: 'Visibility interrupted' })).toBeInTheDocument();
    expect(screen.queryByText('All systems operational')).not.toBeInTheDocument();
  });

  it('keeps the light on and shows a warning light when a monitor is down', () => {
    const { container } = render(NightwatchOverview, { props: props({ checks: [check('API'), check('Cache', 'down')], totalChecks: 2 }) });
    expect(screen.getByRole('heading', { name: 'Partial service disruption' })).toBeInTheDocument();
    expect(container.querySelector('.landscape.down')).not.toBeNull();
    expect(container.querySelector('.landscape .beam')).not.toBeNull();
  });

  it('uses an amber atmosphere for degraded monitors', () => {
    const { container } = render(NightwatchOverview, { props: props({ checks: [check('API'), check('Queue', 'degraded')], totalChecks: 2 }) });
    expect(screen.getByRole('heading', { name: 'Degraded performance' })).toBeInTheDocument();
    expect(container.querySelector('.landscape.degraded')).not.toBeNull();
  });

  it('filters and opens visible monitor rows', async () => {
    const onOpenCheck = vi.fn();
    render(NightwatchOverview, { props: props({ checks: [check('API'), check('Cache', 'down')], totalChecks: 2, onOpenCheck }) });
    await fireEvent.click(screen.getByRole('button', { name: 'Needs attention' }));
    expect(screen.queryByRole('button', { name: /^API,/ })).not.toBeInTheDocument();
    await fireEvent.click(screen.getByRole('button', { name: /^Cache, Down/ }));
    expect(onOpenCheck).toHaveBeenCalledWith('Cache');
  });
});
