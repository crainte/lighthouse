import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, within } from '@testing-library/svelte';
import FleetOverview from './FleetOverview.svelte';
import type { ClusterResponse } from '../lib/admin';

const mocks = vi.hoisted(() => ({
  fetchCluster: vi.fn(),
  fetchAgents: vi.fn(),
}));

vi.mock('../lib/admin', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../lib/admin')>()),
  fetchCluster: mocks.fetchCluster,
}));
vi.mock('../lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../lib/api')>()),
  fetchAgents: mocks.fetchAgents,
}));

const now = Date.now();
function member(node_id: string) {
  return { node_id, url: '', generation: 1, last_seen: now, state: 'alive' as const, build: '0.1.0' };
}
function agent(agent_id: string, region: string, zone: string) {
  return {
    agent_id, region, zone, status: 'up' as const, last_seen: now, checks_reporting: 3,
    results_24h: 10, retired: false, assigned_checks: 0, poll_rtt_ms: 2,
    version: '0.1.0', coordinator: region === 'tw' ? 'tw-coordinator' : 'us-coordinator',
    kinds: ['http'],
  };
}

beforeEach(() => {
  mocks.fetchCluster.mockReset();
  mocks.fetchAgents.mockReset();
});
afterEach(() => cleanup());

describe('Fleet overview', () => {
  it('shows real global peer membership, grouped agents and explicit connectivity', async () => {
    mocks.fetchCluster.mockResolvedValue({
      mode: 'global',
      self: member('global-a'),
      global_peers: [member('global-a'), member('global-b')],
      members: [member('tw'), member('us')],
      now,
    } satisfies ClusterResponse);
    mocks.fetchAgents.mockResolvedValue({
      agents: [agent('agent-tw', 'tw', 'tw-north-1a'), agent('agent-us', 'us', 'us-east-1a')],
      unavailable_regions: [],
    });

    const onNavigate = vi.fn();
    render(FleetOverview, { props: { mode: 'overview', onNavigate } });
    const plane = await screen.findByRole('region', { name: 'Control-plane membership' });
    expect(plane).toHaveTextContent('global-a');
    expect(plane).toHaveTextContent('global-b');
    expect(plane).not.toHaveTextContent('tw-coordinator');
    const tw = screen.getByRole('region', { name: 'Region tw' });
    expect(tw).toHaveTextContent('tw-north-1a');
    expect(tw).toHaveTextContent('agent-tw');
    expect(screen.getByRole('complementary', { name: 'Fleet health and recent agent reports' })).toHaveTextContent('Connectivity only');

    await fireEvent.click(within(tw).getByRole('button', { name: /tw-north-1a/ }));
    expect(tw).not.toHaveTextContent('agent-tw');
    await fireEvent.click(within(tw).getByRole('button', { name: /tw-north-1a/ }));
    expect(tw).toHaveTextContent('agent-tw');
    await fireEvent.click(screen.getByRole('button', { name: /All regions/ }));
    expect(onNavigate).toHaveBeenCalledWith('regions');
  });

  it('does not invent a global control plane for a regional singleton', async () => {
    mocks.fetchCluster.mockResolvedValue({
      mode: 'coordinator',
      self: member('us-c1'),
      members: [],
      now,
    } satisfies ClusterResponse);
    mocks.fetchAgents.mockResolvedValue({
      agents: [agent('agent-us', 'us', 'us-west-1a')],
      unavailable_regions: [],
    });

    render(FleetOverview, { props: { mode: 'overview', onNavigate: vi.fn() } });
    const plane = await screen.findByRole('region', { name: 'Control-plane membership' });
    expect(plane).toHaveTextContent('Regional control plane');
    expect(plane).toHaveTextContent('Standalone');
    expect(screen.queryByText('Global control plane')).not.toBeInTheDocument();
  });

  it('shows failures to fetch regional agent data rather than claiming full health', async () => {
    mocks.fetchCluster.mockResolvedValue({
      mode: 'global', self: member('global-a'), members: [member('tw')], now,
    } satisfies ClusterResponse);
    mocks.fetchAgents.mockResolvedValue({
      agents: [agent('agent-tw', 'tw', 'tw-north-1a')],
      unavailable_regions: ['us'],
    });
    render(FleetOverview, { props: { mode: 'overview', onNavigate: vi.fn() } });
    expect(await screen.findByText(/Region data unavailable from us/)).toBeInTheDocument();
  });
});
