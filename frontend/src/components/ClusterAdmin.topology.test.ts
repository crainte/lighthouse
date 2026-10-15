import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/svelte';
import ClusterAdmin from './ClusterAdmin.svelte';
import type { ClusterMember, ClusterResponse } from '../lib/admin';

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
function member(node_id: string): ClusterMember {
  return { node_id, url: '', generation: 1, last_seen: now, state: 'alive', build: '0.1.0' };
}

function agent(agent_id: string, region: string, zone: string, coordinator: string) {
  return {
    agent_id, region, zone, coordinator, status: 'up', last_seen: now,
    checks_reporting: 1, results_24h: 1, retired: false, assigned_checks: 1,
    poll_rtt_ms: 1, version: '0.1.0', kinds: null,
  };
}

beforeEach(() => {
  mocks.fetchCluster.mockReset();
  mocks.fetchAgents.mockReset();
});

afterEach(() => cleanup());

describe('Topology deployment map', () => {
  it('shows all global cluster peers separately from regions, coordinators, and zone agents', async () => {
    const cluster: ClusterResponse = {
      mode: 'global',
      self: member('global-a'),
      members: [member('tw'), member('us')],
      global_peers: [member('global-a'), member('global-b'), member('global-c')],
      global_links: [],
      links: [],
      now,
      regions: [
        { region: 'tw', members: [member('coordinator-tw')], links: [], now, link_stale_after_ms: 30000, available: true },
        { region: 'us', members: [member('coordinator-us-a'), member('coordinator-us-b')], links: [], now, link_stale_after_ms: 30000, available: true },
      ],
    };
    mocks.fetchCluster.mockResolvedValue(cluster);
    mocks.fetchAgents.mockResolvedValue({
      agents: [
        agent('agent-tw-1', 'tw', 'tw-north-1a', 'coordinator-tw'),
        agent('agent-us-1', 'us', 'us-east-1a', 'coordinator-us-a'),
      ],
    });

    render(ClusterAdmin);
    const plane = await screen.findByRole('region', { name: 'Global control plane' });
    expect(plane).toHaveTextContent('global-a');
    expect(plane).toHaveTextContent('global-b');
    expect(plane).toHaveTextContent('global-c');
    expect(plane).not.toHaveTextContent('coordinator-tw');
    expect(plane).not.toHaveTextContent('us-east-1a');

    expect(screen.getByRole('region', { name: 'Coordinators in tw' })).toHaveTextContent('coordinator-tw');
    expect(screen.getByRole('region', { name: 'Zones in tw' })).toHaveTextContent('agent-tw-1');
    expect(screen.getByRole('region', { name: 'Zones in us' })).toHaveTextContent('agent-us-1');

    await fireEvent.click(screen.getByRole('button', { name: /agent-us-1/ }));
    expect(screen.getByRole('complementary', { name: 'Selected topology details' })).toHaveTextContent('Polls coordinator');
    expect(screen.getByRole('complementary', { name: 'Selected topology details' })).toHaveTextContent('coordinator-us-a');
    expect(screen.getByRole('button', { name: /coordinator-us-a/ })).toHaveClass('polled');
  });

  it('works without a global tier or regional peers', async () => {
    mocks.fetchCluster.mockResolvedValue({
      mode: 'coordinator',
      self: member('us-c1'),
      members: [],
      now,
      links: [],
    } satisfies ClusterResponse);
    mocks.fetchAgents.mockResolvedValue({ agents: [agent('solo-agent', 'us', 'home', 'us-c1')] });

    render(ClusterAdmin);
    expect(await screen.findByRole('region', { name: 'Regional control plane' })).toHaveTextContent('us-c1');
    expect(screen.queryByRole('region', { name: 'Global control plane' })).not.toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Zones in us' })).toHaveTextContent('solo-agent');
    expect(screen.queryByRole('region', { name: 'Coordinators in us' })).not.toBeInTheDocument();
  });

  it('preserves a single global node when running without clustering', async () => {
    mocks.fetchCluster.mockResolvedValue({
      mode: 'global', self: member('global'), members: [], regions: [], now, links: [],
    } satisfies ClusterResponse);
    mocks.fetchAgents.mockResolvedValue({ agents: [] });

    render(ClusterAdmin);
    expect(await screen.findByRole('region', { name: 'Global control plane' })).toHaveTextContent('1 node · standalone');
    expect(screen.getByRole('button', { name: 'global Coordinator · alive · This node' })).toBeInTheDocument();
  });
});
