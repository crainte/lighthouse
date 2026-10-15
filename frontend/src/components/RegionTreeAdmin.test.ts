import { fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import RegionTreeAdmin from './RegionTreeAdmin.svelte';
import type { RegionTreeResponse } from '../lib/admin';

const mocks = vi.hoisted(() => ({
  fetchRegionTree: vi.fn(),
  listRollupPolicies: vi.fn(),
  upsertRollupPolicy: vi.fn(),
}));

vi.mock('../lib/admin', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../lib/admin')>()),
  fetchRegionTree: mocks.fetchRegionTree,
  listRollupPolicies: mocks.listRollupPolicies,
  upsertRollupPolicy: mocks.upsertRollupPolicy,
}));

const policy = { degraded_when: 'any', down_when: 'majority' } as const;
const tree: RegionTreeResponse = {
  checks: [
    {
      check_id: 'api',
      name: 'API',
      state: 'up',
      effective_policy: policy,
      regions: [
        {
          region: 'us',
          state: 'up',
          effective_policy: policy,
          zones: [
            {
              zone: 'us-east-1a',
              state: 'up',
              effective_policy: policy,
              agents: [
                { id: 'agent-us-1', state: 'up', staleness_ms: 5000 },
                { id: 'agent-with-a-very-long-name', state: 'down', staleness_ms: 9000 },
              ],
            },
          ],
        },
      ],
    },
  ],
};

async function setup() {
  mocks.fetchRegionTree.mockResolvedValue(tree);
  mocks.listRollupPolicies.mockResolvedValue([]);
  mocks.upsertRollupPolicy.mockImplementation(async (value) => value);
  const view = render(RegionTreeAdmin, { props: { onexpired: vi.fn() } });
  await screen.findByRole('tree', { name: 'Location policies' });
  return view;
}

describe('RegionTreeAdmin location tree', () => {
  it('renders every visible level as a flat three-cell row', async () => {
    const { container } = await setup();
    const rows = [...container.querySelectorAll('[role="treeitem"]')];

    expect(rows).toHaveLength(5);
    expect(rows.map((row) => row.getAttribute('aria-level'))).toEqual(['1', '2', '3', '4', '4']);
    for (const row of rows) {
      expect(row).toHaveClass('row');
      expect(row.children).toHaveLength(3);
      expect(row.children[0]).toHaveClass('node-cell');
      expect(row.children[1]).toHaveClass('policy');
      expect(row.children[2]).toHaveClass('states');
    }

    const gutterSegments = rows.map((row) => row.querySelectorAll('.tree-gutter > *').length);
    expect(gutterSegments).toEqual([1, 2, 3, 4, 4]);
  });

  it('collapses branches without changing the row structure and opens policy inline', async () => {
    const { container } = await setup();

    await fireEvent.click(screen.getByRole('button', { name: 'Collapse us' }));
    await waitFor(() => expect(screen.queryByText('us-east-1a')).not.toBeInTheDocument());
    expect(container.querySelectorAll('[role="treeitem"]')).toHaveLength(2);

    await fireEvent.click(screen.getAllByRole('button', { name: /Edit policy/ })[0]);
    expect(screen.getByText(/No override; using the built-in global default/)).toBeInTheDocument();
    expect(container.querySelector('.editor-node-cell > .tree-gutter')).not.toBeNull();
  });

  it('explains unknown as missing observations, not an outage', async () => {
    const withUnknown = structuredClone(tree);
    withUnknown.checks[0].regions[0].zones[0].agents[1].state = 'unknown';
    mocks.fetchRegionTree.mockResolvedValueOnce(withUnknown);
    mocks.listRollupPolicies.mockResolvedValue([]);
    render(RegionTreeAdmin, { props: { onexpired: vi.fn() } });
    await screen.findByRole('tree', { name: 'Location policies' });
    expect(screen.getByText(/Unknown means no known observation/)).toBeInTheDocument();
    const unknown = screen.getByText('1 unknown');
    expect(unknown).toHaveAttribute('title', expect.stringMatching(/may be unplaced here/));
  });

  it('allows an explicit override even when the default thresholds are unchanged', async () => {
    const { container } = await setup();
    await fireEvent.click(screen.getAllByRole('button', { name: /Edit policy/ })[0]);
    const create = screen.getByRole('button', { name: 'Create override' });
    expect(create).toBeEnabled();
    expect(create).toHaveClass('primary');
    await fireEvent.click(create);
    await waitFor(() => expect(mocks.upsertRollupPolicy).toHaveBeenCalledWith({
      node_id: 'global',
      node_type: 'global',
      policy,
    }));
    expect(screen.getByText('Override saved.')).toBeInTheDocument();
    expect(container.querySelector('.row.node.selected')).not.toBeNull();
  });

});
