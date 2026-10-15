import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, within } from '@testing-library/svelte';
import AgentInventoryTable from './AgentInventoryTable.svelte';

afterEach(() => cleanup());
const agents = [
  {
    agent_id: 'agent-tw', region: 'tw', zone: 'tw-north-1a',
    status: 'up' as const, retired: false, last_seen: Date.now(),
    checks_reporting: 4, results_24h: 4318, assigned_checks: 1,
    poll_rtt_ms: 2, coordinator: 'coordinator-tw',
    version: '0.0.1 (unknown)', kinds: ['http','dns'],
  },
  {
    agent_id: 'agent-us', region: 'us', zone: 'us-east-1a',
    status: 'down' as const, retired: false, last_seen: Date.now() - 10000,
    checks_reporting: 0, results_24h: 0, assigned_checks: 0,
    poll_rtt_ms: null, coordinator: null,
    version: null, kinds: null,
  },
];

function props(overrides: Record<string, unknown> = {}) {
  return {
    agents, selectedId: 'agent-tw',
    onSelect: vi.fn(), sortKey: 'status' as const, sortDir: 1 as const,
    onSort: vi.fn(), pending: null, purging: false,
    purgeError: null, onArm: vi.fn(), onCancel: vi.fn(),
    onConfirmPurge: vi.fn(), ...overrides,
  };
}

describe('Fleet agent inventory', () => {
  it('renders a flat, sortable table with country flags and real reported fields', async () => {
    const p = props();
    render(AgentInventoryTable, { props: p });
    const table = screen.getByRole('table');
    expect(within(table).getAllByRole('row')).toHaveLength(3);
    expect(screen.queryByRole('button', { name: 'Region tw' })).not.toBeInTheDocument();
    expect(table).toHaveTextContent('coordinator-tw');
    expect(table).toHaveTextContent('tw-north-1a');
    expect(table).toHaveTextContent('4,318');
    expect(table).toHaveTextContent('0.0.1');
    await fireEvent.click(screen.getByRole('button', { name: /Last seen/ }));
    expect(p.onSort).toHaveBeenCalledWith('seen');
  });

  it('inspects the selected agent and closes without deleting or changing its state', async () => {
    const p = props();
    render(AgentInventoryTable, { props: p });
    const inspector = screen.getByRole('complementary', { name: 'Agent details for agent-tw' });
    expect(inspector).toHaveTextContent('coordinator-tw');
    expect(inspector).toHaveTextContent('http');
    expect(inspector).toHaveTextContent('2 ms');
    await fireEvent.click(screen.getByRole('button', { name: 'Close agent details' }));
    expect(p.onSelect).toHaveBeenCalledWith(null);
    expect(p.onArm).not.toHaveBeenCalled();
  });

  it('keeps destructive purge behind a separate confirmation action', async () => {
    const p = props({ pending: 'agent-tw' });
    render(AgentInventoryTable, { props: p });
    expect(screen.getByRole('alert')).toHaveTextContent('cannot be undone');
    await fireEvent.click(screen.getByRole('button', { name: 'Confirm purge' }));
    expect(p.onConfirmPurge).toHaveBeenCalledWith('agent-tw');
  });
});
