import { afterEach, describe, expect, it } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/svelte';
import PlacementEditor from './PlacementEditor.svelte';
import { blank } from './draft';

afterEach(() => cleanup());

const effect = {
  total: 3,
  count: 1,
  domains: [
    { domain: 'tw-north-1a', probes: 1, available: 1 },
    { domain: 'us-east-1a', probes: 1, available: 1 },
    { domain: 'us-west-1a', probes: 1, available: 2 },
  ],
};
const opts = {
  regionOptions: ['tw', 'us'],
  zoneOptions: ['tw-north-1a', 'us-east-1a', 'us-west-1a'],
  agentOptions: ['agent-tw', 'agent-us-1', 'agent-us-2'],
  reporting: new Set(['agent-tw', 'agent-us-1']),
};

describe('progressive check placement', () => {
  it('starts with automatic fleet-wide selection and keeps the projected impact visible', () => {
    const draft = $state(blank());
    render(PlacementEditor, { props: { draft, pinned: false, effect, ...opts } });
    expect(screen.getByRole('radio', { name: /Automatic/ })).toBeChecked();
    expect(screen.getByRole('radio', { name: 'All locations' })).toBeChecked();
    expect(screen.getByRole('region', { name: 'Placement preview' })).toHaveTextContent('3 probes');
    expect(screen.queryByRole('region', { name: 'Filter regions' })).not.toBeInTheDocument();
    expect(screen.queryByText('agent-us-1')).not.toBeInTheDocument();
    expect(draft.placement.regions).toEqual([]);
    expect(draft.agents).toEqual([]);
  });

  it('reveals filters on demand and can restore unsaved location choices', async () => {
    const draft = $state(blank());
    render(PlacementEditor, { props: { draft, pinned: false, effect, ...opts } });
    await fireEvent.click(screen.getByRole('radio', { name: 'Selected locations' }));
    const regions = screen.getByRole('region', { name: 'Filter regions' });
    const zones = screen.getByRole('region', { name: 'Filter zones' });
    await fireEvent.click(regions.getElementsByTagName('input')[0]);
    await fireEvent.click(zones.getElementsByTagName('input')[2]);
    expect(draft.placement.regions).toEqual(['tw']);
    expect(draft.placement.zones).toEqual(['us-west-1a']);
    await fireEvent.click(screen.getByRole('radio', { name: 'All locations' }));
    expect(draft.placement.regions).toEqual([]);
    expect(draft.placement.zones).toEqual([]);
    await fireEvent.click(screen.getByRole('radio', { name: 'Selected locations' }));
    expect(draft.placement.regions).toEqual(['tw']);
    expect(draft.placement.zones).toEqual(['us-west-1a']);
    const advanced = screen.getByText('Add a future region or zone');
    await fireEvent.click(advanced);
    await fireEvent.input(screen.getByRole('textbox', { name: 'New zone' }), { target: { value: 'us-future-1a' } });
    await fireEvent.click(screen.getAllByRole('button', { name: 'Add' })[1]);
    expect(draft.placement.zones).toContain('us-future-1a');
  });

  it('makes pinning mutually exclusive and restores pins on switch-back', async () => {
    const draft = $state(blank());
    draft.placement.regions = ['us'];
    render(PlacementEditor, { props: { draft, pinned: false, effect, ...opts } });
    await fireEvent.click(screen.getByRole('radio', { name: /Specific agents/ }));
    expect(screen.queryByRole('radio', { name: 'Selected locations' })).not.toBeInTheDocument();
    expect(screen.getByText(/Select at least one agent/)).toBeInTheDocument();
    await fireEvent.click(screen.getByRole('checkbox', { name: 'agent-us-1' }));
    expect(draft.agents).toEqual(['agent-us-1']);
    await fireEvent.click(screen.getByRole('radio', { name: /Automatic/ }));
    expect(draft.agents).toEqual([]);
    expect(draft.placement.regions).toEqual(['us']);
    await fireEvent.click(screen.getByRole('radio', { name: /Specific agents/ }));
    expect(draft.agents).toEqual(['agent-us-1']);
  });

  it('opens persisted pins directly and warns when agents cannot report', () => {
    const draft = $state(blank());
    draft.agents = ['agent-us-2'];
    render(PlacementEditor, { props: { draft, pinned: true, effect, ...opts } });
    expect(screen.getByRole('radio', { name: /Specific agents/ })).toBeChecked();
    expect(screen.getByRole('region', { name: 'Placement preview' })).toHaveTextContent('0 of 1 reporting');
    expect(screen.getByText(/None are currently reporting/)).toBeInTheDocument();
  });
});
