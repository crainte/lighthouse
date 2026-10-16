import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/svelte';
import PublicIncidentSpotlight from './PublicIncidentSpotlight.svelte';
import type { Incident } from '../lib/api';

const mocks = vi.hoisted(() => ({ fetchIncident: vi.fn() }));
vi.mock('../lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../lib/api')>()),
  fetchIncident: mocks.fetchIncident,
}));
afterEach(() => { cleanup(); mocks.fetchIncident.mockReset(); });

const incident: Incident = {
  id: 'inc-7', title: 'DNS service disruption', state: 'investigating',
  started_at: Date.now() - 60_000, resolved_at: null, duration_ms: 60_000,
  global: false, origin_check: 'dns',
  checks: [{ check_id: 'dns', source: 'origin' }], acknowledged: true,
};

describe('public incident spotlight', () => {
  it('shows only the actual published narrative and affected service names', async () => {
    mocks.fetchIncident.mockResolvedValue({
      incident: {
        ...incident,
        updates: [
          { id: 'u1', state: 'detected', body: '', author: null, created_at: Date.now() - 60_000 },
          { id: 'u2', state: 'investigating', body: 'Some requests are failing in US West.', author: 'operator', created_at: Date.now() - 30_000 },
        ],
      },
    });
    const onOpen = vi.fn();
    render(PublicIncidentSpotlight, { props: { incident, checks: [{ check_id: 'dns', name: 'DNS resolver' }], onOpen } });
    const spotlight = screen.getByRole('region', { name: 'Active incident updates' });
    expect(spotlight).toHaveTextContent('DNS resolver');
    expect(await screen.findByText('Some requests are failing in US West.')).toBeInTheDocument();
    expect(spotlight).toHaveTextContent('Automatic state change');
    await fireEvent.click(screen.getByRole('button', { name: 'Full incident →' }));
    expect(onOpen).toHaveBeenCalledWith('inc-7');
  });

  it('does not invent a response when no updates have been published', async () => {
    mocks.fetchIncident.mockResolvedValue({ incident: { ...incident, updates: [] } });
    render(PublicIncidentSpotlight, { props: { incident, checks: [], onOpen: vi.fn() } });
    expect(await screen.findByText(/No incident updates have been published/)).toBeInTheDocument();
    expect(screen.queryByText(/We are investigating the cause/)).not.toBeInTheDocument();
  });
});
