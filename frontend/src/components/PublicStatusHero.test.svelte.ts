import { afterEach, describe, expect, it } from 'vitest';
import { cleanup, render, screen } from '@testing-library/svelte';
import PublicStatusHero from './PublicStatusHero.svelte';
import type { Check, Incident } from '../lib/api';

afterEach(() => cleanup());
const check = (name: string, status: Check['status']): Check => ({
  check_id: name, name, status, last_check: Date.now(),
  latency_ms: 20, uptime_24h: 1,
});
const incident: Incident = {
  id: 'inc-1', title: 'DNS service disruption', state: 'investigating',
  started_at: Date.now() - 900_000, resolved_at: null, duration_ms: 900_000,
  global: false, origin_check: 'dns', checks: [{ check_id: 'dns', source: 'origin' }],
  acknowledged: true,
};

describe('public status lighthouse hero', () => {
  it('renders the calm scene and an honest all-clear from healthy observations', () => {
    render(PublicStatusHero, { props: { checks: [check('dns', 'up')], incidents: [] } });
    const region = screen.getByRole('region', { name: 'Current service status' });
    expect(region).toHaveAttribute('data-health', 'operational');
    expect(region).toHaveTextContent('All systems operational');
    expect(region).toHaveTextContent('Latest monitor observation');
  });

  it('changes to an incident treatment when the public API reports one', () => {
    render(PublicStatusHero, { props: { checks: [check('dns', 'up')], incidents: [incident] } });
    const region = screen.getByRole('region', { name: 'Current service status' });
    expect(region).toHaveClass('storm');
    expect(region).toHaveTextContent('DNS service disruption');
    expect(region).toHaveTextContent('investigating');
  });

  it('never reports healthy when a paged section is down or observations are unknown', () => {
    render(PublicStatusHero, {
      props: {
        checks: [check('dns', 'up')],
        incidents: [],
        sections: [{ name: 'remote', status: 'down', checks: 1, down: 1, degraded: 0 }],
      },
    });
    const region = screen.getByRole('region', { name: 'Current service status' });
    expect(region).toHaveAttribute('data-health', 'outage');
    expect(region).not.toHaveTextContent('All systems operational');
  });
});
