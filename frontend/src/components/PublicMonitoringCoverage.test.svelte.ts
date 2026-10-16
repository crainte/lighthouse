import { afterEach, describe, expect, it } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/svelte';
import PublicMonitoringCoverage from './PublicMonitoringCoverage.svelte';
import type { FleetRow } from '../lib/api';

afterEach(() => cleanup());

const fleet: FleetRow[] = [
  { kind: 'region', name: 'tw', status: 'up', healthy: 1, total: 1 },
  { kind: 'region', name: 'us', status: 'degraded', healthy: 2, total: 3 },
];

describe('public monitoring coverage', () => {
  it('is a full-card disclosure with identifiable, complete observer rows', async () => {
    const { container } = render(PublicMonitoringCoverage, { props: { fleet } });
    const details = container.querySelector('details.public-coverage')!;
    const toggle = screen.getByText(/View observers/).closest('summary')!;
    expect(details).toBeInTheDocument();
    expect(toggle).toHaveClass('public-coverage-toggle');
    expect(toggle).toHaveTextContent('2 reported locations');
    expect(container.querySelectorAll('.public-coverage-observers .server-row')).toHaveLength(2);
    expect(details).not.toHaveAttribute('open');
    await fireEvent.click(toggle);
    expect(details).toHaveAttribute('open');
    expect(details).toHaveTextContent('1/1 agents reporting');
    expect(details).toHaveTextContent('2/3 agents reporting');
  });

  it('does not show an empty coverage card', () => {
    const { container } = render(PublicMonitoringCoverage, { props: { fleet: [] } });
    expect(container.querySelector('.public-coverage')).not.toBeInTheDocument();
  });
});
