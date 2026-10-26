import { fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import NotifiersAdmin from './NotifiersAdmin.svelte';

const mocks = vi.hoisted(() => ({
  fetchNotifiers: vi.fn(),
  saveNotifiers: vi.fn(),
  testNotifier: vi.fn(),
}));
vi.mock('../lib/admin', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../lib/admin')>()),
  fetchNotifiers: mocks.fetchNotifiers,
  saveNotifiers: mocks.saveNotifiers,
  testNotifier: mocks.testNotifier,
}));

describe('ntfy channel editor', () => {
  it('previews a custom alert and sends those templates through Save and Test', async () => {
    mocks.fetchNotifiers.mockResolvedValue({
      notifiers: [{ name: 'phone', type: 'ntfy', topic: 'alerts', default: true }],
      unrouted_checks: [], undelivered: [],
    });
    mocks.testNotifier.mockResolvedValue({ delivered: true });
    mocks.saveNotifiers.mockImplementation(async (notifiers) => ({
      notifiers, unrouted_checks: [], undelivered: [],
    }));
    render(NotifiersAdmin, { props: { onexpired: vi.fn() } });
    expect(await screen.findByText('Check API → down')).toBeInTheDocument();
    expect(screen.getByText('Status changed from up to down')).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText('Title template'), {
      target: { value: 'Alert: {name} is {to}' },
    });
    await fireEvent.input(screen.getByLabelText('Message template'), {
      target: { value: '{name}: {from} -> {to}' },
    });
    expect(screen.getByText('Alert: API is down')).toBeInTheDocument();
    expect(screen.getByText('API: up -> down')).toBeInTheDocument();
    await fireEvent.click(screen.getByRole('button', { name: 'Test' }));
    await waitFor(() => expect(mocks.testNotifier).toHaveBeenCalledWith(expect.objectContaining({
      title_template: 'Alert: {name} is {to}',
      message_template: '{name}: {from} -> {to}',
    })));
    await fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(mocks.saveNotifiers).toHaveBeenCalledWith([
      expect.objectContaining({
        title_template: 'Alert: {name} is {to}',
        message_template: '{name}: {from} -> {to}',
      }),
    ]));
  });

  it('shows an invalid placeholder as an error rather than a notification', async () => {
    mocks.fetchNotifiers.mockResolvedValue({
      notifiers: [{ name: 'phone', type: 'ntfy', topic: 'alerts', default: true }],
      unrouted_checks: [], undelivered: [],
    });
    render(NotifiersAdmin, { props: { onexpired: vi.fn() } });
    await screen.findByText('Check API → down');
    await fireEvent.input(screen.getByLabelText('Title template'), {
      target: { value: 'Alert {status}' },
    });
    expect(screen.getByText(/Unknown placeholder \{status\}/)).toBeInTheDocument();
    expect(screen.queryByText('Alert {status}')).not.toBeInTheDocument();
  });
});
