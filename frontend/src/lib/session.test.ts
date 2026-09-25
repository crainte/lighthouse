import { describe, expect, it, vi } from 'vitest';

import { createSessionChecker } from './session';

describe('createSessionChecker', () => {
  it('shares one in-flight request across concurrent callers', async () => {
    let resolve!: (v: { authenticated: boolean; mode: string }) => void;
    const fetchSession = vi.fn(
      () => new Promise<{ authenticated: boolean; mode: string }>((r) => (resolve = r)),
    );
    const onResolved = vi.fn();
    const onSettled = vi.fn();
    const checkSession = createSessionChecker({ fetchSession, onResolved, onSettled });

    const first = checkSession();
    const second = checkSession();
    expect(fetchSession).toHaveBeenCalledTimes(1);

    resolve({ authenticated: true, mode: 'coordinator' });
    await Promise.all([first, second]);

    expect(onResolved).toHaveBeenCalledTimes(1);
    expect(onResolved).toHaveBeenCalledWith({ authenticated: true, mode: 'coordinator' });
    expect(onSettled).toHaveBeenCalledTimes(1);
  });

  it('issues a fresh request once the previous one has settled', async () => {
    const fetchSession = vi
      .fn<() => Promise<{ authenticated: boolean; mode: string }>>()
      .mockResolvedValueOnce({ authenticated: false, mode: 'coordinator' })
      .mockResolvedValueOnce({ authenticated: true, mode: 'coordinator' });
    const onResolved = vi.fn();
    const onSettled = vi.fn();
    const checkSession = createSessionChecker({ fetchSession, onResolved, onSettled });

    await checkSession();
    await checkSession();

    expect(fetchSession).toHaveBeenCalledTimes(2);
    expect(onResolved).toHaveBeenNthCalledWith(1, { authenticated: false, mode: 'coordinator' });
    expect(onResolved).toHaveBeenNthCalledWith(2, { authenticated: true, mode: 'coordinator' });
  });

  it('still settles when the request rejects', async () => {
    const fetchSession = vi.fn().mockRejectedValueOnce(new Error('network down'));
    const onResolved = vi.fn();
    const onSettled = vi.fn();
    const checkSession = createSessionChecker({ fetchSession, onResolved, onSettled });

    await expect(checkSession()).rejects.toThrow('network down');
    expect(onResolved).not.toHaveBeenCalled();
    expect(onSettled).toHaveBeenCalledTimes(1);
  });
});
