import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';

import { debounce, debounceByKey } from './debounce';

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe('debounce', () => {
  it('coalesces a burst of calls into one, after it settles', () => {
    const fn = vi.fn();
    const call = debounce(fn, 100);
    call();
    call();
    call();
    expect(fn).not.toHaveBeenCalled();
    vi.advanceTimersByTime(99);
    expect(fn).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('restarts the window on every call', () => {
    const fn = vi.fn();
    const call = debounce(fn, 100);
    call();
    vi.advanceTimersByTime(60);
    call();
    vi.advanceTimersByTime(60);
    expect(fn).not.toHaveBeenCalled();
    vi.advanceTimersByTime(40);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('cancel prevents a pending call from firing', () => {
    const fn = vi.fn();
    const call = debounce(fn, 100);
    call();
    call.cancel();
    vi.advanceTimersByTime(200);
    expect(fn).not.toHaveBeenCalled();
  });
});

describe('debounceByKey', () => {
  it('coalesces per key without one key delaying another', () => {
    const fn = vi.fn();
    const call = debounceByKey<string>(fn, 100);
    call('a');
    vi.advanceTimersByTime(50);
    call('b');
    vi.advanceTimersByTime(50);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn).toHaveBeenCalledWith('a');
    vi.advanceTimersByTime(50);
    expect(fn).toHaveBeenCalledTimes(2);
    expect(fn).toHaveBeenCalledWith('b');
  });

  it('a second call on the same key restarts only that key', () => {
    const fn = vi.fn();
    const call = debounceByKey<string>(fn, 100);
    call('a');
    vi.advanceTimersByTime(60);
    call('a');
    vi.advanceTimersByTime(60);
    expect(fn).not.toHaveBeenCalled();
    vi.advanceTimersByTime(40);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('cancelAll drops every pending key', () => {
    const fn = vi.fn();
    const call = debounceByKey<string>(fn, 100);
    call('a');
    call('b');
    call.cancelAll();
    vi.advanceTimersByTime(200);
    expect(fn).not.toHaveBeenCalled();
  });
});
