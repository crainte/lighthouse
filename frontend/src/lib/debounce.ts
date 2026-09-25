// Two ad-hoc timer patterns in App.svelte both did this by hand: a live event
// arrives as a burst — several transitions in quick succession — and only the
// settled state after the burst is worth a request. Coalescing here keeps that
// one rule in one place instead of two slightly different re-implementations.

/** Delays `fn` until `ms` have passed with no further call. Each call restarts
 *  the timer, so a burst costs exactly one invocation, after it settles. */
export function debounce(fn: () => void, ms: number): (() => void) & { cancel: () => void } {
  let timer: ReturnType<typeof setTimeout> | null = null;
  const call = () => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      fn();
    }, ms);
  };
  call.cancel = () => {
    if (timer) clearTimeout(timer);
    timer = null;
  };
  return call;
}

/** The same debounce, kept per key, so a burst on one key never delays or
 *  cancels another's — used to coalesce a chart refetch per check id. */
export function debounceByKey<K>(
  fn: (key: K) => void,
  ms: number,
): ((key: K) => void) & { cancelAll: () => void } {
  const timers = new Map<K, ReturnType<typeof setTimeout>>();
  const call = (key: K) => {
    const existing = timers.get(key);
    if (existing) clearTimeout(existing);
    timers.set(
      key,
      setTimeout(() => {
        timers.delete(key);
        fn(key);
      }, ms),
    );
  };
  call.cancelAll = () => {
    for (const t of timers.values()) clearTimeout(t);
    timers.clear();
  };
  return call;
}
