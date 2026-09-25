// The session can die while the tab sits open, and panels that only read
// public endpoints never get a 401 to learn from — so the app has to
// re-verify on its own rather than inferring it from a failed call. The mount
// path and the "entered admin" path can both want an answer at once; sharing
// one in-flight request keeps that down to a single probe of the cookie.

export interface SessionResult {
  authenticated: boolean;
  mode: string;
}

/** Wraps a `fetchSession`-shaped call in single-flight de-duplication: a
 *  second call while one is outstanding gets the same promise rather than
 *  issuing its own request. `onResolved` fires only on success; `onSettled`
 *  always fires once the request finishes, success or failure. */
export function createSessionChecker<T extends SessionResult>(handlers: {
  fetchSession: () => Promise<T>;
  onResolved: (session: T) => void;
  onSettled: () => void;
}): () => Promise<void> {
  let inFlight: Promise<void> | null = null;
  return function checkSession(): Promise<void> {
    if (inFlight) return inFlight;
    inFlight = handlers
      .fetchSession()
      .then(handlers.onResolved)
      .finally(() => {
        handlers.onSettled();
        inFlight = null;
      });
    return inFlight;
  };
}
