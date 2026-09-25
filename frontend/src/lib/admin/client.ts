// Authenticated admin API. Kept apart from api.ts because every call here
// carries the session cookie and, on failure, the server sends a human-readable
// `{error}` string that the form has to render verbatim.
//
// Shared HTTP core for every admin/* domain module below: one error shape and
// one fetch wrapper, so a 401 always looks the same no matter which panel hit it.

/** Carries the HTTP status so callers can tell "your input was wrong" (400/409)
 *  from "you are no longer logged in" (401), which needs a different recovery. */
export class AdminError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "AdminError";
    this.status = status;
  }

  get unauthorized(): boolean {
    return this.status === 401;
  }
}

export async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    let message = res.statusText || `HTTP ${res.status}`;
    try {
      const body: unknown = await res.json();
      const err = (body as { error?: unknown } | null)?.error;
      if (typeof err === "string" && err) message = err;
    } catch {
      // Non-JSON body (proxy error page, empty 502). Status text is all we have.
    }
    throw new AdminError(message, res.status);
  }
  return res.json() as Promise<T>;
}

export function send<T>(method: string, url: string, body?: unknown): Promise<T> {
  return request<T>(url, {
    method,
    headers:
      body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}
