// Authenticated admin API. Kept apart from api.ts because every call here
// carries the session cookie and, on failure, the server sends a human-readable
// `{error}` string that the form has to render verbatim.

import type { CheckKind } from './api';

/** Whether every assertion in a set must hold, or just one of them. Shared by
 *  multi-host checks and DNS expected values so "any/all" reads the same way
 *  wherever it appears. */
export type MatchMode = 'any' | 'all';

/** What a check reads as before it has ever reported. Without this a freshly
 *  created check would show as an outage until its first result lands. */
export type DefaultStatus = 'up' | 'down' | 'unknown';

export interface CheckHeader {
  name: string;
  value: string;
}

/** Where a check runs, described by location rather than by machine so that
 *  growing the fleet does not mean revisiting every check. Empty `zones` means
 *  every zone; empty `regions` means every region. `per_zone` is how many
 *  agents *within each matched zone* probe it — not a fleet-wide total. */
export interface Placement {
  zones: string[];
  regions: string[];
  per_zone: number;
}

/** The admin wire type IS the backend's `CheckConfig`, verbatim. A parallel
 *  flattened DTO drifted from it every time a field was added, so there is now
 *  exactly one shape and no conversion. It carries the target, so it only ever
 *  arrives behind an authenticated session. */
export interface AdminCheck {
  name: string;
  type: CheckKind;
  /** HTTP: a URL. TCP: "host:port". DNS/ICMP/SSL: a hostname. Heartbeat: unused. */
  target: string;
  /** Extra targets probed by the same check, alongside `target`. */
  hosts: string[];
  /** 'all' = every host must pass, 'any' = one passing is enough. */
  host_match: MatchMode;
  // Six fields, seconds first — e.g. "*/30 * * * * *". Five fields are
  // accepted and read as "at second zero".
  cron: string;
  timeout_secs: number;
  group?: string;
  latency_graph: boolean;
  expect: { status?: number; body_contains?: string };
  enabled: boolean;
  /** Publish on the public status page. Off = still probed, just not shown. */
  visible: boolean;
  /** Lua source deciding UP/DOWN. Absent/empty = built-in 2xx-3xx logic. */
  eval?: string;
  /** Display order within its section on the public page, lower first. The
   *  server owns it: assigned on create, rewritten by reorderChecks. */
  position: number;
  placement: Placement;
  /** Explicit pinning, and an override: when non-empty these agents probe the
   *  check and `placement` is ignored entirely. Empty is the common case and
   *  hands the decision to `placement`. */
  agents: string[];
  description?: string;
  /** Emoji or image URL shown beside the name. */
  image?: string;
  tag?: string;
  default_status: DefaultStatus;
  /** HTTP only. */
  headers: CheckHeader[];
  /** HTTP/SSL: accept an untrusted certificate. */
  allow_self_signed: boolean;
  dns: {
    nameserver?: string;
    /** A | AAAA | CNAME | MX | TXT | NS */
    record: string;
    match_type: MatchMode;
    expected: string[];
  };
  ping: { ip_type: string; count: number };
  /** SSL: DOWN once the certificate is this close to expiry. */
  cert_warning_days: number;
  /** Heartbeat: no ping within this window means DOWN. */
  grace_secs: number;
  /** Consecutive DOWN probes that confirm an outage. 1 = flip on the first
   *  failure; 3 = tolerate two flaky probes. Recovery is never delayed. */
  failures_before_down: number;
}

/** What the editor sends. A create has no position yet — the server assigns the
 *  next free one — and an update echoes back whatever the server last gave us,
 *  so nobody downstream has to invent an ordering. */
export type CheckPayload = Omit<AdminCheck, 'position'> & { position?: number };

/** A named, ordered group on the status page. Exists independently of checks,
 *  so an empty section can be created before anything is put in it. */
export interface Section {
  name: string;
  position: number;
  /** The PUBLIC status page renders this section collapsed on first load. The
   *  admin view ignores it. */
  collapsed: boolean;
}

export interface EvalSample {
  eval: string;
  status: number;
  latency: number;
  body: string;
}

export interface EvalResult {
  verdict: 'up' | 'down';
  latency_ms: number;
}

/** Carries the HTTP status so callers can tell "your input was wrong" (400/409)
 *  from "you are no longer logged in" (401), which needs a different recovery. */
export class AdminError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'AdminError';
    this.status = status;
  }

  get unauthorized(): boolean {
    return this.status === 401;
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    let message = res.statusText || `HTTP ${res.status}`;
    try {
      const body: unknown = await res.json();
      const err = (body as { error?: unknown } | null)?.error;
      if (typeof err === 'string' && err) message = err;
    } catch {
      // Non-JSON body (proxy error page, empty 502). Status text is all we have.
    }
    throw new AdminError(message, res.status);
  }
  return res.json() as Promise<T>;
}

function send<T>(method: string, url: string, body?: unknown): Promise<T> {
  return request<T>(url, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

/** Never 401s — safe to call on mount to decide whether to offer the admin UI. */
export async function fetchSession(): Promise<boolean> {
  const r = await request<{ authenticated: boolean }>('/api/v1/session');
  return r.authenticated;
}

export async function login(password: string): Promise<void> {
  await send('POST', '/api/v1/login', { password });
}

export async function logout(): Promise<void> {
  await send('POST', '/api/v1/logout');
}

export async function fetchAdminChecks(): Promise<AdminCheck[]> {
  const r = await request<{ checks: AdminCheck[] }>('/api/v1/admin/checks');
  return r.checks;
}

export async function createCheck(check: CheckPayload): Promise<AdminCheck> {
  const r = await send<{ check: AdminCheck }>('POST', '/api/v1/admin/checks', check);
  return r.check;
}

export async function updateCheck(name: string, check: CheckPayload): Promise<AdminCheck> {
  const r = await send<{ check: AdminCheck }>(
    'PUT',
    `/api/v1/admin/checks/${encodeURIComponent(name)}`,
    check,
  );
  return r.check;
}

export async function deleteCheck(name: string): Promise<void> {
  await send('DELETE', `/api/v1/admin/checks/${encodeURIComponent(name)}`);
}

/** Deletes the recorded history and KEEPS the check, which then reads as its
 *  default status until the next probe lands. Deliberately a separate call from
 *  deleteCheck: "I want a clean slate" and "this check should not exist" are
 *  different intentions with very different consequences. Returns rows removed. */
export async function purgeCheckData(name: string): Promise<number> {
  const r = await send<{ purged: number }>(
    'DELETE',
    `/api/v1/admin/checks/${encodeURIComponent(name)}/data`,
  );
  return r.purged;
}

/** Deletes every result the agent ever reported, which is what removes it from
 *  the agent list — that list is derived from those rows, there is no registry.
 *  409 when checks still name it; the server's message lists them, so callers
 *  render it verbatim. Returns rows removed. */
export async function purgeAgent(id: string): Promise<number> {
  const r = await send<{ purged: number }>(
    'DELETE',
    `/api/v1/admin/agents/${encodeURIComponent(id)}`,
  );
  return r.purged;
}

export async function fetchSections(): Promise<Section[]> {
  const r = await request<{ sections: Section[] }>('/api/v1/admin/sections');
  return r.sections;
}

export async function createSection(name: string): Promise<Section> {
  const r = await send<{ section: Section }>('POST', '/api/v1/admin/sections', { name });
  return r.section;
}

/** Rename and/or set the collapsed flag. A rename moves every check in the
 *  section with it — callers do not patch checks. Omitting `collapsed` leaves
 *  it as it is, so this one call serves both the rename and the toggle. */
export async function updateSection(
  current: string,
  next: { name?: string; collapsed?: boolean },
): Promise<Section> {
  const r = await send<{ section: Section }>(
    'PUT',
    `/api/v1/admin/sections/${encodeURIComponent(current)}`,
    { name: next.name ?? current, collapsed: next.collapsed },
  );
  return r.section;
}

/** Its checks survive and become loose. */
export async function deleteSection(name: string): Promise<void> {
  await send('DELETE', `/api/v1/admin/sections/${encodeURIComponent(name)}`);
}

export async function reorderSections(order: string[]): Promise<Section[]> {
  const r = await send<{ sections: Section[] }>('PUT', '/api/v1/admin/sections', { order });
  return r.sections;
}

/** Names in display order — loose checks first, then each section's. Names left
 *  out keep their relative order after the listed ones. */
export async function reorderChecks(order: string[]): Promise<AdminCheck[]> {
  const r = await send<{ checks: AdminCheck[] }>('PUT', '/api/v1/admin/checks/order', { order });
  return r.checks;
}

/** Dry-run a Lua snippet against a sample probe result. The guard against a
 *  typo silently marking a service down forever. */
export function testEval(sample: EvalSample): Promise<EvalResult> {
  return send<EvalResult>('POST', '/api/v1/admin/eval-test', sample);
}
