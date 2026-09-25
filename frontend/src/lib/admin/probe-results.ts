import type { ProbeStatus } from "../api";
import { request } from "./client";

/** One raw probe result as the admin explorer reads it. Carries the VERBATIM
 *  `error` the agent wrote — resolved addresses and expected values and all —
 *  because it only ever arrives behind an admin session. The public per-check
 *  history endpoint summarises this away; here it is the whole point. */
export interface ProbeResult {
  check_id: string;
  /** Mutable display name, for labels — never the identity. */
  name: string;
  agent_id: string;
  /** Epoch millis. */
  timestamp: number;
  zone?: string;
  region?: string;
  status: ProbeStatus;
  latency_ms: number;
  status_code?: number;
  /** Verbatim probe error. Absent on success. */
  error?: string;
  /** The failure as a category, for a coloured pill. Absent on success. */
  failure?: string;
}

/** Server-side narrowing for the explorer. `query` (free-text error search) is
 *  deliberately absent — it runs client-side on the returned batch. `limit`
 *  caps the batch; the server clamps it to 1000. */
export interface ProbeResultsFilter {
  /** A check id — the server rejects anything that does not parse as one. */
  check?: string;
  agent?: string;
  status?: ProbeStatus;
  region?: string;
  zone?: string;
  limit?: number;
}

/** Recent raw results, newest first, narrowed server-side. A 401 throws
 *  `AdminError` like every other admin call, so the panel drives the same
 *  re-login flow. */
export async function fetchProbeResults(
  filter: ProbeResultsFilter,
): Promise<ProbeResult[]> {
  const params = new URLSearchParams();
  // Skip empty fields so an untouched filter is an absent param, not `?check=`.
  if (filter.check) params.set("check", filter.check);
  if (filter.agent) params.set("agent", filter.agent);
  if (filter.status) params.set("status", filter.status);
  if (filter.region) params.set("region", filter.region);
  if (filter.zone) params.set("zone", filter.zone);
  if (filter.limit != null) params.set("limit", String(filter.limit));
  const qs = params.toString();
  const r = await request<{ results: ProbeResult[] }>(
    `/api/v1/admin/results${qs ? `?${qs}` : ""}`,
  );
  return r.results;
}
