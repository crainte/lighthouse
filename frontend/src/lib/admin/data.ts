import { request, send } from "./client";

/** The unfiltered raw-result total, used to name the exact destructive purge. */
export async function fetchResultsCount(): Promise<number> {
  const r = await request<{ raw_results: number }>("/api/v1/admin/results/count");
  return r.raw_results;
}

/** Deletes every raw probe result. The coordinator's published status remains. */
export async function purgeResults(): Promise<number> {
  const r = await send<{ deleted: number }>("POST", "/api/v1/admin/results/purge");
  return r.deleted;
}

/** Removes resolved incidents no longer than the supplied duration, for cleanup
 *  of brief false alarms. This intentionally has no results counterpart: the
 *  storage helper exists, but the server does not expose an admin route for it. */
export async function purgeShortIncidents(maxSecs: number): Promise<number> {
  const r = await send<{ deleted: number }>(
    "POST",
    "/api/v1/admin/incidents/purge-short",
    { max_secs: maxSecs },
  );
  return r.deleted;
}
