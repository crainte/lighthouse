import type { HistoryPoint } from './api';

/** One labelled block per agent: a single chronological grid mixed the agents
 *  together, where ten of them turn one region's outage into scattered noise
 *  and the reader cannot tell "one probe location is down" from "everything is
 *  flapping". Each block is that agent's own newest probes, oldest to newest,
 *  trimmed to whole rows of `cols` — so rows are flush, and are deliberately
 *  not a shared time axis across agents, which is why each is labelled.
 *
 *  Shared by CheckDetail (newest-N history) and DayDetailModal (one bucket's
 *  exact-window log) — same shape, different source list. */
export function computeProbeRows(
  points: HistoryPoint[],
  cols: number,
  maxPerAgent: number,
): { agent_id: string; probes: HistoryPoint[] }[] {
  // The API serves newest-first, so the first N per agent are the newest.
  const byAgent = new Map<string, HistoryPoint[]>();
  for (const p of points) {
    const row = byAgent.get(p.agent_id);
    if (row === undefined) byAgent.set(p.agent_id, [p]);
    else if (row.length < maxPerAgent) row.push(p);
  }
  return [...byAgent]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([agent_id, newestFirst]) => {
      const whole =
        newestFirst.length >= cols ? Math.floor(newestFirst.length / cols) * cols : newestFirst.length;
      return { agent_id, probes: newestFirst.slice(0, whole).reverse() };
    });
}

/** Three separate numbers, not one string: "7/19/109" reads as a date. */
export function bandParts(
  min: number | null,
  avg: number | null,
  max: number | null,
): string[] | null {
  if (avg === null) return null;
  return [String(min), String(Math.round(avg)), String(max)];
}

/** `.tt-status` has no `unknown` tone — an unrecorded probe reads the same as
 *  an empty bucket on the strip. */
export function probeTone(s: HistoryPoint['status']): string {
  return s === 'up' || s === 'down' ? s : 'nodata';
}
