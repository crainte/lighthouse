import type { Agent } from '../../lib/api';
import { UNKNOWN_LOCATION } from '../../lib/api';

export type SortKey =
  | 'agent'
  | 'region'
  | 'zone'
  | 'status'
  | 'seen'
  | 'rtt'
  | 'reporting'
  | 'results'
  | 'pinned';

export type RegionStat = { count: number; up: number; results: number; zones: number };
export type ZoneStat = { region: string; zone: string; count: number; up: number; results: number };

export type Row =
  | { kind: 'region'; key: string; region: string; stat: RegionStat }
  | { kind: 'zone'; key: string; zone: ZoneStat }
  | { kind: 'agent'; key: string; agent: Agent };

export const zoneKey = (region: string, zone: string) => `${region}\u0000${zone}`;

/** Alphabetical, except the unplaced bucket always sinks to the bottom: it is
 *  a configuration gap rather than a location, and it must not sit between
 *  two real regions where it reads like one. */
export function byLocation(a: string, b: string): number {
  if ((a === UNKNOWN_LOCATION) !== (b === UNKNOWN_LOCATION))
    return a === UNKNOWN_LOCATION ? 1 : -1;
  return a.localeCompare(b);
}

export const placeName = (v: string) => (v === UNKNOWN_LOCATION ? 'unplaced' : v);
