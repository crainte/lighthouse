import { describe, expect, it } from 'vitest';

import { bandParts, computeProbeRows, probeTone } from './probeGrid';
import type { HistoryPoint } from './api';

function point(agent_id: string, timestamp: number, status: HistoryPoint['status'] = 'up'): HistoryPoint {
  return { agent_id, timestamp, status, latency_ms: 10 };
}

describe('computeProbeRows', () => {
  it('groups by agent, oldest to newest, trimmed to whole rows', () => {
    // Newest-first input, as the API serves it; a leftover older than the last
    // whole row is dropped, keeping the newest `cols`-sized batch.
    const points = [point('a', 5), point('a', 4), point('a', 3), point('b', 5), point('b', 4)];
    const rows = computeProbeRows(points, 2, 100);
    expect(rows).toEqual([
      { agent_id: 'a', probes: [point('a', 4), point('a', 5)] },
      { agent_id: 'b', probes: [point('b', 4), point('b', 5)] },
    ]);
  });

  it('keeps a short run as-is rather than dropping it to zero', () => {
    const points = [point('a', 1)];
    const rows = computeProbeRows(points, 60, 100);
    expect(rows).toEqual([{ agent_id: 'a', probes: [point('a', 1)] }]);
  });

  it('caps how many of one agent are kept', () => {
    const points = Array.from({ length: 10 }, (_, i) => point('a', i));
    const rows = computeProbeRows(points, 2, 4);
    expect(rows[0].probes.length).toBe(4);
  });

  it('sorts agents alphabetically', () => {
    const points = [point('z', 1), point('a', 1)];
    const rows = computeProbeRows(points, 1, 10);
    expect(rows.map((r) => r.agent_id)).toEqual(['a', 'z']);
  });
});

describe('bandParts', () => {
  it('renders min/avg/max, rounding only the average', () => {
    expect(bandParts(7, 19.4, 109)).toEqual(['7', '19', '109']);
  });

  it('is null when there is no average to show', () => {
    expect(bandParts(null, null, null)).toBeNull();
  });
});

describe('probeTone', () => {
  it('maps up/down straight through and everything else to nodata', () => {
    expect(probeTone('up')).toBe('up');
    expect(probeTone('down')).toBe('down');
    expect(probeTone('unknown')).toBe('nodata');
  });
});
