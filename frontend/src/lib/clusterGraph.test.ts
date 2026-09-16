import { describe, expect, it } from 'vitest';
import { classifyLink, graphLayout, linkPath } from './clusterGraph';

const member = (node_id: string) => ({
  node_id,
  url: '',
  generation: 1,
  last_seen: 1,
  state: 'alive' as const,
});

describe('cluster graph', () => {
  it('lays nodes out deterministically with self in the center', () => {
    const one = graphLayout(member('a'), []);
    expect(one).toEqual([{ id: 'a', x: 400, y: 220, self: true, member: member('a') }]);

    const many = graphLayout(member('a'), [member('c'), member('b')]);
    expect(many[0]).toMatchObject({ id: 'a', x: 400, y: 220, self: true });
    expect(many.slice(1).map((node) => node.id)).toEqual(['b', 'c']);
    expect(graphLayout(member('a'), [member('c'), member('b')])).toEqual(many);
  });

  it('separates opposite directed edges', () => {
    const a = { x: 100, y: 100 };
    const b = { x: 300, y: 100 };
    expect(linkPath(a, b, 1)).not.toBe(linkPath(b, a, 1));
  });

  it('classifies fresh, failed, stale, and absent observations', () => {
    expect(classifyLink(undefined, 10_000, 2_000)).toBe('unknown');
    expect(
      classifyLink(
        { source: 'a', target: 'b', source_generation: 1, observed_at: 9_000, outcome: 'connected' },
        10_000,
        2_000,
      ),
    ).toBe('connected');
    expect(
      classifyLink(
        { source: 'a', target: 'b', source_generation: 1, observed_at: 9_000, outcome: 'failed' },
        10_000,
        2_000,
      ),
    ).toBe('failed');
    expect(
      classifyLink(
        { source: 'a', target: 'b', source_generation: 1, observed_at: 7_000, outcome: 'connected' },
        10_000,
        2_000,
      ),
    ).toBe('stale');
  });
});
