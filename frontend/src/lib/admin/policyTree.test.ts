import { describe, expect, it } from 'vitest';
import { flattenTree, type Branch } from './policyTree';

type N = { name: string; kids?: N[] };
const n = (name: string, ...kids: N[]): N => ({ name, kids });

/** What `tree(1)` would print for these rows — the shape the CSS draws. */
function ascii(rows: Branch<N>[]): string {
  return rows
    .map((r) => {
      if (r.depth === 0) return r.item.name;
      const pad = r.guides.map((g) => (g ? '│   ' : '    ')).join('');
      return `${pad}${r.last ? '└── ' : '├── '}${r.item.name}`;
    })
    .join('\n');
}

const fleet = n(
  'Global',
  n('tw', n('tw-north-1a', n('agent-tw'))),
  n('us', n('us-east-1a', n('agent-us-3')), n('us-west-1a', n('agent-us-1'), n('agent-us-2'))),
);

describe('flattenTree', () => {
  it('draws continuation lines only past ancestors with later siblings', () => {
    const rows = flattenTree(fleet, (x) => x.kids ?? [], () => true);
    expect(ascii(rows)).toBe(
      [
        'Global',
        '├── tw',
        '│   └── tw-north-1a',
        '│       └── agent-tw',
        '└── us',
        '    ├── us-east-1a',
        '    │   └── agent-us-3',
        '    └── us-west-1a',
        '        ├── agent-us-1',
        '        └── agent-us-2',
      ].join('\n'),
    );
  });

  it('hides a collapsed subtree but keeps the node marked as expandable', () => {
    const rows = flattenTree(fleet, (x) => x.kids ?? [], (x) => x.name !== 'us');
    expect(ascii(rows)).toBe(
      ['Global', '├── tw', '│   └── tw-north-1a', '│       └── agent-tw', '└── us'].join('\n'),
    );
    const us = rows.find((r) => r.item.name === 'us')!;
    expect(us).toMatchObject({ hasChildren: true, open: false, last: true, depth: 1 });
    const leaf = rows.find((r) => r.item.name === 'agent-tw')!;
    expect(leaf).toMatchObject({ hasChildren: false, open: false, depth: 3 });
  });
});
