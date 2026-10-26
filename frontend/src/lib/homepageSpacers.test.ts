import { describe, expect, it } from 'vitest';
import { placeSpacers, moveSpacer, serviceBlocks } from './homepageSpacers';

const blocks = [
  { key: 'check:a', label: 'A' },
  { key: 'check:b', label: 'B' },
  { key: 'section:API', label: 'API' },
];

describe('homepage spacers', () => {
  it('anchors inline sections by first check appearance after loose checks and position-ordered pages', () => {
    const checks = [
      { id: 'late', name: 'Late', position: 9, group: 'Alpha' },
      { id: 'loose-b', name: 'Zed', position: 2 },
      { id: 'orphan', name: 'Orphan', position: 3, group: 'Missing' },
      { id: 'early', name: 'Early', position: 1, group: 'Zeta' },
      { id: 'loose-a', name: 'Aye', position: 2 },
    ];
    const sections = [
      { name: 'Alpha', position: 0, display: 'inline' as const },
      { name: 'Page B', position: 7, display: 'page' as const },
      { name: 'Zeta', position: 1, display: 'inline' as const },
      { name: 'Page A', position: 2, display: 'page' as const },
      { name: 'Empty', position: 3, display: 'inline' as const },
    ];
    const blocks = serviceBlocks(checks, sections, (check) => check.id);
    expect(placeSpacers(blocks, [{ id: 'divider', after: 'section:Zeta' }]).map((item) => item.key)).toEqual([
      'check:loose-a', 'check:loose-b', 'section:Page A', 'section:Page B',
      'section:Zeta', 'spacer:divider', 'section:Missing', 'section:Alpha',
    ]);
  });

  it('places multiple spacers between checks and sections without moving the blocks', () => {
    const spacers = [
      { id: 'one', after: 'check:a' },
      { id: 'two', after: 'check:a' },
      { id: 'three', after: 'section:API' },
    ];
    expect(placeSpacers(blocks, spacers).map((item) => item.key)).toEqual([
      'check:a', 'spacer:one', 'spacer:two', 'check:b', 'section:API', 'spacer:three',
    ]);
  });

  it('moves a spacer across a block without changing block order', () => {
    const next = moveSpacer(blocks, [{ id: 'one', after: 'check:a' }], 'one', 1);
    expect(placeSpacers(blocks, next).map((item) => item.key)).toEqual([
      'check:a', 'check:b', 'spacer:one', 'section:API',
    ]);
  });

  it('keeps orphaned spacers visible at the end until they are moved or removed', () => {
    expect(placeSpacers(blocks, [{ id: 'lost', after: 'section:old' }]).at(-1)?.key)
      .toBe('spacer:lost');
  });

  it('renders many spacers on one anchor and moves each independently', () => {
    const many = Array.from({ length: 5 }, (_, n) => ({ id: `s${n}`, after: 'check:a' as string | null }));
    expect(placeSpacers(blocks, many).map((item) => item.key)).toEqual([
      'check:a', 'spacer:s0', 'spacer:s1', 'spacer:s2', 'spacer:s3', 'spacer:s4', 'check:b', 'section:API',
    ]);
    const moved = moveSpacer(blocks, many, 's4', -1);
    expect(moved.map((s) => s.id)).toEqual(['s0', 's1', 's2', 's4', 's3']);
    expect(moved.every((s) => s.after === 'check:a')).toBe(true);
    const across = moveSpacer(blocks, many, 's4', 1);
    expect(across.find((s) => s.id === 's4')?.after).toBe('check:b');
    expect(across.filter((s) => s.id !== 's4').every((s) => s.after === 'check:a')).toBe(true);
  });
});
