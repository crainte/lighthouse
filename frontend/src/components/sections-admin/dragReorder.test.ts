import { describe, expect, it } from 'vitest';

import { dropEdge, reorder } from './dragReorder';

describe('reorder', () => {
  it('moves an item without mutating the input', () => {
    const input = ['a', 'b', 'c'];
    expect(reorder(input, 0, 2)).toEqual(['b', 'c', 'a']);
    expect(input).toEqual(['a', 'b', 'c']);
  });

  it('moves backwards too', () => {
    expect(reorder(['a', 'b', 'c'], 2, 0)).toEqual(['c', 'a', 'b']);
  });
});

describe('dropEdge', () => {
  it('is null with no drag, a different list, or a different hovered row', () => {
    expect(dropEdge(null, 'sections', 1)).toBeNull();
    expect(dropEdge({ list: 'checks', from: 0, over: 1 }, 'sections', 1)).toBeNull();
    expect(dropEdge({ list: 'sections', from: 0, over: 2 }, 'sections', 1)).toBeNull();
  });

  it('reads below when dragging down, above when dragging up', () => {
    expect(dropEdge({ list: 'sections', from: 0, over: 2 }, 'sections', 2)).toBe('below');
    expect(dropEdge({ list: 'sections', from: 3, over: 1 }, 'sections', 1)).toBe('above');
  });
});
