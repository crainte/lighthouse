import { describe, expect, it } from 'vitest';

import { atBound, clamp, stepValue } from './number';

describe('stepValue', () => {
  it('steps by one when no step is given', () => {
    expect(stepValue(3, 1)).toBe(4);
    expect(stepValue(3, -1)).toBe(2);
  });

  it('starts an empty field at min', () => {
    expect(stepValue(null, 1, { min: 1 })).toBe(1);
    expect(stepValue(null, -1, { min: 1 })).toBe(1);
  });

  it('starts an empty field at zero when there is no min', () => {
    expect(stepValue(null, 1)).toBe(0);
  });

  it('clamps at both ends', () => {
    expect(stepValue(1, -1, { min: 1 })).toBe(1);
    expect(stepValue(599, 1, { min: 100, max: 599 })).toBe(599);
  });

  it('steps a value back into range rather than further out', () => {
    expect(stepValue(9000, 1, { max: 599 })).toBe(599);
    expect(stepValue(9000, -1, { max: 599 })).toBe(599);
    expect(stepValue(-5, 1, { min: 0 })).toBe(0);
  });

  it('honours a step larger than one', () => {
    expect(stepValue(10, 1, { step: 5 })).toBe(15);
    expect(stepValue(10, -1, { step: 5 })).toBe(5);
  });

  it('snaps an off-grid value to the grid', () => {
    expect(stepValue(7, 1, { step: 5 })).toBe(10);
    expect(stepValue(7, -1, { step: 5 })).toBe(5);
  });

  it('anchors the grid at min', () => {
    expect(stepValue(12, 1, { min: 2, step: 5 })).toBe(17);
    expect(stepValue(13, 1, { min: 2, step: 5 })).toBe(17);
  });

  it('does not accumulate float noise', () => {
    // 0.2 + 0.1 is 0.30000000000000004 in binary floating point, and a form
    // field showing that instead of 0.3 is the bug this rounding exists for.
    expect(stepValue(0.2, 1, { step: 0.1 })).toBe(0.3);
  });

  it('snaps an off-grid fraction rather than adding to it', () => {
    expect(stepValue(0.1, 1, { step: 0.2 })).toBe(0.2);
  });

  it('treats a non-positive step as one', () => {
    expect(stepValue(4, 1, { step: 0 })).toBe(5);
  });

  it('treats NaN like an empty field', () => {
    expect(stepValue(NaN, 1, { min: 3 })).toBe(3);
  });
});

describe('clamp', () => {
  it('leaves an in-range value alone', () => {
    expect(clamp(5, { min: 1, max: 10 })).toBe(5);
  });

  it('pulls to the nearer bound', () => {
    expect(clamp(-2, { min: 1, max: 10 })).toBe(1);
    expect(clamp(99, { min: 1, max: 10 })).toBe(10);
  });
});

describe('atBound', () => {
  it('is false for an empty field, which can always be stepped into range', () => {
    expect(atBound(null, 1, { max: 10 })).toBe(false);
    expect(atBound(null, -1, { min: 1 })).toBe(false);
  });

  it('is true only at the matching end', () => {
    expect(atBound(1, -1, { min: 1 })).toBe(true);
    expect(atBound(1, 1, { min: 1 })).toBe(false);
    expect(atBound(10, 1, { min: 1, max: 10 })).toBe(true);
  });

  it('is true past the bound as well as at it', () => {
    expect(atBound(0, -1, { min: 1 })).toBe(true);
  });

  it('is false when the relevant bound is absent', () => {
    expect(atBound(9999, 1, { min: 1 })).toBe(false);
  });
});
