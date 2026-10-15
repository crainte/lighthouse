import { describe, expect, it } from 'vitest';
import { regionFlag } from './regionFlag';

describe('regionFlag', () => {
  it('recognizes country-coded regions without backend metadata', () => {
    expect(regionFlag('tw')).toBe('🇹🇼');
    expect(regionFlag('US')).toBe('🇺🇸');
    expect(regionFlag('gb')).toBe('🇬🇧');
    expect(regionFlag('uk')).toBe('🇬🇧');
  });

  it('never guesses a country for custom deployment regions', () => {
    expect(regionFlag('eu-west')).toBeNull();
    expect(regionFlag('prod')).toBeNull();
    expect(regionFlag('')).toBeNull();
    expect(regionFlag('zz')).toBeNull();
  });
});
