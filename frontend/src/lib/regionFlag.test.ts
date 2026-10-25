import { describe, expect, it } from 'vitest';
import { regionFlag } from './regionFlag';

describe('regionFlag', () => {
  it('recognizes country-coded regions without backend metadata', () => {
    expect(regionFlag('tw')).toBe('🇹🇼');
    expect(regionFlag('US')).toBe('🇺🇸');
    expect(regionFlag('gb')).toBe('🇬🇧');
    expect(regionFlag('uk')).toBe('🇬🇧');
  });

  it('uses a country-coded prefix for hyphenated regions', () => {
    expect(regionFlag('us-west')).toBe('🇺🇸');
    expect(regionFlag(' DE-west-1 ')).toBe('🇩🇪');
    expect(regionFlag('uk-south')).toBe('🇬🇧');
  });

  it('never guesses a country for custom deployment regions', () => {
    expect(regionFlag('eu-west')).toBeNull();
    expect(regionFlag('gcp-us-west-1')).toBeNull();
    expect(regionFlag('prod-us')).toBeNull();
    expect(regionFlag('-us')).toBeNull();
    expect(regionFlag('prod')).toBeNull();
    expect(regionFlag('')).toBeNull();
    expect(regionFlag('zz')).toBeNull();
  });
});
