import { describe, expect, it } from 'vitest';
import normalScene from '../assets/lighthouse-normal.png';
import springScene from '../assets/lighthouse-spring.png';
import summerScene from '../assets/lighthouse-summer.png';
import fallScene from '../assets/lighthouse-fall.png';
import winterScene from '../assets/lighthouse-winter.png';
import { sceneForSeason } from './seasonalScene';
import { DEFAULT_SEASONAL_PREFERENCES, resolveSeason } from './seasonalAtmosphere';

describe('shared seasonal sidebar artwork', () => {
  it('uses the same scene assets as the Overview hero', () => {
    expect(sceneForSeason('spring')).toBe(springScene);
    expect(sceneForSeason('summer')).toBe(summerScene);
    expect(sceneForSeason('fall')).toBe(fallScene);
    expect(sceneForSeason('winter')).toBe(winterScene);
    expect(sceneForSeason(null)).toBe(normalScene);
  });

  it('honors off, auto and manually selected seasonal preferences', () => {
    const inAutumn = new Date(2026, 9, 15, 12);
    expect(sceneForSeason(resolveSeason(DEFAULT_SEASONAL_PREFERENCES, inAutumn))).toBe(fallScene);
    expect(sceneForSeason(resolveSeason({ ...DEFAULT_SEASONAL_PREFERENCES, mode: 'off' }, inAutumn))).toBe(normalScene);
    expect(sceneForSeason(resolveSeason({ ...DEFAULT_SEASONAL_PREFERENCES, mode: 'on', season: 'winter' }, inAutumn))).toBe(winterScene);
  });
});
