import { describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_SEASONAL_PREFERENCES,
  HOLIDAY_SESSION_KEY,
  SEASONAL_STORAGE_KEY,
  holidayDecision,
  isHolidayPeriod,
  loadSeasonalPreferences,
  markHolidayShown,
  parseSeasonalPreferences,
  resolveSeason,
  saveSeasonalPreferences,
  seasonForDate,
} from './seasonalAtmosphere';

const localDate = (year: number, month: number, day = 1) => new Date(year, month - 1, day, 12);

describe('seasonal atmosphere', () => {
  it('uses northern meteorological seasons from the local date', () => {
    expect(seasonForDate(localDate(2026, 2, 28))).toBe('winter');
    expect(seasonForDate(localDate(2026, 3, 1))).toBe('spring');
    expect(seasonForDate(localDate(2026, 6, 1))).toBe('summer');
    expect(seasonForDate(localDate(2026, 9, 1))).toBe('fall');
    expect(seasonForDate(localDate(2026, 12, 1))).toBe('winter');
  });

  it('limits holiday extras to December 18 through 31', () => {
    expect(isHolidayPeriod(localDate(2026, 12, 17))).toBe(false);
    expect(isHolidayPeriod(localDate(2026, 12, 18))).toBe(true);
    expect(isHolidayPeriod(localDate(2026, 12, 31))).toBe(true);
    expect(isHolidayPeriod(localDate(2027, 1, 1))).toBe(false);
  });

  it('resolves off, automatic, and manual modes', () => {
    const date = localDate(2026, 7);
    expect(resolveSeason({ ...DEFAULT_SEASONAL_PREFERENCES, mode: 'off' }, date)).toBeNull();
    expect(resolveSeason(DEFAULT_SEASONAL_PREFERENCES, date)).toBe('summer');
    expect(resolveSeason({ mode: 'on', season: 'fall', holidayExtras: true }, date)).toBe('fall');
  });

  it('round-trips preferences and falls back on malformed or blocked storage', () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    };
    const preferences = { mode: 'on' as const, season: 'spring' as const, holidayExtras: false };
    saveSeasonalPreferences(storage, preferences);
    expect(values.has(SEASONAL_STORAGE_KEY)).toBe(true);
    expect(loadSeasonalPreferences(storage)).toEqual(preferences);
    expect(parseSeasonalPreferences('{nope')).toEqual(DEFAULT_SEASONAL_PREFERENCES);
    expect(loadSeasonalPreferences({ getItem: () => { throw new Error('blocked'); } })).toEqual(DEFAULT_SEASONAL_PREFERENCES);
    expect(() => saveSeasonalPreferences({ setItem: () => { throw new Error('blocked'); } }, preferences)).not.toThrow();
  });

  it('makes and reuses one bounded session holiday decision', () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: vi.fn((key: string, value: string) => values.set(key, value)),
    };
    const random = vi.fn().mockReturnValueOnce(0.19).mockReturnValueOnce(1);
    const first = holidayDecision(storage, random);
    expect(first).toEqual({ eligible: true, shown: false, delayMs: 120_000 });
    expect(holidayDecision(storage, random)).toEqual(first);
    expect(random).toHaveBeenCalledTimes(2);
    expect(storage.setItem).toHaveBeenCalledWith(HOLIDAY_SESSION_KEY, expect.any(String));
    expect(markHolidayShown(storage, first).shown).toBe(true);
  });
});
