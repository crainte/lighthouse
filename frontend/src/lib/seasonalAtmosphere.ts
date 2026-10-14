export type Season = 'fall' | 'winter' | 'spring' | 'summer';
export type SeasonalMode = 'off' | 'auto' | 'on';

export type SeasonalPreferences = {
  mode: SeasonalMode;
  season: Season;
  holidayExtras: boolean;
};

export type HolidayDecision = {
  eligible: boolean;
  shown: boolean;
  delayMs: number;
};

export const SEASONAL_STORAGE_KEY = 'lightkeeper-seasonal-atmosphere-v1';
export const HOLIDAY_SESSION_KEY = 'lightkeeper-holiday-sky-v1';
export const DEFAULT_SEASONAL_PREFERENCES: SeasonalPreferences = {
  mode: 'auto',
  season: 'winter',
  holidayExtras: true,
};

const seasons: readonly Season[] = ['fall', 'winter', 'spring', 'summer'];
const modes: readonly SeasonalMode[] = ['off', 'auto', 'on'];

export function seasonForDate(date: Date): Season {
  const month = date.getMonth();
  if (month === 11 || month < 2) return 'winter';
  if (month < 5) return 'spring';
  if (month < 8) return 'summer';
  return 'fall';
}

export function isHolidayPeriod(date: Date): boolean {
  return date.getMonth() === 11 && date.getDate() >= 18;
}

export function parseSeasonalPreferences(raw: string | null): SeasonalPreferences {
  if (!raw) return { ...DEFAULT_SEASONAL_PREFERENCES };
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return { ...DEFAULT_SEASONAL_PREFERENCES };
    const value = parsed as Record<string, unknown>;
    if (
      value.version !== 1 ||
      !modes.includes(value.mode as SeasonalMode) ||
      !seasons.includes(value.season as Season) ||
      typeof value.holidayExtras !== 'boolean'
    ) {
      return { ...DEFAULT_SEASONAL_PREFERENCES };
    }
    return {
      mode: value.mode as SeasonalMode,
      season: value.season as Season,
      holidayExtras: value.holidayExtras,
    };
  } catch {
    return { ...DEFAULT_SEASONAL_PREFERENCES };
  }
}

export function loadSeasonalPreferences(storage: Pick<Storage, 'getItem'>): SeasonalPreferences {
  try {
    return parseSeasonalPreferences(storage.getItem(SEASONAL_STORAGE_KEY));
  } catch {
    return { ...DEFAULT_SEASONAL_PREFERENCES };
  }
}

export function saveSeasonalPreferences(
  storage: Pick<Storage, 'setItem'>,
  preferences: SeasonalPreferences,
): void {
  try {
    storage.setItem(SEASONAL_STORAGE_KEY, JSON.stringify({ version: 1, ...preferences }));
  } catch {
  }
}

export function resolveSeason(preferences: SeasonalPreferences, date: Date): Season | null {
  if (preferences.mode === 'off') return null;
  return preferences.mode === 'on' ? preferences.season : seasonForDate(date);
}

function parseHolidayDecision(raw: string | null): HolidayDecision | null {
  if (!raw) return null;
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    if (
      typeof value.eligible !== 'boolean' ||
      typeof value.shown !== 'boolean' ||
      typeof value.delayMs !== 'number' ||
      value.delayMs < 45_000 ||
      value.delayMs > 120_000
    ) return null;
    return { eligible: value.eligible, shown: value.shown, delayMs: value.delayMs };
  } catch {
    return null;
  }
}

export function holidayDecision(
  storage: Pick<Storage, 'getItem' | 'setItem'>,
  random: () => number = Math.random,
): HolidayDecision {
  try {
    const existing = parseHolidayDecision(storage.getItem(HOLIDAY_SESSION_KEY));
    if (existing) return existing;
    const decision = {
      eligible: random() < 0.2,
      shown: false,
      delayMs: 45_000 + Math.floor(random() * 75_000),
    };
    storage.setItem(HOLIDAY_SESSION_KEY, JSON.stringify(decision));
    return decision;
  } catch {
    return { eligible: false, shown: false, delayMs: 120_000 };
  }
}

export function markHolidayShown(
  storage: Pick<Storage, 'setItem'>,
  decision: HolidayDecision,
): HolidayDecision {
  const shown = { ...decision, shown: true };
  try {
    storage.setItem(HOLIDAY_SESSION_KEY, JSON.stringify(shown));
  } catch {
  }
  return shown;
}
