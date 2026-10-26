/** Optional visual hint for country-coded regions; a region is not required to be a country.
 * No geo lookup and no invented country for custom names such as "eu-west".
 */
const COUNTRY_CODES = new Set([
  'au', 'at', 'be', 'br', 'ca', 'ch', 'cn', 'cz', 'de', 'dk', 'es', 'fi', 'fr', 'gb',
  'hk', 'ie', 'il', 'in', 'it', 'jp', 'kr', 'mx', 'my', 'nl', 'no', 'nz', 'ph', 'pl',
  'pt', 'se', 'sg', 'th', 'tw', 'ua', 'uk', 'us', 'vn', 'za',
]);

export function regionFlag(region: string): string | null {
  const code = region.trim().toLowerCase().split('-', 1)[0];
  // UK is a common nickname; ISO 3166-1 actually uses GB.
  const normalized = code === 'uk' ? 'gb' : code;
  if (!COUNTRY_CODES.has(code)) return null;
  return [...normalized.toUpperCase()].map((c) => String.fromCodePoint(0x1f1e6 + c.charCodeAt(0) - 65)).join('');
}
