/** Friendly, immediate checks for common cron typos.
 *
 * This intentionally does not replace the Rust cron parser: extended day-of-
 * month/week expressions are deferred to the API's authoritative validator.
 */
export function cronInputError(expr: string): string | null {
  const raw = expr.trim();
  if (!raw) return 'Enter a cron schedule.';
  const fields = raw.split(/\s+/);
  if (![5, 6, 7].includes(fields.length)) {
    return 'Use 6 fields (seconds, minutes, hours, day, month, weekday), or the supported 5-field format.';
  }
  const normalized = fields.length === 5 ? ['0', ...fields] : fields;
  const names = ['seconds', 'minutes', 'hours', 'day of month', 'month', 'day of week', 'year'];
  const bounds: [number, number][] = [
    [0, 59], [0, 59], [0, 23], [1, 31], [1, 12], [0, 7], [1970, 2199],
  ];
  const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
  const weekdays = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
  for (let i = 0; i < normalized.length; i++) {
    const name = names[i];
    const [lo, hi] = bounds[i];
    for (const fragment of normalized[i].toUpperCase().split(',')) {
      if (!fragment) return `Empty value in ${name} field.`;
      if (fragment === '*') continue;
      if (fragment === '?') {
        if (i === 3 || i === 5) continue;
        return `? is only allowed in the day-of-month or day-of-week field.`;
      }
      // Leave extended expressions (L, W, #) to the full server-side cron
      // parser, rather than incorrectly rejecting valid supported schedules.
      if ((i === 3 || i === 5) && /[LW#]/.test(fragment)) continue;

      const pieces = fragment.split('/');
      if (pieces.length > 2) return `Invalid step in ${name} field.`;
      const [base, step] = pieces;
      if (step !== undefined) {
        if (!/^\d+$/.test(step)) return `Invalid ${name} step. Use a number after / (for example */5).`;
        const n = Number(step);
        if (n === 0 || !Number.isSafeInteger(n) || n > hi - lo + 1) {
          return `${name} step must be between 1 and ${hi - lo + 1}.`;
        }
      }
      if (base === '*') continue;
      const range = base.split('-');
      if (range.length > 2 || range.length === 0) return `Invalid range in ${name} field.`;
      const value = (token: string): number | null => {
        if (/^\d+$/.test(token)) return Number(token);
        if (i === 4) {
          const index = months.indexOf(token);
          if (index !== -1) return index + 1;
        }
        if (i === 5) {
          const index = weekdays.indexOf(token);
          if (index !== -1) return index;
        }
        return null;
      };
      const start = value(range[0]);
      const end = range.length === 2 ? value(range[1]) : start;
      if (start === null || end === null || !Number.isSafeInteger(start) || !Number.isSafeInteger(end)) {
        return `Invalid ${name} value “${base}”.`;
      }
      if (start < lo || start > hi || end < lo || end > hi) {
        return `${name} must be between ${lo} and ${hi}.`;
      }
      if (start > end) return `${name} range must start before it ends.`;
    }
  }
  // Exact matching, schedule viability and minimum 5-second frequency are
  // independently checked by CheckConfig::validate() on the server.
  return null;
}
