import { describe, expect, it } from 'vitest';
import { cronInputError } from './cronValidation';
import { normaliseLegacyCron } from '../components/check-editor/draft';

describe('cron input validation', () => {
  it('accepts common 5-, 6-, and 7-field schedules', () => {
    for (const expression of [
      '*/30 * * * * *',
      '0 * * * * *',
      '0 0 * * * *',
      '*/5 * * * *',
      '0 0 9 * JAN,MAR MON-FRI',
      '0 0 12 ? * MON-FRI',
      '0 0 0 1 1 * 2027',
    ]) {
      expect(cronInputError(expression), expression).toBeNull();
    }
  });

  it('shows actionable errors while typing malformed schedules', () => {
    expect(cronInputError('')).toMatch(/Enter a cron/);
    expect(cronInputError('*/? * * * * *')).toMatch(/seconds step/);
    expect(cronInputError('*/0 * * * * *')).toMatch(/seconds step/);
    expect(cronInputError('*/61 * * * * *')).toMatch(/seconds step/);
    expect(cronInputError('61 * * * * *')).toMatch(/seconds must be between 0 and 59/);
    expect(cronInputError('0 80 * * * *')).toMatch(/minutes must be between/);
    expect(cronInputError('0 0 24 * * *')).toMatch(/hours must be between/);
    expect(cronInputError('0 0 0 32 * *')).toMatch(/day of month must be between/);
    expect(cronInputError('0 0 12 * 13 *')).toMatch(/month must be between/);
    expect(cronInputError('0 0 * * ? *')).toMatch(/\? is only allowed/);
    expect(cronInputError('0 0 1-0 * * *')).toMatch(/range must start/);
    expect(cronInputError('0 0 * *')).toMatch(/Use 6 fields/);
  });

  it('validates canonicalized legacy 60-unit fields like the server', () => {
    expect(cronInputError(normaliseLegacyCron('*/60 * * * * *'))).toBeNull();
    expect(cronInputError(normaliseLegacyCron('0 */60 * * * *'))).toBeNull();
  });

  it('defers advanced day expressions to authoritative server validation', () => {
    expect(cronInputError('0 0 9 1W * ?')).toBeNull();
    expect(cronInputError('0 0 12 ? * 2#1')).toBeNull();
  });
});
