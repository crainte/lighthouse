import { describe, expect, it } from 'vitest';
import { buildIdentity } from './admin';

describe('buildIdentity', () => {
  it('ignores compile time when comparing builds', () => {
    expect(buildIdentity('0.1.0 (unknown, built 2026-09-07 21:36:37 UTC)')).toBe(
      '0.1.0 (unknown)',
    );
    expect(buildIdentity('0.1.0 (abc123, built 2026-09-07 21:36:37 UTC)')).not.toBe(
      buildIdentity('0.1.0 (def456, built 2026-09-07 21:36:37 UTC)'),
    );
  });
});
