import { describe, expect, it } from 'vitest';
import { buildIdentity } from './admin';
import { reportedBuild } from './admin/cluster';

describe('buildIdentity', () => {
  it('does not interpret regional reporting origins as software builds', () => {
    expect(reportedBuild('origin us-coordinator-1')).toBeNull();
    expect(reportedBuild(null)).toBeNull();
    expect(reportedBuild('')).toBeNull();
    expect(reportedBuild('0.1.0 (abc123, built today)')).toBe('0.1.0 (abc123, built today)');
  });
  it('ignores compile time when comparing builds', () => {
    expect(buildIdentity('0.1.0 (unknown, built 2026-09-07 21:36:37 UTC)')).toBe(
      '0.1.0 (unknown)',
    );
    expect(buildIdentity('0.1.0 (abc123, built 2026-09-07 21:36:37 UTC)')).not.toBe(
      buildIdentity('0.1.0 (def456, built 2026-09-07 21:36:37 UTC)'),
    );
  });
});
