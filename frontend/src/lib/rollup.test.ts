import { describe, expect, it } from 'vitest';

import type { RegionTreeState, RollupPolicy, Threshold } from './rollup';
import {
  NODE_DEFAULTS,
  policyLabel,
  policyProblem,
  rollup,
  samePolicy,
  satisfied,
  thresholdPhrase,
} from './rollup';

/* These cases are transcribed from the Rust tests in `src/rollup.rs`. This file
 * is a mirror of that module, and the only thing keeping the two honest is that
 * they agree on the same table. If a case here is changed, change it there. */

const ZONE = NODE_DEFAULTS.zone;
const REGION = NODE_DEFAULTS.region;
const CHECK = NODE_DEFAULTS.global;

const up: RegionTreeState = 'up';
const down: RegionTreeState = 'down';
const degraded: RegionTreeState = 'degraded';
const unknown: RegionTreeState = 'unknown';

describe('satisfied', () => {
  it('counts correctly', () => {
    const cases: [Threshold, number, number, boolean][] = [
      ['any', 0, 3, false],
      ['any', 1, 3, true],
      ['majority', 1, 2, false],
      ['majority', 2, 2, true],
      ['majority', 2, 3, true],
      ['majority', 1, 3, false],
      ['all', 2, 3, false],
      ['all', 3, 3, true],
      [{ quorum: 2 }, 1, 3, false],
      [{ quorum: 2 }, 2, 3, true],
      // Clamped down: a quorum larger than the fleet becomes "all".
      [{ quorum: 9 }, 2, 2, true],
      [{ quorum: 9 }, 1, 2, false],
      // Nothing is satisfiable with no children.
      ['any', 0, 0, false],
      ['all', 0, 0, false],
    ];
    for (const [t, k, n, want] of cases) {
      expect(satisfied(t, k, n), `${JSON.stringify(t)} with ${k} of ${n}`).toBe(want);
    }
  });
});

describe('rollup', () => {
  it('treats no opinions as unknown, not down', () => {
    expect(rollup(ZONE, [])).toBe(unknown);
    // A fleet that is entirely silent is the same case: every child filtered
    // out leaves nothing to judge.
    expect(rollup(ZONE, [unknown, unknown])).toBe(unknown);
  });

  it('drops silent children from the denominator', () => {
    // The property the whole module hangs on. A silent agent neither vetoes a
    // real outage nor votes one into existence.
    expect(rollup(ZONE, [down, unknown]), 'a silent agent must not veto an outage').toBe(down);
    expect(rollup(ZONE, [up, unknown])).toBe(up);
  });

  it('never lets a single flaky agent take its zone down', () => {
    // The `ntfy-remote` bug, at the level where it is fixed.
    expect(rollup(ZONE, [down, up]), 'one dissenting agent is a broken agent').toBe(up);
    expect(rollup(ZONE, [up, down])).toBe(up);
    // Zones are never degraded: "my two agents disagree" is not a fact about
    // the service.
    expect(rollup(ZONE, [down, up])).not.toBe(degraded);
  });

  it('downs a zone only when the whole zone agrees', () => {
    expect(rollup(ZONE, [down, down])).toBe(down);
    expect(rollup(ZONE, [down, down, down])).toBe(down);
  });

  it('degrades a region on one dead zone but does not down it', () => {
    expect(rollup(REGION, [down, up]), 'the region is still serving').toBe(degraded);
    expect(rollup(REGION, [down, down])).toBe(down);
    expect(rollup(REGION, [up, up])).toBe(up);
  });

  it('reads a regional outage as a partial outage', () => {
    // us-east down, eu up. Under a flat majority this published UP and the
    // outage vanished.
    expect(rollup(CHECK, [down, up]), 'must not be rounded away to healthy').toBe(degraded);
    expect(rollup(CHECK, [down, down, up])).toBe(down);
    expect(rollup(CHECK, [down, up, up])).toBe(degraded);
  });

  it('counts degraded children toward degraded but never toward down', () => {
    expect(rollup(CHECK, [degraded, degraded]), 'two partial outages are still partial').toBe(
      degraded,
    );
    expect(rollup({ degraded_when: 'any', down_when: 'any' }, [degraded, degraded])).toBe(degraded);
  });

  it('passes a single child through unchanged', () => {
    for (const policy of [ZONE, REGION, CHECK]) {
      expect(rollup(policy, [up])).toBe(up);
      expect(rollup(policy, [down])).toBe(down);
      expect(rollup(policy, [unknown])).toBe(unknown);
    }
  });

  it('tests down before degraded', () => {
    // A policy satisfying both edges reads as down.
    const policy: RollupPolicy = { degraded_when: 'any', down_when: 'any' };
    expect(rollup(policy, [down])).toBe(down);
  });
});

describe('policyProblem', () => {
  it('accepts the built-in defaults', () => {
    for (const policy of [ZONE, REGION, CHECK]) {
      expect(policyProblem(policy)).toBeNull();
    }
  });

  it('rejects a down edge easier than the degraded edge', () => {
    // Down on any, degraded on all: a node would go down without ever being
    // degraded.
    expect(policyProblem({ degraded_when: 'all', down_when: 'any' })).toMatch(/without ever/);
    expect(policyProblem({ degraded_when: 'majority', down_when: 'any' })).toMatch(/without ever/);
  });

  it('rejects a zero quorum', () => {
    expect(policyProblem({ degraded_when: { quorum: 0 }, down_when: 'all' })).toMatch(/at least 1/);
    expect(policyProblem({ degraded_when: 'any', down_when: { quorum: 0 } })).toMatch(/at least 1/);
    expect(policyProblem({ degraded_when: 'any', down_when: { quorum: 1.5 } })).toMatch(
      /whole numbers/,
    );
  });

  it('catches an incoherence that only appears at some fleet sizes', () => {
    // Quorum 2 vs majority: coherent at n=2 (both need 2), incoherent at n=5
    // where majority needs 3 but the quorum still needs 2.
    expect(policyProblem({ degraded_when: 'majority', down_when: { quorum: 2 } })).not.toBeNull();
  });
});

describe('prose', () => {
  it('labels a policy as degraded then down', () => {
    expect(policyLabel(CHECK)).toBe('any / majority');
    expect(policyLabel({ degraded_when: 'all', down_when: { quorum: 3 } })).toBe('all / quorum 3');
  });

  it('phrases each threshold in terms of the child noun', () => {
    expect(thresholdPhrase('any', 'zone')).toBe('at least one zone');
    expect(thresholdPhrase('all', 'zone')).toBe('every zone');
    expect(thresholdPhrase('majority', 'region')).toBe('more than half of the regions');
    expect(thresholdPhrase({ quorum: 1 }, 'agent')).toBe('at least 1 agent');
    expect(thresholdPhrase({ quorum: 3 }, 'agent')).toBe('at least 3 agents');
  });
});

describe('samePolicy', () => {
  it('compares string and quorum thresholds', () => {
    expect(samePolicy(CHECK, { degraded_when: 'any', down_when: 'majority' })).toBe(true);
    expect(samePolicy(CHECK, REGION)).toBe(false);
    expect(
      samePolicy(
        { degraded_when: { quorum: 2 }, down_when: 'all' },
        { degraded_when: { quorum: 2 }, down_when: 'all' },
      ),
    ).toBe(true);
    expect(
      samePolicy(
        { degraded_when: { quorum: 2 }, down_when: 'all' },
        { degraded_when: { quorum: 3 }, down_when: 'all' },
      ),
    ).toBe(false);
  });
});
