/**
 * A faithful mirror of `src/rollup.rs`.
 *
 * The admin UI needs to explain a policy in prose and preview its effect before
 * the operator saves it, which means evaluating the fold client-side. This file
 * is that duplication, kept deliberately small, in one place, and covered by
 * tests transcribed from the Rust ones so the two cannot drift silently.
 *
 * The server remains the authority: everything here is for display and for
 * catching an incoherent policy before it costs a round trip.
 */
import type { RegionTreeState, RollupNodeType, RollupPolicy, Threshold } from './admin';

export type { RegionTreeState, RollupNodeType, RollupPolicy, Threshold } from './admin';

/** Mirrors RollupPolicy::{ZONE,REGION,CHECK}_DEFAULT. */
export const NODE_DEFAULTS: Record<RollupNodeType, RollupPolicy> = {
  // Regions → check. One region down is a partial outage; most regions down is
  // an outage.
  global: { degraded_when: 'any', down_when: 'majority' },
  // Zones → region. A zone losing reach is real signal; the region is only down
  // when every zone agrees.
  region: { degraded_when: 'any', down_when: 'all' },
  // Agents → zone. Redundant agents probe the same target, so disagreement
  // between them is a broken agent, not a broken service. This is corroboration.
  zone: { degraded_when: 'all', down_when: 'all' },
};

/** What a node's children are called, for prose. */
export const CHILD_NOUN: Record<RollupNodeType, string> = {
  global: 'region',
  region: 'zone',
  zone: 'agent',
};

/**
 * A quorum configured against a fleet that has since shrunk is clamped into
 * `1..=n` rather than left permanently unsatisfiable, which would silently
 * disable alerting on the node.
 */
export function clampQuorum(q: number, n: number): number {
  return Math.min(Math.max(q, 1), Math.max(n, 1));
}

/** Is `k` of `n` enough? `n === 0` is never satisfied by anything. */
export function satisfied(t: Threshold, k: number, n: number): boolean {
  if (n === 0) return false;
  if (t === 'any') return k >= 1;
  if (t === 'majority') return k * 2 > n;
  if (t === 'all') return k === n;
  return k >= clampQuorum(t.quorum, n);
}

/** How hard a threshold is to satisfy at a given fleet size. */
export function strictness(t: Threshold, n: number): number {
  if (t === 'any') return 1;
  if (t === 'majority') return Math.floor(n / 2) + 1;
  if (t === 'all') return n;
  return clampQuorum(t.quorum, n);
}

/** Fold child states into this node's state. */
export function rollup(policy: RollupPolicy, children: RegionTreeState[]): RegionTreeState {
  // Unknown children are excluded from the denominator entirely, not counted as
  // healthy and not as failing. A silent agent must never manufacture a phantom
  // outage, nor mask a real one by inflating the healthy count.
  const known = children.filter((s) => s !== 'unknown');
  const n = known.length;
  if (n === 0) return 'unknown';

  const hardDown = known.filter((s) => s === 'down').length;
  const failing = known.filter((s) => s === 'down' || s === 'degraded').length;

  if (satisfied(policy.down_when, hardDown, n)) return 'down';
  if (satisfied(policy.degraded_when, failing, n)) return 'degraded';
  return 'up';
}

/**
 * Mirrors RollupPolicy::validate. Returns a human-readable reason, or null.
 *
 * `down_when` must be no easier to satisfy than `degraded_when`, or a node could
 * go Down without ever being Degraded and the state machine would skip an edge
 * the alerting code assumes exists. Checked across a range of fleet sizes
 * because Majority and Quorum change places as `n` moves.
 */
export function policyProblem(policy: RollupPolicy): string | null {
  for (const t of [policy.degraded_when, policy.down_when]) {
    if (typeof t !== 'string' && !(Number.isInteger(t.quorum) && t.quorum >= 1)) {
      return 'Quorum values must be whole numbers, at least 1.';
    }
  }
  for (let n = 1; n <= 9; n++) {
    if (strictness(policy.down_when, n) < strictness(policy.degraded_when, n)) {
      return `With ${n} ${n === 1 ? 'child' : 'children'}, "down" is easier to satisfy than "degraded", so this node could go down without ever being degraded.`;
    }
  }
  return null;
}

export function sameThreshold(a: Threshold, b: Threshold): boolean {
  if (typeof a === 'string' || typeof b === 'string') return a === b;
  return a.quorum === b.quorum;
}

export function samePolicy(a: RollupPolicy, b: RollupPolicy): boolean {
  return sameThreshold(a.degraded_when, b.degraded_when) && sameThreshold(a.down_when, b.down_when);
}

export function thresholdLabel(value: Threshold): string {
  return typeof value === 'string' ? value : `quorum ${value.quorum}`;
}

/** The terse tree form: degraded threshold, then down threshold. */
export function policyLabel(policy: RollupPolicy): string {
  return `${thresholdLabel(policy.degraded_when)} / ${thresholdLabel(policy.down_when)}`;
}

/** The editor form: a noun phrase naming how many children it takes. */
export function thresholdPhrase(t: Threshold, child: string): string {
  if (t === 'any') return `at least one ${child}`;
  if (t === 'all') return `every ${child}`;
  if (t === 'majority') return `more than half of the ${child}s`;
  return `at least ${t.quorum} ${t.quorum === 1 ? child : `${child}s`}`;
}
