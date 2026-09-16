/**
 * Step arithmetic for the number stepper.
 *
 * Split out of the component so the awkward cases — an empty field, a value
 * already past its bound, a step that does not divide the current value — are
 * covered by tests rather than by clicking the arrows and hoping. The component
 * itself is then only wiring: pointer events in, this function out.
 */

export type StepOpts = {
  min?: number;
  max?: number;
  /** Defaults to 1. Values <= 0 are ignored and treated as 1. */
  step?: number;
};

/** Round to 6 decimals: 0.1 + 0.2 must read as 0.3 in a form field. */
function tidy(n: number): number {
  return Math.round(n * 1e6) / 1e6;
}

export function clamp(value: number, opts: StepOpts = {}): number {
  const { min, max } = opts;
  let v = value;
  if (min !== undefined && v < min) v = min;
  if (max !== undefined && v > max) v = max;
  return tidy(v);
}

/**
 * The value one press of an arrow produces.
 *
 * An empty field starts at `min` rather than at zero — the fields using this are
 * counts and durations whose floor is 1, and stepping up from empty should land
 * on the first legal value, not on an illegal one.
 */
export function stepValue(value: number | null, dir: 1 | -1, opts: StepOpts = {}): number {
  const { min, max } = opts;
  const step = opts.step !== undefined && opts.step > 0 ? opts.step : 1;

  if (value === null || !Number.isFinite(value)) {
    // Stepping down from empty is still a step up into range: there is no
    // value below the floor to move to.
    return clamp(min ?? 0, opts);
  }

  // Snap to the step grid first, so a hand-typed 7 with step 5 goes to 10 or 5
  // rather than to 12 or 2. The grid is anchored at `min` when there is one.
  const base = min ?? 0;
  const offset = (value - base) / step;
  const onGrid = Math.abs(offset - Math.round(offset)) < 1e-9;

  let next: number;
  if (onGrid) {
    next = value + dir * step;
  } else {
    next = base + (dir === 1 ? Math.ceil(offset) : Math.floor(offset)) * step;
  }

  // A value already outside its bounds steps back into range rather than
  // further out: clamping alone would let `max + 10` step up and stay put.
  if (max !== undefined && value > max) next = dir === -1 ? max : max;
  if (min !== undefined && value < min) next = min;

  return clamp(next, opts);
}

/** Whether an arrow should be disabled: pressing it could not change anything. */
export function atBound(value: number | null, dir: 1 | -1, opts: StepOpts = {}): boolean {
  if (value === null || !Number.isFinite(value)) return false;
  const { min, max } = opts;
  if (dir === 1) return max !== undefined && value >= max;
  return min !== undefined && value <= min;
}
