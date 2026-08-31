// Shared presentation for the incident model. Kept out of `format.ts` because
// that file is about numbers and clocks; this is about one domain type.
import type { AffectSource, Incident, IncidentState } from './api';

/// The lifecycle read as colour. `detected` is red because nobody has looked at
/// it yet, and amber through blue as a human takes it over — the ramp says how
/// far the response has got, not how bad the outage is.
const STATE_COLORS: Record<IncidentState, string> = {
  detected: '#e5534b',
  investigating: '#d29922',
  identified: '#d29922',
  monitoring: '#539bf5',
  resolved: '#57ab5a',
};

export function incidentStateColor(state: IncidentState): string {
  return STATE_COLORS[state];
}

/// Why a check is on an incident, as a tooltip. The distinction matters to an
/// operator: only `manual` rows are theirs to remove.
export function affectSourceNote(source: AffectSource): string {
  switch (source) {
    case 'origin':
      return 'The check whose failure opened this incident.';
    case 'dependency':
      return 'Attached automatically: this check declares depends_on an upstream that was already down.';
    case 'manual':
      return 'Added by an operator.';
  }
}

/// A timestamp as an absolute, unambiguous string for the public page, where a
/// reader may be in any timezone and "3 hours ago" is not a record.
export function formatAbsolute(ms: number): string {
  return new Date(ms).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short',
  });
}

/// Epoch millis <-> the value an `<input type="datetime-local">` wants, which
/// is local wall-clock with no zone. Round-trips through the same offset, so
/// editing a start time does not silently shift it.
export function toDatetimeLocal(ms: number): string {
  const d = new Date(ms - new Date(ms).getTimezoneOffset() * 60_000);
  return d.toISOString().slice(0, 16);
}

export function fromDatetimeLocal(value: string): number {
  return new Date(value).getTime();
}

/// The pill class for a lifecycle state, in the admin console's palette. Maps
/// to `.opill` tones in app.css so a state reads the same in a table, a badge
/// and a timeline entry.
export function stateTone(state: IncidentState): 'red' | 'amber' | 'cyan' | 'green' {
  switch (state) {
    case 'detected':
      return 'red';
    case 'investigating':
    case 'identified':
      return 'amber';
    case 'monitoring':
      return 'cyan';
    case 'resolved':
      return 'green';
  }
}

/// Which open incidents render page-wide. `global` is what the flag is *for*:
/// the incident is not about one monitor, and a global one with no checks at
/// all (a maintenance notice) has no monitor row to render against.
export function openGlobalIncidents(incidents: Incident[]): Incident[] {
  return incidents.filter((i) => i.global && i.state !== 'resolved');
}

/// The open incident to mark a monitor row with, or `null`. Globals are
/// deliberately excluded: they already render page-wide, and repeating one
/// title across the eight rows it affects makes one outage look like eight.
///
/// Newest `started_at` wins when a check sits in two at once — that is the one
/// still being written about. Read from the field rather than trusting the
/// caller's array order, which a websocket insert can reshuffle.
export function openIncidentForCheck(incidents: Incident[], checkId: string): Incident | null {
  let best: Incident | null = null;
  for (const i of incidents) {
    if (i.global || i.state === 'resolved') continue;
    if (!i.checks.some((c) => c.check_id === checkId)) continue;
    if (best === null || i.started_at > best.started_at) best = i;
  }
  return best;
}
