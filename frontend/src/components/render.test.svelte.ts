// Render smoke tests.
//
// These exist because two components shipped that crashed the page the moment
// it loaded, and nothing in a 101-test suite could see it: every other test
// covers a pure function in `lib/`, so no template was ever compiled and run.
// Both failures were the same shape — a template naming a variable that does
// not exist — and both are invisible to logic tests and to an HTTP smoke check,
// which happily reports 200 while the SPA is dead.
//
// The bar here is deliberately low: mount the component and assert it did not
// throw. That is enough to catch an undefined reference, a bad import, or a
// runtime error in a `$derived`, which is the class that actually reached the
// user. Behavioural assertions belong with the components themselves.
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { render, cleanup, screen } from '@testing-library/svelte';
import type { Incident, IncidentGroup } from '../lib/api';
import CheckEditor from './CheckEditor.svelte';
import IncidentsList from './IncidentsList.svelte';
import PastIncidents from './PastIncidents.svelte';

// Components fetch on mount. Give them an endpoint that resolves to nothing
// interesting so mounting is deterministic and offline.
beforeEach(() => {
  // One body that satisfies every on-mount fetch these components make
  // (checks, sections, agents, notifiers, incidents). A missing key would make
  // a component's `load()` throw and render its error panel instead of the
  // form — which is a real failure mode, but not the one under test here.
  const body = {
    checks: [],
    has_more: false,
    incidents: [],
    sections: [],
    agents: [],
    notifiers: [],
  };
  vi.stubGlobal(
    'fetch',
    vi.fn(async () =>
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    ),
  );
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe('component render smoke', () => {
  const noop = () => {};

  // The regression: `placeholder="...?token=${API_TOKEN}"`. Svelte reads
  // `{API_TOKEN}` inside attribute text as an interpolation, so the editor
  // threw on an undefined variable and took the whole admin page with it.
  //
  // Mounting alone is not enough and it is worth saying why: the editor renders
  // a loading state first, and the offending field sits behind
  // `{#if kind === 'http'}`. A test that only asserted "mount did not throw"
  // passed against the real bug. So this waits for the form and asserts the
  // HTTP branch actually rendered — the branch is the test.
  it('renders the check editor http fields without throwing', async () => {
    const errors: unknown[] = [];
    const onError = (e: ErrorEvent) => errors.push(e.error ?? e.message);
    window.addEventListener('error', onError);
    try {
      render(CheckEditor, { props: { id: null, onsaved: noop, onclose: noop } });
      // A new check defaults to `type: 'http'`, so the HTTP fields are the
      // branch a fresh editor shows.
      const target = await screen.findByPlaceholderText(/example\.com\/health/);
      expect(target).toBeInTheDocument();
      expect(errors).toEqual([]);
    } finally {
      window.removeEventListener('error', onError);
    }
  });

  // The regression: a call site still passing `incidents` after the variable
  // was split into `publicIncidents`/`adminIncidents`. It only crashed once an
  // incident actually existed, which is precisely when the page matters.
  const incident: Incident = {
    id: 'check-1-1790000000000',
    title: 'api is down',
    state: 'detected',
    started_at: 1790000000000,
    resolved_at: null,
    duration_ms: 60000,
    origin_check: 'check-1',
    global: false,
    acknowledged: false,
    updates: [],
    // The affected-check rows the incident itself carries, distinct from the
    // `checks` prop that resolves their mutable names.
    checks: [{ check_id: 'check-1', source: 'origin' }],
  };

  const checks = [{ check_id: 'check-1', name: 'api' }];

  it('mounts the incidents list with and without incidents', () => {
    const mount = (incidents: Incident[]) =>
      render(IncidentsList, {
        props: { incidents, checks, onEdit: noop, onAck: noop },
      });
    expect(() => mount([])).not.toThrow();
    cleanup();
    expect(() => mount([incident])).not.toThrow();
  });

  it('mounts past incidents with and without incidents', () => {
    const mount = (incidents: Incident[]) =>
      render(PastIncidents, { props: { incidents, checks, onOpen: noop } });
    expect(() => mount([])).not.toThrow();
    cleanup();
    expect(() => mount([incident])).not.toThrow();
  });

  // The global tier adds `groups`; a regional dashboard never sends it. Both
  // shapes must render, since the same component serves both.
  it('mounts grouped incidents as a global node supplies them', () => {
    const groups: IncidentGroup[] = [
      {
        id: incident.id,
        origin_check: 'check-1',
        title: 'api is down',
        state: 'detected',
        started_at: 1790000000000,
        resolved_at: null,
        regions: [
          { region: 'us', id: incident.id, title: 'api is down', state: 'detected', started_at: 1790000000000, resolved_at: null },
          { region: 'tw', id: 'check-1-1790000060000', title: 'api is down', state: 'detected', started_at: 1790000060000, resolved_at: null },
        ],
      },
    ];
    expect(() =>
      render(PastIncidents, {
        props: { incidents: [incident], checks, onOpen: noop, groups },
      }),
    ).not.toThrow();
  });
});
