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
import { render, cleanup, screen, fireEvent } from '@testing-library/svelte';
import ClusterAdmin from './ClusterAdmin.svelte';
import RegionsAdmin from './RegionsAdmin.svelte';
import AgentCredentials from './AgentCredentials.svelte';
import RegionTreeAdmin from './RegionTreeAdmin.svelte';
import ProbeResultsAdmin from './ProbeResultsAdmin.svelte';
import type { Incident, IncidentGroup } from '../lib/api';
import CheckEditor from './CheckEditor.svelte';
import IncidentsList from './IncidentsList.svelte';
import PastIncidents from './PastIncidents.svelte';
import NotifiersAdmin from './NotifiersAdmin.svelte';
import MaintenanceAdmin from './MaintenanceAdmin.svelte';

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

  it('bounds a 10k-check policy preview and does not treat omitted children as silent', async () => {
    const policy = { degraded_when: 'any', down_when: 'all' };
    vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(JSON.stringify(
      url.includes('rollup-policies') ? { policies: [] } : { checks: Array.from({ length: 10000 }, (_, i) => ({ check_id: `check-${i}`, state: 'up', effective_policy: policy, regions: [{ region: 'us', state: 'up', effective_policy: policy, zones: [] }] })) },
    ), { headers: { 'content-type': 'application/json' } })));
    const { container } = render(RegionTreeAdmin, { props: { onexpired: noop } });
    await fireEvent.click(await screen.findByRole('button', { name: /region\s*us\s*Edit policy/i }));
    expect(screen.getByText(/Child details unavailable for 10000 checks/)).toBeInTheDocument();
    expect(container.querySelectorAll('.preview-row')).toHaveLength(25);
    expect(screen.queryByText(/0 zones reporting/)).not.toBeInTheDocument();
    expect(screen.getByText(/0 projected changes/)).toBeInTheDocument();
    await fireEvent.click(screen.getByRole('button', { name: 'Next' }));
    expect(screen.getByText('check-25')).toBeInTheDocument();
    await fireEvent.input(screen.getByRole('searchbox', { name: 'Search policy preview' }), { target: { value: 'check-9999' } });
    expect(container.querySelectorAll('.preview-row')).toHaveLength(1);
    expect(screen.getByText('check-9999')).toBeInTheDocument();
  });

  it('draws location policies as a tree whose branches show who owns whom', async () => {
    const policy = { degraded_when: 'any', down_when: 'all' };
    const zone = (name: string, agents: string[]) => ({
      zone: name,
      state: 'up',
      effective_policy: policy,
      agents: agents.map((id) => ({ id, state: 'up', staleness_ms: 1000 })),
    });
    const tree = {
      checks: [
        {
          check_id: 'c1',
          state: 'up',
          effective_policy: policy,
          regions: [
            { region: 'tw', state: 'up', effective_policy: policy, zones: [zone('tw-1', ['a-tw'])] },
            { region: 'us', state: 'up', effective_policy: policy, zones: [zone('us-1', ['a-1', 'a-2'])] },
          ],
        },
      ],
    };
    vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(JSON.stringify(
      url.includes('rollup-policies') ? { policies: [] } : tree,
    ), { headers: { 'content-type': 'application/json' } })));
    const { container } = render(RegionTreeAdmin, { props: { onexpired: noop } });
    await screen.findByRole('tree');
    // Each row's indentation columns, as tree(1) would print them.
    const glyph: Record<string, string> = { pass: '│', blank: ' ', branch: '├', last: '└' };
    const drawn = [...container.querySelectorAll('[role="treeitem"]')].map((row) => {
      const segs = [...row.querySelectorAll('.tree-seg')].map(
        (s) => glyph[[...s.classList].find((c) => c in glyph)!],
      );
      return `${row.getAttribute('aria-level')} ${segs.join('')}`;
    });
    expect(drawn).toEqual([
      '1 ', //       Global
      '2 ├', //      tw
      '3 │└', //     tw-1
      '4 │ └', //    a-tw
      '2 └', //      us
      '3  └', //     us-1
      '4   ├', //    a-1
      '4   └', //    a-2
    ]);

    await fireEvent.click(screen.getByRole('button', { name: 'Collapse us' }));
    expect(container.querySelectorAll('[role="treeitem"]')).toHaveLength(5);
    expect(screen.getByRole('button', { name: 'Expand us' })).toHaveAttribute('aria-expanded', 'false');
  });

  it('shows regional coordinator rosters, build skew, and which coordinator an agent polls', async () => {
    const now = Date.now();
    const member = (node_id: string, build: string | null) => ({ node_id, build, url: '', state: 'alive', last_seen: now, generation: 1 });
    const agent = { agent_id: 'agent-1', region: 'us', zone: 'us-a', status: 'up', last_seen: now, checks_reporting: 1, results_24h: 1, retired: false, assigned_checks: 0, poll_rtt_ms: 3, version: null, kinds: null, coordinator: 'us-c2' };
    vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(JSON.stringify(
      url.includes('/cluster')
        ? {
            self: member('global', '0.1.0 (abc123, built today)'),
            members: [{ ...member('us', '0.1.0 (abc123, built today)'), origin: 'us-c1' }],
            mode: 'global', now, links: [],
            regions: [{ region: 'us', members: [member('us-c1', '0.1.0 (abc123, built today)'), member('us-c2', '0.0.9 (old, built then)')], links: [], now, link_stale_after_ms: 1000, available: true }],
          }
        : { agents: [agent], regions: [] },
    ), { headers: { 'content-type': 'application/json' } })));
    render(ClusterAdmin);
    expect(await screen.findByText('1 reported build differs')).toBeInTheDocument();
    await fireEvent.click(screen.getByRole('button', { name: /us\s*Region/ }));
    expect(screen.getByText('Reporting origin')).toBeInTheDocument();
    expect(screen.getAllByText('us-c1').length).toBeGreaterThan(0);
    await fireEvent.click(screen.getByRole('button', { name: /agent-1/ }));
    expect(screen.getByText('Polls coordinator').nextElementSibling).toHaveTextContent('us-c2');
    expect(screen.getByRole('button', { name: /us-c2/ })).toHaveClass('polled');
  });

  it('issues a real region-scoped agent token from the global node', async () => {
    const now = Date.now();
    const member = (node_id: string) => ({ node_id, build: null, url: '', state: 'alive', last_seen: now, generation: 1 });
    const fetchMock = vi.fn(async (url: string, init?: RequestInit) => new Response(JSON.stringify(
      url.includes('agent-credentials') && init?.method === 'POST'
        ? { credential: { id: 'c1', zone: 'us-east-1a', region: 'us', created_at: now, revoked: false }, token: 'wa_c1.secret' }
        : url.includes('/cluster') ? { self: member('global'), members: [member('us')], mode: 'global', now, links: [] } : url.includes('/regions') ? { regions: [] } : { agents: [] },
    ), { status: init?.method === 'POST' ? 201 : 200, headers: { 'content-type': 'application/json' } }));
    vi.stubGlobal('fetch', fetchMock);
    render(ClusterAdmin);
    await fireEvent.change(await screen.findByLabelText('Region', { selector: 'select' }), { target: { value: 'us' } });
    await fireEvent.input(screen.getByPlaceholderText('e.g. us-east-1a'), { target: { value: 'us-east-1a' } });
    await fireEvent.click(screen.getByRole('button', { name: 'Issue token' }));
    expect(await screen.findByText(/Copy the token for us \/ us-east-1a now/)).toBeInTheDocument();
    expect(screen.getByText(/wa_c1\.secret/)).toBeInTheDocument();
    const post = fetchMock.mock.calls.find(([url, init]) => url.includes('agent-credentials') && init?.method === 'POST');
    expect(JSON.parse(String(post?.[1]?.body))).toEqual({ zone: 'us-east-1a', region: 'us' });
  });

  it('simulates rollup state transitions from child states', async () => {
    vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(JSON.stringify(
      url.includes('rollup-policies') ? { policies: [] } : { checks: [] },
    ), { headers: { 'content-type': 'application/json' } })));
    render(RegionTreeAdmin, { props: { onexpired: noop } });
    await fireEvent.click(await screen.findByRole('button', { name: /Zone state/ }));
    const result = () => screen.getByText('Resulting state').nextElementSibling;
    expect(result()).toHaveTextContent('up');
    for (const n of [1, 2, 4]) {
      await fireEvent.click(screen.getByRole('button', { name: new RegExp(`agent ${n}: up`) }));
      await fireEvent.click(screen.getByRole('button', { name: new RegExp(`agent ${n}: degraded`) }));
    }
    await fireEvent.click(screen.getByRole('button', { name: /agent 3: degraded/ }));
    expect(result()).toHaveTextContent('down');
  });

  it('decommissions a peer coordinator only after confirmation', async () => {
    const now = Date.now();
    const member = (node_id: string, state: string) => ({ node_id, build: null, url: `http://${node_id}`, state, last_seen: now, generation: 1 });
    const fetchMock = vi.fn(async (url: string, _init?: RequestInit) => new Response(JSON.stringify(
      url.includes('/admin/cluster/members/') ? member('b', 'removed')
        : url.includes('/cluster') ? { self: member('a', 'alive'), members: [member('b', 'dead'), member('c', 'removed')], mode: 'coordinator', now, links: [] }
        : { agents: [] },
    ), { headers: { 'content-type': 'application/json' } }));
    vi.stubGlobal('fetch', fetchMock);
    render(ClusterAdmin);
    await fireEvent.click(await screen.findByRole('button', { name: /c\s*Coordinator · removed/ }));
    expect(screen.queryByRole('button', { name: /Decommission/ })).not.toBeInTheDocument();
    await fireEvent.click(screen.getByRole('button', { name: /b\s*Coordinator · dead/ }));
    await fireEvent.click(screen.getByRole('button', { name: 'Decommission…' }));
    expect(fetchMock).not.toHaveBeenCalledWith(expect.stringContaining('/members/'), expect.anything());
    await fireEvent.click(screen.getByRole('button', { name: 'Decommission' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/admin/cluster/members/b', expect.objectContaining({ method: 'DELETE' }));
  });

  it('copies the full region key once, never the credential ID, and confirms removal', async () => {
    const region = { name: 'tw', credential_id: 'wac_1', created_at: 0, endpoints: [], attached: false, stale: true, age_ms: -1, check_count: 0 };
    const fetchMock = vi.fn(async (url: string, init?: RequestInit) => new Response(JSON.stringify(
      init?.method === 'POST' ? { region: 'eu', credential_id: 'wac_2', token: 'wac_2.secret' }
        : init?.method === 'DELETE' ? { removed: 'tw', checks_updated: 1 }
        : { regions: [region] },
    ), { headers: { 'content-type': 'application/json' } }));
    const writeText = vi.fn(async () => {});
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    vi.stubGlobal('fetch', fetchMock);
    render(RegionsAdmin);
    expect(await screen.findByText(/Awaiting first report/)).toBeInTheDocument();
    expect(screen.getByText('wac_1')).toHaveAttribute('title', expect.stringContaining('not a usable token'));
    expect(screen.queryByRole('button', { name: /Copy.*ID/i })).not.toBeInTheDocument();
    await fireEvent.input(screen.getByRole('textbox', { name: 'Region' }), { target: { value: 'eu' } });
    await fireEvent.click(screen.getByRole('button', { name: 'Register region' }));
    expect(await screen.findByText(/wac_2\.secret/)).toHaveTextContent('token = "wac_2.secret"');
    await fireEvent.click(screen.getByRole('button', { name: 'Copy secret token' }));
    expect(writeText).toHaveBeenCalledExactlyOnceWith('wac_2.secret');
    await fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(screen.queryByText(/wac_2\.secret/)).not.toBeInTheDocument();
    expect(screen.getByText('wac_1')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Copy.*ID/i })).not.toBeInTheDocument();
    await fireEvent.click(screen.getAllByRole('button', { name: 'Remove' })[0]);
    await fireEvent.click(screen.getByRole('button', { name: 'Confirm' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/admin/regions/tw', expect.objectContaining({ method: 'DELETE' }));
  });

  it('keeps agent credential IDs visible but copies only the issued token', async () => {
    const credential = { id: 'wac_3', region: 'eu', zone: 'eu-1a', created_at: 0, revoked: false };
    const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => new Response(JSON.stringify(
      init?.method === 'POST' ? { credential, token: 'wac_3.secret' } : { credentials: [credential] },
    ), { headers: { 'content-type': 'application/json' } }));
    const writeText = vi.fn(async () => {});
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    vi.stubGlobal('fetch', fetchMock);
    render(AgentCredentials, { props: { nodeMode: 'coordinator' } });
    expect(await screen.findByText('wac_3')).toHaveAttribute('title', expect.stringContaining('not a usable token'));
    expect(screen.queryByRole('button', { name: /Copy.*ID/i })).not.toBeInTheDocument();
    await fireEvent.input(screen.getByRole('combobox', { name: 'Region' }), { target: { value: 'eu' } });
    await fireEvent.input(screen.getByRole('textbox', { name: 'Zone' }), { target: { value: 'eu-1a' } });
    await fireEvent.click(screen.getByRole('button', { name: 'Issue agent token' }));
    expect(await screen.findByText(/wac_3\.secret/)).toBeInTheDocument();
    await fireEvent.click(screen.getByRole('button', { name: 'Copy token' }));
    expect(writeText).toHaveBeenCalledExactlyOnceWith('wac_3.secret');
    await fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(screen.queryByText(/wac_3\.secret/)).not.toBeInTheDocument();
    expect(screen.getByText('wac_3')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Copy.*ID/i })).not.toBeInTheDocument();
  });

  it('renders probe results as a ledger with inspectable errors', async () => {
    vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(JSON.stringify(
      url.includes('/admin/checks') ? { checks: [], has_more: false } : { results: [{ check_id: 'api', name: 'Production API', agent_id: 'us-agent', timestamp: Date.now(), status: 'down', latency_ms: 20, status_code: 503, region: 'us', zone: 'east', error: 'Upstream unavailable' }] },
    ), { headers: { 'content-type': 'application/json' } })));
    const onopen = vi.fn();
    render(ProbeResultsAdmin, { props: { onopen, onexpired: noop } });
    await fireEvent.click(await screen.findByRole('button', { name: 'Production API' }));
    expect(onopen).toHaveBeenCalledWith('api');
    expect(screen.getByText('Monitor / agent')).toBeInTheDocument();
    expect(screen.getByText('HTTP 503')).toBeInTheDocument();
    expect(screen.getAllByText('Upstream unavailable')).toHaveLength(2);
  });

  it('renders populated email channel fields with accessible labels', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({
      notifiers: [{ type: 'email', name: 'Operations', default: true, host: 'smtp.example.com', port: 587, tls: 'starttls', from: 'monitor@example.com', to: 'ops@example.com' }],
      unrouted_checks: [], undelivered: [],
    }), { headers: { 'content-type': 'application/json' } })));
    render(NotifiersAdmin, { props: { onexpired: noop } });
    expect(await screen.findByLabelText('SMTP host')).toHaveValue('smtp.example.com');
    expect(screen.getByLabelText('Port')).toHaveValue(587);
    expect(screen.getByLabelText('Encryption')).toHaveValue('starttls');
    expect(screen.getByLabelText('From address')).toHaveValue('monitor@example.com');
    expect(screen.getByLabelText('Default channel')).toBeChecked();
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
  });

  it('renders a populated maintenance window with labeled controls', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({
      windows: [{ title: 'Database upgrade', starts_at: 1790000040000, ends_at: 1790003640000, checks: ['api'], announce: true }],
      checks: [{ id: 'api', name: 'API' }],
    }), { headers: { 'content-type': 'application/json' } })));
    render(MaintenanceAdmin, { props: { onexpired: noop } });
    expect(await screen.findByLabelText('Maintenance title')).toHaveValue('Database upgrade');
    expect(screen.getByLabelText('Starts')).toHaveAttribute('type', 'datetime-local');
    expect(screen.getByLabelText('Ends')).toHaveAttribute('type', 'datetime-local');
    expect(screen.getByLabelText('Announce on status page')).toBeChecked();
    expect(screen.getByRole('option', { name: 'API' })).toBeInTheDocument();
  });

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
      await fireEvent.change(screen.getAllByRole('combobox')[0], { target: { value: 'ntp' } });
      expect(screen.getByPlaceholderText('time.example.com:123')).toBeInTheDocument();
      expect(screen.getByText(/NTP server host:port/)).toBeInTheDocument();
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
