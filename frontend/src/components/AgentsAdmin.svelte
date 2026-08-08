<script lang="ts">
  import type { Agent } from '../lib/api';
  import { UNKNOWN_LOCATION, fetchAgents } from '../lib/api';
  import { formatRelativeTime } from '../lib/format';
  import { AdminError, purgeAgent } from '../lib/admin';

  // `onopen` is the check editor: the orphan warning is only actionable if the
  // named check is one click away.
  let { onopen, onexpired }: { onopen: (name: string) => void; onexpired?: () => void } = $props();

  /** Agent rows mounted before "show more". A fleet is hundreds of agents, not
   *  millions, so a cap holds where virtualisation would only add measured row
   *  heights and scroll maths — and rows here are not a fixed height anyway,
   *  because the purge confirmation expands one of them. */
  const PAGE = 100;
  /** Past this, grouping starts shut: expanding the one region you care about
   *  beats scrolling through every other region's rows to reach it. */
  const LARGE_FLEET = 100;

  let agents = $state<Agent[]>([]);
  let orphaned = $state<string[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let expired = $state(false);

  /** Retired agents are still real and still own their history, so they are
   *  filtered away rather than dropped — the list would otherwise grow a row
   *  per pod that ever existed. */
  let showRetired = $state(false);

  let search = $state('');
  /** `''` rather than `'all'`: a region really could be named "all". */
  let regionFilter = $state('');
  let zoneFilter = $state('');
  let statusFilter = $state<'all' | 'up' | 'silent'>('all');

  type SortKey =
    | 'agent'
    | 'region'
    | 'zone'
    | 'status'
    | 'seen'
    | 'reporting'
    | 'results'
    | 'pinned';
  let sortKey = $state<SortKey>('status');
  let sortDir = $state<1 | -1>(1);

  /** Location grouping is subordinate to the flat list: off until asked for,
   *  because at fleet scale you search and filter far more often than you
   *  browse a tree. */
  let grouped = $state(false);
  /** Keyed by the group row's own key, so one record covers regions and zones. */
  let collapsed = $state<Record<string, boolean>>({});

  let limit = $state(PAGE);

  /** The one agent armed for purging. Only one at a time, so the confirm strip
   *  below its row is unambiguous. */
  let pending = $state<string | null>(null);
  let purging = $state(false);
  let purgeError = $state<string | null>(null);
  let purgeNote = $state<string | null>(null);

  const zoneKey = (region: string, zone: string) => `${region}\u0000${zone}`;

  /** Alphabetical, except the unplaced bucket always sinks to the bottom: it is
   *  a configuration gap rather than a location, and it must not sit between
   *  two real regions where it reads like one. */
  function byLocation(a: string, b: string): number {
    if ((a === UNKNOWN_LOCATION) !== (b === UNKNOWN_LOCATION))
      return a === UNKNOWN_LOCATION ? 1 : -1;
    return a.localeCompare(b);
  }

  type RegionStat = { count: number; up: number; results: number; zones: number };
  type ZoneStat = { region: string; zone: string; count: number; up: number; results: number };

  // One pass over the fleet feeds the summary strip, the dead-zone alarm and
  // every group header. Counts are over every agent, including the retired ones
  // the toggle is hiding: a zone whose only agents retired still has no live
  // agent, and hiding its rows must not hide that fact.
  const fleet = $derived.by(() => {
    const regions = new Map<string, RegionStat>();
    const zones = new Map<string, ZoneStat>();
    let up = 0;
    let retired = 0;
    let unplaced = 0;
    for (const a of agents) {
      const live = a.status === 'up' ? 1 : 0;
      up += live;
      if (a.retired) retired++;
      if (a.region === UNKNOWN_LOCATION || a.zone === UNKNOWN_LOCATION) unplaced++;
      let r = regions.get(a.region);
      if (!r) regions.set(a.region, (r = { count: 0, up: 0, results: 0, zones: 0 }));
      r.count++;
      r.up += live;
      r.results += a.results_24h;
      const zk = zoneKey(a.region, a.zone);
      let z = zones.get(zk);
      if (!z) zones.set(zk, (z = { region: a.region, zone: a.zone, count: 0, up: 0, results: 0 }));
      z.count++;
      z.up += live;
      z.results += a.results_24h;
    }
    for (const z of zones.values()) regions.get(z.region)!.zones++;
    return {
      regions,
      zones,
      up,
      retired,
      unplaced,
      // Retired agents are silent too — they are counted again on their own
      // tile rather than carved out, so "silent" always means "not reporting".
      silent: agents.length - up,
      dead: [...zones.values()]
        .filter((z) => z.up === 0)
        .sort((x, y) => byLocation(x.region, y.region) || byLocation(x.zone, y.zone)),
    };
  });

  const regionOptions = $derived([...fleet.regions.keys()].sort(byLocation));
  /** Narrowed to the chosen region: offering a zone that cannot match anything
   *  is just a way to get an empty table. */
  const zoneOptions = $derived(
    [
      ...new Set(
        [...fleet.zones.values()]
          .filter((z) => !regionFilter || z.region === regionFilter)
          .map((z) => z.zone),
      ),
    ].sort(byLocation),
  );

  const query = $derived(search.trim().toLowerCase());
  const filtersActive = $derived(
    query !== '' || statusFilter !== 'all' || regionFilter !== '' || zoneFilter !== '' || showRetired,
  );

  /** Silent first, so what is broken sorts to the top; retired last, because it
   *  is history rather than a problem anyone can act on. */
  const rank = (a: Agent) => (a.retired ? 2 : a.status === 'up' ? 1 : 0);

  function compare(a: Agent, b: Agent): number {
    let d = 0;
    switch (sortKey) {
      case 'agent':
        d = a.agent_id.localeCompare(b.agent_id);
        break;
      case 'region':
        d = byLocation(a.region, b.region);
        break;
      case 'zone':
        d = byLocation(a.zone, b.zone) || byLocation(a.region, b.region);
        break;
      case 'status':
        d = rank(a) - rank(b);
        break;
      case 'seen':
        d = a.last_seen - b.last_seen;
        break;
      case 'reporting':
        d = a.checks_reporting - b.checks_reporting;
        break;
      case 'results':
        d = a.results_24h - b.results_24h;
        break;
      case 'pinned':
        d = a.assigned_checks - b.assigned_checks;
        break;
    }
    // agent_id is unique and always ascending, so every sort is total and rows
    // never shuffle between renders that compare equal.
    return d * sortDir || a.agent_id.localeCompare(b.agent_id);
  }

  const matched = $derived.by(() => {
    const rows = agents.filter(
      (a) =>
        (showRetired || !a.retired) &&
        (statusFilter === 'all' || (statusFilter === 'up') === (a.status === 'up')) &&
        (!regionFilter || a.region === regionFilter) &&
        (!zoneFilter || a.zone === zoneFilter) &&
        (query === '' ||
          a.agent_id.toLowerCase().includes(query) ||
          a.zone.toLowerCase().includes(query) ||
          a.region.toLowerCase().includes(query)),
    );
    rows.sort(
      grouped
        ? (a, b) => byLocation(a.region, b.region) || byLocation(a.zone, b.zone) || compare(a, b)
        : compare,
    );
    return rows;
  });

  type Row =
    | { kind: 'region'; key: string; region: string; stat: RegionStat }
    | { kind: 'zone'; key: string; zone: ZoneStat }
    | { kind: 'agent'; key: string; agent: Agent };

  /** Rows actually mounted. Group headers are emitted as the region or zone
   *  changes along the already-sorted list, so grouping costs one pass and no
   *  second data shape — and a collapsed group skips its members without
   *  spending any of the cap. */
  const rendered = $derived.by<Row[]>(() => {
    if (!grouped)
      return matched
        .slice(0, limit)
        .map((a) => ({ kind: 'agent', key: a.agent_id, agent: a }) as Row);
    const out: Row[] = [];
    let shown = 0;
    let region: string | null = null;
    let zone: string | null = null;
    let skipRegion = false;
    let skipZone = false;
    for (const a of matched) {
      if (a.region !== region) {
        region = a.region;
        zone = null;
        const key = `r:${region}`;
        out.push({ kind: 'region', key, region, stat: fleet.regions.get(region)! });
        skipRegion = collapsed[key] === true;
      }
      if (skipRegion) continue;
      const zk = zoneKey(a.region, a.zone);
      if (zk !== zone) {
        zone = zk;
        const key = `z:${zk}`;
        out.push({ kind: 'zone', key, zone: fleet.zones.get(zk)! });
        skipZone = collapsed[key] === true;
      }
      if (skipZone) continue;
      // The cap bounds agent rows only. Group headers keep coming, so the
      // fleet's shape stays complete and the region you want to expand cannot
      // hide behind the cap; there are always far fewer zones than agents.
      if (shown >= limit) continue;
      out.push({ kind: 'agent', key: a.agent_id, agent: a });
      shown++;
    }
    return out;
  });

  const shown = $derived(rendered.reduce((n, r) => n + (r.kind === 'agent' ? 1 : 0), 0));
  /** What the cap is holding back — collapsed groups are hidden on purpose and
   *  do not count as "more to show". */
  const capped = $derived.by(() => {
    if (!grouped) return matched.length - shown;
    let visible = 0;
    for (const a of matched)
      if (!collapsed[`r:${a.region}`] && !collapsed[`z:${zoneKey(a.region, a.zone)}`]) visible++;
    return visible - shown;
  });

  // A new filter or a new sort is a new question; answering it 300 rows deep
  // because the last one was expanded is not what anybody meant.
  const filterSignature = $derived(
    `${query}|${statusFilter}|${regionFilter}|${zoneFilter}|${showRetired}|${sortKey}|${sortDir}|${grouped}`,
  );
  $effect(() => {
    filterSignature;
    limit = PAGE;
  });

  function toggleSort(key: SortKey) {
    if (sortKey === key) sortDir = sortDir === 1 ? -1 : 1;
    else {
      sortKey = key;
      // Counts and timestamps are asked about from the top — most results, most
      // recently seen — while names and states read forwards.
      sortDir = key === 'agent' || key === 'region' || key === 'zone' || key === 'status' ? 1 : -1;
    }
  }

  const ariaSort = (key: SortKey) =>
    sortKey !== key ? 'none' : sortDir === 1 ? 'ascending' : 'descending';

  function pickRegion(next: string) {
    regionFilter = next;
    // The chosen zone may not exist in the new region, and a filter pair that
    // matches nothing looks like an empty fleet.
    zoneFilter = '';
  }

  function focusZone(z: ZoneStat) {
    regionFilter = z.region;
    zoneFilter = z.zone;
  }

  function clearFilters() {
    search = '';
    statusFilter = 'all';
    regionFilter = '';
    zoneFilter = '';
    showRetired = false;
  }

  function setGrouped(on: boolean) {
    grouped = on;
    collapsed =
      on && agents.length > LARGE_FLEET
        ? Object.fromEntries([...fleet.regions.keys()].map((r) => [`r:${r}`, true]))
        : {};
  }

  function fail(e: unknown): string {
    const message = e instanceof Error ? e.message : String(e);
    // The admin helpers throw AdminError; fetchAgents() throws a plain Error
    // that carries the status in its text. Both mean the same thing at 401, and
    // this endpoint is admin-gated now, so an expiring session lands here.
    if ((e instanceof AdminError && e.unauthorized) || / 401\b/.test(message)) {
      expired = true;
      onexpired?.();
    }
    return message;
  }

  async function refresh() {
    loading = true;
    loadError = null;
    try {
      const r = await fetchAgents();
      agents = r.agents;
      orphaned = r.checks_with_no_live_agent;
    } catch (e) {
      loadError = fail(e);
    } finally {
      loading = false;
    }
  }

  refresh();

  function arm(id: string) {
    pending = id;
    purgeError = null;
    purgeNote = null;
  }

  async function confirmPurge(id: string) {
    if (purging) return;
    purging = true;
    purgeError = null;
    try {
      const removed = await purgeAgent(id);
      pending = null;
      purgeNote = `Purged ${removed} recorded ${removed === 1 ? 'result' : 'results'} from ${id}.`;
      await refresh();
    } catch (e) {
      // A 409 names the checks still pinned to this agent, so it is rendered
      // word for word rather than summarised into "could not purge".
      purgeError = fail(e);
    } finally {
      purging = false;
    }
  }

  const placeName = (v: string) => (v === UNKNOWN_LOCATION ? 'unplaced' : v);
</script>

{#snippet column(key: SortKey, label: string)}
  <th scope="col" aria-sort={ariaSort(key)}>
    <button class="sort" class:active={sortKey === key} onclick={() => toggleSort(key)}>
      <span class="label">{label}</span>
      <span class="dir" aria-hidden="true">{sortKey === key ? (sortDir === 1 ? '↑' : '↓') : ''}</span>
    </button>
  </th>
{/snippet}

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{purgeError ?? loadError ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to manage agents.</p>
    <div class="range-switch retry">
      <button class="range-btn" onclick={onexpired}>Sign in again</button>
    </div>
  </div>
{:else}
  <div class="area-bar">
    <div class="title-line">
      <span class="panel-title no-gap">Agents</span>
      {#if !loading}<span class="kind-pill">{agents.length}</span>{/if}
    </div>
  </div>

  {#if orphaned.length > 0}
    <div class="admin-panel orphans">
      <div class="orphan-title">
        {orphaned.length}
        {orphaned.length === 1 ? 'check has' : 'checks have'} no live agent
      </div>
      <p class="orphan-note">
        Enabled and configured, but every agent each one names has gone silent. Nothing is probing
        them and nothing else will report it — they will simply stop producing results. Open each
        one and unassign it, or bring its agent back.
      </p>
      <div class="orphan-list">
        <!-- Check names are map keys server-side, so they cannot repeat. -->
        {#each orphaned as n (n)}
          <button class="orphan-link" onclick={() => onopen(n)}>{n}</button>
        {/each}
      </div>
    </div>
  {/if}

  <div class="admin-panel">
    <p class="admin-muted lead">
      Agents self-register by reporting results — there is no registration step, so an agent exists
      because it sent something, and it goes away only by falling silent or by being purged here.
    </p>

    {#if loading}
      <p class="admin-muted">Loading…</p>
    {:else if loadError}
      <p class="admin-error">{loadError}</p>
      <div class="range-switch retry">
        <button class="range-btn" onclick={refresh}>Retry</button>
      </div>
    {:else if agents.length === 0}
      <p class="admin-muted">
        No agent has ever reported. Start one pointed at this coordinator and it will appear here.
      </p>
    {:else}
      <div class="summary">
        <div class="tile">
          <span class="tile-value">{agents.length}</span>
          <span class="tile-label">agents</span>
        </div>
        <div class="tile">
          <span class="tile-value up" class:zero={fleet.up === 0}>{fleet.up}</span>
          <span class="tile-label">reporting</span>
        </div>
        <div class="tile">
          <span class="tile-value down" class:zero={fleet.silent === 0}>{fleet.silent}</span>
          <span class="tile-label">silent</span>
        </div>
        <div class="tile" title="Silent for over a week — counted in silent too.">
          <span class="tile-value">{fleet.retired}</span>
          <span class="tile-label">retired</span>
        </div>
        <div class="tile">
          <span class="tile-value">{fleet.zones.size}</span>
          <span class="tile-label">zones</span>
        </div>
        <div class="tile">
          <span class="tile-value">{fleet.regions.size}</span>
          <span class="tile-label">regions</span>
        </div>
      </div>

      {#if fleet.dead.length > 0}
        <p class="zone-alarm">
          {fleet.dead.length}
          {fleet.dead.length === 1 ? 'zone has' : 'zones have'} no live agent. Every check placed
          there is unprobed — placement selects nobody rather than quietly running it somewhere
          else.
        </p>
        <div class="dead-list">
          <!-- Zone names repeat across regions, so the key carries both. -->
          {#each fleet.dead.slice(0, 12) as z (`${z.region}\u0000${z.zone}`)}
            <button class="dead-chip" onclick={() => focusZone(z)}>
              {placeName(z.region)} / {placeName(z.zone)}
            </button>
          {/each}
          {#if fleet.dead.length > 12}
            <span class="admin-muted">+{fleet.dead.length - 12} more</span>
          {/if}
        </div>
      {/if}

      <div class="controls">
        <input
          class="search"
          type="search"
          bind:value={search}
          placeholder="Search agent, zone or region"
          aria-label="Search agents by id, zone or region"
        />
        <div class="range-switch" role="group" aria-label="Status filter">
          <button
            class="range-btn"
            class:active={statusFilter === 'all'}
            onclick={() => (statusFilter = 'all')}>All</button
          >
          <button
            class="range-btn"
            class:active={statusFilter === 'up'}
            onclick={() => (statusFilter = 'up')}>Up</button
          >
          <button
            class="range-btn"
            class:active={statusFilter === 'silent'}
            onclick={() => (statusFilter = 'silent')}>Silent</button
          >
        </div>
        <label class="pick">
          <span class="pick-label">Region</span>
          <select value={regionFilter} onchange={(e) => pickRegion(e.currentTarget.value)}>
            <option value="">All regions</option>
            {#each regionOptions as r (r)}
              <option value={r}>{placeName(r)}</option>
            {/each}
          </select>
        </label>
        <label class="pick">
          <span class="pick-label">Zone</span>
          <select bind:value={zoneFilter}>
            <option value="">All zones</option>
            <!-- Distinct zone names for the chosen region, so no key repeats. -->
            {#each zoneOptions as z (z)}
              <option value={z}>{placeName(z)}</option>
            {/each}
          </select>
        </label>
        {#if fleet.retired > 0}
          <label class="toggle">
            <input type="checkbox" bind:checked={showRetired} />
            <span>Retired <em>({fleet.retired})</em></span>
          </label>
        {/if}
        <label class="toggle">
          <input
            type="checkbox"
            checked={grouped}
            onchange={(e) => setGrouped(e.currentTarget.checked)}
          />
          <span>Group by location</span>
        </label>
      </div>

      <div class="count-line">
        <span class="count">{matched.length} of {agents.length} agents</span>
        {#if filtersActive}
          <button class="btn" onclick={clearFilters}>Clear filters</button>
        {/if}
        {#if purgeNote}<span class="admin-muted note">{purgeNote}</span>{/if}
      </div>

      <!-- A real table with `table-layout: fixed`: the header cells and the body
           cells are the same columns by definition, so no template can be
           changed in one place and not the other, and a group header is one
           full-width cell rather than a second, drifting track list. -->
      <div class="admin-rows table-wrap">
        <table class="fleet">
          <colgroup>
            <col />
            <col class="c-region" />
            <col class="c-zone" />
            <col class="c-status" />
            <col class="c-seen" />
            <col class="c-num" />
            <col class="c-num" />
            <col class="c-num" />
            <col class="c-act" />
          </colgroup>
          <thead>
            <tr>
              {@render column('agent', 'Agent')}
              {@render column('region', 'Region')}
              {@render column('zone', 'Zone')}
              {@render column('status', 'Status')}
              {@render column('seen', 'Last seen')}
              {@render column('reporting', 'Reporting')}
              {@render column('results', 'Results 24h')}
              {@render column('pinned', 'Pinned')}
              <th scope="col"><span class="vh">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {#if rendered.length === 0}
              <tr>
                <td class="empty" colspan="9">
                  No agent matches these filters.
                  <button class="btn" onclick={clearFilters}>Clear filters</button>
                </td>
              </tr>
            {/if}
            <!-- Keyed on agent_id, which is unique fleet-wide; group rows carry
                 an `r:`/`z:` prefix so they cannot collide with an id. -->
            {#each rendered as row (row.key)}
              {#if row.kind === 'region'}
                <tr class="grp region" class:unplaced={row.region === UNKNOWN_LOCATION}>
                  <th colspan="9" scope="colgroup">
                    <button
                      class="grp-toggle"
                      aria-expanded={!collapsed[row.key]}
                      onclick={() => (collapsed[row.key] = !collapsed[row.key])}
                    >
                      <span class="chev">{collapsed[row.key] ? '▸' : '▾'}</span>
                      <span class="grp-name"
                        >{row.region === UNKNOWN_LOCATION ? 'No location declared' : row.region}</span
                      >
                      <span class="grp-meta">
                        {row.stat.zones}
                        {row.stat.zones === 1 ? 'zone' : 'zones'} · {row.stat.up} up · {row.stat
                          .count - row.stat.up} silent · {row.stat.results} results/24h
                      </span>
                    </button>
                  </th>
                </tr>
              {:else if row.kind === 'zone'}
                <tr class="grp zone" class:dead={row.zone.up === 0}>
                  <th colspan="9" scope="colgroup">
                    <button
                      class="grp-toggle"
                      aria-expanded={!collapsed[row.key]}
                      onclick={() => (collapsed[row.key] = !collapsed[row.key])}
                    >
                      <span class="chev">{collapsed[row.key] ? '▸' : '▾'}</span>
                      <span class="grp-name">{placeName(row.zone.zone)}</span>
                      {#if row.zone.up === 0}<span class="dead-pill">no live agent</span>{/if}
                      <span class="grp-meta">
                        {row.zone.up} up · {row.zone.count - row.zone.up} silent · {row.zone.results}
                        results/24h
                      </span>
                    </button>
                  </th>
                </tr>
              {:else}
                <tr class="agent-row" class:retired={row.agent.retired}>
                  <td title={row.agent.agent_id}>
                    <span class="agent-pill">{row.agent.agent_id}</span>
                  </td>
                  <td class="dim" class:amber={row.agent.region === UNKNOWN_LOCATION}>
                    {placeName(row.agent.region)}
                  </td>
                  <td class="dim" class:amber={row.agent.zone === UNKNOWN_LOCATION}>
                    {placeName(row.agent.zone)}
                  </td>
                  <td class="state" class:on={row.agent.status === 'up'}>
                    {row.agent.retired ? 'Retired' : row.agent.status === 'up' ? 'Reporting' : 'Silent'}
                  </td>
                  <td class="dim" title={new Date(row.agent.last_seen).toLocaleString()}>
                    {formatRelativeTime(row.agent.last_seen)}
                  </td>
                  <td class="mono">{row.agent.checks_reporting}</td>
                  <td class="mono">{row.agent.results_24h}</td>
                  <td class="mono">{row.agent.assigned_checks}</td>
                  <td class="actions">
                    <button
                      class="btn danger"
                      onclick={() => arm(row.agent.agent_id)}
                      disabled={purging}
                    >
                      Purge
                    </button>
                  </td>
                </tr>
                {#if pending === row.agent.agent_id}
                  <!-- One full-width cell on purpose: a confirmation wide enough
                       to say what is destroyed would not fit any column. -->
                  <tr class="confirm-row">
                    <td colspan="9">
                      <p class="confirm-copy">
                        Purge <span class="mono">{row.agent.agent_id}</span>? Every result it ever
                        recorded is deleted with it — {row.agent.results_24h} in the last 24 hours,
                        across {row.agent.checks_reporting}
                        {row.agent.checks_reporting === 1 ? 'check' : 'checks'}. Those checks lose
                        that history from their uptime bars and latency graphs.
                        {#if row.agent.assigned_checks > 0}
                          {row.agent.assigned_checks}
                          {row.agent.assigned_checks === 1 ? 'check names' : 'checks name'} this
                          agent explicitly and would be left with nothing to run
                          {row.agent.assigned_checks === 1 ? 'it' : 'them'}.
                        {/if}
                        The agent disappears from this list because the list is those rows. This
                        cannot be undone.
                      </p>
                      {#if purgeError}<p class="admin-error">{purgeError}</p>{/if}
                      <div class="confirm-actions">
                        <button
                          class="btn danger"
                          onclick={() => confirmPurge(row.agent.agent_id)}
                          disabled={purging}
                        >
                          {purging ? 'Purging…' : 'Yes, purge it and its results'}
                        </button>
                        <button class="btn" onclick={() => (pending = null)} disabled={purging}>
                          Cancel
                        </button>
                      </div>
                    </td>
                  </tr>
                {/if}
              {/if}
            {/each}
          </tbody>
        </table>
      </div>

      {#if capped > 0}
        <div class="more">
          <button class="btn" onclick={() => (limit += PAGE)}>
            Show {Math.min(PAGE, capped)} more
          </button>
          <span class="admin-muted">
            {shown} of {shown + capped} shown — the rest are held back so the table stays quick.
          </span>
        </div>
      {/if}

      {#if fleet.unplaced > 0}
        <p class="admin-muted foot">
          {fleet.unplaced}
          {fleet.unplaced === 1 ? 'agent has' : 'agents have'} no location and are filed as
          <em>unplaced</em> rather than guessed at. Set <code>zone</code> and <code>region</code>
          under <code>[agent]</code> in each agent's own config file; it reports its location on the
          next poll.
        </p>
      {/if}

      {#if fleet.retired > 0 && !showRetired}
        <p class="admin-muted foot">
          {fleet.retired} retired {fleet.retired === 1 ? 'agent is' : 'agents are'} hidden. Retiring
          is just prolonged silence — they keep every result they reported, and one reappears the
          moment it reports again.
        </p>
      {:else if fleet.retired > 0}
        <p class="admin-muted foot">
          Retired agents have been silent for over a week. They keep every result they reported, and
          one reappears the moment it reports again.
        </p>
      {/if}
    {/if}
  </div>
{/if}

<style>
  .area-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    margin-bottom: 0.6rem;
  }
  .no-gap {
    margin-bottom: 0;
  }
  .lead {
    margin-bottom: 0.8rem;
    line-height: 1.5;
  }
  .retry {
    margin-top: 0.7rem;
  }
  .foot {
    margin-top: 0.7rem;
    line-height: 1.5;
    max-width: 46rem;
  }
  .vh {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }

  /* What an operator reads before anything else: one line of counts, no cards,
     no hero number — the point is comparing six figures at a glance. */
  .summary {
    display: flex;
    flex-wrap: wrap;
    align-items: stretch;
    gap: 1.4rem;
    padding: 0 0 0.7rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 0.8rem;
  }
  .tile {
    display: flex;
    align-items: baseline;
    gap: 0.35rem;
  }
  .tile-value {
    font-size: 1.05rem;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
  }
  .tile-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }
  .tile-value.up {
    color: var(--up);
  }
  .tile-value.down {
    color: var(--down);
  }
  /* Nothing silent is the good case; colouring a zero red would cry wolf. */
  .tile-value.zero {
    color: var(--text-dim);
  }

  .zone-alarm {
    color: var(--down);
    font-size: 0.8rem;
    line-height: 1.5;
    margin: 0 0 0.5rem;
    max-width: 46rem;
  }
  .dead-list {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem;
    margin-bottom: 0.8rem;
  }
  /* The tree used to be how you got to a dead zone; now the alarm itself is the
     way in — clicking one filters the table down to it. */
  .dead-chip {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.72rem;
    padding: 0.1rem 0.5rem;
    border: 1px solid var(--down);
    border-radius: 999px;
    background: none;
    color: var(--down);
    cursor: pointer;
  }
  .dead-chip:hover {
    background: rgba(248, 81, 73, 0.16);
  }

  .controls {
    display: flex;
    align-items: center;
    gap: 0.7rem 1rem;
    flex-wrap: wrap;
    margin-bottom: 0.5rem;
  }
  .search {
    width: 18rem;
    max-width: 100%;
  }
  .pick {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }
  .pick-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }
  .pick select {
    width: auto;
    min-width: 9rem;
  }

  .count-line {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    flex-wrap: wrap;
    margin-bottom: 0.5rem;
  }
  .count {
    font-size: 0.78rem;
    color: var(--text-dim);
    font-variant-numeric: tabular-nums;
  }
  .note {
    font-size: 0.78rem;
  }

  .table-wrap {
    overflow-x: auto;
  }
  /* `fixed` is the whole alignment guarantee: column widths come from the
     colgroup, not from cell content, so a long agent id cannot move a heading
     off the values under it at any viewport width. */
  .fleet {
    width: 100%;
    /* Every other column is a fixed width, so this floor is what the agent id —
       the one column that has to stay readable — is left with before the wrap
       starts scrolling. */
    min-width: 66rem;
    table-layout: fixed;
    border-collapse: collapse;
    font-size: 0.8rem;
  }
  .c-region {
    width: 8rem;
  }
  .c-zone {
    width: 9rem;
  }
  .c-status {
    width: 6rem;
  }
  .c-seen {
    width: 7.5rem;
  }
  .c-num {
    width: 6rem;
  }
  .c-act {
    width: 5.5rem;
  }

  .fleet th,
  .fleet td {
    text-align: left;
    font-weight: 400;
    padding: 0.34rem 0.5rem 0.34rem 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .fleet thead th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--bg-elev);
    border-bottom: 1px solid var(--border);
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }
  .sort {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    background: none;
    border: 0;
    padding: 0;
    font: inherit;
    letter-spacing: inherit;
    text-transform: inherit;
    color: inherit;
    cursor: pointer;
    max-width: 100%;
  }
  .sort:hover {
    color: var(--text);
  }
  .sort.active {
    color: var(--accent);
  }
  .sort .label {
    overflow: hidden;
    text-overflow: ellipsis;
  }
  /* Reserved whether or not this column is the sorted one, so switching sort
     never nudges a heading sideways. */
  .dir {
    display: inline-block;
    width: 0.6rem;
    flex: none;
  }

  .fleet tbody tr.agent-row td {
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  /* Shown but stepped back: a retired agent is history, not an operator. */
  .agent-row.retired td {
    opacity: 0.5;
  }
  .agent-pill {
    display: inline-block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    vertical-align: bottom;
  }
  .state {
    color: var(--text-dim);
  }
  .state.on {
    color: var(--up);
  }
  .actions {
    text-align: right;
    padding-right: 0;
  }

  /* Group headers are a single spanning cell, so they share the table's columns
     without declaring any of their own. */
  .grp th {
    padding: 0;
  }
  .grp-toggle {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    width: 100%;
    background: none;
    border: 0;
    color: inherit;
    font: inherit;
    text-align: left;
    cursor: pointer;
  }
  .grp-toggle:hover {
    color: var(--text);
  }
  .chev {
    width: 0.7rem;
    flex: none;
    color: var(--text-dim);
  }
  .grp-name {
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .grp-meta {
    margin-left: auto;
    padding-left: 1rem;
    color: var(--text-dim);
    font-size: 0.72rem;
    font-variant-numeric: tabular-nums;
    text-transform: none;
    letter-spacing: 0;
    white-space: nowrap;
  }
  .grp.region th {
    padding: 0.5rem 0 0.25rem;
    border-bottom: 1px solid var(--border);
    font-size: 0.85rem;
    font-weight: 600;
    letter-spacing: 0.02em;
  }
  .grp.zone th {
    padding: 0.3rem 0 0.2rem 0.55rem;
    font-size: 0.76rem;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .grp.zone th .grp-toggle {
    border-left: 2px solid var(--border);
    padding-left: 0.4rem;
    margin-left: -0.4rem;
  }
  /* An unplaced agent is a config gap, so the bucket reads as a warning rather
     than as one more region. */
  .grp.region.unplaced th {
    color: var(--amber);
    border-bottom-color: var(--amber);
  }
  /* The one thing this panel exists to answer. A zone with nothing live means
     its checks produce no results and no error — nothing else would say so. */
  .grp.zone.dead th {
    color: var(--down);
  }
  .grp.zone.dead th .grp-toggle {
    border-left-color: var(--down);
  }
  .dead-pill {
    text-transform: none;
    letter-spacing: 0;
    font-size: 0.68rem;
    padding: 0.05rem 0.4rem;
    border: 1px solid var(--down);
    border-radius: 999px;
  }

  .empty {
    padding: 0.9rem 0;
    color: var(--text-dim);
  }
  .empty .btn {
    margin-left: 0.5rem;
  }

  .confirm-row td {
    padding: 0.5rem 0 0.7rem;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
    white-space: normal;
  }
  .confirm-copy {
    color: var(--down);
    font-size: 0.78rem;
    line-height: 1.5;
    margin: 0 0 0.5rem;
    max-width: 46rem;
  }
  .confirm-actions {
    display: flex;
    gap: 0.4rem;
  }

  .more {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    flex-wrap: wrap;
    margin-top: 0.7rem;
  }

  /* The whole point of the panel: a check nothing is running produces no error,
     no alert and no missing-data marker, so the warning has to carry the weight
     the absent signal does not. */
  .orphans {
    border-color: var(--down);
    background: rgba(248, 81, 73, 0.07);
  }
  .orphan-title {
    color: var(--down);
    font-weight: 600;
    font-size: 0.95rem;
    margin-bottom: 0.35rem;
  }
  .orphan-note {
    color: var(--text);
    font-size: 0.82rem;
    margin: 0 0 0.6rem;
    line-height: 1.5;
    max-width: 46rem;
  }
  .orphan-list {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }
  .orphan-link {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.78rem;
    padding: 0.15rem 0.5rem;
    border: 1px solid var(--down);
    border-radius: 999px;
    background: none;
    color: var(--down);
    cursor: pointer;
  }
  .orphan-link:hover {
    background: rgba(248, 81, 73, 0.16);
  }

  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .dim {
    color: var(--text-dim);
  }
  .amber {
    color: var(--amber);
  }
</style>
