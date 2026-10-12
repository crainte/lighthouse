<script lang="ts">
  import type { Agent } from '../lib/api';
  import { UNKNOWN_LOCATION, fetchAgents } from '../lib/api';
  import { AdminError, fetchAdminChecks, purgeAgent } from '../lib/admin';
  import type { RegionStat, Row, SortKey, ZoneStat } from './agents-admin/fleet';
  import { byLocation, zoneKey } from './agents-admin/fleet';
  import OrphanBanner from './agents-admin/OrphanBanner.svelte';
  import FleetSummary from './agents-admin/FleetSummary.svelte';
  import AgentFilters from './agents-admin/AgentFilters.svelte';
  import AgentTable from './agents-admin/AgentTable.svelte';

  // `onopen` is the check editor, keyed on the check's immutable id: the orphan
  // warning is only actionable if the named check is one click away.
  let { onopen, onexpired }: { onopen: (id: string) => void; onexpired?: () => void } = $props();

  /** Agent rows mounted before "show more". A fleet is hundreds of agents, not
   *  millions, so a cap holds where virtualisation would only add measured row
   *  heights and scroll maths — and rows here are not a fixed height anyway,
   *  because the purge confirmation expands one of them. */
  const PAGE = 100;
  /** Past this, grouping starts shut: expanding the one region you care about
   *  beats scrolling through every other region's rows to reach it. */
  const LARGE_FLEET = 100;

  let agents = $state<Agent[]>([]);
  // The arrays are a server-capped sample; the totals are the real finding.
  // A region that loses its agents orphans every check it holds, and naming
  // all of them would make this banner cost what the status page costs.
  let orphaned = $state<string[]>([]);
  let orphanedTotal = $state(0);
  // Separate from `orphaned` because the remedy is different: those agents are
  // not missing, they are too old to run the kind they were given.
  let unrunnable = $state<string[]>([]);
  let unrunnableTotal = $state(0);
  // `orphaned`/`unrunnable` are check names, not ids — the editor route needs
  // the id, so this resolves one from the other for the orphan links below.
  let idByName = $state<Map<string, string>>(new Map());
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

  let sortKey = $state<SortKey>('status');
  let sortDir = $state<1 | -1>(1);

  let grouped = $state(true);
  /** Keyed by the group row's own key, so one record covers regions and zones. */
  let collapsed = $state<Record<string, boolean>>({});

  let limit = $state(PAGE);

  /** The one agent armed for purging. Only one at a time, so the confirm strip
   *  below its row is unambiguous. */
  let pending = $state<string | null>(null);
  let purging = $state(false);
  let purgeError = $state<string | null>(null);
  let purgeNote = $state<string | null>(null);

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
      case 'rtt':
        d = (a.poll_rtt_ms ?? Infinity) - (b.poll_rtt_ms ?? Infinity);
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
    let shownCount = 0;
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
      if (shownCount >= limit) continue;
      out.push({ kind: 'agent', key: a.agent_id, agent: a });
      shownCount++;
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
      const [r, checks] = await Promise.all([fetchAgents(), fetchAdminChecks()]);
      agents = r.agents;
      orphaned = r.checks_with_no_live_agent;
      orphanedTotal = r.checks_with_no_live_agent_total;
      unrunnable = r.checks_no_agent_can_run;
      unrunnableTotal = r.checks_no_agent_can_run_total;
      idByName = new Map(checks.map((c) => [c.name, c.id]));
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
</script>

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

  {#if orphanedTotal > 0}
    <OrphanBanner
      title={`${orphanedTotal} ${orphanedTotal === 1 ? 'check has' : 'checks have'} no live agent`}
      names={orphaned}
      {idByName}
      {onopen}
      overflowNote={orphanedTotal > orphaned.length
        ? `Showing ${orphaned.length} of ${orphanedTotal}. A list this long is a fleet problem, not a check problem — find the agents that stopped reporting.`
        : null}
    >
      Enabled and configured, but every agent each one names has gone silent. Nothing is probing
      them and nothing else will report it — they will simply stop producing results. Open each
      one and unassign it, or bring its agent back.
    </OrphanBanner>
  {/if}

  {#if unrunnableTotal > 0}
    <OrphanBanner
      title={`${unrunnableTotal} ${unrunnableTotal === 1 ? 'check needs' : 'checks need'} a newer agent`}
      names={unrunnable}
      {idByName}
      {onopen}
      overflowNote={unrunnableTotal > unrunnable.length
        ? `Showing ${unrunnable.length} of ${unrunnableTotal}.`
        : null}
    >
      Every agent eligible for these is reporting normally but has said it cannot probe that check
      type — it was built before the type existed. The check is assigned and silent, which looks
      exactly like a check nobody has got to yet. Upgrade those agents, or assign these to agents
      that can run them.
    </OrphanBanner>
  {/if}

  <div class="admin-panel fleet-workspace">
    <p class="admin-muted lead">
      Agents appear automatically when they report results. Review connectivity and assignments,
      or retire agents that are no longer in service.
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
      <FleetSummary agentCount={agents.length} {fleet} onFocusZone={focusZone} />

      <AgentFilters
        bind:search
        bind:statusFilter
        bind:regionFilter
        bind:zoneFilter
        bind:showRetired
        {grouped}
        onSetGrouped={setGrouped}
        {regionOptions}
        {zoneOptions}
        retiredCount={fleet.retired}
        matchedCount={matched.length}
        agentCount={agents.length}
        {filtersActive}
        onClear={clearFilters}
        {purgeNote}
      />

      <AgentTable
        {rendered}
        {sortKey}
        {sortDir}
        onToggleSort={toggleSort}
        {ariaSort}
        bind:collapsed
        bind:pending
        {purging}
        {purgeError}
        onArm={arm}
        onConfirmPurge={confirmPurge}
        {capped}
        {shown}
        pageSize={PAGE}
        onShowMore={() => (limit += PAGE)}
        onClearFilters={clearFilters}
      />

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
</style>
