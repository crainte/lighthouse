<script lang="ts">
  import type { Agent } from '../lib/api';
  import { UNKNOWN_LOCATION, fetchAgents } from '../lib/api';
  import { formatRelativeTime } from '../lib/format';
  import { AdminError, purgeAgent } from '../lib/admin';

  // `onopen` is the check editor: the orphan warning is only actionable if the
  // named check is one click away.
  let {
    onopen,
    onexpired,
  }: { onopen: (name: string) => void; onexpired: () => void } = $props();

  let agents = $state<Agent[]>([]);
  let orphaned = $state<string[]>([]);
  let loading = $state(true);
  let loadError = $state<string | null>(null);
  let expired = $state(false);

  /** Retired agents are still real and still own their history, so they are
   *  folded away rather than dropped — the list would otherwise grow a row per
   *  pod that ever existed. */
  let showRetired = $state(false);

  /** The one agent armed for purging. Only one at a time, so the confirm strip
   *  below its row is unambiguous. */
  let pending = $state<string | null>(null);
  let purging = $state(false);
  let purgeError = $state<string | null>(null);
  let purgeNote = $state<string | null>(null);

  const retiredCount = $derived(agents.filter((a) => a.retired).length);

  type ZoneGroup = {
    key: string;
    zone: string;
    live: number;
    silent: number;
    results: number;
    rows: Agent[];
    hidden: number;
  };
  type RegionGroup = {
    region: string;
    unplaced: boolean;
    live: number;
    silent: number;
    results: number;
    dead: number;
    zones: ZoneGroup[];
  };

  /** Alphabetical, except the unplaced bucket always sinks to the bottom: it is
   *  a configuration gap rather than a location, and it must not sit between
   *  two real regions where it reads like one. */
  function byLocation(a: string, b: string): number {
    if ((a === UNKNOWN_LOCATION) !== (b === UNKNOWN_LOCATION))
      return a === UNKNOWN_LOCATION ? 1 : -1;
    return a.localeCompare(b);
  }

  // Counts are over every agent in the group, including retired ones the toggle
  // is hiding: a zone whose only agents retired still has no live agent, and
  // folding its rows away must not fold away that fact.
  const fleet = $derived.by<RegionGroup[]>(() => {
    const regions = new Map<string, Map<string, Agent[]>>();
    for (const a of agents) {
      let zones = regions.get(a.region);
      if (!zones) regions.set(a.region, (zones = new Map()));
      let members = zones.get(a.zone);
      if (!members) zones.set(a.zone, (members = []));
      members.push(a);
    }
    return [...regions]
      .sort((x, y) => byLocation(x[0], y[0]))
      .map(([region, zones]) => {
        const zs = [...zones]
          .sort((x, y) => byLocation(x[0], y[0]))
          .map(([zone, members]) => {
            const rows = showRetired ? members : members.filter((m) => !m.retired);
            return {
              // Zone names repeat across regions — every region has an "a" —
              // so the key carries the region too. Two identical keys abort the
              // render of the entire page.
              key: `${region}\u0000${zone}`,
              zone,
              rows,
              hidden: members.length - rows.length,
              live: members.filter((m) => m.status === 'up').length,
              silent: members.filter((m) => m.status !== 'up').length,
              results: members.reduce((n, m) => n + m.results_24h, 0),
            };
          });
        return {
          region,
          unplaced: region === UNKNOWN_LOCATION,
          zones: zs,
          live: zs.reduce((n, z) => n + z.live, 0),
          silent: zs.reduce((n, z) => n + z.silent, 0),
          results: zs.reduce((n, z) => n + z.results, 0),
          dead: zs.filter((z) => z.live === 0).length,
        };
      });
  });

  const deadZones = $derived(fleet.reduce((n, r) => n + r.dead, 0));

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) expired = true;
    return e instanceof Error ? e.message : String(e);
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
</script>

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{purgeError ?? loadError ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to purge agents.</p>
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
      <div class="controls">
        {#if retiredCount > 0}
          <label class="toggle">
            <input type="checkbox" bind:checked={showRetired} />
            <span>Show retired <em>({retiredCount})</em></span>
          </label>
        {/if}
        {#if purgeNote}<span class="admin-muted">{purgeNote}</span>{/if}
      </div>

      {#if deadZones > 0}
        <p class="zone-alarm">
          {deadZones}
          {deadZones === 1 ? 'zone has' : 'zones have'} no live agent. Every check placed there is
          unprobed — placement selects nobody rather than quietly running it somewhere else.
        </p>
      {/if}

      <div class="admin-rows">
        <div class="row head">
          <span>Agent</span>
          <span>Status</span>
          <span>Last seen</span>
          <span>Reporting</span>
          <span>Results 24h</span>
          <span>Pinned to</span>
          <span></span>
        </div>
        <!-- Region and zone are Map keys above, so neither can repeat; the zone
             key carries its region because zone names repeat across regions. -->
        {#each fleet as r (r.region)}
          <div class="group region" class:unplaced={r.unplaced}>
            <span class="group-name">{r.unplaced ? 'No location declared' : r.region}</span>
            <span class="group-meta">
              {r.zones.length}
              {r.zones.length === 1 ? 'zone' : 'zones'} · {r.live} up · {r.silent} silent ·
              {r.results} results/24h
            </span>
          </div>
          {#if r.unplaced}
            <p class="group-note">
              These agents never said where they are, and the coordinator files them here rather
              than guessing. Set <code>zone</code> and <code>region</code> under
              <code>[agent]</code> in each agent's own config file; it reports its location on the
              next poll.
            </p>
          {/if}
          {#each r.zones as z (z.key)}
            <div class="group zone" class:dead={z.live === 0}>
              <span class="group-name">
                {z.zone}
                {#if z.live === 0}<span class="dead-pill">no live agent</span>{/if}
              </span>
              <span class="group-meta">
                {z.live} up · {z.silent} silent · {z.results} results/24h
              </span>
            </div>
            {#each z.rows as a (a.agent_id)}
              <div class="row" class:retired={a.retired}>
                <span class="agent-pill">{a.agent_id}</span>
                <span class="state" class:on={a.status === 'up'}>
                  {a.status === 'up' ? 'Reporting' : 'Silent'}
                </span>
                <span class="dim">{formatRelativeTime(a.last_seen)}</span>
                <span class="mono">{a.checks_reporting}</span>
                <span class="mono">{a.results_24h}</span>
                <span class="mono">{a.assigned_checks}</span>
                <span class="actions">
                  <button class="btn danger" onclick={() => arm(a.agent_id)} disabled={purging}>
                    Purge
                  </button>
                </span>
              </div>
              {#if pending === a.agent_id}
                <!-- Outside the row grid on purpose: a confirm wide enough to say
                     what is destroyed would not fit any column. -->
                <div class="confirm-bar">
                  <p class="confirm-copy">
                    Purge <span class="mono">{a.agent_id}</span>? Every result it ever recorded is
                    deleted with it — {a.results_24h} in the last 24 hours, across
                    {a.checks_reporting}
                    {a.checks_reporting === 1 ? 'check' : 'checks'}. Those checks lose that history
                    from their uptime bars and latency graphs. The agent disappears from this list
                    because the list is those rows. This cannot be undone.
                  </p>
                  {#if purgeError}<p class="admin-error">{purgeError}</p>{/if}
                  <div class="confirm-actions">
                    <button
                      class="btn danger"
                      onclick={() => confirmPurge(a.agent_id)}
                      disabled={purging}
                    >
                      {purging ? 'Purging…' : 'Yes, purge it and its results'}
                    </button>
                    <button class="btn" onclick={() => (pending = null)} disabled={purging}>
                      Cancel
                    </button>
                  </div>
                </div>
              {/if}
            {/each}
            {#if z.rows.length === 0}
              <p class="group-note">
                Nothing live here — {z.hidden} retired
                {z.hidden === 1 ? 'agent is' : 'agents are'} hidden by the toggle above.
              </p>
            {/if}
          {/each}
        {/each}
      </div>

      {#if retiredCount > 0 && !showRetired}
        <p class="admin-muted foot">
          {retiredCount} retired {retiredCount === 1 ? 'agent is' : 'agents are'} hidden. Retiring is
          just prolonged silence — they keep every result they reported, and one reappears the
          moment it reports again.
        </p>
      {:else if retiredCount > 0}
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
  }
  .controls {
    display: flex;
    align-items: center;
    gap: 1rem;
    flex-wrap: wrap;
    margin-bottom: 0.5rem;
  }

  /* Group bars, not rows: they sit outside the seven-column template on purpose
     so a summary can be as wide as it needs and never shifts the headings. */
  .group {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 1rem;
    flex-wrap: wrap;
  }
  .group-name {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }
  .group-meta {
    color: var(--text-dim);
    font-size: 0.72rem;
    font-variant-numeric: tabular-nums;
  }
  .group.region {
    margin-top: 0.9rem;
    padding: 0.3rem 0 0.25rem;
    border-bottom: 1px solid var(--border);
    font-size: 0.85rem;
    font-weight: 600;
    letter-spacing: 0.02em;
  }
  .group.zone {
    margin-top: 0.35rem;
    padding: 0.2rem 0 0.2rem 0.55rem;
    border-left: 2px solid var(--border);
    font-size: 0.76rem;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  /* An unplaced agent is a config gap, so the bucket reads as a warning rather
     than as one more region. */
  .group.region.unplaced {
    color: var(--amber);
    border-bottom-color: var(--amber);
  }
  /* The one thing this panel exists to answer. A zone with nothing live means
     its checks produce no results and no error — nothing else would say so. */
  .group.zone.dead {
    border-left-color: var(--down);
    color: var(--down);
  }
  .dead-pill {
    text-transform: none;
    letter-spacing: 0;
    font-size: 0.68rem;
    padding: 0.05rem 0.4rem;
    border: 1px solid var(--down);
    border-radius: 999px;
  }
  .zone-alarm {
    color: var(--down);
    font-size: 0.8rem;
    line-height: 1.5;
    margin: 0 0 0.6rem;
    max-width: 46rem;
  }
  .group-note {
    color: var(--text-dim);
    font-size: 0.74rem;
    line-height: 1.5;
    margin: 0.3rem 0 0.1rem 0.6rem;
    max-width: 46rem;
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

  /* Header and body rows share this template, and no track is `auto`: the
     action column's header cell is empty, so `auto` would collapse it there and
     shift every heading off the values beneath it. */
  .row {
    display: grid;
    grid-template-columns:
      minmax(8rem, 1.5fr) 6rem minmax(5.5rem, 0.9fr) 5.5rem
      6rem 5.5rem 5rem;
    gap: 0.6rem;
    align-items: center;
    font-size: 0.8rem;
    padding: 0.34rem 0;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
  }
  .row:last-child {
    border-bottom: none;
  }
  .row.head {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    background: var(--bg-elev);
  }
  /* Shown but stepped back: a retired agent is history, not an operator. */
  .row.retired {
    opacity: 0.5;
  }
  /* Pod-style ids get long; the pill must ellipse rather than spill into the
     status column and break the shared template. */
  .row .agent-pill {
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .state {
    color: var(--text-dim);
  }
  .state.on {
    color: var(--up);
  }
  .actions {
    display: flex;
    justify-content: flex-end;
  }

  .confirm-bar {
    padding: 0.5rem 0 0.7rem;
    border-bottom: 1px solid rgba(48, 54, 61, 0.5);
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

  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  .dim {
    color: var(--text-dim);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
