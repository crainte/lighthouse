<script lang="ts">
  import type { Incident, IncidentGroup } from '../lib/api';
  import { formatDuration, formatRelativeTime } from '../lib/format';
  import {
    checkLabel,
    checkNameMap,
    incidentStateColor,
    regionFromIncidentId,
  } from '../lib/incident';

  let {
    incidents,
    checks,
    onOpen,
    groups,
  }: {
    incidents: Incident[];
    /** For resolving a check's mutable name from the id each `IncidentCheck`
     *  carries. */
    checks: { check_id: string; name: string }[];
    /** Follow the incident's public permalink. */
    onOpen: (id: string) => void;
    /** Only a global node's merged answer sets this: the same `incidents`
     *  folded by same-check overlapping windows. Absent on a regional
     *  dashboard, where each incident is already the whole story. */
    groups?: IncidentGroup[];
  } = $props();

  const names = $derived(checkNameMap(checks));

  const byId = $derived(new Map(incidents.map((i) => [i.id, i])));

  type Card = {
    key: string;
    title: string;
    state: Incident['state'];
    startedAt: number;
    resolvedAt: number | null;
    global: boolean;
    attribution: IncidentGroup['attributed_to'];
    members: Incident[];
  };

  const cards = $derived<Card[]>(
    groups && groups.length > 0
      ? groups.map((g) => {
          const members = g.regions
            .map((r) => byId.get(r.id))
            .filter((i): i is Incident => i !== undefined);
          return {
            key: g.id,
            title: g.title,
            state: g.state,
            startedAt: g.started_at,
            resolvedAt: g.resolved_at,
            global: members.some((m) => m.global),
            attribution: g.attributed_to,
            members,
          };
        })
      : incidents.map((i) => ({
          key: i.id,
          title: i.title,
          state: i.state,
          startedAt: i.started_at,
          resolvedAt: i.resolved_at,
          global: i.global,
          attribution: undefined,
          members: [i],
        })),
  );

  function mergedChecks(members: Incident[]) {
    const byCheckId = new Map<string, Incident['checks'][number]>();
    for (const m of members) {
      for (const c of m.checks) {
        if (!byCheckId.has(c.check_id)) byCheckId.set(c.check_id, c);
      }
    }
    return [...byCheckId.values()];
  }

  /** Deliberately the status page's own card vocabulary rather than the admin
   *  console's table: this sits directly under the monitor cards, and a flat
   *  hairline grid in the middle of them reads as a different site. */
</script>

{#if incidents.length === 0}
  <p class="dim">No incidents recorded.</p>
{:else}
  <div class="components">
    {#each cards as card (card.key)}
      <button class="component" onclick={() => onOpen(card.members[0]?.id ?? card.key)}>
        <div class="component-head">
          <span class="component-name">
            <span class="status-dot" style="background:{incidentStateColor(card.state)}"></span>
            {card.title}
            {#if card.global}<span class="glob">global</span>{/if}
          </span>
          <span class="component-meta">
            <span class="component-uptime">
              {formatDuration((card.resolvedAt ?? Date.now()) - card.startedAt)}
            </span>
            <span class="component-latency">
              {card.resolvedAt ? 'resolved' : card.state}
            </span>
          </span>
        </div>
        {#if card.members.length > 1}
          <div class="regions">
            {#each card.members as m (m.id)}
              <span class="region-chip" class:region-open={m.state !== 'resolved'}>
                {regionFromIncidentId(m.id) || '—'}: {m.state === 'resolved'
                  ? `recovered ${formatRelativeTime(m.resolved_at ?? m.started_at)}`
                  : `since ${formatRelativeTime(m.started_at)}`}
              </span>
            {/each}
          </div>
        {/if}
        {#if card.attribution}
          <div class="attribution">
            probably caused by {checkLabel(names, card.attribution.check_id)} ({card.attribution.region})
          </div>
        {/if}
        <div class="inc-foot">
          <span class="when">
            {#if card.resolvedAt !== null}
              Ended {formatRelativeTime(card.resolvedAt)} · Duration {formatDuration(card.resolvedAt - card.startedAt)}
            {:else}
              Started {formatRelativeTime(card.startedAt)}
            {/if}
          </span>
          {#if mergedChecks(card.members).length}
            <span class="mons">
              {#each mergedChecks(card.members) as c (c.check_id)}
                <span class="mon" class:auto={c.source === 'dependency'}>
                  {checkLabel(names, c.check_id)}
                </span>
              {/each}
            </span>
          {/if}
        </div>
      </button>
    {/each}
  </div>
{/if}

<style>
  /* Head spacing is tighter than a monitor card's: there is no uptime strip
     underneath to separate from. */
  .component-head {
    margin-bottom: 0.45rem;
  }
  .glob {
    font-size: 0.62rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-dim);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0.05rem 0.4rem;
  }
  /* A folded outage's per-region detail: which regions saw it, and — the
     part folding must never hide — which of them has not recovered. */
  .regions {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
    margin-bottom: 0.4rem;
  }
  .region-chip {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.68rem;
    color: var(--text-dim);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.02rem 0.34rem;
  }
  .region-chip.region-open {
    color: var(--ad-red, #ff8078);
    border-color: currentColor;
  }
  .attribution {
    font-size: 0.72rem;
    color: var(--ad-violet, #8b8bf5);
    margin-bottom: 0.4rem;
  }
  .inc-foot {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.75rem;
    flex-wrap: wrap;
  }
  .when {
    font-size: 0.75rem;
    color: var(--text-dim);
  }
  .mons {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }
  .mon {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.7rem;
    color: var(--text-dim);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.02rem 0.34rem;
  }
  /* Attached by dependency rather than by falling over itself. Dashed so a
     reader can see the difference without being told. */
  .mon.auto {
    border-style: dashed;
  }
</style>
