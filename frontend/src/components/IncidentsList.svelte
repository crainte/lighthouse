<script lang="ts">
  import type { Incident, IncidentGroup } from '../lib/api';
  import { formatDuration, formatRelativeTime } from '../lib/format';
  import {
    affectSourceNote,
    checkLabel,
    checkNameMap,
    regionFromIncidentId,
    stateTone,
  } from '../lib/incident';

  /** The admin console's incident table. The public status page has its own
   *  card rendering in `PastIncidents.svelte` — the two share no layout, and
   *  one component carrying both templates was how the console's hairline grid
   *  ended up in the middle of a page built from rounded cards. */
  let {
    incidents,
    checks,
    onEdit,
    onAck,
    groups,
  }: {
    incidents: Incident[];
    /** For resolving a check's mutable name from the id each `IncidentCheck`
     *  carries. */
    checks: { check_id: string; name: string }[];
    onEdit: (i: Incident) => void;
    /** Acknowledging is posting an `investigating` update — there is no
     *  acknowledge endpoint — so this is only offered while `detected`. */
    onAck: (i: Incident) => void;
    /** Only a global node's merged answer sets this: the same `incidents`
     *  folded by same-check overlapping windows, one entry per outage rather
     *  than one per probing region. A regional dashboard never supplies it,
     *  and the table falls back to one row per incident exactly as before —
     *  grouping is presentation the global tier adds, not something either
     *  side of this component has to special-case beyond this prop. */
    groups?: IncidentGroup[];
  } = $props();

  const names = $derived(checkNameMap(checks));

  const open = $derived(incidents.filter((i) => i.state !== 'resolved'));

  const byId = $derived(new Map(incidents.map((i) => [i.id, i])));

  /** One row per outage. A grouped row's `members` is every region's own
   *  incident for that outage — folding rows must not lose which regions were
   *  affected, so the full member list rides along rather than a count. */
  type Row = {
    key: string;
    title: string;
    state: Incident['state'];
    startedAt: number;
    resolvedAt: number | null;
    global: boolean;
    attribution: IncidentGroup['attributed_to'];
    members: Incident[];
  };

  const rows = $derived<Row[]>(
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

  /** Which multi-region groups are expanded to their per-region detail.
   *  Collapsed by default: the fold exists so a fan-out check is one row, not
   *  a wall of duplicates, until an operator asks for the detail. */
  let expanded = $state<Set<string>>(new Set());
  function toggle(key: string) {
    const next = new Set(expanded);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    expanded = next;
  }

  /** The union of every member's attached checks, first attachment wins. Two
   *  regions of the same outage almost always attach the same checks; this
   *  only matters when one region's dependency reconciliation attached
   *  something the other's didn't. */
  function mergedChecks(members: Incident[]) {
    const byCheckId = new Map<string, Incident['checks'][number]>();
    for (const m of members) {
      for (const c of m.checks) {
        if (!byCheckId.has(c.check_id)) byCheckId.set(c.check_id, c);
      }
    }
    return [...byCheckId.values()];
  }
</script>

{#snippet affects(list: Incident['checks'])}
  <span class="affects">
    {#each list.slice(0, 3) as c (c.check_id)}
      <span
        class="opill name {c.source === 'origin'
          ? 'cyan'
          : c.source === 'dependency'
            ? 'violet'
            : 'grey'}"
        title={affectSourceNote(c.source)}
      >
        {checkLabel(names, c.check_id)}
      </span>
    {/each}
    {#if list.length > 3}
      <span class="more">+{list.length - 3}</span>
    {/if}
    {#if list.length === 0}
      <span class="more">—</span>
    {/if}
  </span>
{/snippet}

{#if incidents.length === 0}
  <div class="otable"><div class="ot-empty">No incidents.</div></div>
{:else}
  <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-label">Open</div>
        <div class="stat-value {open.length ? 'red' : 'green'}">{open.length}</div>
        <div class="stat-sub">of {incidents.length} recorded</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Unacknowledged</div>
        <div class="stat-value amber">{open.filter((i) => !i.acknowledged).length}</div>
        <div class="stat-sub">nobody has posted yet</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Global</div>
        <div class="stat-value">{incidents.filter((i) => i.global).length}</div>
        <div class="stat-sub">shown page-wide</div>
      </div>
    </div>

  <div class="otable">
    <div class="ot-head">
      <span>Title</span>
      <span>Duration</span>
      <span>State</span>
      <span>Affects</span>
      <span class="ot-right">Edit</span>
    </div>
    {#each rows as row (row.key)}
      <div class="ot-row" class:flagged={row.state !== 'resolved'} class:bad={row.state === 'detected'}>
        <span class="inc-title">
          {#if row.members.length > 1}
            <button
              class="expand"
              onclick={() => toggle(row.key)}
              aria-expanded={expanded.has(row.key)}
              title="{row.members.length} regions detected this independently — expand for each region's own incident."
            >{expanded.has(row.key) ? '▾' : '▸'}</button>
          {/if}
          <button class="title-link" onclick={() => onEdit(row.members[0])}>{row.title}</button>
          {#if row.global}<span class="opill cyan">global</span>{/if}
          {#if row.members.length > 1}
            <span class="opill grey">{row.members.length} regions</span>
          {/if}
          <span class="started">started {formatRelativeTime(row.startedAt)}</span>
          {#if row.attribution}
            <span
              class="attribution"
              title="This outage overlaps an open incident for its upstream, {checkLabel(names, row.attribution.check_id)}, in {row.attribution.region}."
            >
              probably caused by {checkLabel(names, row.attribution.check_id)} ({row.attribution.region})
            </span>
          {/if}
        </span>

        <span class="dur">
          {formatDuration((row.resolvedAt ?? Date.now()) - row.startedAt)}{row.resolvedAt ? '' : ' · ongoing'}
        </span>

        <span><span class="opill {stateTone(row.state)}">{row.state}</span></span>

        {@render affects(mergedChecks(row.members))}

        <span class="ot-right">
          {#if row.members.length === 1}
            {#if row.state === 'detected' && !row.members[0].acknowledged}
              <button class="btn" onclick={() => onAck(row.members[0])}>Ack</button>
            {/if}
            <button class="btn" onclick={() => onEdit(row.members[0])}>Edit</button>
          {/if}
        </span>
      </div>

      {#if row.members.length > 1 && expanded.has(row.key)}
        {#each row.members as m (m.id)}
          <div class="ot-row region-row" class:flagged={m.state !== 'resolved'} class:bad={m.state === 'detected'}>
            <span class="inc-title">
              <span class="opill grey region-tag">{regionFromIncidentId(m.id) || '—'}</span>
              <button class="title-link" onclick={() => onEdit(m)}>{m.title}</button>
              <span class="started">started {formatRelativeTime(m.started_at)}</span>
            </span>

            <span class="dur">
              {formatDuration(m.duration_ms)}{m.state === 'resolved' ? '' : ' · ongoing'}
            </span>

            <span><span class="opill {stateTone(m.state)}">{m.state}</span></span>

            {@render affects(m.checks)}

            <span class="ot-right">
              {#if m.state === 'detected' && !m.acknowledged}
                <button class="btn" onclick={() => onAck(m)}>Ack</button>
              {/if}
              <button class="btn" onclick={() => onEdit(m)}>Edit</button>
            </span>
          </div>
        {/each}
      {/if}
    {/each}
  </div>
{/if}

<style>
  /* One template for header and body. Two declarations would drift the moment
     either changed. */
  .otable {
    --ot-cols: minmax(12rem, 2.2fr) 9rem 7rem minmax(9rem, 1.4fr) 7.5rem;
  }
  .inc-title {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem;
    min-width: 0;
  }
  .title-link {
    font-size: 0.88rem;
    color: var(--text);
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .title-link {
    background: none;
    border: 0;
    padding: 0;
    font-family: inherit;
    text-align: left;
    cursor: pointer;
  }
  .title-link:hover {
    color: var(--ad-cyan);
  }
  .started {
    flex-basis: 100%;
    font-size: 0.7rem;
    color: var(--ad-faint);
  }
  .dur {
    font-size: 0.8rem;
    color: var(--ad-dim);
  }
  .affects {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem;
    min-width: 0;
  }
  .more {
    font-size: 0.72rem;
    color: var(--ad-faint);
  }
  /* The disclosure toggle for a folded outage. Plain-text arrow rather than an
     icon font: this table has none, and one glyph is not worth adding one. */
  .expand {
    background: none;
    border: 0;
    padding: 0 0.1rem;
    cursor: pointer;
    color: var(--ad-dim);
    font-size: 0.75rem;
    line-height: 1;
  }
  .expand:hover {
    color: var(--ad-cyan);
  }
  /* Attribution reads as a hint, not a fact the operator is meant to act on
     without looking — dim and separate from the title it hangs off. */
  .attribution {
    flex-basis: 100%;
    font-size: 0.72rem;
    color: var(--ad-violet, #a5a5f7);
  }
  .region-row {
    background: var(--ad-panel-2, rgba(255, 255, 255, 0.02));
  }
  .region-row .title-link {
    font-size: 0.82rem;
  }
  .region-tag {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  @media (max-width: 820px) {
    .otable {
      --ot-cols: minmax(0, 1fr);
    }
    .ot-head {
      display: none;
    }
  }
</style>
