<script lang="ts">
  import type { Incident } from '../lib/api';
  import { formatDuration, formatRelativeTime } from '../lib/format';
  import { affectSourceNote, checkLabel, checkNameMap, stateTone } from '../lib/incident';

  /** The admin console's incident table. The public status page has its own
   *  card rendering in `PastIncidents.svelte` — the two share no layout, and
   *  one component carrying both templates was how the console's hairline grid
   *  ended up in the middle of a page built from rounded cards. */
  let {
    incidents,
    checks,
    onEdit,
    onAck,
  }: {
    incidents: Incident[];
    /** For resolving a check's mutable name from the id each `IncidentCheck`
     *  carries. */
    checks: { check_id: string; name: string }[];
    onEdit: (i: Incident) => void;
    /** Acknowledging is posting an `investigating` update — there is no
     *  acknowledge endpoint — so this is only offered while `detected`. */
    onAck: (i: Incident) => void;
  } = $props();

  const names = $derived(checkNameMap(checks));

  const open = $derived(incidents.filter((i) => i.state !== 'resolved'));
</script>

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
    {#each incidents as i (i.id)}
      <div class="ot-row" class:flagged={i.state !== 'resolved'} class:bad={i.state === 'detected'}>
        <span class="inc-title">
          <button class="title-link" onclick={() => onEdit(i)}>{i.title}</button>
          {#if i.global}<span class="opill cyan">global</span>{/if}
          <span class="started">started {formatRelativeTime(i.started_at)}</span>
        </span>

        <span class="dur">
          {formatDuration(i.duration_ms)}{i.state === 'resolved' ? '' : ' · ongoing'}
        </span>

        <span><span class="opill {stateTone(i.state)}">{i.state}</span></span>

        <span class="affects">
          {#each i.checks.slice(0, 3) as c (c.check_id)}
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
          {#if i.checks.length > 3}
            <span class="more">+{i.checks.length - 3}</span>
          {/if}
          {#if i.checks.length === 0}
            <span class="more">—</span>
          {/if}
        </span>

        <span class="ot-right">
          {#if i.state === 'detected'}
            <button class="btn" onclick={() => onAck(i)}>Ack</button>
          {/if}
          <button class="btn" onclick={() => onEdit(i)}>Edit</button>
        </span>
      </div>
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
  @media (max-width: 820px) {
    .otable {
      --ot-cols: minmax(0, 1fr);
    }
    .ot-head {
      display: none;
    }
  }
</style>
