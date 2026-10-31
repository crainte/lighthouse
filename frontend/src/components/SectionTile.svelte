<script lang="ts">
  import type { SectionRollup, Status } from '../lib/api';
  import { statusColor } from '../lib/format';

  let {
    name,
    rollup,
    count,
    onopen,
  }: {
    name: string;
    /** Absent until `/api/v1/status` lands, or when the section has no checks.
     *  Renders unknown rather than guessing a colour from nothing. */
    rollup?: SectionRollup;
    /** Checks in the section, from the status list. `rollup.checks` agrees, but
     *  the caption must still read correctly before the rollup arrives. */
    count: number;
    onopen: (name: string) => void;
  } = $props();

  const status = $derived<Status>(rollup?.status ?? 'unknown');
  // A section whose members are all paused has no live health to assert.
  const paused = $derived(!!rollup?.paused && rollup.paused === rollup.checks && rollup.checks > 0);

  const statusLabel = $derived(
    paused
      ? 'Paused'
      : status === 'up'
        ? 'Operational'
        : status === 'degraded'
          ? 'Degraded'
          : status === 'down'
            ? 'Down'
            : 'Unknown',
  );

  // What the tile says instead of a history strip. A red tile an operator can
  // triage from the summary is the whole reason the counts are here: without
  // them "Down" could mean one check of forty or all forty.
  const caption = $derived.by(() => {
    const parts = [`${count} ${count === 1 ? 'check' : 'checks'}`];
    if (rollup?.down) parts.push(`${rollup.down} down`);
    if (rollup?.degraded) parts.push(`${rollup.degraded} degraded`);
    if (rollup?.paused) parts.push(`${rollup.paused} paused`);
    return parts.join(' · ');
  });
</script>

<div class="group tile">
  <button class="group-head" onclick={() => onopen(name)}>
    <span class="group-name">
      <span class="status-dot" style="background:{statusColor(paused ? 'unknown' : status)}"></span>
      {name}
      <span class="group-count">{caption}</span>
    </span>
    <span class="group-meta">
      <span class="group-status {paused ? 'paused' : status}">{statusLabel}</span>
      <svg class="chevron" viewBox="0 0 24 24" width="16" height="16" aria-hidden="true">
        <path
          d="M9 6l6 6-6 6"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
    </span>
  </button>
</div>

<style>
  /* Deliberately shaped like a collapsed group: to a visitor the difference is
     that opening it is a page rather than an expansion, and nothing else. The
     chevron does not rotate here — there is no open state to signal. */
  .tile .group-head {
    cursor: pointer;
  }
</style>
