<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { Status } from '../lib/api';
  import { statusColor } from '../lib/format';

  let {
    name,
    status = 'up',
    paused = false,
    count,
    noun = 'monitor',
    collapsed = false,
    children,
  }: {
    name: string;
    status?: Status;
    /** All checks in this group are intentionally disabled. */
    paused?: boolean;
    /** Shown beside the name, e.g. "4 monitors". Omitted when undefined. */
    count?: number;
    /** Singular noun for the count label; pluralized with a trailing "s". */
    noun?: string;
    /** Start closed. Read once, at creation: after that the visitor owns the
     *  state, so a re-render must never slam the group shut under them. */
    collapsed?: boolean;
    children: Snippet;
  } = $props();

  // Capturing only the initial value is the point: the admin sets how the page
  // FIRST renders, and after that the visitor's click wins.
  // svelte-ignore state_referenced_locally
  let open = $state(!collapsed);

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
</script>

<div class="group" class:open>
  <button
    class="group-head"
    onclick={() => (open = !open)}
    aria-expanded={open}
  >
    <span class="group-name">
      <span class="status-dot" style="background:{statusColor(paused ? 'unknown' : status)}"></span>
      {name}
      {#if count !== undefined}
        <span class="group-count">{count} {count === 1 ? noun : `${noun}s`}</span>
      {/if}
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

  {#if open}
    <div class="group-body">
      {@render children()}
    </div>
  {/if}
</div>
