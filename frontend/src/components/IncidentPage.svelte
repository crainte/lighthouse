<script lang="ts">
  import { fetchIncident } from '../lib/api';
  import type { Incident } from '../lib/api';
  import { formatDuration } from '../lib/format';
  import {
    affectSourceNote,
    checkLabel,
    checkNameMap,
    formatAbsolute,
    incidentStateColor,
  } from '../lib/incident';

  let {
    incidentId,
    checks,
    onBack,
  }: {
    incidentId: string;
    /** For resolving a check's mutable name from the id each `IncidentCheck`
     *  and `origin_check` carries. */
    checks: { check_id: string; name: string }[];
    onBack: () => void;
  } = $props();

  const names = $derived(checkNameMap(checks));

  let incident = $state<Incident | null>(null);
  let loading = $state(true);
  let error = $state<string | null>(null);

  $effect(() => {
    const id = incidentId;
    let cancelled = false;
    loading = true;
    error = null;
    fetchIncident(id)
      .then((r) => {
        if (!cancelled) incident = r.incident;
      })
      .catch((e: unknown) => {
        if (!cancelled) error = e instanceof Error ? e.message : String(e);
      })
      .finally(() => {
        if (!cancelled) loading = false;
      });
    return () => {
      cancelled = true;
    };
  });

  // Newest first: a visitor wants the latest word, not the beginning. The API
  // serves oldest-first, so copy before reversing — the prop is not ours.
  const timeline = $derived([...(incident?.updates ?? [])].reverse());
</script>

<button class="link back" onclick={onBack}>← All systems</button>

{#if loading}
  <div class="notice">Loading…</div>
{:else if error || !incident}
  <div class="notice">Incident not found.</div>
{:else}
  <article class="inc">
    <!-- A coloured rule keyed to the state, so the outcome registers before the
         text is read. -->
    <div class="inc-rule" style="background:{incidentStateColor(incident.state)}"></div>

    <header class="inc-head">
      <div class="inc-badges">
        <span class="state" style="color:{incidentStateColor(incident.state)}">
          <span class="sdot" style="background:{incidentStateColor(incident.state)}"></span>
          {incident.state}
        </span>
        {#if incident.global}<span class="tag">Global</span>{/if}
      </div>
      <h1>{incident.title}</h1>
      <p class="inc-sub">
        {#if incident.state === 'resolved'}
          Resolved after {formatDuration(incident.duration_ms)}
        {:else}
          Ongoing · {formatDuration(incident.duration_ms)} so far
        {/if}
        <span class="sep">·</span>
        <time datetime={new Date(incident.started_at).toISOString()}>
          began {formatAbsolute(incident.started_at)}
        </time>
      </p>
    </header>

    <section class="inc-block">
      <h2>Updates <span class="count">{timeline.length}</span></h2>
      {#if timeline.length === 0}
        <p class="muted">No updates posted.</p>
      {:else}
        <ol class="tl">
          {#each timeline as u (u.id)}
            <li>
              <span class="tl-dot" style="background:{incidentStateColor(u.state)}"></span>
              <div class="tl-main">
                <div class="tl-head">
                  <span class="tl-state" style="color:{incidentStateColor(u.state)}">
                    {u.state}
                  </span>
                  <time class="muted" datetime={new Date(u.created_at).toISOString()}>
                    {formatAbsolute(u.created_at)}
                  </time>
                  {#if u.author === null}
                    <!-- Written by the coordinator, not a person. Say so rather
                         than letting it read as if someone typed it. -->
                    <span class="auto">automatic</span>
                  {/if}
                </div>
                {#if u.body}<p class="tl-text">{u.body}</p>{/if}
              </div>
            </li>
          {/each}
        </ol>
      {/if}
    </section>

    <section class="inc-block">
      <h2>Affected monitors <span class="count">{incident.checks.length}</span></h2>
      {#if incident.checks.length === 0}
        <p class="muted">No monitors attached.</p>
      {:else}
        <ul class="mons">
          {#each incident.checks as c (c.check_id)}
            <li>
              <span class="mono">{checkLabel(names, c.check_id)}</span>
              {#if c.source === 'dependency'}
                <span class="via" title={affectSourceNote(c.source)}>via dependency</span>
              {/if}
            </li>
          {/each}
        </ul>
      {/if}
    </section>
  </article>
{/if}

<style>
  .back {
    margin-bottom: 1.25rem;
    display: inline-block;
  }
  .inc {
    background: var(--bg-elev);
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow: hidden;
  }
  .inc-rule {
    height: 3px;
  }
  .inc-head {
    padding: 1.3rem 1.5rem 1.15rem;
    border-bottom: 1px solid var(--border);
  }
  .inc-badges {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.55rem;
  }
  .state {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.68rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  .sdot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
  }
  .tag {
    font-size: 0.66rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-dim);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0.05rem 0.45rem;
  }
  .inc-head h1 {
    margin: 0;
    font-size: 1.5rem;
    font-weight: 600;
    line-height: 1.25;
    letter-spacing: -0.015em;
  }
  .inc-sub {
    margin: 0.5rem 0 0;
    font-size: 0.86rem;
    color: var(--text-dim);
  }
  .sep {
    margin: 0 0.35rem;
    opacity: 0.5;
  }
  .inc-block {
    padding: 1.15rem 1.5rem 1.25rem;
    border-bottom: 1px solid var(--border);
  }
  .inc-block:last-child {
    border-bottom: none;
  }
  .inc-block h2 {
    margin: 0 0 0.85rem;
    font-size: 0.72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--text-dim);
  }
  .count {
    color: var(--text);
    margin-left: 0.2rem;
  }
  .muted {
    margin: 0;
    color: var(--text-dim);
    font-size: 0.88rem;
  }
  /* Timeline with a connecting rail: the sequence is the story. */
  .tl {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .tl li {
    position: relative;
    display: flex;
    gap: 0.85rem;
    padding-bottom: 1.15rem;
  }
  .tl li:last-child {
    padding-bottom: 0;
  }
  .tl li:not(:last-child)::before {
    content: '';
    position: absolute;
    left: 4px;
    top: 1.05rem;
    bottom: 0.1rem;
    width: 1px;
    background: var(--border);
  }
  .tl-dot {
    flex: none;
    width: 9px;
    height: 9px;
    border-radius: 50%;
    margin-top: 0.35rem;
  }
  .tl-main {
    min-width: 0;
  }
  .tl-head {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.55rem;
    font-size: 0.78rem;
  }
  .tl-state {
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 0.68rem;
  }
  .auto {
    font-size: 0.68rem;
    color: var(--text-dim);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0 0.4rem;
  }
  .tl-text {
    margin: 0.35rem 0 0;
    font-size: 0.93rem;
    line-height: 1.6;
    white-space: pre-wrap;
  }
  .mons {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .mons li {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.4rem 0;
    border-bottom: 1px solid var(--border);
  }
  .mons li:last-child {
    border-bottom: none;
  }
  .via {
    font-size: 0.72rem;
    color: var(--text-dim);
  }
</style>
