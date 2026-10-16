<script lang="ts">
  import { fetchIncident, type Incident } from '../lib/api';
  import { formatRelativeTime } from '../lib/format';

  let {
    incident,
    checks,
    onOpen,
  }: {
    incident: Incident;
    checks: { check_id: string; name: string }[];
    onOpen: (id: string) => void;
  } = $props();

  let detail = $state<Incident | null>(null);
  let loading = $state(true);
  let error = $state(false);

  // Fetch the public narrative, not just the summary row. A websocket-updated
  // incident object also triggers refetching updated messages.
  $effect(() => {
    const current = incident;
    let cancelled = false;
    detail = null;
    loading = true;
    error = false;
    fetchIncident(current.id)
      .then((response) => { if (!cancelled) detail = response.incident; })
      .catch(() => { if (!cancelled) error = true; })
      .finally(() => { if (!cancelled) loading = false; });
    return () => { cancelled = true; };
  });

  const messages = $derived([...(detail?.updates ?? [])].reverse());
  const affected = $derived(incident.checks.map((entry) =>
    checks.find((check) => check.check_id === entry.check_id)?.name ?? entry.check_id,
  ));
</script>

<section class="public-incident-panel" aria-label="Active incident updates">
  <div class="public-incident-head">
    <div>
      <span class="public-eyebrow">Incident updates</span>
      <h2>{incident.title}</h2>
      <p>Started {formatRelativeTime(incident.started_at)} · <span class="public-incident-state">{incident.state}</span></p>
    </div>
    <button class="public-text-link" onclick={() => onOpen(incident.id)}>Full incident →</button>
  </div>

  {#if affected.length}
    <div class="public-affected">
      <strong>Affected monitors</strong>
      <div>{#each affected as name, i (i)}<span>{name}</span>{/each}</div>
    </div>
  {:else}
    <p class="public-muted">Affected monitors have not been specified for this incident.</p>
  {/if}

  {#if loading}
    <p class="public-muted">Loading published updates…</p>
  {:else if error}
    <p class="public-muted">Incident updates are temporarily unavailable. Open the incident to retry.</p>
  {:else if messages.length === 0}
    <p class="public-muted">No incident updates have been published yet. The current state is {incident.state}.</p>
  {:else}
    <ol class="public-incident-timeline">
      {#each messages.slice(0, 4) as update (update.id)}
        <li>
          <span class="public-timeline-dot" class:resolved={update.state === 'resolved'} aria-hidden="true"></span>
          <div class="public-timeline-body">
            <div class="public-timeline-title">
              <strong>{update.state}</strong>
              <time datetime={new Date(update.created_at).toISOString()}>{formatRelativeTime(update.created_at)}</time>
            </div>
            {#if update.body}
              <p>{update.body}</p>
            {:else}
              <p class="public-muted">Automatic state change</p>
            {/if}
            {#if update.author === null}<small>Automatic update</small>{/if}
          </div>
        </li>
      {/each}
    </ol>
    {#if messages.length > 4}<button class="public-text-link" onclick={() => onOpen(incident.id)}>See all {messages.length} updates →</button>{/if}
  {/if}
</section>
