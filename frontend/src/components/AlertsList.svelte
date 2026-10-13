<script lang="ts">
  import type { Alert } from '../lib/api';
  import { formatRelativeTime } from '../lib/format';
  import { checkLabel, checkNameMap, formatAbsolute } from '../lib/incident';

  let {
    alerts,
    checks,
    onOpenIncident,
  }: {
    alerts: Alert[];
    checks: { check_id: string; name: string }[];
    /** Follow a transition through to the incident it belongs to. In the admin
     *  console this opens the editor, not the public page: an operator reading
     *  the alert feed wants the thing they can act on. */
    onOpenIncident?: (id: string) => void;
  } = $props();

  const names = $derived(checkNameMap(checks));

  function alertLabel(alert: Alert): string {
    if (alert.name) return alert.name;
    return alert.scope === 'check' ? checkLabel(names, alert.node_id) : alert.node_id;
  }

  /** Severity from the transition itself. A recovery is not an alarm, an
   *  un-evaluable probe is not an outage, and degraded is an amber warning. */
  function tone(a: Alert): { pill: string; label: string } {
    if (a.current_state === 'down') return { pill: 'red', label: 'down' };
    if (a.current_state === 'degraded') return { pill: 'amber', label: 'degraded' };
    if (a.current_state === 'unknown') return { pill: 'amber', label: 'unknown' };
    return { pill: 'green', label: 'recovered' };
  }

  const counts = $derived({
    down: alerts.filter((a) => a.current_state === 'down').length,
    degraded: alerts.filter((a) => a.current_state === 'degraded').length,
    unknown: alerts.filter((a) => a.current_state === 'unknown').length,
    up: alerts.filter((a) => a.current_state === 'up').length,
  });

  /** What the transition means, in a sentence, so the row is readable without
   *  decoding two state names. */
  function sentence(a: Alert): string {
    const from = a.previous_state;
    if (from === null) return `First observation read ${a.current_state}.`;
    if (a.current_state === 'up') return `Recovered from ${from}.`;
    if (a.current_state === 'down') return `Confirmed down, previously ${from}.`;
    if (a.current_state === 'degraded') return `Degraded, previously ${from}.`;
    return `Became un-evaluable, previously ${from}.`;
  }
</script>

{#if alerts.length === 0}
  <div class="otable"><div class="ot-empty">No recent state changes. New alerts will appear here as monitors change state.</div></div>
{:else}
  <div class="stat-grid">
    <div class="stat-card">
      <div class="stat-label">Down</div>
      <div class="stat-value {counts.down ? 'red' : 'green'}">{counts.down}</div>
      <div class="stat-sub">of {alerts.length} recorded</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Degraded</div>
      <div class="stat-value amber">{counts.degraded}</div>
      <div class="stat-sub">partially available</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Un-evaluable</div>
      <div class="stat-value amber">{counts.unknown}</div>
      <div class="stat-sub">probe could not decide</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Recoveries</div>
      <div class="stat-value">{counts.up}</div>
      <div class="stat-sub">came back up</div>
    </div>
  </div>

  <ul class="alert-feed">
    {#each alerts as a (a.id)}
      {@const t = tone(a)}
      <li
        class="alert-card"
        class:flagged={a.current_state !== 'up'}
        class:bad={a.current_state === 'down'}
      >
        <div class="ac-top">
          <span class="opill {t.pill}">{t.label}</span>
          <span class="ac-title">{alertLabel(a)}</span>
          <span class="ac-id">{a.alert_type.replace('_', ' ')}</span>
        </div>
        <p class="ac-desc">{sentence(a)}</p>
        <div class="ac-meta">
          <span class="opill grey">{a.scope}</span>
          <span class="field-note" title={formatAbsolute(a.timestamp)}>
            {formatRelativeTime(a.timestamp)}
          </span>
          {#if a.incident_id && onOpenIncident}
            {@const id = a.incident_id}
            <button class="btn ac-action" onclick={() => onOpenIncident(id)}>Open incident</button>
          {:else}
            <span class="field-note ac-action">No incident — inside its confirmation window</span>
          {/if}
        </div>
      </li>
    {/each}
  </ul>
{/if}

<style>
  .alert-feed {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }
  .alert-card {
    background: var(--ad-panel);
    border: 1px solid var(--ad-line);
    border-radius: 8px;
    padding: 0.7rem 0.9rem 0.75rem;
  }
  .ac-top {
    display: flex;
    align-items: center;
    gap: 0.55rem;
  }
  .ac-title {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.9rem;
    color: var(--ad-cyan);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  /* The machine's own classification, pushed to the far edge like a ticket id. */
  .ac-id {
    margin-left: auto;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.68rem;
    color: var(--ad-faint);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .ac-desc {
    margin: 0.3rem 0 0.55rem;
    font-size: 0.83rem;
    color: var(--ad-dim);
  }
  .ac-meta {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
  }
  .ac-action {
    margin-left: auto;
  }
</style>
