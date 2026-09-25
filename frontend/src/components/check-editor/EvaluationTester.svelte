<script lang="ts">
  import type { CheckKind } from '../../lib/api';
  import type { EvalResult } from '../../lib/admin';
  import { testEval } from '../../lib/admin';
  import NumberField from '../NumberField.svelte';

  let { kind, evalScript = $bindable() }: { kind: CheckKind; evalScript: string } = $props();

  // Read once, here, rather than reactively: a saved check that already
  // carries a script must arrive open, but clearing it must not then slam
  // the panel shut under the cursor.
  // svelte-ignore state_referenced_locally
  let open = $state(evalScript.trim().length > 0);

  let evalResult = $state<EvalResult | null>(null);
  let evalError = $state<string | null>(null);
  let testing = $state(false);
  let sample = $state<{
    status: number | null;
    latency: number | null;
    body: string;
    metric: number | null;
  }>({
    status: 200,
    latency: 42,
    body: 'ok',
    metric: 1,
  });

  function fail(e: unknown): string {
    return e instanceof Error ? e.message : String(e);
  }

  async function runTest() {
    if (testing) return;
    testing = true;
    evalResult = null;
    evalError = null;
    try {
      evalResult = await testEval({
        eval: evalScript,
        status: sample.status ?? 0,
        latency: sample.latency ?? 0,
        body: sample.body,
        ...(kind === 'openmetrics' ? { metric: sample.metric ?? 0 } : {}),
      });
    } catch (e) {
      evalError = fail(e);
    } finally {
      testing = false;
    }
  }
</script>

<details class="advanced" bind:open>
  <summary class="advanced-summary">
    <span class="chev" aria-hidden="true">▸</span> Advanced — custom verdict script
    {#if evalScript.trim()}<span class="badge-count">Lua in use</span>{/if}
  </summary>
  <div class="advanced-body">
    <div class="eval-block">
  <div class="field-label">Custom eval (Lua)</div>
  <textarea
    class="mono"
    rows="10"
    spellcheck="false"
    bind:value={evalScript}
    placeholder={'local code = math.floor(status / 100)\nif status == 429 or (code >= 2 and code <= 3) then\n  return "UP", latency\nend\nreturn "DOWN"'}
  ></textarea>
  <p class="field-note">
    In scope: <code>status</code> (number, 0 when the probe never connected),
    <code>latency</code>
    (ms), <code>body</code> (string, truncated), <code>headers</code> (table, lowercased keys).
    Return <code>"UP"</code> or <code>"DOWN"</code>, optionally a latency override as a second
    value. Returning nothing — or leaving this empty — uses the built-in 2xx–3xx logic.
    {#if kind === 'openmetrics'}
      For OpenMetrics, <code>metric</code> is the selected sample's value (number), and
      <code>metrics</code> is an array of every sample from the same scrape.
    {/if}
  </p>

  <div class="try-row">
    {#if kind === 'openmetrics'}
      <label class="field try-num">
        <span class="field-label">metric</span>
        <NumberField bind:value={sample.metric} />
      </label>
    {:else}
      <label class="field try-num">
        <span class="field-label">status</span>
        <NumberField bind:value={sample.status} />
      </label>
      <label class="field try-num">
        <span class="field-label">latency (ms)</span>
        <NumberField bind:value={sample.latency} />
      </label>
      <label class="field try-body">
        <span class="field-label">body</span>
        <input class="mono" bind:value={sample.body} />
      </label>
    {/if}
    <button class="btn" onclick={runTest} disabled={testing}>
      {testing ? 'Testing…' : 'Test script'}
    </button>
  </div>

  {#if evalError}
    <p class="admin-error mono">{evalError}</p>
  {:else if evalResult}
    <p class="verdict">
      <span class="verdict-pill {evalResult.verdict}">{evalResult.verdict.toUpperCase()}</span>
      <span class="admin-muted">reported latency {evalResult.latency_ms} ms</span>
    </p>
  {/if}
    </div>
  </div>
</details>

<style>
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-variant-numeric: tabular-nums;
  }
  /* Nested disclosure for the fields most checks never touch. Quieter than the
     section summaries above it: this is a second-order fold, not a peer. */
  .advanced {
    margin-top: 0.9rem;
    border-top: 1px solid var(--border);
  }
  .advanced-summary {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.55rem 0;
    cursor: pointer;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
    user-select: none;
    list-style: none;
  }
  .advanced-summary::-webkit-details-marker {
    display: none;
  }
  .advanced-summary:hover {
    color: var(--text);
  }
  .advanced-summary:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }
  .advanced-summary .chev {
    transition: transform 0.12s ease;
  }
  .advanced[open] > .advanced-summary .chev {
    transform: rotate(90deg);
  }
  .advanced-body {
    padding-bottom: 0.3rem;
  }
  /* Counts what departs from the defaults. Lit rather than grey: a fold that
     hides a changed value must say so on its own lid. */
  .badge-count {
    font-size: 0.62rem;
    font-weight: 600;
    letter-spacing: 0.03em;
    padding: 0.05rem 0.4rem;
    border-radius: 999px;
    border: 1px solid var(--accent);
    color: var(--accent);
    text-transform: none;
  }
  .eval-block {
    margin-top: 0.9rem;
    border-top: 1px solid var(--border);
    padding-top: 0.7rem;
  }
  .eval-block textarea {
    resize: vertical;
    line-height: 1.45;
    margin-top: 0.3rem;
  }
  .eval-block code {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--text);
  }
  .eval-block .field-note {
    display: block;
    margin: 0.4rem 0 0;
    line-height: 1.5;
  }
  .try-row {
    display: flex;
    align-items: flex-end;
    gap: 0.6rem;
    margin-top: 0.7rem;
  }
  .try-num {
    width: 7rem;
    flex: none;
  }
  .try-body {
    flex: 1 1 auto;
  }
  /* A wrong verdict here is the difference between a page that alerts and one
     that lies, so it is stated at size and in colour, not as a footnote. */
  .verdict {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin: 0.6rem 0 0;
  }
  .verdict-pill {
    font-size: 0.9rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    padding: 0.15rem 0.7rem;
    border-radius: 6px;
  }
  .verdict-pill.up {
    background: rgba(63, 185, 80, 0.16);
    border: 1px solid var(--up);
    color: var(--up);
  }
  .verdict-pill.down {
    background: rgba(248, 81, 73, 0.16);
    border: 1px solid var(--down);
    color: var(--down);
  }
</style>
