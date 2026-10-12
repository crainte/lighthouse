<script lang="ts">
  import type { Draft } from './draft';
  import { POLICY_EDGES, THRESHOLD_KINDS, defaultPolicy } from './draft';
  import NumberField from '../NumberField.svelte';

  let { draft }: { draft: Draft } = $props();

  function togglePolicy(level: 'zone' | 'region' | 'global', enabled: boolean) {
    draft.rollup[level] = enabled ? defaultPolicy(level) : null;
  }
</script>

<div class="field rollup-editor">
  <span class="field-label">Rollup policy overrides</span>
  <span class="field-note">
    Blank levels inherit stored coordinator policy, then built-ins. “Degraded” counts failing
    children; “DOWN” counts hard-down children.
  </span>
  {#snippet policyLevel(level: 'zone' | 'region' | 'global', title: string, children: string)}
    <div class="policy-level">
      <label class="toggle">
        <input
          type="checkbox"
          checked={draft.rollup[level] !== null}
          onchange={(e) => togglePolicy(level, e.currentTarget.checked)}
        />
        <span><strong>{title}</strong> <em>— {children}</em></span>
      </label>
      {#if draft.rollup[level]}
        <div class="policy-thresholds">
          {#each POLICY_EDGES as edge (edge.key)}
            <label class="field">
              <span class="field-label">{edge.label}</span>
              <select bind:value={draft.rollup[level]![edge.key].kind}>
                {#each THRESHOLD_KINDS as kind (kind)}
                  <option value={kind}>{kind}</option>
                {/each}
              </select>
              {#if draft.rollup[level]![edge.key].kind === 'quorum'}
                <NumberField
                  min={1}
                  ariaLabel={`${edge.label} quorum`}
                  bind:value={draft.rollup[level]![edge.key].quorum}
                />
              {/if}
            </label>
          {/each}
        </div>
      {/if}
    </div>
  {/snippet}
  <div class="policy-levels">
    {@render policyLevel('zone', 'Zone', 'agents → zone; built-in all / all')}
    {@render policyLevel('region', 'Region', 'zones → region; built-in any / all')}
    {@render policyLevel('global', 'Check', 'regions → check; built-in any / majority')}
  </div>
</div>

<style>
  .rollup-editor {
    grid-column: 1 / -1;
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.6rem 0.7rem;
  }
  .policy-levels {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(15rem, 1fr));
    gap: 0.7rem;
    margin-top: 0.5rem;
  }
  .policy-level {
    min-width: 0;
  }
  .policy-thresholds {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.5rem;
    margin-top: 0.45rem;
  }
</style>
