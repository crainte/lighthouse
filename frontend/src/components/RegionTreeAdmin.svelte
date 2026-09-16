<script lang="ts">
  import type {
    RegionTreeResponse,
    RegionTreeState,
    RollupNodeType,
    RollupPolicy,
    StoredRollupPolicy,
    Threshold,
  } from '../lib/admin';
  import {
    AdminError,
    deleteRollupPolicy,
    fetchRegionTree,
    listRollupPolicies,
    upsertRollupPolicy,
  } from '../lib/admin';
  import {
    CHILD_NOUN,
    NODE_DEFAULTS,
    policyLabel,
    policyProblem,
    rollup,
    samePolicy,
    thresholdPhrase,
  } from '../lib/rollup';
  import NumberField from './NumberField.svelte';

  let { onexpired }: { onexpired: () => void } = $props();

  type ThresholdKind = 'any' | 'majority' | 'all' | 'quorum';
  type ThresholdDraft = { kind: ThresholdKind; quorum: number };
  type PolicyDraft = { degraded_when: ThresholdDraft; down_when: ThresholdDraft };

  const THRESHOLDS: ThresholdKind[] = ['any', 'majority', 'all', 'quorum'];
  const DEFAULTS = NODE_DEFAULTS;
  const CHILD = CHILD_NOUN;

  /** This node's state under one specific check, plus the child states that produced it. */
  type NodeCheck = {
    check_id: string;
    /** Mutable display name, for labels — never the identity. */
    name: string;
    state: RegionTreeState;
    children: RegionTreeState[];
    effective: RollupPolicy;
  };
  type AgentRow = { id: string; staleness_ms: number; perCheck: RegionTreeState[] };
  type PolicyNode = {
    node_id: string;
    node_type: RollupNodeType;
    label: string;
    perCheck: NodeCheck[];
    agents: AgentRow[];
    children: PolicyNode[];
    /** An override survives for a location no live check reaches. Keep it
     * visible, or an operator could not delete policy after moving the fleet. */
    stale: boolean;
  };
  type Row =
    | { kind: 'node'; key: string; depth: number; node: PolicyNode }
    | { kind: 'agent'; key: string; depth: number; agent: AgentRow };

  let tree = $state<RegionTreeResponse | null>(null);
  let policies = $state<StoredRollupPolicy[]>([]);
  let selectedId = $state<string | null>(null);
  let draft = $state<PolicyDraft>(toDraft(DEFAULTS.global));
  let collapsed = $state(new Set<string>());
  let loading = $state(true);
  let saving = $state(false);
  let expired = $state(false);
  let error = $state<string | null>(null);
  let note = $state<string | null>(null);

  // ---------------------------------------------------------------------------
  // Building one canonical tree.
  //
  // The region/zone skeleton comes from agent credentials, not from beliefs, so
  // it is the same for every check. Policy is likewise stored per node, not per
  // check. Rendering the skeleton once and hanging per-check STATE off each node
  // matches how the data is actually shaped; the old check-rooted tree repeated
  // the entire topology per check, implying a granularity that does not exist.
  // ---------------------------------------------------------------------------

  function blank(
    node_id: string,
    node_type: RollupNodeType,
    label: string,
  ): PolicyNode {
    return { node_id, node_type, label, perCheck: [], agents: [], children: [], stale: false };
  }

  function buildTree(source: RegionTreeResponse, stored: StoredRollupPolicy[]): PolicyNode {
    const root = blank('global', 'global', 'Global');
    const regions = new Map<string, PolicyNode>();

    for (const check of source.checks) {
      root.perCheck.push({
        check_id: check.check_id,
        name: check.name,
        state: check.state,
        children: check.regions.map((r) => r.state),
        effective: check.effective_policy,
      });

      for (const region of check.regions) {
        let rn = regions.get(region.region);
        if (!rn) {
          rn = blank(`region:${region.region}`, 'region', region.region);
          regions.set(region.region, rn);
        }
        rn.perCheck.push({
          check_id: check.check_id,
          name: check.name,
          state: region.state,
          children: region.zones.map((z) => z.state),
          effective: region.effective_policy,
        });

        for (const zone of region.zones) {
          const zoneId = `zone:${region.region}/${zone.zone}`;
          let zn = rn.children.find((z) => z.node_id === zoneId);
          if (!zn) {
            zn = blank(zoneId, 'zone', zone.zone);
            rn.children.push(zn);
          }
          zn.perCheck.push({
            check_id: check.check_id,
            name: check.name,
            state: zone.state,
            children: zone.agents.map((a) => a.state),
            effective: zone.effective_policy,
          });

          for (const agent of zone.agents) {
            let row = zn.agents.find((a) => a.id === agent.id);
            if (!row) {
              row = { id: agent.id, staleness_ms: agent.staleness_ms, perCheck: [] };
              zn.agents.push(row);
            }
            // The freshest contact is the honest "last heard from this agent".
            row.staleness_ms = Math.min(row.staleness_ms, agent.staleness_ms);
            row.perCheck.push(agent.state);
          }
        }
      }
    }

    // Overrides pointing at locations no live check reaches still need to be
    // inspectable and deletable, so graft them on as stale nodes.
    for (const policy of stored) {
      if (policy.node_type === 'region') {
        const name = policy.node_id.slice('region:'.length);
        if (!regions.has(name)) {
          const node = blank(policy.node_id, 'region', name);
          node.stale = true;
          regions.set(name, node);
        }
      } else if (policy.node_type === 'zone') {
        const [regionName, zoneName] = policy.node_id.slice('zone:'.length).split('/');
        let rn = regions.get(regionName);
        if (!rn) {
          rn = blank(`region:${regionName}`, 'region', regionName);
          rn.stale = true;
          regions.set(regionName, rn);
        }
        if (!rn.children.some((z) => z.node_id === policy.node_id)) {
          const node = blank(policy.node_id, 'zone', zoneName);
          node.stale = true;
          rn.children.push(node);
        }
      }
    }

    root.children = [...regions.values()].sort((a, b) => a.label.localeCompare(b.label));
    for (const region of root.children) {
      region.children.sort((a, b) => a.label.localeCompare(b.label));
      for (const zone of region.children) zone.agents.sort((a, b) => a.id.localeCompare(b.id));
    }
    return root;
  }

  function flatten(node: PolicyNode, depth: number, out: Row[]): Row[] {
    out.push({ kind: 'node', key: node.node_id, depth, node });
    if (!collapsed.has(node.node_id)) {
      for (const child of node.children) flatten(child, depth + 1, out);
      for (const agent of node.agents) {
        out.push({ kind: 'agent', key: `${node.node_id}#${agent.id}`, depth: depth + 1, agent });
      }
    }
    return out;
  }

  /** A plain loop rather than `.find`, because a compound predicate does not
   * narrow the row union and the result would fall back to `any`. */
  function findNode(list: Row[], id: string | null): PolicyNode | null {
    if (id === null) return null;
    for (const row of list) {
      if (row.kind === 'node' && row.node.node_id === id) return row.node;
    }
    return null;
  }

  let root = $derived(tree ? buildTree(tree, policies) : null);
  let rows = $derived(root ? flatten(root, 0, []) : []);
  let selected = $derived(findNode(rows, selectedId));
  let storedFor = $derived.by(() => {
    const node = selected;
    return node ? (policies.find((p) => p.node_id === node.node_id) ?? null) : null;
  });

  /** A node's own policy: its stored override, else the built-in default. */
  function nodePolicy(node: PolicyNode): RollupPolicy {
    return policies.find((p) => p.node_id === node.node_id)?.policy ?? DEFAULTS[node.node_type];
  }

  // ---------------------------------------------------------------------------
  // Draft plumbing
  // ---------------------------------------------------------------------------

  function thresholdDraft(value: Threshold): ThresholdDraft {
    return typeof value === 'string' ? { kind: value, quorum: 2 } : { kind: 'quorum', quorum: value.quorum };
  }

  function toDraft(policy: RollupPolicy): PolicyDraft {
    return {
      degraded_when: thresholdDraft(policy.degraded_when),
      down_when: thresholdDraft(policy.down_when),
    };
  }

  function thresholdValue(value: ThresholdDraft): Threshold {
    return value.kind === 'quorum' ? { quorum: value.quorum } : value.kind;
  }

  let draftPolicy = $derived<RollupPolicy>({
    degraded_when: thresholdValue(draft.degraded_when),
    down_when: thresholdValue(draft.down_when),
  });
  let problem = $derived(policyProblem(draftPolicy));
  let dirty = $derived.by(() => {
    const node = selected;
    if (!node) return false;
    return !samePolicy(draftPolicy, storedFor?.policy ?? DEFAULTS[node.node_type]);
  });

  function select(node: PolicyNode) {
    if (selectedId === node.node_id) {
      selectedId = null;
      return;
    }
    selectedId = node.node_id;
    draft = toDraft(nodePolicy(node));
    error = null;
    note = null;
  }

  function toggle(node_id: string) {
    const next = new Set(collapsed);
    if (next.has(node_id)) next.delete(node_id);
    else next.add(node_id);
    collapsed = next;
  }

  // ---------------------------------------------------------------------------
  // Prose. Thresholds render terse in the tree and are spelled out only here.
  // ---------------------------------------------------------------------------


  /** Checks whose own rollup override shadows this node, so a node-level edit
   * will not reach them. Detected by comparing effective policy to node policy. */
  let shadowed = $derived.by(() => {
    const node = selected;
    if (!node) return [] as { check_id: string; name: string }[];
    const own = nodePolicy(node);
    return node.perCheck
      .filter((c) => !samePolicy(c.effective, own))
      .map((c) => ({ check_id: c.check_id, name: c.name }));
  });
  let shadowedIds = $derived(new Set(shadowed.map((c) => c.check_id)));

  /** Per-check effect of the draft, so "what will this do" is answered on screen. */
  let preview = $derived.by(() => {
    const node = selected;
    if (!node) return [];
    return node.perCheck.map((c) => {
      const isShadowed = shadowedIds.has(c.check_id);
      const known = c.children.filter((s) => s !== 'unknown').length;
      const next = isShadowed ? c.state : rollup(draftPolicy, c.children);
      return {
        check_id: c.check_id,
        name: c.name,
        now: c.state,
        next,
        known,
        silent: c.children.length - known,
        shadowed: isShadowed,
        changed: !isShadowed && next !== c.state,
      };
    });
  });

  /** A quorum larger than the reporting fleet clamps, which is worth saying. */
  let clamping = $derived.by(() =>
    preview
      .filter((p) => !p.shadowed && p.known > 0)
      .flatMap((p) =>
        [draftPolicy.degraded_when, draftPolicy.down_when]
          .filter((t): t is { quorum: number } => typeof t !== 'string' && t.quorum > p.known)
          .map((t) => `${p.name}: quorum ${t.quorum} clamps to ${p.known}`),
      ),
  );

  function counts(states: RegionTreeState[]) {
    const c: Record<RegionTreeState, number> = { up: 0, degraded: 0, down: 0, unknown: 0 };
    for (const s of states) c[s]++;
    return (['down', 'degraded', 'up', 'unknown'] as RegionTreeState[])
      .filter((s) => c[s] > 0)
      .map((s) => ({ state: s, n: c[s] }));
  }

  function tone(state: RegionTreeState): string {
    return state === 'up' ? 'green' : state === 'degraded' ? 'amber' : state === 'down' ? 'red' : 'grey';
  }

  function staleness(ms: number): string {
    if (ms < 1000) return `${Math.max(0, ms)} ms`;
    if (ms < 60_000) return `${Math.floor(ms / 1000)} s`;
    if (ms < 3_600_000) return `${Math.floor(ms / 60_000)} min`;
    return `${Math.floor(ms / 3_600_000)} h`;
  }

  // ---------------------------------------------------------------------------
  // I/O
  // ---------------------------------------------------------------------------

  function fail(e: unknown): string {
    if (e instanceof AdminError && e.unauthorized) {
      expired = true;
      onexpired();
    }
    return e instanceof Error ? e.message : String(e);
  }

  async function refresh() {
    loading = true;
    error = null;
    try {
      const [nextTree, nextPolicies] = await Promise.all([fetchRegionTree(), listRollupPolicies()]);
      tree = nextTree;
      policies = nextPolicies;
    } catch (e) {
      error = fail(e);
    } finally {
      loading = false;
    }
  }

  async function save() {
    if (!selected || saving) return;
    if (problem) {
      error = problem;
      return;
    }
    saving = true;
    error = null;
    note = null;
    try {
      const saved = await upsertRollupPolicy({
        node_id: selected.node_id,
        node_type: selected.node_type,
        policy: draftPolicy,
      });
      policies = [...policies.filter((p) => p.node_id !== saved.node_id), saved];
      tree = await fetchRegionTree();
      note = 'Override saved.';
    } catch (e) {
      error = fail(e);
    } finally {
      saving = false;
    }
  }

  async function remove() {
    if (!selected || !storedFor || saving) return;
    const node = selected;
    saving = true;
    error = null;
    note = null;
    try {
      await deleteRollupPolicy(node.node_id, node.node_type);
      policies = policies.filter((p) => p.node_id !== node.node_id);
      draft = toDraft(DEFAULTS[node.node_type]);
      tree = await fetchRegionTree();
      note = 'Override deleted; this node is back on the built-in default.';
    } catch (e) {
      error = fail(e);
    } finally {
      saving = false;
    }
  }

  refresh();
</script>

{#if expired}
  <div class="admin-panel">
    <p class="admin-error">{error ?? 'Session expired.'}</p>
    <p class="admin-muted">Your session expired. Sign in again to view the region tree.</p>
    <button class="btn" onclick={onexpired}>Sign in again</button>
  </div>
{:else if loading}
  <div class="admin-panel"><p class="admin-muted">Loading region tree…</p></div>
{:else if error && !tree}
  <div class="admin-panel">
    <p class="admin-error">{error}</p>
    <button class="btn" onclick={refresh}>Retry</button>
  </div>
{:else if root}
  <div class="head">
    <div>
      <div class="panel-title">Rollup policy</div>
      <p class="admin-muted lede">
        Each node decides its own state from its children. Pick a node to change the thresholds
        that decision uses. Policy is set per node and applies to every check.
      </p>
    </div>
    <button class="btn" onclick={refresh}>Refresh</button>
  </div>

  <div class="admin-panel tree">
    <div class="legend">
      <span>Node</span>
      <span>Degraded / down</span>
      <span>State per check</span>
    </div>

    {#each rows as row (row.key)}
      {#if row.kind === 'agent'}
        <div class="row agent" style="--depth: {row.depth}">
          <span class="label">
            <span class="node-kind">agent</span>{row.agent.id}
          </span>
          <span class="policy">last seen {staleness(row.agent.staleness_ms)} ago</span>
          <span class="states">
            {#each counts(row.agent.perCheck) as c (c.state)}
              <span class="opill {tone(c.state)}"><span class="pdot"></span>{c.n} {c.state}</span>
            {/each}
          </span>
        </div>
      {:else}
        <div class="row-wrap">
          <div class="row node" class:selected={row.node.node_id === selectedId} style="--depth: {row.depth}">
            {#if row.node.children.length || row.node.agents.length}
              <button
                class="chevron"
                aria-expanded={!collapsed.has(row.node.node_id)}
                aria-label="{collapsed.has(row.node.node_id) ? 'Expand' : 'Collapse'} {row.node.label}"
                onclick={() => toggle(row.node.node_id)}>{collapsed.has(row.node.node_id) ? '▸' : '▾'}</button>
            {:else}
              <span class="chevron"></span>
            {/if}
            <button class="label pick" aria-pressed={row.node.node_id === selectedId} onclick={() => select(row.node)}>
              {#if row.node.node_type !== 'global'}<span class="node-kind">{row.node.node_type}</span>{/if}
              {row.node.label}
              {#if policies.some((p) => p.node_id === row.node.node_id)}
                <span class="badge override">override</span>
              {/if}
              {#if row.node.stale}<span class="badge stale">no agents</span>{/if}
            </button>
            <span class="policy">{policyLabel(nodePolicy(row.node))}</span>
            <span class="states">
              {#each counts(row.node.perCheck.map((c) => c.state)) as c (c.state)}
                <span class="opill {tone(c.state)}"><span class="pdot"></span>{c.n} {c.state}</span>
              {/each}
              {#if row.node.perCheck.length === 0}<span class="admin-muted">no checks</span>{/if}
            </span>
          </div>

          {#if selected && row.node.node_id === selectedId}
            <div class="editor" style="--depth: {row.depth}">
              <div class="editor-grid">
                <label class="field">
                  <span class="field-label">Degraded when</span>
                  <div class="threshold">
                    <select bind:value={draft.degraded_when.kind}>
                      {#each THRESHOLDS as kind (kind)}<option value={kind}>{kind}</option>{/each}
                    </select>
                    {#if draft.degraded_when.kind === 'quorum'}
                      <NumberField
                        min={1}
                        ariaLabel="Degraded quorum"
                        bind:value={
                          () => draft.degraded_when.quorum,
                          (v) => (draft.degraded_when.quorum = v ?? 1)
                        }
                      />
                    {/if}
                  </div>
                </label>
                <label class="field">
                  <span class="field-label">Down when</span>
                  <div class="threshold">
                    <select bind:value={draft.down_when.kind}>
                      {#each THRESHOLDS as kind (kind)}<option value={kind}>{kind}</option>{/each}
                    </select>
                    {#if draft.down_when.kind === 'quorum'}
                      <NumberField
                        min={1}
                        ariaLabel="Down quorum"
                        bind:value={
                          () => draft.down_when.quorum,
                          (v) => (draft.down_when.quorum = v ?? 1)
                        }
                      />
                    {/if}
                  </div>
                </label>
              </div>

              <ul class="prose">
                <li>
                  <strong>{selected.label}</strong> is
                  <span class="w red">down</span> when
                  {thresholdPhrase(draftPolicy.down_when, CHILD[selected.node_type])} is down.
                </li>
                <li>
                  Otherwise it is <span class="w amber">degraded</span> when
                  {thresholdPhrase(draftPolicy.degraded_when, CHILD[selected.node_type])} is down or
                  degraded.
                </li>
                <li>Otherwise it is <span class="w green">up</span>.</li>
                <li class="aside">
                  Down is tested first, so a node meeting both rules reads as down.
                  {CHILD[selected.node_type]}s reporting <em>unknown</em> are left out of the count
                  entirely — they neither cause an outage nor hide one. With none reporting, this
                  node is unknown.
                </li>
              </ul>

              {#if problem}<p class="admin-error feedback">{problem}</p>{/if}

              {#if clamping.length}
                <p class="admin-muted feedback">
                  Quorum exceeds the reporting fleet and will clamp — {clamping.join('; ')}.
                </p>
              {/if}

              {#if shadowed.length}
                <p class="admin-muted feedback">
                  Not applied to {shadowed.map((c) => c.name).join(', ')}: {shadowed.length === 1 ? 'this check sets' : 'these checks set'}
                  its own {selected.node_type} policy in the check editor, which wins over this one.
                </p>
              {/if}

              {#if preview.length}
                <div class="preview">
                  <div class="preview-head">Effect on each check</div>
                  {#each preview as p (p.check_id)}
                    <div class="preview-row" class:changed={p.changed}>
                      <span class="pcheck">{p.name}</span>
                      <span class="opill {tone(p.now)}"><span class="pdot"></span>{p.now}</span>
                      {#if p.changed}
                        <span class="arrow">→</span>
                        <span class="opill {tone(p.next)}"><span class="pdot"></span>{p.next}</span>
                      {:else}
                        <span class="arrow muted">{p.shadowed ? 'overridden' : 'unchanged'}</span>
                      {/if}
                      <span class="pcount">
                        {p.known} {p.known === 1 ? CHILD[selected.node_type] : `${CHILD[selected.node_type]}s`} reporting{p.silent ? `, ${p.silent} silent` : ''}
                      </span>
                    </div>
                  {/each}
                </div>
              {:else}
                <p class="admin-muted feedback">
                  No check currently reaches this node, so nothing uses this policy yet.
                </p>
              {/if}

              <div class="editor-foot">
                <p class="admin-muted">
                  {storedFor
                    ? 'Stored override.'
                    : `No override; using the built-in ${selected.node_type} default (${policyLabel(DEFAULTS[selected.node_type])}).`}
                </p>
                <div class="actions">
                  {#if storedFor}
                    <button class="btn danger" disabled={saving} onclick={remove}>Delete override</button>
                  {/if}
                  <button class="btn primary" disabled={saving || !!problem || !dirty} onclick={save}>
                    {saving ? 'Saving…' : storedFor ? 'Save override' : 'Create override'}
                  </button>
                </div>
              </div>

              {#if error}<p class="admin-error feedback">{error}</p>{/if}
              {#if note}<p class="saved-note feedback">{note}</p>{/if}
            </div>
          {/if}
        </div>
      {/if}
    {/each}
  </div>
{/if}

<style>
  .head { display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; margin: 0 0 .6rem; }
  .lede { max-width: 62ch; margin: .3rem 0 0; }
  .tree { padding: 0; overflow: hidden; }

  .legend, .row {
    display: grid;
    grid-template-columns: 1.1rem minmax(9rem, 1fr) minmax(9rem, auto) minmax(10rem, auto);
    align-items: center;
    gap: .65rem;
  }
  .legend {
    grid-template-columns: minmax(9rem, 1fr) minmax(9rem, auto) minmax(10rem, auto);
    padding: .5rem .8rem;
    border-bottom: 1px solid var(--ad-line);
    color: var(--ad-faint);
    font-size: .63rem;
    text-transform: uppercase;
    letter-spacing: .08em;
  }
  .row { padding: .3rem .8rem; min-height: 2.3rem; border-bottom: 1px solid rgba(255, 255, 255, .035); }
  /* Indent encodes depth; the tree is shallow enough that a rule per level is
     unnecessary and a custom property keeps the markup flat. */
  .row { padding-left: calc(.8rem + var(--depth) * 1.15rem); }
  .row.node.selected { background: var(--ad-panel-2); }
  .row.agent { grid-template-columns: 1.1rem minmax(9rem, 1fr) minmax(9rem, auto) minmax(10rem, auto); }
  .row.agent .label { grid-column: 2; }

  .chevron {
    border: 0; background: none; padding: 0; width: 1.1rem;
    color: var(--ad-dim); cursor: pointer; font-size: .7rem; line-height: 1;
  }
  .chevron:hover { color: var(--ad-cyan); }

  .label { min-width: 0; overflow-wrap: anywhere; font-weight: 500; text-align: left; }
  .pick {
    border: 0; background: none; padding: .2rem 0; color: inherit;
    font: inherit; font-weight: 500; cursor: pointer;
  }
  .pick:hover { color: var(--ad-cyan); }
  .node-kind {
    color: var(--ad-faint); font-size: .63rem; text-transform: uppercase;
    letter-spacing: .08em; margin-right: .4rem;
  }
  .badge {
    margin-left: .45rem; padding: .05rem .3rem; border-radius: 4px;
    font-size: .62rem; text-transform: uppercase; letter-spacing: .06em; vertical-align: middle;
  }
  .badge.override { background: rgba(86, 212, 228, .13); color: var(--ad-cyan); }
  .badge.stale { background: var(--ad-panel-2); color: var(--ad-faint); }

  .policy { color: var(--ad-dim); font: .72rem ui-monospace, SFMono-Regular, Menlo, monospace; white-space: nowrap; }
  .states { display: flex; flex-wrap: wrap; gap: .3rem; }

  .editor {
    padding: .85rem .8rem 1rem;
    padding-left: calc(.8rem + var(--depth) * 1.15rem + 1.1rem);
    background: #0c1013;
    border-bottom: 1px solid var(--ad-line);
    box-shadow: inset 3px 0 0 var(--ad-cyan);
  }
  .editor-grid { display: grid; grid-template-columns: repeat(2, minmax(9rem, 14rem)); gap: .75rem; align-items: end; }
  .threshold { display: flex; gap: .4rem; }
  .threshold :global(.num) { max-width: 5rem; }

  .prose { list-style: none; margin: .85rem 0 0; padding: 0; max-width: 68ch; }
  .prose li { color: var(--ad-dim); font-size: .8rem; line-height: 1.5; margin-bottom: .2rem; }
  .prose .aside { color: var(--ad-faint); font-size: .74rem; margin-top: .45rem; }
  .w { font-weight: 600; }
  .w.red { color: var(--ad-red); }
  .w.amber { color: var(--ad-amber); }
  .w.green { color: var(--ad-green); }

  .preview { margin-top: .85rem; border: 1px solid var(--ad-line); border-radius: 7px; overflow: hidden; }
  .preview-head {
    padding: .35rem .6rem; background: var(--ad-panel-2); color: var(--ad-faint);
    font-size: .63rem; text-transform: uppercase; letter-spacing: .08em;
  }
  .preview-row {
    display: flex; flex-wrap: wrap; align-items: center; gap: .5rem;
    padding: .35rem .6rem; border-top: 1px solid rgba(255, 255, 255, .035);
  }
  .preview-row.changed { background: rgba(210, 153, 34, .07); }
  .pcheck { min-width: 8rem; font-size: .78rem; }
  .arrow { color: var(--ad-dim); font-size: .78rem; }
  .arrow.muted { color: var(--ad-faint); font-size: .72rem; }
  .pcount { margin-left: auto; color: var(--ad-faint); font-size: .72rem; }

  .editor-foot { display: flex; justify-content: space-between; align-items: center; gap: 1rem; margin-top: .9rem; }
  .actions { display: flex; gap: .45rem; }
  .feedback { margin: .65rem 0 0; }
  .saved-note { color: var(--ad-green); font-size: .82rem; }

  @media (max-width: 760px) {
    .head { flex-direction: column; align-items: flex-start; }
    .legend { display: none; }
    .row, .row.agent { grid-template-columns: 1.1rem minmax(7rem, 1fr) auto; }
    .row .policy { grid-column: 2; }
    .row .states { grid-column: 2 / -1; }
    .editor-grid { grid-template-columns: 1fr; }
    .editor-foot { flex-direction: column; align-items: flex-start; }
    .pcount { margin-left: 0; width: 100%; }
  }
</style>
