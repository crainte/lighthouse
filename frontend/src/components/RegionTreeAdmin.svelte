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
  import { flattenTree, type Branch } from '../lib/admin/policyTree';

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
    /** `null` when the owning region did not answer the global node. */
    effective: RollupPolicy | null;
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
  type Item =
    | { kind: 'node'; key: string; node: PolicyNode }
    | { kind: 'agent'; key: string; agent: AgentRow };
  type Row = Item & Omit<Branch<Item>, 'item'>;

  let tree = $state<RegionTreeResponse | null>(null);
  let policies = $state<StoredRollupPolicy[]>([]);
  let selectedId = $state<string | null>(null);
  let previewQuery = $state('');
  let previewPage = $state(0);
  const PREVIEW_PAGE_SIZE = 25;
  const SIM_STATES: RegionTreeState[] = ['up', 'degraded', 'down', 'unknown'];
  const TIER_ORDER: RollupNodeType[] = ['zone', 'region', 'global'];
  let simTier = $state<RollupNodeType>('region');
  let simChildren = $state<RegionTreeState[]>(['up', 'up', 'degraded', 'up', 'down']);
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
        name: check.name ?? check.check_id,
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
          name: check.name ?? check.check_id,
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
            name: check.name ?? check.check_id,
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

  /** Child locations first, then the agents reporting into this one. */
  function childrenOf(item: Item): Item[] {
    if (item.kind === 'agent') return [];
    const node = item.node;
    return [
      ...node.children.map((child): Item => ({ kind: 'node', key: child.node_id, node: child })),
      ...node.agents.map(
        (agent): Item => ({ kind: 'agent', key: `${node.node_id}#${agent.id}`, agent }),
      ),
    ];
  }

  function flatten(node: PolicyNode): Row[] {
    return flattenTree<Item>(
      { kind: 'node', key: node.node_id, node },
      childrenOf,
      (item) => item.kind === 'node' && !collapsed.has(item.node.node_id),
    ).map(({ item, ...branch }) => ({ ...item, ...branch }));
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
  let rows = $derived(root ? flatten(root) : []);

  /** One indentation column: `pass` is `│`, `branch` is `├──`, `last` is
   *  `└──`, `blank` is empty — drawn in CSS so lines join across rows of any
   *  height. */
  type Seg = 'pass' | 'blank' | 'branch' | 'last';
  const guideSegs = (row: Row): Seg[] => row.guides.map((g) => (g ? 'pass' : 'blank'));

  function rowSegs(row: Row): Seg[] {
    return row.depth === 0 ? [] : [...guideSegs(row), row.last ? 'last' : 'branch'];
  }

  /** Under an open editor the lines keep running: the row's own, down to its
   *  next sibling, and its children's, down to the first child. */
  function editorSegs(row: Row): Seg[] {
    const own: Seg[] = row.depth === 0 ? [] : [...guideSegs(row), row.last ? 'blank' : 'pass'];
    return row.open ? [...own, 'pass'] : own;
  }
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
    previewQuery = '';
    previewPage = 0;
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
      .filter((c) => c.effective !== null && !samePolicy(c.effective, own))
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
      const unavailable = c.children.length === 0;
      const next = isShadowed || unavailable ? c.state : rollup(draftPolicy, c.children);
      return {
        check_id: c.check_id,
        name: c.name,
        now: c.state,
        next,
        known,
        unavailable,
        silent: c.children.length - known,
        shadowed: isShadowed,
        changed: !isShadowed && !unavailable && next !== c.state,
      };
    });
  });

  const previewMatches = $derived(preview.filter((p) =>
    `${p.name} ${p.check_id}`.toLowerCase().includes(previewQuery.trim().toLowerCase()),
  ));
  const previewPages = $derived(Math.max(1, Math.ceil(previewMatches.length / PREVIEW_PAGE_SIZE)));
  const currentPreviewPage = $derived(Math.min(previewPage, previewPages - 1));
  const previewRows = $derived(previewMatches.slice(currentPreviewPage * PREVIEW_PAGE_SIZE, (currentPreviewPage + 1) * PREVIEW_PAGE_SIZE));
  const changedCount = $derived(preview.filter((p) => p.changed).length);
  const unavailableCount = $derived(preview.filter((p) => p.unavailable).length);

  const tierNodes = $derived.by(() => {
    const out: Record<RollupNodeType, PolicyNode[]> = { global: [], region: [], zone: [] };
    for (const row of rows) if (row.kind === 'node') out[row.node.node_type].push(row.node);
    return out;
  });
  const agentCount = $derived.by(() => {
    const ids = new Set<string>();
    const walk = (node: PolicyNode) => { node.agents.forEach((a) => ids.add(a.id)); node.children.forEach(walk); };
    if (root) walk(root);
    return ids.size;
  });
  const detailMissing = $derived(tierNodes.region.length > 0 && tierNodes.zone.length === 0);
  const simPolicy = $derived(selected?.node_type === simTier ? draftPolicy : DEFAULTS[simTier]);
  const simResult = $derived(rollup(simPolicy, simChildren));
  const simKnown = $derived(simChildren.filter((s) => s !== 'unknown').length);

  function cycleSimChild(index: number) {
    simChildren = simChildren.map((state, i) => i === index ? SIM_STATES[(SIM_STATES.indexOf(state) + 1) % SIM_STATES.length] : state);
  }

  function resizeSim(delta: number) {
    simChildren = delta > 0 ? [...simChildren, 'up' as RegionTreeState].slice(0, 12) : simChildren.slice(0, Math.max(1, simChildren.length - 1));
  }

  function tierTitle(tier: RollupNodeType): string {
    return tier === 'global' ? 'Global check state' : tier === 'region' ? 'Region state' : 'Zone state';
  }

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
  {#if tree?.unavailable_regions?.length}<p class="admin-error">Region detail not read from {tree.unavailable_regions.join(', ')}: those regions did not answer, so their zones, agents and stored policies are missing.</p>{/if}
  <section class="rollup-pipeline" aria-labelledby="rollup-pipeline-title">
    <div class="section-heading"><div><h3 id="rollup-pipeline-title">Escalation path</h3><p>Every check moves through the same chain. Unknown children are excluded from each calculation; they neither create nor hide an outage.</p></div><button class="btn" onclick={refresh}>Refresh</button></div>
    <div class="pipeline-track">
      <div class="pipeline-stage source-stage"><span class="stage-index">01</span><strong>Agent observations</strong><p>Each assigned agent reports up, down, degraded, or unknown.</p><div class="stage-meter">{#if detailMissing}<span>—</span><small>agent detail not supplied</small>{:else}<span>{agentCount}</span><small>{agentCount === 1 ? 'agent' : 'agents'} in rollup data</small>{/if}</div></div>
      {#each TIER_ORDER as tier, i (tier)}
        <button class="pipeline-stage" class:active={simTier === tier} aria-pressed={simTier === tier} onclick={() => (simTier = tier)}>
          <span class="stage-index">0{i + 2}</span><strong>{tierTitle(tier)}</strong>
          <p>Down when {thresholdPhrase(DEFAULTS[tier].down_when, CHILD[tier])} is down.</p>
          <div class="stage-meter">{#if tier === 'zone' && detailMissing}<span>—</span><small>zone detail not supplied</small>{:else}<span>{tierNodes[tier].length}</span><small>{tier === 'global' ? 'aggregate' : tierNodes[tier].length === 1 ? tier : `${tier}s`} · {policies.filter((p) => p.node_type === tier).length} {policies.filter((p) => p.node_type === tier).length === 1 ? 'override' : 'overrides'}</small>{/if}</div>
        </button>
      {/each}
    </div>
  </section>

  <section class="rollup-lab" aria-labelledby="rollup-lab-title">
    <div class="lab-copy">
      <span class="lab-kicker">Interactive simulation</span>
      <h3 id="rollup-lab-title">{tierTitle(simTier)}</h3>
      <p>Select child states to test the {selected?.node_type === simTier ? `${selected.label} draft` : 'built-in default'} policy before saving an override.</p>
      <div class="lab-policy"><span>Degraded when</span><strong>{thresholdPhrase(simPolicy.degraded_when, CHILD[simTier])} is down or degraded</strong><span>Down when</span><strong>{thresholdPhrase(simPolicy.down_when, CHILD[simTier])} is down</strong></div>
    </div>
    <div class="lab-stage">
      <div class="lab-children" aria-label={`Simulated ${CHILD[simTier]} states`}>
        {#each simChildren as state, i (`${i}-${state}`)}
          <button class="sim-child {tone(state)}" aria-label={`${CHILD[simTier]} ${i + 1}: ${state}. Change state`} onclick={() => cycleSimChild(i)}><span></span>{state}</button>
        {/each}
      </div>
      <div class="lab-controls"><button class="btn" disabled={simChildren.length <= 1} onclick={() => resizeSim(-1)}>Remove child</button><span>{simKnown} known of {simChildren.length}</span><button class="btn" disabled={simChildren.length >= 12} onclick={() => resizeSim(1)}>Add child</button></div>
      <div class="lab-result {tone(simResult)}" aria-live="polite"><span>Resulting state</span><strong>{simResult}</strong></div>
    </div>
  </section>

  <div class="section-heading"><div><h3>Location policies</h3><p>Choose a location to adjust its thresholds. Overrides apply to every check rolled up through that location.</p></div></div>

  <div class="admin-panel tree">
    <div class="legend">
      <span>Node</span>
      <span>Degraded / down</span>
      <span>State per check</span>
    </div>

    {#snippet gutter(segs: Seg[], marker: 'leaf' | 'toggle' | null, row: Row | null)}
      <span class="tree-gutter" aria-hidden={marker !== 'toggle'}>
        {#each segs as seg, i (i)}<span class="tree-seg {seg}"></span>{/each}
        {#if marker === 'leaf'}
          <span class="tree-node leaf"><span class="tree-dot"></span></span>
        {:else if marker === 'toggle' && row?.kind === 'node'}
          {@const node = row.node}
          <span class="tree-node" class:open={row.open}>
            <button
              class="chevron"
              aria-expanded={row.open}
              aria-label="{row.open ? 'Collapse' : 'Expand'} {node.label}"
              onclick={() => toggle(node.node_id)}
              ><svg class:closed={!row.open} width="14" height="14" viewBox="0 0 24 24" aria-hidden="true"
                ><path d="m6 9 6 6 6-6" /></svg
              ></button
            >
          </span>
        {/if}
      </span>
    {/snippet}

    <div role="tree" aria-label="Location policies">
    {#each rows as row (row.key)}
      {#if row.kind === 'agent'}
        <div class="row agent" role="treeitem" aria-level={row.depth + 1} aria-selected="false">
          {@render gutter(rowSegs(row), 'leaf', row)}
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
          <div
            class="row node"
            class:selected={row.node.node_id === selectedId}
            role="treeitem"
            aria-level={row.depth + 1}
            aria-selected={row.node.node_id === selectedId}
            aria-expanded={row.hasChildren ? row.open : undefined}
          >
            {@render gutter(rowSegs(row), row.hasChildren ? 'toggle' : 'leaf', row)}
            <button class="label pick" aria-expanded={row.node.node_id === selectedId} onclick={() => select(row.node)}>
              {#if row.node.node_type !== 'global'}<span class="node-kind">{row.node.node_type}</span>{/if}
              {row.node.label}
              <span class="policy-affordance">{row.node.node_id === selectedId ? 'Close policy' : 'Edit policy'}<svg class:policy-open={row.node.node_id === selectedId} width="14" height="14" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 9 6 6 6-6" /></svg></span>
              {#if policies.some((p) => p.node_id === row.node.node_id)}
                <span class="tree-badge override">override</span>
              {/if}
              {#if row.node.stale}<span class="tree-badge stale">no agents</span>{/if}
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
            <div class="editor">
              {@render gutter(editorSegs(row), null, null)}
              <div class="editor-main">
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
                  {clamping.length} quorum thresholds exceed the available child-state count and will clamp.
                </p>
              {/if}

              {#if shadowed.length}
                <p class="admin-muted feedback">
                  {shadowed.length} checks report an effective policy different from this node’s policy.
                  Their current states are retained in this preview; the response does not identify the source of that difference.
                </p>
              {/if}

              {#if preview.length}
                <div class="preview">
                  <div class="preview-head">Policy impact · {preview.length} checks · {changedCount} projected changes</div>
                  {#if unavailableCount}<p class="admin-muted preview-note">Child details unavailable for {unavailableCount} checks. Current states are shown, but policy effects cannot be calculated for them. Missing details do not mean probes stopped reporting.</p>{/if}
                  <div class="preview-controls"><input type="search" aria-label="Search policy preview" placeholder="Find a check by name or ID" bind:value={previewQuery} oninput={() => (previewPage = 0)} /><span>{previewMatches.length} matches · up to {PREVIEW_PAGE_SIZE} per page</span></div>
                  {#each previewRows as p (p.check_id)}
                    <div class="preview-row" class:changed={p.changed}>
                      <span class="pcheck">{p.name}</span>
                      <span class="opill {tone(p.now)}"><span class="pdot"></span>{p.now}</span>
                      {#if p.changed}
                        <span class="arrow">→</span>
                        <span class="opill {tone(p.next)}"><span class="pdot"></span>{p.next}</span>
                      {:else}
                        <span class="arrow muted">{p.unavailable ? 'Preview unavailable' : p.shadowed ? 'Different effective policy' : 'unchanged'}</span>
                      {/if}
                      <span class="pcount">
                        {#if p.unavailable}Child details not supplied{:else}{p.known} {p.known === 1 ? CHILD[selected.node_type] : `${CHILD[selected.node_type]}s`} with known state{p.silent ? `, ${p.silent} unknown` : ''}{/if}
                      </span>
                    </div>
                  {/each}
                  <div class="preview-controls"><button class="btn" disabled={currentPreviewPage === 0} onclick={() => (previewPage = currentPreviewPage - 1)}>Previous</button><span>Page {currentPreviewPage + 1} of {previewPages}</span><button class="btn" disabled={currentPreviewPage + 1 >= previewPages} onclick={() => (previewPage = currentPreviewPage + 1)}>Next</button></div>
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
            </div>
          {/if}
        </div>
      {/if}
    {/each}
    </div>
  </div>
{/if}

<style>
  .rollup-pipeline { margin-bottom: 20px; }
  .pipeline-track { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 0; overflow: hidden; border: 1px solid var(--ad-line); border-radius: 9px; background: var(--ad-line); }
  .pipeline-stage { position: relative; display: grid; align-content: start; gap: 12px; min-width: 0; min-height: 210px; padding: 22px; color: var(--ad-text); text-align: left; background: var(--ad-panel); border: 0; border-left: 1px solid var(--ad-line); font: inherit; }
  .pipeline-stage:first-child { border-left: 0; }
  button.pipeline-stage { cursor: pointer; }
  button.pipeline-stage:hover, .pipeline-stage.active { background: #142538; }
  .pipeline-stage.active { box-shadow: inset 0 2px 0 var(--ad-cyan); }
  .pipeline-stage:not(:last-child)::after { content: ''; position: absolute; z-index: 1; top: 50%; right: -8px; width: 14px; height: 14px; background: inherit; border-top: 1px solid var(--ad-line); border-right: 1px solid var(--ad-line); transform: translateY(-50%) rotate(45deg); }
  .stage-index, .lab-kicker { color: var(--ad-cyan); font-size: 11px; font-weight: 500; }
  .pipeline-stage strong { font-size: 15px; font-weight: 550; }
  .pipeline-stage p { margin: 0; color: var(--ad-dim); font-size: 12px; line-height: 1.6; }
  .stage-meter { display: flex; align-items: baseline; gap: 8px; margin-top: auto; }
  .stage-meter span { font-size: 26px; }
  .stage-meter small { color: var(--ad-dim); font-size: 11px; }
  .rollup-lab { display: grid; grid-template-columns: minmax(240px, .75fr) minmax(0, 1.25fr); gap: 0; margin: 20px 0 32px; overflow: hidden; border: 1px solid var(--ad-line); border-radius: 9px; background: var(--ad-panel); }
  .lab-copy { padding: 28px; border-right: 1px solid var(--ad-line); }
  .lab-copy h3 { margin: 12px 0 10px; font-size: 22px; font-weight: 550; }
  .lab-copy > p { color: var(--ad-dim); font-size: 13px; line-height: 1.65; }
  .lab-policy { display: grid; gap: 8px; margin-top: 24px; padding-top: 20px; border-top: 1px solid var(--ad-line); }
  .lab-policy span { color: var(--ad-dim); font-size: 11px; }
  .lab-policy strong { margin-bottom: 10px; font-size: 13px; font-weight: 500; line-height: 1.5; }
  .lab-stage { display: grid; align-content: center; gap: 24px; padding: 32px; background: radial-gradient(circle at 70% 50%, rgba(86, 212, 228, .055), transparent 55%), #0b1520; }
  .lab-children { display: grid; grid-template-columns: repeat(auto-fill, minmax(104px, 1fr)); gap: 12px; }
  .sim-child { display: flex; align-items: center; gap: 10px; min-height: 48px; padding: 0 14px; color: var(--ad-text); text-transform: capitalize; background: var(--ad-panel); border: 1px solid var(--ad-line); border-radius: 7px; cursor: pointer; font: inherit; font-size: 13px; }
  .sim-child span { width: 9px; height: 9px; border-radius: 50%; background: currentColor; }
  .sim-child.green { color: var(--ad-green); } .sim-child.amber { color: var(--ad-amber); } .sim-child.red { color: var(--ad-red); } .sim-child.grey { color: var(--ad-dim); }
  .lab-controls { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px; color: var(--ad-dim); font-size: 12px; }
  .lab-result { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 22px 24px; border: 1px solid currentColor; border-radius: 8px; background: color-mix(in srgb, currentColor 8%, transparent); }
  .lab-result span { color: var(--ad-dim); font-size: 12px; }
  .lab-result strong { font-size: 30px; font-weight: 500; text-transform: capitalize; }
  .lab-result.green { color: var(--ad-green); } .lab-result.amber { color: var(--ad-amber); } .lab-result.red { color: var(--ad-red); } .lab-result.grey { color: var(--ad-dim); }
  @media (max-width: 1050px) { .pipeline-track { grid-template-columns: repeat(2, minmax(0, 1fr)); } .pipeline-stage:nth-child(3) { border-left: 0; } .pipeline-stage { border-bottom: 1px solid var(--ad-line); } .pipeline-stage::after { display: none; } .rollup-lab { grid-template-columns: 1fr; } .lab-copy { border-right: 0; border-bottom: 1px solid var(--ad-line); } }
  @media (max-width: 600px) { .pipeline-track { grid-template-columns: 1fr; } .pipeline-stage { min-height: 0; border-left: 0; } .lab-copy, .lab-stage { padding: 20px; } .lab-children { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  .head { display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; margin: 0 0 .6rem; }
  .lede { max-width: 62ch; margin: .3rem 0 0; }
  .tree { padding: 0; overflow: hidden; }

  .legend, .row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 11rem 16rem;
    align-items: center;
    column-gap: .65rem;
  }
  /* Each row is its own grid and the gutter's width follows depth, so the
     data columns are fixed: fractions would shift with every indent. */
  .row { grid-template-columns: auto minmax(0, 1fr) 11rem 16rem; }
  .legend {
    padding: 12px 16px;
    border-bottom: 1px solid var(--ad-line);
    color: var(--ad-dim);
    font-size: 11px;
    text-transform: none;
    letter-spacing: 0;
  }
  /* No vertical padding on the row itself: the gutter has to reach both
     edges so one row's lines meet the next row's. The cells pad instead. */
  .row { padding: 0 16px; min-height: 56px; border-bottom: 1px solid rgba(255, 255, 255, .035); }
  .row > :not(.tree-gutter) { padding-block: 12px; }
  .row.node.selected { background: var(--ad-panel-2); }

  /* tree(1) drawn in CSS. Every column carries its line at 10px, the centre
     of the 22px node marker, so a parent's toggle sits exactly above the line
     its children hang from. */
  /* `--knee`: how far down the row the branch meets its marker. */
  .tree-gutter { display: flex; align-self: stretch; --tree-line: #3a4f69; --knee: 50%; }
  .tree-seg, .tree-node { position: relative; flex: none; width: 22px; }
  .tree-seg { width: 28px; }
  .tree-seg.pass::before, .tree-seg.branch::before, .tree-seg.last::before, .tree-node.open::after {
    content: ''; position: absolute; left: 11px; top: 0; bottom: 0; border-left: 1px solid var(--tree-line);
  }
  .tree-seg.last::before { bottom: auto; height: var(--knee); }
  .tree-seg.branch::after, .tree-seg.last::after {
    content: ''; position: absolute; left: 11px; right: -1px; top: var(--knee); border-top: 1px solid var(--tree-line);
  }
  .tree-node { display: grid; justify-items: center; align-items: start; }
  .tree-node > * { margin-top: calc(var(--knee) - 11px); }
  .tree-node.leaf > * { margin-top: calc(var(--knee) - 3px); }
  .tree-node.open::after { top: calc(var(--knee) + 11px); }
  .tree-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--tree-line); box-shadow: 0 0 0 1px var(--ad-faint); }
  .row.agent .tree-dot { box-shadow: none; }

  .chevron {
    display: grid; place-items: center; border: 1px solid var(--ad-line); border-radius: 5px; background: var(--ad-panel-2); padding: 0; width: 22px; height: 22px;
    color: var(--ad-dim); cursor: pointer; font-size: .7rem; line-height: 1;
  }
  .chevron:hover { color: var(--ad-cyan); border-color: var(--ad-cyan); }
  .chevron svg, .policy-affordance svg { display: block; flex: none; fill: none; stroke: currentColor; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
  .row .label { padding-left: 6px; }
  .policy-affordance svg.policy-open { transform: rotate(180deg); }
  .chevron svg.closed { transform: rotate(-90deg); }
  .policy-affordance { display: flex; align-items: center; gap: 6px; margin-top: 6px; color: var(--ad-cyan); font-size: 11px; font-weight: 400; }

  .label { min-width: 0; overflow-wrap: anywhere; font-weight: 500; text-align: left; }
  .pick {
    border: 0; background: none; padding: .2rem 0; color: inherit;
    font: inherit; font-weight: 500; cursor: pointer;
  }
  .pick:hover { color: var(--ad-cyan); }
  .node-kind {
    color: var(--ad-dim); font-size: 11px; text-transform: capitalize;
    letter-spacing: 0; margin-right: 8px;
  }
  .tree-badge {
    margin-left: .45rem; padding: .05rem .3rem; border-radius: 4px;
    font-size: .62rem; text-transform: uppercase; letter-spacing: .06em; vertical-align: middle;
  }
  .tree-badge.override { background: rgba(86, 212, 228, .13); color: var(--ad-cyan); }
  .tree-badge.stale { background: var(--ad-panel-2); color: var(--ad-faint); }

  .policy { color: var(--ad-dim); font: .72rem ui-monospace, SFMono-Regular, Menlo, monospace; white-space: nowrap; }
  .states { display: flex; flex-wrap: wrap; gap: .3rem; }

  .editor {
    display: flex;
    padding: 0 16px;
    background: var(--ad-panel-2);
    border-bottom: 1px solid var(--ad-line);
    box-shadow: inset 3px 0 0 var(--ad-cyan);
  }
  .editor-main { flex: 1; min-width: 0; padding: 24px 24px 24px 12px; }
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
  .preview-controls { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; padding: 12px; font-size: 12px; color: var(--ad-dim); }
  .preview-controls input { flex: 1 1 220px; }
  .preview-note { padding: 0 12px; }
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
    /* The gutter spans every line of a stacked row, so the lines stay joined. */
    .row { grid-template-columns: auto minmax(0, 1fr); }
    /* A stacked row is tall; the branch meets the name, not the middle. */
    .row .tree-gutter { grid-row: 1 / span 3; --knee: 23px; }
    .row > :not(.tree-gutter) { padding-left: 6px; }
    .row > :not(.tree-gutter) { padding-block: 6px; }
    .row > :nth-child(2) { padding-top: 12px; }
    .row > :last-child { padding-bottom: 12px; }
    .editor-main { padding: 16px 8px; }
    .actions { flex-wrap: wrap; }
    .row .policy { grid-column: 2; }
    .row .states { grid-column: 2 / -1; }
    .editor-grid { grid-template-columns: 1fr; }
    .editor-foot { flex-direction: column; align-items: flex-start; }
    .pcount { margin-left: 0; width: 100%; }
  }
</style>
