import type { CheckKind, ProbeStatus } from "../api";
import { request, send } from "./client";

/** Whether every assertion in a set must hold, or just one of them. Shared by
 *  multi-host checks and DNS expected values so "any/all" reads the same way
 *  wherever it appears. */
export type MatchMode = "any" | "all";

/** What a check reads as before it has ever reported. Without this a freshly
 *  created check would show as an outage until its first result lands. */
export type DefaultStatus = ProbeStatus;

/** Serde's externally tagged representation of the Rust Threshold enum. */
export type Threshold = "any" | "majority" | "all" | { quorum: number };

export interface RollupPolicy {
  degraded_when: Threshold;
  down_when: Threshold;
}

/** A missing level inherits its stored node policy, then the built-in default. */
export interface RollupOverrides {
  zone?: RollupPolicy;
  region?: RollupPolicy;
  global?: RollupPolicy;
}

export type RollupNodeType = "global" | "region" | "zone";

export interface StoredRollupPolicy {
  node_id: string;
  node_type: RollupNodeType;
  policy: RollupPolicy;
}

export type RegionTreeState = "up" | "degraded" | "down" | "unknown";

export interface RegionTreeAgent {
  id: string;
  state: RegionTreeState;
  staleness_ms: number;
}

export interface RegionTreeZone {
  zone: string;
  state: RegionTreeState;
  effective_policy: RollupPolicy;
  agents: RegionTreeAgent[];
}

export interface RegionTreeRegion {
  region: string;
  state: RegionTreeState;
  effective_policy: RollupPolicy;
  zones: RegionTreeZone[];
}

export interface RegionTreeCheck {
  check_id: string;
  /** Mutable display name, for labels — never the identity. */
  name: string;
  state: RegionTreeState;
  effective_policy: RollupPolicy;
  regions: RegionTreeRegion[];
}

export interface RegionTreeResponse {
  checks: RegionTreeCheck[];
}

export interface CheckHeader {
  name: string;
  value: string;
}

/** Where a check runs, described by location rather than by machine so that
 *  growing the fleet does not mean revisiting every check. Empty `zones` means
 *  every zone; empty `regions` means every region. Older checks use
 *  `per_zone`; `mode`/`count` select observers per zone or per region. */
export type PlacementMode = "zone" | "region";
export interface Placement {
  zones: string[];
  regions: string[];
  mode?: PlacementMode;
  count?: number;
  per_zone: number;
}

/** The admin wire type IS the backend's `CheckConfig`, verbatim. A parallel
 *  flattened DTO drifted from it every time a field was added, so there is now
 *  exactly one shape and no conversion. It carries the target, so it only ever
 *  arrives behind an authenticated session. */
export interface AdminCheck {
  /** Immutable identity, minted once at creation. Every route, lookup and
   *  relationship (results, incidents, dependencies, maintenance) keys on
   *  this — never on `name`. A create payload must never set this: the
   *  server always mints it (or, for a trusted regional fan-out, carries an
   *  existing one through) and ignores anything a browser sends. */
  id: string;
  /** Mutable display name. Freely renamable — doing so never changes `id`
   *  and never orphans history, alerts or incidents. */
  name: string;
  type: CheckKind;
  /** HTTP: a URL. TCP: "host:port". DNS/ICMP/SSL: a hostname. Heartbeat: unused. */
  target: string;
  /** Extra targets probed by the same check, alongside `target`. */
  hosts: string[];
  /** 'all' = every host must pass, 'any' = one passing is enough. */
  host_match: MatchMode;
  // Six fields, seconds first — e.g. "*/30 * * * * *". Five fields are
  // accepted and read as "at second zero".
  cron: string;
  timeout_secs: number;
  group?: string;
  latency_graph: boolean;
  expect: { status?: number; body_contains?: string };
  enabled: boolean;
  /** Publish on the public status page. Off = still probed, just not shown. */
  visible: boolean;
  /** Send this check's state changes to the notifiers. Off still records the
   *  alert and the incident — some checks are just checks. */
  notify: boolean;
  /** Notifiers by name. Empty routes to whichever are marked default, so a
   *  check needs no routing until two audiences exist. */
  notifiers: string[];
  /** Upstream checks. When this one confirms DOWN and an upstream already has
   *  an open incident, it joins that incident as a dependency instead of
   *  opening a second one for the same root cause. One hop, not transitive. */
  depends_on?: string[];
  /** Lua source deciding UP/DOWN. Absent/empty = built-in 2xx-3xx logic. */
  eval?: string;
  /** Display order within its section on the public page, lower first. The
   *  server owns it: assigned on create, rewritten by reorderChecks. */
  position: number;
  placement: Placement;
  /** Explicit pinning, and an override: when non-empty these agents probe the
   *  check and `placement` is ignored entirely. Empty is the common case and
   *  hands the decision to `placement`. */
  agents: string[];
  description?: string;
  /** Emoji or image URL shown beside the name. */
  image?: string;
  tag?: string;
  default_status: DefaultStatus;
  /** HTTP only. */
  headers: CheckHeader[];
  /** HTTP/SSL: accept an untrusted certificate. */
  allow_self_signed: boolean;
  dns: {
    nameserver?: string;
    /** A | AAAA | CNAME | MX | TXT | NS */
    record: string;
    match_type: MatchMode;
    expected: string[];
  };
  ping: { ip_type: string; count: number };
  /** OpenMetrics only: which sample to select from the scrape. `labels` narrow
   *  a metric family with multiple series (e.g. by `path` or `method`) down to
   *  the one this check evaluates. */
  metric: { name: string; labels: { name: string; value: string }[] };
  /** SSL: DOWN once the certificate is this close to expiry. */
  cert_warning_days: number;
  /** Heartbeat: no ping within this window means DOWN. */
  grace_secs: number;
  /** Consecutive DOWN probes that confirm an outage. 1 = flip on the first
   *  failure; 3 = tolerate two flaky probes. */
  failures_before_down: number;
  /** Seconds between confirmation attempts after a failure, before DOWN is
   *  confirmed. Omitted derives a sane value from the normal schedule.
   *  Heartbeat checks ignore this — nothing is retried between pings. */
  retry_interval_secs?: number;
  /** Seconds between probes while DOWN, until a success returns it to the
   *  normal cron schedule. Omitted derives a sane value from the normal
   *  schedule. Heartbeat checks ignore this. */
  down_interval_secs?: number;
  /** Per-level policy overrides; omitted levels inherit coordinator defaults. */
  rollup: RollupOverrides;
  /** Optional per-check overrides. Explicit zero/false differ from omission. */
  min_incident_secs?: number;
  min_recovery_secs?: number;
  notify_degraded?: boolean;
  degraded_is_downtime?: boolean;
}

/** What the editor sends. A create has no position yet — the server assigns the
 *  next free one — and an update echoes back whatever the server last gave us,
 *  so nobody downstream has to invent an ordering. `id` is never sent: a
 *  create always gets a server-minted one, and an update's identity comes
 *  from the path, not the body — a payload that generated or echoed one back
 *  would just be an id an untrusted client could try to move. */
export type CheckPayload = Omit<AdminCheck, "position" | "id"> & { position?: number };

export interface EvalSample {
  eval: string;
  status: number;
  latency: number;
  body: string;
  /** OpenMetrics only: the selected sample's value, for testing an eval script
   *  without a live scrape. */
  metric?: number;
}

export interface EvalResult {
  verdict: "up" | "down";
  latency_ms: number;
}

/** The whole catalogue, gathered a bounded page at a time.
 *
 *  Half the console treats this as a lookup table — dependency pickers,
 *  section assignment, the maintenance-window chooser — so the array is the
 *  contract and paging is an implementation detail. What is *not* negotiable
 *  is the size of any one request: the coordinator answers from a single
 *  database worker, and one request for ten thousand check documents makes
 *  every other request, including result ingest, wait behind it. */
export async function fetchAdminChecks(): Promise<AdminCheck[]> {
  const checks: AdminCheck[] = [];
  for (let page = 0; ; page += 1) {
    const r = await request<{ checks: AdminCheck[]; has_more: boolean }>(
      `/api/v1/admin/checks?page=${page}`,
    );
    checks.push(...r.checks);
    if (!r.has_more || r.checks.length === 0) return checks;
  }
}

export function fetchRegionTree(): Promise<RegionTreeResponse> {
  return request<RegionTreeResponse>("/api/v1/admin/region-tree");
}

export async function listRollupPolicies(): Promise<StoredRollupPolicy[]> {
  const r = await request<{ policies: StoredRollupPolicy[] }>(
    "/api/v1/admin/rollup-policies",
  );
  return r.policies;
}

export function upsertRollupPolicy(policy: StoredRollupPolicy): Promise<StoredRollupPolicy> {
  return send("PUT", "/api/v1/admin/rollup-policies", policy);
}

export async function deleteRollupPolicy(
  node_id: string,
  node_type: RollupNodeType,
): Promise<void> {
  await send("DELETE", "/api/v1/admin/rollup-policies", { node_id, node_type });
}

export async function createCheck(check: CheckPayload): Promise<AdminCheck> {
  const r = await send<{ check: AdminCheck }>(
    "POST",
    "/api/v1/admin/checks",
    check,
  );
  return r.check;
}

export async function updateCheck(
  id: string,
  check: CheckPayload,
): Promise<AdminCheck> {
  const r = await send<{ check: AdminCheck }>(
    "PUT",
    `/api/v1/admin/checks/${encodeURIComponent(id)}`,
    check,
  );
  return r.check;
}

export async function deleteCheck(id: string): Promise<void> {
  await send("DELETE", `/api/v1/admin/checks/${encodeURIComponent(id)}`);
}

/** Deletes the recorded history and KEEPS the check, which then reads as its
 *  default status until the next probe lands. Deliberately a separate call from
 *  deleteCheck: "I want a clean slate" and "this check should not exist" are
 *  different intentions with very different consequences. Returns rows removed. */
export async function purgeCheckData(id: string): Promise<number> {
  const r = await send<{ purged: number }>(
    "DELETE",
    `/api/v1/admin/checks/${encodeURIComponent(id)}/data`,
  );
  return r.purged;
}

/** Deletes the check definition AND every result, alert and incident it ever
 *  produced — the union of deleteCheck and purgeCheckData, plus the incidents
 *  neither of those touches. There is no partial form: once this runs the
 *  check has no trace left anywhere. Returns rows removed, by kind. */
export async function purgeCheckEverything(
  id: string,
): Promise<{ results: number; incidents: number }> {
  const r = await send<{ deleted: string; results: number; incidents: number }>(
    "DELETE",
    `/api/v1/admin/checks/${encodeURIComponent(id)}/everything`,
  );
  return { results: r.results, incidents: r.incidents };
}

/** Ids in display order — loose checks first, then each section's. Ids left
 *  out keep their relative order after the listed ones. */
export async function reorderChecks(order: string[]): Promise<AdminCheck[]> {
  const r = await send<{ checks: AdminCheck[] }>(
    "PUT",
    "/api/v1/admin/checks/order",
    { order },
  );
  return r.checks;
}

/** Dry-run a Lua snippet against a sample probe result. The guard against a
 *  typo silently marking a service down forever. */
export function testEval(sample: EvalSample): Promise<EvalResult> {
  return send<EvalResult>("POST", "/api/v1/admin/eval-test", sample);
}
