// Authenticated admin API. Kept apart from api.ts because every call here
// carries the session cookie and, on failure, the server sends a human-readable
// `{error}` string that the form has to render verbatim.

import type { CheckKind, ProbeStatus } from "./api";
export type { ProbeStatus } from "./api";

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

/** A named, ordered group on the status page. Exists independently of checks,
 *  so an empty section can be created before anything is put in it. */
export interface Section {
  name: string;
  position: number;
  /** The PUBLIC status page renders this section collapsed on first load. The
   *  admin view ignores it. */
  collapsed: boolean;
}

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

/** Carries the HTTP status so callers can tell "your input was wrong" (400/409)
 *  from "you are no longer logged in" (401), which needs a different recovery. */
export class AdminError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "AdminError";
    this.status = status;
  }

  get unauthorized(): boolean {
    return this.status === 401;
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    let message = res.statusText || `HTTP ${res.status}`;
    try {
      const body: unknown = await res.json();
      const err = (body as { error?: unknown } | null)?.error;
      if (typeof err === "string" && err) message = err;
    } catch {
      // Non-JSON body (proxy error page, empty 502). Status text is all we have.
    }
    throw new AdminError(message, res.status);
  }
  return res.json() as Promise<T>;
}

function send<T>(method: string, url: string, body?: unknown): Promise<T> {
  return request<T>(url, {
    method,
    headers:
      body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

/** Never 401s — safe to call on mount to decide whether to offer the admin UI. */
/** Which kind of node the SPA is talking to. A global node aggregates regions
 *  and has no fleet credentials of its own, so the UI adapts by node mode. */
export type NodeMode = "coordinator" | "global";

export interface SessionInfo {
  authenticated: boolean;
  mode: NodeMode;
}

export async function fetchSession(): Promise<SessionInfo> {
  const r = await request<{ authenticated: boolean; mode?: NodeMode }>(
    "/api/v1/session",
  );
  return { authenticated: r.authenticated, mode: r.mode ?? "coordinator" };
}

export async function login(password: string): Promise<void> {
  await send("POST", "/api/v1/login", { password });
}

export async function logout(): Promise<void> {
  await send("POST", "/api/v1/logout");
}

export async function fetchAdminChecks(): Promise<AdminCheck[]> {
  const r = await request<{ checks: AdminCheck[] }>("/api/v1/admin/checks");
  return r.checks;
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

/** Deletes every result the agent ever reported, which is what removes it from
 *  the agent list — that list is derived from those rows, there is no registry.
 *  409 when checks still name it; the server's message lists them, so callers
 *  render it verbatim. Returns rows removed. */
/** Deletes one incident outright — not a resolve, not an archive, just gone.
 *  A check still down after this simply opens a fresh incident on its next
 *  failed probe; this call has no opinion on the check itself. */
export async function deleteIncident(id: string): Promise<void> {
  await send("DELETE", `/api/v1/admin/incidents/${encodeURIComponent(id)}`);
}

/** The unfiltered raw-result total, used to name the exact destructive purge. */
export async function fetchResultsCount(): Promise<number> {
  const r = await request<{ raw_results: number }>("/api/v1/admin/results/count");
  return r.raw_results;
}

/** Deletes every raw probe result. The coordinator's published status remains. */
export async function purgeResults(): Promise<number> {
  const r = await send<{ deleted: number }>("POST", "/api/v1/admin/results/purge");
  return r.deleted;
}

/** Removes resolved incidents no longer than the supplied duration, for cleanup
 *  of brief false alarms. This intentionally has no results counterpart: the
 *  storage helper exists, but the server does not expose an admin route for it. */
export async function purgeShortIncidents(maxSecs: number): Promise<number> {
  const r = await send<{ deleted: number }>(
    "POST",
    "/api/v1/admin/incidents/purge-short",
    { max_secs: maxSecs },
  );
  return r.deleted;
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

export async function purgeAgent(id: string): Promise<number> {
  const r = await send<{ purged: number }>(
    "DELETE",
    `/api/v1/admin/agents/${encodeURIComponent(id)}`,
  );
  return r.purged;
}

export async function fetchSections(): Promise<Section[]> {
  const r = await request<{ sections: Section[] }>("/api/v1/admin/sections");
  return r.sections;
}

export async function createSection(name: string): Promise<Section> {
  const r = await send<{ section: Section }>("POST", "/api/v1/admin/sections", {
    name,
  });
  return r.section;
}

/** Rename and/or set the collapsed flag. A rename moves every check in the
 *  section with it — callers do not patch checks. Omitting `collapsed` leaves
 *  it as it is, so this one call serves both the rename and the toggle. */
export async function updateSection(
  current: string,
  next: { name?: string; collapsed?: boolean },
): Promise<Section> {
  const r = await send<{ section: Section }>(
    "PUT",
    `/api/v1/admin/sections/${encodeURIComponent(current)}`,
    { name: next.name ?? current, collapsed: next.collapsed },
  );
  return r.section;
}

/** Its checks survive and become loose. */
export async function deleteSection(name: string): Promise<void> {
  await send("DELETE", `/api/v1/admin/sections/${encodeURIComponent(name)}`);
}

export async function reorderSections(order: string[]): Promise<Section[]> {
  const r = await send<{ sections: Section[] }>(
    "PUT",
    "/api/v1/admin/sections",
    { order },
  );
  return r.sections;
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

/** One raw probe result as the admin explorer reads it. Carries the VERBATIM
 *  `error` the agent wrote — resolved addresses and expected values and all —
 *  because it only ever arrives behind an admin session. The public per-check
 *  history endpoint summarises this away; here it is the whole point. */
export interface ProbeResult {
  check_id: string;
  /** Mutable display name, for labels — never the identity. */
  name: string;
  agent_id: string;
  /** Epoch millis. */
  timestamp: number;
  zone?: string;
  region?: string;
  status: ProbeStatus;
  latency_ms: number;
  status_code?: number;
  /** Verbatim probe error. Absent on success. */
  error?: string;
  /** The failure as a category, for a coloured pill. Absent on success. */
  failure?: string;
}

/** Server-side narrowing for the explorer. `query` (free-text error search) is
 *  deliberately absent — it runs client-side on the returned batch. `limit`
 *  caps the batch; the server clamps it to 1000. */
export interface ProbeResultsFilter {
  /** A check id — the server rejects anything that does not parse as one. */
  check?: string;
  agent?: string;
  status?: ProbeStatus;
  region?: string;
  zone?: string;
  limit?: number;
}

/** Recent raw results, newest first, narrowed server-side. A 401 throws
 *  `AdminError` like every other admin call, so the panel drives the same
 *  re-login flow. */
export async function fetchProbeResults(
  filter: ProbeResultsFilter,
): Promise<ProbeResult[]> {
  const params = new URLSearchParams();
  // Skip empty fields so an untouched filter is an absent param, not `?check=`.
  if (filter.check) params.set("check", filter.check);
  if (filter.agent) params.set("agent", filter.agent);
  if (filter.status) params.set("status", filter.status);
  if (filter.region) params.set("region", filter.region);
  if (filter.zone) params.set("zone", filter.zone);
  if (filter.limit != null) params.set("limit", String(filter.limit));
  const qs = params.toString();
  const r = await request<{ results: ProbeResult[] }>(
    `/api/v1/admin/results${qs ? `?${qs}` : ""}`,
  );
  return r.results;
}

/** A per-zone agent credential. The secret itself is never stored here — only
 *  its hash server-side — so the list is safe to render. Revoked ones stay in
 *  the list for audit rather than vanishing. */
export interface AgentCredential {
  id: string;
  zone: string;
  region: string;
  created_at: number;
  revoked: boolean;
}

export async function fetchAgentCredentials(): Promise<AgentCredential[]> {
  const r = await request<{ credentials: AgentCredential[] }>(
    "/api/v1/admin/agent-credentials",
  );
  return r.credentials;
}

/** The only moment `token` exists in cleartext anywhere: the server hashes it
 *  and cannot hand it back, so the caller must show it before dropping it. */
export async function createAgentCredential(
  zone: string,
  region: string,
): Promise<{ credential: AgentCredential; token: string }> {
  return send<{ credential: AgentCredential; token: string }>(
    "POST",
    "/api/v1/admin/agent-credentials",
    { zone, region },
  );
}

/** Permanent and immediate — the credential stays listed as revoked. */
export async function revokeAgentCredential(id: string): Promise<void> {
  await send(
    "DELETE",
    `/api/v1/admin/agent-credentials/${encodeURIComponent(id)}`,
  );
}

export type AgentsMode = "regions" | "flat" | "off";

/** One row an operator placed on the public page: a single agent, or a whole
 *  region aggregated. A region row counts every agent in it, including one
 *  also shown individually above. */
export type FleetEntry =
  { kind: "agent"; name: string } | { kind: "region"; name: string };

export interface AgentsDisplay {
  /** Explicit rows in display order. */
  rows: FleetEntry[];
  /** What happens to agents no explicit row covers. `off` with no rows hides
   *  the section entirely. */
  mode: AgentsMode;
  /** Percent of a region's agents that must be healthy for a green row. */
  warn_below: number;
  /** Below this percent the region row reads down. */
  down_below: number;
}

/** An agent the picker can offer: everyone the coordinator knows about, live
 *  pollers and past reporters alike. */
export interface FleetMember {
  agent_id: string;
  region: string;
  healthy: boolean;
}

/** Settings and the fleet they choose from arrive together — a picker without
 *  the fleet is not useful, and two calls would be two failure modes. */
export interface AgentsDisplaySettings {
  agents: AgentsDisplay;
  fleet: FleetMember[];
}

export function fetchAgentsDisplay(): Promise<AgentsDisplaySettings> {
  return send<AgentsDisplaySettings>("GET", "/api/v1/admin/status-page/agents");
}

/** Echoes back what was stored, so the form renders the server's version of
 *  the settings rather than its own optimistic one. */
export function saveAgentsDisplay(
  agents: AgentsDisplay,
): Promise<AgentsDisplaySettings> {
  return send<AgentsDisplaySettings>(
    "PUT",
    "/api/v1/admin/status-page/agents",
    agents,
  );
}

/** A notification backend. Flat on the wire, tagged by `type`, matching the
 *  Rust struct so the form and the dispatcher cannot disagree about a shape. */
export type Notifier = {
  /** The routing key a check names, so it must be unique. */
  name: string;
  /** Receives alerts from every check that names no notifier at all. */
  default: boolean;
} & (
  | { type: "webhook"; url: string }
  | { type: "ntfy"; topic: string; base_url?: string | null }
  /** Slack wants `text` and rejects anything else, which is why it is not the
   *  generic webhook pointed at a Slack URL. */
  | { type: "slack"; webhook_url: string }
  /** Events API v2 integration key. The check id is the dedup key server-side,
   *  so a recovery resolves the incident the outage opened. */
  | { type: "pagerduty"; routing_key: string }
  | {
      type: "email";
      host: string;
      /** Absent means the standard port for the TLS mode. */
      port?: number | null;
      username?: string | null;
      password?: string | null;
      from: string;
      to: string;
      /** Absent means `starttls`, never plaintext. */
      tls?: "starttls" | "implicit" | "none";
    }
);

/** A notification that has not been delivered: still being retried, or given up
 *  on. A row exists only because a send failed — success deletes it. */
export interface Undelivered {
  notifier: string;
  check_id: string;
  /** Mutable display name, for labels — never the identity. */
  name: string;
  /** The state that was being announced. */
  to: string;
  attempts: number;
  /** Attempts exhausted: this transition reached nobody. */
  given_up: boolean;
  next_attempt?: number;
  error: string;
  queued_at: number;
}

export interface NotifiersResponse {
  notifiers: Notifier[];
  /** Checks routed to a notifier that no longer exists — usually the aftermath
   *  of a rename. They page nobody, and nothing else would say so. */
  unrouted_checks: string[];
  undelivered: Undelivered[];
}

export function fetchNotifiers(): Promise<NotifiersResponse> {
  return send<NotifiersResponse>("GET", "/api/v1/admin/notifiers");
}

/** Replaces the whole list: it is small and always edited as a whole, so the
 *  name is the identity rather than a separate id. */
export function saveNotifiers(
  notifiers: Notifier[],
): Promise<NotifiersResponse> {
  return send<NotifiersResponse>("PUT", "/api/v1/admin/notifiers", {
    notifiers,
  });
}

/** Send one notification now, to a notifier that need not be saved yet. A
 *  refused delivery is a successful request carrying `delivered: false` — the
 *  form has to tell "your input was wrong" from "the endpoint said no". */
export function testNotifier(
  notifier: Notifier,
): Promise<{ delivered: boolean; error?: string }> {
  return send("POST", "/api/v1/admin/notifiers/test", notifier);
}

/** A scheduled span during which the named checks page nobody. Probing,
 *  results, alerts and incidents all continue: it is the per-check `notify`
 *  switch on a timer, not a pause. */
export interface MaintenanceWindow {
  title: string;
  /** Epoch millis, inclusive. */
  starts_at: number;
  /** Epoch millis, exclusive. */
  ends_at: number;
  /** Check ids it covers. Empty means every check. Ids, not names, so a
   *  rename never silently drops a check out of a scheduled window. */
  checks: string[];
  /** Show it on the public status page while it runs. */
  announce: boolean;
}

/** A check the maintenance picker can offer: the id it sends, and the name it
 *  shows. */
export interface MaintenanceCheckOption {
  id: string;
  name: string;
}

export interface MaintenanceResponse {
  windows: MaintenanceWindow[];
  /** Every configured check, so the picker needs no second request. */
  checks: MaintenanceCheckOption[];
}

export function fetchMaintenance(): Promise<MaintenanceResponse> {
  return send<MaintenanceResponse>("GET", "/api/v1/admin/maintenance");
}

/** Replaces the whole list, like the notifiers: a window has no identity
 *  beyond its span, so there is nothing to address individually. */
export function saveMaintenance(
  windows: MaintenanceWindow[],
): Promise<MaintenanceResponse> {
  return send<MaintenanceResponse>("PUT", "/api/v1/admin/maintenance", {
    windows,
  });
}

/** Gossip failure-detector state for one coordinator, as another node currently
 *  believes it — the same shape for `self` and for every peer, so the panel
 *  needs no special case to show where it is standing. */
export type ClusterState = "alive" | "suspect" | "dead";
export type ClusterMode = "coordinator" | "global";
export type ClusterLinkOutcome = "connected" | "failed";

export interface ClusterLink {
  source: string;
  target: string;
  source_generation: number;
  observed_at: number;
  outcome: ClusterLinkOutcome;
}

export function buildIdentity(build: string): string {
  const match = /^(.*) \((.*), built .*\)$/.exec(build);
  return match ? `${match[1]} (${match[2]})` : build;
}

export interface ClusterMember {
  node_id: string;
  url: string;
  /** Bumped on every restart, so a stale message from a previous life of this
   *  node loses to a fresher one carrying the same node_id. */
  generation: number;
  /** Epoch millis, as the responding node's own clock saw it — pair with
   *  `ClusterResponse.now`, not the browser's clock, to read it as an age. */
  last_seen: number;
  state: ClusterState;
  /** What that node reported it is running. Absent for a peer on a build that
   *  predates reporting one, or for an entry restored from disk that no ping
   *  has refreshed yet — builds are deliberately not persisted, since a stored
   *  one describes a process that may since have been replaced. */
  build?: string | null;
  /** The peer wire protocol that node speaks. Absent from a peer predating the
   *  field. Nothing is gated on it; a mismatch is a diagnosis, not a refusal. */
  protocol?: number | null;
}

/** A standalone coordinator answers with `members: []` rather than failing the
 *  call, so the panel can tell "no cluster configured" from "cluster fetch
 *  failed" without a second endpoint. */
export interface ClusterResponse {
  self: ClusterMember;
  members: ClusterMember[];
  /** Epoch millis on the responding node. */
  now: number;
  mode?: ClusterMode;
  links?: ClusterLink[];
  link_stale_after_ms?: number;
}

/** Lives outside `/admin/*`: cluster membership is coordinator state, not
 *  status-page configuration, but it carries the same session and 401s the
 *  same way. */
export function fetchCluster(): Promise<ClusterResponse> {
  return request<ClusterResponse>("/api/v1/cluster");
}
