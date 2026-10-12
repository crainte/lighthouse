import { request, send } from "./client";

/** Gossip failure-detector state for one coordinator, as another node currently
 *  believes it — the same shape for `self` and for every peer, so the panel
 *  needs no special case to show where it is standing. */
export type ClusterState = "alive" | "suspect" | "dead" | "removed";
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

export function reportedBuild(build?: string | null): string | null {
  const value = build?.trim();
  return !value || value.startsWith("origin ") ? null : value;
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

/** Permanently remove a peer from the cluster. It cannot rejoin under the
 *  same node id. */
export function decommissionMember(nodeId: string): Promise<ClusterMember> {
  return send("DELETE", `/api/v1/admin/cluster/members/${encodeURIComponent(nodeId)}`);
}

/** A region registered with the global node. `attached` turns true on its
 *  first authenticated push. The key itself is never returned again. */
export interface RegionRegistration {
  name: string;
  credential_id: string;
  created_at: number;
  endpoints: string[];
  attached: boolean;
  stale: boolean;
  age_ms: number;
  check_count: number;
}

export async function fetchRegions(): Promise<RegionRegistration[]> {
  return (await request<{ regions: RegionRegistration[] }>("/api/v1/admin/regions")).regions;
}

/** Register a region, or rotate its key. The token is shown exactly once. */
export function mintRegion(name: string): Promise<{ region: string; credential_id: string; token: string }> {
  return send("POST", "/api/v1/admin/regions", { name });
}

export function removeRegion(name: string): Promise<{ removed: string; checks_updated: number }> {
  return send("DELETE", `/api/v1/admin/regions/${encodeURIComponent(name)}`);
}
