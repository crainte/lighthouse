import { request, send } from "./client";

/** Deletes every result the agent ever reported, which is what removes it from
 *  the agent list — that list is derived from those rows, there is no registry.
 *  409 when checks still name it; the server's message lists them, so callers
 *  render it verbatim. Returns rows removed. */
export async function purgeAgent(id: string): Promise<number> {
  const r = await send<{ purged: number }>(
    "DELETE",
    `/api/v1/admin/agents/${encodeURIComponent(id)}`,
  );
  return r.purged;
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
