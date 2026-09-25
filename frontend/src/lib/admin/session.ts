import { request, send } from "./client";

/** Which kind of node the SPA is talking to. A global node aggregates regions
 *  and has no fleet credentials of its own, so the UI adapts by node mode. */
export type NodeMode = "coordinator" | "global";

export interface SessionInfo {
  authenticated: boolean;
  mode: NodeMode;
}

/** Never 401s — safe to call on mount to decide whether to offer the admin UI. */
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
