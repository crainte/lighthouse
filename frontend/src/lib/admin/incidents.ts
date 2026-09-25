import { send } from "./client";

/** Deletes one incident outright — not a resolve, not an archive, just gone.
 *  A check still down after this simply opens a fresh incident on its next
 *  failed probe; this call has no opinion on the check itself. */
export async function deleteIncident(id: string): Promise<void> {
  await send("DELETE", `/api/v1/admin/incidents/${encodeURIComponent(id)}`);
}
