import { send } from "./client";

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
