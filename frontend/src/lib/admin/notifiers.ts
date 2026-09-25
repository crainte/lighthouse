import { send } from "./client";

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
