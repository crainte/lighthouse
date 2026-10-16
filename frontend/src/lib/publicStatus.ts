import type { Check, Incident } from './api';

export type PublicStatusTone = 'operational' | 'degraded' | 'outage' | 'unknown';
export interface PublicStatusSummary {
  tone: PublicStatusTone;
  title: string;
  detail: string;
  incident: Incident | null;
}

/** A published incident takes precedence over a healthy check list.
 * Unknown observations never become an invented "all clear".
 * The caller must supply its actual public incident list and check page.
 */
export function summarizePublicStatus(checks: Pick<Check, 'status'>[], incidents: Incident[]): PublicStatusSummary {
  const active = incidents
    .filter((incident) => incident.state !== 'resolved')
    .sort((a, b) => b.started_at - a.started_at);
  const current = active[0] ?? null;
  if (current) {
    return {
      tone: checks.some((check) => check.status === 'down') ? 'outage' : 'degraded',
      title: current.title,
      detail: active.length === 1
        ? 'Active incident · See the latest published updates below.'
        : `${active.length} active incidents · See the latest published updates below.`,
      incident: current,
    };
  }
  if (checks.length === 0) return {
    tone: 'unknown', title: 'No status data available',
    detail: 'No monitor observations are available on this page yet.', incident: null,
  };
  const down = checks.filter((check) => check.status === 'down').length;
  if (down) return {
    tone: 'outage',
    title: down === checks.length ? 'Major service outage' : 'Partial service outage',
    detail: `${down} ${down === 1 ? 'monitor is' : 'monitors are'} down. An incident update may not have been published yet.`,
    incident: null,
  };
  const degraded = checks.filter((check) => check.status === 'degraded').length;
  if (degraded) return {
    tone: 'degraded', title: 'Degraded service performance',
    detail: `${degraded} ${degraded === 1 ? 'monitor is' : 'monitors are'} degraded.`, incident: null,
  };
  const unknown = checks.filter((check) => check.status === 'unknown').length;
  if (unknown) return {
    tone: 'unknown', title: 'Some service status is unknown',
    detail: `${unknown} ${unknown === 1 ? 'monitor has' : 'monitors have'} no known observation. This is not automatically an outage.`,
    incident: null,
  };
  return {
    tone: 'operational', title: 'All systems operational',
    detail: 'No active incidents · All listed monitors are reporting operational.', incident: null,
  };
}
