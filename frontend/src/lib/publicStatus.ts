import type { Check, Incident, SectionRollup } from './api';

export type PublicStatusTone = 'operational' | 'degraded' | 'outage' | 'incident' | 'unknown';
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
export function summarizePublicStatus(
  checks: Pick<Check, 'status' | 'enabled'>[],
  incidents: Incident[],
  sections: Array<Pick<SectionRollup, 'status'> & { paused?: number; checks?: number }> = [],
  waitingForRegions = false,
): PublicStatusSummary {
  // No region has published an observation. Even a healthy global control
  // plane cannot assert that monitored services are operational in this state.
  if (waitingForRegions) {
    return {
      tone: 'unknown',
      title: 'Waiting for regional reports',
      detail: 'The global control plane is online, but no regional coordinators are reporting yet. Service health is not available until a region connects.',
      incident: null,
    };
  }
  const active = incidents
    .filter((incident) => incident.state !== 'resolved')
    .sort((a, b) => b.started_at - a.started_at);
  const current = active[0] ?? null;
  // Public pagination can put entire sections behind a tile. Consult the
  // coordinator's published rollup rather than claiming all-clear from only
  // the visible page of monitors.
  const activeChecks = checks.filter((check) => check.enabled !== false);
  // A section whose members are all paused publishes no live health; its
  // Unknown rollup must not be read as a missing observation.
  const liveSections = sections.filter(
    (section) => !(section.paused && section.paused === (section.checks ?? 0) && (section.checks ?? 0) > 0),
  );
  const states = [...activeChecks.map((check) => check.status), ...liveSections.map((section) => section.status)];
  const sectionsDown = liveSections.some((section) => section.status === 'down');
  const sectionsDegraded = liveSections.some((section) => section.status === 'degraded');
  const sectionsUnknown = liveSections.some((section) => section.status === 'unknown');
  if (current) {
    return {
      tone: activeChecks.some((check) => check.status === 'down') || sectionsDown ? 'outage' : 'incident',
      title: current.title,
      detail: active.length === 1
        ? 'Active incident · See the latest published updates below.'
        : `${active.length} active incidents · See the latest published updates below.`,
      incident: current,
    };
  }
  if (states.length === 0) return {
    tone: 'unknown', title: 'No status data available',
    detail: 'No monitor observations are available on this page yet.', incident: null,
  };
  const down = activeChecks.filter((check) => check.status === 'down').length;
  if (down || sectionsDown) return {
    tone: 'outage',
    title: states.every((status) => status === 'down') ? 'Major service outage' : 'Partial service outage',
    detail: sectionsDown ? 'At least one service section is down. An incident update may not have been published yet.' : `${down} ${down === 1 ? 'monitor is' : 'monitors are'} down. An incident update may not have been published yet.`,
    incident: null,
  };
  const degraded = activeChecks.filter((check) => check.status === 'degraded').length;
  if (degraded || sectionsDegraded) return {
    tone: 'degraded', title: 'Degraded service performance',
    detail: sectionsDegraded ? 'At least one service section is degraded.' : `${degraded} ${degraded === 1 ? 'monitor is' : 'monitors are'} degraded.`, incident: null,
  };
  const unknown = activeChecks.filter((check) => check.status === 'unknown').length;
  if (unknown || sectionsUnknown) return {
    tone: 'unknown', title: 'Some service status is unknown',
    detail: sectionsUnknown ? 'At least one service section has unknown status. This is not automatically an outage.' : `${unknown} ${unknown === 1 ? 'monitor has' : 'monitors have'} no known observation. This is not automatically an outage.`,
    incident: null,
  };
  return {
    tone: 'operational', title: 'All systems operational',
    detail: 'No active incidents · All active monitors are reporting operational.', incident: null,
  };
}
