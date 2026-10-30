import type { Alert, Check, Incident, IncidentGroup } from '../lib/api';

export type OverviewTone = 'up' | 'degraded' | 'down' | 'unknown';

export function attentionChecks(checks: Check[]): Check[] {
  return checks.filter((check) => check.enabled !== false && check.status !== 'up');
}

export function observedLabel(totalChecks: number, shown: number, hasMore: boolean): string {
  if (hasMore) return `Observed subset: ${shown} of ${totalChecks} monitors`;
  return `${shown} monitored checks`;
}

export function activeIncidentCount(incidents: Incident[], groups: IncidentGroup[]): number {
  return incidentFeed(incidents, groups).length;
}

export function incidentFeed(incidents: Incident[], groups: IncidentGroup[]): Array<Incident | IncidentGroup> {
  const represented = new Set(groups.flatMap((group) => group.regions.map((region) => region.id)));
  return [
    ...groups.filter((group) => group.state !== 'resolved'),
    ...incidents.filter((incident) => incident.state !== 'resolved' && !represented.has(incident.id)),
  ].sort((a, b) => b.started_at - a.started_at);
}

export function alertLabel(alert: Alert): string {
  const name = alert.name ?? alert.node_id;
  switch (alert.alert_type) {
    case 'recovery':
      return `${name} recovered`;
    case 'degradation':
      return `${name} degraded`;
    default:
      if (alert.current_state === 'down') return `${name} went down`;
      if (alert.current_state === 'up') return `${name} recovered`;
      if (alert.current_state === 'degraded') return `${name} degraded`;
      return `${name} stopped reporting`;
  }
}

export function historyTone(status: Check['status']): OverviewTone {
  return status === 'up' || status === 'down' || status === 'degraded' ? status : 'unknown';
}

export function isUnauthorized(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false;
  if ('status' in error && (error as { status?: unknown }).status === 401) return true;
  return error instanceof Error && /-> 401\b/.test(error.message);
}
