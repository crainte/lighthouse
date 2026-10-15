import { describe, expect, it } from 'vitest';

import {
  hashForAdmin,
  hashForFleet,
  hashForCheck,
  hashForCheckEditor,
  hashForGroup,
  hashForHome,
  hashForIncident,
  hashForIncidentEditor,
  viewFromHash,
} from './router';

const PANELS = ['checks', 'alerts', 'incidents'] as const;

describe('viewFromHash', () => {
  it('defaults to home', () => {
    expect(viewFromHash('', PANELS, 'checks')).toEqual({ name: 'home' });
    expect(viewFromHash('#/', PANELS, 'checks')).toEqual({ name: 'home' });
  });

  it('parses a check permalink', () => {
    expect(viewFromHash('#/check/abc%2Fdef', PANELS, 'checks')).toEqual({
      name: 'detail',
      checkId: 'abc/def',
    });
  });

  it('parses an incident permalink', () => {
    expect(viewFromHash('#/incident/inc-1', PANELS, 'checks')).toEqual({
      name: 'incident',
      incidentId: 'inc-1',
    });
  });

  it('parses a section page', () => {
    expect(viewFromHash('#/group/Backend%20Services', PANELS, 'checks')).toEqual({
      name: 'group',
      sectionName: 'Backend Services',
    });
  });

  it('falls back to the default panel for an unknown or absent admin id', () => {
    expect(viewFromHash('#/admin', PANELS, 'checks')).toEqual({ name: 'admin', panel: 'checks' });
    expect(viewFromHash('#/admin/nonsense', PANELS, 'checks')).toEqual({
      name: 'admin',
      panel: 'checks',
    });
    expect(viewFromHash('#/admin/alerts', PANELS, 'checks')).toEqual({
      name: 'admin',
      panel: 'alerts',
    });
  });

  it('keeps Fleet tabs addressable and old admin bookmarks compatible', () => {
    const fleetPanels = ['overview', 'fleet', 'checks'] as const;
    for (const tab of ['overview', 'agents', 'regions', 'access'] as const) {
      expect(viewFromHash(hashForFleet(tab), fleetPanels, 'overview')).toEqual({
        name: 'admin', panel: 'fleet', fleetTab: tab,
      });
    }
    expect(viewFromHash('#/admin/fleet', fleetPanels, 'overview')).toEqual({
      name: 'admin', panel: 'fleet', fleetTab: 'overview',
    });
    expect(viewFromHash('#/admin/fleet/credentials', fleetPanels, 'overview')).toEqual({
      name: 'admin', panel: 'fleet', fleetTab: 'access',
    });
    expect(viewFromHash('#/admin/fleet/registration', fleetPanels, 'overview')).toEqual({
      name: 'admin', panel: 'fleet', fleetTab: 'access',
    });
    expect(viewFromHash('#/admin/cluster', fleetPanels, 'overview')).toEqual({
      name: 'admin', panel: 'fleet', fleetTab: 'overview',
    });
    expect(viewFromHash('#/admin/agents', fleetPanels, 'overview')).toEqual({
      name: 'admin', panel: 'fleet', fleetTab: 'agents',
    });
    expect(viewFromHash('#/admin/credentials', fleetPanels, 'overview')).toEqual({
      name: 'admin', panel: 'fleet', fleetTab: 'access',
    });
  });

  it('parses the check editor, with "new" reserved for create', () => {
    expect(viewFromHash('#/admin/checks/new', PANELS, 'checks')).toEqual({
      name: 'admin',
      panel: 'checks',
      edit: { id: null },
    });
    expect(viewFromHash('#/admin/checks/check-1', PANELS, 'checks')).toEqual({
      name: 'admin',
      panel: 'checks',
      edit: { id: 'check-1' },
    });
  });

  it('parses the incident editor, with "new" reserved for create', () => {
    expect(viewFromHash('#/admin/incidents/new', PANELS, 'checks')).toEqual({
      name: 'admin',
      panel: 'incidents',
      incident: { id: null },
    });
    expect(viewFromHash('#/admin/incidents/inc-1', PANELS, 'checks')).toEqual({
      name: 'admin',
      panel: 'incidents',
      incident: { id: 'inc-1' },
    });
  });
});

describe('hashFor*', () => {
  it('builds every route, encoding the dynamic segment', () => {
    expect(hashForHome()).toBe('#/');
    expect(hashForCheck('a/b')).toBe('#/check/a%2Fb');
    expect(hashForIncident('inc 1')).toBe('#/incident/inc%201');
    expect(hashForGroup('Backend Services')).toBe('#/group/Backend%20Services');
    expect(hashForAdmin('alerts')).toBe('#/admin/alerts');
  });

  it('reserves "new" for a null id', () => {
    expect(hashForCheckEditor(null)).toBe('#/admin/checks/new');
    expect(hashForCheckEditor('check-1')).toBe('#/admin/checks/check-1');
    expect(hashForIncidentEditor(null)).toBe('#/admin/incidents/new');
    expect(hashForIncidentEditor('inc-1')).toBe('#/admin/incidents/inc-1');
  });
});
