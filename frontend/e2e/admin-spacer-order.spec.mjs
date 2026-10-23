import { expect, test } from '@playwright/test';

const check = (id, name, position, group) => ({
  id, name, position, group, visible: true, enabled: true, type: 'http',
  target: `https://${id}.example.test`, hosts: [], host_match: 'all',
  cron: '0 * * * * *', timeout_secs: 10, latency_graph: false,
  expect: {}, notify: false, notifiers: [], placement: { zones: [], regions: [], per_zone: 1 },
  agents: [], default_status: 'unknown', headers: [], allow_self_signed: false,
  dns: { mode: 'standard', record: 'A', match_type: 'all', expected: [] },
  ping: { ip_type: 'any', count: 1 },
});

async function mockAdmin(page) {
  let spacers = [];
  const writes = [];
  const checks = [check('api', 'Public API', 0), check('db', 'Database', 1), check('worker', 'Worker', 2, 'Core')];
  const sections = [{ name: 'Core', position: 0, display: 'page', collapsed: false }];
  page.routeWebSocket('**/api/v1/ws', () => {});
  await page.route('**/api/v1/**', async (route) => {
    const { pathname } = new URL(route.request().url());
    const method = route.request().method();
    let payload = {};
    if (pathname === '/api/v1/session') payload = { authenticated: true, mode: 'coordinator' };
    else if (pathname === '/api/v1/admin/sections') payload = { sections };
    else if (pathname === '/api/v1/admin/checks') payload = { checks, has_more: false };
    else if (pathname === '/api/v1/sections') payload = { sections };
    else if (pathname === '/api/v1/incidents') payload = { incidents: [], groups: [] };
    else if (pathname === '/api/v1/alerts') payload = { alerts: [] };
    else if (pathname === '/api/v1/uptime') payload = { days: 30, checks: [] };
    else if (pathname === '/api/v1/latency-points') payload = { checks: [] };
    else if (pathname === '/api/v1/homepage/spacers') payload = { spacers };
    else if (pathname === '/api/v1/admin/homepage/spacers' && method === 'PUT') {
      const body = route.request().postDataJSON();
      writes.push(body);
      spacers = body.spacers;
      payload = { spacers };
    } else if (pathname === '/api/v1/status') {
      payload = { checks: [], page: 0, limit: 100, total_checks: 0, has_more: false, agents: [], maintenance: [], sections: [] };
    }
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(payload) });
  });
  return { writes, getSpacers: () => spacers };
}

test('add, drag after a check, move with arrows, reload, and remove a spacer inline', async ({ page }) => {
  const state = await mockAdmin(page);
  await page.goto('/#/admin/status');
  const panel = page.locator('.admin-panel').filter({ hasText: 'Check order' });
  await expect(panel.getByRole('button', { name: '+ Add spacer' })).toBeVisible();
  await expect(page.getByText('Homepage spacers')).toHaveCount(0);
  await panel.getByRole('button', { name: '+ Add spacer' }).click();
  await expect.poll(() => state.getSpacers().length).toBe(1);
  await expect(panel.locator('.spacer-row')).toHaveCount(1);
  expect(state.getSpacers()[0].after).toBe('section:Core');

  const grip = panel.locator('.spacer-row .grip');
  const target = panel.locator('.orow').filter({ has: page.locator('.oname', { hasText: 'Public API' }) });
  await grip.dragTo(target);
  await expect.poll(() => state.getSpacers()[0].after).toBe('check:api');
  await expect(target.locator('xpath=following-sibling::*[1]')).toHaveClass(/spacer-row/);

  await panel.getByRole('button', { name: 'Move spacer down' }).click();
  await expect.poll(() => state.getSpacers()[0].after).toBe('check:db');
  await page.reload();
  await expect(panel.locator('.spacer-row')).toHaveCount(1);
  await expect(panel.locator('.orow').filter({ has: page.locator('.oname', { hasText: 'Database' }) }).locator('xpath=following-sibling::*[1]')).toHaveClass(/spacer-row/);
  await panel.getByRole('button', { name: 'Remove spacer' }).click();
  await expect.poll(() => state.getSpacers().length).toBe(0);
  expect(state.writes).toHaveLength(4);
  expect(state.writes.every((write) => Object.keys(write).join(',') === 'spacers')).toBe(true);
});

test('dragging a spacer onto a section changes only its layout anchor', async ({ page }) => {
  const state = await mockAdmin(page);
  await page.goto('/#/admin/status');
  const panel = page.locator('.admin-panel').filter({ hasText: 'Check order' });
  await panel.getByRole('button', { name: '+ Add spacer' }).click();
  await expect(panel.locator('.spacer-row')).toHaveCount(1);
  await panel.locator('.spacer-row .grip').dragTo(panel.locator('.orow').filter({ has: page.locator('.oname', { hasText: 'Public API' }) }));
  await expect.poll(() => state.getSpacers()[0].after).toBe('check:api');
  await panel.locator('.spacer-row .grip').dragTo(panel.locator('.olabel', { hasText: 'Core' }));
  await expect.poll(() => state.getSpacers()[0].after).toBe('section:Core');
  await expect(panel.locator('.ogroup').filter({ hasText: 'Core' }).locator('.spacer-row')).toHaveCount(1);
  expect(state.writes.every(({ spacers }) => spacers.length === 1)).toBe(true);
});
