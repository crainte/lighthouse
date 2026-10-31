import { expect, test } from '@playwright/test';

const checks = [
  { id: 'wan-primary', name: 'UniFi WAN primary', tag: null, type: 'openmetrics',
    enabled: false, visible: true, cron: '0 * * * * *' },
  { id: 'bifrost', name: 'bifrost - 2', tag: null, type: 'dns',
    enabled: true, visible: true, cron: '0 * * * * *' },
];

async function mockAdmin(page) {
  page.routeWebSocket('**/api/v1/ws', () => {});
  await page.route('**/api/v1/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    const payload = {
      '/api/v1/session': { authenticated: true, mode: 'coordinator' },
      '/api/v1/admin/checks': { checks, has_more: false },
      '/api/v1/admin/sections': { sections: [] },
      '/api/v1/admin/homepage/spacers': { spacers: [] },
      '/api/v1/homepage/spacers': { spacers: [] },
      '/api/v1/status': { checks: [], page: 0, limit: 100, total_checks: 0, has_more: false, agents: [], maintenance: [], sections: [] },
      '/api/v1/sections': { sections: [] },
      '/api/v1/incidents': { incidents: [], groups: [] },
      '/api/v1/alerts': { alerts: [] },
      '/api/v1/uptime': { days: 30, checks: [] },
      '/api/v1/latency-points': { checks: [] },
    }[path];
    await route.fulfill({ status: payload ? 200 : 501, contentType: 'application/json', body: JSON.stringify(payload ?? { error: `Unhandled mock route: ${path}` }) });
  });
}

for (const width of [1440, 980, 390]) {
  test(`check columns fit OpenMetrics and give cron only its needed space at ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    await mockAdmin(page);
    await page.goto('/#/admin/checks');
    const rows = page.locator('.admin-panel .admin-rows');
    const row = rows.locator('.row:not(.head)').first();
    await expect(row).toBeVisible();
    const cells = row.locator(':scope > span');
    const badge = await cells.nth(2).locator('.kind-pill').boundingBox();
    const state = await cells.nth(3).boundingBox();
    const cron = await cells.nth(5).boundingBox();
    expect(badge && state && cron).toBeTruthy();
    expect(state.x - (badge.x + badge.width)).toBeGreaterThanOrEqual(8);
    expect(cron.width).toBeLessThanOrEqual(180);
    const headerState = await rows.locator('.row.head > span').nth(3).boundingBox();
    expect(Math.abs(headerState.x - state.x)).toBeLessThan(1);
    if (width < 880) {
      const overflow = await rows.evaluate((element) => ({
        scrollWidth: element.scrollWidth,
        clientWidth: element.clientWidth,
      }));
      expect(overflow.scrollWidth).toBeGreaterThan(overflow.clientWidth);
      const scrolled = await rows.evaluate((element) => {
        element.scrollLeft = element.scrollWidth;
        return element.scrollLeft;
      });
      expect(scrolled).toBeGreaterThan(0);
      await expect(row.getByRole('button', { name: 'Configure' })).toBeVisible();
    }
    await rows.screenshot({ path: testInfo.outputPath(`checks-table-${width}.png`) });
  });
}
