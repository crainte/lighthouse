import { expect, test } from '@playwright/test';
import { readFileSync } from 'node:fs';

// Playwright normally launches Chromium with --hide-scrollbars, which would
// erase the very affordance this visual test needs to inspect.
test.use({ launchOptions: {
  args: ['--no-sandbox'], ignoreDefaultArgs: ['--hide-scrollbars'],
  ...(process.env.LH_CHROMIUM_EXECUTABLE_PATH ? { executablePath: process.env.LH_CHROMIUM_EXECUTABLE_PATH } : {}),
} });

const checks = Array.from({ length: 30 }, (_, i) => ({
  id: `check-${i}`, name: `Service ${String(i + 1).padStart(2, '0')}`, position: i,
  enabled: true, visible: true, type: 'http', target: `https://service-${i}.example.test`,
  cron: '0 * * * * *', timeout_secs: 10, latency_graph: false, expect: {},
  notify: false, notifiers: [], agents: [], hosts: [], host_match: 'all',
  placement: { zones: [], regions: [], per_zone: 1 }, default_status: 'unknown',
  headers: [], allow_self_signed: false,
  dns: { mode: 'standard', record: 'A', match_type: 'all', expected: [] },
  ping: { ip_type: 'any', count: 1 },
}));

async function mockAdmin(page) {
  page.routeWebSocket('**/api/v1/ws', () => {});
  await page.route('**/api/v1/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    const payload = {
      '/api/v1/session': { authenticated: true, mode: 'coordinator' },
      '/api/v1/admin/checks': { checks, has_more: false },
      '/api/v1/admin/agent-credentials': { credentials: [{ id: 'credential-1', region: 'local', zone: 'test', created_at: Date.now(), revoked: false }] },
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

test('Firefox keeps standard colors when it also recognizes the WebKit selector', () => {
  const css = readFileSync(new URL('../src/app.css', import.meta.url), 'utf8');
  // Firefox 153–155 recognizes selector(::-webkit-scrollbar) but does not paint it.
  expect(css).toMatch(/@supports\s+selector\(::-webkit-scrollbar\)\s+and\s+\(not\s+\(-moz-appearance:\s*none\)\)/);
});

test('Agent credentials retain the original admin-rows rail', async ({ page }) => {
  await mockAdmin(page);
  await page.goto('/#/admin/fleet/access');
  const list = page.locator('.access-panel .admin-rows');
  await expect(list).toBeVisible();
  const styles = await list.evaluate((element) => ({
    padding: getComputedStyle(element).paddingRight,
    gutter: getComputedStyle(element).scrollbarGutter,
    width: getComputedStyle(element).scrollbarWidth,
    color: getComputedStyle(element).scrollbarColor,
  }));
  expect(styles.padding).toBe('0px');
  expect(styles.gutter).toBe('auto');
  expect(styles.width).toBe('thin');
  expect(styles.color).not.toBe('auto');
});

for (const [section, route, panelName] of [
  ['Checks', '/#/admin/checks', 'Checks'],
  ['Check order', '/#/admin/status', 'Check order'],
]) {
  test(`${section} has a visible inset scroll rail with space from its rows`, async ({ page }, testInfo) => {
    await mockAdmin(page);
    await page.goto(route);
    const list = section === 'Checks'
      ? page.locator('.admin-panel .admin-rows').first()
      : page.locator('.admin-panel').filter({ hasText: panelName }).locator('.admin-rows').first();
    await expect(list).toBeVisible();
    const styles = await list.evaluate((element) => {
      const css = getComputedStyle(element);
      const track = getComputedStyle(element, '::-webkit-scrollbar-track');
      const thumb = getComputedStyle(element, '::-webkit-scrollbar-thumb');
      const scrollbar = getComputedStyle(element, '::-webkit-scrollbar');
      return {
        canScroll: element.scrollHeight > element.clientHeight,
        rightPadding: parseFloat(css.paddingRight),
        gutter: css.scrollbarGutter,
        standardColors: css.scrollbarColor,
        width: parseFloat(scrollbar.width),
        trackColor: track.backgroundColor,
        thumbColor: thumb.backgroundColor,
      };
    });
    expect(styles.canScroll).toBe(true);
    expect(styles.rightPadding).toBeGreaterThanOrEqual(12);
    expect(styles.gutter).toBe('stable');
    expect(styles.standardColors).toBe('auto');
    expect(styles.width).toBeGreaterThanOrEqual(10);
    expect(styles.width).toBeLessThanOrEqual(14);
    expect(styles.trackColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(styles.thumbColor).not.toBe(styles.trackColor);
    await list.screenshot({ path: testInfo.outputPath(`scroll-rail-${section.toLowerCase().replaceAll(' ', '-')}.png`) });
    await list.hover();
    await page.mouse.wheel(0, 270);
    await expect.poll(() => list.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
  });
}
