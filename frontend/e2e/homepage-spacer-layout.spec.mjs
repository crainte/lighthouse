import { expect, test } from '@playwright/test';

const now = Date.now();
const checks = [
  { check_id: 'api', name: 'Public API', status: 'up', latency_ms: 42, last_check: now, uptime_24h: 99.99, type: 'http', position: 0 },
  { check_id: 'database', name: 'Database', status: 'up', latency_ms: 18, last_check: now, uptime_24h: 100, type: 'tcp', position: 1 },
];

async function mockStatusApi(page, spacers = [{ id: 'middle', after: 'check:api' }], statusChecks = checks, sections = []) {
  page.routeWebSocket('**/api/v1/ws', () => {});
  await page.route('**/api/v1/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    let payload = {};
    if (path === '/api/v1/status') {
      payload = { checks: statusChecks, page: 0, limit: 100, total_checks: statusChecks.length, has_more: false, agents: [], maintenance: [], sections: sections.map((section) => ({ name: section.name, status: 'up', checks: 1, down: 0, degraded: 0 })) };
    } else if (path === '/api/v1/sections') {
      payload = { sections };
    } else if (path === '/api/v1/homepage/spacers') {
      payload = { spacers };
    } else if (path === '/api/v1/uptime') {
      payload = { days: 30, checks: [] };
    } else if (path === '/api/v1/latency-points') {
      payload = { from: now - 86_400_000, to: now, buckets: 120, checks: [] };
    } else if (path === '/api/v1/alerts') {
      payload = { alerts: [] };
    } else if (path === '/api/v1/incidents') {
      payload = { incidents: [], groups: [] };
    }
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(payload) });
  });
}

async function readServiceLayout(page) {
  return page.evaluate(() => {
    const list = document.querySelector('.public-services');
    const cards = [...list.querySelectorAll(':scope > .public-service-row')];
    const spacer = list.querySelector(':scope > .service-spacer');
    const first = getComputedStyle(cards[0]);
    const second = getComputedStyle(cards[1]);
    return {
      spacerCount: list.querySelectorAll(':scope > .service-spacer').length,
      gap: cards[1].getBoundingClientRect().top - cards[0].getBoundingClientRect().bottom,
      firstRadius: first.borderRadius,
      secondRadius: second.borderRadius,
      firstBottomRadius: first.borderBottomRightRadius,
      secondTopRadius: second.borderTopLeftRadius,
      firstBottomBorder: first.borderBottomWidth,
      secondTopBorder: second.borderTopWidth,
      spacerHeight: spacer ? spacer.getBoundingClientRect().height : 0,
      spacerBackground: spacer ? getComputedStyle(spacer).backgroundColor : null,
      cardsBackground: first.backgroundColor,
    };
  });
}

test('without spacers adjacent service rows form one flush panel', async ({ page }, testInfo) => {
  await mockStatusApi(page, []);
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Services' })).toBeVisible();

  const layout = await readServiceLayout(page);
  expect(layout.spacerCount).toBe(0);
  expect(layout.gap).toBe(0);
  expect(layout.firstBottomRadius).toBe('0px');
  expect(layout.secondTopRadius).toBe('0px');
  expect(layout.secondTopBorder).toBe('0px');
  expect(layout.firstBottomBorder).toBe('1px');
  await page.locator('.public-services').screenshot({ path: testInfo.outputPath('flush-services.png') });
});

test('only an explicit spacer creates a dark gap and rounded panel boundaries', async ({ page }, testInfo) => {
  await mockStatusApi(page);
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Services' })).toBeVisible();

  const layout = await readServiceLayout(page);
  expect(layout.spacerCount).toBe(1);
  expect(layout.spacerHeight).toBe(8);
  expect(layout.gap).toBe(8);
  expect(layout.firstBottomRadius).toBe('8px');
  expect(layout.secondTopRadius).toBe('8px');
  expect(layout.spacerBackground).toBe('rgba(0, 0, 0, 0)');
  expect(layout.secondTopBorder).toBe('1px');
  await page.locator('.public-services').screenshot({ path: testInfo.outputPath('spaced-services.png') });
});

test('a paged section and a loose check are flush until an explicit spacer separates them', async ({ page }) => {
  const sections = [{ name: 'lighthouse', position: 0, display: 'page', collapsed: false }];
  await mockStatusApi(page, [], [checks[0]], sections);
  await page.goto('/');
  const list = page.locator('.public-services');
  await expect(list.locator(':scope > .public-service-row')).toHaveCount(1);
  await expect(list.locator(':scope > .group')).toHaveCount(1);
  const noSpacer = await page.evaluate(() => {
    const list = document.querySelector('.public-services');
    const row = list.querySelector(':scope > .public-service-row');
    const tile = list.querySelector(':scope > .group');
    return {
      gap: tile.getBoundingClientRect().top - row.getBoundingClientRect().bottom,
      topBorder: getComputedStyle(tile).borderTopWidth,
      bottomBorder: getComputedStyle(tile).borderBottomWidth,
      radius: getComputedStyle(tile).borderBottomRightRadius,
    };
  });
  expect(noSpacer.gap).toBe(0);
  expect(noSpacer.topBorder).toBe('0px');
  expect(noSpacer.bottomBorder).toBe('1px');
  expect(noSpacer.radius).toBe('8px');
});
