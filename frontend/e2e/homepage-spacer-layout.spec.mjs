import { expect, test } from '@playwright/test';

const now = Date.now();
const checks = [
  { check_id: 'api', name: 'Public API', status: 'up', latency_ms: 42, last_check: now, uptime_24h: 99.99, type: 'http', position: 0 },
  { check_id: 'database', name: 'Database', status: 'up', latency_ms: 18, last_check: now, uptime_24h: 100, type: 'tcp', position: 1 },
];

async function mockStatusApi(page, spacers = [{ id: 'middle', after: 'check:api' }]) {
  page.routeWebSocket('**/api/v1/ws', () => {});
  await page.route('**/api/v1/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    let payload = {};
    if (path === '/api/v1/status') {
      payload = { checks, page: 0, limit: 100, total_checks: checks.length, has_more: false, agents: [], maintenance: [], sections: [] };
    } else if (path === '/api/v1/sections') {
      payload = { sections: [] };
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

test('without spacers adjacent service rows form one flush panel', async ({ page }) => {
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
});

test('only an explicit spacer creates a dark gap and rounded panel boundaries', async ({ page }) => {
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
});
