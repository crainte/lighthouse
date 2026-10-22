import { expect, test } from '@playwright/test';

const now = Date.now();
const checks = [
  { check_id: 'api', name: 'Public API', status: 'up', latency_ms: 42, last_check: now, uptime_24h: 99.99, type: 'http', position: 0 },
  { check_id: 'database', name: 'Database', status: 'up', latency_ms: 18, last_check: now, uptime_24h: 100, type: 'tcp', position: 1 },
];

async function mockStatusApi(page) {
  page.routeWebSocket('**/api/v1/ws', () => {});
  await page.route('**/api/v1/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    let payload = {};
    if (path === '/api/v1/status') {
      payload = { checks, page: 0, limit: 100, total_checks: checks.length, has_more: false, agents: [], maintenance: [], sections: [] };
    } else if (path === '/api/v1/sections') {
      payload = { sections: [] };
    } else if (path === '/api/v1/homepage/spacers') {
      payload = { spacers: [{ id: 'middle', after: 'check:api' }] };
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

test('renders the homepage spacer as a dark gap between detached service cards', async ({ page }) => {
  await mockStatusApi(page);
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Services' })).toBeVisible();

  const layout = await page.evaluate(() => {
    const list = document.querySelector('.public-services');
    const cards = [...list.querySelectorAll(':scope > .public-service-row')];
    const spacer = list.querySelector(':scope > .service-spacer');
    const listStyle = getComputedStyle(list);
    const cardStyle = getComputedStyle(cards[0]);
    const spacerStyle = getComputedStyle(spacer);
    const spacerRule = getComputedStyle(spacer, '::after');
    return {
      listBackground: listStyle.backgroundColor,
      listBorderWidth: listStyle.borderTopWidth,
      cardBackground: cardStyle.backgroundColor,
      pageBackground: getComputedStyle(document.querySelector('.public-app')).backgroundColor,
      cardBorderRadius: cardStyle.borderRadius,
      cardBorderWidth: cardStyle.borderTopWidth,
      spacerHeight: spacer.getBoundingClientRect().height,
      spacerPseudoContent: spacerRule.content,
      visibleGap: cards[1].getBoundingClientRect().top - cards[0].getBoundingClientRect().bottom,
    };
  });

  expect(layout.listBackground).toBe('rgba(0, 0, 0, 0)');
  expect(layout.listBorderWidth).toBe('0px');
  expect(layout.cardBackground).not.toBe('rgba(0, 0, 0, 0)');
  expect(layout.cardBackground).not.toBe(layout.pageBackground);
  expect(layout.cardBorderWidth).toBe('1px');
  expect(layout.cardBorderRadius).toBe('8px');
  expect(layout.spacerHeight).toBe(8);
  expect(layout.spacerPseudoContent).toBe('none');
  expect(layout.visibleGap).toBe(8);
});
