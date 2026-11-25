import { readFileSync } from 'node:fs';
import { expect, test } from '@playwright/test';

declare global {
  interface Window {
    __csp?: string[];
  }
}

const token = 'a'.repeat(64);
const fixture = (name: string) => JSON.parse(readFileSync(new URL('../fixtures/' + name, import.meta.url), 'utf8'));
const failedRun: string = fixture('run-failed.json').id;
const policy: { page: string; placeholder: string } = fixture('csp.json');

// The page under its policy: the map, a push drawer, a popover, the transcript and an overlay flyout each render
// without a securitypolicyviolation and without a request off the origin. The four style injectors each show up
// here: a drawer and the colour-mode class through EUI's caches, the popover's focus trap through
// react-remove-scroll-bar, the overlay's mask through the package-default @emotion/css.
test('the page renders every surface under its Content-Security-Policy', async ({ page, baseURL }) => {
  const offOrigin: string[] = [];
  const errors: string[] = [];
  page.on('request', (r) => {
    if (!r.url().startsWith(baseURL!)) {
      offOrigin.push(r.url());
    }
  });
  page.on('console', (m) => {
    if (m.type() === 'error') {
      errors.push(m.text());
    }
  });
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('response', (r) => {
    if (r.status() >= 400) {
      errors.push(r.status() + ' ' + r.url());
    }
  });
  await page.addInitScript(() => {
    window.__csp = [];
    document.addEventListener('securitypolicyviolation', (e) => {
      window.__csp!.push(e.violatedDirective + ' ' + e.blockedURI + ' ' + e.sourceFile + ':' + e.lineNumber);
    });
  });

  const response = await page.goto('/#t=' + token);
  // The policy served is the Go server's, from its own constants through the fixture.
  expect(response!.headers()['content-security-policy']).toMatch(new RegExp('^' + policy.page.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(policy.placeholder, '[A-Za-z0-9+/=]{24}') + '$'));
  await expect(page).toHaveURL(baseURL + '/');
  await expect(page.getByRole('button', { name: 'Receiver nomad-1' })).toBeVisible();
  await expect(page.getByText('Live · model v7')).toBeVisible();

  // A push drawer, with the map scaled to the space left: no sideways scroll at 1280 px.
  await page.getByRole('button', { name: 'Receiver nomad-1' }).click();
  await expect(page.getByRole('heading', { name: 'Receiver nomad-1' })).toBeVisible();
  await expect(page.getByText('gated: draining').first()).toBeVisible();
  const noSidewaysScroll = () => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth);
  expect(await noSidewaysScroll()).toBe(true);
  await page.setViewportSize({ width: 1024, height: 800 });
  await expect(page.getByRole('button', { name: 'Receiver nomad-1' })).toBeVisible();
  expect(await noSidewaysScroll()).toBe(true);
  await page.setViewportSize({ width: 1280, height: 900 });

  // A popover with a focus trap.
  await page.getByRole('button', { name: 'Model' }).click();
  await expect(page.getByText('Recent errors')).toBeVisible();
  await page.keyboard.press('Escape');

  // The run list, a run and its transcript.
  await page.getByRole('button', { name: /^Runs \(/ }).click();
  await expect(page.getByRole('heading', { name: 'Runs' })).toBeVisible();
  await page.getByRole('row', { name: new RegExp(failedRun.slice(0, 16)) }).click();
  await expect(page.getByText(failedRun, { exact: true })).toBeVisible();
  await expect(page.getByText('agent exited with status 1', { exact: false }).first()).toBeVisible();
  await page.getByRole('tab', { name: 'Transcript' }).click();
  await expect(page.getByText('Starting again: running the integration tests.')).toBeVisible();
  await expect(page.getByText('[redacted:Zm9vYmFyYmF6cXV4]')).toBeVisible();
  await expect(page.getByRole('separator')).toContainText('attempt 2 on nomad-1 continues from here');

  // An overlay flyout with its mask.
  await page.getByRole('button', { name: 'Widen' }).click();
  await expect(page.locator('.euiOverlayMask')).toBeVisible();
  await page.getByRole('tab', { name: 'Trace' }).click();
  await page.getByRole('button', { name: 'Load trace' }).click();
  await expect(page.getByRole('button', { name: /^task\.claim, receiver/ })).toBeVisible();

  expect(await page.evaluate(() => window.__csp)).toEqual([]);
  expect(offOrigin).toEqual([]);
  expect(errors).toEqual([]);
});
