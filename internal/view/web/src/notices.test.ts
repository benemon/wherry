import { readdirSync, readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const staticDir = new URL('../../static/', import.meta.url);
const notices = readFileSync(new URL('THIRD_PARTY_NOTICES', staticDir), 'utf8');

// The committed notices cover the assets as well as the scripts: the fonts enter through CSS url(), leave as
// woff2 files, and their packages must be listed with the OFL text. A build whose notices miss them fails here; one
// not rebuilt fails the ui job's porcelain check.
describe('the committed notices', () => {
  it('list the font packages with the Open Font License', () => {
    const fonts = readdirSync(new URL('assets/', staticDir)).filter((f) => f.endsWith('.woff2'));
    expect(fonts.length).toBeGreaterThan(0);
    for (const name of ['@fontsource-variable/inter', '@fontsource-variable/roboto-mono']) {
      expect(notices).toMatch(new RegExp('^' + name.replace('/', '\\/') + ' \\d+\\.\\d+\\.\\d+ \\(OFL-1\\.1\\)$', 'm'));
    }
    expect(notices).toMatch(/SIL OPEN FONT LICENSE/i);
  });

  it('list the three Elastic packages under the Elastic License 2.0 with its text', () => {
    for (const name of ['@elastic/eui', '@elastic/eui-theme-borealis', '@elastic/eui-theme-common']) {
      expect(notices).toMatch(new RegExp('^' + name.replace('/', '\\/') + ' \\d+\\.\\d+\\.\\d+ \\(Elastic-2\\.0\\)$', 'm'));
    }
    expect(notices).toMatch(/Elastic License 2\.0/);
    expect(notices).not.toMatch(/^@elastic\/esql-definitions /m);
  });
});
