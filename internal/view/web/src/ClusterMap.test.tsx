import { EuiProvider } from '@elastic/eui';
import { readFileSync } from 'node:fs';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import type { View } from './App';
import { ClusterMap } from './ClusterMap';
import { tally } from './model';
import type { Model } from './types';

const model = JSON.parse(readFileSync(new URL('../fixtures/model.json', import.meta.url), 'utf8')) as Model;

const view = (model: Model): View => {
  const now = Date.parse(model.now);
  return {
    model,
    now,
    t: now,
    scrubbing: false,
    runs: model.runs,
    tally: tally(model.runs, now),
    windowStart: Date.parse(model.window_start),
    poolOf: Object.fromEntries(model.pools.map((p) => [p.name, p])),
    receiverOf: Object.fromEntries(model.receivers.map((r) => [r.name, r])),
    custodianOf: Object.fromEntries(model.custodians.map((c) => [c.pool, c])),
    sel: null,
    filters: {},
    hideEnded: false,
    open: () => {},
    openList: () => {},
    close: () => {},
  };
};

const render = (v: View) => renderToStaticMarkup(<EuiProvider globalStyles={false}><ClusterMap view={v} /></EuiProvider>);

describe('cluster map receivers', () => {
  it('renders receiver capacity as a determinate progress bar', () => {
    const markup = render(view(model));
    const card = markup.match(/<button[^>]*aria-label="Receiver nomad-1"[\s\S]*?<\/button>/)?.[0];
    const progress = card?.match(/<progress[^>]*>/)?.[0];

    expect(progress).toContain('value="2"');
    expect(progress).toContain('max="3"');
    expect(card).toContain('>slots<');
    expect(card).toContain('>2/3<');
  });

  it('describes compact receiver capacity as slots in use', () => {
    const hosts = Array.from({ length: 15 }, (_, i) => ({ ...model.hosts[0], name: 'compact-' + i }));
    const markup = render(view({ ...model, hosts }));

    expect(markup).toContain('2 of 3 slots in use');
  });
});
