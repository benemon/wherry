import { EuiBadge, EuiHealth, EuiIcon } from '@elastic/eui';
import type { ReactNode } from 'react';
import { useEffect, useRef, useState } from 'react';
import type { View } from './App';
import { bytes, grouped } from './format';
import type { Health } from './model';
import { lagShort, noHost, receiverHealth, recentMs } from './model';
import type { RunSummary } from './types';

// The stage is laid out at a fixed width and scaled to the space the section has, so a drawer never makes the map
// scroll sideways.
const stageWidth = 928;

const ringColor: Record<Health['color'], string> = { danger: 'var(--w-danger)', warning: 'var(--w-warning)', success: 'var(--w-success)', subdued: 'var(--w-grey)', primary: 'var(--w-primary)' };

const ring = (color: string, size: number) => ({ width: size, height: size, border: '3px solid ' + color });

interface Layout {
  height: number;
  base: string;
  dashed: string;
  highlight: string;
  dots: { key: string; path: string; delay: string }[];
  nodes: ReactNode;
  brokerTop: number;
  legendTop: number;
}

export function ClusterMap({ view }: { view: View }) {
  const section = useRef<HTMLElement>(null);
  const [scale, setScale] = useState(1);
  useEffect(() => {
    const el = section.current;
    if (!el) {
      return;
    }
    const fit = () => setScale(Math.min(1, (el.clientWidth - 2) / stageWidth));
    fit();
    const observer = new ResizeObserver(fit);
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const m = view.model;
  const compact = m.hosts.length > 14 || m.receivers.length > 12;
  const lay = compact ? compactLayout(view) : fullLayout(view);
  return (
    <section ref={section} aria-label="Cluster map" className="w-panel w-map" style={{ height: Math.ceil(lay.height * scale) + 16 }}>
      <div className="w-stage" style={{ width: stageWidth, height: lay.height, margin: '8px 0', transform: 'scale(' + scale + ')' }}>
        <svg width={stageWidth} height={lay.height} viewBox={'0 0 ' + stageWidth + ' ' + lay.height} style={{ position: 'absolute', left: 0, top: 0 }} aria-hidden="true">
          <path d={lay.base} fill="none" stroke="var(--w-border-plain)" strokeWidth="1.5" />
          <path d={lay.dashed} fill="none" stroke="var(--w-border-plain)" strokeWidth="1.5" strokeDasharray="4 4" />
          <path d={lay.highlight} fill="none" stroke="var(--w-primary)" strokeWidth="3" />
        </svg>
        {lay.dots.map((d) => (
          <span key={d.key} className="w-flow" aria-hidden="true" style={{ offsetPath: "path('" + d.path + "')", animationDelay: d.delay }} />
        ))}
        {lay.nodes}
        <button type="button" className="w-nb" style={{ left: 360, top: lay.brokerTop, width: 160, flexDirection: 'column', gap: 6, textAlign: 'center' }} onClick={() => view.open({ k: 'broker' })} aria-label="Broker">
          <span className="w-ring" style={ring('var(--w-grey)', 72)}>
            <EuiIcon type="cluster" size="l" />
          </span>
          <span className="w-nt">
            <b>Broker</b>
            {m.brokers.length + (m.brokers.length === 1 ? ' node · ' : ' nodes · ') + m.topics.length + ' topics'}
          </span>
        </button>
        <div className="w-legend" style={{ left: 236, top: lay.legendTop }}>
          <EuiHealth color="success" textSize="xs">
            live
          </EuiHealth>
          <EuiHealth color="warning" textSize="xs">
            live but gated or suspended
          </EuiHealth>
          <EuiHealth color="danger" textSize="xs">
            failed in last {recentMs / 60000} min, stale or silent
          </EuiHealth>
          <EuiHealth color="subdued" textSize="xs">
            idle, ended or no health
          </EuiHealth>
          <span className="w-row" style={{ gap: 6 }}>
            <EuiBadge color="danger">n</EuiBadge>failed in window
          </span>
          <span className="w-row" style={{ gap: 6 }}>
            <span style={{ width: 16, height: 3, background: 'var(--w-primary)' }} />
            selection
          </span>
        </div>
      </div>
    </section>
  );
}

function selectedRun(view: View): RunSummary | undefined {
  if (view.sel?.k !== 'run') {
    return undefined;
  }
  const id = view.sel.id;
  return view.runs.find((r) => r.id === id) || view.model.runs.find((r) => r.id === id);
}

// hostRing is a host's colour: red for a recent failure, green while a run is in flight, grey when idle.
const hostRing = (view: View, name: string) => (view.tally.recentHost[name] ? 'var(--w-danger)' : view.tally.hostWorking[name] ? 'var(--w-success)' : 'var(--w-grey)');

const custodianText = (view: View, pool: string) => {
  const c = view.custodianOf[pool];
  if (!c) {
    return 'no custodian';
  }
  const age = Math.floor((view.now - Date.parse(c.at)) / 3600000);
  return 'custodian ' + c.receiver + (c.suspended ? ' · suspended' : ' · key ' + age + ' h') + (view.scrubbing ? ' · now' : '');
};

// fullLayout places every host and receiver as its own node.
function fullLayout(view: View): Layout {
  const m = view.model;
  const sel = view.sel;
  const selRun = selectedRun(view);
  const live = m.hosts.filter((h) => !h.ended);
  const ended = view.hideEnded ? [] : m.hosts.filter((h) => h.ended);
  const hostCy: Record<string, number> = {};
  const hosts = live.map((h, i) => {
    const top = 98 + i * 90;
    hostCy[h.name] = top + 22;
    const ran = view.tally.hostRuns[h.name] || 0;
    const working = view.tally.hostWorking[h.name] || 0;
    return (
      <button key={'h' + h.name} type="button" className="w-nb" style={{ left: 42, top, width: 180 }} onClick={() => view.open({ k: 'host', id: h.name })} aria-label={'Host ' + h.name}>
        <span className="w-ring" style={ring(hostRing(view, h.name), 44)}>
          <EuiIcon type="user" />
        </span>
        <span className="w-nt">
          <b>{h.name}</b>
          {ran ? ran + ' runs' + (working ? ' · ' + working + ' in flight' : '') : 'idle'}
        </span>
      </button>
    );
  });
  const endedTop = 98 + live.length * 90;
  const unknownRuns = view.tally.hostRuns[noHost] || 0;
  const unknownTop = endedTop + 28 + ended.length * 22 + 60;
  hostCy[noHost] = unknownTop + 22;
  const recCy: Record<string, number> = {};
  const poolCy: Record<string, number> = {};
  const pools = m.pools.map((p, i) => {
    const top = 40 + i * 220;
    const recs = p.receivers.map((name, j) => {
      const r = view.receiverOf[name];
      const rtop = top + 56 + j * 64;
      recCy[name] = rtop + 24;
      const health = r ? receiverHealth(r, p, view.custodianOf[p.name], view.tally) : { color: 'subdued' as const, label: 'unknown' };
      const inFlight = view.scrubbing ? view.tally.receiverWorking[name] || 0 : r?.in_flight || 0;
      const failed = view.tally.receiverFailed[name] || 0;
      const isEnded = !r || r.ended || r.status === 'ended';
      return (
        <span key={'r' + name}>
          <button type="button" className="w-nb" style={{ left: 600, top: rtop, width: 196, opacity: isEnded ? 0.5 : 1 }} onClick={() => view.open({ k: 'recv', id: name })} aria-label={'Receiver ' + name} title={health.label}>
            <span className="w-ring" style={ring(ringColor[health.color], 48)}>
              <EuiIcon type="database" />
            </span>
            <span className="w-nt">
              <b>{name}</b>
              {inFlight + ' of ' + (r?.capacity || 0) + ((r?.capacity || 0) === 1 ? ' slot' : ' slots')}
            </span>
          </button>
          {failed > 0 && (
            <span title={failed + ' failed runs in the window'} style={{ position: 'absolute', left: 520, top: Math.round((360 + rtop + 24) / 2 - 10) }}>
              <EuiBadge color="danger">{failed}</EuiBadge>
            </span>
          )}
        </span>
      );
    });
    const h = 56 + recs.length * 64 + 40;
    poolCy[p.name] = top + h / 2;
    const isSel = (sel?.k === 'pool' && sel.id === p.name) || selRun?.pool === p.name;
    const lagBad = p.lag.status !== 'ok' && p.lag.status !== 'zero';
    return (
      <span key={'p' + p.name}>
        <button type="button" className={'w-box w-boxbutton' + (isSel ? ' w-selected' : '') + (p.suspended ? ' w-suspended' : '')} style={{ left: 580, top, width: 220, height: h }} onClick={() => view.open({ k: 'pool', id: p.name })} aria-label={'Pool ' + p.name} />
        <div className="w-nt" style={{ position: 'absolute', left: 594, top: top + 10, width: 194, display: 'flex', justifyContent: 'space-between', pointerEvents: 'none' }}>
          <span style={{ fontWeight: 600, color: 'var(--w-text-heading)' }}>Pool {p.name}</span>
          <span style={{ color: lagBad ? 'var(--w-warning)' : undefined }}>{lagShort(p) + (view.scrubbing ? ' · now' : '')}</span>
        </div>
        {recs}
        <div className="w-nt" style={{ position: 'absolute', left: 594, top: top + h - 30, width: 200, display: 'flex', alignItems: 'center', gap: 6, pointerEvents: 'none', opacity: view.scrubbing ? 0.6 : 1 }}>
          <EuiIcon type="key" size="s" />
          {custodianText(view, p.name)}
        </div>
      </span>
    );
  });
  const first = m.pools[0];
  const last = m.pools[m.pools.length - 1];
  const firstCy = first ? recCy[first.receivers[0]] : 360;
  const lastCy = last ? recCy[last.receivers[last.receivers.length - 1]] : 360;
  const sCy = Math.round(((firstCy || 360) + (lastCy || 360)) / 2);
  const stores = m.storage.map((s, i) => (
    <button key={'s' + s.bucket} type="button" className="w-nb" style={{ left: 820, top: sCy - 24 + i * 70, width: 100, flexDirection: 'column', gap: 6, textAlign: 'center' }} onClick={() => view.open({ k: 'store', id: s.endpoint + '/' + s.bucket })} aria-label={'Storage ' + s.bucket}>
      <span className="w-ring" style={ring('var(--w-grey)', 48)}>
        <EuiIcon type="storage" />
      </span>
      <span className="w-nt">
        <b>S3</b>
        {s.bucket}
        <br />
        {bytes(s.bytes) + ' · ' + s.outputs + ' files'}
        {s.unreturned > 0 && (
          <>
            <br />
            <EuiBadge color="warning">{s.unreturned + ' not returned'}</EuiBadge>
          </>
        )}
      </span>
    </button>
  ));
  const hostLink = (cy: number) => 'M86 ' + cy + ' C 250 ' + cy + ', 250 360, 404 360 ';
  const recLink = (cy: number) => 'M476 360 C 540 360, 540 ' + cy + ', 600 ' + cy + ' ';
  const poolStore = (cy: number) => 'M800 ' + cy + ' C 836 ' + cy + ', 836 ' + sCy + ', 846 ' + sCy + ' ';
  const base = live.map((h) => hostLink(hostCy[h.name])).join('') + Object.values(recCy).map(recLink).join('') + m.pools.map((p) => poolStore(poolCy[p.name])).join('');
  let highlight = '';
  if (sel?.k === 'host') {
    highlight = hostLink(hostCy[sel.id] ?? hostCy[noHost]);
  }
  if (sel?.k === 'recv' && recCy[sel.id]) {
    highlight = recLink(recCy[sel.id]);
  }
  if (sel?.k === 'pool') {
    highlight = (view.poolOf[sel.id]?.receivers || []).map((n) => recLink(recCy[n])).join('');
  }
  if (sel?.k === 'store') {
    highlight = m.pools.map((p) => poolStore(poolCy[p.name])).join('');
  }
  if (selRun) {
    highlight = hostLink(hostCy[selRun.host || noHost] ?? hostCy[noHost]) + (selRun.receiver && recCy[selRun.receiver] ? recLink(recCy[selRun.receiver]) : '');
  }
  if (sel?.k === 'list') {
    const hs = new Set<string>();
    const rs = new Set<string>();
    view.runs.filter((r) => matchesFilters(view, r)).forEach((r) => {
      hs.add(r.host || noHost);
      if (r.receiver) {
        rs.add(r.receiver);
      }
    });
    if (Object.keys(view.filters).some((k) => view.filters[k as keyof typeof view.filters])) {
      highlight = [...hs].map((x) => hostLink(hostCy[x] ?? hostCy[noHost])).join('') + [...rs].filter((x) => recCy[x]).map((x) => recLink(recCy[x])).join('');
    }
  }
  const flying = view.runs.filter((r) => !r.terminal && r.receiver && recCy[r.receiver]);
  const dots = flying.slice(0, 200).map((r, i) => ({ key: r.id, path: (hostLink(hostCy[r.host || noHost] ?? hostCy[noHost]) + 'L 476 360 ' + recLink(recCy[r.receiver!]).replace(/^M476 360 /, '')).trim(), delay: '-' + ((i * 0.37) % 2.4).toFixed(2) + 's' }));
  const tenantHeight = endedTop + 28 + ended.length * 22 - 40 + 12;
  const nodes = (
    <>
      <div className="w-box" style={{ left: 16, top: 40, width: 220, height: tenantHeight }} />
      <button type="button" className="w-nt w-textbutton" onClick={() => view.openList({ tenant: m.tenants[0]?.name })} title="Show this tenant's runs in the run list" style={{ position: 'absolute', left: 30, top: 50, fontWeight: 600, color: 'var(--w-text-heading)' }}>
        {m.tenants.length === 1 ? 'Tenant ' + m.tenants[0].name : m.tenants.length + ' tenants'}
      </button>
      {hosts}
      {ended.length > 0 && (
        <div style={{ position: 'absolute', left: 30, top: endedTop, width: 192, display: 'flex', flexDirection: 'column', gap: 6, opacity: 0.6, fontSize: 12, lineHeight: '16px', color: 'var(--w-text-subdued)' }}>
          <span style={{ fontWeight: 600 }}>Ended</span>
          {ended.map((h) => (
            <button key={h.name} type="button" className="w-textbutton" onClick={() => view.open({ k: 'host', id: h.name })} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <EuiIcon type="user" size="s" />
              {h.name}
            </button>
          ))}
        </div>
      )}
      {unknownRuns > 0 && (
        <button type="button" className="w-nb" style={{ left: 42, top: unknownTop, width: 180 }} onClick={() => view.open({ k: 'host', id: '' })} aria-label="Runs with an unknown host">
          <span className="w-ring" style={{ width: 44, height: 44, border: '2px dashed var(--w-border-prominent)', background: 'var(--w-subdued)', color: 'var(--w-text-disabled)' }}>
            <EuiIcon type="question" />
          </span>
          <span className="w-nt" style={{ color: 'var(--w-text-disabled)' }}>
            <b style={{ color: 'var(--w-text-subdued)', fontWeight: 450, fontStyle: 'italic' }}>unknown host</b>
            {unknownRuns + (unknownRuns === 1 ? ' run' : ' runs')}
          </span>
        </button>
      )}
      {pools}
      {stores}
    </>
  );
  const height = Math.max(700, 40 + m.pools.length * 220 + 60, unknownTop + 80);
  return { height, base, dashed: unknownRuns ? hostLink(hostCy[noHost]) : '', highlight, dots, nodes, brokerTop: 324, legendTop: height - 36 };
}

const matchesFilters = (view: View, r: RunSummary) => {
  const f = view.filters;
  return (!f.state || r.state === f.state) && (!f.tenant || r.tenant === f.tenant) && (!f.pool || r.pool === f.pool) && (!f.receiver || (r.receiver || '__none') === f.receiver) && (!f.host || (r.host || '__none') === f.host) && (!f.agent || (r.agent || '__none') === f.agent) && (!f.q || r.id.includes(f.q.trim()));
};

// compactLayout collapses hosts into one row per tenant and receivers into rings inside their pool; links run
// tenant to broker to pool to storage.
function compactLayout(view: View): Layout {
  const m = view.model;
  const sel = view.sel;
  const selRun = selectedRun(view);
  const hostTenant: Record<string, string> = {};
  m.hosts.forEach((h) => (hostTenant[h.name] = h.tenant));
  const perRow = 9;
  const recPerRow = 7;
  const tCy: Record<string, number> = {};
  let y = 40;
  const tenants = m.tenants
    .filter((t) => !(view.hideEnded && t.ended))
    .map((t) => {
      const hs = m.hosts.filter((h) => h.tenant === t.name && !(view.hideEnded && h.ended));
      const rowsN = Math.max(1, Math.ceil(hs.length / perRow));
      const hgt = 14 + rowsN * 24;
      const top = y;
      y += hgt + 6;
      tCy[t.name] = top + hgt / 2;
      const working = hs.reduce((n, h) => n + (view.tally.hostWorking[h.name] || 0), 0);
      const ran = hs.reduce((n, h) => n + (view.tally.hostRuns[h.name] || 0), 0);
      const isSel = (sel?.k === 'host' && hostTenant[sel.id] === t.name) || (selRun && hostTenant[selRun.host || ''] === t.name);
      return (
        <div key={'t' + t.name} className={'w-box' + (isSel ? ' w-selected' : '')} style={{ left: 16, top, width: 300, minHeight: hgt, display: 'flex', alignItems: 'center', gap: 10, padding: '4px 10px', borderRadius: 8, opacity: t.ended ? 0.45 : 1 }}>
          <button type="button" className="w-textbutton" onClick={() => view.openList({ tenant: t.name })} title={'Show ' + t.name + ' runs in the run list'} style={{ width: 92, flex: 'none', fontSize: 11, lineHeight: '14px', color: 'var(--w-text-subdued)', overflow: 'hidden' }}>
            <b style={{ display: 'block', fontSize: 12, lineHeight: '16px', fontWeight: 600, color: 'var(--w-text-heading)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{t.name}</b>
            {grouped(ran) + ' runs' + (working ? ' · ' + working + ' live' : '')}
          </button>
          <span style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {hs.map((h) => {
              const wk = view.tally.hostWorking[h.name] || 0;
              const isHostSel = (sel?.k === 'host' && sel.id === h.name) || selRun?.host === h.name;
              return (
                <button key={h.name} type="button" className={'w-pill' + (isHostSel ? ' w-selected' : '')} title={h.name + ' · ' + (view.tally.hostRuns[h.name] || 0) + ' runs' + (wk ? ' · ' + wk + ' in flight' : '') + (h.ended ? ' · ended' : '')} aria-label={'Host ' + h.name} onClick={() => view.open({ k: 'host', id: h.name })} style={{ width: 18, height: 18, border: '3px solid ' + hostRing(view, h.name), opacity: h.ended ? 0.35 : 1 }} />
              );
            })}
          </span>
        </div>
      );
    });
  const unknownRuns = view.tally.hostRuns[noHost] || 0;
  if (unknownRuns) {
    const top = y;
    y += 38;
    tCy[noHost] = top + 16;
    tenants.push(
      <div key="t?" className="w-box" style={{ left: 16, top, width: 300, minHeight: 32, display: 'flex', alignItems: 'center', gap: 10, padding: '4px 10px', borderRadius: 8, border: '1px dashed var(--w-border-prominent)', color: 'var(--w-text-disabled)', fontStyle: 'italic' }}>
        <button type="button" className="w-textbutton" onClick={() => view.openList({ host: '__none' })} style={{ fontSize: 12 }}>
          <b>unknown host</b> {unknownRuns} runs
        </button>
      </div>,
    );
  }
  const leftBottom = y;
  const poolCy: Record<string, number> = {};
  let py = 40;
  const pools = m.pools.map((p) => {
    const rowsN = Math.max(1, Math.ceil(p.receivers.length / recPerRow));
    const hgt = 24 + rowsN * 30 + 22;
    const top = py;
    py += hgt + 10;
    poolCy[p.name] = top + hgt / 2;
    const cust = view.custodianOf[p.name];
    const isSel = (sel?.k === 'pool' && sel.id === p.name) || selRun?.pool === p.name || (sel?.k === 'recv' && view.receiverOf[sel.id]?.pool === p.name);
    const lagBad = p.lag.status !== 'ok' && p.lag.status !== 'zero';
    return (
      <span key={'p' + p.name}>
        <div className={'w-box' + (isSel ? ' w-selected' : '') + (p.suspended ? ' w-suspended' : '')} style={{ left: 580, top, width: 240, minHeight: hgt, display: 'flex', flexDirection: 'column', gap: 4, padding: '4px 10px 6px' }}>
          <button type="button" className="w-textbutton" onClick={() => view.open({ k: 'pool', id: p.name })} aria-label={'Pool ' + p.name} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', width: '100%', padding: '2px 0' }}>
            <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--w-text-heading)' }}>Pool {p.name}</span>
            <span style={{ fontSize: 11, color: lagBad ? 'var(--w-warning)' : 'var(--w-text-subdued)' }}>{lagShort(p) + (view.scrubbing ? ' · now' : '')}</span>
          </button>
          <span style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {p.receivers.map((n) => {
              const r = view.receiverOf[n];
              const health = r ? receiverHealth(r, p, cust, view.tally) : { color: 'subdued' as const, label: 'unknown' };
              const isC = cust?.receiver === n;
              const isEnded = !r || r.ended || r.status === 'ended';
              const isRecSel = (sel?.k === 'recv' && sel.id === n) || selRun?.receiver === n;
              const f = view.scrubbing ? view.tally.receiverWorking[n] || 0 : r?.in_flight || 0;
              return (
                <button key={n} type="button" className={'w-pill' + (isRecSel ? ' w-selected' : '')} title={n + ' · ' + health.label + ' · ' + f + ' of ' + (r?.capacity || 0) + ' slots' + (isC ? ' · custodian' : '')} aria-label={'Receiver ' + n} onClick={() => view.open({ k: 'recv', id: n })} style={{ width: 24, height: 24, background: f ? 'var(--w-primary-bg)' : undefined, border: '3px solid ' + ringColor[health.color], opacity: isEnded ? 0.4 : 1 }}>
                  {isC && <EuiIcon type="key" size="s" />}
                </button>
              );
            })}
          </span>
          <span style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 11, lineHeight: '16px', color: cust?.suspended ? 'var(--w-warning)' : 'var(--w-text-subdued)', opacity: view.scrubbing ? 0.6 : 1 }}>{custodianText(view, p.name)}</span>
        </div>
      </span>
    );
  });
  const bottom = Math.max(leftBottom, py, 520);
  const centre = Math.round((40 + bottom) / 2);
  const badges = m.pools
    .filter((p) => view.tally.poolFailed[p.name])
    .map((p) => (
      <span key={'b' + p.name} title={view.tally.poolFailed[p.name] + ' failed runs in the window'} style={{ position: 'absolute', left: 516, top: Math.round((centre + poolCy[p.name]) / 2 - 10) }}>
        <EuiBadge color="danger">{view.tally.poolFailed[p.name]}</EuiBadge>
      </span>
    ));
  const storeCy: Record<string, number> = {};
  m.storage.forEach((s) => {
    const ys = s.pools.map((n) => poolCy[n]).filter((v) => v !== undefined);
    storeCy[s.bucket] = ys.length ? Math.round(ys.reduce((a, b) => a + b, 0) / ys.length) : centre;
  });
  const stores = m.storage.map((s, i) => (
    <button key={'s' + s.bucket} type="button" className="w-nb" style={{ left: 830, top: storeCy[s.bucket] - 22 + i * 4, width: 96, flexDirection: 'column', gap: 4, textAlign: 'center' }} onClick={() => view.open({ k: 'store', id: s.endpoint + '/' + s.bucket })} aria-label={'Storage ' + s.bucket}>
      <span className="w-ring" style={ring('var(--w-grey)', 44)}>
        <EuiIcon type="storage" />
      </span>
      <span className="w-nt">
        <b>S3</b>
        {s.bucket}
        <br />
        {bytes(s.bytes) + ' · ' + grouped(s.outputs) + ' files'}
        {s.unreturned > 0 && (
          <>
            <br />
            <EuiBadge color="warning">{s.unreturned + ' not returned'}</EuiBadge>
          </>
        )}
      </span>
    </button>
  ));
  const tLink = (cy: number) => 'M316 ' + cy + ' C 362 ' + cy + ', 362 ' + centre + ', 404 ' + centre + ' ';
  const pLink = (cy: number) => 'M476 ' + centre + ' C 528 ' + centre + ', 528 ' + cy + ', 580 ' + cy + ' ';
  const psLink = (pcy: number, scy: number) => 'M820 ' + pcy + ' C 840 ' + pcy + ', 840 ' + scy + ', 856 ' + scy + ' ';
  const poolStores = (name: string) => m.storage.filter((s) => s.pools.includes(name)).map((s) => psLink(poolCy[name], storeCy[s.bucket])).join('');
  const base = Object.keys(tCy).filter((k) => k !== noHost).map((k) => tLink(tCy[k])).join('') + m.pools.map((p) => pLink(poolCy[p.name]) + poolStores(p.name)).join('');
  let highlight = '';
  if (sel?.k === 'host') {
    highlight = tLink(tCy[hostTenant[sel.id]] ?? tCy[noHost] ?? centre);
  }
  if (sel?.k === 'recv' && view.receiverOf[sel.id]) {
    highlight = pLink(poolCy[view.receiverOf[sel.id].pool]);
  }
  if (sel?.k === 'pool' && poolCy[sel.id] !== undefined) {
    highlight = pLink(poolCy[sel.id]);
  }
  if (sel?.k === 'store') {
    highlight = m.storage.filter((s) => s.endpoint + '/' + s.bucket === sel.id).flatMap((s) => s.pools.map((n) => psLink(poolCy[n], storeCy[s.bucket]))).join('');
  }
  if (selRun) {
    highlight = tLink(tCy[hostTenant[selRun.host || '']] ?? tCy[noHost] ?? centre) + (poolCy[selRun.pool] !== undefined ? pLink(poolCy[selRun.pool]) : '');
  }
  if (sel?.k === 'list' && Object.keys(view.filters).some((k) => view.filters[k as keyof typeof view.filters])) {
    const ts = new Set<string>();
    const ps = new Set<string>();
    view.runs.filter((r) => matchesFilters(view, r)).forEach((r) => {
      ts.add(hostTenant[r.host || ''] || noHost);
      ps.add(r.pool);
    });
    highlight = [...ts].filter((k) => tCy[k] !== undefined).map((k) => tLink(tCy[k])).join('') + [...ps].filter((k) => poolCy[k] !== undefined).map((k) => pLink(poolCy[k])).join('');
  }
  const flying = view.runs.filter((r) => !r.terminal && poolCy[r.pool] !== undefined);
  const dots = flying.slice(0, 200).map((r, i) => ({ key: r.id, path: (tLink(tCy[hostTenant[r.host || '']] ?? tCy[noHost] ?? centre) + 'L 476 ' + centre + ' ' + pLink(poolCy[r.pool]).replace(/^M476 \d+ /, '')).trim(), delay: '-' + ((i * 0.53) % 2.4).toFixed(2) + 's' }));
  const height = bottom + 70;
  return { height, base, dashed: tCy[noHost] !== undefined ? tLink(tCy[noHost]) : '', highlight, dots, nodes: [...tenants, ...pools, ...badges, ...stores], brokerTop: centre - 36, legendTop: height - 36 };
}
