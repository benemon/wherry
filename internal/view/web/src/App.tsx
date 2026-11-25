import {
  EuiBadge,
  EuiBreadcrumbs,
  EuiButton,
  EuiButtonEmpty,
  EuiCallOut,
  EuiDescriptionList,
  EuiHeader,
  EuiHeaderSection,
  EuiHeaderSectionItem,
  EuiHealth,
  EuiIcon,
  EuiPopover,
  EuiSwitch,
  EuiText,
  EuiTitle,
  useEuiTheme,
} from '@elastic/eui';
import type { CSSProperties } from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { getModel, postSession } from './api';
import { Drawer } from './Drawer';
import { ClusterMap } from './ClusterMap';
import { Scrubber } from './Scrubber';
import { abs, clock, grouped, rel, stateColor, stateOrder, stateWord } from './format';
import type { Filters } from './model';
import { replay, tally, windowNote } from './model';
import type { Custodian, Model, Pool, Receiver, RunSummary, StreamEvent } from './types';
import type { Tally } from './model';

export type Sel =
  | { k: 'pool' | 'recv' | 'host' | 'store' | 'run'; id: string }
  | { k: 'broker' }
  | { k: 'list' };

// View is what every part of the page reads: the model, the moment shown and the runs as they stood then.
export interface View {
  model: Model;
  now: number;
  t: number;
  scrubbing: boolean;
  runs: RunSummary[];
  tally: Tally;
  windowStart: number;
  poolOf: Record<string, Pool>;
  receiverOf: Record<string, Receiver>;
  custodianOf: Record<string, Custodian>;
  sel: Sel | null;
  filters: Filters;
  hideEnded: boolean;
  open: (sel: Sel) => void;
  openList: (filters: Filters) => void;
  close: () => void;
}

export function App() {
  const { euiTheme } = useEuiTheme();
  const [model, setModel] = useState<Model | null>(null);
  const [status, setStatus] = useState('Loading.');
  const [updated, setUpdated] = useState<string | null>(null);
  const [sel, setSel] = useState<Sel | null>(null);
  const [fromList, setFromList] = useState(false);
  const [filters, setFilters] = useState<Filters>({});
  const [scrub, setScrub] = useState<number | null>(null);
  const [hideEnded, setHideEnded] = useState(false);
  const [wide, setWide] = useState(false);
  const [modelOpen, setModelOpen] = useState(false);
  const scrubbingRef = useRef(false);
  const staleRef = useRef(false);

  const load = useCallback(() => {
    getModel()
      .then((m) => {
        setModel(m);
        setStatus('');
      })
      .catch((err: Error) => setStatus('model: ' + err.message));
  }, []);

  useEffect(() => {
    let source: EventSource | null = null;
    const stream = () => {
      source = new EventSource('/api/stream');
      source.addEventListener('model', (e) => {
        const event = JSON.parse((e as MessageEvent).data) as StreamEvent;
        if (event.now) {
          setUpdated(event.now);
        }
        // Scrubbing pauses live updates; the model is fetched again on the return to live.
        if (scrubbingRef.current) {
          staleRef.current = true;
          return;
        }
        load();
      });
      source.onerror = () => setStatus('stream closed; reload the page');
    };
    const match = /^#t=([0-9a-f]+)$/.exec(window.location.hash);
    if (match) {
      history.replaceState(null, '', window.location.pathname);
      postSession(match[1])
        .then(() => {
          load();
          stream();
        })
        .catch((err: Error) => setStatus('session: ' + err.message));
    } else {
      load();
      stream();
    }
    return () => source?.close();
  }, [load]);

  const setScrubbing = (t: number | null) => {
    scrubbingRef.current = t !== null;
    setScrub(t);
    if (t === null && staleRef.current) {
      staleRef.current = false;
      load();
    }
  };

  const open = useCallback((s: Sel) => {
    setSel(s);
    setFromList(false);
  }, []);
  const openList = useCallback((f: Filters) => {
    setSel({ k: 'list' });
    setFilters(f);
    setFromList(false);
  }, []);
  const close = useCallback(() => setSel(null), []);

  const view = useMemo((): View | null => {
    if (!model) {
      return null;
    }
    const now = Date.parse(model.now);
    const w0 = Date.parse(model.window_start);
    const t = scrub === null ? now : Math.max(w0, Math.min(now, scrub));
    const runs = replay(model.runs, scrub === null ? null : t);
    const poolOf: Record<string, Pool> = {};
    const receiverOf: Record<string, Receiver> = {};
    const custodianOf: Record<string, Custodian> = {};
    model.pools.forEach((p) => (poolOf[p.name] = p));
    model.receivers.forEach((r) => (receiverOf[r.name] = r));
    model.custodians.forEach((c) => (custodianOf[c.pool] = c));
    return { model, now, t, scrubbing: scrub !== null, runs, tally: tally(runs, t), windowStart: w0, poolOf, receiverOf, custodianOf, sel, filters, hideEnded, open, openList, close };
  }, [model, scrub, sel, filters, hideEnded, open, openList, close]);

  const colors = euiTheme.colors;
  const vars = {
    '--w-page': colors.backgroundBaseSubdued,
    '--w-plain': colors.backgroundBasePlain,
    '--w-subdued': colors.backgroundBaseSubdued,
    '--w-primary': colors.primary,
    '--w-primary-bg': colors.backgroundBasePrimary,
    '--w-success': colors.success,
    '--w-warning': colors.warning,
    '--w-warning-bg': colors.backgroundLightWarning,
    '--w-danger': colors.danger,
    '--w-grey': colors.borderBaseProminent,
    '--w-border-subdued': colors.borderBaseSubdued,
    '--w-border-plain': colors.borderBasePlain,
    '--w-border-prominent': colors.borderBaseProminent,
    '--w-border-primary': colors.borderBasePrimary,
    '--w-text-heading': colors.textHeading,
    '--w-text-subdued': colors.textSubdued,
    '--w-text-disabled': colors.textDisabled,
    '--w-text-primary': colors.textPrimary,
    '--w-text-danger': colors.textDanger,
    '--w-hover': colors.backgroundBaseInteractiveHover,
  } as CSSProperties;

  if (!view) {
    return (
      <div className="w-root" style={vars}>
        <EuiText size="s" color="subdued" style={{ padding: 24 }}>
          <p>{status}</p>
        </EuiText>
      </div>
    );
  }

  const m = view.model;
  const broker = m.brokers[0];
  const note = windowNote(m);
  const updatedAt = updated || m.now;
  const totals = stateOrder.filter((k) => view.tally.byState[k]).map((k) => ({ state: k, count: view.tally.byState[k] }));
  const endedAny = m.hosts.some((h) => h.ended) || m.tenants.some((t) => t.ended);
  const modelButton = (
    <EuiButtonEmpty size="s" iconType="info" onClick={() => setModelOpen((o) => !o)} aria-label="Model">
      Model
    </EuiButtonEmpty>
  );

  return (
    <div className="w-root" style={vars}>
      <EuiHeader position="static">
        <EuiHeaderSection>
          <EuiHeaderSectionItem>
            <EuiTitle size="xs">
              <span style={{ padding: '0 8px' }}>wherry</span>
            </EuiTitle>
          </EuiHeaderSectionItem>
          <EuiHeaderSectionItem>
            <EuiBreadcrumbs breadcrumbs={[{ text: broker ? broker.host + ':' + broker.port : 'no broker' }, { text: 'Map' }]} />
          </EuiHeaderSectionItem>
        </EuiHeaderSection>
        <EuiHeaderSection side="right">
          <EuiHeaderSectionItem>
            <EuiHealth color={view.scrubbing ? 'warning' : m.rebuilt ? 'success' : 'primary'}>
              {view.scrubbing ? 'Paused at ' + clock(view.t) : (m.rebuilt ? 'Live' : 'Rebuilding') + ' · model v' + m.version}
            </EuiHealth>
          </EuiHeaderSectionItem>
          <EuiHeaderSectionItem>
            <EuiPopover button={modelButton} isOpen={modelOpen} closePopover={() => setModelOpen(false)} panelPaddingSize="m" anchorPosition="downRight">
              <div style={{ width: 320 }}>
                <EuiDescriptionList
                  compressed
                  type="column"
                  listItems={[
                    { title: 'Version', description: String(m.version) },
                    { title: 'Window', description: abs(m.window_start) + ' to now' },
                    { title: 'Rebuilt', description: m.rebuilt ? rel(view.now, m.rebuilt_at) : 'in progress' },
                    { title: 'Runs kept', description: grouped(m.runs_total) + (m.truncated ? ', list cut to ' + m.runs.length : '') },
                    { title: 'Counts', description: Object.keys(m.counts).sort().map((k) => k + ' ' + m.counts[k]).join(', ') || 'none' },
                    { title: 'Bounds hit', description: m.bounds.length ? m.bounds.map((b) => b.name + ' × ' + b.hits).join(', ') : 'none' },
                    { title: 'Recent errors', description: m.errors?.length ? m.errors.join(', ') : 'none' },
                  ]}
                />
              </div>
            </EuiPopover>
          </EuiHeaderSectionItem>
          <EuiHeaderSectionItem>
            <EuiButtonEmpty iconType="listBullet" size="s" onClick={() => openList({})}>
              Runs ({grouped(view.runs.length)})
            </EuiButtonEmpty>
          </EuiHeaderSectionItem>
        </EuiHeaderSection>
      </EuiHeader>

      <main className="w-main">
        {status && (
          <EuiCallOut title={status} color="danger" iconType="warning" size="s" />
        )}
        <div className="w-panel w-strip">
          <div className="w-row" style={{ gap: '8px 16px' }}>
            <span className="w-row" style={{ gap: 6, color: colors.textHeading, fontWeight: 500 }}>
              <EuiIcon type="clock" color="subdued" />
              Last {Math.round((view.now - view.windowStart) / 3600000)} h
            </span>
            <span className="w-subdued" title={abs(updatedAt)}>
              updated {rel(view.now, updatedAt)}
            </span>
            <span style={{ width: 1, height: 20, background: colors.borderBaseSubdued }} />
            <span className="w-subdued">{grouped(view.runs.length)} runs</span>
            {totals.map((b) => (
              <EuiBadge key={b.state} color={stateColor(b.state)} onClick={() => openList({ state: b.state })} onClickAriaLabel={'Show ' + stateWord(b.state) + ' runs'}>
                {grouped(b.count) + ' ' + stateWord(b.state)}
              </EuiBadge>
            ))}
            <span style={{ flexGrow: 1 }} />
            {endedAny && <EuiSwitch label="Show ended hosts" checked={!hideEnded} onChange={(e) => setHideEnded(!e.target.checked)} compressed />}
            {view.scrubbing && (
              <EuiButton size="s" fill iconType="play" onClick={() => setScrubbing(null)}>
                Back to live
              </EuiButton>
            )}
          </div>
          <Scrubber view={view} onScrub={setScrubbing} />
        </div>
        {note && <EuiCallOut title={note} color="primary" iconType="info" size="s" />}
        <ClusterMap view={view} />
      </main>

      {sel && <Drawer view={view} fromList={fromList} setFromList={setFromList} setFilters={setFilters} wide={wide} setWide={setWide} />}
    </div>
  );
}
