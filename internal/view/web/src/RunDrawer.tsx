import { EuiBadge, EuiButton, EuiButtonEmpty, EuiCallOut, EuiDescriptionList, EuiHealth, EuiTab, EuiTabs, EuiText } from '@elastic/eui';
import { useEffect, useState } from 'react';
import type { View } from './App';
import { ApiError, getRun, getTrace } from './api';
import { Section } from './Drawer';
import { TranscriptPane } from './TranscriptPane';
import { abs, bytes, dur, or, rel, signedBy, stateColor, stateWord } from './format';
import type { Filters } from './model';
import { activeFilters } from './model';
import { flatten, markerText, serviceColors, spanStatus } from './trace';
import { segments } from './transcript';
import type { Attempt, Forest, RunDetail, RunSummary } from './types';

interface Props {
  view: View;
  id: string;
  fromList: boolean;
  setFromList: (v: boolean) => void;
  wide: boolean;
  setWide: (v: boolean) => void;
}

type Tab = 'overview' | 'transcript' | 'trace';

// Segments renders agent text as text: its redaction markers muted, search hits marked.
export const Segments = ({ text, q = '' }: { text: string; q?: string }) => (
  <>
    {segments(text, q).map((s, i) =>
      s.kind === 'text' ? <span key={i}>{s.text}</span> : <span key={i} className={s.kind === 'redacted' ? 'w-redacted' : 'w-hit'}>{s.text}</span>,
    )}
  </>
);

// RunDrawer is the run's header, its path and outputs, its live transcript and its trace; the detail is fetched on
// open and again on each model version while the run works, so the status messages follow it.
export function RunDrawer({ view, id, fromList, setFromList, wide, setWide }: Props) {
  const summary: RunSummary | undefined = view.runs.find((r) => r.id === id) || view.model.runs.find((r) => r.id === id);
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab | null>(null);
  const working = summary ? !summary.terminal : false;
  const version = view.version;
  useEffect(() => {
    let cancelled = false;
    getRun(id)
      .then((d) => {
        if (!cancelled) {
          setDetail(d);
          setError(null);
        }
      })
      .catch((err: Error) => {
        if (!cancelled) {
          setError(err instanceof ApiError && err.status === 404 ? 'This run is no longer in the model.' : err instanceof ApiError && err.status === 507 ? 'The run’s detail is over the response bound.' : 'run: ' + err.message);
        }
      });
    return () => {
      cancelled = true;
    };
    // The detail is fetched again on each model version only while the run works.
  }, [id, working ? version : 0]);
  useEffect(() => setTab(null), [id]);

  if (!summary) {
    return <EuiText size="s">This run is no longer in the model.</EuiText>;
  }
  const r = summary;
  const t0 = Date.parse(r.admitted || r.first);
  const shown: Tab = tab ?? (working ? 'transcript' : 'overview');
  const filterNote = activeFilters(view.filters) ? ' (' + activeFilters(view.filters) + (activeFilters(view.filters) === 1 ? ' filter)' : ' filters)') : '';
  return (
    <>
      {fromList && (
        <div style={{ margin: '-12px 0 -8px -8px' }}>
          <EuiButtonEmpty
            iconType="chevronSingleLeft"
            size="s"
            onClick={() => {
              view.openList(view.filters as Filters);
              setFromList(false);
            }}
          >
            Back to runs{filterNote}
          </EuiButtonEmpty>
        </div>
      )}
      <div className="w-section">
        <div className="w-row" style={{ justifyContent: 'space-between', flexWrap: 'nowrap' }}>
          <span className="w-mono" style={{ fontSize: 13, color: 'var(--w-text-heading)', fontWeight: 500 }}>
            {r.id}
          </span>
          <EuiButtonEmpty size="xs" iconType={wide ? 'fullScreenExit' : 'fullScreen'} onClick={() => setWide(!wide)}>
            {wide ? 'Narrow' : 'Widen'}
          </EuiButtonEmpty>
        </div>
        <div className="w-row">
          <EuiBadge color={stateColor(r.state)}>{stateWord(r.state)}</EuiBadge>
          <EuiBadge color="hollow">attempt {detail?.attempt ?? r.attempt}</EuiBadge>
          <EuiBadge color="hollow">{working ? 'running ' + dur(view.t - t0) : dur(Date.parse(r.last) - t0)}</EuiBadge>
          {r.cancelled && <EuiBadge color="default">cancel requested</EuiBadge>}
          {r.storage_unknown && <EuiBadge color="hollow">storage unknown</EuiBadge>}
          {r.truncated?.map((cap) => (
            <EuiBadge key={cap} color="warning" title={'The run’s retained facts were cut under ' + cap}>
              {'cut: ' + cap}
            </EuiBadge>
          ))}
        </div>
        {r.class && (
          <div style={{ display: 'flex', alignItems: 'flex-start', gap: 8 }}>
            <EuiBadge color="danger">{r.class}</EuiBadge>
            {detail && detail.reason && (
              <div className="w-mono w-reason">
                <Segments text={detail.reason} />
                {reasonCut(detail) && <span className="w-muted"> [cut at 1 KiB]</span>}
              </div>
            )}
            {detail && !detail.reason && <span className="w-muted" style={{ fontSize: 12 }}>no reason recorded</span>}
          </div>
        )}
        <EuiDescriptionList
          compressed
          type="column"
          listItems={[
            { title: 'Agent · model', description: or(r.agent) + ' · ' + or(r.model) },
            { title: 'Tenant · host', description: r.tenant + ' · ' + or(r.host) },
            { title: 'Pool · receiver', description: r.pool + ' · ' + or(r.receiver) },
            { title: 'Admitted', description: <span title={abs(r.admitted || r.first)}>{r.admitted ? rel(view.now, r.admitted) : 'no echo in the window; first seen ' + rel(view.now, r.first)}</span> },
          ]}
        />
      </div>
      {error && <EuiCallOut title={error} color="warning" size="s" />}
      <EuiTabs size="s">
        <EuiTab isSelected={shown === 'overview'} onClick={() => setTab('overview')}>
          Overview
        </EuiTab>
        <EuiTab isSelected={shown === 'transcript'} onClick={() => setTab('transcript')}>
          {working ? 'Transcript · live' : 'Transcript'}
        </EuiTab>
        <EuiTab isSelected={shown === 'trace'} onClick={() => setTab('trace')}>
          Trace
        </EuiTab>
      </EuiTabs>
      {shown === 'overview' && detail && <Overview view={view} detail={detail} t0={t0} />}
      {shown === 'overview' && !detail && !error && <span className="w-subdued">Loading the run.</span>}
      {shown === 'transcript' && <TranscriptPane view={view} run={r} attempts={detail?.attempts || []} wide={wide} />}
      {shown === 'trace' && <TraceTab view={view} run={r} />}
    </>
  );
}

// reasonCut says whether the reason, the highest attempt's terminal message, was cut at 1 KiB.
const reasonCut = (detail: RunDetail): boolean => {
  const highest = detail.attempts.reduce<Attempt | null>((top, a) => (top && top.number > a.number ? top : a), null);
  return !!highest?.events.some((e) => e.kind === 'terminal' && e.truncated);
};

interface Step {
  at: string;
  name: string;
  who: string;
  signer?: string;
  message?: string;
  truncated?: boolean;
  tone: 'ok' | 'bad' | 'key' | 'host';
}

// steps is the run's path from its signed facts, each with its signer: admitted by the joined echo, each attempt's
// claim decision, key request and answer or refusal, start failure and events, the host's cancels and the result.
function steps(detail: RunDetail): { head: string; steps: Step[] }[] {
  const echo = detail.echoes.find((e) => e.joined);
  const host = or(detail.host);
  const groups = detail.attempts.map((a, i) => {
    const s: Step[] = [];
    if (i === 0 && echo) {
      s.push({ at: echo.at, name: 'admitted', who: host + ' → ' + echo.receiver, signer: echo.signer, tone: 'ok' });
    }
    if (a.decision) {
      s.push({ at: a.decision.at, name: 'claim decided', who: 'custodian ' + a.decision.custodian + ' · ' + a.decision.reason + (a.decision.served ? '' : ', not served') + (a.decision.previous ? ', before ' + a.decision.previous : ''), signer: a.decision.signer, tone: 'key' });
    }
    for (const o of detail.opens.filter((o) => o.attempt === a.number)) {
      s.push({ at: o.at, name: 'key requested', who: o.receiver, signer: o.signer, tone: 'ok' });
      if (o.answer) {
        s.push({ at: o.answered_at || o.at, name: 'key handed over', who: 'custodian ' + or(o.custodian) + ' · ' + o.answer, signer: o.answer_signer, tone: 'key' });
      } else if (o.reason) {
        s.push({ at: o.answered_at || o.at, name: 'key refused', who: (o.custodian ? 'custodian ' + o.custodian + ' · ' : '') + o.reason, signer: o.answer_signer, tone: 'bad' });
      }
    }
    if (a.start_failure) {
      s.push({ at: a.start_failure.at, name: 'start failed', who: a.receiver + ' · ' + a.start_failure.class + ' · ' + a.start_failure.agent + ' as ' + a.start_failure.profile, signer: a.start_failure.signer, tone: 'bad' });
    }
    let updates = 0;
    for (const e of a.events) {
      const bad = e.state === 'TASK_STATE_FAILED' || e.state === 'TASK_STATE_REJECTED' || e.state === 'TASK_STATE_CANCELED' || !!e.event;
      let who = e.receiver;
      if (e.kind === 'working') {
        who = 'update ' + ++updates;
      } else if (e.kind === 'terminal' || e.kind === 'rejected') {
        who = stateWord(e.state) + (e.class ? ' · ' + e.class : '') + (e.event ? ' · ' + e.event : '');
      }
      s.push({ at: e.at, name: e.kind, who, signer: e.signer, message: e.message, truncated: e.truncated, tone: bad ? 'bad' : 'ok' });
    }
    return { head: 'Attempt ' + a.number + ' · ' + a.receiver, steps: s };
  });
  if (!groups.length && echo) {
    groups.push({ head: 'Admitted', steps: [{ at: echo.at, name: 'admitted', who: host + ' → ' + echo.receiver, signer: echo.signer, tone: 'ok' }] });
  }
  const last = groups[groups.length - 1];
  if (last) {
    for (const c of detail.cancels) {
      last.steps.push({ at: c.at, name: 'cancelled', who: 'by host ' + c.host, signer: c.signer, tone: 'host' });
    }
    if (detail.result) {
      const res = detail.result;
      last.steps.push({ at: res.at, name: 'result', who: 'back to ' + host + ' · ' + res.outputs.length + ' inline, ' + res.s3_outputs.length + ' via S3' + (res.not_returned.length ? ', ' + res.not_returned.length + ' not returned' : ''), signer: res.signer, tone: stateColor(res.state) === 'danger' ? 'bad' : 'ok' });
    }
  }
  for (const g of groups) {
    g.steps.sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
  }
  return groups;
}

const toneColor: Record<Step['tone'], string> = { ok: 'var(--w-success)', bad: 'var(--w-danger)', key: 'var(--w-primary)', host: 'var(--w-grey)' };

function Overview({ view, detail, t0 }: { view: View; detail: RunDetail; t0: number }) {
  const groups = steps(detail);
  const res = detail.result;
  const storage = detail.storage;
  const where = storage ? storage.endpoint.replace(/^https?:\/\//, '') + ' / ' + storage.bucket : detail.storage_unknown ? 'unknown' : 'none';
  let firstShown = true;
  return (
    <>
      <Section title="Path">
        {detail.mismatched_echoes ? <span className="w-subdued">{detail.mismatched_echoes} echoes naming another task were counted, not shown.</span> : null}
        {groups.map((g) => (
          <div key={g.head}>
            {groups.length > 1 && <div className="w-subdued" style={{ fontWeight: 600, padding: '6px 0 2px' }}>{g.head}</div>}
            {g.steps.map((s, i) => {
              const at = Date.parse(s.at);
              const off = firstShown ? s.at.slice(11, 19) : '+' + dur(Math.max(0, at - t0));
              firstShown = false;
              return (
                <div key={i} className={'w-step' + (at > view.t ? ' w-future' : '')}>
                  <span className="w-subdued" title={abs(s.at)}>
                    {off}
                  </span>
                  <span className="w-dot" style={{ background: toneColor[s.tone] }} />
                  <span className="w-row" style={{ gap: '4px 8px' }}>
                    <span>
                      <b style={{ fontWeight: 500 }}>{s.name}</b> {'· ' + s.who}
                    </span>
                    <EuiBadge color="hollow" iconType="lock">
                      {signedBy(s.signer)}
                    </EuiBadge>
                    {s.message && (
                      <span className="w-mono" style={{ fontSize: 12, flexBasis: '100%', whiteSpace: 'pre-wrap', color: s.tone === 'bad' ? 'var(--w-text-danger)' : undefined }}>
                        <Segments text={s.message} />
                        {s.truncated && <span className="w-muted"> [cut at 1 KiB]</span>}
                      </span>
                    )}
                  </span>
                </div>
              );
            })}
          </div>
        ))}
      </Section>
      <Section title="Outputs">
        <span className="w-subdued">
          {res
            ? res.outputs.length + ' inline · ' + res.s3_outputs.length + ' through S3 · ' + res.not_returned.length + ' not returned · reply ' + bytes(res.reply_size) + (res.reply_cut ? ' (cut)' : '') + ' · ' + res.artifacts + ' artifacts'
            : detail.terminal
              ? 'No result recorded.'
              : 'No result yet: the run is still working.'}
        </span>
        {res && (res.outputs.length > 0 || res.s3_outputs.length > 0 || res.not_returned.length > 0) && (
          <div className="w-list">
            {[...res.outputs.map((o) => ({ ...o, via: 'inline' as const })), ...res.s3_outputs.map((o) => ({ ...o, via: 'S3' as const }))].map((o) => (
              <div key={o.via + o.name + o.sha256} className="w-output">
                <OutputName name={o.name} />
                <EuiBadge color={o.via === 'S3' ? 'primary' : 'default'}>{o.via}</EuiBadge>
                <span className="w-mono w-subdued" style={{ fontSize: 11 }} title={o.sha256}>
                  {bytes(o.size) + ' · sha256 ' + o.sha256.slice(0, 8) + '…' + o.sha256.slice(-8)}
                </span>
              </div>
            ))}
            {res.not_returned.map((n, i) => (
              <div key={'nr' + i} className="w-output w-unreturned">
                <OutputName name={n.name} />
                <EuiBadge color="warning">not returned</EuiBadge>
                <span className="w-subdued">{'declared by the run' + (n.size ? ', ' + bytes(n.size) : '') + ' · ' + n.reason}</span>
              </div>
            ))}
          </div>
        )}
        <span className="w-subdued">{'S3 destination: ' + where + (res && !res.s3_outputs.length ? '; this run used none.' : '')}</span>
        {res && (
          <div className="w-row">
            <EuiBadge color="hollow" iconType="lock">
              {'result ' + signedBy(res.signer)}
            </EuiBadge>
            {res.profile && <EuiBadge color="hollow">{'profile ' + res.profile}</EuiBadge>}
          </div>
        )}
      </Section>
    </>
  );
}

// OutputName is a tenant-chosen name, monospace, breaking at each slash; the placeholder for an unsafe one is muted.
const OutputName = ({ name }: { name: string }) =>
  name === '(invalid name)' ? (
    <span className="w-mono w-muted" style={{ fontSize: 13 }}>
      {name}
    </span>
  ) : (
    <span className="w-mono w-name" style={{ fontSize: 13, color: 'var(--w-text-heading)' }}>
      {name.split('/').map((part, i, all) => (
        <span key={i}>
          {part}
          {i < all.length - 1 && (
            <>
              /<wbr />
            </>
          )}
        </span>
      ))}
    </span>
  );

function TraceTab({ view, run }: { view: View; run: RunSummary }) {
  const [forest, setForest] = useState<Forest | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [picked, setPicked] = useState<string | null>(null);
  useEffect(() => {
    setForest(null);
    setError(null);
    setPicked(null);
  }, [run.id]);
  const load = () => {
    setLoading(true);
    getTrace(run.id)
      .then((f) => {
        setForest(f);
        setError(null);
      })
      .catch((err: Error) => setError(err instanceof ApiError && err.status === 503 ? 'The view is busy reading other traces; try again.' : 'trace: ' + err.message))
      .finally(() => setLoading(false));
  };
  if (!forest) {
    return (
      <div className="w-section" style={{ alignItems: 'flex-start' }}>
        <span className="w-subdued">
          Reads this run's spans from <span className="w-mono">{'wherry.traces.' + run.tenant}</span>. Span facts are advisory timing.
        </span>
        <EuiButton size="s" iconType="inspect" onClick={load} isLoading={loading}>
          Load trace
        </EuiButton>
        {error && <EuiCallOut title={error} color="warning" size="s" />}
      </div>
    );
  }
  const flat = flatten(forest);
  if (!flat.length) {
    return (
      <EuiCallOut title="No spans for this run" color="primary" iconType="info" size="s">
        <p>{forest.read.records + ' records read from ' + forest.topic + (forest.complete ? ', scan complete.' : ', scan not complete.')}</p>
        {forest.markers.map((m) => (
          <p key={m}>{markerText(m)}</p>
        ))}
      </EuiCallOut>
    );
  }
  const s0 = Math.min(...flat.map((f) => Date.parse(f.span.start)));
  const sEnd = Math.max(...flat.map((f) => Date.parse(f.span.end)));
  const total = Math.max(1, sEnd - s0);
  const pickedId = picked ?? (flat.find((f) => spanStatus(f.span).error) || flat[0]).span.id;
  const sel = flat.find((f) => f.span.id === pickedId)?.span;
  return (
    <div className="w-section">
      <div className="w-row w-subdued" style={{ gap: 12 }}>
        {Object.keys(serviceColors).map((k) => (
          <span key={k} className="w-row" style={{ gap: 6 }}>
            <span className="w-swatch" style={{ background: serviceColors[k] }} />
            {k}
          </span>
        ))}
        <span className="w-row" style={{ gap: 6 }}>
          <span className="w-swatch" style={{ background: 'var(--w-danger)' }} />
          error
        </span>
        <span style={{ marginLeft: 'auto' }}>{flat.length + ' spans · ' + dur(total)}</span>
      </div>
      <div className="w-waterfall-head">
        <span>Span</span>
        <span style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>0</span>
          <span>{dur(total / 2)}</span>
          <span>{dur(total)}</span>
        </span>
        <span style={{ textAlign: 'right' }}>Duration</span>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {flat.map(({ span, depth }) => {
          const a = Date.parse(span.start) - s0;
          const b = Date.parse(span.end) - s0;
          const status = spanStatus(span);
          return (
            <button key={span.id} type="button" className={'w-span' + (span.id === pickedId ? ' w-selected' : '')} onClick={() => setPicked(span.id)} aria-label={span.name + ', ' + or(span.service) + ', ' + dur(span.duration_ms)}>
              <span className="w-spanname" style={{ paddingLeft: depth * 12 }}>
                {span.name}
              </span>
              <span className="w-track">
                <span className="w-bar" style={{ left: ((100 * a) / total).toFixed(2) + '%', width: 'max(3px, ' + ((100 * (b - a)) / total).toFixed(2) + '%)', background: status.error ? 'var(--w-danger)' : serviceColors[span.service || ''] || 'var(--w-grey)' }} />
              </span>
              <span style={{ textAlign: 'right', fontSize: 12, color: status.error ? 'var(--w-text-danger)' : 'var(--w-text-subdued)' }}>{dur(span.duration_ms)}</span>
            </button>
          );
        })}
      </div>
      {sel && (
        <div style={{ border: '1px solid var(--w-border-subdued)', borderRadius: 8, padding: 12 }} className="w-section">
          <div className="w-row" style={{ justifyContent: 'space-between' }}>
            <span className="w-mono" style={{ fontSize: 13, fontWeight: 500, color: 'var(--w-text-heading)' }}>
              {sel.name}
            </span>
            <EuiHealth color={spanStatus(sel).color} textSize="xs">
              {spanStatus(sel).label}
            </EuiHealth>
          </div>
          <EuiDescriptionList
            compressed
            type="column"
            listItems={[
              { title: 'Service', description: or(sel.service) },
              { title: 'Kind', description: sel.kind },
              { title: 'Start', description: <span title={abs(sel.start)}>{'+' + dur(Date.parse(sel.start) - s0)}</span> },
              { title: 'Duration', description: dur(sel.duration_ms) },
              { title: 'Events', description: String(sel.events || 0) },
              { title: 'Links', description: <span className="w-mono" style={{ fontSize: 12 }}>{sel.links?.length ? sel.links.join(', ') : 'none'}</span> },
              ...(sel.marker ? [{ title: 'Marker', description: markerText(sel.marker) }] : []),
            ]}
          />
          {Object.keys(sel.attributes || {}).length > 0 && (
            <div className="w-list">
              {Object.keys(sel.attributes)
                .sort()
                .map((k) => (
                  <div key={k} className="w-attrs">
                    <span className="w-mono w-subdued">{k}</span>
                    <span className="w-mono">{sel.attributes[k]}</span>
                  </div>
                ))}
            </div>
          )}
        </div>
      )}
      {forest.markers.length > 0 && <EuiCallOut title={forest.markers.map(markerText).join(' ')} color="primary" iconType="info" size="s" />}
      {!forest.complete && (
        <EuiButtonEmpty size="s" iconType="plus" onClick={load} isLoading={loading}>
          Load the rest of the trace
        </EuiButtonEmpty>
      )}
      <span className="w-subdued">{forest.read.records + ' records read' + (view.scrubbing ? '; the trace is not replayed' : '')}</span>
      {error && <EuiCallOut title={error} color="warning" size="s" />}
    </div>
  );
}
