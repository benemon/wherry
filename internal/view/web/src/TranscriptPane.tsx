import { EuiButton, EuiButtonEmpty, EuiFieldSearch, EuiSwitch } from '@elastic/eui';
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import type { View } from './App';
import { ApiError, getTranscript } from './api';
import { Segments } from './RunDrawer';
import { abs, grouped } from './format';
import type { Query, State, Where } from './transcript';
import { cursor, fail, follow, initial, pageSize, rows, stateText, statusMessages, take } from './transcript';
import type { Attempt, RunSummary } from './types';

interface Props {
  view: View;
  run: RunSummary;
  attempts: Attempt[];
  wide: boolean;
}

// TranscriptPane is the run's agent output with the receiver's status messages in line. It opens at the tail, pages
// back on request and, while the run works, asks from its cursor on each model event and at most every two
// seconds. One request is in flight at a time; a response for a run no longer shown is dropped.
export function TranscriptPane({ view, run, attempts, wide }: Props) {
  const [state, setState] = useState<State>(initial);
  const [following, setFollowing] = useState(true);
  const [statusOnly, setStatusOnly] = useState(false);
  const [q, setQ] = useState('');
  const box = useRef<HTMLDivElement>(null);
  const pending = useRef(false);
  const stateRef = useRef(state);
  stateRef.current = state;
  const runRef = useRef(run.id);
  const working = !run.terminal;

  const ask = useCallback(
    (query: Query, where: Where) => {
      const id = run.id;
      if (pending.current) {
        return;
      }
      pending.current = true;
      getTranscript(id, cursor(stateRef.current, query))
        .then((response) => {
          if (runRef.current !== id) {
            return;
          }
          const step = take(stateRef.current, response, where, query.from ?? 0);
          stateRef.current = step.state;
          setState(step.state);
          if (step.ask) {
            pending.current = false;
            ask(step.ask, step.ask.tail !== undefined ? 'tail' : 'gap');
          }
        })
        .catch((err: Error) => {
          if (runRef.current === id) {
            const next = fail(stateRef.current, err instanceof ApiError ? err.status : 0);
            stateRef.current = next;
            setState(next);
          }
        })
        .finally(() => {
          pending.current = false;
        });
    },
    [run.id],
  );

  useEffect(() => {
    runRef.current = run.id;
    const fresh = initial();
    stateRef.current = fresh;
    setState(fresh);
    setFollowing(true);
    pending.current = false;
    ask({ tail: pageSize }, 'tail');
  }, [run.id, ask]);

  // The follow asks at most every two seconds, the bound docs/operator-view.md states.
  useEffect(() => {
    if (!working) {
      return;
    }
    const tick = () => {
      const query = follow(stateRef.current);
      if (query && !pending.current) {
        ask(query, query.tail !== undefined ? 'tail' : 'from');
      }
    };
    tick();
    const interval = setInterval(tick, 2000);
    return () => clearInterval(interval);
  }, [working, view.model.version, ask]);

  const shown = rows(state.lines, statusMessages(attempts), state.prev !== null)
    .filter((r) => !statusOnly || r.kind !== 'line')
    .filter((r) => !q || r.kind === 'divider' || r.text.toLowerCase().includes(q.trim().toLowerCase()))
    .filter((r) => !view.scrubbing || Date.parse(r.at) <= view.t);

  // Following keeps the newest line in view; a scroll up pauses it.
  const count = shown.length;
  useLayoutEffect(() => {
    const el = box.current;
    if (el && following) {
      el.scrollTop = el.scrollHeight;
    }
  }, [count, following]);
  const onScroll = () => {
    const el = box.current;
    if (!el || !working) {
      return;
    }
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
    if (!atBottom && following) {
      setFollowing(false);
    }
  };

  const agentN = state.lines.length;
  const statusN = shown.filter((r) => r.kind === 'status').length;
  const chunks = attempts.length && attempts.every((a) => a.transcript_chunks) ? attempts.reduce((n, a) => n + (a.transcript_chunks || 0), 0) : null;
  const loading = !state.opened;
  const status = stateText(state, working);
  return (
    <div className="w-section">
      <div className="w-row" style={{ justifyContent: 'space-between' }}>
        <EuiFieldSearch placeholder="Search transcript" compressed value={q} onChange={(e) => setQ(e.target.value)} aria-label="Search transcript" style={{ minWidth: 200 }} />
        <EuiSwitch label="Status messages only" checked={statusOnly} onChange={(e) => setStatusOnly(e.target.checked)} compressed />
      </div>
      <div className="w-row" style={{ justifyContent: 'space-between', minHeight: 32 }}>
        <span className="w-subdued">
          {(statusOnly || q ? shown.length + ' shown · ' : '') + grouped(agentN) + ' lines loaded, ' + statusN + ' status' + (state.evicted ? ' · ' + state.evicted + ' other indexes evicted' : '') + (view.scrubbing ? ' · as at ' + abs(new Date(view.t).toISOString()).slice(11, 19) + ' UTC' : status ? ' · ' + status : '')}
        </span>
        {working && !view.scrubbing && (
          <span className="w-row">
            <EuiSwitch label="Follow" checked={following} onChange={(e) => setFollowing(e.target.checked)} compressed />
            {!following && (
              <EuiButton size="s" iconType="sortDown" onClick={() => setFollowing(true)}>
                Jump to latest
              </EuiButton>
            )}
          </span>
        )}
      </div>
      <div ref={box} className="w-transcript" style={{ height: wide ? 560 : 440 }} onScroll={onScroll}>
        {loading && (
          <div style={{ padding: '24px 16px', display: 'flex', flexDirection: 'column', gap: 4, fontFamily: 'var(--w-font-sans)', fontSize: 13, lineHeight: '20px', color: 'var(--w-text-subdued)' }}>
            <b style={{ color: 'var(--w-text-heading)', fontWeight: 500 }}>Reading this run's transcript</b>
            <span>The first open scans the run's span of the tenant's transcript topic once. Later pages are fast.</span>
          </div>
        )}
        {!loading && state.prev !== null && (
          <div className="w-earlier">
            <EuiButtonEmpty size="xs" iconType="sortUp" onClick={() => ask({ before: state.prev!, limit: pageSize }, 'before')}>
              Load earlier lines
            </EuiButtonEmpty>
            <span className="w-subdued" style={{ fontSize: 11 }}>
              {'before seq ' + grouped(state.lines[0]?.seq ?? 0) + (chunks !== null ? ' · about ' + grouped(Math.max(0, chunks - agentN)) + ' of ' + grouped(chunks) + ' lines not loaded' : '')}
            </span>
          </div>
        )}
        {!loading &&
          shown.map((r, i) =>
            r.kind === 'divider' ? (
              <div key={'d' + i} role="separator" className="w-divider">
                {r.text}
              </div>
            ) : (
              <div key={r.kind + (r.seq ?? '') + r.at + i} className={'w-line' + (r.kind === 'status' ? ' w-status' : '')}>
                <span className="w-subdued" title={'reached Kafka at ' + abs(r.at)}>
                  {r.at.slice(11, 19)}
                </span>
                <span className="w-subdued" title={'attempt ' + r.attempt + ' on ' + r.receiver + (r.kind === 'status' ? ' · status message' : ' · agent output, seq ' + grouped(r.seq || 0))}>
                  {r.attempt + '·' + r.receiver}
                </span>
                <bdi className="w-text">
                  {r.muted ? (
                    <span className="w-muted">{r.muted}</span>
                  ) : (
                    <>
                      <Segments text={r.text} q={q} />
                      {r.truncated && <span className="w-muted">{' [cut at 16 KiB of ' + grouped(r.size || 0) + ']'}</span>}
                    </>
                  )}
                </bdi>
              </div>
            ),
          )}
        {!loading && shown.length === 0 && (
          <div style={{ padding: 16, fontSize: 13, color: 'var(--w-text-subdued)' }}>
            {state.missing ? 'The tenant has no transcript topic.' : state.retry ? 'Indexing the run’s transcript.' : q ? 'No lines match "' + q.trim() + '".' : statusOnly ? 'No status messages yet.' : view.scrubbing ? 'No output had reached Kafka by then.' : 'No output yet.'}
          </div>
        )}
      </div>
      <span className="w-subdued">
        Plain text exactly as the receiver redacted it. Times are when each line reached Kafka. <span className="w-redacted">[redacted:&hellip;]</span> marks a credential the receiver removed.
      </span>
    </div>
  );
}
