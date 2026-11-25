import { EuiRange } from '@elastic/eui';
import type { View } from './App';
import { abs, clock } from './format';
import { admittedAt, incompleteBefore, recentMs } from './model';

const bins = 96;

// Scrubber runs along the window: a histogram of admissions with the failed share in red, the moment shown, and the
// band before which the window is incomplete. The range is in minutes from the window's start.
export function Scrubber({ view, onScrub }: { view: View; onScrub: (t: number | null) => void }) {
  const m = view.model;
  const w0 = Date.parse(m.window_start);
  const span = Math.max(1, view.now - w0);
  const counts = new Array<number>(bins).fill(0);
  const fails = new Array<number>(bins).fill(0);
  for (const r of m.runs) {
    const b = Math.min(bins - 1, Math.max(0, Math.floor(((admittedAt(r) - w0) / span) * bins)));
    counts[b]++;
    if (r.state === 'TASK_STATE_FAILED') {
      fails[b]++;
    }
  }
  const max = Math.max(1, ...counts);
  const pct = (ms: number) => ((100 * (ms - w0)) / span).toFixed(2) + '%';
  const cut = incompleteBefore(m);
  const ago = (ms: number) => {
    const min = Math.round((view.now - ms) / 60000);
    return min < 60 ? min + ' min ago' : Math.floor(min / 60) + ' h ' + (min % 60) + ' min ago';
  };
  const atText = view.scrubbing ? 'Showing ' + clock(view.t) + ' UTC · ' + ago(view.t) : 'Live · red ring: failed in the last ' + recentMs / 60000 + ' min';
  return (
    <>
      <div className="w-bins">
        {cut !== null && <span className="w-cut" title={'The window is incomplete before ' + clock(cut) + ' UTC'} style={{ width: pct(cut) }} />}
        {counts.map((n, i) => (
          <span key={i} className="w-bin" style={{ left: (100 * i) / bins + '%', width: 'calc(' + (100 / bins).toFixed(3) + '% - 1px)', height: n ? Math.max(2, Math.round((16 * n) / max)) : 0 }}>
            <span style={{ height: (n ? Math.round((100 * fails[i]) / n) : 0) + '%' }} />
          </span>
        ))}
        <span className="w-cursor" style={{ left: 'calc(' + pct(view.t) + ' - 1px)', background: view.scrubbing ? 'var(--w-primary)' : 'var(--w-success)' }} />
      </div>
      <EuiRange
        fullWidth
        compressed
        min={0}
        max={Math.max(1, Math.round(span / 60000))}
        step={1}
        value={Math.round((view.t - w0) / 60000)}
        onChange={(e) => {
          const ms = w0 + Number((e.target as HTMLInputElement).value) * 60000;
          onScrub(ms >= view.now - 30000 ? null : ms);
        }}
        aria-label="Time in the window"
        aria-valuetext={atText}
      />
      <div className="w-row" style={{ justifyContent: 'space-between', fontSize: 11 }}>
        <span className="w-subdued" title={abs(m.window_start)}>
          {clock(w0)} UTC, window start
        </span>
        {cut !== null && <span style={{ color: 'var(--w-warning)' }}>incomplete before {clock(cut)}</span>}
        <span className="w-subdued" title={abs(new Date(view.t).toISOString())} style={view.scrubbing ? { color: 'var(--w-text-heading)', fontWeight: 600 } : undefined}>
          {atText}
        </span>
      </div>
    </>
  );
}
