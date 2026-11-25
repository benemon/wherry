import type { BoundHit, Custodian, Model, Pool, Receiver, RunSummary } from './types';
import type { HealthColor } from './format';

// recentMs is the design setting for a red ring: a failure on the node within this long of the moment shown.
export const recentMs = 15 * 60 * 1000;

export const admittedAt = (r: RunSummary): number => Date.parse(r.admitted || r.first);

// replay is the runs as they stood at t: admitted by t, and in flight unless terminal by t. At the live moment the
// list is the server's own.
export function replay(runs: RunSummary[], t: number | null): RunSummary[] {
  if (t === null) {
    return runs;
  }
  const out: RunSummary[] = [];
  for (const r of runs) {
    if (admittedAt(r) > t) {
      continue;
    }
    if (r.terminal && Date.parse(r.last) <= t) {
      out.push(r);
      continue;
    }
    out.push({ ...r, state: 'TASK_STATE_WORKING', terminal: false, class: undefined, stage: 'working', last: new Date(Math.min(Date.parse(r.last), t)).toISOString() });
  }
  return out;
}

export interface Tally {
  hostRuns: Record<string, number>;
  hostWorking: Record<string, number>;
  receiverWorking: Record<string, number>;
  receiverFailed: Record<string, number>;
  poolFailed: Record<string, number>;
  recentHost: Record<string, true>;
  recentReceiver: Record<string, true>;
  recentPool: Record<string, true>;
  byState: Record<string, number>;
}

// The key a run with no host tallies under.
export const noHost = '?';

// tally is one pass over the runs for every per-node number the map needs; recent failures are those within recentMs
// of t, the moment shown.
export function tally(runs: RunSummary[], t: number): Tally {
  const out: Tally = { hostRuns: {}, hostWorking: {}, receiverWorking: {}, receiverFailed: {}, poolFailed: {}, recentHost: {}, recentReceiver: {}, recentPool: {}, byState: {} };
  const inc = (m: Record<string, number>, k: string) => {
    m[k] = (m[k] || 0) + 1;
  };
  for (const r of runs) {
    const h = r.host || noHost;
    const rcv = r.receiver || '';
    inc(out.hostRuns, h);
    inc(out.byState, r.state);
    if (!r.terminal) {
      inc(out.hostWorking, h);
      inc(out.receiverWorking, rcv);
    }
    if (r.state === 'TASK_STATE_FAILED') {
      inc(out.receiverFailed, rcv);
      inc(out.poolFailed, r.pool);
      if (t - Date.parse(r.last) <= recentMs) {
        out.recentHost[h] = true;
        out.recentReceiver[rcv] = true;
        out.recentPool[r.pool] = true;
      }
    }
  }
  return out;
}

export interface Health {
  color: HealthColor;
  label: string;
}

// receiverHealth is a receiver's ring, in the legend's order of severity; the gate is the server's own word.
export function receiverHealth(r: Receiver, pool: Pool | undefined, custodian: Custodian | undefined, tally: Tally): Health {
  if (r.status === 'ended' || r.ended) {
    return { color: 'subdued', label: 'ended' };
  }
  if (r.status === 'silent') {
    return { color: 'danger', label: 'silent: never advertised' };
  }
  if (r.status === 'stale') {
    return { color: 'danger', label: 'stale: not advertising' };
  }
  if (tally.recentReceiver[r.name]) {
    return { color: 'danger', label: 'failed runs in the last 15 min' };
  }
  if (r.gated) {
    return { color: 'warning', label: 'gated: ' + r.gated };
  }
  if (pool?.suspended) {
    return { color: 'warning', label: 'pool suspended' };
  }
  if (custodian && custodian.receiver === r.name && custodian.suspended) {
    return { color: 'warning', label: 'custodian suspended' };
  }
  if (r.status === 'live') {
    return { color: 'success', label: 'live' };
  }
  return { color: 'subdued', label: r.status };
}

// poolHealth is the pool's own line: the worst of its receivers' states, then its own suspension.
export function poolHealth(pool: Pool, receivers: Receiver[], custodian: Custodian | undefined, tally: Tally): Health {
  if (tally.recentPool[pool.name]) {
    return { color: 'danger', label: 'failed runs in the last 15 min' };
  }
  const notAdvertising = receivers.find((r) => r.status === 'stale' || r.status === 'silent');
  if (notAdvertising) {
    return { color: 'danger', label: notAdvertising.name + ' is not advertising' };
  }
  if (pool.suspended) {
    return { color: 'warning', label: 'suspended' };
  }
  if (custodian?.suspended) {
    return { color: 'warning', label: 'custodian suspended' };
  }
  const gated = receivers.find((r) => r.gated);
  if (gated) {
    return { color: 'warning', label: gated.name + ' gated: ' + gated.gated };
  }
  return { color: 'success', label: 'ready' };
}

// The caps that drop whole runs from the window, as docs/operator-view.md lists them; the run_* caps cut one run and
// show on its truncated, the rest cut a response or a scan.
const runDroppingCaps = ['runs', 'events', 'attempts', 'echoes', 'cancels', 'opens', 'bytes'];

export interface Bounds {
  cut: boolean;
  cutBy: string[];
  other: string[];
}

export function bounds(hits: BoundHit[]): Bounds {
  const cutBy = hits.filter((b) => runDroppingCaps.includes(b.name)).map((b) => b.name);
  const other = hits.filter((b) => !runDroppingCaps.includes(b.name)).map((b) => b.name);
  return { cut: cutBy.length > 0, cutBy, other };
}

// windowNote is the banner over the map.
export function windowNote(m: Model): string {
  const b = bounds(m.bounds || []);
  const parts: string[] = [];
  if (b.cut) {
    parts.push('The window is cut short: runs were dropped under ' + b.cutBy.join(', ') + '. Counts cover the kept runs.');
  }
  if (m.truncated) {
    parts.push('Only the newest ' + m.runs.length.toLocaleString('en-GB') + ' of ' + m.runs_total.toLocaleString('en-GB') + ' runs are listed: the list was cut to fit the response.');
  }
  if (b.other.length) {
    parts.push('Limits hit: ' + b.other.join(', ') + '.');
  }
  return parts.join(' ');
}

// incompleteBefore is the moment before which the window is incomplete: the oldest kept run when runs were dropped,
// or the window's start when a topic predates the log; null when nothing marks the window.
export function incompleteBefore(m: Model): number | null {
  if (bounds(m.bounds || []).cut && m.runs.length) {
    return m.runs.reduce((min, r) => Math.min(min, admittedAt(r)), Infinity);
  }
  if ((m.topics || []).some((t) => t.predates_log)) {
    return Date.parse(m.window_start);
  }
  return null;
}

export const lagText = (p: Pool): string => {
  switch (p.lag.status) {
    case 'ok':
      return p.lag.records + ' records behind';
    case 'zero':
      return 'none';
    case 'unavailable':
      return 'unavailable: the broker did not report it';
    case 'error':
      return 'error reading lag' + (p.lag.error ? ': ' + p.lag.error : '');
    default:
      return '[other]';
  }
};

export const lagShort = (p: Pool): string =>
  p.lag.status === 'ok' || p.lag.status === 'zero' ? 'lag ' + (p.lag.records || 0) : 'lag ' + p.lag.status;

// byStateOf is the counts by state the drawers show: the server's at the live moment, recomputed from the replayed
// runs while scrubbing.
export function byStateOf(server: Record<string, number> | undefined, runs: RunSummary[], scrubbing: boolean): Record<string, number> {
  if (!scrubbing) {
    return server || {};
  }
  const out: Record<string, number> = {};
  for (const r of runs) {
    out[r.state] = (out[r.state] || 0) + 1;
  }
  return out;
}

export function byClassOf(server: Record<string, number> | undefined, runs: RunSummary[], scrubbing: boolean): Record<string, number> {
  if (!scrubbing) {
    return server || {};
  }
  const out: Record<string, number> = {};
  for (const r of runs) {
    if (r.class) {
      out[r.class] = (out[r.class] || 0) + 1;
    }
  }
  return out;
}

export interface Filters {
  q?: string;
  state?: string;
  tenant?: string;
  pool?: string;
  receiver?: string;
  host?: string;
  agent?: string;
}

export const filterKeys = ['state', 'tenant', 'pool', 'receiver', 'host', 'agent'] as const;
export type FilterKey = (typeof filterKeys)[number];

// none is the filter value for a run with no host, receiver or agent.
export const none = '__none';

export const filterValue = (r: RunSummary, k: FilterKey): string => r[k] || none;

export function matches(r: RunSummary, f: Filters, skip?: FilterKey): boolean {
  for (const k of filterKeys) {
    if (k !== skip && f[k] && filterValue(r, k) !== f[k]) {
      return false;
    }
  }
  return !f.q || r.id.includes(f.q.trim());
}

export const activeFilters = (f: Filters): number => filterKeys.filter((k) => f[k]).length + (f.q ? 1 : 0);
