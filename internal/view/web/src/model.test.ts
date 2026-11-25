import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { bounds, byStateOf, incompleteBefore, lagText, matches, receiverHealth, replay, tally, windowNote } from './model';
import type { Model, RunDetail } from './types';

const read = <T>(name: string): T => JSON.parse(readFileSync(new URL('../fixtures/' + name, import.meta.url), 'utf8')) as T;

const model = read<Model>('model.json');
const sample = read<Model>('sample-model.json');
const failed = read<RunDetail>('run-failed.json');

const now = Date.parse(model.now);
const custodian = (pool: string) => model.custodians.find((c) => c.pool === pool);
const pool = (name: string) => model.pools.find((p) => p.name === name);
const receiver = (name: string) => model.receivers.find((r) => r.name === name)!;

describe('receiver health', () => {
  const t = tally(model.runs, now);

  it('reads the gate from receivers[].gated, not from the offers', () => {
    const r = receiver('nomad-1');
    expect(r.gated).toBe('draining');
    expect(r.offers.every((o) => !('gated' in o))).toBe(true);
    // nomad-1 has a failed run 3 minutes ago, so its ring is red; with that failure aged out it is amber for its gate.
    expect(receiverHealth(r, pool('nomad'), custodian('nomad'), t).color).toBe('danger');
    const later = tally(model.runs, now + 20 * 60 * 1000);
    expect(receiverHealth(r, pool('nomad'), custodian('nomad'), later)).toEqual({ color: 'warning', label: 'gated: draining' });
    const ungated = { ...r, gated: undefined };
    expect(receiverHealth(ungated, pool('openshift'), undefined, later)).toEqual({ color: 'success', label: 'live' });
  });

  it('is red for stale and silent, with the silent one never advertised', () => {
    const t0 = tally([], now);
    expect(receiverHealth(receiver('openshift-1'), pool('openshift'), undefined, t0).color).toBe('danger');
    const silent = receiver('systemd-1');
    expect(silent.last_advertised).toBeUndefined();
    expect(receiverHealth(silent, pool('systemd'), custodian('systemd'), t0)).toEqual({ color: 'danger', label: 'silent: never advertised' });
  });

  it('is amber for a suspended pool or custodian and grey when ended', () => {
    const t0 = tally([], now);
    expect(receiverHealth({ ...receiver('systemd-1'), status: 'live' }, pool('systemd'), custodian('systemd'), t0).color).toBe('warning');
    expect(receiverHealth({ ...receiver('nomad-1'), gated: undefined }, { ...pool('nomad')!, suspended: false }, custodian('nomad'), t0)).toEqual({ color: 'warning', label: 'custodian suspended' });
    expect(receiverHealth(receiver('nomad-2'), pool('nomad'), custodian('nomad'), t0).color).toBe('subdued');
  });

  it('counts a failure as recent only within 15 minutes of the moment shown', () => {
    expect(t.recentReceiver['nomad-1']).toBe(true);
    expect(t.recentReceiver['openshift-1']).toBeUndefined();
    expect(t.receiverFailed['openshift-1']).toBe(1);
  });
});

describe('bounds', () => {
  it('tells the caps that drop runs from the rest', () => {
    expect(bounds(model.bounds)).toEqual({ cut: true, cutBy: ['runs'], other: ['transcript_scan'] });
    expect(bounds([{ name: 'transcript_scan', hits: 1 }, { name: 'sessions', hits: 2 }]).cut).toBe(false);
    expect(bounds([{ name: 'bytes', hits: 1 }]).cutBy).toEqual(['bytes']);
  });

  it('marks the window incomplete before the oldest kept run, or from its start when a topic predates the log', () => {
    const oldest = Math.min(...model.runs.map((r) => Date.parse(r.admitted || r.first)));
    expect(incompleteBefore(model)).toBe(oldest);
    expect(incompleteBefore({ ...model, bounds: [] })).toBe(Date.parse(model.window_start));
    expect(incompleteBefore({ ...model, bounds: [], topics: [] })).toBeNull();
  });

  it('says runs_total is the runs kept and shows the truncated list as newest N of total', () => {
    expect(model.truncated).toBe(true);
    expect(model.runs_total).toBe(model.runs.length);
    const note = windowNote(model);
    expect(note).toContain('cut short');
    expect(note).toContain('Only the newest 7 of 7 runs are listed');
    expect(note).toContain('Limits hit: transcript_scan');
    expect(windowNote(sample)).toBe('');
  });
});

describe('contract states', () => {
  it('shows pool no_terminal, lag error and unavailable', () => {
    expect(pool('nomad')!.no_terminal).toBe(1);
    expect(lagText(pool('openshift')!)).toBe('error reading lag: timeout');
    expect(lagText(pool('systemd')!)).toBe('unavailable: the broker did not report it');
  });

  it('carries the topic states and the model errors', () => {
    expect(model.topics.map((t) => t.missing || t.predates_log || t.error || false)).toEqual([false, true, true, 'timeout']);
    expect(model.errors).toEqual(['timeout']);
  });

  it('takes the failure reason from the run detail with its truncated flag', () => {
    expect(failed.reason).toMatch(/^agent exited/);
    const terminal = failed.attempts[1].events.find((e) => e.kind === 'terminal')!;
    expect(terminal.truncated).toBe(true);
    expect(failed.result!.not_returned).toEqual([
      { name: 'reports/integration/report.md', size: 2048, reason: 'missing' },
      { name: '(invalid name)', size: 0, reason: 'invalid name' },
    ]);
    expect(failed.storage_unknown).toBe(true);
    expect(failed.cancels).toHaveLength(1);
    expect(failed.attempts[0].start_failure?.class).toBe('dispatch');
    expect(failed.opens[0].answer).toBeUndefined();
    expect(failed.opens[0].reason).toBe('pool suspended');
  });
});

describe('scrubbing', () => {
  it('replays the runs admitted by t and not yet terminal, and recomputes the counts from them', () => {
    const t = now - 5 * 60 * 1000;
    const at = replay(model.runs, t);
    const failedRun = at.find((r) => r.id === failed.id)!;
    expect(failedRun.state).toBe('TASK_STATE_WORKING');
    expect(failedRun.terminal).toBe(false);
    expect(at.find((r) => r.id === 'run-5555555555555555555555555555555e')?.state).toBe('TASK_STATE_WORKING');
    expect(replay(model.runs, now - 38 * 60 * 1000).map((r) => r.id)).not.toContain(model.runs[0].id);
    expect(byStateOf(pool('nomad')!.by_state, at, false)).toBe(pool('nomad')!.by_state);
    expect(byStateOf(pool('nomad')!.by_state, at.filter((r) => r.pool === 'nomad'), true)).toEqual({ TASK_STATE_WORKING: 3, TASK_STATE_CANCELED: 1, TASK_STATE_REJECTED: 1 });
    expect(replay(model.runs, null)).toBe(model.runs);
  });
});

describe('the sample model', () => {
  it('loads and filters', () => {
    expect(sample.runs).toHaveLength(36);
    expect(sample.runs.filter((r) => matches(r, { state: 'TASK_STATE_FAILED' }))).toHaveLength(10);
    expect(sample.runs.filter((r) => matches(r, { host: 'mbp', q: 'run-' })).length).toBeGreaterThan(0);
    const t = tally(sample.runs, Date.parse(sample.now));
    expect(t.byState.TASK_STATE_COMPLETED).toBe(25);
    for (const r of sample.receivers) {
      expect(['success', 'danger', 'warning', 'subdued']).toContain(receiverHealth(r, sample.pools.find((p) => p.name === r.pool), sample.custodians.find((c) => c.pool === r.pool), t).color);
    }
  });
});
