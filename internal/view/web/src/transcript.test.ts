import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { cursor, fail, follow, initial, rows, segments, settle, stateText, statusMessages, take } from './transcript';
import type { RunDetail, Transcript } from './types';

const read = <T>(name: string): T => JSON.parse(readFileSync(new URL('../fixtures/' + name, import.meta.url), 'utf8')) as T;

const tail = read<Transcript>('transcript-tail.json');
const reset = read<Transcript>('transcript-reset.json');
const retry = read<Transcript>('transcript-retry.json');
const predates = read<Transcript>('transcript-predates.json');
const missing = read<Transcript>('transcript-missing.json');
const caps = read<Transcript>('transcript-caps.json');
const failed = read<RunDetail>('run-failed.json');

describe('the transcript cursor', () => {
  it('opens from the tail and keeps the cursor', () => {
    const { state, ask } = take(initial(), tail, 'tail');
    expect(ask).toBeUndefined();
    expect(state.lines).toHaveLength(9);
    expect(state.attempt).toBe(2);
    expect(state.first).toBe(120);
    expect(state.prev).toBe(0);
    expect(state.next).toBeNull();
    expect(state.complete).toBe(true);
    expect(cursor(state, { before: 0 }).toString()).toBe('before=0&limit=200&attempt=2&first=120');
    expect(cursor(state, { tail: 200 }).toString()).toBe('tail=200');
  });

  it('reloads from the tail on a reset', () => {
    const opened = take(initial(), tail, 'tail').state;
    const { state, ask } = take(opened, reset, 'from');
    expect(ask).toEqual({ tail: 200 });
    expect(state.lines).toEqual([]);
    expect(state.attempt).toBe(0);
    expect(state.first).toBeNull();
    expect(state.next).toBeNull();
  });

  it('shows nothing on a retry and asks the tail again', () => {
    const { state, ask } = take(initial(), retry, 'tail');
    expect(ask).toBeUndefined();
    expect(state.retry).toBe(true);
    expect(state.lines).toEqual([]);
    expect(follow(state)).toEqual({ tail: 200 });
    expect(stateText(state, true)).toMatch(/indexing/);
  });

  it('has a state for predates_log, missing, a 503 and the caps', () => {
    const p = take(initial(), predates, 'tail').state;
    expect(p.predatesLog).toBe(true);
    expect(stateText(p, true)).toMatch(/retention removed/);
    const m = take(initial(), missing, 'tail').state;
    expect(m.missing).toBe(true);
    expect(stateText(m, false)).toMatch(/no transcript topic/);
    expect(follow(m)).toBeNull();
    const busy = fail(p, 503);
    expect(busy.error).toBe('busy');
    expect(stateText(busy, true)).toMatch(/busy/);
    expect(fail(p, 502).error).toBe('error');
    const c = take(initial(), caps, 'tail').state;
    expect(c.truncated).toEqual(['transcript_bytes', 'response_bytes']);
    expect(c.evicted).toBe(1);
    expect(stateText(c, true)).toContain('cut by transcript_bytes, response_bytes');
    expect(follow(c)).toEqual({ from: 48, limit: 1000 });
  });

  it('merges older and later pages by seq and fills the gap a cut older page leaves', () => {
    // Opened at the tail of attempt 2 alone: the first known seq is 120.
    const opened = take(initial(), { ...tail, lines: tail.lines.slice(2), prev: 120 }, 'tail').state;
    expect(opened.lines[0].seq).toBe(120);
    const older: Transcript = { ...tail, lines: [{ at: tail.lines[0].at, attempt: 1, receiver: 'wherry.rcv.nomad-1', seq: 10, text: 'older' }], prev: undefined, next: 50 };
    const { state, ask } = take(opened, older, 'before');
    expect(ask).toEqual({ from: 50, limit: 200 });
    expect(state.prev).toBeNull();
    expect(state.lines.map((l) => l.seq).slice(0, 2)).toEqual([10, 120]);
    const gap: Transcript = { ...tail, lines: [{ at: tail.lines[0].at, attempt: 1, receiver: 'wherry.rcv.nomad-1', seq: 50, text: 'gap' }, tail.lines[1]], next: undefined };
    const filled = take(state, gap, 'gap', 50);
    expect(filled.ask).toBeUndefined();
    expect(filled.state.lines.map((l) => l.seq).slice(0, 4)).toEqual([10, 50, 64, 120]);
    // A gap page cut short before reaching a known line asks on from its next.
    const cut = take(state, { ...gap, lines: [gap.lines[0]], next: 60 }, 'gap', 50);
    expect(cut.ask).toEqual({ from: 60, limit: 200 });
    const later: Transcript = { ...tail, lines: [{ at: tail.lines[8].at, attempt: 2, receiver: 'wherry.rcv.nomad-1', seq: 30000, text: 'later' }], next: 30006, complete: false };
    const more = take(filled.state, later, 'from').state;
    expect(more.next).toBe(30006);
    expect(more.lines[more.lines.length - 1].seq).toBe(30000);
    expect(follow(more)).toEqual({ from: 30006, limit: 1000 });
  });
});

describe('a response for another run', () => {
  it('is dropped when the run shown changed while the request was in flight', () => {
    // Run A's tail was asked, then run B was opened: A's response arrives against B's fresh state.
    const a = tail.run;
    const b = caps.run;
    expect(a).not.toBe(b);
    const opened = settle(a, initial(), tail, 'tail');
    expect(opened?.state.lines).toHaveLength(9);
    const fresh = initial();
    expect(settle(b, fresh, tail, 'tail')).toBeNull();
    expect(fresh.lines).toEqual([]);
    // The reverse: B's page arriving while A is shown is dropped too, and B's own is applied.
    expect(settle(a, opened!.state, caps, 'from')).toBeNull();
    expect(settle(b, fresh, caps, 'tail')?.state.lines).toHaveLength(1);
  });
});

describe('transcript rows', () => {
  it('names the receiver, marks cut, binary, oversize and empty lines, and divides attempts', () => {
    const shown = rows(tail.lines, statusMessages(failed.attempts), false);
    const lines = shown.filter((r) => r.kind === 'line');
    expect(lines.every((l) => l.receiver === 'nomad-1')).toBe(true);
    expect(lines.find((l) => l.seq === 168)?.muted).toBe('binary line, 4,096 bytes');
    expect(lines.find((l) => l.seq === 4265)?.muted).toBe('line not stored, 734,003 bytes (over 512 KiB)');
    expect(lines.find((l) => l.seq === 4400)?.muted).toBe('(empty line)');
    expect(lines.find((l) => l.seq === 4401)).toMatchObject({ truncated: true, size: 20000 });
    expect(shown.filter((r) => r.kind === 'divider').map((r) => r.text)).toEqual(['attempt 2 on nomad-1 continues from here']);
    expect(lines.find((l) => l.seq === 64)?.muted).toBeUndefined();
  });

  it('interleaves the status messages by time and keeps the terminal message in line', () => {
    const shown = rows(tail.lines, statusMessages(failed.attempts), false);
    const status = shown.filter((r) => r.kind === 'status');
    expect(status.map((s) => s.text)).toEqual(['running make integration against [redacted-url]', failed.reason]);
    expect(status[1].truncated).toBe(true);
    const terminalAt = shown.findIndex((r) => r.kind === 'status' && r.text === failed.reason);
    const lastLine = shown.findIndex((r) => r.kind === 'line' && r.seq === 24402);
    expect(terminalAt).toBeGreaterThan(lastLine);
    expect(rows(tail.lines.slice(2), statusMessages(failed.attempts), true).filter((r) => r.kind === 'status')).toHaveLength(2);
  });

  it('splits redaction markers and search hits as segments', () => {
    const line = tail.lines.find((l) => l.seq === 4266)!.text!;
    const segs = segments(line, 'acme');
    expect(segs.map((s) => s.kind)).toEqual(['text', 'redacted', 'text', 'hit', 'text']);
    expect(segs[1].text).toBe('[redacted:Zm9vYmFyYmF6cXV4]');
    expect(segments('see [redacted-url] now', '')).toEqual([{ text: 'see ', kind: 'text' }, { text: '[redacted-url]', kind: 'redacted' }, { text: ' now', kind: 'text' }]);
  });
});
