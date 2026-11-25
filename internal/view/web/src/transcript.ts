import type { Attempt, Line, Transcript } from './types';
import { grouped } from './format';

// pageSize is the lines one request asks for; followSize the lines a follow asks from the cursor.
export const pageSize = 200;
const followSize = 1000;

// Query is one transcript request: tail=N, or before or from with limit and the cursor's attempt and first.
export interface Query {
  tail?: number;
  before?: number;
  from?: number;
  limit?: number;
}

// State is the transcript shown: its cursor, the lines merged by seq, and what the last response said. retry is a
// tail not indexed yet; error is the last request's failure, busy a 503, cleared by the next response.
export interface State {
  attempt: number;
  first: number | null;
  next: number | null;
  prev: number | null;
  complete: boolean;
  retry: boolean;
  predatesLog: boolean;
  missing: boolean;
  truncated: string[];
  evicted: number;
  lines: Line[];
  error: 'busy' | 'error' | null;
  opened: boolean;
}

export const initial = (): State => ({ attempt: 0, first: null, next: null, prev: null, complete: false, retry: false, predatesLog: false, missing: false, truncated: [], evicted: 0, lines: [], error: null, opened: false });

// cursor adds the cursor's attempt and first to a before or from query.
export function cursor(s: State, q: Query): URLSearchParams {
  const out = new URLSearchParams();
  if (q.tail !== undefined) {
    out.set('tail', String(q.tail));
    return out;
  }
  if (q.before !== undefined) {
    out.set('before', String(q.before));
  }
  if (q.from !== undefined) {
    out.set('from', String(q.from));
  }
  out.set('limit', String(q.limit ?? pageSize));
  out.set('attempt', String(s.attempt));
  if (s.first !== null) {
    out.set('first', String(s.first));
  }
  return out;
}

// merge folds fresh lines into the known ones by seq, in seq order without duplicates, a fresh line replacing a
// known one at its seq.
function merge(known: Line[], fresh: Line[]): Line[] {
  const bySeq = new Map<number, Line>();
  for (const l of known) {
    bySeq.set(l.seq, l);
  }
  for (const l of fresh) {
    bySeq.set(l.seq, l);
  }
  return [...bySeq.values()].sort((a, b) => a.seq - b.seq);
}

export type Where = 'tail' | 'before' | 'from' | 'gap';

export interface Step {
  state: State;
  // ask is the request that follows at once: the tail after a reset, or the lines of a gap left by a cut page.
  ask?: Query;
}

// take folds a response in. A reset says the attempt or first differs from the cursor's: everything known is
// dropped and the tail asked again. A retry says the tail is not indexed yet: nothing is shown and the follow asks
// tail again. A whole older page's next is the seq asked before; one cut short has a lower next, and the lines
// between it and the first known one are asked from there.
export function take(s: State, r: Transcript, where: Where, asked = 0): Step {
  if (r.reset) {
    return { state: { ...initial(), opened: true }, ask: { tail: pageSize } };
  }
  const state: State = { ...s, opened: true, error: null, attempt: r.attempt, first: r.first, complete: r.complete, retry: r.retry === true, predatesLog: r.predates_log === true, missing: r.missing === true, truncated: r.truncated || [], evicted: r.evicted || 0 };
  if (r.retry) {
    return { state: { ...state, lines: [], prev: null, next: null, complete: false } };
  }
  if (where === 'tail') {
    return { state: { ...state, lines: r.lines, prev: r.prev ?? null, next: r.next ?? null } };
  }
  const firstKnown = s.lines.length ? s.lines[0].seq : null;
  state.lines = merge(s.lines, r.lines);
  if (where === 'before') {
    state.prev = r.prev ?? null;
    if (r.next !== undefined && firstKnown !== null && r.next < firstKnown) {
      return { state, ask: { from: r.next, limit: pageSize } };
    }
    return { state };
  }
  if (where === 'gap') {
    const known = s.lines.filter((l) => l.seq >= asked);
    const reached = r.lines.some((l) => known.some((k) => k.seq === l.seq));
    if (r.next !== undefined && (s.next === null || r.next > s.next)) {
      state.next = r.next;
    }
    if (!reached && r.next !== undefined && known.length && r.next < known[0].seq) {
      return { state, ask: { from: r.next, limit: pageSize } };
    }
    return { state };
  }
  state.next = r.next ?? null;
  return { state };
}

// settle applies a response to the run shown, or drops it: a response names its run, and one that arrives after
// another run was opened is for a transcript no longer on the page.
export function settle(shown: string, s: State, r: Transcript, where: Where, asked = 0): Step | null {
  if (r.run !== shown) {
    return null;
  }
  return take(s, r, where, asked);
}

// fail records a request's failure; a 503 is the server's scan slots all taken, asked again on the next follow.
export const fail = (s: State, status: number): State => ({ ...s, opened: true, error: status === 503 ? 'busy' : 'error' });

// follow is the query a follow asks while the run may have more lines: from the cursor, or the tail again while
// none is indexed.
export function follow(s: State): Query | null {
  if (s.complete) {
    return null;
  }
  if (s.next === null) {
    return { tail: pageSize };
  }
  return { from: s.next, limit: followSize };
}

// receiverName strips the principal prefix a line's receiver carries.
const receiverName = (principal: string): string => principal.replace(/^wherry\.rcv\./, '');

// Shown is one row of the transcript: an agent line, a status message from the run's events, or a divider where
// the agent lines move to another attempt.
interface Shown {
  kind: 'line' | 'status' | 'divider';
  at: string;
  attempt: number;
  receiver: string;
  seq?: number;
  text: string;
  muted?: string;
  truncated?: boolean;
  size?: number;
}

// mutedText is the marker shown for a line that is not text: binary, not stored, or empty.
function mutedText(l: Line): string {
  if (l.oversize) {
    return 'line not stored, ' + grouped(l.size || 0) + ' bytes (over 512 KiB)';
  }
  if (l.raw) {
    return 'binary line, ' + grouped(l.size || 0) + ' bytes';
  }
  if (l.text === '' || l.text === undefined) {
    return '(empty line)';
  }
  return '';
}

export const statusMessages = (attempts: Attempt[]): Shown[] =>
  attempts
    .flatMap((a) => a.events.filter((e) => e.message).map((e): Shown => ({ kind: 'status', at: e.at, attempt: a.number, receiver: e.receiver || a.receiver, text: e.message || '', truncated: e.truncated })))
    .sort((a, b) => Date.parse(a.at) - Date.parse(b.at));

// rows is the lines and the status messages interleaved by time, with a divider at each change of attempt among
// the agent lines; status messages before the first shown line are left out when older lines are not loaded.
export function rows(lines: Line[], status: Shown[], olderUnloaded: boolean): Shown[] {
  const firstAt = lines.length ? Date.parse(lines[0].at) : 0;
  const kept = status.filter((m) => !olderUnloaded || Date.parse(m.at) >= firstAt);
  const all: Shown[] = [];
  let si = 0;
  let lastAttempt: number | null = null;
  for (const l of lines) {
    while (si < kept.length && Date.parse(kept[si].at) < Date.parse(l.at)) {
      all.push(kept[si++]);
    }
    if (lastAttempt !== null && l.attempt !== lastAttempt) {
      all.push({ kind: 'divider', at: l.at, attempt: l.attempt, receiver: receiverName(l.receiver), text: 'attempt ' + l.attempt + ' on ' + receiverName(l.receiver) + ' continues from here' });
    }
    lastAttempt = l.attempt;
    all.push({ kind: 'line', at: l.at, attempt: l.attempt, receiver: receiverName(l.receiver), seq: l.seq, text: l.text ?? '', muted: mutedText(l) || undefined, truncated: l.truncated, size: l.size });
  }
  while (si < kept.length) {
    all.push(kept[si++]);
  }
  return all;
}

interface Segment {
  text: string;
  kind: 'text' | 'redacted' | 'hit';
}

const redaction = /(\[redacted:[A-Za-z0-9_-]{16}\]|\[redacted-url\])/;

// segments splits agent text into its redaction markers, which the page mutes, and the hits of the search query.
export function segments(text: string, q: string): Segment[] {
  const needle = q.trim().toLowerCase();
  const out: Segment[] = [];
  for (const part of text.split(redaction)) {
    if (part === '') {
      continue;
    }
    if (redaction.test(part)) {
      out.push({ text: part, kind: 'redacted' });
      continue;
    }
    if (!needle) {
      out.push({ text: part, kind: 'text' });
      continue;
    }
    const low = part.toLowerCase();
    let i = 0;
    let j: number;
    while ((j = low.indexOf(needle, i)) >= 0) {
      if (j > i) {
        out.push({ text: part.slice(i, j), kind: 'text' });
      }
      out.push({ text: part.slice(j, j + needle.length), kind: 'hit' });
      i = j + needle.length;
    }
    if (i < part.length) {
      out.push({ text: part.slice(i), kind: 'text' });
    }
  }
  return out;
}

// stateText is the line under the transcript's controls: what the last response said.
export function stateText(s: State, working: boolean): string {
  if (s.error === 'busy') {
    return 'the view is busy reading other transcripts; asking again';
  }
  if (s.error === 'error') {
    return 'the last request failed; asking again';
  }
  if (s.missing) {
    return 'the tenant has no transcript topic';
  }
  if (s.retry) {
    return 'indexing the run’s transcript; asking again';
  }
  const parts: string[] = [];
  if (s.predatesLog) {
    parts.push('retention removed the start of this transcript');
  }
  if (s.truncated.length) {
    parts.push('cut by ' + s.truncated.join(', '));
  }
  if (s.complete) {
    parts.push('complete');
  } else if (working) {
    parts.push('following');
  } else if (s.next !== null) {
    parts.push('more from seq ' + grouped(s.next));
  }
  return parts.join(' · ');
}
