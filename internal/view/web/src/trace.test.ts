import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { flatten, markerText, spanStatus } from './trace';
import type { Forest } from './types';

const read = <T>(name: string): T => JSON.parse(readFileSync(new URL('../fixtures/' + name, import.meta.url), 'utf8')) as T;

const trace = read<Forest>('trace.json');
const empty = read<Forest>('trace-empty.json');

describe('spans', () => {
  it('flattens the forest in order with depths', () => {
    const flat = flatten(trace);
    expect(flat.map((f) => f.span.name)).toEqual(['task.submit', 'task.claim', 'key.open', 'task.run', '[other]', 'agent.exec']);
    expect(flat.map((f) => f.depth)).toEqual([0, 0, 1, 1, 2, 2]);
    expect(flatten(empty)).toEqual([]);
  });

  it('reads an absent status as unset, a normal span, never as open', () => {
    const flat = flatten(trace);
    const claim = flat[1].span;
    expect(claim.status).toBeUndefined();
    expect(claim.end).toBeTruthy();
    expect(spanStatus(claim)).toEqual({ label: 'unset', color: 'subdued', error: false });
    expect(spanStatus(flat[0].span)).toEqual({ label: 'ok', color: 'success', error: false });
    // The server writes "error: <class>", never a bare "error" (internal/view/trace.go, allowlist.status).
    expect(flat[3].span.status).toBe('error: failed');
    expect(spanStatus(flat[3].span)).toEqual({ label: 'error: failed', color: 'danger', error: true });
    expect(flat[5].span.status).toBe('error: [other]');
    expect(spanStatus(flat[5].span).error).toBe(true);
    for (const f of flat) {
      expect(spanStatus(f.span).label).not.toMatch(/open|running/);
    }
  });

  it('maps each fixed marker to a sentence', () => {
    expect(trace.markers).toEqual(['cycle cut', 'truncated: trace_spans']);
    expect(markerText('cycle cut')).toBe('A cycle among the spans was cut.');
    expect(markerText('truncated: trace_spans')).toBe('The scan was cut by its trace_spans cap.');
    expect(markerText('attribute filtered')).toMatch(/allowlist/);
    expect(markerText('something else')).toBe('[other]');
    expect(flatten(trace)[4].span.marker).toBe('attribute filtered');
  });
});
