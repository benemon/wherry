import type { Forest, SpanNode } from './types';
import type { HealthColor } from './format';

export interface Flat {
  span: SpanNode;
  depth: number;
}

export function flatten(forest: Forest): Flat[] {
  const out: Flat[] = [];
  const walk = (span: SpanNode, depth: number) => {
    out.push({ span, depth });
    for (const child of span.children || []) {
      walk(child, depth + 1);
    }
  };
  for (const trace of forest.traces || []) {
    for (const root of trace.roots || []) {
      walk(root, 0);
    }
  }
  return out;
}

export interface SpanStatus {
  label: string;
  color: HealthColor;
  error: boolean;
}

// spanStatus reads a span's status as OpenTelemetry defines it: absent is unset, a normal span that set no status,
// shown neutral; ok is success; error is a failure. Every server span has an end, so none is open.
export function spanStatus(span: SpanNode): SpanStatus {
  switch (span.status) {
    case 'error':
      return { label: 'error', color: 'danger', error: true };
    case 'ok':
      return { label: 'ok', color: 'success', error: false };
    default:
      return { label: 'unset', color: 'subdued', error: false };
  }
}

// markerText is the sentence a forest's fixed marker code reads as.
export function markerText(marker: string): string {
  switch (marker) {
    case 'oversized record skipped':
      return 'A record over the trace client’s limit was skipped.';
    case 'malformed record skipped':
      return 'A malformed record was skipped.';
    case 'cycle cut':
      return 'A cycle among the spans was cut.';
    case 'missing parent':
      return 'A span names a parent that was not read.';
    case 'no trace topic':
      return 'The tenant has no trace topic.';
    case 'attribute filtered':
      return 'An attribute outside the allowlist was dropped.';
  }
  if (marker.startsWith('truncated: ')) {
    return 'The scan was cut by its ' + marker.slice('truncated: '.length) + ' cap.';
  }
  return '[other]';
}

export const serviceColors: Record<string, string> = { host: '#61A2FF', receiver: '#16C5C0', plugin: '#EE72A6' };
