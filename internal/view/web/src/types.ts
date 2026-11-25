// The API's shapes, as internal/view/snapshot.go, trace.go and transcript.go encode them. Fields the server omits when
// zero (omitzero) are optional here.

export interface Broker {
  id: number;
  host: string;
  port: number;
  rack?: string;
  controller: boolean;
  listeners?: string[];
}

export interface Storage {
  endpoint: string;
  bucket: string;
}

export interface Group {
  state?: string;
  members: number;
}

export interface Lag {
  status: string;
  records: number;
  error?: string;
  at?: string;
}

export interface Pool {
  name: string;
  suspended: boolean;
  receivers: string[];
  tenants: string[];
  custodian?: string;
  storage?: Storage;
  delivery_limit: number;
  group: Group;
  lag: Lag;
  runs: number;
  in_progress: number;
  by_state: Record<string, number>;
  by_class: Record<string, number>;
  no_terminal: number;
  agents: string[];
}

export interface Offer {
  profile: string;
  agent: string;
  replicas: number;
  in_flight: number;
}

export interface HeldRun {
  run: string;
  attempt: number;
  stage: string;
  stale?: boolean;
  source: string;
}

export interface Receiver {
  name: string;
  pool: string;
  status: string;
  ended: boolean;
  capacity: number;
  in_flight: number;
  gated?: string;
  offers: Offer[];
  last_advertised?: string;
  profiles: string[];
  runs: number;
  by_state: Record<string, number>;
  by_class: Record<string, number>;
  held: HeldRun[];
  stale: HeldRun[];
  unidentified: number;
}

export interface Custodian {
  pool: string;
  receiver: string;
  status: string;
  key_id: string;
  reason: string;
  at: string;
  rotated_for: number;
  suspended_for: number;
  resumed_for: number;
  suspended: boolean;
  signer: string;
}

export interface Host {
  tenant: string;
  name: string;
  ended: boolean;
  runs: number;
  by_state: Record<string, number>;
}

export interface Tenant {
  name: string;
  ended: boolean;
  pools: string[];
  hosts: string[];
  runs: number;
}

export interface StorageNode {
  endpoint: string;
  bucket: string;
  pools: string[];
  runs: number;
  outputs: number;
  bytes: number;
  unreturned: number;
  failed_runs: number;
}

export interface RunSummary {
  id: string;
  tenant: string;
  host?: string;
  pool: string;
  receiver?: string;
  agent?: string;
  model?: string;
  attempt: number;
  stage: string;
  state: string;
  class?: string;
  terminal: boolean;
  storage?: Storage;
  storage_unknown?: boolean;
  cancelled?: boolean;
  admitted?: string;
  first: string;
  last: string;
  outputs: number;
  truncated?: string[];
}

export interface Topic {
  name: string;
  kind: string;
  next: number;
  start: number;
  end: number;
  missing?: boolean;
  predates_log?: boolean;
  error?: string;
}

export interface BoundHit {
  name: string;
  hits: number;
}

export interface Model {
  version: number;
  now: string;
  window_start: string;
  rebuilt: boolean;
  rebuilt_at?: string;
  brokers: Broker[];
  pools: Pool[];
  receivers: Receiver[];
  custodians: Custodian[];
  hosts: Host[];
  tenants: Tenant[];
  storage: StorageNode[];
  runs: RunSummary[];
  runs_total: number;
  truncated?: boolean;
  topics: Topic[];
  counts: Record<string, number>;
  bounds: BoundHit[];
  errors?: string[];
}

export interface Echo {
  receiver: string;
  host: string;
  pool: string;
  agent: string;
  model?: string;
  at: string;
  joined: boolean;
  signer: string;
}

export interface Event {
  kind: string;
  state: string;
  event_id: string;
  receiver: string;
  at: string;
  receiver_at?: string;
  log_offset?: number;
  event?: string;
  class?: string;
  message?: string;
  truncated?: boolean;
  signer: string;
}

export interface Decision {
  custodian: string;
  previous?: string;
  reason: string;
  served: boolean;
  at: string;
  signer: string;
}

export interface StartFailure {
  profile: string;
  agent: string;
  class: string;
  at: string;
  signer: string;
}

export interface Attempt {
  number: number;
  receiver: string;
  task_topic?: string;
  task_offset?: number;
  transcript_chunks?: number;
  events: Event[];
  decision?: Decision;
  start_failure?: StartFailure;
}

export interface Open {
  attempt: number;
  receiver: string;
  at: string;
  signer: string;
  answer?: string;
  custodian?: string;
  reason?: string;
  answered_at?: string;
  answer_signer?: string;
}

export interface Cancel {
  host: string;
  at: string;
  signer: string;
}

export interface Output {
  name: string;
  size: number;
  sha256: string;
}

export interface Unreturned {
  name: string;
  size: number;
  reason: string;
}

export interface Result {
  receiver: string;
  attempt: number;
  state: string;
  at: string;
  profile?: string;
  outputs: Output[];
  s3_outputs: Output[];
  not_returned: Unreturned[];
  reply_size: number;
  reply_cut?: boolean;
  artifacts: number;
  signer: string;
}

export interface PathStep {
  step: string;
  actor: string;
  attempt?: number;
  at: string;
  detail?: string;
}

export interface RunDetail extends RunSummary {
  task_hash?: string;
  reason?: string;
  echoes: Echo[];
  mismatched_echoes?: number;
  attempts: Attempt[];
  opens: Open[];
  cancels: Cancel[];
  result?: Result;
  path: PathStep[];
}

export interface Read {
  bytes: number;
  records: number;
}

export interface SpanNode {
  id: string;
  parent?: string;
  name: string;
  kind: string;
  service?: string;
  start: string;
  end: string;
  duration_ms: number;
  status?: string;
  attributes: Record<string, string>;
  links?: string[];
  events?: number;
  children: SpanNode[];
  marker?: string;
}

export interface Trace {
  id: string;
  roots: SpanNode[];
}

export interface Forest {
  run: string;
  tenant: string;
  topic: string;
  traces: Trace[];
  spans: number;
  markers: string[];
  read: Read;
  next: number;
  complete: boolean;
}

export interface Line {
  at: string;
  attempt: number;
  receiver: string;
  seq: number;
  text?: string;
  raw?: boolean;
  oversize?: boolean;
  truncated?: boolean;
  size?: number;
}

export interface Transcript {
  run: string;
  attempt: number;
  first: number | null;
  reset?: boolean;
  lines: Line[];
  prev?: number;
  next?: number;
  complete: boolean;
  predates_log?: boolean;
  missing?: boolean;
  retry?: boolean;
  indexed: number;
  read: Read;
  seek: Read;
  truncated?: string[];
  evicted?: number;
}

export interface StreamEvent {
  version: number;
  now?: string;
  rebuilt?: boolean;
  runs?: number;
  counts?: Record<string, number>;
}
