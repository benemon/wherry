# wherry A2A extension, version 1

URI: `https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md`

Hosts and receivers exchange A2A tasks over Kafka with no server between
them. This extension fixes the record formats and the checks each reader
applies. Four of the records carry [A2A v1.0](https://a2a-protocol.org/)
objects: submissions, statuses, transcript chunks and results hold one
under its A2A field name, beside a `wherry` object of bindings. Every `Message` and `Artifact` wherry writes
lists the URI above in its `extensions`, and readers reject one that does
not. [Concepts](concepts.md) explains the roles and topics this document
names.

## Encoding

- Payloads are JSON objects. Readers reject unknown members and duplicate
  names.
- Every payload has `v`, the payload version, which is `1`. Readers reject
  any other version.
- A2A objects use the
  [proto3 JSON mapping](https://protobuf.dev/programming-guides/json/) of
  the A2A v1.0 schema. wherry writes `bytes` as padded standard base64.
- A2A identifiers (`messageId`, `taskId`, `contextId`, `artifactId`) and
  wherry's tenant, host, receiver, pool, adapter, agent and event IDs are
  non-empty and lowercase. `metadata`, wherever it appears, is a JSON object.
- A task hash is the lowercase hex SHA-256 of a submission's record value,
  the compact JWS bytes as they appear on the topic.
- Run IDs match `[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`. A run's
  transactional ID is `p.<pool>.<run>`. A receiver's admission
  transactional ID is `p.<pool>.admission.<receiver>`.

## Signed records

Records on topics several principals write are compact
[JWS](https://datatracker.ietf.org/doc/html/rfc7515) with one signature,
algorithm `EdDSA` (Ed25519). The protected header holds:

- `typ`: the record type below;
- `kid`: `<owner>/<key id>`. The owner of a host record is
  `<tenant>.<host>`; the owner of a receiver record is the receiver ID. The
  key ID is `sha256:` and the unpadded base64url SHA-256 of the Ed25519
  public key.

| Record | `typ` | Signer | Topic | Kafka key |
| --- | --- | --- | --- | --- |
| submission | `application/vnd.wherry.submission.v1+jws` | host | `wherry.pool.<pool>.tasks` | run ID |
| cancel | `application/vnd.wherry.cancel.v1+jws` | host | `wherry.pool.<pool>.events` | run ID |
| status | `application/vnd.wherry.status.v1+jws` | receiver | `wherry.pool.<pool>.events` and `wherry.tenant.<tenant>` | run ID |
| chunk | `application/vnd.wherry.chunk.v1+jws` | receiver | `wherry.tenant.<tenant>` | run ID |
| result | `application/vnd.wherry.result.v1+jws` | receiver | `wherry.tenant.<tenant>` | run ID |
| echo | `application/vnd.wherry.echo.v1+jws` | receiver | `wherry.tenant.<tenant>` | run ID |
| advertisement | `application/vnd.wherry.advertisement.v1+jws` | receiver | `wherry.receivers` | receiver ID |
| start failure | `application/vnd.wherry.start-failure.v1+jws` | receiver | `wherry.pool.<pool>.events` | run ID |

A reader accepts a signed record only when all of these hold:

1. It parses as compact JWS with one signature, and the protected header's
   `alg` is `EdDSA` and its `typ` is the type the reader expects.
2. The `kid` owner's current key, folded from the owner's own
   [key topic](#key-records), has the `kid` key ID, and the signature
   verifies against it. A reader that does not know the key ID re-reads
   the owner's key topic once before it rejects the record.
3. The payload decodes strictly, has `v` 1 and passes the rules for its
   type.
4. The owner the payload names equals the `kid` owner.
5. The record passes the checks against `wherry.admin` for its type:
   - A submission or cancel comes from a host on `wherry.admin`, without a
     membership end, of a tenant entitled to the pool. Its `pool` is the
     pool of the topic it arrived on.
   - A status, chunk, result or start failure comes from a receiver on
     `wherry.admin`, without a membership end, in the pool the run was
     placed in. Its `tenant` and `pool` are the run's, and the tenant is
     entitled to the pool.
   - An echo on `wherry.tenant.<tenant>` names that tenant, and comes from a
     live member of the pool it names, to which the tenant is entitled.
   - An advertisement comes from a live member of the pool it names.

When a receiver rebuilds a run's earlier turns, it accepts a submission
signed by one of the host's retired keys only when a pool member's status
names its task hash.

## Submission

A submission is one turn of a run.

```json
{
  "v": 1,
  "message": { "...": "A2A Message" },
  "wherry": {
    "tenant": "alice",
    "host": "laptop",
    "pool": "build",
    "run": "run-0f3c...",
    "turn": 1,
    "placement": {
      "agent": "coder",
      "selector": [{"key": "isolation", "operator": "In", "values": ["container"]}]
    },
    "deadline_ms": 1790000000000,
    "queue_deadline_ms": 1789998800000
  }
}
```

- `message` is the turn's A2A `Message`, with `role` `ROLE_USER` or
  `ROLE_AGENT`. Its `taskId` and `contextId` are the run ID.
- `turn` is 1 for a new run, and one more than the interrupted turn for a
  continuation.
- `placement.agent` is an agent from the pool's terms. `placement.selector`
  is a list of `{key, operator, values}` requirements: `operator` is `In`,
  `NotIn`, `Exists` or `DoesNotExist`; `In` and `NotIn` take one or more
  unique values, `Exists` and `DoesNotExist` none; each key appears once.
  [Reference](reference.md#labels) gives the key and value grammar.
- `deadline_ms` and `queue_deadline_ms` are Unix milliseconds by which the
  run must end and a receiver must claim it. A receiver ends the run no
  later than one hour after `queue_deadline_ms`.
- Each part of the message has exactly one of `text`, `raw`, `url` and
  `data`. A submission carries at most one [git part](#git-context).

## Cancel

A cancel asks the pool to stop a run. It carries no A2A object.

```json
{"v": 1, "tenant": "alice", "host": "laptop", "pool": "build", "run": "run-0f3c...", "turn": 1}
```

`turn` is the run's latest turn when the host signed it, and the cancel
applies to that turn and every later one. Its `tenant` is the run's tenant,
so any host of that tenant can cancel the run. A receiver ignores a cancel
whose Kafka key is not its `run`.

## Status

A status reports one attempt of one turn.

```json
{
  "v": 1,
  "statusUpdate": { "...": "A2A TaskStatusUpdateEvent" },
  "wherry": {
    "receiver": "nomad-1", "tenant": "alice", "pool": "build",
    "run": "run-0f3c...", "turn": 1, "attempt": 1,
    "event_id": "event-5b1e...",
    "kind": "started",
    "task_hash": "9d2a...",
    "log_offset": 0
  }
}
```

- `statusUpdate.taskId` and `contextId` are the run ID.
- `attempt` is the share-group delivery count of the submission, from 1.
- `task_hash` is the hash of the turn's submission.
- `event_id` identifies the fact the status reports. Readers keep one
  status per event ID.
- `log_offset` is the agent output offset the transcript is published up
  to, on `started` and `heartbeat`.

| `kind` | State | Written when | Extra |
| --- | --- | --- | --- |
| `claimed` | `TASK_STATE_WORKING` | the receiver admits the turn | |
| `started` | `TASK_STATE_WORKING` | the job has started | `metadata.git_commit` with a git context |
| `heartbeat` | `TASK_STATE_WORKING` | every 10 seconds while the job runs | pool events only |
| `interrupted` | `TASK_STATE_AUTH_REQUIRED` | the turn waits for the tenant | `metadata.credential` for a missing credential |
| `rejected` | `TASK_STATE_REJECTED` | the receiver refuses the turn | the reason as message text |
| `terminal` | `TASK_STATE_COMPLETED`, `TASK_STATE_FAILED` or `TASK_STATE_CANCELED` | the turn has ended | the reason as message text, except for completion and a failure the agent reports |

Readers also accept an `interrupted` status in `TASK_STATE_INPUT_REQUIRED`.

A receiver writes each status other than a heartbeat to the pool's events
and the tenant feed, with identical bytes, in one Kafka transaction. Readers
read committed records only.

A status message is an A2A `Message` with `role` `ROLE_AGENT` and one text
part. The fixed texts are:

- rejected: `agent is outside the pool terms`, `duplicate submission`,
  `duplicate continuation`, `stale submission`,
  `task is not awaiting continuation`, `continuation turn out of order`,
  `continuation history unavailable`, `queue deadline passed`,
  `run deadline passed`, `unsupported message part`, `run ID in use`;
- interrupted: `credential <name> required`;
- failed: `timeout`, `log stream failed`,
  `agent ended without a result`, `git context unavailable`,
  `receiver denied`, `delivery budget exhausted`,
  `start failed and the job could not be removed`. A failure the agent
  reports carries no message; its result holds the agent's text;
- canceled: `canceled by user`.

`metadata.credential` names the credential. The `wherry` CLI and MCP
server pass the message text through and do not read it.

## Chunk

A chunk is one line of the agent's standard output, as the A2A artifact
`transcript`.

```json
{
  "v": 1,
  "artifactUpdate": { "...": "A2A TaskArtifactUpdateEvent" },
  "wherry": {
    "receiver": "nomad-1", "tenant": "alice", "pool": "build",
    "run": "run-0f3c...", "turn": 1, "attempt": 1,
    "task_hash": "9d2a...",
    "seq": 0
  }
}
```

- The artifact has `artifactId` `transcript` and exactly one part with
  media type `application/x-ndjson`: a text part holding the line, a `raw`
  part for an empty line or one that is not UTF-8, or, for a line over
  512 KiB, a `url` part with the value `unstored:oversize` whose `metadata`
  holds the line's hex `sha256` and its `size` in bytes. The line itself
  is then not stored.
- `seq` is the line's byte offset in the agent's output and orders the
  chunks.
- `append` is false for the line at offset 0 and true after it. `lastChunk`
  is true on the turn's final chunk, written with its terminal status.

A turn's transcript starts at offset 0.

## Result

A result is the A2A `Task` a turn ends with.

```json
{
  "v": 1,
  "task": { "...": "A2A Task" },
  "wherry": {
    "receiver": "nomad-1", "tenant": "alice", "pool": "build",
    "run": "run-0f3c...", "turn": 1, "attempt": 1,
    "task_hash": "9d2a..."
  }
}
```

- `task.id` and `task.contextId` are the run ID. `task.status` is the
  turn's final status.
- `task.history` holds every turn's submitted message in turn order. A
  reader requires `history[turn - 1].messageId` to equal the `message_id`
  of the turn's echo.
- `task.artifacts` holds the artifact `result`, with one part: the agent's
  result as a `data` part with media type `application/json` when it is
  JSON, otherwise a text part. With no result text, the part's text is the
  state's name.
- [Metadata](#metadata) says where the turn ran.

A receiver writes the result in the same transaction as the turn's
`terminal` or `interrupted` status, after the turn's last chunks.

### Metadata

The `metadata` of a result's `Task` says where the turn ran:

- `receiver_id` (string): the receiver that wrote the result.
- `pool` (string): the pool.
- `adapter` (string): the adapter that ran or matched the turn. It is
  absent for a cancelled interrupted turn.
- `labels` (object of strings): the labels of the adapter this attempt
  dispatched the job to. It is absent when no job was dispatched, when the
  attempt adopted an earlier attempt's job, and when the adapter declares no
  labels.
- `job` (string): the adapter's reference for the job on its platform, for
  diagnosis only. It is absent when no job was dispatched or adopted, and
  when the adapter gives none.

The `metadata` of the `result` artifact holds `agent_result` (object): the
agent's final result line, decoded as a JSON object, with its `result`
member removed. The receiver passes the other members through without
interpreting them. It re-encodes them, so member order and string escaping
can differ from the line in the transcript. `agent_result` is absent when
the line is not an RFC 7493 object, when the object without `result`
exceeds 16 KiB as encoded, and when the turn ends without a result line.

## Echo

An echo tells every host of a tenant where a turn was admitted.

```json
{
  "v": 1,
  "receiver": "nomad-1", "tenant": "alice", "pool": "build",
  "run": "run-0f3c...", "turn": 1,
  "placement": {"agent": "coder", "selector": []},
  "task_hash": "9d2a...",
  "message_id": "msg-77c0...",
  "deadline_ms": 1790000000000,
  "queue_deadline_ms": 1789998800000
}
```

A receiver writes the echo in the transaction that holds the turn's
`claimed` or `rejected` status. The echo's fields copy the submission's. A
host that has no record of the run takes the run's pool from its first
valid echo, and ignores records of the run from any other pool. A status or
chunk binds to a turn when its `task_hash` equals the turn's.

## Advertisement

An advertisement reports a receiver's liveness and capacity.

```json
{
  "v": 1,
  "receiver": "nomad-1", "pool": "build",
  "offers": [{"adapter": "container", "agent": "coder"}],
  "capacity": 2, "in_flight": 0,
  "sealing_key": "sha256:...",
  "at_ms": 1789998000000
}
```

- `offers` lists the receiver's ready adapters in declared order, each as
  `adapter` and `agent`. An offer carries no other member. It is empty when
  no adapter is ready. Readers take an adapter's agent and labels from the
  receiver's configuration record on `wherry.admin`, and use `offers` only
  to learn which adapters are ready. Each `adapter` appears at most once.
- `sealing_key` is the fingerprint of the receiver's sealing key: `sha256:`
  and the unpadded base64url SHA-256 of the raw X25519 public key. Readers
  reject an advertisement whose `sealing_key` is not the key registered on
  the receiver's key topic.
- A receiver advertises every 30 seconds, with or without a ready adapter.
  Placement passes over a receiver whose `offers` is empty. Readers take
  each receiver's
  latest valid advertisement, and treat it as live when `at_ms` is within
  120 seconds of their own clock.

## Start failure

A start failure records that a receiver could not start a turn's attempt.
The receiver writes it to the pool's events and releases the task to the
pool. For every class but `offer-unavailable`, the receiver first removes
the turn's job from the platform. When it cannot, it retries for 5 minutes,
then ends the turn `TASK_STATE_FAILED` with the message text
`start failed and the job could not be removed`.

```json
{
  "v": 1,
  "wherry": {"receiver": "nomad-1", "tenant": "alice", "pool": "build", "run": "run-0f3c...", "turn": 1, "attempt": 2},
  "adapter": "container",
  "agent": "coder",
  "class": "seal-unavailable"
}
```

| `class` | Meaning |
| --- | --- |
| `offer-unavailable` | no ready adapter satisfies the task's agent and selector |
| `seal-unavailable` | this receiver opens no seal for the credential, and another live member holds an unexpired one |
| `dispatch` | the adapter created no job for the turn |
| `platform` | the platform ended the job before its agent task started, for example when the image cannot be pulled |

A `platform` start failure can follow the turn's `started` status, when the
job ended before its agent task started.

`adapter` is the adapter the receiver routed the task to. For
`offer-unavailable` it is `none`, since no adapter was chosen.

### Delivery budget

From an attempt of the pool's `delivery_limit` minus 1, a receiver that
would release a task to the pool ends the turn `TASK_STATE_FAILED` with the
message text `delivery budget exhausted` instead. This covers start
failures, a dispatch whose outcome the receiver cannot resolve, a failed
read of the pool's events or the tenant's seals once the turn is claimed,
and a failure to find, stop or remove the turn's job. When the agent has
written its result and only stopping the job fails, the turn ends with that
result. These releases are exempt:

- a receiver that cannot read the pool's events at admission releases the
  task, since it cannot check for a committed terminal;
- a receiver that cannot read the run's turns at admission releases the
  task, since it cannot tell whose run ID it is;
- a receiver that is shutting down releases the task, since an adopter
  continues the run;
- a denied receiver releases the task, since it cannot write;
- a receiver that cannot build the run's transactional producer releases
  the task, since it cannot write.

A receiver whose write of the terminal fails releases the task.

## Key records

Each host and receiver registers its keys on its own topic:
`wherry.host.<tenant>.<host>` or `wherry.receiver.<receiver>`. A key record
is a JWS in the JSON serialization, `typ`
`application/vnd.wherry.key.v1+jws`, algorithm `EdDSA`, with each
signature's protected header carrying its public key as `jwk`.

```json
{
  "v": 1,
  "owner": "nomad-1",
  "action": "rotate",
  "key": {"kty": "OKP", "crv": "Ed25519", "x": "...", "kid": "sha256:...", "alg": "EdDSA", "use": "sig"},
  "previous_key_id": "sha256:...",
  "sealing_key": "<base64url X25519 public key>"
}
```

- `owner` is the topic's owner. `key` is the owner's Ed25519 public key as
  a JWK. `sealing_key` is a receiver's raw X25519 public key, unpadded
  base64url. A host record has no `sealing_key`; only `rotate` has
  `previous_key_id`.
- `register` is the first record on the topic, self-signed by `key`. A
  later `register` is rejected.
- `rotate` replaces the current key with `key`. It carries two signatures,
  by the current key and by `key`, and `previous_key_id` names the current
  key. The replaced key is retired: it verifies the owner's earlier records
  only.
- `revoke` names the current key as `key`, is signed by it, and closes the
  topic. Nothing on it
  verifies afterwards.

Readers fold the topic in order. Once the topic holds a key, every record
must verify against the current key; a record that breaks a rule is
ignored.

## Sealed credentials

A host writes each seal to its own topic, unsigned, keyed
`<receiver>/<credential>`.

```json
{
  "v": 1,
  "tenant": "alice", "receiver": "nomad-1", "pool": "build",
  "key_fingerprint": "sha256:...",
  "credential": "model-api-key",
  "issued_ms": 1789998000000,
  "expiry_ms": 1790084400000,
  "ciphertext": "<base64>"
}
```

- The ciphertext is [HPKE](https://datatracker.ietf.org/doc/html/rfc9180)
  in base mode with DHKEM(X25519, HKDF-SHA256), HKDF-SHA256 and
  AES-256-GCM: the 32-byte encapsulated key followed by the sealed
  plaintext, with empty associated data. The plaintext is at most 16 KiB.
- The HPKE `info` is the bytes `wherry hpke seal`, a zero byte, and the
  JSON object `{"v", "tenant", "receiver", "pool", "key", "credential",
  "issued_ms", "expiry_ms"}` in that member order, where `key` is
  `key_fingerprint`.
- `key_fingerprint` is the receiver's sealing-key fingerprint, as in an
  [advertisement](#advertisement). `expiry_ms` is after `issued_ms`.

A receiver opens, for a task's tenant, pool and credential, the seal
addressed to it with the latest `issued_ms` across the tenant's live hosts'
topics. It refuses a seal past its expiry, for another receiver or pool, or
for a credential the pool's terms do not allow for the task's agent.

## Admin records

`wherry.admin` holds unsigned JSON records that only `wherry.operator`
writes, each with `v`, `kind` and `at_ms` (Unix milliseconds).

| `kind` | Members | Effect |
| --- | --- | --- |
| `pool` | `pool`, `terms` | records or replaces the pool's terms |
| `receiver` | `pool`, `receiver`, `config` | records or replaces the receiver's configuration |
| `host` | `tenant`, `host` | records a host of the tenant |
| `entitlement` | `tenant`, `pool` | entitles the tenant to the pool |
| `end` | `receiver`, or `tenant` and `host` | records a membership end |
| `skip` | `offset`, `sha256` | passes over the undecodable record at that offset with that SHA-256 |

`terms` and `config` have the members of the
[pool terms](reference.md#pool-terms) and
[receiver configuration](reference.md#receiver-configuration) files.
Readers fold the records in order. A reader loads no state when:

- a `receiver` record moves a receiver to another pool or names an unknown
  pool;
- a `receiver` or `host` record follows that member's `end`;
- an `entitlement` names an unknown pool;
- a record fails to decode and no `skip` matches it;
- a `skip` names no undecodable record with that hash.

## Turns and continuations

A run runs in turns. Each turn is a separate submission and runs as a new
job. Readers order a run's statuses, chunks and results by
`(turn, attempt)`, and records of an earlier turn or attempt do not change
the outcome of a later one.

A receiver interrupts a turn with `TASK_STATE_AUTH_REQUIRED` when it opens
no seal for the credential its adapter delivers and no pool member holds
one. It writes the result and the `interrupted` status, and acknowledges the
task.

A continuation is a submission with `turn` greater than 1 and a new
`messageId`. It repeats the tenant, pool and placement of the run's
earlier turns. A receiver admits it only after an `interrupted` status for
the preceding turn from a member of the pool. The receiver rebuilds the
prompt from every turn's text parts, in order, under one opening line:

```
This request was interrupted before any of it was carried out. Carry out all of it now. Its parts follow in the order they were sent.
```

## Admission

A receiver that claims a submission:

1. acknowledges it and writes nothing when the pool's events already hold a
   status in a terminal state, `TASK_STATE_REJECTED` included, with its task
   hash;
2. drops it when it fails the [signed-record checks](#signed-records), or
   when its Kafka key is not its `run`;
3. rejects it when its agent is outside the pool's terms;
4. writes an `offer-unavailable` start failure and releases it when no
   ready adapter satisfies the placement;
5. rejects it with `run ID in use` when the run ID belongs to another
   tenant: the tenant of the run's earliest submission on the pool's task
   topic that a live host's key verifies, or of the run's earliest status
   other than a rejection on the pool's events. Ownership survives the
   owner's host being ended, since the run's statuses still name its
   tenant. This check comes before any record is written under the run ID;
6. rejects a duplicate, stale or out-of-order turn, unless a cancel covers
   it;
7. rejects a continuation whose earlier turns it cannot read;
8. rejects it when its queue deadline or run deadline has passed;
9. rejects it when a part other than text, or other than one git part the
   adapter delivers, appears in the run's turns;
10. writes the echo and the `claimed` status.

A turn a cancel covers skips steps 8 and 9, and ends `TASK_STATE_CANCELED`
after its `claimed` status.

## Git context

A submission can name a git repository for the agent to work on. The
repository is a `url` part with media type `application/vnd.wherry.git`:

- `url` (string): the clone URL. Its scheme is `https` or `ssh` and it has
  a host. An `ssh` URL may carry the user name `git` as its only user
  information; an `https` URL carries none. Neither has a query or
  fragment.
- `metadata.ref` (string, required): a branch, tag or commit.
- `metadata.path` (string, optional): the checkout directory, a relative
  path. It defaults to the last segment of the URL path without a `.git`
  suffix.
- `metadata` has no other members.

A ref, and each segment of a path, starts with a letter, digit or
underscore and holds only letters, digits, `.`, `_` and `-`. A ref can also
hold `/`. It cannot hold `..` or `//`, or end in `/`, `.` or `.lock`.
Readers reject a submission that breaks these rules. A git part applies to
its own turn and to every later turn of the run.

Before the agent starts, the adapter fetches the ref at depth 1, checks it
out, and adds one line to the end of the prompt:

```
The git repository <url> at <ref> is checked out in <dir> at commit <sha>.
```

The turn's `started` status holds `git_commit`, the hex commit SHA checked
out, in its `metadata`. When the checkout fails, the turn ends
`TASK_STATE_FAILED` with the message text `git context unavailable`.
Receivers do not publish git's error output.

## Cancellation

Receivers read the pool's events every 10 seconds, and when they claim a
submission, and act on the lowest turn of each run a valid cancel names:

- A receiver that claims a cancelled turn starts no job. If an earlier
  attempt left the turn's job running, it stops that job first.
- A receiver running a cancelled turn stops its job within a second of
  reading the cancel.
- No receiver holds an interrupted turn. Every pool member that reads a
  cancel for one writes the `terminal` status and result for it, under the
  event ID `<run>/<turn>/canceled`.

In each case the turn ends with a result and a `terminal` status in
`TASK_STATE_CANCELED`, with the message text `canceled by user`. If
stopping a job fails, the receiver releases the submission without a
terminal status, and the next delivery tries again. A turn that ends before
a receiver acts on the cancel keeps the state it ended in.
