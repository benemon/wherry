# wherry A2A extension, version 1

URI: `https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md`

**Change note, 2026-10-25:** edited in place at version 1. The configured
unit a run is routed to is a profile, a named, configured use of an adapter.
The member `adapter` of advertisement offers, start failures and result
metadata is renamed `profile`. The receiver configuration on
`wherry.admin` holds `profiles` in place of `adapters`, each with `plugin`
in place of `type` and no `platform`. Messages that named the adapter name
the profile.

Hosts and receivers exchange A2A tasks over Kafka with no server between
them. This extension fixes the record formats and the checks each reader
applies. Five of the records carry [A2A v1.0](https://a2a-protocol.org/)
objects: submissions, statuses, transcript chunks, output chunks and
results hold one under its A2A field name, beside a `wherry` object of
bindings. Every `Message` and `Artifact` wherry writes
lists the URI above in its `extensions`, and readers reject one that does
not. [Key concepts](concepts.md) explains the roles and topics this document
names.

## Encoding

- Payloads are JSON objects. Readers reject unknown members and duplicate
  names.
- Every payload has `v`, the payload version, which is `1`. Readers reject
  any other version.
- A2A objects use the
  [proto3 JSON mapping](https://protobuf.dev/programming-guides/json/) of
  the A2A v1.0 schema. wherry writes `bytes` as padded standard base64.
  Readers accept standard or URL-safe base64, padded or not.
- Every `TaskStatus` has `timestamp`, an RFC 3339 time. Receivers write it
  in UTC with milliseconds.
- A `Task` has `artifacts` and `history`, and a `Message` and an `Artifact`
  have `parts`, each present even when empty. A `data` part holds valid
  JSON.
- A2A identifiers (`messageId`, `taskId`, `contextId`, `artifactId`) and
  wherry's tenant, host, receiver, pool, profile, agent and event IDs are
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
| chunk | `application/vnd.wherry.chunk.v1+jws` | receiver | `wherry.tenant.<tenant>.transcript` | run ID |
| output chunk | `application/vnd.wherry.output.v1+jws` | receiver | `wherry.tenant.<tenant>.transcript` | run ID |
| result | `application/vnd.wherry.result.v1+jws` | receiver | `wherry.tenant.<tenant>` | run ID |
| echo | `application/vnd.wherry.echo.v1+jws` | receiver | `wherry.tenant.<tenant>` | run ID |
| advertisement | `application/vnd.wherry.advertisement.v1+jws` | receiver | `wherry.receivers` | receiver ID |
| start failure | `application/vnd.wherry.start-failure.v1+jws` | receiver | `wherry.pool.<pool>.events` | run ID |
| seal request | `application/vnd.wherry.seal-request.v1+jws` | receiver | `wherry.tenant.<tenant>` | run ID |

A reader accepts a signed record only when all of these hold:

1. It parses as compact JWS with one signature, and the protected header's
   `alg` is `EdDSA` and its `typ` is the type the reader expects.
2. The `kid` owner's current key, folded from the owner's own
   [key topic](#key-records), has the `kid` key ID, and the signature
   verifies against it. A reader that does not know the key ID re-reads
   the owner's key topic once before it rejects the record. A host reading
   its tenant feed and transcript topic, and a receiver reading its pool's
   statuses, also accept a [retired key](#key-records) for a record the
   broker appended before that key's rotation.
3. The payload decodes strictly, has `v` 1 and passes the rules for its
   type.
4. The owner the payload names equals the `kid` owner.
5. The record passes the checks against `wherry.admin` for its type:
   - A submission or cancel comes from a host on `wherry.admin`, without a
     membership end, of a tenant entitled to the pool. Its `pool` is the
     pool of the topic it arrived on.
   - A status, chunk, output chunk, result, start failure or seal request
     comes from a receiver on `wherry.admin`, without a membership end, in
     the pool the run was placed in. Its `tenant` and `pool` are the run's,
     and the tenant is entitled to the pool.
   - An echo on `wherry.tenant.<tenant>` names that tenant, and comes from a
     live member of the pool it names, to which the tenant is entitled.
   - An advertisement comes from a live member of the pool it names.

## Submission

A submission is the task of one run.

```json
{
  "v": 1,
  "message": { "...": "A2A Message" },
  "wherry": {
    "tenant": "alice",
    "host": "laptop",
    "pool": "build",
    "run": "run-0f3c...",
    "placement": {
      "agent": "coder",
      "selector": [{"key": "isolation", "operator": "In", "values": ["container"]}],
      "model": "sonnet"
    },
    "deadline_at_ms": 1790000000000,
    "queue_deadline_at_ms": 1789998800000,
    "credentials": [{"name": "deploy-key", "file": "git-credential"}, {"name": "vault-token", "env": "VAULT_TOKEN"}],
    "unsupplied_credentials": ["github-token"],
    "outputs": [{"name": "report.md", "required": true}, {"name": "plot.pdf", "storage": "s3"}],
    "s3_inputs": [{"name": "dataset.parquet", "size": 5242880, "sha256": "4f1c..."}],
    "git": {
      "remote": "ssh://git@git.example.com/team/app.git",
      "ref": "main",
      "credential": "deploy-key",
      "host_key": "git.example.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA...\n"
    }
  }
}
```

- `message` is the run's A2A `Message`, with `role` `ROLE_USER` or
  `ROLE_AGENT`. Its `taskId` and `contextId` are the run ID.
- `placement.agent` is an agent from the pool's terms. `placement.selector`
  is a list of `{key, operator, values}` requirements: `operator` is `In`,
  `NotIn`, `Exists` or `DoesNotExist`; `In` and `NotIn` take one or more
  unique values, `Exists` and `DoesNotExist` none; each key appears once.
  `placement.model` is one of the agent's `models` in the pool's terms, and
  absent for an agent with none.
  [Commands](commands.md#labels) gives the key and value grammar.
- `deadline_at_ms` and `queue_deadline_at_ms` are Unix milliseconds by which the
  run must end and a receiver must claim it, each 0 or more. A receiver ends
  the run no later than six hours after `queue_deadline_at_ms`.
- The message has at most 256 parts, each with exactly one of `text`,
  `raw`, `url` and `data`. Receivers reject a `url` part
  ([admission](#admission)).
- Each `raw` and `data` part is an [input file](#inputs-and-outputs). Its
  `filename` is required and is a file name: at most 255 bytes of letters,
  digits, `.`, `_` and `-`, whose first character is not `.`. A `raw` part is
  delivered as its bytes under `filename`, and a `data` part as its JSON under
  `filename` with `.json` added. The delivered names are unique. The signed
  submission is at most the pool's recorded `record_limit` bytes, the
  broker's record limit for the pool's tasks topic, and the `raw` and `data`
  parts together hold at most the smallest `inline_limit` the matching
  profiles declare. Over either, a host carries the inputs as
  [S3 inputs](#s3-inputs-and-outputs) when the pool has storage and the host
  a storage credential, and otherwise refuses the run.
- `credentials` lists task credentials, each `{name, env}` or `{name, file}`:
  a credential of the host, by the name in its configuration, that the agent
  gets in the environment variable `env` or as the file `file` in its
  credentials directory. `name` is at most 32 lowercase letters, digits and
  hyphens, starting and ending with a letter or digit, and appears once.
  Each credential has exactly one target: `env` matches
  `[A-Za-z_][A-Za-z0-9_]*` and is no variable an adapter sets
  (`adapter.ReservedEnv`), and `file` follows the file name rules. No two
  credentials share a target, and no task credential takes the target of one
  of the agent's credentials in the pool's terms. A task credential may
  share its name with one of the agent's credentials; the seal carries the
  value once and the agent gets it at both targets. The pool's terms do not
  otherwise govern task credentials.
- `unsupplied_credentials` names the agent's optional credentials in the
  pool's terms for which the host has no source, each once; absent when there
  are none. The run goes without them, and the prompt names them as not
  supplied. A receiver rejects a submission that names a required credential
  of the agent here.
- `outputs` lists at most 256 files the agent is asked to leave, each
  `{name, required, storage}`. `name` is a relative path of file names joined by
  `/`, at most 512 bytes, and appears once. `required` defaults to false.
  `storage` `s3` returns the output through the pool's bucket
  ([S3 inputs and outputs](#s3-inputs-and-outputs)); absent, it returns on
  the transcript topic.
- `s3_inputs` lists files the host uploaded to the pool's bucket, each
  `{name, size, sha256}`: a file name distinct from every input file's,
  appearing once, the object's size in bytes, at most 5 GiB, and its
  lowercase hex SHA-256. It holds no URL. The message's parts and
  `s3_inputs` number at most 256 together.
- `git` is the run's [git input](#git-input), optional. With a `host_key`,
  no input file or S3 input is named `known_hosts`. A host fills its
  `credential` and `host_key` from the [git entry](configuration.md#client-configuration)
  of its configuration that the remote falls under, and adds that credential
  to `credentials` at the file `git-credential`.

A submission whose input names repeat, among its input files, its S3 inputs
and `known_hosts`, fails to decode, so a receiver drops it at
[admission](#admission) step 2.

## Cancel

A cancel asks the pool to stop a run. It carries no A2A object.

```json
{"v": 1, "tenant": "alice", "host": "laptop", "pool": "build", "run": "run-0f3c..."}
```

Its `tenant` is the run's tenant,
so any host of that tenant can cancel the run. A receiver ignores a cancel
whose Kafka key is not its `run`.

## Status

A status reports one attempt of a run.

```json
{
  "v": 1,
  "statusUpdate": { "...": "A2A TaskStatusUpdateEvent" },
  "wherry": {
    "receiver": "nomad-1", "tenant": "alice", "pool": "build",
    "run": "run-0f3c...", "attempt": 1,
    "event_id": "event-5b1e...",
    "kind": "started",
    "task_hash": "9d2a...",
    "log_offset": 0
  }
}
```

A `terminal` status's `wherry` object adds the attempt's chunk count:

```json
"wherry": {
  "receiver": "nomad-1", "tenant": "alice", "pool": "build",
  "run": "run-0f3c...", "attempt": 1,
  "event_id": "event-7c40...",
  "kind": "terminal",
  "task_hash": "9d2a...",
  "transcript_chunks": 42
}
```

- `statusUpdate.taskId` and `contextId` are the run ID.
- `attempt` is the share-group delivery count of the submission, from 1.
- `task_hash` is the hash of the run's submission.
- `event_id` identifies the fact the status reports. Readers keep one
  status per event ID.
- `log_offset` is the agent output offset an adopter follows from, on
  `started` and `heartbeat`. The transcript is published up to at least
  that offset.
- `transcript_chunks` is the number of [chunks](#chunk) the attempt wrote,
  on `terminal` only, and absent when zero. It commits in the same
  transaction as the attempt's last chunks.

| `kind` | State | Written when | Extra |
| --- | --- | --- | --- |
| `claimed` | `TASK_STATE_WORKING` | the receiver admits the run | |
| `started` | `TASK_STATE_WORKING` | the job has started | |
| `heartbeat` | `TASK_STATE_WORKING` | the transcript has moved, checked every 10 seconds while the job runs | pool events only |
| `rejected` | `TASK_STATE_REJECTED` | the receiver refuses the submission | the reason as message text |
| `terminal` | `TASK_STATE_COMPLETED`, `TASK_STATE_FAILED`, `TASK_STATE_AUTH_REQUIRED` or `TASK_STATE_CANCELED` | the run has ended | the agent's message from its [outcome file](configuration.md#agent-contract), or the reason as message text |

A receiver writes each status other than a heartbeat to the pool's events
and the tenant feed, with identical bytes, in one Kafka transaction. A run's
chunks and output chunks commit in the transaction of the status or result
they belong to, so a transaction spans the tenant feed, the transcript topic
and the pool's events. Readers read committed records only. A reader sees
each topic's commit separately, so a `terminal` status can be visible before
its attempt's last chunks. A host that reads a `terminal` status on the feed
reads the transcript topic to its last stable offset, and waits until that
reaches the high watermark it lists after it reads the terminal, before it
reports the run's transcript or outputs complete. A host reads a run's
chunks from the first transcript-topic offset at or after the append time of
the run's earliest echo on the feed, found by `ListOffsets` by timestamp.
Readers order a run's statuses and results by `attempt`, and records of an
earlier attempt do not change the outcome of a later one.

A status message is an A2A `Message` with `role` `ROLE_AGENT` and one text
part. The message of a `TASK_STATE_AUTH_REQUIRED` status adds the
[`credential` data part](#the-credential-data-part). The agent's text is
redacted, replaced by `[redacted text over 65536 bytes]` when it passes
64 KiB once redacted, and travels as a `raw` part with media type
`text/plain` when escaping it as a JSON string would make it longer than
base64 does. The fixed texts are:

- rejected: `agent is outside the pool terms`,
  `model not in the pool's terms`, `duplicate submission`,
  `run expired`, `queue deadline exceeds the pool's max queue deadline`,
  `queue deadline passed`, `run deadline passed`,
  `unsupported message part`, `run ID in use`,
  `git remote host <host> is not in pool <pool>'s vcs hosts`, and
  `pool <pool> has no storage for S3 inputs and outputs`;
- failed: `timeout`, `log stream failed`, `agent exited <code>`,
  `agent ended by <signal>`, `agent reported no outcome`,
  `an S3 input was not fetched, or did not match its size and SHA-256`,
  `the URL for S3 input <name> is not one of pool <pool>'s storage` (or
  `S3 output <name>`),
  `receiver denied`, `delivery budget exhausted`,
  `start failed and the job could not be removed`,
  `input <name> takes the run's inputs over what profile <profile>
  delivers`, naming the input at which they pass the profile's limit (or
  `an input takes the run's inputs over what profile <profile> delivers`
  when the plugin names no input of the run),
  `required output <name> not returned: <reason>` with a reason from
  [Metadata](#metadata), and, when the [seal wait](#seal-request) expires,
  `credential <name> was not answered within 5m; add it to credentials in
  the host's wherry configuration, keep wherry await or wherry mcp running,
  and submit again`. For several credentials the text opens
  `credentials <name>, <name> were not answered within 5m; add them to
  credentials`. When the request also asked for presigned URLs, the subject
  adds `and presigned URLs for the S3 inputs and outputs` and the
  instruction is `add the credentials and storage`; for URLs alone it is
  `presigned URLs for the S3 inputs and outputs were not answered within
  5m; configure them`. The text names credentials, never their values;
- canceled: `canceled by user`.

The `wherry` CLI and MCP server pass the message text through and do not
read it.

### The credential data part

An agent that needs a credential it lacks ends its run
`TASK_STATE_AUTH_REQUIRED` and names the credential in a data part of its
message:

```json
{"data": {"credential": "deploy-key"}, "mediaType": "application/json"}
```

`credential` is a credential name: at most 32 lowercase letters, digits and
hyphens, redacted like any agent text. It names one of the credentials the
prompt lists, including an optional one the prompt marks `(not supplied)`,
or another the run needs and was not given. A run in
`TASK_STATE_AUTH_REQUIRED` has ended: wherry treats the state as terminal
and does not resume the run. The host submits a new run once it holds the
credential.

## Chunk

A chunk is one line of the agent's standard output, as the A2A artifact
`transcript`.

```json
{
  "v": 1,
  "artifactUpdate": { "...": "A2A TaskArtifactUpdateEvent" },
  "wherry": {
    "receiver": "nomad-1", "tenant": "alice", "pool": "build",
    "run": "run-0f3c...", "attempt": 1,
    "task_hash": "9d2a...",
    "seq": 0
  }
}
```

- The artifact has `artifactId` `transcript` and exactly one part with media
  type `application/x-ndjson`: a text part holding the line, a `raw` part for an
  empty line, one that is not UTF-8, or one longer escaped as a JSON string than
  base64-encoded, or, for a line over 512 KiB, a `url` part with the value
  `unstored:oversize` whose `metadata` holds the line's hex `sha256` and its
  `size` in bytes. The line itself is then not stored. Readers require a
  `url` part's `sha256` to be lowercase hex SHA-256 and its `size` over 0.
- `seq` is the line's byte offset in the agent's output and orders the
  chunks. An adopter follows from the last heartbeat's log offset, so a
  `seq` can repeat across attempts of a run. A reader takes the latest
  attempt's chunks, and the lines below that attempt's first `seq` from the
  highest earlier attempt that has them, so a run's transcript starts at
  offset 0.
- `append` is false for the line at offset 0 and true after it. `lastChunk`
  is true on the run's final chunk, written with its terminal status.

A run's transcript starts at offset 0. A receiver writes chunks to
`wherry.tenant.<tenant>.transcript`.

## Output chunk

An output chunk is part of one output file, as the A2A artifact the file
names.

```json
{
  "v": 1,
  "artifactUpdate": { "...": "A2A TaskArtifactUpdateEvent" },
  "wherry": {
    "receiver": "nomad-1", "tenant": "alice", "pool": "build",
    "run": "run-0f3c...", "attempt": 1,
    "task_hash": "9d2a..."
  }
}
```

- The artifact's `artifactId` is the output's name, a relative path of
  file names joined by `/` that follows the `outputs` name rules. It has
  exactly one `raw` part holding at most 256 KiB of the file.
- `artifactUpdate.metadata` is exactly `{"index": <n>}`, with `n` 0 or
  more. A file's chunks have
  indexes 0, 1, 2 and so on, in file order. `append` is false at index 0
  and true after it. `lastChunk` is true on the file's final chunk. An
  empty file is one chunk with an empty part.
- A chunk's identity is its attempt, artifact and index.

A receiver writes a run's output chunks to
`wherry.tenant.<tenant>.transcript` in the transaction that holds its
result, before the result.

## Result

A result is the A2A `Task` a run ends with.

```json
{
  "v": 1,
  "task": { "...": "A2A Task" },
  "wherry": {
    "receiver": "nomad-1", "tenant": "alice", "pool": "build",
    "run": "run-0f3c...", "attempt": 1,
    "task_hash": "9d2a..."
  }
}
```

- `task.id` and `task.contextId` are the run ID. `task.status` is the
  run's final status, with the message its `terminal` status carries.
- `task.history` holds the run's submitted message alone. Each `raw` and
  `data` part is replaced by a `url` part with the value `unstored:input`,
  the input's `filename` and `mediaType`, and `metadata` holding the hex
  `sha256` and `size` in bytes of its `raw` bytes or its `data` JSON. A
  reader requires `history[0].messageId` to equal the `message_id` of the
  run's echo.
- `task.artifacts` holds the artifact `result`, with one part: the text of
  the agent's message, or the receiver's reason, as a `data` part with media type `application/json` when it is
  JSON, a `raw` part with media type `text/plain` when it is not valid
  UTF-8 or escaping it as a JSON string would make it longer than base64
  does, and otherwise a text
  part. With no result text, the part's text is the state's name. Result
  text over 64 KiB once redacted is replaced by
  `[redacted text over 65536 bytes]`.
- [Metadata](#metadata) says where the run executed.

A receiver writes the result in the same transaction as the run's
`terminal` status, after the run's last chunks.

### Metadata

The `metadata` of a result's `Task` says where the run executed:

- `receiver_id` (string): the receiver that wrote the result.
- `pool` (string): the pool.
- `profile` (string): the profile that ran or matched the run. It is
  absent when no ready profile satisfied the placement.
- `labels` (object of strings): the labels of the profile this attempt
  dispatched the job to. It is absent when no job was dispatched, when the
  attempt adopted an earlier attempt's job, and when the profile declares no
  labels.
- `job` (string): the plugin's reference for the job on its platform, for
  diagnosis only. It is absent when no job was dispatched or adopted, and
  when the plugin gives none.
- `outputs` (array): each output the run returned, as `{name, sha256,
  size}`, where `sha256` is the lowercase hex SHA-256 and `size` the byte
  count of the bytes its output chunks carry. The receiver computes both.
- `s3_outputs` (array): each S3 output the run returned, as `{name,
  sha256, size, key}`, where `sha256` and `size` are of the bytes the
  receiver uploaded, computed as it sent them, and `key` is the object's
  key in the pool's bucket.
- `not_returned` (array): each output the run did not return, as
  `{name, size, reason}`. `size` is present when the receiver knows it.

| `reason` | Meaning |
| --- | --- |
| `missing` | an expected output the agent did not leave |
| `not collected` | the adapter did not collect the outputs; every expected output is listed with it |
| `exceeds output limit` | the file is over the pool's `output_limit`, or would take the run's outputs over it |
| `rejected: symlink`, `rejected: fifo`, `rejected: device`, `rejected: socket`, `rejected: irregular file` | the entry is not a regular file and was not read |
| `rejected: invalid name` | the path is not a relative path of file names of at most 512 bytes; `name` is the path quoted as ASCII, at most 256 bytes |
| `rejected: size changed while read` | the adapter read a length other than the size it listed |
| `exceeds the S3 object limit` | an S3 output over 5 GiB |
| `not streamed by the adapter` | the adapter did not stream an S3 output it found |
| `not uploaded: no upload URL` | the attempt holds no PUT URL for the S3 output |
| `not uploaded: redirected` | the store answered the PUT with a redirect, which the receiver does not follow |
| `not uploaded: status <code>` | the store answered the PUT with another status than 200 |
| `not uploaded: request failed`, `not uploaded: invalid request` | the PUT did not complete, or could not be built |

`outputs`, `s3_outputs` and `not_returned` are present on a result of a run
whose job ended after its agent started, and absent when they would be
empty.

## Echo

An echo tells every host of a tenant where a run was admitted.

```json
{
  "v": 1,
  "receiver": "nomad-1", "tenant": "alice", "pool": "build",
  "run": "run-0f3c...",
  "placement": {"agent": "coder", "selector": []},
  "task_hash": "9d2a...",
  "message_id": "msg-77c0..."
}
```

A receiver writes the echo in the transaction that holds the run's
`claimed` or `rejected` status, or its `terminal` status when the receiver
ends at the [delivery budget](#delivery-budget) a run no ready profile
satisfies. The echo's fields copy the submission's. A
host that has no record of the run takes the run's pool from its first
valid echo, and ignores records of the run from any other pool. A status or
chunk binds to the run when its `task_hash` equals the hash of the run's
submission.

## Advertisement

An advertisement reports a receiver's liveness and capacity.

```json
{
  "v": 1,
  "receiver": "nomad-1", "pool": "build",
  "offers": [{"profile": "container", "agent": "coder"}],
  "capacity": 2, "in_flight": 0,
  "at_ms": 1789998000000
}
```

- `offers` lists the receiver's ready profiles in routing order, each as
  `profile`, `agent` and, optionally, `inline_limit`, the bytes of `raw`
  and `data` parts the profile delivers, absent when it declares no limit
  and never negative. It is empty when no profile is ready. Readers take a
  profile's agent and labels from the receiver's configuration record on
  `wherry.admin`. Each `profile` appears at most once. A host takes the
  smallest `inline_limit` among the offers that satisfy a placement in each
  receiver's last advertisement, live or stale; submitting inputs over it,
  the host carries them as S3 inputs, or refuses the run when the pool or
  the host has no storage.
- `capacity` and `in_flight` are 0 or more, and `at_ms` is over 0.
- A receiver advertises every 30 seconds, with or without a ready profile.
  Placement passes over a receiver whose `offers` is empty. Readers take
  each receiver's
  latest valid advertisement, and treat it as live when `at_ms` is within
  120 seconds of their own clock.

## Start failure

A start failure records that a receiver could not start a run's attempt.
The receiver writes it to the pool's events. For every class but
`input-too-large`, it then removes the run's job from the platform and
releases the task to the pool. When it cannot remove the job, it retries
for 5 minutes, then ends the run `TASK_STATE_FAILED` with the message text
`start failed and the job could not be removed`. An `input-too-large`
start failure ends the run as its class describes.

```json
{
  "v": 1,
  "wherry": {"receiver": "nomad-1", "tenant": "alice", "pool": "build", "run": "run-0f3c...", "attempt": 2},
  "profile": "container",
  "agent": "coder",
  "class": "dispatch"
}
```

| `class` | Meaning |
| --- | --- |
| `dispatch` | the plugin created no job for the run |
| `platform` | the platform ended the job before its agent task started, for example when the image cannot be pulled |
| `input-too-large` | the profile cannot deliver one of the run's inputs; the receiver then ends the run `TASK_STATE_FAILED` naming the input, and releases nothing |

A `platform` start failure can follow the run's `started` status, when the
job ended before its agent task started. The platform's reason for it goes to
the receiver's log, not to the start failure.

`profile` is the profile the receiver routed the task to.

### Delivery budget

From an attempt of the pool's `delivery_limit` minus 1, a receiver that
would release a task to the pool ends the run `TASK_STATE_FAILED` with the
message text `delivery budget exhausted` instead. When the attempt's own
start failure spends the budget, the text continues
`after <attempt> attempts; last: <cause>`, where the cause is
`job never started (<class>): <reason>` with the platform's class and
reason, `job never started`, or `profile <profile> created no job`. The
cause carries the marker of each credential value delivered to a run the
receiver holds in place of the value, has no URLs, and is one line of at
most 512 bytes. This covers start
failures, a dispatch whose outcome the receiver cannot resolve, a failed
read of the pool's events or of the host's topic once the run is claimed,
and a failure to find, stop or remove the run's job. When only stopping the
job fails at the budget, the run ends from the agent's exit status without
its outputs or outcome file, and the plugin's sweep removes the job. These
releases are exempt:

- a receiver that cannot read the pool's events at admission releases the
  task, since it cannot check for a committed terminal;
- a receiver that cannot read the run's submissions at admission releases the
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
  "previous_key_id": "sha256:..."
}
```

- `v` is 1. `owner` is the topic's owner. `key` is the owner's Ed25519
  public key as a public JWK. Writers set `previous_key_id` only on
  `rotate`; readers ignore it on other actions.
- `register` is the first record on the topic, self-signed by `key`. A
  later `register` is rejected.
- `rotate` replaces the current key with `key`. It carries two signatures,
  by the current key and by `key`, and `previous_key_id` names the current
  key. A `rotate` whose `key` is the current key is rejected. The replaced
  key is retired at the rotate record's broker timestamp. A retired key
  verifies the owner's statuses, results and echoes on a tenant feed, its
  transcript chunks and outputs on a tenant's transcript topic, and its
  statuses on a pool's events, that the broker appended before that
  timestamp, and nothing else. A record the
  broker appended at or after it does not verify under the retired key. Readers that admit work, such as a receiver
  admitting a task or a cancel or a host answering a seal request, accept
  the current key alone.
- Any other action is rejected.

Readers fold the topic in order. Once the topic holds a key, every record
must verify against the current key; a record that breaks a rule is
ignored.

## Seal request

A receiver that claims a run whose agent takes credentials asks the run's
host for them. It makes a P-256 key pair for the attempt and commits a
seal request to the tenant feed in a transaction of its own, after the
run's `claimed` status.

```json
{
  "v": 1,
  "receiver": "nomad-1", "tenant": "alice", "pool": "build",
  "run": "run-0f3c...", "attempt": 2,
  "key": {"kty": "EC", "crv": "P-256", "x": "...", "y": "..."},
  "key_id": "hs9I_DeEnEcQZjRh56OShK8IHP_rXpmv2UZ2DQxMXz0",
  "credentials": ["model-api-key"],
  "task_credentials": ["deploy-key"],
  "s3_inputs": ["dataset.parquet"],
  "s3_outputs": ["plot.pdf"],
  "expiry_at_ms": 1789998300000
}
```

- `key` is the attempt's P-256 EC public key as a JWK. `key_id` is its
  [RFC 7638](https://datatracker.ietf.org/doc/html/rfc7638) SHA-256
  thumbprint, unpadded base64url. `expiry_at_ms` is over 0. Credential
  names are non-empty and lowercase, `s3_inputs` are file names and
  `s3_outputs` follow the `outputs` name rules.
- `credentials` names the agent's credentials in the pool's terms, less those
  the submission lists in `unsupplied_credentials`, and is empty when there
  are none.
- `task_credentials` names the run's task credentials other than those,
  and is absent when there are none. A name appears once across both
  lists.
- `s3_inputs` and `s3_outputs` name the run's S3 inputs and S3 outputs, for which the seal is to carry presigned URLs; each is absent
  when empty, and each name appears once in its list. An attempt that
  adopts an earlier attempt's job asks for the credentials and the S3
  outputs, not the S3 inputs, which the job has fetched. A GET presigned
  again differs from the first attempt's, so an adopter cannot redact the
  first attempt's S3 input URLs, and an adapter never exposes its fetch URLs
  to the job.
- The four lists are not all empty. A run needs a seal when its agent takes
  a credential its host supplies, or the run has task credentials, S3 inputs
  or S3 outputs; an adopter, when it has credentials or S3 outputs.
- `expiry_at_ms` is 5 minutes after the request. The receiver holds the claim
  until a seal opens, a cancel of the run arrives, or `expiry_at_ms` passes.
  At expiry it ends the run `TASK_STATE_FAILED` with a message naming the
  credentials it asked for, never their values ([Status](#status)), and
  the task is not returned to the pool.
- An adopter publishes nothing of the job until its seal opens. It holds
  the job's lines, at most 4 MiB, dropping the oldest, and logs how many it
  dropped and the offset its transcript resumes at; the first chunk's `seq`
  shows the gap. At expiry it stops the job before it ends the run, and
  the held lines are never published.
- The private key lives only in the receiver's memory and is gone once the
  wait ends. A redelivered or adopted attempt makes a new key and a new
  request; a seal answering an earlier request does not open for it.

The host that submitted the run answers while `wherry await` or `wherry
mcp` runs, at most once per run and attempt. It answers only when all of
these hold:

1. The request verifies as a [signed record](#signed-records) from a live
   member of the run's pool, for the run's tenant.
2. The host holds a local record of the run, written when it submitted the
   run.
3. `expiry_at_ms` has not passed and is at most 6 minutes away: the receiver's
   5-minute wait and a minute of clock skew.
4. Every `claimed` status of the run comes from one receiver, and the
   request comes from it.
5. The request names the receiver and attempt of the run's latest status,
   which is `claimed`, `started` or `heartbeat`.
6. Every name in `credentials` is in the pool's terms for the run's agent,
   every name in `task_credentials` is a task credential the run declares,
   and the host's configuration gives every name a source.
7. Every name in `s3_inputs` and `s3_outputs` is an S3 input or S3 output
   the run declares, the pool's terms have `storage`, and the host's
   configuration has a storage credential.

The host makes the run's redaction key, 32 random bytes, when it records the
run. It writes the key to its record of the run, readable by its owner
alone, before it publishes the submission, and answers never write it. Every
seal for the run carries that key: every attempt and adoption. A host never makes
a second key for a run, since the markers already published would stop mapping
to names: for a record whose key is not 32 bytes it refuses every request and
logs the record's path.

For each S3 input the host presigns a GET of
`wherry/<tenant>/<run>/inputs/<name>` for 15 minutes, and for each S3 output a
PUT of `wherry/<tenant>/<run>/outputs/<name>` until the run's
`deadline_at_ms`, at most 7 days, path-style on the pool's endpoint with its own
credential.

A host records a run locally before it publishes the run's submission, so a
request can follow the task at once; a request read while the record is
absent is checked again on each later pass until it expires. Once the
request passes checks 1 to 3 and 6, the host resolves each credential's
source, before it takes the attempt's lock, so a slow command holds up no
other answerer: a `file` source reads the file, an `env` source the
answering process's environment variable, and a `command` source runs its
argv without a shell for at most 60 seconds, its stdout, at most 64 KiB,
the value. A source that fails to resolve, an unset or empty variable, a
command that exits non-zero, times out or writes too much, leaves the
request for a later pass, logged by credential name and cause alone; the
host discards a command's stderr. The host holds
an exclusive lock on the attempt, shared by every process using its state
directory, from these checks to the record of its answer, and gives up
waiting for the lock when its own work ends.
Just before it writes the seal it checks again that `expiry_at_ms` has not
passed, then reads the run again and repeats checks 4 and 5, so a status
that ends the attempt, or another claim, while the seal is built stops the
write. It records the attempt as answered once the write
is acknowledged. A host that
stops between the two answers again on its next start; the receiver opens
the first seal that opens. Each start removes the records of attempts whose
request has expired or whose run has ended.

A host logs a skipped request that verifies and belongs to a run it
submitted, by run, receiver and credential names, never a value. It logs
nothing for a request for a run it did not submit, nor, apart from a failure
to read its own run, for a request it cannot verify.

Check 4 is the only trace a forged claim leaves. The broker's share-group
claim is invisible to hosts, so a live pool member can sign a `claimed`
status it never received. A run that a second receiver claims, by
adoption or by forgery, gets no seal and ends `TASK_STATE_FAILED` when its
seal wait expires; an adopter stops the job first. A receiver adopting its own earlier
attempt's job is sealed as usual.

## Seal

A seal answers one seal request. The host writes it to its own topic,
`wherry.host.<tenant>.<host>`, unsigned, keyed by run ID, as a compact
[JWE](https://datatracker.ietf.org/doc/html/rfc7516) encrypted to the
request's `key`.

- `alg` is `ECDH-ES+A256KW` and `enc` is `A256GCM`. Readers accept only
  these algorithms and the compact serialization, and reject `zip`.
- The protected header holds `alg`, `enc`, `epk`, `typ`
  `application/vnd.wherry.seal.v2+jwe` and the binding: `tenant`, `pool`,
  `receiver`, `key_id`, `run`, `attempt`, `credentials`, always present and
  possibly empty, `task_credentials`, `s3_inputs` and `s3_outputs` when
  non-empty, and `expiry_at_ms`, copied from the request. Readers reject any
  other member.
- The plaintext is a JSON object: `v` 1, `binding`, an object holding the
  binding members again, and `values`, mapping each name in `credentials`
  and `task_credentials` to its value, `s3-inputs/<name>` for each S3 input
  to its presigned GET URL, and `s3-outputs/<name>` for each S3 output to
  its presigned PUT URL, each as padded standard base64, and
  `redaction_key`, the run's 32-byte redaction key as padded standard
  base64. Presigned URLs travel nowhere else on the broker.
- A seal is at most 64 KiB in compact form. A host refuses to build a larger
  one and logs the credential names. At submit, a host refuses a run with S3
  inputs or outputs whose seal of its task credentials and presigned URLs
  would exceed 64 KiB, counting no `command` source, which runs only to
  answer a seal.

The receiver reads the host's topic from its end at the time of the request. It
refuses a seal over 64 KiB, and opens a seal only when the header's `typ`
is the seal type and its binding equals the request, with `key_id` the
thumbprint of the receiver's own key, `expiry_at_ms` has not passed, the
plaintext has `v` 1 and its `binding` equals the header's, `values` holds
exactly the named credentials and URLs, and `redaction_key` is 32 bytes.
It checks for a cancel, its shutdown, the run deadline and the request's
expiry before it takes a seal, so a run stopped while its seal arrives is
never dispatched. It passes the values to the adapter in Dispatch. It logs
a seal that does not open by run and attempt, never the reason.

Before it uses a URL, the receiver checks it against the pool's
`storage`: its scheme is `https`, its host and port, 443 when absent, are
the endpoint's, it carries no user information, fragment or opaque part,
and its escaped path is `/<bucket>/` and the object's key exactly. A URL
that fails ends the run `TASK_STATE_FAILED` with
`the URL for S3 input <name> is not one of pool <pool>'s storage`, or
`S3 output`, before anything is dispatched, and the task is not returned to
the pool.

Before it signs any transcript chunk, output chunk, result text or status
message of the attempt, the receiver replaces every occurrence of each
credential value its seal delivered, task credentials and presigned URLs
included, and of each URL's `X-Amz-Signature` value alone, with the value's
marker: `[redacted:`, 16 base64url characters and `]`, 27 bytes. The characters
are the first 96 bits of the HMAC-SHA256, under the run's redaction key, of
`wherry-redaction-v1` followed by the value's name: the credential's name, or
`s3-inputs/<name>` or `s3-outputs/<name>` for a URL and its signature. Every
form of a value takes the same marker, and every attempt of the run the same
markers. No marker maps to a credential name without the run's redaction key.
Seal requests and the failure that ends an unanswered seal wait name
credentials, by design. When
a form of any delivered value occurs within a marker, that marker is `[*]`
instead, so a replacement cannot publish another value. The data passes through
the replacement in pieces of 64 KiB. It replaces two forms of each value: its
bytes without trailing CR and LF, since a credential file usually ends in a
newline that transcripts and outputs drop, and those bytes escaped as a JSON
string, as a transcript line carries them. The value's own bytes contain the
first form, so its redaction leaves only the line ending. It redacts an output
file whole before it splits it into chunks and computes its hash and size. In
result metadata it replaces within JSON string values only, so the metadata
stays valid. The marker can be longer than a value, so the limits hold for the
redacted bytes: an output whose redacted form is over the pool's `output_limit`,
or would take the run's outputs over it, is listed as `exceeds output limit`; a
transcript line whose redacted form is over 512 KiB becomes an
`unstored:oversize` part with that form's hash and size; and a result text,
metadata string or status text whose redacted form is over 64 KiB is replaced by
`[redacted text over 65536 bytes]`. A result whose agent fields are 16 KiB of
8-byte values, each replaced by its 27-byte marker, still fits a default
producer batch. A value shorter than 8 bytes once trimmed is not redacted; the
receiver logs `credential <name> too short to redact` once for the attempt. A
value in any other encoding, such as base64, is not replaced. An adopted attempt
redacts the values its own seal delivers. Each form is replaced once, longest
first, and a marker can join the bytes beside it into a value that was not there
before: when a value starts with a marker's tail, such as `]`, and the rest of
it follows a replaced value, the published text holds that value whole.

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

`terms` has the members of the [pool terms](configuration.md#pool-terms) file
and `record_limit`, the tasks topic's record limit that `pool create` reads
from the broker, with `max_queue_deadline_ms` and `lock_duration_ms` in milliseconds for its
`max_queue_deadline` and `lock_duration`. `config` holds `receiver` and `pool`,
which equal the record's, the receiver's `capacity`, and its `profiles` in
routing order, the plugins in the order of their types. Each profile is the
[profile entry](configuration.md#receiver-configuration) as `name`, `plugin`
naming its plugin's type, `agent`, `labels` when set, and the merged
`config`, which keeps a `credential` name and never a credential value. An `end` names
exactly one of `receiver`, or `tenant` and `host`.
Readers fold the records in order. A reader loads no state when:

- a `receiver` record moves a receiver to another pool or names an unknown
  pool;
- a `receiver` or `host` record follows that member's `end`;
- an `entitlement` names an unknown pool;
- a record fails to decode and no `skip` matches it;
- a `skip` names no undecodable record with that hash;
- two of the Kafka names that pools, receivers and hosts derive (topics,
  share groups, principals and transactional prefixes) collide.

When every undecodable record no `skip` matches fails only on unknown
members, the reader reports that a later wherry release wrote them and that
this one needs an upgrade.

## Admission

A receiver that claims a submission:

1. acknowledges it and writes nothing when the pool's events already hold a
   status in a terminal state, `TASK_STATE_REJECTED` included, with its task
   hash;
2. drops it when it fails the [signed-record checks](#signed-records), or
   when its Kafka key is not its `run`;
3. rejects it with `run ID in use` when the run ID belongs to another
   tenant: the tenant of the run's earliest submission on the pool's task
   topic that a live host's current key verifies, or of the run's earliest
   status other than a rejection on the pool's events. Ownership survives
   the owner's host being ended, since the run's statuses still name its
   tenant. This check comes before any record is written under the run ID;
4. rejects it when its agent is outside the pool's terms; with
   `model not in the pool's terms` when the agent has `models` and the
   placement names no model or one outside the current `models`, or the
   agent has no `models` and the placement names one;
5. releases it when no ready profile satisfies the placement;
6. rejects it with `duplicate submission` when it is not the run's earliest
   submission on the pool's task topic that a live host's current key
   verifies;
7. rejects a run no pool member has claimed with `run expired` when its
   submission reached the pool's task topic more than the pool's
   `max_queue_deadline_ms` ago, and with
   `queue deadline exceeds the pool's max queue deadline` when its queue
   deadline falls more than `max_queue_deadline_ms` after that;
8. rejects a run no pool member has claimed when its queue deadline or run
   deadline has passed;
9. rejects it when the host of the run's [git input](#git-input) is not
   one of the pool's current VCS hosts, and with `pool <pool> has no
   storage for S3 inputs and outputs` when the run has S3 inputs or
   outputs and the pool's terms have no `storage`;
10. rejects it with `unsupported message part` when a `url` part appears
    in the message;
11. writes the echo and the `claimed` status.

A run a cancel covers skips steps 8 to 10, and ends `TASK_STATE_CANCELED`
after its `claimed` status. A redelivered run that a member has claimed
goes on to adoption past its deadlines. When its run deadline has passed,
the receiver stops the job it finds and ends the run `TASK_STATE_FAILED`
with `timeout`.

## Git input

A submission's `git` names a repository the agent clones itself. wherry
runs no git: it carries the repository's description, delivers the host
key and the credential as files, and gives the agent the commands.

- `remote` (string, required): the repository's `https` or `ssh` URL, at
  most 2048 bytes of letters, digits, `.`, `_`, `~`, `%`, `+`, `@`, `:`,
  `/` and `-`, naming a host and a path. An `https` remote carries no user
  information; an `ssh` remote may name a user, without a password.
- `ref` (string, required): a branch, tag or commit, at most 255 bytes.
- `path` (string, optional): the checkout directory, a relative path of at
  most 512 bytes, taken from the agent's working directory. It defaults to
  the last segment of the remote's path without a `.git` suffix.
- `credential` (string, optional): the `name` of one of the submission's
  `credentials` delivered as a file, which the agent clones with: a token
  for an `https` remote, a private key for an `ssh` one.
- `host_key` (string): OpenSSH `known_hosts` lines for the remote's host,
  at most 8 KiB of printable ASCII with no tab or other control character,
  each blank, a comment or an entry of at least three space-separated
  fields after an optional marker such as `@cert-authority`, with at least
  one entry. An `ssh`
  remote requires it and an `https` remote refuses it.

A ref, and each segment of a path, starts with a letter, digit or
underscore and holds only letters, digits, `.`, `_` and `-`. A ref can also
hold `/`. It cannot hold `..` or `//`, or end in `/`, `.` or `.lock`.
Readers reject a submission that breaks these rules.

The remote's host must be one of the pool's
[`vcs`](configuration.md#pool-terms) hosts: its host name in lowercase, with
`:port` when the remote names a port other than its scheme's default, 443
for `https` and 22 for `ssh`. A pool without `vcs` accepts no git input.
Clients refuse a git input outside the VCS hosts before they publish, and
receivers reject a run whose git input is outside the pool's current VCS
hosts, with `git remote host <host> is not in pool <pool>'s
vcs hosts`.

An `ssh` remote's `host_key` entries must each name its host, as known_hosts
does: the host, or `[host]:port` for a port other than 22, among an
entry's comma-separated patterns. Hashed entries, and entries with any
`*` or `?` pattern, are refused.

The receiver delivers `host_key` as the input file `known_hosts`, which no
`raw` or `data` part or S3 input of the run may also name, and opens the
prompt with the commands that clone the remote at the ref, each on its own line
indented by four spaces. They need `git` 2.30 or later, and the prompt asks
the agent to check. The clone takes no template (`--template=`) and sets,
for itself and every later command in it:

- `credential.helper` empty, which resets the helpers, then for an `https`
  remote with a credential, `credential.<scheme>://<host>[:<port>].helper`:
  a helper that answers only `get`, with the user name `x-access-token` and
  a password it reads from the credential file;
- for an `ssh` remote, `core.sshCommand`: `ssh` with `-F /dev/null`, `-i`
  naming the credential file (or `IdentityFile=/dev/null` without one),
  `IdentitiesOnly=yes`, `IdentityAgent=none`, `UserKnownHostsFile` naming
  `known_hosts`, `GlobalKnownHostsFile=/dev/null`,
  `StrictHostKeyChecking=yes`, `BatchMode=yes` and `ProxyCommand=none`;
- `http.followRedirects=false`, `http.sslVerify=true`,
  `protocol.allow=never` and `protocol.<scheme>.allow=always`;
- `user.name` and `user.email`.

Then `git checkout <ref>`, and `refs/wherry/start` set to the commit
checked out.

Every command starts with `git`, with no environment assignment or
redirect, so a harness rule that allows `git` commands allows them all.
Such a rule grants the commands; it confines nothing, since `git` can run
any command.
The commands read the credential from its file, so its value is on no
command line and in no environment variable or prompt. When the run
expects the output `changes.patch`, the lines give the
`git format-patch --output=<file> refs/wherry/start..HEAD` command that
writes it; for `changes.bundle`, the `git bundle create` command for the
same range. They ask the agent to push only when the request says so, and
then only to the branch `wherry/<run>`.

An adapter gives its agent an environment in which `git` reads no system,
global or environment configuration, uses no template, never prompts and
traces nothing (`adapter.AgentEnvironment`), and removes from the
environment it configures the variables no value switches off
(`adapter.AgentUnset`), so the platform's configuration does not rewrite,
redirect, record or trace the clone. The platform's own environment and an
image's are the operator's to keep clear of them. The pool's VCS hosts
bound what a receiver accepts; the platform's egress policy bounds where
the agent connects.

[Operations](operations.md#git-repositories) states the limits
of these settings against an agent that runs commands.

## Inputs and outputs

The agent finds three directories through environment variables that hold
their paths:

- `WHERRY_INPUT_DIR`: the run's input files, from the `raw` and `data`
  parts of its message, under their delivered names, its S3 inputs, and
  `known_hosts` for a git input with a host key.
- `WHERRY_CREDENTIALS_DIR`: the run's credentials with a `file` target, the
  agent's and the task's, each as the file its `file` names. A credential
  with an `env` target is in that environment variable instead. The adapter
  delivers credential values only at their targets, and never into the
  input directory.
- `WHERRY_OUTPUT_DIR`: where the agent leaves its outputs. The agent
  creates the directory when it does not exist.

`WHERRY_MODEL` holds the placement's model when the placement names one,
and is not set otherwise.

The receiver opens the prompt with lines giving the absolute path of each
directory, of each input file, of each credential file and of each expected
output, with `(required)` after a required one, so an agent without a shell
finds them, the environment variables that hold credentials, the commands of
the [git input](#git-input), and the
[outcome contract](configuration.md#agent-contract) with the run's credential
names, an optional credential its host did not supply marked
`(not supplied)`. The lines name S3 inputs by path, never by URL. The adapter reports the
directories' paths as its agent sees them; a directory it does not report
appears as its variable. The lines hold no credential value.

`.wherry/` beneath the outputs directory is reserved for the outcome file. No
output name leads into it, and the receiver collects nothing in it as an
output. A host writes the run's transcript among its outputs as
`transcript.jsonl`, so a submission expecting an output of that name is
invalid.

When the job ends, the receiver stops it, then asks the adapter to collect the
outputs, whatever the job's exit. The adapter lists every entry beneath the outputs
directory without following links, and refuses the whole tree, reading nothing,
when it holds more than 256 entries. It reads only regular files: one at most
the pool's `output_limit`, 8 MiB unless its terms set another, and together at
most that, which the receiver checks again. Every other entry is listed with its
type and left unread. The receiver gives up on a collection that takes over 2
minutes. An adapter that cannot collect, or a job that could not be stopped,
leaves the outputs `not collected`.

The receiver checks every name, redacts every file, and publishes each
returned file as [output chunks](#output-chunk) and in the result's
[metadata](#metadata). A required output listed in `not_returned` ends
the run `TASK_STATE_FAILED` with
`required output <name> not returned: <reason>`; an optional one leaves
the state as the agent's outcome file set it.

A host takes a run's outputs from the attempt and receiver of the result
that ended it, ignoring every other attempt's chunks. For each output the
result lists, it joins the chunks by index and rejects the output when an
index is missing (`gap`), when chunks follow the last one (`overlap`), when
an index appears twice (`duplicate chunk`), or when the joined bytes differ
from the listed size or SHA-256 (`size or hash mismatch`). It writes only
outputs that pass, and reports each rejected output with the reason after
`rejected by this host: `.

## S3 inputs and outputs

A pool whose terms name `storage`, an S3-compatible endpoint and bucket,
carries files of up to 5 GiB each beside the broker. Each run's objects
live under `wherry/<tenant>/<run>/`: an S3 input at `inputs/<name>`, and an
S3 output at `outputs/<name>`. The host's own credential,
scoped by the operator to `wherry/<tenant>/`, uploads, downloads and
deletes them, and never leaves the host. Receivers and adapters get only
the presigned URLs in the [seal](#seal), checked against the pool's
storage before use.

- **In.** The host uploads each S3 input before it publishes the run and lists
  it in `s3_inputs`. The adapter fetches it into `WHERRY_INPUT_DIR` from its
  presigned GET before the agent starts, and verifies its size and SHA-256. The
  adapter never exposes the GET URL to the job: an adopter's seal carries no GET
  URL, so an adopter cannot redact it. A fetch that fails or does not verify
  ends the job before the agent starts, and the receiver ends the run
  `TASK_STATE_FAILED` with `an S3 input was not fetched, or did not match its
  size and SHA-256`, without returning the task to the pool. S3 inputs count
  against neither `record_limit` nor `inline_limit`.
- **Out.** Once the job ends and is stopped, the adapter
  streams each S3 output it finds as it collects, and the receiver sends it
  to its presigned PUT as it reads it, with the size the adapter listed as
  its Content-Length, hashing the bytes it sends. It follows no redirect.
  An S3 output counts against neither `output_limit` nor the run's outputs
  together, but is at most 5 GiB, and among the 256 outputs a submission
  expects. Collection waits until the run's deadline, when the PUT URLs
  expire, if that is later than 2 minutes. The receiver does not redact an
  S3 output, which goes to the host's own bucket and never to the
  transcript topic. An attempt that adopted an earlier attempt's job uploads
  to the PUT URLs its own seal carries.
- **Back.** The host downloads each S3 output in the result's
  `s3_outputs` that the run declares, at the key for its name, and keeps it
  only when it matches the listed size and SHA-256. It reports each other
  S3 output with `rejected by this host: ` and one of
  `not an S3 output the run declares`,
  `listed at a key outside the run's prefix`, `not downloaded`,
  `not written` or `size or hash mismatch`. Once the run is terminal
  and every listed S3 output has verified, it deletes every
  object whose key a listing of `wherry/<tenant>/<run>/` returns and that
  starts with it. A bucket lifecycle rule expiring `wherry/` removes what no
  host deletes.

## Cancellation

Receivers read the pool's events every 10 seconds, and when they claim a
submission, and act on each run a valid cancel names:

- A receiver that claims a cancelled run starts no job. If an earlier
  attempt left the run's job running, it stops that job first.
- A receiver running a cancelled run stops its job within a second of
  reading the cancel.

In each case the run ends with a result and a `terminal` status in
`TASK_STATE_CANCELED`, with the message text `canceled by user`. If
stopping a job fails, the receiver releases the submission without a
terminal status, and the next delivery tries again. A run that ends before
a receiver acts on the cancel keeps the state it ended in.
