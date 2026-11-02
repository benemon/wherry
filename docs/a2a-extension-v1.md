# wherry A2A extension, version 1

URI: `https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md`

**Change note, 2026-11-01 (custody coordination):** edited in place at
version 1. An [advertisement](#advertisement) no longer carries
`started_ms` or `stepping_down`, and a
[custodian snapshot](#custodian-snapshot)'s `reason` is no longer
`handover`: every custodian change publishes `takeover` or `contested`.
A snapshot with reason `handover` is refused, as snapshots are decoded
strictly; that is the change's one incompatibility. An earlier
advertisement's `started_ms` or `stepping_down` is an unknown member,
ignored as on any record, and has no effect. A host no longer refuses to
submit when the current snapshot's custodian has no live advertisement:
advertisements place tasks and custody follows the custodian group's
session.

**Change note, 2026-11-01 (pool key):** edited in place at version 1. A
pool's key custodian replaces the seal exchange: the seal request a
receiver wrote to the tenant feed and the seal its host answered with are
removed, with the seal request's `expiry_at_ms`. A submission carries
its credentials, presigned S3 URLs and redaction key in an `envelope`, with
its content key in a submit `wrap` to the pool's current key
([Submission](#submission)). A claimant writes an
[open request](#open-request). The custodian answers it after the
[claim check](#the-claim-check) with an [attempt wrap](#attempt-wrap) or an
[open refusal](#open-refusal), in one transaction with a
[claim decision](#claim-decision). A receiver holding a replaced pool key
writes a [re-key wrap](#re-key-wrap) to the current key. The custodian
publishes a [custodian snapshot](#custodian-snapshot) on the pool's
`wherry.pool.<pool>.custodian` topic. A [resubmission](#resubmission)
carries `replaces` and `s3_input_run`. Statuses add the kinds
`released`, `adopting` and `reconciling`, and a `terminal` status the
wherry event `key-lost`.

**Change note, 2026-10-29 (A2A conformance):** edited in place at version 1.
A run that ends for want of a credential ends `TASK_STATE_FAILED` with a
[`credential` data part](#the-credential-data-part) for each credential,
when its seal goes unanswered and when its agent reports
`TASK_STATE_AUTH_REQUIRED`. wherry no longer treats
`TASK_STATE_AUTH_REQUIRED` as terminal. A submission's message has `role`
`ROLE_USER`. An output chunk's artifact has `name`, and its part
`filename` and `mediaType`. Content a record does not carry, an input in a
result's history or a transcript line over 512 KiB, is a
[reference part](#reference-parts) in place of a `url` part
`unstored:input` or `unstored:oversize`. Every run's transcript ends with a
`lastChunk` chunk, an empty one when no line is left for the terminal
transaction. The result's reply keeps the part type of the agent's outcome
message, `data` or text, with no test for JSON. The agent contract states
that an outcome message's other parts are dropped. The extension reserves
the bare `metadata` keys wherry writes. New sections: [Binding](#binding),
which maps A2A's operations and errors to records, and
[Agent Card](#agent-card). A result's `artifacts` keep the `result`
artifact and add a [reference artifact](#reference-artifacts) for each
output it returns, its reply chunks and its transcript, so `result` and
`transcript` are reserved output names. Tenant feeds and transcript topics
take records of up to 2 MiB.

**Change note, 2026-10-29:** edited in place at version 1. A reply over
64 KiB once redacted travels as [reply chunks](#the-reply), output chunks with
`artifactId` `.wherry/reply`, up to the pool's `reply_limit`. The result text
and the status text carry its first 64 KiB, the result's metadata lists it as
`reply`, and `[redacted text over 65536 bytes]` no longer replaces either. The
outcome file is bounded by the pool's `output_limit`. A host reserves
`reply.txt` among a run's outputs.

**Change note, 2026-10-27:** edited in place at version 1. The term adapter
is retired in favour of plugin, and the output reason `not streamed by the
adapter` is renamed `not streamed by the plugin`.

**Change note, 2026-10-27:** edited in place at version 1. Chunks and
output chunks move from the tenant feed to the tenant's transcript topic,
`wherry.tenant.<tenant>.transcript`, and a `terminal` status carries
`transcript_chunks`. The `heartbeat` status kind is removed: a receiver
records each held run's committed transcript offset as a
[progress](#progress) record on its own progress topic,
`wherry.receiver.<receiver>.progress`, and an adopter resumes from the
highest of the run's `started` offsets and progress records.

**Change note, 2026-10-25:** edited in place at version 1. The configured
unit a run is routed to is a plugin profile, a named, configured use of a plugin.
The member `adapter` of advertisement offers, start failures and result
metadata is renamed `profile`. The receiver configuration on
`wherry.operator` holds `profiles` in place of `adapters`, each with `plugin`
in place of `type` and no `platform`. Messages that named the adapter name
the profile.

Hosts and receivers exchange A2A tasks over Kafka with no server between
them. wherry is a partial custom A2A protocol binding over Kafka: it
provides some of A2A's operations, as records ([Binding](#binding)). This
extension fixes the record formats and the checks each reader applies. Five
of the records carry [A2A v1.0](https://a2a-protocol.org/) objects:
submissions, statuses, transcript chunks, output chunks and results hold
one under its A2A field name, beside a `wherry` object of the run's
placement and routing. Every `Message` and `Artifact` wherry writes
lists the URI above in its `extensions`, and readers reject one that does
not. [Key concepts](concepts.md) explains the roles this document names, and
[Topics](design.md#topics) lists the topics.

## Encoding

- Payloads are JSON objects. Readers ignore unknown members and reject
  duplicate names. A member added after wherry 1.0 states, beside it, what a
  reader that ignores it does.
- Every payload has `v`, the payload version, which is `1`. Readers reject
  any other version; a reader reports an [operator record](#operator-records)
  with a higher version as written by a later release.
- A2A objects use the
  [proto3 JSON mapping](https://protobuf.dev/programming-guides/json/) of
  the A2A v1.0 schema. wherry writes `bytes` as padded standard base64.
  Readers accept standard or URL-safe base64, padded or not.
- Every `TaskStatus` has `timestamp`, an RFC 3339 time. Receivers write it
  in UTC with milliseconds.
- This extension reserves the bare `metadata` keys wherry writes: in a
  result's task `metadata`, `receiver_id`, `pool`, `profile`, `labels`,
  `job`, `outputs`, `s3_outputs`, `not_returned` and `reply`
  ([Metadata](#metadata)); in an output chunk's `artifactUpdate.metadata`,
  `index`. They carry these meanings in every wherry record, and no other
  extension's key takes their names there.
- A `Task` has `artifacts` and `history`, and a `Message` and an `Artifact`
  have `parts`, each present even when empty. A `data` part holds valid
  JSON.
- A2A identifiers (`messageId`, `taskId`, `contextId`, `artifactId`) and
  wherry's tenant, host, receiver, pool, plugin profile, agent and event IDs are
  non-empty and lowercase. `metadata`, wherever it appears, is a JSON object.
- A task hash is the lowercase hex SHA-256 of a submission's record value,
  the compact JWS bytes as they appear on the topic.
- Run IDs match `[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`. A run's
  transactional ID is `p.<pool>.<run>`. A receiver's admission
  transactional ID is `p.<pool>.admission.<receiver>`.

### Reference parts

A reference part is a `data` part that stands in for content its record does
not carry. Its `mediaType` is `application/vnd.wherry.reference.v1+json`,
which this extension defines, and its data is an object:

```json
{"data": {"sha256": "9f86...", "size": 600000, "unstored": "oversize"}, "mediaType": "application/vnd.wherry.reference.v1+json"}
```

- `sha256` is the content's lowercase hex SHA-256, and `size` its size in
  bytes, 0 or more.
- `unstored` says why the content is not stored: `input` for an input the
  submission carried, `oversize` for a transcript line over 512 KiB.
- Readers reject a part with this media type whose data lacks a valid
  `sha256`, has a negative `size`, or has another `unstored`, except in a
  result's [reference artifacts](#reference-artifacts).

### Reference artifacts

A result names each artifact its run streamed, after its `result`
artifact, as a reference artifact: one [reference part](#reference-parts),
with no `filename`, that stands in for the chunks of the artifact with the
same `artifactId`. It declares the extension, as every artifact wherry
writes does, and has no `unstored`, since the content is stored, in chunks:

```json
{"artifactId": "out/report.md", "name": "out/report.md", "parts": [{"data": {"sha256": "9f86...", "size": 1200}, "mediaType": "application/vnd.wherry.reference.v1+json"}], "extensions": ["https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md"]}
```

- An output the run returned has its name as `artifactId` and `name`, and
  data `{"sha256", "size"}` of the bytes its
  [output chunks](#output-chunk) carry, as `outputs` in the
  [metadata](#metadata) lists them.
- The [reply chunks](#the-reply), when the reply is over 64 KiB, have
  `artifactId` `.wherry/reply`, no `name`, and data `{"sha256", "size"}`
  as `reply` in the metadata lists them.
- The [transcript](#chunk), once it has a line, has `artifactId`
  `transcript`, no `name`, and data `{}`. A receiver that adopts a run
  cannot hash or count an earlier attempt's lines, and a reader reads the
  transcript to its `lastChunk` chunk, so the artifact names it alone.
- They follow the `result` artifact in that order: the outputs in name
  order, the reply, then the transcript. An S3 output is not streamed and
  has none.
- Readers reject a result whose artifact IDs repeat, and a reference
  artifact other than these.

`outputs` and `reply` in the metadata stay the manifest a host verifies
chunks against; the reference artifacts make the task an A2A reader can
follow to them.

wherry writes no `url` part.

## Signed records

Records on topics several principals write are compact
[JWS](https://datatracker.ietf.org/doc/html/rfc7515) with one signature,
algorithm `EdDSA` (Ed25519). The protected header holds:

- `typ`: the record type below;
- `kid`: `<owner>/<key id>`. The owner of a host record is
  `<tenant>.<host>`; the owner of a receiver record is the receiver ID. The
  key ID is the [RFC 7638](https://datatracker.ietf.org/doc/html/rfc7638)
  SHA-256 thumbprint of the Ed25519 public key's JWK, unpadded base64url.

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
| open request | `application/vnd.wherry.open-request.v1+jws` | receiver | `wherry.pool.<pool>.events` | run ID |
| attempt wrap | `application/vnd.wherry.attempt-wrap.v1+jws` | receiver, the custodian | `wherry.pool.<pool>.events` | run ID |
| re-key wrap | `application/vnd.wherry.rekey-wrap.v1+jws` | receiver | `wherry.pool.<pool>.events` | run ID |
| open refusal | `application/vnd.wherry.open-refusal.v1+jws` | receiver, the custodian | `wherry.pool.<pool>.events` | run ID |
| claim decision | `application/vnd.wherry.claim-decision.v1+jws` | receiver, the custodian | `wherry.pool.<pool>.events` | run ID |
| custodian snapshot | `application/vnd.wherry.custodian.v1+jws` | receiver | `wherry.pool.<pool>.custodian` | `custodian` |

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
3. The payload decodes, has `v` 1 and passes the rules for its type.
4. The owner the payload names equals the `kid` owner.
5. The record passes the checks against `wherry.operator` for its type:
   - A submission or cancel comes from a host on `wherry.operator`, without a
     membership end, of a tenant entitled to the pool. Its `pool` is the
     pool of the topic it arrived on.
   - A status, chunk, output chunk, result or start failure comes from a
     receiver on `wherry.operator`, without a membership end, in the pool the run
     was placed in. Its `tenant` and `pool` are the run's, and the tenant is
     entitled to the pool.
   - An open request, attempt wrap, re-key wrap, open refusal or claim
     decision comes from a live member of the pool of its topic
     ([Open request](#open-request)).
   - An echo on `wherry.tenant.<tenant>` names that tenant, and comes from a
     live member of the pool it names, to which the tenant is entitled.
   - An advertisement comes from a live member of the pool it names.
   - A [custodian snapshot](#custodian-snapshot) comes from the live member
     of the pool of its topic that it names as custodian.

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
    "run_nonce": "ZrHrUw-3SKTyOPCMstQuRA",
    "placement": {
      "agent": "coder",
      "selector": [{"key": "isolation", "operator": "In", "values": ["container"]}],
      "model": "sonnet"
    },
    "task_started_by": 1789998800,
    "task_ended_by": 1790000000,
    "credentials": [{"name": "deploy-key", "file": "git-credential"}, {"name": "vault-token", "env": "VAULT_TOKEN"}],
    "unsupplied_credentials": ["github-token"],
    "outputs": [{"name": "report.md", "required": true}, {"name": "plot.pdf", "storage": "s3"}],
    "s3_inputs": [{"name": "dataset.parquet", "size": 5242880, "sha256": "4f1c..."}],
    "git": {
      "remote": "ssh://git@git.example.com/team/app.git",
      "ref": "main",
      "credential": "deploy-key",
      "host_key": "git.example.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA...\n"
    },
    "replaces": "run-7a21...",
    "s3_input_run": "run-7a21...",
    "envelope": "eyJhbGciOiJkaXIi...",
    "wrap": "eyJhbGciOiJFQ0RILUVTK0EyNTZLVyIs..."
  }
}
```

- `message` is the run's A2A `Message`, with `role` `ROLE_USER`. Readers
  reject a submission with another role. Its `taskId` and `contextId` are
  the run ID.
- `run_nonce` is 16 random bytes in base64url without padding: exactly 22
  characters that decode, strictly, to 16 bytes. `run` is `run-` and the
  first 32 lowercase hex digits of the SHA-256 of `wherry-run`, `tenant`,
  `host` and `run_nonce`, joined by NUL bytes. A host draws a fresh
  `run_nonce` for each run, so a run ID is bound to the host that signs it.
- `placement.agent` is an agent from the pool's terms. `placement.selector`
  is a list of `{key, operator, values}` requirements: `operator` is `In`,
  `NotIn`, `Exists` or `DoesNotExist`; `In` and `NotIn` take one or more
  unique values, `Exists` and `DoesNotExist` none; each key appears once.
  `placement.model` is one of the agent's `models` in the pool's terms, and
  absent for an agent with none.
  [Commands](commands.md#labels) gives the key and value grammar.
- `task_started_by` and `task_ended_by` are JWT NumericDates
  ([RFC 7519](https://datatracker.ietf.org/doc/html/rfc7519)): whole seconds
  since the epoch, UTC, each 0 or more. The run's agent job must start by
  `task_started_by`, and the run must reach a terminal state by
  `task_ended_by`. The receiver's clock decides both bounds. A bound is
  passed only when that clock is past its second, so a job that starts at
  `task_started_by`, or a terminal state at `task_ended_by`, meets it. A
  receiver ends the run no later than six hours after `task_started_by`.
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
  plugin profiles declare. Over either, a host carries the inputs as
  [S3 inputs](#s3-inputs-and-outputs) when the pool has storage and the host
  a storage credential, and otherwise refuses the run.
- `credentials` lists task credentials, each `{name, env}` or `{name, file}`:
  a credential of the host, by the name in its configuration, that the agent
  gets in the environment variable `env` or as the file `file` in its
  credentials directory. `name` is at most 32 lowercase letters, digits and
  hyphens, starting and ending with a letter or digit, and appears once.
  Each credential has exactly one target: `env` matches
  `[A-Za-z_][A-Za-z0-9_]*` and is no variable a plugin sets
  (`pluginsdk.ReservedEnv`), and `file` follows the file name rules. No two
  credentials share a target, and no task credential takes the target of one
  of the agent's credentials in the pool's terms. A task credential may
  share its name with one of the agent's credentials; the envelope carries the
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

- `replaces` and `s3_input_run` appear together, on a resubmission alone.
  `replaces` is the run, other than this one, that the resubmission
  replaces, and `s3_input_run` the run under whose prefix its S3 inputs lie,
  the first run of the chain of replacements. Both are run IDs. A
  resubmission's `run` derives from its own `run_nonce`.
- `envelope` and `wrap` appear together, as ordinary members of `wherry`.
  `envelope` is a compact JWE of at most 64 KiB, protected header exactly
  `alg` `dir`, `enc` `A256GCM` and `typ`
  `application/vnd.wherry.envelope.v1+jwe`, encrypted under a fresh 256-bit
  content key. Its plaintext holds exactly `binding`, the task's `run`,
  `tenant`, `pool`, `task_started_by` and `task_ended_by`; `credentials`,
  each credential the run delivers by name, its value in standard base64;
  `s3_inputs` and `s3_outputs`, each S3 input and S3 output by name, its
  presigned URL; and `redaction_key`, the run's 32-byte key in standard
  base64. `wrap` is a compact JWE carrying the content key to the pool's
  current key from its [custodian snapshot](#custodian-snapshot), protected
  header exactly `alg` `ECDH-ES+A256KW`, `enc` `A256GCM`, `kid`, the
  recipient key's RFC 7638 thumbprint, and `epk`, a P-256 public JWK of
  `kty`, `crv`, `x` and `y`. The host signs the task with both in it as any
  record.
- An envelope whose `binding` differs from its task's `run`, `tenant`,
  `pool`, `task_started_by` or `task_ended_by` is refused: by the custodian
  before it wraps the content key to a claimant, and by the claimant at open.
- The envelope's credentials are exactly the agent's credentials in the
  pool's terms other than `unsupplied_credentials`, and the task's
  `credentials`, each once; its S3 inputs are exactly `s3_inputs` and its S3
  outputs exactly the outputs with `storage` `s3`. Each URL is https, or
  http for a loopback storage endpoint, of the storage endpoint's scheme,
  host and port, with no user, opaque part or fragment, and the path of its
  object: the bucket and `wherry/<tenant>/<s3_input_run>/inputs/<name>`, or
  `wherry/<tenant>/<run>/outputs/<name>`, where a run without
  `s3_input_run` is its own. No value is empty. A host refuses to build an
  envelope that breaks these rules.
- A host presigns an S3 input's GET until `task_started_by` plus 15
  minutes, and an S3 output's PUT until `task_ended_by`, each at most 7
  days.

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
- `task_offset` is the offset of the claimed submission on the pool's task
  topic, whose one partition is 0, on `claimed` only, where it is required.
  A status of another kind carrying it, or a `claimed` status without it,
  fails validation.
- `log_offset` is the agent output offset an adopter follows from, on
  `started` only. The transcript is published up to at least that offset.
  Later offsets go to the receiver's [progress](#progress) topic.
- `event` is the wherry event `key-lost`, on a `terminal` status with
  state `TASK_STATE_FAILED` alone, absent otherwise. It marks a run that
  failed before its job started because no holder of the pool key its
  submit wrap names remained. The run's host
  [resubmits](#resubmission) it.
- `transcript_chunks` is the number of [chunks](#chunk) the attempt wrote,
  the empty final chunk included, on `terminal` only, and absent when zero. It commits in the same
  transaction as the attempt's last chunks.

| `kind` | State | Written when | Extra |
| --- | --- | --- | --- |
| `claimed` | `TASK_STATE_WORKING` | the receiver admits the run | `task_offset` |
| `started` | `TASK_STATE_WORKING` | the job has started | |
| `rejected` | `TASK_STATE_REJECTED` | the receiver refuses the submission | the reason as message text |
| `terminal` | `TASK_STATE_COMPLETED`, `TASK_STATE_FAILED` or `TASK_STATE_CANCELED` | the run has ended | the agent's message from its [outcome file](configuration.md#agent-contract), or the reason as message text |
| `released` | `TASK_STATE_SUBMITTED` | the receiver gives back its claim, before it returns the task to the pool | |
| `adopting` | `TASK_STATE_WORKING` | the receiver found the run's job running or ended, before its [open request](#open-request) | |
| `reconciling` | `TASK_STATE_WORKING` | the receiver, refused `key unavailable`, holds the claim while Lookup does not answer ([Open refusal](#open-refusal)) | |

A receiver writes `released`, `adopting` and `reconciling` to the pool's
events alone. It writes each other status to the pool's events and the tenant
feed, with identical bytes, in one Kafka transaction. A run's chunks and output
chunks commit in the transaction of the status, result or
[progress](#progress) record they belong to, so a transaction spans the
tenant feed, the transcript topic, the pool's events and the receiver's
progress topic. Readers read committed records only. A reader sees
each topic's commit separately, so a `terminal` status can be visible before
its attempt's last chunks. A host that reads a `terminal` status on the feed
reads the transcript topic to its last stable offset, and waits until that
reaches the high watermark it lists after it reads the terminal, before it
reports the run's transcript or outputs complete. A host reads a run's
chunks from the first transcript-topic offset at or after one minute before
the append time of the run's earliest echo on the feed, found by
`ListOffsets` by timestamp. The minute allows for the clocks of the two
topics' leaders to differ.
Readers order a run's statuses and results by `attempt`, and records of an
earlier attempt do not change the outcome of a later one.

A status message is an A2A `Message` with `role` `ROLE_AGENT` and one text
part. The message of a run that ends for want of a credential adds a
[`credential` data part](#the-credential-data-part) for each credential. The agent's text is
its [reply](#the-reply): all of it when it fits 64 KiB once redacted, and
otherwise its preview. It travels as a `raw` part with media type
`text/plain` when escaping it as a JSON string would make it longer than
base64 does. The fixed texts are:

- rejected: `agent is outside the pool terms`,
  `model not in the pool's terms`, `duplicate submission`,
  `the task carries no envelope`,
  `task_started_by is past the pool's start_by_limit`,
  `task_started_by is before its append time`,
  `task_ended_by is before task_started_by`,
  `not started by task_started_by (<RFC 3339 time>)`,
  `not ended by task_ended_by (<RFC 3339 time>)`,
  `task_ended_by is past task_started_by plus the 6h run cap`,
  `unsupported message part`, `run ID does not match its signer`,
  `git remote host <host> is not in pool <pool>'s vcs hosts`, and
  `pool <pool> has no storage for S3 inputs and outputs`;
- failed: `not started by task_started_by (<RFC 3339 time>)`,
  `not ended by task_ended_by (<RFC 3339 time>)`, `log stream failed`, `agent exited <code>`,
  `agent ended by <signal>` (each followed by the platform's reason,
  redacted, in parentheses when the plugin gives one, as in
  `agent exited 137 (OOMKilled)`), `agent reported no outcome`,
  `an S3 input was not fetched, or did not match its size and SHA-256`,
  `cannot be opened: the pool's custodian changed and its key was lost`
  ([Open refusal](#open-refusal)), the reason an opened envelope fails its
  check against the task, such as `envelope is bound to another task` or
  `the URL for S3 input <name> is not its object in the pool's storage`,
  `delivery budget exhausted`,
  `start failed and the job could not be removed`,
  `input <name> takes the run's inputs over what profile <profile>
  delivers`, naming the input at which they pass the profile's limit (or
  `an input takes the run's inputs over what profile <profile> delivers`
  when the plugin names no input of the run),
  `required output <name> not returned: <reason>` with a reason from
  [Metadata](#metadata), behind the agent's `agent exited <code>` or
  `agent ended by <signal>` and `; ` when it left no valid outcome file
  and ended by a signal or a non-zero exit. The text carries credential
  names and no values;
- canceled: `canceled by user`.

The `wherry` CLI and MCP server pass the message text through and do not
read it.

### The credential data part

A run that ends for want of a credential ends `TASK_STATE_FAILED`, and its
status message names each credential in a data part:

```json
{"data": {"credential": "deploy-key"}, "mediaType": "application/json"}
```

`credential` is a credential name: at most 32 lowercase letters, digits and
hyphens. The receiver writes one part for the credential an agent names when
its
[outcome file](configuration.md#agent-contract) reports
`TASK_STATE_AUTH_REQUIRED`. The agent's credential name is redacted like any
agent text. It names one of the credentials the prompt lists, including an
optional one the prompt marks `(not supplied)`, or another the run needs and
was not given. The run has ended and is not resumed. `TASK_STATE_AUTH_REQUIRED`
is not a terminal state in wherry, and no record a receiver writes holds it.
The host submits a new run once it holds the credential.

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

- The artifact has `artifactId` `transcript` and exactly one part, or none on an empty final chunk: with media
  type `application/x-ndjson`, a text part holding the line, or a `raw` part
  for an empty line, one that is not UTF-8, or one longer escaped as a JSON
  string than base64-encoded; for a line over 512 KiB, a
  [reference part](#reference-parts) with `unstored` `oversize` and the line's
  `sha256` and `size`. The line itself is then not stored.
- `seq` is the line's byte offset in the agent's output and orders the
  chunks. An adopter follows from the highest offset of the run's
  `started` statuses and [progress](#progress) records, so a
  `seq` can repeat across attempts of a run. A reader takes the latest
  attempt's chunks, and the lines below that attempt's first `seq` from the
  highest earlier attempt that has them, so a run's transcript starts at
  offset 0.
- `append` is false for the line at offset 0 and true after it. `lastChunk`
  is true on the run's final chunk, written with its terminal status. When
  that transaction has no line left to write, the final chunk has an empty
  `parts`, `seq` the offset past the attempt's last line, or the offset it
  resumed from, and readers take no line from it. Every run's transcript
  ends with a `lastChunk` chunk.

A receiver writes chunks to `wherry.tenant.<tenant>.transcript`.

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
  file names joined by `/` that follows the `outputs` name rules, or
  `.wherry/reply` for a chunk of the run's [reply](#the-reply). It has
  exactly one `raw` part holding at most 256 KiB of the file.
- An output's artifact has `name`, the output's name, and its part has
  `filename`, the name's last path segment, and `mediaType`
  `application/octet-stream`. A reply chunk's artifact has none of the
  three.
- `artifactUpdate.metadata` is exactly `{"index": <n>}`, with `n` 0 or
  more. A file's chunks have
  indexes 0, 1, 2 and so on, in file order. `append` is false at index 0
  and true after it. `lastChunk` is true on the file's final chunk. An
  empty file is one chunk with an empty part.
- A chunk's identity is its attempt, artifact and index.

A receiver writes a run's output chunks and reply chunks to
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
  `data` part is replaced by a [reference part](#reference-parts) with the
  input's `filename`, and `unstored` `input` with the `sha256` and `size` of
  its `raw` bytes or its `data` JSON. A
  reader requires `history[0].messageId` to equal the `message_id` of the
  run's echo.
- `task.artifacts` holds the artifact `result`, with one part: the agent's
  [reply](#the-reply) or its preview, or the receiver's reason, as a `data`
  part with media type `application/json` when it is a whole reply the agent
  gave as a `data` part and it is still JSON once redacted, a `raw` part with media type `text/plain` when it is not valid
  UTF-8 or escaping it as a JSON string would make it longer than base64
  does, and otherwise a text
  part. With no result text, the part's text is the state's name. A
  [reference artifact](#reference-artifacts) follows it for each output the
  run returned, its reply chunks and its transcript.
- [Metadata](#metadata) says where the run executed.

A receiver writes the result in the same transaction as the run's
`terminal` status, after the run's last chunks.

### The reply

The reply is the text parts of the agent's outcome message, joined by
newlines, or, in a message with no text part, the JSON of its first `data`
part other than the [`credential` part](#the-credential-data-part). The
receiver drops the message's other parts: `raw` and `url` parts, and further
`data` parts. The reply is redacted whole. Its redacted form is `R`.

| Size of `R` | Result text and status text | Reply chunks | `reply` in the metadata |
| --- | --- | --- | --- |
| at most 64 KiB | `R` | none | absent |
| over 64 KiB, at most the pool's `reply_limit` | the preview | `R` | `{sha256, size}` |
| over the pool's `reply_limit` | the preview | the first `reply_limit` bytes of `R` | `{sha256, size, cut: true}` |

- The preview is the first 64 KiB of `R`. Both cuts fall at a rune boundary
  of the redacted bytes, so no part of a value reaches the feed. A marker
  either cut splits leaves a partial `[redacted:` prefix, which holds no
  value.
- Reply chunks are [output chunks](#output-chunk) with `artifactId`
  `.wherry/reply`, in the transaction that holds the result. `sha256` and
  `size` are of the bytes they carry. `cut` says the redacted reply was
  longer. The run ends in the state its outcome file reports.
- The result forwards the part type the agent used. A whole reply from a
  `data` part travels as a `data` part, and a reply from text parts as text
  or `raw`, whatever it holds. The status text and a preview, cut, travel as
  text or `raw`.

A host verifies the reply chunks as it verifies an output's, from the
result's own attempt and receiver, and names each marker in the joined
bytes, so a marker split across a chunk seam counts once. A rejected reply
leaves the preview. The chunks live under the transcript topic's
retention: a transcript retention shorter than the feed's loses an old
run's reply, and its transcript, before its result, which then reads
`rejected: gap` for the reply.

### Metadata

The `metadata` of a result's `Task` says where the run executed. This
extension reserves its keys ([Encoding](#encoding)):

- `receiver_id` (string): the receiver that wrote the result.
- `pool` (string): the pool.
- `profile` (string): the plugin profile that ran or matched the run. It is
  absent when no ready profile satisfied the placement.
- `labels` (object of strings): the labels of the profile this attempt
  dispatched the job to. It is absent when no job was dispatched, when the
  attempt adopted an earlier attempt's job, and when the profile declares no
  labels.
- `job` (string): the plugin's reference for the job on its platform, for
  diagnosis only, at most 253 bytes. It is absent when no job was dispatched or adopted, and
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
- `reply` (object): the [reply chunks](#the-reply), as `{sha256, size}`,
  with `cut: true` when the reply was over the pool's `reply_limit`. It is
  absent when the result text holds the whole reply.

| `reason` | Meaning |
| --- | --- |
| `missing` | an expected output the agent did not leave |
| `not collected` | the plugin did not collect the outputs; every expected output is listed with it |
| `exceeds output limit` | the file is over the pool's `output_limit`, or would take the run's outputs over it |
| `rejected: symlink`, `rejected: fifo`, `rejected: device`, `rejected: socket`, `rejected: irregular file` | the entry is not a regular file and was not read |
| `rejected: invalid name` | the path is not a relative path of file names of at most 512 bytes; `name` is the path quoted as ASCII, at most 256 bytes |
| `rejected: size changed while read` | the plugin read a length other than the size it listed |
| `rejected: reserved name` | the path is `result` or `transcript`, a result's own [artifact](#reference-artifacts) IDs |
| `exceeds the S3 object limit` | an S3 output over 5 GiB |
| `not streamed by the plugin` | the plugin did not stream an S3 output it found |
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
  "receiver": "nomad-1", "tenant": "alice", "host": "laptop", "pool": "build",
  "run": "run-0f3c...", "run_nonce": "ZrHrUw-3SKTyOPCMstQuRA",
  "placement": {"agent": "coder", "selector": []},
  "task_hash": "9d2a...",
  "message_id": "msg-77c0..."
}
```

A receiver writes the echo in the transaction that holds the run's
`claimed` or `rejected` status, or its `terminal` status when the receiver
ends at the [delivery budget](#delivery-budget) a run no ready plugin profile
satisfies or a run it already failed to start. The echo's fields copy the
submission's, so the echo of a task rejected with `run ID does not match its
signer` carries the `host` and `run_nonce` that do not derive its `run`. A
host that has no record of the run takes the run's pool and submission hash
from its first valid echo whose `tenant`, `host` and `run_nonce` derive its
`run` ([Submission](#submission)), ignores every other echo, and ignores
records of the run from any other pool. A status or chunk binds to the run
when its `task_hash` equals the hash of the run's submission.

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

- `offers` lists the receiver's ready plugin profiles in routing order, each as
  `profile`, `agent` and, optionally, `inline_limit`, the bytes of `raw`
  and `data` parts the profile delivers, absent when it declares no limit
  and never negative. It is empty when no profile is ready. Readers take a
  profile's agent and labels from the receiver's configuration record on
  `wherry.operator`. Each `profile` appears at most once. A host takes the
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

## Custodian snapshot

A custodian snapshot names the receiver holding a pool's private key and
that key's public half, on the pool's compacted
`wherry.pool.<pool>.custodian` topic under the Kafka key `custodian`.

```json
{
  "v": 1,
  "pool": "build",
  "custodian": "nomad-1",
  "reason": "rotation",
  "current": {"key_id": "Qh3T...", "key": {"kty": "EC", "crv": "P-256", "x": "...", "y": "..."}},
  "rotated_for": 4182
}
```

- Readers decode a snapshot strictly: an unknown or duplicate member
  refuses it.
- `custodian` is a receiver ID, and the snapshot's signer.
- `reason` is `bootstrap`, `rotation`, `takeover` or `contested`.
- `current.key` is a P-256 public JWK, and `current.key_id` its RFC 7638
  SHA-256 thumbprint, unpadded base64url.
- `rotated_for` is the `wherry.operator` offset of the latest `rotate-key`
  record for the pool that a key this snapshot or an earlier one published
  was generated after; it is absent before the first. Every later snapshot
  carries it on, and the custodian rotates on a `rotate-key` record whose
  offset is above it.
- `suspended_for` and `resumed_for` are the `wherry.operator` offsets of the
  latest `suspend` and `resume` records for the pool the custodian
  acknowledged; each is absent before the first. A pass over the open
  requests that reads a record above the offset its snapshot carries
  republishes the snapshot with the record's offset before it decides any
  request, and passes begin only once every attempt wrap of the previous
  pass committed. Every later snapshot carries them on.
- The current snapshot is the latest committed record under the key
  `custodian` on the topic, the record compaction keeps for that key. A
  record under another key is not a snapshot. A host submits to the pool
  only when that record verifies, its writer is the custodian it names, and
  that custodian is a member of the pool without a membership end; the
  custodian's advertisement is not read. Otherwise
  the host refuses the submission, and never takes an earlier record in its
  place. It also refuses when the pool has no snapshot, and when
  `wherry.operator` records the pool suspended, before it resolves any
  credential or uploads any input. A host logs a `takeover` or `contested`
  snapshot.

## Resubmission

A run that ended with the wherry event `key-lost` before its
`task_started_by` is resubmitted by its host when `submit`, `status`,
`wait`, `logs -f` or the MCP server next reads it. The host keeps each run's
submission without `envelope` and `wrap` in its local record; it holds no
credential. Under an exclusive `flock` on the failed run, shared by every
process using the host's state directory, the host:

1. produces the stored replacement when the failed run's record names one
   whose bytes are stored and not marked produced, and builds nothing new;
2. otherwise records a new replacement run ID first, then resolves the
   credentials again and builds and signs the replacement: the same message
   content with the new run as `taskId` and `contextId` and a fresh
   `messageId`, the same placement, credentials, outputs, git input, S3
   inputs and bounds, `replaces` the failed run and `s3_input_run` the first
   run of the chain;
3. stores the signed bytes, produces them, and marks them produced.

A crash before the bytes are stored resolves the credentials again on the
next pass. A replacement's S3 outputs use its own prefix. A host keeps the
first run's prefix until the last replacement has ended; a run that ended
`key-lost` before its `task_started_by` has not ended. `wait` and the MCP
server follow a run's replacements, refuse a chain that names a run twice,
and report every run they follow.

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
  "class": "dispatch",
  "task_hash": "9d2a..."
}
```

`task_hash` is the hash of the task record the receiver could not start, as
in a status.

| `class` | Meaning |
| --- | --- |
| `dispatch` | the plugin created no job for the run |
| `platform` | the platform ended the job before its agent task started, for example when the image cannot be pulled |
| `input-too-large` | the plugin profile cannot deliver one of the run's inputs; the receiver then ends the run `TASK_STATE_FAILED` naming the input, and releases nothing |

The set of classes is closed and stable in v1. A record of another class is
invalid, and readers refuse it. A plugin's class for a job that never
started is not a start failure class: the receiver records `platform` for
every such class, including one this specification does not name.

A `platform` start failure can follow the run's `started` status, when the
job ended before its agent task started. The platform's reason for it goes to
the receiver's log alone.

`profile` is the profile the receiver routed the task to.

A receiver delivered a task for which it holds its own start failure, with
the task's `task_hash`, releases it without starting it, and fetches no
task from the pool, in any of its slots, for the pool's lock duration, so
the pool can deliver the task to another member. The release uses a
delivery; at the [delivery budget](#delivery-budget) the receiver ends the
run instead.

### Delivery budget

From an attempt of the pool's `delivery_limit` minus 1, a receiver that
would release a task to the pool ends the run `TASK_STATE_FAILED` with the
message text `delivery budget exhausted`. The releases this covers are
start failures, a dispatch whose outcome the receiver cannot resolve, a
failed read of the pool's events or of the host's topic once the run is
claimed, and a failure to find, stop or remove the run's job.

When the attempt's own start failure spends the budget, the text continues
`after <attempt> attempts; last: <cause>`. The cause is
`job never started (<class>): <reason>` with the platform's class and
reason, `job never started`, or `profile <profile> created no job`. It
carries the marker of each credential value delivered to a run the
receiver holds in place of the value, has no URLs, and is one line of at
most 512 bytes.

When only stopping the job fails at the budget, the run ends from the
agent's exit status without its outputs or outcome file, and the plugin's
sweep removes the job.

These releases are exempt:

- a receiver that cannot read the pool's events at admission releases the
  task, since it cannot check for a committed terminal;
- a receiver that is shutting down releases the task, since an adopter
  continues the run;
- a denied receiver releases the task, since it cannot write;
- a receiver that cannot build the run's transactional producer releases
  the task, since it cannot write.

A receiver whose write of the terminal fails releases the task.

## Progress

A progress record carries the latest transcript offset a receiver has
committed for a run it holds. The receiver writes it to its own progress
topic, `wherry.receiver.<receiver>.progress`, unsigned, keyed by run ID, as
JSON:

```json
{"v": 1, "tenant": "alice", "task_hash": "9d2a...", "attempt": 1, "log_offset": 4096}
```

- Only that receiver writes its progress topic, so Kafka authenticates the
  record, and no other principal can write a key that compaction would let
  supersede it. The live members of its pool read it; hosts do not.
- `log_offset` is the agent output offset the run's transcript is committed
  up to. The record commits in the transaction of the [chunks](#chunk) it
  covers, every 10 seconds while the job runs. An interval with no new
  chunks and an unchanged offset writes nothing.
- The transaction that holds the run's `terminal` status writes a
  tombstone, a null value, under the run's key.

An adopter resumes the job's output from the highest of these offsets:

- the `log_offset` of each `started` status of the run on the pool's
  events with the run's tenant and task hash;
- the `log_offset` of the run's progress record, with the run's tenant and
  task hash, on the progress topic of each receiver that holds or held an
  attempt of the run, as the run's statuses on the pool's events name them.

A receiver whose progress topic the adopter cannot read, such as one whose
membership has ended, contributes nothing, so for an ended holder the
adopter resumes from the `started` offset.

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
  "key": {"kty": "OKP", "crv": "Ed25519", "x": "...", "kid": "...", "alg": "EdDSA", "use": "sig"},
  "previous_key_id": "..."
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
  admitting a task or a cancel, accept
  the current key alone.
- Any other action is rejected.

Readers fold the topic in order. Once the topic holds a key, every record
must verify against the current key; a record that breaks a rule is
ignored.

## Open request

A receiver that claims a run asks the pool's key custodian, named by the
[custodian snapshot](#custodian-snapshot), for the run's content key. It makes
a P-256 key pair for the attempt in memory and commits an open request to the
pool's events in a transaction of its own, after the run's `claimed` status.

```json
{
  "v": 1,
  "run": "run-0f3c...", "attempt": 2, "receiver": "nomad-1",
  "task_hash": "9d2a...",
  "key": {"kty": "EC", "crv": "P-256", "x": "...", "y": "..."}
}
```

- Readers decode an open request, [attempt wrap](#attempt-wrap),
  [re-key wrap](#re-key-wrap), [open refusal](#open-refusal) and
  [claim decision](#claim-decision) strictly: an unknown or duplicate member
  refuses the record. Each verifies under the current key of a live member of
  the pool of its topic, or a key that member retired after the broker appended
  the record, and names that member as its writer. An attempt wrap, open
  refusal or claim decision also verifies under a key its writer held when
  its membership ended, for a record the broker appended before the
  `wherry.operator` record that ended it.
- `key` is the attempt's P-256 public JWK of exactly `kty`, `crv`, `x` and
  `y`. `task_hash` is the hash of the run's task
  ([Admission](#admission)).
- On a redelivery the receiver first asks its profiles' Lookup for the run's
  job. Absent is a first start. A job that never started is purged and
  recorded as a `platform` [start failure](#start-failure), or the run ends
  past its `task_started_by` once that has passed. Running or ended is an
  adoption: the receiver commits an `adopting` status for its claim, then
  requests. A Lookup that does not answer is asked again while the receiver
  holds the claim, with no open request, until it answers or `task_ended_by`
  passes; the run then ends `TASK_STATE_FAILED` with
  `not ended by task_ended_by (<RFC 3339 time>)`, its job and material left
  to the plugin's cleanup.
- The receiver holds the claim until an attempt wrap or a refusal answers its
  request, a cancel of the run arrives, or the earlier of the request's append
  time plus 5 minutes and 120 seconds and the open bound passes. The open
  bound is `task_started_by` for a first start and `task_ended_by` for an
  adoption. With no answer it commits `released` and returns the task to the
  pool, so the delivery budget bounds the cycle; at the open bound the run
  ends past its bound.
- An adopter publishes nothing of the job until it opens. It holds the job's
  lines, at most 4 MiB, dropping the oldest, and logs how many it dropped and
  the offset its transcript resumes at; the first chunk's `seq` shows the gap.
- The private key lives only in the receiver's memory and is gone once the
  wait ends. A redelivered or adopted attempt makes a new key and a new
  request; a wrap answering an earlier request does not open for it.
- A receiver giving back a claim it holds commits `released` before it
  returns the task to the pool. When `released` does not commit, it leaves
  the task unacknowledged, and the share group returns it when its
  acquisition lock expires.

### The claim check

The custodian reads the pool's events to their last stable offset, and
answers each open request that no attempt wrap or refusal answers. It serves a
request only when:

1. it verifies as the live member of the pool it names; otherwise it refuses
   `not the current claimant`;
2. the run has no terminal status and no cancel (otherwise `terminal`), its
   `task_hash` is the run's task, and the submission at the `task_offset` of
   the requester's `claimed` status, read with one fetch, is under the run's
   Kafka key, passes the [signed-record checks](#signed-records), names the
   run under a run ID its signer derives, has the request's `task_hash`, and
   carries an `envelope` with bounds that hold at its append time as
   [Admission](#admission) states; otherwise `not the current claimant`. It
   checks rules 1 and 3 before this one, and reads the task topic for no
   other request;
3. the run's latest ownership status, `claimed` or `released`, is the
   requester's `claimed` for the request's `attempt`; otherwise
   `not the current claimant`;
4. now is within the open bound, which the custodian takes from the events:
   `task_ended_by` when the run has a `started` status or the claim's
   `adopting` status, `task_started_by` otherwise; otherwise
   `past its bound`;
5. the claim has an `adopting` status, which its receiver writes once
   `Lookup` finds the job, or the pool is not suspended in the read of
   `wherry.operator` the custodian makes at the start of its pass; otherwise
   `pool suspended`. A failed read leaves the pass's requests unanswered;
6. when the claim changed receivers, it has no previous holder, or the
   previous holder has no live membership or its latest advertisement in the
   pool appended at or before the claim is more than 120 seconds old when the
   custodian decides; otherwise `two live claimants`. The previous holder is
   the receiver of the latest earlier `claimed` for the task whose claim
   decision served it and that the same receiver has not followed with its
   own `released`. A claim refused or never decided holds nothing, and a
   `released` ends only its own receiver's claim.

The custodian decides rule 6 once for each claim and commits that decision as
a [claim decision](#claim-decision) in the transaction of its answer; a later
request for the same claim takes that decision. It then recovers the content
key from the submit wrap or a [re-key wrap](#re-key-wrap) to a key it holds,
opens the envelope and checks it against the task as [Submission](#submission)
states; a check that fails refuses `envelope invalid`, so a spliced envelope
never releases its content key. With no key it waits for a holder's re-key
wrap until the earlier of the request's append time plus 5 minutes and the
open bound, then refuses `key unavailable`. Otherwise it commits an attempt
wrap. Each answer is one transaction through the transactional ID
`p.<pool>.custodian`, so a fenced custodian answers nothing; a request whose
answer's commit has an unknown outcome is answered again on a later pass only
when the answer is not on the pool's events.

## Attempt wrap

An attempt wrap answers one open request with the run's content key.

```json
{
  "v": 1, "custodian": "nomad-2",
  "task_hash": "9d2a...", "request": "51c0...", "receiver": "nomad-1", "attempt": 2,
  "jwe": "eyJhbGciOiJFQ0RILUVTK0EyNTZLVyIs..."
}
```

- `custodian` is the writer. `request` is the SHA-256, in lowercase hex, of
  the open request's record value, and `receiver` and `attempt` are the
  request's. `jwe` is a wrap of the content key to the request's `key`, with
  the protected header a submit wrap has ([Submission](#submission)).
- The claimant takes an attempt wrap only when its writer is the current
  snapshot's custodian and it names the claimant's own request, receiver,
  attempt and task hash. It unwraps the content key, opens the envelope and
  checks it against the task again. A wrap whose key does not decrypt the
  envelope is ignored, logged and counted in `wherry_ignored_wraps_total`. A
  check that fails ends the run `TASK_STATE_FAILED` with the check's reason
  before anything is dispatched.
- The claimant checks for a cancel, its shutdown and the open bound before it
  takes a wrap, so a run stopped while its wrap arrives is never dispatched.
  It passes the envelope's credentials and S3 URLs to the plugin in Dispatch,
  and redacts with the envelope's redaction key ([Redaction](#redaction)).

## Re-key wrap

A receiver holding a pool key that is not current, such as a former
custodian, answers an open request that passes rules 1 to 3 of the
[claim check](#the-claim-check), for a task it can open, from the task's
submit wrap or a re-key wrap to a key it holds, with the content key wrapped
straight to the current snapshot's key, unless a re-key wrap of the task to
that key is on the pool's events. There are no chains. It writes the re-key
wrap under its admission transactional ID.

```json
{"v": 1, "holder": "nomad-3", "task_hash": "9d2a...", "recipient_kid": "Qh3T...", "jwe": "..."}
```

- `holder` is the writer. `recipient_kid` is the current key's ID, and `jwe`
  is a wrap of the content key to that key.

## Open refusal

An open refusal answers one open request with a reason.

```json
{"v": 1, "custodian": "nomad-2", "request": "51c0...", "reason": "two live claimants"}
```

The claimant takes a refusal of its own request from the current snapshot's
custodian and acts on its `reason`:

| `reason` | Claimant's action |
| --- | --- |
| `two live claimants`, `not the current claimant`, `pool suspended` | commits `released`, then returns the task to the pool |
| `terminal` | acknowledges the task without a dispatch |
| `past its bound` | ends the run past its bound: a first start purges its job, if any, and an adopter stops its job; the run fails with `not started by task_started_by (<RFC 3339 time>)` while the receiver's clock is within `task_ended_by`, and with `not ended by task_ended_by (<RFC 3339 time>)` after it |
| `key unavailable` | decides from the job's state, below |
| `envelope invalid` | ends the run `TASK_STATE_FAILED` with `envelope invalid`, without `key-lost`; an adopter stops its job first |

Refused `key unavailable`, the claimant decides from the job's state, never
from the `started` status:

- On the task's first delivery, attempt 1 with no earlier `claimed` status,
  no job can exist: the run ends `TASK_STATE_FAILED` with
  `cannot be opened: the pool's custodian changed and its key was lost` and
  the wherry event `key-lost`.
- Otherwise it asks its profiles' Lookup. An absent or never-started job ends
  the run the same way. A running or ended job is stopped and the run ends
  with the same message, without `key-lost`.
- While Lookup does not answer, it commits a `reconciling` status and asks
  again, holding the claim, until Lookup answers or `task_ended_by` passes,
  when the run ends `TASK_STATE_FAILED` with
  `not ended by task_ended_by (<RFC 3339 time>)`, never `key-lost`. A
  claimant of a run with a `reconciling` status continues Lookup the same
  way, with no open request and no dispatch.
- An adopter refused `key unavailable` requests once more, then stops the job
  and ends the run with the same message, without `key-lost`.

## Claim decision

A claim decision records the custodian's decision on a claim under rule 6 of
the [claim check](#the-claim-check).

```json
{
  "v": 1, "custodian": "nomad-2",
  "claim": "7e1f...", "previous": "nomad-1", "reason": "lease",
  "advertisement": {"offset": 4412, "at_ms": 1789998100000},
  "served": true
}
```

- `custodian` is the writer. `claim` is the SHA-256, in lowercase hex, of the
  record value of the `claimed` status decided, and `previous` the previous
  holder of rule 6, or with none the receiver of the ownership status before
  the claim, absent for no ownership status before it and when the previous
  holder is the claimant itself.
- `reason` is `unchanged` for no ownership status before the claim or a
  previous holder that is the same receiver, `released` for no previous
  holder after one, `lease` for a previous holder whose advertisement at or
  before the claim is more than 120 seconds old or absent, or `live`.
  `served` is false for `live` alone. A receiver whose membership ended has
  no statuses on the pool's events, so a claim after an ended holder is
  decided as if that holder never claimed, `unchanged` or `released`; it is
  `lease` only when the membership ended after the custodian read the events.
- `advertisement` names the previous holder's advertisement the decision took,
  by its offset on `wherry.receivers` and its append time, and is absent when
  it took none.
- A custodian counts the claims it serves to a new claimant in
  `wherry_claimant_changes_total` by `reason`.

## Redaction

Before it signs any transcript chunk, output chunk, result text or status
message of the attempt, the receiver replaces every occurrence of each
credential value its envelope delivered, task credentials and presigned URLs
included, and of each URL's `X-Amz-Signature` value alone, with the value's
marker: `[redacted:`, 16 base64url characters and `]`, 27 bytes. The characters
are the first 96 bits of the HMAC-SHA256, under the run's redaction key, of
`wherry-redaction-v1` followed by the value's name: the credential's name, or
`s3-inputs/<name>` or `s3-outputs/<name>` for a URL and its signature. Every
form of a value takes the same marker, and every attempt of the run the same
markers. No marker maps to a credential name without the run's redaction key. When
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
transcript line whose redacted form is over 512 KiB becomes a
[reference part](#reference-parts) with that form's hash and size; a reply whose redacted
form is over 64 KiB travels as its preview and [reply chunks](#the-reply), both
cut from the redacted form; and a credential name whose
redacted form is over 64 KiB is replaced by `[redacted text over 65536 bytes]`. A result whose agent fields are 16 KiB of
8-byte values, each replaced by its 27-byte marker, still fits a default
producer batch. A value shorter than 8 bytes once trimmed is not redacted; the
receiver logs `credential <name> too short to redact` once for the attempt. A
value in any other encoding, such as base64, is not replaced. An adopted attempt
redacts the values of its own open of the envelope. Each form is replaced once, longest
first, and a marker can join the bytes beside it into a value that was not there
before: when a value starts with a marker's tail, such as `]`, and the rest of
it follows a replaced value, the published text holds that value whole.

## Operator records

`wherry.operator` holds unsigned JSON records that only `wherry.operator`
writes, each with `v`, `kind` and `at_ms` (Unix milliseconds).

| `kind` | Members | Effect |
| --- | --- | --- |
| `pool` | `pool`, `terms` | records or replaces the pool's terms |
| `receiver` | `pool`, `receiver`, `config` | records or replaces the receiver's configuration |
| `host` | `tenant`, `host` | records a host of the tenant |
| `entitlement` | `tenant`, `pool` | entitles the tenant to the pool |
| `end` | `receiver`, or `tenant` and `host` | records a membership end |
| `revoking` | `receiver`, or `tenant` and `host` | records a revocation in progress: the principal is granted no ACL and stays a member until its `end` |

`terms` has the members of the [pool terms](configuration.md#pool-terms) file
and `record_limit`, the tasks topic's record limit that `pool create` reads
from the broker, with `start_by_limit_ms` and `lock_duration_ms` in milliseconds for its
`start_by_limit` and `lock_duration`. `config` holds `receiver` and `pool`,
which equal the record's, the receiver's `capacity`, and its `profiles` in
routing order, the plugins in the order of their names. Each plugin profile is the
[profile entry](configuration.md#receiver-configuration) as `name`, `plugin`
naming its plugin, `agent`, `labels` when set, and the merged
`config`, which keeps a `credential` name and never a credential value. `config`
also holds `force`, `true` when `receiver update --force` wrote the record. An `end` names
exactly one of `receiver`, or `tenant` and `host`. A `revoking` names exactly one of `receiver`, or
`tenant` and `host`, and no other member.
Readers fold the records in order. A reader loads no state when:

- a `receiver` record moves a receiver to another pool or names an unknown
  pool;
- a `receiver` or `host` record follows that member's `end`;
- an `entitlement` names an unknown pool;
- a record fails to decode;
- two of the Kafka names that pools, receivers and hosts derive (topics,
  share groups, principals and transactional prefixes) collide.

Readers ignore unknown members on a record and reject duplicate names. A
record whose `v` is higher than the reader's release knows was written by a
later release; when every undecodable record is such a
record, the reader reports that a later wherry release wrote them and that
this one needs an upgrade; otherwise it reports that this release cannot read
`wherry.operator` and the deployment needs a reset.

## Admission

A receiver that claims a submission:

1. acknowledges it and writes nothing when the pool's events already hold a
   status in a terminal state, `TASK_STATE_REJECTED` included, with its task
   hash;
2. drops it when it fails the [signed-record checks](#signed-records), or
   when its Kafka key is not its `run`;
3. rejects it with `run ID does not match its signer` when its `run_nonce`
   is missing or not canonical, or its `tenant`, `host` and `run_nonce` do
   not derive its `run` ([Submission](#submission)). The rejection names the
   signer's tenant and task hash, so it binds to no other host's run;
4. rejects it with `duplicate submission` when the run's task is another
   hash. The run's task is the `task_hash` of the run's first status on the
   pool's events, in log order, that is not `rejected`, among the statuses of
   live pool members: the first wins. Revoking the member that wrote that
   status makes the next one first;
5. acknowledges it and writes nothing when the latest `claimed` status with
   its task hash names another `task_offset`: it is a copy, byte for byte, of
   the claimed submission. A redelivery of the claimed submission itself goes
   on;
6. rejects it with `agent is outside the pool terms` when its agent is
   outside the pool's terms; with `model not in the pool's terms` when the agent has `models` and the
   placement names no model or one outside the current `models`, or the
   agent has no `models` and the placement names one;
7. rejects a run no pool member has claimed with
   `not started by task_started_by (<RFC 3339 time>)` or
   `not ended by task_ended_by (<RFC 3339 time>)` when that bound has
   passed;
8. rejects a run no pool member has claimed when it fails a check judged at
   its append time, which its delivery carries, naming the first that fails:
   `the task carries no envelope` when it has none;
   `task_started_by is before its append time`;
   `task_started_by is past the pool's start_by_limit` when its
   `task_started_by` falls more than `start_by_limit_ms` after its append
   time; `task_ended_by is before task_started_by`; and
   `task_ended_by is past task_started_by plus the 6h run cap` when its
   `task_ended_by` is more than six hours after its `task_started_by`. Its
   `rejected` status names no run's task (step 4), so a later submission of
   the run that passes is admitted;
9. releases it when no ready plugin profile satisfies the placement, unless
   a member has claimed the run and its `task_ended_by` has passed, or its
   `task_started_by` has passed with no `started` event;
10. rejects a run no pool member has claimed with
    `task_started_by is past the pool's start_by_limit` when its submission
    reached the pool's task topic more than the pool's `start_by_limit_ms`
    ago;
11. rejects it when the host of the run's [git input](#git-input) is not
    one of the pool's current VCS hosts, and with `pool <pool> has no
    storage for S3 inputs and outputs` when the run has S3 inputs or
    outputs and the pool's terms have no `storage`;
12. rejects it with `unsupported message part` when a `url` part appears
    in the message;
13. writes the echo and the `claimed` status, its `task_offset` the
    submission's offset.

A run a cancel covers skips steps 7, 11 and 12, and ends `TASK_STATE_CANCELED`
after its `claimed` status. A redelivered run that a member has claimed
goes on to adoption past its bounds. When its `task_ended_by` has passed,
the receiver stops the job it finds and ends the run `TASK_STATE_FAILED`
with `not ended by task_ended_by (<RFC 3339 time>)`.

A claimed run whose job has not started by `task_started_by`, while it
waits for its open or for the platform to place its job, ends
`TASK_STATE_FAILED` with
`not started by task_started_by (<RFC 3339 time>)`, and the receiver
purges the job when one exists. A redelivered run with no `started` event
past `task_started_by` ends the same way: the receiver stops a job its
lookup reports running or ended, and purges one that never started. A job
that has started runs on past `task_started_by` until `task_ended_by`. A
redelivered run whose lookup does not answer is held and asked again, with or
without a `started` event, until `task_ended_by` passes, when it ends
`TASK_STATE_FAILED` with `not ended by task_ended_by (<RFC 3339 time>)` and its
job and material in place ([Open request](#open-request)).

A run whose commit has an unknown outcome gives its task back
unacknowledged. The task's next holder acknowledges it without running when
the commit's terminal status is on the pool's events, and runs it again
otherwise, so an ambiguous commit costs one redelivery cycle of latency. The
job of a run whose terminal committed is not released by its holder; it is left
to the plugin's sweep.

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
redirect, so a harness rule that allows `git` commands allows them all,
and `git` can run any command.
The commands read the credential from its file, so its value is on no
command line and in no environment variable or prompt. When the run
expects the output `changes.patch`, the lines give the
`git format-patch --output=<file> refs/wherry/start..HEAD` command that
writes it; for `changes.bundle`, the `git bundle create` command for the
same range. They ask the agent to push only when the request says so, and
then only to the branch `wherry/<run>`.

A plugin gives its agent an environment in which `git` reads no system,
global or environment configuration, uses no template, never prompts and
traces nothing (`pluginsdk.AgentEnvironment`), and removes from the
environment it configures the variables no value switches off
(`pluginsdk.AgentUnset`), so the platform's configuration does not rewrite,
redirect, record or trace the clone. The operator keeps the platform's own
environment and the image's free of them.

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
  with an `env` target is in that environment variable instead. The plugin
  delivers credential values only at their targets, and never into the
  input directory. Only the agent's run can read its credentials, and the
  platform's boundary enforces it, as each plugin's README states.
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
`(not supplied)`. The lines name S3 inputs by path and carry no URL. The plugin reports the
directories' paths as its agent sees them; a directory it does not report
appears as its variable. The lines hold no credential value.

`.wherry/` beneath the outputs directory is reserved for the outcome file. No
output name leads into it, and the receiver collects nothing in it as an
output. An MCP host writes a reply over 64 KiB among its outputs as
`reply.txt`, so a submission expecting an output of that name is invalid.
A result's artifacts take the IDs `result` and `transcript`, and A2A
requires a task's artifact IDs unique, so a submission expecting an output
of either name is invalid, and the receiver lists an output of either name
as not returned, `rejected: reserved name`.

When the job ends, the receiver stops it, then asks the plugin to collect the
outputs, whatever the job's exit. The plugin lists every entry beneath the outputs
directory without following links, and refuses the whole tree, reading nothing,
when it holds more than 256 entries. It reads only regular files: one at most
the pool's `output_limit`, 8 MiB unless its terms set another, and together at
most that, which the receiver checks again. Every other entry is listed with its
type and left unread. The receiver gives up on a collection that takes over 2
minutes. A plugin that cannot collect, or a job that could not be stopped,
leaves the outputs `not collected`; the receiver then asks the plugin for the
outcome file alone, within another 2 minutes. The outcome file counts against
neither output limit, and is invalid over the pool's `output_limit`.

The receiver checks every name, redacts every file, and publishes each
returned file as [output chunks](#output-chunk) and in the result's
[metadata](#metadata). A required output listed in `not_returned` ends
the run `TASK_STATE_FAILED` with
`required output <name> not returned: <reason>`, behind the agent's
`agent exited <code>` or `agent ended by <signal>` and `; ` when it left no
valid outcome file and ended by a signal or a non-zero exit, as in
`agent exited 137 (OOMKilled); required output report.md not returned: missing`;
an optional one leaves the state as the agent's outcome file set it.

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
S3 output at `outputs/<name>`; a resubmission's S3 inputs stay under its
`s3_input_run`. The host's own credential,
scoped by the operator to `wherry/<tenant>/`, uploads, downloads and
deletes them, and never leaves the host. Receivers and plugins get only
the presigned URLs in the task's envelope, which the custodian and the
claimant check against the pool's storage before use.

- **In.** The host uploads each S3 input before it publishes the run and lists
  it in `s3_inputs`. The plugin fetches it into `WHERRY_INPUT_DIR` from its
  presigned GET before the agent starts, and verifies its size and SHA-256. The
  plugin never exposes the GET URL to the job, and an adopter, whose job has
  fetched its inputs, does not take the GET URLs from the envelope or redact
  them. A fetch that fails or does not verify
  ends the job before the agent starts, and the receiver ends the run
  `TASK_STATE_FAILED` with `an S3 input was not fetched, or did not match its
  size and SHA-256`, without returning the task to the pool. S3 inputs count
  against neither `record_limit` nor `inline_limit`.
- **Out.** Once the job ends and is stopped, the plugin
  streams each S3 output it finds as it collects, and the receiver sends it
  to its presigned PUT as it reads it, with the size the plugin listed as
  its Content-Length, hashing the bytes it sends. It follows no redirect.
  An S3 output counts against neither `output_limit` nor the run's outputs
  together, but is at most 5 GiB, and among the 256 outputs a submission
  expects. Collection waits until the run's `task_ended_by`, when the PUT URLs
  expire, if that is later than 2 minutes. The receiver does not redact an
  S3 output, which goes to the host's own bucket and never to the
  transcript topic. An attempt that adopted an earlier attempt's job uploads
  to the PUT URLs of the task's envelope.
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

## Binding

wherry is a partial custom A2A protocol binding over Kafka, as A2A section 12
allows. Its binding URI, which an `AgentInterface`'s `protocolBinding` would
name, is:

```
https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md#binding
```

A host is the A2A client and the pool's receivers together are the A2A
server. There is no endpoint: each operation is a record on a Kafka topic, and
its response is records on the tenant's topics. The A2A objects keep their
proto3 JSON form ([Encoding](#encoding)) inside the signed records.

| A2A operation | wherry |
| --- | --- |
| `SendMessage` | a [submission](#submission) on the pool's tasks topic; the run ID is the task ID and the context ID. The run's records answer it |
| `CancelTask` | a [cancel](#cancel) on the pool's events topic |
| `SubscribeToTask` | reading the tenant feed, `wherry.tenant.<tenant>`, for the run's statuses and result, and the tenant's transcript topic, `wherry.tenant.<tenant>.transcript`, for its chunks and output chunks |
| `GetTask` | the run's latest [result](#result), or, before it has one, its latest [status](#status) |
| `SendStreamingMessage` | a submission, then `SubscribeToTask` |
| `ListTasks` | not provided |
| push notification configuration (create, get, list, delete) | not provided |
| `GetExtendedAgentCard` | not provided |

A run is one task with one message. A run never asks for input
(`TASK_STATE_INPUT_REQUIRED`) and is never resumed, so a host sends no
second message to a task.

### Errors

A2A errors have no wire form here. A receiver answers a submission it can
attribute to a host and run with a `rejected` [status](#status) in
`TASK_STATE_REJECTED`, and drops one it cannot.

| Refusal | Outcome | Message text |
| --- | --- | --- |
| the `run_nonce` is missing or not canonical, or the `tenant`, `host` and `run_nonce` do not derive the `run` | rejected | `run ID does not match its signer` |
| the agent is not in the pool's terms | rejected | `agent is outside the pool terms` |
| the model is not in the agent's terms | rejected | `model not in the pool's terms` |
| a later submission of a run ID | rejected | `duplicate submission` |
| the submission is older than the pool's `start_by_limit_ms`, or its `task_started_by` is later than `start_by_limit_ms` after it | rejected | `task_started_by is past the pool's start_by_limit` |
| `task_ended_by` is more than six hours after `task_started_by` | rejected | `task_ended_by is past task_started_by plus the 6h run cap` |
| the submission carries no envelope | rejected | `the task carries no envelope` |
| `task_started_by` is before the submission's append time | rejected | `task_started_by is before its append time` |
| `task_ended_by` is before `task_started_by` | rejected | `task_ended_by is before task_started_by` |
| `task_started_by` or `task_ended_by` has passed | rejected | `not started by task_started_by (<RFC 3339 time>)`, `not ended by task_ended_by (<RFC 3339 time>)` |
| a required credential of the agent is unsupplied | rejected | `the host does not supply a required credential of the agent` |
| a task credential takes an agent credential's target | rejected | `a task credential takes the target of one of the agent's credentials` |
| the git remote's host is not a VCS host of the pool | rejected | `git remote host <host> is not in pool <pool>'s vcs hosts` |
| S3 inputs or outputs in a pool without storage | rejected | `pool <pool> has no storage for S3 inputs and outputs` |
| a `url` part in the message | rejected | `unsupported message part` |
| the record fails a [signed-record check](#signed-records): its JWS, its signer, its payload, its version, its owner or its checks against `wherry.operator` | dropped | none |
| the record's Kafka key is not its `run` | dropped | none |
| the run already has a terminal status with the submission's task hash | acknowledged, nothing written | none |

A receiver drops a record it cannot verify because a status names a tenant,
run and task hash taken from the record: writing one for an unverified record
would let any principal that can write the tasks topic place statuses on a
tenant's feed. A record that fails to decode, or decodes to an invalid
payload, names no run a status could bind to. A dropped submission's host
learns nothing from the pool and sees its run stay submitted until its
`task_started_by` passes. A submission whose run already ended is a redelivery, and
the run's terminal status already answers it.

A run that ends after admission ends `TASK_STATE_FAILED` or
`TASK_STATE_CANCELED`, with the texts [Status](#status) lists. A cancel that
fails the signed-record checks, or names a run whose key it does not carry, is
ignored.

### Ordering

- Kafka orders records within a topic partition, and every record of a run
  is keyed by its run ID, so a run's records on one topic arrive in the order
  they were written. Records on different topics have no order between them.
- A run's statuses and results carry `attempt`, the share-group delivery
  count. Readers order them by `attempt`, and an earlier attempt's records do
  not change the outcome of a later one.
- A run's transcript orders by `seq`, the line's offset in the agent's
  output. An adopted attempt can repeat a `seq`; a reader
  takes the latest attempt's lines ([Chunk](#chunk)).
- The end transaction commits the run's last chunks, its output chunks and
  reply chunks, its result and its `terminal` status together, across the
  tenant feed, the transcript topic and the pool's events. A reader sees each
  topic's commit separately, so a host that reads the `terminal` status reads
  the transcript topic to its high watermark before it reports the
  transcript or the outputs complete ([Status](#status)). The last chunk has
  `lastChunk` true.

## Agent Card

wherry serves no Agent Card. A card for a wherry pool would declare the
extension in `capabilities.extensions` as this `AgentExtension`:

```json
{
  "uri": "https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md",
  "description": "wherry records: signed submissions, statuses, transcript chunks, output chunks and results on Kafka, with run placement, credentials in an envelope under the pool's key, and inputs and outputs",
  "required": true,
  "params": {"binding": "https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md#binding"}
}
```

`required` is true: a client that does not follow this extension cannot form
a submission a receiver admits. `params` names the binding URI. Activation
is implicit: every `Message` and `Artifact` wherry writes lists the
extension URI in its `extensions`, and readers reject one that does not, so
no request activates the extension and no response confirms it.
