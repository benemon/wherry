# wherry A2A extension, version 1

URI: `https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md`

Five of wherry's twelve record types are A2A-shaped. A `submission`,
`status`, `artifact_update` or `task` record is a JSON object with two
members: an A2A v1.0 object under its A2A field name, and a `wherry` object
holding this extension's fields. A `cancel` record has one member, its A2A
object, and no `wherry` member. Every `Message` and `Artifact` that wherry
produces lists the URI above in its `extensions`.

The other seven record types are wherry's own control-plane and trust-plane
records: `capability`, `enrol_request`, `enrol_approval`, `renew_request`,
`revocation`, `secret_version` and `sealed_secret`. They carry their fields
at the top level, with no A2A object and no `wherry` member.

## Records

A `submission` record (topic `tasks.queue`) carries the A2A `Message` for one
turn of a task, under `message`. Its `wherry` fields are `run_id` (string,
the task id), `turn` (integer, from 1), `attempt` (integer, always 1; the
delivery attempt appears in `status`, `artifact_update` and `task`), `pool`
(string, the receiver pool that may claim it), `selector` (array of `{key,
operator, values}` requirements on receiver labels), `deadline_ms` and
`queue_deadline_ms` (integers, Unix milliseconds by which the run must end
and must be claimed), `reply_topic` (string, where the final task is
published) and `lineage`.

The `lineage` object holds `root_run_id` (string), `depth` (integer),
`ancestors` (array of run ids from the root), `parent_hash` (string, hex
SHA-256 of the parent's envelope, empty for a root) and `chain` (array of the
ancestors' envelopes, base64url). Child lineage is schema-only. The wherry
client publishes only root submissions, with `depth` 0, empty `ancestors`,
`chain` and `parent_hash`, and `root_run_id` equal to `run_id`. Receivers
reject any other submission.

A `status` record (topic `tasks.events`) carries a `TaskStatusUpdateEvent`
under `statusUpdate`. Its `wherry` fields are `turn` and `attempt` (integers,
the submission turn and the receiver's delivery attempt for it), `task_hash`
(string, hex SHA-256 of that turn's submission envelope), `receiver_id`
(string, the receiver that published it), `kind` (one of `claimed`,
`started`, `heartbeat`, `interrupted`, `rejected` and `terminal`) and
`log_offset` (integer, optional, the agent log offset up to which the
transcript is published).

An interrupted status is a `status` record with `kind` `interrupted` and a
state of `TASK_STATE_AUTH_REQUIRED` or `TASK_STATE_INPUT_REQUIRED`. It ends
the turn without ending the task.

An `artifact_update` record (topic `transcripts`) carries a
`TaskArtifactUpdateEvent` under `artifactUpdate`. It is one chunk of the
artifact `transcript`: one agent output line as a text part with media type
`application/x-ndjson`. An empty line or one that is not UTF-8 is a `raw`
part instead. A line over 512 KiB is a `url` part with the value
`unstored:oversize`, whose `metadata` holds the line's hex `sha256` and its
`size` in bytes. The line itself is not stored. Its `wherry` fields are
`turn` and `attempt` (integers, as in `status`) and `seq` (integer, the
line's log offset, which orders the chunks).

A `task` record (topic named by `reply_topic`) carries the final A2A `Task`
under `task`. Its `history` holds every submitted message in turn order and
its `artifacts` hold the result. Its `wherry` fields are `turn` and
`attempt` (integers, as in `status`) and `task_hash` (string, the final
turn's submission hash).

A `cancel` record (topic `tasks.control`, compacted, keyed by run id)
carries an A2A `CancelTaskRequest` under `cancelRequest`. Its `id` is the
run id of the task to cancel, and its `metadata` is optional. It has no
`tenant`. Only the user signs a cancel. Readers ignore a cancel signed by
any other key, and one whose Kafka key differs from its `id`.

## Turns and continuations

A task runs in turns. Each turn is a separate `submission` and runs as a new
job. Readers order status, artifact update and task records by
`(turn, attempt)`.

Within one turn, the transcript's first chunk has `append` false and the rest
true. The last chunk before the terminal status has `lastChunk` true. A
continuation turn starts a new transcript, so its first chunk has `append`
false again.

A receiver interrupts a turn with `TASK_STATE_AUTH_REQUIRED` when the adapter
that matched the task needs a credential the receiver does not hold. The
status message text is `credential <name> required`. The status `metadata`
holds `credential` (string, the credential's name). Clients match a waiting
task to a credential by this member and ignore the message text. The
interrupt publishes this status alone: no transcript chunk and no `task`
record.

A continuation is a user-signed submission with `turn` greater than 1. It
uses the run id as `message.taskId` and has a new `messageId`. It repeats the
`contextId`, `pool`, `reply_topic`, `selector` and `lineage` of the
run's earlier turns. A receiver admits it only after an interrupted status
from a receiver in the same pool for the immediately preceding turn, and
rejects a continuation that differs from the earlier turns in any of those
fields.

## Git context

A submission can name a git repository for the agent to work on. The
repository is a `url` part with media type `application/vnd.wherry.git`:

- `url` (string): the clone URL. Its scheme is `https` or `ssh` and it has
  a host. An `ssh` URL may carry the user name `git` as its only user
  information; an `https` URL carries none. Neither has a query or
  fragment, so credentials never travel in it.
- `metadata.ref` (string, required): a branch, tag or full commit SHA.
- `metadata.path` (string, optional): the checkout directory, a relative
  path. It defaults to the last segment of the URL path without a `.git`
  suffix.

A ref, and each segment of a path, starts with a letter, digit or
underscore and holds only letters, digits, `.`, `_` and `-`. A ref can also
hold `/`. It cannot hold `..` or `//`, or end in `/`, `.` or `.lock`. A
submission carries at most one git part. Readers reject a submission that
breaks these rules.

A receiver admits a git part only when the matched adapter delivers git
context. Any other part that is not text, or a second git part in the
run's turns, ends the turn as `TASK_STATE_REJECTED` with the message text
`unsupported message part`. A git part applies to its own turn and to
every later turn of the run.

Before the agent starts, the adapter fetches the ref at depth 1, checks it
out, and adds one line to the end of the prompt:

```
The git repository <url> at <ref> is checked out in <dir> at commit <sha>.
```

The turn's `started` status holds `git_commit` (string, the hex commit SHA
checked out) in its `metadata`. The Nomad adapter checks out the
repository in a prestart task of the run's job, into
`<NOMAD_ALLOC_DIR>/<path>`, and starts the agent task only when the
checkout succeeds.

The platform the adapter runs on supplies host trust and repository
access; wherry carries no git credential. For an `https` URL the checkout
uses the platform's system git configuration and CA bundle. For an `ssh`
URL it uses the platform's SSH configuration with
`StrictHostKeyChecking=yes`, so the host key must already be in the system
known hosts file, `/etc/ssh/ssh_known_hosts`. Access to a private
repository comes from a deploy key or a credential helper that the
platform configures in the image or on the host. The repositories a
runtime can reach therefore vary by platform. An offer can advertise them
with an operator label, such as `git-access`, and a selector can require
it.

When the checkout fails, the turn ends as `TASK_STATE_FAILED` with the
message text `git context unavailable`. This includes an unknown or
changed host key and a repository the platform cannot authenticate to.
Receivers do not publish git's error output.

## Cancellation

A cancel applies to every turn of a run, including turns submitted after
it. Receivers read the control topic at a fixed interval, when they claim a
submission, and at each heartbeat of a running turn, and act on the cancels
they hold:

- A receiver that claims a submission for a cancelled run starts no job. It
  publishes a `task` record and a `terminal` status, both with
  `TASK_STATE_CANCELED` and the message text `canceled by user`. If an
  earlier delivery attempt left the turn's job running, the receiver stops
  that job first.
- A receiver running a turn for a cancelled run stops its job at the next
  heartbeat and publishes the same `task` record and `terminal` status.

If stopping a job fails, the receiver releases the submission without a
terminal status, and the next delivery tries again.

No receiver holds an interrupted task, so a cancel for one ends it with no
receiver record. Clients treat an interrupted task with a cancel from the
user as `TASK_STATE_CANCELED`. A receiver that later claims a continuation
for it ends that turn as above.

Cancellation is best effort. A turn that ends before a receiver acts on
the cancel keeps the state it ended in.

## Metadata

The `metadata` of the final `Task` in a `task` record describes where the
final turn ran:

- `receiver_id` (string): the receiver that ran the turn.
- `pool` (string): the submission's pool.
- `adapter` (string): the name of the receiver's adapter that ran the turn,
  or that matched it when the turn was cancelled before a job started.
- `offer` (object of string labels): the offer the job was dispatched with.
  It is absent when no job was dispatched, and when the attempt adopted a
  job that an earlier attempt dispatched.
- `job` (string): the platform's id for the job, as the adapter reports it.
  It is absent when no job was dispatched.

The `metadata` of the result artifact (`artifactId` `result`) holds
`agent_result` (object): the agent's final result line, decoded as a JSON
object, with its `result` member removed. That member is already the
artifact's part. The receiver passes the other members through without
interpreting them, so their names and values are the agent's own. It
re-encodes them, so member order and string escaping can differ from the
line in the transcript.

`agent_result` is absent when the result line is not an RFC 7493 object,
when the object without `result` exceeds 16 KiB as encoded, and when the
turn ends without a result line.

## Identifiers

The A2A task id is the run id: `message.taskId` in a submission, `taskId` in
the update events and `id` in the task. The `contextId` is the run id of the
root task, so every task in one delegation tree shares a context.

## Signing and transport

Each Kafka record value is a JSON envelope with members `v` (integer, the
record version), `type`, `signer`, `payload` and `sig`. The `signer` is
`sha256:<hex SHA-256 of the signer's 32-byte Ed25519 public key>`. The
`payload` member holds the record's exact JSON bytes, base64url-encoded
without padding, and `sig` is base64url-encoded the same way. The signature
is Ed25519 over `wherry/v<v>/<type>`, a zero byte, and those payload bytes,
where `<v>` is the envelope's `v` and `<type>` is the record type named
above. A reader decodes `payload` to get the record. Verifiers check the
signature over the bytes as transmitted before parsing them.

Writers emit `submission`, `capability` and `enrol_approval` at version 2 and
every other type at version 1. Readers also accept version 1 of those three
types, the shape before task labels and the receiver selector were removed:

- A version 1 `submission` carries a `labels` object in `wherry`. A reader
  validates it and discards it.
- A version 1 `capability` carries a `selector` array. A reader validates it
  and discards it.
- A version 1 `enrol_approval` carries a `selector` array. With an empty
  selector it is an ordinary approval. With a non-empty selector it still
  holds its generation, so the next approval for that receiver takes a
  higher one, but it enrols nothing and cannot be renewed. A version 1
  renewal of it with the same selector is accepted and keeps the
  generation's chain.

A version 2 record that carries `labels` in a submission's `wherry`, or
`selector` in a capability or approval, is invalid. Hashes of a version 1
record, such as a seal's approval hash or a status's `task_hash`, are over
its version 1 envelope bytes.

## Topic timestamps

The topics `receivers.state`, `tasks.events` and `transcripts` and each
reply topic set `message.timestamp.type` to `LogAppendTime`. The broker
replaces the producer's timestamp on these topics with the time it appended
the record.

## Revoked receivers

Clients and receivers do not trust a revoked receiver for new work: routing,
claims, continuations and the runs waiting for a credential. When a client
reads a run's history with `await` or `events`, it accepts a `status`,
`artifact_update` or `task` record signed by a revoked receiver when all of
these hold:

- The record's timestamp type is `LogAppendTime`.
- The first `revocation` record on `receivers.state` whose
  `floor_generation` covers the receiver's revoked generation has timestamp
  type `LogAppendTime`. Its timestamp is the revocation time.
- The record's timestamp is earlier than the revocation time. A record
  appended in the same millisecond as the revocation is not accepted.
- The record passes the checks that apply to a record from an enrolled
  receiver.

The `wherry` CLI marks such a record with `signer=revoked-before=<time>`,
the revocation time in RFC 3339. Records that a compromised key signed
before the revocation also pass these checks.
