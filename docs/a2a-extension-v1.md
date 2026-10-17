# wherry A2A extension, version 1

URI: `https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md`

Every wherry record is a JSON object with two members: an A2A v1.0 object
under its A2A field name, and a `wherry` object holding this extension's
fields. Every `Message` and `Artifact` that wherry produces lists the URI
above in its `extensions`.

## Records

A `submission` record (topic `tasks.queue`) carries the A2A `Message` that
starts a task, under `message`. Its `wherry` fields are `run_id` (string, the
task id), `attempt` (integer, the delivery attempt, from 1), `pool` (string,
the receiver pool that may claim it), `selector` (array of `{key, operator,
values}` requirements on receiver labels), `labels` (object of string
labels), `deadline_ms` and `queue_deadline_ms` (integers, Unix milliseconds
by which the run must end and must be claimed), `reply_topic` (string, where
the final task is published) and `lineage`. The `lineage` object holds
`root_run_id` (string), `depth` (integer), `ancestors` (array of run ids from
the root), `parent_hash` (string, hex SHA-256 of the parent's envelope, empty
for a root) and `chain` (array of the ancestors' envelopes, base64url).

A `status` record (topic `tasks.events`) carries a `TaskStatusUpdateEvent`
under `statusUpdate`. Its `wherry` fields are `attempt` (integer),
`task_hash` (string, hex SHA-256 of the submission's envelope), `receiver_id`
(string, the receiver that published it), `kind` (one of `claimed`,
`started`, `heartbeat` and `terminal`) and `log_offset` (integer, optional,
the agent log offset up to which the transcript is published).

An `artifact_update` record (topic `transcripts`) carries a
`TaskArtifactUpdateEvent` under `artifactUpdate`. It is one chunk of the
artifact `transcript`: one agent output line as a text part with media type
`application/x-ndjson`. An empty line or one that is not UTF-8 is a `raw`
part instead. A line over 512 KiB is a `url` part whose `metadata` holds the
line's hex `sha256` and its `size` in bytes. The first chunk has `append`
false and the rest true; the last chunk before the terminal status has
`lastChunk` true. Its `wherry` fields are `attempt` (integer) and `seq`
(integer, the line's log offset, which orders the chunks).

A `task` record (topic named by `reply_topic`) carries the final A2A `Task`
under `task`. Its `history` holds the submitted message and its `artifacts`
hold the result. Its `wherry` fields are `attempt` (integer) and `task_hash`
(string).

## Identifiers

The A2A task id is the run id: `message.taskId` in a submission, `taskId` in
the update events and `id` in the task. The `contextId` is the run id of the
root task, so every task in one delegation tree shares a context.

## Signing and transport

Each Kafka record value is a JSON envelope with members `v`, `type`,
`signer`, `payload` and `sig`. The `payload` member holds the record's exact
JSON bytes, base64url-encoded without padding. The signature is Ed25519 over
`wherry/v1/<type>`, a zero byte, and those payload bytes, where `<type>` is
the record type named above. Verifiers check the signature over the bytes as
transmitted before parsing them. Any Kafka consumer can read the records as
plain JSON without wherry code.
