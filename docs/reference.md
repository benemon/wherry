# Reference

The [wherry A2A extension](a2a-extension-v1.md) defines the record formats
and the signed envelope.

## Conventions

- Every command except `receiver` reads the client configuration first.
  Every command except `init`, `key`, `mcp` and `receiver` then connects to
  Kafka.
- A failed command prints `wherry: <error>` on standard error and exits
  with status 1.
- Durations use Go syntax, such as `90s`, `45m` or `720h`. A duration must
  be positive.
- A command that reads records prints `skipped N unverifiable records` on
  standard error when N records fail verification.
- Labels are written `key=value,key=value`, with each key once.
- Selectors are written `key=value|value,key=value`. A requirement holds
  when the label with that key has one of the listed values. `--selector ''`
  is an empty selector, which every label set satisfies.

### Label keys and IDs

A label key is one of `platform`, `runtime`, `region` and `zone`, or has
one of two forms:

- `<name>.<name>`, each name a lowercase letter followed by lowercase
  letters, digits or `_`, such as `nomad.node_pool`;
- `<domain>/<name>`, such as `example.com/team`. Both parts start with a
  lowercase letter or digit. The domain holds lowercase letters, digits,
  `.` and `-`, and the name can also hold `_`.

Label values are non-empty. `platform` is the matched adapter's platform.
Receiver labels and offer labels cannot set it.

Receiver IDs, pool names and adapter names are non-empty lowercase strings.

## Commands

### `wherry init`

Creates the user key at the configured `user_key` path, with mode 0600, and
prints its fingerprint. It refuses to replace an existing file.

### `wherry key`

Prints the user public key and its fingerprint:

```
public_key=<base64url> fingerprint=sha256:<hex>
```

The key file must grant no group or other access.

### `wherry enrol list`

Prints each verified enrolment request, sorted by receiver ID:

```
receiver=<id> request=<hash> labels=<labels> approved=false
receiver=<id> request=<hash> labels=<labels> approved=true generation=<n>
  adapter=<name> platform=<platform>
    offer <labels>
```

The labels are those the receiver requested. An approved line ends with
`renewal=requested` while the receiver's current generation has an
unanswered renew request.

### `wherry enrol approve REQUEST_HASH`

Signs an approval for the request and prints
`receiver=<id> generation=<n>`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--labels` | required | approved receiver labels |
| `--grants` | required | JSON file holding the adapter grants |
| `--pool` | the receiver ID | pool name |
| `--renewal` | `auto` | renewal policy, `auto` or `manual` |
| `--expires` | `720h` | approval lifetime |

The generation is one more than the receiver's highest approved or revoked
generation. The command fails when a grant does not name an adapter and
platform in the request.

### `wherry enrol renew RECEIVER_ID`

Signs a renewal of the receiver's current approval and prints
`receiver=<id> generation=<n> renewed=true`. The renewal keeps every term
and expires 30 days after it is signed. It fails for a receiver never
approved, a revoked receiver, and a renewal that would not extend the
approval.

### `wherry enrol revoke RECEIVER_ID`

Signs a revocation floor at the receiver's highest approved generation and
prints `receiver=<id> floor_generation=<n>`. It warns on standard error that
credentials sealed to the receiver are not rotated.

### `wherry secret put NAME`

Seals the contents of a file to one enrolled receiver as the next version of
the credential `NAME`. It prints `name=<name> receiver=<id> version=<n>`,
then one `waiting run_id=<id> turn=<n>` line for each run in the receiver's
pool that is interrupted for this credential and not cancelled.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--to` | required | receiver ID |
| `--from-file` | required | file holding the credential value |
| `--expires` | `720h` | seal lifetime |

The receiver must hold a current, unexpired approval.

### `wherry receivers`

Lists enrolled and revoked receivers, sorted by pool and receiver ID:

```
receiver=<id> pool=<pool> generation=<n> labels=<labels> capacity=<n>
in_flight=<n> heartbeat_age=<age> status=<status>
  adapter=<name> platform=<platform> defaults=<labels>
    offer <labels>
  secret=<name> version=<n> expires=<RFC 3339 time>
receiver=<id> pool=<pool> generation=<n> floor_generation=<n> status=revoked
```

The first two lines print as one line. `status` is `live` when the latest
capability is at most 120 seconds old, `stale` when it is older, and
`silent` when there is none. The command warns on standard error about each
sealed credential that has expired or expires within 7 days.

`--json` prints a JSON array instead, with one object per receiver:

| Member | Content |
| --- | --- |
| `receiver_id`, `pool` | strings |
| `generation`, `floor_generation` | integers |
| `labels` | approved receiver labels |
| `adapters` | grants: `{name, platform, allowed, defaults}` |
| `offers` | `{adapter, labels}` from the latest capability |
| `capacity`, `in_flight` | integers |
| `heartbeat_age` | Go duration string, or `-` |
| `status` | `live`, `stale`, `silent` or `revoked` |
| `revoked` | boolean |
| `secrets` | `{name, version, exp_ms}` |

### `wherry submit`

Publishes a task to the live receiver, among those with an offer that
matches `--selector`, with the most spare capacity. Ties go to the lowest receiver ID. It prints
`run_id=<id> pool=<pool>`, or fails with `no matching live receiver`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--selector` | required | task selector, applied to offer labels |
| `--prompt` | | prompt text |
| `--prompt-file` | | file holding the prompt |
| `--git` | | repository URL to check out |
| `--git-ref` | | branch, tag or commit to check out |
| `--git-path` | the repository name | checkout directory |
| `--deadline` | `30m` | time from now by which the run must end |
| `--queue-deadline` | `10m` | time from now by which a receiver must claim it |

Exactly one of `--prompt` and `--prompt-file` is required. `--git` and
`--git-ref` go together. The
[extension](a2a-extension-v1.md#git-context) gives the rules for the URL,
ref and path.

### `wherry continue RUN_ID`

Publishes the next turn of a run interrupted in
`TASK_STATE_AUTH_REQUIRED` or `TASK_STATE_INPUT_REQUIRED`, and prints
`run_id=<id> turn=<n>`. `resume` is an alias.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--prompt` | `continue` | text of the new turn |
| `--deadline` | `30m` | run deadline for the turn |
| `--queue-deadline` | `10m` | queue deadline for the turn |

It fails when the run is not interrupted, is cancelled, or already has a
next turn.

### `wherry cancel RUN_ID`

Publishes a user-signed cancel for the run and prints `run_id=<id>`.

### `wherry await RUN_ID`

Waits for the latest turn of a run to finish or be interrupted. `--timeout`
sets the wait, `30m` by default.

It prints the state on the first line. For a run that ended with a result,
each following line is one part of a result artifact. For a run that ended
with a status alone, each line is one part of the status message. A part
prints as its text, its JSON data, `[url <url>]`, or
`[raw <media type> <n> bytes]`.

On standard error it prints `signer=revoked-before=<time>` when a since
revoked receiver signed the outcome, and `continue with: wherry continue
<id>` for `TASK_STATE_AUTH_REQUIRED`. An interrupted run that the user has
cancelled ends as `TASK_STATE_CANCELED`.

`await` exits with status 1, `run ended in <state>`, for any state other
than `TASK_STATE_COMPLETED`, and with status 1 when the timeout passes.

### `wherry events RUN_ID`

Prints a run's verified history: one line per status record in log order,
a cancel line if the user cancelled it, and one line per turn and attempt
with its transcript chunk count.

```
turn=<n> attempt=<n> kind=<kind> state=<state> receiver=<id> time=<time>
message="<text>"
kind=cancel state=TASK_STATE_CANCELED signer=user
turn=<n> attempt=<n> transcript_chunks=<n>
```

The first two lines print as one line. `message` appears when the status has
one. A line from a since-revoked receiver ends with
`signer=revoked-before=<time>`.

### `wherry mcp`

Serves the [MCP tools](#mcp-tools) over stdio.

### `wherry receiver --config PATH`

Runs a receiver with the [receiver configuration](#receiver-configuration)
at `PATH`. It logs to standard output with the prefix `receiver: ` and
exits on `SIGINT` or `SIGTERM`.

## MCP tools

`wherry mcp` serves seven tools. Each takes and returns JSON with a
declared schema, and opens its own Kafka connection. Deadlines given as
`0` or omitted take the CLI defaults, 1800 and 600 seconds.

- `receivers` takes no input and returns `receivers` and `skipped`.
- `submit` takes `prompt`, `selector`, `git`, `deadline_seconds` and
  `queue_deadline_seconds`, and returns `id`, `pool` and `skipped`.
- `status` takes `id` and returns `id`, `state`, `turn`, `message`,
  `signer_revoked_before` and `skipped`.
- `await` takes `id` and returns `id`, `state`, `retry_after`, `message`,
  `artifacts`, `signer_revoked_before` and `skipped`.
- `continue` takes `id`, `prompt`, `deadline_seconds` and
  `queue_deadline_seconds`, and returns `id`, `turn` and `skipped`.
- `cancel` takes `id` and returns `id` and `skipped`.
- `events` takes `id` and returns `id`, `events`, `cancelled`,
  `transcripts` and `skipped`.

`skipped` counts the records that failed verification. Tool by tool:

- `receivers` returns the objects `wherry receivers --json` prints.
- `submit` requires `prompt` and `selector`. `selector` maps each key to the
  values it accepts. `git` is `{url, ref, path}`, with `path` optional.
- `status` returns the latest verified state of the run's latest turn
  without waiting. The state is `TASK_STATE_SUBMITTED` until a receiver
  reports one.
- `await` waits up to 45 seconds. It returns the final state with
  `artifacts` and `message`, or, while the run is unfinished, its current
  `state` with `retry_after` set to 5 seconds. With a progress token it
  sends a progress notification every 5 seconds. Each artifact is
  `{artifact_id, parts}`, and each part has one of `text`, `data`, `url` or
  `raw_bytes` (the size of a binary part, which is not included), with
  `media_type`.
- `continue` requires `id`. `prompt` defaults to `continue`.
- `events` returns `events`, each `{turn, attempt, kind, state, receiver,
  time, message, signer_revoked_before}`, and `transcripts`, each
  `{turn, attempt, chunks}`.

A failed call returns `{"code", "message"}`. The message never carries the
underlying error text.

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | an input is missing or malformed |
| `RUN_NOT_FOUND` | no verified submission has this run ID |
| `NO_MATCHING_RECEIVER` | no live receiver matches the selector |
| `NOT_AWAITING_CONTINUATION` | the run is not interrupted |
| `RUN_CANCELED` | the run is cancelled |
| `TURN_ALREADY_SUBMITTED` | the next turn is already published |
| `CANCELLED` | the request was cancelled |
| `UNAVAILABLE` | any other failure; the CLI reports the cause |

## Client configuration

A JSON object at `~/.config/wherry/config.json`, or at the path in
`WHERRY_CONFIG`. Unknown and duplicate members are errors. A leading `~/`
is expanded in `WHERRY_CONFIG`, `ca_file`, `kafka_password_file` and
`user_key`.

| Member | Default | Content |
| --- | --- | --- |
| `broker` | required | Kafka bootstrap address, `host:port` |
| `ca_file` | required | PEM CA bundle for the broker's TLS certificate |
| `kafka_user` | required | SCRAM-SHA-512 user name |
| `kafka_password_file` | required | file holding the SCRAM password |
| `user_key` | `~/.config/wherry/user.key` | user key file |
| `reply_topic` | `replies.<kafka_user>` | topic receivers publish results to |

Trailing CR and LF characters are trimmed from the password.

## Receiver configuration

A JSON object at the path `wherry receiver --config` names. Unknown and
duplicate members are errors.

| Member | Default | Content |
| --- | --- | --- |
| `broker` | required | Kafka bootstrap address, `host:port` |
| `ca_file` | required | PEM CA bundle for the broker's TLS certificate |
| `kafka_user` | required | SCRAM-SHA-512 user name |
| `kafka_password_file` | required | file holding the SCRAM password |
| `receiver_id` | required | the receiver's ID |
| `state_dir` | required | directory for the receiver's keys |
| `user_public_key` | required | user public key, unpadded base64url |
| `capacity` | `2` | runs the receiver holds at once |
| `labels` | `{}` | labels the enrolment request asks for |
| `adapters` | required | one or more adapter entries |

The receiver creates `state_dir` with mode 0700 and keeps `signing.key` and
`sealing.key` in it. `user_public_key` is the `public_key` value that
`wherry key` prints.

Each adapter entry has these members:

| Member | Default | Content |
| --- | --- | --- |
| `name` | required | adapter name, unique in the receiver |
| `type` | required | `nomad` |
| `credential` | none | name of the sealed credential the agent receives |
| `config` | required | the adapter's own configuration |

### Nomad adapter configuration

| Member | Default | Content |
| --- | --- | --- |
| `address` | `NOMAD_ADDR`, else `http://127.0.0.1:4646` | Nomad API address |
| `ca_file` | none | CA bundle for the Nomad API |
| `namespace` | none | namespace for agent jobs and variables |
| `credential_env` | none | environment variable for the credential |
| `runtimes` | none | runtime name to runtime definition |

`credential_env` is required when the adapter entry sets `credential`, and
must match `[A-Za-z_][A-Za-z0-9_]*`. Each runtime name becomes the value of
the offer label `runtime`.

A runtime definition has these members:

| Member | Content |
| --- | --- |
| `driver` | Nomad task driver, required |
| `config` | the driver's task configuration |
| `env` | environment variables for the agent task |
| `resources` | `cpu` (MHz) and `memory_mb`; unset values take Nomad defaults |

The adapter sets `HOME` in `env`. The
[operator guide](operator-guide.md#agent-jobs) describes the job it builds.

### Grants file

The file `enrol approve --grants` reads is a JSON array of grants. Unknown
and duplicate members are errors.

| Member | Content |
| --- | --- |
| `name` | an adapter name from the enrolment request |
| `platform` | that adapter's platform |
| `allowed` | offer label key to the values the adapter may advertise |
| `defaults` | key to the value used when a task selector omits the key |

`allowed` and `defaults` are required and can be empty objects. Each
default value appears in `allowed` for its key. No `allowed` key is
`platform` or an approved receiver label key.

## Environment variables

| Variable | Read by | Effect |
| --- | --- | --- |
| `WHERRY_CONFIG` | all commands but `receiver` | client configuration path |
| `NOMAD_TOKEN` | `wherry receiver` | Nomad ACL token for every adapter |
| `NOMAD_ADDR` | `wherry receiver` | Nomad address when `address` is empty |
| `NOMAD_REGION` | `wherry receiver` | Nomad region for API requests |
| `WHERRY_KAFKA_TEST_BROKER` | tests | plaintext broker for Kafka tests |
