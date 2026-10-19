# Reference

[Concepts](concepts.md) explains the model. The
[wherry A2A extension](a2a-extension-v1.md) specifies the records.

## Conventions

- `wherry admin` and `wherry receiver` read no client configuration. Every
  other command reads it first, and every command except `key` and `mcp`
  then connects to Kafka.
- A failed command prints `wherry: <error>` on standard error and exits
  with status 1.
- Durations use Go syntax, such as `90s`, `45m` or `720h`, and must be
  positive; a `revoke --wait` of zero or less takes the broker-derived
  wait.
- `continue`, `cancel`, `await`, `status` and `events` print
  `skipped N unverifiable records` on standard error when N records of the
  run fail verification.

### Names

Tenant, host, receiver, pool, adapter, agent and credential names match
`[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?`: 1 to 32 lowercase letters, digits and
hyphens, starting and ending with a letter or digit. Names contain no `.`,
so every principal, topic, group and transactional ID built from them parses
back to one set of names.

Run IDs match `[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`. The client generates
`run-` followed by 32 hex digits.

A key ID is `sha256:` and the unpadded base64url SHA-256 of the raw
public key.

### Labels

A label key is a name, or a prefix, `/` and a name:

- The name is 1 to 63 characters, letters, digits, `-`, `_` and `.`,
  starting and ending with a letter or digit.
- The prefix is a DNS subdomain of at most 253 characters: lowercase
  labels of letters, digits and `-`, joined by `.`.
- The prefix `wherry.io`, and any prefix ending in `.wherry.io`, is
  reserved.

A label value follows the name rule, so it is never empty.

### Selectors

A selector is a list of requirements, each naming one key once.

| Operator | Values | Holds when the label |
| --- | --- | --- |
| `In` | one or more | is present with one of the values |
| `NotIn` | one or more | is absent, or has none of the values |
| `Exists` | none | is present |
| `DoesNotExist` | none | is absent |

Values within a requirement are unique. An empty selector accepts every
adapter.

On the command line a selector is comma-separated:

| Term | Requirement |
| --- | --- |
| `key=v1\|v2` | `In` |
| `key!=v1\|v2` | `NotIn` |
| `key` | `Exists` |
| `!key` | `DoesNotExist` |

## Host commands

### `wherry init`

Creates the host key at `host_key` with mode 0600, unless the file exists,
creating its directory with mode 0700. It then registers the key on the
host's key topic when the topic holds none, and prints the key ID. It fails
when the key topic holds another key or has been revoked.

### `wherry key`

Prints the host key's key ID. The key file must grant no group or other
access.

### `wherry receivers`

Lists the receivers of every pool the tenant is entitled to, sorted by pool
and receiver ID:

```
receiver=<id> pool=<pool> capacity=<n> in_flight=<n> heartbeat_age=<age> status=<status>
  adapter=<name> agent=<agent> ready=<bool> labels=<k=v,...>
  seal=<credential> pool=<pool> expires=<RFC 3339 time>
```

`status` is `live` when the latest advertisement is at most 120 seconds
old, `stale` when it is older, `silent` when there is none, and `ended`
after the receiver's membership end. Adapters and their labels come from the
receiver's configuration record. `ready` is true when the receiver is live
and its advertisement offers the adapter. A live receiver with no ready
adapter lists each adapter with `ready=false`. `seal` lines list this host's
latest seal of each credential to the receiver. The command warns on
standard error about each seal that has expired or expires within 7 days.

`--json` prints a JSON array, one object per receiver:

| Member | Content |
| --- | --- |
| `receiver_id`, `pool` | strings |
| `adapters` | `{name, agent, labels, ready}` |
| `capacity`, `in_flight` | integers from the latest advertisement |
| `heartbeat_age` | Go duration string, or `-` |
| `status` | `live`, `stale`, `silent` or `ended` |
| `ended` | boolean |
| `seals` | `{name, pool, exp_ms}` |

### `wherry submit`

Resolves a placement, seals the agent's credentials, signs the task and
publishes it to the chosen pool. It prints `run_id=<id> pool=<pool>`, or
fails with `no matching live receiver`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--agent` | the pool's default, then the terms' order | agent to run |
| `--selector` | empty | [selector](#selectors) over adapter labels |
| `--prompt` | | prompt text |
| `--prompt-file` | | file holding the prompt |
| `--git` | | repository URL to check out |
| `--git-ref` | | branch, tag or commit to check out |
| `--git-path` | the repository name | checkout directory |
| `--deadline` | `30m` | time from now by which the run must end |
| `--queue-deadline` | `10m` | time from now by which a receiver must claim it |

Exactly one of `--prompt` and `--prompt-file` is required. `--git` and
`--git-ref` go together; the
[extension](a2a-extension-v1.md#git-context) gives the rules for the URL,
ref and path.

The command seals each credential that the pool's terms list for the agent
and the host configuration names, to every live member of the pool that
lacks a seal under its current sealing key lasting past the queue deadline.
When there is a credential to seal, it fails if the pool's seal lifetime
ends before the queue deadline, and if a receiver's sealing key differs from
the one this host pinned.

### `wherry continue RUN_ID`

Publishes the next turn of a run interrupted in `TASK_STATE_AUTH_REQUIRED`
or `TASK_STATE_INPUT_REQUIRED`, to the pool and placement its echo names,
sealing as `submit` does. It prints `run_id=<id> turn=<n>`. `resume` is an
alias.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--prompt` | `continue` | text of the new turn |
| `--deadline` | `30m` | run deadline for the turn |
| `--queue-deadline` | `10m` | queue deadline for the turn |

It fails when the run is not interrupted, is cancelled, or already has a
next turn.

### `wherry cancel RUN_ID`

Publishes a signed cancel of the run's latest turn to its pool's events
and prints `run_id=<id>`.

### `wherry await RUN_ID`

Waits for the latest turn of a run to finish or be interrupted. `--timeout`
sets the wait, `30m` by default.

It prints the state on the first line. For a turn that ended with a result,
each following line is one part of a result artifact. For a turn that ended
with a status alone, each line is one part of the status message. A part
prints as its text, its JSON data, `[url <url>]`, or
`[raw <media type> <n> bytes]`. For `TASK_STATE_AUTH_REQUIRED` it prints
`continue with: wherry continue <id>` on standard error.

`await` exits with status 1, `run ended in <state>`, for any state other
than `TASK_STATE_COMPLETED`, and with status 1 when the timeout passes.

### `wherry status [RUN_ID]`

Reads the latest verified state of a run's latest turn without waiting.
With `RUN_ID` it prints the state on the first line, `turn=<n>` on the
second, then one line per part of the status message. The state is
`TASK_STATE_SUBMITTED` until a receiver reports one. For
`TASK_STATE_AUTH_REQUIRED` it prints `continue with: wherry continue <id>`
on standard error.

Without `RUN_ID` it prints one line per run, latest update first:

```
<run> pool=<pool> turn=<n> <state>
```

The list holds the runs in this host's records in `state_dir` and the runs
a receiver echoed on the tenant feed, which include runs from the tenant's
other hosts. A run's update time is that of its latest status. While it has
none, the time is this host's submission of the turn, else the modification
time of this host's record of the run when the record predates submission
times, else the Kafka timestamp of the turn's echo. A record in `state_dir`
that does not decode counts as skipped, and its run is listed only through
its echo. With no runs it prints nothing.

`status` exits with status 0 whenever it reads a state, whatever the
state. It fails when the run is not found or the read fails.

### `wherry events RUN_ID`

Prints a run's verified history: one line per status, one per event ID, in
feed order, then one line per turn and attempt with its transcript chunk
count.

```
turn=<n> attempt=<n> kind=<kind> state=<state> receiver=<id> time=<time> message="<text>"
turn=<n> attempt=<n> transcript_chunks=<n>
```

`message` appears when the status has one. Heartbeats are not on the feed,
so they do not appear.

`await`, `status`, `events`, `continue` and `cancel` find a run from this host's
record of it in `state_dir`, or from a receiver's echo on the tenant feed.
They fail with `run not found` when there is neither.

### `wherry mcp`

Serves the [MCP tools](#mcp-tools) over stdio.

## Admin commands

`wherry admin` connects twice, as the Kafka super user and as
`wherry.operator`, with the [environment](#environment-variables) set.
It writes every `wherry.admin` record as `wherry.operator`, creating
`wherry.admin` and `wherry.receivers` first if they are missing. Options
take a value and can appear anywhere after `admin`. `pool create`,
`tenant add`, `host add` and `receiver add` then create every ACL the new
state calls for, re-read them, and fail with `ACL plan incomplete` when one
is missing.

A bundle file must not exist beforehand. A command that fails removes the
bundle it created.

### `wherry admin pool create POOL --terms FILE`

Records the [pool terms](#pool-terms) in `FILE`, creates the pool's tasks
and events topics, and sets the configuration of share group `p-<pool>`:
`share.auto.offset.reset=earliest`, `share.isolation.level=read_committed`,
`share.delivery.count.limit` and `share.record.lock.duration.ms` from the
terms. It refuses a delivery limit above the lowest
`group.share.max.delivery.count.limit` across the brokers. For an existing
pool it records the new terms and sets the group configuration again.

### `wherry admin tenant add TENANT --pool POOL`

Entitles the tenant to an existing pool, creates the tenant feed, and
grants the tenant's hosts and the pool's receivers their ACLs. It applies
each host's [quotas](#quotas).

### `wherry admin host add TENANT HOST --bundle FILE`

Records the host, creates its principal, its key topic and the tenant
feed, grants its ACLs, applies its quotas, and writes its bundle.

### `wherry admin receiver add RECEIVER --pool POOL --config FILE --bundle FILE`

Validates the [receiver configuration](#receiver-configuration) against the
pool's terms, records it, creates the receiver's principal and key topic,
grants its ACLs, and writes its bundle. It refuses an existing receiver.

`--platform-credential ADAPTER=FILE` puts the contents of `FILE`, less
trailing CR and LF, in the bundle as the platform credential of the named
adapter.

### `wherry admin receiver configure RECEIVER --config FILE`

Records a new configuration for a live receiver in its pool. The
principal, password and bundle stay as they are. The receiver exits when it
reads the new record, and its platform restarts it.

### `wherry admin revoke host TENANT HOST`

### `wherry admin revoke receiver RECEIVER`

Runs the [revocation](concepts.md#revocation) steps. When it ends, it
prints one line per step it ran:

```
step=<n> complete=<bool> name=<step name>
```

The line of a failed step ends in ` error=<cause>`. It stops at the first
incomplete step and fails with `revocation incomplete`. The probe asks every
broker for the metadata of the principal's own key topic, and step 3
completes when every broker answers `TOPIC_AUTHORIZATION_FAILED`. While a
broker still authorises the principal, step 3 probes again every second for
up to 2 minutes. It fails at once when the login fails or the probe returns
any other error. Step 5 waits the largest of
`connections.max.reauth.ms`, `group.share.max.record.lock.duration.ms` and
`transaction.max.timeout.ms` across the brokers, and fails when one of them
is unbounded. `--wait DURATION` replaces that wait.

### `wherry admin skip OFFSET --sha256 HASH`

Records the decision to pass over the undecodable record at `OFFSET` on
`wherry.admin`, whose SHA-256 is `HASH`. Loading `wherry.admin` fails while
any record on it does not decode, and the error lists each one as
`offset=<n> size=<n> sha256=<hex>`. The command refuses an offset whose
record decodes and a hash that does not match.

### `wherry admin check`

Loads `wherry.admin`, checks that no two names collide, validates every
receiver configuration against its pool's terms, and checks that every ACL
the state calls for is present. It does not report ACLs beyond those, and
does not check an adapter's `type` or `config` section.

## Receiver command

### `wherry receiver --bundle FILE --state-dir DIR`

Runs a receiver as the bundle's principal. It creates `DIR` with mode 0700
and keeps `signing.key` and `sealing.key` in it. On start it:

1. registers both public keys on its key topic when the topic is empty,
   and fails when the topic holds other keys;
2. waits for its configuration record on `wherry.admin` and validates it
   against the pool's terms;
3. builds its adapters, reads its pool's events once, and starts
   advertising and claiming.

It logs to standard output with the prefix `receiver: `. It exits on
`SIGINT` or `SIGTERM` without stopping its jobs, and fails with
`receiver configuration changed` when a new configuration record appears.
It refuses to start after its membership end.

Each adapter takes its platform credential from the bundle's `platform`
entry for its name, or else from `NOMAD_TOKEN`.

## MCP tools

`wherry mcp` serves seven tools over stdio. Each takes and returns JSON with
a declared schema and opens its own Kafka connection. Deadlines given as `0`
or omitted take the CLI defaults, 1800 and 600 seconds.

| Tool | Input | Output |
| --- | --- | --- |
| `receivers` | none | `receivers`, the objects `wherry receivers --json` prints |
| `submit` | `prompt`, `agent`, `selector`, `git`, `deadline_seconds`, `queue_deadline_seconds` | `id`, `pool` |
| `status` | `id`, optional | `runs`, `skipped` |
| `await` | `id` | `id`, `state`, `retry_after`, `message`, `artifacts`, `skipped` |
| `continue` | `id`, `prompt`, `deadline_seconds`, `queue_deadline_seconds` | `id`, `turn`, `skipped` |
| `cancel` | `id` | `id`, `skipped` |
| `events` | `id` | `id`, `events`, `transcripts`, `skipped` |

- `submit` requires `prompt`. `selector` is an array of
  `{key, operator, values}`. `git` is `{url, ref, path}`, with `path`
  optional.
- `status` returns the latest verified state of a run's latest turn
  without waiting. `runs` holds one entry for the run `id` names, or,
  without `id`, one per run `wherry status` lists, latest update first.
  Each entry is `{id, pool, turn, state, message, updated}`. The state is
  `TASK_STATE_SUBMITTED` until a receiver reports one. `updated` is the
  RFC 3339 time of the latest status. While the run has none, it is this
  host's submission of the turn, else the modification time of this host's
  record of the run when the record predates submission times, else the
  Kafka timestamp of the turn's echo.
- `await` waits up to 45 seconds. It returns the outcome with `message` and
  `artifacts`, or, while the run is unfinished, its current `state` with
  `retry_after` set to 5. With a progress token it sends a progress
  notification every 5 seconds. Each artifact is `{artifact_id, parts}`,
  and each part has one of `text`, `data`, `url` or `raw_bytes` (the size
  of a binary part, which is not included), with `media_type`.
- `continue` requires `id`. `prompt` defaults to `continue`.
- `events` returns `events`, each
  `{turn, attempt, kind, state, receiver, time, message}`, and
  `transcripts`, each `{turn, attempt, chunks}`.
- `skipped` counts the run's records that failed verification; for a
  `status` list, the records of every listed run.

A failed call returns `{"code", "message"}`. The message never carries
error text from the bus, the broker or the host key.

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | an input is missing or malformed; the message names the rule it breaks |
| `RUN_NOT_FOUND` | this host has not submitted the run and no receiver has admitted it |
| `NO_MATCHING_RECEIVER` | no live receiver of an entitled pool can take the agent and selector |
| `NOT_AWAITING_CONTINUATION` | the run is not interrupted |
| `RUN_CANCELED` | the run is cancelled |
| `TURN_ALREADY_SUBMITTED` | the next turn is already published |
| `CANCELLED` | the request was cancelled |
| `UNAVAILABLE` | any other failure; the CLI reports the cause |

## Client configuration

A JSON object at `~/.config/wherry/config.json`, or at the path in
`WHERRY_CONFIG`. Unknown and duplicate members are errors. A leading `~/`
is expanded in `WHERRY_CONFIG` and in every path.

| Member | Default | Content |
| --- | --- | --- |
| `bundle` | required | the host's bundle |
| `host_key` | `~/.config/wherry/host.key` | host signing key file |
| `state_dir` | `~/.config/wherry/state` | run records and sealing-key pins |
| `credentials` | none | credential name to the file holding its value |

`state_dir` holds `runs/<run>.json` for each run this host submitted, and
`pins.json`, the sealing-key fingerprint pinned for each receiver. A
credential value is at most 16 KiB. The file is read at each seal, and its
contents are sealed as they are.

## Bundle

A YAML file that `wherry admin` writes with mode 0600. Unknown fields are
errors.

| Field | Content |
| --- | --- |
| `broker` | Kafka bootstrap address, `host:port` |
| `ca` | PEM CA certificate for the broker's TLS certificate |
| `principal` | SCRAM-SHA-512 user name |
| `password` | SCRAM password |
| `platform` | receivers only: adapter name to platform credential |

## Pool terms

The YAML file `pool create --terms` reads. Unknown fields are errors.

| Field | Content |
| --- | --- |
| `agents` | one or more `{name, credentials}`: an agent and the credential names it may take |
| `default_agent` | an agent from `agents`, used when a task names none |
| `seal_lifetime_ms` | lifetime of each seal a host writes |
| `delivery_limit` | the share group's `share.delivery.count.limit` |
| `lock_duration_ms` | the share group's `share.record.lock.duration.ms` |
| `producer_byte_rate` | host quota, bytes per second |
| `request_percentage` | host quota, percentage of broker request time |

Every number is positive. Set `lock_duration_ms` above 10000, since a
receiver renews its lock every 10 seconds; wherry does not check this. The
broker bounds it between
`group.share.min.record.lock.duration.ms` and
`group.share.max.record.lock.duration.ms`. Every delivery counts against
`delivery_limit`, including a release after a start failure. From attempt
`delivery_limit` minus 1, a receiver ends a turn `TASK_STATE_FAILED` with
`delivery budget exhausted` where it would release it, except in the cases
the [extension](a2a-extension-v1.md#delivery-budget) lists.

```yaml
agents:
  - name: coder
    credentials: [model-api-key]
default_agent: coder
seal_lifetime_ms: 86400000
delivery_limit: 5
lock_duration_ms: 15000
producer_byte_rate: 1048576
request_percentage: 10
```

### Quotas

Each host principal gets the Kafka client quotas `producer_byte_rate` and
`request_percentage`, the lowest of each across the pools its tenant is
entitled to.

## Receiver configuration

The YAML file `receiver add` and `receiver configure` read, recorded on
`wherry.admin`. Unknown fields are errors.

| Field | Content |
| --- | --- |
| `receiver` | optional; must equal the command's receiver |
| `pool` | optional; on `receiver add` must equal `--pool`; `receiver configure` keeps the receiver's pool |
| `capacity` | runs the receiver holds at once, positive |
| `adapters` | one or more adapter entries, in routing order |

Each adapter entry:

| Field | Content |
| --- | --- |
| `name` | adapter name, unique in the receiver |
| `type` | `nomad` |
| `agent` | the agent it launches, from the pool's terms |
| `credential` | optional; a credential the terms allow for the agent, delivered to the job |
| `labels` | optional; [labels](#labels) for placement |
| `config` | the adapter type's settings |

### Nomad adapter settings

| Field | Default | Content |
| --- | --- | --- |
| `address` | `NOMAD_ADDR`, else `http://127.0.0.1:4646` | Nomad API address |
| `ca_file` | none | CA bundle for the Nomad API, a path on the receiver |
| `namespace` | `default` | namespace for agent jobs and variables; `NOMAD_NAMESPACE` is not read |
| `credential_env` | none | environment variable the job receives the credential in |
| `node_pool` | `default` | node pool for agent jobs |
| `datacenters` | every datacenter | datacenters for agent jobs |
| `runtime` | required | the agent task's definition |

`credential_env` is required when the adapter sets `credential`, and
matches `[A-Za-z_][A-Za-z0-9_]*`. Unknown fields are errors. `wherry admin`
does not check `type` or the `config` section; the receiver checks them at
start and exits with `adapter <name>: ...`.

`runtime` has these fields:

| Field | Content |
| --- | --- |
| `driver` | Nomad task driver, required |
| `config` | the driver's task configuration |
| `env` | environment variables for the agent task |
| `resources` | `cpu` (MHz) and `memory_mb`; unset values take Nomad defaults |

The adapter sets `HOME` in `env`. [Agent jobs](#agent-jobs) describes the
job it builds.

```yaml
capacity: 2
adapters:
  - name: container
    type: nomad
    agent: coder
    credential: model-api-key
    labels: {isolation: container}
    config:
      address: https://nomad.example.com:4646
      ca_file: /secrets/nomad-ca.pem
      namespace: agents
      credential_env: MODEL_API_KEY
      runtime:
        driver: docker
        config:
          image: registry.example.com/agent:1.0
          command: /bin/sh
          args: ["-c", "exec agent-run < \"$NOMAD_TASK_DIR/prompt.txt\""]
        resources: {cpu: 1000, memory_mb: 2048}
```

#### Agent jobs

The Nomad adapter registers one batch job per turn,
`agent-<pool>-<run>-t<turn>`, in its namespace, node pool and datacenters, with
restarts and rescheduling disabled. Its task group `agent` holds the task
`agent`, built from the adapter's `runtime`, and the prestart task `git`
when the run carries a git context. The job's metadata holds `run_id`,
`turn`, `attempt`, `adapter` and `agent`. A receiver adopts an existing
job only when its `adapter` and `agent` match the adapter that finds it.
It leaves a job whose `adapter` names another of its adapters to that
adapter, and stops and purges any other job under the turn's ID and
dispatches the turn again.

The prompt, the credential and the git context travel in Nomad variables
under `nomad/jobs/agent-<pool>-<run>-t<turn>`, which the job's own workload
identity reads. The adapter deletes them when the turn ends. Every 30
seconds it also deletes the variables of jobs that have stopped, and of
jobs never registered within 10 minutes, and purges agent jobs that have
been dead for 10 minutes. `<pool>` is the first 12 hexadecimal digits of
the SHA-256 of the pool name, since Nomad limits a variable path to 128
characters. A receiver sweeps only its own pool's jobs and variables. The 10 minutes run from a job's last allocation
change, so a turn whose job ended while no pool member could hold it for
longer than that runs again from the start on its next delivery.

An agent task with no `Started` event in a terminal allocation never
started. The receiver records a `platform` start failure, purges the job
and its variables, and returns the task to the pool.

An adapter is ready when some ready, eligible node in its node pool and
datacenters has the runtime's driver detected and healthy. A receiver with
no ready adapter advertises with no offers, so it stays `live` with every
adapter `ready=false`, and placement passes over it.

The receiver stops a job by sending `SIGTERM` to the agent task. If the
task is still running 15 seconds later, it deregisters the job.

#### Agent contract

The agent task receives:

- the prompt in `$NOMAD_TASK_DIR/prompt.txt`;
- the credential in the environment variable `credential_env` names, when
  the adapter delivers one;
- `HOME` set to the task directory;
- the runtime's `env`, driver `config` and `resources`.

The agent writes JSON lines to standard output. The receiver publishes each
line as a transcript chunk. The line that decodes as a JSON object with
`"type": "result"` ends the turn: its `result` string becomes the result,
and `"is_error": true` makes the state `TASK_STATE_FAILED`. The receiver
stops the job once it reads that line. A job that ends without one fails
with the message `agent ended without a result`. Claude Code's
[headless](https://docs.claude.com/en/docs/claude-code/headless) stream-json
output writes this format.

A turn's run time is capped at its deadline, and at one hour after its
queue deadline.

For the `podman` driver the adapter reads the logs in CRI format, and the
git task gets `entrypoint` and `args` in place of `command`. For other
drivers the git task's `command` and `args` are replaced.

## Kafka resources

### Topics

`wherry admin` creates each topic with one partition, the broker's default
replication factor, `cleanup.policy=delete` and
`message.timestamp.type=LogAppendTime`.

| Topic | Created by | `retention.ms` |
| --- | --- | --- |
| `wherry.admin` | any recording command | `-1` |
| `wherry.receivers` | any recording command | `604800000` (7 days) |
| `wherry.pool.<pool>.tasks` | `pool create` | `-1` |
| `wherry.pool.<pool>.events` | `pool create` | `-1` |
| `wherry.tenant.<tenant>` | `tenant add`, `host add` | `2592000000` (30 days) |
| `wherry.receiver.<receiver>` | `receiver add` | `-1` |
| `wherry.host.<tenant>.<host>` | `host add` | `-1` |

`wherry admin` leaves an existing topic's configuration as it is, so a
tenant feed's retention can be changed with Kafka's own tools.

### ACLs

Every grant allows from any host. Resources are literal except the
transactional ID prefix.

| Principal | Resource | Operations |
| --- | --- | --- |
| `wherry.operator` | topic `wherry.admin` | Read, Write, Describe |
| host | topic `wherry.admin` | Read, Describe |
| host | topic `wherry.receivers` | Read, Describe |
| host | topic `wherry.tenant.<own tenant>` | Read, Describe |
| host | topic `wherry.host.<own tenant>.<own host>` | Read, Write, Describe |
| host | topics `wherry.pool.<p>.tasks` and `.events`, each entitled pool | Write, Describe |
| host | topic `wherry.receiver.<r>`, each receiver of an entitled pool | Read, Describe |
| receiver | topic `wherry.admin` | Read, Describe |
| receiver | topic `wherry.receiver.<own>` | Read, Write, Describe |
| receiver | topic `wherry.receiver.<peer>`, each other member of its pool | Read, Describe |
| receiver | topic `wherry.receivers` | Read, Write, Describe |
| receiver | topic `wherry.pool.<own pool>.tasks` | Read, Describe |
| receiver | topic `wherry.pool.<own pool>.events` | Read, Write, Describe |
| receiver | group `p-<own pool>` | Read, Describe, DescribeConfigs |
| receiver | transactional ID prefix `p.<own pool>.` | Write, Describe |
| receiver | topic `wherry.tenant.<t>`, each tenant entitled to its pool | Write, Describe |
| receiver | topic `wherry.host.<t>.<h>`, each host of those tenants | Read, Describe |

A principal whose membership has ended has no grants.

## Environment variables

| Variable | Read by | Effect |
| --- | --- | --- |
| `WHERRY_CONFIG` | host commands | client configuration path |
| `WHERRY_TRUST_BROKER` | `wherry admin` | Kafka bootstrap address, also written into bundles |
| `WHERRY_TRUST_CA` | `wherry admin` | PEM CA file for the broker, also written into bundles |
| `WHERRY_TRUST_ADMIN_USER` | `wherry admin` | super user name |
| `WHERRY_TRUST_ADMIN_PASSWORD_FILE` | `wherry admin` | file holding the super user's SCRAM password |
| `WHERRY_TRUST_OPERATOR_USER` | `wherry admin` | operator principal name; must be `wherry.operator` |
| `WHERRY_TRUST_OPERATOR_PASSWORD_FILE` | `wherry admin` | file holding the operator's SCRAM password |
| `NOMAD_TOKEN` | `wherry receiver` | Nomad token for adapters without a platform credential in the bundle |
| `NOMAD_ADDR` | `wherry receiver` | Nomad address when `address` is empty |
| `NOMAD_REGION` | `wherry receiver` | Nomad region for API requests |

Trailing CR and LF are trimmed from password files.
