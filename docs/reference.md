# Reference

[Concepts](concepts.md) explains the model. The
[wherry A2A extension](a2a-extension-v1.md) specifies the records.

## Conventions

- `wherry admin` and `wherry receiver` read no client configuration. Every
  other command reads it first, and every command except `key` and `mcp`
  then connects to Kafka.
- Every host command takes `--config-file FILE`. It defaults to
  `~/.config/wherry/config.json`. A leading `~/` is expanded.
- A failed command prints `wherry: <error>` on standard error and exits
  with status 1.
- Durations use Go syntax, such as `90s`, `45m` or `720h`, and must be
  positive; a `revoke --wait` of zero or less takes the broker-derived
  wait.
- `continue`, `cancel`, `await`, `status` and `events` print
  `skipped N unverifiable records` on standard error when N records of the
  run fail verification.
- `await` and `status` print each redaction marker of a run this host
  submitted as `[redacted:<name>]`, naming the credential or S3 object,
  and `<name> redacted N times` on standard error for each name whose
  markers they printed. A host maps a marker with the run's redaction key
  in `state_dir`, and prints a marker as it is when the run's record holds
  no 32-byte key.

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
```

`status` is `live` when the latest advertisement is at most 120 seconds
old, `stale` when it is older, `silent` when there is none, and `ended`
after the receiver's membership end. Adapters and their labels come from the
receiver's configuration record. `ready` is true when the receiver is live
and its advertisement offers the adapter. A live receiver with no ready
adapter lists each adapter with `ready=false`.

`--json` prints a JSON array, one object per receiver:

| Member | Content |
| --- | --- |
| `receiver_id`, `pool` | strings |
| `adapters` | `{name, agent, labels, ready}` |
| `capacity`, `in_flight` | integers from the latest advertisement |
| `heartbeat_age` | Go duration string, or `-` |
| `status` | `live`, `stale`, `silent` or `ended` |
| `ended` | boolean |

### `wherry submit`

Resolves a placement, signs the task and publishes it to the chosen pool. It
prints `run_id=<id> pool=<pool>`, or fails with `no matching live receiver`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--agent` | the pool's default, then the terms' order | agent to run |
| `--selector` | empty | [selector](#selectors) over adapter labels |
| `--prompt` | | prompt text |
| `--prompt-file` | | file holding the prompt |
| `--git REMOTE@REF` | | repository the agent clones: an `https` or `ssh` URL, or `user@host:path`, at a branch, tag or commit |
| `--git-path` | the repository name | checkout directory, relative to the agent's working directory |
| `--git-credential NAME` | | the `--credential-file` credential the agent clones with: a token for `https`, a private key for `ssh` |
| `--git-host-key FILE` | | file of OpenSSH `known_hosts` lines naming the remote's host; required for `ssh` |
| `--git-changes patch\|bundle` | | ask for the agent's commits back as the output `changes.patch` or `changes.bundle` |
| `--input FILE` | | file the agent gets in `WHERRY_INPUT_DIR` under its base name; repeatable |
| `--s3-input FILE` | | file of at most 5 GiB that `submit` uploads to the pool's bucket and the agent gets in `WHERRY_INPUT_DIR` under its base name; repeatable |
| `--credential-file NAME=FILE` | | credential `NAME` from `credentials` in the client configuration, which the agent gets as the file `FILE` in `WHERRY_CREDENTIALS_DIR`; repeatable |
| `--output NAME[:required][:s3]` | | file the agent is asked to leave in `WHERRY_OUTPUT_DIR`; `:required` fails the turn without it, and `:s3` returns it through the pool's bucket; repeatable |
| `--deadline` | `30m` | time from now by which the run must end |
| `--queue-deadline` | `10m` | time from now by which a receiver must claim it |

Exactly one of `--prompt` and `--prompt-file` is required. The other
`--git-` flags need `--git`, which splits at its last `@`. `submit` writes
a `user@host:path` remote as `ssh://user@host/path`. The
[extension](a2a-extension-v1.md#git-input) gives the rules for the remote,
ref, path and host key. `submit` refuses, before it publishes, a remote
whose host is not one of the pool's [`vcs`](#pool-terms) hosts:

```
invalid input: git remote host github.com is not in pool build's vcs hosts
```

`continue` refuses a run whose git input, as this host's record of the run
holds it, is no longer in the pool's VCS hosts, and a run with S3 inputs or
outputs whose pool no longer has storage. A host without that record,
another host of the tenant, does not check it, and admission rejects the
turn instead. `--git-host-key` reads at most one byte over
the 8 KiB bound.

`--git-changes` adds `changes.patch` or `changes.bundle` to the expected
outputs, unless `--output` names it already; neither is required.

Input files hold the pool's [`inline_limit`](#pool-terms) together; `submit`
refuses more before it publishes, naming the size and the limit. Input and
credential file names are [file names](a2a-extension-v1.md#submission), and
output names relative paths of them. `submit` refuses a credential its
configuration does not map to a file.

`--s3-input` and `:s3` use the pool's [S3 lane](a2a-extension-v1.md#s3-inputs-and-outputs).
`submit` refuses them, before it uploads or publishes anything, when the
client configuration has no [`storage`](#client-configuration) or the
pool's terms no [`storage`](#pool-terms):

```
invalid input: pool build has no storage for S3 inputs and outputs
```

It uploads each S3 input to `wherry/<tenant>/<run>/inputs/<name>` in the pool's
bucket with one PUT, then publishes the task with each input's name, size and
SHA-256 and no URL. S3 input names are file names, distinct from the input
files' names, and the run's parts and S3 inputs number at most 256 together. S3
inputs do not count against `inline_limit`. When publishing fails, `submit`
deletes what it uploaded. It also refuses a run whose task credentials and
presigned URLs would not fit one 64 KiB seal, naming the seal's size. A
presigned URL of about 420 bytes takes about 840 bytes of the seal, so one seal
carries about 75.

`submit` seals nothing. The receiver that claims the task asks for the
credential its adapter delivers, the task's credentials and presigned URLs
for its S3 inputs and outputs, and `wherry await` or `wherry mcp` answers
([sealed credentials](concepts.md#sealed-credentials)).

### `wherry continue RUN_ID`

Publishes the next turn of a run interrupted in `TASK_STATE_AUTH_REQUIRED`
or `TASK_STATE_INPUT_REQUIRED`, to the pool and placement its echo names.
Like `submit`, it seals nothing. It prints `run_id=<id> turn=<n>`. `resume`
is an alias.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--prompt` | `continue` | text of the new turn |
| `--prompt-file` | | file holding the new turn's prompt |
| `--deadline` | `30m` | run deadline for the turn |
| `--queue-deadline` | `10m` | queue deadline for the turn |

`--prompt` and `--prompt-file` are mutually exclusive. With neither, the
prompt is `continue`.

It fails when the run is not interrupted, is cancelled, or already has a
next turn.

### `wherry cancel RUN_ID`

Publishes a signed cancel of the run's latest turn to its pool's events
and prints `run_id=<id>`.

### `wherry await RUN_ID`

Waits for the latest turn of a run to finish or be interrupted. `--timeout`
sets the wait, `30m` by default. `--outputs-dir DIR` sets where the turn's
outputs go, by default `<state_dir>/runs/<run>/outputs/<turn>/`.

It prints the state on the first line. For a turn that ended with a result,
each following line is one part of a result artifact. For a turn that ended
with a status alone, each line is one part of the status message. A part
prints as its text, its JSON data, `[url <url>]`, or
`[raw <media type> <n> bytes]`. For `TASK_STATE_AUTH_REQUIRED` it prints
`continue with: wherry continue <id>` on standard error.

`await` writes each output that [verifies](a2a-extension-v1.md#inputs-and-outputs)
against the result, at its relative path beneath the outputs directory, and
prints a line for it:

```
output=<name> path=<path> sha256=<hex> size=<bytes>
```

For `changes.patch` it also prints `apply with: git am <path>`, and for
`changes.bundle` `fetch with: git fetch <path> HEAD`, on standard error.

It prints `output <name> not returned: <reason>` on standard error for each
output the result lists as not returned and each one that fails
verification. A write never leaves the outputs directory, whatever links
it holds.

`await` downloads each S3 output the result lists with the host's storage
credential, to the same place, and prints it as it prints any output. It
downloads only an S3 output the run declares, at its key for the turn, and
writes it only when its bytes match the listed size and SHA-256; a file
already there that matches is kept. Once the run's turn is terminal and
every listed S3 output has verified, `await` deletes every object under
`wherry/<tenant>/<run>/` in the pool's bucket, inputs included. An
interrupted turn keeps them for its continuation. A run that ends while
no `await` runs keeps them until `wherry cleanup` or the bucket's
lifecycle rule removes them.

`await` exits with status 1, `run ended in <state>`, for any state other
than `TASK_STATE_COMPLETED`, with status 1 when an output fails
verification, and with status 1 when the timeout passes.

While it waits, `await` answers the seal requests on the tenant feed for
every run this host submitted, once per attempt, and logs on standard error
each answer and each verified request of its own runs that it skips or
refuses, by run, receiver and credential names.

### `wherry cleanup RUN_ID`

Deletes every object under the run's prefix, `wherry/<tenant>/<run>/`, in
its pool's bucket with the host's storage credential, and prints
`run_id=<id>`. It deletes only objects whose keys a listing of that prefix
returns and that start with it, so the prefix of another run or tenant is
never touched. It finds the run as `status` does, and fails with `run not
found` otherwise. Use it for a run the client never saw finish.

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

`await`, `status`, `events`, `cleanup`, `continue` and `cancel` find a run from
this host's record of it in `state_dir`, or from a receiver's echo on the tenant
feed. They fail with `run not found` when there is neither.

### `wherry mcp`

Serves the [MCP tools](#mcp-tools) over stdio. For as long as it runs it
answers seal requests as `await` does, logging to standard error.
The packaged MCP launch uses `["mcp"]` and the default configuration path.
A server that runs `wherry mcp --config-file PATH` uses another; see the
[operator guide](operator-guide.md#harness-integration).

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

### `wherry admin pool create POOL <--config-file FILE|--config-dir DIR>`

Records the [pool terms](#pool-terms), creates the pool's tasks
and events topics, and sets the configuration of share group `p-<pool>`:
`share.auto.offset.reset=earliest`, `share.isolation.level=read_committed`,
`share.delivery.count.limit` and `share.record.lock.duration.ms` from the
terms. It refuses a delivery limit above the lowest
`group.share.max.delivery.count.limit` across the brokers, and terms whose
[records do not fit the broker](#record-limit). For an existing pool it
records the new terms and sets the group configuration again. On new and
existing topics it sets the tasks and events `retention.ms` to
`run_lifetime_ms` plus one hour plus one day, and their
`message.timestamp.type` to `LogAppendTime`. It refuses a run lifetime above
29 days and 23 hours, or one that with one hour added exceeds the
`retention.ms` of the feed of a tenant entitled to the pool. See
[Topics](#topics).

### `wherry admin tenant add TENANT --pool POOL`

Entitles the tenant to an existing pool, creates the tenant feed, and
grants the tenant's hosts and the pool's receivers their ACLs. It refuses
when the pool's records do not fit the [record limit](#record-limit) with
the tenant's feed counted, or when the feed's `retention.ms` is below the
pool's `run_lifetime_ms` plus one hour, and records nothing. It applies
each host's [quotas](#quotas).

### `wherry admin host add TENANT HOST --bundle-file FILE`

Records the host, creates its principal, its key topic and the tenant
feed, grants its ACLs, applies its quotas, and writes its bundle.

### `wherry admin receiver add RECEIVER`

```sh
wherry admin receiver add RECEIVER --pool POOL \
  <--config-file FILE|--config-dir DIR> --bundle-file FILE
```

Reads the [receiver configuration](#receiver-configuration) from a
directory or a single file, validates it against the pool's terms, records
it, creates the receiver's principal and key topic,
grants its ACLs, and writes its bundle. It refuses an existing receiver.

`--platform-credential-file ADAPTER=FILE` puts the contents of `FILE`, less
trailing CR and LF, in the bundle as the platform credential of the named
adapter.

### `wherry admin receiver configure RECEIVER`

```sh
wherry admin receiver configure RECEIVER <--config-file FILE|--config-dir DIR>
```

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
receiver configuration against its pool's terms, checks that every pool's
[records still fit the broker](#record-limit), checks topic retention and
timestamp type against the [bounds](#topics), and checks that every ACL the
state calls for is present. It does not report ACLs beyond those, and does not check an
adapter's `type` or `config` section.

## Receiver command

### `wherry receiver --bundle-file FILE --state-dir DIR --plugin-dir DIR`

Runs a receiver as the bundle's principal. It creates the state directory
with mode 0700 and keeps `signing.key` in it. On start it:

1. deletes `sealing.key` when the directory holds one, registers its public key
   on its key topic when the topic is empty, and fails when the topic holds
   another key;
2. waits for its configuration record on `wherry.admin` and validates it
   against the pool's terms;
3. starts the [adapter plugins](#adapter-plugins) its adapters' types name
   from the plugin directory and configures each adapter, reads its pool's
   events once, and starts advertising and claiming.

It logs to standard output with the prefix `receiver: `. It exits on
`SIGINT` or `SIGTERM` without stopping its jobs, and fails with
`receiver configuration changed` when a new configuration record appears.
It refuses to start after its membership end.

With `--metrics-addr HOST:PORT` it serves its [metrics](#metrics) at
`/metrics` on that address, and fails at start when it cannot listen there.
Without the flag it serves none.

### Adapter plugins

The receiver ships with no adapters. For each adapter type in its
configuration it runs `wherry-plugin-<type>` from the plugin directory, one
process per type, and configures every adapter of that type in it.
[wherry-plugins](https://github.com/benemon/wherry-plugins) holds the
plugins, `nomad` among them.

The plugin directory is made absolute, so the binary checked is the one run and
`PATH` is never searched. The receiver refuses a plugin directory or binary
owned by a user other than root or the receiver's, or that group or others can
write, and a binary that is not a regular file; a symbolic link is refused. It
checks again before every restart. A type with no binary in the directory is a
configuration error, and the receiver exits with
`adapter <name>: no plugin for type <type>`. A plugin checks its adapter's
`config` section when the receiver configures it, and the receiver exits with
`adapter <name>: ...` on an error.

A plugin inherits the receiver's environment. The receiver passes each
adapter its name, pool, agent, credential name and `config` section, and
the bundle's `platform` entry for its name when there is one.

A plugin also reports, for each adapter, the paths of the input, output and
credentials directories as its agent sees them, which the receiver writes
into the prompt. Dispatch carries a turn's input files and task credentials
with the prompt, and a fetch for each S3 input: its name, presigned URL,
size and SHA-256, which the plugin fetches and verifies before the agent
starts, reporting a job that never started with class `fetch` when it
cannot. A plugin that cannot deliver an input reports it, and the turn ends
`TASK_STATE_FAILED` naming it. After the agent's result line the receiver
stops the job and asks the plugin to collect its outputs, which arrive over
the link in pieces of at most 1 MiB. The outputs the receiver names for
streaming, its S3 outputs, the plugin streams as the receiver reads them.

A plugin gives each agent the variables of `adapter.AgentEnvironment` over
the environment it configures, and removes those of `adapter.AgentUnset`
from that environment, so the platform's `git` configuration does not reach
a [git input](a2a-extension-v1.md#git-input)'s clone. It cannot remove
what the platform or the image sets. This protects a cooperative agent,
not against one that runs commands of its own
([operator guide](operator-guide.md#git-repositories)).

The receiver and a plugin talk gRPC over
[go-plugin](https://github.com/hashicorp/go-plugin) with mutual TLS on a
certificate pair made for each start, protocol version 4. The receiver refuses a
plugin of any other protocol version at its start, so the receiver and its
plugins upgrade together. Version 4 adds `Metrics`; the receiver refuses a
version 3 plugin with
`incompatible API version with plugin. Plugin version: 3, Client versions: [4]`.
The receiver refuses a plugin that offers no TLS certificate. Every call names
its adapter. A plugin accepts messages up to 64 MiB; the receiver logs a larger
Dispatch as `dispatch rejected: message too large`.

Each advertisement gives every adapter's sweep and readiness call one
advertise interval, 30 seconds; a call that runs out counts as not ready
and is logged.

Each scrape of `/metrics` calls every adapter's `Metrics` at once, each within
2 seconds. A plugin answers with samples of the `wherry_adapter_` families,
each a family name, labels, and a counter or gauge value or a histogram. A
histogram gives the cumulative count at each bound of
`adapter.PlacementBuckets`, its sum and its count. The receiver drops a
sample, and counts it in `wherry_metrics_dropped_total` by reason, when:

- its family is not one of the four (`unknown_family`);
- it has a label key the family lacks (`unknown_label`);
- a label value is outside the family's set (`label_value`);
- its kind is not the family's (`kind`);
- its value is not finite, a counter is negative, or a histogram has other
  buckets (`value`);
- it repeats a series of the same answer (`duplicate`).

It sets every sample's `adapter` label to the name of the adapter that
answered, so a plugin cannot report for another adapter. A call that fails
or runs out is counted as `error` or `timeout`, and the scrape serves that
adapter's samples from its last answer.

When a plugin exits, the receiver logs `plugin exited; restarting`, starts it
again after one second, waiting twice as long after each failed start up to 30
seconds, and configures every adapter of its type again. Until then those
adapters are not ready and the next advertisement withdraws their offers. A run
whose job the receiver is following reads again from its last offset once the
plugin is back; when the plugin stays down for one lock duration, the run fails
with `log stream failed`.

A receiver with no ready adapter advertises with no offers, so it stays
`live` with every adapter `ready=false`, and placement passes over it. A
job its adapter reports never started is a `platform` start failure: the
receiver purges the job and returns the task to the pool.

A receiver adopts a turn's existing job only when the adapter and agent
recorded with it match the adapter that finds it. It leaves a job whose
adapter is another of its adapters to that adapter, and stops and purges
any other job under the turn's ID and dispatches the turn again.

The protocol is the Go module `github.com/benemon/wherry/adapter` in this
repository. [`adapter.proto`](../adapter/proto/wherry/adapter/v1/adapter.proto)
defines the service, `adapter.Serve` runs a plugin, and `adaptertest` is
the conformance suite a plugin runs against its platform.

### Metrics

A receiver started with `--metrics-addr` serves its metrics in the Prometheus
text format over plain HTTP. `/metrics` is the only path; every other path
answers 404. The scrape holds the Go runtime (`go_`) and
process (`process_`) families, `wherry_build_info` with `version` and
`revision` and value 1, and the families below, each with the label `pool`.

Every label value comes from the receiver's configuration, its pool's terms and
entitlements, or a fixed list. No label or value carries a run ID, prompt,
output, error text, URL, image, path, node or credential name. An agent the
pool's terms do not name is `other`. A turn ended without an adapter has
`adapter="none"`: one given back at the delivery budget before any adapter
took it, and an interrupted turn a member ends on its cancel or expiry, even
after it ran.

| Family | Type | Labels | Counts |
| --- | --- | --- | --- |
| `wherry_claims_total` | counter | `tenant` | claimed tasks whose submission verifies |
| `wherry_admissions_total` | counter | `tenant`, `adapter` | tasks admitted, by the adapter they were routed to |
| `wherry_rejections_total` | counter | `tenant`, `reason` | rejections, by reason |
| `wherry_runs_total` | counter | `tenant`, `adapter`, `agent`, `state` | turns ended `completed`, `failed` or `canceled` |
| `wherry_turn_duration_seconds` | histogram | `tenant`, `adapter`, `agent`, `state` | time from this attempt's claim to the turn's terminal state; an adopted turn counts the adopting attempt only |
| `wherry_queue_wait_seconds` | histogram | `tenant`, `adapter` | time from submission to first claim |
| `wherry_adoptions_total` | counter | `tenant`, `adapter` | turns that adopted an earlier attempt's job |
| `wherry_continuations_total` | counter | `tenant`, `adapter` | continuation turns admitted |
| `wherry_redeliveries_total` | counter | `tenant`, `adapter` | turns run on a redelivery |
| `wherry_in_flight` | gauge | `adapter` | tasks held |
| `wherry_capacity` | gauge | none | the configured capacity |
| `wherry_start_failures_total` | counter | `adapter`, `class` | start failures: `offer-unavailable`, `input-too-large`, `dispatch`, `platform` |
| `wherry_dispatch_outcomes_total` | counter | `adapter`, `outcome` | dispatches: `created`, `nothing`, `unknown` |
| `wherry_stop_failures_total` | counter | `adapter` | failed `Stop` calls |
| `wherry_purge_failures_total` | counter | `adapter` | failed `Purge` calls |
| `wherry_sweep_failures_total` | counter | `adapter` | failed `Sweep` calls |
| `wherry_collection_failures_total` | counter | `adapter`, `reason` | turns whose outputs were not collected: `not_stopped`, `over_limits`, `timeout`, `failed` |
| `wherry_plugin_restarts_total` | counter | `type` | plugin restarts |
| `wherry_seal_waits_total` | counter | `tenant`, `outcome` | seal waits: `sealed`, `expired`, `auth_required`, `adopter_refused` |
| `wherry_seal_wait_seconds` | histogram | `tenant` | time a turn waited for its seal, whatever the outcome |
| `wherry_redactions_total` | counter | `tenant`, `adapter` | credential values replaced in published output |
| `wherry_bus_errors_total` | counter | `op` | Kafka errors: `read`, `write`, `transactional ID`, `share group` |
| `wherry_denials_total` | counter | `op` | Kafka authorisation errors, by the capability denied: `read`, `write`, `transactional ID`, `share group`, `membership` |
| `wherry_renewal_failures_total` | counter | none | failed claim renewals |
| `wherry_transaction_aborts_total` | counter | none | transactions aborted after a failed produce |
| `wherry_ambiguous_commits_total` | counter | none | commits with an unknown outcome, read back from the pool's events |
| `wherry_fenced_total` | counter | none | runs given up to a newer holder |
| `wherry_agent_reported_tokens_total` | counter | `tenant`, `agent`, `kind` | tokens an agent's result reports: `input`, `output`, `cached`, `reasoning` |
| `wherry_agent_reported_cost_usd_total` | counter | `tenant`, `agent` | cost in US dollars an agent's result reports |
| `wherry_metrics_dropped_total` | counter | `adapter`, `reason` | adapter samples dropped and `Metrics` calls that failed |
| `wherry_adapter_platform_errors_total` | counter | `adapter`, `class` | platform API errors: `unavailable`, `denied`, `conflict`, `timeout`, `other` |
| `wherry_adapter_cleanup_pending` | gauge | `adapter` | ended jobs awaiting cleanup |
| `wherry_adapter_sweep_deletions_total` | counter | `adapter`, `kind` | material a sweep deleted: `job`, `variable` |
| `wherry_adapter_placement_seconds` | histogram | `adapter` | time from dispatch to the agent task starting |

A rejection's `reason` is its fixed text, such as `run expired`. A reason that
quotes the task counts as `invalid inputs`,
`git remote host outside the pool's vcs hosts` or `pool has no storage`.

A seal wait is `expired` when the turn's deadline passes,
`auth_required` when the wait ends and the turn is interrupted, and
`adopter_refused` when an adopter's wait ends and it stops the job first.

Agent-reported usage comes from a turn's result: Claude Code's `usage` fields
`input_tokens`, `output_tokens` and `cache_read_input_tokens`, and its
`total_cost_usd`; Codex's `input_tokens`, `output_tokens`,
`cached_input_tokens` and `reasoning_output_tokens`. An absent field adds
nothing. The figures are the agent's own, unchecked.

The core histograms' buckets are 1, 5, 15, 30 and 60 seconds, 2, 5, 10 and 30
minutes, and 1, 2 and 4 hours. `wherry_adapter_placement_seconds` uses
`adapter.PlacementBuckets`: 1, 2, 5, 10 and 30 seconds, and 1, 2, 5, 10 and 30
minutes.

## MCP tools

`wherry mcp` serves seven tools over stdio. Each takes and returns JSON with
a declared schema and opens its own Kafka connection. Deadlines given as `0`
or omitted take the CLI defaults, 1800 and 600 seconds.

| Tool | Input | Output |
| --- | --- | --- |
| `receivers` | none | `receivers`, the objects `wherry receivers --json` prints |
| `submit` | `prompt`, `agent`, `selector`, `git`, `inputs`, `s3_inputs`, `credentials`, `outputs`, `deadline_seconds`, `queue_deadline_seconds` | `id`, `pool` |
| `status` | `id`, optional | `runs`, `skipped` |
| `await` | `id` | `id`, `state`, `retry_after`, `message`, `artifacts`, `outputs`, `not_returned`, `redacted`, `skipped` |
| `continue` | `id`, `prompt`, `deadline_seconds`, `queue_deadline_seconds` | `id`, `turn`, `skipped` |
| `cancel` | `id` | `id`, `skipped` |
| `events` | `id` | `id`, `events`, `transcripts`, `skipped` |

- `submit` requires `prompt`. `selector` is an array of
  `{key, operator, values}`. `git` is
  `{remote, ref, path, credential, host_key, changes}`, as `--git`,
  `--git-path`, `--git-credential`, `--git-host-key` and `--git-changes` take
  them, with `remote` and `ref` required; `host_key` is a file path, read as an
  input is. `inputs` and `s3_inputs` are arrays of file paths, `credentials` an
  array of `{name, file}` and `outputs` an array of `{name, required, storage}`,
  as `--input`, `--s3-input`, `--credential-file` and `--output` take them;
  `storage` `s3` is `:s3`. An input or S3 input must resolve, through any
  symbolic links, to a regular file beneath the server's working directory, and
  to none of `state_dir`, `host_key`, `bundle`, a credential file or a storage
  key file; a relative path is taken from that directory.
- `status` returns the latest verified state of a run's latest turn
  without waiting. `runs` holds one entry for the run `id` names, or,
  without `id`, one per run `wherry status` lists, latest update first.
  Each entry is `{id, pool, turn, state, message, updated, redacted}`. The
  state is `TASK_STATE_SUBMITTED` until a receiver reports one. `updated` is
  the RFC 3339 time of the latest status. While the run has none, it is this
  host's submission of the turn, else the modification time of this host's
  record of the run when the record predates submission times, else the
  Kafka timestamp of the turn's echo.
- `await` waits up to 45 seconds. It returns the outcome with `message` and
  `artifacts`, or, while the run is unfinished, its current `state` with
  `retry_after` set to 5. With a progress token it sends a progress notification
  every 5 seconds. Each artifact is `{artifact_id, parts}`, and each part has
  one of `text`, `data`, `url` or `raw_bytes` (the size of a binary part, which
  is not included), with `media_type`. `await` writes the turn's verified
  outputs as `wherry await` does, to the default outputs directory, and returns
  each in `outputs` as `{name, path, sha256, size}`, S3 outputs included, which
  it downloads and verifies, then deletes the run's prefix, as `wherry await`
  does. `not_returned` holds `{name, size, reason}` for each output the result
  lists as not returned and each one that fails verification.
- `status` and `await` show each redaction marker as `wherry status` and
  `wherry await` print it, and `redacted` maps each name to the number of
  its markers in `message` and `artifacts`.
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
| `RUN_EXPIRED` | the run is past its pool's `run_lifetime_ms`, or the queue deadline falls after it |
| `OUTPUTS_NOT_WRITTEN` | the turn's verified outputs could not be written to the state directory, or its S3 outputs downloaded, or the run's prefix deleted |
| `CANCELLED` | the request was cancelled |
| `UNAVAILABLE` | any other failure; the CLI reports the cause |

## Client configuration

A JSON object at `~/.config/wherry/config.json`, or at the host command's
`--config-file` path. Unknown and duplicate members are errors. A leading
`~/` is expanded in the flag and in every path.

| Member | Default | Content |
| --- | --- | --- |
| `bundle` | required | the host's bundle |
| `host_key` | `~/.config/wherry/host.key` | host signing key file |
| `state_dir` | `~/.config/wherry/state` | run records and answered seal requests |
| `credentials` | none | credential name to the file holding its value; a trailing newline is part of the value the agent gets, and the receiver redacts the value with and without it |
| `storage` | none | `{access_key_file, secret_key_file}`: files holding the access key and secret key of the host's S3 credential, each readable and writable by its owner alone; a host without it uses no S3 lane |

`state_dir` holds `runs/<run>.json` for each run this host submitted,
readable by its owner alone, with `redaction_key`, the run's redaction key,
made when this host first records a turn of the run,
`runs/<run>/outputs/<turn>/` for the outputs `await` writes by default, and
`answered/<run>.<turn>.<attempt>` for each attempt it answered, with a
`.lock` beside it that every process sharing the directory holds while it
answers that attempt. `submit` and `continue` write the run's record before
they publish the turn, and put the previous record back when publishing
fails. The
credential file is read at each answer, and its contents are sealed as
they are. A seal is at most 64 KiB, which bounds the values one seal
carries.

The storage key files are read whenever the host uploads, presigns,
downloads or deletes, with surrounding whitespace removed; the client
refuses a file its group or others can read or write. The credential
never leaves the host: receivers and agents get only the URLs it presigns.
Scope it to `wherry/<tenant>/` in each pool's bucket
([operator guide](operator-guide.md#s3-storage)).

```json
{
  "bundle": "~/.config/wherry/host.yaml",
  "storage": {
    "access_key_file": "~/.config/wherry/s3-access-key",
    "secret_key_file": "~/.config/wherry/s3-secret-key"
  }
}
```

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

The YAML file `pool create --config-file` reads. Unknown fields are errors.
With `--config-dir`, `pool.yaml` holds every field except `agents`, and
`agents/*.yaml` each hold one agent in file-name order. Hidden entries are
ignored. Any other entry and a directory with no agent files are errors.

| Field | Content |
| --- | --- |
| `agents` | one or more `{name, credentials}`: an agent and the credential names it may take |
| `default_agent` | an agent from `agents`, used when a task names none |
| `seal_lifetime_ms` | required and unused: seals expire with their [seal request](a2a-extension-v1.md#seal-request) |
| `run_lifetime_ms` | optional, default 1209600000 (14 days): time from a run's first submission through its last continuation |
| `delivery_limit` | the share group's `share.delivery.count.limit` |
| `lock_duration_ms` | the share group's `share.record.lock.duration.ms` |
| `producer_byte_rate` | host quota, bytes per second |
| `request_percentage` | host quota, percentage of broker request time |
| `inline_limit` | optional, default 262144 (256 KiB): bytes of a task's input files together |
| `output_limit` | optional, default 8388608 (8 MiB): bytes of one output, and of a turn's outputs together |
| `vcs` | optional, default none: the hosts a [git input](a2a-extension-v1.md#git-input) may clone from |
| `storage` | optional, default none: `{endpoint, bucket}`, the S3-compatible store of the pool's [S3 inputs and outputs](a2a-extension-v1.md#s3-inputs-and-outputs) |

Each entry of `vcs` is a lowercase host name, with `:port` for a
remote that names a port other than its scheme's default, 443 for `https`
and 22 for `ssh`, and appears once. `git.example.com` matches
`https://git.example.com/...` and `ssh://git@git.example.com/...`;
`git.example.com:2222` matches `ssh://git@git.example.com:2222/...`. A pool
without `vcs` accepts no git input.

`storage.endpoint` is `https://` and a lowercase host name, with `:port`
for a port other than 443, and nothing else: no path, user information or
query. wherry speaks only https to it. `storage.bucket` is 3 to 63
lowercase letters, digits, dots and hyphens, starting and ending with a
letter or digit. The run's objects live under `wherry/<tenant>/<run>/` in
the bucket, addressed path-style, `https://<host>/<bucket>/<key>`. A pool
without `storage` accepts no S3 input or output.

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
    credentials:
      - model-api-key
default_agent: coder
seal_lifetime_ms: 86400000
delivery_limit: 5
lock_duration_ms: 15000
producer_byte_rate: 1048576
request_percentage: 10
vcs:
  - github.com
storage:
  endpoint: https://s3.example.com
  bucket: wherry
```

### Record limit

A record limit is the lowest of `message.max.bytes` across the brokers,
1000012 bytes, the most a wherry producer batches, and `max.message.bytes`
on the topic the record goes to: the pool's tasks topic for submissions,
and the feeds of the tenants entitled to the pool for chunks.
`pool create`, `tenant add` and `check` sign four records and measure
them as record batches: the largest bounded submission, the largest output
chunk, of 256 KiB, the largest result, and the largest transcript chunk, a
line of 512 KiB. The largest result has the turn's message with its 256
input files named but not carried, 256 returned and 256 unreturned outputs
with 512-byte names, a status text and a result text of 64 KiB each, and
the agent's result record at its 16 KiB bound; a receiver's labels and an
adapter's job name have no bound and are left empty.
Any one over its record limit refuses the terms, naming its size and the
limit, and an `inline_limit` above the tasks topic's record limit is
refused before it is measured.

The largest bounded submission has every field that has a bound at its maximum:
256 parts, each an input file with a 255-byte name, one of them holding
`inline_limit` bytes, and a git input with a 2048-byte remote, a 255-byte ref, a
512-byte path and an 8 KiB host key of quotes, which JSON escapes, naming the
one task credential it declares. Only a first turn carries a git input, so the
submission is turn 1. The prompt's text, part metadata and media types, the
selector, the other task credentials and the outputs have no bound and are left
empty. A task with a large prompt can still exceed the record limit; `submit`
then fails naming the record's size.

An input file travels base64-encoded inside the signed record, which is itself
base64url-encoded, so a submission takes about 16/9 of `inline_limit` plus about
123 KB. As `pool create` measures them, the submission at the default
`inline_limit` is 589210 bytes, the output chunk 468155 bytes, the result 791053
bytes and the transcript chunk 933544 bytes, so a feed's `max.message.bytes`
below 933544 refuses every pool; with brokers and topics at their defaults, the
highest `inline_limit` that fits is 493221 bytes, about 481 KiB.

`pool create` and `tenant add` check against the broker at the time.
Receivers apply the limits from their view of `wherry.admin`, which they
read every 15 seconds, so a changed limit takes effect within that
window.

### Quotas

Each host principal gets the Kafka client quotas `producer_byte_rate` and
`request_percentage`, the lowest of each across the pools its tenant is
entitled to.

## Receiver configuration

What `receiver add` and `receiver configure` read, recorded on
`wherry.admin`: a [directory](#receiver-configuration-directory) named by
`--config-dir`, or a single YAML file with these fields named by
`--config-file`. Exactly one is required. Unknown fields are errors.

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
| `type` | the adapter type; the receiver runs `wherry-plugin-<type>` for it |
| `agent` | the agent it launches, from the pool's terms |
| `output` | optional, default `claude`: `claude` for Claude Code stream-json, `codex` for `codex exec --json`, or `file` for a reply file |
| `reply_file` | optional, only with `output: file`, default `reply.md`: the output name of the reply file |
| `credential` | optional; a credential the terms allow for the agent, delivered to the job |
| `labels` | optional; [labels](#labels) for placement |
| `config` | the adapter type's settings |

### Receiver configuration directory

The directory holds:

```
receiver.yaml
platforms/
  <platform>.yaml
adapters/
  <adapter>.yaml
```

| File | Fields |
| --- | --- |
| `receiver.yaml` | `capacity` |
| `platforms/*.yaml` | one platform: `name`, unique in the directory; `type`; `config`, settings shared by its adapters |
| `adapters/*.yaml` | one adapter: `name`, `platform`, `agent`, `output`, `reply_file`, `credential`, `labels` and `config`, as in an adapter entry |

The receiver and pool come from the command. Each adapter file becomes one
adapter entry:

- Routing order is the lexical order of the file names in `adapters/`.
- `platform` names a platform in `platforms/`, and the entry takes that
  platform's `type`.
- The entry's `config` is the platform's `config` merged with the
  adapter's at the top level. A top-level key set in both is an error that
  names the adapter file and the key. Nested values do not merge.

Unknown fields, an unknown platform, a repeated adapter or platform name,
an empty `adapters/`, a nested directory, and any file other than
`receiver.yaml` and `.yaml` files in `platforms/` and `adapters/` are
errors. Files and directories whose names start with `.` are ignored. The
command writes nothing when the directory has an error.

### Adapter settings

An adapter's `config` section belongs to its plugin. The
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
lists its settings and the jobs it runs.

### Agent contract

The agent receives the prompt in a file and the credential, when its
adapter delivers one, as its plugin documents; for Nomad, in
[the plugin's agent jobs](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-jobs).
`WHERRY_INPUT_DIR`, `WHERRY_CREDENTIALS_DIR` and `WHERRY_OUTPUT_DIR` hold
the paths of its [input, credential and output
directories](a2a-extension-v1.md#inputs-and-outputs).

The agent writes JSON lines to standard output. The receiver publishes each
line as a transcript chunk. With the adapter's `output` absent or `claude`,
the line that decodes as a JSON object with `"type": "result"` ends the
turn: its `result` string becomes the result, and `"is_error": true` makes
the state `TASK_STATE_FAILED`. Claude Code's
[headless](https://docs.claude.com/en/docs/claude-code/headless) stream-json
output writes this format.

With `output: codex`, the format `codex exec --json` writes, the receiver
keeps the text of the last `item.completed` line whose `item.type` is
`agent_message`. A `turn.completed` line ends the turn with that text as the
result and its `usage` as metadata. A `turn.failed` line ends it as
`TASK_STATE_FAILED` with its `error.message`, or `error` itself when it is
a string, as the result. `error` lines and `error` items do not end the
turn.

With `output: file`, no line ends the turn; it ends when the job does. The
receiver collects the outputs and takes the output `reply_file` names as
the result, redacted like any result. The reply is not returned as an output
and counts against no output limit; a reply over 64 KiB becomes
`[redacted text over 65536 bytes]`. The `reply_file` name is reserved on
that adapter: an expected output of that name is listed as not returned with
the reason `is the adapter's reply file`. A job that exits 0 with a reply
completes with it, and one that exits 0 without a reply fails with the
message `agent wrote no reply file`, or `outputs not collected` when the
receiver could not collect the outputs. An empty reply completes the turn
with an empty result; `wherry await` then prints the state and an empty
line. A job that exits non-zero or by a signal fails with its reply as the
result, or without one with `agent exited <code>` or
`agent ended by <signal>`. The result carries no `agent_result` metadata.

The receiver stops the job once it reads the line that ends the turn. A job
that ends without one fails with the message `agent ended without a result`.

A turn's run time is capped at its deadline, and at one hour after its
queue deadline. A receiver rejects a turn no pool member has claimed with
`queue deadline passed` or `run deadline passed` once that deadline has
passed. A redelivered turn that a member has claimed is not rejected: the
receiver adopts its job when it finds one, and dispatches the turn again when
it finds none. Past its run deadline, the receiver stops any job it finds and
ends the turn `TASK_STATE_FAILED` with `timeout`.

A receiver measures a run's lifetime from the broker's append time of its
first turn. It rejects a turn no pool member has claimed with `run expired`
when that time is more than `run_lifetime_ms` ago, and with
`queue deadline exceeds run lifetime` when the turn's queue deadline falls
more than `run_lifetime_ms` after it. A redelivered turn that a member has
claimed skips both checks. `wherry submit`, `wherry continue` and the MCP
tools make the same checks before publishing. Every turn therefore ends
within one hour after the run lifetime. A receiver that next reads an
interrupted run after that time ends it `TASK_STATE_CANCELED` with
`run expired`.

## Kafka resources

### Topics

`wherry admin` creates each topic with one partition, the broker's default
replication factor, `cleanup.policy=delete` and
`message.timestamp.type=LogAppendTime`.

| Topic | Created by | `retention.ms` |
| --- | --- | --- |
| `wherry.admin` | any recording command | `-1` |
| `wherry.receivers` | any recording command | `604800000` (7 days) |
| `wherry.pool.<pool>.tasks` | `pool create` | `run_lifetime_ms + 3600000 + 86400000`; default `1299600000` |
| `wherry.pool.<pool>.events` | `pool create` | at least tasks; `pool create` sets the same value |
| `wherry.tenant.<tenant>` | `tenant add`, `host add` | `2592000000` (30 days) |
| `wherry.receiver.<receiver>` | `receiver add` | `-1` |
| `wherry.host.<tenant>.<host>` | `host add` | `-1` |

The tasks retention keeps the run lifetime, the one-hour maximum turn time,
and a one-day margin. Events stay at least as long as tasks. A tenant feed
must stay for at least `run_lifetime_ms + 3600000`; its 30-day default exceeds
the default requirement of `1213200000`. Identity and configuration topics
stay at `-1`, and `wherry.receivers` remains a seven-day liveness topic.

`wherry admin` leaves an existing topic's configuration as it is, except
that `pool create` sets the retention and timestamp type of the pool's tasks
and events topics. A tenant feed's retention can be changed with Kafka's own
tools. `pool create` and `tenant add` refuse terms that need more than an
entitled tenant's feed keeps, and `wherry admin check` reports a topic below
its bound and a pool topic whose timestamp type is not `LogAppendTime`.

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
| `SSL_CERT_FILE` | host commands | PEM CA file trusted beside the system roots for a pool's S3 endpoint |
| `WHERRY_TRUST_BROKER` | `wherry admin` | Kafka bootstrap address, also written into bundles |
| `WHERRY_TRUST_CA` | `wherry admin` | PEM CA file for the broker, also written into bundles |
| `WHERRY_TRUST_ADMIN_USER` | `wherry admin` | super user name |
| `WHERRY_TRUST_ADMIN_PASSWORD_FILE` | `wherry admin` | file holding the super user's SCRAM password |
| `WHERRY_TRUST_OPERATOR_USER` | `wherry admin` | operator principal name; must be `wherry.operator` |
| `WHERRY_TRUST_OPERATOR_PASSWORD_FILE` | `wherry admin` | file holding the operator's SCRAM password |

A plugin reads its own variables from the receiver's environment; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#credentials)
lists its.

Trailing CR and LF are trimmed from password files.
