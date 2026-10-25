# Commands

[Key concepts](concepts.md) explains the model. The
[wherry A2A extension](a2a-extension-v1.md) specifies the records.
[Configuration](configuration.md) lists the files the commands read, and
[MCP tools](mcp.md) the tools `wherry mcp` serves.

## Conventions

- `wherry admin` and `wherry receiver run` read no client configuration. Every
  other command reads it first, and every command except `key` and `mcp`
  then connects to Kafka. `key` connects only to settle a
  [leftover next key](#wherry-key-rotate).
- Every host command takes `--config-file FILE`. It defaults to
  `~/.config/wherry/config.yaml`. A leading `~/` is expanded.
- A command's flags may come before or after its `RUN_ID`.
- A failed command prints `wherry: <error>` on standard error and exits
  with status 1.
- Durations use Go syntax, such as `90s`, `45m` or `720h`, and must be
  positive; the `--wait` of `host revoke` and `receiver revoke` takes the
  broker-derived wait when zero or less.
- `cancel`, `await`, `status`, `events` and `transcript` print
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
when the key topic holds another key.

### `wherry key`

Prints the host key's key ID. The key file must be a regular file that
grants no group or other access.

### `wherry key rotate`

Replaces the host key with a new one:

1. It refuses while a run this host submitted has a latest state that is
   not terminal, as `wherry status` shows it, and names those runs. A
   receiver admits a task only under its host's current key.
2. It writes the new key to `<host_key>.next` with mode 0600 and syncs the
   file and its directory.
3. It publishes a `rotate` [key record](a2a-extension-v1.md#key-records),
   signed by the old and new keys, to the host's key topic and waits for the
   broker's acknowledgement.
4. It folds the key topic again. When the topic's current key is the new
   key, it renames `<host_key>.next` over `host_key`, which removes the old
   private key, and syncs the directory. It then prints
   `old_key_id=<id> new_key_id=<id>`.

Stop every other wherry process of the host first, such as the MCP server
and any `wherry await`; they hold the old key.

Every host command that finds `<host_key>.next` folds the host's key topic
before anything else. When the current key is the next key's, it renames
`<host_key>.next` over `host_key`; when it is still the old key, it removes
`<host_key>.next`. It fails when the current key is neither.

### `wherry receivers`

Lists the receivers of every pool the tenant is entitled to, sorted by pool
and receiver ID:

```
receiver=<id> pool=<pool> capacity=<n> in_flight=<n> heartbeat_age=<age> status=<status>
  adapter=<name> agent=<agent> ready=<bool> labels=<k=v,...> [inline_limit=<n>] [models=<m,...> default_model=<m>] [credentials=<name[(missing,optional)],...>]
```

`status` is `live` when the latest advertisement is dated within 120
seconds of now, either side, `stale` otherwise, `silent` when there is none,
and `ended` after the receiver's membership end. Adapters and their labels come from the
receiver's configuration record, and an agent's `models` and
`default_model` from the pool's [terms](configuration.md#pool-terms) when it has them.
`ready` is true when the receiver is live
and its advertisement offers the adapter. A live receiver with no ready
adapter lists each adapter with `ready=false`. `inline_limit`, when set, is
the most inline input bytes the offered adapter delivers. `credentials` names
the agent's credentials in the pool's terms, marking `missing` each this
host's configuration has no [source](configuration.md#client-configuration) for and
`optional` each a run goes without, as in
`credentials=openai-api-key,github-token(missing,optional)`. It shows no
target.

`--json` prints a JSON array, one object per receiver:

| Member | Content |
| --- | --- |
| `receiver_id`, `pool` | strings |
| `adapters` | `{name, agent, inline_limit, models, default_model, credentials, labels, ready}`, `credentials` holding `{name, optional, configured}`; optional values are omitted when unset |
| `capacity`, `in_flight` | integers from the latest advertisement |
| `heartbeat_age` | Go duration string, or `-` |
| `status` | `live`, `stale`, `silent` or `ended` |
| `ended` | boolean |

### `wherry submit`

Resolves a placement, signs the task and publishes it to the chosen pool. It
prints `run_id=<id> pool=<pool>`, or fails with `no matching live receiver`.

Before it publishes, it folds the host's key topic and refuses when the host
key is not the topic's current key, since receivers admit a task only under
that key. When `<host_key>.next` exists, the next host command completes the
interrupted `wherry key rotate`; otherwise the host was re-keyed elsewhere
and needs `wherry init` on a re-provisioned host.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--agent` | the pool's default, then the terms' order | agent to run |
| `--model` | the agent's `default_model` | model the agent runs, one of its `models` in the pool's terms |
| `--selector` | empty | [selector](#selectors) over adapter labels |
| `--prompt` | | prompt text |
| `--prompt-file` | | file holding the prompt |
| `--git REMOTE@REF` | | repository the agent clones: an `https` or `ssh` URL, or `user@host:path`, at a branch, tag or commit |
| `--git-path` | the repository name | checkout directory, relative to the agent's working directory |
| `--git-changes patch\|bundle` | | ask for the agent's commits back as the output `changes.patch` or `changes.bundle` |
| `--input FILE` | | file the agent gets in `WHERRY_INPUT_DIR` under its base name; repeatable |
| `--s3-input FILE` | | file of at most 5 GiB that `submit` uploads to the pool's bucket and the agent gets in `WHERRY_INPUT_DIR` under its base name; repeatable |
| `--credential NAME:file=FILE` or `--credential NAME:env=VAR` | | credential `NAME` from `credentials` in the client configuration, which the agent gets as the file `FILE` in `WHERRY_CREDENTIALS_DIR` or in the environment variable `VAR`; repeatable |
| `--output NAME[:required][:s3]` | | file the agent is asked to leave in `WHERRY_OUTPUT_DIR`; `:required` fails the run without it, and `:s3` returns it through the pool's bucket, in either order; repeatable |
| `--deadline` | `2h` | time from now by which the run must end |
| `--queue-deadline` | `10m` | time from now by which a receiver must claim it |

`submit` refuses, before it publishes, a model outside the agent's
`models`, including any model for an agent with none:

```
invalid input: model opus is not in agent claude's models in pool build
```

Without `--agent`, `submit` considers only the agents whose `models` hold
the model, and refuses a model no agent of the pool holds. wherry does not
interpret a model name; the adapter passes it to the agent as
`WHERRY_MODEL`.

Exactly one of `--prompt` and `--prompt-file` is required, and an empty
prompt file fails with `--prompt-file is empty`. The other `--git-` flags
need `--git`, which splits at its last `@`. `submit` writes a
`user@host:path` remote as `ssh://user@host/path`. The
[extension](a2a-extension-v1.md#git-input) gives the rules for the remote,
ref, path and host key. `submit` refuses, before it publishes, a remote
whose host is not one of the pool's [`vcs`](configuration.md#pool-terms) hosts:

```
invalid input: git remote host github.com is not in pool build's vcs hosts
```

The clone's credential and host key come from the client configuration's
[`git`](configuration.md#client-configuration) entry whose `url` is the longest prefix of the
remote. The entry's credential joins the task as a task credential the agent
gets as the file `git-credential` in `WHERRY_CREDENTIALS_DIR`, so no
`--credential` may name it as well, and `submit` reads at most one byte over
the 8 KiB bound of its host key file. A remote under no entry clones without a
credential, and an `ssh` remote under no entry with a host key is refused.
With a host key, no input may be named `known_hosts`.

`--git-changes` adds `changes.patch` or `changes.bundle` to the expected
outputs, unless `--output` names it already; neither is required.

Input files travel inline in the submission record under two limits. The
first is the smallest `inline_limit` an adapter declares among the chosen
pool's receivers whose adapter launches the placed agent and satisfies the
selector, read from each receiver's last advertisement, live or stale; an
ended receiver does not count, and an undeclared adapter limit is zero and
sets none. The second is the pool's [record limit](configuration.md#record-limit), which
`pool create` records in the terms: `submit` measures the signed submission
record against it. `submit` applies the adapter limit to the input files
together, then the record limit to the record. Over either, a pool with
storage and a configured host storage credential uploads every inline input
through S3. Otherwise `submit` refuses before publishing, naming the adapter
and its limit or the record's size and the record limit, and says that a pool
with storage would carry the inputs through S3. A record still over the record
limit with every input in S3 is refused naming the prompt's size and the
record limit. Input and credential file names are [file
names](a2a-extension-v1.md#submission), and output names relative paths of
them. A credential's environment variable matches `[A-Za-z_][A-Za-z0-9_]*`
and is none an adapter sets for the agent, such as `HOME` or
`WHERRY_CREDENTIALS_DIR`. The task's credentials have unique names and
targets, and none takes the target of one of the agent's credentials:

```
invalid input: task credential deploy-key takes the environment variable GH_TOKEN of one of agent codex's credentials
```

A task credential named as one of the agent's is sealed once and delivered at
both targets.

`submit` refuses, before it publishes, a task credential or a required
credential of the placed agent that the client configuration has no
[source](configuration.md#client-configuration) for:

```
invalid input: auth required: credential openai-api-key is not in this host's configuration
```

An optional credential of the agent without a source is named in the
submission's `unsupplied_credentials`; the run goes without it, and its
[prompt](configuration.md#agent-contract) names it as not supplied.

`--s3-input` and `:s3` use the pool's [S3 lane](a2a-extension-v1.md#s3-inputs-and-outputs).
`submit` refuses them, before it uploads or publishes anything, when the
client configuration has no [`storage`](configuration.md#client-configuration) or the
pool's terms no [`storage`](configuration.md#pool-terms):

```
invalid input: pool build has no storage for S3 inputs and outputs
```

It uploads each S3 input to `wherry/<tenant>/<run>/inputs/<name>` in the pool's
bucket with one PUT, then publishes the task with each input's name, size and
SHA-256 and no URL. S3 input names are file names, distinct from the input
files' names, and the run's parts and S3 inputs number at most 256 together. S3
inputs count against neither limit. When publishing fails, `submit`
deletes what it uploaded. It also refuses a run with S3 inputs or outputs whose
task credentials and presigned URLs would not fit one 64 KiB seal, naming the
seal's size; a task credential from a `command` source is not counted, since the
command runs only to answer a seal. A
presigned URL of about 420 bytes takes about 840 bytes of the seal, so one seal
carries about 75.

`submit` seals nothing. The receiver that claims the task asks for the
agent's credentials the host supplies, the task's credentials and presigned URLs
for its S3 inputs and outputs, and `wherry await` or `wherry mcp` answers
([sealed credentials](security.md#sealed-credentials)). When no seal
answers the request within 5 minutes, the run ends `TASK_STATE_FAILED` with a message
that names the credentials, and the presigned URLs when the run has S3
inputs or outputs, but never a value:

```
credential <name> was not answered within 5m; add it to credentials in the host's wherry configuration, keep wherry await or wherry mcp running, and submit again
```

### `wherry cancel RUN_ID`

Publishes a signed cancel of the run to its pool's events and prints
`run_id=<id>`.

### `wherry await RUN_ID`

Waits for a run to finish. `--timeout` sets the wait, `2h10m` by default:
the default queue deadline and run deadline together.
`--outputs-dir DIR` sets where the run's outputs go, by default
`<state_dir>/runs/<run>/outputs/`.

It prints the state on the first line, and each part of the status message
on a line after it, for every state: the agent's answer or what went wrong,
as its [outcome file](configuration.md#agent-contract) reports it, or the
receiver's reason. A part prints as its text, its JSON data,
`[url <url>]`, or `[raw <media type> <n> bytes]`.

For a run whose placement names a model, `await` prints `model=<model>` on
standard error.

`await` writes each output that [verifies](a2a-extension-v1.md#inputs-and-outputs)
against the result, at its relative path beneath the outputs directory, and
prints a line for it:

```
output=<name> path=<path> sha256=<hex> size=<bytes>
```

For `changes.patch` it also prints `apply with: git am <path>`, and for
`changes.bundle` `fetch with: git fetch <path> HEAD`, on standard error.

`await` writes the run's redacted transcript to `transcript.jsonl` in the
outputs directory, one line per transcript line as `wherry transcript`
prints them, and prints its line as it prints an output's, last. No
expected output takes the name `transcript.jsonl`.

It prints `output <name> not returned: <reason>` on standard error for each
output the result lists as not returned and each one that fails
verification. A write never leaves the outputs directory, whatever links
it holds.

`await` downloads each S3 output the result lists with the host's storage
credential, to the same place, and prints it as it prints any output. It
downloads only an S3 output the run declares, at its key
`wherry/<tenant>/<run>/outputs/<name>`, and writes it only when its bytes
match the listed size and SHA-256; a file already there that matches is
kept. Once the run is terminal and every listed S3 output has verified,
`await` deletes every object under `wherry/<tenant>/<run>/` in the pool's
bucket, inputs included. A run that ends while no `await` runs keeps them
until `wherry cleanup` or the bucket's lifecycle rule removes them.

A run this host submitted whose queue deadline passes with no status from
a receiver can never start. `await` then prints `TASK_STATE_SUBMITTED` and
exits at once with status 1 and
`queue deadline passed at <RFC 3339 time> and no receiver claimed the run`.
The queue deadline comes from this host's record of the submission, so
`await` on another host of the tenant waits for its timeout.

`await` exits with status 1, `run ended in <state>`, for any state other
than `TASK_STATE_COMPLETED`, with status 1 when an output fails
verification, and with status 1 when the timeout passes.

While it waits, `await` answers the seal requests on the tenant feed for
every run this host submitted that has not ended, once per attempt, and logs on standard error
each answer and each verified request of its own runs that it skips or
refuses, by run, receiver and credential names. It resolves each credential's
[source](security.md#credential-sources) at the answer.

### `wherry cleanup RUN_ID`

Deletes every object under the run's prefix, `wherry/<tenant>/<run>/`, in
its pool's bucket with the host's storage credential, and prints
`run_id=<id>`. It deletes only objects whose keys a listing of that prefix
returns and that start with it, so the prefix of another run or tenant is
never touched. It finds the run as `status` does, and fails with `run not
found` otherwise. It refuses a run that has not ended, since its
receiver may still read the inputs or write the outputs there. Use it for a
run the client never saw finish.

### `wherry status [RUN_ID]`

Reads the latest verified state of a run without waiting. With `RUN_ID` it
prints the state on the first line, `model=<model>` next when the run's
placement names one, then one line per part of the status message. The
state is `TASK_STATE_SUBMITTED` until a receiver reports one. For a run this
host submitted whose queue deadline passed with no status from a receiver,
the line after the state and model is
`queue deadline passed at <RFC 3339 time> and no receiver claimed the run`.

Without `RUN_ID` it prints one line per run, latest update first:

```
<run> pool=<pool> [model=<model>] <state>
```

The list holds the runs in this host's records in `state_dir` and the runs
a receiver echoed on the tenant feed, which include runs from the tenant's
other hosts. A run's update time is that of its latest status. While it has
none, the time is this host's submission of the run when its record holds
one, else the Kafka timestamp of the run's echo. A record in `state_dir`
that does not decode counts as skipped, and its run is listed only through
its echo. With no runs it prints nothing.

`status` exits with status 0 whenever it reads a state, whatever the
state. It fails when the run is not found or the read fails.

### `wherry events RUN_ID`

Prints a run's verified history: one line per status, one per event ID, in
feed order, then one line per attempt with its transcript chunk count.

```
attempt=<n> kind=<kind> state=<state> receiver=<id> time=<time> message="<text>"
attempt=<n> transcript_chunks=<n>
```

`message` appears when the status has one, with each redaction marker shown
as `wherry status` shows it. Heartbeats are not on the feed, so they do not
appear.

### `wherry transcript RUN_ID`

Prints a run's transcript: each line of the agent's standard output, in
output order, as the receiver redacted it. The lines are those the run's
highest delivery attempt published. Before that attempt's first line, or in
place of its lines while it has none, each line comes from the highest earlier
attempt that has it. An adopter resumes the job's output past the last
heartbeat's offset, so an adopted run reads from offset 0.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--offset` | `0` | lines to skip |
| `--limit` | `0`, every line | most lines to print |

A negative `--offset` or `--limit` fails with
`--offset and --limit must not be negative`.

`transcript` shows a Claude stream-json transcript without each thinking
block's `signature` and without `system` lines of subtype `thinking_tokens`,
and counts `--offset` and `--limit` over the lines it shows. The feed keeps
both. A line it drops a signature from is re-encoded compact with its
members sorted and escapes resolved; a line that does not decode is shown as
published.

A line that is not UTF-8 prints as its bytes. For a line over 512 KiB, which
the receiver did not store, it prints `line at offset <seq> not stored:
sha256=<hex> size=<bytes>` on standard error. Each redaction marker reads
`[redacted:<name>]`, and `<name> redacted <n> times` on standard error counts
the markers in every line of the transcript. The transcript grows while the agent
runs.

`await`, `status`, `events`, `transcript`, `cleanup` and `cancel`
find a run from this host's record of it in `state_dir`, or from a receiver's
echo on the tenant feed. They fail with `run not found` when there is neither.

### `wherry mcp`

Serves the [MCP tools](mcp.md) over stdio. For as long as it runs it
answers seal requests as `await` does, logging to standard error.
The packaged MCP launch uses `["mcp"]` and the default configuration path.
A server that runs `wherry mcp --config-file PATH` uses another; see the
[Operations](operations.md#harness-integration).

### `wherry version`

Prints the binary's release as `version=<release>` and its commit as
`revision=<commit>`, each on its own line. A release build's release is its
git tag, such as `v0.1.0-rc.9`. A build from an untagged commit prints a Go
pseudo-version, and a build without git information prints `(devel)` and an
empty revision. A build from a working tree with uncommitted changes adds
`modified=true`. It takes no flags and reads no configuration.

## Admin commands

`wherry admin` connects twice, as the Kafka super user and as
`wherry.operator`, with the [environment](configuration.md#environment-variables) set.
It writes every `wherry.admin` record as `wherry.operator`, creating
`wherry.admin` and `wherry.receivers` first if they are missing. Options
take a value and can appear anywhere after `admin`. An option without a
value fails with `admin option requires a value`, one the command does not
take with `unknown option --<name>`, and a repeated one other than
`--platform-credential-file` with `option --<name> is given twice`. `pool create`,
`pool entitle`, `host add` and `receiver add` then create every ACL the new
state calls for, re-read them, and fail with `ACL plan incomplete` when one
is missing.

A bundle file must not exist beforehand. A command that fails removes the
bundle it created.

### `wherry admin pool create POOL <--config-file FILE|--config-dir DIR>`

Records the [pool terms](configuration.md#pool-terms), creates the pool's tasks
and events topics, and sets the configuration of share group `p-<pool>`:
`share.auto.offset.reset=earliest`, `share.isolation.level=read_committed`,
`share.delivery.count.limit` and `share.record.lock.duration.ms` from the
terms' `delivery_limit` and `lock_duration`, 5 and `15s` when the terms omit
them. It refuses, before it reads the broker, terms whose agent credentials
break the [pool terms](configuration.md#pool-terms) rules, and a delivery limit above the lowest
`group.share.max.delivery.count.limit` across the brokers, and terms whose
[records do not fit the broker](configuration.md#record-limit). For an existing pool it
records the new terms and sets the group configuration again. On new and
existing topics it sets the tasks and events `retention.ms` to
`max_queue_deadline` plus six hours plus one day, and their
`message.timestamp.type` to `LogAppendTime`. It refuses a `max_queue_deadline` above
29 days and 18 hours, or one that with six hours added exceeds the
`retention.ms` of the feed of a tenant entitled to the pool. See
[Topics](design.md#topics-1).

### `wherry admin pool entitle POOL --tenant TENANT`

Entitles the tenant to an existing pool, creates the tenant feed, and
grants the tenant's hosts and the pool's receivers their ACLs. It refuses
when the pool's records do not fit the [record limit](configuration.md#record-limit) with
the tenant's feed counted, or when the feed's `retention.ms` is below the
pool's `max_queue_deadline` plus six hours, and records nothing. It applies
each host's [quotas](configuration.md#quotas).

### `wherry admin host add HOST --tenant TENANT --bundle-file FILE`

Records the host, creates its principal, its key topic and the tenant
feed, grants its ACLs, applies its quotas, and writes its bundle.

### `wherry admin receiver add RECEIVER`

```sh
wherry admin receiver add RECEIVER --pool POOL \
  <--config-file FILE|--config-dir DIR> --bundle-file FILE
```

Reads the [receiver configuration](configuration.md#receiver-configuration) from a
directory or a single file, validates it against the pool's terms, records
it, creates the receiver's principal and key topic,
grants its ACLs, and writes its bundle. It refuses an existing receiver.

`--platform-credential-file PLATFORM=FILE` puts the contents of `FILE`, less
trailing CR and LF, in the bundle as the credential of the named platform,
which every adapter of that platform receives. It may be given once per
platform, and refuses a platform no adapter uses.

### `wherry admin receiver configure RECEIVER`

```sh
wherry admin receiver configure RECEIVER <--config-file FILE|--config-dir DIR>
```

Records a new configuration for a live receiver in its pool. The
principal, password and bundle stay as they are. The receiver exits when it
reads a record that differs from the configuration it runs, and its platform
restarts it.

### `wherry admin host revoke HOST --tenant TENANT`

### `wherry admin receiver revoke RECEIVER`

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
`offset=<n> size=<n> sha256=<hex>`. When every such record is one a later
release wrote, the error says to upgrade this wherry instead. The command
refuses an offset whose record decodes, a record a later release wrote, and
a hash that does not match.

### `wherry admin check`

Loads `wherry.admin`, checks that no two names collide, validates every
receiver configuration against its pool's terms, checks that every pool's
[records still fit the broker](configuration.md#record-limit), checks topic retention and
timestamp type against the [bounds](design.md#topics-1), and checks that every ACL the
state calls for is present. It does not report ACLs beyond those, and does not check an
adapter's `type` or `config` section.

## Receiver command

### `wherry receiver run --bundle-file FILE`

Runs a receiver as the bundle's principal. `--state-dir DIR` defaults to
`state` beside the bundle file, and `--plugin-dir DIR` to
`/usr/local/libexec/wherry/plugins`. It creates the state directory with
mode 0700 and keeps `signing.key` in it. On start it:

1. settles a leftover `signing.key.next` as a host command settles
   [`<host_key>.next`](#wherry-key-rotate);
2. registers its public key on its key topic when the topic is empty, and
   fails when the topic holds another key;
3. with `--rotate-key`, replaces `signing.key` as
   [`wherry key rotate`](#wherry-key-rotate) replaces the host key, through
   `signing.key.next`, and logs `rotated key: old=<id> new=<id>`;
4. waits for its configuration record on `wherry.admin` and validates it
   against the pool's terms;
5. starts the [adapter plugins](#adapter-plugins) its adapters' types name
   from the plugin directory and configures each adapter, reads its pool's
   events once, and starts advertising and claiming.

It logs its own lines to standard output with the prefix `receiver: `;
plugin and link lines are hclog lines named `plugin`. Plugin lines, the
plugin's raw standard error that go-plugin relays, and adapter sweep and
readiness errors carry the
[marker](a2a-extension-v1.md#seal) of each credential value
delivered to a run the receiver holds in place of the value. It exits on
`SIGINT` or `SIGTERM` without stopping its jobs, and fails with
`receiver configuration changed` when a configuration record that differs
from its own appears. An adapter type that is not a valid name ends it with
`adapter type "<type>" is not a valid name`.
It refuses to start after its membership end.

With `--metrics-addr HOST:PORT` it serves its [metrics](#metrics) at
`/metrics` on that address, and fails at start when it cannot listen there.
Without the flag it serves none.

`--rotate-key` rotates the key at that start only. A start without it never
rotates the key.

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
adapter its name, pool, agent and `config` section, and the bundle's
`platform` entry for the adapter's platform when there is one. An adapter
takes no credential of its own.

A plugin also reports, for each adapter, the paths of the input, output and
credentials directories as its agent sees them, which the receiver writes
into the prompt, and implements `InlineLimit` to report the most inline input
bytes it delivers. Zero declares no adapter limit. Dispatch carries a run's
input files and every delivered credential with the prompt, and a fetch for
each S3 input: its name, presigned URL,
size and SHA-256, which the plugin fetches and verifies before the agent
starts, reporting a job that never started with class `fetch` when it
cannot. A plugin that cannot deliver an input reports it, and the run ends
`TASK_STATE_FAILED` naming it. Once the job ends the receiver
stops it and asks the plugin to collect its outputs, which arrive over
the link in pieces of at most 1 MiB. The outputs the receiver names for
streaming, its S3 outputs, the plugin streams as the receiver reads them.

A plugin gives each agent the variables of `adapter.AgentEnvironment` over
the environment it configures, and removes those of `adapter.AgentUnset`
from that environment, so the platform's `git` configuration does not reach
a [git input](a2a-extension-v1.md#git-input)'s clone. It cannot remove
what the platform or the image sets. This protects a cooperative agent,
not against one that runs commands of its own
([Operations](operations.md#git-repositories)).

The receiver and a plugin talk gRPC over
[go-plugin](https://github.com/hashicorp/go-plugin) with mutual TLS on a
certificate pair made for each start, protocol version 8. The receiver refuses a
plugin of any other protocol version at its start, so the receiver and its
plugins upgrade together. Configure carries no credential. Dispatch carries
every credential of the run as an `adapter.Secret` holding its name, its value
and exactly one target: `Env`, an environment variable the plugin sets for the
agent, or `File`, a file it writes in the agent's credentials directory.
The receiver refuses a version 7 plugin with
`incompatible API version with plugin. Plugin version: 7, Client versions: [8]`.
The receiver refuses a plugin that offers no TLS certificate. Every call names
its adapter. A plugin accepts messages up to 64 MiB; the receiver logs a larger
Dispatch as `dispatch rejected: message too large`.

Each advertisement gives every adapter's sweep and readiness call one
advertise interval, 30 seconds; a call that runs out counts as not ready
and is logged. Each offer carries the adapter's declared inline input limit;
negative limits are invalid.

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

When a plugin exits, the receiver logs `plugin exited; restarting` as a
`plugin` line, starts it again after one second, waiting twice as long after each failed start up to 30
seconds, and configures every adapter of its type again. Until then those
adapters are not ready and the next advertisement withdraws their offers. A run
whose job the receiver is following reads again from its last offset once the
plugin is back; when the plugin stays down for one lock duration, the run fails
with `log stream failed`.

A receiver with no ready adapter advertises with no offers, so it stays
`live` with every adapter `ready=false`, and placement passes over it. A
job its adapter reports never started is a `platform` start failure: the
receiver purges the job and returns the task to the pool. A job that never
started with class `fetch` instead ends the run `TASK_STATE_FAILED` with
`an S3 input was not fetched, or did not match its size and SHA-256`.

A plugin may report, with a job that never started, the platform's reason
in `Terminal.Reason`, which can quote the run's material. The receiver logs
`run <id>: job never started (<class>): <reason>`, or the line without the
reason when there is none. It replaces each delivered credential value in
the reason with its redaction marker, as it does in published output, and
each `http://` or `https://` URL with `[redacted-url]`, turns line breaks
into spaces and cuts the result to 512 bytes. For class `fetch` the line is
`run <id>: job never started: an S3 input was not fetched`, and a reason,
which can quote a presigned URL, is logged as `[omitted]`. The reason goes
to the receiver's log only.

A plugin returns a failed platform call as an `adapter.Error`, which names
the operation (`Op`) and the platform's failure class (`Class`) beside the
error, and crosses the link as a gRPC status detail. The receiver logs an
adapter call's error as `op=<op> class=<class>: <text>`, without
`class=` when the adapter gives none, or as its text alone when the error
has no operation, under the rules for a
reason above. A run's calls (dispatch, lookup, follow, stop, collect, purge
and release) replace each credential value delivered to the run with its
redaction marker; an adopter's lookup comes before its seal and has no values
yet. The pool's calls (sweep, readiness and configure) belong to no run and
replace no values. Every call's text has its URLs replaced, its line breaks
turned into spaces and is cut to 512 bytes. An adapter's configure error ends
the receiver with `adapter <name>: <text>`.

A receiver adopts a run's existing job only when the adapter and agent
recorded with it match the adapter that finds it. It leaves a job whose
adapter is another of its adapters to that adapter, and stops and purges
any other job under the run's ID and dispatches the run again.

The protocol is the Go module `github.com/benemon/wherry/adapter` in this
repository. [`adapter.proto`](https://github.com/benemon/wherry/blob/main/adapter/proto/wherry/adapter/v1/adapter.proto)
defines the service, `adapter.Serve` runs a plugin and gives its factory
the logger whose JSON lines the receiver relays at their level, and
`adaptertest` is the conformance suite a plugin runs against its platform.

### Metrics

A receiver started with `--metrics-addr` serves its metrics in the Prometheus
text format over plain HTTP. `GET /metrics` is the only route; every other
path answers 404, and a method other than GET or HEAD on `/metrics`
answers 405. The scrape holds the Go runtime (`go_`) and
process (`process_`) families, `wherry_build_info` with `version` and
`revision` and value 1, and the families below, each with the label `pool`.

Every label value comes from the receiver's configuration, its pool's terms and
entitlements, or a fixed list. No label or value carries a run ID, prompt,
output, error text, URL, image, path, node or credential name. An agent the
pool's terms do not name is `other`. `model` is the placement's model when the
agent's `models` in the terms hold it, `other` when they do not, and empty for
an agent with no `models`. A `state` outside the A2A task states is `other`.
A run ended without an adapter, one given back at the delivery budget
before any adapter took it, has `adapter="none"`.

| Family | Type | Labels | Counts |
| --- | --- | --- | --- |
| `wherry_claims_total` | counter | `tenant` | claimed tasks whose submission verifies |
| `wherry_admissions_total` | counter | `tenant`, `adapter` | tasks admitted, by the adapter they were routed to |
| `wherry_rejections_total` | counter | `tenant`, `reason` | rejections, by reason |
| `wherry_runs_total` | counter | `tenant`, `adapter`, `agent`, `model`, `state` | runs ended `completed`, `failed` or `canceled` |
| `wherry_run_duration_seconds` | histogram | `tenant`, `adapter`, `agent`, `model`, `state` | time from this attempt's claim to the run's terminal state; an adopted run counts the adopting attempt only |
| `wherry_queue_wait_seconds` | histogram | `tenant`, `adapter` | time from submission to first claim |
| `wherry_adoptions_total` | counter | `tenant`, `adapter` | runs that adopted an earlier attempt's job |
| `wherry_redeliveries_total` | counter | `tenant`, `adapter` | runs executed on a redelivery |
| `wherry_in_flight` | gauge | `adapter` | tasks held |
| `wherry_capacity` | gauge | none | the configured capacity |
| `wherry_start_failures_total` | counter | `adapter`, `class` | start failures: `input-too-large`, `dispatch`, `platform`, and `offer-unavailable`, which counts a release by a member with no matching adapter, has no start-failure record and carries `adapter="none"` |
| `wherry_dispatch_outcomes_total` | counter | `adapter`, `outcome` | dispatches: `created`, `nothing`, `unknown` |
| `wherry_stop_failures_total` | counter | `adapter` | failed `Stop` calls |
| `wherry_purge_failures_total` | counter | `adapter` | failed `Purge` calls |
| `wherry_sweep_failures_total` | counter | `adapter` | failed `Sweep` calls |
| `wherry_collection_failures_total` | counter | `adapter`, `reason` | runs whose outputs were not collected: `not_stopped`, `over_limits`, `timeout`, `failed` |
| `wherry_plugin_restarts_total` | counter | `type` | plugin restarts |
| `wherry_seal_waits_total` | counter | `tenant`, `outcome` | seal waits: `sealed`, `expired`, `unanswered`, `adopter_refused` |
| `wherry_seal_wait_seconds` | histogram | `tenant` | time a run waited for its seal, whatever the outcome |
| `wherry_redactions_total` | counter | `tenant`, `adapter` | credential values replaced in published output |
| `wherry_bus_errors_total` | counter | `op` | Kafka errors: `read`, `write`, `transactional ID`, `share group` |
| `wherry_denials_total` | counter | `op` | Kafka authorisation errors, by the capability denied: `read`, `write`, `transactional ID`, `share group`, `membership` |
| `wherry_renewal_failures_total` | counter | none | failed claim renewals |
| `wherry_transaction_aborts_total` | counter | none | transactions aborted after a failed produce |
| `wherry_ambiguous_commits_total` | counter | none | commits with an unknown outcome, read back from the pool's events |
| `wherry_fenced_total` | counter | none | runs given up to a newer holder |
| `wherry_metrics_dropped_total` | counter | `adapter`, `reason` | adapter samples dropped and `Metrics` calls that failed |
| `wherry_adapter_platform_errors_total` | counter | `adapter`, `class` | platform API errors: `unavailable`, `denied`, `conflict`, `timeout`, `other` |
| `wherry_adapter_cleanup_pending` | gauge | `adapter` | the adapter's ended jobs awaiting cleanup |
| `wherry_adapter_sweep_deletions_total` | counter | `adapter`, `kind` | material a sweep deleted: `job`, `variable` |
| `wherry_adapter_placement_seconds` | histogram | `adapter` | time from dispatch to the agent task starting |

A rejection's `reason` is its fixed text, such as `run expired`. A reason that
quotes the task counts as `git remote host outside the pool's vcs hosts` or
`pool has no storage`.

A seal wait is `expired` when the run's deadline passes, `unanswered` when
the 5-minute wait ends and the run fails, and `adopter_refused` when an
adopter's wait ends and it stops the job before the run fails.

The core histograms' buckets are 1, 5, 15, 30 and 60 seconds, 2, 5, 10 and 30
minutes, and 1, 2, 4 and 6 hours. `wherry_adapter_placement_seconds` uses
`adapter.PlacementBuckets`: 1, 2, 5, 10 and 30 seconds, and 1, 2, 5, 10 and 30
minutes.
