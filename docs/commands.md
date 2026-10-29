# Commands

[Key concepts](concepts.md) explains the model. The
[wherry A2A extension](a2a-extension-v1.md) specifies the records.
[Configuration](configuration.md) lists the files the commands read, and
[MCP tools](mcp.md) the tools `wherry mcp` serves.

## Conventions

- The [MCP tools](mcp.md)' results are the machine interface, and their
  shape freezes at 1.0. The CLI's text output may change. `wherry receivers
  --json` is the one CLI JSON form, and it prints the objects the MCP
  `receivers` tool returns.
- `wherry admin`, `wherry dev`, `wherry receiver run` and `wherry version` read no client
  configuration. Every other command reads it first, and every command except `key` and `mcp`
  then connects to Kafka. `key` connects only to settle a
  [leftover next key](#wherry-key-rotate).
- Every host command other than `version` takes `--config-file FILE`. It defaults to
  `~/.config/wherry/config.yaml`. A leading `~/` is expanded.
- A command's flags may come before or after its `RUN_ID`.
- A failed command prints `wherry: <error>` on standard error and exits
  with status 1. `wait` has its own [exit codes](#exit-codes).
- Durations use Go syntax, such as `90s`, `45m` or `720h`, and must be
  positive. `host revoke` and `receiver revoke` refuse a `--wait` of zero or
  less before they revoke anything; without `--wait` they compute the wait
  from the broker's timeouts.
- `cancel`, `wait`, `status`, `events` and `logs` print
  `skipped N unverifiable records` on standard error when N records of the
  run fail verification.
- Standard output carries a command's result and standard error its
  errors and diagnostics, so `wherry wait ID >/dev/null` keeps only the
  diagnostics and the exit status.
- `wait` and `status` print each redaction marker of a run this host
  submitted as `[redacted:<name>]`, naming the credential or S3 object,
  and `<name> redacted N times` on standard error for each name whose
  markers they printed. A host maps a marker with the run's redaction key
  in `state_dir`, and prints a marker as it is when the run's record holds
  no 32-byte key.

### Names

Tenant, host, receiver, pool, plugin profile, agent and credential names, and plugins, match
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
plugin profile.

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
and any `wherry wait`; they hold the old key.

Every host command that finds `<host_key>.next` folds the host's key topic
before anything else. When the current key is the next key's, it renames
`<host_key>.next` over `host_key`; when it is still the old key, it removes
`<host_key>.next`. It fails when the current key is neither.

### `wherry receivers`

Lists the receivers of every pool the tenant is entitled to, sorted by pool
and receiver ID:

```
receiver=<id> pool=<pool> capacity=<n> in_flight=<n> heartbeat_age=<age> status=<status>
  profile=<name> plugin=<type> agent=<agent> ready=<bool> labels=<k=v,...> [inline_limit=<n>] [models=<m,...> default_model=<m>] [credentials=<name[(missing,optional)],...>]
```

`status` is `live` when the latest advertisement is dated within 120
seconds of now, either side, `stale` otherwise, `silent` when there is none,
and `ended` after the receiver's membership end. Plugin profiles, their plugins and their labels come from the
receiver's configuration record, and an agent's `models` and
`default_model` from the pool's [terms](configuration.md#pool-terms) when it has them.
`ready` is true when the receiver is live
and its advertisement offers the profile. A live receiver with no ready
profile lists each profile with `ready=false`. `inline_limit`, when set, is
the most inline input bytes the offered profile delivers. `credentials` names
the agent's credentials in the pool's terms, marking `missing` each this
host's configuration has no [source](configuration.md#client-configuration) for and
`optional` each a run goes without, as in
`credentials=openai-api-key,github-token(missing,optional)`. It shows no
target.

`--json` prints a JSON array, one object per receiver:

| Member | Content |
| --- | --- |
| `receiver_id`, `pool` | strings |
| `profiles` | `{name, plugin, agent, inline_limit, models, default_model, credentials, labels, ready}`, `credentials` holding `{name, optional, configured}`; optional values are omitted when unset |
| `capacity`, `in_flight` | integers from the latest advertisement |
| `last_advertised` | RFC 3339 time of the latest advertisement; omitted when there is none |
| `status` | `live`, `stale`, `silent` or `ended` |

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
| `--selector` | empty | [selector](#selectors) over plugin profile labels |
| `--prompt` | | prompt text |
| `--prompt-file` | | file holding the prompt; `-` reads standard input |
| `--git REMOTE@REF` | | repository the agent clones: an `https` or `ssh` URL, or `user@host:path`, at a branch, tag or commit |
| `--git-path` | the repository name | checkout directory, relative to the agent's working directory |
| `--git-changes patch\|bundle` | | ask for the agent's commits back as the output `changes.patch` or `changes.bundle` |
| `--input FILE[:s3]` | | file the agent gets in `WHERRY_INPUT_DIR` under its base name; `:s3` uploads it, at most 5 GiB, to the pool's bucket; repeatable |
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
interpret a model name; the profile's plugin passes it to the agent as
`WHERRY_MODEL`.

Exactly one of `--prompt` and `--prompt-file` is required, and an empty
prompt file fails with `--prompt-file is empty`. `--prompt-file -` reads the
prompt from standard input, as in `generate-brief | wherry submit
--prompt-file -`. The other `--git-` flags
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

`--input` and `--output` read everything after the first `:` in their value
as suffixes, so they refuse a path or name that holds a `:`.

Input files travel inline in the submission record under two limits. The
first is the smallest `inline_limit` declared for a profile among the chosen
pool's receivers' profiles that launch the placed agent and satisfy the
selector, read from each receiver's last advertisement, live or stale; an
ended receiver does not count, and an undeclared profile limit is zero and
sets none. The second is the pool's [record limit](configuration.md#record-limit), which
`pool create` records in the terms: `submit` measures the signed submission
record against it. `submit` applies the profile limit to the input files
together, then the record limit to the record. Over either, a pool with
storage and a configured host storage credential uploads every inline input
through S3. Otherwise `submit` refuses before publishing, naming the profile
and its limit or the record's size and the record limit, and says that a pool
with storage would carry the inputs through S3. A record still over the record
limit with every input in S3 is refused naming the prompt's size and the
record limit. Input and credential file names are [file
names](a2a-extension-v1.md#submission), and output names relative paths of
them. A credential's environment variable matches `[A-Za-z_][A-Za-z0-9_]*`
and is none a plugin sets for the agent, such as `HOME` or
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

`--input FILE:s3` and `--output NAME:s3` use the pool's [S3 lane](a2a-extension-v1.md#s3-inputs-and-outputs).
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
for its S3 inputs and outputs, and `wherry wait` or `wherry mcp` answers
([sealed credentials](security.md#sealed-credentials)). When no seal
answers the request within 5 minutes, the run ends `TASK_STATE_FAILED` with a message
that names the credentials, and the presigned URLs when the run has S3
inputs or outputs:

```
credential <name> was not answered within 5m; add it to credentials in the host's wherry configuration, keep wherry wait or wherry mcp running, and submit again
```

### `wherry cancel RUN_ID`

Publishes a signed cancel of the run to its pool's events and prints
`run_id=<id>`.

### `wherry wait RUN_ID`

Waits for a run to finish and collects it. `--timeout` sets the wait,
`2h10m` by default: the default queue deadline and run deadline together.
`--outputs-dir DIR` sets where the run's outputs go, by default
`<state_dir>/runs/<run>/outputs/`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--timeout` | `2h10m` | how long to wait for the run to end |
| `--outputs-dir` | `<state_dir>/runs/<run>/outputs/` | where the run's outputs go |

Standard output carries the result: each part of the status message on its
own line, then the `output=` lines. `wait` prints no transcript;
[`wherry logs`](#wherry-logs-run-id) reads it, and `wherry logs ID > file`
keeps it. Standard error carries errors, the diagnostics below and the log
of the seal requests `wait` answers.

The status message's parts are the agent's answer or what went wrong, as its
[outcome file](configuration.md#agent-contract) reports it, or the
receiver's reason, for every state. A part prints as its text, its JSON
data, `[url <url>]`, or `[raw <media type> <n> bytes]`. For a
[reply](a2a-extension-v1.md#the-reply) over 64 KiB, the reply chunks'
verified bytes print whole in place of the preview the message carries, and
`wait` writes no file of it. Standard error then says
`reply cut at the pool's reply limit` when the reply was longer than the
pool's `reply_limit`, or
`reply rejected by this host: <reason>; printed its preview` when the chunks
do not verify, with a reason as for an output.

For a run whose placement names a model, `wait` prints `model=<model>` on
standard error.

`wait` writes each output that [verifies](a2a-extension-v1.md#inputs-and-outputs)
against the result, at its relative path beneath the outputs directory, and
prints a line for it:

```
output=<name> path=<path> sha256=<hex> size=<bytes>
```

For `changes.patch` it also prints `apply with: git am <path>`, and for
`changes.bundle` `fetch with: git fetch <path> HEAD`, on standard error.
No expected output takes the name `reply.txt`.

`wait` reads the run's statuses and result from the tenant feed, and its
output chunks from the tenant's transcript topic, from the first offset at
or after one minute before the append time of the run's earliest echo on
the feed. Once it sees a terminal status, it reads the transcript topic to
its last stable offset, and waits until that reaches the high watermark it
lists after it sees the terminal, before it writes the outputs.

It prints `output <name> not returned: <reason>` on standard error for each
output the result lists as not returned and each one that fails
verification. A write never leaves the outputs directory, whatever links
it holds.

`wait` downloads each S3 output the result lists with the host's storage
credential, to the same place, and prints it as it prints any output. It
downloads only an S3 output the run declares, at its key
`wherry/<tenant>/<run>/outputs/<name>`, and writes it only when its bytes
match the listed size and SHA-256; a file already there that matches is
kept. Once the run is terminal and every listed S3 output has verified,
`wait` deletes every object under `wherry/<tenant>/<run>/` in the pool's
bucket, inputs included. A run that ends while no `wait` runs keeps them
until `wherry cleanup` or the bucket's lifecycle rule removes them.

A run this host submitted whose queue deadline passes with no status from
a receiver can never start. `wait` then collects nothing, prints nothing on
standard output, and exits at once with status 1 and
`queue deadline passed at <RFC 3339 time> and no receiver claimed the run`.
The queue deadline comes from this host's record of the submission, so
`wait` on another host of the tenant waits for its timeout.

#### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | the run ended `TASK_STATE_COMPLETED` and its outputs were written |
| 1 | `wait` could not wait or collect: usage, run not found, broker unreachable, outputs not written or failing verification, queue deadline passed unclaimed |
| 2 | the run ended `TASK_STATE_FAILED` |
| 3 | the run ended `TASK_STATE_CANCELED` |
| 4 | the run ended `TASK_STATE_REJECTED` |
| 6 | `--timeout` passed with the run still going |

For codes 2 to 4 the error is `run ended in <state>`.

While it waits, `wait` answers the seal requests on the tenant feed for
every run this host submitted that has not ended, once per attempt, and logs on standard error
each answer and each verified request of its own runs that it skips or
refuses, by run, receiver and credential names. It resolves each credential's
[source](security.md#credential-sources) at the answer. Stopping `wait`
can leave a credential request unanswered, and the run then ends
`TASK_STATE_FAILED` after the 5-minute seal wait.

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
feed order, then one line per attempt with its transcript chunk count. An
attempt's terminal status carries its count. For an attempt with no
terminal status, running or released without one, `events` counts its
chunks on the tenant's transcript topic.

```
attempt=<n> kind=<kind> state=<state> receiver=<id> time=<time> message="<text>"
attempt=<n> transcript_chunks=<n>
```

`message` appears when the status has one, with each redaction marker shown
as `wherry status` shows it.

### `wherry logs RUN_ID`

Prints a run's transcript: each line of the agent's standard output, in
output order, as the receiver redacted it. The lines are those the run's
highest delivery attempt published, and, before its first line, those of
the highest earlier attempt that has them, as
[Chunk](a2a-extension-v1.md#chunk) specifies. `logs` is read-only and
answers no seal request.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-f`, `--follow` | off | print lines as the agent prints them, until the run ends |

`logs` reads the lines from the tenant's transcript topic, from the
first offset at or after one minute before the append time of the run's
earliest echo on the tenant feed. Once the feed holds the run's terminal status, it reads the
transcript topic to its last stable offset, and waits until that reaches the
high watermark it lists after it sees the terminal, before it prints.
Without `-f`, it prints the lines the topic holds and exits.

With `-f`, `logs` follows the tenant's transcript topic with one consumer
from the run's echo, so each line prints as it is written, once and in
order. When the run reaches a terminal status, it reads the transcript
topic to the high watermark as above, prints the lines it has not printed,
and exits with status 0. When it sees a seal request for the run that no
answerer of this host has sealed after 3 seconds, it prints once on
standard error:

```
run is waiting for credentials; run wherry wait <id>
```

`logs` prints every line; a range comes from the shell, as in
`wherry logs ID | sed -n '101,200p'`.

`logs` shows a Claude stream-json transcript without each thinking
block's `signature` and without `system` lines of subtype `thinking_tokens`.
The transcript topic keeps both. A line it drops a signature from is
re-encoded compact with its members sorted and escapes resolved; a line
that does not decode is shown as published.

A line that is not UTF-8 prints as its bytes. For a line over 512 KiB, which
the receiver did not store, it prints `line at offset <seq> not stored:
sha256=<hex> size=<bytes>` on standard error. Each redaction marker reads
`[redacted:<name>]`, and `<name> redacted <n> times` on standard error counts
the markers in every line of the transcript. The transcript grows while the agent
runs.

`wait`, `status`, `events`, `logs`, `cleanup` and `cancel`
find a run from this host's record of it in `state_dir`, or from a receiver's
echo on the tenant feed. They fail with `run not found` when there is neither.

### `wherry mcp`

Serves the [MCP tools](mcp.md) over stdio. For as long as it runs it
answers seal requests as `wait` does, logging to standard error.
The packaged MCP launch uses `["mcp"]` and the default configuration path.
A server that runs `wherry mcp --config-file PATH` uses another, as
[Harness integration](operations.md#harness-integration) shows.

With `--listen ADDR` it serves the tools over MCP Streamable HTTP at `/mcp`
on `ADDR` instead, in their [HTTP forms](mcp.md#over-http), as
[An always-on host](operations.md#an-always-on-host) runs it. Every request
carries `Authorization: Bearer TOKEN` with the token in `--token-file`; any
other request gets `401` with `WWW-Authenticate: Bearer`, and the server logs
the refusal with the peer's address. A loopback `ADDR`, `127.0.0.1` or
`::1`, takes plain HTTP; any other takes `--tls-cert` and `--tls-key` and
serves HTTPS. `wherry mcp` refuses to start without a token file, with a
token file that group or others can read or write, with a token shorter than
32 characters, or off loopback without a certificate.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--config-file` | `~/.config/wherry/config.yaml` | the client configuration |
| `--listen ADDR` | | `HOST:PORT` to serve Streamable HTTP on; without it, stdio |
| `--token-file FILE` | | file holding the bearer token, mode `0600`; required with `--listen` |
| `--tls-cert FILE`, `--tls-key FILE` | | PEM certificate and key to serve HTTPS with; required off loopback |
| `--output-bound BYTES` | `24576` | largest output `wait` returns over HTTP as an MCP resource; a larger one is a presigned GET URL |
| `--url-lifetime` | `1h` | lifetime of the presigned URLs `upload` and `wait` return over HTTP, from `1s` to `168h`; a longer one is cut to `168h` |

### `wherry version`

Prints the binary's release as `version=<release>` and its commit as
`revision=<commit>`, each on its own line. A release build's release is its
git tag, such as `v0.1.0-rc.9`. A build from an untagged commit prints a Go
pseudo-version, and a build without git information prints `(devel)` and an
empty revision. A build from a working tree with uncommitted changes adds
`modified=true`. It takes no flags and reads no configuration.

## Dev commands

`wherry dev` runs a whole deployment on one machine for trying wherry and
developing against it, as [Try it on one machine](quickstart.md#try-it-on-one-machine)
walks through. Each command takes `--instance NAME`, default `default`, a
[name](#names). An instance lives in `$XDG_DATA_HOME/wherry/dev/NAME`, by
default `~/.local/share/wherry/dev/NAME`, mode 0700, in two parts:
`cache/`, which `init` fills and `reset` removes, and `env/`, which `up`
creates and `down` removes. The ports are fixed, so one instance is up at
a time: Nomad on 4646, 4647 and 4648, Kafka on 9093 and, with storage, S3
on 18080, all on 127.0.0.1.

An instance is up while its `env/up.json` names a live Nomad agent that
`up` started. Every step prints its name on standard error, and a failed
step fails with `step <name>: <cause>`.

### `wherry dev init`

```sh
wherry dev init [--instance NAME] [--runtime podman|docker] [--storage-enabled] [--agent NAME]
```

Acquires everything `up` runs into `cache/` and records it in
`cache/manifest.json`:

- Nomad CE 2.0.7, the release the nomad plugin's CI tests. Its
  `SHA256SUMS` must carry a signature by HashiCorp's release key,
  fingerprint `C874 011F 0AB4 0511 0D02 1055 3436 5D94 72D7 468F`, and list
  the pinned SHA-256 of the `linux_amd64` zip. The zip must match
  `SHA256SUMS`.
- On Linux with Podman, the newest `nomad-driver-podman`, checked the same
  way.
- The newest `nomad/v*` release of the nomad plugin that passes three
  checks: its `checksums.txt` verifies against its sigstore bundle from the
  plugin's release workflow, its archive matches `checksums.txt`, and its
  binary speaks this wherry's plugin protocol. A binary that speaks another
  is dropped for the next older release.
- `docker.io/apache/kafka:latest`, pulled and tagged
  `wherry-dev/kafka:<id12>` in the runtime's own store. With
  `--storage-enabled`, also `quay.io/benjamin_holmes/ceph-aio:v21`, tagged
  `wherry-dev/ceph-aio:<id12>`. Under Podman the tags start `localhost/`.
- A copy of the running wherry binary, which the receiver job runs.

`--runtime` is required when both `docker` and `podman` answer `version`,
and found otherwise. `init` looks for `claude`, `codex` and `gemini` on
`PATH`. One found is the default agent. With several it asks on a
terminal, and fails with `found claude and codex; choose the default with
--agent` without one. With none and no `--agent` it fails with
`no agent harness found on PATH (looked for claude, codex, gemini); install one, or choose --agent mock`
before it downloads anything. `--agent NAME` takes a harness it found or
`mock`. `init` without `--storage-enabled` drops storage from an instance
that has it.

`init` sends `GH_TOKEN`, else `GITHUB_TOKEN`, to GitHub's API as a bearer
token when one is set. Without one, GitHub allows 60 API requests an hour
from an address, and a spent limit fails naming the time it resets.

`init` downloads only what changed and prints `instance NAME: unchanged`
when nothing did. A harness installed later needs `init` again. It refuses
while the instance is up with `instance NAME is up; run wherry dev down
first`. A manifest of another layout version is refused naming its path.

### `wherry dev up`

```sh
wherry dev up [--instance NAME] [--runtime podman|docker] [--agent-image NAME=IMAGE]...
```

Builds and starts the environment from `cache/`, downloading nothing, and
returns once a run of the mock agent completes. Its steps:

1. `preflight` refuses while this instance is up (`instance NAME is
   already up`) or another is (`instance OTHER is up; run wherry dev down
   --instance OTHER first`), without a manifest (`nothing acquired for
   instance NAME; run wherry dev init first`), with a cached file whose
   SHA-256 differs from the manifest's, with a `--runtime` other than the
   manifest's, with a port in use (`port 9093 is in use`), and when the
   running binary differs from the copy (`the running wherry differs from
   the snapshot; run wherry dev init`).
2. `harnesses`, `credentials`: the agents are the harnesses `init` found,
   the `--agent-image` names and `mock`. For each, the set variables of
   `CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`, `CODEX_API_KEY` (else
   `OPENAI_API_KEY`), `GEMINI_API_KEY` and `WHERRY_DEV_MOCK_TOKEN` are
   copied into `env/host/credentials/`, mode 0600.
3. `tls`, `passwords`: an instance CA, the broker's certificate for
   127.0.0.1, and the SCRAM passwords of `wherry-admin` and
   `wherry.operator`. `env/admin/admin.env` exports the five
   [`WHERRY_TRUST_*` variables](configuration.md#environment-variables)
   for `wherry admin` against the instance.
4. `image` checks that the manifest's tags are still in the runtime, and
   fails with `image TAG is no longer in the runtime; run wherry dev init`.
5. `nomad` starts `nomad agent -dev` as you, detached, its log in
   `env/nomad/nomad.log` and its data directory mode 0700. Its
   `env.denylist` holds Nomad's default list, the copied variables and
   every variable whose name ends `_TOKEN`, `_KEY`, `_SECRET` or
   `_PASSWORD` when `init` ran, so the agents' environment carries none of
   them. On Linux without a Podman socket it starts `podman system
   service` beside Nomad.
6. `kafka` runs the broker as a Nomad job from the local tag and waits for
   a metadata request over SCRAM. `operator-acl` grants `wherry.operator`
   its ACL on `wherry.admin`.
7. `bootstrap` makes the admin calls of the operator's steps: pool `dev`,
   tenant `dev`, host `dev` and receiver `dev-1` with a plugin profile per
   agent, and registers the host key.
8. `receiver` runs `wherry receiver run` from the copy as a `raw_exec` job
   and waits for `dev-1` to be live with every profile ready.
9. `smoke` runs the mock agent and checks the output it returns. `ready`
   writes `env/up.json` and prints the summary.

The harness profiles and `mock` run under `raw_exec` with label
`isolation: host`. `--agent-image NAME=IMAGE` splits at the first `=`. It
runs agent `NAME` in `IMAGE` with label `isolation: container`, never
pulled, so the image must be in the runtime. For `claude`, `codex` and
`gemini` the profile runs the harness from `/local`. Any other `NAME` runs
the image's entrypoint, which meets the
[agent contract](configuration.md#agent-contract).
[`agent.Containerfile`](https://github.com/benemon/wherry/blob/main/docs/agent.Containerfile) builds one for `claude`.

With storage, `storage`, `s3-user` and `bucket` run after `kafka`. ceph-aio
serves S3 on `http://127.0.0.1:18080`. `s3-user` has `radosgw-admin`
generate the user's keys and writes them to `env/host/credentials/`. Pool
`dev` takes the endpoint as its [storage](configuration.md#pool-terms), and
the smoke run sends an S3 input and returns its output through the bucket.
The ceph-aio job reserves 2048 MB of memory, and its data lives in the
container, so `down` removes it.

A failed step leaves the environment for `down` to remove.

### `wherry dev down`

```sh
wherry dev down [--instance NAME]
```

Its steps, each run after a failed one:

1. `jobs` deregisters the receiver, every agent job, storage and Kafka,
   waits up to 60 seconds for their allocations to end, and purges them.
2. `containers` waits up to 60 seconds for the runtime to list no
   container of theirs, then removes those left by ID.
3. `nomad` stops Nomad and the Podman service with `SIGTERM`, and
   `SIGKILL` after 30 seconds.
4. `ports` checks that the ports are free.
5. `data` removes `env/`.

`down` fails naming every failed step. It keeps `cache/` and prints a
`mcp remove` line for each harness, since the registration names a
configuration that is gone. An instance that is not up prints
`instance NAME is not up`, removes a leftover `env/` and exits 0.

### `wherry dev reset`

```sh
wherry dev reset [--instance NAME]
```

Removes `cache/`. It refuses while the instance is up. The images stay in
the runtime's store. Remove `wherry-dev/kafka:<id12>` with the runtime.

## Admin commands

`wherry admin` connects twice, as the Kafka super user and as
`wherry.operator`, with the [environment](configuration.md#environment-variables) set.
It writes every `wherry.admin` record as `wherry.operator`, creating
`wherry.admin` and `wherry.receivers` first if they are missing and setting the
[configuration](design.md#topics-1) of an existing `wherry.receivers`. Options
parse as Go flags, in the forms `--name value`, `--name=value` and
`-name value`, and the last of a repeated option wins. Options other than
`--dry-run` and `--force` take a value, and options can appear anywhere after
`admin`. An option without a value fails with
`flag needs an argument: -<name>`, one no admin command takes with
`flag provided but not defined: -<name>`, and one the command does not take
with `unknown option --<name>`. `--plugin-sha256` is given once per plugin
type; any other repeated option takes its last value. `pool create`, `pool update`,
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
[records do not fit the broker](configuration.md#record-limit). It refuses an
existing pool with `pool exists; change its terms with pool update`. On new and
existing topics it sets the tasks and events `retention.ms` to
`max_queue_deadline` plus six hours plus one day, and their
`message.timestamp.type` to `LogAppendTime`, and creates the progress topic
of each live receiver in the pool, or sets an existing one's configuration,
with the same `retention.ms`. It refuses a `max_queue_deadline` above
29 days and 18 hours, or one that with six hours added exceeds the
`retention.ms` of the feed or transcript topic of a tenant entitled to the
pool. Before it measures the records, it sets `max.message.bytes` to 2 MiB
(2097152) on the feed and transcript topic of every tenant with a host or an
entitlement. See
[Topics](design.md#topics-1).

### `wherry admin pool update POOL`

```sh
wherry admin pool update POOL <--config-file FILE|--config-dir DIR> [--dry-run]
```

Records new terms for an existing pool, makes the checks and settings of
`pool create`, and sets the share group's `share.delivery.count.limit` and
`share.record.lock.duration.ms` from the new terms. It refuses a pool that
does not exist with `no pool by that name`. It first checks the recorded
configuration of every receiver in the pool whose membership has not ended
against the new terms, as `receiver add` checks a new receiver, and refuses
the terms when a plugin profile no longer fits, naming each receiver, plugin
and profile and the reason. Receivers apply the new terms at their next read
of `wherry.admin`, within 15 seconds, without a restart. A lowered
`delivery_limit` applies to a task in flight at its next release.

With `--dry-run`, it makes the same checks, prints nothing, and writes
nothing to Kafka; it measures a tenant topic below 2 MiB as the update would
set it. Terms it refuses fail in the same way with or without
`--dry-run`.

### `wherry admin pool entitle POOL --tenant TENANT`

Entitles the tenant to an existing pool, creates the tenant feed and
transcript topic with `max.message.bytes` 2 MiB, or sets existing ones to
it, and grants the tenant's hosts and the pool's receivers
their ACLs. It refuses when the pool's records do not fit the
[record limit](configuration.md#record-limit) with the tenant's feed and
transcript topic counted, or when the `retention.ms` of either is below the
pool's `max_queue_deadline` plus six hours, and records nothing. It applies
each host's [quotas](configuration.md#quotas).

### `wherry admin host add HOST --tenant TENANT --bundle-file FILE`

Records the host, creates its principal, its key topic and the tenant
feed and transcript topic, the latter two with `max.message.bytes` 2 MiB, grants its ACLs, applies its quotas, and writes
its bundle.

### `wherry admin receiver add RECEIVER`

```sh
wherry admin receiver add RECEIVER --pool POOL \
  <--config-file FILE|--config-dir DIR> --plugin-sha256 TYPE=HEX... <--bundle-file FILE|--dry-run>
```

Reads the [receiver configuration](configuration.md#receiver-configuration) from a
directory or a single file, validates it against the pool's terms, records
it, creates the receiver's principal, key topic and progress topic,
`wherry.receiver.<receiver>.progress`, grants its ACLs, and writes its
bundle. It creates the progress topic of every other live receiver in the
pool that lacks one, and sets the
[progress topic configuration](design.md#topics-1) on each existing one. It
refuses an existing receiver.
`--plugin-sha256 TYPE=HEX`, given once per plugin type, pins the SHA-256 of
that type's binary as lowercase hex: the line for the binary in the plugin
release's signed `checksums.txt`
([Install plugins](operations.md#install-plugins)). Every plugin type in the
configuration needs a pin, and a pin that is not 64 lowercase hex digits is
an error; either refusal writes nothing.
The bundle holds the receiver's Kafka identity alone; the receiver's [local
configuration](configuration.md#receiver-local-configuration) gives the
sources of its [platform credentials](configuration.md#platform-credentials).
With `--dry-run`, it makes the same checks and prints each plugin profile as
the record would hold it, with the pins, in the format of `receiver show`, and creates and
writes nothing; `--bundle-file` is then not needed.

### `wherry admin receiver update RECEIVER`

```sh
wherry admin receiver update RECEIVER <--config-file FILE|--config-dir DIR> --plugin-sha256 TYPE=HEX... [--force] [--dry-run]
```

Records a new configuration for a live receiver in its pool, with the
plugin pins as `receiver add` takes them. The record holds only the pins
the command gives, so give one for every plugin type each time. The
principal, password and bundle stay as they are. When the receiver reads a
record that differs from the configuration it runs, it stops claiming, lets
each run it holds end and commit with no time limit, then exits, and its
platform restarts it.

With `--force`, the record has the receiver release the tasks it holds at
once and exit, leaving their jobs for adoption. It ends a drain already
under way.

With `--dry-run`, it reads and validates the configuration as it does to
record it, against the receiver's pool's terms, and prints the resolution
in the format of [`receiver show`](#wherry-admin-receiver-show-receiver),
writing nothing to Kafka. A configuration it refuses fails in the same way
with or without `--dry-run`.

### `wherry admin receiver show RECEIVER`

Prints the receiver's recorded configuration as YAML: `capacity`, then
`plugin_sha256`, the SHA-256 pinned for each plugin type, then
`profiles`, each plugin profile resolved, in routing order, with its
`name`, `plugin`, `agent`, `labels` and merged `config`
([Plugin profile merge](configuration.md#plugin-profile-merge)):

```yaml
capacity: 3
plugin_sha256:
    nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
profiles:
    - name: claude
      plugin: nomad
      agent: claude
      labels:
        platform: nomad
      config:
        address: https://nomad.example.com:4646
        credential: nomad-token
        runtime:
            config:
                image: registry.example.com/claude-agent:1.0
            driver: docker
```

A `credential` key shows the credential's name, which is all the record
holds; neither `show` nor `--dry-run` prints a credential value.

### `wherry admin host revoke HOST --tenant TENANT`

Runs the [revocation](concepts.md#revocation) steps for the host, as
[`receiver revoke`](#wherry-admin-receiver-revoke-receiver) does for a
receiver.

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
is unbounded. `--wait DURATION` replaces that wait and must be positive.

### `wherry admin skip OFFSET --sha256 HASH`

Records the decision to pass over the undecodable record at `OFFSET` on
`wherry.admin`, whose SHA-256 is `HASH`. Loading `wherry.admin` fails while
any record on it does not decode, and the error lists each one as
`offset=<n> size=<n> sha256=<hex>`. A record whose `v` is higher than this
release knows is one a later release wrote; when every such record is one,
the error says to upgrade this wherry instead. The command refuses an offset
whose record decodes, a record a later release wrote, and a hash that does
not match.

### `wherry admin check`

Loads `wherry.admin`, checks that no two names collide, validates every
receiver configuration against its pool's terms, checks that every pool's
[records still fit the broker](configuration.md#record-limit), checks that every
tenant feed and transcript topic has a `max.message.bytes` of at least 2 MiB, checks topic retention and
timestamp type against the [bounds](design.md#topics-1), checks that each
live receiver's progress topic keeps records at least as long as its pool's
tasks topic, and checks that every ACL the
state calls for is present. It does not report ACLs beyond those, and does not check a
plugin profile's plugin or `config` section.

## Receiver command

### `wherry receiver run`

```sh
wherry receiver run [--config-file FILE] [--rotate-key]
```

Runs a receiver as the principal of the bundle its [local
configuration](configuration.md#receiver-local-configuration) names.
`--config-file` defaults to `/etc/wherry/config.yaml`. It creates the
configuration's `state_dir` with mode 0700 and keeps `signing.key` in it.
On start it:

1. settles a leftover `signing.key.next` as a host command settles
   [`<host_key>.next`](#wherry-key-rotate);
2. registers its public key on its key topic when the topic is empty, and
   fails when the topic holds another key;
3. with `--rotate-key`, replaces `signing.key` as
   [`wherry key rotate`](#wherry-key-rotate) replaces the host key, through
   `signing.key.next`, and logs `rotated key: old=<id> new=<id>`;
4. waits for its configuration record on `wherry.admin` and validates it
   against the pool's terms;
5. for each plugin profile in turn, resolves its [platform
   credential](configuration.md#platform-credentials), starts its
   [plugin](#plugins) from `plugin_dir` when no earlier profile has, and
   configures the profile;
6. reads its pool's events once, and starts advertising and claiming.

It logs its own lines to standard output with the prefix `receiver: `;
plugin and link lines are hclog lines named `plugin`. Plugin lines, the
plugin's raw standard error that go-plugin relays, and profile configure,
sweep and readiness errors carry the
[marker](a2a-extension-v1.md#seal) of each credential value
delivered to a run the receiver holds, and of each [platform
credential](configuration.md#platform-credentials) value, in place of the
value. On
`SIGINT` or `SIGTERM` it stops claiming, closes its idle claims and lets
each held run go on for up to the local configuration's
[`drain_timeout`](configuration.md#drain-timeout-and-the-stop-window),
default `0s`. It then commits the pending transcript of each run still
held, releases their tasks and exits, leaving their jobs running; set the
platform's stop window a few seconds above `drain_timeout`. When a
configuration record that differs from its own appears, it stops claiming,
lets each held run end with no time limit and fails with
`receiver configuration changed`; after a record `receiver update --force`
wrote, it releases the tasks it holds at once instead. A configuration record whose plugin is not a
valid plugin name ends it with
`configuration: profiles need unique valid names and a valid plugin`.
It refuses to start after its membership end.

With `metrics_addr` set it serves its [metrics](#metrics) at `/metrics` on
that address, and fails at start when it cannot listen there. Without it
it serves none.

### Plugins

The receiver ships with no plugins. For each plugin in its configuration
it runs `wherry-plugin-<type>` from the plugin directory, one process per
plugin, and configures every plugin profile of that plugin in it.
[wherry-plugins](https://github.com/benemon/wherry-plugins) holds the
plugins, `nomad` among them.

The plugin directory is made absolute and `PATH` is never searched. The
binary may be a symbolic link; the receiver resolves it and runs the file it
names. Before every start and restart,
[go-plugin's `SecureConfig`](https://pkg.go.dev/github.com/hashicorp/go-plugin#SecureConfig)
checks the binary's SHA-256 against the pin the receiver's record holds for
its type (`--plugin-sha256` on
[`receiver update`](#wherry-admin-receiver-update-receiver)). The receiver
refuses a plugin whose type has no pin, and a binary whose SHA-256 differs,
exiting with
`profile <name>: plugin type <type>: <path> has SHA-256 <hex> and the pin is <hex>`.
A type with no binary in the directory is a configuration error, and the
receiver exits with `profile <name>: no plugin for type <type>`. A plugin checks its profile's
`config` section when the receiver calls its `Configure`, and the receiver exits with
`profile <name>: ...` on an error.

A plugin inherits the receiver's environment. The receiver passes each
profile's name, pool, agent and merged `config` section to the plugin, less
the `credential` key, and the value of the profile's [platform
credential](configuration.md#platform-credentials) when it names one. A
profile with a `credential` name its local configuration has no source for
fails its configuration with an error naming the profile and the
credential.

A plugin also reports, for each profile, the paths of the input, output and
credentials directories as its agent sees them, which the receiver writes
into the prompt, and implements `InlineLimit` to report the most inline input
bytes it delivers. Zero declares no profile limit. Dispatch carries a run's
input files and every delivered credential with the prompt, and a fetch for
each S3 input: its name, presigned URL,
size and SHA-256, which the plugin fetches and verifies before the agent
starts, reporting a job that never started with class `fetch` when it
cannot. A plugin that cannot deliver an input reports it, and the run ends
`TASK_STATE_FAILED` naming it. Once the job ends the receiver
stops it and asks the plugin to collect its outputs, which arrive over
the link in pieces of at most 1 MiB. The outputs the receiver names for
streaming, its S3 outputs, the plugin streams as the receiver reads them.

A plugin gives each agent the `git` environment that
[Git input](a2a-extension-v1.md#git-input) specifies, through
`pluginsdk.AgentEnvironment` and `pluginsdk.AgentUnset`.
[Git repositories](operations.md#git-repositories) states its limits.

The receiver and a plugin talk gRPC over
[go-plugin](https://github.com/hashicorp/go-plugin) with mutual TLS on a
certificate pair made for each start, plugin protocol version 10. The receiver refuses a
plugin of any other protocol version at its start, so the receiver and its
plugins upgrade together. Configure carries the profile's platform
credential, when it names one, and no agent credential. Dispatch carries
every credential of the run as a `pluginsdk.Secret` holding its name, its value
and exactly one target: `Env`, an environment variable the plugin sets for the
agent, or `File`, a file it writes in the agent's credentials directory.
The receiver refuses a version 9 plugin with
`incompatible API version with plugin. Plugin version: 9, Client versions: [10]`.
The receiver refuses a plugin that offers no TLS certificate. Every call names
its profile. A plugin accepts messages up to 64 MiB; the receiver logs a larger
Dispatch as `dispatch rejected: message too large`.

Each advertisement gives every profile's sweep and readiness call one
advertise interval, 30 seconds; a call that runs out counts as not ready
and is logged. Each offer carries the profile's declared inline input limit;
negative limits are invalid.

Each scrape of `/metrics` calls every profile's `Metrics` at once, each within
2 seconds. A plugin answers with samples of the `wherry_plugin_` families,
each a family name, labels, and a counter or gauge value or a histogram. A
histogram gives the cumulative count at each bound of
`pluginsdk.PlacementBuckets`, its sum and its count. The receiver drops a
sample, and counts it in `wherry_metrics_dropped_total` by reason, when:

- its family is not one of the four (`unknown_family`);
- it has a label key the family lacks (`unknown_label`);
- a label value is outside the family's set (`label_value`);
- its kind is not the family's (`kind`);
- its value is not finite, a counter is negative, or a histogram has other
  buckets (`value`);
- it repeats a series of the same answer (`duplicate`).

It sets every sample's `profile` label to the name of the profile it
answered for, so a plugin cannot report for another profile. A call that fails
or runs out is counted as `error` or `timeout`, and the scrape serves that
profile's samples from its last answer.

The receiver pings each plugin every 10 seconds and kills one that fails
the ping or does not answer within 5 seconds, logging
`plugin did not answer a ping; killing it`. Each Configure call has 30
seconds. When a plugin exits, the receiver logs `plugin exited; restarting` as a
`plugin` line, starts it again after one second, waiting twice as long after each failed start up to 30
seconds, and configures every profile of the plugin again. Until then those
profiles are not ready and the next advertisement withdraws their offers. A run
whose job the receiver is following reads again from its last offset once the
plugin is back; when the plugin stays down for one lock duration, the run fails
with `log stream failed`.

A plugin's `Follow` may fail at any time; the receiver recovers the stream,
so a plugin need not. When `Follow` fails otherwise, the receiver asks
`Lookup` for the run's job and logs
`run <id>: follow failed (<class>); following again from offset <offset>`
once per run for each class. A running job (`job running`) it follows
again from the end of the last line it took, as it does a failed `Lookup`
(`lookup failed`) it retries, within one lock duration from the first
failure, which link losses share. An ended job (`job ended`) it follows once
more from that line end: a plugin's `Follow` on an ended job reads the rest of
the log and returns its `Terminal`. A job that never started is a `platform`
start failure. Otherwise the run fails with `log stream failed`.

A receiver with no ready profile advertises with no offers, so it stays
`live` with every profile `ready=false`, and placement passes over it. A
job its plugin reports never started is a `platform` start failure: the
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

A plugin returns a failed platform call as a `pluginsdk.Error`, which names
the operation (`Op`) and the platform's failure class (`Class`) beside the
error, and crosses the link as a gRPC status detail. The receiver logs a
plugin call's error as `op=<op> class=<class>: <text>`, without
`class=` when the plugin gives none, or as its text alone when the error
has no operation, under the rules for a
reason above. A run's calls (dispatch, lookup, follow, stop, collect, purge
and release) replace each credential value delivered to the run with its
redaction marker; an adopter's lookup comes before its seal and has no values
yet. The pool's calls (sweep, readiness and configure) belong to no run and
replace each value delivered to a run the receiver holds and each platform
credential value. Every call's text has its URLs replaced, its line breaks
turned into spaces and is cut to 512 bytes. A profile's configure error ends
the receiver with `profile <name>: <text>`.

A receiver adopts a run's existing job only when the profile and agent
recorded in its platform metadata match the profile that finds it. It
leaves a job whose profile is another of its profiles to that profile, and
stops and purges
any other job under the run's ID and dispatches the run again.

The protocol is the Go module `github.com/benemon/wherry/pluginsdk` in this
repository. [`plugin.proto`](https://github.com/benemon/wherry/blob/main/pluginsdk/proto/wherry/plugin/v1/plugin.proto)
defines the service. `pluginsdk.Serve` runs a plugin and gives its factory
the logger whose JSON lines the receiver relays at their level. A plugin sets
`pluginsdk.Version` before `Serve`, and the receiver logs it when it starts
the plugin. `pluginsdktest` is the conformance suite a plugin runs against its
platform.

### Metrics

A receiver with `metrics_addr` set serves its metrics in the Prometheus
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
A run ended without a plugin profile, one given back at the delivery budget
before any profile took it, has `profile="none"` in `wherry_runs_total` and
`wherry_delivery_budget_exhausted_total`.

| Family | Type | Labels | Counts |
| --- | --- | --- | --- |
| `wherry_claims_total` | counter | `tenant` | claimed tasks whose submission verifies |
| `wherry_admissions_total` | counter | `tenant`, `profile` | tasks admitted, by the profile they were routed to |
| `wherry_rejections_total` | counter | `tenant`, `reason` | rejections, by reason |
| `wherry_runs_total` | counter | `tenant`, `profile`, `agent`, `model`, `state` | runs ended `completed`, `failed` or `canceled` |
| `wherry_delivery_budget_exhausted_total` | counter | `tenant`, `profile` | runs ended `failed` with `delivery budget exhausted` |
| `wherry_run_duration_seconds` | histogram | `tenant`, `profile`, `agent`, `model`, `state` | time from this attempt's claim to the run's terminal state; an adopted run counts the adopting attempt only |
| `wherry_queue_wait_seconds` | histogram | `tenant`, `profile` | time from submission to first claim |
| `wherry_adoptions_total` | counter | `tenant`, `profile` | runs that adopted an earlier attempt's job |
| `wherry_redeliveries_total` | counter | `tenant`, `profile` | runs executed on a redelivery |
| `wherry_in_flight` | gauge | `profile` | tasks held |
| `wherry_capacity` | gauge | none | the configured capacity |
| `wherry_runs_past_deadline` | gauge | `profile` | tasks held past their run's deadline that have not ended |
| `wherry_advertised_timestamp_seconds` | gauge | none | Unix time of the latest advertisement Kafka acknowledged, 0 before the first |
| `wherry_start_failures_total` | counter | `profile`, `class` | start failures: `input-too-large`, `dispatch`, `platform`, and `offer-unavailable`, which counts a release by a member with no matching profile, has no start-failure record and carries `profile="none"` |
| `wherry_dispatch_outcomes_total` | counter | `profile`, `outcome` | dispatches: `created`, `nothing`, `unknown` |
| `wherry_stop_failures_total` | counter | `profile` | failed `Stop` calls |
| `wherry_purge_failures_total` | counter | `profile` | failed `Purge` calls |
| `wherry_sweep_failures_total` | counter | `profile` | failed `Sweep` calls |
| `wherry_collection_failures_total` | counter | `profile`, `reason` | runs whose outputs were not collected: `not_stopped`, `over_limits`, `timeout`, `failed` |
| `wherry_plugin_restarts_total` | counter | `type` | plugin restarts |
| `wherry_seal_waits_total` | counter | `tenant`, `outcome` | seal waits: `sealed`, `expired`, `unanswered`, `adopter_refused` |
| `wherry_seal_wait_seconds` | histogram | `tenant` | time a run waited for its seal, whatever the outcome |
| `wherry_redactions_total` | counter | `tenant`, `profile` | credential values replaced in published output |
| `wherry_bus_errors_total` | counter | `op` | Kafka errors: `read`, `write`, `transactional ID`, `share group` |
| `wherry_denials_total` | counter | `op` | Kafka authorisation errors, by the capability denied: `read`, `write`, `transactional ID`, `share group`, `membership` |
| `wherry_renewal_failures_total` | counter | none | failed claim renewals |
| `wherry_transaction_aborts_total` | counter | none | transactions aborted after a failed produce |
| `wherry_ambiguous_commits_total` | counter | none | commits with an unknown outcome, read back from the pool's events |
| `wherry_fenced_total` | counter | none | runs given up to a newer holder |
| `wherry_metrics_dropped_total` | counter | `profile`, `reason` | plugin samples dropped and `Metrics` calls that failed |
| `wherry_plugin_platform_errors_total` | counter | `profile`, `class` | platform API errors: `unavailable`, `denied`, `conflict`, `timeout`, `other` |
| `wherry_plugin_cleanup_pending` | gauge | `profile` | the profile's ended jobs awaiting cleanup |
| `wherry_plugin_sweep_deletions_total` | counter | `profile`, `kind` | material a sweep deleted: `job`, `variable`, `directory` |
| `wherry_plugin_placement_seconds` | histogram | `profile` | time from dispatch to the agent task starting |
| `wherry_plugin_storage_free_bytes` | gauge | `profile` | free bytes of the storage the profile's runs share |

A rejection's `reason` is its fixed text, such as `run expired`. A reason that
quotes the task counts as `git remote host outside the pool's vcs hosts` or
`pool has no storage`.

A seal wait is `expired` when the run's deadline passes, `unanswered` when
the 5-minute wait ends and the run fails, and `adopter_refused` when an
adopter's wait ends and it stops the job before the run fails.

The core histograms' buckets are 1, 5, 15, 30 and 60 seconds, 2, 5, 10 and 30
minutes, and 1, 2, 4 and 6 hours. `wherry_plugin_placement_seconds` uses
`pluginsdk.PlacementBuckets`: 1, 2, 5, 10 and 30 seconds, and 1, 2, 5, 10 and 30
minutes.
