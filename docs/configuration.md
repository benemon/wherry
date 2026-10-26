# Configuration

The files wherry reads and writes, and the environment variables it takes.
[Security](security.md) covers credential sources and git credentials.

## Client configuration

A YAML file at `~/.config/wherry/config.yaml`, or at the host command's
`--config-file` path. Unknown and duplicate fields are errors, and a decode
error names only its line numbers. A leading `~/` is expanded in the flag and
in every path.

| Field | Default | Content |
| --- | --- | --- |
| `bundle` | required | the host's bundle |
| `host_key` | `~/.config/wherry/host.key` | host signing key file |
| `state_dir` | `~/.config/wherry/state` | run records and answered seal requests |
| `credentials` | none | credential name to its [source](security.md#credential-sources), exactly one of `file: PATH`, `env: VAR` and `command: [PROGRAM, ARG...]` |
| `git` | none | `{url, credential, host_key}` entries, each giving a task whose git remote starts with `url` the credential `credential` and the OpenSSH `known_hosts` file `host_key`, both optional ([git credentials](security.md#git-credentials)) |
| `storage` | none | `{access_key, secret_key}`: the names of the credentials in `credentials` holding the access key and secret key of the host's S3 credential; a host without it uses no S3 lane |
| `input_roots` | the MCP server's working directory | absolute paths of existing directories the MCP server sends inputs from; the CLI reads any path it is given |

`state_dir` holds `runs/<run>.json` for each run this host submitted,
readable by its owner alone, with `redaction_key`, the run's redaction key,
made when this host submits the run, `runs/<run>/outputs/` for the outputs
`await` writes by default, and `answered/<run>.<attempt>` for each attempt
it answered, with a `.lock` beside it that every process sharing the
directory holds while it answers that attempt. `submit` writes the run's
record before it publishes the task, and removes it when publishing
fails. A seal is at most 64 KiB, which bounds the values one seal
carries. The receiver redacts no credential value shorter than 8 bytes
once its trailing CR and LF are removed, and logs
`run <id>: credential <name> too short to redact`.

### Client configuration example

The configuration of host `laptop`, setting every field:

```yaml
bundle: ~/.config/wherry/laptop.bundle
host_key: ~/.config/wherry/laptop.key
state_dir: ~/.local/state/wherry
credentials:
  claude-oauth-token:
    file: ~/.config/wherry/credentials/claude-oauth-token
  model-api-key:
    file: ~/.config/wherry/credentials/model-api-key
  openai-api-key:
    command: [op, read, "op://wherry/openai/credential"]
  github-token:
    env: GITHUB_TOKEN
  deploy-key:
    file: ~/.config/wherry/credentials/deploy-key
  s3-access-key:
    file: ~/.config/wherry/credentials/s3-access-key
  s3-secret-key:
    command: [op, read, "op://wherry/s3/secret-key"]
git:
  - url: https://github.com/acme/
    credential: github-token
  - url: ssh://git@gitlab.example/
    credential: deploy-key
    host_key: ~/.config/wherry/known_hosts/gitlab
input_roots: [~/src, ~/notes]
storage:
  access_key: s3-access-key
  secret_key: s3-secret-key
```

`credentials` holds a source for each credential the agents of the
[pool terms example](#pool-terms-example) take: two files, a password
manager's command for `openai-api-key`, and the answering process's
`GITHUB_TOKEN` for `github-token`, which `git` also gives every task cloning
under `https://github.com/acme/`. `storage` takes the S3 access key from a
file and the secret key from the password manager. Each source resolves each time a receiver
asks for its credential.

## Receiver local configuration

A YAML file at `/etc/wherry/config.yaml`, or at the path
`wherry receiver run --config-file` names, read when the receiver starts. It is decoded as the
client configuration is: unknown and duplicate fields are errors, a decode
error names only its line numbers, and a leading `~/` is expanded in the
flag and in every path.

| Field | Default | Content |
| --- | --- | --- |
| `bundle` | required | the receiver's bundle |
| `state_dir` | `state` in the bundle file's directory | the receiver's signing key |
| `plugin_dir` | `/usr/local/libexec/wherry/plugins` | the directory holding the `wherry-plugin-<type>` binaries |
| `metrics_addr` | none | `HOST:PORT` to serve [`/metrics`](operations.md#metrics) on; without it the receiver serves no metrics |
| `credentials` | none | [platform credential](#platform-credentials) name to its [source](security.md#credential-sources), exactly one of `file: PATH`, `env: VAR` and `command: [PROGRAM, ARG...]`, validated and resolved as the host's |

### Receiver local configuration example

The local configuration of receiver `nomad-1`, setting every field, with a
source for the `nomad-token` credential the [receiver configuration
example](#receiver-configuration-example) names:

```yaml
bundle: /etc/wherry/receiver.bundle
state_dir: /var/lib/wherry/state
plugin_dir: /usr/local/libexec/wherry/plugins
metrics_addr: ":9464"
credentials:
  nomad-token:
    file: /etc/wherry/nomad-token
```

## Bundle

A YAML file that `wherry admin` writes with mode 0600. Unknown fields are
errors, and `broker`, `ca`, `principal` and `password` are required, a `ca`
with no certificate failing with `CA contains no certificates`; a decode
error names only its line numbers. A trailing line break ends the password
and is not part of it, and a password of nothing else is refused.

| Field | Content |
| --- | --- |
| `broker` | Kafka bootstrap address, `host:port` |
| `ca` | PEM CA certificate for the broker's TLS certificate |
| `principal` | SCRAM-SHA-512 user name |
| `password` | SCRAM password |

## Pool terms

The YAML file `pool create --config-file` reads. Unknown fields are errors.
With `--config-dir`, `pool.yaml` holds every field except `agents`, and
`agents/*.yaml` each hold one agent in file-name order. Hidden entries are
ignored. Any other entry and a directory with no agent files are errors.

| Field | Content |
| --- | --- |
| `agents` | one or more `{name, credentials, models, default_model}`: an agent, the [credentials](#agent-credentials) it takes, and optionally the models a task may name for it and the one a task naming none runs |
| `default_agent` | an agent from `agents`, used when a task names none; optional when `agents` holds one, which it then defaults to |
| `max_queue_deadline` | optional duration, default `336h` (14 days): the longest queue deadline a run may ask for, and the time from a run's submission past which a receiver rejects it unclaimed; it also sets the pool topics' retention |
| `delivery_limit` | optional, default 5: the share group's `share.delivery.count.limit` |
| `lock_duration` | optional duration, default `15s`: the share group's `share.record.lock.duration.ms` |
| `producer_byte_rate` | host quota, bytes per second |
| `request_percentage` | host quota, percentage of broker request time |
| `output_limit` | optional, default 8388608 (8 MiB): bytes of one output, and of a run's outputs together |
| `vcs` | optional, default none: the hosts a [git input](a2a-extension-v1.md#git-input) may clone from |
| `storage` | optional, default none: `{endpoint, bucket}`, the S3-compatible store of the pool's [S3 inputs and outputs](a2a-extension-v1.md#s3-inputs-and-outputs) |

An agent's `default_model`, when set, is one of its `models`; it defaults to
the model of an agent listing one, and an agent without `models` has none. A task for an agent with `models` runs the model
it names, or `default_model`; with no `default_model` it must name one, and
`submit` refuses it otherwise. One naming a model outside `models` fails `submit`, and a
receiver rejects one whose model has left the terms since with
`model not in the pool's terms`. A task for an agent without `models` names
no model, and its profiles run the model their command sets. wherry does not
interpret a model name.

### Agent credentials

Each entry of an agent's `credentials` is `{name, env, file, required}`: a
credential name, exactly one target, and whether a run needs it. `env` names
the environment variable the agent gets the value in, matching
`[A-Za-z_][A-Za-z0-9_]*` and none an adapter sets for the agent: `HOME`,
`WHERRY_INPUT_DIR`, `WHERRY_OUTPUT_DIR`, `WHERRY_CREDENTIALS_DIR`,
`WHERRY_MODEL` and the variables of `adapter.AgentEnvironment`. `file` names
the file it gets in `WHERRY_CREDENTIALS_DIR`, a [file
name](a2a-extension-v1.md#submission). `required` defaults to `true`. An
agent's credentials have unique names, and no two share a target; `pool
create` refuses terms that break a rule, naming the agent and credential.

`submit` refuses a task when its host has no source for a required
credential of the placed agent. A run goes without an optional credential
its host has no source for, and its [prompt](#agent-contract) names it as not
supplied. Every profile of the agent delivers the agent's credentials, each at
its target.

Each entry of `vcs` is a lowercase host name, optionally with a `:port` of
1 to 65535, and appears once. A remote's host drops its scheme's default
port, 443 for `https` and 22 for `ssh`, before it is matched, so an entry
names a port only when it is not that default; an entry with the default
port matches no remote. `git.example.com` matches
`https://git.example.com/...` and `ssh://git@git.example.com/...`;
`git.example.com:2222` matches `ssh://git@git.example.com:2222/...`. A pool
without `vcs` accepts no git input.

`storage.endpoint` is `https://` and a lowercase host name, optionally with
a `:port` of 1 to 65535, 443 when absent, and nothing else: no path, user information or
query. wherry speaks only https to it. `storage.bucket` is 3 to 63
lowercase letters, digits, dots and hyphens, starting and ending with a
letter or digit, with no `..`. The run's objects live under `wherry/<tenant>/<run>/` in
the bucket, addressed path-style, `https://<host>/<bucket>/<key>`. A pool
without `storage` accepts no S3 input or output.

No number or duration is negative, and a zero takes the field's default
where it has one; `producer_byte_rate` and `request_percentage` are
positive. Durations use Go syntax and are
stored in milliseconds. Set `lock_duration` above `10s`, since a
receiver renews its lock every 10 seconds; wherry does not check this. The
broker bounds it between
`group.share.min.record.lock.duration.ms` and
`group.share.max.record.lock.duration.ms`. Every delivery counts against
`delivery_limit`, including a release after a start failure and a release
by a member with no matching profile. From attempt
`delivery_limit` minus 1, a receiver ends a run `TASK_STATE_FAILED` with
`delivery budget exhausted` where it would release it, except in the cases
the [extension](a2a-extension-v1.md#delivery-budget) lists.

### Pool terms example

Terms for pool `build` that set every field, as one file for
`pool create build --config-file build.yaml`:

```yaml
agents:
  - name: claude
    credentials:
      - {name: claude-oauth-token, env: CLAUDE_CODE_OAUTH_TOKEN}
    models:
      - haiku
      - sonnet
    default_model: haiku
  - name: coder
    credentials:
      - {name: model-api-key, env: MODEL_API_KEY}
  # Without models, a task names no model and the profile's command picks one.
  - name: codex
    credentials:
      - {name: openai-api-key, env: CODEX_API_KEY}
      # A run whose host has no source for it goes without it.
      - {name: github-token, env: GH_TOKEN, required: false}
default_agent: claude
max_queue_deadline: 72h
# Each receiver with no matching profile spends one delivery, so this covers the pool's receivers plus a margin.
delivery_limit: 6
# Above the receiver's 10s renewal interval.
lock_duration: 30s
producer_byte_rate: 1048576
request_percentage: 10
output_limit: 16777216
vcs:
  - github.com
  # A port appears only when it is not the scheme's default.
  - git.example.com:2222
storage:
  endpoint: https://s3.example.com
  bucket: wherry
```

The same terms as a directory for `pool create build --config-dir build`:

```
build/
  pool.yaml
  agents/
    claude.yaml
    coder.yaml
    codex.yaml
```

`pool.yaml`:

```yaml
default_agent: claude
max_queue_deadline: 72h
# Each receiver with no matching profile spends one delivery, so this covers the pool's receivers plus a margin.
delivery_limit: 6
# Above the receiver's 10s renewal interval.
lock_duration: 30s
producer_byte_rate: 1048576
request_percentage: 10
output_limit: 16777216
vcs:
  - github.com
  # A port appears only when it is not the scheme's default.
  - git.example.com:2222
storage:
  endpoint: https://s3.example.com
  bucket: wherry
```

`agents/claude.yaml`:

```yaml
name: claude
credentials:
  - {name: claude-oauth-token, env: CLAUDE_CODE_OAUTH_TOKEN}
models:
  - haiku
  - sonnet
default_model: haiku
```

`agents/coder.yaml`:

```yaml
name: coder
credentials:
  - {name: model-api-key, env: MODEL_API_KEY}
```

`agents/codex.yaml`:

```yaml
# Without models, a task names no model and the profile's command picks one.
name: codex
credentials:
  - {name: openai-api-key, env: CODEX_API_KEY}
  # A run whose host has no source for it goes without it.
  - {name: github-token, env: GH_TOKEN, required: false}
```

The agent files are read in file-name order, so `coder` precedes `codex` in
both forms.

### Record limit

A record limit is the lowest of `message.max.bytes` across the brokers,
1000012 bytes, the most a wherry producer batches, and `max.message.bytes`
on the topic the record goes to: the pool's tasks topic for submissions,
and the feeds of the tenants entitled to the pool for chunks.
`pool create` records the tasks topic's record limit in the pool's terms,
and [`submit`](commands.md#wherry-submit) measures each submission against it. A pool file
does not set it.

`pool create`, `pool entitle` and `check` sign four records and measure
them as record batches: the largest output chunk, of 256 KiB, two largest
results, and the largest transcript chunk, a line of 512 KiB. Each result
has the run's message with its 256 input files named but not carried, and a
status message and a result text of 64 KiB each, both raw, as an agent's text
that JSON escaping would lengthen travels, with a `TASK_STATE_AUTH_REQUIRED`
status's `credential` part. One result lists
256 returned and 256 unreturned outputs with 512-byte names, the other 256 S3
outputs with their keys. A receiver's labels and an adapter's job name have
no bound and are left empty. Any one over its record limit refuses the terms,
naming its size and the limit.

An input file travels base64-encoded inside the signed record, which is itself
base64url-encoded, so an inline input takes about 16/9 of its size in the
submission record. As `pool create`'s check measures them on a broker at its
defaults, the output chunk is 468119 bytes, the results 791017 and 803627
bytes (with S3 outputs) and the transcript chunk 933508 bytes, so a feed's
`max.message.bytes` below 933508 refuses every pool.

`pool create` and `pool entitle` check against the broker at the time.
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
`--config-dir`, or a single YAML file named by `--config-file` with the same
entries in maps and lists. Exactly one is required. The receiver and pool
come from the command. Unknown fields are errors.

| Field of the single file | Content |
| --- | --- |
| `capacity` | optional, default 1: runs the receiver holds at once, positive |
| `plugins` | one or more plugin entries, keyed by adapter type |

A plugin entry's key is an [adapter](adapters.md) type name; the receiver
runs `wherry-plugin-<type>` from its plugin directory for it. Each plugin
entry:

| Field | Content |
| --- | --- |
| `labels` | optional; [labels](commands.md#labels) every plugin profile of the plugin inherits |
| `config` | optional; settings every plugin profile of the plugin inherits |
| `profiles` | one or more plugin profile entries, in routing order |

Each profile entry:

| Field | Content |
| --- | --- |
| `name` | profile name, unique in the receiver |
| `agent` | the agent it launches, from the pool's terms |
| `labels` | optional; [labels](commands.md#labels) for placement |
| `config` | the plugin's settings for this profile |

A plugin profile's `config` and `labels` are its plugin's merged with its
own, as [Plugin profile merge](#plugin-profile-merge) describes. A
`credential` key in the merged `config` names a [platform
credential](#platform-credentials), and its value is a string.

The receiver routes a run to the first ready profile, in order within its
plugin, whose agent and labels satisfy the task. It visits the plugins in
the order of their types, so the host and the receiver compute the same
match, but labels choose between plugins and no configuration relies on an
order between them. The record on `wherry.admin` holds each profile with
its name, plugin type, agent, labels and merged `config`.

### Plugin profile merge

The plugin holds the base configuration and each plugin profile states how
it differs. The merge of a plugin profile's `config` onto its plugin's
`config`, and of its `labels` onto its plugin's `labels`, follows these
rules:

- Maps merge recursively, and the plugin profile's value wins on every leaf
  it sets.
- A list on a plugin profile replaces the plugin's list whole.
- `key: null`, or `key: ~`, on a plugin profile removes the inherited key at
  that path before the plugin sees the configuration. A null with nothing
  inherited at its path is ignored. A null in a plugin's own `config` or
  `labels` is an error naming the plugin and the key path, such as
  `runtime.config.image`, and so is a null inside a list, such as
  `runtime.config.args[1]`, which has nothing to delete.
- A map at one level and a scalar or a list at the other, for the same key,
  is an error naming the plugin profile and the key path. A null is never
  such a clash.
- `credential` merges like any key: a default on the plugin and an exception
  on a plugin profile.
- `agent` belongs to the plugin profile alone.

No plugin receives a null. Here the plugin holds the cluster's address, its
credential, a Docker runtime and a `platform` label. Plugin profile `claude`
overrides only the image. Plugin profile `host` switches the driver to
`exec` and deletes the inherited `image`, which the `exec` driver does not
take: Nomad accepts a job whatever its driver configuration holds, and the
client then refuses the task, so an inherited key the driver rejects fails
every run.

```yaml
plugins:
  nomad:
    labels: {platform: nomad}
    config:
      address: https://nomad.example.com:4646
      credential: nomad-token
      runtime:
        driver: docker
        config: {image: registry.example.com/agent-base:1.0}
        resources: {cpu: 1000, memory_mb: 2048}
    profiles:
      - name: claude
        agent: claude
        config: {runtime: {config: {image: registry.example.com/claude-agent:1.0}}}
      - name: host
        agent: codex
        labels: {isolation: host}
        config:
          runtime:
            driver: exec
            config: {image: null, command: /usr/local/bin/codex-run}
```

Plugin profile `claude` runs with the address, the credential, the `docker`
driver, the resources and the label `platform: nomad`, and the image
`registry.example.com/claude-agent:1.0`. Plugin profile `host` runs with the
same address, credential and resources, the `exec` driver with `command`
alone in its driver configuration, and the labels `platform: nomad` and
`isolation: host`.
`wherry admin receiver configure ID --dry-run` prints what a file resolves
to, and `wherry admin receiver show ID` prints what a receiver's record
holds ([commands](commands.md#wherry-admin-receiver-show-receiver)).

### Receiver configuration directory

The directory holds:

```
receiver.yaml
plugins/
  <type>/
    plugin.yaml
    profiles/
      <profile>.yaml
```

| File | Fields |
| --- | --- |
| `receiver.yaml` | `capacity` |
| `plugins/<type>/plugin.yaml` | optional: the plugin's `labels` and `config` |
| `plugins/<type>/profiles/*.yaml` | one plugin profile entry; routing order within the plugin is the lexical order of the file names |

Unknown fields, a repeated profile name, a plugin with no profile files, and
any other file or directory are errors. Files and directories whose names
start with `.` are ignored. The command writes nothing when the directory
has an error.

### Receiver configuration example

Receiver `nomad-1` in pool `build`, against the
[pool terms example](#pool-terms-example): one Nomad plugin and two
profiles in routing order, `container` running its agent in a container and
`host` running its agent as an isolated process on the host. The plugin's
`config` holds the cluster's address and the name of its token, and each
plugin profile's `config` holds only what the example needs; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
lists every setting. Neither profile names an agent credential: each
delivers its agent's credentials at the targets the pool's terms give them.
As one file for
`receiver add nomad-1 --pool build --config-file nomad-1.yaml`:

```yaml
capacity: 3
plugins:
  nomad:
    # Every plugin profile of this plugin inherits these keys and can override them.
    config:
      address: https://nomad.example.com:4646
      ca_file: /etc/wherry/nomad-ca.pem
      namespace: agents
      credential: nomad-token
    profiles:
      - name: container
        agent: claude
        labels:
          isolation: container
        config:
          runtime:
            driver: docker
            config:
              image: registry.example.com/claude-agent:1.0
      - name: host
        agent: codex
        labels:
          isolation: host
        config:
          node_pool: codex
          runtime:
            driver: exec
            config:
              command: /bin/sh
              args:
                - -c
                - >-
                  exec codex exec --json --ephemeral --skip-git-repo-check
                  --sandbox workspace-write - < "$NOMAD_TASK_DIR/prompt.txt"
```

The same configuration as a directory for
`receiver add nomad-1 --pool build --config-dir nomad-1`:

```
nomad-1/
  receiver.yaml
  plugins/
    nomad/
      plugin.yaml
      profiles/
        10-container.yaml
        20-host.yaml
```

`receiver.yaml`:

```yaml
capacity: 3
```

`plugins/nomad/plugin.yaml`:

```yaml
# Every plugin profile of this plugin inherits these keys and can override them.
config:
  address: https://nomad.example.com:4646
  ca_file: /etc/wherry/nomad-ca.pem
  namespace: agents
  credential: nomad-token
```

`plugins/nomad/profiles/10-container.yaml`:

```yaml
name: container
agent: claude
labels:
  isolation: container
config:
  runtime:
    driver: docker
    config:
      image: registry.example.com/claude-agent:1.0
```

`plugins/nomad/profiles/20-host.yaml`:

```yaml
name: host
agent: codex
labels:
  isolation: host
config:
  node_pool: codex
  runtime:
    driver: exec
    config:
      command: /bin/sh
      args:
        - -c
        - >-
          exec codex exec --json --ephemeral --skip-git-repo-check
          --sandbox workspace-write - < "$NOMAD_TASK_DIR/prompt.txt"
```

A task runs on the first ready profile of its agent whose labels satisfy its
selector. Each profile here launches a different agent, so a task for
`claude`, the pool's default agent, runs on `container`, and
`--selector isolation=host` leaves it no profile.

### Profile settings

A profile's `config` section belongs to its plugin. The
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
lists its settings and the jobs it runs.

### Platform credentials

A merged plugin profile `config` may set `credential: <name>`, normally in
the plugin's `config` beside the address it authenticates. The name is a
string; any other value is an error naming the plugin profile when the
configuration is read. The receiver
resolves the name from the `credentials` of its [local
configuration](#receiver-local-configuration) when it configures the
profile, at start and after a configuration change, so a rotated value
takes effect on the receiver's next start. It trims a trailing CR and LF
from the value, removes the `credential` key from the `config` it sends the
plugin, and passes the value to the plugin in the profile's `Configure`. A
name with no source fails that plugin profile's configuration, and the
receiver exits with an error naming the plugin profile and the credential,
for plugin profile `container` of the [receiver configuration
example](#receiver-configuration-example):

```
profile container: credential "nomad-token" has no source in the receiver's configuration
```

Sources stay on the receiver's host and are never recorded on
`wherry.admin`. The [Quickstart](quickstart.md#_7-write-the-receiver-s-local-configuration)
writes a local configuration with a source for each platform credential its
receiver configuration names.

From the moment it resolves a value until it exits, the receiver replaces
the value in the plugin lines and plugin standard error it logs and in the
configure, sweep and readiness errors it logs, in the two forms it replaces
a [run credential](a2a-extension-v1.md#seal) in: the bytes without
trailing CR and LF, and those bytes escaped as a JSON string. The marker is
`[redacted:`, 16 base64url characters and `]`, from the HMAC-SHA256 under an
empty key of `wherry-redaction-v1` followed by the credential's name. A
value shorter than 8 bytes once trimmed is not replaced, and the receiver
logs `platform credential <name> too short to redact`.

### Agent contract

The agent receives the prompt in a file as its plugin documents; for Nomad, in
[the plugin's agent jobs](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-jobs).
It receives each of the run's credentials at its target: in the environment
variable or the file in `WHERRY_CREDENTIALS_DIR` the
[agent's terms](#agent-credentials) or the task name.
`WHERRY_INPUT_DIR`, `WHERRY_CREDENTIALS_DIR` and `WHERRY_OUTPUT_DIR` hold
the paths of its [input, credential and output
directories](a2a-extension-v1.md#inputs-and-outputs). `WHERRY_MODEL` holds
the task's model when its agent has [`models`](#pool-terms), and is not set
otherwise, so the profile's command reads it with its own fallback, such as
`test -n "$WHERRY_MODEL" || WHERRY_MODEL=haiku;` before
`--model "$WHERRY_MODEL"`.

The agent writes JSON lines to standard output. The receiver publishes each
line as a transcript chunk and reads nothing from them.

The prompt tells every agent to write, when it finishes, whatever happened,
an A2A `TaskStatus` JSON object to `$WHERRY_OUTPUT_DIR/.wherry/outcome.json`.
It names each credential file's path and each credential environment variable,
and lists the run's credential names, marking each optional credential the
host did not supply, as in
`This run's credentials are openai-api-key, github-token (not supplied).`:

```json
{"state": "TASK_STATE_COMPLETED", "message": {"role": "ROLE_AGENT", "parts": [{"text": "<the answer, or what went wrong>"}]}}
```

`state` is `TASK_STATE_COMPLETED`, `TASK_STATE_FAILED` or
`TASK_STATE_AUTH_REQUIRED`. With `TASK_STATE_AUTH_REQUIRED`, a further part
`{"data": {"credential": "<name>"}}` names the credential the agent needs,
as the [`credential` data part](a2a-extension-v1.md#the-credential-data-part)
defines it.

The run ends when the job ends. The receiver stops the job, collects its
outputs whatever its exit, and reads the outcome file:

| Outcome file | Exit | The run ends |
| --- | --- | --- |
| valid | any | in its state, with its message as the status message and its text as the result |
| missing or invalid | non-zero or a signal | `TASK_STATE_FAILED`, with `agent exited <code>` or `agent ended by <signal>` |
| missing or invalid | 0 | `TASK_STATE_FAILED`, with `agent reported no outcome` |

A file is invalid when it is over 512 KiB, does not decode as a
`TaskStatus`, names another state, has no message or a message whose role is
not `ROLE_AGENT`, or reports `TASK_STATE_AUTH_REQUIRED` without a
`credential` part holding a credential name. The status message keeps the
message's text parts, joined by newlines, and the `credential` part. Its text
is redacted like any agent text and becomes
`[redacted text over 65536 bytes]` when it passes 64 KiB once redacted.
`TASK_STATE_AUTH_REQUIRED` ends the run, which is not resumed; the tenant
supplies the credential and submits again. A required
output that is not returned fails the run with
`required output <name> not returned: <reason>` as the status message,
whatever the file reports. Outputs that are not collected leave the file
unread.

`.wherry/` beneath the output directory is reserved: nothing in it is
collected as an output, and no output name leads into it, since no segment
of an output name starts with a dot.

A run's time is capped at its deadline, and at six hours after its queue
deadline. `submit` refuses a deadline later than the queue deadline plus six
hours. A receiver rejects a run no pool member has claimed with
`queue deadline passed` or `run deadline passed` once that deadline has
passed. A redelivered run that a member has claimed is not rejected: the
receiver adopts its job when it finds one, and dispatches the run again when
it finds none. Past its run deadline, the receiver stops any job it finds and
ends the run `TASK_STATE_FAILED` with `timeout`.

A receiver measures a run's age from the broker's append time of its
submission. It rejects a run no pool member has claimed with `run expired`
when that time is more than `max_queue_deadline` ago, and with
`queue deadline exceeds the pool's max queue deadline` when the run's queue
deadline falls more than `max_queue_deadline` after it. A redelivered run
that a member has claimed skips both checks. `wherry submit` and the MCP
`submit` tool refuse, before publishing, a queue deadline longer than
`max_queue_deadline`, the MCP tool with `INVALID_ARGUMENT`. Every run
therefore ends within six hours after `max_queue_deadline`.

## Environment variables

| Variable | Read by | Effect |
| --- | --- | --- |
| `SSL_CERT_FILE` | host commands | PEM CA file that replaces the system roots, for a pool's S3 endpoint under a private CA; include the public roots in it when the host also reaches public endpoints |
| `WHERRY_TRUST_BROKER` | `wherry admin` | Kafka bootstrap address, also written into bundles |
| `WHERRY_TRUST_CA` | `wherry admin` | PEM CA file for the broker, also written into bundles |
| `WHERRY_TRUST_ADMIN_USER` | `wherry admin` | super user name |
| `WHERRY_TRUST_ADMIN_PASSWORD_FILE` | `wherry admin` | file holding the super user's SCRAM password |
| `WHERRY_TRUST_OPERATOR_USER` | `wherry admin` | operator principal name, `wherry.operator`, which is the default |
| `WHERRY_TRUST_OPERATOR_PASSWORD_FILE` | `wherry admin` | file holding the operator's SCRAM password |

A plugin reads its own variables from the receiver's environment; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#credentials)
lists its.

Trailing CR and LF are trimmed from password files.
