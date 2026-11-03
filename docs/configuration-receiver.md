# Receiver configuration

## Receiver local configuration

A YAML file at `/etc/wherry/config.yaml`, or at the path
`wherry receiver run --config-file` names, read when the receiver starts. It is decoded as the
client configuration is: unknown and duplicate fields are errors, a decode
error names only its line numbers, and a leading `~/` is expanded in the
flag and in every path.

| Field | Default | Content |
| --- | --- | --- |
| `bundle` | required | the receiver's bundle |
| `ca_file` | none | PEM CA certificates the receiver trusts the broker with; with none, the system's trusted roots |
| `state_dir` | `state` in the bundle file's directory | the receiver's signing key and, while it holds any, its pool's keys in `pool-keys/` ([Pool keys](concepts.md#pool-keys-and-the-key-custodian)) |
| `plugin_dir` | `/usr/local/libexec/wherry/plugins` | the directory holding the `wherry-plugin-<type>` binaries, or symbolic links to them; each starts only when its SHA-256 is the receiver record's pin for its type |
| `metrics_addr` | none | `HOST:PORT` to serve [`/metrics`](operations-receivers.md#metrics) on; without it the receiver serves no metrics |
| `drain_timeout` | `0s` | how long the runs the receiver holds may go on after `SIGINT` or `SIGTERM` before it releases their tasks for adoption, a duration such as `20s`; [set it with the platform's stop window](#drain-timeout-and-the-stop-window) |
| `credentials` | none | [platform credential](#platform-credentials) name to its [source](security.md#credential-sources), exactly one of `file: PATH`, `env: VAR` and `command: [PROGRAM, ARG...]`, validated and resolved as the host's |

### Drain timeout and the stop window

`drain_timeout` and the platform's stop window are set together: Nomad's
[`kill_timeout`](https://developer.hashicorp.com/nomad/docs/job-specification/task#kill_timeout),
Kubernetes' [`terminationGracePeriodSeconds`](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
or systemd's [`TimeoutStopSec`](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html#TimeoutStopSec=).
On `SIGINT` or `SIGTERM` the receiver stops claiming, closes its idle claims
and lets the runs it holds go on for up to `drain_timeout`. It then commits
the pending transcript of each run still held and releases its task for
adoption, leaving the job running. The stop fixes one deadline,
`drain_timeout` plus 5 seconds after the signal: each held run's commit, its
`released` status and the acknowledgement to the broker, and the key
custodian's [drain](design.md#key-custody), its re-key
included, all end by it. A run whose commit does not complete by then is
left unacknowledged, and the broker returns its task when its acquisition
lock expires.
The receiver cannot read the window, so `drain_timeout` plus 5 seconds stays
a few seconds below it: `20s` ends a stop within 25 seconds, inside
Kubernetes' default 30-second window. At `0s` the
receiver releases its held tasks at once, and a key custodian leaves without
re-keying its tasks.

The drain timeout is also the time a stopping key custodian waits for a
successor's snapshot before it re-keys its tasks to the successor's key. It
waits whether or not a successor will come, so a custodian stopped alongside
the pool's last other receiver, as in a scale to zero or a whole-pool stop,
waits out `drain_timeout` within the stop window. A custodian stopped with
`0s`, or killed before its successor publishes, keeps its keys. The
successor publishes a `takeover` either way, which raises
[WherryCustodianTakeover](runbooks.md#wherrycustodiantakeover)
([Pool keys](concepts.md#pool-keys-and-the-key-custodian)).

Planned changes go through
[`receiver apply`](operations-operator.md#change-a-receiver-s-configuration), which
drains with no time limit.
[`receiver remove`](operations-operator.md#revoke-a-host-or-receiver) is a hard stop.
A Kafka authorisation error or a plugin process's exit also stops the
receiver at once, with no drain and no key custodian re-key, leaving its
runs' jobs for an adopter. The receiver exits non-zero and relies on its
platform to restart it: Nomad's `restart` block, a Kubernetes
`restartPolicy` or systemd's `Restart=on-failure`.

### Receiver local configuration example

The local configuration of receiver `nomad-1`, setting every field, with a
source for the `nomad-token` credential the [receiver configuration
example](#receiver-configuration-example) names:

```yaml
bundle: /etc/wherry/receiver.bundle
# A private CA the broker's certificate chains to; without ca_file, the system roots.
ca_file: /etc/wherry/kafka-ca.pem
state_dir: /var/lib/wherry/state
plugin_dir: /usr/local/libexec/wherry/plugins
metrics_addr: ":9464"
drain_timeout: 20s
credentials:
  nomad-token:
    file: /etc/wherry/nomad-token
```

## Receiver configuration

What `receiver apply` reads, recorded on
`wherry.operator` but for `broker`: a
[directory](#receiver-configuration-directory) or a single YAML file with the
same entries in maps and lists, named by `-f`, or the single file
`receivers/<receiver>.yaml` in the operator's
[working directory](configuration-operator.md#operator-working-directory). The receiver's name comes
from the command. Unknown fields are errors.

| Field of the single file | Content |
| --- | --- |
| `pool` | the receiver's pool; required for a new receiver, and an existing one's must match |
| `plugin_sha256` | for each plugin type, the SHA-256 of its binary as 64 lowercase hex digits: the line for the plugin binary in its release's signed `checksums.txt` ([Install plugins](operations-receivers.md#install-plugins)) |
| `broker` | optional, default the broker in `operator.yaml`: the Kafka address the receiver's bundle names, which the receiver connects to |
| `capacity` | optional, default 1: runs the receiver holds at once, positive |
| `plugins` | one or more plugin entries, keyed by plugin |

A plugin entry's key is a [plugin](plugins.md) name; the receiver
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
the order of their names, so the host and the receiver compute the same
match. Labels choose between plugins. The record on `wherry.operator` holds each profile with
its name, plugin, agent, labels and merged `config`.

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

Here the plugin holds the cluster's address, its credential, a Docker
runtime and a `platform` label. Plugin profile `claude` overrides only the
image. Plugin profile `host` switches the driver to `exec` and deletes the
inherited `image`, which the `exec` driver does not take. The
[Nomad plugin's settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
describe `runtime` and its driver `config`.

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
`wherry operator receiver apply ID --dry-run` prints what a file resolves
to, with the pins, and `wherry operator receiver show ID` prints what a receiver's record
holds ([commands](commands.md#wherry-operator-receiver-show-receiver)).

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
| `receiver.yaml` | `pool`, `plugin_sha256`, `broker` and `capacity` |
| `plugins/<type>/plugin.yaml` | optional: the plugin's `labels` and `config` |
| `plugins/<type>/profiles/*.yaml` | one plugin profile entry; routing order within the plugin is the lexical order of the file names |

Unknown fields, a repeated profile name, a plugin with no profile files, and
any other file or directory are errors. Files and directories whose names
start with `.` are ignored. The command writes nothing when the directory
has an error.

### Receiver configuration example

Receiver `nomad-1` in pool `build`, against the
[pool terms example](configuration-operator.md#pool-terms-example): one Nomad plugin and two
plugin profiles in routing order, `container` running its agent in a container and
`host` running its agent as an isolated process on the host. The plugin's
`config` holds the cluster's address and the name of its token, and each
plugin profile's `config` holds only what the example needs; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
lists every setting. Neither profile names an agent credential: each
delivers its agent's credentials at the targets the pool's terms give them.
As one file for
`receiver apply nomad-1 -f nomad-1.yaml`, or kept as `receivers/nomad-1.yaml`:

```yaml
pool: build
plugin_sha256:
  nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
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
`receiver apply nomad-1 -f nomad-1`:

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

`receiver.yaml`, with the single file's `pool`, `plugin_sha256`, `broker`
and `capacity`:

```yaml
pool: build
plugin_sha256:
  nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
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

A plugin profile's `config` section belongs to its plugin. The
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

A plugin profile that names no credential leaves the plugin to its own
default, such as the Nomad plugin's `NOMAD_TOKEN`.

Sources stay on the receiver's host. `wherry.operator`, which every principal
reads, records the credential's name alone, and the receiver's bundle holds
its Kafka identity alone. The [Quickstart](quickstart-production.md#_5-write-the-receiver-s-local-configuration)
writes a local configuration with a source for each platform credential its
receiver configuration names.

From the moment it resolves a value until it exits, the receiver replaces
the value in the plugin lines and plugin standard error it logs and in the
configure, sweep and readiness errors it logs, in the two forms it replaces
a [run credential](security.md#credential-envelopes) in: the bytes without
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
[agent's terms](configuration-operator.md#agent-credentials) or the task name.
`WHERRY_INPUT_DIR`, `WHERRY_CREDENTIALS_DIR` and `WHERRY_OUTPUT_DIR` hold
the paths of its [input, credential and output
directories](a2a-extension-v1.md#inputs-and-outputs). `WHERRY_MODEL` holds
the task's model when its agent has [`models`](configuration-operator.md#pool-terms), and is not set
otherwise, so the plugin profile's command reads it with its own fallback, such as
`test -n "$WHERRY_MODEL" || WHERRY_MODEL=haiku;` before
`--model "$WHERRY_MODEL"`.

The agent writes JSON lines to standard output. The receiver publishes each
line as a transcript chunk and reads nothing from them.

The prompt tells every agent to write, when it finishes, whatever happened,
an A2A `TaskStatus` JSON object to `$WHERRY_OUTPUT_DIR/.wherry/outcome.json`.
It names each credential file's path and each credential environment variable,
and lists the run's credential names, marking each optional credential the
host did not supply, as in
`This run's credentials are openai-api-key, github-token (not supplied).`
It then says that the answer goes in the message text, that long or
structured content belongs in files in the output directory, and that a reply
over the pool's `reply_limit` bytes is cut:

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
| valid | any | in its state, `TASK_STATE_FAILED` for `TASK_STATE_AUTH_REQUIRED`, with its message as the status message and its text as the result |
| missing or invalid | non-zero or a signal | `TASK_STATE_FAILED`, with `agent exited <code>` or `agent ended by <signal>`, followed by the platform's reason in parentheses when the plugin gives one, as in `agent exited 137 (OOMKilled)` |
| missing or invalid | 0 | `TASK_STATE_FAILED`, with `agent reported no outcome` |

A file is invalid when it is over the pool's `output_limit`, does not decode
as a `TaskStatus`, names another state, has no message or a message whose role
is not `ROLE_AGENT`, or reports `TASK_STATE_AUTH_REQUIRED` without a
`credential` part holding a credential name. The status message has one text
part, the message's text parts joined by newlines, or, when it has no text
part, the JSON of its first `data` part other than the `credential` part, and
the `credential` part. The receiver drops every other part of the message:
`raw` and `url` parts and further `data` parts. The result carries a reply
from a `data` part as a `data` part. That text is the
[reply](a2a-extension-v1.md#the-reply), redacted whole: over
64 KiB once redacted, the status message and the result carry its first
64 KiB, and the result's reply chunks carry it up to the pool's `reply_limit`.
An outcome file reporting `TASK_STATE_AUTH_REQUIRED` ends the run
`TASK_STATE_FAILED` with the `credential` part. The run is not resumed; the
tenant supplies the credential and submits again. A required
output that is not returned fails the run with
`required output <name> not returned: <reason>` as the status message,
whatever the file reports; when the file is missing or invalid and the exit
is non-zero or a signal, the message keeps that cause in front, as in
`agent exited 137 (OOMKilled); required output report.md not returned: missing`.
When the outputs are not collected, the receiver asks the plugin for the
file alone, within another 2 minutes, reading no other output.

`.wherry/` beneath the output directory is reserved: nothing in it is
collected as an output, and no output name leads into it, since no segment
of an output name starts with a dot.

### Task bounds

A run has two bounds, each a whole second since the epoch in UTC (a JWT
NumericDate): `task_started_by`, by which its agent job must start, and
`task_ended_by`, by which it must reach a terminal state. `wherry submit`
and the MCP `submit` tool take them as `--start-by` and `--end-by`, or
`start_by` and `end_by`, an RFC 3339 time or a Go duration from now, `10m`
and `2h` by default.

A run's time is capped at six hours after `task_started_by`. Before they
publish, `submit` and the MCP tool refuse a `task_ended_by` later than that,
and a `task_started_by` later than the pool's `start_by_limit` from now with
`task_started_by is past the pool's start_by_limit`, the MCP tool with
`INVALID_ARGUMENT`. Every run ends within six hours after `start_by_limit`.

The receiver's clock decides both bounds, and a bound fails only when that
clock is past its second. [Admission](a2a-extension-v1.md#admission) gives
the receiver's checks and the texts a run ends with, and
[Open request](a2a-extension-v1.md#open-request) the bounds of a claimed
run.

