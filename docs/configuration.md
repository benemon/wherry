# Configuration

The files wherry reads and writes, and the environment variables it takes.
[Security](security.md) covers credential sources and git credentials.

## Client configuration

A YAML file at `~/.config/wherry/config.yaml`, or at the path `WHERRY_CONFIG`
names when it is set, or at the host command's `--config-file` path, which
wins over both. [`wherry host init`](commands.md#wherry-host-init) writes it;
a file written in advance is used as written. Unknown and duplicate fields are
errors, and a decode error names only its line numbers. A leading `~/` is
expanded in the flag and in every path.

The host's paths are the same on every OS, macOS included:

| Path | Holds |
| --- | --- |
| `~/.config/wherry/`, or `$XDG_CONFIG_HOME/wherry/` when that is set | `config.yaml`, `bundle.yaml`, `host.key` and, for a private CA, `ca.pem` |
| `~/.local/state/wherry/`, or `$XDG_STATE_HOME/wherry/` when that is set | the host's state, `state_dir` |

[`wherry dev up`](commands.md#wherry-dev-up) creates `config.yaml`,
`bundle.yaml` and `host.key` here for its `default` instance when none of
them is there and `WHERRY_CONFIG` is not set, with `state_dir` inside the
instance, and records what it created; `wherry dev down` removes only those, while each
is still the file `up` created. It never replaces a real host's files:
otherwise, and for every other instance, it writes them inside the
instance and prints the `WHERRY_CONFIG` to use.

| Field | Default | Content |
| --- | --- | --- |
| `bundle` | required | the host's bundle; `host init <bundle>` places it at `~/.config/wherry/bundle.yaml` and names it here |
| `ca_file` | `~/.config/wherry/ca.pem` if that file exists, else none | PEM CA certificates the host trusts the broker with; with none, the system's trusted roots |
| `host_key` | `~/.config/wherry/host.key` | host signing key file |
| `state_dir` | `~/.local/state/wherry` | run records |
| `credentials` | none | credential name to its [source](security.md#credential-sources), exactly one of `file: PATH`, `env: VAR` and `command: [PROGRAM, ARG...]` |
| `git` | none | `{url, credential, host_key}` entries, each giving a task whose git remote starts with `url` the credential `credential` and the OpenSSH `known_hosts` file `host_key`, both optional ([git credentials](security.md#git-credentials)) |
| `storage` | none | pool name to `{access_key, secret_key}`: the names of the credentials in `credentials` holding the access key and secret key of the host's S3 credential for that pool's bucket; a pool without an entry has no S3 lane on this host |
| `input_roots` | the MCP server's working directory | absolute paths of existing directories the MCP server sends inputs from; the CLI reads any path it is given |

`host init` writes `bundle`, a `ca_file` it asked for, and an entry in
`credentials`, `git` and `storage` for each answer, each a reference to a
source and never a value; a re-run keeps every entry and adds only what a
newly entitled pool needs. Without a terminal it asks nothing and lists what
still has no source.

`state_dir` holds `runs/<run>.json` for each run this host submitted,
readable by its owner alone, with the run's submission without its envelope
and wrap, kept to [resubmit](concepts.md#lost-pool-keys) the run if its key
is lost, and `redaction_key`, the run's redaction key, made when this host
submits the run; a replacement's record also holds its signed bytes,
envelope included. It holds no credential value in the clear. It also holds `runs/<run>/outputs/` for the outputs
`wait` writes by default, and `resubmit/<run>.json.lock`, which every
process sharing the directory holds while it resubmits that run. `submit`
writes the run's record before it publishes the task, and removes it only
when publishing fails.

### Client configuration example

The configuration of host `laptop`, setting every field:

```yaml
bundle: ~/.config/wherry/laptop.bundle
# A private CA the broker's certificate chains to; without ca_file, ~/.config/wherry/ca.pem if it exists, else the
# system roots.
ca_file: ~/.config/wherry/kafka-ca.pem
host_key: ~/.config/wherry/laptop.key
state_dir: ~/.local/state/wherry/laptop
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
  build:
    access_key: s3-access-key
    secret_key: s3-secret-key
```

`credentials` holds a source for each credential the agents of the
[pool terms example](#pool-terms-example) take: two files, a password
manager's command for `openai-api-key`, and the submitting process's
`GITHUB_TOKEN` for `github-token`, which `git` also gives every task cloning
under `https://github.com/acme/`. `storage` gives pool `build`'s bucket the S3
access key from a file and the secret key from the password manager; another
pool with storage takes its own entry. Each source resolves at
each submit.

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
| `metrics_addr` | none | `HOST:PORT` to serve [`/metrics`](operations.md#metrics) on; without it the receiver serves no metrics |
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
custodian's [drain](commands.md#wherry-receiver-run), its re-key
included, all end by it. A run whose commit does not complete by then is
left unacknowledged, and the broker returns its task when its acquisition
lock expires. A stop therefore takes at most `drain_timeout` plus 5 seconds.
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
[`receiver apply`](operations.md#change-a-receiver-s-configuration), which
drains with no time limit.
[`receiver remove`](operations.md#revoke-a-host-or-receiver) is a hard stop.
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

## Bundle

A YAML file that `wherry operator` writes with mode 0600 under the working
directory's `bundles/`. Unknown fields are errors, so a bundle carrying a
`ca` is refused, and `broker`, `principal` and `password` are required; a
decode error names only its line numbers. A trailing line break ends the
password and is not part of it, and a password of nothing else is refused.

| Field | Content |
| --- | --- |
| `broker` | Kafka bootstrap address, `host:port` |
| `principal` | SCRAM-SHA-512 user name |
| `password` | SCRAM password |

A bundle carries nothing about the broker's TLS CA. The Kafka platform owns
and renews its CA; each host and receiver trusts the broker through the
system's roots or the `ca_file` of its own configuration, kept current by
whoever distributes the Kafka CA. A `ca_file` with no certificate fails with
`CA contains no certificates`.

## Operator working directory

One directory per installation holds what `wherry operator` keeps. Every
operator command takes `--dir DIR`, the current directory by default.

```
<dir>/
  operator.yaml              written by operator init: how commands connect as wherry.operator
  operator.password          written by operator init, mode 0600
  pools/<pool>.yaml          pool terms, kept; pool apply reads them
  receivers/<receiver>.yaml  receiver configurations, kept; receiver apply reads them
  bundles/                   mode 0700
    hosts/<tenant>/          mode 0700
      <host>.bundle          mode 0600
    receivers/<receiver>.bundle  mode 0600
  .gitignore                 operator init appends operator.password and bundles/
```

`operator.yaml` is decoded as the client configuration is:

| Field | Content |
| --- | --- |
| `broker` | Kafka bootstrap address, `host:port`; host bundles name it, and receiver bundles unless the receiver's file names another |
| `ca_file` | absolute path of the PEM CA certificates the operator trusts the broker with; absent for the system's trusted roots |
| `password_file` | `wherry.operator`'s password file, `operator.password`; a relative path is in the working directory |

It holds nothing about the Kafka super user, which only
[`operator init`](commands.md#wherry-operator-init) uses. Kept files are not
consumed: change one and apply it again. There is no local state: the
`wherry.operator` topic is the state, and when a kept file and the topic
differ, the topic is what runs until the file is applied. The directory holds
every principal's password, in `operator.password` and each bundle
([Security](security.md#the-operator-working-directory)).

## Pool terms

The YAML file `pool apply` reads: `-f PATH`, or `pools/<pool>.yaml` in the
operator's [working directory](#operator-working-directory). Unknown fields
are errors.
When `-f` names a directory, `pool.yaml` holds every field except `agents`, and
`agents/*.yaml` each hold one agent in file-name order. Hidden entries are
ignored. Any other entry and a directory with no agent files are errors.

| Field | Content |
| --- | --- |
| `agents` | one or more `{name, credentials, models, default_model}`: an agent, the [credentials](#agent-credentials) it takes, and optionally the models a task may name for it and the one a task naming none runs |
| `default_agent` | an agent from `agents`, used when a task names none; optional when `agents` holds one, which it then defaults to |
| `start_by_limit` | optional duration, default `72h`: the latest [`task_started_by`](#task-bounds) a run may ask for, from its submission, and the time from a run's submission past which a receiver rejects it unclaimed; it also sets the pool topics' minimum retention, which `pool apply` raises and never lowers. With `storage`, it plus six hours must not exceed 7 days, the longest an S3 SigV4 presigned URL lasts |
| `delivery_limit` | optional, default 5: the share group's `share.delivery.count.limit` |
| `lock_duration` | optional duration, default `15s`: the share group's `share.record.lock.duration.ms` |
| `producer_byte_rate` | host quota, bytes per second |
| `request_percentage` | host quota, percentage of broker request time |
| `output_limit` | optional, default 8388608 (8 MiB): bytes of one output, and of a run's outputs together, and of the outcome file |
| `reply_limit` | optional, default 1048576 (1 MiB), or `output_limit` when that is lower: bytes of the redacted [reply](a2a-extension-v1.md#the-reply) a result returns; a longer reply is cut to it. At most `output_limit` |
| `vcs` | optional, default none: the hosts a [git input](a2a-extension-v1.md#git-input) may clone from |
| `storage` | optional, default none: `{endpoint, bucket}`, the S3-compatible store of the pool's [S3 inputs and outputs](a2a-extension-v1.md#s3-inputs-and-outputs) |

An agent's `default_model`, when set, is one of its `models`; it defaults to
the model of an agent listing one, and an agent without `models` has none. A task for an agent with `models` runs the model
it names, or `default_model`; with no `default_model` it must name one, and
`submit` refuses it otherwise. One naming a model outside `models` fails `submit`, and a
receiver rejects one whose model has left the terms since with
`model not in the pool's terms`. A task for an agent without `models` names
no model, and its plugin profiles run the model their command sets. wherry does not
interpret a model name.

### Agent credentials

Each entry of an agent's `credentials` is `{name, env, file, required}`: a
credential name, exactly one target, and whether a run needs it. `env` names
the environment variable the agent gets the value in, matching
`[A-Za-z_][A-Za-z0-9_]*` and none a plugin sets for the agent: `HOME`,
`WHERRY_INPUT_DIR`, `WHERRY_OUTPUT_DIR`, `WHERRY_CREDENTIALS_DIR`,
`WHERRY_MODEL` and the variables of `pluginsdk.AgentEnvironment`. `file` names
the file it gets in `WHERRY_CREDENTIALS_DIR`, a [file
name](a2a-extension-v1.md#submission). `required` defaults to `true`. An
agent's credentials have unique names, and no two share a target; `pool
create` refuses terms that break a rule, naming the agent and credential.

`submit` refuses a task when its host has no source for a required
credential of the placed agent. A run goes without an optional credential
its host has no source for, and its [prompt](#agent-contract) names it as not
supplied. Every plugin profile of the agent delivers the agent's credentials, each at
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
query. It may be `http://` only when its host is the literal `127.0.0.1` or
`[::1]`, optionally with a `:port`, 80 when absent: storage on the same machine,
as `wherry dev` runs it. A name that resolves to loopback, such as `localhost`,
still requires `https://`. wherry speaks the endpoint's scheme to it, and the
receiver refuses a presigned URL with any other scheme, host or port. `storage.bucket` is 3 to 63
lowercase letters, digits, dots and hyphens, starting and ending with a
letter or digit, with no `..`. The run's objects live under `wherry/<tenant>/<run>/` in
the bucket, addressed path-style, `<endpoint>/<bucket>/<key>`. A pool
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

[`pool apply`](commands.md#wherry-operator-pool-apply-pool) changes an existing
pool's terms. A lowered `delivery_limit` applies to a task in flight at its
next release.

### Pool terms example

Terms for pool `build` that set every field, as one file for
`pool apply build -f build.yaml`:

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
start_by_limit: 72h
# Each receiver with no matching profile spends one delivery, so this covers the pool's receivers plus a margin.
delivery_limit: 6
# Above the receiver's 10s renewal interval.
lock_duration: 30s
producer_byte_rate: 1048576
request_percentage: 10
output_limit: 16777216
reply_limit: 2097152
vcs:
  - github.com
  # A port appears only when it is not the scheme's default.
  - git.example.com:2222
storage:
  endpoint: https://s3.example.com
  bucket: wherry
```

The same terms as a directory for `pool apply build -f build`:

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
start_by_limit: 72h
# Each receiver with no matching profile spends one delivery, so this covers the pool's receivers plus a margin.
delivery_limit: 6
# Above the receiver's 10s renewal interval.
lock_duration: 30s
producer_byte_rate: 1048576
request_percentage: 10
output_limit: 16777216
reply_limit: 2097152
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

A record limit is the lower of what the record's producer batches and what
the topic the record goes to takes: its `max.message.bytes` when the topic
sets one, and otherwise the lowest `message.max.bytes` across the brokers.
Submissions go to the pool's tasks topic, from a host that batches at most
1000012 bytes. Receiver records go to the feeds and transcript topics of
the tenants entitled to the pool, from a receiver that batches at most
2 MiB (2097152 bytes), the `max.message.bytes` wherry sets on every tenant
feed and transcript topic. The Kafka cluster must allow a topic
`max.message.bytes` of 2 MiB.
`pool apply` records the tasks topic's record limit in the pool's terms,
and [`submit`](commands.md#wherry-host-submit) measures each submission against it. A pool file
does not set it.

`pool apply`, `pool entitle` and `check` sign four records and measure
them as record batches: the largest output chunk, of 256 KiB, two largest
results, and the largest transcript chunk, a line of 512 KiB. Each result
has the run's message with its 256 input files named but not carried, and a
status message and a result text of 64 KiB each, both raw, as an agent's text
that JSON escaping would lengthen travels, with a `credential` part, a
`reply` entry, and reference artifacts for the reply and the transcript. A reply chunk is an output
chunk, so `reply_limit` has no record-limit check. One result lists
256 returned and 256 unreturned outputs with 512-byte names, each returned
one also a reference artifact, and is the largest record, a 1259677-byte
batch; the other lists 256 S3 outputs with their keys. Both carry a plugin's job name at its bound of
253 bytes. A receiver's labels have no bound and are left empty. Any one over its record limit refuses the terms,
naming its size and the limit.

An input file travels base64-encoded inside the signed record, which is itself
base64url-encoded, so an inline input takes about 16/9 of its size in the
submission record.

`pool apply` and `pool entitle` check against the broker at the time.
Receivers apply the limits from their view of `wherry.operator`, which they
read every 15 seconds, so a changed limit takes effect within that
window.

### Quotas

Each host principal gets the Kafka client quotas `producer_byte_rate` and
`request_percentage`, the lowest of each across the pools its tenant is
entitled to.

## Receiver configuration

What `receiver apply` reads, recorded on
`wherry.operator` but for `broker`: a
[directory](#receiver-configuration-directory) or a single YAML file with the
same entries in maps and lists, named by `-f`, or the single file
`receivers/<receiver>.yaml` in the operator's
[working directory](#operator-working-directory). The receiver's name comes
from the command. Unknown fields are errors.

| Field of the single file | Content |
| --- | --- |
| `pool` | the receiver's pool; required for a new receiver, and an existing one's must match |
| `plugin_sha256` | for each plugin type, the SHA-256 of its binary as 64 lowercase hex digits: the line for the plugin binary in its release's signed `checksums.txt` ([Install plugins](operations.md#install-plugins)) |
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
[pool terms example](#pool-terms-example): one Nomad plugin and two
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
its Kafka identity alone. The [Quickstart](quickstart.md#_7-write-the-receiver-s-local-configuration)
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
[agent's terms](#agent-credentials) or the task name.
`WHERRY_INPUT_DIR`, `WHERRY_CREDENTIALS_DIR` and `WHERRY_OUTPUT_DIR` hold
the paths of its [input, credential and output
directories](a2a-extension-v1.md#inputs-and-outputs). `WHERRY_MODEL` holds
the task's model when its agent has [`models`](#pool-terms), and is not set
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
`task_ended_by`, by which it must reach a terminal state. The receiver's
clock decides both. A bound fails only when that clock is past its second,
so a job that starts at `task_started_by`, or a terminal state at
`task_ended_by`, meets it. A run's time is also capped at six hours after
`task_started_by`. `submit` refuses a `task_ended_by` later than that, and a
receiver rejects such a run no pool member has claimed with
`task_ended_by is past task_started_by plus the 6h run cap`.

A job starts when the plugin reports it running, at which the receiver
publishes the run's `started` event. A receiver rejects a run no pool member
has claimed with `not started by task_started_by (<RFC 3339 time>)` or
`not ended by task_ended_by (<RFC 3339 time>)` once that bound has passed.
A claimed run whose job has not started by `task_started_by`, while it waits
for its envelope to open or for the platform to place its job, ends `TASK_STATE_FAILED`
with `not started by task_started_by (<RFC 3339 time>)`, and the receiver
purges any job the run has. A job that has started runs on past
`task_started_by`. A redelivered run that a member has claimed is not
rejected: the receiver adopts its job when it finds one, and dispatches the
run again when it finds none and `task_started_by` has not passed. Past
`task_ended_by`, the receiver stops any job it finds and ends the run
`TASK_STATE_FAILED` with `not ended by task_ended_by (<RFC 3339 time>)`.

A receiver measures a run's age from the broker's append time of its
submission. It rejects a run no pool member has claimed with
`task_started_by is past the pool's start_by_limit` when that time is more
than `start_by_limit` ago, or when the run's `task_started_by` falls more
than `start_by_limit` after it. A redelivered run that a member has claimed
skips both checks. `wherry submit` and the MCP `submit` tool refuse, before
publishing, a `task_started_by` later than `start_by_limit` from now with the
same text, the MCP tool with `INVALID_ARGUMENT`. Every run therefore ends
within six hours after `start_by_limit`.

## Environment variables

| Variable | Read by | Effect |
| --- | --- | --- |
| `WHERRY_CONFIG` | host commands | the [client configuration](#client-configuration) path in place of `~/.config/wherry/config.yaml`; `--config-file` wins over it |
| `XDG_CONFIG_HOME`, `XDG_STATE_HOME` | host commands | when set, the parents of the host's `wherry/` configuration and state directories in place of `~/.config` and `~/.local/state` |
| `SSL_CERT_FILE` | host commands, `wherry receiver run`, `wherry operator` | PEM CA file that replaces the system roots, for a pool's S3 endpoint under a private CA and for a broker when no `ca_file` is set; include the public roots in it when the process also reaches public endpoints |

`wherry operator` reads no environment variable: its connection is
[`operator.yaml`](#operator-working-directory).

A plugin reads its own variables from the receiver's environment; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#credentials)
lists its.

Trailing CR and LF are trimmed from password files: `operator.password`, the
super user's password file `operator init` reads, and a bundle's password.
