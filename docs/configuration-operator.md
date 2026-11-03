# Operator configuration

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

In `operator.yaml`, unknown and duplicate fields are errors, a decode error
names only its line numbers, and no `~` is expanded:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `broker` | string | none | Kafka bootstrap address, `host:port`; host bundles name it, and receiver bundles unless the receiver's file names another |
| `ca_file` | string | none | path of the PEM CA certificates the operator trusts the broker with, which `operator init` writes absolute; a relative path is in the working directory; absent for the system's trusted roots |
| `password_file` | string | none | `wherry.operator`'s password file, `operator.password`; a relative path is in the working directory |

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

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `agents` | list | none | one or more `{name, credentials, models, default_model}`: an agent, the [credentials](#agent-credentials) it takes, and optionally the models a task may name for it and the one a task naming none runs |
| `default_agent` | string | none | an agent from `agents`, used when a task names none; optional when `agents` holds one, which it then defaults to |
| `start_by_limit` | duration | none | optional duration, default `72h`: the latest [`task_started_by`](configuration-receiver.md#task-bounds) a run may ask for, from its submission, and the time from a run's submission past which a receiver rejects it unclaimed; it also sets the pool topics' minimum retention, which `pool apply` raises and never lowers. With `storage`, it plus six hours must not exceed 7 days, the longest an S3 SigV4 presigned URL lasts |
| `delivery_limit` | integer | none | optional, default 5: the share group's `share.delivery.count.limit` |
| `lock_duration` | duration | none | optional duration, default `15s`: the share group's `share.record.lock.duration.ms` |
| `producer_byte_rate` | integer | none | host quota, bytes per second |
| `request_percentage` | integer | none | host quota, percentage of broker request time |
| `output_limit` | integer | none | optional, default 8388608 (8 MiB): bytes of one output, and of a run's outputs together, and of the outcome file |
| `reply_limit` | integer | none | optional, default 1048576 (1 MiB), or `output_limit` when that is lower: bytes of the redacted [reply](a2a-extension-v1.md#the-reply) a result returns; a longer reply is cut to it. At most `output_limit` |
| `vcs` | string | none | optional, default none: the hosts a [git input](a2a-extension-v1.md#git-input) may clone from |
| `storage` | mapping | none | optional, default none: `{endpoint, bucket}`, the S3-compatible store of the pool's [S3 inputs and outputs](a2a-extension-v1.md#s3-inputs-and-outputs) |

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
apply` refuses terms that break a rule, naming the agent and credential.

`submit` refuses a task when its host has no source for a required
credential of the placed agent. A run goes without an optional credential
its host has no source for, and its [prompt](configuration-receiver.md#agent-contract) names it as not
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
the topic the record goes to takes, which Kafka's
[`max.message.bytes`](https://kafka.apache.org/documentation/#topicconfigs_max.message.bytes)
sets. wherry reads the topic's own setting, else the lowest
`message.max.bytes` across the brokers. Submissions go to the pool's tasks
topic from a host, which batches at most
[franz-go's default](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#ProducerBatchMaxBytes),
1000012 bytes. Receiver records go to the feeds and transcript topics of
the tenants entitled to the pool, from a receiver that batches at most
2 MiB (2097152 bytes), the `max.message.bytes` wherry sets on every tenant
feed and transcript topic. The Kafka cluster must allow a topic
`max.message.bytes` of 2 MiB.
`pool apply` records the tasks topic's record limit in the pool's terms,
and [`submit`](commands.md#wherry-host-submit) measures each submission against it. A pool file
does not set it.

`pool apply`, `tenant pool add` and `check` compare five record-batch sizes
against the record limits. wherry's tests build and sign each record to pin
its size. The smallest task a host submits with a maximum envelope and its
submit wrap, an 89326-byte batch, goes against the tasks topic's limit. Four
receiver records go against the tenant feeds' limit: the largest output
chunk, of 256 KiB, two largest results, and the largest transcript chunk, a
line of 512 KiB. Each result
has the run's message with its 256 input files named but not carried, and a
status message and a result text of 64 KiB each, both raw, as an agent's text
that JSON escaping would lengthen travels, with a `credential` part, a
`reply` entry, and reference artifacts for the reply and the transcript. A reply chunk is an output
chunk, so `reply_limit` has no record-limit check. One result lists
256 returned and 256 unreturned outputs with 512-byte names, each returned
one also a reference artifact, and is the largest record, a 1259677-byte
batch; the other lists 256 S3 outputs with their keys. Both carry a plugin's job name at its bound of
253 bytes. A receiver's labels have no bound and are left empty. Any one over its record limit refuses the terms,
naming the record, its size and the limit.

An input file travels base64-encoded inside the signed record, which is itself
base64url-encoded, so an inline input takes about 16/9 of its size in the
submission record.

`pool apply` and `tenant pool add` check against the broker at the time.
Receivers apply the limits from their view of `wherry.operator`, which they
read every 15 seconds, so a changed limit takes effect within that
window.

### Quotas

Each host principal gets the Kafka client quotas `producer_byte_rate` and
`request_percentage`, the lowest of each across the pools its tenant is
entitled to.


## Tenants and entitlements

Create a tenant with `wherry operator tenant add TENANT`.
Entitle it to a pool with `wherry operator tenant pool add TENANT POOL`.
Host creation and entitlement changes require an existing live tenant.
The tenant records live on `wherry.operator`.
There is no tenant configuration file.

Remove an entitlement with `wherry operator tenant pool remove TENANT POOL`.
You can add the entitlement again.
Remove the tenant with `wherry operator tenant remove TENANT`.
An ended tenant name cannot be reused.
See [Tenant onboarding and offboarding](operations-operator.md#tenant-onboarding-and-offboarding).
