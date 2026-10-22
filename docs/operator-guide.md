# Operator guide

Before a tenant can submit, an operator prepares the Kafka cluster,
creates a pool with its terms, entitles the tenant, adds its hosts, and
adds and runs at least one receiver. Later the operator reconfigures
receivers, revokes access and recovers from faults. This guide covers each
task. [Concepts](concepts.md) explains the model, and
[Reference](reference.md) lists every command, file and field.

## Prepare Kafka

wherry is tested against Kafka 4.3. `wherry admin` creates its topics, ACLs,
principals and group configurations; the cluster itself is prepared once.

- **Share groups** are enabled, so receivers can claim tasks. See
  [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka).
- **A TLS listener with SCRAM-SHA-512** is reachable from every host and
  receiver. Its address and CA certificate go into every bundle.
- **An authoriser** denies anything no ACL allows, and `super.users` names
  the principal `wherry admin` uses as root. See
  [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz).
- **`auto.create.topics.enable=false`**, so a mistyped topic name fails.
- **`connections.max.reauth.ms`** is set on the listener. Without
  `--wait`, revocation waits for it and fails while it is unbounded.
- **`transaction.max.timeout.ms`** is short enough to wait out: without
  `--wait`, revocation waits the longest of it,
  `connections.max.reauth.ms` and
  `group.share.max.record.lock.duration.ms`. Kafka's default makes each
  revocation wait that long unless `--wait` is given.
- **Two SCRAM principals** exist: the super user, and `wherry.operator`
  with Read, Write and Describe on topic `wherry.admin`.

### Strimzi

On a [Strimzi](https://strimzi.io/documentation/) cluster, declare the
super user and `wherry.operator` as `KafkaUser` resources. `wherry admin`
creates host and receiver principals through the Kafka Admin API, which
Strimzi's User Operator removes unless it ignores them. Set the User
Operator's `STRIMZI_IGNORED_USERS_PATTERN` environment variable, described
in the [Strimzi documentation](https://strimzi.io/documentation/), to:

```
^wherry\.(tnt|rcv)\..+
```

## Set up `wherry admin`

`wherry admin` reads its connection from the environment. The broker
address and CA file are also what it writes into bundles, so use the
address hosts and receivers reach.

```sh
export WHERRY_TRUST_BROKER=kafka.example.com:9093
export WHERRY_TRUST_CA=/etc/wherry/kafka-ca.pem
export WHERRY_TRUST_ADMIN_USER=kafka-admin
export WHERRY_TRUST_ADMIN_PASSWORD_FILE=/etc/wherry/admin-password
export WHERRY_TRUST_OPERATOR_USER=wherry.operator
export WHERRY_TRUST_OPERATOR_PASSWORD_FILE=/etc/wherry/operator-password
```

## Create a pool

Prerequisites: the `wherry admin` environment
([Set up `wherry admin`](#set-up-wherry-admin)).

A pool's terms apply to every receiver in it. Write them as a
[terms file](reference.md#pool-terms) and create the pool:

```sh
wherry admin pool create build --config-file build-terms.yaml
```

- `delivery_limit` bounds how often the broker delivers one task. Each
  start failure, and each member with no matching adapter, returns a task
  to the pool and uses a delivery, so set it to at least the number of
  receivers in the pool, plus a margin. From attempt
  `delivery_limit` minus 1, a receiver ends a turn it would return to the
  pool `TASK_STATE_FAILED` with `delivery budget exhausted`, except in the
  cases the [extension](a2a-extension-v1.md#delivery-budget) lists. The
  broker caps it at
  `group.share.max.delivery.count.limit`.
- `lock_duration_ms` is how long a claim survives without renewal, and how
  long a denied receiver keeps running its jobs. Receivers renew every 10
  seconds.
- `run_lifetime_ms` is the longest time from a run's first submission to
  its last continuation. It defaults to 14 days. See [Retention](#retention).
- `inline_limit` and `output_limit` bound a task's input files and a
  turn's outputs; they default to 256 KiB and 8 MiB. See
  [Size a pool against the broker](#size-a-pool-against-the-broker).
- `vcs` lists the hosts a task's agent may clone from; without it the
  pool takes no git input. See [Git repositories](#git-repositories).
- An agent's `models` lists the models a task may name for it, and its
  optional `default_model`, one of them, the one a task naming none runs.
  With `models` and no `default_model`, every task for the agent names a
  model. wherry does not interpret the names. The agent's adapters read the task's
  model from `WHERRY_MODEL`, as in
  [Add a receiver](#add-a-receiver). Without `models` a task names no model
  and the adapter's command sets it.

Running `pool create` again for an existing pool records the new terms.
Adding or removing an agent's `models` rejects that agent's queued and
continuing runs at their next claim, so drain the agent first.

### Retention

A run takes continuations for `run_lifetime_ms` after its first submission.
After that, receivers reject another turn with `run expired`, and an interrupted
run ends `TASK_STATE_CANCELED` with the same message the next time a receiver
reads the pool's events. Receivers also reject a turn whose queue deadline falls
after the lifetime, so every turn ends within six hours after it. Choose the
lifetime as the longest pause expected before a continuation, such as a wait for
a person to sign in. A longer lifetime keeps prompts on the broker longer.
Lowering the lifetime of a pool with runs in progress lowers its topics'
retention at once: Kafka then deletes the records of runs older than the new
bound, and their continuations fail with `continuation history unavailable`.

`pool create` sets the retention of the pool's tasks and events topics, new and
existing, to the run lifetime plus the six-hour maximum turn time plus a one-day
margin, and their timestamp type to `LogAppendTime`, since the lifetime runs
from the broker's time. With the 14-day default that is `1317600000` ms, 15 days
and six hours, after which a run's prompts leave the broker. Tenant feeds keep
statuses, transcripts and results for 30 days and must keep them for at least
the run lifetime plus six hours, so `pool create` refuses a lifetime above 29
days and 18 hours, and `tenant add` refuses a feed that keeps less. Identity and
configuration topics stay forever. `wherry admin check` reports a topic whose
retention drops below its bound, and a pool topic whose timestamp type is not
`LogAppendTime`.

### Size a pool against the broker

Every task and every output or transcript chunk is one Kafka record, so
the broker's record limit bounds what a pool can carry. `pool create` reads
`message.max.bytes` from every broker and `max.message.bytes` from the
pool's tasks topic and its tenants' feeds, signs the largest bounded
submission `inline_limit` allows and the largest output and transcript
chunks, and refuses the terms when one is over the limit of the topics it
goes to. A transcript chunk can be 933544 bytes, so keep each tenant feed's
`max.message.bytes` at or above that:

```
inline limit 524288 bytes makes a 1029104-byte submission record, over the 1000012-byte record limit
```

The bounded submission fills every field that has a bound, including 256
input files with the longest names. A prompt has no bound, so a task with
a large prompt can still be refused by the broker; `submit` names the
record's size when it is.

Input files grow by about 16/9 on the wire, since they are base64-encoded
inside a record that is itself base64url-encoded. With brokers and topics
at their defaults the 1000012-byte producer batch is the limit, and
`inline_limit` can rise to about 481 KiB. wherry producers batch at most
1000012 bytes, so raising `message.max.bytes` and `max.message.bytes` does
not take it further; lowering them brings it down. An output chunk holds
256 KiB whatever `output_limit` is, so `output_limit` spans as many records
as it needs and the record limit does not bound it.

`tenant add` makes the same check with the new tenant's feed counted.
Lowering a broker or topic limit later can leave a pool's terms too large;
[`wherry admin check`](#check-a-deployment) reports it. Receivers read
their pool's terms from `wherry.admin` every 15 seconds, so admission and
output collection use a changed limit within that window.

## Add tenants and hosts

Prerequisites: the `wherry admin` environment
([Set up `wherry admin`](#set-up-wherry-admin)) and the pool.

1. Entitle the tenant to a pool:

   ```sh
   wherry admin tenant add alice --pool build
   ```

2. Add each host the tenant submits from:

   ```sh
   wherry admin host add alice laptop --bundle-file alice-laptop.bundle
   ```

3. Hand the bundle to the host's user over a channel you trust. It holds
   the host's Kafka password.

The tenant then writes its
[client configuration](reference.md#client-configuration) and runs
`wherry init`, as in [Getting started](getting-started.md#7-configure-the-host).

A tenant can be entitled to several pools; run `tenant add` once per pool.

## Add a receiver

A receiver's configuration lists its adapters in routing order. Each adapter
launches one agent on a platform, may deliver one credential, and carries
the labels tasks select on. Two adapters can launch the same agent with
different labels, such as one running in containers and one on the host.

Write the configuration as a directory:

```
nomad-1/
  receiver.yaml
  platforms/
    nomad.yaml
  adapters/
    10-container.yaml
    20-host.yaml
```

`receiver.yaml` holds the receiver's `capacity`:

```yaml
capacity: 2
```

Each file in `platforms/` names a platform, its adapter type and the
connection settings its adapters share:

```yaml
name: nomad
type: nomad
config:
  address: https://nomad.example.com:4646
  ca_file: /etc/wherry/nomad-ca.pem
  namespace: agents
  credential_env: MODEL_API_KEY
```

Each file in `adapters/` is one adapter on one of those platforms, with the
settings particular to it:

```yaml
name: container
platform: nomad
agent: coder
credential: model-api-key
labels:
  isolation: container
config:
  node_pool: default
  runtime:
    driver: docker
    config:
      image: registry.example.com/agent:1.0
```

`20-host.yaml` declares `name: host`, `labels` with `isolation: host`, and
an `exec` runtime on a node pool whose nodes have the agent installed.

The adapter files' names set routing order, lowest first, so a numeric
prefix orders them. An adapter takes its platform's `type`, and its `config`
is the platform's `config` plus its own. The two share no top-level key: a
key set in both is an error. The
[directory layout](reference.md#receiver-configuration-directory) lists
every rule.

A single YAML file holding the whole
[receiver configuration](reference.md#receiver-configuration) also works in
place of the directory.

A task with no selector runs on the first ready adapter, `container` here,
and `--selector isolation=host` runs on the second. Every member of a pool
must honour the labels its adapters declare. The client places a task by
one member's labels, but any member can claim it. A member with no
matching adapter returns the task.

The [Nomad plugin's settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
cover the `config` section.

A Codex adapter declares `output: codex`, so the receiver reads the
`codex exec --json` format. `codex exec` reads its API key from
`CODEX_API_KEY` and ignores `OPENAI_API_KEY`, so the adapter sets
`credential_env` to `CODEX_API_KEY`. Its platform,
`nomad-codex` here, is the `nomad` platform without `credential_env`, since
a key set in both is an error. The task sends the prompt file to
`codex exec` on standard input, and `--sandbox` takes a mode the platform
allows. Without `--ephemeral`, Codex writes session files under
`CODEX_HOME` (default `~/.codex`), which the task must be able to write:

```yaml
name: codex
platform: nomad-codex
agent: codex
output: codex
credential: openai-api-key
labels:
  isolation: host
config:
  node_pool: codex
  credential_env: CODEX_API_KEY
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

An adapter whose agent has `models` in the pool's terms reads the task's
model from `WHERRY_MODEL`, which the plugin sets only when the task has a
model, and falls back to a model of its own. The shell command sets the
fallback first, `test -n "$WHERRY_MODEL" || WHERRY_MODEL=haiku;`, then passes
`--model "$WHERRY_MODEL"` to Claude Code or `-m "$WHERRY_MODEL"` to Codex.
Nomad reads `${...}` in driver configuration as its own interpolation and
fails the task on `${WHERRY_MODEL:-haiku}`, so the fallback avoids braces.
The value reaches the command through the environment, never pasted into its
arguments.

An adapter for any other harness declares `output: file`: the receiver ends
the turn when the job ends and takes the reply from a file the job writes to
its output directory, `reply.md` unless `reply_file` names another. The
command writes the harness's reply there, so the harness itself is
unchanged, and its exit code decides whether the turn completes:

```yaml
name: harness
platform: nomad
agent: coder
output: file
credential: model-api-key
labels:
  isolation: host
config:
  node_pool: default
  runtime:
    driver: exec
    config:
      command: /bin/sh
      args:
        - -c
        - >-
          exec harness run < "$NOMAD_TASK_DIR/prompt.txt"
          > "$WHERRY_OUTPUT_DIR/reply.md"
```

A receiver or host of an earlier release cannot decode a configuration record
that sets `output` or `reply_file`, and stops reading `wherry.admin` while it
holds one. Upgrade every receiver and host before a configuration sets them.

1. Write the configuration and add the receiver:

   ```sh
   wherry admin receiver add nomad-1 --pool build \
     --config-dir nomad-1 --bundle-file nomad-1.bundle
   ```

   When the platform does not issue the receiver's credential itself, add
   `--platform-credential-file ADAPTER=FILE` to put a token in the bundle for
   one adapter.

2. Deliver the bundle to the receiver through the platform's secret
   mechanism.

3. Start the receiver, as in [Run a receiver](#run-a-receiver).

### Change a receiver's configuration

```sh
wherry admin receiver configure nomad-1 --config-dir nomad-1
```

The receiver exits when it reads the new record at its next configuration check,
which runs every 15 seconds, and its platform restarts it with the new
configuration. Runs in flight are adopted on redelivery. When the new
configuration renames the adapter or changes its agent, the run's job is stopped
and purged, and the run is dispatched again. The bundle and password do not
change.

## Run a receiver

`wherry receiver --bundle-file FILE --state-dir DIR --plugin-dir DIR` needs:

- the bundle, readable by the receiver alone;
- a state directory that survives restarts. It holds the receiver's
  private keys, and a receiver that loses them cannot start under the same
  name again;
- a plugin directory holding the plugin for every adapter type in its
  configuration, as in [Install adapter plugins](#install-adapter-plugins);
- a supervisor that restarts the process when it exits, since a
  configuration change ends it;
- for the Nomad plugin, a Nomad token with the
  [receiver policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy), from the bundle or `NOMAD_TOKEN`
  in the receiver's environment.

On first start the receiver logs `registered key: signing=<key id>`, then
`configured: pool=<pool> adapters=<n> capacity=<n>`. It logs
`waiting for configuration` while its record is missing. After its first
advertisement, `wherry receivers` on a host of an entitled tenant shows it
`live`.

A plugin that exits is started again and its adapters configured again;
the receiver logs `plugin exited; restarting`, then `plugin restarted`.
[Adapter plugins](reference.md#adapter-plugins) covers what runs in flight
meanwhile.

### Install adapter plugins

The receiver ships with no adapters. Build each plugin from
[wherry-plugins](https://github.com/benemon/wherry-plugins) and install it
as `wherry-plugin-<type>` in one directory on the receiver host, owned by
root and writable by no one else:

```sh
install -d -o root -g root -m 0755 /usr/local/libexec/wherry/plugins
install -o root -g root -m 0755 wherry-plugin-nomad /usr/local/libexec/wherry/plugins/
```

The receiver refuses a plugin directory or binary owned by a user other
than root or the receiver's, or that group or others can write, and a
plugin that is not a regular file. It checks again before restarting a
plugin. It exits with
`adapter <name>: no plugin for type <type>` when a type's plugin is
missing. Install a new plugin version while the receiver is stopped, or
restart the receiver after replacing it.

### As a Nomad job

On Nomad, the receiver can run as a service job whose
[workload identity](https://developer.hashicorp.com/nomad/docs/concepts/workload-identity)
supplies `NOMAD_TOKEN`, with the bundle in a
[Nomad variable](https://developer.hashicorp.com/nomad/docs/concepts/variables):

```hcl
job "wherry-receiver" {
  namespace = "agents"
  type      = "service"

  group "receiver" {
    ephemeral_disk {
      sticky  = true
      migrate = true
    }

    task "receiver" {
      driver = "exec"

      config {
        command = "/usr/local/bin/wherry"
        args    = ["receiver", "--bundle-file", "${NOMAD_SECRETS_DIR}/bundle.yaml",
                   "--state-dir", "${NOMAD_ALLOC_DIR}/data",
                   "--plugin-dir", "/usr/local/libexec/wherry/plugins"]
      }

      identity {
        env = true
      }

      template {
        destination = "${NOMAD_SECRETS_DIR}/bundle.yaml"
        data        = "{{ with nomadVar \"nomad/jobs/wherry-receiver\" }}{{ .bundle }}{{ end }}"
      }

      template {
        destination = "${NOMAD_SECRETS_DIR}/nomad-ca.pem"
        data        = "{{ with nomadVar \"nomad/jobs/wherry-receiver\" }}{{ .nomad_ca }}{{ end }}"
      }
    }
  }
}
```

The plugin directory is the one installed on the node, as in
[Install adapter plugins](#install-adapter-plugins). As the task sees them, root
or the receiver's user must own the directory and the binary, neither group nor
others may write them, and the binary must be a regular file. The plugin runs in
the receiver's task with its environment, so the workload identity's
`NOMAD_TOKEN` reaches it.

Nomad treats a sticky, migrating ephemeral disk as best effort. If the node
is lost, the keys go with it and the receiver is re-provisioned under a new
name; a host volume avoids that.

Attach the [receiver policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy) to the job:

```sh
nomad acl policy apply -namespace agents -job wherry-receiver \
  wherry-receiver receiver-policy.hcl
```

The adapter's `ca_file` is then `/secrets/nomad-ca.pem`, the path inside
the task.

### Metrics

Start the receiver with `--metrics-addr HOST:PORT` to serve its metrics at
`/metrics` in the Prometheus text format. The endpoint is plain HTTP with no
authentication, so bind it to an address only the scraper reaches, such as
the receiver host's address on a monitoring network, or put an
authenticating proxy in front of it:

```sh
wherry receiver --bundle-file bundle.yaml --state-dir /var/lib/wherry \
  --plugin-dir /usr/local/libexec/wherry/plugins --metrics-addr 10.0.8.21:9464
```

Scrape it as any Prometheus target:

```yaml
scrape_configs:
  - job_name: wherry-receiver
    static_configs:
      - targets: ["10.0.8.21:9464"]
```

The `tenant` label names each entitled tenant whose runs reach the pool.
Whoever can scrape a receiver sees every tenant's activity on its pool: its
runs and their outcomes, durations, rejections and seal waits, and the
tokens and cost its agents report. Give scrape access only to those who may
see all of the pool's tenants. No label or value carries a prompt, output,
run ID, error text, URL, image, path, node or credential name.
[Metrics](reference.md#metrics) lists the families.

## Revoke a host or receiver

Prerequisites: the broker settings in [Prepare Kafka](#prepare-kafka), and
the `wherry admin` environment.

1. Run the revocation:

   ```sh
   wherry admin revoke host alice laptop
   wherry admin revoke receiver nomad-1
   ```

   When it ends, the command prints one line per
   [revocation step](concepts.md#revocation) it ran,
   `step=<n> complete=<bool> name=<name>`. The wait in step 5 is usually the
   longest; `--wait DURATION` replaces it.

2. If it fails with `revocation incomplete`, the line of the failed step
   ends in ` error=<cause>`. Check what that step depends on (the broker
   connection, the broker settings), then run the command again. It starts
   again from step 1. Step 3 already retries for up to 2 minutes while a
   broker still authorises the principal.

3. For a receiver, rotate each credential sealed to it at its source. The
   receiver has seen their values.

Receivers stop reading a revoked host's topic, so no seal it writes reaches
them.

A revoked name cannot be reused. Provision a returning host or receiver
under a new name.

## Replace a lost key

A host or receiver key cannot be recovered or re-registered.

- A host whose `host_key` is lost fails `wherry init` with
  `host key topic holds another key`.
- A receiver whose state directory is lost fails at start with
  `receiver key topic holds another key than the state directory`.

Revoke it, then add a host or receiver under a new name.

## Check a deployment

```sh
wherry admin check
```

It loads `wherry.admin`, validates every receiver configuration against its
pool's current terms, checks that every pool's largest records still fit the
broker's record limit, reports topic retention below the pool's required
bounds, and confirms that every ACL the state calls for exists. It does not
check an adapter's `type` or `config` section; at start the receiver finds the
type's plugin, the plugin checks the section, and the receiver exits with
`adapter <name>: ...` on an error. Run it after changing a pool's terms, since
receivers keep the configuration they started with.

## Recover from an undecodable `wherry.admin` record

Every reader of `wherry.admin` stops while any record on it fails to
decode, so nothing loads state until the operator decides. The error lists
each such record as `offset=<n> size=<n> sha256=<hex>`.

1. Read the record at that offset with Kafka's own tools and decide that it
   carries no state wherry needs.
2. Record the decision:

   ```sh
   wherry admin skip 42 --sha256 <hex>
   ```

The command refuses a record that decodes, so a skip cannot hide state such
as a membership end. It also refuses a record a later release wrote; the
error says so, and upgrading `wherry` reads it.

## Run credentialed tasks

A receiver asks the submitting host for an adapter's credential when it
claims the turn, and the host answers only while `wherry await` or
`wherry mcp` runs on it. A parent that submits a task whose adapter takes
a credential keeps one of them running until the task starts; without an
answer within 5 minutes, the turn ends `TASK_STATE_AUTH_REQUIRED` with the
message `credential <name> required`, and `wherry continue` followed by
`wherry await` runs it again.

The host answers only for a credential the pool's terms approve for the
run's agent, or a task credential the run declares, that its configuration
maps to a file, and only to the receiver that first claimed the turn.
`wherry submit` refuses a task credential its configuration does not map
to a file. It logs each verified request of its
own runs that it skips or refuses, by run, receiver and credential name, on
standard error. A receiver that adopts a running job asks for its own seal
and publishes nothing of the job until it opens. A turn adopted by a second
receiver gets no seal, so that receiver stops the job and the turn ends
`TASK_STATE_AUTH_REQUIRED`; `wherry continue` runs it again.

## Inputs and outputs

A task's input files hold the pool's `inline_limit` together, and a turn's
outputs its `output_limit` in 256 entries at most. An adapter implements
`InlineLimit` with the most inline input bytes it delivers, or zero when it
declares none. Receivers advertise that cap. A host applies the smallest
nonzero cap among the live adapters in the chosen pool that satisfy the task.
When the task exceeds it, a pool with storage and a configured host storage
credential carries every inline input through S3; otherwise submit refuses
before publishing. The receiver keeps the adapter's dispatch check as a
backstop.

The agent creates `WHERRY_OUTPUT_DIR` when it writes its first output.
Outputs are collected once the agent writes its result line and the job is
stopped, including when that line reports a failure. A turn that ends
without the agent's result line, by timeout or otherwise, returns none.

## S3 storage

A pool can carry inputs and outputs of up to 5 GiB each through an
S3-compatible bucket, the [S3 lane](a2a-extension-v1.md#s3-inputs-and-outputs).
wherry administers no bucket: the operator creates it, sets its lifecycle
rule, issues each tenant's credential and names the endpoint in the pool's
terms. Any store that speaks S3 with SigV4 presigned URLs and path-style
addressing serves, such as Ceph RGW, NooBaa or MinIO.

1. Create one long-lived bucket with the store's default server-side
   encryption, store-managed or KMS. Every run's objects live under
   `wherry/<tenant>/<run>/` in it.

2. Name it in the pool's [terms](reference.md#pool-terms). The endpoint is
   `https://` and the host, with `:port` when it is not 443, and no path;
   wherry refuses a plain `http` endpoint.

   ```yaml
   storage:
     endpoint: https://s3.example.com
     bucket: wherry
   ```

3. Give each tenant's hosts a credential that reaches only that tenant's
   prefix: read, write and delete objects under `wherry/<tenant>/`, and list
   the bucket under that prefix, which `wherry cleanup` and the end of a run
   use. A bucket policy for tenant `alice`, whose principal syntax varies by
   store (Ceph RGW names a user `arn:aws:iam:::user/<uid>`):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {"Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam:::user/wherry-alice"]},
        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        "Resource": ["arn:aws:s3:::wherry/wherry/alice/*"]},
       {"Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam:::user/wherry-alice"]},
        "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::wherry"],
        "Condition": {"StringLike": {"s3:prefix": ["wherry/alice/*"]}}}
     ]
   }
   ```

   ```sh
   aws s3api put-bucket-policy --endpoint-url https://s3.example.com --bucket wherry --policy file://alice.json
   ```

   The host keeps the access key and secret key in two files of mode 0600, named
   under `storage` in its [configuration](reference.md#client-configuration).
   Receivers and agents never hold it; they get presigned URLs, a GET per input
   for 15 minutes and a PUT per output until the turn's deadline, 7 days at
   most.

4. Set a lifecycle rule that expires `wherry/` after a period longer than
   any run: the host deletes a run's objects when it sees the run end with
   every output verified, and the rule removes what no host saw finish.

   ```json
   {"Rules": [{"ID": "wherry-backstop", "Status": "Enabled", "Filter": {"Prefix": "wherry/"}, "Expiration": {"Days": 7}}]}
   ```

   ```sh
   aws s3api put-bucket-lifecycle-configuration --endpoint-url https://s3.example.com --bucket wherry --lifecycle-configuration file://lifecycle.json
   ```

5. Let the parties reach the endpoint and trust its certificate: hosts,
   which upload, download and delete (they trust the system roots and the
   CA file in `SSL_CERT_FILE`); receiver hosts, which upload outputs with
   their system roots; and every platform node that fetches inputs, for the
   Nomad plugin each Nomad client, whose artifact fetcher uses the system
   roots. Agent jobs do not reach the endpoint themselves; see
   [Agent network reach](#agent-network-reach).

The [Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#s3-inputs)
states how Nomad fetches an input, where its presigned GET is visible, and
the limits to set on the Nomad clients. The receiver's upload follows no
redirect.

## Git repositories

A task can name a repository for its agent to clone:

```sh
wherry submit --prompt "Fix the failing test" \
  --git git@github.com:team/app.git@main \
  --credential-file deploy-key=id_ed25519 --git-credential deploy-key \
  --git-host-key github-known-hosts --git-changes patch
```

The agent runs `git` itself, from its image or host, with the commands the
prompt gives it; the receiver and the adapter run no git. The
[extension](a2a-extension-v1.md#git-input) defines the remote, ref and path
rules and the commands. For the agent to follow them:

- The image or host has `git` 2.30 or later, a shell, and `ssh` for `ssh`
  remotes. The prompt asks the agent to stop and report an older `git`.
- The agent may run `git` commands. Every command the prompt gives starts with
  `git`, so for Claude Code
  [`Bash(git:*)`](https://docs.anthropic.com/en/docs/claude-code/iam) in its
  allowed tools covers them, along with whatever else the task runs. The rule
  lets the agent run the commands; it is not a sandbox, since `git` can run any
  command.

Allow the hosts a pool's tasks may clone from with `vcs` in its
[terms](reference.md#pool-terms):

```yaml
vcs:
  - github.com
  - git.example.com:2222
```

`submit` and `continue` refuse a remote outside the VCS hosts before they
publish, and receivers reject one at admission.

VCS hosts limit what wherry accepts. The agent runs commands, and it can
reach any host its network allows. Enforce where agents connect with the
platform's egress policy ([Agent network reach](#agent-network-reach)),
and treat `vcs` as the check that a task names an allowed repository.

The clone's commands keep the credential for the remote's host. No
template seeds the clone. They reset
the credential helpers, so no helper of the image or host is asked for the
token or stores it; the clone's helper answers only for the remote's
scheme, host and port. They refuse HTTP redirects, so the remote must be
the repository's current URL, verify certificates, and allow only the
remote's protocol. `ssh`
reads no configuration file, uses no agent and no identity but the
credential, and trusts only the task's host key.

An adapter gives the agent an environment in which `git` reads no system or
global configuration and no configuration from the environment, uses no
template, never prompts, and traces nothing, since those could rewrite the
remote to another host or record the token. The agent has no global `git`
identity; the clone sets `user.name` to `wherry` and `user.email` to
`wherry@invalid`.

Some variables no value switches off: `GIT_SSH`, `GIT_SSH_COMMAND` and
`GIT_SSH_VARIANT` replace the clone's `ssh` command, `GIT_DIR`,
`GIT_WORK_TREE`, `GIT_COMMON_DIR`, `GIT_INDEX_FILE`, `GIT_OBJECT_DIRECTORY`
and `GIT_ALTERNATE_OBJECT_DIRECTORIES` retarget the repository a command
works on,
`GIT_EXEC_PATH` chooses the programs `git` runs, `GIT_SSL_NO_VERIFY`
disables certificate checks, and `GIT_CURL_VERBOSE` traces requests, with
the Authorization header redacted. An adapter removes them only from the
environment it configures, such as the Nomad runtime's `env`. It cannot
remove what the platform or the image sets: a Nomad `raw_exec` task
inherits the client's environment, and a container keeps its image's
`ENV`. Add them to the Nomad client's `env.denylist`, and keep them out of
agent images.

The CA and proxy settings stay yours: `GIT_SSL_CAINFO`, `SSL_CERT_FILE`,
`HTTPS_PROXY` and the like reach `git` as the image or platform sets them.

Start the agent's harness in a directory above the checkout, never inside
it. Claude Code in print mode skips the workspace trust prompt and runs the
hooks and MCP servers of the project it starts in, so a harness started in a
cloned repository runs that repository's hooks with the agent's credential in
reach. Started above the checkout, it reads the repository's files without
loading its project settings, including after the agent changes directory in
a shell. The start directory is the adapter's to set: on Nomad, a runtime
command such as `cd /local && exec claude ...` starts the harness in the task
directory, and the clone lands below it.

The isolation protects a cooperative agent from the configuration around it. It
is not a boundary against the agent: an agent that runs commands, including
through an allow rule such as
[`Bash(git:*)`](https://docs.anthropic.com/en/docs/claude-code/iam)
(`git -c alias.x='!cmd'` runs any command), can undo every one of these
settings. A hostile or prompt-injected agent holds whatever credentials the
parent sent, and what it can do with them is bounded by their scope and
lifetime, the platform's egress policy, and the receiver's redaction of what it
returns.

The parent supplies least privilege; wherry passes the credential as a
file and checks the host, nothing more:

- For `https`, a short-lived token for one repository, such as a GitHub App
  installation token. The credential helper answers with the user name
  `x-access-token`, which installation tokens require.
- For `ssh`, a deploy key for the repository, created for the run and removed
  afterwards where the forge allows it, and the forge's host key from a source
  you trust, passed as `--git-host-key`. Every entry must name the remote's
  host, or `[host]:port` for a port other than 22; hashed entries, and entries
  with a wildcard pattern even beside the host, are refused. `ssh` checks the
  host key strictly and ignores every other `known_hosts`.
- A public repository needs no credential.

With `--git-changes`, the agent returns its commits as `changes.patch` or
`changes.bundle`, which `await` writes like any output, and the parent
applies them with its own credentials:

```
apply with: git am /home/me/.config/wherry/state/runs/run-.../outputs/1/changes.patch
```

The agent pushes only when the prompt asks it to and the credential allows
it, and then only to the branch `wherry/<run>`; verify the commit it
reports at that branch before trusting it, and delete the branch after.
Forge rules that confine the credential's pushes to `wherry/*` make that
the only branch it can write.

`ssh` refuses a private key that others can read when the agent's user
owns the file, so the owner and mode an adapter gives credential files decide
whether a deploy key works; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-jobs)
says what it gives.

## Agent network reach

An agent runs commands, so it can connect to any host its network allows. The
pool's `vcs` and `storage` bound what a task names; the platform's egress
policy bounds where its agent connects. Give agent jobs a policy that:

- denies the control plane: the Nomad servers and clients, Consul, Vault,
  the Kafka brokers, the Kubernetes API, and cloud metadata addresses such
  as `169.254.169.254` and `fd00:ec2::254`;
- allows DNS, the model API the agent's harness calls (for Claude Code,
  `api.anthropic.com`), and the hosts in the pool's `vcs`;
- denies everything else.

wherry gives the agent no storage URL: none reaches its prompt, its
environment or its files. On Nomad the client fetches S3 inputs before the
task starts, and the receiver uploads S3 outputs after the task stops. The
Nomad clients and the receiver hosts need the storage endpoint; agent jobs
do not. The presigned GET is the source of an `artifact` in the agent job,
so an agent that can read its own job can read the URL: keep the Nomad API
out of the agent's reach, as below.

### On Nomad

The [Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-network-reach)
says how its agent jobs reach the network and how to confine them.

### On Kubernetes

Run agent pods under a service account with
`automountServiceAccountToken: false`, so no API credential is mounted beside
the agent. Apply a default-deny egress NetworkPolicy to the agent pods'
namespace, with rules that allow DNS and the allowed hosts. Policies take effect
only under a CNI that enforces them, and they are additive: check that no other
policy in the namespace allows the control plane or metadata addresses.
NetworkPolicy matches addresses only. To allow by host name, use the cluster's
DNS-aware egress rules, such as an OpenShift `EgressFirewall` with `dnsName` or
a Cilium FQDN policy. These authorise the addresses a name resolves to, so a
name on a shared or CDN address admits its neighbours too; restrict ports and
the DNS resolvers as well.

## Upgrade wherry

Readers reject records with unknown members or versions, so hosts and
receivers run the same release. A host's run records carry `redaction_key`,
so an older `wherry` cannot read a run this release submitted. A run an older
release submitted holds no redaction key, so this release answers no seal
request for it until `wherry continue` records its next turn. A host or
receiver of an earlier release cannot decode pool terms that set `models` or
`default_model`, and stops reading `wherry.admin` while they hold them, so
upgrade every receiver and host before the terms set them. An older release
reports such records as written by a later release; do not skip them, upgrade
it. A harness does not restart a running `wherry mcp` when its binary
changes, so restart the MCP server after upgrading.

Prerequisites: the new `wherry` binary, and each receiver's bundle and
state directory.

1. Replace the binary on every host and receiver host.
2. Replace the plugins in each receiver's plugin directory together with the
   receiver binary; a receiver refuses a plugin of another adapter protocol
   version. This release speaks adapter protocol version 6 and refuses a
   version 5 plugin, which would omit its inline input limit.
3. Restart each receiver with the same bundle and state directory. On
   `SIGINT` or `SIGTERM` it exits without stopping its jobs and releases its
   tasks to the pool. The next delivery of each task adopts the running
   job, on the restarted receiver or another member, and resumes the
   transcript from the last heartbeat offset.
4. Restart the harness sessions that run `wherry mcp`.

## Harness integration

`wherry mcp` serves the host operations as MCP tools over stdio. It reads
the client configuration and opens a Kafka connection for each tool call.
[Reference](reference.md#mcp-tools) lists the tools.

Prerequisites: `wherry` on the harness's `PATH`, and the client
configuration, bundle, host key and credential files readable by the
harness user.

1. Install the plugin. In Claude Code, add this repository as a
   marketplace, install the plugin from it, then run `/reload-plugins` in
   the session:

   ```sh
   claude plugin marketplace add benemon/wherry
   claude plugin install wherry@wherry
   ```

   Another harness installs `plugins/wherry` through its own plugin
   mechanism, then restarts its session.
2. Check the connection with the `receivers` tool, which lists the
   receivers the tenant may use.

The packaged `plugins/wherry/mcp.json` starts `wherry mcp` with the default
configuration path. For a configuration elsewhere, keep it at the default path
or link the default path to it, or register a server of your own that runs
`wherry mcp --config-file PATH` and disable the packaged one, for example with
[`claude mcp add wherry -- wherry mcp --config-file PATH`](https://docs.anthropic.com/en/docs/claude-code/mcp)
in Claude Code. A plugin update replaces the packaged `mcp.json`, so edits to it
do not last.

The server sends an input or `prompt_file` only from beneath its input
roots: the client configuration's `input_roots`, or its working directory
when that is unset. Every input root is a directory whose files the harness
can send to remote agents; list only directories meant for that.

`plugins/wherry` packages the server with the `wherry` Agent Skill:

- `plugin.json`, `mcp.json` and `skills/` form an Agent Plugins 1.0 bundle.
- `.claude-plugin/plugin.json` is the Claude Code manifest for the same
  skill and server, and the repository's `.claude-plugin/marketplace.json`
  lists it as the `wherry` marketplace.
- Other harnesses take `mcp.json` and `skills/wherry` directly.

`plugins/AGENTS.snippet.md` covers harnesses without skills, using the CLI.
