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

A pool's terms apply to every receiver in it. Write them as a
[terms file](reference.md#pool-terms) and create the pool:

```sh
wherry admin pool create build --terms build-terms.yaml
```

- `delivery_limit` bounds how often the broker delivers one task. Each
  start failure returns a task to the pool, so set it to at least the
  number of receivers in the pool, plus a margin. From attempt
  `delivery_limit` minus 1, a receiver ends a turn it would return to the
  pool `TASK_STATE_FAILED` with `delivery budget exhausted`, except in the
  cases the [extension](a2a-extension-v1.md#delivery-budget) lists. The
  broker caps it at
  `group.share.max.delivery.count.limit`.
- `lock_duration_ms` is how long a claim survives without renewal, and how
  long a denied receiver keeps running its jobs. Receivers renew every 10
  seconds.
- `seal_lifetime_ms` is required and unused: a seal expires with its
  [seal request](a2a-extension-v1.md#seal-request), 5 minutes after the
  receiver asks.

Running `pool create` again for an existing pool records the new terms.

## Add tenants and hosts

1. Entitle the tenant to a pool:

   ```sh
   wherry admin tenant add alice --pool build
   ```

2. Add each host the tenant submits from:

   ```sh
   wherry admin host add alice laptop --bundle alice-laptop.bundle
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
matching adapter records a start failure and returns the task.

The [Nomad plugin's settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
cover the `config` section.

1. Write the configuration and add the receiver:

   ```sh
   wherry admin receiver add nomad-1 --pool build \
     --config nomad-1 --bundle nomad-1.bundle
   ```

   When the platform does not issue the receiver's credential itself, add
   `--platform-credential ADAPTER=FILE` to put a token in the bundle for
   one adapter.

2. Deliver the bundle to the receiver through the platform's secret
   mechanism.

3. Start the receiver, as in [Run a receiver](#run-a-receiver).

### Change a receiver's configuration

```sh
wherry admin receiver configure nomad-1 --config nomad-1
```

The receiver exits when it reads the new record at its next configuration
check, which runs every 15 seconds, and its platform restarts it with the new configuration. Runs in flight are
adopted on redelivery. When the new configuration renames the adapter or
changes its agent, the run's job is stopped and purged, and the run is
dispatched again. The bundle and password do not change.

## Run a receiver

`wherry receiver --bundle FILE --state-dir DIR --plugin-dir DIR` needs:

- the bundle, readable by the receiver alone;
- a state directory that survives restarts. It holds the receiver's
  private keys, and a receiver that loses them cannot start under the same
  name again;
- a plugin directory holding the plugin for every adapter type in its
  configuration, as in [Install adapter plugins](#install-adapter-plugins);
- a supervisor that restarts the process when it exits, since a
  configuration change ends it;
- for the Nomad plugin, a Nomad token with the
  [receiver policy](#nomad-acl-policy), from the bundle or `NOMAD_TOKEN`
  in the receiver's environment.

On first start the receiver logs `registered keys`, then
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
        args    = ["receiver", "--bundle", "${NOMAD_SECRETS_DIR}/bundle.yaml",
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
[Install adapter plugins](#install-adapter-plugins). As the task sees them,
root or the receiver's user must own the directory and the binary, neither
group nor others may write them, and the binary must be a regular file. The plugin runs in the
receiver's task with its environment, so the workload identity's
`NOMAD_TOKEN` reaches it.

Nomad treats a sticky, migrating ephemeral disk as best effort. If the node
is lost, the keys go with it and the receiver is re-provisioned under a new
name; a host volume avoids that.

Attach the [receiver policy](#nomad-acl-policy) to the job:

```sh
nomad acl policy apply -namespace agents -job wherry-receiver \
  wherry-receiver receiver-policy.hcl
```

The adapter's `ca_file` is then `/secrets/nomad-ca.pem`, the path inside
the task.

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
pool's current terms, and confirms that every ACL the state calls for
exists. It does not check an adapter's `type` or `config` section; at
start the receiver finds the type's plugin, the plugin checks the section,
and the receiver exits with `adapter <name>: ...` on an error. Run it after changing a pool's terms, since receivers keep the
configuration they started with.

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
as a membership end.

## Upgrade to per-run seals

Upgrade every receiver and every host together. Hosts reject the
advertisements of earlier receivers, which carry `sealing_key`, and earlier
hosts reject the advertisements of upgraded ones, so neither places work on
the other. On first start an upgraded receiver deletes `sealing.key` from
its state directory; its key topic needs no change. Hosts may delete
`pins.json` and `pins.lock` from their state directory.

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
standard error. A turn adopted by a second receiver gets no seal and ends
`TASK_STATE_AUTH_REQUIRED`; `wherry continue` runs it again.

## Nomad ACL policy

The Nomad plugin's calls need this policy, with the namespace adjusted:

```hcl
namespace "agents" {
  capabilities = ["submit-job", "list-jobs", "read-job", "read-logs", "read-fs", "alloc-lifecycle"]

  variables {
    path "nomad/jobs/agent-*" {
      capabilities = ["write", "read", "destroy"]
    }
  }
}

node {
  policy = "read"
}

node_pool "default" {
  policy = "read"
}
```

- `submit-job`: register a run's job, deregister one that outlives its
  stop signal or its dispatch wait, and purge one that never started or
  has been dead for 10 minutes.
- `list-jobs`: find dead agent jobs to purge.
- `read-job`: read a job and its allocations.
- `read-logs`: stream the agent task's standard output.
- `read-fs`: list and read the agent task's outputs directory once the job
  has stopped.
- `alloc-lifecycle`: send `SIGTERM` to the agent task.
- Variables `write`, `read` and `destroy`: hold each run's prompt,
  credentials, input files and git context, and sweep them after the job
  ends. The sweep
  lists variables under `nomad/jobs/agent-`; see
  [Nomad variables](https://developer.hashicorp.com/nomad/docs/concepts/variables)
  for the access rules.
- `node` read: find a ready node with the runtime's driver healthy.
- `node_pool` read: register jobs in the node pools the adapters name.

## Inputs and outputs

A task's input files hold 256 KiB together, and a turn's outputs 8 MiB in
256 entries at most. An adapter can deliver less: the Nomad plugin carries
a turn's input files in one Nomad variable, which Nomad caps at 64 KiB of
item names and base64-encoded contents, so about 48 KiB of input. A turn
whose inputs an adapter cannot deliver ends `TASK_STATE_FAILED` naming the
input.

The agent creates `WHERRY_OUTPUT_DIR` when it writes its first output.
Outputs are collected once the agent writes its result line and the job is
stopped, including when that line reports a failure. A turn that ends
without the agent's result line, by timeout or otherwise, returns none.

## Git context

`wherry submit --git URL --git-ref REF [--git-path PATH]` asks the receiver
to check a repository out before the agent starts. The
[extension](a2a-extension-v1.md#git-context) defines the URL, ref and path
rules and the line added to the prompt.

The `git` task runs the runtime's own driver and configuration with a
`/bin/sh` script, so the image or host needs `/bin/sh`, `git`, and `ssh`
for SSH URLs. It fetches the ref at depth 1 into
`$NOMAD_ALLOC_DIR/<path>`.

The platform supplies host trust and repository access:

- For an HTTPS URL, the system git configuration and CA bundle.
- For an SSH URL, the host key in `/etc/ssh/ssh_known_hosts`. The task runs
  ssh with `StrictHostKeyChecking=yes` and `BatchMode=yes`.
- For a private repository, a deploy key or a git credential helper set up
  in the image or on the host.

`GIT_TERMINAL_PROMPT=0` stops git from asking for input. A failed checkout
ends the turn as `TASK_STATE_FAILED` with the message
`git context unavailable`. The repositories an adapter can reach vary by
platform; a label such as `git-access` lets tasks select for it.

## Upgrade wherry

Readers reject records with unknown members or versions, so hosts and
receivers run the same release.

Prerequisites: the new `wherry` binary, and each receiver's bundle and
state directory.

1. Replace the binary on every host and receiver host.
2. Restart each receiver with the same bundle and state directory. On
   `SIGINT` or `SIGTERM` it exits without stopping its jobs and releases its
   tasks to the pool. The next delivery of each task adopts the running
   job, on the restarted receiver or another member, and resumes the
   transcript from the last heartbeat offset.
3. Restart the harness sessions that run `wherry mcp`.

Agent jobs and variables named before pool-scoped names,
`agent-<run>-t<turn>`, are not swept; remove them by hand after upgrading.

## Harness integration

`wherry mcp` serves the host operations as MCP tools over stdio. It reads
the client configuration and opens a Kafka connection for each tool call.
[Reference](reference.md#mcp-tools) lists the tools.

`plugins/wherry` packages the server with the `wherry` Agent Skill:

- `plugin.json`, `mcp.json` and `skills/` form an Agent Plugins 1.0 bundle.
- `.claude-plugin/plugin.json` is the Claude Code manifest for the same
  skill and server.
- Other harnesses take `mcp.json` and `skills/wherry` directly.

`plugins/AGENTS.snippet.md` covers harnesses without skills, using the CLI.

The harness starts `wherry mcp` by name, so `wherry` must be on its `PATH`,
and the user running the harness needs the client configuration, the
bundle, the host key and the credential files.
