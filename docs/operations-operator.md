# Operator operations

## Prepare Kafka

wherry is tested against Kafka 4.3. `wherry operator` creates its topics, ACLs,
principals and group configurations; the cluster itself is prepared once.

- **Share groups** are enabled, so receivers can claim tasks. See
  [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka).
- **A TLS listener with SCRAM-SHA-512** is reachable from every host and
  receiver. Its address goes into every bundle; its CA does not. Each host
  and receiver trusts the broker through the system's roots, or a `ca_file`
  in its own configuration when the CA is private, kept current by whoever
  distributes the Kafka CA.
- **An authoriser** denies anything no ACL allows, and `super.users` names
  the Kafka super user, which `wherry operator init` uses once. See
  [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz).
- **`auto.create.topics.enable=false`**, so a mistyped topic name fails.
- **`connections.max.reauth.ms`** is set broker-wide on every broker.
  Revocation reads that key from each broker, waits at least the longest
  value, and fails while it is unbounded. A listener-prefixed override does
  not count.
- **`transaction.max.timeout.ms`** is short enough to wait out:
  revocation waits at least the longest of it,
  `connections.max.reauth.ms` and
  `group.share.max.record.lock.duration.ms`. See Kafka's
  [`transaction.max.timeout.ms`](https://kafka.apache.org/documentation/#brokerconfigs_transaction.max.timeout.ms)
  for its default.
- **One SCRAM principal** exists: the Kafka super user. `wherry operator
  init` creates `wherry.operator` and grants it its ACLs.

### Strimzi

On a [Strimzi](https://strimzi.io/documentation/) cluster, declare the
super user as a `KafkaUser` resource. `wherry operator` creates
`wherry.operator` and the host and receiver principals through the Kafka
Admin API, which Strimzi's User Operator removes unless it ignores them. Set
the User Operator's `STRIMZI_IGNORED_USERS_PATTERN` environment variable, as
[Ignoring specific users](https://strimzi.io/docs/operators/latest/deploying.html#con-ignoring-some-users-in-user-operator-str)
describes, to:

```
^wherry\.(operator|tnt\..+|rcv\..+)$
```

The pattern matches `wherry.operator`, every host principal
`wherry.tnt.<tenant>.<host>` and every receiver principal
`wherry.rcv.<receiver>`.

Strimzi issues and renews the cluster CA. The operator, hosts and receivers
name its certificate as their `ca_file`. When Strimzi replaces the CA
certificate, put the new one in every `ca_file` that names it:
`operator.yaml`'s, each host's and each receiver's.
[Certificate renewal](https://strimzi.io/docs/operators/latest/deploying.html#con-certificate-renewal-str)
gives Strimzi's timing.

## Set up `wherry operator`

Make one working directory per installation and run
[`wherry operator init`](commands.md#wherry-operator-init) in it with the
Kafka super user, from flags or, on a terminal, its prompts:

```sh
mkdir wherry-prod && cd wherry-prod
wherry operator init --broker kafka.example.com:9093 --ca-file /etc/wherry/kafka-ca.pem \
  --super-user kafka-admin --super-user-password-file /etc/wherry/admin-password
```

Leave out `--ca-file` when the listener's certificate chains to a system
root. `init` creates `wherry.operator` with a new password, grants it the
[ACLs provisioning needs](security.md#the-operator-principal), creates the
`wherry.operator` topic, and writes `operator.yaml` and
`operator.password`. The broker address is what host bundles name, so use
the address hosts reach; a receiver's file can name another. Every other
`wherry operator` command runs in the working directory, or with `--dir`,
and connects as `wherry.operator` alone. Keep the directory private: it
holds every bundle ([Security](security.md#the-operator-working-directory)).

To rotate `wherry.operator`'s password, run `init` again with the super
user. Commands then use the new password. When every operator command fails
to log in, an interrupted `init` left `operator.password` behind the
broker's: run `init` again with the Kafka super user.

### Concurrent operator commands

Do not run two operator commands at once. Concurrent operator commands are not supported.

Operator commands do not lock each other. Each one that changes Kafka, except `operator init`, ends
by converging Kafka on the records of `wherry.operator` as read to its end,
not on the ones it read when it started
([Operator convergence](design.md#operator-convergence)). When
`wherry.operator` keeps growing, the command fails after five passes with
`wherry.operator moved during each of 5 convergence passes`; run it again.

A command that returns without an error leaves Kafka matching every record
on `wherry.operator` when its last pass ended. A command still running can
change Kafka after that, but its own passes then follow. A command that
fails before it writes a record writes nothing to `wherry.operator`. Topics,
a new host's or receiver's credential, or a new receiver's grants it made
first stay, the grants until the next command's pass. A `tenant host add --force`
or receiver reissue that a revocation overtakes refuses with `cannot return
after its membership end` and writes no bundle. A command's pass deletes the
grants a concurrent command adding a receiver makes before its record. That
command's own pass restores them, and a peer denied meanwhile exits and is
restarted by its supervisor.

A recorded profile's signature topic exists before its record. A topic
missing afterwards was deleted outside wherry: `operator check` names it, and
`pool apply` of the pool with the same terms recreates it before its
no-update path. A receiver never creates a topic: its poll of a missing one logs an error
and retries every 2 seconds.

## Create a pool

Prerequisites: the working directory
([Set up `wherry operator`](#set-up-wherry-operator)).

A pool's terms apply to every receiver in it. Write them as a
[terms file](configuration-operator.md#pool-terms) at `pools/build.yaml` in the
working directory and create the pool:

```sh
wherry operator pool apply build
```

Without the file, on a terminal, `pool apply` asks for the agents, their
models and credentials, the default agent, the quotas and the start-by
limit, writes `pools/build.yaml`, then applies it. `-f PATH` applies a file
or directory elsewhere.

The [pool terms example](configuration-operator.md#pool-terms-example) sets every field,
as one file and as a directory. Each profile signature, an agent and all its
labels, gets its own task topic; a label change moves a profile to a new one,
as [Signatures](configuration-operator.md#pool-terms) describes.

- `delivery_limit` bounds how often the broker delivers one task. A task
  reaches only members with a ready profile of its signature, so the limit
  counts attempts, not hops. Each start failure returns a task to the pool and
  uses a delivery, and the member that failed may be handed the task again
  even when a peer with the same signature could start it. A profile that goes
  unready between a claim's subscription and its fetch has its task released.
  The unready profile's other replicas still polling may each acquire it once
  before they close their idle clients. For that readiness withdrawal, this
  allows up to the profile's effective replica count minus one more
  deliveries. A `replicas` value of 0 or an absent field means one replica.
  From
  attempt `delivery_limit` minus 1, a receiver ends a run it would return to the
  pool `TASK_STATE_FAILED` with `delivery budget exhausted`, except in the
  cases the [extension](a2a-extension-v1.md#delivery-budget) lists. `pool
  apply` refuses a `delivery_limit` above the broker's
  `group.share.max.delivery.count.limit`.
- `lock_duration` is how long a claim survives without renewal. Receivers
  renew every 10 seconds. `pool apply` refuses a `lock_duration` above the
  broker's `group.share.max.record.lock.duration.ms`.
- `start_by_limit` is the latest `task_started_by` a task may ask for, from
  its submission, and bounds how long after its submission a run's task is
  admitted. It defaults to 72 hours. For a pool with `storage`, it plus six
  hours must not exceed 7 days, the longest an S3 SigV4 presigned URL lasts.
  See [Retention](#retention).
- `reply_limit` bounds the redacted reply a result returns beyond the 64 KiB
  its text carries; it defaults to 1 MiB, or to `output_limit` when that is
  lower, and `pool apply` refuses one over `output_limit`,
  which also bounds the outcome file. A longer reply is cut to it, and the
  result says so.
- `output_limit` bounds a run's outputs; it defaults to 8 MiB. The
  broker's record limit bounds a task's inline input files, and
  `pool apply` records it in the terms. See
  [Size a pool against the broker](#size-a-pool-against-the-broker).
- An agent's `credentials` lists the credentials it takes, each
  `{name, env}` or `{name, file}`: the environment variable, or the file in
  the directory `WHERRY_CREDENTIALS_DIR` names, the agent finds it at.
  `required: false` lets a run go without it when the submitting host has no
  source for it. `pool apply` refuses a duplicate name, two credentials on
  one target, both or neither of `env` and `file`, and an `env` a plugin
  sets itself, such as `HOME` or `WHERRY_CREDENTIALS_DIR`:

  ```yaml
  agents:
    - name: codex
      credentials:
        - {name: openai-api-key, env: CODEX_API_KEY}
        - {name: github-token, env: GH_TOKEN, required: false}
  ```

- `vcs` lists the hosts a task's agent may clone from; without it the
  pool takes no git input. See [Git repositories](operations-hosts.md#git-repositories).
- An agent's `models` lists the models a task may name for it, and its
  optional `default_model`, one of them, the one a task naming none runs.
  An agent with one model defaults to it; with two or more `models` and no
  `default_model`, every task for the agent names a model. wherry does not
  interpret the names. The agent's profiles read the task's model from
  `WHERRY_MODEL`, as the [agent contract](configuration-receiver.md#agent-contract)
  describes. Without `models` a task names no model and the profile's
  command sets it.

### Change a pool's terms

Prerequisites: the working directory and the pool's new terms in
`pools/<pool>.yaml`.

1. Find the receivers the new terms would strand: a receiver in the pool
   whose membership has not ended, live or not, with a plugin profile whose
   agent the new terms drop.
   `wherry operator receiver show RECEIVER` prints each profile's `agent`.

2. Reconfigure each of those receivers without the profile, as in
   [Change a receiver's configuration](#change-a-receiver-s-configuration),
   or revoke it, as in [Revoke a host or receiver](#revoke-a-host-or-receiver).

3. When the change adds or removes an agent's `models`, drain the agent's
   runs first: the change rejects its queued and redelivered runs at their
   next claim.

4. When the change alters an agent's `credentials`, including a
   credential's target, stop submitting the agent's work, and let its
   queued and running work finish. Cancel work that must not run, and
   resubmit it after the change. A task is sealed under the terms its host
   read. A receiver that opens it under other terms can fail it. Suspending
   the pool does not drain queued work.

5. When the change alters an agent's `credentials`, suspend the pool:

   ```sh
   wherry operator pool suspend build
   ```

   The command waits only for the key custodian. For
   each running receiver in the pool, read a fresh metrics sample showing
   `wherry_claim_gated{reason="suspended"}` as `1`. A missing sample does
   not show that a receiver is stopped or suspended. Restart or stop a
   running receiver you cannot sample. A receiver reads `wherry.operator`
   when it starts, before it claims. Once a receiver has read the
   suspension, its claim gate stays closed until it reads the resume, and
   that read also carries the new terms. A claim it admitted just before
   reading the suspension can continue, which is why step 4 drains the
   agent's work first.

6. Record the terms:

   ```sh
   wherry operator pool apply build
   ```

   `pool apply` checks every receiver in the pool whose membership has not
   ended against the new terms first, and refuses terms that would strand one, naming each
   receiver, plugin and profile:

   ```
   wherry: pool terms would strand live receivers: receiver nomad-1 plugin nomad profile host: receiver agent is outside pool terms
   ```

   `--dry-run` makes the same checks and writes nothing. Receivers read
   new terms every 15 seconds, without a restart; a slow or failed read
   delays the update. A lowered `delivery_limit` applies to a task in
   flight at its next release.

7. If you suspended the pool in step 5, resume it:

   ```sh
   wherry operator pool resume build
   ```

   Then start submitting the agent's work again.

### Retention

Receivers reject a task still unclaimed `start_by_limit` after its
submission, and a task whose `task_started_by` is later, with
`task_started_by is past the pool's start_by_limit`; `wherry submit`
refuses such a `task_started_by` before it publishes. Every run ends
within six hours after `start_by_limit`. A longer limit keeps prompts on
the broker longer. Lowering `start_by_limit` leaves the pool topics'
retention where it is, so runs submitted under the longer limit keep their
records; `pool apply` raises the retention, and waits until the brokers
apply it, before it records a longer limit.

`pool apply` sets the retention of the pool's task and events topics, new and
existing, to at least `start_by_limit` plus the six-hour maximum run time plus a
one-day margin, their timestamp type to `LogAppendTime`, since a run's age
runs from the broker's time, their `retention.bytes` to `-1` and
`cleanup.policy` to `delete`, so only time deletes a record, and their `segment.ms` to one hour, so
retention deletes a record within about an hour of its bound. The deadlines
of replaced pool keys fall inside this retention
([Rotation](concepts.md#rotation)). The pool's snapshot topic is compacted
and keeps its latest snapshot forever; it holds public keys only. It sets the greatest task topic's retention on the progress
topics of the pool's live receivers. With the 72-hour default that is `367200000` ms, 4 days
and six hours, after which a run's prompts leave the broker. Tenant feeds keep
statuses and results, and tenant transcript topics transcripts, outputs and
replies over 64 KiB, for 30 days, and both must keep them for at least `start_by_limit` plus six
hours, so `pool apply` refuses a limit above 29 days and 18 hours, and
`tenant pool add` refuses a feed or transcript topic that keeps less. Identity and
configuration topics stay forever. `wherry operator check` reports a topic whose
retention drops below its bound, a pool topic whose timestamp type is not
`LogAppendTime`, and a task or events topic without the hourly
`segment.ms`, `cleanup.policy=delete` or `retention.bytes=-1`, or with more
than one partition. `pool apply` refuses a task or events topic with more
than one partition; partitions cannot be removed, so recreate the pool's topics.

A tenant transcript topic whose retention is shorter than its feed's loses an
old run's reply and transcript before its result: the result then keeps the
reply's first 64 KiB, and `wherry wait` reports the rest
`rejected by this host: gap`. `wherry operator check` does not report the
difference.

### Size a pool against the broker

Every task and every output or transcript chunk is one Kafka record, so
the broker's record limit bounds what a pool can carry. The Kafka cluster
must allow a topic `max.message.bytes` of 2 MiB (2097152): every tenant
feed and transcript topic has it, and receivers batch up to it. A result
names each output the run returned, so the largest result is a
1259677-byte record batch, over a broker's default limit. `tenant add` creates a tenant's feed and transcript topic at 2 MiB, and
`pool apply` sets existing ones to it. A
cluster policy that refuses the setting fails the command, and
`wherry operator check` reports a tenant topic below 2 MiB.

`pool apply` reads
`message.max.bytes` from every broker and `max.message.bytes` from the
pool's task topics and its tenants' feeds and transcript topics, records the
task topics' lowest record limit in the pool's terms, the brokers' and the
host's batch limit when it has none yet, signs the largest output and
transcript chunks and results, and refuses the terms when one is over the
limit of those tenant topics. A topic's own `max.message.bytes` overrides
the brokers' `message.max.bytes`. `wherry operator check` measures the tenant
topics as they stand, so a feed lowered to a broker's default since reads:

```
a result record is 1259677 bytes, over the 1048588-byte record limit
```

`wherry submit` measures each signed submission against the recorded
record limit. Input files grow by about 16/9 on the wire, since they are
base64-encoded inside a record that is itself base64url-encoded. A host
batches at most 1000012 bytes, franz-go's default batch maximum, which is
below the brokers' default `message.max.bytes` of 1048588, so the host's
batch is the task topics' limit with brokers and the topics at their
defaults. Raising `message.max.bytes` or a task
topic's `max.message.bytes` leaves the limit there, and lowering either
lowers it. Run `pool apply` after changing either, so the terms record the
new limit. An output chunk holds
256 KiB whatever `output_limit` is, so `output_limit` spans as many records
as it needs and the record limit does not bound it. A reply chunk is an
output chunk, so the same holds for `reply_limit`.

`tenant pool add` makes the same check with the new tenant's feed and
transcript topic counted.
Lowering a broker or topic limit later can leave a pool's terms too large;
[`wherry operator check`](#check-a-deployment) reports it. Receivers read
their pool's terms from `wherry.operator` every 15 seconds, so admission and
output collection use a changed limit within that window.

## Add tenants and hosts

Prerequisites: the working directory
([Set up `wherry operator`](#set-up-wherry-operator)) and the pool.
[Set up a host](install-host.md) walks through these steps for host
`laptop` of tenant `acme`, with its complete configuration.

1. Create the tenant and entitle it to a pool:

   ```sh
   wherry operator tenant add acme
   wherry operator tenant pool add acme build
   ```

2. Add each host the tenant submits from:

   ```sh
   wherry operator tenant host add acme laptop
   ```

3. Hand the bundle, `bundles/hosts/acme/laptop.bundle`, to the host's user over
   a channel you trust. It holds the host's Kafka password. When the
   broker's certificate chains to a private CA, the host also needs that
   CA's certificate, as `ca.pem` in its
   [configuration directory](configuration-host.md#client-configuration), by
   default `~/.config/wherry/ca.pem`, or at a path `wherry host init` asks
   for. `host init` asks only on a terminal, so without one put the
   certificate in place first; the bundle does not carry it.

The host's user then runs `wherry host init <bundle>`, which places the
bundle, registers the host key and writes the
[client configuration](configuration-host.md#client-configuration) with a source
for each credential the tenant's pools take, as in
[Set up a host](install-host.md#_2-initialise-the-host).

A tenant can be entitled to several pools; run `tenant pool add` once per pool.

To rotate a host's Kafka password, replace a lost bundle or change the
broker address a host uses, run `tenant host add` again with `--force`. It writes a
new bundle over the old one, and the old password stops working at once, so
the host is offline until its user puts the new bundle in place.
`receiver apply RECEIVER --force` does the same for a receiver.

## Add a receiver

A receiver runs a pool's tasks on the plugin profiles of its
[receiver configuration](configuration-receiver.md#receiver-configuration);
[Placement](concepts.md#placement) says how a task reaches one.
Give each host-local receiver, such as a systemd one, a `host` label of its own, as
[systemd](plugins.md#systemd) explains.
[Receiver on Nomad](install-receiver-nomad.md#run-the-receiver-in-nomad) adds receiver `nomad-1` and starts it
on Nomad.

Prerequisites: the working directory
([Set up `wherry operator`](#set-up-wherry-operator)), the pool, and a receiver
host that meets [Run a receiver](operations-receivers.md#run-a-receiver).

1. Write the configuration directory. This configuration of receiver
   `nomad-1`, against the
   [pool terms example](configuration-operator.md#pool-terms-example), runs `claude`
   in a container and `codex` under the `exec` driver. Its `plugin_sha256`
   pins the Nomad plugin binary's SHA-256, its line in the release's
   verified `checksums.txt` ([Install plugins](operations-receivers.md#install-plugins)); a
   `broker:` line would give the receiver a broker address other than
   `operator.yaml`'s. A single file at `receivers/nomad-1.yaml` in the
   working directory, as [Receiver on Nomad](install-receiver-nomad.md#run-the-receiver-in-nomad) writes,
   needs no `-f`:

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
   pool: build
   plugin_sha256:
     nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
   ```

   `plugins/nomad/plugin.yaml`, the `config` every profile inherits, with the
   name of the [platform credential](configuration-receiver.md#platform-credentials)
   that authenticates to the cluster:

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

   `plugins/nomad/profiles/20-host.yaml`, on a node pool whose nodes have
   Codex installed:

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

   [Receiver configuration directory](configuration-receiver.md#receiver-configuration-directory)
   gives the layout and recorded order, and
   [Plugin profile merge](configuration-receiver.md#plugin-profile-merge) how a
   profile inherits its plugin's `config` and `labels`. The
   [Nomad plugin's settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
   cover the `config` section, and its
   [agent jobs](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-jobs)
   how a command reads `WHERRY_MODEL`.
   [Codex's non-interactive mode](https://developers.openai.com/codex/noninteractive)
   covers the `codex exec` flags and the API key variable the pool's terms
   target.

2. Check the configuration. `--dry-run` validates the configuration
   against the pool's terms and prints the pins and each plugin profile as
   the record would hold them, writing nothing:

   ```sh
   wherry operator receiver apply nomad-1 -f nomad-1 --dry-run
   ```

3. Add the receiver:

   ```sh
   wherry operator receiver apply nomad-1 -f nomad-1
   ```

4. Deliver the bundle, `bundles/receivers/nomad-1.bundle`, to the receiver
   host over a channel you trust, such as the platform's secret mechanism.
   It holds the receiver's Kafka password and no CA: when the broker's
   certificate chains to a private CA, the receiver host also needs that
   CA's certificate, named as `ca_file` in its local configuration.

5. Install wherry and the receiver's plugins, as in
   [Install plugins](operations-receivers.md#install-plugins), and write the receiver's [local
   configuration](configuration-receiver.md#receiver-local-configuration) with a
   [source](security.md#credential-sources) for each platform credential its
   plugin profiles name. This local configuration gives `nomad-token` a
   file source:

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

6. Start the receiver under a supervisor that restarts it when it exits and
   allows it 30 seconds to stop, with `drain_timeout: 20s`, as in
   [Run a receiver](operations-receivers.md#run-a-receiver). A
   platform credential with no source fails that plugin profile's
   configuration, and the receiver exits with:

   ```
   profile container: credential "nomad-token" has no source in the receiver's configuration
   ```

7. On a host of an entitled tenant, check that `wherry host receivers` shows the
   receiver `live` with each profile `ready=true`.

To rotate a platform credential, replace the value at its source and
restart the receiver; the bundle stays as it is.

### Change a receiver's configuration

Prerequisites: the working directory and the receiver's new
configuration, with a `plugin_sha256` pin for each plugin type, as
[Install plugins](operations-receivers.md#install-plugins) takes it from the release's
`checksums.txt`.

1. Check the configuration. `--dry-run` validates the configuration
   against the pool's terms as `apply` does, and prints the pins and each
   plugin profile as the record would hold them, writing nothing:

   ```sh
   wherry operator receiver apply nomad-1 -f nomad-1 --dry-run
   ```

2. Compare the output with what the receiver runs now, which
   `wherry operator receiver show nomad-1` prints in the same format.

3. Record the configuration:

   ```sh
   wherry operator receiver apply nomad-1 -f nomad-1
   ```

4. At its next configuration check, within 15 seconds, the receiver stops
   claiming and closes its idle claims. Each run it holds goes on to its end
   and commits its result, with no time limit. The receiver then exits and
   its supervisor restarts it with the new configuration. Check that
   `wherry host receivers` shows it `live` with the new profiles.

The password stays as it is. When the configuration's `broker` differs from
the address in `bundles/receivers/nomad-1.bundle`, `receiver apply` rewrites
the bundle with the new address and the same password; deliver it to the
receiver host before the old address stops working. Without the bundle in
`bundles/receivers/`, `receiver apply` records nothing and fails;
`receiver apply nomad-1 --force` writes a new bundle with a new password. Planned changes go
through `receiver apply`, which drains with no window.

To apply a change at once, restart the receiver through its platform, as
with `kubectl rollout restart`, `nomad alloc restart` or
`systemctl restart`. It stops within its
[`drain_timeout`](configuration-receiver.md#drain-timeout-and-the-stop-window) plus
5 seconds, and
its held runs are released to other receivers, whose next delivery of each
task adopts its job. When the new configuration renames a run's profile or
changes its agent, the adopting receiver stops and purges the run's job and
dispatches the run again.

## Revoke a host or receiver

Prerequisites: the broker settings in [Prepare Kafka](#prepare-kafka), and
the operator's working directory. Other operator commands can run
meanwhile: the revocation deletes again, after its membership end record,
any credential or ACL a [concurrent command](#concurrent-operator-commands)
restored.

1. Run the revocation:

   ```sh
   wherry operator tenant host remove alice laptop
   wherry operator receiver remove nomad-1
   ```

   When it ends, the command prints one line per
   [revocation step](concepts.md#revocation) it ran,
   `step=<n> complete=<bool> name=<name>`. The wait in step 5 is usually the
   longest; `--wait DURATION` lengthens it, must be positive, and is
   refused below the broker-derived minimum.

2. If it fails with `revocation incomplete`, the line of the failed step
   ends in ` error=<cause>`. Check what that step depends on (the broker
   connection, the broker settings), then run the command again. It starts
   again from step 1 and continues from the revoking record the failed run
   left. A run that reached step 2 deleted every grant of the principal; a
   stale command can restore wherry-managed grants, which the next pass
   deletes again. A run that failed before step 2 may have left the
   principal's grants, and the run again deletes them. Step 3 already retries for up to 2 minutes while a
   broker still authorises the principal.

3. For a receiver, rotate at its source each credential of the runs it
   opened and, when it held its pool's key as key custodian, of every run
   whose envelope that key opens. The receiver has seen their values.

Revoking a receiver is a hard stop, during a drain too. At its first
authorisation error, or once it reads its membership ended, the receiver
exits non-zero with `denied <op>: <error>`. It leaves the runs it holds
unacknowledged and their jobs running; the broker delivers their tasks
again, and another member adopts each job. Its supervisor restarts it, and
it exits again at its first denial, until you stop it or remove it from the
platform.

A revoked name cannot be reused. Provision a returning host or receiver
under a new name.

## Respond to a compromised receiver or pool key

A suspect receiver, or a copy of a pool key, can open every retained task
under the keys it holds. The response contains the pool, moves its key
custody, and lists what was exposed so that its credentials are rotated at
their issuers.

Prerequisites: the broker settings in [Prepare Kafka](#prepare-kafka), and
the operator's working directory. A [concurrent operator command](#concurrent-operator-commands)
that read `wherry.operator` before the suspension can restore the task
topic grants it withdrew only until one of the two commands' passes
deletes them again.

1. Suspend the pool. It takes no new work: hosts stop submitting to it,
   receivers stop claiming from it, and the key custodian refuses the open
   request of a claim that is not adopting a running job.
   Running jobs complete. A suspension does not take back the credentials a
   running job holds; rotating them at their issuers, in step 5, cuts off
   running jobs' use of them. The command returns once the key custodian
   has acknowledged the suspension; after that no new start opens:

   ```sh
   wherry operator pool suspend build
   ```

2. Revoke the suspect receiver, as in
   [Revoke a host or receiver](#revoke-a-host-or-receiver). Revocation is
   the hard stop for a receiver:

   ```sh
   wherry operator receiver remove nomad-1
   ```

3. When the revoked receiver was the pool's key custodian, wait for a new
   one. Another receiver of the pool takes over and logs
   `custodian: published pool <pool> key <id> (takeover)`.

4. Rotate the pool key and list the tasks under the suspect keys: the key
   IDs the revoked receiver held, which are the names of its state
   directory's `pool-keys/<kid>.json` files without `.json`, and the key IDs
   of its
   `custodian: published pool <pool> key <id> (<reason>)` log lines:

   ```sh
   wherry operator pool rotate-key build --suspect-key <kid> --suspect-key <kid>
   ```

   The command refuses a pool that is not suspended. It waits up to 5
   minutes for the key custodian's snapshot rotated for the request,
   printing a progress line every 10 seconds, and then lists each task
   retained under those keys through that snapshot by run, tenant and
   credential names. The credential names are the task's own and every
   credential any terms of the pool have given its agent, so the list can
   name a credential the task's envelope did not hold but never omits one it
   held. The output states its horizon, the oldest retained task: tasks
   deleted by retention before it are not listed, and the output names any
   offsets retention deleted during the scan. When the wait ends without the
   snapshot, the request stands: wait for the new key custodian and run the
   command again.

5. Rotate each listed credential at its issuer, as in
   [Rotate a pool's credential](security.md#rotate-a-pool-s-credential).

6. Tenants cancel the listed runs with `wherry cancel RUN_ID`. A run
   cancelled while the pool is suspended ends `TASK_STATE_CANCELED` when a
   receiver claims it after the resume, without opening its envelope. A run
   to run again is resubmitted with `wherry submit` once the pool resumes,
   since a host refuses to submit to a suspended pool.

7. Resume the pool, last:

   ```sh
   wherry operator pool resume build
   ```

A drain of the key custodian while the pool is suspended keeps its keys to
their deadlines: it does not scan the task topics, so it re-keys nothing.

## Check a deployment

```sh
wherry operator check
```

It loads `wherry.operator`, validates every receiver configuration against its
pool's current terms, checks that every pool's largest records still fit the
broker's record limit, reports a tenant feed or transcript topic whose
`max.message.bytes` is below 2 MiB, reports topic retention below the pool's required
bounds, including an events or progress topic that keeps less than its
pool's greatest task topic, reports a profile of each receiver whose
membership has not ended whose signature topic is missing, checks each pool's task and events `segment.ms`,
`cleanup.policy`, `retention.bytes` and single partition and its
snapshot topic's configuration and single partition, and confirms that every
ACL the state calls for exists. It does not
check a plugin profile's plugin or `config` section; at start the receiver
finds the type's plugin, the plugin checks each profile's section, and the
receiver exits with `profile <name>: ...` on an error. Run it after changing
a pool's terms or a broker or topic limit: receivers apply both within 15
seconds, and `check` reports a receiver configuration or a record they no
longer fit.

## Reset a deployment

Every reader of `wherry.operator` stops while any record on it fails to
decode, and the error lists each such record as
`offset=<n> size=<n> sha256=<hex>`. When every such record has a higher `v`
than this release knows, a later release wrote it and the error says to
[upgrade wherry](upgrade.md#upgrade-wherry). Otherwise this release cannot read
`wherry.operator`, and the deployment needs a reset: `wherry.operator` is
append-only and read from offset 0, so no record on it can be passed over.

No `wherry` command reads state while this holds, so the reset works on the
platform and on Kafka directly.

1. Stop every receiver and every `wherry host mcp` and host command.
2. Stop and remove every run's platform job (the Nomad job, Kubernetes pod
   or systemd unit each [plugin](plugins.md) creates for a run). Once the
   task topics are gone no receiver can adopt these jobs or end them at
   their deadlines.
3. As the super user, delete what `wherry operator` created: every topic whose
   name starts with `wherry.`, including `wherry.operator`, as
   [Kafka resources](design.md#kafka-resources) lists them; the share groups
   and consumer groups on those topics; every ACL and SCRAM credential of a
   principal named `wherry.rcv.<receiver>` or `wherry.tnt.<tenant>.<host>`;
   and the client quotas set on the `wherry.tnt.` principals. Keep the super
   user, `wherry.operator` and its ACLs.
4. Run [`wherry operator init`](#set-up-wherry-operator) again, which
   creates `wherry.operator`, then set the deployment
   up again from [Create a pool](#create-a-pool), applying the kept files in
   `pools/` and `receivers/`, and give each host and receiver its new
   bundle.

## Tenant onboarding and offboarding

Run one operator command at a time.

To onboard a tenant:

1. Run `wherry operator tenant add acme`.
2. Run `wherry operator tenant pool add acme build`.
3. Run `wherry operator tenant host add acme laptop`.
4. Deliver the bundle over a channel you trust.
5. Run `wherry host init laptop.bundle` on the host.

Collect results before removing an entitlement.
`wherry operator tenant pool remove acme build` withdraws the hosts' pool access.
Hosts lose access to those runs through `status`, `wait` and `logs`.
They also lose the ability to cancel them.
A command whose state load overlaps removal can fail while reading receiver keys.
Run the next command with fresh state.

To offboard:

1. Remove one host with `wherry operator tenant host remove acme laptop`.
   This waits for the broker-derived revocation minimum.
2. To remove the whole tenant, run `wherry operator tenant remove acme`.
   This ends all entitlements before revoking the hosts.
   Host revocations include the broker-derived wait.
   The command removes the tenant's bundle directory after successful removal.
3. Retry a failed removal command to complete its remaining work.

An ended tenant name cannot be reused.
Removal keeps retained data.
See [Removal and retained data](security.md#removal-and-retained-data).
