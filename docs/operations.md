# Operations

Before a tenant can submit, an operator prepares the Kafka cluster,
creates a pool with its terms, entitles the tenant, adds its hosts, and
adds and runs at least one receiver. Later the operator reconfigures
receivers, revokes access and recovers from faults. This guide covers each
task. [Key concepts](concepts.md) explains the model,
[Commands](commands.md) and [Configuration](configuration.md) list every
command, file and field, and [Security](security.md#run-credentialed-tasks)
covers credentialed tasks.

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
export WHERRY_TRUST_OPERATOR_PASSWORD_FILE=/etc/wherry/operator-password
```

## Create a pool

Prerequisites: the `wherry admin` environment
([Set up `wherry admin`](#set-up-wherry-admin)).

A pool's terms apply to every receiver in it. Write them as a
[terms file](configuration.md#pool-terms) and create the pool:

```sh
wherry admin pool create build --config-file build-terms.yaml
```

The [pool terms example](configuration.md#pool-terms-example) sets every field,
as one file and as a `--config-dir` directory.

- `delivery_limit` bounds how often the broker delivers one task. Each
  start failure, and each member with no matching plugin profile, returns a task
  to the pool and uses a delivery. A member handed back a task it failed to
  start releases it again, and that release uses a delivery too. Size it as
  the pool's members times the attempts each may make, plus a margin. From
  attempt `delivery_limit` minus 1, a receiver ends a run it would return to the
  pool `TASK_STATE_FAILED` with `delivery budget exhausted`, except in the
  cases the [extension](a2a-extension-v1.md#delivery-budget) lists. `pool
  create` refuses a `delivery_limit` above the broker's
  `group.share.max.delivery.count.limit`.
- `lock_duration` is how long a claim survives without renewal, and how
  long a denied receiver keeps running its jobs. Receivers renew every 10
  seconds.
- `max_queue_deadline` is the longest queue deadline a task may ask for,
  and bounds how long after its submission a run's task is admitted. It
  defaults to 14 days. See [Retention](#retention).
- `reply_limit` bounds the redacted reply a result returns beyond the 64 KiB
  its text carries; it defaults to 1 MiB, or to `output_limit` when that is
  lower, and `pool create` and `pool update` refuse one over `output_limit`,
  which also bounds the outcome file. A longer reply is cut to it, and the
  result says so.
- `output_limit` bounds a run's outputs; it defaults to 8 MiB. The
  broker's record limit bounds a task's inline input files, and
  `pool create` records it in the terms. See
  [Size a pool against the broker](#size-a-pool-against-the-broker).
- An agent's `credentials` lists the credentials it takes, each
  `{name, env}` or `{name, file}`: the environment variable, or the file in
  the directory `WHERRY_CREDENTIALS_DIR` names, the agent finds it at.
  `required: false` lets a run go without it when the submitting host has no
  source for it. `pool create` refuses a duplicate name, two credentials on
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
  pool takes no git input. See [Git repositories](#git-repositories).
- An agent's `models` lists the models a task may name for it, and its
  optional `default_model`, one of them, the one a task naming none runs.
  An agent with one model defaults to it; with two or more `models` and no
  `default_model`, every task for the agent names a model. wherry does not
  interpret the names. The agent's profiles read the task's model from
  `WHERRY_MODEL`, as the [agent contract](configuration.md#agent-contract)
  describes. Without `models` a task names no model and the profile's
  command sets it.

### Change a pool's terms

Prerequisites: the `wherry admin` environment and the pool's new terms
file.

1. Find the receivers the new terms would strand: a live receiver in the
   pool with a plugin profile whose agent the new terms drop.
   `wherry admin receiver show RECEIVER` prints each profile's `agent`.

2. Reconfigure each of those receivers without the profile, as in
   [Change a receiver's configuration](#change-a-receiver-s-configuration),
   or revoke it, as in [Revoke a host or receiver](#revoke-a-host-or-receiver).

3. When the change adds or removes an agent's `models`, drain the agent's
   runs first: the change rejects its queued and redelivered runs at their
   next claim.

4. Record the terms:

   ```sh
   wherry admin pool update build --config-file build-terms.yaml
   ```

   `pool update` checks every live receiver in the pool against the new
   terms first, and refuses terms that would strand one, naming each
   receiver, plugin and profile:

   ```
   wherry: pool terms would strand live receivers: receiver nomad-1 plugin nomad profile host: receiver agent is outside pool terms
   ```

   `--dry-run` makes the same checks and writes nothing. Receivers apply
   the new terms within 15 seconds, without a restart. A lowered
   `delivery_limit` applies to a task in flight at its next release.

### Retention

Receivers reject a task still unclaimed `max_queue_deadline` after its
submission with `run expired`, and a task whose queue deadline is longer with
`queue deadline exceeds the pool's max queue deadline`; `wherry submit`
refuses such a queue deadline before it publishes. Every run therefore ends
within six hours after `max_queue_deadline`. A longer maximum keeps prompts on
the broker longer. Lowering `max_queue_deadline` of a pool with runs in progress lowers its topics'
retention at once: Kafka then deletes the records of runs older than the new
bound, and receivers reject a redelivered task whose submission is gone.

`pool create` sets the retention of the pool's tasks and events topics, new and
existing, to `max_queue_deadline` plus the six-hour maximum run time plus a
one-day margin, and their timestamp type to `LogAppendTime`, since a run's age
runs from the broker's time. It sets the same retention on the progress
topics of the pool's live receivers. With the 14-day default that is `1317600000` ms, 15 days
and six hours, after which a run's prompts leave the broker. Tenant feeds keep
statuses and results, and tenant transcript topics transcripts, outputs and
replies over 64 KiB, for 30 days, and both must keep them for at least `max_queue_deadline` plus six
hours, so `pool create` refuses a maximum above 29 days and 18 hours, and
`pool entitle` refuses a feed or transcript topic that keeps less. Identity and
configuration topics stay forever. `wherry admin check` reports a topic whose
retention drops below its bound, and a pool topic whose timestamp type is not
`LogAppendTime`.

A tenant transcript topic whose retention is shorter than its feed's loses an
old run's reply and transcript before its result: the result then keeps the
reply's first 64 KiB, and `wherry wait` reports the rest
`rejected by this host: gap`. `wherry admin check` does not report the
difference.

### Size a pool against the broker

Every task and every output or transcript chunk is one Kafka record, so
the broker's record limit bounds what a pool can carry. `pool create` reads
`message.max.bytes` from every broker and `max.message.bytes` from the
pool's tasks topic and its tenants' feeds and transcript topics, records the
tasks topic's record limit in the pool's terms, signs the largest output and
transcript chunks and results, and refuses the terms when one is over the
limit of those tenant topics. A transcript chunk can be 933508 bytes, so keep
each tenant feed's and transcript topic's `max.message.bytes` at or above
that:

```
a transcript chunk record is 933508 bytes, over the 900000-byte record limit
```

`wherry submit` measures each signed submission against the recorded
record limit. Input files grow by about 16/9 on the wire, since they are
base64-encoded inside a record that is itself base64url-encoded. wherry
producers batch at most 1000012 bytes, which is the limit with brokers and
topics at their defaults. Raising `message.max.bytes` or
`max.message.bytes` leaves the limit there, and lowering either lowers it.
Run `pool update` after changing either, so the terms record the new
limit. An output chunk holds
256 KiB whatever `output_limit` is, so `output_limit` spans as many records
as it needs and the record limit does not bound it. A reply chunk is an
output chunk, so the same holds for `reply_limit`.

`pool entitle` makes the same check with the new tenant's feed and
transcript topic counted.
Lowering a broker or topic limit later can leave a pool's terms too large;
[`wherry admin check`](#check-a-deployment) reports it. Receivers read
their pool's terms from `wherry.admin` every 15 seconds, so admission and
output collection use a changed limit within that window.

## Add tenants and hosts

Prerequisites: the `wherry admin` environment
([Set up `wherry admin`](#set-up-wherry-admin)) and the pool.
[Quickstart: Host](quickstart.md#host) walks through these steps for host
`laptop` of tenant `acme`, with its complete configuration.

1. Entitle the tenant to a pool:

   ```sh
   wherry admin pool entitle build --tenant acme
   ```

2. Add each host the tenant submits from:

   ```sh
   wherry admin host add laptop --tenant acme --bundle-file laptop.bundle
   ```

3. Hand the bundle to the host's user over a channel you trust. It holds
   the host's Kafka password.

The tenant then writes its
[client configuration](configuration.md#client-configuration), with a source
for each credential the pool's agents take, and runs `wherry init`, as in
[Quickstart](quickstart.md#_3-write-the-host-configuration).

A tenant can be entitled to several pools; run `pool entitle` once per pool.

## Add a receiver

A receiver runs a pool's tasks on the plugin profiles of its
[receiver configuration](configuration.md#receiver-configuration);
[Placement](concepts.md#placement) says how a task reaches one.
[Quickstart: Receiver](quickstart.md#receiver) walks through adding and
starting receiver `nomad-1`, with every file it writes.

Prerequisites: the `wherry admin` environment
([Set up `wherry admin`](#set-up-wherry-admin)), the pool, and a receiver
host that meets [Run a receiver](#run-a-receiver).

1. Write the configuration directory. This configuration of receiver
   `nomad-1`, against the
   [pool terms example](configuration.md#pool-terms-example), runs `claude`
   in a container and `codex` under the `exec` driver:

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

`plugins/nomad/plugin.yaml`, the `config` every profile inherits, with the
name of the [platform credential](configuration.md#platform-credentials)
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

[Receiver configuration directory](configuration.md#receiver-configuration-directory)
gives the layout and routing order, and
[Plugin profile merge](configuration.md#plugin-profile-merge) how a
profile inherits its plugin's `config` and `labels`. The
[Nomad plugin's settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
cover the `config` section, and its
[agent jobs](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-jobs)
how a command reads `WHERRY_MODEL`.
[Codex's non-interactive mode](https://developers.openai.com/codex/noninteractive)
covers the `codex exec` flags and the API key variable the pool's terms
target.

2. Check the configuration. `--plugin-sha256 nomad=HEX` pins the Nomad
   plugin binary's SHA-256, its line in the release's verified
   `checksums.txt` ([Install plugins](#install-plugins)). `--dry-run`
   validates the configuration against the pool's terms and prints the
   pins and each plugin profile as the record would hold them, writing
   nothing:

   ```sh
   wherry admin receiver add nomad-1 --pool build --config-dir nomad-1 \
     --plugin-sha256 nomad=<sha256> --dry-run
   ```

3. Add the receiver:

   ```sh
   wherry admin receiver add nomad-1 --pool build --config-dir nomad-1 \
     --plugin-sha256 nomad=<sha256> --bundle-file nomad-1.bundle
   ```

4. Deliver the bundle to the receiver host over a channel you trust, such
   as the platform's secret mechanism. It holds the receiver's Kafka
   password.

5. Install wherry and the receiver's plugins, as in
   [Install plugins](#install-plugins), and write the receiver's [local
   configuration](configuration.md#receiver-local-configuration) with a
   [source](security.md#credential-sources) for each platform credential its
   plugin profiles name. This local configuration gives `nomad-token` a
   file source:

```yaml
bundle: /etc/wherry/receiver.bundle
state_dir: /var/lib/wherry/state
plugin_dir: /usr/local/libexec/wherry/plugins
metrics_addr: ":9464"
drain_timeout: 25s
credentials:
  nomad-token:
    file: /etc/wherry/nomad-token
```

Start the receiver under a supervisor that restarts it when it exits and
allows it 30 seconds to stop, with `drain_timeout: 25s`, as in
[Run a receiver](#run-a-receiver). A
platform credential with no source fails that plugin profile's
configuration, and the receiver exits with:

```
profile container: credential "nomad-token" has no source in the receiver's configuration
```

6. On a host of an entitled tenant, check that `wherry receivers` shows the
   receiver `live` with each profile `ready=true`.

To rotate a platform credential, replace the value at its source and
restart the receiver; the bundle stays as it is.

### Change a receiver's configuration

Prerequisites: the `wherry admin` environment and the receiver's new
configuration.

1. Check the configuration. `--plugin-sha256` gives the pin of each
   plugin type, as [Install plugins](#install-plugins) takes it from the
   release's `checksums.txt`. `--dry-run` validates the configuration
   against the pool's terms as `update` does, and prints the pins and each
   plugin profile as the record would hold them, writing nothing:

   ```sh
   wherry admin receiver update nomad-1 --config-dir nomad-1 --plugin-sha256 nomad=<sha256> --dry-run
   ```

2. Compare the output with what the receiver runs now, which
   `wherry admin receiver show nomad-1` prints in the same format.

3. Record the configuration:

   ```sh
   wherry admin receiver update nomad-1 --config-dir nomad-1 --plugin-sha256 nomad=<sha256>
   ```

4. At its next configuration check, within 15 seconds, the receiver stops
   claiming and closes its idle claims. Each run it holds goes on to its end
   and commits its result, with no time limit. The receiver then exits and
   its supervisor restarts it with the new configuration. Check that
   `wherry receivers` shows it `live` with the new profiles.

The bundle and password stay as they are. Planned changes go through
`receiver update`, which drains with no window.

`receiver update --force` is a hard stop. The receiver releases the tasks
it holds at once and exits, leaving their jobs running, and the next
delivery of each task adopts its job. When the new configuration renames a
run's profile or changes its agent, the adopting receiver stops and purges
the run's job and dispatches the run again:

```sh
wherry admin receiver update nomad-1 --config-dir nomad-1 --plugin-sha256 nomad=<sha256> --force
```

A `--force` update during a drain ends it.

## Run a receiver

`wherry receiver run --config-file FILE` needs:

- its [local configuration](configuration.md#receiver-local-configuration),
  at `/etc/wherry/config.yaml` unless `--config-file` names another file;
- the bundle, readable by the receiver alone;
- a state directory that survives restarts. It holds the receiver's
  private keys, and a receiver that loses them cannot start under the same
  name again;
- a plugin directory holding every plugin in its
  configuration, as in [Install plugins](#install-plugins);
- a source for each [platform credential](configuration.md#platform-credentials) its
  plugin profiles name;
- a supervisor that restarts the process when it exits, since a
  configuration change ends it, and that allows it 30 seconds to stop
  after `SIGTERM` before a forced kill, with `drain_timeout` in the local
  configuration set below that window, such as `25s`
  ([Drain timeout and the stop window](configuration.md#drain-timeout-and-the-stop-window));
- for the Nomad plugin, a Nomad token with the
  [receiver policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy), from a platform credential or `NOMAD_TOKEN`
  in the receiver's environment.

On first start the receiver logs `registered key: signing=<key id>`, then
`configured: pool=<pool> profiles=<n> capacity=<n>`. It logs
`waiting for configuration` while its record is missing. After its first
advertisement, `wherry receivers` on a host of an entitled tenant shows it
`live`.

A plugin that exits is started again and its profiles configured again;
the receiver logs `plugin exited; restarting`, then `plugin restarted`.
[Plugins](commands.md#plugins) covers what runs in flight
meanwhile.

### Install plugins

The receiver ships with no plugins. Each plugin has its own
[wherry-plugins releases](https://github.com/benemon/wherry-plugins/releases),
tagged `<plugin>/<version>`, such as `nomad/<version>`. Each carries
`wherry-plugin-<plugin>_<version>_<os>_<arch>.tar.gz` for `amd64` and
`arm64`, `checksums.txt` and `checksums.txt.sigstore.json`. The `os` is
`linux` for every plugin, and also `darwin` for the Nomad and Kubernetes
plugins, which a receiver on macOS runs on its host. Download the
archive and the checksums, verify the checksums' signature with
[cosign](https://docs.sigstore.dev/cosign/system_config/installation/), check
the archive against them, and extract the plugin:

```sh
plugin=nomad version=<version> os=linux arch=amd64
base=https://github.com/benemon/wherry-plugins/releases/download/$plugin/$version
archive=wherry-plugin-${plugin}_${version}_${os}_${arch}.tar.gz
curl -fsSL --remote-name-all "$base/$archive" "$base/checksums.txt" "$base/checksums.txt.sigstore.json"
cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp "^https://github\.com/benemon/wherry-plugins/\.github/workflows/release\.yml@refs/tags/$plugin/v" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
grep "  $archive\$" checksums.txt | sha256sum -c -
tar -xzf "$archive" --strip-components=1 "${archive%.tar.gz}/wherry-plugin-$plugin"
```

On macOS, `shasum -a 256 -c -` takes the place of `sha256sum -c -`.

`gh attestation verify "$archive" -R benemon/wherry-plugins` verifies the
archive's provenance attestation in place of the signature and checksum steps.

Install each plugin as `wherry-plugin-<type>` in one directory on the
receiver host. A symbolic link to the binary is accepted:

```sh
install -d /usr/local/libexec/wherry/plugins
install -m 0755 wherry-plugin-nomad /usr/local/libexec/wherry/plugins/
```

Then pin the binary's SHA-256 on the receiver's record. `checksums.txt`
holds a line for each plugin binary,
`wherry-plugin-<plugin>_<version>_<os>_<arch>`, beside the archive's; the
binary in the archive has that SHA-256. With the `wherry admin` environment
and the receiver's configuration, and the verified `checksums.txt`:

```sh
sha256=$(awk -v name="wherry-plugin-${plugin}_${version}_${os}_${arch}" '$2 == name {print $1}' checksums.txt)
wherry admin receiver update nomad-1 --config-dir nomad-1 --plugin-sha256 "$plugin=$sha256"
```

Give `--plugin-sha256` once for each plugin type the configuration names.
The receiver drains and restarts as in
[Change a receiver's configuration](#change-a-receivers-configuration), and
starts each plugin only from a binary whose SHA-256 is the pin.
[go-plugin's `SecureConfig`](https://pkg.go.dev/github.com/hashicorp/go-plugin#SecureConfig)
checks it before every start and restart. A binary whose SHA-256 differs
fails with
`profile <name>: plugin type <type>: <path> has SHA-256 <hex> and the pin is <hex>`,
and a type with no binary with `profile <name>: no plugin for type <type>`.

An upgrade or rollback takes the same steps with the other version: verify
its `checksums.txt`, install the binary, then record its pin with
`receiver update`. Until the receiver restarts on the new record, a plugin
restart refuses the new binary.

To check the install, start the receiver and find one line per plugin,
logged when the receiver starts it:

```text
receiver: <date> <time> plugin <type>: version <version> protocol <n>
```

A plugin that leaves `pluginsdk.Version` unset logs `version unreported`.

### As a Nomad job

On Nomad, the receiver runs as a service job whose
[workload identity](https://developer.hashicorp.com/nomad/docs/concepts/workload-identity)
gives the Nomad plugin its token. The Nomad plugin owns those jobs:
[Run the receiver in Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#run-the-receiver-in-nomad)
has the receiver container under the docker driver and the exec driver job
on versioned host paths, each with its steps, and CI validates both.

### Metrics

Set `metrics_addr` in the receiver's [local
configuration](configuration.md#receiver-local-configuration) to serve its metrics at
`/metrics` in the Prometheus text format. The endpoint is plain HTTP with no
authentication, so bind it to an address only the scraper reaches, such as
the receiver host's address on a monitoring network, or put an
authenticating proxy in front of it:

```yaml
metrics_addr: 10.0.8.21:9464
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
runs and their outcomes, durations, rejections and seal waits. Give scrape access only to those who may
see all of the pool's tenants. No label or value carries a prompt, output,
run ID, error text, URL, image, path, node or credential name.
[Metrics](commands.md#metrics) lists the families.

[`contrib/observability`](https://github.com/benemon/wherry/tree/main/contrib/observability)
holds a Grafana dashboard, `wherry-dashboard.json`, and Prometheus alert
rules, `wherry-alerts.yaml`, built from these families. Each release archive
carries them under `contrib/observability/`. The dashboard shows receivers
and capacity in use, admissions, rejections and start failures, run
durations and queue wait, seal waits, plugin restarts, platform errors and
cleanup, and Kafka errors, for the pools and receivers its `pool` and
`instance` variables select. [Runbooks](runbooks.md) has a section for each
rule, which its `runbook_url` links.

Prerequisites: Prometheus scraping each receiver under the job name
`wherry-receiver`, as above, and Grafana with that Prometheus as a data
source.

1. Copy `wherry-alerts.yaml` to the Prometheus server, name it in
   `rule_files`, and reload Prometheus:

   ```yaml
   rule_files:
     - /etc/prometheus/rules/wherry-alerts.yaml
   ```

   With another job name, change `job="wherry-receiver"` in the rule
   `WherryReceiverNotAdvertising`.

2. Import `wherry-dashboard.json` in Grafana, under Dashboards, New,
   Import, and choose the Prometheus data source for its `datasource`
   variable.

3. Route the rules' `severity` label, `critical` or `warning`, in
   Alertmanager.

Each runbook states what its rule counts and the threshold it fires at.
Adjust a threshold or window in your copy of the rules to suit a pool's
volume.

#### A receiver that is not running

The shipped rules match series a receiver's scrape target returns. When
service discovery drops the target because the platform no longer runs
the receiver, Prometheus marks the target's series
[stale](https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness)
and they disappear, so `WherryReceiverNotAdvertising` has nothing to
match. A target in `static_configs` stays, and its `up` of 0 fires the
rule.

The platform reports an absent receiver. `wherry-alerts.yaml` names
wherry's series alone, so add these alerts to your own rule files:

- [`absent_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#absent_over_time)
  returns a series when no `wherry-receiver` target is left:
  `absent_over_time(up{job="wherry-receiver"}[5m])`. It fires only once
  every target of the job is gone.
- On Kubernetes,
  [kube-state-metrics](https://github.com/kubernetes/kube-state-metrics/blob/main/docs/metrics/workload/deployment-metrics.md)
  reports `kube_deployment_status_replicas_available` for the receiver's
  Deployment.
- On Nomad, the servers'
  [telemetry](https://developer.hashicorp.com/nomad/docs/operations/metrics-reference)
  reports `nomad_nomad_job_summary_running` for the receiver's job and task
  group.
- Under systemd, node_exporter's
  [systemd collector](https://github.com/prometheus/node_exporter#disabled-by-default),
  which is off by default, reports `node_systemd_unit_state` for the
  receiver's unit.

Each tells one receiver from the others where each runs as its own
Deployment, job or unit. Link these alerts to the
[WherryReceiverNotAdvertising](runbooks.md#wherryreceivernotadvertising)
runbook.

## Revoke a host or receiver

Prerequisites: the broker settings in [Prepare Kafka](#prepare-kafka), and
the `wherry admin` environment.

1. Run the revocation:

   ```sh
   wherry admin host revoke laptop --tenant alice
   wherry admin receiver revoke nomad-1
   ```

   When it ends, the command prints one line per
   [revocation step](concepts.md#revocation) it ran,
   `step=<n> complete=<bool> name=<name>`. The wait in step 5 is usually the
   longest; `--wait DURATION` replaces it and must be positive.

2. If it fails with `revocation incomplete`, the line of the failed step
   ends in ` error=<cause>`. Check what that step depends on (the broker
   connection, the broker settings), then run the command again. It starts
   again from step 1. Step 3 already retries for up to 2 minutes while a
   broker still authorises the principal.

3. For a receiver, rotate each credential sealed to it at its source. The
   receiver has seen their values.

Receivers stop reading a revoked host's topic, so no seal it writes reaches
them.

Revoking a receiver is a hard stop of the runs it holds, during a drain
too. Once its access is gone, the receiver gives up a run at its first
failed write and leaves the job for an adopter. It stops the job of a run
that writes nothing one lock duration after its first denial, since it
cannot tell a revocation from a passing denial and no other member can
take the task before the lock expires, then spends at most 5 seconds
recording the run as failed before it releases the task.

A revoked name cannot be reused. Provision a returning host or receiver
under a new name.

## Rotate a key

A rotation replaces a host's or receiver's signing key and keeps its name.
Readers keep verifying the feed and transcript records the old key signed
before the rotation. For a key that may be compromised, revoke the host or
receiver instead.

For a host:

1. Wait for the runs the host submitted to end. `wherry status` lists them.
2. Stop every other wherry process of the host, such as the MCP server and
   any `wherry wait`. They hold the old key.
3. Run the rotation:

   ```sh
   wherry key rotate
   ```

   It prints `old_key_id=<id> new_key_id=<id>`. It refuses while a run the
   host submitted has not ended, and names the run.

For a receiver, restart it once with `--rotate-key`:

```sh
wherry receiver run --rotate-key
```

It rotates `signing.key` before it claims anything and logs
`rotated key: old=<id> new=<id>`. Start it without the flag afterwards.

A rotation writes the new key beside the old one, as `<host_key>.next` or
`signing.key.next`, before it publishes anything. When a rotation stops
part way, the next host command, or the receiver's next start, settles the
`.next` file from the key topic: it keeps the new key when the topic names
it current and removes the `.next` file when the topic still names the old
key. Leave the `.next` file in place; removing it by hand after the
rotation was published loses the key.

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
bounds, including a receiver's progress topic that keeps less than its
pool's tasks topic, and confirms that every ACL the state calls for exists. It does not
check a plugin profile's plugin or `config` section; at start the receiver
finds the type's plugin, the plugin checks each profile's section, and the
receiver exits with `profile <name>: ...` on an error. Run it after changing a pool's terms, since
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

## Inputs and outputs

A run's outputs hold its `output_limit` in 256 entries at most. The broker's
record limit and each plugin profile's declared limit bound a task's inline input
files, as [`wherry submit`](commands.md#wherry-submit) describes; a plugin
declares each profile's limit through `InlineLimit`, and the receiver keeps
the plugin's dispatch check as a backstop.

The agent creates `WHERRY_OUTPUT_DIR` when it writes its first output.
Outputs are collected once the job ends and is stopped, whatever its exit
and whatever outcome the agent reports. A run that ends before its job does,
by timeout or otherwise, returns none.

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

2. Name it in the pool's [terms](configuration.md#pool-terms). The endpoint is
   `https://` and the host, with `:port` when it is not 443, and no path.
   wherry refuses a plain `http` endpoint except on the literal loopback
   hosts the [pool terms](configuration.md#pool-terms) name.

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

   The host keeps the access key and secret key as two credentials, each with
   its own source, and names them under `storage` in its
   [configuration](configuration.md#client-configuration):

   ```yaml
   credentials:
     s3-access-key:
       file: ~/.config/wherry/credentials/s3-access-key
     s3-secret-key:
       command: [op, read, "op://wherry/s3/secret-key"]
   storage:
     access_key: s3-access-key
     secret_key: s3-secret-key
   ```

   Receivers and agents never hold the S3 credential; they get presigned URLs, a GET per input
   for 15 minutes and a PUT per output until the run's deadline, 7 days at
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
   which upload, download and delete (they trust the system roots, or only
   the CA file in `SSL_CERT_FILE` when it is set); receiver hosts, which upload outputs with
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
  --git https://github.com/acme/app.git@main --git-changes patch
```

The host's `git` configuration gives the clone its credential and, for
`ssh`, its host key:

```yaml
git:
  - url: https://github.com/acme/
    credential: github-token
  - url: ssh://git@gitlab.example/
    credential: deploy-key
    host_key: ~/.config/wherry/known_hosts/gitlab
```

`submit` matches the remote, with a `user@host:path` remote written as
`ssh://user@host/path`, against the entries' URLs and takes the entry whose
URL is its longest prefix. The entry's credential joins the task as a task
credential in the file `git-credential` of the credentials directory, and
its `host_key` file becomes the task's host key. A remote under no entry
clones anonymously, and an `ssh` remote under none is refused for want of
a host key. The URL prefix is the credential's scope: every task cloning
under it receives that credential, so end each URL with `/` and keep it as
narrow as the token. Loading the configuration checks that each entry's
credential has a source and its `host_key` is a readable file. A task
credential of the entry's credential name is refused.

The agent runs `git` itself, from its image or host, with the commands the
prompt gives it; the receiver and the plugin run no git. The
[extension](a2a-extension-v1.md#git-input) defines the remote, ref and path
rules and the commands. For the agent to follow them:

- The image or host has `git` 2.30 or later, a shell, and `ssh` for `ssh`
  remotes. The prompt asks the agent to stop and report an older `git`.
- The agent may run `git` commands. Every command the prompt gives starts with
  `git`, so for Claude Code
  [`Bash(git:*)`](https://docs.anthropic.com/en/docs/claude-code/iam) in its
  allowed tools covers them, along with whatever else the task runs. Since
  `git` can run any command, the rule lets the agent run any command.

Allow the hosts a pool's tasks may clone from with `vcs` in its
[terms](configuration.md#pool-terms):

```yaml
vcs:
  - github.com
  - git.example.com:2222
```

`submit` refuses a remote outside the VCS hosts before it
publishes, and receivers reject one at admission.

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

A plugin gives the agent an environment in which `git` reads no system or
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
the Authorization header redacted. A plugin removes them only from the
environment it configures, and cannot remove what the platform or the image
sets. Each plugin's agent environment section says how to keep them out:
[Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-environment),
[Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#agent-environment)
and [systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd#agent-environment).

The CA and proxy settings stay yours: `GIT_SSL_CAINFO`, `SSL_CERT_FILE`,
`HTTPS_PROXY` and the like reach `git` as the image or platform sets them.

Start the agent's harness in a directory above the checkout. Claude Code in print mode skips the workspace trust prompt and runs the
hooks and MCP servers of the project it starts in, so a harness started in a
cloned repository runs that repository's hooks with the agent's credential in
reach. Started above the checkout, it reads the repository's files without
loading its project settings, including after the agent changes directory in
a shell. The start directory is the plugin profile's to set: on Nomad, a runtime
command such as `cd /local && exec claude ...` starts the harness in the task
directory, and the clone lands below it.

The isolation protects a cooperative agent from the configuration around it.
An agent that runs commands, including
through an allow rule such as
[`Bash(git:*)`](https://docs.anthropic.com/en/docs/claude-code/iam)
(`git -c alias.x='!cmd'` runs any command), can undo every one of these
settings. A hostile or prompt-injected agent holds whatever credentials the
parent sent, and what it can do with them is bounded by their scope and
lifetime, the platform's egress policy, and the receiver's redaction of what it
returns.

The host's configuration supplies least privilege; wherry passes the
credential as a file and checks the host:

- For `https`, a short-lived token for one repository, such as a GitHub App
  installation token. The credential helper answers with the user name
  `x-access-token`, which installation tokens require.
- For `ssh`, a deploy key for the repository, created for the run and removed
  afterwards where the forge allows it, and the forge's host key from a source
  you trust, named by the git entry's `host_key`. Every entry must name the remote's
  host, or `[host]:port` for a port other than 22; hashed entries, and entries
  with a wildcard pattern even beside the host, are refused. `ssh` checks the
  host key strictly and ignores every other `known_hosts`.
- A public repository needs no credential.

With `--git-changes`, the agent returns its commits as `changes.patch` or
`changes.bundle`, which `wait` writes like any output, and the parent
applies them with its own credentials:

```
apply with: git am /home/me/.config/wherry/state/runs/run-.../outputs/changes.patch
```

The agent pushes only when the prompt asks it to and the credential allows
it, and then only to the branch `wherry/<run>`; verify the commit it
reports at that branch before trusting it, and delete the branch after.
Forge rules that confine the credential's pushes to `wherry/*` make that
the only branch it can write.

`ssh` refuses a private key that others can read when the agent's user
owns the file, so the owner and mode a plugin gives credential files decide
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
out of the agent's reach, as the Nomad plugin's README below says.

Each plugin's README says how its agent jobs reach the network and how to
confine them:
[Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-network-reach),
through the task driver's network mode, a node pool for agents and host
firewall rules;
[Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#agent-network-reach),
through a default-deny egress NetworkPolicy; and
[systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd#agent-network-reach),
through `IPAddressDeny=` and `IPAddressAllow=` on `wherry.slice` or nftables
rules.

wherry enforces none of this policy. Restricting agent egress is the
operator's choice, made on the platform. What each party of a deployment
connects to:

| Party | Connects to |
| --- | --- |
| Hosts: `wherry` and `wherry mcp` | the Kafka brokers' TLS listener; the pool's storage endpoint when the host's configuration has `storage` |
| Receiver hosts: the receiver and its plugins | the Kafka brokers' TLS listener; the platform's API (the Nomad or Kubernetes API, or the local systemd bus); the storage endpoint, to upload S3 outputs and, for the Kubernetes and systemd plugins, fetch S3 inputs |
| Nomad clients | the storage endpoint, to fetch S3 inputs |
| Agent jobs | DNS; the model API the agent's harness calls; the hosts in the pool's `vcs`, for git inputs; and whatever else the task's own work needs |
| Prometheus | each receiver's `metrics_addr` |

Kafka is the hosts' and receivers' alone; agent jobs need no route to the
brokers or to the storage endpoint.

## Upgrade wherry

Readers reject `wherry.admin` records with unknown members and every record
with an unknown version, so hosts and receivers run the same release. A
host's run records carry `redaction_key`, so an older `wherry` cannot read a
run this release submitted. A host or
receiver of an earlier release cannot decode pool terms that set `models` or
`default_model`, and stops reading `wherry.admin` while they hold them, so
upgrade every receiver and host before the terms set them. An older release
reports such records as written by a later release; do not skip them, upgrade
it. A harness does not restart a running `wherry mcp` when its binary
changes, so restart the MCP server after upgrading.

The [release notes](https://github.com/benemon/wherry/releases) mark a
release that changes the record formats or the plugin protocol, and give
its upgrade steps.

Between releases that keep the record formats, upgrade in place.
Prerequisites: the new `wherry` binary, and each receiver's local
configuration, bundle and state directory.

1. Replace the binary on every host and receiver host with the new
   release's, verified as in [Install wherry](quickstart.md#install-wherry).
2. Replace the plugins in each receiver's plugin directory together with the
   receiver binary, verified as in [Install plugins](#install-plugins); a receiver refuses a plugin of another
   [plugin protocol version](commands.md#plugins).
3. Restart each receiver with the same local configuration, bundle and
   state directory. On
   `SIGINT` or `SIGTERM` it stops claiming, closes its idle claims and lets
   the runs it holds go on for up to `drain_timeout`. It then exits without
   stopping the jobs still running and releases their tasks to the pool.
   The next delivery of each task adopts the running
   job, on the restarted receiver or another member, and resumes the
   transcript from the run's latest committed offset. Allow the receiver 30 seconds
   to stop before a forced kill, with `drain_timeout` below that window.
4. Restart the harness sessions that run `wherry mcp`.

## Harness integration

`wherry mcp` serves the host operations as MCP tools over stdio. It reads
the client configuration and connects to Kafka as one host when it starts.
Every tool call and seal answer reuses that host and its one tenant-feed
follower, and `wait`, `events` and `logs` also read the tenant's
transcript topic. While the connection fails, the server retries it on each
tool call and every five seconds.
[MCP tools](mcp.md) lists the tools.

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
