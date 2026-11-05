# Receiver operations

## Run a receiver

Before you start, prepare a bundle and persistent state directory.
Install the plugins named in the receiver record.
Configure a source for every platform credential its profiles need.
For Nomad, follow the plugin's [receiver policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy).

1. Write the [local configuration](configuration-receiver.md#receiver-local-configuration).
   Keep the bundle readable by the receiver alone.
2. Keep the state directory across restarts and reschedules.
   It holds the receiver's signing key and any retained pool keys.
3. Configure your platform's supervisor to restart the receiver after it exits.
   A configuration change, Kafka authorisation error or plugin exit ends the process.
   Fix or remove revoked credentials to stop repeated failed starts.
4. Set the supervisor's stop window above `drain_timeout` plus five seconds.
   Use `drain_timeout: 20s` with a 30-second stop window.
5. Run `wherry receiver run --config-file FILE` through the supervisor.
   Without the flag, it reads `/etc/wherry/config.yaml`.
6. Check the logs for `registered key: signing=<key id>` and `configured: pool=<pool> profiles=<n> capacity=<n>`.
   `waiting for configuration` means the configuration record is missing.
7. Run `wherry host receivers` from an entitled tenant's host.
   Check that the receiver is `live` after its first advertisement.

Losing the signing key prevents reuse of the receiver's identity.
Losing pool keys can fail tasks that depend on them.
See [Lost pool keys](concepts.md#lost-pool-keys).
A plugin exit leaves held runs available for adoption.
[Plugin reference](commands.md#plugins) explains process and stream recovery.

### Keep receiver identities stable

Each receiver identity adds two topics: its key topic and its progress topic.
Each host adds one key topic.
wherry never deletes these topics.
Run each receiver with a stable identity.
Keep each receiver's bundle and its state directory.
The bundle names the receiver.
The state directory preserves the receiver's signing key.
On Nomad, the host volume keeps the state directory across restarts.
On Kubernetes, the PersistentVolumeClaim keeps it.
Run one instance per receiver.
Give each receiver its own bundle and state directory.
To scale out again, restart the same stopped receivers.
Do not apply new receiver names to scale out.
Removing a receiver leaves its topics in Kafka.

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
binary in the archive has that SHA-256. In the working directory, with the
verified `checksums.txt`, print it:

```sh
awk -v name="wherry-plugin-${plugin}_${version}_${os}_${arch}" '$2 == name {print $1}' checksums.txt
```

Set it as the type's entry in the receiver configuration's `plugin_sha256`,
which holds one for each plugin type the configuration names, and apply it,
here the configuration directory of [Add a receiver](operations-operator.md#add-a-receiver):

```sh
wherry operator receiver apply nomad-1 -f nomad-1
```

The receiver drains and restarts as in
[Change a receiver's configuration](operations-operator.md#change-a-receiver-s-configuration), and
starts each plugin only from a binary whose SHA-256 is the pin.
[go-plugin's `SecureConfig`](https://pkg.go.dev/github.com/hashicorp/go-plugin#SecureConfig)
checks it when the receiver starts the plugin. A binary whose SHA-256 differs
fails with
`profile <name>: plugin type <type>: <path> has SHA-256 <hex> and the pin is <hex>`,
and a type with no binary with `profile <name>: no plugin for type <type>`.

An upgrade or rollback takes the same steps with the other version, in this
order:

1. Verify the new version's `checksums.txt` and record its pin with
   `wherry operator receiver apply`.
2. Roll the receiver's binary, or its image, to the new version at once.

The receiver reads the new record, drains and exits, and from its next start
runs only a binary with the new pin. A restart between the two steps
refuses the old binary with the SHA-256 error above, and the receiver
restarts until the new binary is in place.

To check the install, start the receiver and find one line per plugin,
logged when the receiver starts it:

```text
receiver: <date> <time> plugin <type>: version <version> protocol <n>
```

A plugin that leaves `pluginsdk.Version` unset logs `version unreported`.

### Platforms

Each plugin's README runs the receiver on its platform, with its steps:

- [Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#run-the-receiver-in-nomad),
  as a service job whose
  [workload identity](https://developer.hashicorp.com/nomad/docs/concepts/workload-identity)
  gives the Nomad plugin its token;
- [Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#run-the-receiver-in-the-cluster),
  OpenShift included, as a Deployment;
- [systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd#run-the-receiver-as-a-systemd-service),
  as a service unit.

[Plugins](plugins.md) embeds each example.

### What a receiver subscribes to

A receiver subscribes, through its pool's share group, to the signature task
topics of its ready profiles: one topic per profile signature, a profile's
agent and all its labels. A task on another signature never reaches it.
After an advertisement, every 30 seconds, that changes the set, it closes its
idle polls and reopens them with the new set; the close waits out an
in-flight fetch, and the join takes the broker's group round trip. A slot
holding a run keeps its share session. A label or agent change comes through
`receiver apply`, which restarts the receiver on the new record and moves the
profile's signature to a new topic.

A profile that goes unready between a claim's subscription and its fetch has
its task released to the pool, logged as
`run <run>: no ready profile for topic <topic>; releasing`, with no metric
and no start-failure record. The member's other slots still polling may
acquire the task once each before they reopen without the topic, and each
acquisition spends a delivery.

### A receiver with no ready offer

A receiver offers a profile only while the profile's plugin reports it ready.
A receiver with no ready offer has an empty subscription and fetches no tasks from its pool.
It closes its idle polls when it observes the empty subscription.
After an advertisement makes a profile ready, it reopens with the new set, unless its pool is suspended or it holds a [pending run](#a-receiver-with-a-pending-run); closing waits for the in-flight fetch, and joining takes the broker's round trip.
Its held runs keep running, renewing and reading cancels.

Each advertisement logs a line for every profile whose plugin reports it not ready:

```text
receiver: <date> <time> profile <name> readiness: not ready
```

When every member of a pool has no ready offer, you see these consequences:

- Queued tasks wait in the queue and spend no more of their delivery budget.
- A task whose `task_started_by` passes while it waits is rejected when a ready member fetches it.
- Adopting or stopping an orphaned job waits until a member is ready again.
- Ending a run that is past its bounds waits until a member is ready again.
- A queued task that its host cancelled gets its terminal status only once a member is ready again.
- None of these waits has a fixed bound.

A receiver with ready offers fetches only the tasks of their signatures.
A task on a signature no live member serves waits on its topic and spends no delivery; its host reports it at `task_started_by` with `no receiver claimed the task`.

### A receiver with a pending run

A run is pending while the platform holds its job and has not placed it.
Pending means not placed only: queued in Nomad, or held by a blocked evaluation with no allocation, and unschedulable or scheduling-gated in Kubernetes.
A systemd job is never pending.
The receiver writes the run's `placing` status, `waiting for placement (<reason>)` with the platform's reason, and writes it again each time the reason changes.

A receiver that observes any held run pending stops claiming:

- It opens no new poll of its pool while any held run is pending.
- It ends an open idle poll and admits a task that poll returns.
- It claims again only when no held run is pending.
- Its held runs keep running, renewing and reading cancels.

A receiver holds at most its capacity of runs, so at most that many of its runs are pending at once.
A pending run past its `task_started_by` is stopped and fails with `not started by task_started_by (<RFC 3339 time>)`.

While a member holds a pending run, a redelivery, an `ending` cleanup or a queued cancel of another task waits for that member to claim again or for another member.
None of these waits has a fixed bound.

A shutdown while a run is pending releases the claim and leaves the job for the adopter.
A plugin exit or Kafka authorisation error leaves the job and writes nothing.
A queued job left that way with no later adopter waits for the plugin's sweep.

Suspending a pool does not stop jobs already queued on the platform.
The key custodian still serves an adopting open on a suspended pool, pending jobs included.

### Metrics

Set `metrics_addr` in the receiver's [local
configuration](configuration-receiver.md#receiver-local-configuration) to serve its metrics at
`/metrics` in the Prometheus text format. The endpoint is plain HTTP with no
authentication, so bind it to an address only the scraper reaches, such as
the receiver host's address on a monitoring network, or put an
authenticating proxy in front of it:

```yaml
metrics_addr: 192.0.2.21:9464
```

Scrape it as any Prometheus target:

```yaml
scrape_configs:
  - job_name: wherry-receiver
    static_configs:
      - targets: ["192.0.2.21:9464"]
```

The `tenant` label names each entitled tenant whose runs reach the pool.
Whoever can scrape a receiver sees every tenant's activity on its pool: its
runs and their outcomes, durations, rejections and open waits. Give scrape access only to those who may
see all of the pool's tenants. No label or value carries a prompt, output,
run ID, error text, URL, image, path, node or credential name.
[Metrics reference](metrics.md) lists the families.

[`contrib/observability`](https://github.com/benemon/wherry/tree/main/contrib/observability)
holds a Grafana dashboard, `wherry-dashboard.json`, and Prometheus alert
rules, `wherry-alerts.yaml`, built from these families. Each release archive
carries them under `contrib/observability/`. The dashboard shows receivers
and capacity in use, admissions, rejections and start failures, run
durations and queue wait, open waits, platform errors and
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

## Stop a pool's key custodian

A key custodian that stops without re-keying its tasks leaves the tasks under its
keys to fail before they start, until their hosts resubmit them
([Lost pool keys](concepts.md#lost-pool-keys)). A planned stop hands its
custody to another receiver of the pool and re-keys those tasks. The
successor publishes a `takeover` snapshot, as for any custodian change, so
each planned stop of a custodian raises
[WherryCustodianTakeover](runbooks.md#wherrycustodiantakeover): expect it, and
check that the new custodian is the receiver the stop handed on to.

Prerequisites: `drain_timeout` over `0s` in each receiver's local
configuration and the platform's stop window set with it, as in
[Drain timeout and the stop window](configuration-receiver.md#drain-timeout-and-the-stop-window),
and another live receiver in the pool.

1. Find the key custodian: the receiver whose latest log line starting
   `custodian: published pool <pool> key` or
   `custodian: continuing as custodian of pool <pool>` is the most recent in
   the pool, and that has not logged
   `custodian: lost the custodian group's assignment for pool <pool>` since.

2. Stop the pool's receivers one at a time, as for an
   [upgrade](upgrade.md#upgrade-wherry), and wait for each to be `live` again in
   `wherry host receivers` before stopping the next.

3. When the custodian stops, check its log for
   `custodian: stepping down in pool <pool>; waiting up to <timeout> for a successor`
   and then `custodian: re-keyed <n> tasks to pool key <id> and deleted <n> pool keys`,
   and the successor's log for
   `custodian: published pool <pool> key <id> (takeover)`.

4. When the stopped custodian logged
   `custodian: no successor in pool <pool>; keeping the pool keys to their deadlines`
   or `custodian: hand over to <receiver>: <error>; keeping the pool keys to their deadlines`,
   start it again with the same state directory and keep it running. While
   it holds its keys it answers each open request for a task under them with
   a re-key wrap to the current key, and it deletes each key at its deadline,
   logging `custodian: deleted pool key <id> at its deadline`.

To remove a pool's only receiver for good, add its replacement to the pool
first, then stop the old receiver as above. A pool's only receiver waits out
`drain_timeout` for a successor that does not come and exits with its keys,
so removing it without a successor loses them. Stopping a whole pool at once,
or scaling it to zero, has its custodian wait out `drain_timeout` the same way,
within the stop window.

## Rotate a receiver key

For a receiver, restart it once with `--rotate-key`:

```sh
wherry receiver run --rotate-key
```

It rotates `signing.key` before it claims anything and logs
`rotated key: old=<id> new=<id>`. Start it without the flag afterwards.

Keep `signing.key.next` if a rotation is interrupted.
The next start reconciles it against the key topic.
Removing it after publication loses the new key.
For a suspected compromise, [revoke the receiver](operations-operator.md#revoke-a-host-or-receiver).
