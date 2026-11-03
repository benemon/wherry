# Receiver operations

## Run a receiver

`wherry receiver run --config-file FILE` needs:

- its [local configuration](configuration-receiver.md#receiver-local-configuration),
  at `/etc/wherry/config.yaml` unless `--config-file` names another file;
- the bundle, readable by the receiver alone;
- a state directory that survives restarts. It holds the receiver's
  signing key, and while the receiver is or was its pool's key custodian,
  the pool keys. A receiver that loses its signing key cannot start under the
  same name again, and lost pool keys fail the runs that only they open
  ([Lost pool keys](concepts.md#lost-pool-keys));
- a plugin directory holding every plugin in its
  configuration, as in [Install plugins](#install-plugins);
- a source for each [platform credential](configuration-receiver.md#platform-credentials) its
  plugin profiles name;
- a supervisor that restarts the process when it exits, since a
  configuration change, a Kafka authorisation error and a plugin's exit end
  it, such as Nomad's `restart` block, a Kubernetes `restartPolicy` or
  systemd's `Restart=on-failure`; a receiver with revoked or wrong
  credentials restarts until you fix or remove it. The supervisor allows it
  30 seconds to stop after `SIGTERM` before a forced kill, with
  `drain_timeout` in the local configuration plus 5 seconds set below that
  window, such as `20s`; a stop takes at most `drain_timeout` plus 5 seconds
  ([Drain timeout and the stop window](configuration-receiver.md#drain-timeout-and-the-stop-window));
- for the Nomad plugin, a Nomad token with the
  [receiver policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy), from a platform credential or `NOMAD_TOKEN`
  in the receiver's environment.

On first start the receiver logs `registered key: signing=<key id>`, then
`configured: pool=<pool> profiles=<n> capacity=<n>`. It logs
`waiting for configuration` while its record is missing. After its first
advertisement, `wherry host receivers` on a host of an entitled tenant shows it
`live`.

A plugin that exits, or fails a ping and is killed, stops the receiver:
it logs `stopping: plugin <type> exited` and exits non-zero, leaving the
runs it holds for an adopter, and its supervisor restarts it.
[Plugins](commands.md#plugins) covers what runs in flight do.

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
   [upgrade](upgrading.md#upgrade-wherry), and wait for each to be `live` again in
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
