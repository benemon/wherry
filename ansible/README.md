# benemon.wherry

An Ansible collection that sets up a wherry operator and host and deploys receivers on Nomad, Kubernetes and systemd, with one role per part; each role's `state: absent` takes its part down again.

The receiver and systemd roles take a plugin version and fetch that plugin's files from `examples/` in `benemon/wherry-plugins` at the commit the role pins for that version; the collection keeps no copy of them. Every Nomad call goes through `benemon.nomad`.

## Requirements

- ansible-core 2.19 or later on the control node.
- `benemon.nomad` 0.4.0 and its Python library, nomadpy 0.3.0 or later, for the Nomad roles.
- `kubernetes.core` and the `kubernetes` Python library for the Kubernetes roles, and `kubectl` on the control node, which renders the receiver's Kustomize overlay.
- `cosign` on the control node for `systemd_receiver`, and a Linux amd64 host with systemd for the systemd roles.
- For `operator` with `wherry_operator_clear_kafka`: Kafka's administration scripts and Java 17 or later on the host the role runs on.
- The `wherry` CLI on the operator's host, at the version the roles are pinned to.

Kafka, its certificates, and the Nomad, Kubernetes or OpenShift credentials are prerequisites; the roles do not create them.

## Installation

Install the collection at the wherry tag whose CLI you run:

```sh
ansible-galaxy collection install git+https://github.com/benemon/wherry.git#/ansible,<tag>
```

## Usage

`examples/inventory.yml` describes one Linux machine that is the operator, the wherry host, the control node and all three receiver platforms. `examples/site.yml` builds it, with one receiver per platform in pool `build` and tenant `acme`, and `examples/retire.yml` decommissions it. The inventory's comment lists the environment variables that supply the secrets and paths.

```sh
cd examples
ansible-playbook -i inventory.yml site.yml
ansible-playbook -i inventory.yml retire.yml -e wherry_confirm_absent=true
```

`retire.yml` moves the operator directory into `wherry_backup_dir` and leaves wherry's objects in Kafka. To clear them too, add `-e wherry_operator_clear_kafka=true` with `wherry_kafka_bin_dir` and `wherry_kafka_admin_config`.

`ansible-doc -t role benemon.wherry.<role>` shows a role's arguments.

The operator role writes each receiver's record to `<operator dir>/receivers/<name>` and applies it; the receiver roles then deploy that record's `config.yaml` and the receiver's bundle, so `site.yml` reads both from the operator directory. Rerun `site.yml` to apply a change: an unchanged deployment reports no changes, and `--check` runs `wherry operator apply --dry-run`.

The Nomad and Kubernetes receiver roles fetch and render their files on the control node.

## Roles

Each role takes `state`, `present` by default. `state: absent` undoes what the role sets up, and refuses to run unless `wherry_confirm_absent` is true.

| Role | `present` | `absent` |
|---|---|---|
| `operator` | Checks that `wherry version` reports `wherry_version`, runs `operator init` once, creates the tenant, applies each pool and allows the tenant on it, then writes and applies each receiver record. | Moves the operator directory into a dated backup. With `wherry_operator_clear_kafka` it first deletes, as the Kafka super user, every `wherry.` topic, the `p-` pool groups the hosts and receivers hold ACLs on, and every ACL, SCRAM credential and quota of a `wherry.rcv.` or `wherry.tnt.` principal; it keeps the super user, `wherry.operator` and every other principal's objects. |
| `host` | Enrols the host when the operator holds no bundle for it: `operator add --host`, `host init`, then `operator approve`. Its `live` entry point runs `operator check` and waits until `host receivers --json` lists each named receiver as `live`. | Archives the host's state directory, bundle, host key, configuration and `scram.next`, then removes the state directory, bundle and key and recreates an empty state directory. The paths are those the configuration names, or wherry's defaults for it: `$XDG_STATE_HOME/wherry` for the default configuration, `<config>.state` beside any other. |
| `nomad` | Creates each Nomad namespace with its capabilities and, on Nomad Enterprise, its node pool configuration. | Deletes the ACL policies whose names start with `wherry_nomad_policy_prefix`, then the namespaces. |
| `nomad_receiver` | Creates the receiver's workload ACL policy, its dynamic host volume, its Nomad variable and its job, from the pinned `wherry-receiver.nomad.hcl`. It fails before any change if the pinned `wherry-receiver.volume.hcl` differs from the role's volume options. | Purges the receiver and agent jobs and their variables, and the receiver volumes, by prefix, in each namespace. |
| `kubernetes` | Creates the Namespace, or an OpenShift Project. | Deletes it and waits until it is gone. |
| `kubernetes_receiver` | Renders the pinned Kustomize base with the bundle, the record's `config.yaml` and an overlay of the namespace, image and your resources and patches, applies the Secrets and then the rest, and waits for the rollout. | Deletes the base's objects, the overlay's resources and the generated Secrets and ConfigMaps, state claim included. |
| `systemd` | Installs the pinned unit, slice and tmpfiles files, creates the state roots and enables the service. | Stops and disables the service and removes the platform's files, `/etc/wherry`, the state roots and any installed releases. |
| `systemd_receiver` | On the control node, verifies each release's `checksums.txt` with cosign against that release's own tag, checks each archive against it and unpacks only its binary into its own directory, and checks the plugin binary against its pin; on the host, installs both under a versioned directory behind a `current` link, writes the configuration and bundle, and starts the service. | Stops the service and removes the releases, the `wherry` link, the configuration and the bundle. |

The prefixes `state: absent` deletes by must be non-empty, and the Nomad policy and receiver prefixes must start with `wherry`.

## Role variables

| Variable | Roles | Default | Meaning |
|---|---|---|---|
| `state` | all | `present` | `present` or `absent`. |
| `wherry_confirm_absent` | all | `false` | Must be true for `state: absent`. |
| `wherry_version` | `operator`, `systemd_receiver` | | wherry release tag: the CLI the operator role expects, and the release `systemd_receiver` installs. |
| `wherry_plugin_version` | `nomad_receiver`, `kubernetes_receiver`, `systemd`, `systemd_receiver` | | Plugin version: `0.3.1` for Nomad, `0.3.0` for Kubernetes and systemd. |
| `wherry_operator_dir` | `operator`, `host` | | Operator directory. |
| `wherry_operator_broker` | `operator` | | Kafka bootstrap address for `operator init`. |
| `wherry_operator_ca_cert` | `operator` | | Kafka CA certificate file. |
| `wherry_operator_super_user`, `wherry_operator_super_user_password` | `operator` | | Kafka super user and its password. |
| `wherry_tenant` | `operator` | | Tenant to create and allow on each pool. |
| `wherry_pools` | `operator` | | List of `{name, file}`: each pool and its terms file. |
| `wherry_receivers` | `operator` | | List of `{name, pool, broker, plugin_sha256, config, plugins}`: `config` is the receiver's local configuration as a mapping, and `plugins` maps each plugin type to its `labels`, `config` and list of `profiles`. |
| `wherry_backup_dir` | `operator`, `host` | | Directory for dated backups, for `state: absent`. |
| `wherry_operator_clear_kafka` | `operator` | `false` | With `state: absent`, also clear wherry's objects from Kafka. |
| `wherry_kafka_bin_dir`, `wherry_kafka_admin_config` | `operator` | | Kafka's `bin` directory and a client configuration file for the super user, for `wherry_operator_clear_kafka`. |
| `wherry_host`, `wherry_host_tenant` | `host` | | Host name and its tenant. |
| `wherry_host_ca_cert` | `host` | | Kafka CA certificate file for `host init`. |
| `wherry_host_config_dir` | `host` | the host's `$XDG_CONFIG_HOME/wherry` | Host configuration directory. |
| `wherry_live_receivers` | `host` (`live`) | | Receivers that must be `live`. |
| `wherry_nomad_address`, `wherry_nomad_token` | Nomad roles | | Nomad API address and ACL token. |
| `wherry_nomad_ca_cert` | Nomad roles | | CA certificate file for an HTTPS Nomad API. |
| `wherry_nomad_namespaces` | `nomad`, `nomad_receiver` | | List of `{name, description, capabilities, node_pool_configuration}`; `nomad_receiver` with `state: absent` reads the names. |
| `wherry_nomad_policy_prefix` | `nomad` | | Prefix of the receiver ACL policies `state: absent` deletes. |
| `wherry_nomad_receiver_name` | `nomad_receiver` | | Job ID, and the name of its ACL policy, volume and variable `nomad/jobs/<name>`. |
| `wherry_nomad_receiver_namespace`, `wherry_nomad_receiver_node_pool` | `nomad_receiver` | | Namespace and node pool of the job and volume. |
| `wherry_nomad_receiver_image`, `wherry_nomad_receiver_driver` | `nomad_receiver` | | Receiver image and the task driver that runs it. |
| `wherry_nomad_receiver_metrics_port` | `nomad_receiver` | | Static host port for metrics; `0` for a dynamic port. |
| `wherry_nomad_receiver_policy_rules` | `nomad_receiver` | | ACL policy rules for the job's workload identity, as the Nomad plugin's README gives them. |
| `wherry_nomad_receiver_bundle`, `wherry_nomad_receiver_config` | `nomad_receiver` | | The receiver's bundle and its record's `config.yaml` as a mapping. |
| `wherry_nomad_receiver_prefix`, `wherry_nomad_agent_prefix` | `nomad_receiver` | | Prefixes of the receiver and agent jobs and variables `state: absent` purges; receiver volumes match the first. |
| `wherry_kubernetes_kubeconfig`, `wherry_kubernetes_namespace` | Kubernetes roles | | kubeconfig file and the receiver's namespace. |
| `wherry_kubernetes_openshift` | `kubernetes` | `false` | Manage an OpenShift Project instead of a Namespace. |
| `wherry_kubernetes_receiver_name` | `kubernetes_receiver` | | Receiver name. |
| `wherry_kubernetes_receiver_image` | `kubernetes_receiver` | | Receiver image, with a tag or a digest. |
| `wherry_kubernetes_receiver_bundle`, `wherry_kubernetes_receiver_config` | `kubernetes_receiver` | | The receiver's bundle and its record's `config.yaml` as a mapping. |
| `wherry_kubernetes_overlay_resources` | `kubernetes_receiver` | `[]` | Resource files the overlay adds, and `state: absent` deletes. |
| `wherry_kubernetes_overlay_patches` | `kubernetes_receiver` | `[]` | Patch files the overlay applies; `base/fsgroup.yaml` names the base's own fsGroup patch. |
| `wherry_systemd_receiver_plugin_sha256` | `systemd_receiver` | | SHA-256 of the systemd plugin binary, as in the receiver record. |
| `wherry_systemd_receiver_config`, `wherry_systemd_receiver_bundle` | `systemd_receiver` | | The record's `config.yaml` as a mapping, and the receiver's bundle. |
| `wherry_systemd_receiver_cache_dir` | `systemd_receiver` | `~/.cache/wherry` | Where the control node keeps downloaded releases. |

Arguments without a default are needed only by the state that uses them; each role checks them before its first change. Secret arguments are `no_log` in the argument specifications. `node_pool_configuration` is a Nomad Enterprise feature; leave it out on Nomad Community Edition.
