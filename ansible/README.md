# benemon.wherry

An Ansible collection that sets up a wherry operator and host and deploys receivers on Nomad, Kubernetes and systemd, with one role per part. Each role's `wherry_<role>_state: absent` takes its part down again.

The receiver and systemd roles take a plugin version and fetch that plugin's files from `examples/` in `benemon/wherry-plugins` at the commit the role pins for that version; the collection keeps no copy of them. Every Nomad call goes through `benemon.nomad`.

## Requirements

- ansible-core 2.19 or later on the control node.
- `benemon.nomad` 0.4.0 and its Python library, nomadpy 0.3.0 or later, for the Nomad roles.
- `kubernetes.core` and the `kubernetes` Python library for the Kubernetes roles, and `kubectl` on the control node, which renders the receiver's Kustomize overlay.
- `cosign` on the control node for `systemd_receiver`, and a Linux amd64 or arm64 host with systemd for the systemd roles.
- For `operator` with `wherry_operator_clear_kafka`: Kafka's administration scripts and Java 17 or later on the host the role runs on.
- The `wherry` CLI on the operator's host, at the version the roles are pinned to.

Kafka, its certificates, and the Nomad, Kubernetes or OpenShift credentials are prerequisites; the roles do not create them.

## Installation

Install the collection at the wherry tag whose CLI you run:

```sh
ansible-galaxy collection install git+https://github.com/benemon/wherry.git#/ansible,<tag>
```

## Usage

The examples are in the wherry repository under [`ansible/examples/`](https://github.com/benemon/wherry/tree/main/ansible/examples); the built collection leaves them out. `inventory.yml` describes one Linux machine that is the operator, the wherry host, the control node and all three receiver platforms. `site.yml` builds it, with one receiver per platform in pool `build` and tenant `acme`, and `retire.yml` decommissions it. The inventory's comment lists the environment variables that supply the secrets and paths.

```sh
cd ansible/examples
ansible-playbook -i inventory.yml site.yml
ansible-playbook -i inventory.yml retire.yml -e @confirm-absent.yml
```

`confirm-absent.yml` sets each role's own `wherry_<role>_confirm_absent`. `retire.yml` moves the operator directory into `wherry_backup_dir` and leaves wherry's objects in Kafka. To clear them too, add `-e wherry_operator_clear_kafka=true` with `wherry_operator_kafka_bin_dir` and `wherry_operator_kafka_admin_config`.

`ansible-doc -t role benemon.wherry.<role>` shows a role's arguments.

The operator role writes each receiver's record to `<operator dir>/receivers/<name>` and each pool's file to `<operator dir>/pools/<name>.yaml`, and applies them; the receiver roles then deploy the record's `config.yaml` and the receiver's bundle, so `site.yml` reads both from the operator directory. `wherry operator apply` reports nothing about change, so a pool or receiver reports a change when its record there changes. Rerun `site.yml` to apply a change: an unchanged deployment reports no changes. `--check` validates the proposed pools and receiver records with `wherry operator apply --dry-run`, and works on a first run, when it skips what needs an operator, a namespace or a unit that does not exist yet.

The Nomad and Kubernetes receiver roles fetch and render their files on the control node.

## Variables

Each role's arguments are named `wherry_<role>_*`. Values several roles share are collection variables, named `wherry_*` without a role, which the roles' defaults read:

| Collection variable | Read by |
|---|---|
| `wherry_version` | `wherry_operator_version`, `wherry_systemd_receiver_version` |
| `wherry_nomad_plugin_version` | `wherry_nomad_receiver_plugin_version` |
| `wherry_kubernetes_plugin_version` | `wherry_kubernetes_receiver_plugin_version` |
| `wherry_systemd_plugin_version` | the `systemd` role, and `wherry_systemd_receiver_plugin_version` |
| `wherry_tenant` | `wherry_operator_tenant`, `wherry_host_tenant` |
| `wherry_broker` | `wherry_operator_broker` |
| `wherry_ca_cert` | `wherry_operator_ca_cert`, `wherry_host_ca_cert` |
| `wherry_operator_dir` | the `operator` role, and `wherry_host_operator_dir` |
| `wherry_backup_dir` | `wherry_operator_backup_dir`, `wherry_host_backup_dir` |
| `wherry_nomad_address`, `wherry_nomad_ca_cert`, `wherry_nomad_token` | the `nomad` role, and `wherry_nomad_receiver_address`, `_ca_cert` and `_token` |
| `wherry_nomad_namespaces` | the `nomad` role, and `wherry_nomad_receiver_purge_namespaces` |
| `wherry_kubernetes_kubeconfig`, `wherry_kubernetes_namespace` | the `kubernetes` role, and `wherry_kubernetes_receiver_kubeconfig` and `_namespace` |

## Roles

Each role takes `wherry_<role>_state`, `present` by default. `absent` undoes what the role sets up, and the role refuses it unless its own `wherry_<role>_confirm_absent` is true.

| Role | `present` | `absent` |
|---|---|---|
| `operator` | Checks that `wherry version` reports `wherry_operator_version`, runs `operator init` once, creates the tenant, applies each pool and allows the tenant on it, then writes and applies each receiver record. | Moves the operator directory into a dated backup. With `wherry_operator_clear_kafka` it first deletes, as the Kafka super user, every `wherry.` topic, wherry's `p-` pool groups, and every ACL, SCRAM credential and quota of a `wherry.rcv.` or `wherry.tnt.` principal; it keeps the super user, `wherry.operator` and every other principal's objects. |
| `host` | Enrols the host when the operator holds no bundle for it: `operator add --host`, `host init`, then `operator approve`. Its `live` entry point runs `operator check` and waits until `host receivers --json` lists each named receiver as `live` (not in check mode). | Archives the host's state directory, bundle, host key, configuration and `scram.next`, then removes the state directory, bundle and key and recreates an empty state directory. The paths are those the configuration names, or wherry's defaults for it: `$XDG_STATE_HOME/wherry` for the default configuration, `<config>.state` beside any other. |
| `nomad` | Creates each namespace in `wherry_nomad_namespaces` with its capabilities and, on Nomad Enterprise, its node pool configuration. | Deletes those namespaces. Nomad refuses to delete one that still holds jobs, variables or volumes. |
| `nomad_receiver` | Creates the receiver's workload ACL policy, its dynamic host volume, its Nomad variable and its job, from the pinned `wherry-receiver.nomad.hcl`. It fails before any change if the pinned `wherry-receiver.volume.hcl` differs from the role's volume options. | Deletes the named receiver's job, variable, volume and policy. With `wherry_nomad_receiver_purge_prefix` it also purges, in each of `wherry_nomad_receiver_purge_namespaces`, every job and variable whose name starts with `wherry_nomad_receiver_job_prefix` or `wherry_nomad_receiver_agent_prefix` (the agent runs), and every volume and policy whose name starts with the first. |
| `kubernetes` | Creates the Namespace, or an OpenShift Project. | Deletes it and waits until it is gone. |
| `kubernetes_receiver` | Renders the pinned Kustomize base with the bundle, the record's `config.yaml` and an overlay of the namespace, image and your resources and patches, applies the Secrets and then the rest, and waits for the rollout. | Deletes the base's objects, the overlay's resources and the generated Secrets and ConfigMaps, state claim included. |
| `systemd` | Installs the pinned unit, slice and tmpfiles files, creates the state roots and enables the service. | Stops and disables the service and removes the platform's files, `/etc/wherry`, the state roots and any installed releases. |
| `systemd_receiver` | On the control node, verifies each release's `checksums.txt` with cosign against that release's own tag, checks each archive against it and unpacks only its binary into its own directory, and checks the plugin binary against its pin; on the host, installs both under a versioned directory behind a `current` link, writes the configuration and bundle, and starts the service. | Stops the service and removes the releases, the `wherry` link, the configuration and the bundle. |

The purge's job prefix must start with `wherry` and its agent prefix must not be empty.

`ansible-doc` lists every argument; arguments without a default are needed only by the state that uses them, and each role checks them before its first change. Secret arguments are `no_log` in the argument specifications. `node_pool_configuration` is a Nomad Enterprise feature; leave it out on Nomad Community Edition.
