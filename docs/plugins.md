# Plugins

A plugin runs jobs on one kind of platform.
The receiver starts its `wherry-plugin-<type>` binary from the plugin directory.
One plugin process serves all of that plugin's profiles on the receiver.
Each profile names an agent, labels and runtime settings.
See [Receiver configuration](configuration-receiver.md#receiver-configuration).

## Which platform do you use?

The [wherry-plugins repository](https://github.com/benemon/wherry-plugins) maintains these plugins.
Install a receiver with its platform's receiver page.
Use each plugin's guide for settings and platform permissions.

| Plugin | Install | Guide |
| --- | --- | --- |
| Nomad | [Receiver on Nomad](install-receiver-nomad.md). | [Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad). |
| Kubernetes and OpenShift | [Receiver on Kubernetes and OpenShift](install-receiver-kubernetes.md). | [Kubernetes plugin](https://github.com/benemon/wherry-plugins/tree/main/kubernetes). |
| systemd | [Receiver on systemd](install-receiver-systemd.md). | [systemd plugin](https://github.com/benemon/wherry-plugins/tree/main/systemd). |

## Why Nomad

The Nomad plugin passes each profile's driver settings to Nomad.
You can define several profiles for the same plugin.
Give each profile labels that describe its capabilities.
Your tasks select profiles by agent and label selector.
Use the [Nomad plugin settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings) to configure each driver.

## Running a receiver

Follow [Run a receiver](operations-receivers.md#run-a-receiver) for wherry's requirements.
The examples below are imported from the plugin repository.
[Plugin protocol](plugin-protocol.md) describes the authoring interface.
[Command reference](commands.md#plugins) explains pins and process recovery.

### systemd

[Receiver on systemd](install-receiver-systemd.md#run-the-receiver-as-a-systemd-service)
describes these files and installs them.

A systemd receiver adopts jobs only on its own host.
Give each systemd receiver a signature of its own: list `host` in the pool's `routable_labels` and give each of its profiles a `host: <name>` label.
With receivers on several hosts, redelivery can start another job while the first still runs.
This can happen after the previous holder's lease expires or its membership ends.
The lease expires 120 seconds after its last advertisement.

`config.yaml`:

<<< @/examples/systemd/config.yaml{yaml}

`wherry-receiver.service`:

<<< @/examples/systemd/wherry-receiver.service{systemd}

`wherry.slice`:

<<< @/examples/systemd/wherry.slice{systemd}

`wherry-tmpfiles.conf`:

<<< @/examples/systemd/wherry-tmpfiles.conf{txt}

### Kubernetes

[Receiver on Kubernetes and OpenShift](install-receiver-kubernetes.md#run-the-receiver-in-the-cluster)
describes these files, on Kubernetes and OpenShift alike, and deploys them.

`config.yaml`:

<<< @/examples/kubernetes/config.yaml{yaml}

`Containerfile`:

<<< @/examples/kubernetes/Containerfile{dockerfile}

`receiver.yaml`:

<<< @/examples/kubernetes/receiver.yaml{yaml}

`kustomization.yaml`:

<<< @/examples/kubernetes/kustomization.yaml{yaml}

`fsgroup.yaml`, for a state volume whose root the receiver cannot write:

<<< @/examples/kubernetes/fsgroup.yaml{yaml}

### Nomad

[Receiver on Nomad](install-receiver-nomad.md#run-the-receiver-in-nomad)
describes these files and runs them.

`config.yaml`:

<<< @/examples/nomad/config.yaml{yaml}

`Containerfile`:

<<< @/examples/nomad/Containerfile{dockerfile}

`wherry-receiver.nomad.hcl`:

<<< @/examples/nomad/wherry-receiver.nomad.hcl{hcl}
