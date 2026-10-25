# Adapters

An adapter is a plugin: the binary `wherry-plugin-<type>` that supplies
support for one kind of platform, which the receiver starts from its plugin
directory and talks to over the adapter protocol. A receiver runs each task
on a profile, a named, configured use of an adapter with its agent, labels
and runtime, and one adapter serves every profile of its type in the
receiver's [configuration](configuration.md#receiver-configuration). The
[plugins reference](commands.md#plugins) specifies what a receiver expects
of a plugin, and the protocol is the Go module in
[`adapter`](https://github.com/benemon/wherry/tree/main/adapter).

The plugins live in
[wherry-plugins](https://github.com/benemon/wherry-plugins), each with its
own README covering installation, settings and the platform's
permissions:

- [Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad)
- [Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes),
  OpenShift included
- [systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd)

## Why Nomad

Nomad is the primary platform for receivers because one Nomad cluster
runs many kinds of workload. Each Nomad
[task driver](https://developer.hashicorp.com/nomad/docs/deploy/task-driver) runs a
different kind: containers through Docker or Podman, an isolated process
on the host through `exec`, a plain host process through `raw_exec`, a
virtual machine through QEMU, and further kinds through community driver
plugins. A cluster's clients can themselves be bare-metal hosts, virtual
machines or cloud instances. Nomad places each job on a client whose driver
is healthy, within the node pool and datacenters the job names.

A profile of the Nomad adapter names one driver and its configuration, and
the receiver's configuration can hold several profiles of the Nomad
adapter, each with its own driver and labels. So one receiver can offer containers, host
processes and virtual machines side by side, and a task chooses between
them by its agent and label selector, the way it chooses anything else.
The Nomad plugin hands the driver to Nomad unchanged; the drivers
exercised with wherry so far are Docker, Podman and `exec`.

The
[Nomad plugin's README](https://github.com/benemon/wherry-plugins/tree/main/nomad)
gives the settings for each driver.

## Running a receiver

The [receiver examples](https://github.com/benemon/wherry-plugins/tree/main/examples) in wherry-plugins show ways to
run a receiver with its plugins. [Run a receiver](operations.md#run-a-receiver)
lists what every receiver needs, and each plugin's README gives the full
instructions.

### systemd

A service unit for the receiver, a slice for the agents' units, and a tmpfiles
entry for the credential mount point. The
[systemd plugin's README](https://github.com/benemon/wherry-plugins/tree/main/systemd#run-the-receiver-as-a-systemd-service)
covers installing them.

`wherry-receiver.service`:

<<< @/examples/systemd/wherry-receiver.service{systemd}

`wherry.slice`:

<<< @/examples/systemd/wherry.slice{systemd}

`wherry-tmpfiles.conf`:

<<< @/examples/systemd/wherry-tmpfiles.conf{txt}

### Kubernetes

A receiver image, a manifest with the Deployment, service account, state claim
and metrics Service, and a script that applies them, on Kubernetes and
OpenShift alike. The
[Kubernetes plugin's README](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#run-the-receiver-in-the-cluster)
covers building the image and deploying it.

`Containerfile`:

<<< @/examples/kubernetes/Containerfile{dockerfile}

`receiver.yaml`:

<<< @/examples/kubernetes/receiver.yaml{yaml}

`apply.sh`:

<<< @/examples/kubernetes/apply.sh{bash}

### Nomad

A receiver image with the Nomad plugin. The
[Nomad plugin's README](https://github.com/benemon/wherry-plugins/tree/main/nomad#run-the-receiver-in-a-container)
covers building and running it.

`Containerfile`:

<<< @/examples/nomad/Containerfile{dockerfile}
