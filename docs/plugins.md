# Plugins

A plugin is the binary `wherry-plugin-<type>` that supplies
support for one kind of platform, which the receiver starts from its plugin
directory and talks to over the plugin protocol. A receiver runs each task
on a plugin profile, a named, configured use of a plugin with its agent, labels
and runtime, and one plugin process serves every one of its profiles in the
receiver's [configuration](configuration.md#receiver-configuration). The
[plugins reference](commands.md#plugins) specifies what a receiver expects
of a plugin, and the protocol is the Go module in
[`pluginsdk`](https://github.com/benemon/wherry/tree/main/pluginsdk).

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

A plugin profile of the Nomad plugin names one driver and its configuration, and
the receiver's configuration can hold several profiles of the Nomad
plugin, each with its own driver and labels. So one receiver can offer containers, host
processes and virtual machines side by side, and a task chooses between
them by its agent and label selector, the way it chooses anything else.
The Nomad plugin hands the driver to Nomad unchanged.

The
[Nomad plugin's README](https://github.com/benemon/wherry-plugins/tree/main/nomad)
gives the settings for each driver.

## Running a receiver

The [receiver examples](https://github.com/benemon/wherry-plugins/tree/main/examples) in wherry-plugins show ways to
run a receiver with its plugins. [Run a receiver](operations.md#run-a-receiver)
lists what every receiver needs, and each plugin's README gives the full
instructions.

### systemd

A local configuration and a service unit for the receiver, a slice for the
agents' units, and a tmpfiles entry for the credential mount point. The
[systemd plugin's README](https://github.com/benemon/wherry-plugins/tree/main/systemd#run-the-receiver-as-a-systemd-service)
covers installing them.

A systemd receiver adopts only the jobs on its own host. In a pool with
receivers on several hosts, a task whose holder is lost, the latest receiver
the key custodian served it to that has not since released it, starts again
on another host once the lease lapses, 120 seconds after the holder's last
advertisement, or once its membership ends, while the first job may still
run. Give each systemd
receiver a pool of its own.

`config.yaml`:

<<< @/examples/systemd/config.yaml{yaml}

`wherry-receiver.service`:

<<< @/examples/systemd/wherry-receiver.service{systemd}

`wherry.slice`:

<<< @/examples/systemd/wherry.slice{systemd}

`wherry-tmpfiles.conf`:

<<< @/examples/systemd/wherry-tmpfiles.conf{txt}

### Kubernetes

A receiver image, its local configuration, and a Kustomize directory with the
Deployment, service account, Role, state claim, runs claim and metrics Service,
applied with `kubectl apply -k`, on Kubernetes and OpenShift alike. The
directory makes the Secret from the receiver's bundle and the ConfigMap from
`config.yaml`. The receiver pod and every agent pod mount the runs claim, so the
cluster needs a storage class that provisions ReadWriteMany claims, such as
CephFS, NFS, EFS or Azure Files. An agent pod runs the agent's image alone, the
plugin's config has no `helper_image`, and an upgrade is an image change. The
[Kubernetes plugin's README](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#run-the-receiver-in-the-cluster)
covers building the image and deploying it.

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

A receiver image with the Nomad plugin, its local configuration, and a job
that renders the bundle and `config.yaml` from a Nomad Variable. The
[Nomad plugin's README](https://github.com/benemon/wherry-plugins/tree/main/nomad#run-the-receiver-in-a-container)
covers building and running it.

`config.yaml`:

<<< @/examples/nomad/config.yaml{yaml}

`Containerfile`:

<<< @/examples/nomad/Containerfile{dockerfile}

`wherry-receiver.nomad.hcl`:

<<< @/examples/nomad/wherry-receiver.nomad.hcl{hcl}
