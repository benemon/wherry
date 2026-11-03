# Plugins

A plugin is the binary `wherry-plugin-<type>` that supplies
support for one kind of platform, which the receiver starts from its plugin
directory and talks to over the plugin protocol. A receiver runs each task
on a plugin profile, a named, configured use of a plugin with its agent, labels
and runtime, and one plugin process serves every one of its profiles in the
receiver's [configuration](configuration.md#receiver-configuration).
[Plugin protocol](plugin-protocol.md) specifies what a plugin implements,
and [Plugins](commands.md#plugins) what a receiver does with it.

The plugins live in
[wherry-plugins](https://github.com/benemon/wherry-plugins), each with its
own README covering installation, settings and the platform's
permissions:

- [Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad)
- [Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes),
  OpenShift included
- [systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd)

## Why Nomad

One Nomad cluster runs many kinds of workload, each through a
[task driver](https://developer.hashicorp.com/nomad/docs/deploy/task-driver).

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

[Run the receiver as a systemd service](https://github.com/benemon/wherry-plugins/tree/main/systemd#run-the-receiver-as-a-systemd-service)
describes these files and installs them.

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

[Run the receiver in the cluster](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#run-the-receiver-in-the-cluster)
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

[Run the receiver in Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#run-the-receiver-in-nomad)
describes these files and runs them.

`config.yaml`:

<<< @/examples/nomad/config.yaml{yaml}

`Containerfile`:

<<< @/examples/nomad/Containerfile{dockerfile}

`wherry-receiver.nomad.hcl`:

<<< @/examples/nomad/wherry-receiver.nomad.hcl{hcl}
