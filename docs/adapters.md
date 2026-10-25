# Adapter plugins

A receiver runs each task through an adapter, and each adapter type is a
plugin: a separate binary the receiver starts from its plugin directory and
talks to over the adapter protocol. The
[adapter plugins reference](commands.md#adapter-plugins) specifies what a
receiver expects of a plugin, and the protocol is the Go module in
[`adapter`](https://github.com/benemon/wherry/tree/main/adapter).

The plugins live in
[wherry-plugins](https://github.com/benemon/wherry-plugins), each with its
own README covering installation, settings and the platform's
permissions:

- [Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad)
- [Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes),
  OpenShift included
- [systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd)

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
