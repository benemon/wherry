# Install

Set up wherry across machines and run a first task from your laptop.

## What you'll build

You'll work in three places.
Each page of this guide happens in one of them.

| Place | What runs there | Where its files live |
| --- | --- | --- |
| Operator machine | `wherry operator` | the working directory `wherry-prod/` |
| The receiver platform you choose | the receiver, and the agent jobs it starts | as your receiver page sets out |
| Your laptop | `wherry host` and your coding agent | `~/.config/wherry/` |

## Choose a receiver platform

The three receiver pages are alternatives.
Follow one of them.

| Platform | Receiver page | Receiver | Profile |
| --- | --- | --- | --- |
| Nomad | [Receiver on Nomad](install-receiver-nomad.md) | `nomad-1` | `container` |
| Kubernetes or OpenShift | [Receiver on Kubernetes and OpenShift](install-receiver-kubernetes.md) | `kubernetes-1` | `container` |
| systemd | [Receiver on systemd](install-receiver-systemd.md) | `systemd-1` | `host` |

The names used throughout are pool `build`, agent `claude`, tenant `acme` and host `laptop`.

## The files you'll make

| File | Made by | Lives in | What it is |
| --- | --- | --- | --- |
| `operator.yaml`, `operator.password` | `operator init` | `wherry-prod/` | how `wherry operator` connects to Kafka |
| `kafka-ca.pem` | you, from your Kafka cluster | `wherry-prod/` | the CA your broker's certificate chains to |
| `pools/build.yaml` | you | `wherry-prod/` | the pool's terms: its agents and limits |
| `receivers/<name>.yaml` | you, on your receiver page | `wherry-prod/` | the receiver's configuration: its pool, plugin pin and profiles |
| `receivers/<name>.config.yaml` | you, on your receiver page | `wherry-prod/` | the receiver's local configuration: where it finds its files on its platform |
| `bundles/receivers/<name>.bundle` | `receiver apply` | `wherry-prod/` | the receiver's Kafka identity: broker, principal and password |
| `bundles/hosts/acme/laptop.bundle` | `tenant host add` | `wherry-prod/` | your laptop's Kafka identity |
| `config.yaml`, `bundle.yaml`, `host.key`, `ca.pem` | `host init` and you | `~/.config/wherry/` on your laptop | your laptop's configuration |

The bundles hold passwords.
`operator init` keeps `bundles/` and `operator.password` out of git through `wherry-prod/.gitignore`.

## The order of the pages

1. [Install the wherry CLI](install-cli.md) on the operator machine and on your laptop.
2. [Set up the operator](install-operator.md): Kafka, the pool, the tenant and your laptop's bundle.
   It lists the [Kafka prerequisites](install-operator.md#before-you-start).
3. Run the receiver on one platform: [Nomad](install-receiver-nomad.md),
   [Kubernetes and OpenShift](install-receiver-kubernetes.md) or [systemd](install-receiver-systemd.md).
4. [Set up a host](install-host.md) on your laptop.
5. [Run a first task](install-first-task.md).
