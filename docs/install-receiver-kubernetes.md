# Receiver on Kubernetes and OpenShift

Run receiver `kubernetes-1` on your Kubernetes or OpenShift cluster.
The three receiver pages are alternatives.
Follow this one only if your receiver runs on Kubernetes or OpenShift.

## Before you start

- The working directory `wherry-prod/` from [Set up the operator](install-operator.md).
- On the operator machine: `git`, `make`, Podman or Docker with push access to a registry your cluster pulls from, and
  `kubectl` or `oc` signed in to the cluster.
- An agent image your cluster can pull, here `registry.example.com/claude-agent:1.0`, with Claude Code installed as
  `claude`. It must meet the [agent contract](configuration-receiver.md#agent-contract).

Each [wherry release](https://github.com/benemon/wherry/releases)'s notes name the plugin releases that pair with it.
Choose a Kubernetes plugin release that pairs with your wherry release.

## Fit the steps to this guide

- The record uses pool `build` from Set up the operator.
  With one receiver, `build` is that receiver's pool.
- The record's `broker:` line names a Kafka inside the cluster.
  Set it to the address you gave `wherry operator init`, or remove the line to use that address.
- This guide's broker uses a private CA, `kafka-ca.pem` from Set up the operator.
  Take the steps' lines for a private broker CA.
- The last step runs `wherry host receivers` from your laptop.
  You run it first on [Set up a host](install-host.md#_3-see-the-receiver).
- The steps copy the example `config.yaml` to `receivers/kubernetes-1.config.yaml`.
  Without this file, `receiver apply` asks for each value instead.
  [Receiver local configuration](configuration-receiver.md#receiver-local-configuration) lists the questions and each
  plugin example's answers.

<!--@include: ./plugins/kubernetes/README.md#install-->
