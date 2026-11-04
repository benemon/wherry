# Receiver on Kubernetes and OpenShift

Run receiver `kubernetes-1` on your Kubernetes or OpenShift cluster.
The three receiver pages are alternatives, so follow this one only if your receiver runs on Kubernetes or OpenShift.

## Before you start

- The working directory `wherry-prod/` from [Set up the operator](install-operator.md).
- An agent image your cluster can pull, here `registry.example.com/claude-agent:1.0`, with Claude Code installed as
  `claude`. It must meet the [agent contract](configuration-receiver.md#agent-contract).

Each [wherry release](https://github.com/benemon/wherry/releases)'s notes name the plugin releases that pair with it.
Choose a Kubernetes plugin release that pairs with your wherry release.

## Fit the steps to this guide

- The record uses pool `build` from Set up the operator.
  With one receiver, `build` is that receiver's pool.
- This guide's broker uses a private CA, `kafka-ca.pem` from Set up the operator.
  Take the steps' lines for a private broker CA.
- The last step runs `wherry host receivers` from your laptop.
  You run it first on [Set up a host](install-host.md#_3-see-the-receiver).

<!--@include: ./plugins/kubernetes/README.md#install-->
