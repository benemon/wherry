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
  A pool is one trust and tenancy boundary; the platform is a label.
  `build` is that boundary here.
- The record's `broker:` line names a Kafka inside the cluster.
  Set it to the address you gave `wherry operator init`, or remove the line to use that address.
- This guide's broker uses a private CA. The bundle carries it, so skip the steps' lines for a separate CA file;
  a `ca_file` in the local configuration would override the bundle's CA.
- The record's profile names `agent: claude`.
  Set it to `claude-code`, the agent of pool `build`.
  `receiver apply` refuses a profile whose agent is outside the pool's terms.
- The last step runs `wherry host receivers` from a host.
  You set up your laptop as a host on a later page.
  Run the check at the end of this page from the operator machine instead.
- The steps copy the example `config.yaml` to `receivers/kubernetes-1.config.yaml`.
  Without this file, `receiver apply` asks for each value instead.
  [Receiver local configuration](configuration-receiver.md#receiver-local-configuration) lists the questions and each
  plugin example's answers.

<!--@include: ./plugins/kubernetes/README.md#install-->

## Check that it worked

On the operator machine, in `wherry-prod/`:

```sh
wherry operator receiver show kubernetes-1
```

The first line is `status: live` once the receiver's first advertisement is verified against its key topic:

```yaml
status: live
last_advertised: 2026-11-20T09:41:07.312Z
```

A receiver that has not advertised shows `status: silent` and no `last_advertised` line.
Read its logs as the steps above show.
[`receiver show`](commands.md#wherry-operator-receiver-show-receiver) describes each status.

Next, [set up a host](install-host.md).
Its last step, `wherry host receivers`, shows the receiver `live` from your laptop.
