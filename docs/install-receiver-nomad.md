# Receiver on Nomad

Run receiver `nomad-1` on your Nomad cluster.
The three receiver pages are alternatives.
Follow this one only if your receiver runs on Nomad.

## Before you start

On the operator machine:

- The working directory `wherry-prod/` from [Set up the operator](install-operator.md).
- `git`, and Docker with push access to a registry your Nomad clients pull from. This guide uses
  `registry.example.com`.
- The `nomad` CLI, set up to reach your cluster: `NOMAD_ADDR`, and whatever
  [TLS settings](https://developer.hashicorp.com/nomad/docs/commands#environment-variables) your cluster needs. With
  ACLs enabled, `NOMAD_TOKEN` must be a management token, because a step below attaches the plugin's ACL policy.

On the Nomad cluster:

- An agent image your clients can pull, here `registry.example.com/claude-agent:1.0`, with Claude Code installed as
  `claude`. It must meet the [agent contract](configuration-receiver.md#agent-contract).

Each [wherry release](https://github.com/benemon/wherry/releases)'s notes name the plugin releases that pair with it.
Choose a Nomad plugin release that pairs with your wherry release.

## Fit the steps to this guide

- The record uses pool `build` from Set up the operator.
  With one receiver, `build` is that receiver's pool.
- This guide's broker uses a private CA, `kafka-ca.pem` from Set up the operator.
  Take the steps' lines for a private broker CA.
- The last step runs `wherry host receivers` from your laptop.
  You run it first on [Set up a host](install-host.md#_3-see-the-receiver).

<!--@include: ./plugins/nomad/README.md#install-->
