# Receiver on systemd

Run receiver `systemd-1` as a systemd service on a Linux host.
The three receiver pages are alternatives.
Follow this one only if your receiver runs under systemd.

## Before you start

- The working directory `wherry-prod/` from [Set up the operator](install-operator.md).
- Claude Code installed as `claude` on the receiver host.
  The record below runs it as `/usr/local/bin/claude`.

Each [wherry release](https://github.com/benemon/wherry/releases)'s notes name the plugin releases that pair with it.
Choose a systemd plugin release that pairs with your wherry release.

## Fit the steps to this guide

- The record uses pool `build` from Set up the operator.
  With one receiver, `build` is that receiver's pool.
  It is a pool of its own, as the steps ask.
- This guide's broker uses a private CA, `kafka-ca.pem` from Set up the operator.
  Take the steps' lines for a private broker CA.
- The steps install files from the operator machine on the receiver host: the receiver's bundle, its local
  configuration and `kafka-ca.pem`.
  Copy them over a channel you trust.
  The bundle holds the receiver's Kafka password.
- The last step runs `wherry host receivers` from your laptop.
  You run it first on [Set up a host](install-host.md#_3-see-the-receiver).

<!--@include: ./plugins/systemd/README.md#install-->
