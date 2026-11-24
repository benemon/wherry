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
  A pool is one trust and tenancy boundary; the platform is a label.
  `build` is that boundary here.
- This guide's broker uses a private CA. Install it where the receiver runs, or put its PEM in the receiver's
  local configuration as `ca_cert`, as [Broker and S3 trust](security.md#broker-and-s3-trust) describes.
- The steps install files from the operator machine on the receiver host: the receiver's bundle and its local
  configuration.
  Copy them over a channel you trust.
  The bundle holds the receiver's Kafka password.
- The last step runs `wherry host receivers` from a host.
  You set up your laptop as a host on a later page.
  Run the check at the end of this page from the operator machine instead.
- The steps copy the example `config.yaml` to `receivers/systemd-1.config.yaml`.
  Without this file, `receiver apply` asks for each value instead.
  [Receiver local configuration](configuration-receiver.md#receiver-local-configuration) lists the questions and each
  plugin example's answers.

<!--@include: ./plugins/systemd/README.md#install-->

## Check that it worked

On the operator machine, in `wherry-prod/`:

```sh
wherry operator receiver show systemd-1
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
