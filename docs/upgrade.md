# Upgrade and uninstall

Upgrade wherry and its plugins in place, and uninstall a receiver or a host.

## Before you start

Read the [release notes](https://github.com/benemon/wherry/releases) for compatibility requirements.
Check record formats, key file formats and the plugin protocol version.
Keep each receiver's local configuration, bundle and state directory.
Verify the new binary as described in [Install the wherry CLI](install-cli.md).

## Upgrade wherry

For compatible record formats, upgrade in place:

1. Replace the binary on each host.
2. Verify each new plugin and record its SHA-256 pin with `wherry operator receiver apply`.
   Follow [Install plugins](operations-receivers.md#install-plugins).
   Use plugins built for the receiver's protocol version.
3. Update each receiver's binary and plugins together.
   Restart it with the same configuration, bundle and state directory.
   An intermediate restart refuses a binary that does not match its recorded pin.
4. Restart the harness sessions that run `wherry host mcp`.
   Replacing the binary does not restart an existing MCP process.

Restart a pool's receivers one at a time.
A stopping key custodian needs a live successor to re-key its tasks.
Follow [Stop a pool's key custodian](operations-receivers.md#stop-a-pool-s-key-custodian).

## What happens to running work?

On `SIGINT` or `SIGTERM`, a receiver stops claiming and closes idle claims.
It lets held runs continue for up to `drain_timeout`.
It then releases their tasks and leaves unfinished jobs running.
A later delivery can adopt the job and resume its transcript from the last committed offset.
Adoption depends on the platform's scope.
See [Plugins](plugins.md#running-a-receiver).

A stop takes up to `drain_timeout` plus five seconds.
Set the supervisor's stop window above that bound.
A 30-second window accommodates `drain_timeout: 20s`.

## How do you check the upgrade?

1. Run `wherry operator check`.
2. Run `wherry host receivers` and check that the expected profiles are live and ready.
3. Submit a task and collect its result with `wherry host wait RUN_ID`.

Readers ignore unknown operator-record members.
They reject a record version higher than they support.
Upgrade a reader that reports this error.
Host and receiver signing keys must be private JWKs.
Follow the release's migration instructions when formats change.
For a [deployment reset](operations-operator.md#reset-a-deployment), move incompatible keys aside before provisioning new identities.

## Upgrade and uninstall a receiver

Each receiver page gives its platform's upgrade and uninstall steps.

| Platform | Upgrade | Uninstall |
| --- | --- | --- |
| Nomad | [Upgrade](install-receiver-nomad.md#upgrade) | [Uninstall](install-receiver-nomad.md#uninstall) |
| Kubernetes and OpenShift | [Upgrade](install-receiver-kubernetes.md#upgrade) | [Uninstall](install-receiver-kubernetes.md#uninstall) |
| systemd | [Upgrade](install-receiver-systemd.md#upgrade) | [Uninstall](install-receiver-systemd.md#uninstall) |

## Uninstall a host

1. On the operator machine, in the working directory, remove the host:

   ```sh
   wherry operator tenant host remove TENANT HOST
   ```

   For this guide's host, that is `wherry operator tenant host remove acme laptop`.
   [`tenant host remove`](commands.md#wherry-operator-tenant-host-remove-tenant-host) runs the host's revocation steps.

2. After it succeeds, delete `~/.config/wherry/` on the laptop.
   It holds `config.yaml`, `bundle.yaml`, `host.key` and `ca.pem`.
3. Delete `~/claude-oauth-token` on the laptop when nothing else uses it.
4. On the operator machine, delete `bundles/hosts/<tenant>/<host>.bundle`.
   Host removal does not delete it.

Removing the host does not revoke the Claude token.

Check that it worked: `wherry operator tenant list` no longer lists the host under its tenant.
