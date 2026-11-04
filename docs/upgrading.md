# Upgrading

## Before you start

Read the [release notes](https://github.com/benemon/wherry/releases) for compatibility requirements.
Check record formats, key file formats and the plugin protocol version.
Keep each receiver's local configuration, bundle and state directory.
Verify the new binary as described in [Install wherry](quickstart-production.md#install-wherry).

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
