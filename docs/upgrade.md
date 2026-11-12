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

## Optional credentials from v0.1.2

From v0.1.2 a host delivers an optional agent credential, one the pool's
terms mark `required: false`, only when the task requests it: with
`wherry host submit --credential NAME`, or through MCP with a `credentials`
entry of its name alone. Before v0.1.2 a host delivered every optional
credential it had a source for. Runs that relied on that automatic delivery
now go without the credential, and their prompt names it as not supplied.

- Request the credential in each task that needs it. `submit` prints
  `unrequested=<name,...>`, and the MCP `submit` tool returns
  `unrequested`, naming the optional credentials a task left out.
- Make a credential every run needs required in the pool's terms, by
  removing `required: false`, and apply the terms with
  `wherry operator pool apply`.
- `wherry dev up` makes each credential it copies required, except
  `anthropic-api-key` beside `claude-oauth-token`; see
  [`wherry dev`](commands-dev.md). A dev run that needs the API key while
  the subscription token is present requests it with
  `--credential anthropic-api-key`.

From v0.1.2 a submission names the agent credentials it delivers, in
`supplied_credentials`, in place of the ones it does not, in
`unsupplied_credentials`. A receiver ignores the old field, so hosts and
receivers of different versions do not work together, and every mismatch
fails closed:

- A run from a v0.1.2 host on a receiver before v0.1.2 fails `envelope invalid`
  when the task does not request every optional credential of the agent, and
  works otherwise.
- A run from a host before v0.1.2 on a v0.1.2 receiver is rejected with `the
  host does not supply a required credential of the agent` when the agent has
  a required credential. Otherwise it fails `envelope invalid` when the old
  host sealed any of the agent's credentials.

Upgrade hosts and receivers together, and let queued tasks drain first.

A v0.1.2 host cannot read a run record that a host before v0.1.2 wrote with
`unsupplied_credentials` in its state directory: `wherry host runs` skips
it, and `status`, `wait` and resubmission of the run fail. Let runs finish,
or wait them out, before you upgrade a host.

A run that loses its pool key before it starts is resubmitted with the
credentials of the agent's terms at resubmission: every required one, and
every one the original run supplied. A credential the terms removed is
dropped, and an optional one they added is not delivered. A credential the
terms made optional after the submit is still delivered, as it was to the
original run. When the terms made a credential required and the host has no
source for it, the resubmission is refused `auth required`, the run's status
is unchanged and the host logs the refusal on each pass until the run's
`task_started_by`. A later pass resubmits it once the credential is
configured.

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
