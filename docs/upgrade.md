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

Upgrade hosts with their receivers.
A host older than its receivers rejects each status of a kind it does not know, and warns on each read.
Kind `working`, which a receiver publishes from an agent's [working file](configuration-receiver.md#agent-contract), is one.

Restart a pool's receivers one at a time.
A stopping key custodian needs a live successor to re-key its tasks.
Follow [Stop a pool's key custodian](operations-receivers.md#stop-a-pool-s-key-custodian).

## The broker CA in the bundle

Bundles carry the broker's CA as `ca` when `operator.yaml` names a
`ca_file`, and hosts and receivers trust the broker through their own
`ca_file`, else the bundle's `ca`, else the system roots. A host no longer
reads `ca.pem` from its configuration directory, and neither `host init` nor
`receiver apply` asks for a CA.

An older binary refuses a bundle that carries `ca`, or the `storage` pair a
host bundle carries once `tenant storage` recorded it, since unknown fields
are errors; the new binary with an older bundle and no `ca_file` cannot
reach a broker under a private CA. Each host and receiver therefore takes
the new binary and its refreshed bundle in one step:

1. Upgrade the operator's binary.
2. Run `wherry operator tenant host refresh TENANT HOST` for each host and
   `wherry operator receiver apply RECEIVER` for each receiver, which
   rewrite each kept bundle with the CA and the same password, and hand
   each refreshed bundle over as the first was.
3. On each host, replace the binary and run `wherry host init BUNDLE` with
   the refreshed bundle, then remove the `ca_file` line from the
   configuration and `ca.pem` from the configuration directory. Restart the
   harness sessions that run `wherry host mcp`.
4. On each receiver, replace the binary and the bundle together and restart
   the receiver; remove `ca_file` from its local configuration.

Until a host or receiver takes both, it keeps the old binary with the old
bundle and its CA file, which still work.
[Renew the broker CA](security.md#renew-the-broker-ca) gives the renewal
procedure for later CA changes.

## Optional credentials the host resolves

A host now sends an optional agent credential, one the pool's terms mark
`required: false`, to every run whose submit resolves it: from the source
the host's configuration names, or, when the credential's target is one of
the [variables the catalog reads](credential-variables.md), from that
variable in the process that submits. The ask-first rule of v0.1.2, below,
no longer applies: a task need not request an optional credential the host
has, and `--credential NAME`, or an MCP `credentials` entry with a name
alone, now means the task cannot go without it, failing the submit when the
host cannot resolve it.

- `submit` prints `unsupplied=<name,...>`, and the MCP `submit` tool returns
  `unsupplied`, naming the optional credentials the host could not resolve,
  in place of the earlier report of the ones a task left out.
- Existing pools keep working as they are. An agent whose name is no
  [credential catalog id](configuration-operator.md#catalog-agents), such
  as `claude` or `gemini`, stays supported with the credential list the
  operator wrote, and `pool apply` records it as before. Renaming is needed
  only to adopt catalog expansion, under which `wherry operator pool apply`
  derives an agent's model credentials from its name, for example
  `claude-code` for Claude Code and `google` for Gemini, and the operator
  writes none. Follow [Rename an agent to its catalog id](#rename-an-agent-to-its-catalog-id)
  below; editing the name in place is refused.
- A non-catalog agent, one that keeps a name that is no catalog id and its
  explicit credential list, with an optional billing credential the host can
  resolve now receives it on every run. An optional `anthropic-api-key`
  beside a Claude subscription token switches Claude Code to API billing on
  every run from a host that has `ANTHROPIC_API_KEY` set where submits run
  or a source for it configured, where before only a task that named it was
  billed that way. Decide in the terms, by removing the credential or making
  it required, or on the host, by removing its source and unsetting its
  variable.
- A host whose configuration names no source for a credential whose target
  is a catalog variable reads the variable where submits run, the MCP server
  a harness starts included. A set variable reaches every run of that agent:
  see the [warning](security.md#credential-envelopes) and, to hold one back,
  unset it in the process that submits and remove any explicit source for
  that credential from the host configuration.
- A resubmission resolves the terms afresh: an optional credential whose
  variable is unset at resubmit is left out, and an explicit source that
  fails is an error, logged on each pass.
- Receivers are unchanged: they deliver what `supplied_credentials` names.
- `wherry host init` asks no question for a credential whose target is a
  catalog variable and prints one line per entitled agent, ready or not
  ready, in place of the list of credentials without a source.

### Rename an agent to its catalog id

A receiver's profiles name their agent, and `pool apply` checks every live
receiver of the pool against the new terms before it records them, so
terms that only rename an agent are refused, in a dry run and in an apply,
and the recorded terms stay as they were:

```
wherry: pool terms would strand live receivers: receiver nomad-1 plugin nomad profile container: receiver agent is outside pool terms
```

Rename in three stages. The example takes `claude`, with
`claude-oauth-token` at `CLAUDE_CODE_OAUTH_TOKEN` written by hand, to
`claude-code`:

1. Add the catalog-named agent beside the old one in `pools/<pool>.yaml`,
   with the old agent's `models` and `default_model` and any credential
   outside the catalog but no model credential, keep the old agent as it
   is, and apply the pool:

   ```sh
   wherry operator pool apply build
   ```

   Copy `default_model` with `models`: an agent with several models and no
   `default_model` makes every task name one, so a task that named none
   for the old agent would be refused for the new. The recorded terms now
   hold both agents, `claude-code` with the derived
   `claude-code-oauth-token`, which a host's `wherry host receivers` shows
   once a receiver offers the agent.

2. Move each receiver to the new agent. In its configuration,
   `receivers/<receiver>.yaml` or the receiver's directory, set
   `agent: claude-code` on each profile that ran `claude`, change the
   deployment settings that go with the agent, such as the image the
   profile's `config` names, then check and apply it:

   ```sh
   wherry operator receiver apply nomad-1 -f nomad-1 --dry-run
   wherry operator receiver apply nomad-1 -f nomad-1
   ```

   The receiver exits once its held runs end and its platform restarts it
   on the new record, as [Change a receiver's configuration](operations-operator.md#change-a-receiver-s-configuration)
   describes; move one receiver at a time. The moved profile's signature
   topic is new, so tasks queued for `claude` stay on its topics, and the
   restart drains only the runs the receiver holds. Empty each
   queue before step 3, in one of two ways. To let the queued tasks finish
   on `claude`, point new submissions at `claude-code` first, keep one
   profile on `claude` for each signature with tasks still queued until
   its queue drains, and move that last profile only then. Otherwise
   cancel the queued tasks and submit them again for `claude-code`; a
   receiver applies a cancel when it claims the task, so a cancel of a
   task on a signature no profile serves stays pending.
   Hosts that name the agent, with `--agent` or the MCP `agent` field, now
   name `claude-code`; a host whose configuration gives the old credential
   name a source moves that entry to the derived name, or drops it when
   `CLAUDE_CODE_OAUTH_TOKEN` is set where submits run.

3. Remove the old agent from the terms, set `default_agent` to the new
   name when it named the old one, and apply the pool again:

   ```sh
   wherry operator pool apply build --dry-run
   wherry operator pool apply build
   ```

   The dry run refuses, naming the receiver and profile, while any live
   receiver still runs the old agent.

## Optional credentials from v0.1.2

Superseded by [Optional credentials the host resolves](#optional-credentials-the-host-resolves),
kept for the history of the wire field. From v0.1.2 a host delivered an
optional agent credential, one the pool's
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

### Per-profile use and the waiting reason

From v0.1.2-rc.2 each offer in a receiver's advertisement carries the
profile's `replicas` and `in_flight`, and the advertisement carries `gated`
while the receiver claims nothing. A host places a task by the spare of the
matching profiles rather than the receiver's, and says why a run waits. An
older receiver's offers read as one replica with nothing in flight, so
upgrade receivers and hosts together.

The text for a run past `task_started_by` that no receiver claimed changes
from `...: no receiver claimed the task` to
`...: no receiver claimed the task; now: <reason>`. Update anything that
matches the old ending exactly.

`wherry host cancel` records when it first cancelled a run in the run's local
record, and a host before v0.1.2-rc.2 cannot read a record holding that time:
`status`, `wait` and resubmission of the run fail on it. Upgrade every wherry
binary that shares a state directory together, and do not go back to an
older one while a cancelled run's record is kept.

## Continued runs and run contexts from v0.1.3

From v0.1.3 a run's A2A `contextId` need not be its own run ID.
[`wherry host submit --continue`](commands.md#wherry-host-submit) gives a
run the context of the run it continues, and every run that loses its pool
key before it starts is resubmitted with its original's `contextId`, not the
replacement's own ID. Receivers, custodians and hosts before v0.1.3 reject a
record whose `contextId` is not its run ID:

- A receiver or custodian before v0.1.3 rejects such a submission without a
  status, so the run shows waiting, then unclaimed at `task_started_by`. One
  such receiver in a pool rejects it for every receiver there. An older
  custodian that hands over its pool keys skips such a task without a
  re-key, so after the custodian changes no receiver can open it and the run
  ends key-lost. Its replacement keeps the `contextId`, so the next such
  handover loses it too.
- A receiver before v0.1.3 ignores a submission's `git.base` and would clone
  the ref; only the `contextId` check stops it.
- A host before v0.1.3 drops every record of such a run from the tenant
  feed, and cannot read a run record that holds a continued run's task or a
  collected `changes.bundle`: `status`, `wait` and resubmission of the run
  fail on it.

Upgrade in this order:

1. Every receiver and custodian in each pool.
2. Every host binary of the tenant, together when they share a state
   directory, and restart each `wherry host mcp` server.
3. Then use `--continue`.

A host upgraded before its receivers breaks the resubmission of every run
that loses its pool key, not only of continued runs.

## Trace topics and the operator's group grant

Spans go to a trace topic per tenant, `wherry.traces.<tenant>`, and to
`wherry.traces` ([Tracing](tracing.md)). `tenant add` now creates the
tenant's topic, and `wherry.operator` needs `Describe` on the pool groups for
the operator view's share-group state. An existing install gets the topics of
its tenants and the grant by running `init` again:

1. Upgrade the operator's binary.
2. Run `wherry operator init` with the Kafka super user, as at setup. It
   grants `wherry.operator` `Describe` on the group prefix `p-`, creates
   `wherry.traces` and the trace topic of every live tenant where missing, and
   sets their configuration.
3. `init` rotates `wherry.operator`'s password, as every re-run does: every
   other copy of the operator's working directory must take the new
   `operator.password` before its commands run again.

`wherry operator check` is the signal that the step is due: on an install
that predates it, check reports
`missing DESCRIBE ACL on p- for wherry.operator` or
`trace topic wherry.traces is missing; operator init creates it`. Until the
step runs, a process with tracing on logs the expected refusals,
`UNKNOWN_TOPIC_OR_PARTITION` and `TOPIC_AUTHORIZATION_FAILED`, once an hour
and loses those spans; nothing else is affected.

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
   It holds `config.yaml`, `bundle.yaml` and `host.key`.
3. Delete `~/claude-oauth-token` on the laptop when Set up a host made it and nothing else uses it.
4. On the operator machine, delete `bundles/hosts/<tenant>/<host>.bundle`.
   Host removal does not delete it.

Removing the host does not revoke the Claude token.

Check that it worked: `wherry operator tenant list` no longer lists the host under its tenant.
