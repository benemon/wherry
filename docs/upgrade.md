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
2. Verify each new plugin and record its SHA-256 pin with `wherry operator apply --receiver`.
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

## Command grammar

Every command is its role word then a verb, one level deep, and the
operator's resources are flags: `wherry operator <verb>` with `--pool POOL`,
`--receiver RECEIVER`, `--tenant TENANT` or `--host HOST --tenant TENANT`.
The commands below are renamed in a clean break: the old forms fail with
`unknown operator command`, and there are no aliases. Short flags are gone
everywhere, and the option that lengthens a removal's wait is `--timeout`;
the table gives each. Output
and exit codes of every renamed command are unchanged. Update scripts that
call the old forms; the plugin READMEs' and e2e scripts' examples follow with
their next release.

| Before | Now |
| --- | --- |
| `wherry operator tenant list` | `wherry operator list tenants`; `list hosts [--tenant T]` for its host lines |
| `wherry operator receiver show RECEIVER` | `wherry operator show --receiver RECEIVER` |
| `wherry operator pool apply POOL [-f PATH] [--dry-run]` | `wherry operator apply --pool POOL [--file PATH] [--dry-run]` |
| `wherry operator receiver apply RECEIVER [-f PATH] [--dry-run]` | `wherry operator apply --receiver RECEIVER [--file PATH] [--dry-run]` |
| `wherry operator receiver apply RECEIVER --force` | `wherry operator apply --receiver RECEIVER --force` |
| `wherry operator tenant add TENANT` | `wherry operator add --tenant TENANT` |
| `wherry operator tenant host add TENANT HOST` | `wherry operator add --host HOST --tenant TENANT` |
| `wherry operator tenant host add TENANT HOST --force` | `wherry operator revoke --tenant TENANT --host HOST` |
| `wherry operator tenant pool add TENANT POOL` | `wherry operator allow --tenant TENANT --pool POOL` |
| `wherry operator tenant pool remove TENANT POOL` | `wherry operator deny --tenant TENANT --pool POOL` |
| `wherry operator tenant remove TENANT` | `wherry operator remove --tenant TENANT` |
| `wherry operator tenant host remove TENANT HOST [--wait D]` | `wherry operator remove --host HOST --tenant TENANT [--timeout D]` |
| `wherry operator receiver remove RECEIVER [--wait D]` | `wherry operator remove --receiver RECEIVER [--timeout D]` |
| `wherry operator pool suspend POOL` | `wherry operator suspend --pool POOL` |
| `wherry operator pool resume POOL` | `wherry operator resume --pool POOL` |
| `wherry operator pool rotate-key POOL [--suspect-key KID]...` | `wherry operator rotate --pool POOL [--suspect-key KID]...` |
| `wherry operator tenant storage TENANT [-]` | `wherry operator storage --tenant TENANT [-]` |
| `wherry operator tenant host refresh TENANT HOST` | `wherry operator rotate --tenant TENANT --host HOST --storage` |
| `wherry host key rotate` | `wherry host rotate --key` |
| `wherry receiver run --rotate-key` | `wherry receiver rotate --key`, a one-shot run while the receiver is stopped |
| `wherry host logs RUN_ID -f` | `wherry host logs RUN_ID --follow` |

[`wherry receiver rotate --key`](commands.md#wherry-receiver-rotate) is a
one-shot: it runs while the receiver is stopped, takes the lock `run` holds on
the state directory, and refuses a receiver that has never started.
`receiver run` no longer rotates the key at its start;
[Rotate a receiver key](operations-receivers.md#rotate-a-receiver-key) gives
the procedure. The operator's trace span names keep the old command names:
readers of the trace topics, the [operator view](operator-view.md) among them,
match on them.

## Host credential rotation

A host now changes its Kafka password by a request the operator approves
([Approve a host's credential request](operations-operator.md#approve-a-host-s-credential-request)),
and `wherry operator revoke --tenant TENANT --host HOST` replaces
`add --host --force`, which is gone: `add --host` of an existing host fails
with `host exists`. The operator records the new state at a new record
version, which an older wherry cannot read: a host or receiver not yet
upgraded stops at the first such record with `upgrade this wherry`. Upgrade in
this order:

1. Upgrade every host and every receiver, as in [Upgrade wherry](#upgrade-wherry).
2. Upgrade the operator's binary.
3. Run `wherry operator init` again with the Kafka super user. It creates
   `wherry.requests` and grants `wherry.operator` `Delete` on it; until then
   `wherry operator check` fails on them.
4. For each host, run `wherry operator revoke --tenant TENANT --host HOST`,
   hand over the new bundle, and have the host's user run
   `wherry host init BUNDLE`, which files a credential request. Approve it.

A host enrolled before this release has no credential ID until step 4:
`wherry host rotate --scram` refuses it with `no credential ID is recorded for
this host; ask the operator to revoke and re-enrol it`, and
`wherry operator list requests` and `wherry operator check` report it as
`tenant=<tenant> host=<host> has no credential ID; revoke and re-enrol it`. Each host is offline from
its `revoke` until its user installs the new bundle.

## Collected outputs and non-git continue

A clean break, as every pre-1.0 change ([ADR-0032](design.md)); no
compatibility path remains.

- `result` records every verified output, not the `changes.bundle` alone, in
  the run's local record, merged by name with the outputs an earlier `result`
  recorded, and fails with `record outputs` when it cannot, after it has
  written and listed the outputs.
- A run record an earlier release wrote with a collected bundle, one holding
  a `bundle` member, no longer decodes. For such a run, `status`, `events`,
  `logs`, `cancel` and `result`, and the MCP tools of the same names, fail
  with
  `decode <state_dir>/runs/<run>.json: json: cannot unmarshal JSON string into Go client.runRecord: unknown object member name "bundle"`,
  and `submit --continue RUN` and the MCP `continue` with
  `continue: the local records of run <run>'s chain cannot be read`. `runs`
  still lists the run from the tenant feed and counts its record in
  `skipped N unverifiable records`. Every other run and the host itself keep
  working. The remedy: delete `<state_dir>/runs/<run>.json`, or edit the
  file and remove its `bundle` member, for each such run; or reset the
  deployment. The outputs on disk, the bundle included, are unaffected, and a
  run whose record is deleted can still be read from the feed but not
  continued.
- `submit --continue` and the MCP `continue` take a run without a git input,
  delivering its collected outputs as `previous/<name>`. No input of the
  caller's is named under `previous/`, on any run.
- `submit --input` and the MCP `inputs` take a directory, delivering its
  files as `<name>/<relative path>`, hidden entries skipped.

## Nested input names and plugin protocol 13

A clean break, as every pre-1.0 change ([ADR-0032](design.md)); no
compatibility path remains.

- An input file's name may be a relative path of portable segments joined by
  `/`, as an output's name is, so inputs keep their directory structure. No
  input name is a directory of another, among input files, S3 inputs and
  `known_hosts`. `previous/` is reserved for a continued run's outputs.
- The plugin protocol is 13. Receivers and plugins upgrade together: a
  protocol-12 plugin is refused at launch, and a protocol-13 plugin refuses a
  protocol-12 receiver. Use plugins built for protocol 13.
- Upgrade receivers before hosts submit nested names: an older receiver
  refuses such a task without publishing a rejection, so the run waits until
  its `task_started_by` passes unclaimed.

## Broker trust

A clean break, as every pre-1.0 change ([ADR-0032](design.md)); no
compatibility path remains. wherry distributes no CA: bundles carry no `ca`,
and the `ca_file` of a host, receiver or operator configuration is now
`ca_cert`, holding the CA's PEM text itself, beside the machine's
trust store ([Broker and S3 trust](security.md#broker-and-s3-trust)). The new
binary refuses a bundle carrying `ca` and a configuration keeping `ca_file`,
naming the key. Upgrade the operator first: an older operator with a
`ca_file` writes `ca` back into every bundle it issues.

1. On the operator's machine, install the binary and replace `ca_file` in
   `operator.yaml` with `ca_cert` holding the PEM.
2. Reissue every bundle: `wherry operator revoke --tenant TENANT --host
   HOST` for each host and `wherry operator apply --receiver
   RECEIVER --force` for each receiver. Each holder is locked out from its
   reissue until it installs its new bundle.
3. For each holder: stop it, install the binary and the new bundle, replace
   `ca_file` in its configuration with `ca_cert` holding the PEM, start it.
   A host deletes `ca_file` from its `config.yaml` first, since `host init`
   refuses a configuration holding it, then runs `wherry host init BUNDLE
   --ca-cert @ca.pem`, which writes `ca_cert`, and restarts the harness
   sessions that run `wherry host mcp`.

Or reset the deployment and provision it again.

## `operator init` reads the password as `@FILE` or `@-`

`--super-user-password-file FILE|-` is now `--super-user-password @FILE|@-`,
the form every flag that takes a value reads. A password as the flag's own
text is refused, since a command line is visible to other users. Scripts
that run `operator init` change the flag; the prompt, when the flag is
absent on a terminal, is as before.

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
  operator wrote, and `apply --pool` records it as before. Renaming is needed
  only to adopt catalog expansion, under which `wherry operator apply --pool`
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

A receiver's profiles name their agent, and `apply --pool` checks every live
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
   wherry operator apply --pool build
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
   wherry operator apply --receiver nomad-1 --file nomad-1 --dry-run
   wherry operator apply --receiver nomad-1 --file nomad-1
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
   wherry operator apply --pool build --dry-run
   wherry operator apply --pool build
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
  `wherry operator apply --pool`.
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
it, and `status`, `result` and resubmission of the run fail. Let runs finish,
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
`status`, `result` and resubmission of the run fail on it. Upgrade every wherry
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
  collected `changes.bundle`: `status`, `result` and resubmission of the run
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
`wherry.traces` ([Tracing](tracing.md)). `add --tenant` now creates the
tenant's topic, and `wherry.operator` needs `Describe` on the pool groups for
the operator view's share-group state. An existing install gets the topics of
its tenants and the grant by running `init` again:

1. Upgrade the operator's binary.
2. Run `wherry operator init` with the Kafka super user, as at setup. It
   grants `wherry.operator` `Describe` on the group prefix `p-`, creates
   `wherry.traces` and the trace topic of every live tenant where missing,
   sets their configuration, and grants hosts and receivers their rows on them.
3. `init` rotates `wherry.operator`'s password, as every re-run does: every
   other copy of the operator's working directory must take the new
   `operator.password` before its commands run again.

`wherry operator check` is the signal that the step is due: on an install
that predates it, check reports
`missing DESCRIBE ACL on p- for wherry.operator` or
`trace topic wherry.traces is missing; operator init creates it`, or a missing
`WRITE` ACL on `wherry.traces` for a receiver. Until the
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
3. Submit a task and collect its result with `wherry host result RUN_ID`.

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
   wherry operator remove --host HOST --tenant TENANT
   ```

   For this guide's host, that is `wherry operator remove --host laptop --tenant acme`.
   [`remove --host`](commands.md#wherry-operator-remove) runs the host's revocation steps.

2. After it succeeds, delete `~/.config/wherry/` on the laptop.
   It holds `config.yaml`, `bundle.yaml` and `host.key`.
3. Delete `~/claude-oauth-token` on the laptop when Set up a host made it and nothing else uses it.
4. On the operator machine, delete `bundles/hosts/<tenant>/<host>.bundle`.
   Host removal does not delete it.

Removing the host does not revoke the Claude token.

Check that it worked: `wherry operator list tenants` no longer lists the host under its tenant.

## Outputs are required, `--git` returns a bundle, inputs go to S3 by size

A clean break, as every pre-1.0 change ([ADR-0032](design.md)); no
compatibility path remains.

- Every declared output is required. `--output NAME:required` and the MCP
  output member `required` are refused with `outputs are always required;
  drop :required`; drop the suffix and the member. The wire field
  `outputs[].required` is gone, and a receiver fails a run that leaves any
  declared output unwritten. The exception is the changes file of a git
  input, `changes.bundle` or `changes.patch`, which a run that commits
  nothing leaves unwritten.
- Every completed run ends with a reply: a `TASK_STATE_COMPLETED` outcome
  file with no text and no data part fails the run with `agent returned no
  reply`. Agents that answered only in files must put a reply in the message.
- `--git` returns `changes.bundle` by default; `--git-changes patch` returns
  a patch and `--git-changes none` nothing. A submit that relied on `--git`
  alone returning nothing must say `--git-changes none`.
- `--input FILE:s3` and the MCP input member `storage` are refused with
  `inputs move to S3 by size; drop :s3`; an input over the inline limits
  goes through the pool's bucket by itself, as it did already.
- An output's route is chosen by size: within the pool's `output_limit` it
  returns on the feed, over it through the pool's bucket, under one S3 POST
  policy per run that the host puts in the envelope. `--output NAME:s3` and
  the MCP output member `storage` are refused with `outputs move to S3 by
  size; drop :s3`; drop the suffix and the member, and a wire `outputs[]`
  entry with `storage` no longer decodes. The envelope's `s3_outputs` is
  gone and `s3_policy` takes its place, so a host and its receivers upgrade
  together, and a run submitted before the upgrade is collected by the old
  release or resubmitted. Every submit on a pool with storage now resolves
  the host's storage credential; one that fails stops the submit. The host's
  local run records drop `s3_outputs` for `policy`: `result` on a run this
  host submitted before the upgrade that declared an `:s3` output fails to
  read its record, so collect such runs first.
- The upload policy lasts ten minutes past `task_ended_by`, so a run
  stopped at the bound or by a cancel still uploads. `apply --pool` now
  refuses a `start_by_limit` that with six hours and ten minutes added
  exceeds seven days; a pool at the old bound needs ten minutes less.
- The policy covers the run's whole output prefix, so set a quota and a
  lifecycle rule on each pool bucket
  ([Operations](operations-operator.md#add-tenants-and-hosts)).

Upgrade hosts and receivers together. A host of this release against an
older receiver gets its bundle, since the receiver reads the same expected
output and requires none; an older host against a receiver of this release
has every declared output required. Scripts and skills that use the dropped
suffixes fail at submit until they drop them; the wherry-plugins end-to-end
scripts are updated with their next release.

## `wait` is now `result`

The host command `wherry wait` is now `wherry result`, also under
`wherry host`. There is no alias. Update scripts and skills that call it.
The MCP tool is now `result`, and submit's `wait` response member is
`command`. The option that lengthens a removal's wait is now `--timeout` ([Command grammar](#command-grammar)).

For a short run, `wherry submit --wait` submits and collects the result in
one command. MCP submit accepts `wait: true` and returns the reply under
`result`, or a sanitised `error` after a successful publish.
