# Commands

[Key concepts](concepts.md) explains the model. The
[wherry A2A extension](a2a-extension-v1.md) specifies the records.
[Configuration](configuration.md) lists the files the commands read, and
[MCP tools](mcp.md) the tools `wherry host mcp` serves.

## Command tree

Every command but `version` and the shortcuts runs under its role's word:

| Role | Commands |
| --- | --- |
| Operator | `wherry operator init`; `wherry operator pool apply`, `entitle`, `suspend`, `resume` and `rotate-key`; `wherry operator host add` and `remove`; `wherry operator receiver apply`, `show` and `remove`; `wherry operator check` |
| Receiver | `wherry receiver run` |
| Host user | `wherry host init`, `key rotate`, `receivers`, `mcp`, `submit`, `wait`, `status`, `events`, `logs` and `cancel` |
| Developer | `wherry dev init`, `up` and `down` |
| None | `wherry version` |

`wherry submit`, `wait`, `status`, `events`, `logs` and `cancel` are
[shortcuts](#shortcuts) for the `wherry host` commands of the same name.
`wherry --help` and `wherry -h` print the tree and list the shortcuts.

## Conventions

- The [MCP tools](mcp.md)' results are the machine interface, and their
  shape freezes at 1.0. The CLI's text output may change. `wherry host receivers
  --json` is the one CLI JSON form, and it prints the objects the MCP
  `receivers` tool returns.
- `wherry operator`, `wherry dev`, `wherry receiver run` and `wherry version` read no client
  configuration. Every `wherry host` command reads it first, and every one except `host mcp`
  then connects to Kafka.
- Every `wherry host` command takes `--config-file FILE`. Without it, the
  command reads the file `WHERRY_CONFIG` names, else
  `~/.config/wherry/config.yaml` ([paths](configuration.md#client-configuration)).
  A leading `~/` is expanded.
- A command's flags may come before or after its `RUN_ID`.
- A failed command prints `wherry: <error>` on standard error and exits
  with status 1. `wait` has its own [exit codes](#exit-codes).
- Durations use Go syntax, such as `90s`, `45m` or `720h`, and must be
  positive. `host remove` and `receiver remove` refuse a `--wait` of zero or
  less before they revoke anything. They compute the minimum wait from the
  broker's timeouts; `--wait` can lengthen it but not shorten it.
- `cancel`, `wait`, `status`, `events` and `logs` print
  `skipped N unverifiable records` on standard error when N records of the
  run fail verification.
- Standard output carries a command's result and standard error its
  errors and diagnostics, so `wherry wait ID >/dev/null` keeps only the
  diagnostics and the exit status.
- `wait` and `status` print each redaction marker of a run this host
  submitted as `[redacted:<name>]`, naming the credential or S3 object,
  and `<name> redacted N times` on standard error for each name whose
  markers they printed. A host maps a marker with the run's redaction key
  in `state_dir`, and prints a marker as it is when the run's record holds
  no 32-byte key.

### Names

Tenant, host, receiver, pool, plugin profile, agent and credential names, and plugins, match
`[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?`: 1 to 32 lowercase letters, digits and
hyphens, starting and ending with a letter or digit. Names contain no `.`,
so every principal, topic, group and transactional ID built from them parses
back to one set of names.

Run IDs match `[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`. The client generates
`run-` followed by 32 hex digits.

A key ID is the RFC 7638 SHA-256 thumbprint of the public key's JWK,
unpadded base64url.

wherry's own key files, the host key, `signing.key` and their `.next`
files, are private JWKs (RFC 7517) with mode 0600. The TLS keys `wherry dev`
writes for its Kafka broker are PEM, the format the broker reads.

### Labels

A label key is a name, or a prefix, `/` and a name:

- The name is 1 to 63 characters, letters, digits, `-`, `_` and `.`,
  starting and ending with a letter or digit.
- The prefix is a DNS subdomain of at most 253 characters: lowercase
  labels of letters, digits and `-`, joined by `.`.
- The prefix `wherry.io`, and any prefix ending in `.wherry.io`, is
  reserved.

A label value follows the name rule, so it is never empty.

### Selectors

A selector is a list of requirements, each naming one key once.

| Operator | Values | Holds when the label |
| --- | --- | --- |
| `In` | one or more | is present with one of the values |
| `NotIn` | one or more | is absent, or has none of the values |
| `Exists` | none | is present |
| `DoesNotExist` | none | is absent |

Values within a requirement are unique. An empty selector accepts every
plugin profile.

On the command line a selector is comma-separated:

| Term | Requirement |
| --- | --- |
| `key=v1\|v2` | `In` |
| `key!=v1\|v2` | `NotIn` |
| `key` | `Exists` |
| `!key` | `DoesNotExist` |

## Operator commands

Every `wherry operator` command works in a working directory, one per
installation: `--dir DIR`, the current directory by default. `operator init`
writes its `operator.yaml`; every other command reads it and connects as
`wherry.operator` alone, with the password in its `password_file`. A missing
`operator.yaml` fails with `<dir>/operator.yaml does not exist; run wherry
operator init`. [Working directory](configuration.md#operator-working-directory)
lists what the directory holds.

`wherry operator` writes every `wherry.operator` record as
`wherry.operator`, creating `wherry.operator` and `wherry.receivers` first if
they are missing and setting the [configuration](design.md#topics-1) of an
existing `wherry.receivers`. `pool apply --dry-run` creates neither, and reads
a missing `wherry.operator` as holding no records. Options parse as Go flags,
in the forms `--name value`, `--name=value` and `-name value`, and the last of
a repeated option wins. Options other than `--dry-run` and `--force` take a
value, and options can appear anywhere after `operator`. An option without a
value fails with `flag needs an argument: -<name>`, one no operator command
takes with `flag provided but not defined: -<name>`, and one the command does
not take with `unknown option --<name>`. `--suspect-key` is given once per key;
any other repeated option takes its last value. `pool apply`,
`pool entitle`, `pool suspend`, `pool resume`, `host add` and `receiver apply` then create every ACL the new
state calls for, re-read them, and fail with `ACL plan incomplete` when one
is missing. They then delete the `WRITE` ACLs on a suspended pool's tasks
topic that the pool's hosts hold unsuspended, each by a filter naming its
principal; another principal's ACL on the topic stays.

`pool apply` and `receiver apply` read the object's file: `-f PATH`, else
`pools/<pool>.yaml` or `receivers/<receiver>.yaml` in the working directory.
Either applies without a prompt. `-f PATH` names a file, or a directory,
which selects the directory form of the
[pool terms](configuration.md#pool-terms) or
[receiver configuration](configuration.md#receiver-configuration). With
neither, on a terminal, the command asks for the object's real choices, with
defaults shown, writes the file, then applies it; a refused apply leaves the
file to correct and apply again. With neither and no terminal it refuses,
naming the fields the file must give. `--dry-run` writes nothing to the
working directory: with neither file it refuses rather than prompt, and it
creates no bundle directory.

`host add` and `receiver apply` write bundles under the working directory's
`bundles/`: `bundles/hosts/<tenant>/<host>.bundle` and
`bundles/receivers/<receiver>.bundle`, mode 0600, in directories mode 0700. A
bundle holds the broker address, the principal and its password, and no CA.
`host add` refuses a host that exists unless given `--force`. `receiver apply` of a receiver that exists
updates its configuration. Without `--force`, a command never replaces a
bundle: before it changes anything in Kafka it creates the bundle file,
failing when a file is there, and writes the bundle into it at the end. A
command that fails leaves no bundle and no temporary file of its own, or, when
it cannot remove one, fails naming it.

`--force` reissues the identity of an existing host or receiver: a new SCRAM
password and a new bundle, written to a temporary file that is then renamed
over the bundle. It appends no record. Kafka holds one SCRAM credential per
user, so the old password stops working at once, and the host or receiver is
offline until its new bundle is in place. Use it to rotate a password, replace
a lost bundle or change a host's broker address. When writing, closing or
renaming the new bundle fails after the password changed, the command removes
its temporary file, leaves the old bundle, and fails saying that the old
bundle no longer works and that running it again with `--force` writes a new
one.

### `wherry operator init`

```sh
wherry operator init [--dir DIR] [--broker ADDR] [--ca-file PATH] \
  [--super-user NAME --super-user-password-file FILE]
```

Delegates provisioning from the Kafka super user to `wherry.operator`. On a
terminal it asks for each value the flags do not give: the broker address,
the CA file the broker's certificate chains to (blank for the system roots),
and the Kafka super user's name and password file. Without a terminal it
refuses any missing flag but `--ca-file`, naming them:
`operator init has no terminal to prompt on; give --broker, --super-user, --super-user-password-file`.

Connected as the super user, once, it:

1. writes a new random password for `wherry.operator` to a temporary file,
   mode 0600, in the working directory;
2. grants `wherry.operator` the [operator ACLs](security.md#the-operator-principal);
3. creates `wherry.operator` and `wherry.receivers` if they are missing;
4. sets the new password on `wherry.operator`, creating the principal or
   replacing its password;
5. renames the temporary file over `operator.password`.

It then writes `operator.yaml`: `broker`, `ca_file` as an absolute path
(omitted for the system roots) and `password_file: operator.password`, and
appends `operator.password` and `bundles/` to `.gitignore`, creating it if
absent and adding only lines it lacks. It closes the super-user connection;
nothing it writes names the super user, which no later command uses.

Running `init` again rotates `wherry.operator`'s password: Kafka holds one
SCRAM credential per user, so commands still running with the old password
fail. A failure between steps 4 and 5 leaves `operator.password` holding the
old password; running `init` again recovers. `init` always takes the super
user from its flags or prompts, never from `operator.yaml`.

### `wherry operator pool apply POOL`

```sh
wherry operator pool apply POOL [-f PATH] [--dry-run]
```

Creates the pool, or records new terms for it, from the
[pool terms](configuration.md#pool-terms) at `PATH` or in
`pools/<pool>.yaml`. The [guided path](#operator-commands) asks for the
agents, each one's models and credentials (`NAME=ENV_VAR` or
`NAME=file:FILE_NAME`), the default agent, `producer_byte_rate` (default
1048576), `request_percentage` (default 10) and `start_by_limit` (default
`72h0m0s`).

With no pool of that name, it records the terms, creates the pool's tasks
and events topics, and sets the configuration of share group `p-<pool>`:
`share.auto.offset.reset=earliest`, `share.isolation.level=read_committed`,
`share.delivery.count.limit` and `share.record.lock.duration.ms` from the
terms' `delivery_limit` and `lock_duration`, 5 and `15s` when the terms omit
them. It refuses, before it reads the broker, terms whose agent credentials
break the [pool terms](configuration.md#pool-terms) rules, and a delivery limit above the lowest
`group.share.max.delivery.count.limit` across the brokers, and terms whose
[records do not fit the broker](configuration.md#record-limit). Before it
records anything it refuses a `lock_duration` above the brokers'
`group.share.max.record.lock.duration.ms`, naming the field and the ceiling.
Before it records the terms it refuses a pool
name that is not valid and an existing tasks or events topic with more than one
partition; it creates the topics after it records the terms, which is when it
checks the pool's names against every other recorded name. On new and
existing topics it sets the tasks `retention.ms` to
`start_by_limit` plus six hours plus one day unless it is already longer, and the
events `retention.ms` to the tasks topic's unless it is already longer, their
`message.timestamp.type` to `LogAppendTime`, their `cleanup.policy` to
`delete` and their `retention.bytes` to `-1`, refuses either with more than
one partition, and creates the progress topic
of each live receiver in the pool, or sets an existing one's configuration,
with the tasks topic's `retention.ms` unless the progress topic's is already longer. It fails when a topic
configuration it sets is not applied within 10 seconds, or when a describe of it
still fails then, naming the describe's error. It refuses a `start_by_limit` above
29 days and 18 hours, or one that with six hours added exceeds the
`retention.ms` of the feed or transcript topic of a tenant entitled to the
pool. For a pool with `storage` it refuses a `start_by_limit` that with six
hours added exceeds 7 days, the longest an S3 SigV4 presigned URL lasts,
with `start_by_limit plus the 6h run cap exceeds 7 days, the longest an S3
SigV4 presigned URL lasts`. Before it measures the records, it sets `max.message.bytes` to 2 MiB
(2097152) on the feed and transcript topic of every tenant with a host or an
entitlement. See
[Topics](design.md#topics-1).

With a pool of that name and other terms, it records the new terms, makes
the checks and settings above, and sets the share group's `share.delivery.count.limit` and
`share.record.lock.duration.ms` from the new terms. It first checks the recorded
configuration of every receiver in the pool whose membership has not ended
against the new terms, as `receiver apply` checks a new receiver, and refuses
the terms when a plugin profile no longer fits, naming each receiver, plugin
and profile and the reason. It sets the pool's topics before it records the terms, so a failure leaves the
old terms in force, and never lowers their `retention.ms`: runs submitted
under a longer `start_by_limit` keep their records. Receivers apply the new terms at their next read
of `wherry.operator`, within 15 seconds, without a restart. A lowered
`delivery_limit` applies to a task in flight at its next release.

With a pool of that name and the terms recorded, it completes the pool,
creating any missing topics, as after an `apply` that failed once it
recorded them. When the tasks topic's
[record limit](configuration.md#record-limit) has changed since, it records
the terms again with the new limit.

With `--dry-run`, it makes the same checks, among them the stranded
receivers, the lock duration ceiling, the record limit and the partitions of
an existing tasks or events topic, prints nothing, and creates and writes
nothing in Kafka; it measures a tenant topic below 2 MiB as `apply` would
set it. Terms it refuses fail in the same way with or without
`--dry-run`.

### `wherry operator pool entitle POOL --tenant TENANT`

Entitles the tenant to an existing pool, creates the tenant feed and
transcript topic with `max.message.bytes` 2 MiB, or sets existing ones to
it, and grants the tenant's hosts and the pool's receivers
their ACLs. It refuses when the pool's records do not fit the
[record limit](configuration.md#record-limit) with the tenant's feed and
transcript topic counted, or when the `retention.ms` of either is below the
pool's `start_by_limit` plus six hours, and records nothing. It applies
each host's [quotas](configuration.md#quotas).

### `wherry operator pool suspend POOL`

Records a `suspend` record for the pool on `wherry.operator` and reconciles the
ACLs. A suspended pool's tenants' hosts hold no `WRITE` on its tasks topic.
The receivers keep `READ` on it, and every other ACL stays. A host refuses a
submission to the pool with `pool POOL is suspended; submit again once it is
resumed` before it resolves any
credential or uploads any input. Receivers fetch nothing from the pool's
share group, so they claim no task. The key custodian refuses the open
request of a run whose job has not started with `pool suspended`; the
claimant publishes `released` and releases the task, which waits in the
queue. Open requests of runs adopting a started job are served. Runs
already holding a claim renew it and continue to their terminal status, and
their tasks are acknowledged as on an unsuspended pool. A suspension does
not stop a running job or take back the credentials it holds: rotating them
at their issuers does, and
[`wherry operator receiver remove`](#wherry-operator-receiver-remove-receiver)
stops a receiver. A key custodian that drains while the pool is
suspended does not scan the task topic or re-key any task. It keeps its
pool keys to their deadlines and logs
`custodian: hand over to <receiver>: pool <pool> is suspended; its re-key scan waits for resume; keeping the pool keys to their deadlines`.
The key custodian keeps rotating.

The key custodian reads `wherry.operator` once at the start of each pass over
the open requests and decides every request of the pass by that read. A
pass begins once every attempt wrap of the previous pass committed. The
first pass that reads the `suspend` record republishes the custodian's
snapshot with `suspended_for` set to the record's `wherry.operator` offset,
before it decides any request. The command returns once the key custodian
has acknowledged: it waits up to 5 minutes for a snapshot whose
`suspended_for` is at or above its record's offset, writing
`waiting for pool <pool>'s key custodian to acknowledge the suspend record at wherry.operator offset <offset>, up to <time>`
to standard error every 10 seconds, then prints
`pool <pool> is suspended: its key custodian acknowledged the suspend record at wherry.operator offset <offset>; no new start opens`.
After that no new start opens. With no acknowledgement it fails with
`pool <pool>'s suspend record is written at wherry.operator offset <offset>, but its key custodian did not acknowledge it within 5m0s; it is not yet in effect`.
When the pool has no snapshot it returns at once with
`pool <pool>'s suspend record is written at wherry.operator offset <offset>; the pool has no key custodian yet, so nothing opens until one reads it`.
A custodian whose advertisement has gone stale is still awaited, since it can still open requests.

### `wherry operator pool resume POOL`

Records a `resume` record for the pool on `wherry.operator` and reconciles the
ACLs, granting the hosts' `WRITE` on the tasks topic again. Receivers claim
from the pool again at their next refresh of `wherry.operator`. The key
custodian serves first starts from the pass that acknowledges the record
with `resumed_for` in its snapshot, and the command waits for that
acknowledgement as `pool suspend` does, printing
`pool <pool> is resumed: its key custodian acknowledged the resume record at wherry.operator offset <offset>; first starts open again`.

### `wherry operator pool rotate-key POOL [--suspect-key KID]...`

Refuses a pool that is not suspended with `pool <pool> is not suspended; run
wherry operator pool suspend <pool> first`. Otherwise records a `rotate-key`
record for the pool on `wherry.operator`. The pool's key custodian publishes a
`rotation` snapshot with a new key on its first pass after it reads a
`rotate-key` record whose `wherry.operator` offset is above the `rotated_for`
its current snapshot carries, as at its daily rotation. A snapshot with a
new key carries as `rotated_for` the offset of the latest `rotate-key`
record its custodian read, and every later snapshot carries it on; append
times do not decide it. Hosts encrypt to the new key once they read that
snapshot.

The command waits up to 5 minutes for a snapshot whose `rotated_for` is at
or above its record's offset, writing
`waiting for pool <pool>'s key custodian to rotate for the request at wherry.operator offset <offset>, up to <time>`
to standard error every 10 seconds. With none, it fails with
`pool <pool>'s key custodian published no snapshot rotated for the rotate-key request at wherry.operator offset <offset> within 5m0s; the request stands`.
It then captures the end offset of the pool's tasks topic, reads the topic
as `wherry.operator` through that offset, and lists every retained task
record whose submit wrap's `kid` is one of the suspect keys. Without
`--suspect-key` the suspect key is the key the current snapshot names when
the command starts; the command refuses when the pool has no snapshot or its
current snapshot does not verify. A task record is listed whether or not its
signature verifies. The output names runs, tenants and credentials, and
holds no credential value:

```
pool <pool>'s key custodian rotated for the rotate-key request; tasks appended through that rotation are listed
suspect_keys=<kid>[,<kid>...]
horizon partition=<n> oldest_retained_offset=<offset>
horizon partition=<n>: retention deleted the tasks at offsets <offset> to <offset> during the scan; they may not be listed
horizon: the oldest retained task was appended at <time>, <age> ago; tasks deleted by retention before it are not listed
task run=<run> tenant=<tenant> key=<kid> credentials=<name>[,<name>...] appended=<time>
tasks=<n>
```

Each partition's horizon is its log start listed after the scan, since a
fetch below the log start begins at the start then in effect. Where
retention moved the start during the scan, the second `horizon` line names
the offsets it deleted, from the start listed before the scan.

The credential names are the task's own credentials and every credential
any terms record of the pool on `wherry.operator` gives the task's agent,
other than the task's `unsupplied_credentials`. The
list can name a credential the envelope did not hold; it never omits one it
did, so rotating every listed credential covers the exposure.

### `wherry operator host add HOST --tenant TENANT`

```sh
wherry operator host add HOST --tenant TENANT [--force]
```

Records the host, creates its principal, its key topic and the tenant
feed and transcript topic, the latter two with `max.message.bytes` 2 MiB, grants its ACLs, applies its quotas, and writes
its bundle to `bundles/hosts/<tenant>/<host>.bundle`, naming the broker in
`operator.yaml`. It refuses a host that exists, and an existing bundle file,
unless given [`--force`](#operator-commands). For a host that exists,
`--force` reissues its identity alone: it records nothing, and sets no topic,
ACL or quota. A host whose membership ended is refused. Host names are unique
within a tenant; hosts of the same name in other tenants have bundles of
their own.

### `wherry operator receiver apply RECEIVER`

```sh
wherry operator receiver apply RECEIVER [-f PATH] [--dry-run]
wherry operator receiver apply RECEIVER --force
```

Reads the [receiver configuration](configuration.md#receiver-configuration)
from `PATH` or `receivers/<receiver>.yaml`, a directory or a single file, and
validates it against the pool's terms. Its `pool` names the receiver's pool;
its `plugin_sha256` pins, for each plugin type, the SHA-256 of that type's
binary as lowercase hex: the line for the binary in the plugin release's
signed `checksums.txt` ([Install plugins](operations.md#install-plugins)).
Every plugin type in the configuration needs a pin, and a pin that is not 64
lowercase hex digits is an error; either refusal writes nothing. Its
`broker` is the address the receiver's bundle names, the broker in
`operator.yaml` when it is empty. The [guided path](#operator-commands) asks
for the pool, the plugin type (default `nomad`), its pin and its `config` as
a YAML flow mapping, the agents (one plugin profile each, named after its
agent) with their labels, the capacity (default 1) and the broker address
(default the operator's).

With no receiver of that name, it records the configuration, creates the
receiver's principal, key topic and progress topic,
`wherry.receiver.<receiver>.progress`, grants its ACLs, and writes its
bundle to `bundles/receivers/<receiver>.bundle`. The file must name a
`pool`. It creates the progress topic of every other live receiver in the
pool that lacks one, and sets the
[progress topic configuration](design.md#topics-1) on each existing one, never lowering its `retention.ms`.
The bundle holds the receiver's Kafka identity alone; the receiver's [local
configuration](configuration.md#receiver-local-configuration) gives the
sources of its [platform credentials](configuration.md#platform-credentials)
and the CA it trusts the broker with.
With `--dry-run`, it makes the same checks and prints each plugin profile as
the record would hold it, with the pins, in the format of `receiver show`, and creates and
writes nothing.

With a receiver of that name, it records the new configuration in the
receiver's pool. The file's `pool` may be empty and otherwise must name that
pool. The record holds only the pins the file gives, so give one for every
plugin type. The principal and password stay as they are. When the file's
broker address differs from the one in the receiver's bundle in
`bundles/receivers/`, it rewrites the bundle with the new address and the
same password. Without that bundle it records nothing and fails, naming the
missing bundle and that `--force` gives the receiver a new password and
bundle. When the receiver reads a record that differs
from the configuration it runs, it stops claiming, lets each run it holds
end and commit with no time limit, then exits, and its platform restarts it.
To apply a change at once, restart the receiver through its platform, as
with `kubectl rollout restart`, `nomad alloc restart` or `systemctl
restart`: it stops within its `drain_timeout` and its held runs are
released to other receivers.

With `--dry-run`, it reads and validates the configuration as it does to
record it, against the receiver's pool's terms, and prints the resolution
in the format of [`receiver show`](#wherry-operator-receiver-show-receiver),
writing nothing. A configuration it refuses fails in the same way
with or without `--dry-run`.

With `--force`, it [reissues](#operator-commands) an existing receiver's
identity and writes its new bundle, naming the broker of
`receivers/<receiver>.yaml` when that file names one, else that of the
receiver's existing bundle, else that of `operator.yaml`. It does not change the
receiver's configuration record, and takes no `-f` or `--dry-run`.

### `wherry operator receiver show RECEIVER`

Prints the receiver's recorded configuration as YAML: `capacity`, then
`plugin_sha256`, the SHA-256 pinned for each plugin type, then
`profiles`, each plugin profile resolved, in routing order, with its
`name`, `plugin`, `agent`, `labels` and merged `config`
([Plugin profile merge](configuration.md#plugin-profile-merge)):

```yaml
capacity: 3
plugin_sha256:
    nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
profiles:
    - name: claude
      plugin: nomad
      agent: claude
      labels:
        platform: nomad
      config:
        address: https://nomad.example.com:4646
        credential: nomad-token
        runtime:
            config:
                image: registry.example.com/claude-agent:1.0
            driver: docker
```

A `credential` key shows the credential's name, which is all the record
holds; neither `show` nor `--dry-run` prints a credential value.

### `wherry operator host remove HOST --tenant TENANT`

Runs the [revocation](concepts.md#revocation) steps for the host, as
[`receiver remove`](#wherry-operator-receiver-remove-receiver) does for a
receiver.

### `wherry operator receiver remove RECEIVER`

Runs the [revocation](concepts.md#revocation) steps. When it ends, it
prints one line per step it ran:

```
step=<n> complete=<bool> name=<step name>
```

The line of a failed step ends in ` error=<cause>`. It stops at the first
incomplete step and fails with `revocation incomplete`. The probe asks every
broker for the metadata of the principal's own key topic, and step 3
completes when every broker answers `TOPIC_AUTHORIZATION_FAILED`. While a
broker still authorises the principal, step 3 probes again every second for
up to 2 minutes. It fails at once when the login fails or the probe returns
any other error. Step 5 waits the largest of
`connections.max.reauth.ms`, `group.share.max.record.lock.duration.ms` and
`transaction.max.timeout.ms` across the brokers, and fails when one of them
is unbounded. `--wait DURATION` lengthens that wait and must be positive;
step 5 refuses a duration below it and names the minimum.

### `wherry operator check`

Loads `wherry.operator`, checks that no two names collide, validates every
receiver configuration against its pool's terms, checks that every pool's
[records still fit the broker](configuration.md#record-limit), checks that every
tenant feed and transcript topic has a `max.message.bytes` of at least 2 MiB, checks topic retention and
timestamp type against the [bounds](design.md#topics-1), checks that each
pool's tasks and events topics have one partition, `cleanup.policy=delete`
and `retention.bytes=-1`, checks that each
live receiver's progress topic keeps records at least as long as its pool's
tasks topic, and checks that every ACL the
state calls for is present, with `wherry.operator`'s own
[grants](security.md#the-operator-principal) but the four Kafka implies by
the others. It does not report ACLs beyond those, and does not check a
plugin profile's plugin or `config` section.

## Receiver command

### `wherry receiver run`

```sh
wherry receiver run [--config-file FILE] [--rotate-key]
```

Runs a receiver as the principal of the bundle its [local
configuration](configuration.md#receiver-local-configuration) names,
trusting the broker through the configuration's `ca_file` or, without one,
the system roots.
`--config-file` defaults to `/etc/wherry/config.yaml`. It creates the
configuration's `state_dir` with mode 0700 and keeps `signing.key`, a
private JWK with mode 0600, in it, and in `pool-keys/` one file with mode
0600 per pool key it holds: the private JWK, its creation time, the append
time of the committed snapshot first read back naming it current, and its
deadline.
On start it:

1. settles a leftover `signing.key.next` as a host command settles
   [`<host_key>.next`](#wherry-host-key-rotate);
2. registers its public key on its key topic when the topic is empty, and
   fails when the topic holds another key;
3. with `--rotate-key`, replaces `signing.key` as
   [`wherry host key rotate`](#wherry-host-key-rotate) replaces the host key, through
   `signing.key.next`, and logs `rotated key: old=<id> new=<id>`;
4. waits for its configuration record on `wherry.operator` and validates it
   against the pool's terms;
5. for each plugin profile in turn, resolves its [platform
   credential](configuration.md#platform-credentials), starts its
   [plugin](#plugins) from `plugin_dir` when no earlier profile has, and
   configures the profile;
6. reads its pool's events once, and starts advertising and claiming;
7. joins the pool's custodian group, `p-<pool>.custodian`, on
   `wherry.pool.<pool>.custodian` with its receiver ID as the static
   instance ID, and acts as the pool's key custodian while the group
   assigns it the topic's partition. A receiver whose membership ended
   leaves the group and never acts as custodian.

The key custodian writes every snapshot through the transactional ID
`p.<pool>.custodian`, one transaction per snapshot, and reads the committed
snapshot back before it marks the key published. When assigned, a receiver
that the current snapshot names and that holds the snapshot's key continues
with that key; any other generates a P-256 key, stores it, and publishes it
with reason `bootstrap` when the topic holds no snapshot and `takeover`
otherwise: after a drain, a crash, or the same receiver returning without
its keys. A takeover logs `custodian: took over pool <pool>
(WherryCustodianTakeover)` and counts in `wherry_custodian_takeovers_total`
with reason `takeover` once, when its snapshot is first read back, also on a
later pass when the commit landed but its read-back failed. The custodian rotates the key at the
append time of the snapshot first read back naming it plus 24 hours plus a
jitter under one hour derived from the pool and key ID, publishing a
`rotation` snapshot, at once on a [`rotate-key`](#wherry-operator-pool-rotate-key-pool-suspect-key-kid) record whose
`wherry.operator` offset is above the snapshot's `rotated_for`, and republishes its snapshot with the same reason after
its signing key rotates; a republish does not move the rotation. A commit
is attempted up to 3 times on the same producer, and a fenced or denied
commit is not retried; a commit with an
unknown outcome counts in `wherry_ambiguous_commits_total`. The custodian
initialises `p.<pool>.custodian` only when assigned. A custodian whose
producer is fenced while assigned logs `custodian: fenced while assigned in
pool <pool>; re-initialising to republish (WherryCustodianTakeover)`, counts
in `wherry_custodian_takeovers_total` with reason `contested`, initialises
`p.<pool>.custodian` again and republishes its snapshot with reason
`contested`, under a new key when the current snapshot names another
receiver. A second fence within the custodian group's 45-second session
timeout logs
`custodian: fenced again within the session timeout in pool <pool>; standing down until assigned again`. A receiver whose assignment
ends, that stood down, or whose failed write is followed by another
receiver's committed snapshot writes no more snapshots until it is assigned
again.

On `SIGINT`, `SIGTERM` or a configuration change, a receiver ends any custody
pass in progress at once, closes its custodian group client and removes its
static membership from the group, which hands the partition on at once. The key custodian with a
`drain_timeout` over `0s` logs `custodian: stepping down in pool <pool>;
waiting up to <timeout> for a successor` and after the removal waits until
its stop plus `drain_timeout` for another receiver's snapshot. Every step of the drain, the
re-key's reads, its transaction and the key deletion included,
ends within `drain_timeout` plus 5 seconds of the stop; a read or write that
bound ends keeps the keys. With a successor's snapshot, it
reads the task topic to its last stable offset and the pool's events, writes
in one transaction a re-key wrap to the successor's current key, once for each
task hash, for every task its keys open that is an authorised host's under its
run's Kafka key and derived run ID, whose bounds hold at its append time, that
is its run's task when the run has one, and that is not terminal, not past its
`task_ended_by` and not already re-keyed to that key, and then deletes its
pool keys. A read or write
error keeps the keys to their deadlines. A task appended under its key after
that read is not re-keyed: it fails key-lost before it starts, and its host
resubmits it before its `task_started_by`. While its pool is suspended it
does not scan the task topic and keeps its keys. With no successor's snapshot
by the deadline it logs `custodian: no successor in pool <pool>; keeping the pool
keys to their deadlines` and exits with its keys, and its restart continues
as custodian. A custodian with no other member to succeed it, in a
one-receiver pool, a scale to zero or a whole-pool stop, waits out
`drain_timeout`. With `drain_timeout` at `0s` it logs `custodian: leaving pool <pool> with no drain timeout;
keeping the pool keys to their deadlines` and exits with its keys; its
successor publishes a `takeover`.

The key custodian answers its pool's open requests, every 2 seconds, with an
attempt wrap or a refusal after the claim check
([Open request](a2a-extension-v1.md#open-request)), each with its claim
decision in one transaction through `p.<pool>.custodian`. A receiver that
holds pool keys but not the current key answers an open request for a task
it can open with a re-key wrap to the current key.

A replaced key's deadline is `start_by_limit` plus 7 hours after the append
time of the snapshot that replaced it, as the receiver's first custody pass
after the replacement reads it. A key replaced before the receiver started
counts from the receiver's first custody pass. A key the receiver generated
and has not yet read back in a committed snapshot is marked published and
kept current when the current snapshot names it. Any other such key's
deadline is `start_by_limit` plus 7 hours after the current snapshot's
append time. A custody pass reads the latest committed snapshot, also while
another writer's transaction is open on the snapshot topic; after a read
error the receiver keeps every key. It deletes a key at its deadline, and all of its
keys at the end of a drain that re-keyed every task they open. Every
other key stays in its state directory.

It logs its own lines to standard output with the prefix `receiver: `;
plugin and link lines are hclog lines named `plugin`. Plugin lines, the
plugin's raw standard error that go-plugin relays, and profile configure,
sweep and readiness errors carry the
[marker](security.md#credential-envelopes) of each credential value
delivered to a run the receiver holds, and of each [platform
credential](configuration.md#platform-credentials) value, in place of the
value. On
`SIGINT` or `SIGTERM` it stops claiming, closes its idle claims and lets
each held run go on for up to the local configuration's
[`drain_timeout`](configuration.md#drain-timeout-and-the-stop-window),
default `0s`. It then commits the pending transcript of each run still
held, releases their tasks and exits, leaving their jobs running. Those
commits, releases and acknowledgements and the key custodian's drain all end
within `drain_timeout` plus 5 seconds of the signal, so a stop takes at most
that; a run whose commit does not complete by then is left unacknowledged
for its acquisition lock to return. Set the
platform's stop window as in
[Drain timeout and the stop window](configuration.md#drain-timeout-and-the-stop-window).
When a
configuration record that differs from its own appears, it stops claiming,
lets each held run end with no time limit and fails with
`receiver configuration changed`. A configuration record whose plugin is not a
valid plugin name ends it with
`configuration: profiles need unique valid names and a valid plugin`.
It refuses to start after its membership end.

With `metrics_addr` set it serves its [metrics](#metrics) at `/metrics` on
that address, and fails at start when it cannot listen there. Without it
it serves none.

### Plugins

The receiver ships with no plugins. For each plugin in its configuration
it runs `wherry-plugin-<type>` from the plugin directory, one process per
plugin, and configures every plugin profile of that plugin in it.
[wherry-plugins](https://github.com/benemon/wherry-plugins) holds the
plugins, `nomad` among them.

The plugin directory is made absolute and `PATH` is never searched. The
binary may be a symbolic link; the receiver resolves it and runs the file it
names. When the receiver starts a plugin,
[go-plugin's `SecureConfig`](https://pkg.go.dev/github.com/hashicorp/go-plugin#SecureConfig)
checks the binary's SHA-256 against the pin the receiver's record holds for
its type (`plugin_sha256` in the file
[`receiver apply`](#wherry-operator-receiver-apply-receiver) reads). The receiver
refuses a plugin whose type has no pin, with
`plugin type <type> has no pinned SHA-256; set plugin_sha256.<type> in the receiver's file and run receiver apply`,
and a binary whose SHA-256 differs, exiting with
`profile <name>: plugin type <type>: <path> has SHA-256 <hex> and the pin is <hex>`.
A type with no binary in the directory is a configuration error, and the
receiver exits with `profile <name>: no plugin for type <type>`. A plugin checks its profile's
`config` section when the receiver calls its `Configure`, and the receiver exits with
`profile <name>: ...` on an error.

A plugin inherits the receiver's environment. The receiver passes each
profile's name, pool, agent and merged `config` section to the plugin, less
the `credential` key, and the value of the profile's [platform
credential](configuration.md#platform-credentials) when it names one. A
profile with a `credential` name its local configuration has no source for
fails its configuration with an error naming the profile and the
credential.

A plugin also reports, for each profile, the paths of the input, output and
credentials directories as its agent sees them, which the receiver writes
into the prompt, and implements `InlineLimit` to report the most inline input
bytes it delivers. Zero declares no profile limit. Dispatch carries a run's
input files and every delivered credential with the prompt, and a fetch for
each S3 input: its name, presigned URL,
size and SHA-256, which the plugin fetches and verifies before the agent
starts, reporting a job that never started with class `fetch` when it
cannot. A plugin that cannot deliver an input reports it, and the run ends
`TASK_STATE_FAILED` naming it. Once the job ends the receiver
stops it and asks the plugin to collect its outputs, which arrive over
the link in pieces of at most 1 MiB. The outputs the receiver names for
streaming, its S3 outputs, the plugin streams as the receiver reads them.

A plugin gives each agent the `git` environment that
[Git input](a2a-extension-v1.md#git-input) specifies, through
`pluginsdk.AgentEnvironment` and `pluginsdk.AgentUnset`.
[Git repositories](operations.md#git-repositories) states its limits.

The receiver and a plugin talk gRPC over
[go-plugin](https://github.com/hashicorp/go-plugin) with mutual TLS on a
certificate pair made for each start, plugin protocol version 10. The receiver refuses a
plugin of any other protocol version at its start, so the receiver and its
plugins upgrade together. Configure carries the profile's platform
credential, when it names one, and no agent credential. Dispatch carries
every credential of the run as a `pluginsdk.Secret` holding its name, its value
and exactly one target: `Env`, an environment variable the plugin sets for the
agent, or `File`, a file it writes in the agent's credentials directory.
The receiver refuses a version 9 plugin with
`incompatible API version with plugin. Plugin version: 9, Client versions: [10]`.
The receiver refuses a plugin that offers no TLS certificate. Every call names
its profile. A plugin accepts messages up to 64 MiB; the receiver logs a larger
Dispatch as `dispatch rejected: message too large`.

Each advertisement gives every profile's sweep and readiness call one
advertise interval, 30 seconds; a call that runs out counts as not ready
and is logged. Each offer carries the profile's declared inline input limit;
negative limits are invalid.

Each scrape of `/metrics` calls every profile's `Metrics` at once, each within
2 seconds. A plugin answers with samples of the `wherry_plugin_` families,
each a family name, labels, and a counter or gauge value or a histogram. A
histogram gives the cumulative count at each bound of
`pluginsdk.PlacementBuckets`, its sum and its count. The receiver drops a
sample, and counts it in `wherry_metrics_dropped_total` by reason, when:

- its family is not one of the four (`unknown_family`);
- it has a label key the family lacks (`unknown_label`);
- a label value is outside the family's set (`label_value`);
- its kind is not the family's (`kind`);
- its value is not finite, a counter is negative, or a histogram has other
  buckets (`value`);
- it repeats a series of the same answer (`duplicate`).

It sets every sample's `profile` label to the name of the profile it
answered for, so a plugin cannot report for another profile. A call that fails
or runs out is counted as `error` or `timeout`, and the scrape serves that
profile's samples from its last answer.

The receiver pings each plugin every 10 seconds and kills one that fails
the ping or does not answer within 5 seconds, logging
`plugin did not answer a ping; killing it`. Each Configure call has 30
seconds. When a plugin exits, or is killed, the link logs `plugin exited` as a
`plugin` line, and the receiver logs `stopping: plugin <type> exited` and
exits non-zero with `plugin <type> exited`, starting no other plugin process. It
stops claiming at once and leaves the runs it holds unacknowledged with their
jobs running; once the broker delivers their tasks again, the restarted
receiver or another member adopts each job and follows it from its last
offset.

A plugin's `Follow` may fail at any time; the receiver recovers the stream,
so a plugin need not. When `Follow` fails otherwise, the receiver asks
`Lookup` for the run's job and logs
`run <id>: follow failed (<class>); following again from offset <offset>`
once per run for each class. A running job (`job running`) it follows
again from the end of the last line it took, as it does a lost plugin link,
and a failed `Lookup` (`lookup failed`) it retries, a second apart, until the
run passes `task_ended_by`, when it ends with that bound. An ended job
(`job ended`) it follows once more from that line end: a plugin's `Follow` on
an ended job reads the rest of the log and returns its `Terminal`. A job that
never started is a `platform` start failure. Otherwise the run fails with
`log stream failed`.

A receiver with no ready profile advertises with no offers, so it stays
`live` with every profile `ready=false`, and placement passes over it. A
job its plugin reports never started is a `platform` start failure: the
receiver purges the job and returns the task to the pool. A job that never
started with class `fetch` instead ends the run `TASK_STATE_FAILED` with
`an S3 input was not fetched, or did not match its size and SHA-256`.

A plugin may report, with a job that never started, the platform's reason
in `Terminal.Reason`, which can quote the run's material. The receiver logs
`run <id>: job never started (<class>): <reason>`, or the line without the
reason when there is none. It replaces each delivered credential value in
the reason with its redaction marker, as it does in published output, and
each `http://` or `https://` URL with `[redacted-url]`, turns line breaks
into spaces and cuts the result to 512 bytes. For class `fetch` the line is
`run <id>: job never started: an S3 input was not fetched`, and a reason,
which can quote a presigned URL, is logged as `[omitted]`. The reason goes
to the receiver's log only.

A plugin returns a failed platform call as a `pluginsdk.Error`, which names
the operation (`Op`) and the platform's failure class (`Class`) beside the
error, and crosses the link as a gRPC status detail. The receiver logs a
plugin call's error as `op=<op> class=<class>: <text>`, without
`class=` when the plugin gives none, or as its text alone when the error
has no operation, under the rules for a
reason above. A run's calls (dispatch, lookup, follow, stop, collect, purge
and release) replace each credential value delivered to the run with its
redaction marker; an adopter's lookup comes before its envelope opens and has no
values yet. The pool's calls (sweep, readiness and configure) belong to no run and
replace each value delivered to a run the receiver holds and each platform
credential value. Every call's text has its URLs replaced, its line breaks
turned into spaces and is cut to 512 bytes. A profile's configure error ends
the receiver with `profile <name>: <text>`.

A receiver adopts a run's existing job only when the profile and agent
recorded in its platform metadata match the profile that finds it. It
leaves a job whose profile is another of its profiles to that profile, and
stops and purges
any other job under the run's ID and dispatches the run again.

The protocol is the Go module `github.com/benemon/wherry/pluginsdk` in this
repository. [`plugin.proto`](https://github.com/benemon/wherry/blob/main/pluginsdk/proto/wherry/plugin/v1/plugin.proto)
defines the service. `pluginsdk.Serve` runs a plugin and gives its factory
the logger whose JSON lines the receiver relays at their level. A plugin sets
`pluginsdk.Version` before `Serve`, and the receiver logs it when it starts
the plugin. `pluginsdktest` is the conformance suite a plugin runs against its
platform.

### Metrics

A receiver with `metrics_addr` set serves its metrics in the Prometheus
text format over plain HTTP. `GET /metrics` is the only route; every other
path answers 404, and a method other than GET or HEAD on `/metrics`
answers 405. The scrape holds the Go runtime (`go_`) and
process (`process_`) families, `wherry_build_info` with `version` and
`revision` and value 1, and the families below, each with the label `pool`.

Every label value comes from the receiver's configuration, its pool's terms and
entitlements, or a fixed list. No label or value carries a run ID, prompt,
output, error text, URL, image, path, node or credential name. An agent the
pool's terms do not name is `other`. `model` is the placement's model when the
agent's `models` in the terms hold it, `other` when they do not, and empty for
an agent with no `models`. A `state` outside the A2A task states is `other`.
A run ended without a plugin profile, one given back at the delivery budget
before any profile took it, has `profile="none"` in `wherry_runs_total` and
`wherry_delivery_budget_exhausted_total`.

| Family | Type | Labels | Counts |
| --- | --- | --- | --- |
| `wherry_claims_total` | counter | `tenant` | claimed tasks whose submission verifies |
| `wherry_admissions_total` | counter | `tenant`, `profile` | tasks admitted, by the profile they were routed to |
| `wherry_rejections_total` | counter | `tenant`, `reason` | rejections, by reason |
| `wherry_runs_total` | counter | `tenant`, `profile`, `agent`, `model`, `state` | runs ended `completed`, `failed` or `canceled`, except a run whose terminal's commit had an unknown outcome and committed, which no receiver counts |
| `wherry_delivery_budget_exhausted_total` | counter | `tenant`, `profile` | runs ended `failed` with `delivery budget exhausted` |
| `wherry_run_duration_seconds` | histogram | `tenant`, `profile`, `agent`, `model`, `state` | time from this attempt's claim to the run's terminal state; an adopted run counts the adopting attempt only, and a run whose terminal's commit had an unknown outcome and committed is not observed |
| `wherry_queue_wait_seconds` | histogram | `tenant`, `profile` | time from submission to first claim |
| `wherry_adoptions_total` | counter | `tenant`, `profile` | runs that adopted an earlier attempt's job |
| `wherry_redeliveries_total` | counter | `tenant`, `profile` | runs executed on a redelivery |
| `wherry_in_flight` | gauge | `profile` | tasks held |
| `wherry_capacity` | gauge | none | the configured capacity |
| `wherry_runs_past_deadline` | gauge | `profile` | tasks held past their task_ended_by that have not ended |
| `wherry_advertised_timestamp_seconds` | gauge | none | Unix time of the latest advertisement Kafka acknowledged, 0 before the first |
| `wherry_start_failures_total` | counter | `profile`, `class` | start failures: `input-too-large`, `dispatch`, `platform`, and `offer-unavailable`, which counts a release by a member with no matching profile, has no start-failure record and carries `profile="none"` |
| `wherry_dispatch_outcomes_total` | counter | `profile`, `outcome` | dispatches: `created`, `nothing`, `unknown` |
| `wherry_stop_failures_total` | counter | `profile` | failed `Stop` calls |
| `wherry_purge_failures_total` | counter | `profile` | failed `Purge` calls |
| `wherry_sweep_failures_total` | counter | `profile` | failed `Sweep` calls |
| `wherry_collection_failures_total` | counter | `profile`, `reason` | runs whose outputs were not collected: `not_stopped`, `over_limits`, `timeout`, `failed` |
| `wherry_open_waits_total` | counter | `tenant`, `outcome` | open request waits: `opened`, `refused`, `unanswered`, `expired` |
| `wherry_open_wait_seconds` | histogram | `tenant` | time a claimed run waited for its attempt wrap or refusal |
| `wherry_redactions_total` | counter | `tenant`, `profile` | credential values replaced in published output |
| `wherry_bus_errors_total` | counter | `op` | Kafka errors: `read`, `write`, `transactional ID`, `share group` |
| `wherry_denials_total` | counter | `op` | Kafka authorisation errors, by the capability denied: `read`, `write`, `transactional ID`, `share group`, `membership` |
| `wherry_renewal_failures_total` | counter | none | failed claim renewals |
| `wherry_transaction_aborts_total` | counter | none | transactions aborted after a failed produce |
| `wherry_ambiguous_commits_total` | counter | none | commits with an unknown outcome: a run gives its task back for redelivery, an open request is answered again only when its answer is absent, and the key custodian reads back its snapshot topic |
| `wherry_fenced_total` | counter | none | runs given up to a newer holder |
| `wherry_custodian_takeovers_total` | counter | `reason` | snapshots this receiver published taking over its pool's key custody: `takeover`, `contested`; both start at 0 |
| `wherry_runs_key_lost_total` | counter | none | runs this receiver ended with the wherry event `key-lost`; starts at 0 |
| `wherry_claimant_changes_total` | counter | `reason` | claims the key custodian served to a new claimant: `released`, `lease` |
| `wherry_ignored_wraps_total` | counter | none | wraps whose key does not decrypt the task's envelope |
| `wherry_metrics_dropped_total` | counter | `profile`, `reason` | plugin samples dropped and `Metrics` calls that failed |
| `wherry_plugin_platform_errors_total` | counter | `profile`, `class` | platform API errors: `unavailable`, `denied`, `conflict`, `timeout`, `other` |
| `wherry_plugin_cleanup_pending` | gauge | `profile` | the profile's ended jobs awaiting cleanup |
| `wherry_plugin_sweep_deletions_total` | counter | `profile`, `kind` | material a sweep deleted: `job`, `variable`, `directory` |
| `wherry_plugin_placement_seconds` | histogram | `profile` | time from dispatch to the agent task starting |
| `wherry_plugin_storage_free_bytes` | gauge | `profile` | free bytes of the storage the profile's runs share |

A rejection's `reason` is its fixed text, such as `duplicate submission`. A
reason that quotes the task counts as `git remote host outside the pool's vcs
hosts`, `pool has no storage`, `not started by task_started_by` or
`not ended by task_ended_by`.

An open wait is `opened` when the key custodian's attempt wrap arrives,
`refused` when the key custodian refuses the request, `unanswered` when no
answer arrives within 5 minutes and 120 seconds of the request's append time
and the receiver releases the task, and `expired` when the run's
`task_started_by`, or for an adopted job its `task_ended_by`, passes first.

The core histograms' buckets are 1, 5, 15, 30 and 60 seconds, 2, 5, 10 and 30
minutes, and 1, 2, 4 and 6 hours. `wherry_plugin_placement_seconds` uses
`pluginsdk.PlacementBuckets`: 1, 2, 5, 10 and 30 seconds, and 1, 2, 5, 10 and 30
minutes.

## Host commands

The host user's commands run under `wherry host`.

### Shortcuts

`wherry submit`, `wherry wait`, `wherry status`, `wherry events`,
`wherry logs` and `wherry cancel` are shortcuts for `wherry host submit`,
`wait`, `status`, `events`, `logs` and `cancel`, with the same arguments and
flags. No other command has a shortcut.

### `wherry host init`

```sh
wherry host init [BUNDLE] [--config-file FILE]
```

Sets the host up from its bundle, and configures it for every pool its tenant
is entitled to:

1. With `BUNDLE`, it reads the bundle and copies it to
   `~/.config/wherry/bundle.yaml`, mode 0600 in a directory of mode 0700,
   replacing any file there atomically, and names it as `bundle` in the
   configuration, even when the configuration named another bundle. Without
   `BUNDLE`, it uses the bundle the configuration names, and refuses when it
   names none.
2. When the configuration names no `ca_file` and `~/.config/wherry/ca.pem` is
   absent, it asks for a CA file; a blank answer keeps the system's trusted
   roots, and a path is recorded as `ca_file` when the file holds at least one
   PEM certificate, and asked again when it holds none.
3. It creates the host key at `host_key`, a private JWK (RFC 7517) with mode
   0600, unless the file exists, creating its directory with mode 0700. It
   registers the key on the host's key topic when the topic holds none, and
   prints the key ID. It fails when the key topic holds another key.
4. It reads the terms of every pool the tenant is entitled to, and asks for a
   source, `file:PATH`, `env:VAR` or `command:ARGV`, for each credential their
   agents take that `credentials` lacks. It names each pool that delivers the
   credential and its target there; a name several pools share is asked
   once. When the agent's target variable is set in the environment, it
   first offers that variable as the source, defaulting to No
   ([suggested sources](security.md#suggested-sources)).
5. For each VCS host of those pools with no `git` entry for it, it asks for a
   URL prefix on that host, `y` taking `https://<host>/`, and the credential the
   prefix takes, blank for anonymous, then that credential's source when it is
   new. A prefix that does not start with `https://` or `ssh://`, names another
   host or is in `git` already is asked again.
6. For each of those pools with storage and no `storage` entry, it asks for
   the names of the credentials holding the access key and the secret key,
   then their sources when they are new.
7. It writes the configuration, at the path `--config-file` or
   `WHERRY_CONFIG` names or `~/.config/wherry/config.yaml`, mode 0600 even
   when the file existed with another mode, with references to sources and
   never a value.

A blank answer skips the credential, git host or pool; a credential whose
source was asked once in a run is not asked again in that run. A re-run keeps
every entry and asks only for what is missing. Without a terminal it asks nothing:
it writes the configuration with the bundle, and lists each credential, git
host and pool storage that still has no source. A tenant entitled to no pool
gets its bundle placed and key registered, and `host init` says to run it
again once the operator entitles the tenant to a pool.

### `wherry host key rotate`

Replaces the host key with a new one:

1. It refuses while a run this host submitted has a latest state that is
   not terminal, as `wherry status` shows it, and names those runs. A
   receiver admits a task only under its host's current key.
2. It writes the new key to `<host_key>.next` with mode 0600 and syncs the
   file and its directory.
3. It publishes a `rotate` [key record](a2a-extension-v1.md#key-records),
   signed by the old and new keys, to the host's key topic and waits for the
   broker's acknowledgement.
4. It folds the key topic again. When the topic's current key is the new
   key, it renames `<host_key>.next` over `host_key`, which removes the old
   private key, and syncs the directory. It then prints
   `old_key_id=<id> new_key_id=<id>`.

Stop every other wherry process of the host first, such as the MCP server
and any `wherry wait`; they hold the old key.

Every host command that finds `<host_key>.next` folds the host's key topic
before anything else. When the current key is the next key's, it renames
`<host_key>.next` over `host_key`; when it is still the old key, it removes
`<host_key>.next`. It fails when the current key is neither.

### `wherry host receivers`

Lists the receivers of every pool the tenant is entitled to, sorted by pool
and receiver ID:

```
receiver=<id> pool=<pool> capacity=<n> in_flight=<n> advertisement_age=<age> status=<status>
  profile=<name> plugin=<type> agent=<agent> ready=<bool> labels=<k=v,...> [inline_limit=<n>] [models=<m,...> default_model=<m>] [credentials=<name:env=VAR|name:file=FILE[(missing,optional)],...>]
```

`status` is `live` when the latest advertisement is dated within 120
seconds of now, either side, `stale` otherwise, `silent` when there is none,
and `ended` after the receiver's membership end. Plugin profiles, their plugins and their labels come from the
receiver's configuration record, and an agent's `models` and
`default_model` from the pool's [terms](configuration.md#pool-terms) when it has them.
`ready` is true when the receiver is live
and its advertisement offers the profile. A live receiver with no ready
profile lists each profile with `ready=false`. `inline_limit`, when set, is
the most inline input bytes the offered profile delivers. `credentials` names
the agent's credentials in the pool's terms, each with its target, the
agent's environment variable or the file in its credentials directory,
marking `missing` each this host's configuration has no
[source](configuration.md#client-configuration) for and `optional` each a run
goes without, as in
`credentials=openai-api-key:env=OPENAI_API_KEY,github-token:env=GH_TOKEN(missing,optional)`.

`--json` prints a JSON array, one object per receiver:

| Member | Content |
| --- | --- |
| `receiver_id`, `pool` | strings |
| `profiles` | `{name, plugin, agent, inline_limit, models, default_model, credentials, labels, ready}`, `credentials` holding `{name, env, file, optional, configured}`, `env` or `file` the target; optional values are omitted when unset |
| `capacity`, `in_flight` | integers from the latest advertisement |
| `last_advertised` | RFC 3339 time of the latest advertisement; omitted when there is none |
| `status` | `live`, `stale`, `silent` or `ended` |

### `wherry host submit`

Resolves a placement, signs the task and publishes it to the chosen pool. It
prints `run_id=<id> pool=<pool>`, or fails with `no matching live receiver`.

Before it publishes, it folds the host's key topic and refuses when the host
key is not the topic's current key, since receivers admit a task only under
that key. When `<host_key>.next` exists, the next host command completes the
interrupted `wherry host key rotate`; otherwise the host was re-keyed elsewhere
and needs `wherry host init` on a re-provisioned host.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--agent` | the pool's default, then the terms' order | agent to run |
| `--model` | the agent's `default_model` | model the agent runs, one of its `models` in the pool's terms |
| `--selector` | empty | [selector](#selectors) over plugin profile labels |
| `--prompt` | | prompt text |
| `--prompt-file` | | file holding the prompt; `-` reads standard input |
| `--git REMOTE@REF` | | repository the agent clones: an `https` or `ssh` URL, or `user@host:path`, at a branch, tag or commit |
| `--git-path` | the repository name | checkout directory, relative to the agent's working directory |
| `--git-changes patch\|bundle` | | ask for the agent's commits back as the output `changes.patch` or `changes.bundle` |
| `--input FILE[:s3]` | | file the agent gets in `WHERRY_INPUT_DIR` under its base name; `:s3` uploads it, at most 5 GiB, to the pool's bucket; repeatable |
| `--credential NAME:file=FILE` or `--credential NAME:env=VAR` | | credential `NAME` from `credentials` in the client configuration, which the agent gets as the file `FILE` in `WHERRY_CREDENTIALS_DIR` or in the environment variable `VAR`; repeatable |
| `--output NAME[:required][:s3]` | | file the agent is asked to leave in `WHERRY_OUTPUT_DIR`; `:required` fails the run without it, and `:s3` returns it through the pool's bucket, in either order; repeatable |
| `--start-by` | `10m` | `task_started_by`: an RFC 3339 time, or a Go duration from now, by which the agent's job must start |
| `--end-by` | `2h` | `task_ended_by`: an RFC 3339 time, or a Go duration from now, by which the run must end |

Each bound must fall after now, and travels in whole seconds. `submit`
refuses, before it publishes, an `--end-by` later than `--start-by` plus
six hours, and a `--start-by` later than the pool's `start_by_limit` from
now with `task_started_by is past the pool's start_by_limit`. A task whose
job has not started by `task_started_by` ends `TASK_STATE_FAILED` with
`not started by task_started_by (<RFC 3339 time>)`.

`submit` refuses, before it publishes, a model outside the agent's
`models`, including any model for an agent with none:

```
invalid input: model opus is not in agent claude's models in pool build
```

Without `--agent`, `submit` considers only the agents whose `models` hold
the model, and refuses a model no agent of the pool holds. wherry does not
interpret a model name; the profile's plugin passes it to the agent as
`WHERRY_MODEL`.

Exactly one of `--prompt` and `--prompt-file` is required, and an empty
prompt file fails with `--prompt-file is empty`. `--prompt-file -` reads the
prompt from standard input, as in `generate-brief | wherry submit
--prompt-file -`. The other `--git-` flags
need `--git`, which splits at its last `@`. `submit` writes a
`user@host:path` remote as `ssh://user@host/path`. The
[extension](a2a-extension-v1.md#git-input) gives the rules for the remote,
ref, path and host key. `submit` refuses, before it publishes, a remote
whose host is not one of the pool's [`vcs`](configuration.md#pool-terms) hosts:

```
invalid input: git remote host github.com is not in pool build's vcs hosts
```

The clone's credential and host key come from the client configuration's
[`git`](configuration.md#client-configuration) entry whose `url` is the longest prefix of the
remote. The entry's credential joins the task as a task credential the agent
gets as the file `git-credential` in `WHERRY_CREDENTIALS_DIR`, so no
`--credential` may name it as well, and `submit` reads at most one byte over
the 8 KiB bound of its host key file. A remote under no entry clones without a
credential, and an `ssh` remote under no entry with a host key is refused.
With a host key, no input may be named `known_hosts`.

`--git-changes` adds `changes.patch` or `changes.bundle` to the expected
outputs, unless `--output` names it already; neither is required.

`--input` and `--output` read everything after the first `:` in their value
as suffixes, so they refuse a path or name that holds a `:`.

Input files travel inline in the submission record under two limits. The
first is the smallest `inline_limit` declared for a profile among the chosen
pool's receivers' profiles that launch the placed agent and satisfy the
selector, read from each receiver's last advertisement, live or stale; an
ended receiver does not count, and an undeclared profile limit is zero and
sets none. The second is the pool's [record limit](configuration.md#record-limit), which
`pool apply` records in the terms: `submit` measures the signed submission
record against it. `submit` applies the profile limit to the input files
together, then the record limit to the record. Over either, a pool with
storage and a configured host storage credential uploads every inline input
through S3. Otherwise `submit` refuses before publishing, naming the profile
and its limit or the record's size and the record limit, and says that a pool
with storage would carry the inputs through S3. A record still over the record
limit with every input in S3 is refused naming the prompt's size and the
record limit. Input and credential file names are [file
names](a2a-extension-v1.md#submission), and output names relative paths of
them. A credential's environment variable matches `[A-Za-z_][A-Za-z0-9_]*`
and is none a plugin sets for the agent, such as `HOME` or
`WHERRY_CREDENTIALS_DIR`. The task's credentials have unique names and
targets, and none takes the target of one of the agent's credentials:

```
invalid input: task credential deploy-key takes the environment variable GH_TOKEN of one of agent codex's credentials
```

A task credential named as one of the agent's travels once in the envelope
and is delivered at both targets.

`submit` refuses, before it publishes, a task credential or a required
credential of the placed agent that the client configuration has no
[source](configuration.md#client-configuration) for:

```
invalid input: auth required: credential openai-api-key is not in this host's configuration
```

An optional credential of the agent without a source is named in the
submission's `unsupplied_credentials`; the run goes without it, and its
[prompt](configuration.md#agent-contract) names it as not supplied.

`--input FILE:s3` and `--output NAME:s3` use the pool's [S3 lane](a2a-extension-v1.md#s3-inputs-and-outputs).
`submit` refuses them, before it uploads or publishes anything, when the
client configuration has no [`storage`](configuration.md#client-configuration) or the
pool's terms no [`storage`](configuration.md#pool-terms):

```
invalid input: pool build has no storage for S3 inputs and outputs
```

It uploads each S3 input to `wherry/<tenant>/<run>/inputs/<name>` in the pool's
bucket with one PUT, then publishes the task with each input's name, size and
SHA-256 and no URL. S3 input names are file names, distinct from the input
files' names, and the run's parts and S3 inputs number at most 256 together. S3
inputs count against neither limit. When publishing fails, `submit`
deletes what it uploaded.

`submit` resolves the agent's credentials the host supplies and the task's
credentials, presigns the S3 inputs and outputs, and encrypts them in the task's
envelope under the pool's key; the host can go offline once it returns
([credential envelopes](security.md#credential-envelopes)). It refuses a run
whose envelope would pass 64 KiB, naming the envelope's size, and a run
whose source fails, naming the credential:

```
resolve credential <name>: its command exited with status 1
```

Before it places the task, `submit` resubmits each run in this host's
records that ended with the wherry event `key-lost` before its
`task_started_by` and has no produced replacement. The replacement is a new
run under a new run ID whose submission `replaces` the failed run
([Resubmission](a2a-extension-v1.md#resubmission)). `submit` logs
`run <id>: lost its pool key before it started; resubmitted as run <id>` on
standard error for each. A resubmission that fails is logged as
`run <id>: resubmit: <error>` and retried by the next host command that
reads the run. It does not fail the submission.

### `wherry host cancel RUN_ID`

Publishes a signed cancel of the run to its pool's events and prints
`run_id=<id>`.

### `wherry host wait RUN_ID`

Waits for a run to finish and collects it. `--timeout` sets the wait,
`2h10m` by default: the default `--start-by` and `--end-by` together.
`--outputs-dir DIR` sets where the run's outputs go, by default
`<state_dir>/runs/<run>/outputs/`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--timeout` | `2h10m` | how long to wait for the run to end |
| `--outputs-dir` | `<state_dir>/runs/<run>/outputs/` | where the run's outputs go |

Standard output carries the result: each part of the status message on its
own line, then the `output=` lines. `wait` prints no transcript;
[`wherry logs`](#wherry-host-logs-run-id) reads it, and `wherry logs ID > file`
keeps it. Standard error carries errors and the diagnostics below.

The status message's parts are the agent's answer or what went wrong, as its
[outcome file](configuration.md#agent-contract) reports it, or the
receiver's reason, for every state. A part prints as its text, its JSON
data, `[url <url>]`, or `[raw <media type> <n> bytes]`. For a
[reply](a2a-extension-v1.md#the-reply) over 64 KiB, the reply chunks'
verified bytes print whole in place of the preview the message carries, and
`wait` writes no file of it. Standard error then says
`reply cut at the pool's reply limit` when the reply was longer than the
pool's `reply_limit`, or
`reply rejected by this host: <reason>; printed its preview` when the chunks
do not verify, with a reason as for an output.

For a run whose placement names a model, `wait` prints `model=<model>` on
standard error.

`wait` writes each output that [verifies](a2a-extension-v1.md#inputs-and-outputs)
against the result, at its relative path beneath the outputs directory, and
prints a line for it:

```
output=<name> path=<path> sha256=<hex> size=<bytes>
```

For `changes.patch` it also prints `apply with: git am <path>`, and for
`changes.bundle` `fetch with: git fetch <path> HEAD`, on standard error.
No expected output takes the name `reply.txt`.

`wait` reads the run's statuses and result from the tenant feed, and its
output chunks from the tenant's transcript topic, from the first offset at
or after one minute before the append time of the run's earliest echo on
the feed. Once it sees a terminal status, it reads the transcript topic to
its last stable offset, and waits until that reaches the high watermark it
lists after it sees the terminal, before it writes the outputs.

It prints `output <name> not returned: <reason>` on standard error for each
output the result lists as not returned and each one that fails
verification. A write never leaves the outputs directory, whatever links
it holds.

`wait` downloads each S3 output the result lists with the host's storage
credential, to the same place, and prints it as it prints any output. It
downloads only an S3 output the run declares, at its key
`wherry/<tenant>/<run>/outputs/<name>`, and writes it only when its bytes
match the listed size and SHA-256; a file already there that matches is
kept. Once the run is terminal and every listed S3 output has verified,
`wait` deletes every object under `wherry/<tenant>/<run>/` in the pool's
bucket, inputs included. A run that ends while no `wait` runs keeps them
until a `wait` on it or the bucket's lifecycle rule removes them.

A run this host submitted whose `task_started_by` passes with no status from
a receiver can never start. `wait` then collects nothing, prints nothing on
standard output, and exits at once with status 1 and
`not started by task_started_by (<RFC 3339 time>): no receiver claimed the task`.
`task_started_by` comes from this host's record of the submission, so
`wait` on another host of the tenant waits for its timeout.

A run this host submitted that ends with the wherry event `key-lost` before
its `task_started_by` never started: its pool key was lost
([Lost pool keys](concepts.md#lost-pool-keys)). `wait` resubmits it under a
new run ID, or produces the replacement this host already stored, and
follows the replacement to its outcome, through every later replacement. It
prints one line on standard error per replacement:

```
run <id> lost its pool key before it started; resubmitted as run <id>
```

The result, outputs and exit code are those of the last run followed, and
the default outputs directory is that run's. `wait` refuses a chain of
replacements that names a run twice. When the resubmission fails, for
example on a suspended pool or a pool whose current snapshot is missing or
not authorised, `wait`
exits with status 1 and
`wait <id>: run <id> lost its pool key and is resubmittable until <RFC 3339 time>; resubmitting it failed: <error>`.

#### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | the run ended `TASK_STATE_COMPLETED` and its outputs were written |
| 1 | `wait` could not wait or collect: usage, run not found, broker unreachable, outputs not written or failing verification, `task_started_by` passed unclaimed |
| 2 | the run ended `TASK_STATE_FAILED` |
| 3 | the run ended `TASK_STATE_CANCELED` |
| 4 | the run ended `TASK_STATE_REJECTED` |
| 6 | `--timeout` passed with the run still going |

For codes 2 to 4 the error is `run ended in <state>`.

### `wherry host status [RUN_ID]`

Reads the latest verified state of a run without waiting. With `RUN_ID` it
prints the state on the first line, `model=<model>` next when the run's
placement names one, then one line per part of the status message. The
state is `TASK_STATE_SUBMITTED` until a receiver reports one. For a run this
host submitted whose `task_started_by` passed with no status from a receiver,
the line after the state and model is
`not started by task_started_by (<RFC 3339 time>): no receiver claimed the task`.

Without `RUN_ID` it prints one line per run, latest update first:

```
<run> pool=<pool> [model=<model>] <state>
```

The list holds the runs in this host's records in `state_dir` and the runs
a receiver echoed on the tenant feed, which include runs from the tenant's
other hosts. A run's update time is that of its latest status. While it has
none, the time is this host's submission of the run when its record holds
one, else the Kafka timestamp of the run's echo. A record in `state_dir`
that does not decode counts as skipped, and its run is listed only through
its echo. With no runs it prints nothing.

A run that ended with the wherry event `key-lost` before its
`task_started_by` shows `TASK_STATE_FAILED` with
`cannot be opened: the pool's custodian changed and its key was lost`.
`status`, with or without `RUN_ID`, resubmits such a run of this host under
a new run ID, as `submit` does. With `RUN_ID`, its last line is then
`replaced_by=<id>`, the replacement's run ID. `wherry status` with that ID
reads the replacement, and `wherry wait` on either run follows it to its
outcome.

`status` exits with status 0 whenever it reads a state, whatever the
state. It fails when the run is not found or the read fails.

### `wherry host events RUN_ID`

Prints a run's verified history: one line per status, one per event ID, in
feed order, then one line per attempt with its transcript chunk count. An
attempt's terminal status carries its count. For an attempt with no
terminal status, running or released without one, `events` counts its
chunks on the tenant's transcript topic.

```
attempt=<n> kind=<kind> state=<state> receiver=<id> time=<time> message="<text>"
attempt=<n> transcript_chunks=<n>
```

`message` appears when the status has one, with each redaction marker shown
as `wherry status` shows it.

### `wherry host logs RUN_ID`

Prints a run's transcript: each line of the agent's standard output, in
output order, as the receiver redacted it. The lines are those the run's
highest delivery attempt published, and, before its first line, those of
the highest earlier attempt that has them, as
[Chunk](a2a-extension-v1.md#chunk) specifies. `logs` is read-only.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-f`, `--follow` | off | print lines as the agent prints them, until the run ends |

`logs` reads the lines from the tenant's transcript topic, from the
first offset at or after one minute before the append time of the run's
earliest echo on the tenant feed. Once the feed holds the run's terminal status, it reads the
transcript topic to its last stable offset, and waits until that reaches the
high watermark it lists after it sees the terminal, before it prints.
Without `-f`, it prints the lines the topic holds and exits.

With `-f`, `logs` follows the tenant's transcript topic with one consumer
from the run's echo, so each line prints as it is written, once and in
order. When the run reaches a terminal status, it reads the transcript
topic to the high watermark as above, prints the lines it has not printed,
and exits with status 0.

A run that ended with the wherry event `key-lost` never started, so it has
no transcript. `logs -f` resubmits such a run of this host, as `submit`
does. The replacement's lines are under its own run ID, which
`wherry status RUN_ID` prints as `replaced_by=<id>`.

`logs` prints every line; a range comes from the shell, as in
`wherry logs ID | sed -n '101,200p'`.

`logs` shows a Claude stream-json transcript without each thinking
block's `signature` and without `system` lines of subtype `thinking_tokens`.
The transcript topic keeps both. A line it drops a signature from is
re-encoded compact with its members sorted and escapes resolved; a line
that does not decode is shown as published.

A line that is not UTF-8 prints as its bytes. For a line over 512 KiB, which
the receiver did not store, it prints `line at offset <seq> not stored:
sha256=<hex> size=<bytes>` on standard error. Each redaction marker reads
`[redacted:<name>]`, and `<name> redacted <n> times` on standard error counts
the markers in every line of the transcript. The transcript grows while the agent
runs.

`wait`, `status`, `events`, `logs` and `cancel`
find a run from this host's record of it in `state_dir`, or from a receiver's
echo on the tenant feed. They fail with `run not found` when there is neither.

### `wherry host mcp`

Serves the [MCP tools](mcp.md) over stdio, logging to standard error.
The packaged MCP launch uses `["host", "mcp"]` and the default configuration path.
A server that runs `wherry host mcp --config-file PATH` uses another, as
[Harness integration](operations.md#harness-integration) shows.

## Dev commands

`wherry dev` runs a whole deployment on one machine for trying wherry and
developing against it, as [Try it on one machine](quickstart.md#try-it-on-one-machine)
walks through. Each command takes `--instance NAME`, default `default`, a
[name](#names). An instance lives in `$XDG_DATA_HOME/wherry/dev/NAME`, by
default `~/.local/share/wherry/dev/NAME`, mode 0700, in two parts:
`cache/`, which `init` fills and `down --reset` removes, and `env/`, which `up`
creates and `down` removes. The ports are fixed, so one instance is up at
a time: Nomad on 4646, 4647 and 4648, Kafka on 9093 and, with storage, S3
on 18080, all on 127.0.0.1.

An instance is up while its `env/up.json` names a live Nomad agent that
`up` started. Every step prints its name on standard error, and a failed
step fails with `step <name>: <cause>`.

`dev` is one developer's tool, and two of its limits follow from that.
Commands run at once on one instance are not serialized: run one at a
time. And `XDG_DATA_HOME`, or `HOME` without it, must be the same for `up`
and `down`, since it locates the instance and its record of the host files
`up` created; `XDG_CONFIG_HOME` may change, since the
record holds absolute paths.

### `wherry dev init`

```sh
wherry dev init [--instance NAME] [--runtime podman|docker] [--storage-enabled] [--agent NAME]
```

Acquires everything `up` runs into `cache/` and records it in
`cache/manifest.json`:

- Nomad CE 2.0.7, the release the nomad plugin's CI tests. Its
  `SHA256SUMS` must carry a signature by HashiCorp's release key,
  fingerprint `C874 011F 0AB4 0511 0D02 1055 3436 5D94 72D7 468F`, and list
  the pinned SHA-256 of the `linux_amd64` zip. The zip must match
  `SHA256SUMS`.
- On Linux with Podman, the newest `nomad-driver-podman`, checked the same
  way.
- The newest `nomad/v*` release of the nomad plugin that passes three
  checks: its `checksums.txt` verifies against its sigstore bundle from the
  plugin's release workflow, its archive matches `checksums.txt`, and its
  binary speaks this wherry's plugin protocol. A binary that speaks another
  is dropped for the next older release.
- `docker.io/apache/kafka:latest`, pulled and tagged
  `wherry-dev/kafka:<id12>` in the runtime's own store. With
  `--storage-enabled`, also `quay.io/benjamin_holmes/ceph-aio:v21`, tagged
  `wherry-dev/ceph-aio:<id12>`. Under Podman the tags start `localhost/`.
- A copy of the running wherry binary, which the receiver job runs.

`--runtime` is required when both `docker` and `podman` answer `version`,
and found otherwise. `init` looks for `claude`, `codex` and `gemini` on
`PATH`. One found is the default agent. With several it asks on a
terminal, and fails with `found claude and codex; choose the default with
--agent` without one. With none and no `--agent` it fails with
`no agent harness found on PATH (looked for claude, codex, gemini); install one, or choose --agent mock`
before it downloads anything. `--agent NAME` takes a harness it found or
`mock`. `init` without `--storage-enabled` drops storage from an instance
that has it.

`init` sends `GH_TOKEN`, else `GITHUB_TOKEN`, to GitHub's API as a bearer
token when one is set. Without one, GitHub allows 60 API requests an hour
from an address, and a spent limit fails naming the time it resets.

`init` downloads only what changed and prints `instance NAME: unchanged`
when nothing did. A harness installed later needs `init` again. It refuses
while the instance is up with `instance NAME is up; run wherry dev down
first`. A manifest of another layout version is refused naming its path.

### `wherry dev up`

```sh
wherry dev up [--instance NAME] [--runtime podman|docker] [--agent-image NAME=IMAGE]...
```

Builds and starts the environment from `cache/`, downloading nothing, and
returns once a run of the mock agent completes. Its steps:

1. `preflight` refuses while this instance is up (`instance NAME is
   already up`) or another is (`instance OTHER is up; run wherry dev down
   --instance OTHER first`), without a manifest (`nothing acquired for
   instance NAME; run wherry dev init first`), with a cached file whose
   SHA-256 differs from the manifest's, with a `--runtime` other than the
   manifest's, with a port in use (`port 9093 is in use`), and when the
   running binary differs from the copy (`the running wherry differs from
   the snapshot; run wherry dev init`).
2. `harnesses`, `credentials`: the agents are the harnesses `init` found,
   the `--agent-image` names and `mock`. For each, the set variables of
   `CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`, `CODEX_API_KEY` (else
   `OPENAI_API_KEY`), `GEMINI_API_KEY` and `WHERRY_DEV_MOCK_TOKEN` are
   copied into `env/host/credentials/`, mode 0600.
3. `tls`, `super-user`: an instance CA, the broker's certificate for
   127.0.0.1, and the password of the Kafka super user `wherry-admin`, the
   only SCRAM credential the broker starts with. These are the Kafka
   administrator's part.
4. `image` checks that the manifest's tags are still in the runtime, and
   fails with `image TAG is no longer in the runtime; run wherry dev init`.
5. `nomad` starts `nomad agent -dev` as you, detached, its log in
   `env/nomad/nomad.log` and its data directory mode 0700. Its
   `env.denylist` holds Nomad's default list, the copied variables and
   every variable whose name ends `_TOKEN`, `_KEY`, `_SECRET` or
   `_PASSWORD` when `init` ran, so the agents' environment carries none of
   them. On Linux without a Podman socket it starts `podman system
   service` beside Nomad.
6. `kafka` runs the broker as a Nomad job from the local tag and waits for
   a metadata request over SCRAM.
7. `operator` runs [`operator init`](#wherry-operator-init) with
   `wherry-admin` given as flags, never prompting, on `env/operator`, the
   instance's operator
   [working directory](configuration.md#operator-working-directory):
   `wherry operator --dir <instance>/env/operator check` runs against it.
8. `bootstrap` makes the operator's calls through that directory: pool
   `dev`, tenant `dev`, host `dev` and receiver `dev-1` with a plugin
   profile per agent, their bundles under `env/operator/bundles/`. The
   receiver's `config.yaml` names the instance CA as `ca_file`.
9. `host` writes the host's `config.yaml`, bundle and key, and registers
   the key. For instance `default`, unless `WHERRY_CONFIG` is set, they go
   at the standard [host paths](configuration.md#client-configuration);
   the config names `env/host/state` as `state_dir`, so dev writes no state
   at the standard state path. `up` creates each file exclusively, so it
   never replaces anything there. `cache/manifest.json` records each path,
   its directory resolved through symlinks, before `up` creates it, and the
   file's device, inode, size and modification time after; no digest of a
   file holding a credential is kept.
   If the config, bundle or key is already there, or appears while
   `up` creates them, `up` removes what it created there and, as for every
   other instance and with `WHERRY_CONFIG` set, writes them in
   `env/host/`. The config names the instance CA as `ca_file`, and the dev
   pool's [storage](configuration.md#client-configuration) credentials
   with storage.
10. `receiver` runs `wherry receiver run` from the copy as a `raw_exec` job
    and waits for `dev-1` to be live with every profile ready.
11. `smoke` runs the mock agent and checks the output it returns. `ready`
    writes `env/up.json` and prints the summary, a `submit:` line and an
    `mcp add` line for each harness. When the config is in `env/host/`, it
    prints `export WHERRY_CONFIG=<path>` first, and each `mcp add` line
    passes `WHERRY_CONFIG` to the server; otherwise neither names the
    config.

The harness profiles and `mock` run under `raw_exec` with label
`isolation: host`. `--agent-image NAME=IMAGE` splits at the first `=`. It
runs agent `NAME` in `IMAGE` with label `isolation: container`, never
pulled, so the image must be in the runtime. For `claude`, `codex` and
`gemini` the profile runs the harness from `/local`. Any other `NAME` runs
the image's entrypoint, which meets the
[agent contract](configuration.md#agent-contract).
[`agent.Containerfile`](https://github.com/benemon/wherry/blob/main/docs/agent.Containerfile) builds one for `claude`.

With storage, `storage`, `s3-user` and `bucket` run after `kafka`. ceph-aio
serves S3 on `http://127.0.0.1:18080`. `s3-user` has `radosgw-admin`
generate the user's keys and writes them to `env/host/credentials/`. Pool
`dev` takes the endpoint as its [storage](configuration.md#pool-terms), and
the smoke run sends an S3 input and returns its output through the bucket.
The ceph-aio job reserves 2048 MB of memory, and its data lives in the
container, so `down` removes it.

A failed step leaves the environment for `down` to remove.

### `wherry dev down`

```sh
wherry dev down [--instance NAME] [--reset]
```

Its steps, each run after a failed one:

1. `jobs` deregisters the receiver, every agent job, storage and Kafka,
   waits up to 60 seconds for their allocations to end, and purges them.
2. `containers` waits up to 60 seconds for the runtime to list no
   container of theirs, then removes those left by ID.
3. `nomad` stops Nomad and the Podman service with `SIGTERM`, and
   `SIGKILL` after 30 seconds.
4. `ports` checks that the ports are free.
5. `data` removes `env/`.
6. `host` removes each file the manifest records `up` creating at the
   standard host paths while it is still that file: a regular file, not a
   symlink, with the recorded device, inode, size and modification time, in a directory
   that still resolves to the recorded one. So a stale default never
   shadows a real host's, and a real host's file stays. Anything else, a
   missing file or one whose device and inode a crash kept `up` from
   recording included, it leaves and names:
   `left <path>: <reason>`. Two cases remain: a file replaced between
   this check and its removal is removed, since the operating system
   offers no removal conditional on identity; and a file edited in place
   that keeps its size and has its modification time set back is removed,
   since `down` keeps no digest of a file holding a credential.

`down` fails naming every failed step. It keeps `cache/` and prints a
`mcp remove` line for each harness, since the registration names a
configuration that is gone. An instance that is not up prints
`instance NAME is not up`, removes a leftover `env/` and the host files as
in step 6, and exits 0.

With `--reset`, `down` removes the host files as in step 6, then
`cache/`. It refuses `--reset` while the instance is up, and when it
cannot read the manifest, which may record host files: it names the
standard paths to check by hand before you remove the manifest. The
images stay in the runtime's store. Remove `wherry-dev/kafka:<id12>` with
the runtime.

## Version command

### `wherry version`

Prints the binary's release as `version=<release>` and its commit as
`revision=<commit>`, each on its own line. A release build's release is its
git tag, such as `v0.1.0-rc.9`. A build from an untagged commit prints a Go
pseudo-version, and a build without git information prints `(devel)` and an
empty revision. A build from a working tree with uncommitted changes adds
`modified=true`. It takes no flags and reads no configuration.
