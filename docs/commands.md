# Commands

[Key concepts](concepts.md) explains the model. The
[wherry A2A extension](a2a-extension-v1.md) specifies the records.
Use the configuration guide for your role:
[operator](configuration-operator.md), [receiver](configuration-receiver.md) or [host](configuration-host.md).
[MCP tools](mcp.md) lists the tools served by `wherry host mcp`.

## Command tree

Every command but `version` and the shortcuts runs under its role's word:

| Role | Commands |
| --- | --- |
| Operator | `wherry operator init`, `check` and `view`; `wherry operator list`, `show`, `apply`, `add`, `remove`, `allow`, `deny`, `suspend`, `resume`, `rotate`, `approve`, `reject`, `revoke` and `storage`, each naming its pool, receiver, tenant or host as a flag, and `approve` and `reject` a request by its ID |
| Receiver | `wherry receiver run` and `rotate` |
| Host user | `wherry host init`, `rotate`, `receivers`, `mcp`, `submit`, `result`, `status`, `events`, `logs` and `cancel` |
| Development | [Development commands](commands-dev.md) |
| None | `wherry version` |

`wherry submit`, `result`, `status`, `events`, `logs` and `cancel` are
[shortcuts](#shortcuts) for the `wherry host` commands of the same name.
`wherry --help` and `wherry -h` print the tree and list the shortcuts.
`-h` after `wherry operator`, `wherry host` or a command prints its usage on standard error and exits with status 0.
Every command is its role word then a verb, one level deep; every flag is a
long flag, `--name`, and the operator's resources are flags
([Operator commands](#operator-commands)).

## Conventions

- The [MCP tools](mcp.md)' results are the machine interface, and their
  shape freezes at 1.0. The CLI's text output may change. `wherry host receivers
  --json` is the one CLI JSON form, and it prints the objects the MCP
  `receivers` tool returns.
- `wherry operator`, `wherry receiver` and `wherry version` read no client
  configuration. Every `wherry host` command reads it first, and every one except `host mcp`
  then connects to Kafka.
- Every `wherry host` command takes `--config-file FILE`. Without it, the
  command reads the file `WHERRY_CONFIG` names, else `config.yaml` in
  `$XDG_CONFIG_HOME/wherry/`, by default `~/.config/wherry/`
  ([paths](configuration-host.md#client-configuration)).
  A leading `~/` is expanded.
- A command's flags may come before or after its `RUN_ID`.
- A failed command prints `wherry: <error>` on standard error and exits
  with status 1. `result`, and `submit --wait`, have their own [exit codes](#exit-codes).
- Durations use Go syntax, such as `90s`, `45m` or `720h`, and must be
  positive. [`remove`](#wherry-operator-remove) gives the rules for
  `--timeout`.
- `cancel`, `result`, `status`, `events` and `logs` print
  `skipped N unverifiable records` on standard error when N records of the
  run fail verification.
- Standard output carries a command's result and standard error its
  errors and diagnostics, so `wherry result ID >/dev/null` keeps only the
  diagnostics and the exit status.
- `result` and `status` print each redaction marker of a run this host
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
files, are private JWKs (RFC 7517) with mode 0600.

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
plugin profile. Every label is part of a profile's
[signature](concepts.md#placement), so a selector may name any key.

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
operator init`. [Working directory](configuration-operator.md#operator-working-directory)
lists what the directory holds.

Every operator command is `wherry operator <verb>`, one level deep, with the
object it acts on as a resource flag: `--pool POOL`, `--receiver RECEIVER`,
`--tenant TENANT`, or `--host HOST` beside the `--tenant` the host belongs
to. The verbs are `init`, `check`, `view`, `list`, `show`, `apply`, `add`,
`remove`, `allow`, `deny`, `suspend`, `resume`, `rotate`, `approve`, `reject`,
`revoke` and `storage`. Resources are flags because a verb spans kinds: `apply` takes a
pool or a receiver, `remove` a tenant, a host or a receiver. A verb whose
operand is only ever one kind takes it positionally, as `list tenants`,
`approve ID`, `host init BUNDLE` and `result RUN_ID` do. A missing, extra, mixed, repeated or
empty resource flag fails with the verb's usage, which lists its accepted
forms. Each form takes the options its section states and no other; one it
does not take fails with `unknown option --<name>`. This table maps the
forms before v0.2.0 to today's; [Upgrade](upgrade.md#command-grammar) lists
the break.

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

`wherry operator` writes every `wherry.operator` record as
`wherry.operator`, creating `wherry.operator` first if it is missing.
`apply --dry-run` creates nothing, and reads a missing
`wherry.operator` as holding no records. Options parse as Go flags,
in the forms `--name value`, `--name=value` and `-name value`, and the last of
a repeated option other than a resource flag wins. Options other than `--dry-run`, `--force`, `--all`,
`--scram` and `--storage` take a value, and options can appear anywhere after `operator`. An option without a
value fails with `flag needs an argument: -<name>`, or with the verb's usage when it is a resource flag, one no operator command
takes with `flag provided but not defined: -<name>`, and one the command does
not take with `unknown option --<name>`. `--suspect-key` is given once per key;
any other repeated option takes its last value.

Every operator command other than `init`, `check`, `view`, `show`, `list`
and a `--dry-run` ends by converging Kafka on `wherry.operator`. A pass reads
`wherry.operator` to its end and creates every ACL those records call for. It
re-reads them and fails with `ACL plan incomplete` when one is missing. It
deletes every other grant of a host or receiver principal on wherry's topics,
groups and transactional IDs, which takes back a suspended pool's hosts'
`WRITE` on its task-topic prefix. It deletes the SCRAM credential of each host and
receiver whose membership ended. When `wherry.operator` grows during a pass,
the command makes another, and after 5 such passes it fails with
`wherry.operator moved during each of 5 convergence passes`. Once a command
has written a record, it converges on every return, failed or not.
[Concurrent operator commands](operations-operator.md#concurrent-operator-commands)
states what this guarantees.

A recorded profile's signature topic exists before its record. A topic
missing afterwards was deleted outside wherry: `operator check` names it, and
`apply --pool` of the pool with the same terms recreates it before its no-update
path. A receiver never creates a topic.

`apply --pool` and `apply --receiver` read the object's file: `--file PATH`, else
`pools/<pool>.yaml` or `receivers/<receiver>.yaml` in the working directory.
Either applies without a prompt. `--file PATH` names a file, or a directory,
which selects the directory form of the
[pool terms](configuration-operator.md#pool-terms) or
[receiver configuration](configuration-receiver.md#receiver-configuration). With
neither, when standard input and standard error are terminals and `TERM` is
not `dumb`, the command asks for the object's real choices, with defaults
shown, writes the file, then applies it; a refused apply leaves the
file to correct and apply again. With neither and no terminal it refuses,
naming the fields the file must give. `--dry-run` writes nothing to the
working directory: with neither file it refuses rather than prompt, and it
creates no bundle directory. The pool and receiver object questions show one
question per page under headings, with each answer collapsing to one line.
Ctrl-C during those questions writes nothing.

`add --host`, `revoke` and `apply --receiver` write bundles under the working directory's
`bundles/`: `bundles/hosts/<tenant>/<host>.bundle` and
`bundles/receivers/<receiver>.bundle`, mode 0600, in directories mode 0700. A
bundle holds the broker address, the principal and its password; its holder
trusts the broker's certificate through its machine's trust store
([Broker and S3 trust](security.md#broker-and-s3-trust)). A host bundle also
holds its enrolment ID as `enrolment`, new at each `add --host` and `revoke`,
and the tenant's S3 key pair as `storage` once
[`storage`](#wherry-operator-storage) recorded it; a
receiver bundle holds neither. Each command that writes or rewrites a bundle
prints its path and the next step:
`wherry host init <file>` for a host, and the platform's bundle step for a receiver.
`add --host` refuses a host that exists with `host exists`, and a host whose removal
has not finished with `host is being removed; run remove --host again`;
[`revoke`](#wherry-operator-revoke) gives an existing host a new password and
bundle. `apply --receiver` of a receiver that exists
updates its configuration, and replaces its bundle only when the broker
address changed, keeping the password; `rotate --storage` does
the same for a host's bundle, the storage keys included. `add --host` and
`apply --receiver` of a new receiver never replace a bundle: before they change
anything in Kafka they create the bundle file, failing when a file is there,
and write the bundle into it at the end. A
command that fails leaves no bundle and no temporary file of its own, or, when
it cannot remove one, fails naming it.

`apply --receiver RECEIVER --force` reissues the identity of an existing receiver: a new SCRAM
password and a new bundle, written to a temporary file that is then renamed
over the bundle. It appends no record. Kafka holds one SCRAM credential per
user, so the old password stops working at once, and the receiver is
offline until its new bundle is in place. Use it to rotate a receiver's password, replace
a lost bundle or change its broker address. When writing, closing or
renaming the new bundle fails after the password changed, the command removes
its temporary file, leaves the old bundle, and fails saying that the old
bundle no longer works and that running it again with `--force` writes a new
one.

### `wherry operator init`

```sh
wherry operator init [--dir DIR] [--broker ADDR] \
  [--super-user NAME --super-user-password @FILE|@-] \
  [--super-user-mechanism SCRAM-SHA-512|SCRAM-SHA-256] \
  [--ca-cert PEM|@FILE|@-]
```

`operator init` delegates provisioning from the Kafka super user to `wherry.operator`.
It also creates `wherry.traces` and the trace topic of every live tenant where
missing and sets their configuration, so a re-run on an install that predates
them gives it every one ([Upgrade](upgrade.md#trace-topics-and-the-operator-s-group-grant)).
It creates `wherry.requests`, the topic hosts write their
[credential requests](#wherry-operator-approve) to, where missing, with one
partition, `cleanup.policy=delete`, `message.timestamp.type=LogAppendTime`,
`retention.ms=-1`, `segment.ms=86400000` and `max.message.bytes=65536`.
When standard input and standard error are terminals and `TERM` is not
`dumb`, you answer each question the flags do not give under a Broker heading.
You see one question per page.
Enter the broker address.
An address without a port uses the Kafka client's default port, 9092
([`kgo.SeedBrokers`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#SeedBrokers)).
Enter the super user's name.
Enter the super user's password. The prompt masks it, never prints it back,
and takes it as typed: leading and trailing spaces are part of the password,
and only a trailing CR or LF is trimmed, as from a file.
Press Ctrl-C to stop before any files are written.
If stderr is redirected, or prompts are otherwise unavailable, give every
required flag.
Missing flags are named in the error:
`operator init has no terminal to prompt on; give --broker, --super-user, --super-user-password`.

`--super-user-password` takes `@FILE`, the file holding the password, or
`@-` to read it from standard input, for example piped from a password
manager, as every flag that takes a value reads them. The password as the
flag's own text is refused, since a command line is visible to the other
users of the machine: `--super-user-password takes @FILE or @-; a password
on the command line is visible to other users`. `@-` is refused when
standard input is a terminal; there the prompt is the route. Errors about
`@-` say standard input and never treat it as a path. The password is
written nowhere, whichever way it arrives.

`--ca-cert` gives a CA the machine's trust store lacks, which the super-user
dial and every later command verify the broker against: the PEM text, `@FILE`
for a file's contents or `@-` for standard input, as every flag that takes a
value reads it. An empty value, and a value from which Go reads no
certificate, a bare path among them, are refused, and `--super-user-password
@-` and `--ca-cert @-` cannot both read standard input. The value goes into
`operator.yaml` as `ca_cert`, with every CR removed, so `"$(cat ca.pem)"` of
a CRLF file reads as the file does, and the file is read back before success
is reported ([Broker and S3 trust](security.md#broker-and-s3-trust)). Without
`--ca-cert`, a re-run keeps the `ca_cert` the existing `operator.yaml` holds,
dials through it and writes it back, as `host init` keeps one.

The super user logs in with SCRAM-SHA-512 unless `--super-user-mechanism SCRAM-SHA-256` says its credential is
SCRAM-SHA-256; the mechanism is never prompted for, and any other value is refused before a prompt. Kafka answers a
wrong password and a missing credential for the mechanism alike, so a refused login names the super user, the
mechanism and the flag. Every principal wherry issues, `wherry.operator` included, uses SCRAM-SHA-512.

It creates the working directory with mode 0755 when it is missing.
Connected as the super user, once, it:

1. writes a new random password for `wherry.operator` to a temporary file,
   mode 0600, in the working directory;
2. grants `wherry.operator` the [operator ACLs](security.md#the-operator-principal);
3. creates `wherry.operator` if it is missing;
4. sets the new password on `wherry.operator`, creating the principal or
   replacing its password;
5. renames the temporary file over `operator.password`.

It then writes `operator.yaml`: `broker`, `password_file: operator.password` and
`ca_cert` when `--ca-cert` was given, and appends `operator.password`, `operator.lock`, `bundles/` and `storage/` to
`.gitignore`, creating it if absent and adding only lines it lacks. It closes the super-user connection;
nothing it writes names the super user, which no later command uses. It ends
by printing `wherry.operator can provision; its password is in <dir>/operator.password`
on standard error.

Running `init` again rotates `wherry.operator`'s password: Kafka holds one
SCRAM credential per user and mechanism, so commands still running with the old password
fail. A failure between steps 4 and 5 leaves `operator.password` holding the
old password; running `init` again recovers. `init` always takes the super
user from its flags or prompts, never from `operator.yaml`.

### `wherry operator check`

```sh
wherry operator check
```

Loads `wherry.operator` and checks, in order:

- that no two names collide;
- every receiver configuration against its pool's terms;
- that every pool's [records still fit the broker](configuration-operator.md#record-limit);
- that every profile of each receiver whose membership has not ended has its
  signature topic, naming the receiver, the profile and the topic, and that
  `apply --pool` of the pool with the same terms creates it;
- topic retention and timestamp type against the [bounds](design.md#topics-1):
  `retention.ms=-1` on `wherry.operator` and every host and receiver key
  topic, and `message.timestamp.type=LogAppendTime` on every topic under each
  pool's task-topic prefix and its events topic;
- that every task topic under each pool's prefix and its events topic have
  `cleanup.policy=delete`, `retention.bytes=-1` and `segment.ms=3600000`;
- that each pool's snapshot topic, `wherry.pool.<pool>.custodian`, has
  `cleanup.policy=compact`, `message.timestamp.type=LogAppendTime` and
  `retention.ms=-1`;
- that each pool's receivers topic, `wherry.pool.<pool>.receivers`, has
  `cleanup.policy=compact,delete`, `message.timestamp.type=LogAppendTime`,
  `retention.ms=604800000`, `segment.ms=3600000` and
  `max.compaction.lag.ms=3600000`;
- that each pool's task, events, snapshot and receivers topics have one
  partition;
- that each pool's task topics keep records at least `start_by_limit` plus
  six hours plus one day, its events topic and each live receiver's progress
  topic at least as long as the greatest of them, `-1` when one is `-1`, and
  each entitled tenant's feed and transcript topic at least `start_by_limit`
  plus six hours;
- that `wherry.requests` has the configuration
  [`operator init`](#wherry-operator-init) gives it, naming the setting, its
  value and the value it wants;
- that every tenant feed and transcript topic has a `max.message.bytes` of at
  least 2 MiB;
- that `wherry.traces` and every live tenant's trace topic exist with one
  partition, `cleanup.policy=delete`, `message.timestamp.type=LogAppendTime`,
  `retention.ms=604800000` and `max.message.bytes=1048576`, naming
  `operator init`, which creates and sets them;
- that every ACL the state calls for is present, the prefixed task-topic
  grants of hosts and receivers included, with `wherry.operator`'s own
  [grants](security.md#the-operator-principal).

It fails naming the first check that does not pass. Once every check passes,
it prints a line for each live host that has no credential ID, which a host
enrolled before host credential rotation has until it is
[revoked](#wherry-operator-revoke) and enrolled again, or that still logs in
with its enrolment password, which `host init` replaces by filing a
[request](#wherry-operator-approve):

```
tenant=<tenant> host=<host> has no credential ID; revoke and re-enrol it
tenant=<tenant> host=<host> is on its enrolment password
```

It then lists what removal left on the broker, one line per topic, and exits 0:

```
leftover <topic> tenant=<tenant> offsets=<n>
leftover <topic> tenant=<tenant> host=<host> offsets=<n>
leftover <topic> receiver=<receiver> offsets=<n>[ kept: holds keys]
leftover <topic> pool=<pool> unattributed offsets=<n>
leftovers: <topics> topics, <offsets> offsets
```

The topics are, in this order: an ended tenant's `wherry.tenant.<tenant>`,
`wherry.tenant.<tenant>.transcript`, `wherry.traces.<tenant>` and each of its
`wherry.host.<tenant>.<host>`; an ended host of a live tenant's
`wherry.host.<tenant>.<host>`; an ended receiver's
`wherry.receiver.<receiver>` and `wherry.receiver.<receiver>.progress`; then
each pool's signature task topics that no profile of a receiver whose
membership has not ended records, which a relabelled or removed profile, an
ended receiver or a failed apply leaves and which belong to the pool, not to
one actor. `offsets` is the end offset less the start offset summed over the
topic's partitions, an upper bound on the records retained: transaction
markers and compacted records take offsets too. `kept: holds keys` marks an
ended receiver's key topic, which readers of its pool's records still verify
against. A topic deleted outside wherry is not listed; a pool's custodian,
receivers and events topics and `wherry.traces` are shared and never listed.
It prints nothing when there are none. It deletes nothing:
[Check a deployment](operations-operator.md#check-a-deployment) says what may
be deleted. It does not report ACLs beyond those, and does not check a plugin
profile's plugin or `config` section.

### `wherry operator view`

```sh
wherry operator view [--dir DIR] [--window 24h] [--port N] [--external-url URL]
```

Rebuilds a model of the cluster from the records of the last `--window`, 24 hours by default, tails them, and serves
it read-only on `127.0.0.1`, on `--port` or a free port, until Ctrl-C: JSON endpoints, a Server-Sent Events stream
and the page, a map of the cluster with a drawer per node and run. It prints the address to open on standard output,
with a token in the URL fragment that the page exchanges once for a session cookie, and the rebuild's run count and
time on standard error. `--external-url` names the https or http origin a reverse proxy serves the view at, whose
`Host` and `Origin` are then accepted and whose scheme makes the cookie `Secure`; a URL with a path is refused. The
view never produces to Kafka. [Operator view](operator-view.md) describes the model, its bounds, freshness, the API
and running it behind a proxy.

### `wherry operator list`

```sh
wherry operator list tenants
wherry operator list pools
wherry operator list receivers
wherry operator list hosts [--tenant TENANT]
wherry operator list requests
```

Lists what `wherry.operator` records, as plain text in deterministic name
order, and changes nothing. `list tenants` prints each live tenant's name on
a line of its own, then `  pool <pool>` for each pool it is entitled to and
`  host <host>` for each of its live hosts, two spaces indented:

```
acme
  pool build
  host laptop
```

`list hosts` prints the same tenant lines with their `  host <host>` lines
alone, so a tenant without a live host is its name and nothing under it;
`--tenant TENANT` restricts it to that tenant, and refuses one that is absent
or has ended with `no tenant by that name`. `list pools` prints each recorded
pool's name on a line, suspended pools included. `list receivers` prints the
name of each live receiver. A host or receiver is live until its removal
starts: revoking and ended ones are omitted from every form, as are ended
tenants.

`list requests` reads `wherry.requests` as well, and prints one line per
[credential request](#wherry-operator-approve) it holds, by ID:

```
request=<id> tenant=<tenant> host=<host> kind=<kind> state=<state>
bad-signature=<n>
```

| State | The request |
| --- | --- |
| `pending` | waits for `approve` or `reject` |
| `approving` | has an approval that began and has not finished; the next `approve` finishes it |
| `approved` | was approved; the host uses its password from its next command |
| `rejected` | was rejected |
| `expired` | was not decided within 7 days of the first copy under the host's current key; the next `approve` records it expired |
| `void` | had an approval that `revoke` or `remove --host` closed |
| `stale` | names a credential ID that is no longer the host's, so it can never be approved |
| `conflict` | was sent with two different payloads under one ID, and is not approved |
| `bad-kind` | asks for a kind of credential this release does not issue; it expires, or `reject` decides it |
| `host-ended` | belongs to a host that is removed or being removed |

A pass deletes the requests it decided when it ends, so a decided request is
listed only until then, and again if a copy of it is sent later. The
`bad-signature` line, printed when there are any, counts the records dropped
because they do not decode, their signature does not verify against the
current key of the host they name, or they name another host than the one
that signed them; such a record has no request to be listed under. The lines for live hosts with no credential ID or on their
enrolment password follow, as [`operator check`](#wherry-operator-check)
prints them. `list requests` takes no lock and changes nothing.

### `wherry operator show`

```sh
wherry operator show --receiver RECEIVER
```

Prints the receiver's liveness and recorded configuration as YAML: `status`,
then `last_advertised` when it has advertised, then `plugin_sha256`, the SHA-256 pinned for each plugin type, then
`profiles`, each plugin profile resolved, in recorded order, with its
`name`, `plugin`, `agent`, `replicas` when above 1, `labels` and merged `config`, in that order
([Plugin profile merge](configuration-receiver.md#plugin-profile-merge)),
its `signature`, the agent and every label, double-quoted with each line
feed shown as `\n`, and the signature's task `topic`:

```yaml
status: live
last_advertised: 2026-11-20T09:41:07.312Z
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
      signature: "claude\nplatform=nomad\n"
      topic: wherry.pool.build.tasks.claude.ef900330120d94dc0ea8292aaff80030aa744d924e6723016308bd36255e19a4
```

A `credential` key shows the credential's name, which is all the record
holds; neither `show` nor `--dry-run` prints a credential value.

`status` is what [`wherry host receivers`](#wherry-host-receivers) reports
for the receiver, read the same way: the operator reads the pool's receivers
topic and verifies each advertisement against the receiver's key topic.
The receiver is `live` when its latest authorised advertisement is dated
within 120 seconds of now, either side; `stale` when it is dated beyond that,
in the past or the future; `silent` when it has none; and `ended` once
revoked. `last_advertised` is that advertisement's time, in UTC, and is
absent for a silent or ended receiver. The latest advertisement is the one
with the latest date, so one dated ahead counts until that time passes. A
`--dry-run` of `apply --receiver` prints no status: the record it would hold
has no receiver to be live.

### `wherry operator apply`

```sh
wherry operator apply --pool POOL [--file PATH] [--dry-run]
wherry operator apply --receiver RECEIVER [--file PATH] [--dry-run]
wherry operator apply --receiver RECEIVER --force
```

`apply` creates or updates the object its resource flag names from that
object's file, and `--receiver R --force` reissues an existing receiver's
identity; `--force` takes no `--file` or `--dry-run`.

#### `apply --pool POOL`

Creates the pool, or records new terms for it, from the
[pool terms](configuration-operator.md#pool-terms) at `PATH` or in
`pools/<pool>.yaml`. An agent named by a
[credential catalog id](configuration-operator.md#catalog-agents) gets the
model credentials its entry derives, before any it lists; the terms recorded
are the expanded ones. The [guided path](#operator-commands) asks for the
agents one at a time, each one's models and credentials, then the default agent
from the entered names. After a catalog agent's name it prints the credentials
the catalog derives and asks only for further ones. Each credential asks for its name, delivery as an
environment variable or file, target, whether it is required, and an optional
description. The remaining questions are `producer_byte_rate` (default
1048576), `request_percentage` (default 10) and `start_by_limit` (default
`72h0m0s`). You correct a refused value in place. Quotas must be positive and the
start-by limit must be non-negative; apply enforces the tenant-feed retention
and storage bounds.

With no pool of that name, it records the terms, creates the pool's events,
snapshot and receivers topics, and sets the configuration of its share group
and topics. A new pool has no receivers, so it creates no task topic:
`apply --receiver` creates the signature topics of a receiver's profiles.

| Resource | Settings |
| --- | --- |
| share group `p-<pool>` | `share.renew.acknowledge.enable=true`, `share.auto.offset.reset=earliest`, `share.isolation.level=read_committed`, `share.delivery.count.limit` from `delivery_limit` (default 5), `share.record.lock.duration.ms` from `lock_duration` (default `15s`) |
| every task topic under `wherry.pool.<pool>.tasks.` | `retention.ms` of `start_by_limit` plus six hours plus one day, unless it is already longer; `message.timestamp.type=LogAppendTime`, `cleanup.policy=delete`, `retention.bytes=-1`, `segment.ms=3600000` |
| events topic `wherry.pool.<pool>.events` | the greatest task topic's `retention.ms`, unless it is already longer; the task topics' other settings |
| snapshot topic `wherry.pool.<pool>.custodian` | `cleanup.policy=compact`, `message.timestamp.type=LogAppendTime`, `retention.ms=-1` |
| receivers topic `wherry.pool.<pool>.receivers` | `cleanup.policy=compact,delete`, `message.timestamp.type=LogAppendTime`, `retention.ms=604800000`, `segment.ms=3600000`, `max.compaction.lag.ms=3600000`, created when missing |
| progress topic of each live receiver in the pool | the greatest task topic's `retention.ms`, or the terms' with none, unless it is already longer, created when missing |
| feed and transcript topic of every tenant with a host or an entitlement | `max.message.bytes=2097152` (2 MiB), set before it measures the records |

It sets these on new and existing topics. [Topics](design.md#topics-1) gives
the reasons.

It refuses, before it reads the broker, terms whose agent credentials
break the [pool terms](configuration-operator.md#pool-terms) rules, a
catalog agent listing a credential the catalog derives for it, and a
`start_by_limit` above 29 days and 18 hours. For a pool with `storage` it
refuses a `start_by_limit` that with six hours and ten minutes added
exceeds 7 days, the longest an S3 SigV4 presigned URL lasts, with
`start_by_limit plus the 6h run cap and the 10m upload grace exceeds 7
days, the longest an S3 SigV4 presigned URL lasts`: a run's upload policy
lasts ten minutes past its `task_ended_by`. It
refuses a delivery limit above the lowest
`group.share.max.delivery.count.limit` across the brokers, and terms whose
[records do not fit the broker](configuration-operator.md#record-limit). Before it
records anything it refuses a `lock_duration` above the brokers'
`group.share.max.record.lock.duration.ms`, naming the field and the ceiling,
and a `start_by_limit` that with six hours added exceeds the `retention.ms`
of the feed or transcript topic of a tenant entitled to the pool. Before it
records the terms it refuses a pool name that is not valid and an existing
task or events topic with more than one partition. It creates and
configures the pool's receivers topic before it records the terms, so a
recorded pool always has one. It creates the other topics after it records
the terms, which is when it checks the pool's names against every other
recorded name. It fails when a topic configuration it
sets is not applied within 10 seconds, or when a describe of it still fails
then, naming the describe's error.

For a pool of that name, before it records the terms, it creates the
signature topic, under the new terms, of every recorded profile of each
receiver in the pool whose membership has not ended, with the task topics'
settings above, so a record never routes to a missing topic.

With a pool of that name and other terms, it records the new terms, makes
the checks and settings above, and sets the share group's `share.delivery.count.limit` and
`share.record.lock.duration.ms` from the new terms. It first checks the recorded
configuration of every receiver in the pool whose membership has not ended
against the new terms, as `apply --receiver` checks a new receiver, and refuses
the terms when a plugin profile no longer fits, naming each receiver, plugin
and profile and the reason. It sets the pool's topics before it records the terms, so a failure leaves the
old terms in force, and never lowers their `retention.ms`: runs submitted
under a longer `start_by_limit` keep their records. Receivers read the new terms from `wherry.operator`
every 15 seconds, without a restart; a slow or failed read delays the update. A lowered
`delivery_limit` applies to a task in flight at its next release.

With a pool of that name and the terms recorded, it completes the pool,
creating any missing topics, the missing signature topic of each recorded
profile of each receiver whose membership has not ended included, before its
no-update path, as after an `apply` that failed once it recorded them. When the
task topics'
[record limit](configuration-operator.md#record-limit) has changed since, it records
the terms again with the new limit.

With `--dry-run`, it makes the same checks, among them the stranded
receivers, the lock duration ceiling, the record limit and the partitions of
an existing task or events topic, prints nothing, and creates and writes
nothing in Kafka; it measures a tenant topic below 2 MiB as `apply` would
set it. Terms it refuses fail in the same way with or without
`--dry-run`.

#### `apply --receiver RECEIVER`

Reads the [receiver configuration](configuration-receiver.md#receiver-configuration)
from `PATH` or `receivers/<receiver>.yaml`, a directory or a single file, and
validates it against the pool's terms. Its `pool` names the receiver's pool;
its `plugin_sha256` pins, for each plugin type, the SHA-256 of that type's
binary as lowercase hex: the line for the binary in the plugin release's
signed `checksums.txt` ([Install plugins](operations-receivers.md#install-plugins)).
Every plugin type in the configuration needs a pin, and a pin that is not 64
lowercase hex digits is an error; apply refuses such a file and records
nothing. Its `broker` is the address the receiver's bundle
names, the broker in `operator.yaml` when it is empty.

The [guided path](#operator-commands) asks, in order:

1. The pool, picked from the pools the operator has recorded, or another
   name. An existing receiver's pool is listed first and selected.
2. The plugin type, picked from `nomad` (selected), `kubernetes` and
   `systemd`, or another name.
3. The plugin's pin, 64 lowercase hex digits.
4. The plugin's `config` as a YAML flow mapping (default `{}`). A blank
   answer, `null` or `~` writes `config: {}`. A null at any depth, and a
   `credential` that is not a valid name, are refused.
5. One plugin profile at a time, at least one. Each asks for its agent,
   picked from the pool's recorded agents, or another name; the profile is
   named after its agent, and an agent that already has a profile is refused.
   It then asks for labels one at a time as `KEY=VALUE`, a blank answer
   ending the list; a key the profile already has is refused.
6. The broker address the receiver connects to (default the operator's),
   checked as `operator init` checks its broker.

The guided path shows why it refuses a value on the same page, before it
writes the file; you correct the value and continue. Apply still checks the
written file, and a refused apply leaves it to correct.

Before recording, you provide the receiver's [local configuration](configuration-receiver.md#generate-or-provide-the-local-configuration).
Wherry uses `PATH/config.yaml` with `--file DIR` when present.
Otherwise it uses `receivers/<receiver>.config.yaml` in your working directory.
If absent, it asks in a terminal and writes that file with mode `0600`.
Without a terminal, it fails naming the file and its fields.
It checks each merged profile's credential source before provisioning.
A provided local configuration is never rewritten.
With `--dry-run`, standard error names the local file or says apply will ask.
An absent local file does not prevent a dry run.
With `--force`, it reissues the identity without reading local configuration.
The local configuration questions come after the receiver's file is written.
Ctrl-C or end of input there writes no local configuration and records no
receiver, but keeps the receiver's file.

With no receiver of that name, it records the configuration, creates the
receiver's principal, key topic and progress topic,
`wherry.receiver.<receiver>.progress`, and the signature topic of each of its
profiles under the pool's recorded terms, with the task topics'
[settings](#wherry-operator-apply), grants its ACLs, and writes its
bundle to `bundles/receivers/<receiver>.bundle`, printing `wrote <bundle>`
and the next step: the bundle goes to the receiver's platform, whose
install guide's bundle step installs it, and the receiver starts. It creates every topic before
it appends the record, so a create that fails leaves no record. The file must name a
`pool`. It creates the progress topic of every other live receiver in the
pool that lacks one, and sets the
[progress topic configuration](design.md#topics-1) on each existing one, never lowering its `retention.ms`.
The bundle holds the receiver's Kafka identity alone; the receiver's machine
trusts the broker's certificate through its trust store. The receiver's [local
configuration](configuration-receiver.md#receiver-local-configuration) gives the
sources of its [platform credentials](configuration-receiver.md#platform-credentials).
With `--dry-run`, it makes the same checks and prints each plugin profile as
the record would hold it, with the pins, in the format of `show`, and creates and
writes nothing.

With a receiver of that name, it creates the signature topics of the new
configuration's profiles under the pool's recorded terms, then records the
new configuration in the receiver's pool. The file's `pool` may be empty and
otherwise must name that pool. Tasks on a signature the new configuration no
longer has wait unless another member shares it. The record holds only the pins the file gives, so give one for every
plugin type. The principal and password stay as they are. When the file's
broker address differs from the receiver's bundle in `bundles/receivers/`,
it rewrites the bundle with the new address and the same password, so a new
address reaches the receiver without a new password, and prints
`refreshed <bundle>` with the next step: install it on the platform and
restart the receiver. Without that bundle it records nothing and fails, naming the
missing bundle and that `--force` gives the receiver a new password and
bundle; a kept bundle carrying the CA earlier operators wrote is refused
the same way, naming the key and `--force`
([Upgrade](upgrade.md#broker-trust)), and one whose principal is
another receiver's is refused before anything is recorded or rewritten,
naming both principals.
When the receiver reads a record that differs
from the configuration it runs, it stops claiming, lets each run it holds
end and commit with no time limit, then exits, and its platform restarts it.
To apply a change at once, restart the receiver through its platform, as
with `kubectl rollout restart`, `nomad alloc restart` or `systemctl
restart`: it stops within its `drain_timeout` and its held runs are
released to other receivers.

With `--dry-run`, it reads and validates the configuration as it does to
record it, against the receiver's pool's terms, and prints the resolution
in the format of [`show`](#wherry-operator-show),
writing nothing. A configuration it refuses fails in the same way
with or without `--dry-run`.

With `--force`, it [reissues](#operator-commands) an existing receiver's
identity and writes its new bundle, naming the broker of
`receivers/<receiver>.yaml` when that file names one, else that of the
receiver's existing bundle, else that of `operator.yaml`. It does not change the
receiver's configuration record, and takes no `-f` or `--dry-run`.

### `wherry operator add`

```sh
wherry operator add --tenant TENANT
wherry operator add --host HOST --tenant TENANT
```

`add` records a new tenant, or a new host of a live tenant. Entitling the
tenant to a pool is [`allow`](#wherry-operator-allow): a tenant cannot be
added twice, so `add` does not take `--pool`.

#### `add --tenant TENANT`

Records a new tenant and creates its feed and transcript topics at 2 MiB per record batch and its trace topic `wherry.traces.<tenant>` at 1 MiB ([Tracing](tracing.md#topics-and-grants)). Refuses a tenant that exists or has ended: a tenant name can never return.

#### `add --host HOST --tenant TENANT`

Records a host of an existing live tenant with a new enrolment ID, creates its principal and key topic, grants its ACLs,
applies its quotas, and writes its bundle to `bundles/hosts/<tenant>/<host>.bundle`, naming the broker in
`operator.yaml` and carrying the tenant's S3 key pair as `storage` when
[`storage`](#wherry-operator-storage) recorded one. It
prints
`wrote <bundle>; hand it to the host, where wherry host init <file> installs it`.
It refuses a host that exists with `host exists`, an existing bundle file, a
host whose removal has not finished with `host is being removed; run remove
--host again`, and a host whose membership ended. To give an existing host a
new password and bundle, or to repair a host add that failed after its record
was written, run [`revoke`](#wherry-operator-revoke). Host names are unique
within a tenant; hosts of the same name in other tenants have bundles of
their own. A storage keys file that cannot be read refuses the add before
anything changes in Kafka.

### `wherry operator remove`

```sh
wherry operator remove --tenant TENANT
wherry operator remove --host HOST --tenant TENANT [--timeout DURATION]
wherry operator remove --receiver RECEIVER [--timeout DURATION]
```

`remove` ends the membership of the object its resource flag names:
[revocation](concepts.md#revocation) for a host or a receiver, and for a
tenant every entitlement, every host, then the tenant. `remove --host` and
`remove --tenant` hold the [operator lock](operations-operator.md#concurrent-operator-commands)
from before their first read to their exit, waits included; `remove --receiver`
takes no lock.

#### `remove --tenant TENANT`

Ends every entitlement, deletes live hosts' quotas, revokes the tenant's hosts, then ends the tenant
and removes `bundles/hosts/<tenant>/`. Each host revocation waits for its broker-derived minimum;
removing a tenant with hosts takes time. Record appends and final convergences are sequential;
hosts' revocation steps 2-5 run in parallel. Re-run after failure to continue, including on a
revoking host. On an ended tenant, converges and retries bundle directory removal. Refuses an absent
tenant. Removal revokes access; retained data and receiver grants remain. There is no `--timeout`
option on tenant removal.

Tenant command states:

| Command | Absent tenant | Live tenant | Ended tenant |
| --- | --- | --- | --- |
| `add --tenant` | Create | Refuse: exists | Refuse: final |
| `allow` | Refuse; use `add --tenant` | Add or converge and apply quotas when already entitled | Refuse |
| `deny` | Refuse | End or converge and apply quotas when not entitled | Refuse |
| `storage` | Refuse | Record the pair in the working directory | Refuse |
| `add --host` | Refuse | Add; refuse live, revoking or ended hosts | Refuse |
| `revoke` | Refuse | Give a live host a new password and bundle; refuse revoking or ended hosts | Refuse |
| `rotate --scram` | Refuse | Ask a live host for a credential request; refuse revoking or ended hosts | Refuse |
| `rotate --storage` | Refuse | Rewrite a live host's kept bundle; refuse revoking or ended hosts | Refuse |
| `remove --host` | Refuse | Revoke; retry from its record; converge when already ended | Refuse |
| `remove --tenant` | Refuse | End entitlements, revoke hosts, end tenant, remove bundles | Converge and retry bundle removal |

#### `remove --host HOST --tenant TENANT`

Runs the [revocation](concepts.md#revocation) steps for the host, as
[`remove --receiver`](#wherry-operator-remove) does for a
receiver, and once they are complete deletes the host's bundle,
`bundles/hosts/TENANT/HOST.bundle`. Its first step closes, void, an approval
of the host's [credential request](#wherry-operator-approve) that an `approve`
left unfinished; it does not wait for one.

#### `remove --receiver RECEIVER`

Runs the [revocation](concepts.md#revocation) steps. It prints one line per
step as the step ends:

```
step=<n> complete=<bool> name=<step name>
```

The line of a failed step ends in ` error=<cause>`. It stops at the first
incomplete step and fails with `revocation incomplete`. Before step 5 waits,
it prints when the wait ends, in UTC:

```
step=5 started name=wait until=<RFC 3339 time>
```

Once the revocation is complete, it deletes the receiver's bundle,
`bundles/receivers/RECEIVER.bundle`: its principal can never log in again.
An incomplete revocation keeps the bundle, and a rerun deletes it once it
finds the principal ended; a bundle already gone is no error. When the
deletion itself fails, the command fails naming the file, with the
revocation complete. The probe asks every
broker for the metadata of the principal's own key topic, and step 3
completes when every broker answers `TOPIC_AUTHORIZATION_FAILED`. While a
broker still authorises the principal, step 3 probes again every second for
up to 2 minutes, and retries a refused login within the same 2 minutes. It
fails at once when the probe returns any other error. Step 5 waits the largest of
`connections.max.reauth.ms`, `group.share.max.record.lock.duration.ms` and
`transaction.max.timeout.ms` across the brokers, and fails when one of them
is unbounded. `--timeout DURATION` lengthens that wait and cannot shorten it.
The command refuses a `--timeout` of zero or less before it revokes anything,
and step 1 refuses a duration below the minimum, naming it, before it
records anything.

### `wherry operator allow`

```sh
wherry operator allow --tenant TENANT --pool POOL
```

Entitles an existing live tenant to an existing pool and grants the tenant's hosts and the pool's receivers their ACLs. An ended entitlement can be added again. A live entitlement converges, applies quotas and reports `already entitled` with exit status zero. It refuses when the pool's records do not fit the
[record limit](configuration-operator.md#record-limit) with the tenant's feed and
transcript topic counted, or when the `retention.ms` of either is below the
pool's `start_by_limit` plus six hours, and records nothing. It applies
each host's [quotas](configuration-operator.md#quotas).

### `wherry operator deny`

```sh
wherry operator deny --tenant TENANT --pool POOL
```

Ends the entitlement immediately and converges access grants. Recomputes each live host's minimum
quotas across its remaining pools, deleting quotas when none remain. An absent or ended entitlement
converges and applies quotas again, reports `not entitled`, and exits zero. Both pool commands
refuse absent tenants (use `add --tenant`) and ended tenants.

### `wherry operator suspend`

```sh
wherry operator suspend --pool POOL
```

Records a `suspend` record for the pool on `wherry.operator` and reconciles the
ACLs. A suspended pool's tenants' hosts hold no `WRITE` on its task-topic
prefix. The receivers keep `READ` on it, and every other ACL stays. A host refuses a
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
[`wherry operator remove --receiver`](#wherry-operator-remove)
stops a receiver. A key custodian that drains while the pool is
suspended does not scan the task topics or re-key any task. It keeps its
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
to standard error every 10 seconds, `<time>` being the time left of the 5
minutes, then prints
`pool <pool> is suspended: its key custodian acknowledged the suspend record at wherry.operator offset <offset>; no new start opens`.
After that no new start opens. With no acknowledgement it fails with
`pool <pool>'s suspend record is written at wherry.operator offset <offset>, but its key custodian did not acknowledge it within 5m0s; it is not yet in effect`.
When the pool has no snapshot it returns at once with
`pool <pool>'s suspend record is written at wherry.operator offset <offset>; the pool has no key custodian yet, so nothing opens until one reads it`.
A custodian whose advertisement has gone stale is still awaited, since it can still open requests.

### `wherry operator resume`

```sh
wherry operator resume --pool POOL
```

Records a `resume` record for the pool on `wherry.operator` and reconciles the
ACLs, granting the hosts' `WRITE` on the task-topic prefix again. Receivers claim
from the pool again at their next refresh of `wherry.operator`. The key
custodian serves first starts from the pass that acknowledges the record
with `resumed_for` in its snapshot, and the command waits for that
acknowledgement as `suspend` does, printing
`pool <pool> is resumed: its key custodian acknowledged the resume record at wherry.operator offset <offset>; first starts open again`.

### `wherry operator rotate`

```sh
wherry operator rotate --pool POOL [--suspect-key KID]...
wherry operator rotate --tenant TENANT --host HOST --scram
wherry operator rotate --tenant TENANT --host HOST --storage
```

`rotate` replaces a secret in use: a suspended pool's key, with `--scram` a
host's Kafka password, or with `--storage` the S3 key pair in a host's kept
bundle. A host takes `--scram` or `--storage`, and an empty `--suspect-key` is
refused.

#### `rotate --pool POOL`

Refuses a pool that is not suspended with `pool <pool> is not suspended; run
wherry operator suspend --pool <pool> first`. Otherwise records a `rotate-key`
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
to standard error every 10 seconds, `<time>` being the time left of the 5
minutes. With none, it fails with
`pool <pool>'s key custodian published no snapshot rotated for the rotate-key request at wherry.operator offset <offset> within 5m0s; the request stands`.
It then lists the pool's task topics once, freezing that set as the report's
topics, captures each one's end offset, reads each as `wherry.operator`
through that offset, and lists every retained task record whose submit
wrap's `kid` is one of the suspect keys. The listing is an inventory of the
topics that existed when the scan started, not a proof that no other was
created after it. Without
`--suspect-key` the suspect key is the key the current snapshot names when
the command starts; the command refuses when the pool has no snapshot or its
current snapshot does not verify. A task record is listed whether or not its
signature verifies. The output names runs, tenants and credentials, and
holds no credential value:

```
pool <pool>'s key custodian rotated for the rotate-key request; tasks appended through that rotation are listed
suspect_keys=<kid>[,<kid>...]
horizon topic=<topic> oldest_retained_offset=<offset>
horizon topic=<topic>: retention deleted the tasks at offsets <offset> to <offset> during the scan; they may not be listed
horizon: the oldest retained task was appended at <time>, <age> ago; tasks deleted by retention before it are not listed
horizon: the task topics retain no task; tasks deleted by retention are not listed
task run=<run> tenant=<tenant> key=<kid> credentials=<name>[,<name>...] appended=<time>
tasks=<n>
```

Each topic's horizon, one line per listed topic, is its log start listed
after the scan, since a fetch below the log start begins at the start then in
effect. Where retention moved a topic's start during the scan, a second
`horizon` line for that topic names the offsets it deleted, from the start
listed before the scan. The last `horizon` line is the first form when the
topics retain a task and the second when they retain none.

The credential names are the task's own credentials and every credential
any terms record of the pool on `wherry.operator` gives the task's agent,
whatever the task's `supplied_credentials` names, so a task written before
v0.1.2 is covered too. The list can name a credential the envelope did not
hold; it never omits one it did, so rotating every listed credential covers
the exposure.

#### `rotate --tenant TENANT --host HOST --scram`

Asks a live host for a new Kafka password: it records a `rotate` record for
the host on `wherry.operator` and prints nothing. The host's next command of
any kind, once connected, files a [credential request](#wherry-operator-approve),
or sends its pending one again, as
[`host rotate --scram`](#wherry-host-rotate) does; a request decided after the
record, approved or rejected, answers it. The password changes only when you
approve that request. It takes no lock. It refuses a tenant or host that does
not exist, is revoking or has ended.

#### `rotate --tenant TENANT --host HOST --storage`

Rewrites the kept bundle, `bundles/hosts/<tenant>/<host>.bundle`, of a live
host with the broker in `operator.yaml` and the tenant's S3 key pair as
`storage`, keeping the principal and password. A new broker address or a
rotated pair so reaches the host without a new password. It prints `refreshed <bundle>` with the
next step when the bundle changed, or `<bundle> is current`. A rerun
changes nothing, so a refresh interrupted between a tenant's hosts is run
again for each of them. Hand the refreshed bundle to the host as the first
was. Its user runs `wherry host init BUNDLE`, which replaces the host's
copy. It appends no record and changes nothing in Kafka. It refuses a
tenant or host that does not exist, is revoking or has ended; a missing
bundle, or a kept one carrying the CA earlier operators wrote
([Upgrade](upgrade.md#broker-trust)), naming
`revoke --host HOST --tenant TENANT`, which reissues the host with a new
password; and a bundle that is another principal's. It takes no lock: do not
run it beside a `revoke` of the same host, whose new bundle it can replace with
the one it read.

### `wherry operator approve`

```sh
wherry operator approve ID
wherry operator approve --all
```

A host asks for a new Kafka password with a credential request, which
[`host rotate --scram`](#wherry-host-rotate) and
[`host init`](#wherry-host-init) write to `wherry.requests`. The request holds
the salt, iterations and salted form of a password the host generated, signed
with the host key, and names the credential ID it replaces; the password
itself stays on the host. `approve ID` approves the request with that ID, and
`approve --all` every pending request.
[`list requests`](#wherry-operator-list) shows what is there.

`approve` holds the [operator lock](operations-operator.md#concurrent-operator-commands)
from before its first read to its exit. It:

1. reads `wherry.requests` and checks each record on its own: a record that
   does not decode, does not verify against the current key of the host it
   names, or names another host than the one that signed it is dropped and
   counted as `bad-signature`;
2. finishes each approval an earlier `approve` began and did not finish;
3. records `expired` for each request no pass decided within 7 days of its
   first copy under the host's current key;
4. for each request it is asked for that is pending, with a salt of 16 to 64
   bytes, a 64-byte salted password and 4096 to 16384 iterations, records that
   its approval has begun, sets the salted password as the host's
   SCRAM-SHA-512 credential, records the decision and prints `approved <id>`;
5. deletes the decided requests from `wherry.requests`, keeping the undecided
   ones and the copies of an open approval; undecided requests stay until a
   pass decides them or records them expired.

A request whose credential ID is no longer the host's is `stale` and is never
approved; the host files a new one. `approve ID` of a request that is not
pending prints `request <id> is <state>`, or `no request <id>` for an ID the
topic does not hold, and exits non-zero; of an ID two hosts' requests share it
names both hosts and does nothing.

When setting the credential fails, the approval stays open: the pass goes on
with the other requests, deletes the decided ones and exits non-zero. An error
the broker returns for the user is printed alone, and changed nothing. A
timeout, a lost connection or an interrupt prints the error and:

```
approval of <id> is still open; the upsert may still be applied, at any time and over a later approve or revoke of this host; run approve again, or, if the host is in doubt, revoke it
```

When the decision record is refused, the pass prints the refusal and
`run revoke for this host again`, and exits non-zero. That happens only when a
`revoke` or `remove --host` ran from another copy of the operator directory
meanwhile, which the lock does not see.
[Approve a host's credential request](operations-operator.md#approve-a-host-s-credential-request)
says what to do after each.

### `wherry operator reject`

```sh
wherry operator reject ID
```

Records the request with that ID rejected, prints `rejected <id>`, and deletes
decided requests as `approve` does. It holds the
[operator lock](operations-operator.md#concurrent-operator-commands). It
refuses an approval that has begun with `approval of <id> has begun; run
approve to finish it`, and does nothing for an ID two hosts' requests share.

### `wherry operator revoke`

```sh
wherry operator revoke --tenant TENANT --host HOST
```

Gives a live host a new random Kafka password and a new enrolment, and writes
its kept bundle, `bundles/hosts/<tenant>/<host>.bundle`. Use it when a host's
password may be known to someone else, to replace a lost bundle, to change the
broker address a host uses, to repair a host add that failed after its record
was written, and to give a host enrolled before host credential rotation its
credential ID. To keep a host out, [remove](#wherry-operator-remove)
it instead ([Security](security.md#host-credential-rotation)).

`revoke` holds the [operator lock](operations-operator.md#concurrent-operator-commands)
from before its first read to its exit, its wait included. It:

1. checks that `connections.max.reauth.ms` is bounded on every broker, before
   it changes anything;
2. records that the host's credential is reset: from then no request of the
   host can be approved, and an approval an `approve` left unfinished is
   closed, void;
3. applies the host's quotas;
4. sets a random password on the host's principal;
5. logs in with it on every broker the metadata lists, retrying a refused
   login for up to 2 minutes;
6. waits `connections.max.reauth.ms`, the largest across the brokers, so that
   no session opened with an earlier password remains;
7. logs in with it on every listed broker again;
8. records a new enrolment ID;
9. writes the kept bundle and prints:

```
wrote <bundle>; hand it to the host, where wherry host init <file> installs it
a session opened with an old password during the wait can last up to <duration> more
```

The host is offline until its user runs `wherry host init` with the new
bundle, which files a credential request at once. A refused login at step 5
after 2 minutes, a broker a login cannot reach, and any other failure end the
command on the error, as `probe incomplete: <error>` for a login. A login
refused at step 7 ends it with:

```
the password revoke set is refused after its wait: an upsert from an earlier command was applied after it, and an old password may work; run revoke again
```

None of these records the enrolment or writes a bundle. Run `revoke` again: a
run starts from step 1 with a new password, whatever an earlier one left.
[Revoke a host's password](operations-operator.md#revoke-a-host-s-password)
says what each outcome means.

### `wherry operator storage`

```sh
wherry operator storage --tenant TENANT
printf '%s\n%s\n' "$ACCESS_KEY" "$SECRET_KEY" | wherry operator storage --tenant TENANT -
```

Records the S3 access key and secret key the store's administrator issued
for a live tenant, once, in `storage/<tenant>.yaml` under the working
directory, mode 0600 in a directory mode 0700, and adds `storage/` to the
directory's `.gitignore`. The pair goes into every host bundle of the tenant
that `add --host`, `revoke` or `rotate --storage` writes from then
on ([S3 storage](operations-hosts.md#s3-storage)), and into no Kafka record.
It prints the file and, for each live host of the tenant, the `rotate
--storage` that puts the pair into its kept bundle.

On a terminal it asks for the access key, then the secret key, each with a
masked input that prints nothing back and no answer line, as `operator init`
asks for the super user's password. With `-` it reads both from standard
input, as a password manager pipes them: exactly two non-empty lines, the
access key then the secret key, with or without a final line break. Both
keys are checked before anything is written, and the file is replaced
atomically, so a refusal or a failed write keeps the pair on file. Ctrl-C at
either question writes nothing. No value reaches an error, the output or a
record. It refuses:

| Refused | Message |
| --- | --- |
| standard input that is a terminal, with `-`, before it is read | `standard input is a terminal; leave out - to be asked for the storage keys, or pipe them in, the access key then the secret key, one per line` |
| standard input holding other than two non-empty lines | `standard input must hold exactly two lines: the access key, then the secret key` |
| no terminal and no `-` | `storage has no terminal to prompt on; pipe the access key and the secret key in, one per line, with -` |
| a key that is empty or holds whitespace or a control character, which would sign every request wrong | a fixed message naming which key |
| a tenant that is absent or has ended | `tenant absent; use add --tenant first` or `tenant has ended` |

A changed pair reaches a host only through its bundle: run `rotate --tenant
TENANT --host HOST --storage` for each host and redistribute, as
[Rotate a tenant's storage keys](runbooks.md#rotate-a-tenant-s-storage-keys)
walks through. `remove --host` and `remove --tenant` leave the pair where
it is; the store's administrator disables it.

## Receiver commands

### `wherry receiver run`

```sh
wherry receiver run [--config-file FILE]
```

Runs a receiver as the principal of the bundle its [local
configuration](configuration-receiver.md#receiver-local-configuration) names,
verifying the broker's certificate, and its pool's bucket, against the
machine's trust store plus the configuration's `ca_cert`. A configuration or
bundle keeping a CA setting of earlier releases is refused naming the key
([Upgrade](upgrade.md#broker-trust)). When neither the trust store nor
`ca_cert` trusts the broker's certificate, the failure says so and names the
configuration: with no `ca_cert` set, install the CA that signed it in the
machine's trust store or set `ca_cert` there; with one set, that the
certificate is signed neither by the trust store nor by the CA in `ca_cert`
([Broker and S3 trust](security.md#broker-and-s3-trust)). Go's x509 error
ends the message.
`--config-file` defaults to `/etc/wherry/config.yaml`; without the flag and
without that file, the receiver runs [without a
file](configuration-receiver.md#without-a-file), reading its bundle where
its platform puts it, and refuses to start naming the path when nothing is
there; its hint on an unknown authority then names the new file and the
fields the file must also set. It creates the
configuration's `state_dir` with mode 0700 and keeps `signing.key`, a
private JWK with mode 0600, in it, and in `pool-keys/` one file with mode
0600 per pool key it holds: the private JWK, its creation time, the append
time of the committed snapshot first read back naming it current, and its
deadline.
On start it:

1. takes an exclusive lock on `receiver.lock` in `state_dir`, held until it
   exits, so a second receiver on the directory, or a
   [`receiver rotate`](#wherry-receiver-rotate), fails with
   `state directory <dir> is held by another receiver process`, and a lock
   the file system refuses fails with `lock state directory <dir>` and its
   error;
   then settles a leftover `signing.key.next` as a host command settles
   [`<host_key>.next`](#wherry-host-rotate);
2. registers its public key on its key topic when the topic is empty, and
   fails when the topic holds another key;
3. waits for its configuration record on `wherry.operator` and validates it
   against the pool's terms;
4. for each plugin profile in turn, resolves its [platform
   credential](configuration-receiver.md#platform-credentials), starts its
   [plugin](#plugins) from `plugin_dir` when no earlier profile has, and
   configures the profile;
5. reads its pool's events once, and starts advertising and claiming;
6. joins the pool's custodian group, `p-<pool>.custodian`, on
   `wherry.pool.<pool>.custodian` with its receiver ID as the static
   instance ID, and acts as the pool's key custodian while the group
   assigns it the topic's partition. A receiver whose membership ended
   leaves the group and never acts as custodian.

It logs its own lines to standard output with the prefix `receiver: `;
plugin and link lines are hclog lines named `plugin`. Plugin lines, the
plugin's raw standard error that go-plugin relays, and profile configure,
sweep and readiness errors carry the
[marker](security.md#credential-envelopes) of each credential value
delivered to a run the receiver holds, and of each [platform
credential](configuration-receiver.md#platform-credentials) value, in place of the
value. On
`SIGINT` or `SIGTERM` it stops claiming, closes its idle claims and lets
each held run go on for up to the local configuration's
[`drain_timeout`](configuration-receiver.md#drain-timeout-and-the-stop-window),
default `0s`. It then commits the pending transcript of each run still
held, releases their tasks and exits, leaving their jobs running. Those
commits, releases and acknowledgements and the key custodian's drain all end
within `drain_timeout` plus 5 seconds of the signal, so a stop takes at most
that; a run whose commit does not complete by then is left unacknowledged
for its acquisition lock to return. Set the
platform's stop window as in
[Drain timeout and the stop window](configuration-receiver.md#drain-timeout-and-the-stop-window).
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

#### Key custody

While the custodian group assigns it the snapshot topic's partition, the
receiver is its pool's key custodian: it publishes snapshots, rotates the
pool key, answers open requests every 2 seconds, and on a stop hands custody
to a successor and re-keys its tasks.
[Key custody](design.md#key-custody) gives the mechanism, and
[Pool keys and the key custodian](concepts.md#pool-keys-and-the-key-custodian)
the model. On `SIGINT`, `SIGTERM` or a configuration change, every step of
a custodian's drain ends within `drain_timeout` plus 5 seconds of the stop.

| Log line | When |
| --- | --- |
| `custodian: took over pool <pool> (WherryCustodianTakeover)` | its `takeover` snapshot is first read back; counts in `wherry_custodian_takeovers_total` with reason `takeover` |
| `custodian: fenced while assigned in pool <pool>; re-initialising to republish (WherryCustodianTakeover)` | its producer is fenced while it is assigned; counts in `wherry_custodian_takeovers_total` with reason `contested` |
| `custodian: fenced again within the session timeout in pool <pool>; standing down until assigned again` | a second fence within the group's 45-second session timeout |
| `custodian: stepping down in pool <pool>; waiting up to <timeout> for a successor` | a stop with a `drain_timeout` over `0s` |
| `custodian: no successor in pool <pool>; keeping the pool keys to their deadlines` | no successor's snapshot by the end of `drain_timeout`; it exits with its keys, and its restart continues as custodian |
| `custodian: leaving pool <pool> with no drain timeout; keeping the pool keys to their deadlines` | a stop with `drain_timeout` at `0s`; its successor publishes a `takeover` |

A snapshot commit with an unknown outcome counts in
`wherry_ambiguous_commits_total`. The custodian's suspended-pool drain logs
the line [`suspend`](#wherry-operator-suspend) shows.

### `wherry receiver rotate`

```sh
wherry receiver rotate --key [--config-file FILE]
```

Replaces the receiver's signing key while the receiver is stopped. It reads
the local configuration as `run` does, connects as the bundle's principal and
takes the lock `run` holds on `state_dir`, so beside a running receiver it
fails with `state directory <dir> is held by another receiver process` and
changes nothing, and a receiver started beside it fails the same way. It
refuses a receiver that has never started, whose key topic is empty, with
`receiver has never started, so it has no key to rotate: its first start
registers one`, and a state directory whose key is not the key topic's
current key, as a restored backup or the wrong volume holds, with `receiver
key topic holds another key than the state directory`; neither writes
anything. A rotation Kafka refuses for want of a grant fails with `denied
rotate key` and Kafka's error. Otherwise it settles a leftover `signing.key.next`, writes
the new key to `signing.key.next`, publishes a `rotate`
[key record](a2a-extension-v1.md#key-records) signed by the old and new keys
to the receiver's key topic, renames the file over `signing.key` as
[`wherry host rotate --key`](#wherry-host-rotate) does, and prints
`old_key_id=<id> new_key_id=<id>`. The procedure is stop, rotate, start:
[Rotate a receiver key](operations-receivers.md#rotate-a-receiver-key).

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
[`apply --receiver`](#wherry-operator-apply) reads). The receiver
refuses a plugin whose type has no pin, with
`plugin type <type> has no pinned SHA-256; set plugin_sha256.<type> in the receiver's file and run apply --receiver`,
and a binary whose SHA-256 differs, exiting with
`profile <name>: plugin type <type>: <path> has SHA-256 <hex> and the pin is <hex>`.
A type with no binary in the directory is a configuration error, and the
receiver exits with `profile <name>: no plugin for type <type>`. A plugin checks its profile's
`config` section when the receiver calls its `Configure`, and the receiver exits with
`profile <name>: ...` on an error.

A plugin inherits the receiver's environment. The receiver passes each
profile's name, pool, agent and merged `config` section to the plugin, less
the `credential` key, and the value of the profile's [platform
credential](configuration-receiver.md#platform-credentials) when it names one. A
profile with a `credential` name its local configuration has no source for
fails its configuration with an error naming the profile and the
credential.

[Plugin protocol](plugin-protocol.md) specifies what a plugin implements and
how it talks to the receiver. The receiver refuses a plugin of another
plugin protocol version at its start, as for a version 9 plugin,
`incompatible API version with plugin. Plugin version: 9, Client versions: [10]`,
so the receiver and its plugins upgrade together. It refuses a plugin that
offers no TLS certificate, and logs a Dispatch over 64 MiB as
`dispatch rejected: message too large`. A plugin that cannot deliver an
input reports it, and the run ends `TASK_STATE_FAILED` naming it.

Each advertisement gives every profile's sweep and readiness call one
advertise interval, 30 seconds; a call that runs out counts as not ready
and is logged. Each offer carries the profile's declared inline input limit;
negative limits are invalid.

Each scrape of `/metrics` calls every profile's `Metrics` at once, each within
2 seconds, and the plugin answers with samples of the `wherry_plugin_`
families ([Metrics](plugin-protocol.md#metrics)). The receiver drops a
sample, and counts it in `wherry_metrics_dropped_total` by reason, when:

- its family is not one of the four (`unknown_family`);
- it has a label key the family lacks (`unknown_label`);
- a label value is outside the family's set (`label_value`);
- its kind is not the family's (`kind`);
- its value is not finite, or a counter is negative (`value`);
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

A plugin's `Follow` may fail at any time, and the receiver recovers the
stream ([Follow](plugin-protocol.md#follow)). When `Follow` fails, the receiver asks
`Lookup` for the run's job and logs
`run <id>: follow failed (<class>); following again from offset <offset>`
once per run for each class. A running job (`job running`) it follows
again from the end of the last line it took, as it does a lost plugin link,
and a failed `Lookup` (`lookup failed`) it retries, a second apart, until the
run passes `task_ended_by`, when it ends with that bound. An ended job
(`job ended`) it follows once more from that line end. A job that
never started is a `platform` start failure. Otherwise the run fails with
`log stream failed`.

A receiver with no ready profile advertises with no offers, so it stays
`live` with every profile `ready=false`, and placement passes over it. A
job its plugin reports never started is a `platform` start failure: the
receiver purges the job and retries it in place
([In-place retries](a2a-extension-v1.md#in-place-retries)), then returns the
task to the pool. A job that never
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

A plugin returns a failed platform call as a
[`pluginsdk.Error`](plugin-protocol.md#errors). The receiver logs a
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

### Metrics

A receiver with `metrics_addr` set serves its metrics in the Prometheus
text format over plain HTTP. `GET /metrics` is the only route; every other
path answers 404, and a method other than GET or HEAD on `/metrics`
answers 405. The scrape holds the Go runtime (`go_`) and
process (`process_`) families, `wherry_build_info` with `version` and
`revision` and value 1, and the [families](metrics.md#families), each with
the label `pool`.

[Metrics reference](metrics.md) lists the families, their labels and the
histogram buckets.

## Host commands

The host user's commands run under `wherry host`.

### Shortcuts

`wherry submit`, `wherry result`, `wherry status`, `wherry events`,
`wherry logs` and `wherry cancel` are shortcuts for `wherry host submit`,
`result`, `status`, `events`, `logs` and `cancel`, with the same arguments and
flags. No other command has a shortcut.

### `wherry host init`

```sh
wherry host init [BUNDLE] [--config-file FILE] [--ca-cert PEM|@FILE|@-]
```

Sets the host up from its bundle, and configures it for every pool its tenant
is entitled to:

1. With `BUNDLE`, it reads the bundle and copies it to the configuration's
   bundle, mode 0600, replacing the file there atomically:

   | Configuration | Where the copy goes |
   | --- | --- |
   | the default configuration | `bundle.yaml` in the [configuration directory](configuration-host.md#client-configuration), `$XDG_CONFIG_HOME/wherry/` or by default `~/.config/wherry/`, named as `bundle` in the configuration even when it named another bundle |
   | one at another `<path>` that names a `bundle` | that `bundle` |
   | one at another `<path>` that names none | the [default bundle path](configuration-host.md#a-configuration-away-from-the-default-path), `<path>.bundle.yaml`, named as its `bundle` |

   The configuration directory is made mode 0700. Elsewhere, a directory
   init creates is mode 0700 and an existing one keeps its mode. Before
   writing anything it refuses when the bundle's destination,
   `<path>.host.key` or `<path>.state` exists and is not the file the
   configuration already names, such as another configuration or another
   host's bundle or key, naming the path and the setting (`bundle`,
   `host_key`, `state_dir`) that avoids it. A rerun on an initialised
   configuration succeeds. Explicit `host_key` and `state_dir` paths and
   the configuration directory's files are not checked. Without `BUNDLE`,
   it uses the bundle the configuration names, and refuses when it names
   none.

   A bundle of a new enrolment, one whose principal or `enrolment` differs
   from the installed bundle's, or the first, is installed whole, and
   `scram.next` beside it is removed. A bundle of the installed enrolment,
   such as one [`rotate --storage`](#rotate-tenant-tenant-host-host-storage)
   rewrote, gives its broker address and `storage`, and the installed
   password and any pending request are kept.
2. It verifies the broker's certificate against the machine's trust store
   plus the configuration's `ca_cert`, which `--ca-cert` writes: the PEM
   text, `@FILE` for a file's contents or `@-` for standard input, as every
   flag that takes a value reads it, with every CR removed, so `"$(cat
   ca.pem)"` of a CRLF file reads as the file does. An empty value, and a
   value from which Go reads no certificate, a bare path among them, are
   refused before anything is written; init dials nothing with the value
   itself, and reads the configuration back before it connects. A
   configuration or `BUNDLE` keeping a CA setting of earlier releases is
   refused naming the key ([Upgrade](upgrade.md#broker-trust)). When neither the
   trust store nor `ca_cert` trusts the broker's certificate, the failure
   says so and names the configuration: with no `ca_cert` set, install the
   CA that signed it in this machine's trust store or set `ca_cert` there;
   with one set, that the certificate is signed neither by the trust store
   nor by the CA in `ca_cert`
   ([Broker and S3 trust](security.md#broker-and-s3-trust)). Go's x509
   error ends the message, and every host command fails so.
   When the file is new or step 1 changed it, init writes the
   configuration now, before it connects.
3. It creates the host key at `host_key`, by default `host.key` in the
   configuration directory for the default configuration and `<path>.host.key`
   beside any other, a private JWK (RFC 7517) with mode
   0600, unless the file exists, creating its directory with mode 0700. It
   registers the key on the host's key topic when the topic holds none, and
   prints the key ID alone when standard input or standard error is not a
   terminal, with `TERM=dumb`, or when stdout is not the same terminal as
   standard error. Otherwise, Summary shows the registered key ID. It fails when
   the key topic holds another key. After a new enrolment it then files a
   credential request, as [`host rotate --scram`](#wherry-host-rotate) does,
   and prints `request <id> sent`: a bundle's password is for enrolment
   alone, and works only until the operator approves the request.
4. It reads every entitled pool's terms. On a terminal it asks by topic:
   Credentials, Git, Storage, then Summary. A credential whose every target
   is a [catalog variable](credential-variables.md) gets no question and no
   line: the host reads the variable where submits run. A missing
   credential's question, for any other environment target and for a file
   target, is
   labelled with its first description in sorted pool order, then the terms'
   agent order, else its name, with `(optional)` when every use is optional.
   Choose Environment variable, File or Command. Environment variable lists detected non-empty targets, then New
   variable; taking a detected target needs two Enter selections. The description
   under the question explains its scope across pools.
5. For each missing Git host, choose Anonymous over https, Credential over
   https or SSH. HTTPS uses the root prefix; set custom HTTPS path prefixes
   by editing the config. SSH requires an `ssh://` prefix for that host and
   a readable known_hosts file, stored as an absolute `host_key` path.
   Init lowercases the host, maps punctuation to hyphens and allocates
   `git-<host>` or `git-<host>-ssh`, truncating and adding `-2`, `-3` and up
   to avoid another pair's name. An unassociated existing source with the
   generated name is presumed to belong to that pair and reused. Existing
   sources are kept; Configured credential uses an existing credential instead.
6. When the bundle carries the tenant's S3 key pair as `storage`, every
   entitled pool with storage uses it and nothing is asked. Otherwise, for
   all missing entitled S3 pool mappings together, Key sources asks for
   the access and secret key sources. Both sources and all missing mappings
   are committed together; cancelling either source discards the pending
   pair. New names are `s3-access-key` and `s3-secret-key`, with numeric
   suffixes on collisions. Configured credential reuses a named source.
   Set per-pool keys by editing the config, which win over the bundle's
   pair for that pool; existing mappings are kept.
7. When answers changed the configuration, it writes it again. Summary
   shows registration and successful writes, then one line per entitled
   agent, pool by pool: `ready`, naming where each credential comes from
   (`from VAR` for a catalog variable, `from env VAR`, `from file`, or
   `from command, unchecked`, since init runs no command), or `not ready`,
   naming the variables to set where submits run, the catalog entry's page,
   each other credential without a source, and the one-line form of an
   explicit source to add to the configuration. Git hosts and S3 storage
   still missing follow.

Choose Skip to leave a request for later, or Back in a nested list to return.
In Git and Storage, Skip on a source returns to the host or S3 storage
question; choose Skip there too to leave the setting for later.
Blank Variable, File, Command or known_hosts returns; blank SSH prefix uses
`ssh://git@<host>/`. Every answer is trimmed of surrounding whitespace, so
whitespace-only input counts as blank. Tab accepts completion for paths,
command words and variable names; Up/Down and ctrl+n/p cycle suggestions.
Hidden paths can be typed explicitly.

Each question owns its input. Type after it appears: bytes typed before the
next form starts may be lost. Ctrl-C ends init with `interrupted`, losing
answers not yet written. If the terminal misbehaves after an abnormal exit,
run `stty sane`.

The configuration path is `--config-file` or `WHERRY_CONFIG`, else
`config.yaml` in the configuration directory. A configuration at any other
path, made absolute with no symlink resolved, keeps its bundle, host key
and state directory beside it, as `<path>.bundle.yaml`, `<path>.host.key`
and `<path>.state/`, so two configurations never share a host key; the
default configuration named relatively, or through a link to it once it
exists, is still the default. Explicit `bundle`, `host_key` and
`state_dir` values are kept. Writes use mode 0600 even
for existing files, with references to sources, never values. A re-run
keeps existing entries and asks only for missing settings. When standard
input or standard error is not a terminal, or with `TERM=dumb`, init asks nothing, prints the
line per entitled agent and lists missing Git hosts and entitled S3
storage, the last only when the bundle carries no pair. Stdout carries only the key
ID. Summary appears even with no entitlement, with the instruction to run
init again once the operator entitles the tenant to a pool.

### `wherry host rotate`

```sh
wherry host rotate --key [--config-file FILE]
wherry host rotate --scram [--config-file FILE]
```

Each form rotates the one credential it names.

#### `host rotate --key`

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
and any `wherry result`; they hold the old key.

Every host command that finds `<host_key>.next` folds the host's key topic
before anything else. When the current key is the next key's, it renames
`<host_key>.next` over `host_key`; when it is still the old key, it removes
`<host_key>.next`. It fails when the current key is neither.

`host rotate --key` sends no pending credential request again under the new
key. Run `host rotate --scram`, or the operator's
[`rotate --scram`](#rotate-tenant-tenant-host-host-scram) has the host do it at
its next command.

#### `host rotate --scram`

Asks the operator for a new Kafka password. The host generates a random
password, salts it with PBKDF2-HMAC-SHA-512 at 4096 iterations, writes the
password and the signed request to `scram.next` beside the bundle, mode 0600,
and sends the request to `wherry.requests`, printing `request <id> sent`. The
password never leaves the host: the request carries the salt and the salted
password, which the operator [approves](#wherry-operator-approve). While the
request is pending, `host rotate --scram` sends the same request again,
signed again when the host key has changed, and prints `request <id> is
pending; sent again`. A host with no credential ID recorded, one enrolled
before host credential rotation, is refused with `no credential ID is recorded
for this host; ask the operator to revoke and re-enrol it`.

Every host command, `host mcp` included, once connected:

- uses the requested password once it is approved, rewriting the bundle with
  it, removes `scram.next`, and prints `credential <id> approved; now in use`;
- removes `scram.next` once its request can no longer be approved, printing
  `request <id> rejected`, `expired` or `void`, or `request <id> superseded`
  when the host's credential ID has changed since it was filed;
- files a request, or sends the pending one again, when the operator asked for
  one with [`rotate --scram`](#rotate-tenant-tenant-host-host-scram).

A command connects with the bundle password, and with the requested one when
the broker refuses the first. One that connects with the requested password
while the approval is under way prints `approval of <id> is in progress` and
goes on. When the broker accepts neither, the command fails with `neither the
bundle password nor the requested one is accepted; ask the operator to revoke
and re-enrol this host`.

One process at a time changes the bundle and `scram.next`: each takes a lock
on `scram.lock` beside them, which is never removed, and reads
`wherry.operator` holding it. When that read does not finish in 30 seconds the
command fails with `could not read wherry.operator within 30s; nothing was
changed`. [Rotate the Kafka password](operations-hosts.md#rotate-the-kafka-password)
says what each message asks of you.

### `wherry host receivers`

Lists the receivers of every pool the tenant is entitled to, sorted by pool
and receiver ID:

```
receiver=<id> pool=<pool> capacity=<n> in_flight=<n> advertisement_age=<age> status=<status> [gated=<reason>]
  profile=<name> plugin=<type> agent=<agent> ready=<bool> labels=<k=v,...> [replicas=<n> in_flight=<n>] [inline_limit=<n>] [models=<m,...> default_model=<m>] [credentials=<name:env=VAR|name:file=FILE[(missing|unset|unchecked[,optional]|optional)],...>]
    [credential=<name> description="<text>"]...
```

Each credential with a description gets its own line under its profile. The
description is quoted as a Go string: `"` and `\` are escaped, and a
non-printable character appears as an escape such as `\u202e`.

`status` is `live` when the latest advertisement is dated within 120
seconds of now, either side, `stale` otherwise, `silent` when there is none,
and `ended` after the receiver's membership end. `gated`, shown only while `status` is
`live`, is why the receiver claimed no task as of its latest advertisement,
`advertisement_age` ago: the first of `suspended`, `no_ready_offer`,
`pending_run` and `draining` that held, as under
[why a run waits](#why-a-run-waits). A receiver with `gated` claims no task
even when its profiles are ready and below `replicas`. Plugin profiles, their plugins and their labels come from the
receiver's configuration record, and an agent's `models` and
`default_model` from the pool's [terms](configuration-operator.md#pool-terms) when it has them.
`ready` is true when the receiver is live
and its advertisement offers the profile. A live receiver with no ready
profile lists each profile with `ready=false`. The receiver line's `capacity`
and `in_flight` are totals over all of the receiver's profiles. A ready
profile adds `replicas`, the tasks it claims at once, and `in_flight`, the
runs it holds, both from the receiver's advertisement. A profile claims one
task per replica, so a profile at `in_flight` equal to `replicas` is full,
whatever the receiver's totals show, and a run placed on it waits. The
profiles' `in_flight` need not add up to the receiver's: a run is counted
against the profile whose replica claimed it. `inline_limit`, when set, is
the most inline input bytes the offered profile delivers. `credentials` names
the agent's credentials in the pool's terms, each with its target, the
agent's environment variable or the file in its credentials directory,
marking `missing` each this host has no source for, neither a
[source](configuration-host.md#client-configuration) its configuration
names nor a set [catalog variable](credential-variables.md), `unset` each
whose configured `env` source names a variable that is unset, `unchecked`
each with a `command` source, which this command never runs, and
`optional` each a run goes without when the host cannot resolve it, as in
`credentials=openai-api-key:env=OPENAI_API_KEY,github-token:env=GH_TOKEN(missing,optional)`.
A credential with no mark is ready: a file source, or a set variable.
Each credential with a description in the pool's terms follows its profile on
a line of its own, as in `credential=github-token description="GitHub token for gh"`.

`--json` prints a JSON array, one object per receiver:

| Member | Content |
| --- | --- |
| `receiver_id`, `pool` | strings |
| `profiles` | `{name, plugin, agent, inline_limit, models, default_model, credentials, labels, ready, replicas, in_flight}`, `credentials` holding `{name, env, file, description, optional, configured, source, variable, status}`, `env` or `file` the target, `source` `file`, `env` or `command` from the configuration or `variable` for the credential's own catalog variable, `variable` the variable an `env` or `variable` source reads, and `status` `ready`, `unchecked`, `unset` or `missing`; `replicas` and `in_flight` are 0 for a profile that is not ready; optional values are omitted when unset |
| `capacity`, `in_flight` | integers from the latest advertisement, totals over the receiver's profiles |
| `last_advertised` | RFC 3339 time of the latest advertisement; omitted when there is none |
| `status` | `live`, `stale`, `silent` or `ended` |
| `gated` | for a live receiver, why its latest advertisement says it claimed no task; omitted while it claims or when not live |

### `wherry host submit`

Resolves a placement, a pool and a profile signature, signs the task and
publishes it to the signature's task topic in the chosen pool. It prints
`run_id=<id> pool=<pool>`, followed by `unsupplied=<name,...>` when the
placed agent has optional credentials this host could not resolve, or fails
with `no matching live receiver` when no live signature satisfies the
agent, model and selector. The agent's credentials come from the sources
the [client configuration](configuration-host.md#client-configuration) names,
else from their [catalog variables](credential-variables.md) in the
submitting process; each distinct source resolves once per submit, and a
configured source that fails refuses the task, as does a required
credential, or a named optional one, with no value.

Before it publishes, it folds the host's key topic and refuses when the host
key is not the topic's current key, since receivers admit a task only under
that key. When `<host_key>.next` exists, the next host command completes the
interrupted `wherry host rotate --key`; otherwise the host was re-keyed elsewhere
and needs `wherry host init` on a re-provisioned host.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--agent` | the pool's default, then the signature with the most spare capacity: the free replicas of its ready profiles on live receivers that are claiming | agent to run |
| `--model` | the agent's `default_model` | model the agent runs, one of its `models` in the pool's terms |
| `--selector` | empty | [selector](#selectors) over plugin profile labels |
| `--prompt` | | the prompt: its text, `@FILE` for a file's contents, or `@-` for standard input |
| `--git REMOTE[@REF]` or `--git .` | | repository the agent clones: an `https` or `ssh` URL, or `user@host:path`, at a branch, tag or commit, or without `@REF` at its default branch's commit; `.` is the working directory's repository, its origin at the commit checked out |
| `--git-path` | the repository name | checkout directory, relative to the agent's working directory |
| `--git-changes bundle\|patch\|none` | `bundle` | how the agent's commits come back: as the output `changes.bundle`, as `changes.patch`, or not at all |
| `--continue RUN_ID` | | continue a run of this host with its context: from the `changes.bundle` `result` collected for a `--git` run, in that run's repository, returning the next bundle; or with any other run's collected outputs as the inputs `previous/<name>` |
| `--input FILE` or `--input DIR` | | file the agent gets in `WHERRY_INPUT_DIR` under its base name, or directory whose files it gets as `<name>/<relative path>`, hidden entries skipped; one over the inline limits goes through the pool's bucket, at most 5 GiB; repeatable |
| `--credential NAME` | | name an optional credential of the placed agent the task cannot go without, at the target the pool's terms give it: the submit fails when this host cannot resolve it; one the host resolves is delivered without being named; repeatable |
| `--credential NAME:file=FILE` or `--credential NAME:env=VAR` | | credential `NAME` from `credentials` in the client configuration, which the agent gets as the file `FILE` in `WHERRY_CREDENTIALS_DIR` or in the environment variable `VAR`; repeatable |
| `--output NAME` | | file the agent must leave in `WHERRY_OUTPUT_DIR`: the run fails without it; one over the pool's output limit returns through the pool's bucket, at most 5 GiB; repeatable |
| `--start-by` | `10m` | `task_started_by`: an RFC 3339 time, or a Go duration from now, by which the agent's job must start |
| `--end-by` | `2h` | `task_ended_by`: an RFC 3339 time, or a Go duration from now, by which the run must end |
| `--wait` | false | collect the result after publishing, as [`result`](#wherry-host-result-run-id) does, the fetch of a `--git .` run's bundle included |
| `--outputs-dir` | `<state_dir>/runs/<run>/outputs/` | where the run's outputs go; requires `--wait` |

An input's delivered name, like an output's, is a relative path of file names
joined by `/`, and no input name is a directory of another; `--input FILE`
delivers the file under its base name. `--input DIR` delivers every file
beneath the directory as `<base>/<relative path>`, `<base>` the directory's
own name as given, a trailing slash removed, so `--input docs/` gives
`docs/guide/a.md` and `--input latest`, a link to `docs`, gives
`latest/guide/a.md`; `--input .` uses the working directory's name, and
`--input /` is refused. Entries named with a leading dot, files and
directories alike, are skipped, since no input name starts with a dot, and
`submit` prints `skipped N hidden entries under DIR` on standard error. A
symbolic link, a device, a socket or a FIFO beneath the directory refuses the
submission naming it, as does a directory with no file left to send. Names
are checked before any file is read past its stat: every input's name,
across every `--input`, must be a portable path, unique, a directory of no
other and not under `previous/`, which is reserved for a continued run's
outputs, and a run carries at most 256 parts and S3 inputs, the prompt
included. The flag is repeatable and mixes with files; each file travels
inline or through the pool's bucket by size, as a file given alone does.

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

`--prompt` is required and takes its value as every flag with a value does:
the text as given, `@FILE` for the contents of a file, `~` expanded, or `@-`
for standard input, as in `generate-brief | wherry submit --prompt @-`. `@-`
is refused when standard input is a terminal, `@` alone is refused, and a
value starting with `@` always names a file, which an error names. An empty
prompt, however given, fails with `--prompt is empty`. The other `--git-` flags
need `--git`, which takes three forms. `.` is the working directory's
repository. Otherwise, when the text before the value's last `@` is a remote,
it is the remote and the rest the ref, so `https://host/a@b` clones `a` at
`b`; when it is not, the whole value is the remote, so
`ssh://git@host/repo` and `git@host:repo` need no ref. `submit` writes a
`user@host:path` remote as `ssh://user@host/path`. The
[extension](a2a-extension-v1.md#git-input) gives the rules for the remote,
ref, path and host key. `submit` refuses, before it publishes, a remote
whose host is not one of the pool's [`vcs`](configuration-operator.md#pool-terms) hosts:

```
invalid input: git remote host github.com is not in pool build's vcs hosts
```

A remote without `@REF` is pinned when `submit` runs: the task carries the
commit the remote's `HEAD` names, read with `git ls-remote --symref`, so the
run clones what the default branch held at submit. A remote whose `HEAD`
names no commit, an empty repository's, is refused with `the remote's HEAD
names no commit: name a branch, tag or commit with @REF`.

`--git .` sends the repository holding the working directory, found by its
`.git`: the URL of its `origin` remote as the configuration holds it, before
any `insteadOf` rewrite, validated and written as any remote is, and the
commit `HEAD` names, which the agent checks out. The commit must be on the
remote: equal to a branch or tag tip `git ls-remote` advertises, or an
ancestor of one the repository holds. Anything else is refused, naming the
branch, or the commit when `HEAD` is detached:

```
invalid input: commit 0123456789ab is not on https://github.com/acme/app.git: push main first, or git fetch and retry
```

A remote-tracking ref does not count, since its branch may be gone from the
remote, and nothing is fetched, so a tip the repository lacks is not
checked: `git fetch` and retry. A repository without `origin` or without a
commit is refused with a hint, and uncommitted changes are neither checked
nor sent. `submit` writes nothing to the repository: it reads it with
`git config`, `git rev-parse`, `git cat-file` and `git merge-base` with
lazy fetching off, so a partial clone fetches nothing, and asks the remote
with `git ls-remote` from a directory of its own, the only call that carries
a credential. `--git .` needs `git` 2.45 or later, refused otherwise naming
the version. The run's local record keeps the repository's top level, and
[`result`](#wherry-host-result-run-id) fetches the returned `changes.bundle`
into its branch `wherry/<run>`; a `--continue` of the run, and a
resubmission, keep the repository.

Both forms run `git` on the host, from `PATH`, and are refused without it;
a remote with `@REF` runs none. That `git` reads no system or global
configuration, no `GIT_*` or `SSH_*` variable and no credential helper,
runs no hook, file system monitor or gc, never prompts, and inherits only
`PATH`, `TMPDIR`, `LANG`, `LC_ALL` and the proxy variables; in the
repository it reads the repository's own configuration, with
`safe.directory` set to the repository alone. Every command refuses every
transport but `file`, `ext`, which runs a program, among those refused, so a
`url.<base>.insteadOf` in the repository's configuration sends no command
anywhere it can run or reach; `git ls-remote` alone is allowed `https` and
`ssh`. `git ls-remote` uses the
matched `git` entry's credential: an `https` token as a `Basic`
`Authorization` header for the user `x-access-token`, passed through the
environment, which needs `git` 2.31 or later, refused otherwise naming the
version; an `ssh` key as a `0600` file removed after the call, read by `ssh`
with no configuration file, agent or other identity, trusting the entry's
`host_key` file alone. A remote under no entry is asked anonymously. An
error names the step and the exit status, `git ls-remote failed (exit
128)`, never git's output.

The clone's credential and host key come from the client configuration's
[`git`](configuration-host.md#client-configuration) entry whose `url` is the longest prefix of the
remote. The entry's credential joins the task as a task credential the agent
gets as the file `git-credential` in `WHERRY_CREDENTIALS_DIR`, so no
`--credential` may name it as well, and `submit` reads at most one byte over
the 8 KiB bound of its host key file. A remote under no entry clones without a
credential, and an `ssh` remote under no entry with a host key is refused.
With a host key, no input may be named `known_hosts`.

`--git` adds `changes.bundle` to the expected outputs; `--git-changes patch`
adds `changes.patch` instead, and `--git-changes none` adds nothing. Beside
`--git`, `--output changes.bundle` and `--output changes.patch` are refused
with `the changes file comes back by default with --git; drop --output
changes.bundle`, as is `--output changes.patch` beside `--continue`. A
receiver never requires a git input's
changes file, since a run that commits nothing leaves none, and charges it
against the pool's `output_limit` after every other output, so a large
bundle is the output that exceeds the limit, never a declared one. On a pool
with storage, a bundle over the limit returns through the pool's bucket like
any output ([S3 lane](#the-s3-lane)). `--output` refuses `:required`, since
every declared output is required, with `outputs are always required; drop
:required`, and `:s3` with `outputs move to S3 by size; drop :s3`.

The prompt asks the agent to write the bundle after every commit and once
more before it finishes, so a run that ends early after committing, by an
exit, a timeout or a cancel, returns the bundle of its commits so far, best
effort, when the agent followed the instruction. The run's upload policy
lasts ten minutes past the run's end-by time, and a run stopped at that
bound or by a cancel uploads within those ten minutes, so this holds
for a bundle sent through the bucket too. A patch is written once, at
the end, and a run that ends before then returns none. A collected bundle is
checked as bytes, by size and SHA-256; git checks its pack when the bundle is
fetched. A run killed in the middle of an export may also return
`changes.bundle.lock`, the export's partial file, which can be ignored.

`--continue RUN_ID` starts the next round of work on a run's commits. It
reuses the run's `--git` remote, ref and `--git-path`, and takes the
credential and host key from the current client configuration, as `--git`
does. It follows the run's
[resubmissions](a2a-extension-v1.md#resubmission) to the last, and needs
that run's `changes.bundle`, which [`result`](#wherry-host-result-run-id)
records when it collects it. The new run:

- gets the bundle as the input `base.bundle`, uploaded through the pool's
  bucket, and the commands that check out the bundle's base commit, not the
  ref, fetch the bundle and check out its head;
- returns its own commits since the base as `changes.bundle` through the
  pool's bucket, so each round needs only the latest bundle. As for any git
  input, the run completes without it: an agent that stops before its fetch
  leaves none. A round that adds no commit still returns the previous
  round's bundle;
- joins the run's A2A context, so every round of a chain shares the first
  run's ID as its `contextId`, and names the run it continues in
  `referenceTaskIds`.

A run that failed, timed out or was canceled may have returned a
`changes.bundle` too. It holds the commits up to the agent's last export, not
the agent's final state, and `--continue` starts the next round from there.

`--continue RUN_ID` of a run without a git input delivers the outputs
[`result`](#wherry-host-result-run-id) collected for it as the inputs
`previous/<name>`, `previous/report.md` for the output `report.md`, each
read once from where `result` wrote it and hashed as it is sent, and joins
the run's A2A context as a git continuation does. The previous prompt and
reply do not travel; the reply arrives only when `reply.txt` was a collected
output. The new run takes no git input and asks for no output of its own
beyond `--output`; each previous output travels inline or through the pool's
bucket by size, as any input does, and over the inline limits without storage
the submission is refused as any input over them is. A run collected again
with another `--outputs-dir` is continued from the files of that collect.

`submit` refuses `--continue`, before it uploads or publishes:

- with `--git`, `--git-path` or `--git-changes`;
- with an `--input` or `--output` named `base.bundle` or `changes.bundle`,
  or an `--input` named under `previous/`;
- for a run this host did not submit under its tenant;
- for a `--git` run whose `changes.bundle` `result` has not collected or is
  missing, or whose bundle is not a git bundle with exactly one prerequisite
  and a `HEAD` ref, the form `git bundle create` gives
  `refs/wherry/start..HEAD`;
- for a `--git` run on a pool without storage, or without a storage
  credential for it: `continue needs a pool with storage and a host storage
  credential: the bundles travel through S3`;
- for a run without a git input that `result` has not collected: `run <id>
  has no collected outputs; run wherry result <id> first`;
- for a collected output that is missing, is no longer a regular file or
  cannot be read, naming it.

A bundle or a collected output whose bytes no longer hash to what `result`
verified is refused: an inline one as it is read, one uploaded to the bucket
after its upload, which is deleted. A run that continues
needs receivers and custodians of this release or later
([Upgrade](upgrade.md)).

`--input` and `--output` read everything after the first `:` in their value
as suffixes, so they refuse a path or name that holds a `:`.

Input files travel inline in the submission record under two limits. The
first is the smallest `inline_limit` declared for a profile among the chosen
pool's receivers' profiles that launch the placed agent and satisfy the
selector, read from each receiver's last advertisement, live or stale; an
ended receiver does not count, and an undeclared profile limit is zero and
sets none. The second is the pool's [record limit](configuration-operator.md#record-limit), which
`apply --pool` records in the terms: `submit` measures the signed submission
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

A task credential named as one of the agent's credentials that the run
gets, a required one or an optional one the host resolves, travels once in
the envelope and is delivered at both targets.

`submit` refuses, before it publishes, a task credential, a required
credential of the placed agent, or one `--credential NAME` names, that the
host has no source for, neither in the client
configuration's [sources](configuration-host.md#client-configuration) nor,
for a target the [catalog reads](credential-variables.md), in the
submitting process's variables:

```
invalid input: auth required: credential openai-api-key is not in this host's configuration
invalid input: auth required: credential claude-code-oauth-token has no source: CLAUDE_CODE_OAUTH_TOKEN is unset in the process that submits, and this host's configuration names no source for it
```

A required credential of the placed agent is delivered to every run. An
optional one is delivered to every run whose submit resolves it; naming it
with `--credential NAME` makes it one the task cannot go without, and
naming a required one changes nothing. The submission's
`supplied_credentials` names the agent's credentials delivered. An optional
credential the host could not resolve is named on the `unsupplied=` of the
output line; the run goes without it, and its
[prompt](configuration-receiver.md#agent-contract) names it as not
supplied. `submit` refuses a name that is not one of the placed agent's
credentials:

```
invalid input: requested credential vault-token is not one of agent claude's credentials
```

A task credential with a target is not a request: a `github-token` task
credential, such as a git entry's, is delivered at the target the task gives
it, and the agent's `github-token` reaches its own target because the host
resolves the same source for it, as for every agent credential;
`--credential github-token` makes the agent's one the task cannot go
without.

### The S3 lane

An `--input` over the inline limits, and an output over the pool's
`output_limit`, use the pool's
[S3 lane](a2a-extension-v1.md#s3-inputs-and-outputs); the route is chosen
by size, so `--input` refuses a `:s3` suffix with `inputs move to S3 by
size; drop :s3` and `--output` one with `outputs move to S3 by size; drop
:s3`. `submit` refuses an input over the limits, before it uploads or
publishes anything, when the host has no storage keys for the pool, neither
the pair its bundle carries nor a
[`storage`](configuration-host.md#client-configuration) mapping in its
configuration, or the pool's terms no
[`storage`](configuration-operator.md#pool-terms):

```
invalid input: pool build has no storage for S3 inputs and outputs
```

An output over the limit on such a run is listed `exceeds output limit`.

On a pool with storage and a host with a storage credential for it, every
`submit` resolves the credential and presigns one S3 POST policy for the
run's output prefix, `wherry/<tenant>/<run>/outputs/`, valid until the
run's end-by time plus ten minutes, which the task's envelope carries; a
credential that fails to resolve, or a store that refuses to sign, fails
the submit whatever the inputs. The receiver returns each output within the
limit on the feed and uploads each one over it under the policy, so a run
need not declare which outputs are large.

It uploads each S3 input to `wherry/<tenant>/<run>/inputs/<name>` in the pool's
bucket with one PUT, then publishes the task with each input's name, size and
SHA-256 and no URL. S3 input names are file names, distinct from the input
files' names, and the run's parts and S3 inputs number at most 256 together. S3
inputs count against neither limit. When publishing fails, `submit`
deletes what it uploaded.

`submit` resolves the agent's credentials the host supplies and the task's
credentials, presigns the S3 inputs and the policy, and encrypts them in the task's
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

With `--wait`, `submit` prints the `run_id=` line, then collects the run as
`result` with no `--timeout` does. `--outputs-dir DIR` sets the output
directory and requires `--wait`. The wait is `result`'s default bound: the
published `task_ended_by` plus the ten-minute upload grace and 30 seconds.
Downloads after the run finishes are outside that bound. On timeout it
prints a command to collect later. It exits with `result`'s
[exit codes](#exit-codes). Leaving the command does not cancel the run.

### `wherry host cancel RUN_ID`

Publishes a signed cancel of the run to its pool's events and prints
`run_id=<id> cancel=requested`. For a run this host resubmitted after it
lost its pool key, the cancel goes to the last replacement, and `run_id` names
it. A run cancelled after it lost its pool key, before this host resubmitted
it, is not resubmitted.

A receiver applies the cancel when it holds the run or claims it. Until a
receiver reports a status of the run, `status` and `result` show
`cancel requested <RFC 3339 time>; applies when a receiver claims the task`,
the time of this host's first cancel of the run, also after
`task_started_by`: a receiver that claims a cancelled run ends it
`TASK_STATE_CANCELED` without checking its bounds. Once a receiver reports a
status, the run's own statuses show the cancel.

A run takes no new input after `submit`
([Binding](a2a-extension-v1.md#binding)). To add a requirement to a run,
cancel it and submit again with the requirement in the prompt, or let it
finish and submit a follow-up that passes its outputs with `--input`.

### `wherry host result RUN_ID`

Waits for a run to finish and collects it. Without `--timeout`, `result`
waits until the run's recorded `task_ended_by` plus the ten-minute upload
grace and 30 seconds, and at least one minute from now, so a finished run's
result stays readable however late `result` runs. The grace covers one
5 GiB output at the slowest upload rate measured, 13 MB/s, which takes
about 6.6 minutes; ten minutes is about 1.5 times that. Several large S3
outputs left by one run stopped at `task_ended_by` are not covered. A run
recorded before `task_ended_by` was takes `2h10m` from its submission, the
default `--start-by` and `--end-by` together, and one this host has no
record of `2h10m` from now; the MCP `result` tool stops offering
`retry_after` at the same bound. `--timeout` sets the wait itself, longer or
shorter. `--outputs-dir DIR` sets where the
run's outputs go, by default `<state_dir>/runs/<run>/outputs/`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--timeout` | `task_ended_by` plus `10m30s`, at least `1m`; `2h10m` from the submission without a recorded `task_ended_by`, from now without a record | how long to wait for the run to end |
| `--outputs-dir` | `<state_dir>/runs/<run>/outputs/` | where the run's outputs go |

Standard output carries the result: the `output=` lines, then each part of
the status message on its own line, so the reply is last. `result` prints no
transcript; [`wherry logs`](#wherry-host-logs-run-id) reads it, and `wherry logs ID > file`
keeps it. Standard error carries errors and the diagnostics below.

The status message's parts are the agent's answer or what went wrong, as its
[outcome file](configuration-receiver.md#agent-contract) reports it, or the
receiver's reason, for every state. A part prints as its text, its JSON
data, `[url <url>]`, or `[raw <media type> <n> bytes]`. For a
[reply](a2a-extension-v1.md#the-reply) over 64 KiB, the reply chunks'
verified bytes print whole in place of the preview the message carries, and
`result` writes no file of it. Standard error then says
`reply cut at the pool's reply limit` when the reply was longer than the
pool's `reply_limit`, or
`reply rejected by this host: <reason>; printed its preview` when the chunks
do not verify, with a reason as for an output.

For a run whose placement names a model, `result` prints `model=<model>` on
standard error.

`result` writes each output that [verifies](a2a-extension-v1.md#inputs-and-outputs)
against the result, at its relative path beneath the outputs directory, and
prints a line for it:

```
output=<name> path=<path> sha256=<hex> size=<bytes>
```

For `changes.patch` it also prints `apply with: git am <path>`, and for
`changes.bundle` `fetch with: git fetch <path> HEAD`, on standard error.
`result` also records each verified output, its name, absolute path, size
and SHA-256, in the run's local record, wherever `--outputs-dir` puts it,
for [`submit --continue`](#wherry-host-submit); a `result` that cannot
record them prints their lines all the same, since the files are written,
then fails with `record outputs`. A later `result` merges the outputs it
collects into the record by name: each replaces the entry of its name, and
an entry it could not collect again stays, so a `result` into another
directory after the run's S3 outputs are deleted from the bucket keeps them
recorded where the first `result` wrote them.

For a run submitted with `--git .`, or a `--continue` of one, `result` also
fetches the bundle into that repository, whose top level the run's record
holds, as the branch `wherry/<run>`, and prints
`fetched into wherry/<run>: git merge wherry/<run>` in place of the
`fetch with` line; `<run>` is the run whose bundle it is, the last of a
resubmitted run's replacements. The working tree, the index and `HEAD` are
untouched, and `git merge wherry/<run>`, from any directory of that
repository, is the next step. The fetch is
`git -c fetch.fsckObjects=true -c maintenance.auto=false fetch --no-tags
--no-recurse-submodules <path> <head>:refs/heads/wherry/<run>`, `<head>` the
commit the bundle's header names, run as
[`--git .`](#wherry-host-submit) runs git, with the repository alone as a
safe directory, no repository above it found and no transport but `file`,
so it imports that one commit or nothing, wherever a `url.<base>.insteadOf`
in the repository's configuration sends the path, fetches nothing for a
submodule or a tag, and runs no hook; the branch is then read back and must
name `<head>`. It turns `fetch.fsckObjects` on over a repository's
`fetch.fsckObjects=false`; the check honours the repository's own
`fetch.fsck.<msg-id>` and `fetch.fsck.skipList` exceptions, which no
command line setting covers. `result` holds the run's local lock from the
check before the fetch to the one after, so two `result` calls for one run
take turns. A `wherry/<run>` that exists is created once and never
moved: at the bundle's head there is nothing to do, and `result` prints the
same line; at any other commit, an ancestor of the head included, it is left
and `result` prints `not fetched (exists); fetch with: git fetch <path> HEAD`
and exits as it would have. Any other failure prints the same line with its
reason, `no git` (none on `PATH`, or older than 2.45), `no repository` (the
recorded directory gone, or no longer a repository), `git refused` (a
prerequisite the repository lacks, a corrupt object, a rewritten path),
`unreadable bundle`, `wrong commit` (the branch the fetch made named another
commit, and was deleted) or `timeout` (the fetch, its checks included, did
not end within 10 minutes), never git's output; the collect still succeeds,
and the bundle at `<path>` is fetched by hand. A refused fetch may leave
unreferenced objects in the repository, which `git gc` removes. A run
submitted with `--git URL` records no repository, and `result` prints the
`fetch with` line alone.
No expected output takes the name `reply.txt`, and a file of that name the
agent leaves is listed as not returned, `rejected: reserved name`.

`result` reads the run's statuses and result from the tenant feed, and its
output chunks from the tenant's transcript topic, from the first offset at
or after one minute before the append time of the run's earliest echo on
the feed. Once it sees a terminal status, it reads the transcript topic to
its last stable offset, and waits until that reaches the high watermark it
lists after it sees the terminal, before it writes the outputs.

It prints `output <name> not returned: <reason>` on standard error for each
output the result lists as not returned and each one that fails
verification. A write never leaves the outputs directory, whatever links
it holds.

`result` first checks the result's output lists as a whole, before it writes
anything: at most 256 names, each returned name valid, unreserved and listed
once, none an ancestor of another, and each S3 output at its key with a
lowercase hash and a size within 5 GiB. A result that fails this is refused
whole, with `output result not returned: rejected by this host: <reason>`.
It then downloads each S3 output the result lists with the host's storage
credential, to the same place, and prints it as it prints any output. It
downloads only at the key `wherry/<tenant>/<run>/outputs/<name>`, and writes
the file only when its bytes match the listed size and SHA-256; a file
already there that matches is kept. Once the run is terminal and every
listed S3 output has verified, `result` deletes every object under
`wherry/<tenant>/<run>/` in the pool's bucket, inputs included and whether
or not the result listed it, for every run the host issued a policy for. A
run that ends while no `result` runs keeps them until a `result` on it or
the bucket's lifecycle rule removes them.

A run this host submitted whose `task_started_by` passes with no status from
a receiver can never start. `result` then collects nothing, prints nothing on
standard output, and exits at once with status 1 and
`not started by task_started_by (<RFC 3339 time>): no receiver claimed the task; now: <reason>`,
with the [waiting reason](#why-a-run-waits) as of the call, and before it
on standard error any pending cancel.
`task_started_by` comes from this host's record of the submission, so
`result` on another host of the tenant waits for its timeout.

When `--timeout` passes before a receiver reports a status of a run this host
submitted, `result` prints the waiting reason, and any pending cancel, on
standard error before it exits with status 6. For a resubmitted run they are
the last replacement's, the run `cancel` names.

A run this host submitted that ends with the wherry event `key-lost` before
its `task_started_by` never started: its pool key was lost
([Lost pool keys](concepts.md#lost-pool-keys)). `result` resubmits it under a
new run ID, or produces the replacement this host already stored, and
follows the replacement to its outcome, through every later replacement. A
run cancelled before its resubmission is not resubmitted. It prints one line
on standard error per replacement:

```
run <id> lost its pool key before it started; resubmitted as run <id>
```

The result, outputs and exit code are those of the last run followed, and
the default outputs directory is that run's. `result` refuses a chain of
replacements that names a run twice. When the resubmission fails, for
example on a suspended pool or a pool whose current snapshot is missing or
not authorised, `result`
exits with status 1 and
`result <id>: run <id> lost its pool key and is resubmittable until <RFC 3339 time>; resubmitting it failed: <error>`.

#### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | the run ended `TASK_STATE_COMPLETED` and its outputs were written |
| 1 | `result` could not wait or collect: usage, run not found, broker unreachable, outputs not written or failing verification, `task_started_by` passed unclaimed |
| 2 | the run ended `TASK_STATE_FAILED` |
| 3 | the run ended `TASK_STATE_CANCELED` |
| 4 | the run ended `TASK_STATE_REJECTED` |
| 6 | `--timeout`, or the default bound `submit --wait` shares, passed with the run still going; without `--timeout` the error names the bound, the run's `task_ended_by` plus the upload grace |

For codes 2 to 4 the error is `run ended in <state>`. A run that ends in
any other state but `TASK_STATE_COMPLETED` exits 1 with the same error.

### `wherry host status [RUN_ID]`

Reads the latest verified state of a run without waiting. With `RUN_ID` it
prints the state on the first line, `model=<model>` next when the run's
placement names one, then one line per part of the status message. The
state is `TASK_STATE_SUBMITTED` until a receiver reports one. For a run this
host submitted whose `task_started_by` passed with no status from a receiver,
the line after the state and model is
`not started by task_started_by (<RFC 3339 time>): no receiver claimed the task; now: <reason>`.
Before `task_started_by`, while no receiver has reported a status, that line
is the [waiting reason](#why-a-run-waits). A pending cancel adds
`cancel requested <RFC 3339 time>; applies when a receiver claims the task`.

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
reads the replacement, and `wherry result` on either run follows it to its
outcome.

`status` exits with status 0 whenever it reads a state, whatever the
state. It fails when the run is not found or the read fails.

#### Why a run waits

For a run this host submitted that no receiver has reported a status of, the
host says why it waits, from its record of the run's pool and signature topic
and the live advertisements of the pool's receivers. It reads them at each
call and stores nothing, so the reason is as of the call. A receiver
advertises every 30 seconds and its advertisement counts as live for 120
seconds, so the numbers can be up to 120 seconds old. The reason is
the first of:

| Reason | When |
| --- | --- |
| `queued: pool <pool> is suspended` | the pool is suspended |
| `no live profile offers the run's signature` | no live receiver offers a profile on the run's signature topic |
| `queued: <profile> on <receiver> advertises room` | a profile on the topic has a free replica and its receiver is claiming; the claim is pending, one per profile with room, comma-separated |
| `queued: <profile> on <receiver> <in_flight>/<replicas> in use` or `queued: <receiver> is not claiming (<gated>)` | every profile on the topic is full or its receiver claims nothing; one per profile or receiver, comma-separated |

`<gated>` is why a receiver claims no task: `suspended`, `no_ready_offer` (no
profile is ready), `pending_run` (a run it holds is waiting for its platform
to place it), or `draining` (it stopped claiming, after a configuration
change or a stop, while its held runs finish). Past `task_started_by` the
reason follows `now:` without `queued: `, as in
`not started by task_started_by (2026-11-12T10:00:00Z): no receiver claimed the task; now: claude on nomad-1 1/1 in use`.
It describes the advertisements at the call, not those when the bound passed.

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
| `--follow` | off | print lines as the agent prints them, until the run ends |

`logs` reads the lines from the tenant's transcript topic, from the
first offset at or after one minute before the append time of the run's
earliest echo on the tenant feed. Once the feed holds the run's terminal status, it reads the
transcript topic to its last stable offset, and waits until that reaches the
high watermark it lists after it sees the terminal, before it prints.
Without `--follow`, it prints the lines the topic holds and exits.

With `--follow`, `logs` follows the tenant's transcript topic with one consumer
from the run's echo, so each line prints as it is written, once and in
order. When the run reaches a terminal status, it reads the transcript
topic to the high watermark as above, prints the lines it has not printed,
and exits with status 0.

A run that ended with the wherry event `key-lost` never started, so it has
no transcript. `logs --follow` resubmits such a run of this host, as `submit`
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

`result`, `status`, `events`, `logs` and `cancel`
find a run from this host's record of it in `state_dir`, or from a receiver's
echo on the tenant feed. They fail with `run not found` when there is neither.

### `wherry host mcp`

Serves the [MCP tools](mcp.md) over stdio, logging to standard error.
The packaged MCP launch uses `["host", "mcp"]` and the default configuration path.
A server that runs `wherry host mcp --config-file PATH` uses another, as
[Harness integration](operations-hosts.md#harness-integration) shows.
A broker certificate the connection does not trust fails each tool call
with `UNAVAILABLE` and logs the explanation [`wherry host init`](#wherry-host-init)
gives, naming the trust store and `ca_cert` in the configuration. A certificate rotated after the server connected
logs Go's x509 error alone; a restart rereads the bundle.

## Version command

### `wherry version`

Prints the binary's release as `version=<release>` and its commit as
`revision=<commit>`, each on its own line. A release build's release is its
git tag, such as `v<version>`. A build from an untagged commit prints a Go
pseudo-version, and a build without git information prints `(devel)` and an
empty revision. A build from a working tree with uncommitted changes adds
`modified=true`. It takes no flags and reads no configuration.
