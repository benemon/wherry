# Security

How a run's credentials travel from the host to one attempt of one
receiver, what the pool key that carries them protects, where the host
takes their values from, and how they reach the agent: the agent holds each
value at its target while it runs.
[Governance](governance.md) covers identity, grants, signed records and
the audit trail.

## Credential envelopes

The agent usually needs a model credential, and each tenant's work runs on
that tenant's own credential. Every credential has a **source** on the
host and a **target** in the agent. The pool's terms list each agent's
credentials, each with its target: an environment variable or a file in
the directory `WHERRY_CREDENTIALS_DIR` names, and whether the agent
requires it. The host's [client configuration](configuration.md#client-configuration)
maps each credential name to one [source](#credential-sources). A task can
also declare **task credentials**: credentials of the host, such as a
deploy key, that the agent gets at the target the task's parent chooses.
The pool's terms do not govern them, and none may take the target of one of
the agent's credentials or of another task credential. Only the agent's run
can read its credentials, and the platform's boundary enforces it, as each
[plugin](plugins.md)'s README states.

`wherry receivers` lists each agent's credential names, marking those this
host has no source for, such as
`credentials=openai-api-key,github-token(missing,optional)`. `wherry
submit` refuses a task at once, with
`auth required: credential <name> is not in this host's configuration`,
when the host has no source for a required credential of the placed agent
or for a task credential. An optional credential the host has no source for
is not delivered, and the prompt names it as not supplied.

A host resolves each credential's [source](#credential-sources) and
presigns the run's S3 links when it submits, and encrypts them, with the run's
redaction key, in the task's **envelope**: a compact JWE (`dir` with
`A256GCM`, through [go-jose](https://github.com/go-jose/go-jose)) of at most
64 KiB under a fresh content key, bound to the task's run, tenant, pool,
`task_started_by` and `task_ended_by`. The task's **wrap** carries the
content key to the pool's current key, which the pool's key custodian holds
and publishes in its [snapshot](a2a-extension-v1.md#custodian-snapshot).
The host signs the task with both in it, and can go offline once `submit`
returns.

The pool's snapshot topic, `wherry.pool.<pool>.custodian`, holds the
custodian's signed snapshots: public keys only. The pool's receivers read
and write it, and the hosts of entitled tenants read it; no other principal
has a grant on it ([ACLs](design.md#acls)). A host takes the latest
committed snapshot alone, refuses to submit when its writer is not the
custodian it names or that custodian is not a live member of the pool, and
never falls back to an earlier snapshot.

The receiver that claims a run makes a P-256 key for that attempt and
writes an **open request** on the pool's events. The key custodian checks
the claim and answers with an **attempt wrap**, the content key encrypted to
the attempt's key, or a refusal. Only that attempt of that receiver opens
the envelope: the attempt's private key lives in the receiver's memory
until the envelope opens or the wait ends. A receiver whose request no key
custodian answers within 5 minutes and 120 seconds releases the task to the
pool. A run that never started and whose envelope no holder of a pool key
can open ends `TASK_STATE_FAILED` with the wherry event `key-lost`, and its
host resubmits it under the pool's current key until its `task_started_by`
([Lost pool keys](concepts.md#lost-pool-keys)).

The receiver redacts each credential of 8 bytes or more that it delivered,
with or without its trailing newline and as JSON escapes it, from
everything it publishes for the attempt. Each marker is an HMAC of the
credential's name under the run's redaction key, so no marker maps to a
name without that key. The receiver redacts no value shorter than 8 bytes
once its trailing CR and LF are removed, and logs
`run <id>: credential <name> too short to redact`.

An agent that finds a credential missing or refused during the run reports
`TASK_STATE_AUTH_REQUIRED` naming the credential, and the run ends
`TASK_STATE_FAILED`. The message names its credentials in `credential` data
parts, and the run is not resumed.

Everything else the agent needs comes from the platform or from the task:
the image or host install, network access, CA bundles and SSH known hosts
from the platform, and files and task credentials from the task.

## The claim check

The key custodian serves an open request only when all of these hold, and
otherwise refuses it with the reason the
[claim check](a2a-extension-v1.md#the-claim-check) names:

- the request is signed by the live member of the pool it names;
- it names the run's earliest accepted task, and the run has no terminal
  status and no cancel;
- the run's latest ownership status is the requester's `claimed` for the
  request's attempt;
- the run is within its open bound: `task_started_by` for a first start,
  `task_ended_by` once the run has started or the claimant is adopting its
  job;
- when the claim changed receivers, the previous claimant committed
  `released`, its membership ended, or its latest advertisement appended at
  or before the claim is more than 120 seconds old.

No receiver keeps a run by being its first claimant. The custodian serves a
new claimant once the previous claimant has released the task or gone
silent, and refuses it with `two live claimants` while the previous claimant
is live. It records each decision on a claim durably and reuses it, and it
opens the envelope and checks its binding before it wraps the content key,
so a spliced envelope never releases its key.

A claimant refused `two live claimants`, `not the current claimant` or
`pool suspended` releases the task to the pool; refused `terminal`, it acknowledges the task
without a run; refused `past its bound` or `envelope invalid`, it ends the
run `TASK_STATE_FAILED`; refused `key unavailable`, it decides from the run's
job as [Lost pool keys](concepts.md#lost-pool-keys) describes.
[Open refusal](a2a-extension-v1.md#open-refusal) gives each action in full.

## What the envelope binds

The envelope's `binding` holds the task's run, tenant, pool,
`task_started_by` and `task_ended_by`. The key custodian checks it against
the signed task before it wraps the content key, and the claimant checks it
again at open, so an envelope copied into another task fails:

- under another run, tenant or pool, the binding differs and the envelope
  does not open;
- under the same run while the original task is retained, the copy is not
  the run's earliest accepted task, and receivers act on the earliest alone
  ([Admission](a2a-extension-v1.md#admission));
- under the same run after retention has deleted the original, the copy must
  carry the original's bounds, which have passed, so admission rejects it.

The binding does not protect the task's other members, such as its message,
placement and outputs. They rest on the host's signature and on
earliest-accepted admission, and only the Kafka operator, which is root, can
append a record before the original. Receivers cannot read the binding at
admission: a malformed first task can win its run and then fail at open with
`envelope invalid`.

## Envelope bounds

- **Size.** An envelope is at most 64 KiB, and the signed task with its
  envelope must fit the pool's record limit; `submit` measures the actual
  record. `pool create` sizes the pool against a task with a maximum
  envelope ([Size a pool against the broker](operations.md#size-a-pool-against-the-broker)).
- **Time on the broker.** The pool's tasks and events topics keep records for
  `start_by_limit` plus six hours plus a day, and roll a segment every hour,
  so retention deletes a record within about an hour of that bound; an
  envelope's exposure is that retention plus the broker's deletion lag.
  Attempt wraps, re-key wraps and refusals live on the events topic and
  expire with it. The snapshot topic is compacted and kept forever, and holds
  public keys only.
- **Presigned links.** A host presigns an S3 input's GET until
  `task_started_by` plus 15 minutes and an S3 output's PUT until
  `task_ended_by`, each at most 7 days, the longest an
  [S3 SigV4](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)
  presigned URL lasts. For a pool with storage, `start_by_limit` plus six
  hours must not exceed 7 days. The host signs with its static storage
  credential, so the 7-day ceiling and those deadlines are the only bounds
  on a link.

## What the pool key protects

The pool remains the trust boundary: its receivers trust each other fully.
A malicious member of the pool forging claim evidence, initialising the
custodian's transactional ID, publishing a snapshot naming itself with any
reason, or withholding re-key wraps is out of scope.

| Party | What it gets |
| --- | --- |
| Kafka operators and broker storage | ciphertext only, without a pool private key |
| another tenant or pool | nothing: the platform's ACLs, TLS and SCRAM keep them apart, and only the pool's receivers can write its snapshot |
| a compromised receiver that is not the key custodian | the credentials of the runs it claims; it can become custodian through the custodian group as an honest receiver would, which raises [WherryCustodianTakeover](runbooks.md#wherrycustodiantakeover) |
| the key custodian, or anyone who copies its keys | every retained envelope under those keys, without claiming |

Daily [rotation](concepts.md#rotation) stops a silently copied key from
receiving new tasks within a day. Deleting a key at its deadline protects
old envelopes only if the key was never copied.

Against an exchange in which the submitting host answers each attempt with
the run's credentials, custody in the pool:

- lets the host go offline once `submit` returns: retries, takeovers and
  drains need no answer from it;
- keeps the envelope on the chosen pool's task topic alone;
- gives a [weaker guarantee](concepts.md#lost-pool-keys) when the
  custodian's keys are lost: never-started tasks under them fail and run
  only when their host resubmits them before their `task_started_by`, and
  hosts refuse to submit between a custodian going silent and its
  successor's snapshot;
- keeps each run's non-secret task content, and its redaction key, on the
  host, so the host can resubmit; the host keeps no credential value in
  the clear.

## Run credentialed tasks

Prerequisites: a host with its
[client configuration](configuration.md#client-configuration), entitled to a
pool whose terms list the agent's credentials.

1. Map each credential the agent takes, and each task credential, to a
   [source](#credential-sources) in `credentials` of the host's
   configuration:

   ```yaml
   credentials:
     openai-api-key:
       file: ~/.config/wherry/credentials/openai
     github-token:
       env: GITHUB_TOKEN
     vault-token:
       command: [vault, print, token]
   ```

2. Run `wherry receivers` and check that the agent lists no required
   credential as `missing`.

3. Submit the task. Give each task credential its target with
   `--credential NAME:file=FILE` or `--credential NAME:env=VAR`. `submit`
   resolves each source and refuses the task, naming the credential, when
   one fails.

4. When the run ends `TASK_STATE_FAILED` naming a credential, fix that
   credential's source and submit a new run.

## A model credential per pool

An agent runs commands, and holds every credential its run delivers while
it runs; a hostile or prompt-injected agent can send a credential anywhere
its network allows. A model credential issued for one pool's agents and
used nowhere else bounds that exposure: a leaked key reaches only that
pool's model usage, the provider's usage and limits for the key show that
pool's runs alone, and revoking it stops that pool's agents without
touching another pool or the tenant's own use of the model. A dedicated,
revocable credential per pool is an operator choice. wherry does not check
it: it delivers whatever value the host's source for the credential's name
resolves, and does not know which key that is.

The pool's terms name each agent's credentials, and a host's
[client configuration](configuration.md#client-configuration) maps each
name to one source for every pool. A credential name used by one pool alone
gives that pool its own source on every host. Each tenant's runs use that
tenant's credential, so each tenant entitled to the pool holds its own key
for it.

### Give a pool's agents a dedicated credential

Prerequisites: the [`wherry admin` environment](operations.md#set-up-wherry-admin),
the pool's terms file, and, for each tenant entitled to the pool, a model
provider account that issues and revokes API keys, such as per-project or
per-workspace keys.

1. Choose a credential name for the pool alone, such as
   `build-anthropic-api-key` for pool `build`.

2. For each tenant entitled to the pool, issue a key at the model provider
   for the pool's agents, scoped to the models and limits the pool's runs
   need.

3. On each of the tenant's hosts, map the name to a
   [source](#credential-sources) holding that key:

   ```yaml
   credentials:
     build-anthropic-api-key:
       command: [op, read, "op://wherry/build-anthropic/credential"]
   ```

4. Name the credential in the pool's terms, at the target the agent reads
   it from, and record the terms as in
   [Change a pool's terms](operations.md#change-a-pool-s-terms):

   ```yaml
   agents:
     - name: claude
       credentials:
         - {name: build-anthropic-api-key, env: ANTHROPIC_API_KEY}
   ```

   ```sh
   wherry admin pool create build --config-file build-terms.yaml
   ```

   The credential is required unless the entry sets `required: false`, so
   `wherry submit` refuses a task for the agent from a host with no source
   for it.

5. On each host, run `wherry receivers` and check that the agent lists
   `build-anthropic-api-key` without `missing`.

### Rotate a pool's credential

Prerequisites: a host of the tenant whose key rotates, and the tenant's
account at the model provider.

1. Issue a new key at the model provider for the pool's agents.
2. Replace the value at the credential's source on each of the tenant's
   hosts. `submit` resolves the source for each run, so runs submitted
   afterwards get the new key with no restart.
3. Wait for the runs submitted with the old key to end. `wherry status`
   lists each run and its state.
4. Revoke the old key at the model provider.

### Revoke a pool's credential

Prerequisites: the tenant's account at the model provider.

1. Revoke the key at the model provider. Agents still running with it get
   refused calls; an agent that reports the refusal ends its run
   `TASK_STATE_FAILED` naming the credential.
2. Remove the value from the credential's source on each of the tenant's
   hosts, or replace it with a new key as in
   [Rotate a pool's credential](#rotate-a-pool-s-credential). Until a source
   resolves, `submit` refuses runs that deliver the credential.

When a receiver is revoked, rotate each credential it could open, as in
[Revoke a host or receiver](operations.md#revoke-a-host-or-receiver).

## Credential sources

A submitting host, and a host resubmitting a run, resolves each source the
run delivers for the run's [envelope](a2a-extension-v1.md#submission). The
configuration holds no value and no source prompts.

| Source | Value |
| --- | --- |
| `file` | the file's contents as they are; a trailing newline is part of the value the agent gets, and the receiver redacts the value with and without it |
| `env` | the variable in the submitting process's environment; unset or empty fails the submission |
| `command` | the standard output of the program, run without a shell with the submitting process's environment and no standard input, byte for byte as for a file; standard error is discarded |

A `command` source runs with `WHERRY_TASK_END_BY` set to the task's
`task_ended_by` as an RFC 3339 time in UTC, in place of every copy the
process would inherit; the submitting process's own environment is
unchanged.

A command has 60 seconds, time for an interactive unlock such as a password
manager's, and at most 64 KiB of output. A command that exits non-zero, runs
out of time or writes more fails the submission, which names the
credential and the cause alone:

```
resolve credential <name>: its command exited with status 1
```

A `file` source must name a regular file that its group cannot write and
others cannot read, write or execute. Mode 0600 is the recommended mode.
Group read is accepted, so a secrets agent can template the file for a
service group at mode 0640. A file that breaks the rule fails the submission:

```
resolve credential <name>: its file is writable by its group or open to others; chmod 600 it
```

The `submit` of the [MCP server](mcp.md) refuses as an input every file
a `file` source names, including the storage keys' files.

## Platform credentials

A receiver takes the credential a plugin authenticates to its platform
with, such as a Nomad token, from a source on the receiver's host. A
plugin profile's `config` names the credential with `credential: <name>`, and the
`credentials` of the receiver's [local
configuration](configuration.md#receiver-local-configuration) map the name
to a `file`, `env` or `command` source. The receiver resolves it with the
host's implementation and under the host's rules: the same file mode rule,
the same 60-second command timeout with standard error discarded, and
errors that name no path or value. An `env` or `command` source uses the
receiver's environment. The receiver trims a trailing CR and LF from the
value and passes it to the plugin when it configures the profile, at start
and after a configuration change.

[Platform credentials](configuration.md#platform-credentials) states what
`wherry.admin` and the bundle hold of it. A rotated platform token takes
effect on the receiver's next start without a new bundle.

## Git credentials

`submit` matches a git input's remote, written as an `ssh://` or `https://`
URL, against the `url` of each `git` entry and takes the entry with the
longest matching prefix, as git's `credential.<url>` settings do. The entry's
credential joins the task as a task credential the agent gets as the file
`git-credential` in `WHERRY_CREDENTIALS_DIR`, and the contents of its
`host_key` file become the git input's host key. A remote no entry matches
clones without a credential. The pool's [`vcs`](configuration.md#pool-terms) hosts still
decide which hosts a remote may name.

The `url` prefix is the credential's scope: every task cloning under it
receives the credential, so a prefix ends with `/` and names no more than the
credential grants. Loading the configuration refuses a `url` that does not
start with `https://` or `ssh://` or repeats another, a `credential` with no
source in `credentials`, and a `host_key` that is not a readable file.

The storage keys resolve through their credentials' sources whenever the
host uploads, presigns, downloads or deletes, with surrounding whitespace
removed. Loading the configuration refuses a `storage` block missing either
name, or naming a credential with no source in `credentials`. A source that
fails names the credential and the cause:
`read storage credential <name>: its environment variable is unset or empty`.
The credential never leaves the host: receivers and agents get only the URLs it presigns.
Scope it to `wherry/<tenant>/` in each pool's bucket
([Operations](operations.md#s3-storage)).
