# Security

How a run's credentials travel from the host to one attempt of one
receiver, where the host takes their values from, and how they reach the
agent: the agent holds each value at its target while it runs.
[Governance](governance.md) covers identity, grants, signed records and
the audit trail.

## Sealed credentials

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
the agent's credentials or of another task credential.

`wherry receivers` lists each agent's credential names, marking those this
host has no source for, such as
`credentials=openai-api-key,github-token(missing,optional)`. `wherry
submit` refuses a task at once, with
`auth required: credential <name> is not in this host's configuration`,
when the host has no source for a required credential of the placed agent
or for a task credential. An optional credential the host has no source for
is not delivered, and the prompt names it as not supplied.

A host seals nothing when it submits. The receiver that claims a run
with credentials to deliver makes a P-256 key pair for that
attempt and publishes a signed **seal request** on the tenant feed: the
run and attempt, the public key, the credential names and an expiry
5 minutes out. The host that submitted the run answers it while `wherry
wait` or `wherry mcp` runs. It checks the request's signature against the
receiver's registered key, that the receiver is a live member of the run's
pool and holds the run's current attempt, that the pool's terms approve
each agent credential for the run's agent, and that the run declares each
task credential. It resolves each credential's [source](#credential-sources),
then writes a **seal** to
its own topic: a compact JWE (`ECDH-ES+A256KW` with `A256GCM`, through
[go-jose](https://github.com/go-jose/go-jose)) encrypted to the request's
key and bound in its protected header to the tenant, pool, receiver, key
thumbprint, run, attempt, credential names and expiry. A seal is at most
64 KiB, which bounds the values one seal carries. The host logs each
verified request of its own runs that it skips or refuses, by run,
receiver and credential name, on standard error.

A seal passes from the host to one attempt of one receiver in these steps.

![Credential delivery: the receiver's seal request, the host's checks and sealed answer, and delivery to the job](diagrams/credential-delivery.svg)

Only the requesting receiver can open a seal, and only for that attempt.
The private key lives in the receiver's memory until the seal opens or the
wait ends, so a seal on the host's topic opens for no later attempt and for
no restarted receiver. A redelivered run asks again, and a receiver that
adopts a running job publishes nothing of the job until its own seal opens.
Hosts cannot see the broker's claim, so a host seals only to the receiver
that wrote the run's first `claimed` status, and refuses when a second
receiver claims the same run. A run adopted by another receiver gets no
seal, so that receiver stops the job and the run ends `TASK_STATE_FAILED`
when its seal wait ends.

The receiver redacts each credential of 8 bytes or more that it delivered,
with or without its trailing newline and as JSON escapes it, from
everything it publishes for the attempt. Each marker is an HMAC of the
credential's name under a key the host makes for the run and sends in every
seal, so no marker maps to a name without that key. The receiver redacts
no value shorter than 8 bytes once its trailing CR and LF are removed, and
logs `run <id>: credential <name> too short to redact`. Seal requests and
the failed status of an unanswered seal request carry credential names and
no values.

A run whose seal does not arrive within 5 minutes ends in
`TASK_STATE_FAILED` with a message naming each credential the host did not
answer. An agent that finds a credential missing or refused during the run
ends it in `TASK_STATE_AUTH_REQUIRED` naming the credential. Both states
are final.

Everything else the agent needs comes from the platform or from the task:
the image or host install, network access, CA bundles and SSH known hosts
from the platform, and files and task credentials from the task.

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
   `--credential NAME:file=FILE` or `--credential NAME:env=VAR`.

4. Keep `wherry wait RUN_ID` or `wherry mcp` running until the task
   starts, so the host answers the receiver's
   [seal request](#sealed-credentials) within 5 minutes.

5. When the run ends `TASK_STATE_FAILED` naming unanswered credentials, or
   `TASK_STATE_AUTH_REQUIRED` naming a credential, fix that credential's
   source and submit a new run.

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
   hosts. The answering process resolves the source each time it answers a
   seal request, so runs that start afterwards get the new key with no
   restart.
3. Wait for the runs that started with the old key to end. `wherry status`
   lists each run and its state.
4. Revoke the old key at the model provider.

### Revoke a pool's credential

Prerequisites: the tenant's account at the model provider.

1. Revoke the key at the model provider. Agents still running with it get
   refused calls; an agent that reports the refusal ends its run
   `TASK_STATE_AUTH_REQUIRED` naming the credential.
2. Remove the value from the credential's source on each of the tenant's
   hosts, or replace it with a new key as in
   [Rotate a pool's credential](#rotate-a-pool-s-credential). Until a source
   resolves, seal requests for the credential go unanswered and those runs
   end `TASK_STATE_FAILED`.

When a receiver is revoked, rotate each credential sealed to it, as in
[Revoke a host or receiver](operations.md#revoke-a-host-or-receiver).

## Credential sources

The answering process, `wherry wait` or `wherry mcp`, resolves each source
when it answers a seal request, after it authorises the request and before it
takes the attempt's lock, so a slow source holds up no other answerer of the
host. The configuration holds no value and no source prompts.

| Source | Value |
| --- | --- |
| `file` | the file's contents as they are; a trailing newline is part of the value the agent gets, and the receiver redacts the value with and without it |
| `env` | the variable in the answering process's environment; unset or empty fails the answer |
| `command` | the standard output of the program, run without a shell with the answering process's environment and no standard input, byte for byte as for a file; standard error is discarded |

A command has 60 seconds, time for an interactive unlock such as a password
manager's, and at most 64 KiB of output. A command that exits non-zero, runs
out of time or writes more fails the answer. A failed source leaves the
request unanswered, logged by credential name and cause alone. The
answerer tries it again each second until the request expires; after a
command source fails, it waits 60 seconds before it resolves the request's
sources again, so an interactive unlock prompts no more often than it can
be answered:

```
run <id>: resolve credential <name>: its command exited with status 1
```

A `file` source must name a regular file that its group cannot write and
others cannot read, write or execute. Mode 0600 is the recommended mode.
Group read is accepted, so a secrets agent can template the file for a
service group at mode 0640. A file that breaks the rule fails the answer:

```
run <id>: resolve credential <name>: its file is writable by its group or open to others; chmod 600 it
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
