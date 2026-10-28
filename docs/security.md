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
requires it. The host's configuration maps each credential name to one
source: a file, an environment variable of the process answering the seal,
or a command whose output is the value. A file source's file is refused
when its group can write it or others can access it. A task can also declare **task
credentials**: credentials of the host, such as a deploy key, that the
agent gets at the target the task's parent chooses. The pool's terms do not
govern them, and none may take the target of one of the agent's
credentials.

`wherry submit` refuses a task at once, with
`auth required: credential <name> is not in this host's configuration`, when the host has no source
for a required credential of the placed agent or for a task credential. An
optional credential the host has no source for is not delivered; the
prompt names it as not supplied.

A host seals nothing when it submits. The receiver that claims a run
with credentials to deliver makes a P-256 key pair for that
attempt and publishes a signed **seal request** on the tenant feed: the
run and attempt, the public key, the credential names and an expiry
5 minutes out. The host that submitted the run answers it while `wherry
await` or `wherry mcp` runs. It checks the request's signature against the
receiver's registered key, that the receiver is a live member of the run's
pool and holds the run's current attempt, that the pool's terms approve
each agent credential for the run's agent, and that the run declares each
task credential. It resolves each credential's source, then writes a **seal** to
its own topic: a compact JWE (`ECDH-ES+A256KW` with `A256GCM`, through
[go-jose](https://github.com/go-jose/go-jose)) encrypted to the request's
key and bound in its protected header to the tenant, pool, receiver, key
thumbprint, run, attempt, credential names and expiry.

A seal passes from the host to one attempt of one receiver in these steps.

![Credential delivery: the receiver's seal request, the host's checks and sealed answer, and delivery to the job](diagrams/credential-delivery.svg)

Only the requesting receiver can open a seal, and only for that attempt.
The private key lives in the receiver's memory until the seal opens or the
wait ends, so a seal on the host's topic opens for no later attempt and for
no restarted receiver. A redelivered run asks again. Hosts cannot see the
broker's claim, so a host seals only to the receiver that wrote the run's
first `claimed` status, and refuses when a second receiver claims the same
run; a run adopted by another receiver ends `TASK_STATE_FAILED` when its
seal wait ends.
The receiver redacts each credential of 8 bytes or more that it delivered,
with or without its trailing newline and as JSON escapes it, from
everything it publishes for the attempt. Each marker is an HMAC of the
credential's name under a key the host makes for the run and sends in every
seal, so no marker maps to a name without that key. Seal requests and
the failed status of an unanswered seal request name credentials, never
their values, by design.

A run whose seal does not arrive within 5 minutes ends in
`TASK_STATE_FAILED` with a message naming each credential the host did not
answer. The tenant fixes the credential's source in `credentials` in its
wherry configuration and submits a new run. An agent that finds a
credential missing or refused during the run ends it in
`TASK_STATE_AUTH_REQUIRED` naming the credential; that state is final, and
the tenant submits a new run. A parent that submits a task needing
credentials keeps `wherry await` or `wherry mcp` running until the task
starts.

Everything else the agent needs comes from the platform or from the task:
the image or host install, network access, CA bundles and SSH known hosts
from the platform, and files and task credentials from the task.

## Run credentialed tasks

Every credential has a source on the host and a target in the agent. The
host's [client configuration](configuration.md#client-configuration) maps each
credential name to one source:

```yaml
credentials:
  openai-api-key:
    file: ~/.config/wherry/credentials/openai
  github-token:
    env: GITHUB_TOKEN
  vault-token:
    command: [vault, print, token]
```

The configuration lives at `~/.config/wherry/config.yaml`. A `file` source is
read as it is, trailing newline included. Its file must be a regular file
that its group cannot write and others cannot access: mode 0600 is the
recommended mode, and mode 0640 is accepted for a secrets agent that
templates the file for a service group. An `env`
source is read from the environment of the process answering the seal,
`wherry await` or `wherry mcp`. A `command` source runs without a shell,
with the answerer's environment, for at most 60 seconds, which leaves time
for an interactive unlock such as a password manager's; its standard output
is the value, at most 64 KiB, and its standard error is discarded. The
configuration holds no literal values.

`wherry receivers` lists each agent's credential names, marking those this
host has no source for, such as
`credentials=openai-api-key,github-token(missing,optional)`. `wherry
submit` refuses a task with `auth required: credential <name> is not in
this host's configuration` when the host has no source for a required
credential of the placed agent, or for a task credential. An optional
credential the host has no source for is not delivered, and the prompt
names it as not supplied. A task credential takes a target with
`--credential NAME:file=FILE` or `--credential NAME:env=VAR`; `submit`
refuses one whose target is one of the agent's credentials' targets or
another task credential's.

A receiver asks the submitting host for the run's credentials when it
claims the task, and the host answers only while `wherry await` or
`wherry mcp` runs on it. A parent that submits a task with credentials
keeps one of them running until the task starts; without an answer within
5 minutes, the run ends `TASK_STATE_FAILED` with a message that names each
unanswered credential, never its value. The answerer resolves each source
when it answers, before it takes the attempt's lock, so a slow command
holds up no other answerer of the host. A source that fails to resolve, an
unset or empty variable, a command that exits non-zero, times out or writes
over 64 KiB, is logged as `run <id>: resolve credential <name>: <cause>`,
by name and exit status alone. The answerer tries the request again each
second until it expires, and 60 seconds after a command source fails.

The host answers only for a credential the pool's terms approve for the
run's agent, or a task credential the run declares, that its configuration
has a source for, and only to the receiver that first claimed the run. It
logs each verified request of its
own runs that it skips or refuses, by run, receiver and credential name, on
standard error. A receiver that adopts a running job asks for its own seal
and publishes nothing of the job until it opens. A run adopted by a second
receiver gets no seal, so that receiver stops the job and the run ends
`TASK_STATE_FAILED` naming the credentials; submit a new run.

## Credential sources

The answering process, `wherry await` or `wherry mcp`, resolves each source
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

The source never leaves the receiver's host: `wherry.admin`, which every
principal reads, records only the credential's name, and the receiver's
bundle holds its Kafka identity alone. A rotated platform token takes effect
on the receiver's next start without a new bundle.

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
