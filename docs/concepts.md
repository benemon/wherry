# Concepts

## The problem

A coding agent that delegates work runs its subagents on the machine the
agent itself runs on, or in the agent vendor's cloud. Neither is
infrastructure the user operates, so a subagent cannot use the user's own
clusters, hardware, network placement or isolation.

wherry sends a subagent task to a cluster the user runs, starts an
unmodified agent there, and returns the result to the caller. Every
exchange is a signed record on a Kafka cluster the user also runs.

## Components

- The client is the `wherry` CLI and its MCP server, `wherry mcp`. It holds
  the user key and signs every task, approval, revocation and credential.
- A receiver is a long-running `wherry receiver` process with access to a
  scheduling platform. It claims tasks, runs each one as a platform job,
  and publishes the job's progress, transcript and result.
- An adapter is the part of a receiver that talks to one platform. Adapters
  are compiled into the binary. The Nomad adapter is the only one.
- The agent is the program the job runs, such as a coding agent's headless
  command. wherry does not modify it. The receiver passes it a prompt file
  and reads its standard output.
- Kafka carries and keeps every record.

## Receivers and adapters

A receiver hosts one or more adapters, each with a name and a platform. An
adapter reports offers: the label sets it can run a job with right now. The
Nomad adapter derives one offer for each combination of a configured
runtime, a node pool and a datacenter among the nodes that are ready,
eligible and have that runtime's driver healthy. Its offers carry the labels
`runtime`, `nomad.node_pool` and `zone`.

Every 30 seconds the receiver signs and publishes a capability record with
its offers, its capacity (the number of runs it holds at once) and the number
of runs in flight. A receiver whose latest capability is at most 120 seconds
old is live.

## Labels, pools and selectors

A label is a `key=value` pair. Three kinds of label take part in placement:

- Receiver labels, such as `region=eu-west`, describe the receiver. The user
  sets them when approving it.
- Offer labels come from the adapter.
- Task labels, such as `example.com/team=platform`, describe the task. The
  submitter sets them.

A pool is a set of receivers that share one queue. A task is placed in a
pool, and any live receiver in that pool can claim it. Each receiver
belongs to one pool, named in its approval. The default pool is the
receiver's own ID.

A selector is a list of requirements. Each requirement names a key and the
values it accepts, and holds when the label with that key has one of them.
Placement checks two selectors:

- The task selector, chosen by the submitter, applies to the placement
  labels of each offer: the receiver labels, `platform` set to the adapter's
  platform, and the offer labels. A grant's defaults add a requirement for
  each default key the task selector does not name.
- The receiver selector, set by the user at approval, applies to the task
  labels.

The client picks, among live receivers with an offer the task selector
matches and a receiver selector the task labels satisfy, the one with the
most spare capacity. It signs that receiver's pool into the task. The
receiver that claims the task repeats both checks.

## Signed records

Every Kafka record value is a signed envelope. The user key signs
submissions, cancels, approvals, revocations and sealed credentials. A
receiver's own key signs its enrolment and renewal requests, capabilities,
statuses, transcripts and results. Readers verify each signature against
the key entitled to sign that record type and skip any record that fails.
The CLI reports the count of skipped records.

Task, status, transcript and result records are A2A v1.0 objects. The
[wherry A2A extension](a2a-extension-v1.md) defines their added fields and
the envelope format.

The agent job holds no Kafka credential. It holds the prompt, at most one
credential, and what its runtime definition gives it.

## Trust model

The user key is the root of trust. It is an Ed25519 key that `wherry init`
creates. Receivers are configured with its public key alone. A receiver
creates its own signing key (Ed25519) and sealing key (X25519) on first
start and keeps them in its state directory.

### Enrolment and approval

A new receiver publishes an enrolment request signed with its own key. The
request carries its ID, its public keys, its configured labels, and each
adapter with the offers it found. The user approves the request with the
user key. The approval sets:

- the receiver labels;
- the pool;
- one grant per adapter: the values allowed for each offer label key, and
  defaults;
- the receiver selector;
- the renewal policy, `auto` or `manual`;
- an expiry;
- a generation, one more than the receiver's previous generation.

The approval bounds what the receiver can advertise. A receiver drops each
offer with a label value outside its grant. A client rejects a capability
whose labels, pool or selector differ from the approval, or whose offers
fall outside the grants.

### Renewal

A receiver asks for renewal once less than a third of its approval's
lifetime is left. Under `auto`, the next client command that reads the trust
state signs a renewal, provided the renewal extends the approval. Under
`manual`, the user runs `wherry enrol renew`. A renewal keeps the generation
and every approved term and sets the expiry to 30 days from when it is
signed. A receiver whose approval has expired stops
claiming, advertising and delivering credentials until it is renewed.

### Revocation

A revocation sets a floor at the receiver's current generation. Approvals at
or below the floor are void. Within one trust refresh (15 seconds) the
receiver stops claiming and advertising and drops its credentials, and it
stops its in-flight runs at their next heartbeat.

### The sealed credential

Each adapter can name one credential, typically the model API key the agent
needs. The user seals it to one receiver with `wherry secret put`. The seal
is HPKE encryption to the receiver's sealing key, bound to the receiver,
the credential name, its version, the receiver's generation and approval,
and an expiry. The receiver opens the current version and, for each run,
passes the value to the agent as one environment variable.

Everything else the agent needs comes from the platform: the image or host
install, network access, CA bundles, SSH known hosts and repository
credentials. wherry carries no git credential.

## Runs, turns and attempts

A run is one task from submission to its final state. A run has one or more
turns. Each turn is a separate signed submission and runs as a new job. A
turn ends in a terminal state, or is interrupted.

A receiver interrupts a turn with `TASK_STATE_AUTH_REQUIRED` when the
adapter that matched the task needs a credential the receiver does not
hold. The user seals the credential and publishes a continuation, the next
turn, with `wherry continue`. The continuation's job receives the text of
every turn so far, each under a `--- turn N ---` heading.

A receiver holds a claimed task through a Kafka share group lock for the
whole turn. When a receiver stops renewing the lock, the broker delivers
the task again, and that delivery is the next attempt. The receiver that
takes it looks for the turn's job on its platform. If the job is still
there, the receiver adopts it and resumes the transcript from the last log
offset recorded in a heartbeat. Each run writes through its own Kafka
transactional producer, so an adopter fences the earlier holder's writes.

## Cancellation

A cancel is a user-signed record that applies to every turn of a run. A
receiver stops a running job at the next heartbeat, and a receiver that
claims a turn of a cancelled run starts no job. Either way the turn ends in
`TASK_STATE_CANCELED`. An interrupted run has no receiver holding it, so the
client treats a cancel for it as the final state. The
[extension](a2a-extension-v1.md#cancellation) gives the details.

## History from revoked receivers

Revocation does not erase what a receiver did before it. When the client
reads a run's history, it accepts a status, transcript or result record from
a revoked receiver if the broker appended it before the revocation. Both
times are broker append times, so the topics involved set
`message.timestamp.type=LogAppendTime`. The CLI marks such a record with
`signer=revoked-before=<time>`. The
[extension](a2a-extension-v1.md#revoked-receivers) lists the checks.
