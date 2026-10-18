# Operator guide

This guide covers running receivers and the Kafka and Nomad resources they
use. [Concepts](concepts.md) describes the trust model and placement, and
[Reference](reference.md) lists every command and configuration field.

## Enrolment and grants

A receiver publishes an enrolment request when it starts without a current
approval for its keys. The request lists each configured adapter with its
platform and the offers it found.

To approve a receiver:

1. Run `wherry enrol list` and find the request. Match its `request=` hash
   against the `enrolment requested` line in the receiver's log.
2. Write a grants file: a JSON array with one grant per adapter to approve.
   Each grant names a detected adapter and its platform, and sets
   `allowed` and `defaults`.
3. Run `wherry enrol approve REQUEST_HASH` with `--labels` and `--grants`,
   and optionally `--pool`, `--renewal` and `--expires`.

A grant's `allowed` maps each offer label key to the values the adapter may
advertise. The receiver drops any offer with a label key or value missing
from it, so a grant lists every key the adapter's offers carry. For the
Nomad adapter these are `runtime`, `nomad.node_pool` and `zone`. An
`allowed` key cannot be `platform` or one of the approved receiver label
keys.

A grant's `defaults` maps a key to one allowed value. A default adds a
requirement to every task selector that does not name its key. With
`"defaults": {"zone": "dc1"}`, a task that does not select a zone runs only
in `dc1`.

`enrol approve` refuses a grant that names no adapter in the request. A
receiver whose approval holds a grant for an adapter it does not have, or
with another platform, logs `enrolment rejected` and claims nothing. An
adapter without a grant advertises no offers.

To change a receiver's labels, pool or grants, approve its request again.
Each approval is a new generation. Credentials sealed under an earlier
generation stop opening, so seal them again after re-approval.

An approval that an earlier release signed with a non-empty receiver
selector enrols nothing and cannot be renewed. `wherry receivers` does not
list that receiver. Approve its request again to enrol it at the next
generation, then seal its credentials again.

## Pools and high availability

Approve several receivers with the same `--pool` to have them share a queue.
Each pool reads `tasks.queue` through the Kafka share group `p-<pool>`.

- Any live receiver in the pool can claim a task placed in it.
- When a receiver stops renewing its lock on a task, the broker delivers the
  task again to a receiver in the pool. That receiver looks up the turn's
  job in each of its adapters and adopts it if it exists. Receivers in one
  pool therefore point their adapters at the same Nomad namespace.
- Receivers in a pool accept each other's status records when they check a
  continuation or resume a transcript.

Approve every receiver in a pool with the same labels and grants, and give
each the same adapters. A receiver that claims a task its offers do not
match ends it as `TASK_STATE_REJECTED` with `no offer matches the task`.

Each receiver has its own keys, so seal credentials to each receiver in a
pool separately.

## Renewal and expiry

`enrol approve --expires` sets the approval lifetime, 30 days by default.
The receiver publishes a renew request once less than a third of the
lifetime is left, measured from the time the broker appended the approval
or last renewal. It repeats the request at most once an hour.

- With `--renewal auto`, the default, any client command that connects to
  Kafka answers a pending request. This includes `wherry mcp` tool calls.
  It signs a renewal only when the renewal extends the approval.
- With `--renewal manual`, `wherry enrol list` marks the receiver
  `renewal=requested`, and `wherry enrol renew RECEIVER_ID` signs the
  renewal.

A renewal expires 30 days after it is signed. It keeps the generation, so
sealed credentials keep opening.

When an approval expires, the receiver stops claiming, advertising and
delivering credentials. It keeps asking for renewal, and a renewal restores
it without a restart.

## Revocation

`wherry enrol revoke RECEIVER_ID` publishes a revocation floor at the
receiver's current generation.

- Clients stop routing to the receiver and ignore its capabilities and
  any record the broker appends after the revocation.
- Within 15 seconds the receiver stops claiming and advertising and drops
  the credentials it holds.
- Within a second of that, each in-flight run stops its job, publishes a
  failed status with the message `receiver revoked`, and releases its task
  to the pool. Clients ignore that status, since the broker appends it after
  the revocation.
- Records the receiver published before the revocation stay readable. See
  [History from revoked receivers](concepts.md#history-from-revoked-receivers).

Revocation does not rotate the credentials sealed to the receiver. The
receiver has seen their values, so rotate each one at its source.

`enrol approve` can approve a revoked receiver's request again, which
trusts its existing keys again. To re-admit the host with new keys, stop the
receiver, delete `signing.key` and `sealing.key` from its `state_dir`, and
start it again. It publishes a new request.

## Sealed credentials

Each adapter names at most one credential in the receiver configuration,
and `credential_env` names the variable the agent receives it in. A run
that matches the adapter while the receiver does not hold the credential
ends its turn in `TASK_STATE_AUTH_REQUIRED`.

`wherry secret put NAME --to RECEIVER_ID --from-file PATH` seals a new
version. `--expires` sets its lifetime, 30 days by default. The command
lists the runs in the receiver's pool waiting for that credential. Continue
each one with `wherry continue RUN_ID`.

A seal opens only on the receiver it was sealed to, under the approval
generation current when it was sealed, and before its expiry. The receiver
opens only the newest version of each name. Once its seal expires, or the
receiver is approved at a new generation, the receiver stops delivering the
credential, and new runs wait for a new seal. A renewal keeps it.

To rotate a credential, run `secret put` again with the new value. The
receiver opens the new version within 15 seconds, and the runs it dispatches
after that receive it. Runs already started keep the value they received.

`wherry receivers` lists each receiver's sealed credentials with their
versions and expiry times. It warns on standard error about each seal that
has expired or expires within 7 days.

## Kafka topics

wherry does not create topics. Create each one with the settings listed.

- `tasks.queue`, keyed by run ID: `cleanup.policy=delete`,
  `retention.ms=-1`.
- `tasks.events`, keyed by run ID: `cleanup.policy=delete`,
  `retention.ms=-1`, `message.timestamp.type=LogAppendTime`.
- `tasks.control`, keyed by run ID: `cleanup.policy=compact`.
- `transcripts`, keyed by run ID: `cleanup.policy=delete`,
  `retention.ms=2592000000` (30 days),
  `message.timestamp.type=LogAppendTime`.
- `receivers.capabilities`, keyed by receiver ID:
  `cleanup.policy=compact`.
- `receivers.state`, keyed by receiver ID: `cleanup.policy=delete`,
  `retention.ms=-1`, `message.timestamp.type=LogAppendTime`.
- `receivers.secrets`, keyed by `RECEIVER_ID/NAME`:
  `cleanup.policy=compact`, `segment.ms=3600000`,
  `delete.retention.ms=3600000`, `min.compaction.lag.ms=0`.

Each user also needs a reply topic, `replies.<kafka_user>` unless the
client configuration sets `reply_topic`. Set
`message.timestamp.type=LogAppendTime` on it.

`LogAppendTime` makes the broker stamp each record with the time it
appended it. The client compares these times to accept history from a
revoked receiver, and accepts none when a topic uses producer timestamps.

Every record on `receivers.state` is replayed to rebuild trust state, so it
keeps infinite retention.

## Kafka ACLs

The client and the receiver authenticate with SCRAM-SHA-512. In Kafka, the
Read and Write operations each imply Describe.

The user principal needs:

| Resource | Operation |
| --- | --- |
| Topic `receivers.state` | Read, Write |
| Topic `receivers.secrets` | Write |
| Topic `receivers.capabilities` | Read |
| Topic `tasks.queue` | Read, Write |
| Topic `tasks.control` | Read, Write |
| Topic `tasks.events` | Read |
| Topic `transcripts` | Read |
| Topic `replies.<kafka_user>` | Read |

Each receiver principal needs:

| Resource | Operation |
| --- | --- |
| Topic `receivers.state` | Read, Write |
| Topic `receivers.secrets` | Read |
| Topic `receivers.capabilities` | Write |
| Topic `tasks.queue` | Read |
| Topic `tasks.control` | Read |
| Topic `tasks.events` | Read, Write |
| Topic `transcripts` | Write |
| Topics with prefix `replies.` | Write |
| Group `p-<pool>` | Read |
| Transactional ID prefix `run-` | Write |

A receiver writes each result to the reply topic the task names, so its
Write grant covers every user's reply topic. A client that sets
`reply_topic` outside the `replies.` prefix needs a matching grant. The
receiver's transactional IDs are run IDs, which start with `run-`.

## Nomad ACL policy

The receiver reads its Nomad token from `NOMAD_TOKEN`. The adapter's calls
need this policy, with the namespace and node pools adjusted:

```hcl
namespace "agents" {
  capabilities = ["submit-job", "read-job", "read-logs", "alloc-lifecycle"]

  variables {
    path "nomad/jobs/agent-*" {
      capabilities = ["write", "read", "destroy"]
    }
  }
}

node {
  policy = "read"
}

node_pool "default" {
  policy = "read"
}
```

The capabilities map to the adapter's calls:

- `submit-job`: register a run's job, and deregister a job that outlives
  its stop signal or its dispatch wait.
- `read-job`: read a job and its allocations.
- `read-logs`: stream the agent task's standard output.
- `alloc-lifecycle`: send `SIGTERM` to the agent task.
- Variables `write`, `read` and `destroy`: hold each run's prompt, model
  credential and git context, and sweep them after the job ends. On Nomad
  2.0, `read` also allows the prefix listing the sweep uses.
- `node` read: list nodes to derive offers.
- `node_pool` read: register jobs in each node pool the offers name.

## Agent jobs

### Job shape

The adapter registers one batch job per turn, `agent-<run_id>-t<turn>`, in
the configured namespace. The job runs in the node pool and datacenter of
the matched offer, with restarts and rescheduling disabled. Its task group
`agent` holds the task `agent`, built from the runtime definition, and the
prestart task `git` when the run carries a git context.

The prompt, the credential and the git context travel in Nomad variables
under `nomad/jobs/agent-<run_id>-t<turn>`, which the job's own workload
identity reads. The adapter deletes them when the turn ends. Every 30
seconds it also deletes the variables of jobs that have stopped, and of
jobs never registered within 10 minutes.

The receiver stops a job by sending `SIGTERM` to the agent task. If the
task is still running 15 seconds later, it deregisters the job.

### Agent contract

The agent task receives:

- the prompt in `$NOMAD_TASK_DIR/prompt.txt`;
- the credential in the environment variable that `credential_env` names;
- `HOME` set to the task directory;
- the runtime's `env`, driver `config` and `resources`.

The agent writes JSON lines to standard output. The receiver publishes each
line as a transcript chunk. The line that decodes as a JSON object with
`"type": "result"` ends the turn: its `result` string becomes the task
result, and `"is_error": true` makes the state `TASK_STATE_FAILED`. The
receiver stops the job once it reads that line. A job that ends without one
fails with the message `agent ended without a result`.

A turn's run time is capped at its deadline, and at one hour after its
queue deadline.

For the `podman` driver the adapter reads the logs in CRI format and
overrides `entrypoint` for the git task. For other drivers it overrides
`command` and `args`.

## Git context

`wherry submit --git URL --git-ref REF [--git-path PATH]` asks the receiver
to check a repository out before the agent starts. The
[extension](a2a-extension-v1.md#git-context) defines the URL, ref and path
rules and the line added to the prompt.

The `git` task runs the runtime's own driver and configuration with a
`/bin/sh` script, so the image or host needs `/bin/sh`, `git`, and `ssh`
for SSH URLs. It fetches the ref at depth 1 into
`$NOMAD_ALLOC_DIR/<path>`.

The platform supplies host trust and repository access:

- For an HTTPS URL, the system git configuration and CA bundle.
- For an SSH URL, the host key in `/etc/ssh/ssh_known_hosts`. The task runs
  ssh with `StrictHostKeyChecking=yes` and `BatchMode=yes`.
- For a private repository, a deploy key or a git credential helper set up
  in the image or on the host.

`GIT_TERMINAL_PROMPT=0` stops git from asking for input. A failed checkout
ends the turn as `TASK_STATE_FAILED` with the message
`git context unavailable`.

## Upgrades

A receiver keeps its identity in `signing.key` and `sealing.key` in its
`state_dir`. Restarting it with the same directory keeps its enrolment.

On `SIGINT` or `SIGTERM` the receiver exits without stopping its jobs, and
its tasks return to the pool. The next delivery of each task adopts the
running job, on the restarted receiver or on another in the pool, and
resumes the transcript from the last heartbeat offset. To upgrade a
receiver, replace the binary and restart it.

Readers skip a record with a member they do not know. A build reads every
record version it has ever written, but an older build rejects a newer
version: version 2 `submission`, `capability` and `enrol_approval` records
need clients and receivers that know version 2. Upgrade every client and
receiver together.

## Harness integration

`wherry mcp` serves the client operations as MCP tools over stdio. It reads
the client configuration and opens a Kafka connection for each tool call.
[Reference](reference.md#mcp-tools) lists the tools.

`plugins/wherry` packages the server with the `wherry` Agent Skill:

- `plugin.json`, `mcp.json` and `skills/` form an Agent Plugins 1.0 bundle.
- `.claude-plugin/plugin.json` is the Claude Code manifest for the same
  skill and server.
- Other harnesses take `mcp.json` and `skills/wherry` directly.

`plugins/AGENTS.snippet.md` covers harnesses without skills, using the CLI.

The harness starts `wherry mcp` by name, so `wherry` must be on its `PATH`,
and the user running the harness needs the client configuration and user
key.
