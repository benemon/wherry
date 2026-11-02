# Governance

Who may take part, how every claim is checked, what history remains, and
where a run's data goes.

## Identity and grants

The operator issues every identity. `wherry operator` creates each host and
receiver as a Kafka principal and writes its bundle, which the operator
hands over outside Kafka. The broker enforces every grant: `wherry operator`
generates each ACL from the state on `wherry.operator`, and no other approval
step exists. A tenant's hosts reach only the pools the tenant is entitled
to, and a receiver reaches only its own pool.
[Identity and authorisation](concepts.md#identity-and-authorisation)
describes the principals and [ACLs](design.md#acls) lists the matrix.

## Signed records

Every record on a shared topic is an EdDSA JWS signed by its owner. A reader
verifies it against the owner's current key, read from the owner's own key
topic, and then checks it against `wherry.operator`: a submission must come
from a live host of a tenant entitled to the pool, and a receiver's record
from a live member of the pool the run was placed in. No host or receiver
can vouch for another's key. [Topics](design.md#topics) gives the detail.

## Run history

Every operator action is a record on `wherry.operator`, which keeps its
records forever. A run's submission, statuses, transcript and result are
records on its pool's and tenant's topics: the task and event topics keep
them for the pool's `start_by_limit` plus a day and six hours, 4 days 6
hours by default, and the tenant feed and transcript topic for 30 days. A
receiver's progress topic holds the latest transcript offset of each run it
holds, and compaction removes a run's record after its terminal status.
[`wherry events`](commands.md#wherry-events-run-id) prints a run's status history.
Retention bounds this history, which holds the records wherry carried. The
platform's logs hold what an agent did inside its job.

## Where your data goes

Everything wherry carries stays on infrastructure the operator runs:

- the prompt, inline inputs, statuses, transcript and result travel as
  records on the operator's Kafka cluster;
- inputs and outputs too large for a record travel through the operator's
  S3-compatible bucket, by URLs the host presigns, and the storage
  credential never leaves the host;
- each credential leaves the host only inside the task's envelope,
  encrypted under the pool's key, is opened by the pool's key custodian to
  check its binding to the task and by one attempt of the claiming
  receiver, and reaches the agent at its target, as
  [Security](security.md) describes;
- the host keeps its run records, each with the run's task content and
  redaction key but no credential value in the clear, in its own state directory.

wherry has no hosted control plane: every host and receiver is one the
operator runs, no component sends data to a third party, and the Kafka
clients' telemetry to the broker is disabled. The agent's own calls, such as those to its model provider, go
where the agent sends them; wherry does not route them. wherry and its
plugins are open source under the Mozilla Public License 2.0.
