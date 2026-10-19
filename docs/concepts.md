# Concepts

## The problem

A coding agent that delegates work runs its subagents on the machine the
agent itself runs on, or in the agent vendor's cloud. Neither is
infrastructure its user operates, so a subagent cannot use the user's own
clusters, hardware, network placement or isolation.

wherry sends a subagent task to a platform the user runs, starts an
unmodified agent there, and returns the result to the caller. Every
exchange is a record on a Kafka cluster the user also runs. Kafka supplies
identity, authorisation, isolation, history, work distribution and atomic
writes.

## Roles

- **The operator** runs `wherry admin`. It issues every Kafka principal,
  grants every ACL, and writes the records on `wherry.admin` that define
  pools, receivers, hosts and entitlements. The operator is root: its
  admin credential can write any topic and grant any ACL.
- **A tenant** submits work. Each tenant has one or more **hosts**,
  machines that run the `wherry` CLI or its MCP server. Each host is its
  own Kafka principal with its own Ed25519 signing key. A compromised host
  is a compromise of its tenant.
- **A pool** is a set of receivers that share terms and trust each other
  fully. Its terms name the agents it runs, the credentials each agent
  takes, the seal lifetime, the share-group delivery limit and lock
  duration, and the quotas its tenants' hosts get. A tenant is entitled to
  one or more pools. A host entitled through several pools gets the lowest
  of their quotas.
- **A receiver** is a long-running `wherry receiver` process in one pool.
  It claims tasks, runs each as a platform job, and publishes the job's
  statuses, transcript and result. Each receiver is its own principal with
  an Ed25519 signing key and an X25519 sealing key, which it creates on
  first start and keeps in its state directory.
- **An adapter** is the part of a receiver that runs jobs on one platform.
  Adapters are compiled into the binary; the Nomad adapter is the only
  one. A receiver holds one or more adapters, and each adapter launches one
  agent.
- **The agent** is the program a job runs, such as a coding agent's
  headless command. wherry does not modify it. The receiver passes it a
  prompt file and reads its standard output.

One person can hold every role in a development setup.

## Identity and authorisation

Identity is a Kafka principal authenticated with
[SCRAM-SHA-512](https://kafka.apache.org/documentation/#security_sasl_scram)
over TLS. `wherry admin` creates each host and receiver principal, sets a
generated password, and writes a **bundle**: a file, mode 0600, holding the
broker address, the broker's CA certificate, the principal and its
password, and for a receiver any platform credential its platform does not
issue by its own identity mechanism. The operator hands the bundle to the
host or receiver. It never travels over Kafka.

| Principal | Name |
| --- | --- |
| host | `wherry.tnt.<tenant>.<host>` |
| receiver | `wherry.rcv.<receiver>` |
| operator | `wherry.operator` |

Authorisation is [Kafka ACLs](https://kafka.apache.org/documentation/#security_authz),
enforced by the broker. `wherry admin` generates every ACL from the state
on `wherry.admin`. Each names one resource literally, except one prefixed
grant per receiver on its pool's transactional IDs. The `wherry admin`
command that records a host or receiver on `wherry.admin` also issues its
principal and ACLs, and `tenant add` records an entitlement with its ACLs.
Those commands are the approval.
[Reference](reference.md#acls) lists the matrix.

## Topics

Kafka cannot filter records on the broker, so records consumed
differently live on different topics.

| Topic | Writers | Readers | Holds |
| --- | --- | --- | --- |
| `wherry.admin` | the operator | every principal | pool terms, receiver configurations, hosts, entitlements, membership ends, skips |
| `wherry.receivers` | receivers | receivers, hosts | advertisements |
| `wherry.receiver.<receiver>` | that receiver | its pool's members, entitled tenants' hosts | its keys |
| `wherry.pool.<pool>.tasks` | entitled tenants' hosts | the pool's receivers, through its share group | submissions |
| `wherry.pool.<pool>.events` | the pool's receivers, entitled tenants' hosts | the pool's receivers | statuses, heartbeats, start failures, cancels |
| `wherry.tenant.<tenant>` | receivers of the tenant's pools | the tenant's hosts | statuses, transcript chunks, results, echoes |
| `wherry.host.<tenant>.<host>` | that host | that host, receivers of the tenant's pools | its key, sealed credentials |

`wherry.admin` records and sealed credentials carry no signature: each
sits on a topic only one principal can write, so Kafka authenticates it.
Every other record, key records included, is a versioned EdDSA
[JWS](https://datatracker.ietf.org/doc/html/rfc7515). A record on a shared
topic names its owner and key in its `kid`, and a reader verifies it against that
owner's current key, read from the owner's own key topic, before it
decodes the payload. No host or receiver can vouch for another's key.

A reader then checks the record against `wherry.admin`. A submission or
cancel must come from a live host of a tenant entitled to the pool whose
topic it arrived on. A receiver record must come from a live member of the
pool the run was placed in, on behalf of a tenant entitled to it.
[The wherry A2A extension](a2a-extension-v1.md) specifies every record.

Every topic has one partition, `LogAppendTime` timestamps and
`cleanup.policy=delete`. Nothing is compacted and no topic is deleted. The
tenant feed keeps records for 30 days and `wherry.receivers` for 7; every
other topic keeps them forever.

Every principal reads all of `wherry.admin`, so tenants see each other's
names, hosts, entitlements and pools.

## Placement

A task names an **agent** and a **selector**. The agent is a name from the
pool's terms; it is the one thing wherry itself interprets. The selector
is a list of requirements over labels, in the style of
[Kubernetes set-based selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#set-based-requirement).
The operators are `In`, `NotIn`, `Exists` and `DoesNotExist`;
[Reference](reference.md#selectors) gives the grammar.

**Labels describe characteristics**, such as a GPU, a network segment or an
isolation level. The operator declares them per adapter in the receiver's
configuration record. Keys are arbitrary and may carry the operator's own
prefix, such as `example.com/gpu`; the prefix `wherry.io/` is reserved.
The platform, runtime, node pool and datacenter an adapter uses are part
of its configuration, and are neither labels nor selectable.

**The client resolves placement before it signs.** It reads the receiver
configurations on `wherry.admin` for their labels, and the advertisements
on `wherry.receivers` for liveness, capacity and which adapters are ready.
A receiver whose latest advertisement is at most 120 seconds old is live.
For each live receiver in a pool the tenant is entitled to, the client
looks for a ready adapter that launches the agent with labels the selector
accepts. With no agent named, each receiver takes the pool's default agent
if a ready adapter launches it, otherwise the first other agent in the
terms' order that one does. Among the receivers that can take the task,
the one with the most spare capacity wins, whichever agent it would run,
and ties go to the lowest receiver ID. The client signs the agent, the selector and the
chosen pool into the task.

**The receiver routes** a task it claims to its first ready adapter, in
declared order, whose agent and labels satisfy the task. An agent outside
the pool's terms is rejected. A task no ready adapter satisfies is a start
failure: the receiver records it on the pool's events and returns the task
to the pool.

## Configuration delivery

A receiver's configuration is a record on `wherry.admin`: its capacity
(the runs it holds at once) and its ordered adapters, each with a name, a
type, an agent, an optional credential, labels and the adapter's own
settings. It holds no secrets. `wherry admin` refuses a configuration that
names an agent or credential outside the pool's terms.

A receiver starts with its bundle and a state directory. It registers its
keys, waits for its configuration record, validates it against the terms
and builds its adapters. It then advertises every 30 seconds. When a new
configuration record appears, the receiver stops and exits, so its
platform restarts it with the new configuration. Runs in flight are
adopted on redelivery.

## Sealed credentials

The agent usually needs a model credential, and each tenant's work runs on
that tenant's own credential. The pool's terms name the credentials each
agent may take, and the receiver configuration names the one an adapter
delivers. The host's configuration maps each credential name to a file.

At every submit and continue, the host seals each credential the agent
takes, and that the host has a file for, to each live member of the chosen
pool lacking a current seal. A seal is HPKE encryption to the receiver's
sealing key, using the Go standard library's
[`crypto/hpke`](https://pkg.go.dev/crypto/hpke), bound to the tenant, the
receiver, the pool, the sealing key's fingerprint, the credential name, and
the seal's issue and expiry times. The seal lifetime comes from the pool's
terms. Seals live on the host's own topic, and a receiver opens the newest
one by issue time across its tenant's hosts.

The host pins each receiver's sealing-key fingerprint the first time it
seals to it, and refuses to seal to a changed key.

A receiver that holds no seal for the task's tenant returns the task to
the pool when another member holds one. When no member holds one, the turn
ends in `TASK_STATE_AUTH_REQUIRED` and waits for the tenant.

Everything else the agent needs comes from the platform: the image or
host install, network access, CA bundles, SSH known hosts and repository
credentials. wherry carries no git credential.

## Runs, turns and attempts

A run is one task from submission to its final state. It has one or more
turns. Each turn is a separate signed submission and runs as a new job. A
turn ends in a terminal state, or is interrupted.

A turn interrupted in `TASK_STATE_AUTH_REQUIRED` (see [Sealed
credentials](#sealed-credentials)) resumes when the tenant adds the
credential and runs `wherry continue`, which seals it. The continuation's
job receives the text of every turn so far as one request, opening with a
line saying none of it was carried out, since an interrupted turn never
reaches an agent.

Each pool reads its tasks through the Kafka
[share group](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
`p-<pool>`. A receiver holds a claimed task through the share group's
acquisition lock for the whole turn and renews it every 10 seconds. When a
receiver stops renewing, the broker delivers the task again, and that
delivery is the next attempt. The receiver that takes it looks for the
turn's job in each of its adapters. If the job exists, the receiver adopts
it and resumes the transcript from the last log offset recorded in a
heartbeat. Each run writes through its own
[transactional producer](https://kafka.apache.org/documentation/#semantics),
`p.<pool>.<run>`, so an adopter fences the earlier holder's writes.

A status goes to the pool's events and the tenant feed in one transaction,
and every reader reads committed records only. Heartbeats go to the pool's
events alone. Transcript chunks, the result and the **echo** go to the
tenant feed. The echo, written when a receiver claims or rejects a turn,
names the pool and placement the turn was admitted to, so any host of the
tenant can follow, continue or cancel the run. The submitting host also
keeps a record of each run in its state directory, so it can report the
run before a receiver claims it.

## Cancellation

A cancel is a host-signed record on the pool's events. It names the run's
latest turn and applies to that turn and every later one. A receiver reads
cancels every 10 seconds, and a running turn checks them every second. The
receiver stops a running job, and a receiver that claims a cancelled turn
starts no job. Either way the turn ends in `TASK_STATE_CANCELED`.

An interrupted run has no receiver holding it. Every pool member that
reads a cancel for one writes its `TASK_STATE_CANCELED` terminal under one
event ID, and readers treat those records as one.

## Revocation

Revoking a host or receiver removes its access at the broker, in six
steps:

1. Rotate the principal's SCRAM password.
2. Delete its ACLs.
3. Probe every broker as the principal, with the new password. The step
   fails when any broker still authorises it.
4. Delete its SCRAM credential.
5. Wait the longest of the broker's re-authentication, share-group lock
   and transaction timeout intervals.
6. Record its membership end on `wherry.admin`.

No claim or write by the principal is authorised after step 5. Readers
reject its records once they read the step 6 record. A host also skips
every record a receiver wrote once that receiver's membership has ended,
including records it wrote before, so the runs it served lose that history
in `await` and `events`.

A receiver treats an authorisation error as denial. It stops claiming at
once, and stops its in-flight jobs once denial has lasted one lock
duration. The broker then delivers their tasks to other members of the
pool.

A revoked principal does not return. A host or receiver that comes back is
provisioned under a new name, with a new principal and key topic.
