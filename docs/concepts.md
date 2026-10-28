# Key concepts

[Introduction](introduction.md) states the problem wherry solves.
[Design](design.md) covers its topics and configuration delivery, and
[Security](security.md) its sealed credentials.

## Roles

- **The operator** runs `wherry admin`. It issues every Kafka principal,
  grants every ACL, and writes the records on `wherry.admin` that define
  pools, receivers, hosts and entitlements. The operator is root: its
  admin credential can write any topic and grant any ACL.
- **A tenant** submits work. Each tenant has one or more **hosts**,
  machines that run the `wherry` CLI or its MCP server. Each host is its
  own Kafka principal with its own Ed25519 signing key. A compromised host
  is a compromise of its tenant.
- **A pool** is a set of receivers that share terms and trust each other fully.
  Its terms name the agents it runs, the credentials each agent takes and their targets, the
  share-group delivery limit and lock duration, and the quotas its tenants'
  hosts get. A tenant is entitled to one or more pools. A host entitled through
  several pools gets the lowest of their quotas.
- **A receiver** is a long-running `wherry receiver run` process in one pool.
  It claims tasks, runs each as a platform job, and publishes the job's
  statuses, transcript and result. Each receiver is its own principal with
  an Ed25519 signing key, which it creates on first start and keeps in its
  state directory.
- **A plugin** is the binary `wherry-plugin-<type>` that runs
  jobs on one kind of platform. The receiver ships with none, and runs one
  plugin process per plugin from its plugin directory, linked to it
  by mutual TLS. Plugins exist for Nomad, Kubernetes and systemd.
- **A plugin profile** is a named, configured use of a plugin: the agent it
  launches, its labels and its runtime. A task is placed on a profile and a
  run is routed to one. A receiver holds one or more profiles, and each
  profile launches one agent.
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
password. The operator hands the bundle to the host or receiver. It never travels over Kafka.

| Principal | Name |
| --- | --- |
| host | `wherry.tnt.<tenant>.<host>` |
| receiver | `wherry.rcv.<receiver>` |
| operator | `wherry.operator` |

Authorisation is
[Kafka ACLs](https://kafka.apache.org/documentation/#security_authz), enforced
by the broker. `wherry admin` generates every ACL from the state on
`wherry.admin`. Each names one resource literally, except one prefixed grant per
receiver on its pool's transactional IDs. The `wherry admin` command that
records a host or receiver on `wherry.admin` also issues its principal and ACLs,
and `pool entitle` records an entitlement with its ACLs; no other approval step
exists. [Design](design.md#acls) lists the matrix.

## Placement

A task names an **agent** and a **selector**. The agent is a name from the
pool's terms; it is the one thing wherry itself interprets. The selector
is a list of requirements over labels, in the style of
[Kubernetes set-based selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#set-based-requirement).
The operators are `In`, `NotIn`, `Exists` and `DoesNotExist`;
[Commands](commands.md#selectors) gives the grammar.

**Labels describe characteristics**, such as a GPU, a network segment or an
isolation level. The operator declares them per plugin profile in the receiver's
configuration record. Keys are arbitrary and may carry the operator's own
prefix, such as `example.com/gpu`; the prefix `wherry.io/` is reserved.
Two profiles can launch the same agent with different labels, such as one
in containers and one on the host.
The platform, runtime, node pool and datacenter a profile uses are part
of its configuration, and are neither labels nor selectable.

**The client resolves placement before it signs.** It reads the receiver
configurations on `wherry.admin` for their labels, and the advertisements on
`wherry.receivers` for liveness, capacity and which profiles are ready. A
receiver whose latest advertisement is at most 120 seconds old is live. For each
live receiver in a pool the tenant is entitled to, the client looks for a ready
profile that launches the agent with labels the selector accepts. With no agent
named, each receiver takes the pool's default agent if a ready profile launches
it, otherwise the first other agent in the terms' order that one does. A
receiver that would run its pool's default agent wins over any receiver that
would run another agent. Among the receivers that remain, the one with the most
spare capacity wins, and ties go to the lowest receiver ID. The client
signs the agent, the selector and the chosen pool into the task.

**The receiver routes** a task it claims to its first ready profile, in
order within its plugin, whose agent and labels satisfy the task. It visits
plugins in the order of their names, the same order the client uses, and
labels choose between plugins. An agent outside
the pool's terms is rejected. A task no ready profile satisfies is a start
failure: the receiver records it on the pool's events and returns the task
to the pool. The client places a task by one member's labels, but any
member of the pool can claim it, so every member must honour the labels its
profiles declare.

## Files in and out

Everything the task receives lands as files. A submission's `raw` and
`data` parts are input files that the agent finds in the directory
`WHERRY_INPUT_DIR` names. They travel inline in the submission record,
within the broker's record limit, which `pool create` records, and the
limit each plugin profile declares. Over either, they go through S3 when the pool
has storage and the host a storage credential; otherwise submit refuses,
naming the limit. Credentials are files in the directory
`WHERRY_CREDENTIALS_DIR` names or environment variables, each at its
target. The receiver opens the prompt with the names of the files, the
credentials' targets and the outputs the task expects.

The agent leaves its outputs in the directory `WHERRY_OUTPUT_DIR` names.
Once the job ends and is stopped, whatever its exit, the plugin
collects the regular files there, up to 256 entries and the pool's output
limit (8 MiB by default), without
following links. The receiver redacts each file, publishes it in chunks on
the tenant's transcript topic in the result's transaction, and lists every
output in the result with the size and SHA-256 it computed, or with the
reason it was not returned. A required output that is not returned fails
the run. `wherry watch` verifies each output's chunks against the result
before it writes the file locally.

A pool can also carry files through an S3-compatible bucket its operator
names, up to 5 GiB each. The host uploads an S3 input with its own
credential before it publishes the task, and the task carries only the
input's name, size and SHA-256. When the claiming receiver asks for the
seal, the host presigns a short-lived GET per input and a PUT per expected
S3 output, and seals them with the credentials, so the URLs never sit on
the broker in the clear. The receiver checks each URL against the pool's
endpoint and bucket; the plugin fetches the inputs into the input
directory and verifies them; the receiver streams each S3 output to its
PUT URL, hashing what it sends, and lists it in the result. The host then
downloads and verifies the outputs and deletes the run's objects. The
host's storage credential never leaves the host, and S3 outputs, which go
to the host's own bucket, are not redacted.

A task can also name a git repository: a remote and a ref. The host's
`git` configuration gives the clone its credential and, for `ssh`, its host
key: the entry whose URL is the longest prefix of the remote. The credential
joins the task as a task credential in the file `git-credential`. The URL
prefix is the credential's scope, since every task cloning under it receives
the credential. A remote under no entry clones anonymously. The agent clones
it with `git` itself; wherry runs no git. The receiver delivers the host key
as the input file `known_hosts` and opens the prompt with the clone
commands, which read the credential from its file. The remote's host must
be one of the pool's VCS hosts. The agent returns its commits as a patch
or a bundle output, which the parent applies with its own credentials, or
pushes them to the branch `wherry/<run>` when the parent asks and its
credential allows.

## Runs and attempts

A run is one task from submission to its final state. It is one signed
submission and one execution, which ends in a terminal state. A receiver
may attempt that execution more than once.

Each pool reads its tasks through the Kafka
[share group](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
`p-<pool>`. A receiver holds a claimed task through the share group's
acquisition lock for the whole run and renews it every 10 seconds. When a
receiver stops renewing, the broker delivers the task again, and that
delivery is the next attempt. The receiver that takes it looks for the
run's job in each of its plugin profiles. If the job exists and came from the
same profile and agent, as its platform metadata records, the receiver adopts it, asks for a seal of its own,
and once it opens resumes the transcript from the run's latest committed
offset, which each holder records on its own progress topic,
`wherry.receiver.<receiver>.progress`. Any other job under the
run's ID is stopped and purged, and the run is dispatched again. Each
run writes through its own
[transactional producer](https://kafka.apache.org/documentation/#semantics),
`p.<pool>.<run>`, so an adopter fences the earlier holder's writes.

A status goes to the pool's events and the tenant feed in one transaction,
and every reader reads committed records only. The result and the **echo**
go to the tenant feed, and transcript and output chunks go to the tenant's
transcript topic, `wherry.tenant.<tenant>.transcript`, in the same
transaction as the status, result or progress record they belong to. While
the job runs, the receiver records the run's latest committed transcript
offset on its progress topic every 10 seconds. A terminal status carries
the number of transcript chunks its attempt wrote. The echo, written when a
receiver claims or rejects a run, names the pool and placement the run was
admitted to, so any host of the tenant can follow or cancel the run. The submitting
host also keeps a record of each run in its state directory, so it can report the
run before a receiver claims it.

A run passes through these records from submission to result.

![A run's lifecycle: submission, claim, seal, dispatch, statuses and transcript, result and watch, with where a redelivery and an adoption enter](diagrams/run-lifecycle.svg)

## Cancellation

A cancel is a host-signed record on the pool's events that names a run.
A receiver reads cancels every 10 seconds, and checks them every second
for each run it holds. The receiver stops a running job, and a receiver
that claims a cancelled run starts no job. Either way the run ends in
`TASK_STATE_CANCELED`.

## Revocation

Revoking a host or receiver removes its access at the broker, in six
steps:

1. Rotate the principal's SCRAM password.
2. Delete its ACLs.
3. Probe every broker as the principal, with the new password. While any
   broker still authorises it, the step probes again every second for up
   to 2 minutes, then fails. A failed login or any other probe error fails
   the step at once.
4. Delete its SCRAM credential.
5. Wait the longest of the broker's re-authentication, share-group lock
   and transaction timeout intervals.
6. Record its membership end on `wherry.admin`.

No claim or write by the principal is authorised after step 5. Readers
reject its records once they read the step 6 record. A host also skips
every record a receiver wrote once that receiver's membership has ended,
including records it wrote before, so the runs it served lose that history
in `watch` and `events`.

A receiver treats an authorisation error as denial. It stops claiming at
once, and stops its in-flight jobs once denial has lasted one lock
duration. The broker then delivers their tasks to other members of the
pool.

A revoked principal does not return. A host or receiver that comes back is
provisioned under a new name, with a new principal and key topic.

A compromised key calls for revocation. A
[key rotation](operations.md#rotate-a-key) keeps the retired key
verifying the owner's feed and transcript records from before the
rotation.
