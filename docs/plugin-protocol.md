# Plugin protocol

Implement this protocol to write a `wherry-plugin-<type>` binary.
Use [Plugins](plugins.md) to find existing implementations.
[Command reference](commands.md#plugins) explains discovery, pins and process recovery.

## Module

Use the Go module `github.com/benemon/wherry/pluginsdk`.
Its releases use `pluginsdk/v*` tags.
[`plugin.proto`](https://github.com/benemon/wherry/blob/main/pluginsdk/proto/wherry/plugin/v1/plugin.proto) defines the service.

1. Set `pluginsdk.Version` to your plugin's version.
2. Call `pluginsdk.Serve` with your factory.
3. Run `pluginsdktest` against your target platform.

`pluginsdktest.Run` takes a profile whose job completes, one whose job never
starts and, optionally, one whose job the platform never places and says why.
Zero `Settings` skip the pending profile. The suite repeats `Lookup` until a
job runs or never starts, within `pluginsdktest.PollDeadline`, 2 minutes.
The completing job writes `pluginsdktest.Working` and a newline to
`.wherry/working` beneath its output directory as it starts, by renaming a
file over it, and runs for at least 10 seconds. The suite repeats
`ReadWorking` on the running job until it returns that line, within the same
deadline, and expects nothing from it once the job is purged.

The factory receives a logger.
The receiver relays its JSON log lines at their original level.
The receiver logs your plugin's version at startup.

## Transport

The receiver and plugin use gRPC over [go-plugin](https://github.com/hashicorp/go-plugin).
Each start creates a certificate pair for mutual TLS.
The protocol version is 12.
The receiver refuses other protocol versions and plugins without a TLS certificate.
A protocol 12 plugin refuses a protocol 11 receiver, so upgrade a receiver and its plugins together.
Every call names its profile.
The maximum plugin message size is 64 MiB.

## Profiles

Your plugin inherits the receiver's environment.
`Configure` supplies the profile's name, pool, agent and merged `config`.
The receiver removes the `credential` key from that mapping.
It supplies the resolved [platform credential](configuration-receiver.md#platform-credentials) separately.
`Configure` carries no agent credential.
Validate the profile configuration within 30 seconds.

A plugin reports, for each profile, the paths of the input, output and
credentials directories as its agent sees them, which the receiver writes
into the prompt. It implements `InlineLimit` to report the most inline input
bytes the profile delivers. Zero declares no profile limit, and a negative
limit is invalid.

## Runs

Dispatch carries a run's prompt, its input files, every delivered
credential, and a fetch for each S3 input: its name, presigned URL, size and
SHA-256. Each credential is a `pluginsdk.Secret` holding its name, its value
and exactly one target: `Env`, an environment variable the plugin sets for
the agent, or `File`, a file it writes in the agent's credentials directory.
The plugin fetches and verifies each S3 input before the agent starts, and
reports a job that never started with class `fetch` when it cannot. A plugin
that cannot deliver an input reports it.

A plugin gives each agent the `git` environment that
[Git input](a2a-extension-v1.md#git-input) specifies, through
`pluginsdk.AgentEnvironment` and `pluginsdk.AgentUnset`.
[Git repositories](operations-hosts.md#git-repositories) states its limits.

Once the job ends the receiver stops it and asks the plugin to collect its
outputs, which arrive over the link in pieces of at most 1 MiB. The plugin
streams the outputs the receiver names for streaming, its S3 outputs, as the
receiver reads them.

A plugin may report, with a job that never started, the platform's reason
in `Terminal.Reason`. The receiver redacts it and writes it to its log only.

### Dispatch and Lookup

`Dispatch` returns once the platform holds the job, placed or not, with
`OutcomeCreated` and a handle. A dispatch that converges on an existing job
returns its handle at once. The receiver passes that handle to `Stop` and
`Purge`, which work on a job the platform has not placed.

`Lookup` makes one observation of a run's job. It returns at once with
`JobPending` while the platform has not placed the job, and with
`JobRunning`, `JobEnded` or `JobNeverStarted` once the job has started or
ended. Any other observation waits, bounded by the call's context: a placed
job whose task is starting, or a job the platform has not yet decided on. A
`Lookup` cut short returns the context's error and removes nothing.

Pending means not placed only. A Nomad job is pending while it is queued, or
while a blocked evaluation holds it with no allocation. A Kubernetes pod is
pending while its `PodScheduled` condition is `False` with reason
`Unschedulable` or `SchedulingGated`. A systemd unit is never pending.

For a pending job, `Job.Reason` is the platform's account of why the job is
not placed, and may be empty. The receiver redacts it and publishes it in the
run's [`placing` status](a2a-extension-v1.md#status). For a job that never
started, `Job.Class` and `Job.Reason` carry what `Follow`'s `Terminal`
carries. Both are empty otherwise.

### Follow

The receiver calls `Follow` and `Collect` with the handle of a later `Lookup`
that reported the job running or ended. A plugin's `Follow` may fail at any
time. The receiver recovers the stream, so a plugin need not. A plugin's
`Follow` on an ended job reads the rest of the log and returns its
`Terminal`.

### ReadWorking

The agent may report its current step in `.wherry/working` beneath its
output directory, `pluginsdk.WorkingFile`, as one line of at most 1 KiB,
`pluginsdk.MaxWorking` bytes ([Agent contract](configuration-receiver.md#agent-contract)).
While the job runs, the receiver calls `ReadWorking` with the handle it
follows, at most once at a time for a run, under a 5-second deadline, and
publishes the line as a status of kind `working`. The receiver redacts what
the call returns; the plugin does not.

The agent controls the file, and can change it while the plugin reads it,
so `ReadWorking` never reads with more access than the agent has. It does
one of two things:

- It reads the file inside the agent's own task, with the agent's
  privileges, so the read reaches nothing the agent could not already print.
- It opens the file itself within a per-run root the agent cannot replace,
  such as the run's directory, refuses a symbolic link in any component
  beneath that root, opens without blocking so a FIFO cannot hold the call,
  and refuses anything that is not a regular file with one link once opened.
  `pluginsdk.ReadWorkingFile` does this on a local file system.

Either way, it reads at most 1025 bytes whatever the file's size, returns
nothing for a file over 1 KiB, never a cut prefix, and returns nothing, and
no error, when the file or a directory above it is missing. A platform API
that checks a path and then opens it again by name does not qualify: an
agent that swaps the file between the two reaches outside its run. A plugin returns a failed read as a `pluginsdk.Error`,
and counts it in `wherry_plugin_platform_errors_total` as for its other
calls. The receiver logs a failed read once for each class.

## Readiness

Each advertisement gives every profile's sweep and readiness call one
advertise interval, 30 seconds. A call that runs out counts as not ready.

## Metrics

Each scrape of the receiver's `/metrics` calls every profile's `Metrics`,
within 2 seconds. A plugin answers with samples of the `wherry_plugin_`
families, each a family name, labels, and a counter or gauge value. The
receiver sets every sample's `profile` label to the profile it answered for,
and drops a sample outside the [families](metrics.md#families) as
[Plugins](commands.md#plugins) lists. The receiver observes
`wherry_plugin_placement_seconds` itself and drops a plugin's sample of it.

## Errors

A plugin returns a failed platform call as a `pluginsdk.Error`, which names
the operation (`Op`) and the platform's failure class (`Class`) beside the
error, and crosses the link as a gRPC status detail.
