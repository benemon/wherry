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

The factory receives a logger.
The receiver relays its JSON log lines at their original level.
The receiver logs your plugin's version at startup.

## Transport

The receiver and plugin use gRPC over [go-plugin](https://github.com/hashicorp/go-plugin).
Each start creates a certificate pair for mutual TLS.
The protocol version is 10.
The receiver refuses other protocol versions and plugins without a TLS certificate.
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

### Follow

A plugin's `Follow` may fail at any time. The receiver recovers the stream,
so a plugin need not. A plugin's `Follow` on an ended job reads the rest of
the log and returns its `Terminal`.

## Readiness

Each advertisement gives every profile's sweep and readiness call one
advertise interval, 30 seconds. A call that runs out counts as not ready.

## Metrics

Each scrape of the receiver's `/metrics` calls every profile's `Metrics`,
within 2 seconds. A plugin answers with samples of the `wherry_plugin_`
families, each a family name, labels, and a counter or gauge value or a
histogram. A histogram gives the cumulative count at each bound of
`pluginsdk.PlacementBuckets`, its sum and its count. The receiver sets every
sample's `profile` label to the profile it answered for, and drops a sample
outside the [families](metrics.md#families) as
[Plugins](commands.md#plugins) lists.

## Errors

A plugin returns a failed platform call as a `pluginsdk.Error`, which names
the operation (`Op`) and the platform's failure class (`Class`) beside the
error, and crosses the link as a gRPC status detail.
