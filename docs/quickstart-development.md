# Quickstart: development

Run a complete instance on one machine.
Use this instance to try wherry or develop integrations.

For a deployment across machines, follow [Install](install.md).

## Before you start

You need Linux or macOS, Docker or Podman, and an installed agent harness.
Without a harness, you can run the built-in mock agent instead.

## Install wherry

Install wherry on this machine, as [Install the wherry CLI](install-cli.md) shows.

## Start your instance

`wherry dev` runs a whole deployment on one machine: a Nomad dev agent, a
single-broker Kafka with TLS and SCRAM, pool `dev`, tenant `dev` with host
`dev`, and receiver `dev-1`. Its agents run under Nomad's `raw_exec`
driver as you, so a harness uses your own login. Nothing in it is fit for
production: Nomad's ACLs are off and every agent runs with your rights.
[Dev commands](commands-dev.md#dev-commands) describes each command.

### What your machine needs

- Linux or macOS, `amd64` or `arm64`, with wherry installed as
  [Install wherry](#install-wherry) shows.
- Docker or Podman that answers `docker version` or `podman version`. On
  macOS, Podman runs through `podman machine`.
- An agent harness on `PATH`, `claude`, `codex` or `gemini`, logged in.
  Without one, use the mock agent, as step 1 shows.
- Ports 4646, 4647, 4648 and 9093 free on 127.0.0.1, and 18080 with
  storage.
- 1536 MB of memory for Kafka, which its Nomad job reserves, and the
  memory the agents use. Storage reserves 2048 MB more.

### 1. Acquire

```sh
wherry dev init
```

Without an agent harness, acquire the mock agent alone:

```sh
wherry dev init --agent mock
```

`init` downloads Nomad 2.1.1, the newest nomad plugin release whose plugin
protocol this wherry speaks, and Kafka's image, checks their signatures and checksums, and records them in the
instance's manifest. It asks for the default agent when it finds more than
one harness, and `--agent NAME` chooses it. `--storage-enabled` adds S3
storage on 127.0.0.1:18080, a ceph-aio job whose data lives in the
container and goes with `down`.

### 2. Start

```sh
wherry dev up
```

`up` downloads nothing. It starts the environment, runs the mock agent
once, and prints:

```
ready: instance default (nomad 2.1.1, kafka 1a2b3c4d5e6f, plugin nomad/v0.1.0, wherry v0.1.0)
config: /home/me/.config/wherry/config.yaml
submit: wherry submit --prompt "..."
claude: claude mcp add wherry-default -- wherry host mcp
```

For the `default` instance, `up` writes the host's config, bundle and key at
the standard host paths, where every host command reads them, so the
commands below take no flags. When a host file `up` did not create is
already there, or `WHERRY_CONFIG` is set, `up` writes the host inside the
instance and prints `export WHERRY_CONFIG=<path>`
([`wherry dev up`](commands-dev.md#wherry-dev-up)). The `claude:` line
registers the MCP server with Claude Code, so a session submits through the
`wherry-default` tools. `up` copies
`CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`, `CODEX_API_KEY` (or
`OPENAI_API_KEY`) and `GEMINI_API_KEY` from its environment into the host's
credential files when they are set, and Nomad keeps them out of the agents'
environment, so they reach an agent only through its run.
`--agent-image NAME=IMAGE` runs agent `NAME` in a container image, such as
one [`agent.Containerfile`](https://github.com/benemon/wherry/blob/main/docs/agent.Containerfile)
builds.

### 3. Submit

```sh
wherry submit --prompt "List the files in your input directory."
```

`submit` prints `run_id=run-<id> pool=dev`, followed by
`unrequested=<names>` when the agent has optional credentials the task did
not request. Each harness runs its own
default model. `wherry submit` is a [shortcut](commands.md#shortcuts) for
`wherry host submit`, as `wherry wait` is for `wherry host wait`.

### 4. Wait

```sh
wherry wait run-<id>
```

`wait` prints the agent's message once the run ends. `wherry logs run-<id>`
prints its transcript.

### Check that it worked

Confirm that `wait` prints the agent's response and exits successfully.
Use `wherry host status RUN_ID` to inspect the terminal state.

### 5. Stop

```sh
wherry dev down
```

`down` stops everything, deletes the environment's data and credentials,
and removes the host files `up` created at the standard paths. Once the
instance is down, `wherry dev down --reset` deletes the downloads in its
cache. It refuses while the instance is up.


## Next steps

Read [Key concepts](concepts.md) to understand placement and recovery.
See [Development instance configuration](configuration-development.md) for the next tasks.
