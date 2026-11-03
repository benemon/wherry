# Quickstart: development

## Install wherry

Install wherry with [Homebrew](https://brew.sh/), on macOS or Linux:

```sh
brew install benemon/tap/wherry
```

The `benemon/tap` formula installs the release archive for the machine's OS
and architecture, and Homebrew checks the archive against the SHA-256 the
formula names. Every release updates the formula.

### Install a release archive

Each [release](https://github.com/benemon/wherry/releases) carries
`wherry_<version>_<os>_<arch>.tar.gz` for `linux` and `darwin` on `amd64` and
`arm64`, `checksums.txt` with each archive's SHA-256,
`checksums.txt.sigstore.json`, the keyless signature over it, and
`<archive>.sbom.json`, an SPDX SBOM for each archive. `<version>` is a
release tag. Download the archive and the checksums, verify the checksums'
signature with [cosign](https://docs.sigstore.dev/cosign/system_config/installation/),
check the archive against them, and extract the binary:

```sh
version=<version> os=linux arch=amd64
base=https://github.com/benemon/wherry/releases/download/$version
archive=wherry_${version}_${os}_${arch}.tar.gz
curl -fsSL --remote-name-all "$base/$archive" "$base/checksums.txt" "$base/checksums.txt.sigstore.json"
cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/benemon/wherry/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
grep "  $archive\$" checksums.txt | sha256sum -c -
tar -xzf "$archive" --strip-components=1 "${archive%.tar.gz}/wherry"
install -m 0755 wherry /usr/local/bin/wherry
```

On macOS, `shasum -a 256 -c -` takes the place of `sha256sum -c -`. Each
archive also carries a provenance attestation, which
[GitHub's CLI](https://cli.github.com/) verifies in place of the signature and
checksum steps:

```sh
gh attestation verify "$archive" -R benemon/wherry
```

Install wherry on the operator's machine before the operator's steps.

## Try it on one machine

To try wherry or develop against it before the operator's steps,
`wherry dev` runs a whole deployment on one machine: a Nomad dev agent, a
single-broker Kafka with TLS and SCRAM, pool `dev`, tenant `dev` with host
`dev`, and receiver `dev-1`. Its agents run under Nomad's `raw_exec`
driver as you, so a harness uses your own login. Nothing in it is fit for
production: Nomad's ACLs are off and every agent runs with your rights.
[Dev commands](commands-dev.md#dev-commands) describes each command.

### Prerequisites

- Linux or macOS, `amd64` or `arm64`, with wherry installed as
  [Install wherry](quickstart-production.md#install-wherry) shows.
- Docker or Podman that answers `docker version` or `podman version`. On
  macOS, Podman runs through `podman machine`.
- An agent harness on `PATH`, `claude`, `codex` or `gemini`, logged in.
  Without one, `--agent mock` runs the built-in mock agent alone.
- Ports 4646, 4647, 4648 and 9093 free on 127.0.0.1, and 18080 with
  storage.
- 1536 MB of memory for Kafka, which its Nomad job reserves, and the
  memory the agents use. Storage reserves 2048 MB more.

### 1. Acquire

```sh
wherry dev init
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
ready: instance default (nomad 2.1.1, kafka 1a2b3c4d5e6f, plugin nomad/v0.1.0-rc.18, wherry v0.1.0-rc.27)
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

`submit` prints `run_id=run-<id> pool=dev`. Each harness runs its own
default model. `wherry submit` is a [shortcut](commands.md#shortcuts) for
`wherry host submit`, as `wherry wait` is for `wherry host wait`.

### 4. Wait

```sh
wherry wait run-<id>
```

`wait` prints the agent's message once the run ends. `wherry logs run-<id>`
prints its transcript.

### 5. Stop

```sh
wherry dev down
```

`down` stops everything, deletes the environment's data and credentials,
and removes the host files `up` created at the standard paths. Once the
instance is down, `wherry dev down --reset` deletes the downloads in its
cache. It refuses while the instance is up.

