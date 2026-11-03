# Quickstart

This guide runs wherry two ways. [Try it on one machine](#try-it-on-one-machine)
runs a whole deployment on one machine with `wherry dev`. The operator path
then sets up one deployment across machines and runs a first task on it, in
three parts: the [operator's](#operator), the [receiver's](#receiver) and
the [host's](#host). That deployment is pool `build`, tenant `acme` with
host `laptop`, and receiver `nomad-1` running agent `claude` on a Nomad
cluster. [Key concepts](concepts.md) explains the parts involved.

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
[Dev commands](commands.md#dev-commands) describes each command.

### Prerequisites

- Linux or macOS, `amd64` or `arm64`, with wherry installed as
  [Install wherry](#install-wherry) shows.
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
([`wherry dev up`](commands.md#wherry-dev-up)). The `claude:` line
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

## Operator

Each step names the machine it runs on:

- the operator's machine, which holds the installation's working directory,
  where `wherry operator` keeps its connection, the pool and receiver files
  and every bundle;
- the receiver host, which runs receiver `nomad-1`;
- `laptop`, the host the tenant submits from.

Two pairs of files name the same credentials:

- The pool terms give agent `claude` the credential `claude-oauth-token`.
  The host's configuration maps that name to a source on `laptop`.
- The receiver configuration names the platform credential `nomad-token`,
  the Nomad token the receiver authenticates with. The receiver's local
  configuration maps that name to a source on the receiver host.

The configuration on `wherry.operator` holds the names. Each value is read from
its source on the machine that resolves it.

### Prerequisites

- A Nomad cluster with:
  - a namespace for agent jobs, `agents`;
  - nodes in the default node pool with the `docker` driver, able to pull
    `registry.example.com/claude-agent:1.0`, an image with Claude Code
    installed as `claude`;
  - a node pool `agents-host` whose nodes run the `exec` driver and have
    Claude Code installed as `claude`;
  - a token carrying the
    [receiver policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy);
  - its CA certificate on the receiver host at `/etc/wherry/nomad-ca.pem`.
- A Claude Code OAuth token, which `claude setup-token` prints, on `laptop`.
- An agent that meets the [agent contract](configuration.md#agent-contract),
  here Claude Code in the image and on the `agents-host` nodes.

### 1. Prepare Kafka

On the Kafka cluster, enable share groups, a TLS listener with
SCRAM-SHA-512, an authoriser, and the settings
[Prepare Kafka](operations.md#prepare-kafka) lists, and create the Kafka
super user's SCRAM credential, here `kafka-admin`. This step happens once per
cluster. On a Strimzi cluster, set the User Operator's ignored-users pattern
before `operator init`, as [Strimzi](operations.md#strimzi) shows, so that it
keeps the principals wherry creates.

On the operator's machine, put the super user's password in
`/etc/wherry/admin-password`, mode 0600. The listener's certificate here
chains to a private CA, so put that CA's certificate in
`/etc/wherry/kafka-ca.pem` too. A listener whose certificate chains to a
system root needs no CA file.

### 2. Initialise the working directory

On the operator's machine, make the installation's working directory and
run `wherry operator init` in it:

```sh
mkdir wherry-prod && cd wherry-prod
wherry operator init --broker kafka.example.com:9093 --ca-file /etc/wherry/kafka-ca.pem \
  --super-user kafka-admin --super-user-password-file /etc/wherry/admin-password
```

On a terminal, `init` asks for whatever the flags leave out. Connected as
the Kafka super user, it creates the principal `wherry.operator` with a new
password, grants it the ACLs provisioning needs, and creates the
installation-wide topics. It writes `operator.yaml` (the broker, the CA file
and `password_file: operator.password`) and `operator.password`, mode 0600,
and adds `operator.password` and `bundles/` to `.gitignore`. The super user is
written nowhere: every later `wherry operator` command, run in this
directory or given `--dir`, connects as `wherry.operator`.

The broker address goes into every host bundle, so it is the one `laptop`
reaches. A receiver can name another in its file.

### 3. Write the pool terms

In the working directory, write the [pool terms](configuration.md#pool-terms)
to `pools/build.yaml`:

```yaml
agents:
  - name: claude
    credentials:
      - {name: claude-oauth-token, env: CLAUDE_CODE_OAUTH_TOKEN}
default_agent: claude
delivery_limit: 5
lock_duration: 15s
producer_byte_rate: 1048576
request_percentage: 10
```

The pool runs one agent, `claude`, the default for a task that names none.
Every run of it takes the credential `claude-oauth-token`, which the agent
finds in the environment variable `CLAUDE_CODE_OAUTH_TOKEN`, where Claude
Code reads its token. The name is the link to the host: `laptop`'s
configuration gives `claude-oauth-token` a source in
[Host step 3](#_3-initialise-the-host), and a host without one has
`wherry submit` refuse the task.

[Create a pool](operations.md#create-a-pool) explains `delivery_limit`,
`lock_duration` and the other fields.

### 4. Create the pool

In the working directory:

```sh
wherry operator pool apply build
```

`pool apply` reads `pools/build.yaml` and creates the pool: it records the
terms on `wherry.operator`, creates the pool's topics, and sets the
configuration of its share group.
Without that file, on a terminal, it asks for the agents, their models and
credentials, the quotas and the start-by limit, writes `pools/build.yaml`,
then applies it.

### 5. Entitle the tenant

In the working directory:

```sh
wherry operator pool entitle build --tenant acme
```

Tenant `acme` may now submit to `build` from any host the operator adds for
it, as in [Host step 1](#_1-add-the-host).

## Receiver

### 1. Write the receiver configuration

On the operator's machine, download the `checksums.txt` of the Nomad
plugin's [wherry-plugins release](https://github.com/benemon/wherry-plugins/releases),
verify its signature, and take the plugin binary's SHA-256 from it, as
[Install plugins](operations.md#install-plugins) describes. The receiver
starts the plugin only from a binary with that SHA-256:

```sh
plugin=nomad version=<version> arch=amd64
base=https://github.com/benemon/wherry-plugins/releases/download/$plugin/$version
curl -fsSL --remote-name-all "$base/checksums.txt" "$base/checksums.txt.sigstore.json"
cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp "^https://github\.com/benemon/wherry-plugins/\.github/workflows/release\.yml@refs/tags/$plugin/v" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
awk -v name="wherry-plugin-${plugin}_${version}_linux_${arch}" '$2 == name {print $1}' checksums.txt
```

In the working directory, write the
[receiver configuration](configuration.md#receiver-configuration) to
`receivers/nomad-1.yaml`, with the SHA-256 `awk` printed as the pin (shown
here as `0123…`):

```yaml
pool: build
plugin_sha256:
  # The SHA-256 of the plugin binary the receiver may run, from the plugin's release.
  nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
capacity: 2
plugins:
  nomad:
    # Both plugin profiles inherit this config; each states only how it differs.
    config:
      address: https://nomad.example.com:4646
      ca_file: /etc/wherry/nomad-ca.pem
      namespace: agents
      credential: nomad-token
      runtime:
        driver: docker
        config:
          image: registry.example.com/claude-agent:1.0
          command: /bin/sh
          args:
            - -c
            - cd "$NOMAD_TASK_DIR" && exec claude -p --output-format stream-json --verbose < prompt.txt
        resources:
          cpu: 1000
          memory_mb: 2048
    profiles:
      - name: container
        agent: claude
        labels:
          isolation: container
      - name: host
        agent: claude
        labels:
          isolation: host
        config:
          node_pool: agents-host
          runtime:
            driver: exec
            # The exec driver takes no image, so the plugin profile removes the inherited one.
            config:
              image: null
```

The receiver connects to the broker `operator.yaml` names, unless a
`broker:` key names another. The plugin's `config` holds what both plugin
profiles share, and the
[Nomad plugin's settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
list every key. `image: null` removes the inherited `image` from profile
`host`, as [Plugin profile merge](configuration.md#plugin-profile-merge)
describes. Both profiles launch `claude`, which takes `claude-oauth-token`
at the target the pool terms give it. A task with no selector runs on
`container`, the first in routing order, and `--selector isolation=host`
runs on `host`.

`credential: nomad-token` names a [platform
credential](configuration.md#platform-credentials). The name is the link to
the receiver host: its local configuration gives `nomad-token` a source in
[Receiver step 7](#_7-write-the-receiver-s-local-configuration). A receiver
without one fails to configure the plugin profile, and exits with:

```
profile container: credential "nomad-token" has no source in the receiver's configuration
```

The same configuration also works as a
[directory](configuration.md#receiver-configuration-directory) with one file
for the receiver, one for the plugin and one for each plugin profile, given
with `-f` naming the directory. Without a file, on a terminal,
`receiver apply` asks for the pool, the plugin, its pin and config, the
agents with their labels, the capacity and the broker address, writes
`receivers/nomad-1.yaml`, then applies it.

### 2. Check the receiver configuration

In the working directory, check the file against the pool's terms:

```sh
wherry operator receiver apply nomad-1 --dry-run
```

`--dry-run` writes nothing and prints the pin and each plugin profile as the
record will hold them, with the plugin's `config` merged in and its keys
sorted:

```yaml
capacity: 2
plugin_sha256:
    nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
profiles:
    - name: container
      plugin: nomad
      agent: claude
      labels:
        isolation: container
      config:
        address: https://nomad.example.com:4646
        ca_file: /etc/wherry/nomad-ca.pem
        credential: nomad-token
        namespace: agents
        runtime:
            config:
                args:
                    - -c
                    - cd "$NOMAD_TASK_DIR" && exec claude -p --output-format stream-json --verbose < prompt.txt
                command: /bin/sh
                image: registry.example.com/claude-agent:1.0
            driver: docker
            resources:
                cpu: 1000
                memory_mb: 2048
    - name: host
      plugin: nomad
      agent: claude
      labels:
        isolation: host
      config:
        address: https://nomad.example.com:4646
        ca_file: /etc/wherry/nomad-ca.pem
        credential: nomad-token
        namespace: agents
        node_pool: agents-host
        runtime:
            config:
                args:
                    - -c
                    - cd "$NOMAD_TASK_DIR" && exec claude -p --output-format stream-json --verbose < prompt.txt
                command: /bin/sh
            driver: exec
            resources:
                cpu: 1000
                memory_mb: 2048
```

Plugin profile `host` has the `exec` driver and no `image`. A plugin
profile whose agent is outside the pool's terms fails with
`receiver agent is outside pool terms`.

### 3. Add the receiver

In the working directory:

```sh
wherry operator receiver apply nomad-1
```

`receiver apply` adds the receiver: it records the configuration and the pin
on `wherry.operator`, creates the receiver's Kafka principal, and writes its
bundle, `bundles/receivers/nomad-1.bundle`, mode 0600 in a directory of mode
0700. The bundle holds the broker address, the principal and its Kafka
password, and no CA. Hand it to the receiver host over a channel you trust.

### 4. Install wherry and the plugin

On the receiver host, install wherry as in [Install wherry](#install-wherry),
then the Nomad plugin from its
[wherry-plugins release](https://github.com/benemon/wherry-plugins/releases),
verified as [Install plugins](operations.md#install-plugins) describes:

```sh
plugin=nomad version=<version> arch=amd64
base=https://github.com/benemon/wherry-plugins/releases/download/$plugin/$version
archive=wherry-plugin-${plugin}_${version}_linux_${arch}.tar.gz
curl -fsSL --remote-name-all "$base/$archive" "$base/checksums.txt" "$base/checksums.txt.sigstore.json"
cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp "^https://github\.com/benemon/wherry-plugins/\.github/workflows/release\.yml@refs/tags/$plugin/v" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
grep "  $archive\$" checksums.txt | sha256sum -c -
tar -xzf "$archive" --strip-components=1 "${archive%.tar.gz}/wherry-plugin-$plugin"
install -d /usr/local/libexec/wherry/plugins
install -m 0755 wherry-plugin-nomad /usr/local/libexec/wherry/plugins/
```

The binary in the archive has the SHA-256 that step 1 pinned. To upgrade
the plugin later, install the new version's binary, then record its pin in
`receivers/nomad-1.yaml` and run `receiver apply`, as
[Install plugins](operations.md#install-plugins) describes.

### 5. Place the bundle

On the receiver host, put the bundle where the local configuration names it,
readable by the receiver's user alone. Copy the broker's CA certificate,
`/etc/wherry/kafka-ca.pem` on the operator's machine, beside it:

```sh
install -m 0600 nomad-1.bundle /etc/wherry/nomad-1.bundle
install -m 0644 kafka-ca.pem /etc/wherry/kafka-ca.pem
```

### 6. Put the Nomad token at its source

On the receiver host, write the Nomad token's secret ID to
`/etc/wherry/nomad-token`, the file source the local configuration gives
`nomad-token`, readable by the receiver's user alone:

```sh
(umask 077 && cat > /etc/wherry/nomad-token)
```

The command reads the token from standard input. The receiver trims a
trailing line break from the value.

### 7. Write the receiver's local configuration

On the receiver host, write the [receiver local
configuration](configuration.md#receiver-local-configuration) to
`/etc/wherry/config.yaml`, the path `wherry receiver run` reads by default:

```yaml
bundle: /etc/wherry/nomad-1.bundle
ca_file: /etc/wherry/kafka-ca.pem
state_dir: /var/lib/wherry
plugin_dir: /usr/local/libexec/wherry/plugins
credentials:
  nomad-token:
    file: /etc/wherry/nomad-token
```

`ca_file` is the CA the receiver trusts the broker with, and without it the
system roots. `credentials` gives the platform credential `nomad-token`, which the
receiver configuration names in [Receiver step 1](#_1-write-the-receiver-configuration),
its source. Without the entry, the receiver exits with the error Receiver
step 1 shows.

`state_dir` keeps the receiver's signing key, and the pool keys while the
receiver is or was its pool's key custodian, and survives restarts. The
receiver reads the token on this host, and neither `wherry.operator` nor the
bundle holds it.

### 8. Start the receiver

On the receiver host:

```sh
wherry receiver run
```

The receiver creates its signing key, `signing.key`, in `/var/lib/wherry`,
registers it, configures both plugin profiles in one plugin process, and
logs:

```
receiver: <time> registered key: signing=<key id>
receiver: <time> configured: pool=build profiles=2 capacity=2
```

A configuration change ends the process, so run it under a supervisor that
restarts it. [Run a receiver](operations.md#run-a-receiver) covers that, and
running the receiver as a Nomad job.

### 9. Check the receiver

In the working directory, print the configuration the receiver runs:

```sh
wherry operator receiver show nomad-1
```

It prints the record, the same YAML `--dry-run` printed in
[Receiver step 2](#_2-check-the-receiver-configuration).

## Host

### 1. Add the host

In the working directory:

```sh
wherry operator host add laptop --tenant acme
```

`host add` records host `laptop` of tenant `acme`, creates its Kafka
principal and key topic, and writes its bundle,
`bundles/hosts/acme/laptop.bundle`, mode 0600. Hand the bundle to `laptop`'s user
over a channel you trust. It holds the host's Kafka password. The bundle
carries nothing about the broker's CA: `laptop` trusts the broker through the
system's trusted roots, or, for a private CA, through the CA file whoever
distributes the Kafka CA puts on it.

### 2. Install wherry

On `laptop`, install wherry as in [Install wherry](#install-wherry).

### 3. Initialise the host

On `laptop`, put the broker's CA certificate, `/etc/wherry/kafka-ca.pem` on
the operator's machine, at `~/kafka-ca.pem`. `host init` asks for it only on
a terminal. Without one, place it at `~/.config/wherry/ca.pem` before
`host init` runs.

Keep the Claude Code OAuth token in a file readable by its owner
alone. The command reads the token from standard input:

```sh
(umask 077 && cat > ~/claude-oauth-token)
```

Then give `host init` the bundle:

```sh
wherry host init laptop.bundle
```

```
CA file for the broker's certificate (blank for the system roots): ~/kafka-ca.pem
wrote /home/me/.config/wherry/config.yaml
<key ID>
pool build delivers credential claude-oauth-token to its agents as CLAUDE_CODE_OAUTH_TOKEN.
Source for credential claude-oauth-token (file:PATH, env:VAR or command:ARGV; blank skips): file:~/claude-oauth-token
wrote /home/me/.config/wherry/config.yaml
```

`host init` copies the bundle to `bundle.yaml` in the host's
[configuration directory](configuration.md#client-configuration),
`~/.config/wherry/` here, mode 0600 in a directory of mode 0700. It creates
`host.key` there, registers it on the host's key topic, and prints its key
ID, its RFC 7638 thumbprint in base64url. On a terminal, it asks for a CA file when its configuration names
none and the directory holds no `ca.pem`. Leave it blank for a broker
whose certificate the system's roots trust. It then reads the terms of every pool
`acme` is entitled to and asks for a source for each credential their agents
take that the configuration lacks: here `claude-oauth-token`, which the pool
terms name in [Operator step 3](#_3-write-the-pool-terms). It writes
`config.yaml` there, the [client
configuration](configuration.md#client-configuration) every host command
reads, with the source's reference, never the token:

```yaml
bundle: /home/me/.config/wherry/bundle.yaml
ca_file: /home/me/kafka-ca.pem
credentials:
  claude-oauth-token:
    file: /home/me/claude-oauth-token
```

`wherry submit` encrypts the file's contents byte for byte in the task's
envelope under the pool's key, and the receiver that claims the task opens
it through the pool's key custodian. A credential can also come from an
environment variable of the submitting process,
`env:CLAUDE_CODE_OAUTH_TOKEN`, or from a command's output,
`command:op read op://wherry/claude/credential`
([credential sources](security.md#credential-sources)). When the agent's
variable is already set where `host init` runs, it offers that variable as
the source, defaulting to No ([suggested sources](security.md#suggested-sources)).

A credential left blank stays unconfigured, and `wherry submit` refuses a
task that needs it:

```
invalid input: auth required: credential claude-oauth-token is not in this host's configuration
```

Run `wherry host init` again, without the bundle, to give it a source. A
re-run keeps every entry and asks only for what is missing, such as the
credentials of a pool `acme` is entitled to later.

On `laptop`, `wherry host receivers` lists the receiver as the tenant sees
it:

```sh
wherry host receivers
```

```
receiver=nomad-1 pool=build capacity=2 in_flight=0 advertisement_age=4s status=live
  profile=container plugin=nomad agent=claude ready=true labels=isolation=container inline_limit=45056 credentials=claude-oauth-token:env=CLAUDE_CODE_OAUTH_TOKEN
  profile=host plugin=nomad agent=claude ready=true labels=isolation=host inline_limit=45056 credentials=claude-oauth-token:env=CLAUDE_CODE_OAUTH_TOKEN
```

`advertisement_age` is the age of the receiver's latest advertisement, and
`status=live` holds while it is within 120 seconds. `ready=true` means the
receiver offers the plugin profile. `inline_limit` is the most inline input
bytes the Nomad plugin delivers. `credentials` lists the agent's credentials
from the pool terms, each with its target. One `laptop` has no source for
shows as `claude-oauth-token:env=CLAUDE_CODE_OAUTH_TOKEN(missing)`.

### 4. Connect a harness


On `laptop`, install the wherry plugin in the harness, so that its agent
submits tasks through `wherry host mcp`. In Claude Code:

```sh
claude plugin marketplace add benemon/wherry
claude plugin install wherry@wherry
```

Run `/reload-plugins` in the session, then ask for the `receivers` tool,
which lists `nomad-1`. The plugin's server reads
`~/.config/wherry/config.yaml`.
[Harness integration](operations.md#harness-integration) covers other
harnesses and a configuration at another path.

### 5. Submit a task

On `laptop`, from the command line:

```sh
wherry submit --prompt "Summarise the release notes in three sentences."
```

The task takes the pool's default agent, `claude`, and an empty selector, so
it runs on plugin profile `container`. The host prints the run ID and the
pool:

```
run_id=run-<id> pool=build
```

`--selector isolation=host` runs it on plugin profile `host`.

### 6. Wait for the run

On `laptop`:

```sh
wherry wait run-<id>
```

`wait` prints the agent's message and a line for each output it wrote,
once the run ends. It exits 0 for `TASK_STATE_COMPLETED` and with a code
per state otherwise, as [exit codes](commands.md#exit-codes) lists. It
waits 2 hours 10 minutes by default (`--timeout` changes that). The run
needs nothing more from the host once `submit` returns.

In another terminal, `wherry logs -f run-<id>` prints the agent's
transcript lines as the agent runs and exits when the run ends.
`wherry logs run-<id> > run.log` keeps the transcript.

`wherry events run-<id>` prints the run's status history.

### If the run needs a credential

`wherry submit` refuses a task whose credential source does not resolve,
with `resolve credential <name>: <cause>`. A run that ends in
`TASK_STATE_FAILED` with a message naming a credential found it missing or
refused while it ran. The run does not resume.

1. Fix the credential's source in `credentials` in the host configuration.
2. Submit the task again with `wherry submit`, which starts a new run.
