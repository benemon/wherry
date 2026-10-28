# Quickstart

This guide sets up one deployment and runs a first task on it, in three
parts: the operator's, the receiver's and the host's. The deployment is pool
`build`, tenant `acme` with host `laptop`, and receiver `nomad-1` running
agent `claude` on a Nomad cluster. [Key concepts](concepts.md) explains the
parts involved.

Each step names the machine it runs on: the operator's machine, which holds
the Kafka credentials `wherry admin` uses; the receiver host, which runs
receiver `nomad-1`; and `laptop`, the host the tenant submits from.

Two pairs of files name the same credentials:

- The pool terms give agent `claude` the credential `claude-oauth-token`.
  The host's configuration maps that name to a source on `laptop`.
- The receiver configuration names the platform credential `nomad-token`,
  the Nomad token the receiver authenticates with. The receiver's local
  configuration maps that name to a source on the receiver host.

The configuration on `wherry.admin` holds the names. Each value is read from
its source on the machine that resolves it.

## Prerequisites

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

## Install wherry

Each [release](https://github.com/benemon/wherry/releases) carries
`wherry_<version>_<os>_<arch>.tar.gz` for `linux` and `darwin` on `amd64` and
`arm64`, `checksums.txt` with each archive's SHA-256,
`checksums.txt.sigstore.json`, the keyless signature over it, and
`wherry.cdx.json`, a CycloneDX SBOM. `<version>` is a release tag. Download the archive and the checksums, verify the checksums'
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

Install wherry this way on the operator's machine before the operator's
steps.

## Operator

### 1. Prepare Kafka

On the Kafka cluster, enable share groups, a TLS listener with
SCRAM-SHA-512, an authoriser, and the settings
[Prepare Kafka](operations.md#prepare-kafka) lists, and create the two
SCRAM principals it names: the super user, here `kafka-admin`, and
`wherry.operator`. This step happens once per cluster.

On the operator's machine, put the listener's CA certificate in
`/etc/wherry/kafka-ca.pem`, the super user's password in
`/etc/wherry/admin-password` and `wherry.operator`'s password in
`/etc/wherry/operator-password`, each with mode 0600.

### 2. Set up `wherry admin`

On the operator's machine, export the five variables `wherry admin` reads.
`WHERRY_TRUST_BROKER` and the CA file also go into every bundle, so the
address is the one the receiver host and `laptop` reach:

```sh
export WHERRY_TRUST_BROKER=kafka.example.com:9093
export WHERRY_TRUST_CA=/etc/wherry/kafka-ca.pem
export WHERRY_TRUST_ADMIN_USER=kafka-admin
export WHERRY_TRUST_ADMIN_PASSWORD_FILE=/etc/wherry/admin-password
export WHERRY_TRUST_OPERATOR_PASSWORD_FILE=/etc/wherry/operator-password
```

### 3. Write the pool terms

On the operator's machine, write the [pool terms](configuration.md#pool-terms)
to `build-terms.yaml`:

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
[Host step 3](#_3-write-the-host-configuration). A host without one has
`wherry submit` refuse the task:

```
invalid input: auth required: credential claude-oauth-token is not in this host's configuration
```

[Create a pool](operations.md#create-a-pool) explains `delivery_limit`,
`lock_duration` and the other fields.

### 4. Create the pool

On the operator's machine:

```sh
wherry admin pool create build --config-file build-terms.yaml
```

`pool create` records the terms on `wherry.admin` and creates the pool's
topics and share group.

### 5. Entitle the tenant

On the operator's machine:

```sh
wherry admin pool entitle build --tenant acme
```

Tenant `acme` may now submit to `build` from any host the operator adds for
it, as in [Host step 1](#_1-add-the-host).

## Receiver

### 1. Write the receiver configuration

On the operator's machine, write the
[receiver configuration](configuration.md#receiver-configuration) to
`nomad-1.yaml`:

```yaml
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

The receiver holds two runs at once and runs one plugin, `nomad`, whose
`config` holds the settings both plugin profiles share: the cluster's
address, its CA file on the receiver host, the namespace, the name of the
Nomad token, and a Docker runtime that starts Claude Code in the task
directory on the prompt file. The
[Nomad plugin's settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
list every key.

Plugin profile `container` sets no `config`, so it runs the plugin's as it
stands. Plugin profile `host` places its jobs in node pool `agents-host`,
switches the driver to `exec`, and sets `image: null`, which removes the
inherited `image` from its driver configuration and keeps `command` and
`args`. [Plugin profile merge](configuration.md#plugin-profile-merge) gives
the rules.

Both plugin profiles launch `claude` and deliver its `claude-oauth-token` at
the target the pool terms give it, so neither names an agent credential.
Their labels tell them apart: a task with no selector runs on `container`,
the first in routing order, and `--selector isolation=host` runs on `host`.

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
with `--config-dir`.

### 2. Check the receiver configuration

On the operator's machine, check the file against the pool's terms:

```sh
wherry admin receiver add nomad-1 --pool build --config-file nomad-1.yaml --dry-run
```

`--dry-run` writes nothing and prints each plugin profile as the record will
hold it, with the plugin's `config` merged in and its keys sorted:

```yaml
capacity: 2
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

On the operator's machine:

```sh
wherry admin receiver add nomad-1 --pool build \
  --config-file nomad-1.yaml --bundle-file nomad-1.bundle
```

`receiver add` records the configuration on `wherry.admin`, creates the
receiver's Kafka principal, and writes its bundle, `nomad-1.bundle`, with mode
0600. The bundle holds the receiver's Kafka password; hand it to the receiver
host over a channel you trust.

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
install -d -o root -g root -m 0755 /usr/local/libexec/wherry/plugins
install -o root -g root -m 0755 wherry-plugin-nomad /usr/local/libexec/wherry/plugins/
```

The receiver refuses a plugin directory or binary that a user other than
root or the receiver's owns, or that group or others can write.

### 5. Place the bundle

On the receiver host, put the bundle where the local configuration names it,
readable by the receiver's user alone:

```sh
install -m 0600 nomad-1.bundle /etc/wherry/nomad-1.bundle
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
state_dir: /var/lib/wherry
plugin_dir: /usr/local/libexec/wherry/plugins
credentials:
  nomad-token:
    file: /etc/wherry/nomad-token
```

`credentials` gives the platform credential `nomad-token`, which the
receiver configuration names in [Receiver step 1](#_1-write-the-receiver-configuration),
its source. Without the entry, the plugin profile fails to configure and the
receiver exits with:

```
profile container: credential "nomad-token" has no source in the receiver's configuration
```

`state_dir` keeps the receiver's signing key and survives restarts. The
receiver reads the token on this host, and neither `wherry.admin` nor the
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

On the operator's machine, print the configuration the receiver runs:

```sh
wherry admin receiver show nomad-1
```

It prints the record, the same YAML `--dry-run` printed in
[Receiver step 2](#_2-check-the-receiver-configuration):

```yaml
capacity: 2
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

On `laptop`, once it is set up as in [Host](#host), `wherry receivers`
lists the receiver as the tenant sees it:

```sh
wherry receivers
```

```
receiver=nomad-1 pool=build capacity=2 in_flight=0 heartbeat_age=4s status=live
  profile=container plugin=nomad agent=claude ready=true labels=isolation=container inline_limit=45056 credentials=claude-oauth-token
  profile=host plugin=nomad agent=claude ready=true labels=isolation=host inline_limit=45056 credentials=claude-oauth-token
```

`heartbeat_age` is the age of the receiver's latest advertisement, and
`status=live` holds while it is within 120 seconds. `ready=true` means the
receiver offers the plugin profile. `inline_limit` is the most inline input
bytes the Nomad plugin delivers. `credentials` lists the agent's credentials
from the pool terms; one `laptop` has no source for shows as
`claude-oauth-token(missing)`.

## Host

### 1. Add the host

On the operator's machine:

```sh
wherry admin host add laptop --tenant acme --bundle-file laptop.bundle
```

`host add` records host `laptop` of tenant `acme`, creates its Kafka
principal and key topic, and writes its bundle, `laptop.bundle`, with mode
0600. Hand the bundle to `laptop`'s user over a channel you trust; it holds
the host's Kafka password. On `laptop`, put it where the host configuration
names it:

```sh
install -d -m 0700 ~/.config/wherry
install -m 0600 laptop.bundle ~/.config/wherry/laptop.bundle
```

### 2. Install wherry

On `laptop`, install wherry as in [Install wherry](#install-wherry).

### 3. Write the host configuration

On `laptop`, put the Claude Code OAuth token in
`~/.config/wherry/credentials/claude-oauth-token`, readable by its owner
alone. The command reads the token from standard input:

```sh
install -d -m 0700 ~/.config/wherry/credentials
(umask 077 && cat > ~/.config/wherry/credentials/claude-oauth-token)
```

Then write the [client configuration](configuration.md#client-configuration)
to `~/.config/wherry/config.yaml`, the path every host command reads by
default:

```yaml
bundle: ~/.config/wherry/laptop.bundle
credentials:
  claude-oauth-token:
    file: ~/.config/wherry/credentials/claude-oauth-token
```

`credentials` gives `claude-oauth-token`, which the pool terms name in
[Operator step 3](#_3-write-the-pool-terms), its source. Without the entry,
`wherry submit` refuses the task:

```
invalid input: auth required: credential claude-oauth-token is not in this host's configuration
```

When a receiver claims a task and asks for the credential, `wherry await`
seals the file's contents byte for byte to that receiver. A credential can
also come from an environment variable of the answering process,
`env: CLAUDE_CODE_OAUTH_TOKEN`, or from a command's output,
`command: [op, read, "op://wherry/claude/credential"]`
([credential sources](security.md#credential-sources)).

### 4. Register the host key

On `laptop`:

```sh
wherry init
```

`init` creates `~/.config/wherry/host.key`, registers it on the host's key
topic, and prints its key ID, `sha256:<base64url>`. `wherry key` prints the
same ID later. `wherry receivers` now lists `nomad-1` as
[Receiver step 9](#_9-check-the-receiver) shows.

### 5. Connect a harness

On `laptop`, install the wherry plugin in the harness, so that its agent
submits tasks through `wherry mcp`. In Claude Code:

```sh
claude plugin marketplace add benemon/wherry
claude plugin install wherry@wherry
```

Run `/reload-plugins` in the session, then ask for the `receivers` tool,
which lists `nomad-1`. The plugin's server reads
`~/.config/wherry/config.yaml`.
[Harness integration](operations.md#harness-integration) covers other
harnesses and a configuration at another path.

### 6. Submit a task

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

### 7. Await the result

On `laptop`:

```sh
wherry await run-<id>
```

`await` prints the final state on its first line and the agent's message after
it, writes the run's transcript to `transcript.jsonl` beside the outputs, and
exits non-zero for any state other than `TASK_STATE_COMPLETED`. It waits
2 hours 10 minutes by default (`--timeout` changes that). While it waits it
answers `nomad-1`'s request for `claude-oauth-token`. Start it within 5
minutes of `nomad-1` claiming the task, or the run ends `TASK_STATE_FAILED`.

`wherry events run-<id>` prints the run's status history.

### If the run needs a credential

A run that ends in `TASK_STATE_FAILED` with a message naming a credential
received no seal for it within 5 minutes: the host was not running `await`,
or its source did not resolve, which the answerer logs as
`run <id>: resolve credential <name>: <cause>`. The run does not resume.

1. Fix the credential's source in `credentials` in the host configuration.
2. Submit the task again with `wherry submit`, which starts a new run.
3. Run `wherry await` with the new run ID, which answers its request.
