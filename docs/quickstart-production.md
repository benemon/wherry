# Quickstart: production

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

## Operator

Each step names the machine it runs on:

- the operator's machine, which holds the installation's working directory,
  where `wherry operator` keeps its connection, the pool and receiver files
  and every bundle;
- the Nomad cluster, which runs receiver `nomad-1` as a Nomad job, driven
  with the `nomad` CLI from the operator's machine;
- `laptop`, the host the tenant submits from.

Two pairs of files name the same credentials:

- The pool terms give agent `claude` the credential `claude-oauth-token`.
  The host's configuration maps that name to a source on `laptop`.
- The receiver configuration names the platform credential `nomad-token`,
  the Nomad token the receiver authenticates with. The receiver's local
  configuration maps that name to a source in the receiver's job.

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
  - clients that can run the receiver job of the
    [Nomad example](https://github.com/benemon/wherry-plugins/tree/main/nomad#run-the-receiver-in-nomad):
    the docker driver, the CNI reference plugins for its bridge network, a
    client running as root, and Nomad 1.10 or later for its dynamic host
    volume;
  - with ACLs enabled, the
    [receiver policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy)
    in `policy.hcl`, and a token that can create host volumes, put
    variables, apply policies and run jobs.
- A checkout of [wherry-plugins](https://github.com/benemon/wherry-plugins)
  on the operator's machine, for `examples/nomad`, and the receiver image
  its `examples/nomad/Containerfile` builds, as
  [Run the receiver in a container](https://github.com/benemon/wherry-plugins/tree/main/nomad#run-the-receiver-in-a-container)
  describes, pushed where the Nomad clients pull from, here
  `registry.example.com/wherry-receiver:<version>`.
- A Claude Code OAuth token, which `claude setup-token` prints, on `laptop`.
- An agent that meets the [agent contract](configuration-receiver.md#agent-contract),
  here Claude Code in the image and on the `agents-host` nodes.

### 1. Prepare Kafka

On the Kafka cluster, enable share groups, a TLS listener with
SCRAM-SHA-512, an authoriser, and the settings
[Prepare Kafka](operations-operator.md#prepare-kafka) lists, and create the Kafka
super user's SCRAM credential, here `kafka-admin`. This step happens once per
cluster. On a Strimzi cluster, set the User Operator's ignored-users pattern
before `operator init`, as [Strimzi](operations-operator.md#strimzi) shows, so that it
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

In the working directory, write the [pool terms](configuration-operator.md#pool-terms)
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

[Create a pool](operations-operator.md#create-a-pool) explains `delivery_limit`,
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
wherry operator tenant add acme
wherry operator tenant pool add acme build
```

Tenant `acme` may now submit to `build` from any host the operator adds for
it, as in [Host step 1](#_1-add-the-host).

## Receiver

Receiver `nomad-1` runs as a Nomad job, the receiver container of the
[Nomad example](https://github.com/benemon/wherry-plugins/tree/main/nomad#run-the-receiver-in-nomad).
The steps below give what wherry needs of it, and the example's README
gives the rest. On Kubernetes or under systemd, the
[Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#run-the-receiver-in-the-cluster)
and
[systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd#run-the-receiver-as-a-systemd-service)
examples take the place of the pin's source in step 1 and of steps 4 to 8.

### 1. Write the receiver configuration

The receiver starts the Nomad plugin only from a binary whose SHA-256 its
record pins. The receiver image builds the plugin, so the pin is the SHA-256
of the binary inside the image, as
[The plugin pin](https://github.com/benemon/wherry-plugins/tree/main/nomad#the-plugin-pin)
describes. On the operator's machine, print it:

```sh
docker run --rm --entrypoint sha256sum registry.example.com/wherry-receiver:<version> \
  /usr/local/libexec/wherry/plugins/wherry-plugin-nomad
```

In the working directory, write the
[receiver configuration](configuration-receiver.md#receiver-configuration) to
`receivers/nomad-1.yaml`, with that SHA-256 as the pin (shown here as
`0123…`):

```yaml
pool: build
plugin_sha256:
  # The SHA-256 of the plugin binary in the receiver image, the one binary the receiver may run.
  nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
capacity: 2
plugins:
  nomad:
    # Both plugin profiles inherit this config; each states only how it differs.
    config:
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
profiles share. It names no `address`, so the plugin reaches Nomad through
the task API socket the receiver's job sets as `NOMAD_ADDR`. The
[Nomad plugin's settings](https://github.com/benemon/wherry-plugins/tree/main/nomad#settings)
list every key. `image: null` removes the inherited `image` from profile
`host`, as [Plugin profile merge](configuration-receiver.md#plugin-profile-merge)
describes. Both profiles launch `claude`, which takes `claude-oauth-token`
at the target the pool terms give it. A task with no selector runs on
`container`, the first in routing order, and `--selector isolation=host`
runs on `host`.

`credential: nomad-token` names a [platform
credential](configuration-receiver.md#platform-credentials). The name is the link to
the receiver's job: its local configuration gives `nomad-token` a source in
[Receiver step 5](#_5-write-the-receiver-s-local-configuration). A receiver
without one fails to configure the plugin profile, and exits with:

```
profile container: credential "nomad-token" has no source in the receiver's configuration
```

The same configuration also works as a
[directory](configuration-receiver.md#receiver-configuration-directory) with one file
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
password, and no CA. Step 6 hands it to the receiver's job through a Nomad
Variable.

### 4. Create the state volume

The receiver's state directory keeps its signing key, and the pool keys while
the receiver is or was its pool's key custodian. It must survive restarts,
reschedules and job updates. A receiver that loses it cannot start under
the same name again. The example keeps it on a dynamic host volume. From the
wherry-plugins checkout:

```sh
nomad volume create examples/nomad/wherry-receiver.volume.hcl
```

### 5. Write the receiver's local configuration

In the wherry-plugins checkout, write the [receiver local
configuration](configuration-receiver.md#receiver-local-configuration) to
`config.yaml`, from `examples/nomad/config.yaml`. The job renders it at
`/local/config.yaml` and runs `wherry receiver run --config-file
/local/config.yaml`:

```yaml
bundle: /secrets/receiver.bundle
ca_file: /secrets/kafka-ca.pem
state_dir: /var/lib/wherry/state
plugin_dir: /usr/local/libexec/wherry/plugins
drain_timeout: 20s
credentials:
  nomad-token:
    env: NOMAD_TOKEN
```

- `bundle` is where the job renders the bundle, readable by the image's
  user alone.
- `ca_file` is where the job renders the broker's CA. The broker's
  certificate here chains to a private CA. Without `ca_file` the receiver
  trusts the system roots.
- `state_dir` is on the state volume of step 4.
- `plugin_dir` is where the image installs the plugin.
- `drain_timeout` stays at least 10 seconds below the job's `kill_timeout`,
  30 seconds, since a stop takes up to `drain_timeout` plus 5 seconds
  ([Drain timeout and the stop window](configuration-receiver.md#drain-timeout-and-the-stop-window)).
- `credentials` gives the platform credential `nomad-token`, which the
  receiver configuration names in
  [Receiver step 1](#_1-write-the-receiver-configuration), its source:
  `NOMAD_TOKEN`, the token of the job's
  [workload identity](https://developer.hashicorp.com/nomad/docs/concepts/workload-identity).
  Neither `wherry.operator` nor the bundle holds it.

### 6. Put the bundle, configuration and CA in a Nomad Variable

The job reads the items `bundle`, `config` and `ca` of the Nomad Variable
`nomad/jobs/wherry-receiver` through its workload identity. From the
wherry-plugins checkout:

```sh
nomad var put nomad/jobs/wherry-receiver \
  bundle=@<working directory>/bundles/receivers/nomad-1.bundle \
  config=@config.yaml ca=@/etc/wherry/kafka-ca.pem
```

The example job's templates restart the receiver's task when an item
changes
([template `change_mode`](https://developer.hashicorp.com/nomad/docs/job-specification/template#change_mode)).

### 7. Attach the receiver policy

With ACLs enabled, attach the receiver policy to the job before it runs, so
that the job's workload identity can reach the agents' namespace:

```sh
nomad acl policy apply -namespace default -job wherry-receiver wherry-receiver policy.hcl
```

### 8. Run the receiver

From the wherry-plugins checkout:

```sh
nomad job run -var image=registry.example.com/wherry-receiver:<version> examples/nomad/wherry-receiver.nomad.hcl
```

The receiver creates its signing key, `signing.key`, in
`/var/lib/wherry/state`, registers it, configures both plugin profiles in
one plugin process, and logs, as `nomad alloc logs` shows:

```
receiver: <time> registered key: signing=<key id>
receiver: <time> configured: pool=build profiles=2 capacity=2
```

A configuration change ends the process, and Nomad restarts it.
[Run a receiver](operations-receivers.md#run-a-receiver) lists what every receiver
needs. To upgrade the receiver image later, record the new image's pin
with `receiver apply`, then run the job with the new image, as
[Install plugins](operations-receivers.md#install-plugins) describes.

### 9. Check the receiver

In the working directory, print the configuration the receiver runs:

```sh
wherry operator receiver show nomad-1
```

It prints the record, the same YAML `--dry-run` printed in
[Receiver step 2](#_2-check-the-receiver-configuration). Once `laptop` is
set up, `wherry host receivers` shows the receiver `live`, as
[Host step 3](#_3-initialise-the-host) shows.

## Host

### 1. Add the host

In the working directory:

```sh
wherry operator tenant host add acme laptop
```

`tenant host add` records host `laptop` of tenant `acme`, creates its Kafka
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
[configuration directory](configuration-host.md#client-configuration),
`~/.config/wherry/` here, mode 0600 in a directory of mode 0700. It creates
`host.key` there, registers it on the host's key topic, and prints its key
ID, its RFC 7638 thumbprint in base64url. On a terminal, it asks for a CA file when its configuration names
none and the directory holds no `ca.pem`. Leave it blank for a broker
whose certificate the system's roots trust. It then reads the terms of every pool
`acme` is entitled to and asks for a source for each credential their agents
take that the configuration lacks: here `claude-oauth-token`, which the pool
terms name in [Operator step 3](#_3-write-the-pool-terms). It writes
`config.yaml` there, the [client
configuration](configuration-host.md#client-configuration) every host command
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
from the pool terms, each with its target. A credential `laptop` has no source for
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
[Harness integration](operations-hosts.md#harness-integration) covers other
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
