# Quickstart

This guide provisions one pool with one Nomad receiver, entitles one tenant
with one host, and runs a first task. [Key concepts](concepts.md) explains the
parts involved.

## Prerequisites

- A Kafka cluster prepared as in
  [Prepare Kafka](operations.md#prepare-kafka), with its super user and
  `wherry.operator` credentials on the operator's machine.
- A Nomad cluster with a namespace for agent jobs (`agents` below) and a
  token or workload identity carrying the
  [receiver policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy).
- The `wherry` binary on the operator's machine, the host and the receiver
  host, installed from a release as in [Install wherry](#install-wherry).
- `wherry-plugin-nomad` on the receiver host, installed from a
  [wherry-plugins release](https://github.com/benemon/wherry-plugins/releases)
  in `/usr/local/libexec/wherry/plugins` as in
  [Install adapter plugins](operations.md#install-adapter-plugins).
- An agent the Nomad nodes can run: an image for a container driver, or an
  install on the node for `exec`. The
  [agent contract](configuration.md#agent-contract) lists what it must do.
- The model credential the agent needs, in a file on the host.

### Install wherry

Each [release](https://github.com/benemon/wherry/releases) carries
`wherry_<version>_<os>_<arch>.tar.gz` for `linux` and `darwin` on `amd64` and
`arm64`, `checksums.txt` with each archive's SHA-256, and
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

## 1. Set up `wherry admin`

On the operator's machine, export the six `WHERRY_TRUST_*` variables in
[Set up `wherry admin`](operations.md#set-up-wherry-admin).

## 2. Create a pool

Write `build-terms.yaml`:

```yaml
agents:
  - name: coder
    credentials:
      - {name: model-api-key, env: MODEL_API_KEY}
default_agent: coder
delivery_limit: 5
lock_duration: 15s
producer_byte_rate: 1048576
request_percentage: 10
```

```sh
wherry admin pool create build --config-file build-terms.yaml
```

The agent gets `model-api-key` in the environment variable `MODEL_API_KEY`.

## 3. Entitle a tenant

```sh
wherry admin pool entitle build --tenant alice
```

## 4. Add the host

```sh
wherry admin host add laptop --tenant alice --bundle-file alice-laptop.bundle
```

Copy `alice-laptop.bundle` to `~/.config/wherry/` on the host, keeping mode
0600.

## 5. Add the receiver

Write the configuration as a directory, `nomad-1/`, with one file for the
receiver, one for each platform, and one for each adapter:

```
nomad-1/
  receiver.yaml
  platforms/
    nomad.yaml
  adapters/
    10-container.yaml
```

`nomad-1/receiver.yaml`:

```yaml
capacity: 2
```

`nomad-1/platforms/nomad.yaml`:

```yaml
name: nomad
type: nomad
config:
  address: https://nomad.example.com:4646
  ca_file: /etc/wherry/nomad-ca.pem
  namespace: agents
```

`nomad-1/adapters/10-container.yaml`:

```yaml
name: container
platform: nomad
agent: coder
labels:
  isolation: container
config:
  runtime:
    driver: docker
    config:
      image: registry.example.com/agent:1.0
      command: /bin/sh
      args:
        - -c
        - exec agent-run < "$NOMAD_TASK_DIR/prompt.txt"
    resources:
      cpu: 1000
      memory_mb: 2048
```

`agent-run` stands for the agent's headless command. The adapter delivers
the tenant's sealed `model-api-key` to the job as `MODEL_API_KEY`.

```sh
wherry admin receiver add nomad-1 --pool build \
  --config-dir nomad-1 --bundle-file nomad-1.bundle
```

Copy `nomad-1.bundle` to `/etc/wherry/` on the receiver host, keeping mode
0600.

## 6. Start the receiver

```sh
NOMAD_TOKEN=<token> wherry receiver run \
  --bundle-file /etc/wherry/nomad-1.bundle --state-dir /var/lib/wherry \
  --plugin-dir /usr/local/libexec/wherry/plugins
```

The receiver creates its signing key, `signing.key`, in `/var/lib/wherry`,
registers it, and logs:

```
receiver: <time> registered key: signing=<key id>
receiver: <time> configured: pool=build adapters=1 capacity=2
```

[Run a receiver](operations.md#run-a-receiver) covers running it
under a supervisor, or as a Nomad job.

## 7. Configure the host

On the host, put the model credential in a file and write
`~/.config/wherry/config.yaml`, keeping the credential file at mode 0600:

```yaml
bundle: ~/.config/wherry/alice-laptop.bundle
credentials:
  model-api-key:
    file: ~/.config/wherry/model-api-key
```

When a receiver claims a task and asks for the credential, `wherry await`
seals the file's contents byte for byte to that receiver. A credential can
also come from an environment variable of the answering process,
`env: MODEL_API_KEY`, or from a command's output,
`command: [op, read, "op://wherry/model/credential"]`
([client configuration](configuration.md#client-configuration)).

## 8. Register the host key

```sh
wherry init
```

`init` creates `~/.config/wherry/host.key`, registers it on the host's key
topic, and prints its key ID, `sha256:<base64url>`. `wherry key` prints the
same ID later.

## 9. Check the receiver

```sh
wherry receivers
```

```
receiver=nomad-1 pool=build capacity=2 in_flight=0 heartbeat_age=12s status=live
  adapter=container agent=coder ready=true labels=isolation=container credentials=model-api-key
```

A credential this host has no source for shows as
`model-api-key(missing)`, and `wherry submit` refuses the task with
`auth required: credential model-api-key is not in this host's configuration`.

## 10. Submit a task

```sh
wherry submit --prompt "Summarise the release notes in three sentences."
```

The task takes the pool's default agent, `coder`, and an empty selector.
The host prints the run ID and the pool:

```
run_id=run-<id> pool=build
```

`--selector isolation=container` asks for the adapter's label explicitly.

## 11. Await the result

```sh
wherry await run-<id>
```

`await` prints the final state on its first line and the agent's message after
it, writes the run's transcript to `transcript.jsonl` beside the outputs, and
exits non-zero for any state other than `TASK_STATE_COMPLETED`. It waits
2 hours 10 minutes by default (`--timeout` changes that). While it waits it answers `nomad-1`'s request for
`model-api-key`. Start it within 5 minutes of `nomad-1` claiming the task,
or the run ends `TASK_STATE_FAILED`.

`wherry events run-<id>` prints the run's status history.

### If the run needs a credential

A run that ends in `TASK_STATE_FAILED` with a message naming a credential
received no seal for it within 5 minutes: the host was not running `await`,
or its source did not resolve, which the answerer logs as
`run <id>: resolve credential <name>: <cause>`. The run does not resume.

1. Fix the credential's source in `credentials` in the host configuration.
2. Submit the task again with `wherry submit`, which starts a new run.
3. Run `wherry await` with the new run ID, which answers its request.
