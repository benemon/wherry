# Quickstart: production

Set up wherry on a Nomad cluster and run a first task from your laptop.

## What you'll build

You'll work in three places.
Each part of this guide happens in one of them.

| Place | What runs there | Where its files live |
| --- | --- | --- |
| Operator machine | `wherry operator`, `docker` and the `nomad` CLI | the working directory `wherry-prod/` |
| Nomad cluster | the receiver `nomad-1`, and the agent jobs it starts | the Nomad Variable `nomad/jobs/wherry-receiver` and a host volume |
| Your laptop | `wherry host` and your coding agent | `~/.config/wherry/` |

The names used throughout are pool `build`, tenant `acme`, receiver `nomad-1` and host `laptop`.

### The files you'll make

| File | Made by | Lives in | What it is |
| --- | --- | --- | --- |
| `operator.yaml`, `operator.password` | `operator init` | `wherry-prod/` | how `wherry operator` connects to Kafka |
| `kafka-ca.pem` | you, from your Kafka cluster | `wherry-prod/` | the CA your broker's certificate chains to |
| `pools/build.yaml` | you | `wherry-prod/` | the pool's terms: its agents and limits |
| `receivers/nomad-1.yaml` | you | `wherry-prod/` | the receiver's configuration: its pool, plugin pin and profiles |
| `receivers/nomad-1.config.yaml` | you, from the plugin example | `wherry-prod/` | the receiver's local configuration: where it finds its files on Nomad |
| `bundles/receivers/nomad-1.bundle` | `receiver apply` | `wherry-prod/` | the receiver's Kafka identity: broker, principal and password |
| `bundles/hosts/acme/laptop.bundle` | `tenant host add` | `wherry-prod/` | your laptop's Kafka identity |
| `nomad/wherry-receiver.nomad.hcl`, `nomad/wherry-receiver.volume.hcl` | you, from the plugin example | `wherry-prod/` | the receiver's Nomad job and its state volume |
| `config.yaml`, `bundle.yaml`, `host.key`, `ca.pem` | `host init` and you | `~/.config/wherry/` on your laptop | your laptop's configuration |

The bundles hold passwords.
`operator init` keeps `bundles/` and `operator.password` out of git through `wherry-prod/.gitignore`.

## Before you start

On the operator machine:

- Docker, and push access to a registry your Nomad clients pull from. This guide uses `registry.example.com`.
- The `nomad` CLI, with a token that can create host volumes, put variables, apply ACL policies and run jobs.
- A Kafka cluster that meets the [Kafka prerequisites](operations-operator.md#prepare-kafka), with a super user's
  SCRAM credential. This guide calls it `kafka-admin`. On Strimzi, set the
  [ignored-users pattern](operations-operator.md#strimzi) first.

On the Nomad cluster:

- Nomad 1.10 or later.
- A namespace for agent jobs, `agents`.
- Clients that run the `docker` driver as root, with the CNI reference plugins.
- An agent image your clients can pull, here `registry.example.com/claude-agent:1.0`, with Claude Code installed as
  `claude`. It must meet the [agent contract](configuration-receiver.md#agent-contract).

On your laptop:

- A Claude Code OAuth token, which `claude setup-token` prints.

## Install wherry

Install wherry on the operator machine and on your laptop:

```sh
brew install benemon/tap/wherry
```

::: details Install from a release archive instead
Each [release](https://github.com/benemon/wherry/releases) has an archive per OS and architecture, `checksums.txt`, and
a signature over the checksums. Verify the signature with
[cosign](https://docs.sigstore.dev/cosign/system_config/installation/), then the archive against the checksums:

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

On macOS, use `shasum -a 256 -c -` in place of `sha256sum -c -`.
:::

## Part 1: on the operator machine

### 1. Create the working directory

Put your Kafka cluster's CA certificate and the super user's password where `operator init` can read them:

```sh
mkdir wherry-prod && cd wherry-prod
cp /path/to/your/kafka-ca.pem kafka-ca.pem
(umask 077 && cat > /tmp/kafka-admin-password)
```

Then initialise the directory:

```sh
wherry operator init --broker kafka.example.com:9093 --ca-file kafka-ca.pem \
  --super-user kafka-admin --super-user-password-file /tmp/kafka-admin-password
rm /tmp/kafka-admin-password
```

`operator init` creates the principal `wherry.operator` and grants it what wherry needs.
It doesn't keep the super user's password.
Every later `wherry operator` command in this directory connects as `wherry.operator`.

The broker address goes into every bundle, so use one your laptop and the receiver can reach.

### 2. Create the pool

Write the pool's terms to `pools/build.yaml`:

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

The pool runs one agent, `claude`.
Each run gets the credential `claude-oauth-token` in the variable Claude Code reads.
Your laptop supplies its value in Part 3.

Apply the terms:

```sh
wherry operator pool apply build
```

[Pool terms](configuration-operator.md#pool-terms) explains every field.

### 3. Add the tenant and give it the pool

```sh
wherry operator tenant add acme
wherry operator tenant pool add acme build
```

### 4. Build and push the receiver image

The receiver runs from an image that holds wherry and the Nomad plugin.
Build it from a checkout of [wherry-plugins](https://github.com/benemon/wherry-plugins) at the plugin release you want:

```sh
git clone --branch nomad/<plugin-version> https://github.com/benemon/wherry-plugins ../wherry-plugins
cd ../wherry-plugins
docker build -f examples/nomad/Containerfile -t registry.example.com/wherry-receiver:<version> \
  --build-arg VERSION=<plugin-version> --build-arg WHERRY_VERSION=<wherry-version> .
docker push registry.example.com/wherry-receiver:<version>
```

Print the plugin binary's SHA-256 from the image:

```sh
docker run --rm --entrypoint sha256sum registry.example.com/wherry-receiver:<version> \
  /usr/local/libexec/wherry/plugins/wherry-plugin-nomad
```

The receiver runs only a plugin binary with this SHA-256, so you'll record it in the next step.
[The plugin pin](https://github.com/benemon/wherry-plugins/tree/main/nomad#the-plugin-pin) explains why.

Copy the example's job file, volume file and local configuration into your working directory, then return there:

```sh
mkdir -p ../wherry-prod/nomad ../wherry-prod/receivers
cp examples/nomad/wherry-receiver.nomad.hcl examples/nomad/wherry-receiver.volume.hcl ../wherry-prod/nomad/
cp examples/nomad/config.yaml ../wherry-prod/receivers/nomad-1.config.yaml
cd ../wherry-prod
```

### 5. Add the receiver

Write the receiver's configuration to `receivers/nomad-1.yaml`, with the SHA-256 from step 4 as the pin:

```yaml
pool: build
plugin_sha256:
  # The SHA-256 of the plugin binary in the receiver image: the one binary the receiver may run.
  nomad: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
capacity: 2
plugins:
  nomad:
    config:
      namespace: agents
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
```

The receiver takes two tasks at a time and starts each as a Docker job in the `agents` namespace.
[Receiver configuration](configuration-receiver.md#receiver-configuration) explains every field.

Edit `receivers/nomad-1.config.yaml`, the receiver's local configuration you copied in step 4.
It says where the receiver finds its files inside the Nomad job.
Your broker's certificate chains to a private CA, so turn on its `ca_file` line:

```yaml
bundle: /secrets/receiver.bundle
ca_file: /secrets/kafka-ca.pem
state_dir: /var/lib/wherry/state
plugin_dir: /usr/local/libexec/wherry/plugins
metrics_addr: ":9464"
drain_timeout: 20s
```

Without this file, `receiver apply` asks for each value instead.
[Receiver local configuration](configuration-receiver.md#receiver-local-configuration) lists the questions and each
plugin example's answers.

Add the receiver:

```sh
wherry operator receiver apply nomad-1
```

This records the receiver and writes its bundle to `bundles/receivers/nomad-1.bundle`.

### 6. Add your laptop

```sh
wherry operator tenant host add acme laptop
```

This writes your laptop's bundle to `bundles/hosts/acme/laptop.bundle`.

> [!IMPORTANT]
> Hand-off: copy `bundles/hosts/acme/laptop.bundle` and `kafka-ca.pem` to your laptop over a channel you trust.
> The bundle holds the laptop's Kafka password.

## Part 2: on the Nomad cluster

Run these commands from `wherry-prod/` with the `nomad` CLI.

### 1. Create the state volume

The receiver keeps its signing key in a volume that outlives restarts and job updates:

```sh
nomad volume create nomad/wherry-receiver.volume.hcl
```

### 2. Hand the receiver its files

The job reads the bundle, the local configuration and the CA from one Nomad Variable:

```sh
nomad var put nomad/jobs/wherry-receiver \
  bundle=@bundles/receivers/nomad-1.bundle config=@receivers/nomad-1.config.yaml ca=@kafka-ca.pem
```

### 3. Attach the receiver's ACL policy

Skip this step if your cluster runs without ACLs.

1. Write the plugin's [ACL policy](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy) to
   `nomad/policy.hcl`:

   ```hcl
   namespace "agents" {
     capabilities = ["submit-job", "list-jobs", "read-job", "read-logs", "read-fs"]

     variables {
       path "nomad/jobs/agent-*" {
         capabilities = ["write", "read", "destroy"]
       }
     }
   }

   node {
     policy = "read"
   }

   node_pool "default" {
     policy = "read"
   }
   ```

2. Attach it to the receiver's job:

   ```sh
   nomad acl policy apply -namespace default -job wherry-receiver wherry-receiver nomad/policy.hcl
   ```

### 4. Run the receiver

```sh
nomad job run -var image=registry.example.com/wherry-receiver:<version> nomad/wherry-receiver.nomad.hcl
```

The receiver registers its signing key and starts taking work.
`nomad alloc logs` shows lines like these:

```
receiver: <time> registered key: signing=<key id>
receiver: <time> configured: pool=build profiles=1 capacity=2
```

## Part 3: on your laptop

### 1. Put the CA and your token in place

```sh
mkdir -p -m 0700 ~/.config/wherry
cp kafka-ca.pem ~/.config/wherry/ca.pem
(umask 077 && cat > ~/claude-oauth-token)
```

The last command reads your Claude Code token from standard input.

### 2. Initialise the host

```sh
wherry host init laptop.bundle
```

`host init` installs the bundle, creates your laptop's signing key, and asks for a source for each credential the
pool needs:

```
pool build delivers credential claude-oauth-token to its agents as CLAUDE_CODE_OAUTH_TOKEN.
Source for credential claude-oauth-token (file:PATH, env:VAR or command:ARGV; blank skips): file:~/claude-oauth-token
wrote /home/me/.config/wherry/config.yaml
```

It writes `~/.config/wherry/config.yaml`, which names the source and never the token:

```yaml
bundle: /home/me/.config/wherry/bundle.yaml
credentials:
  claude-oauth-token:
    file: /home/me/claude-oauth-token
```

[Credential sources](security.md#credential-sources) shows the other kinds of source.

### 3. See the receiver

```sh
wherry host receivers
```

```
receiver=nomad-1 pool=build capacity=2 in_flight=0 advertisement_age=4s status=live
  profile=container plugin=nomad agent=claude ready=true labels=isolation=container inline_limit=45056 credentials=claude-oauth-token:env=CLAUDE_CODE_OAUTH_TOKEN
```

### 4. Connect your coding agent

In Claude Code:

```sh
claude plugin marketplace add benemon/wherry
claude plugin install wherry@wherry
```

Run `/reload-plugins`, then ask for the `receivers` tool.
It lists `nomad-1`.
[Harness integration](operations-hosts.md#harness-integration) covers other harnesses.

### 5. Submit a task and walk away

```sh
wherry submit --prompt "Summarise the release notes in three sentences."
```

```
run_id=run-<id> pool=build
```

The run needs nothing more from your laptop.
Close the lid if you like.

### 6. Collect the result

```sh
wherry wait run-<id>
```

`wait` prints the agent's message once the run ends, and exits 0 when it completed.
`wherry logs -f run-<id>` follows the transcript while the run is going.

## Check that it worked

- `wherry host receivers` shows `nomad-1` as `live`.
- `wherry wait` reports `TASK_STATE_COMPLETED`.

If `submit` refuses with `auth required`, your laptop has no source for the credential: run `wherry host init` again.

## Next steps

- [Key concepts](concepts.md) explains pools, tenants, placement and recovery.
- [Receiver operations](operations-receivers.md) covers upgrades and the plugin pin.
- [Host operations](operations-hosts.md) covers git repositories, files and harnesses.
