# Getting started

This guide provisions one pool with one Nomad receiver, entitles one tenant
with one host, and runs a first task. [Concepts](concepts.md) explains the
parts involved.

## Prerequisites

- A Kafka cluster prepared as in
  [Prepare Kafka](operator-guide.md#prepare-kafka), with its super user and
  `wherry.operator` credentials on the operator's machine.
- A Nomad cluster with a namespace for agent jobs (`agents` below) and a
  token or workload identity carrying the
  [receiver policy](operator-guide.md#nomad-acl-policy).
- The `wherry` binary on the operator's machine, the host and the receiver
  host. Build it with Go 1.27 or later from a checkout of this repository:

  ```sh
  go build -o wherry ./cmd/wherry
  ```

- `wherry-plugin-nomad` on the receiver host, built from
  [wherry-plugins](https://github.com/benemon/wherry-plugins/tree/main/nomad)
  and installed in `/usr/local/libexec/wherry/plugins` as in
  [Install adapter plugins](operator-guide.md#install-adapter-plugins).
- An agent the Nomad nodes can run: an image for a container driver, or an
  install on the node for `exec`. The
  [agent contract](reference.md#agent-contract) lists what it must do.
- The model credential the agent needs, in a file on the host.

## 1. Set up `wherry admin`

On the operator's machine, export the six `WHERRY_TRUST_*` variables in
[Set up `wherry admin`](operator-guide.md#set-up-wherry-admin).

## 2. Create a pool

Write `build-terms.yaml`:

```yaml
agents:
  - name: coder
    credentials:
      - model-api-key
default_agent: coder
seal_lifetime_ms: 86400000
delivery_limit: 5
lock_duration_ms: 15000
producer_byte_rate: 1048576
request_percentage: 10
```

```sh
wherry admin pool create build --config-file build-terms.yaml
```

## 3. Entitle a tenant

```sh
wherry admin tenant add alice --pool build
```

## 4. Add the host

```sh
wherry admin host add alice laptop --bundle-file alice-laptop.bundle
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
  credential_env: MODEL_API_KEY
```

`nomad-1/adapters/10-container.yaml`:

```yaml
name: container
platform: nomad
agent: coder
credential: model-api-key
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
NOMAD_TOKEN=<token> wherry receiver \
  --bundle-file /etc/wherry/nomad-1.bundle --state-dir /var/lib/wherry \
  --plugin-dir /usr/local/libexec/wherry/plugins
```

The receiver creates its keys in `/var/lib/wherry`, registers them, and
logs:

```
receiver: <time> configured: pool=build adapters=1 capacity=2
```

[Run a receiver](operator-guide.md#run-a-receiver) covers running it
under a supervisor, or as a Nomad job.

## 7. Configure the host

On the host, put the model credential in a file and write
`~/.config/wherry/config.json`:

```json
{
  "bundle": "~/.config/wherry/alice-laptop.bundle",
  "credentials": {
    "model-api-key": "~/.config/wherry/model-api-key"
  }
}
```

When a receiver claims a task and asks for the credential, `wherry await`
seals the file's contents byte for byte to that receiver.

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
  adapter=container agent=coder ready=true labels=isolation=container
```

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

`await` prints the final state on its first line and the result after it,
and exits non-zero for any state other than `TASK_STATE_COMPLETED`. It waits
30 minutes by default. While it waits it answers `nomad-1`'s request for
`model-api-key`. Start it within 5 minutes of `nomad-1` claiming the task,
or the run ends `TASK_STATE_AUTH_REQUIRED`.

`wherry events run-<id>` prints the run's status history.

### If the run needs a credential

A run that ends in `TASK_STATE_AUTH_REQUIRED` received no seal for the
credential the message names: the host was not running `await`, or does
not configure the credential.

1. Add the credential's file to `credentials` in the host configuration.
2. Run `wherry continue run-<id>`, which publishes the next turn.
3. Run `wherry await run-<id>`, which answers the next turn's request.
