# Getting started

This guide enrols one receiver on a Nomad cluster and runs a first task on
it. [Concepts](concepts.md) explains the parts involved.

## Prerequisites

- A Kafka 4.x cluster with share groups enabled. Both the client and the
  receiver connect over TLS with SCRAM-SHA-512, so the cluster needs a TLS
  listener and two SCRAM users: one for the user (`alice` below) and one
  for the receiver (`receiver-a`). The wherry topics and a reply topic for
  the user must exist with the settings in
  [Kafka topics](operator-guide.md#kafka-topics), and each user needs the
  [ACLs](operator-guide.md#kafka-acls) listed there.
- A Nomad cluster, a namespace for agent jobs (`agents` below) and an ACL
  token with the [receiver policy](operator-guide.md#nomad-acl-policy).
- The `wherry` binary on both the user's machine and the receiver host.
  Build it with Go 1.27 or later from a checkout of this repository:

  ```sh
  go build -o wherry ./cmd/wherry
  ```

- An agent the Nomad nodes can run: an image for a container driver, or an
  install on the host for `raw_exec` or `exec`. The
  [agent contract](operator-guide.md#agent-contract) lists what the agent
  must do.
- The model credential the agent needs, in a file on the user's machine.

## 1. Write the client configuration

The client reads `~/.config/wherry/config.json`, or the file that
`WHERRY_CONFIG` names. Write it on the user's machine:

```json
{
  "broker": "kafka.example.com:9093",
  "ca_file": "~/.config/wherry/kafka-ca.pem",
  "kafka_user": "alice",
  "kafka_password_file": "~/.config/wherry/kafka-password"
}
```

Place the Kafka CA bundle and the SCRAM password at those paths. The reply
topic defaults to `replies.alice`.

## 2. Create the user key

```sh
wherry init
wherry key
```

`init` writes the user key to `~/.config/wherry/user.key` and prints its
fingerprint. It refuses to replace an existing key. `key` prints the public
key and fingerprint:

```
public_key=<base64url public key> fingerprint=sha256:<hex>
```

Keep the `public_key` value for the receiver configuration.

## 3. Write the receiver configuration

On the receiver host, write `/etc/wherry/receiver.json`. Set
`user_public_key` to the value from step 2:

```json
{
  "broker": "kafka.example.com:9093",
  "ca_file": "/etc/wherry/kafka-ca.pem",
  "kafka_user": "receiver-a",
  "kafka_password_file": "/etc/wherry/kafka-password",
  "receiver_id": "receiver-a",
  "state_dir": "/var/lib/wherry",
  "user_public_key": "<base64url public key>",
  "labels": {"region": "eu-west"},
  "adapters": [
    {
      "name": "nomad",
      "type": "nomad",
      "credential": "model-api-key",
      "config": {
        "address": "https://nomad.example.com:4646",
        "ca_file": "/etc/wherry/nomad-ca.pem",
        "namespace": "agents",
        "credential_env": "MODEL_API_KEY",
        "runtimes": {
          "agent": {
            "driver": "docker",
            "config": {
              "image": "registry.example.com/agent:1.0",
              "command": "/bin/sh",
              "args": ["-c", "exec agent-run < \"$NOMAD_TASK_DIR/prompt.txt\""]
            },
            "env": {},
            "resources": {"cpu": 1000, "memory_mb": 2048}
          }
        }
      }
    }
  ]
}
```

`agent-run` stands for the agent's headless command. The runtime named
`agent` becomes the offer label `runtime=agent`. The adapter delivers the
sealed credential `model-api-key` to the agent as `MODEL_API_KEY`.
[Configuration reference](reference.md#receiver-configuration) describes
every field.

## 4. Start the receiver

```sh
NOMAD_TOKEN=<token> wherry receiver --config /etc/wherry/receiver.json
```

On first start the receiver creates its keys in `state_dir` and publishes an
enrolment request. It logs the request hash:

```
receiver: <time> enrolment requested: receiver=receiver-a request=<hash>
signing=sha256:<hex>
```

The log prints this as one line. The receiver then waits for approval.

## 5. Approve the receiver

1. List the requests on the user's machine:

   ```sh
   wherry enrol list
   ```

   ```
   receiver=receiver-a request=<hash> labels=region=eu-west approved=false
     adapter=nomad platform=nomad
       offer nomad.node_pool=default,runtime=agent,zone=dc1
   ```

   Check that the request hash matches the one in the receiver's log.

2. Write `grants.json`, allowing each offer label value the receiver may
   advertise:

   ```json
   [
     {
       "name": "nomad",
       "platform": "nomad",
       "allowed": {
         "runtime": ["agent"],
         "nomad.node_pool": ["default"],
         "zone": ["dc1"]
       },
       "defaults": {}
     }
   ]
   ```

3. Approve the request:

   ```sh
   wherry enrol approve <hash> --labels region=eu-west \
     --grants grants.json --selector example.com/team=platform
   ```

   The command prints `receiver=receiver-a generation=1`. The receiver logs
   `enrolled: pool=receiver-a generation=1`. The receiver accepts only tasks
   whose labels include `example.com/team=platform`.

4. Wait up to 30 seconds for the first capability, then check that the
   receiver is live:

   ```sh
   wherry receivers
   ```

   The receiver's line shows `status=live`.

## 6. Seal the model credential

```sh
wherry secret put model-api-key --to receiver-a --from-file ./model-api-key
```

The command prints `name=model-api-key receiver=receiver-a version=1`. The
seal expires in 30 days by default.

## 7. Submit a task

```sh
wherry submit --selector runtime=agent \
  --labels example.com/team=platform \
  --prompt "Summarise the release notes in three sentences."
```

The command prints the run ID and the pool:

```
run_id=run-<id> pool=receiver-a
```

## 8. Await the result

```sh
wherry await run-<id>
```

`await` prints the final state on its first line and the result after it,
and exits non-zero for any state other than `TASK_STATE_COMPLETED`. It waits
30 minutes by default.

If the run ends in `TASK_STATE_AUTH_REQUIRED`, the receiver did not hold the
credential when it claimed the task. Seal it as in step 6, then run
`wherry continue run-<id>` and await again.

`wherry events run-<id>` prints the run's status history.
