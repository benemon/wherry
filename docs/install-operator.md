# Set up the operator

Set up the operator's working directory, then create pool `build`, tenant `acme` and host `laptop`.
You work on the operator machine throughout this page.

## Before you start

On the operator machine:

- The `wherry` CLI, as [Install the wherry CLI](install-cli.md) shows.
- A Kafka cluster that meets the [Kafka prerequisites](operations-operator.md#prepare-kafka), with a super user's
  SCRAM credential. This guide calls it `kafka-admin`. On Strimzi, set the
  [ignored-users pattern](operations-operator.md#strimzi) first.

## 1. Create the working directory

Create the working directory and its folders, and copy in your Kafka cluster's CA certificate:

```sh
mkdir -p wherry-prod/pools wherry-prod/receivers
cd wherry-prod
cp /path/to/your/kafka-ca.pem kafka-ca.pem
```

Put the super user's password in a private temporary file.
`read -rs` keeps it off the screen, and `printf %s` writes it without a trailing newline:

```sh
password_file=$(mktemp) && trap 'rm -f "$password_file"' EXIT
printf "Kafka super user's password: " && read -rs password && printf %s "$password" > "$password_file" && unset password
```

The `trap` deletes the file when this shell exits, even if a step fails.
Initialise the directory, then delete the password file:

```sh
wherry operator init --broker kafka.example.com:9093 --ca-file kafka-ca.pem \
  --super-user kafka-admin --super-user-password-file "$password_file"
rm "$password_file"
```

`operator init` creates the principal `wherry.operator` and grants it what wherry needs.
It doesn't keep the super user's password.
Every later `wherry operator` command in this directory connects as `wherry.operator`.

The broker address goes into every bundle, so use one your laptop and the receiver can reach.

## 2. Create the pool

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
Your laptop supplies its value on [Set up a host](install-host.md).

Apply the terms:

```sh
wherry operator pool apply build
```

[Pool terms](configuration-operator.md#pool-terms) explains every field.

## 3. Add the tenant and give it the pool

```sh
wherry operator tenant add acme
wherry operator tenant pool add acme build
```

## 4. Add your laptop

```sh
wherry operator tenant host add acme laptop
```

This writes your laptop's bundle to `bundles/hosts/acme/laptop.bundle`.

> [!IMPORTANT]
> Hand-off: copy `bundles/hosts/acme/laptop.bundle` and `kafka-ca.pem` to your laptop's home directory, over a channel
> you trust. The bundle holds the laptop's Kafka password.

## Check that it worked

```sh
wherry operator tenant list
```

The output lists the tenant, its pool and its host:

```
acme
  pool build
  host laptop
```

Next, run the receiver on one platform: [Nomad](install-receiver-nomad.md),
[Kubernetes and OpenShift](install-receiver-kubernetes.md) or [systemd](install-receiver-systemd.md).
