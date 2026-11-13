# Host operations

## Enrol a host

1. Ask your operator to create your tenant and entitle it to a pool.
2. Ask for a host bundle from `wherry operator tenant host add TENANT HOST`.
3. Install the broker CA if your Kafka cluster uses a private CA.
4. Run `wherry host init HOST.bundle`.
5. Configure the credential sources it requests.
6. Run `wherry host receivers` to check your access.

[Set up a host](install-host.md) gives a complete example.

## Rotate a key

A rotation replaces a host's or receiver's signing key and keeps its name.
A pool's key custodian rotates the pool key itself, daily
([Rotation](concepts.md#rotation)); a suspected pool key calls for
[Respond to a compromised receiver or pool key](operations-operator.md#respond-to-a-compromised-receiver-or-pool-key).
Readers keep verifying the feed and transcript records the old key signed
before the rotation. For a key that may be compromised, revoke the host or
receiver instead.

For a host:

1. Wait for the runs the host submitted to end. `wherry status` lists them.
2. Stop every other wherry process of the host, such as the MCP server and
   any `wherry wait`. They hold the old key.
3. Run the rotation:

   ```sh
   wherry host key rotate
   ```

   It prints `old_key_id=<id> new_key_id=<id>`. It refuses while a run the
   host submitted has not ended, and names the run.

For receiver keys, follow [Receiver key rotation](operations-receivers.md#rotate-a-receiver-key).

A rotation writes the new key beside the old one, as `<host_key>.next` or
`signing.key.next`, before it publishes anything. When a rotation stops
part way, the next host command, or the receiver's next start, settles the
`.next` file from the key topic: it keeps the new key when the topic names
it current and removes the `.next` file when the topic still names the old
key. Leave the `.next` file in place; removing it by hand after the
rotation was published loses the key.

## Replace a lost key

A host or receiver key cannot be recovered or re-registered.

- A host whose `host_key` is lost fails `wherry host init` with
  `host key topic holds another key`.
- A receiver whose state directory is lost fails at start with
  `receiver key topic holds another key than the state directory`.

Revoke it, then add a host or receiver under a new name.

## Inputs and outputs

A run's outputs hold its `output_limit` in 256 entries at most. The broker's
record limit and each plugin profile's declared limit bound a task's inline input
files, as [`wherry submit`](commands.md#wherry-host-submit) describes; a plugin
declares each profile's limit through `InlineLimit`, and the receiver keeps
the plugin's dispatch check as a backstop.

The agent creates `WHERRY_OUTPUT_DIR` when it writes its first output.
Outputs are collected once the job ends and is stopped, whatever its exit
and whatever outcome the agent reports. A run that ends before its job does,
by timeout or otherwise, returns none.

## S3 storage

A pool can carry inputs and outputs of up to 5 GiB each through an
S3-compatible bucket, the [S3 lane](a2a-extension-v1.md#s3-inputs-and-outputs).
wherry administers no bucket: the operator creates it, sets its lifecycle
rule, issues each tenant's credential and names the endpoint in the pool's
terms. Any store that speaks S3 with SigV4 presigned URLs and path-style
addressing serves, such as Ceph RGW, NooBaa or MinIO.

1. Create one long-lived bucket with the store's default server-side
   encryption, store-managed or KMS. Every run's objects live under
   `wherry/<tenant>/<run>/` in it.

2. Name it in the pool's [terms](configuration-operator.md#pool-terms). The endpoint is
   `https://` and the host, with `:port` when it is not 443, and no path.
   wherry refuses a plain `http` endpoint except on the literal loopback
   hosts the [pool terms](configuration-operator.md#pool-terms) name.

   ```yaml
   storage:
     endpoint: https://s3.example.com
     bucket: wherry
   ```

3. Give each tenant's hosts a credential that reaches only that tenant's
   prefix: read, write and delete objects under `wherry/<tenant>/`, and list
   the bucket under that prefix, which `wait` uses at the end of a
   run. A bucket policy for tenant `alice`, whose principal syntax varies by
   store (Ceph RGW names a user `arn:aws:iam:::user/<uid>`):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {"Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam:::user/wherry-alice"]},
        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        "Resource": ["arn:aws:s3:::wherry/wherry/alice/*"]},
       {"Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam:::user/wherry-alice"]},
        "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::wherry"],
        "Condition": {"StringLike": {"s3:prefix": ["wherry/alice/*"]}}}
     ]
   }
   ```

   ```sh
   aws s3api put-bucket-policy --endpoint-url https://s3.example.com --bucket wherry --policy file://alice.json
   ```

   The host keeps the access key and secret key as two credentials, each with
   its own source, and names them under the pool's entry in `storage` in its
   [configuration](configuration-host.md#client-configuration). `wherry host init`
   asks once for an access key source and a secret key source, and maps every
   entitled pool with storage and no mapping yet to that pair. New names are
   `s3-access-key` and `s3-secret-key`, numbered when taken. Edit the
   configuration to give a pool its own keys:

   ```yaml
   credentials:
     s3-access-key:
       file: ~/.config/wherry/credentials/s3-access-key
     s3-secret-key:
       command: [op, read, "op://wherry/s3/secret-key"]
   storage:
     build:
       access_key: s3-access-key
       secret_key: s3-secret-key
   ```

   Receivers and agents never hold the S3 credential; they get presigned URLs, a GET per input
   until the run's `task_started_by` plus 15 minutes and a PUT per output until
   its `task_ended_by`, 7 days at most.

4. Set a lifecycle rule that expires `wherry/` after a period longer than
   any run: the host deletes a run's objects when it sees the run end with
   every output verified, and the rule removes what no host saw finish.

   ```json
   {"Rules": [{"ID": "wherry-backstop", "Status": "Enabled", "Filter": {"Prefix": "wherry/"}, "Expiration": {"Days": 7}}]}
   ```

   ```sh
   aws s3api put-bucket-lifecycle-configuration --endpoint-url https://s3.example.com --bucket wherry --lifecycle-configuration file://lifecycle.json
   ```

5. Let the parties reach the endpoint and trust its certificate: hosts,
   which upload, download and delete, trusting the system roots and the
   certificates in `SSL_CERT_FILE` when it is set
   ([Environment variables](configuration-host.md#environment-variables));
   receiver hosts, which upload outputs with
   their system roots; and every platform node that fetches inputs, for the
   Nomad plugin each Nomad client, whose artifact fetcher uses the system
   roots. Agent jobs do not reach the endpoint themselves; see
   [Agent network reach](#agent-network-reach).

The [Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#s3-inputs)
states how Nomad fetches an input, where its presigned GET is visible, and
the limits to set on the Nomad clients. The receiver's upload follows no
redirect.

## Git repositories

A task can name a repository for its agent to clone:

```sh
wherry submit --prompt "Fix the failing test" \
  --git https://github.com/acme/app.git@main --git-changes patch
```

The host's `git` configuration gives the clone its credential and, for
`ssh`, its host key:

```yaml
git:
  - url: https://github.com/acme/
    credential: github-token
  - url: ssh://git@gitlab.example/
    credential: deploy-key
    host_key: ~/.config/wherry/known_hosts/gitlab
```

`submit` matches the remote, with a `user@host:path` remote written as
`ssh://user@host/path`, against the entries' URLs and takes the entry whose
URL is its longest prefix. The entry's credential joins the task as a task
credential in the file `git-credential` of the credentials directory, and
its `host_key` file becomes the task's host key. A remote under no entry
clones anonymously, and an `ssh` remote under none is refused for want of
a host key. The URL prefix is the credential's scope: every task cloning
under it receives that credential, so end each URL with `/` and keep it as
narrow as the token. Loading the configuration checks that each entry's
credential has a source and its `host_key` is a readable file. A task
credential of the entry's credential name is refused.

The agent runs `git` itself, from its image or host, with the commands the
prompt gives it; the receiver and the plugin run no git. The
[extension](a2a-extension-v1.md#git-input) defines the remote, ref and path
rules and the commands. For the agent to follow them:

- The image or host has `git` 2.30 or later, a shell, and `ssh` for `ssh`
  remotes. The prompt asks the agent to stop and report an older `git`.
- The agent may run `git` commands. Every command the prompt gives starts with
  `git`, so for Claude Code
  [`Bash(git:*)`](https://docs.anthropic.com/en/docs/claude-code/iam) in its
  allowed tools covers them, along with whatever else the task runs. Since
  `git` can run any command, the rule lets the agent run any command.

Allow the hosts a pool's tasks may clone from with `vcs` in its
[terms](configuration-operator.md#pool-terms):

```yaml
vcs:
  - github.com
  - git.example.com:2222
```

`submit` refuses a remote outside the VCS hosts before it
publishes, and receivers reject one at admission.

VCS hosts limit what wherry accepts. The agent runs commands, and it can
reach any host its network allows. Enforce where agents connect with the
platform's egress policy ([Agent network reach](#agent-network-reach)),
and treat `vcs` as the check that a task names an allowed repository.

The clone's commands keep the credential for the remote's host. No
template seeds the clone. They reset
the credential helpers, so no helper of the image or host is asked for the
token or stores it; the clone's helper answers only for the remote's
scheme, host and port. They refuse HTTP redirects, so the remote must be
the repository's current URL, verify certificates, and allow only the
remote's protocol. `ssh`
reads no configuration file, uses no agent and no identity but the
credential, and trusts only the task's host key.

A plugin gives the agent an environment in which `git` reads no system or
global configuration and no configuration from the environment, uses no
template, never prompts, and traces nothing, since those could rewrite the
remote to another host or record the token. The agent has no global `git`
identity; the clone sets `user.name` to `wherry` and `user.email` to
`wherry@invalid`.

Some variables no value switches off: `GIT_SSH`, `GIT_SSH_COMMAND` and
`GIT_SSH_VARIANT` replace the clone's `ssh` command, `GIT_DIR`,
`GIT_WORK_TREE`, `GIT_COMMON_DIR`, `GIT_INDEX_FILE`, `GIT_OBJECT_DIRECTORY`
and `GIT_ALTERNATE_OBJECT_DIRECTORIES` retarget the repository a command
works on,
`GIT_EXEC_PATH` chooses the programs `git` runs, `GIT_SSL_NO_VERIFY`
disables certificate checks, and `GIT_CURL_VERBOSE` traces requests, with
the Authorization header redacted. A plugin removes them only from the
environment it configures, and cannot remove what the platform or the image
sets. Each plugin's agent environment section says how to keep them out:
[Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-environment),
[Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#agent-environment)
and [systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd#agent-environment).

The CA and proxy settings stay yours: `GIT_SSL_CAINFO`, `SSL_CERT_FILE`,
`HTTPS_PROXY` and the like reach `git` as the image or platform sets them.

Start the agent's harness in a directory above the checkout. A harness
loads the project settings of the directory it starts in: Claude Code in
print mode runs that project's hooks and MCP servers without a trust prompt
([Claude Code settings](https://docs.anthropic.com/en/docs/claude-code/settings)).
A harness started in a cloned repository would run its hooks with the
agent's credential in reach. The start directory is the plugin profile's to set: on Nomad, a runtime
command such as `cd /local && exec claude ...` starts the harness in the task
directory, and the clone lands below it.

The isolation protects a cooperative agent from the configuration around it.
An agent that runs commands, including
through an allow rule such as
[`Bash(git:*)`](https://docs.anthropic.com/en/docs/claude-code/iam)
(`git -c alias.x='!cmd'` runs any command), can undo every one of these
settings. A hostile or prompt-injected agent holds whatever credentials the
parent sent, and what it can do with them is bounded by their scope and
lifetime, the platform's egress policy, and the receiver's redaction of what it
returns.

The host's configuration supplies least privilege; wherry passes the
credential as a file and checks the host:

- For `https`, a short-lived token for one repository, such as a GitHub App
  installation token. The credential helper answers with the user name
  `x-access-token`, which installation tokens require.
- For `ssh`, a deploy key for the repository, created for the run and removed
  afterwards where the forge allows it, and the forge's host key from a source
  you trust, named by the git entry's `host_key`. Every entry must name the remote's
  host, or `[host]:port` for a port other than 22; hashed entries, and entries
  with a wildcard pattern even beside the host, are refused. `ssh` checks the
  host key strictly and ignores every other `known_hosts`.
- A public repository needs no credential.

With `--git-changes`, the agent returns its commits as `changes.patch` or
`changes.bundle`, which `wait` writes like any output, and the parent
applies them with its own credentials:

```
apply with: git am /home/me/.local/state/wherry/runs/run-.../outputs/changes.patch
```

On a pool with storage, `--output changes.bundle:s3` beside
`--git-changes bundle` returns the bundle through the pool's bucket. To
build on a run's commits in a new run, `wherry host submit --continue RUN_ID`
reuses its repository and continues from the `changes.bundle` that `wait`
collected; each round returns its bundle through the pool's bucket, so the
pool needs storage and the host a storage credential for it
([`submit`](commands.md#wherry-host-submit)).

The agent pushes only when the prompt asks it to and the credential allows
it, and then only to the branch `wherry/<run>`; verify the commit it
reports at that branch before trusting it, and delete the branch after.
Forge rules that confine the credential's pushes to `wherry/*` make that
the only branch it can write.

`ssh` refuses a private key that others can read when the agent's user
owns the file, so the owner and mode a plugin gives credential files decide
whether a deploy key works; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-jobs)
says what it gives.

## Agent network reach

An agent runs commands, so it can connect to any host its network allows. The
pool's `vcs` and `storage` bound what a task names; the platform's egress
policy bounds where its agent connects. Give agent jobs a policy that:

- denies the control plane: the Nomad servers and clients, Consul, Vault,
  the Kafka brokers, the Kubernetes API, and cloud metadata addresses such
  as `169.254.169.254` and `fd00:ec2::254`;
- allows DNS, the model API the agent's harness calls (for Claude Code,
  `api.anthropic.com`), and the hosts in the pool's `vcs`;
- denies everything else.

wherry gives the agent no storage URL: none reaches its prompt, its
environment or its files. On Nomad the client fetches S3 inputs before the
task starts, and the receiver uploads S3 outputs after the task stops. The
Nomad clients and the receiver hosts need the storage endpoint; agent jobs
do not. The presigned GET is the source of an `artifact` in the agent job,
so an agent that can read its own job can read the URL: keep the Nomad API
out of the agent's reach, as the Nomad plugin's README below says.

Each plugin's README says how its agent jobs reach the network and how to
confine them:
[Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#agent-network-reach),
through the task driver's network mode, a node pool for agents and host
firewall rules;
[Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#agent-network-reach),
through a default-deny egress NetworkPolicy; and
[systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd#agent-network-reach),
through `IPAddressDeny=` and `IPAddressAllow=` on `wherry.slice` or nftables
rules.

wherry enforces none of this policy. Restricting agent egress is the
operator's choice, made on the platform. What each party of a deployment
connects to:

| Party | Connects to |
| --- | --- |
| Hosts: `wherry` and `wherry host mcp` | the Kafka brokers' TLS listener; the pool's storage endpoint when the host's configuration has `storage` |
| Receiver hosts: the receiver and its plugins | the Kafka brokers' TLS listener; the platform's API (the Nomad or Kubernetes API, or the local systemd bus); the storage endpoint, to upload S3 outputs and, for the Kubernetes and systemd plugins, fetch S3 inputs |
| Nomad clients | the storage endpoint, to fetch S3 inputs |
| Agent jobs | DNS; the model API the agent's harness calls; the hosts in the pool's `vcs`, for git inputs; and whatever else the task's own work needs |
| Prometheus | each receiver's `metrics_addr` |

Kafka is the hosts' and receivers' alone; agent jobs need no route to the
brokers or to the storage endpoint.

## Harness integration

`wherry host mcp` serves the host operations as MCP tools over stdio. It reads
the client configuration when it starts, and connects to Kafka as one host at
its first tool call.
Every tool call reuses that host and its one tenant-feed follower, and
`wait`, `events` and `logs` also read the tenant's transcript topic. While
the connection fails, the server retries it on each tool call.
[MCP tools](mcp.md) lists the tools.

Prerequisites: `wherry` on the harness's `PATH`, and the client
configuration, bundle, host key and credential files readable by the
harness user.

1. Install the plugin. In Claude Code, add this repository as a
   marketplace and install the plugin from it, as
   [Claude Code's plugin documentation](https://docs.anthropic.com/en/docs/claude-code/plugins)
   describes:

   ```sh
   claude plugin marketplace add benemon/wherry
   claude plugin install wherry@wherry
   ```

   Another harness installs `plugins/wherry` through its own plugin
   mechanism.
2. Check the connection with the `receivers` tool, which lists the
   receivers the tenant may use.

The packaged `plugins/wherry/mcp.json` starts `wherry host mcp` with the default
configuration path, or the path `WHERRY_CONFIG` names in the harness's
environment. For a configuration elsewhere, set `WHERRY_CONFIG` there, keep
it at the default path or link the default path to it, or register a server of your own that runs
`wherry host mcp --config-file PATH` and disable the packaged one, for example with
[`claude mcp add wherry -- wherry host mcp --config-file PATH`](https://docs.anthropic.com/en/docs/claude-code/mcp)
in Claude Code. A plugin update replaces the packaged `mcp.json`, so edits to it
do not last.

The server sends an input or `prompt_file` only from beneath its input
roots: the client configuration's `input_roots`, or its working directory
when that is unset. Every input root is a directory whose files the harness
can send to remote agents; list only directories meant for that.

`plugins/wherry` packages the server with the `wherry` Agent Skill:

- `plugin.json`, `mcp.json` and `skills/` form an Agent Plugins 1.0 bundle.
- `.claude-plugin/plugin.json` is the Claude Code manifest for the same
  skill and server, and the repository's `.claude-plugin/marketplace.json`
  lists it as the `wherry` marketplace.
- Other harnesses take `mcp.json` and `skills/wherry` directly.

`plugins/AGENTS.snippet.md` covers harnesses without skills, using the CLI.
