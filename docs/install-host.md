# Set up a host

Set up your laptop as host `laptop` of tenant `acme`.
Then check that it sees your receiver.
You work on your laptop throughout this page.

## Before you start

On your laptop:

- The `wherry` CLI, as [Install the wherry CLI](install-cli.md) shows.
- The bundle from the hand-off on [Set up the operator](install-operator.md#_4-add-your-laptop).
  It carries the broker's CA, so you need no CA file.
- A Claude Code OAuth token, which `claude setup-token` prints.

## 1. Set the token where submits run

The pool's agent `claude-code` takes its credential `claude-code-oauth-token` from `CLAUDE_CODE_OAUTH_TOKEN`.
That variable is a [catalog variable](credential-variables.md).
The host reads it from the process that submits, so the configuration needs no entry for it.

Set the variable in the shell you submit from.
Paste the token at the prompt and press Enter:

```sh
printf "Claude Code token: " && read -rs CLAUDE_CODE_OAUTH_TOKEN && export CLAUDE_CODE_OAUTH_TOKEN
```

The variable lasts for this shell session.
When your coding agent submits through the MCP server, the server is the process that submits.
Forward the variable by name in the server's entry in the harness's MCP configuration, as
[Passing variables to an MCP server](configuration-host.md#passing-variables-to-an-mcp-server) shows for each harness.

## 2. Initialise the host

```sh
cd ~
wherry host init laptop.bundle
rm laptop.bundle
```

`host init` copies the bundle into `~/.config/wherry/`, so you can delete the hand-off copy.
It verifies the broker's certificate against your machine's trust store; a CA the store lacks goes in
`--ca-cert @ca.pem` ([Broker and S3 trust](security.md#broker-and-s3-trust)).
It creates your laptop's signing key and registers it.
On a terminal, it asks by topic: Credentials, Git, Storage, then Summary.
Only topics with missing settings get questions.
It asks nothing for a credential whose target is a catalog variable, so this pool gets no question.
Summary always appears.
After Summary, it prints one ready line per agent of each pool your tenant is entitled to:

```
Summary  1 of 1
Host key <key ID> registered
Wrote /home/me/.config/wherry/config.yaml
pool build agent claude-code: ready; claude-code-oauth-token from CLAUDE_CODE_OAUTH_TOKEN
```

The ready line names each credential's source.
When the variable is unset in the shell that runs `host init`, the line reads:

```
pool build agent claude-code: not ready; set CLAUDE_CODE_OAUTH_TOKEN where submits run (https://code.claude.com/docs/en/authentication); or add to /home/me/.config/wherry/config.yaml: credentials: {claude-code-oauth-token: {command: [your-secret-tool, get, NAME]}}
```

Set the variable as step 1 shows and run `wherry host init` again.
A rerun keeps every setting and prints the ready lines again.

When a topic has questions, use the arrows and Enter to choose, or choose Skip to leave a setting for later.
Ctrl-C ends init with `interrupted`.
When standard input or standard error is not a terminal, init asks nothing and prints the key ID alone to stdout.

It writes `~/.config/wherry/config.yaml`, which names the bundle:

```yaml
bundle: /home/me/.config/wherry/bundle.yaml
```

### Read the token from a command or a file

To read the token from a keychain or a secret manager, add a `command` source for the credential to
`~/.config/wherry/config.yaml`.
The command runs without a shell at each submit and prints the token to standard output:

```yaml
bundle: /home/me/.config/wherry/bundle.yaml
credentials:
  claude-code-oauth-token:
    command: [your-secret-tool, get, claude-code-oauth-token]
```

To read it from a file, save the token in a file only you can read, without a trailing newline.
The first command removes any old copy, so the new file gets the private mode:

```sh
rm -f ~/claude-oauth-token
(umask 077 && printf "Claude Code token: " && read -rs token && printf %s "$token" > ~/claude-oauth-token)
```

Then name the file as the source:

```yaml
bundle: /home/me/.config/wherry/bundle.yaml
credentials:
  claude-code-oauth-token:
    file: /home/me/claude-oauth-token
```

An entry in `credentials` wins over the variable.
Run `wherry host init` again to see the ready line for the source: `from command, unchecked` for a command, which init
never runs, or `from file` for a file.
[Credential sources](security.md#credential-sources) sets out the rules for each kind of source.

## 3. See the receiver

```sh
wherry host receivers
```

```
receiver=nomad-1 pool=build capacity=1 in_flight=0 advertisement_age=4s status=live
  profile=container plugin=nomad agent=claude-code ready=true labels=isolation=container replicas=1 in_flight=0 inline_limit=45056 credentials=claude-code-oauth-token:env=CLAUDE_CODE_OAUTH_TOKEN
    credential=claude-code-oauth-token description="Claude Code"
```

This output is the Nomad receiver's.
The receiver name, plugin and profile follow your receiver page.
A credential with no mark after its target is ready.
`missing` after it means this shell has no `CLAUDE_CODE_OAUTH_TOKEN` and the configuration names no source.

## Check that it worked

- `wherry host init` prints `pool build agent claude-code: ready`.
- `wherry host receivers` shows your receiver as `live` and its profile as `ready=true`.

Next, [run a first task](install-first-task.md).
