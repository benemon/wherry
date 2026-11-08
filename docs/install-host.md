# Set up a host

Set up your laptop as host `laptop` of tenant `acme`.
Then check that it sees your receiver.
You work on your laptop throughout this page.

## Before you start

On your laptop:

- The `wherry` CLI, as [Install the wherry CLI](install-cli.md) shows.
- The bundle and CA from the hand-off on [Set up the operator](install-operator.md#_4-add-your-laptop).
- A Claude Code OAuth token, which `claude setup-token` prints.

## 1. Put the CA and your token in place

The files from the hand-off are in your home directory.
Move the CA to where wherry looks for it:

```sh
cd ~
mkdir -p -m 0700 ~/.config/wherry
mv kafka-ca.pem ~/.config/wherry/ca.pem
```

Save your Claude Code token in a new file only you can read, without a trailing newline.
The first command removes any old copy, so the new file gets the private mode:

```sh
rm -f ~/claude-oauth-token
(umask 077 && printf "Claude Code token: " && read -rs token && printf %s "$token" > ~/claude-oauth-token)
```

## 2. Initialise the host

```sh
wherry host init laptop.bundle
rm laptop.bundle
```

`host init` copies the bundle into `~/.config/wherry/`, so you can delete the hand-off copy.
It creates your laptop's signing key. Summary shows its key ID; when stdout is
not the terminal the questions use, init also prints the key ID alone there
for scripts.
On a terminal, it asks by topic: Credentials, Git, Storage, then Summary.
Only topics with missing settings get questions. Summary always appears.
Each answer stays in the session as a collapsed line:

```
Credentials  1 of 2
? claude-oauth-token  File ~/claude-oauth-token
Summary  2 of 2
Host key <key ID> registered
Wrote /home/me/.config/wherry/config.yaml
```

Use the arrows and Enter to choose, or choose Skip to leave a setting for
later. In Git and Storage, Skip on a source returns to the host or Key sources
list, where Skip again leaves the setting. Blank text returns to the parent list; blank SSH prefix accepts its
default. Tab accepts a suggestion; Up/Down and ctrl+n/p cycle suggestions.
Type after the question appears: bytes typed before a form starts may be
lost. Ctrl-C ends init with `interrupted`; answers not yet written are lost.
If the terminal misbehaves after an abnormal exit, run `stty sane`.

It writes `~/.config/wherry/config.yaml`, which names the source and never the token:

```yaml
bundle: /home/me/.config/wherry/bundle.yaml
credentials:
  claude-oauth-token:
    file: /home/me/claude-oauth-token
```

[Credential sources](security.md#credential-sources) shows the other kinds of source.

## 3. See the receiver

```sh
wherry host receivers
```

```
receiver=nomad-1 pool=build capacity=1 in_flight=0 advertisement_age=4s status=live
  profile=container plugin=nomad agent=claude ready=true labels=isolation=container inline_limit=45056 credentials=claude-oauth-token:env=CLAUDE_CODE_OAUTH_TOKEN
```

This output is the Nomad receiver's.
The receiver name, plugin and profile follow your receiver page.

## Check that it worked

`wherry host receivers` shows your receiver as `live`.

Next, [run a first task](install-first-task.md).
