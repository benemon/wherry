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
It creates your laptop's signing key and prints its key ID.
Then use the pages to configure the pools you can use.

Use the Credentials page to select a source for each missing credential.
Choose Environment variable, File or Command.
Press Enter twice to choose a detected environment variable.
These two selections replace the former default-No confirmation.
Press ? to read the source scope warning in optional help.
Only operators who open help see that warning.
Press Esc to skip a credential or topic.
Use Esc in a nested list to return to its parent.
Leave a text answer blank to return.
A blank SSH prefix accepts the default.

The Broker page precedes registration when you need a CA choice.
Git and Storage pages follow Credentials when their settings are missing.
The Summary page lists writes and settings still missing.
Pages print inline and keep your session in scrollback.
Run `stty sane` if external termination or another abnormal failure leaves your terminal raw.
Press Ctrl-C to restore the terminal and return.

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
