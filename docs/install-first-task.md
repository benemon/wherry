# Run a first task

Submit a task from your laptop and collect its result.
You work on your laptop throughout this page.

## 1. Submit a task and walk away

Submit a task and keep its run ID:

```sh
run=$(wherry submit --prompt "Reply with exactly: wherry is working" | sed -n 's/^run_id=\([^ ]*\).*/\1/p')
echo "$run"
```

The run needs nothing more from your laptop.
Close the lid if you like.

## 2. Collect the result

```sh
wherry wait "$run"
```

`wait` prints the agent's reply once the run ends.
It exits 0 when the run completed.
To follow the transcript while the run is going, use `wherry logs -f "$run"` in another terminal.

## Check that it worked

- `wherry host receivers` shows your receiver as `live`: `nomad-1`, `kubernetes-1` or `systemd-1`, as your receiver
  page names it.
- `wherry wait` prints `wherry is working` and exits 0.

If `submit` refuses with `auth required`, your laptop has no source for the credential: run `wherry host init` again.

## Next steps

- Connect your coding agent, so it can delegate tasks itself. In Claude Code, run
  `claude plugin marketplace add benemon/wherry` and `claude plugin install wherry@wherry`.
  [Harness integration](operations-hosts.md#harness-integration) covers other harnesses.
- [Key concepts](concepts.md) explains pools, tenants, placement and recovery.
- [Receiver operations](operations-receivers.md) covers upgrades and the plugin pin.
- [Host operations](operations-hosts.md) covers git repositories, files and harnesses.
