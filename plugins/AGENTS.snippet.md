## Delegating tasks with wherry

`wherry` runs a self-contained task on other compute and returns the result.
Use it for long tasks, tasks that need a platform this machine lacks, or
tasks that can run while you do other work. The remote agent sees only the
prompt and, if given, a checkout of a pushed git ref.

1. `wherry receivers --json` lists receivers. Build the selector from an
   offer's labels; the task labels must satisfy the receiver's `selector`.
2. `wherry submit --selector k=v|v,.. --labels k=v,.. --prompt TEXT`
   prints `run_id` and `pool`. Add `--git URL --git-ref REF` for a
   repository. Keep working.
3. `wherry await RUN_ID` blocks until the run ends (30 minutes at most by
   default), then prints the state and the result. Run it in the
   background where your harness reports background command completion.
4. On `TASK_STATE_AUTH_REQUIRED`, tell the user which credential the
   message names and to run
   `wherry secret put NAME --to RECEIVER_ID --from-file PATH`. Never handle
   the value yourself. Then run `wherry continue RUN_ID` and await again.
5. `wherry cancel RUN_ID` when the result is no longer needed.

Where the `wherry` MCP server (`wherry mcp`) is configured, its tools do the
same: `receivers`, `submit`, `status`, `await`, `continue`, `cancel` and
`events`. Its `await` returns within 45 seconds; call it again while the
reply carries `retry_after`.
