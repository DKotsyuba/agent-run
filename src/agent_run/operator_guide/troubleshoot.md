# troubleshoot

## Central log

Every entrypoint (`mcp`, other CLI verbs, the detached supervisor, the
workflow runner) writes dense, rotating logs to `<home>/logs/<component>.log`
(`mcp.log`, `cli.log`, `supervisor.log`, `workflow-runner.log`). Logging
defaults to `DEBUG` so a postmortem has everything; set `AGENT_RUN_LOG_LEVEL`
(e.g. `INFO`) in the environment to quiet it down once a system is stable.
A log directory that cannot be created never blocks a command — the process
falls back to stderr instead.

## Start with doctor

`agent-run doctor` is the first move for almost any reported problem. It
separates errors (must fix) from warnings (should look at), including
`state_migration_pending` — see `migrations`. The command exits nonzero
whenever any finding is error-severity.

Two checks matter most for a supervisor that cannot even start:

- **Canary handshake** (`component: canary`): exercises the real fork ->
  exec -> identity-proof -> READY path with no provider/runtime, via a
  throwaway home. `supervisor_canary_ok` means the path works; a
  `supervisor_executable_missing` or `supervisor_start_failed` finding
  carries the same bootstrap evidence (stage, error type, pid) a failed
  `start` would, and means every `start` in this home is currently doomed
  the same way.
- **MCP process inventory** (`component: mcp:*`): lists every running
  `agent-run mcp` process it can see, with pid, start time, and the release
  path from its own `ps` argv (macOS has no way to read a running process's
  resolved executable back from the OS). `mcp_process_older_release` fires
  when a process started before the `standalone/current` symlink's last
  switch — it may still be running old code; reconnect MCP in that session
  before pruning releases.

## failure_kind vocabulary

Agent/run failures carry a `failure_kind` plus free-text `failure_text`.
Known kinds include `auth_failed`, `permission_rejected`,
`supervision_failed`, and the `runner-*` family for runner-level failures.
Match on `failure_kind` first, then read `failure_text` for the specific
detail — don't parse `failure_text` to decide behavior.

`permission_rejected` on an opencode child usually means the child tried to
read a path outside its workdir without that path declared in
`read_roots` — check the `start` request's `read_roots` before assuming a
permissions bug elsewhere.

## limits: honest-unknown, not always-fresh

Capacity/limits data is only as fresh as its source last reported:

- claude and codex sources appear for about 15 minutes after their most
  recent run, then age out to unknown rather than showing stale numbers.
- opencode's limits need the OmniRoute quota sync to be alive; if that
  sync has stopped, opencode limits go unknown, not wrong.

An "unknown" limit is the system being honest about missing data, not a
bug to chase.

## Delivery binding

The `PostToolUse` hook on `mcp__agent[-_]run__start` binds a session using
`--transport` per client: codex defaults to `codex_queue`, claude to
`claude_uds`. A missing or wrong `--transport` on that hook is the usual
cause of "the agent ran but never delivered a result back."

## Delivery attempt evidence

When Codex queue delivery retries or fails, inspect `status.delivery.last_attempt`.
`classifier`, `returncode`, `spawn_errno`, `error_class`, and `duration_ms`
separate an executable failure, exit 127, timeout, lost session, malformed
result, and success. Output tails are redacted and capped at 4096 UTF-8 bytes;
byte counts and truncation flags show when the original output was larger.
`null` means no Codex queue attempt has been recorded. Raw messages, session
ids, argument/environment values, and credentials are intentionally unavailable.

## Orphan check

To find agents whose supervisor process died without cleanup: `ps` for
`supervisor_main` processes, and cross-reference with `agents --active`
per home. A home with active agents in the store but no matching
supervisor process is an orphan and safe to reconcile.
