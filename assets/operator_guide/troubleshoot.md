# troubleshoot

## Central log

Every entrypoint (`mcp`, other CLI verbs, and the detached supervisor) writes
dense, append-only logs to `<home>/logs/<component>.log`
(`mcp.log`, `cli.log`, `supervisor.log`). Logging
defaults to `DEBUG` so a postmortem has everything; set `AGENT_RUN_LOG_LEVEL`
(e.g. `INFO`) in the environment to quiet it down once a system is stable.
A log directory that cannot be created never blocks a command — the process
falls back to stderr instead.
There is no built-in log rotation.

## Start with doctor

`agent-run doctor` is the first move for almost any reported problem. It
separates errors (must fix) from warnings (should look at). An older database
returns `migration_required` at public command preflight, before the internal
`state_migration_pending` diagnostic — see `migrations`. The command exits nonzero
whenever any finding is error-severity.

Two checks matter most for a supervisor that cannot even start:

- **Canary handshake** (`component: canary`): exercises the real fork ->
  exec -> identity-proof -> READY path with no provider/runtime, via a
  selected agent-run home. `supervisor_canary_ok` means the path works; a
  `supervisor_executable_missing` or `supervisor_start_failed` finding
  carries the same bootstrap evidence (stage, error type, pid) a failed
  `start` would, and means every `start` in this home is currently doomed
  the same way.
- **MCP process inventory** (`component: mcp:*`): lists every running
  `agent-run mcp` process it can see, with pid, start time, and the release
  path from its `ps` argv. `mcp_process_older_release` fires
  when a process started before the `standalone/current` symlink's last
  switch — it may still be running old code; reconnect MCP in that session
  before pruning releases.

## failure_kind vocabulary

Agent/run failures carry a `failure_kind` plus free-text `failure_text`.
Known kinds include `auth_failed`, `permission_rejected`,
`supervision_failed`, and the `runner-*` family for runner-level failures.
Match on `failure_kind` first, then read `failure_text` for the specific
detail — don't parse `failure_text` to decide behavior.

## limits: honest-unknown, not always-fresh

Quota collection runs independently of model runs. `agent-run limits` reads
the latest stored sample per quota identity without calling providers. It
reports `known`, `remaining_percent`, `reset_at`, `observed_at`, and
`valid_until`, plus account/pool identity for account-bound samples. Freshness
depends on those timestamps, not a fixed interval after the last model run;
unusable samples have no displayed remaining percentage. It does not expose
burn-rate, forecast, or pacing fields. Unknown means no usable known standing,
not proof that a sample's validity timestamp expired.

## Delivery binding

The `PostToolUse` hook on `mcp__agent[-_]run__start` binds a session using
`--transport` per client: codex defaults to `codex_queue`, claude to
`claude_uds`. A missing or wrong `--transport` on that hook is the usual
cause of "the agent ran but never delivered a result back."

## Delivery attempt evidence

When relay-backed Codex delivery retries or fails, inspect
`agent-run delivery status <agent-id>` and its `last_attempt`.
`classifier`, `returncode`, `spawn_errno`, `error_class`, and `duration_ms`
separate relay unavailability, rejection, ambiguous post-write acceptance,
and success. `codex_queue` is only a compatibility binding name: completion
delivery never invokes the Codex UI queue. `null` means no attempt evidence
has been recorded. Raw messages, session ids, argument/environment values,
and credentials are intentionally unavailable.

Desktop relay discovery requires the MCP process to have started with absolute
`CODEX_MCP_NODE_PATH` and `CODEX_APP_TOOLS_PIPE_PATH` values. In that mode the
MCP PID belongs to the supplied signed Node frontend, while its distinct Rust
child has neither variable. A missing `ar-cdx-v4-*.sock` beside the agent-run
home indicates that the frontend could not bind its private relay; MCP continues
without relay delivery and reports the failure on stderr.

## Orphan check

Run `agent-run doctor` and cross-reference its supervisor findings with
`agent-run agents --active` for the same home. Reconcile only records whose
stored PID plus birth identity is observed as dead or reused; unknown or denied
identity is never proof of an orphan.
