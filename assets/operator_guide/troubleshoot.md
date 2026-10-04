# troubleshoot

## Central log

Every entrypoint (`mcp`, other CLI verbs, and the detached supervisor) writes
dense, append-only UTC-daily logs to `<home>/logs/<component>.YYYY-MM-DD.log`
(`mcp.2026-09-28.log`, `cli.2026-09-28.log`, `supervisor.2026-09-28.log`). Logging
defaults to `DEBUG` so a postmortem has everything; set `AGENT_RUN_LOG_LEVEL`
(e.g. `INFO`) in the environment to quiet it down once a system is stable.
A log directory that cannot be created never blocks a command — the process
falls back to stderr instead.
The broker expires old daily files after 30 days; legacy undated logs remain
until their writers are known closed.

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

`claude_uds` attempts record the same evidence shape with `classifier`
`uds_receipt_held`, `uds_receipt_delivered`, `uds_receipt_refused`,
`uds_unconfirmed`, `uds_ambiguous`, `uds_session_gone`, `uds_rejected`, or
`uds_unavailable`. Each send proves the inbox socket's kernel peer identity
(the descriptor's process id as this user) before any frame is written,
advertises an ephemeral reply socket beside the inbox socket, and correlates
the inbox's native hold-receipt by notification id, accepting it only from
the same proven receiver identity: `uds_receipt_held` means the inbox
confirmed it queued the notice (the `delivered` variant means it confirmed
handing it to the session immediately); enqueue confirmation is still not
proof the recipient's model read the message. The inbox sends a receipt only for a message it holds for approval, later
releases or denies, refuses by policy, expires, or drops at admission; a message
the session accepts immediately produces none, so silence can never be reported
delivered and exactly-once delivery cannot be proved. `uds_unconfirmed` (a
clean write with no correlated receipt, for example on an inbox that never
answers or when no reply socket could be bound) and `uds_ambiguous` (a write
that failed, timed out or was reset after it began) mean the notice may already
be in the session's queue. They are not success and are not retried: the
notice ends `failed` with `ambiguous: true` after that single attempt
(at-most-once), because a retry would put a duplicate in the chat. Attempts
that provably wrote nothing (`uds_session_gone`, `uds_unavailable`,
`uds_rejected`) still retry with backoff. A queued notice left in `retry_wait`
by an older broker after such an outcome is ended `failed` with `ambiguous:
true` the next time it is claimed, without a new send. If a notice looks lost,
check the recipient Claude session's queue before resending it by hand.

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

`terminal_attempt_ownership_unresolved` warns that a terminal run still owns
one or more attempts because cleanup could not be proven. Its component uses
the public root agent id; detail contains only the count and a safe cleanup
reason. Ownership and its reservations remain active until normal cleanup
reconciliation proves they can be released. Doctor never releases them.
