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

## Optional prompt context

`hook context` is advisory for both `codex_queue` and `claude_uds`: an optional
context failure never blocks a submitted prompt. If the state was read but its
deduplication write fails or has an unknown outcome, the bounded prepared
context is still injected. Repeated context is possible on that recovery path;
normal successful writes retain changed-only injection. Independently readable
components survive another component's failure. Unavailable data is not invented.

SQL preparation has a 250 ms cooperative deadline; the whole context operation
has a 750 ms budget, including at most 500 ms for its writer acknowledgement.
`hook_context_unavailable` in the bounded CLI log records closed failure facts,
not prompt/configuration text. An empty successful hook response alone is not
proof that context works: verify a first injection and a deduplicated second one.

Ordinary `context`, `hook bind`, start and completion retain their strict error
and durability contracts. A context fallback never confirms notification binding.
No hook migrates an incompatible store. After an accepted installation, project
both external clients' hook commands from Crew against the same compatible
sealed release; do not retain a separately pinned old context binary.

For exact-payload qualification, set `AGENT_RUN_CONTEXT_TEST_BINARY` to the sealed
candidate's `bin/agent-run`, then run the `context_hook_process` integration test
with the project's approved temporary directories. These tests launch no model
and use disposable homes. Native delivery additionally requires independent
receiver evidence; Bridge custody alone is insufficient.

## Delivery binding

The `PostToolUse` hook on `mcp__agent[-_]run__start` binds a session using
`--transport` per client: codex defaults to `codex_queue`, claude to
`claude_uds`. A missing or wrong `--transport` on that hook is the usual
cause of "the agent ran but never delivered a result back."

## Delivery attempt evidence

When Bridge delivery retries or fails, inspect
`agent-run delivery status <agent-id>` and its `last_attempt`.
`classifier`, `returncode`, `spawn_errno`, `error_class`, and `duration_ms`
describe safe historical evidence. Current attempts use bounded `bridge_*`
classifiers and never invoke a Codex queue or Claude inbox. `null` means no evidence
has been recorded. Raw messages, session ids, argument/environment values,
and credentials are intentionally unavailable.

`bridge_unconfigured` holds the notice without exhausting its retry budget.
Inspect the explicit Bridge deployment and its dedicated protected producer grant.
`bridge_unavailable` describes a failed service exchange; `bridge_forbidden` and
`bridge_unsupported` describe grant/contract restrictions. `bridge_owner_changed`
holds the original endpoint and credential-file association rather than re-homing it.

`custody_accepted` proves publication custody only. `outcome_unknown` means an effect
may have committed; recovery reads the original event and validates producer, full
fingerprint and destination. Never change event IDs, replay missing history, switch
senders or replace Bridge storage as recovery. Bridge owns native dispatch and holds.

`legacy_delivery_held` marks previously attempted native delivery that cannot safely
be replayed. Original evidence remains immutable and readable; historical relay/uds
classifiers describe the old attempt, not a current sender. Inspect the actual
recipient conversation before an explicit audited recovery decision. MCP clients no
longer need a Desktop delivery frontend or native-tools environment variables.

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
