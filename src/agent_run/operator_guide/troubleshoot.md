# troubleshoot

## Start with doctor

`agent-run doctor` is the first move for almost any reported problem. It
separates errors (must fix) from warnings (should look at), including
`state_migration_pending` — see `migrations`.

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

## Orphan check

To find agents whose supervisor process died without cleanup: `ps` for
`supervisor_main` processes, and cross-reference with `agents --active`
per home. A home with active agents in the store but no matching
supervisor process is an orphan and safe to reconcile.
