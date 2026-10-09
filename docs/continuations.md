# Continuing a completed agent

`resume` starts a **new durable run in the same native conversation**. It does
not reset the previous run or substitute a summary for the runtime's history.
The public `agent_id` stays stable; execution identifiers remain internal.
Each run retains its own prompt, timestamps, events, transcript, answer proof
and completion notification. Storage retains `parent_agent_id`, `root_agent_id`
and `sequence` for that lineage.

```sh
agent-run resume <agent-id> --task "Address these review findings" --request-id review-2
agent-run resume <agent-id> --task-file review.txt
```

`--task` and `--task-file` are mutually exclusive. A task file is UTF-8 and its
whitespace is preserved; `--task-file -` reads stdin. The CLI submits through
the resident broker, so closing the CLI cannot orphan a preparation worker.
The usual `--session-transport`, `--session-id` and `--session-turn-id` options
bind the **new** run's notification to its caller.

MCP and the socket API expose `resume(agent_id, task,
request_id?, orchestrator?)`, returning the same stable `agent_id`. The broker
continues the latest terminal execution. Old execution identifiers remain
accepted as aliases; internal rows are never renumbered. `answer` reads the
latest result, `transcript` spans retained conversation history, and
`list_agents` shows each logical agent once.

The original provider, harness, model, reasoning effort, generated home, working
directory, write/network/read-root grants, output schema and fast setting are
inherited. The task and caller binding may change. Executions have no wall-clock limit,
expiry margin or inherited allowance. Repeated native continuations are not
limited by a time budget or an independent resume-count cap. Omitting
`orchestrator` keeps the previous binding. Missing identity or incompatible current
permissions fail explicitly, including a changed profile.

Account selection intent is inherited too. An explicit account remains pinned.
An automatically selected account is retained while eligible; Codex may select
another eligible account when the previous one is unavailable. Claude Code
must retain the same account because its native history is account-specific.

The selected predecessor must be terminal and have no child. The database
permits one child per predecessor, so concurrent requests cannot branch the
conversation. A matching `request_id` returns the accepted run even if its
directory or current configuration has since changed; a different parent, task or caller
with that key is a conflict. A failed preparation remains a separate record;
it can be resumed only if that run has its own recorded `runtime_session_id`
and passes the normal history and cleanup checks. An inherited
`resume_of_runtime_session_id` alone is insufficient: the public resume path
refuses that run instead of recovering an earlier ancestor's context.

Terminal status alone does not prove that the previous process stopped. A live
or unprovable process group blocks continuation; this includes `lost` runs.
Native runtimes also reject missing histories or busy/mismatched sessions.
CLI and MCP retry a lost broker acknowledgement once using the same admission
key; this cannot create a second native turn. There is no automatic replay of an
ambiguous native turn or fallback to a new conversation. An explicit caller binding replaces the inherited one;
omitting `orchestrator` retains the previous request's binding.

## Runtime history and compatibility

- Codex uses `thread/resume`, then a new `turn/start`; events from older turns
  cannot satisfy the new run's completion proof.
- Claude Code persists new CLI sessions and uses `--resume <id>`. Historical
  Claude and GLM runs created with the former `--no-session-persistence`
  setting may have no recoverable native history. A schema-1 run cannot be
  resumed under schema 2; its history stays readable.

Native compaction still applies: continuity preserves the history the runtime
retains, not an unlimited verbatim memory. Old runs without a sufficient
identity/grant snapshot are refused rather than assigned today's permissions.
Removing a runtime home may destroy native history even though the durable
agent-run records remain readable.

Usage from cumulative native counters is attributed only when a reliable
baseline exists; otherwise usage is explicitly unknown. Raw evidence remains
available and previous run statistics are not overwritten.

`succeeded` continues to describe runtime/answer completion. An answer explaining
a task blocker is a valid completed run; the orchestrator decides whether the
task's implementation is accepted.

The original continuation storage change used schema migration 13. Stop the
broker before upgrading, retain the automatic pre-migration backup, and restart
compatible clients afterward.
Older clients refuse a newer database; never point an older binary at it as a
rollback. Restore a verified backup with the matching release instead.

## Unlimited execution migration (0.24.0 / schema 28)

Start and resume no longer accept `--timeout` or `timeout_seconds`. A removed
parameter is a typed validation failure; omitting it creates an execution with
no lifetime clock. Operational observer, transport, collector, service and
cleanup bounds remain in force. Historical `timed_out` outcomes stay readable,
but current active transitions cannot create that status.

For an existing schema-2 home, prepare a reviewed target configuration that
removes `core.default_timeout_seconds`, `core.timeout_multiplier`,
`core.warning_fraction` and `core.stalled_after_seconds`. Keep
`core.max_active_agents`, provider/account references, all role grants and every
unrelated setting. Stop admission and wait for all executions and ownership to
finish before stopping services; never cancel unrelated work to upgrade.

Use the candidate's existing paired migration procedure:

```sh
agent-run --home HOME config migrate --target-config TARGET --dry-run
agent-run --home HOME config migrate --target-config TARGET --apply --from-release OLD_RELEASE
```

`OLD_RELEASE` must be the verified sealed release matching the source database.
The procedure preserves the source config/database pair in its immutable
snapshot and refuses live writers. Schema 28 retires live clock columns while
preserving their numeric policy in historical events; original request,
identity, transcript and answer bytes remain unchanged. Frozen provider digests
are checked before the in-memory compatibility projection. One-child,
native-history, account, authority and process-cleanup checks still gate resume.

Rollback uses `config rollback --snapshot SNAPSHOT` under the same quiescence
and divergence protections, then restores the matching old executable. An older
binary must never open the upgraded database. Tests cover both source schemas
26 and 27, exact application values and original configuration bytes on rollback.
