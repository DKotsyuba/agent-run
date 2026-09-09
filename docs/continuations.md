# Continuing a completed agent

`resume` starts a **new durable run in the same native conversation**. It does
not reset the previous run or substitute a summary for the runtime's history.
Each run retains its own prompt, timestamps, events, transcript and answer proof.
`parent_agent_id`, `root_agent_id` and `sequence`
connect the records into one chronological chain.

```sh
agent-run resume <agent-id> --task "Address these review findings" --request-id review-2
agent-run resume <agent-id> --task-file review.txt --timeout 900
```

`--task` and `--task-file` are mutually exclusive. A task file is UTF-8 and its
whitespace is preserved; `--task-file -` reads stdin. The CLI submits through
the resident broker, so closing the CLI cannot orphan a preparation worker.
The usual `--session-transport`, `--session-id` and `--session-turn-id` options
record the **new** run's caller namespace.

MCP and the socket API expose `resume(agent_id, task, timeout_seconds?,
request_id?, orchestrator?)`, which returns the normal asynchronous start
envelope with a new `agent_id`.

The original runtime, model, reasoning effort, account/home, working directory,
write/network/read-root grants, output schema and fast setting are inherited.
Only the task and timeout change. Omitted timeout inherits the previous budget.
Missing or changed identity/permissions fail explicitly, including a profile
changed while the new run is being prepared.

Only the latest terminal run may be continued. The database permits one child
per predecessor, so concurrent requests cannot branch the conversation. A
matching `request_id` returns the accepted run even if its directory or current
configuration has since changed; a different parent, task, timeout or caller
with that key is a conflict. A failed preparation remains a separate record
and can lend the last known native context to its next continuation.

Terminal status alone does not prove that the previous process stopped. A live
or unprovable process group blocks continuation; this includes `lost` runs.
Native runtimes also reject missing histories or busy/mismatched sessions.
There is no automatic replay of an ambiguously submitted prompt and no fallback
to a new conversation. The new caller namespace is recorded just as for
`start`; the old caller is never reused implicitly.

## Runtime history and compatibility

- Codex uses `thread/resume`, then a new `turn/start`; events from older turns
  cannot satisfy the new run's completion proof.
- Claude and GLM persist new CLI sessions and use `--resume <id>`. Runs created
  with the former `--no-session-persistence` setting may have no recoverable
  native history.
- Qwen uses its explicit `--resume <id>` selector, never its latest-session option.

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

The change uses schema migration 13. Stop the broker before upgrading, retain
the automatic pre-migration backup, and restart compatible clients afterward.
Older clients refuse a newer database; never point an older binary at it as a
rollback. Restore a verified backup with the matching release instead.
