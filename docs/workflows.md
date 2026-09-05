# Workflow scripts

Multi-step, multi-engine plans run as durable script workflows: a
restricted Python script executed by a detached runner, journaled step by
step in the state store, resumable after failure. The runner survives the
session that started it.

```bash
agent-run workflow start <name> "$(cat plan.wf)"
agent-run workflow wait   wf_...        # block until terminal
agent-run workflow status wf_...        # per-step truth
agent-run workflow answer wf_...        # latest journaled step result
agent-run workflow resume wf_...        # re-run only the broken tail
agent-run workflow cancel wf_...
```

The CLI positional is the **script source text**, not a path — always pass
`"$(cat file)"`. The MCP tools (`workflow_start`, …) take the same source.

## Script contract

Exactly five names exist; nothing else. No imports, no filesystem, no
network, no subprocess — the script is AST-guarded and escape attempts
fail the run:

- `agent(spec) -> dict` — run one child agent, block until terminal. The
  only way to start work; it goes through the same `AgentService.start`
  as single runs, so limits, profiles, and permissions apply unchanged.
- `parallel([thunk, ...]) -> [result, ...]` — run zero-arg lambdas
  concurrently (real threads); a barrier until all finish.
- `pipeline(items, *stages) -> [result, ...]` — push each item through
  the stage functions independently; no barrier between stages.
- `phase(name)`, `log(text)` — progress markers, land in the runner log.
- The value of the script's **last expression** becomes the run's result.

`agent()` spec fields: `runtime`, `model`, `profile`, `task`, `workdir`,
optional `account`, `effort`, `write`, `read_roots`, `timeout_seconds`,
`output_schema` (the engine validates the answer against the schema;
mismatch = failed step with the raw answer preserved). `effort` is the
runtime's reasoning-effort label, validated exactly as `start` validates it;
omit it to keep the runtime's own default.

A step result is `{"agent_id", "status", "answer", ...}` — **always check
`status`**; a failed step returns a dict, not an exception.

## Example: fan out, then verify

```python
phase("fan")
results = parallel([
    lambda: agent({"runtime": "claude", "model": "sonnet",
                   "profile": "review", "task": TASK, "workdir": WORK}),
    lambda: agent({"runtime": "codex", "model": "gpt-5.6-luna",
                   "profile": "review", "task": TASK, "workdir": WORK,
                   "read_roots": [WORK]}),
])
phase("verify")
oks = [r for r in results if r and r.get("status") == "succeeded"]
checked = pipeline(oks[:1],
    lambda r: agent({"runtime": "claude", "model": "sonnet",
                     "profile": "review",
                     "task": "Verify this claim: " + str(r.get("answer"))[:200],
                     "workdir": WORK}),
)
{"fan": len(oks),
 "verified": bool(checked and checked[0].get("status") == "succeeded")}
```

Write literal step specs: each step's journal key is a deterministic hash
of spec + position, and stable specs are what make the journal replayable.

## Reading the result — verification duty

"Succeeded" alone is not acceptance:

1. `workflow status` shows every step; a run can succeed with failed steps
   if the script tolerated them.
2. A running step records its accepted `agent_id`; completed step truth is
   the journaled result with the agent id and answer.
3. Step failures carry a `failure_kind` and parameters (exception text,
   refused permission, timeout) — read them before re-running anything.
4. `runner.log` under `<home>/workflows/<run_id>/` holds phases, `log()`
   lines, and script-level errors.

## Resume

`workflow resume <run_id>` replays a **failed or lost** run in place,
under the same run id: completed steps return instantly from the journal
cache (no new agents), only the broken tail re-executes.
Succeeded, cancelled, and still-running runs refuse to resume.

Each terminal transition records a separate notification generation with an
immutable status and result snapshot. A later generation supersedes older
pending or retrying notices. A notice already being sent retains its original
status; it never reads a newer run's status. `workflow answer` continues to
return the current run result, so callers should check the returned status
after a resume rather than assume a delayed notice describes the latest state.
The answer command returns the latest available step result from the finished
run; `workflow status` also exposes the run-level script result.

Cancellation checks the recorded runner identity against the live process and
refuses dead, unreadable or mismatched identities. An accepted cancellation
asks the runner to journal `cancelled` and cancel its in-flight child; the
request acknowledgement is not proof that shutdown has finished. The process
probe narrows but cannot eliminate the race between inspection and signalling.

## Batch: the degenerate workflow

For one flat parallel group, skip the script:

```bash
agent-run batch --file jobs.json --name nightly-reviews
```

where `jobs.json` is a list of `agent()` specs. It generates and starts
the one-phase parallel workflow for you — same journal, same verbs.
