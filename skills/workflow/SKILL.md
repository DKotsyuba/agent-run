---
name: workflow
description: Run authorized agent tasks with dependent stages or durable parallel orchestration. Use start for one job; use fanout first for five or more independent units.
---

# Running agent-run workflows

The engine executes a restricted Python script in a detached runner process.
Every step is journaled in the state store; the runner survives the
orchestrator session. This skill is the operator contract — the engine docs
live in the agent-run repo, `agent-run doc` prints the surface reference.

## When a workflow, when not

| Situation | Route |
|---|---|
| one bounded job | `start` — cheaper, no script |
| N independent jobs, no dependencies | `batch` (a degenerate workflow, one call) |
| stages depend on each other (fan → verify → synthesize) | **workflow** |
| work must survive your session or be resumable from the journal | **workflow** |
| five or more independent units | `fanout` skill first; it may still emit a workflow |

## Script contract

Exactly five names exist; nothing else — no imports, no filesystem, no
network, no subprocess (AST-guarded, escape attempts fail the run):

- `agent(spec) -> dict` — run one child, block until terminal. The ONLY way
  to start work; it goes through the same AgentService.start as single runs,
  so limits, profiles, read_roots, and roles all apply unchanged.
- `parallel([thunk, ...]) -> [result, ...]` — run zero-arg lambdas
  concurrently (real threads), barrier until all finish.
- `pipeline(items, *stages) -> [result, ...]` — push each item through the
  stage functions; no barrier between stages.
- `phase(name)`, `log(text)` — progress markers, land in runner.log.
- The value of the script's last expression becomes the run's `result_json`.

Use `parallel` for independent steps and `pipeline` for dependent stages.
Resolve every step's role/model/account through `$delegate` before constructing
the script, rather than copying fixed model names. Pass complete task-relevant
evidence or accessible artifact references into verification; do not truncate
an answer to a short prefix and call the result verified. Gate downstream work
on each required step's status and evidence, not just the wrapper's success.

`agent()` spec fields: `runtime`, `model`, `profile`, `task`, `workdir`,
optional `account`, `effort`, `write`, `read_roots`, `timeout_seconds`,
`output_schema` (the ENGINE
validates the answer against it; mismatch = failed step with the raw answer
preserved). `effort` carries the reasoning-effort label `start` accepts; omit
it to keep the runtime default. Step result: `{"agent_id", "status", "answer", ...}` — always
check `status`, a failed step returns a dict, not an exception.

Write literal step specs. `step_key` is a deterministic hash of spec+position:
stable specs give stable keys, which is what makes the journal replayable.

## Runtime-specific step notes

Apply the `delegate` skill's GPT-6 escalation boundary to each step: only the
hardest read-only architecture/review judgment, never code writing or routine
steps. Both the default Codex account and `personal2` are eligible; do not
reserve GPT-6 for the primary account. Select by the injected account priorities
and confirm the selected account's roster. Skip unavailable accounts without
dropping the selector or borrowing priority.

Choose each step's role/model from the existing eligibility matrix, then use
the first compatible runtime/account/lane in the latest injected Runtime
priorities. Carry the selected `account` into the step spec; never silently
drop it. A null/unlabelled account means omit the field. Models must belong to
the selected quota lane; do not use a Spark-only entry's priority for a
standard Codex model. Skip an incompatible entry and continue down the list.
If no summary is available, obtain `capacity_order` (CLI:
`agent-run capacity order`) once. Do not poll limits or manually recalculate
quota windows; `limits` is for requested diagnostics. Apply updated priorities
to work not yet started, without restarting healthy children.

- **codex**: a read-only profile with no `read_roots` and no `write` is
  refused ("no-filesystem"). Give it `read_roots: [workdir]` at minimum.
  Engine limitation (permanent, codex 0.151.0 schema has no extra-roots
  fields on write threads): codex + external read_roots on a write step
  refuses EARLY with guidance — copy the material into the workdir and omit
  read_roots. Read-only root grants keep working.
- **qwen**: the cheap-OSS lane (Chinese models through the local OmniRoute
  router; combo aliases keep the historical `opencode/` prefix). Check the
  LIVE roster with the `models` tool. `opencode/MiniMaxM3` answers one-liners
  in seconds — the default smoke and verify model. Since 29.08.2026 write
  children have a sandboxed shell and run their own tests (name the exact
  commands in the step brief); read-only children have none, and git commits
  stay with acceptance.
- **glm**: the claude engine on the Z.ai GLM Coding Plan (subscription
  quota reflected in Runtime priorities). glm-5.3-flash is the normal step
  for easy-to-medium coding; glm-5.3 is the escalation for complex steps,
  review, and architecture. Write children have a sandboxed shell.
- **claude**: follow Runtime priorities for eligible steps; sonnet steps
  are the cheap option.

## Launching and watching

Prefer the MCP tools: `workflow_start(name, script, orchestrator, args?)`, `workflow_status`,
`workflow_cancel`, `workflow_answer`, `workflow_resume`. CLI fallback (server not connected or
stale — see cautions):

```bash
agent-run --home ~/.agent-run \
  workflow start <name> "$(cat script.wf)"
```

The CLI positional is the SCRIPT SOURCE TEXT, not a path — always pass
`"$(cat file)"`. Passing a path makes the runner compile the path string and
fail with a SyntaxError.

For MCP-started workflows, follow the live tool's binding and completion
contract; do not add a CLI watcher when chat delivery is confirmed. For an
unbound or CLI-started run, put `agent-run workflow wait <run_id>` in the background (it blocks until terminal — succeeded / failed /
cancelled / lost — and prints the status report with class-mapped exit
codes), and keep doing other work. Single agents have the same watchdog:
`agent-run wait <agent_id>`. Never hand-roll status/sleep poll loops.

## Reading the result — verification duty

"succeeded" alone is not acceptance. Before reporting a run done:

1. `workflow status` — every step's status; the run can succeed with failed
   steps if the script tolerated them.
2. Step truth is `result_json` per step in the journal (workflow_steps in
   the store): real `agent_id` and `answer` per step. A succeeded step with
   no agent_id in result_json never ran a child — treat as fabricated.
3. Step failures carry `failure_kind` + `failure_params_json` (exception
   text, refused path, timeout) — read them before rerunning anything.
4. `runner.log` in `~/.agent-run/workflows/<run_id>/` holds phases, logs,
   and script-level errors.

## Cautions

- After an agent-run release switch that bumps the store schema, MCP servers
  of already-open sessions are stale: every call fails version-checked.
  Reconnect MCP, or use the CLI of the current release until then.
- Chat delivery of the terminal notice only happens for runs started with a
  bound session (MCP path). CLI-started runs deliver nothing and create no
  delivery row — `wait` on them, that IS the delivery.
- Resume is first-class since 31.08.2026: `workflow resume <run_id>` (CLI)
  or the `workflow_resume` MCP tool replays a failed/lost run in place
  under the same run_id — completed steps return from the journal cache
  (proven live: zero new agents on resume), only the broken tail re-runs.
  succeeded/cancelled/running runs refuse.
- Cancel is safe and proven: run → cancelled, in-flight step journaled
  `runner_cancelled`, the child agent is cancelled with it, no orphans.
- A workflow burns quota on every step: use the latest Runtime priorities
  through the delegate skill when choosing compatible step runtimes.
