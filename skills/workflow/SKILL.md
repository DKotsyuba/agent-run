---
name: workflow
description: Orchestrate multi-step, multi-family agent runs through the agent-run workflow engine — deterministic Python scripts with phases, parallel fan-out, and pipelines over codex/claude/qwen children, journaled and survivable across orchestrator sessions. Use when a delegation has two or more dependent stages, a fan-out plus verification, or must outlive the session; on the words «воркфлоу», «запусти цепочку», «фан-аут с проверкой», "workflow", "fan out and verify". For one job use agent-run start; for one flat parallel batch use batch.
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

Example (fan across three families, verify one answer):

```python
phase("fan")
results = parallel([
    lambda: agent({"runtime": "qwen", "model": "opencode/MiniMaxM3",
                   "profile": "review", "task": "...", "workdir": WORK}),
    lambda: agent({"runtime": "claude", "model": "sonnet",
                   "profile": "review", "task": "...", "workdir": WORK}),
    lambda: agent({"runtime": "codex", "model": "gpt-5.6-luna",
                   "profile": "review", "task": "...", "workdir": WORK,
                   "read_roots": [WORK]}),
])
phase("verify")
oks = [r for r in results if r and r.get("status") == "succeeded"]
checked = pipeline(oks[:1],
    lambda r: agent({"runtime": "qwen", "model": "opencode/MiniMaxM3",
                     "profile": "review",
                     "task": "Verify: " + str(r.get("answer", ""))[:80],
                     "workdir": WORK}),
)
{"fan": len(oks), "verified": bool(checked and checked[0].get("status") == "succeeded")}
```

`agent()` spec fields: `runtime`, `model`, `profile`, `task`, `workdir`,
optional `write`, `read_roots`, `timeout_seconds`, `output_schema` (the ENGINE
validates the answer against it; mismatch = failed step with the raw answer
preserved). Step result: `{"agent_id", "status", "answer", ...}` — always
check `status`, a failed step returns a dict, not an exception.

Write literal step specs. `step_key` is a deterministic hash of spec+position:
stable specs give stable keys, which is what makes the journal replayable.

## Runtime-specific step notes

- **codex**: a read-only profile with no `read_roots` and no `write` is
  refused ("no-filesystem"). Give it `read_roots: [workdir]` at minimum.
  Known open bug: codex + external read_roots on an implement/write step is
  refused ("roots mismatch") — copy the material into the workdir instead.
- **qwen**: the cheap-OSS lane (Chinese models through the local OmniRoute
  router; combo aliases keep the historical `opencode/` prefix). Check the
  LIVE roster with the `models` tool. `opencode/MiniMaxM3` answers one-liners
  in seconds — the default smoke and verify model. Since 29.08.2026 write
  children have a sandboxed shell and run their own tests (name the exact
  commands in the step brief); read-only children have none, and git commits
  stay with acceptance.
- **glm**: the claude engine on the Z.ai GLM Coding Plan (subscription
  quota — watch the glm lane in limits). glm-5.3-flash is the normal step
  for easy-to-medium coding; glm-5.3 is the escalation for complex steps,
  review, and architecture. Write children have a sandboxed shell.
- **claude**: watch the capacity advisory before fanning out; sonnet steps
  are the cheap option.

## Launching and watching

Prefer the MCP tools: `workflow_start(script, args?)`, `workflow_status`,
`workflow_cancel`, `workflow_answer`. CLI fallback (server not connected or
stale — see cautions):

```bash
~/.agent-run/standalone/current/venv/bin/agent-run --home ~/.agent-run \
  workflow start <name> "$(cat script.wf)"
```

The CLI positional is the SCRIPT SOURCE TEXT, not a path — always pass
`"$(cat file)"`. Passing a path makes the runner compile the path string and
fail with a SyntaxError.

Do not block your turn on a run: put `agent-run workflow wait <run_id>`
in the background (it blocks until terminal — succeeded / failed /
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
- Resume exists at engine level (journal replay by step_key) but is NOT
  exposed via CLI/MCP yet — a lost run is restarted, cached steps replay.
- Cancel is safe and proven: run → cancelled, in-flight step journaled
  `runner_cancelled`, the child agent is cancelled with it, no orphans.
- A workflow burns quota on every step: intersect the plan with the capacity
  advisory (delegate skill) before a wide fan-out.
