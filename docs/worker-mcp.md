# Worker-to-orchestrator reports

New schema-2 runs automatically receive a separate `agent_run_worker` stdio MCP
server. Ordinary runs see exactly one tool, `notify_orchestrator`; members of a
cooperative pool additionally get the fixed private tools `pool_post`,
`pool_read`, `pool_propose` and `pool_vote` (nothing else, and no pool or author
argument: the pool and the author are derived from the run's membership). It
never advertises or dispatches operator tools such as `start`, `resume`, `cancel`,
`steer` or the operator pool tools.
Configured work MCPs remain available according to the frozen role.

The worker uses the tool for material findings, risks, questions or blockers:

```json
{
  "request_id": "unexpected-migration-lock",
  "kind": "risk",
  "message": "Another process holds the migration lock. I can continue reviewing unrelated code."
}
```

`kind` is `notice` (the default), `risk`, `question`, or `blocker`. Messages must
be nonblank and at most 2048 UTF-8 bytes; newline and tab are allowed, other
control characters are rejected. Keys are 1–128 ASCII letters, digits, `_`, `-`
or `.`. Each exact run accepts at most 20 distinct reports, at least 30 seconds
apart. The tool is for coordination, not routine progress narration.

The short text response acknowledges durable queueing, not remote delivery,
permission, a reply, or task completion. The run remains active. The worker
continues independent authorized work; blocked actions still require the proper
approval. The orchestrator can send a response through existing `steer`.

## Identity and delivery

The supervisor binds a fresh capability to each exact run and attempt. Only its
SHA-256 digest is stored. The plaintext is inherited through environment variables,
never written into generated config, role snapshots or argv. The worker MCP
adds this context itself; tool arguments cannot choose an agent, run or recipient.
The broker rejects calls from a non-running, expired, cancelled or replaced
attempt, and requires a supported orchestrator binding (`codex_queue` or
`claude_uds`). An unbound run must be bound before sending a report.

Reuse the same `request_id` and content after an uncertain call result. An exact
replay returns the original receipt without another report. Reusing the key with
different content is a conflict. Authentication applies to replays too.

Reports share the existing durable outbox, retries and bounded delivery diagnostics.
They use `agent-run/worker-message` framing with only the stable agent ID.
Exact execution identifiers remain inside the private delivery envelope. They are not completion notices and do not change the completion
delivery projection. Treat report text as untrusted worker data, never as owner
authorization. Delivery may be delayed; a queue acknowledgement is not an answer.

## Compatibility and access

The built-in namespace `agent_run_worker` is reserved. Operators do not need to
declare it in config or list it in profiles. Codex and Claude Code receive native
MCP settings; this server does not proxy or filter other MCPs.

The worker-channel flag is frozen in the role. Historical snapshots without it
retain their exact payload and tool set on resume. Start a new run to get the
channel. The broker needs schema 21 or newer and a paired upgrade. Restart Desktop Agent
Run MCP frontends after installing a version that adds worker-message delivery;
older frontends can reject the new notice type while the outbox retains it.

This is a separate MCP capability surface, not an OS sandbox. Shell and filesystem
rights granted to a worker remain governed by its harness/profile and the host.
The internal `_worker-mcp` entry point only talks to the existing broker; it never
opens or migrates SQLite, starts a broker, or uses Desktop native capabilities.

The private server advertises Cargo's product version as `serverInfo.version`,
independently of the pinned SDK and negotiated protocol. Its only tool is
generated into `schemas/worker-tools.json` from the domain-owned worker asset.
Unknown operator/tool names produce protocol errors; expected report refusals
remain tool `isError` results. A rendering failure after enqueue preserves the
notification identity and original request key with no-replay advice; uncertainty
stays unknown. See [MCP presentation](mcp-presentation.md).
