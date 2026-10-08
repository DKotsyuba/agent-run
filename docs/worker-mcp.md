# Worker-to-orchestrator reports

New schema-2 runs automatically receive a separate `agent_run_worker` stdio MCP
server. Ordinary workers advertise five tools: `notify_orchestrator`,
`pool_post`, `pool_read`, `pool_propose` and `pool_vote`. Pool calls authorize
live database membership on each request; an independent worker can report but
cannot use pool actions before admission. No pool or author argument is accepted:
the broker derives both from authenticated membership. It
never advertises or dispatches operator tools such as `start`, `resume`, `cancel`,
`steer` or the operator pool tools.
Configured work MCPs remain available according to the frozen role. Restricted research excludes configured work MCPs and adds only `save_report`; see [research permissions](research-permissions.md).

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

## Joining an active independent worker

`start_pool.members` accepts either `{ "start": { ... }, "role": "review" }`
or `{ "existing_agent_id": "ag-...", "role": "review" }` for each of two to
five seats. Existing members must be independent stable roots with an owned
RUNNING attempt, no prior pool membership, a live original execution deadline
and authenticated proof of the ordinary five-tool or restricted research six-tool catalog. Unknown historical
catalog or independence history is refused; resume behavior is otherwise unchanged.

Admission preserves the existing task, native session, model, account, grants,
run, attempt, deadline and reservation. Only new seats launch. A compatible
existing orchestrator binding can supply the pool binding; incompatible bindings
are rejected atomically. The original request key replays the committed pool
even if a member subsequently ends.

The existing worker receives bounded context through the durable native control
path. It remains `pending` until it calls `pool_read`, then `pool_post` with the
exact opaque broker-issued `request_id` and a brief current-work summary. The
key is pinned to its current attempt and original deadline, including replays.
Transport queueing, a Claude stdin write or Codex `native_accepted` receipt does
not prove awareness. Pending members cannot propose, vote or count toward
completion. The full goal and criteria remain authoritative in `pool_read`.

An ended, replaced or unconfirmed join becomes `needs_action`. An active worker
can read the same challenge and acknowledge after explicit context recovery;
no enrollment action cancels or restarts its ordinary work. Broker-authored
`agent-run/pool-attention` is distinct from completion and uses the existing
outbox; an existing terminal error notice suppresses duplicate attention.
Codex attention requires a v5 frontend, waits with bounded retries for compatible
support, and does not block ordinary v4 notices. Claude uses the existing inbox.

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
independently of the pinned SDK and negotiated protocol. Its ordinary five-tool catalog is
generated into `schemas/worker-tools.json` from the domain-owned worker asset.
Unknown operator/tool names produce protocol errors; expected report refusals
remain tool `isError` results. A rendering failure after enqueue preserves the
notification identity and original request key with no-replay advice; uncertainty
stays unknown. See [MCP presentation](mcp-presentation.md).

Restricted research derives a six-tool surface by adding `save_report`. A
supervisor-selected nonsecret marker selects discovery; the broker independently
authenticates the attempt and checks its frozen report-directory contract.
Ordinary and historical workers keep their existing catalog.
