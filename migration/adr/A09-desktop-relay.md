# ADR A09 — Codex Desktop completion relay boundary

Status: proposed; T71 remains open.

## Context

The frozen Python release used a signed Node wrapper to obtain the Desktop
tools-pipe capability, exposed a private Unix-socket relay, and sent only the
fixed `send_message_to_thread` tool. Historical source citations are
`archive/python-legacy:src/agent_run/delivery/codex_desktop_host.cjs`,
`archive/python-legacy:src/agent_run/delivery/codex_desktop_relay.py`, and
`archive/python-legacy:src/agent_run/delivery/dispatch.py`.

The Rust core preserves the bounded v1/v2/v3 relay protocol, same-user socket
checks, inventory bounds, fixed-tool restriction, timeout classification, and
immutable delivery-attempt evidence in `crates/agent-run-core/src/delivery/`.
The small external bridge is tested by `scripts/check-desktop-transport.cjs`.

## Decision boundary

Static bundle inspection and mock-host tests do not establish that the real
Desktop host admits the current bridge. A release must not claim native Desktop
delivery until an ignored/live smoke runs against the actual signed Desktop host
and records:

- host/tool discovery and selected protocol version;
- one accepted delivery and one bounded rejection/failure;
- no duplicate delivery after an ambiguous result;
- redacted, bounded evidence persisted with the verdict.

Until that evidence exists, T71 is partial. Failure of Desktop delivery does not
invalidate CLI, MCP, socket API, or broker execution; it limits the delivery
claim only.

## Rejected shortcuts

- arbitrary Desktop RPC or user-controlled tool names;
- falling back to the historical `codex queue` sender after an ambiguous relay
  attempt;
- treating possession of environment paths as proof of host authorization;
- claiming the mock Node bridge test as a real Desktop smoke.

Current tracking is in [qualification-scope.md](../evidence/qualification-scope.md).
