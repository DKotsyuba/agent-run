# Python MCP stdio baseline

Captured on 2026-09-16 from this worktree's Python MCP CLI using
`/Users/pluto/projects/agent-run/.venv-py314/bin/python` (uv-managed Python
3.14.3), `PYTHONPATH=src`, and throwaway homes under `/private/tmp`.

`handshake.json` contains the four protocol revisions accepted by the Python
SDK's initialize handshake: 2024-11-05, 2025-03-26, 2025-06-18, and
2025-11-25. Each exchange records initialize, `notifications/initialized`,
`tools/list`, successful `models`, validation and unknown-tool errors, a
cancellation notification, and clean EOF. `broker-unavailable.json` records
the equivalent call with no resident broker. The real Python `api serve`
broker backed successful calls; no user home, credentials, agent task, or
answer content was read.

Dynamic capacity timestamps are retained as provenance and excluded from the
Rust parity assertions; all tested wire values are otherwise exact JSON.
