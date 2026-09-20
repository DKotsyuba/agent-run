# A14: Rust MCP SDK interoperation

Date: 2026-09-16

Status: accepted and implemented. Python references below describe the frozen
compatibility baseline on `archive/python-legacy`, not a runtime dependency.

## Decision

Keep the lockfile-resolved `rmcp` 1.8.x line with only the `server` and
`transport-io` features. Its known protocol revisions include the four Python
initialize-handshake revisions (2024-11-05, 2025-03-26, 2025-06-18, and
2025-11-25), so the Rust proxy accepts each and echoes the negotiated revision.
The 2026-07-28 revision is deliberately not listed as supported here: the
Python baseline uses its separate modern envelope rather than `initialize`,
which is outside this stdio proxy's contract.

The proxy configures baseline-compatible `serverInfo` (`agent-run` / `1`) and
capabilities (`experimental:{}`, `tools:{listChanged:false}`), serves the one
domain tool registry shared with the other transports, and maps every broker/domain failure to an MCP
tool error (`content`, `structuredContent.error`, `isError:true`). It never
opens a local store or falls back to local start; all calls use the resident
Unix-socket broker.

## Workaround and divergences

rmcp's stock stdio adapter reads an unbounded line. `mcp.rs` wraps both stdin
and stdout with 1 MiB LF-frame counters, matching the Python broker limit and
preventing unbounded input allocation or an oversized response frame.

rmcp's defaults differ from the Python SDK in implementation version,
capabilities, fixed structured-result text, unknown tool treatment, and broker
unavailable wording. The transport-local compatibility layer overrides those
defaults. No dependency was added.

## Evidence

Archived Python transcripts in `tests/fixtures/baseline/mcp/` were captured through the
actual CLI and temporary real broker. `crates/agent-run/tests/mcp_parity.rs` compares four
handshakes, tool schemas, success, validation, unknown tool, cancellation,
broker unavailable, and EOF against that corpus. This satisfies migration plan
M10 / inventory TR-3 test T67 for the handshake-era stdio contract.
