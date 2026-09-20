# Isolated sealed-release smoke — 20 September 2026

Candidate: `e8d5b7d6011680f4ff79913d0c25c8d6f92a81c4`.

The candidate was built with the committed lockfile and offline registry,
sealed with the repository `xtask` release path, and verified before execution.
All state, sockets, logs, homes, build outputs, and the fixture engine lived in
one disposable `/private/tmp` directory. The installed agent-run service and
its home were not read or modified, and no credentials were copied.

## Artifact

| Field | Observed value |
|---|---|
| Sealed version | `e8d5b7d` |
| Schema | 16 |
| Binary bytes | 10,911,616 |
| Binary SHA-256 | `7f959c3c85370d899acbf3b8e3e55bd81a1fdd684f39895e8bf2367dacb77151` |
| Manifest verification | passed |

The provider-free fixture engine was built separately and was not included in
the sealed release. Its SHA-256 was
`ee09b09d0ad75ccb87195242e1455a953a3bd8551ca7b782774cb8a679852acd`.

## Runtime smoke

- The sealed binary initialized a private home and served the Unix-socket API.
- API `ping` returned `ok: true`; tool discovery returned the complete tool
  schemas.
- MCP initialization negotiated protocol `2025-06-18`; `tools/list` succeeded.
- A real broker-backed `start --wait` against the fixture runtime reached
  durable `succeeded` state.
- The sealed answer contained `fixture final answer\n`, 21 bytes, SHA-256
  `f9bdecd8db4c20da954c0781de184b2b53639c87af6b5f39b92549662885ed2e`.
- Cleanup evidence recorded `confirmed: true`, `descendants_gone: true`, and
  `group_gone: true`, with no signals required.
- Doctor reported only `supervisor_canary_ok` and `mcp_inventory_self` at info
  severity; there were no warnings or errors.
- The daemon was terminated and reaped, and its socket was absent afterward.

This smoke proves the sealed artifact's local packaging, CLI, API, MCP,
supervisor, fixture-backed execution, answer proof, doctor, and cleanup paths.
It does not replace the pending Desktop-host, supported-engine continuation,
non-macOS, or production-cutover acceptance gates.

Successor evidence: later the same day, real Codex, Claude, and GLM start and
resume canaries closed the supported-engine continuation gate; see the
[live canary checkpoint](../live-canary-checkpoint-2026-09-20.md). Desktop-host,
hosted Linux, and production-cutover evidence remain separate.
