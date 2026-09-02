# Changelog

All notable changes are documented here. Versions follow Semantic Versioning.

## [0.3.1] - 2026-09-02

- Accepted the LSP plugin's `PostToolUseFailure` hook in Codex plugin trust
  materialization, restoring Codex launches after the plugin event expansion.

## [0.3.0] - 2026-09-02

- Made `start` durably asynchronous: runtime authentication, materialization,
  preparation, spawn, and READY no longer block neighboring requests, while
  cancellation and unowned-start reconciliation remain exact.
- Routed one-shot CLI starts through the resident broker so accepted workers
  outlive the CLI process and unavailable brokers fail explicitly.
- Added schema v9 immutable, bounded, redacted Codex queue attempt evidence and
  exposed the latest safe summary through `status.delivery.last_attempt`.
- Resolved the public Claude `fable` alias to Claude Fable 5.1 while preserving
  the stable configured and persisted model id.
- Stopped read-only state inspection from attempting to change database modes.

## [0.2.1] - 2026-09-01

- Prevented an exiting resident API daemon from unlinking the replacement
  daemon's Unix socket during launchd restarts.

## [0.2.0] - 2026-09-01

- Added isolated multi-account Codex authentication, account-aware workflows,
  and per-account capacity reporting.
- Made MCP a uniform thin proxy over the launchd-manageable resident Unix-socket
  daemon while preserving the shared 17-tool surface.
- Hardened detached supervision with targeted child reaping,
  `posix_spawn(..., setsid=True)`, bounded startup headroom, and exact cleanup
  evidence under launchd load.
- Restored live Codex model metadata automatically and allowed slow app-server
  initialization without truncating the agent execution budget.
- Added native Claude/Fable capacity sampling with verified TLS handling.
- Fixed Qwen macOS sandbox startup by bypassing the Xcode Git shim; Qwen can run
  without unsupported Agent-LSP/codegraph grants.

## [0.1.0] - 2026-08-31

First public release.

- Durable asynchronous runs for Codex, Claude Code, GLM, Qwen Code, and OpenCode.
- Shared CLI, MCP, and Unix-socket JSON-RPC tool surface.
- SQLite-backed state, verified outcomes, delivery, capacity tracking, and run statistics.
- Resumable multi-step workflows with parallel and pipeline execution.
- Isolated runtime homes, explicit read/write permissions, diagnostics, and operator guide.

[0.3.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.3.1
[0.3.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.3.0
[0.2.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.2.1
[0.2.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.2.0
[0.1.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.1.0
