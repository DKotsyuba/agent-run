# Changelog

All notable changes are documented here. Versions follow Semantic Versioning.

## [0.6.1] - 2026-09-04

- Fixed the dashboard reporting the API as unavailable on stores with many
  agents: a refresh now lists orchestrators and active agents only, fetches
  finished agents just for the opened session at most every 15 seconds, skips
  transcript polling for runtimes that never stream one, and waits up to 20
  seconds for a busy server instead of 5.

## [0.6.0] - 2026-09-04

- Added `agent-run-tui`, a standard-library curses dashboard shipped as the
  separate `agent_run_tui` package: orchestrator sessions with host runtime,
  working directory, and child counters, then each session's running and
  finished agents as bordered cards with status glyphs and elapsed time. Data
  is read only through the JSON-RPC socket in a background thread; keys work on
  Latin and Russian layouts; selection follows session and agent ids across
  refreshes.
- Resolved session titles from the host runtime's own files (Claude Code
  custom titles and history, Codex thread names and rollout metadata) with
  validated ids and incremental, cached reads.
- Added the read-only `list_orchestrators` tool to the shared CLI/MCP/socket
  surface with active and total child counters per orchestrator session, and
  exposed the launch `effort` on agent views.

## [0.5.0] - 2026-09-04

- Added a bounded priority bonus for available manual Codex reset credits, read
  from the existing account-scoped app-server response and preserved in capacity
  snapshots. One reset gives a 1.5x factor; further resets approach a 2x ceiling.
- Kept exhausted and stale quota routes excluded regardless of reset credits;
  accounts return only when fresh quota evidence confirms availability. Credits
  are never redeemed automatically, and Spark receives no reset-credit bonus.
- Exposed the reset-credit count and bonus separately from configured priority
  multipliers. Older snapshots without the optional metadata remain compatible.

## [0.4.1] - 2026-09-03

- Fixed successful engines being misclassified when they exit before process-group
  discovery; completion still requires the actual outcome, a verified answer,
  and proof that no owned processes remain.
- Prevented native cancellation of unverified owned processes, including PIDs
  observed in the wrong group, and tightened leaderless-group cleanup checks.
- Standardized compact Claude and Codex completion notices with one packaged
  template. The MCP `start` description and `doc completion` now share the
  handling instructions and format instead of repeating them in every notice.
- Stabilized asynchronous-start tests without imposing execution order or
  weakening durable-failure and idempotency checks.
- Kept schema v10 and relay wire v1/v2 compatibility. Reconnect existing Codex
  agent-run MCP hosts after upgrading to load the compact notice template.

## [0.4.0] - 2026-09-03

- Added quota-aware runtime ordering through the CLI, MCP, and socket API,
  using burn rate, reset time, and shared physical quota pools.
- Added configurable runtime, account, and quota-lane priority multipliers;
  exhausted or unknown routes remain excluded from the usable order.
- Replaced automatic raw-quota context with a changed-only priority summary,
  while leaving role and model suitability decisions to the orchestrator.
- Improved scheduled quota collection with account-scoped failure isolation,
  honest degraded outcomes, current OmniRoute cache observations, and reset
  timestamp jitter handling.
- Routed Codex completion delivery exclusively through the signed Desktop
  relay, with no direct queue fallback.
- Structured agent completion notices as a concise list with ID, status,
  runtime/model/effort, and result lookup guidance; retained compatibility
  with older relay hosts.
- Added the schema v10 quota-topology migration. Restart long-lived agent-run
  processes after upgrading, and reconnect the agent-run MCP connection in
  Codex to activate the richer notification format.

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

[0.5.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.5.0
[0.4.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.4.1
[0.4.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.4.0
[0.3.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.3.1
[0.3.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.3.0
[0.2.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.2.1
[0.2.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.2.0
[0.1.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.1.0
