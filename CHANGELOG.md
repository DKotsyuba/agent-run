# Changelog

All notable changes are documented here. Versions follow Semantic Versioning.

## [Unreleased]

## [0.13.0] - 2026-09-23

- feat!: schema-2 provider configuration; public `start` takes an explicit
  provider and model, resolves accounts from the registry, and ranks them from
  account-scoped quota observations. A schema-1 home must run the one-time
  paired `config migrate` (state schema 16 to 17) before any other command
- feat: explicit provider resume continues the recorded native session as a
  new logical child; an authoritative quota exhaustion switches a Codex run to
  another account within the same logical run (Claude Code and custom
  gateways refuse cross-account continuation)
- feat: one enforced overall run deadline, handoff checks serialized with
  cancellation, and proof-gated recovery of orphaned provider attempts
- fix: quota pools are matched to models through recorded per-window
  membership rather than pool names
- 0.13.0 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.5] - 2026-09-22

- fix(mcp): replace Desktop-launched MCP with the supplied signed Node frontend,
  which owns the native-tools pipe and long-lived typed completion relay while
  its Rust MCP child runs without host capabilities
- fix(mcp): preserve direct MCP `initialize` and `tools/list` service with a
  fixed bounded warning when the optional frontend path is malformed, missing,
  non-executable, or fails during `exec`; fallback never opens a native-host
  client
- 0.12.5 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.4] - 2026-09-22

- fix(core): continue Codex relay discovery past stale endpoints so a live
  listener beyond the first sixteen candidates can receive completion notices

## [0.12.3] - 2026-09-21

- fix(core): compare Codex readable and writable root echoes as exact multisets
  so app-server ordering differences no longer reject an otherwise identical
  managed Projects grant
- test(core): keep extra, missing, duplicate, malformed, and reordered readable
  and writable root cases fail-closed under focused regression coverage
- 0.12.3 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.2] - 2026-09-21

- feat(codex): accept multiple operator-authorized write roots through
  `workspace_roots = ["...", "..."]`, while preserving the legacy singular
  `workspace_root` declaration and rejecting configurations that declare both
- fix(codex): admit write workdirs below any configured root, require every root
  in a managed Projects policy, and generate the complete root set for unmanaged
  Projects without widening access to the rest of the home directory
- docs(config): expose the plural snapshot and operator contract, including that
  a write role admitted under one root receives write access to every configured
  Projects root
- 0.12.2 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.1] - 2026-09-21

- fix(cli,mcp): `cancel` returns the current agent view with top-level status
  after enqueuing the durable cancellation, restoring the archived response
  shape shared by CLI, socket, and MCP transports
- fix(core): lost-process reconciliation drains late pending steer/cancel
  commands through the shared completion path, and finalizes pending commands
  inside the transaction that commits the lost status so a crash cannot strand
  them behind a terminal row
- fix(cli): the CLI parser carries the package version so `--version` prints it
- fix(config): reload adoption and rejection are recorded in bounded process
  logs naming the active SHA-256 revision; adoption is logged only after the
  snapshot is installed, and no config contents, paths, environment values, or
  credentials are persisted
- fix(mcp): the `start` tool description names both direct host-visible engine
  aliases and is pinned to the archived Python baseline plus exactly that
  guidance
- fix(platform): parse `/proc/<pid>/stat` as bytes so a non-UTF-8 command name
  can no longer discard leader identity and fail Linux cleanup observation
- fix(core): classify a non-socket Claude UDS endpoint as unavailable instead
  of session-gone; Linux reports `ECONNREFUSED` for both
- test(core,store): derive Codex app-server fixture paths from the host temp
  directory and run the `chflags uchg` reopen fixture only on macOS so Linux
  validation passes
- chore: remove the completed Rust migration archive and its migration-only
  `xtask evidence verify` and `xtask qualify` commands; release safety stays
  with the workspace checks, source archive verification, the sealed native
  release, and the Desktop transport fixture
- 0.12.1 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.0] - 2026-09-20

- feat!: make the self-contained Rust broker the sole primary implementation
- build!: replace interpreter packages with a sealed macOS Apple-silicon
  binary; Linux release and qualification are deferred
- ci!: gate releases with Rust-only macOS checks and sealed verification while
  retaining Linux as visible non-blocking validation
- fix(codex): honor safe plugin-declared relative hook manifest paths
- feat!: remove the deprecated Qwen runtime; legacy Qwen configuration now
  fails with explicit removal guidance

BREAKING CHANGE: Python packages, interpreter-based installation, and the
Python implementation are no longer shipped from the primary branch. Install
the native release artifact and use the built-in `codex`, `claude`, or `glm`
runtime aliases.

## [0.11.15] - 2026-09-15

- docs(codex): clarify snapshot helper contracts
- refactor(codex): isolate runtime snapshot helpers
- fix(codex): prepare project trust before sealing runtime

## [0.11.14] - 2026-09-15

- test: isolate native settings runtime homes
- fix(config): reserve native capability and auth controls
- docs(config): clarify native preference validation
- test(config): materialize snapshot-restored settings
- fix(config): tighten native_settings review findings
- feat(config): declarative native runtime settings
- fix(codex): reuse managed permission profiles
- fix(codex): keep runtime test caches sandboxed

## [0.11.13] - 2026-09-15

- fix(codex): preserve reviewed network rules on prepare

## [0.11.12] - 2026-09-15

- feat(codex): opt into reviewed workspace network

## [0.11.11] - 2026-09-15

- fix(codex): verify named-profile runtime roots

## [0.11.10] - 2026-09-15

- fix(codex): verify Projects profile on launch

## [0.11.9] - 2026-09-14

- feat(codex): add native permission guards

## [0.11.8] - 2026-09-14

- fix(launchd): raise API broker file limit (#46)

## [0.11.7] - 2026-09-10

- docs(claude): clarify unlabelled home and config env contract

## [0.11.6] - 2026-09-10

- docs(claude): document native credential home
- fix(claude): preserve host HOME for native credential state
- fix(claude): resolve native config home from login account

## [0.11.5] - 2026-09-09

- fix(capacity): preserve host path in launchd

## [0.11.4] - 2026-09-09

- refactor(codex): split error classification
- fix(codex): classify provider overloads

## [0.11.3] - 2026-09-09

- fix(adapters): inherit installed Rust homes

## [0.11.2] - 2026-09-09

- fix(codex): admit public GPT-6 profiles

## [0.11.1] - 2026-09-09

- fix(capacity): restore forecast-based routing

## [0.11.0] - 2026-09-09

- test(roles): canonicalize fixture command path
- fix(api): isolate list long polls
- test(supervisor): prepare detached launch fixtures
- test(capacity): remove obsolete history coverage
- test(service): follow supervisor preparation boundary
- test(service): remove obsolete bootstrap wait
- fix(claude): reap leader after group cancellation
- test(runtime): execute preparation at supervisor boundary
- test(capacity): read current identity snapshots
- feat(cli)!: fold waiting into start
- feat: restore push delivery and operator tools
- refactor!: shrink public agent tool surface
- refactor!: remove completion delivery pipeline
- refactor(capacity)!: keep only current snapshots
- test(runtime): align startup fixtures with supervisor preparation
- fix(runtime): require process proof for startup loss
- test(runtime): prepare captured supervisor launches
- refactor(tui)!: remove standalone terminal dashboard
- feat(supervisor): remove automatic runtime timers
- test(supervisor): prove explicit cancel cleanup
- feat(status): expose factual agent revisions
- refactor(supervisor): own preparation before ready
- fix(runtime): tighten resolved role decoding
- fix(runtime): preserve resolved role compatibility
- fix(runtime): compile isolated resolved role contracts
- test(claude): cover inherited host environment
- refactor(config): move runtime readiness out of launch
- refactor(adapters): compile resolved roles through one boundary
- feat(auth): default to native runtime accounts
- feat(roles): resolve one canonical runtime contract
- refactor(adapters): inherit host development environment

## [0.10.2] - 2026-09-08

- fix(snapshots): preserve legacy resume compatibility
- fix(claude): protect scoped credential config
- fix(claude): preserve scoped credential state
- feat(claude): add scoped CLI login
- fix(claude): scope CLI credential refresh state

## [0.10.1] - 2026-09-08

- Fix bounded MCP release smoke (#27)

## [0.10.0] - 2026-09-08

- Stabilize core execution and recovery (#25)

## [0.9.0] - 2026-09-07

- feat(codex): review developer write escalations
- fix(codex): disable login shells for developer environments
- docs(runtime): link supported developer environment adapters
- refactor(codex): move launch environment preparation
- refactor(claude): keep adapter focused
- fix(claude): render effective MCP environment
- fix(qwen): preserve declared developer toolchains
- feat(claude): integrate developer-environment presets and command policy
- feat(codex): apply developer environments to launches
- feat(qwen): connect declared developer environment providers
- fix(codex): align native thread grants with the app server schema
- test(runtime): preserve lexical command path evidence
- feat(runtime): add configurable command denial policies
- feat(config): add declared developer environment presets
- fix(codex): provision declared Rust for shell and MCP

## [0.8.0] - 2026-09-06

- fix(codex): negotiate experimental workspace roots capability
- docs: declare owner-authorized native and agent-run parity
- refactor(adapters): separate static policy and TOML helpers
- fix(rust): provision coding shells and MCP environments explicitly
- docs: verify worktree source selection before tests
- fix(start): distinguish handoff and supervisor diagnostics
- test(start): verify prepare stage before release
- fix(start): log preparation stage entry
- fix(start): log preparation stage on failure
- test(codex): cover multiple runtime workspace roots
- fix(codex): send runtime workspace roots
- docs: clarify scoped fixture checks and Codex root grants
- fix(resume): exclude unsupported opencode continuation
- feat(resume): expose native continuation across runtime adapters
- fix(resume): preserve profile grants and accepted request replay
- chore: checkpoint native session continuation
- feat(adapters): carry native session identity in launch plans
- fix(codex): retain extended context settings on generation

## [0.7.3] - 2026-09-05

- fix(codex): preserve streamed whitespace and early-exit evidence

## [0.7.2] - 2026-09-05

- fix(runtime): harden supervised execution and workflow recovery

## [0.7.1] - 2026-09-05

- fix: keep Python hooks working in isolated runtime homes (#15)

## [0.7.0] - 2026-09-05

- build!: require Python 3.14

## [0.6.4] - 2026-09-05

- feat(delivery): add failure guidance to completion notices

## [0.6.3] - 2026-09-04

- fix(glm): preserve bounded stderr diagnostics
- docs(release): add one-command operational runbook
- feat(release): automate verified publication and local deployment

## [0.6.2] - 2026-09-04

- Prevented accepted starts from being marked lost while a live coordinator is
  preparing them or waiting to hand ownership to the supervisor.
- Bounded preparation ownership to 120 seconds, preserving orphan cleanup and
  preventing expired or cancelled workers from launching a runtime later.
- Kept supervisor process-group updates valid after a successful handoff and
  moved process identity probes outside SQLite write transactions.
- Added schema v11 startup ownership fields. Restart long-lived agent-run
  processes and reconnect MCP clients after upgrading.

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

[Unreleased]: https://github.com/DKotsyuba/agent-run/compare/v0.12.4...HEAD
[0.12.4]: https://github.com/DKotsyuba/agent-run/compare/v0.12.3...v0.12.4
[0.12.3]: https://github.com/DKotsyuba/agent-run/compare/v0.12.2...v0.12.3
[0.12.2]: https://github.com/DKotsyuba/agent-run/compare/v0.12.1...v0.12.2
[0.12.1]: https://github.com/DKotsyuba/agent-run/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.12.0
[0.11.15]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.15
[0.11.14]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.14
[0.11.13]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.13
[0.11.12]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.12
[0.11.11]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.11
[0.11.10]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.10
[0.11.9]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.9
[0.11.8]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.8
[0.11.7]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.7
[0.11.6]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.6
[0.11.5]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.5
[0.11.4]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.4
[0.11.3]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.3
[0.11.2]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.2
[0.11.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.1
[0.11.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.0
[0.10.2]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.10.2
[0.10.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.10.1
[0.10.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.10.0
[0.9.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.9.0
[0.8.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.8.0
[0.7.3]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.7.3
[0.7.2]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.7.2
[0.7.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.7.1
[0.7.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.7.0
[0.6.4]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.4
[0.6.3]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.3
[0.6.2]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.2
[0.6.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.1
[0.6.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.0
[0.5.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.5.0
[0.4.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.4.1
[0.4.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.4.0
[0.3.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.3.1
[0.3.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.3.0
[0.2.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.2.1
[0.2.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.2.0
[0.1.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.1.0
