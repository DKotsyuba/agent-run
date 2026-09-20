# Work log

> **Historical log.** Entries are point-in-time observations. Later successful
> Rust builds and qualification do not make the early blocked observations
> incorrect; current status is in [status.md](status.md).

2026-09-15: Started a real Rust source tree in /mnt/data/agent-run-rust.
Target upstream: c9904f9843ba4a0772bdfa8bac5259f18fad9dc3 (0.11.15).
The prior attempt left no code. The upstream files visible through the GitHub
connector are the input; the terminal cannot clone (DNS/network unavailable).
No cargo/rustc executable is installed. Build/test execution is currently BLOCKED,
not passed. This document must never substitute for an actual test result.

Created Cargo manifest, library, typed errors, validated IDs/requests and lifecycle.

Created strict config, descriptor-anchored filesystem, role and policy modules.

Added PID birth verification, bounded engine framing and subprocess/RPC transport.

Added generated homes, declared skills/plugins, native hook trust digests, authentication bridges and runtime snapshot verification.

## Persisted continuation checkpoints

- Saved the native Rust domain/configuration/filesystem/state/adapter/supervisor/service/transport code rather than only reading the repository.
- Added quota normalization/ranking, completion outbox and signed-Node transport source.
- Added CLI, guide resources, project map, migration status/plan and dependency decisions.
- Authored 78 Rust test functions and an offline fake engine; Rust execution remains blocked by the missing toolchain.
- Executed ten local Node-bridge tests successfully; prepared 58 SQL statements against the included schema; validated configuration/resource syntax and lexical delimiter balance.
- Corrected default delivery retry semantics after reading upstream source, removed provider error prose from notices, checked unexpected Codex network grants, and made start replay independent of later configuration edits.
- Saved actual validation logs and a ZIP source snapshot. Remaining work is listed explicitly in MIGRATION_STATUS.md; no migration-complete claim is made.

## Final archive checkpoint

Refreshed the local Node test log (10 passed) and SQLite preparation checks (58 prepared), verified the current static SQL literals against the stored inventory, and added a machine-readable baseline and Russian evaluation guide. Rust compilation/tests remain unexecuted. A source-only ZIP is packaged with a per-file SHA-256 manifest; archive CRC and extracted manifest checks are verified separately during packaging.
