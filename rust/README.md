# agent-run — Rust development migration

**Status: substantial source port, not a completed or compiled release.**

Based on `DKotsyuba/agent-run` v0.11.15 (`c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`). This archive contains actual Rust application code, not a launcher around Python. It includes a durable supervisor, SQLite schema/state code, native engine adapters, CLI, MCP/socket interfaces, answer verification, quota logic, completion delivery, tests and migration documents.

The authoring environment had neither `rustc` nor Cargo and could not access the package registry from the terminal. **Rust compilation and Rust tests were not run; no Cargo.lock was generated.** Several upstream compatibility paths are explicitly unported. Do not deploy this over a working installation.

Русское введение: [С чего начать](docs/START_HERE_RU.md).

Start with:

- [Migration status and blockers](docs/MIGRATION_STATUS.md)
- [Project map](docs/PROJECT_MAP.md)
- [Migration plan](docs/MIGRATION_PLAN.md)
- [Operator guide](docs/OPERATOR_GUIDE.md)
- [Dependencies and decisions](docs/DEPENDENCIES.md)
- [Checks actually performed](validation/REPORT.md)

## Evaluation build

```sh
cargo generate-lockfile
cargo fmt --all
cargo test --locked --all-targets --features test-fixtures
cargo build --locked --release
./target/release/agent-run --home "$HOME/.agent-run-rust-eval" init
```

Review and commit the lockfile before release. Use an isolated evaluation home; enable runtimes with absolute installed engine paths in `config.toml`. Run `api serve` before submitting `start` or connecting MCP. The fixture tests do not call paid providers.

An optional, small JavaScript transport remains for the signed Codex Desktop Node host. Python is not an application runtime dependency. Real Codex/Claude/GLM/Qwen binaries remain external prerequisites, as they are in the original project.

MIT license; original copyright retained. No repository branch, production installation, account credential, or remote service was changed by producing this archive.
