# Working on agent-run

agent-run is a Rust supervisor for other coding agents. Reliability, durable
evidence, process ownership, and secret safety are product requirements.

## Ground rules

- Use the pinned Rust toolchain and the committed `Cargo.lock`. Prefer the
  standard library and existing workspace dependencies.
- `family.toml` and `.family/manifest.json` are the family-standard
  declaration: keep them truthful, rerun `cargo xtask family update` after a
  reviewed change to a managed adoption file, and never claim qualification
  the repository has not tested (see docs/family-standard.md).
- Keep modules focused. Add tests for every behavior change.
- Agent-task harnesses are created only through adapters and the supervisor.
  Agent Quota owns collection and forecasting; Run reads its bounded snapshots.
- Preserve unrelated work and never weaken lifecycle assertions to hide flakes.
- Release workflows build macOS Apple-silicon and Linux x86-64 artifacts on
  their native runners. Both platform checks gate publication; do not claim
  qualification or publication without the corresponding observed evidence.

## Invariants

- The domain dispatcher owns one public tool table shared by CLI, MCP, and the
  Unix-socket JSON-RPC server.
- `agent-run start` submits through the resident broker; it never silently runs
  asynchronous work under the one-shot CLI process.
- SQLite schema changes require a numbered migration, current-schema update,
  version bump, and historical migration tests.
- Bad inputs and domain failures remain typed across all transports.
- A successful outcome must be derivable from durable answer, completion,
  process-identity, and cleanup evidence.
- Process groups are signalled only while the recorded leader identity is
  verified alive. Do not trade PID-reuse safety for unconditional cleanup.
- Delivery diagnostics are immutable, bounded to 4096 UTF-8 bytes, and never
  persist messages, session ids, arguments, environment values, or credentials.
- Configuration reloads compare the file SHA-256 every 60 seconds and at request
  boundaries. Invalid revisions never replace the last valid configuration.
- Release directories remain immutable and must pass manifest plus `COMPLETE`
  verification before pointer switching.

## Layout

| Path | Responsibility |
|---|---|
| `crates/agent-run` | CLI, API daemon, MCP transport, launchd helpers |
| `crates/agent-run-tui` | interactive terminal observer (broker client, read-only) |
| `crates/agent-run-domain` | public contracts, tools, errors, state machine |
| `crates/agent-run-config` | configuration, profiles, snapshots, role plans |
| `crates/agent-run-store` | SQLite store, migrations, reconciliation |
| `crates/agent-run-adapters` | Codex, Claude, and GLM adapters |
| `crates/agent-run-core` | service, supervisor, capacity, delivery, doctor |
| `crates/agent-run-platform` | process, filesystem, and artifact primitives |
| `xtask` | checks, archives, releases, deployment |
| `assets/operator_guide` | embedded `agent-run doc` pages |
| `docs` | public architecture and operations documentation |

## Verification

```bash
cargo xtask check
cargo xtask archive --verify
cargo build --locked --release --package agent-run --bin agent-run
node --test scripts/check-desktop-transport.cjs
```

`cargo xtask check` requires cargo-nextest: tests run one process per test
with every failure reported, doctests and the custom-harness `launch_fd6`
target run under `cargo test`, and test temporaries go to a private tmpfs
directory (`XTASK_TEST_TMPFS=0` keeps them on disk). `RUST_TEST_THREADS` is
passed to nextest as `--test-threads`. Each package builds its integration
tests as one `tests/it/main.rs` binary: add every new `tests/*.rs` there as a
module (the gate enforces it), declare shared helpers once there, and let a
test that re-runs itself pass its module-qualified name (`exact_test`). Tests
serialized by `FOLLOW_TESTS` must also be listed in `.config/nextest.toml`.
While iterating, run targeted tests such as
`cargo nextest run -p agent-run-core -E 'test(/^delivery::/)'`.

If a transport changed, add a live API/MCP smoke. If an adapter or supervisor
changed, identify the fixture or real-engine run proving it. `agent-run doctor`
must remain clean. Release and deployment procedures are in
`docs/releasing.md`.
