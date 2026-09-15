# Validation report

## Scope

The source port is **not compiled or release-validated**. This report deliberately separates checks that ran from test code that merely exists.

The authoring environment had Node.js v22.16.0, Python with SQLite 3.46.1, and no `rustc` or Cargo on PATH. Direct terminal network access was unavailable. No Cargo dependency closure could be resolved. No Rust build, Rust unit test, Rust integration test, Clippy run or rustfmt run is claimed.

## Checks that actually ran

| Check | Observed result | What it establishes |
|---|---|---|
| `node --check resources/desktop-transport.cjs` | Passed | JavaScript syntax only |
| `node --test scripts/check-desktop-transport.cjs` | **10 passed, 0 failed** | Local mock-host tests of the small Node bridge: fixed tool, accepted/rejected/ambiguous results, disconnects, oversized inventory and invalid private requests |
| SQL schema execution with SQLite 3.46.1 | Passed; user_version 16; 16 application tables | Included SQL parses and creates the expected empty layout |
| SQLite `quick_check` / `foreign_key_check` on that fresh layout | `ok` / no rows | Fresh-schema integrity, not migration of production data |
| Preparation of extracted source SQL using SQLite `EXPLAIN` | **58 statements prepared; 0 errors** | Table/column/SQL-syntax consistency and placeholder binding count under static substitution; not correctness of Rust parameter order or query behavior |
| Cargo TOML / example TOML / resource JSON parsing | Passed | Document syntax and eleven unique tool names |
| Rust lexical delimiter scan using Pygments | No lexical error tokens or unmatched delimiters | A weak source sanity check, **not a Rust parser, compiler or borrow checker** |
| All `include_str!` targets | Present | Packaged static resources are not missing |
| `sh -n scripts/check.sh` | Passed | Shell syntax only |
| `./scripts/check.sh` | Exited with the explicit missing-Cargo diagnostic | Confirms Rust checks were not silently treated as passing |

Raw results: `node-transport-tests.tap`, `static-checks.json`, and `prepared-sql.json`.

The static SQL pass substitutes only the known active-status and list-filter templates; its exact prepared queries are saved. It does not execute application state transitions or validate the Rust-to-SQL type mapping.

## Tests authored, not executed

There are **78 Rust test functions** in this snapshot, including three feature-gated end-to-end tests using an offline fake engine. They target strict identifiers/configuration, status transitions, profiles and policy evidence, request replay, state/lineage/outboxes, answer integrity and no-follow files, quota normalization/freshness/ranking, JSON-RPC framing, process birth identity, broker-independent completion, cancellation and missing terminal results.

The count is inventory, not a pass count. Compilation errors, test assertion failures, behavioral regressions and platform differences may remain. The original Python regression suite has not been exhaustively ported or run against the Rust implementation.

## Not validated

Rust type checking and linking; the locked dependency graph/MSRV; original database migration and historical native-resume fixtures; actual engine invocation on Linux/macOS; macOS sandbox/keychain/launchd behavior; real Codex Desktop or Claude inbox delivery; production-scale concurrency, memory/load and crash/PID-race cases; complete source/API/CLI equivalence; security review or dependency advisory audit.

## Reproduction in a Rust environment

```sh
cargo generate-lockfile
# Review Cargo.lock and the resolved dependency graph before publishing it.
cargo fmt --all
./scripts/check.sh
cargo build --locked --release --bin agent-run
node --test scripts/check-desktop-transport.cjs
```

The first command generates a missing artifact; it was not performed successfully here. The CI definition intentionally requires a reviewed, committed lockfile and does not represent a successful CI run.
