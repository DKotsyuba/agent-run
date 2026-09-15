# Dependency choices

The manifest is a dependency selection, **not a resolved or audited lock**. Semver ranges below permit compatible updates. Only `rmcp` is exactly pinned while its inspected API is being integrated. Generate, review and commit Cargo.lock before release. No dependency audit or Rust build was executed in the authoring environment.

| Crate / requested version | Used for | Choice and caution |
|---|---|---|
| `tokio` 1.44-compatible | Async process pipes, Unix sockets, bounded I/O, SDK execution, timers, signals | Feature list is explicit. Execution itself has no age timeout; transport/cleanup operations do. Blocking SQL/filesystem sections require profiling. |
| `rmcp` =1.8.0 | Official Rust MCP server, stdio transport, protocol lifecycle | Replaces handwritten MCP negotiation. No server macros or unused client transport enabled. Compile/API verification still required. |
| `rusqlite` 0.32-compatible | SQLite transactions, queries, backups | `bundled` provides a consistent SQLite build; `backup` is used. Explicit SQL preserves the existing schema better than introducing a new ORM/migration schema. Requires a C compiler. |
| `serde` 1.0 / `serde_json` 1.0 | Typed requests, stored values, JSON-RPC and native engine frames | Unknown owner configuration/request fields are rejected. Canonical hashing explicitly sorts maps to avoid feature-unification assumptions. |
| `toml` 0.8-compatible | Strict config/frontmatter parsing and native Codex generation | Native settings remain typed values; reserved control roots cannot be overridden. |
| `clap` 4.5-compatible | Typed CLI, help, subcommands and environment defaults | Literal arguments, no shell expansion. CLI parity is not assumed from parsing success. |
| `thiserror` 2.0 | Typed errors | Public errors bound and redact low-level details. |
| `chrono` 0.4 | UTC agent IDs and aware provider timestamps | Only the clock feature is explicitly requested; timestamp freshness remains explicit. |
| `uuid` 1.16-compatible | Agent entropy, lease identities, private temporary names | Uses OS-backed randomness through the dependency closure; no counter-derived IDs. |
| `sha2` 0.10 | Exact answer and generated-file digests | Integrity evidence, not a signature or defense against the same account rewriting both metadata and content. |
| `libc` 0.2 | Unix descriptor operations, signals, process birth inspection, session setup | Unsafe blocks carry comments; OS-specific code needs review and tests. |
| `fs2` 0.4 | Single broker file lock | Prevents concurrent broker ownership of the socket path. |
| `regex` 1.11-compatible | Plugin declaration/path substitutions | Linear-time engine, no external parser process. |
| `reqwest` 0.12.24-compatible | Explicit Claude OAuth usage request | A deliberately selected 0.12 API lane, not a claim to use the newest release. Rustls TLS, no default proxy or redirect behavior for credential-bearing collection; body and time limits are explicit. |
| `tempfile` 3.19-compatible, development only | Isolated fixtures and test state | Not part of product runtime behavior. |

## External executables

Installed `codex`, `claude` and/or `qwen` engine CLIs remain required for their adapters. GLM uses a configured Claude-compatible CLI with the declared Z.ai authentication route. Native logins are delegated to those executables; the Rust application does not implement provider OAuth flows itself.

Codexbar is optional. A signed Node executable and a host tools pipe are used only when Codex Desktop supplies both capabilities for completion delivery. The Node helper has a fixed tool surface and is locally contract-tested, but the actual Desktop authorization/transport has not been tested.

macOS-specific execution may use `/bin/ps`, Xcode's `xcrun` Git discovery and generated launchd definitions. This port does not install or silently download those prerequisites.

## Deliberately not selected

No Python interpreter, embedded Python, PyO3, Python subprocess fallback, HTTP web server framework, ORM, container runtime or generic autonomous-agent framework is needed to implement this application's local orchestration core. The application owns process/state invariants explicitly; commodity MCP, JSON, SQLite, CLI and TLS functionality is delegated to libraries.

## Verification before release

Resolve the lock on supported platforms, confirm the declared Rust floor or raise it honestly, inspect unexpected feature unification, check licenses and advisories, run Cargo tests/Clippy on Linux/macOS, and verify dependency changes are reviewed. Do not publish unlocked binaries or assert that the selected graph is vulnerability-free without an audit.

## Primary references inspected / reference entry points

- Official Rust MCP SDK: https://github.com/modelcontextprotocol/rust-sdk and https://docs.rs/rmcp/1.8.0/rmcp/
- Tokio: https://docs.rs/tokio/latest/tokio/
- Rusqlite: https://docs.rs/rusqlite/0.32.1/rusqlite/
- Reqwest selected API lane: https://docs.rs/reqwest/0.12.24/reqwest/
- Cargo lockfile guidance: https://doc.rust-lang.org/cargo/guide/cargo-toml-vs-cargo-lock.html

These are primary documentation entry points, not evidence of a completed security audit or a successful build of this archive.
