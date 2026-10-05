# Contributing

agent-run is a Rust workspace built with the toolchain pinned in
`rust-toolchain.toml`. Keep changes focused, preserve typed errors and durable
evidence, and add the smallest regression test that proves changed behavior.

Fetch the reviewed lockfile's dependencies before offline checks:

```bash
cargo fetch --locked
```

For the local edit loop, check the affected package and run its relevant test:

```bash
cargo check --frozen -p agent-run-core --all-targets
cargo test --frozen -p agent-run-core --test delivery claude_uds
```

Choose selectors for the actual changed package and behavior. This focused loop
does not replace the full integration gate. Run the primary gate at a coherent
handoff boundary:

```bash
cargo xtask check
```

That command checks formatting, lints with all targets and features, and runs
the full workspace test suite offline. Before a release-affecting change also run:

```bash
cargo xtask archive --verify
cargo build --locked --release --package agent-run --bin agent-run
node --test scripts/check-desktop-transport.cjs
```

Transport changes need a live API/MCP smoke. Adapter or supervisor changes need
recorded fixture or real-engine evidence. Use temporary homes and endpoints;
never point tests at a production database, socket, inbox, or credential store.

Release workflows publish only the qualified macOS Apple-silicon artifact.
Linux x86-64 remains a non-blocking validation lane; its release and
qualification are deferred and failures there must remain visible.

A pull request must state the problem, exact verification commands and results,
and any platform or live-resource check that was not run. Follow the invariants
in [AGENTS.md](AGENTS.md).

CI keeps dependency caches separate for the source-check and explicit native
package workloads. Trusted main jobs warm them; PRs and release jobs restore
them without changing package inputs or skipping checks. The release source
gate and exact payload build run independently, and publication requires both.
A cache hit alone does not prove a faster build: compare restore status, actual
compiler work, source identity and job timings before claiming savings.
