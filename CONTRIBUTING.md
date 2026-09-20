# Contributing

agent-run is a Rust workspace built with the toolchain pinned in
`rust-toolchain.toml`. Keep changes focused, preserve typed errors and durable
evidence, and add the smallest regression test that proves changed behavior.

Run the primary gate:

```bash
cargo xtask check
```

That command formats, lints with all targets and features, and runs the full
workspace test suite offline. Before a release-affecting change also run:

```bash
cargo xtask qualify --release
cargo xtask evidence verify
cargo xtask archive --verify
cargo build --locked --release --package agent-run --bin agent-run
node --test scripts/check-desktop-transport.cjs
```

Transport changes need a live API/MCP smoke. Adapter or supervisor changes need
recorded fixture or real-engine evidence. Use temporary homes and endpoints;
never point tests at a production database, socket, inbox, or credential store.

Release workflows build macOS Apple silicon and Linux x86-64 artifacts. macOS
has accepted qualification evidence. Linux remains pending until a hosted
Linux run passes the full suite and sealed-release verification; do not report
it as qualified before that evidence exists.

A pull request must state the problem, exact verification commands and results,
and any platform or live-resource check that was not run. Follow the invariants
in [AGENTS.md](AGENTS.md).
