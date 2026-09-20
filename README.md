# agent-run

`agent-run` is a self-contained Rust supervisor for Codex, Claude Code, and
GLM coding agents. It runs children as durable asynchronous jobs, records
verified outcomes in SQLite, tracks provider capacity, and exposes the same
tool surface through its CLI, MCP stdio server, and Unix-socket JSON-RPC API.

The broker and its release artifact do not require Python. Engine CLIs remain
external dependencies and must be installed and authenticated separately.

## Supported release target

The first Rust-primary release supports macOS on Apple silicon
(`aarch64-apple-darwin`). Linux is compiled and tested in CI as validation-only;
it is not yet a supported release target. macOS-specific Keychain and launchd
integration are unavailable elsewhere.

## Install

Download `agent-run-0.12.0-aarch64-apple-darwin.tar.gz` and `SHA256SUMS` from
the matching GitHub Release, verify the checksum, then place the binary on
`PATH`:

```bash
shasum -a 256 -c SHA256SUMS
tar -xzf agent-run-0.12.0-aarch64-apple-darwin.tar.gz
install -m 0755 bin/agent-run ~/.local/bin/agent-run
agent-run init
```

Build from source with the pinned Rust toolchain:

```bash
cargo build --locked --release --package agent-run --bin agent-run
install -m 0755 target/release/agent-run ~/.local/bin/agent-run
```

The home defaults to `~/.agent-run`; override it with `AGENT_RUN_HOME` or
`--home`.

## Configure

Configuration lives in `<home>/config.toml`. Unknown agent-run keys and
reserved native control fields fail closed. Runtime aliases are `codex`,
`claude`, and `glm`.

```toml
schema_version = 1

[runtimes.codex]
enabled = true
adapter = "codex"
binary = "/opt/homebrew/bin/codex"
home = "/Users/you/.agent-run/runtimes/codex"
models = ["gpt-5.6-sol"]
limits_source = "codex_appserver"
```

Per-runtime configuration supports declared auth sources, skills, MCP servers,
hooks, plugins, native engine settings, and capacity sources. Credential values
do not belong in this file. The resident broker hashes the file every minute
and reloads valid changes; new requests also check immediately. Invalid changes
leave the last valid configuration active.

Initialize and inspect the installation:

```bash
agent-run init
agent-run doctor
```

## Run the broker

`start` is broker-owned: it submits through the resident Unix socket and never
falls back to a worker owned by the short-lived CLI process.

```bash
agent-run api serve
```

For a long-lived macOS installation:

```bash
agent-run api launchd --binary "$(command -v agent-run)" \
  > ~/Library/LaunchAgents/com.agent-run.api.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.agent-run.api.plist
```

## Use the CLI

```bash
agent-run start --runtime codex --model gpt-5.6-sol --profile review \
  --task "Review this repository." --workdir "$PWD"

agent-run agents
agent-run answer ag-...
agent-run transcript ag-...
agent-run resume ag-... --task "Continue with the highest-priority finding."
```

Output is line-delimited JSON. Other commands include `steer`, `cancel`,
`models`, `limits`, `capacity order`, `delivery status`, and `doc`.

## Use the MCP server

The MCP process is a thin stdio proxy over the resident broker:

```json
{"command":"agent-run","args":["--home","/Users/you/.agent-run","mcp"]}
```

It exposes `start`, `resume`, `cancel`, `steer`, `list_agents`, `answer`,
`transcript`, `capacity_order`, `doc`, `models`, and `limits`.

## Use the socket API

The broker binds `<home>/api.sock` with mode `0600`. It accepts newline-framed
JSON-RPC 2.0 and provides the tool methods plus `ping`, `tools`, and `wait`.
See [docs/api.md](docs/api.md).

## Reliability contract

- SQLite admission precedes execution, so accepted jobs remain durable.
- Success requires answer proof, completion evidence, and verified cleanup.
- Generated runtime homes include only declared auth bridges, tools, skills,
  hooks, plugins, and policy.
- The broker checks configuration hashes every 60 seconds and on requests.
- Release directories are immutable, checksummed, and gated by `COMPLETE`.

Development and release procedures are in [CONTRIBUTING.md](CONTRIBUTING.md)
and [docs/releasing.md](docs/releasing.md).
