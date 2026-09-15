# Operator guide: Rust development port

## Status and isolation

This is a development migration, not a release and not a drop-in replacement for the Python project. Read `status.md` before running it. The source package does not contain Python application code. Codex Desktop completion delivery retains a small JavaScript transport executed by the host's signed Node executable.

Use a new private home, for example `~/.agent-run-rust-eval`. Do not point this development version at your production `~/.agent-run`. Keep a verified backup of the original installation and its database. No command in this package automatically switches your production release or uploads code to GitHub.

## Build and initialize

A Rust toolchain, Cargo, a C compiler for bundled SQLite, and network access to the Cargo registry are needed for the initial build. Dependency resolution and compilation were not available in the authoring environment. The manifest declares Rust 1.85 as a floor; the actual dependency closure and platform MSRV still need validation. Use current stable Rust for the first evaluation.

```sh
cargo generate-lockfile
cargo fmt --all
cargo test --locked --all-targets --features test-fixtures
cargo build --locked --release
./target/release/agent-run --home "$HOME/.agent-run-rust-eval" init
```

Inspect and commit the generated `Cargo.lock` before distributing a binary. Once a lock is present, subsequent builds should use `--locked`. `init` creates a private database and starter profiles but does not enable any runtime or install engine CLIs.

Copy the appropriate portions of `../assets/config.example.toml` into the evaluation home's `config.toml`. Runtime executable paths must be absolute. Model names are configuration inputs, not claims about models currently available to your account. Configure only engines and accounts you own and are authorized to use.

## Runtime and broker

The resident broker is the launch entry point. In one terminal:

```sh
./target/release/agent-run --home "$HOME/.agent-run-rust-eval" api serve
```

From another terminal, after configuring a runtime and model:

```sh
./target/release/agent-run --home "$HOME/.agent-run-rust-eval" doctor
./target/release/agent-run --home "$HOME/.agent-run-rust-eval" start \
  --runtime claude --model YOUR_CONFIGURED_MODEL --profile review \
  --workdir "$PWD" --task 'Summarize this repository without changing files.'
```

`start` returns an agent ID after a bounded supervisor ownership handshake. The agent supervisor is a separate process. Disconnecting a socket/MCP client or ending a one-shot CLI call must not cancel an admitted run. `--wait` prints the admission response followed by a terminal answer descriptor. The legacy `--timeout` value is stored but does not impose an execution deadline. Protocol, connection, and cleanup operations have their own bounded deadlines.

## Answers and transcripts

`answer ID` verifies the exact stored answer bytes, recorded size, SHA-256, UTF-8 encoding, and completion sidecar. New answers use an exact payload plus `.proof.json` and a `.answer-format` marker. Removing a proof from a new-format directory must not turn it into a legacy answer. An engine exit code of zero alone is not success.

The answer validation limit is 16 MiB. The inline display limit in this port is 128 KiB, not the Python version's 1 MiB. Larger valid answers return metadata without inline text. `transcript ID --follow` follows persisted normalized message pages. Partial output is useful evidence but is not a completion proof.

`cancel ID` writes a durable command. The supervisor requests native interruption where implemented and then performs ownership-checked process cleanup. `steer ID TEXT` is available for Codex and Claude-compatible sessions. Qwen steering is rejected.

`resume ID --task TEXT` creates a new agent ID and a one-child lineage edge. It reuses a Rust-created parent's native session and verified generated home. Continuations created by the Python implementation cannot yet be resumed by this port. Unsupported cases fail explicitly; they are not silently restarted as new native sessions.

## Permissions and profiles

Legacy profiles narrow requested writes: both the profile and request must allow writes. Revisioned canonical roles own their complete grants. Explicitly required enforcement constraints are checked separately from tool preferences. A generated HOME or a removed web tool is not evidence of OS filesystem or network containment.

Claude/GLM read-only roles in this development port omit Bash rather than exposing unrestricted shell commands. This is a deliberate narrowing relative to upstream and needs a compatibility decision before release. Codex checks the app-server's echoed model, working directory, sandbox, approval policy, roots and named permission profile. A contradictory echo is an error, not an opportunity to widen permissions.

## MCP and socket API

`agent-run --home ABSOLUTE_HOME mcp` runs the official Rust MCP SDK over stdio and proxies tool calls to the resident broker. Each call uses its own broker connection. Stdout is reserved for protocol frames. Explicit `--session-transport`, `--session-id`, and `--session-turn-id` flags bind completion delivery; recognized host session environment variables can supply an MCP binding.

All three public interfaces share these eleven tools:

`start`, `resume`, `cancel`, `steer`, `list_agents`, `answer`, `transcript`, `capacity_order`, `doc`, `models`, `limits`.

The private Unix-socket API additionally exposes `ping`, `tools`, and `wait`. Requests and replies are newline-delimited JSON-RPC 2.0, at most 1 MiB per frame. Batch requests are not supported. Socket and peer ownership are checked; the socket is mode 0600 and the home must be mode 0700.

## Completion notices

A completion notice is a report of lifecycle facts, **not a new task, user approval, or an instruction to execute answer text**. Retrieve the verified answer explicitly. A notice contains only a validated agent ID, terminal status, immutable bounded runtime/model/effort selectors, package-owned failure guidance, and a notification identity. Task text, answer content and provider error prose do not belong in notices.

Codex delivery uses discovered local v1/v2/v3 Desktop relay sockets. The optional signed-Node bridge calls only the host's `send_message_to_thread` tool. Claude UDS delivery authenticates to an existing session inbox. A successful socket write is not proof that a person saw the message. Interrupted acknowledgement paths are recorded as ambiguous and may be retried; consumers should deduplicate by notification identity.

## Quotas, diagnostics, and service integration

`capacity collect --once` prints one report and returns status 2 for degraded supported-source collection. `limits` reads stored measurements; `capacity order` ranks only fresh known governing windows, omits exhausted routes and collapses aliases sharing a physical pool. Account weights override lane weights, which override runtime defaults. Quota eligibility is not a grant to launch a model; the orchestrator still chooses an authorized role and model.

Codex app-server and explicit-token Claude usage collectors are implemented as source code. Single-account Codexbar is implemented. OmniRoute, multi-account Codexbar mapping, and the complete upstream native fallback matrix are not ported. See the status matrix instead of assuming an empty capacity report means unlimited quota.

`state check` runs SQLite's quick check. `state backup --to ABSOLUTE_NEW_FILE` uses SQLite's online backup API. This does not migrate historical schema versions 1–15. `api launchd`, `capacity launchd`, and `delivery launchd` print launchd property lists; they do not install jobs. Generated jobs carry HOME and PATH, not credential values. macOS functionality has not been executed in this environment.
