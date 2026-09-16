# Rust migration verification — 2026-09-16

BASE: `72e0cbf` (clean worktree). Unix sockets were available. Cargo could not create `target/` in the worktree (`Operation not permitted`), so all Cargo commands used `CARGO_TARGET_DIR=/tmp/agent-run-target-verify`; Cargo was offline with the requested `CARGO_HOME`.

## Checks

- `CARGO_HOME=/Users/pluto/projects/agent-run/.claude/worktrees/agent-run-tui-interface-37777e/.wt/verify/.cargo-home CARGO_TARGET_DIR=/tmp/agent-run-target-verify AGENT_RUN_TEST_TMP=/tmp/arv-test cargo test --offline --workspace --all-features --no-fail-fast`: exit 0; 271 passed, 0 failed, 1 ignored (`macos_keychain_smoke`). No failing test names or assertion text.
- The ten socket-dependent tests all passed: `cli_exit_and_broker_restart_do_not_cancel_admitted_job`, `cancellation_stops_a_hanging_engine_and_records_terminal_state`, `zero_exit_without_terminal_result_is_not_success`; `mcp_matches_python_handshake_tools_calls_notifications_and_eof`, `mcp_tools_list_matches_the_packaged_python_table`; `python_second_owner_refuses_a_live_listener`, `python_disconnect_closes_only_the_client_connection`, `python_control_lane_survives_regular_connection_pressure`, `python_socket_binds_custom_path_and_answers_ping`; `ambiguous_acknowledgement_is_recorded_and_retried`.
- `cargo fmt --all --check`: exit 0, no output.
- `CARGO_HOME=/Users/pluto/projects/agent-run/.claude/worktrees/agent-run-tui-interface-37777e/.wt/verify/.cargo-home CARGO_TARGET_DIR=/tmp/agent-run-target-verify cargo clippy --offline --workspace --all-targets --all-features`: exit 0, 6 warning diagnostics. Top files: `crates/agent-run-core/src/codex.rs` (1), `crates/agent-run-core/src/delivery/relay.rs` (2), `crates/agent-run-core/src/capacity/sources.rs` (1), `crates/agent-run-store/src/delivery.rs` (1), `crates/agent-run/tests/protocol.rs` (1). Warnings were not fixed.
- `CARGO_HOME=/Users/pluto/projects/agent-run/.claude/worktrees/agent-run-tui-interface-37777e/.wt/verify/.cargo-home CARGO_TARGET_DIR=/tmp/agent-run-target-verify cargo build --offline --release --bin agent-run`: exit 0; no release-only failure.

## Isolated-home smoke

Home: `/tmp/agent-run-smoke-4hXd9r`; no engine was configured or launched. `init`: exit 0, JSON with `home`, `config`, and `state`. Home and all created directories (`accounts`, `agents`, `logs`, `probes`, `profiles`, `runtimes`, `skills`) were mode 0700; `config.toml`, `state.db`, and the seven profile files were mode 0600.

`doctor`: exit 2, verbatim output (bounded): `{"broker_available":false,"checks":[{"name":"state","result":{"integrity":"ok","ok":true,"schema_version":16,"tables":16}}],"home":"/private/tmp/agent-run-smoke-4hXd9r","ok":false,"validation_level":"filesystem-and-configuration; provider authentication is checked at launch"}`. This is a clean state check but overall verdict is false because no broker is running.

With `api serve` running: `agents` exit 0, empty page object; `models` exit 0, `{}`; `capacity order` exit 0, route/deferred/unavailable summary object; `limits` exit 0, empty `items` object; `doc` exit 0, `{text,topic}` object with topic `index`; valid nonexistent `answer ag-20260916-000000-0000000000` exit 2, stderr JSON `{error:{message:"unknown agent: ...",type:"AgentNotFound"}}`. Daemon stop exit 0 and `api.sock` was absent afterward.

Live `mcp` over stdio completed with exit 0: initialize returned JSON-RPC id 1, `notifications/initialized` returned no response, `tools/list` id 2 returned exactly 11 tools, and `tools/call` id 3 for `models` returned `structuredContent:{}` with `isError:false`.

## Board

`migration/tasks.csv`: 29 `implemented`, 9 `in_progress`, 44 `planned` (82 total). Planned IDs: M09, M10, M13a, M13b, M14a, M16, M17, M20d, M22b, M25, M28a, M28b, M28c, M29, M30a, M30d, M31b, M32c, M33, M34a, M34b, M35a, M35b, M35c, M35d, M36, M37, M38, M41b, M42, M43a, M43b, M43c, M43d, M45, M48, M50a, M50b, M51, M52, M53, M54, M55, M56.
