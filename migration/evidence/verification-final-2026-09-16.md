# Closing verification — 2026-09-16

Measured at detached `27d8da5` (`docs(migration): regenerate the coverage map`).
The worktree `target/` was not writable (`Operation not permitted`), so all
Cargo commands used `CARGO_TARGET_DIR=/tmp/agent-run-verify-2026-09-16/target`.
The smoke home was `/tmp/agent-run-verify-2026-09-16/smoke-home`; no engine was
configured or launched.

## Suite and static checks

- `AGENT_RUN_TEST_TMP=/tmp/agent-run-verify-2026-09-16/test-tmp CARGO_HOME=/Users/pluto/projects/agent-run/.claude/worktrees/agent-run-tui-interface-37777e/.wt/verify/.cargo-home CARGO_TARGET_DIR=/tmp/agent-run-verify-2026-09-16/target cargo test --offline --workspace --all-features --no-fail-fast`: **426 passed, 0 failed, 1 ignored** (427 tests enumerated). No failing tests or assertion text.
- All fourteen requested socket-dependent tests passed here: the three in `end_to_end.rs` (`cli_exit_and_broker_restart_do_not_cancel_admitted_job`, `cancellation_stops_a_hanging_engine_and_records_terminal_state`, `zero_exit_without_terminal_result_is_not_success`); the two in `mcp_parity.rs` (`mcp_matches_python_handshake_tools_calls_notifications_and_eof`, `mcp_tools_list_matches_the_packaged_python_table`); the four `python_*` socket tests (`python_second_owner_refuses_a_live_listener`, `python_disconnect_closes_only_the_client_connection`, `python_control_lane_survives_regular_connection_pressure`, `python_socket_binds_custom_path_and_answers_ping`); `ambiguous_acknowledgement_is_recorded_and_retried`; `client_maps_a_validation_envelope_to_a_typed_error`; `claude_uds_writes_auth_then_trusted_notice_to_fake_socket`; `desktop_relay_prefers_v3_and_preserves_the_versioned_wire_contract`; and `dispatch_records_retry_and_success_evidence_for_one_bound_notice`.
- `cargo fmt --all --check`: passed, no output.
- `CARGO_HOME=/Users/pluto/projects/agent-run/.claude/worktrees/agent-run-tui-interface-37777e/.wt/verify/.cargo-home CARGO_TARGET_DIR=/tmp/agent-run-verify-2026-09-16/target cargo clippy --offline --workspace --all-targets --all-features`: passed; **1 unique warning** (3 emitted warning lines including the lib-test duplicate), `this function has too many arguments (8/7)`, in `crates/agent-run-core/src/capacity/sources.rs:84`.

## Release binary smoke

- `cargo build --offline --workspace --all-features --release`: passed.
- `init`: exit 0; created `config.toml` and `state.db` under the `700` home, both files mode `600`.
- `doctor`: exit 2; JSON findings include error `profile_directory_missing`, plus informational `supervisor_canary_ok` and `mcp_inventory_self`. Verdict: unhealthy because the throwaway home has no profiles directory.
- With `api serve` running: `agents` exit 0, object shape `{complete,items,limit,next_offset,observed_at,offset,revision,total}`; `models` exit 0, `{}`; `capacity order` exit 0, routing-status object; `limits` exit 0, `{items,observed_at}`; `doc` exit 0, `{text,topic}`; `answer ag-20260916-000000-deadbeef00` exit 2, `{"error":{"message":"unknown agent: ...","type":"AgentNotFound"}}`.
- The daemon socket existed while serving and was gone after stopping the daemon.
- MCP stdio initialize returned protocol/server capabilities; `notifications/initialized` returned no response; `tools/list` returned **11 tools**; `tools/call` for `doc` succeeded with structured content (`topic: index`).

## Coverage and board

- The mandated Python 3.14 interpreter check passed for `/Users/pluto/projects/agent-run/.venv-py314/bin/python` (3.14.3, uv-managed base). The exact coverage command initially failed only because its internal Cargo list could not create worktree `target/`; rerun with the temporary target succeeded: `ported 436`, `planned 711`, `unassigned 0`, total 1147, with 13 matcher-conflict warnings.
- Ten Python files with the most uncovered behaviors: `tests/test_codex_adapter.py` (57), `tests/test_codex_app_server.py` (40), `tests/test_service.py` (32), `tests/test_cli.py` (30), `tests/test_release_script.py` (30), `tests/test_qwen_adapter.py` (25), `tests/test_resume.py` (25), `tests/test_capacity_codex_appserver.py` (20), `tests/test_capacity_sources.py` (20), `tests/test_doctor.py` (19).
- `migration/tasks.csv` status counts: `implemented 49`, `in_progress 9`, `planned 24`.

## Exact command notes

The exact commands were:

`cargo fmt --all --check`

`CARGO_HOME=/Users/pluto/projects/agent-run/.claude/worktrees/agent-run-tui-interface-37777e/.wt/verify/.cargo-home CARGO_TARGET_DIR=/tmp/agent-run-verify-2026-09-16/target cargo build --offline --workspace --all-features --release`

`CARGO_HOME=/Users/pluto/projects/agent-run/.claude/worktrees/agent-run-tui-interface-37777e/.wt/verify/.cargo-home CARGO_TARGET_DIR=/tmp/agent-run-verify-2026-09-16/target PYTHONPATH=src /Users/pluto/projects/agent-run/.venv-py314/bin/python migration/tools/coverage_report.py`
