# Closing verification — Rust migration

Revision: `a27e597576af` (detached HEAD), measured 2026-09-16. Unix-domain
sockets were permitted: an `AF_UNIX` bind succeeded in `/tmp/arvprobe`.

## Suite and gates

- Full suite: `cargo test --offline --workspace --all-features --no-fail-fast` — **1035 passed, 0 failed, 1 ignored**. Failure count is zero; there are no failure names or assertion texts. The worktree `target/` was not creatable (`Operation not permitted`), so `CARGO_TARGET_DIR=/tmp/agent-run-target-20260916b` was used.
- `cargo fmt --all -- --check` — exit 0, **0 warnings**.
- `cargo clippy --offline --workspace --all-targets --all-features` — exit 0, **0 warnings**; no files carry warnings.

## Release smoke

`cargo build --offline --release --workspace --all-features` — exit 0; binary
was `/tmp/agent-run-release-target-20260916b/release/agent-run`. Fresh home:
`/tmp/arvhome.hA6hUc`.

- `init` — exit 0, JSON shape `{config,home,state}`. Initial layout/modes:
  home `0700`; `.state.db.init.lock` `0600`; `config.toml` `0600`; `logs/`
  `0700`; `logs/cli.log` `0644`; `state.db` `0600`.
- `doctor` — exit 2, JSON shape `{checked_at,findings,home}`; verdict **error** (`profile_directory_missing`), with canary and MCP-self informational findings.
- With `api serve`: `agents` exit 0 `{complete,items,limit,next_offset,observed_at,offset,revision,total}`; `models` exit 0 `{}`; `capacity order` exit 0 `{deferred,insufficient_diversity,observed_at,omitted,routes,unavailable_runtimes}`; `limits` exit 0 `{items,observed_at}`; `doc` exit 0 `{text,topic}`; nonexistent valid ID `answer ag-20260916-000000-0000000000` exit 2 `{error:{message,type:AgentNotFound}}`.
- SIGTERM stopped the daemon with exit 0 and the socket file was gone.
- MCP stdio (`initialize`, `notifications/initialized`, `tools/list`, successful `tools/call` for `list_agents`) — exit 0; initialize negotiated, notification produced no response, `tools/list` returned **11 tools**, and the call returned `isError:false` with structured empty-agent data. Its daemon was also stopped and the socket was gone.

## Concurrency spot-checks

- `cargo test --offline --all-features -p agent-run-store --test state_migrations concurrent_openers_migrate_a_v1_store_once -- --exact`: **20/20 passed**.
- `cargo test --offline --all-features -p agent-run --test fake_engine engine_ignoring_sigterm_requires_sigkill -- --exact`: **20/20 passed**.

## Coverage and board

`PYTHONPATH=src /Users/pluto/projects/agent-run/.venv-py314/bin/python migration/tools/coverage_report.py` (run in a disposable archive of this revision, with `CARGO_TARGET_DIR=/tmp/agent-run-coverage-root-20260916b/target` because the checkout target is unwritable) — **1107 ported, 1 planned, 39 divergent**; matches the expected values. The tool reported 23 matcher conflict warnings and retained its 1107/1/39 totals. `migration/tasks.csv` status counts: **implemented 63, in_progress 9, planned 10**.

All measurements above used `CARGO_HOME=.../.wt/verify/.cargo-home`,
`AGENT_RUN_TEST_TMP=/tmp/arvprobe`, and `--offline --all-features` where
applicable.
