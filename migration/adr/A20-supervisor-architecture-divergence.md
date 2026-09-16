# A20 — Supervisor architecture divergence from the Python implementation

Status: accepted for the migration.

## Context

Five behaviors in `tests/test_supervisor.py` describe supervisor mechanics that
depend on Python language features or on a Python-only configuration object.
They were first reported as divergences without per-id evidence, re-examined by
a second agent, and then verified independently against both implementations
before being recorded here. Six further behaviors reported in the same batch
were **not** divergences and have been ported.

## Decision

### 1. Durable failure on `BaseException` — 2 behaviors, not ported

`test_a_base_exception_after_launch_is_durable_and_re_raised`,
`test_a_base_exception_during_launch_is_durable_and_re_raised`.

Python catches `BaseException` (which covers `KeyboardInterrupt` and
`SystemExit`) at `src/agent_run/supervisor.py:301` and `:315`, commits a durable
terminal outcome, and re-raises. Rust has no unwind-catching equivalent on this
path: `catch_unwind` appears nowhere in `crates/`, and the supervisor handles
failure through typed `Result` values only
(`crates/agent-run-core/src/supervisor.rs:257-300`).

The *observable* property that a killed supervisor still leaves a terminal row
is preserved by a different mechanism — reconciliation of abandoned rows — which
is covered by its own ported behaviors. What has no counterpart is the
commit-then-re-raise contract around a Python interpreter-level exception.

### 2. Invalid supervisor settings refused before launch — 1 behavior, not ported

`test_invalid_settings_are_refused_before_adapter_launch`.

Python has a `SupervisorSettings` object (`src/agent_run/supervisor.py:94`) whose
values are validated before the adapter is launched. Rust has no such type — the
identifier appears zero times in `crates/` — and uses fixed lifecycle and process
configuration (`crates/agent-run-core/src/supervisor.rs:182-190`,
`crates/agent-run-adapters/src/io.rs:52-71`). There is no settings object that
can be invalid, so there is nothing to refuse.

### 3. Sessions whose process group the supervisor does not own — 2 behaviors, not ported

`test_shared_service_row_keeps_the_supervisor_group`,
`test_shared_service_session_is_never_signalled_or_reaped`.

Python's adapter contract exposes `owns_process_group`
(`src/agent_run/adapters/base.py:220`), implemented by the Claude session
(`src/agent_run/adapters/claude/session.py:157`) and the Codex app-server
(`src/agent_run/adapters/codex/app_server.py:178`). When it is false, the
supervisor clears the owned pid and group and records its own pid as the group
(`src/agent_run/supervisor.py:335-350`), and thereafter never signals or reaps
that engine (`:570`, `:592`). This lets Python attach to a service it did not
spawn.

Rust has no such concept: `owns_process_group` appears zero times in `crates/`.
`Process::spawn` always creates the engine in its own process group and captures
ownership immediately (`crates/agent-run-adapters/src/io.rs:52-71`), and cleanup
always terminates the verified group
(`crates/agent-run-platform/src/process.rs:572-581`). Rust cannot hold a session
it does not own, so the behavior has no reachable state.

## Consequence

These five rows stay uncovered permanently and are excluded from the remaining
migration work. If Rust ever needs to attach to an externally started engine,
this ADR is the record of what that would require: an ownership flag on the
adapter contract and a supervisor path that skips signalling and reaping.

## Note on verification

The `owns_process_group` divergence was nearly rejected because a search for the
reporting agent's phrasing ("shared service") found nothing in the Python
sources; the mechanism exists under a different name at the cited lines. Search
for the mechanism, not for the vocabulary used to describe it.
