# Working on agent-run

Instructions for coding agents (and humans in a hurry). The product is a
local supervisor for running other coding agents — so the bar for
reliability of *this* code is set by everything that will run on top of it.

## Ground rules

- **Python 3.11+, standard library only.** The runtime package has zero
  dependencies (`pyproject.toml`) and ships as a sealed venv release. Do
  not add runtime dependencies — not for HTTP, not for CLI parsing, not
  for anything. `pytest` is the only dev-time extra.
- **No monolithic modules.** Keep files focused; a module drifting past
  a few hundred lines is a design smell here, not a habit to copy.
- **Engines are driven only through adapters + the supervisor.** Never
  spawn `codex`/`claude`/`qwen` binaries directly from feature code; the
  adapter renders the child's home/config, the supervisor owns the
  process tree, timeouts, and outcome classification.
- **Every change lands with tests.** The suite is `unittest`-style,
  collected by pytest:

  ```bash
  python3 -m pytest -q --rootdir . tests
  ```

  It must end `N passed, 1 skipped, 0 failed`. A handful of
  timing-sensitive tests in `tests/test_launch.py` can flake under heavy
  parallel machine load — rerun the module in isolation before suspecting
  your change, and never weaken their assertions to make them pass.

## Invariants that tests enforce (do not fight them)

- **One tool table, many transports.** `src/agent_run/dispatch.py` owns
  `TOOLS`/`call_tool`; the stdio MCP server (`mcp.py`) and the Unix-socket
  JSON-RPC server (`api_socket.py`) both import it. Parity tests fail if a
  transport grows its own list. Add a tool once, in the dispatch table.
- **SQLite connections are thread-affine.** A `StateStore` connection may
  only be used on the thread that created it (`StateStore.path()` exists
  so another thread can open its own). The socket API routes all dispatch
  through one owning thread — copy that pattern, don't share stores across
  threads.
- **Detached launch is `posix_spawn`-first.** The resident API daemon is
  multithreaded, so never reintroduce Python work between `fork` and `exec`.
  The legacy fork path is allowed only when session-creating `posix_spawn` is
  explicitly unavailable; PID/PGID, readiness, cleanup, and reap evidence must
  stay exact.
- **Schema changes go through migrations.** `state/schema.sql` is the
  current shape; every change also needs a numbered file in
  `state/migrations/` and a schema-version bump. Old MCP servers
  version-check and refuse politely — that is intended behavior.
- **Errors are typed.** Raise `ValidationError` for bad input and
  `AgentRunError` subclasses for domain failures; transports map them to
  protocol errors uniformly. Don't return error-shaped dicts.
- **Answers are evidence, not trust.** Agent outcomes carry
  sha256/size/path and completion sentinels; anything that reports
  "succeeded" must be derivable from recorded state, not from an
  adapter's optimism.

## Layout

| Path | What lives there |
|---|---|
| `src/agent_run/cli.py` | argument parsing + command wiring (entry point `agent-run`) |
| `src/agent_run/dispatch.py` | transport-neutral tool table and dispatcher |
| `src/agent_run/broker_client.py` | client for the resident Unix-socket daemon |
| `src/agent_run/mcp.py` | stdio MCP transport (thin proxy over the socket daemon) |
| `src/agent_run/api_socket.py` | Unix-socket JSON-RPC transport (`docs/api.md`) |
| `src/agent_run/service.py` | AgentService — the domain facade every transport calls |
| `src/agent_run/adapters/` | one package per engine (codex, claude, glm, qwen, …) |
| `src/agent_run/supervisor*.py` | detached child supervision: timeouts, stall watchdog, outcomes |
| `src/agent_run/state/` | SQLite store, schema, migrations, reconciliation |
| `src/agent_run/wait.py` | blocking watchdog logic shared by CLI and socket API |
| `src/agent_run/workflow*` | the multi-step workflow engine and its runner |
| `src/agent_run/capacity/` | limits collection (per-runtime sources) and risk advisory |
| `src/agent_run/doctor.py` | self-diagnosis; keep it free of false alarms |
| `src/agent_run/operator_guide/` | the pages `agent-run doc` serves |
| `docs/` | architecture and integration docs for humans |
| `skills/` | skills shipped to child agents |

## Verifying your work

1. Full suite (command above) — green, honestly gated (check the exit
   code, not the tail line).
2. If you touched a transport: the parity tests plus a live smoke of that
   transport (`agent-run mcp` speaks stdio JSON-RPC; `agent-run api serve`
   binds `<home>/api.sock`).
3. If you touched adapters/supervisor: state your evidence — which live
   run or recorded fixture proves the behavior. Fixtures for engine output
   live next to the adapter tests; prefer captured real transcripts over
   invented ones.
4. `agent-run doctor` should stay clean; a new warning class needs a very
   good reason to exist.
