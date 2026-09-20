# Project map

## Baseline and evidence

Source baseline: v0.11.15 / `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`. The map describes the original architecture from its retrieved source and the corresponding Rust implementation. It is not a claim that every upstream file was read or every behavior transferred. `MIGRATION_STATUS.md` is the completion authority.

## Original application

`agent-run` is a local supervisor for coding-agent command-line engines, not an LLM inference library. It owns durable job admission, process lifecycle, state and evidence; it delegates model execution to installed Codex, Claude Code, and GLM-via-Claude executables. Qwen was part of the Python baseline but is removed under A22.

```text
CLI                 MCP stdio                   Unix socket JSON-RPC
 |                      |                              |
 +---------------- shared tool contract ---------------+
                        |
                    AgentService
                   /      |      \
      strict config       |       quota projections / delivery
      role resolution     |
                    SQLite state
                        |
                detached supervisor
                        |
           materialized private runtime home
                        |
               native engine adapter
                        |
           evidence, transcript, sealed answer
```

One-shot starts are broker-backed. A separate supervisor durably records its process identity and acknowledges ownership before slow authentication and preparation. The originating CLI/MCP connection is not the owner of execution. SQLite stores jobs, attempts, events, messages, command and delivery outboxes, route snapshots, statistics and lineage. Terminal success requires runtime completion evidence plus a valid answer artifact and completed process cleanup. Legacy timeout settings remain readable but do not stop new execution.

## Rust module correspondence

| Original module group | Rust file(s) | Responsibility |
|---|---|---|
| `domain.py`, `errors.py` | `domain.rs`, `error.rs` | IDs, requests, status machine, safe boundary errors |
| `config.py`, `native_settings.py`, `accounts.py` | `config.rs`, `adapters/materialize.rs` | Strict owner configuration and scoped account paths |
| `profiles.py`, `role_plan.py`, `effective_policy.py` | `profiles.rs`, `policy.rs`, `service.rs` | Role grants, required constraints, frozen launch identity |
| `paths.py`, `adapters/home.py` | `fs.rs` | Owned relative paths, no-follow file descriptors, synced publication |
| `state/*` | `state/mod.rs`, `state/schema.sql` | Atomic admission, state, transcripts, lineage and outboxes |
| `launch.py`, `supervisor*.py`, `process_identity.py` | `supervisor.rs`, `process.rs` | READY handoff, engine ownership, cancellation and reconciliation |
| `verify.py` | `verify.rs` | Exact-byte proof v2, legacy terminal frame, size/hash/UTF-8 validation |
| `adapters/base.py`, process transports | `adapters/mod.rs`, `adapters/io.rs` | Adapter selection, environment, bounded native protocol I/O |
| `adapters/codex/*` | `adapters/codex.rs`, materializer/plugins | Model roster, grant translation and echo validation, thread/turn lifecycle |
| `adapters/claude/*`, `glm/*` | `adapters/stream.rs`, materializer | Native stream-JSON commands, result classification, continuation |
| `adapters/snapshot*`, plugin modules | `adapters/materialize.rs`, `adapters/plugins.rs` | Asset copies, hook trust, Rust runtime snapshot proof |
| `service.py`, `resume.py`, `wait.py` | `service.rs` | Application facade, immutable identity, queries, answers and continuations |
| `dispatch.py` | `dispatch.rs`, `resources/tools.json` | One schema/tool table and argument dispatch |
| `api_socket.py`, `broker_client.py` | `transport/socket.rs`, `transport/frame.rs` | Unix socket lifecycle, bounded JSON-RPC, per-call clients |
| `mcp.py` | `transport/mcp.rs` | Official SDK protocol ownership, thin broker proxy |
| `delivery/*` | `delivery/mod.rs`, `delivery/relay.rs`, `resources/desktop-transport.cjs` | Leased delivery, safe notices, existing-session transports |
| `capacity/*` | `capacity/mod.rs`, `capacity/sources.rs` | Exact identities, forecasts, topology, ranking, source collectors |
| `cli.py`, `doctor.py`, launchd helpers | `cli.rs`, `main.rs` | CLI composition, auth invocation, diagnosis, service definitions |

## Durable state layout

The included SQL schema sets `PRAGMA user_version = 16`. The sixteen application tables are `orchestrator_sessions`, `agents`, `attempts`, `events`, `messages`, `commands`, `deliveries`, `delivery_attempt_evidence`, `capacity_samples`, `context_receipts`, `reconciliation_cursors`, `workflow_runs`, `workflow_deliveries`, `workflow_steps`, `run_stats`, and `capacity_route_snapshots`. Historical workflow tables are retained but have no newly implemented orchestration path.

State connections are short-lived local values. An SQL transaction does not span asynchronous provider I/O. Admission checks replay identity and concurrency and writes the starting row atomically. Commands are claimed before execution. Delivery attempts use expiring leases; evidence and owned-claim result commit together.

## Critical lifecycle

1. Validate typed request, runtime, configured model, role grants and explicit policy requirements.
2. Freeze non-secret launch configuration; atomically admit a durable `starting` agent.
3. Spawn a session-detached supervisor, record ownership, and perform the bounded READY handshake.
4. In the supervisor, resolve credentials and publish a generated home; launch the native engine in its own process group.
5. Journal normalized output and commands. Native completion and OS ownership are distinct evidence sources.
6. Stop/reap owned processes; seal the exact final answer if present; derive terminal status.
7. Commit terminal event, answer metadata, usage and optional completion delivery. Clients retrieve the result independently.
8. Reconcile only affirmative dead/reused ownership observations. Unknown or denied process observations cannot prove loss.

## Trust boundaries

Configuration is owner-authored; engine output and external frames are untrusted. No raw credential values belong in configuration snapshots, logs, notices or delivery evidence. Auth files are explicit exceptions to the no-symlink rule and their target identities are recorded. An answer proof is an integrity/completion record, not proof of semantic correctness or protection from an attacker who controls the same Unix account.

The broker socket authenticates by private directory/socket permissions and peer UID. Native engine sandbox enforcement belongs to the engine/OS; Rust ownership and typed enums alone do not provide containment. Codex must echo the intended grants. Delivery sends only fixed lifecycle notices into existing sessions, never executes arbitrary task/answer text.

## Dependency direction

```text
agent-run-domain
        ↑
agent-run-platform
        ↑
agent-run-config ──→ agent-run-adapters
        ↑                    ↑
agent-run-store ─────────────┤
        ↑                    │
agent-run-core ──────────────┘
        ↑
agent-run (CLI, MCP, socket composition)
```

| Crate | Modules |
|---|---|
| `agent-run-domain` | `domain.rs`, `error.rs` |
| `agent-run-platform` | `fs.rs`, `process.rs`, `launch.rs`, bounded `frame.rs`, proof primitives |
| `agent-run-config` | `config.rs`, `profiles.rs`, `policy.rs` |
| `agent-run-store` | SQLite `Store`, records, and `sql/schema.sql` |
| `agent-run-adapters` | launch plans, I/O, materialization, plugins, Codex permission rendering |
| `agent-run-core` | service, supervisor, dispatch, capacity, delivery, engine runners, proof facade |
| `agent-run` | `main.rs`, `cli.rs`, MCP/socket transports and fixture binary |
| `xtask` | standard-library-only workspace checks |

`launch.rs` (posix_spawn-first detached launch, the fork fallback, and the bootstrap identity/READY/error pipe protocol) lives in `agent-run-platform` alongside `process.rs`: it depends only on `process` and serde, spawns and identifies OS processes, and makes no product decision. `agent-run-core::supervisor` composes it with `Store` and the launch identity to decide *when* and *with what evidence* to spawn.

Capacity and delivery depend on state and bounded native transports, never CLI argument parsing. The optional Desktop bridge owns only access to the signed host channel; notice construction and validation stay in Rust.

## Layering exceptions

- `crates/agent-run-domain/src/error.rs:24`: `Error::Sql` still wraps `rusqlite::Error`. Converting this to a store-local source error would require an explicit mapping at every existing SQLite `?` boundary; that follow-up is deliberately deferred to avoid changing the established error behavior during the layout-only refactor.
- `crates/agent-run-store/src/lib.rs:1`: answer-proof verification is implemented by the platform primitive and re-exported by core. This keeps the store-to-core edge absent while preserving terminal-proof validation.

Some composition shortcuts and behavioral differences remain in this development version. They are tracked explicitly instead of presenting the diagram as a proven full migration.
