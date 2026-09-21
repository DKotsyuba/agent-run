# agent-run architecture

agent-run is one Rust workspace and one native `agent-run` executable. It owns
durable admission, detached supervision, runtime materialization, evidence, and
completion delivery for Codex, Claude, and GLM. For the local wire contract, see
[api.md](api.md).

## Component map

```text
CLI ───────────────┐
MCP stdio proxy ───┼─> shared dispatcher ─> service ─> store / adapters
Unix socket API ───┘                              └─> detached supervisor
```

| Crate | Responsibility |
|---|---|
| `agent-run` | CLI, MCP proxy, Unix-socket daemon, launchd helper |
| `agent-run-domain` | public requests, responses, tools, errors, states |
| `agent-run-config` | strict config, profiles, role plans, snapshots |
| `agent-run-store` | SQLite schema 16, migrations, events, projections |
| `agent-run-adapters` | Codex, Claude, and GLM preparation and protocols |
| `agent-run-core` | service, supervisor, lifecycle, delivery, capacity, doctor |
| `agent-run-platform` | native launch, process identity, safe files, snapshots |

The dispatcher is the only public tool table. CLI, MCP, and socket JSON-RPC
route through it, and parity tests prevent transport-specific tool surfaces.
MCP is a thin proxy to the resident broker, so the broker remains the only
launch host.

## Configuration and generated homes

`<home>/config.toml` is strict and credential-free. The service caches the last
valid revision, compares the file SHA-256 every 60 seconds, and checks again at
request boundaries. A malformed changed file is rejected without replacing the
cached configuration.

Each fresh run gets a generated lineage home. The adapter materializes only the
declared runtime settings, account bridge, skills, MCP servers, hooks, plugins,
and role policy. Managed trees and the sanitized config snapshot are hashed and
indexed. A continuation reuses the lineage only after the stored snapshot
verifies; it does not silently rebuild from changed live assets. See
[artifact-snapshots.md](artifact-snapshots.md) and
[runtime-contract.md](runtime-contract.md).

## Admission and supervision

`start` validates the request and commits a `starting` row before process
creation. The broker starts the hidden `_supervisor` command as a detached
session leader, then requires three bounded bootstrap steps:

1. the child reports its exact PID;
2. ownership and native birth identity are committed;
3. the child reports READY.

Only then does `start` return ownership to the caller. The supervisor opens its
own store connection, materializes the runtime, starts the engine, journals its
stream, seals an answer, records terminal evidence, and performs cleanup.

The runtime execution itself has no automatic deadline or silence watchdog.
Compatibility timeout fields remain in request identity, but a run ends only
when the engine finishes or cancellation is requested. Success requires a
verified answer plus completion and cleanup evidence; exit code alone is never
enough.

Native process identity and signalling are described in
[process-identity.md](process-identity.md).

## Runtime adapters

- **Codex** speaks the app-server protocol, preserves normalized stream deltas,
  and stores the native thread identity for continuation.
- **Claude** and **GLM** use their supported native CLI protocols and retain the
  corresponding session identity when continuation is available.
- Engine binaries and authentication remain external. agent-run never embeds
  provider credentials or invokes an engine outside its adapter and supervisor.

`resume` admits a new durable row linked to the latest terminal run and reuses
the native conversation only after its immutable authority and generated-home
snapshot verify. See [continuations.md](continuations.md).

## Durable state

SQLite is the source of truth for agents, events, messages, answers, native
session lineage, deliveries, cleanup evidence, capacity, and statistics. The
current schema is version 16. Numbered migrations live in `sql/migrations/`;
write opens migrate transactionally after making a pre-version backup, while
read-only opens report that migration is required. A binary refuses a database
newer than its supported schema.

Large payloads live under the run directory and are referenced by path, size,
and SHA-256. A terminal success must be reproducible from stored state and
sealed files.

## Completion delivery

Bound runs create durable delivery rows. Codex Desktop delivery crosses the
host boundary through the signed Node relay supplied by the host; the Rust MCP
child receives no host capability. Claude uses its configured local UDS
transport. Unbound callers retrieve completion through `wait`, `answer`, or
`list_agents`.

Delivery attempts are leased, bounded, and retried with backoff. Persisted
diagnostics contain safe classifications and redacted tails, never task or
answer text, session IDs, argument or environment values, or credentials.

## Capacity and diagnostics

Capacity collectors store timestamped provider observations and explicit
physical quota topology. `limits` reports freshness and projections without
calling providers; `capacity order` returns an advisory compatible-route order
and never launches work. Missing or stale evidence becomes unknown rather than
an invented zero.

`agent-run doctor` checks configuration, binaries, role assets, state,
supervisor identity, MCP processes, delivery, and capacity freshness. Its
provider-free canary exercises the production detach, identity, and READY path.

## Releases

A sealed native release contains `bin/agent-run`, metadata, checksums, and a
`COMPLETE` marker. Release directories are immutable and the deployment helper
switches `standalone/current` only after manifest verification, writer
quiescence, backup, and schema checks.

Release automation publishes a macOS Apple-silicon artifact. macOS is the only
qualified 0.12.1 release platform. Linux x86-64 remains a visible non-blocking
validation lane; its release and qualification are deferred. launchd
integration is built in on macOS. Future Linux deployments use an external
service manager. A platform-only integration such as launchd fails explicitly
where it is unavailable. See [releasing.md](releasing.md).

## Design invariants

1. Configuration fails closed and credentials stay outside snapshots.
2. Admission is durable before execution starts.
3. PID reuse or unreadable identity never authorizes a signal.
4. Success is derived from evidence, not adapter optimism.
5. One dispatcher defines every transport's public tool surface.
