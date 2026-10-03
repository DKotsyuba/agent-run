# agent-run architecture

agent-run is one Rust workspace with the `agent-run` executable and a separate
read-only `agent-run-tui` observer, published at the same version. It owns
durable admission, detached supervision, runtime materialization, evidence, and
completion delivery for providers using Codex and Claude Code. Historical GLM
adapter records remain readable. For the local wire contract, see
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
| `agent-run-store` | SQLite schema, migrations, events, projections |
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

Schema 2 separates `[harnesses.<id>]` launch settings from `[providers.<id>]`
models, connections and account bindings; `runtimes.*` belongs to schema 1.

Each fresh start gets a generated lineage home. The adapter materializes only the
declared runtime settings, account bridge, skills, MCP servers, hooks, plugins,
and role policy. Managed assets are indexed and hashed; the durable launch
identity retains the frozen configuration and authority. A continuation reuses
the lineage only after the stored snapshot
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

A provider run has one execution deadline: admission time plus its stored
`timeout_seconds`. Preparation and every account-switch attempt consume that
same budget. Before each spawn the supervisor checks the remaining time; it
bounds execution by that remainder, cleans up on expiry, and records
`timed_out`. There is no independent silence watchdog. Success requires a
verified answer plus completion and cleanup evidence; exit code alone is never
enough.

Native process identity and signalling are described in
[process-identity.md](process-identity.md).

## Runtime adapters

- **Codex** speaks the app-server protocol, preserves normalized stream deltas,
  and stores the native thread identity for continuation.
- **Claude Code** uses its native CLI protocol and retains the session identity
  for continuation. The GLM adapter belongs to historical schema-1 runs.
- Engine binaries and authentication remain external. Agent tasks execute through
  adapters and the supervisor. Quota metadata comes from separately configured,
  bounded executables; the shipped Codex collector starts no model turn.

`resume` keeps the public `agent_id` and admits a new internal execution linked
to the latest terminal run. Each predecessor can
have only one child. It reuses the native conversation only after its immutable
authority, generated-home snapshot, history and cleanup proofs verify. See
[continuations.md](continuations.md).

## Durable state

SQLite is the source of truth for agents, events, messages, answers, native
session lineage, deliveries, cleanup evidence, capacity, and statistics. The
current schema is version 25. Numbered migrations live in `sql/migrations/`
and apply transactionally after a pre-version backup. The step to 17 is paired
with the schema-2 config: ordinary commands and the broker refuse an older
database with `migration_required` until `agent-run config migrate` runs. A
binary refuses a database newer than its supported schema.

Schema 25 lays the foundation for cooperative pools (a small roster of
ordinary executions sharing one goal) with `pools`, `pool_members` and
`pool_entries`. The schema, the validated domain types, the compact entry renderer and
atomic batch admission exist so far: the store admits every member agent, its
reservations and the pool roster in one transaction (or none), and the core
composes each member's task with the common goal, its own seat and every peer's
stable identity before any member is launched. Replay is keyed by the original
client request, never by the composed text. Pool members carry a fixed private
worker catalog — `notify_orchestrator`, `pool_post`, `pool_read`,
`pool_propose` and `pool_vote` — served over one private broker route that
authenticates the hidden per-attempt capability and stamps the author (kind,
name, role, stable identity) from durable membership inside the same
transaction; refusals such as `not_pool_member`, `pool_completed` or
`stale_proposal` are typed codes, never prose. Members pull their pool: the
append-only log pages by immutable cursor (optionally holding a bounded wait
for a new entry), renders each entry as one compact plain-text block through
the shared renderer, and reports derived status — current roster, current
proposal and each member's vote validity with its reason. Ordinary chat is
budgeted while proposals, votes, blocks and revokes stay possible, and
agreement is reported without completing the pool. Every appended entry also enqueues
one `pool` command, holding only its sequence number, for each current peer's
tip in the same transaction (a member's `notify_orchestrator` report likewise
writes one linked team-copy entry, and the orchestrator's notice is rendered
from that entry with the same stamped sender, direction and stable identity). The command
claims after cancel and steer, re-checks that the recipient is still a current
member's tip of that entry's pool, and records only a finite push disposition:
`native_accepted` for a correlated Codex reply, `written` for a Claude/GLM stdin
write, `rejected`, `unsent`, `refused` (with a typed reason) or `unknown` after
a possible write. None of them means the member consumed the entry; the log
remains the source of truth and members catch up through `pool_read`.
The core also offers operator operations, not yet exposed through any tool, CLI
or MCP method: posting a message stamped as the operator (fanned out through the
same path and chat budget, idempotent by key), reading a pool's status and
cursor-paged log through the projection members read, and replacing one current
member. A replacement is refused with `member_busy` unless the member's latest
execution is terminal and every attempt of its lineage has verified cleanup
(decided again inside the same immediate transaction as the write); it admits
the new execution, retires the old seat while keeping its row, installs the new
seat in the same slot, bumps the roster revision, appends one broker roster
entry to the peers and records the original request digest, so a repeat of the
same key returns the same new identity even after later replacements. An omitted
start restores the seat's original user spec (never the account a prior
automatic choice picked). The new execution's task names the goal, the current
roster and a catch-up instruction; peers' frozen prompts are corrected by the
roster entry, never rewritten.

A pool completes only by formal verification at one consistent moment inside one
immediate transaction: the current proposal has a valid ready vote, covering
every acceptance criterion, from every current member for the current roster
revision; every current member's latest execution `succeeded`; and every attempt
of every member lineage has verified cleanup. Agreement while anyone still runs,
a failed, timed-out, cancelled or lost member, uncleared ownership, a missing,
revoked, blocking or stale vote, or a roster that moved never completes the
pool; no timer or heuristic infers success, and agent-run never judges whether
the result is right. Completion freezes one immutable `pool_completed` event on
a member row (goal, criteria, accepted proposal, stable roster, votes and proofs,
plus the compact notice text) and one linked outbox row, then marks the pool
completed, all in the same transaction, so concurrent or repeated settlement
records exactly one of each. It is attempted after votes, after every supervisor
terminal path through `complete_terminal`, and by the bounded maintenance sweep
(at most twenty open pools per pass, rotating) that also converges cleanup proof
which arrives after the terminal write. A completed pool's status is read from
the frozen record: a member resumed later does not change it or mint another
notice, and the closed pool refuses further writes and replacements.

The notice is a broker conclusion for the whole pool, delivered over the
existing outbox as a typed pool payload (the Desktop relay's `pool_completion`
operation, rendered by the frontend's fixed template, and the Claude inbox as
text). It names the stable pool ID, goal, accepted result and roster, never a
run, attempt or session identity; a result longer than the 4096-byte bound is
shortened with an explicit marker naming the full proposal entry in the pool
log. An unbound pool still completes: its notice stays `waiting_binding`
(exempt from the one-hour binding window and not activated by binding a single
member) until the pool itself is bound, which the store's `bind_pool` does for
the pool and every current member tip. No tool, CLI or MCP method calls
`bind_pool` or exposes completion yet, and retention of completed pools is a
later stage. The tables retain replaced members, keep one current member per slot, store
an immutable author stamped at send time, and reference agents and deliveries
without cascades so a later purge can delete pool rows first.

Large payloads live under the run directory and are referenced by path, size,
and SHA-256. A terminal success must be reproducible from stored state and
sealed files.

Completed database history expires after fourteen days, and everything beyond
the newest hundred logical sessions (resume lineages) expires by count,
through the resident broker's bounded maintenance loop; active work and
retained lineage are protected.
Idle compaction returns unused SQLite pages to disk. See
[history-retention.md](history-retention.md) for retention and space-reclamation rules.

## Completion delivery

Bound runs create durable delivery rows. When Desktop supplies both capability
paths, `agent-run mcp` replaces its process image with that exact signed Node
executable. The frontend owns the private typed v1-v4 relay and native-tools
pipe, then starts the same Rust MCP command with both capability variables
removed. It renders only the embedded completion-notice contract and can call
only the namespaced `send_message_to_thread` tool; the Rust child owns MCP stdio
but cannot contact native host tools. Without both capabilities, the Rust MCP
runs directly. Claude uses its configured local UDS transport. Unbound callers
retrieve completion through `wait`, `answer`, or `list_agents`.

Desktop tool calls identify their caller with `callerSource: "codex"` in the
native host envelope. A correlated JSON-RPC invalid-parameters rejection is a
known refusal, while disconnects after sending, uncorrelated replies and generic
execution errors remain ambiguous. The frontend code lives in its MCP process;
after updating it, reconnect the Agent Run MCP client as well as the broker.

The frontend passes its PID to its Rust MCP child through a private environment
marker. The child observes its own parent relationship every 250 milliseconds
and exits if that relationship ends, even when inherited stdin stays open.
This also covers frontend SIGKILL, which cannot run JavaScript cleanup handlers.
The resident broker and its admitted detached jobs have independent ownership.

Delivery attempts are leased, bounded, and retried with backoff. Persisted
diagnostics contain safe classifications and redacted tails, never task or
answer text, session IDs, argument or environment values, or credentials.
The dispatcher reads retry policy from the active configuration schema, so
schema-2 homes complete each claimed attempt before its lease can be retried.

## Capacity and diagnostics

Capacity collectors store timestamped provider observations and explicit
physical quota topology. `limits` reports freshness and projections without
calling providers; `capacity order` returns an advisory compatible-route order
and never launches work. Missing or stale evidence becomes unknown rather than
an invented zero.

`agent-run doctor` checks configuration, binaries, role assets, state,
schema-2 provider bindings against a read-only account-registry snapshot,
supervisor identity, MCP processes, delivery, and capacity freshness. Its
provider-free canary exercises the production detach, identity, and READY path.

## Releases

A sealed native release contains `bin/agent-run`, metadata, checksums, and a
`COMPLETE` marker. Release directories are immutable and the deployment helper
switches `standalone/current` only after manifest verification, writer
quiescence, backup, and schema checks.

Release automation publishes a macOS Apple-silicon artifact. macOS is the only
qualified 0.12.3 release platform. Linux x86-64 remains a visible non-blocking
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
