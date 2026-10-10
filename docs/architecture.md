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

Both provider and legacy supervisors checkpoint root and descendant identities.
Legacy success requires confirmed descendant cleanup, rather than an empty group
alone. Recovery uses an attempt-matching snapshot even if a crash occurred before
the agent group field was updated, and persists newly captured members before
recording an unresolved cleanup. PID/token/birth fences still guard every signal.

Provider and legacy executions have no wall-clock limit. Preparation and account
switches do not consume an overall time budget. Native completion, failure or
explicit cancellation ends execution; silence only provides diagnostic evidence.
Success requires a verified answer plus completion and cleanup evidence; exit
code alone is never enough. Operational bounds remain on startup ownership,
transport I/O, collector commands, service probes and cleanup.

Native process identity and signalling are described in
[process-identity.md](process-identity.md).

## Runtime adapters

- **Codex** speaks the app-server protocol, preserves normalized stream deltas,
  and stores the native thread identity for continuation.
- **Claude Code** uses its native CLI protocol and retains the session identity
  for continuation. The GLM adapter belongs to historical schema-1 runs.
- Claude CLI background-task notifications are contextual native input. Only the
  closed top-level replay origin (task-notification/session-task, with an optional
  bounded native runId label), canonical UUID,
  user text shape and established session can register one. Notification text
  cannot grant task authority. Runner inputs still own task/steering correlation;
  coalesced batches may end in a notification, and notification-only results
  cannot replace a task answer. Unknown, duplicate, malformed or foreign-session
  replays fail closed. A bounded refusal event retains only a fixed reason tag.
- Engine binaries and authentication remain external. Agent tasks execute through
  adapters and the supervisor. Quota metadata comes from separately configured,
  bounded executables; the shipped Codex collector starts no model turn.

`resume` keeps the public `agent_id` and admits a new internal execution linked
to the latest terminal run. Each predecessor can
have only one child. It reuses the native conversation only after its immutable
authority, generated-home snapshot, history and cleanup proofs verify. See
[continuations.md](continuations.md). Exact resume replays validate immutable
intent before checking current configuration or directory existence. New legacy
resumes freeze the raw timeout override in the replay hash; historical hashless
rows compare their stored request using frozen policy. New admissions still
require existing canonical directories.

## Durable state

SQLite is the source of truth for agents, events, messages, answers, native
session lineage, deliveries, cleanup evidence, capacity, and statistics. The
current schema is version 26. Numbered migrations live in `sql/migrations/`
and apply transactionally after a pre-version backup. The step to 17 is paired
with the schema-2 config: ordinary commands and the broker refuse an older
database with `migration_required` until `agent-run config migrate` runs. A
binary refuses a database newer than its supported schema.

Schema 25 lays the foundation for cooperative pools (a small roster of
ordinary executions sharing one goal) with `pools`, `pool_members` and
`pool_entries`. The schema, the validated domain types, the compact entry renderer and
atomic batch admission share one immediate transaction: new executions,
reservations and the roster commit together (or none), and the core
composes each member's task with the common goal, its own seat and every peer's
stable identity before any member is launched. Replay is keyed by the original
client request, never by the composed text. Schema 26 adds attempt-pinned
`pool_enrollments`, observed worker catalog proof, and a permanent per-root
membership marker. A mixed batch can attach supported independent RUNNING
workers without another execution or reservation; the transaction rechecks the
exact attempt, immutable launch facts and compatible binding.
The membership marker also covers new seats and replacements and survives pool
history collection; historical independence remains unknown conservatively.
Existing seats receive durable context and remain pending until an authenticated
`pool_post` summary matches their opaque challenge. Replays authenticate the
pinned attempt before idempotent lookup; native transport receipts never imply
awareness. Unjoined seats cannot vote or settle. Their overlay is retained with
active pool history and removed before eligible entry/member GC. Failure queues
one broker-authored attention through the existing outbox, suppressed by an
already queued individual error. Codex attention requires v5 support and defers
before a send attempt on older peers without blocking ordinary v4 notices.

All current workers carry a fixed private
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
The operator operations (public tools `start_pool`, `pool_post`, `pool_replace`
and `pool`, one CLI command each, MCP and the broker socket from the one shared
tool table; the private worker catalog stays the fixed five tools): posting a message stamped as the operator (fanned out through the
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
the pool and every current member tip. `agent-run bind --pool <id>` and the
post-tool hook (a structured `start_pool` reply, recognized before any
single-agent receipt) call it; a conflict with an existing binding changes
nothing, and the binding is immutable. Replacements and resumed members join the
pool's actual stored session, not the reference frozen at the first start.

Retention treats a pool as a unit. Every member lineage, replaced members
included, stays stored (with its native history, seals and cleanup evidence)
until every execution of every member has expired by the ordinary rule
(fourteen days, or ranked outside the newest hundred logical sessions), none
owns an attempt or lacks verified cleanup, and no linked notice is pending,
retrying or being sent. Then pool entries, members and the pool row are purged
before the underlying agent, delivery and session rows, as the foreign keys
require; a common notice still `waiting_binding` at that point is expired
explicitly (`pool_binding_expired`) rather than lingering. The read-only
preflight uses the same predicate, so protected-only pools never wake the
writer. The maintenance sweep for completion likewise evaluates readiness
read-only and takes the writer lock only for a pool that can actually complete.
The tables retain replaced members, keep one current member per slot, store
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
The dispatcher reads retry policy from the active configuration schema once,
before it leases a delivery, so schema-2 homes complete each claimed attempt
before its lease can be retried. A missing or malformed configuration at that
point leaves the delivery unsent; a change after the lease never discards an
acknowledgement or its evidence.

## Capacity and diagnostics

Capacity collectors store timestamped provider observations and explicit
physical quota topology. With schema 2, `delegation_guide` provides compact routing guidance
with exact provider/model/profile filters. Optional `limits` diagnostics retain
quota percentages, resets and freshness and add numerical provider/model
standing from the same committed snapshot and advice clock, without calling
providers. `models` and `capacity_order` remain call-only compatibility views
with their original CLI/socket responses; they are absent from discovery.
Schema 1 retains its historical limits windows and compatibility views; the
guide is Unsupported there. Missing or stale evidence becomes unknown rather
than an invented zero.

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
