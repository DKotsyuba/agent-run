# agent-run architecture

agent-run is one Rust workspace with the `agent-run` executable (the former
`agent-run-tui` observer was removed in 0.32.0; releases seal only a fixed
compatibility shim under that name). It owns
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
| `sqlite-access` | Reusable bounded tokio-rusqlite queue, read-only handles, leases and SQL scopes |
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

Only then does `start` return ownership to the caller. The supervisor opens a
read-only store and an authenticated typed writer connection. It materializes
the runtime, starts the engine, submits observed stream/ownership evidence to
the resident writer, seals an answer, records terminal evidence, and performs cleanup.

Both provider and legacy supervisors checkpoint root and descendant identities.
Legacy success requires confirmed descendant cleanup, rather than an empty group
alone. Recovery uses an attempt-matching snapshot even if a crash occurred before
the agent group field was updated, and persists newly captured members before
recording an unresolved cleanup. PID/token/birth fences still guard every signal.

Ownership persistence gates native progress. Online mutations share one bounded
resident queue and one SQLite writer connection. A foreign WAL writer receives
a bounded 250 ms acquire wait; authenticated producer lanes may resubmit the
same operation identity and original deadline after a known pre-mutation refusal.
An operation and its durable receipt commit atomically. An uncertain commit is
resolved from its original receipt, never by repeating a model, tool or native
action. Process observation remains responsive while waiting for SQL. Empty
command polls and held native wakes remain
read-only. Before final database writes, each supervisor retains private
per-attempt cleanup/identity evidence and any sealed candidate separately from
SQLite. This receipt cannot prove terminal success or authorize signalling;
recovery must verify current database and native ownership. See
[sqlite-contention.md](sqlite-contention.md).

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
- **OpenCode** seals the configured first-party Bridge native-context ABI 2
  component and hook into each eligible home. Selected local MCP commands retain
  their persistent backends behind the protected native-context transport; the
  private worker MCP stays direct. Original native permission checks and results
  remain unchanged. The owner directory and private native data store exist before
  spawn, so immediate identity observation has a canonical store. An older
  lifecycle-only Bridge component is refused; arbitrary plugins and restricted
  research/IPC policies remain denied.
- **Claude Code** uses its native CLI protocol and retains the session identity
  for continuation. The GLM adapter belongs to historical schema-1 runs.
- Claude CLI background-task notifications are contextual native input. Only the
  closed top-level replay origin (task-notification/session-task, with an optional
  bounded native runId label), canonical UUID,
  user text shape and established session can register one. Notification text
  cannot grant task authority. Runner inputs still own task/steering correlation;
  coalesced terminals may name the last notification or the most recent runner
  input followed only by validated native context. An older steer cannot complete
  a newer task, and notification-only results
  cannot replace a task answer. Unknown, duplicate, malformed or foreign-session
  replays fail closed. A bounded refusal event retains only a fixed reason tag.
- Engine binaries and authentication remain external. Agent tasks execute through
  adapters and the supervisor. Agent Quota collects and evaluates quota separately;
  Run reads its results. Old Run history does not populate Quota.

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
current schema is version 40. Numbered migrations live in `sql/migrations/`
and apply transactionally after a pre-version backup. Ordinary commands and
the broker refuse an older database with `migration_required` until the paired
`agent-run config migrate` runs. The step to 17 also introduces the schema-2
config and account registry. A binary refuses a database newer than its
supported schema.

The resident broker owns the sole cooperative writer lease. `sqlite-access`
runs finite SQL callbacks on tokio-rusqlite's thread; accepted job/byte credits
survive caller cancellation and drain retains the lease until actual closure.
Typed application operations own authority, ordering, receipts and process
identity checks. Remote callers cannot submit SQL or closures. `Store::open`
is read-only and never migrates; initialization and schema upgrades require
an exclusive offline guard with broker exclusion and proven quiescence.
No online direct-write fallback exists. History maintenance advances a volatile
keyset cursor between bounded pages, rechecking complete durable evidence inside
the writer before deletion. See [the reusable crate](../crates/sqlite-access/README.md).

## Legacy Run pools (removed in 0.32.0)

Schemas 25, 26 and 28 created the legacy cooperative pool tables (`pools`,
`pool_members`, `pool_entries`, `pool_enrollments`), the per-root
`agents.pool_membership_ever` marker and the `worker_capabilities.pool_catalog_*`
catalog-proof columns. The pool domain, broker dispatch, public tools
(`start_pool`, `pool`, `pool_post`, `pool_replace`, `list_pools`), worker tools
(`pool_post`, `pool_read`, `pool_propose`, `pool_vote`), CLI commands,
`bind --pool`, settlement, push commands, pool notices and the terminal
observer were removed; Agent Pool owns groups, membership and consensus.
Nothing was migrated.

The schema is unchanged: no table or column is dropped and every migration
stays replayable, so an older binary still opens the database. No code path
inserts or updates the legacy pool tables. Their writer operations are removed
from the closed inventory and their wire ids (4-7, 47-51, 95) stay reserved:
a frame naming one is refused as an unknown kind and the id is never reused.
A due notice with historical native attempts and no Bridge intent ends
`failed` with `legacy_delivery_held`. Other due legacy pool completion and
enrollment attention notices end `failed` with `legacy_pool_retired` at claim.
Unbound terminal agent notices expire after one hour on a claim sweep, with
legacy pool completion rows excluded. A queued `pool` command completes
without a push (`unsupported_command`, or `agent_terminal` once the run
ended). Legacy pool-loop and enrollment steers are refused before native
input, and stored health snapshots carrying old pool wait
evidence still decode. Pool membership no longer suppresses individual
completion notices. The `pool_membership_ever` independence fence (and its
immutability trigger) remains durable; managed Agent Pool enrollment still
sets it. The `pool_catalog_*` columns keep recording the current worker
catalog proof under their historical names.

Retention still treats an old pool as a unit: every member lineage stays
protected until all of them expire by the ordinary rule, then entries, members
and the pool row are deleted before the agent, delivery and session rows, as
the foreign keys require. Archival reads use read-only SQL
(`agent-run doc completion`).

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

Bound terminal runs create durable notice rows. Agent Bridge is the sole external
sender for completion, worker reports and maintenance notices. The
pinned SDK publishes Address/Message/KeepAll without expiry or caller-registration
authority. MCP runs directly; Agent Run has no native message frontend, Claude
inbox sender, relay inventory/probe or fallback process.

The first lease always selects Bridge, even without deployment configuration.
Run uses the orchestrator UID reported by the Unix socket for start, batch
start, explicit resume, and hook binding. MCP over stdio reaches the broker
through the MCP process's socket connection, so the broker sees that process's
UID. Direct CLI binding uses the writer socket's peer UID. Private managed
admission uses the authenticated Pool peer's UID for an explicit orchestrator
reference. The binding records the first verified UID and refuses a conflicting
UID for the same session.

For shared brokers, configure each owner's Bridge stores in `config.toml`.

```toml
[delivery.bridge]
registration_socket = "/path/to/bridge/registration.sock"
core_socket = "/path/to/bridge/core.sock"
token_file = "/path/to/protected/delivery-token"
host_id = "dobi"
claude_store = "claude-agent-claude"
codex_store = "codex-agent-codex"

[delivery.bridge.stores]
1000 = { claude-code = "claude-pluto", codex = "codex-pluto" }
1001 = { claude-code = "claude-agent-codex", codex = "codex-agent-codex" }
1002 = { claude-code = "claude-agent-claude", codex = "codex-agent-claude" }
```

Run selects the store for the bound UID and harness. A missing UID (including
migrated bindings), an unmapped UID, or an unmapped harness uses the corresponding
fallback store. `agent-run doctor` reports the validated configuration in
`bridge_stores`. The mapping applies when a notice's destination is first frozen.
Unconfigured notices retain their domain outcome and retry at least 60 seconds apart
without exhausting the attempt allowance. Previously attempted native rows never
gain Bridge ownership or new event IDs. Accepted historical facts remain readable;
unresolved native attempts end held with `legacy_delivery_held` and retained evidence.

Before possible I/O, the sole writer freezes IDs, destination, complete normalized
publication and producer/service/credential association. Verified custody releases
only local publication. Unknown custody permits same-owner status-only recovery;
absent history never permits fresh publication. Receipts must match the producer,
event, full publication hash and direct destination. Endpoint or credential-file
changes cannot re-home a frozen publication. Bridge owns native dispatch and holds;
native acceptance, model visibility, answer proof and cleanup remain separate facts.

Delivery attempts are leased, bounded, and retried with backoff. Persisted
diagnostics contain safe classifications and redacted tails, never task or
answer text, session IDs, argument or environment values, or credentials.
The dispatcher reads retry policy from the active configuration schema once,
before it leases a delivery, so schema-2 homes complete each claimed attempt
before its lease can be retried. A missing or malformed configuration at that
point leaves the delivery unsent; a change after the lease never discards an
acknowledgement or its evidence.

## Capacity and diagnostics

Agent Quota collects observations, computes quota standings and presents
quota windows and capacity to operators and orchestrators. With schema 2, Run
keeps only the read its admission needs: each admission decision reads one
fresh Quota snapshot, combines it with its own local account, cooldown,
multiplier and reservation snapshot, selects the account, and refuses an
exhausted choice with `QuotaExhausted`. Run presents no quota: since 0.32.0
`delegation_guide` is a configuration-only catalog and the UserPromptSubmit
context carries only active agents. Missing, stale or unavailable quota is
unknown, not an invented zero; Run may still attempt a launch.

`agent-run doctor` checks configuration, binaries, role assets, state,
schema-2 provider bindings against a read-only account-registry snapshot,
supervisor identity, MCP processes and delivery. It does not check quota
freshness: use `agent-quota limits` for quota evidence and `agent-quota doctor`
for service availability. Its provider-free canary exercises the production
detach, identity, and READY path.

## Releases

A sealed native release contains `bin/agent-run`, metadata, checksums, and a
`COMPLETE` marker. Release directories are immutable and the deployment helper
switches `standalone/current` only after manifest verification, writer
quiescence, backup, and schema checks.

The release workflow builds separate macOS Apple-silicon and Linux x86-64
artifacts and requires both native gates. Historical releases contain only
macOS; Linux qualification remains a candidate until hosted evidence exists.
launchd integration is built in on macOS. Linux can use the bundled systemd
service examples. A platform-only integration such as launchd fails explicitly
where it is unavailable. See [releasing.md](releasing.md).

## Design invariants

1. Configuration fails closed and credentials stay outside snapshots.
2. Admission is durable before execution starts.
3. PID reuse or unreadable identity never authorizes a signal.
4. Success is derived from evidence, not adapter optimism.
5. One dispatcher defines every transport's public tool surface.
