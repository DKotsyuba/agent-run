# SQLite contention during native execution

A required ownership checkpoint is not a best-effort metric. Native frames may
advance only after newly captured PID/token/birth identities are durable. A
storage failure must not become process-death evidence or a successful result.

The failure and slice-based correction below describe versions 0.25.0–0.25.1.
Version 0.26 routes online mutations through the resident broker's sole
tokio-rusqlite writer; supervisors keep read-only connections and submit typed
operations with atomic durable receipts. The writer's acquire allowance is
250 ms, and producer retries retain the exact operation and original deadline.
Unknown commits require receipt resolution, never replay of a native action.
The ownership and recovery-evidence requirements below remain in force.
See [the current database architecture](architecture.md#durable-state).

## Failure path and correction

In the 0.25.0 implementation, `supervisor::ownership_writer` used two synchronous
100 ms lock waits inside a 250 ms streaming/control slice. Ordinary store
connections already allowed five seconds. `Process` treated exhaustion of one
slice as fatal ownership loss, so a 450 ms WAL writer could stop a healthy
native execution with `runtime_ownership_checkpoint_failed`.

The correction keeps that short synchronous slice and the existing five-second
ordinary allowance. `Process::checkpoint_ownership_ready` gates the same pending
frame across async waits. It replays only `remember_processes` whose **BEGIN
IMMEDIATE returned SQLITE_BUSY before a transaction was acquired**. The
append-only ownership snapshot is idempotent, and its captured revision is
advanced only after success. No transaction crosses an await; no model, input,
tool, event or business action is replayed. `SQLITE_LOCKED`, writes, commits and
identity/validation errors remain fatal. The clock, last refusal and attempt
count survive cancellation of an outer read. An in-flight synchronous slice
remains bounded; expiry prevents a subsequent attempt. Primary exit observation
and the existing two-second verified-descendant escalation continue between
slices.

`claim_command_for_turn` first probes eligible work read-only. Empty queues and
native-completion wakes held during an active turn do not acquire the WAL
writer. An actual claim rechecks eligibility inside its immediate transaction,
so queue priorities and exactly-once claims remain unchanged.

All examined ownership, journal and runtime-storage mutation transactions contain
only synchronous SQLite work. Spool/file preparation occurs outside ownership
transactions. No ownership transaction waits for an engine, remote service or
async task. The old content-free incident diagnostic does not identify the
historical competing writer, exact SQL statement or elapsed wait; the synthetic
reproduction establishes this code defect, not those unavailable forensic facts.

## Diagnostics and recovery evidence

Persistence errors retain the original typed SQLite cause and public machine
code. Supervisor diagnostics contain only a closed operation, stage, SQLite
code/extended code, monotonic operation elapsed milliseconds (`wait_ms`) and
completed attempt count. Operations include ownership acquire/write/commit,
store open, transcript append, session identity, service/native-result events,
command claim/acknowledgement, cleanup and terminal commit. Single non-replayed
operations report one attempt. SQL, query parameters, source messages, environment
values and credentials are excluded.

After cleanup and before final SQLite operations, both supervisor routes write
`recovery.<attempt>.json` under their private execution directory using the
existing atomic, no-follow, fsynced directory primitive. It retains the captured
ownership snapshot, actual cleanup proof or a closed observation refusal, exit
evidence and whether an engine result is available. An available redacted answer
is sealed as `recovery.<attempt>.answer.md`; it is a **candidate**, not the
accepted answer. A later SQL failure also writes `execution-failure.json` with
its safe diagnostic independently of SQLite.

`terminal_commit=not_proven_by_receipt` is deliberately permanent: the receipt
cannot change the database verdict, release a reservation, grant signalling
rights or justify replay. Existing reconciliation still requires fresh native
identity/death evidence, matching attempt ownership and its normal cleanup
checks. If final storage is unavailable, the run remains incomplete/owned until
that proof-gated recovery can act. A normal successful run may retain the same
receipt; its SQLite terminal state, accepted sealed answer and notice are the
completion evidence. No new automatic import/replay path is introduced.

The production WAL, synchronous durability, schema, busy timeouts, lifecycle
limits and PID-reuse protections are unchanged. No schema migration is needed.

## Reproduction and regression checks

All fixtures use disposable homes/databases and finite shell or bundled native
fixture engines, with no paid model calls. Run checks with a child-only environment
that removes `AGENT_RUN_HOME` and **every** `AGENT_RUN_WORKER_*` binding; never
change the executing agent's real environment. Provider supervisor/broker test
constructors also remove these ambient bindings before adding explicit fresh
fixture credentials.

```sh
cargo test --offline --locked -p agent-run-core --lib streaming_snapshot
cargo test --offline --locked -p agent-run-core --lib busy_ownership_wait
cargo test --offline --locked -p agent-run-store --test sqlite_busy
cargo test --offline --locked -p agent-run --features test-fixtures \
  --test provider_initial sqlite_busy
cargo xtask check
cargo xtask archive --verify
cargo build --locked --release --package agent-run --bin agent-run
node --test scripts/check-desktop-transport.cjs
```

Record stdout/stderr and the **direct exit status** for each command; a filtered
pipeline without separately preserved Cargo status is not passing evidence.
The tests cover short/exhausted streaming contention, read cancellation, primary
exit escalation, rollback on statement/commit-hook failures, held-wake/empty
polls, duplicate prevention, three parallel real supervisors including one isolated statement refusal, final storage outage,
private cleanup/candidate retention and native-session resume lineage. A commit
hook models a commit refusal; it does not simulate disk loss or prove a specific
historical ambiguous commit. Real disk-full, power-loss and malicious filesystem
interference are not qualified by these contention fixtures.
