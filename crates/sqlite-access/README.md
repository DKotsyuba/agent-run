# sqlite-access

An application-independent Rust crate for bounded asynchronous SQLite work
using tokio-rusqlite 0.8.0. It depends on no agent-run crates. Source metadata
inherits the repository's version and lint policy; another project can use the
crate through its repository path or a Git dependency. For an independent copy,
`cargo package` exports a normalized manifest without workspace inheritance.

```toml
[dependencies]
sqlite-access = { path = "../agent-run/crates/sqlite-access" }
```

`Database::from_connection` adopts an already-configured connection. Keep one
writer handle in the process that owns your database service; other processes
must send application-specific authenticated requests to that service. Cloning
the handle shares the same worker connection, not another writer.

Before adoption, configure WAL, FULL durability, foreign keys and the existing
finite SQLite busy allowance. Hold `WriterLease` for that same canonical database
namespace; all cooperating writers and offline tools must use that lock. A file
lock does not block rogue SQLite clients or prove application process quiescence.

`Database::call(Operation::Write, retained_bytes, closure).await` admits without
waiting when job/byte credits are free, or returns overload before enqueue.
The closure must do finite SQL-only work, with no process/network/file effects
or await. Credits live through SQL completion even if the waiter is cancelled.
Use `transaction` for an IMMEDIATE atomic SQL unit. A leaked transaction is
rolled back and refused; callback panic poisons the connection and prevents
already-queued neighbor callbacks from executing.

`Database::reader` opens an existing WAL-aware READ_ONLY/query_only connection,
without creation, migration or immutable mode. `ConnectionRef` lets an existing
synchronous SQL layer temporarily borrow the exact library-owned connection.

`close` seals admission, drains accepted work, and retains the actual close
outcome independently of a cancelled waiter. It does not interrupt arbitrary
callback CPU, SQLite I/O or external effects. Unknown outcomes never imply
rollback. There is no automatic callback/native-action replay.

Your application owns durable request identities/receipts, replay conflicts,
authorization, migrations and ownership/cleanup rules. Do not transmit arbitrary
closures or SQL across a remote interface. Retained byte weight is a trusted
caller contract, not an allocator-level bound on arbitrary closure allocations.

Tests cover real BUSY holds, cancellation, poisoning/transaction isolation,
WAL readers, drain, cross-process leases, and concurrent 1/8/32/64-task load.
They do not independently qualify another application's routing or durability.
