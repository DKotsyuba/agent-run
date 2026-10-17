//! Application-independent, bounded access to one library-owned SQLite connection.
//!
//! SQL runs on tokio-rusqlite's thread. Admission is nonblocking and bounded by
//! job count and caller-declared retained payload bytes. Accepted calls survive
//! caller cancellation; no mutation is automatically retried. Application
//! authorization, durable request receipts and remote transport belong above
//! this crate. A local SQL closure must never perform external effects or await.

use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use std::{
    error::Error as StdError,
    fmt,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, Semaphore, watch};

/// Cross-process exclusion for cooperative writers and offline maintenance.
pub mod lease;
pub use lease::WriterLease;
/// Reusable nested SQL transaction scopes, with rollback on Drop.
pub mod scope;
pub use scope::WriteScope;

/// Open an existing WAL-aware read-only connection for a synchronous SQL adapter.
/// READ_ONLY/NOFOLLOW and query_only refuse writes; no create or migration occurs.
/// The caller sets a finite existing busy allowance and keeps read snapshots short.
pub fn reader_connection(path: &Path, busy_wait: Duration) -> rusqlite::Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    connection.busy_timeout(busy_wait)?;
    connection.pragma_update(None, "query_only", true)?;
    Ok(connection)
}

/// Owned or temporarily borrowed SQLite connection for existing SQL adapters.
///
/// A borrowed variant lets legacy synchronous SQL helpers operate on the exact
/// connection owned by tokio-rusqlite, without opening a second connection.
/// The borrow cannot escape its callback or outlive the library connection.
pub enum ConnectionRef<'connection> {
    /// A synchronous connection before adoption or an offline/read-only handle.
    Owned(Connection),
    /// The library-owned connection borrowed for one synchronous SQL callback.
    Borrowed(&'connection mut Connection),
}

impl From<Connection> for ConnectionRef<'_> {
    /// Wrap a newly opened synchronous connection without changing its settings.
    fn from(connection: Connection) -> Self {
        Self::Owned(connection)
    }
}

impl std::ops::Deref for ConnectionRef<'_> {
    type Target = Connection;
    /// Expose this callback's connection for existing synchronous SQL methods.
    fn deref(&self) -> &Connection {
        match self {
            Self::Owned(conn) => conn,
            Self::Borrowed(conn) => conn,
        }
    }
}

impl std::ops::DerefMut for ConnectionRef<'_> {
    /// Borrow this same connection mutably; never create an independent writer.
    fn deref_mut(&mut self) -> &mut Connection {
        match self {
            Self::Owned(conn) => conn,
            Self::Borrowed(conn) => conn,
        }
    }
}

/// Closed operation labels suitable for diagnostics without SQL or arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// A read performed on this connection.
    Read,
    /// An application mutation, with its own transaction/receipt semantics.
    Write,
    /// Schema work performed only under application-proven offline exclusion.
    Migration,
    /// Checkpoint or bounded maintenance outside a business transaction.
    Maintenance,
}

/// Limits cover accepted work, including a currently executing call.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum accepted operations; overload rejects before enqueueing.
    pub jobs: usize,
    /// Maximum sum of retained payload byte weights of accepted operations.
    pub bytes: usize,
    /// Maximum declared retained payload bytes for one operation.
    pub job_bytes: usize,
}

impl Default for Limits {
    /// A modest bounded default; applications may choose lower measured limits.
    fn default() -> Self {
        Self {
            jobs: 128,
            bytes: 8 * 1024 * 1024,
            job_bytes: 1024 * 1024,
        }
    }
}

impl Limits {
    /// Reject zero, impossible semaphore capacities or a per-job bound above total.
    fn validate(self) -> Result<Self, CallError<std::convert::Infallible>> {
        if self.jobs == 0
            || self.jobs > Semaphore::MAX_PERMITS
            || self.bytes == 0
            || self.bytes > u32::MAX as usize
            || self.bytes > Semaphore::MAX_PERMITS
            || self.job_bytes == 0
            || self.job_bytes > self.bytes
        {
            return Err(CallError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Content-free measurements for a failed executed call; one attempt, no replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Diagnostic {
    /// Closed kind of the failed local operation.
    pub operation: Operation,
    /// Time between admission and starting the SQL closure, in milliseconds.
    pub queue_ms: u64,
    /// Time spent inside the SQL closure, including SQLite busy waits.
    pub execution_ms: u64,
    /// SQLite primary numeric code, when the error chain contains one.
    pub sqlite_code: Option<i32>,
    /// SQLite extended numeric code, when available.
    pub sqlite_extended_code: Option<i32>,
    /// Always one: this layer never repeats callbacks or commits.
    pub attempts: u32,
}

/// Errors distinguish unaccepted work from failed/uncertain accepted work.
#[derive(Debug)]
pub enum CallError<E> {
    /// No request was enqueued: job count budget was already occupied.
    Overloaded,
    /// No request was enqueued: retained byte credits were already occupied.
    BytesFull,
    /// No request was enqueued: declared payload exceeds its permitted size.
    PayloadTooLarge,
    /// No request was enqueued: drain/close has begun.
    Closing,
    /// Construction rejected inconsistent limits.
    InvalidLimits,
    /// Construction rejected an already-open transaction.
    OpenTransaction,
    /// Executed callback left a transaction open; rollback was attempted.
    /// No successful outcome is returned, even if the callback returned Ok.
    UnclosedTransaction,
    /// Accepted work lost its connection/result; commit may be unknown.
    ConnectionLost,
    /// A callback panicked; do not infer rollback or retry its external effects.
    Panicked,
    /// Accepted callback was not run because an earlier callback poisoned SQL state.
    Poisoned,
    /// Drain/close failed or its outcome could not be verified; failure is cached.
    CloseFailed {
        /// Closed SQLite codes when close produced a concrete SQLite failure.
        diagnostic: Option<Diagnostic>,
    },
    /// Executed callback failed, retaining typed application error and safe metrics.
    Work {
        /// Original error for typed handling; never automatically formatted/logged.
        source: Box<E>,
        /// Closed timing and SQLite codes without SQL, parameters or secrets.
        diagnostic: Diagnostic,
    },
}

impl<E> fmt::Display for CallError<E> {
    /// Format a closed message only; application/SQLite strings may contain secrets.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Overloaded => "database queue occupied",
            Self::BytesFull => "database byte budget occupied",
            Self::PayloadTooLarge => "database payload exceeds limit",
            Self::Closing => "database closing",
            Self::InvalidLimits => "invalid database limits",
            Self::OpenTransaction => "database connection has an open transaction",
            Self::UnclosedTransaction => "database callback left a transaction open",
            Self::ConnectionLost => "database result unavailable; outcome unknown",
            Self::Panicked => "database callback panicked; outcome unknown",
            Self::Poisoned => "database poisoned; callback not executed",
            Self::CloseFailed { .. } => "database close failed or unverified",
            Self::Work { .. } => "database operation failed",
        })
    }
}

impl<E: StdError + 'static> StdError for CallError<E> {
    /// Typed source is inspectable explicitly; the default message remains closed.
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Work { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

/// Hidden connection and shared admission/drain state; clones cannot bypass limits.
struct Inner {
    /// Only this crate can submit directly to the library's unbounded channel.
    connection: tokio_rusqlite::Connection,
    /// Count credits retained by queued/executing SQL closures.
    jobs: Arc<Semaphore>,
    /// Byte credits retained by queued/executing SQL closures.
    bytes: Arc<Semaphore>,
    /// Fixed validated maximum declared payload of one call.
    job_bytes: usize,
    /// Fixed aggregate limits used by the nonblocking observation snapshot.
    limits: Limits,
    /// Read guards pin accepted work; close obtains exclusive drain authority.
    lifecycle: Arc<RwLock<()>>,
    /// Once set, no new application call is accepted, across all clones.
    closing: AtomicBool,
    /// Stops already accepted callbacks after an uncertain/unrecoverable failure.
    poisoned: AtomicBool,
    /// A close task's retained outcome; it is independent of waiting callers.
    close_state: watch::Sender<CloseState>,
    /// Optional cooperative cross-process lock; retained by accepted SQL jobs.
    lease: Option<Arc<WriterLease>>,
}

/// Cloneable bounded handle to one SQLite connection, not a connection pool.
#[derive(Clone)]
pub struct Database(Arc<Inner>);

/// Nonblocking admission snapshot; counts include queued and executing jobs.
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    /// Currently accepted operations, including one executing callback.
    pub accepted_jobs: usize,
    /// Retained byte credits belonging to accepted operations.
    pub accepted_bytes: usize,
    /// Admission was sealed for drain or failure.
    pub closing: bool,
    /// An uncertain/unrecoverable callback failure poisoned this connection.
    pub poisoned: bool,
}

/// Shared close progression, retaining one truthful terminal close outcome.
#[derive(Clone)]
enum CloseState {
    /// No close request has been accepted.
    Open,
    /// One detached task is draining and closing; others subscribe to its result.
    Closing,
    /// SQL drained and a live library worker acknowledged closure successfully.
    Closed,
    /// SQL/worker/close failure, never replaced by an apparent later success.
    Failed(Option<Diagnostic>),
}

impl Database {
    /// Observe bounded admission without waiting on SQL or application metadata.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            accepted_jobs: self.0.limits.jobs - self.0.jobs.available_permits(),
            accepted_bytes: self.0.limits.bytes - self.0.bytes.available_permits(),
            closing: self.0.closing.load(Ordering::Acquire),
            poisoned: self.0.poisoned.load(Ordering::Acquire),
        }
    }
    /// Adopt `connection` without changing SQLite settings or running migrations.
    ///
    /// `lease` must exclude other cooperating processes writing this same database;
    /// `None` is intended for isolated in-memory tests or read-only connections.
    /// The caller configures WAL, durability, foreign keys and bounded busy wait
    /// before adoption. An open transaction is refused. No raw handle is exposed.
    pub fn from_connection(
        connection: Connection,
        limits: Limits,
        lease: Option<WriterLease>,
    ) -> Result<Self, CallError<std::convert::Infallible>> {
        let limits = limits.validate()?;
        if !connection.is_autocommit() {
            return Err(CallError::OpenTransaction);
        }
        Ok(Self(Arc::new(Inner {
            connection: tokio_rusqlite::Connection::from(connection),
            jobs: Arc::new(Semaphore::new(limits.jobs)),
            bytes: Arc::new(Semaphore::new(limits.bytes)),
            job_bytes: limits.job_bytes,
            limits,
            lifecycle: Arc::new(RwLock::new(())),
            closing: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            close_state: watch::channel(CloseState::Open).0,
            lease: lease.map(Arc::new),
        })))
    }

    /// Open an existing WAL-aware read-only database; never create or migrate it.
    ///
    /// Call on a blocking thread when filesystem latency matters. `busy_wait`
    /// bounds SQLite lock waiting, not arbitrary callback CPU or filesystem I/O.
    /// Both READ_ONLY and query_only are enforced; immutable mode is never used.
    pub fn reader(
        path: &Path,
        busy_wait: Duration,
        limits: Limits,
    ) -> Result<Self, CallError<rusqlite::Error>> {
        let connection = reader_connection(path, busy_wait).map_err(|source| CallError::Work {
            diagnostic: diagnostic(Operation::Read, Duration::ZERO, Duration::ZERO, &source),
            source: Box::new(source),
        })?;
        Self::from_connection(connection, limits, None).map_err(widen)
    }

    /// Execute one local SQL-only closure with bounded, nonwaiting admission.
    ///
    /// `retained_bytes` is the full memory retained by the captured request,
    /// including decoded strings/collections, and must be at least one. Callers
    /// must validate this weight before constructing untrusted payloads. This is
    /// not an allocator-level bound on arbitrary trusted callback allocations.
    /// Accepted work runs once even if this future is dropped. Permits and the
    /// writer lease live in the SQL closure, not the cancelled caller future.
    /// No generic automatic retry occurs. `ConnectionLost`/`Panicked` are unknown
    /// outcomes, never proof of rollback; resolve an application receipt instead.
    pub async fn call<F, R, E>(
        &self,
        operation: Operation,
        retained_bytes: usize,
        work: F,
    ) -> Result<R, CallError<E>>
    where
        F: FnOnce(&mut Connection) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: StdError + Send + 'static,
    {
        let inner = &self.0;
        if retained_bytes == 0 || retained_bytes > inner.job_bytes {
            return Err(CallError::PayloadTooLarge);
        }
        if inner.closing.load(Ordering::Acquire) {
            return Err(CallError::Closing);
        }
        let lifecycle = inner
            .lifecycle
            .clone()
            .try_read_owned()
            .map_err(|_| CallError::Closing)?;
        if inner.closing.load(Ordering::Acquire) {
            return Err(CallError::Closing);
        }
        let jobs = inner
            .jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| CallError::Overloaded)?;
        let bytes = inner
            .bytes
            .clone()
            .try_acquire_many_owned(retained_bytes as u32)
            .map_err(|_| CallError::BytesFull)?;
        let connection = inner.connection.clone();
        let lease = inner.lease.clone();
        let state = self.0.clone();
        let admitted = Instant::now();
        let failed_state = state.clone();
        // call_raw enqueues synchronously on its first poll, before awaiting.
        // No helper task can reorder admission or release credits prematurely.
        connection
            .call_raw(move |conn| {
                let _lifetime = (jobs, bytes, lifecycle, lease);
                if state.poisoned.load(Ordering::Acquire) {
                    return Err(CallError::Poisoned);
                }
                let queue_wait = admitted.elapsed();
                let started = Instant::now();
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(conn)));
                if !conn.is_autocommit() {
                    if conn.execute_batch("ROLLBACK").is_err() || !conn.is_autocommit() {
                        state.poisoned.store(true, Ordering::Release);
                        state.closing.store(true, Ordering::Release);
                    }
                    if outcome.is_err() {
                        state.poisoned.store(true, Ordering::Release);
                        state.closing.store(true, Ordering::Release);
                        return Err(CallError::Panicked);
                    }
                    return Err(CallError::UnclosedTransaction);
                }
                match outcome {
                    Ok(output) => output.map_err(|source| CallError::Work {
                        diagnostic: diagnostic(operation, queue_wait, started.elapsed(), &source),
                        source: Box::new(source),
                    }),
                    Err(_) => {
                        state.poisoned.store(true, Ordering::Release);
                        state.closing.store(true, Ordering::Release);
                        Err(CallError::Panicked)
                    }
                }
            })
            .await
            .map_err(|_| {
                failed_state.poisoned.store(true, Ordering::Release);
                failed_state.closing.store(true, Ordering::Release);
                CallError::ConnectionLost
            })?
    }

    /// Stop admission across all clones, drain accepted SQL, then close connection.
    ///
    /// Dropping the close future leaves admission stopped; a later call can finish
    /// draining. No running SQL is interrupted or presumed rolled back. A callback
    /// must be finite; a caller timeout cannot cancel a library-owned SQL closure.
    /// SQLite close errors propagate as typed failures, never a successful drain.
    pub async fn close(&self) -> Result<(), CallError<rusqlite::Error>> {
        self.0.closing.store(true, Ordering::Release);
        let mut outcome = self.0.close_state.subscribe();
        let start = self.0.close_state.send_if_modified(|state| {
            if matches!(state, CloseState::Open) {
                *state = CloseState::Closing;
                true
            } else {
                false
            }
        });
        if start {
            let inner = self.0.clone();
            tokio::spawn(async move {
                let _drain = inner.lifecycle.write().await;
                // The library treats a dead sender as successful close; require
                // a live worker observation before trusting its close response.
                let live = inner.connection.call_raw(|conn| conn.is_autocommit()).await;
                let result = if matches!(live, Ok(true)) {
                    inner.connection.clone().close().await
                } else {
                    inner.close_state.send_replace(CloseState::Failed(None));
                    return;
                };
                let terminal = match result {
                    Ok(()) if !inner.poisoned.load(Ordering::Acquire) => CloseState::Closed,
                    Err(tokio_rusqlite::Error::Close((_, source)))
                    | Err(tokio_rusqlite::Error::Error(source)) => {
                        CloseState::Failed(Some(diagnostic(
                            Operation::Maintenance,
                            Duration::ZERO,
                            Duration::ZERO,
                            &source,
                        )))
                    }
                    _ => CloseState::Failed(None),
                };
                inner.close_state.send_replace(terminal);
            });
        }
        loop {
            match outcome.borrow_and_update().clone() {
                CloseState::Closed => return Ok(()),
                CloseState::Failed(diagnostic) => {
                    return Err(CallError::CloseFailed { diagnostic });
                }
                CloseState::Open | CloseState::Closing => {}
            }
            outcome
                .changed()
                .await
                .map_err(|_| CallError::ConnectionLost)?;
        }
    }
}

/// Convert a construction-only infallible error into any typed callback error.
fn widen<E>(error: CallError<std::convert::Infallible>) -> CallError<E> {
    match error {
        CallError::InvalidLimits => CallError::InvalidLimits,
        CallError::OpenTransaction => CallError::OpenTransaction,
        CallError::Work { source, .. } => match *source {},
        _ => CallError::ConnectionLost,
    }
}

/// Record only bounded durations and numeric SQLite codes from the error chain.
fn diagnostic(
    operation: Operation,
    queue: Duration,
    execution: Duration,
    error: &(dyn StdError + 'static),
) -> Diagnostic {
    let mut current = Some(error);
    let mut extended = None;
    // Error chains are application supplied: bound traversal, even if cyclic.
    for _ in 0..16 {
        let Some(cause) = current else {
            break;
        };
        if let Some(rusqlite::Error::SqliteFailure(code, _)) =
            cause.downcast_ref::<rusqlite::Error>()
        {
            extended = Some(code.extended_code);
            break;
        }
        current = cause.source();
    }
    Diagnostic {
        operation,
        queue_ms: queue.as_millis().min(u128::from(u64::MAX)) as u64,
        execution_ms: execution.as_millis().min(u128::from(u64::MAX)) as u64,
        sqlite_code: extended.map(|code| code & 0xff),
        sqlite_extended_code: extended,
        attempts: 1,
    }
}

/// Run one SQL-only mutation atomically, with SQLite rolling back on early error.
///
/// The closure receives an IMMEDIATE transaction, cannot await, and must not
/// execute external effects. A commit error is returned without replay. Statement
/// and commit failure do not by themselves prove what an external observer saw.
pub fn transaction<R>(
    connection: &mut Connection,
    work: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<R>,
) -> rusqlite::Result<R> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let output = work(&tx)?;
    tx.commit()?;
    Ok(output)
}
