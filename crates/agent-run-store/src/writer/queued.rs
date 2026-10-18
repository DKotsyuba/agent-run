//! Thin application adapter: typed operations on the library-owned connection.
//!
//! Scheduling, bounded admission and cancellation lifetime are sqlite-access's
//! responsibility. This module retains agent-run authority/receipts and gives
//! existing SQL methods a temporary borrow of that exact connection.

use super::{Faults, OnlineWriterFence, StoreWriter};
use crate::{
    Store,
    ops::{LocalEnvelope, MaintenanceOp, OpKindRef, WriteEnvelope},
};
use agent_run_domain::{
    Error, Result,
    storage::{
        MaintenanceOutcome, MonoDeadline, NotEnqueuedReason, OpStatus, OperationId, PayloadDigest,
        RejectCode, RejectStage, StorageRejection, SubmitResult, UnknownCause, WriteAuthority,
    },
};
use sqlite_access::{CallError, ConnectionRef, Database, Limits, Operation};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

/// Mutable application metadata touched only inside serialized SQL callbacks.
struct State {
    /// Canonical application home attached to borrowed Store views.
    home: PathBuf,
    /// Application-proven sole writer authority, retained by accepted callbacks.
    fence: Option<Arc<OnlineWriterFence>>,
    /// Epoch generated under the fenced initialization transaction.
    epoch: u64,
    /// Uncertain connection/commit state; never erased by an apparent success.
    degraded: Option<crate::ops::Degradation>,
    /// Existing test-only SQL fault switches, moved into each borrowed view.
    faults: Faults,
}

/// Weak registrations of accepted/pending producers, collected at admission and pruning.
/// The registry never owns a live token: discarded closures release protection without locks.
type Producers = Arc<Mutex<BTreeMap<String, Weak<()>>>>;

/// Shared registration retained through actual SQL, even if its caller cancels.
/// Last-owner destruction is lock-free; a later admission/prune collects the dead key.
type ProducerGuard = Arc<()>;

/// Cloneable application writer over one bounded sqlite-access connection.
#[derive(Clone)]
pub struct QueuedWriter {
    /// Hidden library connection; there is no direct-write fallback.
    database: Database,
    /// Mutable application metadata, accessed only on the SQL worker thread.
    state: Arc<Mutex<State>>,
    /// Producer registrations, bounded by the number of accepted/pending callers.
    producers: Producers,
    /// Immutable epoch, available without blocking an async runtime on SQL.
    epoch: u64,
    /// Nonblocking readiness mirror of application-level degradation.
    degraded: Arc<AtomicBool>,
    /// Bounded nonblocking diagnostics, separate from serialized SQL metadata.
    progress: Arc<Mutex<OperationProgress>>,
}

/// At most one current and one finished callback; no unbounded metric labels.
#[derive(Default)]
struct OperationProgress {
    /// Current typed callback and its monotonic start.
    current: Option<(agent_run_domain::storage::WriterTiming, Instant)>,
    /// Finished callback timing, never an inferred commit result.
    last: Option<agent_run_domain::storage::WriterTiming>,
}

/// Restore only disposable telemetry after poison; no business/queue state is
/// reset. Missing observations remain unknown and never certify readiness.
fn progress_lock(
    progress: &Mutex<OperationProgress>,
) -> std::sync::MutexGuard<'_, OperationProgress> {
    progress.lock().unwrap_or_else(|poison| {
        let mut state = poison.into_inner();
        *state = OperationProgress::default();
        progress.clear_poison();
        state
    })
}

/// Callback-owned observation survives caller cancellation and clears on panic.
struct OperationObservation {
    /// Shared small state, never held across SQL or await.
    progress: Arc<Mutex<OperationProgress>>,
}
impl Drop for OperationObservation {
    /// Finish timing only when the actual callback leaves, including unwind.
    fn drop(&mut self) {
        let mut progress = progress_lock(&self.progress);
        if let Some((mut timing, started)) = progress.current.take() {
            timing.execution_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            progress.last = Some(timing);
        }
    }
}

impl StoreWriter<'static> {
    /// Move the existing fenced connection into tokio-rusqlite, without reopening it.
    ///
    /// Existing SQL methods receive ConnectionRef::Borrowed during callbacks;
    /// they cannot independently commit the outer mutation/receipt transaction.
    /// Construction errors retain the application fence until the consumed writer
    /// is dropped. No migration, native action or automatic SQL replay occurs.
    pub fn into_queue(self, limits: Limits) -> Result<QueuedWriter> {
        let Store { conn, home, .. } = self.store;
        let ConnectionRef::Owned(connection) = conn else {
            return Err(Error::Runtime(
                "writer adoption requires an owned connection".into(),
            ));
        };
        let epoch = self.epoch;
        let degraded = Arc::new(AtomicBool::new(self.degraded.is_some()));
        let database = Database::from_connection(connection, limits, None)
            .map_err(|_| Error::Runtime("writer queue construction refused".into()))?;
        Ok(QueuedWriter {
            database,
            state: Arc::new(Mutex::new(State {
                home,
                fence: Some(self._fence),
                epoch,
                degraded: self.degraded,
                faults: self.faults,
            })),
            producers: Arc::default(),
            epoch,
            degraded,
            progress: Arc::default(),
        })
    }
}

impl QueuedWriter {
    /// Observe one current/last typed operation without waiting for SQL.
    /// Current execution time is monotonic; history is bounded to one callback.
    pub fn timing(
        &self,
    ) -> (
        Option<agent_run_domain::storage::WriterTiming>,
        Option<agent_run_domain::storage::WriterTiming>,
    ) {
        let progress = progress_lock(&self.progress);
        let current = progress.current.as_ref().map(|(timing, started)| {
            let mut timing = timing.clone();
            timing.execution_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            timing
        });
        (current, progress.last.clone())
    }

    /// Queue a typed callback with an exact safe identity. The observation is
    /// owned by actual SQL execution, not by this cancellable caller future.
    async fn execute_observed<R: Send + 'static>(
        &self,
        weight: usize,
        producer: ProducerGuard,
        prune: bool,
        identity: (OpKindRef, OperationId),
        work: impl FnOnce(&mut StoreWriter<'_>) -> Result<R> + Send + 'static,
    ) -> std::result::Result<R, CallError<Error>> {
        let progress = self.progress.clone();
        let queued = Instant::now();
        self.execute(weight, Some(producer), prune, move |writer| {
            let timing = agent_run_domain::storage::WriterTiming {
                operation_id: identity.1,
                op_kind: identity.0.name().into(),
                queue_wait_ms: queued.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                execution_ms: 0,
            };
            progress_lock(&progress).current = Some((timing, Instant::now()));
            let _observation = OperationObservation { progress };
            work(writer)
        })
        .await
    }

    /// This incarnation's immutable durable writer epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Observe accepted jobs/byte credits without waiting on application SQL.
    pub fn snapshot(&self) -> sqlite_access::Snapshot {
        self.database.snapshot()
    }

    /// Application SQL is unresolved/degraded even if the library thread is healthy.
    pub fn degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    /// Block a disposable fixture callback before any transaction for a controlled test.
    /// Production builds expose no barrier. The entered signal proves the callback
    /// is executing; a bounded release wait prevents abandoned tests hanging forever.
    #[cfg(feature = "test-fixtures")]
    pub async fn fixture_barrier(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        self.execute(1024, None, false, move |_| {
            let _ = entered.send(());
            release
                .recv_timeout(std::time::Duration::from_secs(10))
                .map_err(|_| Error::Runtime("fixture barrier release unavailable".into()))?;
            Ok(())
        })
        .await
        .map_err(|_| Error::Runtime("fixture barrier failed".into()))
    }

    /// Holds the actual receipt-pruning admission gate on the SQL worker until released.
    /// Fixture-only, with a finite escape; exposes neither raw SQL nor a production RPC.
    #[cfg(feature = "test-fixtures")]
    pub async fn fixture_retention_barrier(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        self.execute(1024, None, true, move |_| {
            let _ = entered.send(());
            release
                .recv_timeout(std::time::Duration::from_secs(10))
                .map_err(|_| Error::Runtime("fixture retention release unavailable".into()))?;
            Ok(())
        })
        .await
        .map_err(|_| Error::Runtime("fixture retention barrier failed".into()))
    }

    /// Advance a disposable fixture's quota revision on the real authorized connection.
    /// This test-only race seam opens no independent writer and is not an RPC op.
    #[cfg(feature = "test-fixtures")]
    pub async fn fixture_advance_quota_revision(&self) -> Result<()> {
        self.execute(1024, None, false, |writer| {
            let tx = super::WriteTx::begin(&writer.store.conn)?;
            Store::advance_quota_capacity_revision(&tx)?;
            tx.commit()
        })
        .await
        .map_err(|_| Error::Runtime("fixture revision advance failed".into()))
    }

    /// Register an operation before queue admission, refusing during actual pruning.
    ///
    /// try_lock avoids blocking the async caller while pruning executes SQL. The
    /// pruner holds this same gate through its transaction, so a new producer
    /// cannot slip between protected-key capture and watermark deletion.
    fn protect(&self, key: String) -> std::result::Result<ProducerGuard, NotEnqueuedReason> {
        let mut producers = self
            .producers
            .try_lock()
            .map_err(|_| NotEnqueuedReason::RetentionGate)?;
        producers.retain(|_, token| token.strong_count() != 0);
        let token = producers
            .get(&key)
            .and_then(Weak::upgrade)
            .unwrap_or_default();
        producers.insert(key, Arc::downgrade(&token));
        Ok(token)
    }

    /// Borrow the library connection for one application callback, retaining metadata.
    ///
    /// Weight is bounded retained request memory; SQL errors and uncertainty stay
    /// typed. The callback receives no independent connection or wire-controlled SQL.
    async fn execute<R: Send + 'static>(
        &self,
        weight: usize,
        producer: Option<ProducerGuard>,
        prune: bool,
        work: impl FnOnce(&mut StoreWriter<'_>) -> Result<R> + Send + 'static,
    ) -> std::result::Result<R, CallError<Error>> {
        let state = self.state.clone();
        let producers = self.producers.clone();
        let degraded = self.degraded.clone();
        self.database
            .call(Operation::Write, weight, move |conn| {
                let _producer = producer;
                let mut state = state
                    .lock()
                    .map_err(|_| Error::Runtime("writer metadata poisoned".into()))?;
                // Registration before admission prevents a queue/capture race. Only
                // receipt pruning holds the gate during SQL; producers refuse instead
                // of blocking on it. All other callbacks merely copy bounded keys.
                let prune_gate = if prune {
                    let mut gate = producers.lock().unwrap_or_else(|e| e.into_inner());
                    gate.retain(|_, token| token.strong_count() != 0);
                    Some(gate)
                } else {
                    None
                };
                let live_producers = match &prune_gate {
                    Some(gate) => gate.keys().cloned().collect(),
                    None => Vec::new(),
                };
                let mut writer = StoreWriter {
                    store: Store {
                        conn: ConnectionRef::Borrowed(conn),
                        home: state.home.clone(),
                        attempt: None,
                    },
                    _fence: state
                        .fence
                        .as_ref()
                        .ok_or_else(|| Error::Runtime("writer lease released".into()))?
                        .clone(),
                    epoch: state.epoch,
                    degraded: state.degraded,
                    faults: std::mem::take(&mut state.faults),
                    live_producers,
                };
                let output = work(&mut writer);
                state.degraded = writer.degraded;
                degraded.store(state.degraded.is_some(), Ordering::Release);
                state.faults = std::mem::take(&mut writer.faults);
                output
            })
            .await
    }

    /// Apply an already-validated typed remote operation once, with its durable receipt.
    /// Cancellation after enqueue does not cancel SQL or release its producer guard.
    pub async fn submit(
        &self,
        authority: WriteAuthority,
        mut envelope: WriteEnvelope,
    ) -> SubmitResult {
        envelope.strip_decoded();
        let key = envelope.header().producer.key();
        let guard = match self.protect(key) {
            Ok(guard) => guard,
            Err(reason) => return SubmitResult::NotEnqueued(reason),
        };
        let weight = envelope.encoded_len().saturating_add(1024);
        let identity = (envelope.kind(), envelope.header().operation_id.clone());
        match self
            .execute_observed(weight, guard, false, identity, move |writer| {
                if writer.degraded.is_some()
                    && writer.store.conn.is_autocommit()
                    && let Some(outcome) = writer.lookup_digest(
                        &envelope.header().operation_id,
                        &authority,
                        &envelope.header().digest,
                    )?
                {
                    return Ok(outcome);
                }
                if let Some(result) = ready(writer) {
                    return Ok(result);
                }
                Ok(writer.apply_owned(&authority, envelope))
            })
            .await
        {
            Ok(result) => result,
            Err(error) => submit_error(error),
        }
    }

    /// Apply a broker-local typed operation; arbitrary local closures never cross RPC.
    pub async fn local(&self, authority: WriteAuthority, envelope: LocalEnvelope) -> SubmitResult {
        let guard = match self.protect(envelope.header().producer.key()) {
            Ok(guard) => guard,
            Err(reason) => return SubmitResult::NotEnqueued(reason),
        };
        // Local operations retain a decoded graph. Canonical JSON is only the
        // wire-size metric; account a conservative graph/header allowance too.
        let weight = envelope
            .encoded_len()
            .saturating_mul(16)
            .saturating_add(1024);
        let prune = envelope.kind() == OpKindRef::PruneReceipts;
        let identity = (envelope.kind(), envelope.header().operation_id.clone());
        match self
            .execute_observed(weight, guard, prune, identity, move |writer| {
                if writer.degraded.is_some()
                    && writer.store.conn.is_autocommit()
                    && let Some(outcome) = writer.lookup_digest(
                        &envelope.header().operation_id,
                        &authority,
                        &envelope.header().digest,
                    )?
                {
                    return Ok(outcome);
                }
                if let Some(result) = ready(writer) {
                    return Ok(result);
                }
                Ok(writer.apply_local_owned(&authority, envelope))
            })
            .await
        {
            Ok(result) => result,
            Err(error) => submit_error(error),
        }
    }

    /// Resolve durable outcome by original identity and authority; missing is unknown.
    pub async fn status(
        &self,
        authority: WriteAuthority,
        id: OperationId,
        digest: PayloadDigest,
    ) -> OpStatus {
        let epoch = self.epoch;
        self.execute(1024, None, false, move |writer| {
            let result = writer.lookup_digest(&id, &authority, &digest)?;
            Ok(match result {
                Some(SubmitResult::Committed(receipt)) if receipt.digest == digest => {
                    OpStatus::Committed(receipt)
                }
                Some(SubmitResult::Rejected(rejection)) => OpStatus::Rejected(rejection),
                _ => OpStatus::NotFound {
                    writer_epoch: epoch,
                },
            })
        })
        .await
        .unwrap_or(OpStatus::NotFound {
            writer_epoch: epoch,
        })
    }

    /// Run bounded physical maintenance between business operations, never replayed.
    pub async fn maintain(&self, op: MaintenanceOp, deadline: MonoDeadline) -> MaintenanceOutcome {
        self.execute(1024, None, false, move |writer| {
            Ok(writer.maintain(op, &deadline))
        })
        .await
        .unwrap_or(MaintenanceOutcome::Interrupted)
    }

    /// Drain accepted work and close SQL; keep failure visible and never infer success.
    pub async fn close(&self) -> Result<()> {
        self.database
            .close()
            .await
            .map_err(|_| Error::Runtime("writer close failed or unverified".into()))?;
        self.state
            .lock()
            .map_err(|_| Error::Runtime("writer metadata poisoned".into()))?
            .fence
            .take();
        Ok(())
    }
}

/// Recover only a known failure to acquire SQL, under unchanged writer authority.
///
/// A bounded same-connection probe verifies autocommit and durable epoch before
/// clearing UnknownWriter. It never repeats the business mutation, native action
/// or an ambiguous commit. Truly uncertain states remain unknown/fail closed.
fn ready(writer: &mut StoreWriter<'_>) -> Option<SubmitResult> {
    use crate::ops::Degradation;
    match writer.degraded {
        None => None,
        Some(Degradation::CommitUncertain) => {
            Some(SubmitResult::Unknown(UnknownCause::CommitUncertain))
        }
        Some(Degradation::RollbackUncertain) => {
            Some(SubmitResult::Unknown(UnknownCause::RollbackUncertain))
        }
        Some(Degradation::UnknownWriter) => {
            if !writer.store.conn.is_autocommit() {
                writer.degraded = Some(Degradation::RollbackUncertain);
                return Some(SubmitResult::Unknown(UnknownCause::RollbackUncertain));
            }
            let started = Instant::now();
            let epoch = writer.epoch;
            let tx = match writer
                .store
                .conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            {
                Ok(tx) => tx,
                Err(source) => return Some(probe_failure(source, RejectStage::Acquire, started)),
            };
            let owned = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM writer_authority WHERE id=1 AND epoch=?1)",
                [epoch as i64],
                |row| row.get::<_, bool>(0),
            );
            // Recovery probe has no mutation/commit ambiguity: verify the fenced
            // epoch under the write lock, then explicitly roll back that empty unit.
            if tx.rollback().is_err() || !writer.store.conn.is_autocommit() {
                writer.degraded = Some(Degradation::RollbackUncertain);
                return Some(SubmitResult::Unknown(UnknownCause::RollbackUncertain));
            }
            match owned {
                Ok(true) => {
                    writer.degraded = None;
                    None
                }
                Ok(false) => Some(SubmitResult::Rejected(StorageRejection::new(
                    RejectCode::Conflict,
                    RejectStage::Authorization,
                    "writer epoch changed",
                ))),
                Err(source) => Some(probe_failure(source, RejectStage::Statement, started)),
            }
        }
    }
}

/// Preserve phase/codes of a failed empty recovery probe; retry only BUSY acquire.
fn probe_failure(source: rusqlite::Error, phase: RejectStage, started: Instant) -> SubmitResult {
    let retryable = phase == RejectStage::Acquire
        && source.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy);
    let error = Error::Persistence {
        writer_identity: None,
        operation: agent_run_domain::error::PersistenceOperation::WriterApply,
        elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        attempts: 1,
        phase: Some(phase),
        source: Box::new(Error::Sql(source)),
    };
    let mut refusal = StorageRejection::from_error(&error, phase);
    if retryable {
        refusal.code = RejectCode::StorageUnavailable;
    }
    SubmitResult::Rejected(refusal)
}

/// Translate queue refusal versus uncertain accepted result without exposing SQL.
fn submit_error(error: CallError<Error>) -> SubmitResult {
    match error {
        CallError::Overloaded => SubmitResult::NotEnqueued(NotEnqueuedReason::QueueFull),
        CallError::BytesFull => SubmitResult::NotEnqueued(NotEnqueuedReason::BytesFull),
        CallError::Closing => SubmitResult::NotEnqueued(NotEnqueuedReason::Draining),
        CallError::PayloadTooLarge => SubmitResult::Rejected(StorageRejection::new(
            RejectCode::PayloadTooLarge,
            RejectStage::Gate,
            "writer retained payload exceeds limit",
        )),
        CallError::Work { source, .. } => SubmitResult::Rejected(StorageRejection::from_error(
            &source,
            RejectStage::Statement,
        )),
        _ => SubmitResult::Unknown(UnknownCause::WriterStopped),
    }
}

#[cfg(test)]
mod tests {
    //! Real SQL-worker interleavings, isolated under each test's private home.
    use super::*;
    use std::{sync::mpsc, time::Duration};

    /// Initializes schema32 and adopts its fenced writer with the selected queue bounds.
    fn queue(limits: Limits) -> (tempfile::TempDir, QueuedWriter) {
        let home = tempfile::tempdir().unwrap();
        crate::fixture::initialize(home.path()).unwrap();
        let writer =
            StoreWriter::open(super::super::WriterLease::acquire_online(home.path()).unwrap())
                .unwrap();
        (home, writer.into_queue(limits).unwrap())
    }

    /// Builds one same-boot broker retention envelope; ordinal increases within this fixture.
    fn prune_envelope(ordinal: u64) -> Result<LocalEnvelope> {
        let (boot_id, now_ns) = agent_run_platform::monotonic::now()?;
        LocalEnvelope::new(
            OperationId::new(&boot_id, now_ns),
            agent_run_domain::storage::ProducerId {
                kind: agent_run_domain::storage::ProducerKind::Broker,
                scope: Some("retention".into()),
                instance: "fixture".into(),
            },
            ordinal,
            MonoDeadline {
                boot_id: boot_id.clone(),
                ns: now_ns + 5_000_000_000,
            },
            crate::ops::LocalOp::from(crate::ops::PruneReceipts {
                at: agent_run_domain::domain::now(),
                boot_id,
                now_ns,
            }),
        )
    }

    /// Actual prune-gate contention is distinguishable from a full SQL queue.
    #[tokio::test]
    async fn retention_gate_has_its_own_refusal() {
        let (_home, queue) = queue(Limits::default());
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = mpsc::channel();
        let owner = queue.clone();
        let prune = tokio::spawn(async move {
            owner
                .execute(1, None, true, move |_| {
                    entered.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
                .await
                .unwrap();
        });
        ready.await.unwrap();
        let reason = queue.protect("refused".into()).err().unwrap();
        release.send(()).unwrap();
        prune.await.unwrap();
        queue.close().await.unwrap();
        assert_eq!(serde_json::to_value(reason).unwrap(), "retention_gate");
    }

    /// Shared registrations keep a real stale watermark until the last holder disappears.
    /// Pruning then collects the dead registry entry and deletes the orphan on the same writer.
    #[tokio::test]
    async fn shared_guard_lifetime_protects_actual_watermark() {
        let (_home, queue) = queue(Limits::default());
        queue
            .execute(1, None, false, |writer| {
                writer.store.conn.execute(
                    "INSERT INTO writer_producers VALUES('p-live',1,'0000000000000000',0,NULL,0)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let first = queue.protect("p-live".into()).unwrap();
        let last = queue.protect("p-live".into()).unwrap();
        drop(first);
        let protected = queue
            .local(WriteAuthority::Broker, prune_envelope(1).unwrap())
            .await;
        let retained = queue
            .execute(1, None, false, |writer| {
                Ok(writer.store.conn.query_row(
                    "SELECT count(*) FROM writer_producers WHERE producer_key='p-live'",
                    [],
                    |r| r.get::<_, i64>(0),
                )?)
            })
            .await
            .unwrap();
        drop(last);
        let unprotected = queue
            .local(WriteAuthority::Broker, prune_envelope(2).unwrap())
            .await;
        let remaining = queue
            .execute(1, None, false, |writer| {
                Ok(writer.store.conn.query_row(
                    "SELECT count(*) FROM writer_producers WHERE producer_key='p-live'",
                    [],
                    |r| r.get::<_, i64>(0),
                )?)
            })
            .await
            .unwrap();
        queue.close().await.unwrap();
        assert!(
            matches!(protected,SubmitResult::Committed(ref r) if r.result.as_u64()==Some(0)),
            "{protected:?}"
        );
        assert_eq!(retained, 1);
        assert!(
            matches!(unprotected,SubmitResult::Committed(ref r) if r.result.as_u64()==Some(1)),
            "{unprotected:?}"
        );
        assert_eq!(remaining, 0);
    }

    /// Real job and byte credits report separate closed refusal causes before effects.
    #[tokio::test]
    async fn actual_queue_and_byte_refusals_remain_distinct() {
        for (jobs, expected) in [(1, "queue_full"), (8, "bytes_full")] {
            let (_home, queue) = queue(Limits {
                jobs,
                bytes: 8,
                job_bytes: 8,
            });
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = mpsc::channel();
            let owner = queue.clone();
            let blocked = tokio::spawn(async move {
                owner
                    .execute(8, None, false, move |_| {
                        entered.send(()).unwrap();
                        wait.recv_timeout(Duration::from_secs(5)).unwrap();
                        Ok(())
                    })
                    .await
                    .unwrap();
            });
            ready.await.unwrap();
            let error = queue
                .execute::<()>(1, None, false, |_| panic!("refused SQL must never execute"))
                .await
                .unwrap_err();
            let outcome = submit_error(error);
            release.send(()).unwrap();
            blocked.await.unwrap();
            queue.close().await.unwrap();
            assert_eq!(serde_json::to_value(outcome).unwrap()["value"], expected);
        }
    }

    /// Rejection and destruction of an unpolled submission cannot block Tokio on SQL pruning.
    /// The release thread waits for the unrelated pulse, with a finite escape for broken Drop.
    #[tokio::test(flavor = "current_thread")]
    async fn rejected_and_cancelled_guards_do_not_block_async_progress() {
        for cancelled in [false, true] {
            let (_home, queue) = queue(Limits {
                jobs: 1,
                bytes: 8,
                job_bytes: 8,
            });
            let guard = queue.protect("protected".into()).unwrap();
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = mpsc::channel();
            let owner = queue.clone();
            let prune = tokio::spawn(async move {
                owner
                    .execute(1, None, true, move |writer| {
                        assert!(writer.live_producers.iter().any(|key| key == "protected"));
                        entered.send(()).unwrap();
                        wait.recv_timeout(Duration::from_secs(5)).unwrap();
                        Ok(())
                    })
                    .await
                    .unwrap();
            });
            ready.await.unwrap();
            let (pulse, observed) = mpsc::channel();
            let released = Arc::new(AtomicBool::new(false));
            let signal = released.clone();
            let release_thread = std::thread::spawn(move || {
                let progressed = observed.recv_timeout(Duration::from_millis(500)).is_ok();
                signal.store(true, Ordering::Release);
                release.send(()).unwrap();
                progressed
            });
            let neighbour = tokio::spawn(async move {
                let _ = pulse.send(());
            });
            let pending = Box::pin(queue.execute(1, Some(guard), false, |_| Ok(())));
            if cancelled {
                drop(pending);
            } else {
                assert!(matches!(pending.await, Err(CallError::Overloaded)));
            }
            let blocked_until_release = released.load(Ordering::Acquire);
            neighbour.await.unwrap();
            prune.await.unwrap();
            let progressed = release_thread.join().unwrap();
            queue.close().await.unwrap();
            assert!(
                !blocked_until_release && progressed,
                "guard destruction blocked unrelated async progress; cancelled={cancelled}"
            );
        }
    }

    /// A full watermark scan must not discard a cheap selected receipt page.
    /// The SQL hook spends the actual production200ms budget mid-SELECT on the owned writer.
    #[tokio::test]
    async fn rust_writer_keeps_receipt_progress_before_watermark_expiry() {
        let (_home, queue) = queue(Limits::default());
        let result = queue.execute(1, None, true, |writer| {
            let conn = &writer.store.conn;
            conn.execute_batch("BEGIN IMMEDIATE; WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO writer_producers SELECT printf('recent-%06d',x),1,'0000000000000000',0,'scoped',0 FROM n; COMMIT;")?;
            conn.execute("INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) VALUES(?1,?2,'fixture','expired',1,'broker',?3,NULL,'committed','{}',1,0)", rusqlite::params![OperationId::new("old-boot",1).as_str(), "d".repeat(64), "f".repeat(64)])?;
            crate::ops::apply::WATERMARK_READ_PAUSE.with(|pause| *pause.borrow_mut() = Some(Box::new(|| std::thread::sleep(Duration::from_millis(210)))));
            let first = writer.apply_local_owned(&WriteAuthority::Broker,prune_envelope(1)?);
            let remaining: i64 = writer.store.conn.query_row("SELECT count(*) FROM operation_receipts WHERE producer_key='expired'",[],|r|r.get(0))?;
            let scoped: i64 = writer.store.conn.query_row("SELECT count(*) FROM writer_producers WHERE execution_id='scoped'",[],|r|r.get(0))?;
            let second = writer.apply_local_owned(&WriteAuthority::Broker,prune_envelope(2)?);
            crate::ops::apply::WATERMARK_READ_PAUSE.with(|pause| pause.borrow_mut().take());
            let autocommit = writer.store.conn.is_autocommit();
            let sum: i64 = writer.store.conn.query_row("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT SUM(x) FROM n",[],|r|r.get(0))?;
            Ok((first,second,remaining,scoped,autocommit,sum))
        }).await;
        queue.close().await.unwrap();
        let (first, second, remaining, scoped, autocommit, sum) = result.unwrap();
        assert!(
            matches!(first, SubmitResult::Committed(ref r) if r.result.as_u64().is_some_and(|n| n>0)),
            "cheap receipt progress lost: {first:?}"
        );
        assert_eq!(remaining, 0);
        assert_eq!(scoped, 100000);
        assert!(
            matches!(second, SubmitResult::Rejected(ref r) if r.persistence.as_ref().and_then(|d|d.sqlite_primary)==Some(9)),
            "watermark-only expiry must remain a typed SQL failure: {second:?}"
        );
        assert!(autocommit);
        assert_eq!(sum, 50_005_000, "read hook cleared before follow-on work");
    }

    /// A non-budget DELETE failure remains a rolled-back SQL refusal; removing the
    /// owned fault permits the next real pruning transaction on the same writer.
    #[tokio::test]
    async fn receipt_sql_failure_rolls_back_and_writer_recovers() {
        let (_home, queue) = queue(Limits::default());
        let observations = queue.execute(1,None,true,|writer| {
            writer.store.conn.execute("INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) VALUES(?1,?2,'fixture','expired',1,'broker',?3,NULL,'committed','{}',1,0)",rusqlite::params![OperationId::new("old-boot",1).as_str(),"d".repeat(64),"f".repeat(64)])?;
            writer.store.conn.execute_batch("CREATE TEMP TRIGGER fail_receipt_prune BEFORE DELETE ON operation_receipts BEGIN SELECT RAISE(ABORT,'fixture'); END;")?;
            let failed = writer.apply_local_owned(&WriteAuthority::Broker,prune_envelope(1)?);
            let retained: i64 = writer.store.conn.query_row("SELECT count(*) FROM operation_receipts WHERE producer_key='expired'",[],|r|r.get(0))?;
            let settled = writer.store.conn.is_autocommit();
            writer.store.conn.execute_batch("DROP TRIGGER fail_receipt_prune;")?;
            let recovered = writer.apply_local_owned(&WriteAuthority::Broker,prune_envelope(2)?);
            Ok((failed,retained,settled,recovered))
        }).await;
        queue.close().await.unwrap();
        let (failed, retained, settled, recovered) = observations.unwrap();
        assert!(
            matches!(failed,SubmitResult::Rejected(ref r) if r.code==RejectCode::StorageRolledBack && r.persistence.as_ref().and_then(|d|d.sqlite_primary)==Some(19)),
            "{failed:?}"
        );
        assert_eq!(retained, 1);
        assert!(settled);
        assert!(
            matches!(recovered,SubmitResult::Committed(ref r) if r.result.as_u64()==Some(1)),
            "{recovered:?}"
        );
    }
}
