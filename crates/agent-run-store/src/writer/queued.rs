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
        Arc, Mutex,
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

/// Accepted/pending producer identities protecting receipt watermarks from pruning.
type Producers = Arc<Mutex<BTreeMap<String, usize>>>;

/// Registration remains alive through executed SQL, even when the caller cancels.
struct ProducerGuard {
    /// Registry shared with the sole receipt-retention callback.
    producers: Producers,
    /// Canonical producer identity; never contains credentials or SQL.
    key: String,
}

impl Drop for ProducerGuard {
    /// Release one outstanding registration after the SQL callback is discarded/done.
    fn drop(&mut self) {
        let mut producers = self.producers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = producers.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                producers.remove(&self.key);
            }
        }
    }
}

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
        })
    }
}

impl QueuedWriter {
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
            .map_err(|_| NotEnqueuedReason::QueueFull)?;
        *producers.entry(key.clone()).or_default() += 1;
        Ok(ProducerGuard {
            producers: self.producers.clone(),
            key,
        })
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
                    Some(producers.lock().unwrap_or_else(|e| e.into_inner()))
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
        match self
            .execute(weight, Some(guard), false, move |writer| {
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
        match self
            .execute(weight, Some(guard), prune, move |writer| {
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
