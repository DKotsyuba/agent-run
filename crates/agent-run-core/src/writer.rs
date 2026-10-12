//! Broker facade for the reusable sqlite-access queue and typed Store adapter.
//!
//! The library serializes SQL; producer lanes preserve per-producer order.
//! Receipts and authority remain store-owned. No custom scheduler lives here.

use agent_run_domain::{
    Error, Result,
    storage::{
        MaintenanceOutcome, MonoDeadline, NotEnqueuedReason, OpStatus, OperationId,
        OutcomeCounters, PayloadDigest, SubmitResult, WriteAuthority, WriterHealth,
    },
};
use agent_run_store::{
    ops::{LocalEnvelope, MaintenanceOp, OpKindRef, WriteEnvelope},
    writer::{StoreWriter, WriterLease, queued::QueuedWriter},
};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// Aggregate limits cover accepted jobs including executing SQL.
#[derive(Debug, Clone, PartialEq)]
pub struct WriterConfig {
    /// Maximum accepted jobs; additional requests receive NotEnqueued.
    pub max_jobs: u32,
    /// Total accepted retained payload byte credits.
    pub max_queued_bytes: u64,
    /// Maximum retained payload weight of an individual typed operation.
    pub max_job_bytes: u64,
    /// SQLite busy wait, capped at the existing writer allowance.
    pub writer_busy: std::time::Duration,
}

impl Default for WriterConfig {
    /// Bounded starting point, subject to isolated load qualification.
    fn default() -> Self {
        Self {
            max_jobs: 128,
            max_queued_bytes: 256 * 1024 * 1024,
            max_job_bytes: 64 * 1024 * 1024,
            writer_busy: agent_run_store::writer::WRITER_BUSY,
        }
    }
}

/// Broker-owned lifecycle; surviving handles cannot write after drain.
pub struct WriterService {
    /// Shared handle retained until accepted SQL is drained.
    handle: WriterHandle,
}

/// Typed submission handle; no raw SQLite or arbitrary SQL RPC surface.
#[derive(Clone)]
pub struct WriterHandle {
    /// Adapter over the library-owned connection.
    writer: QueuedWriter,
    /// Replies actually observed by callers, not durable business counters.
    counters: Arc<Mutex<OutcomeCounters>>,
    /// Admission seal independent of evidence writes for existing executions.
    admissions_closed: Arc<AtomicBool>,
}

impl WriterService {
    /// Acquire the writer lease and adopt its existing connection.
    /// Missing/old schema refuses; called before broker accepts clients.
    pub fn start(home: &Path, config: WriterConfig) -> Result<Self> {
        let fence = WriterLease::acquire_online(home)?;
        let mut store = StoreWriter::open(fence)?;
        store.set_busy_wait(config.writer_busy)?;
        let writer = store.into_queue(sqlite_access::Limits {
            jobs: config.max_jobs as usize,
            bytes: usize::try_from(config.max_queued_bytes)
                .map_err(|_| Error::Runtime("writer byte limit invalid".into()))?,
            job_bytes: usize::try_from(config.max_job_bytes)
                .map_err(|_| Error::Runtime("writer job limit invalid".into()))?,
        })?;
        Ok(Self {
            handle: WriterHandle {
                writer,
                counters: Arc::default(),
                admissions_closed: Arc::new(AtomicBool::new(false)),
            },
        })
    }

    /// Clone the typed handle; the connection remains private.
    pub fn handle(&self) -> WriterHandle {
        self.handle.clone()
    }

    /// Seal and drain without cancelling SQL or releasing its lease early.
    /// Returns at the supplied same-boot deadline; a timed-out close keeps draining
    /// in an owned task with the connection and lease retained until acknowledgement.
    /// Timeout never represents successful closure or permission to start another writer.
    pub async fn shutdown(self, deadline: MonoDeadline) -> Result<()> {
        self.handle.admissions_closed.store(true, Ordering::Release);
        let (boot, now) = agent_run_platform::monotonic::now()?;
        let remaining = if deadline.expired(&boot, now) {
            0
        } else {
            deadline.ns - now
        };
        let writer = self.handle.writer;
        let close = tokio::spawn(async move { writer.close().await });
        tokio::time::timeout(std::time::Duration::from_nanos(remaining), close)
            .await
            .map_err(|_| {
                Error::Runtime("writer drain deadline exceeded; closure unconfirmed".into())
            })?
            .map_err(|_| Error::Runtime("writer close task failed; closure unconfirmed".into()))?
    }
}

impl WriterHandle {
    /// Test-only finite callback barrier before SQL; exposes no production RPC.
    #[cfg(feature = "test-fixtures")]
    pub async fn fixture_barrier(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        self.writer.fixture_barrier(entered, release).await
    }

    /// Test-only revision-race control using the actual queue/fenced connection.
    #[cfg(feature = "test-fixtures")]
    pub async fn fixture_advance_quota_revision(&self) -> Result<()> {
        self.writer.fixture_advance_quota_revision().await
    }
    /// Count an observed reply without interpreting unknown as success.
    fn count(&self, result: SubmitResult) -> SubmitResult {
        let mut counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        match &result {
            SubmitResult::Committed(_) => counters.committed += 1,
            SubmitResult::Rejected(_) => counters.rejected += 1,
            SubmitResult::NotEnqueued(_) => counters.not_enqueued += 1,
            SubmitResult::Unknown(_) => counters.unknown += 1,
        }
        result
    }

    /// Submit a typed remote unit under broker-authenticated authority.
    pub async fn submit(&self, authority: WriteAuthority, envelope: WriteEnvelope) -> SubmitResult {
        self.count(self.writer.submit(authority, envelope).await)
    }

    /// Submit a typed broker-local unit through the same connection and bounds.
    /// An admission seal refuses new starts but still resolves exact durable replays
    /// and accepts evidence for already-running executions.
    pub async fn local(&self, authority: WriteAuthority, envelope: LocalEnvelope) -> SubmitResult {
        if self.admissions_closed.load(Ordering::Acquire)
            && matches!(
                envelope.kind(),
                OpKindRef::AdmitStart
                    | OpKindRef::AdmitProvider
                    | OpKindRef::AdmitProviderResume
                    | OpKindRef::AdmitPool
                    | OpKindRef::ReplacePoolMember
            )
        {
            let header = envelope.header();
            let result = match self
                .status(authority, &header.operation_id, &header.digest)
                .await
            {
                OpStatus::Committed(receipt) => SubmitResult::Committed(receipt),
                OpStatus::Rejected(rejection) => SubmitResult::Rejected(rejection),
                _ => SubmitResult::NotEnqueued(NotEnqueuedReason::AdmissionsClosed),
            };
            return self.count(result);
        }
        self.count(self.writer.local(authority, envelope).await)
    }

    /// Resolve original identity without rerunning a native action.
    pub async fn status(
        &self,
        authority: WriteAuthority,
        id: &OperationId,
        digest: &PayloadDigest,
    ) -> OpStatus {
        self.writer
            .status(authority, id.clone(), digest.clone())
            .await
    }

    /// Run bounded maintenance between business transactions.
    pub async fn maintain(&self, op: MaintenanceOp, deadline: MonoDeadline) -> MaintenanceOutcome {
        self.writer.maintain(op, deadline).await
    }

    /// Nonblocking aggregate FIFO snapshot; no fabricated class/reservation metrics.
    pub fn health(&self) -> WriterHealth {
        let snapshot = self.writer.snapshot();
        WriterHealth {
            epoch: self.writer.epoch(),
            admissions_open: !snapshot.closing
                && !snapshot.poisoned
                && !self.writer.degraded()
                && !self.admissions_closed.load(Ordering::Acquire),
            accepted_jobs: snapshot.accepted_jobs as u32,
            accepted_bytes: snapshot.accepted_bytes as u64,
            poisoned: snapshot.poisoned,
            counters: self
                .counters
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }

    /// Stop new-admission readiness while retaining existing evidence authority.
    pub fn close_admissions(&self, _reason: &'static str) {
        self.admissions_closed.store(true, Ordering::Release);
    }
}
