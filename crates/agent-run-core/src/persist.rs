//! Persistence facade of the processes that are NOT the resident broker.
//!
//! A [`crate::persist::Producer`] submits typed remote operations through one
//! [`crate::storage_client::ProducerLane`] to the broker's private writer socket and decodes the
//! typed receipt. [`crate::persist::Persist`] wraps one producer for a detached execution
//! supervisor: reads go through a read-only [`agent_run_store::writer::StoreReader`] (dereference), every
//! mutation is exactly one typed operation, and journal rows are coalesced into
//! bounded [`agent_run_store::ops::AppendJournal`] batches that are always flushed before any other
//! operation of the same producer, so the transcript order the engine observed is
//! the order the writer applies.
//!
//! No operation here ever replays a native action: a lost acknowledgement is
//! resolved by resubmitting the identical operation under its original identity
//! and deadline (see [`crate::storage_client`]), and an `Unknown` outcome is
//! reported as such, never as success or as proof of non-commit.

use crate::{
    Error, Result,
    domain::{AgentId, Outcome},
    storage_client::{
        Dial, Hello, PendingRepin, PendingWrite, ProducerLane, UnixDial, WireRequest,
    },
};
use agent_run_domain::{
    error::PersistenceOperation,
    storage::{
        MonoDeadline, NotEnqueuedReason, OperationId, PayloadDigest, ProducerId, ProducerKind,
        RejectStage, SubmitResult,
    },
};
use agent_run_platform::{monotonic, process::OwnershipSnapshot, verify::Proof};
use agent_run_store::{
    ops::{self, JournalFragment, RemoteOp, WriteEnvelope, WriteOp},
    writer::StoreReader,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{ops::Deref, path::Path, time::Duration};

/// Budget of an ownership, cleanup, terminal or other critical operation: from
/// the moment the operation reaches the head of its lane until its original
/// monotonic deadline.
pub const CRITICAL_BUDGET: Duration = Duration::from_secs(30);
/// Budget of an ordinary journal flush.
pub const JOURNAL_BUDGET: Duration = Duration::from_secs(30);
/// Budget of the background run-statistics write after a terminal commit.
const STATS_BUDGET: Duration = Duration::from_secs(10);
/// Fragments per [`ops::AppendJournal`] batch (contract section 5a).
const BATCH_FRAGMENTS: usize = 32;
/// Buffered journal bytes that force a flush; well under the 1 MiB event and
/// 2 MiB total bounds of one operation.
const BATCH_BYTES: usize = 512 * 1024;
/// Longest a buffered journal fragment waits before [`Persist::flush_due`] writes it.
pub const JOURNAL_MAX_AGE: Duration = Duration::from_millis(150);

/// Maps a final writer outcome to the typed output or an error.
///
/// `Committed` decodes the receipt's result; `Rejected` becomes the stored
/// domain error with its closed persistence diagnostic; a refusal before
/// acceptance and an unknown outcome become [`Error::Persistence`] with the
/// closed operation label `writer_apply`, so they are never mistaken for success.
///
/// # Errors
/// The mapped failure described above, or a decode error for a receipt whose
/// result does not match `O`.
pub fn into_result<O: DeserializeOwned>(result: SubmitResult) -> Result<O> {
    match result {
        SubmitResult::Committed(receipt) => receipt.output(),
        SubmitResult::Rejected(rejection) => Err(rejection.into_error()),
        SubmitResult::NotEnqueued(reason) => Err(Error::Persistence {
            operation: PersistenceOperation::WriterApply,
            elapsed_ms: 0,
            attempts: 1,
            phase: Some(RejectStage::Gate),
            source: Box::new(Error::WriterFailure(
                agent_run_domain::storage::WriterFailure::NotEnqueued(reason),
            )),
        }),
        SubmitResult::Unknown(cause) => Err(Error::Persistence {
            operation: PersistenceOperation::WriterApply,
            elapsed_ms: 0,
            attempts: 1,
            phase: None,
            source: Box::new(Error::WriterFailure(
                agent_run_domain::storage::WriterFailure::Unknown(cause),
            )),
        }),
    }
}

/// [`into_result`] with the submission's truthful totals.
///
/// A refusal before acceptance that outlived the original deadline reports the
/// writer's last actual admission reason or storage refusal (operation, closed
/// phase, SQLite primary/extended codes) with the real attempt count and elapsed time instead of a generic
/// expiry. An unknown outcome stays unknown (it is never turned into a
/// definite refusal) but carries the same real numbers.
pub fn into_result_totals<O: DeserializeOwned>(
    result: SubmitResult,
    totals: &crate::storage_client::Totals,
) -> Result<O> {
    match result {
        SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
            if totals.last_not_enqueued.is_some() =>
        {
            into_result::<O>(SubmitResult::NotEnqueued(
                totals.last_not_enqueued.expect("checked above"),
            ))
            .map_err(|error| error.persistence_totals(totals.elapsed, totals.attempts))
        }
        SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
            if totals.last_rejection.is_some() =>
        {
            let rejection = totals.last_rejection.clone().expect("checked above");
            Err(rejection
                .into_error()
                .persistence_totals(totals.elapsed, totals.attempts))
        }
        SubmitResult::NotEnqueued(_) | SubmitResult::Unknown(_) => into_result::<O>(result)
            .map_err(|error| error.persistence_totals(totals.elapsed, totals.attempts.max(1))),
        other => into_result(other),
    }
}

/// Whether `result` leaves the operation's durability genuinely unknown.
pub fn is_unknown(result: &SubmitResult) -> bool {
    matches!(result, SubmitResult::Unknown(_))
}

/// One ordered remote producer: a lane plus the producer identity every
/// envelope carries. Cheap to clone; clones share the same lane and ordinals.
#[derive(Clone)]
pub struct Producer {
    /// The ordered, deadline-bound, replay-safe lane.
    lane: ProducerLane,
    /// Identity stamped into every envelope.
    id: ProducerId,
}

impl Producer {
    /// A producer dialing `<home>/writer.sock`.
    pub fn connect(socket: &Path, hello: Hello, id: ProducerId) -> Self {
        Self::with_dial(
            UnixDial {
                path: socket.to_path_buf(),
            },
            hello,
            id,
        )
    }

    /// A producer over an arbitrary dialer (tests and alternate transports).
    pub fn with_dial(dial: impl Dial, hello: Hello, id: ProducerId) -> Self {
        Self {
            lane: ProducerLane::new(dial, hello, 1),
            id,
        }
    }

    /// The producer identity.
    pub fn id(&self) -> &ProducerId {
        &self.id
    }

    /// Re-pins the supervisor attempt claim through an ordered re-hello.
    pub fn repin(&self, attempt: Option<String>) -> PendingRepin {
        self.lane.repin(attempt)
    }

    /// Claims the next ordinal and submits `op`; the returned handle resolves to
    /// the final outcome and may be dropped without withdrawing the operation.
    ///
    /// Admission order is claimed NOW, so two calls reach the writer in call
    /// order. `budget` starts when the operation reaches the head of the lane:
    /// time spent behind earlier operations of this producer is not charged.
    pub fn submit<P>(&self, op: P, budget: Duration) -> PendingWrite
    where
        P: WriteOp + Send + 'static,
        RemoteOp: From<P>,
    {
        self.submit_identified(op, budget, None)
    }

    /// [`Self::submit`] that also reports the operation identity (id and digest)
    /// the moment the envelope is built, so a caller can name the original
    /// operation in a later recovery observation even when its outcome stays
    /// unknown. The hook runs after the envelope was built and before any send;
    /// its error refuses this submission without sending the operation.
    pub fn submit_identified<P>(
        &self,
        op: P,
        budget: Duration,
        identity: Option<IdentityHook>,
    ) -> PendingWrite
    where
        P: WriteOp + Send + 'static,
        RemoteOp: From<P>,
    {
        let producer = self.id.clone();
        self.lane.submit(move |ordinal| {
            let (boot, now) = monotonic::now().map_err(Error::Io)?;
            let operation_id = OperationId::new(&boot, now);
            let deadline = MonoDeadline {
                boot_id: boot,
                ns: now.saturating_add(budget.as_nanos().min(u128::from(u64::MAX)) as u64),
            };
            let envelope = WriteEnvelope::new(
                operation_id.clone(),
                producer.clone(),
                ordinal,
                deadline.clone(),
                RemoteOp::from(op),
            )?;
            let (kind, body) = envelope.to_wire();
            if let Some(identity) = identity {
                identity(&operation_id, &envelope.header().digest)?;
            }
            Ok(WireRequest {
                operation_id,
                digest: envelope.header().digest.clone(),
                producer,
                ordinal,
                op_kind: Some(P::KIND.name().to_owned()),
                kind,
                body: body.to_vec(),
                deadline,
            })
        })
    }

    /// Submits `op` and returns its raw final outcome, for callers that must
    /// distinguish `Unknown` from a definite refusal.
    pub async fn call_raw<P>(&self, op: P, budget: Duration) -> SubmitResult
    where
        P: WriteOp + Send + 'static,
        RemoteOp: From<P>,
    {
        self.submit(op, budget).await
    }

    /// Submits `op` and returns its typed output.
    ///
    /// # Errors
    /// See [`into_result`].
    pub async fn call<P>(&self, op: P, budget: Duration) -> Result<P::Output>
    where
        P: WriteOp + Send + 'static,
        RemoteOp: From<P>,
    {
        let pending = self.submit(op, budget);
        let totals = pending.totals();
        let result = pending.await;
        into_result_totals(result, &totals.snapshot())
    }
}

/// Called with the original operation's identity the moment its envelope is
/// built, before anything is sent. An error prevents sending, so required
/// recovery identity evidence must be durable before the operation can run.
pub type IdentityHook = Box<dyn FnOnce(&OperationId, &PayloadDigest) -> Result<()> + Send>;

/// Appender of one execution's private recovery log.
#[derive(Clone)]
pub struct RecoveryLog {
    /// The execution's own directory.
    directory: std::path::PathBuf,
}

/// The recovery log of `execution` under `home`.
fn recovery_log(home: &Path, execution: &AgentId) -> RecoveryLog {
    RecoveryLog {
        directory: home.join("agents").join(execution.as_str()),
    }
}

impl RecoveryLog {
    /// Appends one JSON line, flushed to stable storage. The file is created
    /// mode 0600 and opened without following links; it must be a regular file
    /// of this user with exactly mode 0600 and no other hard link. The append
    /// uses the held owned directory descriptor, then syncs file and directory;
    /// ownership, permission and I/O failures propagate before further writes.
    fn append(&self, line: &Value) -> Result<()> {
        use std::{
            io::Write as _,
            os::fd::{AsRawFd as _, FromRawFd as _},
            os::unix::fs::{MetadataExt as _, OpenOptionsExt as _},
        };
        let mut bytes = serde_json::to_vec(line)?;
        bytes.push(b'\n');
        crate::fs::private_dir(&self.directory)?;
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(&self.directory)?;
        // SAFETY: geteuid only reads this process's effective user id.
        let uid = unsafe { libc::geteuid() };
        if directory.metadata()?.uid() != uid {
            return Err(Error::Integrity("recovery directory is not owned".into()));
        }
        let name = std::ffi::CString::new(RECOVERY_LOG).expect("fixed file name has no NUL");
        // SAFETY: the held directory descriptor and fixed name remain live;
        // O_NOFOLLOW refuses links and O_NONBLOCK prevents a FIFO from stalling.
        let raw = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_WRONLY
                    | libc::O_CREAT
                    | libc::O_APPEND
                    | libc::O_NOFOLLOW
                    | libc::O_CLOEXEC
                    | libc::O_NONBLOCK,
                0o600 as libc::c_uint,
            )
        };
        if raw < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: this call uniquely owns the new openat descriptor.
        let mut file = unsafe { std::fs::File::from_raw_fd(raw) };
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != uid
            || metadata.nlink() != 1
            || metadata.mode() & 0o777 != 0o600
        {
            return Err(Error::Integrity(
                "recovery log is not a private regular file".into(),
            ));
        }
        file.write_all(&bytes)?;
        file.sync_all()?;
        directory.sync_all()?;
        Ok(())
    }

    /// Durably logs the built operation's identity for local recovery entry
    /// `seq`. Filesystem or validation failures propagate before any send.
    fn identity(&self, seq: u64, operation_id: &OperationId, digest: &PayloadDigest) -> Result<()> {
        self.append(&json!({
            "line": "identity",
            "seq": seq,
            "operation_id": operation_id,
            "digest": digest,
        }))
    }
}

/// Journal rows waiting to be written, in observation order.
#[derive(Default)]
struct Pending {
    /// Fragments not yet submitted.
    fragments: Vec<JournalFragment>,
    /// Compact UTF-8 JSON bytes of [`Self::fragments`], including separators.
    bytes: usize,
    /// When the oldest buffered fragment was buffered.
    since: Option<std::time::Instant>,
}

/// Largest decoded size one `AppendJournal` batch may reach: below the row's
/// 2 MiB decoded bound with room for the envelope and the other fields.
const MAX_BATCH_DECODED: usize = 1536 * 1024;

/// Exact size of one fragment in the operation's canonical `ensure_ascii` wire
/// body (the wire bound's measure; escapes of non-ASCII text are charged as
/// they are written), plus a separator.
fn fragment_wire_bytes(fragment: &JournalFragment) -> usize {
    serde_json::to_value(fragment).map_or(usize::MAX / 4, |value| {
        agent_run_domain::canonical::dumps(&value, true).len() + 1
    })
}

/// Exact encoded size of one fragment inside an operation's fragment list
/// (the operation's decoded-size measure: compact JSON plus a separator), so
/// control characters and escapes are charged as they are written.
fn fragment_bytes(fragment: &JournalFragment) -> usize {
    serde_json::to_vec(fragment).map_or(MAX_BATCH_DECODED, |bytes| bytes.len() + 1)
}

/// The supervisor's storage facade: reads through a read-only handle,
/// mutations as typed operations of one ordered producer.
pub struct Persist {
    /// Ordered remote producer of this execution.
    producer: Producer,
    /// Read-only store handle; `Persist` dereferences to it for every read.
    reader: StoreReader,
    /// Home directory (private spool for oversize messages).
    home: std::path::PathBuf,
    /// The execution this supervisor owns.
    execution: AgentId,
    /// Attempt that journal rows and session records are attributed to.
    attempt: Option<String>,
    /// Coalesced journal rows.
    pending: Pending,
    /// Observed results whose evidence is not yet confirmed durable, oldest first.
    recoveries: Vec<Recovery>,
    /// Next local recovery sequence number.
    recovery_seq: u64,
}

/// The payload of one observed command result, kept until its evidence is
/// confirmed durable (the native effect already happened; this is storage
/// material only and is never a reason to repeat the effect).
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ObservedPayload {
    /// Claimed command.
    command_id: i64,
    /// Recorded result.
    result: Value,
    /// Evidence label.
    strength: String,
    /// Worker-turn effect.
    worker_turn: Option<bool>,
    /// Journal rows of the same atomic operation.
    fragments: Vec<JournalFragment>,
}

/// One unconfirmed observed result.
#[derive(Clone)]
struct Recovery {
    /// Local sequence number naming this entry in the private log.
    seq: u64,
    /// Attempt the observation belongs to.
    attempt: Option<String>,
    /// The payload to re-submit as storage evidence when nothing was sent.
    payload: ObservedPayload,
    /// Identity of the original operation once its envelope was built; `None`
    /// means nothing was ever sent.
    original: Option<(OperationId, PayloadDigest)>,
    /// Final outcome label of the original operation, once known.
    outcome: Option<Value>,
}

/// File name of the private append-only recovery log inside the execution's
/// own directory.
const RECOVERY_LOG: &str = "recovery-observations.jsonl";

impl Deref for Persist {
    /// The read-only store handle every read goes through.
    type Target = StoreReader;

    /// All reads (`get`, `provider_attempt`, `worker_finish_intent`, ...) use the
    /// read-only handle; nothing reached through it can write.
    fn deref(&self) -> &StoreReader {
        &self.reader
    }
}

impl Persist {
    /// A facade for `execution` over `producer`, reading from the store of `home`.
    ///
    /// # Errors
    /// The reader cannot open the existing schema-30 store.
    pub fn open(home: &Path, execution: &AgentId, producer: Producer) -> Result<Self> {
        Ok(Self {
            producer,
            reader: StoreReader::open_with(home, Duration::from_millis(250))?,
            home: home.to_path_buf(),
            execution: execution.clone(),
            attempt: None,
            pending: Pending::default(),
            recoveries: Vec::new(),
            recovery_seq: 0,
        })
    }

    /// The execution this facade writes for.
    pub fn execution(&self) -> &AgentId {
        &self.execution
    }

    /// The underlying producer (clone it to submit from a detached sink).
    pub fn producer(&self) -> &Producer {
        &self.producer
    }

    /// Home directory of the store.
    pub fn home(&self) -> &Path {
        &self.home
    }

    /// Attributes later journal rows and session records to `attempt`.
    pub fn bind_attempt(&mut self, attempt: &str) {
        self.attempt = Some(attempt.to_owned());
    }

    /// The attempt later rows are attributed to.
    pub fn bound_attempt(&self) -> Option<&str> {
        self.attempt.as_deref()
    }

    /// Re-pins the broker-side attempt claim after a failover and binds it here.
    ///
    /// # Errors
    /// The re-hello failed; the lane reconnects with the new claim on the next call.
    pub async fn repin(&mut self, attempt: &str) -> Result<()> {
        self.flush().await?;
        self.producer
            .repin(Some(attempt.to_owned()))
            .await
            .map_err(Error::Io)?;
        self.bind_attempt(attempt);
        Ok(())
    }

    /// Checks that `id` is this facade's own execution.
    fn own(&self, id: &AgentId) -> Result<()> {
        if *id == self.execution {
            Ok(())
        } else {
            Err(Error::Validation(
                "a supervisor persists only its own execution".into(),
            ))
        }
    }

    /// Buffers one fragment; flushes when a batch bound is reached.
    async fn push(&mut self, fragment: JournalFragment) -> Result<()> {
        let size = fragment_bytes(&fragment);
        if size >= MAX_BATCH_DECODED {
            // A row that can never fit one batch is refused now, not buffered
            // where it would block every later flush.
            return Err(Error::Validation(
                "journal row exceeds its size bound".into(),
            ));
        }
        self.pending.bytes += size;
        self.pending.fragments.push(fragment);
        self.pending
            .since
            .get_or_insert_with(std::time::Instant::now);
        if self.pending.fragments.len() >= BATCH_FRAGMENTS || self.pending.bytes >= BATCH_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    /// Writes every buffered journal row now, in observation order.
    ///
    /// # Errors
    /// The writer refused or could not confirm a batch; rows of that batch are
    /// not retried under a new identity (the lane already retried the identical
    /// operation within its original deadline).
    pub async fn flush(&mut self) -> Result<()> {
        while !self.pending.fragments.is_empty() {
            // Cut a batch by exact encoded size (always at least one fragment).
            // Both bounds are honored independently: the operation's decoded
            // payload size and its canonical wire body size.
            let wire_cap = ops::OpKindRef::AppendJournal
                .wire_max()
                .saturating_sub(64 * 1024);
            let mut take = 0;
            let (mut decoded, mut wire) = (0, 0);
            for fragment in &self.pending.fragments {
                let (next_decoded, next_wire) =
                    (fragment_bytes(fragment), fragment_wire_bytes(fragment));
                if take == BATCH_FRAGMENTS
                    || (take > 0
                        && (decoded + next_decoded > MAX_BATCH_DECODED
                            || wire + next_wire > wire_cap))
                {
                    break;
                }
                decoded += next_decoded;
                wire += next_wire;
                take += 1;
            }
            let fragments: Vec<_> = self.pending.fragments.drain(..take).collect();
            if self.pending.fragments.is_empty() {
                self.pending = Pending::default();
            } else {
                self.pending.bytes = self.pending.fragments.iter().map(fragment_bytes).sum();
            }
            let pending = self.producer.submit(
                ops::AppendJournal {
                    execution: self.execution.clone(),
                    fragments: fragments.clone(),
                },
                JOURNAL_BUDGET,
            );
            let totals = pending.totals();
            let outcome = pending.await;
            // A batch that provably never reached the writer (refused before
            // acceptance, or by the gate before anything was built or applied)
            // stays buffered in order; anything else is final for these rows.
            let unsent = match &outcome {
                SubmitResult::NotEnqueued(_) => true,
                SubmitResult::Rejected(rejection) => !rejection.durable,
                _ => false,
            };
            if unsent {
                let mut restored = fragments;
                restored.append(&mut self.pending.fragments);
                self.pending.bytes = restored.iter().map(fragment_bytes).sum();
                self.pending.fragments = restored;
                self.pending
                    .since
                    .get_or_insert_with(std::time::Instant::now);
            }
            into_result_totals::<Vec<i64>>(outcome, &totals.snapshot())?;
        }
        Ok(())
    }

    /// Flushes when the oldest buffered row has waited [`JOURNAL_MAX_AGE`].
    pub async fn flush_due(&mut self) -> Result<()> {
        if self
            .pending
            .since
            .is_some_and(|since| since.elapsed() >= JOURNAL_MAX_AGE)
        {
            self.flush().await?;
        }
        Ok(())
    }

    /// Whether journal rows are waiting to be written.
    pub fn has_pending(&self) -> bool {
        !self.pending.fragments.is_empty()
    }

    /// Buffers one event row attributed to the bound attempt.
    ///
    /// # Errors
    /// A flush triggered by the batch bounds failed.
    pub async fn event(&mut self, id: &AgentId, kind: &str, data: &Value) -> Result<()> {
        self.own(id)?;
        let attempt = self.attempt.clone();
        self.push(JournalFragment::Event {
            kind: kind.to_owned(),
            data: data.clone(),
            attempt,
        })
        .await
    }

    /// Buffers one event row and writes everything buffered, durably.
    pub async fn event_durable(&mut self, id: &AgentId, kind: &str, data: &Value) -> Result<()> {
        self.event(id, kind, data).await?;
        self.flush().await
    }

    /// Buffers transcript text in bounded UTF-8 chunks without altering it.
    /// Bodies above the inline bound are spooled to the private agent
    /// directory first (filesystem work stays off the writer thread).
    pub async fn journal(
        &mut self,
        id: &AgentId,
        role: &str,
        text: &str,
        name: Option<&str>,
        raw_ref: Option<&str>,
    ) -> Result<()> {
        let mut remaining = text;
        while !remaining.is_empty() {
            let mut end = remaining.len().min(16 * 1024);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            self.message_with_error(id, role, &remaining[..end], name, raw_ref, None)
                .await?;
            remaining = &remaining[end..];
        }
        Ok(())
    }

    /// Buffers native tool text in bounded chunks with optional error provenance;
    /// an empty tool result still records evidence.
    pub async fn journal_with_error(
        &mut self,
        id: &AgentId,
        role: &str,
        text: &str,
        name: Option<&str>,
        raw_ref: Option<&str>,
        error: Option<(bool, &str)>,
    ) -> Result<()> {
        let mut remaining = text;
        loop {
            let mut end = remaining.len().min(16 * 1024);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            self.message_with_error(id, role, &remaining[..end], name, raw_ref, error)
                .await?;
            remaining = &remaining[end..];
            if remaining.is_empty() {
                break;
            }
        }
        Ok(())
    }

    /// Buffers one transcript row.
    pub async fn message_with_error(
        &mut self,
        id: &AgentId,
        role: &str,
        text: &str,
        name: Option<&str>,
        raw_ref: Option<&str>,
        error: Option<(bool, &str)>,
    ) -> Result<()> {
        self.own(id)?;
        let fragment = agent_run_store::journal::prepare_message_fragment(
            &self.home,
            id,
            role,
            text,
            name,
            raw_ref,
            self.attempt.as_deref(),
            error,
        )?;
        self.push(fragment).await
    }

    /// Atomically records one normalized native item with its replay marker.
    /// The caller supplies already-redacted text and a stable key/digest marker;
    /// spool preparation reuses the ordinary transcript boundary. No native action
    /// is retried when this write is uncertain. Returns only after writer commit.
    #[allow(clippy::too_many_arguments)] // One transcript row plus its atomic replay marker.
    pub async fn native_item(
        &mut self,
        id: &AgentId,
        role: &str,
        text: &str,
        name: Option<&str>,
        raw_ref: Option<&str>,
        error: Option<(bool, &str)>,
        marker: Value,
    ) -> Result<()> {
        self.own(id)?;
        let fragment = agent_run_store::journal::prepare_message_fragment(
            &self.home,
            id,
            role,
            text,
            name,
            raw_ref,
            self.attempt.as_deref(),
            error,
        )?;
        self.flush().await?;
        self.producer
            .call(
                ops::AppendJournal {
                    execution: id.clone(),
                    fragments: vec![
                        fragment,
                        JournalFragment::Event {
                            kind: "opencode_item_v1".into(),
                            data: marker,
                            attempt: self.attempt.clone(),
                        },
                    ],
                },
                JOURNAL_BUDGET,
            )
            .await?;
        Ok(())
    }

    /// Flushes, then submits one critical operation.
    async fn critical<P>(&mut self, op: P) -> Result<P::Output>
    where
        P: WriteOp + Send + 'static,
        RemoteOp: From<P>,
    {
        self.flush().await?;
        self.producer.call(op, CRITICAL_BUDGET).await
    }

    /// Records the supervisor as the execution's owner (`starting` to owned).
    pub async fn record_owner(
        &mut self,
        owner: &agent_run_platform::process::Identity,
        process_group_id: Option<i32>,
    ) -> Result<()> {
        let op = ops::RecordSupervisorOwner {
            execution: self.execution.clone(),
            pid: owner.pid,
            identity: owner.token.clone(),
            process_group_id,
            birth_time: Some(owner.birth),
            at: crate::domain::now(),
        };
        self.critical(op).await
    }

    /// Seals the frozen identity with runtime home and digests (revision CAS).
    pub async fn update_identity(
        &mut self,
        id: &AgentId,
        expected_revision: &str,
        new_revision: &str,
        runtime_home: &str,
        snapshot_sha256: &str,
        assets_sha256: Option<&str>,
    ) -> Result<()> {
        self.own(id)?;
        self.critical(ops::UpdateIdentity {
            execution: id.clone(),
            expected_revision: expected_revision.to_owned(),
            new_revision: new_revision.to_owned(),
            runtime_home: runtime_home.to_owned(),
            snapshot_sha256: snapshot_sha256.to_owned(),
            assets_sha256: assets_sha256.map(str::to_owned),
        })
        .await
    }

    /// Marks the execution running in process group `pgid`.
    pub async fn running(&mut self, id: &AgentId, pgid: i32) -> Result<()> {
        self.own(id)?;
        self.critical(ops::Running {
            execution: id.clone(),
            pgid,
        })
        .await
    }

    /// Records the native runtime session of the bound attempt.
    pub async fn runtime_session(&mut self, id: &AgentId, session: &str) -> Result<()> {
        self.own(id)?;
        let attempt = self.attempt.clone();
        self.critical(ops::RuntimeSession {
            execution: id.clone(),
            attempt,
            session: session.to_owned(),
        })
        .await
    }

    /// Anchors the legacy single attempt as owned and spawning.
    pub async fn anchor_legacy_attempt(
        &mut self,
        attempt: &str,
        leader: &agent_run_platform::process::Identity,
    ) -> Result<()> {
        self.critical(ops::RetainCleanupEvidence {
            execution: self.execution.clone(),
            attempt: attempt.to_owned(),
            leader_token: leader.token.clone(),
            leader_birth: leader.birth,
        })
        .await
    }

    /// Claims the provider attempt's spawn; false when a cancel is pending.
    pub async fn provider_spawning(&mut self, id: &AgentId, attempt: &str) -> Result<bool> {
        self.own(id)?;
        self.critical(ops::ProviderSpawning {
            target: target(id, attempt),
        })
        .await
    }

    /// Records that the OS refused the spawn.
    pub async fn provider_spawn_failed(&mut self, id: &AgentId, attempt: &str) -> Result<()> {
        self.own(id)?;
        self.critical(ops::ProviderSpawnFailed {
            target: target(id, attempt),
        })
        .await
    }

    /// Records the spawned leader identity.
    pub async fn provider_process(
        &mut self,
        id: &AgentId,
        attempt: &str,
        leader: &agent_run_platform::process::Identity,
    ) -> Result<()> {
        self.own(id)?;
        self.critical(ops::ProviderProcess {
            target: target(id, attempt),
            leader: leader.clone(),
        })
        .await
    }

    /// Records the attempt as running again in a new process group.
    pub async fn provider_rerunning(
        &mut self,
        id: &AgentId,
        attempt: &str,
        pgid: i32,
    ) -> Result<()> {
        self.own(id)?;
        self.critical(ops::ProviderRerunning {
            target: target(id, attempt),
            pgid,
        })
        .await
    }

    /// Records verified cleanup of the attempt's process tree.
    pub async fn provider_cleanup(
        &mut self,
        id: &AgentId,
        attempt: &str,
        proof: &agent_run_platform::process::Cleanup,
    ) -> Result<()> {
        self.own(id)?;
        self.critical(ops::ProviderCleanup {
            target: target(id, attempt),
            proof: proof.clone(),
        })
        .await
    }

    /// Records bounded continuation metadata of the attempt.
    pub async fn provider_history(
        &mut self,
        id: &AgentId,
        attempt: &str,
        metadata: ops::ProviderHistoryMetadata,
    ) -> Result<()> {
        self.own(id)?;
        self.critical(ops::ProviderHistory {
            target: target(id, attempt),
            metadata,
        })
        .await
    }

    /// Persists a snapshot of the owned process tree for `kind`/`owner`.
    ///
    /// Takes `&self`-independent data so ownership sinks can call it from a
    /// detached future; see [`ownership_future`].
    pub async fn remember_processes(
        &mut self,
        kind: &str,
        owner: &str,
        snapshot: OwnershipSnapshot,
    ) -> Result<()> {
        self.critical(ops::RememberProcesses {
            kind: kind.to_owned(),
            owner: owner.to_owned(),
            snapshot,
        })
        .await
    }

    /// Issues the attempt-bound worker capability.
    pub async fn issue_worker_capability(
        &mut self,
        id: &AgentId,
        attempt: &str,
        token: &str,
        at: f64,
    ) -> Result<()> {
        self.own(id)?;
        self.critical(ops::IssueWorkerCapability {
            execution: id.clone(),
            attempt: attempt.to_owned(),
            token: token.to_owned(),
            at,
        })
        .await
    }

    /// Begins the attempt's worker lifecycle.
    pub async fn begin_worker_lifecycle(
        &mut self,
        id: &AgentId,
        attempt: &str,
        at: f64,
    ) -> Result<()> {
        self.own(id)?;
        self.critical(ops::BeginWorkerLifecycle {
            execution: id.clone(),
            attempt: attempt.to_owned(),
            at,
        })
        .await
    }

    /// Records the resumed execution's usage baseline once.
    pub async fn record_resume_usage_baseline(&mut self, id: &AgentId) -> Result<()> {
        self.own(id)?;
        self.critical(ops::RecordResumeUsageBaseline {
            execution: id.clone(),
        })
        .await
    }

    /// Claims the next pending command and reads its payload.
    ///
    /// The claim is one operation; its payload is read from the read-only
    /// handle, so a replayed claim never resends anything.
    pub async fn claim_command(&mut self, id: &AgentId) -> Result<Option<(i64, String, Value)>> {
        self.claim_command_for_turn(id, true).await
    }

    /// [`Self::claim_command`] holding native-completion wakes back until idle.
    pub async fn claim_command_for_turn(
        &mut self,
        id: &AgentId,
        allow_native_wake: bool,
    ) -> Result<Option<(i64, String, Value)>> {
        self.own(id)?;
        if !self.recoveries.is_empty() {
            self.drain_recoveries().await?;
        }
        // F1: nothing pending means no writer round trip at all.
        if !self.reader.command_pending(id, allow_native_wake)? {
            return Ok(None);
        }
        let claimed: Option<ops::ClaimedCommand> = self
            .critical(ops::ClaimCommand {
                execution: id.clone(),
                allow_native_wake,
            })
            .await?;
        let Some(claimed) = claimed else {
            return Ok(None);
        };
        let payload = self
            .reader
            .command_payload(id, claimed.command_id)?
            .map(|(_, payload)| payload)
            .unwrap_or(Value::Null);
        Ok(Some((claimed.command_id, claimed.kind, payload)))
    }

    /// Completes a command with its recorded result.
    pub async fn complete_command(
        &mut self,
        id: &AgentId,
        command_id: i64,
        result: &Value,
    ) -> Result<()> {
        self.own(id)?;
        self.critical(ops::CompleteCommand {
            execution: id.clone(),
            command_id,
            result: result.clone(),
        })
        .await
    }

    /// Records the observed result of a claimed command together with its
    /// worker-turn effect and journal rows as ONE operation (F4): the native
    /// effect has already happened, so the evidence is never split.
    ///
    /// `strength` is the evidence label (`stdin_written`, `native_accepted`).
    /// Journal rows buffered before this call are written first.
    ///
    /// # Errors
    /// See [`into_result`]; an `Unknown` outcome is an error the caller must
    /// treat as "evidence may or may not be durable", never as a reason to
    /// repeat the native action. Recovery-log errors propagate and retain the
    /// in-memory entry; pending payload and operation identity must be synced
    /// successfully before submission, and a resolution marker before removal.
    pub async fn observed_command_result(
        &mut self,
        id: &AgentId,
        command_id: i64,
        result: &Value,
        strength: &str,
        worker_turn: Option<bool>,
        fragments: Vec<JournalFragment>,
    ) -> Result<Vec<i64>> {
        self.own(id)?;
        let payload = ObservedPayload {
            command_id,
            result: result.clone(),
            strength: strength.to_owned(),
            worker_turn,
            fragments,
        };
        // The native effect already happened: its recovery material is durable
        // BEFORE any storage wait that could fail, so no later failure loses it.
        let attempt = self.recovery_attempt();
        let seq = self.recovery_seq;
        self.recovery_seq += 1;
        let recorded = self.append_recovery(&json!({
            "line": "pending",
            "seq": seq,
            "execution": self.execution,
            "attempt": attempt,
            "payload": payload,
        }));
        self.recoveries.push(Recovery {
            seq,
            attempt,
            payload: payload.clone(),
            original: None,
            outcome: None,
        });
        recorded?;
        // Rows buffered before the effect are written first; a failure here
        // leaves the entry pending, to be submitted by a later drain.
        self.flush().await?;
        let log = self.recovery_log_writer();
        let built = std::sync::Arc::new(std::sync::Mutex::new(None));
        let hook_slot = built.clone();
        let pending = self.producer.submit_identified(
            ops::ObservedCommandResult {
                execution: id.clone(),
                command_id,
                result: payload.result,
                strength: payload.strength,
                worker_turn,
                fragments: payload.fragments,
            },
            CRITICAL_BUDGET,
            Some(Box::new(move |operation_id, digest| {
                log.identity(seq, operation_id, digest)?;
                *hook_slot.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some((operation_id.clone(), digest.clone()));
                Ok(())
            })),
        );
        let totals = pending.totals();
        let outcome = pending.await;
        if let Some(entry) = self.recoveries.iter_mut().find(|entry| entry.seq == seq) {
            entry.original = built.lock().unwrap_or_else(|p| p.into_inner()).take();
            entry.outcome = Some(outcome_label(&outcome));
        }
        if matches!(outcome, SubmitResult::Committed(_)) {
            self.resolve_recovery(seq)?;
        } else {
            self.append_recovery(
                &json!({"line": "outcome", "seq": seq, "writer": outcome_label(&outcome)}),
            )?;
        }
        into_result_totals(outcome, &totals.snapshot())
    }

    /// The attempt a recovery observation belongs to: the bound attempt, else
    /// the legacy `<execution>:1` attempt when that row is durable and owned by
    /// this execution.
    fn recovery_attempt(&self) -> Option<String> {
        self.attempt.clone().or_else(|| {
            let legacy = format!("{}:1", self.execution);
            self.reader
                .conn
                .query_row(
                    "SELECT id FROM attempts WHERE id=? AND agent_id=?",
                    rusqlite::params![legacy, self.execution.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .ok()
        })
    }

    /// Appends one line to the execution's private recovery log: an owned
    /// directory, a mode-0600 regular file opened without following links, and
    /// the line flushed to stable storage before returning.
    fn append_recovery(&self, line: &Value) -> Result<()> {
        recovery_log(&self.home, &self.execution).append(line)
    }

    /// A detached writer of identity lines for the producer's envelope builder.
    fn recovery_log_writer(&self) -> RecoveryLog {
        recovery_log(&self.home, &self.execution)
    }

    /// Logs a confirmed entry's resolution durably before dropping it. An
    /// append failure retains the entry and propagates to the caller.
    fn resolve_recovery(&mut self, seq: u64) -> Result<()> {
        self.append_recovery(&json!({"line": "resolved", "seq": seq}))?;
        self.recoveries.retain(|entry| entry.seq != seq);
        Ok(())
    }

    /// Number of observed results whose evidence is not yet confirmed durable.
    pub fn unresolved_recoveries(&self) -> usize {
        self.recoveries.len()
    }

    /// Makes every unconfirmed observed result durable, oldest first, without
    /// ever repeating a native action: an original that was sent is recorded as
    /// a [`ops::PersistRecoveryObservation`] under its OWN new operation
    /// identity naming the original (unverified evidence only); an original
    /// that was never sent is submitted once as the same storage evidence.
    /// Returns how many entries were resolved; writer failures keep their
    /// entry. Filesystem failures propagate without losing any remaining entry.
    /// A newly submitted original retains its identity even when its outcome is
    /// unknown, so the next drain records recovery evidence rather than sending
    /// that original under a fresh identity.
    pub async fn drain_recoveries(&mut self) -> Result<usize> {
        let mut resolved = 0;
        for mut entry in self.recoveries.clone() {
            // Also covers an initial append failure: durable payload always
            // precedes any later attempt to submit storage evidence.
            self.append_recovery(&json!({
                "line": "pending", "seq": entry.seq,
                "execution": self.execution, "attempt": entry.attempt,
                "payload": entry.payload,
            }))?;
            let done = match (&entry.original, &entry.attempt) {
                (Some((operation_id, digest)), Some(attempt)) => self
                    .producer
                    .call(
                        ops::PersistRecoveryObservation {
                            execution: self.execution.clone(),
                            attempt: attempt.clone(),
                            original_operation_id: operation_id.clone(),
                            original_digest: digest.clone(),
                            kind: "observed_command_result".to_owned(),
                            result: entry.payload.result.clone(),
                            evidence: json!({
                                "command_id": entry.payload.command_id,
                                "strength": entry.payload.strength,
                                "writer": entry.outcome,
                            }),
                        },
                        CRITICAL_BUDGET,
                    )
                    .await
                    .is_ok(),
                (None, _) => {
                    let built = std::sync::Arc::new(std::sync::Mutex::new(None));
                    let hook_slot = built.clone();
                    let log = self.recovery_log_writer();
                    let seq = entry.seq;
                    let outcome = self
                        .producer
                        .submit_identified(
                            ops::ObservedCommandResult {
                                execution: self.execution.clone(),
                                command_id: entry.payload.command_id,
                                result: entry.payload.result.clone(),
                                strength: entry.payload.strength.clone(),
                                worker_turn: entry.payload.worker_turn,
                                fragments: entry.payload.fragments.clone(),
                            },
                            CRITICAL_BUDGET,
                            Some(Box::new(move |operation_id, digest| {
                                log.identity(seq, operation_id, digest)?;
                                *hook_slot.lock().unwrap_or_else(|p| p.into_inner()) =
                                    Some((operation_id.clone(), digest.clone()));
                                Ok(())
                            })),
                        )
                        .await;
                    entry.original = built.lock().unwrap_or_else(|p| p.into_inner()).take();
                    entry.outcome = Some(outcome_label(&outcome));
                    if let Some(saved) = self.recoveries.iter_mut().find(|r| r.seq == seq) {
                        *saved = entry.clone();
                    }
                    if !matches!(outcome, SubmitResult::Committed(_)) {
                        self.append_recovery(&json!({
                            "line": "outcome", "seq": seq, "writer": outcome_label(&outcome),
                        }))?;
                    }
                    matches!(outcome, SubmitResult::Committed(_))
                }
                // Sent but no attempt association is known: kept for the
                // broker's reconciliation, never guessed.
                (Some(_), None) => false,
            };
            if done {
                self.resolve_recovery(entry.seq)?;
                resolved += 1;
            }
        }
        Ok(resolved)
    }

    /// Builds the user transcript fragments of `text` for the bound attempt, in
    /// bounded UTF-8 chunks, for inclusion in one atomic observed-result operation.
    pub fn user_fragments(&self, id: &AgentId, text: &str) -> Result<Vec<JournalFragment>> {
        let mut fragments = Vec::new();
        let mut remaining = text;
        while !remaining.is_empty() {
            let mut end = remaining.len().min(16 * 1024);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            fragments.push(agent_run_store::journal::prepare_message_fragment(
                &self.home,
                id,
                "user",
                &remaining[..end],
                None,
                None,
                self.attempt.as_deref(),
                None,
            )?);
            remaining = &remaining[end..];
        }
        Ok(fragments)
    }

    /// Completes commands that arrived after the execution ended.
    pub async fn complete_terminal(&mut self, id: &AgentId) -> Result<u64> {
        self.own(id)?;
        self.critical(ops::CompleteTerminalCommands {
            execution: id.clone(),
            at: crate::domain::now(),
        })
        .await
    }

    /// Records a worker turn when the read-only preflight says it would change
    /// the phase (F1). A no-change answer is the local value `false`, never a
    /// fabricated receipt.
    pub async fn worker_turn(&mut self, id: &AgentId, idle: bool, at: f64) -> Result<bool> {
        self.own(id)?;
        let Some(attempt) = self.attempt.clone() else {
            return Ok(false);
        };
        if !self.reader.worker_turn_pending(id, &attempt, idle)? {
            return Ok(false);
        }
        self.critical(ops::WorkerTurn {
            execution: id.clone(),
            idle,
            at,
        })
        .await
    }

    /// Queues the native completion wake; false when it was already queued.
    pub async fn enqueue_native_wake(
        &mut self,
        id: &AgentId,
        digest: &str,
        text: &str,
        at: f64,
    ) -> Result<bool> {
        self.own(id)?;
        self.critical(ops::EnqueueNativeWake {
            execution: id.clone(),
            digest: digest.to_owned(),
            text: text.to_owned(),
            at,
        })
        .await
    }

    /// Marks the accepted finish receipt as observed in the native transcript.
    pub async fn observe_worker_finish_receipt(&mut self, id: &AgentId) -> Result<()> {
        self.own(id)?;
        self.critical(ops::ObserveWorkerFinishReceipt {
            execution: id.clone(),
        })
        .await
    }

    /// Latches native quota exhaustion for the bound account.
    #[allow(clippy::too_many_arguments)]
    pub async fn latch_native_exhaustion(&mut self, op: ops::LatchNativeExhaustion) -> Result<i64> {
        self.own(&op.execution)?;
        self.critical(op).await
    }

    /// Allocates the next attempt after an account switch.
    pub async fn allocate_next_attempt(
        &mut self,
        op: ops::AllocateNextAttempt,
    ) -> Result<ops::NextAttemptOutput> {
        self.own(&op.execution)?;
        self.critical(op).await
    }

    /// Writes the terminal outcome. `Unknown` is returned as an error; the
    /// caller must not claim the terminal state without proof.
    pub async fn finish(
        &mut self,
        id: &AgentId,
        outcome: &Outcome,
        proof: Option<&Proof>,
        usage: Option<&Value>,
    ) -> Result<String> {
        self.own(id)?;
        // Unconfirmed observed results are settled before the terminal state;
        // whatever stays unresolved is announced, never silently dropped.
        if !self.recoveries.is_empty() {
            self.drain_recoveries().await?;
            let left = self.recoveries.len();
            if left > 0 {
                let _ = self
                    .event(id, "recovery_unresolved_v1", &json!({"entries": left}))
                    .await;
            }
        }
        let status = self
            .critical(ops::FinishTerminal {
                execution: id.clone(),
                outcome: outcome.clone(),
                proof: proof.cloned(),
                usage: usage.cloned(),
            })
            .await?;
        // The terminal commit carries no statistics: they are computed from the
        // committed rows by a bounded read off the executor and persisted as a
        // separate background operation. A failure never changes the committed
        // terminal outcome.
        self.persist_run_stats(id).await;
        Ok(status)
    }

    /// Computes and persists the run statistics of the terminal execution
    /// (best effort, after the terminal commit; a failure is recorded as a
    /// closed event only).
    async fn persist_run_stats(&mut self, id: &AgentId) {
        let (home, execution) = (self.home.clone(), id.clone());
        let row = tokio::task::spawn_blocking(move || {
            StoreReader::open_with(&home, Duration::from_millis(250))
                .and_then(|reader| reader.run_stats_projection(&execution))
        })
        .await;
        let stored = match row {
            Ok(Ok(row)) => self
                .producer
                .call(ops::PersistRunStats { row }, STATS_BUDGET)
                .await
                .map(drop),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Runtime("run statistics task failed".into())),
        };
        if let Err(error) = stored {
            let _ = self
                .event_durable(
                    id,
                    "run_stats_unavailable_v1",
                    &crate::supervisor::execution_error_metadata(&error),
                )
                .await;
        }
    }

    /// Prepares a managed-runtime storage layout (supervisor materialization).
    pub async fn prepare_runtime_layout(
        &mut self,
        op: ops::PrepareRuntimeLayout,
    ) -> Result<ops::LayoutReceipt> {
        self.critical(op).await
    }

    /// Commits a prepared managed-runtime storage layout.
    pub async fn commit_runtime_layout(
        &mut self,
        op: ops::CommitRuntimeLayout,
    ) -> Result<ops::LayoutReceipt> {
        self.critical(op).await
    }
}

/// Target of an attempt-scoped operation.
fn target(id: &AgentId, attempt: &str) -> ops::AttemptTarget {
    ops::AttemptTarget {
        execution: id.clone(),
        attempt: attempt.to_owned(),
    }
}

/// The detached persistence future of one ownership snapshot.
///
/// The submission position is claimed when this function is CALLED (inside the
/// sink closure), the returned future owns everything it needs, and dropping it
/// never withdraws the operation: the lane keeps the head until the operation
/// reaches a final outcome or its original deadline.
pub fn ownership_future(
    producer: &Producer,
    owner: String,
    snapshot: OwnershipSnapshot,
    budget: Duration,
) -> crate::adapters::io::PersistFuture {
    let pending = producer.submit(
        ops::RememberProcesses {
            kind: "attempt".to_owned(),
            owner,
            snapshot,
        },
        budget,
    );
    let totals = pending.totals();
    Box::pin(async move { into_result_totals::<()>(pending.await, &totals.snapshot()) })
}

/// A producer identity for a supervisor of `execution`.
pub fn supervisor_producer_id(execution: &AgentId) -> ProducerId {
    ProducerId {
        kind: ProducerKind::Supervisor,
        scope: Some(execution.to_string()),
        instance: uuid::Uuid::new_v4().simple().to_string(),
    }
}

/// The hello claim of a supervisor.
pub fn supervisor_hello(logical: &AgentId, execution: &AgentId, capability_hex: String) -> Hello {
    Hello {
        protocol: agent_run_domain::storage::WRITE_PROTOCOL,
        role: "supervisor".to_owned(),
        logical: Some(logical.to_string()),
        execution: Some(execution.to_string()),
        attempt: None,
        generation: None,
        capability_hex: Some(capability_hex),
    }
}

/// Redacted diagnostic of a writer outcome for logs: kind only, no content.
pub fn outcome_label(result: &SubmitResult) -> Value {
    match result {
        SubmitResult::Committed(_) => json!({"outcome": "committed"}),
        SubmitResult::Rejected(rejection) => {
            json!({"outcome": "rejected", "code": rejection.code, "durable": rejection.durable})
        }
        SubmitResult::NotEnqueued(reason) => json!({"outcome": "not_enqueued", "reason": reason}),
        SubmitResult::Unknown(cause) => json!({"outcome": "unknown", "cause": cause}),
    }
}

/// A producer identity for one operator command-line process.
pub fn cli_producer_id() -> ProducerId {
    ProducerId {
        kind: ProducerKind::Cli,
        scope: Some("cli".to_owned()),
        instance: uuid::Uuid::new_v4().simple().to_string(),
    }
}

/// The hello claim of a same-user command-line process (no capability: the
/// kernel-attested peer identity is the claim's proof).
pub fn cli_hello() -> Hello {
    Hello {
        protocol: agent_run_domain::storage::WRITE_PROTOCOL,
        role: "cli".to_owned(),
        logical: None,
        execution: None,
        attempt: None,
        generation: None,
        capability_hex: None,
    }
}

/// Build a same-user operator client to the existing resident writer.
/// Construction performs no I/O; calls require authenticated broker transport
/// and never fall back to a local writable database when the broker is absent.
pub fn operator_producer(home: &Path) -> Producer {
    Producer::connect(&home.join("writer.sock"), cli_hello(), cli_producer_id())
}

/// Build the metadata collector's authenticated writer client, without opening SQL.
/// Its authority only permits collector operations, never ownership or completion.
pub fn collector_producer(home: &Path) -> Producer {
    let mut hello = cli_hello();
    hello.role = "collector".into();
    let mut id = cli_producer_id();
    id.kind = ProducerKind::Collector;
    Producer::connect(&home.join("writer.sock"), hello, id)
}

/// Whether `execution` owns an attempt that was prepared but never spawned a
/// child (read-only). The mutating certification of that fact belongs to the
/// terminal commit; asking never changes a row.
pub fn attempt_never_spawned(conn: &rusqlite::Connection, execution: &AgentId) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM attempts WHERE agent_id=? AND ownership_active=1 \
         AND phase='prepared' AND process_identity IS NULL)",
        [execution.as_str()],
        |row| row.get(0),
    )?)
}
