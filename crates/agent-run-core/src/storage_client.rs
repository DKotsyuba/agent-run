//! Client side of the private writer socket: one ordered producer lane.
//!
//! A [`crate::storage_client::ProducerLane`] is the only way a process other than the broker submits
//! storage operations. It owns exactly one connection and one strictly
//! increasing ordinal sequence and keeps a *head lock* across ordinal
//! allocation, submission, retry and the final outcome of every operation:
//! a later operation cannot start while an earlier one is unresolved, and the
//! earlier one is never abandoned because its caller was cancelled (the work
//! runs in a detached task whose result is read through [`crate::storage_client::PendingWrite`]).
//!
//! Retries are always the SAME full tuple: the byte-identical request body
//! (operation id, digest, producer, ordinal and the original [`agent_run_domain::storage::MonoDeadline`]
//! are inside it), never a new ordinal and never a renewed budget. The deadline
//! is checked against the shared same-host monotonic clock before every
//! (re)send; after it passes no new attempt starts and the lane reports
//! `NotEnqueued(DeadlineExpired)` if nothing was ever sent, otherwise the last
//! `Unknown` cause. A lost reply is resolved by resubmitting the identical body
//! (the writer answers from its durable receipt), and no outcome here ever
//! triggers a native action: replay is evidence persistence only.
//!
//! Admission order is claimed synchronously when [`crate::storage_client::ProducerLane::submit`] (or
//! [`crate::storage_client::ProducerLane::repin`]) is CALLED, before any work is detached: each call
//! takes the previous call's completion signal, so scheduler placement can never
//! reorder two submissions. Uncertainty is sticky: once any attempt of an
//! operation may have reached the writer (a written frame whose reply was lost,
//! or an `Unknown` reply) only a definitive `Committed`, a durable `Rejected`
//! or the original deadline ends the operation, and a later non-durable
//! refusal never replaces the earlier uncertainty.
//!
//! This module is transport only. The typed `WriteEnvelope`
//! is turned into a [`crate::storage_client::WireRequest`] by the caller; authority is never supplied
//! by the request and the server derives it from the kernel's peer identity and
//! the [`crate::storage_client::Hello`] capability.
use crate::storage_wire::{self as wire, Message};
use agent_run_domain::storage::{
    MAX_REPLY_BYTES, MonoDeadline, NotEnqueuedReason, OperationId, PayloadDigest, ProducerId,
    RejectCode, SubmitResult, UnknownCause,
};
use agent_run_platform::monotonic;
use serde::{Deserialize, Serialize};
use std::{future::Future, io, pin::Pin, sync::Arc, task::Poll, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, oneshot},
    task::JoinHandle,
};

/// A duplex byte stream carrying frames.
pub trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

/// One boxed connection.
pub type Connection = Box<dyn Duplex>;

/// Factory that opens a fresh connection to the writer socket.
pub trait Dial: Send + Sync + 'static {
    /// Open one connection (a single attempt, bounded by the caller).
    fn dial(&self) -> Pin<Box<dyn Future<Output = io::Result<Connection>> + Send + '_>>;
}

/// Dials a Unix-domain socket path.
pub struct UnixDial {
    /// Socket path (normally `<home>/writer.sock`).
    pub path: std::path::PathBuf,
}

impl Dial for UnixDial {
    /// Connects to the socket; same-user verification is the server's job.
    fn dial(&self) -> Pin<Box<dyn Future<Output = io::Result<Connection>> + Send + '_>> {
        Box::pin(async move {
            let stream = tokio::net::UnixStream::connect(&self.path).await?;
            Ok(Box::new(stream) as Connection)
        })
    }
}

/// Client introduction, the body of a [`Message::Hello`] frame.
///
/// Every field is a CLAIM: the server derives the authority from the kernel
/// peer identity and the durable records, and the writer re-checks them inside
/// each transaction. The capability secret travels only here, on the private
/// socket, and is never logged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    /// Framing/storage protocol version the client speaks.
    pub protocol: u16,
    /// Claimed role: `supervisor`, `service_exec`, `cli` or `collector`.
    pub role: String,
    /// Logical (root) agent id for a supervisor.
    pub logical: Option<String>,
    /// Execution id for a supervisor.
    pub execution: Option<String>,
    /// Currently pinned attempt for a supervisor; changed only by [`ProducerLane::repin`].
    pub attempt: Option<String>,
    /// Managed-service generation for a service executor.
    pub generation: Option<String>,
    /// Hex of the per-launch capability secret, when the role needs one.
    pub capability_hex: Option<String>,
    /// A supervisor's launch-secret context for its pinned attempt. The
    /// server installs it only for a verified supervisor session, keeps it in
    /// memory while that connection lives, and checks first finish intents of
    /// that exact attempt against it. Older servers ignore the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_guard: Option<FinishGuardContext>,
}

/// The nonempty secret-shaped variables of one native launch environment,
/// names with values. Sent only inside a supervisor [`Hello`] on the private
/// writer socket; `Debug` never shows a value.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FinishGuardContext(pub std::collections::BTreeMap<String, String>);

impl FinishGuardContext {
    /// Selects from `environment` exactly the variables the launch's process
    /// redactor protects: nonempty values under secret-shaped names.
    pub fn from_environment(environment: &std::collections::BTreeMap<String, String>) -> Self {
        Self(
            environment
                .iter()
                .filter(|(name, value)| {
                    !value.is_empty() && agent_run_adapters::redact::is_secret_name(name)
                })
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        )
    }
}

impl std::fmt::Debug for FinishGuardContext {
    /// Shows only how many variables the context holds.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "FinishGuardContext({} variables)", self.0.len())
    }
}

/// Server acknowledgement of a [`Hello`], the body of a [`Message::HelloOk`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloOk {
    /// Writer epoch of the answering broker.
    pub epoch: u64,
    /// Boot session of the broker; must equal the client's, otherwise the
    /// peer is on another boot or host and its clock is not comparable.
    pub boot_id: String,
    /// The broker's monotonic reading when it answered (diagnostic).
    pub mono_ns: u64,
}

/// One fully built request: the verified canonical body and its identity.
#[derive(Debug, Clone)]
pub struct WireRequest {
    /// Operation identity, constant across retries.
    pub operation_id: OperationId,
    /// Payload digest the writer's receipt must carry (correlation check).
    pub digest: PayloadDigest,
    /// Producer identity inside the envelope; the receipt must name it.
    pub producer: ProducerId,
    /// Ordinal inside the envelope, equal to the one the lane allocated; the
    /// receipt must carry it.
    pub ordinal: u64,
    /// Closed operation kind name the receipt must carry, when known.
    pub op_kind: Option<String>,
    /// Stable operation wire id for the frame header.
    pub kind: u16,
    /// Canonical envelope body; retried byte-for-byte.
    pub body: Vec<u8>,
    /// The envelope's original deadline.
    pub deadline: MonoDeadline,
}

/// Outcome of a lane submission, read by awaiting [`PendingWrite`].
///
/// Dropping a `PendingWrite` never cancels the submission: the detached task
/// keeps the head lock until the operation reaches a final outcome or its
/// original deadline, so a cancelled caller cannot let a later operation
/// overtake an unresolved earlier one.
pub struct PendingWrite {
    handle: JoinHandle<SubmitResult>,
    /// Truthful totals of the submission, filled as it runs.
    totals: Arc<std::sync::Mutex<Totals>>,
}

/// What a submission actually did: attempts made, time spent from the head of
/// the lane to its final outcome, and the writer's last refusal before the
/// acceptance (kept so an expired deadline still reports the real operation,
/// phase and SQLite codes instead of a generic expiry).
#[derive(Debug, Clone, Default)]
pub struct Totals {
    /// Submissions of the identical operation that reached the wire.
    pub attempts: u32,
    /// Time from the head of the lane to the final outcome.
    pub elapsed: Duration,
    /// The writer's last non-durable refusal, when it caused retries.
    pub last_rejection: Option<agent_run_domain::storage::StorageRejection>,
}

/// Shared view of a submission's [`Totals`], readable after awaiting it.
#[derive(Clone)]
pub struct TotalsHandle(Arc<std::sync::Mutex<Totals>>);

impl TotalsHandle {
    /// The totals recorded so far.
    pub fn snapshot(&self) -> Totals {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl PendingWrite {
    /// A handle to read the submission's totals after awaiting it.
    pub fn totals(&self) -> TotalsHandle {
        TotalsHandle(self.totals.clone())
    }
}

impl Future for PendingWrite {
    type Output = SubmitResult;

    /// Resolves to the final outcome; a panicked task is an unknown transport result.
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<SubmitResult> {
        match Pin::new(&mut self.handle).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(SubmitResult::Unknown(UnknownCause::Transport)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Outcome of a detached [`ProducerLane::repin`].
///
/// Like [`PendingWrite`], dropping it (even before it was ever polled, or while
/// it still waits for an earlier call) never cancels the re-pin and never
/// releases the lane's admission turn early: the detached task keeps the
/// predecessor barrier until the re-pin has run.
pub struct PendingRepin {
    /// Result of the detached task; dropping this handle keeps its admission barrier alive.
    handle: JoinHandle<io::Result<()>>,
}

impl Future for PendingRepin {
    type Output = io::Result<()>;

    /// Resolves to the re-hello result; a panicked task is an interrupted error.
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.handle).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "re-pin task failed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Mutable lane state, guarded by the head lock.
struct State {
    /// Next ordinal to hand out; strictly increasing, gaps allowed.
    next_ordinal: u64,
    /// Next frame sequence number.
    seq: u64,
    /// Current hello claim (attempt changes only through repin).
    hello: Hello,
    /// Live connection, dropped on any transport error.
    connection: Option<Connection>,
}

/// Immutable lane configuration.
struct Config {
    dial: Box<dyn Dial>,
    /// Longest wait for one connect + hello exchange.
    connect_budget: Duration,
    /// First pause between retries of the same tuple (grows to [`BACKOFF_MAX`]).
    backoff_first: Duration,
}

/// One ordered producer lane over the writer socket.
#[derive(Clone)]
pub struct ProducerLane {
    config: Arc<Config>,
    state: Arc<Mutex<State>>,
    /// Completion signal of the most recently admitted call, replaced
    /// synchronously at call time; the next call waits for it.
    tail: Arc<std::sync::Mutex<Option<oneshot::Receiver<()>>>>,
}

/// Signals the next admitted call when dropped, on every exit path including
/// panic and cancellation of the detached task.
struct Turn(Option<oneshot::Sender<()>>);

impl Drop for Turn {
    /// Releases the head to the next call.
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

/// Longest wait for a single connect + hello exchange.
const CONNECT_BUDGET: Duration = Duration::from_secs(2);
/// First and largest pause between retries of the same tuple.
const BACKOFF_FIRST: Duration = Duration::from_millis(20);
/// Largest pause between retries of the same tuple.
const BACKOFF_MAX: Duration = Duration::from_millis(500);

impl ProducerLane {
    /// Creates a lane that dials with `dial` and introduces itself with `hello`.
    ///
    /// `first_ordinal` is the first ordinal this lane hands out; use `1` for a
    /// fresh process (the producer identity carries a per-process instance, so
    /// ordinals never collide with an earlier process).
    pub fn new(dial: impl Dial, hello: Hello, first_ordinal: u64) -> Self {
        Self::with_timing(dial, hello, first_ordinal, CONNECT_BUDGET, BACKOFF_FIRST)
    }

    /// [`Self::new`] with an explicit connect + hello budget and first retry pause.
    pub fn with_timing(
        dial: impl Dial,
        hello: Hello,
        first_ordinal: u64,
        connect_budget: Duration,
        backoff_first: Duration,
    ) -> Self {
        Self {
            config: Arc::new(Config {
                dial: Box::new(dial),
                connect_budget,
                backoff_first,
            }),
            tail: Arc::default(),
            state: Arc::new(Mutex::new(State {
                next_ordinal: first_ordinal,
                seq: 1,
                hello,
                connection: None,
            })),
        }
    }

    /// Submits one operation and returns its detached pending result.
    ///
    /// `build` runs while the head lock is held and receives the allocated
    /// ordinal; it must construct the request (typically
    /// `WriteEnvelope::new(.., ordinal, ..)` then `to_wire()`) without blocking.
    /// A build error becomes `Rejected(Validation)`; the ordinal it consumed
    /// stays consumed (gaps are allowed). Must be called inside a Tokio runtime.
    pub fn submit<F>(&self, build: F) -> PendingWrite
    where
        F: FnOnce(u64) -> Result<WireRequest, agent_run_domain::Error> + Send + 'static,
    {
        let config = self.config.clone();
        let state = self.state.clone();
        // Claim the admission position NOW, on the caller's side.
        let (previous, turn) = self.claim_turn();
        let totals = Arc::new(std::sync::Mutex::new(Totals::default()));
        let recorded = totals.clone();
        let handle = tokio::spawn(async move {
            let _turn = turn;
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            let mut state = state.lock_owned().await;
            let ordinal = state.next_ordinal;
            state.next_ordinal += 1;
            let request = match build(ordinal) {
                Ok(request) => request,
                Err(error) => {
                    return SubmitResult::Rejected(
                        agent_run_domain::storage::StorageRejection::from_error(
                            &error,
                            agent_run_domain::storage::RejectStage::Gate,
                        ),
                    );
                }
            };
            if request.ordinal != ordinal {
                return SubmitResult::Rejected(
                    agent_run_domain::storage::StorageRejection::from_error(
                        &agent_run_domain::error::invalid(
                            "request ordinal differs from the lane's allocation",
                        ),
                        agent_run_domain::storage::RejectStage::Gate,
                    ),
                );
            }
            let started = std::time::Instant::now();
            let result = drive(&config, &mut state, &request, &recorded).await;
            recorded
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .elapsed = started.elapsed();
            result
        });
        PendingWrite { handle, totals }
    }

    /// Takes the admission position of this call: the previous call's completion
    /// signal (if any) and the guard that signals the next call.
    fn claim_turn(&self) -> (Option<oneshot::Receiver<()>>, Turn) {
        let (sender, receiver) = oneshot::channel();
        let previous = self
            .tail
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(receiver);
        (previous, Turn(Some(sender)))
    }

    /// Re-pins the supervisor's attempt claim with a fresh hello.
    ///
    /// The admission position is claimed when this method is CALLED and the work
    /// is detached exactly like [`Self::submit`]: the returned [`PendingRepin`]
    /// may be dropped at any time (never polled, or while waiting for an earlier
    /// call) without releasing the predecessor barrier, so a later call can never
    /// overtake an unresolved earlier one. The capability and process identity
    /// are unchanged by construction; only the attempt claim moves, and any
    /// launch-secret context of the previous attempt is dropped from the claim.
    /// If a connection is open the new hello is exchanged immediately,
    /// otherwise it is sent on the next connect. Must be called inside a Tokio
    /// runtime.
    ///
    /// # Errors
    /// (through the pending result) The transport or refusal error of the
    /// immediate exchange; the lane then reconnects with the new claim on the
    /// next submission.
    pub fn repin(&self, attempt: Option<String>) -> PendingRepin {
        self.rehello(
            move |hello| {
                hello.attempt = attempt;
                hello.finish_guard = None;
            },
            false,
        )
    }

    /// Replaces the hello's launch-secret context for the pinned attempt and
    /// exchanges it at once, connecting when no connection is open, so a
    /// verified server holds it before the caller releases the native engine.
    /// Ordered and detached exactly like [`Self::repin`]. A failed connect is
    /// not an error: the context travels with the next connection's hello, and
    /// until then the server refuses that attempt's first finish intent.
    ///
    /// # Errors
    /// (through the pending result) A protocol violation of the exchange.
    pub fn set_finish_guard(&self, guard: Option<FinishGuardContext>) -> PendingRepin {
        self.rehello(move |hello| hello.finish_guard = guard, true)
    }

    /// Re-sends the current hello, reconnecting when the connection is gone, so
    /// a restarted or reconnected broker regains this supervisor's finish
    /// context even while the lane is otherwise idle. Ordered and detached
    /// exactly like [`Self::repin`].
    ///
    /// # Errors
    /// (through the pending result) A protocol violation of the exchange.
    pub fn refresh(&self) -> PendingRepin {
        self.rehello(|_| {}, true)
    }

    /// Applies `change` to the stored hello under the head lock and exchanges
    /// it on the open connection; a lost connection is dropped and, when
    /// `connect` is set, one fresh connection is dialed within the connect
    /// budget. Only a protocol violation is reported.
    fn rehello(
        &self,
        change: impl FnOnce(&mut Hello) + Send + 'static,
        connect: bool,
    ) -> PendingRepin {
        let state = self.state.clone();
        let config = self.config.clone();
        let budget = self.config.connect_budget;
        let (previous, turn) = self.claim_turn();
        let handle = tokio::spawn(async move {
            let _turn = turn;
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            let mut state = state.lock().await;
            change(&mut state.hello);
            if let Some(mut connection) = state.connection.take() {
                let hello = state.hello.clone();
                let seq = state.seq;
                state.seq += 1;
                match tokio::time::timeout(budget, exchange_hello(&mut connection, &hello, seq))
                    .await
                {
                    Ok(Ok(_)) => {
                        state.connection = Some(connection);
                        return Ok(());
                    }
                    // The connection is gone (the broker restarted): the new
                    // claim is already stored and the next connection carries
                    // it, where the server verifies it. Only a protocol
                    // violation is reported here.
                    Ok(Err(error)) if lost_connection(&error) => {}
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {}
                }
            }
            if connect {
                let hello = state.hello.clone();
                let seq = state.seq;
                let dialed = tokio::time::timeout(budget, async {
                    let mut connection = config.dial.dial().await?;
                    exchange_hello(&mut connection, &hello, seq).await?;
                    Ok::<_, io::Error>(connection)
                })
                .await;
                state.seq = seq + 1;
                if let Ok(Ok(connection)) = dialed {
                    state.connection = Some(connection);
                }
            }
            Ok(())
        });
        PendingRepin { handle }
    }
}

/// Whether `error` means the connection itself is gone (reconnecting is the
/// remedy), as opposed to a protocol violation.
fn lost_connection(error: &io::Error) -> bool {
    use io::ErrorKind::{
        BrokenPipe, ConnectionAborted, ConnectionRefused, ConnectionReset, NotConnected, NotFound,
        TimedOut, UnexpectedEof,
    };
    matches!(
        error.kind(),
        BrokenPipe
            | ConnectionAborted
            | ConnectionRefused
            | ConnectionReset
            | NotConnected
            | NotFound
            | TimedOut
            | UnexpectedEof
    )
}

/// Send a hello and read its acknowledgement, validating the clock domain.
async fn exchange_hello(
    connection: &mut Connection,
    hello: &Hello,
    seq: u64,
) -> io::Result<HelloOk> {
    let body = serde_json::to_vec(hello).map_err(io::Error::other)?;
    wire::write_frame(connection, Message::Hello, 0, seq, &body).await?;
    let header = wire::read_header(connection, |message, _| {
        (message == Message::HelloOk).then_some(wire::HELLO_MAX)
    })
    .await
    .map_err(frame_io)?
    .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "writer closed during hello"))?;
    if header.seq != seq {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "hello acknowledgement sequence mismatch",
        ));
    }
    let reply = wire::read_body(connection, &header)
        .await
        .map_err(frame_io)?;
    let ok: HelloOk = serde_json::from_slice(&reply).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed hello acknowledgement",
        )
    })?;
    let boot = monotonic::boot_id()?;
    if ok.boot_id != boot {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "writer is on another boot session; its clock is not comparable",
        ));
    }
    Ok(ok)
}

/// Map a framing error to an I/O error without losing its closed description.
fn frame_io(error: wire::FrameError) -> io::Error {
    match error {
        wire::FrameError::Io(error) => error,
        other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
    }
}

/// Whether a not-enqueued refusal is transient (retry the same tuple).
pub(crate) fn transient(reason: NotEnqueuedReason) -> bool {
    matches!(
        reason,
        NotEnqueuedReason::QueueFull
            | NotEnqueuedReason::BytesFull
            | NotEnqueuedReason::ProducerFull
            | NotEnqueuedReason::ReservedForCritical
            | NotEnqueuedReason::Degraded
    )
}

/// Remaining time to `deadline`, or `None` when it has passed (or belongs to
/// another boot).
pub(crate) fn remaining(deadline: &MonoDeadline) -> Option<Duration> {
    let (boot, now) = monotonic::now().ok()?;
    if deadline.expired(&boot, now) {
        None
    } else {
        Some(Duration::from_nanos(deadline.ns - now))
    }
}

/// Run one request to its final outcome under the held head lock.
///
/// `uncertain` is sticky: it is set the moment a frame may have reached the
/// writer without a definitive answer (lost reply, timeout after a write, an
/// `Unknown` reply) and is cleared only by returning. While it is set, a
/// transient refusal merely schedules another identical attempt and any
/// non-definitive result (non-durable rejection, non-transient refusal, the
/// deadline) resolves to `Unknown(cause)`, never to a `NotEnqueued` or
/// non-durable rejection that would claim the earlier attempt did not happen.
async fn drive(
    config: &Config,
    state: &mut State,
    request: &WireRequest,
    totals: &std::sync::Mutex<Totals>,
) -> SubmitResult {
    let note = |change: &mut dyn FnMut(&mut Totals)| {
        change(
            &mut totals
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    };
    let mut uncertain: Option<UnknownCause> = None;
    let mut backoff = config.backoff_first;
    loop {
        let Some(budget) = remaining(&request.deadline) else {
            return expired(uncertain);
        };
        if state.connection.is_none() {
            let attempt = tokio::time::timeout(budget.min(config.connect_budget), async {
                let mut connection = config.dial.dial().await?;
                let hello = state.hello.clone();
                let seq = state.seq;
                exchange_hello(&mut connection, &hello, seq).await?;
                Ok::<_, io::Error>((connection, seq))
            })
            .await;
            match attempt {
                Ok(Ok((connection, seq))) => {
                    state.seq = seq + 1;
                    state.connection = Some(connection);
                }
                _ => {
                    pause(&mut backoff, &request.deadline).await;
                    continue;
                }
            }
        }
        // The handshake consumed part of the original budget: recompute from the
        // original deadline immediately before the send; never reuse `budget`.
        let Some(budget) = remaining(&request.deadline) else {
            return expired(uncertain);
        };
        let seq = state.seq;
        state.seq += 1;
        note(&mut |totals| totals.attempts = totals.attempts.saturating_add(1));
        let connection = state.connection.as_mut().expect("connected above");
        let mut written = false;
        let exchange = tokio::time::timeout(budget, async {
            written = true;
            wire::write_frame(
                connection,
                Message::Request,
                request.kind,
                seq,
                &request.body,
            )
            .await?;
            let header = wire::read_header(connection, |message, _| {
                (message == Message::Reply).then_some(MAX_REPLY_BYTES)
            })
            .await
            .map_err(frame_io)?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "writer closed the connection")
            })?;
            if header.seq != seq {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "reply sequence mismatch",
                ));
            }
            let body = wire::read_body(connection, &header)
                .await
                .map_err(frame_io)?;
            let result = serde_json::from_slice::<SubmitResult>(&body).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "malformed writer reply")
            })?;
            correlate(&result, request)?;
            Ok(result)
        })
        .await;
        let outcome = match exchange {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "no reply in budget",
            )),
        };
        match outcome {
            Ok(SubmitResult::Committed(receipt)) => return SubmitResult::Committed(receipt),
            Ok(SubmitResult::Rejected(rejection)) if rejection.durable => {
                return SubmitResult::Rejected(rejection);
            }
            Ok(SubmitResult::Rejected(rejection))
                if rejection.code == RejectCode::StorageUnavailable =>
            {
                // Nothing was acquired or executed by THIS attempt; retry identically.
                note(&mut |totals| totals.last_rejection = Some(rejection.clone()));
                pause(&mut backoff, &request.deadline).await;
            }
            Ok(SubmitResult::Rejected(rejection)) => match uncertain {
                None => return SubmitResult::Rejected(rejection),
                Some(cause) => return SubmitResult::Unknown(cause),
            },
            Ok(SubmitResult::NotEnqueued(reason)) if transient(reason) => {
                pause(&mut backoff, &request.deadline).await;
            }
            Ok(SubmitResult::NotEnqueued(reason)) => match uncertain {
                None => return SubmitResult::NotEnqueued(reason),
                Some(cause) => return SubmitResult::Unknown(cause),
            },
            Ok(SubmitResult::Unknown(cause)) => {
                uncertain = Some(cause);
                pause(&mut backoff, &request.deadline).await;
            }
            Err(error) => {
                // A write may have started: the outcome is unknown until the same
                // operation resolves; reconnect and resubmit the identical body.
                state.connection = None;
                if written {
                    uncertain = Some(
                        if remaining(&request.deadline).is_none()
                            || error.kind() == io::ErrorKind::TimedOut
                        {
                            UnknownCause::DeadlineDuringExecution
                        } else {
                            UnknownCause::Transport
                        },
                    );
                }
                pause(&mut backoff, &request.deadline).await;
            }
        }
    }
}

/// Final result when the original deadline has passed.
pub(crate) fn expired(uncertain: Option<UnknownCause>) -> SubmitResult {
    match uncertain {
        Some(cause) => SubmitResult::Unknown(cause),
        None => SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired),
    }
}

/// Reject a committed reply that does not belong to this request: the receipt
/// must name the same operation, digest, producer, ordinal and (when known) kind.
fn correlate(result: &SubmitResult, request: &WireRequest) -> io::Result<()> {
    if let SubmitResult::Committed(receipt) = result
        && (receipt.operation_id != request.operation_id
            || receipt.digest != request.digest
            || receipt.producer != request.producer
            || receipt.ordinal != request.ordinal
            || request
                .op_kind
                .as_ref()
                .is_some_and(|kind| *kind != receipt.op_kind))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "reply receipt does not match the request identity",
        ));
    }
    Ok(())
}

/// Sleep for the current backoff, never past the CURRENT remaining time of the
/// original deadline (recomputed now, not at the top of the loop), and grow it.
/// Returns immediately when the deadline has already passed; the caller's next
/// loop turn then reports the expiry without any extra wait or attempt.
pub(crate) async fn pause(backoff: &mut Duration, deadline: &MonoDeadline) {
    let left = remaining(deadline).unwrap_or_default();
    tokio::time::sleep((*backoff).min(left)).await;
    *backoff = (*backoff * 2).min(BACKOFF_MAX);
}

#[cfg(test)]
mod tests {
    //! Scripted-peer protocol tests of the lane. The peer is a protocol test
    //! double, not a writer: these tests prove client behavior only and say
    //! nothing about queue or storage correctness.
    use super::*;
    use agent_run_domain::storage::{RejectStage, StorageRejection};
    use std::sync::Mutex as StdMutex;
    use tokio::net::UnixListener;

    /// What the scripted peer does with each request, in order.
    #[derive(Clone)]
    #[allow(clippy::large_enum_variant)]
    enum Step {
        /// Reply with a committed receipt echoing the request's identity.
        Commit,
        /// Reply with a receipt whose named field differs from the request:
        /// `operation_id`, `digest`, `ordinal`, `producer` or `op_kind`.
        CommitMismatch(&'static str),
        /// Reply with this result.
        Reply(SubmitResult),
        /// Reply with a correct committed receipt but this operation id in the header.
        ReplyKind(u16),
        /// Read the request, then drop the connection without replying.
        Drop,
    }

    /// Peer behavior: scripted steps, then `rest` forever.
    #[derive(Clone)]
    struct Script {
        steps: Vec<Step>,
        rest: Step,
        /// Pause before answering a hello.
        hello_delay: Duration,
        /// Pause before answering a request.
        reply_delay: Duration,
        /// Added to the echoed sequence number of every hello acknowledgement.
        hello_seq_skew: u64,
        /// Operation id placed in the header of every hello acknowledgement.
        hello_kind: u16,
        /// Ordered log of `H:<attempt>` hellos and `R:<ordinal>` requests.
        events: Arc<StdMutex<Vec<String>>>,
    }

    impl Script {
        /// Steps followed by committing everything.
        fn new(steps: Vec<Step>) -> Self {
            Self {
                steps,
                rest: Step::Commit,
                hello_delay: Duration::ZERO,
                reply_delay: Duration::ZERO,
                hello_seq_skew: 0,
                hello_kind: 0,
                events: Arc::default(),
            }
        }
    }

    /// Records every request body the peer received.
    type Seen = Arc<StdMutex<Vec<(u16, Vec<u8>)>>>;

    /// A receipt derived from the request body, with one field falsified when
    /// `wrong` names it.
    fn receipt_for(body: &[u8], wrong: Option<&str>) -> SubmitResult {
        let value: serde_json::Value = serde_json::from_slice(body).unwrap();
        let pick = |field: &str, other: serde_json::Value| {
            if wrong == Some(field) {
                other
            } else {
                value[field].clone()
            }
        };
        let receipt = serde_json::json!({
            "operation_id": pick("operation_id", serde_json::json!(OperationId::new("b", 1))),
            "digest": pick("digest", serde_json::json!("f".repeat(64))),
            "op_kind": pick("op_kind", serde_json::json!("other_kind")),
            "producer": pick("producer", serde_json::json!({"kind": "cli", "scope": null, "instance": "x"})),
            "ordinal": pick("ordinal", serde_json::json!(9999)),
            "scope": null, "writer_epoch": 1, "commit_seq": 1,
            "committed_at": 0.0, "replayed": false, "result": {}
        });
        SubmitResult::Committed(serde_json::from_value(receipt).unwrap())
    }

    /// Spawn a scripted peer on a fresh socket; returns its dial and request log.
    async fn peer(script: Script) -> (UnixDial, Seen, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("w.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let seen: Seen = Arc::default();
        let log = seen.clone();
        let boot = monotonic::boot_id().unwrap();
        tokio::spawn(async move {
            let mut steps = script.steps.clone().into_iter();
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let script = script.clone();
                loop {
                    let Ok(Some(header)) = wire::read_header(&mut stream, |m, _| {
                        matches!(m, Message::Hello | Message::Request).then_some(1 << 20)
                    })
                    .await
                    else {
                        break;
                    };
                    let body = wire::read_body(&mut stream, &header).await.unwrap();
                    if header.message == Message::Hello {
                        let claim: Hello = serde_json::from_slice(&body).unwrap();
                        script
                            .events
                            .lock()
                            .unwrap()
                            .push(format!("H:{}", claim.attempt.unwrap_or_default()));
                        tokio::time::sleep(script.hello_delay).await;
                        let ok = serde_json::to_vec(&HelloOk {
                            epoch: 1,
                            boot_id: boot.clone(),
                            mono_ns: monotonic::now_ns(),
                        })
                        .unwrap();
                        let _ = wire::write_frame(
                            &mut stream,
                            Message::HelloOk,
                            script.hello_kind,
                            header.seq + script.hello_seq_skew,
                            &ok,
                        )
                        .await;
                        continue;
                    }
                    log.lock().unwrap().push((header.op_kind, body.clone()));
                    script.events.lock().unwrap().push(format!(
                        "R:{}",
                        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["ordinal"]
                    ));
                    tokio::time::sleep(script.reply_delay).await;
                    let step = steps.next().unwrap_or_else(|| script.rest.clone());
                    let mut reply_kind = 0;
                    let result = match step {
                        Step::ReplyKind(kind) => {
                            reply_kind = kind;
                            receipt_for(&body, None)
                        }
                        Step::Commit => receipt_for(&body, None),
                        Step::CommitMismatch(field) => receipt_for(&body, Some(field)),
                        Step::Reply(result) => result,
                        Step::Drop => break,
                    };
                    let reply = serde_json::to_vec(&result).unwrap();
                    if wire::write_frame(
                        &mut stream,
                        Message::Reply,
                        reply_kind,
                        header.seq,
                        &reply,
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
            }
        });
        (UnixDial { path }, seen, directory)
    }

    /// A supervisor-shaped hello claim.
    fn hello() -> Hello {
        Hello {
            protocol: 1,
            role: "supervisor".into(),
            logical: Some("ag-1".into()),
            execution: Some("ag-1".into()),
            attempt: Some("ag-1:1".into()),
            generation: None,
            capability_hex: Some("00".into()),
            finish_guard: None,
        }
    }

    /// A request whose JSON body carries its identity and ordinal so replays are
    /// comparable and the peer can echo a matching receipt.
    fn request(ordinal: u64, budget: Duration) -> WireRequest {
        let (boot, now) = monotonic::now().unwrap();
        let operation_id = OperationId::new(&boot, now);
        let digest = PayloadDigest::of(&serde_json::json!({"ordinal": ordinal}));
        let producer = ProducerId {
            kind: agent_run_domain::storage::ProducerKind::Supervisor,
            scope: Some("ag-1".into()),
            instance: "inst".into(),
        };
        let body = serde_json::to_vec(&serde_json::json!({
            "operation_id": operation_id, "digest": digest, "ordinal": ordinal,
            "producer": producer, "op_kind": "running"
        }))
        .unwrap();
        WireRequest {
            operation_id,
            digest,
            producer,
            ordinal,
            op_kind: Some("running".into()),
            kind: 15,
            body,
            deadline: MonoDeadline {
                boot_id: boot,
                ns: now + budget.as_nanos() as u64,
            },
        }
    }

    /// The ordinal recorded in a logged request body.
    fn logged_ordinal(entry: &(u16, Vec<u8>)) -> u64 {
        serde_json::from_slice::<serde_json::Value>(&entry.1).unwrap()["ordinal"]
            .as_u64()
            .unwrap()
    }

    /// Ordinals are strictly increasing and a final reply is returned as is.
    #[tokio::test]
    async fn ordinals_increase_and_final_replies_pass_through() {
        let (dial, seen, _dir) = peer(Script::new(vec![])).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        for expected in 1..=2u64 {
            let result = lane
                .submit(move |ordinal| {
                    assert_eq!(ordinal, expected);
                    Ok(request(ordinal, Duration::from_secs(5)))
                })
                .await;
            assert!(matches!(result, SubmitResult::Committed(_)));
        }
        let log = seen.lock().unwrap();
        assert_eq!((logged_ordinal(&log[0]), logged_ordinal(&log[1])), (1, 2));
    }

    /// A transient refusal retries the byte-identical body with the same ordinal.
    #[tokio::test]
    async fn transient_refusal_retries_the_same_tuple() {
        let (dial, seen, _dir) = peer(Script::new(vec![
            Step::Reply(SubmitResult::NotEnqueued(NotEnqueuedReason::QueueFull)),
            Step::Reply(SubmitResult::Rejected(StorageRejection::new(
                RejectCode::StorageUnavailable,
                RejectStage::Acquire,
                "busy",
            ))),
        ]))
        .await;
        let lane = ProducerLane::new(dial, hello(), 7);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5))))
            .await;
        assert!(matches!(result, SubmitResult::Committed(_)));
        let log = seen.lock().unwrap();
        assert_eq!(log.len(), 3);
        assert!(
            log.iter()
                .all(|entry| entry == &log[0] && logged_ordinal(entry) == 7)
        );
    }

    /// A refusal before acceptance that persists until the original deadline ends
    /// as `DeadlineExpired` (nothing was applied) and the lane keeps the writer's
    /// LAST refusal and the true attempt count, so the final error reports the
    /// writer's own operation, phase and SQLite codes instead of a generic expiry.
    #[tokio::test]
    async fn refusal_until_expiry_keeps_the_writers_last_facts() {
        use agent_run_domain::{Error, error::PersistenceOperation};
        let busy = Error::Persistence {
            operation: PersistenceOperation::EventAppend,
            elapsed_ms: 5,
            attempts: 1,
            phase: Some(RejectStage::Acquire),
            source: Box::new(Error::Sql(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(5),
                None,
            ))),
        };
        let mut rejection = StorageRejection::from_error(&busy, RejectStage::Acquire);
        rejection.code = RejectCode::StorageUnavailable;
        let mut script = Script::new(vec![]);
        script.rest = Step::Reply(SubmitResult::Rejected(rejection));
        let (dial, seen, _dir) = peer(script).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let pending = lane.submit(|ordinal| Ok(request(ordinal, Duration::from_millis(400))));
        let totals = pending.totals();
        let result = pending.await;
        assert_eq!(
            result,
            SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
        );
        let totals = totals.snapshot();
        assert_eq!(totals.attempts as usize, seen.lock().unwrap().len());
        assert!(totals.attempts >= 2, "{totals:?}");
        let error = crate::persist::into_result_totals::<()>(result, &totals).unwrap_err();
        let Error::Persistence {
            operation,
            elapsed_ms,
            attempts,
            phase,
            ..
        } = error
        else {
            panic!("closed persistence diagnostic expected");
        };
        assert_eq!(operation, PersistenceOperation::EventAppend);
        assert_eq!(phase, Some(RejectStage::Acquire));
        assert_eq!(attempts, totals.attempts);
        assert!(elapsed_ms >= 350, "{elapsed_ms}");
    }

    /// A lost reply is resolved by resubmitting the identical body on a new connection.
    #[tokio::test]
    async fn lost_reply_resubmits_the_identical_body() {
        let (dial, seen, _dir) = peer(Script::new(vec![Step::Drop])).await;
        let lane = ProducerLane::new(dial, hello(), 3);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5))))
            .await;
        assert!(matches!(result, SubmitResult::Committed(_)));
        let log = seen.lock().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0], log[1], "the retry must be byte-identical");
    }

    /// Past the original deadline nothing more is sent: unknown after a possible
    /// send, `DeadlineExpired` when nothing was ever sent.
    #[tokio::test]
    async fn deadline_is_never_renewed() {
        let mut script = Script::new(vec![]);
        script.rest = Step::Drop;
        let (dial, seen, _dir) = peer(script).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_millis(300))))
            .await;
        assert!(matches!(result, SubmitResult::Unknown(_)), "{result:?}");
        let sent = seen.lock().unwrap().len();
        assert!(sent >= 1);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            seen.lock().unwrap().len(),
            sent,
            "no send after the deadline"
        );

        // An already expired request is never sent at all.
        let never = lane
            .submit(|ordinal| {
                let mut r = request(ordinal, Duration::from_millis(1));
                r.deadline.ns -= 1_000_000_000;
                Ok(r)
            })
            .await;
        assert_eq!(
            never,
            SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
        );
        assert_eq!(seen.lock().unwrap().len(), sent);
    }

    /// A lost reply until the original deadline stays UNKNOWN (never success,
    /// never a definite refusal) and the recorded totals are true: several
    /// identical submissions were made and the elapsed time spans the budget.
    #[tokio::test]
    async fn unknown_outcome_keeps_true_totals() {
        let mut script = Script::new(vec![]);
        script.rest = Step::Drop;
        let (dial, seen, _dir) = peer(script).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let pending = lane.submit(|ordinal| Ok(request(ordinal, Duration::from_millis(500))));
        let totals = pending.totals();
        let result = pending.await;
        assert!(matches!(result, SubmitResult::Unknown(_)), "{result:?}");
        let totals = totals.snapshot();
        assert_eq!(
            totals.attempts as usize,
            seen.lock().unwrap().len(),
            "one counted attempt per request that reached the wire"
        );
        assert!(totals.attempts >= 2, "{totals:?}");
        assert!(totals.elapsed >= Duration::from_millis(450), "{totals:?}");
        assert!(totals.last_rejection.is_none());
    }

    /// Dropping the caller's future keeps the head: the operation still completes
    /// and the next operation waits for it.
    #[tokio::test]
    async fn dropping_the_pending_write_does_not_release_the_head() {
        let (dial, seen, _dir) = peer(Script::new(vec![Step::Reply(SubmitResult::NotEnqueued(
            NotEnqueuedReason::QueueFull,
        ))]))
        .await;
        let lane = ProducerLane::new(dial, hello(), 1);
        drop(lane.submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5)))));
        let second = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5))))
            .await;
        assert!(matches!(second, SubmitResult::Committed(_)));
        let log = seen.lock().unwrap();
        let ordinals: Vec<_> = log.iter().map(logged_ordinal).collect();
        assert_eq!(
            ordinals,
            vec![1, 1, 2],
            "the first operation resolves completely before the second starts"
        );
    }

    /// A build failure is a validation rejection and still consumes its ordinal.
    #[tokio::test]
    async fn build_failure_is_a_rejection_that_consumes_the_ordinal() {
        let (dial, seen, _dir) = peer(Script::new(vec![])).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let rejected = lane
            .submit(|_| Err(agent_run_domain::error::invalid("bad envelope")))
            .await;
        assert!(matches!(rejected, SubmitResult::Rejected(_)));
        let ok = lane
            .submit(|ordinal| {
                assert_eq!(ordinal, 2);
                Ok(request(ordinal, Duration::from_secs(5)))
            })
            .await;
        assert!(matches!(ok, SubmitResult::Committed(_)));
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    /// C1: call order is admission order even on a multi-threaded scheduler,
    /// because the position is claimed when `submit` is called.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn call_order_is_admission_order_on_a_multithreaded_scheduler() {
        let (dial, seen, _dir) = peer(Script::new(vec![])).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let calls = 300u64;
        let built: Arc<StdMutex<Vec<u64>>> = Arc::default();
        let mut pending = Vec::new();
        for call in 0..calls {
            let built = built.clone();
            pending.push(lane.submit(move |ordinal| {
                built.lock().unwrap().push(call);
                Ok(request(ordinal, Duration::from_secs(30)))
            }));
        }
        for write in pending {
            assert!(matches!(write.await, SubmitResult::Committed(_)));
        }
        assert_eq!(
            *built.lock().unwrap(),
            (0..calls).collect::<Vec<_>>(),
            "builds must run in call order"
        );
        let ordinals: Vec<_> = seen.lock().unwrap().iter().map(logged_ordinal).collect();
        assert_eq!(ordinals, (1..=calls).collect::<Vec<_>>());
    }

    /// C2: the time a slow handshake consumed is deducted from the ORIGINAL
    /// deadline; the reply wait never extends past it.
    #[tokio::test]
    async fn slow_handshake_consumes_the_original_budget() {
        let mut script = Script::new(vec![]);
        script.hello_delay = Duration::from_millis(350);
        script.reply_delay = Duration::from_secs(2);
        let (dial, _seen, _dir) = peer(script).await;
        let lane =
            ProducerLane::with_timing(dial, hello(), 1, Duration::from_millis(500), BACKOFF_FIRST);
        let started = std::time::Instant::now();
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_millis(600))))
            .await;
        assert!(matches!(result, SubmitResult::Unknown(_)), "{result:?}");
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "the wait must end at the original deadline, took {:?}",
            started.elapsed()
        );
    }

    /// C2: a handshake that outlives the deadline never lets a frame out, and the
    /// honest answer is `DeadlineExpired`.
    #[tokio::test]
    async fn no_frame_is_sent_when_the_handshake_outlives_the_deadline() {
        let mut script = Script::new(vec![]);
        script.hello_delay = Duration::from_millis(600);
        let (dial, seen, _dir) = peer(script).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_millis(300))))
            .await;
        assert_eq!(
            result,
            SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(seen.lock().unwrap().is_empty());
    }

    /// C4: a possibly committed write whose reply was lost stays uncertain
    /// through later transient refusals; the deadline reports `Unknown`.
    #[tokio::test]
    async fn lost_reply_then_queue_full_then_deadline_stays_unknown() {
        let mut script = Script::new(vec![Step::Drop]);
        script.rest = Step::Reply(SubmitResult::NotEnqueued(NotEnqueuedReason::QueueFull));
        let (dial, seen, _dir) = peer(script).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_millis(500))))
            .await;
        assert!(matches!(result, SubmitResult::Unknown(_)), "{result:?}");
        assert!(seen.lock().unwrap().len() >= 2);
    }

    /// C4: after an `Unknown` reply a later non-durable refusal does not replace
    /// the uncertainty.
    #[tokio::test]
    async fn unknown_then_nondurable_refusal_stays_unknown() {
        let (dial, _seen, _dir) = peer(Script::new(vec![
            Step::Reply(SubmitResult::Unknown(UnknownCause::CommitUncertain)),
            Step::Reply(SubmitResult::Rejected(StorageRejection::new(
                RejectCode::StorageRolledBack,
                RejectStage::Statement,
                "rolled back",
            ))),
        ]))
        .await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5))))
            .await;
        assert_eq!(result, SubmitResult::Unknown(UnknownCause::CommitUncertain));
    }

    /// Without prior uncertainty a non-durable refusal is the final answer.
    #[tokio::test]
    async fn clean_nondurable_refusal_is_final() {
        let (dial, seen, _dir) = peer(Script::new(vec![Step::Reply(SubmitResult::NotEnqueued(
            NotEnqueuedReason::AdmissionsClosed,
        ))]))
        .await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5))))
            .await;
        assert_eq!(
            result,
            SubmitResult::NotEnqueued(NotEnqueuedReason::AdmissionsClosed)
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    /// C5: a receipt naming another operation, digest, producer, ordinal or kind
    /// is never accepted as this request's answer; the identical body is
    /// resubmitted and only the matching receipt resolves it.
    #[tokio::test]
    async fn mismatched_receipts_are_never_accepted() {
        for field in ["operation_id", "digest", "ordinal", "producer", "op_kind"] {
            let (dial, seen, _dir) = peer(Script::new(vec![Step::CommitMismatch(field)])).await;
            let lane = ProducerLane::new(dial, hello(), 1);
            let result = lane
                .submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5))))
                .await;
            let SubmitResult::Committed(receipt) = result else {
                panic!("{field}: {result:?}");
            };
            assert_ne!(
                receipt.ordinal, 9999,
                "{field}: the falsified receipt was accepted"
            );
            let log = seen.lock().unwrap();
            assert_eq!(log.len(), 2, "{field}: one falsified answer, one correct");
            assert_eq!(log[0], log[1], "{field}");
        }
    }

    /// C5: a hello acknowledgement that does not echo the hello's sequence number
    /// is a protocol error; no request frame is ever sent over it.
    #[tokio::test]
    async fn hello_acknowledgement_must_echo_the_sequence() {
        let mut script = Script::new(vec![]);
        script.hello_seq_skew = 5;
        let (dial, seen, _dir) = peer(script).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_millis(400))))
            .await;
        assert_eq!(
            result,
            SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    /// A re-hello on an open connection changes only the claimed attempt.
    #[tokio::test]
    async fn repin_sends_a_fresh_hello() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("w.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let claims: Arc<StdMutex<Vec<Hello>>> = Arc::default();
        let log = claims.clone();
        let boot = monotonic::boot_id().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            while let Ok(Some(header)) = wire::read_header(&mut stream, |m, _| {
                matches!(m, Message::Hello | Message::Request).then_some(1 << 20)
            })
            .await
            {
                let body = wire::read_body(&mut stream, &header).await.unwrap();
                if header.message == Message::Hello {
                    log.lock()
                        .unwrap()
                        .push(serde_json::from_slice(&body).unwrap());
                    let ok = serde_json::to_vec(&HelloOk {
                        epoch: 1,
                        boot_id: boot.clone(),
                        mono_ns: 0,
                    })
                    .unwrap();
                    wire::write_frame(&mut stream, Message::HelloOk, 0, header.seq, &ok)
                        .await
                        .unwrap();
                } else {
                    let reply = serde_json::to_vec(&receipt_for(&body, None)).unwrap();
                    wire::write_frame(&mut stream, Message::Reply, 0, header.seq, &reply)
                        .await
                        .unwrap();
                }
            }
        });
        let lane = ProducerLane::new(UnixDial { path }, hello(), 1);
        let first = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5))))
            .await;
        assert!(matches!(first, SubmitResult::Committed(_)));
        lane.repin(Some("ag-1:2".into())).await.unwrap();
        let claims = claims.lock().unwrap();
        assert_eq!(claims.len(), 2);
        assert_eq!(claims[0].attempt.as_deref(), Some("ag-1:1"));
        assert_eq!(claims[1].attempt.as_deref(), Some("ag-1:2"));
        assert_eq!(claims[0].capability_hex, claims[1].capability_hex);
    }

    /// The finish context is exchanged at once (dialing when no connection is
    /// open), dropped with an attempt re-pin, and restored by an idle refresh
    /// on a fresh connection after the broker closed the previous one.
    #[tokio::test]
    async fn finish_guard_hellos_follow_attempt_and_reconnect() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("w.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let claims: Arc<StdMutex<Vec<(usize, Hello)>>> = Arc::default();
        let log = claims.clone();
        let boot = monotonic::boot_id().unwrap();
        tokio::spawn(async move {
            for connection in 0.. {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                while let Ok(Some(header)) = wire::read_header(&mut stream, |m, _| {
                    (m == Message::Hello).then_some(wire::HELLO_MAX)
                })
                .await
                {
                    let body = wire::read_body(&mut stream, &header).await.unwrap();
                    let total = {
                        let mut log = log.lock().unwrap();
                        log.push((connection, serde_json::from_slice(&body).unwrap()));
                        log.len()
                    };
                    let ok = serde_json::to_vec(&HelloOk {
                        epoch: 1,
                        boot_id: boot.clone(),
                        mono_ns: 0,
                    })
                    .unwrap();
                    wire::write_frame(&mut stream, Message::HelloOk, 0, header.seq, &ok)
                        .await
                        .unwrap();
                    if total == 3 {
                        // A broker restart: this connection goes away.
                        break;
                    }
                }
            }
        });
        let context = FinishGuardContext(std::collections::BTreeMap::from([(
            "PROVIDER_TOKEN".to_owned(),
            "synthetic".to_owned(),
        )]));
        let lane = ProducerLane::new(UnixDial { path }, hello(), 1);
        lane.set_finish_guard(Some(context.clone())).await.unwrap();
        lane.repin(Some("ag-1:2".into())).await.unwrap();
        lane.set_finish_guard(Some(context.clone())).await.unwrap();
        lane.refresh().await.unwrap();
        let claims = claims.lock().unwrap();
        let seen: Vec<(usize, Option<&str>, bool)> = claims
            .iter()
            .map(|(connection, hello)| {
                (
                    *connection,
                    hello.attempt.as_deref(),
                    hello.finish_guard.as_ref() == Some(&context),
                )
            })
            .collect();
        assert_eq!(
            seen,
            [
                (0, Some("ag-1:1"), true),
                (0, Some("ag-1:2"), false),
                (0, Some("ag-1:2"), true),
                (1, Some("ag-1:2"), true),
            ]
        );
        assert!(!format!("{:?}", claims[0].1).contains("synthetic"));
    }

    /// C1: dropping a repin that was never polled, or cancelling one that waits for
    /// its predecessor, does not release the barrier: a later call cannot overtake
    /// an earlier one. A executes, B waits for A, the repin waits for B, C last.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropped_or_cancelled_repin_keeps_the_predecessor_barrier() {
        for cancel_after_poll in [false, true] {
            let mut script = Script::new(vec![]);
            script.reply_delay = Duration::from_millis(300);
            let events = script.events.clone();
            let (dial, _seen, _dir) = peer(script).await;
            let lane = ProducerLane::new(dial, hello(), 1);
            let a = lane.submit(|ordinal| Ok(request(ordinal, Duration::from_secs(10))));
            // Let A reach the writer so the connection exists for the re-hello.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let b = lane.submit(|ordinal| Ok(request(ordinal, Duration::from_secs(10))));
            let mut repin = lane.repin(Some("ag-1:2".into()));
            if cancel_after_poll {
                let _ = tokio::time::timeout(Duration::from_millis(20), &mut repin).await;
            }
            drop(repin);
            let c = lane.submit(|ordinal| Ok(request(ordinal, Duration::from_secs(10))));
            for write in [a, b, c] {
                assert!(matches!(write.await, SubmitResult::Committed(_)));
            }
            assert_eq!(
                *events.lock().unwrap(),
                vec!["H:ag-1:1", "R:1", "R:2", "H:ag-1:2", "R:3"],
                "cancel_after_poll={cancel_after_poll}"
            );
        }
    }

    /// C2: a handshake that fails after consuming time is followed by a retry
    /// pause measured against the CURRENT remaining budget, so the lane returns at
    /// the original deadline instead of sleeping past it.
    #[tokio::test]
    async fn failed_handshake_backoff_never_outlives_the_deadline() {
        let mut script = Script::new(vec![]);
        script.hello_delay = Duration::from_millis(200);
        script.hello_seq_skew = 5;
        let (dial, seen, _dir) = peer(script).await;
        let lane = ProducerLane::with_timing(
            dial,
            hello(),
            1,
            Duration::from_secs(2),
            Duration::from_millis(300),
        );
        let started = std::time::Instant::now();
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_millis(400))))
            .await;
        assert_eq!(
            result,
            SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
        );
        assert!(
            started.elapsed() < Duration::from_millis(460),
            "took {:?}",
            started.elapsed()
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    /// C3: a hello acknowledgement that names an operation is a protocol error on
    /// the real client path; no request is ever sent over it.
    #[tokio::test]
    async fn hello_acknowledgement_naming_an_operation_is_refused() {
        let mut script = Script::new(vec![]);
        script.hello_kind = 7;
        let (dial, seen, _dir) = peer(script).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_millis(400))))
            .await;
        assert_eq!(
            result,
            SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    /// C3: a reply naming an operation is never accepted even with a valid
    /// sequence and a matching receipt; the identical body is resubmitted and the
    /// well-formed reply of the second attempt resolves it.
    #[tokio::test]
    async fn reply_naming_an_operation_is_never_accepted() {
        let (dial, seen, _dir) = peer(Script::new(vec![Step::ReplyKind(7)])).await;
        let lane = ProducerLane::new(dial, hello(), 1);
        let result = lane
            .submit(|ordinal| Ok(request(ordinal, Duration::from_secs(5))))
            .await;
        assert!(matches!(result, SubmitResult::Committed(_)));
        let log = seen.lock().unwrap();
        assert_eq!(
            log.len(),
            2,
            "the malformed reply must not have resolved it"
        );
        assert_eq!(log[0], log[1]);
    }
}
