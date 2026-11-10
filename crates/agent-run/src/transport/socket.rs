//! Bounded LF-delimited JSON-RPC 2.0 over authenticated Unix sockets.
use super::frame;
use crate::{Error, Result, dispatch, service::Service};
use agent_run_domain::ProviderStartRequest;
use fs2::FileExt;
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::BufReader,
    net::{UnixListener, UnixStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};

/// Default deadline for one reusable broker-client call, in seconds.
const CLIENT_DEFAULT_TIMEOUT: f64 = 600.0;

/// A reusable, serialized client session for the resident Unix-socket broker.
pub struct BrokerClient {
    /// Broker endpoint selected by the caller.
    socket_path: PathBuf,
    /// Explicit trusted server UID, or the effective UID at each connection.
    expected_server_uid: Option<u32>,
    /// Monotonic request identity and cancellation state for this client.
    next_id: AtomicU64,
    aborted: AtomicBool,
    wake: Arc<tokio::sync::Notify>,
    /// One connection is shared by sequential calls and never by concurrent requests.
    connection: tokio::sync::Mutex<Option<ClientConnection>>,
}

/// The broker's minimal asynchronous start acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerStartResult {
    /// Stable logical agent id assigned by the broker.
    pub agent_id: String,
    /// Exact execution id; absent only from a pre-stable-id broker.
    pub run_id: Option<String>,
    /// Whether this request admitted a new row rather than replaying one.
    pub created: bool,
    /// The provider attempt the admission owns; absent from legacy brokers.
    pub attempt_id: Option<String>,
}

/// The split broker stream retained by one [`BrokerClient`] session.
struct ClientConnection {
    /// Framed request writer.
    output: tokio::net::unix::OwnedWriteHalf,
    /// Framed response reader.
    input: BufReader<tokio::net::unix::OwnedReadHalf>,
}

/// Failure classes used to retry transport loss without retrying domain errors.
enum ClientAttemptError {
    /// The socket or response was lost before a usable broker envelope arrived.
    Transport,
    /// A batch response arrived but was malformed; do not retry it implicitly.
    MalformedBatchResponse,
    /// A hold reply cannot be correlated safely and must not be retried implicitly.
    MalformedHoldResponse,
    /// The broker returned a typed protocol/domain error.
    Domain(Error),
    /// The caller explicitly aborted this client.
    Cancelled,
}

impl BrokerClient {
    /// Creates a lazy broker client that trusts only its effective UID at connection time.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            expected_server_uid: None,
            next_id: AtomicU64::new(1),
            aborted: AtomicBool::new(false),
            wake: Arc::new(tokio::sync::Notify::new()),
            connection: tokio::sync::Mutex::new(None),
        }
    }

    /// Creates a lazy client for an explicit socket and trusted server UID.
    ///
    /// Every connection checks the kernel-reported peer UID before sending a request.
    pub fn with_expected_server_uid(
        socket_path: impl Into<PathBuf>,
        expected_server_uid: u32,
    ) -> Self {
        let mut client = Self::new(socket_path);
        client.expected_server_uid = Some(expected_server_uid);
        client
    }

    /// Sends one object-valued request using Python's default 600-second deadline.
    pub async fn call(&self, method: &str, params: Option<Value>) -> Result<Value> {
        self.call_with_timeout(method, params, CLIENT_DEFAULT_TIMEOUT)
            .await
    }

    /// Sends one request after validating method, object parameters, and deadline.
    /// Resume calls without a key receive one idempotency key shared by reconnect
    /// retries; an explicitly supplied key is preserved, including invalid values
    /// which remain the server's responsibility to reject.
    /// A batch or lineage-hold mutation transport loss stays unknown without
    /// an implicit retry.
    pub async fn call_with_timeout(
        &self,
        method: &str,
        mut params: Option<Value>,
        timeout_seconds: f64,
    ) -> Result<Value> {
        if method.is_empty() {
            return Err(crate::error::invalid("method must be a nonblank string"));
        }
        if params.as_ref().is_some_and(|value| !value.is_object()) {
            return Err(crate::error::invalid("params must be an object or null"));
        }
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(crate::error::invalid("timeout must be positive and finite"));
        }
        if self.aborted.load(Ordering::Acquire) {
            return Err(Error::Runtime("broker call cancelled".into()));
        }
        // A stable agent may advance while a lost response is retried. Pin the
        // resume intent once, outside the reconnect loop, to prevent a second turn.
        if method == "resume"
            && let Some(arguments) = params.as_mut().and_then(Value::as_object_mut)
            && arguments.get("request_id").is_none_or(Value::is_null)
        {
            arguments.insert("request_id".into(), json!(uuid::Uuid::new_v4().to_string()));
        }
        let mut connection = self.connection.lock().await;
        if self.aborted.load(Ordering::Acquire) {
            return Err(Error::Runtime("broker call cancelled".into()));
        }
        for attempt in 0..2 {
            if connection.is_none() {
                match self.connect(timeout_seconds).await {
                    Ok(value) => *connection = Some(value),
                    Err(Error::BrokerUnavailable) if attempt == 0 => continue,
                    Err(error) => return Err(error),
                }
            }
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            match self
                .request(&mut connection, id, method, params.clone(), timeout_seconds)
                .await
            {
                Ok(value) => {
                    if LINEAGE_HOLD_METHODS.contains(&method)
                        && params.as_ref().is_none_or(|params| {
                            !valid_lineage_hold_response(method, &value, params)
                        })
                    {
                        *connection = None;
                        return Err(
                            Error::Runtime(
                                if matches!(
                                    method,
                                    "_lineage_hold_acquire" | "_lineage_hold_release"
                                ) {
                                    "lineage hold outcome unknown"
                                } else {
                                    "invalid lineage hold response"
                                }
                                .into(),
                            ),
                        );
                    }
                    if method == "batch_receipt"
                        && params
                            .as_ref()
                            .is_none_or(|params| !valid_batch_receipt_response(&value, params))
                    {
                        *connection = None;
                        return Err(Error::Runtime(
                            "invalid batch receipt lookup response".into(),
                        ));
                    }
                    if method == "transcript_exact"
                        && params
                            .as_ref()
                            .is_none_or(|params| !valid_transcript_exact_response(&value, params))
                    {
                        *connection = None;
                        return Err(Error::Runtime("invalid exact transcript response".into()));
                    }
                    if method == "initial_task"
                        && params
                            .as_ref()
                            .is_none_or(|params| !valid_initial_task_response(&value, params))
                    {
                        *connection = None;
                        return Err(Error::Runtime("invalid initial task response".into()));
                    }
                    if method == "status"
                        && params
                            .as_ref()
                            .is_none_or(|params| !valid_status_response(&value, params))
                    {
                        *connection = None;
                        return Err(Error::Runtime("invalid status response".into()));
                    }
                    if method == "capabilities"
                        && params
                            .as_ref()
                            .is_some_and(|params| !valid_capabilities_scope(&value, params))
                    {
                        *connection = None;
                        return Err(Error::Runtime("invalid capabilities store identity".into()));
                    }
                    if method == "start_batch"
                        && params
                            .as_ref()
                            .is_none_or(|params| !valid_batch_response(&value, params))
                    {
                        *connection = None;
                        return Err(batch_response_error(
                            method,
                            crate::error::invalid("invalid batch receipt"),
                        ));
                    }
                    if method == "history_pins"
                        && params
                            .as_ref()
                            .is_none_or(|params| !valid_history_pin_response(&value, params))
                    {
                        *connection = None;
                        return Err(Error::Runtime(
                            "history pin result is invalid; reconcile release by pin_id".into(),
                        ));
                    }
                    return Ok(value);
                }
                Err(ClientAttemptError::Domain(error)) => return Err(error),
                Err(ClientAttemptError::MalformedBatchResponse) => {
                    *connection = None;
                    return Err(batch_response_error(
                        method,
                        Error::Runtime("invalid batch broker response".into()),
                    ));
                }
                Err(ClientAttemptError::MalformedHoldResponse) => {
                    *connection = None;
                    return Err(Error::Runtime(
                        if matches!(method, "_lineage_hold_acquire" | "_lineage_hold_release") {
                            "lineage hold outcome unknown"
                        } else {
                            "invalid lineage hold response"
                        }
                        .into(),
                    ));
                }
                Err(ClientAttemptError::Cancelled) => {
                    *connection = None;
                    if matches!(method, "_lineage_hold_acquire" | "_lineage_hold_release") {
                        return Err(Error::Runtime("lineage hold outcome unknown".into()));
                    }
                    return Err(batch_response_error(
                        method,
                        Error::Runtime("broker call cancelled".into()),
                    ));
                }
                Err(ClientAttemptError::Transport) => {
                    *connection = None;
                    if method == "start_batch" {
                        return Err(batch_response_error(method, Error::BrokerUnavailable));
                    }
                    if matches!(method, "_lineage_hold_acquire" | "_lineage_hold_release") {
                        return Err(Error::Runtime("lineage hold outcome unknown".into()));
                    }
                    if self.aborted.load(Ordering::Acquire) {
                        return Err(Error::Runtime("broker call cancelled".into()));
                    }
                    if attempt == 1 {
                        return Err(Error::BrokerUnavailable);
                    }
                }
            }
        }
        unreachable!("the bounded broker retry loop always returns")
    }

    /// Serializes the strict schema-2 provider start request the broker
    /// dispatcher accepts (never the retired `runtime` shape) and rejects
    /// malformed broker acknowledgements.
    pub async fn start(&self, request: &ProviderStartRequest) -> Result<BrokerStartResult> {
        let params = serde_json::to_value(request)?;
        let result = self.call("start", Some(params)).await?;
        let Some(object) = result.as_object() else {
            return Err(Error::Runtime(
                "broker returned an invalid start result".into(),
            ));
        };
        let Some(agent_id) = object.get("agent_id").and_then(Value::as_str) else {
            return Err(Error::Runtime(
                "broker returned an invalid start result".into(),
            ));
        };
        let Some(created) = object.get("created").and_then(Value::as_bool) else {
            return Err(Error::Runtime(
                "broker returned an invalid start result".into(),
            ));
        };
        let attempt_id = match object.get("attempt_id") {
            None | Some(Value::Null) => None,
            Some(Value::String(attempt)) => Some(attempt.clone()),
            Some(_) => {
                return Err(Error::Runtime(
                    "broker returned an invalid start result".into(),
                ));
            }
        };
        let run_id = match object.get("run_id") {
            None => None,
            Some(Value::String(id)) => Some(id.clone()),
            Some(_) => return Err(Error::Runtime("broker returned an invalid run_id".into())),
        };
        Ok(BrokerStartResult {
            agent_id: agent_id.into(),
            run_id,
            created,
            attempt_id,
        })
    }

    /// Aborts an in-flight connect/read and prevents later calls from opening work.
    pub fn abort(&self) {
        self.aborted.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }

    /// Closes the retained stream while leaving this client available for reconnects.
    pub async fn close(&self) {
        *self.connection.lock().await = None;
    }

    /// Opens and authenticates one broker stream, interruptible by [`Self::abort`].
    async fn connect(&self, timeout_seconds: f64) -> Result<ClientConnection> {
        let notified = self.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.aborted.load(Ordering::Acquire) {
            return Err(Error::Runtime("broker call cancelled".into()));
        }
        let stream = tokio::select! {
            biased;
            _ = &mut notified => return Err(Error::Runtime("broker call cancelled".into())),
            result = tokio::time::timeout(Duration::from_secs_f64(timeout_seconds), UnixStream::connect(&self.socket_path)) => {
                result.map_err(|_| Error::BrokerUnavailable)?.map_err(|_| Error::BrokerUnavailable)?
            }
        };
        let expected_uid = self.expected_server_uid.unwrap_or_else(|| {
            // SAFETY: geteuid is a read-only process identity query.
            unsafe { libc::geteuid() }
        });
        if stream.peer_cred()?.uid() != expected_uid {
            return Err(crate::error::invalid("broker belongs to another user"));
        }
        let (input, output) = stream.into_split();
        Ok(ClientConnection {
            output,
            input: BufReader::new(input),
        })
    }

    /// Writes one request and maps the matching response envelope to a value.
    async fn request(
        &self,
        connection: &mut Option<ClientConnection>,
        id: u64,
        method: &str,
        params: Option<Value>,
        timeout_seconds: f64,
    ) -> std::result::Result<Value, ClientAttemptError> {
        let Some(connection) = connection.as_mut() else {
            return Err(ClientAttemptError::Transport);
        };
        let mut request = json!({"jsonrpc":"2.0","id":id,"method":method});
        if let Some(params) = params {
            request["params"] = params;
        }
        let notified = self.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.aborted.load(Ordering::Acquire) {
            return Err(ClientAttemptError::Cancelled);
        }
        let written = tokio::select! {
            biased;
            _ = &mut notified => return Err(ClientAttemptError::Cancelled),
            result = tokio::time::timeout(Duration::from_secs_f64(timeout_seconds), frame::write(&mut connection.output, &request, MAX_FRAME)) => result,
        };
        match written {
            Ok(Ok(())) => {}
            _ => return Err(ClientAttemptError::Transport),
        }
        let notified = self.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.aborted.load(Ordering::Acquire) {
            return Err(ClientAttemptError::Cancelled);
        }
        let raw = tokio::select! {
            biased;
            _ = &mut notified => return Err(ClientAttemptError::Cancelled),
            result = tokio::time::timeout(Duration::from_secs_f64(timeout_seconds), frame::read(&mut connection.input, MAX_FRAME)) => result,
        };
        let raw = match raw {
            Ok(Ok(Some(raw))) => raw,
            _ => return Err(ClientAttemptError::Transport),
        };
        if !valid_raw_batch_response(method, &raw) {
            return Err(ClientAttemptError::MalformedBatchResponse);
        }
        if !valid_raw_lineage_hold_response(method, &raw) {
            return Err(ClientAttemptError::MalformedHoldResponse);
        }
        let malformed = || {
            if matches!(method, "start_batch" | "batch_receipt") {
                ClientAttemptError::MalformedBatchResponse
            } else if LINEAGE_HOLD_METHODS.contains(&method) {
                ClientAttemptError::MalformedHoldResponse
            } else {
                ClientAttemptError::Transport
            }
        };
        let value: Value = serde_json::from_slice(&raw).map_err(|_| malformed())?;
        if value.get("id") != Some(&json!(id))
            || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        {
            return Err(malformed());
        }
        if matches!(method, "start_batch" | "batch_receipt")
            && value.get("error").is_some()
            && value.get("result").is_some()
        {
            return Err(malformed());
        }
        if LINEAGE_HOLD_METHODS.contains(&method)
            && value.get("error").is_some()
            && value.get("result").is_some()
        {
            return Err(malformed());
        }
        if let Some(error) = value.get("error") {
            let Some(object) = error.as_object() else {
                return Err(malformed());
            };
            if matches!(method, "start_batch" | "batch_receipt")
                && (object.get("code").and_then(Value::as_i64).is_none()
                    || object.get("message").and_then(Value::as_str).is_none()
                    || !valid_batch_precondition_error(method, object))
            {
                return Err(malformed());
            }
            if LINEAGE_HOLD_METHODS.contains(&method) && !valid_lineage_hold_error(object) {
                return Err(ClientAttemptError::MalformedHoldResponse);
            }
            let error = broker_error(object);
            if method == "batch_receipt" && matches!(error, Error::BatchOutcomeUnknown { .. }) {
                return Err(ClientAttemptError::MalformedBatchResponse);
            }
            return Err(ClientAttemptError::Domain(error));
        }
        value.get("result").cloned().ok_or_else(malformed)
    }
}

/// Python-compatible upper bound for one newline-delimited JSON RPC message.
pub const MAX_FRAME: usize = agent_run_domain::observation::SOCKET_MAX_FRAME_BYTES;
const MAX_CONNECTIONS: usize = 32;
const MAX_PENDING_REQUESTS: usize = 64;
const CONTROL_FRAME_DEADLINE: Duration = Duration::from_millis(500);
/// Interval between content-digest checks for a running broker's configuration.
const CONFIG_RELOAD_INTERVAL: Duration = Duration::from_secs(60);
const CONTROL_METHODS: &[&str] = &[
    "start",
    "start_batch",
    "history_pins",
    "_lineage_hold_acquire",
    "_lineage_hold_release",
    "resume",
    "cancel",
    "steer",
    "start_pool",
    "pool_post",
    "pool_replace",
    "_hook_bind",
];
/// Steady database maintenance interval: expiry found no work and compaction
/// reclaimed no pages, so nothing eligible remains.
const DATABASE_STEADY: Duration = Duration::from_secs(3600);
/// Database maintenance interval while a pass removed rows or reclaimed pages:
/// a bounded backlog keeps draining at one-second batches.
const DATABASE_BACKLOG: Duration = Duration::from_secs(1);
/// Steady filesystem retention interval: the last pass removed nothing and
/// reported no further pass.
const FILESYSTEM_STEADY: Duration = Duration::from_secs(3600);
/// Filesystem retention interval while a pass removed entries or reported that
/// another pass is needed; database outcomes never influence it.
const FILESYSTEM_BACKLOG: Duration = Duration::from_secs(1);
/// Retry interval after a maintenance pass deferred on writer contention or
/// failed; long enough that a busy broker is never hammered by its own cleanup.
const MAINTENANCE_RETRY: Duration = Duration::from_secs(60);

/// Returns the next database maintenance delay after one pass removed
/// `removed` history/receipt/producer rows plus reclaimed-page batches.
///
/// `removed == 0` means this pass committed no eligible removal/reclamation,
/// so database maintenance returns to its hourly steady cadence. A receipt
/// backlog keeps draining even when ordinary history is idle. Any filesystem
/// outcome is invisible here: the two
/// schedules are independent, so a filesystem backlog cannot force a database
/// writer transaction every second.
fn database_maintenance_delay(removed: usize) -> Duration {
    if removed == 0 {
        DATABASE_STEADY
    } else {
        DATABASE_BACKLOG
    }
}

/// Returns the next filesystem retention delay after one pass removed
/// `removed` entries or reported that another pass is needed.
///
/// `removed == 0` means the pass found no actionable work, so retention
/// returns to its hourly steady cadence. Database outcomes are invisible here.
fn filesystem_maintenance_delay(removed: usize) -> Duration {
    if removed == 0 {
        FILESYSTEM_STEADY
    } else {
        FILESYSTEM_BACKLOG
    }
}

/// The two broker maintenance schedules' next due times.
///
/// Database expiry/compaction and filesystem retention run on independent
/// cadences: each pass outcome advances only its own schedule, so a filesystem
/// backlog drains in one-second passes without forcing a database writer
/// transaction every second, and a database backlog never starves filesystem
/// cleanup. Both schedules start due immediately, matching the startup pass.
#[derive(Debug)]
struct MaintenanceSchedules {
    /// Next due time of the database expiry and compaction pass.
    next_database: tokio::time::Instant,
    /// Next due time of the filesystem retention pass.
    next_filesystem: tokio::time::Instant,
}

impl MaintenanceSchedules {
    /// Returns both schedules due at `at`, so the first broker cycle runs each
    /// pass once at startup.
    fn starting_at(at: tokio::time::Instant) -> Self {
        Self {
            next_database: at,
            next_filesystem: at,
        }
    }

    /// The earlier of both due times; the worker sleeps until it.
    fn next_due(&self) -> tokio::time::Instant {
        self.next_database.min(self.next_filesystem)
    }

    /// Advances only the database schedule after one pass completed at `at`
    /// having removed `removed` rows plus reclaimed-page batches. A deferred
    /// or failed pass is recorded by the caller through [`MAINTENANCE_RETRY`].
    fn database_pass(&mut self, at: tokio::time::Instant, removed: usize) {
        self.next_database = at + database_maintenance_delay(removed);
    }

    /// Advances only the filesystem schedule after one pass completed at `at`
    /// having removed `removed` entries or reported that another pass is
    /// needed. A deferred or failed pass is recorded by the caller through
    /// [`MAINTENANCE_RETRY`].
    fn filesystem_pass(&mut self, at: tokio::time::Instant, removed: usize) {
        self.next_filesystem = at + filesystem_maintenance_delay(removed);
    }
}

/// Prints one structured, secret-free diagnostic line for a failed
/// maintenance pass.
///
/// `operation` and `stage` are static labels identifying the failing pass.
/// A SQLite failure adds only numeric result codes and static code names via
/// [`agent_run_store::retention::sqlite_codes`]: SQLite messages, SQL text,
/// paths and payloads are never printed because they can echo configuration
/// or transcript content. Writer contention (busy, locked or interrupted) is
/// reported as a deferral; every other failure keeps its stable public kind.
fn maintenance_diagnostic(operation: &str, stage: &str, error: &Error) {
    match crate::state::retention::sqlite_codes(error) {
        Some(codes) => {
            let extended = codes
                .extended_name
                .map(|name| format!(" {name}"))
                .unwrap_or_default();
            let disposition = if crate::state::retention::is_writer_contention(error) {
                "deferred"
            } else {
                "failed"
            };
            eprintln!(
                "{operation} maintenance {disposition}: stage={stage} sqlite_primary={} sqlite_primary_name={} sqlite_extended={}{}",
                codes.primary, codes.primary_name, codes.extended, extended
            );
        }
        None => eprintln!(
            "{operation} maintenance failed: stage={stage} error={}",
            error.public().kind
        ),
    }
}

/// Prunes one receipt page, checkpointing only when history and receipts made no progress.
/// Returns committed row credit for the database cadence. The optional checkpoint
/// gets its own fresh250ms budget after pruning; SQL failures still propagate.
async fn receipt_maintenance(
    lane: &agent_run_core::broker_writer::BrokerLane,
    removed: usize,
) -> Result<usize> {
    let (boot_id, now_ns) = agent_run_platform::monotonic::now()?;
    let receipt_rows = lane
        .local(
            crate::state::ops::PruneReceipts {
                at: crate::domain::now(),
                boot_id: boot_id.clone(),
                now_ns,
            },
            agent_run_core::broker_writer::BROKER_BUDGET,
        )
        .await?;
    if removed.saturating_add(receipt_rows) == 0 {
        let (boot_id, now_ns) = agent_run_platform::monotonic::now()?;
        let checkpoint = lane
            .handle()
            .maintain(
                crate::state::ops::MaintenanceOp::CheckpointWal { truncate: false },
                agent_run_domain::storage::MonoDeadline {
                    boot_id,
                    ns: now_ns.saturating_add(250_000_000),
                },
            )
            .await;
        match checkpoint {
            agent_run_domain::storage::MaintenanceOutcome::Interrupted => {
                return Err(Error::Runtime("history checkpoint interrupted".into()));
            }
            agent_run_domain::storage::MaintenanceOutcome::Refused(error) => {
                return Err(error.into_error());
            }
            agent_run_domain::storage::MaintenanceOutcome::Checkpoint {
                busy,
                log,
                checkpointed,
            } if busy != 0 || checkpointed < log => {
                eprintln!(
                    "history checkpoint deferred: busy={busy} backlog={}",
                    log.saturating_sub(checkpointed)
                );
            }
            _ => {}
        }
    }
    Ok(removed.saturating_add(receipt_rows))
}

/// Bounds one socket server's connections, request queue, and frame deadlines.
#[derive(Debug, Clone)]
pub struct ServeOptions {
    /// Maximum simultaneous client handlers, including the reserved control slot.
    pub max_connections: usize,
    /// Maximum pending requests per ordinary/control lane.
    pub max_pending_requests: usize,
    /// Absolute execution deadline for one request.
    pub request_timeout: Duration,
    /// Absolute deadline for receiving the next ordinary frame.
    pub idle_timeout: Duration,
}

/// Explicit system-only writer and launcher configuration supplied by the
/// authenticated service bootstrap, never by an operator request.
pub struct SystemServeOptions {
    /// Protected group-authorized writer endpoint for target supervisors.
    pub writer_socket: PathBuf,
    /// Extra operator endpoints, each controlled by its directory permissions.
    pub operator_sockets: Vec<PathBuf>,
    /// Launcher bound to the authenticated root coordinator.
    pub launcher: Arc<dyn agent_run_core::system_launch::SystemLauncher>,
}

impl Default for ServeOptions {
    /// Returns the production limits matching the Python socket server.
    fn default() -> Self {
        Self {
            max_connections: MAX_CONNECTIONS,
            max_pending_requests: MAX_PENDING_REQUESTS,
            request_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
        }
    }
}

/// Bounded ownership lanes for ordinary requests.
///
/// Admission and lifecycle commands use `control`; all other ordinary calls
/// use `read`. Long polls deliberately acquire neither permit: their service
/// opens and drops SQLite connections around each poll, so no transaction is
/// retained while sleeping.
#[derive(Clone)]
struct Lanes {
    control: Arc<Semaphore>,
    read: Arc<Semaphore>,
}

impl Lanes {
    /// Creates the two independently bounded Python-compatible work queues.
    fn new(max_pending_requests: usize) -> Self {
        let capacity = max_pending_requests.saturating_add(1);
        Self {
            control: Arc::new(Semaphore::new(capacity)),
            read: Arc::new(Semaphore::new(capacity)),
        }
    }

    /// Reserves a lane without queueing a socket handler behind overload.
    fn try_acquire(&self, method: &str) -> Option<OwnedSemaphorePermit> {
        let lane = if CONTROL_METHODS.contains(&method) {
            &self.control
        } else {
            &self.read
        };
        lane.clone().try_acquire_owned().ok()
    }
}

/// Owns one batch control slot and records an interrupted task without payloads.
struct BatchControlGuard {
    /// Released only when the owned dispatch future ends or is dropped.
    _permit: OwnedSemaphorePermit,
    /// Set only after the dispatch future returns normally.
    completed: bool,
}

impl Drop for BatchControlGuard {
    /// The logger is best effort and must not panic during another unwind.
    fn drop(&mut self) {
        if !self.completed {
            let _ = std::panic::catch_unwind(|| {
                if let Some(logger) = agent_run_core::logging::configured() {
                    logger.log(
                        agent_run_core::logging::Level::Warning,
                        "batch_dispatch_interrupted outcome=unknown",
                    );
                }
            });
        }
    }
}

/// Keeps a control slot owned by work even after its caller stops awaiting it.
fn spawn_controlled<T: Send + 'static>(
    permit: OwnedSemaphorePermit,
    work: impl std::future::Future<Output = T> + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    tokio::spawn(async move {
        let mut guard = BatchControlGuard {
            _permit: permit,
            completed: false,
        };
        let output = work.await;
        guard.completed = true;
        output
    })
}
/// Removes only the listener inode this broker created while retaining its lock.
pub(crate) struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Ok(m) = std::fs::symlink_metadata(&self.path)
            && m.dev() == self.device
            && m.ino() == self.inode
            && m.file_type().is_socket()
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
/// Builds a JSON-RPC error, omitting `data` when this failure class has none.
fn err(id: Value, code: i32, message: &str, data: Option<Value>) -> Value {
    let mut error = json!({"code":code,"message":message});
    if let Some(data) = data {
        error["data"] = data;
    }
    json!({"jsonrpc":"2.0","id":id,"error":error})
}
/// Turns one domain failure into the Python socket's stable domain envelope.
fn domain_err(id: Value, error: &Error) -> Value {
    let public = error.public();
    // This socket-only code is stable for a bounded page retry; it is not a
    // new public machine code or part of MCP discovery.
    if let Error::Broker {
        broker_error_code: Some(code),
        ..
    } = error
        && code == dispatch::CLI_TRANSCRIPT_PAGE_CHANGED_CODE
    {
        return err(
            id,
            -32000,
            &public.message,
            Some(json!({"code":code,"message":public.message})),
        );
    }
    if public.kind == "PreconditionFailed" && public.reason.is_none() {
        return domain_err(
            id,
            &Error::BatchOutcomeUnknown {
                source: Box::new(crate::error::invalid("invalid precondition refusal")),
            },
        );
    }
    let mut data = json!({"code":public.kind,"message":public.message});
    if let Some(reason) = public.reason {
        data["reason"] = json!(reason);
    }
    err(id, -32000, &public.message, Some(data))
}

/// Preserves batch uncertainty when dispatch fails or outlives the socket wait.
async fn batch_control_response(
    task: tokio::task::JoinHandle<Option<Value>>,
    deadline: Duration,
    id: Value,
    has_id: bool,
) -> Option<Value> {
    let failure = match tokio::time::timeout(deadline, task).await {
        Ok(Ok(response)) => return response,
        Ok(Err(_)) => (-32000, "batch outcome unknown; retry the same request"),
        Err(_) => (
            -32002,
            "API request deadline exceeded; batch outcome unknown",
        ),
    };
    has_id.then(|| {
        err(
            id,
            failure.0,
            failure.1,
            Some(json!({
                "code": agent_run_domain::MachineCode::BatchOutcomeUnknown.as_str(),
                "message": failure.1,
            })),
        )
    })
}
/// Validates and dispatches one decoded JSON-RPC object without socket I/O.
///
/// Notifications return `None`; malformed requests use the JSON-RPC envelope
/// required by the Python broker. The caller owns `service` and therefore the
/// SQLite connection lifetime remains outside this pure transport boundary.
pub async fn respond(service: &Service, v: Value) -> Option<Value> {
    respond_with_access(service, v, SocketAccess::Private).await
}

/// Group-socket access is granted by the operating system; worker methods
/// retain their existing attempt-bound capability checks.
#[derive(Clone, Copy)]
enum SocketAccess {
    /// The existing same-effective-UID API, including capability-bound worker calls.
    Private,
    /// Filesystem-group-authorized operator calls, never private worker calls.
    Operator,
    /// The shared service endpoint also exposes capability-bound worker calls.
    SystemGroup,
}

/// Narrow CLI compatibility reads excluded from public tool discovery.
const CLI_READ_METHODS: &[&str] = &[
    "_cli_answer",
    "_cli_answer_chunk",
    "_cli_transcript_select",
    "_cli_transcript_page",
    "_cli_transcript_status",
    "_hook_context",
    "_hook_bind",
    "_runtime_retention",
    "_system_policy",
];

/// Operator-only restart controls; deliberately absent from MCP discovery.
const SERVICE_RESTART_METHODS: &[&str] = &[
    "_service_restart_begin",
    "_service_restart_status",
    "_service_restart_failed",
    "_service_restart_claim",
];

/// Generic hold controls are operator-socket methods, never MCP model tools.
const LINEAGE_HOLD_METHODS: &[&str] = &[
    "_lineage_hold_acquire",
    "_lineage_hold_lookup",
    "_lineage_hold_list",
    "_lineage_hold_release",
];

/// Returns only the broker's fixed policy verdict for a stored execution.
/// No caller-supplied file path, credential or configuration is accepted.
fn system_policy(service: &Service, params: Value) -> Result<Value> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        execution: crate::domain::AgentId,
    }
    let request: Request = serde_json::from_value(params)
        .map_err(|_| crate::error::invalid("invalid system policy request"))?;
    Ok(serde_json::to_value(
        service.system_current_policy(&request.execution),
    )?)
}

/// Exposes configured runtime roots and path references, but no file contents.
/// An invalid or unreadable configuration never authorizes account-local cleanup.
fn runtime_retention_config(service: &Service, params: &Value) -> Result<Value> {
    let object = params
        .as_object()
        .ok_or_else(|| crate::error::invalid("runtime retention parameters must be an object"))?;
    if !object.is_empty() {
        return Err(crate::error::invalid("unknown runtime retention parameter"));
    }
    let (config, _) = agent_run_config::provider_config::ProviderConfig::load(&service.home)?;
    let roots: Vec<PathBuf> = config
        .harnesses
        .values()
        .map(|harness| harness.home.clone())
        .collect();
    let paths = agent_run_core::housekeeping::config_paths(&service.home)?;
    Ok(json!({
        "roots": serde_json::to_value(roots)?,
        "protected_paths": serde_json::to_value(paths)?,
    }))
}

/// Validates the kernel peer before reading requests or replying to overload.
/// Operator membership is enforced by pathname DAC, including supplementary groups.
fn authenticate_socket(stream: &UnixStream, access: SocketAccess) -> Result<()> {
    let credentials = stream.peer_cred()?;
    match access {
        SocketAccess::Private => {
            // SAFETY: geteuid only reads the effective process identity.
            if credentials.uid() != unsafe { libc::geteuid() } {
                return Err(crate::error::invalid("socket peer has another user ID"));
            }
        }
        SocketAccess::Operator | SocketAccess::SystemGroup => {
            if credentials.pid().is_none_or(|pid| pid <= 0) {
                return Err(crate::error::invalid("operator peer identity unavailable"));
            }
        }
    }
    Ok(())
}

/// Uses the normal protocol envelope while fencing private methods before dispatch.
async fn respond_with_access(service: &Service, v: Value, access: SocketAccess) -> Option<Value> {
    if v.is_array() {
        return Some(err(
            Value::Null,
            -32600,
            "batch requests are not supported",
            None,
        ));
    }
    let Some(obj) = v.as_object() else {
        return Some(err(Value::Null, -32600, "invalid request", None));
    };
    let absent_id = !obj.contains_key("id");
    let id = obj.get("id").cloned().unwrap_or(Value::Null);
    let valid_id = id.is_null() || id.is_string() || id.is_number();
    if !valid_id {
        return Some(err(Value::Null, -32600, "invalid request id", None));
    }
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || !obj.get("method").is_some_and(Value::is_string)
    {
        return if absent_id {
            None
        } else {
            Some(err(id, -32600, "invalid request", None))
        };
    }
    let method = obj.get("method").and_then(Value::as_str).unwrap_or("");
    let notification = absent_id;
    // Keep a bounded response envelope for a committed batch receipt.
    if matches!(
        method,
        "start_batch"
            | "status"
            | "batch_receipt"
            | "transcript_exact"
            | "initial_task"
            | "_lineage_hold_acquire"
            | "_lineage_hold_lookup"
            | "_lineage_hold_list"
            | "_lineage_hold_release"
    ) && serde_json::to_vec(&id)
        .is_ok_and(|bytes| bytes.len() > agent_run_domain::observation::MAX_RPC_ID_BYTES)
    {
        return if notification {
            None
        } else {
            Some(err(Value::Null, -32600, "request id is too large", None))
        };
    }
    let params = obj.get("params").cloned().unwrap_or_else(|| json!({}));
    if !params.is_object() {
        return if notification {
            None
        } else {
            Some(err(id, -32602, "params must be an object", None))
        };
    }
    // Do not create an unacknowledged hold or release through a notification.
    if notification && matches!(method, "_lineage_hold_acquire" | "_lineage_hold_release") {
        return None;
    }
    let known = dispatch::is_tool(method)
        || CLI_READ_METHODS.contains(&method)
        || SERVICE_RESTART_METHODS.contains(&method)
        || LINEAGE_HOLD_METHODS.contains(&method)
        || [
            "tools",
            "ping",
            "wait",
            agent_run_domain::worker::METHOD,
            agent_run_domain::worker::TOOL_METHOD,
            agent_run_domain::worker::CATALOG_METHOD,
        ]
        .contains(&method);
    let worker_method = [
        agent_run_domain::worker::METHOD,
        agent_run_domain::worker::TOOL_METHOD,
        agent_run_domain::worker::CATALOG_METHOD,
    ]
    .contains(&method);
    let known = known
        && (method != "_system_policy" || matches!(access, SocketAccess::SystemGroup))
        && match access {
            SocketAccess::Private => true,
            SocketAccess::Operator => !worker_method,
            SocketAccess::SystemGroup => true,
        };
    let response = if !known {
        err(id, -32601, "method not found", None)
    } else {
        let result = if method == "_runtime_retention" {
            runtime_retention_config(service, &params)
        } else if method == "_system_policy" {
            system_policy(service, params)
        } else {
            dispatch::call(service, method, params).await
        };
        match result {
            Ok(value) => json!({"jsonrpc":"2.0","id":id,"result":value}),
            Err(e) => {
                if e.rpc_code() == -32602 {
                    // Invalid-params responses stay bare for ordinary
                    // validation; a more specific typed class (for example
                    // `PathEscapeError`) keeps its code in `data`.
                    let public = e.public();
                    let data = (public.kind != "ValidationError")
                        .then(|| json!({"code":public.kind,"message":public.message}));
                    err(id, -32602, &public.message, data)
                } else {
                    domain_err(id, &e)
                }
            }
        }
    };
    if notification { None } else { Some(response) }
}
/// Runs one connection with framed parsing, lane admission, and ordered writes.
/// Oversized successful answers shed inline content before the frame cap is
/// enforced; all other reply and lifecycle/deadline behavior is preserved.
async fn connection(
    stream: UnixStream,
    service: Service,
    lanes: Lanes,
    control_only: bool,
    options: ServeOptions,
    access: SocketAccess,
) -> Result<()> {
    authenticate_socket(&stream, access)?;
    let (input, mut output) = stream.into_split();
    let mut input = BufReader::new(input);
    loop {
        let deadline = if control_only {
            CONTROL_FRAME_DEADLINE
        } else {
            options.idle_timeout
        };
        let raw = match tokio::time::timeout(deadline, frame::read(&mut input, MAX_FRAME)).await {
            Ok(Ok(Some(raw))) => raw,
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(_)) => {
                let _ = frame::write(
                    &mut output,
                    &err(Value::Null, -32700, "request exceeds maximum size", None),
                    MAX_FRAME,
                )
                .await;
                return Ok(());
            }
            Err(_) => return Ok(()),
        };
        let mut answer_response = false;
        let mut batch_response = false;
        let response = match serde_json::from_slice::<Value>(&raw) {
            Ok(mut value) => {
                if !valid_start_frame(
                    &raw,
                    value.get("method").and_then(Value::as_str).unwrap_or(""),
                ) {
                    // Keep envelope/notification validation in the normal responder;
                    // this marker can never decode as a valid admission request.
                    value["params"] = json!({"invalid_start_frame":true});
                }
                let method = value.get("method").and_then(Value::as_str).unwrap_or("");
                answer_response = method == "answer";
                batch_response = method == "start_batch";
                if control_only && !CONTROL_METHODS.contains(&method) {
                    let id = value.get("id").cloned().unwrap_or(Value::Null);
                    if value.get("id").is_some() {
                        Some(err(
                            id,
                            -32001,
                            "API capacity is reserved for control requests",
                            None,
                        ))
                    } else {
                        None
                    }
                } else {
                    let long_poll = method == "wait"
                        || (method == "list_agents"
                            && value
                                .get("params")
                                .and_then(|params| params.get("wait_seconds"))
                                .and_then(Value::as_f64)
                                .is_some_and(|seconds| seconds > 0.0));
                    if long_poll {
                        respond_with_access(&service, value, access).await
                    } else if let Some(permit) = lanes.try_acquire(method) {
                        let request_id = value.get("id").cloned().unwrap_or(Value::Null);
                        let has_id = value.get("id").is_some();
                        if method == "start_batch" {
                            let service = service.clone();
                            // The detached operation retains its control slot even
                            // when a caller's wait expires or its socket closes.
                            let task = spawn_controlled(permit, async move {
                                respond_with_access(&service, value, access).await
                            });
                            batch_control_response(
                                task,
                                options.request_timeout,
                                request_id,
                                has_id,
                            )
                            .await
                        } else if matches!(
                            method,
                            "_lineage_hold_acquire" | "_lineage_hold_release"
                        ) {
                            let service = service.clone();
                            // An accepted mutation keeps its slot and completes even
                            // if the socket caller stops waiting for its response.
                            let task = tokio::spawn(async move {
                                let _permit = permit;
                                respond_with_access(&service, value, access).await
                            });
                            match tokio::time::timeout(options.request_timeout, task).await {
                                Ok(Ok(response)) => response,
                                Ok(Err(_)) => has_id.then(|| {
                                    err(request_id, -32000, "lineage hold outcome unknown", None)
                                }),
                                Err(_) => has_id.then(|| {
                                    err(request_id, -32002, "lineage hold outcome unknown", None)
                                }),
                            }
                        } else if matches!(
                            method,
                            "status"
                                | "batch_receipt"
                                | "transcript_exact"
                                | "initial_task"
                                | "_lineage_hold_lookup"
                                | "_lineage_hold_list"
                        ) {
                            let service = service.clone();
                            let failure = if method == "status" {
                                "status read failed"
                            } else if method == "transcript_exact" {
                                "exact transcript read failed"
                            } else if method == "initial_task" {
                                "initial task read failed"
                            } else if method == "batch_receipt" {
                                "batch receipt read failed"
                            } else {
                                "lineage hold read failed"
                            };
                            // A timed-out observer leaves its read slot with the
                            // owned task until its blocking snapshot actually ends.
                            let task = tokio::spawn(async move {
                                let _permit = permit;
                                respond_with_access(&service, value, access).await
                            });
                            match tokio::time::timeout(options.request_timeout, task).await {
                                Ok(Ok(response)) => response,
                                Ok(Err(_)) => {
                                    has_id.then(|| err(request_id, -32000, failure, None))
                                }
                                Err(_) => has_id.then(|| {
                                    err(request_id, -32002, "API request deadline exceeded", None)
                                }),
                            }
                        } else {
                            let _permit = permit;
                            match tokio::time::timeout(
                                options.request_timeout,
                                respond_with_access(&service, value, access),
                            )
                            .await
                            {
                                Ok(response) => response,
                                Err(_) => has_id.then(|| {
                                    err(request_id, -32002, "API request deadline exceeded", None)
                                }),
                            }
                        }
                    } else {
                        let id = value.get("id").cloned().unwrap_or(Value::Null);
                        if value.get("id").is_some() {
                            Some(err(id, -32001, "API request queue is full", None))
                        } else {
                            None
                        }
                    }
                }
            }
            Err(_) => Some(err(Value::Null, -32700, "parse error", None)),
        };
        if let Some(response) = response {
            let response = fit_response(response, answer_response, batch_response)?;
            tokio::time::timeout(
                Duration::from_secs(15),
                frame::write(&mut output, &response, MAX_FRAME),
            )
            .await
            .map_err(|_| crate::error::invalid("socket output timeout"))??;
        }
        if control_only {
            return Ok(());
        }
    }
}

/// Fits a complete newline-terminated reply to the wire budget. Only oversized
/// successful answer envelopes may shed inline content through AnswerView;
/// ordinary answers are byte-for-byte unchanged. An oversized batch result
/// remains uncertain rather than becoming a partial receipt or a refusal.
fn fit_response(mut response: Value, answer_response: bool, batch_response: bool) -> Result<Value> {
    if serde_json::to_vec(&response)?.len() < MAX_FRAME {
        return Ok(response);
    }
    if answer_response
        && response.get("error").is_none()
        && let Some(result) = response.get("result")
        && let Ok(mut answer) =
            serde_json::from_value::<agent_run_domain::AnswerView>(result.clone())
    {
        answer.omit_content();
        response["result"] = serde_json::to_value(answer)?;
        if serde_json::to_vec(&response)?.len() < MAX_FRAME {
            return Ok(response);
        }
    }
    if batch_response && response.get("result").is_some() {
        return Ok(domain_err(
            response["id"].clone(),
            &Error::BatchOutcomeUnknown {
                source: Box::new(crate::error::invalid("batch receipt exceeds maximum size")),
            },
        ));
    }
    Ok(err(
        response["id"].clone(),
        -32603,
        "response exceeds maximum size",
        None,
    ))
}

/// Checks selected start, status and hold requests from their original frame
/// before a generic JSON map can erase duplicate fields. No filesystem,
/// configuration or admission work occurs at this protocol boundary.
fn valid_start_frame(raw: &[u8], method: &str) -> bool {
    /// Only parameters need typed validation here; the normal responder owns the envelope.
    #[derive(serde::Deserialize)]
    struct Frame<T> {
        /// Validated start request; read only to ensure deserialization completes.
        params: T,
    }
    match method {
        "start" => serde_json::from_slice::<Frame<agent_run_domain::ProviderStartRequest>>(raw)
            .map(|frame| frame.params)
            .is_ok(),
        "start_batch" => {
            serde_json::from_slice::<Frame<agent_run_domain::batch::StartBatchRequest>>(raw)
                .map(|frame| frame.params)
                .is_ok()
        }
        "batch_receipt" => {
            serde_json::from_slice::<Frame<agent_run_domain::batch::StartBatchRequest>>(raw)
                .map(|frame| frame.params)
                .is_ok()
        }
        "start_pool" => {
            serde_json::from_slice::<Frame<agent_run_domain::pool::PoolStartRequest>>(raw)
                .map(|frame| frame.params)
                .is_ok()
        }
        "transcript_exact" => serde_json::from_slice::<
            Frame<agent_run_domain::transcript_exact::TranscriptExactRequest>,
        >(raw)
        .map(|frame| frame.params)
        .is_ok(),
        "initial_task" => {
            serde_json::from_slice::<Frame<agent_run_domain::initial_task::InitialTaskRequest>>(raw)
                .map(|frame| frame.params)
                .is_ok()
        }
        "status" => serde_json::from_slice::<Frame<agent_run_domain::status::StatusRequest>>(raw)
            .map(|frame| frame.params)
            .is_ok(),
        "_lineage_hold_acquire" => valid_hold_frame::<
            agent_run_domain::lineage_hold_wire::LineageHoldAcquireRequest,
        >(raw, method),
        "_lineage_hold_lookup" => valid_hold_frame::<
            agent_run_domain::lineage_hold_wire::LineageHoldLookupRequest,
        >(raw, method),
        "_lineage_hold_list" => valid_hold_frame::<
            agent_run_domain::lineage_hold_wire::LineageHoldListRequest,
        >(raw, method),
        "_lineage_hold_release" => valid_hold_frame::<
            agent_run_domain::lineage_hold_wire::LineageHoldReleaseRequest,
        >(raw, method),
        _ => true,
    }
}

/// Checks the original hold envelope and nested DTO before JSON maps erase duplicates.
fn valid_hold_frame<T: serde::de::DeserializeOwned>(raw: &[u8], method: &str) -> bool {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct HoldFrame<T> {
        jsonrpc: String,
        method: String,
        #[serde(default, rename = "id")]
        _id: Option<Value>,
        #[serde(rename = "params")]
        _params: T,
    }
    serde_json::from_slice::<JsonObject<HoldFrame<T>>>(raw)
        .is_ok_and(|JsonObject(frame)| frame.jsonrpc == "2.0" && frame.method == method)
}
/// Binds one private same-user broker socket under its exclusive startup lock.
///
/// The adjacent `.<name>.lock` file fences stale-socket probing and binding for
/// the complete listener lifetime (it is also the lock offline tools take to
/// prove the broker is not serving). A stale socket inode of this user is
/// reclaimed; a live listener, a foreign inode or a non-socket is refused.
/// The returned guard removes only the inode it created and keeps the lock.
///
/// # Errors
/// The lock is held (`socket is already in use`), the path is not a private
/// owned socket, or the bind fails.
pub(crate) async fn bind_private(socket_path: &Path) -> Result<(UnixListener, SocketGuard)> {
    // SAFETY: geteuid is a read-only process identity query.
    let uid = unsafe { libc::geteuid() };
    let path = socket_path.to_path_buf();
    let lock_path = path.with_file_name(format!(
        ".{}.lock",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("api.sock")
    ));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&lock_path)?;
    let lock_meta = lock.metadata()?;
    if !lock_meta.is_file() || lock_meta.uid() != uid {
        return Err(crate::error::invalid(
            "API startup lock is not a private owned file",
        ));
    }
    std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o600))?;
    lock.try_lock_exclusive()
        .map_err(|_| crate::error::invalid("API socket is already in use"))?;
    if let Ok(meta) = std::fs::symlink_metadata(&path) {
        if !meta.file_type().is_socket() || meta.uid() != uid {
            return Err(crate::error::invalid(
                "refusing to replace non-socket or foreign socket path",
            ));
        }
        match UnixStream::connect(&path).await {
            Ok(_) => return Err(crate::error::invalid("another broker is listening")),
            Err(e)
                if [Some(libc::ECONNREFUSED), Some(libc::ENOENT)].contains(&e.raw_os_error()) =>
            {
                let current = std::fs::symlink_metadata(&path)?;
                if current.dev() != meta.dev()
                    || current.ino() != meta.ino()
                    || !current.file_type().is_socket()
                {
                    return Err(crate::error::invalid(
                        "API socket changed during stale reclaim",
                    ));
                }
                std::fs::remove_file(&path)?
            }
            Err(e) => return Err(e.into()),
        }
    }
    let listener = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let meta = std::fs::symlink_metadata(&path)?;
    let guard = SocketGuard {
        path,
        device: meta.dev(),
        inode: meta.ino(),
        _lock: lock,
    };
    Ok((listener, guard))
}

/// Binds a shared socket without changing the host-configured owner, group or
/// mode. The OS umask and parent directory determine who can connect.
pub(crate) async fn bind_shared(path: &Path) -> Result<(UnixListener, SocketGuard)> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(crate::error::invalid("shared socket path must be absolute"));
    }
    let lock_path = path.with_file_name(format!(
        ".{}.lock",
        path.file_name().unwrap().to_string_lossy()
    ));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(lock_path)?;
    if !lock.metadata()?.is_file() {
        return Err(crate::error::invalid("shared socket lock is not a file"));
    }
    lock.try_lock_exclusive()
        .map_err(|_| crate::error::invalid("shared socket is already in use"))?;
    if let Ok(previous) = std::fs::symlink_metadata(path) {
        if !previous.file_type().is_socket() {
            return Err(crate::error::invalid(
                "refusing to replace a non-socket path",
            ));
        }
        match tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(path)).await {
            Ok(Ok(_)) => return Err(crate::error::invalid("another worker is listening")),
            Ok(Err(error))
                if [Some(libc::ECONNREFUSED), Some(libc::ENOENT)]
                    .contains(&error.raw_os_error()) =>
            {
                let current = std::fs::symlink_metadata(path)?;
                if (current.dev(), current.ino()) != (previous.dev(), previous.ino())
                    || !current.file_type().is_socket()
                {
                    return Err(crate::error::invalid(
                        "shared socket changed during stale reclaim",
                    ));
                }
                std::fs::remove_file(path)?;
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err(crate::error::invalid("shared socket probe timed out")),
        }
    }
    let listener = UnixListener::bind(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    Ok((
        listener,
        SocketGuard {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
            _lock: lock,
        },
    ))
}

/// Proves traversal for operator-group members without assuming their other
/// groups. With a different owning group, either group class might apply, so
/// both group and other execute are required. Callers must exclude access ACLs.
#[cfg(target_os = "linux")]
fn operator_group_can_traverse(mode: u32, owning_gid: u32, operator_gid: u32) -> bool {
    mode & 0o010 != 0 && (owning_gid == operator_gid || mode & 0o001 != 0)
}

/// Refuses a POSIX ACL without reading its entries or changing its permissions.
/// Path-based lookup supports searchable ancestors without directory read access.
#[cfg(target_os = "linux")]
fn reject_operator_acl(path: &Path, name: &std::ffi::CStr) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| crate::error::invalid("invalid operator directory path"))?;
    // SAFETY: both C strings outlive the call. A null buffer and zero length
    // ask only for the xattr size; lgetxattr does not follow a final symlink.
    let size = unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size >= 0 {
        return Err(crate::error::invalid(
            "operator directory ACLs are not supported",
        ));
    }
    let error = std::io::Error::last_os_error();
    if ![Some(libc::ENODATA), Some(libc::EOPNOTSUPP)].contains(&error.raw_os_error()) {
        return Err(error.into());
    }
    Ok(())
}

/// Requires a service-owned 0750 operator directory with its exact group and
/// root/service-owned, non-writable ancestors searchable by every operator-group
/// member. Symlinks/access ACLs anywhere and default ACLs on the directory refuse.
#[cfg(target_os = "linux")]
fn validate_operator_directory(path: &Path, gid: u32) -> Result<()> {
    if !path.is_absolute()
        || gid == u32::MAX
        || path.components().any(|part| {
            !matches!(
                part,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        })
    {
        return Err(crate::error::invalid(
            "invalid operator socket path or group",
        ));
    }
    let directory = path
        .parent()
        .filter(|_| path.file_name().is_some())
        .ok_or_else(|| crate::error::invalid("operator socket needs a directory and name"))?;
    // SAFETY: geteuid only reads the process identity.
    let uid = unsafe { libc::geteuid() };
    for ancestor in directory.ancestors() {
        let metadata = std::fs::symlink_metadata(ancestor)?;
        if !metadata.is_dir() || ![0, uid].contains(&metadata.uid()) || metadata.mode() & 0o022 != 0
        {
            return Err(crate::error::invalid(
                "operator socket ancestors are not protected",
            ));
        }
        if !operator_group_can_traverse(metadata.mode(), metadata.gid(), gid) {
            return Err(crate::error::invalid(
                "operator group cannot traverse a socket ancestor",
            ));
        }
        reject_operator_acl(ancestor, c"system.posix_acl_access")?;
        if ancestor == directory
            && (metadata.uid() != uid || metadata.gid() != gid || metadata.mode() & 0o7777 != 0o750)
        {
            return Err(crate::error::invalid(
                "operator directory must be service-owned with its group and mode 0750",
            ));
        }
    }
    reject_operator_acl(directory, c"system.posix_acl_default")?;
    Ok(())
}

/// Removes only the empty private staging directory created for this publication.
#[cfg(target_os = "linux")]
struct OperatorStage {
    /// Unpredictable owner-only staging directory inside the protected parent.
    path: PathBuf,
    /// Filesystem identity, retained so cleanup never removes a replacement.
    identity: (u64, u64),
}

#[cfg(target_os = "linux")]
impl Drop for OperatorStage {
    fn drop(&mut self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path)
            && metadata.is_dir()
            && (metadata.dev(), metadata.ino()) == self.identity
        {
            let _ = std::fs::remove_dir(&self.path);
        }
    }
}

/// Publishes a group-authorized Linux socket only after its ownership and mode
/// are final. The private staging directory prevents early connections; an
/// atomic no-replace rename and the lifetime lock prevent endpoint takeover.
#[cfg(target_os = "linux")]
pub(super) async fn bind_operator(path: &Path, gid: u32) -> Result<(UnixListener, SocketGuard)> {
    use std::{
        ffi::CString,
        os::unix::{ffi::OsStrExt, fs::DirBuilderExt},
    };
    validate_operator_directory(path, gid)?;
    // SAFETY: geteuid only reads the process identity.
    let uid = unsafe { libc::geteuid() };
    let directory = path.parent().expect("validated parent");
    let mut lock_name = std::ffi::OsString::from(".");
    lock_name.push(path.file_name().expect("validated name"));
    lock_name.push(".lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory.join(lock_name))?;
    let metadata = lock.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != uid
        || metadata.nlink() != 1
        || metadata.mode() & 0o7777 != 0o600
    {
        return Err(crate::error::invalid(
            "operator startup lock is not private and owned",
        ));
    }
    lock.try_lock_exclusive()
        .map_err(|_| crate::error::invalid("operator socket is already in use"))?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_socket()
                || metadata.uid() != uid
                || metadata.gid() != gid
                || metadata.mode() & 0o7777 != 0o660
            {
                return Err(crate::error::invalid(
                    "refusing to replace an untrusted operator socket",
                ));
            }
            match tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(path)).await {
                Ok(Err(error))
                    if [Some(libc::ECONNREFUSED), Some(libc::ENOENT)]
                        .contains(&error.raw_os_error()) =>
                {
                    let current = std::fs::symlink_metadata(path)?;
                    if (current.dev(), current.ino()) != (metadata.dev(), metadata.ino())
                        || !current.file_type().is_socket()
                    {
                        return Err(crate::error::invalid(
                            "operator socket changed during stale reclaim",
                        ));
                    }
                    std::fs::remove_file(path)?;
                }
                Ok(Ok(_)) => {
                    return Err(crate::error::invalid(
                        "another operator broker is listening",
                    ));
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => return Err(crate::error::invalid("operator socket probe timed out")),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    validate_operator_directory(path, gid)?;
    let stage_path = directory.join(format!(
        ".op-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    ));
    std::fs::DirBuilder::new().mode(0o700).create(&stage_path)?;
    let metadata = std::fs::symlink_metadata(&stage_path)?;
    let stage = OperatorStage {
        path: stage_path,
        identity: (metadata.dev(), metadata.ino()),
    };
    let staged_path = stage.path.join("s");
    let listener = UnixListener::bind(&staged_path)?;
    let metadata = std::fs::symlink_metadata(&staged_path)?;
    let mut guard = SocketGuard {
        path: staged_path.clone(),
        device: metadata.dev(),
        inode: metadata.ino(),
        _lock: lock,
    };
    std::os::unix::fs::chown(&staged_path, Some(uid), Some(gid))?;
    std::fs::set_permissions(&staged_path, std::fs::Permissions::from_mode(0o660))?;
    let metadata = std::fs::symlink_metadata(&staged_path)?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != uid
        || metadata.gid() != gid
        || metadata.mode() & 0o7777 != 0o660
        || (metadata.dev(), metadata.ino()) != (guard.device, guard.inode)
    {
        return Err(crate::error::invalid(
            "operator socket ownership verification failed",
        ));
    }
    let source = CString::new(staged_path.as_os_str().as_bytes())
        .map_err(|_| crate::error::invalid("invalid operator staging path"))?;
    let destination = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| crate::error::invalid("invalid operator socket path"))?;
    // SAFETY: both C paths outlive this call. RENAME_NOREPLACE atomically
    // publishes only into an absent name; unsupported kernels fail closed.
    if unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    guard.path = path.to_path_buf();
    std::fs::remove_dir(&stage.path)?;
    Ok((listener, guard))
}

/// Serves the default `<home>/api.sock` broker endpoint until a termination signal.
pub async fn serve(home: &Path) -> Result<()> {
    serve_at(home, &home.join("api.sock")).await
}

/// Serves one selected private socket until SIGINT or SIGTERM.
///
/// The adjacent lock fences stale-socket probing and binding for the complete
/// listener lifetime. Shutdown stops accepts and gives admitted handlers five
/// seconds to finish before refusing remaining work by closing their streams.
pub async fn serve_at(home: &Path, socket_path: &Path) -> Result<()> {
    serve_at_with_options(home, socket_path, ServeOptions::default()).await
}

/// Serves one selected socket with explicit bounded connection and deadline options.
/// Owns reconciliation, service supervision and bounded history-retention workers;
/// shutdown cancels their schedules while in-flight SQLite work uses progress deadlines.
pub async fn serve_at_with_options(
    home: &Path,
    socket_path: &Path,
    options: ServeOptions,
) -> Result<()> {
    serve_endpoints(home, socket_path, None, None, options).await
}

/// Adds an opt-in Linux operator endpoint to the unchanged private API/writer.
/// Both listeners share one service, writer, connection pool, and request lanes.
/// The pre-existing operator directory must satisfy the protected 0750/group
/// contract; the socket is published 0660 and authorizes the group's full
/// public operator API, never worker-only methods. Other platforms refuse.
pub async fn serve_with_operator(
    home: &Path,
    operator_path: &Path,
    operator_gid: u32,
    options: ServeOptions,
) -> Result<()> {
    serve_endpoints(
        home,
        &home.join("api.sock"),
        Some((operator_path, operator_gid)),
        None,
        options,
    )
    .await
}

/// Serves the shared system broker with separate operator and supervisor
/// sockets. Group-authorized clients may use existing capability-bound worker
/// methods on the shared endpoint.
pub async fn serve_system(
    home: &Path,
    operator_path: &Path,
    system: SystemServeOptions,
    options: ServeOptions,
) -> Result<()> {
    serve_endpoints(
        home,
        &home.join("api.sock"),
        Some((operator_path, 0)),
        Some(system),
        options,
    )
    .await
}

/// Accepts the private or any operator endpoint, rotating their priority.
async fn accept_endpoint(
    private: &UnixListener,
    operators: &[(UnixListener, SocketGuard)],
    system_group: bool,
    cursor: &mut usize,
) -> std::io::Result<(UnixStream, SocketAccess)> {
    std::future::poll_fn(|context| {
        use std::task::Poll;
        let count = operators.len() + 1;
        for offset in 0..count {
            let index = (*cursor + offset) % count;
            let listener = if index == 0 {
                private
            } else {
                &operators[index - 1].0
            };
            if let Poll::Ready(result) = listener.poll_accept(context) {
                *cursor = (index + 1) % count;
                let access = if index == 0 {
                    SocketAccess::Private
                } else if system_group {
                    SocketAccess::SystemGroup
                } else {
                    SocketAccess::Operator
                };
                return Poll::Ready(result.map(|(stream, _)| (stream, access)));
            }
        }
        Poll::Pending
    })
    .await
}

/// Owns the shared broker lifecycle and optional protected operator listener.
async fn serve_endpoints(
    home: &Path,
    socket_path: &Path,
    operator: Option<(&Path, u32)>,
    system: Option<SystemServeOptions>,
    options: ServeOptions,
) -> Result<()> {
    if options.max_connections == 0 || options.max_pending_requests == 0 {
        return Err(crate::error::invalid("socket limits must be positive"));
    }
    if options.request_timeout.is_zero() || options.idle_timeout.is_zero() {
        return Err(crate::error::invalid("socket deadlines must be positive"));
    }
    // SAFETY: geteuid is a read-only process identity query.
    let uid = unsafe { libc::geteuid() };
    // Validate the opt-in boundary before creating any endpoint or writer.
    #[cfg(target_os = "linux")]
    if system.is_none()
        && let Some((path, gid)) = operator
    {
        validate_operator_directory(path, gid)?;
    }
    #[cfg(not(target_os = "linux"))]
    if operator.is_some() && system.is_none() {
        return Err(Error::Unsupported("operator socket requires Linux".into()));
    }
    agent_run_core::logging::configure(home, "api");
    // An older database is only ever upgraded by the paired config migration.
    crate::migrate::require_current_store(home)?;
    // Either a valid schema-2 provider config or a valid schema-1 config.
    if agent_run_config::provider_config::ProviderConfig::load(home).is_err() {
        let _ = crate::config::Config::load(home)?;
    }
    let directory = std::fs::symlink_metadata(home)?;
    if !directory.is_dir()
        || directory.file_type().is_symlink()
        || directory.uid() != uid
        || (system.is_none() && directory.mode() & 0o077 != 0)
    {
        return Err(crate::error::invalid(
            "agent-run home must be an owned mode-0700 directory",
        ));
    }
    let (listener, _guard) = bind_private(socket_path).await?;
    // The writer socket is bound before anything is served: its startup lock is
    // what offline tools take to prove that no broker serves this home.
    let (writer_listener, writer_guard) = bind_private(&home.join("writer.sock")).await?;
    let operator_endpoint = match operator {
        Some((path, gid)) if system.is_some() => {
            let _ = gid;
            Some(bind_shared(path).await?)
        }
        #[cfg(target_os = "linux")]
        Some((path, gid)) => Some(bind_operator(path, gid).await?),
        #[cfg(not(target_os = "linux"))]
        Some(_) => return Err(Error::Unsupported("operator socket requires Linux".into())),
        None => None,
    };
    let mut operator_endpoints: Vec<_> = operator_endpoint.into_iter().collect();
    if let Some(settings) = &system {
        for path in &settings.operator_sockets {
            operator_endpoints.push(bind_shared(path).await?);
        }
    }
    let system_endpoint = match &system {
        Some(settings) => Some(bind_shared(&settings.writer_socket).await?),
        None => None,
    };
    // Explicitly configured readers retain exact-key recovery after disabling
    // the managed switch, including across a broker restart.
    let bridge_receipt_endpoint = if system.is_none()
        && agent_run_config::provider_config::ProviderConfig::load(home)
            .ok()
            .is_some_and(|(config, _)| {
                config.managed_attempt_binding.is_some_and(|settings| {
                    !settings.bridge_receipt_readers.is_empty()
                        || !settings.bridge_custody_readers.is_empty()
                })
            }) {
        Some(bind_private(&home.join("attempt-receipts.sock")).await?)
    } else {
        None
    };
    let pool_attempt_endpoint = if system.is_none()
        && agent_run_config::provider_config::ProviderConfig::load(home)
            .ok()
            .is_some_and(|(config, _)| {
                config
                    .managed_attempt_binding
                    .is_some_and(|settings| settings.transport.is_some())
            }) {
        Some(bind_private(&home.join("attempt.sock")).await?)
    } else {
        None
    };
    let launcher = system.as_ref().map(|settings| settings.launcher.clone());
    let system_group = system.is_some();
    // The one resident writer: acquires the home-wide writer lease and opens the
    // EXISTING schema-30 store on its dedicated thread (never initializes or
    // migrates; an absent or old schema is a typed refusal).
    let writer_service = agent_run_core::writer::WriterService::start(
        home,
        agent_run_core::writer::WriterConfig::default(),
    )?;
    let broker_writer = agent_run_core::broker_writer::BrokerWriter::new(writer_service.handle());
    let endpoint = agent_run_core::writer_server::WriterEndpoint::new(
        writer_service.handle(),
        crate::process::inspect(std::process::id() as i32)?,
        home,
    );
    let (writer_stop, writer_stopped) = tokio::sync::oneshot::channel::<()>();
    // The listener's result is observed: a fatal error ends API admission too
    // (a broker whose writer socket is dead must not look alive).
    let mut writer_task = Some(tokio::spawn(async move {
        let _guard = writer_guard;
        if let Some((system_listener, system_guard)) = system_endpoint {
            let _system_guard = system_guard;
            return super::writer_socket::serve_system_listeners(
                writer_listener,
                system_listener,
                endpoint,
                async {
                    let _ = writer_stopped.await;
                },
            )
            .await;
        }
        super::writer_socket::serve_listener(writer_listener, endpoint, async {
            let _ = writer_stopped.await;
        })
        .await
    }));
    let mut writer_failure: Option<Error> = None;
    let mut service = Service::new(home.to_owned()).with_writer(broker_writer.clone());
    if let Some(launcher) = launcher {
        service = service.with_system_launcher(launcher);
    }
    service.recover_service_restart().await?;
    let _ = service.reconcile().await?;
    let mut services =
        agent_run_core::managed_services::Manager::new(home, std::env::current_exe()?)?
            .with_writer(broker_writer.lane("services"));
    // JoinSet aborts maintenance on every exit path, including cancellation of
    // serve_at itself; dropping a bare JoinHandle would detach these loops.
    let mut workers = JoinSet::new();
    if let Some((listener, guard)) = pool_attempt_endpoint {
        let attempt_service = service.clone();
        workers.spawn(async move {
            let _guard=guard;let capacity=Arc::new(Semaphore::new(32));let mut handlers=JoinSet::new();
            loop {tokio::select! {
                accepted=listener.accept()=>match accepted {
                    Ok((stream,_))=>{let Ok(permit)=capacity.clone().try_acquire_owned() else {continue};let service=attempt_service.clone();handlers.spawn(async move {let _permit=permit;let _=agent_run_core::attempt_wire::serve_pool_attempt(stream,&service).await;});},
                    Err(_)=>break,
                },
                Some(_)=handlers.join_next(),if !handlers.is_empty()=>{},
            }}
        });
    }
    if let Some((listener, guard)) = bridge_receipt_endpoint {
        let receipt_service = service.clone();
        workers.spawn(async move {
            let _guard=guard;
            let capacity=Arc::new(Semaphore::new(32));
            let mut readers=JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>match accepted {
                        Ok((stream,_))=>{
                            let Ok(permit)=capacity.clone().try_acquire_owned() else {continue};
                            let service=receipt_service.clone();
                            readers.spawn(async move {let _permit=permit;let _=agent_run_core::attempt_wire::serve_bridge_receipt(stream,&service).await;});
                        }
                        Err(_)=>break,
                    },
                    Some(_)=readers.join_next(), if !readers.is_empty()=>{},
                }
            }
        });
    }
    workers.spawn(agent_run_core::service_restart::run(service.clone()));
    let history_home = home.to_owned();
    let retention_lane = broker_writer.lane("retention");
    let shared_retention = system_group;
    workers.spawn(async move {
        // Database expiry/compaction and filesystem retention keep independent
        // schedules: a filesystem backlog drains in one-second passes without
        // forcing a database writer transaction every second, and one side
        // deferring, failing or backlogging never changes the other's cadence.
        let mut schedules = MaintenanceSchedules::starting_at(tokio::time::Instant::now());
        // Volatile scan position only; the writer rechecks every deletion proof.
        let mut history_cursor = None;
        loop {
            if schedules.next_due() > tokio::time::Instant::now() {
                tokio::time::sleep_until(schedules.next_due()).await;
            }
            if schedules.next_database <= tokio::time::Instant::now() {
                let home = history_home.clone();
                let after = history_cursor.clone();
                // SQLite work runs outside the async executor. One job at a time;
                // short SQL deadlines also bound it if the broker task is aborted.
                let result: Result<usize> = async {
                    let (batch, next_cursor, pending) = tokio::task::spawn_blocking(move || {
                        let mut reader = crate::state::writer::StoreReader::open_with(
                            &home,
                            Duration::from_millis(100),
                        )?;
                        let at = crate::domain::now();
                        let pending = reader.history_work_pending(at)?;
                        let (batch, cursor) = reader.history_page(at, 32, after.as_ref())?;
                        Ok::<_, Error>((batch, cursor, pending))
                    })
                    .await
                    .map_err(|_| Error::Runtime("history reader stopped".into()))??;
                    let removed = if batch.candidates.is_empty() && !pending {
                        0
                    } else {
                        let result = retention_lane
                            .local(batch, agent_run_core::broker_writer::BROKER_BUDGET)
                            .await?;
                        result["removed"]
                            .as_u64()
                            .and_then(|v| usize::try_from(v).ok())
                            .ok_or_else(|| {
                                Error::Integrity("invalid history prune result".into())
                            })?
                    };
                    history_cursor = next_cursor;
                    retention_lane
                        .local(
                            crate::state::ops::PruneIncidentLedger {
                                at: crate::domain::now(),
                            },
                            agent_run_core::broker_writer::BROKER_BUDGET,
                        )
                        .await?;
                    receipt_maintenance(&retention_lane, removed).await
                }
                .await;
                let at = tokio::time::Instant::now();
                match result {
                    Ok(removed) => schedules.database_pass(at, removed),
                    Err(error) => {
                        maintenance_diagnostic("history", "queued", &error);
                        schedules.next_database = at + MAINTENANCE_RETRY;
                    }
                }
            }
            if schedules.next_filesystem <= tokio::time::Instant::now() {
                let home = history_home.clone();
                // Filesystem retention runs on its own schedule, even while
                // database batches still report work, so a large journal
                // backlog can never starve reclaiming disposable files on disk.
                // Reads use a read-only handle; the one registry mutation of the
                // pass is an ordered operation of the retention lane.
                let result: Result<usize> = async {
                    let reader = crate::state::writer::StoreReader::open_with(
                        &home,
                        Duration::from_millis(100),
                    )?;
                    let mut layout = agent_run_core::runtime_storage::BrokerLayout {
                        lane: retention_lane.clone(),
                        reader,
                    };
                    if shared_retention {
                        agent_run_core::housekeeping::sweep_shared_on(
                            &home,
                            crate::domain::now(),
                            &mut layout,
                        )
                        .await
                    } else {
                        agent_run_core::housekeeping::sweep_on(
                            &home,
                            crate::domain::now(),
                            &mut layout,
                        )
                        .await
                    }
                }
                .await;
                let at = tokio::time::Instant::now();
                match result {
                    Ok(removed) => schedules.filesystem_pass(at, removed),
                    Err(error) => {
                        maintenance_diagnostic("filesystem", "sweep", &error);
                        schedules.next_filesystem = at + MAINTENANCE_RETRY;
                    }
                }
            }
        }
    });
    workers.spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(error) = services.tick().await {
                eprintln!("service maintenance: {}", error.public().kind);
            }
        }
    });
    let reconciler = service.clone();
    workers.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            if let Err(error) = reconciler.reconcile().await {
                maintenance_diagnostic("reconcile", "queued", &error);
            }
        }
    });
    let checker = service.clone();
    workers.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = checker.health_tick().await {
                maintenance_diagnostic("health", "sweep", &error);
            }
        }
    });
    let refresher = service.clone();

    let home = service.home.clone();
    spawn_delivery_loop(
        &mut workers,
        Duration::from_secs(1),
        CONFIG_RELOAD_INTERVAL,
        move || {
            let home = home.clone();
            async move { crate::delivery::dispatch_once(&home).await }
        },
        move || refresher.refresh_config(),
    );
    let reserved = usize::from(options.max_connections > 1);
    let gate = Arc::new(Semaphore::new(options.max_connections - reserved));
    let control_gate = Arc::new(Semaphore::new(reserved));
    let lanes = Lanes::new(options.max_pending_requests);
    let mut handlers = JoinSet::new();
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut draining = false;
    let mut accept_cursor = 0;
    let mut drain_tick = tokio::time::interval(Duration::from_secs(1));
    let drain_home = service.home.clone();
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>{
                if !system_group { break; }
                writer_service.handle().close_admissions("shared service stopping");
                draining = true;
            },
            _=term.recv()=>{
                if !system_group { break; }
                writer_service.handle().close_admissions("shared service stopping");
                draining = true;
            },
            _=drain_tick.tick(), if draining => {
                match shared_service_idle(drain_home.clone()).await {
                    Ok(true) => break,
                    Ok(false) => {},
                    Err(error) => maintenance_diagnostic("shutdown", "drain", &error),
                }
            },
            ended = writer_task.as_mut().expect("held while selectable"), if writer_task.is_some() => {
                writer_task = None;
                writer_failure = Some(match ended {
                    Ok(Ok(())) => Error::Runtime("writer socket stopped unexpectedly".into()),
                    Ok(Err(error)) => error,
                    Err(_) => Error::Runtime("writer socket task failed".into()),
                });
                break;
            }
            accepted=accept_endpoint(&listener, &operator_endpoints, system_group, &mut accept_cursor)=>{
                let (mut stream, access) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        writer_failure = Some(error.into());
                        break;
                    }
                };
                if authenticate_socket(&stream, access).is_err() {
                    continue;
                }
                let (permit, control_only) = match gate.clone().try_acquire_owned() {
                    Ok(permit) => (permit, false),
                    Err(_) => match control_gate.clone().try_acquire_owned() {
                        Ok(permit) => (permit, true),
                        Err(_) => {
                            let _ = frame::write(&mut stream, &err(Value::Null, -32001, "API connection limit reached", None), MAX_FRAME).await;
                            continue;
                        }
                    },
                };
                let service=service.clone();
                let lanes=lanes.clone();
                let options = options.clone();
                handlers.spawn(async move { let _permit=permit; let _=connection(stream,service,lanes,control_only,options,access).await; });
            }
        }
    }
    workers.abort_all();
    while workers.join_next().await.is_some() {}
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !handlers.is_empty() && tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if tokio::time::timeout(remaining, handlers.join_next())
            .await
            .is_err()
        {
            break;
        }
    }
    handlers.abort_all();
    // The writer socket stops accepting, then the writer drains: queued work is
    // answered `NotEnqueued(Draining)`, the executing job finishes, the thread
    // is joined and the writer lease is released.
    let _ = writer_stop.send(());
    if let Some(task) = writer_task {
        let _ = task.await;
    }
    let (boot_id, now_ns) = agent_run_platform::monotonic::now()?;
    let drained = writer_service
        .shutdown(agent_run_domain::storage::MonoDeadline {
            boot_id,
            ns: now_ns + 5_000_000_000,
        })
        .await;
    match writer_failure {
        Some(error) => Err(error),
        None => drained,
    }
}

/// A planned shared-service stop waits for every active execution and process
/// ownership claim; an unreadable database is not proof that it is safe to stop.
async fn shared_service_idle(home: PathBuf) -> Result<bool> {
    tokio::task::spawn_blocking(move || {
        let store = crate::state::writer::StoreReader::open_with(&home, Duration::from_millis(250))?;
        let idle = store.conn().query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM agents WHERE status IN ('created','starting','running','cancelling')) \
             AND NOT EXISTS(SELECT 1 FROM attempts WHERE ownership_active=1)",
            [], |row| row.get(0),
        )?;
        Ok(idle)
    }).await.map_err(|_| Error::Runtime("shared service drain check failed".into()))?
}
/// Spawns the serial lease/process reconciliation loop on `workers`.
///
/// `run` is synchronous and may block (cleanup sleeps, SQLite waits), so every
/// pass runs on the blocking pool and is awaited before the next tick: at most
/// one pass is active, missed ticks are delayed rather than queued, and the
/// delivery loop never waits on it. Aborting the task (shutdown) stops the loop
/// at its next await; an already running bounded pass is left to finish rather
/// than being cut mid-cleanup.
#[cfg(test)]
fn spawn_reconcile_loop<R>(workers: &mut JoinSet<()>, period: Duration, run: R)
where
    R: Fn() + Send + Sync + 'static,
{
    let run = Arc::new(run);
    workers.spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let run = run.clone();
            if tokio::task::spawn_blocking(move || run()).await.is_err() {
                eprintln!("reconcile maintenance: worker failed");
            }
        }
    });
}

/// Spawns the resident delivery and configuration-reload loop on `workers`.
///
/// Each `period` tick awaits one bounded `dispatch` pass (the production
/// closure drains up to the dispatcher's batch limit) and reports its failure by
/// kind only; `refresh` reloads configuration every `config_period` (first
/// reload one period after start). Both periods must be nonzero, as for
/// `tokio::time::interval`. Independent of reconciliation.
fn spawn_delivery_loop<D, Fut, C>(
    workers: &mut JoinSet<()>,
    period: Duration,
    config_period: Duration,
    mut dispatch: D,
    refresh: C,
) where
    D: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<usize>> + Send,
    C: Fn() -> Result<bool> + Send + 'static,
{
    workers.spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut config_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + config_period, config_period);
        config_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if let Err(e) = dispatch().await {
                        eprintln!("delivery maintenance: {}", e.public().kind);
                    }
                }
                _ = config_tick.tick() => match refresh() {
                    Ok(true) => eprintln!("configuration reloaded"),
                    Ok(false) => {}
                    Err(error) => eprintln!("configuration reload: {}", error.public().kind),
                },
            }
        }
    });
}

/// Calls the selected home's broker without sharing response-routing state.
/// Resume uses one idempotency key across at most one transport reconnect;
/// explicit keys are preserved and domain errors are never retried. Wait keeps
/// its bounded observation loop; other methods retain their one-shot behavior.
pub async fn client(home: &Path, method: &str, params: Value) -> Result<Value> {
    client_with_uid(&home.join("api.sock"), None, method, params).await
}

/// Calls an explicit socket, trusting only the specified kernel-reported server UID.
///
/// Uses the same deadlines and retry rules as [`client`], checking the UID before
/// each request, including reconnects.
pub async fn client_at(
    socket_path: &Path,
    expected_server_uid: u32,
    method: &str,
    params: Value,
) -> Result<Value> {
    client_with_uid(socket_path, Some(expected_server_uid), method, params).await
}

/// Runs a one-shot call with the selected server identity policy.
async fn client_with_uid(
    socket_path: &Path,
    expected_server_uid: Option<u32>,
    method: &str,
    params: Value,
) -> Result<Value> {
    if method == "resume" {
        let client = match expected_server_uid {
            Some(uid) => BrokerClient::with_expected_server_uid(socket_path, uid),
            None => BrokerClient::new(socket_path),
        };
        return client.call_with_timeout(method, Some(params), 120.0).await;
    }
    client_with_wait_deadlines(
        socket_path,
        expected_server_uid,
        method,
        params,
        Duration::from_secs(60),
        Duration::from_secs(65),
    )
    .await
}

/// Decodes one broker JSON-RPC error object, shared by every client.
///
/// A `-32602` response is a validation error unless its `data.code` names
/// another allowlisted public class (for example `PathEscapeError`); every
/// such typed error keeps the broker's `data.code` and bounded `data` as
/// [`Error::Broker`], whose public rendering reports that code when it is
/// allowlisted, so the CLI, MCP and socket clients show the same class. The
/// message is bounded to 512 characters.
fn broker_error(object: &serde_json::Map<String, Value>) -> Error {
    let data = object.get("data").and_then(Value::as_object);
    if data
        .and_then(|data| data.get("code"))
        .and_then(Value::as_str)
        == Some("PreconditionFailed")
        && !matches!(
            data.and_then(|data| data.get("reason"))
                .and_then(Value::as_str),
            Some("not_met" | "unknown")
        )
    {
        return Error::BatchOutcomeUnknown {
            source: Box::new(crate::error::invalid("invalid precondition refusal")),
        };
    }
    let message: String = object
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("broker request failed")
        .chars()
        .take(512)
        .collect();
    let typed = object
        .get("data")
        .and_then(|data| data.get("code"))
        .and_then(Value::as_str)
        .and_then(agent_run_domain::MachineCode::from_wire)
        .is_some_and(|code| code != agent_run_domain::MachineCode::ValidationError);
    if object.get("code").and_then(Value::as_i64) == Some(-32602) && !typed {
        return Error::Validation(message);
    }
    let (broker_error_code, broker_error_data) = object
        .get("data")
        .and_then(Value::as_object)
        .map(|data| {
            (
                data.get("code").and_then(Value::as_str).map(str::to_owned),
                Some(Value::Object(data.clone())),
            )
        })
        .unwrap_or((None, None));
    Error::Broker {
        message,
        broker_error_code,
        broker_error_data,
    }
}

/// Batch error envelopes may only claim a proved precondition refusal with
/// the server's domain-error code, never its timeout/transport codes.
fn valid_batch_precondition_error(method: &str, object: &serde_json::Map<String, Value>) -> bool {
    !matches!(method, "start_batch" | "batch_receipt")
        || object
            .get("data")
            .and_then(|data| data.get("code"))
            .and_then(Value::as_str)
            != Some("PreconditionFailed")
        || object.get("code").and_then(Value::as_i64) == Some(-32000)
}

/// Hold errors require typed fields before they can be treated as a broker refusal.
fn valid_lineage_hold_error(object: &serde_json::Map<String, Value>) -> bool {
    if object.get("code").and_then(Value::as_i64).is_none()
        || object.get("message").and_then(Value::as_str).is_none()
    {
        return false;
    }
    match object.get("data") {
        None => true,
        Some(Value::Object(data)) => ["code", "message", "reason"]
            .into_iter()
            .all(|key| data.get(key).is_none_or(Value::is_string)),
        _ => false,
    }
}

/// A nonrecursive object wrapper: nested values retain their own decoders.
struct JsonObject<T>(T);

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for JsonObject<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
            type Value = JsonObject<T>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an object")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(JsonObject)
            }
        }
        deserializer.deserialize_map(Visitor(std::marker::PhantomData))
    }
}

/// Duplicate known error data keys must not become a fabricated closed reason.
#[derive(serde::Deserialize)]
struct BatchErrorData {
    #[serde(default, rename = "code")]
    _code: Option<Value>,
    #[serde(default, rename = "message")]
    _message: Option<Value>,
    #[serde(default, rename = "reason")]
    _reason: Option<Value>,
}

/// Error fields are checked from the raw stream before any generic JSON map.
#[derive(serde::Deserialize)]
struct BatchRpcError {
    #[serde(rename = "code")]
    _code: Value,
    #[serde(rename = "message")]
    _message: Value,
    #[serde(default, rename = "data")]
    _data: Option<JsonObject<BatchErrorData>>,
}

/// Method-specific batch envelopes preserve duplicate-key detection.
#[derive(serde::Deserialize)]
#[serde(bound(deserialize = "T: serde::Deserialize<'de>"))]
struct BatchRpcEnvelope<T> {
    #[serde(rename = "jsonrpc")]
    _jsonrpc: Value,
    #[serde(rename = "id")]
    _id: Value,
    #[serde(default, rename = "result")]
    _result: Option<T>,
    #[serde(default, rename = "error")]
    _error: Option<JsonObject<BatchRpcError>>,
}

/// Checks only the two batch response shapes; unrelated JSON remains untouched.
fn valid_raw_batch_response(method: &str, raw: &[u8]) -> bool {
    match method {
        "start_batch" => serde_json::from_slice::<
            JsonObject<BatchRpcEnvelope<agent_run_domain::batch::StartBatchResult>>,
        >(raw)
        .is_ok(),
        "batch_receipt" => serde_json::from_slice::<
            JsonObject<BatchRpcEnvelope<agent_run_domain::batch::BatchReceiptLookup>>,
        >(raw)
        .is_ok(),
        _ => true,
    }
}

/// Strict client-side receipt; IDs and selectors use their domain parsers.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldClientReceipt {
    version: u32,
    store_id: agent_run_domain::StoreId,
    hold_id: agent_run_domain::lineage_hold::HoldId,
    fingerprint: agent_run_domain::lineage_hold_wire::LowerHex32,
    targets: Vec<agent_run_domain::lineage_hold::LineageHoldTarget>,
    canonical_roots: Vec<agent_run_domain::domain::AgentId>,
}

/// Only these durable hold states may occur in a client response.
#[derive(Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum HoldClientState {
    Active,
    Released,
}

/// One exact acquire receipt and its durable lifecycle state.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldClientAcquire {
    receipt: HoldClientReceipt,
    state: HoldClientState,
    replayed: bool,
}

/// A listed receipt with the state observed in that page's snapshot.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldClientObservation {
    receipt: HoldClientReceipt,
    state: HoldClientState,
}

/// Optional result of one store-scoped hold lookup.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldClientLookup {
    store_id: agent_run_domain::StoreId,
    found: bool,
    #[serde(default)]
    receipt: Option<HoldClientReceipt>,
    #[serde(default)]
    state: Option<HoldClientState>,
}

/// One ordered, cursor-bounded page of hold observations.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldClientList {
    store_id: agent_run_domain::StoreId,
    holds: Vec<HoldClientObservation>,
    next_after_hold_id: Option<agent_run_domain::lineage_hold::HoldId>,
}

/// Optional authenticated release result for one exact hold.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldClientRelease {
    store_id: agent_run_domain::StoreId,
    found: bool,
    #[serde(default)]
    hold_id: Option<agent_run_domain::lineage_hold::HoldId>,
    #[serde(default)]
    state: Option<HoldClientState>,
    #[serde(default)]
    already_released: Option<bool>,
}

/// Rejects repeated fields in the original response before decoding a JSON map.
fn valid_raw_lineage_hold_response(method: &str, raw: &[u8]) -> bool {
    match method {
        "_lineage_hold_acquire" => serde_json::from_slice::<
            JsonObject<BatchRpcEnvelope<JsonObject<HoldClientAcquire>>>,
        >(raw)
        .is_ok(),
        "_lineage_hold_lookup" => serde_json::from_slice::<
            JsonObject<BatchRpcEnvelope<JsonObject<HoldClientLookup>>>,
        >(raw)
        .is_ok(),
        "_lineage_hold_list" => {
            serde_json::from_slice::<JsonObject<BatchRpcEnvelope<JsonObject<HoldClientList>>>>(raw)
                .is_ok()
        }
        "_lineage_hold_release" => serde_json::from_slice::<
            JsonObject<BatchRpcEnvelope<JsonObject<HoldClientRelease>>>,
        >(raw)
        .is_ok(),
        _ => true,
    }
}

/// Checks the fixed receipt shape and uniqueness without granting worker authority.
fn valid_hold_receipt(receipt: &HoldClientReceipt, store: &agent_run_domain::StoreId) -> bool {
    let count = receipt.targets.len();
    if receipt.version != 1
        || &receipt.store_id != store
        || !(1..=20).contains(&count)
        || receipt.canonical_roots.len() != count
    {
        return false;
    }
    let mut roots = std::collections::HashSet::new();
    let mut pins = std::collections::HashSet::new();
    let mut executions = std::collections::HashSet::new();
    receipt
        .canonical_roots
        .iter()
        .all(|root| roots.insert(root))
        && receipt.targets.iter().all(|target| {
            target.sequence != 0
                && pins.insert(&target.pin_id)
                && executions.insert(&target.execution_ref)
        })
}

/// Requires target entries to be JSON objects rather than positional arrays.
fn hold_target_objects(receipt: &Value) -> bool {
    receipt["targets"]
        .as_array()
        .is_some_and(|targets| targets.iter().all(Value::is_object))
}

/// Correlates a complete hold response to the exact store, intent and cursor.
fn valid_lineage_hold_response(method: &str, value: &Value, params: &Value) -> bool {
    use agent_run_domain::lineage_hold::LineageHoldAcquire;
    use agent_run_domain::lineage_hold_wire::{
        LineageHoldAcquireRequest, LineageHoldListRequest, LineageHoldListState,
        LineageHoldLookupRequest, LineageHoldReleaseRequest,
    };

    match method {
        "_lineage_hold_acquire" => {
            let Ok(request) = serde_json::from_value::<LineageHoldAcquireRequest>(params.clone())
            else {
                return false;
            };
            let Ok(intent) = LineageHoldAcquire::try_from(request) else {
                return false;
            };
            let Ok(response) = serde_json::from_value::<HoldClientAcquire>(value.clone()) else {
                return false;
            };
            let Ok(fingerprint) = intent.fingerprint() else {
                return false;
            };
            valid_hold_receipt(&response.receipt, &intent.store_id)
                && hold_target_objects(&value["receipt"])
                && response.receipt.fingerprint.into_bytes() == fingerprint
                && response.receipt.targets == intent.targets
                && (response.replayed || response.state == HoldClientState::Active)
        }
        "_lineage_hold_lookup" => {
            let Ok(request) = serde_json::from_value::<LineageHoldLookupRequest>(params.clone())
            else {
                return false;
            };
            let Ok(response) = serde_json::from_value::<HoldClientLookup>(value.clone()) else {
                return false;
            };
            if request.validate().is_err() || response.store_id != request.expected_store_id {
                return false;
            }
            match (response.found, response.receipt, response.state) {
                (false, None, None) => {
                    value.get("receipt").is_none() && value.get("state").is_none()
                }
                (true, Some(receipt), Some(_)) => {
                    valid_hold_receipt(&receipt, &request.expected_store_id)
                        && hold_target_objects(&value["receipt"])
                        && receipt.fingerprint == request.fingerprint
                }
                _ => false,
            }
        }
        "_lineage_hold_list" => {
            let Ok(request) = serde_json::from_value::<LineageHoldListRequest>(params.clone())
            else {
                return false;
            };
            let Ok(response) = serde_json::from_value::<HoldClientList>(value.clone()) else {
                return false;
            };
            if request.validate().is_err()
                || response.store_id != request.expected_store_id
                || response.holds.len() > request.limit
                || value.get("next_after_hold_id").is_none()
            {
                return false;
            }
            let Some(raw_holds) = value["holds"].as_array() else {
                return false;
            };
            let mut previous = request.after_hold_id.as_ref().map(|hold| hold.as_str());
            for (observation, raw) in response.holds.iter().zip(raw_holds) {
                let receipt = &observation.receipt;
                if !valid_hold_receipt(receipt, &request.expected_store_id)
                    || !raw.is_object()
                    || !raw["receipt"].is_object()
                    || !hold_target_objects(&raw["receipt"])
                    || previous.is_some_and(|id| receipt.hold_id.as_str() <= id)
                    || matches!(request.state, LineageHoldListState::Active)
                        && observation.state != HoldClientState::Active
                    || matches!(request.state, LineageHoldListState::Released)
                        && observation.state != HoldClientState::Released
                {
                    return false;
                }
                previous = Some(receipt.hold_id.as_str());
            }
            response.next_after_hold_id.as_ref().is_none_or(|next| {
                response.holds.len() == request.limit
                    && response
                        .holds
                        .last()
                        .is_some_and(|last| &last.receipt.hold_id == next)
            })
        }
        "_lineage_hold_release" => {
            let Ok(request) = serde_json::from_value::<LineageHoldReleaseRequest>(params.clone())
            else {
                return false;
            };
            let Ok(response) = serde_json::from_value::<HoldClientRelease>(value.clone()) else {
                return false;
            };
            if request.validate().is_err() || response.store_id != request.expected_store_id {
                return false;
            }
            match (
                response.found,
                response.hold_id,
                response.state,
                response.already_released,
            ) {
                (false, None, None, None) => {
                    value.get("hold_id").is_none()
                        && value.get("state").is_none()
                        && value.get("already_released").is_none()
                }
                (true, Some(hold_id), Some(HoldClientState::Released), Some(_)) => {
                    hold_id == request.hold_id
                }
                _ => false,
            }
        }
        _ => true,
    }
}

/// A malformed response after a batch send cannot prove the admission was refused.
fn batch_response_error(method: &str, source: Error) -> Error {
    if method == "start_batch" {
        Error::BatchOutcomeUnknown {
            source: Box::new(source),
        }
    } else {
        source
    }
}

/// A malformed lookup is a read failure, while old scalar error classes stay intact.
fn malformed_response_error(method: &str, message: &str) -> Error {
    let error = if method == "batch_receipt" {
        Error::Runtime(message.into())
    } else {
        crate::error::invalid(message)
    };
    batch_response_error(method, error)
}

/// Requires a canonical database identity only when capabilities opted in.
pub(crate) fn valid_capabilities_scope(value: &Value, params: &Value) -> bool {
    (params.get("include_store_id") != Some(&json!(true))
        || value
            .get("store_id")
            .and_then(Value::as_str)
            .is_some_and(|id| id.parse::<agent_run_domain::StoreId>().is_ok()))
        && (params.get("include_initial_task") != Some(&json!(true))
            || value.get("initial_task").is_some_and(|capability| {
                serde_json::from_value::<agent_run_domain::initial_task::InitialTaskCapability>(
                    capability.clone(),
                )
                .is_ok()
            }))
}

/// Rejects wrong-store/selector or inconsistent exact-export continuations.
pub(crate) fn valid_transcript_exact_response(value: &Value, params: &Value) -> bool {
    let Ok(request) = serde_json::from_value::<
        agent_run_domain::transcript_exact::TranscriptExactRequest,
    >(params.clone()) else {
        return false;
    };
    let Ok(response) = serde_json::from_value::<
        agent_run_domain::transcript_exact::TranscriptExactResponse,
    >(value.clone()) else {
        return false;
    };
    response.validate_for(&request).is_ok()
}

/// Rejects wrong-store/agent/fence or inconsistent initial-task pages.
pub(crate) fn valid_initial_task_response(value: &Value, params: &Value) -> bool {
    let Ok(request) = serde_json::from_value::<agent_run_domain::initial_task::InitialTaskRequest>(
        params.clone(),
    ) else {
        return false;
    };
    let Ok(response) = serde_json::from_value::<agent_run_domain::initial_task::InitialTaskResponse>(
        value.clone(),
    ) else {
        return false;
    };
    response.validate_for(&request).is_ok()
}

/// Rejects incomplete or unrelated status proofs instead of publishing them.
pub(crate) fn valid_status_response(value: &Value, params: &Value) -> bool {
    let Ok(request) =
        serde_json::from_value::<agent_run_domain::status::StatusRequest>(params.clone())
    else {
        return false;
    };
    let Ok(output) =
        serde_json::from_value::<agent_run_domain::status::StatusOutput>(value.clone())
    else {
        return false;
    };
    output.validate_for(&request).is_ok()
}

/// Validates the entire batch receipt against the exact submitted intent.
pub(crate) fn valid_batch_response(value: &Value, params: &Value) -> bool {
    let Ok(result) =
        serde_json::from_value::<agent_run_domain::batch::StartBatchResult>(value.clone())
    else {
        return false;
    };
    let Ok(request) =
        serde_json::from_value::<agent_run_domain::batch::StartBatchRequest>(params.clone())
    else {
        return false;
    };
    result.validate_for_request(&request).is_ok()
}

/// Validates a read-only receipt lookup against its exact scoped batch intent.
pub(crate) fn valid_batch_receipt_response(value: &Value, params: &Value) -> bool {
    let Ok(request) =
        serde_json::from_value::<agent_run_domain::batch::StartBatchRequest>(params.clone())
    else {
        return false;
    };
    let Ok(output) =
        serde_json::from_value::<agent_run_domain::batch::BatchReceiptLookup>(value.clone())
    else {
        return false;
    };
    output.validate_for(&request).is_ok()
}

/// Verifies a pin response against the operator request before reporting success.
pub(crate) fn valid_history_pin_response(value: &Value, params: &Value) -> bool {
    use agent_run_domain::batch::{
        HistoryPinFilter, HistoryPinRelease, HistoryPinState, HistoryPinsPage, HistoryPinsRequest,
    };
    let Ok(request) = serde_json::from_value::<HistoryPinsRequest>(params.clone()) else {
        return false;
    };
    match request {
        HistoryPinsRequest::Release { pin_id, .. } => {
            serde_json::from_value::<HistoryPinRelease>(value.clone()).is_ok_and(|response| {
                response.pin_id == pin_id && response.status == HistoryPinState::Released
            })
        }
        HistoryPinsRequest::List {
            status,
            limit,
            after_pin_id,
            ..
        } => {
            let Ok(page) = serde_json::from_value::<HistoryPinsPage>(value.clone()) else {
                return false;
            };
            if !(1..=1000).contains(&limit) || page.pins.len() > limit {
                return false;
            }
            let mut previous = after_pin_id.as_ref().map(|pin| pin.as_str());
            for pin in &page.pins {
                if previous.is_some_and(|previous| pin.pin_id.as_str() <= previous)
                    || (status == HistoryPinFilter::Active && pin.status != HistoryPinState::Active)
                    || (status == HistoryPinFilter::Released
                        && pin.status != HistoryPinState::Released)
                {
                    return false;
                }
                previous = Some(pin.pin_id.as_str());
            }
            page.next_after_pin_id.as_ref().is_none_or(|next| {
                page.pins.len() == limit && page.pins.last().is_some_and(|pin| &pin.pin_id == next)
            })
        }
    }
}

/// Calls the broker, bounding wait observations and retrying only timed-out observations.
async fn client_with_wait_deadlines(
    socket_path: &Path,
    expected_server_uid: Option<u32>,
    method: &str,
    mut params: Value,
    wait_seconds: Duration,
    wait_call_timeout: Duration,
) -> Result<Value> {
    let is_wait = method == "wait";
    if is_wait && let Some(object) = params.as_object_mut() {
        object.insert("timeout_seconds".into(), json!(wait_seconds.as_secs_f64()));
    }
    loop {
        let mut stream =
            tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(socket_path))
                .await
                .map_err(|_| Error::BrokerUnavailable)?
                .map_err(|_| Error::BrokerUnavailable)?;
        let expected_uid = expected_server_uid.unwrap_or_else(|| {
            // SAFETY: geteuid is a read-only query.
            unsafe { libc::geteuid() }
        });
        if stream.peer_cred()?.uid() != expected_uid {
            return Err(crate::error::invalid("broker belongs to another user"));
        }
        let id = uuid::Uuid::new_v4().to_string();
        tokio::time::timeout(
            Duration::from_secs(15),
            frame::write(
                &mut stream,
                &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
                MAX_FRAME,
            ),
        )
        .await
        .map_err(|_| {
            batch_response_error(
                method,
                Error::Runtime(
                    "broker request write deadline exceeded; admission may be ambiguous".into(),
                ),
            )
        })?
        .map_err(|error| batch_response_error(method, error))?;
        let mut input = BufReader::new(stream);
        // Wait uses a finite observer deadline and a slightly longer client read deadline;
        // timed-out observations are retried against the same durable agent.
        let response_timeout = if is_wait {
            wait_call_timeout
        } else {
            Duration::from_secs(120)
        };
        let raw = tokio::time::timeout(response_timeout, frame::read(&mut input, MAX_FRAME))
            .await
            .map_err(|_| {
                batch_response_error(
                    method,
                    Error::Runtime(
                        "broker response deadline exceeded; an admitted run is not cancelled"
                            .into(),
                    ),
                )
            })?
            .map_err(|error| batch_response_error(method, error))?;
        let raw = raw.ok_or_else(|| {
            batch_response_error(
                method,
                Error::Runtime("broker disconnected before its response".into()),
            )
        })?;
        if !valid_raw_batch_response(method, &raw) {
            return Err(batch_response_error(
                method,
                Error::Runtime("invalid batch broker response".into()),
            ));
        }
        let value: Value = serde_json::from_slice(&raw)
            .map_err(|error| batch_response_error(method, Error::Json(error)))?;
        if value.get("id").and_then(Value::as_str) != Some(id.as_str())
            || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        {
            return Err(malformed_response_error(
                method,
                "invalid broker response identity",
            ));
        }
        if matches!(method, "start_batch" | "batch_receipt")
            && value.get("error").is_some()
            && value.get("result").is_some()
        {
            return Err(malformed_response_error(
                method,
                "conflicting broker response",
            ));
        }
        if let Some(error) = value.get("error") {
            if matches!(method, "start_batch" | "batch_receipt")
                && !error.as_object().is_some_and(|object| {
                    object.get("code").and_then(Value::as_i64).is_some()
                        && object.get("message").and_then(Value::as_str).is_some()
                        && valid_batch_precondition_error(method, object)
                })
            {
                return Err(malformed_response_error(method, "invalid broker error"));
            }
            let error = error
                .as_object()
                .map(broker_error)
                .unwrap_or_else(|| Error::Runtime("broker request failed".into()));
            if method == "batch_receipt" && matches!(error, Error::BatchOutcomeUnknown { .. }) {
                return Err(Error::Runtime("invalid batch broker error".into()));
            }
            return Err(error);
        }
        if is_wait && value["result"]["timed_out"] == true {
            continue;
        }
        let result = value.get("result").cloned().ok_or_else(|| {
            malformed_response_error(method, "broker returned neither result nor error")
        })?;
        if method == "capabilities" && !valid_capabilities_scope(&result, &params) {
            return Err(Error::Runtime("invalid capabilities store identity".into()));
        }
        if method == "transcript_exact" && !valid_transcript_exact_response(&result, &params) {
            return Err(Error::Runtime("invalid exact transcript response".into()));
        }
        if method == "initial_task" && !valid_initial_task_response(&result, &params) {
            return Err(Error::Runtime("invalid initial task response".into()));
        }
        if method == "status" && !valid_status_response(&result, &params) {
            return Err(Error::Runtime("invalid status response".into()));
        }
        if method == "batch_receipt" && !valid_batch_receipt_response(&result, &params) {
            return Err(Error::Runtime(
                "invalid batch receipt lookup response".into(),
            ));
        }
        if method == "start_batch" && !valid_batch_response(&result, &params) {
            return Err(batch_response_error(
                method,
                crate::error::invalid("invalid batch receipt"),
            ));
        }
        if method == "history_pins" && !valid_history_pin_response(&result, &params) {
            return Err(Error::Runtime(
                "history pin result is invalid; reconcile release by pin_id".into(),
            ));
        }
        return Ok(result);
    }
}

#[cfg(test)]
mod tests {
    /// A pin acknowledgement must match the exact release or requested page.
    #[test]
    fn history_pin_responses_are_correlated_and_bounded() {
        let first = "pin_550e8400e29b41d4a716446655440000";
        let next = "pin_550e8400e29b41d4a716446655440001";
        let request = json!({"action":"release","pin_id":first});
        let mut result = json!({"pin_id":first,"status":"released","already_released":false});
        assert!(valid_history_pin_response(&result, &request));
        result["status"] = json!("active");
        assert!(!valid_history_pin_response(&result, &request));
        result["status"] = json!("released");
        result["pin_id"] = json!(next);
        assert!(!valid_history_pin_response(&result, &request));

        let request = json!({"action":"list","status":"active","limit":1});
        let mut result = json!({"pins":[{"pin_id":first,"agent_id":"ag-20260826-120000-0123456789","status":"active"}],"next_after_pin_id":first});
        assert!(valid_history_pin_response(&result, &request));
        result["next_after_pin_id"] = json!(next);
        assert!(!valid_history_pin_response(&result, &request));
        result["next_after_pin_id"] = json!(null);
        result["pins"][0]["status"] = json!("released");
        assert!(!valid_history_pin_response(&result, &request));
    }

    /// A batch receipt must provide exactly the requested set of pin IDs.
    #[test]
    fn batch_pin_receipt_matches_requested_retention() {
        let job = json!({"provider":"p","model":"m","profile":"review","task":"fixture","workdir":"/missing"});
        let mut request = json!({"request_id":"batch", "jobs":[job.clone(),job]});
        let mut result = json!({"replayed":false,"receipt":{"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"batch","jobs":[
            {"index":0,"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":format!("ex1_{}", "a".repeat(64))},
            {"index":1,"agent_id":"ag-20260826-120001-0123456789","sequence":1,"execution_ref":format!("ex1_{}", "b".repeat(64))}
        ]}});
        assert!(valid_batch_response(&result, &request));
        request["retain_history"] = json!(true);
        assert!(!valid_batch_response(&result, &request));
        result["receipt"]["jobs"][0]["pin_id"] = json!("pin_550e8400e29b41d4a716446655440000");
        assert!(!valid_batch_response(&result, &request));
        result["receipt"]["jobs"][1]["pin_id"] = json!("pin_550e8400e29b41d4a716446655440001");
        assert!(valid_batch_response(&result, &request));
        result["receipt"]["jobs"][1]["pin_id"] = result["receipt"]["jobs"][0]["pin_id"].clone();
        assert!(!valid_batch_response(&result, &request));
        result["receipt"]["jobs"][1]["pin_id"] = json!("pin_550e8400e29b41d4a716446655440001");
        request["expected_store_id"] = json!("store_0123456789abcdef0123456789abcdef");
        assert!(valid_batch_response(&result, &request));
        result["receipt"]["store_id"] = json!("store_ffffffffffffffffffffffffffffffff");
        assert!(!valid_batch_response(&result, &request));
        result["receipt"]["store_id"] = json!("bad-store");
        assert!(!valid_batch_response(&result, &request));
    }

    /// A batch result can only confirm the exact ordered prerequisites sent.
    #[test]
    fn batch_receipt_checks_ordered_preconditions_and_omission() {
        let a = json!({"agent_id":"ag-20260826-120000-0123456789","sequence":1,
            "execution_ref":format!("ex1_{}", "a".repeat(64)), "pin_id":"pin_550e8400e29b41d4a716446655440000"});
        let b = json!({"agent_id":"ag-20260826-120001-0123456789","sequence":2,
            "execution_ref":format!("ex1_{}", "b".repeat(64)), "pin_id":"pin_550e8400e29b41d4a716446655440001"});
        let request = json!({"request_id":"batch","expected_store_id":"store_0123456789abcdef0123456789abcdef",
            "preconditions":[a.clone(),b.clone()],"jobs":[{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/missing"}]});
        let mut result = json!({"replayed":false,"receipt":{"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"batch",
            "preconditions":[a,b],"jobs":[{"index":0,"agent_id":"ag-20260826-120002-0123456789","sequence":1,"execution_ref":format!("ex1_{}", "c".repeat(64))}]}});
        assert!(valid_batch_response(&result, &request));
        result["receipt"]["preconditions"]
            .as_array_mut()
            .unwrap()
            .reverse();
        assert!(!valid_batch_response(&result, &request));
        result["receipt"]
            .as_object_mut()
            .unwrap()
            .remove("preconditions");
        assert!(!valid_batch_response(&result, &request));
        let mut old = request.clone();
        old.as_object_mut().unwrap().remove("preconditions");
        assert!(valid_batch_response(&result, &old));
        let mut lookup = json!({"found":true,"receipt":result["receipt"]});
        assert!(!valid_batch_receipt_response(&lookup, &request));
        lookup["receipt"]["preconditions"] = request["preconditions"].clone();
        assert!(valid_batch_receipt_response(&lookup, &request));
    }

    /// Unknown or missing reason cannot convert a possibly committed batch into refusal.
    #[test]
    fn precondition_refusal_preserves_only_closed_reasons() {
        for reason in ["not_met", "unknown"] {
            let wire = json!({"code":-32000,"message":"refused", "data":{"code":"PreconditionFailed","reason":reason}});
            let error = broker_error(wire.as_object().unwrap());
            assert_eq!(error.machine_code().as_str(), "PreconditionFailed");
            assert_eq!(
                serde_json::to_value(error.public().reason).unwrap(),
                json!(reason)
            );
            let response = domain_err(json!(1), &error);
            assert_eq!(response["error"]["data"]["reason"], reason);
        }
        for data in [
            json!({"code":"PreconditionFailed"}),
            json!({"code":"PreconditionFailed","reason":"unrecognized"}),
        ] {
            let wire = json!({"code":-32000,"message":"refused", "data":data});
            let error = broker_error(wire.as_object().unwrap());
            assert!(matches!(error, Error::BatchOutcomeUnknown { .. }));
            assert_eq!(
                domain_err(json!(1), &error)["error"]["data"]["code"],
                "BatchOutcomeUnknown"
            );
        }
    }

    /// A response frame never drops part of a committed batch receipt.
    #[test]
    fn oversized_batch_frame_is_unknown_without_partial_receipt() {
        let response = json!({"jsonrpc":"2.0","id":1,"result":{"receipt":{"preconditions":[{"oversized":"x".repeat(MAX_FRAME)}]}}});
        let fitted = fit_response(response, false, true).unwrap();
        assert!(fitted.get("result").is_none());
        assert_eq!(fitted["error"]["data"]["code"], "BatchOutcomeUnknown");
        assert!(serde_json::to_vec(&fitted).unwrap().len() < MAX_FRAME);
    }
    /// Restart control stays on the existing operator socket, not the MCP catalog.
    #[tokio::test]
    async fn service_restart_control_is_not_an_mcp_tool() {
        let service = Service::new("/unused-restart-fixture".into());
        for method in SERVICE_RESTART_METHODS {
            assert!(!dispatch::is_tool(method));
            let response = respond_with_access(
                &service,
                json!({"jsonrpc":"2.0","id":1,"method":method,"params":{}}),
                SocketAccess::Operator,
            )
            .await
            .unwrap();
            assert_eq!(response["error"]["code"], -32602);
        }
    }
    use super::*;
    use tokio::net::UnixListener;

    /// The narrow CLI answer reads are operator-socket methods, not advertised MCP tools.
    #[tokio::test]
    async fn cli_answer_methods_are_socket_only() {
        let service = Service::new("/unused-answer-fixture".into());
        for method in [
            "_cli_answer",
            "_cli_answer_chunk",
            "_cli_transcript_select",
            "_cli_transcript_page",
            "_cli_transcript_status",
        ] {
            assert!(!dispatch::is_tool(method));
            let response = respond_with_access(
                &service,
                json!({"jsonrpc":"2.0","id":1,"method":method,"params":{}}),
                SocketAccess::Operator,
            )
            .await
            .unwrap();
            assert_eq!(response["error"]["code"], -32602);
        }
    }

    /// Unrelated supplementary groups must not select a non-searchable class
    /// even when the ancestor's other execute bit is set.
    #[cfg(target_os = "linux")]
    #[test]
    fn operator_traversal_requires_every_possible_group_class() {
        for (mode, owning_gid, expected) in [
            (0o700, 1001, false),
            (0o750, 1001, true),
            (0o750, 2002, false),
            (0o701, 1001, false),
            (0o701, 2002, false),
            (0o711, 2002, true),
            (0o755, 2002, true),
        ] {
            assert_eq!(
                operator_group_can_traverse(mode, owning_gid, 1001),
                expected,
                "mode={mode:o}, owning_gid={owning_gid}"
            );
        }
    }

    /// Creates only a synthetic store/sealed answer; never starts an engine.
    fn answer_fixture(
        text: &str,
    ) -> (
        tempfile::TempDir,
        Service,
        crate::domain::AgentId,
        agent_run_platform::verify::Proof,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        crate::fs::private_dir(&home).unwrap();
        std::fs::write(home.join("config.toml"), format!("schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary='/usr/bin/true'\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",toml::Value::String(home.join("runtimes/mock").to_string_lossy().into_owned()))).unwrap();
        let config = crate::config::Config::load(&home).unwrap();
        agent_run_store::fixture::initialize(&home).unwrap();
        let mut store = agent_run_store::fixture::open(&home).unwrap();
        let request = serde_json::from_value(json!({"runtime":"mock","model":"fixture","profile":"review","task":"fixture","workdir":home})).unwrap();
        let (id, _) = store.admit(&request, &config, &json!({}), None).unwrap();
        let root = home.join("agents").join(id.as_str());
        crate::fs::private_dir(&root).unwrap();
        let proof = agent_run_platform::verify::seal(&root, Path::new("answer.md"), text).unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET answer_path=?1,answer_bytes=?2,answer_sha256=?3 WHERE id=?4",
                rusqlite::params![
                    proof.path.to_str().unwrap(),
                    proof.bytes,
                    proof.sha256,
                    id.as_str()
                ],
            )
            .unwrap();
        (temp, Service::new(home), id, proof)
    }

    /// Full CLI answers retain their cutoff; only overflowing successful socket
    /// answers lose content. ASCII envelopes, escaping and Unicode all count.
    #[test]
    fn answer_frame_budget_downgrades_only_oversized_successes() {
        for text in [
            "x".repeat(MAX_FRAME),
            "\"".repeat(MAX_FRAME / 2),
            "\u{0000}".repeat(MAX_FRAME / 6),
            "🦀".repeat(MAX_FRAME / 4),
        ] {
            let (_temp, service, id, _) = answer_fixture(&text);
            let answer = service.answer(&id).unwrap();
            assert_eq!(
                answer["content"], text,
                "CLI inline behavior remains unchanged"
            );
            let original = json!({"jsonrpc":"2.0","id":1,"result":answer});
            assert!(serde_json::to_vec(&original).unwrap().len() + 1 > MAX_FRAME);
            let fitted = fit_response(original.clone(), true, false).unwrap();
            assert!(fitted["result"]["content"].is_null());
            assert_eq!(fitted["result"]["inline_complete"], false);
            for key in [
                "agent_id",
                "available",
                "status",
                "path",
                "size_bytes",
                "sha256",
                "proof_version",
            ] {
                assert_eq!(fitted["result"][key], original["result"][key]);
            }
            assert!(serde_json::to_vec(&fitted).unwrap().len() < MAX_FRAME);
            assert_eq!(
                fit_response(original, false, false).unwrap()["error"]["code"],
                -32603
            );
        }
        let (_temp, service, id, _) = answer_fixture("ordinary \"é\"\n");
        let mut ordinary =
            json!({"jsonrpc":"2.0","id":"ordinary","result":service.answer(&id).unwrap()});
        assert_eq!(
            fit_response(ordinary.clone(), true, false).unwrap(),
            ordinary
        );
        // Account for escaping by measuring an empty content envelope directly.
        ordinary["result"]["content"] = json!("");
        let empty_bytes = serde_json::to_vec(&ordinary).unwrap().len() + 1;
        ordinary["result"]["content"] = json!("x".repeat(MAX_FRAME - empty_bytes));
        assert_eq!(serde_json::to_vec(&ordinary).unwrap().len() + 1, MAX_FRAME);
        assert_eq!(
            fit_response(ordinary.clone(), true, false).unwrap(),
            ordinary
        );
        ordinary["result"]["content"] = json!("x".repeat(MAX_FRAME - empty_bytes + 1));
        assert!(
            fit_response(ordinary.clone(), true, false).unwrap()["result"]["content"].is_null()
        );
        ordinary["result"]["path"] = json!("x".repeat(MAX_FRAME));
        assert_eq!(
            fit_response(ordinary, true, false).unwrap()["error"]["code"],
            -32603
        );
    }

    /// A real framed connection exercises metadata fallback and chunk recovery,
    /// keeps ping exact and closes its handler after EOF without lifecycle work.
    #[tokio::test]
    async fn observation_reads_work_over_live_socket_frames() {
        let text = "\u{0000}\"é🦀".repeat(100_000);
        let (_temp, service, id, proof) = answer_fixture(&text);
        let revision = agent_run_store::writer::StoreReader::open(&service.home)
            .unwrap()
            .revision()
            .unwrap();
        let (client, server) = UnixStream::pair().unwrap();
        let serving = tokio::spawn(connection(
            server,
            service.clone(),
            Lanes::new(1),
            false,
            ServeOptions::default(),
            SocketAccess::Private,
        ));
        let (input, mut output) = client.into_split();
        let mut input = BufReader::new(input);
        for (method, params) in [
            ("ping", json!({})),
            ("capabilities", json!({})),
            ("answer", json!({"agent_id":id})),
        ] {
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":method,"method":method,"params":params}),
                MAX_FRAME,
            )
            .await
            .unwrap();
            let raw = frame::read(&mut input, MAX_FRAME).await.unwrap().unwrap();
            let response: Value = serde_json::from_slice(&raw).unwrap();
            assert!(response.get("error").is_none(), "{response}");
            match method {
                "ping" => assert_eq!(response["result"], json!({"ok":true})),
                "capabilities" => assert_eq!(
                    response["result"],
                    agent_run_core::observation::capabilities()
                ),
                "answer" => {
                    assert_eq!(response["result"]["available"], true);
                    assert!(response["result"]["content"].is_null());
                    assert_eq!(response["result"]["sha256"], proof.sha256);
                }
                _ => unreachable!(),
            }
        }
        let mut offset = 0;
        let mut full = String::new();
        loop {
            let params = json!({"agent_id":id,"sequence":1,"expected_sha256":proof.sha256,"offset_bytes":offset,"max_bytes":131072});
            let expected = dispatch::call(&service, "answer_chunk", params.clone())
                .await
                .unwrap();
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":offset,"method":"answer_chunk","params":params}),
                MAX_FRAME,
            )
            .await
            .unwrap();
            let raw = frame::read(&mut input, MAX_FRAME).await.unwrap().unwrap();
            let response: Value = serde_json::from_slice(&raw).unwrap();
            assert_eq!(response["result"], expected);
            let chunk = &response["result"];
            full.push_str(chunk["content"].as_str().unwrap());
            offset = chunk["next_offset"].as_u64().unwrap();
            if chunk["complete"] == true {
                break;
            }
        }
        assert_eq!(full, text);
        assert_eq!(crate::fs::sha256(full.as_bytes()), proof.sha256);
        assert_eq!(
            agent_run_store::writer::StoreReader::open(&service.home)
                .unwrap()
                .revision()
                .unwrap(),
            revision
        );
        drop(output);
        drop(input);
        tokio::time::timeout(Duration::from_secs(5), serving)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    /// Duplicate server keys cannot collapse into an admissible socket start.
    #[test]
    fn required_mcp_duplicate_socket_keys_fail_before_dispatch() {
        let frame = br#"{"jsonrpc":"2.0","id":1,"method":"start","params":{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/gone","required_mcp":{"pool":[],"pool":[]}}}"#;
        assert!(!valid_start_frame(frame, "start"));
        let valid = br#"{"jsonrpc":"2.0","id":1,"method":"start","params":{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/gone","required_mcp":{"pool":[]}}}"#;
        assert!(valid_start_frame(valid, "start"));
        let batch = br#"{"jsonrpc":"2.0","id":1,"method":"start_batch","params":{"request_id":"b","jobs":[{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/gone","required_mcp":{"pool":[],"pool":[]}}]}}"#;
        assert!(!valid_start_frame(batch, "start_batch"));
        let receipt = br#"{"jsonrpc":"2.0","id":1,"method":"batch_receipt","params":{"request_id":"b","expected_store_id":"store_0123456789abcdef0123456789abcdef","jobs":[{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/gone","required_mcp":{"pool":[],"pool":[]}}]}}"#;
        assert!(!valid_start_frame(receipt, "batch_receipt"));
        let unknown = br#"{"jsonrpc":"2.0","id":1,"method":"batch_receipt","params":{"request_id":"b","expected_store_id":"store_0123456789abcdef0123456789abcdef","jobs":[{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/gone","unexpected":true}]}}"#;
        assert!(!valid_start_frame(unknown, "batch_receipt"));
        let status = br#"{"jsonrpc":"2.0","id":1,"method":"status","params":{"version":1,"targets":[{"agent_id":"ag-20260826-120000-0123456789","sequence":1,"sequence":2}]}}"#;
        assert!(!valid_start_frame(status, "status"));
    }

    /// Duplicate prerequisite fields never survive as a lossy JSON map.
    #[test]
    fn batch_precondition_duplicate_socket_keys_are_rejected() {
        for method in ["start_batch", "batch_receipt"] {
            let base = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"request_id":"b","expected_store_id":"store_0123456789abcdef0123456789abcdef","jobs":[{{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/missing"}}],"preconditions":[{{"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":"ex1_{}","pin_id":"pin_550e8400e29b41d4a716446655440000"}}]}}}}"#,
                "a".repeat(64)
            );
            assert!(valid_start_frame(base.as_bytes(), method));
            for bad in [
                base.replace("\"sequence\":1", "\"sequence\":1,\"sequence\":2"),
                base.replace(
                    "\"preconditions\":[",
                    "\"preconditions\":[],\"preconditions\":[",
                ),
            ] {
                assert!(!valid_start_frame(bad.as_bytes(), method), "{method}");
            }
            let mut positional: Value = serde_json::from_str(&base).unwrap();
            let selector = positional["params"]["preconditions"][0].clone();
            positional["params"]["preconditions"][0] = json!([
                selector["agent_id"],
                selector["sequence"],
                selector["execution_ref"],
                selector["pin_id"]
            ]);
            assert!(!valid_start_frame(
                positional.to_string().as_bytes(),
                method
            ));
            let mut positional: Value = serde_json::from_str(&base).unwrap();
            positional["params"]["jobs"][0] = json!(["p", "m", "review", "t", "/missing"]);
            assert!(!valid_start_frame(
                positional.to_string().as_bytes(),
                method
            ));
            let mut positional: Value = serde_json::from_str(&base).unwrap();
            positional["params"] = json!(["b", [], false, null, null]);
            assert!(!valid_start_frame(
                positional.to_string().as_bytes(),
                method
            ));
        }
    }

    /// The older scalar TUI call cannot be misinterpreted as a status view.
    #[tokio::test]
    async fn status_legacy_scalar_is_rejected() {
        let service = Service::new("/unused-status-fixture".into());
        let response = respond(&service,json!({"jsonrpc":"2.0","id":1,"method":"status","params":{"agent_id":"ag-20260826-120000-0123456789"}})).await.unwrap();
        assert_eq!(response["error"]["code"], -32602);
        assert!(response.get("result").is_none());
    }

    /// A large encoded task is refused by the frame boundary before admission.
    #[tokio::test]
    async fn batch_frame_limit_counts_escaped_json_bytes() {
        let args = json!({"request_id":"b","jobs":[{"provider":"p","model":"m","profile":"review","task":"\\".repeat(512 * 1024),"workdir":"/gone"}]});
        let mut typed: agent_run_domain::batch::StartBatchRequest =
            serde_json::from_value(args.clone()).unwrap();
        typed.validate_intent().unwrap();
        let mut bytes = serde_json::to_vec(
            &json!({"jsonrpc":"2.0","id":1,"method":"start_batch","params":args}),
        )
        .unwrap();
        bytes.push(b'\n');
        assert!(bytes.len() > MAX_FRAME);
        let mut reader = tokio::io::BufReader::new(bytes.as_slice());
        assert!(frame::read(&mut reader, MAX_FRAME).await.is_err());
    }

    /// Oversized response-correlation IDs are refused before touching the broker.
    #[tokio::test]
    async fn batch_oversized_rpc_id_refuses_before_dispatch() {
        let service = Service::new(PathBuf::from("/nonexistent-batch-protocol-fixture"));
        let reply = respond(
            &service,
            json!({"jsonrpc":"2.0","id":"x".repeat(1025),"method":"start_batch","params":{}}),
        )
        .await
        .unwrap();
        assert_eq!(reply["error"]["code"], -32600);
    }

    /// Raw duplicate-map requests retain protocol correlation, envelope precedence
    /// and notification silence, and never reach a nonexistent service home.
    #[tokio::test]
    async fn required_mcp_raw_socket_rejection_preserves_protocol() {
        use tokio::io::AsyncWriteExt;
        let (client, server) = UnixStream::pair().unwrap();
        let service = Service::new(PathBuf::from("/nonexistent-required-mcp-fixture"));
        let serving = tokio::spawn(connection(
            server,
            service,
            Lanes::new(1),
            false,
            ServeOptions::default(),
            SocketAccess::Private,
        ));
        let (reader, mut writer) = client.into_split();
        let mut reader = BufReader::new(reader);
        let params = r#"{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/gone","required_mcp":{"pool":[],"pool":[]}}"#;
        for (version, id, code) in [("2.0", 1, -32602), ("1.0", 2, -32600)] {
            writer.write_all(format!("{{\"jsonrpc\":\"{version}\",\"id\":{id},\"method\":\"start\",\"params\":{params}}}\n").as_bytes()).await.unwrap();
            let raw =
                tokio::time::timeout(Duration::from_secs(5), frame::read(&mut reader, MAX_FRAME))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            let value: Value = serde_json::from_slice(&raw).unwrap();
            assert_eq!(value["id"], id);
            assert_eq!(value["error"]["code"], code);
        }
        writer.write_all(format!("{{\"jsonrpc\":\"2.0\",\"method\":\"start\",\"params\":{params}}}\n{{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"ping\"}}\n").as_bytes()).await.unwrap();
        let raw = tokio::time::timeout(Duration::from_secs(5), frame::read(&mut reader, MAX_FRAME))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(value["id"], 3);
        assert_eq!(value["result"]["ok"], true);
        drop(writer);
        drop(reader);
        tokio::time::timeout(Duration::from_secs(5), serving)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    /// Pool discovery consumes only the read lane and no control reservation.
    #[test]
    fn list_pools_uses_the_read_lane() {
        let lanes = Lanes::new(0);
        let read = lanes.try_acquire("list_pools").unwrap();
        assert!(lanes.try_acquire("list_agents").is_none());
        assert!(lanes.try_acquire("start_pool").is_some());
        assert!(lanes.try_acquire("start_batch").is_some());
        drop(read);
        assert!(lanes.try_acquire("list_pools").is_some());
    }

    /// A cancelled batch wait cannot release control capacity while work is running.
    #[tokio::test]
    async fn batch_control_slot_outlives_cancelled_wait() {
        let lanes = Lanes::new(0);
        let permit = lanes.try_acquire("start_batch").unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut task = spawn_controlled(permit, async move {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
        });
        entered_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut task)
                .await
                .is_err()
        );
        drop(task);
        assert!(lanes.try_acquire("start_batch").is_none());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if lanes.try_acquire("start_batch").is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    /// Both dispatch interruption paths keep uncertainty typed through socket and MCP.
    #[tokio::test]
    async fn batch_control_timeout_and_panic_are_typed_unknown() {
        let lanes = Lanes::new(0);
        let permit = lanes.try_acquire("start_batch").unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let task = spawn_controlled(permit, async move {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            None
        });
        entered_rx.await.unwrap();
        let timeout = batch_control_response(task, Duration::ZERO, json!("batch-1"), true)
            .await
            .unwrap();
        assert!(lanes.try_acquire("start_batch").is_none());
        release_tx.send(()).unwrap();

        let other_lanes = Lanes::new(0);
        let permit = other_lanes.try_acquire("start_batch").unwrap();
        let task: tokio::task::JoinHandle<Option<Value>> = spawn_controlled(permit, async {
            panic!("synthetic dispatch panic");
        });
        let panic = batch_control_response(task, Duration::from_secs(2), json!("batch-2"), true)
            .await
            .unwrap();
        for (response, id, rpc_code) in [(timeout, "batch-1", -32002), (panic, "batch-2", -32000)] {
            assert_eq!(response["id"], id);
            assert_eq!(response["error"]["code"], rpc_code);
            assert_eq!(response["error"]["data"]["code"], "BatchOutcomeUnknown");
            let error = broker_error(response["error"].as_object().unwrap());
            assert_eq!(error.public().kind, "BatchOutcomeUnknown");
            let mcp = crate::transport::mcp_text::failure_result("start_batch", &error, None, None);
            assert_eq!(
                mcp.structured_content.unwrap()["code"],
                "BatchOutcomeUnknown"
            );
        }
    }

    /// Runtime shutdown drops pending work and releases the bounded control slot.
    #[test]
    fn batch_control_slot_releases_on_runtime_shutdown() {
        let lanes = Lanes::new(0);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let permit = lanes.try_acquire("start_batch").unwrap();
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let task = spawn_controlled(permit, async move {
                entered_tx.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            entered_rx.await.unwrap();
            drop(task);
        });
        assert!(lanes.try_acquire("start_batch").is_none());
        drop(runtime);
        assert!(lanes.try_acquire("start_batch").is_some());
    }

    /// Observation reads use the ordinary read permit and leave the control
    /// lane available, preserving lifecycle admission under observer pressure.
    #[test]
    fn observation_methods_use_only_the_read_lane() {
        for method in ["capabilities", "answer_chunk"] {
            let lanes = Lanes::new(0);
            let read = lanes.try_acquire(method).unwrap();
            assert!(lanes.try_acquire("list_agents").is_none());
            assert!(lanes.try_acquire("resume").is_some());
            drop(read);
            assert!(lanes.try_acquire(method).is_some());
        }
    }

    // Protects the client from a broker that holds a wait response open forever.
    #[tokio::test]
    async fn wait_client_deadline_closes_when_broker_holds_response() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("api.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, _output) = stream.into_split();
            let mut input = BufReader::new(input);
            frame::read(&mut input, MAX_FRAME).await.unwrap();
            std::future::pending::<()>().await;
        });

        let result = client_with_wait_deadlines(
            &path,
            None,
            "wait",
            json!({"agent_id":"ag-test"}),
            Duration::from_millis(10),
            Duration::from_millis(20),
        )
        .await;
        assert!(
            matches!(result, Err(Error::Runtime(ref message)) if message.contains("deadline")),
            "result={result:?}"
        );
        server.abort();
    }

    // Protects durable wait retries from changing admission or cancelling the run.
    #[tokio::test]
    async fn timed_out_wait_retries_the_same_agent_until_terminal() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("api.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            for expected in [json!({"timed_out":true}), json!({"terminal":true})] {
                let (stream, _) = listener.accept().await.unwrap();
                let (input, mut output) = stream.into_split();
                let mut input = BufReader::new(input);
                let request: Value = serde_json::from_slice(
                    &frame::read(&mut input, MAX_FRAME).await.unwrap().unwrap(),
                )
                .unwrap();
                assert_eq!(request["params"]["agent_id"], "ag-test");
                assert_eq!(request["params"]["timeout_seconds"], 0.01);
                frame::write(
                    &mut output,
                    &json!({"jsonrpc":"2.0","id":request["id"],"result":expected}),
                    MAX_FRAME,
                )
                .await
                .unwrap();
            }
        });

        let result = client_with_wait_deadlines(
            &path,
            None,
            "wait",
            json!({"agent_id":"ag-test"}),
            Duration::from_millis(10),
            Duration::from_millis(20),
        )
        .await
        .unwrap();
        assert_eq!(result["terminal"], true);
        server.await.unwrap();
    }

    // A filesystem backlog drains on its own one-second cadence without
    // forcing a database writer transaction every second. Drives the same
    // MaintenanceSchedules seam the broker loop uses, so a regression to the
    // old shared cadence — which reran the prune transaction after any
    // removal, including filesystem ones — fails here.
    #[test]
    fn filesystem_backlog_keeps_database_prune_on_its_steady_cadence() {
        let mut clock = tokio::time::Instant::now();
        let mut schedules = MaintenanceSchedules::starting_at(clock);
        let mut database_passes = 0usize;
        let mut filesystem_passes = 0usize;
        for _ in 0..20 {
            if schedules.next_database <= clock {
                database_passes += 1;
                // The database stays idle: expiry removes nothing.
                schedules.database_pass(clock, 0);
            }
            if schedules.next_filesystem <= clock {
                filesystem_passes += 1;
                // The filesystem backlog never drains this pass.
                schedules.filesystem_pass(clock, 1);
            }
            // Advance to whichever schedule is due next, exactly as the
            // maintenance loop's sleep_until does in real time.
            clock = clock.max(schedules.next_due());
        }
        assert_eq!(filesystem_passes, 20, "backlog keeps draining");
        assert_eq!(
            database_passes, 1,
            "a filesystem backlog must not accelerate database prune passes"
        );
    }

    // A database backlog keeps its own one-second drain cadence, and deferral
    // or failure backs off for a retry interval instead of hammering.
    #[test]
    fn database_backlog_and_retry_have_documented_cadences() {
        assert_eq!(database_maintenance_delay(0), DATABASE_STEADY);
        assert_eq!(database_maintenance_delay(1), DATABASE_BACKLOG);
        assert_eq!(database_maintenance_delay(2_001), DATABASE_BACKLOG);
        assert_eq!(filesystem_maintenance_delay(0), FILESYSTEM_STEADY);
        assert_eq!(filesystem_maintenance_delay(1), FILESYSTEM_BACKLOG);
        assert_eq!(MAINTENANCE_RETRY, Duration::from_secs(60));
    }

    /// An owned SQL barrier spends the old checkpoint window before receipt pruning.
    /// Both committed receipt credit and an empty pass must survive that queue delay.
    #[cfg(feature = "test-fixtures")]
    #[tokio::test]
    async fn receipt_maintenance_keeps_progress_after_owned_queue_delay() {
        use agent_run_core::{
            broker_writer::BrokerWriter,
            writer::{WriterConfig, WriterService},
        };
        use agent_run_domain::storage::{MonoDeadline, OperationId};
        use std::{future::Future, task::Poll};
        for rows in [1, 0] {
            let home = tempfile::tempdir().unwrap();
            agent_run_store::fixture::initialize(home.path()).unwrap();
            if rows > 0 {
                let store = agent_run_store::fixture::open(home.path()).unwrap();
                store.conn.execute("INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) VALUES(?1,?2,'fixture','expired',1,'broker',?3,NULL,'committed','{}',1,0)",rusqlite::params![OperationId::new("old-boot",1).as_str(),"d".repeat(64),"f".repeat(64)]).unwrap();
            }
            let service = WriterService::start(home.path(), WriterConfig::default()).unwrap();
            let handle = service.handle();
            let lane = BrokerWriter::new(handle.clone()).lane("retention");
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let owner = handle.clone();
            let barrier = tokio::spawn(async move {
                owner.fixture_barrier(entered, wait).await.unwrap();
            });
            ready.await.unwrap();
            let mut pass = Box::pin(receipt_maintenance(&lane, 0));
            std::future::poll_fn(|cx| {
                assert!(matches!(pass.as_mut().poll(cx), Poll::Pending));
                Poll::Ready(())
            })
            .await;
            let delayed_release = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(300));
                release.send(()).unwrap();
            });
            let result = pass.await;
            delayed_release.join().unwrap();
            barrier.await.unwrap();
            let (boot_id, now) = agent_run_platform::monotonic::now().unwrap();
            service
                .shutdown(MonoDeadline {
                    boot_id,
                    ns: now + 5_000_000_000,
                })
                .await
                .unwrap();
            let removed = result.expect("queue delay cannot consume the later checkpoint budget or erase committed receipt progress");
            assert_eq!(removed, rows);
            let at = tokio::time::Instant::now();
            let mut schedules = MaintenanceSchedules::starting_at(at);
            schedules.database_pass(at, removed);
            assert_eq!(
                schedules.next_database - at,
                if rows > 0 {
                    DATABASE_BACKLOG
                } else {
                    DATABASE_STEADY
                }
            );
        }
    }

    /// Polls `condition` every 5 ms for at most 2 s and reports whether it held.
    async fn eventually(condition: impl Fn() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !condition() {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        true
    }

    /// A blocked reconcile pass must not stall resident delivery, and passes
    /// never overlap: reconcile stays gated until the delivery ticks have been
    /// observed, then resumes, and both loops stop with the `JoinSet`. Waits are
    /// bounded readiness polls, so a loaded host only delays, never flakes.
    #[tokio::test]
    async fn blocked_reconcile_does_not_stall_delivery_or_overlap() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let (release, gate) = std::sync::mpsc::channel::<()>();
        let (active, peak, passes, drained) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        let gate = std::sync::Mutex::new(gate);
        let mut workers = JoinSet::new();
        let (a, pk, ps) = (active.clone(), peak.clone(), passes.clone());
        spawn_reconcile_loop(&mut workers, Duration::from_millis(10), move || {
            ps.fetch_add(1, SeqCst);
            pk.fetch_max(a.fetch_add(1, SeqCst) + 1, SeqCst);
            // Bounded gate: released by the test or by the 10 s ceiling.
            let _ = gate.lock().unwrap().recv_timeout(Duration::from_secs(10));
            a.fetch_sub(1, SeqCst);
        });
        let d = drained.clone();
        spawn_delivery_loop(
            &mut workers,
            Duration::from_millis(10),
            Duration::from_secs(3600),
            move || {
                let d = d.clone();
                async move { Ok(d.fetch_add(1, SeqCst) + 1) }
            },
            || Ok(false),
        );
        assert!(
            eventually(|| passes.load(SeqCst) >= 1 && drained.load(SeqCst) >= 5).await,
            "delivery stalled behind a blocked reconcile"
        );
        // The gate is still held: ticks were delayed, not queued or overlapped.
        assert_eq!(passes.load(SeqCst), 1, "one gated pass, ticks are delayed");
        release.send(()).unwrap();
        assert!(
            eventually(|| passes.load(SeqCst) >= 2).await,
            "reconcile resumes after the gate"
        );
        assert_eq!(peak.load(SeqCst), 1, "reconcile passes overlapped");
        workers.abort_all();
        while workers.join_next().await.is_some() {}
        drop(release);
        let (settled_d, settled_p) = (drained.load(SeqCst), passes.load(SeqCst));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            drained.load(SeqCst),
            settled_d,
            "delivery loop outlived shutdown"
        );
        assert!(
            passes.load(SeqCst) <= settled_p + 1,
            "reconcile loop outlived shutdown"
        );
    }
    /// Duplicate continuation fields must fail before generic JSON map decoding.
    #[test]
    fn transcript_exact_duplicate_cursor_rejected() {
        let params = format!(
            r#"{{"version":1,"expected_store_id":"store_00000000000000000000000000000000","selector":{{"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":"ex1_{}","pin_id":"pin_00000000000040008000000000000000"}},"cursor":"0","cursor":"1","offset":0}}"#,
            "0".repeat(64)
        );
        let frame =
            format!(r#"{{"jsonrpc":"2.0","id":7,"method":"transcript_exact","params":{params}}}"#);
        assert!(!valid_start_frame(frame.as_bytes(), "transcript_exact"));
    }

    /// Hold methods remain unavailable to model tools and reject duplicate raw fields.
    #[test]
    fn lineage_hold_frames_reject_duplicate_proofs() {
        for method in LINEAGE_HOLD_METHODS {
            assert!(!dispatch::is_tool(method));
        }
        let proof = "0".repeat(128);
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"_lineage_hold_release","params":{{"version":1,"expected_store_id":"store_00000000000000000000000000000000","hold_id":"hold_00000000000040008000000000000000","signature":"{proof}","signature":"{proof}"}}}}"#
        );
        assert!(!valid_start_frame(
            frame.as_bytes(),
            "_lineage_hold_release"
        ));
        let valid = format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"_lineage_hold_release","params":{{"version":1,"expected_store_id":"store_00000000000000000000000000000000","hold_id":"hold_00000000000040008000000000000000","signature":"{proof}"}}}}"#
        );
        assert!(valid_start_frame(valid.as_bytes(), "_lineage_hold_release"));
        for duplicate in [
            valid.replace(r#""id":7"#, r#""id":7,"id":8"#),
            valid.replace(
                r#""method":"_lineage_hold_release""#,
                r#""method":"_lineage_hold_release","method":"_lineage_hold_release""#,
            ),
        ] {
            assert!(!valid_start_frame(
                duplicate.as_bytes(),
                "_lineage_hold_release"
            ));
        }
    }

    /// The client never accepts a hold receipt for a different store or intent.
    #[test]
    fn lineage_hold_client_correlates_receipts_and_rejects_ambiguous_results() {
        let store = "store_00112233445566778899aabbccddeeff";
        let id = "hold_0123456789ab4cde8f0123456789abcd";
        let agent = "ag-20261107-010203-0123456789";
        let target = json!({"agent_id":agent,"sequence":2,"execution_ref":format!("ex1_{}", "2".repeat(64)),
            "pin_id":"pin_112233445566478899aabbccddeeff00"});
        let acquire = json!({"version":1,"expected_store_id":store,"request_id":"hold-fixture-1",
            "targets":[target.clone()],
            "verifying_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"});
        let receipt = json!({"version":1,"store_id":store,"hold_id":id,
            "fingerprint":"5407d58ddf96ca0e0b2fe7c7f3a74b363a7b502c121e4b594d7c0aa190d12f66",
            "targets":[target],"canonical_roots":[agent]});
        let response = json!({"receipt":receipt,"state":"active","replayed":false});
        assert!(valid_lineage_hold_response(
            "_lineage_hold_acquire",
            &response,
            &acquire
        ));
        let mut wrong = response.clone();
        wrong["receipt"]["store_id"] = json!("store_ffffffffffffffffffffffffffffffff");
        assert!(!valid_lineage_hold_response(
            "_lineage_hold_acquire",
            &wrong,
            &acquire
        ));
        wrong = response.clone();
        wrong["receipt"]["targets"][0]["sequence"] = json!(3);
        assert!(!valid_lineage_hold_response(
            "_lineage_hold_acquire",
            &wrong,
            &acquire
        ));
        wrong = response.clone();
        wrong["state"] = json!("released");
        assert!(!valid_lineage_hold_response(
            "_lineage_hold_acquire",
            &wrong,
            &acquire
        ));

        let lookup = json!({"version":1,"expected_store_id":store,"request_id":"hold-fixture-1",
            "fingerprint":receipt["fingerprint"]});
        assert!(valid_lineage_hold_response(
            "_lineage_hold_lookup",
            &json!({"store_id":store,"found":true,"receipt":receipt,"state":"active"}),
            &lookup
        ));
        assert!(!valid_lineage_hold_response(
            "_lineage_hold_lookup",
            &json!({"store_id":store,"found":false,"receipt":null}),
            &lookup
        ));
        let release =
            json!({"version":1,"expected_store_id":store,"hold_id":id,"signature":"0".repeat(128)});
        let done = json!({"store_id":store,"found":true,"hold_id":id,"state":"released","already_released":false});
        assert!(valid_lineage_hold_response(
            "_lineage_hold_release",
            &done,
            &release
        ));
        let mut wrong = done.clone();
        wrong["hold_id"] = json!("hold_ffffffffffff4fff8fffffffffffffff");
        assert!(!valid_lineage_hold_response(
            "_lineage_hold_release",
            &wrong,
            &release
        ));
        let raw = format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":{{"store_id":"{store}","found":true,"hold_id":"{id}","state":"active","state":"released","already_released":false}}}}"#
        );
        assert!(!valid_raw_lineage_hold_response(
            "_lineage_hold_release",
            raw.as_bytes()
        ));
    }

    /// A call already waiting for the client lock cannot send after abort.
    #[tokio::test]
    async fn lineage_hold_abort_while_waiting_for_client_lock_never_sends() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("hold.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let client = BrokerClient::new(path);
        let guard = client.connection.lock().await;
        let mut request = Box::pin(client.call_with_timeout(
            "_lineage_hold_acquire",
            Some(json!({"fixture":true})),
            2.0,
        ));
        tokio::select! {
            biased;
            result = &mut request => panic!("request unexpectedly completed: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        client.abort();
        drop(guard);
        let error = request.await.unwrap_err();
        assert!(matches!(error, Error::Runtime(ref message) if message == "broker call cancelled"));
        assert!(
            tokio::time::timeout(Duration::from_millis(250), listener.accept())
                .await
                .is_err()
        );
    }
}
