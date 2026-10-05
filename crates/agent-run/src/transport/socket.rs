//! Bounded LF-delimited JSON-RPC 2.0 over a private same-user Unix socket.
use super::frame;
use crate::{dispatch, service::Service, Error, Result};
use agent_run_domain::ProviderStartRequest;
use fs2::FileExt;
use serde_json::{json, Value};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
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
    /// Private broker endpoint owned by the selected agent-run home.
    socket_path: PathBuf,
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
    /// The broker returned a typed protocol/domain error.
    Domain(Error),
    /// The caller explicitly aborted this client.
    Cancelled,
}

impl BrokerClient {
    /// Creates a lazy broker client; no socket is opened until its first call.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            next_id: AtomicU64::new(1),
            aborted: AtomicBool::new(false),
            wake: Arc::new(tokio::sync::Notify::new()),
            connection: tokio::sync::Mutex::new(None),
        }
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
        if method == "resume" {
            if let Some(arguments) = params.as_mut().and_then(Value::as_object_mut) {
                if arguments.get("request_id").is_none_or(Value::is_null) {
                    arguments.insert("request_id".into(), json!(uuid::Uuid::new_v4().to_string()));
                }
            }
        }
        let mut connection = self.connection.lock().await;
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
                Ok(value) => return Ok(value),
                Err(ClientAttemptError::Domain(error)) => return Err(error),
                Err(ClientAttemptError::Cancelled) => {
                    *connection = None;
                    return Err(Error::Runtime("broker call cancelled".into()));
                }
                Err(ClientAttemptError::Transport) => {
                    *connection = None;
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
                ))
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
        let stream = tokio::select! {
            _ = notified => return Err(Error::Runtime("broker call cancelled".into())),
            result = tokio::time::timeout(Duration::from_secs_f64(timeout_seconds), UnixStream::connect(&self.socket_path)) => {
                result.map_err(|_| Error::BrokerUnavailable)?.map_err(|_| Error::BrokerUnavailable)?
            }
        };
        // SAFETY: geteuid is a read-only process identity query.
        if stream.peer_cred()?.uid() != unsafe { libc::geteuid() } {
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
        let written = tokio::select! {
            _ = notified => return Err(ClientAttemptError::Cancelled),
            result = tokio::time::timeout(Duration::from_secs_f64(timeout_seconds), frame::write(&mut connection.output, &request, MAX_FRAME)) => result,
        };
        match written {
            Ok(Ok(())) => {}
            _ => return Err(ClientAttemptError::Transport),
        }
        let notified = self.wake.notified();
        let raw = tokio::select! {
            _ = notified => return Err(ClientAttemptError::Cancelled),
            result = tokio::time::timeout(Duration::from_secs_f64(timeout_seconds), frame::read(&mut connection.input, MAX_FRAME)) => result,
        };
        let raw = match raw {
            Ok(Ok(Some(raw))) => raw,
            _ => return Err(ClientAttemptError::Transport),
        };
        let value: Value =
            serde_json::from_slice(&raw).map_err(|_| ClientAttemptError::Transport)?;
        if value.get("id") != Some(&json!(id))
            || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        {
            return Err(ClientAttemptError::Transport);
        }
        if let Some(error) = value.get("error") {
            let Some(object) = error.as_object() else {
                return Err(ClientAttemptError::Transport);
            };
            return Err(ClientAttemptError::Domain(broker_error(object)));
        }
        value
            .get("result")
            .cloned()
            .ok_or(ClientAttemptError::Transport)
    }
}

/// Python-compatible upper bound for one newline-delimited JSON RPC message.
pub const MAX_FRAME: usize = 1024 * 1024;
const MAX_CONNECTIONS: usize = 32;
const MAX_PENDING_REQUESTS: usize = 64;
const CONTROL_FRAME_DEADLINE: Duration = Duration::from_millis(500);
/// Interval between content-digest checks for a running broker's configuration.
const CONFIG_RELOAD_INTERVAL: Duration = Duration::from_secs(60);
const CONTROL_METHODS: &[&str] = &[
    "start",
    "resume",
    "cancel",
    "steer",
    "start_pool",
    "pool_post",
    "pool_replace",
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
/// `removed` rows plus reclaimed-page batches.
///
/// `removed == 0` means the pass proved the database idle (a read-only probe
/// can skip the writer transaction entirely), so expiry returns to its hourly
/// steady cadence. Any filesystem outcome is invisible here: the two
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
/// Removes only the listener inode this broker created while retaining its lock.
struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Ok(m) = std::fs::symlink_metadata(&self.path) {
            if m.dev() == self.device && m.ino() == self.inode && m.file_type().is_socket() {
                let _ = std::fs::remove_file(&self.path);
            }
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
    err(
        id,
        -32000,
        &public.message,
        Some(json!({"code":public.kind,"message":public.message})),
    )
}
/// Validates and dispatches one decoded JSON-RPC object without socket I/O.
///
/// Notifications return `None`; malformed requests use the JSON-RPC envelope
/// required by the Python broker. The caller owns `service` and therefore the
/// SQLite connection lifetime remains outside this pure transport boundary.
pub async fn respond(service: &Service, v: Value) -> Option<Value> {
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
    let params = obj.get("params").cloned().unwrap_or_else(|| json!({}));
    if !params.is_object() {
        return if notification {
            None
        } else {
            Some(err(id, -32602, "params must be an object", None))
        };
    }
    let known = dispatch::is_tool(method)
        || [
            "tools",
            "ping",
            "wait",
            agent_run_domain::worker::METHOD,
            agent_run_domain::worker::TOOL_METHOD,
        ]
        .contains(&method);
    let response = if !known {
        err(id, -32601, "method not found", None)
    } else {
        match dispatch::call(service, method, params).await {
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
    if notification {
        None
    } else {
        Some(response)
    }
}
/// Runs one connection with framed parsing, lane admission, and ordered writes.
async fn connection(
    stream: UnixStream,
    service: Service,
    lanes: Lanes,
    control_only: bool,
    options: ServeOptions,
) -> Result<()> {
    // SAFETY: geteuid has no arguments or memory safety preconditions.
    let uid = unsafe { libc::geteuid() };
    if stream.peer_cred()?.uid() != uid {
        return Err(crate::error::invalid("socket peer has another user ID"));
    }
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
        let response = match serde_json::from_slice::<Value>(&raw) {
            Ok(value) => {
                let method = value.get("method").and_then(Value::as_str).unwrap_or("");
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
                        respond(&service, value).await
                    } else if let Some(_permit) = lanes.try_acquire(method) {
                        let request_id = value.get("id").cloned().unwrap_or(Value::Null);
                        let has_id = value.get("id").is_some();
                        match tokio::time::timeout(
                            options.request_timeout,
                            respond(&service, value),
                        )
                        .await
                        {
                            Ok(response) => response,
                            Err(_) => has_id.then(|| {
                                err(request_id, -32002, "API request deadline exceeded", None)
                            }),
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
        if let Some(mut response) = response {
            if serde_json::to_vec(&response)?.len() + 1 > MAX_FRAME {
                response = err(
                    response["id"].clone(),
                    -32603,
                    "response exceeds maximum size",
                    None,
                );
            }
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
    if options.max_connections == 0 || options.max_pending_requests == 0 {
        return Err(crate::error::invalid("socket limits must be positive"));
    }
    if options.request_timeout.is_zero() || options.idle_timeout.is_zero() {
        return Err(crate::error::invalid("socket deadlines must be positive"));
    }
    agent_run_core::logging::configure(home, "api");
    // An older database is only ever upgraded by the paired config migration.
    crate::migrate::require_current_store(home)?;
    // Either a valid schema-2 provider config or a valid schema-1 config.
    if agent_run_config::provider_config::ProviderConfig::load(home).is_err() {
        let _ = crate::config::Config::load(home)?;
    }
    let _ = crate::state::Store::open(home)?;
    let directory = std::fs::symlink_metadata(home)?;
    // SAFETY: geteuid is a read-only process identity query.
    let uid = unsafe { libc::geteuid() };
    if !directory.is_dir()
        || directory.file_type().is_symlink()
        || directory.uid() != uid
        || directory.mode() & 0o077 != 0
    {
        return Err(crate::error::invalid(
            "agent-run home must be an owned mode-0700 directory",
        ));
    }
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
    let _guard = SocketGuard {
        path,
        device: meta.dev(),
        inode: meta.ino(),
        _lock: lock,
    };
    let service = Service::new(home.to_owned());
    let _ = service.reconcile()?;
    let mut services =
        agent_run_core::managed_services::Manager::new(home, std::env::current_exe()?)?;
    // JoinSet aborts maintenance on every exit path, including cancellation of
    // serve_at itself; dropping a bare JoinHandle would detach these loops.
    let mut workers = JoinSet::new();
    let history_home = home.to_owned();
    workers.spawn(async move {
        // Database expiry/compaction and filesystem retention keep independent
        // schedules: a filesystem backlog drains in one-second passes without
        // forcing a database writer transaction every second, and one side
        // deferring, failing or backlogging never changes the other's cadence.
        let mut schedules = MaintenanceSchedules::starting_at(tokio::time::Instant::now());
        loop {
            if schedules.next_due() > tokio::time::Instant::now() {
                tokio::time::sleep_until(schedules.next_due()).await;
            }
            if schedules.next_database <= tokio::time::Instant::now() {
                let home = history_home.clone();
                // SQLite work runs outside the async executor. One job at a time;
                // short SQL deadlines also bound it if the broker task is aborted.
                let result = tokio::task::spawn_blocking(
                    move || -> std::result::Result<usize, (&'static str, Error)> {
                        let mut store =
                            crate::state::Store::open(&home).map_err(|error| ("open", error))?;
                        store
                            .conn
                            .busy_timeout(Duration::from_millis(100))
                            .map_err(|error| ("open", Error::from(error)))?;
                        let now = crate::domain::now();
                        let removed = store.prune_history(now).map_err(|error| ("prune", error))?;
                        // SQLite compaction follows its own idle rules: it only
                        // runs once expiry reports no remaining work, so a page
                        // backlog keeps its own cadence without delaying expiry.
                        Ok(removed
                            + usize::from(
                                removed == 0
                                    && store.vacuum_history().map_err(|error| ("vacuum", error))?,
                            ))
                    },
                )
                .await;
                let at = tokio::time::Instant::now();
                match result {
                    Ok(Ok(removed)) => schedules.database_pass(at, removed),
                    Ok(Err((stage, error))) => {
                        maintenance_diagnostic("history", stage, &error);
                        schedules.next_database = at + MAINTENANCE_RETRY;
                    }
                    Err(_) => {
                        eprintln!("history maintenance: worker failed");
                        schedules.next_database = at + MAINTENANCE_RETRY;
                    }
                }
            }
            if schedules.next_filesystem <= tokio::time::Instant::now() {
                let home = history_home.clone();
                // Filesystem retention runs on its own schedule, even while
                // database batches still report work, so a large journal
                // backlog can never starve reclaiming disposable files on disk.
                let result = tokio::task::spawn_blocking(move || -> Result<usize> {
                    let mut store = crate::state::Store::open(&home)?;
                    store.conn.busy_timeout(Duration::from_millis(100))?;
                    let now = crate::domain::now();
                    agent_run_core::housekeeping::sweep(&home, now, &mut store)
                })
                .await;
                let at = tokio::time::Instant::now();
                match result {
                    Ok(Ok(removed)) => schedules.filesystem_pass(at, removed),
                    Ok(Err(error)) => {
                        maintenance_diagnostic("filesystem", "sweep", &error);
                        schedules.next_filesystem = at + MAINTENANCE_RETRY;
                    }
                    Err(_) => {
                        eprintln!("filesystem maintenance: worker failed");
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
    spawn_reconcile_loop(&mut workers, Duration::from_secs(1), move || {
        let _ = reconciler.reconcile();
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
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            _=term.recv()=>break,
            accepted=listener.accept()=>{
                let(mut stream,_)=accepted?;
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
                handlers.spawn(async move { let _permit=permit; let _=connection(stream,service,lanes,control_only,options).await; });
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
    Ok(())
}
/// Spawns the serial lease/process reconciliation loop on `workers`.
///
/// `run` is synchronous and may block (cleanup sleeps, SQLite waits), so every
/// pass runs on the blocking pool and is awaited before the next tick: at most
/// one pass is active, missed ticks are delayed rather than queued, and the
/// delivery loop never waits on it. Aborting the task (shutdown) stops the loop
/// at its next await; an already running bounded pass is left to finish rather
/// than being cut mid-cleanup.
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
    if method == "resume" {
        return BrokerClient::new(home.join("api.sock"))
            .call_with_timeout(method, Some(params), 120.0)
            .await;
    }
    client_with_wait_deadlines(
        home,
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

/// Calls the broker, bounding wait observations and retrying only timed-out observations.
async fn client_with_wait_deadlines(
    home: &Path,
    method: &str,
    mut params: Value,
    wait_seconds: Duration,
    wait_call_timeout: Duration,
) -> Result<Value> {
    let is_wait = method == "wait";
    if is_wait {
        if let Some(object) = params.as_object_mut() {
            object.insert("timeout_seconds".into(), json!(wait_seconds.as_secs_f64()));
        }
    }
    loop {
        let mut stream = tokio::time::timeout(
            Duration::from_secs(3),
            UnixStream::connect(home.join("api.sock")),
        )
        .await
        .map_err(|_| Error::BrokerUnavailable)?
        .map_err(|_| Error::BrokerUnavailable)?;
        // SAFETY: geteuid is a read-only query.
        if stream.peer_cred()?.uid() != unsafe { libc::geteuid() } {
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
            Error::Runtime(
                "broker request write deadline exceeded; admission may be ambiguous".into(),
            )
        })??;
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
                Error::Runtime(
                    "broker response deadline exceeded; an admitted run is not cancelled".into(),
                )
            })??;
        let value: Value =
            serde_json::from_slice(&raw.ok_or_else(|| {
                Error::Runtime("broker disconnected before its response".into())
            })?)?;
        if value.get("id").and_then(Value::as_str) != Some(id.as_str())
            || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        {
            return Err(crate::error::invalid("invalid broker response identity"));
        }
        if let Some(error) = value.get("error") {
            return Err(error
                .as_object()
                .map(broker_error)
                .unwrap_or_else(|| Error::Runtime("broker request failed".into())));
        }
        if is_wait && value["result"]["timed_out"] == true {
            continue;
        }
        return value
            .get("result")
            .cloned()
            .ok_or_else(|| crate::error::invalid("broker returned neither result nor error"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    /// Pool discovery consumes only the read lane and no control reservation.
    #[test]
    fn list_pools_uses_the_read_lane() {
        let lanes = Lanes::new(0);
        let read = lanes.try_acquire("list_pools").unwrap();
        assert!(lanes.try_acquire("list_agents").is_none());
        assert!(lanes.try_acquire("start_pool").is_some());
        drop(read);
        assert!(lanes.try_acquire("list_pools").is_some());
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
            home.path(),
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
            home.path(),
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
}
