//! Bounded LF-delimited JSON-RPC 2.0 over a private same-user Unix socket.
use super::frame;
use crate::{dispatch, service::Service, Error, Result};
use agent_run_domain::types::StartRequest;
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
    /// Durable id assigned by the broker.
    pub agent_id: String,
    /// Whether this request admitted a new row rather than replaying one.
    pub created: bool,
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
    pub async fn call_with_timeout(
        &self,
        method: &str,
        params: Option<Value>,
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

    /// Serializes a typed start request and rejects malformed broker acknowledgements.
    pub async fn start(&self, request: &StartRequest) -> Result<BrokerStartResult> {
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
        Ok(BrokerStartResult {
            agent_id: agent_id.into(),
            created,
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
            let message = object
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("broker request failed")
                .to_owned();
            if object.get("code").and_then(Value::as_i64) == Some(-32602) {
                return Err(ClientAttemptError::Domain(Error::Validation(message)));
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
            return Err(ClientAttemptError::Domain(Error::Broker {
                message,
                broker_error_code,
                broker_error_data,
            }));
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
const CONTROL_METHODS: &[&str] = &["start", "resume", "cancel", "steer"];

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
    fn new() -> Self {
        Self {
            control: Arc::new(Semaphore::new(MAX_PENDING_REQUESTS)),
            read: Arc::new(Semaphore::new(MAX_PENDING_REQUESTS)),
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
    let known = dispatch::is_tool(method) || ["tools", "ping", "wait"].contains(&method);
    let response = if !known {
        err(id, -32601, "method not found", None)
    } else {
        match dispatch::call(service, method, params).await {
            Ok(value) => json!({"jsonrpc":"2.0","id":id,"result":value}),
            Err(e) => {
                if e.rpc_code() == -32602 {
                    err(id, -32602, &e.public().message, None)
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
            Duration::from_secs(120)
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
                        respond(&service, value).await
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
    agent_run_core::logging::configure(home, "api");
    let _ = crate::config::Config::load(home)?;
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
    let maintenance = service.clone();
    let worker = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let _ = maintenance.reconcile();
            if let Err(e) = crate::delivery::dispatch_once(&maintenance.home).await {
                eprintln!("delivery maintenance: {}", e.public().kind);
            }
        }
    });
    let gate = Arc::new(Semaphore::new(MAX_CONNECTIONS - 1));
    let control_gate = Arc::new(Semaphore::new(1));
    let lanes = Lanes::new();
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
                handlers.spawn(async move { let _permit=permit; let _=connection(stream,service,lanes,control_only).await; });
            }
        }
    }
    worker.abort();
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
/// A separate connection for every call; no shared response-routing state.
pub async fn client(home: &Path, method: &str, params: Value) -> Result<Value> {
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
        Error::Runtime("broker request write deadline exceeded; admission may be ambiguous".into())
    })??;
    let mut input = BufReader::new(stream);
    // wait deliberately has no client-owned execution deadline.
    let raw = if method == "wait" {
        frame::read(&mut input, MAX_FRAME).await?
    } else {
        tokio::time::timeout(Duration::from_secs(120), frame::read(&mut input, MAX_FRAME))
            .await
            .map_err(|_| {
                Error::Runtime(
                    "broker response deadline exceeded; an admitted run is not cancelled".into(),
                )
            })??
    };
    let value: Value = serde_json::from_slice(
        &raw.ok_or_else(|| Error::Runtime("broker disconnected before its response".into()))?,
    )?;
    if value.get("id").and_then(Value::as_str) != Some(id.as_str())
        || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
    {
        return Err(crate::error::invalid("invalid broker response identity"));
    }
    if let Some(error) = value.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("broker request failed")
            .chars()
            .take(512)
            .collect();
        return Err(
            if error.get("code").and_then(Value::as_i64) == Some(-32602) {
                Error::Validation(message)
            } else {
                Error::Runtime(message)
            },
        );
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| crate::error::invalid("broker returned neither result nor error"))
}
