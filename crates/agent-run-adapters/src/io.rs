//! Bounded engine streams with independent observation of the primary harness PID.

use crate::{
    LaunchPlan,
    redact::{DiagnosticTail, Redactor, StreamingRedactor},
};
use agent_run_domain::{Error, Result};
use agent_run_platform::{
    frame,
    process::{OwnedProcess, OwnershipSnapshot},
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
};
/// Maximum UTF-8 JSON-RPC line retained from an engine before failing closed.
pub const ENGINE_FRAME: usize = 8 * 1024 * 1024;

/// Fixed capacity of interleaved notifications retained while one RPC waits.
///
/// The bound is deliberately not configurable: a control exchange must never
/// trade unbounded memory for liveness, and a full backlog is a typed
/// disposition instead of a dropped notification.
pub const RPC_BACKLOG_LIMIT: usize = 64;

/// Delivery disposition of one engine RPC exchange.
///
/// Proven outcomes are separated from merely unknown ones: a correlated
/// native reply proves acceptance or rejection, a full backlog before the
/// send proves the request was never written, and every bounded end of the
/// wait after a possible write is explicitly uncertain — the request may
/// have reached the engine. Interleaved notifications are retained in order
/// up to [`RPC_BACKLOG_LIMIT`]; when the backlog is full the exchange stops
/// before reading, so the next engine event stays in the channel.
#[derive(Debug, Clone, PartialEq)]
pub enum RpcDisposition {
    /// A correlated native reply carried a result.
    Replied(Value),
    /// A correlated rejection supplied an integer or bounded nonblank string
    /// code; malformed envelopes never prove rejection.
    Rejected { code: String },
    /// The backlog was already full before the send, so nothing was written.
    UnsentPressure,
    /// The send may have completed, but no correlated reply was observed
    /// before the bounded reason ended the wait.
    Uncertain(RpcUncertain),
}

/// The finite reason an in-flight exchange ended without a correlated reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcUncertain {
    /// The caller's bounded deadline expired.
    Timeout,
    /// The backlog filled after the send; the wait stopped with the next
    /// engine event still unread in the channel.
    Pressure,
    /// The transport closed after the send.
    Closed,
    /// The trusted reader/ownership observer's finite failure category.
    Transport(&'static str),
    /// A correlated envelope was not a valid result or rejection.
    MalformedReply,
    /// Writing a request or denial failed; a typed write timeout is Timeout.
    Send,
}

impl RpcUncertain {
    /// The bounded, machine-readable reason name used in durable results.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Timeout => "uncertain_timeout",
            Self::Pressure => "uncertain_backlog_pressure",
            Self::Closed => "uncertain_transport_closed",
            Self::Transport(_) => "uncertain_transport_failure",
            Self::MalformedReply => "uncertain_malformed_reply",
            Self::Send => "uncertain_send_failed",
        }
    }
}

/// Synchronous durable checkpoint installed by the supervisor; receives identity metadata only.
type OwnershipObserver = Box<dyn FnMut(&OwnershipSnapshot) -> Result<()> + Send>;

/// One bounded result from the engine stdout reader.
#[derive(Debug)]
pub enum Event {
    /// A complete parsed JSON envelope.
    Json(Value),
    /// The child closed stdout before a complete protocol terminal event.
    Eof,
    /// Framing or JSON decoding failed without retaining unbounded input.
    Failure(&'static str),
}

/// Returns whether an adapter write failed because the child closed its pipe.
fn is_broken_pipe(error: &Error) -> bool {
    matches!(error, Error::Io(source) if source.kind() == std::io::ErrorKind::BrokenPipe)
}

/// Validates a positive caller budget and caps running controls at one second.
/// Startup/discovery methods retain their supplied duration; no new budget is
/// created while replying to unsolicited requests or collecting diagnostics.
fn rpc_timeout(method: &str, timeout: Duration) -> Result<Duration> {
    if timeout.is_zero() {
        return Err(Error::Validation("RPC timeout must be positive".into()));
    }
    Ok(if matches!(method, "turn/steer" | "turn/interrupt") {
        timeout.min(Duration::from_secs(1))
    } else {
        timeout
    })
}

/// Classifies only typed write failures: timer expiry is Timeout, a broken
/// pipe/closed input is Closed, and other failures remain Send uncertainty.
/// It never guesses native acceptance or parses arbitrary diagnostic words.
fn rpc_send_uncertainty(error: &Error, input_closed: bool) -> RpcUncertain {
    if matches!(error, Error::Io(io) if io.kind() == std::io::ErrorKind::TimedOut) {
        RpcUncertain::Timeout
    } else if is_broken_pipe(error) || input_closed {
        RpcUncertain::Closed
    } else {
        RpcUncertain::Send
    }
}

/// Owns an app-server child, its bounded streams, and request correlation state.
pub struct Process {
    pub child: Child,
    pub input: Option<ChildStdin>,
    pub owner: OwnedProcess,
    events: mpsc::Receiver<Event>,
    tasks: Vec<JoinHandle<()>>,
    pub backlog: VecDeque<Event>,
    next_id: u64,
    pub stderr_bytes: Arc<AtomicU64>,
    redactor: Redactor,
    diagnostic_tail: Arc<Mutex<DiagnosticTail>>,
    /// True once waitpid proves the primary exited, independent of stdout ownership.
    primary_exited: bool,
    /// Deadline of the current idle read after exit; survives cancellation, not consumer processing time.
    exit_drain_deadline: Option<tokio::time::Instant>,
    /// One fixed escalation deadline for captured descendants still writing after primary exit.
    exit_kill_deadline: Option<tokio::time::Instant>,
    /// Last descendant snapshot, shared by startup RPC and streaming reads.
    observed_at: tokio::time::Instant,
    /// Optional persistence boundary supplied by core without coupling adapters to SQLite.
    ownership_observer: Option<OwnershipObserver>,
    /// Last successfully persisted append-only capture revision.
    ownership_revision: Option<(usize, bool)>,
}
impl Process {
    /// Spawns the planned engine with isolated environment and line-framed pipes.
    pub fn spawn(plan: &LaunchPlan) -> Result<Self> {
        let redactor = Redactor::from_environment(&plan.environment);
        let mut command = Command::new(&plan.binary);
        command
            .args(&plan.args)
            .current_dir(&plan.cwd)
            .env_clear()
            .envs(&plan.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
        let mut child = command.spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| Error::Runtime("child PID unavailable".into()))?
            as i32;
        let owner = OwnedProcess::capture(pid);
        let input = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Runtime("child stdout unavailable".into()))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::Runtime("child stderr unavailable".into()))?;
        let (send, events) = mpsc::channel(8);
        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                let event = match frame::read(&mut reader, ENGINE_FRAME).await {
                    Ok(Some(data)) => {
                        if data.iter().all(u8::is_ascii_whitespace) {
                            continue;
                        }
                        match serde_json::from_slice(&data) {
                            Ok(v) => Event::Json(v),
                            Err(_) => Event::Failure("malformed_engine_json"),
                        }
                    }
                    Ok(None) => Event::Eof,
                    Err(_) => Event::Failure("engine_transport_failure"),
                };
                let done = matches!(event, Event::Eof | Event::Failure(_));
                if send.send(event).await.is_err() || done {
                    break;
                }
            }
        });
        let stderr_bytes = Arc::new(AtomicU64::new(0));
        let count = stderr_bytes.clone();
        let diagnostic_tail = Arc::new(Mutex::new(DiagnosticTail::new(redactor.clone())));
        let tail = diagnostic_tail.clone();
        let errors = tokio::spawn(async move {
            let mut bytes = [0u8; 8192];
            while let Ok(n) = stderr.read(&mut bytes).await {
                if n == 0 {
                    break;
                }
                count.fetch_add(n as u64, Ordering::Relaxed);
                if let Ok(mut tail) = tail.lock() {
                    tail.push(&bytes[..n]);
                }
            }
        });
        Ok(Self {
            child,
            input,
            owner,
            events,
            tasks: vec![reader, errors],
            backlog: VecDeque::new(),
            next_id: 1,
            stderr_bytes,
            redactor,
            diagnostic_tail,
            exit_drain_deadline: None,
            primary_exited: false,
            exit_kill_deadline: None,
            observed_at: tokio::time::Instant::now(),
            ownership_observer: None,
            ownership_revision: None,
        })
    }

    /// Attaches the supervisor's durable ownership writer and immediately checkpoints the current root.
    /// Errors propagate so the supervisor cleans up instead of running without required evidence.
    pub fn observe_ownership(
        &mut self,
        observer: impl FnMut(&OwnershipSnapshot) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        self.ownership_observer = Some(Box::new(observer));
        self.ownership_revision = None;
        self.checkpoint_ownership()
    }

    /// Persists newly captured members once; successful unchanged captures never touch the database.
    pub fn checkpoint_ownership(&mut self) -> Result<()> {
        let Some(observer) = self.ownership_observer.as_mut() else {
            return Ok(());
        };
        let revision = self.owner.capture_revision();
        if self.ownership_revision == Some(revision) {
            return Ok(());
        }
        let snapshot = self
            .owner
            .snapshot()
            .ok_or_else(|| Error::Integrity("process root identity is unavailable".into()))?;
        observer(&snapshot)?;
        self.ownership_revision = Some(revision);
        Ok(())
    }

    /// Redacts launch secrets from text before core code persists it.
    pub fn redact(&self, text: &str) -> String {
        self.redactor.redact(text)
    }

    /// Sanitizes one parsed engine event before durable storage.
    pub fn redact_value(&self, value: &Value) -> Value {
        self.redactor.redact_value(value)
    }

    /// Creates one message-local redactor for streamed stdout fragments.
    pub fn stream_redactor(&self) -> StreamingRedactor {
        self.redactor.stream()
    }

    /// Returns the redacted bounded stderr evidence retained for a failed launch.
    pub fn diagnostic_tail(&self) -> Option<String> {
        self.diagnostic_tail
            .lock()
            .ok()
            .and_then(|tail| tail.text())
    }
    /// Writes one bounded JSON-RPC envelope to the owned child within 15 seconds.
    pub async fn send(&mut self, message: &Value) -> Result<()> {
        self.send_until(
            message,
            tokio::time::Instant::now() + Duration::from_secs(15),
        )
        .await
    }
    /// Writes one engine envelope before a caller-owned deadline.
    pub async fn send_before(
        &mut self,
        message: &Value,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        self.send_until(message, deadline).await
    }
    /// Writes one frame before a caller-owned deadline, including pipe backpressure.
    async fn send_until(&mut self, message: &Value, deadline: tokio::time::Instant) -> Result<()> {
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| Error::Runtime("engine stdin is closed".into()))?;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "engine input delivery timed out",
            )
            .into());
        }
        tokio::time::timeout(remaining, frame::write(input, message, ENGINE_FRAME))
            .await
            .map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "engine input delivery timed out",
                ))
            })?
    }
    /// Writes raw text for non-JSON stream engines while retaining the same deadline.
    pub async fn text(&mut self, text: &str) -> Result<()> {
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| Error::Runtime("engine stdin is closed".into()))?;
        tokio::time::timeout(Duration::from_secs(15), input.write_all(text.as_bytes()))
            .await
            .map_err(|_| Error::Runtime("engine input delivery timed out".into()))??;
        input.flush().await?;
        Ok(())
    }
    /// Returns a retained notification or observes stdout and the primary PID together.
    pub async fn next(&mut self) -> Event {
        if !self.backlog.is_empty() {
            if self.observe_primary_exit().is_err() {
                return Event::Failure("engine_process_observation_failed");
            }
            if self.checkpoint_ownership().is_err() {
                return Event::Failure("engine_process_ownership_failed");
            }
            self.backlog.pop_front().expect("checked backlog")
        } else {
            self.receive().await
        }
    }
    /// Starts TERM on primary exit and escalates captured survivors after two seconds, even during output traffic.
    fn observe_primary_exit(&mut self) -> Result<()> {
        if !self.primary_exited && self.child.try_wait()?.is_some() {
            self.primary_exited = true;
            self.owner.signal_descendants(libc::SIGTERM);
            self.input.take();
            self.exit_kill_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(2));
        }
        if self
            .exit_kill_deadline
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            self.owner.signal_descendants(libc::SIGKILL);
            self.exit_kill_deadline = None;
        }
        Ok(())
    }
    /// Begins descendant termination on observed primary exit, then drains already-written output.
    ///
    /// Each idle wait after exit has a 200ms deadline, preserved across cancellation.
    /// Receiving a frame ends that wait, so slow downstream processing cannot discard
    /// queued output. A separate fixed deadline escalates captured writers; the run's
    /// overall deadline also bounds any unobserved writers. The supervisor owns final proof.
    async fn receive(&mut self) -> Event {
        loop {
            let now = tokio::time::Instant::now();
            if now.duration_since(self.observed_at) >= Duration::from_millis(200) {
                self.owner.refresh();
                self.observed_at = now;
            }
            if self.observe_primary_exit().is_err() {
                return Event::Failure("engine_process_observation_failed");
            }
            if self.checkpoint_ownership().is_err() {
                return Event::Failure("engine_process_ownership_failed");
            }
            if self.primary_exited {
                let deadline = *self.exit_drain_deadline.get_or_insert_with(|| {
                    tokio::time::Instant::now() + Duration::from_millis(200)
                });
                let event = tokio::select! {
                    biased;
                    event = self.events.recv() => event.unwrap_or(Event::Eof),
                    _ = tokio::time::sleep_until(deadline) => Event::Eof,
                };
                if !matches!(event, Event::Eof) {
                    self.exit_drain_deadline = None;
                }
                return event;
            }
            tokio::select! {
                biased;
                exit = self.child.wait() => {
                    if exit.is_err() { return Event::Failure("engine_process_observation_failed"); }
                    // The next iteration records exit and sends the first TERM sweep.
                }
                event = self.events.recv() => return event.unwrap_or(Event::Eof),
                _ = tokio::time::sleep_until(self.observed_at + Duration::from_millis(200)) => {},
            }
        }
    }
    /// Sends one request, correlates its reply and retains interleaved events.
    /// Control methods keep their one-second cap. Startup callers retain timeout,
    /// closed-stream and finite reader/ownership diagnostics. Closed diagnostics
    /// use only the original remaining budget; no deadline is renewed.
    pub async fn rpc(&mut self, method: &str, params: Value, timeout: Duration) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + rpc_timeout(method, timeout)?;
        match self.rpc_exchange_until(method, params, deadline).await? {
            RpcDisposition::Replied(result) => Ok(result),
            RpcDisposition::Rejected { code } => Err(Error::Runtime(format!(
                "app-server rejected {method} ({code})"
            ))),
            RpcDisposition::UnsentPressure => Err(Error::Runtime(
                "app-server control backlog is full; request not sent".into(),
            )),
            RpcDisposition::Uncertain(RpcUncertain::Timeout) => {
                Err(Error::Runtime("app-server RPC timed out".into()))
            }
            RpcDisposition::Uncertain(RpcUncertain::Pressure) => Err(Error::Runtime(format!(
                "app-server backlog filled while waiting for {method}"
            ))),
            RpcDisposition::Uncertain(RpcUncertain::Closed) => {
                Err(self.closed_error(method, deadline).await)
            }
            RpcDisposition::Uncertain(RpcUncertain::Transport(kind)) => {
                Err(Error::Runtime(kind.into()))
            }
            RpcDisposition::Uncertain(RpcUncertain::MalformedReply) => {
                Err(Error::Runtime("RPC result is missing or malformed".into()))
            }
            RpcDisposition::Uncertain(reason) => Err(Error::Runtime(format!(
                "app-server RPC for {method} ended {}",
                reason.reason()
            ))),
        }
    }

    /// Runs one lossless bounded exchange. Zero timeout rejects before writing;
    /// running controls are capped at one second. Only correlated results/errors
    /// prove acceptance/rejection. Preflight pressure proves no write; all ends
    /// after a possible send, including denial failure, are explicitly uncertain.
    /// The fixed backlog is checked before receive, leaving event 65 unread.
    pub async fn rpc_exchange(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<RpcDisposition> {
        let deadline = tokio::time::Instant::now() + rpc_timeout(method, timeout)?;
        self.rpc_exchange_until(method, params, deadline).await
    }

    /// Executes a request using the caller's unchanged absolute deadline,
    /// including replies to unsolicited requests. IDs are allocated only after
    /// unsent pressure refusal. No post-send transport/protocol failure aborts
    /// a running control; its uncertainty is returned for durable recording.
    async fn rpc_exchange_until(
        &mut self,
        method: &str,
        params: Value,
        deadline: tokio::time::Instant,
    ) -> Result<RpcDisposition> {
        if self.backlog.len() >= RPC_BACKLOG_LIMIT {
            return Ok(RpcDisposition::UnsentPressure);
        }
        let id = self.next_id;
        self.next_id += 1;
        if let Err(error) = self
            .send_until(&json!({"id":id,"method":method,"params":params}), deadline)
            .await
        {
            return Ok(RpcDisposition::Uncertain(rpc_send_uncertainty(
                &error,
                self.input.is_none(),
            )));
        }
        loop {
            if self.backlog.len() >= RPC_BACKLOG_LIMIT {
                return Ok(RpcDisposition::Uncertain(RpcUncertain::Pressure));
            }
            let event = match tokio::time::timeout_at(deadline, self.receive()).await {
                Ok(event) => event,
                Err(_) => return Ok(RpcDisposition::Uncertain(RpcUncertain::Timeout)),
            };
            match event {
                Event::Json(v)
                    if v.get("id").and_then(Value::as_u64) == Some(id)
                        && v.get("method").is_none() =>
                {
                    if let Some(error) = v.get("error") {
                        let code = error.get("code").filter(|code| {
                            code.as_i64().is_some()
                                || code.as_u64().is_some()
                                || code.as_str().is_some_and(|code| {
                                    !code.trim().is_empty()
                                        && code.chars().count() <= 64
                                        && !code.chars().any(char::is_control)
                                })
                        });
                        if v.get("result").is_some() || code.is_none() {
                            return Ok(RpcDisposition::Uncertain(RpcUncertain::MalformedReply));
                        }
                        let code = code.expect("validated rejection code").to_string();
                        return Ok(RpcDisposition::Rejected {
                            code: code.chars().take(64).collect(),
                        });
                    }
                    return Ok(match v.get("result") {
                        Some(result) => RpcDisposition::Replied(result.clone()),
                        None => RpcDisposition::Uncertain(RpcUncertain::MalformedReply),
                    });
                }
                Event::Json(v) if v.get("method").is_some() && v.get("id").is_some() => {
                    if let Err(error) = self.deny_request_before(&v, deadline).await {
                        return Ok(RpcDisposition::Uncertain(rpc_send_uncertainty(
                            &error,
                            self.input.is_none(),
                        )));
                    }
                }
                Event::Json(v) => self.backlog.push_back(Event::Json(v)),
                Event::Eof => return Ok(RpcDisposition::Uncertain(RpcUncertain::Closed)),
                Event::Failure(kind) => {
                    return Ok(RpcDisposition::Uncertain(RpcUncertain::Transport(kind)))
                }
            }
        }
    }

    /// Returns only an issued, bounded numeric response ID, never arbitrary
    /// native JSON. The envelope must carry result/error and no method. This
    /// identifies late/duplicate replies without claiming model consumption.
    pub fn rpc_reply_id(&self, value: &Value) -> Option<u64> {
        if value.get("method").is_some()
            || (value.get("result").is_none() && value.get("error").is_none())
        {
            return None;
        }
        value
            .get("id")?
            .as_u64()
            .filter(|id| *id > 0 && *id < self.next_id)
    }
    /// Describes an early child exit using only bounded, redacted diagnostics.
    ///
    /// Stderr draining receives at most one second of the caller's remaining
    /// deadline, followed by at most 100 milliseconds for the child status.
    /// Neither wait may extend the RPC deadline.
    async fn closed_error(&mut self, method: &str, deadline: tokio::time::Instant) -> Error {
        let stderr_deadline = deadline.min(tokio::time::Instant::now() + Duration::from_secs(1));
        let stderr_drained = if let Some(stderr_reader) = self.tasks.last_mut() {
            tokio::time::timeout_at(stderr_deadline, stderr_reader)
                .await
                .is_ok()
        } else {
            false
        };
        if stderr_drained {
            self.tasks.pop();
        }
        let mut status = self.child.try_wait().ok().flatten();
        if status.is_none() {
            let exit_deadline =
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(100));
            if exit_deadline > tokio::time::Instant::now()
                && let Ok(Ok(exited)) =
                    tokio::time::timeout_at(exit_deadline, self.child.wait()).await
            {
                status = Some(exited);
            }
        }
        let code = status
            .and_then(|status| status.code())
            .map(|code| format!("exit code {code}"))
            .unwrap_or_else(|| "unknown exit status".into());
        let detail = self
            .diagnostic_tail()
            .map(|tail| format!(": {tail}"))
            .unwrap_or_default();
        Error::Runtime(format!(
            "app-server closed the stream while waiting for {method}: {code}{detail}"
        ))
    }
    /// Declines an unsolicited server request with the historical 15-second
    /// write budget. It never grants native permissions or process authority.
    pub async fn deny_request(&mut self, v: &Value) -> Result<()> {
        self.deny_request_before(v, tokio::time::Instant::now() + Duration::from_secs(15))
            .await
    }

    /// Sends a native refusal within the caller's absolute deadline. Protocol
    /// selection is shared with ordinary denial; typed timeout/I/O errors
    /// propagate, and neither serialization nor write renews the RPC budget.
    pub async fn deny_request_before(
        &mut self,
        v: &Value,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let method = v.get("method").and_then(Value::as_str).unwrap_or("");
        let response = if method == "item/permissions/requestApproval" {
            json!({"id":v["id"],"result":{"permissions":{},"scope":"turn"}})
        } else if method.ends_with("requestApproval") {
            json!({"id":v["id"],"result":{"decision":"decline"}})
        } else {
            json!({"id":v["id"],"error":{"code":-32601,"message":"interactive request unavailable in durable headless execution"}})
        };
        self.send_until(&response, deadline).await
    }
    /// Waits briefly for child exit and bounded reader cleanup before releasing pipes.
    pub async fn reap(&mut self) -> Option<i32> {
        let code = match tokio::time::timeout(Duration::from_secs(3), self.child.wait()).await {
            Ok(Ok(s)) => s.code(),
            _ => None,
        };
        for mut task in self.tasks.drain(..) {
            if tokio::time::timeout(Duration::from_secs(1), &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        }
        code
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    //! Deterministic classifications for write-side process transport failures.

    use super::is_broken_pipe;
    use agent_run_domain::{Error, MachineCode};
    use std::io::ErrorKind;

    /// Keeps unrelated write failures in the generic I/O error category.
    #[test]
    fn non_broken_pipe_write_errors_retain_io_classification() {
        let error = Error::Io(std::io::Error::new(
            ErrorKind::PermissionDenied,
            "fixture write denied",
        ));

        assert!(!is_broken_pipe(&error));
        assert_eq!(error.machine_code(), MachineCode::IOError);
    }
}
