//! Rust MCP stdio compatibility proxy; durable execution stays in the broker.
use crate::{cli::CliBroker, dispatch, domain::OrchestratorRef, Error, Result};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResponse, ErrorData, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerConfig, Tool,
    },
    service::{RequestContext, RoleServer},
    ServerHandler, ServiceExt,
};
use serde_json::{json, Value};
use std::{
    ffi::CString,
    os::unix::{ffi::OsStrExt, process::CommandExt},
    path::Path,
    path::PathBuf,
    pin::Pin,
    process::Command,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Match the Python broker and MCP stdin ceiling before the SDK allocates a frame.
const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Preserve Python's stable, operator-actionable broker-unavailable wording.
const BROKER_UNAVAILABLE: &str =
    "agent-run broker is not running; start it with `agent-run api serve` or its launchd job";

/// Fixed diagnostic for an optional Desktop frontend that cannot replace this process.
const DESKTOP_FRONTEND_UNAVAILABLE: &str =
    "agent-run: Desktop MCP frontend unavailable; continuing without relay delivery";

/// Return whether `path` names a regular file executable by the current process identity.
fn is_executable_file(path: &Path) -> bool {
    path.is_file()
        && CString::new(path.as_os_str().as_bytes())
            // SAFETY: the CString is NUL-terminated and remains alive for the read-only query.
            .is_ok_and(|path| unsafe { libc::access(path.as_ptr(), libc::X_OK) == 0 })
}

/// Replace a production Desktop MCP process with the host-supplied signed Node frontend.
///
/// The replacement occurs only when both absolute capability paths are present. The frontend
/// receives the current executable, resolved home, trusted notice contract, and exact original
/// argument vector without shell interpretation. It removes both capability variables before
/// starting the Rust MCP child, which makes recursion impossible and keeps native host access in
/// the signed process. Successful replacement never returns. Missing, malformed, non-executable,
/// or failed optional frontends emit one fixed
/// diagnostic and leave the caller to run the direct Rust MCP without native-host access.
pub fn exec_desktop_frontend(home: &Path) -> Result<()> {
    let (Some(pipe), Some(node)) = (
        std::env::var_os("CODEX_APP_TOOLS_PIPE_PATH"),
        std::env::var_os("CODEX_MCP_NODE_PATH"),
    ) else {
        return Ok(());
    };
    let pipe = PathBuf::from(pipe);
    let node = PathBuf::from(node);
    if !pipe.is_absolute() || !node.is_absolute() || !is_executable_file(&node) {
        eprintln!("{DESKTOP_FRONTEND_UNAVAILABLE}");
        // Direct MCP only proxies stdio to the resident broker. It opens no native-host client
        // and spawns no engine, so these capabilities remain inert without unsafe environment
        // mutation after Tokio worker threads have started.
        return Ok(());
    }
    let Ok(executable) = std::env::current_exe() else {
        eprintln!("{DESKTOP_FRONTEND_UNAVAILABLE}");
        return Ok(());
    };
    let _error = Command::new(node)
        .arg("-e")
        .arg(include_str!("../../../../assets/desktop-transport.cjs"))
        .arg("--")
        .arg(executable)
        .arg(home)
        .arg(include_str!("../../../../assets/completion_notice.json"))
        .args(std::env::args_os().skip(1))
        .exec();
    eprintln!("{DESKTOP_FRONTEND_UNAVAILABLE}");
    // The failed exec leaves this same direct, capability-inert MCP process in place.
    Ok(())
}

/// Bound stdin one LF-delimited MCP frame at a time for the SDK transport.
struct BoundedReader<R> {
    /// Input byte stream whose bytes are fed to the MCP SDK.
    inner: R,
    /// Bytes received since the most recent LF delimiter.
    line_bytes: usize,
}

impl BoundedReader<tokio::io::Stdin> {
    /// Wrap process stdin while retaining only the current frame's byte count.
    fn new() -> Self {
        Self {
            inner: tokio::io::stdin(),
            line_bytes: 0,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BoundedReader<R> {
    /// Read one chunk and reject a frame before it can grow beyond the wire limit.
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let start = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                for byte in &buf.filled()[start..] {
                    if *byte == b'\n' {
                        self.line_bytes = 0;
                    } else {
                        self.line_bytes += 1;
                        if self.line_bytes > MAX_FRAME_BYTES {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "MCP frame exceeds maximum size",
                            )));
                        }
                    }
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// Process-stdin specialization used by the production MCP transport.
type BoundedStdin = BoundedReader<tokio::io::Stdin>;

/// Bound stdout one LF-delimited MCP response frame before it reaches the host.
struct BoundedStdout {
    /// The process stdout used exclusively for MCP protocol responses.
    inner: tokio::io::Stdout,
    /// Bytes written since the most recent LF delimiter.
    line_bytes: usize,
}

impl BoundedStdout {
    /// Wrap process stdout while retaining only the current frame's byte count.
    fn new() -> Self {
        Self {
            inner: tokio::io::stdout(),
            line_bytes: 0,
        }
    }

    /// Reject a write that would extend any single LF-delimited frame beyond the limit.
    fn accepts(&self, bytes: &[u8]) -> bool {
        let mut line_bytes = self.line_bytes;
        for byte in bytes {
            if *byte == b'\n' {
                line_bytes = 0;
            } else {
                line_bytes += 1;
                if line_bytes > MAX_FRAME_BYTES {
                    return false;
                }
            }
        }
        true
    }

    /// Commit byte accounting only after the underlying stdout accepted the same prefix.
    fn account(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if *byte == b'\n' {
                self.line_bytes = 0;
            } else {
                self.line_bytes += 1;
            }
        }
    }
}

impl AsyncWrite for BoundedStdout {
    /// Write a bounded protocol prefix or reject the response before an oversized frame leaks.
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if !self.accepts(buf) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "MCP frame exceeds maximum size",
            )));
        }
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                self.account(&buf[..written]);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    /// Flush only the protocol stdout owned by this wrapper.
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    /// Close only the protocol stdout owned by this wrapper.
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Retain the broker location and optional host binding for one MCP session.
#[derive(Clone)]
pub struct Proxy {
    /// Resident broker used for tool calls.
    pub broker: Arc<dyn CliBroker>,
    /// Agent-run home retained for relay setup and diagnostics.
    pub home: PathBuf,
    /// Optional orchestrator binding added to admission requests.
    pub orchestrator: Option<OrchestratorRef>,
}
impl ServerHandler for Proxy {
    /// Report only the Python-compatible MCP capabilities and implementation identity.
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default();
        info.capabilities = serde_json::from_value(json!({
            "experimental": {}, "tools": {"listChanged": false}
        }))
        .expect("Python-compatible MCP capabilities are valid");
        info.server_info = Implementation::new("agent-run", "1");
        info
    }
    /// Serve the single packaged Python-equivalent tool table without pagination.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, ErrorData> {
        let tools: Vec<Tool> = dispatch::tools()
            .into_iter()
            .map(serde_json::from_value)
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| ErrorData::internal_error("invalid packaged tool schema", None))?;
        Ok(ListToolsResult::with_all_items(tools))
    }
    /// Look up one advertised tool so rmcp can route its call without a second registry.
    fn get_tool(&self, name: &str) -> Option<Tool> {
        dispatch::tools()
            .into_iter()
            .find(|v| v["name"].as_str() == Some(name))
            .and_then(|v| serde_json::from_value(v).ok())
    }
    /// Inject the host binding and forward the call through the resident broker.
    /// Return completed text results, including typed tool failures; SDK negotiation owns framing.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, ErrorData> {
        if !dispatch::is_tool(request.name.as_ref()) {
            return Ok(crate::transport::mcp_text::error_result(
                "unknown_tool",
                format!("unknown tool: {}", request.name).as_str(),
            )
            .into());
        }
        let mut arguments = request.arguments.unwrap_or_default();
        // Read tools accept exactly the arguments their shared registry
        // schema declares (none for `limits` and `delegation_guide`, exact
        // filters for the others).
        if matches!(
            request.name.as_ref(),
            "capacity_order" | "models" | "limits" | "delegation_guide"
        ) {
            let declared = dispatch::tool(request.name.as_ref())
                .map(|tool| tool.arguments())
                .unwrap_or_default();
            let names: Vec<_> = arguments
                .keys()
                .filter(|name| !declared.iter().any(|argument| argument.name == *name))
                .collect();
            if !names.is_empty() {
                return Ok(super::mcp_text::error_result(
                    "ValidationError",
                    python_unknown_arguments(names).as_str(),
                )
                .into());
            }
        }
        if matches!(request.name.as_ref(), "start" | "resume")
            && !arguments.contains_key("orchestrator")
        {
            if let Some(o) = &self.orchestrator {
                arguments.insert("orchestrator".into(), json!(o));
            }
        }
        let result = match detached_broker_call(
            self.broker.clone(),
            request.name.to_string(),
            Value::Object(arguments),
        )
        .await
        {
            Ok(value) => crate::transport::mcp_text::success_result(request.name.as_ref(), &value),
            Err(error) => {
                let public = error.public();
                let message = if matches!(error, Error::BrokerUnavailable) {
                    BROKER_UNAVAILABLE.to_owned()
                } else {
                    public.message
                };
                crate::transport::mcp_text::error_result(public.kind, &message)
            }
        };
        Ok(result.into())
    }
}

/// Admit broker work in a detached task so cancellation only abandons the MCP wait.
async fn detached_broker_call(
    broker: Arc<dyn CliBroker>,
    method: String,
    params: Value,
) -> Result<Value> {
    tokio::spawn(async move { broker.call(&method, params).await })
        .await
        .map_err(|_| Error::Runtime("broker worker failed".into()))?
}

/// Render unknown object keys with the single-quoted Python validation spelling.
fn python_unknown_arguments(names: Vec<&String>) -> String {
    let names = names
        .into_iter()
        .map(|name| format!("'{}'", name.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("unknown arguments: [{names}]")
}

/// Run the stdio server until EOF while retaining no local execution capability.
pub async fn serve(home: PathBuf, orchestrator: Option<OrchestratorRef>) -> Result<()> {
    let broker = Arc::new(crate::cli::SocketBroker { home: home.clone() });
    serve_with(home, orchestrator, broker).await
}

/// Run the stdio server with an injected broker while retaining the production defaults.
pub async fn serve_with(
    home: PathBuf,
    orchestrator: Option<OrchestratorRef>,
    broker: Arc<dyn CliBroker>,
) -> Result<()> {
    let parent = std::env::var_os("AGENT_RUN_MCP_PARENT_PID")
        .map(|value| {
            value
                .to_str()
                .and_then(|value| value.parse::<i32>().ok())
                .filter(|pid| *pid > 1)
                .ok_or_else(|| crate::error::invalid("invalid MCP parent identity"))
        })
        .transpose()?;
    let serving = serve_io(
        home,
        orchestrator,
        broker,
        BoundedStdin::new(),
        BoundedStdout::new(),
    );
    match parent {
        None => serving.await,
        Some(parent) => tokio::select! {
            result = serving => result,
            _ = parent_ended(parent) => Ok(()),
        },
    }
}

/// Waits for the frontend parent relationship to end, without probing or signalling another PID.
/// The expected PID is supplied before spawn, so death before Rust starts is detected too.
async fn parent_ended(expected: i32) {
    loop {
        // SAFETY: getppid reads this process's current kernel parent relationship.
        if unsafe { libc::getppid() } != expected {
            return;
        }
        // ponytail: parent exit is observed within 250 ms; use a kernel watch only if tighter latency is needed.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// Run MCP over caller-owned streams while retaining the same broker and protocol implementation.
pub async fn serve_io<R, W>(
    home: PathBuf,
    orchestrator: Option<OrchestratorRef>,
    broker: Arc<dyn CliBroker>,
    input: R,
    output: W,
) -> Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let service = Proxy {
        broker,
        home,
        orchestrator,
    }
    .serve((input, output))
    .await
    .map_err(|_| Error::Runtime("MCP protocol initialization failed".into()))?;
    service
        .waiting()
        .await
        .map_err(|_| Error::Runtime("MCP transport ended with a protocol error".into()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BoundedReader, MAX_FRAME_BYTES};
    use crate::cli::{CliBroker, CliFuture};
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Mirrors `tests/test_mcp.py::McpSdkTests::test_pipe_reader_preserves_a_split_utf8_frame`
    #[tokio::test]
    async fn pipe_reader_preserves_a_split_utf8_frame() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let task = tokio::spawn(async move {
            writer.write_all(b"{\"text\":\"\xc3").await.unwrap();
            writer.write_all(b"\xa9\"}\n").await.unwrap();
        });
        let mut bounded = BoundedReader {
            inner: reader,
            line_bytes: 0,
        };
        let mut bytes = Vec::new();
        bounded.read_to_end(&mut bytes).await.unwrap();
        task.await.unwrap();
        assert_eq!(bytes, b"{\"text\":\"\xc3\xa9\"}\n");
    }

    /// Mirrors `tests/test_mcp.py::McpSdkTests::test_full_one_mib_frame_completes_within_bounded_deadline`
    #[tokio::test]
    async fn full_one_mib_frame_completes_within_bounded_deadline() {
        let (mut writer, reader) = tokio::io::duplex(8192);
        let task = tokio::spawn(async move {
            let payload = vec![b'x'; MAX_FRAME_BYTES];
            writer.write_all(&payload).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
        });
        let mut bounded = BoundedReader {
            inner: reader,
            line_bytes: 0,
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async move {
            let mut bytes = Vec::new();
            bounded.read_to_end(&mut bytes).await.unwrap();
            bytes
        })
        .await
        .expect("bounded frame deadline");
        task.await.unwrap();
        assert_eq!(result.len(), MAX_FRAME_BYTES + 1);
    }

    /// Mirrors `tests/test_mcp.py::McpSdkTests::test_idle_pipe_cancellation_releases_the_raw_read_worker`
    #[tokio::test]
    async fn idle_pipe_cancellation_releases_the_raw_read_worker() {
        let (_writer, reader) = tokio::io::duplex(64);
        let bounded = BoundedReader {
            inner: reader,
            line_bytes: 0,
        };
        let read = tokio::spawn(async move {
            let mut bounded = bounded;
            let mut buffer = [0_u8; 1];
            bounded.read_exact(&mut buffer).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        read.abort();
        assert!(read.await.unwrap_err().is_cancelled());
    }

    /// Mirrors `tests/test_mcp.py::McpSdkTests::test_endless_oversized_frame_remains_cancellable`
    #[tokio::test]
    async fn endless_oversized_frame_remains_cancellable() {
        let (mut writer, reader) = tokio::io::duplex(8192);
        let task = tokio::spawn(async move {
            loop {
                if writer.write_all(&[b'x'; 8192]).await.is_err() {
                    break;
                }
            }
        });
        let mut bounded = BoundedReader {
            inner: reader,
            line_bytes: 0,
        };
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), async move {
            let mut byte = [0_u8; 1];
            bounded.read_exact(&mut byte).await
        })
        .await;
        task.abort();
        assert!(outcome.is_ok());
    }

    /// Mirrors `tests/test_mcp.py::McpSdkTests::test_cancelled_caller_does_not_cancel_admitted_broker_call`
    #[tokio::test]
    async fn cancelled_caller_does_not_cancel_admitted_broker_call() {
        /// Broker fixture that waits for an explicit release after admission.
        struct DelayedBroker {
            admitted: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
            completed: Arc<AtomicBool>,
        }
        impl CliBroker for DelayedBroker {
            fn call<'a>(&'a self, _method: &'a str, _params: serde_json::Value) -> CliFuture<'a> {
                let admitted = self.admitted.clone();
                let release = self.release.clone();
                let completed = self.completed.clone();
                Box::pin(async move {
                    admitted.notify_one();
                    release.notified().await;
                    completed.store(true, Ordering::Release);
                    Ok(json!({"ok": true}))
                })
            }
        }
        let admitted = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(AtomicBool::new(false));
        let broker = Arc::new(DelayedBroker {
            admitted: admitted.clone(),
            release: release.clone(),
            completed: completed.clone(),
        });
        let caller = tokio::spawn(super::detached_broker_call(
            broker,
            "answer".into(),
            json!({}),
        ));
        admitted.notified().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        release.notify_one();
        for _ in 0..20 {
            if completed.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(completed.load(Ordering::Acquire));
    }

    /// Mirrors `tests/test_mcp.py::McpSdkTests::test_domain_error_is_an_official_tool_error_result`
    /// for the compact text presentation: a domain failure is one official
    /// tool error line with its typed kind, never a JSON dump.
    #[test]
    fn domain_error_is_an_official_tool_error_result() {
        let result =
            crate::transport::mcp_text::error_result("AgentRunError", "controlled broker failure");
        assert_eq!(result.is_error, Some(true));
        assert_eq!(result.structured_content, None);
        assert_eq!(
            serde_json::to_value(&result.content).unwrap()[0]["text"],
            "agent-run error AgentRunError: controlled broker failure\n"
        );
    }
}
