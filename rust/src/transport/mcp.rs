//! Rust MCP stdio compatibility proxy; durable execution stays in the broker.
use crate::{dispatch, domain::OrchestratorRef, Error, Result};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResult, Content, ErrorData, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerInfo, Tool,
    },
    service::{RequestContext, RoleServer},
    ServerHandler, ServiceExt,
};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Match the Python broker and MCP stdin ceiling before the SDK allocates a frame.
const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Preserve Python's stable, operator-actionable broker-unavailable wording.
const BROKER_UNAVAILABLE: &str =
    "agent-run broker is not running; start it with `agent-run api serve` or its launchd job";

/// Bound stdin one LF-delimited MCP frame at a time for the SDK transport.
struct BoundedStdin {
    /// The process stdin whose bytes are fed to the MCP SDK.
    inner: tokio::io::Stdin,
    /// Bytes received since the most recent LF delimiter.
    line_bytes: usize,
}

impl BoundedStdin {
    /// Wrap process stdin while retaining only the current frame's byte count.
    fn new() -> Self {
        Self {
            inner: tokio::io::stdin(),
            line_bytes: 0,
        }
    }
}

impl AsyncRead for BoundedStdin {
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
    pub home: PathBuf,
    pub orchestrator: Option<OrchestratorRef>,
}
impl ServerHandler for Proxy {
    /// Report only the Python-compatible MCP capabilities and implementation identity.
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
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
    /// Inject the host binding when needed, then forward the call through the resident broker.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        if !dispatch::TOOL_NAMES.contains(&request.name.as_ref()) {
            return Ok(tool_error(
                "unknown_tool",
                format!("unknown tool: {}", request.name),
            ));
        }
        let mut arguments = request.arguments.unwrap_or_default();
        if matches!(
            request.name.as_ref(),
            "capacity_order" | "models" | "limits"
        ) && !arguments.is_empty()
        {
            let names: Vec<_> = arguments.keys().collect();
            return Ok(tool_error(
                "ValidationError",
                python_unknown_arguments(names),
            ));
        }
        if matches!(request.name.as_ref(), "start" | "resume")
            && !arguments.contains_key("orchestrator")
        {
            if let Some(o) = &self.orchestrator {
                arguments.insert("orchestrator".into(), json!(o));
            }
        }
        match super::socket::client(&self.home, request.name.as_ref(), Value::Object(arguments))
            .await
        {
            Ok(value) => Ok(tool_result(value)),
            Err(error) => {
                let public = error.public();
                let message = if matches!(error, Error::BrokerUnavailable) {
                    BROKER_UNAVAILABLE.to_owned()
                } else {
                    public.message
                };
                Ok(tool_error(public.kind, message))
            }
        }
    }
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

/// Render one successful broker value with Python's fixed text-content marker.
fn tool_result(value: Value) -> CallToolResult {
    let mut result = CallToolResult::structured(value);
    result.content = vec![Content::text("result in structuredContent")];
    result
}

/// Render one domain failure as a tool result instead of a JSON-RPC protocol error.
fn tool_error(code: impl Into<String>, message: impl Into<String>) -> CallToolResult {
    let data = json!({"error": {"code": code.into(), "message": message.into()}});
    CallToolResult::structured_error(data)
}

/// Run the stdio server until EOF while retaining no local execution capability.
pub async fn serve(home: PathBuf, orchestrator: Option<OrchestratorRef>) -> Result<()> {
    let _relay = crate::delivery::relay::host(&home)?;
    let service = Proxy { home, orchestrator }
        .serve((BoundedStdin::new(), BoundedStdout::new()))
        .await
        .map_err(|_| Error::Runtime("MCP protocol initialization failed".into()))?;
    service
        .waiting()
        .await
        .map_err(|_| Error::Runtime("MCP transport ended with a protocol error".into()))?;
    Ok(())
}
