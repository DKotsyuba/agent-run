//! Run-bound subagent MCP. No operator tools, recipient selection or Desktop relay.
use crate::{
    cli::{CliBroker, SocketBroker},
    error::invalid,
    Error, Result,
};
use agent_run_domain::{
    domain::AgentId,
    worker::{NotifyRequest, WorkerToolCall, ENV_NAMES},
};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResponse, ErrorData, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerConfig, Tool,
    },
    service::{RequestContext, RoleServer},
    ServerHandler, ServiceExt,
};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};

/// One authenticated attempt's transport context. Never serializable or Debug:
/// the token is a transient capability, not a model-supplied tool argument.
#[derive(Clone)]
pub struct WorkerProxy {
    /// Existing resident broker; only METHOD can be invoked by this server.
    broker: Arc<dyn CliBroker>,
    /// Exact run, not a moving stable-agent alias.
    run_id: AgentId,
    /// The supervisor's current attempt identity.
    attempt_id: String,
    /// Ephemeral run capability; only its hash is persisted by the broker.
    token: String,
}

/// Decode the embedded worker-only discovery table. An invalid asset is a build defect.
fn tools() -> Vec<Tool> {
    serde_json::from_str(include_str!("../../../../assets/worker_tools.json"))
        .expect("valid embedded worker tool registry")
}

impl WorkerProxy {
    /// Validate model arguments locally, attach immutable hidden context and
    /// route one fixed catalog tool through the private broker method. Unknown
    /// tools/arguments cannot reach an operator dispatch path, and denials
    /// arrive typed instead of guessed from prose.
    async fn call(&self, name: &str, arguments: Value) -> rmcp::model::CallToolResult {
        let tool = match agent_run_domain::worker::WorkerTool::parse(name) {
            Some(tool) => tool,
            None => {
                return super::mcp_text::error_result(
                    "unknown_tool",
                    "worker MCP exposes only its fixed tool catalog",
                )
            }
        };
        // Local argument shape checks keep obviously malformed calls off the
        // broker; the broker revalidates everything authoritatively.
        let validated: std::result::Result<Value, crate::Error> = match tool {
            agent_run_domain::worker::WorkerTool::Notify => {
                match serde_json::from_value::<NotifyRequest>(arguments) {
                    Ok(input) => match input.validate() {
                        Ok(()) => serde_json::to_value(input).map_err(|_| invalid_report()),
                        Err(error) => Err(error),
                    },
                    Err(_) => Err(invalid_report()),
                }
            }
            agent_run_domain::worker::WorkerTool::PoolRead => {
                match serde_json::from_value::<agent_run_domain::pool::PoolReadRequest>(arguments) {
                    Ok(input) => match input.validate() {
                        Ok(()) => {
                            serde_json::to_value(input).map_err(|_| invalid_arguments("pool_read"))
                        }
                        Err(error) => Err(error),
                    },
                    Err(_) => Err(invalid_arguments("pool_read")),
                }
            }
            _ => Ok(arguments),
        };
        let arguments = match validated {
            Ok(arguments) => arguments,
            Err(error) => {
                return super::mcp_text::error_result(error.public().kind, &error.public().message)
            }
        };
        let call = WorkerToolCall {
            run_id: self.run_id.clone(),
            attempt_id: self.attempt_id.clone(),
            token: self.token.clone(),
            tool: tool.as_str().to_owned(),
            input: arguments,
        };
        let broker = self.broker.clone();
        // A disconnected worker wait does not cancel an in-flight durable
        // write. Retrying with the same request_id resolves the uncertain
        // result idempotently.
        let result = tokio::spawn(async move {
            broker
                .call(
                    agent_run_domain::worker::TOOL_METHOD,
                    serde_json::to_value(call)?,
                )
                .await
        })
        .await;
        match result {
            Ok(Ok(value)) => self.render(tool, value),
            Ok(Err(error)) => {
                super::mcp_text::error_result(error.public().kind, &error.public().message)
            }
            Err(_) => super::mcp_text::error_result("RuntimeError", "worker call failed"),
        }
    }

    /// Renders one broker answer: typed in-band denials as compact errors,
    /// pool reads with each entry pre-rendered through the one shared
    /// formatter, and everything else through the compact tool templates.
    fn render(
        &self,
        tool: agent_run_domain::worker::WorkerTool,
        value: Value,
    ) -> rmcp::model::CallToolResult {
        if let Some(error) = value.get("error").filter(|e| e.is_object()) {
            let code = error["code"].as_str().unwrap_or("RuntimeError");
            let message = error["message"].as_str().unwrap_or("pool call refused");
            return super::mcp_text::error_result(code, message);
        }
        if tool == agent_run_domain::worker::WorkerTool::PoolRead {
            return match super::mcp_text::pool_page(value) {
                Ok(page) => super::mcp_text::success_result("pool_read", &page),
                Err(message) => super::mcp_text::error_result("RuntimeError", &message),
            };
        }
        super::mcp_text::success_result(tool.as_str(), &value)
    }
}

/// The historical report-argument rejection text.
fn invalid_report() -> crate::Error {
    crate::Error::Validation("unknown, missing, or incorrectly typed report argument".into())
}

/// One bounded per-tool argument rejection.
fn invalid_arguments(tool: &str) -> crate::Error {
    crate::Error::Validation(format!(
        "unknown, missing, or incorrectly typed {tool} argument"
    ))
}

impl ServerHandler for WorkerProxy {
    /// Advertise tools only; this surface has no resources, prompts or operator capabilities.
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default();
        info.capabilities = serde_json::from_value(json!({"tools":{"listChanged":false}}))
            .expect("static capabilities");
        info.server_info = Implementation::new("agent-run-worker", "1");
        info
    }

    /// Return the entire bounded worker registry without pagination;
    /// 2026-07-28 requests also carry cache hints.
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, ErrorData> {
        Ok(super::mcp_cache::tools_list_result(&context, tools()))
    }

    /// Resolve only an embedded worker tool, never the operator tool table.
    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools().into_iter().find(|tool| tool.name == name)
    }

    /// Execute one validated report and return compact text with no secret context.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, ErrorData> {
        Ok(self
            .call(
                &request.name,
                Value::Object(request.arguments.unwrap_or_default()),
            )
            .await
            .into())
    }
}

/// Read a supervisor-provided nonempty UTF-8 context variable. Errors deliberately
/// omit its value so missing or corrupt capability material cannot reach stderr.
fn inherited(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("worker MCP requires supervisor-provided context"))
}

/// Serve until stdin EOF using only inherited attempt context and bounded frames.
/// Never opens SQLite, loads config, starts a broker or invokes the Desktop host.
pub async fn serve_from_env() -> Result<()> {
    let home = PathBuf::from(inherited(ENV_NAMES[0])?);
    if !home.is_absolute() {
        return Err(invalid("worker MCP home must be absolute"));
    }
    let run_id = inherited(ENV_NAMES[1])?
        .parse()
        .map_err(|_| invalid("invalid worker run identity"))?;
    let attempt_id = inherited(ENV_NAMES[2])?;
    let token = inherited(ENV_NAMES[3])?;
    if attempt_id.len() > 128 || token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid("invalid worker capability"));
    }
    let proxy = WorkerProxy {
        broker: Arc::new(SocketBroker { home }),
        run_id,
        attempt_id,
        token,
    };
    let service = proxy
        .serve((
            super::mcp::BoundedReader::new(),
            super::mcp::BoundedStdout::new(),
        ))
        .await
        .map_err(|_| Error::Runtime("worker MCP initialization failed".into()))?;
    service
        .waiting()
        .await
        .map_err(|_| Error::Runtime("worker MCP transport failed".into()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::CliFuture;
    use std::sync::Mutex;

    /// Records broker calls without executing a run or touching production state.
    #[derive(Default)]
    struct Broker(Mutex<Vec<(String, Value)>>);
    impl CliBroker for Broker {
        /// Return a compact fake queue receipt while retaining the private request for assertions.
        fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
            Box::pin(async move {
                self.0.lock().unwrap().push((method.into(), params));
                Ok(json!({"notification_id":"ntf_test","state":"pending","duplicate":false}))
            })
        }
    }

    /// Operator tools and destination injection never reach the broker; valid
    /// reports carry fixed run context and return plain text without the capability.
    #[tokio::test]
    async fn worker_surface_is_separate_and_bound() {
        let broker = Arc::new(Broker::default());
        let proxy = WorkerProxy {
            broker: broker.clone(),
            run_id: "ag-20260928-000000-0000000001".parse().unwrap(),
            attempt_id: "attempt".into(),
            token: "a".repeat(64),
        };
        let names: Vec<_> = tools().iter().map(|tool| tool.name.to_string()).collect();
        assert_eq!(
            names,
            [
                "notify_orchestrator",
                "pool_post",
                "pool_read",
                "pool_propose",
                "pool_vote"
            ]
        );
        for name in [
            "start",
            "resume",
            "cancel",
            "steer",
            "tools",
            agent_run_domain::worker::METHOD,
        ] {
            assert!(proxy.get_tool(name).is_none());
            assert_eq!(proxy.call(name, json!({})).await.is_error, Some(true));
        }
        for extra in ["orchestrator", "thread_id", "run_id", "token"] {
            let mut input = json!({"request_id":"report-1","message":"Found an issue"});
            input[extra] = json!("forged");
            assert_eq!(
                proxy.call("notify_orchestrator", input).await.is_error,
                Some(true)
            );
        }
        assert!(broker.0.lock().unwrap().is_empty());
        let result = proxy
            .call(
                "notify_orchestrator",
                json!({"request_id":"report-1","message":"Found an issue"}),
            )
            .await;
        assert_ne!(result.is_error, Some(true));
        let rendered = serde_json::to_string(&result).unwrap();
        assert!(rendered.contains("ntf_test"));
        assert!(!rendered.contains(&proxy.token));
        let calls = broker.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, agent_run_domain::worker::TOOL_METHOD);
        assert_eq!(calls[0].1["run_id"], proxy.run_id.as_str());
        assert_eq!(calls[0].1["attempt_id"], "attempt");
        assert_eq!(calls[0].1["token"], proxy.token);
        assert!(!crate::dispatch::is_tool("notify_orchestrator"));
    }

    /// A raw 2026-07-28 `tools/list` (no `initialize`) carries private cache hints,
    /// so Claude children on that protocol keep `notify_orchestrator`; legacy sessions do not.
    #[tokio::test]
    async fn worker_modern_tools_list_carries_cache_hints() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let proxy = WorkerProxy {
            broker: Arc::new(Broker::default()),
            run_id: "ag-20260928-000000-0000000001".parse().unwrap(),
            attempt_id: "attempt".into(),
            token: "a".repeat(64),
        };
        let (mut input_writer, input_reader) = tokio::io::duplex(64 * 1024);
        let (output_writer, output_reader) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            proxy
                .serve((input_reader, output_writer))
                .await
                .unwrap()
                .waiting()
                .await
        });
        let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{
            "io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientCapabilities":{}
        }}});
        input_writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        BufReader::new(output_reader)
            .read_line(&mut line)
            .await
            .unwrap();
        let reply: Value = serde_json::from_str(&line).unwrap();
        let result = &reply["result"];
        assert_eq!(result["resultType"], "complete", "{reply}");
        assert_eq!(result["ttlMs"], 60_000, "{reply}");
        assert_eq!(result["cacheScope"], "private", "{reply}");
        assert_eq!(result["tools"][0]["name"], "notify_orchestrator");
        drop(input_writer);
        let _ = server.await;
    }
}
