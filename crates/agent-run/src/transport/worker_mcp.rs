//! Run-bound subagent MCP. No operator tools, recipient selection or Desktop relay.
use crate::{
    Error, Result,
    cli::{CliBroker, SocketBroker},
    error::invalid,
};
use agent_run_domain::{
    domain::AgentId,
    worker::{ENV_NAMES, METHOD, NotifyRequest, WorkerCall},
};
use rmcp::{
    ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, ErrorData, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerConfig, Tool,
    },
    service::{RequestContext, RoleServer},
};
use serde_json::{Value, json};
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
    /// Validate model arguments locally, attach immutable context and queue through
    /// the broker. Unknown tools/arguments cannot reach an operator dispatch path.
    async fn call(&self, name: &str, arguments: Value) -> rmcp::model::CallToolResult {
        if name != "notify_orchestrator" {
            return super::mcp_text::error_result(
                "unknown_tool",
                "worker MCP exposes only notify_orchestrator",
            );
        }
        let input = match serde_json::from_value::<NotifyRequest>(arguments) {
            Ok(input) => input,
            Err(_) => {
                return super::mcp_text::error_result(
                    "ValidationError",
                    "unknown, missing, or incorrectly typed report argument",
                );
            }
        };
        if let Err(error) = input.validate() {
            return super::mcp_text::error_result(error.public().kind, &error.public().message);
        }
        let call = WorkerCall {
            run_id: self.run_id.clone(),
            attempt_id: self.attempt_id.clone(),
            token: self.token.clone(),
            input,
        };
        let broker = self.broker.clone();
        // A disconnected worker wait does not cancel an in-flight durable enqueue.
        // Retrying with the same request_id resolves the uncertain result.
        let result =
            tokio::spawn(async move { broker.call(METHOD, serde_json::to_value(call)?).await })
                .await;
        match result {
            Ok(Ok(value)) => super::mcp_text::success_result("notify_orchestrator", &value),
            Ok(Err(error)) => {
                super::mcp_text::error_result(error.public().kind, &error.public().message)
            }
            Err(_) => super::mcp_text::error_result("RuntimeError", "worker notification failed"),
        }
    }
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

    /// Return the entire bounded worker registry without pagination.
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(tools()))
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
        assert_eq!(tools().len(), 1);
        assert_eq!(tools()[0].name, "notify_orchestrator");
        for name in ["start", "resume", "cancel", "steer", "tools", METHOD] {
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
        assert_eq!(calls[0].0, METHOD);
        assert_eq!(calls[0].1["run_id"], proxy.run_id.as_str());
        assert_eq!(calls[0].1["attempt_id"], "attempt");
        assert_eq!(calls[0].1["token"], proxy.token);
        assert!(!crate::dispatch::is_tool("notify_orchestrator"));
    }
}
