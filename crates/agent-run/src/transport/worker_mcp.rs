//! Run-bound subagent MCP. No operator tools, recipient selection or Desktop relay.
use crate::cli::CliFuture;
use crate::{
    Error, Result,
    cli::{CliBroker, SocketBroker},
    error::invalid,
};
use agent_run_domain::{
    domain::AgentId,
    worker::{ENV_NAMES, NotifyRequest, WorkerToolCall},
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
use std::path::Component;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// One authenticated attempt's transport context. Never serializable or Debug:
/// the token is a transient capability, not a model-supplied tool argument.
#[derive(Clone)]
pub struct WorkerProxy {
    /// Existing resident broker, restricted to authenticated private catalog/tool routes.
    broker: Arc<dyn CliBroker>,
    /// Exact run, not a moving stable-agent alias.
    run_id: AgentId,
    /// The supervisor's current attempt identity.
    attempt_id: String,
    /// Ephemeral run capability; only its hash is persisted by the broker.
    token: String,
    /// Durable proof success for this attempt, shared by server clones only.
    catalog_registered: Arc<AtomicBool>,
    /// Frozen supervisor-selected report surface; broker authorization remains authoritative.
    research: bool,
    /// Native completion catalog selected by frozen supervisor admission.
    explicit_finish: bool,
    /// Explicit opt-in to the v5 maintenance surface; existing supervisors omit it.
    maintenance_pause: bool,
}

/// Converts every domain-owned worker definition through the pinned SDK.
/// Invalid embedded definitions fail explicitly; no tool is silently dropped.
fn tools() -> std::result::Result<Vec<Tool>, ErrorData> {
    agent_run_domain::tools::worker_tools_json()
        .into_iter()
        .map(|tool| {
            serde_json::from_value(tool)
                .map_err(|_| ErrorData::internal_error("invalid worker registry", None))
        })
        .collect()
}

/// Decode only the supervisor-selected completion marker; all other values
/// retain the legacy catalog. The existing supervisor sets only `0` or `1`.
fn finish_mode(value: &str) -> (bool, bool) {
    let maintenance = value == agent_run_domain::worker::MAINTENANCE_PAUSE_ENV_VALUE;
    (value == "1" || maintenance, maintenance)
}

impl WorkerProxy {
    /// Returns the role-bound embedded surface. Research adds only the report
    /// writer; legacy ordinary workers retain their exact five-tool inventory.
    /// Explicit mode adds finish; only the opt-in v5 surface adds maintenance.
    fn tools(&self) -> std::result::Result<Vec<Tool>, ErrorData> {
        if self.explicit_finish {
            let catalog = if self.maintenance_pause {
                agent_run_domain::worker::maintenance_pause_tools_json(self.research)
            } else {
                agent_run_domain::worker::finish_tools_json(self.research)
            };
            return catalog
                .into_iter()
                .map(|tool| {
                    serde_json::from_value(tool).map_err(|_| {
                        ErrorData::internal_error("invalid finish worker registry", None)
                    })
                })
                .collect();
        }
        if !self.research {
            return tools();
        }
        agent_run_domain::worker::research_tools_json()
            .into_iter()
            .map(|tool| {
                serde_json::from_value(tool).map_err(|_| {
                    ErrorData::internal_error("invalid research worker registry", None)
                })
            })
            .collect()
    }

    /// Registers this actual native catalog once per exact immutable attempt.
    /// Only an explicit successful broker receipt is memoized across clones;
    /// errors, unknown replies and timeouts remain retryable on legitimate
    /// discovery/calls and never prevent healthy ordinary MCP startup.
    async fn register_catalog(&self) {
        if self.catalog_registered.load(Ordering::Acquire) {
            return;
        }
        let proof = agent_run_domain::worker::WorkerCatalogProof {
            run_id: self.run_id.clone(),
            attempt_id: self.attempt_id.clone(),
            token: self.token.clone(),
            version: if self.explicit_finish && self.maintenance_pause {
                agent_run_domain::worker::MAINTENANCE_PAUSE_CATALOG_VERSION
            } else if self.explicit_finish {
                agent_run_domain::worker::FINISH_CATALOG_VERSION
            } else {
                agent_run_domain::worker::POOL_CATALOG_VERSION
            },
            digest: if self.explicit_finish && self.maintenance_pause {
                agent_run_domain::worker::maintenance_pause_catalog_digest(self.research)
            } else if self.explicit_finish {
                agent_run_domain::worker::finish_catalog_digest(self.research)
            } else if self.research {
                agent_run_domain::worker::research_catalog_digest()
            } else {
                agent_run_domain::worker::pool_catalog_digest()
            },
        };
        if let Ok(value) = serde_json::to_value(proof)
            && let Ok(Ok(receipt)) = tokio::time::timeout(
                std::time::Duration::from_millis(250),
                self.broker
                    .call(agent_run_domain::worker::CATALOG_METHOD, value),
            )
            .await
            && receipt["registered"] == true
        {
            self.catalog_registered.store(true, Ordering::Release);
        }
    }

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
                );
            }
        };
        // The broker repeats these strict domain checks authoritatively.
        use agent_run_domain::pool::{PoolMessage, PoolPropose, PoolReadRequest, PoolVote};
        use agent_run_domain::worker::WorkerTool;
        if tool == WorkerTool::MaintenancePause && !(self.explicit_finish && self.maintenance_pause)
        {
            return super::mcp_text::error_result(
                "unknown_tool",
                "worker MCP exposes only its fixed tool catalog",
            );
        }
        let validated = match tool {
            WorkerTool::HealthReply => validated_input(
                arguments,
                name,
                agent_run_domain::health::PingReply::validate,
            ),
            WorkerTool::Finish => validated_input(
                arguments,
                name,
                agent_run_domain::worker::FinishRequest::validate,
            ),
            WorkerTool::MaintenancePause => validated_input(
                arguments,
                name,
                agent_run_domain::worker::MaintenancePauseRequest::validate,
            ),
            WorkerTool::Notify => validated_input(arguments, name, NotifyRequest::validate),
            WorkerTool::SaveReport => validated_input(
                arguments,
                name,
                agent_run_domain::worker::SaveReportRequest::validate,
            ),
            WorkerTool::PoolRead => validated_input(arguments, name, PoolReadRequest::validate),
            WorkerTool::PoolPost => validated_input(arguments, name, PoolMessage::validate),
            WorkerTool::PoolPropose => validated_input(arguments, name, PoolPropose::validate),
            WorkerTool::PoolVote => validated_input(arguments, name, PoolVote::validate),
        };
        let arguments = match validated {
            Ok(arguments) => arguments,
            Err(error) => {
                return super::mcp_text::error_result(error.public().kind, &error.public().message);
            }
        };
        let request_id = arguments["request_id"].as_str().map(str::to_owned);
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
            Ok(Ok(value)) => self.render(tool, value, request_id.as_deref()),
            Ok(Err(error)) => {
                super::mcp_text::failure_result(name, &error, request_id.as_deref(), None)
            }
            Err(_) => super::mcp_text::failure_result(
                name,
                &Error::Runtime("worker wait ended".into()),
                request_id.as_deref(),
                None,
            ),
        }
    }

    /// Renders a validated in-band pool denial or one closed typed tool view.
    /// Receipt capture precedes presentation, preserving writes and retry identities.
    fn render(
        &self,
        tool: agent_run_domain::worker::WorkerTool,
        value: Value,
        request_id: Option<&str>,
    ) -> rmcp::model::CallToolResult {
        if tool == agent_run_domain::worker::WorkerTool::HealthReply {
            return rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                value.to_string(),
            )]);
        }
        if tool == agent_run_domain::worker::WorkerTool::Finish {
            return match serde_json::from_value::<agent_run_domain::worker::FinishReceipt>(value) {
                Ok(receipt) => {
                    rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                        serde_json::to_string(&receipt).unwrap_or_default(),
                    )])
                }
                Err(_) => super::mcp_text::error_result(
                    "invalid_finish_receipt",
                    "Finish receipt is uncertain; retry the exact same payload.",
                ),
            };
        }
        if tool == agent_run_domain::worker::WorkerTool::MaintenancePause {
            return match serde_json::from_value::<agent_run_domain::worker::MaintenancePauseReceipt>(value)
                .ok()
                .filter(|receipt| receipt.validate().is_ok())
            {
                Some(receipt) => rmcp::model::CallToolResult::success(vec![
                    rmcp::model::ContentBlock::text(
                        serde_json::to_string(&receipt).unwrap_or_default(),
                    ),
                ]),
                None => super::mcp_text::error_result(
                    "invalid_maintenance_receipt",
                    "Maintenance checkpoint receipt is uncertain; retry the exact same payload.",
                ),
            };
        }
        if tool == agent_run_domain::worker::WorkerTool::SaveReport {
            return match serde_json::from_value::<agent_run_domain::worker::SaveReportReceipt>(
                value,
            ) {
                Ok(receipt) => {
                    rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                        format!(
                            "Report saved: {} ({} bytes, sha256={}, duplicate={}). Verify it with the final answer; this receipt is not task completion.",
                            serde_json::to_string(&receipt.filename).unwrap_or_default(),
                            receipt.bytes,
                            receipt.sha256,
                            receipt.duplicate
                        ),
                    )])
                }
                Err(_) => super::mcp_text::error_result(
                    "invalid_report_receipt",
                    "Report outcome is uncertain; reconcile the same filename and content.",
                ),
            };
        }
        if let Some(error) = value.get("error") {
            return match pool_denial(error) {
                Ok((denial, message)) => super::mcp_text::error_result(denial.code(), &message),
                Err(error) => {
                    super::mcp_text::failure_result(tool.as_str(), &error, request_id, None)
                }
            };
        }
        super::mcp_text::success_result_with_request(tool.as_str(), &value, request_id)
    }
}

/// Decodes one strict domain input, validates its semantic bounds, then serializes
/// only that input. The tool name selects a fixed safe shape-error diagnostic.
fn validated_input<T: serde::de::DeserializeOwned + serde::Serialize>(
    arguments: Value,
    tool: &str,
    validate: fn(&T) -> Result<()>,
) -> Result<Value> {
    let invalid = || {
        if tool == "notify_orchestrator" {
            invalid_report()
        } else {
            invalid_arguments(tool)
        }
    };
    let input: T = serde_json::from_value(arguments).map_err(|_| invalid())?;
    validate(&input)?;
    serde_json::to_value(input).map_err(|_| invalid())
}

/// Decodes only the known pool-denial codes; unknown or malformed responses
/// remain uncertain. Bounded broker messages carry canonical typed diagnostics,
/// never arbitrary error metadata or capability context.
fn pool_denial(error: &Value) -> Result<(agent_run_domain::pool::PoolDenial, String)> {
    use agent_run_domain::pool::PoolDenial;
    let denied = || Error::Runtime("invalid worker pool denial".into());
    let message = error["message"]
        .as_str()
        .filter(|s| s.len() <= 1024)
        .ok_or_else(denied)?;
    let denial = match error["code"].as_str() {
        Some("not_pool_member") => PoolDenial::NotPoolMember,
        Some("pool_completed") => PoolDenial::PoolCompleted,
        Some("stale_proposal") => PoolDenial::StaleProposal { current: None },
        Some("stale_roster") => PoolDenial::StaleRoster { current: 0 },
        Some("conflict") => PoolDenial::Conflict,
        Some("chat_budget_exhausted") => PoolDenial::ChatBudgetExhausted,
        Some("proposal_budget_exhausted") => PoolDenial::ProposalBudgetExhausted,
        Some("vote_budget_exhausted") => PoolDenial::VoteBudgetExhausted,
        Some("malformed_checks") => PoolDenial::MalformedChecks,
        Some("pool_not_found") => PoolDenial::PoolNotFound,
        Some("member_busy") => PoolDenial::MemberBusy,
        Some("member_not_current") => PoolDenial::MemberNotCurrent,
        _ => return Err(denied()),
    };
    Ok((denial, message.to_owned()))
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
    /// Malformed known methods remain protocol errors, using the shared SDK
    /// fallback classification; no custom method can reach the operator broker.
    async fn on_custom_request(
        &self,
        request: rmcp::model::CustomRequest,
        _: RequestContext<RoleServer>,
    ) -> std::result::Result<rmcp::model::CustomResult, ErrorData> {
        Err(super::mcp::unparsed_request_error(&request.method))
    }

    /// Advertise tools only; this surface has no resources, prompts or operator capabilities.
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default();
        info.capabilities = serde_json::from_value(json!({"tools":{"listChanged":false}}))
            .expect("static capabilities");
        info.server_info = Implementation::new("agent-run-worker", env!("CARGO_PKG_VERSION"));
        let catalog = if self.explicit_finish {
            "mode-bound explicit-finish catalog"
        } else if self.research {
            "fixed six-tool research catalog"
        } else {
            "fixed five-tool catalog"
        };
        info.instructions = Some(format!(
            "This private surface exposes the {catalog} for the supervisor-bound attempt. Receipts confirm durable recording, never delivery, reading, approval or completion. Reconcile uncertainty with the same request_id and content; never send credentials."
        ));
        if self.explicit_finish {
            info.instructions=Some("This private MCP binds the CURRENT execution. A supervised explicit resume admits a new execution in retained native history; earlier finish receipts do not close this one. Preserve earlier summaries and finish the current assigned task with its own new summary. End a turn while waiting for an event; this execution stays active. Stop after this execution's own accepted finish. Retry its uncertain finish with identical payload. Receipts are intent, not cleanup or owner approval. Other tools keep their bounded role authority.".into());
        }
        info
    }

    /// Return the entire bounded worker registry without pagination;
    /// 2026-07-28 requests also carry cache hints.
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, ErrorData> {
        self.register_catalog().await;
        Ok(super::mcp_cache::tools_list_result(&context, self.tools()?))
    }

    /// Resolve only an embedded worker tool, never the operator tool table.
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools()
            .ok()?
            .into_iter()
            .find(|tool| tool.name == name)
    }

    /// Execute one validated report and return compact text with no secret context.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, ErrorData> {
        self.register_catalog().await;
        if self.get_tool(&request.name).is_none() {
            return Err(ErrorData::invalid_params("unknown worker tool", None));
        }
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

/// A worker's inherited local endpoint, parsed before any capability is sent.
#[derive(Debug, PartialEq, Eq)]
enum WorkerEndpoint {
    Private(PathBuf),
    System { socket: PathBuf, broker_uid: u32 },
}

impl WorkerEndpoint {
    /// Refuses malformed or nonabsolute endpoints and invalid system UIDs.
    fn parse(value: &str) -> Result<Self> {
        if let Some(encoded) = value.strip_prefix("system:") {
            let (uid, path) = encoded
                .split_once(':')
                .ok_or_else(|| invalid("invalid system worker endpoint"))?;
            let broker_uid = uid
                .parse::<u32>()
                .ok()
                .ok_or_else(|| invalid("invalid system worker broker UID"))?;
            let socket = PathBuf::from(path);
            if !socket.is_absolute()
                || socket
                    .components()
                    .any(|part| matches!(part, Component::ParentDir))
            {
                return Err(invalid("system worker socket must be absolute"));
            }
            return Ok(Self::System { socket, broker_uid });
        }
        let home = PathBuf::from(value);
        if !home.is_absolute() {
            return Err(invalid("worker MCP home must be absolute"));
        }
        Ok(Self::Private(home))
    }
}

/// Worker client for the shared endpoint; the kernel peer UID is checked before send.
struct SystemWorkerBroker {
    socket: PathBuf,
    broker_uid: u32,
}

impl CliBroker for SystemWorkerBroker {
    /// Sends a worker request only to the expected broker identity.
    fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
        Box::pin(super::socket::client_at(
            &self.socket,
            self.broker_uid,
            method,
            params,
        ))
    }
}

/// Serve until stdin EOF using only inherited attempt context and bounded frames.
/// Never opens SQLite, loads config, starts a broker or invokes the Desktop host.
pub async fn serve_from_env() -> Result<()> {
    let endpoint = WorkerEndpoint::parse(&inherited(ENV_NAMES[0])?)?;
    let broker: Arc<dyn CliBroker> = match endpoint {
        WorkerEndpoint::Private(home) => Arc::new(SocketBroker { home }),
        WorkerEndpoint::System { socket, broker_uid } => {
            Arc::new(SystemWorkerBroker { socket, broker_uid })
        }
    };
    let run_id = inherited(ENV_NAMES[1])?
        .parse()
        .map_err(|_| invalid("invalid worker run identity"))?;
    let attempt_id = inherited(ENV_NAMES[2])?;
    let token = inherited(ENV_NAMES[3])?;
    if attempt_id.len() > 128 || token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid("invalid worker capability"));
    }
    super::mcp_text::initialize()?;
    tools().map_err(|_| Error::Runtime("worker registry initialization failed".into()))?;
    let mode = std::env::var(agent_run_domain::worker::FINISH_ENV).unwrap_or_default();
    let (explicit_finish, maintenance_pause) = finish_mode(&mode);
    let proxy = WorkerProxy {
        broker,
        run_id,
        attempt_id,
        token,
        catalog_registered: Arc::new(AtomicBool::new(false)),
        research: std::env::var(agent_run_domain::worker::RESEARCH_ENV)
            .is_ok_and(|value| value == "1"),
        explicit_finish,
        maintenance_pause,
    };
    proxy.register_catalog().await;
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

    /// Private homes keep their existing shape; Linux system endpoints require
    /// an absolute socket and numeric broker UID before capability transmission.
    #[test]
    fn worker_endpoint_rejects_invalid_locations() {
        assert_eq!(
            WorkerEndpoint::parse("/home/example").unwrap(),
            WorkerEndpoint::Private("/home/example".into())
        );
        assert!(WorkerEndpoint::parse("relative/home").is_err());
        {
            assert_eq!(
                WorkerEndpoint::parse("system:1000:/run/agent-run/api.sock").unwrap(),
                WorkerEndpoint::System {
                    socket: "/run/agent-run/api.sock".into(),
                    broker_uid: 1000
                }
            );
            for bad in [
                "system:1000:relative",
                "system:1000:/run/../api.sock",
                "system:no:/run/api.sock",
            ] {
                assert!(WorkerEndpoint::parse(bad).is_err(), "{bad}");
            }
        }
    }

    /// Completion mode adds finish and health_reply; research adds its confined report
    /// writer. No mode exposes operator launches, shell or arbitrary writes.
    #[test]
    fn research_inventory_preserves_grants_and_explicit_callbacks() {
        let mut proxy = WorkerProxy {
            broker: Arc::new(Broker::default()),
            run_id: "ag-20260928-000000-0000000001".parse().unwrap(),
            attempt_id: "attempt".into(),
            token: "a".repeat(64),
            catalog_registered: Arc::new(AtomicBool::new(false)),
            research: false,
            explicit_finish: false,
            maintenance_pause: false,
        };
        let ordinary = proxy.tools().unwrap();
        assert_eq!(ordinary.len(), 5);
        assert!(proxy.get_tool("save_report").is_none());
        proxy.research = true;
        let research = proxy.tools().unwrap();
        assert_eq!(research.len(), 6);
        assert!(
            ordinary
                .iter()
                .all(|tool| research.iter().any(|item| item.name == tool.name))
        );
        assert!(proxy.get_tool("save_report").is_some());
        for research in [false, true] {
            proxy.research = research;
            proxy.explicit_finish = true;
            assert_eq!(proxy.tools().unwrap().len(), if research { 8 } else { 7 });
            assert!(proxy.get_tool("finish").is_some());
            assert!(proxy.get_tool("health_reply").is_some());
            assert_eq!(proxy.get_tool("save_report").is_some(), research);
        }
        for name in ["start", "resume", "Bash", "exec_command", "write_file"] {
            assert!(proxy.get_tool(name).is_none(), "{name}");
        }
    }

    /// Every private pool write keeps its own strict input and durable receipt.
    /// Invalid destination injection is rejected before broker contact; unknown
    /// transport replies never advise creating a replacement mutation.
    #[tokio::test]
    async fn private_pool_writes_keep_independent_receipts() {
        let broker = Arc::new(Broker::default());
        let proxy = WorkerProxy {
            broker: broker.clone(),
            run_id: "ag-20260928-000000-0000000001".parse().unwrap(),
            attempt_id: "attempt".into(),
            token: "a".repeat(64),
            catalog_registered: Arc::new(AtomicBool::new(false)),
            research: false,
            explicit_finish: false,
            maintenance_pause: false,
        };
        for (tool, input) in [
            (
                "pool_post",
                json!({"request_id":"post","message":"bounded"}),
            ),
            (
                "pool_propose",
                json!({"request_id":"propose","message":"bounded","snapshot":"exact"}),
            ),
            (
                "pool_vote",
                json!({"request_id":"vote","proposal_seq":1,"decision":"block","checks":[],"message":"blocked"}),
            ),
        ] {
            let mut forged = input.clone();
            forged["pool_id"] = json!("foreign");
            assert_eq!(proxy.call(tool, forged).await.is_error, Some(true));
            let result = proxy.call(tool, input.clone()).await;
            let wire = serde_json::to_string(&result).unwrap();
            assert_eq!(result.is_error, Some(false), "{wire}");
            assert!(wire.contains("#7") && wire.contains(input["request_id"].as_str().unwrap()));
            assert!(!wire.contains(&proxy.token));
        }
        assert_eq!(broker.0.lock().unwrap().len(), 3);
        let refused = proxy.render(
            agent_run_domain::worker::WorkerTool::PoolPost,
            json!({"error":{"code":"pool_completed","message":"the pool is completed; no further writes are accepted"}}),
            Some("original"),
        );
        assert_eq!(refused.is_error, Some(true));
        assert!(
            !serde_json::to_string(&refused)
                .unwrap()
                .contains("ACCEPTED")
        );
        let unknown = proxy.render(
            agent_run_domain::worker::WorkerTool::PoolVote,
            json!({"error":{"code":"unknown_future","message":"SECRET_CANARY"}}),
            Some("original"),
        );
        let wire = serde_json::to_string(&unknown).unwrap();
        assert!(wire.contains("OUTCOME_UNKNOWN") && wire.contains("original"));
        assert!(!wire.contains("SECRET_CANARY"));
    }

    use super::*;
    use crate::cli::CliFuture;
    use std::sync::Mutex;

    /// In-process proof/callback double; it never opens a socket or invokes a model.
    #[derive(Default)]
    struct MaintenanceBroker(Mutex<Vec<(String, Value)>>);
    impl CliBroker for MaintenanceBroker {
        /// Record the private envelope and return only a content-free receipt.
        fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
            Box::pin(async move {
                self.0.lock().unwrap().push((method.into(), params));
                if method == agent_run_domain::worker::CATALOG_METHOD {
                    Ok(json!({"registered":true}))
                } else {
                    Ok(json!({"sha256":"c".repeat(64),"duplicate":false}))
                }
            })
        }
    }

    /// Only explicit marker 5 advertises/calls v5; capability and role remain
    /// broker-bound, arguments carry no epoch, and receipts never echo secrets.
    #[tokio::test]
    async fn maintenance_pause_is_opt_in_with_role_bound_proof() {
        for (value, expected) in [
            ("", (false, false)),
            ("0", (false, false)),
            ("1", (true, false)),
            ("5", (true, true)),
            ("other", (false, false)),
        ] {
            assert_eq!(finish_mode(value), expected);
        }
        let broker = Arc::new(MaintenanceBroker::default());
        let mut proxy = WorkerProxy {
            broker: broker.clone(),
            run_id: "ag-20260928-000000-0000000001".parse().unwrap(),
            attempt_id: "attempt".into(),
            token: "a".repeat(64),
            catalog_registered: Arc::new(AtomicBool::new(false)),
            research: false,
            explicit_finish: true,
            maintenance_pause: false,
        };
        let input = json!({"challenge_id":"b".repeat(64),"checkpoint":"fixture checkpoint"});
        assert!(proxy.get_tool("maintenance_pause").is_none());
        assert_eq!(
            proxy
                .call("maintenance_pause", input.clone())
                .await
                .is_error,
            Some(true)
        );
        assert!(broker.0.lock().unwrap().is_empty());
        proxy.maintenance_pause = true;
        assert!(proxy.get_tool("maintenance_pause").is_some());
        proxy.register_catalog().await;
        {
            let calls = broker.0.lock().unwrap();
            assert_eq!(calls[0].1["version"], 5);
            assert_eq!(
                calls[0].1["digest"],
                agent_run_domain::worker::maintenance_pause_catalog_digest(false)
            );
        }
        let mut bad = input.clone();
        bad["epoch"] = json!(1);
        assert_eq!(
            proxy.call("maintenance_pause", bad).await.is_error,
            Some(true)
        );
        let reply = proxy.call("maintenance_pause", input.clone()).await;
        assert_eq!(reply.is_error, Some(false));
        let text = serde_json::to_string(&reply).unwrap();
        assert!(!text.contains(&proxy.token) && !text.contains("fixture checkpoint"));
        {
            let calls = broker.0.lock().unwrap();
            assert_eq!(calls.len(), 2);
            assert_eq!(calls[1].0, agent_run_domain::worker::TOOL_METHOD);
            assert_eq!(calls[1].1["input"], input);
            assert!(calls[1].1["input"].get("epoch").is_none());
        }
        proxy.research = true;
        proxy.catalog_registered.store(false, Ordering::Release);
        proxy.register_catalog().await;
        assert!(proxy.get_tool("save_report").is_some());
        assert_eq!(
            broker.0.lock().unwrap().last().unwrap().1["digest"],
            agent_run_domain::worker::maintenance_pause_catalog_digest(true)
        );
        let invalid = proxy.render(
            agent_run_domain::worker::WorkerTool::MaintenancePause,
            json!({"sha256":"z".repeat(64),"duplicate":false}),
            None,
        );
        assert_eq!(invalid.is_error, Some(true));
        assert!(
            serde_json::to_string(&invalid)
                .unwrap()
                .contains("retry the exact same payload")
        );
    }

    /// Records broker calls without executing a run or touching production state.
    #[derive(Default)]
    struct Broker(Mutex<Vec<(String, Value)>>);
    impl CliBroker for Broker {
        /// Return a compact fake queue receipt while retaining the private request for assertions.
        fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
            Box::pin(async move {
                let pool = params["tool"] != "notify_orchestrator";
                self.0.lock().unwrap().push((method.into(), params));
                Ok(if pool {
                    json!({"pool_id":"p","seq":7,"duplicate":false})
                } else {
                    json!({"notification_id":"ntf_test","state":"pending","duplicate":false})
                })
            })
        }
    }

    /// Finite catalog-only broker double: failure, timeout, unknown reply, then
    /// explicit success. Counts RPCs without persisting transient capability data.
    #[derive(Default)]
    struct CatalogBroker(std::sync::atomic::AtomicUsize);

    impl CliBroker for CatalogBroker {
        /// Checks the immutable private route/context and models startup races;
        /// no provider or socket is contacted and every delayed reply is finite.
        fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
            Box::pin(async move {
                assert_eq!(method, agent_run_domain::worker::CATALOG_METHOD);
                assert_eq!(params["attempt_id"], "attempt");
                match self.0.fetch_add(1, Ordering::Relaxed) {
                    0 => Err(Error::Runtime("fixture broker unavailable".into())),
                    1 => {
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                        Ok(json!({"registered":true}))
                    }
                    2 => Ok(json!({"unconfirmed":true})),
                    _ => Ok(json!({"registered":true})),
                }
            })
        }
    }

    /// Failed/late/unknown startup proof retries; verified success suppresses
    /// subsequent hello RPCs for both the current proxy and its server clones.
    #[tokio::test]
    async fn catalog_registration_retries_unknown_and_memoizes_only_success() {
        let broker = Arc::new(CatalogBroker::default());
        let proxy = WorkerProxy {
            broker: broker.clone(),
            run_id: "ag-20260928-000000-0000000001".parse().unwrap(),
            attempt_id: "attempt".into(),
            token: "a".repeat(64),
            catalog_registered: Arc::new(AtomicBool::new(false)),
            research: false,
            explicit_finish: false,
            maintenance_pause: false,
        };
        for _ in 0..3 {
            proxy.register_catalog().await;
            assert!(!proxy.catalog_registered.load(Ordering::Acquire));
        }
        proxy.register_catalog().await;
        assert!(proxy.catalog_registered.load(Ordering::Acquire));
        proxy.register_catalog().await;
        proxy.clone().register_catalog().await;
        assert_eq!(broker.0.load(Ordering::Relaxed), 4);
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
            catalog_registered: Arc::new(AtomicBool::new(false)),
            research: false,
            explicit_finish: false,
            maintenance_pause: false,
        };
        let names: Vec<_> = tools()
            .unwrap()
            .iter()
            .map(|tool| tool.name.to_string())
            .collect();
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
            catalog_registered: Arc::new(AtomicBool::new(false)),
            research: false,
            explicit_finish: false,
            maintenance_pause: false,
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
