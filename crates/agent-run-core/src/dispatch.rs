//! One tool inventory and strict argument decoder for every public transport.
use crate::{
    agent_identity,
    domain::{AgentId, OrchestratorRef},
    error::invalid,
    service::{Query, Service},
    Result,
};
pub use agent_run_domain::tools::{is_tool, tool, tools_json};
use serde::Deserialize;
use serde_json::{json, Value};
/// Returns the one domain-owned, Python-compatible public discovery table.
pub fn tools() -> Vec<Value> {
    tools_json()
}
fn args<T: serde::de::DeserializeOwned>(raw: Value) -> Result<T> {
    if !raw.is_object() {
        return Err(invalid("tool arguments must be an object"));
    }
    serde_json::from_value(raw)
        .map_err(|_| invalid("unknown, missing, or incorrectly typed tool argument"))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// Stable agent selection with an optional exact historical execution.
struct Id {
    /// Root identity or historical alias for the lineage.
    agent_id: AgentId,
    /// Exact execution within that lineage; omission selects the latest.
    #[serde(default)]
    run_id: Option<AgentId>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// A bounded steering message addressed to a stable agent or pinned run.
struct Steer {
    /// Root identity or historical alias for the lineage.
    agent_id: AgentId,
    /// Exact execution, otherwise the latest run is selected once.
    #[serde(default)]
    run_id: Option<AgentId>,
    /// Nonblank UTF-8 control text, validated by the service before enqueue.
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// One transcript page; run_id pins subsequent pages to the same execution.
struct Transcript {
    /// Stable agent or historical alias.
    agent_id: AgentId,
    /// Optional historical execution, otherwise the latest run.
    #[serde(default)]
    run_id: Option<AgentId>,
    #[serde(default)]
    /// Exclusive journal cursor, zero for the first page.
    cursor: i64,
    #[serde(default = "transcript_limit")]
    /// Bounded maximum number of returned messages.
    limit: usize,
}
/// Default bounded MCP transcript page size.
fn transcript_limit() -> usize {
    200
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// Continuation of a logical agent, retaining its native context and grants.
struct Resume {
    /// Stable agent identity or historical alias.
    agent_id: AgentId,
    /// Optional exact parent, otherwise the terminal lineage tip.
    #[serde(default)]
    run_id: Option<AgentId>,
    /// Nonblank next instruction for the retained native conversation.
    task: String,
    #[serde(default)]
    /// Whole-run deadline override in seconds; omission inherits the parent.
    timeout_seconds: Option<f64>,
    #[serde(default)]
    /// Idempotency key in the caller's namespace, reused for transport retries.
    request_id: Option<String>,
    #[serde(default)]
    /// Optional delivery destination override; omission inherits the parent.
    orchestrator: Option<OrchestratorRef>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Doc {
    #[serde(default)]
    topic: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// Private socket wait arguments; omission means wait without a client deadline.
struct Wait {
    /// Stable agent identity; resolution is pinned for the entire observation.
    agent_id: AgentId,
    /// Exact execution returned by the admission being observed.
    #[serde(default)]
    run_id: Option<AgentId>,
    #[serde(default)]
    /// Optional observer deadline in seconds, distinct from the run deadline.
    timeout_seconds: Option<f64>,
}
/// Decodes and dispatches one named tool against the caller-owned service.
///
/// `raw` must be an object matching the named schema. Errors are typed so the
/// socket, MCP, and CLI transports can independently render their protocols.
pub async fn call(service: &Service, name: &str, raw: Value) -> Result<Value> {
    match name {
        // Public initial start is provider + explicit model only; a legacy
        // `runtime` payload is an unknown field (ValidationError).
        "start" => {
            let result = service
                .start_provider(args::<agent_run_domain::ProviderStartRequest>(raw)?)
                .await?;
            agent_identity::admission(result)
        }
        "resume" => {
            let a: Resume = args(raw)?;
            service
                .resume_public(
                    &a.agent_id,
                    a.run_id.as_ref(),
                    a.task,
                    a.timeout_seconds,
                    a.request_id,
                    a.orchestrator,
                )
                .await
        }
        "cancel" => {
            let a: Id = args(raw)?;
            let row = service.resolve_run(&a.agent_id, a.run_id.as_ref())?;
            agent_identity::result(&row, service.cancel(&row.id)?)
        }
        "steer" => {
            let a: Steer = args(raw)?;
            let row = service.resolve_run(&a.agent_id, a.run_id.as_ref())?;
            agent_identity::result(&row, service.steer(&row.id, &a.text)?)
        }
        "answer" => {
            let a: Id = args(raw)?;
            let row = service.resolve_run(&a.agent_id, a.run_id.as_ref())?;
            agent_identity::result(&row, service.answer(&row.id)?)
        }
        "transcript" => {
            let a: Transcript = args(raw)?;
            let row = service.resolve_run(&a.agent_id, a.run_id.as_ref())?;
            agent_identity::result(&row, service.transcript(&row.id, a.cursor, a.limit)?)
        }
        "list_agents" => service.list_public(args::<Query>(raw)?).await,
        "doc" => {
            let a: Doc = args(raw)?;
            let topic = a.topic.as_deref().unwrap_or("index");
            Ok(json!({"topic":topic,"text":doc(topic)?}))
        }
        "models" => service.models(args(raw)?).await,
        "limits" => {
            let _: Empty = args(raw)?;
            service.limits()
        }
        "capacity_order" => service.capacity_order(args(raw)?),
        "delegation_guide" => {
            let _: Empty = args(raw)?;
            service.delegation_guide()
        }
        // Socket-only control/discovery methods; not part of the twelve MCP tools.
        "tools" => {
            let _: Empty = args(raw)?;
            // The private socket discovery method predates MCP and returns the
            // table itself, not an MCP-shaped wrapper.
            Ok(Value::Array(tools()))
        }
        "ping" => {
            let _: Empty = args(raw)?;
            Ok(json!({"ok":true}))
        }
        "wait" => {
            let a: Wait = args(raw)?;
            if a.timeout_seconds
                .is_some_and(|seconds| !seconds.is_finite() || seconds <= 0.0)
            {
                return Err(invalid("timeout_seconds must be a positive finite number"));
            }
            let row = service.resolve_run(&a.agent_id, a.run_id.as_ref())?;
            let mut result =
                agent_identity::result(&row, service.wait(&row.id, a.timeout_seconds).await?)?;
            if a.timeout_seconds.is_some() && result["terminal"] == false {
                result["timed_out"] = Value::Bool(true);
            }
            Ok(result)
        }
        _ => Err(invalid("unknown tool")),
    }
}

/// Returns one packaged operator-guide topic using the transport-neutral registry.
pub fn doc(topic: &str) -> Result<&'static str> {
    crate::doc::topic_text(topic)
}
