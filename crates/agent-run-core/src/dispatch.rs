//! One tool inventory and strict argument decoder for every public transport.
use crate::{
    domain::{AgentId, OrchestratorRef, StartRequest},
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
struct Id {
    agent_id: AgentId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Steer {
    agent_id: AgentId,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Transcript {
    agent_id: AgentId,
    #[serde(default)]
    cursor: i64,
    #[serde(default = "transcript_limit")]
    limit: usize,
}
fn transcript_limit() -> usize {
    200
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resume {
    agent_id: AgentId,
    task: String,
    #[serde(default)]
    timeout_seconds: Option<f64>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
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
    agent_id: AgentId,
    #[serde(default)]
    timeout_seconds: Option<f64>,
}
/// Decodes and dispatches one named tool against the caller-owned service.
///
/// `raw` must be an object matching the named schema. Errors are typed so the
/// socket, MCP, and CLI transports can independently render their protocols.
pub async fn call(service: &Service, name: &str, raw: Value) -> Result<Value> {
    match name {
        "start" => service.start(args::<StartRequest>(raw)?).await,
        "resume" => {
            let a: Resume = args(raw)?;
            service
                .resume(
                    &a.agent_id,
                    a.task,
                    a.timeout_seconds,
                    a.request_id,
                    a.orchestrator,
                )
                .await
        }
        "cancel" => {
            let a: Id = args(raw)?;
            service.cancel(&a.agent_id)
        }
        "steer" => {
            let a: Steer = args(raw)?;
            service.steer(&a.agent_id, &a.text)
        }
        "answer" => {
            let a: Id = args(raw)?;
            service.answer(&a.agent_id)
        }
        "transcript" => {
            let a: Transcript = args(raw)?;
            service.transcript(&a.agent_id, a.cursor, a.limit)
        }
        "list_agents" => service.list(args::<Query>(raw)?).await,
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
        // Socket-only control/discovery methods; not part of the eleven MCP tools.
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
            let mut result = service.wait(&a.agent_id, a.timeout_seconds).await?;
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
