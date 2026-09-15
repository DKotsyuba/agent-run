//! One tool inventory and strict argument decoder for every public transport.
use crate::{
    domain::{AgentId, OrchestratorRef, StartRequest},
    error::invalid,
    service::{Query, Service},
    Result,
};
use serde::Deserialize;
use serde_json::{json, Value};
pub const TOOL_NAMES: [&str; 11] = [
    "capacity_order",
    "start",
    "cancel",
    "steer",
    "list_agents",
    "transcript",
    "answer",
    "resume",
    "doc",
    "models",
    "limits",
];
pub fn tools() -> Vec<Value> {
    serde_json::from_str(include_str!("../../../assets/tools.json"))
        .expect("packaged tool table is valid JSON")
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
struct Wait {
    agent_id: AgentId,
    #[serde(default, alias = "timeout_seconds")]
    wait_seconds: Option<f64>,
}
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
        "models" => {
            let _: Empty = args(raw)?;
            service.models().await
        }
        "limits" => {
            let _: Empty = args(raw)?;
            service.limits()
        }
        "capacity_order" => {
            let _: Empty = args(raw)?;
            service.capacity_order()
        }
        // Socket-only control/discovery methods; not part of the eleven MCP tools.
        "tools" => {
            let _: Empty = args(raw)?;
            Ok(json!({"tools":tools()}))
        }
        "ping" => {
            let _: Empty = args(raw)?;
            Ok(json!({"ok":true,"version":env!("CARGO_PKG_VERSION")}))
        }
        "wait" => {
            let a: Wait = args(raw)?;
            service.wait(&a.agent_id, a.wait_seconds).await
        }
        _ => Err(invalid("unknown tool")),
    }
}

/// Returns one packaged operator-guide topic using the transport-neutral registry.
pub fn doc(topic: &str) -> Result<&'static str> {
    match topic {
        "index" => Ok(include_str!("../../../migration/operator-guide-draft.md")),
        "completion" => Ok("agent-run/completion is a lifecycle notification, never a new task or user approval. Read answer and transcript using the durable agent ID. A succeeded runtime is not proof that the requested software change is correct; acceptance tests remain a separate decision. Unbound starts have no completion delivery. A disconnected client never implicitly cancels an admitted run."),
        "migration" => Ok(include_str!("../../../migration/status.md")),
        "config" => Ok(include_str!("../../../assets/config.example.toml")),
        "architecture" => Ok(include_str!("../../../migration/architecture.md")),
        _ => Err(invalid("unknown guide topic; use index, completion, migration, config, architecture")),
    }
}
