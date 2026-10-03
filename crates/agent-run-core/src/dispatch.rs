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
use std::time::Duration;
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
    /// Shared representation, bounds and exclusive cursor options.
    #[serde(flatten)]
    page: agent_run_domain::transcript::TranscriptQuery,
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
    /// Optional replacement display label; omission inherits the parent's.
    display_name: Option<String>,
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
    /// Admission receipt counter, pinning a delayed wait without exposing run IDs.
    #[serde(default)]
    sequence: Option<u32>,
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
        agent_run_domain::worker::METHOD => {
            let call: agent_run_domain::worker::WorkerCall = args(raw)?;
            let home = service.home.clone();
            tokio::task::spawn_blocking(move || {
                let receipt = crate::state::Store::open(&home)?.notify_orchestrator(
                    &call.run_id,
                    &call.attempt_id,
                    &call.token,
                    &call.input,
                    crate::domain::now(),
                )?;
                Ok(serde_json::to_value(receipt)?)
            })
            .await
            .map_err(|_| crate::Error::Runtime("worker notification failed".into()))?
        }
        agent_run_domain::worker::TOOL_METHOD => {
            let call: agent_run_domain::worker::WorkerToolCall = args(raw)?;
            let tool = agent_run_domain::worker::WorkerTool::parse(&call.tool)
                .ok_or_else(|| invalid("unknown worker tool"))?;
            worker_tool_call(service, call, tool).await
        }
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
                    a.display_name,
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
            service.transcript_with_options(&a.agent_id, a.run_id.as_ref(), &a.page)
        }
        "start_pool" => {
            service
                .start_pool(args::<agent_run_domain::pool::PoolStartRequest>(raw)?)
                .await
        }
        "pool_post" => {
            let request: agent_run_domain::pool::PoolPost = args(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || service.pool_post(request))
                .await
                .map_err(|_| crate::Error::Runtime("pool post failed".into()))??
                .map_err(agent_run_domain::pool::PoolDenial::into_error)
        }
        "pool_replace" => service
            .replace_pool_member(args::<agent_run_domain::pool::PoolReplace>(raw)?)
            .await?
            .map_err(agent_run_domain::pool::PoolDenial::into_error),
        "pool" => {
            let query: agent_run_domain::pool::PoolQuery = args(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || service.pool_status(query))
                .await
                .map_err(|_| crate::Error::Runtime("pool read failed".into()))??
                .map_err(agent_run_domain::pool::PoolDenial::into_error)
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
            if a.sequence.is_some() && a.run_id.is_some() {
                return Err(invalid("conflicting execution receipt selectors"));
            }
            if a.timeout_seconds
                .is_some_and(|seconds| !seconds.is_finite() || seconds <= 0.0)
            {
                return Err(invalid("timeout_seconds must be a positive finite number"));
            }
            let row = match a.sequence {
                Some(sequence) => agent_identity::resolve_sequence(
                    &crate::state::Store::open(&service.home)?,
                    &a.agent_id,
                    sequence,
                )?,
                None => service.resolve_run(&a.agent_id, a.run_id.as_ref())?,
            };
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

/// Maximum seconds one private `pool_read` may hold for a new entry, kept
/// below the broker socket's per-request deadline so the wait never outlives
/// its transport.
const POOL_READ_WAIT_SECONDS: f64 = 25.0;

/// Routes one authenticated private worker tool through the shared broker.
///
/// Every tool authenticates the hidden capability server-side; pool denials
/// are returned in-band as one typed `{error: {code, message}}` object so the
/// worker transport can render a typed refusal instead of guessing from
/// prose. The read wait is bounded, holds no database lock across awaits,
/// and ends as soon as the attempt stops being usable.
async fn worker_tool_call(
    service: &Service,
    call: agent_run_domain::worker::WorkerToolCall,
    tool: agent_run_domain::worker::WorkerTool,
) -> Result<Value> {
    use agent_run_domain::pool::{PoolDenial, PoolMessage, PoolPropose, PoolReadRequest, PoolVote};
    use agent_run_domain::worker::WorkerTool;
    let denial = |denial: PoolDenial| {
        Ok(json!({"error": {"code": denial.code(), "message": denial.message()}}))
    };
    match tool {
        WorkerTool::Notify => {
            let input: agent_run_domain::worker::NotifyRequest = serde_json::from_value(call.input)
                .map_err(|_| invalid("unknown, missing, or incorrectly typed report argument"))?;
            let home = service.home.clone();
            let (run_id, attempt_id, token) = (
                call.run_id.clone(),
                call.attempt_id.clone(),
                call.token.clone(),
            );
            let receipt = tokio::task::spawn_blocking(move || {
                crate::state::Store::open(&home)?.notify_orchestrator(
                    &run_id,
                    &attempt_id,
                    &token,
                    &input,
                    crate::domain::now(),
                )
            })
            .await
            .map_err(|_| crate::Error::Runtime("worker notification failed".into()))??;
            Ok(serde_json::to_value(receipt)?)
        }
        WorkerTool::PoolPost | WorkerTool::PoolPropose | WorkerTool::PoolVote => {
            let write = match tool {
                WorkerTool::PoolPost => agent_run_store::pool_log::PoolWrite::Message(
                    serde_json::from_value::<PoolMessage>(call.input)
                        .map_err(|_| invalid("invalid pool_post arguments"))?,
                ),
                WorkerTool::PoolPropose => agent_run_store::pool_log::PoolWrite::Proposal(
                    serde_json::from_value::<PoolPropose>(call.input)
                        .map_err(|_| invalid("invalid pool_propose arguments"))?,
                ),
                _ => agent_run_store::pool_log::PoolWrite::Vote(
                    serde_json::from_value::<PoolVote>(call.input)
                        .map_err(|_| invalid("invalid pool_vote arguments"))?,
                ),
            };
            let home = service.home.clone();
            let (run_id, attempt_id, token) = (
                call.run_id.clone(),
                call.attempt_id.clone(),
                call.token.clone(),
            );
            let outcome = tokio::task::spawn_blocking(move || {
                crate::state::Store::open(&home)?.pool_write(&run_id, &attempt_id, &token, write)
            })
            .await
            .map_err(|_| crate::Error::Runtime("worker pool write failed".into()))??;
            match outcome {
                Ok(receipt) => Ok(json!({
                    "pool_id": receipt.pool_id,
                    "seq": receipt.seq,
                    "duplicate": receipt.duplicate,
                    "recorded": true,
                    "note": "recorded durably and readable through pool_read; not pushed or read by anyone",
                })),
                Err(reason) => denial(reason),
            }
        }
        WorkerTool::PoolRead => {
            let request: PoolReadRequest = serde_json::from_value(call.input)
                .map_err(|_| invalid("invalid pool_read arguments"))?;
            request.validate()?;
            let after_seq = request.after_seq.unwrap_or(0).min(i64::MAX as u64);
            let limit = request.limit.unwrap_or(50);
            // The bounded optional wait: one short store probe per interval,
            // never a lock across awaits, ended by data, deadline or a
            // stopped attempt. A reverse page never waits.
            let deadline = tokio::time::Instant::now()
                + Duration::from_secs_f64(
                    request
                        .wait_seconds
                        .unwrap_or(0.0)
                        .min(POOL_READ_WAIT_SECONDS),
                );
            loop {
                let home = service.home.clone();
                let (run_id, attempt_id, token) = (
                    call.run_id.clone(),
                    call.attempt_id.clone(),
                    call.token.clone(),
                );
                let probe_after = after_seq;
                let ready = tokio::task::spawn_blocking(move || {
                    crate::state::Store::open(&home)?.pool_has_entries_after(
                        &run_id,
                        &attempt_id,
                        &token,
                        probe_after,
                    )
                })
                .await
                .map_err(|_| crate::Error::Runtime("worker pool read failed".into()))??;
                match ready {
                    Err(reason) => return denial(reason),
                    Ok(true) => break,
                    Ok(false) => {
                        if tokio::time::Instant::now() >= deadline {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                }
            }
            let home = service.home.clone();
            let (run_id, attempt_id, token) = (
                call.run_id.clone(),
                call.attempt_id.clone(),
                call.token.clone(),
            );
            let page = tokio::task::spawn_blocking(move || {
                crate::state::Store::open(&home)?.pool_read(
                    &run_id,
                    &attempt_id,
                    &token,
                    after_seq,
                    request.before_seq.map(|before| before.min(i64::MAX as u64)),
                    limit,
                )
            })
            .await
            .map_err(|_| crate::Error::Runtime("worker pool read failed".into()))??;
            match page {
                Ok(page) => Ok(page),
                Err(reason) => denial(reason),
            }
        }
    }
}
