//! Immutable post-tool session binding and raw host-payload normalization.

use crate::{
    Error, Result,
    domain::{AgentId, OrchestratorRef, now},
    error::invalid,
    state::Store,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;

/// Native host names accepted by the common normalized lifecycle-hook projection.
const HOOK_TRANSPORTS: [&str; 3] = ["claude_uds", "codex_queue", "opencode"];

/// One successful immutable durable-agent binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindResult {
    /// Stable agent identity for the bound execution's lineage.
    pub agent_id: AgentId,
    /// Exact durable execution whose receipt was bound, never a moving alias.
    #[serde(skip_serializing)]
    pub run_id: AgentId,
    /// Shown name for the selected execution.
    #[serde(skip_serializing)]
    pub name: String,
    /// Profile for the selected execution.
    #[serde(skip_serializing)]
    pub profile: String,
    /// The opaque durable orchestrator-session primary key.
    pub session_id: String,
}

impl BindResult {
    /// Formats the display identity used in both binding confirmations.
    pub(crate) fn agent_label(&self) -> String {
        agent_run_domain::agent_label(&self.name, &self.profile, self.agent_id.as_str())
    }

    /// Returns the exact safe confirmation injected into the post-tool host turn.
    pub fn message(&self) -> String {
        format!(
            "agent-run: agent {} is bound; its completion will be delivered to this chat.",
            self.agent_label()
        )
    }
}

/// Binds one known agent once and activates an already-terminal waiting receipt.
///
/// The same target is idempotent.  A conflicting target, malformed reference,
/// or missing agent is converted by [`run_hook`] into the deliberately loud
/// confirmation failure expected by host hooks.
pub async fn bind(
    home: &std::path::Path,
    agent_id: AgentId,
    reference: OrchestratorRef,
    at: f64,
) -> Result<BindResult> {
    bind_with_service(home, agent_id, reference, at, None).await
}

/// Bind through the resident writer, using API peer evidence when present.
async fn bind_with_service(
    home: &std::path::Path,
    agent_id: AgentId,
    reference: OrchestratorRef,
    at: f64,
    service: Option<&crate::service::Service>,
) -> Result<BindResult> {
    let row = Store::open(home)?.get(&agent_id)?;
    let binding = agent_run_store::ops::BindOrchestrator {
        execution: agent_id.clone(),
        reference,
        at,
    };
    let budget = crate::broker_writer::BROKER_BUDGET;
    let session_id = match service.and_then(|service| service.peer_uid.map(|uid| (service, uid))) {
        Some((service, owner_uid)) => {
            service
                .lane("hooks")?
                .local(
                    agent_run_store::ops::BindOrchestratorPeer { binding, owner_uid },
                    budget,
                )
                .await?
        }
        None => {
            crate::persist::operator_producer(home)
                .call(binding, budget)
                .await?
        }
    };
    Ok(BindResult {
        name: row.shown_name(),
        profile: row.request.profile,
        agent_id: row.root_agent_id,
        run_id: agent_id,
        session_id,
    })
}

/// Normalize one native host hook payload and bind its discovered agent.
///
/// Raw envelopes are accepted only with their expected event name and derive
/// the session from `session_id`; normalized payloads must contain exactly the
/// documented bind fields.  Every failure is rendered as a loud, secret-safe
/// error so the host keeps the turn available for recovery.
pub async fn run_hook(
    home: &std::path::Path,
    payload: &Value,
    transport: &str,
    at: Option<f64>,
) -> Result<BindResult> {
    run_hook_with_service(home, payload, transport, at, None).await
}

/// Normalize one hook payload and route the binding through its verified API connection.
pub(crate) async fn run_hook_with_service(
    home: &std::path::Path,
    payload: &Value,
    transport: &str,
    at: Option<f64>,
    service: Option<&crate::service::Service>,
) -> Result<BindResult> {
    let agent_hint = payload
        .get("agent_id")
        .and_then(Value::as_str)
        .unwrap_or("None");
    let normalized =
        normalize(payload, true, transport).map_err(|error| loud(agent_hint, &error))?;
    let agent_id = normalized
        .agent_id
        .ok_or_else(|| loud(agent_hint, "unusable hook payload (missing agent_id)"))?;
    let parsed = agent_id
        .parse::<AgentId>()
        .map_err(|error| loud(&agent_id, &error))?;
    let execution = match normalized.sequence {
        Some(sequence) => {
            crate::agent_identity::resolve_sequence(&Store::open(home)?, &parsed, sequence)
                .map_err(|error| loud(&agent_id, &error))?
                .id
        }
        None => parsed,
    };
    bind_with_service(
        home,
        execution,
        normalized.reference,
        at.unwrap_or_else(now),
        service,
    )
    .await
    .map_err(|error| loud(&agent_id, &error))
}

/// Represents the normalized session reference and optional discovered agent id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookPayload {
    /// The normalized orchestrator identity scoped to one host chat session.
    pub reference: OrchestratorRef,
    /// The post-tool durable agent id; user-prompt context has no agent id.
    pub agent_id: Option<String>,
    /// Exact internal receipt counter; absent for legacy exact-id payloads.
    pub sequence: Option<u32>,
}

/// Read a blocked-start receipt from the existing post-tool hook. The host
/// supplies the session even when no agent was created by the refused call.
pub fn restart_waiter(payload: &Value, transport: &str) -> Result<Option<(i64, OrchestratorRef)>> {
    let Some(object) = payload.as_object() else {
        return Err(invalid("hook payload must be a JSON object"));
    };
    let mut ids = BTreeSet::new();
    if let Some(response) = object.get("tool_response") {
        collect_restart_ids(response, &mut ids)?;
    }
    if ids.is_empty() {
        return Ok(None);
    }
    if ids.len() != 1
        || object
            .get("hook_event_name")
            .is_some_and(|event| event.as_str() != Some("PostToolUse"))
    {
        return Err(invalid("invalid restart hook receipt"));
    }
    let reference = OrchestratorRef {
        transport: transport.into(),
        external_session_id: required_string(object.get("session_id"), "session_id")?,
        external_turn_id: object.get("turn_id").map(value_string).transpose()?,
    };
    reference.validate()?;
    Ok(Some((ids.pop_first().expect("one restart id"), reference)))
}

/// Inspect only structured or JSON-encoded response envelopes for the marker.
fn collect_restart_ids(value: &Value, ids: &mut BTreeSet<i64>) -> Result<()> {
    match value {
        Value::Object(object) => {
            if object.get("accepted") == Some(&Value::Bool(false))
                && let Some(id) = object.get("agent_run_restart_id")
            {
                ids.insert(
                    id.as_i64()
                        .filter(|id| *id > 0)
                        .ok_or_else(|| invalid("invalid restart receipt"))?,
                );
            }
            for item in object.values() {
                collect_restart_ids(item, ids)?;
            }
        }
        Value::Array(values) => {
            for item in values {
                collect_restart_ids(item, ids)?;
            }
        }
        Value::String(text) => {
            if let Ok(decoded) = serde_json::from_str::<Value>(text) {
                collect_restart_ids(&decoded, ids)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Converts raw or normalized context/bind payloads into one strict host-neutral form.
pub fn normalize(payload: &Value, bind: bool, transport: &str) -> Result<HookPayload> {
    if !HOOK_TRANSPORTS.contains(&transport) {
        return Err(invalid("unknown delivery transport"));
    }
    let object = payload
        .as_object()
        .ok_or_else(|| invalid("hook payload must be a JSON object"))?;
    if object.contains_key("session_id") {
        let expected = if bind {
            "PostToolUse"
        } else {
            "UserPromptSubmit"
        };
        if object
            .get("hook_event_name")
            .is_some_and(|event| event.as_str() != Some(expected))
        {
            return Err(invalid(format!("raw hook_event_name must be {expected}")));
        }
        let session_id = object
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("raw hook session_id must be nonblank"))?;
        let reference = OrchestratorRef {
            transport: transport.into(),
            external_session_id: session_id.into(),
            external_turn_id: object.get("turn_id").map(value_string).transpose()?,
        };
        reference.validate()?;
        let (agent_id, sequence) = if bind {
            let (id, sequence) = raw_selection(object.get("tool_response"))?;
            (Some(id), sequence)
        } else {
            (None, None)
        };
        return Ok(HookPayload {
            reference,
            agent_id,
            sequence,
        });
    }
    let allowed: BTreeSet<&str> = if bind {
        BTreeSet::from([
            "agent_id",
            "run_id",
            "sequence",
            "transport",
            "external_session_id",
            "external_turn_id",
        ])
    } else {
        BTreeSet::from(["transport", "external_session_id", "external_turn_id"])
    };
    if object.keys().any(|key| !allowed.contains(key.as_str())) {
        return Err(invalid("unknown hook payload keys"));
    }
    let reference = OrchestratorRef {
        transport: required_string(object.get("transport"), "transport")?,
        external_session_id: required_string(
            object.get("external_session_id"),
            "external_session_id",
        )?,
        external_turn_id: object
            .get("external_turn_id")
            .map(value_string)
            .transpose()?,
    };
    reference.validate()?;
    let agent_id = bind
        .then(|| {
            let field = if object.contains_key("run_id") {
                "run_id"
            } else {
                "agent_id"
            };
            required_string(object.get(field), field)
        })
        .transpose()?;
    Ok(HookPayload {
        reference,
        agent_id,
        sequence: if bind && !object.contains_key("run_id") {
            object.get("sequence").map(receipt_sequence).transpose()?
        } else {
            None
        },
    })
}

/// Parses the positive bounded counter carried only by machine binding receipts.
fn receipt_sequence(value: &Value) -> Result<u32> {
    value
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("binding receipt sequence must be a positive integer"))
}

/// Accepts old exact-id receipts or one stable-id/sequence pair without following a tip.
fn raw_selection(value: Option<&Value>) -> Result<(String, Option<u32>)> {
    let mut legacy = BTreeSet::new();
    if let Some(value) = value {
        collect_ids(value, "run_id", &mut legacy)?;
    }
    if !legacy.is_empty() {
        return Ok((raw_agent_id(value)?, None));
    }
    let mut receipts = BTreeSet::new();
    if let Some(value) = value {
        collect_receipts(value, &mut receipts)?;
    }
    match receipts.len() {
        0 => Ok((raw_agent_id(value)?, None)),
        1 => {
            let (id, sequence) = receipts.pop_first().expect("one receipt");
            Ok((id, Some(sequence)))
        }
        _ => Err(invalid(
            "raw PostToolUse payload has conflicting binding receipts",
        )),
    }
}

/// Collects receipt metadata while keeping task, answer and policy content opaque.
fn collect_receipts(value: &Value, receipts: &mut BTreeSet<(String, u32)>) -> Result<()> {
    match value {
        Value::Object(object) if object.contains_key("agent_id") => {
            let id = required_string(object.get("agent_id"), "agent_id")?;
            if let Some(sequence) = object.get("sequence") {
                receipts.insert((id, receipt_sequence(sequence)?));
            }
            for name in ["agent", "delivery"] {
                if let Some(value) = object.get(name) {
                    collect_receipts(value, receipts)?;
                }
            }
        }
        Value::Object(object) => {
            for value in object.values() {
                collect_receipts(value, receipts)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_receipts(value, receipts)?;
            }
        }
        Value::String(text) => {
            if let Ok(value) = serde_json::from_str::<Value>(text) {
                collect_receipts(&value, receipts)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Returns a mandatory string field without echoing its potentially sensitive value.
fn required_string(value: Option<&Value>, name: &str) -> Result<String> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("{name} must be a nonblank string")))
}

/// Returns an optional string field, rejecting non-string values.
fn value_string(value: &Value) -> Result<String> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("hook identifier must be a string"))
}

/// Extract one exact run id, falling back to legacy agent ids only when absent.
///
/// Stable agent ids must never make a delayed resume hook bind the root run.
/// Conflicting ids still fail closed; no latest-run lookup is performed here.
fn raw_agent_id(value: Option<&Value>) -> Result<String> {
    let mut ids = BTreeSet::new();
    if let Some(value) = value {
        collect_ids(value, "run_id", &mut ids)?;
        if ids.is_empty() {
            collect_ids(value, "agent_id", &mut ids)?;
        }
    }
    match ids.len() {
        0 => Err(invalid("raw PostToolUse payload has no agent_id")),
        1 => Ok(ids.pop_first().expect("one id")),
        _ => Err(invalid(
            "raw PostToolUse payload has conflicting agent_id values",
        )),
    }
}

/// Search structured and JSON-encoded envelopes for one identity field.
/// Run ids are read only from agent metadata, never from task/answer text inside
/// an admission object. Known nested agent/delivery metadata still participates
/// in conflict detection, as do separate mirrored host envelopes.
fn collect_ids(value: &Value, field: &str, ids: &mut BTreeSet<String>) -> Result<()> {
    match value {
        Value::Object(object) => {
            if field == "run_id" && object.contains_key("agent_id") {
                required_string(object.get("agent_id"), "agent_id")?;
                if let Some(run) = object.get("run_id") {
                    ids.insert(required_string(Some(run), "run_id")?);
                }
                for key in ["agent", "delivery"] {
                    if let Some(metadata) = object.get(key) {
                        collect_ids(metadata, field, ids)?;
                    }
                }
                return Ok(());
            }
            for (key, item) in object {
                if key == field && field != "run_id" {
                    ids.insert(required_string(Some(item), field)?);
                } else {
                    collect_ids(item, field, ids)?;
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_ids(item, field, ids)?;
            }
        }
        Value::String(text) => {
            if let Ok(decoded) = serde_json::from_str(text) {
                collect_ids(&decoded, field, ids)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Formats the required host-visible notification-confirmation failure.
fn loud(agent_id: &str, reason: &(impl std::fmt::Display + ?Sized)) -> Error {
    Error::Runtime(format!(
        "agent-run: chat notification is NOT confirmed for {agent_id}: {reason}. The agent may still be running; keep this turn alive and recover with agent-run bind."
    ))
}
