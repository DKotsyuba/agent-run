//! Immutable post-tool session binding and raw host-payload normalization.

use crate::{
    domain::{now, AgentId, OrchestratorRef},
    error::invalid,
    state::Store,
    Error, Result,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;

const HOOK_TRANSPORTS: [&str; 2] = ["claude_uds", "codex_queue"];

/// One successful immutable durable-agent binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindResult {
    /// Stable agent identity for the bound execution's lineage.
    pub agent_id: AgentId,
    /// Exact durable execution whose receipt was bound, never a moving alias.
    pub run_id: AgentId,
    /// The opaque durable orchestrator-session primary key.
    pub session_id: String,
}

impl BindResult {
    /// Returns the exact safe confirmation injected into the post-tool host turn.
    pub fn message(&self) -> String {
        let execution = if self.run_id == self.agent_id {
            String::new()
        } else {
            format!(" (run {})", self.run_id)
        };
        format!(
            "agent-run: agent {}{} is bound to session {}; its completion will be delivered to this chat.",
            self.agent_id, execution, self.session_id
        )
    }
}

/// Binds one known agent once and activates an already-terminal waiting receipt.
///
/// The same target is idempotent.  A conflicting target, malformed reference,
/// or missing agent is converted by [`run_hook`] into the deliberately loud
/// confirmation failure expected by host hooks.
pub fn bind(
    store: &mut Store,
    agent_id: AgentId,
    reference: OrchestratorRef,
    at: f64,
) -> Result<BindResult> {
    let root = store.get(&agent_id)?.root_agent_id;
    let session_id = store.bind_orchestrator(&agent_id, &reference, at)?;
    Ok(BindResult {
        agent_id: root,
        run_id: agent_id,
        session_id,
    })
}

/// Normalizes one Codex/Claude hook payload and binds its discovered agent.
///
/// Raw envelopes are accepted only with their expected event name and derive
/// the session from `session_id`; normalized payloads must contain exactly the
/// documented bind fields.  Every failure is rendered as a loud, secret-safe
/// error so the host keeps the turn available for recovery.
pub fn run_hook(
    store: &mut Store,
    payload: &Value,
    transport: &str,
    at: Option<f64>,
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
    bind(store, parsed, normalized.reference, at.unwrap_or_else(now))
        .map_err(|error| loud(&agent_id, &error))
}

/// Represents the normalized session reference and optional discovered agent id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookPayload {
    /// The normalized orchestrator identity scoped to one host chat session.
    pub reference: OrchestratorRef,
    /// The post-tool durable agent id; user-prompt context has no agent id.
    pub agent_id: Option<String>,
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
        let agent_id = if bind {
            Some(raw_agent_id(object.get("tool_response"))?)
        } else {
            None
        };
        return Ok(HookPayload {
            reference,
            agent_id,
        });
    }
    let allowed: BTreeSet<&str> = if bind {
        BTreeSet::from([
            "agent_id",
            "run_id",
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
    })
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
    Error::Runtime(format!("agent-run: chat notification is NOT confirmed for {agent_id}: {reason}. The agent may still be running; keep this turn alive and recover with agent-run bind."))
}
