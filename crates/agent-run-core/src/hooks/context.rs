//! Bounded changed-only routing context for UserPromptSubmit hooks.

use crate::{
    capacity,
    config::Config,
    domain::{now, OrchestratorRef},
    error::invalid,
    state::Store,
    Result,
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

/// Absolute ceiling for every host-injected context block.
pub const CONTEXT_HARD_LIMIT_CHARS: usize = 2500;
/// Reserved maximum allocation for the active-agent summary.
pub const ACTIVE_BLOCK_MAX_CHARS: usize = 600;
const MAX_LISTED_AGENTS: usize = 5;
const SILENCE_THRESHOLD_SECONDS: f64 = 60.0;

/// The visible context outcome for one host turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContextResult {
    /// Existing or newly created durable session id, unless the budget was zero.
    pub orchestrator_session_id: Option<String>,
    /// Stable bounded digest of the currently visible component fingerprints.
    pub context_key: String,
    /// Changed visible components only, in priority then active-agent order.
    pub text: String,
    /// Whether `text` should be injected by the host hook response.
    pub injected: bool,
}

/// Builds changed-only session context from persisted capacity and active-agent state.
///
/// The function opens only short-lived store connections.  A zero budget makes
/// no write, while a nonzero visible component is atomically deduplicated by
/// the store's receipt operation.  Task and transcript text are never read;
/// only the bounded `task_summary` projection is eligible for display.
pub fn build(home: &Path, reference: &OrchestratorRef, at: Option<f64>) -> Result<ContextResult> {
    reference.validate()?;
    let config = Config::load(home)?;
    let now = at.unwrap_or_else(now);
    if !now.is_finite() || now < 0.0 {
        return Err(invalid("now must be a finite nonnegative number"));
    }
    let budget = config
        .capacity
        .context_max_chars
        .min(CONTEXT_HARD_LIMIT_CHARS);
    let mut store = Store::open(home)?;
    let prior_session = store.find_orchestrator_session(reference)?;
    let capacity_text = capacity_block(home)?;
    let agents = match &prior_session {
        Some(session) => store.active_context(session, now, 1000)?,
        None => Vec::new(),
    };
    let (active_text, active_key) = active_block(&agents, now);
    let (priority_text, active_text, active_slot) = assemble(&capacity_text, &active_text, budget);
    let mut components = BTreeMap::new();
    if !priority_text.is_empty() {
        components.insert("priority".into(), digest(&priority_text));
    }
    if !active_text.is_empty() {
        components.insert(
            "active".into(),
            digest(&format!("{active_slot}:{active_key}")),
        );
    }
    if components.is_empty() {
        return Ok(ContextResult {
            orchestrator_session_id: prior_session,
            context_key: combine_key("", &active_key),
            text: String::new(),
            injected: false,
        });
    }
    let (session_id, changed) =
        store.record_context_components_for_ref(reference, &components, now)?;
    let text = [
        changed
            .iter()
            .any(|name| name == "priority")
            .then_some(priority_text),
        changed
            .iter()
            .any(|name| name == "active")
            .then_some(active_text),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n");
    Ok(ContextResult {
        orchestrator_session_id: Some(session_id),
        context_key: combine_key(
            components.get("priority").map_or("", String::as_str),
            components.get("active").map_or("", String::as_str),
        ),
        injected: !text.is_empty(),
        text,
    })
}

/// Renders the ordered capacity routes without exposing measurements or credentials.
fn capacity_block(home: &Path) -> Result<String> {
    let order = capacity::order(home)?;
    let routes = order
        .get("routes")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("capacity order has invalid routes"))?;
    let mut lines = vec!["Runtime priorities (highest first). Choose the first compatible subagent using the role/model table. A route applies only to a model belonging to its quota lane; if incompatible, skip the entire entry. account=null means omit the account selector. Do not recheck raw limits.".into()];
    if routes.is_empty() {
        lines.push("No currently available routes.".into());
    }
    for (index, route) in routes.iter().enumerate() {
        let runtime = route
            .get("runtime")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("capacity route runtime is invalid"))?;
        let priority = route
            .get("priority")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
            .ok_or_else(|| invalid("capacity route priority is invalid"))?;
        let aliases = route
            .get("aliases")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("capacity route aliases are invalid"))?;
        let selectors = aliases
            .iter()
            .map(selector)
            .collect::<Result<Vec<_>>>()?
            .join(",");
        lines.push(format!(
            "{}. runtime={}; selectors=[{}]; priority={priority:.3}",
            index + 1,
            json_ascii(runtime),
            selectors
        ));
    }
    Ok(lines.join("\n"))
}

/// Renders one compact JSON-safe selector from a ranked route alias.
fn selector(alias: &Value) -> Result<String> {
    let lane = alias
        .get("quota_lane")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("capacity alias quota lane is invalid"))?;
    let account = match alias.get("account") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(value)) => format!(",\"account\":{}", json_ascii(value)),
        Some(_) => return Err(invalid("capacity alias account is invalid")),
    };
    Ok(format!("{{\"quota_lane\":{}{account}}}", json_ascii(lane)))
}

/// Encodes a JSON string with Python-compatible ASCII-only escaping.
fn json_ascii(value: &str) -> String {
    let encoded = serde_json::to_string(value).expect("strings serialize");
    let mut output = String::with_capacity(encoded.len());
    for character in encoded.chars() {
        if character.is_ascii() {
            output.push(character);
        } else if (character as u32) <= 0xffff {
            output.push_str(&format!("\\u{:04x}", character as u32));
        } else {
            let code = character as u32 - 0x1_0000;
            output.push_str(&format!(
                "\\u{:04x}\\u{:04x}",
                0xd800 + (code >> 10),
                0xdc00 + (code & 0x3ff)
            ));
        }
    }
    output
}

/// Creates the bounded active-agent summary and its semantic receipt key.
fn active_block(agents: &[Value], at: f64) -> (String, String) {
    if agents.is_empty() {
        return (String::new(), "0:0".into());
    }
    let total = agents.len();
    let mut key = vec![total.to_string()];
    let entries = agents
        .iter()
        .take(MAX_LISTED_AGENTS)
        .map(|agent| {
            let id = string(agent, "id");
            let runtime = string(agent, "runtime");
            let model = string(agent, "model");
            let profile = string(agent, "profile");
            let summary = safe_summary(&string(agent, "task_summary"));
            let status = string(agent, "status");
            let started = number(agent, "started_at")
                .or_else(|| number(agent, "created_at"))
                .unwrap_or(at);
            let elapsed = ((at - started).max(0.0) / 60.0).floor() as u64;
            let warned = agent
                .get("warned")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let silent = number(agent, "silent_seconds")
                .is_some_and(|seconds| seconds >= SILENCE_THRESHOLD_SECONDS);
            key.push(format!("{id}:{status}:{warned}:{silent}"));
            format!(
                "{id} {runtime}/{model} {profile} {summary} {status} {elapsed}m{}{}",
                if warned { " warn" } else { "" },
                if silent { " silent" } else { "" }
            )
        })
        .collect::<Vec<_>>();
    let suffix = if total > MAX_LISTED_AGENTS {
        format!("; +{} more", total - MAX_LISTED_AGENTS)
    } else {
        String::new()
    };
    let guidance = " Use agent-run status/transcript; do not start replacements for existing ids.";
    let body = format!("Active agents ({total}): {}{suffix}.", entries.join("; "));
    (
        format!(
            "{}{}",
            truncate(
                &body,
                ACTIVE_BLOCK_MAX_CHARS.saturating_sub(guidance.chars().count())
            ),
            guidance
        ),
        key.join("|"),
    )
}

/// Gets a safe string field from an internal projection, treating malformed data as empty.
fn string(value: &Value, name: &str) -> String {
    value
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .into()
}

/// Gets one finite numeric field from an internal projection.
fn number(value: &Value, name: &str) -> Option<f64> {
    value
        .get(name)
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite())
}

/// Normalizes whitespace/control characters and limits summaries to 48 code points.
fn safe_summary(value: &str) -> String {
    truncate(
        &value
            .chars()
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
        48,
    )
}

/// Truncates by Unicode code point and appends Python's ellipsis when possible.
fn truncate(value: &str, limit: usize) -> String {
    let count = value.chars().count();
    if count <= limit {
        return value.into();
    }
    if limit <= 1 {
        return value.chars().take(limit).collect();
    }
    let prefix = value.chars().take(limit - 1).collect::<String>();
    format!("{}…", prefix.trim_end())
}

/// Keeps complete priority lines and the prescribed omission instruction within a slot.
fn truncate_priority(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.into();
    }
    let lines = value.lines().collect::<Vec<_>>();
    let hint = "More routes omitted; use capacity_order if needed";
    if lines.is_empty() || lines[0].chars().count() + 1 + hint.chars().count() > limit {
        return String::new();
    }
    let mut kept = vec![lines[0]];
    for line in &lines[1..] {
        let candidate = [kept.join("\n"), (*line).into(), hint.into()].join("\n");
        if candidate.chars().count() > limit {
            break;
        }
        kept.push(line);
    }
    [kept.join("\n"), hint.into()].join("\n")
}

/// Reserves a fixed active slot so active changes never resize priority text.
fn assemble(priority: &str, active: &str, budget: usize) -> (String, String, usize) {
    let active_slot = ACTIVE_BLOCK_MAX_CHARS.min(budget);
    let priority_slot = budget.saturating_sub(active_slot + 1);
    (
        truncate_priority(priority, priority_slot),
        truncate(active, active_slot),
        active_slot,
    )
}

/// Hashes a visible component into the receipt representation.
fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

/// Combines component hashes into the Python-compatible 32-character context key.
fn combine_key(priority: &str, active: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("{priority}::{active}").as_bytes())
    )[..32]
        .into()
}

#[cfg(test)]
mod tests {
    /// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_unicode_separators_cannot_split_a_route_line`.
    #[test]
    fn unicode_separators_are_escaped_inside_one_route_line() {
        let text = format!(
            "Runtime priorities.\n1. runtime={}",
            super::json_ascii("name\u{2028}line")
        );
        assert!(!text.contains('\u{2028}'));
        assert!(text.contains("\\u2028"));
        assert_eq!(text.lines().count(), 2);
    }
}
