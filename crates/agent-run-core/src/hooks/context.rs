//! Bounded changed-only routing context for UserPromptSubmit hooks.

use crate::{
    Error, Result, capacity,
    config::Config,
    domain::{OrchestratorRef, now},
    error::invalid,
    state::Store,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

/// Absolute ceiling for every host-injected context block.
pub const CONTEXT_HARD_LIMIT_CHARS: usize = 2500;
/// Reserved maximum allocation for the active-agent summary.
pub const ACTIVE_BLOCK_MAX_CHARS: usize = 600;
/// End-to-end cooperative budget for optional context, including SQL and persistence.
const HOOK_BUDGET: Duration = Duration::from_millis(750);
/// SQL work stops early enough to leave the existing write budget available.
const READ_BUDGET: Duration = Duration::from_millis(250);
/// Existing maximum wait for the writer acknowledgement.
const WRITE_BUDGET: Duration = Duration::from_millis(500);
const MAX_LISTED_AGENTS: usize = 5;
const SILENCE_THRESHOLD_SECONDS: f64 = 60.0;

/// The visible context outcome for one host turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContextResult {
    /// Existing or newly recorded session id; absent for disabled or unrecorded context.
    pub orchestrator_session_id: Option<String>,
    /// Stable bounded digest of the currently visible component fingerprints.
    pub context_key: String,
    /// Changed components, or the full prepared snapshot after an advisory write failure.
    pub text: String,
    /// Whether `text` should be injected by the host hook response.
    pub injected: bool,
}

/// Builds strict changed-only context for an explicit operator request.
///
/// Uses the existing read and write allowances. Configuration, validation and
/// persistence failures propagate; no successful result claims an unconfirmed
/// write. `home` must contain a current store and validated configuration;
/// `reference` identifies the host session. `at` is finite nonnegative Unix
/// seconds or absent for the current clock. A zero budget makes no write.
pub async fn build(
    home: &Path,
    reference: &OrchestratorRef,
    at: Option<f64>,
) -> Result<ContextResult> {
    build_inner(home, reference, at, false).await
}

/// Builds optional host context without losing a prepared snapshot when its
/// deduplication write fails or becomes unknown. Reads share one read-only
/// snapshot and a 250 ms SQL deadline; persistence uses the remainder of the
/// 750 ms call budget, never more than 500 ms. No background read is detached.
///
/// Available components survive an independent component read failure. Failures
/// before any context can be prepared propagate for the CLI's nonblocking hook
/// boundary. No fallback confirms a session binding, write or completion.
/// `home`, `reference` and optional Unix timestamp `at` follow [`build`]. A zero
/// character budget performs no state read or mutation and returns no session.
pub async fn build_for_hook(
    home: &Path,
    reference: &OrchestratorRef,
    at: Option<f64>,
) -> Result<ContextResult> {
    build_inner(home, reference, at, true).await
}

/// Records a secret-safe optional-context failure in the existing bounded log.
/// `stored_schema` is an independently observed schema number, or absent when
/// unknown. With no configured sink, emits only closed metadata to stderr.
/// Never reads the database or exposes the raw error, prompt or config.
pub fn log_failure(error: &Error, stored_schema: Option<i64>) {
    let mut metadata = crate::supervisor::execution_error_metadata(error);
    metadata["stage"] = serde_json::json!("hook_context");
    metadata["supported_schema"] = serde_json::json!(agent_run_store::VERSION);
    if let Some(version) = stored_schema {
        metadata["stored_schema"] = serde_json::json!(version);
    }
    let message = format!("hook_context_unavailable {metadata}");
    if let Some(logger) = crate::logging::configured() {
        logger.log(crate::logging::Level::Warning, &message);
    } else {
        eprintln!("{message}");
    }
}

/// Shared strict/advisory implementation. Only the advisory caller may retain
/// read components after errors and bypass a failed deduplication write. All
/// store handles are dropped before awaiting the authenticated writer operation.
async fn build_inner(
    home: &Path,
    reference: &OrchestratorRef,
    at: Option<f64>,
    advisory: bool,
) -> Result<ContextResult> {
    let started = Instant::now();
    let deadline = started + HOOK_BUDGET;
    reference.validate()?;
    let providers = agent_run_config::provider_config::ProviderConfig::load(home).ok();
    let legacy = if providers.is_none() {
        Some(Config::load(home)?)
    } else {
        None
    };
    let max_chars = match &providers {
        Some((config, _)) => config.capacity.context_max_chars,
        None => {
            legacy
                .as_ref()
                .expect("loaded legacy config")
                .capacity
                .context_max_chars
        }
    };
    let now = at.unwrap_or_else(now);
    if !now.is_finite() || now < 0.0 {
        return Err(invalid("now must be a finite nonnegative number"));
    }
    let budget = max_chars.min(CONTEXT_HARD_LIMIT_CHARS);
    if budget == 0 && advisory {
        return Ok(ContextResult {
            orchestrator_session_id: None,
            context_key: combine_key("", "0:0"),
            text: String::new(),
            injected: false,
        });
    }
    let read_deadline = started + READ_BUDGET;
    let store = if advisory {
        let remaining = read_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::Runtime("context read budget exhausted".into()));
        }
        let store = agent_run_store::writer::StoreReader::open_with(home, remaining)?.into_inner();
        store
            .conn
            .progress_handler(1000, Some(move || Instant::now() >= read_deadline))?;
        store
    } else {
        Store::open(home)?
    };
    let snapshot = store.conn.unchecked_transaction()?;
    let active = (|| {
        let session = store.find_orchestrator_session(reference)?;
        let agents = match &session {
            Some(session) => store.active_context(session, now, 1000)?,
            None => Vec::new(),
        };
        Ok::<_, Error>((session, agents))
    })();
    let (prior_session, agents, active_unavailable) = match active {
        Ok((session, agents)) => (session, agents, false),
        Err(error) if advisory => {
            log_failure(&error, None);
            (None, Vec::new(), true)
        }
        Err(error) => return Err(error),
    };
    let capacity = if advisory && Instant::now() >= read_deadline {
        Err(Error::Runtime("context read budget exhausted".into()))
    } else {
        if advisory {
            store
                .conn
                .busy_timeout(read_deadline.saturating_duration_since(Instant::now()))?;
        }
        match &providers {
            Some((config, revision)) => provider_block(&store, config, revision),
            None => capacity_block(&store, legacy.as_ref().expect("loaded legacy config")),
        }
    };
    let capacity_text = match capacity {
        Ok(text) => text,
        Err(error) if advisory => {
            log_failure(&error, None);
            "Capacity order unavailable; do not infer current provider availability.".into()
        }
        Err(error) => return Err(error),
    };
    let (active_text, active_key) = if active_unavailable {
        ("Active-agent state unavailable; do not start replacements for existing agents. Retry their stored ids when state is readable.".into(), "unavailable".into())
    } else {
        active_block(&agents, now)
    };
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
    drop(snapshot);
    drop(store);
    let context_key = combine_key(
        components.get("priority").map_or("", String::as_str),
        components.get("active").map_or("", String::as_str),
    );
    if components.is_empty() {
        return Ok(ContextResult {
            orchestrator_session_id: prior_session,
            context_key: combine_key("", &active_key),
            text: String::new(),
            injected: false,
        });
    }
    let write_budget = if advisory {
        deadline
            .saturating_duration_since(Instant::now())
            .min(WRITE_BUDGET)
    } else {
        WRITE_BUDGET
    };
    let persisted = if write_budget.is_zero() {
        Err(Error::Runtime(
            "context persistence budget exhausted".into(),
        ))
    } else {
        crate::persist::operator_producer(home)
            .call(
                agent_run_store::ops::RecordContextComponents {
                    reference: reference.clone(),
                    components: components.clone(),
                    at: now,
                },
                write_budget,
            )
            .await
    };
    let (session_id, changed) = match persisted {
        Ok((session_id, changed)) => (Some(session_id), changed),
        Err(error) if advisory => {
            log_failure(&error, None);
            (prior_session, components.keys().cloned().collect())
        }
        Err(error) => return Err(error),
    };
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
        orchestrator_session_id: session_id,
        context_key,
        injected: !text.is_empty(),
        text,
    })
}

/// Renders the schema-2 provider-only capacity order: one line per provider
/// with each offered model's cached status, never an account or credential.
/// It is read-only standing, not a choice; the caller picks provider/model.
fn provider_block(
    store: &Store,
    config: &agent_run_config::provider_config::ProviderConfig,
    revision: &str,
) -> Result<String> {
    let order =
        capacity::provider_catalog::order_from_store(store, config, revision, &Default::default())?;
    let providers = order["providers"]
        .as_array()
        .ok_or_else(|| invalid("provider order has invalid providers"))?;
    let mut lines = vec!["Provider capacity order (highest first; read-only). Pick provider, model, effort and profile yourself; use models for roles, recommendations and evidence. unknown = no current sample (missing or stale), not a health claim.".to_owned()];
    if providers.is_empty() {
        lines.push("No configured providers.".into());
    }
    for (index, entry) in providers.iter().enumerate() {
        let models = entry["models"]
            .as_array()
            .ok_or_else(|| invalid("provider order has invalid models"))?
            .iter()
            .map(|model| {
                format!(
                    "{}:{}",
                    json_ascii(model["model"].as_str().unwrap_or_default()),
                    model["quota"]["status"].as_str().unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        lines.push(format!(
            "{}. provider={}; models=[{}]",
            index + 1,
            json_ascii(entry["provider"].as_str().unwrap_or_default()),
            models
        ));
    }
    Ok(lines.join("\n"))
}

/// Renders the ordered capacity routes without exposing measurements or credentials.
fn capacity_block(store: &Store, config: &Config) -> Result<String> {
    let order = capacity::order_from_store(config, store, crate::domain::now())?;
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
            let root = string(agent, "root_agent_id");
            let id = if root.is_empty() {
                string(agent, "id")
            } else {
                root
            };
            let runtime = string(agent, "runtime");
            let model = string(agent, "model");
            let profile = string(agent, "profile");
            let summary = safe_summary(&string(agent, "task_summary"));
            let status = string(agent, "status");
            let started = number(agent, "started_at")
                .or_else(|| number(agent, "created_at"))
                .unwrap_or(at);
            let elapsed = ((at - started).max(0.0) / 60.0).floor() as u64;
            let silent = number(agent, "silent_seconds")
                .is_some_and(|seconds| seconds >= SILENCE_THRESHOLD_SECONDS);
            key.push(format!("{id}:{status}:{silent}"));
            format!(
                "{id} {runtime}/{model} {profile} {summary} {status} {elapsed}m{}",
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
    agent_run_platform::fs::sha256(value.as_bytes())
}

/// Combines component hashes into the Python-compatible 32-character context key.
fn combine_key(priority: &str, active: &str) -> String {
    digest(&format!("{priority}::{active}"))[..32].into()
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
