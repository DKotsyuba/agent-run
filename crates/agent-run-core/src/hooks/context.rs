//! Bounded changed-only active-agent context for UserPromptSubmit hooks.
//!
//! Run injects only its own execution state. Provider capacity and quota
//! context belong to Agent Quota's own hook.

use crate::{
    Error, Result,
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
/// Local SQL allowance before persistence.
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
/// deduplication write fails or becomes unknown. Local SQL has a 250 ms read
/// deadline; persistence uses the remainder of the 750 ms call budget, never
/// more than 500 ms. No SQL transaction spans an await.
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
/// read components after errors and bypass a failed deduplication write.
/// Store handles are dropped before awaiting the authenticated writer operation.
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
    if at.is_some_and(|at| !at.is_finite() || at < 0.0) {
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
    let now = at.unwrap_or_else(now);
    let store = if advisory {
        let remaining = read_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::Runtime("context read budget exhausted".into()));
        }
        let mut store =
            agent_run_store::writer::StoreReader::open_with(home, remaining)?.into_inner();
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
    let (active_text, active_key) = if active_unavailable {
        ("Active-agent state unavailable; do not start replacements for existing agents. Retry their stored ids when state is readable.".into(), "unavailable".into())
    } else {
        active_block(&agents, now)
    };
    let active_slot = ACTIVE_BLOCK_MAX_CHARS.min(budget);
    let active_text = truncate(&active_text, active_slot);
    let mut components = BTreeMap::new();
    if !active_text.is_empty() {
        components.insert(
            "active".into(),
            digest(&format!("{active_slot}:{active_key}")),
        );
    }
    drop(snapshot);
    drop(store);
    let context_key = combine_key("", components.get("active").map_or("", String::as_str));
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
    let text = if changed.iter().any(|name| name == "active") {
        active_text
    } else {
        String::new()
    };
    Ok(ContextResult {
        orchestrator_session_id: session_id,
        context_key,
        injected: !text.is_empty(),
        text,
    })
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
    let guidance =
        " Use agent-run list_agents/transcript; do not start replacements for existing ids.";
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

/// Hashes a visible component into the receipt representation.
fn digest(value: &str) -> String {
    agent_run_platform::fs::sha256(value.as_bytes())
}

/// Combines component hashes into the Python-compatible 32-character context key.
fn combine_key(priority: &str, active: &str) -> String {
    digest(&format!("{priority}::{active}"))[..32].into()
}
