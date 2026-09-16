//! Isolated Codex app-server model roster cache.

use agent_run_domain::{error::invalid, Result};
use agent_run_platform::fs::Dir;
use serde_json::{json, Value};
use std::{path::Path, time::SystemTime};

/// Relative location of the bounded app-server roster evidence.
pub const CACHE_RELATIVE_PATH: &str = "cache/models.json";
/// Python's maximum age before it attempts another roster collection.
pub const MAX_AGE_SECONDS: u64 = 24 * 60 * 60;
const MAX_CACHE_BYTES: usize = 4 * 1024 * 1024;

/// A configured model as confirmed by one account's app-server roster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    /// Provider model identifier accepted by `thread/start`.
    pub id: String,
    /// Human-readable provider description, if supplied.
    pub description: String,
    /// Distinct reasoning effort labels supported by this model.
    pub efforts: Vec<String>,
}

/// Returns whether an existing isolated cache is young enough to avoid refresh.
///
/// Missing, unreadable, future-dated, or stale files are not evidence. The
/// caller must query the app-server before launching rather than substitute a
/// configured model for an unproven roster entry.
pub fn cache_is_fresh(home: &Path, now: SystemTime) -> bool {
    let path = home.join(CACHE_RELATIVE_PATH);
    let Ok(modified) = std::fs::metadata(path).and_then(|meta| meta.modified()) else {
        return false;
    };
    now.duration_since(modified)
        .map(|age| age.as_secs() <= MAX_AGE_SECONDS)
        .unwrap_or(false)
}

/// Reads valid roster entries from the owned cache, treating bad cache bytes as absent evidence.
///
/// The cache is advisory only: a returned roster still must be confirmed by
/// the app-server before a native turn. This matches Python's refresh failure
/// behavior while preventing a malformed cache from becoming a model choice.
pub fn read_cache(home: &Path) -> Option<Vec<Model>> {
    let dir = Dir::open(home).ok()?;
    let bytes = dir
        .optional(Path::new(CACHE_RELATIVE_PATH), MAX_CACHE_BYTES)
        .ok()??;
    let payload: Value = serde_json::from_slice(&bytes).ok()?;
    parse_roster(&payload).ok()
}

/// Rejects a model or reasoning effort disproved by fresh roster evidence.
///
/// Missing, stale, and unreadable caches deliberately return success so the
/// caller can perform Python's bounded live refresh. Only a fresh cache may
/// reject before spawn, and it uses Python's model/effort error wording rather
/// than choosing a configured fallback model.
pub fn validate_cached_selection(home: &Path, model_id: &str, effort: Option<&str>) -> Result<()> {
    if !cache_is_fresh(home, SystemTime::now()) {
        return Ok(());
    }
    let Some(models) = read_cache(home) else {
        return Ok(());
    };
    let model = models
        .iter()
        .find(|model| model.id == model_id)
        .ok_or_else(|| {
            invalid(format!(
                "model is not discovered in the codex roster cache: {model_id}"
            ))
        })?;
    if let Some(effort) = effort {
        if !model.efforts.iter().any(|choice| choice == effort) {
            return Err(invalid(format!(
                "effort {effort:?} is not offered for model {model_id:?}"
            )));
        }
    }
    Ok(())
}

/// Publishes a validated roster atomically beneath the owned runtime home.
///
/// `roster` is the exact `model/list` result received by the caller. Callers
/// may intentionally ignore write failures: cache publication is advisory
/// after a successful live roster observation.
pub fn write_cache(home: &Path, roster: &[Value]) -> Result<()> {
    let payload = json!({"models": roster});
    parse_roster(&payload)?;
    let bytes = serde_json::to_vec(&payload)?;
    if bytes.len() > MAX_CACHE_BYTES {
        return Err(invalid("Codex model roster exceeds cache bound"));
    }
    Dir::open(home)?.write(Path::new(CACHE_RELATIVE_PATH), &bytes, 0o600)
}

/// Parses provider roster variants accepted by the Python migration baseline.
///
/// Malformed individual entries are ignored like Python's cache reader. An
/// empty result remains unproven evidence; callers must reject a requested
/// model that is absent rather than substitute another configured model.
pub fn parse_roster(payload: &Value) -> Result<Vec<Model>> {
    let entries = payload
        .get("models")
        .or_else(|| payload.get("data"))
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("malformed model roster"))?;
    let mut models = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(object) = entry.as_object() else {
            continue;
        };
        let Some(id) = ["slug", "id", "model"]
            .iter()
            .find_map(|key| object.get(*key).and_then(Value::as_str))
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let mut efforts = Vec::new();
        if let Some(values) = [
            "supportedReasoningEfforts",
            "supported_reasoning_levels",
            "supported_reasoning_efforts",
            "efforts",
        ]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_array))
        {
            for value in values {
                let effort = value.as_str().or_else(|| {
                    value
                        .get("reasoningEffort")
                        .or_else(|| value.get("reasoning_effort"))
                        .or_else(|| value.get("effort"))
                        .and_then(Value::as_str)
                });
                if let Some(effort) = effort.filter(|value| !value.is_empty()) {
                    if !efforts.iter().any(|known| known == effort) {
                        efforts.push(effort.to_owned());
                    }
                }
            }
        }
        models.push(Model {
            id: id.to_owned(),
            description: object
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            efforts,
        });
    }
    Ok(models)
}
