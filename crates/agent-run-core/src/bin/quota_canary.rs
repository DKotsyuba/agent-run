//! Credential-safe live quota-source canary.
//!
//! Performs one real read-only quota request against an already approved
//! account and prints ONLY normalized facts: HTTP status, typed category,
//! presence and bounded value of `retry-after`, the payload's top-level key
//! names, and the `limits[]` entry field names and kind/type spellings.
//! Credential bytes, raw bodies, headers, and full remote payloads are never
//! printed, logged, or persisted; production configuration and databases are
//! untouched.
//!
//! Usage: `quota_canary <glm|anthropic> [origin]` or
//! `quota_canary codex [label]` (see `codex`).
//! `glm` resolves the managed Keychain item (`com.pluto.agent-run.glm`,
//! account `GLM_CODING_KEY`) exactly as the GLM adapter does; `anthropic`
//! resolves the host native Claude login through the quota reader (the
//! same store the provider adapter launches with). Beyond key and field
//! names, only allowlisted scalar window/scope facts are printed (see
//! `facts`); credential failures print one fixed code.

use agent_run_adapters::authorized_request::CredentialReader;
use agent_run_core::capacity::collectors::QuotaCredentialReader;
use agent_run_domain::CredentialRef;
use serde_json::{json, Value};
use std::{path::PathBuf, str::FromStr, time::Duration};

/// Upper bound on the inspected response body.
const BODY_MAX: usize = 2 * 1024 * 1024;

/// Extracts only the normalized shape facts of one quota payload.
fn shape(body: &str) -> Value {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return json!({"parse": "malformed"});
    };
    let root = value.get("data").unwrap_or(&value);
    let mut keys: Vec<&str> = root
        .as_object()
        .map(|map| map.keys().map(String::as_str).collect())
        .unwrap_or_default();
    keys.sort_unstable();
    let limits = root
        .get("limits")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let mut fields: Vec<&str> = entry
                        .as_object()
                        .map(|map| map.keys().map(String::as_str).collect())
                        .unwrap_or_default();
                    fields.sort_unstable();
                    let kind = entry
                        .get("kind")
                        .or_else(|| entry.get("type"))
                        .and_then(Value::as_str)
                        .unwrap_or("<absent>");
                    json!({"kind": kind, "fields": fields, "facts": facts(entry)})
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({"top_level_keys": keys, "limits": limits})
}

/// Keeps only allowlisted scalar window/scope/number facts of one `limits[]`
/// entry: GLM `unit`/`number`/`percentage`/count consistency/reset horizon,
/// and Anthropic `kind`/`group`/`scope`/`is_active`/`severity`/`percent`.
/// Absolute counts are reduced to a consistency flag; no other field, and
/// nothing from outside `limits[]`, is ever copied.
fn facts(entry: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for key in [
        "unit",
        "number",
        "percentage",
        "percent",
        "group",
        "scope",
        "is_active",
        "severity",
    ] {
        if let Some(value) = entry.get(key).filter(|value| {
            value.is_number()
                || value.is_boolean()
                || value.is_null()
                || value.as_str().is_some_and(|text| text.len() <= 48)
        }) {
            out.insert(key.into(), value.clone());
        }
    }
    if let (Some(total), Some(used)) = (
        entry.get("usage").and_then(Value::as_f64),
        entry.get("currentValue").and_then(Value::as_f64),
    ) {
        let remaining = entry.get("remaining").and_then(Value::as_f64);
        out.insert(
            "counted_percent".into(),
            json!((total > 0.0).then(|| (used * 1000.0 / total).round() / 10.0)),
        );
        out.insert(
            "remaining_consistent".into(),
            json!(remaining.map(|left| (total - used - left).abs() < 1.0)),
        );
    }
    if let Some(ms) = entry.get("nextResetTime").and_then(Value::as_f64) {
        let hours = (ms / 1000.0 - agent_run_core::domain::now()) / 3600.0;
        out.insert(
            "reset_in_hours".into(),
            json!((hours * 10.0).round() / 10.0),
        );
    }
    // A structured scope keeps only its key names and short scalar values.
    if let Some(scope) = entry.get("scope").filter(|scope| !scope.is_null()) {
        out.insert("scope".into(), reduce(scope, 3));
    }
    if let Some(text) = entry.get("resets_at").and_then(Value::as_str) {
        out.insert(
            "resets_at_rfc3339".into(),
            json!(chrono::DateTime::parse_from_rfc3339(text).is_ok()),
        );
    }
    Value::Object(out)
}

/// Reduces a structured scope to key names and short scalars, at most
/// `depth` levels deep; longer strings and deeper nesting become markers.
fn reduce(value: &Value, depth: u8) -> Value {
    match value {
        Value::Bool(_) | Value::Number(_) | Value::Null => value.clone(),
        Value::String(text) if text.len() <= 48 => value.clone(),
        Value::String(_) => json!("<long>"),
        _ if depth == 0 => json!("<structured>"),
        Value::Array(items) => json!(items
            .iter()
            .take(16)
            .map(|item| reduce(item, depth - 1))
            .collect::<Vec<_>>()),
        Value::Object(map) => Value::Object(
            map.iter()
                .take(16)
                .map(|(key, item)| (key.clone(), reduce(item, depth - 1)))
                .collect(),
        ),
    }
}

/// Probes one Codex login through the verified app-server probe and keeps
/// only each bucket's id, short `limitName`, and per-window duration and
/// used percent. `label` selects `$AGENT_RUN_HOME/accounts/codex/<label>`;
/// absent, the host native login (`CODEX_HOME`, else `~/.codex`) is used.
async fn codex(label: Option<&str>) -> Result<Value, String> {
    let home = std::env::var_os("AGENT_RUN_HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "AGENT_RUN_HOME is required".to_owned())?;
    let config: agent_run_core::config::Config =
        toml::from_str("schema_version = 1").map_err(|_| "config unavailable".to_owned())?;
    let runtime: agent_run_core::config::Runtime = serde_json::from_value(json!({
        "enabled": true,
        "adapter": "codex",
        "binary": std::env::var("CODEX_BINARY").unwrap_or_else(|_| "codex".into()),
        "home": home.join("runtimes/codex"),
        "models": ["probe"],
    }))
    .map_err(|_| "runtime unavailable".to_owned())?;
    let value = agent_run_core::capacity::sources::codex_probe(
        &home,
        &config,
        &runtime,
        label,
        "account/rateLimits/read",
    )
    .await
    .map_err(|_| "probe_failed".to_owned())?;
    let raw = value.get("result").unwrap_or(&value);
    let buckets = raw
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .map(|(id, bucket)| {
                    let window = |field: &str| {
                        bucket.get(field).filter(|w| !w.is_null()).map(|w| {
                            json!({
                                "minutes": w.get("windowDurationMins"),
                                "used": w.get("usedPercent"),
                            })
                        })
                    };
                    json!({
                        "id": reduce(&json!(id), 0),
                        "limitName": bucket.get("limitName").map(|name| reduce(name, 0)),
                        "primary": window("primary"),
                        "secondary": window("secondary"),
                    })
                })
                .collect::<Vec<_>>()
        });
    Ok(json!({
        "legacy_single_bucket": raw.get("rateLimits").is_some_and(Value::is_object),
        "buckets": buckets,
    }))
}

/// Runs one canary and returns its normalized report.
#[tokio::main]
async fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let report = match run(&arguments).await {
        Ok(report) => report,
        Err(failure) => json!({"typed_failure": failure}),
    };
    println!("{}", report);
}

/// Resolves the approved credential, performs one GET, normalizes the shape.
async fn run(arguments: &[String]) -> Result<Value, String> {
    let kind = arguments.first().map(String::as_str).unwrap_or("glm");
    if kind == "codex" {
        return codex(arguments.get(1).map(String::as_str)).await;
    }
    let (reference, path) = match kind {
        "glm" => (
            CredentialRef::Keychain {
                service: "com.pluto.agent-run.glm".into(),
                account: "GLM_CODING_KEY".into(),
            },
            "/api/monitor/usage/quota/limit".to_owned(),
        ),
        "anthropic" => (
            CredentialRef::from_str("native:claude-code").map_err(|e| e.to_string())?,
            "/api/oauth/usage".to_owned(),
        ),
        other => return Err(format!("unknown canary kind {other:?}")),
    };
    let origin = arguments
        .get(1)
        .filter(|_| kind == "glm")
        .cloned()
        .unwrap_or_else(|| match kind {
            "glm" => "https://api.z.ai".into(),
            _ => "https://api.anthropic.com".into(),
        });
    let app_home = std::env::var_os("AGENT_RUN_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".agent-run")))
        .ok_or_else(|| "HOME is unavailable".to_owned())?;
    // Only the native login is read here, so the harness home is nominal.
    let runtime_home = app_home.join("runtimes/claude/home");
    let secret = QuotaCredentialReader::from_host(app_home, runtime_home)
        .ok_or_else(|| "HOME is unavailable".to_owned())?
        .read(&reference)
        .map_err(|_| "credential_unavailable".to_owned())?;
    let url = format!("{origin}{path}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "transport unavailable".to_owned())?;
    let mut request = client.get(&url);
    if kind == "glm" {
        request = request.header("authorization", secret);
    } else {
        request = request
            .bearer_auth(secret)
            .header("anthropic-beta", "oauth-2025-04-20");
    }
    let mut response = request
        .send()
        .await
        .map_err(|_| "request_unreachable".to_owned())?;
    let status = response.status().as_u16();
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| (1..=900).contains(seconds));
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "body unreadable".to_owned())?
    {
        if body.len() + chunk.len() > BODY_MAX {
            return Ok(json!({"status": status, "typed_failure": "body_too_large"}));
        }
        body.extend_from_slice(&chunk);
    }
    let body = String::from_utf8_lossy(&body).into_owned();
    Ok(json!({
        "status": status,
        "retry_after_seconds": retry_after,
        "shape": shape(&body),
    }))
}
