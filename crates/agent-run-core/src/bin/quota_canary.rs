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
//! Usage: `quota_canary <glm|anthropic> [origin]`
//! `glm` resolves the managed Keychain item (`com.pluto.agent-run.glm`,
//! account `GLM_CODING_KEY`) exactly as the GLM adapter does; `anthropic`
//! resolves the Claude harness's own OAuth store under
//! `$claude_home` (default `~/.agent-run/runtimes/claude/home`).

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
                    json!({"kind": kind, "fields": fields})
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({"top_level_keys": keys, "limits": limits})
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
    let (reference, path, claude_home) = match kind {
        "glm" => (
            CredentialRef::Keychain {
                service: "com.pluto.agent-run.glm".into(),
                account: "GLM_CODING_KEY".into(),
            },
            "/api/monitor/usage/quota/limit".to_owned(),
            PathBuf::from("/nonexistent"),
        ),
        "anthropic" => (
            CredentialRef::from_str("native:claude-code").map_err(|e| e.to_string())?,
            "/api/oauth/usage".to_owned(),
            arguments
                .get(1)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/Users/pluto/.agent-run/runtimes/claude/home")),
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
    let secret = QuotaCredentialReader::new(claude_home)
        .read(&reference)
        .map_err(|error| error.to_string().chars().take(96).collect::<String>())?;
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
