//! Credential resolution shared by adapters that use managed macOS Keychain items.

use agent_run_domain::{error::invalid, Result};
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

/// GLM's fixed Anthropic-protocol endpoint.
pub const GLM_BASE_URL: &str = "https://api.z.ai/api/anthropic";
const GLM_ACCOUNT: &str = "GLM_CODING_KEY";
const GLM_SERVICE: &str = "com.pluto.agent-run.glm";
const QWEN_ACCOUNT: &str = "OMNIROUTE_API_KEY";
const QWEN_SERVICE: &str = "com.pluto.agent-run.opencode.omniroute";
/// Qwen's local OpenAI-compatible router used when no endpoint is exported.
pub const QWEN_BASE_URL: &str = "http://127.0.0.1:20128/v1";
/// Last successful GLM Keychain lookup; failed lookups remain uncached.
static GLM_KEY_CACHE: Mutex<Option<(Instant, String)>> = Mutex::new(None);
/// Qwen's process-lifetime Keychain lookup, including a missing item.
static QWEN_KEY_CACHE: OnceLock<Option<String>> = OnceLock::new();

/// Reads GLM's Keychain item, retaining only successful lookups for five minutes.
fn glm_keychain_value() -> Option<String> {
    let mut cache = GLM_KEY_CACHE.lock().ok()?;
    if let Some((_, value)) = cache
        .as_ref()
        .filter(|(at, _)| at.elapsed() < Duration::from_secs(300))
    {
        return Some(value.clone());
    }
    let value = agent_run_platform::keychain::generic_password(GLM_ACCOUNT, GLM_SERVICE);
    *cache = value.clone().map(|value| (Instant::now(), value));
    value
}

/// Reads Qwen's Keychain item once per process, including an unavailable item.
fn qwen_keychain_value() -> Option<String> {
    QWEN_KEY_CACHE
        .get_or_init(|| agent_run_platform::keychain::generic_password(QWEN_ACCOUNT, QWEN_SERVICE))
        .clone()
}

/// Resolves GLM credentials with Keychain precedence over the host environment.
///
/// The closure exists so callers can use the platform Keychain helper while
/// tests prove precedence without reading a real credential.  The returned
/// map contains only the live process environment and is never serialized.
pub fn glm_environment_with(
    host: &BTreeMap<String, String>,
    lookup: impl FnOnce() -> Option<String>,
) -> Result<BTreeMap<String, String>> {
    let token = lookup().or_else(|| host.get("ANTHROPIC_AUTH_TOKEN").cloned());
    let token = token.ok_or_else(|| {
        invalid(
            "glm requires the macOS Keychain entry 'com.pluto.agent-run.glm' (account GLM_CODING_KEY) or ANTHROPIC_AUTH_TOKEN in the process environment",
        )
    })?;
    Ok(BTreeMap::from([
        ("ANTHROPIC_AUTH_TOKEN".into(), token),
        ("ANTHROPIC_BASE_URL".into(), GLM_BASE_URL.into()),
    ]))
}

/// Resolves GLM credentials through the platform-owned macOS Keychain helper.
pub fn glm_environment(host: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    glm_environment_with(host, glm_keychain_value)
}

/// Resolves one Qwen authentication value, preserving Python's lookup order.
///
/// Exported nonblank host values win; only `OPENAI_API_KEY` falls back to the
/// managed Keychain item and only `OPENAI_BASE_URL` falls back to the local
/// router.  Other names have no implicit source.
pub fn qwen_auth_value_with(
    name: &str,
    host: &BTreeMap<String, String>,
    lookup: impl FnOnce() -> Option<String>,
) -> Option<String> {
    host.get(name)
        .filter(|value| !value.is_empty())
        .cloned()
        .or_else(|| match name {
            "OPENAI_API_KEY" => lookup(),
            "OPENAI_BASE_URL" => Some(QWEN_BASE_URL.into()),
            _ => None,
        })
}

/// Resolves Qwen authentication through the platform-owned macOS Keychain helper.
pub fn qwen_auth_value(name: &str, host: &BTreeMap<String, String>) -> Option<String> {
    qwen_auth_value_with(name, host, qwen_keychain_value)
}
