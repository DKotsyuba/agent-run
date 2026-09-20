//! Credential resolution shared by adapters that use managed macOS Keychain items.

use agent_run_domain::{error::invalid, Result};
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};

/// GLM's fixed Anthropic-protocol endpoint.
pub const GLM_BASE_URL: &str = "https://api.z.ai/api/anthropic";
/// Account name of the managed GLM Coding Plan Keychain item.
pub const GLM_ACCOUNT: &str = "GLM_CODING_KEY";
/// Service name of the managed GLM Coding Plan Keychain item.
pub const GLM_SERVICE: &str = "com.pluto.agent-run.glm";
/// Last successful GLM Keychain lookup; failed lookups remain uncached.
static GLM_KEY_CACHE: Mutex<Option<(Instant, String)>> = Mutex::new(None);

/// Reads GLM's Keychain item, retaining only successful lookups for five minutes.
fn glm_keychain_value() -> Option<String> {
    cached_glm_key(&GLM_KEY_CACHE, Instant::now(), || {
        agent_run_platform::keychain::generic_password(GLM_ACCOUNT, GLM_SERVICE)
    })
}

/// Resolve one serialized GLM cache lookup, retaining successful values for five minutes.
///
/// The mutex is held across the lookup so concurrent misses share one refresh;
/// failed lookups clear the cache and successful values replace stale entries.
fn cached_glm_key(
    cache: &Mutex<Option<(Instant, String)>>,
    now: Instant,
    lookup: impl FnOnce() -> Option<String>,
) -> Option<String> {
    let mut cache = cache.lock().ok()?;
    if let Some((_at, value)) = cache
        .as_ref()
        .filter(|(at, _)| now.duration_since(*at) < Duration::from_secs(300))
    {
        return Some(value.clone());
    }
    let value = lookup().filter(|value| !value.is_empty());
    *cache = value.clone().map(|value| (now, value));
    value
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
    let token = lookup().filter(|value| !value.is_empty()).or_else(|| {
        host.get("ANTHROPIC_AUTH_TOKEN")
            .filter(|v| !v.is_empty())
            .cloned()
    });
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

/// Report GLM credential presence without consulting Keychain when an export exists.
pub fn glm_authenticated_with(
    host: &BTreeMap<String, String>,
    lookup: impl FnOnce() -> Option<String>,
) -> bool {
    host.get("ANTHROPIC_AUTH_TOKEN")
        .is_some_and(|value| !value.is_empty())
        || lookup().is_some_and(|value| !value.is_empty())
}

/// Report GLM credential presence using the managed Keychain fallback.
pub fn glm_authenticated(host: &BTreeMap<String, String>) -> bool {
    glm_authenticated_with(host, glm_keychain_value)
}

/// Clear the successful GLM Keychain result so rotation/provisioning is visible immediately.
pub fn reset_glm_keychain_cache() {
    if let Ok(mut cache) = GLM_KEY_CACHE.lock() {
        *cache = None;
    }
}

/// Resolves GLM credentials through the platform-owned macOS Keychain helper.
pub fn glm_environment(host: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    glm_environment_with(host, glm_keychain_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::thread;

    /// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_keychain_cache_retries_misses_reuses_success_rotates_and_resets`.
    #[test]
    fn keychain_cache_retries_misses_reuses_success_rotates_and_resets() {
        let cache = Mutex::new(None);
        let start = Instant::now();
        let values = [
            None,
            Some("synthetic-first".into()),
            Some("synthetic-rotated".into()),
        ];
        let mut calls = 0;
        assert_eq!(cached_glm_key(&cache, start, || values[0].clone()), None);
        calls += 1;
        assert_eq!(
            cached_glm_key(&cache, start + Duration::from_secs(1), || values[1].clone()),
            Some("synthetic-first".into())
        );
        calls += 1;
        assert_eq!(
            cached_glm_key(&cache, start + Duration::from_secs(2), || values[2].clone()),
            Some("synthetic-first".into())
        );
        assert_eq!(calls, 2);
        assert_eq!(
            cached_glm_key(&cache, start + Duration::from_secs(302), || values[2]
                .clone()),
            Some("synthetic-rotated".into())
        );
        *cache.lock().unwrap() = None;
        assert_eq!(
            cached_glm_key(&cache, start + Duration::from_secs(303), || {
                Some("synthetic-after-reset".into())
            }),
            Some("synthetic-after-reset".into())
        );
    }

    /// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_concurrent_keychain_reads_share_one_successful_refresh`.
    #[test]
    fn concurrent_keychain_reads_share_one_successful_refresh() {
        let cache = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            workers.push(thread::spawn(move || {
                cached_glm_key(&cache, Instant::now(), || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some("synthetic-shared".into())
                })
            }));
        }
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            results,
            vec![
                Some("synthetic-shared".into()),
                Some("synthetic-shared".into())
            ]
        );
    }
}
