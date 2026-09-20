//! Credential fallback regressions using injected Keychain lookups only.

use agent_run_adapters::auth::{glm_environment_with, GLM_BASE_URL};
use std::collections::BTreeMap;

/// GLM prefers its managed Keychain credential over an inherited Anthropic token.
#[test]
fn glm_keychain_precedes_ambient_token() {
    let host = BTreeMap::from([("ANTHROPIC_AUTH_TOKEN".into(), "ambient".into())]);
    let environment = glm_environment_with(&host, || Some("keychain".into())).unwrap();
    assert_eq!(
        environment.get("ANTHROPIC_AUTH_TOKEN"),
        Some(&"keychain".into())
    );
    assert_eq!(
        environment.get("ANTHROPIC_BASE_URL"),
        Some(&GLM_BASE_URL.into())
    );
}

/// A missing GLM credential reports Python's source-oriented error without exposing values.
#[test]
fn glm_missing_credential_has_no_value_in_error() {
    let error = glm_environment_with(&BTreeMap::new(), || None).unwrap_err();
    assert!(error.to_string().contains("com.pluto.agent-run.glm"));
}

/// Exercises the real macOS Keychain command only when an operator opts in on macOS.
#[test]
#[ignore]
#[cfg(target_os = "macos")]
fn macos_keychain_smoke() {
    let _ =
        agent_run_platform::keychain::generic_password("GLM_CODING_KEY", "com.pluto.agent-run.glm");
}
