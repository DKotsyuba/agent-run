//! GLM adapter and Claude-family launch contract regressions.

use agent_run_adapters::{
    auth::{glm_authenticated_with, glm_environment_with, GLM_ACCOUNT, GLM_BASE_URL, GLM_SERVICE},
    capabilities,
    claude::validate_runtime,
    glm::cli_model,
    redact::Redactor,
    validate,
};
use agent_run_config::{
    config::{Auth, Hook, Runtime},
    profiles::Profile,
};
use agent_run_domain::domain::StartRequest;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

/// Build a minimal GLM runtime for validation-only tests.
fn runtime(auth: Option<Auth>) -> Runtime {
    let mut runtime: Runtime = serde_json::from_value(json!({
        "enabled": true,
        "adapter": "glm",
        "binary": "/usr/bin/true",
        "home": "/tmp/glm-home",
        "models": ["glm-5.3"],
    }))
    .expect("runtime");
    runtime.auth = auth;
    runtime
}

/// Build a request and role accepted by adapter-level validation.
fn validation_inputs() -> (StartRequest, Profile) {
    let request: StartRequest = serde_json::from_value(json!({
        "runtime": "glm",
        "model": "glm-5.3",
        "profile": "review",
        "task": "fixture",
        "workdir": "/tmp",
    }))
    .expect("request");
    let profile = Profile {
        name: "review".into(),
        body: "Review carefully.".into(),
        write: false,
        network: false,
        revision: "legacy".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    (request, profile)
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_auth_module_exposes_the_keychain_contract`.
#[test]
fn auth_module_exposes_the_keychain_contract() {
    assert_eq!(GLM_SERVICE, "com.pluto.agent-run.glm");
    assert_eq!(GLM_ACCOUNT, "GLM_CODING_KEY");
    assert_eq!(GLM_BASE_URL, "https://api.z.ai/api/anthropic");
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_describe_names_glm_with_claude_capabilities`.
#[test]
fn describe_names_glm_with_claude_capabilities() {
    assert_eq!(
        capabilities(agent_run_config::config::Adapter::Glm),
        capabilities(agent_run_config::config::Adapter::Claude)
    );
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_environment_token_is_the_fallback_when_keychain_is_empty`.
#[test]
fn environment_token_is_the_fallback_when_keychain_is_empty() {
    let host = BTreeMap::from([("ANTHROPIC_AUTH_TOKEN".into(), "sk-exported".into())]);
    let environment = glm_environment_with(&host, || None).expect("environment");
    assert_eq!(environment["ANTHROPIC_AUTH_TOKEN"], "sk-exported");
    assert_eq!(environment["ANTHROPIC_BASE_URL"], GLM_BASE_URL);
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_injected_token_is_registered_and_redacted_for_sparse_auth_names`.
#[test]
fn injected_token_is_redacted_for_declared_sparse_auth_names() {
    let secret = "SYNTHETIC_GLM_TOKEN_XYZ";
    let environment = BTreeMap::from([
        ("ANTHROPIC_AUTH_TOKEN".into(), secret.into()),
        ("ANTHROPIC_BASE_URL".into(), GLM_BASE_URL.into()),
    ]);
    let text = format!("{secret} {}", environment["ANTHROPIC_BASE_URL"]);
    let empty = Redactor::from_environment_with_secret_names(&environment, &BTreeSet::new());
    assert_eq!(empty.redact(&text), format!("<redacted> {GLM_BASE_URL}"));
    let declared = BTreeSet::from(["ANTHROPIC_BASE_URL".into()]);
    let redactor = Redactor::from_environment_with_secret_names(&environment, &declared);
    assert_eq!(redactor.redact(&text), "<redacted> <redacted>");
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_keychain_wins_over_process_environment`.
#[test]
fn keychain_wins_over_process_environment() {
    let host = BTreeMap::from([("ANTHROPIC_AUTH_TOKEN".into(), "sk-exported".into())]);
    let environment = glm_environment_with(&host, || Some("sk-keychain".into())).unwrap();
    assert_eq!(environment["ANTHROPIC_AUTH_TOKEN"], "sk-keychain");
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_million_context_models_carry_the_1m_suffix`.
#[test]
fn million_context_models_carry_the_1m_suffix() {
    assert_eq!(cli_model("glm-5.3"), "glm-5.3[1m]");
    assert_eq!(cli_model("glm-5.3-flash"), "glm-5.3-flash[1m]");
    assert_eq!(cli_model("glm-5-turbo"), "glm-5-turbo");
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_prepare_consults_keychain_when_environment_is_empty`.
#[test]
fn prepare_consults_keychain_when_environment_is_empty() {
    let mut consulted = false;
    let environment = glm_environment_with(&BTreeMap::new(), || {
        consulted = true;
        Some("sk-keychain".into())
    })
    .unwrap();
    assert!(consulted);
    assert_eq!(environment["ANTHROPIC_AUTH_TOKEN"], "sk-keychain");
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_prepare_falls_back_to_default_base_url`.
#[test]
fn prepare_falls_back_to_default_base_url() {
    let environment = glm_environment_with(
        &BTreeMap::from([("ANTHROPIC_AUTH_TOKEN".into(), "sk-test-token".into())]),
        || None,
    )
    .unwrap();
    assert_eq!(environment["ANTHROPIC_BASE_URL"], GLM_BASE_URL);
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_prepare_ignores_inherited_environment`.
#[test]
fn prepare_ignores_inherited_environment() {
    let host = BTreeMap::from([
        ("ANTHROPIC_AUTH_TOKEN".into(), "sk-orchestrator".into()),
        (
            "ANTHROPIC_BASE_URL".into(),
            "https://api.anthropic.com".into(),
        ),
    ]);
    let environment = glm_environment_with(&host, || Some("sk-plan-key".into())).unwrap();
    assert_eq!(environment["ANTHROPIC_AUTH_TOKEN"], "sk-plan-key");
    assert_eq!(environment["ANTHROPIC_BASE_URL"], GLM_BASE_URL);
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_prepare_restores_process_environment`.
#[test]
fn prepare_does_not_mutate_process_environment() {
    let host = BTreeMap::from([("PATH".into(), "/usr/bin".into())]);
    let before = host.clone();
    let _ = glm_environment_with(&host, || Some("sk-keychain".into())).unwrap();
    assert_eq!(host, before);
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_prepare_without_token_anywhere_is_a_validation_error`.
#[test]
fn prepare_without_token_anywhere_is_a_validation_error() {
    let error = glm_environment_with(&BTreeMap::new(), || None).unwrap_err();
    assert!(error.to_string().contains("com.pluto.agent-run.glm"));
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_probe_reports_credential_presence_without_network`.
#[test]
fn probe_reports_credential_presence_without_network() {
    assert!(!glm_authenticated_with(&BTreeMap::new(), || None));
    assert!(glm_authenticated_with(&BTreeMap::new(), || Some(
        "sk-keychain".into()
    )));
    let host = BTreeMap::from([("ANTHROPIC_AUTH_TOKEN".into(), "sk-exported".into())]);
    assert!(glm_authenticated_with(&host, || panic!(
        "Keychain must not be consulted"
    )));
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_validate_accepts_the_glm_auth_names`.
#[test]
fn validate_accepts_the_glm_auth_names() {
    let (request, profile) = validation_inputs();
    validate(&request, &runtime(None), &profile).unwrap();
    validate(
        &request,
        &runtime(Some(Auth::Environment {
            names: vec!["ANTHROPIC_AUTH_TOKEN".into()],
        })),
        &profile,
    )
    .unwrap();
    validate(
        &request,
        &runtime(Some(Auth::Environment {
            names: vec!["ANTHROPIC_AUTH_TOKEN".into(), "ANTHROPIC_BASE_URL".into()],
        })),
        &profile,
    )
    .unwrap();
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_validate_keeps_claude_hook_semantics`.
#[test]
fn validate_keeps_claude_hook_semantics() {
    let mut runtime = runtime(None);
    runtime.hooks.push(Hook {
        event: "BogusEvent".into(),
        command: vec!["echo".into()],
        matcher: None,
    });
    assert!(validate_runtime(&runtime, agent_run_config::config::Adapter::Glm).is_err());
    runtime.hooks[0].event = "PostToolUse".into();
    validate_runtime(&runtime, agent_run_config::config::Adapter::Glm).unwrap();
}

/// Mirrors `tests/test_glm_adapter.py::GlmAdapterTests::test_validate_rejects_foreign_auth_names_and_kinds`.
#[test]
fn validate_rejects_foreign_auth_names_and_kinds() {
    let (request, profile) = validation_inputs();
    assert!(validate(
        &request,
        &runtime(Some(Auth::FileLink {
            source: PathBuf::from("/tmp/auth"),
            target: "auth.json".into()
        })),
        &profile,
    )
    .is_err());
    for name in ["ROGUE_VAR", "CLAUDE_CODE_OAUTH_TOKEN"] {
        assert!(validate(
            &request,
            &runtime(Some(Auth::Environment {
                names: vec![name.into()]
            })),
            &profile,
        )
        .is_err());
    }
}
