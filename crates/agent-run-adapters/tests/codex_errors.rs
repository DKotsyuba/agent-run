//! Acceptance evidence for Codex roster parsing and bounded error categories.

use agent_run_adapters::codex::{models::parse_roster, session::failure_kind};
use serde_json::json;

/// Mirrors `tests/test_codex_adapter.py::CodexAdapterTests::test_models_parses_current_model_list_shape`
#[test]
fn current_model_list_shape_preserves_description_and_efforts() {
    let models = parse_roster(&json!({
        "models": [{
            "model": "gpt-5.6-sol",
            "description": "current",
            "supportedReasoningEfforts": [
                {"reasoningEffort": "minimal", "description": "Minimal"},
                {"reasoningEffort": "high", "description": "High"}
            ]
        }]
    }))
    .unwrap();
    assert_eq!(models[0].id, "gpt-5.6-sol");
    assert_eq!(models[0].description, "current");
    assert_eq!(models[0].efforts, ["minimal", "high"]);
}

/// Mirrors `tests/test_codex_adapter.py::CodexAdapterTests::test_models_normalizes_real_cache_and_keeps_present_cache_strict`
// The malformed-entry cases also protect the Rust parser's fail-soft cache contract; no single Python test isolates those entries.
#[test]
fn real_cache_shape_deduplicates_efforts_and_ignores_malformed_entries() {
    let models = parse_roster(&json!({
        "data": [
            {"slug": "gpt-5.6-sol", "description": "real cache", "supported_reasoning_levels": [
                {"effort": "low"}, {"effort": "high"}, {"effort": "high"}
            ]},
            {"description": "missing id"},
            "not an object"
        ]
    }))
    .unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].id, "gpt-5.6-sol");
    assert_eq!(models[0].description, "real cache");
    assert_eq!(models[0].efforts, ["low", "high"]);
}

/// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_structured_provider_errors_preserve_precedence_and_overload`
#[test]
fn structured_provider_errors_keep_precedence_and_bound_diagnostics() {
    assert_eq!(
        failure_kind(&json!({"codexErrorInfo": "serverOverloaded"})),
        Some("provider_overloaded".into())
    );
    assert_eq!(
        failure_kind(&json!({
            "kind": "kind_first",
            "code": "code_second",
            "codexErrorInfo": "serverOverloaded"
        })),
        Some("kind_first".into())
    );
    let kind = failure_kind(&json!({
        "codexErrorInfo": "future/Provider Code\n".repeat(8)
    }))
    .unwrap();
    assert!(kind.starts_with("codex_future_Provider_Code"));
    assert!(kind.len() <= 64);
    assert!(!kind.contains('\n'));
}
