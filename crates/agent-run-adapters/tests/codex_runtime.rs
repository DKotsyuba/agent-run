//! Python-parity tests for Codex account selection and roster cache behavior.

use agent_run_adapters::{
    codex::models::{
        cache_is_fresh, parse_roster, read_cache, validate_cached_selection, write_cache,
    },
    materialize::account_home,
};
use agent_run_config::config::{Adapter, Runtime};
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};

/// Builds the smallest valid runtime shape needed for account selection.
fn runtime() -> Runtime {
    Runtime {
        enabled: true,
        adapter: "codex".into(),
        binary: PathBuf::from("/bin/true"),
        home: PathBuf::from("/tmp/codex"),
        models: vec!["fixture".into()],
        skills: vec![],
        mcp: vec![],
        max_active_agents: None,
        auth: None,
        hooks: vec![],
        plugins: vec![],
        limits_source: None,
        accounts: vec!["work".into()],
        default_account: Some("work".into()),
        priority_multiplier: 1.0,
        priority_account_multipliers: BTreeMap::new(),
        priority_lane_multipliers: BTreeMap::new(),
        rust: None,
        environment: None,
        plugin_snapshot_assets: BTreeMap::new(),
        workspace_root: None,
        workspace_network: false,
        native_settings: BTreeMap::new(),
    }
}

/// Mirrors `test_resume.py::test_unlabelled_base_account_stays_resumable_after_default_changes`.
#[test]
fn python_test_omitted_codex_account_means_native_global_account() {
    let runtime = runtime();
    assert_eq!(runtime.selected_account(None).unwrap(), None);
    assert_eq!(
        runtime.selected_account(Some("work")).unwrap().as_deref(),
        Some("work")
    );
    assert!(runtime.selected_account(Some("other")).is_err());
}

/// Mirrors `accounts.py::account_runtime_home` and `account_auth_source` layout.
#[test]
fn python_test_labelled_account_uses_private_account_credential_layout() {
    let app_home = tempfile::tempdir().unwrap();
    assert_eq!(
        account_home(app_home.path(), Adapter::Codex, "work").join("auth.json"),
        app_home.path().join("accounts/codex/work/auth.json")
    );
}

/// Mirrors `test_codex_adapter.py` model-cache parsing and malformed-cache behavior.
#[test]
fn python_test_codex_roster_cache_preserves_supported_efforts_and_fails_closed() {
    let temporary = tempfile::tempdir().unwrap();
    let roster = vec![json!({
        "slug":"gpt-fixture",
        "description":"fixture model",
        "supportedReasoningEfforts":["low", {"reasoningEffort":"high"}, "low"]
    })];
    write_cache(temporary.path(), &roster).unwrap();
    assert!(cache_is_fresh(temporary.path(), SystemTime::now()));
    assert_eq!(
        read_cache(temporary.path()).unwrap(),
        parse_roster(&json!({"models":roster})).unwrap()
    );
    assert!(
        parse_roster(&json!({"models":[{"description":"missing id"}]}))
            .unwrap()
            .is_empty()
    );
    assert!(validate_cached_selection(temporary.path(), "gpt-fixture", Some("medium")).is_err());
    assert!(validate_cached_selection(temporary.path(), "not-configured", None).is_err());
}

/// Mirrors `test_codex_adapter.py::test_models_parses_current_model_list_shape`.
#[test]
fn python_test_codex_adapter_models_parses_current_model_list_shape() {
    assert_eq!(
        parse_roster(&json!({"data": [{
            "id": "gpt-5.6-sol",
            "description": "current shape",
            "supported_reasoning_efforts": [{"reasoning_effort": "low"}, "high"]
        }]}))
        .unwrap(),
        vec![agent_run_adapters::codex::models::Model {
            id: "gpt-5.6-sol".into(),
            description: "current shape".into(),
            efforts: vec!["low".into(), "high".into()],
        }]
    );
}

/// Mirrors `test_codex_adapter.py::test_models_normalizes_real_cache_and_keeps_present_cache_strict`.
#[test]
fn python_test_codex_adapter_models_normalizes_real_cache_and_keeps_present_cache_strict() {
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(home.path().join("cache")).unwrap();
    std::fs::write(
        home.path().join("cache/models.json"),
        r#"{"models":[{"slug":"gpt-5.6-sol","supportedReasoningEfforts":["low","low",{"effort":"high"}]}]}"#,
    )
    .unwrap();
    assert_eq!(
        read_cache(home.path()).unwrap()[0].efforts,
        vec!["low".to_owned(), "high".to_owned()]
    );
    assert!(validate_cached_selection(home.path(), "gpt-5.6-sol", Some("medium")).is_err());
}

/// Mirrors `test_codex_adapter.py::test_models_without_cache_falls_back_to_config_only`.
#[test]
fn python_test_codex_adapter_models_without_cache_falls_back_to_config_only() {
    let home = tempfile::tempdir().unwrap();
    assert_eq!(read_cache(home.path()), None);
    assert!(validate_cached_selection(home.path(), "configured-model", Some("high")).is_ok());
}

/// Mirrors `test_codex_adapter.py::test_models_with_unreadable_cache_falls_back_to_config_only`.
#[test]
fn python_test_codex_adapter_models_with_unreadable_cache_falls_back_to_config_only() {
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(home.path().join("cache")).unwrap();
    std::fs::write(home.path().join("cache/models.json"), b"not JSON").unwrap();
    assert_eq!(read_cache(home.path()), None);
    assert!(validate_cached_selection(home.path(), "configured-model", Some("high")).is_ok());
}
