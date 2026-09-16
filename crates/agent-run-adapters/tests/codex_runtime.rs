//! Python-parity tests for Codex account selection and roster cache behavior.

use agent_run_adapters::{
    codex::models::{cache_is_fresh, parse_roster, read_cache, write_cache},
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
}
