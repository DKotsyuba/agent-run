//! Acceptance evidence for account auth references and private paths.

use agent_run_config::{
    config::{self, Config},
    profiles::Profile,
    role_plan::resolve_role_plan,
};
use agent_run_platform::paths::{
    agent_dir, agent_run_home, config_path, runtime_skills_dir, state_db_path,
};
use serde_json::json;
use std::{collections::BTreeSet, fs, path::PathBuf};

/// Build a minimal validated runtime configuration with scoped accounts.
fn account_config(root: &std::path::Path, adapter: &str) -> Config {
    let binary = if PathBuf::from("/usr/bin/true").is_file() {
        "/usr/bin/true"
    } else {
        "/bin/true"
    };
    let text = format!(
        "schema_version=1\n[runtimes.main]\nenabled=true\nadapter={adapter:?}\nbinary={binary:?}\nhome={:?}\nmodels=['fixture']\naccounts=['personal2','work_1']\ndefault_account='personal2'\n",
        root.join("runtime")
    );
    fs::write(root.join("config.toml"), text).unwrap();
    Config::load(root).unwrap()
}

/// Mirrors `tests/test_config.py::ConfigTests::test_accounts_parse_and_validate`
/// Mirrors `tests/test_config.py::ConfigTests::test_accounts_require_supported_adapter_and_legacy_default_is_declared`
#[test]
fn account_declarations_validate_for_supported_adapters() {
    let root = tempfile::tempdir().unwrap();
    let config = account_config(root.path(), "codex");
    let runtime = config.runtime("main").unwrap();
    assert_eq!(runtime.accounts, ["personal2", "work_1"]);
    assert_eq!(runtime.default_account.as_deref(), Some("personal2"));
    assert_eq!(runtime.selected_account(None).unwrap(), None);
    assert_eq!(
        runtime.selected_account(Some("work_1")).unwrap(),
        Some("work_1".into())
    );
    assert!(runtime.selected_account(Some("missing")).is_err());
    assert!(config::account("personal2"));
    assert!(!config::account("../escape"));

    let mut invalid = account_config(root.path(), "codex");
    invalid.runtimes.get_mut("main").unwrap().accounts = vec!["personal".into()];
    invalid.runtimes.get_mut("main").unwrap().adapter = "glm".into();
    assert!(invalid.validate(root.path()).is_err());
    invalid.runtimes.get_mut("main").unwrap().adapter = "codex".into();
    invalid.runtimes.get_mut("main").unwrap().default_account = Some("missing".into());
    assert!(invalid.validate(root.path()).is_err());
}

/// Mirrors `tests/test_role_plan.py::ResolvedRolePlanTests::test_resolves_serializable_revisioned_role`
#[test]
fn role_plan_serializes_global_and_account_auth_choices() {
    let profile = Profile {
        name: "review".into(),
        body: "Review.".into(),
        write: false,
        network: false,
        revision: "3".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    let account = resolve_role_plan(
        &profile,
        PathBuf::from("/tmp/agent-run-skills").as_path(),
        &Default::default(),
        "account",
        Some("personal2"),
    )
    .unwrap();
    assert_eq!(
        account.to_payload()["auth"],
        json!({"mode":"account","reference":"personal2"})
    );
    let global = resolve_role_plan(
        &profile,
        PathBuf::from("/tmp/agent-run-skills").as_path(),
        &Default::default(),
        "global",
        None,
    )
    .unwrap();
    assert_eq!(
        global.to_payload()["auth"],
        json!({"mode":"global","reference":null})
    );
    assert!(resolve_role_plan(
        &profile,
        PathBuf::from("/tmp/agent-run-skills").as_path(),
        &Default::default(),
        "global",
        Some("personal2"),
    )
    .is_err());
}

/// Mirrors `tests/test_paths.py::PathTests::test_home_uses_environment_and_returns_resolved_paths`
/// Mirrors `tests/test_paths.py::PathTests::test_agent_directory_rejects_traversal_id`
/// Mirrors `tests/test_paths.py::PathTests::test_agent_directory_cannot_escape_through_symlink`
#[test]
fn safe_paths_stay_beneath_the_private_home() {
    let root = tempfile::tempdir().unwrap();
    let home = agent_run_home(Some(root.path().to_path_buf())).unwrap();
    assert_eq!(
        config_path(Some(root.path().to_path_buf())).unwrap(),
        home.join("config.toml")
    );
    assert_eq!(
        state_db_path(Some(root.path().to_path_buf())).unwrap(),
        home.join("state.db")
    );
    assert_eq!(
        runtime_skills_dir("codex", Some(root.path().to_path_buf())).unwrap(),
        home.join("skills/codex")
    );
    let id = "ag-20260825-010203-0123456789";
    assert_eq!(
        agent_dir(id, Some(root.path().to_path_buf())).unwrap(),
        home.join("agents").join(id)
    );
    assert!(agent_dir("../outside", Some(root.path().to_path_buf())).is_err());

    let outside = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("agents")).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("agents").join(id)).unwrap();
    assert!(agent_dir(id, Some(root.path().to_path_buf())).is_err());
}
