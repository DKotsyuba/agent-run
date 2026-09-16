//! Python `tests/test_doctor.py` and `tests/test_cli.py` init parity.

use agent_run::doctor;
use std::{
    fs,
    os::unix::fs::{symlink, MetadataExt, PermissionsExt},
};

/// Mirrors `CliTests.test_init_bootstraps_private_minimal_home_without_credentials`.
#[test]
fn python_init_bootstraps_a_private_minimal_home_idempotently() {
    let temp = tempfile::tempdir().expect("temporary parent");
    let home = temp.path().join("fresh");
    let output = agent_run::init::initialize(&home).expect("initialize home");
    assert_eq!(output["home"], home.display().to_string());
    assert_eq!(
        fs::read_to_string(home.join("config.toml")).unwrap(),
        "schema_version = 1\n"
    );
    assert_eq!(
        fs::metadata(&home).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(home.join("config.toml"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(home.join("state.db"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let inode = fs::metadata(home.join("config.toml")).unwrap().ino();
    agent_run::init::initialize(&home).expect("idempotent initialization");
    assert_eq!(fs::metadata(home.join("config.toml")).unwrap().ino(), inode);
}

/// Mirrors the unsafe-config refusal in Python `_initialize`.
#[test]
fn python_init_refuses_a_symlinked_config() {
    let temp = tempfile::tempdir().expect("temporary home");
    let target = temp.path().join("target.toml");
    fs::write(&target, "schema_version = 1\n").unwrap();
    symlink(&target, temp.path().join("config.toml")).unwrap();
    let error = agent_run::init::initialize(temp.path()).expect_err("unsafe config is refused");
    assert_eq!(error.to_string(), "config.toml must not be a symlink");
}

/// Mirrors `DoctorTests.test_plaintext_secret_is_named_but_never_returned`.
#[test]
fn python_doctor_names_plaintext_secret_without_its_value() {
    let temp = tempfile::tempdir().expect("temporary home");
    fs::write(
        temp.path().join("config.toml"),
        "schema_version = 1\napi_key = \"must-not-leak\"\n",
    )
    .unwrap();
    let report = doctor::run(temp.path()).expect("doctor report");
    assert!(report
        .findings
        .iter()
        .any(|finding| finding.code == "plaintext_secret_config"));
    assert!(!serde_json::to_string(&report)
        .unwrap()
        .contains("must-not-leak"));
}

/// Mirrors `DoctorTests.test_reports_bounded_metadata_without_mutating_state`.
#[test]
fn python_doctor_reports_missing_static_runtime_artifacts() {
    let temp = tempfile::tempdir().expect("temporary home");
    let home = temp.path();
    let missing = home.join("missing");
    fs::write(
        home.join("config.toml"),
        format!(
            "schema_version = 1\n[mcp.missing]\ntransport = \"stdio\"\ncommand = {missing}\n[runtimes.codex]\nenabled = true\nadapter = \"codex\"\nbinary = {missing}\nhome = {missing}\nmodels = [\"model\"]\nskills = [\"review\"]\n[[runtimes.codex.hooks]]\nevent = \"PostToolUse\"\ncommand = [\"relative-hook\"]\n[runtimes.codex.auth]\nkind = \"file_link\"\nsource = {missing}\ntarget = \"auth.json\"\n",
            missing = toml::Value::String(missing.display().to_string())
        ),
    )
    .unwrap();
    let store = agent_run::state::Store::initialize(home).expect("initialize state");
    drop(store);
    let report = doctor::run(home).expect("doctor report");
    let codes = report
        .findings
        .iter()
        .map(|finding| finding.code.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for code in [
        "mcp_executable_missing",
        "runtime_binary_missing",
        "profile_directory_missing",
        "runtime_skill_missing",
        "hook_executable_missing",
        "hook_untrusted",
        "auth_source_missing",
        "auth_bridge_missing",
        "mcp_inventory_self",
    ] {
        assert!(codes.contains(code), "missing {code}: {codes:?}");
    }
    assert!(!report.ok());
}
