//! Python `tests/test_doctor.py` and `tests/test_cli.py` init parity.

use agent_run::doctor;
use std::{
    fs,
    os::unix::fs::{symlink, MetadataExt, PermissionsExt},
};

/// Mirrors `test_cli.py::test_init_bootstraps_private_minimal_home_without_credentials`.
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

/// Mirrors `test_doctor.py::test_plaintext_secret_is_named_but_never_returned`.
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

/// Mirrors `test_doctor.py::test_reports_bounded_metadata_without_mutating_state`.
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

/// Mirrors `test_doctor.py::test_canonical_role_readiness_uses_shared_catalog_without_runtime_home`.
#[test]
fn python_doctor_validates_canonical_roles_from_shared_catalogs() {
    let temp = tempfile::tempdir().expect("temporary home");
    let home = temp.path();
    let profiles = home.join("profiles");
    let skills = home.join("skills");
    fs::create_dir_all(skills.join("code-reading")).unwrap();
    fs::write(skills.join("code-reading/SKILL.md"), "Read code.\n").unwrap();
    fs::create_dir_all(&profiles).unwrap();
    fs::write(
        profiles.join("review.md"),
        "+++\nrevision = \"1\"\nwrite = false\nnetwork = false\nallow_external_read_roots = true\nskills = [\"code-reading\"]\nmcp = [\"echo\"]\nrequired_constraints = []\n+++\nReview.\n",
    )
    .unwrap();
    fs::write(
        home.join("config.toml"),
        format!(
            "schema_version = 1\n[profiles]\ndirectory = {profiles}\n[skills]\ndirectory = {skills}\n[mcp.echo]\ntransport = \"stdio\"\ncommand = \"/bin/echo\"\n[runtimes.fake]\nenabled = true\nadapter = \"claude\"\nbinary = \"/bin/echo\"\nhome = {runtime_home}\nmodels = [\"model\"]\n",
            profiles = toml::Value::String(profiles.display().to_string()),
            skills = toml::Value::String(skills.display().to_string()),
            runtime_home = toml::Value::String(home.join("unmaterialized").display().to_string()),
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
    assert!(!codes.contains("role_invalid"), "{codes:?}");
    assert!(!codes.contains("mixed_role_assets"), "{codes:?}");
}

/// Mirrors `test_doctor.py::test_canary_handshake_ok_reports_a_completed_real_handshake`.
#[test]
fn python_doctor_cli_canary_proves_the_production_ready_handshake() {
    let temp = tempfile::tempdir().expect("temporary home");
    let home = temp.path();
    agent_run::init::initialize(home).expect("initialize healthy home");
    fs::create_dir_all(home.join("profiles")).expect("empty healthy profile catalog");
    let binary = env!("CARGO_BIN_EXE_agent-run");
    let output = std::process::Command::new(binary)
        .args(["--home", home.to_str().unwrap(), "doctor"])
        .output()
        .expect("doctor CLI starts");
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON report");
    assert_eq!(
        report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .find(|finding| finding["component"] == "canary")
            .and_then(|finding| finding["code"].as_str()),
        Some("supervisor_canary_ok")
    );
}

/// Mirrors Python CLI doctor exit behavior for a malformed configuration.
#[test]
fn python_doctor_cli_broken_config_exits_two_with_config_invalid() {
    let temp = tempfile::tempdir().expect("temporary home");
    let home = temp.path();
    fs::write(home.join("config.toml"), "schema_version = \"bad\"\n").unwrap();
    let binary = env!("CARGO_BIN_EXE_agent-run");
    let output = std::process::Command::new(binary)
        .args(["--home", home.to_str().unwrap(), "doctor"])
        .output()
        .expect("doctor CLI starts");
    assert_eq!(output.status.code(), Some(2), "{:?}", output);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON report");
    assert_eq!(report["findings"][0]["code"], "config_invalid");
}
