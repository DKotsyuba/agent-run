//! Python command-policy refusal and native-rendering ports.

use agent_run_adapters::command_policy;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, process::Command};
use tempfile::TempDir;

/// Create one executable shell fixture with the exact refusal-test mode.
fn executable(path: &std::path::Path, body: &str) {
    std::fs::write(path, format!("#!/bin/sh\n{body}\n")).expect("fixture executable");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).expect("mode");
}

/// Mirrors `tests/test_command_policy.py::CommandPolicyTest::test_refusal_shadows_only_denied_normal_invocation`
#[test]
fn refusal_shadows_only_denied_normal_invocation() {
    let root = TempDir::new().expect("temporary root");
    let host = root.path().join("host");
    std::fs::create_dir(&host).expect("host");
    executable(&host.join("gh"), "exit 0");
    executable(&host.join("git"), "printf allowed");
    let policy = command_policy::materialize_refusal_commands(
        &["gh".into()],
        &root.path().join("policy"),
        std::slice::from_ref(&host),
    )
    .expect("policy");
    let output = Command::new("gh")
        .env_clear()
        .env(
            "PATH",
            format!("{}:{}", policy.directory.display(), host.display()),
        )
        .output()
        .expect("denied command");
    assert_eq!(output.status.code(), Some(126));
    assert!(String::from_utf8_lossy(&output.stderr).contains("denied by owner policy"));
    let output = Command::new("git")
        .env_clear()
        .env(
            "PATH",
            format!("{}:{}", policy.directory.display(), host.display()),
        )
        .output()
        .expect("allowed command");
    assert_eq!(output.stdout, b"allowed");
    assert_eq!(policy.resolved_commands["gh"], host.join("gh"));
}

/// Mirrors `tests/test_command_policy.py::CommandPolicyTest::test_refresh_removes_only_unchanged_managed_entries`
#[test]
fn refresh_removes_only_unchanged_managed_entries() {
    let root = TempDir::new().expect("temporary root");
    let policy = root.path().join("policy");
    command_policy::materialize_refusal_commands(&["gh".into(), "hub".into()], &policy, &[])
        .expect("initial policy");
    command_policy::materialize_refusal_commands(&["hub".into()], &policy, &[]).expect("refresh");
    assert!(!policy.join("gh").exists());
    assert!(policy.join("hub").is_file());
    std::fs::write(policy.join("hub"), "user file").expect("user file");
    assert!(command_policy::materialize_refusal_commands(&[], &policy, &[]).is_err());
}

/// Mirrors `tests/test_command_policy.py::CommandPolicyTest::test_rejects_traversal_and_never_follows_symlinks`
#[test]
fn rejects_traversal_and_never_follows_symlinks() {
    let root = TempDir::new().expect("temporary root");
    assert!(command_policy::validate_denied_commands(&["../gh".into()]).is_err());
    let policy = root.path().join("policy");
    command_policy::materialize_refusal_commands(&["gh".into()], &policy, &[]).expect("policy");
    std::fs::remove_file(policy.join("gh")).expect("remove shim");
    std::os::unix::fs::symlink(root.path().join("target"), policy.join("gh")).expect("link");
    assert!(command_policy::materialize_refusal_commands(&["gh".into()], &policy, &[]).is_err());
}

/// Mirrors `tests/test_command_policy.py::CommandPolicyTest::test_marker_symlinks_are_rejected_before_refresh`
#[test]
fn marker_symlinks_are_rejected_before_refresh() {
    let root = TempDir::new().expect("temporary root");
    let policy = root.path().join("policy");
    command_policy::materialize_refusal_commands(&["gh".into()], &policy, &[]).expect("policy");
    let marker = policy.join(".agent-run-command-policy.json");
    let target = root.path().join("foreign-marker.json");
    let bytes = std::fs::read(&marker).expect("marker");
    std::fs::write(&target, &bytes).expect("foreign marker");
    std::fs::remove_file(&marker).expect("remove marker");
    std::os::unix::fs::symlink(&target, &marker).expect("marker link");
    assert!(command_policy::materialize_refusal_commands(&["hub".into()], &policy, &[]).is_err());
    assert_eq!(std::fs::read(&target).expect("target"), bytes);
    assert!(policy.join("gh").is_file());
    assert!(!policy.join("hub").exists());
}

/// Mirrors `tests/test_command_policy.py::CommandPolicyTest::test_native_rules_include_executable_symlink_and_target`
#[test]
fn native_rules_include_executable_symlink_and_target() {
    let root = TempDir::new().expect("temporary root");
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).expect("bin");
    let target = root.path().join("actual-gh");
    executable(&target, "exit 0");
    std::os::unix::fs::symlink(&target, bin.join("gh")).expect("executable link");
    let policy = command_policy::materialize_refusal_commands(
        &["gh".into()],
        &root.path().join("policy"),
        std::slice::from_ref(&bin),
    )
    .expect("policy");
    let rules = command_policy::render_codex_denial_rules(
        &["gh".into()],
        &[policy.resolved_commands["gh"].clone()],
    )
    .expect("rules");
    assert!(rules.contains(&bin.join("gh").display().to_string()));
    assert!(rules.contains(&target.display().to_string()));
}

/// Mirrors `tests/test_command_policy.py::CommandPolicyTest::test_codex_review_rules_cover_bare_and_resolved_commands`
#[test]
fn codex_review_rules_cover_bare_and_resolved_commands() {
    let root = TempDir::new().expect("temporary root");
    let executable_path = root.path().join("curl");
    executable(&executable_path, "exit 0");
    let environment = BTreeMap::from([(String::from("PATH"), root.path().display().to_string())]);
    let rules =
        command_policy::render_codex_review_rules(&["curl".into()], &environment).expect("rules");
    assert!(rules.contains("pattern=[\"curl\"]"));
    assert!(rules.contains(&executable_path.display().to_string()));
    assert!(rules.contains("decision=\"prompt\""));
}
