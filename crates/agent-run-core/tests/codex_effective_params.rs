//! App-server echo verification coverage using only in-memory protocol frames.

use agent_run_core::codex::Grant;
use serde_json::{json, Value};

/// Returns the baseline read-only grant used by the app-server echo tests.
fn read_only_grant() -> Grant {
    Grant {
        model: "gpt-5.6-sol".into(),
        cwd: "/work".into(),
        roots: vec!["/work".into()],
        writable_roots: vec![],
        sandbox: "read-only".into(),
        approval_policy: "never".into(),
        reviewer: None,
        network_access: false,
        permission_profile: None,
    }
}

/// Returns an app-server thread echo, with caller-provided compatibility fields.
fn echo(sandbox: Value, roots: Value, writable_roots: Value) -> Value {
    json!({
        "model": "gpt-5.6-sol",
        "cwd": "/work",
        "roots": roots,
        "writableRoots": writable_roots,
        "sandbox": sandbox,
        "approvalPolicy": "never",
        "threadId": "th_1",
    })
}

/// Mirrors `test_codex_app_server.py::test_matching_params_pass`.
#[test]
fn python_test_codex_app_server_matching_params_pass() {
    assert!(read_only_grant()
        .verify(&echo(json!("read-only"), json!(["/work"]), json!([])))
        .is_ok());
}

/// Mirrors `test_codex_app_server.py::test_read_root_leaking_into_writable_roots_is_refused`.
#[test]
fn python_test_codex_app_server_read_root_leaking_into_writable_roots_is_refused() {
    let mut grant = read_only_grant();
    grant.roots.push("/extra".into());
    assert!(grant
        .verify(&echo(
            json!("read-only"),
            json!(["/work", "/extra"]),
            json!(["/extra"]),
        ))
        .is_err());
}

/// Mirrors `test_codex_app_server.py::test_sandbox_mismatch_is_refused`.
#[test]
fn python_test_codex_app_server_sandbox_mismatch_is_refused() {
    assert!(read_only_grant()
        .verify(&echo(json!("workspace-write"), json!(["/work"]), json!([]),))
        .is_err());
}

/// Mirrors `test_codex_app_server.py::test_named_permission_profile_must_match_exactly`.
#[test]
fn python_test_codex_app_server_named_permission_profile_must_match_exactly() {
    let mut grant = read_only_grant();
    grant.permission_profile = Some("Projects".into());
    let mut actual = echo(json!("read-only"), json!(["/work"]), json!([]));
    actual["activePermissionProfile"] = json!({"id": "Projects", "extends": ":workspace"});
    assert!(grant.verify(&actual).is_ok());
    actual["activePermissionProfile"] = json!({"id": ":workspace"});
    assert!(grant.verify(&actual).is_err());
}

/// Mirrors `test_codex_app_server.py::test_sandbox_beta_object_echo_matching_type_passes`.
#[test]
fn python_test_codex_app_server_sandbox_beta_object_echo_matching_type_passes() {
    assert!(read_only_grant()
        .verify(&echo(
            json!({"type": "readOnly", "networkAccess": false}),
            json!(["/work"]),
            json!([]),
        ))
        .is_ok());
}

/// Mirrors `test_codex_app_server.py::test_requested_network_access_must_be_echoed`.
#[test]
fn python_test_codex_app_server_requested_network_access_must_be_echoed() {
    let mut grant = read_only_grant();
    grant.network_access = true;
    let mut actual = echo(
        json!({"type": "readOnly", "networkAccess": true}),
        json!(["/work"]),
        json!([]),
    );
    assert!(grant.verify(&actual).is_ok());
    actual["sandbox"]["networkAccess"] = json!(false);
    assert!(grant.verify(&actual).is_err());
}

/// Mirrors `test_codex_app_server.py::test_sandbox_legacy_string_echo_still_passes`.
#[test]
fn python_test_codex_app_server_sandbox_legacy_string_echo_still_passes() {
    assert!(read_only_grant()
        .verify(&echo(json!("read-only"), json!(["/work"]), json!([])))
        .is_ok());
}

/// Mirrors `test_codex_app_server.py::test_sandbox_beta_object_echo_mismatched_type_is_refused`.
#[test]
fn python_test_codex_app_server_sandbox_beta_object_echo_mismatched_type_is_refused() {
    assert!(read_only_grant()
        .verify(&echo(
            json!({"type": "workspaceWrite", "networkAccess": true}),
            json!(["/work"]),
            json!([]),
        ))
        .is_err());
}

/// Mirrors `test_codex_app_server.py::test_sandbox_beta_object_echo_unknown_type_is_refused`.
#[test]
fn python_test_codex_app_server_sandbox_beta_object_echo_unknown_type_is_refused() {
    assert!(read_only_grant()
        .verify(&echo(
            json!({"type": "somethingNew", "networkAccess": false}),
            json!(["/work"]),
            json!([]),
        ))
        .is_err());
}

/// Mirrors `test_codex_app_server.py::test_live_read_only_echo_shape_passes`.
#[test]
fn python_test_codex_app_server_live_read_only_echo_shape_passes() {
    let mut grant = read_only_grant();
    grant.model = "gpt-5.6-luna".into();
    grant.cwd = "/Users/pluto/projects/agent-run".into();
    grant.roots = vec![grant.cwd.clone()];
    let actual = json!({
        "thread": {"id": "thread"},
        "model": "gpt-5.6-luna",
        "cwd": grant.cwd,
        "runtimeWorkspaceRoots": ["/Users/pluto/projects/agent-run"],
        "approvalPolicy": "never",
        "sandbox": {"type": "readOnly", "networkAccess": false},
    });
    assert!(grant.verify(&actual).is_ok());
}

/// Mirrors `test_codex_app_server.py::test_live_workspace_write_echo_shape_passes`.
#[test]
fn python_test_codex_app_server_live_workspace_write_echo_shape_passes() {
    let cwd = "/Users/pluto/projects/agent-run";
    let grant = Grant {
        model: "gpt-5.6-luna".into(),
        cwd: cwd.into(),
        roots: vec![cwd.into()],
        writable_roots: vec![cwd.into()],
        sandbox: "workspace-write".into(),
        approval_policy: "never".into(),
        reviewer: None,
        network_access: false,
        permission_profile: None,
    };
    assert!(grant
        .verify(&json!({
            "thread": {"id": "thread"},
            "model": "gpt-5.6-luna",
            "cwd": cwd,
            "runtimeWorkspaceRoots": [cwd],
            "approvalPolicy": "never",
            "sandbox": {"type": "workspaceWrite", "writableRoots": [], "networkAccess": false},
        }))
        .is_ok());
}

/// Mirrors `test_codex_app_server.py::test_beta_roots_key_missing_entirely_is_refused`.
#[test]
fn python_test_codex_app_server_beta_roots_key_missing_entirely_is_refused() {
    let mut actual = echo(
        json!({"type": "readOnly", "networkAccess": false}),
        json!(["/work"]),
        json!([]),
    );
    actual.as_object_mut().unwrap().remove("roots");
    assert!(read_only_grant().verify(&actual).is_err());
}

/// Mirrors `test_codex_app_server.py::test_beta_writable_roots_unexpected_entry_is_reported_verbatim`.
#[test]
fn python_test_codex_app_server_beta_writable_roots_unexpected_entry_is_reported_verbatim() {
    let mut grant = read_only_grant();
    grant.sandbox = "workspace-write".into();
    grant.writable_roots = vec!["/work".into()];
    let actual = json!({
        "model": "gpt-5.6-sol", "cwd": "/work", "runtimeWorkspaceRoots": ["/work"],
        "approvalPolicy": "never",
        "sandbox": {"type": "workspaceWrite", "writableRoots": ["/other"], "networkAccess": false}
    });
    assert!(grant.verify(&actual).is_err());
}
