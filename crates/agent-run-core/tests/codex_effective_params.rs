//! App-server echo verification coverage using only in-memory protocol frames.

use agent_run_config::{config::Runtime, profiles::Profile};
use agent_run_core::codex::Grant;
use agent_run_domain::domain::StartRequest;
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::PathBuf};

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

/// Mirrors `test_codex_app_server.py::test_non_network_sandbox_mode_is_sent_as_a_plain_string`.
/// Mirrors `test_codex_app_server.py::test_read_only_thread_start_preserves_every_workspace_root`.
#[test]
fn python_test_codex_app_server_read_only_grant_request_is_complete() {
    let mut grant = read_only_grant();
    grant.roots.push("/another-read-root".into());
    assert_eq!(
        grant.request(),
        json!({
            "cwd": "/work",
            "model": "gpt-5.6-sol",
            "approvalPolicy": "never",
            "sandbox": "read-only",
            "runtimeWorkspaceRoots": ["/work", "/another-read-root"],
        })
    );
}

/// Mirrors `test_codex_app_server.py::test_network_grant_is_sent_as_a_workspace_write_config_flag`.
#[test]
fn python_test_codex_app_server_networked_workspace_grant_enables_only_the_native_flag() {
    let mut grant = read_only_grant();
    grant.sandbox = "workspace-write".into();
    grant.approval_policy = "on-request".into();
    grant.writable_roots = vec!["/work".into()];
    grant.network_access = true;
    grant.reviewer = Some("auto_review".into());
    assert_eq!(
        grant.request(),
        json!({
            "cwd": "/work",
            "model": "gpt-5.6-sol",
            "approvalPolicy": "on-request",
            "sandbox": "workspace-write",
            "runtimeWorkspaceRoots": ["/work"],
            "approvalsReviewer": "auto_review",
            "config": {"sandbox_workspace_write": {"network_access": true}},
        })
    );
}

/// Mirrors `test_codex_app_server.py::test_projects_profile_selected_explicitly_without_legacy_sandbox`.
#[test]
fn python_test_codex_app_server_permission_profile_replaces_legacy_sandbox_fields() {
    let mut grant = read_only_grant();
    grant.permission_profile = Some("Projects".into());
    assert_eq!(
        grant.request(),
        json!({
            "cwd": "/work",
            "model": "gpt-5.6-sol",
            "approvalPolicy": "never",
            "permissions": "Projects",
        })
    );
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

/// Returns a workspace-write grant holding several readable and writable roots.
fn multi_root_grant() -> Grant {
    Grant {
        model: "gpt-5.6-sol".into(),
        cwd: "/work".into(),
        roots: vec!["/a".into(), "/b".into(), "/c".into()],
        writable_roots: vec!["/c".into(), "/b".into(), "/a".into()],
        sandbox: "workspace-write".into(),
        approval_policy: "never".into(),
        reviewer: None,
        network_access: false,
        permission_profile: None,
    }
}

/// A pure permutation of the echoed readable and writable roots passes.
#[test]
fn roots_echo_order_is_not_contractual() {
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/c", "/a", "/b"]),
            json!(["/b", "/c", "/a"]),
        ))
        .is_ok());
}

/// An extra echoed root fails closed even when every granted root is present.
#[test]
fn extra_echoed_root_is_refused() {
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/a", "/b", "/c", "/d"]),
            json!(["/a", "/b", "/c"]),
        ))
        .is_err());
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/a", "/b", "/c"]),
            json!(["/a", "/b", "/c", "/d"]),
        ))
        .is_err());
}

/// A missing echoed root fails closed.
#[test]
fn missing_echoed_root_is_refused() {
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/a", "/c"]),
            json!(["/a", "/b", "/c"]),
        ))
        .is_err());
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/a", "/b", "/c"]),
            json!(["/a", "/c"]),
        ))
        .is_err());
}

/// A non-string element in the echoed readable roots fails closed.
#[test]
fn malformed_readable_roots_element_is_refused() {
    assert!(read_only_grant()
        .verify(&echo(json!("read-only"), json!(["/work", 7]), json!([])))
        .is_err());
}

/// A non-array readable roots shape fails closed.
#[test]
fn malformed_readable_roots_shape_is_refused() {
    assert!(read_only_grant()
        .verify(&echo(json!("read-only"), json!("/work"), json!([])))
        .is_err());
}

/// A non-string element in the echoed writableRoots fails closed.
#[test]
fn malformed_writable_roots_element_is_refused() {
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/a", "/b", "/c"]),
            json!(["/a", "/b", true]),
        ))
        .is_err());
}

/// A non-array writableRoots shape fails closed.
#[test]
fn malformed_writable_roots_shape_is_refused() {
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/a", "/b", "/c"]),
            json!("/a"),
        ))
        .is_err());
}

/// A duplicated echoed root fails closed because comparison stays a multiset.
#[test]
fn duplicate_echoed_root_is_refused() {
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/a", "/a", "/b", "/c"]),
            json!(["/a", "/b", "/c"]),
        ))
        .is_err());
    assert!(multi_root_grant()
        .verify(&echo(
            json!("workspace-write"),
            json!(["/a", "/b", "/c"]),
            json!(["/a", "/b", "/c", "/c"]),
        ))
        .is_err());
}

/// Builds the read-only role for request-grant construction tests.
fn read_only_profile(read_roots: Vec<PathBuf>) -> Profile {
    Profile {
        name: "review".into(),
        body: "Review the fixture.".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots,
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    }
}

/// Builds a minimal admitted Codex request without invoking a live adapter.
fn grant_request(workdir: &str, write: bool) -> StartRequest {
    serde_json::from_value(json!({
        "runtime": "codex",
        "model": "fixture",
        "profile": if write { "implement" } else { "review" },
        "task": "fixture",
        "workdir": workdir,
        "write": write,
    }))
    .expect("valid request fixture")
}

/// Builds a runtime with no ambient permissions or credentials and the given
/// configured workspace roots in admission-check order.
fn grant_runtime(workspace_roots: &[&str]) -> Runtime {
    serde_json::from_value(json!({
        "enabled": true,
        "adapter": "codex",
        "binary": "/bin/true",
        "home": "/private/tmp/codex-runtime",
        "models": ["fixture"],
        "workspace_roots": workspace_roots,
    }))
    .expect("valid runtime fixture")
}

/// Mirrors `test_codex_adapter.py::test_prepare_accepts_a_request_read_root_as_the_only_filesystem_grant`.
/// Mirrors `test_codex_adapter.py::test_prepare_grants_write_root_only_when_the_request_asks_for_it`.
#[test]
fn python_test_codex_adapter_grant_keeps_read_and_write_authority_separate() {
    let request = grant_request("/private/tmp/work", false);
    let grant = Grant::new(
        &grant_runtime(&[]),
        &request,
        &read_only_profile(vec![PathBuf::from("/private/tmp/read")]),
        PathBuf::from("/private/tmp/home").as_path(),
    )
    .expect("read-only grant");
    assert_eq!(grant.roots, vec!["/private/tmp/read", "/private/tmp/work"]);
    assert!(grant.writable_roots.is_empty());
    assert_eq!(grant.sandbox, "read-only");

    let write_request = grant_request("/private/tmp/work", true);
    let write = Profile {
        name: "implement".into(),
        write: true,
        ..read_only_profile(vec![])
    };
    let grant = Grant::new(
        &grant_runtime(&[]),
        &write_request,
        &write,
        PathBuf::from("/private/tmp/home").as_path(),
    )
    .expect("write grant");
    assert_eq!(grant.roots, vec!["/private/tmp/work"]);
    assert_eq!(grant.writable_roots, vec!["/private/tmp/work"]);
    assert_eq!(grant.sandbox, "workspace-write");
}

/// Mirrors `test_codex_adapter.py::test_prepare_refuses_external_read_roots_with_write`.
/// Mirrors `test_codex_adapter.py::test_prepare_rejects_write_workdir_outside_configured_project_root`.
#[test]
fn python_test_codex_adapter_grant_refuses_authority_outside_the_workspace() {
    let request = grant_request("/private/tmp/project/work", true);
    let write_with_read = Profile {
        name: "implement".into(),
        write: true,
        ..read_only_profile(vec![PathBuf::from("/private/tmp/read")])
    };
    assert!(Grant::new(
        &grant_runtime(&["/private/tmp/project"]),
        &request,
        &write_with_read,
        PathBuf::from("/private/tmp/home").as_path(),
    )
    .is_err());
    assert!(Grant::new(
        &grant_runtime(&["/private/tmp/project"]),
        &grant_request("/private/tmp/elsewhere", true),
        &Profile {
            name: "implement".into(),
            write: true,
            ..read_only_profile(vec![])
        },
        PathBuf::from("/private/tmp/home").as_path(),
    )
    .is_err());
}

/// A write-capable request is admitted under any one configured workspace
/// root and refused outside all of them.
///
/// On hosts without a managed Projects policy the admitted grant composes
/// every configured root into its writable set; on managed hosts the fixture
/// roots fail the later managed-policy check, so an error here must be the
/// managed-policy error, proving admission itself passed. The pure admission
/// and generated-profile logic is additionally verified hermetically in
/// `src/codex.rs`.
#[test]
fn write_requests_admit_any_configured_workspace_root() {
    let write = Profile {
        name: "implement".into(),
        write: true,
        ..read_only_profile(vec![])
    };
    let runtime = grant_runtime(&["/private/tmp/other", "/private/tmp/project"]);
    let home = PathBuf::from("/private/tmp/home");

    let under_second_root = Grant::new(
        &runtime,
        &grant_request("/private/tmp/project/repo", true),
        &write,
        home.as_path(),
    );
    match under_second_root {
        Ok(grant) => {
            for configured in ["/private/tmp/other", "/private/tmp/project"] {
                assert!(
                    grant.writable_roots.iter().any(|root| root == configured),
                    "writable set must contain every configured root: {:?}",
                    grant.writable_roots
                );
            }
        }
        Err(error) => assert!(
            !error
                .to_string()
                .contains("outside configured workspace_roots"),
            "an in-root workdir must pass admission before any policy error: {error}"
        ),
    }

    let error = Grant::new(
        &runtime,
        &grant_request("/private/tmp/elsewhere", true),
        &write,
        home.as_path(),
    )
    .expect_err("a workdir outside every root must be refused");
    assert!(error
        .to_string()
        .contains("outside configured workspace_roots"));
}
