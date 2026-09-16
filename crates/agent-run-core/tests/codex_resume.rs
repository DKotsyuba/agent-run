//! Python-parity tests for native Codex continuation identity and account homes.

mod common;

use agent_run_core::{
    codex::runtime_home,
    service::{LaunchIdentity, Service},
};
use agent_run_domain::{domain::Outcome, Error};
use serde_json::json;

/// Mirrors `accounts.py::account_runtime_home` for global and labelled accounts.
#[test]
fn python_test_codex_account_runtime_home_is_scoped_only_for_labels() {
    let home = common::Home::new();
    let id = "ag-20260101-000010-0000000001".parse().unwrap();
    assert_eq!(
        runtime_home(&home.path.join("codex"), None, &id).unwrap(),
        home.path.join("codex/runs").join(id.as_str())
    );
    assert_eq!(
        runtime_home(&home.path.join("codex"), Some("work"), &id).unwrap(),
        home.path.join("codex@work/runs").join(id.as_str())
    );
}

/// Mirrors Python `tests/test_resume.py::ResumeTests::test_legacy_row_without_an_identity_snapshot_is_refused`.
///
/// Python's identity object cannot prove Rust's frozen config, role grant, or
/// sealed runtime-home snapshot, so native continuation reports `Unsupported`
/// instead of silently replaying it under current configuration.
#[tokio::test]
async fn python_test_python_created_codex_run_is_explicitly_refused() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.runtime_session(&id, "thread-python").unwrap();
    store
        .update_identity(
            &id,
            &json!({
                "runtime":"codex",
                "account":null,
                "home":"/python/runtime-home",
                "auth_target":"auth.json",
                "profile":"review",
                "write":false,
                "read_roots":[],
                "fast":false
            }),
            "python-snapshot",
        )
        .unwrap();
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let row = store.get(&id).unwrap();
    assert!(matches!(
        LaunchIdentity::read(&row),
        Err(Error::Unsupported(_))
    ));
    let error = Service::new(home.path.clone())
        .resume(&id, "continue".into(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(message) if message.contains("Python-created")));
}

/// Mirrors Python `tests/test_resume.py::ResumeTests::test_parent_without_a_native_session_is_refused`.
///
/// Refuse a terminal durable parent before interpreting its identity when it
/// lacks the session selector required for a native continuation.
#[tokio::test]
async fn python_test_resume_without_native_session_is_refused() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    home.store()
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();

    let error = Service::new(home.path.clone())
        .resume(&id, "continue".into(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Validation(message) if message.contains("native session ID")));
}
