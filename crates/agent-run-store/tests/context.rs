//! Store ports of Python binding, receipt, and expiry contracts.
//!
//! These tests use SQLite temporary homes only; none require Unix sockets.

mod common;

use agent_run_domain::domain::{OrchestratorRef, Outcome};
use serde_json::json;
use std::collections::BTreeMap;

/// Mirrors Python `test_bind_hook.py::test_binding_is_immutable_empty_then_same_target_but_never_another`.
#[test]
fn python_test_bind_hook_double_bind_activates_once_and_refuses_another_session() {
    let home = common::Home::new();
    let (agent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let first = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-one".into(),
        external_turn_id: None,
    };
    let session = store.bind_orchestrator(&agent_id, &first, 1.0).unwrap();
    assert_eq!(
        store.bind_orchestrator(&agent_id, &first, 2.0).unwrap(),
        session
    );
    assert_eq!(
        store.delivery_status(&agent_id).unwrap()["state"],
        "pending"
    );
    let other = OrchestratorRef {
        external_session_id: "session-two".into(),
        ..first
    };
    assert!(store.bind_orchestrator(&agent_id, &other, 3.0).is_err());
}

/// Mirrors Python `test_state_outbox.py` context receipt deduplication used by `test_context_hook.py`.
#[test]
fn python_test_context_hook_receipts_are_changed_only_and_session_scoped() {
    let home = common::Home::new();
    let mut store = home.store();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-one".into(),
        external_turn_id: Some("turn-one".into()),
    };
    let components = BTreeMap::from([
        ("priority".into(), "priority-hash".into()),
        ("active".into(), "active-hash".into()),
    ]);
    let (session, changed) = store
        .record_context_components_for_ref(&reference, &components, 1.0)
        .unwrap();
    assert_eq!(changed, vec!["active", "priority"]);
    assert_eq!(
        store
            .record_context_components_for_ref(&reference, &components, 2.0)
            .unwrap(),
        (session.clone(), Vec::new())
    );
    let changed = BTreeMap::from([("active".into(), "next-active-hash".into())]);
    assert_eq!(
        store
            .record_context_components_for_ref(&reference, &changed, 3.0)
            .unwrap(),
        (session, vec!["active".into()])
    );
}
