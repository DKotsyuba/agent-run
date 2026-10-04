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
    let first = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-one".into(),
        external_turn_id: None,
    };
    let session = store.bind_orchestrator(&agent_id, &first, 1.0).unwrap();
    store
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
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

/// Binding, context receipts and session filters share one canonical row for aliases.
#[test]
fn orchestrator_alias_and_canonical_transport_share_session_identity() {
    let home = common::Home::new();
    let (agent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let alias = OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "physical-session".into(),
        external_turn_id: Some("turn-one".into()),
    };
    let session = store.bind_orchestrator(&agent_id, &alias, 1.0).unwrap();
    let canonical = OrchestratorRef {
        transport: "codex_queue".into(),
        external_turn_id: Some("turn-two".into()),
        ..alias.clone()
    };
    assert_eq!(
        store
            .record_context_receipt_for_ref(&canonical, "context", 2.0)
            .unwrap(),
        (session.clone(), true)
    );
    assert_eq!(
        store.find_orchestrator_session(&alias).unwrap(),
        Some(session)
    );
    let (rows, total) = store.list(false, 0, 100, Some(&canonical)).unwrap();
    assert_eq!((rows.len(), total), (1, 1));
    let transports: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM orchestrator_sessions WHERE external_session_id='physical-session' AND transport='codex_queue'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(transports, 1);

    let unknown = OrchestratorRef {
        transport: "other".into(),
        external_session_id: "rejected-session".into(),
        external_turn_id: None,
    };
    assert_eq!(
        store
            .record_context_receipt_for_ref(&unknown, "context", 3.0)
            .unwrap_err()
            .machine_code(),
        agent_run_domain::error::MachineCode::ValidationError
    );
    let rejected: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM orchestrator_sessions WHERE external_session_id='rejected-session'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rejected, 0);
}

/// A canonical context lookup reuses an exact legacy alias session without changing its row.
#[test]
fn canonical_context_reuses_legacy_alias_session_row() {
    let home = common::Home::new();
    let mut store = home.store();
    store
        .conn
        .execute(
            "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('os-legacy','codex','old-session',1,1)",
            [],
        )
        .unwrap();
    let canonical = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "old-session".into(),
        external_turn_id: None,
    };
    assert_eq!(
        store
            .record_context_receipt_for_ref(&canonical, "context", 2.0)
            .unwrap(),
        ("os-legacy".into(), true)
    );
    assert_eq!(
        store.find_orchestrator_session(&canonical).unwrap(),
        Some("os-legacy".into())
    );
    let rows: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM orchestrator_sessions WHERE external_session_id='old-session'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 1);
}

/// A canonical session filter includes agents on both known historical session rows.
#[test]
fn session_list_filter_includes_split_canonical_and_alias_rows() {
    let home = common::Home::new();
    let mut store = home.store();
    let (canonical_agent, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let (legacy_agent, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let canonical = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "split-chat".into(),
        external_turn_id: None,
    };
    let canonical_session = store
        .bind_orchestrator(&canonical_agent, &canonical, 1.0)
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('os-split-legacy','codex','split-chat',1,1)",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET orchestrator_session_id='os-split-legacy' WHERE id=?",
            [legacy_agent.as_str()],
        )
        .unwrap();

    let (rows, total) = store.list(false, 0, 100, Some(&canonical)).unwrap();
    assert_eq!(total, 2);
    assert!(rows.iter().any(|row| row.id == canonical_agent));
    assert!(rows.iter().any(|row| row.id == legacy_agent));
    assert_ne!(canonical_session, "os-split-legacy");
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
