//! Store ports of Python terminal/outbox durability tests.

mod common;

use agent_run_domain::domain::{OrchestratorRef, Outcome, Status};
use agent_run_platform::{fs, verify};
use serde_json::json;
use std::path::Path;

/// Mirrors `test_state_store.py::test_terminal_update_rolls_back_when_delivery_activation_fails`.
///
/// A delivery-insert fault rolls back every terminal fact.
#[test]
fn python_test_state_store_terminal_transition_is_atomic() {
    let home = common::Home::new();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "fixture-session".into(),
        external_turn_id: None,
    });
    let (id, _) = home
        .store()
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.running(&id, 42).unwrap();
    let before_events: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    store
        .conn
        .execute_batch("CREATE TRIGGER abort_terminal_delivery BEFORE INSERT ON deliveries BEGIN SELECT RAISE(ABORT, 'injected terminal delivery failure'); END;")
        .unwrap();
    assert!(store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .is_err());
    assert_eq!(store.get(&id).unwrap().status.as_str(), "running");
    let after_events: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_events, before_events);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [id.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// Mirrors Python `test_state_store.py::test_pending_cancel_atomically_overrides_successful_terminal_commit`.
///
/// A pending cancellation wins an otherwise successful terminal commit in the same transaction.
#[test]
fn python_test_state_store_terminal_success_loses_to_pending_cancel_atomically() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.running(&id, 42).unwrap();
    let root = home.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "fixture answer").unwrap();
    store.enqueue(&id, "cancel", &json!({})).unwrap();
    store
        .finish(&id, &Outcome::success(None), Some(&proof), None)
        .unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Cancelled);
    let (state, result): (String, String) = store
        .conn
        .query_row(
            "SELECT state,result_json FROM commands WHERE agent_id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "completed");
    assert_eq!(result, r#"{"accepted":true,"reason":"terminal_cancel"}"#);
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_final_drain_completes_late_cancel_steer_and_unknown`.
///
/// Two durable cancels are terminalized independently: the first wins the
/// terminal transition and the duplicate receives the stable stopping result.
#[test]
fn duplicate_cancel_is_completed_without_breaking_the_terminal_fsm() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.running(&id, 42).unwrap();
    let root = home.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "fixture answer").unwrap();
    store.enqueue(&id, "cancel", &json!({})).unwrap();
    store.enqueue(&id, "cancel", &json!({})).unwrap();
    store
        .finish(&id, &Outcome::success(None), Some(&proof), None)
        .unwrap();
    agent_run_core::commands::complete_terminal(&mut store, &id).unwrap();

    assert_eq!(store.get(&id).unwrap().status, Status::Cancelled);
    let results: Vec<(String, String)> = store
        .conn
        .prepare("SELECT state,result_json FROM commands WHERE agent_id=? ORDER BY id")
        .unwrap()
        .query_map([id.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(state, _)| state == "completed"));
    assert!(results
        .iter()
        .any(|(_, result)| result == r#"{"accepted":true,"reason":"terminal_cancel"}"#));
    assert!(results
        .iter()
        .any(|(_, result)| result == r#"{"accepted":true,"reason":"already_stopping"}"#));
}

/// Mirrors `tests/test_state_outbox.py::test_terminal_before_binding_activates_once_and_expired_lease_reclaims_once`.
#[test]
fn python_test_state_outbox_waiting_binding_activates_and_expires() {
    let home = common::Home::new();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "fixture-session".into(),
        external_turn_id: None,
    });
    let (id, _) = home
        .store()
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let (delivery, event_at): (String, f64) = store
        .conn
        .query_row(
            "SELECT d.id,e.at FROM deliveries d JOIN events e ON e.seq=d.terminal_event_seq WHERE d.agent_id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let first = store
        .claim_delivery("worker-1", event_at, 10.0)
        .unwrap()
        .unwrap();
    assert_eq!(first["id"], delivery);
    assert_eq!(first["attempts"], 1);
    assert!(store
        .claim_delivery("worker-2", event_at + 9.0, 10.0)
        .unwrap()
        .is_none());
    assert!(store
        .complete_delivery(&delivery, "worker-1", event_at + 10.0, None, false, None)
        .is_err());
    let reclaimed = store
        .claim_delivery("worker-2", event_at + 10.0, 10.0)
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed["id"], delivery);
    assert_eq!(reclaimed["attempts"], 2);
    assert!(store
        .claim_delivery("worker-3", event_at + 10.0, 10.0)
        .unwrap()
        .is_none());
    assert!(store
        .complete_delivery(&delivery, "worker-1", event_at + 11.0, None, false, None)
        .is_err());
    store
        .complete_delivery(
            &delivery,
            "worker-2",
            event_at + 11.0,
            Some("remote-1"),
            false,
            None,
        )
        .unwrap();
    assert_eq!(store.delivery_status(&id).unwrap()["state"], "delivered");
}

/// Mirrors `test_state_outbox.py::test_delivery_attempt_evidence_is_immutable_and_latest_is_validated`.
#[test]
fn python_test_state_outbox_evidence_and_verdict_share_one_transaction() {
    let home = common::Home::new();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "fixture-session".into(),
        external_turn_id: None,
    });
    let (id, _) = home
        .store()
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let delivery: String = store
        .conn
        .query_row(
            "SELECT id FROM deliveries WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    store.conn.execute("UPDATE deliveries SET state='sending',attempts=1,lease_owner='worker',lease_until=? WHERE id=?", (agent_run_domain::domain::now() + 30.0, &delivery)).unwrap();
    let evidence = json!({
        "classifier":"delivered", "executable":"codex", "argv_shape":["executable","queue"], "duration_ms":1,
        "returncode":0, "spawn_errno":null, "error_class":null,
        "stdout_tail":"token top-secret", "stderr_tail":"", "stdout_bytes":16, "stderr_bytes":0,
        "stdout_truncated":false, "stderr_truncated":false, "message_id_present":true
    });
    store
        .persist_delivery_verdict(&delivery, "worker", "delivered", &evidence)
        .unwrap();
    let (state, stored): (String, String) = store.conn.query_row("SELECT d.state,e.evidence_json FROM deliveries d JOIN delivery_attempt_evidence e ON e.delivery_id=d.id WHERE d.id=?", [&delivery], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    assert_eq!(state, "delivered");
    assert!(stored.contains("[redacted]"));
    assert!(!stored.contains("top-secret"));
}
