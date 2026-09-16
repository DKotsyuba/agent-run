//! Direct ports of the remaining Python completion-outbox contracts.

mod common;

use agent_run_domain::domain::{OrchestratorRef, Outcome};
use agent_run_domain::Error;
use agent_run_platform::{fs, verify};
use serde_json::{json, Value};
use std::path::Path;

/// Creates one admitted agent for an outbox scenario.
fn create(home: &common::Home) -> agent_run_domain::domain::AgentId {
    home.store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap()
        .0
}

/// Finishes an agent and binds its terminal delivery to one queue session.
fn bound_delivery(home: &common::Home) -> (agent_run_domain::domain::AgentId, String) {
    let id = create(home);
    let mut store = home.store();
    let root = home.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "fixture answer").unwrap();
    store.running(&id, 42).unwrap();
    store
        .finish(&id, &Outcome::success(None), Some(&proof), None)
        .unwrap();
    let session = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: Some("turn".into()),
    };
    store.bind_orchestrator(&id, &session, 5.0).unwrap();
    let delivery: String = store
        .conn
        .query_row(
            "SELECT id FROM deliveries WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    (id, delivery)
}

/// Returns the complete evidence shape used by the Python delivery tests.
fn evidence(classifier: &str, duration_ms: u64) -> Value {
    json!({
        "classifier": classifier, "executable": "/bin/codex", "argv_shape": ["executable", "queue"],
        "duration_ms": duration_ms, "returncode": 0, "spawn_errno": null, "error_class": null,
        "stdout_tail": "", "stderr_tail": "", "stdout_bytes": 0, "stderr_bytes": 0,
        "stdout_truncated": false, "stderr_truncated": false, "message_id_present": true
    })
}

/// Mirrors `tests/test_state_outbox.py::test_terminal_before_binding_activates_once_and_expired_lease_reclaims_once`.
#[test]
fn python_test_state_outbox_claim_reclaims_expired_lease_once() {
    let home = common::Home::new();
    let id = create(&home);
    let mut store = home.store();
    let root = home.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "fixture answer").unwrap();
    store.running(&id, 42).unwrap();
    store
        .finish(&id, &Outcome::success(None), Some(&proof), None)
        .unwrap();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: None,
    };
    store.bind_orchestrator(&id, &reference, 5.0).unwrap();
    let delivery: String = store
        .conn
        .query_row(
            "SELECT id FROM deliveries WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let first = store
        .claim_delivery("worker-1", 5.0, 10.0)
        .unwrap()
        .unwrap();
    assert_eq!(first["id"], delivery);
    assert_eq!(first["attempts"], 1);
    assert!(store
        .claim_delivery("worker-2", 14.0, 10.0)
        .unwrap()
        .is_none());
    assert!(matches!(
        store.complete_delivery(&delivery, "worker-1", 15.0, None, false, None),
        Err(Error::Validation(_))
    ));
    let reclaimed = store
        .claim_delivery("worker-2", 15.0, 10.0)
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed["attempts"], 2);
    assert!(store
        .claim_delivery("worker-3", 15.0, 10.0)
        .unwrap()
        .is_none());
    assert!(store
        .complete_delivery(&delivery, "worker-1", 16.0, None, false, None)
        .is_err());
    store
        .complete_delivery(&delivery, "worker-2", 16.0, Some("remote-1"), false, None)
        .unwrap();
    assert_eq!(reclaimed["agent_status"], "succeeded");
}

/// Mirrors `tests/test_state_outbox.py::test_retry_backoff_and_cancellation_preserve_terminal_result`.
#[test]
fn python_test_state_outbox_retry_backoff_and_cancellation_preserve_terminal_result() {
    let home = common::Home::new();
    let (id, _) = bound_delivery(&home);
    let mut store = home.store();
    let delivery = store.claim_delivery("worker", 5.0, 10.0).unwrap().unwrap();
    let next = store
        .retry_delivery(
            delivery["id"].as_str().unwrap(),
            "worker",
            "ambiguous timeout",
            6.0,
            true,
            None,
            1.0,
            300.0,
        )
        .unwrap();
    assert_eq!(next, 7.0);
    assert!(store.claim_delivery("worker", 6.9, 10.0).unwrap().is_none());
    let retried = store.claim_delivery("worker", 7.0, 10.0).unwrap().unwrap();
    assert_eq!(retried["attempts"], 2);
    assert!(store
        .cancel_delivery(retried["id"].as_str().unwrap())
        .unwrap());
    assert_eq!(store.get(&id).unwrap().status.as_str(), "succeeded");
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ambiguous_result FROM deliveries WHERE id=?",
                [retried["id"].as_str().unwrap()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert!(store
        .claim_delivery("other", 1000.0, 10.0)
        .unwrap()
        .is_none());
}

/// Mirrors `tests/test_state_outbox.py::test_delivery_attempt_evidence_is_immutable_and_latest_is_validated`.
#[test]
fn python_test_state_outbox_each_owned_attempt_has_immutable_evidence() {
    let home = common::Home::new();
    let (_, delivery) = bound_delivery(&home);
    let mut store = home.store();
    store.claim_delivery("worker", 5.0, 10.0).unwrap();
    store
        .retry_delivery(
            &delivery,
            "worker",
            "exit 127",
            6.0,
            false,
            Some(&evidence("exit", 4)),
            1.0,
            300.0,
        )
        .unwrap();
    store.claim_delivery("worker", 7.0, 10.0).unwrap();
    store
        .complete_delivery(
            &delivery,
            "worker",
            8.0,
            None,
            false,
            Some(&evidence("success", 2)),
        )
        .unwrap();
    let rows: Vec<(u32, f64)> = store.conn.prepare("SELECT attempt,recorded_at FROM delivery_attempt_evidence WHERE delivery_id=? ORDER BY attempt").unwrap().query_map([&delivery], |row| Ok((row.get(0)?, row.get(1)?))).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows, vec![(1, 6.0), (2, 8.0)]);
    assert_eq!(
        store.latest_delivery_attempt(&delivery).unwrap().unwrap()["classifier"],
        "success"
    );
}

/// Mirrors `tests/test_state_outbox.py::test_failed_delivery_requires_live_owned_lease`.
#[test]
fn python_test_state_outbox_failure_requires_a_live_owned_lease() {
    let home = common::Home::new();
    let (_, delivery) = bound_delivery(&home);
    let mut store = home.store();
    store.claim_delivery("worker-1", 5.0, 10.0).unwrap();
    assert!(store
        .fail_delivery(&delivery, "worker-2", "wrong owner", 6.0, false, None)
        .is_err());
    assert!(store
        .fail_delivery(&delivery, "worker-1", "expired", 15.0, false, None)
        .is_err());
    let unchanged: (String, Option<String>, i64, Option<String>) = store
        .conn
        .query_row(
            "SELECT state,last_error,ambiguous_result,lease_owner FROM deliveries WHERE id=?",
            [&delivery],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        unchanged,
        ("sending".into(), None, 0, Some("worker-1".into()))
    );
    store.claim_delivery("worker-2", 15.0, 10.0).unwrap();
    store
        .fail_delivery(&delivery, "worker-2", "permanent", 16.0, true, None)
        .unwrap();
    let failed: (String, String, i64, Option<String>, Option<f64>) = store.conn.query_row("SELECT state,last_error,ambiguous_result,lease_owner,lease_until FROM deliveries WHERE id=?", [&delivery], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).unwrap();
    assert_eq!(failed, ("failed".into(), "permanent".into(), 1, None, None));
}

/// Mirrors `tests/test_state_outbox.py::test_context_receipt_upserts_only_changed_keys`.
#[test]
fn python_test_state_outbox_context_receipt_changes_only_changed_keys() {
    let home = common::Home::new();
    let id = create(&home);
    let mut store = home.store();
    let session = store
        .bind_orchestrator(
            &id,
            &OrchestratorRef {
                transport: "codex_queue".into(),
                external_session_id: "session".into(),
                external_turn_id: None,
            },
            5.0,
        )
        .unwrap();
    assert!(store
        .record_context_receipt(&session, "first", 6.0)
        .unwrap());
    assert!(!store
        .record_context_receipt(&session, "first", 7.0)
        .unwrap());
    assert_eq!(store.conn.query_row("SELECT context_key,injected_at FROM context_receipts WHERE orchestrator_session_id=?", [&session], |row| Ok((row.get::<_,String>(0)?,row.get::<_,f64>(1)?))).unwrap(), ("first".into(), 6.0));
    assert!(store
        .record_context_receipt(&session, "second", 8.0)
        .unwrap());
    assert!(store
        .record_context_receipt("unknown-session", "key", 9.0)
        .is_err());
}

/// Mirrors `tests/test_state_outbox.py::test_context_receipt_for_ref_creates_and_reuses_session`.
#[test]
fn python_test_state_outbox_context_receipt_for_ref_reuses_session() {
    let home = common::Home::new();
    let mut store = home.store();
    let ref_one = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "bookkeeping".into(),
        external_turn_id: Some("turn-1".into()),
    };
    let (session, changed) = store
        .record_context_receipt_for_ref(&ref_one, "first", 5.0)
        .unwrap();
    assert!(changed);
    assert_eq!(
        store
            .record_context_receipt_for_ref(
                &OrchestratorRef {
                    external_turn_id: None,
                    ..ref_one.clone()
                },
                "first",
                6.0
            )
            .unwrap(),
        (session.clone(), false)
    );
    assert_eq!(
        store
            .record_context_receipt_for_ref(&ref_one, "second", 7.0)
            .unwrap(),
        (session.clone(), true)
    );
    let id = create(&home);
    assert_eq!(
        store
            .bind_orchestrator(
                &id,
                &OrchestratorRef {
                    external_turn_id: Some("turn-2".into()),
                    ..ref_one
                },
                8.0
            )
            .unwrap(),
        session
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM orchestrator_sessions", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        1
    );
}
