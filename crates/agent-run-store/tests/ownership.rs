//! Acceptance evidence for startup ownership claims and supervisor handoff.

mod common;

use agent_run_core::{domain::Status, lifecycle::reconcile::reconcile_with, process::ProcessState};
use agent_run_domain::Error;
use serde_json::json;

/// Remove the automatic broker claim so the fixture can install a controlled owner.
fn clear_startup_claim(store: &agent_run_store::Store, id: &agent_run_domain::domain::AgentId) {
    store
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=NULL,startup_owner_birth_time=NULL,startup_deadline_at=NULL WHERE id=?",
            [id.as_str()],
        )
        .expect("clear startup claim");
}

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_lost_convergence_releases_active_capacity`
#[test]
fn dead_startup_owner_is_reconciled_and_capacity_is_released() {
    let home = common::Home::new();
    let mut config = home.config.clone();
    config.core.max_active_agents = 1;
    let mut store = home.store();
    let id = store
        .admit(&home.request(), &config, &json!({}), None)
        .unwrap()
        .0;
    clear_startup_claim(&store, &id);
    store
        .claim_startup(&id, "123 dead-owner", Some(12.5), 10.0, 120.0)
        .unwrap();
    assert!(matches!(
        store.admit(&home.request(), &config, &json!({}), None),
        Err(Error::Capacity)
    ));

    assert_eq!(
        reconcile_with(&mut store, 10, |pid, _, birth| {
            assert_eq!(pid, Some(123));
            assert_eq!(birth, Some(12.5));
            ProcessState::Dead
        })
        .unwrap(),
        vec![id.clone()]
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Lost);
    assert!(store
        .admit(&home.request(), &config, &json!({}), None)
        .is_ok());
}

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_handoff_renews_deadline_until_late_supervisor_proof`
#[test]
fn live_startup_owner_survives_handoff_and_late_supervisor_proof() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap()
        .0;
    clear_startup_claim(&store, &id);
    let owner = "123 detached-supervisor";
    store
        .claim_startup(&id, owner, Some(12.5), 10.0, 120.0)
        .unwrap();
    assert!(store
        .begin_supervisor_handoff(&id, owner, 129.0, 40.0)
        .unwrap());
    let deadline: f64 = store
        .conn
        .query_row(
            "SELECT startup_deadline_at FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(deadline, 169.0);
    assert!(
        reconcile_with(&mut store, 10, |_, _, _| ProcessState::Alive)
            .unwrap()
            .is_empty()
    );
    store
        .record_supervisor(&id, 123, "identity", 123, Some(12.5), 132.0)
        .unwrap();
    assert!(
        reconcile_with(&mut store, 10, |_, _, _| ProcessState::Alive)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Starting);
}
