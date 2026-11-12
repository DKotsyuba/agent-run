//! Acceptance evidence for startup ownership claims and supervisor handoff.

use crate::common;

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

/// A system launch cannot replace another owner or release a capability for a different route.
#[test]
fn system_startup_owner_requires_the_frozen_route_capability_and_broker() {
    use agent_run_store::ops::SystemStartupFence;
    let home = common::Home::new();
    let mut store = home.store();
    let fingerprint = "a".repeat(64);
    let capability = "b".repeat(64);
    let broker = json!({"pid":123,"token":"linux:broker","birth":12.5});
    let target = json!({"pid":456,"token":"linux:target","birth":14.5});
    let identity = json!({"system_route":{"fingerprint":fingerprint}});
    let id = store
        .admit(&home.request(), &home.config, &identity, None)
        .unwrap()
        .0;
    store
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=? WHERE id=?",
            rusqlite::params![broker.to_string(), 12.5, id.as_str()],
        )
        .unwrap();
    store.record_supervisor_launch(&id, &capability).unwrap();
    let fence = SystemStartupFence {
        capability_sha256: capability,
        route_fingerprint: fingerprint,
        broker_identity: broker.clone(),
    };
    let mut attempts = Vec::new();
    let mut wrong = fence.clone();
    wrong.capability_sha256 = "c".repeat(64);
    attempts.push(wrong);
    let mut wrong = fence.clone();
    wrong.route_fingerprint = "d".repeat(64);
    attempts.push(wrong);
    let mut wrong = fence.clone();
    wrong.broker_identity["token"] = json!("linux:other");
    attempts.push(wrong);
    for wrong in attempts {
        assert!(matches!(
            store.record_system_startup_owner(&id, &target, &wrong),
            Err(Error::Conflict)
        ));
        let current: String = store
            .conn
            .query_row(
                "SELECT startup_owner_pid_identity FROM agents WHERE id=?",
                [id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(current, broker.to_string());
    }
    store
        .record_system_startup_owner(&id, &target, &fence)
        .unwrap();
    let (current, birth): (String, f64) = store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(current, target.to_string());
    assert_eq!(birth, 14.5);
    assert!(matches!(
        store.record_system_startup_owner(&id, &target, &fence),
        Err(Error::Conflict)
    ));
}

/// A late local launch cannot overwrite the owner after its capability rotates.
#[test]
fn local_startup_owner_requires_the_current_capability() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap()
        .0;
    let old = "a".repeat(64);
    let current = "b".repeat(64);
    store.record_supervisor_launch(&id, &old).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET supervisor_capability_sha256=? WHERE id=?",
            rusqlite::params![current, id.as_str()],
        )
        .unwrap();
    let child = json!({"pid":123,"token":"linux:fixture","birth":1});
    assert!(matches!(
        store.record_local_startup_owner(&id, &child, &old),
        Err(Error::Conflict)
    ));
    store
        .record_local_startup_owner(&id, &child, &current)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert!(matches!(
        store.record_local_startup_owner(&id, &child, &current),
        Err(Error::Conflict)
    ));
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
    assert!(
        store
            .admit(&home.request(), &config, &json!({}), None)
            .is_ok()
    );
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
    assert!(
        store
            .begin_supervisor_handoff(&id, owner, 129.0, 40.0)
            .unwrap()
    );
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
