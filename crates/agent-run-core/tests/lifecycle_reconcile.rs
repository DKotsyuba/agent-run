//! Ports recovery and wait cases from Python's reconciliation and wait tests.
//!
//! Each test name states the Python regression it mirrors.  The observer hook
//! lets the tests cover unavailable PID evidence without fabricating OS state.

mod common;

use agent_run_core::{
    domain::{Outcome, Status},
    lifecycle::reconcile::{reconcile, reconcile_with},
    process::{self, ProcessState},
    service::Service,
};
use agent_run_store::Store;
use serde_json::json;
use std::{
    process::Command,
    thread,
    time::{Duration, Instant},
};

/// Admit a row without launching a supervisor, returning its durable id.
fn admitted(
    home: &common::Home,
    store: &mut Store,
    timeout: Option<f64>,
) -> agent_run_core::domain::AgentId {
    let mut request = home.request();
    request.timeout_seconds = timeout;
    store
        .admit(&request, &home.config, &json!({}), None)
        .expect("admission succeeds")
        .0
}

/// Write a complete active supervisor ownership record for deterministic probes.
fn active(
    store: &Store,
    id: &agent_run_core::domain::AgentId,
    pid: i32,
    identity: &str,
    birth: f64,
) {
    store.conn.execute(
        "UPDATE agents SET status='running',supervisor_pid=?,process_group_id=?,supervisor_identity=?,supervisor_birth_time=?,heartbeat_at=? WHERE id=?",
        rusqlite::params![pid, pid, identity, birth, agent_run_core::domain::now(), id.as_str()],
    ).expect("active ownership row");
}

/// Mirrors `test_unowned_starting_reaps_dead_start_owner_after_broker_crash`.
#[test]
fn broker_crash_reconciles_only_a_dead_start_owner() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    store
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=? WHERE id=?",
            rusqlite::params![
                json!({"pid":4242,"token":"linux:fixture:1","birth":1.0}).to_string(),
                1.0,
                id.as_str()
            ],
        )
        .unwrap();

    let changed = reconcile_with(&mut store, 10, |pid, _, _| {
        assert_eq!(pid, Some(4242));
        ProcessState::Dead
    })
    .unwrap();

    assert_eq!(changed, vec![id.clone()]);
    let row = store.get(&id).unwrap();
    assert_eq!(row.status, Status::Lost);
    assert_eq!(row.failure_kind.as_deref(), Some("unowned_starting"));
}

/// Mirrors `test_reused_supervisor_is_lost_with_identity_mismatch`.
#[test]
fn reused_pid_is_lost_with_identity_mismatch() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    active(&store, &id, 4243, "linux:fixture:2", 2.0);

    let changed = reconcile_with(&mut store, 10, |_, _, _| ProcessState::Reused).unwrap();

    assert_eq!(changed, vec![id.clone()]);
    let row = store.get(&id).unwrap();
    assert_eq!(row.status, Status::Lost);
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("supervisor_identity_mismatch")
    );
}

/// Mirrors `test_unavailable_startup_birth_proof_is_not_death_before_deadline`.
#[test]
fn unknown_observation_never_reconciles_an_active_row() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    active(&store, &id, 4244, "linux:fixture:3", 3.0);

    assert!(
        reconcile_with(&mut store, 10, |_, _, _| ProcessState::Unknown)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Running);
}

/// Mirrors `test_active_sweep_cursor_reaches_a_late_dead_supervisor`.
#[test]
fn fair_cursor_advances_past_live_rows_to_a_late_dead_supervisor() {
    let home = common::Home::new();
    let mut store = home.store();
    let ids = (0..3)
        .map(|index| {
            let id = admitted(&home, &mut store, None);
            active(
                &store,
                &id,
                4300 + index,
                &format!("linux:fixture:{index}"),
                index as f64 + 1.0,
            );
            id
        })
        .collect::<Vec<_>>();

    for expected in [None, None, Some(ids[2].clone())] {
        let changed = reconcile_with(&mut store, 1, |pid, _, _| {
            if pid == Some(4302) {
                ProcessState::Dead
            } else {
                ProcessState::Alive
            }
        })
        .unwrap();
        assert_eq!(changed.into_iter().next(), expected);
    }
    let cursor: String = store
        .conn
        .query_row(
            "SELECT agent_id FROM reconciliation_cursors WHERE name='active_supervisors'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cursor, ids[2].as_str());
}

/// Mirrors the killed-supervisor path in `test_launch_reaper.py`.
#[test]
fn killed_supervisor_is_observed_dead_by_the_native_platform_probe() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    let mut child = Command::new("sh").args(["-c", "sleep 5"]).spawn().unwrap();
    let identity = process::inspect(child.id() as i32).expect("child identity");
    active(&store, &id, identity.pid, &identity.token, identity.birth);
    child.kill().unwrap();
    child.wait().unwrap();

    assert_eq!(reconcile(&mut store, 10).unwrap(), vec![id.clone()]);
    assert_eq!(
        store.get(&id).unwrap().failure_kind.as_deref(),
        Some("supervisor_dead")
    );
}

/// Mirrors `test_running_agent_transitions_to_succeeded_and_returns_the_answer`.
#[tokio::test]
async fn wait_observes_a_terminal_transition_committed_by_another_connection() {
    let home = common::Home::new();
    let mut initial = home.store();
    let id = admitted(&home, &mut initial, Some(0.001));
    active(&initial, &id, 4400, "linux:fixture:wait", 4.0);
    drop(initial);
    let path = home.path.clone();
    let other_id = id.clone();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(40));
        let mut store = Store::open(&path).unwrap();
        let mut outcome = Outcome::failure("external_terminal_transition");
        outcome.status = Status::Lost;
        store.finish(&other_id, &outcome, None, None).unwrap();
    });

    let value = Service::new(home.path.clone())
        .wait(&id, Some(2.0))
        .await
        .unwrap();
    writer.join().unwrap();
    assert_eq!(value["status"], "lost");
    assert_eq!(value["available"], false);
}

/// Mirrors `test_watcher_gives_up_with_the_current_status_and_a_note`.
#[tokio::test]
async fn wait_honors_its_own_bound_without_using_the_legacy_run_timeout() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, Some(0.001));
    active(&store, &id, 4401, "linux:fixture:bound", 5.0);
    let started = Instant::now();
    let value = Service::new(home.path.clone())
        .wait(&id, Some(0.03))
        .await
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(150));
    assert_eq!(value["terminal"], false);
    assert_eq!(value["status"], "running");
}
