//! Ports recovery and wait cases from Python's reconciliation and wait tests.
//!
//! Each test name states the Python regression it mirrors.  The observer hook
//! lets the tests cover unavailable PID evidence without fabricating OS state.

mod common;

use agent_run_core::{
    domain::{Outcome, Status},
    lifecycle::reconcile::{
        reconcile, reconcile_reaped_agent, reconcile_reaped_supervisor, reconcile_with,
    },
    process::{self, ProcessState},
    service::Service,
};
use agent_run_domain::Error;
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

/// Clears the automatic broker claim so a fixture models an identity-less start.
fn clear_startup_claim(store: &Store, id: &agent_run_core::domain::AgentId) {
    store
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=NULL,startup_owner_birth_time=NULL,startup_deadline_at=NULL WHERE id=?",
            [id.as_str()],
        )
        .expect("clear startup claim");
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
/// Mirrors Python `tests/test_supervisor.py::SupervisorTests::test_reused_group_id_never_receives_native_cancel_or_signal`.
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

/// Proves a lost reconciliation drains late pending commands through the same
/// shared terminal path as a normal completion, so no command stays pending.
#[test]
fn lost_reconciliation_finalizes_pending_commands() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    active(&store, &id, 4245, "linux:fixture:4", 4.0);
    store
        .enqueue(&id, "steer", &json!({"text":"continue"}))
        .unwrap();
    store.enqueue(&id, "cancel", &json!({})).unwrap();

    let changed = reconcile_with(&mut store, 10, |_, _, _| ProcessState::Dead).unwrap();

    assert_eq!(changed, vec![id.clone()]);
    assert_eq!(store.get(&id).unwrap().status, Status::Lost);
    let (pending, completed): (i64, i64) = store
        .conn
        .query_row(
            "SELECT SUM(state='pending'),SUM(state='completed') FROM commands WHERE agent_id=?",
            [id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(pending, 0, "no pending command survives the terminal loss");
    assert_eq!(completed, 2, "both commands receive one terminal result");
    let cancelled: String = store
        .conn
        .query_row(
            "SELECT result_json FROM commands WHERE agent_id=? AND kind='cancel'",
            [id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&cancelled).unwrap(),
        json!({"accepted": true, "reason": "already_stopping"})
    );
}

/// Proves the lost transition and pending-command finalization are one atomic
/// commit: a failure while finalizing commands rolls the whole loss back, so a
/// retry can still reconcile the row, and a claimed command is never replayed.
#[test]
fn lost_reconciliation_is_atomic_with_command_finalization() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    active(&store, &id, 4246, "linux:fixture:5", 5.0);
    store
        .enqueue(&id, "steer", &json!({"text":"claimed"}))
        .unwrap();
    store.claim_command(&id).unwrap().unwrap();
    store
        .enqueue(&id, "steer", &json!({"text":"pending"}))
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER fail_finalize BEFORE UPDATE OF state ON commands
             WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'injected'); END",
        )
        .unwrap();

    reconcile_with(&mut store, 10, |_, _, _| ProcessState::Dead).unwrap_err();

    assert_eq!(store.get(&id).unwrap().status, Status::Running);
    let states = |store: &Store| -> Vec<String> {
        let mut statement = store
            .conn
            .prepare("SELECT state FROM commands WHERE agent_id=? ORDER BY id")
            .unwrap();
        let rows = statement
            .query_map([id.as_str()], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap();
        rows
    };
    assert_eq!(states(&store), ["claimed", "pending"]);
    store
        .conn
        .execute_batch("DROP TRIGGER fail_finalize")
        .unwrap();

    let changed = reconcile_with(&mut store, 10, |_, _, _| ProcessState::Dead).unwrap();

    assert_eq!(changed, vec![id.clone()]);
    assert_eq!(store.get(&id).unwrap().status, Status::Lost);
    assert_eq!(states(&store), ["claimed", "completed"]);
}

/// Mirrors `test_unavailable_startup_birth_proof_is_not_death_before_deadline`.
/// Mirrors Python `tests/test_supervisor.py::SupervisorTests::test_supervisor_identity_needs_no_process_probe`.
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

/// Mirrors `tests/test_state_outbox.py::test_reconciliation_requires_supplied_proof_before_persisting_lost`.
#[test]
fn python_test_state_outbox_reconciliation_requires_valid_supplied_proof() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    assert!(reconcile_reaped_agent(&mut store, &id, 1, 3.0).is_err());
    assert_eq!(store.get(&id).unwrap().status, Status::Starting);
    active(&store, &id, 100, "pid100:start1", 20.0);
    assert!(!reconcile_reaped_agent(&mut store, &id, 101, 3.0).unwrap());
    assert_eq!(store.get(&id).unwrap().status, Status::Running);
    assert!(
        reconcile_with(&mut store, 10, |_, _, _| ProcessState::Unknown)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Running);
    assert_eq!(
        reconcile_with(&mut store, 10, |_, _, _| ProcessState::Reused).unwrap(),
        vec![id.clone()]
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Lost);
}

/// Mirrors `tests/test_state_outbox.py::test_reaped_supervisor_reconciles_only_its_active_rows`.
#[test]
fn python_test_state_outbox_reaped_supervisor_reconciles_only_active_rows() {
    let home = common::Home::new();
    let mut store = home.store();
    let dead = admitted(&home, &mut store, None);
    let other = admitted(&home, &mut store, None);
    let terminal = admitted(&home, &mut store, None);
    active(&store, &dead, 100, "pid-100", 1.0);
    active(&store, &other, 200, "pid-200", 1.0);
    active(&store, &terminal, 100, "pid-100", 1.0);
    store
        .finish(&terminal, &Outcome::failure("fixture"), None, None)
        .unwrap();
    assert_eq!(
        reconcile_reaped_supervisor(&mut store, 100, agent_run_core::domain::now() + 1.0, 100)
            .unwrap(),
        vec![dead]
    );
    assert_eq!(store.get(&other).unwrap().status, Status::Running);
    assert_eq!(store.get(&terminal).unwrap().status, Status::Failed);
    assert!(reconcile_reaped_supervisor(&mut store, 100, 7.0, 100)
        .unwrap()
        .is_empty());
}

/// Mirrors `tests/test_state_outbox.py::test_reaped_agent_closes_the_pre_identity_starting_window`.
#[test]
fn python_test_state_outbox_reaped_agent_closes_pre_identity_starting_window() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    assert!(reconcile_reaped_agent(&mut store, &id, 321, 3.0).unwrap());
    assert_eq!(store.get(&id).unwrap().status, Status::Lost);
    assert!(!reconcile_reaped_agent(&mut store, &id, 321, 4.0).unwrap());
    let other = admitted(&home, &mut store, None);
    active(&store, &other, 999, "pid-999", 1.0);
    assert!(!reconcile_reaped_agent(&mut store, &other, 321, 7.0).unwrap());
    assert_eq!(store.get(&other).unwrap().status, Status::Running);
}

/// Mirrors `tests/test_state_outbox.py::test_supervisor_group_refines_once_from_the_supervisors_own_group`.
#[test]
fn python_test_state_outbox_supervisor_group_refines_once() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    store
        .record_supervisor(&id, 100, "pid-100", 100, None, 6.0)
        .unwrap();
    store
        .record_supervisor(&id, 100, "pid-100", 4242, None, 8.0)
        .unwrap();
    for (pid, identity, group) in [
        (100, "pid-100", 4243),
        (100, "pid-100", 100),
        (101, "pid-100", 4242),
        (100, "other", 4242),
    ] {
        assert!(store
            .record_supervisor(&id, pid, identity, group, None, 9.0)
            .is_err());
    }
    let row = store.get(&id).unwrap();
    assert_eq!(
        (
            row.supervisor_pid,
            row.supervisor_identity,
            row.process_group_id
        ),
        (Some(100), Some("pid-100".into()), Some(4242))
    );
}

/// Mirrors `tests/test_state_outbox.py::test_sweep_closes_dead_supervisors_with_live_groups_without_signalling`.
#[test]
fn python_test_state_outbox_sweep_closes_dead_supervisors_without_signalling() {
    let home = common::Home::new();
    let mut store = home.store();
    let surviving = admitted(&home, &mut store, None);
    let foreign = admitted(&home, &mut store, None);
    let terminal = admitted(&home, &mut store, None);
    active(&store, &surviving, 100, "pid-100", 1.0);
    active(&store, &foreign, 101, "pid-101", 1.0);
    active(&store, &terminal, 102, "pid-102", 1.0);
    store
        .finish(&terminal, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let probed = std::sync::Mutex::new(Vec::new());
    let changed = reconcile_with(&mut store, 100, |pid, _, _| {
        probed.lock().unwrap().push(pid);
        ProcessState::Dead
    })
    .unwrap();
    assert_eq!(changed, vec![surviving.clone(), foreign.clone()]);
    assert_eq!(*probed.lock().unwrap(), vec![Some(100), Some(101)]);
    assert_eq!(store.get(&terminal).unwrap().status, Status::Failed);
}

/// Mirrors `tests/test_state_outbox.py::test_sweep_reconciles_only_a_proven_reused_pid`.
#[test]
fn python_test_state_outbox_sweep_reconciles_only_proven_reused_pid() {
    let home = common::Home::new();
    let mut store = home.store();
    let exact = admitted(&home, &mut store, None);
    let boundary = admitted(&home, &mut store, None);
    let unavailable = admitted(&home, &mut store, None);
    let mismatch = admitted(&home, &mut store, None);
    for (id, pid, birth) in [
        (&exact, 200, 20.0),
        (&boundary, 201, 21.0),
        (&unavailable, 202, 22.0),
        (&mismatch, 203, 23.0),
    ] {
        active(&store, id, pid, "agent-run supervisor", birth);
    }
    let changed = reconcile_with(&mut store, 100, |pid, _, _| match pid {
        Some(203) => ProcessState::Reused,
        Some(202) => ProcessState::Denied,
        _ => ProcessState::Alive,
    })
    .unwrap();
    assert_eq!(changed, vec![mismatch.clone()]);
    assert_eq!(store.get(&exact).unwrap().status, Status::Running);
    assert_eq!(store.get(&boundary).unwrap().status, Status::Running);
    assert_eq!(store.get(&unavailable).unwrap().status, Status::Running);
    assert_eq!(
        store.get(&mismatch).unwrap().failure_kind.as_deref(),
        Some("supervisor_identity_mismatch")
    );
}

/// Mirrors `tests/test_state_outbox.py::test_one_stale_row_does_not_abort_the_rest_of_the_sweep`.
#[test]
fn python_test_state_outbox_one_stale_row_does_not_abort_sweep() {
    let home = common::Home::new();
    let mut store = home.store();
    let stale = admitted(&home, &mut store, None);
    let healthy = admitted(&home, &mut store, None);
    active(&store, &stale, 300, "pid-300", 1.0);
    active(&store, &healthy, 301, "pid-301", 2.0);
    store
        .conn
        .execute(
            "UPDATE agents SET heartbeat_at=? WHERE id=?",
            (agent_run_core::domain::now() + 100.0, stale.as_str()),
        )
        .unwrap();
    let changed = reconcile_with(&mut store, 100, |_, _, _| ProcessState::Dead).unwrap();
    assert_eq!(changed, vec![healthy.clone()]);
    assert_eq!(store.get(&stale).unwrap().status, Status::Running);
    assert_eq!(store.get(&healthy).unwrap().status, Status::Lost);
}

/// Mirrors `tests/test_state_outbox.py::test_each_row_is_timed_after_its_own_probe`.
#[test]
fn python_test_state_outbox_each_row_is_timed_after_its_probe() {
    let home = common::Home::new();
    let mut store = home.store();
    let first = admitted(&home, &mut store, None);
    let second = admitted(&home, &mut store, None);
    active(&store, &first, 400, "pid-400", 1.0);
    active(&store, &second, 401, "pid-401", 1.0);
    let path = home.path.clone();
    let changed = reconcile_with(&mut store, 100, |pid, _, _| {
        if pid == Some(400) {
            let heartbeat_store = Store::open(&path).unwrap();
            heartbeat_store
                .conn
                .execute(
                    "UPDATE agents SET heartbeat_at=? WHERE id=?",
                    (agent_run_core::domain::now(), second.as_str()),
                )
                .unwrap();
        }
        ProcessState::Dead
    })
    .unwrap();
    assert_eq!(changed, vec![first, second]);
}

/// Mirrors `test_wait.py::test_running_agent_transitions_to_succeeded_and_returns_the_answer`.
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

/// Mirrors `test_wait.py::test_watcher_gives_up_with_the_current_status_and_a_note`.
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

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_identity_less_starting_rows_are_never_lost_by_age`
#[test]
fn identity_less_starting_rows_are_never_lost_by_age() {
    let home = common::Home::new();
    let mut store = home.store();
    let stale = admitted(&home, &mut store, None);
    let recent = admitted(&home, &mut store, None);
    let owned = admitted(&home, &mut store, None);
    clear_startup_claim(&store, &stale);
    clear_startup_claim(&store, &recent);
    clear_startup_claim(&store, &owned);
    store
        .record_supervisor(&owned, 123, "identity", 123, None, 10.0)
        .unwrap();

    let changed = reconcile_with(&mut store, 10, |_, _, _| ProcessState::Alive).unwrap();

    assert!(changed.is_empty());
    assert_eq!(store.get(&stale).unwrap().status, Status::Starting);
    assert_eq!(store.get(&recent).unwrap().status, Status::Starting);
    assert_eq!(store.get(&owned).unwrap().status, Status::Starting);
}

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_lost_convergence_releases_active_capacity`
#[test]
fn lost_convergence_releases_active_capacity() {
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
        vec![id]
    );
    assert!(store
        .admit(&home.request(), &config, &json!({}), None)
        .is_ok());
}

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_live_owner_survives_elapsed_startup_deadline`
#[test]
fn live_owner_survives_elapsed_startup_deadline() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    clear_startup_claim(&store, &id);
    store
        .claim_startup(
            &id,
            "123 deliberately-wrong-command",
            Some(12.5),
            10.0,
            120.0,
        )
        .unwrap();

    assert!(
        reconcile_with(&mut store, 10, |_, _, _| ProcessState::Alive)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Starting);
}

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_handoff_renews_deadline_until_late_supervisor_proof`
#[test]
fn handoff_renews_deadline_until_late_supervisor_proof() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
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

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_elapsed_handoff_with_live_owner_remains_starting`
#[test]
fn elapsed_handoff_with_live_owner_remains_starting() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    clear_startup_claim(&store, &id);
    let owner = "123 detached-supervisor";
    store
        .claim_startup(&id, owner, Some(12.5), 10.0, 120.0)
        .unwrap();
    assert!(store
        .begin_supervisor_handoff(&id, owner, 129.0, 10.0)
        .unwrap());

    assert!(
        reconcile_with(&mut store, 10, |_, _, _| ProcessState::Alive)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Starting);
}

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_generated_handoff_never_uses_elapsed_time_as_loss_proof`
#[test]
fn generated_handoff_never_uses_elapsed_time_as_loss_proof() {
    for preparation_delay in 1..=9 {
        for handoff_extension in 0..=10 {
            for ready_before_expiry in [false, true] {
                let home = common::Home::new();
                let mut store = home.store();
                let id = admitted(&home, &mut store, None);
                clear_startup_claim(&store, &id);
                let owner = "123 generated-owner";
                store
                    .claim_startup(&id, owner, Some(12.5), 0.0, 10.0)
                    .unwrap();
                let handoff_seconds = 11 - preparation_delay + handoff_extension;
                let handoff_at = preparation_delay as f64;
                let deadline = handoff_at + handoff_seconds as f64;
                assert!(store
                    .begin_supervisor_handoff(&id, owner, handoff_at, handoff_seconds as f64)
                    .unwrap());
                assert!(
                    reconcile_with(&mut store, 10, |_, _, _| ProcessState::Alive)
                        .unwrap()
                        .is_empty()
                );
                if ready_before_expiry {
                    store
                        .record_supervisor(&id, 123, "identity", 123, Some(12.5), deadline - 0.5)
                        .unwrap();
                }
                assert!(
                    reconcile_with(&mut store, 10, |_, _, _| ProcessState::Alive)
                        .unwrap()
                        .is_empty()
                );
                assert_eq!(store.get(&id).unwrap().status, Status::Starting);
            }
        }
    }
}

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_delayed_startup_can_bind_a_supervisor`
#[test]
fn delayed_startup_can_bind_a_supervisor() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    clear_startup_claim(&store, &id);
    store
        .claim_startup(&id, "1 stale", None, 10.0, 1.0)
        .unwrap();

    store
        .record_supervisor(&id, 123, "identity", 123, None, 11.0)
        .unwrap();
    assert_eq!(store.get(&id).unwrap().supervisor_pid, Some(123));
}

/// Mirrors `tests/test_reconciliation.py::UnownedStartingReconciliationTests::test_owned_supervisor_can_refine_its_group_after_startup_expiry`
#[test]
fn owned_supervisor_can_refine_its_group_after_startup_expiry() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = admitted(&home, &mut store, None);
    clear_startup_claim(&store, &id);
    store
        .claim_startup(&id, "1 owner", None, 10.0, 1.0)
        .unwrap();
    store
        .record_supervisor(&id, 123, "identity", 123, None, 10.5)
        .unwrap();
    store
        .record_supervisor(&id, 123, "identity", 456, None, 12.0)
        .unwrap();
    assert_eq!(store.get(&id).unwrap().process_group_id, Some(456));
}
