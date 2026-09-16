//! Admission transaction regressions ported from the Python state/service suite.

mod common;

use agent_run_domain::{domain::Status, Error};
use agent_run_store::Store;
use serde_json::json;
use std::{
    process::Command,
    sync::{Arc, Barrier},
    thread,
};

/// Mirrors Python `test_limited_creation_checks_idempotency_before_caps_and_validates_limits`.
#[test]
fn replay_precedes_capacity_and_keeps_the_original_agent() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("replay-before-capacity".into());
    let identity = json!({"replay_request_sha256": "request-v1"});
    let (agent_id, created) = home
        .store()
        .admit(&request, &home.config, &identity, None)
        .unwrap();
    assert!(created);

    let mut exhausted = home.config.clone();
    exhausted.core.max_active_agents = 1;
    let (replay_id, created) = home
        .store()
        .admit(&request, &exhausted, &identity, None)
        .unwrap();
    assert!(!created);
    assert_eq!(replay_id, agent_id);
}

/// Mirrors Python `test_request_id_returns_one_agent_and_launches_once`.
#[test]
fn replay_does_not_rewrite_frozen_identity_or_initial_events() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("frozen-replay".into());
    let identity = json!({
        "replay_request_sha256": "request-v1",
        "role_plan": {"config_revision": "a".repeat(64)},
    });
    let (agent_id, created) = home
        .store()
        .admit_with_config_revision(&request, &home.config, &"a".repeat(64), &identity, None)
        .unwrap();
    assert!(created);
    let mut edited = home.config.clone();
    edited.core.max_active_agents = 1;
    let (_, created) = home
        .store()
        .admit_with_config_revision(&request, &edited, &"a".repeat(64), &identity, None)
        .unwrap();
    assert!(!created);

    let store = home.store();
    let record = store.get(&agent_id).unwrap();
    assert_eq!(record.status, Status::Starting);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT config_revision FROM agents WHERE id=?",
                [agent_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "a".repeat(64)
    );
    let events = store
        .conn
        .prepare("SELECT kind FROM events WHERE agent_id=? ORDER BY seq")
        .unwrap()
        .query_map([agent_id.as_str()], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(events, ["created", "start_accepted"]);
}

/// Mirrors Python `test_request_id_was_reused_for_a_different_request`.
#[test]
fn conflicting_request_id_rolls_back_before_session_or_agent_writes() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("conflicting-replay".into());
    home.store()
        .admit(
            &request,
            &home.config,
            &json!({"replay_request_sha256": "request-v1"}),
            None,
        )
        .unwrap();
    request.task = "a different task".into();
    assert!(matches!(
        home.store().admit(
            &request,
            &home.config,
            &json!({"replay_request_sha256": "request-v2"}),
            None,
        ),
        Err(Error::Conflict)
    ));
    assert_eq!(
        home.store()
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_concurrent_limited_creation_allows_exactly_one_process`.
/// Mirrors Python `test_global_runtime_caps_and_terminal_rows_release_capacity` and T20.
#[test]
fn parallel_admissions_share_one_active_slot() {
    let home = common::Home::new();
    let path = home.path.clone();
    let mut config = home.config.clone();
    config.core.max_active_agents = 1;
    let barrier = Arc::new(Barrier::new(2));
    let workers = ["parallel-one", "parallel-two"].map(|task| {
        let path = path.clone();
        let config = config.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            let mut store = Store::open(&path).unwrap();
            let mut request: agent_run_domain::domain::StartRequest =
                serde_json::from_value(json!({
                    "runtime": "mock", "model": "fixture", "profile": "review", "task": task,
                    "workdir": path,
                }))
                .unwrap();
            request.validate().unwrap();
            barrier.wait();
            store.admit(&request, &config, &json!({}), None)
        })
    });
    let results = workers.map(|worker| worker.join().unwrap());
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::Capacity)))
            .count(),
        1
    );
    assert_eq!(
        home.store()
            .conn
            .query_row(
                "SELECT COUNT(*) FROM agents WHERE status IN ('created','starting','running','cancelling')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

/// Runs only as a subprocess, dying after the agent-row write and before events.
#[test]
fn crash_worker_dies_between_admission_writes() {
    let Ok(home) = std::env::var("AGENT_RUN_ADMISSION_CRASH_HOME") else {
        return;
    };
    let connection =
        rusqlite::Connection::open(std::path::Path::new(&home).join("state.db")).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    connection
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id,sequence) VALUES('ag-20260101-000000-0000000000','mock','fixture','review','crash','crash','/tmp','{}','starting',1,480,'pending:materialization','ag-20260101-000000-0000000000',1)",
            [],
        )
        .unwrap();
    std::process::exit(91);
}

/// Mirrors Python `test_create_agent_is_atomic_when_event_insert_crashes`.
/// Mirrors Python `test_state_store.py::test_admission_rolls_back_fault_or_commits_complete_starting_owner`.
///
/// A killed writer leaves neither the agent row nor its initial journal rows visible.
#[test]
fn process_death_mid_transaction_leaves_no_partial_admission() {
    let home = common::Home::new();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "crash_worker_dies_between_admission_writes",
            "--nocapture",
        ])
        .env("AGENT_RUN_ADMISSION_CRASH_HOME", &home.path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(91));
    let store = home.store();
    assert_eq!(store.health().unwrap()["integrity"], "ok");
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}
