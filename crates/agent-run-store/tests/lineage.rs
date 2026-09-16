//! Store ports of Python resume-lineage safety tests.

mod common;

use agent_run_domain::{domain::Outcome, Error};
use serde_json::json;
use std::{
    sync::{Arc, Barrier},
    thread,
};

/// Mirrors `test_resume.py::test_unfinished_parent_is_refused`.
#[test]
fn python_test_resume_unfinished_parent_is_refused() {
    let home = common::Home::new();
    let (parent, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let parent = home.store().get(&parent).unwrap();
    let error = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), Some(&parent))
        .unwrap_err();
    assert!(matches!(error, Error::Validation(message) if message.contains("not resumable")));
}

/// Mirrors `test_resume.py::test_finished_parent_whose_process_group_still_lives_is_refused`.
#[test]
fn python_test_resume_live_parent_group_is_refused() {
    let home = common::Home::new();
    let (parent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store
        .runtime_session(&parent_id, "fixture-session")
        .unwrap();
    store
        .finish(&parent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    // SAFETY: getpgrp only reads this test process's current process-group id.
    let live_group = unsafe { libc::getpgrp() };
    store
        .conn
        .execute(
            "UPDATE agents SET process_group_id=? WHERE id=?",
            (live_group, parent_id.as_str()),
        )
        .unwrap();
    let parent = store.get(&parent_id).unwrap();
    let error = store
        .admit(&home.request(), &home.config, &json!({}), Some(&parent))
        .unwrap_err();
    assert!(
        matches!(error, Error::Validation(message) if message.contains("still alive or unprovable"))
    );
}

/// Mirrors `test_resume.py::test_resume_makes_a_new_agent_that_inherits_and_attaches` and its one-child race.
#[test]
fn python_test_resume_lineage_has_one_concurrent_latest_child() {
    let home = common::Home::new();
    let (parent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut parent_store = home.store();
    parent_store.running(&parent_id, 42).unwrap();
    parent_store
        .runtime_session(&parent_id, "fixture-session")
        .unwrap();
    parent_store
        .finish(&parent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let parent = parent_store.get(&parent_id).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let path = home.path.clone();
        let config = home.config.clone();
        let request = home.request();
        let parent = parent.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            let mut store = agent_run_store::Store::open(&path).unwrap();
            barrier.wait();
            store.admit(&request, &config, &json!({}), Some(&parent))
        }));
    }
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::Conflict)))
            .count(),
        1
    );
    let children: i64 = parent_store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE parent_agent_id=?",
            [parent_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 1);
    let (root, sequence, inherited): (String, u32, String) = parent_store.conn.query_row("SELECT root_agent_id,sequence,resume_of_runtime_session_id FROM agents WHERE parent_agent_id=?", [parent_id.as_str()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(root, parent_id.as_str());
    assert_eq!(sequence, 2);
    assert_eq!(inherited, "fixture-session");
}

/// Mirrors the golden rows used by Python `test_state_outbox.py` and `test_resume.py` without mutating the fixture.
#[test]
fn python_golden_v16_lineage_and_outbox_rows_remain_readable() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/baseline/db/current-v16.sqlite"
    );
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let delivery: (String, String, u32) = connection
        .query_row(
            "SELECT id,state,attempts FROM deliveries WHERE id='delivery-succeeded'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        delivery,
        ("delivery-succeeded".into(), "delivered".into(), 2)
    );
    let lineage: (String, String, u32, String) = connection.query_row("SELECT id,root_agent_id,sequence,resume_of_runtime_session_id FROM agents WHERE parent_agent_id IS NOT NULL", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).unwrap();
    assert_eq!(lineage.1, "ag-20260101-000010-0000000001");
    assert_eq!(lineage.2, 2);
    assert_eq!(lineage.3, "session-0a");
}
