//! Contention and commit-boundary evidence on disposable WAL databases only.
mod common;
use agent_run_domain::{Error, domain::Outcome, error::PersistenceOperation};
use agent_run_platform::process::{Identity, OwnershipSnapshot};
use serde_json::json;
use std::time::{Duration, Instant};

/// An empty/held-wake command poll remains a read while another connection owns
/// the WAL writer. Eligible commands still claim atomically once after release.
#[test]
fn idle_command_poll_does_not_acquire_the_wal_writer() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let wake = store
        .enqueue(&id, "steer", &json!({"native_completion":true}))
        .unwrap()["command_id"]
        .as_i64()
        .unwrap();
    let lock = home.store();
    lock.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before = Instant::now();
    assert!(store.claim_command_for_turn(&id, false).unwrap().is_none());
    assert!(before.elapsed() < Duration::from_millis(200));
    assert!(store.conn.is_autocommit());
    lock.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        store.claim_command_for_turn(&id, true).unwrap().unwrap().0,
        wake
    );
    assert!(store.claim_command_for_turn(&id, true).unwrap().is_none());
    store
        .complete_command(&id, wake, &json!({"accepted":true}))
        .unwrap();
    let lock = home.store();
    lock.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(store.claim_command(&id).unwrap().is_none());
    lock.conn.execute_batch("ROLLBACK").unwrap();
}

/// Only BEGIN SQLITE_BUSY is retryable. Real trigger and commit-hook refusals
/// roll back completely, keep their distinct closed operations, and never replay.
/// Inert identities are structural evidence; no OS process is signalled.
#[test]
fn ownership_write_and_commit_failures_are_not_replayable() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let attempt = store.create_attempt(&id, "running", &json!({})).unwrap();
    let root = Identity {
        pid: 10,
        ppid: 2,
        group: 10,
        birth: 123.0,
        token: "inert-root".into(),
        zombie: false,
    };
    let snapshot = OwnershipSnapshot {
        leader: root.clone(),
        members: vec![root],
        descendants_observed: true,
    };
    store.conn.execute_batch("CREATE TEMP TRIGGER deny_members BEFORE INSERT ON process_members BEGIN SELECT RAISE(ABORT,'SOURCE_SECRET'); END").unwrap();
    let error = store
        .remember_processes("attempt", &attempt, &snapshot)
        .unwrap_err();
    assert!(matches!(
        &error,
        Error::Persistence {
            operation: PersistenceOperation::OwnershipWrite,
            ..
        }
    ));
    assert!(!error.retryable_ownership_acquire());
    assert!(!error.public().message.contains("SOURCE_SECRET"));
    assert!(store.conn.is_autocommit());
    assert!(
        store
            .remembered_processes("attempt", &attempt)
            .unwrap()
            .is_none()
    );
    store
        .conn
        .execute_batch("DROP TRIGGER deny_members")
        .unwrap();
    store.conn.commit_hook(Some(|| true)).unwrap();
    let error = store
        .remember_processes("attempt", &attempt, &snapshot)
        .unwrap_err();
    assert!(matches!(
        &error,
        Error::Persistence {
            operation: PersistenceOperation::OwnershipCommit,
            ..
        }
    ));
    assert!(!error.retryable_ownership_acquire());
    assert!(store.conn.is_autocommit());
    store.conn.commit_hook(None::<fn() -> bool>).unwrap();
    assert!(
        store
            .remembered_processes("attempt", &attempt)
            .unwrap()
            .is_none()
    );
    store
        .remember_processes("attempt", &attempt, &snapshot)
        .unwrap();
    store
        .remember_processes("attempt", &attempt, &snapshot)
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM process_members WHERE owner_id=?",
                [&attempt],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    for (operation, code, retry) in [
        (PersistenceOperation::OwnershipAcquire, 5, true),
        (PersistenceOperation::OwnershipAcquire, 6, false),
        (PersistenceOperation::OwnershipWrite, 5, false),
        (PersistenceOperation::OwnershipCommit, 5, false),
    ] {
        let error = Error::persistence(
            operation,
            Duration::from_millis(7),
            Error::Sql(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                Some("SOURCE_SECRET".into()),
            )),
        );
        assert_eq!(error.retryable_ownership_acquire(), retry);
        assert_eq!(error.public().kind, "StorageError");
        assert!(!error.public().message.contains("SOURCE_SECRET"));
    }
}

/// Ordinary transcript/session/event/terminal operations use their unchanged
/// connection allowance. A released writer commits each record once; a refused
/// terminal BEGIN leaves status/outbox unchanged and its safe operation labelled.
#[test]
fn ordinary_storage_boundaries_commit_once_and_preserve_failed_terminal_state() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .bind_orchestrator(
            &id,
            &agent_run_domain::domain::OrchestratorRef {
                transport: "codex_queue".into(),
                external_session_id: "synthetic-notice".into(),
                external_turn_id: None,
            },
            agent_run_domain::domain::now(),
        )
        .unwrap();
    let path = home.path.clone();
    let (send, receive) = std::sync::mpsc::sync_channel(0);
    let held = std::thread::spawn(move || {
        let lock = agent_run_store::Store::open(&path).unwrap();
        lock.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        send.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(450));
        lock.conn.execute_batch("ROLLBACK").unwrap();
    });
    receive.recv_timeout(Duration::from_secs(2)).unwrap();
    store
        .message(&id, "assistant", "single transcript", None, None)
        .unwrap();
    held.join().unwrap();
    store.runtime_session(&id, "native-single").unwrap();
    store
        .event(&id, "native_result_frame_v1", &json!({"index":1}))
        .unwrap();
    for table in ["messages", "events"] {
        assert_eq!(
            store
                .conn
                .query_row(
                    &format!(
                        "SELECT count(*) FROM {table} WHERE agent_id=? {}",
                        if table == "events" {
                            "AND kind='native_result_frame_v1'"
                        } else {
                            ""
                        }
                    ),
                    [id.as_str()],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }
    let lock = home.store();
    lock.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    // This test connection alone uses a short bound; production's ordinary five
    // seconds is separately exercised by the real-supervisor exhaustion fixture.
    store.conn.busy_timeout(Duration::from_millis(50)).unwrap();
    let error = store
        .finish(&id, &Outcome::failure("fixture_incomplete"), None, None)
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Persistence {
            operation: PersistenceOperation::TerminalCommit,
            attempts: 1,
            ..
        }
    ));
    assert!(!store.get(&id).unwrap().status.terminal());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM deliveries WHERE agent_id=?",
                [id.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    lock.conn.execute_batch("ROLLBACK").unwrap();
    store
        .finish(&id, &Outcome::failure("fixture_incomplete"), None, None)
        .unwrap();
    // Explicit caller retry on already terminal state is an existing no-op; no
    // automatic replay of ambiguous commits or native actions is introduced.
    store
        .finish(&id, &Outcome::failure("fixture_incomplete"), None, None)
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM deliveries WHERE agent_id=?",
                [id.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}
