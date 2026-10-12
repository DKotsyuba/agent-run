//! Idle delivery preflight must remain a read while unrelated SQLite writes are blocked.

/// No due schedule requires no config, broker connection, filesystem lease or SQL write.
#[tokio::test]
async fn empty_schedule_is_read_only_under_foreign_write_lock() {
    let home = tempfile::tempdir().unwrap();
    let store = agent_run_store::fixture::initialize(home.path()).unwrap();
    // A committed fixture-only write materializes WAL before the no-write snapshot.
    store.conn.execute_batch("CREATE TABLE fixture_wal_seed(id INTEGER PRIMARY KEY); INSERT INTO fixture_wal_seed VALUES(1)").unwrap();
    store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before_db = std::fs::read(home.path().join("state.db")).unwrap();
    let before_wal = std::fs::read(home.path().join("state.db-wal")).unwrap();
    let before: std::collections::BTreeSet<_> = std::fs::read_dir(home.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            agent_run_core::delivery::dispatch_once(home.path())
        )
        .await
        .expect("empty preflight never waits for writer lock")
        .unwrap(),
        0
    );
    store.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        std::fs::read(home.path().join("state.db")).unwrap(),
        before_db
    );
    assert_eq!(
        std::fs::read(home.path().join("state.db-wal")).unwrap(),
        before_wal
    );
    assert_eq!(
        std::fs::read_dir(home.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>(),
        before
    );
}
