//! Isolated library acceptance tests; these do not qualify application IPC.

use rusqlite::Connection;
use sqlite_access::{CallError, Database, Limits, Operation, WriterLease, transaction};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::Poll,
    time::{Duration, Instant},
};

/// Construct an isolated table with caller-selected admission limits.
fn database(limits: Limits) -> Database {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY)")
        .unwrap();
    Database::from_connection(connection, limits, None).unwrap()
}

/// Poll admission once without waiting for SQL completion; dropping then cancels only the caller.
async fn admit<F: Future>(future: Pin<&mut F>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

/// Accepted jobs survive cancelled waiters and retain both admission budgets until execution ends.
#[tokio::test]
async fn cancelled_queued_work_retains_permits_and_runs_once() {
    let db = database(Limits {
        jobs: 2,
        bytes: 8,
        job_bytes: 4,
    });
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let effects = std::sync::Arc::new(AtomicUsize::new(0));
    let first_effect = effects.clone();
    let first_db = db.clone();
    let first = tokio::spawn(async move {
        first_db
            .call(Operation::Write, 4, move |c| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                c.execute("INSERT INTO items VALUES(1)", [])?;
                first_effect.fetch_add(1, Ordering::SeqCst);
                Ok::<_, rusqlite::Error>(())
            })
            .await
    });
    started_rx.await.unwrap();
    let second_effect = effects.clone();
    let mut second = Box::pin(db.call(Operation::Write, 4, move |c| {
        assert_eq!(
            c.query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))?,
            1
        );
        c.execute("INSERT INTO items VALUES(2)", [])?;
        second_effect.fetch_add(1, Ordering::SeqCst);
        Ok::<_, rusqlite::Error>(())
    }));
    admit(second.as_mut()).await;
    drop(second);
    first.abort();
    assert!(matches!(
        db.call(Operation::Write, 1, |_| Ok::<_, rusqlite::Error>(()))
            .await,
        Err(CallError::Overloaded)
    ));
    release_tx.send(()).unwrap();
    db.close().await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    // close drained the two accepted calls, including both abandoned waiters.
}

/// Byte limits independently reject overload before a callback can have an effect.
#[tokio::test]
async fn bytes_and_payload_reject_before_enqueue() {
    let db = database(Limits {
        jobs: 8,
        bytes: 4,
        job_bytes: 4,
    });
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let owned = db.clone();
    let task = tokio::spawn(async move {
        owned
            .call(Operation::Write, 4, move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok::<_, rusqlite::Error>(())
            })
            .await
    });
    started_rx.await.unwrap();
    assert!(matches!(
        db.call(Operation::Write, 1, |_| -> rusqlite::Result<()> {
            panic!("overloaded callback ran")
        })
        .await,
        Err(CallError::<rusqlite::Error>::Overloaded)
    ));
    assert!(matches!(
        db.call(Operation::Write, 5, |_| -> rusqlite::Result<()> {
            panic!("oversized callback ran")
        })
        .await,
        Err(CallError::<rusqlite::Error>::PayloadTooLarge)
    ));
    release_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    db.close().await.unwrap();
}

/// Rollback removes all changes of a failed transaction and keeps the connection usable.
#[tokio::test]
async fn atomic_rollback_and_serial_order() {
    let db = database(Limits::default());
    let failure = db
        .call(Operation::Write, 1, |c| {
            transaction(c, |tx| {
                tx.execute("INSERT INTO items VALUES(1)", [])?;
                tx.execute("INSERT INTO items VALUES(1)", [])?;
                Ok(())
            })
        })
        .await;
    assert!(matches!(failure, Err(CallError::Work { .. })));
    let count: i64 = db
        .call(Operation::Read, 1, |c| {
            c.query_row("SELECT count(*) FROM items", [], |r| r.get(0))
        })
        .await
        .unwrap();
    assert_eq!(count, 0);
    for id in 1..=8 {
        db.call(Operation::Write, 1, move |c| {
            c.execute("INSERT INTO items VALUES(?1)", [id])
        })
        .await
        .unwrap();
    }
    let count: i64 = db
        .call(Operation::Read, 1, |c| {
            c.query_row("SELECT count(*) FROM items", [], |r| r.get(0))
        })
        .await
        .unwrap();
    assert_eq!(count, 8);
    db.close().await.unwrap();
}

/// Closing seals all clones immediately and waits for an already accepted job.
#[tokio::test]
async fn close_drains_and_seals_admission() {
    let db = database(Limits::default());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let owned = db.clone();
    let task = tokio::spawn(async move {
        owned
            .call(Operation::Write, 1, move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok::<_, rusqlite::Error>(())
            })
            .await
    });
    started_rx.await.unwrap();
    let mut close = Box::pin(db.close());
    admit(close.as_mut()).await;
    assert!(matches!(
        db.call(Operation::Read, 1, |_| Ok::<_, rusqlite::Error>(()))
            .await,
        Err(CallError::Closing)
    ));
    release_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    close.await.unwrap();
}

/// Signal an actual SQLite lock conflict without capturing callback arguments.
static BUSY_SIGNAL: Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>> = Mutex::new(None);

/// Notify the test on contention and permit bounded SQLite-level waiting, without replaying work.
fn signal_busy(attempt: i32) -> bool {
    if attempt != 0 {
        return false;
    }
    let signal = BUSY_SIGNAL.lock().unwrap();
    let Some((entered, released)) = signal.as_ref() else {
        return false;
    };
    entered.send(()).is_ok() && released.recv_timeout(Duration::from_secs(5)).is_ok()
}

/// A foreign lock released after actual contention permits the original callback to finish once.
#[tokio::test]
async fn busy_release_finishes_original_call_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let blocker = Connection::open(&path).unwrap();
    blocker
        .execute_batch("CREATE TABLE items(id INTEGER); BEGIN IMMEDIATE")
        .unwrap();
    let connection = Connection::open(&path).unwrap();
    connection.busy_handler(Some(signal_busy)).unwrap();
    let db = Database::from_connection(connection, Limits::default(), None).unwrap();
    let (signal_tx, signal_rx) = mpsc::channel();
    let (released_tx, released_rx) = mpsc::channel();
    *BUSY_SIGNAL.lock().unwrap() = Some((signal_tx, released_rx));
    let calls = std::sync::Arc::new(AtomicUsize::new(0));
    let measured = calls.clone();
    let owned = db.clone();
    let task = tokio::spawn(async move {
        owned
            .call(Operation::Write, 1, move |c| {
                measured.fetch_add(1, Ordering::SeqCst);
                c.execute("INSERT INTO items VALUES(1)", [])
            })
            .await
    });
    tokio::task::spawn_blocking(move || signal_rx.recv_timeout(Duration::from_secs(5)))
        .await
        .unwrap()
        .unwrap();
    blocker.execute_batch("COMMIT").unwrap();
    released_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    *BUSY_SIGNAL.lock().unwrap() = None;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        blocker
            .query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    db.close().await.unwrap();
}

/// Exhausted SQLite waiting reports BUSY, one attempt and no falsely committed row.
#[tokio::test]
async fn busy_over_budget_is_truthful_and_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let blocker = Connection::open(&path).unwrap();
    blocker
        .execute_batch("CREATE TABLE items(id INTEGER); BEGIN IMMEDIATE")
        .unwrap();
    let connection = Connection::open(&path).unwrap();
    connection.busy_timeout(Duration::from_millis(30)).unwrap();
    let db = Database::from_connection(connection, Limits::default(), None).unwrap();
    let before = Instant::now();
    match db
        .call(Operation::Write, 1, |c| {
            c.execute("INSERT INTO items VALUES(1)", [])
        })
        .await
    {
        Err(CallError::Work { diagnostic, .. }) => {
            assert_eq!(diagnostic.sqlite_code, Some(5));
            assert_eq!(diagnostic.attempts, 1);
            assert_eq!(diagnostic.operation, Operation::Write);
        }
        other => panic!("expected bounded busy failure: {other:?}"),
    }
    assert!(before.elapsed() < Duration::from_secs(2));
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        blocker
            .query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    db.close().await.unwrap();
}

/// Readers use a canonical private parent, observe committed WAL-only rows, refuse writes,
/// never create missing databases, and still reject a Unix leaf symlink.
#[tokio::test]
async fn reader_observes_wal_without_write_authority() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().canonicalize().unwrap();
    let path = home.join("state.db");
    let writer = Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(42)").unwrap();
    assert!(home.join("state.db-wal").metadata().unwrap().len() > 0);
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let db = Database::reader(&path, Duration::from_millis(50), Limits::default()).unwrap();
    let value: i64 = db
        .call(Operation::Read, 1, |c| {
            c.query_row("SELECT id FROM items", [], |r| r.get(0))
        })
        .await
        .unwrap();
    assert_eq!(value, 42);
    assert!(matches!(
        db.call(Operation::Write, 1, |c| c
            .execute("INSERT INTO items VALUES(43)", []))
            .await,
        Err(CallError::Work { .. })
    ));
    db.close().await.unwrap();
    writer.execute_batch("ROLLBACK").unwrap();
    let missing = home.join("missing.db");
    assert!(Database::reader(&missing, Duration::ZERO, Limits::default()).is_err());
    assert!(!missing.exists());
    #[cfg(unix)]
    {
        let link = home.join("reader-symlink.db");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(Database::reader(&link, Duration::ZERO, Limits::default()).is_err());
        assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
    }
}

/// Construction refuses inconsistent capacities and preexisting transactions before accepting SQL.
#[test]
fn construction_refuses_unsafe_state() {
    for limits in [
        Limits {
            jobs: 0,
            bytes: 8,
            job_bytes: 1,
        },
        Limits {
            jobs: 1,
            bytes: 0,
            job_bytes: 1,
        },
        Limits {
            jobs: 1,
            bytes: 8,
            job_bytes: 9,
        },
    ] {
        assert!(matches!(
            Database::from_connection(Connection::open_in_memory().unwrap(), limits, None),
            Err(CallError::InvalidLimits)
        ));
    }
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(matches!(
        Database::from_connection(connection, Limits::default(), None),
        Err(CallError::OpenTransaction)
    ));
}

/// Public failure text excludes SQL/application secrets while retaining a typed source and label.
#[tokio::test]
async fn diagnostics_do_not_format_application_content() {
    let db = database(Limits::default());
    let error = db
        .call(Operation::Maintenance, 1, |_| {
            Err::<(), _>(rusqlite::Error::InvalidParameterName(
                "SECRET_SENTINEL".into(),
            ))
        })
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("SECRET_SENTINEL"));
    match error {
        CallError::Work { source, diagnostic } => {
            assert!(source.to_string().contains("SECRET_SENTINEL"));
            assert_eq!(diagnostic.operation, Operation::Maintenance);
            assert_eq!(diagnostic.sqlite_code, None);
            assert_eq!(diagnostic.attempts, 1);
        }
        other => panic!("unexpected typed failure: {other:?}"),
    }
    db.close().await.unwrap();
}

/// Local parallel callers execute once without duplicate rows at measured concurrency levels.
#[tokio::test]
async fn bounded_concurrent_load() {
    for concurrency in [1usize, 8, 32, 64] {
        let db = database(Limits {
            jobs: concurrency,
            bytes: concurrency * 8,
            job_bytes: 8,
        });
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(concurrency));
        let mut tasks = Vec::new();
        for id in 0..concurrency {
            let owned = db.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                owned
                    .call(Operation::Write, 8, move |c| {
                        transaction(c, |tx| {
                            tx.execute("INSERT INTO items VALUES(?1)", [id as i64])
                        })
                    })
                    .await
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap().unwrap(), 1);
        }
        let count: i64 = db
            .call(Operation::Read, 1, |c| {
                c.query_row("SELECT count(*) FROM items", [], |r| r.get(0))
            })
            .await
            .unwrap();
        assert_eq!(count, concurrency as i64);
        db.close().await.unwrap();
    }
}

/// A separately invoked test process checks the lease without inheriting agent capabilities.
#[test]
fn lease_child() {
    let Some(path) = std::env::var_os("SQLITE_ACCESS_TEST_LOCK") else {
        return;
    };
    assert_eq!(
        WriterLease::acquire(std::path::Path::new(&path))
            .err()
            .unwrap()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
}

/// Kernel exclusion spans processes; unsafe lock inodes are refused rather than replaced.
#[cfg(unix)]
#[test]
fn lease_exclusion_and_unsafe_files() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writer.lock");
    let lease = WriterLease::acquire(&path).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .env_clear()
        .env("SQLITE_ACCESS_TEST_LOCK", &path)
        .arg("--exact")
        .arg("lease_child")
        .arg("--nocapture");
    assert!(child.status().unwrap().success());
    drop(lease);
    let lease = WriterLease::acquire(&path).unwrap();
    drop(lease);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(WriterLease::acquire(&path).is_err());
    let link = dir.path().join("symlink.lock");
    symlink(&path, &link).unwrap();
    assert!(WriterLease::acquire(&link).is_err());
}

/// An accidentally leaked transaction is rolled back whether the callback returned success or error.
#[tokio::test]
async fn leaked_transaction_never_reaches_neighbor() {
    for return_error in [false, true] {
        let db = database(Limits::default());
        let result = db
            .call(Operation::Write, 1, move |c| {
                c.execute_batch("BEGIN IMMEDIATE; INSERT INTO items VALUES(1)")?;
                if return_error {
                    Err(rusqlite::Error::InvalidQuery)
                } else {
                    Ok(())
                }
            })
            .await;
        assert!(matches!(result, Err(CallError::UnclosedTransaction)));
        let count = db
            .call(Operation::Read, 1, |c| {
                assert!(c.is_autocommit());
                c.query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
        db.close().await.unwrap();
    }
}

/// A panicking transaction is rolled back, suppresses an already queued neighbor, and caches failed close.
#[tokio::test]
async fn panic_poison_blocks_queued_neighbor_and_close_stays_failed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY)")
        .unwrap();
    let db = Database::from_connection(connection, Limits::default(), None).unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut failed = Box::pin(
        db.call(Operation::Write, 1, move |c| -> rusqlite::Result<()> {
            c.execute_batch("BEGIN IMMEDIATE; INSERT INTO items VALUES(1)")?;
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            panic!("synthetic callback panic")
        }),
    );
    admit(failed.as_mut()).await;
    started_rx.await.unwrap();
    let executed = std::sync::Arc::new(AtomicUsize::new(0));
    let counted = executed.clone();
    let mut neighbor = Box::pin(db.call(Operation::Write, 1, move |c| {
        counted.fetch_add(1, Ordering::SeqCst);
        c.execute("INSERT INTO items VALUES(2)", [])
    }));
    admit(neighbor.as_mut()).await;
    release_tx.send(()).unwrap();
    assert!(matches!(failed.await, Err(CallError::Panicked)));
    assert!(matches!(neighbor.await, Err(CallError::Poisoned)));
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    for _ in 0..2 {
        assert!(matches!(
            db.close().await,
            Err(CallError::CloseFailed { .. })
        ));
    }
    let observer = Connection::open(&path).unwrap();
    assert_eq!(
        observer
            .query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

/// Cancelling the waiting close future cannot cancel drain, and a later close returns the cached success.
#[tokio::test]
async fn cancelled_close_resumes_owned_drain() {
    let db = database(Limits::default());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut work = Box::pin(db.call(Operation::Write, 1, move |c| {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        c.execute("INSERT INTO items VALUES(1)", [])
    }));
    admit(work.as_mut()).await;
    started_rx.await.unwrap();
    let mut close = Box::pin(db.close());
    admit(close.as_mut()).await;
    drop(close);
    assert!(matches!(
        db.call(Operation::Read, 1, |_| Ok::<_, rusqlite::Error>(()))
            .await,
        Err(CallError::Closing)
    ));
    release_tx.send(()).unwrap();
    assert_eq!(work.await.unwrap(), 1);
    db.close().await.unwrap();
    db.close().await.unwrap();
}

/// The first polls enqueue A then B in that exact order, independent of task scheduling.
#[tokio::test]
async fn first_poll_preserves_admission_order() {
    let db = database(Limits::default());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let observed = std::sync::Arc::new(Mutex::new(Vec::new()));
    let mut blocker = Box::pin(db.call(Operation::Write, 1, move |_| {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        Ok::<_, rusqlite::Error>(())
    }));
    admit(blocker.as_mut()).await;
    started_rx.await.unwrap();
    let a_log = observed.clone();
    let mut a = Box::pin(db.call(Operation::Write, 1, move |_| {
        a_log.lock().unwrap().push('A');
        Ok::<_, rusqlite::Error>(())
    }));
    admit(a.as_mut()).await;
    let b_log = observed.clone();
    let mut b = Box::pin(db.call(Operation::Write, 1, move |_| {
        b_log.lock().unwrap().push('B');
        Ok::<_, rusqlite::Error>(())
    }));
    admit(b.as_mut()).await;
    release_tx.send(()).unwrap();
    blocker.await.unwrap();
    a.await.unwrap();
    b.await.unwrap();
    assert_eq!(*observed.lock().unwrap(), vec!['A', 'B']);
    db.close().await.unwrap();
}

/// Accepted SQL retains its writer lease after every application handle and waiter is dropped.
#[tokio::test]
async fn dropped_handles_keep_running_writer_lease() {
    let dir = tempfile::tempdir().unwrap();
    let lock = dir.path().join("writer.lock");
    let lease = WriterLease::acquire(&lock).unwrap();
    let db = Database::from_connection(
        Connection::open_in_memory().unwrap(),
        Limits::default(),
        Some(lease),
    )
    .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let mut work = Box::pin(db.call(Operation::Write, 1, move |_| {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        done_tx.send(()).unwrap();
        Ok::<_, rusqlite::Error>(())
    }));
    admit(work.as_mut()).await;
    started_rx.await.unwrap();
    drop(work);
    drop(db);
    assert_eq!(
        WriterLease::acquire(&lock).err().unwrap().kind(),
        std::io::ErrorKind::WouldBlock
    );
    release_tx.send(()).unwrap();
    done_rx.await.unwrap();
}
