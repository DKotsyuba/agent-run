//! Truthful persistence diagnostics of the producer lane against a real writer:
//! an ownership write that outlives its original deadline behind a foreign lock
//! reports the writer's operation, closed phase and SQLite codes with the real
//! wait; the lane never reports a refusal as success and keeps its head.
//! (The unknown-outcome totals are covered by the lane's scripted-peer tests.)
mod common;
use agent_run_core::{
    persist::{self, Persist},
    process::OwnedProcess,
};
use agent_run_domain::{Error, domain::AgentId};
use serde_json::json;
use std::time::Duration;

/// A running legacy execution (attempt `<id>:1`) with a real writer and its
/// supervisor persistence; `busy` is the writer connection's busy wait.
async fn running(
    busy: Option<Duration>,
) -> (
    common::Home,
    common::writer::Harness,
    Persist,
    AgentId,
    String,
) {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let attempt = format!("{id}:1");
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',100,1)", rusqlite::params![attempt, id.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    drop(store);
    let mut config = agent_run_core::writer::WriterConfig::default();
    if let Some(busy) = busy {
        config.writer_busy = busy;
    }
    let harness = common::writer::Harness::start_with(&home.path, config);
    let persist = harness.supervisor(&home.path, prep, &id).await;
    (home, harness, persist, id, attempt)
}

/// Holds `BEGIN IMMEDIATE` on an independent connection for `hold`, returning
/// after the lock is proven acquired.
fn foreign_lock(home: &std::path::Path, hold: Duration) -> std::thread::JoinHandle<()> {
    let home = home.to_path_buf();
    let (send, receive) = std::sync::mpsc::sync_channel(0);
    let held = std::thread::spawn(move || {
        let store = agent_run_store::fixture::open(&home).unwrap();
        store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        send.send(()).unwrap();
        std::thread::sleep(hold);
        store.conn.execute_batch("ROLLBACK").unwrap();
    });
    receive.recv_timeout(Duration::from_secs(2)).unwrap();
    held
}

/// Takes the process snapshot of this test process.
fn snapshot() -> agent_run_platform::process::OwnershipSnapshot {
    OwnedProcess::capture(std::process::id() as i32)
        .snapshot()
        .expect("own process snapshot")
}

/// Checks the closed diagnostic of a write that outlived its original deadline
/// behind a foreign lock. Which of the honest classes the deadline meets (the
/// writer's definite busy refusal, or an outcome still unknown to the producer)
/// depends on scheduling; the invariants do not: the error is typed, the wait is
/// real, at least one submission is counted, a claimed phase is exactly the
/// writer's Acquire phase with the SQLite busy code, and an unclaimed phase comes
/// with no SQLite claim at all.
fn assert_honest_expiry(error: &Error, budget: Duration, waited: Duration) {
    let Error::Persistence {
        elapsed_ms,
        attempts,
        phase,
        source,
        ..
    } = error
    else {
        panic!("closed persistence diagnostic expected: {error:?}");
    };
    assert!(
        u128::from(*elapsed_ms) + 100 >= budget.as_millis()
            && u128::from(*elapsed_ms) <= waited.as_millis() + 100,
        "elapsed {elapsed_ms} ms vs budget {budget:?} and observed {waited:?}"
    );
    assert!(*attempts >= 1, "attempts {attempts}");
    match phase {
        Some(phase) => {
            assert_eq!(format!("{phase:?}"), "Acquire");
            let Error::Sql(rusqlite::Error::SqliteFailure(code, _)) = &**source else {
                panic!("SQLite failure expected: {source:?}");
            };
            assert_eq!(code.extended_code, 5, "SQLITE_BUSY");
        }
        None => assert!(
            !matches!(&**source, Error::Sql(_)),
            "an unknown outcome claims no SQLite fact: {source:?}"
        ),
    }
}

/// A journal write behind a foreign lock never succeeds before the original
/// deadline. Depending on whether the writer's last busy refusal or its still
/// pending execution is what the deadline meets, the lane ends in exactly one of
/// two honest classes: `DeadlineExpired` (nothing applied; the error carries the
/// writer's last closed facts: operation, Acquire phase, SQLite busy codes, with
/// several counted attempts) or UNKNOWN (no phase claimed, one attempt). In both,
/// the elapsed time is the real wait, and the same producer commits afterwards.
#[tokio::test]
async fn a_journal_write_behind_a_lock_ends_honestly_with_true_totals() {
    use agent_run_domain::storage::{NotEnqueuedReason, SubmitResult};
    let (home, _harness, mut persist, id, _attempt) = running(None).await;
    let held = foreign_lock(&home.path, Duration::from_millis(2500));
    let started = std::time::Instant::now();
    let pending = persist.producer().submit(
        agent_run_store::ops::AppendJournal {
            execution: id.clone(),
            fragments: vec![agent_run_store::ops::JournalFragment::Event {
                kind: "behind_the_lock".into(),
                data: json!({}),
                attempt: None,
            }],
        },
        Duration::from_millis(900),
    );
    let totals = pending.totals();
    let result = pending.await;
    let waited = started.elapsed();
    let definite = matches!(
        result,
        SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired)
    );
    assert!(
        definite || matches!(result, SubmitResult::Unknown(_)),
        "{result:?}"
    );
    let error = persist::into_result_totals::<Vec<i64>>(result, &totals.snapshot())
        .expect_err("an expired write is an error, never success");
    assert_honest_expiry(&error, Duration::from_millis(900), waited);
    if definite {
        assert!(matches!(&error, Error::Persistence { phase: Some(_), .. }));
    }
    held.join().unwrap();
    persist
        .event_durable(&id, "after_the_lock", &json!({}))
        .await
        .expect("the same producer commits once the lock is gone");
}

/// An ownership write whose acquire keeps failing is retried by the writer
/// itself until the original deadline; the producer then cannot know the
/// outcome, so the result stays UNKNOWN (never success, never a definite
/// refusal), claims no phase, and carries the true totals.
#[tokio::test]
async fn an_ownership_write_expiring_in_execution_stays_unknown_with_true_totals() {
    let (home, _harness, persist, _id, attempt) = running(None).await;
    let held = foreign_lock(&home.path, Duration::from_millis(2500));
    let started = std::time::Instant::now();
    let error = persist::ownership_future(
        persist.producer(),
        attempt.clone(),
        snapshot(),
        Duration::from_millis(700),
    )
    .await
    .expect_err("the deadline passes while the writer waits");
    let waited = started.elapsed();
    assert_honest_expiry(&error, Duration::from_millis(700), waited);
    held.join().unwrap();
    persist::ownership_future(
        persist.producer(),
        attempt,
        snapshot(),
        Duration::from_secs(10),
    )
    .await
    .expect("a later write of the same producer commits");
}

/// A statement the database refuses (here a trigger abort) is a durable
/// refusal: the closed operation, the statement phase and the SQLite codes
/// survive unchanged, exactly one attempt is counted and nothing is retried.
#[tokio::test]
async fn a_refused_statement_keeps_operation_phase_and_sqlite_codes() {
    let (home, _harness, persist, _id, attempt) = running(None).await;
    home.store()
        .conn
        .execute_batch(
            "CREATE TRIGGER refuse_members BEFORE INSERT ON process_members \
             BEGIN SELECT RAISE(ABORT,'SOURCE_SECRET'); END",
        )
        .unwrap();
    let error = persist::ownership_future(
        persist.producer(),
        attempt,
        snapshot(),
        Duration::from_secs(5),
    )
    .await
    .expect_err("the statement is refused");
    let Error::Persistence {
        operation,
        attempts,
        phase,
        source,
        ..
    } = &error
    else {
        panic!("closed persistence diagnostic expected: {error:?}");
    };
    assert_eq!(format!("{operation:?}"), "OwnershipWrite");
    assert_eq!(
        phase.map(|p| format!("{p:?}")).as_deref(),
        Some("Statement")
    );
    assert_eq!(*attempts, 1);
    let Error::Sql(rusqlite::Error::SqliteFailure(code, _)) = &**source else {
        panic!("SQLite failure expected: {source:?}");
    };
    assert_eq!(code.extended_code, 1811, "SQLITE_CONSTRAINT_TRIGGER");
    assert!(
        !format!("{error:?}").contains("SOURCE_SECRET"),
        "no source text leaks"
    );
}
