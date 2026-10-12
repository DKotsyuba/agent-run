//! One-transaction apply semantics on disposable private homes: receipts
//! commit with their mutation, identical replays answer the stored outcome
//! without mutating, conflicting reuse, stale identities and out-of-order
//! ordinals are refused without effect, domain refusals are durable, scoped
//! operations on a missing execution leave no receipt, and supervisor scope is
//! checked inside the transaction; a busy acquire carries closed diagnostics;
//! recovery, reservations and maintenance keep their sole-writer bounds.
mod common;

use agent_run_domain::{
    domain::AgentId,
    storage::{
        MonoDeadline, OperationId, ProducerId, ProducerKind, RejectCode, RejectStage, SubmitResult,
        WriteAuthority,
    },
};
use agent_run_store::{
    ops::{AppendJournal, ClaimCommand, JournalFragment, RemoteOp, WriteEnvelope},
    writer::{StoreWriter, WriterLease},
};
use serde_json::json;

/// Current boot id and monotonic time from the platform clock.
fn clock() -> (String, u64) {
    agent_run_platform::monotonic::now().unwrap()
}

/// A producer of one Broker subsystem.
fn producer() -> ProducerId {
    ProducerId {
        kind: ProducerKind::Broker,
        scope: Some("test".into()),
        instance: "instance-1".into(),
    }
}

/// Builds an envelope with a fresh or given identity.
fn envelope(id: OperationId, ordinal: u64, op: RemoteOp) -> WriteEnvelope {
    let (boot, now) = clock();
    WriteEnvelope::new(
        id,
        producer(),
        ordinal,
        MonoDeadline {
            boot_id: boot,
            ns: now + 60_000_000_000,
        },
        op,
    )
    .unwrap()
}

/// A fresh operation identity of the current boot.
fn fresh_id() -> OperationId {
    let (boot, now) = clock();
    OperationId::new(&boot, now)
}

/// One journal event append for `id` with distinguishing `value`.
fn journal(id: &AgentId, value: &str) -> RemoteOp {
    RemoteOp::from(AppendJournal {
        execution: id.clone(),
        fragments: vec![JournalFragment::Event {
            kind: "test_event".into(),
            data: json!({ "value": value }),
            attempt: None,
        }],
    })
}

/// A home with one admitted agent and an open writer.
fn setup() -> (common::Home, StoreWriter<'static>, AgentId) {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let writer = StoreWriter::open(WriterLease::acquire_online(&home.path).unwrap()).unwrap();
    (home, writer, id)
}

/// Counts test events of one agent.
fn events(home: &common::Home, id: &AgentId) -> i64 {
    home.store()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='test_event'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap()
}

/// Counts receipt rows.
fn receipts(home: &common::Home) -> i64 {
    home.store()
        .conn
        .query_row("SELECT COUNT(*) FROM operation_receipts", [], |row| {
            row.get(0)
        })
        .unwrap()
}

/// The first apply commits mutation and receipt together; resubmitting the
/// identical envelope answers the same receipt with `replayed` and adds no
/// row.
#[test]
fn identical_replay_returns_the_stored_receipt_without_mutation() {
    let (home, mut writer, id) = setup();
    let first = envelope(fresh_id(), 1, journal(&id, "a"));
    let SubmitResult::Committed(receipt) = writer.apply(&WriteAuthority::Broker, &first) else {
        panic!("not committed");
    };
    assert!(!receipt.replayed);
    assert_eq!(events(&home, &id), 1);
    let SubmitResult::Committed(again) = writer.apply(&WriteAuthority::Broker, &first) else {
        panic!("not replayed");
    };
    assert!(again.replayed);
    assert_eq!(again.commit_seq, receipt.commit_seq);
    assert_eq!(again.result, receipt.result);
    assert_eq!(events(&home, &id), 1);
    assert_eq!(receipts(&home), 1);
}

/// A busy write lock held by an unknown connection is a non-durable
/// `StorageUnavailable` acquire refusal with constructed closed diagnostics
/// (the journal boundary, acquire phase, SQLite BUSY, measured wait, one
/// attempt); the writer degrades and nothing is stored.
#[test]
fn busy_acquire_reports_closed_diagnostics_and_degrades() {
    let (home, mut writer, id) = setup();
    let holder = rusqlite::Connection::open(home.path.join("state.db")).unwrap();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
    let result = writer.apply(
        &WriteAuthority::Broker,
        &envelope(fresh_id(), 1, journal(&id, "busy")),
    );
    holder.execute_batch("ROLLBACK").unwrap();
    let SubmitResult::Rejected(refusal) = result else {
        panic!("not rejected: {result:?}");
    };
    assert_eq!(refusal.code, RejectCode::StorageUnavailable);
    assert_eq!(refusal.stage, RejectStage::Acquire);
    assert!(!refusal.durable);
    let diag = refusal.persistence.unwrap();
    assert_eq!(
        diag.operation,
        agent_run_domain::error::PersistenceOperation::TranscriptAppend
    );
    assert_eq!(diag.phase, RejectStage::Acquire);
    assert_eq!(diag.sqlite_primary, Some(5));
    assert!(diag.wait_ms >= 200);
    assert_eq!(diag.attempts, 1);
    assert_eq!(
        writer.degraded(),
        Some(agent_run_store::ops::Degradation::UnknownWriter)
    );
    assert_eq!(receipts(&home), 0);
    assert_eq!(events(&home, &id), 0);
}

/// Reusing an identity for different content, from another authority, with
/// a non-increasing ordinal or from another boot is refused without effect.
#[test]
fn conflicts_order_and_stale_identities_are_refused_without_effect() {
    let (home, mut writer, id) = setup();
    let op_id = fresh_id();
    assert!(matches!(
        writer.apply(
            &WriteAuthority::Broker,
            &envelope(op_id.clone(), 5, journal(&id, "a"))
        ),
        SubmitResult::Committed(_)
    ));
    let code = |result: SubmitResult| match result {
        SubmitResult::Rejected(refusal) => {
            assert!(!refusal.durable);
            refusal.code
        }
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(
        code(writer.apply(
            &WriteAuthority::Broker,
            &envelope(op_id.clone(), 5, journal(&id, "b"))
        )),
        RejectCode::ReplayConflict
    );
    let cli = WriteAuthority::Cli {
        uid: 1,
        pid: 2,
        birth: 3.0,
        token: "t".into(),
    };
    assert_eq!(
        code(writer.apply(&cli, &envelope(op_id, 5, journal(&id, "a")))),
        RejectCode::Unauthorized
    );
    assert_eq!(
        code(writer.apply(
            &WriteAuthority::Broker,
            &envelope(fresh_id(), 5, journal(&id, "c"))
        )),
        RejectCode::OrderViolation
    );
    let stale = OperationId::new("another-boot", clock().1);
    assert_eq!(
        code(writer.apply(
            &WriteAuthority::Broker,
            &envelope(stale, 9, journal(&id, "d"))
        )),
        RejectCode::StaleOperation
    );
    assert_eq!(events(&home, &id), 1);
    assert_eq!(receipts(&home), 1);
}

/// A domain refusal rolls back the operation but stores a durable rejected
/// receipt; replay returns the same durable refusal.
#[test]
fn domain_refusals_are_durable_and_replayable() {
    let (home, mut writer, _) = setup();
    let ghost: AgentId = "ag-20261011-000000-00000000ee".parse().unwrap();
    let op = envelope(fresh_id(), 1, journal(&ghost, "x"));
    let first = writer.apply(&WriteAuthority::Broker, &op);
    let SubmitResult::Rejected(refusal) = &first else {
        panic!("unexpected {first:?}");
    };
    assert!(refusal.durable);
    assert_eq!(refusal.code, RejectCode::NotFound);
    assert_eq!(writer.apply(&WriteAuthority::Broker, &op), first);
    assert_eq!(receipts(&home), 1);
}

/// A scoped operation whose execution does not exist is refused as
/// non-durable `UnknownScope` and leaves no receipt.
#[test]
fn scoped_operation_on_missing_execution_leaves_no_receipt() {
    let (home, mut writer, _) = setup();
    let ghost: AgentId = "ag-20261011-000000-00000000ff".parse().unwrap();
    let supervisor = WriteAuthority::Supervisor {
        logical: ghost.clone(),
        execution: ghost.clone(),
        attempt: None,
        capability_sha256: "c".repeat(64),
        pid: 10,
        birth: 1.0,
        token: "t".into(),
    };
    let op = envelope(
        fresh_id(),
        1,
        RemoteOp::from(ClaimCommand {
            execution: ghost,
            allow_native_wake: true,
        }),
    );
    match writer.apply(&supervisor, &op) {
        SubmitResult::Rejected(refusal) => {
            assert_eq!(refusal.code, RejectCode::UnknownScope);
            assert!(!refusal.durable);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(receipts(&home), 0);
}

/// A supervisor whose capability is not recorded for the execution is a
/// durable `StaleAuthority` refusal and nothing is claimed.
#[test]
fn supervisor_scope_is_checked_inside_the_transaction() {
    let (home, mut writer, id) = setup();
    home.store()
        .enqueue(&id, "steer", &json!({"text": "x"}))
        .unwrap();
    let supervisor = WriteAuthority::Supervisor {
        logical: id.clone(),
        execution: id.clone(),
        attempt: None,
        capability_sha256: "c".repeat(64),
        pid: 10,
        birth: 1.0,
        token: "t".into(),
    };
    let op = envelope(
        fresh_id(),
        1,
        RemoteOp::from(ClaimCommand {
            execution: id.clone(),
            allow_native_wake: true,
        }),
    );
    match writer.apply(&supervisor, &op) {
        SubmitResult::Rejected(refusal) => {
            assert_eq!(refusal.code, RejectCode::StaleAuthority);
            assert!(refusal.durable);
        }
        other => panic!("unexpected {other:?}"),
    }
    let pending: i64 = home
        .store()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND state='pending'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 1);
}

/// A top-level `WriteTx` whose `COMMIT` fails on a deferred foreign key
/// returns the honest constraint error and is rolled back: no transaction or
/// write lock leaks, another connection can write, and the next independent
/// scope commits only its own rows.
#[test]
fn failed_deferred_commit_rolls_back_without_leaking_the_write_scope() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fk.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;\
         CREATE TABLE parent(id INTEGER PRIMARY KEY);\
         CREATE TABLE child(parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);",
    )
    .unwrap();
    let tx = agent_run_store::writer::WriteTx::begin(&conn).unwrap();
    tx.execute("INSERT INTO child VALUES(1)", []).unwrap();
    let error = tx.commit().unwrap_err();
    assert!(
        matches!(
            &error,
            agent_run_domain::Error::Sql(rusqlite::Error::SqliteFailure(failure, _))
                if failure.extended_code == 787
        ),
        "{error:?}"
    );
    assert!(conn.is_autocommit());
    let other = rusqlite::Connection::open(&path).unwrap();
    other.execute_batch("BEGIN IMMEDIATE; ROLLBACK").unwrap();
    let tx = agent_run_store::writer::WriteTx::begin(&conn).unwrap();
    tx.execute("INSERT INTO parent VALUES(7)", []).unwrap();
    tx.commit().unwrap();
    let counts: (i64, i64) = conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM parent),(SELECT COUNT(*) FROM child)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, (1, 0));
}

/// Counts this process's open descriptors whose path is exactly `path`.
fn open_descriptors(path: &std::path::Path) -> usize {
    let path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    (0..1024)
        .filter(|fd| {
            #[cfg(target_os = "macos")]
            {
                let mut buffer = vec![0u8; libc::PATH_MAX as usize];
                // SAFETY: F_GETPATH writes at most PATH_MAX bytes into the buffer.
                let found = unsafe { libc::fcntl(*fd, libc::F_GETPATH, buffer.as_mut_ptr()) } == 0;
                found
                    && std::ffi::CStr::from_bytes_until_nul(&buffer)
                        .map(|text| std::path::Path::new(text.to_str().unwrap_or("")) == path)
                        .unwrap_or(false)
            }
            #[cfg(not(target_os = "macos"))]
            {
                std::fs::read_link(format!("/proc/self/fd/{fd}")).is_ok_and(|target| target == path)
            }
        })
        .count()
}

/// Recovery closes the old read-write connection before opening a new one:
/// when the new open fails no read-write connection to the database
/// remains, and a successful recovery leaves exactly one.
#[test]
fn recover_closes_the_old_connection_before_opening() {
    let (home, mut writer, _id) = setup();
    let db = home.path.join("state.db");
    assert_eq!(open_descriptors(&db), 1);
    let aside = home.path.join("state.db.aside");
    std::fs::rename(&db, &aside).unwrap();
    std::os::unix::fs::symlink(&aside, &db).unwrap();
    assert!(writer.recover().is_err());
    assert_eq!(open_descriptors(&aside), 0);
    std::fs::remove_file(&db).unwrap();
    std::fs::rename(&aside, &db).unwrap();
    writer.recover().unwrap();
    assert_eq!(open_descriptors(&db), 1);
}

/// Service generations reserve writer capacity until their release is
/// confirmed (`stopped`); stopping and unknown generations still reserve.
#[test]
fn reservations_count_every_unreleased_generation() {
    let (home, writer, _id) = setup();
    let store = home.store();
    for state in [
        "starting",
        "ready",
        "unhealthy",
        "stopping",
        "unknown",
        "stopped",
    ] {
        store
            .conn
            .execute(
                "INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,created_at) \
                 VALUES(?1,?1,?2,'{}',?3,'{}',1)",
                rusqlite::params![format!("g-{state}"), "a".repeat(64), state],
            )
            .unwrap();
    }
    assert_eq!(writer.reservation_counts().unwrap().1, 5);
}

/// A truncating checkpoint blocked by a reader waits no longer than the
/// remaining deadline instead of the full writer busy wait.
#[test]
fn checkpoint_wait_is_bounded_by_the_deadline() {
    let (home, mut writer, id) = setup();
    writer.apply(
        &WriteAuthority::Broker,
        &envelope(fresh_id(), 1, journal(&id, "c")),
    );
    // Checkpoint everything first, so the measured checkpoint copies no
    // pages and only waits for the reader.
    let (boot, now) = clock();
    writer.maintain(
        agent_run_store::ops::MaintenanceOp::CheckpointWal { truncate: false },
        &MonoDeadline {
            boot_id: boot,
            ns: now + 5_000_000_000,
        },
    );
    let reader = rusqlite::Connection::open(home.path.join("state.db")).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _: i64 = reader
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    writer.apply(
        &WriteAuthority::Broker,
        &envelope(fresh_id(), 2, journal(&id, "d")),
    );
    let (boot, now) = clock();
    let started = std::time::Instant::now();
    let outcome = writer.maintain(
        agent_run_store::ops::MaintenanceOp::CheckpointWal { truncate: true },
        &MonoDeadline {
            boot_id: boot,
            ns: now + 20_000_000,
        },
    );
    let elapsed = started.elapsed();
    reader.execute_batch("ROLLBACK").unwrap();
    assert!(
        elapsed < std::time::Duration::from_millis(200),
        "{elapsed:?} {outcome:?}"
    );
}

/// An incremental vacuum with real free pages, blocked by a foreign write
/// lock, waits no longer than its remaining deadline instead of the full
/// writer busy wait and is not reported as vacuumed; once the lock is gone
/// the same vacuum reclaims pages.
#[test]
fn blocked_vacuum_wait_is_bounded_by_the_deadline() {
    let (home, mut writer, _id) = setup();
    let scratch = rusqlite::Connection::open(home.path.join("state.db")).unwrap();
    scratch
        .execute_batch(
            "CREATE TABLE scratch(x BLOB); \
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<200) \
             INSERT INTO scratch SELECT zeroblob(4096) FROM n; DELETE FROM scratch;",
        )
        .unwrap();
    let free: i64 = scratch
        .pragma_query_value(None, "freelist_count", |row| row.get(0))
        .unwrap();
    assert!(free > 0);
    scratch.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (boot, now) = clock();
    let started = std::time::Instant::now();
    let blocked = writer.maintain(
        agent_run_store::ops::MaintenanceOp::VacuumIncremental { pages: 100 },
        &MonoDeadline {
            boot_id: boot,
            ns: now + 20_000_000,
        },
    );
    let elapsed = started.elapsed();
    scratch.execute_batch("ROLLBACK").unwrap();
    assert!(
        elapsed < std::time::Duration::from_millis(200),
        "{elapsed:?} {blocked:?}"
    );
    assert!(
        !matches!(
            blocked,
            agent_run_domain::storage::MaintenanceOutcome::Vacuumed { .. }
        ),
        "{blocked:?}"
    );
    let (boot, now) = clock();
    let reclaimed = writer.maintain(
        agent_run_store::ops::MaintenanceOp::VacuumIncremental { pages: 100 },
        &MonoDeadline {
            boot_id: boot,
            ns: now + 5_000_000_000,
        },
    );
    assert_eq!(
        reclaimed,
        agent_run_domain::storage::MaintenanceOutcome::Vacuumed { reclaimed: true }
    );
}

/// A maintenance job whose connection settlement fails degrades the writer
/// and claims no outcome; an injected reservation read failure surfaces as
/// an error once and the next read succeeds.
#[test]
fn injected_settlement_and_reservation_faults_are_reported() {
    let (_home, mut writer, _id) = setup();
    writer.inject_settle_failure();
    let (boot, now) = clock();
    let outcome = writer.maintain(
        agent_run_store::ops::MaintenanceOp::CheckpointWal { truncate: false },
        &MonoDeadline {
            boot_id: boot,
            ns: now + 5_000_000_000,
        },
    );
    assert!(
        matches!(
            outcome,
            agent_run_domain::storage::MaintenanceOutcome::Refused(_)
        ),
        "{outcome:?}"
    );
    assert_eq!(
        writer.degraded(),
        Some(agent_run_store::ops::Degradation::RollbackUncertain)
    );
    writer.inject_reservation_read_failure();
    assert!(writer.reservation_scopes().is_err());
    assert!(writer.reservation_scopes().is_ok());
}

/// A predecoded envelope can drop its decoded graph, and applying an owned
/// envelope commits like the borrowed path.
#[test]
fn predecoded_cache_strips_and_owned_apply_commits() {
    let (home, mut writer, id) = setup();
    let mut envelope = envelope(fresh_id(), 1, journal(&id, "owned"));
    envelope.predecode().unwrap();
    assert!(envelope.is_decoded());
    envelope.strip_decoded();
    assert!(!envelope.is_decoded());
    envelope.predecode().unwrap();
    assert!(matches!(
        writer.apply_owned(&WriteAuthority::Broker, envelope),
        SubmitResult::Committed(_)
    ));
    assert_eq!(events(&home, &id), 1);
}

/// Reservation scopes list every ownership-active attempt with its
/// execution, live generations and registered probes.
#[test]
fn reservation_scopes_carry_attempt_and_probe_owners() {
    let (home, writer, id) = setup();
    home.store()
        .conn
        .execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('owned',?,1,'running','{}',1,1)",
            [id.as_str()],
        )
        .unwrap();
    let scopes = writer.reservation_scopes().unwrap();
    assert!(scopes.attempts.contains(&(id, "owned".to_owned())));
    assert!(scopes.generations.is_empty() && scopes.probes.is_empty());
}
