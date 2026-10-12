//! F1 on disposable private homes: while a foreign connection holds the
//! database write lock, each read-only preflight (worker turn, command,
//! catalog registration) answers within 200 ms and commits nothing, as seen
//! by an independent observer's `data_version`; a preflight reporting no
//! change agrees with the in-transaction recheck of the worker turn unit.
use agent_run_domain::{domain::AgentId, worker::WorkerCatalogProof};
use agent_run_store::writer::{CatalogPreflight, StoreReader};
use rusqlite::Connection;
use serde_json::json;
use std::time::{Duration, Instant};

/// Static fixture capability with no live provider or credential material.
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Contract bound of one preflight answer (DOC-002 section 8).
const PREFLIGHT_BOUND: Duration = Duration::from_millis(200);

/// A home with one running explicit-finish execution, its owned attempt,
/// worker capability and running lifecycle, plus one pending steer.
fn running() -> (tempfile::TempDir, AgentId) {
    let home = tempfile::tempdir().unwrap();
    let mut store = agent_run_store::fixture::initialize(home.path()).unwrap();
    let run: AgentId = "ag-20261010-000000-0123456789".parse().unwrap();
    store
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) \
             VALUES(?,'codex','fixture','review','task','task','/tmp',?,'running',100,'fixture',?)",
            rusqlite::params![
                run.as_str(),
                json!({"runtime":"codex","model":"fixture","profile":"review","task":"task","workdir":"/tmp","explicit_finish":true}).to_string(),
                run.as_str()
            ],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',100,1)",
            [run.as_str()],
        )
        .unwrap();
    store
        .issue_worker_capability(&run, "attempt", TOKEN, 100.0)
        .unwrap();
    store
        .begin_worker_lifecycle(&run, "attempt", 100.0)
        .unwrap();
    store.enqueue(&run, "steer", &json!({"text": "x"})).unwrap();
    (home, run)
}

/// The observer handle's `data_version`, which changes only when another
/// connection commits to the database.
fn data_version(observer: &Connection) -> i64 {
    observer
        .pragma_query_value(None, "data_version", |row| row.get(0))
        .unwrap()
}

/// Runs one preflight and asserts it answered within the contract bound.
fn timed<T>(preflight: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let answer = preflight();
    let elapsed = started.elapsed();
    assert!(elapsed < PREFLIGHT_BOUND, "preflight took {elapsed:?}");
    answer
}

/// A catalog proof for the fixture attempt.
fn proof(run: &AgentId, token: &str, digest: String) -> WorkerCatalogProof {
    WorkerCatalogProof {
        run_id: run.clone(),
        attempt_id: "attempt".into(),
        token: token.into(),
        version: agent_run_domain::worker::POOL_CATALOG_VERSION,
        digest,
    }
}

/// Holds a foreign write lock around `preflights` and asserts the observer
/// saw no commit while they ran.
fn under_foreign_lock(home: &std::path::Path, preflights: impl FnOnce()) {
    let observer = Connection::open(home.join("state.db")).unwrap();
    let holder = Connection::open(home.join("state.db")).unwrap();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before = data_version(&observer);
    preflights();
    assert_eq!(data_version(&observer), before);
    holder.execute_batch("ROLLBACK").unwrap();
}

/// Every preflight answers within 200 ms under a held foreign write lock and
/// commits nothing, both before the catalog is registered (needed,
/// unauthenticated) and after (identical no-op, conflict).
#[test]
fn preflights_answer_under_a_held_write_lock_without_writing() {
    let (home, run) = running();
    let digest = agent_run_domain::worker::pool_catalog_digest();
    let reader = StoreReader::open(home.path()).unwrap();
    under_foreign_lock(home.path(), || {
        assert!(!timed(|| reader
            .worker_turn_pending(&run, "attempt", false)
            .unwrap()));
        assert!(timed(|| reader
            .worker_turn_pending(&run, "attempt", true)
            .unwrap()));
        assert!(!timed(|| reader
            .worker_turn_pending(&run, "other", true)
            .unwrap()));
        assert!(timed(|| reader.command_pending(&run, false).unwrap()));
        assert_eq!(
            timed(|| reader
                .worker_catalog_preflight(&proof(&run, TOKEN, digest.clone()), 101.0)
                .unwrap()),
            CatalogPreflight::NeedsWrite
        );
        assert_eq!(
            timed(|| reader
                .worker_catalog_preflight(&proof(&run, &"b".repeat(64), digest.clone()), 101.0)
                .unwrap()),
            CatalogPreflight::Unauthenticated
        );
    });
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    store
        .register_worker_catalog(&proof(&run, TOKEN, digest.clone()), 102.0)
        .unwrap();
    drop(store);
    under_foreign_lock(home.path(), || {
        assert_eq!(
            timed(|| reader
                .worker_catalog_preflight(&proof(&run, TOKEN, digest.clone()), 103.0)
                .unwrap()),
            CatalogPreflight::AlreadyRegistered
        );
        assert_eq!(
            timed(|| reader
                .worker_catalog_preflight(&proof(&run, TOKEN, "c".repeat(64)), 103.0)
                .unwrap()),
            CatalogPreflight::Conflict
        );
    });
}

/// The preflight and the in-transaction recheck agree: once the phase
/// matches, the worker turn unit itself reports no change.
#[test]
fn preflight_agrees_with_the_in_transaction_recheck() {
    let (home, run) = running();
    let reader = StoreReader::open(home.path()).unwrap();
    assert!(!reader.worker_turn_pending(&run, "attempt", false).unwrap());
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    assert!(!store.worker_turn(&run, false, 101.0).unwrap());
    assert!(reader.worker_turn_pending(&run, "attempt", true).unwrap());
    assert!(store.worker_turn(&run, true, 102.0).unwrap());
    assert!(!reader.worker_turn_pending(&run, "attempt", true).unwrap());
}
