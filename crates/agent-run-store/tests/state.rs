mod common;
use agent_run_core::service::Service;
use agent_run_domain::{
    domain::{OrchestratorRef, Outcome, Status},
    Error,
};
use agent_run_platform::{fs, verify};
use agent_run_store::Store;
use serde_json::json;
use std::{
    path::Path,
    sync::{Arc, Barrier, Mutex},
    thread,
};

/// Mirrors `test_state_db.py::test_fresh_init_and_reopen_apply_schema_pragmas_and_private_modes`.
///
/// Initialization and a second connection retain the durable schema and WAL safety settings.
#[test]
fn schema_initialization_and_reopen() {
    let h = common::Home::new();
    let a = h.store().health().unwrap();
    assert_eq!(a["ok"], true);
    assert_eq!(a["schema_version"], agent_run_store::VERSION);
    assert_eq!(a["tables"], 26);
    assert_eq!(h.store().health().unwrap()["integrity"], "ok");
    let store = h.store();
    assert_eq!(
        store
            .conn
            .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    assert_eq!(
        store
            .conn
            .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .conn
            .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
}
#[test]
fn admission_is_durable_and_replay_is_exact() {
    let h = common::Home::new();
    let mut r = h.request();
    r.request_id = Some("replay".into());
    let (id, created) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    assert!(created);
    assert_eq!(h.store().get(&id).unwrap().status, Status::Starting);
    let (same, created) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    assert!(!created);
    assert_eq!(same, id);
    r.task = "different".into();
    assert!(matches!(
        h.store().admit(&r, &h.config, &json!({}), None),
        Err(Error::Conflict)
    ));
}
#[test]
fn request_ids_are_scoped_to_orchestrator() {
    let h = common::Home::new();
    let mut r = h.request();
    r.request_id = Some("same".into());
    let (a, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    r.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: None,
    });
    let (b, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    assert_ne!(a, b);
}
#[test]
fn concurrency_limit_does_not_create_an_extra_row() {
    let h = common::Home::new();
    let mut config = h.config.clone();
    config.core.max_active_agents = 1;
    h.store()
        .admit(&h.request(), &config, &json!({}), None)
        .unwrap();
    assert!(matches!(
        h.store().admit(&h.request(), &config, &json!({}), None),
        Err(Error::Capacity)
    ));
    assert_eq!(h.store().list(false, 0, 100, None).unwrap().1, 1);
}
#[test]
fn terminal_success_requires_valid_artifact() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    h.store().running(&id, 42).unwrap();
    let outcome = Outcome::success(None);
    assert!(h.store().finish(&id, &outcome, None, None).is_err());
    let root = h.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "Complete answer").unwrap();
    std::fs::write(&proof.path, "Tampered answer").unwrap();
    assert!(h.store().finish(&id, &outcome, Some(&proof), None).is_err());
    assert_eq!(h.store().get(&id).unwrap().status, Status::Running);
}
#[test]
fn success_is_durable_and_terminal_state_is_immutable() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    h.store().running(&id, 42).unwrap();
    let root = h.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "Answer").unwrap();
    h.store()
        .finish(
            &id,
            &Outcome::success(Some("native-session".into())),
            Some(&proof),
            None,
        )
        .unwrap();
    h.store()
        .finish(&id, &Outcome::failure("late_error"), None, None)
        .unwrap();
    let row = h.store().get(&id).unwrap();
    assert_eq!(row.status, Status::Succeeded);
    assert_eq!(row.answer_bytes, Some(6));
    assert_eq!(row.runtime_session_id.as_deref(), Some("native-session"));
}
#[test]
fn commands_are_claimed_once_and_completed() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    h.store().enqueue(&id, "cancel", &json!({})).unwrap();
    assert!(h.store().cancel_pending(&id).unwrap());
    let (command_id, _, _) = h.store().claim_command(&id).unwrap().unwrap();
    assert!(h.store().claim_command(&id).unwrap().is_none());
    h.store()
        .complete_command(&id, command_id, &json!({"accepted":true}))
        .unwrap();
    assert!(!h.store().cancel_pending(&id).unwrap());
}

/// Mirrors `tests/test_supervisor.py::StoreEventSinkThreadingTests::test_sink_write_from_a_non_owner_thread_lands_durably`
#[test]
fn event_write_from_a_non_owner_thread_lands_durably() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    let path = h.path.clone();
    let worker_id = id.clone();
    thread::spawn(move || {
        Store::open(&path)
            .unwrap()
            .event(&worker_id, "worker_event", &json!({"from":"worker"}))
            .unwrap();
    })
    .join()
    .unwrap();
    assert_eq!(
        h.store().last_event(&id, "worker_event").unwrap().unwrap()["from"],
        "worker"
    );
}

/// Mirrors `tests/test_supervisor.py::StoreEventSinkThreadingTests::test_sink_message_and_session_from_a_non_owner_thread_land_durably`
#[test]
fn message_and_session_from_a_non_owner_thread_land_durably() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    let path = h.path.clone();
    let worker_id = id.clone();
    thread::spawn(move || {
        let mut store = Store::open(&path).unwrap();
        store
            .runtime_session(&worker_id, "runtime-session-1")
            .unwrap();
        store
            .message(&worker_id, "assistant", "hello", None, None)
            .unwrap();
    })
    .join()
    .unwrap();
    let store = h.store();
    assert_eq!(
        store.get(&id).unwrap().runtime_session_id.as_deref(),
        Some("runtime-session-1")
    );
    assert_eq!(
        store.last_event(&id, "runtime_session").unwrap().unwrap()["id"],
        "runtime-session-1"
    );
    assert_eq!(
        store.transcript(&id, 0, 10).unwrap()["messages"][0]["content"],
        "hello"
    );
}

/// Mirrors `tests/test_supervisor.py::StoreEventSinkThreadingTests::test_concurrent_sink_writes_from_two_threads_all_land_without_error`
#[test]
fn concurrent_event_writes_from_two_threads_all_land_without_error() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let workers = (0..2)
        .map(|index| {
            let path = h.path.clone();
            let worker_id = id.clone();
            let barrier = Arc::clone(&barrier);
            let errors = Arc::clone(&errors);
            thread::spawn(move || {
                barrier.wait();
                if let Err(error) = Store::open(&path).and_then(|store| {
                    store.event(
                        &worker_id,
                        &format!("concurrent_event_{index}"),
                        &json!({"index":index}),
                    )
                }) {
                    errors.lock().unwrap().push(error.to_string());
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(errors.lock().unwrap().is_empty());
    let store = h.store();
    for index in 0..2 {
        let count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind=?",
                rusqlite::params![id.as_str(), format!("concurrent_event_{index}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }
}
#[test]
fn message_cursors_preserve_order_whitespace_and_repetitions() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    for text in ["x", " ", "x", "\n"] {
        h.store()
            .message(&id, "assistant", text, None, None)
            .unwrap();
    }
    let first = h.store().transcript(&id, 0, 2).unwrap();
    assert_eq!(first["complete"], false);
    let next = first["next_cursor"].as_i64().unwrap();
    let second = h.store().transcript(&id, next, 2).unwrap();
    assert_eq!(second["complete"], true);
    let all = first["messages"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["messages"].as_array().unwrap())
        .map(|m| m["content"].as_str().unwrap())
        .collect::<String>();
    assert_eq!(all, "x x\n");
}
/// Terminal runs without a session have no delivery row; bound runs create one pending notice.
#[test]
fn terminal_runs_create_pending_or_waiting_binding_deliveries() {
    let h = common::Home::new();
    let mut r = h.request();
    let (a, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    h.store()
        .finish(&a, &Outcome::failure("test"), None, None)
        .unwrap();
    assert_eq!(
        h.store().delivery_status(&a).unwrap()["state"],
        "not_created"
    );
    r.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: None,
    });
    let (b, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    h.store()
        .finish(&b, &Outcome::failure("test"), None, None)
        .unwrap();
    assert_eq!(h.store().delivery_status(&b).unwrap()["state"], "pending");
}
#[test]
fn resume_lineage_has_one_child_and_a_stable_root() {
    let h = common::Home::new();
    let r = h.request();
    let (id, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    h.store().runtime_session(&id, "native-session").unwrap();
    h.store()
        .finish(&id, &Outcome::failure("test"), None, None)
        .unwrap();
    let parent = h.store().get(&id).unwrap();
    let (child, _) = h
        .store()
        .admit(&r, &h.config, &json!({}), Some(&parent))
        .unwrap();
    let row = h.store().get(&child).unwrap();
    assert_eq!(row.parent_agent_id, Some(id.clone()));
    assert_eq!(row.root_agent_id, id);
    assert_eq!(row.sequence, 2);
    assert!(matches!(
        h.store().admit(&r, &h.config, &json!({}), Some(&parent)),
        Err(Error::Conflict)
    ));
}
#[test]
fn backup_is_openable_and_never_overwrites_an_existing_file() {
    let h = common::Home::new();
    let dst = h.path.join("backup.db");
    let mut store = h.store();
    store
        .conn
        .pragma_update(None, "wal_autocheckpoint", 1_000_000_i64)
        .unwrap();
    let (id, _) = store
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    store
        .message(&id, "assistant", "committed WAL page", None, None)
        .unwrap();
    let wal = h.path.join("state.db-wal");
    assert!(
        wal.is_file() && std::fs::metadata(&wal).unwrap().len() > 0,
        "the committed fixture row must still have a WAL page"
    );
    store.backup(&dst).unwrap();
    assert!(store.backup(&dst).is_err());
    let db = rusqlite::Connection::open(dst).unwrap();
    db.pragma_update(None, "foreign_keys", true).unwrap();
    assert_eq!(
        db.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))
            .unwrap(),
        agent_run_store::VERSION
    );
    assert_eq!(
        db.query_row(
            "SELECT content FROM messages WHERE agent_id=?",
            [id.as_str()],
            |r| { r.get::<_, String>(0) }
        )
        .unwrap(),
        "committed WAL page"
    );
    assert_eq!(
        db.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    let foreign_key_rows = db
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .query_map([], |_| Ok(()))
        .unwrap()
        .count();
    assert_eq!(foreign_key_rows, 0);
}
/// Mirrors `test_state_db.py::test_invalid_and_newer_versions_refuse_without_schema_mutation`.
///
/// A newer schema version is rejected without being changed by this binary.
#[test]
fn newer_database_version_is_refused_without_upgrade() {
    let h = common::Home::new();
    let db = rusqlite::Connection::open(h.path.join("state.db")).unwrap();
    db.pragma_update(None, "user_version", agent_run_store::VERSION + 1)
        .unwrap();
    drop(db);
    assert!(Store::open(&h.path).is_err());
    let db = rusqlite::Connection::open(h.path.join("state.db")).unwrap();
    assert_eq!(
        db.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))
            .unwrap(),
        agent_run_store::VERSION + 1
    );
}
#[test]
/// Opens a real historical schema-15 fixture and upgrades it through every current migration.
fn legacy_database_version_is_migrated_on_open() {
    let h = common::Home::new();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/baseline/db/historical-v15.sqlite");
    std::fs::copy(fixture, h.path.join("state.db")).unwrap();
    let store = Store::open(&h.path).unwrap();
    assert_eq!(
        store.health().unwrap()["schema_version"],
        agent_run_store::VERSION
    );
}
#[tokio::test]
async fn start_replay_is_independent_of_later_configuration_edits() {
    let h = common::Home::new();
    let mut original = h.request();
    original.request_id = Some("stable-replay".into());
    let fingerprint =
        fs::sha256(&fs::canonical_json(&serde_json::to_value(&original).unwrap()).unwrap());
    let mut effective = original.clone();
    effective.timeout_seconds = Some(480.0);
    let (id, _) = h
        .store()
        .admit(
            &effective,
            &h.config,
            &json!({"replay_request_sha256":fingerprint}),
            None,
        )
        .unwrap();
    std::fs::write(h.path.join("config.toml"), "invalid mutable configuration").unwrap();
    let replay = Service::new(h.path.clone()).start(original).await.unwrap();
    assert_eq!(replay["agent_id"], id.as_str());
    assert_eq!(replay["created"], false);
}
#[tokio::test]
async fn changed_start_payload_cannot_reuse_a_recorded_fingerprint() {
    let h = common::Home::new();
    let mut original = h.request();
    original.request_id = Some("stable-replay".into());
    let fingerprint =
        fs::sha256(&fs::canonical_json(&serde_json::to_value(&original).unwrap()).unwrap());
    h.store()
        .admit(
            &original,
            &h.config,
            &json!({"replay_request_sha256":fingerprint}),
            None,
        )
        .unwrap();
    original.task = "different task".into();
    assert!(matches!(
        Service::new(h.path.clone()).start(original).await,
        Err(Error::Conflict)
    ));
}

/// The nine tables schema v1 created, used to forge structurally wrong stores.
const V1_TABLES: [&str; 9] = [
    "orchestrator_sessions",
    "agents",
    "attempts",
    "events",
    "messages",
    "commands",
    "deliveries",
    "capacity_samples",
    "context_receipts",
];

/// Mirrors `tests/test_state_db.py::StateDatabaseTests::test_open_refuses_missing_or_incomplete_v1_database`
///
/// Opening is not a creation path: neither an absent store nor one that only
/// carries a version stamp may be silently promoted into a usable schema.
#[test]
fn open_refuses_missing_or_incomplete_v1_database() {
    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().to_path_buf();
    assert!(
        Store::open(&home).is_err(),
        "a missing state database must be refused, not created"
    );
    let stamped = rusqlite::Connection::open(home.join("state.db")).unwrap();
    stamped.pragma_update(None, "user_version", 1).unwrap();
    drop(stamped);
    assert!(
        Store::open(&home).is_err(),
        "a versioned but tableless database must be refused"
    );
}

// Protects the Rust store boundary against a foreign SQLite file: Python has
// no separate database-identity marker, so structural refusal is the identity
// proof and the unrelated table must survive untouched.
#[test]
fn open_refuses_a_foreign_database_without_reinitializing_it() {
    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().to_path_buf();
    let database = home.join("state.db");
    let foreign = rusqlite::Connection::open(&database).unwrap();
    foreign
        .execute_batch(
            "CREATE TABLE foreign_records(value TEXT); \
             INSERT INTO foreign_records VALUES('keep-me'); \
             PRAGMA user_version=1",
        )
        .unwrap();
    drop(foreign);

    assert!(Store::open(&home).is_err());
    let untouched = rusqlite::Connection::open(&database).unwrap();
    assert_eq!(
        untouched
            .query_row("SELECT value FROM foreign_records", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "keep-me"
    );
    assert_eq!(
        untouched
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        untouched
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

/// Mirrors `tests/test_state_db.py::StateDatabaseTests::test_open_refuses_v1_tables_with_corrupt_column_shapes`
///
/// Table names alone do not prove a store is ours: a v1 stamp over tables whose
/// columns and primary keys do not match the shipped schema is refused instead
/// of being migrated into silent corruption.
#[test]
fn open_refuses_v1_tables_with_corrupt_column_shapes() {
    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().to_path_buf();
    let corrupt = rusqlite::Connection::open(home.join("state.db")).unwrap();
    for table in V1_TABLES {
        corrupt
            .execute_batch(&format!(
                "CREATE TABLE \"{table}\" (wrong BLOB PRIMARY KEY)"
            ))
            .unwrap();
    }
    corrupt.pragma_update(None, "user_version", 1).unwrap();
    drop(corrupt);
    let error = Store::open(&home)
        .err()
        .expect("corrupt column shapes must be refused")
        .to_string();
    assert!(
        error.contains("columns or primary keys") || error.contains("foreign key"),
        "refusal must name the structural defect, got: {error}"
    );
}

/// Mirrors `tests/test_state_db.py::StateDatabaseTests::test_reopen_tolerates_chmod_denied_but_creation_does_not`
///
/// Re-tightening modes is best effort for a store this process did not create,
/// so a sandboxed reader still gets a connection; a creating caller keeps the
/// strict behavior because a store left readable at creation is a defect.
/// The fixture makes `chmod` fail through the `uchg` immutable flag, a BSD
/// file flag with no Linux equivalent, so the scenario runs only on macOS.
#[cfg(target_os = "macos")]
#[test]
fn reopen_tolerates_chmod_denied_but_creation_does_not() {
    let home = common::Home::new();
    let database = home.path.join("state.db");
    let immutable = std::process::Command::new("/usr/bin/chflags")
        .arg("uchg")
        .arg(&database)
        .status()
        .expect("chflags runs");
    assert!(
        immutable.success(),
        "test needs an undeletable-flag capable fs"
    );
    let reopened = Store::open(&home.path);
    let _ = std::process::Command::new("/usr/bin/chflags")
        .arg("nouchg")
        .arg(&database)
        .status();
    reopened.expect("a denied chmod must not block reopening an existing store");

    let fresh = tempfile::tempdir().unwrap();
    let blocked = fresh.path().join("state.db");
    std::fs::write(&blocked, b"").unwrap();
    assert!(std::process::Command::new("/usr/bin/chflags")
        .arg("uchg")
        .arg(&blocked)
        .status()
        .expect("chflags runs")
        .success());
    let created = Store::initialize(fresh.path());
    let _ = std::process::Command::new("/usr/bin/chflags")
        .arg("nouchg")
        .arg(&blocked)
        .status();
    assert!(
        created.is_err(),
        "creation must refuse rather than leave a store it could not protect"
    );
}

/// Mirrors `tests/test_state_db.py::StateDatabaseTests::test_concurrent_first_initializers_share_one_atomic_schema`
///
/// Racing first initializers must agree on one committed schema rather than
/// letting a second creator observe or duplicate a half-built one.
#[test]
fn concurrent_first_initializers_share_one_atomic_schema() {
    use std::sync::{Arc, Barrier};

    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().join("private");
    let callers = 8;
    let barrier = Arc::new(Barrier::new(callers));
    let versions = (0..callers)
        .map(|_| {
            let home = home.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let store = Store::initialize(&home).expect("concurrent initialize");
                store.health().expect("health")["schema_version"]
                    .as_i64()
                    .expect("schema version")
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|handle| handle.join().expect("initializer thread"))
        .collect::<Vec<_>>();
    assert_eq!(versions, vec![agent_run_store::VERSION; callers]);
    Store::open(&home).expect("the shared schema is usable afterwards");
}
