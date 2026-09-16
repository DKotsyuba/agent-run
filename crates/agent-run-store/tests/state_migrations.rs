//! Ports the behaviors of `tests/test_state_migrations.py` to the Rust store.
//!
//! Historical-version fixtures are built from the migration SQL chain itself
//! (the v1 fixture is the same `tests/fixtures/schema_v1.sql` the Python
//! suite uses, then `agent_run::state::migrations::pending_files()` --
//! embedded verbatim from `src/agent_run/state/migrations/*.sql` -- is
//! replayed forward), so no Python process runs at test time.
use agent_run_store::{migrations, Store, VERSION};
use regex::Regex;
use rusqlite::{params, Connection};
use std::path::Path;

const V1_SCHEMA: &str = include_str!("fixtures/schema_v1.sql");
const V1_AGENTS: [&str; 3] = ["agt_alpha", "agt_beta", "agt_gamma"];

/// A `state.db` built at `version` by executing the v1 fixture and then every
/// migration file up to and including `version`, exactly as the real chain
/// would leave a store that stopped at that version.
fn build_fixture(path: &Path, version: i64) -> Connection {
    if version >= 2 {
        let fixture_name = if version == VERSION {
            "current-v16.sqlite".to_owned()
        } else {
            format!("historical-v{version}.sqlite")
        };
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../../tests/fixtures/baseline/db/{fixture_name}"));
        std::fs::copy(fixture, path).unwrap();
        return Connection::open(path).unwrap();
    }
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(V1_SCHEMA).unwrap();
    for (target, sql) in migrations::pending_files() {
        if *target > version {
            break;
        }
        conn.execute_batch(sql).unwrap();
    }
    conn.pragma_update(None, "user_version", version).unwrap();
    conn
}

fn insert_v1_agents(conn: &Connection) {
    for (index, id) in V1_AGENTS.iter().enumerate() {
        conn.execute(
            "INSERT INTO agents (id, runtime, model, profile, task, task_summary, workdir, \
             request_json, status, created_at, timeout_seconds, config_revision) \
             VALUES (?1,'codex','model','profile','task','summary','/tmp','{}','running',?2,10.0,'cfg')",
            params![id, index as f64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO events (agent_id, at, kind) VALUES (?1, 1.0, 'created')",
            params![id],
        )
        .unwrap();
    }
}

fn open_ro(path: &Path) -> Connection {
    Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
}

fn user_version(conn: &Connection) -> i64 {
    conn.pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap()
}

fn agent_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM agents", [], |r| r.get(0))
        .unwrap()
}

/// Every application table/index with insignificant whitespace and
/// SQLite-added identifier quotes normalized away (mirrors Python's
/// `_schema_objects` in `tests/test_state_migrations.py`).
fn schema_objects(conn: &Connection) -> Vec<(String, String, String)> {
    let quotes = Regex::new(r#""([A-Za-z_][A-Za-z0-9_]*)""#).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT type, name, sql FROM sqlite_master \
             WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'",
        )
        .unwrap();
    let mut rows: Vec<(String, String, String)> = stmt
        .query_map([], |r| {
            let sql: Option<String> = r.get(2)?;
            Ok((r.get(0)?, r.get(1)?, sql.unwrap_or_default()))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    for row in &mut rows {
        let unquoted = quotes.replace_all(&row.2, "$1");
        row.2 = unquoted.split_whitespace().collect::<Vec<_>>().join(" ");
    }
    rows.sort();
    rows
}

fn fresh_schema_objects() -> Vec<(String, String, String)> {
    let home = tempfile::tempdir().unwrap();
    Store::initialize(home.path()).unwrap();
    let conn = open_ro(&home.path().join("state.db"));
    schema_objects(&conn)
}

// --- (a) every historical version reaches a schema identical to fresh ---

/// Mirrors `tests/test_state_migrations.py::V1UpgradeTests::test_migrated_store_is_indistinguishable_from_a_fresh_one`.
/// Mirrors `tests/test_state_migrations.py::V2UpgradeTests::test_v2_migrated_store_is_indistinguishable_from_a_fresh_one`.
#[test]
fn every_historical_version_migrates_to_a_schema_indistinguishable_from_fresh() {
    let fresh = fresh_schema_objects();
    for version in 1..VERSION {
        let home = tempfile::tempdir().unwrap();
        let db_path = home.path().join("state.db");
        drop(build_fixture(&db_path, version));
        assert_eq!(
            migrations::migrate(&db_path).unwrap(),
            VERSION,
            "version {version}"
        );
        let migrated = open_ro(&db_path);
        assert_eq!(user_version(&migrated), VERSION, "version {version}");
        assert_eq!(
            schema_objects(&migrated),
            fresh,
            "store migrated from v{version} drifted from a fresh schema"
        );
    }
}

// --- (b) existing rows survive migration ---

/// Mirrors `tests/test_state_migrations.py::V1UpgradeTests::test_v1_home_opens_transparently_and_keeps_its_rows`.
#[test]
fn existing_rows_survive_migration_from_v1() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    let build = build_fixture(&db_path, 1);
    insert_v1_agents(&build);
    drop(build);

    let store = Store::open(home.path()).unwrap();
    assert_eq!(store.health().unwrap()["schema_version"], VERSION);
    let conn = open_ro(&db_path);
    assert_eq!(agent_count(&conn), V1_AGENTS.len() as i64);
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM events", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        V1_AGENTS.len() as i64
    );
    for id in V1_AGENTS {
        let status: String = conn
            .query_row("SELECT status FROM agents WHERE id=?1", params![id], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(status, "running");
    }
}

// --- (c) a newer-than-supported database is refused without modification ---

/// Mirrors `tests/test_state_migrations.py::MigrationRefusalTests::test_newer_schema_is_refused_without_touching_the_store`.
#[test]
fn newer_schema_is_refused_without_touching_the_store() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    let build = build_fixture(&db_path, 1);
    insert_v1_agents(&build);
    build
        .execute_batch(&format!("PRAGMA user_version={}", VERSION + 1))
        .unwrap();
    drop(build);

    let error = migrations::migrate(&db_path).unwrap_err();
    assert!(
        error.to_string().contains("newer than this agent-run"),
        "{error}"
    );
    assert!(Store::open(home.path()).is_err());

    let conn = open_ro(&db_path);
    assert_eq!(user_version(&conn), VERSION + 1);
    assert_eq!(agent_count(&conn), V1_AGENTS.len() as i64);
    assert!(!migrations::backup_path(&db_path, VERSION).exists());
}

// --- (d) backup behavior: cleaned on success, retained (with rollback) on
// failure, and stale-but-understood backups are swept on a later open ---

/// Mirrors `tests/test_state_migrations.py::V1UpgradeTests::test_migration_is_idempotent_and_clears_a_stale_backup`.
#[test]
fn successful_migration_leaves_no_backup_files_behind() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    drop(build_fixture(&db_path, 1));
    assert_eq!(migrations::migrate(&db_path).unwrap(), VERSION);
    let stale = migrations::backup_path(&db_path, VERSION);
    std::fs::write(&stale, b"stale").unwrap();
    assert_eq!(migrations::migrate(&db_path).unwrap(), VERSION);
    assert!(!stale.exists());
    for version in 2..=VERSION {
        assert!(!migrations::backup_path(&db_path, version).exists());
    }
}

/// Mirrors `tests/test_state_migrations.py::MigrationRefusalTests::test_failed_migration_rolls_back_and_leaves_the_backup`.
#[test]
fn failed_migration_rolls_back_and_leaves_the_pre_migration_backup() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    let build = build_fixture(&db_path, 1);
    insert_v1_agents(&build);
    // Migration 002 creates `workflow_runs`; pre-creating it collides and the
    // v1->v2 step fails, so only the pre-v2 backup should ever exist.
    build
        .execute_batch("CREATE TABLE workflow_runs (wrong TEXT)")
        .unwrap();
    drop(build);

    let error = migrations::migrate(&db_path).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("rolled back"), "{message}");
    let backup = migrations::backup_path(&db_path, 2);
    assert!(message.contains(&backup.display().to_string()), "{message}");

    assert!(backup.is_file());
    let backup_conn = open_ro(&backup);
    assert_eq!(user_version(&backup_conn), 1);
    assert_eq!(agent_count(&backup_conn), V1_AGENTS.len() as i64);

    // The live store is untouched: still v1, same rows, so a retry starts
    // from the same place.
    let conn = open_ro(&db_path);
    assert_eq!(user_version(&conn), 1);
    assert_eq!(agent_count(&conn), V1_AGENTS.len() as i64);
}

/// Mirrors `tests/test_state_migrations.py::MigrationRegistryTests::test_stale_backup_cleanup_spares_newer_versions_snapshots`.
#[test]
fn opening_an_up_to_date_store_sweeps_understood_stale_backups_but_spares_newer_ones() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    drop(build_fixture(&db_path, VERSION));
    let own = migrations::backup_path(&db_path, VERSION);
    let newer = migrations::backup_path(&db_path, VERSION + 1);
    std::fs::write(&own, b"stale").unwrap();
    std::fs::write(&newer, b"stale").unwrap();

    assert_eq!(migrations::migrate(&db_path).unwrap(), VERSION);

    assert!(!own.exists());
    assert!(newer.exists());
}

// --- migration 012's data transform is ported faithfully, not just its SQL shape ---

/// Mirrors `tests/test_state_migrations.py::MigrationRegistryTests::test_v11_to_v12_snapshots_or_retires_every_existing_workflow_notice`.
#[test]
fn migration_012_snapshots_or_retires_every_pre_existing_workflow_notice() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    let conn = build_fixture(&db_path, 11);
    conn.execute_batch(
        "INSERT INTO orchestrator_sessions (id, transport, external_session_id, created_at, last_seen_at) \
         VALUES ('os_test','stub','session',1,1);
         INSERT INTO workflow_runs (id, name, script_sha, status, created_at, result_json, orchestrator_session_id) \
         VALUES ('wf_kept','kept','sha','failed',1,'{\"attempt\":1}','os_test');
         INSERT INTO workflow_runs (id, name, script_sha, status, created_at, orchestrator_session_id) \
         VALUES ('wf_retired','retired','sha','running',1,'os_test');
         INSERT INTO workflow_deliveries (id, run_id, orchestrator_session_id, state, next_attempt_at) \
         VALUES ('wd_kept','wf_kept','os_test','pending',1);
         INSERT INTO workflow_deliveries (id, run_id, orchestrator_session_id, state, next_attempt_at) \
         VALUES ('wd_retired','wf_retired','os_test','pending',1);",
    )
    .unwrap();
    drop(conn);

    assert_eq!(migrations::migrate(&db_path).unwrap(), VERSION);

    let migrated = open_ro(&db_path);
    let (state, run_status, generation, result_json): (String, String, i64, Option<String>) = migrated
        .query_row(
            "SELECT state, run_status, attempt_generation, result_json FROM workflow_deliveries WHERE run_id='wf_kept'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(state, "pending");
    assert_eq!(run_status, "failed");
    assert_eq!(generation, 1);
    assert_eq!(result_json.as_deref(), Some(r#"{"attempt":1}"#));

    let (retired_state, retired_result, last_error): (String, Option<String>, String) = migrated
        .query_row(
            "SELECT state, result_json, last_error FROM workflow_deliveries WHERE run_id='wf_retired'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(retired_state, "cancelled");
    assert_eq!(retired_result, None);
    assert!(last_error.contains("v12"), "{last_error}");
}

/// Mirrors `tests/test_state_migrations.py::MigrationRegistryTests::test_files_are_numbered_and_cover_every_version_after_one`.
#[test]
fn migration_registry_is_contiguous() {
    let versions: Vec<_> = migrations::pending_files()
        .iter()
        .map(|(version, _)| *version)
        .collect();
    assert_eq!(versions, (2..=VERSION).collect::<Vec<_>>());
}

/// Mirrors `tests/test_state_migrations.py::MigrationRegistryTests::test_v9_to_v10_creates_bounded_route_snapshot_table`.
#[test]
fn migration_010_creates_a_bounded_route_snapshot_table() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    drop(build_fixture(&db_path, 9));
    let store = Store::open(home.path()).unwrap();
    let columns: Vec<String> = store
        .conn
        .prepare("PRAGMA table_info(capacity_route_snapshots)")
        .unwrap()
        .query_map([], |row| row.get(1))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        columns,
        [
            "runtime",
            "scope_id",
            "observed_at",
            "valid_until",
            "payload_json"
        ]
    );
    let error = store.conn.execute(
        "INSERT INTO capacity_route_snapshots(runtime,scope_id,observed_at,valid_until,payload_json) VALUES('codex','oversized',1,2,?)",
        ["x".repeat(65_537)],
    );
    assert!(error.is_err());
}

/// Mirrors `tests/test_state_migrations.py::MigrationRegistryTests::test_foreign_key_check_is_clean_after_each_migration`.
#[test]
fn each_migration_restores_pragmas_and_foreign_key_integrity() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    drop(build_fixture(&db_path, 1));
    let conn = Connection::open(&db_path).unwrap();
    conn.pragma_update(None, "foreign_keys", true).unwrap();
    conn.pragma_update(None, "legacy_alter_table", false)
        .unwrap();
    for (target, sql) in migrations::pending_files() {
        migrations::apply_one(&conn, &db_path, *target, sql).unwrap();
        let foreign_keys: i64 = conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        let legacy_alter_table: i64 = conn
            .pragma_query_value(None, "legacy_alter_table", |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1, "migration v{target}");
        assert_eq!(legacy_alter_table, 0, "migration v{target}");
        assert!(conn
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query_map([], |_| Ok(()))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .is_empty());
    }
}

/// Mirrors `tests/test_state_migrations.py::MigrationRegistryTests::test_open_repairs_poisoned_v5_store_and_preserves_history`.
#[test]
fn opening_v5_preserves_workflow_history_after_foreign_key_repair() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    drop(build_fixture(&db_path, 5));
    let conn = Connection::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO workflow_runs(id,name,script_sha,status,created_at) VALUES('wf_poisoned','flow','sha','running',1)",
        [],
    )
    .unwrap();
    conn.execute_batch("PRAGMA user_version=5").unwrap();
    drop(conn);
    let store = Store::open(home.path()).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT status FROM workflow_runs WHERE id='wf_poisoned'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "running"
    );
    assert!(store
        .conn
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .query_map([], |_| Ok(()))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .is_empty());
}

/// Mirrors `tests/test_state_migrations.py::V1UpgradeTests::test_initialize_also_upgrades_an_existing_v1_home`.
#[test]
fn initialize_upgrades_an_existing_v1_store() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    let conn = build_fixture(&db_path, 1);
    insert_v1_agents(&conn);
    drop(conn);
    let store = Store::initialize(home.path()).unwrap();
    assert_eq!(store.health().unwrap()["schema_version"], VERSION);
    assert_eq!(agent_count(&store.conn), V1_AGENTS.len() as i64);
}

/// Mirrors `tests/test_state_migrations.py::V1UpgradeTests::test_concurrent_openers_migrate_a_v1_store_exactly_once`.
#[test]
fn concurrent_openers_migrate_a_v1_store_once() {
    use std::sync::{Arc, Barrier};
    use std::thread;
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    let conn = build_fixture(&db_path, 1);
    insert_v1_agents(&conn);
    drop(conn);
    let barrier = Arc::new(Barrier::new(8));
    let paths = (0..8).map(|_| db_path.clone()).collect::<Vec<_>>();
    let threads: Vec<_> = paths
        .into_iter()
        .map(|path| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                Store::open(path.parent().unwrap())
                    .unwrap()
                    .health()
                    .unwrap()["schema_version"]
                    .as_i64()
                    .unwrap()
            })
        })
        .collect();
    let versions: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(versions, vec![VERSION; 8]);
    assert_eq!(agent_count(&open_ro(&db_path)), V1_AGENTS.len() as i64);
}

/// Mirrors `tests/test_state_migrations.py::V1UpgradeTests::test_workflow_status_enums_are_enforced`.
#[test]
fn workflow_status_enums_are_enforced_after_upgrade() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    drop(build_fixture(&db_path, 1));
    let store = Store::open(home.path()).unwrap();
    assert!(store.conn.execute(
        "INSERT INTO workflow_runs(id,name,script_sha,status,created_at) VALUES('bad','flow','sha','bogus',1)",
        [],
    ).is_err());
    store.conn.execute(
        "INSERT INTO workflow_runs(id,name,script_sha,status,created_at) VALUES('good','flow','sha','running',1)",
        [],
    ).unwrap();
    assert!(store.conn.execute(
        "INSERT INTO workflow_steps(run_id,step_key,spec_json,status) VALUES('good','step','{}','bogus')",
        [],
    ).is_err());
}

/// Mirrors `tests/test_state_migrations.py::V2UpgradeTests::test_v2_home_opens_to_v3_with_plan_json_present`.
#[test]
fn v2_upgrade_adds_nullable_workflow_plan_json() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    let conn = build_fixture(&db_path, 2);
    conn.execute(
        "INSERT INTO workflow_runs(id,name,script_sha,status,created_at) VALUES('wr_1','flow','sha','failed',1)",
        [],
    ).unwrap();
    drop(conn);
    let store = Store::open(home.path()).unwrap();
    let columns: Vec<String> = store
        .conn
        .prepare("PRAGMA table_info(workflow_runs)")
        .unwrap()
        .query_map([], |row| row.get(1))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(columns.iter().any(|column| column == "plan_json"));
    assert!(store
        .conn
        .query_row(
            "SELECT plan_json FROM workflow_runs WHERE id='wr_1'",
            [],
            |row| row.get::<_, Option<String>>(0)
        )
        .unwrap()
        .is_none());
}

/// Mirrors `tests/test_state_migrations.py::MigrationRefusalTests::test_incomplete_store_claiming_a_version_is_refused_unmigrated`.
#[test]
fn incomplete_versioned_store_is_refused_without_mutation() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch("CREATE TABLE agents (id TEXT PRIMARY KEY); PRAGMA user_version=1")
        .unwrap();
    drop(conn);
    assert!(Store::open(home.path()).is_err());
    assert_eq!(user_version(&open_ro(&db_path)), 1);
}

/// Mirrors `tests/test_state_migrations.py::MigrationDiagnosticsTests::test_read_only_snapshot_reports_the_pending_migration`.
#[test]
fn read_only_snapshot_refuses_a_pending_migration_without_mutation() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    drop(build_fixture(&db_path, 1));
    let error = agent_run_store::diagnostics::diagnostic_snapshot(&db_path, 1.0, 256).unwrap_err();
    assert!(error.to_string().contains("usable schema"));
    assert_eq!(user_version(&open_ro(&db_path)), 1);
}
