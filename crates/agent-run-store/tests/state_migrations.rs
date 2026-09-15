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

#[test]
fn successful_migration_leaves_no_backup_files_behind() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("state.db");
    drop(build_fixture(&db_path, 1));
    assert_eq!(migrations::migrate(&db_path).unwrap(), VERSION);
    for version in 2..=VERSION {
        assert!(!migrations::backup_path(&db_path, version).exists());
    }
}

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
