//! Numbered, transactional, backup-backed schema migrations.
//!
//! Ported from `src/agent_run/state/migrations.py`. Every migration is a
//! numbered SQL file in `migrations/`, embedded verbatim via `include_str!`,
//! named `NNN_slug.sql` where `NNN` is the schema version the file produces.
//! Version 1 has no file: it is created wholesale from `schema.sql`.
//!
//! Each migration runs inside one `BEGIN IMMEDIATE` transaction, so an
//! interrupted migration leaves the store at its previous version rather
//! than half-upgraded. Before the transaction opens, the store is
//! snapshotted next to itself through SQLite's own backup API, which copies
//! the database under a read transaction and is therefore consistent in WAL
//! mode. The snapshot is removed only once the transaction commits.
//!
//! Unlike Python's `sqlite3.executescript`, `rusqlite::Connection::execute_batch`
//! does not implicitly commit a pending transaction before running, so a
//! migration script can be executed directly inside our own `BEGIN
//! IMMEDIATE` without the statement-by-statement splitting the Python port
//! needs to work around that quirk.

use crate::{error::invalid, Error, Result};
use rusqlite::{Connection, DatabaseName};
use std::{
    collections::HashSet,
    fs::OpenOptions,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// The tables schema v1 created. A store stamped with a version at or above 1
/// must still have all of them before any migration is allowed to touch it,
/// so a truncated or foreign database is refused instead of upgraded.
const V1_TABLES: &[&str] = &[
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

/// Every migration file, ordered by the version it produces, embedded at
/// compile time so the binary needs no data directory alongside it.
const PENDING_FILES: &[(i64, &str)] = &[
    (2, include_str!("migrations/002_workflow_tables.sql")),
    (3, include_str!("migrations/003_workflow_run_plan.sql")),
    (4, include_str!("migrations/004_workflow_deliveries.sql")),
    (5, include_str!("migrations/005_workflow_run_result.sql")),
    (
        6,
        include_str!("migrations/006_repair_workflow_foreign_keys.sql"),
    ),
    (7, include_str!("migrations/007_delivery_expired_state.sql")),
    (8, include_str!("migrations/008_run_stats.sql")),
    (
        9,
        include_str!("migrations/009_delivery_attempt_evidence.sql"),
    ),
    (
        10,
        include_str!("migrations/010_capacity_route_snapshots.sql"),
    ),
    (11, include_str!("migrations/011_startup_owner.sql")),
    (
        12,
        include_str!("migrations/012_workflow_delivery_generation.sql"),
    ),
    (13, include_str!("migrations/013_agent_lineage.sql")),
    (14, include_str!("migrations/014_process_birth.sql")),
    (15, include_str!("migrations/015_request_id_lookup.sql")),
    (
        16,
        include_str!("migrations/016_reconciliation_cursors.sql"),
    ),
];

/// Every migration file, ordered by the version it produces.
pub fn pending_files() -> &'static [(i64, &'static str)] {
    PENDING_FILES
}

pub fn version_of(conn: &Connection) -> Result<i64> {
    Ok(conn.pragma_query_value(None, "user_version", |r| r.get(0))?)
}

pub fn table_names(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<HashSet<_>>>()?)
}

pub fn backup_path(store_path: &Path, target: i64) -> PathBuf {
    let name = store_path
        .file_name()
        .expect("store path must have a file name")
        .to_string_lossy();
    store_path.with_file_name(format!("{name}.pre-v{target}.backup"))
}

fn refuse_newer(path: &Path, version: i64) -> Result<()> {
    if version > super::VERSION {
        return Err(invalid(format!(
            "state database {} is schema v{version}, newer than this agent-run understands \
             (v{}); upgrade agent-run instead of downgrading the store",
            path.display(),
            super::VERSION
        )));
    }
    Ok(())
}

/// Remove snapshots left by a migration that committed but died before cleanup.
///
/// Only versions this binary understands are dropped: during a rollout an
/// older binary runs beside a newer one, and an unguarded glob would delete
/// the newer binary's in-flight snapshot out from under it.
fn drop_stale_backups(path: &Path) {
    let Some(dir) = path.parent() else { return };
    let name = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let prefix = format!("{name}.pre-v");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let Some(rest) = file_name.strip_prefix(&prefix) else {
            continue;
        };
        let Some(digits) = rest.strip_suffix(".backup") else {
            continue;
        };
        if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(version) = digits.parse::<i64>() {
                if version <= super::VERSION {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
}

/// Serialize schema creation and migration across processes.
struct SchemaLock(std::fs::File);
impl SchemaLock {
    fn acquire(path: &Path) -> Result<Self> {
        use fs2::FileExt;
        let lock_name = format!(
            ".{}.init.lock",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
        let lock_path = path.with_file_name(lock_name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&lock_path)?;
        file.lock_exclusive().map_err(Error::Io)?;
        Ok(Self(file))
    }
}
impl Drop for SchemaLock {
    fn drop(&mut self) {
        use fs2::FileExt;
        let _ = FileExt::unlock(&self.0);
    }
}

fn snapshot(conn: &Connection, path: &Path, target: i64) -> Result<PathBuf> {
    let backup = backup_path(path, target);
    let _ = std::fs::remove_file(&backup);
    conn.backup(DatabaseName::Main, &backup, None)?;
    std::fs::set_permissions(&backup, std::fs::Permissions::from_mode(0o600))?;
    Ok(backup)
}

fn foreign_key_violation_count(conn: &Connection) -> Result<usize> {
    let mut stmt = conn.prepare("PRAGMA foreign_key_check")?;
    let rows = stmt.query_map([], |_| Ok(()))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?.len())
}

/// Apply one numbered migration atomically under safe SQLite rebuild settings.
///
/// The connection temporarily disables foreign-key enforcement and enables
/// legacy `ALTER TABLE` behavior so table renames cannot rewrite references
/// in other tables (SQLite pragmas of this kind may only change outside an
/// open transaction). The migration is rolled back if execution or the
/// explicit foreign-key audit fails, and both connection settings are
/// restored before returning or raising an error.
pub fn apply_one(conn: &Connection, path: &Path, target: i64, sql: &str) -> Result<()> {
    let backup = snapshot(conn, path, target)?;
    let foreign_keys: i64 = conn.pragma_query_value(None, "foreign_keys", |r| r.get(0))?;
    let legacy_alter_table: i64 =
        conn.pragma_query_value(None, "legacy_alter_table", |r| r.get(0))?;
    conn.pragma_update(None, "foreign_keys", false)?;
    conn.pragma_update(None, "legacy_alter_table", true)?;
    let outcome: Result<()> = (|| {
        conn.execute_batch("BEGIN IMMEDIATE")?;
        let applied: Result<()> = (|| {
            conn.execute_batch(sql)?;
            let violations = foreign_key_violation_count(conn)?;
            if violations > 0 {
                return Err(Error::Integrity(format!(
                    "foreign_key_check found {violations} violation(s)"
                )));
            }
            conn.execute_batch(&format!("PRAGMA user_version={target}"))?;
            Ok(())
        })();
        if applied.is_err() {
            let _ = conn.execute_batch("ROLLBACK");
            return applied;
        }
        conn.execute_batch("COMMIT")?;
        Ok(())
    })();
    conn.pragma_update(None, "legacy_alter_table", legacy_alter_table)?;
    conn.pragma_update(None, "foreign_keys", foreign_keys)?;
    match outcome {
        Ok(()) => {
            let _ = std::fs::remove_file(&backup);
            Ok(())
        }
        Err(error) => Err(invalid(format!(
            "state schema migration to v{target} failed and was rolled back: {error}. \
             The pre-migration backup of {} is intact at {}",
            path.display(),
            backup.display()
        ))),
    }
}

fn apply_pending(conn: &Connection, path: &Path) -> Result<i64> {
    let mut version = version_of(conn)?;
    refuse_newer(path, version)?;
    for (target, sql) in pending_files() {
        if *target <= version {
            continue;
        }
        apply_one(conn, path, *target, sql)?;
        version = *target;
    }
    Ok(version)
}

/// Bring an existing store up to [`super::VERSION`] and return its version.
///
/// Returns 0 for a database that carries no version stamp at all -- it is
/// not an agent-run store, and the caller's schema validation owns that
/// refusal.
pub fn migrate(path: &Path) -> Result<i64> {
    if !path.is_file() {
        return Err(invalid(format!(
            "state database does not exist: {}",
            path.display()
        )));
    }
    let conn = Connection::open(path)?;
    let version = version_of(&conn)?;
    if version == 0 {
        return Ok(0);
    }
    refuse_newer(path, version)?;
    if version == super::VERSION {
        drop_stale_backups(path);
        return Ok(version);
    }
    let existing = table_names(&conn)?;
    let mut missing: Vec<&str> = V1_TABLES
        .iter()
        .filter(|table| !existing.contains(**table))
        .copied()
        .collect();
    if !missing.is_empty() {
        missing.sort_unstable();
        return Err(invalid(format!(
            "state database {} claims schema v{version} but is missing {}; refusing to migrate \
             an incomplete store",
            path.display(),
            missing.join(", ")
        )));
    }
    let _lock = SchemaLock::acquire(path)?;
    apply_pending(&conn, path)
}
