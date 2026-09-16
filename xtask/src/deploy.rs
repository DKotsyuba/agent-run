//! Disposable-prefix deployment, journal, backup, and explicit restore commands.

use crate::release;
use agent_run_platform::process::{self, ProcessState};
use rusqlite::{Connection, DatabaseName};
use serde_json::json;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// Returns a filesystem-safe monotonically unique deployment identifier.
fn stamp() -> Result<String, String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos()
        .to_string())
}

/// Reads the immutable release selected by a prefix, if it has one.
fn current(prefix: &Path) -> Result<Option<PathBuf>, String> {
    let pointer = prefix.join("current");
    if !pointer.exists() {
        return Ok(None);
    }
    fs::read_link(pointer)
        .map(Some)
        .map_err(|error| error.to_string())
}

/// Returns whether one archived writer's observed state leaves any chance it
/// can still write to the store.
///
/// `state` is the verdict [`process::observe`] produced for the PID recorded in
/// `workflow_runs.owner_pid_identity`. Exactly two verdicts prove the recorded
/// writer is gone: [`ProcessState::Dead`] (the PID no longer exists, or exists
/// only as an unreaped zombie that can no longer execute) and
/// [`ProcessState::Reused`] (the PID exists but was born at a different time,
/// so it is a different process). Every other verdict, including
/// [`ProcessState::Denied`] and [`ProcessState::Unknown`], is uncertainty and
/// never evidence of death, so it blocks the switch. Mirrors the
/// `psutil.AccessDenied`/generic-error branches of
/// `_legacy_writer_may_be_live` (`scripts/release_local.py:272-301`), which
/// both answer "may be live".
pub fn writer_state_blocks(state: ProcessState) -> bool {
    !matches!(state, ProcessState::Dead | ProcessState::Reused)
}

/// Returns whether the archived writer recorded by `owner`/`birth` may still
/// be live, and must therefore block a release switch.
///
/// `owner` is a `workflow_runs.owner_pid_identity` value whose first
/// space-separated field is the historical writer PID; a value that does not
/// parse as a PID above 1 is unusable evidence and blocks. `birth` is the
/// stored `owner_birth_time` (psutil's `create_time` float) when that column
/// exists and `None` when it does not — missing birth evidence can prove only
/// that a PID is absent, never that a present PID is a different process.
/// Non-finite or negative birth evidence cannot prove reuse either, so it is
/// discarded rather than compared: any process that still exists then observes
/// as [`ProcessState::Unknown`] and blocks, which is what Python's explicit
/// `math.isfinite`/negative guard achieves.
///
/// This performs one live kernel observation of the PID and has no other side
/// effects.
pub fn writer_may_be_live(owner: &str, birth: Option<f64>) -> bool {
    let Ok(pid) = owner.split(' ').next().unwrap_or_default().parse::<i32>() else {
        return true;
    };
    if pid <= 1 {
        return true;
    }
    let birth = birth.filter(|value| value.is_finite() && *value >= 0.0);
    writer_state_blocks(process::observe(Some(pid), None, birth))
}

/// Counts archived workflow writers that may still be able to write to `home`.
///
/// Stores older than the workflow tables have no `workflow_runs` at all, and
/// older shapes of it lack the ownership columns; either way the store
/// contributes zero, because nothing in it can identify a writer. A store
/// carrying `status`/`owner_pid_identity` but no `owner_birth_time` is
/// consulted with `None` birth evidence. Only `created`/`running` rows with a
/// recorded owner are examined; historical rows without one never block.
/// Mirrors `active` (`scripts/release_local.py:304-329`) minus its agent
/// count, which the caller queries separately.
///
/// Returns the SQLite error text on any failure to read the table shape or the
/// rows; an unreadable store is never silently treated as quiescent.
fn live_writers(connection: &Connection) -> Result<i64, String> {
    let mut columns = BTreeSet::new();
    let mut shape = connection
        .prepare("PRAGMA table_info(workflow_runs)")
        .map_err(|error| error.to_string())?;
    let mut rows = shape.query([]).map_err(|error| error.to_string())?;
    while let Some(row) = rows.next().map_err(|error| error.to_string())? {
        columns.insert(row.get::<_, String>(1).map_err(|error| error.to_string())?);
    }
    if !columns.contains("status") || !columns.contains("owner_pid_identity") {
        return Ok(0);
    }
    let birth = if columns.contains("owner_birth_time") {
        "owner_birth_time"
    } else {
        "NULL"
    };
    let mut writers = connection
        .prepare(&format!(
            "SELECT owner_pid_identity, {birth} FROM workflow_runs
             WHERE status IN ('created', 'running') AND owner_pid_identity IS NOT NULL"
        ))
        .map_err(|error| error.to_string())?;
    let mut rows = writers.query([]).map_err(|error| error.to_string())?;
    let mut live = 0;
    while let Some(row) = rows.next().map_err(|error| error.to_string())? {
        let owner: String = row.get(0).map_err(|error| error.to_string())?;
        let birth: Option<f64> = row.get(1).map_err(|error| error.to_string())?;
        if writer_may_be_live(&owner, birth) {
            live += 1;
        }
    }
    Ok(live)
}

/// Rejects a switch while durable active agents or plausibly live archived
/// workflow writers are present, unless forced.
///
/// A home with no `state.db` yet (a first install) is quiescent by definition.
/// Otherwise both populations are counted: agent rows in a non-terminal status,
/// and `workflow_runs` owners that a live kernel observation cannot prove gone
/// (see [`live_writers`]). `force` skips only this safety gate; it neither
/// terminates workers nor loads services, so an operator must establish
/// quiescence before using it.
fn quiescent(home: &Path, force: bool) -> Result<(), String> {
    let state = home.join("state.db");
    if !state.exists() || force {
        return Ok(());
    }
    let connection = Connection::open(state).map_err(|error| error.to_string())?;
    let agents: i64 = connection.query_row("SELECT COUNT(*) FROM agents WHERE status NOT IN ('succeeded','failed','lost','timed_out','cancelled')", [], |row| row.get(0)).map_err(|error| error.to_string())?;
    let writers = live_writers(&connection)?;
    if agents == 0 && writers == 0 {
        Ok(())
    } else {
        Err(format!(
            "refusing switch with {agents} active agents and {writers} live workflow writers; --force skips the quiescence check only"
        ))
    }
}

/// Atomically replaces the prefix `current` symlink with a verified release.
fn switch(prefix: &Path, release: &Path) -> Result<(), String> {
    let temporary = prefix.join(".current-next");
    let _ = fs::remove_file(&temporary);
    #[cfg(unix)]
    std::os::unix::fs::symlink(release, &temporary).map_err(|error| error.to_string())?;
    #[cfg(not(unix))]
    return Err("native deployment requires Unix symlinks".into());
    fs::rename(temporary, prefix.join("current")).map_err(|error| error.to_string())
}

/// Saves state with SQLite's backup API, config, and the prior pointer privately.
fn backup(prefix: &Path, home: &Path, old: Option<&Path>) -> Result<PathBuf, String> {
    let directory = prefix.join("backups").join(stamp()?);
    fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let state = home.join("state.db");
    if state.is_file() {
        let source = Connection::open(&state).map_err(|error| error.to_string())?;
        source
            .backup(DatabaseName::Main, directory.join("state.db"), None)
            .map_err(|error| error.to_string())?;
    }
    if home.join("config.toml").is_file() {
        fs::copy(home.join("config.toml"), directory.join("config.toml"))
            .map_err(|error| error.to_string())?;
    }
    if let Some(old) = old {
        fs::write(
            directory.join("previous-release"),
            old.display().to_string(),
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(directory)
}

/// Installs or updates `prefix` from a sealed release after a quiescent backup.
pub fn deploy(prefix: &Path, home: &Path, release: &Path, force: bool) -> Result<(), String> {
    release::verify(release)?;
    quiescent(home, force)?;
    fs::create_dir_all(prefix).map_err(|error| error.to_string())?;
    let old = current(prefix)?;
    let backup = backup(prefix, home, old.as_deref())?;
    let journal = json!({"phase":"prepared","old_release":old,"new_release":release,"backup":backup,"force_skips":"quiescence only"});
    fs::write(
        prefix.join("deploy.json"),
        serde_json::to_vec_pretty(&journal).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    switch(prefix, release)?;
    let journal = json!({"phase":"committed","old_release":old,"new_release":release,"backup":backup,"force_skips":"quiescence only"});
    fs::write(
        prefix.join("deploy.json"),
        serde_json::to_vec_pretty(&journal).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Restores the prior pointer and retained state/config backup from the journal.
pub fn rollback(prefix: &Path, home: &Path, force: bool) -> Result<(), String> {
    quiescent(home, force)?;
    let journal: serde_json::Value = serde_json::from_slice(
        &fs::read(prefix.join("deploy.json")).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let old = journal["old_release"]
        .as_str()
        .ok_or("journal has no previous release")?;
    let backup = PathBuf::from(journal["backup"].as_str().ok_or("journal has no backup")?);
    for name in ["state.db", "config.toml"] {
        if backup.join(name).is_file() {
            fs::copy(backup.join(name), home.join(name)).map_err(|error| error.to_string())?;
        }
    }
    switch(prefix, Path::new(old))?;
    fs::write(prefix.join("deploy.json"), serde_json::to_vec_pretty(&json!({"phase":"rolled_back","restored_release":old,"backup":backup,"force_skips":"quiescence only"})).map_err(|error| error.to_string())?).map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{deploy, rollback};
    use crate::release::build;
    use rusqlite::Connection;
    use std::fs;
    use tempfile::tempdir;

    /// Mirrors `tests/test_release_script.py` install-update-rollback recovery drill.
    #[test]
    fn python_release_script_install_update_rollback_drill() {
        let temporary = tempdir().expect("temporary prefix");
        let prefix = temporary.path().join("standalone");
        let home = temporary.path().join("home");
        fs::create_dir_all(&home).expect("temporary home");
        fs::write(home.join("config.toml"), "before").expect("configuration fixture");
        let one = temporary.path().join("one");
        let two = temporary.path().join("two");
        fs::write(&one, "one").expect("first binary");
        fs::write(&two, "two").expect("second binary");
        let first = build(&prefix, "one", &one).expect("first release");
        let second = build(&prefix, "two", &two).expect("second release");
        deploy(&prefix, &home, &first, false).expect("install");
        let connection = Connection::open(home.join("state.db")).expect("state fixture");
        connection
            .execute("CREATE TABLE agents (status TEXT NOT NULL)", [])
            .expect("agent fixture table");
        connection
            .execute("INSERT INTO agents(status) VALUES ('running')", [])
            .expect("active agent fixture");
        assert!(
            deploy(&prefix, &home, &second, false).is_err(),
            "active work blocks switching"
        );
        connection
            .execute("DELETE FROM agents", [])
            .expect("quiescent fixture");
        deploy(&prefix, &home, &second, false).expect("update");
        rollback(&prefix, &home, false).expect("rollback");
        assert_eq!(
            fs::read_link(prefix.join("current")).expect("current pointer"),
            first
        );
    }
}
