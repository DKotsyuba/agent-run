//! Disposable-prefix deployment, journal, backup, and explicit restore commands.

use crate::release;
use rusqlite::Connection;
use serde_json::json;
use std::{
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

/// Rejects a switch when durable active-agent rows are present unless forced.
///
/// `force` skips only this safety gate; it neither terminates workers nor loads
/// services, so an operator must establish quiescence before using it.
fn quiescent(home: &Path, force: bool) -> Result<(), String> {
    let state = home.join("state.db");
    if !state.exists() || force {
        return Ok(());
    }
    let connection = Connection::open(state).map_err(|error| error.to_string())?;
    let count: i64 = connection.query_row("SELECT COUNT(*) FROM agents WHERE status NOT IN ('succeeded','failed','lost','timed_out','cancelled')", [], |row| row.get(0)).map_err(|error| error.to_string())?;
    if count == 0 {
        Ok(())
    } else {
        Err(format!(
            "refusing switch with {count} active agents; --force skips the quiescence check only"
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

/// Saves state/config and the prior pointer in a retained private backup.
fn backup(prefix: &Path, home: &Path, old: Option<&Path>) -> Result<PathBuf, String> {
    let directory = prefix.join("backups").join(stamp()?);
    fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    for name in ["state.db", "config.toml"] {
        if home.join(name).is_file() {
            fs::copy(home.join(name), directory.join(name)).map_err(|error| error.to_string())?;
        }
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
