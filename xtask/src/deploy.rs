//! Disposable-prefix deployment, journal, backup, and explicit restore commands.

use crate::release;
use agent_run_platform::process::{self, ProcessState};
use rusqlite::{Connection, DatabaseName};
use serde_json::json;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Names the deterministic cutover boundaries used by the crash-drill tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, allow(dead_code))]
enum CutoverStage {
    /// The journal is durable, but the temporary pointer has not been created.
    Before,
    /// The temporary pointer exists, but the atomic rename has not happened.
    During,
    /// The atomic rename completed, but the committed journal has not been written.
    After,
}

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
    if fs::symlink_metadata(&pointer).is_err() {
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
    let connection = open_state(&state)?;
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

/// Opens a deployment state database with a short busy deadline so recovery diagnostics stay prompt.
fn open_state(path: &Path) -> Result<Connection, String> {
    let connection = Connection::open(path).map_err(|error| error.to_string())?;
    connection
        .busy_timeout(Duration::from_millis(100))
        .map_err(|error| error.to_string())?;
    Ok(connection)
}

/// Atomically replaces the prefix `current` symlink with a verified release.
fn switch(prefix: &Path, release: &Path) -> Result<(), String> {
    switch_with_failure(prefix, release, None)
}

/// Atomically replaces `current`, optionally failing at the temporary-link boundary.
fn switch_with_failure(
    prefix: &Path,
    release: &Path,
    failure: Option<CutoverStage>,
) -> Result<(), String> {
    let temporary = prefix.join(".current-next");
    let _ = fs::remove_file(&temporary);
    #[cfg(unix)]
    std::os::unix::fs::symlink(release, &temporary).map_err(|error| error.to_string())?;
    #[cfg(not(unix))]
    return Err("native deployment requires Unix symlinks".into());
    if failure == Some(CutoverStage::During) {
        return Err("injected cutover failure during swap".into());
    }
    fs::rename(temporary, prefix.join("current")).map_err(|error| error.to_string())
}

/// Writes a deployment journal through a temporary file so a crash cannot erase its evidence.
fn write_journal(prefix: &Path, journal: &serde_json::Value) -> Result<(), String> {
    let temporary = prefix.join(format!(".deploy-{}.next", stamp()?));
    let result = (|| {
        let bytes = serde_json::to_vec_pretty(journal).map_err(|error| error.to_string())?;
        let mut file = fs::File::create(&temporary).map_err(|error| error.to_string())?;
        std::io::Write::write_all(&mut file, &bytes).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        fs::rename(&temporary, prefix.join("deploy.json")).map_err(|error| error.to_string())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// Reads the retained deployment journal without inferring missing recovery evidence.
fn read_journal(prefix: &Path) -> Result<serde_json::Value, String> {
    serde_json::from_slice(
        &fs::read(prefix.join("deploy.json")).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("invalid deployment journal: {error}"))
}

/// Returns the SQLite user version, or `None` for a first install without a store.
fn state_schema(home: &Path) -> Result<Option<u64>, String> {
    let state = home.join("state.db");
    if !state.is_file() {
        return Ok(None);
    }
    let connection = open_state(&state)?;
    connection
        .query_row("PRAGMA user_version", [], |row| row.get::<_, u64>(0))
        .map(Some)
        .map_err(|error| error.to_string())
}

/// Rejects a release when the observed store is newer than its supported schema.
fn schema_compatible(home: &Path, release: &Path) -> Result<(), String> {
    if let Some(schema) = state_schema(home)? {
        let supported = release::schema_version(release)?;
        if schema > supported {
            return Err(format!(
                "refusing release {} on newer schema {schema} (supports {supported})",
                release.display()
            ));
        }
    }
    Ok(())
}

/// Chooses a private, not-yet-created directory for one deployment backup.
fn backup_path(prefix: &Path) -> Result<PathBuf, String> {
    Ok(prefix.join("backups").join(stamp()?))
}

/// Saves state with SQLite's backup API, config, and the prior pointer privately.
fn backup_at(directory: &Path, home: &Path, old: Option<&Path>) -> Result<(), String> {
    fs::create_dir_all(directory).map_err(|error| error.to_string())?;
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
    Ok(())
}

/// Verifies a candidate and turns common sealed-release defects into operator-facing errors.
fn verify_candidate(release: &Path) -> Result<(), String> {
    match release::verify(release) {
        Ok(()) => Ok(()),
        Err(error) => {
            for relative in ["COMPLETE", "SHA256SUMS", "bin/agent-run", "metadata.json"] {
                if !release.join(relative).exists() {
                    return Err(format!("candidate release is missing asset {relative}"));
                }
            }
            if error.to_ascii_lowercase().contains("permission denied") {
                Err(format!(
                    "candidate release asset access was denied: {error}"
                ))
            } else {
                Err(format!("candidate release is corrupt: {error}"))
            }
        }
    }
}

/// Selects a concrete corrective operation for a failed deployment boundary.
fn next_operation(error: &str) -> &'static str {
    let lower = error.to_ascii_lowercase();
    if error.contains("missing asset") {
        "restore the named asset or rebuild the sealed candidate, then rerun `release deploy`"
    } else if error.contains("asset access was denied") {
        "restore read permission on the named candidate asset, then rerun `release deploy`"
    } else if lower.contains("database is locked") || lower.contains("database is busy") {
        "release the SQLite lock held by another writer, then rerun `release deploy`"
    } else if lower.contains("no space left on device") {
        "free disk space in the deployment home, then rerun `release deploy`"
    } else if lower.contains("not a database") {
        "repair the corrupt state database or restore a valid copy, then rerun `release deploy`"
    } else if error.contains("newer schema") {
        "use a release compatible with the recorded schema, then rerun `release recover`"
    } else if error.contains("active agents") || error.contains("live workflow writers") {
        "stop or wait for the named active agents and workflow writers, then rerun `release deploy`"
    } else if lower.contains("permission denied") {
        "restore read/write permission for the deployment path, then rerun `release deploy`"
    } else {
        "correct the reported deployment error, inspect the retained journal, then rerun `release deploy`"
    }
}

/// Retains a failed phase and appends a specific next operation to its journal.
fn failed_deployment(prefix: &Path, mut journal: serde_json::Value, error: String) -> String {
    let next = next_operation(&error);
    journal["error"] = json!(error);
    journal["next_operation"] = json!(next);
    if let Err(journal_error) = write_journal(prefix, &journal) {
        return format!("{error}; next operation: {next}; journal update failed: {journal_error}");
    }
    format!("{error}; next operation: {next}")
}

/// Installs or updates `prefix` from a sealed release after a quiescent backup.
pub fn deploy(prefix: &Path, home: &Path, release: &Path, force: bool) -> Result<(), String> {
    deploy_inner(prefix, home, release, force, None)
}

/// Runs one deployment with a deterministic failure at a cutover boundary.
#[cfg(test)]
fn deploy_with_failure(
    prefix: &Path,
    home: &Path,
    release: &Path,
    failure: CutoverStage,
) -> Result<(), String> {
    deploy_inner(prefix, home, release, false, Some(failure))
}

/// Performs the deployment sequence and leaves its journal intact on failure.
fn deploy_inner(
    prefix: &Path,
    home: &Path,
    release: &Path,
    force: bool,
    failure: Option<CutoverStage>,
) -> Result<(), String> {
    fs::create_dir_all(prefix).map_err(|error| error.to_string())?;
    let old = current(prefix)?;
    let backup = backup_path(prefix)?;
    let mut journal = json!({
        "phase":"prepared",
        "old_release":old,
        "new_release":release,
        "backup":backup,
        "old_schema":null,
        "new_schema":null,
        "force_skips":"quiescence only",
        "next_operation":"verify the candidate, database, and quiescence before switching the current pointer"
    });
    write_journal(prefix, &journal)?;

    let (old_schema, new_schema) = match (|| {
        verify_candidate(release)?;
        schema_compatible(home, release)?;
        quiescent(home, force)?;
        let old_schema = old.as_deref().map(release::schema_version).transpose()?;
        let new_schema = release::schema_version(release)?;
        backup_at(&backup, home, old.as_deref())?;
        Ok::<_, String>((old_schema, new_schema))
    })() {
        Ok(schemas) => schemas,
        Err(error) => return Err(failed_deployment(prefix, journal, error)),
    };
    journal["old_schema"] = json!(old_schema);
    journal["new_schema"] = json!(new_schema);
    journal["next_operation"] =
        json!("atomically switch the current pointer to the verified immutable release");
    if let Err(error) = write_journal(prefix, &journal) {
        return Err(failed_deployment(prefix, journal, error));
    }
    if failure == Some(CutoverStage::Before) {
        return Err(failed_deployment(
            prefix,
            journal,
            "injected cutover failure before swap".into(),
        ));
    }
    if let Err(error) = switch_with_failure(prefix, release, failure) {
        return Err(failed_deployment(prefix, journal, error));
    }
    if failure == Some(CutoverStage::After) {
        return Err(failed_deployment(
            prefix,
            journal,
            "injected cutover failure after swap".into(),
        ));
    }
    let committed = json!({
        "phase":"committed",
        "old_release":old,
        "new_release":release,
        "backup":backup,
        "old_schema":old_schema,
        "new_schema":new_schema,
        "force_skips":"quiescence only",
        "next_operation":"outside xtask, restore services/jobs, verify API readiness and capability discovery, then run the isolated release smoke"
    });
    if let Err(error) = write_journal(prefix, &committed) {
        return Err(failed_deployment(prefix, journal, error));
    }
    Ok(())
}

/// Recovers a prepared or partially switched deployment without guessing schema state.
pub fn recover(prefix: &Path, home: &Path, force: bool) -> Result<(), String> {
    quiescent(home, force)?;
    let journal = read_journal(prefix)?;
    let old = journal["old_release"].as_str().map(PathBuf::from);
    let target = PathBuf::from(
        journal["new_release"]
            .as_str()
            .ok_or("journal has no new release")?,
    );
    if let Some(old) = &old {
        release::verify(old)?;
    }
    release::verify(&target)?;
    let current = current(prefix)?;
    let schema = state_schema(home)?;
    let old_schema = journal["old_schema"].as_u64();
    let new_schema = journal["new_schema"]
        .as_u64()
        .ok_or("journal has no new schema evidence")?;
    if let Some(schema) = schema {
        if schema > new_schema {
            return Err(format!(
                "database schema {schema} is newer than recovery target {new_schema}"
            ));
        }
    }
    let compatible = match (schema, old.as_ref(), old_schema) {
        (Some(schema), Some(_old), Some(old_schema)) if schema > old_schema => target.clone(),
        (_, Some(old), _) if current.as_deref() == Some(old.as_path()) => old.clone(),
        (_, _, _) if current.as_deref() == Some(target.as_path()) => target.clone(),
        (_, Some(old), _) => old.clone(),
        (_, None, _) => target.clone(),
    };
    release::verify(&compatible)?;
    let _ = fs::remove_file(prefix.join(".current-next"));
    if current.as_deref() != Some(compatible.as_path()) {
        switch(prefix, &compatible)?;
    }
    let mut recovered = journal;
    recovered["phase"] = json!("recovered");
    recovered["current_release"] = json!(compatible);
    recovered["next_operation"] = json!(
        "outside xtask, restore services/jobs, verify API readiness and capability discovery, then run the isolated release smoke"
    );
    write_journal(prefix, &recovered)
}

/// Completes a retained deployment by selecting its verified new release.
pub fn roll_forward(prefix: &Path, home: &Path, force: bool) -> Result<(), String> {
    quiescent(home, force)?;
    let journal = read_journal(prefix)?;
    let target = PathBuf::from(
        journal["new_release"]
            .as_str()
            .ok_or("journal has no new release")?,
    );
    release::verify(&target)?;
    schema_compatible(home, &target)?;
    switch(prefix, &target)?;
    let mut forwarded = journal;
    forwarded["phase"] = json!("rolled_forward");
    forwarded["current_release"] = json!(target);
    forwarded["next_operation"] = json!(
        "outside xtask, restore services/jobs, verify API readiness and capability discovery, then run the isolated release smoke"
    );
    write_journal(prefix, &forwarded)
}

/// Restores the prior pointer and retained state/config backup from the journal.
///
/// A journal already in `rolled_back` phase is rejected explicitly so an
/// operator never mistakes a no-op retry for a second completed restore.
pub fn rollback(prefix: &Path, home: &Path, force: bool) -> Result<(), String> {
    quiescent(home, force)?;
    let journal = read_journal(prefix)?;
    if journal["phase"] == "rolled_back" {
        return Err("deployment is already rolled back".into());
    }
    let old = PathBuf::from(
        journal["old_release"]
            .as_str()
            .ok_or("journal has no previous release")?,
    );
    let backup = PathBuf::from(journal["backup"].as_str().ok_or("journal has no backup")?);
    release::verify(&old)?;
    schema_compatible(home, &old)?;
    for name in ["state.db", "config.toml"] {
        if backup.join(name).is_file() {
            fs::copy(backup.join(name), home.join(name)).map_err(|error| error.to_string())?;
        }
    }
    switch(prefix, &old)?;
    let mut rolled_back = journal;
    rolled_back["phase"] = json!("rolled_back");
    rolled_back["current_release"] = json!(old);
    rolled_back["next_operation"] = json!(
        "outside xtask, restore compatible services/jobs and verify API readiness before reconnecting hosts"
    );
    write_journal(prefix, &rolled_back)
}

#[cfg(test)]
mod tests {
    use super::{deploy, deploy_with_failure, recover, roll_forward, rollback, CutoverStage};
    use crate::{release, release::build};
    use rusqlite::Connection;
    use std::fs;
    use tempfile::tempdir;

    /// Creates two sealed releases and installs the first one as the cutover baseline.
    fn cutover_fixture() -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let temporary = tempdir().expect("temporary prefix");
        let prefix = temporary.path().join("standalone");
        let home = temporary.path().join("home");
        fs::create_dir_all(&home).expect("temporary home");
        fs::write(home.join("config.toml"), "fixture\n").expect("configuration fixture");
        Connection::open(home.join("state.db"))
            .expect("state fixture")
            .execute("CREATE TABLE agents (status TEXT NOT NULL)", [])
            .expect("agent fixture table");
        let old_binary = temporary.path().join("old");
        let new_binary = temporary.path().join("new");
        fs::write(&old_binary, "old binary").expect("old binary");
        fs::write(&new_binary, "new binary").expect("new binary");
        let old = build(&prefix, "old", &old_binary).expect("old release");
        let new = build(&prefix, "new", &new_binary).expect("new release");
        deploy(&prefix, &home, &old, false).expect("baseline install");
        (temporary, prefix, home, old, new)
    }

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

    // Rust-only: deterministic boundary failures protect the atomic cutover state machine.
    #[test]
    fn t84_crash_drill_recovery_keeps_old_or_new_current() {
        for failure in [
            CutoverStage::Before,
            CutoverStage::During,
            CutoverStage::After,
        ] {
            let (_temporary, prefix, home, old, new) = cutover_fixture();
            assert!(deploy_with_failure(&prefix, &home, &new, failure).is_err());
            recover(&prefix, &home, false).expect("recover cutover fixture");
            let selected = fs::read_link(prefix.join("current")).expect("current pointer");
            assert!(selected == old || selected == new, "selected {selected:?}");
            release::verify(&selected).expect("recovered release is sealed");
            assert!(!prefix.join(".current-next").exists());
        }
    }

    // Rust-only: recovery must refuse a legacy release once the store is newer than its evidence.
    #[test]
    fn t84_recovery_refuses_old_release_after_schema_advance() {
        let (_temporary, prefix, home, old, new) = cutover_fixture();
        assert!(deploy_with_failure(&prefix, &home, &new, CutoverStage::Before).is_err());
        Connection::open(home.join("state.db"))
            .expect("state fixture")
            .execute("PRAGMA user_version = 17", [])
            .expect("advance schema fixture");
        let error = recover(&prefix, &home, false).expect_err("newer schema must refuse recovery");
        assert!(
            error.contains("newer than recovery target"),
            "unexpected error: {error}"
        );
        assert_eq!(
            fs::read_link(prefix.join("current")).expect("current pointer"),
            old
        );
    }

    // Rust-only: every failed boundary retains both journal pointer candidates for recovery.
    #[test]
    fn t84_current_pointer_evidence_survives_each_cutover_failure() {
        for failure in [
            CutoverStage::Before,
            CutoverStage::During,
            CutoverStage::After,
        ] {
            let (_temporary, prefix, home, old, new) = cutover_fixture();
            assert!(deploy_with_failure(&prefix, &home, &new, failure).is_err());
            let journal: serde_json::Value = serde_json::from_slice(
                &fs::read(prefix.join("deploy.json")).expect("retained journal"),
            )
            .expect("journal evidence remains readable");
            assert_eq!(journal["old_release"], old.to_string_lossy().as_ref());
            assert_eq!(journal["new_release"], new.to_string_lossy().as_ref());
            let selected = fs::read_link(prefix.join("current")).expect("current pointer");
            assert!(selected == old || selected == new, "selected {selected:?}");
            recover(&prefix, &home, false).expect("recover retained evidence");
        }
    }

    // Rust-only: retry and roll-forward are repeatable; repeated restore is explicit.
    #[test]
    fn t84_retry_and_roll_forward_are_idempotent_and_repeat_rollback_is_explicit() {
        let (_temporary, prefix, home, old, new) = cutover_fixture();
        assert!(deploy_with_failure(&prefix, &home, &new, CutoverStage::Before).is_err());
        deploy(&prefix, &home, &new, false).expect("retry deployment");
        roll_forward(&prefix, &home, false).expect("roll forward");
        roll_forward(&prefix, &home, false).expect("repeat roll forward");
        assert_eq!(
            fs::read_link(prefix.join("current")).expect("current pointer"),
            new
        );
        rollback(&prefix, &home, false).expect("explicit restore");
        assert_eq!(
            rollback(&prefix, &home, false).unwrap_err(),
            "deployment is already rolled back"
        );
        assert_eq!(
            fs::read_link(prefix.join("current")).expect("current pointer"),
            old
        );
        let journal: serde_json::Value = serde_json::from_slice(
            &fs::read(prefix.join("deploy.json")).expect("retained journal"),
        )
        .expect("journal remains valid");
        assert_eq!(journal["phase"], "rolled_back");
    }

    // Rust-only: the ENOSPC substitution keeps the real corrective operation specific.
    #[test]
    fn t84_insufficient_disk_substitution_names_free_space_operation() {
        // A portable unprivileged test cannot fill the host filesystem safely; this
        // exercises the same journal classification with the kernel's ENOSPC text.
        assert_eq!(
            super::next_operation("No space left on device"),
            "free disk space in the deployment home, then rerun `release deploy`"
        );
    }
}
