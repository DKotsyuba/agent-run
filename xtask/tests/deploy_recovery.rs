//! Quiescence, idempotence, and rollback contracts for `xtask::deploy`.
//!
//! Mirrors selected behaviors from
//! `tests/test_release_script.py::LocalTests`. Python's `release_local.py`
//! deploy pipeline also stops and restarts three launchd services around a
//! SQLite schema migration (`local.migrate`, `local.restart`,
//! `local.recover`, `local.validate_live`). None of that exists in
//! `xtask/src/deploy.rs`: it never shells out to `launchctl`, it never
//! migrates a schema during a switch, and it has no post-switch live API
//! probe. Those behaviors have no Rust counterpart to test and are not
//! ported here; see the task's final report for the specific Python tests
//! this affects.
//!
//! Archived-writer quiescence *is* ported. `sql/schema.sql:164-179` — the
//! schema the Rust store itself creates — carries `workflow_runs`'
//! `owner_pid_identity` and `owner_birth_time`, so the same rows Python's
//! `local.active` consults are present in the very database
//! `xtask::deploy::quiescent` opens, and the same PID/birth-time verdicts are
//! available through `agent_run_platform::process::observe`.

use agent_run_platform::process::{inspect, ProcessState};
use rusqlite::{params, Connection};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};
use tempfile::tempdir;
use xtask::{deploy, release};

/// Lays out a temporary `prefix`/`home` pair with an `agents` table ready
/// for quiescence checks, returning `(prefix, home)`.
fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let temporary = tempdir().expect("temporary directories");
    let prefix = temporary.path().join("standalone");
    let home = temporary.path().join("home");
    fs::create_dir_all(&home).expect("temporary home");
    fs::write(home.join("config.toml"), "before\n").expect("fixture config");
    let connection = Connection::open(home.join("state.db")).expect("fixture database");
    connection
        .execute("CREATE TABLE agents (status TEXT NOT NULL)", [])
        .expect("agents table");
    (temporary, prefix, home)
}

/// Seals one disposable release named `version` under `prefix`.
fn sealed(prefix: &Path, version: &str) -> PathBuf {
    let binary = prefix
        .parent()
        .expect("prefix has a parent")
        .join(format!("binary-{version}"));
    fs::write(&binary, format!("binary {version}")).expect("fixture binary");
    release::build(prefix, version, &binary).expect("sealed release")
}

/// Creates the archived-writer table `local.active` consults.
///
/// `birth` selects between the current shape (`sql/schema.sql:164-179`, with
/// `owner_birth_time`) and the older shape that can prove only PID absence.
fn workflow_table(connection: &Connection, birth: bool) {
    let column = if birth { ", owner_birth_time REAL" } else { "" };
    connection
        .execute(
            &format!(
                "CREATE TABLE workflow_runs (
                   status TEXT NOT NULL, owner_pid_identity TEXT{column}
                 )"
            ),
            [],
        )
        .expect("workflow_runs table");
}

/// Records one `running` workflow row owned by `owner`, with optional birth
/// evidence, in the database that `quiescent` will read.
fn writer_row(home: &Path, owner: &str, birth: Option<f64>) {
    let connection = Connection::open(home.join("state.db")).expect("open fixture database");
    match birth {
        Some(birth) => connection.execute(
            "INSERT INTO workflow_runs(status, owner_pid_identity, owner_birth_time)
             VALUES ('running', ?1, ?2)",
            params![owner, birth],
        ),
        None => connection.execute(
            "INSERT INTO workflow_runs(status, owner_pid_identity) VALUES ('running', ?1)",
            params![owner],
        ),
    }
    .expect("insert archived writer row");
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_active_work_waits_and_shutdown_race_refuses_migration`
/// (the quiescence-refusal assertion only; Python's admission-race recheck
/// after stopping services has no Rust counterpart -- see the module doc
/// comment).
#[test]
fn python_deploy_refuses_active_agents_but_ignores_terminal_statuses() {
    let (_temporary, prefix, home) = fixture();
    let binary_path = prefix.parent().unwrap().join("binary");
    fs::write(&binary_path, "binary").expect("fixture binary");
    let release = release::build(&prefix, "1.0.0", &binary_path).expect("sealed release");

    let connection = Connection::open(home.join("state.db")).expect("open fixture database");
    for terminal in ["succeeded", "failed", "lost", "timed_out", "cancelled"] {
        connection
            .execute("INSERT INTO agents(status) VALUES (?1)", [terminal])
            .expect("insert terminal agent row");
    }
    deploy::deploy(&prefix, &home, &release, false)
        .expect("only terminal-status agents must not block a deploy");

    connection
        .execute("INSERT INTO agents(status) VALUES ('running')", [])
        .expect("insert active agent row");
    let second_binary = prefix.parent().unwrap().join("binary-2");
    fs::write(&second_binary, "binary-2").expect("fixture binary");
    let second_release = release::build(&prefix, "2.0.0", &second_binary).expect("sealed release");
    assert!(
        deploy::deploy(&prefix, &home, &second_release, false).is_err(),
        "a non-terminal agent row must block the deploy"
    );
    assert!(
        deploy::deploy(&prefix, &home, &second_release, true).is_ok(),
        "--force must skip only the quiescence check"
    );
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_backup_before_migration_default_home_plist_and_idempotence`
/// (the redeploy-idempotence assertion only; Python's default-`AGENT_RUN_HOME`
/// plist discovery has no Rust counterpart -- `--home` is always explicit on
/// the Rust CLI).
#[test]
fn python_deploy_is_idempotent_when_rerun_against_the_same_release() {
    let (_temporary, prefix, home) = fixture();
    let binary_path = prefix.parent().unwrap().join("binary");
    fs::write(&binary_path, "binary").expect("fixture binary");
    let release = release::build(&prefix, "1.0.0", &binary_path).expect("sealed release");

    deploy::deploy(&prefix, &home, &release, false).expect("first install");
    let first_current = fs::read_link(prefix.join("current")).expect("current pointer");
    deploy::deploy(&prefix, &home, &release, false).expect("idempotent redeploy");
    let second_current = fs::read_link(prefix.join("current")).expect("current pointer");
    assert_eq!(first_current, second_current);
    assert_eq!(second_current, release);
}

/// Mirrors the backup/rollback contract exercised across
/// `tests/test_release_script.py::LocalTests::test_recovery_failure_preserves_new_pointer_and_reports_stopped_jobs`
/// and `test_failed_migration_restores_old_only_when_schema_is_unchanged`:
/// a rollback restores both the prior release pointer and the state/config
/// snapshot taken just before the switch that is being undone.
#[test]
fn python_rollback_restores_release_pointer_state_and_config() {
    let (_temporary, prefix, home) = fixture();
    let first_binary = prefix.parent().unwrap().join("one");
    let second_binary = prefix.parent().unwrap().join("two");
    fs::write(&first_binary, "one").expect("fixture binary");
    fs::write(&second_binary, "two").expect("fixture binary");
    let first = release::build(&prefix, "one", &first_binary).expect("sealed release");
    let second = release::build(&prefix, "two", &second_binary).expect("sealed release");

    deploy::deploy(&prefix, &home, &first, false).expect("install");
    fs::write(home.join("config.toml"), "after-first\n").expect("advance config fixture");
    deploy::deploy(&prefix, &home, &second, false).expect("update");
    fs::write(home.join("config.toml"), "after-second\n").expect("advance config fixture again");

    deploy::rollback(&prefix, &home, false).expect("rollback");

    assert_eq!(
        fs::read_link(prefix.join("current")).expect("current pointer"),
        first,
        "rollback must restore the previous release pointer"
    );
    assert_eq!(
        fs::read_to_string(home.join("config.toml")).expect("restored config"),
        "after-first\n",
        "rollback must restore the config snapshot taken before the undone switch"
    );

    let journal: serde_json::Value = serde_json::from_slice(
        &fs::read(prefix.join("deploy.json")).expect("journal persists after rollback"),
    )
    .expect("journal is valid JSON");
    assert_eq!(journal["phase"], "rolled_back");
    assert_eq!(
        deploy::rollback(&prefix, &home, false).unwrap_err(),
        "deployment is already rolled back",
        "a repeated rollback must identify the completed phase"
    );
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_live_birth_verified_legacy_writer_blocks_release`
#[test]
fn python_deploy_refuses_a_live_birth_verified_archived_writer() {
    let (_temporary, prefix, home) = fixture();
    let release = sealed(&prefix, "1.0.0");
    let connection = Connection::open(home.join("state.db")).expect("open fixture database");
    workflow_table(&connection, true);

    let me = std::process::id() as i32;
    let birth = inspect(me).expect("own process identity").birth;
    writer_row(&home, &format!("{me} fixture"), Some(birth));
    assert!(
        deploy::deploy(&prefix, &home, &release, false).is_err(),
        "an exact live archived writer must block the release"
    );

    connection
        .execute(
            "UPDATE workflow_runs SET owner_birth_time = owner_birth_time - 1",
            [],
        )
        .expect("age the recorded birth evidence");
    deploy::deploy(&prefix, &home, &release, false)
        .expect("a different birth time proves the recorded writer is gone");
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_schema_without_birth_still_blocks_a_live_archived_writer`
#[test]
fn python_schema_without_birth_still_blocks_a_live_archived_writer() {
    let (_temporary, prefix, home) = fixture();
    let release = sealed(&prefix, "1.0.0");
    let connection = Connection::open(home.join("state.db")).expect("open fixture database");
    workflow_table(&connection, false);

    writer_row(&home, "999999 missing", None);
    deploy::deploy(&prefix, &home, &release, false)
        .expect("an absent PID is proven gone even without birth evidence");

    connection
        .execute(
            "UPDATE workflow_runs SET owner_pid_identity = ?1",
            params![format!("{} fixture", std::process::id())],
        )
        .expect("point the row at this live process");
    assert!(
        deploy::deploy(&prefix, &home, &release, false).is_err(),
        "an older schema cannot dismiss a PID that still exists"
    );
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_unobservable_archived_writer_blocks_release`
///
/// Python injects `psutil.AccessDenied`; nothing in the Rust path can force
/// the kernel to deny an observation, so the denied/unknown verdicts are
/// asserted at the policy seam and the end-to-end case uses the other form of
/// unusable evidence — an owner string that carries no PID at all.
#[test]
fn python_unobservable_archived_writer_blocks_release() {
    for state in [
        ProcessState::Denied,
        ProcessState::Unknown,
        ProcessState::NotStarted,
        ProcessState::Alive,
    ] {
        assert!(
            deploy::writer_state_blocks(state),
            "{state:?} is uncertainty, never evidence that a writer is dead"
        );
    }
    for state in [ProcessState::Dead, ProcessState::Reused] {
        assert!(
            !deploy::writer_state_blocks(state),
            "{state:?} proves the recorded writer can no longer write"
        );
    }

    let (_temporary, prefix, home) = fixture();
    let release = sealed(&prefix, "1.0.0");
    let connection = Connection::open(home.join("state.db")).expect("open fixture database");
    workflow_table(&connection, true);
    writer_row(&home, "not-a-pid fixture", Some(1.0));
    assert!(
        deploy::deploy(&prefix, &home, &release, false).is_err(),
        "an unreadable writer identity must block the release"
    );
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_birth_verified_zombie_writer_does_not_block_release`
///
/// Python mocks psutil into reporting a running zombie. The zombie here is
/// real — a child that exited and was never reaped — which also exposes a
/// platform difference: macOS answers `proc_pidinfo` for such a child with
/// `ESRCH`, so `Identity::zombie` is only ever set on Linux. Both routes reach
/// the same verdict (`ProcessState::Dead`), which is the behavior Python pins:
/// a process that can no longer execute must not hold up a release.
#[test]
fn python_birth_verified_zombie_writer_does_not_block_release() {
    let (_temporary, prefix, home) = fixture();
    let release = sealed(&prefix, "1.0.0");
    let connection = Connection::open(home.join("state.db")).expect("open fixture database");
    workflow_table(&connection, true);

    let mut child = Command::new("/bin/sh")
        .args(["-c", "exit 0"])
        .spawn()
        .expect("spawn a short-lived fixture process");
    let pid = child.id() as i32;
    let birth = inspect(pid).expect("fixture process identity").birth;
    let executable = || inspect(pid).is_ok_and(|identity| !identity.zombie);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && executable() {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !executable(),
        "the exited, unreaped child must stop observing as an executable process"
    );

    writer_row(&home, &format!("{pid} writer"), Some(birth));
    deploy::deploy(&prefix, &home, &release, false)
        .expect("a birth-verified zombie writer cannot write and must not block");
    assert!(
        deploy::writer_may_be_live(&format!("{} writer", std::process::id()), None),
        "without birth evidence a PID that still resolves must block"
    );
    let _ = child.wait();
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_malformed_archived_writer_birth_blocks_release`
#[test]
fn python_malformed_archived_writer_birth_blocks_release() {
    let (_temporary, prefix, home) = fixture();
    let release = sealed(&prefix, "1.0.0");
    let connection = Connection::open(home.join("state.db")).expect("open fixture database");
    workflow_table(&connection, true);

    let me = std::process::id() as i32;
    let birth = inspect(me).expect("own process identity").birth;
    writer_row(&home, &format!("{me} fixture"), Some(birth - 1.0));
    deploy::deploy(&prefix, &home, &release, false)
        .expect("well-formed mismatched birth evidence proves reuse");

    for malformed in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
        connection
            .execute(
                "UPDATE workflow_runs SET owner_birth_time = ?1",
                params![malformed],
            )
            .expect("store malformed birth evidence");
        assert!(
            deploy::deploy(&prefix, &home, &release, false).is_err(),
            "birth evidence {malformed} cannot prove PID reuse"
        );
    }
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_schema_probe_retries_locks_but_refuses_missing_corrupt_or_mismatched_db`
///
/// Only the "real state defects stay errors" half carries over. Rust never
/// probes a schema version during a switch — migrations run when the store is
/// opened by the runtime, not by the deployer — so Python's retry-on-lock,
/// integrity check and live version/schema comparison have no counterpart,
/// and a home with no `state.db` at all is a first install rather than the
/// error Python raises.
#[test]
fn python_corrupt_state_database_is_never_treated_as_quiescent() {
    let (_temporary, prefix, home) = fixture();
    let release = sealed(&prefix, "1.0.0");
    fs::write(home.join("state.db"), b"this file is not a database")
        .expect("corrupt the fixture store");
    let error = deploy::deploy(&prefix, &home, &release, false)
        .expect_err("an unreadable store can never be read as quiescent");
    assert!(
        error.contains("not a database"),
        "the refusal should name the SQLite defect: {error}"
    );
}

/// Mirrors `tests/test_release_script.py::LocalTests::test_post_shutdown_recheck_prevents_backup_and_migration`
///
/// Only the "no backup was taken" half carries over: Python's admission
/// reservation and service shutdown have no Rust counterpart, but the refusal
/// must still land before any state is copied or the journal advanced.
#[test]
fn python_refused_deploy_takes_no_backup_and_leaves_the_journal() {
    let (_temporary, prefix, home) = fixture();
    let first = sealed(&prefix, "1.0.0");
    let second = sealed(&prefix, "2.0.0");
    deploy::deploy(&prefix, &home, &first, false).expect("install");

    let backups = || {
        fs::read_dir(prefix.join("backups"))
            .expect("backups directory")
            .count()
    };
    let taken = backups();
    let journal = fs::read(prefix.join("deploy.json")).expect("journal after install");

    let connection = Connection::open(home.join("state.db")).expect("open fixture database");
    connection
        .execute("INSERT INTO agents(status) VALUES ('running')", [])
        .expect("insert active agent row");
    let error = deploy::deploy(&prefix, &home, &second, false)
        .expect_err("active work must refuse the switch");
    assert!(error.contains(
        "next operation: stop or wait for the named active agents and workflow writers, then rerun `release deploy`"
    ));
    assert_eq!(backups(), taken, "a refused deploy must not take a backup");
    let refused_journal: serde_json::Value = serde_json::from_slice(
        &fs::read(prefix.join("deploy.json")).expect("journal after refusal"),
    )
    .expect("refusal journal is readable");
    let original_journal: serde_json::Value =
        serde_json::from_slice(&journal).expect("original journal is readable");
    assert_eq!(refused_journal["phase"], "prepared");
    assert_eq!(
        refused_journal["old_release"],
        original_journal["new_release"]
    );
    assert_eq!(
        refused_journal["new_release"],
        second.to_string_lossy().as_ref()
    );
    assert_eq!(
        refused_journal["next_operation"],
        "stop or wait for the named active agents and workflow writers, then rerun `release deploy`"
    );
    assert_eq!(
        fs::read_link(prefix.join("current")).expect("current pointer"),
        first,
        "a refused deploy must not move the current pointer"
    );
}

// Protects the C2 boundary: a real SQLite writer lock must leave a readable
// prepared journal and tell the operator to release that lock.
#[test]
fn t84_db_busy_retains_prepared_phase_and_lock_next_operation() {
    let (_temporary, prefix, home, old, new) = {
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
        let old = release::build(&prefix, "old", &old_binary).expect("old release");
        let new = release::build(&prefix, "new", &new_binary).expect("new release");
        deploy::deploy(&prefix, &home, &old, false).expect("baseline install");
        (temporary, prefix, home, old, new)
    };
    let lock = Connection::open(home.join("state.db")).expect("lock connection");
    lock.execute_batch("BEGIN EXCLUSIVE")
        .expect("hold an exclusive SQLite lock");
    let backups_before = fs::read_dir(prefix.join("backups"))
        .expect("baseline backups")
        .count();
    let candidate_manifest = fs::read(new.join("SHA256SUMS")).expect("candidate manifest");

    let expected =
        "next operation: release the SQLite lock held by another writer, then rerun `release deploy`";
    let error = deploy::deploy(&prefix, &home, &new, false).expect_err("locked DB must refuse");
    assert!(
        error.contains("database is locked"),
        "unexpected error: {error}"
    );
    assert!(
        error.contains(expected),
        "unexpected next operation: {error}"
    );
    let journal: serde_json::Value =
        serde_json::from_slice(&fs::read(prefix.join("deploy.json")).expect("retained journal"))
            .expect("journal is readable");
    assert_eq!(journal["phase"], "prepared");
    assert_eq!(
        journal["next_operation"],
        expected.strip_prefix("next operation: ").unwrap()
    );

    let retry = deploy::deploy(&prefix, &home, &new, false).expect_err("lock is still held");
    assert!(
        retry.contains(expected),
        "retry must name lock operation: {retry}"
    );
    assert_eq!(
        fs::read_link(prefix.join("current")).expect("current pointer"),
        old
    );
    assert_eq!(
        fs::read_dir(prefix.join("backups"))
            .expect("backups after refusal")
            .count(),
        backups_before
    );
    assert_eq!(
        fs::read(new.join("SHA256SUMS")).expect("candidate manifest after refusal"),
        candidate_manifest
    );
}

// Protects C3: removing a named sealed-release asset must not move current or
// turn a missing candidate into a generic retry instruction.
#[test]
fn t84_missing_release_asset_retains_prepared_phase_and_asset_next_operation() {
    let (_temporary, prefix, home, old, new) = cutover_fixture_for_recovery();
    fs::remove_file(new.join("bin/agent-run")).expect("remove named release asset");
    let expected =
        "next operation: restore the named asset or rebuild the sealed candidate, then rerun `release deploy`";
    let error = deploy::deploy(&prefix, &home, &new, false).expect_err("missing asset must refuse");
    assert!(error.contains("candidate release is missing asset bin/agent-run"));
    assert!(
        error.contains(expected),
        "unexpected next operation: {error}"
    );
    let journal: serde_json::Value =
        serde_json::from_slice(&fs::read(prefix.join("deploy.json")).expect("retained journal"))
            .expect("journal is readable");
    assert_eq!(journal["phase"], "prepared");
    assert_eq!(
        journal["next_operation"],
        expected.strip_prefix("next operation: ").unwrap()
    );
    let retry = deploy::deploy(&prefix, &home, &new, false).expect_err("candidate remains broken");
    assert!(
        retry.contains(expected),
        "retry must name asset operation: {retry}"
    );
    assert_eq!(
        fs::read_link(prefix.join("current")).expect("current pointer"),
        old
    );
    assert!(!new.join("bin/agent-run").exists());
}

#[cfg(unix)]
// Protects C3: a real mode-000 candidate asset must fail closed and identify
// the permission repair required before a retry.
#[test]
fn t84_permission_denied_release_asset_retains_prepared_phase_and_permission_next_operation() {
    let (_temporary, prefix, home, old, new) = cutover_fixture_for_recovery();
    let complete = new.join("COMPLETE");
    fs::set_permissions(&complete, fs::Permissions::from_mode(0o000))
        .expect("deny candidate asset read permission");
    let expected =
        "next operation: restore read permission on the named candidate asset, then rerun `release deploy`";
    let error = deploy::deploy(&prefix, &home, &new, false)
        .expect_err("permission-denied asset must refuse");
    assert!(error.contains("candidate release asset access was denied"));
    assert!(
        error.contains(expected),
        "unexpected next operation: {error}"
    );
    let journal: serde_json::Value =
        serde_json::from_slice(&fs::read(prefix.join("deploy.json")).expect("retained journal"))
            .expect("journal is readable");
    assert_eq!(journal["phase"], "prepared");
    assert_eq!(
        journal["next_operation"],
        expected.strip_prefix("next operation: ").unwrap()
    );
    let retry = deploy::deploy(&prefix, &home, &new, false).expect_err("permission remains denied");
    assert!(
        retry.contains(expected),
        "retry must name permission operation: {retry}"
    );
    assert_eq!(
        fs::read_link(prefix.join("current")).expect("current pointer"),
        old
    );
    assert_eq!(
        fs::metadata(&complete)
            .expect("candidate asset remains present")
            .permissions()
            .mode()
            & 0o777,
        0
    );
    fs::set_permissions(&complete, fs::Permissions::from_mode(0o644))
        .expect("restore candidate asset permission");
}

// Protects C4/C9: once xtask has switched its pointer, the journal must name
// the service/readiness/smoke work it cannot perform instead of implying full deployment success.
#[test]
fn t84_committed_journal_names_external_postcondition_operation() {
    let (_temporary, prefix, home, _old, new) = cutover_fixture_for_recovery();
    deploy::deploy(&prefix, &home, &new, false).expect("pointer cutover");
    let journal: serde_json::Value =
        serde_json::from_slice(&fs::read(prefix.join("deploy.json")).expect("retained journal"))
            .expect("journal is readable");
    assert_eq!(journal["phase"], "committed");
    assert_eq!(
        journal["next_operation"],
        "outside xtask, restore services/jobs, verify API readiness and capability discovery, then run the isolated release smoke"
    );
}

/// Builds the common old/current/new release fixture for integration recovery drills.
fn cutover_fixture_for_recovery() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf, PathBuf) {
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
    let old = release::build(&prefix, "old", &old_binary).expect("old release");
    let new = release::build(&prefix, "new", &new_binary).expect("new release");
    deploy::deploy(&prefix, &home, &old, false).expect("baseline install");
    (temporary, prefix, home, old, new)
}
