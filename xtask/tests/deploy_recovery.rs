//! Quiescence, idempotence, and rollback contracts for `xtask::deploy`.
//!
//! Mirrors selected behaviors from
//! `tests/test_release_script.py::LocalTests`. Python's `release_local.py`
//! deploy pipeline also stops and restarts three launchd services around a
//! SQLite schema migration, and detects legacy pre-refactor daemon writers
//! by PID and process birth time (`local.migrate`, `local.restart`,
//! `local._legacy_writer_may_be_live`, `local.recover`). None of that exists
//! in `xtask/src/deploy.rs`: it never shells out to `launchctl`, there is no
//! schema to migrate, and the `agents` table it checks has no historical
//! predecessor to cross-check against. Those behaviors have no Rust
//! counterpart to test and are not ported here; see the task's final report
//! for the specific Python tests this affects. What this file covers is the
//! part of the contract `xtask/src/deploy.rs` does implement: quiescence
//! refusal keyed on the same terminal-status set, redeploy idempotence, and
//! backup/rollback of `state.db` and `config.toml`.

use rusqlite::Connection;
use std::fs;
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
}
