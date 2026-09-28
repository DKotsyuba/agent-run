//! Disposable-home checks for bounded filesystem retention.

mod common;

use agent_run_core::housekeeping::sweep;
use agent_run_platform::fs;
use rusqlite::params;
use std::{ffi::CString, os::unix::fs::PermissionsExt};

/// Fixed future timestamp makes 2023–2026 fixtures safely older than retention.
const NOW: f64 = 2_000_000_000.0;

/// Creates one private orphan run tree with a content file.
fn run_tree(home: &common::Home, id: &str) {
    let dir = home.path.join("agents").join(id);
    fs::private_dir(&dir).unwrap();
    std::fs::write(dir.join("answer.md"), "fixture").unwrap();
}

/// Inserts a retained row whose structured identity may reference another run path.
fn retained(store: &agent_run_store::Store, id: &str, identity: &str) {
    store
        .conn
        .execute(
            r#"INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,
         status,created_at,timeout_seconds,config_revision,root_agent_id,identity_json)
         VALUES(?,'mock','fixture','review','task','task','/tmp','{"read_roots":[]}',
         'running',1,100,'fixture',?,?)"#,
            params![id, id, identity],
        )
        .unwrap();
}

/// A bounded number of passes removes only old orphan trees, preserving live references.
#[test]
fn orphan_recent_retained_and_escaped_paths() {
    let home = common::Home::new();
    let orphan = "ag-20260101-000000-0000000001";
    let recent = "ag-20350101-000000-0000000002";
    let owned = "ag-20260101-000000-0000000003";
    let referenced = "ag-20260101-000000-0000000004";
    let account = "ag-20260101-000000-0000000005";
    for id in [orphan, recent, owned, referenced, account] {
        run_tree(&home, id);
    }
    let store = home.store();
    retained(&store, owned, "{}");
    let ref_path = home.path.join("agents").join(referenced).join("é");
    let escaped = ref_path.to_string_lossy().replace('é', "\\u00e9");
    retained(
        &store,
        "ag-20340101-000000-0000000006",
        &format!("{{\"runtime_home\":\"{escaped}\"}}"),
    );
    std::fs::create_dir(home.path.join("other")).unwrap();
    let credential = format!(
        "file:{}/other/../agents/{account}/secret",
        home.path.display()
    );
    store.conn.execute(
        "INSERT INTO provider_accounts(account_id,auth_family,secret_ref,status,created_at,updated_at)
         VALUES('fixture','api_key',?,'disabled',1,1)", [credential],
    ).unwrap();
    let proof = store.storage_protection_snapshot().unwrap();
    assert!(!proof.retains(orphan, &home.path.join("agents").join(orphan)));
    for _ in 0..4 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    assert!(!home.path.join("agents").join(orphan).exists());
    for id in [recent, owned, referenced, account] {
        assert!(home.path.join("agents").join(id).exists(), "retained {id}");
    }
}

/// An unlocked persistent install lock permits completed backup expiry; uncertain journals block.
#[test]
fn completed_backup_expires_but_pending_and_corrupt_journals_block() {
    let home = common::Home::new();
    let store = home.store();
    let prefix = home.path.join("standalone");
    fs::private_dir(&prefix.join("backups/1700000000")).unwrap();
    std::fs::write(prefix.join("backups/1700000000/state.db"), "fixture").unwrap();
    std::fs::write(prefix.join(".install.lock"), "").unwrap();
    std::fs::write(prefix.join("deploy.json"), r#"{"phase":"prepared"}"#).unwrap();
    sweep(&home.path, NOW, &store).unwrap();
    assert!(prefix.join("backups/1700000000").exists());
    std::fs::write(prefix.join("deploy.json"), "broken").unwrap();
    sweep(&home.path, NOW, &store).unwrap();
    assert!(prefix.join("backups/1700000000").exists());
    std::fs::write(prefix.join("deploy.json"), r#"{"phase":"committed"}"#).unwrap();
    for _ in 0..4 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    assert!(!prefix.join("backups/1700000000").exists());
}

/// Registered and configured file references protect backups in every cleanup category.
#[test]
fn registered_and_configured_file_refs_protect_backups() {
    let home = common::Home::new();
    let store = home.store();
    let config_backup = home.path.join("config.toml.bak-1700000000");
    std::fs::write(&config_backup, "registered secret").unwrap();
    let unreferenced = home.path.join("config.toml.bak-1700000001");
    std::fs::write(&unreferenced, "obsolete").unwrap();
    let backup = home.path.join("standalone/backups/1700000000");
    fs::private_dir(&backup).unwrap();
    let backup_secret = backup.join("secret");
    std::fs::write(&backup_secret, "registered secret").unwrap();
    let profile = home.path.join("profiles/fixture.md.bak-old");
    fs::private_dir(profile.parent().unwrap()).unwrap();
    std::fs::write(&profile, "configured ref").unwrap();
    let config = home.path.join("config.toml");
    let mut current = std::fs::read_to_string(&config).unwrap();
    current.push_str(&format!(
        "\n[environments.retention_fixture.variables]\nREF_PATH = 'file:{}'\n",
        profile.display()
    ));
    std::fs::write(config, current).unwrap();
    for (id, path) in [
        ("config", config_backup.as_path()),
        ("backup", backup_secret.as_path()),
    ] {
        store.conn.execute(
            "INSERT INTO provider_accounts(account_id,auth_family,secret_ref,status,created_at,updated_at)
             VALUES(?,'api_key',?,'disabled',1,1)",
            params![id,format!("file:{}",path.display())],
        ).unwrap();
    }
    for _ in 0..4 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    assert!(config_backup.exists());
    assert!(backup.exists());
    assert!(profile.exists());
    assert!(!unreferenced.exists());

    std::fs::write(home.path.join("config.toml"), "invalid = [").unwrap();
    let old_log = home.path.join("logs/cli.2026-01-01.log");
    fs::private_dir(old_log.parent().unwrap()).unwrap();
    std::fs::write(&old_log, "old").unwrap();
    assert!(sweep(&home.path, NOW, &store).is_err());
    assert!(
        old_log.exists(),
        "bad protection data blocks every deletion category"
    );
    std::fs::write(home.path.join("config.toml"), "schema_version = 999\n").unwrap();
    assert!(sweep(&home.path, NOW, &store).is_err());
    assert!(
        old_log.exists(),
        "invalid schema also blocks every deletion category"
    );
}

/// A read-only applied snapshot drains across passes while pending migration remains untouched.
#[test]
fn readonly_migration_snapshot_resumes_and_pending_blocks() {
    let home = common::Home::new();
    let store = home.store();
    let migrations = home.path.join("migrations");
    fs::private_dir(&migrations).unwrap();
    let name = "1700000000-0123456789abcdef0123456789abcdef-v1-to-v2";
    let snapshot = migrations.join(name);
    fs::private_dir(&snapshot).unwrap();
    std::fs::write(snapshot.join("COMPLETE"), "ok").unwrap();
    std::fs::write(snapshot.join("state.db"), "fixture").unwrap();
    std::fs::write(migrations.join(format!("{name}.applied.json")), "{}").unwrap();
    std::fs::set_permissions(
        snapshot.join("COMPLETE"),
        std::fs::Permissions::from_mode(0o400),
    )
    .unwrap();
    std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o500)).unwrap();
    std::fs::write(migrations.join("in-progress.json"), "pending").unwrap();
    sweep(&home.path, NOW, &store).unwrap();
    assert!(snapshot.exists());
    std::fs::remove_file(migrations.join("in-progress.json")).unwrap();
    for _ in 0..6 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    if snapshot.exists() {
        std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    assert!(!snapshot.exists());
    assert!(!migrations.join(format!("{name}.applied.json")).exists());
}

/// A retained special entry on an earlier page keeps COMPLETE until the entire tree is clear.
#[test]
fn migration_complete_survives_paginated_special_entry() {
    let home = common::Home::new();
    let store = home.store();
    let migrations = home.path.join("migrations");
    fs::private_dir(&migrations).unwrap();
    let name = "1700000000-abcdef0123456789abcdef0123456789-v2-state-upgrade";
    let snapshot = migrations.join(name);
    fs::private_dir(&snapshot).unwrap();
    let fifo = snapshot.join("00-special");
    let c_path = CString::new(fifo.to_string_lossy().as_bytes()).unwrap();
    // SAFETY: this path is NUL-free and confined to the disposable test home.
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    for n in 0..20 {
        std::fs::write(snapshot.join(format!("file-{n:02}")), "x").unwrap();
    }
    std::fs::write(snapshot.join("COMPLETE"), "ok").unwrap();
    std::fs::write(migrations.join(format!("{name}.applied.json")), "{}").unwrap();
    std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o500)).unwrap();
    for _ in 0..5 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    assert!(
        snapshot.join("COMPLETE").exists(),
        "a paginated FIFO cannot strand the snapshot"
    );
    std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::remove_file(&fifo).unwrap();
    std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o500)).unwrap();
    for _ in 0..5 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    if snapshot.exists() {
        std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    assert!(!snapshot.exists());
}

/// Cursor progress reaches an eligible tree after more than sixteen undeletable roots.
#[test]
fn blocked_first_batch_does_not_starve_later_orphan() {
    let home = common::Home::new();
    let store = home.store();
    for number in 0..17 {
        let id = format!("ag-20260101-000000-{number:010x}");
        let dir = home.path.join("agents").join(&id);
        fs::private_dir(&dir).unwrap();
        let fifo = CString::new(dir.join("held").to_string_lossy().as_bytes()).unwrap();
        // SAFETY: the temporary path is NUL-free and owned by this test.
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    }
    let eligible = "ag-20260101-000000-fffffffffe";
    run_tree(&home, eligible);
    for _ in 0..6 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    assert!(!home.path.join("agents").join(eligible).exists());
}

/// Thirty-day daily logs expire; openable legacy streams and current-day names remain.
#[test]
fn daily_logs_expire_without_touching_legacy_or_recent_names() {
    let home = common::Home::new();
    let store = home.store();
    let logs = home.path.join("logs");
    fs::private_dir(&logs).unwrap();
    std::fs::write(logs.join("cli.2026-01-01.log"), "old").unwrap();
    std::fs::write(logs.join("cli.2035-01-01.log"), "future").unwrap();
    std::fs::write(logs.join("cli.log"), "legacy").unwrap();
    std::fs::write(home.path.join("capacity-worker.err.log"), "legacy launchd").unwrap();
    for _ in 0..4 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    assert!(!logs.join("cli.2026-01-01.log").exists());
    assert!(logs.join("cli.2035-01-01.log").exists());
    assert_eq!(
        std::fs::read_to_string(logs.join("cli.log")).unwrap(),
        "legacy"
    );
    assert!(home.path.join("capacity-worker.err.log").exists());
}

/// A planted symlink cannot redirect cleanup outside the disposable home.
#[test]
fn symlink_escape_is_retained_without_touching_target() {
    let home = common::Home::new();
    let store = home.store();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("sentinel"), "safe").unwrap();
    fs::private_dir(&home.path.join("agents")).unwrap();
    std::os::unix::fs::symlink(
        outside.path(),
        home.path.join("agents/ag-20260101-000000-0123456789"),
    )
    .unwrap();
    sweep(&home.path, NOW, &store).unwrap();
    assert_eq!(
        std::fs::read_to_string(outside.path().join("sentinel")).unwrap(),
        "safe"
    );
}

/// A live Desktop relay stays; a refused socket with the same verified inode is removed.
#[test]
fn live_and_stale_relay_sockets_are_distinguished() {
    let home = common::Home::new();
    let store = home.store();
    let live = home.path.join("ar-cdx-v4-live.sock");
    let stale = home.path.join("ar-cdx-v4-stale.sock");
    let listener = std::os::unix::net::UnixListener::bind(&live).unwrap();
    drop(std::os::unix::net::UnixListener::bind(&stale).unwrap());
    for _ in 0..3 {
        sweep(&home.path, NOW, &store).unwrap();
    }
    assert!(live.exists());
    assert!(!stale.exists());
    drop(listener);
}

/// A complete no-garbage scan ends by the longest namespace, not the LCM of batch counts.
#[test]
fn retained_namespaces_finish_one_scan_round_promptly() {
    let home = common::Home::new();
    let store = home.store();
    for index in 0..31 {
        run_tree(&home, &format!("ag-20350101-000000-{index:010x}"));
    }
    let logs = home.path.join("logs");
    fs::private_dir(&logs).unwrap();
    for day in 1..=31 {
        std::fs::write(logs.join(format!("cli.2035-01-{day:02}.log")), "future").unwrap();
    }
    for day in 1..=16 {
        std::fs::write(logs.join(format!("mcp.2035-02-{day:02}.log")), "future").unwrap();
    }
    let mut calls = 0;
    loop {
        calls += 1;
        let progress = sweep(&home.path, NOW, &store).unwrap();
        if progress == 0 {
            break;
        }
        assert!(
            calls < 4,
            "retained namespaces must finish by the longest 3-batch scan"
        );
    }
    assert_eq!(
        std::fs::read_dir(home.path.join("agents")).unwrap().count(),
        31
    );
    assert_eq!(std::fs::read_dir(logs).unwrap().count(), 47);
}

/// A parent runtime reaching EOF still revisits its late child until that child drains.
#[test]
fn nested_runtime_scan_reaches_late_orphan() {
    let home = common::Home::new();
    let store = home.store();
    fs::private_dir(&home.path.join("agents")).unwrap();
    let mut late_runs = None;
    for runtime in 0..17 {
        let runs = home.path.join(format!("runtimes/r{runtime:02}/home/runs"));
        fs::private_dir(&runs).unwrap();
        if runtime == 16 {
            late_runs = Some(runs);
        }
    }
    let runs = late_runs.unwrap();
    for index in 0..31 {
        fs::private_dir(&runs.join(format!("ag-20350101-000000-{index:010x}"))).unwrap();
    }
    let orphan = runs.join("ag-20260101-000000-fffffffffe");
    fs::private_dir(&orphan).unwrap();
    std::fs::write(orphan.join("answer.md"), "old").unwrap();
    let mut finished = false;
    let mut observed = Vec::new();
    for _ in 0..6 {
        let progress = sweep(&home.path, NOW, &store).unwrap();
        observed.push((progress, orphan.exists()));
        if !orphan.exists() && progress == 0 {
            finished = true;
            break;
        }
    }
    assert!(
        finished,
        "late nested orphan must drain and the scan round must become idle: {observed:?}"
    );
}
