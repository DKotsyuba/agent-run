//! Operator storage-administration surface: status, compact, recover.
//! Gated on the crate's deterministic test seams for interrupted installs.
#![cfg(feature = "test-fixtures")]

use agent_run::storage_admin;
use agent_run_core::{fs, runtime_storage, runtime_storage::StorageFault, state::Store};
use fs2::FileExt;
use agent_run_platform::snapshot_tree::{self, RUNTIME_SNAPSHOT_INDEX};
use rusqlite::params;
use std::{
    fs as stdfs,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};
use tempfile::TempDir;

/// One canonical, current-schema home with an initialized store.
struct Home {
    _temp: TempDir,
    path: PathBuf,
}

/// Creates the home with an owned state database.
fn home() -> Home {
    let temp = tempfile::Builder::new()
        .prefix("ar-storage-")
        .tempdir_in(std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into()))
        .expect("fixture home");
    let path = temp.path().canonicalize().expect("canonical home");
    fs::private_dir(&path).expect("private home");
    Store::initialize(&path).expect("store");
    Home { _temp: temp, path }
}

/// Seals one managed runtime home with an index digest and history bytes.
fn seal(root: &Path, name: &str, payload: &[u8]) -> (PathBuf, String, Vec<u8>, Vec<u8>) {
    let source = root.join(format!("{name}-source"));
    stdfs::create_dir_all(source.join("scripts")).expect("source");
    stdfs::write(source.join("SKILL.md"), "# skill\n").expect("skill");
    stdfs::write(source.join("scripts/payload.bin"), payload).expect("payload");
    let home = root.join(name);
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None)
        .expect("managed tree");
    stdfs::write(home.join("config.toml"), "key = \"value\"\n").expect("config");
    let digest = snapshot_tree::finalize_runtime_snapshots(
        &home,
        &format!("revision-{name}"),
        &["config.toml".into()],
        &[],
    )
    .expect("index");
    let history = b"{\"sessions\":[]}\n".to_vec();
    stdfs::write(home.join("history.json"), &history).expect("history");
    let index = stdfs::read(home.join(RUNTIME_SNAPSHOT_INDEX)).expect("index bytes");
    (home, digest, index, history)
}

/// Records one agent row whose frozen identity binds `runtime_home` with the
/// sealed index digest.
fn retained(store: &Store, id: &str, runtime_home: &Path, digest: &str, status: &str) {
    store
        .conn
        .execute(
            r#"INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,
         status,created_at,timeout_seconds,config_revision,root_agent_id,identity_json)
         VALUES(?1,'glm-user','fixture','review','task','task','/tmp','{"read_roots":[]}',
         ?4,1,100,'fixture',?1,json_object('runtime_home',?2,'snapshot_sha256',?3))"#,
            params![id, runtime_home.to_string_lossy(), digest, status],
        )
        .expect("agent row");
}

/// Digest of every regular file below one path, for change detection.
fn tree_digest(path: &Path) -> String {
    fn walk(dir: &Path, out: &mut String) {
        let mut names: Vec<_> = stdfs::read_dir(dir)
            .expect("read dir")
            .flatten()
            .map(|entry| entry.path())
            .collect();
        names.sort();
        for child in names {
            let metadata = stdfs::symlink_metadata(&child).expect("metadata");
            out.push_str(&child.to_string_lossy());
            if metadata.is_dir() {
                walk(&child, out);
            } else if metadata.is_file() {
                out.push_str(&fs::sha256(&stdfs::read(&child).expect("bytes")));
            }
        }
    }
    let mut out = String::new();
    walk(path, &mut out);
    fs::sha256(out.as_bytes())
}

/// Restores owner write below one fixture tree so the temporary dir can drop.
fn permit(path: &Path) {
    if let Ok(metadata) = stdfs::symlink_metadata(path) {
        if metadata.is_dir() {
            let _ = stdfs::set_permissions(path, stdfs::Permissions::from_mode(0o700));
            if let Ok(children) = stdfs::read_dir(path) {
                for child in children.flatten() {
                    permit(&child.path());
                }
            }
        }
    }
}

/// Holds the broker startup lock so `--apply` must refuse.
fn hold_broker_lock(home: &Path) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(home.join(".api.sock.lock"))
        .expect("broker lock")
}

/// The dry run changes no database, configuration or file, classifies the
/// homes from durable evidence, and reports store and free-space numbers
/// separately.
#[test]
fn dry_run_is_read_only_and_reports_both_size_bases() {
    let fixture = home();
    let root = fixture._temp.path();
    let (runtime, digest, _index, _history) = seal(root, "legacy", b"private-bytes\n");
    let store = Store::open(&fixture.path).expect("store");
    retained(&store, "ag-20260101-000000-0000000001", &runtime, &digest, "succeeded");
    drop(store);

    let before_config = stdfs::read(fixture.path.join("config.toml")).ok();
    let before_db = stdfs::read(fixture.path.join("state.db")).expect("database");
    let before_tree = tree_digest(&fixture.path);
    let status = storage_admin::status(&fixture.path).expect("status");
    assert_eq!(status["state_schema_version"], 22);
    assert_eq!(status["homes"]["entries"].as_array().map(Vec::len), Some(1));
    let entry = &status["homes"]["entries"][0];
    assert_eq!(entry["state"], "eligible");
    assert_eq!(entry["measured"], true);
    assert_eq!(status["store_present"], false);
    assert!(
        status["filesystem_free_bytes"].as_u64().unwrap_or(0) > 0,
        "physical free space is reported separately"
    );
    let compact = storage_admin::compact(&fixture.path, false).expect("dry run");
    assert_eq!(compact["applied"], false);
    assert_eq!(compact["plan"]["homes"]["entries"][0]["state"], "eligible");
    assert_eq!(before_config, stdfs::read(fixture.path.join("config.toml")).ok());
    assert_eq!(before_db, stdfs::read(fixture.path.join("state.db")).expect("database"));
    assert_eq!(before_tree, tree_digest(&fixture.path));
    permit(&runtime);
}

/// An older state schema is refused with `migration_required` and nothing is
/// opened or upgraded implicitly.
#[test]
fn unsupported_schema_is_refused_without_upgrade() {
    let fixture = home();
    let connection = rusqlite::Connection::open(fixture.path.join("state.db")).expect("db");
    connection
        .pragma_update(None, "user_version", 21_i64)
        .expect("stamp v21");
    drop(connection);
    let before = stdfs::read(fixture.path.join("state.db")).expect("database");
    for result in [
        storage_admin::status(&fixture.path),
        storage_admin::compact(&fixture.path, false),
        storage_admin::compact(&fixture.path, true),
        storage_admin::recover(&fixture.path),
    ] {
        let error = result.expect_err("older schema is refused");
        assert!(
            error.to_string().contains("migration_required"),
            "{error}"
        );
    }
    assert_eq!(
        stdfs::read(fixture.path.join("state.db")).expect("database"),
        before,
        "no command opened or upgraded the older database"
    );
}

/// Apply refuses while the resident broker or service manager holds its
/// startup lock, and while any agent is active.
#[test]
fn apply_refuses_busy_broker_and_active_agents() {
    let fixture = home();
    let root = fixture._temp.path();
    let (runtime, digest, _index, _history) = seal(root, "legacy", b"private-bytes\n");
    let store = Store::open(&fixture.path).expect("store");
    retained(&store, "ag-20260101-000000-0000000001", &runtime, &digest, "succeeded");
    drop(store);
    let lock = hold_broker_lock(&fixture.path);
    lock.lock_exclusive().expect("broker lock held");
    let error = storage_admin::compact(&fixture.path, true).expect_err("broker is excluded");
    assert!(error.to_string().contains("broker"), "{error}");
    drop(lock);

    let store = Store::open(&fixture.path).expect("store");
    retained(
        &store,
        "ag-20260101-000000-0000000002",
        &runtime,
        &digest,
        "running",
    );
    drop(store);
    let error = storage_admin::compact(&fixture.path, true).expect_err("active agent refused");
    assert!(
        error.to_string().contains("active agents"),
        "{error}"
    );
    let error = storage_admin::recover(&fixture.path).expect_err("active agent refused");
    assert!(error.to_string().contains("active agents"), "{error}");
    permit(&runtime);
}

/// A retained legacy home that cannot be qualified is skipped with its reason
/// and preserved byte count; nothing is rewritten and the plan stays honest.
#[test]
fn unverifiable_home_is_skipped_and_preserved() {
    let fixture = home();
    let root = fixture._temp.path();
    let (runtime, digest, index, history) = seal(root, "legacy", b"precious-bytes\n");
    let store = Store::open(&fixture.path).expect("store");
    retained(&store, "ag-20260101-000000-0000000001", &runtime, &digest, "succeeded");
    drop(store);
    // The recorded identity carries none of the provider authority a
    // relocation preflight needs, so the home must be skipped, not forced.
    let result = storage_admin::compact(&fixture.path, true).expect("apply");
    assert_eq!(result["applied"], true);
    let relocations = result["relocations"].as_array().expect("relocations");
    assert_eq!(relocations.len(), 1);
    assert_eq!(relocations[0]["relocated"], false);
    assert!(
        !relocations[0]["reason"].as_str().unwrap_or_default().is_empty(),
        "the skip states its reason: {relocations:?}"
    );
    assert!(
        relocations[0]["preserved_private_bytes"].as_u64().unwrap_or(0) > 0,
        "the preserved bytes are measured: {relocations:?}"
    );
    assert_eq!(
        stdfs::read(runtime.join(RUNTIME_SNAPSHOT_INDEX)).expect("index"),
        index,
        "the frozen index bytes are untouched"
    );
    assert_eq!(
        stdfs::read(runtime.join("history.json")).expect("history"),
        history,
        "native history is untouched"
    );
    assert!(
        stdfs::metadata(runtime.join("skills/demo"))
            .expect("managed root")
            .is_dir(),
        "the private managed root is preserved"
    );
    permit(&runtime);
}

/// One interrupted relocation is recovered forward, idempotently, with the
/// original index and history bytes unchanged throughout.
#[test]
fn interrupted_relocation_recovers_forward_and_stays_idempotent() {
    let fixture = home();
    let root = fixture._temp.path();
    let (runtime, digest, index, history) = seal(root, "legacy", b"recoverable-bytes\n");
    let mut store = Store::open(&fixture.path).expect("store");
    retained(&store, "ag-20260101-000000-0000000001", &runtime, &digest, "succeeded");
    let layout = runtime_storage::plan(
        &store,
        &fixture.path,
        &runtime,
        &digest,
        &agent_run_core::supervisor::managed_scope(),
    )
    .expect("plan")
    .expect("managed roots");
    let fault = |_: runtime_storage::StorageFault| -> agent_run_domain::Result<()> {
        Err(agent_run_domain::Error::Validation("simulated crash".into()))
    };
    runtime_storage::install_with_fault(&mut store, &fixture.path, &layout, None, Some(&fault))
        .expect_err("crash before commit");
    drop(store);

    let recovered = storage_admin::recover(&fixture.path).expect("recover");
    assert_eq!(recovered["recovered"], 1, "{recovered}");
    assert_eq!(recovered["refused"], 0);
    assert_eq!(
        stdfs::read(runtime.join(RUNTIME_SNAPSHOT_INDEX)).expect("index"),
        index,
        "the frozen index bytes stay byte-exact through recovery"
    );
    assert_eq!(
        stdfs::read(runtime.join("history.json")).expect("history"),
        history
    );
    // Recovering again is a no-op, and the recovered home keeps verifying.
    let again = storage_admin::recover(&fixture.path).expect("recover again");
    assert_eq!(again["recovered"], 0);
    assert_eq!(again["refused"], 0);
    let store = Store::open(&fixture.path).expect("store");
    runtime_storage::verify(&store, &fixture.path, &runtime, &digest).expect("bridge verify");
    let status = storage_admin::status(&fixture.path).expect("status");
    assert_eq!(status["homes"]["entries"][0]["state"], "shared");
    permit(&runtime_storage::store_root(&fixture.path).expect("store root"));
    permit(&runtime);
}

/// A prepared row whose home was altered mid-switch — here a foreign link
/// planted at a managed-root name — is refused explicitly and left exactly as
/// found; recovery never guesses a path or rolls back.
#[test]
fn foreign_prepared_home_is_refused_not_guessed() {
    let fixture = home();
    let root = fixture._temp.path();
    let (runtime, digest, _index, _history) = seal(root, "legacy", b"foreign-bytes\n");
    let mut store = Store::open(&fixture.path).expect("store");
    retained(&store, "ag-20260101-000000-0000000001", &runtime, &digest, "succeeded");
    let layout = runtime_storage::plan(
        &store,
        &fixture.path,
        &runtime,
        &digest,
        &agent_run_core::supervisor::managed_scope(),
    )
    .expect("plan")
    .expect("managed roots");
    let fault = |point: StorageFault| -> agent_run_domain::Result<()> {
        if point == StorageFault::AfterRename {
            Err(agent_run_domain::Error::Validation("simulated crash".into()))
        } else {
            Ok(())
        }
    };
    runtime_storage::install_with_fault(&mut store, &fixture.path, &layout, None, Some(&fault))
        .expect_err("crash after the private root moved into its backup");
    drop(store);
    // Plant a foreign link at the missing managed-root name.
    let outside = root.join("outside");
    stdfs::create_dir_all(&outside).expect("outside tree");
    std::os::unix::fs::symlink(&outside, runtime.join("skills/demo")).expect("foreign link");

    let recovered = storage_admin::recover(&fixture.path).expect("recover");
    assert_eq!(recovered["recovered"], 0, "{recovered}");
    assert_eq!(recovered["refused"], 1);
    let reason = recovered["outcomes"][0]["reason"].as_str().unwrap_or_default();
    assert!(reason.contains("foreign"), "the refusal states its reason: {reason}");
    let store = Store::open(&fixture.path).expect("store");
    let state = store
        .runtime_storage_layout(&layout.runtime_home)
        .expect("row")
        .expect("still prepared");
    assert_eq!(
        state.state,
        agent_run_store::runtime_storage::LayoutState::Prepared,
        "the foreign row is left exactly as found"
    );
    assert!(
        stdfs::symlink_metadata(runtime.join("skills/demo"))
            .expect("foreign link")
            .file_type()
            .is_symlink(),
        "nothing was overwritten or removed"
    );
    permit(&runtime);
    permit(&runtime_storage::store_root(&fixture.path).expect("store root"));
}
