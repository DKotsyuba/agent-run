//! Coordinator checks for shared runtime-tree relocation and recovery.
//! Gated on the crate's deterministic test-seams feature.
#![cfg(feature = "test-fixtures")]

use agent_run_core::{
    adapters::materialize,
    runtime_storage::{self, StorageFault},
    state::{runtime_storage::LayoutState, Store},
};
use agent_run_platform::{
    fs,
    shared_assets::{self, SharedTreeRef},
    snapshot_tree::{self, RUNTIME_SNAPSHOT_INDEX},
};
use std::{
    fs as stdfs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};
use tempfile::TempDir;

/// One sealed home fixture: managed tree, flat config, credential link, and
/// a home-local history file outside the asset index.
struct Sealed {
    home: PathBuf,
    index_bytes: Vec<u8>,
    index_sha256: String,
    history: Vec<u8>,
}

/// Seals one home whose managed tree carries `payload`; `stamp` varies the
/// manifest so distinct homes stay distinct trees.
fn seal(root: &Path, name: &str, stamp: &str, payload: &[u8]) -> Sealed {
    let source = root.join(format!("{name}-source"));
    stdfs::create_dir_all(source.join("scripts")).expect("source tree");
    stdfs::write(source.join("SKILL.md"), format!("# skill {stamp}\n")).expect("skill");
    stdfs::write(source.join("scripts/payload.bin"), payload).expect("payload");
    let home = root.join(name);
    snapshot_tree::snapshot_managed_tree(&home, Path::new("skills/demo"), &source, None)
        .expect("managed tree");
    stdfs::write(home.join("config.toml"), "key = \"value\"\n").expect("flat config");
    let credential = root.join(format!("{name}-auth.json"));
    stdfs::write(&credential, "{}\n").expect("credential");
    std::os::unix::fs::symlink(&credential, home.join("auth.json")).expect("credential link");
    let revision = format!("revision-{name}");
    let digest = snapshot_tree::finalize_runtime_snapshots(
        &home,
        &revision,
        &["config.toml".into()],
        &[(
            "auth.json".into(),
            credential.to_string_lossy().into_owned(),
        )],
    )
    .expect("runtime index");
    let index_bytes = stdfs::read(home.join(RUNTIME_SNAPSHOT_INDEX)).expect("index bytes");
    let history = b"{\"sessions\":[]}\n".to_vec();
    stdfs::write(home.join("history.json"), &history).expect("history file");
    Sealed {
        home,
        index_sha256: digest,
        index_bytes,
        history,
    }
}

/// One canonical app home carrying its own disposable store database.
struct App {
    _root: TempDir,
    home: PathBuf,
}

/// Creates the canonical app home fixture.
fn app(root: &Path) -> App {
    let home = root.join("app");
    stdfs::create_dir_all(&home).expect("app home");
    let home = home.canonicalize().expect("canonical app home");
    App {
        _root: TempDir::new().expect("app lifetime"),
        home,
    }
}

/// Backup directory names still present inside one runtime home.
fn backups(home: &Path) -> Vec<String> {
    stdfs::read_dir(home)
        .expect("home listing")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".agent-run-storage-"))
        .collect()
}

/// Makes every readonly store directory removable, then drops the fixture.
fn cleanup_store(store_root: &Path) {
    permit_tree(store_root);
}

/// Restores owner write permission below one fixture tree.
fn permit_tree(path: &Path) {
    if let Ok(metadata) = stdfs::symlink_metadata(path) {
        if metadata.is_dir() {
            let _ = stdfs::set_permissions(path, stdfs::Permissions::from_mode(0o700));
            if let Ok(children) = stdfs::read_dir(path) {
                for child in children.flatten() {
                    permit_tree(&child.path());
                }
            }
        }
    }
}

/// One converted home's managed-root payload inode.
fn tree_payload_inode(store: &Path, reference: &SharedTreeRef) -> u64 {
    stdfs::metadata(
        shared_assets::shared_tree_root(store, reference)
            .expect("tree root")
            .join("scripts/payload.bin"),
    )
    .expect("shared payload")
    .ino()
}

/// The full plan -> install -> verify round trip keeps the original index and
/// history bytes exact while the strict verifier keeps refusing the link.
#[test]
fn plan_install_verify_round_trip_preserves_original_bytes() {
    let root = TempDir::new().expect("fixture root");
    let sealed = seal(root.path(), "home", "a", b"payload-bytes\n");
    let app = app(root.path());
    let mut store = Store::initialize(&app.home).expect("store");
    let scope = fs::sha256(b"coordinator-scope");
    let store_root = runtime_storage::store_root(&app.home).expect("derived store root");
    assert_eq!(
        store_root,
        app.home.join("shared-assets").join("v1"),
        "a missing namespace is derived without mutation"
    );
    assert!(!store_root.exists());
    let layout = runtime_storage::plan(
        &store,
        &app.home,
        &sealed.home,
        &sealed.index_sha256,
        &scope,
    )
    .expect("plan")
    .expect("one managed root");
    assert_eq!(layout.roots.len(), 1);
    assert!(layout.roots.contains_key("skills/demo"));
    runtime_storage::install(&mut store, &app.home, &layout, None).expect("install");
    let record = store
        .runtime_storage_layout(&layout.runtime_home)
        .expect("row")
        .expect("committed row");
    assert_eq!(record.state, LayoutState::Committed);
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("bridge verification");
    assert_eq!(
        stdfs::read(sealed.home.join(RUNTIME_SNAPSHOT_INDEX)).unwrap(),
        sealed.index_bytes,
        "the frozen index bytes stay byte-exact"
    );
    assert_eq!(
        stdfs::read(sealed.home.join("history.json")).unwrap(),
        sealed.history,
        "history stays untouched"
    );
    assert!(
        materialize::verify(&sealed.home, &sealed.index_sha256).is_err(),
        "the strict public verifier still refuses the converted home"
    );
    assert!(
        backups(&sealed.home).is_empty(),
        "proven backups are removed"
    );
    let again = runtime_storage::plan(
        &store,
        &app.home,
        &sealed.home,
        &sealed.index_sha256,
        &scope,
    )
    .expect("re-plan")
    .expect("committed layout");
    assert_eq!(again, layout);
    cleanup_store(&store_root);
}

/// Repeated install and recover after a complete install are idempotent.
#[test]
fn duplicate_install_and_recover_are_idempotent() {
    let root = TempDir::new().expect("fixture root");
    let sealed = seal(root.path(), "home", "a", b"payload-bytes\n");
    let app = app(root.path());
    let mut store = Store::initialize(&app.home).expect("store");
    let scope = fs::sha256(b"coordinator-scope");
    let layout = runtime_storage::plan(
        &store,
        &app.home,
        &sealed.home,
        &sealed.index_sha256,
        &scope,
    )
    .expect("plan")
    .expect("layout");
    runtime_storage::install(&mut store, &app.home, &layout, None).expect("first install");
    runtime_storage::install(&mut store, &app.home, &layout, None).expect("idempotent install");
    runtime_storage::recover(&mut store, &app.home, &sealed.home).expect("recover after success");
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("still verified");
    assert!(backups(&sealed.home).is_empty());
    let rows = store.pending_runtime_storage_layouts(10).expect("pending");
    assert!(rows.is_empty(), "no pending rows remain");
    cleanup_store(&runtime_storage::store_root(&app.home).unwrap());
}

/// Each crash point in the switch sequence leaves a recoverable prepared row
/// that roll-forward recovery completes without re-downloading.
#[test]
fn interrupted_installs_roll_forward_through_recovery() {
    for point in [
        StorageFault::BeforeRename,
        StorageFault::AfterRename,
        StorageFault::AfterLink,
        StorageFault::AfterCommit,
    ] {
        let root = TempDir::new().expect("fixture root");
        let sealed = seal(root.path(), "home", "a", b"payload-bytes\n");
        let app = app(root.path());
        let mut store = Store::initialize(&app.home).expect("store");
        let scope = fs::sha256(b"coordinator-scope");
        let layout = runtime_storage::plan(
            &store,
            &app.home,
            &sealed.home,
            &sealed.index_sha256,
            &scope,
        )
        .expect("plan")
        .expect("layout");
        let fault = |observed: StorageFault| -> agent_run_domain::Result<()> {
            if observed == point {
                Err(agent_run_domain::Error::Validation(
                    "simulated crash".into(),
                ))
            } else {
                Ok(())
            }
        };
        runtime_storage::install_with_fault(&mut store, &app.home, &layout, None, Some(&fault))
            .expect_err("simulated crash aborts the install");
        let record = store
            .runtime_storage_layout(&layout.runtime_home)
            .expect("row")
            .expect("row present");
        assert_eq!(
            record.state,
            if point == StorageFault::AfterCommit {
                LayoutState::Committed
            } else {
                LayoutState::Prepared
            },
            "crash point {point:?} leaves the expected durable state"
        );
        if record.state == LayoutState::Prepared {
            assert!(
                runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
                    .is_err(),
                "a prepared row is an explicit verification failure"
            );
        }
        runtime_storage::recover(&mut store, &app.home, &sealed.home)
            .expect("roll-forward recovery completes");
        runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
            .expect("verified after recovery");
        assert_eq!(
            stdfs::read(sealed.home.join(RUNTIME_SNAPSHOT_INDEX)).unwrap(),
            sealed.index_bytes,
            "recovery never rewrites the frozen index"
        );
        assert!(
            materialize::verify(&sealed.home, &sealed.index_sha256).is_err(),
            "the strict verifier keeps refusing the converted home"
        );
        assert!(
            backups(&sealed.home).is_empty(),
            "backups cleaned: {point:?}"
        );
        assert!(
            store
                .pending_runtime_storage_layouts(10)
                .unwrap()
                .is_empty(),
            "no pending rows remain: {point:?}"
        );
        cleanup_store(&runtime_storage::store_root(&app.home).unwrap());
    }
}

/// A tampered backup and a foreign link are both refused without data loss,
/// and repairing the link restores verification.
#[test]
fn tampered_backup_and_foreign_link_are_refused_without_loss() {
    let root = TempDir::new().expect("fixture root");
    let sealed = seal(root.path(), "home", "a", b"payload-bytes\n");
    let app = app(root.path());
    let mut store = Store::initialize(&app.home).expect("store");
    let scope = fs::sha256(b"coordinator-scope");
    let layout = runtime_storage::plan(
        &store,
        &app.home,
        &sealed.home,
        &sealed.index_sha256,
        &scope,
    )
    .expect("plan")
    .expect("layout");
    let fault = |observed: StorageFault| -> agent_run_domain::Result<()> {
        if observed == StorageFault::AfterCommit {
            Err(agent_run_domain::Error::Validation(
                "simulated crash".into(),
            ))
        } else {
            Ok(())
        }
    };
    runtime_storage::install_with_fault(&mut store, &app.home, &layout, None, Some(&fault))
        .expect_err("crash after commit keeps the backups");
    assert_eq!(backups(&sealed.home).len(), 1);
    let backup = sealed
        .home
        .join(&backups(&sealed.home)[0])
        .join("skills/demo/SKILL.md");
    stdfs::write(&backup, b"tampered\n").expect("tamper staged backup");
    runtime_storage::recover(&mut store, &app.home, &sealed.home)
        .expect_err("an unproven backup is refused");
    assert_eq!(backups(&sealed.home).len(), 1, "the backup stays in place");
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("the installed home itself still verifies");

    let link = sealed.home.join("skills/demo");
    let target = stdfs::read_link(&link).expect("original target");
    stdfs::remove_file(&link).expect("remove link");
    std::os::unix::fs::symlink(&app.home, &link).expect("foreign link");
    runtime_storage::recover(&mut store, &app.home, &sealed.home)
        .expect_err("a foreign link is refused");
    stdfs::remove_file(&link).expect("remove foreign link");
    std::os::unix::fs::symlink(&target, &link).expect("repair link");
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("verification returns after repair");
    cleanup_store(&runtime_storage::store_root(&app.home).unwrap());
}

/// An active home holder blocks a fresh install, and a prepared row refuses
/// replacement by a different layout while explicitly failing verification.
#[test]
fn holders_block_install_and_prepared_rows_refuse_replacement() {
    let root = TempDir::new().expect("fixture root");
    let sealed = seal(root.path(), "home", "a", b"payload-bytes\n");
    let app = app(root.path());
    let mut store = Store::initialize(&app.home).expect("store");
    let canonical = sealed.home.canonicalize().unwrap();
    store
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,\
             status,created_at,timeout_seconds,config_revision,identity_json) \
             VALUES('ag_blocker','codex','m','p','t','t','/tmp','{}','starting',?1,1,'x',?2)",
            rusqlite::params![
                1.0,
                serde_json::json!({"runtime_home": canonical.to_string_lossy()}).to_string()
            ],
        )
        .expect("holder row");
    let scope = fs::sha256(b"coordinator-scope");
    let layout = runtime_storage::plan(
        &store,
        &app.home,
        &sealed.home,
        &sealed.index_sha256,
        &scope,
    )
    .expect("plan")
    .expect("layout");
    let blocked = runtime_storage::install(&mut store, &app.home, &layout, None)
        .expect_err("an unresolved holder blocks installation");
    assert!(matches!(blocked, agent_run_domain::Error::Conflict));
    assert!(store
        .runtime_storage_layout(&layout.runtime_home)
        .unwrap()
        .is_none());

    store
        .conn
        .execute("DELETE FROM agents WHERE id='ag_blocker'", [])
        .expect("release holder");
    let fault = |observed: StorageFault| -> agent_run_domain::Result<()> {
        if observed == StorageFault::BeforeRename {
            Err(agent_run_domain::Error::Validation(
                "simulated crash".into(),
            ))
        } else {
            Ok(())
        }
    };
    runtime_storage::install_with_fault(&mut store, &app.home, &layout, None, Some(&fault))
        .expect_err("crash prepares the row");
    let other_scope = fs::sha256(b"coordinator-other");
    let other = runtime_storage::plan(
        &store,
        &app.home,
        &sealed.home,
        &sealed.index_sha256,
        &other_scope,
    )
    .expect("plan")
    .expect("layout");
    assert!(
        runtime_storage::install(&mut store, &app.home, &other, None).is_err(),
        "a prepared row refuses a different layout"
    );
    runtime_storage::recover(&mut store, &app.home, &sealed.home).expect("finish original");
    runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
        .expect("original layout verified");
    cleanup_store(&runtime_storage::store_root(&app.home).unwrap());
}

/// Two homes carrying the same payload converge on one shared payload inode,
/// and deleting their replaced private copies preserves both.
#[test]
fn two_homes_share_one_payload_inode_after_replacement() {
    let root = TempDir::new().expect("fixture root");
    let first = seal(root.path(), "first", "same", b"shared payload\n");
    let second = seal(root.path(), "second", "same", b"shared payload\n");
    let app = app(root.path());
    let mut store = Store::initialize(&app.home).expect("store");
    let scope = fs::sha256(b"coordinator-scope");
    let store_root = runtime_storage::store_root(&app.home).unwrap();
    let mut references = Vec::new();
    for sealed in [&first, &second] {
        let layout = runtime_storage::plan(
            &store,
            &app.home,
            &sealed.home,
            &sealed.index_sha256,
            &scope,
        )
        .expect("plan")
        .expect("layout");
        runtime_storage::install(&mut store, &app.home, &layout, None).expect("install");
        let mapping = layout.roots.get("skills/demo").expect("mapping");
        references.push(SharedTreeRef {
            scope: mapping.scope.clone(),
            manifest_sha256: mapping.manifest_sha256.clone(),
        });
        assert!(
            backups(&sealed.home).is_empty(),
            "each home's replaced private copy is removed after proof"
        );
    }
    assert_eq!(references[0], references[1]);
    let shared_inode = tree_payload_inode(&store_root, &references[0]);
    for sealed in [&first, &second] {
        assert_eq!(
            stdfs::metadata(sealed.home.join("skills/demo/scripts/payload.bin"))
                .expect("linked payload")
                .ino(),
            shared_inode
        );
        runtime_storage::verify(&store, &app.home, &sealed.home, &sealed.index_sha256)
            .expect("both homes still verify after their copies were deleted");
    }
    cleanup_store(&store_root);
}
